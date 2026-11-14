import {
  ProviderDriverKind,
  ProviderInstanceId,
  UsageLimitSourceId,
  type ServerProvider,
  type UsageLimitSourceSnapshots,
  type UsageLimitsReport,
} from "@t3tools/contracts";
import { describe, expect, it } from "vite-plus/test";
import {
  collectThreadUsageLimits,
  hasThreadUsageLimits,
  pickUsageGauge,
  threadDetailsLimits,
} from "./threadUsageLimits.ts";

const checkedAt = "2026-10-22T15:00:00-05:00";
const now = Date.parse(checkedAt);
const limits = {
  checkedAt,
  windows: [{ id: "session", kind: "session" as const, label: "Session", usedPercent: 42 }],
};
function provider(id: string, driver: string, withLimits = false): ServerProvider {
  return {
    instanceId: ProviderInstanceId.make(id),
    driver: ProviderDriverKind.make(driver),
    enabled: true,
    installed: true,
    version: null,
    status: "ready",
    auth: { status: "authenticated" },
    checkedAt,
    models: [],
    slashCommands: [],
    skills: [],
    ...(withLimits ? { usageLimits: limits } : {}),
  };
}
const pi = provider("pi-main", "pi");
const piSources: UsageLimitSourceSnapshots = [
  {
    id: UsageLimitSourceId.make("pi"),
    kind: "pi",
    label: "Pi",
    checkedAt,
    accounts: ["Claude", "Codex"].map((id) => ({
      id,
      driver: pi.driver,
      usageLimits: limits,
    })),
  },
];

describe("pickUsageGauge", () => {
  it.each([
    ["claudeAgent", "account-b.json"],
    ["pi", "anthropic-cache"],
  ])("matches opaque backing drivers and Pi cache labels (%s)", (driver, label) => {
    const report = {
      accounts: [
        {
          id: "account-a.json",
          label: "account-a.json",
          driver: "codex",
          limits: { windows: [{ usedPercent: 99 }] },
        },
        { id: "account-b.json", label, driver, limits: { windows: [{ usedPercent: 10 }] } },
      ],
    } as unknown as UsageLimitsReport;
    expect(
      pickUsageGauge(report, "claude-sonnet-4-6", ProviderDriverKind.make("claudeAgent"))?.window
        .usedPercent,
    ).toBe(10);
    expect(
      pickUsageGauge(report, "gpt-5", ProviderDriverKind.make("codex"))?.window.usedPercent,
    ).toBe(99);
  });
});

describe("Pi thread usage limits", () => {
  it("shows no Pi gauge for unknown models, even with only one cached account", () => {
    const providers = [pi, provider("claude-main", "claudeAgent", true)];
    for (const sources of [
      piSources,
      [{ ...piSources[0]!, accounts: piSources[0]!.accounts.slice(0, 1) }],
    ]) {
      const report = collectThreadUsageLimits(pi.instanceId, providers, sources, now);
      for (const model of ["grok-4", "gemini-3-pro", "ollama/llama", "default", null]) {
        expect(pickUsageGauge(report, model, pi.driver)).toBeNull();
        expect(threadDetailsLimits(pi.instanceId, providers, sources, now, model)).toBeUndefined();
      }
    }
  });

  it("keeps two native logins separate and gauges only Pi's own vendor report", () => {
    const logins = ["a@example.com", "b@example.com"].map((email, index) => ({
      ...provider(`claude-${index}`, "claudeAgent", true),
      auth: { status: "authenticated" as const, email },
      usageLimits: { ...limits, windows: [{ ...limits.windows[0]!, usedPercent: 90 + index }] },
    }));
    const providers = [pi, ...logins, provider("codex-main", "codex", true)];
    const report = collectThreadUsageLimits(pi.instanceId, providers, piSources, now);
    expect(report?.accounts.map((account) => account.id)).toEqual([
      "pi:Claude",
      "pi:Codex",
      "codex-main",
      "claude-0",
      "claude-1",
    ]);
    for (const [model, id] of [
      ["claude-opus", "pi:Claude"],
      ["gpt-5.4", "pi:Codex"],
    ]) {
      const gauge = pickUsageGauge(report, model, pi.driver);
      expect(gauge?.account.id).toBe(id);
      expect(gauge?.window.usedPercent).toBe(42);
      expect(threadDetailsLimits(pi.instanceId, providers, piSources, now, model)).toBe(limits);
    }
    expect(
      threadDetailsLimits(logins[0]!.instanceId, providers, piSources, now, "claude-opus"),
    ).toBe(logins[0]!.usageLimits);
  });

  it("does not substitute native quotas for missing Pi reports", () => {
    const codex = {
      ...provider("codex-main", "codex", true),
      usageLimits: { ...limits, windows: [{ ...limits.windows[0]!, usedPercent: 81 }] },
    };
    const claude = provider("claude-main", "claudeAgent", true);
    const providers = [pi, codex, claude];
    expect(threadDetailsLimits(pi.instanceId, providers, [], now, "gpt-5.4")).toBeUndefined();
    expect(threadDetailsLimits(pi.instanceId, providers, [], now, "claude-opus")).toBeUndefined();
    expect(threadDetailsLimits(claude.instanceId, providers, [], now, "gpt-5.4")).toBe(
      claude.usageLimits,
    );
  });

  it("collects native Codex and Claude accounts without relabelling them as Pi", () => {
    const providers = [
      pi,
      provider("codex-main", "codex", true),
      provider("claude-main", "claudeAgent", true),
      provider("cursor-main", "cursor", true),
    ];
    expect(hasThreadUsageLimits(pi.driver, providers, [])).toBe(true);
    const report = collectThreadUsageLimits(pi.instanceId, providers, [], now);
    expect(report?.accounts.map((account) => [account.id, account.driver])).toEqual([
      ["codex-main", "codex"],
      ["claude-main", "claudeAgent"],
    ]);
    expect(report?.accounts.map((account) => account.resetCreditInput)).toEqual([
      { instanceId: "codex-main" },
      { instanceId: "claude-main" },
    ]);
  });

  it("resolves source-only backing accounts and preserves Pi's own cache", () => {
    const sources: UsageLimitSourceSnapshots = [
      {
        id: UsageLimitSourceId.make("hub"),
        kind: "cliproxy",
        label: "Hub",
        checkedAt,
        accounts: [
          { id: "codex", driver: ProviderDriverKind.make("codex"), usageLimits: limits },
          { id: "claude", driver: ProviderDriverKind.make("claudeAgent"), usageLimits: limits },
          { id: "cursor", driver: ProviderDriverKind.make("cursor"), usageLimits: limits },
        ],
      },
      {
        id: UsageLimitSourceId.make("pi"),
        kind: "pi",
        label: "Pi",
        checkedAt,
        accounts: [{ id: "Claude", driver: pi.driver, usageLimits: limits }],
      },
    ];
    expect(hasThreadUsageLimits(pi.driver, [pi], sources.slice(0, 1))).toBe(true);
    const report = collectThreadUsageLimits(pi.instanceId, [pi], sources, now);
    expect(report?.accounts.map((account) => [account.id, account.driver])).toEqual([
      ["pi:Claude", "pi"],
      ["hub:codex", "codex"],
      ["hub:claude", "claudeAgent"],
    ]);
  });

  it("does not offer unavailable Pi quotas or broaden another driver's report", () => {
    expect(hasThreadUsageLimits(pi.driver, [pi], [])).toBe(false);
    expect(collectThreadUsageLimits(pi.instanceId, [pi], [], now)).toBeNull();
    const codex = provider("codex-main", "codex", true);
    const providers = [pi, codex, provider("claude-main", "claudeAgent", true)];
    expect(
      collectThreadUsageLimits(codex.instanceId, providers, [], now)?.accounts.map(
        (account) => account.id,
      ),
    ).toEqual(["codex-main"]);
    expect(
      collectThreadUsageLimits(ProviderInstanceId.make("missing"), providers, [], now),
    ).toBeNull();
  });

  it("shows a failed source once rather than once per backing driver", () => {
    const sources: UsageLimitSourceSnapshots = [
      {
        id: UsageLimitSourceId.make("hub"),
        kind: "cliproxy",
        label: "Hub",
        checkedAt,
        accounts: [],
        error: "offline",
      },
    ];
    expect(collectThreadUsageLimits(pi.instanceId, [pi], sources, now)?.notices).toEqual([
      "Hub: offline",
    ]);
  });
});
