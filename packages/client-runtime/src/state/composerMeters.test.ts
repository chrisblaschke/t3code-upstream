import type { OrchestrationV2ThreadProjection } from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import { describe, expect, it } from "vite-plus/test";

import { contextUsageBudget, latestContextUsage } from "./composerMeters.ts";

type Input = Parameters<typeof latestContextUsage>[0];
const turn = (tokenUsage?: object, providerThreadId = "live") =>
  ({
    tokenUsage,
    providerThreadId,
  }) as unknown as OrchestrationV2ThreadProjection["providerTurns"][number];
const providerThread = (id: string, contextUsage: object) =>
  ({ id, contextUsage }) as unknown as OrchestrationV2ThreadProjection["providerThreads"][number];

describe("latestContextUsage", () => {
  it.each([
    ["2026-01-03T00:00:00Z", 25_000],
    ["2026-01-01T00:00:00Z", 180_000],
    ["2026-01-02T00:00:00Z", 180_000],
  ])("compares the active thread snapshot timestamp %s to the turn", (updatedAt, usedTokens) => {
    const input = {
      activeProviderThreadId: "live",
      providerTurns: [
        turn({
          usedTokens: 180_000,
          maxTokens: 200_000,
          updatedAt: "2026-01-02T00:00:00Z",
        }),
      ],
      providerThreads: [
        {
          ...providerThread("live", { usedTokens: 25_000, maxTokens: 200_000, updatedAt }),
          updatedAt: DateTime.makeUnsafe(updatedAt),
        },
      ],
    } as unknown as Input;
    expect(latestContextUsage(input)?.usedTokens).toBe(usedTokens);
  });

  it.each(["2026-01-01T00:00:00Z", undefined])(
    "ignores a status-only thread update with usage timestamp %s",
    (updatedAt) => {
      const input = {
        activeProviderThreadId: "live",
        providerTurns: [
          turn({ usedTokens: 180_000, maxTokens: 200_000, updatedAt: "2026-01-02T00:00:00Z" }),
        ],
        providerThreads: [
          {
            ...providerThread("live", { usedTokens: 25_000, maxTokens: 200_000, updatedAt }),
            updatedAt: DateTime.makeUnsafe("2026-01-03T00:00:00Z"),
          },
        ],
      } as unknown as Input;
      expect(latestContextUsage(input)?.usedTokens).toBe(180_000);
    },
  );

  it("takes the newest report by timestamp, regardless of provider-thread array order", () => {
    const input = {
      providerTurns: [
        turn({
          usedTokens: 40,
          maxTokens: 100,
          autoCompactThreshold: 80,
          updatedAt: "2026-01-02T00:00:00Z",
        }),
        turn({ usedTokens: 10, maxTokens: 100, updatedAt: "2026-01-01T00:00:00Z" }),
        turn(),
        turn({ usedTokens: 900, maxTokens: 1000, updatedAt: "2026-01-03T00:00:00Z" }, "old"),
      ],
      providerThreads: [],
      activeProviderThreadId: "live",
    } as unknown as Input;
    expect(latestContextUsage(input)).toEqual({
      usedTokens: 40,
      maxTokens: 100,
      autoCompactThreshold: 80,
    });
  });

  it("falls back to the active provider thread's stored usage", () => {
    const input = {
      providerTurns: [
        turn({ usedTokens: 900, maxTokens: 1000, updatedAt: "2026-01-03T00:00:00Z" }, "old"),
      ],
      providerThreads: [
        providerThread("old", { usedTokens: 1 }),
        providerThread("live", { usedTokens: 5, maxTokens: 50 }),
      ],
      activeProviderThreadId: "live",
    } as unknown as Input;
    expect(latestContextUsage(input)).toEqual({
      usedTokens: 5,
      maxTokens: 50,
      autoCompactThreshold: null,
    });
  });
});

describe("contextUsageBudget", () => {
  it("fills toward Pi's compaction point rather than the whole window", () => {
    const budget = contextUsageBudget({
      usedTokens: 86_000,
      maxTokens: 1_000_000,
      autoCompactThreshold: 200_000,
    });
    expect(budget.limit).toBe(200_000);
    expect(budget.usedPercentage).toBeCloseTo(43);
  });

  it("uses the window when nothing compacts earlier", () => {
    expect(
      contextUsageBudget({ usedTokens: 50, maxTokens: 200, autoCompactThreshold: null })
        .usedPercentage,
    ).toBe(25);
  });
});
