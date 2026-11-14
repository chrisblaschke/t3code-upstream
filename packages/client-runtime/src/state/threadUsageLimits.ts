import {
  ProviderDriverKind,
  type ProviderInstanceId,
  type ServerProviderUsageWindow,
  type ServerProvider,
  type UsageLimitSourceSnapshots,
  type UsageLimitsReport,
} from "@t3tools/contracts";
import { collectProviderUsageLimits, hasProviderUsageLimits } from "@t3tools/shared/usageLimits";

type Account = UsageLimitsReport["accounts"][number];

// Preserve the selected instance for ordinary providers.
export function threadDetailsLimits(
  instanceId: ProviderInstanceId,
  providers: readonly ServerProvider[],
  sources: UsageLimitSourceSnapshots,
  now: number,
  model?: string | null,
) {
  const selected = providers.find((provider) => provider.instanceId === instanceId);
  const report = collectThreadUsageLimits(instanceId, providers, sources, now);
  if (selected?.driver === "pi") {
    return pickUsageGauge(report, model, selected.driver)?.account.limits;
  }
  return (
    report?.accounts.find((entry) => entry.instanceId === instanceId)?.limits ??
    providers.find((provider) => provider.instanceId === instanceId)?.usageLimits
  );
}

// Pi routes models to these subscription providers as well as reporting its
// own cache. Keep the backing drivers intact so account identity and reset
// credit targets still belong to the real Codex/Claude account.
const PI_QUOTA_DRIVERS = [ProviderDriverKind.make("codex"), ProviderDriverKind.make("claudeAgent")];

export function hasThreadUsageLimits(
  driver: ServerProvider["driver"],
  providers: readonly ServerProvider[],
  sources: UsageLimitSourceSnapshots,
): boolean {
  return (
    hasProviderUsageLimits(driver, providers, sources) ||
    (driver === "pi" &&
      PI_QUOTA_DRIVERS.some((backing) => hasProviderUsageLimits(backing, providers, sources)))
  );
}

export function collectThreadUsageLimits(
  instanceId: ProviderInstanceId,
  providers: readonly ServerProvider[],
  sources: UsageLimitSourceSnapshots,
  now: number,
): UsageLimitsReport | null {
  const selected = providers.find((provider) => provider.instanceId === instanceId);
  const own = collectProviderUsageLimits(instanceId, providers, sources, now);
  if (selected?.driver !== "pi") return own;

  const reports = [
    own,
    ...PI_QUOTA_DRIVERS.map((driver) =>
      collectProviderUsageLimits(
        instanceId,
        [
          // A disabled selector lets the shared collector find source-only
          // accounts too, without inventing a native quota for Pi itself.
          { ...selected, driver, enabled: false },
          ...providers.filter((provider) => provider.instanceId !== instanceId),
        ],
        sources,
        now,
      ),
    ),
  ].filter((report): report is UsageLimitsReport => report !== null);
  if (reports.length === 0) return null;
  return {
    createdAt: reports[0]!.createdAt,
    accounts: reports.flatMap((report) => report.accounts),
    notices: [...new Set(reports.flatMap((report) => report.notices))],
  };
}

export interface UsageGauge {
  readonly account: Account;
  readonly window: ServerProviderUsageWindow;
}

// Pi spends its own cached subscription, not a native CLI login.
function accountsForModel(
  accounts: ReadonlyArray<Account>,
  model: string | null | undefined,
  driver: ServerProvider["driver"] | undefined,
) {
  if (driver !== "pi" && (accounts.length <= 1 || !model)) return accounts;
  const vendor = /claude|anthropic|opus|sonnet|haiku|fable/i.test(model ?? "")
    ? /claude|anthropic/i
    : /gpt|codex|openai/i.test(model ?? "")
      ? /codex|openai|gpt/i
      : null;
  if (vendor === null) return driver === "pi" ? [] : accounts;
  const matched = accounts.filter((account) =>
    driver === "pi"
      ? account.driver === "pi" && vendor.test(`${account.id} ${account.label}`)
      : vendor.test(
          account.driver === "codex" || account.driver === "claudeAgent"
            ? account.driver
            : `${account.id} ${account.label}`,
        ),
  );
  return driver === "pi" || matched.length > 0 ? matched : accounts;
}

/** The window closest to its limit among the accounts the thread's model draws on. */
export function pickUsageGauge(
  report: UsageLimitsReport | null,
  model: string | null | undefined,
  driver: ServerProvider["driver"] | undefined,
): UsageGauge | null {
  if (report === null) return null;
  let tightest: UsageGauge | null = null;
  for (const account of accountsForModel(report.accounts, model, driver)) {
    if (account.limits.unavailable) continue;
    for (const window of account.limits.windows) {
      if (tightest === null || window.usedPercent > tightest.window.usedPercent) {
        tightest = { account, window };
      }
    }
  }
  return tightest;
}
