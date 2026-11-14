import type {
  OrchestrationV2ProviderThread,
  OrchestrationV2ProviderTurnTokenUsage,
  OrchestrationV2ThreadProjection,
} from "@t3tools/contracts";

/** Only usage measurement time, never thread status time, can supersede a turn report. */
export function hasNewerThreadContextUsage(
  providerThread:
    | Pick<OrchestrationV2ProviderThread, "contextUsage" | "updatedAt">
    | null
    | undefined,
  turnUsage: OrchestrationV2ProviderTurnTokenUsage,
): boolean {
  return (
    providerThread?.contextUsage?.updatedAt !== undefined &&
    Date.parse(providerThread.contextUsage.updatedAt) > Date.parse(turnUsage.updatedAt)
  );
}

/** The context figures a composer meter needs, from whichever report is newest. */
export interface ContextUsage {
  readonly usedTokens: number;
  readonly maxTokens: number | null;
  readonly autoCompactThreshold: number | null;
}

/**
 * The thread's current context usage: compare the newest provider turn report
 * with the active provider thread's stored snapshot (for example, after compaction).
 */
export function latestContextUsage(
  projection: Pick<OrchestrationV2ThreadProjection, "providerTurns" | "providerThreads"> & {
    readonly activeProviderThreadId: OrchestrationV2ThreadProjection["thread"]["activeProviderThreadId"];
  },
): ContextUsage | null {
  let latest: (typeof projection.providerTurns)[number]["tokenUsage"];
  for (const { tokenUsage, providerThreadId } of projection.providerTurns) {
    if (providerThreadId !== projection.activeProviderThreadId) continue;
    if (
      tokenUsage !== undefined &&
      (latest === undefined || Date.parse(tokenUsage.updatedAt) >= Date.parse(latest.updatedAt))
    )
      latest = tokenUsage;
  }
  const providerThread = projection.providerThreads.find(
    (thread) => thread.id === projection.activeProviderThreadId,
  );
  if (latest !== undefined && !hasNewerThreadContextUsage(providerThread, latest))
    return {
      usedTokens: Math.max(0, latest.usedTokens),
      maxTokens: latest.maxTokens ?? null,
      autoCompactThreshold: latest.autoCompactThreshold ?? null,
    };
  const stored = providerThread?.contextUsage;
  if (!stored) return null;
  return {
    usedTokens: stored.usedTokens,
    maxTokens: stored.maxTokens ?? null,
    autoCompactThreshold: stored.autoCompactThreshold ?? null,
  };
}

/**
 * What the meter fills toward. A provider that compacts at a set size (Pi
 * compacts a 1M window at 200k) is full at that size, not at the window, so
 * the ring reaches the red zone as compaction nears instead of idling at 20%.
 */
function contextMeterBudget(usage: {
  readonly usedTokens: number;
  readonly maxTokens?: number | null | undefined;
  readonly usedPercentage: number | null;
  readonly autoCompactThreshold?: number | null | undefined;
}): { readonly limit: number | null; readonly usedPercentage: number | null } {
  const window = usage.maxTokens ?? null;
  const threshold = usage.autoCompactThreshold ?? null;
  if (threshold !== null && threshold > 0 && (window === null || threshold < window)) {
    return {
      limit: threshold,
      usedPercentage: Math.min(100, (usage.usedTokens / threshold) * 100),
    };
  }
  return { limit: window, usedPercentage: usage.usedPercentage };
}

/** {@link contextMeterBudget} for a {@link ContextUsage} that has no percentage yet. */
export function contextUsageBudget(usage: ContextUsage) {
  const usedPercentage =
    usage.maxTokens !== null && usage.maxTokens > 0
      ? Math.min(100, (usage.usedTokens / usage.maxTokens) * 100)
      : null;
  return contextMeterBudget({ ...usage, usedPercentage });
}
