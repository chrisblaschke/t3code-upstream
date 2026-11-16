import type { OrchestrationV2ThreadProjection } from "@t3tools/contracts";

/** Only eligibility is client-side; the server resolves the actual launch. */
export function canOpenPiTui(
  projection: {
    readonly thread: Pick<OrchestrationV2ThreadProjection["thread"], "activeProviderThreadId">;
    readonly providerThreads: OrchestrationV2ThreadProjection["providerThreads"];
  } | null,
): boolean {
  const providerThread = projection?.providerThreads.find(
    (candidate) => candidate.id === projection.thread.activeProviderThreadId,
  );
  return (
    providerThread?.driver === "pi" &&
    providerThread.nativeThreadRef?.nativeId?.endsWith(".jsonl") === true
  );
}
