import { type OrchestrationV2ThreadProjection, type RunId } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Stream from "effect/Stream";
import * as TestClock from "effect/testing/TestClock";
import * as SqlitePersistence from "../../persistence/Sqlite.ts";
import * as ServerSettings from "../../serverSettings.ts";
import * as CommandReceiptStore from "../CommandReceiptStore.ts";
import * as Orchestrator from "../Orchestrator.ts";
import * as ProviderSessionManager from "../ProviderSessionManager.ts";
import * as ThreadManagementService from "../ThreadManagementService.ts";

/** Run the real scoped waiter against a producer/SQL snapshot, without a future wake. */
export const waitForOwnedWork = Effect.fn("waitForOwnedWork")(function* (
  records: Pick<
    OrchestrationV2ThreadProjection,
    "thread" | "runs" | "providerThreads" | "subagents" | "messages"
  >,
  runId: RunId,
) {
  const service = yield* ThreadManagementService.ThreadManagementService.pipe(
    Effect.provide(
      ThreadManagementService.layer.pipe(
        Layer.provide(Layer.mock(ProviderSessionManager.ProviderSessionManagerV2)({})),
        Layer.provide(
          Layer.mock(Orchestrator.OrchestratorV2)({
            getThreadEventSequence: () => Effect.succeed(0),
            restartContinuationPending: () => Effect.succeed(false),
            delegatedTaskResultPending: () => Effect.die("unscoped completion check"),
            streamStoredEventsFrom: () => Stream.never,
            getThreadRecords: () => Effect.succeed(records as OrchestrationV2ThreadProjection),
          }),
        ),
        Layer.provide(ServerSettings.layerTest()),
        Layer.provide(CommandReceiptStore.layer.pipe(Layer.provide(SqlitePersistence.layerMemory))),
      ),
    ),
  );
  const fiber = yield* service
    .waitForThread({
      projectId: records.thread.projectId,
      threadId: records.thread.id,
      runId,
      untilDelegatedWorkSettled: true,
      timeoutMs: 1_000,
    })
    .pipe(Effect.forkChild);
  yield* TestClock.adjust(1_000);
  return yield* Fiber.join(fiber);
});
