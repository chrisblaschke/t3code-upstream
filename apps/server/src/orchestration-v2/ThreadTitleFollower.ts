/**
 * Re-summarize titles after early turns, then at most every REFRESH_INTERVAL_MS.
 * Keep accurate titles unchanged to avoid churn.
 */

import { CommandId, type ThreadId } from "@t3tools/contracts";
import * as Clock from "effect/Clock";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Stream from "effect/Stream";

import * as Orchestrator from "./Orchestrator.ts";

const EARLY_TURNS = 3;
const REFRESH_INTERVAL_MS = 20 * 60 * 1000;
/** Runs PiSessionSync writes for CLI prompts; it reports their turn ends itself. */
const PI_CLI_RUN_PREFIX = "run:pi-cli:";

export interface TitleFollowState {
  readonly refreshes: number;
  readonly lastAtMs: number;
  readonly lastKey: string;
}

/** Whether a turn that just ended (identified by `key`) should retitle its thread. */
export function titleRefreshDue(
  state: TitleFollowState | undefined,
  key: string,
  nowMs: number,
): boolean {
  if (state === undefined) return true;
  if (state.lastKey === key) return false;
  return state.refreshes < EARLY_TURNS || nowMs - state.lastAtMs >= REFRESH_INTERVAL_MS;
}

/**
 * In memory: after a restart each active thread gets a few fresh summaries
 * again, which doubles as a lazy backfill of titles made before this existed.
 */
export const makeThreadTitleFollower = Effect.gen(function* () {
  const orchestrator = yield* Orchestrator.OrchestratorV2;
  const followed = new Map<ThreadId, TitleFollowState>();

  const turnEnded = Effect.fn("ThreadTitleFollower.turnEnded")(
    function* (threadId: ThreadId, key: string) {
      const nowMs = yield* Clock.currentTimeMillis;
      const state = followed.get(threadId);
      if (!titleRefreshDue(state, key, nowMs)) return;
      const { thread } = yield* orchestrator.getThreadRecords(threadId, []);
      if (
        thread.lineage.parentThreadId !== null ||
        thread.archivedAt !== null ||
        thread.deletedAt !== null ||
        thread.titleRegeneration != null
      ) {
        return;
      }
      followed.set(threadId, {
        refreshes: (state?.refreshes ?? 0) + 1,
        lastAtMs: nowMs,
        lastKey: key,
      });
      yield* orchestrator.dispatch({
        type: "thread.metadata.update",
        commandId: CommandId.make(`title-follow:${threadId}:${key}`),
        threadId,
        regenerateTitle: true,
      });
    },
    (effect, threadId) =>
      effect.pipe(
        Effect.catchCause((cause) =>
          Effect.logWarning("Thread title refresh failed", { threadId, cause }),
        ),
      ),
  );

  return { turnEnded };
});

/** Retitles T3's own threads as their runs complete. */
export const layer = Layer.effectDiscard(
  Effect.gen(function* () {
    const orchestrator = yield* Orchestrator.OrchestratorV2;
    const { turnEnded } = yield* makeThreadTitleFollower;
    yield* Stream.runForEach(orchestrator.streamDomainEvents, (event) =>
      event.type === "run.updated" &&
      event.payload.status === "completed" &&
      !event.payload.id.startsWith(PI_CLI_RUN_PREFIX)
        ? turnEnded(event.threadId, event.payload.id)
        : Effect.void,
    ).pipe(
      Effect.catchCause((cause) => Effect.logWarning("Thread title follower stopped", { cause })),
      Effect.forkScoped,
    );
  }),
);
