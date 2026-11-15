import { EventId } from "@t3tools/contracts";
import { randomUuidV4 } from "@t3tools/provider-core/server/randomUuid";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Stream from "effect/Stream";

import * as GitWorkflowService from "../git/GitWorkflowService.ts";
import { forkParked } from "../serverActivation.ts";
import * as VcsStatusBroadcaster from "../vcs/VcsStatusBroadcaster.ts";
import * as EventSink from "./EventSink.ts";
import * as ProjectionStore from "./ProjectionStore.ts";
import * as ThreadCommandExecutor from "./ThreadCommandExecutor.ts";
import * as ThreadManagement from "./ThreadManagementService.ts";

export const makeReconcile = Effect.gen(function* () {
  const threads = yield* ThreadManagement.ThreadManagementService;
  const eventSink = yield* EventSink.EventSinkV2;
  const commands = yield* ThreadCommandExecutor.ThreadCommandExecutor;
  const git = yield* GitWorkflowService.GitWorkflowService;
  const projections = yield* ProjectionStore.ProjectionStoreV2;
  const fs = yield* FileSystem.FileSystem;
  return Effect.fn("WorktreeBranchSync.reconcile")(function* (input: {
    readonly cwd: string;
    readonly refName: string | null;
  }) {
    if (input.refName === null) return;
    const paths = yield* projections.getThreadWorktreePaths({ includeArchived: true });
    const matchingPaths = new Set<string>();
    for (const worktreePath of new Set(paths.map((thread) => thread.worktreePath))) {
      const realPath =
        worktreePath === input.cwd
          ? worktreePath
          : yield* fs.realPath(worktreePath).pipe(Effect.orElseSucceed(() => null));
      if (realPath === input.cwd) matchingPaths.add(worktreePath);
    }
    const matchingThreads = new Map(
      paths
        .filter((thread) => matchingPaths.has(thread.worktreePath))
        .map((thread) => [thread.threadId, thread.worktreePath]),
    );
    if (matchingThreads.size === 0) return;
    const snapshot = yield* threads.getShellSnapshot({ threadIds: [...matchingThreads.keys()] });
    const sharing = [...snapshot.threads, ...snapshot.archivedThreads].filter(
      (thread) =>
        thread.worktreePath === matchingThreads.get(thread.id) && thread.branch !== input.refName,
    );
    if (sharing.length === 0) return;
    // Status events may queue behind a generated rename or another checkout.
    if ((yield* git.currentBranch(input.cwd)) !== input.refName) return;
    for (const thread of sharing) {
      yield* eventSink
        .write({
          events: [
            {
              id: EventId.make(yield* randomUuidV4),
              type: "thread.branch-reconciled",
              threadId: thread.id,
              occurredAt: yield* DateTime.now,
              payload: {
                branch: input.refName,
                expectedBranch: thread.branch,
                worktreePath: thread.worktreePath!,
              },
            },
          ],
        })
        .pipe(
          (write) => commands.withLock(thread.id, write),
          Effect.catchTags({
            EventSinkWriteError: (cause) =>
              Effect.logWarning("Worktree branch update was rejected", {
                threadId: thread.id,
                cause,
              }),
          }),
        );
    }
  });
});

export const layer = Layer.effectDiscard(
  Effect.gen(function* () {
    const broadcaster = yield* VcsStatusBroadcaster.VcsStatusBroadcaster;
    const reconcile = yield* makeReconcile;
    yield* forkParked(
      Stream.runForEach(broadcaster.streamBranchChanges, (input) =>
        reconcile(input).pipe(
          Effect.catchCause((cause) =>
            Effect.logWarning("Failed to reconcile worktree branch", { cwd: input.cwd, cause }),
          ),
        ),
      ),
    );
  }),
);
