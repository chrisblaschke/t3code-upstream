import { assert, it } from "@effect/vitest";
import {
  CommandId,
  EventId,
  ProjectId,
  ProviderDriverKind,
  ProviderInstanceId,
  ProviderThreadId,
  ThreadId,
} from "@t3tools/contracts";
import type * as ProviderAdapter from "@t3tools/provider-core/server/ProviderAdapter";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Stream from "effect/Stream";
import { vi } from "vite-plus/test";

import * as GitWorkflowService from "../git/GitWorkflowService.ts";
import * as SqlitePersistence from "../persistence/Sqlite.ts";
import * as VcsStatusBroadcaster from "../vcs/VcsStatusBroadcaster.ts";
import { CodexProviderCapabilitiesV2 } from "./Adapters/CodexAdapterV2.ts";
import * as EventSink from "./EventSink.ts";
import * as Orchestrator from "./Orchestrator.ts";
import * as ProjectionStore from "./ProjectionStore.ts";
import * as ProviderAdapterRegistry from "./ProviderAdapterRegistry.ts";
import * as ProviderReplayHarness from "./testkit/ProviderReplayHarness.ts";
import * as ThreadManagement from "./ThreadManagementService.ts";
import * as ThreadCommandExecutor from "./ThreadCommandExecutor.ts";
import * as WorktreeBranchSync from "./WorktreeBranchSync.ts";

const instanceId = ProviderInstanceId.make("codex");
const adapter = {
  instanceId,
  driver: ProviderDriverKind.make("codex"),
  getCapabilities: () => Effect.succeed(CodexProviderCapabilitiesV2),
  planSelectionTransition: () => Effect.succeed({ type: "apply_on_next_turn" as const }),
  openSession: () => Effect.die("No provider process needed for branch reconciliation"),
} as ProviderAdapter.ProviderAdapterV2["Service"];
const database = SqlitePersistence.layerMemory;
const layerTest = Layer.mergeAll(
  ThreadCommandExecutor.layer,
  ProjectionStore.layer.pipe(Layer.provide(database)),
  ProviderReplayHarness.layerWithRegistry(
    { name: "worktree-branch-race" },
    ProviderAdapterRegistry.layerFromAdapters([adapter]),
    { databaseLayer: database, runEffectWorker: false },
  ),
);

it.effect("retains the reconciled branch when a title command was already planned", () =>
  Effect.gen(function* () {
    const orchestrator = yield* Orchestrator.OrchestratorV2;
    const projections = yield* ProjectionStore.ProjectionStoreV2;
    const sink = yield* EventSink.EventSinkV2;
    const commands = yield* ThreadCommandExecutor.ThreadCommandExecutor;
    const threadId = ThreadId.make("thread:command-race");
    yield* orchestrator.dispatch({
      type: "thread.create",
      commandId: CommandId.make("create-command-race"),
      threadId,
      projectId: ProjectId.make("project:command-race"),
      title: "Before",
      modelSelection: { instanceId, model: "fixture" },
      runtimeMode: "full-access",
      interactionMode: "default",
      branch: "t3/temporary",
      worktreePath: "/git-worktree",
      createdBy: "user",
      creationSource: "web",
    });
    const planned = yield* Deferred.make<void>();
    const allowCommit = yield* Deferred.make<void>();
    const watcherAtLock = yield* Deferred.make<void>();
    const reconciled = yield* Deferred.make<void>();
    const commit = sink.commitCommand;
    using _commitSpy = vi
      .spyOn(sink, "commitCommand")
      .mockImplementation((input) =>
        input.commandId === CommandId.make("rename-title")
          ? Deferred.succeed(planned, undefined).pipe(
              Effect.andThen(Deferred.await(allowCommit)),
              Effect.andThen(commit(input)),
            )
          : commit(input),
      );
    const titleJob = yield* orchestrator
      .dispatch({
        type: "thread.metadata.update",
        commandId: CommandId.make("rename-title"),
        threadId,
        title: "After",
      })
      .pipe(Effect.forkChild);
    yield* Deferred.await(planned);
    const write = sink.write;
    using _writeSpy = vi
      .spyOn(sink, "write")
      .mockImplementation((input) =>
        write(input).pipe(Effect.tap(() => Deferred.succeed(watcherAtLock, undefined))),
      );
    const withLock = commands.withLock;
    const observeLock: ThreadCommandExecutor.ThreadCommandExecutor["Service"]["withLock"] = (
      key,
      effect,
    ) => Deferred.succeed(watcherAtLock, undefined).pipe(Effect.andThen(withLock(key, effect)));
    using _lockSpy = vi.spyOn(commands, "withLock").mockImplementation(observeLock);
    yield* Layer.build(WorktreeBranchSync.layer).pipe(
      Effect.provide(
        Layer.mergeAll(
          FileSystem.layerNoop({}),
          Layer.mock(GitWorkflowService.GitWorkflowService)({
            currentBranch: () => Effect.succeed("renamed"),
          }),
          Layer.mock(VcsStatusBroadcaster.VcsStatusBroadcaster)({
            streamBranchChanges: Stream.make({ cwd: "/git-worktree", refName: "renamed" }).pipe(
              Stream.ensuring(Deferred.succeed(reconciled, undefined)),
            ),
          }),
          Layer.mock(ThreadManagement.ThreadManagementService)({
            getShellSnapshot: orchestrator.getShellSnapshot,
            dispatch: orchestrator.dispatch,
          }),
        ),
      ),
    );
    yield* Deferred.await(watcherAtLock);
    yield* Deferred.succeed(allowCommit, undefined);
    yield* Fiber.join(titleJob);
    yield* Deferred.await(reconciled);
    const result = yield* projections.getThread(threadId);
    assert.equal(result.title, "After");
    assert.equal(result.branch, "renamed");
  }).pipe(Effect.scoped, Effect.provide(layerTest)),
);

it.effect("preserves a provider attachment committed after branch planning", () =>
  Effect.gen(function* () {
    const orchestrator = yield* Orchestrator.OrchestratorV2;
    const projections = yield* ProjectionStore.ProjectionStoreV2;
    const sink = yield* EventSink.EventSinkV2;
    const threadId = ThreadId.make("thread:branch-race");
    const providerId = ProviderThreadId.make("provider:concurrent-attachment");
    const worktreePath = "/git-worktree";
    yield* orchestrator.dispatch({
      type: "thread.create",
      commandId: CommandId.make("create-branch-race"),
      threadId,
      projectId: ProjectId.make("project:branch-race"),
      title: "Branch race",
      modelSelection: { instanceId, model: "fixture" },
      runtimeMode: "full-access",
      interactionMode: "default",
      branch: "t3/temporary",
      worktreePath,
      createdBy: "user",
      creationSource: "web",
    });
    const write = sink.write;
    let injected = false;
    const attach = Effect.gen(function* () {
      if (injected) return;
      injected = true;
      const now = yield* DateTime.now;
      yield* write({
        events: [
          {
            id: EventId.make("attach-during-branch-write"),
            type: "provider-thread.updated",
            threadId,
            occurredAt: now,
            payload: {
              id: providerId,
              driver: adapter.driver,
              providerInstanceId: instanceId,
              providerSessionId: null,
              appThreadId: threadId,
              ownerNodeId: null,
              nativeThreadRef: null,
              nativeConversationHeadRef: null,
              status: "idle",
              firstRunOrdinal: null,
              lastRunOrdinal: null,
              handoffIds: [],
              forkedFrom: null,
              createdAt: now,
              updatedAt: now,
            },
          },
        ],
      });
      assert.equal((yield* projections.getThread(threadId)).activeProviderThreadId, providerId);
    }).pipe(Effect.orDie);
    const commitCommand = sink.commitCommand;
    // Provider ingestion writes outside the command lock, between planning and commit.
    using _commitSpy = vi
      .spyOn(sink, "commitCommand")
      .mockImplementation((input) => attach.pipe(Effect.andThen(commitCommand(input))));
    using _writeSpy = vi
      .spyOn(sink, "write")
      .mockImplementation((input) => attach.pipe(Effect.andThen(write(input))));
    const reconciled = yield* Deferred.make<void>();
    yield* Layer.build(WorktreeBranchSync.layer).pipe(
      Effect.provide(
        Layer.mergeAll(
          FileSystem.layerNoop({}),
          Layer.mock(GitWorkflowService.GitWorkflowService)({
            currentBranch: () => Effect.succeed("renamed"),
          }),
          Layer.mock(VcsStatusBroadcaster.VcsStatusBroadcaster)({
            streamBranchChanges: Stream.make({ cwd: worktreePath, refName: "renamed" }).pipe(
              Stream.ensuring(Deferred.succeed(reconciled, undefined)),
            ),
          }),
          Layer.mock(ThreadManagement.ThreadManagementService)({
            getShellSnapshot: orchestrator.getShellSnapshot,
            dispatch: orchestrator.dispatch,
          }),
        ),
      ),
    );
    yield* Deferred.await(reconciled);
    const result = yield* projections.getThread(threadId);
    assert.equal(injected, true);
    assert.equal(result.branch, "renamed");
    assert.equal(result.activeProviderThreadId, providerId);
  }).pipe(Effect.scoped, Effect.provide(layerTest)),
);
