import { expect, it } from "@effect/vitest";
import {
  CommandId,
  EnvironmentId,
  MessageId,
  NodeId,
  type OrchestrationV2Command,
  type OrchestrationV2ServerCommand,
  type OrchestrationV2Run,
  type OrchestrationV2ProviderThread,
  ProviderThreadId,
  ProviderDriverKind,
  ProviderSessionId,
  type OrchestrationV2StoredEvent,
  type OrchestrationV2ThreadProjection,
  ProjectId,
  ProviderInstanceId,
  RunId,
  ThreadId,
} from "@t3tools/contracts";
import * as Deferred from "effect/Deferred";
import * as DateTime from "effect/DateTime";
import * as Duration from "effect/Duration";
import * as Option from "effect/Option";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Queue from "effect/Queue";
import * as Stream from "effect/Stream";
import * as TestClock from "effect/testing/TestClock";

import { AgentScope } from "@t3tools/shared/AgentScope";

import * as ServerSettings from "../serverSettings.ts";
import * as SqlitePersistence from "../persistence/Sqlite.ts";
import * as CommandReceiptStore from "./CommandReceiptStore.ts";
import * as Orchestrator from "./Orchestrator.ts";
import * as ProviderSessionManager from "./ProviderSessionManager.ts";
import * as ProviderAdapter from "@t3tools/provider-core/server/ProviderAdapter";
import * as ThreadManagementService from "./ThreadManagementService.ts";
import * as ThreadCommandExecutor from "./ThreadCommandExecutor.ts";

it("stamps authoritative provenance on commands that create threads or messages", () => {
  const command: OrchestrationV2Command = {
    type: "thread.create",
    createdBy: "agent",
    creationSource: "mcp",
    commandId: CommandId.make("command:thread-management:create"),
    threadId: ThreadId.make("thread:thread-management:create"),
    projectId: ProjectId.make("project:thread-management"),
    title: "Thread management",
    modelSelection: {
      instanceId: ProviderInstanceId.make("codex"),
      model: "gpt-5-codex",
    },
    runtimeMode: "full-access",
    interactionMode: "default",
    branch: null,
    worktreePath: null,
    // A client cannot claim a linked environment's origin, or a parent there.
    linkOrigin: { sessionId: "forged", label: "Somewhere else" },
    delegatedFrom: {
      environmentId: EnvironmentId.make("environment:elsewhere"),
      threadId: ThreadId.make("thread:elsewhere"),
      title: "Somewhere else",
    },
  };

  const stamped = ThreadManagementService.withCreationProvenance(command, {
    createdBy: "user",
    creationSource: "web",
  });
  expect(stamped).toMatchObject({
    createdBy: "user",
    creationSource: "web",
  });
  expect(stamped).not.toHaveProperty("linkOrigin");
  expect(stamped).not.toHaveProperty("delegatedFrom");
});

const layerReceiptsTest = CommandReceiptStore.layer.pipe(
  Layer.provide(SqlitePersistence.layerMemory),
);

const layerDispatchChokepointTest =
  (settingsLayer: ReturnType<typeof ServerSettings.layerTest>) => (dispatched: Array<string>) =>
    ThreadManagementService.layer.pipe(
      Layer.provide(Layer.mock(ProviderSessionManager.ProviderSessionManagerV2)({})),
      Layer.provide(
        Layer.mock(Orchestrator.OrchestratorV2)({
          getThreadRecords: () =>
            Effect.succeed({
              thread: {
                id: ThreadId.make("thread:thread-management:chokepoint"),
                projectId: ProjectId.make("project:thread-management:chokepoint"),
                deletedAt: null,
                modelSelection: {
                  instanceId: ProviderInstanceId.make("codex_work"),
                  model: "test",
                },
              },
            } as unknown as OrchestrationV2ThreadProjection),
          dispatch: (command) =>
            Effect.sync(() => {
              dispatched.push(command.type);
              return { sequence: 1, storedEvents: [] } as never;
            }),
        }),
      ),
      Layer.provide(settingsLayer),
      Layer.provideMerge(layerReceiptsTest),
    );

it.effect(
  "the dispatch chokepoint refuses a continuation message.dispatch on a project-disabled instance before reaching the orchestrator",
  () => {
    const projectId = ProjectId.make("project:thread-management:chokepoint");
    const threadId = ThreadId.make("thread:thread-management:chokepoint");
    const instanceId = ProviderInstanceId.make("codex_work");
    const commandId = CommandId.make("command:thread-management:chokepoint-disabled");
    const dispatched: Array<string> = [];
    const layerTest = layerDispatchChokepointTest(
      ServerSettings.layerTest({
        providerInstances: { [instanceId]: { driver: "codex", enabled: true } },
        projectSettingsOverrides: {
          [projectId]: { providerInstances: { [instanceId]: { enabled: false } } },
        },
      }),
    )(dispatched);

    return Effect.gen(function* () {
      const threads = yield* ThreadManagementService.ThreadManagementService;
      const error = yield* Effect.flip(
        threads.dispatch({
          type: "message.dispatch",
          commandId,
          threadId,
          messageId: MessageId.make("message:thread-management:chokepoint-disabled"),
          text: "Background task completed.",
          attachments: [],
          dispatchMode: { type: "queue_after_active" },
          createdBy: "agent",
          creationSource: "server",
        }),
      );

      expect(error).toBeInstanceOf(
        ThreadManagementService.ThreadManagementProviderInstanceDisabledError,
      );
      expect(error).toMatchObject({ projectId, instanceId });
      expect(dispatched).toEqual([]);
    }).pipe(Effect.provide(layerTest));
  },
);

it.effect(
  "a replay of an already-accepted command id dispatches despite the instance now being disabled",
  () => {
    const projectId = ProjectId.make("project:thread-management:chokepoint");
    const threadId = ThreadId.make("thread:thread-management:chokepoint");
    const instanceId = ProviderInstanceId.make("codex_work");
    const commandId = CommandId.make("command:thread-management:chokepoint-replay");
    const dispatched: Array<string> = [];
    const layerTest = layerDispatchChokepointTest(
      ServerSettings.layerTest({
        providerInstances: { [instanceId]: { driver: "codex", enabled: true } },
        projectSettingsOverrides: {
          [projectId]: { providerInstances: { [instanceId]: { enabled: false } } },
        },
      }),
    )(dispatched);

    return Effect.gen(function* () {
      const receipts = yield* CommandReceiptStore.CommandReceiptStoreV2;
      yield* receipts.insertIfAbsent({
        commandId,
        threadId,
        commandType: "message.dispatch",
        acceptedAt: yield* DateTime.now,
        resultSequence: 1,
        status: "accepted",
        error: null,
      });

      const threads = yield* ThreadManagementService.ThreadManagementService;
      yield* threads.dispatch({
        type: "message.dispatch",
        commandId,
        threadId,
        messageId: MessageId.make("message:thread-management:chokepoint-replay"),
        text: "Background task completed.",
        attachments: [],
        dispatchMode: { type: "queue_after_active" },
        createdBy: "agent",
        creationSource: "server",
      });

      expect(dispatched).toEqual(["message.dispatch"]);
    }).pipe(Effect.provide(layerTest));
  },
);

it.effect("waitForThread follows a restart continuation instead of the cut run", () =>
  Effect.gen(function* () {
    const projectId = ProjectId.make("project:thread-management:wait-restart");
    const threadId = ThreadId.make("thread:thread-management:wait-restart");
    const runId = RunId.make("run:thread-management:wait-restart");
    const continuationId = RunId.make("run:thread-management:wait-restart:2");
    const subscribed = yield* Deferred.make<void>();
    const events = yield* Queue.unbounded<OrchestrationV2StoredEvent>();
    let runs: Array<{
      id: RunId;
      status: OrchestrationV2Run["status"];
      restartContinuationOfRunId?: RunId;
    }> = [{ id: runId, status: "cancelled" }];
    let pending = true;
    const stored = (sequence: number, event: object) =>
      ({ sequence, event: { threadId, ...event } }) as unknown as OrchestrationV2StoredEvent;
    const layerTest = ThreadManagementService.layer.pipe(
      Layer.provide(Layer.mock(ProviderSessionManager.ProviderSessionManagerV2)({})),
      Layer.provide(
        Layer.mock(Orchestrator.OrchestratorV2)({
          getThreadEventSequence: () => Effect.succeed(0),
          restartContinuationPending: (run) => Effect.succeed(pending && run.id === runId),
          awaitRestartContinuation: () => Effect.never,
          streamStoredEventsFrom: (input) =>
            input?.eventType === "run.updated"
              ? Stream.fromEffect(Deferred.succeed(subscribed, undefined)).pipe(
                  Stream.drain,
                  Stream.concat(Stream.fromQueue(events)),
                )
              : Stream.never,
          getThreadRecords: () =>
            Effect.sync(
              () =>
                ({
                  thread: { id: threadId, projectId, deletedAt: null },
                  runs,
                }) as unknown as OrchestrationV2ThreadProjection,
            ),
        }),
      ),
      Layer.provide(ServerSettings.layerTest()),
      Layer.provide(layerReceiptsTest),
    );
    const service = yield* ThreadManagementService.ThreadManagementService.pipe(
      Effect.provide(layerTest),
    );
    const fiber = yield* service
      .waitForThread({ projectId, threadId, runId, timeoutMs: 60 * 60 * 1_000 })
      .pipe(Effect.forkChild);
    yield* Deferred.await(subscribed);
    const continuation = { id: continuationId, restartContinuationOfRunId: runId };
    runs = [...runs, { ...continuation, status: "running" }];
    pending = false;
    yield* Queue.offer(
      events,
      stored(1, { type: "run.updated", payload: { ...continuation, status: "running" } }),
    );
    runs = [runs[0]!, { ...continuation, status: "completed" }];
    yield* Queue.offer(
      events,
      stored(2, { type: "run.updated", payload: { ...continuation, status: "completed" } }),
    );
    const result = yield* Fiber.join(fiber);

    expect(result).toMatchObject({
      timedOut: false,
      run: { id: continuationId, status: "completed" },
    });
  }),
);

it.effect("waitForThread can wait past an ended run for the thread's delegated work", () =>
  Effect.gen(function* () {
    const projectId = ProjectId.make("project:thread-management:wait-delegated");
    const threadId = ThreadId.make("thread:thread-management:wait-delegated");
    const runId = RunId.make("run:thread-management:wait-delegated");
    const wakeRunId = RunId.make("run:thread-management:wait-delegated:wake");
    const subscribed = yield* Deferred.make<void>();
    const events = yield* Queue.unbounded<OrchestrationV2StoredEvent>();
    const at = (seconds: number) => DateTime.makeUnsafe(seconds * 1_000);
    const run = (id: RunId, ordinal: number, status: OrchestrationV2Run["status"], end: number) =>
      ({
        id,
        ordinal,
        status,
        startedAt: at(end - 1),
        completedAt: status === "running" ? null : at(end),
      }) as unknown as OrchestrationV2Run;
    // The run ended, but a task it delegated still runs and will wake the thread.
    let runs = [run(runId, 1, "completed", 1)];
    let subagents: OrchestrationV2ThreadProjection["subagents"] = [
      {
        id: NodeId.make("task:wait-delegated"),
        runId,
        status: "running",
      } as OrchestrationV2ThreadProjection["subagents"][number],
    ];
    const stored = (sequence: number, event: object) =>
      ({ sequence, event: { threadId, ...event } }) as unknown as OrchestrationV2StoredEvent;
    const layerTest = ThreadManagementService.layer.pipe(
      Layer.provide(Layer.mock(ProviderSessionManager.ProviderSessionManagerV2)({})),
      Layer.provide(
        Layer.mock(Orchestrator.OrchestratorV2)({
          getThreadEventSequence: () => Effect.succeed(0),
          restartContinuationPending: () => Effect.succeed(false),
          delegatedTaskResultPending: () => Effect.succeed(false),
          streamStoredEventsFrom: (input) =>
            input?.eventType === "subagent.updated"
              ? Stream.fromEffect(Deferred.succeed(subscribed, undefined)).pipe(
                  Stream.drain,
                  Stream.concat(Stream.fromQueue(events)),
                )
              : Stream.never,
          getThreadRecords: () =>
            Effect.sync(
              () =>
                ({
                  thread: { id: threadId, projectId, deletedAt: null },
                  runs,
                  messages: [],
                  subagents,
                  providerThreads: [],
                }) as unknown as OrchestrationV2ThreadProjection,
            ),
        }),
      ),
      Layer.provide(ServerSettings.layerTest()),
      Layer.provide(layerReceiptsTest),
    );
    const service = yield* ThreadManagementService.ThreadManagementService.pipe(
      Effect.provide(layerTest),
    );
    // Without the option, the ended run is the answer, as before.
    expect(
      yield* service.waitForThread({ projectId, threadId, runId, timeoutMs: 60 * 60 * 1_000 }),
    ).toMatchObject({ timedOut: false, run: { id: runId } });
    expect(
      yield* service.waitForThread({ projectId, threadId, runId, timeoutMs: 60 * 60 * 1_000 }),
    ).not.toHaveProperty("resultRun");

    const fiber = yield* service
      .waitForThread({
        projectId,
        threadId,
        runId,
        timeoutMs: 60 * 60 * 1_000,
        untilDelegatedWorkSettled: true,
      })
      .pipe(Effect.forkChild);
    // A wait that returns early fails below instead of hanging here.
    yield* Effect.raceFirst(Deferred.await(subscribed), Fiber.await(fiber));
    // The task finishes; its delivery wakes the thread, whose next run answers.
    subagents = [{ ...subagents[0]!, status: "completed" }];
    runs = [...runs, { ...run(wakeRunId, 2, "completed", 3), workStartedAt: runs[0]!.startedAt! }];
    yield* Queue.offer(events, stored(1, { type: "subagent.updated", payload: {} }));
    const result = yield* Fiber.join(fiber);

    expect(result).toMatchObject({
      timedOut: false,
      run: { id: runId, status: "completed" },
      resultRun: { id: wakeRunId, status: "completed" },
    });
  }),
);

it.effect("waitForThread settles delegated work on a thread rolled back past its only run", () =>
  Effect.gen(function* () {
    const projectId = ProjectId.make("project:thread-management:wait-rolled-back");
    const threadId = ThreadId.make("thread:thread-management:wait-rolled-back");
    const runId = RunId.make("run:thread-management:wait-rolled-back");
    const rolledBack = {
      id: runId,
      ordinal: 1,
      status: "rolled_back",
      startedAt: DateTime.makeUnsafe(0),
      completedAt: DateTime.makeUnsafe(1_000),
    } as unknown as OrchestrationV2Run;
    const layerTest = ThreadManagementService.layer.pipe(
      Layer.provide(Layer.mock(ProviderSessionManager.ProviderSessionManagerV2)({})),
      Layer.provide(
        Layer.mock(Orchestrator.OrchestratorV2)({
          getThreadEventSequence: () => Effect.succeed(0),
          restartContinuationPending: () => Effect.succeed(false),
          delegatedTaskResultPending: () => Effect.succeed(false),
          streamStoredEventsFrom: () => Stream.never,
          getThreadRecords: () =>
            Effect.succeed({
              thread: { id: threadId, projectId, deletedAt: null },
              runs: [rolledBack],
              messages: [],
              subagents: [],
              providerThreads: [],
            } as unknown as OrchestrationV2ThreadProjection),
        }),
      ),
      Layer.provide(ServerSettings.layerTest()),
      Layer.provide(layerReceiptsTest),
    );
    const service = yield* ThreadManagementService.ThreadManagementService.pipe(
      Effect.provide(layerTest),
    );
    const fiber = yield* service
      .waitForThread({
        projectId,
        threadId,
        runId,
        timeoutMs: 60_000,
        untilDelegatedWorkSettled: true,
      })
      .pipe(Effect.forkChild);
    // Nothing will ever run there again, so a wait still open runs out here.
    yield* TestClock.adjust(Duration.millis(60_000));
    const result = yield* Fiber.join(fiber);

    expect(result).toMatchObject({
      timedOut: false,
      run: { id: runId, status: "rolled_back" },
      resultRun: { id: runId, status: "rolled_back" },
    });
  }),
);

const waitOwnershipProjectId = ProjectId.make("project:wait-ownership");
const waitOwnershipThreadId = ThreadId.make("thread:wait-ownership");
const ownershipRun = (
  id: string,
  ordinal: number,
  extra: Partial<OrchestrationV2Run> = {},
): OrchestrationV2Run =>
  ({
    id: RunId.make(id),
    userMessageId: MessageId.make(`message:${id}`),
    ordinal,
    status: "completed",
    startedAt: DateTime.makeUnsafe(ordinal * 1_000),
    completedAt: DateTime.makeUnsafe((ordinal + 1) * 1_000),
    ...extra,
  }) as OrchestrationV2Run;

const waitOwnershipLayer = (mock: Partial<Orchestrator.OrchestratorV2["Service"]>) =>
  ThreadManagementService.layer.pipe(
    Layer.provide(Layer.mock(ProviderSessionManager.ProviderSessionManagerV2)({})),
    Layer.provide(
      Layer.mock(Orchestrator.OrchestratorV2)({
        getThreadEventSequence: () => Effect.succeed(0),
        restartContinuationPending: () => Effect.succeed(false),
        delegatedTaskResultPending: () => Effect.succeed(false),
        ...mock,
      }),
    ),
    Layer.provide(ServerSettings.layerTest()),
    Layer.provide(layerReceiptsTest),
  );

const ownershipRecords = (
  runs: ReadonlyArray<OrchestrationV2Run>,
  extra: {
    messages?: ReadonlyArray<Partial<OrchestrationV2ThreadProjection["messages"][number]>>;
    subagents?: ReadonlyArray<Partial<OrchestrationV2ThreadProjection["subagents"][number]>>;
    providerThreads?: ReadonlyArray<Partial<OrchestrationV2ProviderThread>>;
  } = {},
) =>
  ({
    thread: { id: waitOwnershipThreadId, projectId: waitOwnershipProjectId, deletedAt: null },
    runs,
    subagents: [],
    providerThreads: [],
    ...extra,
    messages: (extra.messages ?? []).map((message) => ({ role: "user", ...message })),
  }) as unknown as OrchestrationV2ThreadProjection;

const waitForOwnedRecords = (records: OrchestrationV2ThreadProjection) =>
  Effect.gen(function* () {
    const service = yield* ThreadManagementService.ThreadManagementService.pipe(
      Effect.provide(
        waitOwnershipLayer({
          getThreadRecords: (_threadId, fields, filter) =>
            Effect.succeed({
              ...records,
              messages: fields.some((field) => field === "messages")
                ? records.messages.filter(
                    (message) =>
                      (filter?.messageRoles === undefined ||
                        filter.messageRoles.includes(message.role)) &&
                      (filter?.monitorNotificationsOnly !== true ||
                        message.notification?.source.kind === "monitor"),
                  )
                : [],
            }),
          // An unrelated cut must not block the selected task either.
          restartContinuationPending: (run) => Effect.succeed(run.status === "interrupted"),
          // The old unscoped second gate must not be consulted.
          delegatedTaskResultPending: () => Effect.die("thread-wide completion check"),
          streamStoredEventsFrom: () => Stream.never,
        }),
      ),
    );
    const fiber = yield* service
      .waitForThread({
        projectId: waitOwnershipProjectId,
        threadId: waitOwnershipThreadId,
        runId: records.runs[0]!.id,
        untilDelegatedWorkSettled: true,
        timeoutMs: 1_000,
      })
      .pipe(Effect.forkChild);
    yield* TestClock.adjust(Duration.millis(1_000));
    return yield* Fiber.join(fiber);
  });

it.effect.each(["running", "queued", "interrupted", "completed"] as const)(
  "delegated wait ignores an unrelated %s run and its children",
  (status) =>
    Effect.gen(function* () {
      const selected = ownershipRun("run:owned", 1);
      const unrelated = ownershipRun("run:unrelated", 2, {
        status,
        userMessageId: MessageId.make("message:background"),
        ...(status === "queued" ? { startedAt: null } : { workStartedAt: selected.completedAt! }),
      });
      const records = ownershipRecords([selected, unrelated], {
        messages: [
          {
            id: unrelated.userMessageId,
            runId: unrelated.id,
            notification: {
              originRunIds: [unrelated.id],
              source: { kind: "background_task" },
              outcome: "completed",
              summary: "Other work",
            },
          },
        ],
        subagents: [{ id: NodeId.make("task:unrelated"), runId: unrelated.id, status: "running" }],
      });
      const result = yield* waitForOwnedRecords(records);
      expect(result.timedOut).toBe(false);
      expect(result.resultRun?.id).toBe(selected.id);
    }),
);

it.effect.each(["later-run", "owned", "legacy"] as const)(
  "opaque background work retains its launching run: %s",
  (owner) =>
    Effect.gen(function* () {
      const providerThreadId = ProviderThreadId.make("provider:reused");
      const selected = ownershipRun("run:opaque-a", 1, { providerThreadId });
      const later = ownershipRun("run:opaque-b", 2, { providerThreadId });
      const result = yield* waitForOwnedRecords(
        ownershipRecords([selected, later], {
          providerThreads: [
            {
              id: providerThreadId,
              pendingBackgroundTasks: [
                {
                  taskId: "opaque-task",
                  kind: "background_task",
                  ...(owner === "legacy"
                    ? {}
                    : { originRunId: owner === "owned" ? selected.id : later.id }),
                },
              ],
            },
          ],
        }),
      );
      expect(result.timedOut).toBe(owner !== "later-run");
      if (!result.timedOut) expect(result.resultRun?.id).toBe(selected.id);
    }),
);

it.effect("owned opaque work waits through an interleaved wake and returns its answer", () =>
  Effect.gen(function* () {
    const selected = ownershipRun("run:opaque-a", 1);
    const later = ownershipRun("run:opaque-b", 2);
    const wake = ownershipRun("run:opaque-wake", 3, { status: "queued", startedAt: null });
    const message = {
      id: wake.userMessageId,
      runId: wake.id,
      notification: {
        originRunIds: [selected.id],
        source: { kind: "background_task" as const },
        outcome: "completed" as const,
        summary: "Opaque work finished",
      },
    };
    const queued = yield* waitForOwnedRecords(
      ownershipRecords([selected, later, wake], { messages: [message] }),
    );
    expect(queued.timedOut).toBe(true);
    const completed = yield* waitForOwnedRecords(
      ownershipRecords(
        [selected, later, { ...wake, status: "completed", startedAt: later.completedAt }],
        { messages: [message] },
      ),
    );
    expect(completed.timedOut).toBe(false);
    expect(completed.resultRun?.id).toBe(wake.id);
  }),
);

it.effect("a child completion queued before its run still belongs to A, not interleaved B", () =>
  Effect.gen(function* () {
    const taskId = NodeId.make("task:queued-for-a");
    const messageId = MessageId.make("message:queued-for-a");
    const selected = ownershipRun("run:parent-a", 1, {
      delegatedCompletion: {
        disposition: "open",
        nextGeneration: 2,
        delivery: { generation: 1, messageId, taskIds: [taskId] },
      },
    });
    const later = ownershipRun("run:parent-b", 2);
    const records = ownershipRecords([selected, later], {
      subagents: [
        {
          id: taskId,
          runId: selected.id,
          status: "completed",
          completionDelivery: { state: "acknowledged", observedByRunId: null },
        },
      ],
      messages: [
        {
          id: messageId,
          runId: null,
          delegatedCompletion: { parentRunId: selected.id, generation: 1, taskIds: [taskId] },
        },
      ],
    });
    expect((yield* waitForOwnedRecords(records)).timedOut).toBe(true);
    const otherWait = yield* waitForOwnedRecords({ ...records, runs: [later, selected] });
    expect(otherWait.timedOut).toBe(false);
    expect(otherWait.resultRun?.id).toBe(later.id);
  }),
);

it.effect.each(["running", "pending-delivery", "nested"] as const)(
  "delegated wait retains owned descendant work: %s",
  (scenario) =>
    Effect.gen(function* () {
      const selected = ownershipRun("run:owned", 1);
      const taskId = NodeId.make("task:owned-child");
      const records = ownershipRecords([selected], {
        subagents: [
          {
            id: taskId,
            runId: selected.id,
            status: scenario === "running" ? "running" : "completed",
            ...(scenario === "pending-delivery"
              ? { completionDelivery: { state: "pending", observedByRunId: null } }
              : {}),
          },
          ...(scenario === "nested"
            ? [
                {
                  id: NodeId.make("task:nested"),
                  runId: RunId.make("run:native-child"),
                  parentNodeId: taskId,
                  status: "running",
                } as const,
              ]
            : []),
        ],
      });
      expect((yield* waitForOwnedRecords(records)).timedOut).toBe(true);
    }),
);

it.effect.each(["completed", "queued", "restart", "new-descendant"] as const)(
  "delegated wait follows its task notification across unrelated work: %s",
  (scenario) =>
    Effect.gen(function* () {
      const selected = ownershipRun("run:owned", 1);
      const unrelated = ownershipRun("run:unrelated", 2, { status: "running" });
      const taskId = NodeId.make("task:owned-child");
      const wake = ownershipRun("run:owned-wake", 3, {
        userMessageId: MessageId.make("message:owned-wake"),
        workStartedAt: unrelated.startedAt!,
        ...(scenario === "queued" ? { status: "queued", startedAt: null } : {}),
      });
      const continuation = ownershipRun("run:owned-restart", 4, {
        status: "running",
        restartContinuationOfRunId: wake.id,
      });
      const records = ownershipRecords(
        [selected, unrelated, wake, ...(scenario === "restart" ? [continuation] : [])],
        {
          messages: [
            {
              id: wake.userMessageId,
              runId: wake.id,
              notification: {
                source: { kind: "delegated_task", taskIds: [taskId] },
                outcome: "completed",
                summary: "Child done",
              },
            },
          ],
          subagents: [
            // Reverse dependency order exercises the ownership fixed point.
            ...(scenario === "new-descendant"
              ? [{ id: NodeId.make("task:from-wake"), runId: wake.id, status: "running" } as const]
              : []),
            { id: taskId, runId: selected.id, status: "completed" },
          ],
        },
      );
      const result = yield* waitForOwnedRecords(records);
      expect(result.timedOut).toBe(scenario !== "completed");
      if (scenario === "completed") expect(result.resultRun?.id).toBe(wake.id);
    }),
);

it.effect.each([
  "owned",
  "unrelated-provider",
  "unrelated-native",
  "opaque-shared",
  "unawaited",
] as const)("delegated wait scopes provider background work: %s", (scenario) =>
  Effect.gen(function* () {
    const providerThreadId = ProviderThreadId.make("provider:owned");
    const otherProviderThreadId = ProviderThreadId.make("provider:other");
    const providerInstanceId = ProviderInstanceId.make("claude");
    const selected = ownershipRun("run:owned", 1, { providerThreadId });
    const unrelated = ownershipRun("run:unrelated", 2, {
      providerThreadId:
        scenario === "unrelated-provider" ? otherProviderThreadId : providerThreadId,
      status: "running",
    });
    const background = {
      kind: "command" as const,
      taskId: "native:command",
      ...(scenario === "unawaited" ? {} : { awaited: true as const }),
    };
    const records = ownershipRecords([selected, unrelated], {
      providerThreads: [
        {
          id: scenario === "unrelated-provider" ? otherProviderThreadId : providerThreadId,
          providerInstanceId,
          pendingBackgroundTasks: [background],
        },
      ],
      subagents:
        scenario === "opaque-shared"
          ? []
          : [
              {
                id: NodeId.make("task:native"),
                runId:
                  scenario === "owned" || scenario === "unawaited" ? selected.id : unrelated.id,
                providerInstanceId,
                nativeTaskRef: {
                  driver: ProviderDriverKind.make("claude"),
                  nativeId: background.taskId,
                  strength: "strong",
                },
                status: "completed",
              },
            ],
    });
    const result = yield* waitForOwnedRecords(records);
    expect(result.timedOut).toBe(scenario === "owned" || scenario === "opaque-shared");
    if (!result.timedOut) expect(result.resultRun?.id).toBe(selected.id);
  }),
);

it.effect("an unrelated task notification does not inherit timestamp ownership", () =>
  Effect.gen(function* () {
    const selected = ownershipRun("run:owned", 1);
    const wake = ownershipRun("run:other-task-wake", 2, {
      status: "queued",
      userMessageId: MessageId.make("message:other-wake"),
      workStartedAt: selected.startedAt!,
    });
    const records = ownershipRecords([selected, wake], {
      messages: [
        {
          id: wake.userMessageId,
          runId: wake.id,
          notification: {
            source: { kind: "delegated_task", taskIds: [NodeId.make("task:other")] },
            outcome: "completed",
            summary: "Other task done",
          },
        },
      ],
    });
    const result = yield* waitForOwnedRecords(records);
    expect(result.timedOut).toBe(false);
    expect(result.resultRun?.id).toBe(selected.id);
  }),
);

it.effect.each([
  "untagged",
  "task",
  "wake",
  "same-time-wake",
  "restart",
  "rolled_back",
  "unrelated-wake",
] as const)("delegated wait preserves ownership: %s", (scenario) =>
  Effect.gen(function* () {
    const taskId = NodeId.make("task:owned");
    const selected = ownershipRun("run:owned", 1, {
      ...(scenario === "untagged" || scenario === "unrelated-wake" || scenario === "same-time-wake"
        ? {}
        : { delegatedTaskId: taskId }),
      ...(scenario === "rolled_back" ? { status: "rolled_back", startedAt: null } : {}),
    });
    const ownedLater = ownershipRun("run:owned-later", 2, {
      ...(scenario === "wake" || scenario === "same-time-wake"
        ? { workStartedAt: selected.startedAt! }
        : {}),
      ...(scenario === "same-time-wake" ? { startedAt: selected.startedAt } : {}),
      ...(scenario === "restart" ? { restartContinuationOfRunId: selected.id } : {}),
    });
    const unrelated = ownershipRun("run:unrelated", 3, {
      delegatedTaskId: NodeId.make("task:unrelated"),
    });
    const runs =
      scenario === "rolled_back"
        ? [selected]
        : scenario === "unrelated-wake"
          ? [
              selected,
              {
                ...unrelated,
                ordinal: 2,
                startedAt: selected.startedAt,
                delegatedTaskId: undefined,
              },
              ownershipRun("run:unrelated-wake", 3, {
                startedAt: selected.startedAt,
                workStartedAt: selected.startedAt!,
              }),
            ]
          : [selected, ownedLater, unrelated];
    const wakeRun = scenario === "unrelated-wake" ? runs[2]! : ownedLater;
    const records = ownershipRecords(runs, {
      messages: ["wake", "same-time-wake", "unrelated-wake"].includes(scenario)
        ? [
            {
              id: wakeRun.userMessageId,
              runId: wakeRun.id,
              notification: {
                originRunIds: [scenario === "unrelated-wake" ? unrelated.id : selected.id],
                source: { kind: "background_task" },
                outcome: "completed",
                summary: "Background work completed",
              },
            },
          ]
        : [],
    });
    const layer = waitOwnershipLayer({
      getThreadRecords: () => Effect.succeed(records),
    });
    const service = yield* ThreadManagementService.ThreadManagementService.pipe(
      Effect.provide(layer),
    );
    const result = yield* service.waitForThread({
      projectId: waitOwnershipProjectId,
      threadId: waitOwnershipThreadId,
      runId: selected.id,
      timeoutMs: 60 * 60 * 1_000,
      untilDelegatedWorkSettled: true,
    });
    expect(result.timedOut).toBe(false);
    expect(result.resultRun?.id).toBe(
      scenario === "wake" || scenario === "same-time-wake" || scenario === "restart"
        ? ownedLater.id
        : selected.id,
    );
  }),
);

it.effect.each(["cancelled", "failed"] as const)(
  "delegated wait settles a follow-up that %s before starting",
  (status) =>
    Effect.gen(function* () {
      const previous = ownershipRun("run:previous", 1);
      const selected = ownershipRun("run:never-started", 2, { status, startedAt: null });
      const service = yield* ThreadManagementService.ThreadManagementService.pipe(
        Effect.provide(
          waitOwnershipLayer({
            getThreadRecords: () => Effect.succeed(ownershipRecords([previous, selected])),
          }),
        ),
      );
      const result = yield* service.waitForThread({
        projectId: waitOwnershipProjectId,
        threadId: waitOwnershipThreadId,
        runId: selected.id,
        timeoutMs: 60 * 60 * 1_000,
        untilDelegatedWorkSettled: true,
      });
      expect(result).toMatchObject({
        timedOut: false,
        run: { id: selected.id, status },
        resultRun: { id: selected.id, status },
      });
    }),
);

it.effect("delegated wait subscribes to a later answer's restart decline without an event", () =>
  Effect.gen(function* () {
    const selected = ownershipRun("run:selected", 1);
    const wake = ownershipRun("run:wake", 2, {
      status: "running",
      completedAt: null,
      workStartedAt: selected.startedAt!,
    });
    let runs = [selected, wake];
    let pending = false;
    const subscribed = yield* Deferred.make<void>();
    const awaitingDecline = yield* Queue.unbounded<RunId>();
    let activeWaiters = 0;
    const declined = yield* Deferred.make<void>();
    const events = yield* Queue.unbounded<OrchestrationV2StoredEvent>();
    const layer = waitOwnershipLayer({
      getThreadRecords: () => Effect.sync(() => ownershipRecords(runs)),
      restartContinuationPending: (run) => Effect.succeed(pending && run.id === wake.id),
      awaitRestartContinuation: (run) =>
        Effect.gen(function* () {
          activeWaiters++;
          yield* Queue.offer(awaitingDecline, run.id);
          yield* Deferred.await(declined);
        }).pipe(Effect.ensuring(Effect.sync(() => activeWaiters--))),
      streamStoredEventsFrom: (input) =>
        input?.eventType === "run.updated"
          ? Stream.fromEffect(Deferred.succeed(subscribed, undefined)).pipe(
              Stream.drain,
              Stream.concat(Stream.fromQueue(events)),
            )
          : Stream.never,
    });
    const service = yield* ThreadManagementService.ThreadManagementService.pipe(
      Effect.provide(layer),
    );
    const fiber = yield* service
      .waitForThread({
        projectId: waitOwnershipProjectId,
        threadId: waitOwnershipThreadId,
        runId: selected.id,
        timeoutMs: 60 * 60 * 1_000,
        untilDelegatedWorkSettled: true,
      })
      .pipe(Effect.forkChild);
    yield* Deferred.await(subscribed);
    pending = true;
    runs = [selected, { ...wake, status: "cancelled", completedAt: DateTime.makeUnsafe(4_000) }];
    yield* Queue.offer(events, {
      sequence: 1,
      event: { threadId: waitOwnershipThreadId, type: "run.updated", payload: runs[1] },
    } as unknown as OrchestrationV2StoredEvent);
    expect(yield* Queue.take(awaitingDecline)).toBe(wake.id);
    for (const sequence of [2, 3]) {
      yield* Queue.offer(events, {
        sequence,
        event: { threadId: waitOwnershipThreadId, type: "run.updated", payload: runs[1] },
      } as unknown as OrchestrationV2StoredEvent);
      expect(yield* Queue.take(awaitingDecline)).toBe(wake.id);
      expect(activeWaiters).toBe(1);
    }
    pending = false;
    yield* Deferred.succeed(declined, undefined);
    const result = yield* Fiber.join(fiber);
    expect(result).toMatchObject({
      timedOut: false,
      resultRun: { id: wake.id, status: "cancelled" },
    });
  }),
);

it.effect("stopDelegatedTasks records a remote stop even after its task has finished", () =>
  Effect.gen(function* () {
    const taskId = NodeId.make("task:remote-finished");
    const commands: Array<OrchestrationV2ServerCommand> = [];
    const layer = waitOwnershipLayer({
      getThreadRecords: () =>
        Effect.succeed({
          ...ownershipRecords([]),
          subagents: [
            {
              id: taskId,
              origin: "app_owned",
              childThreadId: null,
              status: "completed",
              result: "Original answer",
              remoteChild: { environmentId: EnvironmentId.make("environment:remote") },
            },
          ],
        } as unknown as OrchestrationV2ThreadProjection),
      dispatch: (command) =>
        Effect.sync(() => {
          commands.push(command);
          return { sequence: 1, storedEvents: [] } as never;
        }),
    });
    const service = yield* ThreadManagementService.ThreadManagementService.pipe(
      Effect.provide(layer),
    );
    yield* service.stopDelegatedTasks({
      threadId: waitOwnershipThreadId,
      commandId: CommandId.make("command:stop-finished"),
    });
    expect(commands).toEqual([
      {
        type: "delegated_task.remote.stop",
        commandId: CommandId.make(`command:stop-finished:stop:${taskId}`),
        parentThreadId: waitOwnershipThreadId,
        taskId,
      },
    ]);
  }),
);

it.effect("waitForThread returns the cut run once its continuation is declined", () =>
  Effect.gen(function* () {
    const projectId = ProjectId.make("project:thread-management:wait-declined");
    const threadId = ThreadId.make("thread:thread-management:wait-declined");
    const runId = RunId.make("run:thread-management:wait-declined");
    const subscribed = yield* Deferred.make<void>();
    const declined = yield* Deferred.make<void>();
    let pending = true;
    const layerTest = ThreadManagementService.layer.pipe(
      Layer.provide(Layer.mock(ProviderSessionManager.ProviderSessionManagerV2)({})),
      Layer.provide(
        Layer.mock(Orchestrator.OrchestratorV2)({
          getThreadEventSequence: () => Effect.succeed(0),
          restartContinuationPending: () => Effect.succeed(pending),
          awaitRestartContinuation: () => Deferred.await(declined),
          streamStoredEventsFrom: (input) =>
            input?.eventType === "run.updated"
              ? Stream.fromEffect(Deferred.succeed(subscribed, undefined)).pipe(
                  Stream.drain,
                  Stream.concat(Stream.never),
                )
              : Stream.never,
          getThreadRecords: () =>
            Effect.succeed({
              thread: { id: threadId, projectId, deletedAt: null },
              runs: [{ id: runId, status: "cancelled" }],
            } as unknown as OrchestrationV2ThreadProjection),
        }),
      ),
      Layer.provide(ServerSettings.layerTest()),
      Layer.provide(layerReceiptsTest),
    );
    const service = yield* ThreadManagementService.ThreadManagementService.pipe(
      Effect.provide(layerTest),
    );
    const fiber = yield* service
      .waitForThread({ projectId, threadId, runId, timeoutMs: 60 * 60 * 1_000 })
      .pipe(Effect.forkChild);
    yield* Deferred.await(subscribed);
    pending = false;
    yield* Deferred.succeed(declined, undefined);
    const result = yield* Fiber.join(fiber);

    expect(result).toMatchObject({ timedOut: false, run: { id: runId, status: "cancelled" } });
  }),
);

it.effect.each(["live", "no-session", "unsupported-runtime"] as const)(
  "marks background work awaited on the thread's active provider thread with state %s",
  (state) =>
    Effect.gen(function* () {
      const threadId = ThreadId.make("thread:background-wait");
      const providerThreadId = ProviderThreadId.make("provider-thread:background-wait");
      const providerSessionId = ProviderSessionId.make("provider-session:background-wait");
      const driver = ProviderDriverKind.make("claudeAgent");
      const now = yield* DateTime.now;
      const providerThread: OrchestrationV2ProviderThread = {
        id: providerThreadId,
        driver,
        providerInstanceId: ProviderInstanceId.make("claudeAgent"),
        providerSessionId,
        appThreadId: threadId,
        ownerNodeId: null,
        nativeThreadRef: { driver, nativeId: "native-thread", strength: "strong" },
        nativeConversationHeadRef: null,
        status: "idle",
        firstRunOrdinal: 1,
        lastRunOrdinal: 1,
        handoffIds: [],
        forkedFrom: null,
        pendingBackgroundTasks: [{ taskId: "tests", kind: "command" }],
        contextUsage: null,
        nativeMetadata: null,
        createdAt: now,
        updatedAt: now,
      };
      const calls: Array<{ providerThreadId: ProviderThreadId; awaited: boolean }> = [];
      const runtime = {
        driver,
        providerSessionId,
        ...(state === "unsupported-runtime"
          ? {}
          : {
              awaitBackgroundTasks: (input: {
                readonly providerThread: OrchestrationV2ProviderThread;
                readonly taskIds: ReadonlyArray<string>;
                readonly awaited: boolean;
              }) =>
                Effect.sync(() => {
                  calls.push({
                    providerThreadId: input.providerThread.id,
                    awaited: input.awaited,
                  });
                  return { matched: input.taskIds.filter((taskId) => taskId === "tests") };
                }),
            }),
      } as unknown as ProviderAdapter.ProviderAdapterV2SessionRuntime;
      const layerTest = ThreadManagementService.layer.pipe(
        Layer.provide(
          Layer.mergeAll(
            Layer.mock(Orchestrator.OrchestratorV2)({
              getThreadRecords: () =>
                Effect.succeed({
                  thread: {
                    id: threadId,
                    deletedAt: null,
                    activeProviderThreadId: providerThreadId,
                  },
                  providerThreads: [providerThread],
                } as unknown as OrchestrationV2ThreadProjection),
            }),
            Layer.mock(ProviderSessionManager.ProviderSessionManagerV2)({
              get: () =>
                Effect.succeed(state === "no-session" ? Option.none() : Option.some(runtime)),
            }),
          ),
        ),
        Layer.provide(ServerSettings.layerTest()),
        Layer.provide(layerReceiptsTest),
      );
      const result = yield* Effect.gen(function* () {
        const service = yield* ThreadManagementService.ThreadManagementService;
        return yield* service
          .awaitBackgroundTasks({ threadId, taskIds: ["tests", "finished"], awaited: true })
          .pipe(Effect.result);
      }).pipe(Effect.provide(Layer.merge(layerTest, ThreadCommandExecutor.layer)));

      if (state === "live") {
        expect(result).toMatchObject({
          success: { matched: ["tests"], unmatched: ["finished"] },
        });
        expect(calls).toEqual([{ providerThreadId, awaited: true }]);
      } else {
        expect(result).toMatchObject({
          failure: {
            _tag: "ThreadManagementBackgroundWaitError",
            reason: state === "no-session" ? "unavailable" : "unsupported",
          },
        });
        expect(calls).toEqual([]);
      }
    }),
);

it.effect.each([
  { status: "completed" as const, followUp: "none", settles: true },
  { status: "failed" as const, followUp: "none", settles: false },
  { status: "interrupted" as const, followUp: "none", settles: false },
  { status: "completed" as const, followUp: "notification", settles: true },
  { status: "completed" as const, followUp: "replayed-notification", settles: true },
  { status: "completed" as const, followUp: "delegated", settles: true },
  { status: "completed" as const, followUp: "failed-notification", settles: false },
  { status: "completed" as const, followUp: "held-notification", settles: true },
  { status: "completed" as const, followUp: "user", settles: false },
  { status: "completed" as const, followUp: "steered-notification", settles: false },
])("settleAfterRun handles $status with $followUp follow-up", ({ status, followUp, settles }) =>
  Effect.gen(function* () {
    const projectId = ProjectId.make("project:thread-management:settle-after-run");
    const threadId = ThreadId.make("thread:thread-management:settle-after-run");
    const runId = RunId.make("run:thread-management:settle-after-run");
    const followUpId = RunId.make(`${runId}:follow-up`);
    const subscribed = yield* Deferred.make<void>();
    const events = yield* Queue.unbounded<OrchestrationV2StoredEvent>();
    const replayRejected = yield* Deferred.make<void>();
    let sequence = 0;
    let sequenceReads = 0;
    let rejectedCommandId: CommandId | undefined;
    const runs = [{ id: runId, ordinal: 1, status, userMessageId: MessageId.make("original") }];
    const nextRun = {
      id: followUpId,
      ordinal: 2,
      status: "running" as OrchestrationV2Run["status"],
      userMessageId: MessageId.make("follow-up"),
    };
    const dispatched: Array<string> = [];
    // The notification starts after the completed run was observed, before settlement commits.
    let raced = false;
    const layerTest = ThreadManagementService.layer.pipe(
      Layer.provide(Layer.mock(ProviderSessionManager.ProviderSessionManagerV2)({})),
      Layer.provide(
        Layer.mock(Orchestrator.OrchestratorV2)({
          getThreadEventSequence: () =>
            Effect.sync(() => {
              if (followUp === "replayed-notification" && sequenceReads++ === 2) sequence = 1;
              return sequence;
            }),
          restartContinuationPending: () => Effect.succeed(false),
          streamStoredEventsFrom: () =>
            Stream.fromEffect(Deferred.succeed(subscribed, undefined)).pipe(
              Stream.drain,
              Stream.concat(Stream.fromQueue(events)),
            ),
          getThreadRecords: (_threadId, _fields, filter) =>
            Effect.sync(
              () =>
                ({
                  thread: { id: threadId, projectId, deletedAt: null },
                  runs: raced ? [...runs, nextRun] : runs,
                  messages: [
                    { id: MessageId.make("original"), runId, role: "user" as const },
                    {
                      id: nextRun.userMessageId,
                      runId: followUpId,
                      role: "user" as const,
                      ...(followUp.endsWith("notification") ? { notification: {} } : {}),
                      ...(followUp === "delegated" ? { delegatedCompletion: {} } : {}),
                    },
                    { id: MessageId.make("reply"), runId: followUpId, role: "assistant" as const },
                    ...(followUp === "steered-notification"
                      ? [
                          {
                            id: MessageId.make("user-steer"),
                            runId: followUpId,
                            role: "user" as const,
                          },
                        ]
                      : []),
                  ].filter(
                    (message) =>
                      (filter?.messageIds === undefined ||
                        filter.messageIds.includes(message.id)) &&
                      (filter?.messageRunIds === undefined ||
                        filter.messageRunIds.includes(message.runId)) &&
                      (filter?.messageRoles === undefined ||
                        filter.messageRoles.includes(message.role)),
                  ),
                }) as unknown as OrchestrationV2ThreadProjection,
            ),
          dispatch: (command) =>
            Effect.gen(function* () {
              if (followUp !== "none" && !raced) {
                raced = true;
                rejectedCommandId = command.commandId;
                return yield* new Orchestrator.OrchestratorDispatchError({
                  commandId: command.commandId,
                  commandType: command.type,
                  cause: "Thread is still running",
                });
              }
              if (followUp === "replayed-notification" && command.commandId === rejectedCommandId) {
                yield* Deferred.succeed(replayRejected, undefined);
                return yield* new Orchestrator.OrchestratorCommandPreviouslyRejectedError({
                  commandId: command.commandId,
                  commandType: command.type,
                  detail: "Thread is still running",
                });
              }
              dispatched.push(command.type);
              return { sequence: 1, storedEvents: [] } as never;
            }),
        }),
      ),
      Layer.provide(ServerSettings.layerTest()),
      Layer.provide(layerReceiptsTest),
    );
    const service = yield* ThreadManagementService.ThreadManagementService.pipe(
      Effect.provide(layerTest),
    );
    const fiber = yield* service
      .settleAfterRun({ projectId, threadId, runId })
      .pipe(Effect.forkChild);
    if (followUp !== "none") {
      yield* Deferred.await(subscribed);
      expect(dispatched).toEqual([]);
      nextRun.status =
        followUp === "failed-notification"
          ? "failed"
          : followUp === "held-notification"
            ? "queued"
            : "completed";
      yield* Queue.offer(events, {
        sequence: 1,
        event: { type: "run.updated", threadId, payload: nextRun },
      } as unknown as OrchestrationV2StoredEvent);
    }
    if (followUp === "replayed-notification") {
      yield* Deferred.await(replayRejected);
      expect(dispatched).toEqual([]);
      sequence = 2;
      yield* Queue.offer(events, {
        sequence,
        event: { type: "run.updated", threadId, payload: nextRun },
      } as unknown as OrchestrationV2StoredEvent);
    }
    yield* Fiber.join(fiber);
    expect(dispatched).toEqual(settles ? ["thread.settle"] : []);
  }),
);

it.effect.each([
  {
    name: "kills the thread's own agent and ends its turn",
    others: [] as ReadonlyArray<string>,
    killed: true,
    expected: { outcome: "killed", sharedWithThreadIds: [] },
    teardown: ["release"],
  },
  {
    name: "keeps a process another working thread shares and only ends this turn",
    others: ["thread:thread-management:force-stop:other"],
    killed: false,
    expected: {
      outcome: "shared",
      sharedWithThreadIds: ["thread:thread-management:force-stop:other"],
    },
    teardown: ["detach"],
  },
  {
    name: "still settles a finished run's background work in a shared process",
    others: ["thread:thread-management:force-stop:other"],
    killed: false,
    settledRun: true,
    expected: {
      outcome: "shared",
      sharedWithThreadIds: ["thread:thread-management:force-stop:other"],
    },
    teardown: ["detach"],
  },
  {
    name: "detaches when a sibling starts a turn just before the session is released",
    others: [] as ReadonlyArray<string>,
    killed: false,
    siblingBusy: true,
    expected: { outcome: "shared", sharedWithThreadIds: [] },
    teardown: ["release", "detach"],
  },
  {
    name: "reports a teardown that has not finished in time as still releasing",
    others: [] as ReadonlyArray<string>,
    killed: false,
    releaseHangs: true,
    expected: { outcome: "releasing", sharedWithThreadIds: [] },
    teardown: ["release"],
  },
])("Force stop $name", ({ others, killed, expected, teardown, ...row }) =>
  Effect.gen(function* () {
    const threadId = ThreadId.make("thread:thread-management:force-stop");
    const providerSessionId = ProviderSessionId.make("provider-session:force-stop");
    const providerThreadId = ProviderThreadId.make("provider-thread:force-stop");
    const runId = RunId.make("run:thread-management:force-stop");
    const dispatched: Array<OrchestrationV2Command | { readonly type: string }> = [];
    const teardowns: Array<string> = [];
    const tornDown = yield* Deferred.make<void>();
    const layerTest = ThreadManagementService.layer.pipe(
      Layer.provide(
        Layer.mock(ProviderSessionManager.ProviderSessionManagerV2)({
          isLive: () => Effect.succeed(true),
          otherWorkingThreads: () => Effect.succeed(others.map((id) => ThreadId.make(id))),
          releaseUnlessBusy: () =>
            Effect.sync(() => teardowns.push("release")).pipe(
              Effect.andThen(
                "releaseHangs" in row ? Effect.never : Effect.succeed(!("siblingBusy" in row)),
              ),
            ),
          detach: () =>
            Effect.sync(() => teardowns.push("detach")).pipe(
              Effect.andThen(Deferred.succeed(tornDown, undefined)),
              Effect.asVoid,
            ),
        }),
      ),
      Layer.provide(
        Layer.mock(Orchestrator.OrchestratorV2)({
          getThreadRecords: () =>
            Effect.succeed({
              thread: { id: threadId, deletedAt: null },
              runs: [
                {
                  id: runId,
                  ordinal: 1,
                  status: "settledRun" in row ? "completed" : "running",
                  activeAttemptId: "attempt",
                },
                // A queued follow-up neither supersedes the request nor becomes the target.
                { id: RunId.make("run:queued-follow-up"), ordinal: 2, status: "queued" },
              ],
              providerThreads: [{ id: providerThreadId, appThreadId: threadId, providerSessionId }],
              providerTurns: [
                {
                  id: "provider-turn:force-stop",
                  providerThreadId,
                  runAttemptId: "attempt",
                  status: "settledRun" in row ? "completed" : "running",
                },
              ],
            } as unknown as OrchestrationV2ThreadProjection),
          dispatch: (command) =>
            Effect.sync(() => {
              dispatched.push(command);
              return { sequence: 1, storedEvents: [] } as never;
            }),
        }),
      ),
      Layer.provide(
        Layer.succeed(AgentScope, {
          ...AgentScope.defaultValue(),
          kill: () => Effect.succeed(killed),
        }),
      ),
      Layer.provide(ServerSettings.layerTest()),
      Layer.provide(layerReceiptsTest),
    );
    yield* Effect.gen(function* () {
      const service = yield* ThreadManagementService.ThreadManagementService;
      const stopping = yield* service.forceStopThread({ threadId, runId }).pipe(Effect.forkChild);
      yield* TestClock.adjust("10 seconds");
      expect(yield* Fiber.join(stopping)).toEqual(expected);
      expect(dispatched).toMatchObject([
        {
          type: "thread.background-work.settle",
          providerThreadId,
          reason: "Force stopped by user",
        },
      ]);
      if (teardown.at(-1) === "detach") yield* Deferred.await(tornDown);
      expect(teardowns).toEqual(teardown);
    }).pipe(Effect.provide(layerTest));
  }),
);

it.effect("Force stop refuses once a newer run has started, before killing anything", () =>
  Effect.gen(function* () {
    const threadId = ThreadId.make("thread:thread-management:force-stop-superseded");
    let killed = false;
    const layerTest = ThreadManagementService.layer.pipe(
      Layer.provide(Layer.mock(ProviderSessionManager.ProviderSessionManagerV2)({})),
      Layer.provide(
        Layer.mock(Orchestrator.OrchestratorV2)({
          getThreadRecords: () =>
            Effect.succeed({
              thread: { id: threadId, deletedAt: null },
              runs: [
                { id: RunId.make("run:stopped"), ordinal: 1, status: "running" },
                { id: RunId.make("run:newer"), ordinal: 2, status: "running" },
              ],
              providerThreads: [],
              providerTurns: [],
            } as unknown as OrchestrationV2ThreadProjection),
        }),
      ),
      Layer.provide(
        Layer.succeed(AgentScope, {
          ...AgentScope.defaultValue(),
          kill: () => Effect.sync(() => (killed = true)),
        }),
      ),
      Layer.provide(ServerSettings.layerTest()),
      Layer.provide(layerReceiptsTest),
    );
    const error = yield* Effect.gen(function* () {
      const service = yield* ThreadManagementService.ThreadManagementService;
      return yield* Effect.flip(
        service.forceStopThread({ threadId, runId: RunId.make("run:stopped") }),
      );
    }).pipe(Effect.provide(layerTest));
    expect(error.reason).toBe("superseded");
    expect(killed).toBe(false);
  }),
);
