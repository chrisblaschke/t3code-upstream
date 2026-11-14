import * as DocumentText from "../provider/DocumentText.ts";
import { NodeHttpServer } from "@effect/platform-node";
import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import * as NodeServices from "@effect/platform-node/NodeServices";
import {
  CommandId,
  EventId,
  MessageId,
  NodeId,
  OrchestratorMcpFailure,
  type OrchestrationV2Run,
  ProjectId,
  ProviderDriverKind,
  ProviderInstanceId,
  ProviderThreadId,
  type Project,
  type RepositoryIdentity,
  RunId,
  ThreadId,
  TurnItemId,
} from "@t3tools/contracts";
import { expect, it } from "@effect/vitest";
import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Duration from "effect/Duration";
import * as Fiber from "effect/Fiber";
import * as TestClock from "effect/testing/TestClock";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Queue from "effect/Queue";
import * as Ref from "effect/Ref";
import * as Schedule from "effect/Schedule";
import * as Stream from "effect/Stream";
import * as SubscriptionRef from "effect/SubscriptionRef";
import * as Tracer from "effect/Tracer";
import type * as SqlClient from "effect/sql/SqlClient";
import { McpSchema, McpServer } from "effect/ai";

import * as ServerConfig from "../config.ts";
import * as McpHttpServer from "../mcp/McpHttpServer.ts";
import * as McpInvocationContext from "../mcp/McpInvocationContext.ts";
import { idleThreadProjection, liveThreadShell } from "../mcp/McpToolAccess.testkit.ts";
import { CodexProviderCapabilitiesV2 } from "../orchestration-v2/Adapters/CodexAdapterV2.ts";
import * as EventSink from "../orchestration-v2/EventSink.ts";
import * as Orchestrator from "../orchestration-v2/Orchestrator.ts";
import * as ProjectionStore from "../orchestration-v2/ProjectionStore.ts";
import type * as ProviderAdapter from "@t3tools/provider-core/server/ProviderAdapter";
import * as ProviderAdapterRegistry from "../orchestration-v2/ProviderAdapterRegistry.ts";
import * as ProviderContinuationRequests from "@t3tools/provider-core/server/ProviderContinuationRequests";
import * as ThreadLaunch from "../orchestration-v2/ThreadLaunchService.ts";
import * as ThreadSearch from "../orchestration-v2/ThreadSearch.ts";
import * as ThreadManagement from "../orchestration-v2/ThreadManagementService.ts";
import * as ProviderReplayHarness from "../orchestration-v2/testkit/ProviderReplayHarness.ts";
import * as SqlitePersistence from "../persistence/Sqlite.ts";
import * as ManagedProjectFolders from "../project/ManagedProjectFolders.ts";
import * as ProjectCloneTracker from "../project/ProjectCloneTracker.ts";
import * as ProjectService from "../project/ProjectService.ts";
import * as RepositoryIdentityResolver from "../project/RepositoryIdentityResolver.ts";
import * as ProviderRegistry from "../provider/ProviderRegistry.ts";
import * as ScheduledTaskService from "../scheduledTasks/ScheduledTaskService.ts";
import * as SecretRequests from "../secrets/SecretRequests.ts";
import * as ServerSettings from "../serverSettings.ts";
import * as CommandReceiptStore from "../orchestration-v2/CommandReceiptStore.ts";
import * as SourceControlRepositoryService from "../sourceControl/SourceControlRepositoryService.ts";
import * as GitVcsDriver from "../vcs/GitVcsDriver.ts";
import * as PeerForwarding from "./PeerForwarding.ts";
import * as PeerLinks from "./PeerLinks.ts";
import { descriptorOf, layerLinkingEnvironment, linkTo, servePeer } from "./PeerLinks.testkit.ts";
import * as RemoteDelegation from "./RemoteDelegation.ts";
import * as PeerLinkRequests from "./PeerLinkRequests.ts";

// The laptop's agent delegates a task to the box through a link. The box is
// its real /mcp behind real OAuth, with one thread the test finishes; the
// laptop is its real orchestrator and toolkits. The parent here wakes as it
// would for a local child.

const laptop = descriptorOf("environment-laptop", "Laptop");
const box = descriptorOf("environment-box", "Box");
const boxProject = ProjectId.make("project:box");
const instanceId = ProviderInstanceId.make("codex");
const driver = ProviderDriverKind.make("codex");
const modelSelection = { instanceId, model: "gpt-5.4" };

type LaunchedWith = Pick<
  ThreadLaunch.ThreadLaunchInput,
  "modelSelection" | "runtimeMode" | "interactionMode"
>;

/** The thread the box launches for the task, which the test finishes. */
interface BoxThread {
  readonly launched: Ref.Ref<ThreadId | null>;
  /** The link session that launched it, as the box stamps it. */
  readonly linkOrigin: Ref.Ref<{ readonly sessionId: string; readonly label: string } | null>;
  /** The parent the launch names, so the thread there can link back to it. */
  readonly delegatedFrom: Ref.Ref<ThreadLaunch.ThreadLaunchInput["delegatedFrom"] | null>;
  /** The model and modes it was launched with. */
  readonly launchedWith: Ref.Ref<LaunchedWith | null>;
  /** The first message the thread there was launched with. */
  readonly message: Ref.Ref<string | null>;
  readonly status: Ref.Ref<OrchestrationV2Run["status"]>;
  readonly reply: Ref.Ref<string | null>;
  readonly error: Ref.Ref<string | null>;
  readonly laterError: Ref.Ref<string | null>;
  /** A later run's reply, after the task's own, as a follow-up message would leave. */
  readonly laterReply: Ref.Ref<string | null>;
  /** The model the provider there reported running the task's run. */
  readonly executedModel: Ref.Ref<string | null>;
  /** The run each wait asked for, in order. */
  readonly waitedFor: Ref.Ref<ReadonlyArray<RunId | undefined>>;
  /** The thread's run ends with a task of its own still running, whose result wakes it again. */
  readonly nested: Ref.Ref<boolean>;
  /** Completed once a wait there blocks on that task. */
  readonly nestedWaiting: Deferred.Deferred<void>;
  /** That task settled and the run it woke answered, with `laterReply`. */
  readonly nestedSettled: Deferred.Deferred<void>;
  /** Waiting there fails with this, as a thread deleted there does. */
  readonly gone: Ref.Ref<boolean>;
  readonly interrupted: Ref.Ref<number>;
  /** What each interrupt or stop there did, once per command id. */
  readonly stops: Ref.Ref<ReadonlyArray<"interrupt" | "thread.stop" | "delegated-tasks">>;
  /** The launch starts no run, as one recorded before its run was kept. */
  readonly runless: Ref.Ref<boolean>;
  /** Completed on the first interrupt there. */
  readonly interruptedOnce: Deferred.Deferred<void>;
  readonly finished: Deferred.Deferred<void>;
  /** While set, reading the thread there waits: completed once a read starts, released by `releaseRead`. */
  readonly holdRead: Ref.Ref<boolean>;
  readonly readStarted: Deferred.Deferred<void>;
  readonly releaseRead: Deferred.Deferred<void>;
  readonly holdFirstStop: Ref.Ref<boolean>;
  readonly stopStarted: Deferred.Deferred<void>;
  readonly releaseStop: Deferred.Deferred<void>;
  /** Messages sent to the thread there after its launch, each starting a later run. */
  readonly sent: Ref.Ref<ReadonlyArray<string>>;
  /** Runs as the launch there is answered, before the laptop hears back. */
  readonly afterLaunch: Ref.Ref<Effect.Effect<void>>;
  readonly workspace: Ref.Ref<ThreadLaunch.ThreadLaunchInput["workspaceStrategy"] | null>;
  readonly questions: Ref.Ref<ReadonlyArray<{ readonly id: string; readonly question: string }>>;
  readonly answers: Ref.Ref<
    ReadonlyArray<{ readonly requestId: string; readonly answers: unknown }>
  >;
}

const makeBoxThread = Effect.gen(function* () {
  return {
    launched: yield* Ref.make<ThreadId | null>(null),
    linkOrigin: yield* Ref.make<{ readonly sessionId: string; readonly label: string } | null>(
      null,
    ),
    delegatedFrom: yield* Ref.make<ThreadLaunch.ThreadLaunchInput["delegatedFrom"] | null>(null),
    launchedWith: yield* Ref.make<LaunchedWith | null>(null),
    message: yield* Ref.make<string | null>(null),
    status: yield* Ref.make<OrchestrationV2Run["status"]>("running"),
    reply: yield* Ref.make<string | null>("Working…"),
    error: yield* Ref.make<string | null>(null),
    laterError: yield* Ref.make<string | null>(null),
    laterReply: yield* Ref.make<string | null>(null),
    executedModel: yield* Ref.make<string | null>(null),
    waitedFor: yield* Ref.make<ReadonlyArray<RunId | undefined>>([]),
    nested: yield* Ref.make(false),
    nestedWaiting: yield* Deferred.make<void>(),
    nestedSettled: yield* Deferred.make<void>(),
    gone: yield* Ref.make(false),
    interrupted: yield* Ref.make(0),
    stops: yield* Ref.make<ReadonlyArray<"interrupt" | "thread.stop" | "delegated-tasks">>([]),
    runless: yield* Ref.make(false),
    interruptedOnce: yield* Deferred.make<void>(),
    finished: yield* Deferred.make<void>(),
    holdRead: yield* Ref.make(false),
    readStarted: yield* Deferred.make<void>(),
    releaseRead: yield* Deferred.make<void>(),
    holdFirstStop: yield* Ref.make(false),
    stopStarted: yield* Deferred.make<void>(),
    releaseStop: yield* Deferred.make<void>(),
    sent: yield* Ref.make<ReadonlyArray<string>>([]),
    afterLaunch: yield* Ref.make<Effect.Effect<void>>(Effect.void),
    workspace: yield* Ref.make<ThreadLaunch.ThreadLaunchInput["workspaceStrategy"] | null>(null),
    questions: yield* Ref.make<ReadonlyArray<{ readonly id: string; readonly question: string }>>(
      [],
    ),
    answers: yield* Ref.make<
      ReadonlyArray<{ readonly requestId: string; readonly answers: unknown }>
    >([]),
  } satisfies BoxThread;
});

/** The box's projects: more than a page, with this repository's last. */
const boxProjects: ReadonlyArray<Project> = [
  ...Array.from({ length: 120 }, (_, index) => ({
    id: ProjectId.make(`project:box-other-${index}`),
    title: `other ${index}`,
    workspaceRoot: `/srv/other-${index}`,
    repositoryIdentity: null,
    defaultModelSelection: null,
    scripts: [],
    createdAt: "2026-10-01T00:00:00.000Z",
    updatedAt: "2026-10-01T00:00:00.000Z",
    deletedAt: null,
  })),
  {
    id: boxProject,
    title: "app",
    workspaceRoot: "/srv/app",
    repositoryIdentity: {
      canonicalKey: "github.com/acme/app",
      locator: {
        source: "git-remote",
        remoteName: "origin",
        remoteUrl: "git@github.com:acme/app.git",
      },
    },
    defaultModelSelection: null,
    scripts: [],
    createdAt: "2026-10-01T00:00:00.000Z",
    updatedAt: "2026-10-01T00:00:00.000Z",
    deletedAt: null,
  },
];

const serveBox = (thread: BoxThread) => {
  return servePeer(box, boxToolkitLayer(thread));
};

const boxRunId = RunId.make("run:box");
const boxLaterRunId = RunId.make("run:box-later");

const boxToolkitLayer = (thread: BoxThread) => {
  const runId = boxRunId;
  const interrupts = new Set<string>();
  const shellOf = (threadId: ThreadId) =>
    Effect.gen(function* () {
      if ((yield* Ref.get(thread.launched)) !== threadId) return null;
      const linkOrigin = yield* Ref.get(thread.linkOrigin);
      return {
        ...liveThreadShell(threadId, { runtimeMode: "full-access" }),
        projectId: boxProject,
        ...(linkOrigin === null ? {} : { linkOrigin }),
      };
    });
  const runOf = (status: OrchestrationV2Run["status"]) =>
    ({
      id: runId,
      threadId: ThreadId.make("thread:box"),
      ordinal: 1,
      status,
      modelSelection,
      activeAttemptId: "attempt:box",
      requestedAt: DateTime.makeUnsafe(0),
      startedAt: DateTime.makeUnsafe(0),
      completedAt: null,
    }) as unknown as OrchestrationV2Run;
  const projectionOf = (threadId: ThreadId) =>
    Effect.gen(function* () {
      const shell = yield* shellOf(threadId);
      const status = yield* Ref.get(thread.status);
      const runless = yield* Ref.get(thread.runless);
      const model = yield* Ref.get(thread.executedModel);
      const questions = yield* Ref.get(thread.questions);
      return shell === null
        ? (null as never)
        : {
            ...idleThreadProjection(shell),
            runs: runless ? [] : [runOf(status)],
            providerTurns: (model === null
              ? []
              : [{ runAttemptId: "attempt:box", ordinal: 1, model }]) as never,
            runtimeRequests: questions.map(
              ({ id }) => ({ id, kind: "user_input", status: "pending" }) as never,
            ),
            turnItems: questions.map(
              ({ id, question }) =>
                ({
                  type: "user_input_request",
                  requestId: id,
                  questions: [{ id: "q", header: "Question", question, options: [] }],
                }) as never,
            ),
          };
    });
  // One record per command id, as its receipt replays a repeat.
  const recordStop = (commandId: string, kind: "interrupt" | "thread.stop" | "delegated-tasks") =>
    Effect.gen(function* () {
      if (interrupts.has(`${kind}:${commandId}`)) return;
      interrupts.add(`${kind}:${commandId}`);
      yield* Ref.update(thread.stops, (seen) => [...seen, kind]);
      if (kind !== "delegated-tasks") {
        yield* Ref.update(thread.interrupted, (count) => count + 1);
        yield* Deferred.succeed(thread.interruptedOnce, undefined);
      }
    });
  const layerBoxThreads = Layer.mock(ThreadManagement.ThreadManagementService)({
    getThreadShell: shellOf,
    getProjectThreadRecords: (input) => projectionOf(input.threadId),
    getThreadRecords: (threadId) => projectionOf(threadId),
    waitForThread: (input) =>
      Effect.gen(function* () {
        yield* Ref.update(thread.waitedFor, (seen) => [...seen, input.runId]);
        // A thread with no run has nothing to wait for.
        if (yield* Ref.get(thread.runless)) {
          return { threadId: input.threadId, run: null, timedOut: false };
        }
        if (yield* Ref.get(thread.gone)) {
          return yield* new ThreadManagement.ThreadManagementThreadNotFoundError({
            projectId: boxProject,
            threadId: input.threadId,
          });
        }
        const done = yield* Deferred.await(thread.finished).pipe(
          Effect.timeoutOption(input.timeoutMs),
        );
        // Only a waiter that asks waits past the run for the thread's own task.
        if (
          Option.isSome(done) &&
          input.untilDelegatedWorkSettled === true &&
          (yield* Ref.get(thread.nested))
        ) {
          yield* Deferred.succeed(thread.nestedWaiting, undefined);
          const settled = yield* Deferred.await(thread.nestedSettled).pipe(
            Effect.timeoutOption(input.timeoutMs),
          );
          return {
            threadId: input.threadId,
            run: { id: runId, status: yield* Ref.get(thread.status) } as OrchestrationV2Run,
            timedOut: Option.isNone(settled),
            ...(Option.isSome(settled)
              ? { resultRun: { id: boxLaterRunId, status: "completed" } as OrchestrationV2Run }
              : {}),
          };
        }
        // Unpinned, a wait reports the latest run: a later one once there is one.
        const later = input.runId === undefined && (yield* Ref.get(thread.laterReply)) !== null;
        return {
          threadId: input.threadId,
          run: {
            id: later ? boxLaterRunId : runId,
            status: later ? "completed" : yield* Ref.get(thread.status),
          } as OrchestrationV2Run,
          timedOut: Option.isNone(done),
        };
      }),
    getTimelinePage: (threadId, options) =>
      Effect.gen(function* () {
        if (yield* Ref.get(thread.holdRead)) {
          yield* Deferred.succeed(thread.readStarted, undefined);
          yield* Deferred.await(thread.releaseRead);
        }
        const reply = yield* Ref.get(thread.reply);
        const later = yield* Ref.get(thread.laterReply);
        const error = yield* Ref.get(thread.error);
        const laterError = yield* Ref.get(thread.laterError);
        // One item per page, with errors available only in the activity view.
        const replies = [
          ...(reply === null
            ? []
            : [
                {
                  id: TurnItemId.make("item:reply"),
                  runId,
                  text: reply,
                  type: "assistant_message",
                },
              ]),
          ...(error === null
            ? []
            : [{ id: TurnItemId.make("item:error"), runId, text: error, type: "error" }]),
          ...(later === null
            ? []
            : [
                {
                  id: TurnItemId.make("item:later-reply"),
                  runId: boxLaterRunId,
                  text: later,
                  type: "assistant_message",
                },
              ]),
          ...(laterError === null
            ? []
            : [
                {
                  id: TurnItemId.make("item:later-error"),
                  runId: boxLaterRunId,
                  text: laterError,
                  type: "error",
                },
              ]),
        ].map((entry, position) => ({ ...entry, position }));
        const visible =
          options.itemId === undefined
            ? replies.filter(
                (entry) => options.view === "activity" || entry.type === "assistant_message",
              )
            : replies.filter((entry) => entry.id === options.itemId);
        const matching = visible
          .filter((entry) => entry.position > (options.afterPosition ?? -1))
          .slice(0, 1);
        return {
          items: matching.map(({ id, runId: itemRunId, text, position, type }) => ({
            position,
            visibility: "local" as const,
            sourceThreadId: threadId,
            sourceItemId: id,
            item: {
              id,
              threadId,
              runId: itemRunId,
              nodeId: null,
              providerThreadId: null,
              providerTurnId: null,
              nativeItemRef: null,
              parentItemId: null,
              ordinal: 1,
              status: "completed" as const,
              title: null,
              startedAt: DateTime.makeUnsafe(0),
              completedAt: DateTime.makeUnsafe(0),
              updatedAt: DateTime.makeUnsafe(0),
              ...(type === "error"
                ? { type, failure: { message: text } }
                : {
                    type: "assistant_message" as const,
                    messageId: MessageId.make(`message:${id}`),
                    text,
                    streaming: false,
                  }),
            } as never,
          })),
          totalItems: replies.length,
          hasMore:
            options.itemId === undefined &&
            matching.length > 0 &&
            visible.some((entry) => entry.position > matching.at(-1)!.position),
        };
      }),
    interruptThread: (input) =>
      recordStop(input.commandId, "interrupt").pipe(
        Effect.as({ type: "interrupt_requested", run: runOf("running") } as never),
      ),
    dispatch: (command) =>
      command.type === "thread.stop"
        ? recordStop(command.commandId, "thread.stop").pipe(
            Effect.tap(() =>
              Effect.gen(function* () {
                if (!(yield* Ref.getAndSet(thread.holdFirstStop, false))) return;
                yield* Deferred.succeed(thread.stopStarted, undefined);
                yield* Deferred.await(thread.releaseStop);
              }),
            ),
            Effect.as({
              sequence: 0,
              storedEvents: [
                // A queued run update is unrelated to the turn this stop interrupted.
                {
                  event: {
                    type: "run.updated",
                    runId: boxLaterRunId,
                    payload: { status: "cancelled" },
                  },
                },
                {
                  event: {
                    type: "turn-item.updated",
                    runId,
                    payload: { type: "run_interrupt_request", providerTurnId: "turn:box" },
                  },
                },
              ],
            } as never),
          )
        : command.type === "runtime-request.respond"
          ? Effect.gen(function* () {
              yield* Ref.update(thread.answers, (seen) => [
                ...seen,
                { requestId: command.requestId, answers: command.answers },
              ]);
              yield* Ref.update(thread.questions, (pending) =>
                pending.filter(({ id }) => id !== command.requestId),
              );
              return { sequence: 0, storedEvents: [] } as never;
            })
          : Effect.die(`The box does not expect ${command.type}`),
    stopDelegatedTasks: (input) => recordStop(input.commandId, "delegated-tasks"),
    sendToThread: (input) =>
      Ref.update(thread.sent, (seen) => [...seen, input.text]).pipe(
        Effect.as({
          run: { id: boxLaterRunId, status: "running" },
          delivery: "started",
        } as never),
      ),
  });
  const layerBoxLaunches = Layer.mock(ThreadLaunch.ThreadLaunchService)({
    checkWorktreeBase: () => Effect.void,
    launch: (input) =>
      Ref.get(thread.runless).pipe(
        Effect.flatMap((runless) =>
          Ref.set(thread.launched, input.threadId!).pipe(
            Effect.andThen(Ref.set(thread.linkOrigin, input.linkOrigin ?? null)),
            Effect.andThen(Ref.set(thread.delegatedFrom, input.delegatedFrom ?? null)),
            Effect.andThen(Ref.set(thread.message, input.initialMessage?.text ?? null)),
            Effect.andThen(Ref.set(thread.workspace, input.workspaceStrategy)),
            Effect.andThen(
              Ref.set(thread.launchedWith, {
                modelSelection: input.modelSelection,
                runtimeMode: input.runtimeMode,
                interactionMode: input.interactionMode,
              }),
            ),
            Effect.andThen(Effect.flatten(Ref.get(thread.afterLaunch))),
            Effect.as({
              threadId: input.threadId,
              projection: {
                thread: {
                  id: input.threadId,
                  projectId: input.projectId,
                  title: input.title,
                  modelSelection: input.modelSelection,
                },
                runs:
                  input.initialMessage?.messageId && !runless
                    ? [
                        {
                          id: runId,
                          status: "running",
                          userMessageId: input.initialMessage.messageId,
                        },
                      ]
                    : [],
              },
              resumed: false,
            } as unknown as ThreadLaunch.ThreadLaunchResult),
          ),
        ),
      ),
  });
  return Layer.mergeAll(
    McpHttpServer.layerOrchestratorToolkit,
    McpHttpServer.layerProjectRegistration,
    McpHttpServer.layerThreadToolkit,
  ).pipe(
    Layer.provide(DocumentText.layer.pipe(Layer.provideMerge(NodeServices.layer))),
    Layer.provide(NodeCrypto.layer),
    Layer.provide(layerBoxThreads),
    Layer.provide(layerBoxLaunches),
    Layer.provide(Layer.mock(RemoteDelegation.RemoteDelegation)({})),
    Layer.provide(Layer.mock(PeerForwarding.PeerForwarding)({})),
    Layer.provide(Layer.mock(PeerLinkRequests.PeerLinkRequests)({})),
    Layer.provide(Layer.mock(PeerLinks.PeerLinks)({})),
    Layer.provide(
      Layer.mock(ManagedProjectFolders.ManagedProjectFolders)({
        namedProjectsRoot: "/p",
      }),
    ),
    Layer.provide(Layer.mock(GitVcsDriver.GitVcsDriver)({})),
    Layer.provide(Layer.mock(SourceControlRepositoryService.SourceControlRepositoryService)({})),
    Layer.provide(
      ServerConfig.layerTest(process.cwd(), { prefix: "t3-remote-delegation-box-" }).pipe(
        Layer.provide(NodeServices.layer),
      ),
    ),
    Layer.provide(
      Layer.mock(ProviderRegistry.ProviderRegistry)({ getProviders: Effect.succeed([]) }),
    ),
    Layer.provide(Layer.mock(ProviderAdapterRegistry.ProviderAdapterRegistryV2)({})),
    Layer.provide(Layer.mock(ScheduledTaskService.ScheduledTaskService)({})),
    Layer.provide(Layer.mock(ThreadSearch.ThreadSearch)({})),
    Layer.provide(
      Layer.mock(ProjectService.ProjectService)({
        getById: () => Effect.succeedNone,
        // More than a page, so finding the project with this repository pages.
        listPage: ({ cursor, limit }) =>
          Effect.succeed({
            projects: boxProjects.slice(cursor, cursor + limit),
            nextCursor: cursor + limit < boxProjects.length ? cursor + limit : null,
          }),
      }),
    ),
    Layer.provide(
      Layer.mergeAll(
        Layer.mock(ProjectCloneTracker.ProjectCloneTracker)({}),
        Layer.mock(SecretRequests.SecretRequests)({}),
      ),
    ),
    Layer.provide(ServerSettings.layerTest()),
    Layer.provide(NodeServices.layer),
  );
};

const mcpClient = McpSchema.McpServerClient.of({
  clientId: 1,
  protocolVersion: "2025-06-18",
  clientCapabilities: {},
  clientInfo: { name: "remote-delegation", version: "1" },
  initializePayload: {
    protocolVersion: "2025-06-18",
    capabilities: {},
    clientInfo: { name: "remote-delegation", version: "1" },
  },
  getClient: Effect.die("unused"),
});

const adapter = {
  instanceId,
  driver,
  getCapabilities: () => Effect.succeed(CodexProviderCapabilitiesV2),
  planSelectionTransition: () => Effect.succeed({ type: "apply_on_next_turn" as const }),
  openSession: () => Effect.die("No provider process needed"),
} as ProviderAdapter.ProviderAdapterV2["Service"];

/**
 * The laptop: a real orchestrator (in-memory SQLite) whose parent thread has
 * a live run, the real orchestrator toolkit, and RemoteDelegation. Built in
 * its own scope so a test can tear it down and build it again on the same
 * database, as a restart does.
 */
const makeLaptop = (
  database: Context.Context<SqlClient.SqlClient>,
  offers: SubscriptionRef.SubscriptionRef<
    ReadonlyArray<ProviderContinuationRequests.ProviderContinuationRequest>
  >,
  /** The laptop's links from before a restart, which keep their sessions there. */
  kept?: Context.Context<Layer.Success<ReturnType<typeof layerLinkingEnvironment>>>,
  hooks: {
    readonly afterPendingQuestions?: Effect.Effect<void>;
    readonly beforeWait?: Effect.Effect<void, OrchestratorMcpFailure>;
    readonly afterCall?: (name: string) => Effect.Effect<void>;
    readonly queueNotifications?: boolean;
    readonly afterRecords?: (
      fields: ReadonlyArray<ProjectionStore.ProjectionRecordField>,
      filter: ProjectionStore.ProjectionRecordFilter | undefined,
    ) => Effect.Effect<void>;
    readonly beforeQuestionCancel?: (
      runId: RunId,
      threads: ThreadManagement.ThreadManagementService["Service"],
    ) => Effect.Effect<void>;
  } = {},
) =>
  Effect.gen(function* () {
    const linking = kept ?? (yield* layerLinkingEnvironment(laptop).pipe(Layer.build));
    const layerDatabase = Layer.succeedContext(database);
    const layerOrchestrator = Layer.mergeAll(
      layerDatabase,
      ProjectionStore.layer.pipe(Layer.provide(layerDatabase)),
      ProviderReplayHarness.layerWithRegistry(
        { name: "remote-delegation-laptop" },
        ProviderAdapterRegistry.layerFromAdapters([
          hooks.queueNotifications
            ? {
                ...adapter,
                getCapabilities: () =>
                  Effect.succeed({
                    ...CodexProviderCapabilitiesV2,
                    turns: { ...CodexProviderCapabilitiesV2.turns, supportsActiveSteering: false },
                  }),
              }
            : adapter,
        ]),
        { databaseLayer: layerDatabase, runEffectWorker: false },
      ),
    ).pipe(
      Layer.provide(
        Layer.succeed(ProviderContinuationRequests.ProviderContinuationRequests, {
          offer: (request) => SubscriptionRef.update(offers, (seen) => [...seen, request]),
          take: Effect.never,
        }),
      ),
    );
    const layerThreads = ThreadManagement.layer.pipe(
      Layer.provide(CommandReceiptStore.layer.pipe(Layer.provide(layerDatabase))),
      Layer.provideMerge(Layer.merge(layerOrchestrator, ServerSettings.layerTest())),
    );
    const layerHere = Layer.merge(
      McpHttpServer.layerOrchestratorToolkit,
      McpHttpServer.layerThreadToolkit,
    ).pipe(
      Layer.provideMerge(McpHttpServer.layerInProcessMcpServer),
      Layer.provideMerge(
        RemoteDelegation.layer.pipe(
          Layer.provide(
            Layer.effect(
              ThreadManagement.ThreadManagementService,
              Effect.gen(function* () {
                const threads = yield* ThreadManagement.ThreadManagementService;
                return ThreadManagement.ThreadManagementService.of({
                  ...threads,
                  getThreadRecords: (threadId, fields, filter) =>
                    threads
                      .getThreadRecords(threadId, fields, filter)
                      .pipe(Effect.tap(() => hooks.afterRecords?.(fields, filter) ?? Effect.void)),
                  dispatch: (command) =>
                    Effect.suspend(() =>
                      command.type === "queued-run.cancel"
                        ? (hooks.beforeQuestionCancel?.(command.runId, threads) ?? Effect.void)
                        : Effect.void,
                    ).pipe(Effect.andThen(threads.dispatch(command))),
                });
              }),
            ),
          ),
          Layer.provideMerge(
            Layer.effect(
              PeerForwarding.PeerForwarding,
              Effect.gen(function* () {
                const forwarding = yield* PeerForwarding.PeerForwarding;
                return PeerForwarding.PeerForwarding.of({
                  ...forwarding,
                  call: (scope, tool, environmentId, params) =>
                    forwarding
                      .call(scope, tool, environmentId, params)
                      .pipe(Effect.tap(() => hooks.afterCall?.(tool.name) ?? Effect.void)),
                  waitForThread: (...args) =>
                    (hooks.beforeWait ?? Effect.void).pipe(
                      Effect.andThen(forwarding.waitForThread(...args)),
                    ),
                });
              }),
            ).pipe(Layer.provide(PeerForwarding.layer)),
          ),
          Layer.provide(
            Layer.mock(ProjectService.ProjectService)({
              // A cold enrichment cache: the project itself reports no identity yet.
              getById: () =>
                Effect.succeedSome({
                  workspaceRoot: "/home/me/app",
                  repositoryIdentity: null,
                } as never),
            }),
          ),
          Layer.provide(
            Layer.mock(RepositoryIdentityResolver.RepositoryIdentityResolver)({
              resolve: (cwd) =>
                Effect.succeed(
                  cwd === "/home/me/app"
                    ? ({ canonicalKey: "github.com/acme/app" } as RepositoryIdentity)
                    : null,
                ),
            }),
          ),
          (layer) =>
            Layer.effect(
              RemoteDelegation.RemoteDelegation,
              Effect.gen(function* () {
                const remote = yield* RemoteDelegation.RemoteDelegation;
                return RemoteDelegation.RemoteDelegation.of({
                  ...remote,
                  pendingQuestions: (...args) =>
                    remote
                      .pendingQuestions(...args)
                      .pipe(Effect.tap(() => hooks.afterPendingQuestions ?? Effect.void)),
                });
              }),
            ).pipe(Layer.provideMerge(layer)),
        ),
      ),
      Layer.provide(Layer.succeedContext(linking)),
      Layer.provide(Layer.mock(PeerLinkRequests.PeerLinkRequests)({})),
      Layer.provide(NodeCrypto.layer),
      Layer.provideMerge(layerThreads),
      Layer.provide(Layer.mock(ProviderRegistry.ProviderRegistry)({})),
      Layer.provide(Layer.mock(ProviderAdapterRegistry.ProviderAdapterRegistryV2)({})),
      Layer.provide(Layer.mock(ScheduledTaskService.ScheduledTaskService)({})),
      Layer.provide(Layer.mock(ProjectCloneTracker.ProjectCloneTracker)({})),
      Layer.provide(Layer.mock(ThreadSearch.ThreadSearch)({})),
      Layer.provide(
        Layer.mock(ProjectService.ProjectService)({
          getById: () => Effect.succeedSome({ workspaceRoot: "/home/me/app" } as never),
        }),
      ),
      Layer.provide(Layer.mock(SecretRequests.SecretRequests)({})),
      Layer.provide(Layer.mock(ThreadLaunch.ThreadLaunchService)({})),
      Layer.provide(
        Layer.mock(GitVcsDriver.GitVcsDriver)({
          resolveCommit: () => Effect.succeed({ commitSha: parentCommit } as never),
        }),
      ),
      Layer.fresh,
    );
    const here = yield* Layer.build(layerHere);
    const server = Context.get(here, McpServer.McpServer);
    return {
      links: Context.get(linking, PeerLinks.PeerLinks),
      settings: Context.get(linking, ServerSettings.ServerSettingsService),
      orchestrator: Context.get(here, Orchestrator.OrchestratorV2),
      remote: Context.get(here, RemoteDelegation.RemoteDelegation),
      threads: Context.get(here, ThreadManagement.ThreadManagementService),
      projections: Context.get(here, ProjectionStore.ProjectionStoreV2),
      sink: Context.get(here, EventSink.EventSinkV2),
      call: (name: string, args: Record<string, unknown>, scope = parentScope) =>
        server
          .callTool({ name, arguments: args })
          .pipe(
            Effect.provideService(McpInvocationContext.McpInvocationContext, scope),
            Effect.provideService(McpSchema.McpServerClient, mcpClient),
          ),
    };
  });

const parentThreadId = ThreadId.make("thread:laptop-parent");
/** The commit the parent's checkout is at. */
const parentCommit = "0123456789abcdef0123456789abcdef01234567";
const parentRunId = RunId.make("run:laptop-parent");
const parentRootNode = NodeId.make("node:laptop-parent:root");
const parentScope: McpInvocationContext.McpInvocationScope = {
  environmentId: laptop.environmentId,
  requestNamespace: "provider-session:laptop-parent",
  thread: {
    threadId: parentThreadId,
    providerSessionId: "provider-session:laptop-parent",
    providerInstanceId: instanceId,
  },
  client: undefined,
  capabilities: new Set(["orchestration"]),
  issuedAt: 1,
};

/** The laptop's parent thread, mid-turn when its agent delegates. */
const seedParent = (
  laptopEnv: Effect.Success<ReturnType<typeof makeLaptop>>,
  parentThreadId: ThreadId = parentScope.thread!.threadId,
) =>
  Effect.gen(function* () {
    const now = yield* DateTime.now;
    const providerThreadId = ProviderThreadId.make("provider-thread:laptop-parent");
    yield* laptopEnv.orchestrator.dispatch({
      type: "thread.create",
      createdBy: "user",
      creationSource: "web",
      commandId: CommandId.make("command:create:laptop-parent"),
      threadId: parentThreadId,
      projectId: ProjectId.make("project:laptop"),
      title: "Laptop parent",
      modelSelection,
      runtimeMode: "full-access",
      interactionMode: "default",
      branch: null,
      worktreePath: null,
    });
    yield* laptopEnv.sink.write({
      commandId: CommandId.make("command:seed:laptop-parent"),
      events: [
        {
          id: EventId.make("event:seed-provider-thread:laptop-parent"),
          type: "provider-thread.updated",
          threadId: parentThreadId,
          driver,
          providerInstanceId: instanceId,
          occurredAt: now,
          payload: {
            id: providerThreadId,
            driver,
            providerInstanceId: instanceId,
            providerSessionId: null,
            appThreadId: parentThreadId,
            ownerNodeId: parentRootNode,
            nativeThreadRef: { driver, nativeId: "native:laptop-parent", strength: "strong" },
            nativeConversationHeadRef: null,
            status: "active",
            firstRunOrdinal: 1,
            lastRunOrdinal: 1,
            handoffIds: [],
            forkedFrom: null,
            createdAt: now,
            updatedAt: now,
          },
        },
        {
          id: EventId.make("event:seed-node:laptop-parent"),
          type: "node.updated",
          threadId: parentThreadId,
          runId: parentRunId,
          nodeId: parentRootNode,
          driver,
          providerInstanceId: instanceId,
          occurredAt: now,
          payload: {
            id: parentRootNode,
            threadId: parentThreadId,
            runId: parentRunId,
            parentNodeId: null,
            rootNodeId: parentRootNode,
            kind: "root_turn",
            status: "running",
            countsForRun: true,
            providerThreadId,
            providerTurnId: null,
            nativeItemRef: null,
            runtimeRequestId: null,
            checkpointScopeId: null,
            startedAt: now,
            completedAt: null,
          },
        },
        {
          id: EventId.make("event:seed-run:laptop-parent"),
          type: "run.updated",
          threadId: parentThreadId,
          runId: parentRunId,
          nodeId: parentRootNode,
          providerInstanceId: instanceId,
          occurredAt: now,
          payload: {
            id: parentRunId,
            threadId: parentThreadId,
            ordinal: 1,
            providerInstanceId: instanceId,
            modelSelection,
            providerThreadId,
            userMessageId: MessageId.make("message:seed-user:laptop-parent"),
            rootNodeId: parentRootNode,
            activeAttemptId: null,
            status: "running",
            requestedAt: now,
            startedAt: now,
            completedAt: null,
            checkpointId: null,
            contextHandoffId: null,
          },
        },
      ],
    });
  });

const delegateToBox = (laptopEnv: Effect.Success<ReturnType<typeof makeLaptop>>) =>
  laptopEnv
    .call("delegate_task", {
      task: "Run the full suite on the box.",
      target: {
        environmentId: box.environmentId,
        providerInstanceId: instanceId,
        model: "gpt-5.4",
      },
      mode: "async",
      clientRequestId: "suite-on-box",
    })
    .pipe(
      Effect.map((result) => {
        expect(result.isError, JSON.stringify(result.content)).toBe(false);
        return result.structuredContent as {
          taskId: NodeId;
          status: string;
          childThreadId: null;
          remoteChild: { environmentId: string; threadId: ThreadId; label: string };
          link?: string;
        };
      }),
    );

/** Waits until the task here has the result its thread there ended with. */
const taskResult = (laptopEnv: Effect.Success<ReturnType<typeof makeLaptop>>, taskId: NodeId) =>
  laptopEnv.orchestrator
    .streamStoredEventsFrom({
      threadId: parentThreadId,
      afterSequence: 0,
      eventType: "subagent.updated",
    })
    .pipe(
      Stream.filter(
        (stored) =>
          stored.event.type === "subagent.updated" &&
          stored.event.payload.id === taskId &&
          stored.event.payload.result !== null,
      ),
      Stream.runHead,
      Effect.map((stored) =>
        Option.isSome(stored) && stored.value.event.type === "subagent.updated"
          ? stored.value.event.payload
          : undefined,
      ),
    );

it.effect("a maximum-length launch key stays bounded and retries the same remote task", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const thread = yield* makeBoxThread;
      const b = yield* serveBox(thread);
      const offers = yield* SubscriptionRef.make<
        ReadonlyArray<ProviderContinuationRequests.ProviderContinuationRequest>
      >([]);
      const database = yield* SqlitePersistence.layerMemory.pipe(Layer.build);
      const a = yield* makeLaptop(database, offers);
      yield* linkTo(a.links, b, "full-access");
      const longThreadId = ThreadId.make("t".repeat(256));
      yield* seedParent(a, longThreadId);
      const scope = { ...parentScope, thread: { ...parentScope.thread!, threadId: longThreadId } };
      const input = {
        task: "Run it.",
        target: { environmentId: box.environmentId },
        clientRequestId: "x".repeat(256),
      };
      const first = yield* a.call("delegate_task", input, scope);
      const retry = yield* a.call("delegate_task", input, scope);
      expect(first.isError, JSON.stringify(first.content)).toBe(false);
      expect(retry.isError, JSON.stringify(retry.content)).toBe(false);
      expect(retry.structuredContent).toMatchObject({
        taskId: (first.structuredContent as { taskId: NodeId }).taskId,
      });
      expect((yield* a.orchestrator.getThreadProjection(longThreadId)).subagents).toHaveLength(1);
    }),
  ).pipe(Effect.provide(NodeHttpServer.layerTest)),
);

it.effect("a task delegated to the box wakes the laptop's parent when it ends there", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const thread = yield* makeBoxThread;
      const b = yield* serveBox(thread);
      const offers = yield* SubscriptionRef.make<
        ReadonlyArray<ProviderContinuationRequests.ProviderContinuationRequest>
      >([]);
      const database = yield* SqlitePersistence.layerMemory.pipe(Layer.build);
      const a = yield* makeLaptop(database, offers);
      yield* linkTo(a.links, b, "full-access");
      yield* seedParent(a);

      const delegated = yield* delegateToBox(a);
      expect(delegated).toMatchObject({
        status: "running",
        childThreadId: null,
        remoteChild: { environmentId: box.environmentId, label: "Box" },
      });
      // The box launched it in the project with the same repository.
      expect(yield* Ref.get(thread.launched)).toBe(delegated.remoteChild.threadId);
      // Its link opens the thread there, so the parent can mention it like any other.
      expect(delegated.link).toBe(
        `[Laptop parent](t3-thread://v2/${box.environmentId}/${delegated.remoteChild.threadId})`,
      );
      // ...naming the parent here, so the thread there links back to it.
      expect(yield* Ref.get(thread.delegatedFrom)).toEqual({
        environmentId: laptop.environmentId,
        threadId: parentThreadId,
        title: "Laptop parent",
      });
      // A retry with the same key launches nothing new and records nothing new.
      const retried = yield* delegateToBox(a);
      expect(retried.taskId).toBe(delegated.taskId);
      expect((yield* a.orchestrator.getThreadProjection(parentThreadId)).subagents).toHaveLength(1);

      const historyReads: Array<string> = [];
      const historyTracer = Tracer.make({
        span(options) {
          const span = new Tracer.NativeSpan(options);
          const end = span.end.bind(span);
          span.end = (endTime, exit) => {
            end(endTime, exit);
            const query = span.attributes.get("db.query.text");
            if (
              typeof query === "string" &&
              /\bSELECT\s+payload_json\s+FROM orchestration_v2_projection_(?:runs|context_transfers)\b/i.test(
                query,
              )
            ) {
              historyReads.push(query);
            }
          };
          return span;
        },
      });
      const running = yield* a
        .call("task_status", { taskId: delegated.taskId })
        .pipe(Effect.withTracer(historyTracer));
      expect(running.structuredContent).toMatchObject({
        taskId: delegated.taskId,
        childThreadId: null,
        remoteChild: delegated.remoteChild,
        status: "running",
        workState: "working",
        summary: null,
        hasPendingQuestion: false,
        pendingApproval: null,
      });
      // A running task there needs its local row, not the parent's run or transfer history.
      expect(historyReads).toEqual([]);

      yield* Ref.set(thread.reply, "All 412 tests pass on the box.");
      yield* Ref.set(thread.status, "completed");
      yield* Deferred.succeed(thread.finished, undefined);
      const task = yield* taskResult(a, delegated.taskId);
      expect(task).toMatchObject({ status: "completed", result: "All 412 tests pass on the box." });
      // The parent's live run claims the result, as for a child here.
      const parentRun = (yield* a.orchestrator.getThreadProjection(parentThreadId)).runs.find(
        (run) => run.id === parentRunId,
      );
      expect(parentRun?.delegatedCompletion?.delivery?.taskIds).toEqual([delegated.taskId]);
      // The wake is offered after the result commits, so the result can be seen first.
      const offered = yield* SubscriptionRef.changes(offers).pipe(
        Stream.filter((seen) => seen.some((offer) => offer.threadId === parentThreadId)),
        Stream.runHead,
      );
      expect(Option.isSome(offered)).toBe(true);

      const messageReads: Array<string> = [];
      const tracer = Tracer.make({
        span(options) {
          const span = new Tracer.NativeSpan(options);
          const end = span.end.bind(span);
          span.end = (endTime, exit) => {
            end(endTime, exit);
            const query = span.attributes.get("db.query.text");
            if (
              typeof query === "string" &&
              /SELECT[\s\S]*FROM orchestration_v2_projection_messages/i.test(query)
            ) {
              messageReads.push(query);
            }
          };
          return span;
        },
      });
      const status = yield* a
        .call("task_status", { taskId: delegated.taskId })
        .pipe(Effect.withTracer(tracer));
      expect(status.structuredContent).toMatchObject({
        status: "completed",
        summary: "All 412 tests pass on the box.",
      });
      // Delivery validation still reads messages; choosing the observer does not need another read.
      expect(messageReads).toHaveLength(2);
      expect(messageReads.filter((query) => !/\bmessage_id\s+IN\b/i.test(query))).toHaveLength(1);
      const acknowledged = yield* a.projections.getThreadRecords(parentThreadId, ["subagents"], {
        subagentIds: [delegated.taskId],
      });
      expect(acknowledged.subagents[0]?.completionDelivery).toEqual({
        state: "acknowledged",
        observedByRunId: parentRunId,
      });
    }),
  ).pipe(Effect.provide(NodeHttpServer.layerTest)),
);

it.effect.each(["no active parent run", "a newer run owned by another provider"] as const)(
  "preserves remote observation guards with %s",
  (observer) =>
    Effect.scoped(
      Effect.gen(function* () {
        const thread = yield* makeBoxThread;
        const b = yield* serveBox(thread);
        const offers = yield* SubscriptionRef.make<
          ReadonlyArray<ProviderContinuationRequests.ProviderContinuationRequest>
        >([]);
        const database = yield* SqlitePersistence.layerMemory.pipe(Layer.build);
        const a = yield* makeLaptop(database, offers);
        yield* linkTo(a.links, b, "full-access");
        yield* seedParent(a);
        const delegated = yield* delegateToBox(a);

        yield* Ref.set(thread.reply, "Remote result remains available.");
        yield* Ref.set(thread.status, "completed");
        yield* Deferred.succeed(thread.finished, undefined);
        yield* taskResult(a, delegated.taskId);
        yield* SubscriptionRef.changes(offers).pipe(
          Stream.filter((seen) => seen.some((offer) => offer.threadId === parentThreadId)),
          Stream.runHead,
        );

        const parent = yield* a.projections.getThreadRecords(
          parentThreadId,
          ["runs", "subagents"],
          {
            subagentIds: [delegated.taskId],
          },
        );
        expect(parent.subagents[0]?.completionDelivery?.state).not.toBe("acknowledged");
        const parentRun = parent.runs.find((run) => run.id === parentRunId);
        if (parentRun === undefined) return yield* Effect.die("The parent run was not seeded.");
        const now = yield* DateTime.now;
        const otherInstanceId = ProviderInstanceId.make("codex-other");
        const observingRun: OrchestrationV2Run =
          observer === "no active parent run"
            ? { ...parentRun, status: "completed", completedAt: now }
            : {
                id: RunId.make("run:laptop-other-provider"),
                threadId: parentThreadId,
                ordinal: parentRun.ordinal + 1,
                providerInstanceId: otherInstanceId,
                modelSelection: { instanceId: otherInstanceId, model: "gpt-5.4" },
                providerThreadId: null,
                userMessageId: MessageId.make("message:laptop-other-provider"),
                rootNodeId: null,
                activeAttemptId: null,
                status: "preparing",
                requestedAt: now,
                startedAt: null,
                completedAt: null,
                checkpointId: null,
                contextHandoffId: null,
              };
        yield* a.sink.write({
          commandId: CommandId.make("command:change-remote-result-observer"),
          events: [
            {
              id: EventId.make("event:change-remote-result-observer"),
              type: "run.updated",
              threadId: parentThreadId,
              runId: observingRun.id,
              providerInstanceId: observingRun.providerInstanceId,
              occurredAt: now,
              payload: observingRun,
            },
          ],
        });

        const status = yield* a.call("task_status", { taskId: delegated.taskId });
        if (observer === "no active parent run") {
          expect(status.isError).toBe(true);
          expect(status.content).toEqual([
            {
              type: "text",
              text: JSON.stringify({
                _tag: "OrchestratorMcpFailure",
                code: "parent_not_active",
                message: "The calling provider no longer owns an active thread run.",
              }),
            },
          ]);
          const unacknowledged = yield* a.projections.getThreadRecords(
            parentThreadId,
            ["subagents"],
            { subagentIds: [delegated.taskId] },
          );
          expect(unacknowledged.subagents[0]?.result).toBe("Remote result remains available.");
          expect(unacknowledged.subagents[0]?.completionDelivery?.state).not.toBe("acknowledged");
          return;
        }
        expect(status.isError, JSON.stringify(status.content)).not.toBe(true);
        expect(status.structuredContent).toMatchObject({
          status: "completed",
          summary: "Remote result remains available.",
        });
        const acknowledged = yield* a.projections.getThreadRecords(parentThreadId, ["subagents"], {
          subagentIds: [delegated.taskId],
        });
        expect(acknowledged.subagents[0]?.completionDelivery).toEqual({
          state: "acknowledged",
          observedByRunId: parentRunId,
        });
      }),
    ).pipe(Effect.provide(NodeHttpServer.layerTest)),
);

it.effect(
  "with the beta off here, a task already running there still ends, and no new one starts",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const thread = yield* makeBoxThread;
        const b = yield* serveBox(thread);
        const offers = yield* SubscriptionRef.make<
          ReadonlyArray<ProviderContinuationRequests.ProviderContinuationRequest>
        >([]);
        const database = yield* SqlitePersistence.layerMemory.pipe(Layer.build);
        const a = yield* makeLaptop(database, offers);
        yield* linkTo(a.links, b, "full-access");
        yield* seedParent(a);
        const delegated = yield* delegateToBox(a);

        yield* a.settings.updateSettings({ linkedEnvironmentsEnabled: false });
        const refused = yield* a.call("delegate_task", {
          task: "Run the linter on the box.",
          target: {
            environmentId: box.environmentId,
            providerInstanceId: instanceId,
            model: "gpt-5.4",
          },
          mode: "async",
          clientRequestId: "lint-on-box",
        });
        expect(refused.isError).toBe(true);
        expect(JSON.stringify(refused.content)).toContain(
          PeerForwarding.linkedEnvironmentsOff.message,
        );
        expect((yield* a.orchestrator.getThreadProjection(parentThreadId)).subagents).toHaveLength(
          1,
        );

        yield* Ref.set(thread.reply, "All 412 tests pass on the box.");
        yield* Ref.set(thread.status, "completed");
        yield* Deferred.succeed(thread.finished, undefined);
        expect(yield* taskResult(a, delegated.taskId)).toMatchObject({
          status: "completed",
          result: "All 412 tests pass on the box.",
        });
      }),
    ).pipe(Effect.provide(NodeHttpServer.layerTest)),
);

it.effect("a task named only by environment inherits the parent's model, within the link", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const thread = yield* makeBoxThread;
      const b = yield* serveBox(thread);
      const offers = yield* SubscriptionRef.make<
        ReadonlyArray<ProviderContinuationRequests.ProviderContinuationRequest>
      >([]);
      const database = yield* SqlitePersistence.layerMemory.pipe(Layer.build);
      const a = yield* makeLaptop(database, offers);
      // The parent runs with full access; the link allows less.
      yield* linkTo(a.links, b, "auto-accept-edits");
      yield* seedParent(a);

      const result = yield* a.call("delegate_task", {
        task: "Run the suite on the box.",
        target: { environmentId: box.environmentId },
      });
      expect(result.isError, JSON.stringify(result.content)).toBe(false);
      // The parent's model, and the link's modes rather than the parent's.
      expect(yield* Ref.get(thread.launchedWith)).toEqual({
        modelSelection,
        runtimeMode: "auto-accept-edits",
        interactionMode: "default",
      });

      // Asking for more than the link allows is still refused there.
      const broader = yield* a.call("delegate_task", {
        task: "Run the suite on the box.",
        target: { environmentId: box.environmentId },
        runtimeMode: "full-access",
        clientRequestId: "broader",
      });
      expect(broader.isError).toBe(true);
    }),
  ).pipe(Effect.provide(NodeHttpServer.layerTest)),
);

it.effect.each([
  ["worktree", { type: "worktree", baseRef: parentCommit, startFromOrigin: true }],
  ["rift", { type: "rift" }],
  ["shared", null],
] as const)("a %s task there gets the matching workspace there", ([isolation, expected]) =>
  Effect.scoped(
    Effect.gen(function* () {
      const thread = yield* makeBoxThread;
      const b = yield* serveBox(thread);
      const offers = yield* SubscriptionRef.make<
        ReadonlyArray<ProviderContinuationRequests.ProviderContinuationRequest>
      >([]);
      const database = yield* SqlitePersistence.layerMemory.pipe(Layer.build);
      const a = yield* makeLaptop(database, offers);
      yield* linkTo(a.links, b, "auto");
      yield* seedParent(a);

      const result = yield* a.call("delegate_task", {
        task: "Run the suite on the box.",
        target: { environmentId: box.environmentId },
        isolation,
      });
      expect(result.isError, JSON.stringify(result.content)).toBe(false);
      // A worktree there starts at this checkout's commit, fetched from origin.
      const workspace = yield* Ref.get(thread.workspace);
      if (expected === null) expect(workspace?.type ?? "root").toBe("root");
      else expect(workspace).toEqual(expected);
    }),
  ).pipe(Effect.provide(NodeHttpServer.layerTest)),
);

/** The persisted queued wake, not a timer or a projection poll. */
const questionWake = (a: Effect.Success<ReturnType<typeof makeLaptop>>, requestId?: string) =>
  a.orchestrator
    .streamStoredEventsFrom({
      threadId: parentThreadId,
      afterSequence: 0,
      eventType: "run.created",
    })
    .pipe(
      Stream.filter(
        (stored) =>
          stored.event.type === "run.created" &&
          stored.event.payload.status === "queued" &&
          stored.event.payload.userMessageId.startsWith("message:remote-task-question:") &&
          (requestId === undefined || stored.event.payload.userMessageId.endsWith(`:${requestId}`)),
      ),
      Stream.runHead,
      Effect.map((stored) => {
        if (Option.isNone(stored) || stored.value.event.type !== "run.created")
          throw new Error("No question wake.");
        return stored.value.event.payload.id;
      }),
    );

const cancelledRun = (a: Effect.Success<ReturnType<typeof makeLaptop>>, runId: RunId) =>
  a.orchestrator
    .streamStoredEventsFrom({
      threadId: parentThreadId,
      afterSequence: 0,
      eventType: "run.updated",
    })
    .pipe(
      Stream.filter(
        (stored) =>
          stored.event.type === "run.updated" &&
          stored.event.payload.id === runId &&
          stored.event.payload.status === "cancelled",
      ),
      Stream.runHead,
    );

it.effect("a timed-out question probe keeps the task handle and promotes wake delivery", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const thread = yield* makeBoxThread;
      const b = yield* serveBox(thread);
      const offers = yield* SubscriptionRef.make<
        ReadonlyArray<ProviderContinuationRequests.ProviderContinuationRequest>
      >([]);
      const database = yield* SqlitePersistence.layerMemory.pipe(Layer.build);
      const firstPoll = yield* Deferred.make<void>();
      const secondPoll = yield* Deferred.make<void>();
      const reads: Array<{
        readonly fields: ReadonlyArray<ProjectionStore.ProjectionRecordField>;
        readonly filter: ProjectionStore.ProjectionRecordFilter | undefined;
      }> = [];
      let runReads = 0;
      const a = yield* makeLaptop(database, offers, undefined, {
        // Keep the blocking delegate and follower waiting while questions poll.
        beforeWait: Effect.never,
        afterRecords: (fields, filter) =>
          Effect.gen(function* () {
            reads.push({ fields, filter });
            if (!fields.includes("runs")) return;
            runReads++;
            // The first read selects the delegating parent run, before polling.
            if (runReads === 2) yield* Deferred.succeed(firstPoll, undefined);
            if (runReads === 3) yield* Deferred.succeed(secondPoll, undefined);
          }),
      });
      yield* linkTo(a.links, b, "full-access");
      yield* seedParent(a);
      const waiting = yield* a
        .call("delegate_task", {
          task: "Run the full suite on the box.",
          target: { environmentId: box.environmentId, providerInstanceId: instanceId },
          mode: "wait",
          timeoutMs: 600_000,
          clientRequestId: "scoped-question-poll",
        })
        .pipe(Effect.forkScoped);
      yield* Deferred.await(firstPoll);
      yield* TestClock.adjust(Duration.seconds(15));
      yield* Deferred.await(secondPoll);
      const projection = yield* a.orchestrator.getThreadProjection(parentThreadId);
      const task = projection.subagents[0];
      const result = yield* Fiber.join(waiting);
      expect(result.isError).not.toBe(true);
      expect(result.structuredContent).toMatchObject({
        taskId: task?.id,
        remoteChild: {
          environmentId: task?.remoteChild?.environmentId,
          threadId: task?.remoteChild?.threadId,
        },
        status: "running",
        summary: expect.stringContaining("questions could not be checked"),
      });
      expect(task?.completionWake).toBe("always");
      // The last read belongs to the wake-policy command, not the question poll.
      expect(reads.at(-1)).toEqual({ fields: ["runs"], filter: undefined });
      const pollingReads = reads.slice(1, -1);
      expect(pollingReads.filter((read) => read.fields.includes("runs"))).toHaveLength(1);
      for (const read of pollingReads) {
        if (read.fields.includes("subagents")) {
          expect(read.filter?.subagentIds).toEqual([task?.id]);
        }
        if (read.fields.includes("runs")) {
          expect(read.filter?.runIds).toEqual([parentRunId]);
        }
      }
    }),
  ).pipe(Effect.provide(NodeHttpServer.layerTest)),
);

it.effect("a question from a task there reaches the parent here and is answered there", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const thread = yield* makeBoxThread;
      const b = yield* serveBox(thread);
      const offers = yield* SubscriptionRef.make<
        ReadonlyArray<ProviderContinuationRequests.ProviderContinuationRequest>
      >([]);
      const database = yield* SqlitePersistence.layerMemory.pipe(Layer.build);
      const a = yield* makeLaptop(database, offers);
      yield* linkTo(a.links, b, "full-access");
      yield* seedParent(a);

      yield* Ref.set(thread.questions, [{ id: "request:box-1", question: "Which suite?" }]);
      const delegated = yield* delegateToBox(a);
      const remote = delegated.remoteChild;

      const questionMessages = Effect.gen(function* () {
        const projection = yield* a.orchestrator.getThreadProjection(parentThreadId);
        return projection.messages.filter((message) => message.text.includes("request:box-1"));
      });
      yield* a.orchestrator
        .streamStoredEventsFrom({
          threadId: parentThreadId,
          afterSequence: 0,
          eventType: "message.updated",
        })
        .pipe(
          Stream.filter(
            (stored) =>
              stored.event.type === "message.updated" &&
              stored.event.payload.text.includes("request:box-1"),
          ),
          Stream.runHead,
        );
      const [message] = yield* questionMessages;
      const wake = yield* questionWake(a);
      // The parent is told where the question is, so it can answer it there.
      expect(message?.text).toContain(box.environmentId);
      expect(message?.text).toContain(remote.threadId);

      const status = yield* a.call("task_status", { taskId: delegated.taskId });
      expect(status.structuredContent).toMatchObject({
        status: "running",
        hasPendingQuestion: true,
      });

      const listed = yield* a.call("t3_pending_request", {
        action: "list",
        environmentId: box.environmentId,
        threadId: remote.threadId,
      });
      expect(listed.structuredContent).toEqual({ requestIds: ["request:box-1"] });
      const read = yield* a.call("t3_pending_request", {
        action: "read",
        environmentId: box.environmentId,
        threadId: remote.threadId,
        requestId: "request:box-1",
      });
      expect(JSON.stringify(read.structuredContent)).toContain("Which suite?");
      const answered = yield* a.call("t3_pending_request", {
        action: "respond",
        environmentId: box.environmentId,
        threadId: remote.threadId,
        requestId: "request:box-1",
        answers: { q: "unit" },
      });
      expect(answered.isError, JSON.stringify(answered.content)).toBe(false);
      expect(yield* Ref.get(thread.answers)).toEqual([
        { requestId: "request:box-1", answers: { q: "unit" } },
      ]);

      // An answer there withdraws a wake this active parent has not consumed.
      yield* TestClock.adjust(Duration.seconds(15));
      yield* cancelledRun(a, wake);
      // Ordinary status probes do not cause another notification.
      yield* a.call("task_status", { taskId: delegated.taskId });
      expect(yield* questionMessages).toHaveLength(1);
      const after = yield* a.call("task_status", { taskId: delegated.taskId });
      expect(after.structuredContent).toMatchObject({ hasPendingQuestion: false });
    }),
  ).pipe(Effect.provide(NodeHttpServer.layerTest)),
);

it.effect.each(["completed", "failed", "cancelled"] as const)(
  "finishing a follower as %s withdraws its queued question wake",
  (status) =>
    Effect.scoped(
      Effect.gen(function* () {
        const thread = yield* makeBoxThread;
        const b = yield* serveBox(thread);
        const offers = yield* SubscriptionRef.make<
          ReadonlyArray<ProviderContinuationRequests.ProviderContinuationRequest>
        >([]);
        const database = yield* SqlitePersistence.layerMemory.pipe(Layer.build);
        const a = yield* makeLaptop(database, offers, undefined, { queueNotifications: true });
        yield* linkTo(a.links, b, "full-access");
        yield* seedParent(a);
        yield* Ref.set(thread.questions, [{ id: "request:box-1", question: "Which suite?" }]);
        const task = yield* delegateToBox(a);
        const wake = yield* questionWake(a);
        if (status === "cancelled") yield* a.call("task_cancel", { taskId: task.taskId });
        else {
          yield* Ref.set(thread.status, status);
          yield* Deferred.succeed(thread.finished, undefined);
        }
        expect(yield* taskResult(a, task.taskId)).toMatchObject({ status });
        yield* cancelledRun(a, wake);
        const projection = yield* a.orchestrator.getThreadProjection(parentThreadId);
        expect(projection.runs.find((run) => run.id === wake)?.status).toBe("cancelled");
      }),
    ).pipe(Effect.provide(NodeHttpServer.layerTest)),
);

it.effect.each(["running", "completed"] as const)(
  "a restarted %s follower withdraws a question wake queued before restart",
  (status) =>
    Effect.scoped(
      Effect.gen(function* () {
        const thread = yield* makeBoxThread;
        const b = yield* serveBox(thread);
        const offers = yield* SubscriptionRef.make<
          ReadonlyArray<ProviderContinuationRequests.ProviderContinuationRequest>
        >([]);
        const database = yield* SqlitePersistence.layerMemory.pipe(Layer.build);
        const linking = yield* layerLinkingEnvironment(laptop).pipe(Layer.build);
        const { task, wake } = yield* Effect.scoped(
          Effect.gen(function* () {
            const before = yield* makeLaptop(database, offers, linking, {
              queueNotifications: true,
            });
            yield* linkTo(before.links, b, "full-access");
            yield* seedParent(before);
            yield* Ref.set(thread.questions, [{ id: "request:box-1", question: "Which suite?" }]);
            const task = yield* delegateToBox(before);
            const wake = yield* questionWake(before);
            return { task, wake };
          }),
        );
        const after = yield* makeLaptop(database, offers, linking, { queueNotifications: true });
        // The remote answer arrives while the laptop is offline. The task
        // need not finish for its persisted, now-stale question wake to go away.
        yield* Ref.set(thread.questions, []);
        yield* Ref.set(thread.status, status);
        if (status === "completed") yield* Deferred.succeed(thread.finished, undefined);
        yield* after.remote.start();
        yield* cancelledRun(after, wake);
        if (status === "completed") yield* taskResult(after, task.taskId);
        else {
          const result = yield* after.call("task_status", { taskId: task.taskId });
          expect(result.structuredContent).toMatchObject({
            status: "running",
            hasPendingQuestion: false,
          });
        }
        const projection = yield* after.orchestrator.getThreadProjection(parentThreadId);
        expect(projection.runs.find((run) => run.id === wake)?.status).toBe("cancelled");
      }),
    ).pipe(Effect.provide(NodeHttpServer.layerTest)),
);

it.effect("recovered question cleanup tolerates a cancellation after its queue snapshot", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const thread = yield* makeBoxThread;
      const b = yield* serveBox(thread);
      const offers = yield* SubscriptionRef.make<
        ReadonlyArray<ProviderContinuationRequests.ProviderContinuationRequest>
      >([]);
      const database = yield* SqlitePersistence.layerMemory.pipe(Layer.build);
      const linking = yield* layerLinkingEnvironment(laptop).pipe(Layer.build);
      const wakes = yield* Effect.scoped(
        Effect.gen(function* () {
          const before = yield* makeLaptop(database, offers, linking, { queueNotifications: true });
          yield* linkTo(before.links, b, "full-access");
          yield* seedParent(before);
          yield* Ref.set(thread.questions, [
            { id: "request:race", question: "First?" },
            { id: "request:remaining", question: "Second?" },
          ]);
          yield* delegateToBox(before);
          return [
            yield* questionWake(before, "request:race"),
            yield* questionWake(before, "request:remaining"),
          ];
        }),
      );
      const raced = yield* Deferred.make<void>();
      const after = yield* makeLaptop(database, offers, linking, {
        queueNotifications: true,
        beforeQuestionCancel: (runId, threads) =>
          runId === wakes[0]
            ? threads
                .dispatch({
                  type: "queued-run.cancel",
                  commandId: CommandId.make("test:race-question-cancel"),
                  threadId: parentThreadId,
                  runId,
                })
                .pipe(
                  Effect.orDie,
                  Effect.andThen(Deferred.succeed(raced, undefined)),
                  Effect.asVoid,
                )
            : Effect.void,
      });
      yield* Ref.set(thread.questions, []);
      yield* Ref.set(thread.status, "completed");
      yield* Deferred.succeed(thread.finished, undefined);
      yield* after.remote.start();
      yield* Deferred.await(raced);
      // The stale cancellation must not skip the other queued question.
      yield* cancelledRun(after, wakes[1]!);
      const projection = yield* after.orchestrator.getThreadProjection(parentThreadId);
      expect(wakes.map((id) => projection.runs.find((run) => run.id === id)?.status)).toEqual([
        "cancelled",
        "cancelled",
      ]);
    }),
  ).pipe(Effect.provide(NodeHttpServer.layerTest)),
);

it.effect(
  "reports an unreachable linked machine instead of claiming there is no pending question",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const thread = yield* makeBoxThread;
        const b = yield* serveBox(thread);
        const offers = yield* SubscriptionRef.make<
          ReadonlyArray<ProviderContinuationRequests.ProviderContinuationRequest>
        >([]);
        const database = yield* SqlitePersistence.layerMemory.pipe(Layer.build);
        const a = yield* makeLaptop(database, offers);
        yield* linkTo(a.links, b, "full-access");
        yield* seedParent(a);
        const delegated = yield* delegateToBox(a);
        const task = (yield* a.orchestrator.getThreadProjection(parentThreadId)).subagents.find(
          (task) => task.id === delegated.taskId,
        )!;
        yield* b.settings.updateSettings({ linkedEnvironmentsEnabled: false });
        const result = yield* a.remote
          .pendingQuestions(parentScope as McpInvocationContext.McpThreadInvocationScope, task)
          .pipe(Effect.result);
        expect(result._tag).toBe("Failure");
        const status = yield* a.call("task_status", { taskId: delegated.taskId });
        expect(status.isError).toBe(false);
        expect(status.structuredContent).toMatchObject({
          taskId: delegated.taskId,
          status: "running",
          remoteChild: { environmentId: box.environmentId },
          summary: expect.stringContaining("questions could not be checked"),
        });
      }),
    ).pipe(Effect.provide(NodeHttpServer.layerTest)),
);

it.effect("acknowledging a question returned by wait withdraws an already queued wake", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const thread = yield* makeBoxThread;
      const b = yield* serveBox(thread);
      const offers = yield* SubscriptionRef.make<
        ReadonlyArray<ProviderContinuationRequests.ProviderContinuationRequest>
      >([]);
      const database = yield* SqlitePersistence.layerMemory.pipe(Layer.build);
      const a = yield* makeLaptop(database, offers, undefined, { queueNotifications: true });
      yield* linkTo(a.links, b, "full-access");
      yield* seedParent(a);
      yield* Ref.set(thread.questions, [{ id: "request:box-1", question: "Which suite?" }]);
      const delegated = yield* delegateToBox(a);
      const wake = yield* questionWake(a);
      const task = (yield* a.orchestrator.getThreadProjection(parentThreadId)).subagents.find(
        (task) => task.id === delegated.taskId,
      )!;
      expect(
        yield* a.remote.pendingQuestions(
          parentScope as McpInvocationContext.McpThreadInvocationScope,
          task,
        ),
      ).toEqual(["request:box-1"]);
      expect(
        (yield* a.orchestrator.getThreadProjection(parentThreadId)).runs.find(
          (run) => run.id === wake,
        )?.status,
      ).toBe("queued");
      expect(
        yield* a.remote.pendingQuestions(
          parentScope as McpInvocationContext.McpThreadInvocationScope,
          task,
          { acknowledge: true },
        ),
      ).toEqual(["request:box-1"]);
      yield* cancelledRun(a, wake);
    }),
  ).pipe(Effect.provide(NodeHttpServer.layerTest)),
);

it.effect.each(["interrupt", "failure"] as const)(
  "retries a remote question when its blocking wait exits by %s before delivery",
  (exit) =>
    Effect.scoped(
      Effect.gen(function* () {
        const thread = yield* makeBoxThread;
        const b = yield* serveBox(thread);
        const offers = yield* SubscriptionRef.make<
          ReadonlyArray<ProviderContinuationRequests.ProviderContinuationRequest>
        >([]);
        const database = yield* SqlitePersistence.layerMemory.pipe(Layer.build);
        const acknowledging = yield* Deferred.make<void>();
        const secondCheck = yield* Deferred.make<void>();
        const thirdCheck = yield* Deferred.make<void>();
        let checks = 0;
        let abandon = true;
        const a = yield* makeLaptop(database, offers, undefined, {
          queueNotifications: true,
          afterCall: (name) =>
            Effect.gen(function* () {
              if (name !== "t3_pending_request") return;
              checks++;
              if (checks === 2) yield* Deferred.succeed(secondCheck, undefined);
              if (checks >= 3) yield* Deferred.succeed(thirdCheck, undefined);
            }),
          afterPendingQuestions: Effect.gen(function* () {
            if (abandon && checks === 1) {
              abandon = false;
              yield* Deferred.succeed(acknowledging, undefined);
              return yield* exit === "interrupt"
                ? Effect.never
                : Effect.die(new Error("Lost wait"));
            }
          }),
        });
        yield* linkTo(a.links, b, "full-access");
        yield* seedParent(a);
        yield* Ref.set(thread.questions, [{ id: "request:box-1", question: "Which suite?" }]);
        const waiting = yield* a
          .call("delegate_task", {
            task: "Run the suite on the box.",
            target: { environmentId: box.environmentId, providerInstanceId: instanceId },
            mode: "wait",
            timeoutMs: 600_000,
            clientRequestId: "abandoned-question",
          })
          .pipe(Effect.forkScoped);
        yield* Deferred.await(acknowledging);
        if (exit === "interrupt") yield* Fiber.interrupt(waiting);
        else yield* Fiber.await(waiting);
        yield* TestClock.adjust(Duration.seconds(15));
        yield* Deferred.await(secondCheck);
        yield* TestClock.adjust(Duration.seconds(15));
        yield* Deferred.await(thirdCheck);
        const projection = yield* a.orchestrator.getThreadProjection(parentThreadId);
        expect(projection.subagents[0]?.completionWake).toBe("always");
        expect(
          projection.messages.filter((message) => message.text.includes("request:box-1")),
        ).toHaveLength(1);
      }),
    ).pipe(Effect.provide(NodeHttpServer.layerTest)),
);

it.effect.each(["queued wake", "delivered question"] as const)(
  "cancelling a replayed wait preserves the %s without another notification",
  (scenario) =>
    Effect.scoped(
      Effect.gen(function* () {
        const thread = yield* makeBoxThread;
        const b = yield* serveBox(thread);
        const offers = yield* SubscriptionRef.make<
          ReadonlyArray<ProviderContinuationRequests.ProviderContinuationRequest>
        >([]);
        const database = yield* SqlitePersistence.layerMemory.pipe(Layer.build);
        const readQuestion = yield* Deferred.make<void>();
        const pollSignals = yield* Effect.all([
          Deferred.make<void>(),
          Deferred.make<void>(),
          Deferred.make<void>(),
        ]);
        let pauseRead = false;
        let polls = -1;
        const a = yield* makeLaptop(database, offers, undefined, {
          queueNotifications: true,
          afterPendingQuestions: Effect.suspend(() =>
            pauseRead
              ? Deferred.succeed(readQuestion, undefined).pipe(Effect.andThen(Effect.never))
              : Effect.void,
          ),
          afterCall: (name) =>
            Effect.gen(function* () {
              if (name === "t3_pending_request" && polls >= 0) {
                const signal = pollSignals[polls++];
                if (signal !== undefined) yield* Deferred.succeed(signal, undefined);
              }
            }),
        });
        yield* linkTo(a.links, b, "full-access");
        yield* seedParent(a);
        yield* Ref.set(thread.questions, [{ id: "request:box-1", question: "Which suite?" }]);
        const input = {
          task: "Run the suite on the box.",
          target: { environmentId: box.environmentId, providerInstanceId: instanceId },
          clientRequestId: "replayed-question-wait",
        };
        let wake: RunId | undefined;
        if (scenario === "queued wake") {
          yield* a.call("delegate_task", { ...input, mode: "async" });
          wake = yield* questionWake(a);
        } else {
          const delivered = yield* a.call("delegate_task", { ...input, mode: "wait" });
          expect(delivered.structuredContent).toMatchObject({ hasPendingQuestion: true });
        }
        pauseRead = true;
        const retry = yield* a
          .call("delegate_task", {
            ...input,
            mode: "wait",
            timeoutMs: 600_000,
          })
          .pipe(Effect.forkScoped);
        yield* Deferred.await(readQuestion);
        yield* Fiber.interrupt(retry);
        polls = 0;
        for (const signal of pollSignals) {
          yield* TestClock.adjust(Duration.seconds(15));
          yield* Deferred.await(signal);
        }
        const projection = yield* a.orchestrator.getThreadProjection(parentThreadId);
        const notifications = projection.messages.filter((message) =>
          message.text.includes("request:box-1"),
        );
        expect(notifications).toHaveLength(scenario === "queued wake" ? 1 : 0);
        if (wake !== undefined) {
          expect(projection.runs.find((run) => run.id === wake)?.status).toBe("queued");
        }
      }),
    ).pipe(Effect.provide(NodeHttpServer.layerTest)),
);

it.effect("a blocking wait a question ends still lets the result there wake the parent", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const thread = yield* makeBoxThread;
      const b = yield* serveBox(thread);
      const offers = yield* SubscriptionRef.make<
        ReadonlyArray<ProviderContinuationRequests.ProviderContinuationRequest>
      >([]);
      const database = yield* SqlitePersistence.layerMemory.pipe(Layer.build);
      const secondCheck = yield* Deferred.make<void>();
      const thirdCheck = yield* Deferred.make<void>();
      let questionChecks = 0;
      const a = yield* makeLaptop(database, offers, undefined, {
        afterCall: (name) =>
          Effect.gen(function* () {
            if (name !== "t3_pending_request") return;
            questionChecks++;
            if (questionChecks === 2) yield* Deferred.succeed(secondCheck, undefined);
            if (questionChecks === 3) yield* Deferred.succeed(thirdCheck, undefined);
          }),
      });
      yield* linkTo(a.links, b, "full-access");
      yield* seedParent(a);
      yield* Ref.set(thread.questions, [{ id: "request:box-1", question: "Which suite?" }]);

      const waiting = yield* a
        .call("delegate_task", {
          task: "Run the full suite on the box.",
          target: { environmentId: box.environmentId, providerInstanceId: instanceId },
          mode: "wait",
          timeoutMs: 600_000,
          clientRequestId: "suite-on-box-wait",
        })
        .pipe(Effect.forkScoped);
      const result = yield* Fiber.join(waiting);
      expect(result.isError, JSON.stringify(result.content)).toBe(false);
      // The question ends the wait before its timeout, as for a local task.
      expect(result.structuredContent).toMatchObject({
        status: "running",
        hasPendingQuestion: true,
        waitTimedOut: false,
      });
      const { taskId } = result.structuredContent as { taskId: NodeId };
      // Wait for later probes to actually return, not for a guessed network delay.
      yield* TestClock.adjust(Duration.seconds(15));
      yield* Deferred.await(secondCheck);
      yield* TestClock.adjust(Duration.seconds(15));
      yield* Deferred.await(thirdCheck);
      const projection = yield* a.orchestrator.getThreadProjection(parentThreadId);
      // Its result can no longer return through the wait, so it wakes the parent.
      expect(projection.subagents.find((task) => task.id === taskId)?.completionWake).toBe(
        "always",
      );
      expect(
        projection.messages.filter((message) => message.text.includes("request:box-1")),
      ).toEqual([]);
      // A status probe still sees the pending question but cannot queue it again.
      const status = yield* a.call("task_status", { taskId });
      expect(status.structuredContent).toMatchObject({ hasPendingQuestion: true });
      yield* Ref.set(thread.reply, "All green.");
      yield* Ref.set(thread.status, "completed");
      yield* Deferred.succeed(thread.finished, undefined);
      expect(yield* taskResult(a, taskId)).toMatchObject({
        status: "completed",
        result: "All green.",
      });
    }),
  ).pipe(Effect.provide(NodeHttpServer.layerTest)),
);

it.effect("follows an open task again after the laptop restarts", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const thread = yield* makeBoxThread;
      const b = yield* serveBox(thread);
      const offers = yield* SubscriptionRef.make<
        ReadonlyArray<ProviderContinuationRequests.ProviderContinuationRequest>
      >([]);
      const database = yield* SqlitePersistence.layerMemory.pipe(Layer.build);
      // The laptop before the restart delegates, then goes away.
      const delegated = yield* Effect.scoped(
        Effect.gen(function* () {
          const before = yield* makeLaptop(database, offers);
          yield* linkTo(before.links, b, "full-access");
          yield* seedParent(before);
          return yield* delegateToBox(before);
        }),
      );
      // The task finishes on the box while the laptop is down.
      yield* Ref.set(thread.reply, "Finished while you were away.");
      yield* Ref.set(thread.status, "completed");
      yield* Deferred.succeed(thread.finished, undefined);

      const after = yield* makeLaptop(database, offers);
      // The link lives in the laptop's own store, so it needs linking again here.
      yield* linkTo(after.links, b, "full-access");
      yield* after.remote.start();
      const task = yield* taskResult(after, delegated.taskId);
      expect(task).toMatchObject({ status: "completed", result: "Finished while you were away." });
    }),
  ).pipe(Effect.provide(NodeHttpServer.layerTest)),
);

it.effect("fails the task when the box revokes the link, and cancel interrupts it there", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const thread = yield* makeBoxThread;
      const b = yield* serveBox(thread);
      const offers = yield* SubscriptionRef.make<
        ReadonlyArray<ProviderContinuationRequests.ProviderContinuationRequest>
      >([]);
      const database = yield* SqlitePersistence.layerMemory.pipe(Layer.build);
      const a = yield* makeLaptop(database, offers);
      yield* linkTo(a.links, b, "full-access");
      yield* seedParent(a);

      const cancelled = yield* delegateToBox(a);
      const cancel = yield* a.call("task_cancel", {
        taskId: cancelled.taskId,
        reason: "Not needed",
      });
      expect(cancel.structuredContent).toEqual({
        taskId: cancelled.taskId,
        status: "cancel_requested",
      });
      // Cancelling stops the thread there as Stop does: its queue and its own tasks too.
      expect(yield* Ref.get(thread.stops)).toEqual(["thread.stop", "delegated-tasks"]);
      expect(yield* taskResult(a, cancelled.taskId)).toMatchObject({ status: "cancelled" });

      // Cancelling again, once the result is in, still stops what came after it there.
      const again = yield* a.call("task_cancel", {
        taskId: cancelled.taskId,
        clientRequestId: "cancel-again",
      });
      expect(again.structuredContent).toEqual({ taskId: cancelled.taskId, status: "cancelled" });
      expect(yield* Ref.get(thread.interrupted)).toBe(2);
      // ...and a retry of that cancel stops nothing new.
      yield* a.call("task_cancel", { taskId: cancelled.taskId, clientRequestId: "cancel-again" });
      expect(yield* Ref.get(thread.interrupted)).toBe(2);

      // A second task, then the box revokes the link before it ends.
      const revoked = yield* a
        .call("delegate_task", {
          task: "Another one.",
          target: {
            environmentId: box.environmentId,
            providerInstanceId: instanceId,
            model: "gpt-5.4",
          },
          clientRequestId: "second",
        })
        .pipe(Effect.map((result) => result.structuredContent as { taskId: NodeId }));
      const [session] = yield* b.linkedSessions;
      yield* b.auth.revokeSession(session!.sessionId);
      yield* Deferred.succeed(thread.finished, undefined);
      const failed = yield* taskResult(a, revoked.taskId);
      expect(failed?.status).toBe("failed");
      expect(failed?.result).toBe(
        "The linked environment no longer accepts this link. It may have been revoked there; link it again.",
      );
    }),
  ).pipe(Effect.provide(NodeHttpServer.layerTest)),
);

it.effect("a task's result is its own run's whole last reply, not a later run's", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const thread = yield* makeBoxThread;
      const b = yield* serveBox(thread);
      const offers = yield* SubscriptionRef.make<
        ReadonlyArray<ProviderContinuationRequests.ProviderContinuationRequest>
      >([]);
      const database = yield* SqlitePersistence.layerMemory.pipe(Layer.build);
      const a = yield* makeLaptop(database, offers);
      yield* linkTo(a.links, b, "full-access");
      yield* seedParent(a);

      const result = yield* a.call("delegate_task", {
        task: "Review the diff.",
        role: "review",
        target: { environmentId: box.environmentId },
        clientRequestId: "review-on-box",
      });
      expect(result.isError, JSON.stringify(result.content)).toBe(false);
      const { taskId } = result.structuredContent as { taskId: NodeId };
      // The role reaches the thread there, as it does a child here.
      expect(yield* Ref.get(thread.message)).toBe(
        "Act as the review sub-agent for this task.\n\nReview the diff.",
      );

      // A reply longer than one read, then a later run's reply on the same thread.
      const long = "x".repeat(45_000);
      yield* Ref.set(thread.reply, long);
      yield* Ref.set(thread.laterReply, "Someone else asked a follow-up.");
      yield* Ref.set(thread.executedModel, "gpt-5.4-mini");
      yield* Ref.set(thread.status, "completed");
      yield* Deferred.succeed(thread.finished, undefined);
      const task = yield* taskResult(a, taskId);
      expect(task?.status).toBe("completed");
      expect(task?.result).toBe(
        `Requested model gpt-5.4; the provider reported running gpt-5.4-mini.\n\n${long}`,
      );
      // Every wait was for the run the launch started.
      const waits = yield* Ref.get(thread.waitedFor);
      expect(waits.length).toBeGreaterThan(0);
      expect(waits.every((runId) => runId === boxRunId)).toBe(true);
    }),
  ).pipe(Effect.provide(NodeHttpServer.layerTest)),
);

it.effect("a task whose thread there waits on its own task ends with the answer after it", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const thread = yield* makeBoxThread;
      const b = yield* serveBox(thread);
      const offers = yield* SubscriptionRef.make<
        ReadonlyArray<ProviderContinuationRequests.ProviderContinuationRequest>
      >([]);
      const database = yield* SqlitePersistence.layerMemory.pipe(Layer.build);
      const a = yield* makeLaptop(database, offers);
      yield* linkTo(a.links, b, "full-access");
      yield* seedParent(a);
      const delegated = yield* delegateToBox(a);

      // The task's run there ends after delegating a review of its own.
      yield* Ref.set(thread.nested, true);
      yield* Ref.set(thread.reply, "Started a review; I will answer when it reports.");
      yield* Ref.set(thread.status, "completed");
      yield* Deferred.succeed(thread.finished, undefined);
      const first = yield* Effect.raceFirst(
        Deferred.await(thread.nestedWaiting).pipe(Effect.as("waiting there" as const)),
        taskResult(a, delegated.taskId).pipe(Effect.as("completed here" as const)),
      );
      expect(first).toBe("waiting there");

      // The review reports and wakes the thread there, whose next run answers.
      yield* Ref.set(thread.laterReply, "The review found two issues; both are fixed.");
      yield* Deferred.succeed(thread.nestedSettled, undefined);
      expect(yield* taskResult(a, delegated.taskId)).toMatchObject({
        status: "completed",
        result: "The review found two issues; both are fixed.",
      });
    }),
  ).pipe(Effect.provide(NodeHttpServer.layerTest)),
);

it.effect("a follower resumed after restart uses the settled continuation's result", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const thread = yield* makeBoxThread;
      const b = yield* serveBox(thread);
      const offers = yield* SubscriptionRef.make<
        ReadonlyArray<ProviderContinuationRequests.ProviderContinuationRequest>
      >([]);
      const database = yield* SqlitePersistence.layerMemory.pipe(Layer.build);
      const linking = yield* layerLinkingEnvironment(laptop).pipe(Layer.build);
      const task = yield* Effect.scoped(
        Effect.gen(function* () {
          const before = yield* makeLaptop(database, offers, linking);
          yield* linkTo(before.links, b, "full-access");
          yield* seedParent(before);
          yield* Ref.set(thread.nested, true);
          yield* Ref.set(thread.reply, "A review is still running.");
          const task = yield* delegateToBox(before);
          yield* Ref.set(thread.status, "completed");
          yield* Deferred.succeed(thread.finished, undefined);
          yield* Deferred.await(thread.nestedWaiting);
          return task;
        }),
      );
      const after = yield* makeLaptop(database, offers, linking);
      yield* Ref.set(thread.laterReply, "The resumed review found and fixed two issues.");
      yield* Deferred.succeed(thread.nestedSettled, undefined);
      yield* after.remote.start();
      expect(yield* taskResult(after, task.taskId)).toMatchObject({
        status: "completed",
        result: "The resumed review found and fixed two issues.",
      });
    }),
  ).pipe(Effect.provide(NodeHttpServer.layerTest)),
);

it.effect(
  "a running stop reports the interrupted run, not an earlier unrelated queued update",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const thread = yield* makeBoxThread;
        const b = yield* serveBox(thread);
        const offers = yield* SubscriptionRef.make<
          ReadonlyArray<ProviderContinuationRequests.ProviderContinuationRequest>
        >([]);
        const database = yield* SqlitePersistence.layerMemory.pipe(Layer.build);
        const a = yield* makeLaptop(database, offers);
        yield* linkTo(a.links, b, "full-access");
        yield* seedParent(a);
        const task = yield* delegateToBox(a);
        const stopped = yield* a.call("t3_thread_interrupt", {
          environmentId: box.environmentId,
          threadId: task.remoteChild.threadId,
          stop: true,
          clientRequestId: "running-stop",
        });
        expect(stopped.isError, JSON.stringify(stopped.content)).toBe(false);
        expect(stopped.structuredContent).toMatchObject({
          runId: boxRunId,
          status: "interrupt_requested",
        });
      }),
    ).pipe(Effect.provide(NodeHttpServer.layerTest)),
);

it.effect("the same key from another provider session is another task", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const thread = yield* makeBoxThread;
      const b = yield* serveBox(thread);
      const offers = yield* SubscriptionRef.make<
        ReadonlyArray<ProviderContinuationRequests.ProviderContinuationRequest>
      >([]);
      const database = yield* SqlitePersistence.layerMemory.pipe(Layer.build);
      const a = yield* makeLaptop(database, offers);
      yield* linkTo(a.links, b, "full-access");
      yield* seedParent(a);

      const first = yield* delegateToBox(a);
      const later = yield* a.call(
        "delegate_task",
        {
          task: "Run the full suite on the box.",
          target: { environmentId: box.environmentId },
          clientRequestId: "suite-on-box",
        },
        { ...parentScope, requestNamespace: "provider-session:laptop-parent-2" },
      );
      expect(later.isError, JSON.stringify(later.content)).toBe(false);
      const second = later.structuredContent as {
        taskId: NodeId;
        remoteChild: { threadId: ThreadId };
      };
      expect(second.taskId).not.toBe(first.taskId);
      expect(second.remoteChild.threadId).not.toBe(first.remoteChild.threadId);
      expect((yield* a.orchestrator.getThreadProjection(parentThreadId)).subagents).toHaveLength(2);
    }),
  ).pipe(Effect.provide(NodeHttpServer.layerTest)),
);

it.effect.each([null, "Working before the provider failed."])(
  "reports the failed remote run's error instead of its earlier reply (%s)",
  (reply) =>
    Effect.scoped(
      Effect.gen(function* () {
        const thread = yield* makeBoxThread;
        const b = yield* serveBox(thread);
        const offers = yield* SubscriptionRef.make<
          ReadonlyArray<ProviderContinuationRequests.ProviderContinuationRequest>
        >([]);
        const database = yield* SqlitePersistence.layerMemory.pipe(Layer.build);
        const a = yield* makeLaptop(database, offers);
        yield* linkTo(a.links, b, "full-access");
        yield* seedParent(a);
        const delegated = yield* delegateToBox(a);
        yield* Ref.set(thread.reply, reply);
        const error = "Provider failed: authentication expired on the destination.";
        yield* Ref.set(thread.error, error);
        yield* Ref.set(thread.status, "failed");
        yield* Deferred.succeed(thread.finished, undefined);
        expect(yield* taskResult(a, delegated.taskId)).toMatchObject({
          status: "failed",
          result: error,
        });
        const status = yield* a.call("task_status", { taskId: delegated.taskId });
        expect(status.structuredContent).toMatchObject({
          status: "failed",
          summary: error,
          latestTerminalSummary: error,
        });
        const offered = yield* SubscriptionRef.changes(offers).pipe(
          Stream.filter((seen) => seen.some((offer) => offer.threadId === parentThreadId)),
          Stream.runHead,
        );
        expect(Option.isSome(offered)).toBe(true);
      }),
    ).pipe(Effect.provide(NodeHttpServer.layerTest)),
);

it.effect.each([false, true])(
  "ignores later-run errors and reads the failed run's full error (ownError=%s)",
  (ownError) =>
    Effect.scoped(
      Effect.gen(function* () {
        const thread = yield* makeBoxThread;
        const b = yield* serveBox(thread);
        const offers = yield* SubscriptionRef.make<
          ReadonlyArray<ProviderContinuationRequests.ProviderContinuationRequest>
        >([]);
        const database = yield* SqlitePersistence.layerMemory.pipe(Layer.build);
        const a = yield* makeLaptop(database, offers);
        yield* linkTo(a.links, b, "full-access");
        yield* seedParent(a);
        const delegated = yield* delegateToBox(a);
        yield* Ref.set(thread.reply, null);
        const error = `Provider failed: ${"detail ".repeat(10_000)}end of error.`;
        if (ownError) yield* Ref.set(thread.error, error);
        yield* Ref.set(thread.laterReply, "A later user run answered.");
        yield* Ref.set(thread.laterError, "Another run's provider error.");
        yield* Ref.set(thread.status, "failed");
        yield* Deferred.succeed(thread.finished, undefined);
        expect(yield* taskResult(a, delegated.taskId)).toMatchObject({
          status: "failed",
          result: ownError
            ? error
            : "(The thread in the linked environment ended without a reply.)",
        });
        expect(yield* Ref.get(thread.waitedFor)).toEqual([boxRunId]);
      }),
    ).pipe(Effect.provide(NodeHttpServer.layerTest)),
);

it.effect("keeps a successful remote reply despite an error item in the run's activity", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const thread = yield* makeBoxThread;
      const b = yield* serveBox(thread);
      const offers = yield* SubscriptionRef.make<
        ReadonlyArray<ProviderContinuationRequests.ProviderContinuationRequest>
      >([]);
      const database = yield* SqlitePersistence.layerMemory.pipe(Layer.build);
      const a = yield* makeLaptop(database, offers);
      yield* linkTo(a.links, b, "full-access");
      yield* seedParent(a);
      const delegated = yield* delegateToBox(a);
      yield* Ref.set(thread.reply, "Recovered and completed the task.");
      yield* Ref.set(thread.error, "A recoverable tool error.");
      yield* Ref.set(thread.status, "completed");
      yield* Deferred.succeed(thread.finished, undefined);
      expect(yield* taskResult(a, delegated.taskId)).toMatchObject({
        status: "completed",
        result: "Recovered and completed the task.",
      });
    }),
  ).pipe(Effect.provide(NodeHttpServer.layerTest)),
);

it.effect("fails the task when its thread there is gone, or the link here is", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const thread = yield* makeBoxThread;
      const b = yield* serveBox(thread);
      const offers = yield* SubscriptionRef.make<
        ReadonlyArray<ProviderContinuationRequests.ProviderContinuationRequest>
      >([]);
      const database = yield* SqlitePersistence.layerMemory.pipe(Layer.build);
      const a = yield* makeLaptop(database, offers);
      yield* linkTo(a.links, b, "full-access");
      yield* seedParent(a);

      yield* Ref.set(thread.gone, true);
      const gone = yield* delegateToBox(a);
      const failed = yield* taskResult(a, gone.taskId);
      expect(failed?.status).toBe("failed");
      expect(failed?.result).toContain("was not found");

      // The link is forgotten here while the task runs there.
      yield* Ref.set(thread.gone, false);
      const forgotten = yield* a
        .call("delegate_task", {
          task: "Another one.",
          target: { environmentId: box.environmentId },
          clientRequestId: "forgotten",
        })
        .pipe(Effect.map((result) => result.structuredContent as { taskId: NodeId }));
      yield* a.links.unlink(box.environmentId);
      yield* Ref.set(thread.status, "completed");
      yield* Deferred.succeed(thread.finished, undefined);
      const unlinked = yield* taskResult(a, forgotten.taskId);
      expect(unlinked?.status).toBe("failed");
      expect(unlinked?.result).toContain("not linked to");
    }),
  ).pipe(Effect.provide(NodeHttpServer.layerTest)),
);

it.effect("cancel ends the task here even when the link is gone", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const thread = yield* makeBoxThread;
      const b = yield* serveBox(thread);
      const offers = yield* SubscriptionRef.make<
        ReadonlyArray<ProviderContinuationRequests.ProviderContinuationRequest>
      >([]);
      const database = yield* SqlitePersistence.layerMemory.pipe(Layer.build);
      const a = yield* makeLaptop(database, offers);
      yield* linkTo(a.links, b, "full-access");
      yield* seedParent(a);

      const delegated = yield* delegateToBox(a);
      yield* a.links.unlink(box.environmentId);
      const cancel = yield* a.call("task_cancel", { taskId: delegated.taskId });
      expect(cancel.structuredContent).toEqual({
        taskId: delegated.taskId,
        status: "cancel_requested",
      });
      const task = yield* taskResult(a, delegated.taskId);
      expect(task?.status).toBe("cancelled");
      expect(task?.result).toContain("could not be stopped in Box");
      expect(yield* Ref.get(thread.interrupted)).toBe(0);
    }),
  ).pipe(Effect.provide(NodeHttpServer.layerTest)),
);

it.effect("a parent stopped while the task's result is read still stops its thread there", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const thread = yield* makeBoxThread;
      const b = yield* serveBox(thread);
      const offers = yield* SubscriptionRef.make<
        ReadonlyArray<ProviderContinuationRequests.ProviderContinuationRequest>
      >([]);
      const database = yield* SqlitePersistence.layerMemory.pipe(Layer.build);
      const a = yield* makeLaptop(database, offers);
      yield* linkTo(a.links, b, "full-access");
      yield* seedParent(a);
      const delegated = yield* delegateToBox(a);

      // The task's run ends there, and while the follower reads its reply,
      // the parent here is stopped: more work may follow that run there.
      yield* Ref.set(thread.holdRead, true);
      yield* Ref.set(thread.reply, "Done there.");
      yield* Ref.set(thread.status, "completed");
      yield* Deferred.succeed(thread.finished, undefined);
      yield* Deferred.await(thread.readStarted);
      yield* a.threads.stopDelegatedTasks({
        threadId: parentThreadId,
        commandId: CommandId.make("command:stop:laptop-parent-mid-read"),
      });
      yield* Deferred.succeed(thread.releaseRead, undefined);

      // The Stop's result stands, and the thread there is still stopped.
      expect(yield* taskResult(a, delegated.taskId)).toMatchObject({ status: "interrupted" });
      yield* Deferred.await(thread.interruptedOnce);
      expect(yield* Ref.get(thread.stops)).toContain("thread.stop");
    }),
  ).pipe(Effect.provide(NodeHttpServer.layerTest)),
);

it.effect("a stop the box refuses because the parent was lowered ends instead of retrying", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const thread = yield* makeBoxThread;
      const b = yield* serveBox(thread);
      const offers = yield* SubscriptionRef.make<
        ReadonlyArray<ProviderContinuationRequests.ProviderContinuationRequest>
      >([]);
      const database = yield* SqlitePersistence.layerMemory.pipe(Layer.build);
      const a = yield* makeLaptop(database, offers);
      yield* linkTo(a.links, b, "full-access");
      yield* seedParent(a);
      const delegated = yield* delegateToBox(a);

      // The parent is lowered below the full-access thread there, then stopped.
      yield* a.orchestrator.dispatch({
        type: "thread.runtime-mode.set",
        commandId: CommandId.make("command:lower:laptop-parent"),
        threadId: parentThreadId,
        runtimeMode: "approval-required",
      });
      yield* a.threads.stopDelegatedTasks({
        threadId: parentThreadId,
        commandId: CommandId.make("command:stop:laptop-parent-lowered"),
      });

      // The box refuses that stop; the follower records it as settled and
      // stops, so nothing retries it, now or after a restart.
      const settled = yield* a.orchestrator
        .streamStoredEventsFrom({
          threadId: parentThreadId,
          afterSequence: 0,
          eventType: "subagent.updated",
        })
        .pipe(
          Stream.filter(
            (stored) =>
              stored.event.type === "subagent.updated" &&
              stored.event.payload.id === delegated.taskId &&
              stored.event.payload.remoteChild?.stoppedThere === true,
          ),
          Stream.runHead,
        );
      expect(Option.isSome(settled)).toBe(true);
      expect(yield* Ref.get(thread.stops)).toEqual([]);
      expect(yield* a.projections.getOpenRemoteDelegatedTasks).toEqual([]);
    }),
  ).pipe(Effect.provide(NodeHttpServer.layerTest)),
);

it.effect("stopping the parent stops its task there", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const thread = yield* makeBoxThread;
      const b = yield* serveBox(thread);
      const offers = yield* SubscriptionRef.make<
        ReadonlyArray<ProviderContinuationRequests.ProviderContinuationRequest>
      >([]);
      const database = yield* SqlitePersistence.layerMemory.pipe(Layer.build);
      const a = yield* makeLaptop(database, offers);
      yield* linkTo(a.links, b, "full-access");
      yield* seedParent(a);

      const delegated = yield* delegateToBox(a);
      // What the delegated-tasks.stop effect runs once the parent's Stop commits.
      yield* a.threads.stopDelegatedTasks({
        threadId: parentThreadId,
        commandId: CommandId.make("command:stop:laptop-parent"),
        reason: "User stopped",
      });
      expect(yield* taskResult(a, delegated.taskId)).toMatchObject({
        status: "interrupted",
        result: "Stopped with its parent: User stopped",
      });
      // The follower sees it ended here and stops it there.
      yield* Deferred.await(thread.interruptedOnce);
      expect(yield* Ref.get(thread.interrupted)).toBe(1);
    }),
  ).pipe(Effect.provide(NodeHttpServer.layerTest)),
);

/** A follower retry schedule the test steps: each failed attempt is offered, then waits for `retry`. */
const steppedRetries = Effect.gen(function* () {
  const failed = yield* Queue.unbounded<void>();
  const retry = yield* Queue.unbounded<void>();
  const schedule = Schedule.forever.pipe(
    Schedule.tap(() => Queue.offer(failed, undefined).pipe(Effect.andThen(Queue.take(retry)))),
  );
  return { failed, retry, schedule };
});

/** Waits until the task here records that the `stopCount`th Stop after its result reached there. */
const stoppedThere = (
  laptopEnv: Effect.Success<ReturnType<typeof makeLaptop>>,
  taskId: NodeId,
  // `null` for a cancel's stop, which carries no count.
  stopCount: number | null = 1,
) =>
  laptopEnv.orchestrator
    .streamStoredEventsFrom({
      threadId: parentThreadId,
      afterSequence: 0,
      eventType: "subagent.updated",
    })
    .pipe(
      Stream.filter(
        (stored) =>
          stored.event.type === "subagent.updated" &&
          stored.event.payload.id === taskId &&
          stored.event.payload.remoteChild?.stoppedThere === true &&
          stored.event.payload.remoteChild.stopCount === (stopCount ?? undefined),
      ),
      Stream.runHead,
      Effect.map((stored) =>
        Option.isSome(stored) && stored.value.event.type === "subagent.updated"
          ? stored.value.event.payload
          : undefined,
      ),
    );

it.effect("stopping the parent stops follow-up work on a task there that has its result", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const thread = yield* makeBoxThread;
      const b = yield* serveBox(thread);
      const offers = yield* SubscriptionRef.make<
        ReadonlyArray<ProviderContinuationRequests.ProviderContinuationRequest>
      >([]);
      const database = yield* SqlitePersistence.layerMemory.pipe(Layer.build);
      const a = yield* makeLaptop(database, offers);
      yield* a.remote.start();
      yield* linkTo(a.links, b, "full-access");
      yield* seedParent(a);
      const delegated = yield* delegateToBox(a);
      yield* Ref.set(thread.reply, "All green.");
      yield* Ref.set(thread.status, "completed");
      yield* Deferred.succeed(thread.finished, undefined);
      expect(yield* taskResult(a, delegated.taskId)).toMatchObject({
        status: "completed",
        result: "All green.",
      });

      // The parent sends the thread there a follow-up, which starts a run there.
      const sent = yield* a.call("t3_thread_send", {
        environmentId: box.environmentId,
        threadId: delegated.remoteChild.threadId,
        message: "Now run the slow suite too.",
      });
      expect(sent.isError, JSON.stringify(sent.content)).toBe(false);
      expect(yield* Ref.get(thread.sent)).toEqual(["Now run the slow suite too."]);
      expect(yield* Ref.get(thread.interrupted)).toBe(0);

      yield* a.threads.stopDelegatedTasks({
        threadId: parentThreadId,
        commandId: CommandId.make("command:stop:laptop-parent-after-result"),
      });
      // The thread there is stopped as Stop does, and the task keeps its result.
      const task = yield* stoppedThere(a, delegated.taskId);
      expect(yield* Ref.get(thread.stops)).toEqual(["thread.stop", "delegated-tasks"]);
      expect(task).toMatchObject({ status: "completed", result: "All green." });
      expect(yield* a.projections.getOpenRemoteDelegatedTasks).toEqual([]);

      // A second Stop owes a second stop there, not a replay of the first.
      yield* a.threads.stopDelegatedTasks({
        threadId: parentThreadId,
        commandId: CommandId.make("command:stop:laptop-parent-again"),
      });
      expect(yield* stoppedThere(a, delegated.taskId, 2)).toMatchObject({ status: "completed" });
      expect(yield* Ref.get(thread.interrupted)).toBe(2);
    }),
  ).pipe(Effect.provide(NodeHttpServer.layerTest)),
);

it.effect("a delayed Stop acknowledgement leaves a newer Stop owed", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const thread = yield* makeBoxThread;
      const b = yield* serveBox(thread);
      const offers = yield* SubscriptionRef.make<
        ReadonlyArray<ProviderContinuationRequests.ProviderContinuationRequest>
      >([]);
      const database = yield* SqlitePersistence.layerMemory.pipe(Layer.build);
      const a = yield* makeLaptop(database, offers);
      yield* a.remote.start();
      yield* linkTo(a.links, b, "full-access");
      yield* seedParent(a);
      const delegated = yield* delegateToBox(a);
      yield* Ref.set(thread.reply, "All green.");
      yield* Ref.set(thread.status, "completed");
      yield* Deferred.succeed(thread.finished, undefined);
      yield* taskResult(a, delegated.taskId);

      yield* Ref.set(thread.holdFirstStop, true);
      yield* a.threads.stopDelegatedTasks({
        threadId: parentThreadId,
        commandId: CommandId.make("command:stop:delayed-first"),
      });
      yield* Deferred.await(thread.stopStarted);
      // Work starts after the first Stop landed there, but before its acknowledgement returns.
      yield* a.call("t3_thread_send", {
        environmentId: box.environmentId,
        threadId: delegated.remoteChild.threadId,
        message: "Run another check.",
      });
      yield* a.threads.stopDelegatedTasks({
        threadId: parentThreadId,
        commandId: CommandId.make("command:stop:delayed-second"),
      });
      const pending = yield* a.threads.getThreadRecords(parentThreadId, ["subagents"]);
      expect(
        pending.subagents.find((task) => task.id === delegated.taskId)?.remoteChild,
      ).toMatchObject({ stoppedThere: false, stopCount: 2 });
      yield* Deferred.succeed(thread.releaseStop, undefined);
      expect(yield* stoppedThere(a, delegated.taskId, 2)).toMatchObject({
        status: "completed",
        result: "All green.",
      });
      expect(yield* Ref.get(thread.interrupted)).toBe(2);
      expect(yield* a.projections.getOpenRemoteDelegatedTasks).toEqual([]);
    }),
  ).pipe(Effect.provide(NodeHttpServer.layerTest)),
);

it.effect("a Stop on a task that has its result reaches there after a restart", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const thread = yield* makeBoxThread;
      const b = yield* serveBox(thread);
      const offers = yield* SubscriptionRef.make<
        ReadonlyArray<ProviderContinuationRequests.ProviderContinuationRequest>
      >([]);
      const database = yield* SqlitePersistence.layerMemory.pipe(Layer.build);
      const linking = yield* layerLinkingEnvironment(laptop).pipe(Layer.build);
      // The laptop records the Stop, then goes away before its follower sends it.
      const delegated = yield* Effect.scoped(
        Effect.gen(function* () {
          const before = yield* makeLaptop(database, offers, linking);
          yield* linkTo(before.links, b, "full-access");
          yield* seedParent(before);
          const task = yield* delegateToBox(before);
          yield* Ref.set(thread.status, "completed");
          yield* Deferred.succeed(thread.finished, undefined);
          yield* taskResult(before, task.taskId);
          yield* before.threads.stopDelegatedTasks({
            threadId: parentThreadId,
            commandId: CommandId.make("command:stop:laptop-parent-before-restart"),
          });
          return task;
        }),
      );
      expect(yield* Ref.get(thread.interrupted)).toBe(0);

      const after = yield* makeLaptop(database, offers, linking);
      expect(yield* after.projections.getOpenRemoteDelegatedTasks).toEqual([
        { parentThreadId, taskId: delegated.taskId },
      ]);
      yield* after.remote.start();
      expect(yield* stoppedThere(after, delegated.taskId)).toMatchObject({ status: "completed" });
      expect(yield* Ref.get(thread.stops)).toEqual(["thread.stop", "delegated-tasks"]);
    }),
  ).pipe(Effect.provide(NodeHttpServer.layerTest)),
);

it.effect("a parent stopped while the box is unreachable still stops its task there", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const thread = yield* makeBoxThread;
      const b = yield* serveBox(thread);
      const offers = yield* SubscriptionRef.make<
        ReadonlyArray<ProviderContinuationRequests.ProviderContinuationRequest>
      >([]);
      const database = yield* SqlitePersistence.layerMemory.pipe(Layer.build);
      const retries = yield* steppedRetries;
      const a = yield* makeLaptop(database, offers).pipe(
        Effect.provideService(RemoteDelegation.FollowRetrySchedule, retries.schedule),
      );
      yield* linkTo(a.links, b, "full-access");
      yield* seedParent(a);
      // The box sleeps as soon as the task is there, so its follower's first attempt fails.
      yield* Ref.set(
        thread.afterLaunch,
        Ref.set(b.descriptor, descriptorOf("environment-elsewhere", "Elsewhere")),
      );
      const result = yield* a.call("delegate_task", {
        task: "Run the full suite on the box.",
        target: { environmentId: box.environmentId },
        mode: "async",
        clientRequestId: "suite-on-box",
      });
      // The task was accepted: a failed probe must not hide its handle from the caller.
      expect(result.isError).toBe(false);
      expect(result.structuredContent).toMatchObject({
        status: "running",
        remoteChild: { environmentId: box.environmentId },
        summary: expect.stringContaining("questions could not be checked"),
      });
      const delegated = result.structuredContent as { taskId: NodeId };
      expect(delegated.taskId).toBe(
        (yield* a.orchestrator.getThreadProjection(parentThreadId)).subagents[0]!.id,
      );
      yield* Queue.take(retries.failed);
      // The parent is stopped meanwhile.
      yield* a.threads.stopDelegatedTasks({
        threadId: parentThreadId,
        commandId: CommandId.make("command:stop:laptop-parent-offline"),
      });
      expect(yield* taskResult(a, delegated.taskId)).toMatchObject({ status: "interrupted" });

      // Once the box is back, the follower's next attempt stops the task there.
      yield* Ref.set(b.descriptor, box);
      yield* Queue.offer(retries.retry, undefined);
      yield* Deferred.await(thread.interruptedOnce);
      expect(yield* Ref.get(thread.interrupted)).toBe(1);
    }),
  ).pipe(Effect.provide(NodeHttpServer.layerTest)),
);

it.effect("a task cancelled while the box is unreachable is stopped there after a restart", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const thread = yield* makeBoxThread;
      const b = yield* serveBox(thread);
      const offers = yield* SubscriptionRef.make<
        ReadonlyArray<ProviderContinuationRequests.ProviderContinuationRequest>
      >([]);
      const database = yield* SqlitePersistence.layerMemory.pipe(Layer.build);
      // Its links outlive the restart, so the box knows it as the same link session.
      const linking = yield* layerLinkingEnvironment(laptop).pipe(Layer.build);
      // The laptop cancels while the box sleeps, then restarts before it wakes.
      const delegated = yield* Effect.scoped(
        Effect.gen(function* () {
          const before = yield* makeLaptop(database, offers, linking);
          yield* linkTo(before.links, b, "full-access");
          yield* seedParent(before);
          const task = yield* delegateToBox(before);
          yield* Ref.set(b.descriptor, descriptorOf("environment-elsewhere", "Elsewhere"));
          yield* before.call("task_cancel", { taskId: task.taskId });
          expect(yield* taskResult(before, task.taskId)).toMatchObject({ status: "cancelled" });
          return task;
        }),
      );
      expect(yield* Ref.get(thread.interrupted)).toBe(0);

      yield* Ref.set(b.descriptor, box);
      const after = yield* makeLaptop(database, offers, linking);
      yield* after.remote.start();
      yield* Deferred.await(thread.interruptedOnce);
      // Once the stop is recorded, nothing is owed there any more.
      yield* stoppedThere(after, delegated.taskId, null);
      expect(yield* after.projections.getOpenRemoteDelegatedTasks).toEqual([]);
      expect(yield* Ref.get(thread.stops)).toEqual(["thread.stop", "delegated-tasks"]);
      // The result here is still the cancel's.
      expect(yield* taskResult(after, delegated.taskId)).toMatchObject({ status: "cancelled" });
    }),
  ).pipe(Effect.provide(NodeHttpServer.layerTest)),
);

it.effect("a task whose thread there has no run ends with its last reply", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const thread = yield* makeBoxThread;
      const b = yield* serveBox(thread);
      const offers = yield* SubscriptionRef.make<
        ReadonlyArray<ProviderContinuationRequests.ProviderContinuationRequest>
      >([]);
      const database = yield* SqlitePersistence.layerMemory.pipe(Layer.build);
      const a = yield* makeLaptop(database, offers);
      yield* linkTo(a.links, b, "full-access");
      yield* seedParent(a);
      // As a task recorded before its run was kept, whose thread there is idle.
      yield* Ref.set(thread.runless, true);
      yield* Ref.set(thread.reply, "Done before it was followed.");

      const delegated = yield* delegateToBox(a);
      expect(yield* taskResult(a, delegated.taskId)).toMatchObject({
        status: "completed",
        result: "Done before it was followed.",
      });
      // One look, not a loop.
      expect(yield* Ref.get(thread.waitedFor)).toEqual([undefined]);
    }),
  ).pipe(Effect.provide(NodeHttpServer.layerTest)),
);

it.effect("a local task runs in this thread's project", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const offers = yield* SubscriptionRef.make<
        ReadonlyArray<ProviderContinuationRequests.ProviderContinuationRequest>
      >([]);
      const database = yield* SqlitePersistence.layerMemory.pipe(Layer.build);
      const a = yield* makeLaptop(database, offers);
      yield* seedParent(a);

      const result = yield* a.call("delegate_task", {
        task: "Run it here.",
        target: { projectId: "project:elsewhere" },
      });
      expect(result.isError).toBe(true);
      expect(JSON.stringify(result.content)).toContain("invalid_request");
    }),
  ).pipe(Effect.provide(NodeHttpServer.layerTest)),
);
