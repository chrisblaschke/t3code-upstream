import {
  CommandId,
  type EnvironmentId,
  MessageId,
  NodeId,
  OrchestratorMcpFailure,
  type OrchestratorMcpDelegateTaskInput,
  type OrchestratorMcpTaskCancelResult,
  type OrchestratorMcpThreadRun,
  type OrchestratorMcpThreadTimelineItem,
  type OrchestrationV2RemoteTaskChild,
  type OrchestrationV2Subagent,
  type ProjectId,
  type ProviderInstanceId,
  ProviderDriverKind,
  type RunId,
  RuntimeRequestId,
  type ThreadId,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as SecureRandom from "effect/crypto/SecureRandom";
import * as Digest from "effect/crypto/Digest";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Hex from "effect/encoding/Hex";
import * as Semaphore from "effect/Semaphore";
import * as Option from "effect/Option";
import * as Schedule from "effect/Schedule";
import * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";
import type * as Tool from "effect/ai/Tool";
import { withExecutedModelNote } from "@t3tools/provider-core/server/subagentProjection";

import { reportLabel } from "@t3tools/provider-core/server/notification";

import type * as McpInvocationContext from "../mcp/McpInvocationContext.ts";
import { ProjectToolkit } from "../mcp/toolkits/project/tools.ts";
import { ThreadToolkit } from "../mcp/toolkits/thread/tools.ts";
import {
  resolveInteractionMode,
  resolveRuntimeMode,
  taskPrompt,
} from "../mcp/delegatedTaskModes.ts";
import * as ProjectionStore from "../orchestration-v2/ProjectionStore.ts";
import * as ThreadManagement from "../orchestration-v2/ThreadManagementService.ts";
import * as ServerEnvironment from "../environment/ServerEnvironment.ts";
import * as ProjectService from "../project/ProjectService.ts";
import * as RepositoryIdentityResolver from "../project/RepositoryIdentityResolver.ts";
import { forkParked } from "../serverActivation.ts";
import * as PeerForwarding from "./PeerForwarding.ts";
import * as PeerLinks from "./PeerLinks.ts";

/** How long one follow of a remote task waits on its thread there before checking again. */
const FOLLOW_WAIT_MS = 10 * 60 * 1_000;
const QUESTION_POLL = Duration.seconds(15);
const QUESTION_PROBE_TIMEOUT = Duration.seconds(5);
/** A linked environment that stops answering is retried, backing off to this. */
const FOLLOW_MAX_BACKOFF = Duration.minutes(5);
/** How a follower retries; tests step it by hand. */
export const FollowRetrySchedule = Context.Reference<Schedule.Schedule<unknown>>(
  "t3/peer/RemoteDelegation/FollowRetrySchedule",
  {
    defaultValue: () =>
      Schedule.exponential(Duration.seconds(2)).pipe(
        Schedule.modifyDelay(({ duration }) =>
          Effect.succeed(Duration.min(duration, FOLLOW_MAX_BACKOFF)),
        ),
      ),
  },
);

type ThreadScope = McpInvocationContext.McpThreadInvocationScope;

/**
 * `delegate_task` to a linked environment. The task's child is an ordinary
 * thread there, launched through the link; the parent here records the task
 * like a local one, without a child thread, and a follower completes it when
 * the thread there ends, so the parent wakes as it would for a local child.
 */
export class RemoteDelegation extends Context.Service<
  RemoteDelegation,
  {
    readonly delegate: (
      scope: ThreadScope,
      input: OrchestratorMcpDelegateTaskInput & {
        readonly target: NonNullable<OrchestratorMcpDelegateTaskInput["target"]> & {
          readonly environmentId: EnvironmentId;
        };
        /** The parent checkout's commit, which `isolation: "worktree"` checks out there. */
        readonly parentCommit?: string | undefined;
      },
    ) => Effect.Effect<{ readonly taskId: NodeId }, OrchestratorMcpFailure>;
    /**
     * The questions the task's thread there is waiting on. Nothing when the
     * task has ended; an error when the other machine does not answer.
     */
    readonly pendingQuestions: (
      scope: ThreadScope,
      task: OrchestrationV2Subagent,
      options?: { readonly acknowledge?: boolean | undefined },
    ) => Effect.Effect<ReadonlyArray<RuntimeRequestId>, OrchestratorMcpFailure>;
    /** Acknowledge only the question ids carried by a delivered wait snapshot. */
    readonly acknowledgeQuestions: (
      scope: ThreadScope,
      taskId: NodeId,
      remote: OrchestrationV2RemoteTaskChild,
      ids: ReadonlyArray<RuntimeRequestId>,
    ) => Effect.Effect<void, OrchestratorMcpFailure>;
    /**
     * Stops the task's thread there, then completes it here as cancelled. A
     * task that already has its result keeps it; its thread there is still
     * stopped, as cancelling a local task stops follow-up work on its child.
     */
    readonly cancel: (
      scope: ThreadScope,
      task: OrchestrationV2Subagent,
      input: {
        readonly reason?: string | undefined;
        readonly clientRequestId?: string | undefined;
      },
    ) => Effect.Effect<OrchestratorMcpTaskCancelResult, OrchestratorMcpFailure>;
    /**
     * Follows every open remote task until it ends, including after a
     * restart, and a finished one again when a Stop here owes it a stop there.
     */
    readonly start: () => Effect.Effect<void, never, Scope.Scope>;
  }
>()("t3/peer/RemoteDelegation") {}

/** The other side refused because the thread there runs above this caller's modes. */
const isModeRefusal = (error: OrchestratorMcpFailure) =>
  error.code === "runtime_mode_escalation_denied" ||
  error.code === "interaction_mode_escalation_denied";

/**
 * Refusals that following again cannot change: the link was revoked, expired
 * or forgotten, or the thread or run there is gone. Anything else (the other
 * machine asleep, a dropped route) is worth retrying.
 */
const isFinal = (error: OrchestratorMcpFailure) =>
  error.code === "capability_denied" ||
  error.code === "thread_not_found" ||
  error.code === "run_not_found";

const make = Effect.gen(function* () {
  const followRetry = yield* FollowRetrySchedule;
  const forwarding = yield* PeerForwarding.PeerForwarding;
  const links = yield* PeerLinks.PeerLinks;
  const threads = yield* ThreadManagement.ThreadManagementService;
  const projections = yield* ProjectionStore.ProjectionStoreV2;
  const projects = yield* ProjectService.ProjectService;
  const repositoryIdentities = yield* RepositoryIdentityResolver.RepositoryIdentityResolver;
  const random = yield* SecureRandom.SecureRandom;
  const crypto = yield* Digest.Digest;
  const hereId = yield* ServerEnvironment.ServerEnvironment.pipe(
    Effect.flatMap((environment) => environment.getEnvironmentId),
  );
  // Followers live as long as this service, so shutting down stops them.
  const followers = yield* Scope.Scope;

  // One follower runs per open task and holds what it read across a wait of
  // up to FOLLOW_WAIT_MS, so it reads its own task, never the parent's whole
  // task list: a parent with thousands of tasks made every restart decode
  // that list once per open task at the same time and ran out of heap.
  const readTask = (parentThreadId: ThreadId, taskId: NodeId) =>
    threads
      .getThreadRecords(parentThreadId, ["subagents"], { subagentIds: [taskId] })
      .pipe(Effect.map(({ thread, subagents }) => ({ thread, task: subagents[0] })));

  /** The peer's project with this thread's repository, or the one the caller named. */
  const resolveRemoteProject = Effect.fn("RemoteDelegation.resolveRemoteProject")(function* (
    scope: ThreadScope,
    environmentId: EnvironmentId,
    parentProjectId: ProjectId,
    requested: ProjectId | undefined,
  ) {
    if (requested !== undefined) return requested;
    const here = yield* projects.getById(parentProjectId).pipe(
      Effect.map(Option.getOrUndefined),
      Effect.orElseSucceed(() => undefined),
    );
    // The project's own identity is a one-minute cache that reads blank when
    // cold, so resolve it here, as the other side's t3_project_list does.
    const key =
      here === undefined
        ? undefined
        : (yield* repositoryIdentities.resolve(here.workspaceRoot))?.canonicalKey;
    if (key === undefined) {
      return yield* new OrchestratorMcpFailure({
        code: "target_required",
        message: "Pass target.projectId: this thread's project has no repository to match there.",
      });
    }
    const matches: Array<{ readonly id: ProjectId; readonly title: string }> = [];
    let cursor: number | null = 0;
    while (cursor !== null) {
      const listed: Tool.Success<typeof ProjectToolkit.tools.t3_project_list> =
        yield* forwarding.call(scope, ProjectToolkit.tools.t3_project_list, environmentId, {
          cursor,
          limit: 100,
        });
      matches.push(
        ...listed.projects.filter((project) => project.repositoryIdentity?.canonicalKey === key),
      );
      cursor = listed.nextCursor;
    }
    if (matches.length === 1) return matches[0]!.id;
    return yield* new OrchestratorMcpFailure({
      code: "invalid_request",
      message:
        matches.length === 0
          ? `No project there has this repository (${key}). Pass target.projectId; t3_project_list with this environmentId lists them. To bring the repository there, use t3_project_clone and then t3_project_create with this environmentId.`
          : `Several projects there have this repository: ${matches.map((project) => `${project.title} (${project.id})`).join(", ")}. Pass target.projectId.`,
    });
  });

  const delegate: RemoteDelegation["Service"]["delegate"] = Effect.fn("RemoteDelegation.delegate")(
    function* (scope, input) {
      const parent = yield* threads.getThreadRecords(scope.thread.threadId, ["runs"]).pipe(
        Effect.mapError(
          () =>
            new OrchestratorMcpFailure({
              code: "thread_not_found",
              message: "The calling thread was not found.",
            }),
        ),
      );
      const parentRun = parent.runs
        .filter(ThreadManagement.isActiveRun)
        .toSorted((left, right) => right.ordinal - left.ordinal)[0];
      if (
        parentRun === undefined ||
        parentRun.rootNodeId === null ||
        parentRun.providerInstanceId !== scope.thread.providerInstanceId
      ) {
        return yield* new OrchestratorMcpFailure({
          code: "parent_not_active",
          message: "Delegated tasks require an active run owned by this MCP provider session.",
        });
      }
      const parentNodeId = parentRun.rootNodeId;
      const { target } = input;
      // A worktree there checks out this checkout's commit, fetching origin
      // only if absent there; a rift snapshots the checkout there.
      if (input.isolation === "worktree" && input.parentCommit === undefined) {
        return yield* new OrchestratorMcpFailure({
          code: "invalid_request",
          message: "Unable to resolve the parent checkout commit.",
        });
      }
      const workspaceStrategy =
        input.isolation === "worktree" && input.parentCommit !== undefined
          ? ({ type: "worktree", baseRef: input.parentCommit, startFromOrigin: true } as const)
          : input.isolation === "rift"
            ? ({ type: "rift" } as const)
            : undefined;
      // The provider and model inherit as they do for a task here. Another
      // provider there has no model to inherit, so the caller names one.
      const inherited = parent.thread.modelSelection;
      const instanceId = target.providerInstanceId ?? inherited.instanceId;
      const model =
        target.model ?? (instanceId === inherited.instanceId ? inherited.model : undefined);
      if (model === undefined) {
        return yield* new OrchestratorMcpFailure({
          code: "target_required",
          message: "Pass target.model from orchestrator_capabilities with this environmentId.",
        });
      }
      const options =
        target.options ??
        (instanceId === inherited.instanceId && model === inherited.model
          ? inherited.options
          : undefined);
      // Modes asked for are checked against this thread. Omitted ones are left
      // to the other side, which inherits this thread's modes capped by the
      // link's access, so a narrower link still takes the task.
      const runtimeMode =
        input.runtimeMode === undefined || input.runtimeMode === "inherit"
          ? undefined
          : yield* resolveRuntimeMode(parent.thread.runtimeMode, input.runtimeMode);
      const interactionMode =
        input.interactionMode === undefined || input.interactionMode === "inherit"
          ? undefined
          : yield* resolveInteractionMode(parent.thread.interactionMode, input.interactionMode);
      const projectId = yield* resolveRemoteProject(
        scope,
        target.environmentId,
        parent.thread.projectId,
        target.projectId,
      );
      // One key for the launch there and the record here, so a retry finds both.
      // Both are scoped to the provider session, as every key a caller passes is:
      // the forwarder scopes the launch's, and the record's names it.
      const key = input.clientRequestId ?? (yield* random.uuidV4());
      // Either component may fill the caller's 256-character limit. Hash an
      // unambiguous tuple to preserve retries without exceeding that limit.
      const launchKey = yield* crypto
        .digest(
          Digest.sha256,
          new TextEncoder().encode(
            `${scope.thread.threadId.length}:${scope.thread.threadId}${key}`,
          ),
        )
        .pipe(
          Effect.map((digest) => `delegate:${Hex.encode(digest)}`),
          Effect.orDie,
        );
      const task = taskPrompt(input);
      const title = input.title ?? parent.thread.title;
      const launched = yield* forwarding.call(
        scope,
        ProjectToolkit.tools.t3_thread_launch,
        target.environmentId,
        {
          projectId,
          title,
          modelSelection: { instanceId, model, ...(options === undefined ? {} : { options }) },
          ...(runtimeMode === undefined ? {} : { runtimeMode }),
          ...(interactionMode === undefined ? {} : { interactionMode }),
          message: task,
          ...(workspaceStrategy === undefined ? {} : { workspaceStrategy }),
          clientRequestId: launchKey,
          // The thread there shows it is this thread's subagent, with a way back.
          delegatedFrom: {
            environmentId: hereId,
            threadId: parent.thread.id,
            title: parent.thread.title,
          },
        },
      );
      const link = yield* links.get(target.environmentId).pipe(
        Effect.map(Option.getOrUndefined),
        Effect.orElseSucceed(() => undefined),
      );
      const commandId = CommandId.make(
        `command:mcp:${encodeURIComponent(scope.requestNamespace)}:remote-delegate:${encodeURIComponent(key)}`,
      );
      const remoteChild: OrchestrationV2RemoteTaskChild = {
        environmentId: target.environmentId,
        threadId: launched.threadId,
        label: link?.label ?? target.environmentId,
        title,
        ...(launched.runId === null ? {} : { runId: launched.runId }),
      };
      const recorded = yield* threads
        .dispatch({
          type: "delegated_task.remote.request",
          commandId,
          parentThreadId: scope.thread.threadId,
          parentRunId: parentRun.id,
          parentNodeId,
          task,
          ...(input.title === undefined ? {} : { title: input.title }),
          // The driver runs there; this side only labels the task with it.
          driver: ProviderDriverKind.make("remote"),
          modelSelection: launched.modelSelection,
          remoteChild,
          completionWake: input.mode === "wait" ? "settled_only" : "always",
        })
        .pipe(
          // The cause goes to the log; the caller learns only that recording failed.
          Effect.tapError((cause) =>
            Effect.logWarning("remote-delegation.record-failed", {
              parentThreadId: scope.thread.threadId,
              cause,
            }),
          ),
          Effect.mapError(
            () =>
              new OrchestratorMcpFailure({
                code: "orchestration_error",
                message: "Unable to record the delegated task.",
              }),
          ),
          // Nothing here follows a thread the task was never recorded for, so stop it there.
          // A crash between the launch and this record still leaves it running there
          // until a retry with the same clientRequestId records it.
          Effect.tapError(() =>
            stopThere(followScope(scope.thread, remoteChild), remoteChild, "stop").pipe(
              Effect.ignoreCause({ log: true }),
            ),
          ),
        );
      const taskEvent = recorded.storedEvents.find(
        (stored) => stored.event.type === "subagent.updated",
      );
      const taskId =
        taskEvent?.event.type === "subagent.updated"
          ? taskEvent.event.payload.id
          : // A replayed request recorded the task the first time.
            yield* threads.getThreadRecords(scope.thread.threadId, ["subagents"]).pipe(
              Effect.map(
                ({ subagents }) =>
                  subagents.find((task) => task.remoteChild?.threadId === launched.threadId)?.id,
              ),
              Effect.orElseSucceed(() => undefined),
            );
      if (taskId === undefined) {
        return yield* new OrchestratorMcpFailure({
          code: "orchestration_error",
          message: "The delegated task was not recorded.",
        });
      }
      yield* followOne(scope.thread.threadId, taskId).pipe(Effect.forkIn(followers));
      return { taskId };
    },
  );

  /**
   * Completes `task` here with its thread there's final state. Each attempt is its own
   * command, so a failed one never blocks the next; a report refused because the task already
   * has its result (a cancel, a Stop or an earlier report) is done. `stoppedThere` records that
   * nothing is left to stop there, even on a task that already has its result.
   */
  const complete = Effect.fn("RemoteDelegation.complete")(function* (
    parentThreadId: ThreadId,
    taskId: NodeId,
    status: "completed" | "failed" | "cancelled" | "interrupted",
    result: string,
    stoppedThere: "ran-out" | "stopped" | undefined,
    stopCount?: number,
  ) {
    const attempt = yield* random.uuidV4();
    yield* threads
      .dispatch({
        type: "delegated_task.remote.complete",
        commandId: CommandId.make(`command:remote-task-complete:${taskId}:${attempt}`),
        parentThreadId,
        taskId,
        status,
        result,
        ...(stoppedThere === undefined ? {} : { stoppedThere }),
        ...(stoppedThere === undefined ? {} : { stopCount: stopCount ?? 0 }),
      })
      .pipe(
        Effect.catch((error) =>
          readTask(parentThreadId, taskId).pipe(
            Effect.flatMap(({ task }) => (task?.result != null ? Effect.void : Effect.fail(error))),
          ),
        ),
      );
  });

  /**
   * How this environment reaches a task's thread there: as the parent
   * thread, under one namespace per parent, so a stop sent again after a
   * restart or a dropped route is recorded there once.
   */
  const followScope = (
    parent: { readonly threadId: ThreadId; readonly providerInstanceId: ProviderInstanceId },
    remote: OrchestrationV2RemoteTaskChild,
  ): ThreadScope => ({
    environmentId: hereId,
    requestNamespace: `remote-task:${parent.threadId}`,
    thread: {
      threadId: parent.threadId,
      providerSessionId: `remote-task:${remote.threadId}`,
      providerInstanceId: parent.providerInstanceId,
    },
    client: undefined,
    capabilities: new Set(["orchestration"]),
    issuedAt: 0,
  });

  /**
   * Stops the task's own thread there as its Stop button does: its running turn, queued
   * messages, pull request watches and delegated tasks. `key` names the stop (the follower
   * sends one, each cancel its own), and each is recorded there once however often it is sent.
   */
  const stopThere = (
    scope: ThreadScope,
    remote: OrchestrationV2RemoteTaskChild,
    key: string,
    reason?: string,
  ) =>
    forwarding
      .interruptThread(scope, remote.environmentId, {
        threadId: remote.threadId,
        stop: true,
        ...(reason === undefined ? {} : { reason }),
        clientRequestId: `${key}:${remote.threadId}`,
      })
      .pipe(Effect.asVoid);

  /**
   * The last reply `runId` gave on the task's thread there, which is its
   * result, read whole however long it is. A failed run prefers its error
   * from the activity view over an earlier reply, without taking another run's.
   * Led by a note when its provider reported running another model than it asked for.
   */
  const lastReply = Effect.fn("RemoteDelegation.lastReply")(function* (
    scope: ThreadScope,
    remote: OrchestrationV2RemoteTaskChild,
    runId: RunId | null,
    failed = false,
  ) {
    let reply: OrchestratorMcpThreadTimelineItem | undefined;
    let error: OrchestratorMcpThreadTimelineItem | undefined;
    let afterPosition: number | undefined;
    let run: OrchestratorMcpThreadRun | undefined;
    while (true) {
      const read = yield* forwarding.readThread(scope, remote.environmentId, {
        threadId: remote.threadId,
        view: failed ? "activity" : "messages",
        limit: 100,
        runLimit: 50,
        ...(afterPosition === undefined ? {} : { afterPosition }),
      });
      run ??= read.recentRuns.find((candidate) => candidate.runId === runId);
      reply =
        read.items.findLast(
          (item) => item.type === "assistant_message" && (runId === null || item.runId === runId),
        ) ?? reply;
      if (failed) {
        error =
          read.items.findLast(
            (item) => item.type === "error" && (runId === null || item.runId === runId),
          ) ?? error;
      }
      if (!read.hasMore || read.nextPosition === null) break;
      afterPosition = read.nextPosition;
    }
    reply = error ?? reply;
    if (reply === undefined) return undefined;
    let text = reply.text ?? "";
    let textOffset = reply.textTruncated ? (reply.nextTextOffset ?? null) : null;
    while (textOffset !== null) {
      const rest = yield* forwarding.readThread(scope, remote.environmentId, {
        threadId: remote.threadId,
        itemId: reply.itemId,
        textOffset,
        maxCharsPerItem: 50_000,
      });
      const part = rest.items[0];
      if (part === undefined) break;
      text += part.text ?? "";
      textOffset = part.textTruncated ? (part.nextTextOffset ?? null) : null;
    }
    return run === undefined ? text : withExecutedModelNote(text, run.model, run.executedModel);
  });

  /**
   * Why following a task can never succeed, if that is what `error` means.
   * The far side's message already says what was refused and what to do; a
   * link forgotten here only reads as a bad request, so it is named.
   */
  const finalReason = Effect.fn("RemoteDelegation.finalReason")(
    function* (parentThreadId: ThreadId, taskId: NodeId, error: OrchestratorMcpFailure) {
      if (isFinal(error)) return error.message;
      if (error.code !== "invalid_request") return undefined;
      const { task } = yield* readTask(parentThreadId, taskId);
      const environmentId = task?.remoteChild?.environmentId;
      if (environmentId === undefined) return error.message;
      const link = yield* links.get(environmentId);
      return Option.isSome(link) ? undefined : error.message;
    },
    Effect.orElseSucceed(() => undefined),
  );

  /** A task's thread there that ended without a reply. */
  const noReply = "(The thread in the linked environment ended without a reply.)";

  /**
   * Waits for the task's run there to end, completes the task here, and stops it there if it
   * ended here first. A revoked, expired or forgotten link, or a thread or run gone there,
   * fails the task with nothing left to stop; anything else (the other machine asleep, a
   * dropped route) is retried with back-off, and so is completing.
   */
  const follow = (parentThreadId: ThreadId, taskId: NodeId): Effect.Effect<void> =>
    followOnce(parentThreadId, taskId).pipe(
      // `false` means it is still running there: follow again straight away.
      Effect.repeat({ while: (done) => !done }),
      Effect.catchTags({
        OrchestratorMcpFailure: Effect.fn("RemoteDelegation.followFailed")(function* (error) {
          const reason = yield* finalReason(parentThreadId, taskId, error);
          if (reason === undefined) return yield* error;
          // Gone there, or no link to reach it by: nothing left to stop there.
          const { task } = yield* readTask(parentThreadId, taskId);
          const stopCount = task?.remoteChild?.stopCount;
          yield* complete(parentThreadId, taskId, "failed", reason, "stopped", stopCount);
        }),
      }),
      Effect.retry(followRetry),
      Effect.ignoreCause({ log: true }),
    );

  /**
   * One wait on the task's thread there; true once the task here is done.
   * A task completed here first (cancelled, or its parent stopped) is
   * stopped there, and done once that stop is recorded here.
   */
  const followOnce = Effect.fn("RemoteDelegation.followOnce")(function* (
    parentThreadId: ThreadId,
    taskId: NodeId,
  ) {
    const sequence = yield* threads.getThreadEventSequence(parentThreadId);
    const { thread, task } = yield* readTask(parentThreadId, taskId);
    if (task?.remoteChild === undefined) return true;
    const remote = task.remoteChild;
    const scope = followScope(
      { threadId: parentThreadId, providerInstanceId: thread.providerInstanceId },
      remote,
    );
    // Ended here, maybe while this follower backed off from an unreachable
    // machine or before a restart: it still has to be stopped there. So
    // does one its parent here was stopped after, for what followed there.
    if (task.result !== null) {
      if (
        remote.stoppedThere === false ||
        ((task.status === "cancelled" || task.status === "interrupted") &&
          remote.stoppedThere !== true)
      ) {
        const key = remote.stopCount === undefined ? "stop" : `stop-${remote.stopCount}`;
        yield* stopThere(scope, remote, key).pipe(
          // Its thread there now runs above what this side may touch, say
          // because the parent here was lowered since; no retry changes that.
          Effect.catchIf(isModeRefusal, (error) =>
            Effect.logWarning("A linked environment refused to stop a delegated task", {
              taskId,
              reason: error.message,
            }),
          ),
        );
        yield* complete(
          parentThreadId,
          taskId,
          terminalStatus(task.status),
          task.result,
          "stopped",
          remote.stopCount,
        );
        return yield* settled(parentThreadId, taskId);
      }
      return true;
    }
    const endedHere = threads
      .streamStoredEventsFrom({
        threadId: parentThreadId,
        afterSequence: sequence,
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
        Effect.flatMap((ended) =>
          Option.isSome(ended) ? Effect.succeed({ endedHere: true } as const) : Effect.never,
        ),
      );
    const waited = yield* forwarding
      .waitForThread(scope, remote.environmentId, {
        threadId: remote.threadId,
        ...(remote.runId === undefined ? {} : { runId: remote.runId }),
        timeoutMs: FOLLOW_WAIT_MS,
        // A thread there whose run ended may still be waiting on its own
        // tasks to wake it for the answer, which a task here waits for too.
        // An older peer ignores this and reports the run alone.
        untilDelegatedWorkSettled: true,
      })
      .pipe(Effect.raceFirst(endedHere));
    // The next look reads the result and stops it there.
    if ("endedHere" in waited) return false;
    if (waited.timedOut) return false;
    // A task recorded without its run waits on the thread's latest; a
    // thread there with none has nothing left to wait for.
    if (waited.runId === null) {
      const reply = yield* lastReply(scope, remote, null);
      yield* complete(
        parentThreadId,
        taskId,
        reply === undefined ? "failed" : "completed",
        reply ?? "The thread in the linked environment has no run to follow.",
        "ran-out",
      );
      return yield* settled(parentThreadId, taskId);
    }
    if (!isTerminal(waited.status)) return false;
    const resultRun = waited.resultRun ?? {
      runId: remote.runId ?? waited.runId,
      status: waited.status,
    };
    const result = yield* lastReply(scope, remote, resultRun.runId, resultRun.status === "failed");
    yield* complete(
      parentThreadId,
      taskId,
      terminalStatus(resultRun.status),
      result ?? noReply,
      "ran-out",
    );
    return yield* settled(parentThreadId, taskId);
  });

  /**
   * Whether the task is done here after the follower reported its run ran
   * out there. A cancel or Stop that landed while the reply was read keeps
   * its result, and still owes a stop there for what may follow the run;
   * the next look sends it.
   */
  const settled = (parentThreadId: ThreadId, taskId: NodeId) =>
    readTask(parentThreadId, taskId).pipe(
      Effect.map(({ task }) => task?.remoteChild?.stoppedThere === true),
    );

  const pendingThere = (scope: ThreadScope, remote: OrchestrationV2RemoteTaskChild) =>
    forwarding
      .call(scope, ThreadToolkit.tools.t3_pending_request, remote.environmentId, {
        action: "list",
        environmentId: remote.environmentId,
        threadId: remote.threadId,
      })
      .pipe(
        Effect.flatMap((result) =>
          "requestIds" in result
            ? Effect.succeed(result.requestIds)
            : Effect.die("Remote pending-request list returned an unexpected result"),
        ),
      );

  // Wait delivery and asynchronous delivery share a lock: an acknowledgement
  // withdraws any queued wake and prevents a concurrent probe from requeuing it.
  const questionDelivery = yield* Semaphore.make(1);
  const observedQuestions = new Map<NodeId, Set<RuntimeRequestId>>();

  const pendingQuestions: RemoteDelegation["Service"]["pendingQuestions"] = (
    scope,
    task,
    options,
  ) =>
    task.remoteChild === undefined || task.result !== null
      ? Effect.succeed([])
      : pendingThere(scope, task.remoteChild).pipe(
          Effect.timeout(QUESTION_PROBE_TIMEOUT),
          Effect.tap((ids) =>
            options?.acknowledge !== true
              ? Effect.void
              : acknowledgeQuestions(scope, task.id, task.remoteChild!, ids),
          ),
          Effect.mapError((error) =>
            error._tag === "OrchestratorMcpFailure"
              ? error
              : new OrchestratorMcpFailure({
                  code: "orchestration_error",
                  message: `Unable to check pending questions on linked environment "${task.remoteChild!.environmentId}": ${error.message}`,
                }),
          ),
        );

  const acknowledgeQuestions: RemoteDelegation["Service"]["acknowledgeQuestions"] = (
    scope,
    taskId,
    remote,
    ids,
  ) =>
    questionDelivery.withPermits(1)(
      Effect.gen(function* () {
        const observed = observedQuestions.get(taskId) ?? new Set<RuntimeRequestId>();
        observedQuestions.set(taskId, observed);
        for (const id of ids) {
          observed.add(id);
          yield* withdrawQuestion(scope.thread.threadId, remote, id).pipe(
            Effect.mapError(
              (error) =>
                new OrchestratorMcpFailure({
                  code: "orchestration_error",
                  message: `Unable to acknowledge remote question ${id}: ${error.message}`,
                }),
            ),
          );
        }
      }),
    );

  /** One key per question, so each wakes its parent once however often it is seen. */
  const questionKey = (
    parentThreadId: ThreadId,
    remote: OrchestrationV2RemoteTaskChild,
    requestId: RuntimeRequestId,
  ) => `${parentThreadId}:${remote.environmentId}:${remote.threadId}:${requestId}`;

  /**
   * Tells the parent its task there asked a question, as a local task's
   * question does, with what it needs to answer it there.
   */
  const askParent = (
    parentThreadId: ThreadId,
    task: OrchestrationV2Subagent,
    remote: OrchestrationV2RemoteTaskChild,
    requestId: RuntimeRequestId,
  ) => {
    const key = questionKey(parentThreadId, remote, requestId);
    const label = reportLabel(task.title?.trim() || task.prompt);
    return threads.dispatch({
      type: "message.dispatch",
      commandId: CommandId.make(`server:remote-task-question:${key}`),
      threadId: parentThreadId,
      messageId: MessageId.make(`message:remote-task-question:${key}`),
      text: `Delegated task ${task.id}, running in ${remote.label}, asked a question and may be blocked until it is answered. Read it with t3_pending_request action "read" using environmentId ${remote.environmentId}, threadId ${remote.threadId} and requestId ${requestId}. Answer it with t3_pending_request action "respond", passing the same environmentId and threadId, when the answer follows from the task or the user's instructions; otherwise ask the user, then answer it. If it is no longer pending, it was answered or dismissed.`,
      notification: {
        source: { kind: "delegated_task", taskIds: [task.id] },
        outcome: "updated",
        summary: `Delegated task${label === undefined ? "" : ` "${label}"`} has a question`,
      },
      attachments: [],
      dispatchMode: { type: "queue_after_active" },
      createdBy: "agent",
      creationSource: "server",
    });
  };

  /** A question answered there before its parent got to it no longer starts a turn. */
  const withdrawQuestion = Effect.fn("RemoteDelegation.withdrawQuestion")(function* (
    parentThreadId: ThreadId,
    remote: OrchestrationV2RemoteTaskChild,
    requestId: RuntimeRequestId,
  ) {
    const key = questionKey(parentThreadId, remote, requestId);
    const messageId = MessageId.make(`message:remote-task-question:${key}`);
    const { runs } = yield* threads.getThreadRecords(parentThreadId, ["runs"]);
    const queued = runs.find((run) => run.status === "queued" && run.userMessageId === messageId);
    if (queued === undefined) return;
    yield* threads.dispatch({
      type: "queued-run.cancel",
      commandId: CommandId.make(`server:remote-task-question-cancel:${key}`),
      threadId: parentThreadId,
      runId: queued.id,
    });
  });

  /**
   * While a task runs there, checks its thread for questions and puts each
   * new one to the parent here. Runs beside the follower and ends with it.
   */
  const checkQuestions = Effect.fn("RemoteDelegation.checkQuestions")(function* (
    parentThreadId: ThreadId,
    taskId: NodeId,
    questions: { readonly asked: Set<RuntimeRequestId>; recovered: boolean },
  ) {
    const { asked } = questions;
    const records = yield* threads.getThreadRecords(parentThreadId, ["subagents"], {
      subagentIds: [taskId],
    });
    const task = records.subagents.find((candidate) => candidate.id === taskId);
    if (
      task?.remoteChild === undefined ||
      task.result !== null ||
      task.completionDelivery?.state === "disposed" ||
      records.thread.archivedAt !== null ||
      records.thread.deletedAt !== null
    )
      return;
    // As for a local task, a blocking delegate_task wait delivers the
    // question itself; once that run ends, the next check tells the parent.
    if (task.completionWake === "settled_only" && task.runId !== null) {
      const { runs } = yield* threads.getThreadRecords(parentThreadId, ["runs"], {
        runIds: [task.runId],
      });
      if (runs.some(ThreadManagement.isActiveRun)) return;
    }
    const remote = task.remoteChild;
    const scope = followScope(
      { threadId: parentThreadId, providerInstanceId: records.thread.providerInstanceId },
      remote,
    );
    const pending = yield* pendingThere(scope, remote);
    yield* questionDelivery.withPermits(1)(
      Effect.gen(function* () {
        if (!questions.recovered) {
          // A restart loses the in-memory set, but a question answered while
          // offline must not leave its queued wake behind until the task ends.
          const prefix = `message:remote-task-question:${parentThreadId}:${remote.environmentId}:${remote.threadId}:`;
          const { runs } = yield* threads.getThreadRecords(parentThreadId, ["runs"]);
          for (const run of runs) {
            if (run.status === "queued" && run.userMessageId.startsWith(prefix)) {
              asked.add(RuntimeRequestId.make(run.userMessageId.slice(prefix.length)));
            }
          }
          questions.recovered = true;
        }
        for (const requestId of pending) {
          if (asked.has(requestId) || observedQuestions.get(taskId)?.has(requestId)) continue;
          yield* askParent(parentThreadId, task, remote, requestId);
          asked.add(requestId);
        }
        for (const requestId of asked) {
          if (pending.includes(requestId)) continue;
          yield* withdrawQuestion(parentThreadId, remote, requestId);
          asked.delete(requestId);
        }
      }),
    );
  });
  const watchQuestions = (
    parentThreadId: ThreadId,
    taskId: NodeId,
    questions: { readonly asked: Set<RuntimeRequestId>; recovered: boolean },
  ) =>
    checkQuestions(parentThreadId, taskId, questions).pipe(
      // The other machine asleep or a dropped route: look again next time.
      Effect.catchCause((cause) =>
        Effect.logDebug("remote-delegation.question-check-failed", { taskId, cause }),
      ),
      Effect.repeat(Schedule.spaced(QUESTION_POLL)),
      Effect.asVoid,
    );

  const following = new Map<NodeId, { again: boolean }>();

  /**
   * `follow`, at most once per task. Asked again while it runs, say by a
   * Stop here on a task that already has its result, it looks once more
   * before it ends, so that stop is not missed.
   */
  const followOne = (parentThreadId: ThreadId, taskId: NodeId) =>
    Effect.suspend(() => {
      const running = following.get(taskId);
      if (running !== undefined) {
        running.again = true;
        return Effect.void;
      }
      const state = { again: false };
      following.set(taskId, state);
      const questions = { asked: new Set<RuntimeRequestId>(), recovered: false };
      return follow(parentThreadId, taskId).pipe(
        Effect.raceFirst(
          watchQuestions(parentThreadId, taskId, questions).pipe(Effect.andThen(Effect.never)),
        ),
        Effect.tap(() =>
          questionDelivery.withPermits(1)(
            Effect.gen(function* () {
              const { subagents, runs } = yield* threads.getThreadRecords(parentThreadId, [
                "subagents",
                "runs",
              ]);
              const remote = subagents.find((task) => task.id === taskId)?.remoteChild;
              if (remote !== undefined) {
                // Include wakes recovered after a restart, not just this watcher’s set.
                const prefix = `message:remote-task-question:${parentThreadId}:${remote.environmentId}:${remote.threadId}:`;
                for (const run of runs) {
                  if (run.status !== "queued" || !run.userMessageId.startsWith(prefix)) continue;
                  yield* threads
                    .dispatch({
                      type: "queued-run.cancel",
                      commandId: CommandId.make(
                        `server:remote-task-question-cancel:${run.userMessageId.slice("message:remote-task-question:".length)}`,
                      ),
                      threadId: parentThreadId,
                      runId: run.id,
                    })
                    .pipe(
                      Effect.catch((cause) =>
                        Effect.gen(function* () {
                          // The snapshot can race a start or cancellation. Neither
                          // needs cleanup, and neither may stop the other wakes.
                          const current = yield* threads.getThreadRecords(parentThreadId, ["runs"]);
                          if (
                            current.runs.some(
                              (entry) => entry.id === run.id && entry.status === "queued",
                            )
                          ) {
                            yield* Effect.logWarning("remote-delegation.question-cancel-failed", {
                              taskId,
                              runId: run.id,
                              cause,
                            });
                          }
                        }),
                      ),
                    );
                }
              }
            }).pipe(
              // Startup follows all recovered tasks together. Cleanup of one
              // task must never interrupt the other followers.
              Effect.catchCause((cause) =>
                Effect.logWarning("remote-delegation.question-cleanup-failed", { taskId, cause }),
              ),
              Effect.ensuring(
                Effect.sync(() => {
                  observedQuestions.delete(taskId);
                  questions.asked.clear();
                }),
              ),
            ),
          ),
        ),
        // Checked and released in one step, so a request between them is never lost.
        Effect.repeat({
          while: () => {
            if (state.again) {
              state.again = false;
              return true;
            }
            following.delete(taskId);
            return false;
          },
        }),
        Effect.ensuring(
          Effect.sync(() => {
            if (following.get(taskId) === state) following.delete(taskId);
          }),
        ),
      );
    });

  const cancel: RemoteDelegation["Service"]["cancel"] = Effect.fn("RemoteDelegation.cancel")(
    function* (scope, task, input) {
      const remote = task.remoteChild;
      if (remote === undefined) {
        return yield* new OrchestratorMcpFailure({
          code: "task_not_found",
          message: `Delegated task ${task.id} is not remote.`,
        });
      }
      const { reason } = input;
      const there = followScope(scope.thread, remote);
      // A later cancel stops what started there since; a retry of this one, nothing new.
      const key = `cancel:${encodeURIComponent(scope.requestNamespace)}:${
        input.clientRequestId ?? (yield* random.uuidV4())
      }`;
      if (task.result !== null) {
        // Best effort: the result stands whether or not the stop gets there.
        yield* stopThere(there, remote, key, reason).pipe(
          Effect.andThen(
            complete(
              task.threadId,
              task.id,
              terminalStatus(task.status),
              task.result,
              "stopped",
              remote.stopCount,
            ),
          ),
          Effect.ignoreCause({ log: true }),
        );
        return {
          taskId: task.id,
          status:
            task.status === "completed" || task.status === "failed" || task.status === "interrupted"
              ? task.status
              : "cancelled",
        } satisfies OrchestratorMcpTaskCancelResult;
      }
      // A link that is gone or a machine that is down still cancels the task
      // here; its follower keeps trying to stop it there, across restarts.
      const unstopped = yield* stopThere(there, remote, key, reason).pipe(
        Effect.as(undefined),
        Effect.catch((error) => Effect.succeed(error.message)),
      );
      const cancelled = reason === undefined ? "Cancelled." : `Cancelled: ${reason}`;
      yield* complete(
        task.threadId,
        task.id,
        "cancelled",
        unstopped === undefined
          ? cancelled
          : `${cancelled} It could not be stopped in ${remote.label} and may still run there: ${unstopped}`,
        unstopped === undefined ? "stopped" : undefined,
        remote.stopCount,
      ).pipe(
        Effect.tapError((cause) =>
          Effect.logWarning("remote-delegation.cancel-record-failed", { taskId: task.id, cause }),
        ),
        Effect.mapError(
          () =>
            new OrchestratorMcpFailure({
              code: "orchestration_error",
              message: "Unable to record the cancellation.",
            }),
        ),
      );
      return {
        taskId: task.id,
        status: "cancel_requested",
      } satisfies OrchestratorMcpTaskCancelResult;
    },
  );

  const start: RemoteDelegation["Service"]["start"] = () =>
    forkParked(
      Effect.gen(function* () {
        // A finished task's follower has ended, so a Stop here that owes its
        // thread there a stop starts it again. Started before the scan below;
        // a Stop that slips between the two stays owed for the next start.
        yield* Stream.runForEach(threads.streamDomainEvents, (event) =>
          event.type === "subagent.updated" && event.payload.remoteChild?.stoppedThere === false
            ? followOne(event.threadId, event.payload.id).pipe(Effect.forkIn(followers))
            : Effect.void,
        ).pipe(
          Effect.ignoreCause({ log: true }),
          Effect.forkIn(followers, { startImmediately: true }),
        );
        const open = yield* projections.getOpenRemoteDelegatedTasks;
        yield* Effect.forEach(
          open,
          ({ parentThreadId, taskId }) => followOne(parentThreadId, taskId),
          {
            concurrency: "unbounded",
            discard: true,
          },
        );
      }).pipe(Effect.ignoreCause({ log: true })),
    );

  return RemoteDelegation.of({ delegate, pendingQuestions, acknowledgeQuestions, cancel, start });
});

const isTerminal = (status: string) =>
  status === "completed" ||
  status === "failed" ||
  status === "cancelled" ||
  status === "interrupted" ||
  status === "rolled_back";

const terminalStatus = (status: string): "completed" | "failed" | "cancelled" | "interrupted" =>
  status === "completed" || status === "failed" || status === "interrupted" ? status : "cancelled";

export const layer = Layer.effect(RemoteDelegation, make);
