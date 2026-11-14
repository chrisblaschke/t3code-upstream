/**
 * Idempotently sync CLI messages and import new sessions from the shared dir.
 * Skip in-flight threads and writes older than the provider thread's update
 * so T3's own turns are never copied back.
 */
import {
  EventId,
  MessageId,
  NodeId,
  type OrchestrationV2ConversationMessage,
  type OrchestrationV2DomainEvent,
  type OrchestrationV2Notification,
  type OrchestrationV2Run,
  type OrchestrationV2TurnItem,
  PlanId,
  ProviderDriverKind,
  type ProviderInstanceConfigMap,
  resolveProviderInstanceEnabled,
  RunId,
  ThreadId,
  TurnItemId,
} from "@t3tools/contracts";
import * as HostProcess from "@t3tools/shared/HostProcess";
import * as Cause from "effect/Cause";
import * as DateTime from "effect/DateTime";
import * as Schema from "effect/Schema";
import * as Duration from "effect/Duration";
import * as Clock from "effect/Clock";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as Semaphore from "effect/Semaphore";
import * as Stream from "effect/Stream";
import * as SqlClient from "effect/sql/SqlClient";

import * as EventSink from "../orchestration-v2/EventSink.ts";
import * as ThreadCommandExecutor from "../orchestration-v2/ThreadCommandExecutor.ts";
import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import * as Orchestrator from "../orchestration-v2/Orchestrator.ts";
import {
  piFileChangeFields,
  piPlanSteps,
  piResultPreviewPath,
} from "@t3tools/provider-pi/server/session";
import { ServerConfig } from "../config.ts";
import { notificationTurnItem } from "@t3tools/provider-core/server/notification";
import { resolvePiSessionsDir } from "@t3tools/provider-pi/server/usage";
import { mergeProviderInstanceEnvironment } from "@t3tools/provider-core/server/instanceEnvironment";
import { AgentSessionImporter, messageEvents } from "./AgentSessionImporter.ts";
import {
  decodePiTranscriptEntry,
  piMessagesAfterHead,
  type DecodedTranscriptRecord,
} from "./AgentSessionScanner.ts";
import * as ProjectService from "./ProjectService.ts";
import * as ServerSettings from "../serverSettings.ts";
import {
  type PiAgentTurn,
  type PiNotice,
  type PiThought,
  type PiToolActivity,
  piAgentTurns,
  piBranchEntryAtOrBefore,
  piToolActivityAfterHead,
  piTurnEnded,
  parsePiSessionEntries,
  type RawEntry,
} from "./piSessionItems.ts";
import { makeThreadTitleFollower } from "../orchestration-v2/ThreadTitleFollower.ts";

/** Timeline summary for a Pi-initiated turn, from the message that triggered it. */
function piWakeSummary(detail: string | null): string {
  const firstLine = detail?.trim().split("\n")[0]?.trim().replace(/:$/, "").trim() ?? "";
  if (firstLine.length === 0) return "Pi continued on its own";
  return firstLine.length > 120 ? `${firstLine.slice(0, 119)}…` : firstLine;
}

/** An extension's persisted notice as a timeline system notice. */
function piNoticeTurnItemEvent(input: {
  readonly threadId: ThreadId;
  readonly notice: PiNotice;
  readonly runId: RunId | null;
  readonly ordinal: number;
}): OrchestrationV2DomainEvent {
  const { notice } = input;
  const at = DateTime.makeUnsafe(notice.at);
  const lines = notice.message.split("\n");
  const message = notice.message.trim();
  return {
    id: EventId.make(`${SYNC_EVENT_PREFIX}:notice:${input.threadId}:${notice.entryId}`),
    type: "turn-item.updated",
    threadId: input.threadId,
    occurredAt: at,
    payload: {
      id: TurnItemId.make(`${SYNC_EVENT_PREFIX}:notice:${input.threadId}:${notice.entryId}`),
      threadId: input.threadId,
      runId: input.runId,
      nodeId: null,
      providerThreadId: null,
      providerTurnId: null,
      nativeItemRef: { driver: PI_DRIVER, nativeId: notice.entryId, strength: "strong" },
      parentItemId: null,
      ordinal: input.ordinal,
      status: "completed",
      title: lines[0]!.trim(),
      startedAt: at,
      completedAt: at,
      updatedAt: at,
      // Upstream's live Pi notices are system notices; imported ones match.
      type: "system_notice",
      message,
    },
  };
}

/** A Pi tool call as timeline events, shaped like the Pi adapter's live items. */
export function piToolTurnItemEvents(input: {
  readonly threadId: ThreadId;
  readonly tool: PiToolActivity;
  readonly runId: RunId | null;
  readonly ordinal: number;
  /** The process that ran the call is gone and no result will come. */
  readonly endedAt?: DateTime.Utc;
  /** The file the row shows for the result (see `piResultPreviewPath`). */
  readonly viewedImagePath?: string | undefined;
}): ReadonlyArray<OrchestrationV2DomainEvent> {
  const { tool } = input;
  const startedAt = DateTime.makeUnsafe(tool.startedAt);
  const completedAt =
    tool.result === null ? (input.endedAt ?? null) : DateTime.makeUnsafe(tool.result.completedAt);
  const status =
    tool.result === null
      ? input.endedAt === undefined
        ? "running"
        : "interrupted"
      : tool.result.isError
        ? "failed"
        : "completed";
  const output = tool.result?.text ?? "";
  const base = {
    id: TurnItemId.make(`${SYNC_EVENT_PREFIX}:tool:${input.threadId}:${tool.toolCallId}`),
    threadId: input.threadId,
    runId: input.runId,
    nodeId: null,
    providerThreadId: null,
    providerTurnId: null,
    nativeItemRef: { driver: PI_DRIVER, nativeId: tool.toolCallId, strength: "strong" as const },
    parentItemId: null,
    ordinal: input.ordinal,
    status,
    title: tool.name,
    startedAt,
    completedAt,
    updatedAt: completedAt ?? startedAt,
  } as const;
  const command = typeof tool.args.command === "string" ? tool.args.command : undefined;
  const path =
    typeof tool.args.path === "string"
      ? tool.args.path
      : typeof tool.args.file_path === "string"
        ? tool.args.file_path
        : undefined;
  const planSteps =
    tool.result === null || tool.result.isError ? null : piPlanSteps(tool.name, tool.args);
  if (planSteps !== null) {
    // SoL-Pi's plan: the run's task list, as the live adapter shows it.
    const scope = input.runId ?? input.threadId;
    const planId = PlanId.make(`plan:pi:${scope}`);
    const nodeId = NodeId.make(`${SYNC_EVENT_PREFIX}:plan:${scope}`);
    const occurredAt = completedAt ?? startedAt;
    return [
      {
        id: EventId.make(`${SYNC_EVENT_PREFIX}:plan:${input.threadId}:${tool.toolCallId}`),
        type: "plan.updated",
        threadId: input.threadId,
        occurredAt,
        payload: {
          id: planId,
          threadId: input.threadId,
          runId: input.runId,
          nodeId,
          kind: "todo_list",
          status: planSteps.every((step) => step.status === "completed") ? "completed" : "active",
          steps: planSteps,
        },
      },
      {
        id: EventId.make(
          `${SYNC_EVENT_PREFIX}:tool:${input.threadId}:${tool.toolCallId}:${status}`,
        ),
        type: "turn-item.updated",
        threadId: input.threadId,
        occurredAt,
        payload: { ...base, type: "todo_list", planId, steps: planSteps },
      },
    ];
  }
  const payload: OrchestrationV2TurnItem =
    tool.name === "bash" && command !== undefined
      ? {
          ...base,
          type: "command_execution",
          input: command,
          ...(output.length > 0 ? { output } : {}),
          ...(tool.result?.isError ? { outputIndicatesFailure: true } : {}),
        }
      : (tool.name === "edit" || tool.name === "write") && path !== undefined && path.trim() !== ""
        ? {
            ...base,
            type: "file_change",
            fileName: path,
            ...(tool.result !== null && !tool.result.isError
              ? piFileChangeFields(tool.name, tool.args, tool.result.details)
              : {}),
          }
        : {
            ...base,
            type: "dynamic_tool",
            toolName: tool.name,
            input: tool.args,
            ...(output.length > 0 ? { output } : {}),
            ...(input.viewedImagePath === undefined
              ? {}
              : { viewedImagePath: input.viewedImagePath }),
          };
  return [
    {
      id: EventId.make(`${SYNC_EVENT_PREFIX}:tool:${input.threadId}:${tool.toolCallId}:${status}`),
      type: "turn-item.updated",
      threadId: input.threadId,
      occurredAt: completedAt ?? startedAt,
      payload,
    },
  ];
}

/** A thinking block as the reasoning item the Pi adapter streams live. */
function piThoughtTurnItemEvent(input: {
  readonly threadId: ThreadId;
  readonly thought: PiThought;
  readonly runId: RunId | null;
  readonly ordinal: number;
}): OrchestrationV2DomainEvent {
  const { thought } = input;
  const nativeId = `${thought.entryId}:${thought.blockIndex}`;
  const startedAt = DateTime.makeUnsafe(thought.startedAt);
  const completedAt = DateTime.max(startedAt, DateTime.makeUnsafe(thought.completedAt));
  return {
    id: EventId.make(`${SYNC_EVENT_PREFIX}:thought:${input.threadId}:${nativeId}`),
    type: "turn-item.updated",
    threadId: input.threadId,
    occurredAt: completedAt,
    payload: {
      id: TurnItemId.make(`${SYNC_EVENT_PREFIX}:thought:${input.threadId}:${nativeId}`),
      threadId: input.threadId,
      runId: input.runId,
      nodeId: null,
      providerThreadId: null,
      providerTurnId: null,
      nativeItemRef: { driver: PI_DRIVER, nativeId, strength: "strong" },
      parentItemId: null,
      ordinal: input.ordinal,
      status: "completed",
      title: null,
      startedAt,
      completedAt,
      updatedAt: completedAt,
      type: "reasoning",
      text: thought.text,
      streaming: false,
    },
  };
}

const SYNC_EVENT_PREFIX = "pi-session-sync:v1";
const CLI_RUN_PREFIX = "run:pi-cli:";
/** Sort room per branch entry: one slot per content block of a message. */
const STEP_SLOT = 1024;
const PI_DRIVER = ProviderDriverKind.make("pi");
const DEBOUNCE = Duration.millis(1_500);
/** Reconcile projection changes that do not touch a Pi file. */
const FULL_SYNC_INTERVAL_MS = 5 * 60 * 1000;
const fileSignature = (info: FileSystem.File.Info) =>
  `${info.dev}:${Option.getOrNull(info.ino)}:${Option.getOrNull(info.birthtime)?.getTime()}:${Option.getOrNull(info.mtime)?.getTime()}:${info.size}`;
type FollowSnapshot = {
  readonly providerThreadId: string;
  readonly file: string;
  readonly signature: string;
  readonly quiet: boolean;
  readonly lastFullMs: number;
};
/** Newly found CLI sessions active this recently land outside the settled shelf. */
const ACTIVE_WINDOW_MS = 24 * 60 * 60 * 1000;
// A full day of agent work easily passes 16 MiB, and parsing 20 MiB takes
// ~30 ms. This only guards against a runaway file; skipping one is logged.
const MAX_SESSION_BYTES = 256 * 1024 * 1024;

class PiSessionChangedWhileReading extends Schema.TaggedError<PiSessionChangedWhileReading>()(
  "PiSessionChangedWhileReading",
  { file: Schema.String },
) {}

/** Parsed JSONL is retained for active followers, with a soft heap-size LRU bound. */
export function makePiSessionFileReader(fileSystem: FileSystem.FileSystem) {
  type Entries = { entries: Array<RawEntry>; transcript: Array<DecodedTranscriptRecord> };
  type Cached = Entries & {
    size: number;
    heapBytes: number;
    offset: number;
    identity: string;
    signature: string;
    prefix: Buffer;
    suffix: Buffer;
  };
  const cache = new Map<string, Cached>();
  const lock = Semaphore.makeUnsafe(1);
  const budget = 64 * 1024 * 1024;
  const decoder = new TextDecoder();
  const drop = (file: string) => cache.delete(file);
  const parse = (text: string): Entries => {
    const entries: Array<RawEntry> = [];
    const transcript: Array<DecodedTranscriptRecord> = [];
    for (const line of text.split("\n")) {
      const entry = parsePiSessionEntries(line)[0];
      if (entry === undefined) continue;
      entries.push(entry);
      // Unlike bounded initial imports, live following must decode every append.
      const decoded = decodePiTranscriptEntry(entry);
      if (Option.isSome(decoded)) transcript.push(decoded.value);
    }
    return { entries, transcript };
  };
  // Estimate the retained raw and decoded object graphs, not the source bytes.
  // Strings use a conservative UTF-16 size; shared strings may be counted twice.
  const estimateHeapBytes = ({ entries, transcript }: Entries) => {
    const pending: Array<unknown> = [...entries, ...transcript];
    const seen = new WeakSet<object>();
    let bytes = 16 * pending.length;
    while (pending.length > 0) {
      const value = pending.pop();
      if (typeof value === "string") bytes += 32 + 2 * value.length;
      else if (typeof value === "object" && value !== null && !seen.has(value)) {
        seen.add(value);
        bytes += 32;
        if (Array.isArray(value)) {
          bytes += 8 * value.length;
          for (const item of value) pending.push(item);
        } else {
          for (const [key, item] of Object.entries(value)) {
            bytes += 16 + 2 * key.length;
            pending.push(item);
          }
        }
      }
    }
    return bytes;
  };
  const read = (file: string) =>
    Effect.scoped(
      Effect.gen(function* () {
        const handle = yield* fileSystem.open(file);
        const info = yield* handle.stat;
        const size = Number(info.size);
        if (size > MAX_SESSION_BYTES) {
          drop(file);
          return null;
        }
        const identity = Option.isSome(info.ino)
          ? `${info.dev}:${info.ino.value}:${Option.getOrNull(info.birthtime)?.getTime()}`
          : null;
        const signature = fileSignature(info);
        const range = Effect.fnUntraced(function* (start: number, end: number) {
          yield* handle.seek(BigInt(start), "start");
          const chunks: Array<Uint8Array> = [];
          let remaining = end - start;
          while (remaining > 0) {
            const chunk = yield* handle.readAlloc(remaining);
            if (Option.isNone(chunk) || chunk.value.length === 0) {
              return yield* new PiSessionChangedWhileReading({ file });
            }
            chunks.push(chunk.value);
            remaining -= chunk.value.length;
          }
          return Buffer.concat(chunks);
        });
        let previous = cache.get(file);
        if (
          previous !== undefined &&
          identity !== null &&
          previous.identity === identity &&
          previous.signature === signature
        ) {
          // Re-read only the pending tail, if any (it is not part of cached entries).
        } else if (
          previous !== undefined &&
          identity !== null &&
          previous.identity === identity &&
          size > previous.size
        ) {
          // Detect ordinary truncate-and-regrow rewrites, even on the same inode.
          const prefix = yield* range(0, previous.prefix.length);
          const suffix = yield* range(previous.size - previous.suffix.length, previous.size);
          if (!prefix.equals(previous.prefix) || !suffix.equals(previous.suffix))
            previous = undefined;
        } else {
          previous = undefined;
        }
        const offset = previous?.offset ?? 0;
        const bytes = yield* range(offset, size);
        const completeBytes = bytes.lastIndexOf(10) + 1;
        const added = parse(decoder.decode(bytes.subarray(0, completeBytes)));
        const entries = previous?.entries ?? [];
        const transcript = previous?.transcript ?? [];
        for (const entry of added.entries) entries.push(entry);
        for (const entry of added.transcript) transcript.push(entry);
        // Accept a valid final record without a newline, as the old reader did,
        // but never commit it: the next append must re-read it from its first byte.
        const tail = parse(decoder.decode(bytes.subarray(completeBytes)));
        const prefix =
          previous?.prefix.length === 256 ? previous.prefix : yield* range(0, Math.min(size, 256));
        const suffix = yield* range(Math.max(0, size - 256), size);
        cache.delete(file);
        if (identity !== null) {
          cache.set(file, {
            entries,
            transcript,
            size,
            heapBytes: (previous?.heapBytes ?? 0) + estimateHeapBytes(added),
            offset: offset + completeBytes,
            identity,
            signature,
            prefix,
            suffix,
          });
          let total = 0;
          for (const value of cache.values()) total += value.heapBytes;
          // Keep the newest file even over budget so a large active session
          // still parses only appends. It is then the only retained file.
          while (cache.size > 1 && (total > budget || cache.size > 32)) {
            const oldest = cache.keys().next().value!;
            total -= cache.get(oldest)!.heapBytes;
            cache.delete(oldest);
          }
        }
        return {
          entries: tail.entries.length === 0 ? entries : entries.concat(tail.entries),
          transcript:
            tail.transcript.length === 0 ? transcript : transcript.concat(tail.transcript),
        };
      }),
    ).pipe(
      Effect.onError(() =>
        Effect.sync(() => {
          drop(file);
        }),
      ),
      lock.withPermits(1),
    );
  return {
    read,
    drop,
    retain: (files: ReadonlySet<string>) => {
      for (const file of cache.keys()) if (!files.has(file)) drop(file);
    },
  };
}

/** Use native discovery's path rules; unresolved secrets must never become paths. */
export const piSessionWatchDirectories = Effect.fn(function* (
  instances: ProviderInstanceConfigMap,
  sessionDir: string,
) {
  const environment = yield* HostProcess.Environment;
  const home = yield* HostProcess.HomeDirectory;
  const path = yield* Path.Path;
  const directories = new Set([sessionDir]);
  for (const instance of Object.values(instances)) {
    if (instance.driver !== "pi" || !resolveProviderInstanceEnabled(instance)) continue;
    const env = yield* mergeProviderInstanceEnvironment(instance.environment, environment);
    directories.add(resolvePiSessionsDir(env, home, path));
  }
  return directories;
});

export const makePiSessionSync = Effect.gen(function* () {
  const threadCommands = yield* ThreadCommandExecutor.ThreadCommandExecutor;
  const fileSystem = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const sql = yield* SqlClient.SqlClient;
  const environment = yield* HostProcess.Environment;
  const orchestrator = yield* Orchestrator.OrchestratorV2;
  const eventSink = yield* EventSink.EventSinkV2;
  const projects = yield* ProjectService.ProjectService;
  const importer = yield* AgentSessionImporter;
  const { stateDir } = yield* ServerConfig;
  const toolImagePath = (tool: PiToolActivity) =>
    tool.result === null
      ? Effect.void
      : piResultPreviewPath(stateDir, tool.result).pipe(
          Effect.provideService(FileSystem.FileSystem, fileSystem),
        );

  const homeDirectory = yield* HostProcess.HomeDirectory;
  const sessionDir = resolvePiSessionsDir(environment, homeDirectory, path);
  const idAllocator = yield* IdAllocator.IdAllocatorV2;
  const titles = yield* makeThreadTitleFollower;
  const oversizeLogged = new Set<ThreadId>();
  const missingLogged = new Set<ThreadId>();
  const followed = new Map<ThreadId, FollowSnapshot>();
  const sessionFiles = makePiSessionFileReader(fileSystem);

  // One follow pass at a time, so a pass reads what the previous one wrote.
  const followLock = yield* Semaphore.make(1);
  const followThread = (threadId: ThreadId, providerThreadId: string) =>
    threadCommands.withLock(threadId, followThreadUnlocked(threadId, providerThreadId)).pipe(
      Effect.flatMap((titleKey) =>
        titleKey === undefined ? Effect.void : titles.turnEnded(threadId, titleKey),
      ),
      followLock.withPermits(1),
    );
  // Keep the stat-only path outside the traced full pass as well as the DB.
  const followThreadUnlocked = Effect.fnUntraced(function* (
    threadId: ThreadId,
    providerThreadId: string,
  ) {
    const now = yield* DateTime.now;
    const cached = followed.get(threadId);
    if (
      cached?.providerThreadId === providerThreadId &&
      cached.quiet &&
      DateTime.toEpochMillis(now) - cached.lastFullMs < FULL_SYNC_INTERVAL_MS
    ) {
      const info = yield* Effect.option(fileSystem.stat(cached.file));
      if (Option.isSome(info) && fileSignature(info.value) === cached.signature) return;
    }
    // Disable the fast path until this pass succeeds, but keep the previous
    // signature (so a failed same-mtime append is still detected on retry)
    // and what the last good pass saw (so a reopened run is too).
    if (cached !== undefined) followed.set(threadId, { ...cached, lastFullMs: 0 });
    return yield* followThreadFull(threadId, providerThreadId, now, cached);
  });
  const followThreadFull = Effect.fn("PiSessionSync.followThread")(function* (
    threadId: ThreadId,
    providerThreadId: string,
    now: DateTime.Utc,
    cached: FollowSnapshot | undefined,
  ) {
    const records = yield* orchestrator.getThreadRecords(threadId, ["providerThreads", "runs"]);
    const { thread } = records;
    const providerThread = records.providerThreads.find((entry) => entry.id === providerThreadId);
    if (providerThread === undefined) {
      if (cached !== undefined) sessionFiles.drop(cached.file);
      followed.delete(threadId);
      return;
    }
    const file = providerThread.nativeThreadRef?.nativeId;
    // No head yet: read from the start.
    const head = providerThread.nativeConversationHeadRef?.nativeId ?? null;
    const cliRunPrefix = `${CLI_RUN_PREFIX}${threadId}:`;
    const hasOpenCliRun = records.runs.some(
      (run) => run.id.startsWith(cliRunPrefix) && run.completedAt === null,
    );
    if (
      !file ||
      providerThread.status === "active" ||
      thread.activeProviderThreadId !== providerThread.id ||
      thread.archivedAt !== null ||
      thread.deletedAt !== null ||
      records.runs.some((run) => run.completedAt === null && !run.id.startsWith(cliRunPrefix))
    ) {
      if (cached !== undefined) sessionFiles.drop(cached.file);
      followed.delete(threadId);
      return;
    }

    // A deleted session file (a removed temp project) has
    // nothing left to follow; stat failing on it every pass is just noise.
    const stat = yield* Effect.option(fileSystem.stat(file));
    if (Option.isNone(stat)) {
      sessionFiles.drop(file);
      followed.delete(threadId);
      if (!missingLogged.has(threadId)) {
        missingLogged.add(threadId);
        yield* Effect.logInfo("Pi session file is gone; not following it", { threadId, file });
      }
      // No span remains to read, but a run must not stay open.
      if (hasOpenCliRun) {
        const events: Array<OrchestrationV2DomainEvent> = [];
        for (const run of records.runs) {
          if (!run.id.startsWith(cliRunPrefix) || run.completedAt !== null) continue;
          events.push({
            id: yield* idAllocator.allocate.event({ threadId }),
            type: "run.updated",
            threadId,
            runId: run.id,
            providerInstanceId: run.providerInstanceId,
            occurredAt: now,
            payload: {
              ...run,
              status: "completed",
              completedAt: DateTime.max(run.startedAt ?? now, now),
            },
          });
        }
        yield* eventSink.write({ events });
      }
      return;
    }
    const info = stat.value;
    const mtimeMs = Option.match(info.mtime, { onNone: () => 0, onSome: (at) => at.getTime() });
    const signature = fileSignature(info);
    const fileChanged =
      mtimeMs > DateTime.toEpochMillis(providerThread.updatedAt) ||
      (cached !== undefined && (cached.file !== file || cached.signature !== signature));
    if (cached !== undefined && cached.file !== file) sessionFiles.drop(cached.file);
    const remember = (quiet: boolean) => {
      followed.set(threadId, {
        providerThreadId,
        file,
        signature,
        quiet,
        lastFullMs: DateTime.toEpochMillis(now),
      });
    };
    // Reread on first look or a reopened CLI run to fix unfinished spans.
    // The five-minute backstop rechecks the projection, not gigabytes of files.
    const firstLook = cached === undefined || (cached.quiet && hasOpenCliRun);
    // An open CLI run (left by an earlier pass) is closed on the next one;
    // an unchanged file with no open run has nothing new.
    if (!fileChanged && !firstLook && !hasOpenCliRun) {
      remember(!hasOpenCliRun);
      return;
    }
    if (Number(info.size) > MAX_SESSION_BYTES) {
      sessionFiles.drop(file);
      if (!oversizeLogged.has(threadId)) {
        oversizeLogged.add(threadId);
        yield* Effect.logWarning("Pi session file too large to follow", {
          threadId,
          file,
          bytes: Number(info.size),
        });
      }
      return;
    }

    const parsed = yield* sessionFiles.read(file);
    if (parsed === null) return;
    const contents = parsed.entries;
    let from = head;
    let update = piMessagesAfterHead(parsed.transcript, from, DateTime.formatIso(now));
    if (update === null && head !== null) {
      // The head left the active branch (Pi retried or navigated the tree, or
      // T3's last turn couldn't record its leaf). Entries written before the
      // provider thread's last update are already in the thread.
      from = piBranchEntryAtOrBefore(contents, DateTime.formatIso(providerThread.updatedAt));
      update = piMessagesAfterHead(parsed.transcript, from, DateTime.formatIso(now));
      yield* Effect.logInfo("Pi session head left the active branch; resuming by time", {
        threadId,
        head,
        resumeAfter: from,
      });
    }
    const activity = piToolActivityAfterHead(contents, from, DateTime.formatIso(now));
    const { turns, turnIdByEntryId } = activity ?? piAgentTurns(contents, DateTime.formatIso(now));

    // A run spans its agent turn, prompt to last reply.
    const cliRunId = (entryId: string) => RunId.make(`${cliRunPrefix}${entryId}`);
    const turnSpan = (turn: PiAgentTurn, startedAt: DateTime.Utc) => ({
      status: "completed" as const,
      completedAt: DateTime.max(startedAt, DateTime.makeUnsafe(turn.endedAt)),
    });
    const spanEvents: Array<OrchestrationV2DomainEvent> = [];
    for (const run of records.runs) {
      if (!run.id.startsWith(cliRunPrefix) || run.startedAt === null) continue;
      const turn = turns.get(run.id.slice(cliRunPrefix.length));
      // A navigated/pruned branch may no longer contain the open turn: close
      // it, with observation time as a fallback end.
      if (turn === undefined && run.completedAt !== null) continue;
      const { status, completedAt } =
        turn === undefined
          ? { status: "completed" as const, completedAt: DateTime.max(run.startedAt, now) }
          : turnSpan(turn, run.startedAt);
      if (
        run.status === status &&
        (run.completedAt === null
          ? completedAt === null
          : completedAt !== null && DateTime.Equivalence(run.completedAt, completedAt))
      )
        continue;
      spanEvents.push({
        // Liveness can change without a file write; reusing a content-based
        // event id would drop a repeated open -> closed transition.
        id: yield* idAllocator.allocate.event({ threadId }),
        type: "run.updated",
        threadId,
        runId: run.id,
        providerInstanceId: run.providerInstanceId,
        occurredAt: now,
        payload: { ...run, status, completedAt },
      });
    }
    // Separate from the item write: a span that can't be recorded must not
    // hold back the transcript.
    let spansWritten = true;
    const writeSpans =
      spanEvents.length === 0
        ? Effect.void
        : eventSink.write({ events: spanEvents }).pipe(
            Effect.catchCause((cause) => {
              spansWritten = false;
              return Effect.logWarning("Could not update Pi run spans", {
                threadId,
                cause: Cause.pretty(cause),
              });
            }),
          );
    const quiet = records.runs.every((run) => {
      if (!run.id.startsWith(cliRunPrefix)) return true;
      const span = spanEvents.find(
        (event) => event.type === "run.updated" && event.runId === run.id,
      );
      return (span?.type === "run.updated" ? span.payload.completedAt : run.completedAt) !== null;
    });
    // Spans don't depend on the head, so they're fixed even when it left the branch.
    if (update === null || activity === null || !fileChanged || update.leafId === from) {
      yield* writeSpans;
      if (spansWritten) remember(quiet);
      return;
    }

    // Messages, thoughts and tool calls in session order. Events are written
    // in this order, which fixes each item's position in its run; within an
    // assistant message, items follow the message's content blocks.
    type Step =
      | { readonly kind: "message"; readonly index: number; readonly sort: number }
      | { readonly kind: "thought"; readonly thought: PiThought; readonly sort: number }
      | { readonly kind: "tool"; readonly tool: PiToolActivity; readonly sort: number }
      | { readonly kind: "notice"; readonly notice: PiNotice; readonly sort: number };
    const slot = (branchIndex: number) => (branchIndex + 1) * STEP_SLOT;
    const steps: Array<Step> = [
      ...update.messages.map((message, index): Step => ({
        kind: "message",
        index,
        sort:
          slot(activity.branchIndexById.get(message.id) ?? 0) +
          1 +
          (activity.textBlockIndexById.get(message.id) ?? 0),
      })),
      ...activity.notices.map((notice): Step => ({
        kind: "notice",
        notice,
        sort: slot(notice.branchIndex),
      })),
      ...activity.thoughts.map((thought): Step => ({
        kind: "thought",
        thought,
        sort: slot(thought.branchIndex) + 1 + thought.blockIndex,
      })),
      ...activity.tools.map((tool): Step => ({
        kind: "tool",
        tool,
        sort: slot(tool.branchIndex) + 1 + tool.blockIndex,
      })),
    ].toSorted((left, right) => left.sort - right.sort);

    // Each agent turn becomes a run: the timeline positions and
    // folds turn items by run, so CLI turns then read like T3's own turns.
    const events: Array<OrchestrationV2DomainEvent> = [];
    const knownRunIds = new Set(records.runs.map((run) => run.id));
    let latestRun = records.runs.toSorted((left, right) => left.ordinal - right.ordinal).at(-1);
    let lastRunOrdinal = providerThread.lastRunOrdinal;
    const startRun = (turn: PiAgentTurn, userMessageId: MessageId) => {
      const startedAt = DateTime.makeUnsafe(turn.startedAt);
      const run: OrchestrationV2Run = {
        id: cliRunId(turn.entryId),
        threadId,
        ordinal: (latestRun?.ordinal ?? 0) + 1,
        providerInstanceId: providerThread.providerInstanceId,
        modelSelection: thread.modelSelection,
        providerThreadId: providerThread.id,
        userMessageId,
        rootNodeId: null,
        activeAttemptId: null,
        ...turnSpan(turn, startedAt),
        requestedAt: startedAt,
        startedAt,
        checkpointId: null,
        contextHandoffId: null,
      };
      latestRun = run;
      lastRunOrdinal = run.ordinal;
      knownRunIds.add(run.id);
      events.push({
        id: EventId.make(`${SYNC_EVENT_PREFIX}:run:${run.id}`),
        type: "run.created",
        threadId,
        runId: run.id,
        providerInstanceId: run.providerInstanceId,
        occurredAt: startedAt,
        payload: run,
      });
      return run.id;
    };
    // A wake (an extension message that restarted an idle agent) gets its own run,
    // as T3 gives the continuation of its own Pi sessions. A live T3 session
    // adopts such turns itself, so leave those to it.
    const startWakeRun = (turn: PiAgentTurn) => {
      const suffix = `pi:${turn.entryId}`;
      const runId = startRun(turn, MessageId.make(`${threadId}:${suffix}`));
      const text = turn.text.length > 0 ? turn.text : piWakeSummary(null);
      const notification: OrchestrationV2Notification = {
        source: { kind: "background_task" },
        outcome: "updated",
        summary: piWakeSummary(turn.text),
        ...(turn.text.length === 0 ? {} : { detail: turn.text }),
      };
      let message: OrchestrationV2ConversationMessage | null = null;
      for (const event of messageEvents({
        threadId,
        index: 0,
        message: { role: "user", text, createdAt: turn.startedAt },
        suffix,
        runId,
        ordinal: slot(activity.branchIndexById.get(turn.entryId) ?? 0),
      })) {
        if (event.type === "message.updated") {
          message = { ...event.payload, createdBy: "agent", notification };
          events.push({ ...event, payload: message });
        } else if (event.type === "turn-item.updated" && message !== null) {
          events.push({ ...event, payload: notificationTurnItem(event.payload, message, []) });
        }
      }
      return runId;
    };
    const runIdFor = (entryId: string): RunId | null => {
      const turnId = turnIdByEntryId.get(entryId);
      const turn = turnId === undefined ? undefined : turns.get(turnId);
      if (turn !== undefined) {
        const runId = cliRunId(turn.entryId);
        if (knownRunIds.has(runId)) return runId;
        if (turn.kind === "wake" && providerThread.providerSessionId === null) {
          return startWakeRun(turn);
        }
      }
      // Turns T3 ran itself, and history imported before the thread was followed.
      return latestRun?.id ?? null;
    };
    for (const step of steps) {
      if (step.kind === "notice") {
        events.push(
          piNoticeTurnItemEvent({
            threadId,
            notice: step.notice,
            runId: runIdFor(step.notice.entryId),
            ordinal: step.sort,
          }),
        );
        continue;
      }
      if (step.kind === "thought") {
        events.push(
          piThoughtTurnItemEvent({
            threadId,
            thought: step.thought,
            runId: runIdFor(step.thought.entryId),
            ordinal: step.sort,
          }),
        );
        continue;
      }
      if (step.kind === "tool") {
        events.push(
          ...piToolTurnItemEvents({
            threadId,
            tool: step.tool,
            ...(step.tool.endedAt === undefined
              ? {}
              : { endedAt: DateTime.makeUnsafe(step.tool.endedAt) }),
            runId: runIdFor(step.tool.entryId),
            ordinal: step.sort,
            viewedImagePath: (yield* toolImagePath(step.tool)) ?? undefined,
          }),
        );
        continue;
      }
      const index = step.index;
      const message = update.messages[index]!;
      const suffix = `pi:${message.id}`;
      const userMessageId = MessageId.make(`${threadId}:${suffix}`);
      const runId =
        message.role === "user"
          ? startRun(
              turns.get(message.id) ?? {
                entryId: message.id,
                kind: "prompt",
                startedAt: message.createdAt,
                endedAt: message.createdAt,
                text: "",
              },
              userMessageId,
            )
          : runIdFor(message.id);
      events.push(
        ...messageEvents({
          threadId,
          index,
          message,
          suffix,
          runId,
          ordinal: step.sort,
        }),
      );
    }
    if (steps.length > 0 && thread.settledOverride === "settled") {
      events.push({
        id: EventId.make(`${SYNC_EVENT_PREFIX}:unsettled:${threadId}:${update.leafId}`),
        type: "thread.unsettled",
        threadId,
        providerInstanceId: thread.providerInstanceId,
        occurredAt: now,
        payload: { ...thread, settledOverride: null, settledAt: null, unsettledAt: now },
      });
    }
    events.push({
      id: EventId.make(`${SYNC_EVENT_PREFIX}:head:${providerThread.id}:${update.leafId}`),
      type: "provider-thread.updated",
      threadId,
      driver: providerThread.driver,
      providerInstanceId: providerThread.providerInstanceId,
      occurredAt: now,
      payload: {
        ...providerThread,
        nativeConversationHeadRef: {
          driver: providerThread.driver,
          strength: "strong",
          ...providerThread.nativeConversationHeadRef,
          nativeId: update.leafId,
        },
        firstRunOrdinal: providerThread.firstRunOrdinal ?? lastRunOrdinal,
        lastRunOrdinal,
        updatedAt: now,
      },
    });
    yield* eventSink.write({ events });
    yield* writeSpans;
    // PiAdapterV2 reloads before its next turn; t3-follow holds writes until
    // caught up. After a CLI answer, re-summarize the imported prompt title.
    if (spansWritten) {
      const createdOpenRun = events.some(
        (event) => event.type === "run.created" && event.payload.completedAt === null,
      );
      remember(quiet && !createdOpenRun);
    }
    yield* Effect.logInfo("Synced Pi CLI messages into T3 thread", {
      threadId,
      messages: update.messages.length,
      tools: activity.tools.length,
    });
    // Title dispatch takes the same non-reentrant thread lock. Return the
    // completed turn to the caller, which refreshes only after releasing it.
    return update.messages.length > 0 && piTurnEnded(contents) ? update.leafId : undefined;
  });

  const followThreads = Effect.fnUntraced(function* (changedFiles?: ReadonlySet<string>) {
    const rows = yield* sql<{ readonly thread_id: string; readonly provider_thread_id: string }>`
      SELECT provider.thread_id, provider.provider_thread_id
      FROM orchestration_v2_projection_provider_threads AS provider
      JOIN orchestration_v2_projection_threads AS thread
        ON thread.thread_id = provider.thread_id
        AND thread.active_provider_thread_id = provider.provider_thread_id
      WHERE provider.driver = 'pi' AND provider.status <> 'active'
        AND thread.archived_at IS NULL AND thread.deleted_at IS NULL
    `;
    const active = new Set(rows.map((row) => ThreadId.make(row.thread_id)));
    for (const [threadId, snapshot] of followed) {
      if (!active.has(threadId)) {
        sessionFiles.drop(snapshot.file);
        followed.delete(threadId);
      }
    }
    sessionFiles.retain(new Set(Array.from(followed.values(), (snapshot) => snapshot.file)));
    let selected = rows;
    if (changedFiles !== undefined && changedFiles.size > 0) {
      const changed = new Set(Array.from(changedFiles, (file) => path.resolve(file)));
      const remaining = new Set(changed);
      selected = rows.filter((row) => {
        const snapshot = followed.get(ThreadId.make(row.thread_id));
        // Newly eligible or switched followers need an initial reconciliation.
        if (snapshot?.providerThreadId !== row.provider_thread_id) return true;
        const file = path.resolve(snapshot.file);
        if (!changed.has(file)) return false;
        remaining.delete(file);
        return true;
      });
      // An unknown file may belong to a new or newly eligible follower.
      if (remaining.size > 0) selected = rows;
    }
    yield* Effect.forEach(
      selected,
      (row) =>
        followThread(ThreadId.make(row.thread_id), row.provider_thread_id).pipe(
          Effect.catchCause((cause) =>
            Effect.logWarning("Could not sync a Pi session", {
              threadId: row.thread_id,
              cause: Cause.pretty(cause),
            }),
          ),
        ),
      { discard: true },
    );
  });

  const importNewSessions = Effect.gen(function* () {
    const activeSinceMs = DateTime.toEpochMillis(yield* DateTime.now) - ACTIVE_WINDOW_MS;
    const { projects: all } = yield* projects.snapshot;
    yield* Effect.forEach(
      all,
      (project) =>
        importer
          .importRecentAgentThreads({ projectId: project.id }, { sources: ["pi"], activeSinceMs })
          .pipe(
            Effect.tap(({ importedCount }) =>
              importedCount > 0
                ? Effect.logDebug("Pi CLI sessions available in project", {
                    projectId: project.id,
                    importedCount,
                  })
                : Effect.void,
            ),
            Effect.catchCause((cause) =>
              Effect.logWarning("Could not import Pi CLI sessions", { cause }),
            ),
          ),
      { discard: true },
    );
  });

  const sync = (importSessions: boolean, changedFiles?: ReadonlySet<string>) =>
    Effect.all(
      importSessions ? [followThreads(), importNewSessions] : [followThreads(changedFiles)],
      { discard: true },
    ).pipe(Effect.catchCause((cause) => Effect.logWarning("Pi session sync failed", { cause })));

  return {
    sessionDir,
    sync,
    followThread,
  };
});

/** Keep every path in a burst, including events received while the consumer is syncing. */
export function debouncePiSessionChanges<E, R>(
  events: Stream.Stream<FileSystem.WatchEvent | void, E, R>,
): Stream.Stream<ReadonlySet<string> | undefined, E, R> {
  return Stream.suspend(() => {
    let paths = new Set<string>();
    let unknown = false;
    return events.pipe(
      Stream.tap((event) =>
        Effect.sync(() => {
          if (event === undefined || event.path.length === 0) unknown = true;
          else paths.add(event.path);
        }),
      ),
      Stream.debounce(DEBOUNCE),
      Stream.map(() => {
        const batch = unknown ? undefined : paths;
        paths = new Set();
        unknown = false;
        return batch;
      }),
    );
  });
}

/**
 * Runs a sync at startup and after every burst of changes in Pi's session directory.
 * Appends from running threads fire constantly, so the import scan (which reads every
 * recent transcript) runs only when a file was created, or as a catch-all every few minutes.
 */
export const layer = Layer.effectDiscard(
  Effect.gen(function* () {
    const fileSystem = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const { sessionDir, sync } = yield* makePiSessionSync;
    const settings = yield* ServerSettings.ServerSettingsService;
    // Subscribe before reading the snapshot so settings changes cannot be lost.
    const changes = yield* settings.subscribeChanges;
    let sawNewFile = true;
    let lastImportMs = 0;
    const syncAfterChanges = Effect.fnUntraced(function* (files?: ReadonlySet<string>) {
      const now = yield* Clock.currentTimeMillis;
      const importSessions = sawNewFile || now - lastImportMs >= FULL_SYNC_INTERVAL_MS;
      if (importSessions) {
        sawNewFile = false;
        lastImportMs = now;
      }
      yield* sync(importSessions, importSessions ? undefined : files);
    });
    yield* Stream.concat(Stream.fromEffect(settings.getSettings), changes).pipe(
      Stream.switchMap((current) =>
        Stream.unwrap(
          Effect.gen(function* () {
            const directories = yield* piSessionWatchDirectories(
              current.providerInstances,
              sessionDir,
            );
            return Stream.mergeAll(
              Array.from(directories, (directory) =>
                Stream.unwrap(
                  fileSystem.makeDirectory(directory, { recursive: true }).pipe(
                    Effect.ignore,
                    Effect.as(
                      fileSystem.watch(directory, { recursive: true }).pipe(
                        Stream.tap((event) =>
                          Effect.sync(() => {
                            if (event._tag === "Create") sawNewFile = true;
                          }),
                        ),
                        Stream.map((event) => ({
                          ...event,
                          // Node watchers report paths relative to the watched directory.
                          path: event.path.length === 0 ? "" : path.resolve(directory, event.path),
                        })),
                        Stream.catchCause((cause) =>
                          Stream.fromEffect(
                            Effect.logWarning("Pi session directory watcher stopped", {
                              directory,
                              cause,
                            }),
                          ),
                        ),
                      ),
                    ),
                  ),
                ),
              ),
              { concurrency: "unbounded" },
            ).pipe(debouncePiSessionChanges, (events) =>
              Stream.concat(Stream.make(undefined), events),
            );
          }),
        ),
      ),
      Stream.runForEach(syncAfterChanges),
      Effect.catchCause((cause) => Effect.logWarning("Pi session watcher stopped", { cause })),
      Effect.forkScoped,
    );
  }),
).pipe(Layer.provide(ThreadCommandExecutor.layer));
