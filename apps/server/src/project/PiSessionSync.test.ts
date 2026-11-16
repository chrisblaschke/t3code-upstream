import { expect, it } from "@effect/vitest";
import * as ChildProcessSpawner from "effect/process/ChildProcessSpawner";
import { NodeServices } from "@effect/platform-node";
import {
  ProviderDriverKind,
  ProviderInstanceId,
  ProviderThreadId,
  RunId,
  SecretProviderId,
  ThreadId,
  type OrchestrationV2DomainEvent,
  type OrchestrationV2ProviderThread,
  type OrchestrationV2Run,
} from "@t3tools/contracts";
import * as HostProcess from "@t3tools/shared/HostProcess";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import type * as PlatformError from "effect/PlatformError";
import * as TestClock from "effect/testing/TestClock";
import * as Scope from "effect/Scope";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as Queue from "effect/Queue";
import * as Stream from "effect/Stream";
import * as SqlClient from "effect/sql/SqlClient";

import { ServerConfig } from "../config.ts";
import * as EventSink from "../orchestration-v2/EventSink.ts";
import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import * as Orchestrator from "../orchestration-v2/Orchestrator.ts";
import { AgentSessionImporter } from "./AgentSessionImporter.ts";
import * as ProjectService from "./ProjectService.ts";
import * as KeyedLock from "@t3tools/shared/KeyedLock";
import * as ThreadCommandExecutor from "../orchestration-v2/ThreadCommandExecutor.ts";
import {
  debouncePiSessionChanges,
  makePiSessionFileReader,
  makePiSessionSync,
  piSessionWatchDirectories,
} from "./PiSessionSync.ts";
import { parsePiSessionEntries } from "./piSessionItems.ts";
import * as PiSessionSync from "./PiSessionSync.ts";
import * as ServerSettings from "../serverSettings.ts";

it.effect("watch directories share discovery's path rules and tolerate secret references", () =>
  Effect.gen(function* () {
    const driver = ProviderDriverKind.make("pi");
    const directories = yield* piSessionWatchDirectories(
      {
        [ProviderInstanceId.make("explicit")]: {
          driver,
          enabled: true,
          environment: [{ name: "PI_CODING_AGENT_SESSION_DIR", value: "~/cli", sensitive: false }],
        },
        [ProviderInstanceId.make("agent")]: {
          driver,
          enabled: true,
          environment: [{ name: "PI_CODING_AGENT_DIR", value: "~/agent", sensitive: false }],
        },
        [ProviderInstanceId.make("secret")]: {
          driver,
          enabled: true,
          environment: [
            {
              name: "PI_CODING_AGENT_SESSION_DIR",
              value: { provider: SecretProviderId.make("keychain"), reference: "session-dir" },
              sensitive: true,
            },
            {
              name: "PI_CODING_AGENT_DIR",
              value: { provider: SecretProviderId.make("keychain"), reference: "agent-dir" },
              sensitive: true,
            },
          ],
        },
        [ProviderInstanceId.make("disabled")]: {
          driver,
          enabled: false,
          environment: [{ name: "PI_CODING_AGENT_DIR", value: "/disabled", sensitive: false }],
        },
        [ProviderInstanceId.make("claude")]: {
          driver: ProviderDriverKind.make("claudeAgent"),
          environment: [{ name: "PI_CODING_AGENT_DIR", value: "/other", sensitive: false }],
        },
      },
      "/default/sessions",
    );
    expect([...directories]).toEqual([
      "/default/sessions",
      "/home/test/cli",
      "/home/test/agent/sessions",
      "/inherited/sessions",
    ]);
  }).pipe(
    Effect.provideService(HostProcess.Environment, { PI_CODING_AGENT_DIR: "/inherited" }),
    Effect.provideService(HostProcess.HomeDirectory, "/home/test"),
    Effect.provide(NodeServices.layer),
  ),
);

const at = (seconds: number) => `2026-10-07T10:00:${String(seconds).padStart(2, "0")}.000Z`;
const message = (id: string, parentId: string | null, seconds: number, body: object) => ({
  type: "message",
  id,
  parentId,
  timestamp: at(seconds),
  message: body,
});
const midTurn = [
  { type: "session", version: 3, id: "session", cwd: "/project" },
  message("u1", null, 0, { role: "user", content: "Work on it" }),
  message("a1", "u1", 1, {
    role: "assistant",
    stopReason: "toolUse",
    content: [{ type: "toolCall", id: "call-1", name: "bash", arguments: { command: "pwd" } }],
  }),
  message("r1", "a1", 2, { role: "toolResult", toolCallId: "call-1", content: "done" }),
];
const answer = message("a2", "r1", 8, {
  role: "assistant",
  stopReason: "stop",
  content: [{ type: "text", text: "Finished" }],
});

const makeHarness = (
  sql?: SqlClient.SqlClient,
  additionalFollowers: ReadonlyMap<ThreadId, string> = new Map(),
) =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const commands = yield* KeyedLock.make<ThreadId>();
    const dir = yield* fs.makeTempDirectoryScoped({ prefix: "pi-sync-status-" });
    const sessionFile = `${dir}/session.jsonl`;
    const threadId = ThreadId.make("cli-thread");
    const driver = ProviderDriverKind.make("pi");
    const providerInstanceId = ProviderInstanceId.make("pi");
    let providerThread = {
      id: ProviderThreadId.make("provider-thread"),
      appThreadId: threadId,
      driver,
      providerInstanceId,
      status: "idle",
      nativeThreadRef: { driver, nativeId: sessionFile, strength: "strong" },
      nativeConversationHeadRef: null,
      firstRunOrdinal: null,
      lastRunOrdinal: null,
      updatedAt: DateTime.makeUnsafe(at(0)),
    } as OrchestrationV2ProviderThread;
    const thread = {
      id: threadId,
      activeProviderThreadId: providerThread.id,
      providerInstanceId,
      archivedAt: null,
      deletedAt: null,
      settledOverride: null,
      updatedAt: DateTime.makeUnsafe(at(0)),
      modelSelection: { instanceId: providerInstanceId, model: "default" },
      lineage: { parentThreadId: null },
    };
    const runs: Array<OrchestrationV2Run> = [];
    const events: Array<OrchestrationV2DomainEvent> = [];
    const stats = { reads: 0, writes: 0, sql: 0, failWrites: 0, titleRefreshes: 0 };
    const followedFiles: Array<string> = [];
    let fileTime = DateTime.toEpochMillis(DateTime.makeUnsafe(at(10)));
    const writeSession = (entries: ReadonlyArray<object>) =>
      Effect.gen(function* () {
        yield* fs.writeFileString(
          sessionFile,
          entries.map((entry) => JSON.stringify(entry)).join("\n"),
        );
        fileTime += 1000;
        yield* fs.utimes(sessionFile, fileTime / 1000, fileTime / 1000);
        yield* TestClock.setTime(fileTime + 100);
      });
    yield* writeSession(midTurn);
    const sync = yield* makePiSessionSync.pipe(
      Effect.provide(
        Layer.mergeAll(
          Layer.succeed(HostProcess.Environment, { PI_CODING_AGENT_DIR: dir }),
          ServerConfig.layerTest(dir, dir),
          Layer.succeed(
            SqlClient.SqlClient,
            sql ??
              ((() =>
                Effect.sync(() => {
                  stats.sql++;
                  return [];
                })) as unknown as SqlClient.SqlClient),
          ),
          Layer.mock(ProjectService.ProjectService)({
            snapshot: Effect.succeed({ projects: [] } as never),
          }),
          Layer.mock(AgentSessionImporter)({}),
          IdAllocator.layer,
          Layer.succeed(ThreadCommandExecutor.ThreadCommandExecutor, commands),
          Layer.mock(Orchestrator.OrchestratorV2)({
            dispatch: (command) =>
              Effect.gen(function* () {
                // Fail before attempting a recursive acquire so a regression
                // reports an assertion rather than hanging the test runner.
                expect(yield* commands.activeKeys).not.toContain(threadId);
                return yield* commands.withLock(
                  threadId,
                  Effect.sync(() => {
                    if (command.type === "thread.metadata.update" && command.regenerateTitle) {
                      stats.titleRefreshes++;
                    }
                    return {} as never;
                  }),
                );
              }),
            getThreadRecords: (id) =>
              Effect.sync(() => {
                stats.reads++;
                const file = additionalFollowers.get(id);
                if (file !== undefined) {
                  return {
                    thread: { ...thread, id },
                    providerThreads: [
                      {
                        ...providerThread,
                        appThreadId: id,
                        nativeThreadRef: { driver, nativeId: file, strength: "strong" },
                      },
                    ],
                    runs: [],
                  } as never;
                }
                return { thread, providerThreads: [providerThread], runs } as never;
              }),
          }),
          Layer.mock(EventSink.EventSinkV2)({
            write: (input) =>
              Effect.sync(() => {
                stats.writes++;
                if (stats.failWrites > 0) {
                  stats.failWrites--;
                  throw new Error("Injected event write failure");
                }
                const knownIds = new Set(events.map((event) => event.id));
                const fresh = input.events.filter((event) => !knownIds.has(event.id));
                events.push(...fresh);
                for (const event of fresh) {
                  if (event.type === "run.created" || event.type === "run.updated") {
                    const index = runs.findIndex((run) => run.id === event.payload.id);
                    if (index === -1) runs.push(event.payload);
                    else runs[index] = event.payload;
                  } else if (event.type === "provider-thread.updated")
                    providerThread = event.payload;
                }
                return [];
              }),
          }),
        ),
      ),
      Effect.provideService(FileSystem.FileSystem, {
        ...fs,
        stat: (file) => {
          followedFiles.push(file);
          return fs.stat(file);
        },
      }),
    );
    return {
      commands,
      threadId,
      activate: () => {
        providerThread = { ...providerThread, status: "active" };
      },
      runs,
      events,
      stats,
      followedFiles,
      sessionFile,
      writeSession,
      appendSession: (entry: object, keepMtime = false) =>
        Effect.gen(function* () {
          yield* fs.writeFileString(sessionFile, `\n${JSON.stringify(entry)}`, { flag: "a" });
          if (!keepMtime) fileTime += 1000;
          yield* fs.utimes(sessionFile, fileTime / 1000, fileTime / 1000);
          yield* TestClock.setTime(fileTime + 100);
        }),
      removeSession: fs.remove(sessionFile),
      follow: sync.followThread(threadId, providerThread.id),
      sync: sync.sync(false),
      syncChanges: (files: ReadonlySet<string>) => sync.sync(false, files),
      syncImport: (files: ReadonlySet<string>) => sync.sync(true, files),
    };
  });

type Harness = Effect.Success<ReturnType<typeof makeHarness>>;
type TestError = Effect.Error<Harness["follow"]> | PlatformError.PlatformError;
const test = (
  name: string,
  body: (
    h: Harness,
  ) => Effect.Effect<
    void,
    TestError,
    FileSystem.FileSystem | Scope.Scope | Path.Path | ChildProcessSpawner.ChildProcessSpawner
  >,
) =>
  it.effect(name, () =>
    Effect.gen(function* () {
      yield* body(yield* makeHarness());
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );

it.effect("sync only loads selected Pi sessions on visible threads", () =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* sql`CREATE TABLE orchestration_v2_projection_provider_threads (
      thread_id TEXT, provider_thread_id TEXT, driver TEXT, status TEXT
    )`;
    yield* sql`CREATE TABLE orchestration_v2_projection_threads (
      thread_id TEXT, active_provider_thread_id TEXT, archived_at TEXT, deleted_at TEXT
    )`;
    yield* sql`INSERT INTO orchestration_v2_projection_threads VALUES
      ('cli-thread', 'provider-thread', NULL, NULL),
      ('archived', 'archived-session', '2026-01-01', NULL),
      ('deleted', 'deleted-session', NULL, '2026-01-01')`;
    yield* sql`INSERT INTO orchestration_v2_projection_provider_threads VALUES
      ('cli-thread', 'provider-thread', 'pi', 'idle'),
      ('cli-thread', 'superseded-session', 'pi', 'idle'),
      ('archived', 'archived-session', 'pi', 'idle'),
      ('deleted', 'deleted-session', 'pi', 'idle'),
      ('missing-thread', 'orphaned-session', 'pi', 'idle'),
      ('cli-thread', 'running-session', 'pi', 'active'),
      ('cli-thread', 'other-provider', 'codex', 'idle')`;
    const h = yield* makeHarness(sql);
    // The runtime can activate after enumeration; the full pass still guards that race.
    h.activate();
    yield* h.sync;
    expect(h.stats.reads).toBe(1);
    expect(h.stats.writes).toBe(0);
  }).pipe(
    Effect.scoped,
    Effect.provide(
      Layer.mergeAll(NodeSqliteClient.layer({ filename: ":memory:" }), NodeServices.layer),
    ),
  ),
);

it.effect("syncs only changed followers and falls back for unmapped paths or imports", () =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    const fs = yield* FileSystem.FileSystem;
    const dir = yield* fs.makeTempDirectoryScoped({ prefix: "pi-follow-targets-" });
    const otherFile = `${dir}/other.jsonl`;
    yield* fs.writeFileString(otherFile, `${JSON.stringify(midTurn[0])}\n`);
    yield* sql`CREATE TABLE orchestration_v2_projection_provider_threads (
      thread_id TEXT, provider_thread_id TEXT, driver TEXT, status TEXT
    )`;
    yield* sql`CREATE TABLE orchestration_v2_projection_threads (
      thread_id TEXT, active_provider_thread_id TEXT, archived_at TEXT, deleted_at TEXT
    )`;
    yield* sql`INSERT INTO orchestration_v2_projection_threads VALUES
      ('cli-thread', 'provider-thread', NULL, NULL),
      ('other-thread', 'provider-thread', NULL, NULL)`;
    yield* sql`INSERT INTO orchestration_v2_projection_provider_threads VALUES
      ('cli-thread', 'provider-thread', 'pi', 'idle'),
      ('other-thread', 'provider-thread', 'pi', 'idle')`;
    const h = yield* makeHarness(sql, new Map([[ThreadId.make("other-thread"), otherFile]]));
    yield* h.writeSession([...midTurn, answer]);
    yield* h.sync;
    expect(new Set(h.followedFiles)).toEqual(new Set([h.sessionFile, otherFile]));
    h.followedFiles.length = 0;
    yield* h.appendSession(message("u2", "a2", 9, { role: "user", content: "Next task" }));
    yield* h.syncChanges(new Set([h.sessionFile.replace("/session.jsonl", "/./session.jsonl")]));
    expect(new Set(h.followedFiles)).toEqual(new Set([h.sessionFile]));
    expect(
      h.events.some(
        (event) => event.type === "message.updated" && event.payload.text === "Next task",
      ),
    ).toBe(true);
    for (const files of [
      new Set([h.sessionFile, otherFile]),
      new Set([`${dir}/unknown.jsonl`]),
      new Set<string>(),
    ]) {
      h.followedFiles.length = 0;
      yield* h.syncChanges(files);
      expect(new Set(h.followedFiles)).toEqual(new Set([h.sessionFile, otherFile]));
    }
    h.followedFiles.length = 0;
    yield* h.syncImport(new Set([h.sessionFile]));
    expect(new Set(h.followedFiles)).toEqual(new Set([h.sessionFile, otherFile]));
    h.followedFiles.length = 0;
    yield* TestClock.adjust("5 minutes");
    yield* h.sync;
    expect(new Set(h.followedFiles)).toEqual(new Set([h.sessionFile, otherFile]));
  }).pipe(
    Effect.scoped,
    Effect.provide(
      Layer.mergeAll(NodeSqliteClient.layer({ filename: ":memory:" }), NodeServices.layer),
    ),
  ),
);

it.effect(
  "targeted sync reconciles a newly unarchived follower even when its path is unchanged",
  () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      const fs = yield* FileSystem.FileSystem;
      const dir = yield* fs.makeTempDirectoryScoped({ prefix: "pi-follow-unarchive-" });
      const otherFile = `${dir}/other.jsonl`;
      yield* fs.writeFileString(otherFile, `${JSON.stringify(midTurn[0])}\n`);
      yield* sql`CREATE TABLE orchestration_v2_projection_provider_threads (
      thread_id TEXT, provider_thread_id TEXT, driver TEXT, status TEXT
    )`;
      yield* sql`CREATE TABLE orchestration_v2_projection_threads (
      thread_id TEXT, active_provider_thread_id TEXT, archived_at TEXT, deleted_at TEXT
    )`;
      yield* sql`INSERT INTO orchestration_v2_projection_threads VALUES
      ('cli-thread', 'provider-thread', NULL, NULL),
      ('other-thread', 'provider-thread', NULL, NULL)`;
      yield* sql`INSERT INTO orchestration_v2_projection_provider_threads VALUES
      ('cli-thread', 'provider-thread', 'pi', 'idle'),
      ('other-thread', 'provider-thread', 'pi', 'idle')`;
      const h = yield* makeHarness(sql, new Map([[ThreadId.make("other-thread"), otherFile]]));
      yield* h.writeSession([midTurn[0]!]);
      yield* h.sync;
      expect(new Set(h.followedFiles)).toEqual(new Set([h.sessionFile, otherFile]));

      yield* sql`UPDATE orchestration_v2_projection_threads
      SET archived_at = '2026-01-01' WHERE thread_id = 'other-thread'`;
      yield* h.syncChanges(new Set([h.sessionFile]));
      yield* fs.writeFileString(
        otherFile,
        [
          midTurn[0],
          message("archived-u1", null, 9, { role: "user", content: "Written while archived" }),
        ]
          .map((entry) => JSON.stringify(entry))
          .join("\n"),
      );
      yield* sql`UPDATE orchestration_v2_projection_threads
      SET archived_at = NULL WHERE thread_id = 'other-thread'`;
      h.followedFiles.length = 0;
      yield* h.syncChanges(new Set([h.sessionFile]));
      expect(
        h.events.some(
          (event) =>
            event.type === "message.updated" && event.payload.text === "Written while archived",
        ),
      ).toBe(true);
      expect(new Set(h.followedFiles)).toEqual(new Set([h.sessionFile, otherFile]));
    }).pipe(
      Effect.scoped,
      Effect.provide(
        Layer.mergeAll(NodeSqliteClient.layer({ filename: ":memory:" }), NodeServices.layer),
      ),
    ),
);

it.effect("reconciles periodically without filesystem events", () =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const dir = yield* fs.makeTempDirectoryScoped({ prefix: "pi-periodic-" });
    const started = yield* Deferred.make<void>();
    let queries = 0;
    yield* Layer.build(PiSessionSync.layer).pipe(
      Effect.provide(
        Layer.mergeAll(
          ServerConfig.layerTest(dir, dir),
          IdAllocator.layer,
          Layer.succeed(HostProcess.Environment, { PI_CODING_AGENT_DIR: dir }),
          Layer.succeed(SqlClient.SqlClient, (() =>
            Effect.sync(() => {
              queries++;
              return [];
            })) as unknown as SqlClient.SqlClient),
          Layer.mock(Orchestrator.OrchestratorV2)({}),
          Layer.mock(EventSink.EventSinkV2)({}),
          Layer.mock(AgentSessionImporter)({}),
          Layer.mock(ProjectService.ProjectService)({
            snapshot: Deferred.succeed(started, undefined).pipe(
              Effect.as({ projects: [] } as never),
            ),
          }),
          Layer.mock(ServerSettings.ServerSettingsService)({
            getSettings: Effect.succeed({ providerInstances: {} } as never),
            subscribeChanges: Effect.succeed(Stream.never),
          }),
        ),
      ),
      Effect.provideService(FileSystem.FileSystem, { ...fs, watch: () => Stream.never }),
    );
    yield* Deferred.await(started);
    expect(queries).toBe(1);
    yield* TestClock.adjust("5 minutes");
    expect(queries).toBe(2);
    yield* TestClock.adjust("5 minutes");
    expect(queries).toBe(3);
  }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
);

it.effect("keeps all debounced paths, including events arriving during a sync", () =>
  Effect.gen(function* () {
    const events = yield* Queue.unbounded<FileSystem.WatchEvent>();
    const observed = yield* Queue.unbounded<string>();
    const batches = yield* Queue.unbounded<ReadonlySet<string> | undefined>();
    const release = yield* Deferred.make<void>();
    let first = true;
    yield* Stream.fromQueue(events).pipe(
      Stream.tap((event) => Queue.offer(observed, event.path)),
      debouncePiSessionChanges,
      Stream.runForEach((files) =>
        Effect.gen(function* () {
          yield* Queue.offer(batches, files);
          if (first) {
            first = false;
            yield* Deferred.await(release);
          }
        }),
      ),
      Effect.forkScoped,
    );
    const offer = (path: string) =>
      Effect.gen(function* () {
        yield* Queue.offer(events, { _tag: "Update", path });
        yield* Queue.take(observed);
      });
    yield* offer("/a.jsonl");
    yield* offer("/b.jsonl");
    yield* TestClock.adjust("1500 millis");
    expect(yield* Queue.take(batches)).toEqual(new Set(["/a.jsonl", "/b.jsonl"]));
    // The watcher queues events while downstream is busy with the first sync.
    yield* Queue.offer(events, { _tag: "Update", path: "/c.jsonl" });
    yield* Queue.offer(events, { _tag: "Update", path: "/d.jsonl" });
    yield* Deferred.succeed(release, undefined);
    yield* Queue.take(observed);
    yield* Queue.take(observed);
    yield* TestClock.adjust("1500 millis");
    expect(yield* Queue.take(batches)).toEqual(new Set(["/c.jsonl", "/d.jsonl"]));
    yield* offer("/a.jsonl");
    yield* offer("");
    yield* TestClock.adjust("1500 millis");
    expect(yield* Queue.take(batches)).toBeUndefined();
  }).pipe(Effect.scoped),
);

test("interrupts an abandoned tool call when the next CLI prompt arrives", (h) =>
  Effect.gen(function* () {
    yield* h.writeSession(midTurn.slice(0, -1));
    yield* h.follow;
    const statuses = () =>
      h.events.flatMap((event) =>
        event.type === "turn-item.updated" && event.payload.type === "command_execution"
          ? [event.payload.status]
          : [],
      );
    expect(statuses()).toContain("running");
    yield* h.appendSession(message("u2", "a1", 10, { role: "user", content: "try again" }));
    yield* h.follow;
    expect(statuses().at(-1)).toBe("interrupted");
  }));

test("completes title refresh outside the thread lock and leaves the thread mutable", (h) =>
  Effect.gen(function* () {
    yield* h.writeSession([...midTurn, answer]);
    // The real title follower synchronously dispatches through the same keyed
    // executor. Its pre-acquire assertion catches a held lock without hanging.
    yield* h.follow;
    expect(h.stats.titleRefreshes).toBe(1);
    let mutated = false;
    yield* h.commands.withLock(
      h.threadId,
      Effect.sync(() => {
        mutated = true;
      }),
    );
    expect(mutated).toBe(true);
    yield* h.appendSession(message("u2", "a2", 9, { role: "user", content: "Next" }));
    yield* h.follow;
    expect(
      h.events.some((event) => event.type === "message.updated" && event.payload.text === "Next"),
    ).toBe(true);
  }));

test("serializes file following with ordinary thread mutations", (h) =>
  Effect.gen(function* () {
    const follow = yield* h.commands.withLock(
      h.threadId,
      Effect.gen(function* () {
        const fiber = yield* h.follow.pipe(Effect.forkScoped);
        yield* Effect.yieldNow;
        expect(h.stats.reads).toBe(0);
        h.activate();
        return fiber;
      }),
    );
    yield* Fiber.join(follow);
    expect(h.stats.writes).toBe(0);
  }));

test("projects appended turns and a split UTF-8 tail exactly like a full read", (h) =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const final = message("a2", "r1", 8, {
      role: "assistant",
      stopReason: "stop",
      content: [{ type: "text", text: "Finished 🧪" }],
    });
    yield* h.writeSession(midTurn.slice(0, 2));
    yield* h.follow;
    for (const entry of midTurn.slice(2)) {
      yield* h.appendSession(entry);
      yield* h.follow;
    }
    const tail = Buffer.from(`\n${JSON.stringify(final)}`);
    const split = tail.indexOf(Buffer.from("🧪")) + 2;
    yield* fs.writeFile(h.sessionFile, tail.subarray(0, split), { flag: "a" });
    yield* h.follow;
    expect(
      h.events.some(
        (event) => event.type === "message.updated" && event.payload.text.includes("Finished"),
      ),
    ).toBe(false);
    yield* fs.writeFile(h.sessionFile, tail.subarray(split), { flag: "a" });
    yield* h.follow;
    // Repeated passes must not republish the provisional, newline-less record.
    const writes = h.stats.writes;
    yield* h.follow;
    expect(h.stats.writes).toBe(writes);

    const full = yield* makeHarness();
    yield* full.writeSession([...midTurn, final]);
    yield* full.follow;
    const items = (events: ReadonlyArray<OrchestrationV2DomainEvent>) => {
      const latest = new Map<string, unknown>();
      for (const event of events) {
        if (event.type === "message.updated" || event.type === "turn-item.updated") {
          latest.set(event.payload.id, event.payload);
        }
      }
      return [...latest].sort(([a], [b]) => a.localeCompare(b));
    };
    expect(items(h.events)).toEqual(items(full.events));
    expect(h.runs).toEqual(full.runs);
  }));

it.effect("follows records beyond the initial import line limit", () =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const dir = yield* fs.makeTempDirectoryScoped({ prefix: "pi-long-follow-" });
    const file = `${dir}/session.jsonl`;
    const reader = makePiSessionFileReader(fs);
    yield* fs.writeFileString(file, "\n".repeat(100_001) + `${JSON.stringify(midTurn[1])}\n`);
    expect((yield* reader.read(file))?.transcript).toHaveLength(1);
    yield* fs.writeFileString(file, `${JSON.stringify(answer)}\n`, { flag: "a" });
    expect((yield* reader.read(file))?.transcript).toHaveLength(2);
  }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
);

it.effect(
  "reads over-budget appends incrementally, and rebuilds after truncation or replacement",
  () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const dir = yield* fs.makeTempDirectoryScoped({ prefix: "pi-append-reader-" });
      const file = `${dir}/session.jsonl`;
      let bytesRead = 0;
      const reader = makePiSessionFileReader({
        ...fs,
        open: (...args: Parameters<typeof fs.open>) =>
          fs.open(...args).pipe(
            Effect.map(
              (handle) =>
                new Proxy(handle, {
                  get(target, key) {
                    if (key === "readAlloc")
                      return (size: number) =>
                        target.readAlloc(size).pipe(
                          Effect.tap((chunk) =>
                            Effect.sync(() => {
                              if (Option.isSome(chunk)) bytesRead += chunk.value.length;
                            }),
                          ),
                        );
                    const value = Reflect.get(target, key, target);
                    return typeof value === "function" ? value.bind(target) : value;
                  },
                }),
            ),
          ),
      });
      // Exceed the production 64 MiB budget, not just a test-only threshold.
      let contents = `${JSON.stringify({ ...midTurn[0], padding: "x".repeat(65 * 1024 * 1024) })}\n${JSON.stringify(midTurn[1])}\n`;
      yield* fs.writeFileString(file, contents);
      expect((yield* reader.read(file))?.entries).toEqual(parsePiSessionEntries(contents));
      const initialBytes = bytesRead;
      const added = `${JSON.stringify(midTurn[2])}\n`;
      contents += added;
      yield* fs.writeFileString(file, added, { flag: "a" });
      expect((yield* reader.read(file))?.entries).toEqual(parsePiSessionEntries(contents));
      expect(bytesRead - initialBytes).toBeLessThanOrEqual(Buffer.byteLength(added) + 1024);

      // A same-inode truncate invalidates the accumulated entries.
      contents = `${JSON.stringify(midTurn[1])}\n`;
      yield* fs.writeFileString(file, contents);
      expect((yield* reader.read(file))?.entries).toEqual(parsePiSessionEntries(contents));
      // Replacement must be detected even with identical size and mtime.
      const info = yield* fs.stat(file);
      contents = contents.replace("Work on it", "Other task");
      yield* fs.writeFileString(`${dir}/replacement`, contents);
      const mtime = Option.getOrThrow(info.mtime);
      yield* fs.utimes(`${dir}/replacement`, mtime, mtime);
      yield* fs.rename(`${dir}/replacement`, file);
      expect((yield* reader.read(file))?.entries).toEqual(parsePiSessionEntries(contents));
      // A truncate-and-regrow on that same inode must also rebuild.
      contents = `${JSON.stringify({ ...midTurn[0], padding: "y".repeat(8192) })}\n`;
      yield* fs.writeFileString(file, contents);
      expect((yield* reader.read(file))?.entries).toEqual(parsePiSessionEntries(contents));
      yield* fs.remove(file);
      expect(Option.isNone(yield* Effect.option(reader.read(file)))).toBe(true);
      contents = `${JSON.stringify(answer)}\n`;
      yield* fs.writeFileString(file, contents);
      expect((yield* reader.read(file))?.entries).toEqual(parsePiSessionEntries(contents));
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
);

// Rereading every session file (gigabytes across all threads) every five
// minutes would cost more than the passes the cache saves.
test("reconciles a quiet follow after five minutes without rereading its unchanged file", (h) =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    yield* h.writeSession([...midTurn, answer]);
    yield* h.follow;
    const before = h.stats.reads;
    yield* fs.chmod(h.sessionFile, 0o000); // stat still works; any read fails the follow
    yield* TestClock.adjust("5 minutes");
    yield* h.follow.pipe(Effect.ensuring(fs.chmod(h.sessionFile, 0o644).pipe(Effect.ignore)));
    expect(h.stats.reads).toBeGreaterThan(before);
  }));

test("skips quiet follows, rechecks changed files and reconciles after five minutes", (h) =>
  Effect.gen(function* () {
    yield* h.writeSession([...midTurn, answer]);
    yield* h.follow;
    let before = { ...h.stats };
    yield* h.follow;
    expect(h.stats).toEqual(before);
    yield* h.appendSession(
      message("a3", "a2", 9, {
        role: "assistant",
        stopReason: "stop",
        content: [{ type: "text", text: "One more reply" }],
      }),
      true,
    );
    yield* h.follow;
    expect(h.stats.reads).toBeGreaterThan(before.reads);
    expect(
      h.events.some(
        (event) => event.type === "message.updated" && event.payload.text === "One more reply",
      ),
    ).toBe(true);
    before = { ...h.stats };
    yield* TestClock.adjust("5 minutes");
    yield* h.follow;
    expect(h.stats.reads).toBeGreaterThan(before.reads);
    before = { ...h.stats };
    yield* h.follow;
    expect(h.stats).toEqual(before);
  }));

test("retries a failed quiet run-span reconciliation rather than caching it", (h) =>
  Effect.gen(function* () {
    yield* h.writeSession([...midTurn, answer]);
    yield* h.follow;
    // Simulate an external projection change without a file write.
    h.runs[0] = { ...h.runs[0]!, status: "running", completedAt: null };
    yield* TestClock.adjust("5 minutes");
    h.stats.failWrites = 1;
    yield* h.follow;
    expect(h.runs[0]?.completedAt).toBeNull();
    const before = h.stats.reads;
    yield* h.follow;
    expect(h.stats.reads).toBeGreaterThan(before);
    expect(h.runs[0]?.status).toBe("completed");
  }));

test("retries a failed same-mtime append without forgetting the old signature", (h) =>
  Effect.gen(function* () {
    yield* h.writeSession([...midTurn, answer]);
    yield* h.follow;
    yield* h.appendSession(
      message("a3", "a2", 9, {
        role: "assistant",
        stopReason: "stop",
        content: [{ type: "text", text: "Retry this reply" }],
      }),
      true,
    );
    h.stats.failWrites = 1;
    const failed = yield* Effect.exit(h.follow);
    expect(failed._tag).toBe("Failure");
    yield* h.follow;
    expect(
      h.events.some(
        (event) => event.type === "message.updated" && event.payload.text === "Retry this reply",
      ),
    ).toBe(true);
    const before = { ...h.stats };
    yield* h.follow;
    expect(h.stats).toEqual(before);
  }));

test("rechecks a quiet follow whose session file disappeared", (h) =>
  Effect.gen(function* () {
    yield* h.writeSession([...midTurn, answer]);
    yield* h.follow;
    const before = h.stats.reads;
    yield* h.removeSession;
    yield* h.follow;
    expect(h.stats.reads).toBeGreaterThan(before);
  }));

test("imports every CLI turn as completed history", (h) =>
  Effect.gen(function* () {
    yield* h.writeSession([
      ...midTurn,
      answer,
      message("u2", "a2", 9, { role: "user", content: "Next task" }),
    ]);
    yield* h.follow;
    expect(h.runs.map((run) => run.status)).toEqual(["completed", "completed"]);
    expect(DateTime.formatIso(h.runs[0]!.completedAt!)).toBe(at(8));
  }));

test("still blocks following while a T3-owned run is open", (h) =>
  Effect.gen(function* () {
    yield* h.follow;
    h.runs.push({
      ...h.runs[0]!,
      id: RunId.make("t3-owned-run"),
      ordinal: 2,
      status: "running",
      completedAt: null,
    });
    const before = h.events.length;
    yield* h.writeSession([...midTurn, answer]);
    yield* h.follow;
    expect(h.events).toHaveLength(before);
  }));

test("closes an open run whose session file was removed", (h) =>
  Effect.gen(function* () {
    yield* h.follow;
    // A run left open by an earlier pass (or an older build).
    h.runs[0] = { ...h.runs[0]!, status: "running", completedAt: null };
    yield* h.removeSession;
    yield* h.follow;
    expect(h.runs[0]?.status).toBe("completed");
    expect(h.runs[0]?.completedAt).not.toBeNull();
  }));

test("closes an open run after its turn left the active branch", (h) =>
  Effect.gen(function* () {
    yield* h.follow;
    h.runs[0] = { ...h.runs[0]!, status: "running", completedAt: null };
    yield* h.writeSession([
      midTurn[0]!,
      message("other", null, 9, { role: "user", content: "Another branch" }),
    ]);
    yield* h.follow;
    expect(h.runs.every((run) => run.status === "completed" && run.completedAt !== null)).toBe(
      true,
    );
  }));

it.effect("keeps a mid-turn CLI run historical", () =>
  Effect.gen(function* () {
    const h = yield* makeHarness();
    yield* h.follow;
    expect(h.runs[0]?.status).toBe("completed");
    expect(DateTime.formatIso(h.runs[0]!.completedAt!)).toBe(at(2));
  }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
);
