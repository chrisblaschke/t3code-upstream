import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it } from "@effect/vitest";
import { ProviderInstanceId, ThreadId, TerminalOpenPiInput } from "@t3tools/contracts";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as Path from "effect/Path";
import * as HostProcess from "@t3tools/shared/HostProcess";
import { resolvePiTerminalLaunch } from "@t3tools/provider-pi/server";
import * as ThreadManagement from "../orchestration-v2/ThreadManagementService.ts";
import * as ProjectStore from "../orchestration-v2/ProjectStore.ts";
import * as ProviderInstances from "../provider/ProviderInstanceRegistry.ts";
import * as ProcessRunner from "../processRunner.ts";
import * as TerminalManager from "./Manager.ts";
import * as NodePtyAdapter from "./NodePtyAdapter.ts";
import * as PtyAdapter from "@t3tools/shared/PtyAdapter";
import { openPiTerminal } from "./PiTui.ts";

const decodeInput = Schema.decodeUnknownEffect(TerminalOpenPiInput);
const threadId = ThreadId.make("thread-pi");
const providerInstanceId = ProviderInstanceId.make("pi-work");
const layer = Layer.mergeAll(ProcessRunner.layer, NodePtyAdapter.layer).pipe(
  Layer.provideMerge(NodeServices.layer),
);

it.layer(layer, { excludeTestServices: true })("Pi terminal", (it) => {
  it.effect.each([false, true])(
    "launches a real PTY with the server configuration, custom=%s",
    (custom) =>
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const path = yield* Path.Path;
        const dir = yield* fs.makeTempDirectoryScoped({ prefix: "t3-pi-tui-" });
        const binary = path.join(dir, "pi");
        const capture = path.join(dir, "captured.json");
        // This executable stands in for Pi, not for the PTY or launch resolver.
        yield* fs.writeFileString(
          binary,
          `#!${process.execPath}
const fs = require("node:fs");
fs.writeFileSync(${JSON.stringify(capture)}, JSON.stringify({ args: process.argv.slice(2), profile: process.env.PI_CODING_AGENT_DIR, credential: process.env.API_KEY }));
process.stdout.write("READY\\n");
process.stdin.resume();
`,
        );
        yield* fs.chmod(binary, 0o755);
        const launch = yield* resolvePiTerminalLaunch({
          config: {
            binaryPath: custom ? binary : "pi",
            launchArgs: custom ? '--extension "work tools.ts"' : "",
            customModels: [],
            enabled: true,
          },
          environment: [
            { name: "PI_CODING_AGENT_DIR", value: path.join(dir, "profile"), sensitive: false },
            { name: "API_KEY", value: "test-only-credential", sensitive: true },
            {
              name: "PATH",
              value: `${dir}:${(yield* HostProcess.Environment).PATH ?? ""}`,
              sensitive: false,
            },
          ],
        });
        const manager = yield* TerminalManager.makeWithOptions({
          logsDir: path.join(dir, "logs"),
          ptyAdapter: yield* PtyAdapter.PtyAdapter,
          env: {},
          subprocessInspector: () =>
            Effect.succeed({ hasRunningSubprocess: false, childCommand: null, processIds: [] }),
        });
        const ready = yield* Deferred.make<void>();
        const metadata: unknown[] = [];
        yield* manager.subscribeMetadata((event) =>
          Effect.sync(() => {
            metadata.push(event);
          }),
        );
        const events: unknown[] = [];
        yield* manager.subscribe((event) => {
          events.push(event);
          return event.type === "output" && event.data.includes("READY")
            ? Deferred.succeed(ready, undefined).pipe(Effect.asVoid)
            : Effect.void;
        });
        const input = yield* decodeInput({
          threadId,
          terminalId: "client-chosen",
          command: "evil",
          args: ["evil"],
          env: { API_KEY: "evil" },
          binaryPath: "evil",
          cwd: "/evil",
        });
        assert.deepEqual(input, { threadId });
        const result = yield* openPiTerminal(input).pipe(
          Effect.provide(
            Layer.mergeAll(
              Layer.mock(ThreadManagement.ThreadManagementService)({
                getThreadShell: () =>
                  Effect.succeed({
                    id: threadId,
                    projectId: "project",
                    activeProviderThreadId: "provider-thread",
                    worktreePath: null,
                  } as never),
                getThreadRecords: () =>
                  Effect.succeed({
                    providerThreads: [
                      {
                        id: "provider-thread",
                        driver: "pi",
                        providerInstanceId,
                        nativeThreadRef: { nativeId: path.join(dir, "session.jsonl") },
                      },
                    ],
                  } as never),
              }),
              Layer.mock(ProjectStore.ProjectStoreV2)({
                get: () => Effect.succeedSome({ workspaceRoot: dir } as never),
              }),
              Layer.mock(ProviderInstances.ProviderInstanceRegistry)({
                getInstance: () =>
                  Effect.succeed({
                    driverKind: "pi",
                    enabled: true,
                    terminalLaunch: () => Effect.succeed(launch),
                  } as never),
              }),
              Layer.succeed(TerminalManager.TerminalManager, manager),
            ),
          ),
        );
        assert.equal(result.status, "running");
        yield* Deferred.await(ready);
        const captured = JSON.parse(yield* fs.readFileString(capture));
        assert.deepEqual(captured.args, [
          ...(custom ? ["--extension", "work tools.ts"] : []),
          "--session",
          path.join(dir, "session.jsonl"),
        ]);
        assert.equal(captured.profile, path.join(dir, "profile"));
        assert.equal(captured.credential, "test-only-credential");
        const snapshots: unknown[] = [];
        const stop = yield* manager.observeStream(
          { threadId, terminalId: result.terminalId },
          (event) =>
            Effect.sync(() => {
              snapshots.push(event);
            }),
        );
        stop();
        assert.notInclude(
          JSON.stringify([input, result, events, snapshots, metadata]),
          "test-only-credential",
        );
        yield* manager.close({ threadId, terminalId: result.terminalId });
        for (const file of yield* fs.readDirectory(path.join(dir, "logs"))) {
          assert.notInclude(
            yield* fs.readFileString(path.join(dir, "logs", file)),
            "test-only-credential",
          );
        }
      }).pipe(Effect.scoped),
  );
});

it.effect.each([
  { driver: "codex", nativeId: "/sessions/work.jsonl" },
  { driver: "pi", nativeId: null },
  { driver: "pi", nativeId: "/sessions/not-a-session" },
])("refuses threads without a Pi session: %s", ({ driver, nativeId }) =>
  openPiTerminal({ threadId }).pipe(
    Effect.provide(
      Layer.mergeAll(
        Layer.mock(ThreadManagement.ThreadManagementService)({
          getThreadShell: () => Effect.succeed({ activeProviderThreadId: "p" } as never),
          getThreadRecords: () =>
            Effect.succeed({
              providerThreads: [
                { id: "p", driver, nativeThreadRef: nativeId ? { nativeId } : null },
              ],
            } as never),
        }),
        Layer.mock(ProjectStore.ProjectStoreV2)({}),
        Layer.mock(ProviderInstances.ProviderInstanceRegistry)({}),
        Layer.mock(TerminalManager.TerminalManager)({}),
      ),
    ),
    Effect.flip,
    Effect.tap((error) => Effect.sync(() => assert.equal(error._tag, "TerminalPiSessionError"))),
  ),
);
