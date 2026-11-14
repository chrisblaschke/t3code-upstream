import { assert, it } from "@effect/vitest";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as HostProcess from "@t3tools/shared/HostProcess";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as ChildProcess from "effect/process/ChildProcess";
import * as ChildProcessSpawner from "effect/process/ChildProcessSpawner";
import { findKittyApp, openKittyTerminal } from "./kittyTerminal.ts";

it.effect("does not offer the Mac launcher on other platforms", () =>
  Effect.gen(function* () {
    assert.isNull(yield* findKittyApp("/home/user"));
  }).pipe(Effect.provideService(HostProcess.Platform, "linux"), Effect.provide(NodeServices.layer)),
);

it.effect("rejects relative working directories before launching", () =>
  Effect.gen(function* () {
    const error = yield* openKittyTerminal({ cwd: "../project" }).pipe(Effect.flip);
    assert.equal(error._tag, "ExternalTerminalLaunchError");
    assert.include(error.detail, "absolute directory");
  }).pipe(Effect.provide(NodeServices.layer)),
);

it.effect("opens a new kitty instance with individually quoted command arguments", () =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const launched: Array<ChildProcess.Command> = [];
    yield* openKittyTerminal({
      cwd: process.cwd(),
      command: ["pi", "--session", "/tmp/User's session.jsonl"],
    }).pipe(
      Effect.provideService(FileSystem.FileSystem, {
        ...fs,
        exists: () => Effect.succeed(true),
        readDirectory: () => Effect.succeed([]),
      }),
      Effect.provide(
        Layer.mock(ChildProcessSpawner.ChildProcessSpawner)({
          exitCode: (command) => {
            launched.push(command);
            return Effect.succeed(ChildProcessSpawner.ExitCode(0));
          },
        }),
      ),
      Effect.provideService(HostProcess.Platform, "darwin"),
    );
    assert.lengthOf(launched, 1);
    const launch = launched[0]!;
    assert.isTrue(ChildProcess.isStandardCommand(launch));
    if (!ChildProcess.isStandardCommand(launch)) return;
    assert.equal(launch.command, "open");
    assert.include(launch.args, "-n");
    assert.include(launch.args, `--directory=${process.cwd()}`);
    assert.include(launch.args.join(" "), "'pi' '--session' '/tmp/User'\\''s session.jsonl'");
  }).pipe(Effect.provide(NodeServices.layer)),
);
