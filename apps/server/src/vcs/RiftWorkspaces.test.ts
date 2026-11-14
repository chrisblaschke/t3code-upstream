import * as NodeServices from "@effect/platform-node/NodeServices";
import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Path from "effect/Path";
import * as Sink from "effect/Sink";
import * as Stream from "effect/Stream";
import { ChildProcess, ChildProcessSpawner } from "effect/process";
import * as ServerConfig from "../config.ts";
import * as ProcessRunner from "../processRunner.ts";
import { makeGitVcsDriverCore } from "./GitVcsDriverCore.ts";
import {
  filesystemIsReady,
  filesystemSupportsCow,
  initializationHint,
  makeBinaryAvailabilityCheck,
  make,
  RiftHostChecks,
  RiftWorkspaces,
} from "./RiftWorkspaces.ts";

const layerTest = ServerConfig.layerTest(process.cwd(), { prefix: "t3-rift-test-" }).pipe(
  Layer.provideMerge(Layer.mergeAll(NodeServices.layer, NodeCrypto.layer)),
);
const handle = (stdout: string, code = 0) =>
  ChildProcessSpawner.makeHandle({
    pid: ChildProcessSpawner.ProcessId(1),
    exitCode: Effect.succeed(ChildProcessSpawner.ExitCode(code)),
    isRunning: Effect.succeed(false),
    kill: () => Effect.void,
    unref: Effect.succeed(Effect.void),
    stdin: Sink.drain,
    stdout: Stream.encodeText(Stream.make(stdout)),
    stderr: Stream.empty,
    all: Stream.empty,
    getInputFd: () => Sink.drain,
    getOutputFd: () => Stream.empty,
  });

const fixtureWithAvailability = (available = true) =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const parent = yield* fs.makeTempDirectoryScoped({ prefix: "rift-driver-" });
    const cwd = path.join(parent, "project");
    const copy = path.join(parent, "snapshot");
    yield* fs.makeDirectory(cwd);
    const delegate = yield* ChildProcessSpawner.ChildProcessSpawner;
    const riftCalls: string[][] = [];
    const gitCalls: string[][] = [];
    let registered = false;
    let ready = true;
    const spawner = ChildProcessSpawner.make((command) =>
      Effect.gen(function* () {
        if (!ChildProcess.isStandardCommand(command)) return yield* delegate.spawn(command);
        if (path.basename(command.command) !== "rift") {
          gitCalls.push(command.args.map(String));
          return yield* delegate.spawn(command);
        }
        const args = command.args.map(String);
        riftCalls.push(args);
        if (!available) return handle("", 1);
        if (args[0] === "create") {
          yield* fs.copy(cwd, copy);
          registered = true;
          return handle(`${copy}\n`);
        }
        if (args[0] === "list") return handle(registered ? `${copy}\n` : "");
        if (args[0] === "remove") {
          yield* fs.remove(copy, { recursive: true });
          registered = false;
        }
        return handle("");
      }),
    );
    const host = {
      binaryAvailable: async () => available,
      filesystemReady: async () => ready,
      filesystemSupportsCow: async () => ready,
    };
    const rift = yield* make.pipe(
      Effect.provide(ProcessRunner.layer),
      Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, spawner),
      Effect.provideService(RiftHostChecks, host),
    );
    const driver = yield* makeGitVcsDriverCore().pipe(
      Effect.provideService(RiftWorkspaces, rift),
      Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, spawner),
    );
    const git = (cwd: string, args: string[]) => driver.execute({ operation: "test", cwd, args });
    yield* git(cwd, ["init", "-b", "main"]);
    yield* git(cwd, ["config", "user.email", "test@example.invalid"]);
    yield* git(cwd, ["config", "user.name", "Test"]);
    yield* fs.writeFileString(path.join(cwd, "tracked"), "original\n");
    yield* git(cwd, ["add", "."]);
    yield* git(cwd, ["commit", "-m", "initial"]);
    yield* fs.writeFileString(path.join(cwd, ".rift"), "test-workspace-id\n");
    return {
      fs,
      path,
      cwd,
      copy,
      driver,
      rift,
      git,
      riftCalls,
      gitCalls,
      notNative: () => {
        ready = false;
      },
    };
  });

const fixture = fixtureWithAvailability();

it.effect("initializes only the newly claimed folder, never its Git ancestor", () =>
  Effect.gen(function* () {
    const f = yield* fixture;
    const leaf = f.path.join(f.cwd, "new-project");
    yield* f.fs.makeDirectory(leaf);
    yield* f.rift.initializeNewFolder(leaf);
    assert.deepEqual(f.riftCalls, [["init", "--here"]]);
  }).pipe(Effect.scoped, Effect.provide(layerTest)),
);

it.effect("repeated usable checks never spawn a process and reflect filesystem readiness", () =>
  Effect.gen(function* () {
    const f = yield* fixture;
    const gitCount = f.gitCalls.length;
    for (let i = 0; i < 5; i++) assert.isTrue(yield* f.rift.usable(f.cwd));
    assert.deepEqual(f.riftCalls, []);
    assert.equal(f.gitCalls.length, gitCount);
    f.notNative();
    assert.isFalse((yield* f.driver.listRefs({ cwd: f.cwd })).riftUsable);
    const failure = yield* f.rift.create(f.cwd, "task").pipe(Effect.flip);
    assert.include(failure.message, initializationHint(f.cwd));
    assert.deepEqual(f.riftCalls, []);
  }).pipe(Effect.scoped, Effect.provide(layerTest)),
);

it.effect("hides rift and rejects launch when the executable is unavailable", () =>
  Effect.gen(function* () {
    const f = yield* fixtureWithAvailability(false);
    assert.isFalse((yield* f.driver.listRefs({ cwd: f.cwd })).riftUsable);
    const failure = yield* f.rift.create(f.cwd, "task").pipe(Effect.flip);
    assert.include(failure.message, "Install it and restart T3");
    assert.deepEqual(f.riftCalls, []);
  }).pipe(Effect.scoped, Effect.provide(layerTest)),
);

it.effect("caches positive and negative executable discovery until a new process check", () =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const bin = yield* fs.makeTempDirectoryScoped({ prefix: "rift-bin-" });
    const executable = path.join(bin, "rift");
    const unavailable = makeBinaryAvailabilityCheck(bin);
    assert.isFalse(yield* Effect.promise(unavailable));
    yield* fs.writeFileString(executable, "#!/bin/sh\nexit 0\n");
    yield* fs.chmod(executable, 0o755);
    assert.isFalse(yield* Effect.promise(unavailable));
    const available = makeBinaryAvailabilityCheck(bin);
    assert.isTrue(yield* Effect.promise(available));
    yield* fs.remove(executable);
    assert.isTrue(yield* Effect.promise(available));
    assert.isFalse(yield* Effect.promise(makeBinaryAvailabilityCheck(bin)));
  }).pipe(Effect.scoped, Effect.provide(layerTest)),
);

it("requires btrfs subvolume inode 256, accepts initialized XFS/APFS, rejects other filesystems", () => {
  assert.isTrue(filesystemSupportsCow("linux", 0x9123683e));
  assert.isTrue(filesystemSupportsCow("linux", 0x58465342));
  assert.isTrue(filesystemSupportsCow("darwin", 26));
  assert.isFalse(filesystemSupportsCow("linux", 0xef53));
  assert.isFalse(filesystemSupportsCow("darwin", 17));
  assert.isFalse(filesystemSupportsCow("win32", 26));
  assert.isTrue(filesystemIsReady("linux", 0x9123683e, 256));
  assert.isFalse(filesystemIsReady("linux", 0x9123683e, 1000));
  assert.isFalse(filesystemIsReady("linux", 0xef53, 256));
  assert.isTrue(filesystemIsReady("linux", 0x58465342, 1000));
  assert.isTrue(filesystemIsReady("darwin", 26, 1000));
  assert.isFalse(filesystemIsReady("darwin", 17, 1000));
  assert.isFalse(filesystemIsReady("win32", 26, 256));
});

it.effect(
  "creates a rift snapshot, branches only in the copy, accepts membership and removes via rift",
  () =>
    Effect.gen(function* () {
      const f = yield* fixture;
      yield* f.fs.writeFileString(f.path.join(f.cwd, "tracked"), "dirty\n");
      assert.isTrue((yield* f.driver.listRefs({ cwd: f.cwd })).riftUsable);
      const claimed: string[] = [];
      const created = yield* f.driver.createWorktree(
        {
          cwd: f.cwd,
          refName: "ignored-base-ref",
          newRefName: "t3/feature",
          path: null,
          workspaceKind: "rift",
        },
        {
          progress: {
            onWorktreeClaimed: (path) =>
              Effect.sync(() => {
                claimed.push(path);
              }),
          },
        },
      );
      assert.equal(created.worktree.path, f.copy);
      assert.isFalse(f.riftCalls.some((args) => args[0] === "init"));
      assert.deepEqual(claimed, [f.copy]);
      assert.isTrue(
        f.riftCalls.some(
          (args) =>
            JSON.stringify(args) ===
            JSON.stringify(["create", "--copy-all", "--name", "t3-feature"]),
        ),
      );
      assert.isTrue(f.gitCalls.some((args) => args.join(" ") === "switch -c t3/feature"));
      assert.isFalse(f.gitCalls.some((args) => args.includes("add") && args.includes("worktree")));
      assert.equal(
        (yield* f.git(f.copy, ["branch", "--show-current"])).stdout.trim(),
        "t3/feature",
      );
      assert.equal((yield* f.git(f.cwd, ["branch", "--show-current"])).stdout.trim(), "main");
      assert.equal(yield* f.fs.readFileString(f.path.join(f.copy, "tracked")), "dirty\n");
      assert.include(yield* f.driver.listWorktreePaths(f.cwd), f.copy);
      yield* f.driver.removeWorktree({ cwd: f.cwd, path: f.copy, force: true });
      assert.deepEqual(f.riftCalls.at(-1), ["remove", f.copy]);
      assert.isFalse(yield* f.fs.exists(f.copy));
      assert.isFalse(
        f.gitCalls.some((args) => args.includes("remove") && args.includes("worktree")),
      );
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
);

it.effect("hides rift for linked worktrees without spawning rift", () =>
  Effect.gen(function* () {
    const f = yield* fixture;
    const linked = f.path.join(f.path.dirname(f.cwd), "linked");
    yield* f.git(f.cwd, ["worktree", "add", "-b", "linked", linked]);
    assert.isFalse((yield* f.driver.listRefs({ cwd: linked })).riftUsable);
    assert.deepEqual(f.riftCalls, []);
    yield* f.fs.remove(f.path.join(f.cwd, ".rift"));
    assert.isFalse((yield* f.driver.listRefs({ cwd: f.cwd })).riftUsable);
    const failure = yield* f.driver
      .createWorktree({
        cwd: f.cwd,
        refName: "HEAD",
        newRefName: "task",
        path: null,
        workspaceKind: "rift",
      })
      .pipe(Effect.flip);
    assert.include(failure.message, initializationHint(f.cwd));
    assert.isFalse(f.riftCalls.some((args) => args[0] === "init" || args[0] === "create"));
  }).pipe(Effect.scoped, Effect.provide(layerTest)),
);

it.effect("rolls back a registered copy when git cannot create its branch", () =>
  Effect.gen(function* () {
    const f = yield* fixture;
    yield* f.driver
      .createWorktree({
        cwd: f.cwd,
        refName: "HEAD",
        newRefName: "bad..branch",
        path: null,
        workspaceKind: "rift",
      })
      .pipe(Effect.flip);
    assert.include(
      f.riftCalls.map((args) => args[0]),
      "remove",
    );
    assert.isFalse(yield* f.fs.exists(f.copy));
  }).pipe(Effect.scoped, Effect.provide(layerTest)),
);
