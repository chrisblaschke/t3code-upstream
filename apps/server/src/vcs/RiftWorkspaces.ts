// @effect-diagnostics nodeBuiltinImport:off - Effect has no statfs; executable discovery is cached process-wide, outside an Effect runtime.
import * as NodeFS from "node:fs";
import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Path from "effect/Path";
import { GitCommandError } from "@t3tools/contracts";
import * as HostProcess from "@t3tools/shared/HostProcess";
import * as ProcessRunner from "../processRunner.ts";

/** Resolve the executable once, including negative results. No shim/Node startup. */
export const makeBinaryAvailabilityCheck = (searchPath?: string) => {
  let cached: Promise<boolean> | undefined;
  return () =>
    (cached ??= (async () => {
      // Read PATH at first use, after the server has initialized its environment.
      for (const directory of (searchPath ?? process.env.PATH ?? "").split(NodePath.delimiter)) {
        if (!directory) continue;
        const executable = NodePath.join(directory, "rift");
        try {
          await NodeFSP.access(executable, NodeFS.constants.X_OK);
          if ((await NodeFSP.stat(executable)).isFile()) return true;
        } catch {
          // Missing or inaccessible PATH entries are not capabilities.
        }
      }
      return false;
    })());
};

export const filesystemSupportsCow = (platform: string, type: number) =>
  (platform === "linux" && (type === 0x9123683e || type === 0x58465342)) ||
  (platform === "darwin" && type === 26);

export const filesystemIsReady = (platform: string, type: number, inode: number) =>
  filesystemSupportsCow(platform, type) && (type !== 0x9123683e || inode === 256);

const binaryAvailable = makeBinaryAvailabilityCheck();
export const RiftHostChecks = Context.Reference<{
  readonly binaryAvailable: () => Promise<boolean>;
  readonly filesystemReady: (cwd: string, platform: NodeJS.Platform) => Promise<boolean>;
  readonly filesystemSupportsCow: (cwd: string, platform: NodeJS.Platform) => Promise<boolean>;
}>("t3/vcs/RiftHostChecks", {
  defaultValue: () => ({
    binaryAvailable,
    filesystemSupportsCow: async (cwd, platform) =>
      filesystemSupportsCow(platform, (await NodeFSP.statfs(cwd)).type),
    filesystemReady: async (cwd, platform) => {
      const [filesystem, directory] = await Promise.all([NodeFSP.statfs(cwd), NodeFSP.stat(cwd)]);
      return filesystemIsReady(platform, filesystem.type, directory.ino);
    },
  }),
});

export const initializationHint = (cwd: string) =>
  `Run \`rift init\` in ${cwd} once, while nothing is working in this project.`;

export class RiftWorkspaces extends Context.Service<RiftWorkspaces, Effect.Success<typeof make>>()(
  "t3/vcs/RiftWorkspaces",
) {}

/** Rift owns the registry and storage layout; never infer ownership from a path prefix. */
export const make = Effect.gen(function* () {
  const runner = yield* ProcessRunner.ProcessRunner;
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const host = yield* RiftHostChecks;
  const platform = yield* HostProcess.Platform;
  const hostCheckError = (cwd: string, detail: string) => (cause: unknown) =>
    new GitCommandError({
      operation: "RiftWorkspaces.hostCheck",
      command: "rift",
      cwd,
      detail,
      cause,
    });
  const available = yield* Effect.tryPromise({
    try: host.binaryAvailable,
    catch: hostCheckError(process.cwd(), "Could not inspect the rift executable."),
  }).pipe(
    Effect.catchCause((cause) =>
      Effect.logWarning("Failed to check rift executable; continuing without Copy", { cause }).pipe(
        Effect.as(false),
      ),
    ),
  );
  const run = Effect.fn("RiftWorkspaces.run")(function* (cwd: string, args: string[]) {
    const context = { operation: "RiftWorkspaces", command: "rift", cwd };
    const result = yield* runner
      .run({
        command: "rift",
        args,
        cwd,
        timeout: "5 minutes",
        maxOutputBytes: 1024 * 1024,
      })
      .pipe(
        Effect.mapError(
          (cause) =>
            new GitCommandError({
              ...context,
              detail: "Could not run rift. Check that rift is installed on this server.",
              cause,
            }),
        ),
      );
    if (result.code !== 0)
      return yield* new GitCommandError({
        ...context,
        detail: `rift ${args[0]} failed. Check the filesystem supports copy-on-write, the repository is not a worktree or mid-operation, and any .rift.toml hooks succeed.`,
        ...(result.code === null ? {} : { exitCode: result.code }),
        stderrLength: result.stderr.length,
      });
    return result.stdout;
  });
  const usable = Effect.fn("RiftWorkspaces.usable")(
    function* (cwd: string) {
      // Readiness is always read-only. T3 only initializes folders it just
      // created, before publishing a project: btrfs init replaces the directory
      // and would invalidate existing processes' cwd and watchers.
      if (!available || (yield* fs.stat(path.join(cwd, ".git"))).type !== "Directory") return false;
      // init writes this marker on every platform. Rift itself validates registry
      // membership on create/list; a stale or copied marker cannot authorize a launch.
      if ((yield* fs.stat(path.join(cwd, ".rift"))).type !== "File") return false;
      if (!(yield* fs.readFileString(path.join(cwd, ".rift"))).trim()) return false;
      return yield* Effect.tryPromise({
        try: () => host.filesystemReady(cwd, platform),
        catch: hostCheckError(cwd, "Could not inspect filesystem readiness for rift."),
      });
    },
    Effect.orElseSucceed(() => false),
  );
  const list = (cwd: string) =>
    run(cwd, ["list"]).pipe(
      Effect.map((stdout) =>
        stdout
          .split(/\r?\n/)
          .map((line) => line.trim())
          .filter((line) => path.isAbsolute(line)),
      ),
    );
  const remove = (cwd: string, workspace: string) =>
    run(cwd, ["remove", workspace]).pipe(Effect.asVoid);
  const create = Effect.fn("RiftWorkspaces.create")(function* (cwd: string, branch: string) {
    if (!(yield* usable(cwd)))
      return yield* new GitCommandError({
        operation: "RiftWorkspaces.create",
        command: "rift",
        cwd,
        detail: available
          ? initializationHint(cwd)
          : "Copy (rift) requires rift on this server. Install it and restart T3.",
      });
    const before = new Set(yield* list(cwd));
    // Git branch names can contain slash; Rift names are single path components.
    const name = branch.replace(/[^a-zA-Z0-9._-]/g, "-");
    const stdout = yield* run(cwd, ["create", "--copy-all", "--name", name]).pipe(
      Effect.onError(() =>
        list(cwd).pipe(
          Effect.flatMap((children) =>
            Effect.forEach(
              children.filter((child) => !before.has(child) && path.basename(child) === name),
              (child) => remove(cwd, child),
            ),
          ),
          Effect.catchCause((cause) =>
            Effect.logWarning("Failed to clean up unsuccessful rift creation", { cause }),
          ),
        ),
      ),
    );
    const workspace = stdout.trim();
    // Only bind a direct registered child, never a hook's output or the root.
    if (
      !path.isAbsolute(workspace) ||
      !(yield* list(cwd)).includes(workspace) ||
      before.has(workspace)
    ) {
      return yield* new GitCommandError({
        operation: "RiftWorkspaces.create",
        command: "rift",
        cwd,
        detail: "rift create did not return a new registered workspace path.",
      });
    }
    return workspace;
  });
  const initializeNewFolder = Effect.fn("RiftWorkspaces.initializeNewFolder")(
    function* (cwd: string) {
      if (
        !available ||
        !(yield* Effect.tryPromise({
          try: () => host.filesystemSupportsCow(cwd, platform),
          catch: hostCheckError(cwd, "Could not inspect copy-on-write support for rift."),
        }))
      )
        return;
      yield* run(cwd, ["init", "--here"]);
    },
    (effect, cwd) =>
      effect.pipe(
        Effect.catchCause((cause) =>
          // init can write its marker before registration fails. Do not offer
          // Copy for a partially initialized folder; this root is still ours.
          unregisterNewFolder(cwd).pipe(
            Effect.andThen(fs.remove(path.join(cwd, ".rift"), { force: true })),
            Effect.ignore,
            Effect.andThen(
              Effect.logWarning(
                "Failed to initialize new project with rift; continuing without Copy",
                {
                  cwd,
                  cause,
                },
              ),
            ),
          ),
        ),
      ),
  );
  // Only for rollback of an exclusively claimed, still-unpublished folder.
  // Root removal unregisters metadata (it does not trash the root directory).
  // Never run global gc here; it could affect unrelated workspaces.
  const unregisterNewFolder = Effect.fn("RiftWorkspaces.unregisterNewFolder")(
    function* (cwd: string) {
      if (!available || !(yield* fs.exists(path.join(cwd, ".rift")))) return;
      yield* run(cwd, ["remove", "--no-hooks", "--force", cwd]);
    },
    (effect, cwd) =>
      effect.pipe(
        Effect.catchCause((cause) =>
          Effect.logWarning("Failed to unregister unused new rift project folder", { cwd, cause }),
        ),
      ),
  );
  return { usable, list, create, remove, initializeNewFolder, unregisterNewFolder };
});

export const layer = Layer.effect(RiftWorkspaces, make).pipe(Layer.provide(ProcessRunner.layer));
