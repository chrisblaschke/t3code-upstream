import * as NodeServices from "@effect/platform-node/NodeServices";
import { it, describe, expect } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Path from "effect/Path";
import * as PlatformError from "effect/PlatformError";

import * as HostProcess from "@t3tools/shared/HostProcess";

import * as WorkspacePaths from "./WorkspacePaths.ts";
import * as RiftWorkspaces from "../vcs/RiftWorkspaces.ts";

const layerTest = Layer.empty.pipe(
  Layer.provideMerge(WorkspacePaths.layer),
  Layer.provideMerge(NodeServices.layer),
);

const makeTempDir = Effect.fn("makeTempDir")(function* () {
  const fileSystem = yield* FileSystem.FileSystem;
  return yield* fileSystem.makeTempDirectoryScoped({
    prefix: "t3code-project-paths-",
  });
});

it.layer(layerTest)("WorkspacePathsLive", (it) => {
  describe("normalizeWorkspaceRoot", () => {
    it.effect("creates missing directories when createIfMissing is enabled", () =>
      Effect.gen(function* () {
        const workspacePaths = yield* WorkspacePaths.WorkspacePaths;
        const fileSystem = yield* FileSystem.FileSystem;
        const cwd = yield* makeTempDir();
        const path = yield* Path.Path;
        const missingPath = path.join(cwd, "nested", "new-project");

        const resolved = yield* workspacePaths.normalizeWorkspaceRoot(missingPath, {
          createIfMissing: true,
        });
        const stat = yield* fileSystem.stat(resolved);

        expect(resolved).toBe(missingPath);
        expect(stat.type).toBe("Directory");
      }),
    );

    it.effect("waits for initialization even when a concurrent caller sees the new folder", () =>
      Effect.gen(function* () {
        const path = yield* Path.Path;
        const root = path.join(yield* makeTempDir(), "new-project");
        let enter!: () => void;
        let release!: () => void;
        const entered = new Promise<void>((resolve) => {
          enter = resolve;
        });
        const gate = new Promise<void>((resolve) => {
          release = resolve;
        });
        const paths = yield* WorkspacePaths.make.pipe(
          Effect.provide(Layer.fresh(RiftWorkspaces.layer)),
          Effect.provideService(RiftWorkspaces.RiftHostChecks, {
            binaryAvailable: async () => true,
            filesystemReady: async () => false,
            filesystemSupportsCow: async () => {
              enter();
              await gate;
              return false;
            },
          }),
        );
        const first = yield* paths
          .normalizeWorkspaceRoot(root, { createIfMissing: true })
          .pipe(Effect.forkScoped);
        yield* Effect.promise(() => entered);
        let secondFinished = false;
        const second = yield* paths.normalizeWorkspaceRoot(root, { createIfMissing: true }).pipe(
          Effect.tap(() =>
            Effect.sync(() => {
              secondFinished = true;
            }),
          ),
          Effect.forkScoped,
        );
        yield* Effect.yieldNow;
        const publishedDuringInitialization = secondFinished;
        release();
        expect(publishedDuringInitialization).toBe(false);
        expect(yield* Fiber.join(first)).toBe(root);
        expect(yield* Fiber.join(second)).toBe(root);
      }),
    );

    it.effect("does not initialize when another creator wins the leaf mkdir race", () =>
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const path = yield* Path.Path;
        const parent = yield* makeTempDir();
        const root = path.join(parent, "racing-root");
        let cowChecks = 0;
        const workspacePaths = yield* WorkspacePaths.make.pipe(
          Effect.provide(Layer.fresh(RiftWorkspaces.layer)),
          Effect.provideService(RiftWorkspaces.RiftHostChecks, {
            binaryAvailable: async () => true,
            filesystemReady: async () => false,
            filesystemSupportsCow: async () => {
              cowChecks++;
              return false;
            },
          }),
          Effect.provideService(FileSystem.FileSystem, {
            ...fs,
            makeDirectory: (folder, options) =>
              folder === root
                ? fs.makeDirectory(root).pipe(
                    Effect.andThen(
                      Effect.fail(
                        PlatformError.systemError({
                          _tag: "AlreadyExists",
                          module: "FileSystem",
                          method: "mkdir",
                          pathOrDescriptor: root,
                        }),
                      ),
                    ),
                  )
                : fs.makeDirectory(folder, options),
          }),
        );
        expect(yield* workspacePaths.normalizeWorkspaceRoot(root, { createIfMissing: true })).toBe(
          root,
        );
        expect(cowChecks).toBe(0);
      }),
    );
  });

  describe("resolveRelativePathWithinRoot", () => {
    it.effect("rejects paths that escape the workspace root", () =>
      Effect.gen(function* () {
        const workspacePaths = yield* WorkspacePaths.WorkspacePaths;
        const cwd = yield* makeTempDir();

        const error = yield* workspacePaths
          .resolveRelativePathWithinRoot({
            workspaceRoot: cwd,
            relativePath: "../escape.md",
          })
          .pipe(Effect.flip);

        expect(error.message).toContain(
          "Workspace file path must be relative to the project root: ../escape.md",
        );
      }),
    );

    // On Windows `\` is a separator, so these names cannot exist there.
    it.effect.skipIf(HostProcess.Platform.defaultValue() === "win32")(
      "keeps a backslash in a name as part of the name",
      () =>
        Effect.gen(function* () {
          const workspacePaths = yield* WorkspacePaths.WorkspacePaths;
          const path = yield* Path.Path;
          const cwd = yield* makeTempDir();

          for (const relativePath of ["weird\\name.txt", "..\\..\\etc"]) {
            expect(
              yield* workspacePaths.resolveRelativePathWithinRoot({
                workspaceRoot: cwd,
                relativePath,
              }),
            ).toEqual({ absolutePath: path.join(cwd, relativePath), relativePath });
          }
        }),
    );
  });
});
