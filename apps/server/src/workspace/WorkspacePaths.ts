import * as KeyedLock from "@t3tools/shared/KeyedLock";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";

import { expandHomePath } from "@t3tools/provider-core/server/pathExpansion";
import * as HostProcess from "@t3tools/shared/HostProcess";
import * as RiftWorkspaces from "../vcs/RiftWorkspaces.ts";

export class WorkspaceRootNotExistsError extends Schema.TaggedError<WorkspaceRootNotExistsError>()(
  "WorkspaceRootNotExistsError",
  {
    workspaceRoot: Schema.String,
    normalizedWorkspaceRoot: Schema.String,
  },
) {
  override get message(): string {
    return `Workspace root does not exist: ${this.normalizedWorkspaceRoot}`;
  }
}

export class WorkspaceRootCreateFailedError extends Schema.TaggedError<WorkspaceRootCreateFailedError>()(
  "WorkspaceRootCreateFailedError",
  {
    workspaceRoot: Schema.String,
    normalizedWorkspaceRoot: Schema.String,
    cause: Schema.Defect(),
  },
) {
  override get message(): string {
    return `Failed to create workspace root: ${this.normalizedWorkspaceRoot}`;
  }
}

export class WorkspaceRootStatFailedError extends Schema.TaggedError<WorkspaceRootStatFailedError>()(
  "WorkspaceRootStatFailedError",
  {
    workspaceRoot: Schema.String,
    normalizedWorkspaceRoot: Schema.String,
    phase: Schema.Literals(["validate-existing", "verify-created"]),
    cause: Schema.Defect(),
  },
) {
  override get message(): string {
    return `Failed to stat workspace root '${this.normalizedWorkspaceRoot}' during '${this.phase}'.`;
  }
}

export class WorkspaceRootNotDirectoryError extends Schema.TaggedError<WorkspaceRootNotDirectoryError>()(
  "WorkspaceRootNotDirectoryError",
  {
    workspaceRoot: Schema.String,
    normalizedWorkspaceRoot: Schema.String,
  },
) {
  override get message(): string {
    return `Workspace root is not a directory: ${this.normalizedWorkspaceRoot}`;
  }
}

export class WorkspacePathOutsideRootError extends Schema.TaggedError<WorkspacePathOutsideRootError>()(
  "WorkspacePathOutsideRootError",
  {
    workspaceRoot: Schema.String,
    relativePath: Schema.String,
  },
) {
  override get message(): string {
    return `Workspace file path must be relative to the project root: ${this.relativePath}`;
  }
}

export const WorkspacePathsError = Schema.Union([
  WorkspaceRootNotExistsError,
  WorkspaceRootCreateFailedError,
  WorkspaceRootStatFailedError,
  WorkspaceRootNotDirectoryError,
  WorkspacePathOutsideRootError,
]);
export type WorkspacePathsError = typeof WorkspacePathsError.Type;

export class WorkspacePaths extends Context.Service<
  WorkspacePaths,
  {
    readonly normalizeWorkspaceRoot: (
      workspaceRoot: string,
      options?: {
        readonly createIfMissing?: boolean;
        /** Disable for placeholders which must remain empty for git clone. */
        readonly initializeWithRift?: boolean;
      },
    ) => Effect.Effect<
      string,
      | WorkspaceRootNotExistsError
      | WorkspaceRootCreateFailedError
      | WorkspaceRootStatFailedError
      | WorkspaceRootNotDirectoryError
    >;
    readonly resolveRelativePathWithinRoot: (input: {
      workspaceRoot: string;
      relativePath: string;
    }) => Effect.Effect<
      { absolutePath: string; relativePath: string },
      WorkspacePathOutsideRootError
    >;
  }
>()("t3/workspace/WorkspacePaths") {}

export const make = Effect.gen(function* () {
  const fileSystem = yield* FileSystem.FileSystem;
  // Every creator normalizes before publishing. Even callers that see an
  // existing directory must wait until its exclusive creator finishes init.
  const initializationLocks = yield* KeyedLock.make<string>();
  const homeDirectory = yield* HostProcess.HomeDirectory;
  const path = yield* Path.Path;
  const rift = yield* RiftWorkspaces.RiftWorkspaces;

  const statWorkspaceRoot = Effect.fn("WorkspacePaths.statWorkspaceRoot")(function* (
    workspaceRoot: string,
    normalizedWorkspaceRoot: string,
    phase: WorkspaceRootStatFailedError["phase"],
  ) {
    return yield* fileSystem.stat(normalizedWorkspaceRoot).pipe(
      Effect.matchEffect({
        onFailure: (cause) =>
          cause.reason._tag === "NotFound"
            ? Effect.succeed(null)
            : Effect.fail(
                new WorkspaceRootStatFailedError({
                  workspaceRoot,
                  normalizedWorkspaceRoot,
                  phase,
                  cause,
                }),
              ),
        onSuccess: Effect.succeed,
      }),
    );
  });

  const normalizeWorkspaceRoot: WorkspacePaths["Service"]["normalizeWorkspaceRoot"] = Effect.fn(
    "WorkspacePaths.normalizeWorkspaceRoot",
  )(
    function* (workspaceRoot, options) {
      const normalizedWorkspaceRoot = path.resolve(
        expandHomePath(workspaceRoot.trim(), homeDirectory),
      );
      let workspaceStat = yield* statWorkspaceRoot(
        workspaceRoot,
        normalizedWorkspaceRoot,
        "validate-existing",
      );
      let created = false;
      if (!workspaceStat && options?.createIfMissing) {
        // mkdir -p cannot tell whether a concurrent creator won. Make parents
        // first, then exclusively claim the leaf; only that winner may run init.
        yield* Effect.gen(function* () {
          yield* fileSystem.makeDirectory(path.dirname(normalizedWorkspaceRoot), {
            recursive: true,
          });
          created = yield* fileSystem.makeDirectory(normalizedWorkspaceRoot).pipe(
            Effect.as(true),
            Effect.catchIf(
              (cause) => cause.reason._tag === "AlreadyExists",
              () => Effect.succeed(false),
            ),
          );
        }).pipe(
          Effect.mapError(
            (cause) =>
              new WorkspaceRootCreateFailedError({
                workspaceRoot,
                normalizedWorkspaceRoot,
                cause,
              }),
          ),
        );
        workspaceStat = yield* statWorkspaceRoot(
          workspaceRoot,
          normalizedWorkspaceRoot,
          "verify-created",
        );
      }
      if (!workspaceStat) {
        return yield* new WorkspaceRootNotExistsError({
          workspaceRoot,
          normalizedWorkspaceRoot,
        });
      }
      if (workspaceStat.type !== "Directory") {
        return yield* new WorkspaceRootNotDirectoryError({
          workspaceRoot,
          normalizedWorkspaceRoot,
        });
      }
      if (created && options?.initializeWithRift !== false)
        yield* rift.initializeNewFolder(normalizedWorkspaceRoot);
      return normalizedWorkspaceRoot;
    },
    (effect, workspaceRoot) =>
      initializationLocks.withLock(
        path.resolve(expandHomePath(workspaceRoot.trim(), homeDirectory)),
        effect,
      ),
  );

  const resolveRelativePathWithinRoot: WorkspacePaths["Service"]["resolveRelativePathWithinRoot"] =
    Effect.fn("WorkspacePaths.resolveRelativePathWithinRoot")(function* (input) {
      const normalizedInputPath = input.relativePath.trim();
      if (path.isAbsolute(normalizedInputPath)) {
        return yield* new WorkspacePathOutsideRootError({
          workspaceRoot: input.workspaceRoot,
          relativePath: input.relativePath,
        });
      }

      const absolutePath = path.resolve(input.workspaceRoot, normalizedInputPath);
      // Only the host separator becomes `/`: on POSIX `\` is a file name character.
      const relativeToRoot = path
        .relative(input.workspaceRoot, absolutePath)
        .replaceAll(path.sep, "/");
      if (
        relativeToRoot.length === 0 ||
        relativeToRoot === "." ||
        relativeToRoot.startsWith("../") ||
        relativeToRoot === ".." ||
        path.isAbsolute(relativeToRoot)
      ) {
        return yield* new WorkspacePathOutsideRootError({
          workspaceRoot: input.workspaceRoot,
          relativePath: input.relativePath,
        });
      }

      return {
        absolutePath,
        relativePath: relativeToRoot,
      };
    });

  return WorkspacePaths.of({ normalizeWorkspaceRoot, resolveRelativePathWithinRoot });
});

export const layer = Layer.effect(WorkspacePaths, make).pipe(Layer.provide(RiftWorkspaces.layer));
