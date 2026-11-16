import { OrchestratorMcpFailure, type WorktreeMcpListResult } from "@t3tools/contracts";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import * as Orchestrator from "../../../orchestration-v2/Orchestrator.ts";
import { primaryWorktree, worktreeStatus } from "./inventory.ts";
import * as Option from "effect/Option";
import * as Project from "../../../project/ProjectService.ts";
import { readThread, unavailable } from "../../threadAccess.ts";
import * as Effect from "effect/Effect";

import * as McpInvocationContext from "../../McpInvocationContext.ts";
import * as McpToolAccess from "../../McpToolAccess.ts";
import * as WorktreeMcpService from "../../WorktreeMcpService.ts";
import { WorktreeToolkit } from "./tools.ts";

const handlers = {
  t3_worktree_list: McpToolAccess.reads((input) =>
    Effect.gen(function* () {
      const context = yield* McpInvocationContext.McpInvocationContext;
      if (!context.capabilities.has("worktree"))
        return yield* new OrchestratorMcpFailure({
          code: "capability_denied",
          message: "This credential cannot inspect worktrees.",
        });
      const {
        threadId,
        projectId,
        includeStatus,
        includeOwners,
        environmentId: _,
        ...refs
      } = input;
      if (threadId !== undefined && projectId !== undefined)
        return yield* new OrchestratorMcpFailure({
          code: "invalid_request",
          message: "Pass either projectId or threadId, not both.",
        });
      const thread =
        projectId === undefined ? (yield* readThread(threadId)).projection.thread : undefined;
      const projects = yield* Project.ProjectService;
      const project = yield* projects
        .getById(projectId ?? thread!.projectId)
        .pipe(Effect.mapError(unavailable));
      if (Option.isNone(project))
        return yield* new OrchestratorMcpFailure({
          code: "invalid_request",
          message: "The project was not found.",
        });
      const service = yield* WorktreeMcpService.WorktreeMcpService;
      const listed = yield* service
        .listRefs({
          ...refs,
          worktreePath: thread?.worktreePath ?? null,
          workspaceRoot: project.value.workspaceRoot,
        })
        .pipe(Effect.mapError(unavailable));
      if (!listed.refs.some((ref) => ref.worktreePath !== null)) return listed;
      const pathService = yield* Path.Path;
      const fs = yield* FileSystem.FileSystem;
      const cwd =
        thread?.worktreePath &&
        (yield* fs.exists(thread.worktreePath).pipe(Effect.mapError(unavailable)))
          ? thread.worktreePath
          : project.value.workspaceRoot;
      const resolve = (path: string) => pathService.resolve(path);
      // Git prints symlink-resolved paths (/private/var on macOS); T3 may record the link form.
      const canonical = (path: string) =>
        fs.realPath(path).pipe(Effect.orElseSucceed(() => resolve(path)));
      const primaryPath = yield* primaryWorktree(cwd).pipe(Effect.mapError(unavailable));
      const primary = primaryPath === null ? null : yield* canonical(primaryPath);
      const snapshot = includeOwners
        ? yield* (yield* Orchestrator.OrchestratorV2)
            .getShellSnapshot()
            .pipe(Effect.mapError(unavailable))
        : undefined;
      const ownerProjects = includeOwners
        ? (yield* projects.snapshot.pipe(Effect.mapError(unavailable))).projects
        : [];
      const shells = yield* Effect.forEach(
        snapshot ? [...snapshot.threads, ...snapshot.archivedThreads] : [],
        (owner) => {
          const ownerPath =
            owner.worktreePath ??
            ownerProjects.find((project) => project.id === owner.projectId)?.workspaceRoot;
          return owner.deletedAt !== null || ownerPath === undefined
            ? Effect.succeed([])
            : canonical(ownerPath).pipe(Effect.map((path) => [{ owner, path }]));
        },
        { concurrency: 4 },
      ).pipe(Effect.map((owners) => owners.flat()));
      const paths = [
        ...new Set(
          listed.refs.flatMap((ref) =>
            ref.worktreePath === null ? [] : [resolve(ref.worktreePath)],
          ),
        ),
      ];
      const inventory = new Map(
        yield* Effect.forEach(
          paths,
          (path) =>
            Effect.gen(function* () {
              const real = yield* canonical(path);
              const keys = new Set([path, real]);
              return [
                path,
                {
                  isMainWorktree: primary !== null && primary === real,
                  ...(includeStatus ? { status: yield* worktreeStatus(path) } : {}),
                  ...(includeOwners
                    ? {
                        owners: shells
                          .filter(({ path }) => keys.has(path))
                          .map(({ owner }) => ({
                            threadId: owner.id,
                            projectId: owner.projectId,
                            settled: owner.settledOverride === "settled",
                            archived: owner.archivedAt !== null,
                          })),
                      }
                    : {}),
                },
              ] as const;
            }),
          { concurrency: 4 },
        ),
      );
      return {
        ...listed,
        refs: listed.refs.map((ref) =>
          ref.worktreePath === null ? ref : { ...ref, ...inventory.get(resolve(ref.worktreePath)) },
        ),
      } satisfies WorktreeMcpListResult;
    }),
  ),
  t3_worktree_handoff: McpToolAccess.actsAsCaller((input) =>
    Effect.gen(function* () {
      const scope = yield* McpInvocationContext.McpInvocationContext;
      const service = yield* WorktreeMcpService.WorktreeMcpService;
      return yield* service.handoff(scope, input);
    }),
  ),
  t3_worktree_status: McpToolAccess.readsAsCaller(() =>
    Effect.gen(function* () {
      const scope = yield* McpInvocationContext.McpInvocationContext;
      const service = yield* WorktreeMcpService.WorktreeMcpService;
      return yield* service.status(scope);
    }),
  ),
} satisfies McpToolAccess.Handlers<typeof WorktreeToolkit.tools>;

export const layer = McpToolAccess.toLayer(WorktreeToolkit, handlers);
