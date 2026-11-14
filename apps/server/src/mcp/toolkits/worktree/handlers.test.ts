import * as NodeServices from "@effect/platform-node/NodeServices";
import { expect, it } from "@effect/vitest";
import {
  EnvironmentId,
  ProjectId,
  ThreadId,
  WorktreeMcpListResult,
  type OrchestrationV2ThreadShell,
  type VcsRef,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Stream from "effect/Stream";
import * as Schema from "effect/Schema";
import * as Path from "effect/Path";

import * as Orchestrator from "../../../orchestration-v2/Orchestrator.ts";
import * as ThreadManagement from "../../../orchestration-v2/ThreadManagementService.ts";
import * as Project from "../../../project/ProjectService.ts";
import * as VcsProcess from "../../../vcs/VcsProcess.ts";
import * as McpInvocationContext from "../../McpInvocationContext.ts";
import * as McpToolAccess from "../../McpToolAccess.ts";
import { liveThreadShell } from "../../McpToolAccess.testkit.ts";
import * as WorktreeMcpService from "../../WorktreeMcpService.ts";
import * as Handlers from "./handlers.ts";
import { worktreeStatus } from "./inventory.ts";
import { WorktreeToolkit } from "./tools.ts";

const decodeListResult = Schema.decodeUnknownEffect(WorktreeMcpListResult);
const projectId = ProjectId.make("project:anchor");
const otherProject = ProjectId.make("project:other");
const context: McpInvocationContext.McpInvocationScope = {
  environmentId: EnvironmentId.make("environment:inventory"),
  requestNamespace: "client:inventory",
  thread: undefined,
  client: { sessionId: "inventory", label: "inventory", access: "approval-required" },
  capabilities: new Set(["worktree", "orchestration"]),
  issuedAt: 0,
};
const layerPlatform = Layer.mergeAll(
  NodeServices.layer,
  VcsProcess.layer.pipe(Layer.provide(NodeServices.layer)),
);

const fixture = Effect.gen(function* () {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const root = yield* fs.makeTempDirectoryScoped({ prefix: "t3-inventory-" });
  const main = path.join(root, "main");
  const linked = path.join(root, "linked");
  yield* fs.makeDirectory(main);
  const process = yield* VcsProcess.VcsProcess;
  const git = (cwd: string, args: readonly string[]) =>
    process.run({
      operation: "test.inventory.setup",
      command: "git",
      cwd,
      args: ["-c", "user.name=Inventory Test", "-c", "user.email=inventory@example.test", ...args],
      timeoutMs: 10_000,
    });
  yield* git(main, ["init", "-b", "main"]);
  yield* fs.writeFileString(path.join(main, "tracked"), "base\n");
  yield* git(main, ["add", "."]);
  yield* git(main, ["commit", "-m", "Private base subject"]);
  yield* git(main, ["worktree", "add", "-b", "feature", linked]);
  yield* fs.writeFileString(path.join(linked, "tracked"), "base\nfeature\n");
  yield* git(linked, ["commit", "-am", "Private unshipped subject"]);
  yield* fs.writeFileString(path.join(linked, "tracked"), "dirty\n");
  yield* fs.writeFileString(path.join(linked, "private-untracked-name"), "new\n");
  const indexPath = (yield* git(linked, ["rev-parse", "--git-path", "index"])).stdout.trim();
  const refs: VcsRef[] = [
    // Neither current nor default identifies the primary checkout.
    { name: "main", current: false, isDefault: false, worktreePath: main },
    { name: "feature", current: true, isDefault: true, worktreePath: linked },
    { name: "unused", current: false, isDefault: false, worktreePath: null },
  ];
  const owners: OrchestrationV2ThreadShell[] = [
    { ...liveThreadShell(ThreadId.make("thread:owner-a")), projectId, worktreePath: linked + "/" },
    {
      ...liveThreadShell(ThreadId.make("thread:owner-b")),
      projectId: otherProject,
      worktreePath: linked,
      settledOverride: "settled",
      settledAt: DateTime.makeUnsafe("2026-11-01T00:00:00Z"),
      archivedAt: DateTime.makeUnsafe("2026-11-02T00:00:00Z"),
    },
    {
      ...liveThreadShell(ThreadId.make("thread:deleted")),
      projectId: otherProject,
      worktreePath: linked,
      deletedAt: DateTime.makeUnsafe("2026-11-02T00:00:00Z"),
    },
  ];
  const calls: unknown[] = [];
  const dependencies = Layer.mergeAll(
    Layer.succeed(McpInvocationContext.McpInvocationContext, context),
    Layer.mock(ThreadManagement.ThreadManagementService)({
      getThreadShell: () =>
        Effect.succeed({
          ...liveThreadShell(ThreadId.make("thread:removed")),
          projectId,
          worktreePath: path.join(root, "removed"),
        }),
      getProjectThreadRecords: () =>
        Effect.succeed({
          thread: {
            ...liveThreadShell(ThreadId.make("thread:removed")),
            projectId,
            worktreePath: path.join(root, "removed"),
          },
        } as never),
    }),
    Layer.mock(Project.ProjectService)({
      getById: (id) => {
        expect(id).toBe(projectId);
        return Effect.succeedSome({ id: projectId, workspaceRoot: main } as never);
      },
    }),
    Layer.mock(WorktreeMcpService.WorktreeMcpService)({
      listRefs: (input) => {
        calls.push(input);
        return Effect.succeed({
          refs,
          totalCount: refs.length,
          nextCursor: null,
          isRepo: true,
          hasPrimaryRemote: false,
        });
      },
    }),
    Layer.mock(Orchestrator.OrchestratorV2)({
      getShellSnapshot: (options) => {
        expect(options?.projectId).toBeUndefined();
        expect(options?.location).toBeUndefined();
        return Effect.succeed({ threads: owners } as never);
      },
    }),
  );
  const list = (
    flags: { includeStatus?: boolean; includeOwners?: boolean; threadId?: ThreadId } = {},
  ) =>
    Effect.gen(function* () {
      const tools = yield* WorktreeToolkit.pipe(
        Effect.provide(
          McpToolAccess.HandlersLayer.layer(Handlers.layer).pipe(Layer.provide(dependencies)),
        ),
      );
      const results = yield* tools
        .handle("t3_worktree_list", {
          ...(flags.threadId === undefined ? { projectId } : {}),
          ...flags,
        })
        .pipe(Stream.unwrap, Stream.runCollect, Effect.provide(dependencies));
      const result = results.at(-1)!;
      if (result.isFailure) throw new Error(JSON.stringify(result.result));
      return yield* decodeListResult(result.result);
    });
  return { fs, path, main, linked, root, git, indexPath, refs, list, calls };
});

it.effect("keeps inventory available after the thread worktree was removed", () =>
  Effect.gen(function* () {
    const f = yield* fixture;
    const result = yield* f.list({
      threadId: ThreadId.make("thread:removed"),
      includeStatus: true,
    });
    expect(result.refs[0]?.isMainWorktree).toBe(true);
    expect(result.refs[1]?.status).toMatchObject({ exists: true });
  }).pipe(Effect.scoped, Effect.provide(layerPlatform)),
);

it.effect(
  "anchors on project root without a caller thread and marks only Git's primary checkout",
  () =>
    Effect.gen(function* () {
      const f = yield* fixture;
      const result = yield* f.list();
      expect(f.calls).toEqual([{ workspaceRoot: f.main, worktreePath: null }]);
      expect(result.refs.map((ref) => ref.isMainWorktree)).toEqual([true, false, undefined]);
      expect(result.refs[2]).toEqual(f.refs[2]);
      for (const ref of result.refs) {
        expect(ref).not.toHaveProperty("status");
        expect(ref).not.toHaveProperty("owners");
      }
    }).pipe(Effect.scoped, Effect.provide(layerPlatform)),
);

it.effect(
  "counts dirty/unshipped linked work and finds owners across projects without changing the index",
  () =>
    Effect.gen(function* () {
      const f = yield* fixture;
      const before = yield* f.fs.readFile(f.indexPath);
      const mainIndex = f.path.join(f.main, ".git", "index");
      const mainBefore = yield* f.fs.readFile(mainIndex);
      const process = yield* VcsProcess.VcsProcess;
      const commands: VcsProcess.VcsProcessInput[] = [];
      const result = yield* f.list({ includeStatus: true, includeOwners: true }).pipe(
        Effect.provideService(VcsProcess.VcsProcess, {
          run: (input) => {
            commands.push(input);
            return process.run(input);
          },
        }),
      );
      expect(commands.length).toBeGreaterThan(1);
      for (const command of commands) {
        expect(command.env).toMatchObject({ GIT_OPTIONAL_LOCKS: "0", GIT_NO_LAZY_FETCH: "1" });
        expect(command.args.slice(0, 2)).toEqual(["-c", "core.fsmonitor=false"]);
        expect(command.timeoutMs).toBeGreaterThan(0);
        expect(command.args).not.toContain("fetch");
      }
      expect(yield* f.fs.readFile(f.indexPath)).toEqual(before);
      expect(yield* f.fs.readFile(mainIndex)).toEqual(mainBefore);
      expect(result.refs[1]?.status).toEqual({
        exists: true,
        dirty: 2,
        upstream: false,
        ahead: null,
        behind: null,
        defaultBranch: "main",
        notInDefault: 1,
        onlyHere: 1,
        lastCommitAt: expect.stringMatching(/^\d{4}-\d\d-\d\dT/),
      });
      expect(result.refs[0]?.status).toMatchObject({ dirty: 0, onlyHere: 0, notInDefault: 0 });
      expect(result.refs[1]?.owners).toEqual([
        { threadId: "thread:owner-a", projectId, settled: false, archived: false },
        { threadId: "thread:owner-b", projectId: otherProject, settled: true, archived: true },
      ]);
      expect(result.refs[0]?.owners).toEqual([]);
      expect(result.refs[2]).not.toHaveProperty("status");
      const serialized = JSON.stringify(result);
      expect(serialized).not.toContain("Private unshipped subject");
      expect(serialized).not.toContain("private-untracked-name");
      yield* f.git(f.linked, ["branch", "also-here"]);
      expect(yield* worktreeStatus(f.linked)).toMatchObject({ onlyHere: 0, notInDefault: 1 });
    }).pipe(Effect.scoped, Effect.provide(layerPlatform)),
);

it.effect(
  "uses cached origin/HEAD and upstream counts, and reports missing and invalid worktrees separately",
  () =>
    Effect.gen(function* () {
      const f = yield* fixture;
      yield* f.fs.writeFileString(f.path.join(f.main, "tracked"), "main advanced\n");
      yield* f.git(f.main, ["commit", "-am", "private main subject"]);
      yield* f.git(f.main, ["remote", "add", "origin", "https://invalid.example/no-network.git"]);
      yield* f.git(f.main, ["update-ref", "refs/remotes/origin/main", "main"]);
      yield* f.git(f.main, [
        "symbolic-ref",
        "refs/remotes/origin/HEAD",
        "refs/remotes/origin/main",
      ]);
      yield* f.git(f.linked, ["branch", "--set-upstream-to=origin/main"]);
      expect(yield* worktreeStatus(f.linked)).toMatchObject({
        upstream: true,
        ahead: 1,
        behind: 1,
        defaultBranch: "origin/main",
        notInDefault: 1,
        onlyHere: 1,
      });
      expect(yield* worktreeStatus(f.path.join(f.root, "missing"))).toMatchObject({
        exists: false,
      });
      expect(yield* worktreeStatus(f.root)).toEqual({ error: expect.any(String) });
      f.refs.push({ name: "broken", current: false, isDefault: false, worktreePath: f.root });
      const listed = yield* f.list({ includeStatus: true });
      expect(listed.refs[0]?.status).toMatchObject({ exists: true });
      expect(listed.refs[3]?.status).toEqual({ error: expect.any(String) });
    }).pipe(Effect.scoped, Effect.provide(layerPlatform)),
);
