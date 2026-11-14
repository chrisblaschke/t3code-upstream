import type { WorktreeMcpInventoryStatus } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";

import * as VcsProcess from "../../../vcs/VcsProcess.ts";

/** All inventory commands are local, bounded reads; never refresh Git's index. */
const gitRead = Effect.fn("WorktreeInventory.gitRead")(function* (
  cwd: string,
  args: ReadonlyArray<string>,
  options?: { readonly allowNonZeroExit?: boolean; readonly stdin?: string },
) {
  const process = yield* VcsProcess.VcsProcess;
  return yield* process.run({
    operation: "WorktreeInventory.read",
    command: "git",
    args: ["-c", "core.fsmonitor=false", ...args],
    cwd,
    env: { GIT_OPTIONAL_LOCKS: "0", GIT_NO_LAZY_FETCH: "1" },
    timeoutMs: 10_000,
    maxOutputBytes: 10_000_000,
    outputMode: "error",
    ...options,
  });
});

export const primaryWorktree = (cwd: string) =>
  gitRead(cwd, ["worktree", "list", "--porcelain", "-z"]).pipe(
    Effect.map((result) => result.stdout.split("\0")[0]?.replace(/^worktree /, "") ?? null),
  );

export const worktreeStatus = Effect.fn("WorktreeInventory.status")(function* (
  cwd: string,
): Effect.fn.Return<
  WorktreeMcpInventoryStatus,
  never,
  FileSystem.FileSystem | VcsProcess.VcsProcess
> {
  return yield* Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const empty = {
      exists: false,
      dirty: 0,
      upstream: false,
      ahead: null,
      behind: null,
      defaultBranch: null,
      notInDefault: null,
      onlyHere: 0,
      lastCommitAt: null,
    } as const;
    if (!(yield* fs.exists(cwd))) return empty;
    const dirty = (yield* gitRead(cwd, ["status", "--porcelain"])).stdout
      .split("\n")
      .filter((line) => line.length > 0).length;
    const head = yield* gitRead(cwd, ["rev-parse", "--verify", "--quiet", "HEAD"], {
      allowNonZeroExit: true,
    });
    if (head.exitCode !== 0 && head.exitCode !== 1) return yield* Effect.fail("HEAD unavailable");
    const branch = yield* gitRead(cwd, ["symbolic-ref", "--quiet", "HEAD"], {
      allowNonZeroExit: true,
    });
    if (branch.exitCode !== 0 && branch.exitCode !== 1)
      return yield* Effect.fail("Branch unavailable");
    const refs = (yield* gitRead(cwd, [
      "for-each-ref",
      "--format=%(refname)%00%(upstream)",
      "refs/heads",
      "refs/remotes",
    ])).stdout
      .split("\n")
      .filter(Boolean)
      .map((line) => line.split("\0"));
    const upstreamRef = refs.find(([ref]) => ref === branch.stdout.trim())?.[1];
    const originHead = yield* gitRead(
      cwd,
      ["symbolic-ref", "--quiet", "refs/remotes/origin/HEAD"],
      { allowNonZeroExit: true },
    );
    if (originHead.exitCode !== 0 && originHead.exitCode !== 1)
      return yield* Effect.fail("Default branch unavailable");
    const defaultRef =
      originHead.stdout.trim() ||
      ["refs/heads/main", "refs/heads/master"].find((candidate) =>
        refs.some(([ref]) => ref === candidate),
      ) ||
      null;
    const count = (args: ReadonlyArray<string>, stdin?: string) =>
      gitRead(
        cwd,
        ["rev-list", "--count", ...args],
        stdin === undefined ? undefined : { stdin },
      ).pipe(Effect.map((result) => Number(result.stdout.trim())));
    const hasHead = head.exitCode === 0;
    let ahead: number | null = null;
    let behind: number | null = null;
    if (upstreamRef && hasHead) {
      ahead = yield* count(["HEAD", "--not", upstreamRef]);
      behind = yield* count([upstreamRef, "--not", "HEAD"]);
    }
    const otherRefs = refs.map(([ref]) => ref!).filter((ref) => ref !== branch.stdout.trim());
    return {
      exists: true,
      dirty,
      upstream: Boolean(upstreamRef),
      ahead,
      behind,
      defaultBranch: defaultRef?.replace(/^refs\/(?:heads|remotes)\//, "") ?? null,
      notInDefault: defaultRef && hasHead ? yield* count(["HEAD", "--not", defaultRef]) : null,
      onlyHere: hasHead
        ? yield* count(["HEAD", "--stdin"], otherRefs.map((ref) => `^${ref}`).join("\n") + "\n")
        : 0,
      lastCommitAt: hasHead
        ? (yield* gitRead(cwd, ["show", "-s", "--format=%cI", "HEAD"])).stdout.trim()
        : null,
    };
  }).pipe(
    // Never return stderr: it can contain filenames, config values or subjects.
    Effect.orElseSucceed(() => ({
      error:
        "Unable to read worktree Git status (command failed, timed out, or exceeded output limit).",
    })),
  );
});
