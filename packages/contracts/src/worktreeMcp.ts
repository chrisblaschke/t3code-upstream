import * as Schema from "effect/Schema";

import { NonNegativeInt, ProjectId, ThreadId, TrimmedNonEmptyString } from "./baseSchemas.ts";
import { VcsListRefsResult, VcsRef } from "./git.ts";

export const WorktreeMcpInventoryStatus = Schema.Union([
  Schema.Struct({
    exists: Schema.Boolean,
    dirty: NonNegativeInt,
    upstream: Schema.Boolean,
    /** Divergence is unknown without an upstream (or a HEAD commit). */
    ahead: Schema.NullOr(NonNegativeInt),
    behind: Schema.NullOr(NonNegativeInt),
    /** origin/HEAD's target, else a local main or master; null if none exists. */
    defaultBranch: Schema.NullOr(Schema.String),
    notInDefault: Schema.NullOr(NonNegativeInt),
    /** HEAD commits unreachable from any other local branch or remote ref. */
    onlyHere: NonNegativeInt,
    lastCommitAt: Schema.NullOr(Schema.String),
  }),
  Schema.Struct({ error: Schema.String }),
]);
export type WorktreeMcpInventoryStatus = typeof WorktreeMcpInventoryStatus.Type;

export const WorktreeMcpListResult = Schema.Struct({
  ...VcsListRefsResult.fields,
  refs: Schema.Array(
    Schema.Struct({
      ...VcsRef.fields,
      isMainWorktree: Schema.optional(Schema.Boolean),
      status: Schema.optional(WorktreeMcpInventoryStatus),
      owners: Schema.optional(
        Schema.Array(
          Schema.Struct({
            threadId: ThreadId,
            projectId: ProjectId,
            settled: Schema.Boolean,
            archived: Schema.Boolean,
          }),
        ),
      ),
    }),
  ),
});

export type WorktreeMcpListResult = typeof WorktreeMcpListResult.Type;

/** Uses the managed worktree folder; handoff detaches the provider, ending this turn and resuming there next run. */
export const WorktreeMcpHandoffInput = Schema.Struct({
  branch: TrimmedNonEmptyString,
  baseRef: Schema.optional(
    TrimmedNonEmptyString.annotate({
      description: "Default: checked-out branch.",
    }),
  ),
  startFromOrigin: Schema.optional(
    Schema.Boolean.annotate({
      description:
        "True: fetch, start from the remote ref; check `startedFromOrigin`. Default: server setting.",
    }),
  ),
  runSetupScript: Schema.optional(
    Schema.Boolean.annotate({
      description: "Default true.",
    }),
  ),
  continuationPrompt: Schema.optional(
    TrimmedNonEmptyString.check(Schema.isMaxLength(120_000)).annotate({
      description: "Work to resume in the worktree next turn. Omitted: thread waits.",
    }),
  ),
});
export type WorktreeMcpHandoffInput = typeof WorktreeMcpHandoffInput.Type;

export const WorktreeMcpSetupScriptStatus = Schema.Union([
  Schema.Struct({
    status: Schema.Literal("started"),
    scriptName: TrimmedNonEmptyString,
    terminalId: TrimmedNonEmptyString,
  }),
  Schema.Struct({
    status: Schema.Literal("no-script"),
  }),
  Schema.Struct({
    status: Schema.Literal("skipped"),
  }),
  Schema.Struct({
    status: Schema.Literal("failed"),
    detail: Schema.String,
  }),
]);
export type WorktreeMcpSetupScriptStatus = typeof WorktreeMcpSetupScriptStatus.Type;

export const WorktreeMcpContinuationStatus = Schema.Union([
  Schema.Struct({
    status: Schema.Literal("scheduled"),
    delivery: Schema.Literals(["started", "queued", "steered", "restarted"]),
  }),
  Schema.Struct({
    status: Schema.Literal("skipped"),
  }),
  Schema.Struct({
    status: Schema.Literal("failed"),
    detail: Schema.String,
  }),
]);
export type WorktreeMcpContinuationStatus = typeof WorktreeMcpContinuationStatus.Type;

export const WorktreeMcpHandoffResult = Schema.Struct({
  worktreePath: TrimmedNonEmptyString,
  branch: TrimmedNonEmptyString,
  baseRef: TrimmedNonEmptyString,
  startedFromOrigin: Schema.Boolean,
  setupScript: WorktreeMcpSetupScriptStatus,
  continuation: WorktreeMcpContinuationStatus,
  note: Schema.String,
});
export type WorktreeMcpHandoffResult = typeof WorktreeMcpHandoffResult.Type;

export const WorktreeMcpStatusResult = Schema.Struct({
  attached: Schema.Boolean,
  worktreePath: Schema.NullOr(TrimmedNonEmptyString),
  branch: Schema.NullOr(TrimmedNonEmptyString),
  projectWorkspaceRoot: TrimmedNonEmptyString,
  defaultStartFromOrigin: Schema.Boolean.annotate({
    description: "`startFromOrigin` default for `t3_worktree_handoff`.",
  }),
});
export type WorktreeMcpStatusResult = typeof WorktreeMcpStatusResult.Type;

export class WorktreeMcpFailure extends Schema.TaggedError<WorktreeMcpFailure>()(
  "WorktreeMcpFailure",
  {
    code: Schema.Literals([
      "capability_denied",
      "thread_not_found",
      "project_not_found",
      "already_in_worktree",
      "handoff_in_progress",
      "invalid_request",
      "operation_failed",
      "thread_credential_required",
    ]),
    message: Schema.String,
  },
) {}
