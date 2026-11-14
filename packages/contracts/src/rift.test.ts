import { describe, expect, it } from "vite-plus/test";
import * as Schema from "effect/Schema";
import { OrchestrationV2ThreadLaunchWorkspaceStrategy } from "./orchestrationV2.ts";
import { VcsCreateWorktreeInput, VcsListRefsResult } from "./git.ts";

const decodeStrategy = Schema.decodeUnknownSync(OrchestrationV2ThreadLaunchWorkspaceStrategy);
const encodeStrategy = Schema.encodeSync(OrchestrationV2ThreadLaunchWorkspaceStrategy);
const decodeProvision = Schema.decodeUnknownSync(VcsCreateWorktreeInput);
const decodeRefs = Schema.decodeUnknownSync(VcsListRefsResult);

describe("rift workspace wire contracts", () => {
  it("accepts snapshots without a base ref and carries the CLI workspace kind", () => {
    const strategy = decodeStrategy({
      type: "rift",
      branch: "t3/task",
    });
    expect(encodeStrategy(strategy)).toEqual({
      type: "rift",
      branch: "t3/task",
    });
    const provision = decodeProvision({
      cwd: "/project",
      refName: "HEAD",
      newRefName: strategy.branch,
      path: null,
      workspaceKind: "rift",
    });
    expect(provision.workspaceKind).toBe("rift");
  });
  it("keeps older servers compatible but requires an explicit usable capability to offer rift", () => {
    const base = {
      refs: [],
      isRepo: true,
      hasPrimaryRemote: false,
      nextCursor: null,
      totalCount: 0,
    };
    expect(decodeRefs(base).riftUsable).toBeUndefined();
    expect(decodeRefs({ ...base, riftUsable: true }).riftUsable).toBe(true);
    expect(() => decodeRefs({ ...base, riftUsable: "yes" })).toThrow();
  });
});
