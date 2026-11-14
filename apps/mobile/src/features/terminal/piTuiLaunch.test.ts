import { describe, expect, it, vi } from "vite-plus/test";

vi.mock("../../lib/uuid", () => {
  let sequence = 0;
  return { uuidv4: () => `test-uuid-${++sequence}` };
});

import { buildPiTuiTerminalLaunch } from "./piTuiLaunch";

const piTui = {
  argv: ["pi", "--session", "/home/me/.pi/sessions/a b.jsonl"],
  shellCommand: "'pi' '--session' '/home/me/.pi/sessions/a b.jsonl'",
  sessionFile: "/home/me/.pi/sessions/a b.jsonl",
};

describe("buildPiTuiTerminalLaunch", () => {
  it("uses unique terminal IDs when inventory is unavailable or unreadable", () => {
    const input = {
      piTui,
      listedTerminalIds: null,
      workspaceRoot: "/repo",
      threadShellWorktreePath: null,
      threadDetailWorktreePath: null,
    };
    const first = buildPiTuiTerminalLaunch(input);
    const second = buildPiTuiTerminalLaunch(input);
    expect(first.terminalId).not.toBe("term-1");
    expect(first.terminalId).not.toBe(second.terminalId);
    expect(first.launch.initialInput).toBe(`${piTui.shellCommand}\r`);
  });
  it("opens a new terminal in the thread worktree and submits the Pi command", () => {
    expect(
      buildPiTuiTerminalLaunch({
        piTui,
        listedTerminalIds: ["term-1", "term-2"],
        workspaceRoot: "/repo",
        threadShellWorktreePath: "/repo/.worktrees/stale",
        threadDetailWorktreePath: "/repo/.worktrees/feature",
      }),
    ).toEqual({
      terminalId: "term-3",
      launch: {
        cwd: "/repo/.worktrees/feature",
        worktreePath: "/repo/.worktrees/feature",
        initialInput: "'pi' '--session' '/home/me/.pi/sessions/a b.jsonl'\r",
      },
    });
  });

  it("falls back to the project root when the thread has no worktree", () => {
    const { terminalId, launch } = buildPiTuiTerminalLaunch({
      piTui,
      listedTerminalIds: [],
      workspaceRoot: "/repo",
      threadShellWorktreePath: null,
      threadDetailWorktreePath: null,
    });
    expect(terminalId).toBe("term-1");
    expect(launch.cwd).toBe("/repo");
    expect(launch.worktreePath).toBeNull();
  });
});
