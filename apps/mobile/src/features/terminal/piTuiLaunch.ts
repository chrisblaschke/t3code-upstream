import type { PiTuiLaunch } from "@t3tools/client-runtime/state/pi-tui";

import { uuidv4 } from "../../lib/uuid";
import {
  resolvePreferredThreadWorktreePath,
  type PendingTerminalLaunch,
} from "./terminalLaunchContext";
import { nextOpenTerminalId } from "./terminalMenu";

/**
 * "Open in Pi terminal": a fresh terminal for the thread, in the cwd a new
 * thread terminal would get, that types the Pi TUI command for the thread's
 * session so the phone shows the same Pi session live.
 */
export function buildPiTuiTerminalLaunch(input: {
  readonly piTui: PiTuiLaunch;
  readonly listedTerminalIds: ReadonlyArray<string> | null;
  readonly workspaceRoot: string;
  readonly threadShellWorktreePath: string | null;
  readonly threadDetailWorktreePath: string | null;
}): { readonly terminalId: string; readonly launch: PendingTerminalLaunch } {
  const worktreePath = resolvePreferredThreadWorktreePath({
    threadShellWorktreePath: input.threadShellWorktreePath,
    threadDetailWorktreePath: input.threadDetailWorktreePath,
  });
  return {
    terminalId: nextOpenTerminalId({
      listedTerminalIds: input.listedTerminalIds ?? [],
      ...(input.listedTerminalIds === null ? { uniqueSuffix: uuidv4() } : {}),
    }),
    launch: {
      cwd: worktreePath ?? input.workspaceRoot,
      worktreePath,
      initialInput: `${input.piTui.shellCommand}\r`,
    },
  };
}
