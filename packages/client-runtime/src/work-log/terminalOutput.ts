import type { OrchestrationV2TurnItem } from "@t3tools/contracts";
import { turnItemOutputText } from "./itemDetail.ts";

/**
 * How much of a tool's output the terminal transcript shows before "more",
 * following the Pi terminal: the last lines of a command (where results and
 * errors land), the first lines of anything else.
 */
export type TerminalOutputKind = "command" | "diff" | "text";

const PREVIEW_LINES: Record<TerminalOutputKind, number> = {
  command: 5,
  diff: 16,
  text: 10,
};
/** Clip absurd lines (minified JSON, base64) so one row cannot become huge. */
const MAX_LINE_CHARS = 400;

const FINISHED_STATUSES: ReadonlySet<OrchestrationV2TurnItem["status"]> = new Set([
  "completed",
  "failed",
  "cancelled",
  "interrupted",
]);

/** Which finished items carry output worth showing inline, and how to cut it. */
export function terminalOutputKind(item: OrchestrationV2TurnItem): TerminalOutputKind | null {
  if (!FINISHED_STATUSES.has(item.status)) return null;
  if (item.type === "command_execution") return "command";
  if (item.type === "file_change") return "diff";
  if (item.type === "dynamic_tool") return "text";
  return null;
}

export interface TerminalOutputPreview {
  readonly lines: ReadonlyArray<string>;
  /** Lines left out of the preview; they sit before it for commands, after it otherwise. */
  readonly hiddenLineCount: number;
  /** Characters clipped from the shown lines (not from omitted lines). */
  readonly hiddenCharacterCount: number;
}

function isDiffHeader(line: string): boolean {
  return (
    line.startsWith("diff ") ||
    line.startsWith("index ") ||
    line.startsWith("--- ") ||
    line.startsWith("+++ ") ||
    line.startsWith("===")
  );
}

export function outputLines(text: string, kind: TerminalOutputKind): Array<string> {
  const lines = text.trimEnd().split("\n");
  return kind === "diff" ? lines.filter((line) => !isDiffHeader(line)) : lines;
}

export function terminalOutputPreview(
  text: string,
  kind: TerminalOutputKind,
): TerminalOutputPreview | null {
  const lines = outputLines(text, kind);
  if (lines.length === 0 || (lines.length === 1 && lines[0]!.trim() === "")) return null;
  const limit = PREVIEW_LINES[kind];
  const shown = kind === "command" ? lines.slice(-limit) : lines.slice(0, limit);
  return {
    lines: shown.map((line) =>
      line.length > MAX_LINE_CHARS ? `${line.slice(0, MAX_LINE_CHARS)}…` : line,
    ),
    hiddenLineCount: lines.length - shown.length,
    hiddenCharacterCount: shown.reduce(
      (count, line) => count + Math.max(0, line.length - MAX_LINE_CHARS),
      0,
    ),
  };
}

/** The text the transcript prints under a finished item read with `getTurnItem`. */
export function terminalOutputText(item: OrchestrationV2TurnItem): string | null {
  switch (item.type) {
    case "command_execution":
      return turnItemOutputText(item);
    case "file_change":
      return item.diffStr ?? item.newStr ?? null;
    case "dynamic_tool":
      return turnItemOutputText(item);
    default:
      return null;
  }
}
