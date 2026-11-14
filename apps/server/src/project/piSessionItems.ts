/**
 * Read tool calls and results from the active JSONL branch so followed CLI
 * threads show tool activity, not just the transcript scanner's message text.
 */

import * as DateTime from "effect/DateTime";
import * as Option from "effect/Option";

import { piResultImage, type PiResultImage } from "@t3tools/provider-pi/server/session";

/** Keep followed-thread projections bounded; the session file has the full text. */
const MAX_OUTPUT_CHARS = 20_000;

export interface RawEntry {
  readonly type?: unknown;
  readonly id?: unknown;
  readonly parentId?: unknown;
  readonly timestamp?: unknown;
  readonly customType?: unknown;
  readonly data?: unknown;
  readonly display?: unknown;
  readonly content?: unknown;
  readonly message?: {
    readonly role?: unknown;
    readonly content?: unknown;
    readonly toolCallId?: unknown;
    readonly isError?: unknown;
    readonly details?: unknown;
    readonly timestamp?: unknown;
    readonly stopReason?: unknown;
  };
}

/**
 * Whether the session's newest message ends an agent turn: an assistant reply
 * that did not stop to call tools. Reads backwards from the end of the file.
 */
export function piTurnEnded(contents: string | ReadonlyArray<RawEntry>): boolean {
  if (typeof contents !== "string") {
    for (let index = contents.length - 1; index >= 0; index--) {
      const entry = contents[index]!;
      if (entry.type !== "message") continue;
      return entry.message?.role === "assistant" && entry.message.stopReason !== "toolUse";
    }
    return false;
  }
  let end = contents.length;
  while (end > 0) {
    const start = contents.lastIndexOf("\n", end - 1) + 1;
    const line = contents.slice(start, end).trim();
    end = start - 1;
    if (line.length === 0) continue;
    let entry: RawEntry;
    try {
      entry = JSON.parse(line) as RawEntry;
    } catch {
      continue;
    }
    if (entry.type !== "message") continue;
    return entry.message?.role === "assistant" && entry.message.stopReason !== "toolUse";
  }
  return false;
}

export interface PiToolActivity {
  /** Pi's tool call id; stable across syncs. */
  readonly toolCallId: string;
  /** Session entry that holds the call (the assistant message). */
  readonly entryId: string;
  /** Position of that entry on the active branch; orders items within a run. */
  readonly branchIndex: number;
  /** Position of the call among the message's tool calls. */
  readonly callIndex: number;
  /** Position of the call among all of the message's content blocks. */
  readonly blockIndex: number;
  readonly name: string;
  readonly args: Record<string, unknown>;
  readonly startedAt: string;
  readonly result: {
    readonly text: string;
    readonly isError: boolean;
    /** The image the Pi terminal draws for this result, if any. */
    readonly image?: PiResultImage;
    /** Tool-specific result details, e.g. an edit's `patch`. */
    readonly details?: unknown;
    readonly completedAt: string;
    /** Whether the result entry is after the head, i.e. new in this sync. */
    readonly isNew: boolean;
  } | null;
  /** An unfinished call whose turn ended must no longer appear running. */
  readonly endedAt?: string;
  /** Whether the call entry is after the head. */
  readonly isNew: boolean;
}

/**
 * Custom entry type for a notice an extension wants followers to see. Pi
 * doesn't persist `ctx.ui.notify`, so a TUI extension appends
 * `pi.appendEntry("ui-notice", { message, level })` (kept out of model context).
 */
const PI_NOTICE_ENTRY_TYPE = "ui-notice";

export interface PiNotice {
  readonly entryId: string;
  readonly branchIndex: number;
  readonly message: string;
  readonly level: "info" | "warning" | "error";
  readonly at: string;
}

/** A thinking block of an assistant message, shown the way live Pi reasoning is. */
export interface PiThought {
  readonly entryId: string;
  readonly branchIndex: number;
  readonly blockIndex: number;
  readonly text: string;
  /** When the assistant message began streaming. */
  readonly startedAt: string;
  /** When the message was written, i.e. finished. */
  readonly completedAt: string;
}

/**
 * One agent turn on the active branch: a user prompt, or a displayed extension
 * message (a delegate "Helper update") that woke an idle agent, through the
 * last message the agent wrote before the next turn.
 */
export interface PiAgentTurn {
  /** The entry that started the turn. */
  readonly entryId: string;
  readonly kind: "prompt" | "wake";
  readonly startedAt: string;
  /** The turn's last assistant message or tool result; its start before either. */
  readonly endedAt: string;
  /** Wake only: the text of the message that woke the agent. */
  readonly text: string;
}

function asRecord(value: unknown): Record<string, unknown> | undefined {
  return typeof value === "object" && value !== null && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : undefined;
}

function isoTime(value: unknown, fallback: string): string {
  if (typeof value !== "number" && typeof value !== "string") return fallback;
  if (typeof value === "number" && !Number.isFinite(value)) return fallback;
  return Option.match(DateTime.make(value), {
    onNone: () => fallback,
    onSome: DateTime.formatIso,
  });
}

function resultText(content: unknown): string {
  if (typeof content === "string") return content;
  if (!Array.isArray(content)) return "";
  const text = content
    .flatMap((block) => {
      const record = asRecord(block);
      if (record?.type === "text" && typeof record.text === "string") return [record.text];
      if (record?.type === "image") return ["[image]"];
      return [];
    })
    .join("\n");
  return text.length > MAX_OUTPUT_CHARS
    ? `${text.slice(0, MAX_OUTPUT_CHARS)}\n...[${text.length - MAX_OUTPUT_CHARS} more chars]`
    : text;
}

function withImage(content: unknown): { readonly image?: PiResultImage } {
  const image = piResultImage(content);
  return image === undefined ? {} : { image };
}

/** Parse once at the file boundary; tolerate malformed/in-progress records. */
export function parsePiSessionEntries(contents: string): Array<RawEntry> {
  const entries: Array<RawEntry> = [];
  for (const line of contents.split("\n")) {
    if (line.trim().length === 0) continue;
    let entry: RawEntry;
    try {
      entry = JSON.parse(line) as RawEntry;
    } catch {
      continue;
    }
    if (typeof entry === "object" && entry !== null) entries.push(entry);
  }
  return entries;
}

/** Entries on the path from the newest entry back to the root, oldest first. */
function activeBranch(
  contents: string | ReadonlyArray<RawEntry>,
): ReadonlyArray<RawEntry & { readonly id: string }> {
  const byId = new Map<string, RawEntry & { readonly id: string }>();
  let leaf: (RawEntry & { readonly id: string }) | undefined;
  for (const entry of typeof contents === "string" ? parsePiSessionEntries(contents) : contents) {
    if (entry.type === "session" || typeof entry.id !== "string") continue;
    const withId = entry as RawEntry & { readonly id: string };
    byId.set(withId.id, withId);
    leaf = withId;
  }
  const branch: Array<RawEntry & { readonly id: string }> = [];
  const seen = new Set<string>();
  for (let entry = leaf; entry !== undefined && !seen.has(entry.id);) {
    seen.add(entry.id);
    branch.push(entry);
    entry = typeof entry.parentId === "string" ? byId.get(entry.parentId) : undefined;
  }
  return branch.reverse();
}

/**
 * Tool calls to (re)publish after `headId`: calls made after the head, and
 * earlier calls whose result arrived after it. Null when the head is no
 * longer on the active branch. `branchIndexById` positions messages too.
 */
export function piToolActivityAfterHead(
  contents: string | ReadonlyArray<RawEntry>,
  headId: string | null,
  fallbackTimestamp: string,
  /** Calls to return however old they are (items a thread still shows as running). */
  include: ReadonlySet<string> = new Set(),
): {
  readonly tools: ReadonlyArray<PiToolActivity>;
  readonly thoughts: ReadonlyArray<PiThought>;
  readonly notices: ReadonlyArray<PiNotice>;
  readonly branchIndexById: ReadonlyMap<string, number>;
  /** Content position of each new assistant message's first text block. */
  readonly textBlockIndexById: ReadonlyMap<string, number>;
  readonly turns: ReadonlyMap<string, PiAgentTurn>;
  readonly turnIdByEntryId: ReadonlyMap<string, string>;
} | null {
  const branch = activeBranch(contents);
  const headIndex = headId === null ? -1 : branch.findIndex((entry) => entry.id === headId);
  if (headId !== null && headIndex === -1) return null;

  const branchIndexById = new Map(branch.map((entry, index) => [entry.id, index]));
  const results = new Map<string, { entry: RawEntry; index: number }>();
  branch.forEach((entry, index) => {
    const message = entry.message;
    if (entry.type === "message" && message?.role === "toolResult") {
      if (typeof message.toolCallId === "string") results.set(message.toolCallId, { entry, index });
    }
  });

  const { turns, turnIdByEntryId } = agentTurnsOnBranch(branch, fallbackTimestamp);
  const closedTurns = new Set<string>();
  // A final assistant response also closes its turn without another prompt.
  for (const entry of branch) {
    const turnId = turnIdByEntryId.get(entry.id);
    if (turnId === undefined || entry.message?.role !== "assistant") continue;
    if (entry.message.stopReason === "toolUse") closedTurns.delete(turnId);
    else if (typeof entry.message.stopReason === "string") closedTurns.add(turnId);
  }
  // A newer prompt abandons any incomplete tool loop in the preceding turn.
  for (const turnId of [...turns.keys()].slice(0, -1)) closedTurns.add(turnId);

  const tools: Array<PiToolActivity> = [];
  const thoughts: Array<PiThought> = [];
  const textBlockIndexById = new Map<string, number>();
  branch.forEach((entry, index) => {
    const message = entry.message;
    if (entry.type !== "message" || message?.role !== "assistant") return;
    if (!Array.isArray(message.content)) return;
    let callIndex = 0;
    for (const [blockIndex, block] of message.content.entries()) {
      const call = asRecord(block);
      if (index > headIndex && call?.type === "text" && !textBlockIndexById.has(entry.id)) {
        textBlockIndexById.set(entry.id, blockIndex);
      }
      if (index > headIndex && call?.type === "thinking" && typeof call.thinking === "string") {
        const text = call.thinking.trim();
        if (text.length > 0) {
          const completedAt = isoTime(entry.timestamp ?? message.timestamp, fallbackTimestamp);
          thoughts.push({
            entryId: entry.id,
            branchIndex: index,
            blockIndex,
            text:
              text.length > MAX_OUTPUT_CHARS
                ? `${text.slice(0, MAX_OUTPUT_CHARS)}\n...[${text.length - MAX_OUTPUT_CHARS} more chars]`
                : text,
            startedAt: isoTime(message.timestamp, completedAt),
            completedAt,
          });
        }
      }
      if (call?.type !== "toolCall" || typeof call.id !== "string") continue;
      const found = results.get(call.id);
      const isNew = index > headIndex;
      const resultIsNew = found !== undefined && found.index > headIndex;
      const turnId = turnIdByEntryId.get(entry.id);
      const endedAt =
        found === undefined && turnId !== undefined && closedTurns.has(turnId)
          ? turns.get(turnId)?.endedAt
          : undefined;
      // Missing results are not final: reconcile them after their turn closes.
      if (!isNew && !resultIsNew && endedAt === undefined && !include.has(call.id)) {
        callIndex += 1;
        continue;
      }
      const startedAt = isoTime(entry.timestamp ?? message.timestamp, fallbackTimestamp);
      tools.push({
        toolCallId: call.id,
        entryId: entry.id,
        branchIndex: index,
        callIndex,
        blockIndex,
        name: typeof call.name === "string" && call.name.trim().length > 0 ? call.name : "tool",
        args: asRecord(call.arguments) ?? {},
        startedAt,
        isNew,
        ...(endedAt === undefined ? {} : { endedAt }),
        result:
          found === undefined
            ? null
            : {
                text: resultText(found.entry.message?.content),
                isError: found.entry.message?.isError === true,
                ...withImage(found.entry.message?.content),
                details: found.entry.message?.details,
                completedAt: isoTime(
                  found.entry.timestamp ?? found.entry.message?.timestamp,
                  startedAt,
                ),
                isNew: resultIsNew,
              },
      });
      callIndex += 1;
    }
  });
  const notices = branch.flatMap((entry, index): ReadonlyArray<PiNotice> => {
    if (index <= headIndex || entry.type !== "custom") return [];
    if (entry.customType !== PI_NOTICE_ENTRY_TYPE) return [];
    const data = asRecord(entry.data);
    const message = typeof data?.message === "string" ? data.message.trim() : "";
    if (message.length === 0) return [];
    const level = data?.level === "warning" || data?.level === "error" ? data.level : "info";
    const at = isoTime(entry.timestamp, fallbackTimestamp);
    return [{ entryId: entry.id, branchIndex: index, message, level, at }];
  });
  return { tools, thoughts, notices, branchIndexById, textBlockIndexById, turns, turnIdByEntryId };
}

/**
 * The last active-branch entry written at or before `cutoff`, or null when
 * none was. Resumes a followed file whose recorded head left the branch (a
 * retry or tree navigation abandoned it): everything up to the follower's
 * last update is already in the thread, as a sync or as the follower's own turn.
 */
export function piBranchEntryAtOrBefore(
  contents: string | ReadonlyArray<RawEntry>,
  cutoff: string,
): string | null {
  const cutoffMs = Date.parse(cutoff);
  let found: string | null = null;
  for (const entry of activeBranch(contents)) {
    const at = typeof entry.timestamp === "string" ? Date.parse(entry.timestamp) : Number.NaN;
    if (Number.isFinite(at) && at > cutoffMs) break;
    found = entry.id;
  }
  return found;
}

function customMessageText(content: unknown): string {
  if (typeof content === "string") return content.trim();
  return resultText(content).trim();
}

/**
 * Map active-branch entries to turns ending at the last agent message, not the
 * next prompt. Extension messages start a turn only when they wake an idle
 * agent that answers; messages arriving during work stay in the current turn.
 */
export function piAgentTurns(
  contents: string | ReadonlyArray<RawEntry>,
  fallbackTimestamp: string,
): {
  readonly turns: ReadonlyMap<string, PiAgentTurn>;
  readonly turnIdByEntryId: ReadonlyMap<string, string>;
} {
  return agentTurnsOnBranch(activeBranch(contents), fallbackTimestamp);
}

function agentTurnsOnBranch(
  branch: ReadonlyArray<RawEntry & { readonly id: string }>,
  fallbackTimestamp: string,
) {
  const role = (entry: RawEntry) => (entry.type === "message" ? entry.message?.role : undefined);
  // Whether the agent answers before anyone else speaks, per branch position.
  const answeredAt: Array<boolean> = [];
  let answered = false;
  for (let index = branch.length - 1; index >= 0; index--) {
    answeredAt[index] = answered;
    const entryRole = role(branch[index]!);
    if (entryRole === "assistant") answered = true;
    else if (entryRole !== undefined) answered = false;
  }

  const turns = new Map<string, PiAgentTurn>();
  const turnIdByEntryId = new Map<string, string>();
  let current: { -readonly [K in keyof PiAgentTurn]: PiAgentTurn[K] } | null = null;
  // The last message or displayed extension message, whoever wrote it.
  let previous: RawEntry | null = null;
  branch.forEach((entry, index) => {
    const entryRole = role(entry);
    const displayed = entry.type === "custom_message" && entry.display === true;
    const at = () => isoTime(entry.timestamp ?? entry.message?.timestamp, fallbackTimestamp);
    const agentWasIdle =
      previous !== null &&
      role(previous) === "assistant" &&
      previous.message?.stopReason !== "toolUse";
    if (entryRole === "user" || (displayed && agentWasIdle && answeredAt[index] === true)) {
      const startedAt = at();
      current = {
        entryId: entry.id,
        kind: entryRole === "user" ? "prompt" : "wake",
        startedAt,
        endedAt: startedAt,
        text: entryRole === "user" ? "" : customMessageText(entry.content),
      };
      turns.set(entry.id, current);
    } else if (current !== null && (entryRole === "assistant" || entryRole === "toolResult")) {
      const endedAt = at();
      if (endedAt > current.endedAt) current.endedAt = endedAt;
    }
    if (current !== null) turnIdByEntryId.set(entry.id, current.entryId);
    if (entryRole !== undefined || displayed) previous = entry;
  });
  return { turns, turnIdByEntryId };
}
