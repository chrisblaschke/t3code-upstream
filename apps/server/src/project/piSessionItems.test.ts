import { describe, expect, it } from "@effect/vitest";

import {
  piAgentTurns,
  piBranchEntryAtOrBefore,
  piToolActivityAfterHead,
  piTurnEnded,
} from "./piSessionItems.ts";

const FALLBACK = "2026-09-24T00:00:00.000Z";

const lines = (...records: ReadonlyArray<object>) =>
  records.map((record) => JSON.stringify(record)).join("\n");

const session = lines(
  { type: "session", version: 3, id: "s", cwd: "/project" },
  { type: "message", id: "u1", parentId: null, message: { role: "user", content: "brief" } },
  {
    type: "message",
    id: "a1",
    parentId: "u1",
    timestamp: "2026-09-24T10:00:00.000Z",
    message: {
      role: "assistant",
      content: [
        { type: "text", text: "Looking." },
        { type: "toolCall", id: "call-rg", name: "bash", arguments: { command: "rg foo" } },
        { type: "toolCall", id: "call-read", name: "read", arguments: { path: "a.ts" } },
      ],
    },
  },
  {
    type: "message",
    id: "r1",
    parentId: "a1",
    timestamp: "2026-09-24T10:00:02.000Z",
    message: {
      role: "toolResult",
      toolCallId: "call-rg",
      isError: true,
      content: [{ type: "text", text: "no matches" }],
    },
  },
);

const readResult = lines({
  type: "message",
  id: "r2",
  parentId: "r1",
  timestamp: "2026-09-24T10:00:05.000Z",
  message: {
    role: "toolResult",
    toolCallId: "call-read",
    content: [{ type: "text", text: "file body" }],
  },
});

describe("piToolActivityAfterHead", () => {
  it("reconciles an unfinished call before the saved head when a new prompt abandons it", () => {
    expect(piToolActivityAfterHead(session, "r1", FALLBACK)?.tools).toEqual([]);
    const contents = `${session}\n${lines({
      type: "message",
      id: "u2",
      parentId: "r1",
      message: { role: "user", content: "try again" },
    })}`;
    expect(piToolActivityAfterHead(contents, "r1", FALLBACK)?.tools).toMatchObject([
      { toolCallId: "call-read", isNew: false, result: null, endedAt: "2026-09-24T10:00:02.000Z" },
    ]);
  });
  it("reconciles an unfinished call when the assistant finishes its turn", () => {
    const contents = `${session}\n${lines({
      type: "message",
      id: "a2",
      parentId: "r1",
      timestamp: "2026-09-24T10:00:03.000Z",
      message: { role: "assistant", stopReason: "stop", content: [{ type: "text", text: "done" }] },
    })}`;
    expect(piToolActivityAfterHead(contents, "r1", FALLBACK)?.tools).toMatchObject([
      { toolCallId: "call-read", result: null, endedAt: "2026-09-24T10:00:03.000Z" },
    ]);
  });

  it("reads calls from the start when the thread has no head yet", () => {
    const activity = piToolActivityAfterHead(session, null, FALLBACK);
    expect(
      activity?.tools.map((tool) => ({
        id: tool.toolCallId,
        name: tool.name,
        callIndex: tool.callIndex,
        result: tool.result && { text: tool.result.text, isError: tool.result.isError },
      })),
    ).toEqual([
      { id: "call-rg", name: "bash", callIndex: 0, result: { text: "no matches", isError: true } },
      { id: "call-read", name: "read", callIndex: 1, result: null },
    ]);
  });

  it("republishes an earlier call once its result lands after the head", () => {
    const activity = piToolActivityAfterHead(`${session}\n${readResult}`, "r1", FALLBACK);
    // call-rg settled before the head, so only call-read is new information.
    expect(activity?.tools).toHaveLength(1);
    expect(activity?.tools[0]).toMatchObject({
      toolCallId: "call-read",
      isNew: false,
      args: { path: "a.ts" },
      result: { text: "file body", isNew: true, completedAt: "2026-09-24T10:00:05.000Z" },
    });
  });

  it("refuses to guess when the head left the active branch", () => {
    expect(piToolActivityAfterHead(session, "gone", FALLBACK)).toBeNull();
  });

  it("reads extension notices appended after the head", () => {
    const notices = lines(
      {
        type: "custom",
        id: "n1",
        parentId: "r1",
        customType: "ui-notice",
        data: { message: "\u26a1 SoL-Pi \u00b7 Action Fusion\nMoney saved \u00b7 1 round-trip" },
        timestamp: "2026-09-24T10:00:03.000Z",
      },
      {
        type: "custom",
        id: "x1",
        parentId: "n1",
        customType: "other-state",
        data: { message: "x" },
      },
      {
        type: "custom",
        id: "n2",
        parentId: "x1",
        customType: "ui-notice",
        data: { message: "careful", level: "warning" },
      },
    );
    const contents = `${session}\n${notices}`;
    expect(
      piToolActivityAfterHead(contents, null, FALLBACK)?.notices.map(({ entryId, level }) => ({
        entryId,
        level,
      })),
    ).toEqual([
      { entryId: "n1", level: "info" },
      { entryId: "n2", level: "warning" },
    ]);
    expect(
      piToolActivityAfterHead(contents, "n1", FALLBACK)?.notices.map((notice) => notice.entryId),
    ).toEqual(["n2"]);
  });
});

describe("piTurnEnded", () => {
  const user = { type: "message", id: "u", message: { role: "user", content: "go" } };
  const toolTurn = {
    type: "message",
    id: "a1",
    message: { role: "assistant", content: [], stopReason: "toolUse" },
  };
  const toolResult = { type: "message", id: "r", message: { role: "toolResult", content: [] } };
  const answer = {
    type: "message",
    id: "a2",
    message: { role: "assistant", content: [], stopReason: "stop" },
  };

  it("is false while the agent is between tool calls", () => {
    expect(piTurnEnded(lines(user, toolTurn))).toBe(false);
    expect(piTurnEnded(lines(user, toolTurn, toolResult))).toBe(false);
  });

  it("is true once the assistant answers, past trailing non-message entries", () => {
    const notice = { type: "custom", id: "n", customType: "ui-notice", data: {} };
    expect(piTurnEnded(`${lines(user, toolTurn, toolResult, answer, notice)}\n`)).toBe(true);
  });

  it("is false when a new prompt is waiting for an answer", () => {
    expect(piTurnEnded(lines(user, answer, user))).toBe(false);
  });
});

describe("piAgentTurns", () => {
  // A chain of entries, each the child of the one before, at the given minute.
  const chain = (...entries: ReadonlyArray<[minute: number, entry: object]>) =>
    lines(
      ...entries.map(([minute, entry], index) => ({
        parentId: index === 0 ? null : `e${index - 1}`,
        id: `e${index}`,
        timestamp: `2026-09-24T10:${String(minute).padStart(2, "0")}:00.000Z`,
        ...entry,
      })),
    );
  const prompt = { type: "message", message: { role: "user", content: "go" } };
  const call = { type: "message", message: { role: "assistant", stopReason: "toolUse" } };
  const result = { type: "message", message: { role: "toolResult" } };
  const answer = { type: "message", message: { role: "assistant", stopReason: "stop" } };
  const state = { type: "custom", customType: "sol-pi-online-context-state-v1" };
  const update = (text: string) => ({
    type: "custom_message",
    customType: "delegate",
    display: true,
    content: text,
  });
  const spans = (contents: string) =>
    [...piAgentTurns(contents, FALLBACK).turns.values()].map((turn) => ({
      at: turn.entryId,
      kind: turn.kind,
      from: turn.startedAt.slice(11, 16),
      to: turn.endedAt.slice(11, 16),
    }));

  it("ends a turn at the agent's last message, not the next prompt or later entries", () => {
    const contents = chain(
      [0, prompt],
      [1, call],
      [3, result],
      [4, answer],
      [5, state],
      [30, prompt],
    );
    expect(spans(contents)).toEqual([
      { at: "e0", kind: "prompt", from: "10:00", to: "10:04" },
      { at: "e5", kind: "prompt", from: "10:30", to: "10:30" },
    ]);
  });

  it("gives a helper update that woke the idle agent its own turn", () => {
    const contents = chain(
      [0, prompt],
      [2, answer],
      [20, update("Helper update:\n[h1] done")],
      [21, call],
      [22, result],
      [23, answer],
    );
    expect(spans(contents)).toEqual([
      { at: "e0", kind: "prompt", from: "10:00", to: "10:02" },
      { at: "e2", kind: "wake", from: "10:20", to: "10:23" },
    ]);
    const { turns, turnIdByEntryId } = piAgentTurns(contents, FALLBACK);
    expect(turns.get("e2")?.text).toBe("Helper update:\n[h1] done");
    expect(turnIdByEntryId.get("e4")).toBe("e2");
    expect(turnIdByEntryId.get("e1")).toBe("e0");
  });

  it("keeps updates delivered mid-turn, unanswered, or back to back in one turn", () => {
    const midTurn = chain([0, prompt], [1, call], [2, update("u")], [3, result], [4, answer]);
    expect(spans(midTurn)).toEqual([{ at: "e0", kind: "prompt", from: "10:00", to: "10:04" }]);

    const unanswered = chain([0, prompt], [1, answer], [5, update("u")]);
    expect(spans(unanswered)).toEqual([{ at: "e0", kind: "prompt", from: "10:00", to: "10:01" }]);

    const burst = chain([0, prompt], [1, answer], [5, update("a")], [5, update("b")], [7, answer]);
    expect(spans(burst)).toEqual([
      { at: "e0", kind: "prompt", from: "10:00", to: "10:01" },
      { at: "e2", kind: "wake", from: "10:05", to: "10:07" },
    ]);
  });
});

describe("piToolActivityAfterHead thoughts", () => {
  const thinking = lines(
    { type: "message", id: "u1", parentId: null, message: { role: "user", content: "go" } },
    {
      type: "message",
      id: "a1",
      parentId: "u1",
      timestamp: "2026-09-24T10:00:09.000Z",
      message: {
        role: "assistant",
        timestamp: Date.parse("2026-09-24T10:00:01.000Z"),
        content: [
          { type: "thinking", thinking: "  Check the config first.  " },
          { type: "thinking", thinking: "" },
          { type: "text", text: "Looking." },
          { type: "toolCall", id: "call-1", name: "read", arguments: { path: "a.ts" } },
        ],
      },
    },
  );

  it("reads non-empty thinking blocks, timed from stream start to message end", () => {
    const activity = piToolActivityAfterHead(thinking, null, FALLBACK);
    expect(activity?.thoughts).toEqual([
      {
        entryId: "a1",
        branchIndex: 1,
        blockIndex: 0,
        text: "Check the config first.",
        startedAt: "2026-09-24T10:00:01.000Z",
        completedAt: "2026-09-24T10:00:09.000Z",
      },
    ]);
    expect(activity?.textBlockIndexById.get("a1")).toBe(2);
    expect(activity?.tools.map((tool) => tool.blockIndex)).toEqual([3]);
  });

  it("skips thoughts at or before the head", () => {
    expect(piToolActivityAfterHead(thinking, "a1", FALLBACK)?.thoughts).toEqual([]);
  });
});

describe("piBranchEntryAtOrBefore", () => {
  // T3 recorded head "err"; Pi then retried from "u1", abandoning it, and a
  // CLI prompt followed after T3's last update at 10:05.
  const retried = lines(
    { type: "session", id: "s" },
    { type: "message", id: "u1", parentId: null, timestamp: "2026-09-24T10:00:00.000Z" },
    { type: "message", id: "err", parentId: "u1", timestamp: "2026-09-24T10:01:00.000Z" },
    { type: "message", id: "retry", parentId: "u1", timestamp: "2026-09-24T10:02:00.000Z" },
    { type: "custom", id: "state", parentId: "retry" },
    { type: "message", id: "u2", parentId: "state", timestamp: "2026-09-24T10:09:00.000Z" },
  );

  it("resumes after the last branch entry written by the follower's last update", () => {
    expect(piBranchEntryAtOrBefore(retried, "2026-09-24T10:05:00.000Z")).toBe("state");
    expect(piBranchEntryAtOrBefore(retried, "2026-09-24T10:09:00.000Z")).toBe("u2");
  });

  it("reads from the start when every entry is newer", () => {
    expect(piBranchEntryAtOrBefore(retried, "2026-09-24T09:00:00.000Z")).toBeNull();
  });
});
