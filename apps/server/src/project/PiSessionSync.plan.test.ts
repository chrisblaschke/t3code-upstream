import { RunId, ThreadId } from "@t3tools/contracts";
import { describe, expect, it } from "@effect/vitest";

import type { PiToolActivity } from "./piSessionItems.ts";
import { piToolTurnItemEvents } from "./PiSessionSync.ts";

const threadId = ThreadId.make("thread-1");
const runId = RunId.make("run-1");

const planCall = (
  toolCallId: string,
  statuses: ReadonlyArray<string>,
  isError = false,
): PiToolActivity => ({
  toolCallId,
  entryId: `entry-${toolCallId}`,
  branchIndex: 1,
  callIndex: 0,
  blockIndex: 0,
  name: "update_plan",
  args: {
    steps: statuses.map((status, index) => ({ id: `s${index}`, goal: `step ${index}`, status })),
  },
  startedAt: "2026-09-24T00:00:00.000Z",
  result: { text: "ok", isError, completedAt: "2026-09-24T00:00:01.000Z", isNew: true },
  isNew: true,
});

const plans = (tool: PiToolActivity) =>
  piToolTurnItemEvents({ threadId, tool, runId, ordinal: 17 }).flatMap((event) =>
    event.type === "plan.updated" && event.payload.kind === "todo_list" ? [event.payload] : [],
  );

describe("piToolTurnItemEvents", () => {
  it("shows a CLI session's update_plan calls as one task list per run", () => {
    const [first] = plans(planCall("a", ["completed", "in_progress", "pending"]));
    const [second] = plans(planCall("b", ["completed", "completed", "completed"]));
    expect(first?.steps.map((step) => step.status)).toEqual(["completed", "running", "pending"]);
    expect(first?.status).toBe("active");
    expect(second?.id).toBe(first?.id);
    expect(second?.status).toBe("completed");
  });

  it("keeps a failed update_plan call as an ordinary tool row", () => {
    const events = piToolTurnItemEvents({
      threadId,
      tool: planCall("c", ["pending"], true),
      runId,
      ordinal: 17,
    });
    expect(events.map((event) => event.type)).toEqual(["turn-item.updated"]);
    expect(events[0]?.type === "turn-item.updated" && events[0].payload.type).toBe("dynamic_tool");
  });
});
