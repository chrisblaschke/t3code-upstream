import * as DateTime from "effect/DateTime";
import {
  EnvironmentId,
  ThreadId,
  TurnItemId,
  type OrchestrationV2ProjectedTurnItem,
} from "@t3tools/contracts";
import { describe, expect, it, vi } from "vite-plus/test";
import { render } from "../../../test/render";
import { TerminalOutputPreview } from "./TerminalOutputPreview";

const query = vi.hoisted(() => ({
  read: vi.fn(),
  result: { data: undefined, error: null } as { data: unknown; error: unknown },
}));
vi.mock(
  import("../../state/queries"),
  () =>
    ({
      useTurnItemDetail: (...args: unknown[]) => {
        query.read(...args);
        return query.result;
      },
    }) as never,
);
vi.mock(
  import("./ChatMarkdownMedia"),
  () =>
    ({
      ChatMarkdownAssetImage: () => null,
    }) as never,
);

const environmentId = EnvironmentId.make("test-environment");
const sourceThreadId = ThreadId.make("source-thread");
const sourceItemId = TurnItemId.make("source-item");
function projected(output: string, omitted = false): OrchestrationV2ProjectedTurnItem {
  return {
    sourceThreadId,
    sourceItemId,
    item: {
      id: TurnItemId.make("projected-item"),
      threadId: ThreadId.make("display-thread"),
      type: "command_execution",
      status: "completed",
      updatedAt: DateTime.makeUnsafe("2026-10-29T10:00:00Z"),
      output,
      outputOmitted: omitted,
    },
  } as OrchestrationV2ProjectedTurnItem;
}

function mockQuery(result = { data: undefined as unknown, error: null as unknown }): Disposable {
  const previous = query.result;
  query.read.mockClear();
  query.result = result;
  return {
    [Symbol.dispose]() {
      query.result = previous;
      query.read.mockClear();
    },
  };
}

describe("TerminalOutputPreview", () => {
  it("expands an overlong single line and collapses it again", async () => {
    using _query = mockQuery();
    const output = "x".repeat(420) + "THE_END";
    await using screen = await render(
      <TerminalOutputPreview
        environmentId={environmentId}
        projectedItem={projected(output)}
        kind="command"
        onImageExpand={() => {}}
      />,
    );
    expect(screen.container.textContent).not.toContain("THE_END");
    await screen.getByRole("button", { name: "Show full output" }).click();
    expect(screen.container.textContent).toContain(output);
    await screen.getByRole("button", { name: "Show less" }).click();
    expect(screen.container.textContent).not.toContain("THE_END");
    expect(query.read).toHaveBeenCalledWith(null, undefined);
  });

  it("reads omitted output through the projected source and surfaces fetch errors", async () => {
    using _query = mockQuery({ data: undefined, error: new Error("offline") });
    const source = { detail: vi.fn(), commandOutput: vi.fn() };
    await using screen = await render(
      <TerminalOutputPreview
        environmentId={environmentId}
        projectedItem={projected("", true)}
        kind="command"
        source={source as never}
        onImageExpand={() => {}}
      />,
    );
    expect(query.read).toHaveBeenCalledWith(
      {
        environmentId,
        threadId: sourceThreadId,
        itemId: sourceItemId,
        revision: "2026-10-29T10:00:00.000Z",
      },
      source.detail,
    );
    await expect
      .element(screen.getByRole("status"))
      .toHaveTextContent("Could not load tool output.");
  });
});
