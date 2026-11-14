import { describe, expect, it } from "vite-plus/test";

import type { OrchestrationV2TurnItem } from "@t3tools/contracts";

import { outputLines, terminalOutputPreview, terminalOutputText } from "./terminalOutput.ts";

const numbered = (count: number) =>
  Array.from({ length: count }, (_, index) => `line ${index + 1}`).join("\n");

describe("terminalOutputPreview", () => {
  it("keeps the end of a command's output, where results and errors land", () => {
    expect(terminalOutputPreview(`${numbered(8)}\n`, "command")).toEqual({
      lines: ["line 4", "line 5", "line 6", "line 7", "line 8"],
      hiddenLineCount: 3,
      hiddenCharacterCount: 0,
    });
  });

  it("keeps the start of other output and drops diff file headers", () => {
    expect(terminalOutputPreview(numbered(12), "text")?.hiddenLineCount).toBe(2);
    const patch = "--- a/x.ts\n+++ b/x.ts\n@@ -1,2 +1,2 @@\n-old\n+new\n same";
    expect(terminalOutputPreview(patch, "diff")?.lines).toEqual([
      "@@ -1,2 +1,2 @@",
      "-old",
      "+new",
      " same",
    ]);
  });

  it.each(["command", "diff", "text"] as const)(
    "tracks clipped characters without hidden lines for %s output",
    (kind) => {
      const text = `+${"x".repeat(450)}\n${"y".repeat(410)}`;
      expect(terminalOutputPreview(text, kind)).toEqual({
        lines: [`+${"x".repeat(399)}…`, `${"y".repeat(400)}…`],
        hiddenLineCount: 0,
        hiddenCharacterCount: 61,
      });
      // Expanding uses the original lines, not the clipped preview.
      expect(outputLines(text, kind).join("\n")).toBe(text);
    },
  );

  it("does not count omitted lines as clipped characters", () => {
    const text = `${"x".repeat(500)}\n${numbered(5)}`;
    expect(terminalOutputPreview(text, "command")?.hiddenCharacterCount).toBe(0);
    expect(terminalOutputPreview(text, "command")?.hiddenLineCount).toBe(1);
    expect(terminalOutputPreview("x".repeat(400), "text")?.hiddenCharacterCount).toBe(0);
  });

  it("shows nothing for blank output", () => {
    expect(terminalOutputPreview("  \n\n", "text")).toBeNull();
  });
});

describe("terminalOutputText", () => {
  it("does not serialize an image-only result's base64 payload", () => {
    expect(
      terminalOutputText(
        item({
          type: "dynamic_tool",
          output: {
            content: [{ type: "image", mimeType: "image/png", data: "SECRET_BASE64_PAYLOAD" }],
          },
        }),
      ),
    ).toBeNull();
  });
  const item = (fields: Record<string, unknown>) => fields as unknown as OrchestrationV2TurnItem;

  it("reads the text blocks of a content-block tool result", () => {
    const output = [
      { type: "text", text: "first" },
      { type: "image", data: "…" },
      { type: "text", text: "second" },
    ];
    expect(terminalOutputText(item({ type: "dynamic_tool", output }))).toBe(
      "first\n[image]\nsecond",
    );
    expect(terminalOutputText(item({ type: "dynamic_tool", output: { content: output } }))).toBe(
      "first\n[image]\nsecond",
    );
  });

  it("prints command stdout and stderr rather than their serialized envelope", () => {
    expect(
      terminalOutputText(
        item({
          type: "command_execution",
          output: JSON.stringify({ stdout: "hello", stderr: "warning", interrupted: false }),
        }),
      ),
    ).toBe("hello\nwarning");
  });

  it("uses a file change's diff, falling back to the written text", () => {
    expect(terminalOutputText(item({ type: "file_change", diffStr: "+a" }))).toBe("+a");
    expect(terminalOutputText(item({ type: "file_change", newStr: "hello" }))).toBe("hello");
    expect(terminalOutputText(item({ type: "file_change" }))).toBeNull();
  });
});
