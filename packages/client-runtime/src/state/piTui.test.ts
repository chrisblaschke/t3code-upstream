import { describe, expect, it } from "vite-plus/test";
import { canOpenPiTui } from "./piTui.ts";

const projection = (driver: string, sessionFile: string | null) =>
  ({
    thread: { activeProviderThreadId: "p" },
    providerThreads: [
      { id: "p", driver, nativeThreadRef: sessionFile ? { nativeId: sessionFile } : null },
    ],
  }) as unknown as Parameters<typeof canOpenPiTui>[0];

describe("canOpenPiTui", () => {
  it("offers only Pi threads with a session file", () => {
    expect(canOpenPiTui(projection("pi", "/s.jsonl"))).toBe(true);
    expect(canOpenPiTui(projection("codex", "/s.jsonl"))).toBe(false);
    expect(canOpenPiTui(projection("pi", null))).toBe(false);
    expect(canOpenPiTui(null)).toBe(false);
  });
});
