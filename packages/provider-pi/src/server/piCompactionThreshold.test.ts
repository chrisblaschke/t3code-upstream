import { describe, expect, it } from "vite-plus/test";

import { piAutoCompactThreshold } from "./piCompactionThreshold.ts";

const opus = { provider: "anthropic", id: "claude-opus-5-5" };

describe("piAutoCompactThreshold", () => {
  it("uses Pi's default reserve when nothing is configured", () => {
    expect(piAutoCompactThreshold([{}, {}], opus, 200_000)).toBe(183_616);
  });

  it("prefers a model override over the ordinary reserve, and project over global", () => {
    const global = {
      compaction: {
        reserveTokens: 50_000,
        modelOverrides: { "anthropic/claude-opus-5-5": { reserveTokens: 800_000 } },
      },
    };
    expect(piAutoCompactThreshold([global, {}], opus, 1_000_000)).toBe(200_000);
    expect(
      piAutoCompactThreshold([global, {}], { provider: "openai-codex", id: "gpt-6-sol" }, 272_000),
    ).toBe(222_000);
    const project = {
      compaction: { modelOverrides: { "anthropic/claude-opus-5-5": { reserveTokens: 900_000 } } },
    };
    expect(piAutoCompactThreshold([global, project], opus, 1_000_000)).toBe(100_000);
  });

  it("is null when compaction is off or the reserve swallows the window", () => {
    expect(piAutoCompactThreshold([{ compaction: { enabled: false } }], opus, 200_000)).toBeNull();
    expect(
      piAutoCompactThreshold([{ compaction: { reserveTokens: 300_000 } }], opus, 200_000),
    ).toBeNull();
  });
});
