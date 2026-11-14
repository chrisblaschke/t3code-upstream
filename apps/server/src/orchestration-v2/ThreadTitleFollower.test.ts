import { describe, expect, it } from "@effect/vitest";

import { titleRefreshDue } from "./ThreadTitleFollower.ts";

const MINUTE = 60 * 1000;

describe("titleRefreshDue", () => {
  it("retitles after each of the first turns, then only after the interval", () => {
    const now = 1_000 * MINUTE;
    expect(titleRefreshDue(undefined, "t1", now)).toBe(true);
    expect(titleRefreshDue({ refreshes: 2, lastAtMs: now, lastKey: "t2" }, "t3", now)).toBe(true);
    expect(titleRefreshDue({ refreshes: 3, lastAtMs: now, lastKey: "t3" }, "t4", now)).toBe(false);
    expect(
      titleRefreshDue({ refreshes: 3, lastAtMs: now - 20 * MINUTE, lastKey: "t3" }, "t4", now),
    ).toBe(true);
  });

  it("does not retitle twice for the same turn", () => {
    expect(titleRefreshDue({ refreshes: 1, lastAtMs: 0, lastKey: "t1" }, "t1", 1e9)).toBe(false);
  });
});
