import { describe, expect, it } from "@effect/vitest";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";

import { decodePiUsageCache, piUsageLimitsSnapshot } from "./piUsageLimits.ts";

const iso = (millis: number) => DateTime.formatIso(DateTime.makeUnsafe(millis));
const readAt = "2026-09-24T00:00:00.000Z";

describe("piUsageLimitsSnapshot", () => {
  it.effect("maps the shared cache file onto a pi source", () =>
    Effect.gen(function* () {
      const cache = yield* decodePiUsageCache(
        `{"claude":{"vendor":{"name":"claude","windows":[{"label":"5h","pct":12,"resetMs":1790279400000},{"label":"7d","pct":47,"resetMs":1790798400000},{"label":"7d fable","pct":2}],"at":1790277456343},"tried":1790277456343},"codex":{"vendor":{"name":"codex","windows":[{"label":"7d","pct":30}],"plan":"prolite","at":1790277387738},"tried":1790277387738}}`,
      );
      const snapshot = piUsageLimitsSnapshot(cache, readAt);
      expect(snapshot).toMatchObject({ id: "pi", kind: "pi", label: "Pi" });
      // The newest vendor read, not the moment the file was read.
      expect(snapshot.checkedAt).toBe(iso(1790277456343));
      const [claude, codex] = snapshot.accounts;
      expect(claude).toMatchObject({ id: "Claude", driver: "pi" });
      expect(claude?.usageLimits.windows).toEqual([
        {
          id: "5h",
          kind: "session",
          label: "5h",
          usedPercent: 12,
          resetsAt: iso(1790279400000),
          windowDurationMins: 300,
        },
        {
          id: "7d",
          kind: "weekly",
          label: "7d",
          usedPercent: 47,
          resetsAt: iso(1790798400000),
          windowDurationMins: 10080,
        },
        {
          id: "7d-fable",
          kind: "weekly",
          label: "7d fable",
          usedPercent: 2,
          windowDurationMins: 10080,
        },
      ]);
      expect(codex).toMatchObject({ id: "Codex", driver: "pi", plan: "prolite" });
    }),
  );

  it.effect("reports a vendor error as a failed probe", () =>
    Effect.gen(function* () {
      const cache = yield* decodePiUsageCache(
        `{"claude":{"vendor":{"name":"claude","windows":[],"error":"HTTP 429"}}}`,
      );
      const snapshot = piUsageLimitsSnapshot(cache, readAt);
      expect(snapshot.checkedAt).toBe(readAt);
      expect(snapshot.accounts).toEqual([
        {
          id: "Claude",
          driver: "pi",
          usageLimits: {
            checkedAt: readAt,
            windows: [],
            unavailable: { reason: "probeFailed", message: "HTTP 429" },
          },
        },
      ]);
    }),
  );
});
