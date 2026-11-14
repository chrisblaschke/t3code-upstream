/**
 * Map Pi's shared usage cache to snapshots with driver `pi`; never poll APIs.
 * `UsageLimitSources` owns reading and watching the extension-maintained file.
 */
import type {
  ServerProviderUsageLimits,
  ServerProviderUsageWindow,
  UsageLimitSourceAccount,
  UsageLimitSourceSnapshot,
} from "@t3tools/contracts";
import { ProviderDriverKind, UsageLimitSourceId } from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Schema from "effect/Schema";

const PI_USAGE_SOURCE_ID = UsageLimitSourceId.make("pi");
const PI_DRIVER = ProviderDriverKind.make("pi");

const PiUsageWindow = Schema.Struct({
  label: Schema.String,
  pct: Schema.Number,
  resetMs: Schema.optional(Schema.Number),
});

const PiUsageVendor = Schema.Struct({
  windows: Schema.optional(Schema.Array(PiUsageWindow)),
  plan: Schema.optional(Schema.String),
  at: Schema.optional(Schema.Number),
  error: Schema.optional(Schema.String),
});

const PiUsageEntry = Schema.Struct({ vendor: Schema.optional(PiUsageVendor) });

export const PiUsageCache = Schema.Struct({
  claude: Schema.optional(PiUsageEntry),
  codex: Schema.optional(PiUsageEntry),
});
export type PiUsageCache = typeof PiUsageCache.Type;

export const decodePiUsageCache = Schema.decodeUnknownEffect(Schema.fromJsonString(PiUsageCache));

const iso = (millis: number) => DateTime.formatIso(DateTime.makeUnsafe(millis));

function windowKind(label: string): ServerProviderUsageWindow["kind"] {
  if (/^\d+h\b/i.test(label)) return "session";
  if (/^7d\b/i.test(label)) return "weekly";
  if (/^(30d|1mo)\b/i.test(label)) return "monthly";
  return "other";
}

function windowDurationMins(label: string): number | undefined {
  const match = /^(\d+)([hd])\b/i.exec(label);
  if (!match) return undefined;
  return Number(match[1]) * (match[2]!.toLowerCase() === "h" ? 60 : 60 * 24);
}

function mapWindow(window: typeof PiUsageWindow.Type): ServerProviderUsageWindow | null {
  const label = window.label.trim();
  const id = label
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-|-$/g, "");
  if (!id || !Number.isFinite(window.pct)) return null;
  const duration = windowDurationMins(label);
  return {
    id,
    kind: windowKind(label),
    label,
    usedPercent: Math.max(0, Math.min(100, window.pct)),
    ...(window.resetMs !== undefined && Number.isFinite(window.resetMs)
      ? { resetsAt: iso(window.resetMs) }
      : {}),
    ...(duration !== undefined ? { windowDurationMins: duration } : {}),
  };
}

function mapAccount(
  id: string,
  vendor: typeof PiUsageVendor.Type,
  checkedAt: string,
): UsageLimitSourceAccount {
  const windows = (vendor.windows ?? []).flatMap((window) => mapWindow(window) ?? []);
  const error = vendor.error?.trim();
  const plan = vendor.plan?.trim();
  const usageLimits: ServerProviderUsageLimits = {
    checkedAt: vendor.at !== undefined && Number.isFinite(vendor.at) ? iso(vendor.at) : checkedAt,
    windows,
    ...(error ? { unavailable: { reason: "probeFailed", message: error } } : {}),
  };
  return { id, driver: PI_DRIVER, ...(plan ? { plan } : {}), usageLimits };
}

/**
 * Maps a decoded cache onto the `pi` source. `readAt` stands in for a vendor
 * that has no timestamp; otherwise `checkedAt` is the newest vendor read, so
 * an unchanged file maps to an equal snapshot and publishes nothing.
 */
export function piUsageLimitsSnapshot(
  cache: PiUsageCache,
  readAt: string,
): UsageLimitSourceSnapshot {
  const accounts = (
    [
      ["Claude", cache.claude?.vendor],
      ["Codex", cache.codex?.vendor],
    ] as const
  ).flatMap(([id, vendor]) => (vendor ? [mapAccount(id, vendor, readAt)] : []));
  const readTimes = [cache.claude?.vendor?.at, cache.codex?.vendor?.at].filter(
    (at): at is number => at !== undefined && Number.isFinite(at),
  );
  const checkedAt = readTimes.length > 0 ? iso(Math.max(...readTimes)) : readAt;
  return { id: PI_USAGE_SOURCE_ID, kind: "pi", label: "Pi", checkedAt, accounts };
}

/** The row kept when the file exists but cannot be read, so the failure is visible. */
export function piUsageLimitsError(checkedAt: string, error: string): UsageLimitSourceSnapshot {
  return { id: PI_USAGE_SOURCE_ID, kind: "pi", label: "Pi", checkedAt, accounts: [], error };
}
