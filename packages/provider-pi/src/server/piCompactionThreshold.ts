// @effect-diagnostics nodeBuiltinImport:off
import * as NodeFS from "node:fs";
import * as NodePath from "node:path";

/** Pi's built-in `compaction.reserveTokens` (settings-manager DEFAULT_COMPACTION_TOKEN_SETTINGS). */
const PI_DEFAULT_RESERVE_TOKENS = 16_384;

type Settings = { readonly compaction?: unknown };

const record = (value: unknown): Record<string, unknown> | undefined =>
  value !== null && typeof value === "object" && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : undefined;

const tokenCount = (value: unknown): number | undefined =>
  typeof value === "number" && Number.isSafeInteger(value) && value >= 0 ? value : undefined;

/**
 * Mirror Pi's `tokens > contextWindow - reserveTokens` threshold: project over
 * global, model override over ordinary value, then default. Null means off or
 * no room after reserve; callers fall back to the plain context window.
 */
export function piAutoCompactThreshold(
  layers: ReadonlyArray<Settings>,
  model: { readonly provider: string; readonly id: string },
  contextWindow: number,
): number | null {
  let enabled = true;
  let reserve: number | undefined;
  let override: number | undefined;
  for (const layer of layers) {
    const compaction = record(layer.compaction);
    if (!compaction) continue;
    if (typeof compaction.enabled === "boolean") enabled = compaction.enabled;
    reserve = tokenCount(compaction.reserveTokens) ?? reserve;
    const entry = record(record(compaction.modelOverrides)?.[`${model.provider}/${model.id}`]);
    override = tokenCount(entry?.reserveTokens) ?? override;
  }
  if (!enabled) return null;
  const threshold = contextWindow - (override ?? reserve ?? PI_DEFAULT_RESERVE_TOKENS);
  return threshold > 0 ? threshold : null;
}

const readSettings = (file: string): Settings => {
  try {
    return record(JSON.parse(NodeFS.readFileSync(file, "utf8"))) ?? {};
  } catch {
    return {};
  }
};

/** Global then project settings, the order Pi merges them in. */
export function readPiCompactionSettings(agentDir: string, cwd: string): ReadonlyArray<Settings> {
  return [
    readSettings(NodePath.join(agentDir, "settings.json")),
    readSettings(NodePath.join(cwd, ".pi", "settings.json")),
  ];
}
