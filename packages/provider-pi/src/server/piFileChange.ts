/**
 * What a Pi `edit` or `write` changed, as file_change fields: the unified
 * patch Pi returns in `details.patch` (the diff the Pi terminal draws) or, for
 * a write, the start of the written content. Both are bounded so a large file
 * does not bloat the projection; clients read them on demand.
 */

const MAX_PATCH_CHARS = 64 * 1024;
const MAX_WRITTEN_CHARS = 16 * 1024;

export interface PiFileChangeFields {
  readonly diffStr?: string;
  readonly newStr?: string;
  readonly additions?: number;
  readonly deletions?: number;
}

function field(record: unknown, key: string): unknown {
  return typeof record === "object" && record !== null
    ? (record as Record<string, unknown>)[key]
    : undefined;
}

export function piFileChangeFields(
  toolName: string,
  args: unknown,
  details: unknown,
): PiFileChangeFields {
  const patch = field(details, "patch");
  if (toolName === "edit" && typeof patch === "string" && patch.length > 0) {
    let additions = 0;
    let deletions = 0;
    for (const line of patch.split("\n")) {
      if (line.startsWith("+") && !line.startsWith("+++")) additions += 1;
      else if (line.startsWith("-") && !line.startsWith("---")) deletions += 1;
    }
    return { diffStr: patch.slice(0, MAX_PATCH_CHARS), additions, deletions };
  }
  const content = field(args, "content");
  if (toolName === "write" && typeof content === "string") {
    return {
      newStr: content.slice(0, MAX_WRITTEN_CHARS),
      additions: content.length === 0 ? 0 : content.replace(/\n$/, "").split("\n").length,
    };
  }
  return {};
}
