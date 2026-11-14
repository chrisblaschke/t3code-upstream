import type { EnvironmentId, OrchestrationV2ProjectedTurnItem } from "@t3tools/contracts";
import { memo, useState } from "react";

import { cn } from "~/lib/utils";
import { useTurnItemDetail } from "../../state/queries";
import { ChatMarkdownAssetImage } from "./ChatMarkdownMedia";
import type { ExpandedImagePreview } from "./ExpandedImagePreview";
import { turnItemOutputImages } from "@t3tools/client-runtime/work-log/item-detail";
import {
  turnItemDetailRevision,
  turnItemNeedsDetailFetch,
} from "@t3tools/client-runtime/work-log/item-detail";
import type { TurnItemSource } from "./timelineRowContext";
import {
  outputLines,
  terminalOutputPreview,
  terminalOutputText,
  type TerminalOutputKind,
} from "@t3tools/client-runtime/work-log/terminal-output";

function lineClassName(line: string, kind: TerminalOutputKind): string | undefined {
  if (kind !== "diff") return undefined;
  if (line.startsWith("+")) return "text-diff-addition";
  if (line.startsWith("-")) return "text-diff-deletion";
  if (line.startsWith("@@")) return "text-muted-foreground/70";
  return undefined;
}

// Consecutive lines with one colour share a text node, so a full output is a
// handful of nodes (one for plain output), not one element per line.
function OutputLines({ lines, kind }: { lines: ReadonlyArray<string>; kind: TerminalOutputKind }) {
  const runs: Array<{ className: string | undefined; text: string }> = [];
  for (const line of lines) {
    const className = lineClassName(line, kind);
    const last = runs.at(-1);
    if (last !== undefined && last.className === className) last.text += `\n${line}`;
    else runs.push({ className, text: runs.length === 0 ? line : `\n${line}` });
  }
  return (
    <div className="min-h-[1lh]">
      {runs.map((run, index) => (
        // Runs have no identity beyond their position in a fixed output.
        // oxlint-disable-next-line react/no-array-index-key
        <span key={index} className={run.className}>
          {run.text}
        </span>
      ))}
    </div>
  );
}

/**
 * A finished tool's output under its row, as the Pi terminal prints it.
 * The output is fetched only when the row mounts (the list is virtualized),
 * so snapshots and compact mode never carry it.
 */
export const TerminalOutputPreview = memo(function TerminalOutputPreview({
  environmentId,
  projectedItem,
  kind,
  onImageExpand,
  source,
}: {
  environmentId: EnvironmentId;
  projectedItem: OrchestrationV2ProjectedTurnItem;
  source?: TurnItemSource | undefined;
  kind: TerminalOutputKind;
  onImageExpand: (preview: ExpandedImagePreview) => void;
}) {
  const item = projectedItem.item;
  const fetches = turnItemNeedsDetailFetch(item);
  const detail = useTurnItemDetail(
    fetches
      ? {
          environmentId,
          threadId: projectedItem.sourceThreadId,
          itemId: projectedItem.sourceItemId,
          revision: turnItemDetailRevision(item),
        }
      : null,
    source?.detail,
  );
  const [showAll, setShowAll] = useState(false);
  const fetched = detail.data?.item;
  const outputItem = fetched?.type === item.type ? fetched : item;
  const text = terminalOutputText(outputItem);
  const preview = text === null ? null : terminalOutputPreview(text, kind);
  const allLines = showAll && text !== null ? outputLines(text, kind) : null;
  const images = turnItemOutputImages(outputItem);
  const error = fetches && (detail.error || detail.data?.item === null);
  if (error)
    return (
      <p role="status" className="ms-7 text-xs text-destructive">
        Could not load tool output.
      </p>
    );
  if (preview === null && images.length === 0) return null;

  const toggle =
    preview !== null && (preview.hiddenLineCount > 0 || preview.hiddenCharacterCount > 0) ? (
      <button
        type="button"
        className="text-muted-foreground/70 hover:text-foreground focus-visible:outline-none focus-visible:underline"
        onClick={() => setShowAll((value) => !value)}
      >
        {showAll
          ? "Show less"
          : preview.hiddenLineCount === 0
            ? "Show full output"
            : kind === "command"
              ? `… ${preview.hiddenLineCount} earlier lines`
              : `… ${preview.hiddenLineCount} more lines`}
      </button>
    ) : null;

  return (
    <div
      className={cn(
        "ms-7 mb-1 cursor-auto whitespace-pre-wrap break-words font-mono text-secondary-label",
        "text-(length:--font-size-code,var(--text-2xs)) leading-relaxed select-text",
        allLines !== null && "max-h-96 overflow-auto",
      )}
      data-terminal-output={kind}
      onClick={(event) => event.stopPropagation()}
      onPointerDown={(event) => event.stopPropagation()}
    >
      {kind === "command" && toggle}
      {preview !== null && <OutputLines lines={allLines ?? preview.lines} kind={kind} />}
      {images.map((resource) => (
        <ChatMarkdownAssetImage
          key={resource.index}
          environmentId={environmentId}
          resource={resource}
          alt="Tool output"
          onImageExpand={onImageExpand}
        />
      ))}
      {kind !== "command" && toggle}
    </div>
  );
});
