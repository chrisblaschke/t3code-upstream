import { ThreadMarkdownImage } from "./ThreadMarkdownImage";
import type { FilePreviewSource } from "../../components/ui/FilePreviewModal";
import { FilePreviewGallery } from "../../components/ui/FilePreviewGallery";
import { turnItemOutputImages } from "@t3tools/client-runtime/work-log/item-detail";
import { turnItemDetailRevision } from "@t3tools/client-runtime/work-log/item-detail";
import {
  outputLines,
  terminalOutputPreview,
  terminalOutputText,
  type TerminalOutputKind,
} from "@t3tools/client-runtime/work-log/terminal-output";
import type { EnvironmentId, OrchestrationV2TurnItem } from "@t3tools/contracts";
import { memo, useMemo, useState } from "react";
import { Pressable, ScrollView, View } from "react-native";

import { AppText as Text } from "../../components/ui/AppText";
import { orchestrationEnvironment } from "../../state/orchestration";
import { useEnvironmentQuery } from "../../state/query";

function lineClassName(line: string, kind: TerminalOutputKind): string {
  if (kind === "diff") {
    if (line.startsWith("+")) return "text-adaptive-emerald-700-300";
    if (line.startsWith("-")) return "text-adaptive-rose-700-300";
    if (line.startsWith("@@")) return "text-foreground-subtle";
  }
  return "text-foreground-muted";
}

function OutputText(props: {
  readonly lines: ReadonlyArray<string>;
  readonly kind: TerminalOutputKind;
}) {
  // One Text keeps long outputs to a single native text view. Consecutive
  // lines with one colour share a nested span, so plain output is one span
  // and a diff is one per colour change, not one per line.
  const runs: Array<{ className: string; text: string }> = [];
  for (const line of props.lines) {
    const className = lineClassName(line, props.kind);
    const last = runs.at(-1);
    if (last !== undefined && last.className === className) last.text += `\n${line}`;
    else runs.push({ className, text: runs.length === 0 ? line : `\n${line}` });
  }
  return (
    <Text selectable className="font-mono text-2xs leading-normal text-foreground-muted">
      {runs.map((run, index) => (
        // Runs have no identity beyond their position in a fixed output.
        // oxlint-disable-next-line react/no-array-index-key
        <Text key={index} className={run.className}>
          {run.text}
        </Text>
      ))}
    </Text>
  );
}

/**
 * A finished tool's output under its row, as the Pi terminal prints it. It is
 * fetched only when the row mounts (the feed is virtualized), so snapshots and
 * compact mode never carry it.
 */
export const TerminalOutputPreview = memo(function TerminalOutputPreview(props: {
  readonly environmentId: EnvironmentId;
  readonly item: OrchestrationV2TurnItem;
  readonly onPressPreview: (source: FilePreviewSource) => void;
  readonly kind: TerminalOutputKind;
}) {
  const { environmentId, item, kind } = props;
  const detail = useEnvironmentQuery(
    orchestrationEnvironment.turnItem({
      environmentId,
      input: { threadId: item.threadId, itemId: item.id, revision: turnItemDetailRevision(item) },
    }),
  );
  const [showAll, setShowAll] = useState(false);
  const fetched = detail.data?.item;
  const text = fetched ? terminalOutputText(fetched) : null;
  const preview = useMemo(
    () => (text === null ? null : terminalOutputPreview(text, kind)),
    [text, kind],
  );
  const allLines = useMemo(
    () => (showAll && text !== null ? outputLines(text, kind) : null),
    [showAll, text, kind],
  );
  const images = fetched ? turnItemOutputImages(fetched) : [];
  if (preview === null && images.length === 0) return null;

  const toggle =
    preview !== null && (preview.hiddenLineCount > 0 || preview.hiddenCharacterCount > 0) ? (
      <Pressable
        accessibilityRole="button"
        hitSlop={6}
        onPress={() => setShowAll((value) => !value)}
      >
        <Text className="font-mono text-2xs leading-normal text-foreground-subtle">
          {showAll
            ? "Show less"
            : preview.hiddenLineCount === 0
              ? "Show full output"
              : kind === "command"
                ? `… ${preview.hiddenLineCount} earlier lines`
                : `… ${preview.hiddenLineCount} more lines`}
        </Text>
      </Pressable>
    ) : null;

  return (
    <View className="ml-7 pb-1">
      {kind === "command" ? toggle : null}
      {allLines !== null ? (
        <ScrollView
          nestedScrollEnabled
          directionalLockEnabled
          showsVerticalScrollIndicator
          className="max-h-80"
        >
          <OutputText lines={allLines} kind={kind} />
        </ScrollView>
      ) : (
        preview !== null && <OutputText lines={preview.lines} kind={kind} />
      )}
      {kind !== "command" ? toggle : null}
      <FilePreviewGallery
        value={images.map((resource) => ({ kind: "image", environmentId, resource }))}
      >
        {images.map((resource) => (
          <ThreadMarkdownImage
            key={resource.index}
            environmentId={environmentId}
            resource={resource}
            alt={null}
            onPressPreview={props.onPressPreview}
          />
        ))}
      </FilePreviewGallery>
    </View>
  );
});
