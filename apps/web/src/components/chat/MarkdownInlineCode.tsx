import type { HTMLAttributes, MouseEvent } from "react";

import { useCopyToClipboard } from "../../hooks/useCopyToClipboard";
import { reportMarkdownActionFailure } from "./markdownActionFailure";

// Native word selection stops at hyphens and dots, so a double-click takes the whole span.
function selectInlineCode(event: MouseEvent<HTMLElement>) {
  window.getSelection()?.selectAllChildren(event.currentTarget);
}

/** Inline code that copies its text on a plain click. A drag that selects part of it copies nothing. */
export function MarkdownInlineCode({
  text,
  children,
  ...props
}: HTMLAttributes<HTMLElement> & { text: string }) {
  const { copyToClipboard, isCopied } = useCopyToClipboard({
    timeout: 1200,
    onError: (cause) => reportMarkdownActionFailure({ operation: "copy-inline-code" }, cause),
  });
  return (
    <code
      {...props}
      data-copyable=""
      data-copied={isCopied ? "" : undefined}
      onClick={() => {
        if (window.getSelection()?.isCollapsed !== false) copyToClipboard(text);
      }}
      onDoubleClick={selectInlineCode}
    >
      {children}
    </code>
  );
}
