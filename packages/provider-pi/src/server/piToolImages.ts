/**
 * Store Pi tool images as content-addressed files under the server state dir.
 * Clients load `viewedImagePath` as an asset URL, never as websocket image bytes.
 */
// @effect-diagnostics-next-line nodeBuiltinImport:off -- a synchronous content hash names the image file.
import * as NodeCrypto from "node:crypto";

import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";

const EXTENSION_BY_MIME: Readonly<Record<string, string>> = {
  "image/png": "png",
  "image/jpeg": "jpg",
  "image/jpg": "jpg",
  "image/gif": "gif",
  "image/webp": "webp",
};

/** Pi's ~/.pi/agent/images and read cap images well below this. */
const MAX_IMAGE_BASE64_CHARS = 40 * 1024 * 1024;

export interface PiResultImage {
  readonly data: string;
  readonly extension: string;
}

/** The first image block of a Pi tool result's content, if any. */
export function piResultImage(content: unknown): PiResultImage | undefined {
  if (!Array.isArray(content)) return undefined;
  for (const block of content) {
    if (typeof block !== "object" || block === null) continue;
    const record = block as Record<string, unknown>;
    if (record.type !== "image" || typeof record.data !== "string") continue;
    const mimeType = typeof record.mimeType === "string" ? record.mimeType.toLowerCase() : "";
    const extension = EXTENSION_BY_MIME[mimeType];
    if (extension === undefined) continue;
    if (record.data.length === 0 || record.data.length > MAX_IMAGE_BASE64_CHARS) continue;
    return { data: record.data, extension };
  }
  return undefined;
}

/**
 * A GIF the tool saved and names in `details.path`. The result's image can be a still of it
 * (image_generate returns the sheet of frames), so the row plays the file instead.
 */
function piResultGifPath(details: unknown): string | undefined {
  if (typeof details !== "object" || details === null) return undefined;
  const path = (details as Record<string, unknown>).path;
  return typeof path === "string" && /^(?:\/|[a-z]:[\\/])[^\r\n]*\.gif$/i.test(path)
    ? path
    : undefined;
}

/**
 * Writes the image (once; the name is its hash) and returns its absolute path.
 * Undefined when it cannot be stored; a missing preview never fails a turn.
 */
const persistPiResultImage = Effect.fn("persistPiResultImage")(
  function* (stateDir: string, image: PiResultImage) {
    const fileSystem = yield* FileSystem.FileSystem;
    const hash = NodeCrypto.createHash("sha256").update(image.data).digest("hex").slice(0, 32);
    const directory = `${stateDir}/pi-tool-images`;
    const file = `${directory}/${hash}.${image.extension}`;
    if (yield* fileSystem.exists(file)) return file;
    yield* fileSystem.makeDirectory(directory, { recursive: true });
    // Write under a temporary name so a reader never sees a partial image.
    const temporary = `${file}.${process.pid}.tmp`;
    yield* fileSystem.writeFile(temporary, Buffer.from(image.data, "base64"));
    yield* fileSystem.rename(temporary, file);
    return file;
  },
  Effect.orElseSucceed(() => undefined),
);

/** The file a finished tool's row shows: the GIF it saved, else its stored result image. */
export const piResultPreviewPath = Effect.fn("piResultPreviewPath")(function* (
  stateDir: string,
  result: { readonly image?: PiResultImage | undefined; readonly details?: unknown },
) {
  const fileSystem = yield* FileSystem.FileSystem;
  const gif = piResultGifPath(result.details);
  if (gif !== undefined && (yield* fileSystem.exists(gif).pipe(Effect.orElseSucceed(() => false))))
    return gif;
  return result.image === undefined
    ? undefined
    : yield* persistPiResultImage(stateDir, result.image);
});
