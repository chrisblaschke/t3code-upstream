/**
 * Read `t3-follow` busy markers (`<agentDir>/session-busy/<pid>.json`) and the
 * session's last entry so T3 and the Pi TUI do not branch from stale leaves.
 */
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";

const TAIL_BYTES = 64 * 1024;

function isProcessAlive(pid: number): boolean {
  try {
    process.kill(pid, 0);
    return true;
  } catch (error) {
    // EPERM: alive but owned by someone else.
    return (error as NodeJS.ErrnoException).code === "EPERM";
  }
}

const BusyMarker = Schema.fromJsonString(
  Schema.Struct({ pid: Schema.Number, sessionFile: Schema.String, mode: Schema.String }),
);
const decodeMarker = Schema.decodeUnknownOption(BusyMarker);
const EntryId = Schema.fromJsonString(Schema.Struct({ type: Schema.String, id: Schema.String }));
const decodeEntryId = Schema.decodeUnknownOption(EntryId);

/** A live Pi TUI (not an rpc process like T3's own) mid-turn on `sessionFile`. */
export const piTuiWriter = Effect.fn("piTuiWriter")(function* (
  agentDir: string,
  sessionFile: string,
  alive: (pid: number) => boolean = isProcessAlive,
) {
  const fileSystem = yield* FileSystem.FileSystem;
  const dir = `${agentDir}/session-busy`;
  const names = yield* fileSystem.readDirectory(dir).pipe(Effect.orElseSucceed(() => []));
  for (const name of names) {
    if (!name.endsWith(".json")) continue;
    const contents = yield* Effect.option(fileSystem.readFileString(`${dir}/${name}`));
    const marker = Option.flatMap(contents, decodeMarker);
    if (Option.isNone(marker)) continue;
    const { pid, sessionFile: file, mode } = marker.value;
    if (file === sessionFile && mode !== "rpc" && alive(pid)) return { pid };
  }
  return undefined;
});

/** Id of the last complete entry in a Pi session file, read from its tail. */
export const piSessionLastEntryId = Effect.fn("piSessionLastEntryId")(function* (
  sessionFile: string,
) {
  const fileSystem = yield* FileSystem.FileSystem;
  return yield* Effect.scoped(
    Effect.gen(function* () {
      const file = yield* fileSystem.open(sessionFile, { flag: "r" });
      const size = Number((yield* file.stat).size);
      let length = Math.min(size, TAIL_BYTES);
      while (length > 0) {
        const start = size - length;
        yield* file.seek(BigInt(start), "start");
        const bytes = new Uint8Array(length);
        let read = 0;
        while (read < length) {
          const n = Number(yield* file.read(bytes.subarray(read)));
          if (n === 0) break;
          read += n;
        }
        const lines = new TextDecoder().decode(bytes.subarray(0, read)).trimEnd().split("\n");
        // The first line may start before this window; only trust it at the start of the file.
        for (let i = lines.length - 1; i >= (start > 0 ? 1 : 0); i--) {
          const entry = decodeEntryId(lines[i]);
          // The header line carries the session id, not an entry id.
          if (Option.isSome(entry) && entry.value.type !== "session") return entry.value.id;
        }
        if (start === 0) break;
        length = Math.min(size, length * 2);
      }
      return undefined;
    }),
  ).pipe(Effect.orElseSucceed(() => undefined));
});
