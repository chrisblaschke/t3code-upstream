import * as NodeServices from "@effect/platform-node/NodeServices";
import { describe, expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";

import { piSessionLastEntryId, piTuiWriter } from "./piSessionWriters.ts";

const line = (record: object) => `${JSON.stringify(record)}\n`;
const header = line({ type: "session", version: 3, id: "session-uuid", cwd: "/tmp" });

const sessionFile = Effect.fn(function* (contents: string) {
  const fileSystem = yield* FileSystem.FileSystem;
  const dir = yield* fileSystem.makeTempDirectoryScoped();
  yield* fileSystem.writeFileString(`${dir}/s.jsonl`, contents);
  return `${dir}/s.jsonl`;
});

describe("piSessionLastEntryId", () => {
  it.effect("never mistakes the header's session id for an entry", () =>
    Effect.gen(function* () {
      expect(yield* piSessionLastEntryId(yield* sessionFile(header))).toBeUndefined();
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );

  it.effect("skips a record another Pi is still appending", () =>
    Effect.gen(function* () {
      const file = yield* sessionFile(
        `${header}${line({ type: "message", id: "a1", parentId: null })}{"type":"message","id":"a2","mes`,
      );
      expect(yield* piSessionLastEntryId(file)).toBe("a1");
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );

  it.effect("reads past the tail window when the last entry is larger than it", () =>
    Effect.gen(function* () {
      const big = line({ type: "message", id: "big", text: "x".repeat(200_000) });
      const file = yield* sessionFile(`${header}${line({ type: "message", id: "a1" })}${big}`);
      expect(yield* piSessionLastEntryId(file)).toBe("big");
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );
});

describe("piTuiWriter", () => {
  it.effect("reports only a live TUI busy on this session file", () =>
    Effect.gen(function* () {
      const fileSystem = yield* FileSystem.FileSystem;
      const agentDir = yield* fileSystem.makeTempDirectoryScoped();
      yield* fileSystem.makeDirectory(`${agentDir}/session-busy`);
      const marker = (pid: number, sessionFile: string, mode: string) =>
        fileSystem.writeFileString(
          `${agentDir}/session-busy/${pid}.json`,
          JSON.stringify({ pid, sessionFile, mode, since: 0 }),
        );
      yield* marker(11, "/s.jsonl", "rpc"); // T3's own kind of process
      yield* marker(12, "/other.jsonl", "tui");
      yield* marker(13, "/s.jsonl", "tui"); // exited without cleaning up
      const alive = (pid: number) => pid !== 13;
      expect(yield* piTuiWriter(agentDir, "/s.jsonl", alive)).toBeUndefined();
      yield* marker(14, "/s.jsonl", "tui");
      expect(yield* piTuiWriter(agentDir, "/s.jsonl", alive)).toEqual({ pid: 14 });
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );
});
