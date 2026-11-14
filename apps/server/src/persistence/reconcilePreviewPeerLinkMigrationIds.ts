import * as Effect from "effect/Effect";
import * as Migrator from "effect/sql/Migrator";
import * as SqlClient from "effect/sql/SqlClient";

import ShellStopRequestIndex from "./Migrations/068_ShellStopRequestIndex.ts";
import ThreadMessageSearch from "./Migrations/069_ThreadMessageSearch.ts";

type History = ReadonlyArray<readonly [number, string]>;
const previewTail: History = [
  [68, "PeerLinks"],
  [69, "PeerLinkFence"],
  [70, "WaitingLinkRequestIndex"],
];
const badState = (reason: string) =>
  new Migrator.MigrationError({
    kind: "BadState",
    message: `Cannot reconcile preview peer-link migrations: ${reason}.`,
  });
const normalizeSql = (value: string) => value.toLowerCase().replace(/[\s"`[\]]/g, "");

// The preview used 68..70 before main reserved 68/69. Do not infer a repair
// from a maximum id: only this complete history and schema are safe to move.
export const reconcilePreviewPeerLinkMigrationIds = Effect.fn(
  "reconcilePreviewPeerLinkMigrationIds",
)(function* (manifest: History, toMigrationInclusive?: number) {
  const sql = yield* SqlClient.SqlClient;
  return yield* sql.withTransaction(
    Effect.gen(function* () {
      const tables = yield* sql`
        SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'effect_sql_migrations'
      `;
      if (tables.length === 0) return [];
      const history = yield* sql<{ migration_id: number; name: string }>`
        SELECT migration_id, name FROM effect_sql_migrations WHERE migration_id >= 55 ORDER BY migration_id
      `;
      if (
        !history.some(
          (row) =>
            (row.name === "PeerLinks" ||
              row.name === "PeerLinkFence" ||
              row.name === "WaitingLinkRequestIndex") &&
            row.migration_id < 70,
        )
      )
        return [];
      const expected = [...manifest.filter(([id]) => id >= 55 && id <= 67), ...previewTail];
      const sameHistory = (rows: typeof history, entries: History) =>
        rows.length === entries.length &&
        rows.every(
          (row, i) => row.migration_id === entries[i]?.[0] && row.name === entries[i]?.[1],
        );
      if (!sameHistory(history, expected)) {
        return yield* badState(
          "expected exactly main 55..67 and preview 68 PeerLinks, 69 PeerLinkFence, 70 WaitingLinkRequestIndex; older 63/64 and partial histories are unsupported",
        );
      }
      if (toMigrationInclusive !== undefined && toMigrationInclusive < 72) {
        return yield* badState("the preview repair requires a migration ceiling of at least 72");
      }
      const columns = yield* sql<{
        name: string;
        type: string;
        notnull: number;
        pk: number;
        dflt_value: string | null;
      }>`
        PRAGMA table_info(peer_environment_links)
      `;
      const requiredColumns = [
        ["environment_id", "TEXT", 0, 1, null],
        ["secret_name", "TEXT", 1, 0, null],
        ["label", "TEXT", 1, 0, null],
        ["urls_json", "TEXT", 1, 0, null],
        ["access", "TEXT", 1, 0, null],
        ["linked_at", "TEXT", 1, 0, null],
        ["expires_at", "TEXT", 1, 0, null],
        ["last_reached_at", "TEXT", 0, 0, null],
        ["last_error", "TEXT", 0, 0, null],
        ["fenced", "INTEGER", 1, 0, "0"],
      ] as const;
      if (
        columns.length !== requiredColumns.length ||
        requiredColumns.some(([name, type, notnull, pk, defaultValue]) => {
          const column = columns.find((candidate) => candidate.name === name);
          return (
            !column ||
            column.type.toUpperCase() !== type ||
            column.notnull !== notnull ||
            column.pk !== pk ||
            column.dflt_value !== defaultValue
          );
        })
      ) {
        return yield* badState("peer_environment_links does not have the expected fenced schema");
      }
      const objects = yield* sql<{ name: string; type: string; sql: string | null }>`
        SELECT name, type, sql FROM sqlite_master
        WHERE name = 'orchestration_v2_projection_turn_items_waiting_link_idx'
          OR name = 'orchestration_v2_projection_turn_items_stop_request_idx'
          OR name LIKE 'orchestration_v2_message_search%'
      `;
      const waiting = objects.find(
        (object) => object.name === "orchestration_v2_projection_turn_items_waiting_link_idx",
      );
      const expectedIndex =
        "CREATE INDEX orchestration_v2_projection_turn_items_waiting_link_idx ON orchestration_v2_projection_turn_items(thread_id, updated_at DESC, turn_item_id DESC) WHERE type = 'link_request' AND status = 'waiting'";
      if (
        objects.length !== 1 ||
        waiting?.type !== "index" ||
        normalizeSql(waiting.sql ?? "") !== normalizeSql(expectedIndex)
      ) {
        return yield* badState(
          "expected the waiting-link index and no main stop-request or message-search objects",
        );
      }
      for (const [id, name] of [...previewTail].reverse()) {
        yield* sql`UPDATE effect_sql_migrations SET migration_id = ${id + 2} WHERE migration_id = ${id} AND name = ${name}`;
      }
      // Run the real Effects, including the FTS backfill, in the same transaction
      // as the history moves. Any DDL, backfill or history failure rolls all of it back.
      yield* ShellStopRequestIndex;
      yield* ThreadMessageSearch;
      yield* sql`INSERT INTO effect_sql_migrations (migration_id, name) VALUES (68, 'ShellStopRequestIndex'), (69, 'ThreadMessageSearch')`;
      const repaired = yield* sql<{ migration_id: number; name: string }>`
        SELECT migration_id, name FROM effect_sql_migrations WHERE migration_id >= 55 ORDER BY migration_id
      `;
      if (
        !sameHistory(
          repaired,
          manifest.filter(([id]) => id >= 55 && id <= 72),
        )
      ) {
        return yield* badState("post-repair history is not canonical 55..72");
      }
      return [
        [68, "ShellStopRequestIndex"],
        [69, "ThreadMessageSearch"],
      ] as const;
    }),
  );
});
