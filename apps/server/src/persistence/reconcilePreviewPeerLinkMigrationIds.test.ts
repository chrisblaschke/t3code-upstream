import { assert, describe, it } from "@effect/vitest";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as SqlClient from "effect/sql/SqlClient";

import { migrationManifest, runMigrations } from "./Migrations.ts";
import PeerLinks from "./Migrations/070_PeerLinks.ts";
import PeerLinkFence from "./Migrations/071_PeerLinkFence.ts";
import WaitingLinkRequestIndex from "./Migrations/072_WaitingLinkRequestIndex.ts";
import { reconcilePreviewPeerLinkMigrationIds } from "./reconcilePreviewPeerLinkMigrationIds.ts";

const layer = NodeSqliteClient.layer({ filename: ":memory:" });
const history = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const rows = yield* sql<{
    migration_id: number;
    name: string;
  }>`SELECT migration_id, name FROM effect_sql_migrations ORDER BY migration_id`;
  return rows.map(({ migration_id, name }) => [migration_id, name] as const);
});
const snapshot = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  return {
    history: yield* history,
    schema: yield* sql`SELECT type, name, sql FROM sqlite_master ORDER BY name`,
  };
});
const preview = Effect.gen(function* () {
  yield* runMigrations({ toMigrationInclusive: 67 });
  yield* PeerLinks;
  yield* PeerLinkFence;
  yield* WaitingLinkRequestIndex;
  const sql = yield* SqlClient.SqlClient;
  yield* sql`INSERT INTO effect_sql_migrations (migration_id, name) VALUES (68, 'PeerLinks'), (69, 'PeerLinkFence'), (70, 'WaitingLinkRequestIndex')`;
  yield* sql`INSERT INTO peer_environment_links (environment_id, secret_name, label, urls_json, access, linked_at, expires_at, fenced) VALUES ('env', 'secret-ref', 'label', '[]', 'full-access', 'now', 'later', 1)`;
  yield* sql`INSERT INTO orchestration_v2_projection_messages (message_id, thread_id, role, streaming, created_at, updated_at, payload_json) VALUES ('before', 'thread', 'user', 0, 'now', 'now', '{"text":"orchard migration verification"}')`;
});

describe("preview peer-link migration ids", () => {
  it.effect("upgrades a pi176 ledger from 80 to 82 without replaying peer migrations", () =>
    Effect.gen(function* () {
      yield* runMigrations({ toMigrationInclusive: 80 });
      const sql = yield* SqlClient.SqlClient;
      yield* sql`INSERT INTO peer_environment_links (environment_id, secret_name, label, urls_json, access, linked_at, expires_at, fenced) VALUES ('env', 'secret-ref', 'label', '[]', 'full-access', 'now', 'later', 1)`;
      assert.deepStrictEqual(yield* runMigrations(), [
        [81, "ProjectionMessageSearchText"],
        [82, "TurnItemConversationIndex"],
      ]);
      assert.deepStrictEqual(yield* history, migrationManifest);
      assert.deepStrictEqual(yield* sql`SELECT fenced FROM peer_environment_links`, [
        { fenced: 1 },
      ]);
      // The new bodies also tolerate a missing tail ledger without resetting linked peers.
      yield* PeerLinkFence;
      yield* WaitingLinkRequestIndex;
      assert.deepStrictEqual(yield* sql`SELECT fenced FROM peer_environment_links`, [
        { fenced: 1 },
      ]);
      assert.deepStrictEqual(yield* sql`PRAGMA quick_check`, [{ quick_check: "ok" }]);
      const before = yield* snapshot;
      assert.deepStrictEqual(yield* runMigrations(), []);
      assert.deepStrictEqual(yield* snapshot, before);
    }).pipe(Effect.provide(layer)),
  );

  it.effect(
    "repairs before the V2 guard, preserves fencing, backfills search and is idempotent",
    () =>
      Effect.gen(function* () {
        yield* preview;
        // Pin this legacy-repair scenario; later migrations have their own tests.
        assert.deepStrictEqual(yield* runMigrations({ toMigrationInclusive: 77 }), [
          [68, "ShellStopRequestIndex"],
          [69, "ThreadMessageSearch"],
          [73, "ActiveTurnItemIndexes"],
          [74, "ExcludeAttachmentPlaceholdersFromSearch"],
          [75, "SubagentDeliveryState"],
          [76, "ProjectionProjectsPinArchive"],
          [77, "RepairBooleanSettledOverride"],
        ]);
        assert.deepStrictEqual(
          yield* history,
          migrationManifest.filter(([id]) => id <= 77),
        );
        const sql = yield* SqlClient.SqlClient;
        assert.deepStrictEqual(yield* sql`SELECT fenced FROM peer_environment_links`, [
          { fenced: 1 },
        ]);
        assert.lengthOf(
          yield* sql`SELECT rowid FROM orchestration_v2_message_search WHERE orchestration_v2_message_search MATCH 'orchard'`,
          1,
        );
        yield* sql`INSERT INTO orchestration_v2_projection_messages (message_id, thread_id, role, streaming, created_at, updated_at, payload_json) VALUES ('after', 'thread', 'user', 0, 'now', 'now', '{"text":"orchard fresh message"}')`;
        assert.lengthOf(
          yield* sql`SELECT rowid FROM orchestration_v2_message_search WHERE orchestration_v2_message_search MATCH 'orchard'`,
          2,
        );
        const before = yield* snapshot;
        assert.deepStrictEqual(yield* runMigrations({ toMigrationInclusive: 77 }), []);
        assert.deepStrictEqual(yield* snapshot, before);
      }).pipe(Effect.provide(layer)),
  );

  it.effect("upgrades the live canonical 72 ledger to 77 without replaying the repair", () =>
    Effect.gen(function* () {
      yield* runMigrations({ toMigrationInclusive: 72 });
      assert.deepStrictEqual(yield* reconcilePreviewPeerLinkMigrationIds(migrationManifest), []);
      assert.deepStrictEqual(yield* runMigrations({ toMigrationInclusive: 77 }), [
        [73, "ActiveTurnItemIndexes"],
        [74, "ExcludeAttachmentPlaceholdersFromSearch"],
        [75, "SubagentDeliveryState"],
        [76, "ProjectionProjectsPinArchive"],
        [77, "RepairBooleanSettledOverride"],
      ]);
      assert.deepStrictEqual(
        yield* history,
        migrationManifest.filter(([id]) => id <= 77),
      );
      const sql = yield* SqlClient.SqlClient;
      const indexes = yield* sql<{
        name: string;
      }>`SELECT name FROM sqlite_schema WHERE type = 'index' AND name LIKE 'orchestration_v2_projection_turn_items_active_%' ORDER BY name`;
      assert.deepStrictEqual(
        indexes.map(({ name }) => name),
        [
          "orchestration_v2_projection_turn_items_active_ordinal_idx",
          "orchestration_v2_projection_turn_items_active_run_idx",
        ],
      );
      assert.deepStrictEqual(yield* runMigrations({ toMigrationInclusive: 77 }), []);
    }).pipe(Effect.provide(layer)),
  );

  it.effect(
    "runs real 069 search triggers during bounded preview repair before 074 replaces them",
    () =>
      Effect.gen(function* () {
        yield* preview;
        yield* runMigrations({ toMigrationInclusive: 72 });
        const sql = yield* SqlClient.SqlClient;
        const matches = (term: string) =>
          sql`SELECT rowid FROM orchestration_v2_message_search WHERE orchestration_v2_message_search MATCH ${term}`;
        assert.lengthOf(yield* matches("orchard"), 1);
        yield* sql`UPDATE orchestration_v2_projection_messages SET payload_json = '{"text":"cedar updated"}' WHERE message_id = 'before'`;
        assert.lengthOf(yield* matches("orchard"), 0);
        assert.lengthOf(yield* matches("cedar"), 1);
        yield* sql`DELETE FROM orchestration_v2_projection_messages WHERE message_id = 'before'`;
        assert.lengthOf(yield* matches("cedar"), 0);
      }).pipe(Effect.provide(layer)),
  );

  it.effect("leaves fresh and canonical databases to the normal migrator", () =>
    Effect.gen(function* () {
      assert.deepStrictEqual(yield* reconcilePreviewPeerLinkMigrationIds(migrationManifest), []);
      yield* runMigrations();
      const before = yield* snapshot;
      assert.deepStrictEqual(yield* reconcilePreviewPeerLinkMigrationIds(migrationManifest), []);
      assert.deepStrictEqual(yield* snapshot, before);
    }).pipe(Effect.provide(layer)),
  );

  it.effect.each([0, 54, 67, 68, 69, 70, 71])(
    "refuses bounded ceiling %s without changing anything",
    (ceiling) =>
      Effect.gen(function* () {
        yield* preview;
        const before = yield* snapshot;
        const result = yield* Effect.exit(runMigrations({ toMigrationInclusive: ceiling }));
        assert.isTrue(Exit.isFailure(result));
        assert.include(String(result), "ceiling of at least 72");
        assert.deepStrictEqual(yield* snapshot, before);
      }).pipe(Effect.provide(layer)),
  );

  it.effect("rolls back DDL and history moves when inserting history fails", () =>
    Effect.gen(function* () {
      yield* preview;
      const sql = yield* SqlClient.SqlClient;
      yield* sql`CREATE TRIGGER fail_repair BEFORE INSERT ON effect_sql_migrations WHEN new.migration_id = 69 BEGIN SELECT RAISE(ABORT, 'injected failure'); END`;
      const before = yield* snapshot;
      assert.isTrue(Exit.isFailure(yield* Effect.exit(runMigrations())));
      assert.deepStrictEqual(yield* snapshot, before);
      assert.deepStrictEqual(yield* sql`SELECT fenced FROM peer_environment_links`, [
        { fenced: 1 },
      ]);
    }).pipe(Effect.provide(layer)),
  );

  const invalidCases = [
    ["missing history", "DELETE FROM effect_sql_migrations WHERE migration_id = 69"],
    ["wrong name", "UPDATE effect_sql_migrations SET name = 'Unexpected' WHERE migration_id = 67"],
    [
      "future history",
      "INSERT INTO effect_sql_migrations (migration_id, name) VALUES (73, 'Future')",
    ],
    [
      "old 63/64 shape",
      "DELETE FROM effect_sql_migrations WHERE migration_id IN (63,64); UPDATE effect_sql_migrations SET migration_id = 63 WHERE migration_id = 68; UPDATE effect_sql_migrations SET migration_id = 64 WHERE migration_id = 69",
    ],
    ["unfenced schema", "ALTER TABLE peer_environment_links DROP COLUMN fenced"],
    ["missing waiting index", "DROP INDEX orchestration_v2_projection_turn_items_waiting_link_idx"],
    [
      "wrong waiting predicate",
      "DROP INDEX orchestration_v2_projection_turn_items_waiting_link_idx; CREATE INDEX orchestration_v2_projection_turn_items_waiting_link_idx ON orchestration_v2_projection_turn_items(thread_id)",
    ],
    [
      "existing stop index",
      "CREATE INDEX orchestration_v2_projection_turn_items_stop_request_idx ON orchestration_v2_projection_turn_items(thread_id)",
    ],
    ["existing search objects", "CREATE TABLE orchestration_v2_message_search(text TEXT)"],
  ] as const;
  it.effect.each(invalidCases)("rejects %s without mutation", ([, change]) =>
    Effect.gen(function* () {
      yield* preview;
      const sql = yield* SqlClient.SqlClient;
      for (const statement of change.split(";")) yield* sql.unsafe(statement);
      const before = yield* snapshot;
      assert.isTrue(Exit.isFailure(yield* Effect.exit(runMigrations())));
      assert.deepStrictEqual(yield* snapshot, before);
    }).pipe(Effect.provide(layer)),
  );
});
