import * as Effect from "effect/Effect";
import * as Migrator from "effect/sql/Migrator";
import * as SqlClient from "effect/sql/SqlClient";

import ActiveTurnItemIndexes from "./Migrations/073_ActiveTurnItemIndexes.ts";

type MigrationRow = { readonly id: number; readonly name: string };

export function reconciledLegacyThreadFollowUpHistory(
  history: ReadonlyArray<MigrationRow>,
): ReadonlyArray<MigrationRow> | null {
  const legacy = history.find((row) => row.id === 73 && row.name === "ThreadFollowUp");
  if (legacy === undefined) return history;
  if (history.some((row) => row.id > 73)) return null;
  return history.map((row) => (row === legacy ? { id: 73, name: "ActiveTurnItemIndexes" } : row));
}

// Startup runs this and its history assertion in one transaction.
export const reconcileLegacyThreadFollowUpMigration = Effect.fn(
  "reconcileLegacyThreadFollowUpMigration",
)(function* () {
  const sql = yield* SqlClient.SqlClient;
  const tables = yield* sql`
    SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'effect_sql_migrations'
  `;
  if (tables.length === 0) return [];
  const rows = yield* sql<{ readonly migration_id: number; readonly name: string }>`
    SELECT migration_id, name FROM effect_sql_migrations WHERE migration_id >= 73
  `;
  const history = rows.map((row) => ({ id: row.migration_id, name: row.name }));
  const reconciled = reconciledLegacyThreadFollowUpHistory(history);
  if (reconciled === history) return [];
  if (reconciled === null) {
    return yield* new Migrator.MigrationError({
      kind: "BadState",
      message: "Cannot upgrade legacy ThreadFollowUp with unexpected later migrations.",
    });
  }
  // Moving this row to 97 would make the migrator skip upstream schema 73–96.
  yield* ActiveTurnItemIndexes;
  yield* sql`
    UPDATE effect_sql_migrations SET name = 'ActiveTurnItemIndexes'
    WHERE migration_id = 73 AND name = 'ThreadFollowUp'
  `;
  return [[73, "ActiveTurnItemIndexes"] as const];
});
