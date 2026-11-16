import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it } from "@effect/vitest";
import { ThreadId } from "@t3tools/contracts";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Path from "effect/Path";
import * as Stream from "effect/Stream";
import * as SqlClient from "effect/sql/SqlClient";

import * as EventStore from "../../orchestration-v2/EventStore.ts";
import * as ProjectionStore from "../../orchestration-v2/ProjectionStore.ts";
import { migrationManifest, runMigrations } from "../Migrations.ts";
import ThreadFollowUp from "./097_ThreadFollowUp.ts";

const at = "2026-11-04T00:00:00.000Z";
const fixtures = [
  { id: "legacy-waiting", marker: { waitingOnOthers: true }, expected: "waiting-on-others" },
  { id: "legacy-dismissed", marker: { waitingOnOthers: false }, expected: null },
  { id: "unmarked", marker: {}, expected: null },
  {
    id: "canonical-blue",
    marker: { waitingOnOthers: true, followUp: "circle-back" },
    expected: "circle-back",
  },
  { id: "canonical-dismissed", marker: { waitingOnOthers: true, followUp: null }, expected: null },
  {
    id: "canonical-waiting",
    marker: { followUp: "waiting-on-others" },
    expected: "waiting-on-others",
  },
] as const;
const thread = (fixture: (typeof fixtures)[number]) => ({
  id: fixture.id,
  projectId: "project-1",
  title: fixture.id,
  providerInstanceId: "codex",
  modelSelection: { instanceId: "codex", model: "gpt-5.4" },
  runtimeMode: "full-access",
  interactionMode: "default",
  createdBy: "user",
  creationSource: "web",
  branch: null,
  worktreePath: null,
  activeProviderThreadId: null,
  lineage: { parentThreadId: null, relationshipToParent: null, rootThreadId: fixture.id },
  forkedFrom: null,
  createdAt: at,
  updatedAt: at,
  archivedAt: null,
  deletedAt: null,
  ...fixture.marker,
});

it.effect.each(["upstream", "legacy-fork"] as const)(
  "migrates saved follow-ups once from %s and preserves them across restart and historical replay",
  (origin) =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const filename = path.join(
        yield* fs.makeTempDirectoryScoped({ prefix: "t3-follow-up-" }),
        "statev2.sqlite",
      );
      const database = NodeSqliteClient.layer({ filename });
      yield* Effect.gen(function* () {
        const sql = yield* SqlClient.SqlClient;
        yield* runMigrations({ toMigrationInclusive: origin === "legacy-fork" ? 72 : 96 });
        for (const fixture of fixtures) {
          const payload = JSON.stringify(thread(fixture));
          yield* sql`
          INSERT INTO orchestration_v2_projection_threads (
            thread_id, project_id, title, default_provider, provider_instance_id,
            runtime_mode, interaction_mode, created_at, updated_at, payload_json
          ) VALUES (
            ${fixture.id}, 'project-1', ${fixture.id}, 'codex', 'codex',
            'full-access', 'default', ${at}, ${at}, ${payload}
          )
        `;
          for (const [version, type] of [
            "thread.created",
            "thread.metadata-updated",
            "thread.archived",
            "thread.unarchived",
            "thread.visited",
          ].entries()) {
            yield* sql`
            INSERT INTO orchestration_events (
              event_id, aggregate_kind, stream_id, stream_version, event_type,
              occurred_at, actor_kind, payload_json, metadata_json, application_event_version
            ) VALUES (
              ${`${fixture.id}:${version}`}, 'thread', ${fixture.id}, ${version}, ${type},
              ${at}, 'user', ${payload}, '{}', 2
            )
          `;
          }
        }
        const v1Payload = JSON.stringify({ waitingOnOthers: true });
        yield* sql`
        INSERT INTO orchestration_events (
          event_id, aggregate_kind, stream_id, stream_version, event_type,
          occurred_at, actor_kind, payload_json, metadata_json, application_event_version
        ) VALUES ('legacy-v1', 'thread', 'legacy-v1', 0, 'thread.created',
          ${at}, 'user', ${v1Payload}, '{}', 1)
      `;
        if (origin === "legacy-fork") {
          yield* ThreadFollowUp;
          yield* sql`INSERT INTO effect_sql_migrations (migration_id, name) VALUES (73, 'ThreadFollowUp')`;
        }
        assert.deepEqual(
          yield* runMigrations(),
          migrationManifest.filter(([id]) => id >= (origin === "legacy-fork" ? 73 : 97)),
        );
        const rows = yield* sql<{ readonly thread_id: string; readonly payload_json: string }>`
        SELECT thread_id, payload_json FROM orchestration_v2_projection_threads
      `;
        for (const fixture of fixtures) {
          const { waitingOnOthers: _legacy, ...expected } = thread(fixture) as Record<
            string,
            unknown
          >;
          if ("waitingOnOthers" in fixture.marker && !("followUp" in fixture.marker)) {
            expected.followUp = fixture.expected;
          }
          const row = rows.find((row) => row.thread_id === fixture.id)!;
          assert.deepEqual(JSON.parse(row.payload_json), expected);
          const events = yield* sql<{ readonly payload_json: string }>`
          SELECT payload_json FROM orchestration_events
          WHERE application_event_version = 2 AND stream_id = ${fixture.id}
        `;
          assert.lengthOf(events, 5);
          for (const event of events) assert.deepEqual(JSON.parse(event.payload_json), expected);
        }
        const [v1] = yield* sql<{ readonly payload_json: string }>`
        SELECT payload_json FROM orchestration_events WHERE event_id = 'legacy-v1'
      `;
        assert.equal(v1?.payload_json, v1Payload);
      }).pipe(Effect.provide(database));

      // A fresh connection and fresh stores exercise the next startup, not cached reads.
      yield* Effect.gen(function* () {
        assert.deepEqual(yield* runMigrations(), []);
        const projections = yield* ProjectionStore.ProjectionStoreV2;
        const checkThreads = Effect.gen(function* () {
          for (const fixture of fixtures) {
            const id = ThreadId.make(fixture.id);
            const projection = yield* projections.getThreadProjection(id);
            const shell = yield* projections.getThreadShell(id);
            assert.equal(projection.thread.followUp ?? null, fixture.expected);
            assert.equal(shell?.followUp, fixture.expected);
            assert.equal(DateTime.formatIso(projection.thread.updatedAt), at);
            assert.equal(projection.thread.runtimeMode, "full-access");
            assert.isNull(shell?.pendingRuntimeRequest);
          }
        });
        yield* checkThreads;
        const events = yield* (yield* EventStore.EventStoreV2).read().pipe(
          Stream.map((stored) => stored.event),
          Stream.runCollect,
        );
        assert.lengthOf(events, fixtures.length * 5);
        for (const fixture of fixtures) {
          const [created, ...remaining] = events.filter((event) => event.threadId === fixture.id);
          if (created?.type !== "thread.created") throw new Error("Expected saved thread creation");
          const replayed = remaining.reduce(
            ProjectionStore.applyToProjection,
            ProjectionStore.emptyProjection(created),
          );
          assert.equal(replayed.thread.followUp ?? null, fixture.expected);
        }
        assert.deepEqual(yield* runMigrations(), []);
      }).pipe(
        Effect.provide(
          Layer.mergeAll(EventStore.layer, ProjectionStore.layer).pipe(
            Layer.provideMerge(database),
          ),
        ),
      );
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
);
