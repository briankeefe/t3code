import * as NodeServices from "@effect/platform-node/NodeServices";
import * as HostProcess from "@t3tools/shared/HostProcess";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import * as ServerConfig from "../config.ts";
import * as ServerSettings from "../serverSettings.ts";
import { expect, it } from "@effect/vitest";
import {
  EventId,
  MessageId,
  TurnItemId,
  ProjectId,
  ProviderDriverKind,
  ProviderInstanceId,
  ThreadId,
  type ApplicationStoredEvent,
  type AgentSessionImportInput,
  type OrchestrationV2AppThread,
  type OrchestrationV2ProviderThread,
} from "@t3tools/contracts";
import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Stream from "effect/Stream";
import * as SqlClient from "effect/sql/SqlClient";

import * as EventSink from "../orchestration-v2/EventSink.ts";
import * as EventStore from "../orchestration-v2/EventStore.ts";
import * as Orchestrator from "../orchestration-v2/Orchestrator.ts";
import * as ProjectionStore from "../orchestration-v2/ProjectionStore.ts";
import * as ThreadCommandExecutor from "../orchestration-v2/ThreadCommandExecutor.ts";
import {
  CLAUDE_MODEL_SELECTION,
  CODEX_MODEL_SELECTION,
} from "../orchestration-v2/testkit/fixtures/shared.ts";
import * as ProviderSessionRuntime from "../persistence/ProviderSessionRuntime.ts";
import * as SqlitePersistence from "../persistence/Sqlite.ts";
import * as AgentSessionImporter from "./AgentSessionImporter.ts";
import * as AgentSessionScanner from "./AgentSessionScanner.ts";
import * as ProjectService from "./ProjectService.ts";

const projectId = ProjectId.make("agent-session-import-project");
const timestamp = "2026-09-01T10:00:00.000Z";
const now = DateTime.makeUnsafe(timestamp);
const stores = Layer.mergeAll(EventStore.layer, ProjectionStore.layer).pipe(
  Layer.provideMerge(SqlitePersistence.layerMemory),
);
const persistence = Layer.mergeAll(
  stores,
  EventSink.layer.pipe(Layer.provide(stores)),
  ProviderSessionRuntime.layer.pipe(Layer.provide(SqlitePersistence.layerMemory)),
  IdAllocator.layer,
);

function candidate(
  modelSelection: typeof CODEX_MODEL_SELECTION,
  providerSessionId: string,
  provider: "codex" | "claudeAgent" = "codex",
) {
  const providerInstanceId = modelSelection.instanceId;
  return {
    _tag: "Importable" as const,
    source: {
      provider,
      providerInstanceId,
      providerSessionId,
      filePath: `/tmp/${providerInstanceId}/${providerSessionId}.jsonl`,
      size: 100,
      mtimeMs: 2,
      device: 3,
      inode: 4,
      birthtimeMs: 1,
    },
    thread: {
      source: provider,
      providerInstanceId,
      providerSessionId,
      title: "Imported thread",
      model: modelSelection.model,
      createdAt: timestamp,
      updatedAt: timestamp,
      messages: [
        { role: "user" as const, text: "Fix it", createdAt: timestamp },
        { role: "assistant" as const, text: "Fixed", createdAt: timestamp },
      ],
    },
  } satisfies AgentSessionScanner.AgentSessionRecentThread;
}

function importId(source: ReturnType<typeof candidate>) {
  return ThreadId.make(
    `import:${source.source.providerInstanceId}:${source.source.providerSessionId}`,
  );
}

const seedOwner = Effect.fn(function* (
  source: ReturnType<typeof candidate>,
  lifecycle: "active" | "archived" | "deleted" = "active",
) {
  const sink = yield* EventSink.EventSinkV2;
  const ids = yield* IdAllocator.IdAllocatorV2;
  const { providerInstanceId, providerSessionId, provider } = source.source;
  const threadId = ThreadId.make(`owner:${providerInstanceId}:${providerSessionId}`);
  const driver = ProviderDriverKind.make(provider);
  const providerThread: OrchestrationV2ProviderThread = {
    id: ids.derive.providerThread({
      driver,
      nativeThreadId: providerSessionId,
    }),
    driver,
    providerInstanceId,
    providerSessionId: null,
    appThreadId: threadId,
    ownerNodeId: null,
    nativeThreadRef: { driver, nativeId: providerSessionId, strength: "strong" },
    nativeConversationHeadRef: null,
    status: "idle",
    firstRunOrdinal: 1,
    lastRunOrdinal: 1,
    handoffIds: [],
    forkedFrom: null,
    createdAt: now,
    updatedAt: now,
  };
  const thread: OrchestrationV2AppThread = {
    createdBy: "user",
    creationSource: "web",
    id: threadId,
    projectId,
    title: "Native thread",
    providerInstanceId,
    modelSelection: { instanceId: providerInstanceId, model: source.thread.model },
    runtimeMode: "full-access",
    interactionMode: "default",
    branch: null,
    worktreePath: null,
    activeProviderThreadId: providerThread.id,
    lineage: { parentThreadId: null, relationshipToParent: null, rootThreadId: threadId },
    forkedFrom: null,
    createdAt: now,
    updatedAt: now,
    archivedAt: lifecycle === "archived" ? now : null,
    deletedAt: lifecycle === "deleted" ? now : null,
    settledOverride: null,
    settledAt: null,
    lastVisitedAt: null,
  };
  yield* sink.write({
    events: [
      {
        id: EventId.make(`${threadId}:created`),
        type: "thread.created",
        threadId,
        occurredAt: now,
        payload: thread,
      },
      {
        id: EventId.make(`${threadId}:provider`),
        type: "provider-thread.updated",
        threadId,
        occurredAt: now,
        payload: providerThread,
      },
    ],
  });
  return threadId;
});

const makeImporter = Effect.fn(function* (
  candidates: ReadonlyArray<ReturnType<typeof candidate>>,
  beforeTransaction: Effect.Effect<void> = Effect.void,
  input: Omit<AgentSessionImportInput, "projectId"> = {},
  workspaceRoot = "/workspace/project",
) {
  const config = yield* Effect.serviceOption(ServerConfig.ServerConfig);
  const settings = yield* Effect.serviceOption(ServerSettings.ServerSettingsService);
  const home = yield* HostProcess.HomeDirectory;
  const sql = yield* SqlClient.SqlClient;
  const events = yield* EventStore.EventStoreV2;
  const projections = yield* ProjectionStore.ProjectionStoreV2;
  const published: Array<ApplicationStoredEvent> = [];
  const withTransaction: typeof sql.withTransaction = (effect) =>
    beforeTransaction.pipe(Effect.andThen(sql.withTransaction(effect)));
  const importSql = new Proxy(sql, {
    get: (target, key, receiver) =>
      key === "withTransaction" ? withTransaction : Reflect.get(target, key, receiver),
  });
  const sinkLayer = Layer.fresh(EventSink.layer).pipe(
    Layer.provide(
      Layer.mergeAll(
        Layer.succeed(SqlClient.SqlClient, importSql),
        Layer.succeed(ProjectionStore.ProjectionStoreV2, projections),
        Layer.succeed(EventStore.EventStoreV2, {
          ...events,
          publishCommitted: (batch) =>
            events.publishCommitted(batch).pipe(
              Effect.tap(() =>
                Effect.sync(() => {
                  published.push(...batch);
                }),
              ),
            ),
        }),
      ),
    ),
  );
  const sink = Context.get(yield* Layer.build(sinkLayer), EventSink.EventSinkV2);
  const context = yield* Layer.build(
    AgentSessionImporter.layer.pipe(
      Layer.provide(
        Layer.mergeAll(
          Layer.succeed(EventSink.EventSinkV2, sink),
          Layer.succeed(ProjectionStore.ProjectionStoreV2, projections),
          Layer.succeed(SqlClient.SqlClient, importSql),
          ThreadCommandExecutor.layer,
          Option.isSome(config)
            ? Layer.succeed(ServerConfig.ServerConfig, config.value)
            : ServerConfig.layerTest("/workspace/project", { prefix: "t3-importer-test-" }),
          Option.isSome(settings)
            ? Layer.succeed(ServerSettings.ServerSettingsService, settings.value)
            : ServerSettings.layerTest(),
          Layer.succeed(HostProcess.HomeDirectory, home),
          Layer.succeed(IdAllocator.IdAllocatorV2, yield* IdAllocator.IdAllocatorV2),
          Layer.succeed(
            ProviderSessionRuntime.ProviderSessionRuntimeRepository,
            yield* ProviderSessionRuntime.ProviderSessionRuntimeRepository,
          ),
          Layer.mock(AgentSessionScanner.AgentSessionScanner)({
            recentThreads: () => Stream.fromIterable(candidates),
          }),
          Layer.mock(ProjectService.ProjectService)({
            getById: () =>
              Effect.succeedSome({
                id: projectId,
                title: "Project",
                workspaceRoot,
                defaultModelSelection: null,
                scripts: [],
                createdAt: timestamp,
                updatedAt: timestamp,
                deletedAt: null,
              }),
          }),
          Layer.mock(Orchestrator.OrchestratorV2)({
            getThreadRecords: (threadId, fields) =>
              projections
                .getThreadRecords(threadId, fields)
                .pipe(
                  Effect.mapError(
                    (cause) => new Orchestrator.OrchestratorProjectionError({ threadId, cause }),
                  ),
                ),
          }),
        ),
      ),
      Layer.provide(NodeServices.layer),
    ),
  );
  const importer = Context.get(context, AgentSessionImporter.AgentSessionImporter);
  return {
    run: importer.importRecentAgentThreads({ projectId, ...input }),
    import: importer.importRecentAgentThreads,
    write: sink.write,
    published,
  };
});

it.effect(
  "ignores owned sessions, including archived and deleted threads, but imports other instances and unowned sessions",
  () =>
    Effect.gen(function* () {
      const projections = yield* ProjectionStore.ProjectionStoreV2;
      const events = yield* EventStore.EventStoreV2;
      const runtimes = yield* ProviderSessionRuntime.ProviderSessionRuntimeRepository;
      const active = candidate(CODEX_MODEL_SELECTION, "active");
      const archived = candidate(
        CLAUDE_MODEL_SELECTION,
        "020e7f00-dce6-406b-8ec6-ed90498945d6",
        "claudeAgent",
      );
      const deleted = candidate(CODEX_MODEL_SELECTION, "deleted");
      const owners = [
        yield* seedOwner(active),
        yield* seedOwner(archived, "archived"),
        yield* seedOwner(deleted, "deleted"),
      ];
      const before = yield* Effect.forEach(owners, (id) =>
        projections.getThreadRecords(id, ["providerThreads"]),
      );
      const otherInstance = candidate(
        { ...CLAUDE_MODEL_SELECTION, instanceId: ProviderInstanceId.make("claude-other") },
        archived.source.providerSessionId,
        "claudeAgent",
      );
      const unowned = candidate(CODEX_MODEL_SELECTION, "unowned");
      const importer = yield* makeImporter([active, archived, deleted, otherInstance, unowned]);

      expect(yield* importer.run).toEqual({ importedCount: 2, skippedCount: 0 });
      for (const source of [active, archived, deleted]) {
        const threadId = importId(source);
        expect(yield* Effect.option(projections.getThread(threadId))).toEqual(Option.none());
        expect(yield* events.read({ threadId }).pipe(Stream.runCollect)).toEqual([]);
        expect(yield* runtimes.getByThreadId({ threadId })).toEqual(Option.none());
      }
      expect(
        yield* Effect.forEach(owners, (id) =>
          projections.getThreadRecords(id, ["providerThreads"]),
        ),
      ).toEqual(before);
      expect(
        new Set(
          importer.published.flatMap((stored) =>
            "event" in stored ? [stored.event.threadId] : [],
          ),
        ),
      ).toEqual(new Set([importId(otherInstance), importId(unowned)]));
      for (const source of [otherInstance, unowned]) {
        const threadId = importId(source);
        const records = yield* projections.getThreadRecords(threadId, [
          "messages",
          "providerThreads",
        ]);
        expect(records.messages.map((message) => message.text)).toEqual(["Fix it", "Fixed"]);
        expect(records.thread.historyOrigin).toBe("v1_import");
        expect(records.thread.activeProviderThreadId).toBe(records.providerThreads[0]?.id);
        expect(records.providerThreads[0]?.nativeThreadRef?.nativeId).toBe(
          source.source.providerSessionId,
        );
        expect(Option.getOrThrow(yield* runtimes.getByThreadId({ threadId }))).toMatchObject({
          providerInstanceId: source.source.providerInstanceId,
          resumeCursor:
            source.source.provider === "codex"
              ? { threadId: source.source.providerSessionId }
              : { threadId, resume: source.source.providerSessionId },
          runtimePayload: { importedTranscripts: [source.source] },
        });
      }
    }).pipe(Effect.scoped, Effect.provide(persistence)),
);

it.effect("rechecks ownership after the import reaches the commit transaction", () =>
  Effect.gen(function* () {
    const enteringTransaction = yield* Deferred.make<void>();
    const continueTransaction = yield* Deferred.make<void>();
    const source = candidate(CODEX_MODEL_SELECTION, "claimed-during-import");
    const importer = yield* makeImporter(
      [source],
      Deferred.succeed(enteringTransaction, undefined).pipe(
        Effect.andThen(Deferred.await(continueTransaction)),
      ),
    );
    const pending = yield* Effect.forkScoped(importer.run);
    yield* Deferred.await(enteringTransaction);
    // Commit ownership after all pre-transaction reads, before the import acquires SQLite.
    const ownerId = yield* seedOwner(source);
    yield* Deferred.succeed(continueTransaction, undefined);
    expect(yield* Fiber.join(pending)).toEqual({ importedCount: 0, skippedCount: 0 });
    expect(yield* importer.run).toEqual({ importedCount: 0, skippedCount: 0 });
    const projections = yield* ProjectionStore.ProjectionStoreV2;
    const events = yield* EventStore.EventStoreV2;
    const runtimes = yield* ProviderSessionRuntime.ProviderSessionRuntimeRepository;
    expect(yield* Effect.option(projections.getThread(importId(source)))).toEqual(Option.none());
    expect(yield* events.read({ threadId: importId(source) }).pipe(Stream.runCollect)).toEqual([]);
    expect(yield* runtimes.list()).toEqual([]);
    expect(importer.published).toEqual([]);
    expect(
      (yield* projections.getThreadRecords(ownerId, ["providerThreads"])).providerThreads[0]
        ?.appThreadId,
    ).toBe(ownerId);
  }).pipe(Effect.scoped, Effect.provide(persistence)),
);

it.effect(
  "retries failed commits and runtime persistence without replaying committed history",
  () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      const events = yield* EventStore.EventStoreV2;
      const projections = yield* ProjectionStore.ProjectionStoreV2;
      const runtimes = yield* ProviderSessionRuntime.ProviderSessionRuntimeRepository;
      const source = candidate(CODEX_MODEL_SELECTION, "retry");
      const threadId = importId(source);
      const importer = yield* makeImporter([source]);
      yield* sql`CREATE TEMP TRIGGER fail_import BEFORE INSERT ON orchestration_events WHEN NEW.event_type = 'message.updated' BEGIN SELECT RAISE(ABORT, 'test publication failure'); END`;
      expect(yield* importer.run).toEqual({ importedCount: 0, skippedCount: 1 });
      expect(yield* events.read({ threadId }).pipe(Stream.runCollect)).toEqual([]);
      expect(yield* Effect.option(projections.getThread(threadId))).toEqual(Option.none());
      expect(yield* runtimes.list()).toEqual([]);
      expect(importer.published).toEqual([]);
      yield* sql`DROP TRIGGER fail_import`;

      yield* sql`CREATE TEMP TRIGGER fail_runtime BEFORE INSERT ON provider_session_runtime BEGIN SELECT RAISE(ABORT, 'test runtime failure'); END`;
      expect(yield* importer.run).toEqual({ importedCount: 0, skippedCount: 1 });
      const committed = yield* events.read({ threadId }).pipe(Stream.runCollect);
      expect(committed).toHaveLength(6);
      expect(importer.published).toHaveLength(6);
      expect(yield* runtimes.list()).toEqual([]);
      yield* sql`DROP TRIGGER fail_runtime`;

      expect(yield* importer.run).toEqual({ importedCount: 1, skippedCount: 0 });
      expect(yield* importer.run).toEqual({ importedCount: 1, skippedCount: 0 });
      expect(yield* events.read({ threadId }).pipe(Stream.runCollect)).toEqual(committed);
      expect(importer.published).toHaveLength(6);
      expect(
        (yield* projections.getThreadRecords(threadId, ["messages"])).messages.map(
          (message) => message.text,
        ),
      ).toEqual(["Fix it", "Fixed"]);
      expect(Option.getOrThrow(yield* runtimes.getByThreadId({ threadId }))).toMatchObject({
        providerInstanceId: source.source.providerInstanceId,
        resumeCursor: { threadId: source.source.providerSessionId },
        runtimePayload: { importedTranscripts: [source.source] },
      });
    }).pipe(Effect.scoped, Effect.provide(persistence)),
);

it.effect(
  "imports private Herdr snapshots and reports divergence without overwriting local history",
  () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const home = yield* fs.makeTempDirectoryScoped();
      const workspaceRoot = path.join(home, "workspace");
      yield* fs.makeDirectory(workspaceRoot);
      const original = path.join(home, "native.jsonl");
      const registry = path.join(home, ".config", "herdr");
      const content =
        [
          { type: "session", version: 3, id: "synthetic-omp", cwd: workspaceRoot, timestamp },
          {
            type: "message",
            id: "user",
            parentId: null,
            timestamp,
            message: { role: "user", content: [{ type: "text", text: "Synthetic request" }] },
          },
          {
            type: "message",
            id: "assistant",
            parentId: "user",
            timestamp,
            message: { role: "assistant", content: [{ type: "text", text: "Synthetic response" }] },
          },
        ]
          .map((record) => JSON.stringify(record))
          .join("\n") + "\n";
      yield* fs.writeFileString(original, content);
      yield* fs.makeDirectory(registry, { recursive: true });
      yield* fs.writeFileString(
        path.join(registry, "session.json"),
        JSON.stringify({
          workspaces: [
            {
              tabs: [
                {
                  custom_name: "Synthetic title",
                  panes: {
                    main: {
                      cwd: workspaceRoot,
                      agent_session: {
                        source: "herdr:omp",
                        agent: "omp",
                        kind: "path",
                        value: original,
                      },
                    },
                  },
                },
              ],
            },
          ],
        }),
      );
      yield* Effect.gen(function* () {
        const importer = yield* makeImporter([], Effect.void, { source: "herdr" }, workspaceRoot);
        expect(yield* importer.run).toEqual({ importedCount: 1, skippedCount: 0 });
        const runtimes = yield* ProviderSessionRuntime.ProviderSessionRuntimeRepository;
        const rows = yield* runtimes.list();
        expect(rows).toHaveLength(1);
        const row = rows[0]!;
        expect(row.runtimeMode).toBe("approval-required");
        const cursor = row.resumeCursor;
        if (
          cursor === null ||
          typeof cursor !== "object" ||
          !("sessionFile" in cursor) ||
          typeof cursor.sessionFile !== "string"
        ) {
          throw new Error("Expected an OMP native sessionFile cursor");
        }
        const snapshot = cursor.sessionFile;
        expect(snapshot).not.toBe(original);
        expect(yield* fs.readFileString(snapshot)).toBe(content);
        const projections = yield* ProjectionStore.ProjectionStoreV2;
        const records = yield* projections.getThreadRecords(row.threadId, [
          "messages",
          "providerThreads",
        ]);
        expect(records.thread.runtimeMode).toBe("approval-required");
        expect(records.messages.map((message) => message.text)).toEqual([
          "Synthetic request",
          "Synthetic response",
        ]);
        expect(records.providerThreads[0]?.nativeThreadRef).toMatchObject({
          driver: "omp",
          nativeId: snapshot,
        });
        const events = yield* EventStore.EventStoreV2;
        let committed = yield* events.read({ threadId: row.threadId }).pipe(Stream.runCollect);
        expect(committed).toHaveLength(6);
        const continued =
          content +
          JSON.stringify({
            type: "message",
            id: "continued",
            parentId: "assistant",
            timestamp,
            message: { role: "user", content: [{ type: "text", text: "Synthetic continuation" }] },
          }) +
          "\n";
        yield* fs.writeFileString(snapshot, continued);
        const before = yield* projections.getThreadProjection(row.threadId);
        const firstUserItem = before.turnItems.find((item) => item.type === "user_message")!;
        yield* importer.write({
          events: [
            {
              id: EventId.make("t3-custom-title"),
              type: "thread.metadata-updated",
              threadId: row.threadId,
              occurredAt: now,
              payload: { ...before.thread, title: "My T3 title", pinnedAt: now },
            },
            {
              id: EventId.make("t3-local-message"),
              type: "message.updated",
              threadId: row.threadId,
              occurredAt: now,
              payload: {
                ...before.messages[0]!,
                id: MessageId.make("t3-local-message"),
                text: "Synthetic continuation",
              },
            },
            {
              id: EventId.make("t3-local-item"),
              type: "turn-item.updated",
              threadId: row.threadId,
              occurredAt: now,
              payload: {
                ...firstUserItem,
                id: TurnItemId.make("t3-local-item"),
                messageId: MessageId.make("t3-local-message"),
                text: "Synthetic continuation",
                ordinal: 3,
              },
            },
          ],
        });
        committed = yield* events.read({ threadId: row.threadId }).pipe(Stream.runCollect);
        expect(yield* importer.run).toMatchObject({
          importedCount: 1,
          skippedCount: 0,
          herdrChanges: [{ relationship: "t3_ahead" }],
        });
        expect(yield* events.read({ threadId: row.threadId }).pipe(Stream.runCollect)).toEqual(
          committed,
        );
        expect(yield* fs.readFileString(snapshot)).toBe(continued);
        expect(yield* runtimes.list()).toHaveLength(1);
        expect(yield* fs.readFileString(original)).toBe(content);
        yield* fs.writeFileString(
          original,
          content +
            JSON.stringify({
              type: "message",
              id: "herdr-continued",
              parentId: "assistant",
              timestamp,
              message: { role: "user", content: [{ type: "text", text: "Herdr continuation" }] },
            }) +
            "\n",
        );
        const preview = yield* importer.run;
        expect(preview).toMatchObject({
          herdrChanges: [{ threadId: row.threadId, relationship: "diverged" }],
        });
        expect(yield* fs.readFileString(snapshot)).toBe(continued);
        expect(yield* events.read({ threadId: row.threadId }).pipe(Stream.runCollect)).toEqual(
          committed,
        );
        const sql = yield* SqlClient.SqlClient;
        const snapshotDirectory = path.dirname(snapshot);
        const filesBefore = yield* fs.readDirectory(snapshotDirectory);
        const activeProvider = records.providerThreads[0]!;
        yield* importer.write({
          events: [
            {
              id: EventId.make("herdr-busy"),
              type: "provider-thread.updated",
              threadId: row.threadId,
              occurredAt: now,
              payload: {
                ...activeProvider,
                pendingBackgroundTasks: [{ kind: "subagent", taskId: "busy-child" }],
              },
            },
          ],
        });
        const busy = yield* importer.import({
          projectId,
          source: "herdr",
          reimport: preview.herdrChanges,
        });
        expect(busy.reimportedCount).toBeUndefined();
        expect(busy.herdrChanges?.[0]?.blockedReason).not.toBeNull();
        yield* importer.write({
          events: [
            {
              id: EventId.make("herdr-idle"),
              type: "provider-thread.updated",
              threadId: row.threadId,
              occurredAt: now,
              payload: activeProvider,
            },
          ],
        });
        committed = yield* events.read({ threadId: row.threadId }).pipe(Stream.runCollect);
        const stale = yield* importer.import({
          projectId,
          source: "herdr",
          reimport: [{ ...preview.herdrChanges![0]!, sourceVersion: "stale" }],
        });
        expect(stale.reimportedCount).toBeUndefined();
        expect(stale.herdrChanges?.[0]?.blockedReason).not.toBeNull();
        yield* sql`CREATE TRIGGER fail_herdr_refresh BEFORE UPDATE OF resume_cursor_json ON provider_session_runtime BEGIN SELECT RAISE(FAIL, 'reject refresh'); END`;
        const failed = yield* importer.import({
          projectId,
          source: "herdr",
          reimport: preview.herdrChanges,
        });
        expect(failed.reimportedCount).toBeUndefined();
        expect(failed.herdrChanges?.[0]?.blockedReason).not.toBeNull();
        expect(yield* events.read({ threadId: row.threadId }).pipe(Stream.runCollect)).toEqual(
          committed,
        );
        expect(yield* runtimes.list()).toHaveLength(1);
        expect((yield* fs.readDirectory(snapshotDirectory)).sort()).toEqual(filesBefore.sort());
        expect(yield* fs.readFileString(snapshot)).toBe(continued);
        yield* sql`DROP TRIGGER fail_herdr_refresh`;
        const refreshed = yield* importer.import({
          projectId,
          source: "herdr",
          reimport: preview.herdrChanges,
        });
        expect(refreshed).toMatchObject({ reimportedCount: 1 });
        const savedId = refreshed.preservedThreadIds![0]!;
        const refreshedRecords = yield* projections.getThreadRecords(row.threadId, [
          "messages",
          "providerThreads",
          "runs",
        ]);
        expect(refreshedRecords.messages.map((message) => message.text)).toEqual([
          "Synthetic request",
          "Synthetic response",
          "Herdr continuation",
        ]);
        expect(refreshedRecords.thread.title).toBe("My T3 title");
        expect(refreshedRecords.thread.pinnedAt).toEqual(now);
        expect(refreshedRecords.thread.projectId).toBe(records.thread.projectId);
        const refreshedRuntime = Option.getOrThrow(
          yield* runtimes.getByThreadId({ threadId: row.threadId }),
        );
        const sourceFile = refreshedRecords.providerThreads[0]!.nativeThreadRef!.nativeId!;
        expect(refreshedRuntime.resumeCursor).toEqual({ sessionFile: sourceFile });
        expect(yield* fs.readFileString(sourceFile)).toBe(yield* fs.readFileString(original));
        const savedRecords = yield* projections.getThreadRecords(savedId, [
          "messages",
          "providerThreads",
        ]);
        expect(savedRecords.messages.map((message) => message.text)).toEqual([
          "Synthetic request",
          "Synthetic response",
          "Synthetic continuation",
        ]);
        const savedRuntime = Option.getOrThrow(
          yield* runtimes.getByThreadId({ threadId: savedId }),
        );
        const savedFile = savedRecords.providerThreads[0]!.nativeThreadRef!.nativeId!;
        expect(savedRuntime.resumeCursor).toEqual({ sessionFile: savedFile });
        expect(yield* fs.readFileString(savedFile)).toBe(continued);
        const refreshedEvents = yield* events
          .read({ threadId: row.threadId })
          .pipe(Stream.runCollect);
        expect(yield* importer.run).toEqual({ importedCount: 1, skippedCount: 0 });
        expect(yield* events.read({ threadId: row.threadId }).pipe(Stream.runCollect)).toEqual(
          refreshedEvents,
        );
        expect(yield* runtimes.list()).toHaveLength(2);
      }).pipe(
        Effect.provide(
          Layer.mergeAll(
            Layer.succeed(HostProcess.HomeDirectory, home),
            ServerConfig.layerTest(home, path.join(home, "t3")),
            ServerSettings.layerTest({
              providerInstances: {
                [ProviderInstanceId.make("omp")]: {
                  driver: ProviderDriverKind.make("omp"),
                  enabled: true,
                },
              },
            }),
          ),
        ),
      );
    }).pipe(Effect.scoped, Effect.provide(Layer.mergeAll(persistence, NodeServices.layer))),
);
