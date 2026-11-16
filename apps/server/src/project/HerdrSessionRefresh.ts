import {
  CommandId,
  EventId,
  ThreadId,
  isOrchestrationV2WorkActive,
  type HerdrSessionReimport,
  type OrchestrationV2AppThread,
  type OrchestrationV2DomainEvent,
  type OrchestrationV2ThreadProjection,
} from "@t3tools/contracts";
import * as Crypto from "effect/Crypto";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as SqlClient from "effect/sql/SqlClient";
import * as EventSink from "../orchestration-v2/EventSink.ts";
import * as ProjectionStore from "../orchestration-v2/ProjectionStore.ts";
import * as ThreadCommandExecutor from "../orchestration-v2/ThreadCommandExecutor.ts";
import * as ProviderSessionRuntime from "../persistence/ProviderSessionRuntime.ts";
import { readHerdrOmpFile, type HerdrOmpRecentThread } from "./HerdrOmpSessions.ts";

type Importable = Extract<HerdrOmpRecentThread, { _tag: "Importable" }>;
type WorkState = Pick<
  OrchestrationV2ThreadProjection,
  "thread" | "runs" | "providerThreads" | "providerSessions" | "subagents"
>;
export type HerdrSessionRefreshServices =
  | ThreadCommandExecutor.ThreadCommandExecutor
  | ProjectionStore.ProjectionStoreV2
  | EventSink.EventSinkV2
  | ProviderSessionRuntime.ProviderSessionRuntimeRepository
  | FileSystem.FileSystem
  | Path.Path
  | Crypto.Crypto
  | SqlClient.SqlClient;

export function herdrRefreshBlockedReason(projection: WorkState): string | null {
  if (projection.thread.deletedAt !== null)
    return "This thread was deleted; it cannot be reimported.";
  if (
    projection.runs.some(
      (run) =>
        !["completed", "failed", "interrupted", "cancelled", "rolled_back"].includes(run.status),
    ) ||
    projection.providerSessions.some((session) =>
      ["starting", "running", "waiting"].includes(session.status),
    ) ||
    projection.providerThreads.some((thread) => (thread.pendingBackgroundTasks?.length ?? 0) > 0) ||
    projection.subagents.some((agent) => isOrchestrationV2WorkActive(agent.status))
  ) {
    return "Stop this thread and its background work before reimporting from Herdr.";
  }
  const active = projection.providerThreads.find(
    (thread) => thread.id === projection.thread.activeProviderThreadId,
  );
  if (active?.driver !== "omp")
    return "This thread is no longer using OMP; its current conversation has not been replaced.";
  return null;
}

type EventPayloads = { [Event in OrchestrationV2DomainEvent as Event["type"]]: Event["payload"] };
function savedHistoryEvents(
  projection: OrchestrationV2ThreadProjection,
  saved: OrchestrationV2AppThread,
  nativeFile: string,
  revision: string,
  at: DateTime.Utc,
): OrchestrationV2DomainEvent[] {
  const events: OrchestrationV2DomainEvent[] = [];
  const emit = <K extends OrchestrationV2DomainEvent["type"]>(
    type: K,
    payload: EventPayloads[K],
  ) => {
    events.push({
      id: EventId.make(`${revision}:saved:${events.length}`),
      type,
      threadId: saved.id,
      occurredAt: at,
      payload,
    } as OrchestrationV2DomainEvent);
  };
  const owned = <T extends object>(value: T): T => ({
    ...value,
    ...("threadId" in value ? { threadId: saved.id } : {}),
  });
  emit("thread.created", saved);
  for (const row of projection.runs) emit("run.created", owned(row));
  for (const row of projection.attempts) emit("run-attempt.created", owned(row));
  for (const row of projection.nodes) emit("node.updated", owned(row));
  for (const row of projection.subagents) emit("subagent.updated", owned(row));
  for (const row of projection.providerSessions)
    emit("provider-session.updated", { ...row, status: "stopped" });
  for (const row of projection.providerThreads)
    emit("provider-thread.updated", {
      ...row,
      appThreadId: saved.id,
      providerSessionId: null,
      ownerNodeId: null,
      ...(row.id === saved.activeProviderThreadId
        ? {
            nativeThreadRef: {
              driver: row.driver,
              nativeId: nativeFile,
              strength: "strong" as const,
            },
            nativeConversationHeadRef: null,
          }
        : {}),
    });
  for (const row of projection.providerTurns) emit("provider-turn.updated", owned(row));
  for (const row of projection.runtimeRequests) emit("runtime-request.updated", owned(row));
  for (const row of projection.messages) emit("message.updated", owned(row));
  for (const row of projection.plans) emit("plan.updated", owned(row));
  for (const row of projection.turnItems) emit("turn-item.updated", owned(row));
  for (const row of projection.checkpointScopes) emit("checkpoint-scope.created", owned(row));
  for (const row of projection.checkpoints) emit("checkpoint.captured", owned(row));
  for (const row of projection.contextHandoffs) emit("context-handoff.updated", owned(row));
  for (const row of projection.contextTransfers)
    emit("context-transfer.updated", {
      ...row,
      sourceThreadId: row.sourceThreadId === projection.thread.id ? saved.id : row.sourceThreadId,
      targetThreadId: row.targetThreadId === projection.thread.id ? saved.id : row.targetThreadId,
    });
  return events;
}

export const refreshHerdrSession = Effect.fn("refreshHerdrSession")(function* (input: {
  readonly threadId: ThreadId;
  readonly outcome: Importable;
  readonly confirmation: HerdrSessionReimport;
  readonly snapshotDirectory: string;
  readonly workspaceRoot: string;
  readonly events: (
    thread: OrchestrationV2AppThread,
    nativeFile: string,
    revision: string,
    at: DateTime.Utc,
  ) => ReadonlyArray<OrchestrationV2DomainEvent>;
}) {
  const executor = yield* ThreadCommandExecutor.ThreadCommandExecutor;
  return yield* executor.withLock(
    input.threadId,
    Effect.gen(function* () {
      const projections = yield* ProjectionStore.ProjectionStoreV2;
      const sink = yield* EventSink.EventSinkV2;
      const runtimes = yield* ProviderSessionRuntime.ProviderSessionRuntimeRepository;
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const crypto = yield* Crypto.Crypto;
      const sql = yield* SqlClient.SqlClient;
      const projection = yield* projections.getThreadProjection(input.threadId);
      const blocked = herdrRefreshBlockedReason(projection);
      if (blocked !== null) return { blockedReason: blocked };
      const currentRuntime = yield* runtimes.getByThreadId({ threadId: input.threadId });
      if (Option.isNone(currentRuntime) || currentRuntime.value.providerName !== "omp")
        return {
          blockedReason: "The OMP resume context is unavailable; this thread has not been changed.",
        };
      const complete = (bytes: Uint8Array) => bytes.subarray(0, bytes.lastIndexOf(10) + 1);
      const source = complete(
        (yield* readHerdrOmpFile(input.outcome.herdr.originalFilePath)).bytes,
      );
      const working = complete((yield* readHerdrOmpFile(input.outcome.source.filePath)).bytes);
      const version = (bytes: Uint8Array) =>
        crypto
          .digest("SHA-256", bytes)
          .pipe(Effect.map((hash) => Buffer.from(hash).toString("hex")));
      if (
        input.confirmation.sourceVersion !== input.outcome.herdr.sourceVersion ||
        input.confirmation.workingVersion !== input.outcome.herdr.workingVersion ||
        (yield* version(source)) !== input.confirmation.sourceVersion ||
        (yield* version(working)) !== input.confirmation.workingVersion
      ) {
        return {
          blockedReason:
            "The conversation changed after confirmation. Import from Herdr again to review the latest version.",
        };
      }
      const revision = yield* crypto.randomUUIDv4();
      const at = yield* DateTime.now;
      const directory = yield* fs.realPath(input.snapshotDirectory);
      const nativeFile = path.join(directory, `${revision}.jsonl`);
      const saveHistory =
        input.outcome.herdr.relationship !== "herdr_ahead" || projection.runs.length > 0;
      const savedId = saveHistory
        ? ThreadId.make(`${input.threadId}:before-herdr:${revision}`)
        : null;
      const savedNativeFile = path.join(directory, `${revision}-t3.jsonl`);
      const saved: OrchestrationV2AppThread | null =
        savedId === null
          ? null
          : {
              ...projection.thread,
              id: savedId,
              title: `${projection.thread.title} (before Herdr reimport)`,
              lineage: {
                rootThreadId: projection.thread.lineage.rootThreadId,
                parentThreadId: input.threadId,
                relationshipToParent: "fork",
              },
              forkedFrom: null,
              pullRequests: projection.thread.pullRequests?.map(
                ({ watch: _watch, ...link }) => link,
              ),
              guestShare: null,
              archivedAt: null,
              deletedAt: null,
              pinnedAt: null,
              pinOrderKey: null,
              settledOverride: "settled",
              settledAt: at,
              snoozedAt: null,
              snoozedUntil: null,
              lastVisitedAt: null,
              opens: null,
              createdAt: at,
              updatedAt: at,
            };
      let committed = false;
      const created: string[] = [];
      const writeCopy = (filePath: string, contents: Uint8Array) =>
        Effect.scoped(
          Effect.gen(function* () {
            const file = yield* fs.open(filePath, { flag: "wx", mode: 0o600 });
            created.push(filePath);
            yield* file.writeAll(contents);
          }),
        );
      return yield* Effect.gen(function* () {
        yield* writeCopy(nativeFile, source);
        if (saved !== null) yield* writeCopy(savedNativeFile, working);
        const replacementEvents = input.events(projection.thread, nativeFile, revision, at);
        const reset = replacementEvents[0];
        if (reset?.type !== "thread.history-reset")
          return yield* Effect.die("Herdr refresh must reset the old history first");
        const events = [
          reset,
          ...(saved === null
            ? []
            : savedHistoryEvents(projection, saved, savedNativeFile, revision, at)),
          ...replacementEvents.slice(1),
        ];
        if (saved !== null) {
          const children = yield* sql<{
            readonly threadId: string;
          }>`SELECT thread_id AS threadId FROM orchestration_v2_projection_threads WHERE json_extract(payload_json, '$.forkedFrom.threadId') = ${input.threadId}`;
          for (const child of children)
            events.push({
              id: EventId.make(`${revision}:fork:${child.threadId}`),
              type: "thread.fork-source-updated",
              threadId: ThreadId.make(child.threadId),
              occurredAt: at,
              payload: { fromThreadId: input.threadId, toThreadId: saved.id },
            });
        }
        const oldRuntime = currentRuntime.value;
        yield* sink.writeWithEffects({
          events,
          effects: projection.providerSessions.map((session) => ({
            id: `effect:${revision}:detach:${session.id}`,
            commandId: CommandId.make(`herdr-reimport:${revision}`),
            threadId: input.threadId,
            request: {
              type: "provider-session.detach" as const,
              providerSessionId: session.id,
              detail: "Conversation reimported from Herdr.",
            },
          })),
          persist: Effect.gen(function* () {
            yield* runtimes.upsert({
              ...oldRuntime,
              status: "stopped",
              lastSeenAt: DateTime.formatIso(at),
              resumeCursor: { sessionFile: nativeFile },
              runtimePayload: { cwd: input.workspaceRoot },
            });
            yield* runtimes.recordImportedTranscript({
              threadId: input.threadId,
              source: { ...input.outcome.source, filePath: nativeFile },
            });
            if (saved !== null)
              yield* runtimes.upsert({
                ...oldRuntime,
                threadId: saved.id,
                status: "stopped",
                lastSeenAt: DateTime.formatIso(at),
                resumeCursor: { sessionFile: savedNativeFile },
                runtimePayload: { cwd: input.workspaceRoot },
              });
          }),
        });
        committed = true;
        return { blockedReason: null, preservedThreadId: savedId };
      }).pipe(
        Effect.uninterruptible,
        Effect.ensuring(
          Effect.suspend(() =>
            committed
              ? Effect.void
              : Effect.forEach(created, (filePath) => fs.remove(filePath).pipe(Effect.ignore), {
                  discard: true,
                }),
          ),
        ),
      );
    }),
  );
});
