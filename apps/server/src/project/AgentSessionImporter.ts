import {
  DEFAULT_MODEL,
  DEFAULT_MODEL_BY_PROVIDER,
  DEFAULT_PROVIDER_INTERACTION_MODE,
  DEFAULT_RUNTIME_MODE,
  AgentSessionImportProjectChangedError,
  AgentSessionImportProjectNotFoundError,
  AgentSessionImportSource,
  AgentSessionScanError,
  AgentSessionSource,
  EventId,
  MessageId,
  ProjectId,
  ProviderDriverKind,
  ProviderInstanceId,
  resolveProviderInstanceEnabled,
  ThreadId,
  TurnItemId,
  type AgentSessionImportInput,
  type AgentSessionImportResult,
  type HerdrSessionImportChange,
  type OrchestrationV2AppThread,
  type OrchestrationV2ConversationMessage,
  type OrchestrationV2DomainEvent,
  type OrchestrationV2ProviderThread,
  type OrchestrationV2TurnItem,
} from "@t3tools/contracts";
import { normalizeProjectPathForComparison } from "@t3tools/shared/path";
import * as Context from "effect/Context";
import * as Crypto from "effect/Crypto";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import * as HostProcess from "@t3tools/shared/HostProcess";
import * as ServerConfig from "../config.ts";
import * as ServerSettings from "../serverSettings.ts";
import {
  herdrOmpThreads,
  type HerdrOmpRecentThread,
  type HerdrOmpWorkingCopy,
} from "./HerdrOmpSessions.ts";
import {
  herdrRefreshBlockedReason,
  refreshHerdrSession,
  type HerdrSessionRefreshServices,
} from "./HerdrSessionRefresh.ts";
import * as ProjectionStore from "../orchestration-v2/ProjectionStore.ts";

import * as EventSink from "../orchestration-v2/EventSink.ts";
import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import * as Orchestrator from "../orchestration-v2/Orchestrator.ts";
import * as ProviderSessionRuntime from "../persistence/ProviderSessionRuntime.ts";
import * as AgentSessionScanner from "./AgentSessionScanner.ts";
import * as ProjectService from "./ProjectService.ts";

const IMPORT_EVENT_PREFIX = "agent-session-import:v2";
const CLAUDE_SESSION_ID_PATTERN =
  /^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;
const decodeImportedTranscriptPayload = Schema.decodeUnknownOption(
  Schema.Struct({
    cwd: Schema.optional(Schema.String),
    importedTranscripts: Schema.optional(Schema.Array(AgentSessionImportSource)),
  }),
);

class AgentSessionUnresumableSessionError extends Schema.TaggedError<AgentSessionUnresumableSessionError>()(
  "AgentSessionUnresumableSessionError",
  {
    source: AgentSessionSource,
    providerSessionId: Schema.String,
  },
) {
  override get message(): string {
    return `Session '${this.providerSessionId}' from '${this.source}' cannot be resumed.`;
  }
}

class AgentSessionThreadProjectConflictError extends Schema.TaggedError<AgentSessionThreadProjectConflictError>()(
  "AgentSessionThreadProjectConflictError",
  {
    threadId: ThreadId,
    expectedProjectId: ProjectId,
    actualProjectId: ProjectId,
  },
) {
  override get message(): string {
    return `Imported thread '${this.threadId}' belongs to project '${this.actualProjectId}', not '${this.expectedProjectId}'.`;
  }
}

class AgentSessionThreadModifiedError extends Schema.TaggedError<AgentSessionThreadModifiedError>()(
  "AgentSessionThreadModifiedError",
  { threadId: ThreadId },
) {
  override get message(): string {
    return `Imported thread '${this.threadId}' already contains non-imported activity.`;
  }
}

function dateTime(value: string): DateTime.Utc {
  return DateTime.makeUnsafe(value);
}

function messageEvents(input: {
  readonly threadId: ThreadId;
  readonly index: number;
  readonly message: AgentSessionScanner.AgentSessionThreadMessage;
  readonly revision?: string;
}): ReadonlyArray<OrchestrationV2DomainEvent> {
  const ordinal = input.index + 1;
  const suffix = String(input.index).padStart(6, "0");
  const identity =
    input.revision === undefined ? input.threadId : `${input.threadId}:${input.revision}`;
  const messageId = MessageId.make(`${identity}:${suffix}`);
  const turnItemId = TurnItemId.make(`${IMPORT_EVENT_PREFIX}:turn-item:${identity}:${suffix}`);
  const at = dateTime(input.message.createdAt);
  const message: OrchestrationV2ConversationMessage = {
    createdBy: input.message.role === "user" ? "user" : "agent",
    creationSource: "server",
    id: messageId,
    threadId: input.threadId,
    runId: null,
    nodeId: null,
    role: input.message.role,
    text: input.message.text,
    attachments: [],
    streaming: false,
    createdAt: at,
    updatedAt: at,
  };
  const common = {
    id: turnItemId,
    threadId: input.threadId,
    runId: null,
    nodeId: null,
    providerThreadId: null,
    providerTurnId: null,
    nativeItemRef: null,
    parentItemId: null,
    ordinal,
    status: "completed" as const,
    title: null,
    startedAt: at,
    completedAt: at,
    updatedAt: at,
  };
  const turnItem: OrchestrationV2TurnItem =
    input.message.role === "user"
      ? {
          ...common,
          createdBy: "user",
          creationSource: "server",
          type: "user_message",
          messageId,
          inputIntent: "turn_start",
          text: input.message.text,
          attachments: [],
        }
      : {
          ...common,
          type: "assistant_message",
          messageId,
          text: input.message.text,
          streaming: false,
        };
  return [
    {
      id: EventId.make(`${IMPORT_EVENT_PREFIX}:message:${identity}:${suffix}`),
      type: "message.updated",
      threadId: input.threadId,
      occurredAt: at,
      payload: message,
    },
    {
      id: EventId.make(`${IMPORT_EVENT_PREFIX}:turn-item:${identity}:${suffix}`),
      type: "turn-item.updated",
      threadId: input.threadId,
      occurredAt: at,
      payload: turnItem,
    },
  ];
}

const make = Effect.gen(function* () {
  const scanner = yield* AgentSessionScanner.AgentSessionScanner;
  const orchestrator = yield* Orchestrator.OrchestratorV2;
  const projects = yield* ProjectService.ProjectService;
  const eventSink = yield* EventSink.EventSinkV2;
  const idAllocator = yield* IdAllocator.IdAllocatorV2;
  const runtimes = yield* ProviderSessionRuntime.ProviderSessionRuntimeRepository;
  const fileSystem = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const crypto = yield* Crypto.Crypto;
  const config = yield* ServerConfig.ServerConfig;
  const settings = yield* ServerSettings.ServerSettingsService;
  const home = yield* HostProcess.HomeDirectory;
  const refreshContext = yield* Effect.context<HerdrSessionRefreshServices>();
  const projections = Context.get(refreshContext, ProjectionStore.ProjectionStoreV2);
  const importRecentAgentThreads = Effect.fn("importRecentAgentThreadsV2")(function* (
    input: AgentSessionImportInput,
  ) {
    const project = yield* projects.getById(input.projectId).pipe(
      Effect.mapError((cause) => new AgentSessionScanError({ operation: "read-projects", cause })),
      Effect.flatMap(
        Option.match({
          onNone: () =>
            Effect.fail(new AgentSessionImportProjectNotFoundError({ projectId: input.projectId })),
          onSome: Effect.succeed,
        }),
      ),
    );
    if (
      input.expectedWorkspaceRoot !== undefined &&
      normalizeProjectPathForComparison(project.workspaceRoot) !==
        normalizeProjectPathForComparison(input.expectedWorkspaceRoot)
    ) {
      return yield* new AgentSessionImportProjectChangedError({ projectId: input.projectId });
    }
    if (input.reimport !== undefined && input.source !== "herdr")
      return yield* new AgentSessionScanError({
        operation: "read-projects",
        cause: new Error("Reimport is available only for Herdr sessions."),
      });
    const runtimeRows = yield* runtimes
      .list()
      .pipe(
        Effect.mapError(
          (cause) => new AgentSessionScanError({ operation: "read-projects", cause }),
        ),
      );
    const completedSources = runtimeRows.flatMap((runtime) => {
      const payload = decodeImportedTranscriptPayload(runtime.runtimePayload);
      if (
        Option.isNone(payload) ||
        payload.value.cwd === undefined ||
        normalizeProjectPathForComparison(payload.value.cwd) !==
          normalizeProjectPathForComparison(project.workspaceRoot)
      ) {
        return [];
      }
      return payload.value.importedTranscripts ?? [];
    });
    const snapshotDirectory = path.join(config.stateDir, "herdr-omp-snapshots");
    const workingCopies = new Map<string, HerdrOmpWorkingCopy>();
    for (const runtime of runtimeRows) {
      const payload = decodeImportedTranscriptPayload(runtime.runtimePayload);
      if (
        Option.isNone(payload) ||
        payload.value.cwd === undefined ||
        normalizeProjectPathForComparison(payload.value.cwd) !==
          normalizeProjectPathForComparison(project.workspaceRoot)
      )
        continue;
      for (const source of payload.value.importedTranscripts ?? []) {
        if (
          source.provider !== "omp" ||
          (source.herdr === undefined &&
            path.dirname(source.providerSessionId) !== snapshotDirectory)
        )
          continue;
        const cursor = runtime.resumeCursor;
        workingCopies.set(source.providerSessionId, {
          filePath:
            runtime.providerName === "omp" &&
            cursor !== null &&
            typeof cursor === "object" &&
            "sessionFile" in cursor &&
            typeof cursor.sessionFile === "string"
              ? cursor.sessionFile
              : source.filePath,
          ...(source.herdr === undefined
            ? {}
            : { originalFilePath: source.herdr.filePath, sessionId: source.herdr.sessionId }),
        });
      }
    }
    const outcomes =
      input.source === "herdr"
        ? yield* Effect.gen(function* () {
            const currentSettings = yield* settings.getSettings.pipe(
              Effect.mapError(
                (cause) => new AgentSessionScanError({ operation: "read-settings", cause }),
              ),
            );
            const instances = Object.entries(currentSettings.providerInstances)
              .filter(
                ([, instance]) =>
                  instance.driver === "omp" && resolveProviderInstanceEnabled(instance),
              )
              .sort(([left], [right]) => Number(right === "omp") - Number(left === "omp"));
            const instance = instances[0];
            if (instance === undefined) {
              return yield* new AgentSessionScanError({
                operation: "read-settings",
                cause: new Error(
                  "Enable an OMP provider instance before importing Herdr sessions.",
                ),
              });
            }
            return herdrOmpThreads({
              registryPath: path.join(home, ".config", "herdr", "session.json"),
              workspaceRoot: project.workspaceRoot,
              snapshotDirectory,
              providerInstanceId: ProviderInstanceId.make(instance[0]),
              workingCopies,
            }).pipe(
              Stream.provideService(FileSystem.FileSystem, fileSystem),
              Stream.provideService(Path.Path, path),
              Stream.provideService(Crypto.Crypto, crypto),
            );
          })
        : scanner.recentThreads(project.workspaceRoot, completedSources);
    const importedThreadIds = new Set<ThreadId>();
    let importedCount = 0;
    let skippedCount = 0;
    let reimportedCount = 0;
    const preservedThreadIds: ThreadId[] = [];
    const herdrChanges: HerdrSessionImportChange[] = [];
    const confirmations = new Map(input.reimport?.map((item) => [item.threadId, item]));

    yield* Stream.runForEach(
      outcomes,
      (outcome: AgentSessionScanner.AgentSessionRecentThread | HerdrOmpRecentThread) =>
        Effect.gen(function* () {
          if (outcome._tag === "Skipped") {
            skippedCount += 1;
            return;
          }
          const source = outcome.source;
          const threadId = ThreadId.make(
            `import:${source.providerInstanceId}:${source.providerSessionId}`,
          );
          if (outcome._tag === "AlreadyImported") {
            importedThreadIds.add(threadId);
            importedCount += 1;
            return;
          }
          if (outcome._tag === "Duplicate") {
            if (importedThreadIds.has(threadId)) {
              yield* runtimes.recordImportedTranscript({ threadId, source }).pipe(Effect.ignore);
            }
            return;
          }

          const imported = yield* Effect.gen(function* () {
            const thread = outcome.thread;
            if (
              thread.source === "claudeAgent" &&
              !CLAUDE_SESSION_ID_PATTERN.test(thread.providerSessionId)
            ) {
              return yield* new AgentSessionUnresumableSessionError({
                source: thread.source,
                providerSessionId: thread.providerSessionId,
              });
            }
            const driver = ProviderDriverKind.make(thread.source);
            const persistRuntime = runtimes.upsert(
              {
                threadId,
                providerName: driver,
                providerInstanceId: thread.providerInstanceId,
                adapterKey: driver,
                runtimeMode: thread.source === "omp" ? "approval-required" : DEFAULT_RUNTIME_MODE,
                status: "stopped",
                lastSeenAt: thread.updatedAt,
                resumeCursor:
                  thread.source === "omp"
                    ? { sessionFile: source.filePath }
                    : thread.source === "codex"
                      ? { threadId: thread.providerSessionId }
                      : { threadId, resume: thread.providerSessionId },
                runtimePayload: { cwd: project.workspaceRoot },
              },
              { onConflict: "ignore" },
            );
            const existing = yield* Effect.option(orchestrator.getThreadRecords(threadId, []));
            if (Option.isSome(existing)) {
              if (existing.value.thread.projectId !== input.projectId) {
                return yield* new AgentSessionThreadProjectConflictError({
                  threadId,
                  expectedProjectId: input.projectId,
                  actualProjectId: existing.value.thread.projectId,
                });
              }
              if ("herdr" in outcome) {
                const herdr = outcome.herdr;
                if (herdr.relationship !== "same") {
                  const state = yield* projections.getThreadRecords(threadId, [
                    "runs",
                    "providerThreads",
                    "providerSessions",
                    "subagents",
                  ]);
                  let blockedReason = herdrRefreshBlockedReason(state);
                  const confirmation = confirmations.get(threadId);
                  if (confirmation !== undefined && blockedReason === null) {
                    const result = yield* refreshHerdrSession({
                      threadId,
                      outcome,
                      confirmation,
                      snapshotDirectory,
                      workspaceRoot: project.workspaceRoot,
                      events: (current, nativeFile, revision, at) => {
                        const providerThreadId = idAllocator.derive.providerThread({
                          driver,
                          providerInstanceId: current.providerInstanceId,
                          nativeThreadId: nativeFile,
                        });
                        const providerThread: OrchestrationV2ProviderThread = {
                          id: providerThreadId,
                          driver,
                          providerInstanceId: current.providerInstanceId,
                          appThreadId: threadId,
                          providerSessionId: null,
                          ownerNodeId: null,
                          nativeThreadRef: { driver, nativeId: nativeFile, strength: "strong" },
                          nativeConversationHeadRef: null,
                          status: "idle",
                          firstRunOrdinal: null,
                          lastRunOrdinal: null,
                          handoffIds: [],
                          forkedFrom: null,
                          pendingBackgroundTasks: [],
                          createdAt: at,
                          updatedAt: at,
                        };
                        return [
                          {
                            id: EventId.make(`${revision}:reset`),
                            type: "thread.history-reset",
                            threadId,
                            occurredAt: at,
                            payload: {
                              ...current,
                              activeProviderThreadId: providerThreadId,
                              historyOrigin: "v1_import",
                              updatedAt: at,
                            },
                          },
                          ...herdr.sourceThread.messages.flatMap((message, index) =>
                            messageEvents({ threadId, index, message, revision }),
                          ),
                          {
                            id: EventId.make(`${revision}:provider-thread`),
                            type: "provider-thread.updated",
                            threadId,
                            driver,
                            providerInstanceId: current.providerInstanceId,
                            occurredAt: at,
                            payload: providerThread,
                          },
                        ];
                      },
                    }).pipe(
                      Effect.provide(refreshContext),
                      Effect.catchCause(() =>
                        Effect.succeed({
                          blockedReason:
                            "Reimport failed. The existing conversation is unchanged; import from Herdr again to retry.",
                        }),
                      ),
                    );
                    if (result.blockedReason === null) {
                      reimportedCount += 1;
                      if ("preservedThreadId" in result && result.preservedThreadId !== null)
                        preservedThreadIds.push(result.preservedThreadId);
                      return true;
                    }
                    blockedReason = result.blockedReason;
                  }
                  herdrChanges.push({
                    threadId,
                    title: existing.value.thread.title,
                    relationship: herdr.relationship,
                    sourceVersion: herdr.sourceVersion,
                    workingVersion: herdr.workingVersion,
                    blockedReason,
                  });
                }
                yield* persistRuntime;
                yield* runtimes.recordImportedTranscript({ threadId, source });
                return true;
              }
              if (existing.value.thread.historyOrigin !== "v1_import") {
                return yield* new AgentSessionThreadModifiedError({ threadId });
              }
              yield* persistRuntime;
              yield* runtimes.recordImportedTranscript({ threadId, source });
              return true;
            }

            const model = thread.model ?? DEFAULT_MODEL_BY_PROVIDER[driver] ?? DEFAULT_MODEL;
            const providerThreadId = idAllocator.derive.providerThread({
              driver,
              providerInstanceId: thread.providerInstanceId,
              nativeThreadId: thread.providerSessionId,
            });
            const createdAt = dateTime(thread.createdAt);
            const updatedAt = dateTime(thread.updatedAt);
            const appThread: OrchestrationV2AppThread = {
              createdBy: "system",
              creationSource: "server",
              id: threadId,
              projectId: input.projectId,
              title: thread.title.trim() === "" ? "Untitled thread" : thread.title,
              providerInstanceId: thread.providerInstanceId,
              modelSelection: { instanceId: thread.providerInstanceId, model },
              runtimeMode: thread.source === "omp" ? "approval-required" : DEFAULT_RUNTIME_MODE,
              interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
              branch: null,
              worktreePath: null,
              linkedPullRequest: null,
              branchPullRequest: null,
              activeProviderThreadId: providerThreadId,
              historyOrigin: "v1_import",
              lineage: {
                parentThreadId: null,
                relationshipToParent: null,
                rootThreadId: threadId,
              },
              forkedFrom: null,
              createdAt,
              updatedAt,
              archivedAt: null,
              settledOverride: "settled",
              settledAt: updatedAt,
              unsettledAt: null,
              snoozedUntil: null,
              snoozedAt: null,
              pinnedAt: null,
              pinOrderKey: null,
              activeOrderKey: null,
              lastVisitedAt: null,
              deletedAt: null,
            };
            const providerThread: OrchestrationV2ProviderThread = {
              id: providerThreadId,
              driver,
              providerInstanceId: thread.providerInstanceId,
              providerSessionId: null,
              appThreadId: threadId,
              ownerNodeId: null,
              nativeThreadRef: {
                driver,
                nativeId: thread.providerSessionId,
                strength: "strong",
              },
              nativeConversationHeadRef: null,
              status: "idle",
              firstRunOrdinal: null,
              lastRunOrdinal: null,
              handoffIds: [],
              forkedFrom: null,
              pendingBackgroundTasks: [],
              createdAt,
              updatedAt,
            };

            const committed = yield* eventSink.write({
              unlessNativeThreadOwned: providerThread,
              events: [
                {
                  id: EventId.make(`${IMPORT_EVENT_PREFIX}:thread:${threadId}:created`),
                  type: "thread.created",
                  threadId,
                  providerInstanceId: thread.providerInstanceId,
                  occurredAt: createdAt,
                  payload: appThread,
                },
                ...thread.messages.flatMap((message, index) =>
                  messageEvents({ threadId, index, message }),
                ),
                {
                  id: EventId.make(`${IMPORT_EVENT_PREFIX}:provider-thread:${providerThreadId}`),
                  type: "provider-thread.updated",
                  threadId,
                  driver,
                  providerInstanceId: thread.providerInstanceId,
                  occurredAt: updatedAt,
                  payload: providerThread,
                },
              ],
            });
            if (committed.length === 0) return null;
            yield* persistRuntime;
            yield* runtimes.recordImportedTranscript({ threadId, source });
            return true;
          }).pipe(
            Effect.catch((cause) =>
              Effect.logWarning("Could not import an agent session", {
                provider: outcome.thread.source,
                sessionId: outcome.thread.providerSessionId,
                cause,
              }).pipe(Effect.as(false)),
            ),
          );
          if (imported === true) {
            importedThreadIds.add(threadId);
            importedCount += 1;
          } else if (imported === false) {
            skippedCount += 1;
          }
        }),
    );

    return {
      importedCount,
      skippedCount,
      ...(herdrChanges.length === 0 ? {} : { herdrChanges }),
      ...(reimportedCount === 0 ? {} : { reimportedCount }),
      ...(preservedThreadIds.length === 0 ? {} : { preservedThreadIds }),
    } satisfies AgentSessionImportResult;
  });

  return { importRecentAgentThreads };
});

type AgentSessionImporterShape = Effect.Success<typeof make>;

export class AgentSessionImporter extends Context.Service<
  AgentSessionImporter,
  AgentSessionImporterShape
>()("t3/project/AgentSessionImporter") {}

export const layer = Layer.effect(AgentSessionImporter, make);
