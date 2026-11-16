import {
  DEFAULT_MODEL,
  DEFAULT_MODEL_BY_PROVIDER,
  DEFAULT_PROVIDER_INTERACTION_MODE,
  DEFAULT_RUNTIME_MODE,
  isRuntimeModeAbove,
  AgentSessionImportProjectChangedError,
  AgentSessionImportProjectNotFoundError,
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
import { resolveProjectSettings } from "@t3tools/shared/projectSettings";
import * as Context from "effect/Context";
import * as Digest from "effect/crypto/Digest";
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

import * as EventSink from "../orchestration-v2/EventSink.ts";
import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import * as Orchestrator from "../orchestration-v2/Orchestrator.ts";
import * as ProjectionStore from "../orchestration-v2/ProjectionStore.ts";
import * as ProviderAdapterRegistry from "../orchestration-v2/ProviderAdapterRegistry.ts";
import * as ThreadCommandExecutor from "../orchestration-v2/ThreadCommandExecutor.ts";
import * as ProviderSessionRuntime from "../persistence/ProviderSessionRuntime.ts";
import * as AgentSessionScanner from "./AgentSessionScanner.ts";
import * as ProjectService from "./ProjectService.ts";

const isProjectionStoreThreadNotFoundError = Schema.is(
  ProjectionStore.ProjectionStoreThreadNotFoundError,
);

const IMPORT_EVENT_PREFIX = "agent-session-import:v2";
const CLAUDE_SESSION_ID_PATTERN =
  /^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;

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

// ACP imports read the history while holding the session-mutation lock, and an agent can
// leave session/resume or initialize unanswered. Longer than session/load's own deadline.
const NATIVE_HISTORY_READ_TIMEOUT = "2 minutes";

/**
 * Whether a thread bound to a native session still lacks its native history.
 * Once a turn has run, the native session also holds that turn's messages, so
 * its history can no longer be imported as run-less items.
 */
export function isNativeHistoryPending(input: {
  readonly historyOrigin: OrchestrationV2AppThread["historyOrigin"];
  readonly hasRun: boolean;
}): boolean {
  return input.historyOrigin !== "v1_import" && !input.hasRun;
}

function dateTime(value: string): DateTime.Utc {
  return DateTime.makeUnsafe(value);
}

type HistoryMessage = Omit<AgentSessionScanner.AgentSessionThreadMessage, "role"> & {
  readonly role: AgentSessionScanner.AgentSessionThreadMessage["role"] | "reasoning";
};

function messageEvents(input: {
  readonly threadId: ThreadId;
  readonly index: number;
  readonly message: HistoryMessage;
  readonly revision?: string;
}): ReadonlyArray<OrchestrationV2DomainEvent> {
  const ordinal = input.index + 1;
  const suffix = String(input.index).padStart(6, "0");
  const identity =
    input.revision === undefined ? input.threadId : `${input.threadId}:${input.revision}`;
  const messageId = MessageId.make(`${identity}:${suffix}`);
  const turnItemId = TurnItemId.make(`${IMPORT_EVENT_PREFIX}:turn-item:${identity}:${suffix}`);
  const at = dateTime(input.message.createdAt);
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
  const turnItemEvent = (payload: OrchestrationV2TurnItem): OrchestrationV2DomainEvent => ({
    id: EventId.make(turnItemId),
    type: "turn-item.updated",
    threadId: input.threadId,
    occurredAt: at,
    payload,
  });
  if (input.message.role === "reasoning") {
    return [
      turnItemEvent({ ...common, type: "reasoning", text: input.message.text, streaming: false }),
    ];
  }
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
    turnItemEvent(turnItem),
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
  const crypto = yield* Digest.Digest;
  const config = yield* ServerConfig.ServerConfig;
  const home = yield* HostProcess.HomeDirectory;
  const refreshContext = yield* Effect.context<HerdrSessionRefreshServices>();
  const projections = Context.get(refreshContext, ProjectionStore.ProjectionStoreV2);
  const registry = yield* ProviderAdapterRegistry.ProviderAdapterRegistryV2;
  const threadCommands = yield* ThreadCommandExecutor.ThreadCommandExecutor;
  const settingsService = yield* ServerSettings.ServerSettingsService;
  const ceiling = settingsService.managedPolicy?.limits?.maxRuntimeMode;
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
    // Imports run in full access, except under a policy ceiling: then in the project's default,
    // which the policy keeps under it, so imported threads never sit above the ceiling.
    const runtimeMode =
      ceiling === undefined
        ? DEFAULT_RUNTIME_MODE
        : yield* settingsService.getSettings.pipe(
            Effect.mapError(
              (cause) => new AgentSessionScanError({ operation: "read-settings", cause }),
            ),
            Effect.map((settings) => {
              const mode = resolveProjectSettings(settings, input.projectId, project).settings
                .defaultRuntimeMode;
              return isRuntimeModeAbove(mode, ceiling) ? ceiling : mode;
            }),
          );
    const runtimeRows = yield* runtimes
      .list()
      .pipe(
        Effect.mapError(
          (cause) => new AgentSessionScanError({ operation: "read-projects", cause }),
        ),
      );
    const projectRuntimeIds = new Set<ThreadId>();
    // The thread's project owns its transcripts: a linked worktree import records the worktree as cwd.
    const completedSources = (yield* Effect.forEach(runtimeRows, (runtime) => {
      const imported = AgentSessionScanner.decodeImportedTranscriptPayload(
        runtime.runtimePayload,
      ).pipe(Option.flatMapNullishOr((payload) => payload.importedTranscripts));
      if (Option.isNone(imported) || imported.value.length === 0) return Effect.succeed([]);
      return orchestrator.getThreadRecords(runtime.threadId, []).pipe(
        Effect.map(({ thread }) => {
          if (thread.projectId !== input.projectId) return [];
          projectRuntimeIds.add(runtime.threadId);
          return imported.value;
        }),
        Effect.catch((cause) =>
          isProjectionStoreThreadNotFoundError(cause.cause)
            ? Effect.succeed([])
            : Effect.fail(new AgentSessionScanError({ operation: "read-projects", cause })),
        ),
      );
    })).flat();
    const snapshotDirectory = path.join(config.stateDir, "herdr-omp-snapshots");
    const workingCopies = new Map<string, HerdrOmpWorkingCopy>();
    for (const runtime of runtimeRows) {
      if (!projectRuntimeIds.has(runtime.threadId)) continue;
      const payload = AgentSessionScanner.decodeImportedTranscriptPayload(runtime.runtimePayload);
      if (Option.isNone(payload)) continue;
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
            const currentSettings = yield* settingsService.getSettings.pipe(
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
            if (instance === undefined)
              return yield* new AgentSessionScanError({
                operation: "read-settings",
                cause: new Error(
                  "Enable an OMP provider instance before importing Herdr sessions.",
                ),
              });
            return herdrOmpThreads({
              registryPath: path.join(home, ".config", "herdr", "session.json"),
              workspaceRoot: project.workspaceRoot,
              snapshotDirectory,
              providerInstanceId: ProviderInstanceId.make(instance[0]),
              workingCopies,
            }).pipe(
              Stream.provideService(FileSystem.FileSystem, fileSystem),
              Stream.provideService(Path.Path, path),
              Stream.provideService(Digest.Digest, crypto),
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
            const nativeThreadId =
              thread.source === "pi" || thread.source === "omp"
                ? source.filePath
                : thread.providerSessionId;
            const importRuntimeMode =
              thread.source === "omp"
                ? ceiling !== undefined && isRuntimeModeAbove("approval-required", ceiling)
                  ? ceiling
                  : "approval-required"
                : runtimeMode;
            const persistRuntime = runtimes.upsert(
              {
                threadId,
                providerName: driver,
                providerInstanceId: thread.providerInstanceId,
                adapterKey: driver,
                runtimeMode: importRuntimeMode,
                status: "stopped",
                lastSeenAt: thread.updatedAt,
                resumeCursor:
                  thread.source === "omp"
                    ? { sessionFile: source.filePath }
                    : thread.source === "claudeAgent"
                      ? { threadId, resume: thread.providerSessionId }
                      : { threadId: nativeThreadId },
                runtimePayload: { cwd: outcome.worktree?.path ?? project.workspaceRoot },
              },
              { onConflict: "ignore" },
            );
            const existing = yield* Effect.option(
              orchestrator.getThreadRecords(threadId, ["messages"]),
            );
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
              // A transcript can change after the import, so record it only when the
              // thread already holds exactly its messages.
              const stored = existing.value.messages;
              if (
                stored.length !== thread.messages.length ||
                stored.some(
                  (message, index) =>
                    message.role !== thread.messages[index]?.role ||
                    message.text !== thread.messages[index]?.text,
                )
              ) {
                return false;
              }
              yield* persistRuntime;
              yield* runtimes.recordImportedTranscript({ threadId, source });
              importedThreadIds.add(threadId);
              return null;
            }

            const model = thread.model ?? DEFAULT_MODEL_BY_PROVIDER[driver] ?? DEFAULT_MODEL;
            const providerThreadId = idAllocator.derive.providerThread({
              driver,
              providerInstanceId: thread.providerInstanceId,
              nativeThreadId,
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
              runtimeMode: importRuntimeMode,
              interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
              branch: outcome.worktree?.branch ?? null,
              worktreePath: outcome.worktree?.path ?? null,
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
                nativeId: nativeThreadId,
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
                ...(thread.omittedMessageCount > 0
                  ? [
                      {
                        id: EventId.make(`${IMPORT_EVENT_PREFIX}:history-truncated:${threadId}`),
                        type: "turn-item.updated" as const,
                        threadId,
                        occurredAt: createdAt,
                        payload: {
                          id: TurnItemId.make(
                            `${IMPORT_EVENT_PREFIX}:history-truncated:${threadId}`,
                          ),
                          threadId,
                          runId: null,
                          nodeId: null,
                          providerThreadId: null,
                          providerTurnId: null,
                          nativeItemRef: null,
                          parentItemId: null,
                          ordinal: 0,
                          status: "completed" as const,
                          title: null,
                          startedAt: createdAt,
                          completedAt: createdAt,
                          updatedAt: createdAt,
                          type: "system_notice" as const,
                          message: `${thread.omittedMessageCount} earlier ${thread.omittedMessageCount === 1 ? "message was" : "messages were"} not imported. This thread keeps the first user message and the most recent history. The original session transcript is unchanged.`,
                        },
                      },
                    ]
                  : []),
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

  /**
   * Loading a native session replays its history only into the adapter's own
   * snapshot, so copy that snapshot into the thread as run-less history. The
   * `v1_import` origin keeps it in forks and marks the import done, so a call
   * after a failed read repairs the thread and any later call does nothing.
   */
  const importNativeHistory = Effect.fn("importNativeHistoryV2")(function* (input: {
    readonly threadId: ThreadId;
    readonly workspaceRoot: string;
  }) {
    const pendingThread = Effect.map(
      orchestrator.getThreadRecords(input.threadId, ["providerThreads", "runs"]),
      (records) =>
        isNativeHistoryPending({
          historyOrigin: records.thread.historyOrigin,
          hasRun: records.runs.length > 0,
        })
          ? records
          : null,
    );
    // Thread commands write whole thread rows, so hold their lock from the
    // re-read through the write to keep both this marker and their changes.
    const commit = (messages: ReadonlyArray<HistoryMessage>) =>
      threadCommands.withLock(
        input.threadId,
        Effect.gen(function* () {
          const current = yield* pendingThread;
          if (current === null) return 0;
          yield* eventSink.write({
            events: [
              ...messages.flatMap((message, index) =>
                messageEvents({ threadId: input.threadId, index, message }),
              ),
              {
                id: EventId.make(`${IMPORT_EVENT_PREFIX}:thread:${input.threadId}:native-history`),
                type: "thread.metadata-updated",
                threadId: input.threadId,
                providerInstanceId: current.thread.providerInstanceId,
                occurredAt: yield* DateTime.now,
                payload: { ...current.thread, historyOrigin: "v1_import" },
              },
            ],
          });
          return messages.length;
        }),
      );
    const records = yield* pendingThread;
    if (records === null) return 0;
    const { thread, providerThreads } = records;
    const providerThread = providerThreads.find(
      (candidate) => candidate.appThreadId === input.threadId && candidate.nativeThreadRef !== null,
    );
    const nativeThreadId = providerThread?.nativeThreadRef?.nativeId;
    if (providerThread === undefined || nativeThreadId == null) return 0;
    const providerSessionId = yield* idAllocator.allocate.providerSession({
      providerInstanceId: providerThread.providerInstanceId,
      threadId: input.threadId,
    });
    // The registry admits this open against sign-in changes, like any other open.
    const adapter = yield* registry.get(thread.modelSelection.instanceId);
    const snapshot = yield* adapter
      .openSession({
        threadId: input.threadId,
        providerSessionId,
        modelSelection: thread.modelSelection,
        runtimePolicy: {
          runtimeMode: thread.runtimeMode,
          interactionMode: thread.interactionMode,
          cwd: thread.worktreePath ?? input.workspaceRoot,
        },
        initialNativeThreadId: nativeThreadId,
        ...(providerThread.nativeMetadata?.itemIdentityVersion === undefined
          ? {}
          : {
              initialProviderItemIdentityVersion: providerThread.nativeMetadata.itemIdentityVersion,
            }),
      })
      .pipe(
        Effect.flatMap((session) => session.readThreadSnapshot({ providerThread })),
        Effect.timeout(NATIVE_HISTORY_READ_TIMEOUT),
        Effect.scoped,
      );
    const reasoningIds = new Set(snapshot.reasoningMessageIds);
    const messages = snapshot.messages.flatMap((message) =>
      (message.role === "user" || message.role === "assistant") && message.text.trim() !== ""
        ? [
            {
              role: reasoningIds.has(message.id) ? ("reasoning" as const) : message.role,
              text: message.text,
              createdAt: DateTime.formatIso(message.createdAt),
            },
          ]
        : [],
    );
    // Loading the session can take a while, and the thread is usable meanwhile.
    return yield* commit(messages);
  });

  return { importRecentAgentThreads, importNativeHistory };
});

type AgentSessionImporterShape = Effect.Success<typeof make>;

export class AgentSessionImporter extends Context.Service<
  AgentSessionImporter,
  AgentSessionImporterShape
>()("t3/project/AgentSessionImporter") {}

export const layer = Layer.effect(AgentSessionImporter, make);
