import {
  type ChatAttachment,
  type ModelSelection,
  type OrchestrationV2ProviderSession,
  type OrchestrationV2ProviderThread,
  type OrchestrationV2ProviderTurn,
  type ProviderInstanceId,
} from "@t3tools/contracts";
import * as HostProcess from "@t3tools/shared/HostProcess";
import { AgentScope } from "@t3tools/shared/AgentScope";
import { getModelSelectionStringOptionValue } from "@t3tools/shared/model";
import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import * as McpProviderSessions from "@t3tools/provider-core/server/McpProviderSessions";
import * as ProviderAdapter from "@t3tools/provider-core/server/ProviderAdapter";
import * as ProviderHost from "@t3tools/provider-core/server/ProviderHost";
import {
  ProviderAdapterDriverCreateError,
  type ProviderAdapterDriver,
  type ProviderAdapterDriverCreateInput,
} from "@t3tools/provider-core/server/adapterDriver";
import { providerMessageTextWithAttachmentPaths } from "@t3tools/provider-core/server/attachmentPrompt";
import { mergeProviderInstanceEnvironment } from "@t3tools/provider-core/server/instanceEnvironment";
import { turnScopedSelectionTransition } from "@t3tools/provider-core/server/selectionTransition";
import * as Cause from "effect/Cause";
import * as Clock from "effect/Clock";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Option from "effect/Option";
import * as Queue from "effect/Queue";
import * as Result from "effect/Result";
import * as Schema from "effect/Schema";
import * as Semaphore from "effect/Semaphore";
import * as Stream from "effect/Stream";
import * as HttpClient from "effect/http/HttpClient";
import * as ChildProcessSpawner from "effect/process/ChildProcessSpawner";
import { OmpSettings } from "../settings.ts";
import { OMP_CAPABILITIES } from "./capabilities.ts";
import { OMP_PROVIDER, makeOmpTurnEvents, ompRef, type OmpTurnEvents } from "./events.ts";
import { parseOmpDiscoveredCommands, type OmpDiscoveredCommands } from "./discovery.ts";
import {
  contentText,
  field,
  number,
  ompCompletion,
  string,
  walkOmpBranch,
  type OmpRecord,
} from "./frames.ts";
import {
  buildOmpRpcLaunch,
  materializeOmpT3Extension,
  OMP_PERMISSION_GUARD_COMMAND,
  resolveOmpLaunchArgs,
} from "./mcpInjection.ts";
import { makeOmpMcpBridge, type OmpMcpBridge } from "./mcpBridge.ts";
import { makeOmpRpcConnection, parseOmpModelSlug } from "./rpc.ts";
import { makeOmpUi } from "./ui.ts";

const decodeOmpSettings = Schema.decodeSync(OmpSettings);

export interface OmpAdapterV2Options {
  readonly instanceId: ProviderInstanceId;
  readonly settings: OmpSettings;
  readonly environment: NodeJS.ProcessEnv;
  readonly onWorkspaceCommands?: (cwd: string, data: OmpDiscoveredCommands) => Effect.Effect<void>;
}

interface ActiveTurn {
  readonly input: ProviderAdapter.ProviderAdapterV2TurnInput;
  providerTurn: OrchestrationV2ProviderTurn;
  readonly promptId: string;
  readonly events: OmpTurnEvents;
  interrupted: boolean;
  failure: string | null;
  nativeFailure: string | null;
  nativeInterrupted: boolean;
  retryable: boolean;
  admitted: boolean;
  sawActivity: boolean;
  sawResult: boolean;
}

export const makeOmpAdapterV2 = Effect.fn("makeOmpAdapterV2")(function* (
  options: OmpAdapterV2Options,
) {
  const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
  const fileSystem = yield* FileSystem.FileSystem;
  const ids = yield* IdAllocator.IdAllocatorV2;
  const host = yield* ProviderHost.ProviderHost;
  const mcpSessions = yield* McpProviderSessions.McpProviderSessions;
  const agentScope = yield* AgentScope;
  const httpClient = yield* HttpClient.HttpClient;
  const protocolError = (detail: string, cause?: unknown) =>
    new ProviderAdapter.ProviderAdapterProtocolError({
      driver: OMP_PROVIDER,
      detail,
      ...(cause === undefined ? {} : { cause }),
    });
  const args = resolveOmpLaunchArgs(options.settings.launchArgs);
  if (!args.ok) return yield* protocolError(args.message);
  const launchArgs = args.args;

  return ProviderAdapter.ProviderAdapterV2.of({
    instanceId: options.instanceId,
    driver: OMP_PROVIDER,
    getCapabilities: Effect.fn("OmpAdapter.getCapabilities")(() =>
      Effect.succeed(OMP_CAPABILITIES),
    ),
    planSelectionTransition: Effect.fn("OmpAdapter.planSelectionTransition")(() =>
      Effect.succeed(turnScopedSelectionTransition()),
    ),
    openSession: Effect.fn("OmpAdapter.openSession")(function* (
      input: ProviderAdapter.ProviderAdapterV2OpenSessionInput,
    ) {
      const scope = yield* Effect.scope;
      const cwd = input.runtimePolicy.cwd ?? host.paths.cwd;
      const mcpSession = yield* mcpSessions.read(input.threadId);
      const extensionPath = yield* materializeOmpT3Extension(
        host.paths.providerStatusCacheDir,
      ).pipe(
        Effect.provideService(FileSystem.FileSystem, fileSystem),
        Effect.mapError(
          (cause) =>
            new ProviderAdapter.ProviderAdapterOpenSessionError({
              driver: OMP_PROVIDER,
              providerSessionId: input.providerSessionId,
              cause,
            }),
        ),
      );
      const launch = buildOmpRpcLaunch({
        launchArgs,
        environment: options.environment,
        extensionPath,
        mcpSession,
        runtimeMode: input.runtimePolicy.runtimeMode,
        sessionPath: input.initialNativeThreadId,
      });
      const scopedLaunch = yield* agentScope.wrap({
        command: options.settings.binaryPath || "omp",
        args: launch.args,
        env: launch.env,
        name: "omp",
        threadId: input.threadId,
      });
      const stopScope =
        scopedLaunch.unit === undefined
          ? Effect.void
          : yield* Effect.cached(agentScope.stop(scopedLaunch.unit));
      yield* Effect.addFinalizer(() => stopScope);
      const connection = yield* makeOmpRpcConnection({
        command: scopedLaunch.command,
        args: scopedLaunch.args,
        cwd,
        env: launch.env,
      }).pipe(
        Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, spawner),
        Effect.mapError(
          (cause) =>
            new ProviderAdapter.ProviderAdapterOpenSessionError({
              driver: OMP_PROVIDER,
              providerSessionId: input.providerSessionId,
              cause,
            }),
        ),
      );
      const request = (record: OmpRecord, timeoutMs = 30_000) =>
        connection
          .request(record, timeoutMs)
          .pipe(
            Effect.mapError((cause) =>
              protocolError(
                `OMP ${string(record, "type") ?? "request"} failed: ${cause.message}`,
                cause,
              ),
            ),
          );
      const lifecycle = (record: OmpRecord) =>
        connection.request(record, 60_000).pipe(
          Effect.tapError((error) =>
            error._tag === "OmpRpcTimeoutError" ? connection.terminate : Effect.void,
          ),
          Effect.onInterrupt(() => connection.terminate),
          Effect.mapError((cause) =>
            protocolError(
              `OMP ${string(record, "type") ?? "session transition"} failed: ${cause.message}`,
              cause,
            ),
          ),
        );
      const createdAt = yield* DateTime.now;
      let session: OrchestrationV2ProviderSession = {
        id: input.providerSessionId,
        driver: OMP_PROVIDER,
        providerInstanceId: options.instanceId,
        status: "ready",
        cwd,
        model: input.modelSelection.model,
        capabilities: OMP_CAPABILITIES,
        createdAt,
        updatedAt: createdAt,
        lastError: null,
      };
      const queue = yield* Queue.unbounded<
        ProviderAdapter.ProviderAdapterV2Event,
        ProviderAdapter.ProviderAdapterV2Error | Cause.Done
      >();
      const emit = (event: ProviderAdapter.ProviderAdapterV2Event) =>
        Queue.offer(queue, event).pipe(Effect.asVoid);
      const permit = yield* Semaphore.make(1);
      let thread: OrchestrationV2ProviderThread | null = null;
      let active: ActiveTurn | null = null;
      let closed = false;
      let settled = true;
      let settledSignal = yield* Deferred.make<void>();
      yield* Deferred.succeed(settledSignal, undefined);
      let changingSession = false;
      let registered = false;
      let currentFile: string | undefined;
      let baselineModel: { provider: string; modelId: string } | null = null;
      let baselineThinking = "inherit";
      let appliedModel: string | null = null;
      let appliedThinking: string | null = null;
      let nextPromptId = 0;
      const turns = new Map<string, OrchestrationV2ProviderTurn>();
      const modelWindows = new Map<string, number>();
      const subagentOwners = new Map<string, OmpTurnEvents>();
      const subagentIndices = new Map<number, string>();
      const publishCommands = Effect.fn("OmpAdapter.publishCommands")(function* (data: unknown) {
        if (options.onWorkspaceCommands === undefined) return;
        const parsed = parseOmpDiscoveredCommands(data);
        if (Result.isFailure(parsed))
          return yield* protocolError("OMP returned an invalid command inventory", parsed.failure);
        yield* options.onWorkspaceCommands(cwd, parsed.success);
      });
      const ui = makeOmpUi({
        ids,
        connection,
        input,
        emit,
        thread: () => thread,
        active: () => active,
        protocolError,
      });
      const updateSession = Effect.fnUntraced(function* (
        status: OrchestrationV2ProviderSession["status"],
        error: string | null = null,
      ) {
        session = { ...session, status, lastError: error, updatedAt: yield* DateTime.now };
        yield* emit({
          type: "provider_session.updated",
          driver: OMP_PROVIDER,
          providerSession: session,
        });
      });
      const updateThread = Effect.fnUntraced(function* (
        patch: Partial<OrchestrationV2ProviderThread>,
      ) {
        if (thread === null) return;
        thread = { ...thread, ...patch, updatedAt: yield* DateTime.now };
        yield* emit({
          type: "provider_thread.updated",
          driver: OMP_PROVIDER,
          providerThread: thread,
        });
      });
      const readBranch = () =>
        request({ type: "get_entries" }).pipe(
          Effect.flatMap((data) =>
            Effect.try({
              try: () => walkOmpBranch(data),
              catch: (cause) => protocolError("OMP returned an invalid session tree", cause),
            }),
          ),
        );
      const rememberModel = (model: unknown) => {
        const provider = string(model, "provider");
        const id = string(model, "id");
        const window = number(model, "contextWindow");
        if (provider && id && window && window > 0) modelWindows.set(`${provider}/${id}`, window);
      };
      const ensureHosted = (wanted: OrchestrationV2ProviderThread) =>
        thread !== null &&
        wanted.id === thread.id &&
        wanted.nativeThreadRef?.nativeId === thread.nativeThreadRef?.nativeId;
      const idle = Effect.fnUntraced(function* () {
        if (active !== null || !settled || changingSession)
          return yield* protocolError("OMP session still has live or pending work");
      });
      const sessionChange = <A, E, R>(operation: Effect.Effect<A, E, R>) =>
        Effect.acquireUseRelease(
          permit.withPermits(1)(
            Effect.gen(function* () {
              yield* idle();
              changingSession = true;
            }),
          ),
          () => operation,
          () =>
            Effect.sync(() => {
              changingSession = false;
            }),
        );
      const captureState = Effect.fnUntraced(function* () {
        const state = yield* request({ type: "get_state" });
        const file = string(state, "sessionFile");
        if (file === undefined)
          return yield* protocolError("OMP did not return a durable sessionFile");
        currentFile = file;
        settled =
          field(state, "isSettled") !== false &&
          field(state, "hasPendingAsyncWork") !== true &&
          field(state, "isStreaming") !== true;
        rememberModel(field(state, "model"));
        return { state, file };
      });
      const snapshot = Effect.fn("OmpAdapter.snapshot")(function* (
        wanted: OrchestrationV2ProviderThread,
        duringChange = false,
      ): Effect.fn.Return<
        ProviderAdapter.ProviderAdapterV2ThreadSnapshot,
        ProviderAdapter.ProviderAdapterV2Error
      > {
        if (!ensureHosted(wanted) || thread === null)
          return yield* protocolError("OMP snapshot requested for an unhosted thread");
        if (!duringChange) yield* idle();
        const branch = yield* readBranch();
        const messages = branch.flatMap((entry) => {
          if (entry.type !== "message") return [];
          const role = string(entry.message, "role");
          if (role !== "user" && role !== "assistant") return [];
          const text = contentText(field(entry.message, "content"));
          if (!text) return [];
          const at = Option.getOrElse(
            DateTime.make(
              string(entry, "timestamp") ?? number(entry.message, "timestamp") ?? Number.NaN,
            ),
            () => wanted.createdAt,
          );
          return [
            {
              id: ids.derive.messageFromProviderItem({
                driver: OMP_PROVIDER,
                nativeItemId: `${wanted.nativeThreadRef?.nativeId}:${entry.id}`,
              }),
              threadId: wanted.appThreadId ?? input.threadId,
              runId: null,
              nodeId: null,
              role: role as "user" | "assistant",
              text,
              attachments: [],
              streaming: false,
              createdBy: role === "user" ? ("user" as const) : ("agent" as const),
              creationSource: "provider" as const,
              createdAt: at,
              updatedAt: at,
            },
          ];
        });
        return {
          providerThread: thread,
          providerTurns: Array.from(turns.values()).filter(
            (turn) => turn.providerThreadId === wanted.id,
          ),
          messages,
          runtimeRequests: [],
          providerPayload: { sessionFile: currentFile, leafId: branch.at(-1)?.id ?? null },
        };
      });
      const finish = Effect.fnUntraced(function* (
        requestedStatus: "completed" | "interrupted" | "failed",
        broken = false,
      ) {
        const turn = active;
        if (turn === null) return;
        active = null;
        const failureMessage = turn.failure ?? turn.nativeFailure;
        const status =
          turn.interrupted || turn.nativeInterrupted || requestedStatus === "interrupted"
            ? "interrupted"
            : failureMessage !== null || requestedStatus === "failed"
              ? "failed"
              : "completed";
        yield* turn.events.finish(status);
        yield* ui.cancelAll();
        const now = yield* DateTime.now;
        const branch = broken ? null : yield* readBranch().pipe(Effect.orElseSucceed(() => null));
        const leaf = branch?.at(-1)?.id;
        const lastMessage = branch?.findLast((entry) => entry.type === "message");
        const nativeTurnRef =
          lastMessage === undefined ? turn.providerTurn.nativeTurnRef : ompRef(lastMessage.id);
        const state = broken
          ? undefined
          : yield* request({ type: "get_state" }).pipe(Effect.orElseSucceed(() => undefined));
        const usage = field(state, "contextUsage");
        const usedTokens = number(usage, "tokens");
        const maxTokens =
          number(usage, "contextWindow") ?? number(field(state, "model"), "contextWindow");
        turn.providerTurn = {
          ...turn.providerTurn,
          status,
          completedAt: now,
          nativeTurnRef,
          ...(usedTokens === undefined || maxTokens === undefined || maxTokens <= 0
            ? {}
            : {
                tokenUsage: {
                  usedTokens: Math.max(0, usedTokens),
                  maxTokens,
                  updatedAt: DateTime.formatIso(now),
                },
              }),
        };
        turns.set(turn.providerTurn.id, turn.providerTurn);
        yield* emit({
          type: "provider_turn.updated",
          driver: OMP_PROVIDER,
          threadId: turn.input.threadId,
          providerTurn: turn.providerTurn,
        });
        yield* updateThread({
          status: broken ? "error" : "idle",
          ...(leaf === undefined ? {} : { nativeConversationHeadRef: ompRef(leaf) }),
        });
        yield* updateSession(broken || status === "failed" ? "error" : "ready", failureMessage);
        if (status === "failed") {
          const failure = yield* turn.events.failureItem(
            failureMessage ?? "OMP turn failed.",
            turn.retryable,
          );
          yield* emit({
            type: "turn.terminal",
            driver: OMP_PROVIDER,
            providerThreadId: turn.input.providerThread.id,
            providerTurnId: turn.providerTurn.id,
            runOrdinal: turn.input.runOrdinal,
            failureItemOrdinal: failure.ordinal,
            status: "failed",
            failure: failure.failure,
            threadDisposition: broken ? "broken" : "reusable",
          });
        } else
          yield* emit({
            type: "turn.terminal",
            driver: OMP_PROVIDER,
            providerThreadId: turn.input.providerThread.id,
            providerTurnId: turn.providerTurn.id,
            runOrdinal: turn.input.runOrdinal,
            status,
            failure: null,
            threadDisposition: broken ? "broken" : "reusable",
          });
        yield* Deferred.succeed(settledSignal, undefined);
      });
      let bridge: OmpMcpBridge | null = null;
      const handle = Effect.fnUntraced(function* (frame: OmpRecord) {
        if (closed) return;
        if (bridge !== null && (yield* bridge.handle(frame))) return;
        if (frame.type === "available_commands_update") {
          yield* publishCommands(frame);
          return;
        }
        if (frame.type === "host_uri_request" && bridge === null) {
          yield* connection.send({
            type: "host_uri_result",
            id: frame.id,
            isError: true,
            error: "T3 has no handler for this host URI.",
          });
          return;
        }
        if (frame.type === "host_tool_call" && bridge === null) {
          yield* connection.send({
            type: "host_tool_result",
            id: frame.id,
            isError: true,
            result: { content: [{ type: "text", text: "T3 has no handler for this host tool." }] },
          });
          return;
        }
        if (frame.type === "subagent_lifecycle" || frame.type === "subagent_progress") {
          const payload = frame.payload;
          const progress = field(payload, "progress");
          const index = number(payload, "index");
          const id =
            string(payload, "id") ?? string(progress, "id") ?? subagentIndices.get(index ?? -1);
          if (id !== undefined) {
            if (index !== undefined) subagentIndices.set(index, id);
            const owner = subagentOwners.get(id) ?? active?.events;
            if (owner !== undefined) {
              subagentOwners.set(id, owner);
              yield* owner.handle(frame);
            }
          }
          return;
        }
        if (yield* ui.handle(frame)) {
          if (active !== null) yield* updateSession(ui.hasPending ? "waiting" : "running");
          return;
        }
        const turn = active;
        if (turn === null) {
          if (frame.type === "agent_start" && !changingSession) {
            settled = false;
            yield* updateSession(
              "error",
              "OMP started work outside an active T3 turn; the process was stopped.",
            );
            yield* connection.terminate;
          }
          if (frame.type === "session_settled") {
            settled = true;
            yield* Deferred.succeed(settledSignal, undefined);
          }
          return;
        }
        yield* turn.events.handle(frame);
        if (frame.type === "agent_start") {
          turn.admitted = true;
          turn.sawActivity = true;
          settled = false;
          yield* updateSession("running");
        }
        if (frame.type === "agent_end") {
          if (frame.isTerminal === false) settled = false;
          const messages = frame.messages;
          if (Array.isArray(messages) && (frame.isTerminal !== false || frame.yielded === true)) {
            const last = messages.findLast((message) => string(message, "role") === "assistant");
            if (last !== undefined) {
              turn.nativeFailure =
                string(last, "stopReason") === "error"
                  ? (string(last, "errorMessage") ?? "OMP provider request failed.")
                  : null;
              turn.nativeInterrupted = string(last, "stopReason") === "aborted";
            }
          }
        }
        if (frame.type === "prompt_result" && frame.id === turn.promptId) {
          turn.sawResult = true;
          if (frame.status === "error") {
            turn.nativeFailure =
              string(frame.error, "message") ?? string(frame, "error") ?? "OMP prompt failed.";
            turn.retryable = field(frame.error, "retryable") === true;
          }
          if (frame.status === "aborted") turn.nativeInterrupted = true;
          if (frame.status === "completed") {
            turn.nativeFailure = null;
            turn.nativeInterrupted = false;
            turn.retryable = false;
          }
          settled = frame.sessionSettled === true;
        }
        if (frame.type === "response" && frame.id === turn.promptId && frame.success === true)
          turn.admitted = true;
        if (frame.type === "response" && frame.id === turn.promptId && frame.success === false)
          turn.failure = string(frame, "error") ?? "OMP rejected the prompt.";
        if (
          frame.type === "rpc_frame_error" ||
          frame.type === "extension_error" ||
          (frame.type === "notice" && string(frame, "level") === "error")
        ) {
          turn.failure =
            string(frame, "error") ?? string(frame, "message") ?? "OMP reported a runtime error.";
          if (
            frame.type === "rpc_frame_error" ||
            (frame.type === "extension_error" && string(frame, "extensionPath") === extensionPath)
          ) {
            yield* finish("failed", true);
            yield* connection.terminate;
            return;
          }
          yield* turn.events.failureItem(turn.failure);
        }
        if (frame.type === "session_settled" && !turn.sawActivity && !turn.sawResult) return;
        const completion = ompCompletion(frame, turn.promptId, turn.admitted);
        if (completion !== null) {
          settled = true;
          yield* finish(completion);
        }
      });
      yield* Stream.fromQueue(connection.events).pipe(
        Stream.runForEach((frame) => permit.withPermits(1)(handle(frame))),
        Effect.catch((cause) =>
          permit.withPermits(1)(
            Effect.gen(function* () {
              if (closed) return;
              if (active !== null) active.failure = `OMP transport ended: ${cause.message}`;
              yield* finish("failed", true);
              yield* updateSession("error", "OMP transport ended unexpectedly.");
            }),
          ),
        ),
        Effect.forkIn(scope),
      );
      yield* connection.exited.pipe(
        Effect.matchEffect({
          onFailure: () => stopScope,
          onSuccess: Effect.fn("OmpAdapter.onExit")(function* (code: number) {
            yield* stopScope;
            yield* permit.withPermits(1)(
              Effect.gen(function* () {
                if (closed) return;
                if (active !== null) active.failure = `OMP exited with code ${code}.`;
                yield* finish("failed", true);
                yield* ui.cancelAll();
                yield* updateSession(
                  code === 0 ? "stopped" : "error",
                  code === 0 ? null : `OMP exited with code ${code}.`,
                );
              }),
            );
          }),
        }),
        Effect.forkIn(scope),
      );
      yield* Effect.addFinalizer(() =>
        Effect.gen(function* () {
          closed = true;
          yield* ui.cancelAll();
          yield* Queue.end(queue);
        }),
      );
      if (mcpSession !== undefined)
        bridge = yield* makeOmpMcpBridge({
          connection,
          mcpSession,
          runtimeMode: input.runtimePolicy.runtimeMode,
        }).pipe(
          Effect.provideService(HttpClient.HttpClient, httpClient),
          Effect.mapError((cause) =>
            protocolError("OMP MCP bridge could not be initialized", cause),
          ),
        );
      const commands = yield* request({ type: "get_available_commands" });
      const catalog = field(commands, "commands");
      if (
        !Array.isArray(catalog) ||
        !catalog.some((command) => string(command, "name") === OMP_PERMISSION_GUARD_COMMAND)
      ) {
        yield* connection.terminate;
        return yield* protocolError(
          "OMP did not load the T3 permission extension; refusing to start an unguarded session",
        );
      }
      yield* publishCommands(commands);
      yield* request({ type: "set_ask_dialog", enabled: true });
      yield* request({ type: "set_subagent_subscription", level: "progress" });
      const initialState = yield* captureState();
      const initialModel = field(initialState.state, "model");
      const initialProvider = string(initialModel, "provider");
      const initialModelId = string(initialModel, "id");
      if (initialProvider && initialModelId)
        baselineModel = { provider: initialProvider, modelId: initialModelId };
      baselineThinking = string(initialState.state, "thinkingLevel") ?? "inherit";
      const models = field(yield* request({ type: "get_available_models" }), "models");
      if (Array.isArray(models)) for (const model of models) rememberModel(model);

      const applySelection = Effect.fn("OmpAdapter.applySelection")(function* (
        selection: ModelSelection,
      ) {
        if (appliedModel !== selection.model) {
          const model =
            selection.model === "default" ? baselineModel : parseOmpModelSlug(selection.model);
          if (selection.model !== "default" && model === null)
            return yield* protocolError("OMP model must use provider/model format");
          if (selection.model === "default" && model === null && appliedModel !== null)
            return yield* protocolError("OMP did not expose an initial model to restore");
          if (model !== null && (selection.model !== "default" || appliedModel !== null))
            rememberModel(yield* request({ type: "set_model", ...model }));
          appliedThinking = null;
          appliedModel = selection.model;
          session = { ...session, model: selection.model };
        }
        const thinking =
          getModelSelectionStringOptionValue(selection, "thinking") ?? baselineThinking;
        if (thinking !== appliedThinking) {
          yield* request({ type: "set_thinking_level", level: thinking });
          appliedThinking = thinking;
        }
      });
      const register = Effect.fn("OmpAdapter.ensureThread")(function* (
        threadInput: ProviderAdapter.ProviderAdapterV2EnsureThreadInput,
        publish = true,
      ) {
        if (threadInput.runtimePolicy.runtimeMode !== input.runtimePolicy.runtimeMode)
          return yield* protocolError("OMP runtime mode changes require a new session");
        const existing = threadInput.existingProviderThread;
        const resumeFile = existing?.nativeThreadRef?.nativeId;
        const needsSwitch = resumeFile !== undefined && resumeFile !== currentFile;
        const needsNew =
          threadInput.forceNewNativeThread === true ||
          (resumeFile === undefined &&
            (!registered
              ? input.initialNativeThreadId === undefined
              : thread?.appThreadId !== threadInput.threadId));
        if (needsSwitch || needsNew) {
          const result = yield* lifecycle(
            needsNew
              ? { type: "new_session" }
              : { type: "switch_session", sessionPath: resumeFile },
          );
          if (field(result, "cancelled") === true)
            return yield* protocolError("An OMP extension cancelled the session transition");
          thread = null;
          appliedModel = null;
          appliedThinking = null;
        }
        const captured = yield* captureState();
        if (resumeFile !== undefined && !needsNew && captured.file !== resumeFile)
          return yield* protocolError("OMP resumed a different session than requested");
        const now = yield* DateTime.now;
        thread =
          existing !== undefined
            ? {
                ...existing,
                providerSessionId: input.providerSessionId,
                providerInstanceId: options.instanceId,
                appThreadId: threadInput.threadId,
                nativeThreadRef: ompRef(captured.file),
                status: "idle",
                updatedAt: now,
              }
            : {
                id: ids.derive.providerThread({
                  driver: OMP_PROVIDER,
                  providerInstanceId: options.instanceId,
                  nativeThreadId: captured.file,
                }),
                driver: OMP_PROVIDER,
                providerInstanceId: options.instanceId,
                providerSessionId: input.providerSessionId,
                appThreadId: threadInput.threadId,
                ownerNodeId: null,
                nativeThreadRef: ompRef(captured.file),
                nativeConversationHeadRef: null,
                status: "idle",
                firstRunOrdinal: null,
                lastRunOrdinal: null,
                handoffIds: [],
                forkedFrom: null,
                pendingBackgroundTasks: [],
                createdAt: now,
                updatedAt: now,
              };
        registered = true;
        const branch = yield* readBranch();
        thread = {
          ...thread,
          nativeConversationHeadRef: branch.length ? ompRef(branch.at(-1)!.id) : null,
        };
        if (publish)
          yield* emit({
            type: "provider_thread.updated",
            driver: OMP_PROVIDER,
            providerThread: thread,
          });
        return thread;
      });
      const payload = Effect.fn("OmpAdapter.promptPayload")(function* (
        text: string,
        attachments: ReadonlyArray<ChatAttachment>,
      ) {
        const message = yield* providerMessageTextWithAttachmentPaths({
          text,
          attachments,
          resolveAttachmentPath: host.resolveAttachmentPath,
          resolveExtractedTextPath: host.resolveExtractedTextPath,
        }).pipe(
          Effect.mapError((cause) =>
            protocolError("OMP attachment prompt could not be resolved", cause),
          ),
        );
        const images: Array<{ type: "image"; data: string; mimeType: string }> = [];
        for (const attachment of attachments) {
          if (!attachment.mimeType.startsWith("image/")) continue;
          const path = host.resolveAttachmentPath(attachment);
          if (path === null)
            return yield* protocolError("OMP image attachment could not be resolved");
          const bytes = yield* fileSystem
            .readFile(path)
            .pipe(
              Effect.mapError((cause) =>
                protocolError("OMP image attachment could not be read", cause),
              ),
            );
          images.push({
            type: "image",
            data: Buffer.from(bytes).toString("base64"),
            mimeType: attachment.mimeType,
          });
        }
        return { message, ...(images.length ? { images } : {}) };
      });
      const runtime: ProviderAdapter.ProviderAdapterV2SessionRuntime = {
        instanceId: options.instanceId,
        driver: OMP_PROVIDER,
        providerSessionId: input.providerSessionId,
        get providerSession() {
          return session;
        },
        events: Stream.fromQueue(queue),
        hasPendingBackgroundWork: Effect.sync(
          () =>
            !settled ||
            Array.from(subagentOwners.values()).some((owner) => owner.hasLiveSubagents()),
        ),
        hasPendingBackgroundWorkForThread: (wanted) =>
          Effect.sync(
            () =>
              (thread?.id === wanted.id && !settled) ||
              Array.from(subagentOwners.values()).some(
                (owner) => owner.providerThreadId === wanted.id && owner.hasLiveSubagents(),
              ),
          ),
        getModelContextWindow: (selection) =>
          selection.instanceId !== options.instanceId
            ? undefined
            : modelWindows.get(
                selection.model === "default" && baselineModel
                  ? `${baselineModel.provider}/${baselineModel.modelId}`
                  : selection.model,
              ),
        ensureThread: (ensure) => register(ensure).pipe(sessionChange),
        resumeThread: (resume) =>
          register({
            threadId: resume.threadId ?? resume.providerThread.appThreadId ?? input.threadId,
            modelSelection: resume.modelSelection ?? input.modelSelection,
            runtimePolicy: resume.runtimePolicy ?? input.runtimePolicy,
            existingProviderThread: resume.providerThread,
          }).pipe(sessionChange),
        injectHistory: () => Effect.succeed(false),
        compactThread: (turnInput) =>
          runtime.startTurn({ ...turnInput, message: { ...turnInput.message, text: "/compact" } }),
        startTurn: Effect.fn("OmpAdapter.startTurn")(function* (turnInput) {
          const hosted = thread;
          if (
            closed ||
            hosted === null ||
            hosted.appThreadId !== turnInput.threadId ||
            hosted.nativeThreadRef?.nativeId !== turnInput.providerThread.nativeThreadRef?.nativeId
          )
            return yield* protocolError("OMP turn requested for an unavailable thread");
          if (active !== null || !settled || changingSession)
            return yield* new ProviderAdapter.ProviderAdapterProtocolError({
              driver: OMP_PROVIDER,
              detail: "OMP still has an active turn or pending work",
              reason: "previous_turn_active",
            });
          if (turnInput.runtimePolicy.runtimeMode !== input.runtimePolicy.runtimeMode)
            return yield* protocolError("OMP runtime mode changes require a new session");
          if (turnInput.runtimePolicy.sandboxPolicy !== undefined)
            return yield* protocolError("OMP does not implement an operating-system sandbox");
          yield* applySelection(turnInput.modelSelection);
          const prompt = yield* payload(turnInput.message.text, turnInput.message.attachments);
          const context = turnInput.appContext
            ?.map((entry) => entry.text)
            .filter(Boolean)
            .join("\n\n");
          const message =
            context && !prompt.message.trimStart().startsWith("/")
              ? `${context}\n\n${prompt.message}`
              : prompt.message;
          yield* permit.withPermits(1)(
            Effect.gen(function* () {
              if (thread !== hosted || active !== null || closed || changingSession || !settled)
                return yield* protocolError("OMP turn ownership changed during preparation");
              // Fork adoption preserves the orchestrator-allocated binding identity.
              thread = turnInput.providerThread;
              const now = yield* DateTime.now;
              const nativeTurnId = `${turnInput.providerThread.id}:attempt:${turnInput.attemptId}`;
              const providerTurn: OrchestrationV2ProviderTurn = {
                id: ids.derive.providerTurn({ driver: OMP_PROVIDER, nativeTurnId }),
                providerThreadId: turnInput.providerThread.id,
                nodeId: turnInput.rootNodeId,
                runAttemptId: turnInput.attemptId,
                nativeTurnRef: ompRef(nativeTurnId, "weak"),
                ordinal: turnInput.providerTurnOrdinal,
                status: "running",
                startedAt: now,
                completedAt: null,
              };
              const turn: ActiveTurn = {
                input: turnInput,
                providerTurn,
                promptId: `t3-prompt-${++nextPromptId}`,
                events: makeOmpTurnEvents({ input: turnInput, providerTurn, ids, emit, scope }),
                interrupted: false,
                failure: null,
                nativeFailure: null,
                nativeInterrupted: false,
                retryable: false,
                admitted: false,
                sawActivity: false,
                sawResult: false,
              };
              settledSignal = yield* Deferred.make<void>();
              active = turn;
              settled = false;
              thread = turnInput.providerThread;
              yield* emit({
                type: "provider_turn.updated",
                driver: OMP_PROVIDER,
                threadId: turnInput.threadId,
                providerTurn,
              });
              yield* updateThread({
                status: "active",
                firstRunOrdinal: thread.firstRunOrdinal ?? turnInput.runOrdinal,
                lastRunOrdinal: turnInput.runOrdinal,
              });
              yield* updateSession("running");
              yield* connection
                .send({
                  id: turn.promptId,
                  type: "prompt",
                  ...prompt,
                  message,
                  streamingBehavior: "steer",
                })
                .pipe(
                  Effect.mapError((cause) => protocolError("OMP prompt could not be sent", cause)),
                  Effect.tapError(() => finish("failed", true)),
                );
            }),
          );
        }),
        steerTurn: Effect.fn("OmpAdapter.steerTurn")(function* (steer) {
          const turn = active;
          if (
            turn === null ||
            !ensureHosted(steer.providerThread) ||
            turn.providerTurn.id !== steer.providerTurnId
          )
            return yield* protocolError("OMP steer requested for an inactive turn");
          const prompt = yield* payload(steer.message.text, steer.message.attachments);
          yield* permit.withPermits(1)(
            Effect.gen(function* () {
              if (
                active !== turn ||
                !ensureHosted(steer.providerThread) ||
                turn.interrupted ||
                closed ||
                changingSession
              )
                return yield* protocolError(
                  "OMP steer ownership changed during attachment preparation",
                );
              yield* request({ type: "steer", ...prompt });
            }),
          );
        }),
        interruptTurn: Effect.fn("OmpAdapter.interruptTurn")(function* (interrupt) {
          const turn = active;
          if (
            !ensureHosted(interrupt.providerThread) ||
            (turn === null
              ? interrupt.requestRuntimeRestart !== true
              : turn.providerTurn.id !== interrupt.providerTurnId)
          )
            return yield* protocolError("OMP interrupt requested for an inactive turn");
          if (turn !== null) turn.interrupted = true;
          const stopped = settledSignal;
          yield* permit.withPermits(1)(ui.cancelAll());
          yield* request({ type: "abort" }, 10_000).pipe(
            Effect.catch((cause) =>
              Effect.gen(function* () {
                yield* connection.terminate;
                yield* permit.withPermits(1)(finish("interrupted", true));
                return yield* cause;
              }),
            ),
          );
          const state = yield* request({ type: "get_state" });
          if (field(state, "isSettled") === true) {
            yield* permit.withPermits(1)(
              Effect.gen(function* () {
                settled = true;
                yield* finish("interrupted");
              }),
            );
            return;
          }
          const completed = yield* Deferred.await(stopped).pipe(
            Effect.timeoutOption("10 seconds"),
            Effect.provideService(Clock.Clock, Clock.Clock.defaultValue()),
          );
          if (Option.isSome(completed)) return;
          if (interrupt.requestRuntimeRestart === true) {
            const branch = yield* readBranch().pipe(Effect.orElseSucceed(() => []));
            const last = branch.findLast((entry) => entry.type === "message");
            if (turn !== null && last !== undefined)
              turn.providerTurn = { ...turn.providerTurn, nativeTurnRef: ompRef(last.id) };
            yield* connection.terminate;
            yield* permit.withPermits(1)(finish("interrupted", true));
            return;
          }
          return yield* protocolError("OMP did not settle after abort within 10 seconds");
        }),
        stopSubagent: Effect.fn("OmpAdapter.stopSubagent")(function* (stop) {
          if (!ensureHosted(stop.providerThread))
            return yield* protocolError("OMP subagent requested for an unhosted thread");
          const agent = subagentOwners.get(stop.nativeTaskId)?.subagent(stop.nativeTaskId);
          if (
            stop.requestedAt !== undefined &&
            agent?.startedAt != null &&
            DateTime.toEpochMillis(agent.startedAt) > DateTime.toEpochMillis(stop.requestedAt)
          )
            return;
          const result = yield* request({ type: "cancel_subagent", subagentId: stop.nativeTaskId });
          if (field(result, "cancelled") !== true)
            return yield* protocolError("OMP subagent is not running");
        }),
        respondToRuntimeRequest: (response) =>
          permit
            .withPermits(1)(
              Effect.gen(function* () {
                yield* ui.respond(response);
                if (active !== null) yield* updateSession(ui.hasPending ? "waiting" : "running");
              }),
            )
            .pipe(
              Effect.mapError(
                (cause) =>
                  new ProviderAdapter.ProviderAdapterRuntimeRequestResponseError({
                    driver: OMP_PROVIDER,
                    requestId: response.requestId,
                    cause,
                  }),
              ),
            ),
        readThreadSnapshot: ({ providerThread }) => snapshot(providerThread),
        rollbackThread: Effect.fn("OmpAdapter.rollbackThread")(function* (rollback) {
          if (!ensureHosted(rollback.providerThread))
            return yield* protocolError("OMP rollback requested for an unhosted thread");
          const branch = yield* readBranch();
          if (!branch.length) return yield* snapshot(rollback.providerThread, true);
          let command: OmpRecord;
          if (rollback.target.type === "thread_start") {
            const firstUser = branch.find(
              (entry) => entry.type === "message" && string(entry.message, "role") === "user",
            );
            if (firstUser === undefined)
              return yield* protocolError("OMP has no user boundary for thread-start rollback");
            command = { type: "navigate_tree", entryId: firstUser.id, summarize: false };
          } else {
            const ref = rollback.target.providerTurn.nativeTurnRef;
            if (ref?.strength !== "strong" || !branch.some((entry) => entry.id === ref.nativeId))
              return yield* protocolError("OMP rollback target has no captured durable boundary");
            command = { type: "fork", entryId: ref.nativeId };
          }
          const result = yield* lifecycle(command);
          if (field(result, "cancelled") === true)
            return yield* protocolError("An OMP extension cancelled rollback");
          const state = yield* captureState();
          const kept = yield* readBranch();
          const keptIds = new Set(kept.map((entry) => entry.id));
          for (const [id, turn] of turns)
            if (
              rollback.target.type === "thread_start" ||
              (turn.nativeTurnRef?.strength === "strong" &&
                turn.nativeTurnRef.nativeId !== null &&
                !keptIds.has(turn.nativeTurnRef.nativeId))
            )
              turns.delete(id);
          yield* updateThread({
            nativeThreadRef: ompRef(state.file),
            nativeConversationHeadRef: kept.length ? ompRef(kept.at(-1)!.id) : null,
            status: "idle",
          });
          return yield* snapshot(thread!, true);
        }, sessionChange),
        forkThread: Effect.fn("OmpAdapter.forkThread")(function* (fork) {
          const source = fork.sourceProviderThread;
          const sourceFile = source.nativeThreadRef?.nativeId;
          if (sourceFile == null)
            return yield* protocolError("OMP fork source has no durable session file");
          const target =
            fork.providerTurnId === undefined
              ? undefined
              : fork.sourceProviderTurns?.find((turn) => turn.id === fork.providerTurnId);
          if (
            fork.providerTurnId !== undefined &&
            (target?.nativeTurnRef?.strength !== "strong" || target.nativeTurnRef.nativeId === null)
          )
            return yield* protocolError("OMP fork target has no captured durable boundary");
          const forkLaunch = buildOmpRpcLaunch({
            launchArgs,
            environment: options.environment,
            sessionPath: sourceFile,
            disableExtensions: true,
            disableTools: true,
          });
          const forkFile = yield* Effect.scoped(
            Effect.gen(function* () {
              const forkConnection = yield* makeOmpRpcConnection({
                command: options.settings.binaryPath || "omp",
                args: forkLaunch.args,
                env: forkLaunch.env,
                cwd: fork.runtimePolicy?.cwd ?? cwd,
              });
              yield* Stream.fromQueue(forkConnection.events).pipe(
                Stream.runDrain,
                Effect.ignore,
                Effect.forkScoped,
              );
              const result = yield* forkConnection.request({
                type: "fork",
                ...(target?.nativeTurnRef === undefined || target.nativeTurnRef === null
                  ? {}
                  : { entryId: target.nativeTurnRef.nativeId }),
              });
              if (field(result, "cancelled") === true)
                return yield* protocolError("OMP cancelled the session fork");
              const state = yield* forkConnection.request({ type: "get_state" });
              const file = string(state, "sessionFile");
              if (file === undefined || file === sourceFile)
                return yield* protocolError("OMP fork did not create a distinct session file");
              return file;
            }),
          ).pipe(
            Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, spawner),
            Effect.mapError(
              (cause) =>
                new ProviderAdapter.ProviderAdapterForkThreadError({
                  driver: OMP_PROVIDER,
                  providerThreadId: source.id,
                  cause,
                }),
            ),
          );
          const now = yield* DateTime.now;
          const forked: OrchestrationV2ProviderThread = {
            ...source,
            id: ids.derive.providerThread({
              driver: OMP_PROVIDER,
              providerInstanceId: options.instanceId,
              nativeThreadId: forkFile,
            }),
            appThreadId: fork.targetThreadId,
            providerSessionId: input.providerSessionId,
            providerInstanceId: options.instanceId,
            ownerNodeId: fork.ownerNodeId ?? null,
            nativeThreadRef: ompRef(forkFile),
            nativeConversationHeadRef: target?.nativeTurnRef ?? source.nativeConversationHeadRef,
            status: "idle" as const,
            firstRunOrdinal: null,
            lastRunOrdinal: null,
            handoffIds: [],
            pendingBackgroundTasks: [],
            forkedFrom: {
              providerThreadId: source.id,
              ...(fork.providerTurnId === undefined ? {} : { providerTurnId: fork.providerTurnId }),
            },
            createdAt: now,
            updatedAt: now,
          };
          return yield* register(
            {
              threadId: fork.targetThreadId,
              modelSelection: fork.modelSelection ?? input.modelSelection,
              runtimePolicy: fork.runtimePolicy ?? input.runtimePolicy,
              existingProviderThread: forked,
            },
            false,
          );
        }, sessionChange),
      };
      return runtime;
    }),
  });
});

export type OmpAdapterV2DriverEnv =
  | ChildProcessSpawner.ChildProcessSpawner
  | FileSystem.FileSystem
  | HttpClient.HttpClient
  | IdAllocator.IdAllocatorV2
  | McpProviderSessions.McpProviderSessions
  | ProviderHost.ProviderHost;

export const OmpAdapterV2Driver: ProviderAdapterDriver<OmpSettings, OmpAdapterV2DriverEnv> = {
  driverKind: OMP_PROVIDER,
  configSchema: OmpSettings,
  defaultConfig: () => decodeOmpSettings({}),
  create: Effect.fn("OmpAdapterV2Driver.create")(
    function* (input: ProviderAdapterDriverCreateInput<OmpSettings>) {
      const environment = yield* HostProcess.Environment;
      return yield* makeOmpAdapterV2({
        instanceId: input.instanceId,
        settings: { ...input.config, enabled: input.enabled },
        environment: yield* mergeProviderInstanceEnvironment(input.environment, environment),
      });
    },
    (effect, input) =>
      effect.pipe(
        Effect.mapError(
          (cause) =>
            new ProviderAdapterDriverCreateError({
              driver: OMP_PROVIDER,
              instanceId: input.instanceId,
              detail: "Failed to create OMP adapter.",
              cause,
            }),
        ),
      ),
  ),
};
