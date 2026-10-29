import {
  ProviderDriverKind,
  type OrchestrationV2ExecutionNode,
  type OrchestrationV2ProviderFailure,
  type OrchestrationV2ProviderTurn,
  type OrchestrationV2Subagent,
  type OrchestrationV2TurnItem,
} from "@t3tools/contracts";
import type * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import type * as ProviderAdapter from "@t3tools/provider-core/server/ProviderAdapter";
import { mcpToolPresentation } from "@t3tools/provider-core/server/mcpToolPresentation";
import { makeProviderFailure } from "@t3tools/provider-core/server/failure";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import type * as Scope from "effect/Scope";
import { contentText, field, number, string, type OmpRecord } from "./frames.ts";

export const OMP_PROVIDER = ProviderDriverKind.make("omp");
export const ompRef = (nativeId: string, strength: "strong" | "weak" = "strong") => ({
  driver: OMP_PROVIDER,
  nativeId,
  strength,
});
export type OmpEmit = (event: ProviderAdapter.ProviderAdapterV2Event) => Effect.Effect<void>;

export interface OmpTurnEvents {
  readonly ordinal: (id: string) => number;
  readonly nextOrdinal: number;
  readonly providerThreadId: OmpTurnEventInput["providerThread"]["id"];
  readonly subagent: (nativeTaskId: string) => OrchestrationV2Subagent | undefined;
  readonly hasLiveSubagents: () => boolean;
  readonly handle: (frame: OmpRecord) => Effect.Effect<void>;
  readonly finish: (status: "completed" | "failed" | "interrupted") => Effect.Effect<void>;
  readonly failureItem: (
    message: string,
    retryable?: boolean,
  ) => Effect.Effect<{
    readonly failure: OrchestrationV2ProviderFailure;
    readonly ordinal: number;
  }>;
}

export type OmpTurnEventInput = Pick<
  ProviderAdapter.ProviderAdapterV2TurnInput,
  "threadId" | "runId" | "rootNodeId" | "providerTurnOrdinal" | "modelSelection"
> & {
  readonly providerThread: Pick<ProviderAdapter.ProviderAdapterV2TurnInput["providerThread"], "id">;
};

interface StreamItem {
  readonly nativeId: string;
  readonly kind: "assistant_message" | "reasoning";
  readonly startedAt: DateTime.Utc;
  text: string;
  completed: boolean;
  scheduled: boolean;
}

export function makeOmpTurnEvents(options: {
  readonly input: OmpTurnEventInput;
  readonly providerTurn: OrchestrationV2ProviderTurn;
  readonly ids: IdAllocator.IdAllocatorV2["Service"];
  readonly emit: OmpEmit;
  readonly scope: Scope.Scope;
}): OmpTurnEvents {
  const { input, ids, providerTurn, emit, scope } = options;
  const items = new Map<string, StreamItem>();
  const ordinals = new Map<string, number>();
  const tools = new Map<
    string,
    { args: unknown; name: string; startedAt: DateTime.Utc; completed: boolean }
  >();
  const subagents = new Map<string, OrchestrationV2Subagent>();
  const subagentIndices = new Map<number, string>();
  let nextOrdinal = input.providerTurnOrdinal * 100 + 1;
  let messageOrdinal = 0;
  let currentMessageId = "0";
  const nativeId = (id: string) => `${providerTurn.id}:${id}`;
  const ordinal = (id: string) => {
    let value = ordinals.get(id);
    if (value === undefined) {
      value = nextOrdinal++;
      ordinals.set(id, value);
    }
    return value;
  };
  const base = (id: string, startedAt: DateTime.Utc, updatedAt: DateTime.Utc) => ({
    id: ids.derive.turnItemFromProviderItem({ driver: OMP_PROVIDER, nativeItemId: id }),
    threadId: input.threadId,
    runId: input.runId,
    nodeId: ids.derive.nodeFromProviderItem({ driver: OMP_PROVIDER, nativeItemId: id }),
    providerThreadId: input.providerThread.id,
    providerTurnId: providerTurn.id,
    nativeItemRef: ompRef(id, "weak"),
    parentItemId: null,
    ordinal: ordinal(id),
    startedAt,
    updatedAt,
  });
  const node = (
    id: string,
    kind: OrchestrationV2ExecutionNode["kind"],
    status: OrchestrationV2ExecutionNode["status"],
    startedAt: DateTime.Utc,
    completedAt: DateTime.Utc | null,
  ) =>
    emit({
      type: "node.updated",
      driver: OMP_PROVIDER,
      node: {
        id: ids.derive.nodeFromProviderItem({ driver: OMP_PROVIDER, nativeItemId: id }),
        threadId: input.threadId,
        runId: input.runId,
        parentNodeId: input.rootNodeId,
        rootNodeId: input.rootNodeId,
        kind,
        status,
        countsForRun: false,
        providerThreadId: input.providerThread.id,
        providerTurnId: providerTurn.id,
        nativeItemRef: ompRef(id, "weak"),
        runtimeRequestId: null,
        checkpointScopeId: null,
        startedAt,
        completedAt,
      },
    });
  // Streaming updates share one flush per item to avoid persisting every token.
  const emitStream = Effect.fnUntraced(function* (item: StreamItem) {
    const now = yield* DateTime.now;
    const streaming = !item.completed;
    const common = {
      ...base(item.nativeId, item.startedAt, now),
      status: streaming ? ("running" as const) : ("completed" as const),
      title: null,
      completedAt: streaming ? null : now,
      text: item.text,
      streaming,
    };
    yield* node(item.nativeId, item.kind, common.status, item.startedAt, common.completedAt);
    if (item.kind === "reasoning") {
      yield* emit({
        type: "turn_item.updated",
        driver: OMP_PROVIDER,
        turnItem: { ...common, type: "reasoning" },
      });
      return;
    }
    const messageId = ids.derive.messageFromProviderItem({
      driver: OMP_PROVIDER,
      nativeItemId: item.nativeId,
    });
    yield* emit({
      type: "turn_item.updated",
      driver: OMP_PROVIDER,
      turnItem: { ...common, type: "assistant_message", messageId },
    });
    yield* emit({
      type: "message.updated",
      driver: OMP_PROVIDER,
      message: {
        id: messageId,
        threadId: input.threadId,
        runId: input.runId,
        nodeId: common.nodeId,
        role: "assistant",
        text: item.text,
        attachments: [],
        streaming,
        createdBy: "agent",
        creationSource: "provider",
        createdAt: item.startedAt,
        updatedAt: now,
      },
    });
  });
  const complete = Effect.fnUntraced(function* (item: StreamItem) {
    if (item.completed) return;
    item.completed = true;
    if (item.text.length > 0) yield* emitStream(item);
  });
  const streamItem = Effect.fnUntraced(function* (
    kind: StreamItem["kind"],
    messageId: string,
    index: number,
  ) {
    const id = nativeId(`message:${messageId}:${index}`);
    const existing = items.get(id);
    if (existing !== undefined) return existing;
    const item: StreamItem = {
      nativeId: id,
      kind,
      startedAt: yield* DateTime.now,
      text: "",
      completed: false,
      scheduled: false,
    };
    items.set(id, item);
    return item;
  });
  const schedule = Effect.fnUntraced(function* (item: StreamItem) {
    if (item.scheduled || item.completed) return;
    item.scheduled = true;
    yield* Effect.sleep("50 millis").pipe(
      Effect.andThen(
        Effect.suspend(() => {
          item.scheduled = false;
          return item.completed ? Effect.void : emitStream(item);
        }),
      ),
      Effect.forkIn(scope),
    );
  });
  const tool = Effect.fnUntraced(function* (frame: OmpRecord, phase: "start" | "update" | "end") {
    const callId = string(frame, "toolCallId");
    if (callId === undefined) return;
    const id = nativeId(`tool:${callId}`);
    const now = yield* DateTime.now;
    const previous = tools.get(callId);
    const state = {
      args: frame.args ?? previous?.args ?? {},
      name: string(frame, "toolName") ?? previous?.name ?? "tool",
      startedAt: previous?.startedAt ?? now,
      completed: phase === "end",
    };
    tools.set(callId, state);
    const result = phase === "end" ? frame.result : frame.partialResult;
    const output = contentText(field(result, "content"));
    const status = state.completed ? (frame.isError === true ? "failed" : "completed") : "running";
    const common = {
      ...base(id, state.startedAt, now),
      status,
      completedAt: state.completed ? now : null,
    } as const;
    yield* node(id, "tool_call", status, state.startedAt, common.completedAt);
    let item: OrchestrationV2TurnItem;
    if (state.name === "bash" || state.name === "powershell") {
      const exitCode =
        number(field(result, "details"), "exitCode") ??
        number(field(result, "structuredContent"), "exit_code");
      item = {
        ...common,
        type: "command_execution",
        title: state.name,
        input: string(state.args, "command") ?? "",
        output,
        ...(exitCode === undefined ? {} : { exitCode }),
      };
    } else if (
      (state.name === "edit" || state.name === "write") &&
      (string(state.args, "path") ?? string(state.args, "file_path")) !== undefined
    ) {
      item = {
        ...common,
        type: "file_change",
        title: state.name,
        fileName: (string(state.args, "path") ?? string(state.args, "file_path"))!,
        ...(string(field(result, "details"), "patch") === undefined
          ? {}
          : { diffStr: string(field(result, "details"), "patch")! }),
        ...(state.name === "write" ? { newStr: string(state.args, "content") ?? "" } : {}),
      };
    } else {
      item = {
        ...common,
        type: "dynamic_tool",
        title: state.name,
        toolName: state.name,
        ...mcpToolPresentation({ toolName: state.name }),
        input: state.args,
        ...(result === undefined
          ? {}
          : {
              output: {
                content: field(result, "content") ?? [],
                ...(field(result, "structuredContent") === undefined
                  ? {}
                  : { structuredContent: field(result, "structuredContent") }),
              },
            }),
      };
    }
    yield* emit({ type: "turn_item.updated", driver: OMP_PROVIDER, turnItem: item });
  });
  const subagent = Effect.fnUntraced(function* (frame: OmpRecord) {
    const payload = frame.payload;
    const progress = field(payload, "progress");
    const id =
      string(payload, "id") ??
      string(progress, "id") ??
      subagentIndices.get(number(payload, "index") ?? -1);
    if (id === undefined) return;
    const index = number(payload, "index");
    if (index !== undefined) subagentIndices.set(index, id);
    const previous = subagents.get(id);
    const nativeStatus = string(payload, "status") ?? string(progress, "status") ?? "running";
    const status =
      nativeStatus === "started"
        ? "running"
        : nativeStatus === "aborted"
          ? "cancelled"
          : nativeStatus === "completed" || nativeStatus === "failed" || nativeStatus === "pending"
            ? nativeStatus
            : "running";
    const now = yield* DateTime.now;
    const recentOutput = field(progress, "recentOutput");
    const text = Array.isArray(recentOutput)
      ? recentOutput.filter((line): line is string => typeof line === "string").join("\n")
      : undefined;
    const parentTool = string(payload, "parentToolCallId");
    const entity: OrchestrationV2Subagent = {
      id:
        previous?.id ??
        ids.derive.nodeFromProviderItem({
          driver: OMP_PROVIDER,
          nativeItemId: nativeId(`subagent:${id}`),
        }),
      threadId: input.threadId,
      runId: input.runId,
      parentNodeId:
        previous?.parentNodeId ??
        (parentTool === undefined
          ? input.rootNodeId
          : ids.derive.nodeFromProviderItem({
              driver: OMP_PROVIDER,
              nativeItemId: nativeId(`tool:${parentTool}`),
            })),
      origin: "provider_native",
      createdBy: "agent",
      driver: OMP_PROVIDER,
      providerInstanceId: input.modelSelection.instanceId,
      providerThreadId: input.providerThread.id,
      childThreadId: null,
      nativeTaskRef: ompRef(id),
      prompt:
        string(payload, "assignment") ??
        string(payload, "task") ??
        string(progress, "task") ??
        previous?.prompt ??
        "",
      title: string(payload, "description") ?? string(payload, "agent") ?? previous?.title ?? id,
      model: string(progress, "resolvedModel") ?? previous?.model ?? null,
      status,
      progress:
        string(progress, "stage") ??
        string(progress, "lastIntent") ??
        text ??
        previous?.progress ??
        "",
      result:
        status === "completed" || status === "failed" ? (text ?? previous?.result ?? null) : null,
      startedAt: nativeStatus === "started" ? now : (previous?.startedAt ?? now),
      completedAt:
        status === "completed" || status === "failed" || status === "cancelled" ? now : null,
      updatedAt: now,
    };
    subagents.set(id, entity);
    yield* emit({ type: "subagent.updated", driver: OMP_PROVIDER, subagent: entity });
    yield* emit({
      type: "turn_item.updated",
      driver: OMP_PROVIDER,
      turnItem: {
        ...base(nativeId(`subagent:${id}`), entity.startedAt ?? now, now),
        type: "subagent",
        status: entity.status,
        title: entity.title,
        completedAt: entity.completedAt ?? null,
        subagentId: entity.id,
        origin: "provider_native",
        driver: OMP_PROVIDER,
        providerInstanceId: input.modelSelection.instanceId,
        childThreadId: null,
        prompt: entity.prompt,
        progress: entity.progress,
        result: entity.result,
      },
    });
  });
  const notice = Effect.fnUntraced(function* (message: string) {
    if (!message) return;
    const now = yield* DateTime.now;
    const id = nativeId(`notice:${nextOrdinal}`);
    yield* emit({
      type: "turn_item.updated",
      driver: OMP_PROVIDER,
      turnItem: {
        ...base(id, now, now),
        type: "system_notice",
        status: "completed",
        title: null,
        completedAt: now,
        message,
      },
    });
  });
  return {
    ordinal,
    get nextOrdinal() {
      return nextOrdinal;
    },
    providerThreadId: input.providerThread.id,
    subagent: (nativeTaskId) => subagents.get(nativeTaskId),
    hasLiveSubagents: () =>
      Array.from(subagents.values()).some(
        (agent) =>
          agent.status === "running" || agent.status === "pending" || agent.status === "waiting",
      ),
    handle: Effect.fnUntraced(function* (frame: OmpRecord) {
      const type = string(frame, "type");
      if (type === "subagent_lifecycle" || type === "subagent_progress") {
        yield* subagent(frame);
        return;
      }
      if (
        type === "tool_execution_start" ||
        type === "tool_execution_update" ||
        type === "tool_execution_end"
      ) {
        yield* tool(
          frame,
          type === "tool_execution_start"
            ? "start"
            : type === "tool_execution_end"
              ? "end"
              : "update",
        );
        return;
      }
      if (type === "command_output") {
        yield* notice(string(frame, "text") ?? string(frame, "output") ?? "");
        return;
      }
      if (type === "extension_ui_request" && frame.method === "notify") {
        yield* notice(string(frame, "message") ?? "");
        return;
      }
      if (type === "message_start") {
        currentMessageId = string(frame, "messageId") ?? String(++messageOrdinal);
        return;
      }
      if (type !== "message_update" && type !== "message_end") return;
      if (string(frame.message, "role") !== "assistant") return;
      const messageId = string(frame, "messageId") ?? currentMessageId;
      if (type === "message_update") {
        const event = frame.assistantMessageEvent;
        const eventType = string(event, "type");
        if (eventType === "text_delta" || eventType === "thinking_delta") {
          const item = yield* streamItem(
            eventType === "text_delta" ? "assistant_message" : "reasoning",
            messageId,
            number(event, "contentIndex") ?? 0,
          );
          item.text += string(event, "delta") ?? "";
          yield* schedule(item);
        }
      } else {
        const content = field(frame.message, "content");
        if (Array.isArray(content))
          for (let index = 0; index < content.length; index++) {
            const block = content[index];
            const kind = string(block, "type");
            if (kind !== "text" && kind !== "thinking") continue;
            const item = yield* streamItem(
              kind === "text" ? "assistant_message" : "reasoning",
              messageId,
              index,
            );
            item.text = string(block, kind === "text" ? "text" : "thinking") ?? item.text;
            yield* complete(item);
          }
      }
    }),
    finish: Effect.fnUntraced(function* (status: "completed" | "failed" | "interrupted") {
      for (const item of items.values()) yield* complete(item);
      const now = yield* DateTime.now;
      for (const [id, state] of tools)
        if (!state.completed) {
          const scopedId = nativeId(`tool:${id}`);
          yield* node(
            scopedId,
            "tool_call",
            status === "completed" ? "interrupted" : status,
            state.startedAt,
            now,
          );
          yield* emit({
            type: "turn_item.updated",
            driver: OMP_PROVIDER,
            turnItem: {
              ...base(scopedId, state.startedAt, now),
              type: "dynamic_tool",
              title: state.name,
              toolName: state.name,
              input: state.args,
              status: status === "completed" ? "interrupted" : status,
              completedAt: now,
            },
          });
        }
      items.clear();
      tools.clear();
    }),
    failureItem: Effect.fnUntraced(function* (message: string, retryable = false) {
      const now = yield* DateTime.now;
      const id = nativeId("failure");
      const failure = makeProviderFailure({ message, class: "provider_error", retryable });
      yield* emit({
        type: "turn_item.updated",
        driver: OMP_PROVIDER,
        turnItem: {
          ...base(id, now, now),
          type: "error",
          title: null,
          status: "failed",
          completedAt: now,
          failure,
        },
      });
      return { failure, ordinal: ordinal(id) };
    }),
  };
}
