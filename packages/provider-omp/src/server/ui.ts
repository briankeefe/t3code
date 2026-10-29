import type {
  OrchestrationV2ExecutionNode,
  OrchestrationV2ProviderThread,
  OrchestrationV2RuntimeRequest,
  OrchestrationV2TurnItem,
} from "@t3tools/contracts";
import type * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import type * as ProviderAdapter from "@t3tools/provider-core/server/ProviderAdapter";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import { OMP_PROVIDER, ompRef, type OmpEmit } from "./events.ts";
import { ompQuestions, ompUiResponse, string, type OmpRecord } from "./frames.ts";
import type { OmpRpcConnection } from "./rpc.ts";

interface Pending {
  readonly frame: OmpRecord;
  readonly request: OrchestrationV2RuntimeRequest;
  readonly node: OrchestrationV2ExecutionNode;
  readonly item: OrchestrationV2TurnItem;
  readonly approval: boolean;
  readonly approvalKey: string;
}

export function makeOmpUi(options: {
  readonly ids: IdAllocator.IdAllocatorV2["Service"];
  readonly connection: OmpRpcConnection;
  readonly input: ProviderAdapter.ProviderAdapterV2OpenSessionInput;
  readonly emit: OmpEmit;
  readonly thread: () => OrchestrationV2ProviderThread | null;
  readonly active: () => {
    readonly input: ProviderAdapter.ProviderAdapterV2TurnInput;
    readonly providerTurn: { readonly id: OrchestrationV2RuntimeRequest["providerTurnId"] };
    readonly events: { readonly ordinal: (id: string) => number };
  } | null;
  readonly protocolError: (detail: string) => ProviderAdapter.ProviderAdapterProtocolError;
}) {
  const { ids, connection, input, emit } = options;
  const pending = new Map<string, Pending>();
  const sessionApprovals = new Set<string>();
  const resolve = Effect.fnUntraced(function* (
    entry: Pending,
    status: "resolved" | "cancelled" | "expired",
    response?: ProviderAdapter.ProviderAdapterV2RuntimeRequestResponseInput,
  ) {
    const now = yield* DateTime.now;
    pending.delete(entry.request.id);
    yield* emit({
      type: "runtime_request.updated",
      driver: OMP_PROVIDER,
      threadId: entry.item.threadId,
      runtimeRequest: {
        ...entry.request,
        status,
        resolvedAt: now,
        ...(response?.decision === undefined ? {} : { decision: response.decision }),
        ...(response?.answers === undefined ? {} : { answers: response.answers }),
      },
    });
    const itemStatus = status === "resolved" ? "completed" : "cancelled";
    yield* emit({
      type: "node.updated",
      driver: OMP_PROVIDER,
      node: { ...entry.node, status: itemStatus, completedAt: now },
    });
    yield* emit({
      type: "turn_item.updated",
      driver: OMP_PROVIDER,
      turnItem: { ...entry.item, status: itemStatus, completedAt: now, updatedAt: now },
    });
  });
  return {
    get hasPending() {
      return pending.size > 0;
    },
    cancelAll: Effect.fnUntraced(function* () {
      for (const entry of pending.values()) {
        yield* connection
          .send({ type: "extension_ui_response", id: entry.frame.id, cancelled: true })
          .pipe(Effect.ignore);
        yield* resolve(entry, "cancelled");
      }
    }),
    handle: Effect.fnUntraced(function* (frame: OmpRecord) {
      if (frame.type !== "extension_ui_request") return false;
      if (frame.method === "cancel") {
        const targetId = string(frame, "targetId");
        for (const entry of pending.values())
          if (entry.frame.id === targetId) yield* resolve(entry, "expired");
        return true;
      }
      const method = string(frame, "method");
      if (
        method !== "confirm" &&
        method !== "select" &&
        method !== "input" &&
        method !== "editor" &&
        method !== "ask"
      )
        return false;
      const nativeId = string(frame, "id");
      if (nativeId === undefined) return true;
      if (frame.secret === true) {
        yield* connection.send({ type: "extension_ui_response", id: nativeId, cancelled: true });
        return true;
      }
      const turn = options.active();
      const thread = options.thread();
      const nativeOptions = Array.isArray(frame.options) ? frame.options : [];
      const nativeApproval =
        method === "select" &&
        nativeOptions.includes("Deny") &&
        (nativeOptions.includes("Approve") || nativeOptions.includes("Approve Once"));
      const approval = method === "confirm" || nativeApproval;
      const approvalKey = `${method}:${string(frame, "title") ?? ""}:${string(frame, "message") ?? ""}`;
      if (approval && sessionApprovals.has(approvalKey)) {
        yield* connection.send(
          method === "select"
            ? {
                type: "extension_ui_response",
                id: nativeId,
                value: nativeOptions.includes("Approve Once") ? "Approve Once" : "Approve",
              }
            : { type: "extension_ui_response", id: nativeId, confirmed: true },
        );
        return true;
      }
      const createdAt = yield* DateTime.now;
      const requestId = yield* ids.allocate.runtimeRequest({
        driver: OMP_PROVIDER,
        nativeRequestId: nativeId,
        ...(turn?.providerTurn.id == null ? {} : { providerTurnId: turn.providerTurn.id }),
      });
      const nodeId = ids.derive.approvalNode({ requestId });
      const threadId = turn?.input.threadId ?? thread?.appThreadId ?? input.threadId;
      const title = string(frame, "title")?.trim() || "OMP question";
      const requestKind = /\b(edit|write|patch)\b/i.test(title)
        ? ("file-change" as const)
        : ("command" as const);
      const request: OrchestrationV2RuntimeRequest = {
        id: requestId,
        nodeId,
        providerTurnId: turn?.providerTurn.id ?? null,
        nativeRequestRef: ompRef(nativeId),
        kind: approval ? requestKind : "user_input",
        status: "pending",
        responseCapability: { type: "live", providerSessionId: input.providerSessionId },
        createdAt,
        resolvedAt: null,
      };
      const node: OrchestrationV2ExecutionNode = {
        id: nodeId,
        threadId,
        runId: turn?.input.runId ?? null,
        parentNodeId: turn?.input.rootNodeId ?? null,
        rootNodeId: turn?.input.rootNodeId ?? nodeId,
        kind: approval ? "approval_request" : "user_input_request",
        status: "waiting",
        countsForRun: false,
        providerThreadId: thread?.id ?? null,
        providerTurnId: turn?.providerTurn.id ?? null,
        nativeItemRef: ompRef(nativeId),
        runtimeRequestId: requestId,
        checkpointScopeId: null,
        startedAt: createdAt,
        completedAt: null,
      };
      const base = {
        id: ids.derive.approvalTurnItem({ requestId }),
        threadId,
        runId: turn?.input.runId ?? null,
        nodeId,
        providerThreadId: thread?.id ?? null,
        providerTurnId: turn?.providerTurn.id ?? null,
        nativeItemRef: ompRef(nativeId),
        parentItemId: null,
        ordinal: turn?.events.ordinal(nativeId) ?? 0,
        status: "waiting" as const,
        title,
        startedAt: createdAt,
        completedAt: null,
        updatedAt: createdAt,
      };
      const item: OrchestrationV2TurnItem = approval
        ? {
            ...base,
            type: "approval_request",
            requestId,
            requestKind,
            prompt: string(frame, "message") ?? title,
            options: [
              { decision: "accept", label: "Allow once" },
              { decision: "acceptForSession", label: "Allow the same request for this session" },
              { decision: "decline", label: "Deny" },
            ],
          }
        : { ...base, type: "user_input_request", requestId, questions: ompQuestions(frame) };
      pending.set(requestId, { frame, request, node, item, approval, approvalKey });
      yield* emit({
        type: "runtime_request.updated",
        driver: OMP_PROVIDER,
        threadId,
        runtimeRequest: request,
      });
      yield* emit({ type: "node.updated", driver: OMP_PROVIDER, node });
      yield* emit({ type: "turn_item.updated", driver: OMP_PROVIDER, turnItem: item });
      return true;
    }),
    respond: Effect.fn("OmpUi.respond")(function* (
      response: ProviderAdapter.ProviderAdapterV2RuntimeRequestResponseInput,
    ) {
      const entry = pending.get(response.requestId);
      if (entry === undefined)
        return yield* options.protocolError("OMP runtime request is no longer pending");
      let reply: OmpRecord;
      if (entry.approval && entry.frame.method === "select") {
        const choices = Array.isArray(entry.frame.options)
          ? entry.frame.options.filter((v): v is string => typeof v === "string")
          : [];
        const once = choices.includes("Approve Once") ? "Approve Once" : "Approve";
        const session = choices.find((choice) => /^Approve .* for this session$/.test(choice));
        const accepted = response.decision === "accept" || response.decision === "acceptForSession";
        reply = {
          type: "extension_ui_response",
          id: entry.frame.id,
          value: accepted
            ? response.decision === "acceptForSession"
              ? (session ?? once)
              : once
            : "Deny",
        };
      } else reply = ompUiResponse(entry.frame, response.decision, response.answers);
      yield* connection.send(reply);
      if (entry.approval && response.decision === "acceptForSession")
        sessionApprovals.add(entry.approvalKey);
      yield* resolve(entry, "resolved", response);
    }),
  };
}
