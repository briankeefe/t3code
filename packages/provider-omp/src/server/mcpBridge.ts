import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Schema from "effect/Schema";
import * as Scope from "effect/Scope";
import * as HttpClient from "effect/http/HttpClient";
import * as HttpClientRequest from "effect/http/HttpClientRequest";
import * as HttpClientResponse from "effect/http/HttpClientResponse";
import { collectUint8StreamText } from "@t3tools/provider-core/server/collectStreamText";
import type { McpProviderSessionConfig } from "@t3tools/provider-core/server/mcpSession";
import {
  OmpRpcError,
  ompRecordField,
  ompRecordString,
  type OmpRpcConnection,
  type OmpRpcRecord,
} from "./rpc.ts";
import type { OmpRuntimeMode } from "./mcpInjection.ts";

const McpTool = Schema.Struct({
  name: Schema.String,
  description: Schema.optional(Schema.String),
  inputSchema: Schema.optional(Schema.Record(Schema.String, Schema.Unknown)),
});
const decodeTools = Schema.decodeUnknownSync(
  Schema.Struct({
    tools: Schema.Array(McpTool),
    nextCursor: Schema.optional(Schema.String),
  }),
);
const decodeResult = Schema.decodeUnknownSync(
  Schema.Struct({
    content: Schema.Array(Schema.Unknown),
    structuredContent: Schema.optional(Schema.Unknown),
    isError: Schema.optional(Schema.Boolean),
  }),
);
const Envelope = Schema.Struct({
  id: Schema.optional(Schema.Union([Schema.String, Schema.Number])),
  result: Schema.optional(Schema.Unknown),
  error: Schema.optional(Schema.Unknown),
});
const decodeEnvelopeText = Schema.decodeSync(Schema.fromJsonString(Envelope));
const encodeJson = Schema.encodeSync(Schema.fromJsonString(Schema.Unknown));
const PROTOCOL = "2025-06-18";
const MAX_RESPONSE_BYTES = 64 * 1024 * 1024;
export interface OmpMcpBridge {
  readonly handle: (record: OmpRpcRecord) => Effect.Effect<boolean, OmpRpcError>;
}

export const makeOmpMcpBridge = Effect.fn("OmpMcp.connect")(function* (input: {
  readonly connection: OmpRpcConnection;
  readonly mcpSession: McpProviderSessionConfig;
  readonly runtimeMode?: OmpRuntimeMode;
}) {
  const scope = yield* Effect.scope;
  const httpClient = yield* HttpClient.HttpClient;
  let nextId = 0;
  let httpSession: string | undefined;
  const pending = new Map<string, Fiber.Fiber<void, never>>();
  const names = new Set<string>();

  const request = Effect.fn("OmpMcp.request")(function* (
    method: string,
    params: unknown,
    notification = false,
  ) {
    const id = ++nextId;
    const outgoing = yield* Effect.try({
      try: () =>
        HttpClientRequest.post(input.mcpSession.endpoint).pipe(
          HttpClientRequest.setHeaders({
            accept: "application/json, text/event-stream",
            authorization: input.mcpSession.authorizationHeader,
            "mcp-protocol-version": PROTOCOL,
            ...(httpSession ? { "mcp-session-id": httpSession } : {}),
          }),
          HttpClientRequest.bodyText(
            encodeJson({ jsonrpc: "2.0", ...(notification ? {} : { id }), method, params }),
            "application/json",
          ),
        ),
      catch: (cause) =>
        new OmpRpcError({ operation: "mcp", detail: `MCP ${method} encoding failed`, cause }),
    });
    const response = yield* httpClient.execute(outgoing).pipe(
      Effect.flatMap(HttpClientResponse.filterStatusOk),
      Effect.mapError(
        (cause) =>
          new OmpRpcError({ operation: "mcp", detail: `MCP ${method} HTTP request failed`, cause }),
      ),
    );
    const session = response.headers["mcp-session-id"];
    if (session) httpSession = session;
    const body = yield* collectUint8StreamText({
      stream: response.stream,
      maxBytes: MAX_RESPONSE_BYTES,
    }).pipe(
      Effect.mapError(
        (cause) =>
          new OmpRpcError({ operation: "mcp", detail: `MCP ${method} response failed`, cause }),
      ),
    );
    if (body.truncated || body.invalidUtf8)
      return yield* new OmpRpcError({
        operation: "mcp",
        detail: "Invalid or oversized MCP response",
      });
    if (notification || response.status === 202 || response.status === 204) return undefined;
    const envelope = yield* Effect.try({
      try: () => {
        const envelopes = (response.headers["content-type"] ?? "").includes("text/event-stream")
          ? body.text.split(/\r?\n\r?\n/).flatMap((event) => {
              const data = event
                .split(/\r?\n/)
                .filter((line) => line.startsWith("data:"))
                .map((line) => line.slice(5).trimStart())
                .join("\n");
              return data ? [decodeEnvelopeText(data)] : [];
            })
          : [decodeEnvelopeText(body.text)];
        return envelopes.find((value) => value.id === id);
      },
      catch: (cause) =>
        new OmpRpcError({
          operation: "mcp",
          detail: `MCP ${method} response decoding failed`,
          cause,
        }),
    });
    if (!envelope)
      return yield* new OmpRpcError({
        operation: "mcp",
        detail: "MCP response did not match request",
      });
    if (envelope.error !== undefined)
      return yield* new OmpRpcError({
        operation: "mcp",
        detail: `MCP ${method} rejected request`,
        cause: envelope.error,
      });
    return envelope.result;
  });

  yield* request("initialize", {
    protocolVersion: PROTOCOL,
    capabilities: {},
    clientInfo: { name: "t3-omp", version: "1.0.0" },
  });
  yield* request("notifications/initialized", {}, true);
  const tools: Array<typeof McpTool.Type> = [];
  const cursors = new Set<string>();
  let cursor: string | undefined;
  do {
    const page = yield* request("tools/list", cursor ? { cursor } : {}).pipe(
      Effect.flatMap((result) =>
        Effect.try({
          try: () => decodeTools(result),
          catch: (cause) =>
            new OmpRpcError({ operation: "mcp", detail: "Invalid MCP tool catalog", cause }),
        }),
      ),
    );
    for (const tool of page.tools) {
      if (!input.mcpSession.browserToolsAvailable && tool.name.startsWith("preview_")) continue;
      if (names.has(tool.name))
        return yield* new OmpRpcError({ operation: "mcp", detail: "Duplicate MCP tool name" });
      names.add(tool.name);
      tools.push(tool);
    }
    cursor = page.nextCursor;
    if (cursor && cursors.has(cursor))
      return yield* new OmpRpcError({
        operation: "mcp",
        detail: "MCP tool catalog repeated its cursor",
      });
    if (cursor) cursors.add(cursor);
  } while (cursor);
  yield* input.connection
    .request({
      type: "set_host_tools",
      tools: tools.map((tool) => ({
        name: tool.name,
        description: tool.description ?? tool.name,
        parameters: {
          ...tool.inputSchema,
          type: tool.inputSchema?.type ?? "object",
          properties: tool.inputSchema?.properties ?? {},
        },
        loadMode: "essential",
      })),
    })
    .pipe(Effect.mapError((cause) => new OmpRpcError({ operation: "set_host_tools", cause })));
  yield* Scope.addFinalizer(
    scope,
    Effect.sync(() => pending.clear()),
  );

  const handle = Effect.fn("OmpMcp.handle")(function* (
    record: OmpRpcRecord,
  ): Effect.fn.Return<boolean, OmpRpcError> {
    const id = ompRecordString(record, "id");
    if (!id) return false;
    if (record.type === "host_uri_request") {
      yield* input.connection.send({
        type: "host_uri_result",
        id,
        isError: true,
        error: "T3 has not registered this host URI scheme.",
      });
      return true;
    }
    if (record.type === "host_uri_cancel") return true;
    if (record.type === "host_tool_cancel") {
      const target = ompRecordString(record, "targetId");
      const fiber = target ? pending.get(target) : undefined;
      if (fiber) yield* Fiber.interrupt(fiber);
      return true;
    }
    if (record.type !== "host_tool_call") return false;
    const name = ompRecordString(record, "toolName");
    if (!name || !names.has(name) || pending.has(id)) {
      yield* input.connection.send({
        type: "host_tool_result",
        id,
        isError: true,
        result: { content: [{ type: "text", text: "Unknown or duplicate T3 host tool request." }] },
      });
      return true;
    }
    const run = Effect.gen(function* () {
      const result = yield* request("tools/call", {
        name,
        arguments: ompRecordField(record, "arguments") ?? {},
      });
      const parsed = yield* Effect.try({
        try: () => decodeResult(result),
        catch: (cause) =>
          new OmpRpcError({ operation: "mcp", detail: "Invalid MCP tool result", cause }),
      });
      const content = parsed.content.flatMap<
        { type: "text"; text: string } | { type: "image"; data: string; mimeType: string }
      >((part) => {
        const type = ompRecordString(part, "type");
        const text = ompRecordString(part, "text");
        if (type === "text" && text !== undefined) return [{ type: "text", text }];
        const data = ompRecordString(part, "data");
        const mimeType = ompRecordString(part, "mimeType");
        return type === "image" && data && mimeType ? [{ type: "image", data, mimeType }] : [];
      });
      if (content.length === 0)
        content.push({ type: "text", text: encodeJson(parsed.structuredContent ?? result) });
      yield* input.connection.send({
        type: "host_tool_result",
        id,
        isError: parsed.isError === true,
        result: { content, details: parsed.structuredContent ?? {} },
      });
    }).pipe(
      Effect.catch((cause) =>
        input.connection
          .send({
            type: "host_tool_result",
            id,
            isError: true,
            result: { content: [{ type: "text", text: cause.message }] },
          })
          .pipe(Effect.ignore),
      ),
      Effect.ensuring(Effect.sync(() => pending.delete(id))),
    );
    pending.set(id, yield* run.pipe(Effect.forkIn(scope)));
    return true;
  });
  return { handle } satisfies OmpMcpBridge;
});
