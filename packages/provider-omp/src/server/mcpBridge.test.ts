import { assert, it } from "@effect/vitest";
// @effect-diagnostics-next-line nodeBuiltinImport:off -- This fixture observes native socket closure to prove HTTP cancellation.
import * as NodeHttp from "node:http";
import { EnvironmentId, ProviderInstanceId, ThreadId } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Queue from "effect/Queue";
import * as FetchHttpClient from "effect/http/FetchHttpClient";
import { makeOmpMcpBridge } from "./mcpBridge.ts";
import type { OmpRpcConnection, OmpRpcRecord, OmpRpcError } from "./rpc.ts";

it.live("bridges real MCP errors and cancellation, and fails unsupported host URIs promptly", () =>
  Effect.gen(function* () {
    const frames = yield* Queue.unbounded<OmpRpcRecord>();
    const { promise: called, resolve: onCall } = Promise.withResolvers<void>();
    const { promise: cancelled, resolve: onCancel } = Promise.withResolvers<void>();
    const server = NodeHttp.createServer(async (req, res) => {
      let body = "";
      for await (const chunk of req) body += chunk.toString();
      const message = JSON.parse(body) as {
        id?: number;
        method: string;
        params?: { name?: string };
      };
      res.setHeader("content-type", "application/json");
      if (message.method === "notifications/initialized") {
        res.writeHead(202).end();
        return;
      }
      if (message.method === "tools/call" && message.params?.name === "wait") {
        res.on("close", onCancel);
        onCall();
        return;
      }
      const result =
        message.method === "tools/list"
          ? {
              tools: [
                { name: "fails", inputSchema: { type: "object" } },
                { name: "wait", inputSchema: { type: "object" } },
                { name: "preview_open", inputSchema: { type: "object" } },
              ],
            }
          : {};
      res.end(
        JSON.stringify({
          jsonrpc: "2.0",
          id: message.id,
          ...(message.method === "tools/call"
            ? { error: { code: -32000, message: "Denied upstream" } }
            : { result }),
        }),
      );
    });
    yield* Effect.acquireRelease(
      Effect.promise(() => {
        const { promise, resolve, reject } = Promise.withResolvers<void>();
        server.once("error", reject);
        server.listen(0, "127.0.0.1", resolve);
        return promise;
      }),
      () =>
        Effect.promise(() => {
          const { promise, resolve } = Promise.withResolvers<void>();
          server.closeAllConnections();
          server.close(() => resolve());
          return promise;
        }),
    );
    const address = server.address();
    if (address === null || typeof address === "string")
      return yield* Effect.die("Missing HTTP fixture address");
    const events = yield* Queue.unbounded<OmpRpcRecord, OmpRpcError>();
    let registered: unknown;
    const connection: OmpRpcConnection = {
      send: (record) => Queue.offer(frames, record).pipe(Effect.asVoid),
      request: (record) =>
        Effect.sync(() => {
          registered = record.tools;
          return {};
        }),
      events,
      exited: Effect.never,
      terminate: Effect.void,
    };
    const bridge = yield* makeOmpMcpBridge({
      connection,
      mcpSession: {
        endpoint: `http://127.0.0.1:${address.port}/mcp`,
        authorizationHeader: "Bearer test-only",
        environmentId: EnvironmentId.make("test"),
        threadId: ThreadId.make("test"),
        providerSessionId: "test",
        providerInstanceId: ProviderInstanceId.make("omp"),
        browserToolsAvailable: false,
      },
    });
    assert.notInclude(JSON.stringify(registered), "preview_open");
    yield* bridge.handle({
      type: "host_tool_call",
      id: "failed",
      toolName: "fails",
      arguments: {},
    });
    const failure = yield* Queue.take(frames);
    assert.equal(failure.type, "host_tool_result");
    assert.equal(failure.id, "failed");
    assert.isTrue(failure.isError);
    yield* bridge.handle({
      type: "host_uri_request",
      id: "uri",
      operation: "read",
      url: "unknown://path",
    });
    assert.deepInclude(yield* Queue.take(frames), {
      type: "host_uri_result",
      id: "uri",
      isError: true,
    });
    yield* bridge.handle({
      type: "host_tool_call",
      id: "waiting",
      toolName: "wait",
      arguments: {},
    });
    yield* Effect.promise(() => called);
    yield* bridge.handle({ type: "host_tool_cancel", id: "cancel", targetId: "waiting" });
    yield* Effect.promise(() => cancelled);
  }).pipe(Effect.provide(FetchHttpClient.layer), Effect.scoped),
);
