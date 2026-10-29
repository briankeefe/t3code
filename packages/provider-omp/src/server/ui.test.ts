import { assert, describe, it } from "@effect/vitest";
import { ProviderInstanceId, ProviderSessionId, ThreadId } from "@t3tools/contracts";
import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import {
  ProviderAdapterProtocolError,
  type ProviderAdapterV2Event,
} from "@t3tools/provider-core/server/ProviderAdapter";
import * as Effect from "effect/Effect";
import * as Queue from "effect/Queue";
import { OMP_PROVIDER } from "./events.ts";
import type { OmpRecord } from "./frames.ts";
import type { OmpRpcConnection, OmpRpcError } from "./rpc.ts";
import { makeOmpUi } from "./ui.ts";

const setup = Effect.gen(function* () {
  const emitted: ProviderAdapterV2Event[] = [];
  const sent: OmpRecord[] = [];
  const connection: OmpRpcConnection = {
    events: yield* Queue.unbounded<OmpRecord, OmpRpcError>(),
    exited: Effect.never,
    terminate: Effect.void,
    send: (record) =>
      Effect.sync(() => {
        sent.push(record);
      }),
    request: () => Effect.succeed({}),
  };
  const ui = makeOmpUi({
    ids: yield* IdAllocator.IdAllocatorV2,
    connection,
    input: {
      threadId: ThreadId.make("omp-thread"),
      providerSessionId: ProviderSessionId.make("omp-session"),
      modelSelection: { instanceId: ProviderInstanceId.make("omp"), model: "default" },
      runtimePolicy: { runtimeMode: "approval-required", interactionMode: "default", cwd: null },
    },
    thread: () => null,
    active: () => null,
    emit: (event) =>
      Effect.sync(() => {
        emitted.push(event);
      }),
    protocolError: (detail) => new ProviderAdapterProtocolError({ driver: OMP_PROVIDER, detail }),
  });
  return { ui, emitted, sent };
});

describe("OMP native UI", () => {
  it.effect(
    "maps native approval choices to live requests without persisting Always Approve grants",
    () =>
      Effect.gen(function* () {
        const { ui, emitted, sent } = yield* setup;
        yield* ui.handle({
          type: "extension_ui_request",
          id: "approval",
          method: "select",
          title: "Allow tool: bash",
          options: [
            "Approve Directory: /tmp",
            "Always Approve Directory: /tmp",
            "Approve Once",
            "Approve bash for this session",
            "Deny",
          ],
        });
        const request = emitted.find((event) => event.type === "runtime_request.updated");
        assert.isDefined(request);
        if (request?.type !== "runtime_request.updated") return;
        assert.equal(request.runtimeRequest.kind, "command");
        assert.isTrue(ui.hasPending);
        yield* ui.respond({ requestId: request.runtimeRequest.id, decision: "acceptAlways" });
        assert.deepEqual(sent, [{ type: "extension_ui_response", id: "approval", value: "Deny" }]);
        assert.isFalse(ui.hasPending);
        assert.isTrue(
          emitted.some(
            (event) =>
              event.type === "runtime_request.updated" &&
              event.runtimeRequest.status === "resolved",
          ),
        );
      }).pipe(Effect.provide(IdAllocator.layer)),
  );

  it.effect("cancels pending guard dialogs before stop and expires native cancelled requests", () =>
    Effect.gen(function* () {
      const { ui, emitted, sent } = yield* setup;
      yield* ui.handle({
        type: "extension_ui_request",
        id: "guard",
        method: "confirm",
        title: "T3 approval: write",
        message: "fixture.txt",
      });
      yield* ui.handle({
        type: "extension_ui_request",
        id: "ask",
        method: "input",
        title: "Branch name",
      });
      yield* ui.handle({ type: "extension_ui_request", method: "cancel", targetId: "ask" });
      yield* ui.cancelAll();
      assert.deepEqual(sent, [{ type: "extension_ui_response", id: "guard", cancelled: true }]);
      assert.isFalse(ui.hasPending);
      const resolved = emitted.flatMap((event) =>
        event.type === "runtime_request.updated" && event.runtimeRequest.status !== "pending"
          ? [event.runtimeRequest]
          : [],
      );
      assert.equal(resolved.length, 2);
      assert.equal(resolved[0]?.status, "expired");
      assert.equal(resolved[1]?.status, "cancelled");
    }).pipe(Effect.provide(IdAllocator.layer)),
  );
});
