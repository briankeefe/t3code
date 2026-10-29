import { assert, describe, it } from "@effect/vitest";
import * as NodeServices from "@effect/platform-node/NodeServices";
import {
  ChatAttachmentId,
  MessageId,
  NodeId,
  ProjectId,
  ProviderInstanceId,
  ProviderSessionId,
  ProviderThreadId,
  RunAttemptId,
  RunId,
  ThreadId,
  type OrchestrationV2AppThread,
} from "@t3tools/contracts";
import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import * as McpProviderSessions from "@t3tools/provider-core/server/McpProviderSessions";
import * as ProviderAdapter from "@t3tools/provider-core/server/ProviderAdapter";
import * as ProviderHost from "@t3tools/provider-core/server/ProviderHost";
import * as TestProviderHost from "@t3tools/provider-testing/TestProviderHost";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Queue from "effect/Queue";
import * as Stream from "effect/Stream";
import * as FetchHttpClient from "effect/http/FetchHttpClient";
import { vi } from "vite-plus/test";
import { makeOmpAdapterV2 } from "./adapter.ts";
import { OMP_PERMISSION_GUARD_COMMAND } from "./mcpInjection.ts";
import type { OmpRpcConnection, OmpRpcError, OmpRpcRecord } from "./rpc.ts";
import type * as OmpRpcModule from "./rpc.ts";

const mocked = vi.hoisted(() => ({ connect: vi.fn() }));
vi.mock("./rpc.ts", async (importOriginal) => ({
  ...(await importOriginal<typeof OmpRpcModule>()),
  makeOmpRpcConnection: mocked.connect,
}));

const layer = Layer.mergeAll(
  NodeServices.layer,
  FetchHttpClient.layer,
  IdAllocator.layer,
  McpProviderSessions.layer,
  TestProviderHost.layer().pipe(Layer.provide(NodeServices.layer)),
);
const threadId = ThreadId.make("omp-regression-thread");
const selection = { instanceId: ProviderInstanceId.make("omp"), model: "default" };
const policy = ProviderAdapter.ProviderAdapterV2RuntimePolicy.make({
  runtimeMode: "full-access",
  interactionMode: "default",
  cwd: null,
});

const setup = Effect.fnUntraced(function* () {
  const nativeEvents = yield* Queue.unbounded<OmpRpcRecord, OmpRpcError>();
  const emitted = yield* Queue.unbounded<ProviderAdapter.ProviderAdapterV2Event>();
  const frames: OmpRpcRecord[] = [];
  const attachmentStarted = yield* Deferred.make<void>();
  const attachmentRelease = yield* Deferred.make<void>();
  const abortStarted = yield* Deferred.make<void>();
  const abortRelease = yield* Deferred.make<void>();
  let nativeFile = "/fake/omp-session.jsonl";
  const connection: OmpRpcConnection = {
    events: nativeEvents,
    exited: Effect.never,
    terminate: Effect.void,
    send: (record) =>
      Effect.sync(() => {
        frames.push(record);
      }),
    request: (record) => {
      frames.push(record);
      switch (record.type) {
        case "get_state":
          return Effect.succeed({
            sessionFile: nativeFile,
            isSettled: true,
            isStreaming: false,
            hasPendingAsyncWork: false,
            model: { provider: "test", id: "model", contextWindow: 8192 },
            thinkingLevel: "off",
          });
        case "get_available_commands":
          return Effect.succeed({ commands: [{ name: OMP_PERMISSION_GUARD_COMMAND }] });
        case "get_available_models":
          return Effect.succeed({ models: [] });
        case "get_entries":
          return Effect.succeed({ entries: [], leafId: null });
        case "switch_session":
          nativeFile = String(record.sessionPath);
          return Effect.succeed({});
        case "abort":
          return Deferred.succeed(abortStarted, undefined).pipe(
            Effect.andThen(Deferred.await(abortRelease)),
            Effect.as({}),
          );
        default:
          return Effect.succeed({});
      }
    },
  };
  mocked.connect.mockReturnValue(Effect.succeed(connection));
  const fs = yield* FileSystem.FileSystem;
  const host = yield* ProviderHost.ProviderHost;
  const imagePath = host.resolveAttachmentPath({
    id: ChatAttachmentId.make("image"),
    type: "image",
    name: "image.png",
    mimeType: "image/png",
    sizeBytes: 3,
  })!;
  const adapter = yield* makeOmpAdapterV2({
    instanceId: selection.instanceId,
    settings: { enabled: true, binaryPath: "omp", launchArgs: "", customModels: [] },
    environment: {},
  }).pipe(
    Effect.provideService(FileSystem.FileSystem, {
      ...fs,
      readFile: (path) =>
        path === imagePath
          ? Deferred.succeed(attachmentStarted, undefined).pipe(
              Effect.andThen(Deferred.await(attachmentRelease)),
              Effect.as(new Uint8Array([1, 2, 3])),
            )
          : fs.readFile(path),
    }),
  );
  const runtime = yield* adapter.openSession({
    threadId,
    providerSessionId: ProviderSessionId.make("omp-regression-session"),
    modelSelection: selection,
    runtimePolicy: policy,
  });
  yield* runtime.events.pipe(
    Stream.runForEach((event) => Queue.offer(emitted, event)),
    Effect.forkScoped,
  );
  const providerThread = yield* runtime.ensureThread({
    threadId,
    modelSelection: selection,
    runtimePolicy: policy,
  });
  const now = yield* DateTime.now;
  const appThread: OrchestrationV2AppThread = {
    id: threadId,
    projectId: ProjectId.make("omp-regression-project"),
    title: "OMP regression",
    createdBy: "user",
    creationSource: "web",
    providerInstanceId: selection.instanceId,
    modelSelection: selection,
    runtimeMode: "full-access",
    interactionMode: "default",
    branch: null,
    worktreePath: null,
    activeProviderThreadId: null,
    lineage: { parentThreadId: null, relationshipToParent: null, rootThreadId: threadId },
    forkedFrom: null,
    createdAt: now,
    updatedAt: now,
    archivedAt: null,
    settledOverride: null,
    settledAt: null,
    lastVisitedAt: null,
    deletedAt: null,
  };
  const start = Effect.fnUntraced(function* (ordinal = 1, target = providerThread) {
    const runId = RunId.make(`omp-regression-run-${ordinal}`);
    const input: ProviderAdapter.ProviderAdapterV2TurnInput = {
      appThread: { ...appThread, id: target.appThreadId ?? threadId },
      threadId: target.appThreadId ?? threadId,
      runId,
      runOrdinal: ordinal,
      providerTurnOrdinal: ordinal,
      attemptId: RunAttemptId.make(`omp-attempt-${ordinal}`),
      rootNodeId: NodeId.make(`omp-root-${ordinal}`),
      providerThread: target,
      message: {
        messageId: MessageId.make(`omp-message-${ordinal}`),
        text: "Hello",
        attachments: [],
        createdBy: "user",
        creationSource: "web",
      },
      modelSelection: selection,
      runtimePolicy: policy,
    };
    yield* runtime.startTurn(input);
    return input;
  });
  const take = Effect.fnUntraced(function* (type: "turn.terminal" | "provider_turn.updated") {
    while (true) {
      const event = yield* Queue.take(emitted);
      if (event.type === type) return event;
    }
  });
  const emit = (frame: OmpRpcRecord) => Queue.offer(nativeEvents, frame).pipe(Effect.asVoid);
  return {
    runtime,
    connection,
    providerThread,
    frames,
    start,
    take,
    emit,
    attachmentStarted,
    attachmentRelease,
    abortStarted,
    abortRelease,
  };
});

describe("OMP adapter outcome and ownership", () => {
  it.effect.each([{ stopReason: "error" }, { stopReason: "aborted" }])(
    "recovers a nonterminal $stopReason attempt before successful settlement",
    ({ stopReason }) =>
      Effect.scoped(
        Effect.gen(function* () {
          const test = yield* setup();
          yield* test.start();
          yield* test.emit({ type: "agent_start" });
          yield* test.emit({
            type: "agent_end",
            isTerminal: false,
            yielded: false,
            messages: [
              { role: "assistant", stopReason, errorMessage: "Transient provider failure" },
            ],
          });
          yield* test.emit({ type: "agent_start" });
          yield* test.emit({
            type: "agent_end",
            isTerminal: true,
            yielded: true,
            messages: [{ role: "assistant", stopReason: "stop" }],
          });
          yield* test.emit({
            type: "prompt_result",
            id: "t3-prompt-1",
            status: "completed",
            sessionSettled: true,
          });
          const terminal = yield* test.take("turn.terminal");
          assert.equal(
            terminal.type === "turn.terminal" ? terminal.status : undefined,
            "completed",
          );
        }),
      ).pipe(Effect.provide(layer)),
  );

  it.effect("preserves an explicit user Stop when a successful native result races abort", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const test = yield* setup();
        yield* test.start();
        const started = yield* test.take("provider_turn.updated");
        assert.equal(started.type, "provider_turn.updated");
        if (started.type !== "provider_turn.updated") return;
        const stopping = yield* test.runtime
          .interruptTurn({
            providerThread: test.providerThread,
            providerTurnId: started.providerTurn.id,
          })
          .pipe(Effect.forkScoped);
        yield* Deferred.await(test.abortStarted);
        yield* test.emit({
          type: "prompt_result",
          id: "t3-prompt-1",
          status: "completed",
          sessionSettled: true,
        });
        const terminal = yield* test.take("turn.terminal");
        assert.equal(
          terminal.type === "turn.terminal" ? terminal.status : undefined,
          "interrupted",
        );
        yield* Deferred.succeed(test.abortRelease, undefined);
        yield* Fiber.join(stopping);
      }),
    ).pipe(Effect.provide(layer)),
  );

  it.effect("rejects a steer prepared after its owner completed and another turn started", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const test = yield* setup();
        const input = yield* test.start();
        const started = yield* test.take("provider_turn.updated");
        assert.equal(started.type, "provider_turn.updated");
        if (started.type !== "provider_turn.updated") return;
        const steering = yield* test.runtime
          .steerTurn({
            threadId,
            runId: input.runId,
            providerThread: test.providerThread,
            providerTurnId: started.providerTurn.id,
            message: {
              ...input.message,
              text: "Inspect this image",
              attachments: [
                {
                  id: ChatAttachmentId.make("image"),
                  type: "image",
                  name: "image.png",
                  mimeType: "image/png",
                  sizeBytes: 3,
                },
              ],
            },
          })
          .pipe(Effect.exit, Effect.forkScoped);
        yield* Deferred.await(test.attachmentStarted);
        yield* test.emit({
          type: "prompt_result",
          id: "t3-prompt-1",
          status: "completed",
          sessionSettled: true,
        });
        yield* test.take("turn.terminal");
        yield* test.start(2);
        yield* Deferred.succeed(test.attachmentRelease, undefined);
        const result = yield* Fiber.join(steering);
        assert.isTrue(Exit.isFailure(result));
        assert.equal(test.frames.filter((frame) => frame.type === "steer").length, 0);
      }),
    ).pipe(Effect.provide(layer)),
  );
  it.effect(
    "hosts a fork and adopts the orchestrator binding without accepting another native session",
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const test = yield* setup();
          mocked.connect.mockReturnValueOnce(
            Effect.succeed({
              ...test.connection,
              events: yield* Queue.unbounded<OmpRpcRecord, OmpRpcError>(),
              request: (record: OmpRpcRecord) =>
                Effect.succeed(
                  record.type === "get_state" ? { sessionFile: "/fake/omp-fork.jsonl" } : {},
                ),
            }),
          );
          const forked = yield* test.runtime.forkThread({
            sourceProviderThread: test.providerThread,
            targetThreadId: ThreadId.make("omp-forked-thread"),
            modelSelection: selection,
            runtimePolicy: policy,
          });
          assert.notEqual(
            forked.nativeThreadRef?.nativeId,
            test.providerThread.nativeThreadRef?.nativeId,
          );
          const adopted = { ...forked, id: ProviderThreadId.make("omp-canonical-fork-binding") };
          const wrongNative = yield* test
            .start(1, {
              ...adopted,
              nativeThreadRef: {
                driver: forked.driver,
                nativeId: "/fake/other-session.jsonl",
                strength: "strong",
              },
            })
            .pipe(Effect.exit);
          assert.isTrue(Exit.isFailure(wrongNative));
          yield* test.start(1, adopted);
          yield* test.emit({
            type: "prompt_result",
            id: "t3-prompt-1",
            status: "completed",
            sessionSettled: true,
          });
          const terminal = yield* test.take("turn.terminal");
          assert.equal(
            terminal.type === "turn.terminal" ? terminal.status : undefined,
            "completed",
          );
          assert.equal(test.providerThread.nativeThreadRef?.nativeId, "/fake/omp-session.jsonl");
        }),
      ).pipe(Effect.provide(layer)),
  );
});
