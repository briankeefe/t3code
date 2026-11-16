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
  type BackgroundQuestionSnapshot,
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
  const replies = new Map<string, unknown>();
  const attachmentStarted = yield* Deferred.make<void>();
  const attachmentRelease = yield* Deferred.make<void>();
  const abortStarted = yield* Deferred.make<void>();
  const abortRelease = yield* Deferred.make<void>();
  const btwStarted = yield* Deferred.make<void>();
  const btwRelease = yield* Deferred.make<void>();
  let pauseBtw = false;
  let nativeFile = "/fake/omp-session.jsonl";
  let nativeThinking = "off";
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
      if (record.type === "btw" && pauseBtw)
        return Deferred.succeed(btwStarted, undefined).pipe(
          Effect.andThen(Deferred.await(btwRelease)),
          Effect.as(replies.get("btw")),
        );
      if (replies.has(String(record.type))) return Effect.succeed(replies.get(String(record.type)));
      switch (record.type) {
        case "get_state":
          return Effect.succeed({
            sessionFile: nativeFile,
            isSettled: true,
            isStreaming: false,
            hasPendingAsyncWork: false,
            model: { provider: "test", id: "model", contextWindow: 8192 },
            thinkingLevel: nativeThinking,
          });
        case "get_available_commands":
          return Effect.succeed({ commands: [{ name: OMP_PERMISSION_GUARD_COMMAND }] });
        case "get_available_models":
          return Effect.succeed({ models: [] });
        case "get_entries":
          return Effect.succeed({ entries: [], leafId: null });
        case "get_btw_history":
          return Effect.succeed({ records: [] });
        case "switch_session":
          nativeFile = String(record.sessionPath);
          return Effect.succeed({});
        case "set_thinking_level":
          nativeThinking = String(record.level);
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
  const start = Effect.fnUntraced(function* (
    ordinal = 1,
    target = providerThread,
    thinking?: string,
  ) {
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
      modelSelection:
        thinking === undefined
          ? selection
          : { ...selection, options: [{ id: "thinking", value: thinking }] },
      runtimePolicy: policy,
    };
    yield* runtime.startTurn(input);
    return input;
  });
  const take = Effect.fnUntraced(function* (
    type: "turn.terminal" | "provider_turn.updated" | "provider_thread.updated",
  ) {
    while (true) {
      const event = yield* Queue.take(emitted);
      if (event.type === type) return event;
    }
  });
  const emit = (frame: OmpRpcRecord) => Queue.offer(nativeEvents, frame).pipe(Effect.asVoid);
  return {
    runtime,
    connection,
    replies,
    providerThread,
    frames,
    start,
    take,
    emit,
    attachmentStarted,
    attachmentRelease,
    abortStarted,
    abortRelease,
    btwStarted,
    btwRelease,
    pauseBtw: () => {
      pauseBtw = true;
    },
    setNativeThinking: (level: string) => {
      nativeThinking = level;
    },
  };
});

it.effect("keeps BTW streaming and cancellation separate from an active main turn", () =>
  Effect.gen(function* () {
    const { runtime, providerThread, start, emit, take, frames, replies } = yield* setup();
    assert.isDefined(runtime.backgroundQuestions);
    const side = runtime.backgroundQuestions!;
    yield* start();
    yield* emit({ type: "agent_start" });
    const record = {
      id: "side-topic",
      leafId: null,
      question: "Side question",
      answer: "",
      status: "running",
      createdAt: 1,
      updatedAt: 1,
    };
    replies.set("btw", { record });
    replies.set("btw_cancel", { cancelled: true });
    const admitted = yield* side.ask({ providerThread, kind: "btw", question: record.question });
    assert.equal(admitted.id, record.id);
    assert.isTrue(
      Exit.isFailure(yield* side.ask({ providerThread, kind: "pulse" }).pipe(Effect.exit)),
    );
    const updates = yield* Queue.unbounded<unknown>();
    const streamFailed = yield* Deferred.make<void>();
    yield* side.subscribe({ providerThread }).pipe(
      Stream.runForEach((snapshot) => Queue.offer(updates, snapshot)),
      Effect.catch(() => Deferred.succeed(streamFailed, undefined)),
      Effect.forkScoped,
    );
    yield* emit({ type: "btw_record", record });
    yield* emit({ type: "btw_delta", recordId: record.id, delta: "Side answer" });
    let snapshot = yield* Queue.take(updates);
    while (!JSON.stringify(snapshot).includes("Side answer")) snapshot = yield* Queue.take(updates);
    assert.equal(runtime.providerSession.status, "running");
    const switched = yield* runtime
      .resumeThread({
        providerThread: {
          ...providerThread,
          nativeThreadRef: { ...providerThread.nativeThreadRef!, nativeId: "/other.jsonl" },
        },
      })
      .pipe(Effect.exit);
    assert.isTrue(Exit.isFailure(switched));
    assert.isTrue((yield* side.cancel({ providerThread, recordId: record.id })).cancelled);
    assert.isFalse(frames.some((frame) => frame.type === "abort"));
    yield* emit({
      type: "btw_record",
      record: { ...record, answer: "Side answer", status: "cancelled" },
    });
    while (!JSON.stringify(yield* Queue.take(updates)).includes("cancelled")) {}
    yield* emit({
      type: "notice",
      source: "btw-history",
      level: "error",
      message: "Side history write failed",
    });
    yield* Deferred.await(streamFailed);
    assert.equal(runtime.providerSession.status, "running");
    yield* emit({
      type: "prompt_result",
      id: "t3-prompt-1",
      status: "completed",
      sessionSettled: true,
    });
    const terminal = yield* take("turn.terminal");
    assert.equal(terminal.type, "turn.terminal");
    if (terminal.type === "turn.terminal") assert.equal(terminal.status, "completed");
  }).pipe(Effect.scoped, Effect.provide(layer)),
);

it.effect("cancels a side question still starting without blocking main completion", () =>
  Effect.gen(function* () {
    const test = yield* setup();
    yield* test.start();
    test.pauseBtw();
    test.replies.set("btw", {
      record: {
        id: "starting-side",
        question: "Starting question",
        answer: "",
        status: "running",
        createdAt: 1,
        updatedAt: 1,
      },
    });
    test.replies.set("btw_cancel", { cancelled: true });
    const ask = yield* test.runtime
      .backgroundQuestions!.ask({
        providerThread: test.providerThread,
        kind: "btw",
        question: "Starting question",
      })
      .pipe(Effect.forkScoped);
    yield* Deferred.await(test.btwStarted);
    assert.isTrue(
      (yield* test.runtime.backgroundQuestions!.cancel({ providerThread: test.providerThread }))
        .cancelled,
    );
    yield* test.emit({
      type: "prompt_result",
      id: "t3-prompt-1",
      status: "completed",
      sessionSettled: true,
    });
    yield* test.take("turn.terminal");
    assert.equal(test.runtime.providerSession.status, "ready");
    yield* Deferred.succeed(test.btwRelease, undefined);
    yield* Fiber.join(ask);
    assert.isTrue(
      test.frames.some(
        (frame) => frame.type === "btw_cancel" && frame.recordId === "starting-side",
      ),
    );
    assert.isFalse(test.frames.some((frame) => frame.type === "abort"));
  }).pipe(Effect.scoped, Effect.provide(layer)),
);

it.effect(
  "pins side questions without changing main status and rejects unsafe session or model changes",
  () =>
    Effect.gen(function* () {
      const { runtime, providerThread, replies, start, emit, take, frames } = yield* setup();
      yield* start();
      yield* emit({
        type: "prompt_result",
        id: "t3-prompt-1",
        status: "completed",
        sessionSettled: true,
      });
      yield* take("turn.terminal");
      const record = {
        id: "idle-side",
        leafId: null,
        question: "Read only",
        answer: "",
        status: "running",
        createdAt: 1,
        updatedAt: 1,
      };
      replies.set("btw", { record });
      yield* runtime.backgroundQuestions!.ask({
        providerThread,
        kind: "btw",
        question: record.question,
      });
      assert.equal(runtime.providerSession.status, "ready");
      assert.isTrue(yield* runtime.hasPendingBackgroundWork!);
      assert.isFalse(yield* runtime.hasPendingBackgroundWorkForThread!(providerThread));
      assert.isTrue(
        Exit.isFailure(yield* runtime.resumeThread({ providerThread }).pipe(Effect.exit)),
      );
      assert.isTrue(Exit.isFailure(yield* start(2, providerThread, "high").pipe(Effect.exit)));
      assert.isFalse(
        frames.some(
          (frame) =>
            frame.type === "switch_session" ||
            (frame.type === "set_thinking_level" && frame.level === "high"),
        ),
      );
      yield* runtime.readThreadSnapshot({ providerThread });
    }).pipe(Effect.scoped, Effect.provide(layer)),
);

it.effect.each([true, false])(
  "recovers native followups and validates pulse reports (valid=%s)",
  (valid) =>
    Effect.gen(function* () {
      const { runtime, providerThread, replies, frames } = yield* setup();
      const side = runtime.backgroundQuestions!;
      const root = {
        id: "persisted",
        leafId: null,
        question: "Old question",
        answer: "Old answer",
        status: "complete",
        createdAt: 1,
        updatedAt: 2,
      };
      const followup = {
        question: "Follow up",
        answer: "Recovered answer",
        status: "complete",
        createdAt: 3,
        updatedAt: 4,
      };
      replies.set("get_btw_history", { records: [{ ...root, followUps: [followup] }] });
      const history = yield* side.history({ providerThread });
      assert.deepEqual(
        history.records[0]?.turns.map((turn) => turn.answer),
        ["Old answer", "Recovered answer"],
      );
      replies.set("btw", {
        record: { ...root, followUps: [followup, { ...followup, answer: "", status: "running" }] },
      });
      yield* side.ask({ providerThread, kind: "btw", question: "Continue", recordId: root.id });
      assert.deepEqual(frames.at(-1), { type: "btw", question: "Continue", recordId: root.id });
      // A separate native session reopening starts from its persisted terminal record.
      const reopened = yield* setup();
      const pulseReply = { ...root, id: "pulse", status: "running" };
      reopened.replies.set("btw", { record: pulseReply });
      yield* reopened.runtime.backgroundQuestions!.ask({
        providerThread: reopened.providerThread,
        kind: "pulse",
      });
      const prompt = reopened.frames.findLast((frame) => frame.type === "btw")?.question;
      assert.isString(prompt);
      const report = {
        phase: "Build",
        progress: "About half",
        momentum: "On track",
        evidence: "One done, one gap",
        risk: "Unverified",
        next: "Test",
        steer: "Review the failing check",
      };
      reopened.replies.set("get_btw_history", {
        records: [
          {
            ...pulseReply,
            question: prompt,
            status: "complete",
            answer: valid ? JSON.stringify(report) : '{"phase":7}',
          },
        ],
      });
      // Complete the live record before requesting a disk refresh.
      yield* reopened.emit({
        type: "btw_record",
        record: {
          ...pulseReply,
          status: "complete",
          question: prompt,
          answer: valid ? JSON.stringify(report) : "{}",
        },
      });
      const snapshots = yield* Queue.unbounded<BackgroundQuestionSnapshot>();
      yield* reopened.runtime
        .backgroundQuestions!.subscribe({ providerThread: reopened.providerThread })
        .pipe(
          Stream.runForEach((value) => Queue.offer(snapshots, value)),
          Effect.forkScoped,
        );
      let snapshot = yield* Queue.take(snapshots);
      while (snapshot.records[0]?.turns.at(-1)?.status !== "complete")
        snapshot = yield* Queue.take(snapshots);
      assert.equal(snapshot.records[0]?.kind, "pulse");
      assert.deepEqual(snapshot.records[0]?.report, valid ? report : null);
      if (!valid) assert.match(snapshot.records[0]!.reportError!, /invalid report/);
      assert.isFalse(
        reopened.frames.some((frame) => frame.type === "steer" || frame.type === "prompt"),
      );
    }).pipe(Effect.scoped, Effect.provide(layer)),
);

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
  it.effect(
    "reports native effort at registration and after a turn instead of guessing from the requested choice",
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const test = yield* setup();
          const registered = yield* test.take("provider_thread.updated");
          if (registered.type !== "provider_thread.updated")
            throw new Error("Missing registered thread");
          assert.deepEqual(registered.providerThread.nativeMetadata?.modelSelection, {
            instanceId: selection.instanceId,
            model: "test/model",
            options: [{ id: "thinking", value: "off" }],
          });
          yield* test.start();
          yield* test.take("provider_thread.updated");
          test.setNativeThinking("high");
          yield* test.emit({
            type: "prompt_result",
            id: "t3-prompt-1",
            status: "completed",
            sessionSettled: true,
          });
          const completed = yield* test.take("provider_thread.updated");
          if (completed.type !== "provider_thread.updated")
            throw new Error("Missing completed thread");
          assert.equal(completed.providerThread.status, "idle");
          assert.deepEqual(completed.providerThread.nativeMetadata?.modelSelection?.options, [
            { id: "thinking", value: "high" },
          ]);
          yield* test.take("turn.terminal");
          yield* test.start(2, test.providerThread, "off");
          yield* test.take("provider_thread.updated");
          yield* test.emit({
            type: "prompt_result",
            id: "t3-prompt-2",
            status: "completed",
            sessionSettled: true,
          });
          const selected = yield* test.take("provider_thread.updated");
          if (selected.type !== "provider_thread.updated")
            throw new Error("Missing selected effort");
          assert.deepEqual(selected.providerThread.nativeMetadata?.modelSelection?.options, [
            { id: "thinking", value: "off" },
          ]);
        }),
      ).pipe(Effect.provide(layer)),
  );
});
