import { assert, describe, it } from "@effect/vitest";
import {
  NodeId,
  ProviderInstanceId,
  ProviderThreadId,
  ProviderTurnId,
  RunAttemptId,
  RunId,
  ThreadId,
  type OrchestrationV2ProviderTurn,
} from "@t3tools/contracts";
import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import type { ProviderAdapterV2Event } from "@t3tools/provider-core/server/ProviderAdapter";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import { makeOmpTurnEvents, OMP_PROVIDER, type OmpTurnEventInput } from "./events.ts";

const input: OmpTurnEventInput = {
  threadId: ThreadId.make("omp-thread"),
  runId: RunId.make("omp-run"),
  rootNodeId: NodeId.make("omp-root"),
  providerTurnOrdinal: 1,
  modelSelection: { instanceId: ProviderInstanceId.make("omp"), model: "provider/model" },
  providerThread: { id: ProviderThreadId.make("omp-provider-thread") },
};
const providerTurn: OrchestrationV2ProviderTurn = {
  id: ProviderTurnId.make("omp-turn"),
  providerThreadId: input.providerThread.id,
  nodeId: input.rootNodeId,
  runAttemptId: RunAttemptId.make("omp-attempt"),
  nativeTurnRef: { driver: OMP_PROVIDER, nativeId: "weak-turn", strength: "weak" },
  ordinal: 1,
  status: "running",
  startedAt: DateTime.makeUnsafe(0),
  completedAt: null,
};

describe("OMP native turn event translation", () => {
  it.effect(
    "completes independent text/thinking blocks and preserves native tool inputs/results",
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const events: ProviderAdapterV2Event[] = [];
          const emitter = makeOmpTurnEvents({
            input,
            providerTurn,
            ids: yield* IdAllocator.IdAllocatorV2,
            scope: yield* Effect.scope,
            emit: (event) =>
              Effect.sync(() => {
                events.push(event);
              }),
          });
          yield* emitter.handle({
            type: "message_start",
            messageId: "m1",
            message: { role: "assistant" },
          });
          yield* emitter.handle({
            type: "message_update",
            messageId: "m1",
            message: { role: "assistant" },
            assistantMessageEvent: { type: "thinking_delta", contentIndex: 0, delta: "Consider" },
          });
          yield* emitter.handle({
            type: "message_update",
            messageId: "m1",
            message: { role: "assistant" },
            assistantMessageEvent: { type: "text_delta", contentIndex: 1, delta: "Hello" },
          });
          yield* emitter.handle({
            type: "message_end",
            messageId: "m1",
            message: {
              role: "assistant",
              content: [
                { type: "thinking", thinking: "Consider carefully" },
                { type: "text", text: "Hello world" },
              ],
            },
          });
          yield* emitter.handle({
            type: "tool_execution_start",
            toolCallId: "bash1",
            toolName: "bash",
            args: { command: "printf result" },
          });
          yield* emitter.handle({
            type: "tool_execution_end",
            toolCallId: "bash1",
            toolName: "bash",
            isError: false,
            result: { content: [{ type: "text", text: "result" }], details: { exitCode: 0 } },
          });
          yield* emitter.handle({
            type: "tool_execution_start",
            toolCallId: "read1",
            toolName: "read",
            args: { path: "fixture.txt" },
          });
          yield* emitter.handle({
            type: "tool_execution_end",
            toolCallId: "read1",
            toolName: "read",
            isError: true,
            result: { content: [{ type: "text", text: "not found" }] },
          });
          yield* emitter.finish("completed");
          const items = events.flatMap((event) =>
            event.type === "turn_item.updated" ? [event.turnItem] : [],
          );
          assert.isTrue(
            items.some(
              (item) =>
                item.type === "reasoning" && item.text === "Consider carefully" && !item.streaming,
            ),
          );
          assert.isTrue(
            items.some(
              (item) =>
                item.type === "assistant_message" && item.text === "Hello world" && !item.streaming,
            ),
          );
          assert.isTrue(
            items.some(
              (item) =>
                item.type === "command_execution" &&
                item.input === "printf result" &&
                item.output === "result" &&
                item.exitCode === 0 &&
                item.status === "completed",
            ),
          );
          assert.isTrue(
            items.some(
              (item) =>
                item.type === "dynamic_tool" &&
                item.toolName === "read" &&
                item.status === "failed",
            ),
          );
          assert.isTrue(
            events.some(
              (event) =>
                event.type === "message.updated" &&
                event.message.text === "Hello world" &&
                !event.message.streaming,
            ),
          );
        }),
      ).pipe(Effect.provide(IdAllocator.layer)),
  );

  it.effect("keeps native subagent progress attached to its original run through completion", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const events: ProviderAdapterV2Event[] = [];
        const emitter = makeOmpTurnEvents({
          input,
          providerTurn,
          ids: yield* IdAllocator.IdAllocatorV2,
          scope: yield* Effect.scope,
          emit: (event) =>
            Effect.sync(() => {
              events.push(event);
            }),
        });
        yield* emitter.handle({
          type: "subagent_lifecycle",
          payload: {
            id: "worker",
            index: 1,
            agent: "scout",
            status: "started",
            parentToolCallId: "task1",
          },
        });
        assert.isTrue(emitter.hasLiveSubagents());
        yield* emitter.handle({
          type: "subagent_progress",
          payload: {
            index: 1,
            agent: "scout",
            task: "Inspect",
            progress: {
              id: "worker",
              status: "running",
              task: "Inspect",
              stage: "Reading source",
              recentOutput: ["Evidence"],
            },
          },
        });
        yield* emitter.finish("completed");
        yield* emitter.handle({
          type: "subagent_lifecycle",
          payload: { id: "worker", index: 1, agent: "scout", status: "completed" },
        });
        assert.isFalse(emitter.hasLiveSubagents());
        const agents = events.flatMap((event) =>
          event.type === "subagent.updated" ? [event.subagent] : [],
        );
        assert.equal(agents.length, 3);
        assert.equal(agents[1]?.progress, "Reading source");
        assert.equal(agents[2]?.status, "completed");
        assert.equal(agents[2]?.prompt, "Inspect");
        assert.isTrue(
          agents.every(
            (agent) => agent.runId === input.runId && agent.nativeTaskRef?.nativeId === "worker",
          ),
        );
        const timeline = events.flatMap((event) =>
          event.type === "turn_item.updated" && event.turnItem.type === "subagent"
            ? [event.turnItem]
            : [],
        );
        assert.equal(timeline[0]?.status, "running");
        assert.equal(timeline.at(-1)?.status, "completed");
        assert.equal(timeline.at(-1)?.subagentId, agents[2]?.id);
        assert.equal(timeline.at(-1)?.runId, input.runId);
      }),
    ).pipe(Effect.provide(IdAllocator.layer)),
  );
});
