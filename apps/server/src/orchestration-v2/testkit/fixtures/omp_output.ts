import { assert } from "@effect/vitest";
import type { ProviderReplayTranscript } from "@t3tools/contracts";
import type { OrchestratorV2ScenarioResult } from "../OrchestratorScenario.ts";
import {
  assertAllRuntimeRequestsResolved,
  assertBaseProjection,
  assertSemanticProjectionIntegrity,
  projectionFor,
  assertVisibleUserMessagesExclude,
  THREAD_ROLLBACK_SECOND_PROMPT,
} from "./shared.ts";

export function assertOmpOutput(
  result: OrchestratorV2ScenarioResult,
  transcript: ProviderReplayTranscript,
) {
  const interrupted = transcript.scenario === "turn_interrupt";
  const rollback = transcript.scenario === "thread_rollback";
  const twoTurns = ["multi_turn", "queued_turn", "provider_thread_resume"].includes(
    transcript.scenario,
  );
  assertBaseProjection({
    result,
    transcript,
    runCount: rollback ? 3 : twoTurns ? 2 : 1,
    runStatuses: rollback
      ? ["completed", "rolled_back", "completed"]
      : interrupted
        ? ["interrupted"]
        : twoTurns
          ? ["completed", "completed"]
          : ["completed"],
  });
  const projection = projectionFor(result, transcript.scenario);
  assertSemanticProjectionIntegrity(projection);
  assertAllRuntimeRequestsResolved(projection);
  if (interrupted) {
    assert.include(["interrupted", "cancelled"], projection.providerTurns[0]?.status);
    assert.equal(projection.providerThreads[0]?.status, "idle");
    return;
  }
  const answers = projection.turnItems.flatMap((item) =>
    item.type === "assistant_message" ? [item.text] : [],
  );
  if (rollback) {
    assertVisibleUserMessagesExclude(projection, [THREAD_ROLLBACK_SECOND_PROMPT]);
    assert.include(answers.at(-1), "rollback fixture first turn complete");
    assert.notInclude(answers.at(-1), "second turn");
    assert.lengthOf(projection.providerThreads, 1);
  }
  if (transcript.scenario === "simple") assert.include(answers.join("\n"), "fixture simple ok");
  if (transcript.scenario === "multi_turn" || transcript.scenario === "queued_turn") {
    assert.include(answers[0], "first fixture turn complete");
    assert.include(answers[1], "second fixture turn complete");
  }
  if (transcript.scenario === "provider_thread_resume") {
    assert.lengthOf(projection.providerThreads, 1);
    assert.include(answers[1], "provider thread resume fixture first turn complete");
    assert.include(answers[1], "provider thread resume fixture second turn complete");
  }
  if (transcript.scenario === "message_steering")
    assert.include(answers.join("\n"), "steering fixture observed");
  if (transcript.scenario === "tool_call_denied_write") {
    assert.isTrue(projection.runtimeRequests.some((request) => request.decision === "decline"));
  }
}
