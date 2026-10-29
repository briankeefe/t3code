import { assert, describe, it } from "@effect/vitest";
import { ompCompletion, ompQuestions, ompUiResponse, walkOmpBranch } from "./frames.ts";

describe("OMP native lifecycle", () => {
  it("waits through nonterminal yields and background work until session settlement", () => {
    assert.equal(
      ompCompletion({ type: "agent_end", isTerminal: false, yielded: false }, "p1"),
      null,
    );
    assert.equal(
      ompCompletion(
        { type: "agent_end", isTerminal: false, yielded: true, awaitingAsyncWork: true },
        "p1",
      ),
      null,
    );
    assert.equal(
      ompCompletion(
        { type: "prompt_result", id: "p1", status: "completed", sessionSettled: false },
        "p1",
      ),
      null,
    );
    assert.equal(
      ompCompletion(
        { type: "prompt_result", id: "old", status: "error", sessionSettled: true },
        "p1",
      ),
      null,
    );
    assert.equal(ompCompletion({ type: "session_settled" }, "p1"), "completed");
    assert.equal(
      ompCompletion(
        { type: "prompt_result", id: "p1", status: "error", sessionSettled: true },
        "p1",
      ),
      "failed",
    );
    assert.equal(
      ompCompletion(
        { type: "prompt_result", id: "p1", status: "aborted", sessionSettled: true },
        "p1",
      ),
      "interrupted",
    );
    assert.equal(
      ompCompletion(
        { type: "response", id: "p1", success: true, data: { agentInvoked: false } },
        "p1",
      ),
      "completed",
    );
  });

  it("preserves structured ask ids, choices, and free text in native replies", () => {
    const frame = {
      type: "extension_ui_request",
      method: "ask",
      id: "ui1",
      questions: [
        { id: "db", question: "Database?", options: [{ label: "SQLite" }] },
        {
          id: "features",
          question: "Features?",
          multi: true,
          options: [{ label: "Auth" }, { label: "Search" }],
        },
      ],
    };
    assert.deepEqual(
      ompQuestions(frame).map((q) => q.id),
      ["db", "features"],
    );
    assert.deepEqual(
      ompUiResponse(frame, undefined, { db: "DuckDB", features: ["Auth", "Search"] }),
      {
        type: "extension_ui_response",
        id: "ui1",
        answers: [
          { id: "db", selectedOptions: [], customInput: "DuckDB" },
          { id: "features", selectedOptions: ["Auth", "Search"] },
        ],
      },
    );
  });

  it("follows the active leaf rather than append order and rejects broken ancestry", () => {
    const entries = [
      { id: "u1", parentId: null, type: "message", message: { role: "user" } },
      { id: "a1", parentId: "u1", type: "message", message: { role: "assistant" } },
      { id: "discarded", parentId: "a1", type: "message", message: { role: "user" } },
      { id: "u2", parentId: "a1", type: "message", message: { role: "user" } },
      { id: "a2", parentId: "u2", type: "message", message: { role: "assistant" } },
    ];
    assert.deepEqual(
      walkOmpBranch({ entries, leafId: "a2" }).map((e) => e.id),
      ["u1", "a1", "u2", "a2"],
    );
    assert.throws(() => walkOmpBranch({ entries, leafId: "missing" }), /ancestry/);
    assert.throws(
      () => walkOmpBranch({ entries: [{ id: "loop", parentId: "loop" }], leafId: "loop" }),
      /ancestry/,
    );
  });
});
