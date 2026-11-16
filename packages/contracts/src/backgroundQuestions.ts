import * as Schema from "effect/Schema";
import { ThreadId, TrimmedNonEmptyString } from "./baseSchemas.ts";

export const PulseReport = Schema.Struct({
  phase: Schema.String,
  progress: Schema.String,
  momentum: Schema.String,
  evidence: Schema.String,
  risk: Schema.String,
  next: Schema.String,
  steer: Schema.NullOr(Schema.String),
});
export type PulseReport = typeof PulseReport.Type;
export const BackgroundQuestionTurn = Schema.Struct({
  question: Schema.String,
  answer: Schema.String,
  status: Schema.Literals(["running", "complete", "cancelled", "error", "interrupted"]),
  createdAt: Schema.Number,
  updatedAt: Schema.Number,
  error: Schema.optional(Schema.String),
});
export type BackgroundQuestionTurn = typeof BackgroundQuestionTurn.Type;
export const BackgroundQuestionRecord = Schema.Struct({
  id: Schema.String,
  kind: Schema.Literals(["btw", "pulse"]),
  createdAt: Schema.Number,
  updatedAt: Schema.Number,
  turns: Schema.Array(BackgroundQuestionTurn),
  report: Schema.NullOr(PulseReport),
  reportError: Schema.NullOr(Schema.String),
});
export type BackgroundQuestionRecord = typeof BackgroundQuestionRecord.Type;
export const BackgroundQuestionSnapshot = Schema.Struct({
  records: Schema.Array(BackgroundQuestionRecord),
});
export type BackgroundQuestionSnapshot = typeof BackgroundQuestionSnapshot.Type;
export const BackgroundQuestionInput = Schema.Struct({ threadId: ThreadId });
export type BackgroundQuestionInput = typeof BackgroundQuestionInput.Type;
export const BackgroundQuestionAskInput = Schema.Struct({
  threadId: ThreadId,
  kind: Schema.Literals(["btw", "pulse"]),
  question: Schema.optional(TrimmedNonEmptyString),
  recordId: Schema.optional(TrimmedNonEmptyString),
});
export type BackgroundQuestionAskInput = typeof BackgroundQuestionAskInput.Type;
export const BackgroundQuestionCancelInput = Schema.Struct({
  threadId: ThreadId,
  recordId: Schema.optional(TrimmedNonEmptyString),
});
export type BackgroundQuestionCancelInput = typeof BackgroundQuestionCancelInput.Type;
export class BackgroundQuestionError extends Schema.TaggedError<BackgroundQuestionError>()(
  "BackgroundQuestionError",
  {
    threadId: ThreadId,
    reason: Schema.Literals(["provider-unsupported", "thread-unavailable", "request-failed"]),
    cause: Schema.optional(Schema.Defect()),
  },
) {
  override get message(): string {
    switch (this.reason) {
      case "provider-unsupported":
        return "Background questions and /pulse require the OMP provider.";
      case "thread-unavailable":
        return "This thread has no available OMP conversation. Start a main turn first.";
      case "request-failed":
        return "The background question request failed. Reopen background history and retry.";
    }
  }
}
