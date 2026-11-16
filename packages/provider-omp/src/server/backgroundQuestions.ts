import {
  BackgroundQuestionTurn,
  PulseReport,
  type BackgroundQuestionRecord,
  type BackgroundQuestionSnapshot,
} from "@t3tools/contracts";
import type * as ProviderAdapter from "@t3tools/provider-core/server/ProviderAdapter";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import * as SubscriptionRef from "effect/SubscriptionRef";
import { field, string, type OmpRecord } from "./frames.ts";

const pulsePrompt = `Give a read-only health check of the CURRENT session's actual task, including any work in progress. Do not take actions or call tools. Return ONLY a JSON object with string fields "phase", "progress", "momentum", "evidence", "risk", "next" and a string or null field "steer".
phase: current work stage, with remaining major stages.
progress: approximate fraction or range of the *whole requested outcome* complete, not tokens/time/number of tool calls. Explain the basis briefly; if not estimable say so. Never imply a precise measurement.
momentum: On track / Slow / Stalled, with a concrete reason. Flag repeated searches, repeated failed checks, reversals, scope drift, or waiting without a next action, but do not mistake normal verification or a long-running job for wheel-spinning.
evidence: one concrete completed milestone and one specific remaining gap; distinguish observed facts from inference.
risk: the most material blocker, safety issue, or verification gap; say "None evident" if there is none.
next: the single highest-value next action in the current plan.
steer: null unless the session actually needs USER steering to correct a specific problem. If needed, give one short, ready-to-paste instruction addressed to the working agent. Do not suggest steering just because work remains, and do not ask the agent to ignore safeguards. Do not execute the steer. Keep each field under 180 characters and the steer under 240; be honest about missing visibility.`;
const NativeRecord = Schema.Struct({
  ...BackgroundQuestionTurn.fields,
  id: Schema.String,
  followUps: Schema.optional(Schema.Array(BackgroundQuestionTurn)),
});
const decodeRecord = Schema.decodeUnknownEffect(NativeRecord);
const decodeReport = Schema.decodeUnknownSync(PulseReport);

const normalize = (record: typeof NativeRecord.Type): BackgroundQuestionRecord => {
  const turns = [record, ...(record.followUps ?? [])].map(
    ({ question, answer, status, createdAt, updatedAt, error }) => ({
      question,
      answer,
      status,
      createdAt,
      updatedAt,
      ...(error === undefined ? {} : { error }),
    }),
  );
  const kind = record.question === pulsePrompt ? "pulse" : "btw";
  const latest = turns.at(-1)!;
  let report: PulseReport | null = null;
  let reportError: string | null = null;
  if (kind === "pulse" && latest.status === "complete") {
    try {
      report = decodeReport(JSON.parse(latest.answer.replace(/^```(?:json)?\s*|\s*```$/g, "")));
    } catch {
      reportError = "Run health returned an invalid report. No steering was applied.";
    }
  }
  return {
    id: record.id,
    kind,
    createdAt: record.createdAt,
    updatedAt: latest.updatedAt,
    turns,
    report,
    reportError,
  };
};

export const makeOmpBackgroundQuestions = Effect.fn("OmpBackgroundQuestions.make")(
  function* (options: {
    readonly request: (
      frame: OmpRecord,
    ) => Effect.Effect<unknown, ProviderAdapter.ProviderAdapterV2Error>;
    readonly error: (detail: string, cause?: unknown) => ProviderAdapter.ProviderAdapterV2Error;
    readonly check: (
      thread: Parameters<
        ProviderAdapter.ProviderBackgroundQuestions["history"]
      >[0]["providerThread"],
    ) => Effect.Effect<void, ProviderAdapter.ProviderAdapterV2Error>;
    readonly lock: <A, E, R>(effect: Effect.Effect<A, E, R>) => Effect.Effect<A, E, R>;
  }) {
    const state = yield* SubscriptionRef.make<BackgroundQuestionSnapshot>({ records: [] });
    let stopped = yield* Deferred.make<never, ProviderAdapter.ProviderAdapterV2Error>();
    let startingTopic: string | null | undefined;
    let uncertain = false;
    let ended = false;
    let requests = 0;
    let revision = 0;
    let cancelEpoch = 0;
    const running = () =>
      startingTopic !== undefined ||
      uncertain ||
      state.value.records.some((record) => record.turns.at(-1)?.status === "running");
    const busy = () => requests > 0 || running();
    const upsert = (record: BackgroundQuestionRecord) =>
      SubscriptionRef.update(state, ({ records }) => ({
        records: [record, ...records.filter((entry) => entry.id !== record.id)].sort(
          (a, b) => b.createdAt - a.createdAt,
        ),
      }));
    const parse = (value: unknown) =>
      decodeRecord(value).pipe(
        Effect.map(normalize),
        Effect.mapError((cause) => options.error("OMP returned invalid BTW history", cause)),
      );
    const handle = Effect.fn("OmpBackgroundQuestions.handle")(function* (frame: OmpRecord) {
      if (frame.type === "btw_record") {
        const record = yield* parse(frame.record);
        if (
          startingTopic !== undefined ||
          uncertain ||
          state.value.records.some((entry) => entry.id === record.id)
        ) {
          revision++;
          yield* upsert(record);
        }
        return true;
      }
      if (frame.type === "btw_delta") {
        const id = string(frame, "recordId");
        const delta = string(frame, "delta");
        if (id === undefined || delta === undefined)
          return yield* options.error("OMP returned an invalid BTW delta");
        revision++;
        yield* SubscriptionRef.update(state, ({ records }) => ({
          records: records.map((record) =>
            record.id !== id
              ? record
              : {
                  ...record,
                  turns: record.turns.map((turn, index) =>
                    index !== record.turns.length - 1
                      ? turn
                      : { ...turn, answer: turn.answer + delta },
                  ),
                },
          ),
        }));
        return true;
      }
      if (frame.type === "notice" && frame.source === "btw-history") {
        return yield* options.error(
          string(frame, "message") ?? "OMP could not persist BTW history",
        );
      }
      return false;
    });
    const check = Effect.fn("OmpBackgroundQuestions.check")(function* (
      input: Parameters<ProviderAdapter.ProviderBackgroundQuestions["history"]>[0],
    ) {
      if (ended) return yield* options.error("OMP background question session has ended");
      yield* options.check(input.providerThread);
    });
    const history = Effect.fn("OmpBackgroundQuestions.history")(function* (
      input: Parameters<ProviderAdapter.ProviderBackgroundQuestions["history"]>[0],
    ) {
      yield* options.lock(
        Effect.gen(function* () {
          yield* check(input);
          requests++;
        }),
      );
      return yield* Effect.gen(function* () {
        // Native history already includes deltas still queued by the transport.
        if (running() && !uncertain) return state.value;
        const version = revision;
        const result = yield* options.request({ type: "get_btw_history" });
        const values = field(result, "records");
        if (!Array.isArray(values))
          return yield* options.error("Installed OMP does not support BTW history; update OMP");
        const records = yield* Effect.forEach(values, parse);
        uncertain = false;
        if (revision !== version) return state.value;
        yield* SubscriptionRef.set(state, { records });
        return { records };
      }).pipe(
        Effect.ensuring(
          Effect.sync(() => {
            requests--;
          }),
        ),
      );
    });
    const api: ProviderAdapter.ProviderBackgroundQuestions = {
      history,
      ask: Effect.fn("OmpBackgroundQuestions.ask")(function* (input) {
        const epoch = yield* options.lock(
          Effect.gen(function* () {
            yield* check(input);
            if (running())
              return yield* options.error(
                "A background question is still running; cancel it first",
              );
            startingTopic = input.recordId ?? null;
            revision++;
            return cancelEpoch;
          }),
        );
        return yield* Effect.gen(function* () {
          const question = input.kind === "pulse" ? pulsePrompt : input.question?.trim();
          if (!question) return yield* options.error("BTW requires a non-empty question");
          if (input.recordId !== undefined) {
            let previous = state.value.records.find((record) => record.id === input.recordId);
            if (previous === undefined) {
              const result = yield* options.request({ type: "get_btw_history" });
              const records = field(result, "records");
              if (!Array.isArray(records))
                return yield* options.error("OMP returned invalid BTW history");
              previous = (yield* Effect.forEach(records, parse)).find(
                (record) => record.id === input.recordId,
              );
            }
            if (previous === undefined || previous.kind !== input.kind)
              return yield* options.error(
                "The background topic is missing or has a different kind; reopen history",
              );
          }
          const result = yield* options.request({
            type: "btw",
            question,
            ...(input.recordId === undefined ? {} : { recordId: input.recordId }),
          });
          const record = yield* parse(field(result, "record"));
          const current = state.value.records.find((entry) => entry.id === record.id);
          if (current === undefined || current.turns.length < record.turns.length)
            yield* upsert(record);
          if (epoch !== cancelEpoch)
            yield* options.request({ type: "btw_cancel", recordId: record.id });
          return state.value.records.find((entry) => entry.id === record.id) ?? record;
        }).pipe(
          Effect.tapError(() =>
            Effect.sync(() => {
              uncertain = true;
            }),
          ),
          Effect.onInterrupt(() =>
            Effect.sync(() => {
              uncertain = true;
            }),
          ),
          Effect.ensuring(
            Effect.sync(() => {
              startingTopic = undefined;
            }),
          ),
        );
      }),
      cancel: Effect.fn("OmpBackgroundQuestions.cancel")(function* (input) {
        yield* options.lock(
          Effect.gen(function* () {
            yield* check(input);
            if (input.recordId === undefined || startingTopic === input.recordId) cancelEpoch++;
            requests++;
          }),
        );
        return yield* options
          .request({
            type: "btw_cancel",
            ...(input.recordId === undefined ? {} : { recordId: input.recordId }),
          })
          .pipe(
            Effect.map((result) => {
              uncertain = false;
              return { cancelled: field(result, "cancelled") === true };
            }),
            Effect.ensuring(
              Effect.sync(() => {
                requests--;
              }),
            ),
          );
      }),
      subscribe: (input) =>
        Stream.unwrap(
          history(input).pipe(
            Effect.as(
              SubscriptionRef.changes(state).pipe(
                Stream.merge(Stream.fromEffect(Deferred.await(stopped))),
              ),
            ),
          ),
        ),
    };
    const close = Effect.fn("OmpBackgroundQuestions.close")(function* () {
      ended = true;
      yield* SubscriptionRef.update(state, ({ records }) => ({
        records: records.map((record) => ({
          ...record,
          turns: record.turns.map((turn) =>
            turn.status === "running" ? { ...turn, status: "interrupted" as const } : turn,
          ),
        })),
      }));
      yield* Deferred.fail(stopped, options.error("OMP background question session has ended"));
    });
    const reset = Effect.fn("OmpBackgroundQuestions.reset")(function* () {
      yield* Deferred.fail(
        stopped,
        options.error("OMP background conversation changed; reopen history"),
      );
      stopped = yield* Deferred.make<never, ProviderAdapter.ProviderAdapterV2Error>();
      yield* SubscriptionRef.set(state, { records: [] });
      uncertain = false;
      revision++;
    });
    const fail = Effect.fn("OmpBackgroundQuestions.fail")(function* (
      error: ProviderAdapter.ProviderAdapterV2Error,
    ) {
      yield* Deferred.fail(stopped, error);
      stopped = yield* Deferred.make<never, ProviderAdapter.ProviderAdapterV2Error>();
    });
    return { api, handle, busy, running, close, reset, fail };
  },
);
