import {
  BackgroundQuestionError,
  type BackgroundQuestionAskInput,
  type BackgroundQuestionCancelInput,
  type BackgroundQuestionInput,
  type BackgroundQuestionRecord,
  type BackgroundQuestionSnapshot,
} from "@t3tools/contracts";
import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Stream from "effect/Stream";
import * as Schema from "effect/Schema";
import * as ThreadManagementService from "./ThreadManagementService.ts";
import * as ProviderSessionManager from "./ProviderSessionManager.ts";
import * as RuntimePolicy from "./RuntimePolicy.ts";

export class BackgroundQuestions extends Context.Service<
  BackgroundQuestions,
  {
    readonly history: (
      input: BackgroundQuestionInput,
    ) => Effect.Effect<BackgroundQuestionSnapshot, BackgroundQuestionError>;
    readonly ask: (
      input: BackgroundQuestionAskInput,
    ) => Effect.Effect<BackgroundQuestionRecord, BackgroundQuestionError>;
    readonly cancel: (
      input: BackgroundQuestionCancelInput,
    ) => Effect.Effect<{ readonly cancelled: boolean }, BackgroundQuestionError>;
    readonly subscribe: (
      input: BackgroundQuestionInput,
    ) => Stream.Stream<BackgroundQuestionSnapshot, BackgroundQuestionError>;
  }
>()("t3/orchestration-v2/BackgroundQuestions") {}

const isBackgroundQuestionError = Schema.is(BackgroundQuestionError);

const make = Effect.gen(function* () {
  const threads = yield* ThreadManagementService.ThreadManagementService;
  const sessions = yield* ProviderSessionManager.ProviderSessionManagerV2;
  const policies = yield* RuntimePolicy.RuntimePolicyV2;
  const ids = yield* IdAllocator.IdAllocatorV2;
  const resolve = Effect.fn("BackgroundQuestions.resolve")(
    function* ({ threadId }: BackgroundQuestionInput) {
      const projection = yield* threads.getThreadRecords(threadId, [
        "providerThreads",
        "providerSessions",
      ]);
      const { thread } = projection;
      const providerThread = projection.providerThreads.find(
        (entry) => entry.id === thread.activeProviderThreadId,
      );
      if (
        thread.deletedAt !== null ||
        providerThread === undefined ||
        providerThread.appThreadId !== threadId ||
        providerThread.nativeThreadRef === null ||
        providerThread.nativeThreadRef.nativeId === null
      )
        return yield* new BackgroundQuestionError({ threadId, reason: "thread-unavailable" });
      if (providerThread.driver !== "omp")
        return yield* new BackgroundQuestionError({ threadId, reason: "provider-unsupported" });
      const providerSessionId =
        providerThread.providerSessionId ??
        (yield* ids.allocate.providerSession({
          threadId,
          providerInstanceId: providerThread.providerInstanceId,
        }));
      const live = yield* sessions.get(providerSessionId);
      let runtime = Option.getOrUndefined(live);
      if (runtime === undefined) {
        const modelSelection = thread.modelSelection;
        if (modelSelection.instanceId !== providerThread.providerInstanceId)
          return yield* new BackgroundQuestionError({ threadId, reason: "thread-unavailable" });
        const runtimePolicy = yield* policies.resolve({ thread, modelSelection });
        const previous = projection.providerSessions.find(
          (entry) => entry.id === providerSessionId,
        );
        runtime = yield* sessions.open({
          threadId,
          providerSessionId,
          modelSelection,
          runtimePolicy,
          initialNativeThreadId: providerThread.nativeThreadRef.nativeId,
          ...(previous === undefined ? {} : { resumeFromSession: previous }),
        });
        yield* runtime.resumeThread({ providerThread, threadId, modelSelection, runtimePolicy });
      }
      if (
        runtime.driver !== "omp" ||
        runtime.instanceId !== providerThread.providerInstanceId ||
        runtime.backgroundQuestions === undefined
      )
        return yield* new BackgroundQuestionError({ threadId, reason: "provider-unsupported" });
      return { api: runtime.backgroundQuestions, providerThread };
    },
    (effect, { threadId }) =>
      effect.pipe(
        Effect.mapError((cause) =>
          isBackgroundQuestionError(cause)
            ? cause
            : new BackgroundQuestionError({ threadId, reason: "request-failed", cause }),
        ),
      ),
  );
  return BackgroundQuestions.of({
    history: Effect.fn("BackgroundQuestions.history")(function* (input) {
      const { api, providerThread } = yield* resolve(input);
      return yield* api.history({ providerThread }).pipe(
        Effect.mapError(
          (cause) =>
            new BackgroundQuestionError({
              threadId: input.threadId,
              reason: "request-failed",
              cause,
            }),
        ),
      );
    }),
    ask: Effect.fn("BackgroundQuestions.ask")(function* (input) {
      const { api, providerThread } = yield* resolve(input);
      return yield* api.ask({ ...input, providerThread }).pipe(
        Effect.mapError(
          (cause) =>
            new BackgroundQuestionError({
              threadId: input.threadId,
              reason: "request-failed",
              cause,
            }),
        ),
      );
    }),
    cancel: Effect.fn("BackgroundQuestions.cancel")(function* (input) {
      const { api, providerThread } = yield* resolve(input);
      return yield* api.cancel({ ...input, providerThread }).pipe(
        Effect.mapError(
          (cause) =>
            new BackgroundQuestionError({
              threadId: input.threadId,
              reason: "request-failed",
              cause,
            }),
        ),
      );
    }),
    subscribe: (input) =>
      Stream.unwrap(
        resolve(input).pipe(
          Effect.map(({ api, providerThread }) =>
            api.subscribe({ providerThread }).pipe(
              Stream.mapError(
                (cause) =>
                  new BackgroundQuestionError({
                    threadId: input.threadId,
                    reason: "request-failed",
                    cause,
                  }),
              ),
            ),
          ),
        ),
      ),
  });
});
export const layer = Layer.effect(BackgroundQuestions, make);
