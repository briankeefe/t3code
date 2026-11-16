import { assert, it } from "@effect/vitest";
import {
  ProviderInstanceId,
  ProviderSessionId,
  ProviderThreadId,
  ThreadId,
} from "@t3tools/contracts";
import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import type * as ProviderAdapter from "@t3tools/provider-core/server/ProviderAdapter";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as BackgroundQuestions from "./BackgroundQuestions.ts";
import * as ProviderSessionManager from "./ProviderSessionManager.ts";
import * as RuntimePolicy from "./RuntimePolicy.ts";
import * as ThreadManagementService from "./ThreadManagementService.ts";

it.effect.each(["live", "cold", "wrong-owner", "unsupported", "missing-native-id"] as const)(
  "resolves background history without disturbing an active session (%s)",
  (mode) =>
    Effect.gen(function* () {
      const threadId = ThreadId.make("side-owner");
      const instanceId = ProviderInstanceId.make("omp");
      const providerSessionId = ProviderSessionId.make("side-session");
      const providerThreadId = ProviderThreadId.make("side-native-thread");
      const calls: string[] = [];
      const providerThread = {
        id: providerThreadId,
        providerSessionId,
        providerInstanceId: instanceId,
        driver: mode === "unsupported" ? "codex" : "omp",
        appThreadId: mode === "wrong-owner" ? ThreadId.make("other-thread") : threadId,
        nativeThreadRef: { nativeId: mode === "missing-native-id" ? null : "/saved/session.jsonl" },
      };
      const runtime = {
        driver: "omp",
        instanceId,
        resumeThread: () =>
          Effect.sync(() => {
            calls.push("resume");
            return providerThread;
          }),
        backgroundQuestions: {
          history: () =>
            Effect.sync(() => {
              calls.push("history");
              return { records: [] };
            }),
        },
      } as unknown as ProviderAdapter.ProviderAdapterV2SessionRuntime;
      const services = Layer.mergeAll(
        IdAllocator.layer,
        Layer.mock(ThreadManagementService.ThreadManagementService)({
          getThreadRecords: () =>
            Effect.succeed({
              thread: {
                id: threadId,
                activeProviderThreadId: providerThreadId,
                deletedAt: null,
                modelSelection: { instanceId, model: "default" },
              },
              providerThreads: [providerThread],
              providerSessions: [],
            } as never),
        }),
        Layer.mock(ProviderSessionManager.ProviderSessionManagerV2)({
          get: () => Effect.succeed(mode === "cold" ? Option.none() : Option.some(runtime)),
          open: (input) =>
            Effect.sync(() => {
              assert.equal(input.initialNativeThreadId, "/saved/session.jsonl");
              assert.equal(input.providerSessionId, providerSessionId);
              calls.push("open");
              return runtime;
            }),
        }),
        Layer.mock(RuntimePolicy.RuntimePolicyV2)({
          resolve: () =>
            Effect.succeed({ cwd: null, runtimeMode: "full-access", interactionMode: "default" }),
        }),
      );
      const result = yield* Effect.gen(function* () {
        const service = yield* BackgroundQuestions.BackgroundQuestions;
        return yield* service.history({ threadId }).pipe(Effect.result);
      }).pipe(Effect.provide(BackgroundQuestions.layer.pipe(Layer.provide(services))));
      if (mode === "wrong-owner" || mode === "unsupported" || mode === "missing-native-id") {
        assert.equal(result._tag, "Failure");
        if (result._tag === "Failure")
          assert.equal(
            result.failure.reason,
            mode === "unsupported" ? "provider-unsupported" : "thread-unavailable",
          );
        assert.deepEqual(calls, []);
      } else {
        assert.equal(result._tag, "Success");
        assert.deepEqual(calls, mode === "live" ? ["history"] : ["open", "resume", "history"]);
      }
    }),
);
