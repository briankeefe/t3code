import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, describe, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Sink from "effect/Sink";
import * as Stream from "effect/Stream";
import * as ChildProcessSpawner from "effect/process/ChildProcessSpawner";

import { checkOmpProviderStatus } from "./status.ts";

const settings = { enabled: true, binaryPath: "omp", launchArgs: "", customModels: [] } as const;

function catalogSpawner(stdout: string, code = 0) {
  return ChildProcessSpawner.make(() =>
    Effect.succeed(
      ChildProcessSpawner.makeHandle({
        pid: ChildProcessSpawner.ProcessId(900_000_002),
        exitCode: Effect.succeed(ChildProcessSpawner.ExitCode(code)),
        isRunning: Effect.succeed(false),
        kill: () => Effect.void,
        unref: Effect.succeed(Effect.void),
        stdin: Sink.drain,
        stdout: Stream.succeed(new TextEncoder().encode(stdout)),
        stderr: Stream.empty,
        all: Stream.empty,
        getInputFd: () => Sink.drain,
        getOutputFd: () => Stream.empty,
      }),
    ),
  );
}

describe("OMP provider status", () => {
  it.effect("discovers native catalog thinking arrays without using Pi's version threshold", () =>
    Effect.gen(function* () {
      const snapshot = yield* checkOmpProviderStatus(settings).pipe(
        Effect.provideService(
          ChildProcessSpawner.ChildProcessSpawner,
          catalogSpawner(
            JSON.stringify({
              models: [
                {
                  provider: "openai-codex",
                  id: "gpt-5.4",
                  name: "GPT 5.4",
                  reasoning: true,
                  thinking: ["low", "high"],
                },
              ],
            }),
          ),
        ),
      );
      assert.equal(snapshot.status, "ready");
      assert.equal(snapshot.auth.status, "authenticated");
      assert.deepEqual(
        snapshot.models.map((model) => model.slug),
        ["default", "openai-codex/gpt-5.4"],
      );
      assert.deepEqual(snapshot.models[1]?.capabilities?.optionDescriptors, [
        {
          id: "thinking",
          label: "Thinking",
          type: "select",
          options: [
            { id: "low", label: "Low" },
            { id: "high", label: "High" },
          ],
        },
      ]);
    }).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect.each([
    { label: "invalid JSON", stdout: "not JSON", code: 0 },
    {
      label: "invalid model identity",
      stdout: JSON.stringify({ models: [{ provider: "openai" }] }),
      code: 0,
    },
    { label: "failed catalog process", stdout: JSON.stringify({ models: [] }), code: 1 },
  ])("does not present failed catalog discovery as ready: $label", ({ stdout, code }) =>
    Effect.gen(function* () {
      const snapshot = yield* checkOmpProviderStatus(settings).pipe(
        Effect.provideService(
          ChildProcessSpawner.ChildProcessSpawner,
          catalogSpawner(stdout, code),
        ),
      );
      assert.equal(snapshot.status, "error");
      assert.equal(snapshot.auth.status, "unknown");
      assert.deepEqual(
        snapshot.models.map((model) => model.slug),
        ["default"],
      );
    }).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect("reports signed out only for a valid empty inventory", () =>
    Effect.gen(function* () {
      const snapshot = yield* checkOmpProviderStatus(settings).pipe(
        Effect.provideService(
          ChildProcessSpawner.ChildProcessSpawner,
          catalogSpawner('{"models":[]}'),
        ),
      );
      assert.equal(snapshot.status, "warning");
      assert.equal(snapshot.auth.status, "unauthenticated");
    }).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect.each([
    { launchArgs: "--api-key private-secret", message: "environment variable" },
    { launchArgs: "--models openai/*", message: "same model inventory" },
    { launchArgs: "--profile other-account", message: "controlled by T3 Code" },
    { launchArgs: "--config alternate.yml", message: "controlled by T3 Code" },
  ])("does not silently ignore catalog configuration: $launchArgs", ({ launchArgs, message }) =>
    Effect.gen(function* () {
      const snapshot = yield* checkOmpProviderStatus({ ...settings, launchArgs }).pipe(
        Effect.provideService(
          ChildProcessSpawner.ChildProcessSpawner,
          catalogSpawner('{"models":[]}'),
        ),
      );
      assert.equal(snapshot.status, "error");
      assert.equal(snapshot.auth.status, "unknown");
      assert.include(snapshot.message ?? "", message);
      assert.notInclude(snapshot.message ?? "", "private-secret");
    }).pipe(Effect.provide(NodeServices.layer)),
  );
});
