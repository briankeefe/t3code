import * as NodeServices from "@effect/platform-node/NodeServices";
import {
  ProviderDriverKind,
  type ProviderReplayEntry,
  type ProviderReplayTranscript,
} from "@t3tools/contracts";
import * as Console from "effect/Console";
import * as DateTime from "effect/DateTime";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Path from "effect/Path";
import * as Predicate from "effect/Predicate";
import { OmpFrameDecoder, MAX_OMP_FRAME_BYTES } from "@t3tools/provider-omp/testing";
import { TestClock } from "effect/testing";
import * as Schema from "effect/Schema";
import { ChildProcess, ChildProcessSpawner } from "effect/process";
import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import {
  makeCheckpointWorkspace,
  checkpointWorkspace,
} from "@t3tools/provider-testing/replayWorkspace";
import {
  makePiRecordingSpawner,
  PI_REPLAY_ANY,
} from "../src/orchestration-v2/Adapters/PiAdapterV2.testkit.ts";
import * as OmpTestkit from "../src/orchestration-v2/Adapters/OmpAdapterV2.testkit.ts";
import { ORCHESTRATOR_REPLAY_FIXTURES } from "../src/orchestration-v2/testkit/fixtures/index.ts";
import { materializeFixtureInput } from "../src/orchestration-v2/testkit/fixtures/shared.ts";
import { provideDeterministicTestRuntime } from "../src/orchestration-v2/testkit/DeterministicRuntime.ts";
import { runOrchestratorV2ProviderReplayScenario } from "../src/orchestration-v2/testkit/ProviderReplayHarness.ts";

const OMP = ProviderDriverKind.make("omp");
const scenario = process.argv[process.argv.indexOf("--scenario") + 1];
const fixture = ORCHESTRATOR_REPLAY_FIXTURES.find((item) => item.name === scenario);
const variant = fixture?.providers.find((item) => item.driver === OMP);
if (fixture === undefined || variant === undefined)
  throw new Error("Pass --scenario with a registered OMP fixture.");
const binary = process.env.T3_OMP_BIN ?? "omp";
const launchArgs = "--no-context-files --no-extensions --no-skills --no-rules";
const encodeJson = Schema.encodeSync(Schema.fromJsonString(Schema.Unknown));
const decodeRecord = Schema.decodeUnknownSync(Schema.Record(Schema.String, Schema.Unknown));

function normalizeEntries(
  entries: ReadonlyArray<ProviderReplayEntry>,
  workspace: string,
): ProviderReplayEntry[] {
  const ids = new Map<string, string>();
  const sessionFiles = new Map<string, string>();
  let timeAnchor: number | undefined;
  const epoch = DateTime.toEpochMillis(DateTime.makeUnsafe("2026-01-01T00:00:00.000Z"));
  const shift = (time: number) => {
    timeAnchor ??= time;
    return epoch + time - timeAnchor;
  };
  const text = (value: string): string =>
    value
      .replace(/[^\s"']+\.jsonl/gu, (file) => {
        if (!file.startsWith("/")) return file;
        let normalized = sessionFiles.get(file);
        if (normalized === undefined) {
          normalized = `/omp-sessions/session-${sessionFiles.size + 1}.jsonl`;
          sessionFiles.set(file, normalized);
        }
        return normalized;
      })
      .replaceAll(workspace, "<workspace>")
      .replaceAll(process.env.HOME || "\0", "~")
      .replace(/\b\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?Z\b/gu, (value) =>
        DateTime.formatIso(
          DateTime.makeUnsafe(shift(DateTime.toEpochMillis(DateTime.makeUnsafe(value)))),
        ),
      )
      .replace(/[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}/giu, (id) => {
        let normalized = ids.get(id);
        if (normalized === undefined) {
          normalized = `00000000-0000-4000-8000-${String(ids.size + 1).padStart(12, "0")}`;
          ids.set(id, normalized);
        }
        return normalized;
      });
  const normalize = (value: unknown): unknown => {
    if (typeof value === "string") return text(value);
    if (Array.isArray(value)) return value.map(normalize);
    if (!Predicate.isObject(value)) return value;
    return Object.fromEntries(
      Object.entries(value).map(([key, item]) => [
        key,
        key === "systemPrompt" || key === "dumpTools"
          ? []
          : key === "accountAccess"
            ? {}
            : ["thinkingSignature", "textSignature", "apiKey", "authorization"].includes(key)
              ? "<redacted>"
              : key === "timestamp" && typeof item === "number"
                ? shift(item)
                : normalize(item),
      ]),
    );
  };
  const decoders = new Map<string, OmpFrameDecoder>();
  const output: ProviderReplayEntry[] = [];
  for (const entry of entries) {
    if (entry.type === "runtime_exit") {
      output.push(entry);
      continue;
    }
    let frame = entry.frame;
    if (entry.type === "emit_inbound") {
      const process = /@p\d+$/u.exec(entry.label ?? "")?.[0] ?? "@p1";
      let decoder = decoders.get(process);
      if (decoder === undefined) {
        decoder = new OmpFrameDecoder();
        decoders.set(process, decoder);
      }
      frame = decoder.push(decodeRecord(frame));
      if (frame === undefined) continue;
    }
    if (Predicate.isObject(frame) && frame.type === "process_start" && Array.isArray(frame.args)) {
      const args = frame.args;
      output.push({
        ...entry,
        frame: {
          ...frame,
          args: args.map((arg, index) =>
            ["--extension", "-e", "--session-dir"].includes(String(args[index - 1]))
              ? PI_REPLAY_ANY
              : normalize(arg),
          ),
        },
      });
      continue;
    }
    const normalized = normalize(frame);
    const bytes = Buffer.from(encodeJson(normalized));
    if (entry.type !== "emit_inbound" || bytes.length < MAX_OMP_FRAME_BYTES) {
      output.push({ ...entry, frame: normalized });
      continue;
    }
    const chunkBytes = 256 * 1024;
    const count = Math.ceil(bytes.length / chunkBytes);
    for (let index = 0; index < count; index++) {
      output.push({
        ...entry,
        frame: {
          type: "rpc_chunk",
          chunkId: `recorded-${output.length - index}`,
          index,
          count,
          byteLength: bytes.length,
          data: bytes.subarray(index * chunkBytes, (index + 1) * chunkBytes).toString("base64"),
        },
      });
    }
  }
  for (const decoder of decoders.values()) decoder.finish();
  return output;
}

const record = Effect.gen(function* () {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
  const version = (yield* spawner.string(ChildProcess.make(binary, ["--version"]))).trim();
  const input = {
    ...fixture.buildInput(),
    ...(variant.runtimeMode === undefined ? {} : { runtimeMode: variant.runtimeMode }),
  };
  const workspace = yield* Effect.promise(() =>
    makeCheckpointWorkspace(`omp-record-${fixture.name}`, input.workspaceFiles),
  );
  yield* Effect.addFinalizer(() =>
    fs.remove(workspace, { recursive: true, force: true }).pipe(Effect.ignore),
  );
  const entries: ProviderReplayEntry[] = [];
  const transcript: ProviderReplayTranscript = {
    provider: OMP,
    protocol: OmpTestkit.OMP_RPC_REPLAY_PROTOCOL,
    scenario: fixture.name,
    version,
    entries: [],
  };
  const materialize = materializeFixtureInput({
    scenario: fixture.name,
    fixtureInput: input,
    driver: OMP,
    modelSelection: variant.modelSelection,
  }).pipe(Effect.provide(IdAllocator.layer));
  const layerRecording = Layer.effect(
    ChildProcessSpawner.ChildProcessSpawner,
    Effect.map(Effect.service(ChildProcessSpawner.ChildProcessSpawner), (live) =>
      makePiRecordingSpawner(live, entries),
    ),
  ).pipe(Layer.provide(NodeServices.layer));
  yield* Effect.gen(function* () {
    yield* Effect.gen(function* () {
      while (true) {
        yield* TestClock.withLive(Effect.sleep(Duration.millis(20)));
        yield* TestClock.adjust(Duration.millis(20));
      }
    }).pipe(Effect.forkScoped);
    const materialized = yield* materialize;
    yield* runOrchestratorV2ProviderReplayScenario(
      {
        name: `${fixture.name}/omp:record`,
        transcript,
        commands: materialized.commands,
        steps: materialized.steps,
        projectionThreadIds: materialized.projectionThreadIds,
        runtimePolicyOverride: { ...variant.runtimePolicyOverride, cwd: workspace },
      },
      {
        driver: OMP,
        decodeTranscript: Effect.succeed,
        makeProviderAdapterRegistryLayer: () =>
          OmpTestkit.layer({ binaryPath: binary, launchArgs, spawner: layerRecording }),
      },
    );
  }).pipe(Effect.scoped, provideDeterministicTestRuntime);
  const normalized: ProviderReplayTranscript = {
    ...transcript,
    metadata: { generatedBy: "live-omp-recorder", model: variant.modelSelection.model, launchArgs },
    entries: normalizeEntries(entries, workspace),
  };
  const { entries: recorded, ...header } = normalized;
  const encoded = [
    encodeJson({ type: "transcript_start", ...header }),
    ...recorded.map((entry) => encodeJson(entry)),
    "",
  ].join("\n");
  const captures = path.join(process.cwd(), ".t3", "omp-replay-captures");
  yield* fs.makeDirectory(captures, { recursive: true });
  yield* fs.writeFileString(path.join(captures, `${fixture.name}.ndjson`), encoded);
  yield* Effect.gen(function* () {
    const replayWorkspace = yield* checkpointWorkspace(fixture.name, input.workspaceFiles);
    const materialized = yield* materialize;
    const replay = yield* runOrchestratorV2ProviderReplayScenario(
      {
        name: `${fixture.name}/omp:verify`,
        transcript: yield* OmpTestkit.OmpOrchestratorReplayHarness.decodeTranscript(normalized),
        commands: materialized.commands,
        steps: materialized.steps,
        projectionThreadIds: materialized.projectionThreadIds,
        runtimePolicyOverride: { ...variant.runtimePolicyOverride, cwd: replayWorkspace },
      },
      OmpTestkit.OmpOrchestratorReplayHarness,
    );
    variant.assertOutput(replay, normalized);
  }).pipe(Effect.scoped, provideDeterministicTestRuntime);
  const output = yield* path.fromFileUrl(variant.transcriptFile);
  yield* fs.writeFileString(output, encoded);
  yield* Console.log(`Wrote ${recorded.length} native OMP replay entries to ${output}`);
});
await Effect.runPromise(record.pipe(Effect.scoped, Effect.provide(NodeServices.layer)));
