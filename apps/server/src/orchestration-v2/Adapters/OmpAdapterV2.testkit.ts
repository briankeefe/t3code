import * as NodeServices from "@effect/platform-node/NodeServices";
import {
  ProviderDriverKind,
  ProviderInstanceId,
  type ProviderInstanceEnvironment,
  ProviderReplayTranscript,
} from "@t3tools/contracts";
import { OmpAdapterV2Driver } from "@t3tools/provider-omp/server";
import * as TestProviderHost from "@t3tools/provider-testing/TestProviderHost";
import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import type { ProviderAdapterDriverCreateError } from "@t3tools/provider-core/server/adapterDriver";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as ChildProcessSpawner from "effect/process/ChildProcessSpawner";
import * as FetchHttpClient from "effect/http/FetchHttpClient";
import * as ProviderAdapterRegistry from "../ProviderAdapterRegistry.ts";
import type { OrchestratorV2ProviderReplayHarness } from "../testkit/ProviderReplayHarness.ts";
import { makeRpcReplaySpawnerLayer } from "./PiAdapterV2.testkit.ts";

export const OMP_RPC_REPLAY_PROTOCOL = "omp.rpc-jsonl";
const OMP = ProviderDriverKind.make("omp");
const OmpTranscript = Schema.Struct({
  ...ProviderReplayTranscript.fields,
  provider: Schema.Literal(OMP),
  protocol: Schema.Literal(OMP_RPC_REPLAY_PROTOCOL),
});

export function layer<E, R>(input: {
  readonly spawner: Layer.Layer<ChildProcessSpawner.ChildProcessSpawner, E, R>;
  readonly binaryPath: string;
  readonly launchArgs: string;
  readonly environment?: ProviderInstanceEnvironment;
}) {
  return ProviderAdapterRegistry.layerFromAdaptersEffect(
    OmpAdapterV2Driver.create({
      instanceId: ProviderInstanceId.make("omp"),
      environment: input.environment ?? [],
      displayName: undefined,
      enabled: true,
      config: {
        ...OmpAdapterV2Driver.defaultConfig(),
        binaryPath: input.binaryPath,
        launchArgs: input.launchArgs,
      },
    }).pipe(Effect.map((adapter) => [adapter])),
  ).pipe(
    Layer.provide(input.spawner),
    Layer.provide(
      Layer.mergeAll(
        TestProviderHost.layer().pipe(Layer.provide(NodeServices.layer)),
        NodeServices.layer,
        IdAllocator.layer,
        FetchHttpClient.layer,
      ),
    ),
  );
}

export const OmpOrchestratorReplayHarness: OrchestratorV2ProviderReplayHarness<
  typeof OmpTranscript.Type,
  Schema.SchemaError | ProviderAdapterDriverCreateError
> = {
  driver: OMP,
  decodeTranscript: Schema.decodeUnknownEffect(OmpTranscript),
  makeProviderAdapterRegistryLayer: (transcript: ProviderReplayTranscript) =>
    layer({
      binaryPath: "omp",
      launchArgs:
        typeof transcript.metadata?.launchArgs === "string" ? transcript.metadata.launchArgs : "",
      spawner: makeRpcReplaySpawnerLayer(transcript),
    }),
};
