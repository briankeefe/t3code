import type { ServerProviderModel } from "@t3tools/contracts";
import { resolveSpawnCommand } from "@t3tools/shared/shell";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Result from "effect/Result";
import * as Schema from "effect/Schema";
import * as ChildProcess from "effect/process/ChildProcess";
import type * as ChildProcessSpawner from "effect/process/ChildProcessSpawner";

import {
  buildServerProvider,
  isCommandMissingCause,
  providerModelsFromSettings,
  spawnAndCollect,
  type ServerProviderDraft,
} from "@t3tools/provider-core/server/snapshotProbe";
import type { OmpSettings } from "../settings.ts";
import { EMPTY_OMP_MODEL_CAPABILITIES, parseOmpDiscoveredModels } from "./discovery.ts";
import { resolveOmpLaunchArgs } from "./mcpInjection.ts";
import { OmpRpcError } from "./rpc.ts";

const OMP_PRESENTATION = {
  displayName: "OMP",
  showInteractionModeToggle: false,
  supportedRuntimeModes: ["approval-required", "auto-accept-edits", "full-access"],
  reportsContextWindow: true,
} as const;

const DISCOVERY_TIMEOUT_MS = 30_000;
const decodeInventory = Schema.decodeUnknownEffect(Schema.fromJsonString(Schema.Unknown));
const DEFAULT_MODEL: ServerProviderModel = {
  slug: "default",
  name: "OMP default",
  isCustom: false,
  capabilities: EMPTY_OMP_MODEL_CAPABILITIES,
};

function buildOmpProvider(input: Parameters<typeof buildServerProvider>[0]): ServerProviderDraft {
  return { ...buildServerProvider(input), supportsTextGeneration: false };
}

function modelsFromSettings(
  settings: OmpSettings,
  discovered: ReadonlyArray<ServerProviderModel> = [],
) {
  return providerModelsFromSettings(
    [DEFAULT_MODEL, ...discovered],
    settings.customModels,
    EMPTY_OMP_MODEL_CAPABILITIES,
  );
}

const discoverOmpModels = Effect.fn("OmpProviderStatus.discoverModels")(function* (
  settings: OmpSettings,
  environment: NodeJS.ProcessEnv,
  cwd: string | undefined,
) {
  const launch = resolveOmpLaunchArgs(settings.launchArgs);
  if (!launch.ok) return yield* new OmpRpcError({ operation: "catalog", detail: launch.message });
  if (launch.args.includes("--api-key")) {
    return yield* new OmpRpcError({
      operation: "catalog",
      detail:
        "Model discovery cannot use --api-key. Configure the model provider's API-key environment variable for this instance instead.",
    });
  }
  if (launch.args.includes("--models")) {
    return yield* new OmpRpcError({
      operation: "catalog",
      detail:
        "Model discovery cannot apply the --models launch filter. Remove it so catalog discovery and the running session use the same model inventory.",
    });
  }
  const args = ["models", "--json", "--no-extensions"];
  const binary = settings.binaryPath || "omp";
  const command = yield* resolveSpawnCommand(binary, args, { env: environment });
  // The catalog command does not create an agent session, run hooks, or start MCP servers.
  const output = yield* spawnAndCollect(
    binary,
    ChildProcess.make(command.command, command.args, {
      env: environment,
      cwd,
      shell: command.shell,
    }),
  );
  if (output.code !== 0) {
    return yield* new OmpRpcError({
      operation: "catalog",
      detail: `Model catalog exited with code ${output.code}`,
    });
  }
  const inventory = yield* decodeInventory(output.stdout).pipe(
    Effect.mapError(
      (cause) =>
        new OmpRpcError({ operation: "catalog", detail: "Invalid JSON model inventory", cause }),
    ),
  );
  return yield* Effect.fromResult(parseOmpDiscoveredModels(inventory, {}));
});

export const buildInitialOmpProviderSnapshot = Effect.fn("OmpProviderStatus.buildInitialSnapshot")(
  function* (settings: OmpSettings): Effect.fn.Return<ServerProviderDraft> {
    return buildOmpProvider({
      presentation: OMP_PRESENTATION,
      enabled: settings.enabled,
      checkedAt: DateTime.formatIso(yield* DateTime.now),
      models: modelsFromSettings(settings),
      probe: {
        installed: settings.enabled,
        version: null,
        status: "warning",
        auth: { status: "unknown" },
        message: settings.enabled
          ? "Checking OMP model catalog..."
          : "OMP is disabled in T3 Code settings.",
      },
    });
  },
);

export const checkOmpProviderStatus = Effect.fn("OmpProviderStatus.check")(function* (
  settings: OmpSettings,
  environment: NodeJS.ProcessEnv = process.env,
  cwd?: string,
): Effect.fn.Return<ServerProviderDraft, never, ChildProcessSpawner.ChildProcessSpawner> {
  if (!settings.enabled) return yield* buildInitialOmpProviderSnapshot(settings);
  const checkedAt = DateTime.formatIso(yield* DateTime.now);
  const discovery = yield* discoverOmpModels(settings, environment, cwd).pipe(
    Effect.timeoutOption(DISCOVERY_TIMEOUT_MS),
    Effect.result,
  );
  if (Result.isFailure(discovery) || Option.isNone(discovery.success)) {
    const error = Result.isFailure(discovery) ? discovery.failure : undefined;
    const missing = isCommandMissingCause(error);
    return buildOmpProvider({
      presentation: OMP_PRESENTATION,
      enabled: true,
      checkedAt,
      models: modelsFromSettings(settings),
      probe: {
        installed: !missing,
        version: null,
        status: "error",
        auth: { status: "unknown" },
        message: missing
          ? "OMP (`omp`) is not installed or not on PATH. Install Oh My Pi on the server or set its binary path."
          : error
            ? `OMP model discovery failed: ${error.message}`
            : "OMP model discovery timed out. Check the executable and model provider configuration on the server.",
      },
    });
  }
  const models = discovery.success.value;
  const authenticated = models.length > 0;
  return buildOmpProvider({
    presentation: OMP_PRESENTATION,
    enabled: true,
    checkedAt,
    models: modelsFromSettings(settings, models),
    probe: {
      installed: true,
      version: null,
      status: authenticated ? "ready" : "warning",
      auth: { status: authenticated ? "authenticated" : "unauthenticated", type: "omp" },
      ...(authenticated
        ? {}
        : {
            message:
              "OMP reports no usable models. Run `omp` on the server and use /login, or configure a model provider API key.",
          }),
    },
  });
});
