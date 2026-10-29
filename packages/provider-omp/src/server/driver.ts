import { ProviderDriverKind, TextGenerationError, type ServerProvider } from "@t3tools/contracts";
import * as HostProcess from "@t3tools/shared/HostProcess";
import * as Effect from "effect/Effect";
import * as DateTime from "effect/DateTime";
import * as Stream from "effect/Stream";
import * as SubscriptionRef from "effect/SubscriptionRef";
import * as Schema from "effect/Schema";
import * as ChildProcessSpawner from "effect/process/ChildProcessSpawner";

import * as ProviderHost from "@t3tools/provider-core/server/ProviderHost";
import {
  defaultProviderContinuationIdentity,
  type ProviderDriver,
  type ProviderInstance,
} from "@t3tools/provider-core/server/driver";
import { ProviderDriverError } from "@t3tools/provider-core/server/errors";
import { withInstanceIdentity } from "@t3tools/provider-core/server/instanceIdentity";
import { mergeProviderInstanceEnvironment } from "@t3tools/provider-core/server/instanceEnvironment";
import { makeManagedServerProvider } from "@t3tools/provider-core/server/managedProvider";
import { makeManualOnlyProviderMaintenanceCapabilities } from "@t3tools/provider-core/server/maintenanceResolver";
import {
  haveProviderSnapshotSettingsChanged,
  makeProviderSnapshotSettingsSource,
  type ProviderSnapshotSettings,
} from "@t3tools/provider-core/server/snapshotSettings";
import type { ProviderTextGeneration } from "@t3tools/provider-core/server/textGeneration";
import { OmpSettings } from "../settings.ts";
import { makeOmpAdapterV2, type OmpAdapterV2DriverEnv } from "./adapter.ts";
import { buildInitialOmpProviderSnapshot, checkOmpProviderStatus } from "./status.ts";
import type { OmpDiscoveredCommands } from "./discovery.ts";

const DRIVER_KIND = ProviderDriverKind.make("omp");
const decodeSettings = Schema.decodeSync(OmpSettings);
const MAINTENANCE = makeManualOnlyProviderMaintenanceCapabilities({
  provider: DRIVER_KIND,
  packageName: null,
});

const unsupportedGeneration = (operation: string) =>
  Effect.fail(
    new TextGenerationError({
      operation,
      detail: "OMP does not provide application text generation in T3 Code.",
    }),
  );
const textGeneration: ProviderTextGeneration = {
  generateCommitMessage: () => unsupportedGeneration("generateCommitMessage"),
  generatePrContent: () => unsupportedGeneration("generatePrContent"),
  generateBranchName: () => unsupportedGeneration("generateBranchName"),
  generateThreadTitle: () => unsupportedGeneration("generateThreadTitle"),
  generateReviewGuide: () => unsupportedGeneration("generateReviewGuide"),
};

export type OmpDriverEnv =
  | OmpAdapterV2DriverEnv
  | ProviderHost.ProviderHost
  | ChildProcessSpawner.ChildProcessSpawner;

export const OmpDriver: ProviderDriver<OmpSettings, OmpDriverEnv> = {
  driverKind: DRIVER_KIND,
  metadata: { displayName: "OMP", supportsMultipleInstances: true },
  configSchema: OmpSettings,
  defaultConfig: (): OmpSettings => decodeSettings({}),
  create: Effect.fn("OmpDriver.create")(function* ({
    instanceId,
    displayName,
    accentColor,
    environment,
    enabled,
    config,
  }) {
    const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
    const host = yield* ProviderHost.ProviderHost;
    const processEnv = yield* mergeProviderInstanceEnvironment(
      environment,
      yield* HostProcess.Environment,
    );
    const continuationIdentity = defaultProviderContinuationIdentity({
      driverKind: DRIVER_KIND,
      instanceId,
    });
    const stampIdentity = withInstanceIdentity({
      instanceId,
      driverKind: DRIVER_KIND,
      displayName,
      accentColor,
      continuationGroupKey: continuationIdentity.continuationKey,
    });
    const effectiveConfig = { ...config, enabled } satisfies OmpSettings;
    const workspaceCatalogs = yield* SubscriptionRef.make<
      NonNullable<ServerProvider["workspaceSnapshots"]>
    >([]);
    const onWorkspaceCommands = Effect.fn("OmpDriver.onWorkspaceCommands")(function* (
      cwd: string,
      commands: OmpDiscoveredCommands,
    ) {
      const checkedAt = DateTime.formatIso(yield* DateTime.now);
      yield* SubscriptionRef.update(workspaceCatalogs, (catalogs) =>
        [
          ...catalogs.filter((catalog) => catalog.cwd !== cwd),
          { cwd, checkedAt, ...commands },
        ].slice(-32),
      );
    });
    const orchestrationAdapter = yield* makeOmpAdapterV2({
      instanceId,
      settings: effectiveConfig,
      environment: processEnv,
      onWorkspaceCommands,
    }).pipe(
      Effect.mapError(
        (cause) =>
          new ProviderDriverError({
            driver: DRIVER_KIND,
            instanceId,
            detail: "Failed to build the OMP orchestration adapter.",
            cause,
          }),
      ),
    );
    const snapshotSettings = yield* makeProviderSnapshotSettingsSource(effectiveConfig);
    const machineSnapshot = yield* makeManagedServerProvider<ProviderSnapshotSettings<OmpSettings>>(
      {
        resolveMaintenance: () => Effect.succeed(MAINTENANCE),
        getSettings: snapshotSettings.getSettings,
        streamSettings: snapshotSettings.streamSettings,
        haveSettingsChanged: haveProviderSnapshotSettingsChanged,
        initialSnapshot: (settings) =>
          buildInitialOmpProviderSnapshot(settings.provider).pipe(Effect.map(stampIdentity)),
        checkProvider: checkOmpProviderStatus(effectiveConfig, processEnv, host.paths.cwd).pipe(
          Effect.map(stampIdentity),
          Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, spawner),
        ),
      },
    ).pipe(
      Effect.mapError(
        (cause) =>
          new ProviderDriverError({
            driver: DRIVER_KIND,
            instanceId,
            detail: "Failed to build the OMP provider snapshot.",
            cause,
          }),
      ),
    );
    const withWorkspaceCatalogs = (provider: ServerProvider) =>
      SubscriptionRef.get(workspaceCatalogs).pipe(
        Effect.map((workspaceSnapshots) => ({
          ...provider,
          slashCommands: workspaceSnapshots.at(-1)?.slashCommands ?? provider.slashCommands,
          skills: workspaceSnapshots.at(-1)?.skills ?? provider.skills,
          workspaceSnapshots,
        })),
      );
    const snapshot = {
      ...machineSnapshot,
      getSnapshot: machineSnapshot.getSnapshot.pipe(Effect.flatMap(withWorkspaceCatalogs)),
      refresh: machineSnapshot.refresh.pipe(Effect.flatMap(withWorkspaceCatalogs)),
      streamChanges: machineSnapshot.streamChanges.pipe(
        Stream.mapEffect(withWorkspaceCatalogs),
        Stream.merge(
          SubscriptionRef.changes(workspaceCatalogs).pipe(
            Stream.mapEffect(() =>
              machineSnapshot.getSnapshot.pipe(Effect.flatMap(withWorkspaceCatalogs)),
            ),
          ),
        ),
      ),
    };

    return {
      instanceId,
      driverKind: DRIVER_KIND,
      continuationIdentity,
      displayName,
      accentColor,
      enabled,
      snapshot,
      snapshotForCwd: (cwd) =>
        snapshot.getSnapshot.pipe(
          Effect.map((provider) => {
            const catalog = enabled
              ? provider.workspaceSnapshots.find((workspace) => workspace.cwd === cwd)
              : undefined;
            return {
              ...provider,
              slashCommands: catalog?.slashCommands ?? [],
              skills: catalog?.skills ?? [],
              slashCommandsPending: enabled && catalog === undefined,
            };
          }),
        ),
      orchestrationAdapter,
      textGeneration,
    } satisfies ProviderInstance;
  }),
};
