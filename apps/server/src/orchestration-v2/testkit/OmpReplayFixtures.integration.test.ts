import { describe } from "@effect/vitest";
import { OmpOrchestratorReplayHarness } from "../Adapters/OmpAdapterV2.testkit.ts";
import { itRunsReplayFixtures, runReplayFixture } from "./ProviderReplayFixtures.ts";

describe("OMP replay fixtures", () => {
  itRunsReplayFixtures("omp", (input) =>
    runReplayFixture({ ...input, harness: OmpOrchestratorReplayHarness }),
  );
});
