import { assert, describe, it } from "@effect/vitest";
import * as Result from "effect/Result";

import { parseOmpDiscoveredCommands, parseOmpDiscoveredModels } from "./discovery.ts";

describe("OMP discovery", () => {
  it("maps native thinking metadata without advertising Pi's assumed effort ladder", () => {
    const models = Result.getOrThrow(
      parseOmpDiscoveredModels(
        {
          models: [
            {
              provider: "openai",
              id: "gpt-5.4",
              name: "GPT 5.4",
              reasoning: true,
              thinking: { efforts: ["low", "high"], requiresEffort: true },
            },
            { provider: "openai", id: "gpt-5.4" },
            { provider: "local", id: "plain", reasoning: false },
          ],
        },
        { model: { provider: "openai", id: "gpt-5.4" }, thinkingLevel: "high" },
      ),
    );
    assert.deepEqual(
      models.map((model) => model.slug),
      ["openai/gpt-5.4", "local/plain"],
    );
    assert.equal(models[0]?.subProvider, "openai");
    assert.deepEqual(models[0]?.capabilities?.optionDescriptors, [
      {
        id: "thinking",
        label: "Thinking",
        type: "select",
        options: [
          { id: "low", label: "Low" },
          { id: "high", label: "High", isDefault: true },
        ],
      },
    ]);
    assert.deepEqual(models[1]?.capabilities?.optionDescriptors, []);
  });

  it.each([
    {},
    { models: [{ provider: "openai" }] },
    { models: [{ provider: "openai", id: "valid" }, { provider: "broken" }] },
    { models: [{ provider: " ", id: "invalid" }] },
  ])("rejects malformed inventories instead of treating them as signed out: %j", (inventory) => {
    assert.isTrue(Result.isFailure(parseOmpDiscoveredModels(inventory, {})));
  });

  it("does not invent thinking levels when a reasoning model reports no controllable effort", () => {
    const models = Result.getOrThrow(
      parseOmpDiscoveredModels(
        { models: [{ provider: "custom", id: "reasoner", reasoning: true }] },
        {},
      ),
    );
    assert.deepEqual(models[0]?.capabilities?.optionDescriptors, []);
  });

  it("preserves command hints and discovers native skill commands", () => {
    const commands = Result.getOrThrow(
      parseOmpDiscoveredCommands({
        commands: [
          {
            name: "compact",
            source: "builtin",
            description: "Compact context",
            input: { hint: "instructions" },
          },
          {
            name: "skill:review",
            source: "skill",
            description: "Review code",
            input: { hint: "arguments" },
          },
          { name: "deploy", source: "custom" },
        ],
      }),
    );
    assert.deepEqual(commands.slashCommands, [
      { name: "compact", description: "Compact context", input: { hint: "instructions" } },
      { name: "deploy" },
    ]);
    assert.deepEqual(commands.skills, [
      { name: "review", path: "omp:skill:review", enabled: true, description: "Review code" },
    ]);
  });

  it("distinguishes a malformed command response from an empty catalog", () => {
    assert.isTrue(Result.isFailure(parseOmpDiscoveredCommands({})));
    assert.deepEqual(Result.getOrThrow(parseOmpDiscoveredCommands({ commands: [] })), {
      slashCommands: [],
      skills: [],
    });
  });
});
