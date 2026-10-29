import type {
  ModelCapabilities,
  ServerProviderModel,
  ServerProviderSkill,
  ServerProviderSlashCommand,
} from "@t3tools/contracts";
import { createModelCapabilities } from "@t3tools/shared/model";
import * as Result from "effect/Result";

import {
  OmpRpcError,
  ompRecordField as recordField,
  ompRecordString as recordString,
} from "./rpc.ts";

const THINKING_LEVELS = ["off", "minimal", "low", "medium", "high", "xhigh", "max"] as const;
const THINKING_LABELS = {
  off: "Off",
  minimal: "Minimal",
  low: "Low",
  medium: "Medium",
  high: "High",
  xhigh: "Extra High",
  max: "Max",
} as const;

export const EMPTY_OMP_MODEL_CAPABILITIES: ModelCapabilities = createModelCapabilities({
  optionDescriptors: [],
});

function thinkingCapabilitiesForOmpModel(model: unknown, defaultLevel: unknown): ModelCapabilities {
  const thinking = recordField(model, "thinking");
  const catalogEfforts = Array.isArray(thinking);
  const efforts = catalogEfforts ? thinking : recordField(thinking, "efforts");
  if (
    recordField(model, "reasoning") !== true ||
    recordField(thinking, "unverified") === true ||
    !Array.isArray(efforts) ||
    efforts.length === 0
  ) {
    return EMPTY_OMP_MODEL_CAPABILITIES;
  }
  const levels = THINKING_LEVELS.filter((level) =>
    level === "off"
      ? !catalogEfforts && recordField(thinking, "requiresEffort") !== true
      : efforts.includes(level),
  );
  if (levels.length === 0) return EMPTY_OMP_MODEL_CAPABILITIES;
  return createModelCapabilities({
    optionDescriptors: [
      {
        id: "thinking",
        label: "Thinking",
        type: "select",
        options: levels.map((level) => ({
          id: level,
          label: THINKING_LABELS[level],
          ...(level === defaultLevel ? { isDefault: true } : {}),
        })),
      },
    ],
  });
}

export function parseOmpDiscoveredModels(
  data: unknown,
  state: unknown,
): Result.Result<ReadonlyArray<ServerProviderModel>, OmpRpcError> {
  const models = recordField(data, "models");
  if (!Array.isArray(models)) {
    return Result.fail(
      new OmpRpcError({ operation: "get_available_models", detail: "Invalid model inventory" }),
    );
  }
  const activeModel = recordField(state, "model");
  const seen = new Set<string>();
  const parsed: Array<ServerProviderModel> = [];
  for (const model of models) {
    const provider = recordString(model, "provider");
    const id = recordString(model, "id");
    if (!provider?.trim() || !id?.trim()) {
      return Result.fail(
        new OmpRpcError({ operation: "get_available_models", detail: "Invalid model identity" }),
      );
    }
    const slug = `${provider}/${id}`;
    if (seen.has(slug)) continue;
    seen.add(slug);
    parsed.push({
      slug,
      name: recordString(model, "name") ?? slug,
      subProvider: provider,
      isCustom: false,
      capabilities: thinkingCapabilitiesForOmpModel(
        model,
        provider === recordString(activeModel, "provider") && id === recordString(activeModel, "id")
          ? recordString(state, "thinkingLevel")
          : undefined,
      ),
    });
  }
  return Result.succeed(parsed);
}

export interface OmpDiscoveredCommands {
  readonly slashCommands: ReadonlyArray<ServerProviderSlashCommand>;
  readonly skills: ReadonlyArray<ServerProviderSkill>;
}

export function parseOmpDiscoveredCommands(
  data: unknown,
): Result.Result<OmpDiscoveredCommands, OmpRpcError> {
  const commands = recordField(data, "commands");
  if (!Array.isArray(commands)) {
    return Result.fail(
      new OmpRpcError({ operation: "get_available_commands", detail: "Invalid command inventory" }),
    );
  }
  const slashCommands: Array<ServerProviderSlashCommand> = [];
  const skills: Array<ServerProviderSkill> = [];
  for (const command of commands) {
    const name = recordString(command, "name");
    if (!name?.trim()) {
      return Result.fail(
        new OmpRpcError({
          operation: "get_available_commands",
          detail: "Invalid command identity",
        }),
      );
    }
    const description = recordString(command, "description");
    if (recordString(command, "source") === "skill") {
      const skillName = name.startsWith("skill:") ? name.slice("skill:".length) : name;
      if (skillName.length === 0) {
        return Result.fail(
          new OmpRpcError({
            operation: "get_available_commands",
            detail: "Invalid skill identity",
          }),
        );
      }
      skills.push({
        name: skillName,
        path: `omp:skill:${skillName}`,
        enabled: true,
        ...(description === undefined ? {} : { description }),
      });
    } else {
      const hint = recordString(recordField(command, "input"), "hint");
      slashCommands.push({
        name,
        ...(description === undefined ? {} : { description }),
        ...(hint === undefined ? {} : { input: { hint } }),
      });
    }
  }
  return Result.succeed({ slashCommands, skills });
}
