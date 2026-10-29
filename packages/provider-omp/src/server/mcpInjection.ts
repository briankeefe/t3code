import { tokenizeCliArgs } from "@t3tools/shared/cliArgs";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import type { McpProviderSessionConfig } from "@t3tools/provider-core/server/mcpSession";
import {
  T3_CODE_ORCHESTRATION_INSTRUCTIONS,
  T3_CODE_BROWSER_TOOL_INSTRUCTIONS,
  T3_CODE_DEVICE_TOOL_INSTRUCTIONS,
} from "@t3tools/provider-core/server/orchestrationInstructions";

export const OMP_PERMISSION_GUARD_COMMAND = "t3-permission-guard";
const T3_OMP_RUNTIME_MODE_ENV = "T3_OMP_RUNTIME_MODE";
export type OmpRuntimeMode = "approval-required" | "auto-accept-edits" | "auto" | "full-access";

const OWNED: ReadonlyArray<string> = [
  "--mode",
  "--continue",
  "-c",
  "--resume",
  "-r",
  "--restore",
  "--session",
  "--session-dir",
  "--session-id",
  "--fork",
  "--no-session",
  "--print",
  "-p",
  "--export",
  "--help",
  "-h",
  "--version",
  "-v",
  "--list-models",
  "--approval-mode",
  "--auto-approve",
  "--yolo",
  "--plan-yolo",
  "--no-ui",
  "--config",
  "--profile",
  "--alias",
  "--cwd",
  "--from-claude",
  "--from-codex",
];
const VALUES: ReadonlyArray<string> = [
  "--api-key",
  "--provider",
  "--model",
  "--models",
  "--thinking",
  "--system-prompt",
  "--append-system-prompt",
  "--extension",
  "-e",
  "--tools",
  "--skill",
  "--theme",
  "--name",
  "--max-time",
  "--max-cost",
  "--add-dir",
];
const FLAGS: ReadonlyArray<string> = [
  "--no-tools",
  "--no-extensions",
  "--no-skills",
  "--no-rules",
  "--no-context-files",
  "--no-title",
  "--no-lsp",
  "--no-pty",
  "--hide-thinking",
  "--print-thoughts",
  "--external-thinking",
  "--prewalk",
  "--no-prewalk",
  "--wait-async",
];
export type OmpLaunchArgsResolution =
  | { readonly ok: true; readonly args: ReadonlyArray<string> }
  | { readonly ok: false; readonly message: string };

export function resolveOmpLaunchArgs(input: string): OmpLaunchArgsResolution {
  const tokens = tokenizeCliArgs(input);
  const args: string[] = [];
  for (let index = 0; index < tokens.length; index++) {
    const token = tokens[index]!;
    const equals = token.indexOf("=");
    const flag = equals > 0 ? token.slice(0, equals) : token;
    if (OWNED.includes(flag))
      return { ok: false, message: `OMP launch argument '${flag}' is controlled by T3 Code.` };
    if (VALUES.includes(flag)) {
      const value = equals > 0 ? token.slice(equals + 1) : tokens[++index];
      if (value === undefined || (equals < 0 && value.startsWith("--"))) {
        return { ok: false, message: `OMP launch argument '${flag}' requires a value.` };
      }
      args.push(flag, value);
    } else if (FLAGS.includes(flag) && equals < 0) {
      args.push(flag);
    } else {
      return {
        ok: false,
        message: token.startsWith("-")
          ? `OMP launch argument '${flag}' is not supported by T3 Code.`
          : "OMP launch arguments cannot contain positional prompts or file attachments.",
      };
    }
  }
  return { ok: true, args };
}

// OMP rebinds extension factories into native subagents; no UI means no approval.
export const OMP_PERMISSION_EXTENSION_SOURCE = `import type { ExtensionAPI } from "@oh-my-pi/pi-coding-agent";
export default function t3Permissions(omp: ExtensionAPI) {
  const mode = process.env.${T3_OMP_RUNTIME_MODE_ENV};
  const readOnly: Record<string, true> = { read: true, grep: true, glob: true, find: true, ls: true };
  omp.registerCommand("${OMP_PERMISSION_GUARD_COMMAND}", {
    description: "T3 permission guard is active",
    handler: async (_args, ctx) => { ctx.ui.notify("T3 permission guard is active", "info"); },
  });
  omp.on("before_agent_start", (event) => {
    const instructions = process.env.T3_OMP_INSTRUCTIONS;
    if (instructions) return { systemPrompt: [...event.systemPrompt, instructions] };
  });
  omp.on("tool_call", async (event, ctx) => {
    if (mode === "full-access") return;
    if (Object.hasOwn(readOnly, event.toolName)) return;
    const path = event.input.path;
    const localEdit = (event.toolName === "edit" || event.toolName === "write") &&
      typeof path === "string" && !/^\\s*(?:\\[|[a-z][a-z0-9+.-]*:\\/\\/)/i.test(path);
    if (mode === "auto-accept-edits" && localEdit) return;
    if (!ctx.hasUI) return { block: true, reason: "T3 approval is required; this agent has no approval UI." };
    let approved = false;
    try {
      approved = await ctx.ui.confirm("T3 approval: " + event.toolName, JSON.stringify(event.input, null, 2));
    } catch {}
    if (approved !== true) return { block: true, reason: "Tool execution denied by T3 user." };
  });
}
`;

export const materializeOmpT3Extension = Effect.fn("Omp.materializePermissionGuard")(function* (
  cacheDir: string,
) {
  const fs = yield* FileSystem.FileSystem;
  yield* fs.makeDirectory(cacheDir, { recursive: true });
  const path = `${cacheDir.replace(/\\/g, "/").replace(/\/$/, "")}/omp-t3-permissions.ts`;
  const existing = yield* fs.readFileString(path).pipe(Effect.orElseSucceed(() => ""));
  if (existing !== OMP_PERMISSION_EXTENSION_SOURCE)
    yield* fs.writeFileString(path, OMP_PERMISSION_EXTENSION_SOURCE);
  return path;
});

export function buildOmpRpcLaunch(input: {
  readonly launchArgs: ReadonlyArray<string>;
  readonly environment: NodeJS.ProcessEnv;
  readonly mcpSession?: McpProviderSessionConfig | undefined;
  readonly extensionPath?: string | undefined;
  readonly runtimeMode?: OmpRuntimeMode;
  readonly sessionPath?: string | undefined;
  readonly ephemeral?: boolean;
  readonly disableExtensions?: boolean;
  readonly disableTools?: boolean;
}): { readonly args: ReadonlyArray<string>; readonly env: NodeJS.ProcessEnv } {
  const mode = input.runtimeMode ?? "full-access";
  if (
    mode !== "full-access" &&
    input.disableTools !== true &&
    (!input.extensionPath || input.disableExtensions)
  ) {
    throw new Error("OMP supervised mode requires the T3 permission guard extension.");
  }
  const userArgs: string[] = [];
  for (let index = 0; index < input.launchArgs.length; index++) {
    const arg = input.launchArgs[index]!;
    if (
      (input.disableExtensions && (arg === "--extension" || arg === "-e")) ||
      (input.disableTools && arg === "--tools")
    ) {
      index++;
      continue;
    }
    if (
      (input.disableExtensions && arg.startsWith("--extension=")) ||
      (input.disableTools && arg.startsWith("--tools="))
    )
      continue;
    userArgs.push(arg);
  }
  const args = [
    "--mode",
    "rpc-ui",
    ...userArgs,
    ...(input.ephemeral
      ? ["--no-session"]
      : input.sessionPath
        ? ["--session", input.sessionPath]
        : []),
    ...(input.disableExtensions ? ["--no-extensions"] : []),
    ...(input.disableTools ? ["--no-tools"] : []),
    ...(!input.disableExtensions && input.extensionPath
      ? ["--extension", input.extensionPath]
      : []),
    // The guard enforces T3 policy; native user deny/prompt policies still apply in yolo mode.
    "--approval-mode",
    "yolo",
  ];
  const env: NodeJS.ProcessEnv = {
    ...input.environment,
    [T3_OMP_RUNTIME_MODE_ENV]: mode,
    T3_OMP_INSTRUCTIONS: input.mcpSession
      ? [
          T3_CODE_ORCHESTRATION_INSTRUCTIONS,
          ...(input.mcpSession.browserToolsAvailable ? [T3_CODE_BROWSER_TOOL_INSTRUCTIONS] : []),
          ...(input.mcpSession.capabilities?.has("device")
            ? [T3_CODE_DEVICE_TOOL_INSTRUCTIONS]
            : []),
        ].join("\n\n")
      : "",
  };
  delete env.T3_MCP_URL;
  delete env.T3_MCP_BEARER_TOKEN;
  delete env.T3_PI_MCP_EXTENSION_PATH;
  return { args, env };
}
