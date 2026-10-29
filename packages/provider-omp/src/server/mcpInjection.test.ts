import { assert, describe, it } from "@effect/vitest";
import * as NodeModule from "node:module";
import * as NodeVM from "node:vm";
import {
  buildOmpRpcLaunch,
  resolveOmpLaunchArgs,
  OMP_PERMISSION_EXTENSION_SOURCE,
} from "./mcpInjection.ts";

describe("OMP managed launch", () => {
  it("rejects owned modes, sessions, approval overrides and positional prompts", () => {
    for (const input of [
      "--mode rpc",
      "--mode=rpc-ui",
      "--session /tmp/s",
      "--session-dir=/tmp",
      "--resume",
      "--restore foo",
      "--continue",
      "--no-session",
      "-p hello",
      "hello",
      "@file",
      "-- hello",
      "--approval-mode yolo",
      "--auto-approve",
      "--config private.yml",
      "--profile work",
      "--alias unsafe",
      "--no-ui",
    ]) {
      assert.isFalse(resolveOmpLaunchArgs(input).ok, input);
    }
    assert.deepEqual(resolveOmpLaunchArgs('--model="anthropic/claude-haiku" --thinking low'), {
      ok: true,
      args: ["--model", "anthropic/claude-haiku", "--thinking", "low"],
    });
  });
  it("uses RPC UI and a mandatory session-local permission guard, without global settings writes", () => {
    assert.throws(
      () =>
        buildOmpRpcLaunch({ launchArgs: [], environment: {}, runtimeMode: "approval-required" }),
      /guard/,
    );
    for (const mode of ["approval-required", "auto-accept-edits", "full-access"] as const) {
      const launch = buildOmpRpcLaunch({
        launchArgs: [],
        environment: { KEEP: "yes" },
        extensionPath: "/tmp/t3-guard.ts",
        runtimeMode: mode,
        ephemeral: true,
      });
      assert.includeMembers(
        [...launch.args],
        [
          "--mode",
          "rpc-ui",
          "--approval-mode",
          "yolo",
          "--no-session",
          "--extension",
          "/tmp/t3-guard.ts",
        ],
      );
      assert.notInclude(launch.args, "--no-ui");
      assert.notInclude(launch.args, "--save");
      assert.equal(launch.env.KEEP, "yes");
    }
  });
  it("blocks supervised execution independently of native grants and fails closed without UI", async () => {
    type Call = { toolName: string; input: Record<string, unknown> };
    type Context = { hasUI: boolean; ui: { confirm: () => Promise<boolean> } };
    for (const mode of ["approval-required", "auto-accept-edits", "full-access"]) {
      let handler:
        | ((event: Call, ctx: Context) => Promise<{ block?: boolean } | undefined>)
        | undefined;
      const sandbox = {
        process: { env: { T3_OMP_RUNTIME_MODE: mode } },
        install: undefined as unknown,
      };
      NodeVM.runInNewContext(
        NodeModule.stripTypeScriptTypes(
          OMP_PERMISSION_EXTENSION_SOURCE.replace(
            "export default function",
            "globalThis.install = function",
          ),
        ),
        sandbox,
      );
      (sandbox.install as (api: unknown) => void)({
        registerCommand: () => {},
        on: (event: string, fn: typeof handler) => {
          if (event === "tool_call") handler = fn;
        },
      });
      assert.isDefined(handler);
      const denied: Context = { hasUI: true, ui: { confirm: async () => false } };
      const accepted: Context = { hasUI: true, ui: { confirm: async () => true } };
      const command: Call = { toolName: "bash", input: { command: "touch denied.txt" } };
      assert.equal(
        (await handler!(command, denied))?.block,
        mode === "full-access" ? undefined : true,
      );
      assert.isUndefined(await handler!(command, accepted));
      assert.equal(
        (await handler!(command, { ...accepted, hasUI: false }))?.block,
        mode === "full-access" ? undefined : true,
      );
      assert.isUndefined(await handler!({ toolName: "read", input: { path: "file" } }, denied));
      assert.equal(
        (await handler!({ toolName: "write", input: { path: "xd://device" } }, denied))?.block,
        mode === "full-access" ? undefined : true,
      );
      for (const path of ["[ssh://host/file#ABCD]", "  ssh://host/file", "[local.txt#ABCD]"]) {
        assert.equal(
          (await handler!({ toolName: "write", input: { path } }, denied))?.block,
          mode === "full-access" ? undefined : true,
        );
        assert.equal(
          (await handler!({ toolName: "write", input: { path } }, { ...accepted, hasUI: false }))
            ?.block,
          mode === "full-access" ? undefined : true,
        );
      }
      assert.equal(
        (await handler!({ toolName: "edit", input: { path: "file" } }, denied))?.block,
        mode === "approval-required" ? true : undefined,
      );
    }
  });
});
