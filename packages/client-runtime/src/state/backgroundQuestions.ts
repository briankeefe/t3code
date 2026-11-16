import { WS_METHODS } from "@t3tools/contracts";
import type * as Atom from "effect/reactivity/Atom";
import type { EnvironmentRegistry } from "../connection/registry.ts";
import {
  createEnvironmentRpcCommand,
  createEnvironmentRpcSubscriptionAtomFamily,
} from "./runtime.ts";

export function createBackgroundQuestionEnvironmentAtoms<R, E>(
  runtime: Atom.AtomRuntime<EnvironmentRegistry | R, E>,
) {
  return {
    snapshot: createEnvironmentRpcSubscriptionAtomFamily(runtime, {
      label: "environment-data:background-questions:snapshot",
      tag: WS_METHODS.backgroundQuestionsSubscribe,
      idleTtlMs: 0,
    }),
    ask: createEnvironmentRpcCommand(runtime, {
      label: "environment-command:background-questions:ask",
      tag: WS_METHODS.backgroundQuestionsAsk,
    }),
    cancel: createEnvironmentRpcCommand(runtime, {
      label: "environment-command:background-questions:cancel",
      tag: WS_METHODS.backgroundQuestionsCancel,
    }),
  };
}
