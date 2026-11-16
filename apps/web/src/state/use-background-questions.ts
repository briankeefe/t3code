import { RegistryContext, useAtomValue } from "@effect/atom-react";
import { createBackgroundQuestionEnvironmentAtoms } from "@t3tools/client-runtime/state/backgroundQuestions";
import {
  createBackgroundQuestionController,
  type BackgroundQuestionTarget,
  type BackgroundQuestionsView,
} from "@t3tools/client-runtime/state/backgroundQuestionController";
import { useContext } from "react";
import { connectionAtomRuntime } from "../connection/runtime";
import { useEnvironmentQuery } from "./query";

const backgroundQuestions = createBackgroundQuestionEnvironmentAtoms(connectionAtomRuntime);
const controller = createBackgroundQuestionController(backgroundQuestions);

export function useBackgroundQuestions(target: BackgroundQuestionTarget) {
  const registry = useContext(RegistryContext);
  const panel = useAtomValue(controller.state(target));
  const supported = target.provider === "omp" && target.threadId !== null;
  const query = useEnvironmentQuery(
    supported && panel.open && target.threadId !== null
      ? backgroundQuestions.snapshot({
          environmentId: target.environmentId,
          input: { threadId: target.threadId },
        })
      : null,
  );
  return {
    ...panel,
    supported,
    records: query.data?.records ?? [],
    error: panel.error ?? query.error,
    loading: query.isPending && query.data === null,
    canAsk: useAtomValue(backgroundQuestions.ask.permissionAtom(target.environmentId)),
    canCancel: useAtomValue(backgroundQuestions.cancel.permissionAtom(target.environmentId)),
    ask: (request) => controller.ask(registry, target, request),
    cancel: (recordId) => controller.cancel(registry, target, recordId),
    handleCommand: (text) => controller.handleCommand(registry, target, text),
    refresh: query.refresh,
    setOpen: (open) => controller.update(registry, target, { open }),
    select: (selectedId) => controller.update(registry, target, { selectedId, error: null }),
  } satisfies BackgroundQuestionsView;
}
