import type {
  BackgroundQuestionAskInput,
  BackgroundQuestionCancelInput,
  BackgroundQuestionRecord,
  EnvironmentId,
  ThreadId,
} from "@t3tools/contracts";
import { AsyncResult, Atom, type AtomRegistry } from "effect/reactivity";
import { runAtomCommand, squashAtomCommandFailure, type AtomCommand } from "./runtime.ts";

export interface BackgroundQuestionPanelState {
  readonly open: boolean;
  readonly selectedId: string | null;
  readonly error: string | null;
  readonly pending: boolean;
}

export interface BackgroundQuestionTarget {
  readonly environmentId: EnvironmentId;
  readonly threadId: ThreadId | null;
  readonly provider: string | null;
}

export interface BackgroundQuestionsView extends BackgroundQuestionPanelState {
  readonly supported: boolean;
  readonly records: ReadonlyArray<BackgroundQuestionRecord>;
  readonly loading: boolean;
  readonly canAsk: boolean;
  readonly canCancel: boolean;
  readonly ask: (request: Omit<BackgroundQuestionAskInput, "threadId">) => Promise<boolean>;
  readonly cancel: (recordId: string) => Promise<void>;
  readonly handleCommand: (text: string) => Promise<boolean>;
  readonly refresh: () => void;
  readonly setOpen: (open: boolean) => void;
  readonly select: (selectedId: string) => void;
}

type BackgroundQuestionCommand<I, A, E> = AtomCommand<
  {
    readonly environmentId: EnvironmentId;
    readonly input: I;
  },
  A,
  E
> & { readonly permissionAtom: (environmentId: EnvironmentId | null) => Atom.Atom<boolean> };

export function parseBackgroundQuestionCommand(text: string) {
  const match = /^\/(btw|pulse)(?:\s+([\s\S]*))?$/u.exec(text.trim());
  return match
    ? {
        kind: match[1] === "pulse" ? ("pulse" as const) : ("btw" as const),
        question: match[2]?.trim() || undefined,
      }
    : null;
}

export function createBackgroundQuestionController<EA, EC>(commands: {
  readonly ask: BackgroundQuestionCommand<BackgroundQuestionAskInput, BackgroundQuestionRecord, EA>;
  readonly cancel: BackgroundQuestionCommand<
    BackgroundQuestionCancelInput,
    { readonly cancelled: boolean },
    EC
  >;
}) {
  const panel = Atom.family((_key: string) =>
    Atom.make<BackgroundQuestionPanelState>({
      open: false,
      selectedId: null,
      error: null,
      pending: false,
    }),
  );
  const state = (target: BackgroundQuestionTarget) =>
    panel(JSON.stringify([target.environmentId, target.threadId]));
  const update = (
    registry: AtomRegistry.AtomRegistry,
    target: BackgroundQuestionTarget,
    change: Partial<BackgroundQuestionPanelState>,
  ) => {
    const atom = state(target);
    registry.set(atom, { ...registry.get(atom), ...change });
  };
  const ask = async (
    registry: AtomRegistry.AtomRegistry,
    target: BackgroundQuestionTarget,
    request: Omit<BackgroundQuestionAskInput, "threadId">,
  ) => {
    update(registry, target, { open: true, error: null });
    if (target.provider !== "omp" || target.threadId === null) {
      update(registry, target, {
        error:
          target.provider !== "omp"
            ? "/btw and /pulse are only supported by OMP."
            : "Start an OMP conversation before asking a background question.",
      });
      return false;
    }
    if (!registry.get(commands.ask.permissionAtom(target.environmentId))) {
      update(registry, target, { error: "This connection cannot ask background questions." });
      return false;
    }
    if (registry.get(state(target)).pending) return false;
    update(registry, target, { pending: true });
    try {
      const result = await runAtomCommand(
        registry,
        commands.ask,
        {
          environmentId: target.environmentId,
          input: { threadId: target.threadId, ...request },
        },
        { reportFailure: false },
      );
      if (AsyncResult.isFailure(result)) {
        update(registry, target, { error: String(squashAtomCommandFailure(result)) });
        return false;
      }
      update(registry, target, { selectedId: result.value.id });
      return true;
    } finally {
      update(registry, target, { pending: false });
    }
  };
  return {
    state,
    update,
    ask,
    handleCommand: async (
      registry: AtomRegistry.AtomRegistry,
      target: BackgroundQuestionTarget,
      text: string,
    ) => {
      const command = parseBackgroundQuestionCommand(text);
      if (!command) return false;
      update(registry, target, { open: true, error: null });
      if (target.provider !== "omp" || target.threadId === null)
        return ask(registry, target, command);
      if (command.kind === "btw" && !command.question) return true;
      if (command.kind === "pulse" && command.question) {
        update(registry, target, {
          error: "Use /pulse without arguments. Ask a question with /btw instead.",
        });
        return false;
      }
      return ask(registry, target, command);
    },
    cancel: async (
      registry: AtomRegistry.AtomRegistry,
      target: BackgroundQuestionTarget,
      recordId: string,
    ) => {
      if (
        target.threadId === null ||
        !registry.get(commands.cancel.permissionAtom(target.environmentId))
      )
        return;
      const result = await runAtomCommand(
        registry,
        commands.cancel,
        {
          environmentId: target.environmentId,
          input: { threadId: target.threadId, recordId },
        },
        { reportFailure: false },
      );
      if (AsyncResult.isFailure(result))
        update(registry, target, { error: String(squashAtomCommandFailure(result)) });
    },
  };
}
