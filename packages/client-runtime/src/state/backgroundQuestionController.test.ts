import { describe, expect, it, vi } from "vite-plus/test";
import { EnvironmentId, ThreadId, type BackgroundQuestionRecord } from "@t3tools/contracts";
import * as Cause from "effect/Cause";
import { AsyncResult, Atom, AtomRegistry } from "effect/reactivity";
import { type AtomCommandResult } from "./runtime.ts";
import {
  createBackgroundQuestionController,
  parseBackgroundQuestionCommand,
} from "./backgroundQuestionController.ts";

describe("background question controller", () => {
  it("keeps reserved commands out of normal text and rejects unsupported requests without asking", async () => {
    const registry = AtomRegistry.make();
    const allowed = Atom.make(true);
    const ask = vi.fn(async () =>
      AsyncResult.success<BackgroundQuestionRecord>({
        id: "topic",
        kind: "btw",
        createdAt: 1,
        updatedAt: 1,
        turns: [],
        report: null,
        reportError: null,
      }),
    );
    const controller = createBackgroundQuestionController({
      ask: { label: "ask", run: ask, permissionAtom: () => allowed },
      cancel: {
        label: "cancel",
        run: async () => AsyncResult.success({ cancelled: true }),
        permissionAtom: () => allowed,
      },
    });
    const target = {
      environmentId: EnvironmentId.make("env-a"),
      threadId: ThreadId.make("thread-a"),
      provider: "omp",
    };
    try {
      expect(parseBackgroundQuestionCommand("say /btw hello")).toBeNull();
      expect(parseBackgroundQuestionCommand("/btwice hello")).toBeNull();
      expect(parseBackgroundQuestionCommand(" /btw why?\nmore detail ")).toEqual({
        kind: "btw",
        question: "why?\nmore detail",
      });
      expect(await controller.handleCommand(registry, target, "/btw")).toBe(true);
      expect(registry.get(controller.state(target)).open).toBe(true);
      expect(ask).not.toHaveBeenCalled();
      expect(await controller.handleCommand(registry, target, "/pulse steer now")).toBe(false);
      expect(registry.get(controller.state(target)).error).toContain("without arguments");
      expect(
        await controller.handleCommand(registry, { ...target, provider: "codex" }, "/btw hello"),
      ).toBe(false);
      expect(registry.get(controller.state(target)).error).toContain("only supported by OMP");
      expect(
        await controller.handleCommand(registry, { ...target, threadId: null }, "/pulse"),
      ).toBe(false);
      registry.set(allowed, false);
      expect(await controller.handleCommand(registry, target, "/pulse")).toBe(false);
      expect(registry.get(controller.state(target)).error).toContain("cannot ask");
      expect(ask).not.toHaveBeenCalled();
      registry.set(allowed, true);
      expect(await controller.handleCommand(registry, target, "/pulse")).toBe(true);
      expect(ask.mock.calls[0]).toEqual([
        registry,
        {
          environmentId: target.environmentId,
          input: { threadId: target.threadId, kind: "pulse", question: undefined },
        },
      ]);
    } finally {
      registry.dispose();
    }
  });

  it("isolates pending answers by thread and environment, retains selection on close, and surfaces cancellation errors", async () => {
    const registry = AtomRegistry.make();
    const allowed = Atom.make(true);
    const answer = Promise.withResolvers<AtomCommandResult<BackgroundQuestionRecord, Error>>();
    const ask = vi.fn(() => answer.promise);
    const cancel = vi.fn(
      async (): Promise<AtomCommandResult<{ readonly cancelled: boolean }, Error>> =>
        AsyncResult.failure(Cause.fail(new Error("cancel failed"))),
    );
    const controller = createBackgroundQuestionController({
      ask: { label: "ask", run: ask, permissionAtom: () => allowed },
      cancel: { label: "cancel", run: cancel, permissionAtom: () => allowed },
    });
    const target = {
      environmentId: EnvironmentId.make("env-a"),
      threadId: ThreadId.make("thread-a"),
      provider: "omp",
    };
    const otherThread = { ...target, threadId: ThreadId.make("thread-b") };
    const otherEnvironment = { ...target, environmentId: EnvironmentId.make("env-b") };
    try {
      const request = controller.ask(registry, target, {
        kind: "btw",
        question: "follow up",
        recordId: "topic",
      });
      expect(registry.get(controller.state(target)).pending).toBe(true);
      expect(await controller.ask(registry, target, { kind: "pulse" })).toBe(false);
      controller.update(registry, target, { open: false });
      answer.resolve(
        AsyncResult.success({
          id: "topic",
          kind: "btw",
          createdAt: 1,
          updatedAt: 2,
          turns: [],
          report: null,
          reportError: null,
        }),
      );
      expect(await request).toBe(true);
      expect(registry.get(controller.state(target))).toMatchObject({
        open: false,
        pending: false,
        selectedId: "topic",
      });
      expect(registry.get(controller.state(otherThread))).toMatchObject({
        open: false,
        pending: false,
        selectedId: null,
      });
      expect(registry.get(controller.state(otherEnvironment))).toMatchObject({
        open: false,
        pending: false,
        selectedId: null,
      });
      await controller.handleCommand(registry, target, "/btw");
      expect(registry.get(controller.state(target))).toMatchObject({
        open: true,
        selectedId: "topic",
      });
      expect(ask).toHaveBeenCalledTimes(1);
      expect(ask.mock.calls[0]).toEqual([
        registry,
        {
          environmentId: target.environmentId,
          input: {
            threadId: target.threadId,
            kind: "btw",
            question: "follow up",
            recordId: "topic",
          },
        },
      ]);
      await controller.cancel(registry, target, "topic");
      expect(registry.get(controller.state(target)).error).toContain("cancel failed");
      expect(registry.get(controller.state(otherThread)).error).toBeNull();
    } finally {
      registry.dispose();
    }
  });
});
