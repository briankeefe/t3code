import * as NodeServices from "@effect/platform-node/NodeServices";
import { describe, expect, it } from "@effect/vitest";
import { ProviderInstanceId } from "@t3tools/contracts";
import { symlinksSupported } from "@t3tools/shared/testing/symlinks";
import * as Crypto from "effect/Crypto";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import * as Stream from "effect/Stream";

import { herdrOmpThreads, type HerdrOmpWorkingCopy } from "./HerdrOmpSessions.ts";

const providerInstanceId = ProviderInstanceId.make("omp-test");
const createdAt = "2026-01-01T00:00:00.000Z";
const encode = (records: ReadonlyArray<unknown>) =>
  records.map((entry) => JSON.stringify(entry)).join("\n") + "\n";
const version = (contents: string) =>
  Effect.gen(function* () {
    const crypto = yield* Crypto.Crypto;
    return Buffer.from(
      yield* crypto.digest("SHA-256", new TextEncoder().encode(contents)),
    ).toString("hex");
  });
const entry = (
  id: string,
  parentId: string | null,
  fields: Record<string, unknown> & { type: string },
) => ({
  id,
  parentId,
  timestamp: createdAt,
  ...fields,
});
const message = (id: string, parentId: string | null, role: string, text: string) =>
  entry(id, parentId, { type: "message", message: { role, content: [{ type: "text", text }] } });

const setup = Effect.gen(function* () {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const home = yield* fs
    .makeTempDirectoryScoped({ prefix: "t3-herdr-omp-" })
    .pipe(Effect.flatMap(fs.realPath));
  const workspaceRoot = path.join(home, "workspace");
  const snapshotDirectory = path.join(home, "snapshots");
  const registryPath = path.join(home, "session.json");
  const original = path.join(home, "native.jsonl");
  yield* fs.makeDirectory(workspaceRoot);
  const header = {
    type: "session",
    version: 3,
    id: "synthetic",
    cwd: workspaceRoot,
    timestamp: createdAt,
  };
  const ref = (filePath = original, cwd = workspaceRoot) => ({
    cwd,
    agent_session: { source: "herdr:omp", agent: "omp", kind: "path", value: filePath },
  });
  const registry = (
    panes: Record<string, unknown> = { main: ref() },
    names: { tab?: string; workspace?: string } = {},
  ) =>
    fs.writeFileString(
      registryPath,
      JSON.stringify({
        workspaces: [{ custom_name: names.workspace, tabs: [{ custom_name: names.tab, panes }] }],
      }),
    );
  const collectWithWorkingCopies = (workingCopies?: ReadonlyMap<string, HerdrOmpWorkingCopy>) =>
    herdrOmpThreads({
      registryPath,
      workspaceRoot,
      snapshotDirectory,
      providerInstanceId,
      ...(workingCopies === undefined ? {} : { workingCopies }),
    }).pipe(
      Stream.runCollect,
      Effect.map((outcomes) => outcomes.filter((outcome) => outcome._tag === "Importable")),
    );
  const collect = collectWithWorkingCopies();
  yield* registry();
  yield* fs.writeFileString(
    original,
    encode([header, message("u", null, "user", "Synthetic prompt")]),
  );
  return {
    fs,
    path,
    home,
    workspaceRoot,
    snapshotDirectory,
    registryPath,
    original,
    header,
    ref,
    registry,
    collect,
    collectWithWorkingCopies,
  };
});

describe("HerdrOmpSessions", () => {
  it.effect("copies the selected native branch, preserving text, model, title and timestamps", () =>
    Effect.gen(function* () {
      const fixture = yield* setup;
      const { fs, original, header, collect } = fixture;
      const userTime = "2026-01-01T00:01:00.000Z";
      const assistantTime = "2026-01-01T00:02:00.000Z";
      const contents = encode([
        { type: "title", v: 1, title: "Native title", pad: "", updatedAt: createdAt },
        { ...header, title: "Outdated header title" },
        entry("model", null, { type: "model_change", model: "test/model-one" }),
        entry("u", "model", {
          type: "message",
          timestamp: userTime,
          message: {
            role: "user",
            content: "  Synthetic prompt\n",
            timestamp: Date.parse(userTime),
          },
        }),
        message("abandoned", "u", "assistant", "Abandoned synthetic branch"),
        message("tool", "u", "toolResult", "Synthetic tool output"),
        entry("answer", "tool", {
          type: "message",
          timestamp: assistantTime,
          message: {
            role: "assistant",
            provider: "test",
            model: "model-two",
            content: [
              { type: "thinking", thinking: "Internal synthetic reasoning" },
              { type: "text", text: " First\n" },
              { type: "text", text: "second " },
            ],
          },
        }),
        entry("background", "abandoned", {
          type: "message",
          activeLeafId: "answer",
          timestamp: "2026-01-01T00:09:00.000Z",
          message: { role: "assistant", content: "Off-branch synthetic output" },
        }),
      ]);
      yield* fixture.registry({ one: fixture.ref(), duplicate: fixture.ref() });
      yield* fs.writeFileString(original, contents);
      const before = yield* fs.stat(original);
      const [outcome, ...rest] = yield* collect;
      expect(rest).toHaveLength(0);
      expect(outcome!.thread).toMatchObject({
        source: "omp",
        providerInstanceId,
        title: "Native title",
        model: "test/model-two",
        createdAt,
        updatedAt: assistantTime,
        messages: [
          { role: "user", text: "  Synthetic prompt\n", createdAt: userTime },
          { role: "assistant", text: " First\nsecond ", createdAt: assistantTime },
        ],
      });
      expect(outcome!.thread.providerSessionId).toBe(outcome!.source.filePath);
      expect(outcome!.source).toMatchObject({
        provider: "omp",
        providerInstanceId,
        providerSessionId: outcome!.source.filePath,
        size: new TextEncoder().encode(contents).length,
      });
      expect(outcome!.source.inode).not.toBeNull();
      expect(outcome!.source.herdr).toEqual({ filePath: original, sessionId: header.id });
      expect(outcome!.herdr).toMatchObject({
        originalFilePath: original,
        originalSessionId: header.id,
        relationship: "same",
        sourceVersion: yield* version(contents),
        workingVersion: yield* version(contents),
        sourceThread: outcome!.thread,
      });
      expect(new TextDecoder().decode(outcome!.herdr.sourceContents)).toBe(contents);
      expect(yield* fs.readFileString(outcome!.source.filePath)).toBe(contents);
      expect(yield* fs.readFileString(original)).toBe(contents);
      const after = yield* fs.stat(original);
      expect(after.ino).toEqual(before.ino);
      expect(after.mtime).toEqual(before.mtime);
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );

  it.effect("drops only an incomplete trailing record and retains every message beyond 200", () =>
    Effect.gen(function* () {
      const { fs, original, header, collect } = yield* setup;
      const messages = Array.from({ length: 205 }, (_, index) =>
        message(
          `m${index}`,
          index === 0 ? null : `m${index - 1}`,
          index % 2 === 0 ? "user" : "assistant",
          `Synthetic text ${index}`,
        ),
      );
      const complete = encode([header, ...messages]);
      const contents = complete + '{"type":"message","message":{"content":"unfinished';
      yield* fs.writeFileString(original, contents);
      const [outcome] = yield* collect;
      expect(outcome!.thread.messages).toHaveLength(205);
      expect(new TextDecoder().decode(outcome!.herdr.sourceContents)).toBe(complete);
      expect(outcome!.herdr.sourceVersion).toBe(yield* version(complete));
      expect(outcome!.thread.messages[0]!.text).toBe("Synthetic text 0");
      expect(outcome!.thread.messages.at(-1)!.text).toBe("Synthetic text 204");
      expect(yield* fs.readFileString(outcome!.source.filePath)).toBe(complete);
      expect(yield* fs.readFileString(original)).toBe(contents);
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );

  it.effect("never replaces an existing snapshot, including its independent continuation", () =>
    Effect.gen(function* () {
      const { fs, original, collect, snapshotDirectory } = yield* setup;
      const nativeBefore = yield* fs.readFileString(original);
      const [[first], [concurrent]] = yield* Effect.all([collect, collect], {
        concurrency: "unbounded",
      });
      expect(first!.source.filePath).toBe(concurrent!.source.filePath);
      const nativeAdvanced =
        nativeBefore + encode([message("native-next", "u", "assistant", "New native text")]);
      yield* fs.writeFileString(original, nativeAdvanced);
      const [second] = yield* collect;
      expect(second!.source).toEqual(first!.source);
      expect(second!.herdr.relationship).toBe("herdr_ahead");
      expect(second!.herdr.sourceThread.messages.at(-1)?.text).toBe("New native text");
      expect(yield* fs.readFileString(second!.source.filePath)).toBe(nativeBefore);
      const continued =
        nativeBefore +
        encode([message("private-next", "u", "assistant", "Private continued text")]);
      yield* fs.writeFileString(first!.source.filePath, continued);
      const [third] = yield* collect;
      expect(third!.thread.providerSessionId).toBe(first!.thread.providerSessionId);
      expect(third!.herdr.relationship).toBe("diverged");
      expect(third!.thread.messages.map((value) => value.text)).toEqual([
        "Synthetic prompt",
        "Private continued text",
      ]);
      expect(yield* fs.readFileString(third!.source.filePath)).toBe(continued);
      expect(yield* fs.readFileString(original)).toBe(nativeAdvanced);
      expect(yield* fs.readDirectory(snapshotDirectory)).toHaveLength(1);
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );

  it.effect(
    "compares selected native ancestry and content rather than visible text or timestamps",
    () =>
      Effect.gen(function* () {
        const fixture = yield* setup;
        const [first] = yield* fixture.collect;
        const user = message("u", null, "user", "Synthetic prompt");
        const answer = message("a", "u", "assistant", "Identical visible answer");
        const otherAnswer = message("b", "u", "assistant", "Identical visible answer");
        const tool = message("tool", "u", "toolResult", "Native tool result");
        const background = entry("off", "u", {
          type: "model_usage",
          activeLeafId: "u",
          inputTokens: 12,
        });
        const cases = [
          { source: [user, answer], working: [user], relationship: "herdr_ahead" },
          { source: [user], working: [user, answer], relationship: "t3_ahead" },
          { source: [user, answer], working: [user, otherAnswer], relationship: "diverged" },
          { source: [user, tool], working: [user], relationship: "herdr_ahead" },
          {
            source: [user, tool],
            working: [user, message("tool", "u", "toolResult", "Different native tool result")],
            relationship: "diverged",
          },
          {
            source: [
              user,
              entry("a", "u", {
                type: "message",
                message: {
                  role: "assistant",
                  content: [
                    { type: "thinking", thinking: "Native reasoning one" },
                    { type: "text", text: "Identical visible answer" },
                  ],
                },
              }),
            ],
            working: [
              user,
              entry("a", "u", {
                type: "message",
                message: {
                  role: "assistant",
                  content: [
                    { type: "thinking", thinking: "Native reasoning two" },
                    { type: "text", text: "Identical visible answer" },
                  ],
                },
              }),
            ],
            relationship: "diverged",
          },
          {
            source: [user, entry("context", "u", { type: "compaction", summary: "One" })],
            working: [user, entry("context", "u", { type: "compaction", summary: "Two" })],
            relationship: "diverged",
          },
          {
            source: [user, { ...answer, timestamp: "2026-01-02T00:00:00.000Z" }],
            working: [user, answer],
            relationship: "diverged",
          },
          { source: [user, background], working: [user], relationship: "same" },
          {
            source: [user, answer, { ...background, activeLeafId: "a" }],
            working: [user, answer],
            relationship: "same",
          },
          {
            source: [user, answer, background],
            working: [user, answer],
            relationship: "t3_ahead",
          },
          {
            source: [user, { ...background, activeLeafId: "removed-by-rollback" }],
            working: [user],
            relationship: "same",
          },
          {
            source: [user, { ...background, activeLeafId: null }],
            working: [user],
            relationship: "t3_ahead",
          },
          {
            source: [
              {
                type: "title",
                v: 1,
                title: "Renamed native session",
                pad: "",
                updatedAt: "2026-01-02T00:00:00.000Z",
              },
              user,
            ],
            working: [user],
            relationship: "same",
          },
        ];
        for (const testCase of cases) {
          const sourceContents = encode([
            ...(testCase.source[0]?.type === "title" ? [testCase.source[0]] : []),
            fixture.header,
            ...testCase.source.filter((value) => value.type !== "title"),
          ]);
          const workingContents = encode([fixture.header, ...testCase.working]);
          yield* fixture.fs.writeFileString(fixture.original, sourceContents);
          yield* fixture.fs.writeFileString(first!.source.filePath, workingContents);
          const [outcome] = yield* fixture.collect;
          expect(outcome!.herdr.relationship).toBe(testCase.relationship);
          expect(outcome!.herdr.sourceVersion).toBe(yield* version(sourceContents));
          expect(outcome!.herdr.workingVersion).toBe(yield* version(workingContents));
          expect(yield* fixture.fs.readFileString(first!.source.filePath)).toBe(workingContents);
          expect(yield* fixture.fs.readFileString(fixture.original)).toBe(sourceContents);
        }
      }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );

  it.effect(
    "uses continued copies without changing stable identity and finds persisted originals",
    () =>
      Effect.gen(function* () {
        const fixture = yield* setup;
        const [first] = yield* fixture.collect;
        const stableId = first!.thread.providerSessionId;
        const continuedPath = fixture.path.join(fixture.home, "continued.jsonl");
        const continued = encode([
          fixture.header,
          message("u", null, "user", "Synthetic prompt"),
          entry("context", "u", { type: "compaction", summary: "Native continuation context" }),
          message("a", "context", "assistant", "Continued answer"),
        ]);
        yield* fixture.fs.writeFileString(continuedPath, continued);
        const workingCopies = new Map<string, HerdrOmpWorkingCopy>([
          [stableId, { filePath: continuedPath, sessionId: fixture.header.id }],
        ]);
        const [legacy] = yield* fixture.collectWithWorkingCopies(workingCopies);
        expect(legacy!.thread.providerSessionId).toBe(stableId);
        expect(legacy!.source.providerSessionId).toBe(stableId);
        expect(legacy!.source.filePath).toBe(continuedPath);
        expect(legacy!.thread.messages.at(-1)?.text).toBe("Continued answer");
        expect(legacy!.herdr.relationship).toBe("t3_ahead");
        expect(legacy!.herdr.sourceThread.providerSessionId).toBe(stableId);
        workingCopies.set(stableId, {
          filePath: continuedPath,
          sessionId: fixture.header.id,
          originalFilePath: fixture.original,
          title: "Preserved thread title",
        });
        yield* fixture.registry({});
        const [persisted] = yield* fixture.collectWithWorkingCopies(workingCopies);
        expect(persisted!.source).toEqual(legacy!.source);
        expect(persisted!.thread.title).toBe("Preserved thread title");
        expect(persisted!.herdr.relationship).toBe("t3_ahead");
        expect(yield* fixture.fs.readFileString(continuedPath)).toBe(continued);
        expect(yield* fixture.fs.readDirectory(fixture.snapshotDirectory)).toHaveLength(1);
        yield* fixture.fs.remove(fixture.registryPath);
        const [withoutRegistry] = yield* fixture.collectWithWorkingCopies(workingCopies);
        expect(withoutRegistry!.thread.providerSessionId).toBe(stableId);
        expect(withoutRegistry!.source.filePath).toBe(continuedPath);
        expect(withoutRegistry!.herdr.sourceThread.messages.map((value) => value.text)).toEqual([
          "Synthetic prompt",
        ]);
        expect(withoutRegistry!.herdr.relationship).toBe("t3_ahead");
        yield* fixture.fs.remove(fixture.original);
        expect(
          (yield* fixture.collectWithWorkingCopies(workingCopies).pipe(Effect.flip))._tag,
        ).toBe("AgentSessionScanError");
        expect(yield* fixture.fs.readFileString(continuedPath)).toBe(continued);
      }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );

  it.effect("only rediscovers persisted originals from the expected workspace", () =>
    Effect.gen(function* () {
      const fixture = yield* setup;
      const [first] = yield* fixture.collect;
      const otherWorkspace = fixture.path.join(fixture.home, "other");
      yield* fixture.fs.makeDirectory(otherWorkspace);
      yield* fixture.fs.writeFileString(
        fixture.original,
        encode([
          { ...fixture.header, cwd: otherWorkspace },
          message("u", null, "user", "Another workspace prompt"),
        ]),
      );
      yield* fixture.registry({});
      const workingCopies = new Map([
        [
          first!.thread.providerSessionId,
          {
            filePath: first!.source.filePath,
            originalFilePath: fixture.original,
            sessionId: fixture.header.id,
          },
        ],
      ]);
      expect(yield* fixture.collectWithWorkingCopies(workingCopies)).toHaveLength(0);
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );

  it.effect(
    "rejects changed session identities and unreadable originals without replacing copies",
    () =>
      Effect.gen(function* () {
        const fixture = yield* setup;
        const [first] = yield* fixture.collect;
        const native = yield* fixture.fs.readFileString(fixture.original);
        const other = encode([
          { ...fixture.header, id: "unrelated-session" },
          message("u", null, "user", "Private unrelated text"),
        ]);
        yield* fixture.fs.writeFileString(fixture.original, other);
        expect(String((yield* fixture.collect.pipe(Effect.flip)).cause)).toContain(
          "session identity",
        );
        expect(yield* fixture.fs.readFileString(first!.source.filePath)).toBe(native);
        yield* fixture.fs.writeFileString(fixture.original, native);
        yield* fixture.fs.writeFileString(first!.source.filePath, other);
        expect(String((yield* fixture.collect.pipe(Effect.flip)).cause)).toContain(
          "session identity",
        );
        expect(yield* fixture.fs.readFileString(first!.source.filePath)).toBe(other);
        yield* fixture.fs.writeFileString(first!.source.filePath, native);
        const workingCopies = new Map<string, HerdrOmpWorkingCopy>([
          [
            first!.thread.providerSessionId,
            {
              filePath: first!.source.filePath,
              originalFilePath: fixture.original,
              sessionId: "wrong-persisted-id",
            },
          ],
        ]);
        expect(
          String((yield* fixture.collectWithWorkingCopies(workingCopies).pipe(Effect.flip)).cause),
        ).toContain("session identity");
        yield* fixture.fs.writeFileString(
          fixture.original,
          native + encode([message("broken", "absent", "assistant", "Malformed continuation")]),
        );
        expect(String((yield* fixture.collect.pipe(Effect.flip)).cause)).toContain("ancestry");
        expect(yield* fixture.fs.readFileString(first!.source.filePath)).toBe(native);
        yield* fixture.fs.writeFileString(fixture.original, native);
        yield* fixture.fs.remove(fixture.original);
        expect((yield* fixture.collect.pipe(Effect.flip))._tag).toBe("AgentSessionScanError");
        expect(yield* fixture.fs.readFileString(first!.source.filePath)).toBe(native);
        yield* fixture.fs.makeDirectory(fixture.original);
        expect((yield* fixture.collect.pipe(Effect.flip))._tag).toBe("AgentSessionScanError");
        expect(yield* fixture.fs.readFileString(first!.source.filePath)).toBe(native);
      }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );

  it.effect("uses native cleared and dangling-captured leaf replay semantics", () =>
    Effect.gen(function* () {
      const fixture = yield* setup;
      for (const [suffix, activeLeafId, expected] of [
        ["cleared", null, []],
        ["missing", "removed-by-rollback", ["Synthetic prompt"]],
      ] as const) {
        const original = fixture.path.join(fixture.home, `${suffix}.jsonl`);
        yield* fixture.fs.writeFileString(
          original,
          encode([
            fixture.header,
            message("u", null, "user", "Synthetic prompt"),
            entry("usage", "u", { type: "model_usage", activeLeafId }),
          ]),
        );
        yield* fixture.registry({ main: fixture.ref(original) });
        const [outcome] = yield* fixture.collect;
        expect(outcome!.thread.messages.map((value) => value.text)).toEqual(expected);
      }
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );

  it.effect(
    "prefers named Herdr tabs and workspaces over native auto titles, with branch label fallback",
    () =>
      Effect.gen(function* () {
        const fixture = yield* setup;
        const names = [
          { tab: "Tab title", workspace: "Workspace title", expected: "Tab title" },
          { workspace: "Workspace title", expected: "Workspace title" },
          { expected: "Persisted workspace label" },
        ];
        for (const [index, naming] of names.entries()) {
          const original = fixture.path.join(fixture.home, `named-${index}.jsonl`);
          yield* fixture.fs.writeFileString(
            original,
            encode([
              ...(index < 2
                ? [
                    {
                      type: "title",
                      v: 1,
                      title: "Native automatic title",
                      source: "auto",
                      updatedAt: createdAt,
                      pad: "",
                    },
                  ]
                : []),
              fixture.header,
              message("u", null, "user", "Synthetic prompt"),
              entry("name", "u", {
                type: "custom",
                customType: "herdr-workspace-name",
                data: { label: "Persisted workspace label", lock: "manual" },
              }),
            ]),
          );
          yield* fixture.registry({ main: fixture.ref(original) }, naming);
          const [outcome] = yield* fixture.collect;
          expect(outcome!.thread.title).toBe(naming.expected);
        }
      }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );

  it.effect(
    "reads only current matching registry refs and canonicalizes workspace and source aliases",
    () =>
      Effect.gen(function* () {
        const fixture = yield* setup;
        const { fs, path, home, original, workspaceRoot, collect } = fixture;
        const otherWorkspace = path.join(home, "other");
        yield* fs.makeDirectory(otherWorkspace);
        const workspaceAlias = path.join(home, "workspace-alias");
        const sourceAlias = path.join(home, "native-alias.jsonl");
        if (symlinksSupported) {
          yield* fs.symlink(workspaceRoot, workspaceAlias);
          yield* fs.symlink(original, sourceAlias);
          yield* fs.writeFileString(
            original,
            encode([
              { ...fixture.header, cwd: workspaceAlias },
              message("u", null, "user", "Synthetic prompt"),
            ]),
          );
        }
        yield* fixture.registry({
          matching: fixture.ref(original),
          alias: fixture.ref(
            symlinksSupported ? sourceAlias : original,
            symlinksSupported ? workspaceAlias : workspaceRoot,
          ),
          other: fixture.ref(path.join(home, "missing.jsonl"), otherWorkspace),
          unrelated: { agent_session: { source: "other", kind: "path", value: "/not-used" } },
        });
        expect(yield* collect).toHaveLength(1);
        yield* fixture.registry({ other: fixture.ref(original, otherWorkspace) });
        expect(yield* collect).toHaveLength(0);
      }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );

  it.effect(
    "rejects corrupted complete records without transcript text in errors or durable copies",
    () =>
      Effect.gen(function* () {
        const fixture = yield* setup;
        const cases = [
          encode([fixture.header]) + '{"private synthetic marker"\n',
          encode([fixture.header, message("u", "missing", "user", "private synthetic marker")]),
          encode([fixture.header, message("u", "u", "user", "private synthetic marker")]),
          encode([
            fixture.header,
            message("u", null, "user", "private synthetic marker"),
            message("u", null, "assistant", "duplicate"),
          ]),
          encode([
            fixture.header,
            entry("u", null, {
              type: "message",
              message: { role: "user", content: [{ type: "text", text: 5 }] },
            }),
          ]),
          encode([{ ...fixture.header, version: 99 }]),
        ];
        for (const contents of cases) {
          yield* fixture.fs.writeFileString(fixture.original, contents);
          const error = yield* fixture.collect.pipe(Effect.flip);
          expect(error._tag).toBe("AgentSessionScanError");
          expect(String(error.cause)).not.toContain("private synthetic marker");
          expect(yield* fixture.fs.readFileString(fixture.original)).toBe(contents);
          expect(yield* fixture.fs.readDirectory(fixture.snapshotDirectory)).toHaveLength(0);
        }
      }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );

  it.effect("rejects oversized native files explicitly before allocating their contents", () =>
    Effect.gen(function* () {
      const fixture = yield* setup;
      const file = yield* fixture.fs.open(fixture.original, { flag: "r+" });
      yield* file.truncate(256 * 1024 * 1024 + 1);
      yield* fixture.collect.pipe(Effect.flip);
      expect(yield* fixture.fs.readDirectory(fixture.snapshotDirectory)).toHaveLength(0);
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );

  it.effect("rejects mismatched headers, unsafe paths, and missing native sources honestly", () =>
    Effect.gen(function* () {
      const fixture = yield* setup;
      const otherWorkspace = fixture.path.join(fixture.home, "other");
      yield* fixture.fs.makeDirectory(otherWorkspace);
      yield* fixture.fs.writeFileString(
        fixture.original,
        encode([
          { ...fixture.header, cwd: otherWorkspace },
          message("u", null, "user", "Synthetic prompt"),
        ]),
      );
      expect((yield* fixture.collect.pipe(Effect.flip))._tag).toBe("AgentSessionScanError");
      for (const ref of [
        fixture.ref("relative.jsonl"),
        fixture.ref(fixture.path.join(fixture.home, "missing.jsonl")),
        fixture.ref(fixture.original, "relative-workspace"),
        {
          cwd: fixture.workspaceRoot,
          agent_session: { source: "herdr:omp", agent: "omp", kind: "id", value: "unsafe" },
        },
      ]) {
        yield* fixture.registry({ main: ref });
        expect((yield* fixture.collect.pipe(Effect.flip))._tag).toBe("AgentSessionScanError");
      }
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );
});
