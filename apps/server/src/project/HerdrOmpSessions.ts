import * as NodeUtil from "node:util";

import { AgentSessionScanError, type ProviderInstanceId } from "@t3tools/contracts";
import * as Digest from "effect/crypto/Digest";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";

import type {
  AgentSessionRecentThread,
  AgentSessionThread,
  AgentSessionThreadMessage,
} from "./AgentSessionScanner.ts";

export interface HerdrOmpWorkingCopy {
  readonly filePath: string;
  readonly originalFilePath?: string;
  readonly sessionId?: string;
  readonly title?: string;
}

export type HerdrOmpRecentThread =
  | (Extract<AgentSessionRecentThread, { readonly _tag: "Importable" }> & {
      readonly herdr: {
        readonly originalFilePath: string;
        readonly originalSessionId: string;
        readonly sourceVersion: string;
        readonly workingVersion: string;
        readonly relationship: "same" | "herdr_ahead" | "t3_ahead" | "diverged";
        readonly sourceContents: Uint8Array;
        readonly sourceThread: AgentSessionThread;
      };
    })
  | Extract<AgentSessionRecentThread, { readonly _tag: "Skipped" }>;

// Refuse unusually large native files explicitly rather than allocating without bound or truncating history.
const MAX_SNAPSHOT_BYTES = 256 * 1024 * 1024;

export const readHerdrOmpFile = (filePath: string) =>
  Effect.scoped(
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      if ((yield* fs.stat(filePath)).type !== "File")
        return invalid("Herdr OMP session is not a regular file");
      const file = yield* fs.open(filePath, { flag: "r", noFollow: true });
      const before = yield* file.stat;
      const size = Number(before.size);
      if (before.type !== "File" || !Number.isSafeInteger(size) || size > MAX_SNAPSHOT_BYTES)
        return invalid("Herdr OMP session exceeds the regular-file snapshot limit");
      const bytes = new Uint8Array(size);
      let offset = 0;
      while (offset < size) {
        const count = yield* file.read(bytes.subarray(offset));
        if (count === 0) return invalid("Herdr OMP session changed while reading");
        offset += count;
      }
      const after = yield* file.stat;
      const current = yield* fs.stat(filePath);
      if (
        before.size !== after.size ||
        Option.getOrNull(before.mtime)?.getTime() !== Option.getOrNull(after.mtime)?.getTime() ||
        after.dev !== current.dev ||
        Option.getOrNull(after.ino) !== Option.getOrNull(current.ino) ||
        after.size !== current.size ||
        Option.getOrNull(after.mtime)?.getTime() !== Option.getOrNull(current.mtime)?.getTime()
      )
        return invalid("Herdr OMP session changed while reading; retry the import");
      return { bytes, stats: after };
    }),
  );

type RecordValue = Record<string, unknown>;

class HerdrSessionError extends Schema.TaggedError<
  HerdrSessionError,
  { readonly brand: unique symbol }
>()("HerdrSessionError", { message: Schema.String }) {}
const isHerdrSessionError = Schema.is(HerdrSessionError);

function invalid(reason: string): never {
  throw new HerdrSessionError({ message: reason });
}

function record(value: unknown): RecordValue {
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    return invalid("Invalid Herdr OMP record shape");
  }
  return value as RecordValue;
}

function parseJson(text: string): unknown {
  try {
    return JSON.parse(text);
  } catch {
    // JSON parser errors can include private conversation text.
    return invalid("Malformed Herdr OMP JSON record");
  }
}

function timestamp(value: unknown): string {
  if (typeof value !== "string" && typeof value !== "number") {
    return invalid("Invalid Herdr OMP timestamp");
  }
  const parsed = DateTime.make(value);
  if (Option.isNone(parsed)) return invalid("Invalid Herdr OMP timestamp");
  return DateTime.formatIso(parsed.value);
}

function name(value: unknown): string | undefined {
  return typeof value === "string" && value.trim().length > 0 ? value : undefined;
}

function array(value: unknown): unknown[] {
  if (!Array.isArray(value)) return invalid("Invalid Herdr registry collection");
  return value;
}

function references(contents: string) {
  const refs: Array<{ cwd: string; filePath: string; title: string | undefined }> = [];
  for (const rawWorkspace of array(record(parseJson(contents)).workspaces)) {
    const workspace = record(rawWorkspace);
    for (const rawTab of array(workspace.tabs)) {
      const tab = record(rawTab);
      if (typeof tab.panes !== "object" || tab.panes === null) {
        return invalid("Invalid Herdr registry panes");
      }
      for (const rawPane of Object.values(tab.panes)) {
        const pane = record(rawPane);
        if (pane.agent_session === undefined || pane.agent_session === null) continue;
        const session = record(pane.agent_session);
        if (session.source !== "herdr:omp") continue;
        if (
          session.agent !== "omp" ||
          session.kind !== "path" ||
          typeof session.value !== "string" ||
          typeof pane.cwd !== "string"
        ) {
          return invalid("Invalid Herdr OMP session reference");
        }
        refs.push({
          cwd: pane.cwd,
          filePath: session.value,
          title: name(tab.custom_name) ?? name(workspace.custom_name),
        });
      }
    }
  }
  return refs;
}

interface Entry extends RecordValue {
  readonly id: string;
  readonly parentId: string | null;
  readonly timestamp: string | number;
}

function parseSession(bytes: Uint8Array) {
  const end = bytes.lastIndexOf(10) + 1;
  if (end === 0) return invalid("Herdr OMP session has no complete records");
  const complete = bytes.subarray(0, end);
  let text: string;
  try {
    text = new TextDecoder("utf-8", { fatal: true }).decode(complete);
  } catch {
    return invalid("Invalid Herdr OMP session encoding");
  }
  const records = text
    .slice(0, -1)
    .split("\n")
    .map((line) => record(parseJson(line)));
  const slot = records[0]?.type === "title" ? records.shift() : undefined;
  if (
    slot !== undefined &&
    (slot.v !== 1 || typeof slot.title !== "string" || typeof slot.pad !== "string")
  ) {
    return invalid("Invalid Herdr OMP title slot");
  }
  if (slot !== undefined) timestamp(slot.updatedAt);
  const header = records.shift();
  if (
    header?.type !== "session" ||
    header.version !== 3 ||
    typeof header.id !== "string" ||
    header.id.length === 0 ||
    typeof header.cwd !== "string"
  ) {
    return invalid("Invalid or unsupported Herdr OMP session header");
  }
  const createdAt = timestamp(header.timestamp);
  const entries = new Map<string, Entry>();
  let leafId: string | null = null;
  let lastSelfSelectedLeafId: string | null = null;
  for (const raw of records) {
    if (
      typeof raw.type !== "string" ||
      raw.type === "session" ||
      raw.type === "title" ||
      typeof raw.id !== "string" ||
      raw.id.length === 0 ||
      entries.has(raw.id) ||
      (raw.parentId !== null && typeof raw.parentId !== "string") ||
      (raw.activeLeafId !== undefined &&
        raw.activeLeafId !== null &&
        typeof raw.activeLeafId !== "string")
    ) {
      return invalid("Invalid Herdr OMP journal entry");
    }
    timestamp(raw.timestamp);
    const entry = raw as Entry;
    if (entry.type === "message") {
      const message = record(entry.message);
      if (typeof message.role !== "string") return invalid("Invalid Herdr OMP message role");
      if (message.timestamp !== undefined) timestamp(message.timestamp);
      if (message.role === "user" || message.role === "assistant") visibleText(message.content);
    }
    if (entry.type === "model_change" && typeof entry.model !== "string")
      return invalid("Invalid Herdr OMP model change");
    entries.set(entry.id, entry);
    // Native session-entries.ts resolves activeLeafId for off-branch/background appends.
    leafId = raw.activeLeafId === undefined ? entry.id : (raw.activeLeafId as string | null);
    if (leafId === entry.id) lastSelfSelectedLeafId = entry.id;
  }
  // Validate every branch, not only the selected path, before persisting the copy.
  const validated = new Set<string>();
  for (const entry of entries.values()) {
    const visiting = new Set<string>();
    let cursor: string | null = entry.id;
    while (cursor !== null && !validated.has(cursor)) {
      const ancestor = entries.get(cursor);
      if (ancestor === undefined || visiting.has(cursor))
        return invalid("Incomplete or cyclic Herdr OMP ancestry");
      visiting.add(cursor);
      cursor = ancestor.parentId;
    }
    for (const id of visiting) validated.add(id);
  }
  // Native resolveSessionLeafId falls back when rollback removed a captured active leaf.
  if (leafId !== null && !entries.has(leafId)) leafId = lastSelfSelectedLeafId;
  const branch: Entry[] = [];
  while (leafId !== null) {
    const entry = entries.get(leafId)!;
    branch.push(entry);
    leafId = entry.parentId;
  }
  branch.reverse();
  let model: string | null = null;
  let workspaceName: string | undefined;
  const titleUpdatedAt = slot === undefined ? createdAt : timestamp(slot.updatedAt);
  let updatedAt = titleUpdatedAt > createdAt ? titleUpdatedAt : createdAt;
  const messages: AgentSessionThreadMessage[] = [];
  for (const entry of branch) {
    const entryTimestamp = timestamp(entry.timestamp);
    if (entryTimestamp > updatedAt) updatedAt = entryTimestamp;
    if (entry.type === "model_change") model = entry.model as string;
    if (entry.type === "custom" && entry.customType === "herdr-workspace-name")
      workspaceName = name(record(entry.data).label) ?? workspaceName;
    if (entry.type !== "message") continue;
    const message = record(entry.message);
    if (message.role !== "user" && message.role !== "assistant") continue;
    if (message.role === "assistant" && typeof message.model === "string") {
      model =
        typeof message.provider === "string" && !message.model.startsWith(`${message.provider}/`)
          ? `${message.provider}/${message.model}`
          : message.model;
    }
    const text = visibleText(message.content);
    const messageTime =
      message.timestamp === undefined ? entryTimestamp : timestamp(message.timestamp);
    if (messageTime > updatedAt) updatedAt = messageTime;
    if (text.length > 0) messages.push({ role: message.role, text, createdAt: messageTime });
  }
  return {
    complete,
    sessionId: header.id,
    branch,
    cwd: header.cwd,
    createdAt,
    updatedAt,
    messages,
    model,
    title: name(slot === undefined ? header.title : slot.title),
    workspaceName,
  };
}

function visibleText(content: unknown): string {
  if (typeof content === "string") return content;
  if (!Array.isArray(content)) return invalid("Invalid Herdr OMP message content");
  return content
    .map((raw) => {
      const block = record(raw);
      if (typeof block.type !== "string") return invalid("Invalid Herdr OMP content block");
      if (block.type !== "text") return "";
      if (typeof block.text !== "string") return invalid("Invalid Herdr OMP text block");
      return block.text;
    })
    .join("");
}

export function herdrOmpThreads(input: {
  readonly registryPath: string;
  readonly workspaceRoot: string;
  readonly snapshotDirectory: string;
  readonly providerInstanceId: ProviderInstanceId;
  readonly workingCopies?: ReadonlyMap<string, HerdrOmpWorkingCopy>;
}): Stream.Stream<
  HerdrOmpRecentThread,
  AgentSessionScanError,
  FileSystem.FileSystem | Path.Path | Digest.Digest
> {
  return Stream.unwrap(
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const crypto = yield* Digest.Digest;
      const absolute = (value: string) => {
        if (!path.isAbsolute(value) || value.includes("\0"))
          return invalid("Herdr OMP paths must be absolute");
        return value;
      };
      const directory = Effect.fn(function* (value: string) {
        const canonical = yield* fs.realPath(absolute(value));
        if ((yield* fs.stat(canonical)).type !== "Directory")
          return invalid("Herdr OMP workspace is not a directory");
        return canonical;
      });
      const root = yield* directory(input.workspaceRoot);
      const registryContents = yield* fs.readFileString(absolute(input.registryPath)).pipe(
        Effect.catchIf(
          (error) => error.reason._tag === "NotFound",
          () => Effect.succeed(null),
        ),
      );
      const refs: Array<{
        cwd: string;
        filePath: string;
        title: string | undefined;
        providerSessionId?: string;
      }> = registryContents === null ? [] : references(registryContents);
      for (const [providerSessionId, working] of input.workingCopies ?? []) {
        if (working.originalFilePath !== undefined) {
          refs.push({
            cwd: root,
            filePath: working.originalFilePath,
            title: undefined,
            providerSessionId,
          });
        }
      }
      const seen = new Set<string>();
      return Stream.fromIterable(refs).pipe(
        Stream.mapEffect((ref) =>
          Effect.scoped(
            Effect.gen(function* () {
              if ((yield* directory(ref.cwd)) !== root) return { _tag: "Skipped" } as const;
              const original = yield* fs.realPath(absolute(ref.filePath));
              if (path.extname(original) !== ".jsonl")
                return invalid("Herdr OMP session must be a native JSONL file");
              if ((yield* fs.stat(original)).type !== "File")
                return invalid("Herdr OMP source is not a regular file");
              const duplicate = seen.has(original);
              yield* fs.makeDirectory(absolute(input.snapshotDirectory), {
                recursive: true,
                mode: 0o700,
              });
              const snapshots = yield* fs.realPath(input.snapshotDirectory);
              const digest = yield* crypto.digest(
                Digest.sha256,
                new TextEncoder().encode(original),
              );
              const providerSessionId = path.join(
                snapshots,
                `${Buffer.from(digest).toString("hex")}.jsonl`,
              );
              if (
                ref.providerSessionId !== undefined &&
                ref.providerSessionId !== providerSessionId
              ) {
                return invalid(
                  "Herdr OMP source provenance does not match its stable snapshot path",
                );
              }
              if (duplicate) return { _tag: "Duplicate" } as const;
              seen.add(original);
              const working = input.workingCopies?.get(providerSessionId);
              if (
                working?.originalFilePath !== undefined &&
                (yield* fs.realPath(absolute(working.originalFilePath))) !== original
              ) {
                return invalid("Herdr OMP source provenance does not match its original file");
              }
              const filePath = absolute(working?.filePath ?? providerSessionId);
              if (original === filePath)
                return invalid("Herdr OMP snapshot cannot reference its source");
              const source = yield* readHerdrOmpFile(original);
              const sourceParsed = parseSession(source.bytes);
              if ((yield* directory(sourceParsed.cwd)) !== root) {
                if (ref.providerSessionId !== undefined) return { _tag: "Skipped" } as const;
                return invalid("Herdr OMP session workspace does not match its registry reference");
              }
              if (working === undefined && !(yield* fs.exists(filePath))) {
                // Publish a complete private file atomically without replacing an existing/continued copy.
                const temporary = yield* fs.makeTempFileScoped({
                  directory: snapshots,
                  prefix: ".herdr-omp-",
                });
                yield* fs.chmod(temporary, 0o600);
                yield* fs.writeFile(temporary, sourceParsed.complete);
                yield* fs.link(temporary, filePath).pipe(
                  Effect.catchIf(
                    (error) => error.reason._tag === "AlreadyExists",
                    () => Effect.void,
                  ),
                );
              }
              if ((yield* fs.realPath(filePath)) !== filePath)
                return invalid("Herdr OMP snapshot must not be a symbolic link");
              const snapshot = yield* readHerdrOmpFile(filePath);
              const parsed = parseSession(snapshot.bytes);
              if (
                parsed.sessionId !== sourceParsed.sessionId ||
                (working?.sessionId !== undefined && working.sessionId !== sourceParsed.sessionId)
              ) {
                return invalid("Herdr OMP session identity does not match its source provenance");
              }
              if ((yield* directory(parsed.cwd)) !== root)
                return invalid(
                  "Herdr OMP snapshot workspace does not match its registry reference",
                );
              const prefixLength = Math.min(parsed.branch.length, sourceParsed.branch.length);
              let sharedPrefix = true;
              for (let index = 0; index < prefixLength; index++) {
                if (!NodeUtil.isDeepStrictEqual(parsed.branch[index], sourceParsed.branch[index])) {
                  sharedPrefix = false;
                  break;
                }
              }
              const relationship = !sharedPrefix
                ? "diverged"
                : parsed.branch.length === sourceParsed.branch.length
                  ? "same"
                  : parsed.branch.length < sourceParsed.branch.length
                    ? "herdr_ahead"
                    : "t3_ahead";
              const version = Effect.fn(function* (contents: Uint8Array) {
                return Buffer.from(yield* crypto.digest(Digest.sha256, contents)).toString("hex");
              });
              const makeThread = (session: typeof sourceParsed): AgentSessionThread => ({
                source: "omp",
                providerInstanceId: input.providerInstanceId,
                providerSessionId,
                title:
                  ref.title ??
                  session.title ??
                  session.workspaceName ??
                  session.messages.find((message) => message.role === "user")?.text ??
                  path.basename(original, ".jsonl"),
                model: session.model,
                createdAt: session.createdAt,
                updatedAt: session.updatedAt,
                messages: session.messages,
                omittedMessageCount: 0,
              });
              const thread = makeThread(parsed);
              const stats = snapshot.stats;
              return {
                _tag: "Importable",
                thread: working?.title === undefined ? thread : { ...thread, title: working.title },
                worktree: null,
                source: {
                  provider: "omp",
                  providerInstanceId: input.providerInstanceId,
                  providerSessionId,
                  filePath,
                  size: Number(stats.size),
                  mtimeMs: Option.getOrNull(stats.mtime)?.getTime() ?? null,
                  device: Number(stats.dev),
                  inode: Option.match(stats.ino, { onNone: () => null, onSome: String }),
                  birthtimeMs: Option.getOrNull(stats.birthtime)?.getTime() ?? null,
                  herdr: { filePath: original, sessionId: sourceParsed.sessionId },
                },
                herdr: {
                  originalFilePath: original,
                  originalSessionId: sourceParsed.sessionId,
                  sourceVersion: yield* version(sourceParsed.complete),
                  workingVersion: yield* version(parsed.complete),
                  relationship,
                  sourceContents: sourceParsed.complete,
                  sourceThread: makeThread(sourceParsed),
                },
              } satisfies HerdrOmpRecentThread;
            }),
          ),
        ),
        Stream.filter((outcome) => outcome._tag !== "Duplicate"),
      );
    }),
  ).pipe(
    Stream.catchDefect((cause) =>
      isHerdrSessionError(cause) ? Stream.fail(cause) : Stream.die(cause),
    ),
    Stream.mapError(
      (cause) =>
        new AgentSessionScanError({
          operation: "read-projects",
          cause: new Error(
            isHerdrSessionError(cause)
              ? cause.message
              : "Could not read Herdr OMP registry or session snapshot",
          ),
        }),
    ),
  );
}
