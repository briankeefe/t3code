import * as Clock from "effect/Clock";
import * as Deferred from "effect/Deferred";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Predicate from "effect/Predicate";
import * as Queue from "effect/Queue";
import * as Scope from "effect/Scope";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import * as ChildProcess from "effect/process/ChildProcess";
import * as ChildProcessSpawner from "effect/process/ChildProcessSpawner";
import * as HostProcess from "@t3tools/shared/HostProcess";
import { resolveSpawnCommand } from "@t3tools/shared/shell";
import { signalProcessGroup } from "@t3tools/provider-core/server/processGroup";

export class OmpRpcError extends Schema.TaggedError<OmpRpcError>()("OmpRpcError", {
  operation: Schema.String,
  detail: Schema.optional(Schema.String),
  cause: Schema.optional(Schema.Defect()),
}) {
  override get message(): string {
    return `OMP RPC ${this.operation} failed${this.detail === undefined ? "" : `: ${this.detail}`}.`;
  }
}

export class OmpRpcTimeoutError extends Schema.TaggedError<OmpRpcTimeoutError>()(
  "OmpRpcTimeoutError",
  {
    operation: Schema.String,
    timeoutMs: Schema.Int.check(Schema.isBetween({ minimum: 0, maximum: Number.MAX_SAFE_INTEGER })),
  },
) {
  override get message(): string {
    return `OMP RPC ${this.operation} timed out after ${this.timeoutMs}ms.`;
  }
}

export type OmpRpcRecord = Record<string, unknown>;
export function ompRecordField(input: unknown, key: string): unknown {
  return Predicate.isObject(input) ? input[key] : undefined;
}
export function ompRecordString(input: unknown, key: string): string | undefined {
  const value = ompRecordField(input, key);
  return typeof value === "string" ? value : undefined;
}
function ompRecordNumber(input: unknown, key: string): number | undefined {
  const value = ompRecordField(input, key);
  return typeof value === "number" && Number.isFinite(value) ? value : undefined;
}
export function parseOmpModelSlug(slug: string): { provider: string; modelId: string } | null {
  const separator = slug.indexOf("/");
  return separator <= 0 || separator === slug.length - 1
    ? null
    : {
        provider: slug.slice(0, separator),
        modelId: slug.slice(separator + 1),
      };
}
export interface OmpRpcSpawnOptions {
  readonly command: string;
  readonly args: ReadonlyArray<string>;
  readonly cwd: string | undefined;
  readonly env: NodeJS.ProcessEnv;
}
export interface OmpRpcConnection {
  readonly send: (record: OmpRpcRecord) => Effect.Effect<void, OmpRpcError>;
  readonly request: (
    record: OmpRpcRecord,
    timeoutMs?: number,
  ) => Effect.Effect<unknown, OmpRpcError | OmpRpcTimeoutError>;
  readonly events: Queue.Queue<OmpRpcRecord, OmpRpcError>;
  readonly exited: Effect.Effect<number, OmpRpcError>;
  readonly terminate: Effect.Effect<void>;
}

export const MAX_OMP_FRAME_BYTES = 1024 * 1024;
const MAX_REASSEMBLED_BYTES = 64 * 1024 * 1024;
const CHUNK_BYTES = 256 * 1024;
const RecordFromJsonString = Schema.fromJsonString(Schema.Record(Schema.String, Schema.Unknown));
const decodeRecord = Schema.decodeSync(RecordFromJsonString);
const encodeRecord = Schema.encodeSync(RecordFromJsonString);

// OMP's protocol-v2 wire format requires one contiguous, ordered chunk sequence.
export class OmpFrameDecoder {
  #pending:
    | {
        id: string;
        count: number;
        length: number;
        next: number;
        chunks: Buffer[];
        received: number;
      }
    | undefined;

  push(value: OmpRpcRecord): OmpRpcRecord | undefined {
    if (value.type !== "rpc_chunk") {
      if (this.#pending) throw new Error("OMP chunk sequence interrupted");
      return value;
    }
    const { chunkId, index, count, byteLength, data } = value;
    if (
      typeof chunkId !== "string" ||
      !chunkId.length ||
      chunkId.length > 128 ||
      typeof index !== "number" ||
      !Number.isSafeInteger(index) ||
      index < 0 ||
      typeof count !== "number" ||
      !Number.isSafeInteger(count) ||
      count < 2 ||
      count > MAX_REASSEMBLED_BYTES / CHUNK_BYTES ||
      typeof byteLength !== "number" ||
      !Number.isSafeInteger(byteLength) ||
      byteLength < MAX_OMP_FRAME_BYTES ||
      byteLength > MAX_REASSEMBLED_BYTES ||
      count !== Math.ceil(byteLength / CHUNK_BYTES) ||
      index >= count
    ) {
      throw new Error("Invalid OMP chunk metadata");
    }
    if (
      typeof data !== "string" ||
      data.length > 4 * Math.ceil(CHUNK_BYTES / 3) ||
      !/^(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=)?$/.test(data)
    ) {
      throw new Error("Invalid OMP chunk base64");
    }
    const bytes = Buffer.from(data, "base64");
    if (
      bytes.toString("base64") !== data ||
      bytes.length !== Math.min(CHUNK_BYTES, byteLength - index * CHUNK_BYTES)
    ) {
      throw new Error("Invalid OMP chunk payload length");
    }
    if (!this.#pending) {
      if (index !== 0) throw new Error("OMP chunk sequence must start at zero");
      this.#pending = { id: chunkId, count, length: byteLength, next: 0, chunks: [], received: 0 };
    }
    const pending = this.#pending;
    if (
      pending.id !== chunkId ||
      pending.count !== count ||
      pending.length !== byteLength ||
      pending.next !== index
    ) {
      throw new Error("OMP chunk sequence mismatch");
    }
    pending.chunks.push(bytes);
    pending.received += bytes.length;
    pending.next++;
    if (pending.next < count) return undefined;
    if (pending.received !== byteLength) throw new Error("OMP chunk length mismatch");
    this.#pending = undefined;
    const result = decodeRecord(
      new TextDecoder("utf-8", { fatal: true }).decode(Buffer.concat(pending.chunks, byteLength)),
    );
    if (result.type === "rpc_chunk") throw new Error("Invalid reassembled OMP frame");
    return result;
  }

  finish(): void {
    if (this.#pending) throw new Error("Incomplete OMP chunk sequence at EOF");
  }
}

export const makeOmpRpcConnection = Effect.fn("OmpRpc.connect")(function* (
  options: OmpRpcSpawnOptions,
) {
  const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
  const platform = yield* HostProcess.Platform;
  const scope = yield* Effect.scope;
  const spawn = yield* resolveSpawnCommand(options.command, [...options.args], {
    env: options.env,
  }).pipe(Effect.mapError((cause) => new OmpRpcError({ operation: "spawn", cause })));
  let childExited = false;
  let terminating = false;
  const exit = yield* Deferred.make<number, OmpRpcError>();
  const stopped = yield* Deferred.make<never, OmpRpcError>();
  const ready = yield* Deferred.make<OmpRpcRecord, OmpRpcError>();
  const events = yield* Queue.unbounded<OmpRpcRecord, OmpRpcError>();
  const outgoing = yield* Queue.unbounded<Uint8Array, OmpRpcError>();
  const pending = new Map<
    string,
    { command: string; deferred: Deferred.Deferred<unknown, OmpRpcError> }
  >();
  let nextId = 0;
  let sawReady = false;
  let negotiated = false;

  const fail = Effect.fn("OmpRpc.fail")(function* (error: OmpRpcError) {
    if (!(yield* Deferred.fail(stopped, error))) return;
    yield* Deferred.fail(ready, error);
    for (const request of pending.values()) yield* Deferred.fail(request.deferred, error);
    pending.clear();
    yield* Queue.fail(outgoing, error);
    yield* Queue.fail(events, error);
  });
  const child = yield* spawner
    .spawn(
      ChildProcess.make(spawn.command, spawn.args, {
        ...(options.cwd === undefined ? {} : { cwd: options.cwd }),
        env: options.env,
        extendEnv: false,
        shell: spawn.shell,
        detached: platform !== "win32",
      }),
    )
    .pipe(Effect.mapError((cause) => new OmpRpcError({ operation: "spawn", cause })));
  const signal = Effect.fn("OmpRpc.signal")(function* (kind: NodeJS.Signals) {
    if (childExited || !(yield* child.isRunning.pipe(Effect.orElseSucceed(() => false))))
      return false;
    return yield* Effect.try(() => {
      if (platform !== "win32") signalProcessGroup(Number(child.pid), kind);
      else process.kill(Number(child.pid), kind);
      return true;
    }).pipe(Effect.orElseSucceed(() => false));
  });
  const terminate = Effect.gen(function* () {
    if (terminating) return;
    terminating = true;
    yield* fail(new OmpRpcError({ operation: "terminate", detail: "OMP process stopped" }));
    if (childExited || !(yield* child.isRunning.pipe(Effect.orElseSucceed(() => false)))) return;
    if (!Number.isSafeInteger(Number(child.pid)) || Number(child.pid) <= 1) return;
    if (platform === "win32") {
      const taskkill = yield* spawner.spawn(
        ChildProcess.make("taskkill", ["/PID", String(child.pid), "/T", "/F"]),
      );
      yield* taskkill.exitCode;
    } else {
      if (!(yield* signal("SIGTERM"))) return;
      yield* Deferred.await(exit).pipe(
        Effect.timeoutOption(Duration.seconds(1)),
        Effect.provideService(Clock.Clock, Clock.Clock.defaultValue()),
        Effect.ignore,
      );
      // Never signal a recycled pid after the original child's exit was observed.
      if (!childExited) yield* signal("SIGKILL");
    }
  }).pipe(Effect.scoped, Effect.ignore, Effect.uninterruptible);
  yield* Scope.addFinalizer(scope, terminate);

  // Per-frame handlers share the connection span instead of tracing every token.
  const route = Effect.fnUntraced(function* (record: OmpRpcRecord) {
    if (!sawReady) {
      if (record.type !== "ready")
        return yield* new OmpRpcError({
          operation: "ready",
          detail: "Expected OMP rpc-ui ready frame",
        });
      sawReady = true;
      yield* Deferred.succeed(ready, record);
      return;
    }
    if (record.type === "response" && typeof record.id === "string") {
      const request = pending.get(record.id);
      if (request) {
        pending.delete(record.id);
        if (record.command !== request.command) {
          const error = new OmpRpcError({
            operation: "protocol",
            detail: "Response command does not match its request",
          });
          yield* Deferred.fail(request.deferred, error);
          return yield* error;
        }
        if (record.success === true) {
          if (request.command === "negotiate_protocol")
            negotiated = ompRecordNumber(record.data, "protocolVersion") === 2;
          yield* Deferred.succeed(request.deferred, record.data);
        } else {
          yield* Deferred.fail(
            request.deferred,
            new OmpRpcError({
              operation: request.command,
              detail:
                typeof record.error === "string"
                  ? record.error.slice(0, 500)
                  : "OMP rejected request",
              cause: record.error,
            }),
          );
        }
        return;
      }
    }
    yield* Queue.offer(events, record);
  });

  yield* child.exitCode.pipe(
    Effect.matchEffect({
      onFailure: (cause) => {
        const error = new OmpRpcError({ operation: "exit", cause });
        return Deferred.fail(exit, error).pipe(Effect.andThen(fail(error)));
      },
      onSuccess: Effect.fn("OmpRpc.observeExit")(function* (code: ChildProcessSpawner.ExitCode) {
        childExited = true;
        yield* Deferred.succeed(exit, Number(code));
        yield* fail(
          new OmpRpcError({
            operation: "exit",
            detail: `OMP process exited with code ${Number(code)}`,
          }),
        );
      }),
    }),
    Effect.forkIn(scope),
  );

  yield* Effect.gen(function* () {
    const decoder = new OmpFrameDecoder();
    const utf8 = new TextDecoder("utf-8", { fatal: true });
    let buffer = "";
    let bufferedBytes = 0;
    const consume = Effect.fnUntraced(function* (text: string) {
      let start = 0;
      while (start < text.length) {
        const newline = text.indexOf("\n", start);
        const part = text.slice(start, newline < 0 ? text.length : newline);
        bufferedBytes += Buffer.byteLength(part);
        if (bufferedBytes + 1 > MAX_OMP_FRAME_BYTES)
          return yield* new OmpRpcError({
            operation: "protocol",
            detail: "OMP frame exceeds transport limit",
          });
        buffer += part;
        if (newline < 0) break;
        if (buffer.endsWith("\r")) buffer = buffer.slice(0, -1);
        if (buffer.length > 0) {
          const record = yield* Effect.try({
            try: () => {
              const parsed = decodeRecord(buffer);
              if (parsed.type === "rpc_chunk" && !negotiated)
                throw new Error("OMP chunk before protocol-v2 negotiation");
              return decoder.push(parsed);
            },
            catch: (cause) =>
              new OmpRpcError({ operation: "protocol", detail: "Malformed OMP RPC frame", cause }),
          });
          if (record) yield* route(record);
        }
        buffer = "";
        bufferedBytes = 0;
        start = newline + 1;
      }
    });
    yield* child.stdout.pipe(
      Stream.runForEach((bytes) =>
        Effect.try({
          try: () => utf8.decode(bytes, { stream: true }),
          catch: (cause) =>
            new OmpRpcError({ operation: "protocol", detail: "Invalid OMP UTF-8", cause }),
        }).pipe(Effect.flatMap(consume)),
      ),
    );
    yield* consume(
      yield* Effect.try({
        try: () => utf8.decode(),
        catch: (cause) => new OmpRpcError({ operation: "protocol", cause }),
      }),
    );
    if (buffer.length > 0) yield* consume("\n");
    yield* Effect.try({
      try: () => decoder.finish(),
      catch: (cause) =>
        new OmpRpcError({ operation: "protocol", detail: "Incomplete OMP frame at EOF", cause }),
    });
    yield* fail(new OmpRpcError({ operation: "read", detail: "OMP process closed stdout" }));
  }).pipe(
    Effect.catchCause((cause) => fail(new OmpRpcError({ operation: "read", cause }))),
    Effect.forkIn(scope),
  );
  yield* child.stderr.pipe(Stream.runDrain, Effect.ignore, Effect.forkIn(scope));
  yield* Stream.fromQueue(outgoing).pipe(
    Stream.run(child.stdin),
    Effect.catchCause((cause) => fail(new OmpRpcError({ operation: "write", cause }))),
    Effect.forkIn(scope),
  );

  const send = Effect.fn("OmpRpc.send")(function* (
    record: OmpRpcRecord,
  ): Effect.fn.Return<void, OmpRpcError> {
    const bytes = yield* Effect.try({
      try: () => new TextEncoder().encode(`${encodeRecord(record)}\n`),
      catch: (cause) => new OmpRpcError({ operation: "encode", cause }),
    });
    if (bytes.length > MAX_REASSEMBLED_BYTES)
      return yield* new OmpRpcError({
        operation: "write",
        detail: "OMP command exceeds transport limit",
      });
    const accepted = yield* Queue.offer(outgoing, bytes);
    if (!accepted) return yield* Deferred.await(stopped);
  });
  const request = Effect.fn("OmpRpc.request")(function* (
    record: OmpRpcRecord,
    timeoutMs = 30_000,
  ): Effect.fn.Return<unknown, OmpRpcError | OmpRpcTimeoutError> {
    const id = `t3-${nextId++}`;
    const command = typeof record.type === "string" ? record.type : "request";
    const deferred = yield* Deferred.make<unknown, OmpRpcError>();
    pending.set(id, { command, deferred });
    return yield* send({ ...record, id }).pipe(
      Effect.andThen(Effect.raceFirst(Deferred.await(deferred), Deferred.await(stopped))),
      Effect.timeoutOrElse({
        duration: Duration.millis(timeoutMs),
        orElse: () => Effect.fail(new OmpRpcTimeoutError({ operation: command, timeoutMs })),
      }),
      Effect.ensuring(Effect.sync(() => pending.delete(id))),
    );
  });
  const greeting = yield* Deferred.await(ready).pipe(
    Effect.timeoutOrElse({
      duration: Duration.seconds(15),
      orElse: () =>
        Effect.fail(
          new OmpRpcError({ operation: "ready", detail: "OMP rpc-ui did not become ready" }),
        ),
    }),
  );
  if (
    !Array.isArray(greeting.supportedProtocolVersions) ||
    !greeting.supportedProtocolVersions.includes(2) ||
    greeting.maxFrameBytes !== MAX_OMP_FRAME_BYTES ||
    greeting.maxReassembledFrameBytes !== MAX_REASSEMBLED_BYTES
  ) {
    return yield* new OmpRpcError({
      operation: "negotiate_protocol",
      detail: "OMP rpc-ui protocol v2 is required; upgrade OMP",
    });
  }
  yield* request({ type: "negotiate_protocol", protocolVersion: 2 });
  if (!negotiated)
    return yield* new OmpRpcError({
      operation: "negotiate_protocol",
      detail: "OMP did not accept protocol v2",
    });
  return {
    send,
    request,
    events,
    exited: Deferred.await(exit),
    terminate,
  } satisfies OmpRpcConnection;
});
