import { assert, describe, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Deferred from "effect/Deferred";
import type * as Cause from "effect/Cause";
import * as Fiber from "effect/Fiber";
import * as Queue from "effect/Queue";
import * as Result from "effect/Result";
import * as Sink from "effect/Sink";
import * as Stream from "effect/Stream";
import * as ChildProcessSpawner from "effect/process/ChildProcessSpawner";
import {
  OmpFrameDecoder,
  MAX_OMP_FRAME_BYTES,
  makeOmpRpcConnection,
  type OmpRpcRecord,
} from "./rpc.ts";

function chunks(value: Record<string, unknown>) {
  const bytes = Buffer.from(JSON.stringify(value));
  const size = 256 * 1024;
  return Array.from({ length: Math.ceil(bytes.length / size) }, (_, index) => ({
    type: "rpc_chunk",
    chunkId: "frame-1",
    index,
    count: Math.ceil(bytes.length / size),
    byteLength: bytes.length,
    data: bytes.subarray(index * size, (index + 1) * size).toString("base64"),
  }));
}

function failureOf<A, E>(result: Result.Result<A, E>): E {
  if (Result.isSuccess(result)) throw new Error("Expected a protocol failure");
  return result.failure;
}

const makeTransportFixture = Effect.gen(function* () {
  const output = yield* Queue.unbounded<Uint8Array, Cause.Done>();
  const commands = yield* Queue.unbounded<OmpRpcRecord>();
  const exit = yield* Deferred.make<ChildProcessSpawner.ExitCode>();
  const released = yield* Deferred.make<void>();
  let releaseCount = 0;
  const emit = (record: OmpRpcRecord) =>
    Queue.offer(output, new TextEncoder().encode(`${JSON.stringify(record)}\n`)).pipe(
      Effect.asVoid,
    );
  const spawner = ChildProcessSpawner.make(() =>
    Effect.acquireRelease(
      Effect.succeed(
        ChildProcessSpawner.makeHandle({
          // Not a real pid: the transport cannot signal any process during fixture cleanup.
          pid: ChildProcessSpawner.ProcessId(999_999_999),
          exitCode: Deferred.await(exit),
          isRunning: Effect.succeed(true),
          kill: () => Effect.void,
          unref: Effect.succeed(Effect.void),
          stdin: Sink.forEach((bytes: Uint8Array) =>
            Effect.gen(function* () {
              const command = JSON.parse(new TextDecoder().decode(bytes)) as OmpRpcRecord;
              if (command.type === "negotiate_protocol") {
                yield* emit({
                  type: "response",
                  id: command.id,
                  command: command.type,
                  success: true,
                  data: { protocolVersion: 2 },
                });
              } else yield* Queue.offer(commands, command);
            }),
          ),
          stdout: Stream.fromQueue(output),
          stderr: Stream.empty,
          all: Stream.empty,
          getInputFd: () => Sink.drain,
          getOutputFd: () => Stream.empty,
        }),
      ),
      () =>
        Effect.gen(function* () {
          releaseCount++;
          yield* Deferred.succeed(released, undefined);
        }),
    ),
  );
  yield* emit({
    type: "ready",
    protocolVersion: 1,
    supportedProtocolVersions: [1, 2],
    maxFrameBytes: MAX_OMP_FRAME_BYTES,
    maxReassembledFrameBytes: 64 * 1024 * 1024,
  });
  const connect = makeOmpRpcConnection({
    command: "omp",
    args: ["--mode", "rpc-ui"],
    cwd: undefined,
    env: {},
  }).pipe(Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, spawner));
  return { output, commands, exit, released, emit, connect, releaseCount: () => releaseCount };
});

describe("OMP protocol framing", () => {
  it("reassembles bounded UTF-8 frames and rejects corruption rather than dropping it", () => {
    const value = { type: "message_end", text: "a".repeat(MAX_OMP_FRAME_BYTES) + "😀\u2028" };
    const frames = chunks(value);
    const decoder = new OmpFrameDecoder();
    for (const frame of frames.slice(0, -1)) assert.isUndefined(decoder.push(frame));
    assert.deepEqual(decoder.push(frames.at(-1)!), value);
    decoder.finish();
    for (const broken of [
      { ...frames[0], index: 1 },
      { ...frames[0], data: "!!!!" },
      { ...frames[0], byteLength: 65 * 1024 * 1024 },
      { ...frames[0], count: 1_000_000 },
    ])
      assert.throws(() => new OmpFrameDecoder().push(broken));
    const partial = new OmpFrameDecoder();
    partial.push(frames[0]!);
    assert.throws(() => partial.finish(), /incomplete/i);
    assert.throws(() => partial.push({ type: "message_end" }), /interrupt/i);
    const duplicate = new OmpFrameDecoder();
    duplicate.push(frames[0]!);
    assert.throws(() => duplicate.push(frames[0]!), /sequence/i);
    const mismatch = new OmpFrameDecoder();
    mismatch.push(frames[0]!);
    assert.throws(() => mismatch.push({ ...frames[1], chunkId: "other" }), /sequence/i);
  });
  it.effect("correlates rejection errors and ends pending requests on EOF", () =>
    Effect.gen(function* () {
      const fixture = yield* makeTransportFixture;
      yield* Effect.scoped(
        Effect.gen(function* () {
          const connection = yield* fixture.connect;
          const rejected = yield* connection
            .request({ type: "set_model" })
            .pipe(Effect.result, Effect.forkChild);
          const command = yield* Queue.take(fixture.commands);
          yield* fixture.emit({
            type: "response",
            id: command.id,
            command: "set_model",
            success: false,
            error: "Unknown model",
          });
          const error = failureOf(yield* Fiber.join(rejected));
          assert.equal(error._tag, "OmpRpcError");
          assert.equal(error.operation, "set_model");
          const pending = yield* connection
            .request({ type: "get_state" })
            .pipe(Effect.result, Effect.forkChild);
          yield* Queue.take(fixture.commands);
          yield* Queue.end(fixture.output);
          assert.equal(failureOf(yield* Fiber.join(pending))._tag, "OmpRpcError");
          assert.equal(
            failureOf(yield* Queue.take(connection.events).pipe(Effect.result))._tag,
            "OmpRpcError",
          );
          yield* connection.terminate;
          yield* connection.terminate;
        }),
      );
      yield* Deferred.await(fixture.released);
      assert.equal(fixture.releaseCount(), 1);
    }),
  );
  it.effect("fails requests on process exit even if a descendant holds stdout open", () =>
    Effect.gen(function* () {
      const fixture = yield* makeTransportFixture;
      yield* Effect.scoped(
        Effect.gen(function* () {
          const connection = yield* fixture.connect;
          const pending = yield* connection
            .request({ type: "get_state" })
            .pipe(Effect.result, Effect.forkChild);
          yield* Queue.take(fixture.commands);
          yield* Deferred.succeed(fixture.exit, ChildProcessSpawner.ExitCode(19));
          assert.equal(yield* connection.exited, 19);
          const error = failureOf(yield* Fiber.join(pending));
          assert.equal(error._tag, "OmpRpcError");
          assert.equal(error.operation, "exit");
          assert.equal(
            failureOf(yield* connection.send({ type: "abort" }).pipe(Effect.result))._tag,
            "OmpRpcError",
          );
        }),
      );
      assert.equal(fixture.releaseCount(), 1);
    }),
  );
  it.effect("fails a corrupt chunk stream instead of leaving an outstanding request hung", () =>
    Effect.gen(function* () {
      const fixture = yield* makeTransportFixture;
      yield* Effect.scoped(
        Effect.gen(function* () {
          const connection = yield* fixture.connect;
          const pending = yield* connection
            .request({ type: "get_state" })
            .pipe(Effect.result, Effect.forkChild);
          yield* Queue.take(fixture.commands);
          yield* fixture.emit({
            type: "rpc_chunk",
            chunkId: "bad",
            index: 0,
            count: 5,
            byteLength: MAX_OMP_FRAME_BYTES,
            data: "!!!!",
          });
          assert.equal(failureOf(yield* Fiber.join(pending))._tag, "OmpRpcError");
        }),
      );
    }),
  );
});
