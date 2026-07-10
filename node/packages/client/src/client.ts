import {
  ClientDuplexStream,
  ClientReadableStream,
  ClientUnaryCall,
  ClientWritableStream,
  RequestCallback,
  statusToError,
} from "./call.js";
import { CallContext, createCallContext, statusForAbort } from "./context.js";
import { FetchTransport } from "./fetch-transport.js";
import { Metadata } from "./metadata.js";
import { ServiceError, Status } from "./status.js";
import type { StatusResult, Transport, TransportCallOptions } from "./transport.js";
import { WebSocketTransport } from "./ws-transport.js";
import { H2tsTransport } from "./h2ts-transport.js";

/** Per-call options, mirroring the subset of grpc-js `CallOptions` we support. */
export interface CallOptions {
  /** Absolute deadline: a Date or ms-since-epoch. */
  deadline?: Date | number;
  /** Abort signal (maps to CANCELLED). */
  signal?: AbortSignal;
}

/** Wire codec for application messages. */
export type Codec = "proto" | "json";

/** Where unary calls go: real gRPC over h2ts (proto default), or the buffered-trailer Fetch path. */
export type UnaryTransport = "h2ts" | "fetch";
/**
 * Where streaming calls go: real gRPC over h2ts, H2-multiplexed over one WebSocket
 * (proto default), or the custom `Frame` protocol, one WebSocket per stream.
 */
export type StreamTransport = "h2ts" | "ws";

export interface ClientOptions {
  /** Base URL, e.g. "http://localhost:8080" or "https://api.example.com". */
  baseUrl: string;
  maxMessageBytes?: number;
  /** Override the WebSocket constructor (node needs the `ws` package). */
  webSocketImpl?: typeof WebSocket;
  fetch?: typeof fetch;
  /** Message codec: binary protobuf (default) or JSON. */
  codec?: Codec;
  /** Multiplex streams over a pool of WebSockets. Off by default (one WS per stream). */
  multiplex?: boolean;
  /** WebSocket pool size when `multiplex` is set. Default 1. */
  poolSize?: number;
  /**
   * Where unary calls go. Proto default `"h2ts"` (real gRPC over one HTTP/2 tunnel);
   * `"fetch"` uses the buffered-trailer Fetch path. Forced to `"fetch"` for `codec: "json"`.
   */
  unary?: UnaryTransport;
  /**
   * Where streaming calls go. Proto default `"h2ts"` (H2-multiplexed over one WebSocket);
   * `"ws"` uses the custom Frame protocol, one WebSocket per stream. Forced to `"ws"`
   * for `codec: "json"`.
   */
  streaming?: StreamTransport;
}

/**
 * Resolve the per-client transport selection from the config, applying the JSON lock and
 * rejecting unsupported combinations. Exposed for testing.
 *
 * - `codec: "json"` is locked to `{ unary: "fetch", streaming: "ws" }` (JSON stays
 *   plaintext and never rides h2ts); combining it with an h2ts knob throws.
 * - proto defaults to `{ unary: "h2ts", streaming: "h2ts" }`; an explicit `streaming: "ws"`
 *   defaults unary to `"fetch"` (its only valid pairing).
 * - `{ unary: "h2ts", streaming: "ws" }` (both explicit) throws: an h2ts unary already opens
 *   the tunnel, so streaming should ride it too (or send unary over Fetch).
 */
export function resolveTransportSelection(options: {
  codec?: Codec;
  unary?: UnaryTransport;
  streaming?: StreamTransport;
}): { unary: UnaryTransport; streaming: StreamTransport } {
  if ((options.codec ?? "proto") === "json") {
    if (options.unary === "h2ts" || options.streaming === "h2ts") {
      throw new Error(
        "grpc-webnext: codec 'json' cannot use h2ts — JSON stays plaintext (unary over Fetch, one WebSocket per stream)",
      );
    }
    return { unary: "fetch", streaming: "ws" };
  }
  const streaming = options.streaming ?? "h2ts";
  // `ws` streaming pairs only with `fetch` unary, so default unary to match.
  const unary = options.unary ?? (streaming === "ws" ? "fetch" : "h2ts");
  if (unary === "h2ts" && streaming === "ws") {
    throw new Error(
      "grpc-webnext: { unary: 'h2ts', streaming: 'ws' } is unsupported — an h2ts unary opens the tunnel, so use streaming: 'h2ts', or unary: 'fetch' to keep streams on 'ws'",
    );
  }
  return { unary, streaming };
}

type Serialize<T> = (value: T) => Uint8Array;
type Deserialize<T> = (bytes: Uint8Array) => T;

/**
 * Base client, mirroring @grpc/grpc-js `Client`. Generated service stubs extend
 * this and call the `make*Request` methods. Unary uses Fetch; streaming uses
 * WebSocket — matching the grpc-webnext protocol split.
 */
export class Client {
  private readonly unaryTransport: Transport;
  private readonly streamTransport: Transport;

  constructor(options: ClientOptions) {
    const selection = resolveTransportSelection(options);
    // When either surface uses h2ts, share ONE H2Connection across both (H2 multiplexes).
    const h2ts =
      selection.unary === "h2ts" || selection.streaming === "h2ts"
        ? new H2tsTransport({ baseUrl: options.baseUrl, webSocketImpl: options.webSocketImpl })
        : undefined;

    this.unaryTransport =
      selection.unary === "h2ts"
        ? h2ts!
        : new FetchTransport({
            baseUrl: options.baseUrl,
            maxMessageBytes: options.maxMessageBytes,
            fetch: options.fetch,
            codec: options.codec,
          });

    this.streamTransport =
      selection.streaming === "h2ts"
        ? h2ts!
        : new WebSocketTransport({
            baseUrl: options.baseUrl,
            webSocketImpl: options.webSocketImpl,
            codec: options.codec,
            multiplex: options.multiplex,
            poolSize: options.poolSize,
          });
  }

  close(): void {
    this.unaryTransport.close();
    this.streamTransport.close();
  }

  makeUnaryRequest<Req, Res>(
    path: string,
    serialize: Serialize<Req>,
    deserialize: Deserialize<Res>,
    argument: Req,
    metadata: Metadata,
    options: CallOptions,
    callback: RequestCallback<Res>,
  ): ClientUnaryCall {
    const ctx = createCallContext(options?.deadline, options?.signal);
    const call = new ClientUnaryCall(() => ctx.abort());

    this.unaryTransport
      .unary(path, serialize(argument), transportOptions(metadata, ctx))
      .then((res) => {
        ctx.dispose();
        call.emit("metadata", res.headers);
        call.emit("status", res.status);
        const err = statusToError(res.status);
        if (err) callback(err);
        else callback(null, deserialize(res.message));
      })
      .catch((e) => {
        ctx.dispose();
        callback(errorForFailure(ctx.signal, e));
      });

    return call;
  }

  makeServerStreamRequest<Req, Res>(
    path: string,
    serialize: Serialize<Req>,
    deserialize: Deserialize<Res>,
    argument: Req,
    metadata: Metadata,
    options: CallOptions,
  ): ClientReadableStream<Res> {
    const ctx = createCallContext(options?.deadline, options?.signal);
    let stream!: ClientReadableStream<Res>;
    const call = this.streamTransport.startStream(
      path,
      transportOptions(metadata, ctx),
      streamHandlers(() => stream, deserialize, ctx),
    );
    stream = new ClientReadableStream<Res>(() => call.cancel());
    call.send(serialize(argument));
    call.halfClose();
    return stream;
  }

  makeClientStreamRequest<Req, Res>(
    path: string,
    serialize: Serialize<Req>,
    deserialize: Deserialize<Res>,
    metadata: Metadata,
    options: CallOptions,
    callback: RequestCallback<Res>,
  ): ClientWritableStream<Req> {
    const ctx = createCallContext(options?.deadline, options?.signal);
    let last: Res | undefined;
    const call = this.streamTransport.startStream(path, transportOptions(metadata, ctx), {
      onMessage: (bytes) => {
        last = deserialize(bytes);
      },
      onStatus: (status) => {
        ctx.dispose();
        const err = statusToError(status);
        if (err) callback(err);
        else callback(null, last);
      },
    });
    return new ClientWritableStream<Req>(call, serialize);
  }

  makeBidiStreamRequest<Req, Res>(
    path: string,
    serialize: Serialize<Req>,
    deserialize: Deserialize<Res>,
    metadata: Metadata,
    options: CallOptions,
  ): ClientDuplexStream<Req, Res> {
    const ctx = createCallContext(options?.deadline, options?.signal);
    let stream!: ClientDuplexStream<Req, Res>;
    const call = this.streamTransport.startStream(
      path,
      transportOptions(metadata, ctx),
      streamHandlers(() => stream, deserialize, ctx),
    );
    stream = new ClientDuplexStream<Req, Res>(call, serialize);
    return stream;
  }
}

function streamHandlers<Res>(
  getStream: () => ClientReadableStream<Res>,
  deserialize: Deserialize<Res>,
  ctx: CallContext,
) {
  return {
    onHeaders: (metadata: Metadata) => getStream().emit("metadata", metadata),
    onMessage: (bytes: Uint8Array) => getStream().emit("data", deserialize(bytes)),
    onStatus: (status: StatusResult) => {
      ctx.dispose();
      const stream = getStream();
      stream.emit("status", status);
      const err = statusToError(status);
      if (err) stream.emit("error", err);
      else stream.emit("end");
    },
  };
}

function transportOptions(metadata: Metadata, ctx: CallContext): TransportCallOptions {
  return {
    metadata: metadata ?? new Metadata(),
    timeoutMillis: ctx.timeoutMillis,
    signal: ctx.signal,
  };
}

/** Map a transport failure to a ServiceError. An aborted signal means the call
 * was cancelled or timed out (fetch rejects with the abort reason, not always an
 * AbortError), so classify by the signal, not the error shape. */
function errorForFailure(signal: AbortSignal, e: unknown): ServiceError {
  if (signal.aborted) {
    const status = statusForAbort(signal);
    return new ServiceError(status.code, status.details);
  }
  return new ServiceError(Status.UNKNOWN, e instanceof Error ? e.message : String(e));
}
