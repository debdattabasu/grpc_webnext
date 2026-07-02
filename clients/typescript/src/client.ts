import {
  ClientDuplexStream,
  ClientReadableStream,
  ClientUnaryCall,
  ClientWritableStream,
  RequestCallback,
  statusToError,
} from "./call.js";
import { FetchTransport } from "./fetch-transport.js";
import { Metadata } from "./metadata.js";
import { ServiceError, Status } from "./status.js";
import type { StatusResult, Transport, TransportCallOptions } from "./transport.js";
import { WebSocketTransport } from "./ws-transport.js";

/** Per-call options, mirroring the subset of grpc-js `CallOptions` we support. */
export interface CallOptions {
  /** Absolute deadline: a Date or ms-since-epoch. */
  deadline?: Date | number;
  /** Abort signal (maps to CANCELLED). */
  signal?: AbortSignal;
}

export interface ClientOptions {
  /** Base URL, e.g. "http://localhost:8080" or "https://api.example.com". */
  baseUrl: string;
  maxMessageBytes?: number;
  /** Override the WebSocket constructor (node needs the `ws` package). */
  webSocketImpl?: typeof WebSocket;
  fetch?: typeof fetch;
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
    this.unaryTransport = new FetchTransport({
      baseUrl: options.baseUrl,
      maxMessageBytes: options.maxMessageBytes,
      fetch: options.fetch,
    });
    this.streamTransport = new WebSocketTransport({
      baseUrl: options.baseUrl,
      webSocketImpl: options.webSocketImpl,
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
    const controller = new AbortController();
    const call = new ClientUnaryCall(() => controller.abort());
    const opts = toTransportOptions(metadata, options, controller.signal);

    this.unaryTransport
      .unary(path, serialize(argument), opts)
      .then((res) => {
        call.emit("metadata", res.headers);
        call.emit("status", res.status);
        const err = statusToError(res.status);
        if (err) callback(err);
        else callback(null, deserialize(res.message));
      })
      .catch((e) => callback(abortToError(e)));

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
    let stream!: ClientReadableStream<Res>;
    const call = this.streamTransport.startStream(
      path,
      toTransportOptions(metadata, options),
      streamHandlers(() => stream, deserialize),
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
    let last: Res | undefined;
    const call = this.streamTransport.startStream(path, toTransportOptions(metadata, options), {
      onMessage: (bytes) => {
        last = deserialize(bytes);
      },
      onStatus: (status) => {
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
    let stream!: ClientDuplexStream<Req, Res>;
    const call = this.streamTransport.startStream(
      path,
      toTransportOptions(metadata, options),
      streamHandlers(() => stream, deserialize),
    );
    stream = new ClientDuplexStream<Req, Res>(call, serialize);
    return stream;
  }
}

function streamHandlers<Res>(
  getStream: () => ClientReadableStream<Res>,
  deserialize: Deserialize<Res>,
) {
  return {
    onHeaders: (metadata: Metadata) => getStream().emit("metadata", metadata),
    onMessage: (bytes: Uint8Array) => getStream().emit("data", deserialize(bytes)),
    onStatus: (status: StatusResult) => {
      const stream = getStream();
      stream.emit("status", status);
      const err = statusToError(status);
      if (err) stream.emit("error", err);
      else stream.emit("end");
    },
  };
}

function toTransportOptions(
  metadata: Metadata,
  options: CallOptions,
  signal?: AbortSignal,
): TransportCallOptions {
  return {
    metadata: metadata ?? new Metadata(),
    timeoutMillis: resolveTimeout(options?.deadline),
    signal: signal ?? options?.signal,
  };
}

function resolveTimeout(deadline?: Date | number): number | undefined {
  if (deadline === undefined) return undefined;
  const ms = (deadline instanceof Date ? deadline.getTime() : deadline) - Date.now();
  return ms > 0 ? ms : 1;
}

function abortToError(e: unknown): ServiceError {
  const isAbort = e instanceof Error && e.name === "AbortError";
  return new ServiceError(
    isAbort ? Status.CANCELLED : Status.UNKNOWN,
    e instanceof Error ? e.message : String(e),
  );
}
