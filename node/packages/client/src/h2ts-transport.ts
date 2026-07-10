// The binary path over real HTTP/2, tunneled through a WebSocket by h2ts
// (@debdattabasu/h2ts). Both unary and streaming ride ONE multiplexed H2Connection
// — this is the `{ encoding: proto, unary: h2ts, streaming: h2ts }` default. The
// server is unmodified tonic behind an h2ts gateway, so we speak real gRPC:
// 5-byte length-prefixed messages, status in HTTP/2 trailers, metadata as headers.
import { connectWebSocket } from "@debdattabasu/h2ts";
import type { H2Connection, H2Response, WebSocketConnectOptions } from "@debdattabasu/h2ts";

import { statusForAbort } from "./context.js";
import { Metadata } from "./metadata.js";
import { Status } from "./status.js";
import type {
  StatusResult,
  StreamCall,
  StreamHandlers,
  Transport,
  TransportCallOptions,
  UnaryResponse,
} from "./transport.js";

export interface H2tsTransportOptions {
  baseUrl: string;
  /** Node needs a WebSocket implementation (the `ws` package); browsers use the global. */
  webSocketImpl?: typeof WebSocket;
}

export class H2tsTransport implements Transport {
  private readonly wsUrl: string;
  private readonly authority: string;
  private readonly webSocketImpl?: typeof WebSocket;
  private connection: Promise<H2Connection> | null = null;
  private closed = false;

  constructor(options: H2tsTransportOptions) {
    const url = new URL(options.baseUrl);
    this.authority = url.host;
    // The tunnelled HTTP/2 is always h2c (cleartext); the outer WebSocket carries
    // transport security. So ws<->http, wss<->https on the outside.
    const wsScheme = url.protocol === "https:" ? "wss:" : "ws:";
    this.wsUrl = `${wsScheme}//${url.host}${url.pathname.replace(/\/$/, "")}`;
    this.webSocketImpl = options.webSocketImpl;
  }

  /** One lazily-opened H2 connection, reused (and H2-multiplexed) across all calls. */
  private conn(): Promise<H2Connection> {
    if (!this.connection) {
      const opts: WebSocketConnectOptions = {
        WebSocket: this.webSocketImpl as WebSocketConnectOptions["WebSocket"],
      };
      this.connection = connectWebSocket(this.wsUrl, opts);
    }
    return this.connection;
  }

  async unary(
    path: string,
    request: Uint8Array,
    options: TransportCallOptions,
  ): Promise<UnaryResponse> {
    const conn = await this.conn();
    const res = await conn.request({
      method: "POST",
      path,
      authority: this.authority,
      scheme: "http",
      headers: buildHeaders(options),
      body: encodeMessage(request),
      signal: options.signal,
    });
    const body = await res.bytes(); // consume the body so trailers() is populated
    const messages = new GrpcFrameParser().push(body);
    return {
      message: messages[0] ?? new Uint8Array(0),
      headers: metadataFromRecord(res.headers),
      status: statusFromResponse(res),
    };
  }

  startStream(
    path: string,
    options: TransportCallOptions,
    handlers: StreamHandlers,
  ): StreamCall {
    // One controller drives cancellation — fired by the call's signal (deadline or user
    // abort, which the client layer merges into `options.signal`) or by an explicit cancel().
    const controller = new AbortController();
    if (options.signal) {
      if (options.signal.aborted) controller.abort();
      else options.signal.addEventListener("abort", () => controller.abort(), { once: true });
    }
    // The request body is a stream we push into via `send()` and close via `halfClose()`,
    // so h2ts can pump it full-duplex while we read the response — real bidi.
    let sink!: ReadableStreamDefaultController<Uint8Array>;
    const requestBody = new ReadableStream<Uint8Array>({ start: (c) => (sink = c) });
    let bodyClosed = false;
    const closeBody = () => {
      if (bodyClosed) return;
      bodyClosed = true;
      try {
        sink.close();
      } catch {
        // already closed/errored
      }
    };

    void (async () => {
      try {
        const conn = await this.conn();
        const res = await conn.request({
          method: "POST",
          path,
          authority: this.authority,
          scheme: "http",
          headers: buildHeaders(options),
          body: requestBody,
          signal: controller.signal,
        });
        handlers.onHeaders?.(metadataFromRecord(res.headers));
        const reader = res.body.getReader();
        const parser = new GrpcFrameParser();
        for (;;) {
          const { value, done } = await reader.read();
          if (done) break;
          if (value) for (const message of parser.push(value)) handlers.onMessage?.(message);
        }
        handlers.onStatus?.(statusFromResponse(res));
      } catch (e) {
        handlers.onStatus?.(abortOrUnknown(controller.signal, options.signal, e));
      }
    })();

    return {
      send: (message: Uint8Array) => {
        if (!bodyClosed) sink.enqueue(encodeMessage(message));
      },
      halfClose: closeBody,
      cancel: () => {
        controller.abort();
        closeBody();
      },
    };
  }

  close(): void {
    if (this.closed) return;
    this.closed = true;
    const conn = this.connection;
    this.connection = null;
    if (conn) void conn.then((c) => c.close()).catch(() => {});
  }
}

// --- gRPC framing: [1-byte compression flag | u32 big-endian length | message] --------

function encodeMessage(message: Uint8Array): Uint8Array {
  const out = new Uint8Array(5 + message.byteLength);
  out[0] = 0; // no compression
  new DataView(out.buffer).setUint32(1, message.byteLength, false); // big-endian length
  out.set(message, 5);
  return out;
}

/** Incrementally reassembles length-prefixed gRPC messages from a byte stream. */
class GrpcFrameParser {
  private buf = new Uint8Array(0);

  push(chunk: Uint8Array): Uint8Array[] {
    if (chunk.byteLength > 0) {
      const merged = new Uint8Array(this.buf.byteLength + chunk.byteLength);
      merged.set(this.buf, 0);
      merged.set(chunk, this.buf.byteLength);
      this.buf = merged;
    }
    const out: Uint8Array[] = [];
    let offset = 0;
    for (;;) {
      if (this.buf.byteLength - offset < 5) break;
      const len = new DataView(this.buf.buffer, this.buf.byteOffset + offset, 5).getUint32(1, false);
      if (this.buf.byteLength - offset - 5 < len) break;
      out.push(this.buf.slice(offset + 5, offset + 5 + len));
      offset += 5 + len;
    }
    this.buf = this.buf.subarray(offset);
    return out;
  }
}

// --- headers / status ------------------------------------------------------------------

const GRPC_CONTENT_TYPE = "application/grpc+proto";
const CONTROL_HEADERS = new Set(["grpc-status", "grpc-message", "grpc-status-details-bin"]);

function buildHeaders(options: TransportCallOptions): Array<[string, string]> {
  const headers: Array<[string, string]> = [
    ["content-type", GRPC_CONTENT_TYPE],
    ["te", "trailers"],
  ];
  if (options.timeoutMillis && options.timeoutMillis > 0) {
    headers.push(["grpc-timeout", `${Math.ceil(options.timeoutMillis)}m`]); // "m" = milliseconds
  }
  options.metadata.toHeaders().forEach((value, key) => {
    if (key !== "content-type" && key !== "te" && key !== "grpc-timeout") {
      headers.push([key, value]);
    }
  });
  return headers;
}

function metadataFromRecord(record: Record<string, string>): Metadata {
  const headers = new Headers();
  for (const [key, value] of Object.entries(record)) {
    if (CONTROL_HEADERS.has(key) || key.startsWith(":")) continue;
    headers.append(key, value);
  }
  return Metadata.fromHeaders(headers);
}

function statusFromResponse(res: H2Response): StatusResult {
  // Status lives in the trailers; a trailers-only (error) response puts it in the headers.
  const trailers = res.trailers();
  const fromTrailers = readStatus(trailers);
  const raw = fromTrailers ?? readStatus(res.headers);
  if (raw) {
    // A trailers-only response carries its trailing metadata in the headers block, not a
    // separate trailers block — read the trailing metadata from wherever the status came.
    const source = fromTrailers ? trailers : res.headers;
    return { code: raw.code, details: raw.message, metadata: metadataFromRecord(source ?? {}) };
  }
  if (res.status === 200) return { code: Status.OK, details: "", metadata: new Metadata() };
  return { code: httpToStatus(res.status), details: `HTTP ${res.status}`, metadata: new Metadata() };
}

/** Map a failed streaming call to a status: deadline vs cancel (via the call signal), else UNKNOWN. */
function abortOrUnknown(
  effective: AbortSignal,
  userSignal: AbortSignal | undefined,
  error: unknown,
): StatusResult {
  if (effective.aborted) {
    return userSignal?.aborted
      ? statusForAbort(userSignal)
      : { code: Status.CANCELLED, details: "cancelled", metadata: new Metadata() };
  }
  return {
    code: Status.UNKNOWN,
    details: error instanceof Error ? error.message : String(error),
    metadata: new Metadata(),
  };
}

function readStatus(source: Record<string, string> | undefined): { code: Status; message: string } | null {
  const raw = source?.["grpc-status"];
  if (raw === undefined) return null;
  const code = Number.parseInt(raw, 10);
  return {
    code: (Number.isFinite(code) ? code : Status.UNKNOWN) as Status,
    message: decodeGrpcMessage(source?.["grpc-message"] ?? ""),
  };
}

/** grpc-message is percent-encoded (RFC 3986-ish) on the wire. */
function decodeGrpcMessage(value: string): string {
  try {
    return decodeURIComponent(value);
  } catch {
    return value;
  }
}

function httpToStatus(status: number): Status {
  switch (status) {
    case 400:
      return Status.INTERNAL;
    case 401:
      return Status.UNAUTHENTICATED;
    case 403:
      return Status.PERMISSION_DENIED;
    case 404:
      return Status.UNIMPLEMENTED;
    case 429:
    case 502:
    case 503:
    case 504:
      return Status.UNAVAILABLE;
    default:
      return Status.UNKNOWN;
  }
}
