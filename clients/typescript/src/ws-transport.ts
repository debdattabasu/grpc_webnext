import { statusForAbort } from "./context.js";
import { decodeFrame, encodeFrame } from "./frame.js";
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

export interface WebSocketTransportOptions {
  /** Base URL, e.g. "http://localhost:8080"; scheme is mapped to ws/wss. */
  baseUrl: string;
  /** Number of WebSockets in the multiplex pool (streams round-robin). Default 1. */
  poolSize?: number;
  /** Override the WebSocket constructor (node needs the `ws` package). */
  webSocketImpl?: typeof WebSocket;
  /** Message codec: "proto" (default) or "json". Sets `Subscribe.json`. */
  codec?: "proto" | "json";
}

/** Streaming transport over WebSocket, with a client-side multiplex pool. */
export class WebSocketTransport implements Transport {
  private readonly url: string;
  private readonly WS: typeof WebSocket;
  private readonly pool: Conn[] = [];
  private readonly poolSize: number;
  private readonly json: boolean;
  private next = 0;

  constructor(options: WebSocketTransportOptions) {
    this.url = toWsUrl(options.baseUrl);
    this.poolSize = Math.max(1, options.poolSize ?? 1);
    this.json = options.codec === "json";
    const impl = options.webSocketImpl ?? (globalThis as any).WebSocket;
    if (!impl) {
      throw new Error(
        "no WebSocket implementation; pass webSocketImpl (e.g. the `ws` package) in node",
      );
    }
    this.WS = impl;
  }

  unary(_path: string, _request: Uint8Array, _options: TransportCallOptions): Promise<UnaryResponse> {
    return Promise.reject(new Error("WebSocketTransport handles streaming only; unary uses Fetch"));
  }

  startStream(path: string, options: TransportCallOptions, handlers: StreamHandlers): StreamCall {
    const conn = this.pickConn();
    return conn.open(path, options, handlers);
  }

  close(): void {
    for (const conn of this.pool) conn.close();
    this.pool.length = 0;
  }

  private pickConn(): Conn {
    if (this.pool.length < this.poolSize) {
      const conn = new Conn(this.url, this.WS, this.json);
      this.pool.push(conn);
      return conn;
    }
    const conn = this.pool[this.next % this.pool.length];
    this.next++;
    return conn;
  }
}

/** One WebSocket carrying multiple logical streams keyed by stream_id. */
class Conn {
  private readonly ws: WebSocket;
  private readonly streams = new Map<number, StreamHandlers>();
  private readonly pending: (string | Uint8Array)[] = [];
  private readonly enc = new TextEncoder();
  private readonly dec = new TextDecoder();
  private open_ = false;
  private nextStreamId = 1;

  constructor(
    url: string,
    WS: typeof WebSocket,
    private readonly json: boolean,
  ) {
    this.ws = new WS(url);
    this.ws.binaryType = "arraybuffer";
    this.ws.addEventListener("open", () => {
      this.open_ = true;
      for (const frame of this.pending) this.ws.send(frame);
      this.pending.length = 0;
    });
    this.ws.addEventListener("message", (ev: MessageEvent) => this.onMessage(ev));
    this.ws.addEventListener("close", () => this.onClose());
    this.ws.addEventListener("error", () => this.onClose());
  }

  open(path: string, options: TransportCallOptions, handlers: StreamHandlers): StreamCall {
    const streamId = this.nextStreamId++;
    this.streams.set(streamId, handlers);

    const timeoutMillis = options.timeoutMillis ? Math.ceil(options.timeoutMillis) : 0;
    if (this.json) {
      // Flat open frame: has `method`.
      const frame: Record<string, unknown> = { streamId, method: path };
      const metadata = metaToJson(options.metadata);
      if (Object.keys(metadata).length) frame.metadata = metadata;
      if (timeoutMillis) frame.timeoutMillis = timeoutMillis;
      this.sendRaw(JSON.stringify(frame));
    } else {
      this.sendRaw(
        encodeFrame({
          subscribe: {
            streamId,
            method: path,
            headers: options.metadata.toMetadatumList(),
            timeoutMillis,
            initialPayload: new Uint8Array(),
            json: false,
          },
        }),
      );
    }

    // Terminate the stream: send a Reset and deliver the status locally so
    // consumers (for-await) stop.
    const terminate = (status: StatusResult) => {
      const handlers = this.streams.get(streamId);
      if (!handlers) return; // already finished/cancelled
      this.streams.delete(streamId);
      this.sendRaw(this.encodeReset(streamId, status));
      handlers.onStatus?.(status);
    };

    const cancel = () =>
      terminate({ code: Status.CANCELLED, details: "cancelled", metadata: new Metadata() });

    // AbortSignal -> Reset (the call's `context`). Deadline aborts report
    // DEADLINE_EXCEEDED; any other abort is CANCELLED.
    const signal = options.signal;
    if (signal) {
      const onAbort = () => terminate(statusForAbort(signal));
      if (signal.aborted) queueMicrotask(onAbort);
      else signal.addEventListener("abort", onAbort, { once: true });
    }

    return {
      send: (message) => this.sendRaw(this.encodeMessage(streamId, message)),
      halfClose: () => this.sendRaw(this.encodeHalfClose(streamId)),
      cancel,
    };
  }

  close(): void {
    try {
      this.ws.close();
    } catch {
      /* ignore */
    }
  }

  private sendRaw(frame: string | Uint8Array): void {
    if (this.open_) this.ws.send(frame);
    else this.pending.push(frame);
  }

  private encodeMessage(streamId: number, message: Uint8Array): string | Uint8Array {
    if (this.json) {
      return JSON.stringify({ streamId, message: JSON.parse(this.dec.decode(message)) });
    }
    return encodeFrame({ message: { streamId, payload: message } });
  }

  private encodeHalfClose(streamId: number): string | Uint8Array {
    return this.json
      ? JSON.stringify({ streamId, halfClose: true })
      : encodeFrame({ halfClose: { streamId } });
  }

  private encodeReset(streamId: number, status: StatusResult): string | Uint8Array {
    return this.json
      ? JSON.stringify({ streamId, status: { code: status.code, message: status.details } })
      : encodeFrame({ reset: { streamId, statusCode: status.code, statusMessage: status.details } });
  }

  private onMessage(ev: MessageEvent): void {
    const data = ev.data;
    if (typeof data === "string") {
      this.onJsonFrame(data);
      return;
    }
    const bytes =
      data instanceof ArrayBuffer
        ? new Uint8Array(data)
        : ArrayBuffer.isView(data)
          ? new Uint8Array(data.buffer, data.byteOffset, data.byteLength)
          : null;
    if (!bytes) return;
    const frame = decodeFrame(bytes);

    if (frame.header) {
      this.streams
        .get(frame.header.streamId)
        ?.onHeaders?.(Metadata.fromMetadatumList(frame.header.headers));
    } else if (frame.message) {
      this.streams.get(frame.message.streamId)?.onMessage?.(frame.message.payload);
    } else if (frame.trailer) {
      const t = frame.trailer;
      this.deliverStatus(t.streamId, {
        code: t.statusCode as Status,
        details: t.statusMessage,
        metadata: Metadata.fromMetadatumList(t.trailers),
      });
    } else if (frame.reset) {
      const r = frame.reset;
      this.deliverStatus(r.streamId, {
        code: r.statusCode as Status,
        details: r.statusMessage,
        metadata: new Metadata(),
      });
    }
  }

  /** Handle an inbound JSON text frame (the `+json` codec). */
  private onJsonFrame(text: string): void {
    let jf: any;
    try {
      jf = JSON.parse(text);
    } catch {
      return;
    }
    const streamId: number = jf.streamId;
    if (jf.status) {
      // Terminal: trailer / reset.
      this.deliverStatus(streamId, {
        code: jf.status.code as Status,
        details: jf.status.message ?? "",
        metadata: jsonToMeta(jf.metadata),
      });
    } else if (jf.message !== undefined) {
      // Re-serialize the native JSON message to bytes for the deserializer.
      this.streams.get(streamId)?.onMessage?.(this.enc.encode(JSON.stringify(jf.message)));
    } else if (jf.metadata) {
      // Initial response metadata (header).
      this.streams.get(streamId)?.onHeaders?.(jsonToMeta(jf.metadata));
    }
  }

  private deliverStatus(streamId: number, status: StatusResult): void {
    const handlers = this.streams.get(streamId);
    this.streams.delete(streamId);
    handlers?.onStatus?.(status);
  }

  private onClose(): void {
    // Fail any still-open streams.
    const status: StatusResult = {
      code: Status.UNAVAILABLE,
      details: "websocket closed",
      metadata: new Metadata(),
    };
    for (const [id, handlers] of [...this.streams]) {
      this.streams.delete(id);
      handlers.onStatus?.(status);
    }
    this.open_ = false;
  }
}

function toWsUrl(baseUrl: string): string {
  const url = new URL(baseUrl);
  url.protocol = url.protocol === "https:" ? "wss:" : "ws:";
  return url.toString();
}

/** Metadata -> JSON object (ASCII values only) for a JSON frame. */
function metaToJson(md: Metadata): Record<string, string> {
  const out: Record<string, string> = {};
  for (const [k, v] of Object.entries(md.getMap())) {
    if (typeof v === "string") out[k] = v;
  }
  return out;
}

/** JSON object -> Metadata. */
function jsonToMeta(obj?: Record<string, string>): Metadata {
  const md = new Metadata();
  for (const [k, v] of Object.entries(obj ?? {})) md.set(k, v);
  return md;
}
