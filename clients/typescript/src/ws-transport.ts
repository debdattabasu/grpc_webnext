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

/** Base subprotocol; the codec (and `+multi`) are appended. */
const WS_SUBPROTOCOL = "grpc-webnext";

export interface WebSocketTransportOptions {
  /** Base URL, e.g. "http://localhost:8080"; scheme is mapped to ws/wss. */
  baseUrl: string;
  /** Override the WebSocket constructor (node needs the `ws` package). */
  webSocketImpl?: typeof WebSocket;
  /** Message codec: "proto" (default) or "json". */
  codec?: "proto" | "json";
  /**
   * Multiplex many streams over a pool of WebSockets (`+multi` subprotocol). Off by
   * default — each stream gets its own WebSocket connected to the method's URL, and
   * JSON frames are human-readable (no `streamId`/`method`).
   */
  multiplex?: boolean;
  /** Number of WebSockets in the multiplex pool. Only used when `multiplex`. Default 1. */
  poolSize?: number;
}

/** Streaming transport over WebSocket. */
export class WebSocketTransport implements Transport {
  private readonly baseUrl: string;
  private readonly WS: typeof WebSocket;
  private readonly json: boolean;
  private readonly multiplex: boolean;
  private readonly poolSize: number;
  private readonly pool: MultiplexConn[] = [];
  private readonly singles = new Set<SingleStreamConn>();
  private next = 0;

  constructor(options: WebSocketTransportOptions) {
    this.baseUrl = options.baseUrl;
    this.json = options.codec === "json";
    this.multiplex = options.multiplex ?? false;
    this.poolSize = Math.max(1, options.poolSize ?? 1);
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
    if (this.multiplex) {
      // A new pooled socket carries this call's credential in its subprotocol.
      return this.pickConn(path, options.metadata).open(path, options, handlers);
    }
    // Single-stream: one WebSocket per stream, connected to the method's URL.
    const url = methodWsUrl(this.baseUrl, path);
    const conn = new SingleStreamConn(url, this.WS, this.json, options, handlers, () =>
      this.singles.delete(conn),
    );
    this.singles.add(conn);
    return conn.call;
  }

  close(): void {
    for (const conn of this.pool) conn.close();
    this.pool.length = 0;
    for (const conn of [...this.singles]) conn.close();
    this.singles.clear();
  }

  private pickConn(path: string, metadata: Metadata): MultiplexConn {
    if (this.pool.length < this.poolSize) {
      const bearer = bearerSubprotocol(metadata);
      // A pooled socket carrying a credential passes the opening call's method as
      // a query param, so the server can authenticate the credential against it.
      const base = toWsUrl(this.baseUrl);
      const url = bearer ? `${base}?method=${encodeURIComponent(path)}` : base;
      const conn = new MultiplexConn(url, this.WS, this.json, bearer);
      this.pool.push(conn);
      return conn;
    }
    const conn = this.pool[this.next % this.pool.length];
    this.next++;
    return conn;
  }
}

/**
 * One WebSocket carrying exactly one stream. The method is the WS URL, so JSON
 * frames omit `streamId`/`method`.
 */
class SingleStreamConn {
  private readonly ws: WebSocket;
  private readonly enc = new TextEncoder();
  private readonly dec = new TextDecoder();
  private readonly pending: (string | Uint8Array)[] = [];
  private open_ = false;
  private finished = false;
  readonly call: StreamCall;

  constructor(
    url: string,
    WS: typeof WebSocket,
    private readonly json: boolean,
    options: TransportCallOptions,
    private readonly handlers: StreamHandlers,
    private readonly onDone: () => void,
  ) {
    const codecSub = json ? `${WS_SUBPROTOCOL}+json` : `${WS_SUBPROTOCOL}+proto`;
    const protocols = [WS_SUBPROTOCOL, codecSub];
    // Connection-level auth: this call's `authorization` metadata rides in the
    // subprotocol so the server can hard-reject at the handshake (before any frame).
    const bearer = bearerSubprotocol(options.metadata);
    if (bearer) protocols.push(bearer);
    this.ws = new WS(url, protocols);
    this.ws.binaryType = "arraybuffer";
    this.ws.addEventListener("open", () => {
      this.open_ = true;
      for (const frame of this.pending) this.ws.send(frame);
      this.pending.length = 0;
    });
    this.ws.addEventListener("message", (ev: MessageEvent) => this.onMessage(ev));
    this.ws.addEventListener("close", () => this.onClose());
    this.ws.addEventListener("error", () => this.onClose());

    // Eager open frame: carries metadata/deadline and unambiguously starts the stream.
    this.sendOpen(options);

    const signal = options.signal;
    if (signal) {
      const onAbort = () => this.terminate(statusForAbort(signal));
      if (signal.aborted) queueMicrotask(onAbort);
      else signal.addEventListener("abort", onAbort, { once: true });
    }

    this.call = {
      send: (message) => this.sendRaw(this.encodeMessage(message)),
      halfClose: () => this.sendRaw(this.encodeHalfClose()),
      cancel: () =>
        this.terminate({ code: Status.CANCELLED, details: "cancelled", metadata: new Metadata() }),
    };
  }

  close(): void {
    try {
      this.ws.close();
    } catch {
      /* ignore */
    }
  }

  private sendOpen(options: TransportCallOptions): void {
    const timeoutMillis = options.timeoutMillis ? Math.ceil(options.timeoutMillis) : 0;
    if (this.json) {
      const open: Record<string, unknown> = {};
      const metadata = metaToJson(options.metadata);
      if (Object.keys(metadata).length) open.metadata = metadata;
      if (timeoutMillis) open.timeoutMillis = timeoutMillis;
      this.sendRaw(JSON.stringify(open));
    } else {
      this.sendRaw(
        encodeFrame({
          subscribe: {
            streamId: 1,
            method: "", // ignored by the server; taken from the URL
            headers: options.metadata.toMetadatumList(),
            timeoutMillis,
            initialPayload: new Uint8Array(),
            json: false,
          },
        }),
      );
    }
  }

  private encodeMessage(message: Uint8Array): string | Uint8Array {
    return this.json
      ? JSON.stringify({ message: JSON.parse(this.dec.decode(message)) })
      : encodeFrame({ message: { streamId: 1, payload: message } });
  }

  private encodeHalfClose(): string | Uint8Array {
    return this.json ? JSON.stringify({ halfClose: true }) : encodeFrame({ halfClose: { streamId: 1 } });
  }

  private encodeReset(status: StatusResult): string | Uint8Array {
    return this.json
      ? JSON.stringify({ status: { code: status.code, message: status.details } })
      : encodeFrame({ reset: { streamId: 1, statusCode: status.code, statusMessage: status.details } });
  }

  private sendRaw(frame: string | Uint8Array): void {
    if (this.finished) return;
    if (this.open_) this.ws.send(frame);
    else this.pending.push(frame);
  }

  private terminate(status: StatusResult): void {
    if (this.finished) return;
    this.sendRaw(this.encodeReset(status));
    this.finish(status);
    this.close();
  }

  private finish(status: StatusResult): void {
    if (this.finished) return;
    this.finished = true;
    this.onDone();
    this.handlers.onStatus?.(status);
  }

  private onMessage(ev: MessageEvent): void {
    if (this.finished) return;
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
      this.handlers.onHeaders?.(Metadata.fromMetadatumList(frame.header.headers));
    } else if (frame.message) {
      this.handlers.onMessage?.(frame.message.payload);
    } else if (frame.trailer) {
      const t = frame.trailer;
      this.finish({
        code: t.statusCode as Status,
        details: t.statusMessage,
        metadata: Metadata.fromMetadatumList(t.trailers),
      });
    } else if (frame.reset) {
      const r = frame.reset;
      this.finish({ code: r.statusCode as Status, details: r.statusMessage, metadata: new Metadata() });
    }
  }

  private onJsonFrame(text: string): void {
    let jf: any;
    try {
      jf = JSON.parse(text);
    } catch {
      return;
    }
    if (jf.status) {
      this.finish({
        code: jf.status.code as Status,
        details: jf.status.message ?? "",
        metadata: jsonToMeta(jf.metadata),
      });
    } else if (jf.message !== undefined) {
      this.handlers.onMessage?.(this.enc.encode(JSON.stringify(jf.message)));
    } else if (jf.metadata) {
      this.handlers.onHeaders?.(jsonToMeta(jf.metadata));
    }
  }

  private onClose(): void {
    this.open_ = false;
    this.finish({ code: Status.UNAVAILABLE, details: "websocket closed", metadata: new Metadata() });
  }
}

/** One WebSocket carrying multiple logical streams keyed by stream_id (`+multi`). */
class MultiplexConn {
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
    bearer?: string,
  ) {
    const codecSub = json ? `${WS_SUBPROTOCOL}+json+multi` : `${WS_SUBPROTOCOL}+proto+multi`;
    const protocols = [WS_SUBPROTOCOL, codecSub];
    // The subprotocol credential (if any) gates this pooled socket at the handshake.
    if (bearer) protocols.push(bearer);
    this.ws = new WS(url, protocols);
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

    const terminate = (status: StatusResult) => {
      const h = this.streams.get(streamId);
      if (!h) return;
      this.streams.delete(streamId);
      this.sendRaw(this.encodeReset(streamId, status));
      h.onStatus?.(status);
    };

    const signal = options.signal;
    if (signal) {
      const onAbort = () => terminate(statusForAbort(signal));
      if (signal.aborted) queueMicrotask(onAbort);
      else signal.addEventListener("abort", onAbort, { once: true });
    }

    return {
      send: (message) => this.sendRaw(this.encodeMessage(streamId, message)),
      halfClose: () => this.sendRaw(this.encodeHalfClose(streamId)),
      cancel: () =>
        terminate({ code: Status.CANCELLED, details: "cancelled", metadata: new Metadata() }),
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

  private onJsonFrame(text: string): void {
    let jf: any;
    try {
      jf = JSON.parse(text);
    } catch {
      return;
    }
    const streamId: number = jf.streamId;
    if (jf.status) {
      this.deliverStatus(streamId, {
        code: jf.status.code as Status,
        details: jf.status.message ?? "",
        metadata: jsonToMeta(jf.metadata),
      });
    } else if (jf.message !== undefined) {
      this.streams.get(streamId)?.onMessage?.(this.enc.encode(JSON.stringify(jf.message)));
    } else if (jf.metadata) {
      this.streams.get(streamId)?.onHeaders?.(jsonToMeta(jf.metadata));
    }
  }

  private deliverStatus(streamId: number, status: StatusResult): void {
    const handlers = this.streams.get(streamId);
    this.streams.delete(streamId);
    handlers?.onStatus?.(status);
  }

  private onClose(): void {
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

/** Map an http(s) base URL to its ws(s) form (base path). */
function toWsUrl(baseUrl: string): string {
  const url = new URL(baseUrl);
  url.protocol = url.protocol === "https:" ? "wss:" : "ws:";
  return url.toString();
}

/** Build the ws(s) URL for a method path (single-stream mode). */
function methodWsUrl(baseUrl: string, path: string): string {
  const url = new URL(path, baseUrl);
  url.protocol = url.protocol === "https:" ? "wss:" : "ws:";
  return url.toString();
}

/** RFC 7230 token characters — a WebSocket subprotocol must be a valid token. */
const SUBPROTOCOL_TOKEN = /^[!#$%&'*+\-.^_`|~0-9A-Za-z]+$/;

/**
 * Derive the connection-level WebSocket credential from a call's `authorization`
 * metadata: strip a `Bearer ` scheme and, if the remaining token is a valid
 * subprotocol token (JWTs and typical API keys are), offer it as `bearer.<token>`.
 * Non-token-safe credentials aren't sent this way — the full metadata still rides
 * in the open frame for per-stream authorization.
 */
function bearerSubprotocol(metadata: Metadata): string | undefined {
  const value = metadata.get("authorization")[0];
  if (typeof value !== "string") return undefined;
  const token = value.replace(/^Bearer\s+/i, "");
  return SUBPROTOCOL_TOKEN.test(token) ? `bearer.${token}` : undefined;
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
