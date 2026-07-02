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
}

/** Streaming transport over WebSocket, with a client-side multiplex pool. */
export class WebSocketTransport implements Transport {
  private readonly url: string;
  private readonly WS: typeof WebSocket;
  private readonly pool: Conn[] = [];
  private readonly poolSize: number;
  private next = 0;

  constructor(options: WebSocketTransportOptions) {
    this.url = toWsUrl(options.baseUrl);
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
    const conn = this.pickConn();
    return conn.open(path, options, handlers);
  }

  close(): void {
    for (const conn of this.pool) conn.close();
    this.pool.length = 0;
  }

  private pickConn(): Conn {
    if (this.pool.length < this.poolSize) {
      const conn = new Conn(this.url, this.WS);
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
  private readonly pending: Uint8Array[] = [];
  private open_ = false;
  private nextStreamId = 1;

  constructor(url: string, WS: typeof WebSocket) {
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

    this.sendFrame(
      encodeFrame({
        subscribe: {
          streamId,
          method: path,
          headers: options.metadata.toMetadatumList(),
          timeoutMillis: options.timeoutMillis ? Math.ceil(options.timeoutMillis) : 0,
          initialPayload: new Uint8Array(),
        },
      }),
    );

    return {
      send: (message) => this.sendFrame(encodeFrame({ message: { streamId, payload: message } })),
      halfClose: () => this.sendFrame(encodeFrame({ halfClose: { streamId } })),
      cancel: () => {
        this.sendFrame(
          encodeFrame({
            reset: { streamId, statusCode: Status.CANCELLED, statusMessage: "cancelled" },
          }),
        );
        this.streams.delete(streamId);
      },
    };
  }

  close(): void {
    try {
      this.ws.close();
    } catch {
      /* ignore */
    }
  }

  private sendFrame(frame: Uint8Array): void {
    if (this.open_) this.ws.send(frame);
    else this.pending.push(frame);
  }

  private onMessage(ev: MessageEvent): void {
    const data = ev.data;
    const bytes =
      data instanceof ArrayBuffer
        ? new Uint8Array(data)
        : ArrayBuffer.isView(data)
          ? new Uint8Array(data.buffer, data.byteOffset, data.byteLength)
          : null;
    if (!bytes) return; // ignore text frames
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
