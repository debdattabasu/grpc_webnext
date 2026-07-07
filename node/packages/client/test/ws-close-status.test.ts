//! On WebSocket close, the client reconstructs a gRPC status from the close frame:
//! the server encodes pre-frame failures (e.g. a rejected auth handshake) as private
//! code `4000 + gRPC code` with the message in the reason. Any other close is a
//! transport failure and surfaces as UNAVAILABLE.

import { describe, it, expect, beforeEach } from "vitest";
import WsClient, { WebSocketServer } from "ws";
import { Metadata, Status, WebSocketTransport } from "../src/index.js";
import type { StatusResult } from "../src/transport.js";

/** Mock that records constructor args and lets a test emit lifecycle events. */
class MockWebSocket {
  static instances: MockWebSocket[] = [];
  readonly protocols: string[];
  binaryType = "";
  private listeners: Record<string, ((ev: any) => void)[]> = {};
  constructor(
    readonly url: string,
    protocols?: string | string[],
  ) {
    this.protocols = protocols == null ? [] : Array.isArray(protocols) ? protocols : [protocols];
    MockWebSocket.instances.push(this);
  }
  addEventListener(type: string, cb: (ev: any) => void) {
    (this.listeners[type] ??= []).push(cb);
  }
  send() {}
  close() {}
  emit(type: string, ev: any = {}) {
    for (const cb of this.listeners[type] ?? []) cb(ev);
  }
}

function md(authorization?: string): Metadata {
  const m = new Metadata();
  if (authorization !== undefined) m.set("authorization", authorization);
  return m;
}

const opts = (extra: object = {}) => ({
  baseUrl: "http://localhost:1234",
  webSocketImpl: MockWebSocket as unknown as typeof WebSocket,
  ...extra,
});

/** Start a stream and capture the terminal status delivered to its handler. */
function startCapturing(extra: object = {}, authorization?: string) {
  const transport = new WebSocketTransport(opts(extra));
  let status: StatusResult | undefined;
  transport.startStream(
    "/echo.v1.Echo/Stream",
    { metadata: md(authorization) },
    { onStatus: (s) => (status = s) },
  );
  const ws = MockWebSocket.instances[MockWebSocket.instances.length - 1];
  return { ws, status: () => status };
}

describe("status reconstruction from WebSocket close", () => {
  beforeEach(() => {
    MockWebSocket.instances = [];
  });

  for (const multiplex of [false, true]) {
    const label = multiplex ? "multiplex" : "single-stream";

    it(`${label}: a private 4000+code close becomes that gRPC status with the reason`, () => {
      const { ws, status } = startCapturing({ multiplex }, "Bearer bad");
      ws.emit("close", { code: 4000 + Status.UNAUTHENTICATED, reason: "bad token" });
      expect(status()).toEqual({
        code: Status.UNAUTHENTICATED,
        details: "bad token",
        metadata: new Metadata(),
      });
    });

    it(`${label}: FAILED_PRECONDITION (4009) round-trips`, () => {
      const { ws, status } = startCapturing({ multiplex });
      ws.emit("close", { code: 4000 + Status.FAILED_PRECONDITION, reason: "no codec" });
      expect(status()?.code).toBe(Status.FAILED_PRECONDITION);
      expect(status()?.details).toBe("no codec");
    });

    it(`${label}: a normal 1000 close is UNAVAILABLE`, () => {
      const { ws, status } = startCapturing({ multiplex });
      ws.emit("close", { code: 1000, reason: "" });
      expect(status()?.code).toBe(Status.UNAVAILABLE);
      expect(status()?.details).toBe("websocket closed");
    });

    it(`${label}: an abnormal 1006 close is UNAVAILABLE`, () => {
      const { ws, status } = startCapturing({ multiplex });
      ws.emit("close", { code: 1006, reason: "" });
      expect(status()?.code).toBe(Status.UNAVAILABLE);
    });

    it(`${label}: an error event (no CloseEvent) is UNAVAILABLE`, () => {
      const { ws, status } = startCapturing({ multiplex });
      ws.emit("error");
      expect(status()?.code).toBe(Status.UNAVAILABLE);
    });

    it(`${label}: a code just past the gRPC range (4017) is UNAVAILABLE`, () => {
      const { ws, status } = startCapturing({ multiplex });
      ws.emit("close", { code: 4017, reason: "out of range" });
      expect(status()?.code).toBe(Status.UNAVAILABLE);
    });
  }

  it("multiplex: a socket close fans the status out to every open stream", () => {
    const transport = new WebSocketTransport(opts({ multiplex: true }));
    const got: StatusResult[] = [];
    for (let i = 0; i < 3; i++) {
      transport.startStream("/echo.v1.Echo/Stream", { metadata: md() }, { onStatus: (s) => got.push(s) });
    }
    // poolSize defaults to 1, so all three streams share one socket.
    expect(MockWebSocket.instances).toHaveLength(1);
    MockWebSocket.instances[0].emit("close", { code: 4000 + Status.UNAUTHENTICATED, reason: "bad token" });
    expect(got).toHaveLength(3);
    for (const s of got) {
      expect(s.code).toBe(Status.UNAUTHENTICATED);
      expect(s.details).toBe("bad token");
    }
  });
});

// A real `ws` server that closes with a private code, to confirm a genuine
// CloseEvent's `.code`/`.reason` plumb through the decode path (not just the mock).
describe("status reconstruction over a real WebSocket", () => {
  it("a 4016 handshake close surfaces as UNAUTHENTICATED with the reason", async () => {
    const wss = new WebSocketServer({ port: 0 });
    await new Promise<void>((resolve) => wss.on("listening", () => resolve()));
    const port = (wss.address() as { port: number }).port;
    wss.on("connection", (socket) => socket.close(4000 + Status.UNAUTHENTICATED, "bad token"));

    const transport = new WebSocketTransport({
      baseUrl: `http://127.0.0.1:${port}`,
      webSocketImpl: WsClient as unknown as typeof globalThis.WebSocket,
    });
    try {
      const status = await new Promise<StatusResult>((resolve) => {
        transport.startStream("/echo.v1.Echo/Stream", { metadata: new Metadata() }, { onStatus: resolve });
      });
      expect(status.code).toBe(Status.UNAUTHENTICATED);
      expect(status.details).toBe("bad token");
    } finally {
      transport.close();
      await new Promise<void>((resolve) => wss.close(() => resolve()));
    }
  });
});
