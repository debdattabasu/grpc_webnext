//! The WebSocket transport derives a connection-level `bearer.<token>` subprotocol
//! from a call's `authorization` metadata (one WebSocket per stream, on the method URL).

import { describe, it, expect, beforeEach } from "vitest";
import { Metadata, WebSocketTransport } from "../src/index.js";

/** Records constructor args and fires `open` so pending frames flush. */
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
}

function md(authorization?: string): Metadata {
  const m = new Metadata();
  if (authorization !== undefined) m.set("authorization", authorization);
  return m;
}

function start(transport: WebSocketTransport, metadata: Metadata) {
  transport.startStream("/echo.v1.Echo/Stream", { metadata }, {});
}

describe("auth via WebSocket subprotocol", () => {
  beforeEach(() => {
    MockWebSocket.instances = [];
  });

  const opts = (extra: object = {}) => ({
    baseUrl: "http://localhost:1234",
    webSocketImpl: MockWebSocket as unknown as typeof WebSocket,
    ...extra,
  });

  it("single-stream: authorization -> bearer.<token> on the method URL", () => {
    start(new WebSocketTransport(opts()), md("Bearer abc.def-token"));
    const ws = MockWebSocket.instances[0];
    expect(ws.url).toContain("/echo.v1.Echo/Stream");
    expect(ws.protocols).toContain("grpc-webnext+proto");
    expect(ws.protocols).toContain("bearer.abc.def-token");
  });

  it("no authorization -> no bearer subprotocol", () => {
    start(new WebSocketTransport(opts()), md());
    const ws = MockWebSocket.instances[0];
    expect(ws.protocols.some((p) => p.startsWith("bearer."))).toBe(false);
  });

  it("non-token-safe credential is not sent via subprotocol (metadata-only)", () => {
    start(new WebSocketTransport(opts()), md("Bearer has a space"));
    const ws = MockWebSocket.instances[0];
    expect(ws.protocols.some((p) => p.startsWith("bearer."))).toBe(false);
  });
});
