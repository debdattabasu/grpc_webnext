/**
 * grpc-webnext in-process server for Node.
 *
 * The Node sibling of the Rust `grpc-webnext` crate and the Go `webnext` package:
 * serve full gRPC semantics to browsers — unary over Fetch, streaming over
 * WebSocket — in front of a native Node gRPC server (@grpc/grpc-js), on the same
 * port as native gRPC. Held to /spec/PROTOCOL.md by the /conformance suite.
 *
 * STATUS: skeleton. The public surface below mirrors the Rust `ServerConfig` /
 * `serve_in_process` and the Go `ServerConfig` / `Serve`; the implementation is
 * pending. The shared frame codec should be factored into a sibling
 * `@grpc-webnext/wire` package used by both this server and `@grpc-webnext/client`
 * (the same client+server split every language uses).
 */

/** Default maximum decoded message size (4 MiB), matching the Rust/Go servers. */
export const DEFAULT_MAX_MESSAGE_BYTES = 4 * 1024 * 1024;

export interface ServerConfig {
  /** Max decoded message size in bytes; over-size terminates with RESOURCE_EXHAUSTED (8). */
  maxMessageBytes?: number;
  /** Allow a blank WebSocket codec subprotocol to default to binary (default: false/strict). */
  allowImplicitCodec?: boolean;
  /** Send a keepalive ping every this-many ms; 0 disables. */
  wsKeepaliveMs?: number;
  /** Drop the connection if a pong doesn't arrive within this-many ms. */
  wsKeepaliveTimeoutMs?: number;
  // TODO(spec): transcoder (for +json), connect/stream auth hooks.
}

/**
 * Serve grpc-webnext (Fetch + WebSocket) and native gRPC on one port, dispatching
 * to an in-process native gRPC handler.
 *
 * TODO(spec): implement. Signature mirrors the Rust `serve_in_process`.
 */
export function serveInProcess(_config: ServerConfig = {}): never {
  throw new Error("@grpc-webnext/server: not yet implemented");
}
