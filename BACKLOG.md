# Backlog / deferred work

Tracked items intentionally not done yet, so they aren't forgotten. Nothing here
blocks the current milestone; each is a follow-up pass.

## Proxy — full gRPC semantics (README point 8)

The proxy round-trips unary (Fetch) and streaming (WebSocket) end-to-end, but these
connection/semantics details are still stubbed:

- [ ] **Client cancellation → upstream.** A `Reset` frame currently aborts the local
  response-pump task but does not propagate cancellation to the upstream gRPC call.
  Need to drop/abort the tonic call so the upstream sees CANCELLED.
- [ ] **Deadline enforcement proxy-side.** `grpc-timeout` (Fetch) and
  `Subscribe.timeout_millis` (WS) are forwarded to the upstream, but the proxy does not
  independently enforce the deadline / emit DEADLINE_EXCEEDED on its own timer.
- [ ] **Retry & connection management.** No retry policy, backoff, hedging, or
  wait-for-ready yet. Must match standard gRPC service-config semantics.
- [ ] **Backpressure / flow control.** WS request path uses a fixed-size bounded
  channel; no credit-based per-stream flow control (acceptable per design — atomic
  messages — but revisit if large messages starve a multiplexed connection).
- [ ] **Trailing vs initial metadata fidelity (unary).** Fetch unary currently emits
  response metadata as HTTP headers and only status in the trailer block; trailing
  metadata from a unary call is not separated out.
- [ ] **Same-port native gRPC coexistence.** Proxy currently only serves
  grpc-webnext; passing `application/grpc` through to the upstream on the same port
  (README point 9) is not wired.
- [ ] **Map cleanup edge cases / max-concurrent-streams cap per WS.**

## Native server library

Serves native gRPC pass-through + grpc-webnext unary + streaming on one port,
backed by a tonic `Routes`. Deferred:

- [ ] **`+json` is binary-only (UNIMPLEMENTED).** Same reason as the proxy: the
  inner tonic router speaks binary protobuf, so JSON needs either a JSON-capable
  codec registered on the services or descriptor-based transcoding.
- [ ] **Graceful shutdown / drain** is not wired (serve loop runs until dropped).
- [ ] **Per-WS max-concurrent-streams cap** and idle cleanup.
- [ ] **Unary buffers the whole response** before framing (fine for unary; matches
  the Fetch contract).
- Note: deadlines *are* enforced here (grpc-timeout is forwarded and the inner
  tonic server honors it), and client cancellation drops the inner call future —
  both stronger than the proxy today.

## Protocol

- [ ] **Ping/Pong keepalive** frames are defined but not driven by a timer yet.
- [ ] **Fragmentation** (README point 11 "another day"): large-message fragmentation
  across frames, round-robin, no flow control. New `Frame` kind, additive.

## TypeScript client

The client round-trips unary (Fetch) and bidi streaming (WebSocket) end-to-end, but:

- [ ] **Deadlines are sent, not locally enforced.** `grpc-timeout` / `timeout_millis`
  is transmitted, but there is no client-side timer emitting DEADLINE_EXCEEDED if the
  server never responds.
- [ ] **Server-streaming and client-streaming are wired but untested.**
  `makeServerStreamRequest` / `makeClientStreamRequest` have no e2e coverage yet
  (only unary + bidi do).
- [ ] **No retry / reconnect** and the WebSocket pool never reaps idle connections.
- [ ] **`ClientReadableStream` has no backpressure / pause** — messages buffer
  unboundedly if the consumer is slow.
- [ ] **AbortSignal → WebSocket cancel** is only wired on the unary (Fetch) path;
  streaming cancel goes through `.cancel()`, not `options.signal`.

## Codec

- [ ] **JSON transcoding in the proxy** is intentionally out (binary-only v1). If ever
  wanted, needs descriptors via reflection or a bundled FileDescriptorSet.
