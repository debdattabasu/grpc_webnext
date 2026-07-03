# Backlog / deferred work

Tracked items intentionally not done yet, so they aren't forgotten. Nothing here
blocks the current milestone; each is a follow-up pass.

## Proxy — full gRPC semantics (README point 8)

The proxy round-trips unary (Fetch) and streaming (WebSocket) end-to-end, but these
connection/semantics details are still stubbed:

- [x] ~~Client cancellation → upstream.~~ Verified: a `Reset` frame (or full WebSocket
  disconnect) aborts the response task, which drops the tonic `Streaming` and sends
  RST_STREAM upstream. Covered by `proxy/tests/cancel.rs` (Reset + disconnect) and
  `server/tests/cancel.rs` (in-process handler drop).
- [ ] **Backpressure / flow control.** WS request path uses a fixed-size bounded
  channel; no credit-based per-stream flow control (acceptable per design — atomic
  messages — but revisit if large messages starve a multiplexed connection).
- [ ] **Trailing vs initial metadata fidelity (unary).** Fetch unary currently emits
  response metadata as HTTP headers and only status in the trailer block; trailing
  metadata from a unary call is not separated out.
- [x] ~~Retry & connection management (unary).~~ `RetryPolicy` (max-attempts,
  exponential backoff + full jitter, retryable codes) applied to unary upstream
  calls, bounded by the deadline. Covered by `proxy/tests/retry.rs`. **Remaining:**
  streaming retry (needs commit-point + outbound-buffer semantics), per-method
  service-config keying, and retry throttling (token bucket).
- [x] ~~Max-concurrent-streams cap per WS.~~ `max_concurrent_streams` rejects excess
  `Subscribe`s with RESOURCE_EXHAUSTED. Covered by `proxy/tests/cancel.rs`.
- [x] ~~Deadline enforcement proxy-side.~~ The proxy now drops the call at the
  deadline (DEADLINE_EXCEEDED) on both unary and streaming, and forwards
  `grpc-timeout` downstream with a grace backstop. Covered by `proxy/tests/deadline.rs`.
- [x] ~~Same-port native gRPC coexistence.~~ `application/grpc` is forwarded to the
  upstream untouched (README #9). Covered by `proxy/tests/passthrough.rs`.
- [x] ~~Client cancellation → upstream.~~ (see below)

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

Two client flavors ship: callback/EventEmitter (`makeClient`) and promise/async-iterable
(`makePromiseClient`). All four cardinalities + AbortSignal cancellation are covered
end-to-end. Remaining:

- [ ] **No retry / reconnect** and the WebSocket pool never reaps idle connections.
- [ ] **`ClientReadableStream` has no backpressure / pause** — messages buffer
  unboundedly if the consumer is slow. More visible with the async-iterable API.
- [x] ~~Deadlines sent but not locally enforced~~ — a client-side timer (`context.ts`)
  now fires DEADLINE_EXCEEDED on both the Fetch and WebSocket paths.
- [x] ~~Server/client-streaming untested~~ — covered via the promise-client e2e
  (Greeter server-stream, client-stream, bidi).
- [x] ~~AbortSignal → WebSocket cancel~~ — `signal` now sends a `Reset` and locally
  terminates the stream with CANCELLED (deadline aborts report DEADLINE_EXCEEDED).

## Codec

- [x] ~~JSON support~~ — the native server transcodes `+json` <-> protobuf via a
  descriptor-set `Transcoder` (`ServerConfig::transcoder`). JSON is **native on the
  wire**: Fetch responds with a bare JSON body + status in HTTP headers; WebSocket
  uses JSON **text** frames (native message, not base64) — the WS text/binary type
  selects the codec. The TS client has a `codec: "json"` option. Covered by
  `server/tests/json.rs` and `clients/typescript/test/json.test.ts`.
- [ ] **`Subscribe.json` flag is now vestigial** — the WS text/binary frame type
  selects the codec, so the proto field is unused by the server. Harmless; remove on
  a future proto cleanup.
- [ ] **Binary metadata (`-bin`) is omitted from JSON frames** (ASCII only) — add
  base64 handling if needed.
- [ ] **JSON in the proxy** remains out (binary-only): the proxy is schema-agnostic and
  would need a bundled FileDescriptorSet or upstream reflection to transcode. JSON is
  served by the native library instead.
