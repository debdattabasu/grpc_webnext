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

## Auth

- [x] ~~WebSocket connection-time auth (hard reject).~~ `ServerConfig::connect_auth`
  inspects the handshake headers (the `Sec-WebSocket-Protocol` subprotocol list —
  the only header browser JS can set). On rejection the server accepts the upgrade
  then immediately closes with a **private close code `4000 + gRPC code`** and the
  status message as the close reason, so browser JS reads a real status off
  `CloseEvent.code`/`.reason` instead of a blind `1006`. No stream state is created,
  so the cost matches a refused upgrade. The TS client carries a token as a
  `bearer.<token>` subprotocol (`subprotocolToken` option). Both the native server
  and the proxy echo the `grpc-webnext` subprotocol. Covered by `server/tests/auth.rs`.
- [x] ~~Per-stream authorization.~~ `ServerConfig::stream_auth` runs on every
  `Subscribe` against the call's method + metadata; rejection answers that stream
  with `Reset{ <code> }` (the authoritative, gRPC-faithful check). Covered by
  `server/tests/auth.rs`.
- [ ] **Proxy-side auth hooks** — the proxy has no `connect_auth`/`stream_auth`
  equivalents yet (it forwards opaquely). Add if the proxy needs to gate independently
  of the upstream.

## HTTP transcoding (`google.api.http`)

- [x] ~~REST transcoding on the Fetch path.~~ `crates/core/src/httprule.rs` compiles
  `google.api.http` bindings from the descriptor pool (via prost-reflect's extension
  reading) and maps `(HTTP method, path)` onto a gRPC method, binding path segments,
  query params, and the body into the request message. The native server tries a REST
  match first and falls back to a direct `/pkg.Service/Method` JSON call. Covered by
  `server/tests/json.rs` (`transcode_*`). The google/api protos are vendored under
  `crates/testecho/proto/google/api/` for the test service.
- [ ] **Unsupported HttpRule bits:** `response_body` (response comes back whole),
  regex path patterns beyond `*`/`**`, non-scalar query binding, and repeated-message
  body fields. Add as needed.
- [ ] **Transcoding in the proxy** — same as `+json`: the proxy is schema-agnostic, so
  REST transcoding lives in the native library. Would need a bundled descriptor set.
- [ ] **Client-side REST helper** — the generated TS client still calls the gRPC-style
  path; there's no helper to construct the annotated REST URL from the client.
- [x] ~~Surface model (two rules).~~ (1) Plain HTTP (`application/json`/blank) reaches
  annotated REST endpoints always, and main gRPC paths only with
  `ServerConfig::allow_implicit_codec` (off by default). (2) grpc-webnext is the SDK:
  `+proto`/`+json` on all main paths; `+json` also on annotated routes. `+proto`/`+multi`
  on a REST route is the wrong surface (415 / WS close `4009`). Rejections are explicit
  (415, or a `4000+code` WS close; unknown content-type → 415; unknown method →
  UNIMPLEMENTED). Covered by `server/tests/json.rs` (`main_endpoint_rejects_*`,
  `implicit_codec_flag_allows_*`, `fetch_*`, `ws_rejects_missing_codec_subprotocol_by_default`).
- [x] ~~WS → annotated-endpoint routing.~~ A WebSocket whose upgrade URL matches a
  binding is routed to the RPC: text-locked single-stream JSON (blank / `application/json`
  / `grpc-webnext+json`), method + path/query from the binding (`Subscribe` method
  ignored). `body:"*"` routes take each frame as a request message; body-less (GET) routes
  build the single request from the URL and stream responses. Covered by
  `server/tests/json.rs` (`ws_annotation_*`).
- [ ] **WS annotation routing in the proxy** — like `+json`/transcoding generally, the
  proxy is schema-agnostic and doesn't do it; the native library does.

## WebSocket streams / multiplexing

- [x] ~~Multiplexing off by default; human-readable single-stream JSON.~~ Default is
  **one WebSocket per stream**, connected to the method's URL — JSON frames carry no
  `streamId` and no `method` (both implied by the route), and the first inbound frame
  opens the stream. Multiplexing is opt-in via a `+multi` subprotocol
  (`grpc-webnext+{json,proto}+multi`) and the client `multiplex`/`poolSize` options;
  `+multi` frames carry `streamId` (+ `method` on the JSON open). Server, proxy, and
  TS client all implement both; covered by `server/tests/json.rs`
  (`streaming_json_round_trip`, `ws_multiplex_two_streams`), `proxy/tests/*`, and the
  TS e2e (`multiplex: two concurrent streams`).
- [x] ~~WebSocket handshake auth gate (method-scoped).~~ Auth is per-stream (`stream_auth`
  on the open-frame metadata); the WS handshake gate is an *optional early reject*. The TS
  client derives a `bearer.<token>` subprotocol from the call's `authorization` metadata;
  the server runs `connect_auth(method, headers)` (read the token via `ws_bearer_token`)
  **only when a credential is present**, scoped to the method — the URL path (single-stream)
  or a `?method=` query (multiplexed). A credential with no resolvable method is a hard
  reject; a credential-less connection just opens. Covered by
  `clients/typescript/test/auth-subprotocol.test.ts` and `server/tests/auth.rs`
  (`connect_gate_*`, `multiplex_auth_*`, `no_credential_opens_the_connection`).
- [ ] **WS pool never reaps idle connections** (multiplex mode).

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
