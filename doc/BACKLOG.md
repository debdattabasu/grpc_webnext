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
- [x] ~~Retry (unary) — REMOVED (2026-07-04).~~ A `RetryPolicy` was briefly on the
  proxy, then removed on principle: retry belongs in the **client** (gRPC service
  config). A protocol-level wire proxy fans many clients into one upstream, so
  proxy-side retry amplifies load exactly when the upstream is failing (retry storms)
  and compounds with client retries. Removing it also unblocked response streaming
  (retry forced buffering to replay the request / peek the status). Not planned.
- [x] ~~Max-concurrent-streams cap per WS.~~ `max_concurrent_streams` rejects excess
  `Subscribe`s with RESOURCE_EXHAUSTED. Covered by `proxy/tests/cancel.rs`.
- [x] ~~Deadline enforcement proxy-side.~~ The proxy now drops the call at the
  deadline (DEADLINE_EXCEEDED) on both unary and streaming, and forwards
  `grpc-timeout` downstream with a grace backstop. Covered by `proxy/tests/deadline.rs`.
- [x] ~~Same-port native gRPC coexistence.~~ `application/grpc` is forwarded to the
  upstream untouched (README #9). Covered by `proxy/tests/passthrough.rs`.
- [x] ~~Client cancellation → upstream.~~ (see below)
- [x] ~~`+json` termination (2026-07-05).~~ The proxy transcodes `+json` to/from the
  upstream's binary protobuf on both Fetch and WebSocket, reusing the core `Transcoder`
  (identical output to the native server). Descriptors come from **upstream reflection**
  (v1 → v1alpha fallback), a **bundled `FileDescriptorSet`**, or
  **both** — `SchemaSource::{Reflection, Bundled, ReflectionOrBundled}`, the last being
  reflection-primary with the bundle serving immediately and as a fallback when the
  upstream has no reflection. No source → `UNIMPLEMENTED`. Binary `+proto` stays
  schema-agnostic. Reflection is loaded **eagerly and whole** (`list_services` +
  fetch-all into one snapshot; `crates/proxy/src/reflect.rs`), refreshed on a TTL
  (`reflection_ttl`, default 4h), with a `POST` management endpoint (`admin_reload_path`)
  + `Schema::reload()` to force a reload. The binary composes the source from
  `SCHEMA` × `DESCRIPTOR_SET` env. Covered by `proxy/tests/json.rs`.
- [x] ~~REST / `google.api.http` annotation routing on the proxy (2026-07-05).~~ Both
  surfaces resolve annotation URLs against the proxy's transcoder (bundle or eager
  reflection snapshot): Fetch via `transcode_http_request` (`handle_json_fetch` tries a
  REST binding before the main method path), WebSocket via `match_ws` at upgrade
  (single-stream JSON, method from the binding, requests built from the URL — GET-style
  no-body streams inject one empty payload + half-close). Covered by `proxy/tests/json.rs`
  (`rest_*`, `ws_rest_*`). **Caveat:** REST over *reflection* needs the upstream's
  reflection to preserve custom options — the proxy frames raw descriptor bytes verbatim
  so options survive, but tonic-reflection round-trips through prost and strips
  `google.api.http` (Go/Java/C++ preserve it). Bundled descriptors always carry them.
  Filed upstream: [grpc/grpc-rust#2719](https://github.com/grpc/grpc-rust/issues/2719)
  (root cause + proposed fix; PR offered).

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
- [x] ~~REST transcoding in the proxy (2026-07-05).~~ Done on both Fetch and WebSocket;
  see the proxy section above for details and the reflection option-preservation caveat.
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

- [x] ~~Keepalive~~ — done as native **WebSocket ping/pong** on a timer
  (`ServerConfig::ws_keepalive` / `ProxyConfig::ws_keepalive`), with gRPC-style
  pong-timeout drop (`ws_keepalive_timeout`). The old app-level `Ping`/`Pong` frame
  kinds were removed (field numbers reserved). See `doc/STATUS.md`.
- [ ] **Fragmentation** (README point 11 "another day"): large-message fragmentation
  across frames, round-robin, no flow control. New `Frame` kind, additive. Solves both
  peak memory (bounded frames) and multiplex fairness (interleaving), but is opt-in —
  the *sender* must fragment, so it only helps clients that do.
- [ ] **Proxy: stream large WS message payloads without buffering the whole frame.**
  Distinct from fragmentation — a *transparent, proxy-only* peak-memory win that helps
  **any** client (no wire change, no client cooperation). The proxy forwards `+proto`
  opaquely, so it never needs the whole message; but tungstenite reads each WS frame
  fully into memory before yielding it. Fix: read frames incrementally with **wslay**
  (its `on_frame_recv_chunk_callback` delivers payload chunks; it handles masking /
  control / continuation), peek just enough of the protobuf `Frame` envelope to reach
  the payload length, and pipe the payload straight to the upstream gRPC frame via the
  raw-h2 `StreamBody` pattern already used on the Fetch path. wslay is C with no crate —
  needs a thin FFI wrapper crate. **Scope/caveats:** proxy-only (the native server
  decodes messages, so it materializes them regardless); still needs the raw-h2 upstream
  write; and it reintroduces multiplex head-of-line blocking (streaming one frame blocks
  reading the next on that socket) — acceptable for single-stream and a deliberate
  tradeoff for `+multi`, since fragmentation is the interleaving fix. The prost `Bytes`
  change already made the payload a zero-copy slice of the frame buffer, but that buffer
  is still the whole message — this removes that last materialization. Bounded by
  `max_message_bytes`, so the win scales with how large messages are allowed to get.

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
