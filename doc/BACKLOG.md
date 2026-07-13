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

- [x] ~~WebSocket connection-time auth (`connect_auth`).~~ **Removed** — a connection-scoped
  app-credential gate is non-canonical (the ecosystem gates connections only on network
  identity, e.g. mTLS) and can't be uniform (Fetch has no connection), so it was a footgun.
  With it went the `bearer.<token>` subprotocol machinery (`ws_bearer_token`, the client's
  subprotocol derivation). Auth is now purely per-RPC at the router; a browser credential
  needed at the edge travels as request metadata (or dynamic metadata via an Envoy filter,
  below). The `4000 + code` handshake close remains for codec/surface rejections. See
  `spec/PROTOCOL.md` "Auth".
- [x] ~~Per-stream authorization.~~ **Removed** — a grpc-webnext-specific `stream_auth`
  hook was redundant with a tonic interceptor, which the request already reaches on every
  transport (and which, unlike the hook, also covers the native/h2ts path). Per-RPC auth is
  now a router interceptor (in-process) / the upstream server (proxy). Pinned by
  `tests/inproc_auth.rs::tonic_interceptor_guards_both_grpc_webnext_surfaces`.
- [x] ~~Proxy-side per-stream auth hooks~~ — moot: proxy per-RPC auth is the upstream
  server's interceptors; the proxy forwards metadata opaquely.

## Envoy integration (dynamic-module filters)

Goal: a user runs **stock Envoy** — no sidecar process, no custom Envoy build — and the
grpc-webnext browser transports are terminated **inside** Envoy by runtime-loaded native
**dynamic modules** (Rust SDK; `.so` located via `ENVOY_DYNAMIC_MODULES_SEARCH_PATH`).
After termination the traffic is ordinary HTTP/2 gRPC, so Envoy does routing, `ext_authz`,
rate-limit, LB, and tracing natively and grpc-webnext does **zero** L7 — the same clean split
the proxy already has (it already translates every path to clean gRPC; see `src/h2ts.rs`,
`fetch.rs`, `ws.rs`). This is the ecosystem analog of Envoy's own `grpc_web` filter; the wire
contract is `spec/PROTOCOL.md`, so each filter is a **port of the existing translation logic**
(and h2ts's wslay bridge), not new semantics. Dynamic modules confirmed to support HTTP **and
network** filters (docs), but are "under active development", so treat the ABI specifics as
needing verification.

- [x] ~~Spike: confirm the load-bearing network-filter ABI capabilities.~~ **Confirmed** in
  the SDK (`EnvoyNetworkFilter` trait, envoyproxy/envoy Rust SDK, verified against the pinned
  rev). Read path: `get_read_buffer_chunks` + `drain_read_buffer` consume the inbound WS bytes;
  **`inject_read_data(data, end_stream)` — "inject data into the read filter chain (after this
  filter)"** forwards the de-framed h2c to the next filter (HCM). Downstream write:
  **`write(data, end_stream)` — "write directly to the connection (downstream)"** emits the
  `101` + outbound WS frames; response re-framing uses `get_write_buffer_chunks` /
  `drain_write_buffer` / `inject_write_data`. Lifecycle via `on_new_connection` / `on_event` /
  `close`. **Bonus:** the filter can stash handshake material — SNI (`get_requested_server_name`),
  cert SANs (`get_ssl_uri_sans`), a subprotocol token — into **dynamic metadata**
  (`set_dynamic_metadata_*`) for Envoy `ext_authz`/RBAC, a clean bridge for browser-handshake
  credentials. **Caveat:** the SDK is **version-locked** to Envoy (git dep pinned to a rev,
  strict ABI compat) — the module builds against a matching Envoy release.
- [ ] **Fetch (unary / server-stream) → HTTP filter.** grpc-web-shaped body translation:
  rewrite the length-prefixed request into gRPC and buffer the response trailer into the
  `[msg][trailer]` body (browsers can't read trailers). 1 request = 1 stream → an in-place
  transform. Easiest path; directly analogous to `grpc_web`. Likely viable in Wasm too, but
  a Rust dynamic module keeps one toolchain.
- [ ] **Custom-Frame WS → filter (single-stream 1:1).** Decode `Frame` protobufs off the WS
  upgrade and map the one WS to one gRPC stream (Subscribe→HEADERS, Message→DATA, response→
  Header/Message/Trailer frames). Retiring multiplexing made this a clean 1:1 map.
- [ ] **h2ts → network filter before HCM.** Run the wslay de-frame (own the WS handshake +
  framing) as a **network** filter and hand the inner h2c byte stream to a downstream
  `HttpConnectionManager` (codec `HTTP2`), which natively demuxes it into N routed streams —
  so "1 WS → N streams" is HCM's job, not the filter's. Essentially h2ts-server's `accept` +
  `bridge` ported to the Envoy net-filter ABI — **confirmed viable** by the spike above
  (`inject_read_data` forwards h2c to HCM; `write` emits the `101`/WS frames).
- [ ] **Deployment doc + client-profile guidance.** Topology: browser → stock Envoy (these
  filters terminate) → routing/authz/LB. Document the h2c details the filters must set for
  Envoy routing/authz (`:path`, `:authority`, `content-type`, `te`, metadata pass-through),
  and the transport split: behind a mesh any client profile is terminable; for
  **direct-to-server** (no Envoy) h2ts stays the default. Note Wasm can host network filters
  too but is impractical for a high-throughput H2 tunnel (per-byte VM boundary) — dynamic
  modules (Rust, native) are the chosen mechanism.

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
- [x] ~~WebSocket handshake auth gate (method-scoped).~~ **Removed** with `connect_auth`
  (above) — auth is per-RPC at the router on every transport, with no WS-handshake gate.
  (This item also predated single-stream; the `?method=` multiplex variant is gone too.)
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
- [x] ~~**Proxy: stream large WS message payloads without buffering the whole frame.**~~
  **Resolved by the h2ts integration.** The motivation was a proxy-only peak-memory win: the
  proxy forwards `+proto` opaquely, so it never needs the whole message — only tungstenite's
  read-frame-into-memory forced materialization. The fix envisioned here (drive **wslay**'s
  `on_frame_recv_chunk_callback` to pipe payload chunks straight through) is now exactly what
  the **default proto path** does: it runs over h2ts, and the proxy forwards it with
  `h2ts_server::bridge` (`src/h2ts.rs`) — an opaque, zero-buffer, sub-frame byte pump to the
  h2c upstream (wslay `no_buffering`; never holds a whole message). The old blocker ("wslay is
  C with no crate — needs a thin FFI wrapper") is gone: it's vendored in `wslay-sys`, pulled in
  via `h2ts-server`. **Deliberately not pursued** for the remaining custom-`Frame` proxy paths:
  `proto` + `streaming:"ws"` still materializes each message, but it's an opt-out from the h2ts
  default (use the default for the opaque win) — and `WsByteStream` can't serve it directly
  because it erases the WS message boundaries the `Frame` protocol uses as delimiters (would
  need an upstream h2ts API that preserves them). The `+json` proxy path can never be
  incremental — it must transcode each message, so it materializes by definition.

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
