# Protocol conformance status

Audit of `PROTOCOL.md` against the implementation and test suites (2026-07-03).
Verdict: the doc is largely accurate — routing rules, close codes, frame shapes,
transcoding, and the auth handshake all match the code, with solid coverage on the
happy paths. Three real behavioral drifts, one internal doc contradiction, and a
cluster of test gaps concentrated in the httprule subset and the proxy's JSON
handling. Drift 2 (client close-code reconstruction) and drift 3 (keepalive) are
fixed as of 2026-07-04; drift 1 remains open.

## Drift: doc says X, code does Y

### 1. The proxy does not reject `+json` on WebSocket

PROTOCOL.md says the v1 proxy "rejects `+json` with `UNIMPLEMENTED`". Split by
transport:

- **Fetch:** the rejection exists but is an HTTP `501` with a plain-text body
  (`crates/proxy/src/lib.rs:181-185`), not a gRPC `UNIMPLEMENTED` trailer.
- **WebSocket:** no rejection at all. The upgrade handler recognizes and echoes
  back `grpc-webnext+json` and `+json+multi` (`crates/proxy/src/lib.rs:144-161`)
  — the WS way of saying "yes, I speak this" — then forwards JSON payloads
  opaquely to an upstream `application/grpc` server that expects binary protobuf.

The failure is deferred and misattributed: instead of a clean `4012` close at
handshake, the client gets a negotiated connection followed by garbage errors
from the upstream. No test covers proxy WS `+json`. Fix: treat the two `+json`
subprotocol variants like the native server treats unsupported codecs — accept
the upgrade, close `4012`.

### 2. The client never reconstructs a `Status` from the close frame — RESOLVED (2026-07-04)

PROTOCOL.md: the JS client "reads `CloseEvent.code`/`.reason` and reconstructs a
`Status`". Server side was already implemented and tested — close before the read
loop (`crates/server/src/ws.rs:76-80`), `4000 + code` with truncated reason
(`ws.rs:192-198`), asserted by `crates/server/tests/auth.rs:89-99`.

Client side was not: both close listeners discarded the event and `onClose()`
always emitted `UNAVAILABLE "websocket closed"`. An auth rejection was
indistinguishable from the server being down, and UNAVAILABLE is conventionally
retryable while UNAUTHENTICATED is not — retry loops would hammer a hopeless auth
failure.

Fixed in `clients/typescript/src/ws-transport.ts`: a `statusForClose(event)`
helper maps a private close code (`4000 + gRPC code`, range 4000..=4016) to the
gRPC status with `event.reason` as the message; any other close (normal 1000,
abnormal 1006, or an `error` event with no CloseEvent) falls back to
UNAVAILABLE. Both `SingleStreamConn.onClose` and `MultiplexConn.onClose` now
pass the `CloseEvent` through it; the multiplex path fans the reconstructed
status out to every open stream. Covered by
`clients/typescript/test/ws-close-status.test.ts` — mock-driven cases for both
transport modes (each gRPC code, out-of-range codes, error events, fan-out) plus
one real `ws` server that closes with `4016` to exercise a genuine
`CloseEvent.code`/`.reason`.

### 3. App-level `Ping`/`Pong` exist on paper only — RESOLVED (2026-07-04)

PROTOCOL.md presented them as app-level keepalive on both codecs, but they were
declared only in the binary envelope with no JSON wire form, the server's
dispatcher fell through on them, and nothing ever sent one.

Resolved by dropping the app-level frames and using **native WebSocket ping/pong
control frames** (RFC 6455 §5.5.2) for keepalive — the right layer, since a browser
can't send an app frame on an idle timer from JS but *does* auto-answer a server
ping with a pong. Changes:

- Removed `Ping`/`Pong` from `proto/grpc_webnext.proto` and reserved field numbers
  6/7 (`reserved 6, 7;`) so they can't be silently reused; regenerated the Rust
  (build.rs/prost) and TS (`npm run gen`) bindings.
- Added `ServerConfig::ws_keepalive` and `ProxyConfig::ws_keepalive`
  (`Option<Duration>`, default `None`). When set, the connection's writer task
  (`crates/server/src/ws.rs`, `crates/proxy/src/ws.rs`) `select!`s a ticker and
  emits a `TungMessage::Ping` each period; the peer's automatic pong is the return
  traffic that keeps an idle-timeout proxy/LB from dropping a quiet stream. The
  ticker's first tick is one period out and missed ticks are skipped (no ping
  bursts after a busy period).
- **Dead-peer detection (gRPC-style), added 2026-07-04.** `ws_keepalive_timeout`
  (`Duration`, default 20s — gRPC's default) bounds the wait for a response. The
  read loop keeps a liveness deadline that any inbound frame (the pong, or ordinary
  data) pushes out; if nothing arrives for `ws_keepalive + ws_keepalive_timeout`,
  the connection is dropped, surfacing `UNAVAILABLE` to its streams. This detects
  half-open connections in seconds instead of waiting out the OS TCP timeout.
  Notably this needed **no new shared state**: pongs already arrive in the read loop
  and the drop is just its `break`, so the writer keeps pinging while the read loop
  owns detection.
- No client change needed: browsers and the Node `ws` package auto-answer pings.
- Covered by `crates/server/tests/keepalive.rs` and `crates/proxy/tests/keepalive.rs`:
  pings arrive when enabled and not when disabled; a peer that stops answering (a
  client that stops polling, so tokio-tungstenite stops auto-ponging) is dropped
  within the window; a peer that keeps answering stays connected. PROTOCOL.md's
  keepalive paragraph documents both the ping mechanism and the timeout.

### Smaller inaccuracies

- **stream_id "omitted entirely"** (PROTOCOL.md stream_id section): true only
  for JSON, where the serializer skips it outside multi mode
  (`json_frame.rs:160`). Binary frames always carry `stream_id: 1` in
  single-stream mode — the doc says so itself in the binary single-stream
  paragraph, so the stream_id section contradicts it. Accurate statement: JSON
  omits it; binary carries it fixed at 1; either way it's meaningless in
  single-stream mode.
- **"Fetch is the same" for auth**: `ServerConfig::stream_auth`'s only call
  site is the WebSocket Subscribe handler (`crates/server/src/ws.rs:247`). On
  Fetch, `authorization` flows through to the inner tonic service; a
  `grpc-status: 16` only appears if the service/interceptor rejects. A team
  registering `stream_auth` thinking it guards everything has an
  unauthenticated Fetch surface. Either document "enforce Fetch auth in your
  service/interceptor" or run the hook on the Fetch path too.
- **`Reset{ UNAUTHENTICATED }` isn't hardcoded**: the Reset carries whatever
  `Status` the `stream_auth` hook returns (`ws.rs:245-252`) — more flexible
  than documented (e.g. PERMISSION_DENIED for valid-but-unauthorized).

## Internal doc contradiction

The content-type table says `grpc-webnext+json` also works on annotated REST
endpoints; the REST section says annotated endpoints accept "never the
grpc-webnext content-types". The code implements the table's version — all
JSON-ish content types route through the same transcode-first path
(`crates/server/src/lib.rs:351-357`), with a test asserting `+json` transcodes
on a REST URL (`crates/server/tests/json.rs:192`). Only `+proto` is rejected on
annotated URLs. The REST-section sentence is stale.

## Undocumented wire-observable behavior

- **Request-size limit → 413** (`max_message_bytes`, native server and proxy).
  The doc only mentions the client's response-buffer limit.
- **Proxy unary retry policy** — backoff with jitter, retryable-code gating,
  deadline-bounded, off by default (`crates/proxy/src/lib.rs:71-94`); well
  tested (`crates/proxy/tests/retry.rs`) but absent from the doc.
- **Stream-level error cases**: duplicate `stream_id` in a Subscribe →
  `Reset{INVALID_ARGUMENT, "stream_id in use"}`; `+json` without a transcoder →
  `Reset{UNIMPLEMENTED}` on WS but HTTP `501` on Fetch; exceeding the proxy's
  `max_concurrent_streams` → `Reset{RESOURCE_EXHAUSTED}`.
- **JSON frame edge semantics**: a frame with no recognized field (e.g. `{}`)
  decodes as a half-close — a typo'd field name silently ends the send side. A
  multi-mode open may combine `{streamId, method, message}` in one frame
  (message becomes the initial payload). `-bin` metadata is silently dropped
  crossing into the JSON codec.
- **Encoding details**: `grpc-message` headers are percent-encoded; close
  reasons truncate to 123 bytes on a UTF-8 boundary; "token-safe" for
  `bearer.<token>` means RFC 7230 token chars, embedded raw, matched against a
  case-sensitive lowercase `bearer.` prefix.
- **Pool behavior**: the client opens a new socket per stream until `poolSize`
  is reached, then round-robins — the doc describes steady state only.
- **Query params are ignored when `body: "*"`** in REST transcoding; path vars
  still overlay.

## Test coverage gaps

- **httprule has zero unit tests.** `crates/core/src/httprule.rs` implements
  verbs, path templates, `{field=**}` rest-captures, dotted paths, body rules,
  and query coercion with no `#[cfg(test)]` module. All coverage is indirect
  via `crates/server/tests/json.rs`, whose echo.proto annotations use only
  GET/POST, single-segment `{message}`, and `body:"*"`/none. Never executed
  under test: `put/delete/patch/custom`, `{field=**}`, dotted field paths,
  `body:"<field>"`, repeated-field query params, and the three documented
  rejection paths (`response_body`, regex patterns, non-scalar query). Highest
  value place to add tests — pure functions, easy to unit-test.
- **`+json+multi` on annotated WS routes**: the guard is `proto || multi` but
  only the `proto` half has a test (`json.rs:489`); the `multi` half could be
  dropped without a test failing.
- **Proxy negative paths**: WS `+json` (drift 1 — the current forwarding
  behavior is also unpinned), the 415 unknown-content-type branch, the 413
  size limit, duplicate-stream_id reset.
- **Client-side limits and errors**: `maxMessageBytes` overflow (Rust core
  limit is tested, TS client's isn't), the `FetchTransport.startStream` throw,
  `poolSize > 1` round-robin distribution, `grpc-timeout` header emission on
  Fetch unary (deadline tests all go through streaming).
- **Fetch-path auth**: no test in `auth.rs`.
- **Close-event handling**: covered as of drift 2's fix
  (`test/ws-close-status.test.ts`). Remaining gap: no full cross-language e2e of
  a *native-server* handshake reject reaching the client, because the e2e
  `devserver` fronts the upstream with the proxy (no `connect_auth` gate); the
  reject path is covered on the Rust side (`crates/server/tests/auth.rs:89-99`)
  and the client decode by the real-`ws` test.

The through-line: happy paths are well covered on both sides of the wire;
almost every gap is a rejection or limit branch — exactly the branches that
define protocol behavior for non-conforming or misconfigured clients.
