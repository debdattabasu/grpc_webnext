# Protocol conformance status

Audit of `PROTOCOL.md` against the implementation and test suites (2026-07-03).
Verdict: the doc is largely accurate — routing rules, close codes, frame shapes,
transcoding, and the auth handshake all match the code, with solid coverage on the
happy paths. Three real behavioral drifts, one internal doc contradiction, and a
cluster of test gaps concentrated in the httprule subset and the proxy's JSON
handling.

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

### 2. The client never reconstructs a `Status` from the close frame

PROTOCOL.md: the JS client "reads `CloseEvent.code`/`.reason` and reconstructs a
`Status`". Server side is fully implemented and tested — close before the read
loop (`crates/server/src/ws.rs:76-80`), `4000 + code` with truncated reason
(`ws.rs:192-198`), asserted by `crates/server/tests/auth.rs:89-99`.

Client side is not: both close listeners discard the event
(`clients/typescript/src/ws-transport.ts:136`, `:311`) and `onClose()` always
emits `UNAVAILABLE "websocket closed"`. An auth rejection is indistinguishable
from the server being down, and UNAVAILABLE is conventionally retryable while
UNAUTHENTICATED is not — retry loops will hammer a hopeless auth failure. Fix:
in the close handler, map `4000 ≤ code < 4100` to `code - 4000` with
`event.reason` as the message; fall back to UNAVAILABLE otherwise. The mock
WebSocket in `auth-subprotocol.test.ts` never fires `close`, so it can't catch
this until extended.

### 3. App-level `Ping`/`Pong` exist on paper only

PROTOCOL.md presents them as app-level keepalive on both codecs. Reality:

- Declared only in the binary envelope (`proto/grpc_webnext.proto:25-26`); the
  JSON codec has no wire form for them (`crates/core/src/json_frame.rs`).
- The server's frame dispatcher falls through on them
  (`crates/server/src/ws.rs:306`), so a Ping would never get a Pong back.
- Nothing ever sends one — not the TS client, not the proxy. The proxy's
  "ping/pong handled by tungstenite" comment refers to WS *protocol-level*
  ping/pong, a different layer browsers can't drive anyway.

The rationale (idle-timeout LBs killing quiet sockets) is real, so either
implement it (client Ping on an idle timer, server echoes Pong, plus a JSON
form) or move it to "Reserved for later" next to fragmentation.

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
- **Close-event handling**: blocked on drift 2; once implemented, the mock
  WebSocket needs to fire `close` events.

The through-line: happy paths are well covered on both sides of the wire;
almost every gap is a rejection or limit branch — exactly the branches that
define protocol behavior for non-conforming or misconfigured clients.
