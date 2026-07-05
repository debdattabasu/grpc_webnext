# grpc-webnext wire protocol

Two transports, one set of gRPC semantics. Content-type selects the codec on the
request path; the WebSocket upgrade selects the streaming path.

## Content types

The **URL** picks the route — a `google.api.http` binding (`/v1/…`) is an **annotated
REST endpoint**, anything else is a **main gRPC path** (`/pkg.Service/Method`). The
content-type (Fetch) / subprotocol (WebSocket) then picks the codec, under **two
rules**:

1. **Plain HTTP** (`application/json` / *blank*) reaches annotated REST endpoints
   **always**; it reaches main paths **only with `ServerConfig::allow_implicit_codec`**.
2. **grpc-webnext** is the SDK: `+proto`/`+json` work on **all main paths**; `+json`
   **also** works on annotated endpoints (it's JSON). `+proto` doesn't fit the JSON
   REST surface.

| Content-type / WS subprotocol | Main path (`/pkg.Svc/Method`) | Annotated URL (`/v1/…`) |
|---|---|---|
| `application/grpc*` | passthrough (native gRPC) | — |
| `application/grpc-webnext+proto` (±`+multi`) | ✅ binary | ✗ 415 / close 4009 |
| `application/grpc-webnext+json` (±`+multi`) | ✅ JSON | ✅ JSON¹ |
| `application/json` | flag off → **415** · on → ✅ | ✅ JSON |
| *(blank / no codec subprotocol)* | flag off → **415 / close 4012** · on → ✅ | ✅ JSON |

¹ On WebSocket, annotated routes are single-stream, so `+multi` is rejected there too.

`allow_implicit_codec` is off by default; its whole job is rule 1's flag — *"may plain
HTTP reach the real gRPC method paths, or only the REST routes?"* Rejections are
**explicit**: Fetch → `415 Unsupported Media Type`; WebSocket → a close frame with a
private code `4000 + gRPC code` (`4012` UNIMPLEMENTED, `4009` FAILED_PRECONDITION). An
unknown content-type is `415`; a JSON call to a nonexistent method is `UNIMPLEMENTED`.

The codec is **native** on both transports: proto uses binary framing; JSON is
idiomatic JSON on the wire (never wrapped in the binary framing).

## Unary — Fetch

- **Request:** HTTP POST. Metadata → HTTP request headers; `grpc-timeout` carries the
  deadline. For **`+proto`** the body is the single message **length-prefixed** —
  `[ 4-byte big-endian length | message bytes ]`, mirroring the response's message block.
  The client already has the whole serialized message, so it prepends the length it
  knows; the server/proxy then turn it into a gRPC frame (prepend the `[1-byte flag]`)
  and **stream** it upstream without buffering to measure. For **`+json`** the body is
  the bare JSON text (buffered, since it's transcoded).
- **Response (`+proto`):** the browser cannot read HTTP trailers, so the server
  writes the body as two length-prefixed blocks:

  ```
  [ 4-byte big-endian length | message bytes ]
  [ 4-byte big-endian length | Trailer block ]
  ```

  The trailer block is an encoded `Trailer` (status + trailing metadata). The
  client buffers the whole body up to a **configurable size limit**. On the **server
  and the proxy** this response is **streamed, not buffered**: because the status rides
  in the trailer block *after* the message (not in a header), neither needs the whole
  message up front. The inner gRPC frame is already `[1-byte flag][4-byte length |
  message]`, so dropping the compression-flag byte yields our first block verbatim — the
  message is piped straight to the socket and the trailer block appended at the end. A
  large binary blob therefore isn't malloc'd. (The proxy does this opaquely — it never
  decodes the message — which is also why it does no retry: a wire proxy can't replay a
  streamed call, and retry belongs in the client anyway.)
- **Response (`+json`):** the body is the **bare JSON message**; gRPC status and
  metadata travel in HTTP response headers (`grpc-status`, `grpc-message`, plus
  metadata). No length-prefix, no trailer block — a plain JSON API you can `curl`.
  Unlike `+proto`, JSON **is** buffered on the server: it transcodes the whole protobuf
  message to JSON and the status goes in a header (which must precede the body), so the
  full message is needed before anything is written.
- Server-streaming does **not** use Fetch — it goes over WebSocket (Fetch would have
  to buffer the entire stream). Fetch is unary only. A genuinely *streamed* upload of
  unknown length also goes over WebSocket — the Fetch upload path assumes a known-length
  message (always true for protobuf), which is what lets the client supply the length
  prefix.

### REST transcoding (`google.api.http`)

Core gRPC is always `POST /pkg.Service/Method`. To get real HTTP verbs and REST
URLs, annotate methods with `google.api.http` (the grpc-gateway / Envoy standard):

```proto
rpc GetUser(GetUserReq) returns (User) {
  option (google.api.http) = {
    get: "/v1/users/{id}"
    additional_bindings { post: "/v1/users" body: "*" }
  };
}
```

The native server compiles these bindings from the descriptor set (the same
`Transcoder` used for `+json`) and, on the Fetch path, maps a matching
`(method, path)` onto the gRPC method — binding path segments, query params, and
the body into the request message, and returning the response as JSON. So
`GET /v1/users/123` and `POST /v1/users {…}` both reach `GetUser`. These annotated
endpoints are **JSON-only**: they accept plain HTTP (blank / `application/json`) *and*
`grpc-webnext+json` — all three transcode through the same path — but reject `+proto`
with **415** (`"REST-annotated endpoints are JSON-only"`), since binary framing doesn't
fit the REST surface. A plain request whose URL matches no binding is a direct
main-endpoint call, which requires `allow_implicit_codec` (else 415).

**Streaming methods** can be annotated too — the annotation URL is then a **WebSocket**
endpoint (`ws://host/v1/watch/123`, verb-agnostic since a WS upgrade is a GET). Such a
connection is **text-locked, single-stream JSON**, accepts a blank / `application/json`
/ `grpc-webnext+json` subprotocol (proto and `+multi` are rejected), and its gRPC method
+ path/query bindings come from the annotation (the `Subscribe` method is ignored). A
`body:"*"` route takes each frame's JSON as a request message; a body-less route (GET)
builds the single request entirely from the URL and streams the responses. See
"Streaming — WebSocket" below.

An annotation only **adds** the REST alias; it does not change the RPC's main path,
which stays reachable by its grpc-webnext content-types/subprotocols (the SDK contract)
and — when `allow_implicit_codec` is on — by plain HTTP like any other main path.

Supported subset: verbs `get/put/post/delete/patch` + `custom`; `additional_bindings`;
path templates with literal segments, `{field}`/`{field=*}` single-segment captures,
`{field=**}` rest-captures, and dotted field paths; `body: "*"` / `body: "<field>"` /
none; query-param binding to scalar/repeated fields. Not yet: `response_body`, regex
path patterns, non-scalar query binding (see `crates/core/src/httprule.rs`).

## Streaming — WebSocket

A WebSocket is either a **main-endpoint** connection (described here) or an **annotated
route** — if the upgrade URL matches a `google.api.http` binding, the connection is
text-locked, single-stream, JSON, accepts a blank / `application/json` /
`grpc-webnext+json` subprotocol (proto and `+multi` rejected), and its method +
path/query bindings come from the annotation (see the REST section). The rest of this
section describes the main-endpoint surface.

The connection's codec **and** multiplexing mode are chosen by the subprotocol:

| Subprotocol | Codec | Streams |
|---|---|---|
| `grpc-webnext+proto` | binary | **single** (default) |
| `grpc-webnext+json` | JSON | **single** (default) |
| `grpc-webnext+proto+multi` | binary | multiplexed |
| `grpc-webnext+json+multi` | JSON | multiplexed |

The client offers one of these alongside the base `grpc-webnext`; the server pins the
mode and negotiates the subprotocol back. A connection with **no** codec subprotocol
(or a plain `application/json` one) is rejected by default — closed with `UNIMPLEMENTED`
(close code `4012`); `ServerConfig::allow_implicit_codec` re-enables first-frame
inference (blank ⇒ lock to first frame's type; `application/json` ⇒ text). Either way
the connection is single-codec — frames of the other type are dropped.

### Single-stream (default)

**One WebSocket per stream.** The WS **URL is the route** (`ws://host/pkg.Svc/Method`),
so the method is implied and frames carry neither `method` nor `streamId`. The **first
inbound frame opens** the stream. JSON is human-readable:

```jsonc
// client → server
{ "metadata": {…}, "timeoutMillis": 5000 } // open (optional; carries metadata/deadline)
{ "message": {…} }                         // data
{ "halfClose": true }                      // done sending
{ "status": { "code": 1, "message": "…" } }// client reset
// server → client
{ "metadata": {…} }                        // initial headers
{ "message": {…} }                         // data
{ "status": { "code": 0, "message": "" } } // terminal (trailer / reset)
```

Binary single-stream uses the same `Frame` envelope with `stream_id` fixed to `1` and
a leading `Subscribe` whose `method` is ignored (taken from the URL).

### Multiplexed (`+multi`)

**One WebSocket, many streams.** The URL is the base (`ws://host/`); every frame carries
`streamId`, and the JSON open carries `method`:

```jsonc
{ "streamId": 1, "method": "/pkg.Svc/M", "metadata": {…} } // open
{ "streamId": 1, "message": {…} }                          // data
{ "streamId": 1, "status": { "code": 0, "message": "" } }  // terminal
```

Streams are assigned round-robin across a client-side pool. A second `Subscribe` on a
non-`multi` connection is impossible (the frames carry no method); the wire otherwise
matches single-stream with `streamId` added. See `crates/core/src/json_frame.rs`.

**One message per frame, no fragmentation** (both codecs).

**Keepalive** uses native **WebSocket ping/pong control frames** (RFC 6455 §5.5.2),
not application frames — HTTP/2 PING isn't reachable from browser JS, and a browser
can't send a WS ping from JS either, but it *does* auto-answer a server ping with a
pong. So the **server drives keepalive** (mirroring gRPC's `keepalive_time` /
`keepalive_timeout`):

- With `ServerConfig::ws_keepalive` (or `ProxyConfig::ws_keepalive`) set to an
  interval, an open streaming connection emits a ping each period. The peer's
  automatic pong is the return traffic that stops an idle-timeout proxy/LB from
  dropping a quiet stream.
- `ws_keepalive_timeout` (default 20s, gRPC's default) bounds the wait for a
  response. Any inbound frame — the pong, or ordinary stream data — proves liveness;
  if **nothing** arrives for `ws_keepalive + ws_keepalive_timeout`, the peer is
  presumed dead and the connection is **dropped** (the streams then surface
  `UNAVAILABLE`). This detects half-open connections in seconds instead of waiting
  out the OS TCP timeout.

Both are off by default (`ws_keepalive: None`). There is no application-level
ping/pong frame (the `Frame` oneof reserves the old field numbers 6/7).

### Auth

Auth is **per stream** (like gRPC call credentials / grpc-web — the browser has no
connection-level credential it can drive). `ServerConfig::stream_auth` is the
authoritative check, run on **every grpc-webnext stream on both transports** — each
WebSocket `Subscribe` and each unary Fetch call (a unary RPC is a one-shot stream). It
receives the method and the request metadata (`authorization` rides in the `Subscribe`
frame on WebSocket, or the HTTP request headers on Fetch) and returns a `Status`. That
status is not hardcoded to `UNAUTHENTICATED`: the hook may return any code (e.g.
`PERMISSION_DENIED` for a valid-but-unauthorized token). On WebSocket a failure becomes
a `Reset` carrying that status; on Fetch it becomes the response's `grpc-status` (so a
denied `+proto` call still returns HTTP 200 with the status in the trailer block, and a
`+json` call carries it in the `grpc-status` header). Native `application/grpc`
passthrough is **exempt** — that's the raw gRPC surface, guarded by the router's own
interceptors, not a grpc-webnext-translated stream.

On WebSocket there's an **optional handshake gate** so a bad credential can be rejected
*before any frame is read*. A browser can set only one handshake header —
`Sec-WebSocket-Protocol` — so the client auto-derives a `bearer.<token>` subprotocol from
the call's `authorization` metadata (stripping a `Bearer ` scheme; non-token-safe
credentials are skipped and just flow in the frame). It fires **only when that credential
is present**, and it needs to know *which method* the credential is scoped to:

- **Single-stream**: the method is the URL path — unambiguous (socket ≡ stream).
- **Multiplexed**: the socket is shared, so the opening call adds its method as a
  `?method=` query. A `bearer.*` subprotocol **without** `?method=` is a hard reject
  (there's nothing to scope it to). Later streams on the socket carry their own
  `authorization` in their `Subscribe` frame (per-stream `stream_auth`); the handshake
  gate only vets the opener.
- **No credential** → the connection just opens; every stream self-authenticates.

`ServerConfig::connect_auth(method, headers)` runs the gate (read the token with the
`ws_bearer_token` helper). On `Err(status)` the server **accepts the upgrade then
immediately closes** with a private code **`4000 + gRPC status`** (`4016` UNAUTHENTICATED),
message in the reason — JS reads `CloseEvent.code`/`.reason` and reconstructs a `Status`;
no stream is created. This is a WebSocket-specific early-reject optimization, not a
gRPC connection-auth layer (gRPC's connection auth is mTLS, which browser JS can't drive).

### stream_id

Meaningful only in **multiplexed** (`+multi`) mode, where it disambiguates streams on a
shared socket. In single-stream mode the socket *is* the stream, so `stream_id` carries
no information: the **JSON** codec omits it from the wire entirely, while the **binary**
codec still carries it fixed at `1` (protobuf has no field omission — see the binary
single-stream note above). Either way the server ignores the wire value and uses `1`.

### Multiplexing

**Off by default** — each stream gets its own WebSocket. A client opts in by offering a
`+multi` subprotocol; the pool size is a **client config** (N WebSockets, streams
round-robin). This is not part of the wire format beyond the subprotocol — the server
only ever sees streams arriving on connections. See `COMPATIBILITY.md`.

## Limits & error surfaces (wire-observable)

The happy paths above are the contract; these are the rejection and limit branches a
non-conforming or oversized client will actually hit. Where the native server and the
proxy differ, both are called out — those differences are wire-observable.

### Message size limits (`max_message_bytes`, default 4 MiB)

- **Fetch `+proto`** (server and proxy): only the 4-byte length prefix is read; if the
  *declared* length exceeds the limit → **HTTP 413** with a plain-text body
  `"request message exceeds size limit"`. Nothing is buffered to measure.
- **Fetch `+json`** (server and proxy): the oversized body is rejected with
  **`RESOURCE_EXHAUSTED` in the `grpc-status` header** (HTTP 200) — *not* an HTTP 413. The
  JSON codec always carries status in the header, so both surfaces answer identically.
- **WebSocket** (server and proxy): an inbound message whose payload exceeds the limit
  (including a `Subscribe`'s inline `initial_payload`) terminates that stream with a
  `Reset{RESOURCE_EXHAUSTED, "request message exceeds size limit"}`; the connection and its
  other streams keep running. The transport's own frame limits (tungstenite's defaults)
  are a coarser backstop above that.

### Stream-level errors — in-band frames (WebSocket)

These arrive as ordinary frames on an already-open connection (not close codes):

- **Duplicate `stream_id`** in a `Subscribe` → `Reset{INVALID_ARGUMENT, "stream_id already
  in use"}` (server and proxy).
- **`+json` with no transcoder/schema** → `Reset{UNIMPLEMENTED}` (server and proxy) — a
  pre-RPC capability rejection, so a `Reset`, not a terminal `Trailer`. (The message
  differs: the server says `"+json needs a transcoder"`, the proxy explains it's
  binary-only.)
- **Too many concurrent streams** — proxy only (`max_concurrent_streams`, default 100) →
  `Reset{RESOURCE_EXHAUSTED, "too many concurrent streams"}`. The native server has no
  per-connection stream cap.

The split between the two frame kinds is a **convention** worth stating: a `Reset` carries
a status for a stream rejected *before or outside* the RPC lifecycle (bad/oversized frame,
duplicate id, missing capability, auth failure, client cancel); a `Trailer` carries the
*terminal gRPC status of an RPC that reached the router/upstream* (normal completion or an
error status it returned). Both render to the same `{status}` JSON frame, so on a JSON
connection the distinction is invisible; it's observable only on a binary connection.

The `4000 + code` **close code** is used only for **connection-level** rejections at the
handshake (missing codec subprotocol → `4012`; auth-gate reject → `4016`), never for these
per-stream errors on an open connection.

### Fetch error surfaces (no transcoder)

- `+json`/JSON with **no** transcoder/schema configured → **`UNIMPLEMENTED` in the
  `grpc-status` header** (HTTP 200) on both the native server (no `ServerConfig::transcoder`)
  and the proxy (`SchemaSource::None`) — *not* an HTTP 501. With a transcoder present but the
  *method* unknown → likewise HTTP 200 + `grpc-status: 12`.
- Unknown content-type → **415**; `+proto` on a REST-annotated URL → **415**.

### JSON frame edge semantics (`json_frame_to_proto`)

The frame kind is chosen by which field is present, in priority order
`method` → `status` → `halfClose` → `message` → *(none)*. Consequences:

- A frame with **no recognized field** (`{}`, or a mistyped field name) decodes as a
  **half-close** — a typo silently ends the send side.
- A multi-mode **open may combine** `{streamId, method, message}` in one frame: `message`
  becomes the `Subscribe`'s initial payload (open + first data together). A single-stream
  open frame carrying `message` does the same.
- Because `halfClose` outranks `message`, a frame carrying **both** ends the stream and
  drops the message.
- **Binary (`-bin`) metadata is dropped** crossing into the JSON codec — JSON frames carry
  ASCII metadata only.

### Encoding details

- **`grpc-message`** header values are **percent-encoded** — ASCII alphanumerics plus
  `` (space) `-` `_` `.` `/` `:` `` pass through, everything else becomes `%XX` — on the
  JSON Fetch path. The `+proto` paths carry status in a protobuf `Trailer`, so no header
  encoding applies there.
- **WebSocket close reasons** truncate to **123 bytes** on a UTF-8 char boundary
  (WebSocket caps the reason at 123 bytes). Native server only — the proxy has no
  close-code path.
- **`bearer.<token>` subprotocol**: "token-safe" means RFC 7230 token chars (`tchar`). The
  client strips a case-insensitive `Bearer ` scheme, then offers a **lowercase**
  `bearer.<token>` subprotocol only if the remaining token is token-safe (otherwise the
  credential just flows in the frame). The server matches the lowercase `bearer.` prefix
  **case-sensitively**.

### Multiplex pool ramp-up (client)

In `+multi` mode the client opens a **new WebSocket per stream until `poolSize` is
reached**, then round-robins streams across the pool (`poolSize` defaults to 1, clamped
≥ 1). Single-stream mode is always one socket per stream (no pool). The "round-robin" in
*Multiplexing* above describes steady state; the ramp-up is per-stream socket creation.

### REST binding precedence

- **Path variables** always overlay the message (applied unconditionally).
- **Query parameters** bind only when `body` is **not** `"*"`. With `body: "*"` (the whole
  body *is* the message) query params are **ignored entirely**; path vars still overlay.
  For a non-wildcard body, a query param naming a field already set by a path var is
  skipped.

## Proxy vs native library: what needs schemas

The proxy always parses the WebSocket `Frame` envelope (stream_id, method, headers,
frame kind) — that is the protocol. Whether it must decode the **application payload**
depends on the codec:

- `+proto` upstream `application/grpc` — payload forwarded **opaquely**, no schema. The
  proxy stays fully schema-agnostic here, fronting any gRPC server (Go, Java, …) with
  zero `.proto` knowledge.
- `+json` (and REST) — needs message descriptors to transcode JSON ↔ protobuf. The proxy
  **terminates** these: JSON request → binary protobuf upstream, binary response → JSON
  back, reusing the same core `Transcoder` as the native server so a client can't tell
  the two surfaces apart.

Descriptors come from a configurable **schema source** (`ProxyConfig::schema`):

| `SchemaSource` | Descriptors from |
|---|---|
| `None` (default) | — `+json`/REST answer `UNIMPLEMENTED` |
| `Reflection` | upstream gRPC server reflection (v1, `v1alpha` fallback) |
| `Bundled(fds)` | a bundled `FileDescriptorSet` |
| `ReflectionOrBundled(fds)` | reflection, with the bundle as an immediate fallback |

Reflection is loaded **eagerly and whole** at startup — `list_services` plus each
service's transitive file closure, assembled into one snapshot — and refreshed on a TTL
(`reflection_ttl`, default 4h); an optional `admin_reload_path` (`POST`) forces an
immediate reload. Requests block (bounded) only for the very first load. The proxy frames
the **raw** descriptor bytes verbatim so custom options (e.g. `google.api.http`) survive —
but the upstream's reflection must itself preserve them; tonic-reflection currently strips
custom options ([grpc/grpc-rust#2719](https://github.com/grpc/grpc-rust/issues/2719), see
`doc/BACKLOG.md`), so REST-over-reflection against a tonic upstream needs a bundled set.
With `None`, `+json`/REST return `UNIMPLEMENTED` as a proper status-in-header (Fetch) /
`Trailer` frame (WS) — never an HTTP 501 (that surface exists only on the native server;
see "Limits & error surfaces").

## Reserved for later (not in v1)

Fragmentation of a single message across frames (for very large messages / fairer
multiplexing) is intentionally out of scope. It would be added as a new `Frame` kind
(e.g. `fragment`) so existing frames are unaffected — a simple round-robin, no
flow control. Not built until there's demand.
