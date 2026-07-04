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

- **Request:** HTTP POST. Metadata → HTTP request headers. Body is the single
  encoded message (binary protobuf, or JSON text for `+json`). `grpc-timeout`
  header carries the deadline.
- **Response (`+proto`):** the browser cannot read HTTP trailers, so the server
  writes the body as two length-prefixed blocks:

  ```
  [ 4-byte big-endian length | message bytes ]
  [ 4-byte big-endian length | Trailer block ]
  ```

  The trailer block is an encoded `Trailer` (status + trailing metadata). The
  client buffers the whole body up to a **configurable size limit**.
- **Response (`+json`):** the body is the **bare JSON message**; gRPC status and
  metadata travel in HTTP response headers (`grpc-status`, `grpc-message`, plus
  metadata). No length-prefix, no trailer block — a plain JSON API you can `curl`.
- Server-streaming does **not** use Fetch — it goes over WebSocket (Fetch would have
  to buffer the entire stream). Fetch is unary only.

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
endpoints accept only plain HTTP (no content-type or `application/json`), never the
grpc-webnext content-types. A plain request whose URL matches no binding is a direct
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
connection-level credential it can drive). The `authorization` metadata rides in each
stream's open/`Subscribe` frame and is the authoritative check (`ServerConfig::stream_auth`
→ `Reset{ UNAUTHENTICATED }` on failure). Fetch is the same: `authorization` is an HTTP
request header, and the response carries `grpc-status` (`16`).

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

Only present in **multiplexed** (`+multi`) mode, where it disambiguates streams on a
shared socket. Single-stream mode omits it entirely (the socket *is* the stream);
internally the server fixes it to `1`.

### Multiplexing

**Off by default** — each stream gets its own WebSocket. A client opts in by offering a
`+multi` subprotocol; the pool size is a **client config** (N WebSockets, streams
round-robin). This is not part of the wire format beyond the subprotocol — the server
only ever sees streams arriving on connections. See `COMPATIBILITY.md`.

## Proxy vs native library: what needs schemas

The proxy always parses the WebSocket `Frame` envelope (stream_id, method, headers,
frame kind) — that is the protocol. Whether it must decode the **application payload**
depends on the codec:

- `+proto` upstream `application/grpc` — payload forwarded **opaquely**, no schema.
- `+json` — would require message descriptors to transcode JSON ↔ protobuf.

**v1 decision:** the proxy is **binary-only**. It forwards `+proto` opaquely and rejects
`+json` with `UNIMPLEMENTED`. JSON is served by the native library, which already has the
message descriptors in-process. This keeps the proxy fully schema-agnostic and able to
front any gRPC server (Go, Java, …) with zero `.proto` knowledge.

## Reserved for later (not in v1)

Fragmentation of a single message across frames (for very large messages / fairer
multiplexing) is intentionally out of scope. It would be added as a new `Frame` kind
(e.g. `fragment`) so existing frames are unaffected — a simple round-robin, no
flow control. Not built until there's demand.
