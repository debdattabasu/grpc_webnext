# grpc-webnext wire protocol

Two transports, one set of gRPC semantics. Content-type selects the codec on the
request path; the WebSocket upgrade selects the streaming path.

## Content types

| Content-type | Meaning |
|---|---|
| `application/grpc` | native gRPC (untouched, same port) |
| `application/grpc-webnext+proto` | grpc-webnext, binary protobuf |
| `application/grpc-webnext+json` | grpc-webnext, JSON |

## Unary — Fetch

- **Request:** HTTP POST. Metadata → HTTP request headers. Body is the single
  encoded message. `grpc-timeout` header carries the deadline.
- **Response:** the browser cannot read HTTP trailers, so the server writes the
  body as:

  ```
  [ 4-byte big-endian length | message bytes ]
  [ 4-byte big-endian length | trailer block ]
  ```

  The trailer block is an encoded `Trailer` (status + trailing metadata). Response
  headers still carry the initial metadata. The client buffers the whole body up to
  a **configurable size limit** and then splits message / trailer.
- Server-streaming does **not** use Fetch — it goes over WebSocket (Fetch would have
  to buffer the entire stream). Fetch is unary only.

## Streaming — WebSocket

One protobuf `Frame` per WebSocket message (see `proto/grpc_webnext.proto`).
**One message per frame, no fragmentation.**

Lifecycle of a call:

1. Client → `Subscribe{ stream_id, method, headers, timeout_millis, initial_payload? }`
2. Client → `Message{ stream_id, payload }` … (client-streaming)
3. Client → `HalfClose{ stream_id }` when done sending
4. Server → `Message{ stream_id, payload }` … (server-streaming)
5. Server → `Trailer{ stream_id, status_code, status_message, trailers }` ends the stream
6. Either side → `Reset{ stream_id, status_code }` aborts just that stream

`Ping`/`Pong` are app-level keepalive (HTTP/2 PING is not reachable from browser JS).

### stream_id

Client-allocated, unique per WebSocket. Set on every stream frame so single-stream
and multiplexed-pool modes are the **same wire format**. In single-WS-per-stream mode
it is still present (typically `1`).

### Multiplexing

Optional, client-side. Many streams may share one WebSocket. If the server has
multiplexing disabled and a client sends a second `Subscribe` on a WebSocket, the
server answers `Reset` for that stream — **no negotiation**. See `COMPATIBILITY.md`.

Pool sizing is a **client config**: a fixed pool of N WebSockets, streams assigned
round-robin. This is not part of the wire format — the server only ever sees streams
arriving on connections.

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
