# Transport Compatibility Notes

grpc-webnext aims for **identical gRPC semantics**. Everything that is *client-side
policy* matches standard gRPC exactly. A few things are properties of the HTTP/2
transport itself, which the browser does not expose — there we match the semantics
and the config surface, but the underlying mechanism differs by transport.

Legend: ✅ identical · ⚠️ semantics match, mechanism differs · ⛔ accepted for API
compatibility but inert on this transport.

| Feature | Rust (native/proxy) | Node client | Browser — Fetch (unary) | Browser — WebSocket (stream) |
|---|---|---|---|---|
| Deadlines / timeouts | ✅ | ✅ | ✅ `grpc-timeout` + timer | ✅ envelope field + timer |
| Retries (service config, backoff, hedging) | ✅ | ✅ | ✅ client-side policy | ✅ client-side policy |
| Cancellation | ✅ | ✅ | ✅ `AbortController` | ✅ control frame |
| Wait-for-ready | ✅ | ✅ | ✅ | ✅ |
| Max message size / compression | ✅ | ✅ | ✅ | ✅ |
| Resolver (name → endpoint list) | ✅ | ✅ | ✅ endpoints are **URLs** | ✅ endpoints are **URLs** |
| LB policy (pick_first, round_robin, custom) | ✅ | ✅ | ✅ picks among URLs | ✅ picks among WS connections |
| Subchannel = managed transport connection | ✅ | ✅ | ⚠️ logical URL bucket; state **inferred** from responses, browser owns the socket pool | ✅ subchannel = a `WebSocket`, **real** connection state |
| Keepalive pings (`GRPC_ARG_KEEPALIVE_*`) | ✅ | ✅ | ⛔ browser owns the connection; no JS access to h2 PING | ⚠️ emulated with app-level ping frame |
| DNS fan-out under one authority (many IPs → many subchannels) | ✅ | ✅ | ⛔ no per-IP pinning; resolver must emit distinct URLs | ⛔ same |

## Why the browser diverges

- **Subchannels (Fetch path).** `fetch(url)` selects a hostname; the browser does DNS
  + connection pooling and gives no way to pin a request to a specific resolved IP,
  and no persistent socket object to observe. So a "subchannel" on the fetch path is a
  logical routing bucket at **URL granularity**, and its connectivity state is inferred
  from request success/failure rather than read from the transport. The LB architecture
  (resolver → policy → picker) is fully faithful; only the subchannel↔connection binding
  degrades.
- **Subchannels (WebSocket path).** A subchannel *is* a `WebSocket` object, so
  `readyState` / `onopen` / `onclose` give real CONNECTING→READY→TRANSIENT_FAILURE
  transitions. This is a faithful port. The Node client (real sockets) matches on both
  transports.
- **Keepalive.** HTTP/2 PING is not reachable from browser JS. Accepted as config for
  compatibility; emulated over WebSocket with an app-level ping frame; a no-op on Fetch.

## Same-port serving (README point 9)

Content-type disambiguates the **request-based** RPCs on one HTTP/2 listener:
`application/grpc` (native) vs `application/grpc-webnext+proto` / `+json`. The
**WebSocket** streaming transport is *not* content-type disambiguated — it arrives as an
HTTP/1.1 `Upgrade: websocket` handshake, so the server must accept, on one socket: h2
gRPC, h2 grpc-webnext unary, and an h1 WebSocket upgrade. Browsers negotiate h2 only over
TLS (ALPN), so "same port" means a TLS port; plaintext h2c from a browser is not
available.

## Multiplexing (README points 10–11)

This is **not** HTTP/2-style framing. The rules are deliberately minimal:

- **1 gRPC message = 1 WebSocket message. No fragmentation.** No reassembly, no frame
  interleaving, no per-stream credit windows. Backpressure is TCP + `bufferedAmount`.
  Keeping messages atomic is also what keeps the browser DevTools Network → Messages
  panel readable.
- **No negotiation.** If a server has a feature disabled and the client opens a stream
  that needs it, the server replies with a `Reset`. Nothing is negotiated in the handshake.
- **One WebSocket per stream on this path; multiplexing lives elsewhere.** The custom
  `Frame` path opens a fresh WebSocket per stream, which under HTTP/1.1 is subject to the
  browser's ~6-connections/host cap. That cap is exactly why the **binary default runs real
  HTTP/2 over h2ts** — one WebSocket, many streams, multiplexed natively (see
  [doc/H2TS_INTEGRATION.md](../doc/H2TS_INTEGRATION.md)). Over HTTP/2 the browser also
  multiplexes WebSockets for free (RFC 8441 extended CONNECT), though Safari does so only
  when it can reuse an already-open h2 connection.

### Consequences to design for

- **Hard max-message-size.** Because a message cannot span frames, an oversized message
  is one giant WS frame — both ends must enforce a configurable size limit (same knob as
  README point 5).
- **No cross-stream head-of-line blocking on this path.** Each stream has its own
  WebSocket, so a large message on one stream never delays another. (Within a stream, a
  large atomic message still occupies the socket for its transmit duration — bounded by
  max-message-size.)
