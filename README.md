# grpc-webnext

Full gRPC semantics in the browser — unary over **Fetch**, streaming over
**WebSocket** — with a Rust server/proxy and a TypeScript client that mimics the
Node gRPC API. Native gRPC and grpc-webnext coexist on **one port**.

```
        Browser / Node (TypeScript client)
              │  unary → Fetch          │  streaming → WebSocket
              ▼                          ▼
        ┌─────────────────────────────────────────┐
        │  grpc-webnext endpoint (one port)        │
        │   • native server library  — wrap tonic  │
        │   • standalone proxy       — front any    │
        │                              gRPC server  │
        └─────────────────────────────────────────┘
              │ native application/grpc (same port, passthrough)
              ▼
        Any gRPC server (tonic, grpc-go, …)
```

## Status

All three deliverables work end-to-end, covered by integration tests
(`cargo test`, `npm test`) and a runnable [demo](examples/README.md).

| Deliverable | Crate / package | State |
|---|---|---|
| Rust proxy — front any gRPC server | [`grpc-webnext-proxy`](crates/proxy) | ✅ unary + streaming |
| Native Rust server — wrap a tonic `Router` | [`grpc-webnext-server`](crates/server) | ✅ + native gRPC same-port |
| TypeScript client — browser + Node | [`@grpc-webnext/client`](clients/typescript) | ✅ Fetch + WebSocket, typed codegen |
| Shared wire codec & translation | [`grpc-webnext-core`](crates/core) | ✅ |

Binary protobuf is fully wired. JSON, and the remaining connection-management
polish (retry, independent deadline timers, cancellation propagation in the
proxy) are tracked in [BACKLOG.md](BACKLOG.md).

## Quickstart

Run the full demo (starts the native Rust server, drives it from the TS client):

```bash
cd clients/typescript && npm install && npm run demo
```

Use the client in your own code:

```ts
import { makeClient, Metadata } from "@grpc-webnext/client";
import { GreeterDefinition } from "./gen/greeter.js"; // ts-proto generic-definitions

const client = makeClient(GreeterDefinition, { baseUrl: "https://api.example.com" });

client.sayHello({ name: "world" }, (err, reply) => console.log(reply.message)); // Fetch
const ticks = client.countdown({ from: 3 });                                    // WebSocket
for await (const t of ticks) console.log(t.value);
```

Front an existing gRPC server with the proxy:

```bash
UPSTREAM=http://localhost:50051 LISTEN=127.0.0.1:8080 cargo run -p grpc-webnext-proxy
```

Wrap a tonic server in-process (serves grpc-webnext + native gRPC on one port):

```rust
let routes = tonic::service::Routes::new(GreeterServer::new(svc));
grpc_webnext_server::serve(listener, routes, Default::default()).await?;
```

## How it works

- **Unary → Fetch.** Browsers can't read HTTP trailers, so the response body is
  `[u32 len | message][u32 len | trailer]`; the trailer block carries gRPC status.
- **Streaming → WebSocket.** One protobuf `Frame` per WebSocket message
  (`Subscribe` / `Message` / `HalfClose` / `Trailer` / `Reset` / `Header`), no
  fragmentation. Optional client-side multiplexing of many streams over a pool.
- **Same port.** `content-type` disambiguates `application/grpc` from
  `application/grpc-webnext+proto`; WebSocket arrives as an HTTP/1.1 upgrade.
- **Schema-agnostic proxy.** A passthrough `BytesCodec` forwards message bytes
  without needing the `.proto`.

See [PROTOCOL.md](PROTOCOL.md) for the wire format and
[COMPATIBILITY.md](COMPATIBILITY.md) for per-transport gRPC-semantics fidelity.

## Repository layout

```
proto/                     grpc-webnext wire envelope (Frame, Metadatum, …)
crates/
  core/                    wire codec, gRPC framing, passthrough codec, metadata
  proxy/                   standalone proxy (front any gRPC server)
  server/                  native server library (wrap a tonic Router)
  testecho/  devserver/    test-only Echo service + dev harness
clients/typescript/        @grpc-webnext/client (Fetch + WebSocket transports)
examples/                  Greeter service + end-to-end demo
```

## Design goals

The original goals this project targets:

1. Unary RPC over Fetch.
2. All streaming RPCs over WebSocket.
3. Serve binary (`application/grpc-webnext+proto`) or JSON
   (`application/grpc-webnext+json`).
4. WebSocket sends headers and trailers as protobuf messages.
5. Fetch sends headers and trailers as HTTP headers / a buffered trailer block,
   with a configurable size limit.
6. Existing protoc works for both TypeScript and the backend.
7. Frontend API mimics Node gRPC (need not be 1:1).
8. Connection management, retry, deadline semantics match standard gRPC.
9. Serve on the same port as native gRPC; content-type disambiguates.
10. Optional client-side multiplexing of streams over a WebSocket pool.
11. WebSocket multiplexing is strictly one message per WebSocket message (no
    HTTP/2-style fragmentation).

## Development

```bash
cargo test                       # Rust: core, proxy, server
cd clients/typescript && npm test # TypeScript: codec + e2e
```
