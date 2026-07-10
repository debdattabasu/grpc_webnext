# grpc-webnext

Full gRPC semantics in the browser — unary over **Fetch**, streaming over
**WebSocket** — on the **same port** as native gRPC. A polyglot implementation of
one wire protocol: native in-process servers per language, a schema-agnostic proxy
for everything else, and clients that mimic the Node gRPC API.

```
        Browser / Node / Rust-WASM clients   (TypeScript ✅ · Rust ⬜)
              │  unary → Fetch          │  streaming → WebSocket
              ▼                          ▼
        ┌──────────────────────────────────────────────────┐
        │  grpc-webnext endpoint (one port)                │
        │   in-process server:  Rust ✅ · Go ⬜ · Node ⬜     │
        │   or standalone proxy (Rust) — front any gRPC    │
        └──────────────────────────────────────────────────┘
              │ native application/grpc (same port, passthrough)
              ▼
        Any gRPC server (tonic, grpc-go, …)
```

The wire format is defined once, in [`proto/grpc_webnext.proto`](proto/grpc_webnext.proto)
and [`spec/PROTOCOL.md`](spec/PROTOCOL.md), and every implementation is held to it by a
language-neutral [conformance suite](conformance/). The **proxy** is the universal
fallback — front any gRPC server in any language; the per-language **in-process servers**
are the native, no-extra-hop path for that runtime.

## Status

| Component | Location | State |
|---|---|---|
| Wire protocol + normative spec | [`proto/`](proto/), [`spec/PROTOCOL.md`](spec/PROTOCOL.md) | ✅ the contract |
| Rust server + proxy | [`rust/crates/grpc-webnext`](rust/crates/grpc-webnext) | ✅ unary + streaming, `+json`, deadlines, cancel, native gRPC same-port |
| TypeScript client (browser + Node) | [`node/packages/client`](node/packages/client) | ✅ Fetch + WebSocket, typed codegen, callback + promise APIs |
| Go in-process server | [`go/webnext`](go/webnext) | ⬜ skeleton (API + router; protocol logic stubbed) |
| Node in-process server | [`node/packages/server`](node/packages/server) | ⬜ skeleton |
| Rust client (WASM / frontend) | `rust/crates/grpc-webnext-client` | ⬜ planned |
| Conformance suite | [`conformance/`](conformance/) | ⬜ proto + cases + contract; harness next |

## Quickstart

Run the full demo (starts the native Rust server, drives it from the TS client):

```bash
cd node/packages/client && npm install && npm run demo
```

Use the TypeScript client in your own code. Two flavors share one transport:

```ts
import { makeClient, makePromiseClient, Metadata } from "@grpc-webnext/client";
import { GreeterDefinition } from "./gen/greeter.js"; // ts-proto generic-definitions

// Callback / EventEmitter (mirrors @grpc/grpc-js)
const cb = makeClient(GreeterDefinition, { baseUrl: "https://api.example.com" });
cb.sayHello({ name: "world" }, (err, reply) => console.log(reply.message)); // Fetch
const ticks = cb.countdown({ from: 3 });                                    // WebSocket
for await (const t of ticks) console.log(t.value);

// Promise / async-iterable (mirrors Connect / nice-grpc)
// (pass `codec: "json"` to send messages as JSON instead of binary protobuf)
const rpc = makePromiseClient(GreeterDefinition, { baseUrl: "https://api.example.com" });
const reply = await rpc.sayHello({ name: "world" });          // unary -> Promise
for await (const t of rpc.countdown({ from: 3 })) log(t);     // server-stream -> AsyncIterable
const sum = await rpc.concat(source);                         // client-stream -> Promise
for await (const m of rpc.chat(source, { signal })) log(m);   // bidi, cancel via AbortSignal
```

Front an existing gRPC server with the proxy (language-agnostic fallback):

```bash
cd rust
UPSTREAM=http://localhost:50051 LISTEN=127.0.0.1:8080 cargo run -p grpc-webnext-proxy
```

Wrap a tonic server in-process (serves grpc-webnext + native gRPC on one port):

```rust
use grpc_webnext::{bind_and_serve_in_process, ServerConfig};

let routes = tonic::service::Routes::new(GreeterServer::new(svc));
let (addr, handle) = bind_and_serve_in_process(routes, ServerConfig::default()).await?;
```

## How it works

- **Binary (default) → real gRPC over h2ts.** The browser speaks real HTTP/2 — trailers,
  flow control, native multiplexing — tunneled over a WebSocket by
  [h2ts](https://github.com/debdattabasu/h2ts), and the server is unmodified tonic behind
  an h2ts gateway. Real gRPC end to end, no translation. Transport is a per-client config
  `{ codec, unary, streaming }`; proto defaults to h2ts for both surfaces.
- **JSON (and binary `streaming: "ws"`) → the custom protocol.** Unary → Fetch, with the
  response body `[u32 len | message][u32 len | trailer]` (browsers can't read HTTP
  trailers). Streaming → **one WebSocket per stream**, one protobuf `Frame` per message
  (`Subscribe` / `Message` / `HalfClose` / `Trailer` / `Reset` / `Header`), no
  fragmentation, plaintext JSON for browser debuggability.
- **Same port.** `content-type` disambiguates `application/grpc` from
  `application/grpc-webnext+proto`; WebSocket arrives as an HTTP/1.1 upgrade. Both
  the proxy and the native server forward native `application/grpc` untouched, so
  browsers and existing gRPC clients share one endpoint.
- **Deadlines & cancellation.** The client sends `grpc-timeout`; the server enforces
  it locally and forwards it downstream. A client `Reset`/disconnect propagates to
  the upstream as a stream reset.
- **Schema-agnostic proxy.** A passthrough codec forwards message bytes without
  needing the `.proto`; `+json` transcoding uses reflection or a bundled descriptor set.

See [spec/PROTOCOL.md](spec/PROTOCOL.md) for the wire format and
[spec/COMPATIBILITY.md](spec/COMPATIBILITY.md) for per-transport gRPC-semantics fidelity.

## Repository layout

Organized by language ecosystem — each toolchain owns its subtree — with the
cross-language contract at the root.

```
proto/                 grpc-webnext wire envelope (Frame, Metadatum, …) — source of truth
spec/                  PROTOCOL.md (normative) + COMPATIBILITY.md
conformance/           language-neutral conformance suite (proto, cases, contract)
doc/                   design notes (STATUS, BACKLOG, UNIFICATION)

rust/                  Cargo workspace
  crates/grpc-webnext/   server library + proxy binary (grpc-webnext-proxy)
  crates/testecho/       test-only Echo service
  crates/devserver/      dev harness (testecho behind the proxy)
  examples/              Greeter service + end-to-end demo

node/                  npm workspace
  packages/client/       @grpc-webnext/client (Fetch + WebSocket transports)
  packages/server/       @grpc-webnext/server (in-process, skeleton)

go/                    Go module (github.com/grpc-webnext/grpc-webnext/go)
  webnext/               in-process server (skeleton)
```

## Design goals

1. Unary RPC over Fetch.
2. All streaming RPCs over WebSocket.
3. Serve binary (`application/grpc-webnext+proto`) or JSON (`application/grpc-webnext+json`).
4. WebSocket sends headers and trailers as protobuf messages.
5. Fetch sends headers and trailers as HTTP headers / a buffered trailer block, with a configurable size limit.
6. Existing protoc works for both TypeScript and the backend.
7. Frontend API mimics Node gRPC (need not be 1:1).
8. Connection management, retry, deadline semantics match standard gRPC.
9. Serve on the same port as native gRPC; content-type disambiguates.
10. Multiplexing: on the binary path, provided natively by HTTP/2 over h2ts (one WebSocket, many streams); the custom `Frame` path is one WebSocket per stream.
11. On the custom `Frame` path, strictly one message per WebSocket message (no HTTP/2-style fragmentation); the binary path uses real HTTP/2 framing via h2ts.

## Development

```bash
cd rust && cargo test                 # Rust: server + proxy (one crate), 90 tests
cd node/packages/client && npm test   # TypeScript: codec + e2e (spawns the Rust example servers)
cd go && go build ./... && go vet ./...  # Go: skeleton builds clean
```

## Releasing

One repo, independent publish targets on their own cadences:

- **Rust** → crates.io (`cargo publish` from `rust/crates/grpc-webnext`).
- **TypeScript** → npm (`@grpc-webnext/client`, `@grpc-webnext/server`).
- **Go** → module `github.com/grpc-webnext/grpc-webnext/go`, tagged **`go/vX.Y.Z`**
  (subdirectory modules require the path-prefixed tag).
