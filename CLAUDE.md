# grpc-webnext — working notes

Full gRPC semantics in the browser — unary over **Fetch**, streaming over
**WebSocket** — on the same port as native gRPC. Polyglot: one wire protocol,
native in-process servers per language, plus a schema-agnostic proxy.

## Layout (organized by language ecosystem; contract at the root)

```
proto/          wire envelope (Frame, Metadatum, …) — shared source of truth
spec/           PROTOCOL.md (normative) + COMPATIBILITY.md
conformance/    language-neutral conformance suite (proto, cases, contract)
doc/            design notes (STATUS, BACKLOG, UNIFICATION, H2TS_INTEGRATION)
rust/           Cargo workspace  ← NOTE: the workspace is here, NOT the repo root
  crates/grpc-webnext/   server library + proxy binary (grpc-webnext-proxy)
  crates/testecho/       test-only Echo service
  crates/devserver/      dev harness (testecho behind the proxy)
  examples/greeter-server/
node/           npm workspace
  packages/client/       @grpc-webnext/client (Fetch + WebSocket)  ✅
  packages/server/       @grpc-webnext/server (in-process)  ⬜ skeleton
go/             Go module github.com/grpc-webnext/grpc-webnext/go
  webnext/               in-process server  ⬜ skeleton
```

## Build & test

```bash
# Rust (server + proxy live in one crate). Run from rust/.
cd rust && cargo test --workspace          # 90 tests
cd rust && cargo clippy --workspace --all-targets   # keep clean

# TypeScript client. The e2e/json/promise tests spawn the Rust example servers
# (cargo run in ../rust), so a Rust toolchain is needed for the full suite.
cd node/packages/client && npm install && npm test  # 40 tests

# Go skeleton (stdlib-only; builds + vets clean).
cd go && go build ./... && go vet ./...
```

Servers signal readiness by printing `LISTENING http://<addr>` on stdout — the
harness (and conformance runner) parse that line.

## Architecture

One port; `content-type` and the WebSocket subprotocol disambiguate. Transport is a
per-client config `{ codec, unary, streaming }` (Phases 1–3 of the h2ts pivot are done —
see [doc/H2TS_INTEGRATION.md](doc/H2TS_INTEGRATION.md)):

- **Binary (default) → real gRPC over h2ts.** The browser speaks real HTTP/2 (trailers,
  multiplexing) tunneled over a WebSocket by [h2ts](https://github.com/debdattabasu/h2ts);
  the server is unmodified tonic behind an h2ts gateway — `serve_h2` in-process, or a
  byte-transparent `bridge` in the proxy. No translation on this path (`src/h2ts.rs`,
  client `h2ts-transport.ts`).
- **JSON (and binary `streaming: "ws"`) → the custom `Frame` protocol, single-stream.**
  Unary → Fetch, trailer buffered into the body as `[u32 len | message][u32 len | trailer]`
  (browsers can't read HTTP trailers). Streaming → **one WebSocket per stream** carrying
  `Frame`s (Subscribe / Message / HalfClose / Trailer / Reset / Header); the WS URL is the
  method. Plaintext JSON for browser debuggability. No multiplexing — no `stream_id`.

The in-process server (wrap a tonic `Routes`) and the standalone proxy (front any gRPC
upstream) share one code path via a two-variant `Backend` enum
(`InProcess(Routes)` | `Upstream(Channel)`).

## Conventions & gotchas

- The Cargo workspace is in `rust/`, not the repo root — `cargo` commands run from there;
  TS harnesses spawn it via `cwd: <repo>/rust`.
- `proto/grpc_webnext.proto` at the repo root is the shared contract; each language
  generates its own bindings (prost, ts-proto, protoc-gen-go).
- `spec/PROTOCOL.md` is normative; `conformance/` is the cross-language anti-drift guard
  (run every server impl × every client driver over the real wire).
- gRPC status codes are canonical; WS pre-RPC rejection uses close code `4000 + code`.
- macOS BSD `sed` has no `\b` word boundaries — don't use them in scripts here.
```
