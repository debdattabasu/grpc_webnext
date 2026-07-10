# grpc-webnext conformance suite

**Purpose: keep independent implementations of one wire protocol from drifting.**

grpc-webnext is now implemented five times over — Rust server + proxy, Go server, Node
server, and TS + Rust clients — all speaking the single wire format in
[`/proto/grpc_webnext.proto`](../proto/grpc_webnext.proto) and
[`/spec/PROTOCOL.md`](../spec/PROTOCOL.md). Across languages you cannot share code, so the
thing that guarantees they agree is a **language-neutral, wire-level conformance suite**:
declarative scenarios, run over the actual transports, against every implementation.

This is the polyglot analog of what unified the Rust proxy and server internally (one
`Backend` enum, so a protocol change is written once). That single audit found *four*
proxy/server divergences (see [`/doc/STATUS.md`](../doc/STATUS.md)). With three server
implementations and two clients, that class of bug can only be caught by running the
protocol, not by reading the code. This suite is that runner.

## The model

```
             cases/*.yaml  (declarative scenarios, this directory)
                   │
                   ▼
   ┌───────────────────────────────┐        grpc-webnext wire
   │  client driver (under test)   │  ────────────────────────────►  ┌──────────────────────┐
   │  TS driver | Rust driver | …  │   Fetch (unary) / WS (stream)    │  server (under test) │
   └───────────────────────────────┘  ◄────────────────────────────  │  Rust | Go | Node    │
                   │                                                  │  serves              │
                   ▼                                                  │  ConformanceService  │
              pass / fail / skip report                               └──────────────────────┘
```

Every **server** serves one fixed service —
[`ConformanceService`](proto/conformance.proto) — whose requests carry a
`ResponseDefinition` telling the server exactly how to respond (payload, status,
metadata, timing, oversize). One generic service therefore exercises every protocol
feature with no per-case server code.

Every **client driver** reads the case files, drives the RPCs over the requested
transport + codec, and asserts the observed wire behavior.

The guarantee is the **matrix**: `{client drivers} × {server impls} × {transports} × {codecs}`.

## The runner contract

An implementation joins the matrix by providing one (or both) of:

### A conformance **server**

An executable that:

1. Serves `grpc.webnext.conformance.v1.ConformanceService` over grpc-webnext (Fetch +
   WebSocket + native gRPC on one port, per the spec).
2. Is configured by a **profile** (see below), passed via environment variables.
3. Prints exactly `LISTENING http://<addr>` to stdout once ready, then runs until killed.
   *(This is the same readiness convention the Rust `devserver`/greeter examples already
   use, so existing harness code carries over.)*

### A conformance **client driver**

An executable that:

1. Takes a target base URL and a set of case files.
2. Runs each case (expanding `transports × codecs`) and attaches any `request_metadata`.
3. Emits a report: one `PASS` / `FAIL` / `SKIP` per expanded case, with a diff on failure.

The reference client driver is the **TS client** (`node/packages/client`), because it is
the mature reference implementation; the **Rust client** gains a driver once it exists.

## Server config profiles

Some cases only make sense under a specific server configuration (`requires:` in a case).
The harness starts the server in the matching profile via env vars; a server that cannot
honor a profile marks those cases **SKIPPED** — surfaced in the report, **never silently
passed**.

| `requires:` key        | env the harness sets            | meaning |
|------------------------|---------------------------------|---------|
| `max_message_bytes: N` | `CONFORMANCE_MAX_MESSAGE_BYTES` | server enforces an N-byte message cap |
| `transcoder: true`     | `CONFORMANCE_TRANSCODER=1`      | server has a `+json` transcoder for the conformance descriptors |
| `transcoder: false`    | `CONFORMANCE_TRANSCODER=0`      | server has **no** transcoder (capability-gap cases) |

## The cases

Declarative YAML validated by [`schema/case.schema.json`](schema/case.schema.json).
Byte values are `{ text: "…" }` (UTF-8) or `{ b64: "…" }`. See the schema for the full
grammar. Current coverage:

| Suite | File | Covers |
|-------|------|--------|
| unary | [cases/unary.yaml](cases/unary.yaml) | OK, empty payload, non-OK status + trailing metadata, response headers |
| streaming | [cases/streaming.yaml](cases/streaming.yaml) | server-stream (incl. messages-then-error), client-stream aggregate, bidi echo, client cancel → CANCELLED |
| deadline | [cases/deadline.yaml](cases/deadline.yaml) | unary + stream `grpc-timeout` expiry (DEADLINE_EXCEEDED); within-deadline passes |
| limits | [cases/limits.yaml](cases/limits.yaml) | oversize request rejected on every path, large response intact, `+json` w/o transcoder → UNIMPLEMENTED, ASCII+`-bin` metadata round-trip |

Each case runs under every applicable **transport profile**: `proto/h2ts` (real gRPC over
the h2ts tunnel), `proto/ws` (the custom `Frame` path, unary over Fetch), and `json` (the
custom path, Fetch + WS). 45 case×profile runs, all green.

**Known gaps** (surfaced by the run — tracked, not silently passed):
- **Response-size enforcement.** `max_message_bytes` now bounds inbound *request* messages on
  every path (fetch/ws bound the request body; the h2ts path checks the gRPC frame length
  prefix). But an oversize *response* isn't rejected — the custom paths don't check outbound
  message size and the h2ts path leaves it to tonic's own encode limit. Also, a *mid-upload*
  request rejection surfaces as a stream/transport failure rather than a clean
  RESOURCE_EXHAUSTED on the Fetch/h2ts paths (inherent HTTP/2 semantics), so the case asserts
  only that it was rejected. Settle the response-size policy, then extend.

The first run also **found two real bugs, now fixed**: trailing metadata on a trailers-only
(error) response was dropped on the h2ts client and the Fetch server path (both read
trailing metadata only from a trailers block, but a trailers-only response carries it in the
headers block).

**Not yet covered** (tracked, not silently omitted): WebSocket keepalive/idle-timeout,
connection-level auth (Subscribe rejection), REST/HttpRule transcoding routes, half-close
ordering edge cases. Add these as new suites; extend the table when you do.

## Running

The harness is the TypeScript driver in
[`node/packages/client/test/conformance.test.ts`](../node/packages/client/test/conformance.test.ts):
it loads `cases/*.yaml`, spawns the Rust `conformance-server`
([`rust/examples/conformance-server`](../rust/examples/conformance-server)) once per required
config profile, and drives each case across every transport profile via the TS client,
asserting the observed wire behavior.

```bash
cd node/packages/client && npm test                              # the full suite (incl. conformance)
cd node/packages/client && npx vitest run test/conformance.test.ts   # just the matrix
```

The Rust server is thin: it implements `ConformanceService` on the grpc-webnext in-process
server (modeled on `rust/examples/greeter-server`). A second server impl (Go, Node) plugs in
the same way (below) and the driver gains it as another target.

## Adding an implementation

1. Implement `ConformanceService` on top of your language's grpc-webnext server.
2. Honor the config profiles (env vars above) and the `LISTENING http://<addr>` readiness line.
3. Register it in the harness server table.
4. Run the matrix; every applicable case must PASS or explicitly SKIP.
