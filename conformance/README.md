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
| unary | [cases/unary.yaml](cases/unary.yaml) | OK, empty payload, non-OK status + trailers, response headers — Fetch **and** WS, both codecs |
| streaming | [cases/streaming.yaml](cases/streaming.yaml) | server-stream (incl. messages-then-error), client-stream aggregate, bidi echo, client cancel → Reset |
| deadline | [cases/deadline.yaml](cases/deadline.yaml) | unary + stream `grpc-timeout` expiry (DEADLINE_EXCEEDED) on both surfaces; within-deadline passes |
| limits | [cases/limits.yaml](cases/limits.yaml) | oversize response → RESOURCE_EXHAUSTED, `+json` w/o transcoder → UNIMPLEMENTED, ASCII+`-bin` metadata round-trip |

**Not yet covered** (tracked, not silently omitted): WebSocket keepalive/idle-timeout,
connection-level auth (Subscribe rejection), REST/HttpRule transcoding routes, half-close
ordering edge cases, trailers-only responses. Add these as
new suites; extend the table above when you do.

## Running (once a harness exists)

The harness itself (case loader + driver glue + server lifecycle) is the next
implementation step — deliberately built *after* a second server exists, so it is written
against two real targets rather than one. Intended shape:

```
conformance/
  proto/conformance.proto     # the service every server implements  ✅
  schema/case.schema.json      # case-file grammar                    ✅
  cases/*.yaml                 # scenarios                            ✅ (first batch)
  runner/                      # harness: load cases, drive, report   ⬜ next
  servers/                     # per-impl conformance server entrypoints ⬜ as impls land
```

Each server entrypoint is thin: it depends on that language's grpc-webnext server library
and implements `ConformanceService`. For Rust that is an example bin in
`rust/crates/grpc-webnext`; for Go, `go/conformance`; for Node,
`node/packages/server` (conformance entry).

## Adding an implementation

1. Implement `ConformanceService` on top of your language's grpc-webnext server.
2. Honor the config profiles (env vars above) and the `LISTENING http://<addr>` readiness line.
3. Register it in the harness server table.
4. Run the matrix; every applicable case must PASS or explicitly SKIP.
