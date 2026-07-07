# Examples

## Greeter end-to-end demo

A [`Greeter`](greeter.proto) service exercising every RPC cardinality:

| RPC | Cardinality | Transport used by the client |
|---|---|---|
| `SayHello` | unary | Fetch |
| `Countdown` | server streaming | WebSocket |
| `Chat` | bidi streaming | WebSocket |

- [`greeter-server/`](greeter-server/) — a Rust binary that serves `Greeter` over
  grpc-webnext **and** native gRPC on one port, using the native server library
  (the `grpc-webnext` crate).
- The TypeScript client demo lives at
  [`node/packages/client/examples/greeter.ts`](../../node/packages/client/examples/greeter.ts).
  It spawns the server (via `cargo run` in `../rust`), then drives all three RPCs
  with the generated client.

### Run it

```bash
cd node/packages/client
npm install
npm run demo
```

Expected output:

```
[unary]  SayHello -> "Hello, world!"
[server-stream]  Countdown(3):
   tick 3 … tick 0
[bidi]  Chat:
   client: "hi" … server: "echo: hi" …
Demo complete. ✅
```

### Run just the server

```bash
cargo run -p example-greeter-server
# prints: LISTENING http://127.0.0.1:PORT
```

You can then point any grpc-webnext client at it, or a native gRPC client
(`grpcurl`, tonic) at the same port — the content-type disambiguates.
