# grpc-webnext — JavaScript / TypeScript

npm workspace for the JS/TS side of grpc-webnext.

```
node/
  package.json                 workspace root (packages/*)
  packages/
    client/    @grpc-webnext/client   browser + Node client (Fetch + WebSocket)  ✅
    server/    @grpc-webnext/server   in-process server (front @grpc/grpc-js)     ⬜ skeleton
```

A future `packages/wire` should hold the shared frame codec used by both `client`
and `server` (the same client/server split every language uses). The client and
server are separate packages because they have different runtime targets (browser
vs Node server) and dependency graphs.

## Develop

```bash
cd node
npm install            # installs all workspace packages
npm test               # runs tests across packages (client today)
```

The client's e2e/json/promise tests spawn the Rust example servers
(`cargo run` in `../rust`) to exercise the real wire, so a Rust toolchain is
needed to run the full client suite.
