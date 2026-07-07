# grpc-webnext — Go

In-process grpc-webnext server for Go: serve full gRPC semantics to browsers
(unary over Fetch, streaming over WebSocket) in front of a native `grpc-go` server,
on the same port as native gRPC.

> **Status: skeleton.** The public API and request router are in place; the
> per-surface protocol logic is stubbed with `TODO(spec)` markers pointing at
> [`/spec/PROTOCOL.md`](../spec/PROTOCOL.md). It builds and vets clean with zero
> external dependencies. See [package doc](webnext/doc.go) for what's done vs pending.

## Layout

```
go/
  go.mod                     module github.com/grpc-webnext/grpc-webnext/go
  webnext/                   the library package
    doc.go                   package overview + status
    config.go                ServerConfig, content-type + subprotocol constants
    backend.go               Backend: InProcess(grpc-go handler) | Upstream(remote)
    server.go                Serve / BindAndServe + the content-type router
    status.go                gRPC status codes + WS close-code mapping
    frame.go                 wire Frame types (hand-written placeholder for generated)
  examples/greeter/          usage sketch (stub backend, builds offline)
```

## Intended usage

```go
grpcServer := grpc.NewServer()          // your native grpc-go server
pb.RegisterGreeterServer(grpcServer, svc)

backend := webnext.InProcess(grpcServer) // *grpc.Server implements http.Handler
addr, run, err := webnext.BindAndServe("127.0.0.1:8080", backend, webnext.ServerConfig{})
if err != nil { log.Fatal(err) }
log.Printf("LISTENING http://%s", addr)
log.Fatal(run())
```

## Build

```bash
cd go
go build ./...
go vet ./...
```

## Roadmap (in dependency order)

1. **Codegen.** Generate the wire types from `/proto/grpc_webnext.proto` (protoc-gen-go
   / buf) and delete the hand-written `frame.go` placeholder.
2. **Fetch unary.** Implement `handleUnaryProto`: length-prefixed request framing,
   dispatch to the backend, `[len|message][len|trailer]` response. Enforce
   `MaxMessageBytes`.
3. **Same-port h2c.** Wrap the server so native `grpc-go` clients (HTTP/2 cleartext)
   and Fetch/WS (HTTP/1.1) share one port.
4. **WebSocket streaming.** Add a WebSocket library (e.g. `github.com/coder/websocket`),
   implement the handshake + Frame loop + keepalive + per-stream deadlines.
5. **`+json`.** Transcoding via a descriptor set; UNIMPLEMENTED when absent.
6. **Conformance.** Add a `ConformanceService` server entrypoint (see
   [`/conformance`](../conformance)) and join the cross-language matrix.

Each step should land green against the conformance suite before the next.
