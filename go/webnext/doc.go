// Package webnext is the Go implementation of grpc-webnext: it serves full gRPC
// semantics to browsers — unary over Fetch, streaming over WebSocket — in front
// of an in-process grpc-go server, on the same port as native gRPC.
//
// It is the Go sibling of the Rust `grpc-webnext` crate and the Node
// `@grpc-webnext/server` package. All three implement the one wire format defined
// in /proto/grpc_webnext.proto and /spec/PROTOCOL.md, and are held to it by the
// cross-language conformance suite in /conformance.
//
// STATUS: skeleton. The request router (content-type dispatch) is wired; the
// per-surface protocol logic (Fetch framing, WebSocket Frame handling, deadline
// and size enforcement, native-gRPC passthrough on the same port) is stubbed with
// TODO(spec) markers pointing at the relevant PROTOCOL.md sections. The wire types
// in frame.go are a hand-written placeholder for code generated from the proto
// (protoc-gen-go / buf) once codegen is wired into the monorepo.
package webnext
