// Command greeter sketches serving a gRPC service over grpc-webnext in Go.
//
// It uses a stub backend so it builds without grpc-go. In a real server you pass a
// *grpc.Server (which implements http.Handler) to webnext.InProcess, and the same
// port serves Fetch + WebSocket + native gRPC.
package main

import (
	"log"
	"net/http"

	"github.com/grpc-webnext/grpc-webnext/go/webnext"
)

func main() {
	// Real usage: backend := webnext.InProcess(grpcServer) // grpcServer *grpc.Server
	backend := webnext.InProcess(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "native gRPC handler goes here", http.StatusNotImplemented)
	}))

	addr, run, err := webnext.BindAndServe("127.0.0.1:0", backend, webnext.ServerConfig{})
	if err != nil {
		log.Fatal(err)
	}
	// Same readiness convention as the Rust examples and the conformance harness.
	log.Printf("LISTENING http://%s", addr)
	log.Fatal(run())
}
