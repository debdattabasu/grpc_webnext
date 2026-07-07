package webnext

import "net/http"

// Backend is where a translated gRPC call is dispatched. It mirrors the Rust
// `Backend` enum's single job: take a gRPC-framed http.Request and produce the
// gRPC-framed http.Response. Everything wrapped around it — Fetch/WebSocket
// framing, codecs, deadlines, size limits — is surface-agnostic and lives in the
// handler, written once.
//
// A gRPC handler in Go already IS an http.Handler: a *grpc.Server serves gRPC via
// ServeHTTP. So the in-process backend is just that handler; the upstream backend
// is a reverse proxy to a remote gRPC server. Both satisfy http.Handler.
type Backend interface {
	http.Handler
	// isUpstream reports whether calls leave the process (affects deadline handling:
	// a remote call is enforced locally + forwarded; an in-process call is delegated).
	isUpstream() bool
}

// InProcess wraps a native gRPC handler (typically a *grpc.Server) served in the
// same process. Native application/grpc traffic is passed through to it untouched,
// so browsers and existing gRPC clients share one endpoint.
func InProcess(h http.Handler) Backend { return inProcess{h} }

type inProcess struct{ h http.Handler }

func (b inProcess) ServeHTTP(w http.ResponseWriter, r *http.Request) { b.h.ServeHTTP(w, r) }
func (b inProcess) isUpstream() bool                                 { return false }

// Upstream forwards to a remote gRPC server. This is the schema-agnostic proxy
// backend — the Go analog of the Rust `Backend::Upstream(Channel)`.
//
// TODO(spec): implement the HTTP/2 (h2c) reverse proxy to `target`. The Rust proxy
// binary already covers the language-agnostic proxy use case, so a Go proxy is
// optional; this exists to keep the in-process and upstream shapes symmetric.
func Upstream(target string) Backend { return upstream{target} }

type upstream struct{ target string }

func (b upstream) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	http.Error(w, "upstream backend not yet implemented", http.StatusNotImplemented)
}
func (b upstream) isUpstream() bool { return true }
