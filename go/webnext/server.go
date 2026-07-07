package webnext

import (
	"net"
	"net/http"
	"strings"
)

// Serve runs a grpc-webnext server on l until it errors, dispatching translated
// calls to backend. It serves grpc-webnext (unary over Fetch, streaming over
// WebSocket) and native gRPC on the one listener — content-type disambiguates.
func Serve(l net.Listener, backend Backend, cfg ServerConfig) error {
	srv := &http.Server{Handler: &handler{backend: backend, cfg: cfg}}
	// TODO(spec) "Same port": native grpc-go clients speak HTTP/2 cleartext (h2c).
	// Serving both h2c (native gRPC) and HTTP/1.1 (Fetch + WS upgrade) on one port
	// needs an h2c wrapper (golang.org/x/net/http2/h2c) — the first external dep.
	// Until then, only the HTTP/1.1 surfaces (Fetch + WebSocket) are served.
	return srv.Serve(l)
}

// BindAndServe binds addr and returns the bound address plus a run func that
// serves until error (mirrors the Rust `bind_and_serve_in_process` shape). The
// caller typically prints the address, then calls run().
func BindAndServe(addr string, backend Backend, cfg ServerConfig) (net.Addr, func() error, error) {
	l, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, nil, err
	}
	return l.Addr(), func() error { return Serve(l, backend, cfg) }, nil
}

type handler struct {
	backend Backend
	cfg     ServerConfig
}

// ServeHTTP routes a request to the right surface. This mirrors the Rust
// `fetch::handle` dispatch; the per-surface bodies are the remaining work.
func (h *handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if isWebSocketUpgrade(r) {
		h.handleWS(w, r)
		return
	}

	ct := r.Header.Get("Content-Type")
	switch {
	case strings.HasPrefix(ct, CTProto):
		h.handleUnaryProto(w, r)
	case strings.HasPrefix(ct, CTJSON):
		h.handleUnaryJSON(w, r)
	case strings.HasPrefix(ct, CTGRPC):
		// Native gRPC (application/grpc, application/grpc+proto, …). Checked AFTER the
		// +proto/+json cases, which also start with "application/grpc". Passthrough.
		h.backend.ServeHTTP(w, r)
	default:
		http.Error(w, "unsupported content-type: "+ct, http.StatusUnsupportedMediaType)
	}
}

func isWebSocketUpgrade(r *http.Request) bool {
	return strings.EqualFold(r.Header.Get("Upgrade"), "websocket") &&
		strings.Contains(strings.ToLower(r.Header.Get("Connection")), "upgrade")
}

// handleUnaryProto: unary RPC over Fetch, binary protobuf.
//
// TODO(spec) "Unary → Fetch": read the length-prefixed request message (enforcing
// cfg.maxMessageBytes()), build an application/grpc request, dispatch via
// h.backend, then write the response as [u32 len | message][u32 len | trailer],
// the trailer block carrying the gRPC status. See the Rust `unary_proto`.
func (h *handler) handleUnaryProto(w http.ResponseWriter, r *http.Request) {
	http.Error(w, "grpc-webnext+proto unary: not yet implemented", http.StatusNotImplemented)
}

// handleUnaryJSON: unary RPC over Fetch, +json codec (needs a transcoder).
//
// TODO(spec) "+json": transcode JSON<->protobuf around the same dispatch as
// handleUnaryProto. With no transcoder configured, answer UNIMPLEMENTED (code 12).
func (h *handler) handleUnaryJSON(w http.ResponseWriter, r *http.Request) {
	http.Error(w, "grpc-webnext+json unary: not yet implemented", http.StatusNotImplemented)
}

// handleWS: all streaming, over WebSocket.
//
// TODO(spec) "Streaming → WebSocket": complete the handshake (negotiate the codec
// subprotocol; reject a blank codec unless cfg.AllowImplicitCodec), then run the
// Frame loop (Subscribe/Message/HalfClose/Trailer/Reset/Header) with keepalive,
// per-stream deadlines, and size limits. Needs a WebSocket library (e.g.
// github.com/coder/websocket) — to be added as the first external dependency.
func (h *handler) handleWS(w http.ResponseWriter, r *http.Request) {
	http.Error(w, "grpc-webnext websocket: not yet implemented", http.StatusNotImplemented)
}
