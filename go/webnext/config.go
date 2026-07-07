package webnext

import "time"

// Content-type tokens that disambiguate grpc-webnext from native gRPC on one port.
// (See PROTOCOL.md "Same port".)
const (
	CTProto = "application/grpc-webnext+proto"
	CTJSON  = "application/grpc-webnext+json"
	CTGRPC  = "application/grpc" // native gRPC (passthrough), matched as a prefix

	// WebSocket subprotocols offered/selected during the handshake.
	WSSubprotocolProto = "grpc-webnext+proto"
	WSSubprotocolJSON  = "grpc-webnext+json"
)

// DefaultMaxMessageBytes mirrors the Rust server/proxy default (4 MiB).
const DefaultMaxMessageBytes = 4 * 1024 * 1024

// ServerConfig configures an in-process grpc-webnext server. Field names and
// defaults mirror the Rust `ServerConfig` so behavior is portable across
// implementations (and identical under the conformance suite).
type ServerConfig struct {
	// Maximum decoded message size, in bytes. A message exceeding this terminates
	// the call with RESOURCE_EXHAUSTED (code 8). 0 uses DefaultMaxMessageBytes.
	MaxMessageBytes int

	// AllowImplicitCodec: if true, a WebSocket handshake with a blank codec
	// subprotocol on a main endpoint defaults to binary instead of being rejected.
	// Defaults to false (strict), matching the unified Rust handshake.
	AllowImplicitCodec bool

	// WebSocket keepalive: send a ping every WSKeepalive; drop the connection if a
	// pong does not arrive within WSKeepaliveTimeout. Zero disables keepalive.
	WSKeepalive        time.Duration
	WSKeepaliveTimeout time.Duration

	// TODO(spec): Transcoder (for +json), ConnectAuth / StreamAuth hooks — add once
	// the JSON and auth surfaces are implemented. See ServerConfig in the Rust crate.
}

func (c ServerConfig) maxMessageBytes() int {
	if c.MaxMessageBytes <= 0 {
		return DefaultMaxMessageBytes
	}
	return c.MaxMessageBytes
}
