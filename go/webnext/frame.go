package webnext

// Wire types for the WebSocket envelope. THESE ARE A HAND-WRITTEN PLACEHOLDER for
// code generated from /proto/grpc_webnext.proto (protoc-gen-go / buf). They mirror
// the proto so the handler can be written against a stable shape now; swap in the
// generated package once codegen is wired into the monorepo, then delete this file.
//
// Exactly one field of Frame is set per WebSocket message (the proto `oneof kind`).

type Frame struct {
	Subscribe *Subscribe
	Message   *Message
	HalfClose *HalfClose
	Trailer   *Trailer
	Reset     *Reset
	Header    *Header
}

// Metadatum is one metadata entry. Binary values (keys ending "-bin") use BinValue.
type Metadatum struct {
	Key        string
	ASCIIValue string
	BinValue   []byte
}

// Subscribe opens a new logical stream; first frame of every call.
type Subscribe struct {
	StreamID       uint32
	Method         string // "/pkg.Service/Method"
	Headers        []Metadatum
	TimeoutMillis  uint32 // gRPC deadline; 0 = none
	InitialPayload []byte // optional first message (unary-style)
	JSON           bool   // payloads are +json, not binary
}

// Header carries initial response metadata, sent once before the first Message.
type Header struct {
	StreamID uint32
	Headers  []Metadatum
}

type Message struct {
	StreamID uint32
	Payload  []byte
}

type HalfClose struct {
	StreamID uint32
}

// Trailer ends a stream with a terminal gRPC status (server -> client).
type Trailer struct {
	StreamID      uint32
	StatusCode    Code
	StatusMessage string
	Trailers      []Metadatum
}

// Reset aborts one stream, or rejects it pre-RPC (no prior capability negotiation).
type Reset struct {
	StreamID      uint32
	StatusCode    Code
	StatusMessage string
}
