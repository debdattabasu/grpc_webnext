package webnext

// Wire types for the WebSocket envelope. THESE ARE A HAND-WRITTEN PLACEHOLDER for
// code generated from /proto/grpc_webnext.proto (protoc-gen-go / buf). They mirror
// the proto so the handler can be written against a stable shape now; swap in the
// generated package once codegen is wired into the monorepo, then delete this file.
//
// One WebSocket carries exactly ONE gRPC stream, so frames carry no stream id: the
// first frame is a Subscribe (method from the WS URL), then Message / HalfClose /
// Reset flow on the same socket. Exactly one field of Frame is set per message.

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

// Subscribe opens the stream; the first frame of the connection.
type Subscribe struct {
	Method         string // "/pkg.Service/Method"
	Headers        []Metadatum
	TimeoutMillis  uint32 // gRPC deadline; 0 = none
	InitialPayload []byte // optional first message (unary-style)
	JSON           bool   // payloads are +json, not binary
}

// Header carries initial response metadata, sent once before the first Message.
type Header struct {
	Headers []Metadatum
}

type Message struct {
	Payload []byte
}

// HalfClose signals the client is done sending (a bare marker).
type HalfClose struct{}

// Trailer ends the stream with a terminal gRPC status (server -> client).
type Trailer struct {
	StatusCode    Code
	StatusMessage string
	Trailers      []Metadatum
}

// Reset aborts the stream, or rejects it pre-RPC (no prior capability negotiation).
type Reset struct {
	StatusCode    Code
	StatusMessage string
}
