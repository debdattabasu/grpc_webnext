package webnext

// Code is a gRPC status code. Values match the canonical gRPC set (and the codes
// the wire protocol carries in Trailer/Reset frames and Fetch trailer blocks).
type Code uint32

const (
	OK                 Code = 0
	Canceled           Code = 1
	Unknown            Code = 2
	InvalidArgument    Code = 3
	DeadlineExceeded   Code = 4
	NotFound           Code = 5
	AlreadyExists      Code = 6
	PermissionDenied   Code = 7
	ResourceExhausted  Code = 8
	FailedPrecondition Code = 9
	Aborted            Code = 10
	OutOfRange         Code = 11
	Unimplemented      Code = 12
	Internal           Code = 13
	Unavailable        Code = 14
	DataLoss           Code = 15
	Unauthenticated    Code = 16
)

// WSCloseCode maps a gRPC status to the WebSocket close code used when a stream is
// rejected pre-RPC (handshake/connection gate): 4000 + code. (See PROTOCOL.md "Auth".)
func WSCloseCode(c Code) int { return 4000 + int(c) }
