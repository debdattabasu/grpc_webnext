//! grpc-webnext shared core: generated wire types, frame codec, and the
//! Fetch-response framing that both the proxy and the native server library use.

/// Generated protobuf types from `proto/grpc_webnext.proto`.
pub mod pb {
    include!(concat!(env!("OUT_DIR"), "/grpc.webnext.v1.rs"));
}

pub mod codec;
pub mod fetch;
pub mod frame;
pub mod grpc_framing;
pub mod metadata;

pub use codec::BytesCodec;
pub use fetch::{decode_response_body, encode_response_body, FetchError};
pub use frame::{decode_frame, encode_frame, FrameError};
pub use grpc_framing::{deframe_all, frame as grpc_frame, Deframer};
