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
pub mod httprule;
pub mod json_frame;
pub mod metadata;
pub mod transcode;

pub use codec::BytesCodec;
pub use fetch::{
    decode_response_body, encode_request_body, encode_response_body, encode_trailer_block, FetchError,
    EMPTY_MESSAGE_BLOCK, LEN_PREFIX,
};
pub use frame::{decode_frame, encode_frame, FrameError};
pub use grpc_framing::{deframe_all, frame as grpc_frame, Deframer};
pub use httprule::{HttpCall, HttpRouter, WsBinding};
pub use transcode::{Transcoder, TranscodeError};
