//! Encode/decode a single WebSocket `Frame` (one frame per WS message).

use crate::pb::Frame;
use bytes::{Bytes, BytesMut};
use prost::Message;

#[derive(Debug, thiserror::Error)]
pub enum FrameError {
    #[error("failed to decode Frame: {0}")]
    Decode(#[from] prost::DecodeError),
}

/// Decode one WebSocket binary message into a `Frame`.
pub fn decode_frame(bytes: &[u8]) -> Result<Frame, FrameError> {
    Ok(Frame::decode(bytes)?)
}

/// Encode a `Frame` into the bytes of one WebSocket binary message.
pub fn encode_frame(frame: &Frame) -> Bytes {
    let mut buf = BytesMut::with_capacity(frame.encoded_len());
    frame.encode(&mut buf).expect("BytesMut has capacity");
    buf.freeze()
}
