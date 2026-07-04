//! Fetch (unary) response framing.
//!
//! Browsers cannot read HTTP trailers, so a unary response body is written as
//! two 4-byte-big-endian length-prefixed blocks:
//!
//! ```text
//! [ u32 len | message bytes ]
//! [ u32 len | Trailer bytes ]
//! ```
//!
//! Initial metadata still travels in HTTP response headers; the `Trailer` block
//! carries the gRPC status and trailing metadata.

use crate::pb::Trailer;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use prost::Message;

pub const LEN_PREFIX: usize = 4;

#[derive(Debug, thiserror::Error)]
pub enum FetchError {
    #[error("response body exceeds configured size limit ({limit} bytes)")]
    TooLarge { limit: usize },
    #[error("response body truncated: expected {expected} more bytes, had {had}")]
    Truncated { expected: usize, had: usize },
    #[error("failed to decode Trailer: {0}")]
    Decode(#[from] prost::DecodeError),
}

/// Encode a unary response body from the message bytes and its trailer.
pub fn encode_response_body(message: &[u8], trailer: &Trailer) -> Bytes {
    let trailer_len = trailer.encoded_len();
    let mut buf = BytesMut::with_capacity(LEN_PREFIX + message.len() + LEN_PREFIX + trailer_len);
    buf.put_u32(message.len() as u32);
    buf.put_slice(message);
    buf.put_u32(trailer_len as u32);
    trailer.encode(&mut buf).expect("BytesMut has capacity");
    buf.freeze()
}

/// The empty message block (`[u32 len = 0]`): a trailers-only response (an error
/// with no message) still needs the leading message block before the trailer block.
pub const EMPTY_MESSAGE_BLOCK: [u8; LEN_PREFIX] = [0; LEN_PREFIX];

/// Encode a `+proto` **unary request** body: a single `[u32 len | message]` block,
/// mirroring the response's message block. The client prepends the length it already
/// knows (protobuf is serialized whole), so the server/proxy can turn it into a gRPC
/// frame — `[1-byte flag]` + this — and stream it upstream without buffering to measure.
pub fn encode_request_body(message: &[u8]) -> Bytes {
    let mut buf = BytesMut::with_capacity(LEN_PREFIX + message.len());
    buf.put_u32(message.len() as u32);
    buf.put_slice(message);
    buf.freeze()
}

/// Encode just the trailing `[u32 len | Trailer bytes]` block. Used by the streaming
/// response path, which forwards the message block straight from the inner gRPC frame
/// (its `[u32 len | message]` layout already matches ours once the 1-byte compression
/// flag is dropped) and only needs to append this at the end.
pub fn encode_trailer_block(trailer: &Trailer) -> Bytes {
    let trailer_len = trailer.encoded_len();
    let mut buf = BytesMut::with_capacity(LEN_PREFIX + trailer_len);
    buf.put_u32(trailer_len as u32);
    trailer.encode(&mut buf).expect("BytesMut has capacity");
    buf.freeze()
}

/// Decode a buffered unary response body into `(message bytes, Trailer)`.
///
/// `limit` bounds the total body size the caller is willing to buffer.
pub fn decode_response_body(mut body: Bytes, limit: usize) -> Result<(Bytes, Trailer), FetchError> {
    if body.len() > limit {
        return Err(FetchError::TooLarge { limit });
    }

    let message = take_block(&mut body)?;
    let trailer_bytes = take_block(&mut body)?;
    let trailer = Trailer::decode(trailer_bytes)?;
    Ok((message, trailer))
}

/// Read one `[u32 len | bytes]` block, advancing `body`.
fn take_block(body: &mut Bytes) -> Result<Bytes, FetchError> {
    if body.remaining() < LEN_PREFIX {
        return Err(FetchError::Truncated {
            expected: LEN_PREFIX,
            had: body.remaining(),
        });
    }
    let len = body.get_u32() as usize;
    if body.remaining() < len {
        return Err(FetchError::Truncated {
            expected: len,
            had: body.remaining(),
        });
    }
    Ok(body.split_to(len))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pb::Metadatum;

    #[test]
    fn round_trips_message_and_trailer() {
        let trailer = Trailer {
            stream_id: 0,
            status_code: 0,
            status_message: "OK".into(),
            trailers: vec![Metadatum {
                key: "x-custom".into(),
                value: Some(crate::pb::metadatum::Value::AsciiValue("v".into())),
            }],
        };
        let msg = b"hello world";
        let body = encode_response_body(msg, &trailer);

        let (got_msg, got_trailer) = decode_response_body(body, 1024).unwrap();
        assert_eq!(&got_msg[..], msg);
        assert_eq!(got_trailer.status_message, "OK");
        assert_eq!(got_trailer.trailers.len(), 1);
    }

    #[test]
    fn enforces_size_limit() {
        let trailer = Trailer::default();
        let body = encode_response_body(&[0u8; 100], &trailer);
        assert!(matches!(
            decode_response_body(body, 10),
            Err(FetchError::TooLarge { limit: 10 })
        ));
    }

    #[test]
    fn detects_truncation() {
        let body = Bytes::from_static(&[0, 0, 0, 5, 1, 2]); // claims 5, has 2
        assert!(matches!(
            decode_response_body(body, 1024),
            Err(FetchError::Truncated { .. })
        ));
    }
}
