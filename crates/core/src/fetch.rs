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

const LEN_PREFIX: usize = 4;

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
