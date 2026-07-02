//! A tonic [`Codec`] that passes message bytes through untouched.
//!
//! This is what makes the proxy schema-agnostic: tonic handles the gRPC
//! length-prefix framing and hands us exactly one message body per `decode`
//! call, so we never need the `.proto` to forward `+proto` payloads.

use bytes::{Buf, BufMut, Bytes};
use tonic::codec::{Codec, DecodeBuf, Decoder, EncodeBuf, Encoder};
use tonic::Status;

/// Encodes/decodes gRPC messages as opaque [`Bytes`].
#[derive(Debug, Clone, Default)]
pub struct BytesCodec;

impl Codec for BytesCodec {
    type Encode = Bytes;
    type Decode = Bytes;
    type Encoder = BytesEncoder;
    type Decoder = BytesDecoder;

    fn encoder(&mut self) -> Self::Encoder {
        BytesEncoder
    }
    fn decoder(&mut self) -> Self::Decoder {
        BytesDecoder
    }
}

#[derive(Debug)]
pub struct BytesEncoder;

impl Encoder for BytesEncoder {
    type Item = Bytes;
    type Error = Status;

    fn encode(&mut self, item: Bytes, dst: &mut EncodeBuf<'_>) -> Result<(), Status> {
        dst.put_slice(&item);
        Ok(())
    }
}

#[derive(Debug)]
pub struct BytesDecoder;

impl Decoder for BytesDecoder {
    type Item = Bytes;
    type Error = Status;

    fn decode(&mut self, src: &mut DecodeBuf<'_>) -> Result<Option<Bytes>, Status> {
        // tonic limits `src` to exactly one message body (possibly empty).
        let len = src.remaining();
        Ok(Some(src.copy_to_bytes(len)))
    }
}
