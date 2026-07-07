//! gRPC wire message framing: `[1-byte compression flag][4-byte big-endian
//! length][message bytes]`.
//!
//! The proxy never needs this (tonic's client codec does the framing), but the
//! native server library calls a tonic `Routes` directly and must frame the
//! request body and de-frame the response body itself.

use bytes::{Buf, BufMut, Bytes, BytesMut};

const HEADER_LEN: usize = 5;

/// Frame a single message for the gRPC wire (uncompressed).
pub fn frame(message: &[u8]) -> Bytes {
    let mut buf = BytesMut::with_capacity(HEADER_LEN + message.len());
    buf.put_u8(0); // compression flag: none
    buf.put_u32(message.len() as u32);
    buf.put_slice(message);
    buf.freeze()
}

/// Incremental de-framer: push raw body chunks, pull complete messages.
#[derive(Default)]
pub struct Deframer {
    buf: BytesMut,
}

impl Deframer {
    pub fn new() -> Self {
        Self::default()
    }

    /// Append a chunk of the gRPC body stream.
    pub fn push(&mut self, chunk: &[u8]) {
        self.buf.put_slice(chunk);
    }

    /// Pull the next complete message, if one is fully buffered.
    pub fn next_message(&mut self) -> Option<Bytes> {
        if self.buf.len() < HEADER_LEN {
            return None;
        }
        // Peek length without consuming, in case the body isn't complete yet.
        let len = u32::from_be_bytes([self.buf[1], self.buf[2], self.buf[3], self.buf[4]]) as usize;
        if self.buf.len() < HEADER_LEN + len {
            return None;
        }
        self.buf.advance(HEADER_LEN);
        Some(self.buf.split_to(len).freeze())
    }

    /// Whether all pushed bytes have been consumed into messages.
    pub fn is_empty(&self) -> bool {
        self.buf.is_empty()
    }
}

/// De-frame a fully-buffered body into all its messages (unary/collected use).
pub fn deframe_all(body: &[u8]) -> Vec<Bytes> {
    let mut d = Deframer::new();
    d.push(body);
    let mut out = Vec::new();
    while let Some(m) = d.next_message() {
        out.push(m);
    }
    out
}
