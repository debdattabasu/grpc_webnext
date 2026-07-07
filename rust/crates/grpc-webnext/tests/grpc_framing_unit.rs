//! gRPC length-prefixed message framing: frame/deframe + split-chunk deframing.
//! (Moved out of `src/grpc_framing.rs`'s inline `#[cfg(test)]`.)

use grpc_webnext::grpc_framing::{deframe_all, frame, Deframer};

#[test]
fn frames_and_deframes() {
    let a = frame(b"hello");
    let b = frame(b"world");
    let mut joined = Vec::new();
    joined.extend_from_slice(&a);
    joined.extend_from_slice(&b);

    let msgs = deframe_all(&joined);
    assert_eq!(msgs.len(), 2);
    assert_eq!(&msgs[0][..], b"hello");
    assert_eq!(&msgs[1][..], b"world");
}

#[test]
fn deframer_handles_split_chunks() {
    let framed = frame(b"chunky");
    let mut d = Deframer::new();
    d.push(&framed[..3]); // partial header
    assert!(d.next_message().is_none());
    d.push(&framed[3..]);
    assert_eq!(&d.next_message().unwrap()[..], b"chunky");
    assert!(d.next_message().is_none());
}
