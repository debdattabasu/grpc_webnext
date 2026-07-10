//! Fetch response framing: `[len·message][len·trailer]` encode/decode, size limit,
//! truncation detection. (Moved out of `src/framing.rs`'s inline `#[cfg(test)]`.)

use bytes::Bytes;
use grpc_webnext::pb::{metadatum, Metadatum, Trailer};
use grpc_webnext::{decode_response_body, encode_response_body, FetchError};

#[test]
fn round_trips_message_and_trailer() {
    let trailer = Trailer {
        status_code: 0,
        status_message: "OK".into(),
        trailers: vec![Metadatum {
            key: "x-custom".into(),
            value: Some(metadatum::Value::AsciiValue("v".into())),
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
