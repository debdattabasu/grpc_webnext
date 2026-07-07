//! Header/metadata mapping: grpc-timeout parsing and the request-header denylist.
//! (Moved out of `src/metadata.rs`'s inline `#[cfg(test)]`.)

use grpc_webnext::metadata::{parse_grpc_timeout, request_headers_to_metadata};
use http::{HeaderMap, HeaderValue};
use std::time::Duration;

#[test]
fn parses_timeout_units() {
    let mut h = HeaderMap::new();
    h.insert("grpc-timeout", HeaderValue::from_static("100m"));
    assert_eq!(parse_grpc_timeout(&h), Some(Duration::from_millis(100)));
    h.insert("grpc-timeout", HeaderValue::from_static("5S"));
    assert_eq!(parse_grpc_timeout(&h), Some(Duration::from_secs(5)));
}

#[test]
fn drops_denied_headers() {
    let mut h = HeaderMap::new();
    h.insert("content-type", HeaderValue::from_static("x"));
    h.insert("x-custom", HeaderValue::from_static("v"));
    let back = request_headers_to_metadata(&h).into_headers();
    assert!(back.get("content-type").is_none());
    assert!(back.get("x-custom").is_some());
}
