//! HTTP header <-> gRPC metadata conversion, and grpc-timeout parsing.

use std::time::Duration;

use grpc_webnext_core::pb::{metadatum, Metadatum};
use http::{HeaderMap, HeaderName, HeaderValue};
use tonic::metadata::MetadataMap;

/// Headers that must not be forwarded as gRPC metadata (hop-by-hop, framing,
/// or set by the gRPC client itself).
const DENY: &[&str] = &[
    "host",
    "connection",
    "content-length",
    "content-type",
    "keep-alive",
    "proxy-connection",
    "transfer-encoding",
    "te",
    "upgrade",
    "grpc-timeout", // handled separately as a deadline
];

fn is_denied(name: &HeaderName) -> bool {
    DENY.contains(&name.as_str())
}

/// Copy client request headers into a gRPC metadata map, minus the denylist.
pub fn request_headers_to_metadata(headers: &HeaderMap) -> MetadataMap {
    let mut filtered = HeaderMap::new();
    for (name, value) in headers.iter() {
        if !is_denied(name) {
            filtered.append(name.clone(), value.clone());
        }
    }
    MetadataMap::from_headers(filtered)
}

/// Merge gRPC response metadata into HTTP response headers, skipping any that
/// would collide with framing/content-type.
pub fn merge_metadata_into_headers(meta: &MetadataMap, headers: &mut HeaderMap) {
    for (name, value) in meta.clone().into_headers().iter() {
        if is_denied(name) {
            continue;
        }
        headers.append(name.clone(), value.clone());
    }
}

/// Convert a protobuf metadata list (from a `Subscribe`/`Header`/`Trailer`
/// frame) into a gRPC metadata map.
pub fn metadata_vec_to_metadata(items: &[Metadatum]) -> MetadataMap {
    let mut headers = HeaderMap::new();
    for m in items {
        let name = match HeaderName::from_bytes(m.key.as_bytes()) {
            Ok(n) if !is_denied(&n) => n,
            _ => continue,
        };
        match &m.value {
            Some(metadatum::Value::AsciiValue(s)) => {
                if let Ok(v) = HeaderValue::from_str(s) {
                    headers.append(name, v);
                }
            }
            Some(metadatum::Value::BinValue(b)) => {
                if let Ok(v) = HeaderValue::from_bytes(b) {
                    headers.append(name, v);
                }
            }
            None => {}
        }
    }
    MetadataMap::from_headers(headers)
}

/// Convert a gRPC metadata map into a protobuf metadata list for a frame.
pub fn metadata_to_vec(meta: &MetadataMap) -> Vec<Metadatum> {
    let mut out = Vec::new();
    for (name, value) in meta.clone().into_headers().iter() {
        if is_denied(name) {
            continue;
        }
        let key = name.as_str().to_string();
        if key.ends_with("-bin") {
            out.push(Metadatum {
                key,
                value: Some(metadatum::Value::BinValue(value.as_bytes().to_vec())),
            });
        } else if let Ok(s) = value.to_str() {
            out.push(Metadatum {
                key,
                value: Some(metadatum::Value::AsciiValue(s.to_string())),
            });
        }
    }
    out
}

/// Parse a gRPC `grpc-timeout` header (e.g. `100m`, `5S`) into a Duration.
///
/// Format: an ASCII integer followed by a unit char:
/// H(ours) M(inutes) S(econds) m(illis) u(micros) n(nanos).
pub fn parse_grpc_timeout(headers: &HeaderMap) -> Option<Duration> {
    let raw = headers.get("grpc-timeout")?.to_str().ok()?;
    let (value, unit) = raw.split_at(raw.len().checked_sub(1)?);
    let n: u64 = value.parse().ok()?;
    let d = match unit {
        "H" => Duration::from_secs(n.checked_mul(3600)?),
        "M" => Duration::from_secs(n.checked_mul(60)?),
        "S" => Duration::from_secs(n),
        "m" => Duration::from_millis(n),
        "u" => Duration::from_micros(n),
        "n" => Duration::from_nanos(n),
        _ => return None,
    };
    Some(d)
}

/// Convert a `Subscribe.timeout_millis` field (0 = no deadline) to a Duration.
pub fn grpc_timeout_from_metadatum(timeout_millis: &u32) -> Option<Duration> {
    match *timeout_millis {
        0 => None,
        m => Some(Duration::from_millis(u64::from(m))),
    }
}

/// Build a `grpc-timeout` header value from a Duration (millisecond unit).
pub fn format_grpc_timeout(d: Duration) -> HeaderValue {
    let millis = d.as_millis().min(u128::from(u64::MAX));
    HeaderValue::from_str(&format!("{millis}m")).expect("valid header value")
}

#[cfg(test)]
mod tests {
    use super::*;

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
        let m = request_headers_to_metadata(&h);
        let back = m.into_headers();
        assert!(back.get("content-type").is_none());
        assert!(back.get("x-custom").is_some());
    }
}
