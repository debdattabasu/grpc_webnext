//! Metadata + grpc-timeout helpers now live in `grpc-webnext-core` so the proxy
//! and the native server library share one implementation.

pub use grpc_webnext_core::metadata::*;

/// Back-compat shim: the proxy WS path passes `&u32`.
pub fn grpc_timeout_from_metadatum(timeout_millis: &u32) -> Option<std::time::Duration> {
    grpc_webnext_core::metadata::grpc_timeout_from_millis(*timeout_millis)
}
