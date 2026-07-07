//! grpc-webnext standalone proxy binary — front any upstream gRPC server.
//!
//! Env:
//!   * `LISTEN`, `UPSTREAM`
//!   * `+json` descriptor source, from `SCHEMA=reflection` (fetch from the upstream's
//!     reflection service) and/or `DESCRIPTOR_SET=<path>` (a compiled `FileDescriptorSet`):
//!       - neither             → binary-only
//!       - `SCHEMA=reflection` → reflection
//!       - `DESCRIPTOR_SET`    → bundled
//!       - both                → reflection, with the bundle as fallback
//!   * `REFLECTION_TTL_SECS` — reflection refresh interval (default 4h).
//!   * `ADMIN_RELOAD_PATH` — if set, `POST` to this path forces a reflection reload.

use std::net::SocketAddr;
use std::time::Duration;

use bytes::Bytes;
use grpc_webnext::{serve_proxy, ProxyConfig, SchemaSource};
use tokio::net::TcpListener;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();

    let listen: SocketAddr =
        std::env::var("LISTEN").unwrap_or_else(|_| "127.0.0.1:8080".into()).parse()?;
    let upstream: http::Uri =
        std::env::var("UPSTREAM").unwrap_or_else(|_| "http://127.0.0.1:50051".into()).parse()?;

    // Descriptor source for `+json`, composed from the two env knobs.
    let reflection = std::env::var("SCHEMA").ok().as_deref() == Some("reflection");
    let bundle: Option<Bytes> =
        std::env::var("DESCRIPTOR_SET").ok().map(std::fs::read).transpose()?.map(Bytes::from);
    let schema = match (reflection, bundle) {
        (true, Some(b)) => SchemaSource::ReflectionOrBundled(b),
        (true, None) => SchemaSource::Reflection,
        (false, Some(b)) => SchemaSource::Bundled(b),
        (false, None) => SchemaSource::None,
    };

    let mut config = ProxyConfig { upstream, schema, ..Default::default() };
    if let Ok(secs) = std::env::var("REFLECTION_TTL_SECS") {
        config.reflection_ttl = Duration::from_secs(secs.parse()?);
    }
    config.admin_reload_path = std::env::var("ADMIN_RELOAD_PATH").ok();

    tracing::info!(%listen, upstream = %config.upstream, "grpc-webnext-proxy listening");
    let listener = TcpListener::bind(listen).await?;
    serve_proxy(listener, config).await?;
    Ok(())
}
