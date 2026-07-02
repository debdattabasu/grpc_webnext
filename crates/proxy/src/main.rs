//! grpc-webnext proxy binary (binary-only for v1).

use std::net::SocketAddr;

use grpc_webnext_proxy::{serve, ProxyConfig};
use tokio::net::TcpListener;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();

    let listen: SocketAddr = std::env::var("LISTEN")
        .unwrap_or_else(|_| "127.0.0.1:8080".into())
        .parse()?;
    let upstream: http::Uri = std::env::var("UPSTREAM")
        .unwrap_or_else(|_| "http://127.0.0.1:50051".into())
        .parse()?;

    let config = ProxyConfig {
        upstream,
        max_message_bytes: 4 * 1024 * 1024,
    };

    tracing::info!(%listen, upstream = %config.upstream, "grpc-webnext-proxy listening");
    let listener = TcpListener::bind(listen).await?;
    serve(listener, config).await?;
    Ok(())
}
