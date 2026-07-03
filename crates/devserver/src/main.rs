//! Dev harness for the TypeScript client e2e tests.
//!
//! Spawns the testecho gRPC server, fronts it with grpc-webnext-proxy, prints
//! `LISTENING http://ADDR` to stdout, then runs until killed.

use std::io::Write;

use grpc_webnext_proxy::{bind_and_serve, ProxyConfig};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let upstream_addr = testecho::spawn().await;
    let (proxy_addr, _handle) = bind_and_serve(ProxyConfig {
        upstream: format!("http://{upstream_addr}").parse()?,
        max_message_bytes: 4 * 1024 * 1024,
        ..Default::default()
    })
    .await?;

    // Signal readiness with the bound address on stdout.
    let mut stdout = std::io::stdout();
    writeln!(stdout, "LISTENING http://{proxy_addr}")?;
    stdout.flush()?;

    std::future::pending::<()>().await;
    Ok(())
}
