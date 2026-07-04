//! Native server: WebSocket keepalive pings. With `ServerConfig::ws_keepalive` set,
//! an open streaming connection emits native WebSocket ping frames (RFC 6455 §5.5.2)
//! each period so idle-timeout proxies/LBs see traffic on a quiet stream. Browsers
//! auto-answer these with pongs; here tokio-tungstenite surfaces the ping to us.

use std::time::Duration;

use futures::StreamExt;
use grpc_webnext_server::{bind_and_serve, ServerConfig};
use testecho::pb::echo_server::EchoServer;
use testecho::EchoSvc;
use tokio_tungstenite::tungstenite::Message as TungMessage;
use tonic::service::Routes;

/// Read WS messages until a Ping arrives or `budget` elapses. Returns whether a ping
/// was seen (an `Err` timeout / a closed stream both mean "no ping").
async fn saw_ping(ws: &mut Ws, budget: Duration) -> bool {
    tokio::time::timeout(budget, async {
        while let Some(msg) = ws.next().await {
            if matches!(msg, Ok(TungMessage::Ping(_))) {
                return true;
            }
        }
        false
    })
    .await
    .unwrap_or(false)
}

type Ws = tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

async fn start(config: ServerConfig) -> String {
    let routes = Routes::new(EchoServer::new(EchoSvc::default()));
    // Dropping the JoinHandle detaches the server task rather than aborting it, so
    // the server keeps running for the test body.
    let (addr, _handle) = bind_and_serve(routes, config).await.unwrap();
    format!("ws://{addr}/echo.v1.Echo/Stream")
}

/// Poll until the connection closes (a Close frame, a read error, or end of stream)
/// or `budget` elapses. Returns whether the server closed the connection.
async fn closed_within(ws: &mut Ws, budget: Duration) -> bool {
    tokio::time::timeout(budget, async {
        while let Some(msg) = ws.next().await {
            if matches!(msg, Ok(TungMessage::Close(_)) | Err(_)) {
                return true;
            }
            // Buffered pings etc. — keep draining. Note: reading here makes
            // tokio-tungstenite auto-pong, so a live server would keep us open.
        }
        true // stream ended => socket closed
    })
    .await
    .unwrap_or(false)
}

#[tokio::test]
async fn server_sends_keepalive_pings_on_idle_connection() {
    let url = start(ServerConfig {
        allow_implicit_codec: true,
        ws_keepalive: Some(Duration::from_millis(50)),
        ..Default::default()
    })
    .await;

    // Bare connection, no Subscribe: keepalive is a connection-level concern, so a
    // quiet socket must still get pings.
    let (mut ws, _) = tokio_tungstenite::connect_async(url).await.unwrap();
    assert!(
        saw_ping(&mut ws, Duration::from_secs(2)).await,
        "expected a keepalive ping within a few periods",
    );
}

#[tokio::test]
async fn no_keepalive_pings_by_default() {
    // ws_keepalive defaults to None -> an idle connection gets no pings.
    let url = start(ServerConfig { allow_implicit_codec: true, ..Default::default() }).await;
    let (mut ws, _) = tokio_tungstenite::connect_async(url).await.unwrap();
    assert!(
        !saw_ping(&mut ws, Duration::from_millis(300)).await,
        "no keepalive ping should arrive when ws_keepalive is disabled",
    );
}

#[tokio::test]
async fn drops_connection_that_stops_ponging() {
    let url = start(ServerConfig {
        allow_implicit_codec: true,
        ws_keepalive: Some(Duration::from_millis(50)),
        ws_keepalive_timeout: Duration::from_millis(100),
        ..Default::default()
    })
    .await;
    let (mut ws, _) = tokio_tungstenite::connect_async(url).await.unwrap();

    // Deliberately do NOT read for a while: tokio-tungstenite only auto-pongs while
    // being polled, so this simulates a peer that has gone silent. Within
    // keepalive + timeout (~150ms) the server must give up on us.
    tokio::time::sleep(Duration::from_millis(400)).await;
    assert!(
        closed_within(&mut ws, Duration::from_secs(2)).await,
        "server should drop a connection that stops answering keepalive pings",
    );
}

#[tokio::test]
async fn keeps_connection_that_keeps_ponging() {
    let url = start(ServerConfig {
        allow_implicit_codec: true,
        ws_keepalive: Some(Duration::from_millis(50)),
        ws_keepalive_timeout: Duration::from_millis(100),
        ..Default::default()
    })
    .await;
    let (mut ws, _) = tokio_tungstenite::connect_async(url).await.unwrap();

    // Keep reading (so tokio-tungstenite auto-pongs) across many keepalive periods.
    // A healthy, ponging peer must never be dropped.
    assert!(
        !closed_within(&mut ws, Duration::from_millis(500)).await,
        "a peer that keeps answering pings must stay connected",
    );
}
