//! Proxy: WebSocket keepalive pings. With `ProxyConfig::ws_keepalive` set, an open
//! streaming connection emits native WebSocket ping frames (RFC 6455 §5.5.2) each
//! period so idle-timeout proxies/LBs see traffic on a quiet stream.

use std::time::Duration;

use futures::StreamExt;
use grpc_webnext::{bind_and_serve_proxy, ProxyConfig};
use tokio_tungstenite::tungstenite::Message as TungMessage;

type Ws = tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

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

async fn start(ws_keepalive: Option<Duration>, ws_keepalive_timeout: Duration) -> String {
    let upstream_addr = testecho::spawn().await;
    let (proxy_addr, _handle) = bind_and_serve_proxy(ProxyConfig {
        upstream: format!("http://{upstream_addr}").parse().unwrap(),
        ws_keepalive,
        ws_keepalive_timeout,
        ..Default::default()
    })
    .await
    .unwrap();
    format!("ws://{proxy_addr}/echo.v1.Echo/Stream")
}

/// Poll until the connection closes (a Close frame, a read error, or end of stream)
/// or `budget` elapses. Returns whether the proxy closed the connection.
async fn closed_within(ws: &mut Ws, budget: Duration) -> bool {
    tokio::time::timeout(budget, async {
        while let Some(msg) = ws.next().await {
            if matches!(msg, Ok(TungMessage::Close(_)) | Err(_)) {
                return true;
            }
            // Buffered pings etc. — keep draining. Reading here makes tokio-tungstenite
            // auto-pong, so a live proxy would keep us open.
        }
        true // stream ended => socket closed
    })
    .await
    .unwrap_or(false)
}

#[tokio::test]
async fn proxy_sends_keepalive_pings_on_idle_connection() {
    let url = start(Some(Duration::from_millis(50)), Duration::from_secs(20)).await;
    let mut ws = connect_proto(&url).await;
    assert!(
        saw_ping(&mut ws, Duration::from_secs(2)).await,
        "expected a keepalive ping within a few periods",
    );
}

#[tokio::test]
async fn no_keepalive_pings_by_default() {
    let url = start(None, Duration::from_secs(20)).await;
    let mut ws = connect_proto(&url).await;
    assert!(
        !saw_ping(&mut ws, Duration::from_millis(300)).await,
        "no keepalive ping should arrive when ws_keepalive is disabled",
    );
}

#[tokio::test]
async fn drops_connection_that_stops_ponging() {
    let url = start(Some(Duration::from_millis(50)), Duration::from_millis(100)).await;
    let mut ws = connect_proto(&url).await;

    // Do NOT read for a while: tokio-tungstenite only auto-pongs while polled, so this
    // simulates a peer gone silent. Within keepalive + timeout (~150ms) the proxy must
    // give up on us.
    tokio::time::sleep(Duration::from_millis(400)).await;
    assert!(
        closed_within(&mut ws, Duration::from_secs(2)).await,
        "proxy should drop a connection that stops answering keepalive pings",
    );
}

#[tokio::test]
async fn keeps_connection_that_keeps_ponging() {
    let url = start(Some(Duration::from_millis(50)), Duration::from_millis(100)).await;
    let mut ws = connect_proto(&url).await;

    // Keep reading (auto-pong) across many keepalive periods: a healthy peer stays up.
    assert!(
        !closed_within(&mut ws, Duration::from_millis(500)).await,
        "a peer that keeps answering pings must stay connected",
    );
}

/// Connect a single-stream binary WebSocket (offers the `grpc-webnext+proto` subprotocol,
/// as a real SDK client does).
async fn connect_proto(
    url: &str,
) -> tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>> {
    use tokio_tungstenite::tungstenite::client::IntoClientRequest;
    let mut req = url.into_client_request().unwrap();
    req.headers_mut()
        .insert("sec-websocket-protocol", "grpc-webnext+proto".parse().unwrap());
    tokio_tungstenite::connect_async(req).await.unwrap().0
}
