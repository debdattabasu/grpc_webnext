//! Client cancellation over WebSocket must propagate to the upstream gRPC call.

use std::time::Duration;

use futures::{SinkExt, StreamExt};
use grpc_webnext::pb::{frame::Kind, Frame, HalfClose, Reset, Subscribe};
use grpc_webnext::{decode_frame, encode_frame};
use grpc_webnext::{bind_and_serve_proxy, ProxyConfig};
use prost::Message;
use testecho::pb::EchoRequest;
use tokio::time::timeout;
use tokio_tungstenite::tungstenite::Message as TungMessage;
use tokio_tungstenite::WebSocketStream;

fn frame(kind: Kind) -> TungMessage {
    TungMessage::Binary(encode_frame(&Frame { kind: Some(kind) }))
}

async fn proxy_for_hang() -> (String, tokio::sync::mpsc::UnboundedReceiver<()>) {
    let (upstream_addr, cancel_rx) = testecho::spawn_with_cancel().await;
    let (proxy_addr, _handle) = bind_and_serve_proxy(ProxyConfig {
        upstream: format!("http://{upstream_addr}").parse().unwrap(),
        max_message_bytes: 4 * 1024 * 1024,
        ..Default::default()
    })
    .await
    .unwrap();
    // Single-stream: the method is the WS URL path.
    (format!("ws://{proxy_addr}/echo.v1.Echo/Hang"), cancel_rx)
}

fn subscribe(stream_id: u32) -> TungMessage {
    frame(Kind::Subscribe(Subscribe {
        stream_id,
        method: "/echo.v1.Echo/Hang".into(),
        headers: vec![],
        timeout_millis: 0,
        initial_payload: EchoRequest { message: "go".into() }.encode_to_vec().into(),
        json: false,
    }))
}

#[tokio::test]
async fn caps_concurrent_streams() {
    let upstream_addr = testecho::spawn().await;
    let (proxy_addr, _handle) = bind_and_serve_proxy(ProxyConfig {
        upstream: format!("http://{upstream_addr}").parse().unwrap(),
        max_concurrent_streams: 1,
        ..Default::default()
    })
    .await
    .unwrap();

    // Two streams on one socket -> multiplexed (method comes from each Subscribe).
    use tokio_tungstenite::tungstenite::client::IntoClientRequest;
    let mut req = format!("ws://{proxy_addr}/").into_client_request().unwrap();
    req.headers_mut()
        .insert("sec-websocket-protocol", "grpc-webnext+proto+multi".parse().unwrap());
    let (mut ws, _) = tokio_tungstenite::connect_async(req).await.unwrap();

    // First stream is accepted (starts, emits a Message); second is rejected.
    ws.send(subscribe(1)).await.unwrap();
    ws.send(subscribe(2)).await.unwrap();

    let mut got_reset_for_2 = false;
    let _ = timeout(Duration::from_secs(5), async {
        while let Some(msg) = ws.next().await {
            if let Ok(TungMessage::Binary(data)) = msg {
                if let Some(Kind::Reset(r)) = decode_frame(&data).unwrap().kind {
                    if r.stream_id == 2 && r.status_code == 8 {
                        // RESOURCE_EXHAUSTED
                        got_reset_for_2 = true;
                        return;
                    }
                }
            }
        }
    })
    .await;
    assert!(got_reset_for_2, "second stream was not rejected with RESOURCE_EXHAUSTED");
}

/// Open the hanging server-stream and wait until it has started upstream (the
/// first `Message` frame). Times out rather than hanging on failure.
async fn open_and_await_start<S>(ws: &mut WebSocketStream<S>)
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    ws.send(frame(Kind::Subscribe(Subscribe {
        stream_id: 1,
        method: "/echo.v1.Echo/Hang".into(),
        headers: vec![],
        timeout_millis: 0,
        initial_payload: EchoRequest { message: "go".into() }.encode_to_vec().into(),
        json: false,
    })))
    .await
    .unwrap();
    ws.send(frame(Kind::HalfClose(HalfClose { stream_id: 1 })))
        .await
        .unwrap();

    timeout(Duration::from_secs(10), async {
        loop {
            let msg = ws.next().await.expect("frame").unwrap();
            if let TungMessage::Binary(data) = msg {
                if matches!(decode_frame(&data).unwrap().kind, Some(Kind::Message(_))) {
                    return;
                }
            }
        }
    })
    .await
    .expect("stream never started upstream");
}

#[tokio::test]
async fn reset_propagates_to_upstream() {
    let (url, mut cancel_rx) = proxy_for_hang().await;
    let mut ws = connect_proto(&url).await;
    open_and_await_start(&mut ws).await;

    assert!(cancel_rx.try_recv().is_err(), "cancelled before reset");

    ws.send(frame(Kind::Reset(Reset {
        stream_id: 1,
        status_code: 1, // CANCELLED
        status_message: "client cancelled".into(),
    })))
    .await
    .unwrap();

    assert!(
        timeout(Duration::from_secs(5), cancel_rx.recv()).await.is_ok(),
        "upstream was not cancelled after Reset",
    );
}

#[tokio::test]
async fn disconnect_propagates_to_upstream() {
    let (url, mut cancel_rx) = proxy_for_hang().await;
    let mut ws = connect_proto(&url).await;
    open_and_await_start(&mut ws).await;

    drop(ws); // client disconnects entirely

    assert!(
        timeout(Duration::from_secs(5), cancel_rx.recv()).await.is_ok(),
        "upstream was not cancelled after disconnect",
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
