//! Client cancellation over WebSocket must cancel the in-process gRPC handler.

use std::time::Duration;

use futures::{SinkExt, StreamExt};
use grpc_webnext_core::pb::{frame::Kind, Frame, HalfClose, Reset, Subscribe};
use grpc_webnext_core::{decode_frame, encode_frame};
use grpc_webnext_server::{bind_and_serve, ServerConfig};
use prost::Message;
use testecho::pb::echo_server::EchoServer;
use testecho::pb::EchoRequest;
use testecho::EchoSvc;
use tokio::time::timeout;
use tokio_tungstenite::tungstenite::Message as TungMessage;
use tonic::service::Routes;

fn frame(kind: Kind) -> TungMessage {
    TungMessage::Binary(encode_frame(&Frame { kind: Some(kind) }))
}

#[tokio::test]
async fn reset_cancels_in_process_handler() {
    let (svc, mut cancel_rx) = EchoSvc::with_cancel();
    let routes = Routes::new(EchoServer::new(svc));
    let (addr, _handle) = bind_and_serve(routes, ServerConfig::default()).await.unwrap();

    let (mut ws, _) = tokio_tungstenite::connect_async(format!("ws://{addr}/"))
        .await
        .unwrap();

    ws.send(frame(Kind::Subscribe(Subscribe {
        stream_id: 1,
        method: "/echo.v1.Echo/Hang".into(),
        headers: vec![],
        timeout_millis: 0,
        initial_payload: EchoRequest { message: "go".into() }.encode_to_vec(),
        json: false,
    })))
    .await
    .unwrap();
    ws.send(frame(Kind::HalfClose(HalfClose { stream_id: 1 })))
        .await
        .unwrap();

    // Wait until the handler has started (first Message frame).
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
    .expect("handler never started");

    assert!(cancel_rx.try_recv().is_err(), "cancelled before reset");

    ws.send(frame(Kind::Reset(Reset {
        stream_id: 1,
        status_code: 1,
        status_message: "cancelled".into(),
    })))
    .await
    .unwrap();

    assert!(
        timeout(Duration::from_secs(5), cancel_rx.recv()).await.is_ok(),
        "in-process handler was not cancelled after Reset",
    );
}
