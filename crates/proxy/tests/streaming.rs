//! End-to-end: grpc-webnext WebSocket streaming -> proxy -> tonic echo (bidi).

use futures::{SinkExt, StreamExt};
use grpc_webnext_core::pb::{frame::Kind, Frame, HalfClose, Message as WsMessage, Subscribe};
use grpc_webnext_core::{decode_frame, encode_frame};
use grpc_webnext_proxy::{bind_and_serve, ProxyConfig};
use prost::Message;
use testecho::pb::{EchoRequest, EchoResponse};
use tokio_tungstenite::tungstenite::Message as TungMessage;

fn frame(kind: Kind) -> TungMessage {
    TungMessage::Binary(encode_frame(&Frame { kind: Some(kind) }))
}

#[tokio::test]
async fn streaming_round_trip() {
    let upstream_addr = testecho::spawn().await;
    let (proxy_addr, _handle) = bind_and_serve(ProxyConfig {
        upstream: format!("http://{upstream_addr}").parse().unwrap(),
        max_message_bytes: 4 * 1024 * 1024,
    })
    .await
    .unwrap();

    let (mut ws, _resp) = tokio_tungstenite::connect_async(format!("ws://{proxy_addr}/"))
        .await
        .unwrap();

    // Open the bidi stream, sending the first request as initial_payload.
    ws.send(frame(Kind::Subscribe(Subscribe {
        stream_id: 1,
        method: "/echo.v1.Echo/Stream".into(),
        headers: vec![],
        timeout_millis: 0,
        initial_payload: EchoRequest { message: "a".into() }.encode_to_vec(),
    })))
    .await
    .unwrap();

    // Second request message, then half-close.
    ws.send(frame(Kind::Message(WsMessage {
        stream_id: 1,
        payload: EchoRequest { message: "b".into() }.encode_to_vec(),
    })))
    .await
    .unwrap();
    ws.send(frame(Kind::HalfClose(HalfClose { stream_id: 1 })))
        .await
        .unwrap();

    // Collect frames until the Trailer.
    let mut echoed = Vec::new();
    let mut got_header = false;
    let mut status_code = None;

    while let Some(msg) = ws.next().await {
        let TungMessage::Binary(data) = msg.unwrap() else { continue };
        let f = decode_frame(&data).unwrap();
        match f.kind.unwrap() {
            Kind::Header(h) => {
                assert_eq!(h.stream_id, 1);
                got_header = true;
            }
            Kind::Message(m) => {
                assert_eq!(m.stream_id, 1);
                echoed.push(EchoResponse::decode(&m.payload[..]).unwrap().message);
            }
            Kind::Trailer(t) => {
                assert_eq!(t.stream_id, 1);
                status_code = Some(t.status_code);
                break;
            }
            other => panic!("unexpected frame: {other:?}"),
        }
    }

    assert!(got_header, "expected a Header frame before messages");
    assert_eq!(echoed, vec!["a", "b"]);
    assert_eq!(status_code, Some(0));
}
