//! Native server +json: descriptor-driven JSON<->protobuf transcoding.

use std::sync::Arc;

use futures::{SinkExt, StreamExt};
use grpc_webnext_core::pb::{frame::Kind, Frame, HalfClose, Subscribe};
use grpc_webnext_core::{decode_frame, encode_frame, Transcoder};
use grpc_webnext_server::{bind_and_serve, ServerConfig, CT_JSON};
use prost::Message as _;
use testecho::pb::echo_server::EchoServer;
use testecho::pb::{EchoRequest, EchoResponse};
use testecho::EchoSvc;
use tokio_tungstenite::tungstenite::Message as TungMessage;
use tonic::service::Routes;

const CT_JSON_STR: &str = CT_JSON;

fn bin(kind: Kind) -> TungMessage {
    TungMessage::Binary(encode_frame(&Frame { kind: Some(kind) }))
}

async fn start_json_server() -> String {
    let transcoder = Arc::new(Transcoder::from_file_descriptor_set(testecho::FILE_DESCRIPTOR_SET).unwrap());
    let routes = Routes::new(EchoServer::new(EchoSvc::default()));
    let (addr, _handle) = bind_and_serve(
        routes,
        ServerConfig { transcoder: Some(transcoder), ..Default::default() },
    )
    .await
    .unwrap();
    format!("http://{addr}")
}

#[tokio::test]
async fn unary_json_round_trip() {
    let base = start_json_server().await;

    // Native JSON: bare JSON body, gRPC status in HTTP headers.
    let resp = reqwest::Client::new()
        .post(format!("{base}/echo.v1.Echo/Unary"))
        .header("content-type", CT_JSON_STR)
        .body(r#"{"message":"hello json"}"#)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    assert_eq!(resp.headers().get("content-type").unwrap(), CT_JSON_STR);
    assert_eq!(resp.headers().get("grpc-status").unwrap(), "0");

    let body = resp.bytes().await.unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["message"], "hello json");
}

#[tokio::test]
async fn unary_json_bad_input_is_invalid_argument() {
    let base = start_json_server().await;
    let resp = reqwest::Client::new()
        .post(format!("{base}/echo.v1.Echo/Unary"))
        .header("content-type", CT_JSON_STR)
        .body(r#"{"message": 12345}"#) // wrong type: message is a string
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.headers().get("grpc-status").unwrap().to_str().unwrap(),
        (tonic::Code::InvalidArgument as u32).to_string(),
    );
}

fn text(json: serde_json::Value) -> tokio_tungstenite::tungstenite::Message {
    tokio_tungstenite::tungstenite::Message::Text(json.to_string().into())
}

#[tokio::test]
async fn streaming_json_round_trip() {
    // Native JSON WebSocket: text frames with native-JSON messages.
    let base = start_json_server().await;
    let url = base.replacen("http", "ws", 1);
    let (mut ws, _) = tokio_tungstenite::connect_async(url).await.unwrap();

    // Flat frames: open (has method), data (has message), half-close, terminal (status).
    ws.send(text(serde_json::json!({
        "streamId": 1, "method": "/echo.v1.Echo/Stream", "message": {"message": "a"}
    })))
    .await
    .unwrap();
    ws.send(text(serde_json::json!({ "streamId": 1, "message": {"message": "b"} })))
        .await
        .unwrap();
    ws.send(text(serde_json::json!({ "streamId": 1, "halfClose": true })))
        .await
        .unwrap();

    let mut echoed = Vec::new();
    while let Some(msg) = ws.next().await {
        let tokio_tungstenite::tungstenite::Message::Text(t) = msg.unwrap() else { continue };
        let jf: serde_json::Value = serde_json::from_str(&t).unwrap();
        if jf.get("status").is_some() {
            assert_eq!(jf["status"]["code"], 0);
            break;
        } else if let Some(m) = jf.get("message") {
            echoed.push(m["message"].as_str().unwrap().to_string());
        }
    }
    assert_eq!(echoed, vec!["a", "b"]);
}

#[tokio::test]
async fn unary_application_json_alias() {
    // `application/json` is accepted as an alias for the JSON codec, and the
    // response echoes the request's content-type.
    let base = start_json_server().await;
    let resp = reqwest::Client::new()
        .post(format!("{base}/echo.v1.Echo/Unary"))
        .header("content-type", "application/json")
        .body(r#"{"message":"plain"}"#)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    assert_eq!(resp.headers().get("content-type").unwrap(), "application/json");
    assert_eq!(resp.headers().get("grpc-status").unwrap(), "0");

    let body = resp.bytes().await.unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["message"], "plain");
}

#[tokio::test]
async fn ws_locks_to_text_on_first_text_frame() {
    let base = start_json_server().await;
    let url = base.replacen("http", "ws", 1);
    let (mut ws, _) = tokio_tungstenite::connect_async(url).await.unwrap();

    // First frame is text -> connection locked to JSON.
    ws.send(text(serde_json::json!({
        "streamId": 1, "method": "/echo.v1.Echo/Stream", "message": {"message": "a"}
    })))
    .await
    .unwrap();
    // A later binary frame must be dropped (locked to text).
    ws.send(bin(Kind::Subscribe(Subscribe {
        stream_id: 2,
        method: "/echo.v1.Echo/Stream".into(),
        headers: vec![],
        timeout_millis: 0,
        initial_payload: EchoRequest { message: "b".into() }.encode_to_vec(),
        json: false,
    })))
    .await
    .unwrap();
    ws.send(text(serde_json::json!({ "streamId": 1, "halfClose": true })))
        .await
        .unwrap();

    let mut echoed = Vec::new();
    while let Some(msg) = ws.next().await {
        match msg.unwrap() {
            TungMessage::Binary(_) => panic!("binary frame on a text-locked connection"),
            TungMessage::Text(t) => {
                let jf: serde_json::Value = serde_json::from_str(&t).unwrap();
                assert_eq!(jf["streamId"], 1, "stream 2 (binary) should have been dropped");
                if jf.get("status").is_some() {
                    break;
                } else if let Some(m) = jf.get("message") {
                    echoed.push(m["message"].as_str().unwrap().to_string());
                }
            }
            _ => {}
        }
    }
    assert_eq!(echoed, vec!["a"]);
}

#[tokio::test]
async fn ws_locks_to_binary_on_first_binary_frame() {
    let base = start_json_server().await;
    let url = base.replacen("http", "ws", 1);
    let (mut ws, _) = tokio_tungstenite::connect_async(url).await.unwrap();

    // First frame is binary -> connection locked to protobuf.
    ws.send(bin(Kind::Subscribe(Subscribe {
        stream_id: 1,
        method: "/echo.v1.Echo/Stream".into(),
        headers: vec![],
        timeout_millis: 0,
        initial_payload: EchoRequest { message: "a".into() }.encode_to_vec(),
        json: false,
    })))
    .await
    .unwrap();
    // A later text frame must be dropped (locked to binary).
    ws.send(text(serde_json::json!({
        "streamId": 2, "method": "/echo.v1.Echo/Stream", "message": {"message": "b"}
    })))
    .await
    .unwrap();
    ws.send(bin(Kind::HalfClose(HalfClose { stream_id: 1 })))
        .await
        .unwrap();

    let mut echoed = Vec::new();
    while let Some(msg) = ws.next().await {
        match msg.unwrap() {
            TungMessage::Text(_) => panic!("text frame on a binary-locked connection"),
            TungMessage::Binary(data) => match decode_frame(&data).unwrap().kind.unwrap() {
                Kind::Header(h) => assert_eq!(h.stream_id, 1),
                Kind::Message(m) => {
                    assert_eq!(m.stream_id, 1, "stream 2 (text) should have been dropped");
                    echoed.push(EchoResponse::decode(&m.payload[..]).unwrap().message);
                }
                Kind::Trailer(t) => {
                    assert_eq!(t.stream_id, 1);
                    break;
                }
                other => panic!("unexpected: {other:?}"),
            },
            _ => {}
        }
    }
    assert_eq!(echoed, vec!["a"]);
}
