//! End-to-end: grpc-webnext Fetch unary request -> proxy -> tonic echo server.

use bytes::Bytes;
use grpc_webnext_core::decode_response_body;
use grpc_webnext_proxy::{bind_and_serve, ProxyConfig, CT_PROTO};
use prost::Message;
use testecho::pb::{EchoRequest, EchoResponse};

async fn setup() -> String {
    let upstream_addr = testecho::spawn().await;
    let (proxy_addr, _handle) = bind_and_serve(ProxyConfig {
        upstream: format!("http://{upstream_addr}").parse().unwrap(),
        max_message_bytes: 4 * 1024 * 1024,
    })
    .await
    .unwrap();
    format!("http://{proxy_addr}")
}

#[tokio::test]
async fn unary_round_trip() {
    let base = setup().await;

    // Client sends the raw encoded message as the Fetch body.
    let req_msg = EchoRequest { message: "ping".into() };
    let body = req_msg.encode_to_vec();

    let client = reqwest::Client::new();
    let resp = client
        .post(format!("{base}/echo.v1.Echo/Unary"))
        .header("content-type", CT_PROTO)
        .header("x-custom", "meta-value")
        .body(body)
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 200);
    assert_eq!(
        resp.headers().get("content-type").unwrap(),
        CT_PROTO
    );

    let raw = Bytes::from(resp.bytes().await.unwrap().to_vec());
    let (message, trailer) = decode_response_body(raw, 4 * 1024 * 1024).unwrap();

    // gRPC status OK in the trailer block.
    assert_eq!(trailer.status_code, 0, "status: {}", trailer.status_message);

    // Echoed message decodes and matches.
    let echoed = EchoResponse::decode(message).unwrap();
    assert_eq!(echoed.message, "ping");
}

#[tokio::test]
async fn rejects_json_content_type() {
    let base = setup().await;
    let client = reqwest::Client::new();
    let resp = client
        .post(format!("{base}/echo.v1.Echo/Unary"))
        .header("content-type", "application/grpc-webnext+json")
        .body(Vec::new())
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 501); // NOT_IMPLEMENTED
}
