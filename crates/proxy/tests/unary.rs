//! End-to-end: grpc-webnext Fetch unary request -> proxy -> tonic echo server.

use bytes::Bytes;
use grpc_webnext_core::{decode_response_body, encode_request_body};
use grpc_webnext_proxy::{bind_and_serve, ProxyConfig, CT_PROTO};
use prost::Message;
use testecho::pb::{EchoRequest, EchoResponse};

async fn setup() -> String {
    setup_over(testecho::spawn().await).await
}

async fn setup_over(upstream_addr: std::net::SocketAddr) -> String {
    let (proxy_addr, _handle) = bind_and_serve(ProxyConfig {
        upstream: format!("http://{upstream_addr}").parse().unwrap(),
        max_message_bytes: 4 * 1024 * 1024,
        ..Default::default()
    })
    .await
    .unwrap();
    format!("http://{proxy_addr}")
}

/// POST a `+proto` unary request and decode the streamed `(message, status)`.
async fn post_proto(base: &str, method: &str, body: Vec<u8>) -> (Bytes, u32) {
    let resp = reqwest::Client::new()
        .post(format!("{base}{method}"))
        .header("content-type", CT_PROTO)
        .body(encode_request_body(&body).to_vec())
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let raw = Bytes::from(resp.bytes().await.unwrap().to_vec());
    let (message, trailer) = decode_response_body(raw, 8 * 1024 * 1024).unwrap();
    (message, trailer.status_code)
}

#[tokio::test]
async fn unary_round_trip() {
    let base = setup().await;

    // Client sends the length-prefixed encoded message as the Fetch body.
    let req_msg = EchoRequest { message: "ping".into() };
    let body = encode_request_body(&req_msg.encode_to_vec()).to_vec();

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
async fn large_response_streams_intact() {
    // A ~3 MiB echo forces a multi-chunk streamed response; the proxy pipes it through
    // opaquely without buffering the whole message. Assert it round-trips byte-for-byte.
    let base = setup().await;
    let big = "y".repeat(3 * 1024 * 1024);
    let (message, code) =
        post_proto(&base, "/echo.v1.Echo/Unary", EchoRequest { message: big.clone() }.encode_to_vec()).await;
    assert_eq!(code, 0);
    assert_eq!(EchoResponse::decode(message).unwrap().message, big);
}

#[tokio::test]
async fn upstream_error_is_trailers_only() {
    // The upstream's FlakyUnary errors on the first call (trailers-only, no message).
    // The proxy's streamed body must still carry an empty message block + error trailer.
    let base = setup_over(testecho::spawn_flaky(1).await).await;
    let (message, code) =
        post_proto(&base, "/echo.v1.Echo/FlakyUnary", EchoRequest { message: "hi".into() }.encode_to_vec()).await;
    assert_eq!(code, tonic::Code::Unavailable as u32);
    assert!(message.is_empty(), "error response should carry no message");
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
