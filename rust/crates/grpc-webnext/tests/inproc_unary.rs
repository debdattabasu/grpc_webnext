//! Native server: same-port native gRPC pass-through + grpc-webnext unary.

use bytes::Bytes;
use grpc_webnext::{decode_response_body, encode_request_body};
use grpc_webnext::{bind_and_serve_in_process, ServerConfig, CT_PROTO};
use prost::Message;
use testecho::pb::echo_client::EchoClient;
use testecho::pb::echo_server::EchoServer;
use testecho::pb::{EchoRequest, EchoResponse};
use testecho::EchoSvc;
use tonic::service::Routes;

async fn start() -> String {
    start_with(EchoSvc::default()).await
}

async fn start_with(svc: EchoSvc) -> String {
    let routes = Routes::new(EchoServer::new(svc));
    let (addr, _handle) = bind_and_serve_in_process(routes, ServerConfig::default()).await.unwrap();
    format!("http://{addr}")
}

/// POST a `+proto` unary request and decode the streamed `(message, trailer)` body.
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
async fn native_grpc_passthrough() {
    let base = start().await;
    // A real tonic client speaks native application/grpc to the same port.
    let mut client = EchoClient::connect(base).await.unwrap();
    let resp = client
        .unary(EchoRequest { message: "native".into() })
        .await
        .unwrap();
    assert_eq!(resp.into_inner().message, "native");
}

#[tokio::test]
async fn grpc_webnext_unary() {
    let base = start().await;
    let body = encode_request_body(&EchoRequest { message: "webnext".into() }.encode_to_vec()).to_vec();

    let client = reqwest::Client::new();
    let resp = client
        .post(format!("{base}/echo.v1.Echo/Unary"))
        .header("content-type", CT_PROTO)
        .body(body)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    let raw = Bytes::from(resp.bytes().await.unwrap().to_vec());
    let (message, trailer) = decode_response_body(raw, 4 * 1024 * 1024).unwrap();
    assert_eq!(trailer.status_code, 0, "status: {}", trailer.status_message);
    assert_eq!(EchoResponse::decode(message).unwrap().message, "webnext");
}

#[tokio::test]
async fn large_response_streams_intact() {
    // A ~3 MiB echo forces a multi-chunk streamed response body; assert it round-trips
    // byte-for-byte (the server pipes it through without buffering the whole message).
    let base = start().await;
    let big = "x".repeat(3 * 1024 * 1024);
    let (message, code) =
        post_proto(&base, "/echo.v1.Echo/Unary", EchoRequest { message: big.clone() }.encode_to_vec()).await;
    assert_eq!(code, 0);
    assert_eq!(EchoResponse::decode(message).unwrap().message, big);
}

#[tokio::test]
async fn empty_ok_message_streams() {
    // An empty (but OK) message: the gRPC frame is header-only, so after dropping the
    // flag byte the message block is `[0,0,0,0]`. Client must decode an empty message.
    let base = start().await;
    let (message, code) =
        post_proto(&base, "/echo.v1.Echo/Unary", EchoRequest { message: String::new() }.encode_to_vec()).await;
    assert_eq!(code, 0);
    assert_eq!(EchoResponse::decode(message).unwrap().message, "");
}

#[tokio::test]
async fn error_response_is_trailers_only() {
    // FlakyUnary(1) errors on the first call with no message frame (trailers-only). The
    // streamed body must still carry a leading empty message block + the error trailer.
    let base = start_with(EchoSvc::flaky(1)).await;
    let (message, code) =
        post_proto(&base, "/echo.v1.Echo/FlakyUnary", EchoRequest { message: "hi".into() }.encode_to_vec()).await;
    assert_eq!(code, tonic::Code::Unavailable as u32);
    assert!(message.is_empty(), "error response should carry no message");
}
