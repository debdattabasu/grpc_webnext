//! Native server: same-port native gRPC pass-through + grpc-webnext unary.

use bytes::Bytes;
use grpc_webnext_core::decode_response_body;
use grpc_webnext_server::{bind_and_serve, ServerConfig, CT_PROTO};
use prost::Message;
use testecho::pb::echo_client::EchoClient;
use testecho::pb::echo_server::EchoServer;
use testecho::pb::{EchoRequest, EchoResponse};
use testecho::EchoSvc;
use tonic::service::Routes;

async fn start() -> String {
    let routes = Routes::new(EchoServer::new(EchoSvc::default()));
    let (addr, _handle) = bind_and_serve(routes, ServerConfig::default()).await.unwrap();
    format!("http://{addr}")
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
    let body = EchoRequest { message: "webnext".into() }.encode_to_vec();

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
