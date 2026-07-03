//! Native gRPC clients hit the proxy port and are forwarded to the upstream.

use grpc_webnext_proxy::{bind_and_serve, ProxyConfig};
use testecho::pb::echo_client::EchoClient;
use testecho::pb::EchoRequest;

#[tokio::test]
async fn native_grpc_passthrough() {
    let upstream_addr = testecho::spawn().await;
    let (proxy_addr, _handle) = bind_and_serve(ProxyConfig {
        upstream: format!("http://{upstream_addr}").parse().unwrap(),
        max_message_bytes: 4 * 1024 * 1024,
        ..Default::default()
    })
    .await
    .unwrap();

    // A real tonic client speaks native application/grpc to the proxy port.
    let mut client = EchoClient::connect(format!("http://{proxy_addr}")).await.unwrap();
    let resp = client
        .unary(EchoRequest { message: "through-proxy".into() })
        .await
        .unwrap();
    assert_eq!(resp.into_inner().message, "through-proxy");
}
