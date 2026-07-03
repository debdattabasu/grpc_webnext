//! Proxy unary retry: backoff, retryable-code gating, max-attempts, deadline bound.

use std::time::Duration;

use bytes::Bytes;
use grpc_webnext_core::decode_response_body;
use grpc_webnext_proxy::{bind_and_serve, ProxyConfig, RetryPolicy, CT_PROTO};
use prost::Message;
use testecho::pb::{EchoRequest, EchoResponse};
use tonic::Code;

fn retry_policy(max_attempts: u32) -> RetryPolicy {
    RetryPolicy {
        max_attempts,
        initial_backoff: Duration::from_millis(10),
        max_backoff: Duration::from_millis(50),
        backoff_multiplier: 2.0,
        retryable_codes: vec![Code::Unavailable],
    }
}

async fn proxy(upstream: std::net::SocketAddr, retry: RetryPolicy) -> String {
    let (addr, _handle) = bind_and_serve(ProxyConfig {
        upstream: format!("http://{upstream}").parse().unwrap(),
        retry,
        ..Default::default()
    })
    .await
    .unwrap();
    format!("http://{addr}")
}

async fn call_flaky(base: &str) -> grpc_webnext_core::pb::Trailer {
    let body = EchoRequest { message: "hi".into() }.encode_to_vec();
    let resp = reqwest::Client::new()
        .post(format!("{base}/echo.v1.Echo/FlakyUnary"))
        .header("content-type", CT_PROTO)
        .body(body)
        .send()
        .await
        .unwrap();
    let raw = Bytes::from(resp.bytes().await.unwrap().to_vec());
    decode_response_body(raw, 4 * 1024 * 1024).unwrap().1
}

#[tokio::test]
async fn retries_until_success() {
    // Upstream fails twice, then succeeds; 3 attempts allowed.
    let upstream = testecho::spawn_flaky(2).await;
    let base = proxy(upstream, retry_policy(3)).await;

    let body = EchoRequest { message: "hi".into() }.encode_to_vec();
    let resp = reqwest::Client::new()
        .post(format!("{base}/echo.v1.Echo/FlakyUnary"))
        .header("content-type", CT_PROTO)
        .body(body)
        .send()
        .await
        .unwrap();
    let raw = Bytes::from(resp.bytes().await.unwrap().to_vec());
    let (message, trailer) = decode_response_body(raw, 4 * 1024 * 1024).unwrap();
    assert_eq!(trailer.status_code, 0, "status: {}", trailer.status_message);
    assert_eq!(EchoResponse::decode(message).unwrap().message, "hi");
}

#[tokio::test]
async fn exhausts_max_attempts() {
    // Upstream fails 5 times but only 3 attempts allowed -> still UNAVAILABLE.
    let upstream = testecho::spawn_flaky(5).await;
    let base = proxy(upstream, retry_policy(3)).await;
    let trailer = call_flaky(&base).await;
    assert_eq!(trailer.status_code, Code::Unavailable as u32);
}

#[tokio::test]
async fn no_retry_when_disabled() {
    // Default policy (max_attempts = 1) must not retry a single failure.
    let upstream = testecho::spawn_flaky(1).await;
    let base = proxy(upstream, RetryPolicy::default()).await;
    let trailer = call_flaky(&base).await;
    assert_eq!(trailer.status_code, Code::Unavailable as u32);
}

#[tokio::test]
async fn deadline_bounds_retry() {
    // Upstream always fails; retries would run forever, but a 150ms deadline
    // must cut them off with DEADLINE_EXCEEDED.
    let upstream = testecho::spawn_flaky(u32::MAX).await;
    let base = proxy(upstream, retry_policy(1_000_000)).await;

    let body = EchoRequest { message: "hi".into() }.encode_to_vec();
    let resp = reqwest::Client::new()
        .post(format!("{base}/echo.v1.Echo/FlakyUnary"))
        .header("content-type", CT_PROTO)
        .header("grpc-timeout", "150m")
        .body(body)
        .send()
        .await
        .unwrap();
    let raw = Bytes::from(resp.bytes().await.unwrap().to_vec());
    let (_msg, trailer) = decode_response_body(raw, 4 * 1024 * 1024).unwrap();
    assert_eq!(trailer.status_code, Code::DeadlineExceeded as u32);
}
