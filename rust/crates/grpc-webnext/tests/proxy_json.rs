//! End-to-end: grpc-webnext `+json` -> proxy transcodes -> binary gRPC upstream.
//!
//! Covers both descriptor sources (a bundled `FileDescriptorSet` and upstream
//! reflection) over both surfaces (Fetch unary and WebSocket streaming), plus the
//! no-descriptors case.

use futures::{SinkExt, StreamExt};
use grpc_webnext::json_frame::{decode_json_frame, encode_json_frame, JsonFrame};
use grpc_webnext::{bind_and_serve_proxy, ProxyConfig, SchemaSource, CT_JSON};
use serde_json::json;
use std::net::SocketAddr;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::Message as TungMessage;

/// Start a proxy in front of `upstream` with the given descriptor source.
async fn proxy_over(upstream: SocketAddr, schema: SchemaSource) -> String {
    let (proxy_addr, _handle) = bind_and_serve_proxy(ProxyConfig {
        upstream: format!("http://{upstream}").parse().unwrap(),
        max_message_bytes: 4 * 1024 * 1024,
        schema,
        ..Default::default()
    })
    .await
    .unwrap();
    format!("http://{proxy_addr}")
}

/// Start a proxy with a small message-size limit, to exercise the size guard.
async fn proxy_over_limit(upstream: SocketAddr, schema: SchemaSource, max_message_bytes: usize) -> String {
    let (proxy_addr, _handle) = bind_and_serve_proxy(ProxyConfig {
        upstream: format!("http://{upstream}").parse().unwrap(),
        max_message_bytes,
        schema,
        ..Default::default()
    })
    .await
    .unwrap();
    format!("http://{proxy_addr}")
}

/// An Echo upstream that also serves gRPC reflection. Uses testecho's raw-preserving
/// reflection service (annotations intact) rather than tonic-reflection, which strips
/// custom options like `google.api.http`.
async fn spawn_reflection_upstream() -> SocketAddr {
    testecho::spawn_with_reflection().await
}

/// POST a `+json` unary request; return `(grpc-status, body)`.
async fn post_json(base: &str, method: &str, body: serde_json::Value) -> (u32, String) {
    let resp = reqwest::Client::new()
        .post(format!("{base}{method}"))
        .header("content-type", CT_JSON)
        .body(body.to_string())
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    assert_eq!(resp.headers().get("content-type").unwrap(), CT_JSON);
    let code = resp
        .headers()
        .get("grpc-status")
        .expect("grpc-status header")
        .to_str()
        .unwrap()
        .parse()
        .unwrap();
    (code, resp.text().await.unwrap())
}

// --- Fetch unary ------------------------------------------------------------

#[tokio::test]
async fn fetch_unary_bundled() {
    let base = proxy_over(
        testecho::spawn().await,
        SchemaSource::Bundled(testecho::FILE_DESCRIPTOR_SET.into()),
    )
    .await;

    let (code, body) = post_json(&base, "/echo.v1.Echo/Unary", json!({"message": "ping"})).await;
    assert_eq!(code, 0);
    assert_eq!(serde_json::from_str::<serde_json::Value>(&body).unwrap(), json!({"message": "ping"}));
}

#[tokio::test]
async fn fetch_unary_reflection() {
    let base = proxy_over(spawn_reflection_upstream().await, SchemaSource::Reflection).await;

    let (code, body) = post_json(&base, "/echo.v1.Echo/Unary", json!({"message": "reflect"})).await;
    assert_eq!(code, 0);
    assert_eq!(serde_json::from_str::<serde_json::Value>(&body).unwrap(), json!({"message": "reflect"}));
}

#[tokio::test]
async fn fetch_json_without_schema_is_unimplemented() {
    // Default config keeps the proxy binary-only; +json returns a gRPC UNIMPLEMENTED
    // status (HTTP 200, status in the header) rather than an HTTP error.
    let base = proxy_over(testecho::spawn().await, SchemaSource::None).await;
    let (code, body) = post_json(&base, "/echo.v1.Echo/Unary", json!({"message": "x"})).await;
    assert_eq!(code, tonic::Code::Unimplemented as u32);
    assert!(body.is_empty(), "error response carries no body");
}

#[tokio::test]
async fn reflection_or_bundled_falls_back_when_no_reflection() {
    // Upstream has NO reflection service, so the bundle must carry the request. The
    // fallback is non-blocking (the bundle serves immediately, no wait-for-first-load).
    let base = proxy_over(
        testecho::spawn().await,
        SchemaSource::ReflectionOrBundled(testecho::FILE_DESCRIPTOR_SET.into()),
    )
    .await;
    let (code, body) = post_json(&base, "/echo.v1.Echo/Unary", json!({"message": "fallback"})).await;
    assert_eq!(code, 0);
    assert_eq!(serde_json::from_str::<serde_json::Value>(&body).unwrap(), json!({"message": "fallback"}));
    // REST also resolves against the bundle.
    let (rcode, rbody) = rest_get(&base, "/v1/echo/via-bundle").await;
    assert_eq!(rcode, 0);
    assert_eq!(serde_json::from_str::<serde_json::Value>(&rbody).unwrap(), json!({"message": "via-bundle"}));
}

#[tokio::test]
async fn reflection_or_bundled_prefers_reflection() {
    // Upstream HAS reflection; requests succeed whether served from the bundle (before
    // the first load lands) or the reflection snapshot (after) — both cover echo.
    let base = proxy_over(
        spawn_reflection_upstream().await,
        SchemaSource::ReflectionOrBundled(testecho::FILE_DESCRIPTOR_SET.into()),
    )
    .await;
    let (code, body) = post_json(&base, "/echo.v1.Echo/Unary", json!({"message": "live"})).await;
    assert_eq!(code, 0);
    assert_eq!(serde_json::from_str::<serde_json::Value>(&body).unwrap(), json!({"message": "live"}));
}

#[tokio::test]
async fn fetch_unary_reflection_unknown_method() {
    // A method whose service the upstream can't describe surfaces UNIMPLEMENTED.
    let base = proxy_over(spawn_reflection_upstream().await, SchemaSource::Reflection).await;
    let (code, _) = post_json(&base, "/nope.Missing/Method", json!({})).await;
    assert_eq!(code, tonic::Code::Unimplemented as u32);
}

// --- REST (google.api.http) annotations, Fetch ------------------------------

async fn rest_get(base: &str, path: &str) -> (u32, String) {
    let resp = reqwest::Client::new().get(format!("{base}{path}")).send().await.unwrap();
    assert_eq!(resp.status(), 200);
    let code = resp.headers().get("grpc-status").unwrap().to_str().unwrap().parse().unwrap();
    (code, resp.text().await.unwrap())
}

async fn rest_post(base: &str, path: &str, body: serde_json::Value) -> (u32, String) {
    let resp = reqwest::Client::new()
        .post(format!("{base}{path}"))
        .header("content-type", "application/json")
        .body(body.to_string())
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let code = resp.headers().get("grpc-status").unwrap().to_str().unwrap().parse().unwrap();
    (code, resp.text().await.unwrap())
}

#[tokio::test]
async fn rest_post_body_wildcard() {
    // POST /v1/echo  body:"*"  -> echo.v1.Echo/Unary
    let base = proxy_over(
        testecho::spawn().await,
        SchemaSource::Bundled(testecho::FILE_DESCRIPTOR_SET.into()),
    )
    .await;
    let (code, body) = rest_post(&base, "/v1/echo", json!({"message": "rest-post"})).await;
    assert_eq!(code, 0);
    assert_eq!(serde_json::from_str::<serde_json::Value>(&body).unwrap(), json!({"message": "rest-post"}));
}

#[tokio::test]
async fn rest_get_path_var() {
    // GET /v1/echo/{message}  -> Unary, message bound from the path segment
    let base = proxy_over(
        testecho::spawn().await,
        SchemaSource::Bundled(testecho::FILE_DESCRIPTOR_SET.into()),
    )
    .await;
    let (code, body) = rest_get(&base, "/v1/echo/hello").await;
    assert_eq!(code, 0);
    assert_eq!(serde_json::from_str::<serde_json::Value>(&body).unwrap(), json!({"message": "hello"}));
}

#[tokio::test]
async fn rest_over_reflection() {
    // Annotations travel over reflection too: GET /v1/sleep -> Echo/Sleep -> "awake".
    let base = proxy_over(spawn_reflection_upstream().await, SchemaSource::Reflection).await;
    let (code, body) = rest_get(&base, "/v1/sleep").await;
    assert_eq!(code, 0);
    assert_eq!(serde_json::from_str::<serde_json::Value>(&body).unwrap(), json!({"message": "awake"}));
}

// --- WebSocket streaming ----------------------------------------------------

/// Connect a JSON WebSocket (single-stream) to `method`.
async fn connect_json_ws(
    base: &str,
    method: &str,
) -> tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>> {
    let ws_base = base.strip_prefix("http://").unwrap();
    let mut request = format!("ws://{ws_base}{method}").into_client_request().unwrap();
    request
        .headers_mut()
        .insert("sec-websocket-protocol", "grpc-webnext+json".parse().unwrap());
    let (ws, _resp) = tokio_tungstenite::connect_async(request).await.unwrap();
    ws
}

async fn ws_streaming_case(base: &str) {
    let mut ws = connect_json_ws(base, "/echo.v1.Echo/Stream").await;

    // Single-stream JSON: the first text frame opens the stream (method from the URL);
    // the app message is native JSON under `message`.
    let open = JsonFrame { message: Some(json!({"message": "a"})), ..Default::default() };
    ws.send(TungMessage::Text(encode_json_frame(&open).into())).await.unwrap();
    let msg = JsonFrame { message: Some(json!({"message": "b"})), ..Default::default() };
    ws.send(TungMessage::Text(encode_json_frame(&msg).into())).await.unwrap();
    let half = JsonFrame { half_close: Some(true), ..Default::default() };
    ws.send(TungMessage::Text(encode_json_frame(&half).into())).await.unwrap();

    let mut echoed = Vec::new();
    let mut status = None;
    while let Some(msg) = ws.next().await {
        let TungMessage::Text(text) = msg.unwrap() else { continue };
        let frame = decode_json_frame(&text).unwrap();
        if let Some(m) = frame.message {
            echoed.push(m["message"].as_str().unwrap().to_string());
        }
        if let Some(s) = frame.status {
            status = Some(s.code);
            break;
        }
    }
    assert_eq!(echoed, vec!["a", "b"]);
    assert_eq!(status, Some(0));
}

#[tokio::test]
async fn ws_streaming_bundled() {
    let base = proxy_over(
        testecho::spawn().await,
        SchemaSource::Bundled(testecho::FILE_DESCRIPTOR_SET.into()),
    )
    .await;
    ws_streaming_case(&base).await;
}

#[tokio::test]
async fn ws_streaming_reflection() {
    let base = proxy_over(spawn_reflection_upstream().await, SchemaSource::Reflection).await;
    ws_streaming_case(&base).await;
}

#[tokio::test]
async fn ws_json_over_size_is_resource_exhausted() {
    // A WebSocket message larger than max_message_bytes resets the stream with
    // RESOURCE_EXHAUSTED — the WS analogue of the Fetch size limit (previously WS
    // enforced no limit at all).
    let base = proxy_over_limit(
        testecho::spawn().await,
        SchemaSource::Bundled(testecho::FILE_DESCRIPTOR_SET.into()),
        128,
    )
    .await;
    let mut ws = connect_json_ws(&base, "/echo.v1.Echo/Stream").await;
    // Open with a small message, then send an oversized one.
    let open = JsonFrame { message: Some(json!({"message": "ok"})), ..Default::default() };
    ws.send(TungMessage::Text(encode_json_frame(&open).into())).await.unwrap();
    let big = JsonFrame { message: Some(json!({"message": "a".repeat(4096)})), ..Default::default() };
    ws.send(TungMessage::Text(encode_json_frame(&big).into())).await.unwrap();

    let (_echoed, status) = collect_ws_json(&mut ws).await;
    assert_eq!(status, Some(tonic::Code::ResourceExhausted as u32));
}

/// Collect message strings + terminal status from a JSON WebSocket.
async fn collect_ws_json(
    ws: &mut tokio_tungstenite::WebSocketStream<
        tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
    >,
) -> (Vec<String>, Option<u32>) {
    let mut echoed = Vec::new();
    let mut status = None;
    while let Some(msg) = ws.next().await {
        let TungMessage::Text(text) = msg.unwrap() else { continue };
        let frame = decode_json_frame(&text).unwrap();
        if let Some(m) = frame.message {
            echoed.push(m["message"].as_str().unwrap().to_string());
        }
        if let Some(s) = frame.status {
            status = Some(s.code);
            break;
        }
    }
    (echoed, status)
}

// --- REST (google.api.http) annotations, WebSocket streaming ----------------

async fn ws_rest_repeat_case(base: &str) {
    // GET /v1/repeat/{message}?count=2 -> Echo/Repeat, a no-body server-stream whose
    // whole request (message from the path, count from the query) comes from the URL.
    let mut ws = connect_json_ws(base, "/v1/repeat/hi?count=2").await;
    // Open the stream; no application message is needed.
    ws.send(TungMessage::Text(encode_json_frame(&JsonFrame::default()).into())).await.unwrap();

    let (echoed, status) = collect_ws_json(&mut ws).await;
    assert_eq!(echoed, vec!["hi", "hi"]);
    assert_eq!(status, Some(0));
}

async fn ws_rest_chat_case(base: &str) {
    // POST /v1/chat body:"*" -> Echo/Chat bidi: each frame body is a whole message.
    let mut ws = connect_json_ws(base, "/v1/chat").await;
    let open = JsonFrame { message: Some(json!({"message": "a"})), ..Default::default() };
    ws.send(TungMessage::Text(encode_json_frame(&open).into())).await.unwrap();
    let m2 = JsonFrame { message: Some(json!({"message": "b"})), ..Default::default() };
    ws.send(TungMessage::Text(encode_json_frame(&m2).into())).await.unwrap();
    ws.send(TungMessage::Text(encode_json_frame(&JsonFrame { half_close: Some(true), ..Default::default() }).into()))
        .await
        .unwrap();

    let (echoed, status) = collect_ws_json(&mut ws).await;
    assert_eq!(echoed, vec!["a", "b"]);
    assert_eq!(status, Some(0));
}

#[tokio::test]
async fn ws_rest_repeat_bundled() {
    let base = proxy_over(
        testecho::spawn().await,
        SchemaSource::Bundled(testecho::FILE_DESCRIPTOR_SET.into()),
    )
    .await;
    ws_rest_repeat_case(&base).await;
}

#[tokio::test]
async fn ws_rest_chat_bundled() {
    let base = proxy_over(
        testecho::spawn().await,
        SchemaSource::Bundled(testecho::FILE_DESCRIPTOR_SET.into()),
    )
    .await;
    ws_rest_chat_case(&base).await;
}

#[tokio::test]
async fn ws_rest_repeat_reflection() {
    let base = proxy_over(spawn_reflection_upstream().await, SchemaSource::Reflection).await;
    ws_rest_repeat_case(&base).await;
}

// --- Management: force reflection reload -------------------------------------

async fn proxy_with_admin(upstream: SocketAddr, schema: SchemaSource, admin: &str) -> String {
    let (proxy_addr, _handle) = bind_and_serve_proxy(ProxyConfig {
        upstream: format!("http://{upstream}").parse().unwrap(),
        max_message_bytes: 4 * 1024 * 1024,
        schema,
        admin_reload_path: Some(admin.to_string()),
        ..Default::default()
    })
    .await
    .unwrap();
    format!("http://{proxy_addr}")
}

#[tokio::test]
async fn admin_reload_reflection() {
    let base =
        proxy_with_admin(spawn_reflection_upstream().await, SchemaSource::Reflection, "/-/reload").await;

    let resp = reqwest::Client::new().post(format!("{base}/-/reload")).send().await.unwrap();
    assert_eq!(resp.status(), 200);

    // Transcoding still works after a forced reload.
    let (code, body) = post_json(&base, "/echo.v1.Echo/Unary", json!({"message": "after"})).await;
    assert_eq!(code, 0);
    assert_eq!(serde_json::from_str::<serde_json::Value>(&body).unwrap(), json!({"message": "after"}));
}

#[tokio::test]
async fn admin_reload_rejects_get() {
    let base =
        proxy_with_admin(spawn_reflection_upstream().await, SchemaSource::Reflection, "/-/reload").await;
    let resp = reqwest::Client::new().get(format!("{base}/-/reload")).send().await.unwrap();
    assert_eq!(resp.status(), 405);
}

#[tokio::test]
async fn admin_reload_without_reflection_conflicts() {
    // Bundled/None have nothing to reload -> 409.
    let base = proxy_with_admin(
        testecho::spawn().await,
        SchemaSource::Bundled(testecho::FILE_DESCRIPTOR_SET.into()),
        "/-/reload",
    )
    .await;
    let resp = reqwest::Client::new().post(format!("{base}/-/reload")).send().await.unwrap();
    assert_eq!(resp.status(), 409);
}
