//! WebSocket auth: connection-level handshake gate (subprotocol credential ->
//! close frame with a gRPC status) and per-stream authorization (Subscribe
//! metadata -> Reset).

use std::sync::Arc;

use bytes::Bytes;
use futures::{SinkExt, StreamExt};
use grpc_webnext_core::pb::{frame::Kind, Frame, Subscribe};
use grpc_webnext_core::{decode_frame, decode_response_body, encode_frame, encode_request_body};
use grpc_webnext_server::{bind_and_serve, ws_bearer_token, ServerConfig, CT_PROTO};
use prost::Message as _;
use testecho::pb::echo_client::EchoClient;
use testecho::pb::echo_server::EchoServer;
use testecho::pb::{EchoRequest, EchoResponse};
use testecho::EchoSvc;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::Message as TungMessage;
use tonic::{Code, Status};

async fn start(config: ServerConfig) -> String {
    let routes = tonic::service::Routes::new(EchoServer::new(EchoSvc::default()));
    let (addr, _handle) = bind_and_serve(routes, config).await.unwrap();
    format!("ws://{addr}")
}

type Ws = tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;
type Resp = tokio_tungstenite::tungstenite::handshake::client::Response;

async fn connect(url: &str, subprotocol: Option<&str>) -> (Ws, Resp) {
    let mut req = url.into_client_request().unwrap();
    if let Some(sp) = subprotocol {
        req.headers_mut().insert("sec-websocket-protocol", sp.parse().unwrap());
    }
    tokio_tungstenite::connect_async(req).await.unwrap()
}

fn subscribe(stream_id: u32, metadata: &[(&str, &str)]) -> TungMessage {
    let headers = metadata
        .iter()
        .map(|(k, v)| grpc_webnext_core::pb::Metadatum {
            key: k.to_string(),
            value: Some(grpc_webnext_core::pb::metadatum::Value::AsciiValue(v.to_string())),
        })
        .collect();
    TungMessage::Binary(encode_frame(&Frame {
        kind: Some(Kind::Subscribe(Subscribe {
            stream_id,
            method: "/echo.v1.Echo/Unary".into(),
            headers,
            timeout_millis: 0,
            initial_payload: EchoRequest { message: "hi".into() }.encode_to_vec().into(),
            json: false,
        })),
    }))
}

fn half_close(stream_id: u32) -> TungMessage {
    TungMessage::Binary(encode_frame(&Frame {
        kind: Some(Kind::HalfClose(grpc_webnext_core::pb::HalfClose { stream_id })),
    }))
}

/// A connect gate (only fires when a `bearer.*` credential is present) that admits
/// only `bearer.good`, scoped to the resolved method.
fn bearer_good_gate() -> ServerConfig {
    ServerConfig {
        connect_auth: Some(Arc::new(|method: &str, headers: &http::HeaderMap| {
            assert!(!method.is_empty(), "connect_auth is scoped to a method");
            match ws_bearer_token(headers).as_deref() {
                Some("good") => Ok(()),
                _ => Err(Status::unauthenticated("bad token")),
            }
        })),
        // These tests exercise auth, not codec gating; allow implicit codecs so the
        // plain (no codec subprotocol) connections aren't rejected first.
        allow_implicit_codec: true,
        ..Default::default()
    }
}

async fn read_close(ws: &mut Ws) -> tokio_tungstenite::tungstenite::protocol::CloseFrame {
    while let Some(msg) = ws.next().await {
        if let TungMessage::Close(cf) = msg.unwrap() {
            return cf.expect("close frame carries a gRPC status");
        }
    }
    panic!("expected a close frame");
}

#[tokio::test]
async fn connect_gate_rejects_bad_token_with_close_status() {
    let url = start(bearer_good_gate()).await;
    // A credential is presented (bearer.bad) scoped to the method URL -> gate rejects.
    let (mut ws, _) =
        connect(&format!("{url}/echo.v1.Echo/Unary"), Some("grpc-webnext, bearer.bad")).await;
    let cf = read_close(&mut ws).await;
    // UNAUTHENTICATED (16) -> private close code 4016, message in the reason.
    assert_eq!(u16::from(cf.code), 4000 + Code::Unauthenticated as u16);
    assert_eq!(cf.reason.as_str(), "bad token");
}

#[tokio::test]
async fn no_credential_opens_the_connection() {
    // No `bearer.*` subprotocol -> the connection opens even though connect_auth is
    // configured; the stream self-authenticates per call.
    let url = start(bearer_good_gate()).await;
    let (mut ws, _) = connect(&format!("{url}/echo.v1.Echo/Unary"), Some("grpc-webnext+proto")).await;
    ws.send(subscribe(1, &[])).await.unwrap();
    ws.send(half_close(1)).await.unwrap();
    let (code, _) = read_until_status(&mut ws).await.expect("expected a status");
    assert_eq!(code, 0);
}

#[tokio::test]
async fn multiplex_auth_without_method_query_is_rejected() {
    // A multiplexed socket carrying a credential must pass ?method= — else there is
    // nothing to authenticate it against, so it's a hard reject.
    let url = start(bearer_good_gate()).await;
    let (mut ws, _) = connect(&url, Some("grpc-webnext+proto+multi, bearer.good")).await;
    let cf = read_close(&mut ws).await;
    assert_eq!(u16::from(cf.code), 4000 + Code::FailedPrecondition as u16);
}

#[tokio::test]
async fn multiplex_auth_with_method_query_admits() {
    // With ?method=, the credential is authenticated against that method and the
    // connection opens.
    let url = start(bearer_good_gate()).await;
    let (mut ws, _) = connect(
        &format!("{url}/?method=/echo.v1.Echo/Unary"),
        Some("grpc-webnext+proto+multi, bearer.good"),
    )
    .await;
    ws.send(subscribe(1, &[])).await.unwrap();
    ws.send(half_close(1)).await.unwrap();
    let (code, _) = read_until_status(&mut ws).await.expect("expected a status");
    assert_eq!(code, 0);
}

#[tokio::test]
async fn connect_gate_admits_valid_token_and_echoes_subprotocol() {
    let url = start(bearer_good_gate()).await;
    // Single-stream: the URL is the route; carry the credential in the subprotocol
    // list (plus the base `grpc-webnext`), like a browser would.
    let (mut ws, resp) =
        connect(&format!("{url}/echo.v1.Echo/Unary"), Some("grpc-webnext, bearer.good")).await;

    // Server negotiated our subprotocol on the 101.
    assert_eq!(
        resp.headers().get("sec-websocket-protocol").unwrap(),
        "grpc-webnext",
    );

    // The stream works: unary echo round-trips (half-close ends the request).
    ws.send(subscribe(1, &[])).await.unwrap();
    ws.send(half_close(1)).await.unwrap();
    let (code, _) = read_until_status(&mut ws).await.expect("expected a status");
    assert_eq!(code, 0);
}

#[tokio::test]
async fn multiplex_auth_validates_bearer_against_query_method() {
    // The credential is authenticated *against the ?method= value*: a gate that only
    // admits one method rejects a connection whose query names a different one.
    let config = ServerConfig {
        connect_auth: Some(Arc::new(|method: &str, headers: &http::HeaderMap| {
            if method == "/echo.v1.Echo/Unary" && ws_bearer_token(headers).as_deref() == Some("good") {
                Ok(())
            } else {
                Err(Status::unauthenticated("denied"))
            }
        })),
        allow_implicit_codec: true,
        ..Default::default()
    };
    let url = start(config).await;

    // Query names the admitted method -> connection opens and the stream runs.
    let (mut ws, _) = connect(
        &format!("{url}/?method=/echo.v1.Echo/Unary"),
        Some("grpc-webnext+proto+multi, bearer.good"),
    )
    .await;
    ws.send(subscribe(1, &[])).await.unwrap();
    ws.send(half_close(1)).await.unwrap();
    assert_eq!(read_until_status(&mut ws).await.expect("status").0, 0);

    // Query names a different method -> rejected (proves the query method reaches the gate).
    let (mut ws2, _) = connect(
        &format!("{url}/?method=/echo.v1.Echo/Other"),
        Some("grpc-webnext+proto+multi, bearer.good"),
    )
    .await;
    let cf = read_close(&mut ws2).await;
    assert_eq!(u16::from(cf.code), 4000 + Code::Unauthenticated as u16);
}

#[tokio::test]
async fn single_stream_stream_auth_rejects_with_reset() {
    // stream_auth also gates single-stream connections, off the open-frame metadata.
    let config = ServerConfig {
        stream_auth: Some(Arc::new(|_m: &str, md: &tonic::metadata::MetadataMap| {
            match md.get("authorization").and_then(|v| v.to_str().ok()) {
                Some("Bearer good") => Ok(()),
                _ => Err(Status::unauthenticated("stream denied")),
            }
        })),
        allow_implicit_codec: true,
        ..Default::default()
    };
    let url = start(config).await;
    // No bearer (so connect_auth doesn't apply); the open frame carries no auth metadata.
    let (mut ws, _) = connect(&format!("{url}/echo.v1.Echo/Unary"), Some("grpc-webnext+proto")).await;
    ws.send(subscribe(1, &[])).await.unwrap();
    ws.send(half_close(1)).await.unwrap();
    let (code, msg) = read_until_status(&mut ws).await.expect("status");
    assert_eq!(code, Code::Unauthenticated as u32);
    assert_eq!(msg, "stream denied");
}

#[tokio::test]
async fn stream_auth_rejects_with_reset() {
    let config = ServerConfig {
        stream_auth: Some(Arc::new(|_method: &str, md: &tonic::metadata::MetadataMap| {
            match md.get("authorization").and_then(|v| v.to_str().ok()) {
                Some("Bearer good") => Ok(()),
                _ => Err(Status::unauthenticated("stream denied")),
            }
        })),
        allow_implicit_codec: true,
        ..Default::default()
    };
    let url = start(config).await;
    // Two streams on one socket -> multiplexed (frames carry method + streamId).
    let (mut ws, _) = connect(&url, Some("grpc-webnext+proto+multi")).await;

    // No metadata -> Reset UNAUTHENTICATED for that stream.
    ws.send(subscribe(1, &[])).await.unwrap();
    let (code, msg) = read_until_status(&mut ws).await.expect("expected a status");
    assert_eq!(code, Code::Unauthenticated as u32);
    assert_eq!(msg, "stream denied");

    // Correct metadata -> the call proceeds and echoes OK (status 0).
    ws.send(subscribe(2, &[("authorization", "Bearer good")])).await.unwrap();
    ws.send(half_close(2)).await.unwrap();
    let (code, _) = read_until_status(&mut ws).await.expect("expected a status");
    assert_eq!(code, 0);
}

// ---- Fetch-path per-stream auth: the same `stream_auth` hook guards unary Fetch ----

/// A `stream_auth` gate admitting only `authorization: Bearer good`.
fn stream_auth_good_gate() -> ServerConfig {
    ServerConfig {
        stream_auth: Some(Arc::new(|_m: &str, md: &tonic::metadata::MetadataMap| {
            match md.get("authorization").and_then(|v| v.to_str().ok()) {
                Some("Bearer good") => Ok(()),
                _ => Err(Status::unauthenticated("stream denied")),
            }
        })),
        ..Default::default()
    }
}

/// POST a grpc-webnext `+proto` unary request; decode the framed `(message, trailer)`.
async fn fetch_unary(base: &str, authorization: Option<&str>) -> (Bytes, u32, String) {
    let mut req = reqwest::Client::new()
        .post(format!("{base}/echo.v1.Echo/Unary"))
        .header("content-type", CT_PROTO)
        .body(encode_request_body(&EchoRequest { message: "hi".into() }.encode_to_vec()).to_vec());
    if let Some(a) = authorization {
        req = req.header("authorization", a);
    }
    let resp = req.send().await.unwrap();
    // grpc-webnext always carries the gRPC status in the trailer block, so the HTTP
    // status is 200 even on an auth failure.
    assert_eq!(resp.status(), 200);
    let raw = Bytes::from(resp.bytes().await.unwrap().to_vec());
    let (message, trailer) = decode_response_body(raw, 4 * 1024 * 1024).unwrap();
    (message, trailer.status_code, trailer.status_message)
}

#[tokio::test]
async fn fetch_stream_auth_rejects_bad_token() {
    let base = start(stream_auth_good_gate()).await.replace("ws://", "http://");
    let (_msg, code, message) = fetch_unary(&base, None).await;
    assert_eq!(code, Code::Unauthenticated as u32);
    assert_eq!(message, "stream denied");
}

#[tokio::test]
async fn fetch_stream_auth_admits_good_token() {
    let base = start(stream_auth_good_gate()).await.replace("ws://", "http://");
    let (msg, code, message) = fetch_unary(&base, Some("Bearer good")).await;
    assert_eq!(code, 0, "status: {message}");
    assert_eq!(EchoResponse::decode(msg).unwrap().message, "hi");
}

#[tokio::test]
async fn fetch_native_passthrough_is_exempt_from_stream_auth() {
    // stream_auth guards the grpc-webnext surface, not native application/grpc — that's
    // the raw gRPC surface, guarded by the router's own interceptors. A real tonic
    // client must still get through even with a deny-all stream_auth configured.
    let base = start(stream_auth_good_gate()).await.replace("ws://", "http://");
    let mut client = EchoClient::connect(base).await.unwrap();
    let resp = client.unary(EchoRequest { message: "native".into() }).await.unwrap();
    assert_eq!(resp.into_inner().message, "native");
}

#[test]
fn ws_bearer_token_extracts_the_token() {
    use grpc_webnext_server::ws_bearer_token;
    let mut h = http::HeaderMap::new();
    h.insert(
        "sec-websocket-protocol",
        "grpc-webnext, grpc-webnext+proto, bearer.abc.def-token".parse().unwrap(),
    );
    assert_eq!(ws_bearer_token(&h), Some("abc.def-token".to_string()));

    // No bearer entry -> None.
    let mut h2 = http::HeaderMap::new();
    h2.insert("sec-websocket-protocol", "grpc-webnext+json".parse().unwrap());
    assert_eq!(ws_bearer_token(&h2), None);
}

/// Drain frames until a terminal Trailer/Reset; returns (status_code, message).
async fn read_until_status<S>(ws: &mut S) -> Option<(u32, String)>
where
    S: StreamExt<Item = Result<TungMessage, tokio_tungstenite::tungstenite::Error>> + Unpin,
{
    while let Some(msg) = ws.next().await {
        let TungMessage::Binary(data) = msg.ok()? else { continue };
        match decode_frame(&data).ok()?.kind? {
            Kind::Trailer(t) => return Some((t.status_code, t.status_message)),
            Kind::Reset(r) => return Some((r.status_code, r.status_message)),
            _ => {}
        }
    }
    None
}
