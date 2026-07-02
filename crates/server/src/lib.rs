//! Native grpc-webnext server.
//!
//! Wraps a tonic [`Routes`] and serves, on one port:
//!   * native `application/grpc` — forwarded to the router untouched (README #9),
//!   * grpc-webnext unary over Fetch — translated into a native gRPC call,
//!   * grpc-webnext streaming over WebSocket — translated per stream.
//!
//! Binary-only for v1 (`+json` -> UNIMPLEMENTED), like the proxy.

use std::convert::Infallible;
use std::net::SocketAddr;

use bytes::Bytes;
use grpc_webnext_core::pb::Trailer;
use grpc_webnext_core::{deframe_all, encode_response_body, grpc_frame, metadata};
use http::{HeaderMap, Request, Response, StatusCode};
use http_body_util::combinators::UnsyncBoxBody;
use http_body_util::{BodyExt, Full};
use hyper::body::Incoming;
use hyper_util::rt::{TokioExecutor, TokioIo};
use tokio::net::TcpListener;
use tonic::body::Body as TonicBody;
use tonic::service::Routes;
use tower::ServiceExt;

pub mod ws;

pub const CT_PROTO: &str = "application/grpc-webnext+proto";
pub const CT_JSON: &str = "application/grpc-webnext+json";
const CT_GRPC: &str = "application/grpc";

pub type BoxError = Box<dyn std::error::Error + Send + Sync>;
pub type ResBody = UnsyncBoxBody<Bytes, BoxError>;

#[derive(Clone)]
pub struct ServerConfig {
    pub max_message_bytes: usize,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self { max_message_bytes: 4 * 1024 * 1024 }
    }
}

/// Serve grpc-webnext + native gRPC from `routes` on `listener`.
pub async fn serve(listener: TcpListener, routes: Routes, config: ServerConfig) -> std::io::Result<()> {
    loop {
        let (stream, _peer) = listener.accept().await?;
        let io = TokioIo::new(stream);
        let routes = routes.clone();
        let config = config.clone();

        tokio::spawn(async move {
            let service = hyper::service::service_fn(move |req| {
                handle(routes.clone(), config.clone(), req)
            });
            if let Err(e) = hyper_util::server::conn::auto::Builder::new(TokioExecutor::new())
                .serve_connection_with_upgrades(io, service)
                .await
            {
                tracing::debug!("connection error: {e}");
            }
        });
    }
}

/// Bind an ephemeral local address and serve; convenience for tests/mains.
pub async fn bind_and_serve(
    routes: Routes,
    config: ServerConfig,
) -> std::io::Result<(SocketAddr, tokio::task::JoinHandle<std::io::Result<()>>)> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let handle = tokio::spawn(serve(listener, routes, config));
    Ok((addr, handle))
}

async fn handle(
    routes: Routes,
    config: ServerConfig,
    mut req: Request<Incoming>,
) -> Result<Response<ResBody>, Infallible> {
    // WebSocket streaming path.
    if hyper_tungstenite::is_upgrade_request(&req) {
        match hyper_tungstenite::upgrade(&mut req, None) {
            Ok((response, websocket)) => {
                tokio::spawn(ws::serve(routes, websocket, config));
                return Ok(response.map(boxed_full));
            }
            Err(e) => return Ok(text_response(StatusCode::BAD_REQUEST, format!("upgrade failed: {e}"))),
        }
    }

    let content_type = req
        .headers()
        .get(http::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_string();

    match content_type.as_str() {
        CT_PROTO => Ok(unary(routes, config, req).await),
        CT_JSON => Ok(text_response(
            StatusCode::NOT_IMPLEMENTED,
            "native server is binary-only in v1; +json unsupported",
        )),
        // Native gRPC (and anything else): forward to the router untouched.
        _ => Ok(passthrough(routes, req).await),
    }
}

/// Forward a request to the inner router unchanged (native gRPC same-port).
async fn passthrough(routes: Routes, req: Request<Incoming>) -> Response<ResBody> {
    let resp = routes.oneshot(req).await.unwrap_or_else(|e| match e {});
    resp.map(|b| b.map_err(Into::into).boxed_unsync())
}

/// Translate a grpc-webnext unary request into a native gRPC call to the router
/// and write the `[len|message][len|trailer]` Fetch response body.
async fn unary(routes: Routes, config: ServerConfig, req: Request<Incoming>) -> Response<ResBody> {
    let (parts, body) = req.into_parts();
    let path = match parts.uri.path_and_query().cloned() {
        Some(p) => p,
        None => return text_response(StatusCode::BAD_REQUEST, "missing method path"),
    };

    let message = match body.collect().await {
        Ok(c) => c.to_bytes(),
        Err(e) => return text_response(StatusCode::BAD_REQUEST, format!("read body: {e}")),
    };
    if message.len() > config.max_message_bytes {
        return text_response(StatusCode::PAYLOAD_TOO_LARGE, "request message exceeds size limit");
    }

    // Build a native gRPC request into the router: reframe body, force content-type.
    let mut builder = Request::builder().method(http::Method::POST).uri(path);
    for (name, value) in parts.headers.iter() {
        if !metadata::is_denied(name) {
            builder = builder.header(name.clone(), value.clone());
        }
    }
    builder = builder
        .header(http::header::CONTENT_TYPE, CT_GRPC)
        .header("te", "trailers");
    if let Some(v) = parts.headers.get("grpc-timeout") {
        builder = builder.header("grpc-timeout", v.clone());
    }
    let grpc_req = builder
        .body(TonicBody::new(Full::new(grpc_frame(&message))))
        .expect("valid request");

    let resp = routes.oneshot(grpc_req).await.unwrap_or_else(|e| match e {});
    let (resp_parts, resp_body) = resp.into_parts();

    let collected = match resp_body.collect().await {
        Ok(c) => c,
        Err(e) => return text_response(StatusCode::BAD_GATEWAY, format!("upstream body: {e}")),
    };
    let trailer_headers = collected.trailers().cloned().unwrap_or_default();
    let body_bytes = collected.to_bytes();
    let out_message = deframe_all(&body_bytes).into_iter().next().unwrap_or_default();

    // gRPC status: prefer trailers, fall back to headers (trailers-only errors).
    let (status_code, status_message) = read_status(&trailer_headers, &resp_parts.headers);
    let trailer = Trailer {
        stream_id: 0,
        status_code,
        status_message,
        trailers: metadata::metadata_to_vec(&tonic::metadata::MetadataMap::from_headers(trailer_headers)),
    };

    let framed = encode_response_body(&out_message, &trailer);
    let mut response = Response::builder()
        .status(StatusCode::OK)
        .header(http::header::CONTENT_TYPE, CT_PROTO);
    if let Some(headers) = response.headers_mut() {
        metadata::merge_metadata_into_headers(
            &tonic::metadata::MetadataMap::from_headers(resp_parts.headers),
            headers,
        );
    }
    response.body(boxed_full(Full::new(framed))).expect("valid response")
}

/// Read gRPC status code + message from trailers, falling back to headers.
fn read_status(trailers: &HeaderMap, headers: &HeaderMap) -> (u32, String) {
    let get = |name: &str| trailers.get(name).or_else(|| headers.get(name));
    let code = get("grpc-status")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.parse::<u32>().ok())
        .unwrap_or(0);
    let message = get("grpc-message")
        .and_then(|v| v.to_str().ok())
        .map(percent_decode)
        .unwrap_or_default();
    (code, message)
}

/// Minimal gRPC `grpc-message` percent-decoding (%XX).
fn percent_decode(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            if let Ok(byte) = u8::from_str_radix(&s[i + 1..i + 3], 16) {
                out.push(byte);
                i += 3;
                continue;
            }
        }
        out.push(bytes[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

fn boxed_full(body: Full<Bytes>) -> ResBody {
    body.map_err(|e: Infallible| match e {}).boxed_unsync()
}

fn text_response(status: StatusCode, message: impl Into<String>) -> Response<ResBody> {
    Response::builder()
        .status(status)
        .body(boxed_full(Full::new(Bytes::from(message.into()))))
        .unwrap()
}
