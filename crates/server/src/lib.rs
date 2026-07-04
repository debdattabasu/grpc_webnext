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
use grpc_webnext_core::{deframe_all, encode_response_body, grpc_frame, metadata, TranscodeError, Transcoder};
use std::sync::Arc;
use http::uri::PathAndQuery;
use http::{HeaderMap, Request, Response, StatusCode};
use http_body_util::combinators::UnsyncBoxBody;
use http_body_util::{BodyExt, Full};
use hyper::body::Incoming;
use hyper_util::rt::{TokioExecutor, TokioIo};
use tokio::net::TcpListener;
use tonic::body::Body as TonicBody;
use tonic::metadata::MetadataMap;
use tonic::service::Routes;
use tonic::{Code, Status};
use tower::ServiceExt;

pub mod ws;

pub const CT_PROTO: &str = "application/grpc-webnext+proto";
pub const CT_JSON: &str = "application/grpc-webnext+json";
const CT_GRPC: &str = "application/grpc";

/// The WebSocket subprotocol name this server negotiates. A browser can also pass
/// an auth credential alongside it (e.g. `["grpc-webnext", "bearer.<token>"]`) —
/// the subprotocol list is the only handshake header browser JS can set.
pub const WS_SUBPROTOCOL: &str = "grpc-webnext";
/// Optional subprotocol that pins the connection codec to JSON up front (otherwise
/// the codec is chosen by the first frame's type: text -> JSON, binary -> proto).
pub const WS_SUBPROTOCOL_JSON: &str = "grpc-webnext+json";
/// Optional subprotocol that pins the connection codec to binary protobuf up front.
pub const WS_SUBPROTOCOL_PROTO: &str = "grpc-webnext+proto";
/// Multiplexing variants: many streams share one socket (frames carry `streamId`).
pub const WS_SUBPROTOCOL_JSON_MULTI: &str = "grpc-webnext+json+multi";
pub const WS_SUBPROTOCOL_PROTO_MULTI: &str = "grpc-webnext+proto+multi";

pub type BoxError = Box<dyn std::error::Error + Send + Sync>;
pub type ResBody = UnsyncBoxBody<Bytes, BoxError>;

/// Authorize a WebSocket connection at handshake time, given the gRPC method the
/// connection's credential is scoped to and the request headers (the credential
/// itself rides the `Sec-WebSocket-Protocol` list as `bearer.<token>`, read with
/// [`ws_bearer_token`]). Invoked **only when a `bearer.*` subprotocol is present** —
/// a token-less connection opens and its streams self-authenticate per call. The
/// method is the URL path (single-stream), the `?method=` query (multiplexed), or
/// the annotation binding. Returning `Err(status)` accepts the upgrade then
/// immediately closes with a private close code `4000 + status.code()` — no stream
/// is created, so the cost matches a refused upgrade while still handing browser JS
/// a readable `CloseEvent.code`/`.reason`.
pub type ConnectAuthFn = Arc<dyn Fn(&str, &HeaderMap) -> Result<(), Status> + Send + Sync>;

/// Authorize a single stream from its method path and request metadata. Returning
/// `Err(status)` answers that `Subscribe` with a `Reset` carrying the status. This
/// is the authoritative, gRPC-faithful check — run on every new stream.
pub type StreamAuthFn = Arc<dyn Fn(&str, &MetadataMap) -> Result<(), Status> + Send + Sync>;

#[derive(Clone)]
pub struct ServerConfig {
    pub max_message_bytes: usize,
    /// Descriptor-based JSON<->proto transcoder. When set, `+json` requests are
    /// transcoded to the router's binary protobuf and back. When `None`, `+json`
    /// is answered with UNIMPLEMENTED.
    pub transcoder: Option<Arc<Transcoder>>,
    /// Optional connection-level WebSocket gate (see [`ConnectAuthFn`]).
    pub connect_auth: Option<ConnectAuthFn>,
    /// Optional per-stream authorization (see [`StreamAuthFn`]).
    pub stream_auth: Option<StreamAuthFn>,
    /// Allow "implicit codec" access to **main** endpoints (`/pkg.Service/Method`):
    /// a Fetch request with no content-type or `application/json`, and a WebSocket
    /// with no codec subprotocol (first-frame inference). Off by default — main
    /// endpoints then require an explicit grpc-webnext content-type/subprotocol,
    /// and plain `application/json`/blank is reserved for annotated REST endpoints.
    pub allow_implicit_codec: bool,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            max_message_bytes: 4 * 1024 * 1024,
            transcoder: None,
            connect_auth: None,
            stream_auth: None,
            allow_implicit_codec: false,
        }
    }
}

/// Parse the `Sec-WebSocket-Protocol` request header into its comma-separated
/// tokens. Use inside a [`ConnectAuthFn`] to read a credential a browser smuggled
/// through the subprotocol list (e.g. find the entry starting `bearer.`).
pub fn ws_subprotocols(headers: &HeaderMap) -> Vec<String> {
    headers
        .get(http::header::SEC_WEBSOCKET_PROTOCOL)
        .and_then(|v| v.to_str().ok())
        .map(|s| s.split(',').map(|t| t.trim().to_string()).filter(|t| !t.is_empty()).collect())
        .unwrap_or_default()
}

/// Extract the bearer token a client placed in the WebSocket subprotocol list as
/// `bearer.<token>`. The client derives it from the call's `authorization` metadata
/// (stripping a `Bearer ` scheme), so a `ConnectAuthFn` can hard-reject the handshake
/// before any frame is read. Returns the raw `<token>`.
pub fn ws_bearer_token(headers: &HeaderMap) -> Option<String> {
    ws_subprotocols(headers)
        .into_iter()
        .find_map(|p| p.strip_prefix("bearer.").map(|t| t.to_string()))
}

/// Read a single query parameter's (percent-decoded) value.
fn query_param(query: Option<&str>, key: &str) -> Option<String> {
    query?.split('&').find_map(|kv| {
        let (k, v) = kv.split_once('=')?;
        (k == key).then(|| percent_decode(v))
    })
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
        // Connection-level auth from the handshake headers (the subprotocol slot
        // carries any credential). A rejection is deferred to the accepted socket
        // so the client can read a gRPC status off the close frame.
        let offered = ws_subprotocols(req.headers());
        let has = |s: &str| offered.iter().any(|p| p == s);
        // Resolve codec + multiplexing from the codec subprotocol, if any.
        // `explicit` = a grpc-webnext codec subprotocol was offered (main-endpoint OK).
        let (codec, multi, explicit) = if has(WS_SUBPROTOCOL_PROTO_MULTI) {
            (Some(false), true, true)
        } else if has(WS_SUBPROTOCOL_JSON_MULTI) {
            (Some(true), true, true)
        } else if has(WS_SUBPROTOCOL_PROTO) {
            (Some(false), false, true)
        } else if has(WS_SUBPROTOCOL_JSON) {
            (Some(true), false, true)
        } else if has("application/json") {
            (Some(true), false, false)
        } else {
            (None, false, false)
        };

        // Annotation route: the WS URL matches a `google.api.http` binding.
        let ws_annotation = config
            .transcoder
            .as_ref()
            .and_then(|tc| tc.match_ws(req.uri().path(), req.uri().query()))
            .map(Arc::new);

        // Surface gating. Annotation routes are JSON/text and accept only a blank or
        // `application/json` subprotocol. Main routes require a grpc-webnext codec
        // subprotocol; blank/`application/json` is rejected unless implicit codecs are
        // allowed — and never for an RPC that has an annotation of its own (its plain
        // surface is the annotated route).
        let codec_reject = if ws_annotation.is_some() {
            // REST route: single-stream JSON only — blank / application/json /
            // grpc-webnext+json. Binary and multiplexing are the wrong surface.
            let proto = codec == Some(false);
            (proto || multi).then(|| {
                Status::failed_precondition(
                    "REST WebSocket routes are single-stream JSON: use a blank, application/json, or grpc-webnext+json subprotocol",
                )
            })
        } else if explicit {
            None // main + a grpc-webnext codec is the SDK contract
        } else {
            // main + plain (blank/application-json): allowed only with implicit codecs.
            (!config.allow_implicit_codec).then(|| {
                Status::unimplemented("this websocket requires a grpc-webnext+proto or grpc-webnext+json subprotocol")
            })
        };
        // Connection auth. Only fires when the client presented a credential (a
        // `bearer.*` subprotocol). The method it is scoped to is the URL path
        // (single-stream), the `?method=` query (multiplexed), or the annotation
        // binding. A credential with no resolvable method is a hard reject.
        // The gate applies only when the server does connection auth *and* the client
        // presented a credential. Otherwise the connection just opens.
        let auth_reject = match &config.connect_auth {
            Some(auth) if ws_bearer_token(req.headers()).is_some() => {
                let auth_method = if let Some(ann) = &ws_annotation {
                    Some(ann.grpc_method().to_string())
                } else if !multi {
                    Some(req.uri().path().to_string())
                } else {
                    query_param(req.uri().query(), "method")
                };
                match auth_method {
                    Some(m) => auth(&m, req.headers()).err(),
                    None => Some(Status::failed_precondition(
                        "a multiplexed WebSocket carrying an auth subprotocol must pass ?method=",
                    )),
                }
            }
            _ => None, // no connect_auth, or no credential -> open; streams self-auth per call
        };
        let reject = auth_reject.or(codec_reject);
        // Echo whichever recognized subprotocol the client offered (browser `ws`
        // clients fail the handshake if the server ignores an offered subprotocol).
        let echo = [
            WS_SUBPROTOCOL_PROTO_MULTI,
            WS_SUBPROTOCOL_JSON_MULTI,
            WS_SUBPROTOCOL_PROTO,
            WS_SUBPROTOCOL_JSON,
            WS_SUBPROTOCOL,
            "application/json",
        ]
        .into_iter()
        .find(|&p| has(p));
        // Mode. Annotation routes are text-locked, single-stream, method from the
        // binding. Otherwise single-stream takes the method from the URL, and
        // multiplexed connections carry the method in each Subscribe frame.
        let params = match &ws_annotation {
            Some(ann) => ws::WsParams {
                codec: Some(true),
                multi: false,
                method: Some(ann.grpc_method().to_string()),
                annotation: ws_annotation.clone(),
            },
            None => ws::WsParams {
                codec,
                multi,
                method: (!multi).then(|| req.uri().path().to_string()),
                annotation: None,
            },
        };
        match hyper_tungstenite::upgrade(&mut req, None) {
            Ok((mut response, websocket)) => {
                if let Some(p) = echo {
                    response.headers_mut().insert(
                        http::header::SEC_WEBSOCKET_PROTOCOL,
                        http::HeaderValue::from_static(p),
                    );
                }
                tokio::spawn(ws::serve(routes, websocket, config, reject, params));
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

    if content_type == CT_PROTO {
        // Binary is the SDK contract on main gRPC paths. A REST-annotated URL is
        // JSON-only, so binary there is the wrong surface — reject it explicitly
        // rather than letting the path be (mis)parsed as a gRPC method.
        if let Some(tc) = &config.transcoder {
            if tc.match_ws(req.uri().path(), req.uri().query()).is_some() {
                return Ok(text_response(
                    StatusCode::UNSUPPORTED_MEDIA_TYPE,
                    "REST-annotated endpoints are JSON-only; use application/json or application/grpc-webnext+json",
                ));
            }
        }
        Ok(unary(routes, config, req, CT_PROTO.to_string()).await)
    } else if content_type == CT_JSON || content_type == "application/json" || content_type.is_empty() {
        // All JSON forms route through the REST matcher first. `+json` is the SDK
        // JSON contract (always allowed on a main path); plain JSON is flag-gated.
        if config.transcoder.is_none() {
            return Ok(text_response(
                StatusCode::NOT_IMPLEMENTED,
                "JSON requires a transcoder (ServerConfig::transcoder)",
            ));
        }
        Ok(json_fetch(routes, config, req, content_type == CT_JSON).await)
    } else if content_type.starts_with("application/grpc") {
        // Native gRPC family (same-port coexistence): forward untouched.
        Ok(passthrough(routes, req).await)
    } else {
        Ok(text_response(
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            format!("unsupported content-type: {content_type}"),
        ))
    }
}

/// The JSON Fetch path, routed by URL. An annotated REST binding is transcoded; a
/// main gRPC path is a direct call — `sdk_json` (grpc-webnext+json) is always allowed
/// there, while plain JSON (`application/json` / none) requires `allow_implicit_codec`.
async fn json_fetch(
    routes: Routes,
    config: ServerConfig,
    req: Request<Incoming>,
    sdk_json: bool,
) -> Response<ResBody> {
    let tc = config.transcoder.clone().expect("json_fetch requires a transcoder");
    let (parts, body) = req.into_parts();
    let body_bytes = match body.collect().await {
        Ok(c) => c.to_bytes(),
        Err(e) => return text_response(StatusCode::BAD_REQUEST, format!("read body: {e}")),
    };
    if body_bytes.len() > config.max_message_bytes {
        return text_response(StatusCode::PAYLOAD_TOO_LARGE, "request message exceeds size limit");
    }
    let path = parts.uri.path().to_string();
    let query = parts.uri.query();

    // 1) Annotated REST endpoint: (method, path) maps onto a gRPC method.
    match tc.transcode_http_request(parts.method.as_str(), &path, query, &body_bytes) {
        Ok(Some(call)) => {
            let gp: PathAndQuery = match call.grpc_method.parse() {
                Ok(p) => p,
                Err(_) => return webnext_error("application/json", Code::Internal, "bad transcoded method"),
            };
            return json_unary_call(routes, &config, gp, call.message.into(), &parts.headers, "application/json").await;
        }
        Ok(None) => {}
        Err(e) => return webnext_error("application/json", Code::InvalidArgument, &format!("bad REST request: {e}")),
    }

    // 2) Main gRPC path. `+json` is the SDK contract; plain JSON needs the flag.
    let resp_ct = if sdk_json { CT_JSON } else { "application/json" };
    if !sdk_json && !config.allow_implicit_codec {
        return text_response(
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            "this gRPC method path requires content-type application/grpc-webnext+json (or set allow_implicit_codec to accept plain JSON here)",
        );
    }
    let pq = match parts.uri.path_and_query().cloned() {
        Some(p) => p,
        None => return text_response(StatusCode::BAD_REQUEST, "missing method path"),
    };
    let proto = match tc.request_json_to_proto(pq.path(), &body_bytes) {
        Ok(p) => p,
        Err(TranscodeError::UnknownMethod(_)) => {
            return webnext_error(resp_ct, Code::Unimplemented, &format!("no such method: {}", pq.path()))
        }
        Err(e) => return webnext_error(resp_ct, Code::InvalidArgument, &format!("bad json request: {e}")),
    };
    json_unary_call(routes, &config, pq, proto.into(), &parts.headers, resp_ct).await
}

/// Run a unary gRPC call with an already-encoded protobuf request and render a
/// native-JSON Fetch response (bare JSON message body; status/metadata in headers).
async fn json_unary_call(
    routes: Routes,
    config: &ServerConfig,
    grpc_path: PathAndQuery,
    message: Bytes,
    req_headers: &HeaderMap,
    resp_ct: &str,
) -> Response<ResBody> {
    let mut builder = Request::builder().method(http::Method::POST).uri(grpc_path.clone());
    for (name, value) in req_headers.iter() {
        if !metadata::is_denied(name) {
            builder = builder.header(name.clone(), value.clone());
        }
    }
    builder = builder.header(http::header::CONTENT_TYPE, CT_GRPC).header("te", "trailers");
    if let Some(v) = req_headers.get("grpc-timeout") {
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
    let mut out_message = deframe_all(&body_bytes).into_iter().next().unwrap_or_default();
    let (status_code, status_message) = read_status(&trailer_headers, &resp_parts.headers);

    if status_code == 0 && !out_message.is_empty() {
        match config.transcoder.as_ref().unwrap().response_proto_to_json(grpc_path.path(), &out_message) {
            Ok(j) => out_message = j.into(),
            Err(e) => return webnext_error(resp_ct, Code::Internal, &format!("bad json response: {e}")),
        }
    }
    json_fetch_response(resp_ct, out_message, status_code, &status_message, &resp_parts.headers, &trailer_headers)
}

/// Whether a content-type selects the JSON codec. `application/json` is an alias
/// for `application/grpc-webnext+json` on the Fetch path.
fn is_json_ct(ct: &str) -> bool {
    ct == CT_JSON || ct == "application/json"
}

/// Forward a request to the inner router unchanged (native gRPC same-port).
async fn passthrough(routes: Routes, req: Request<Incoming>) -> Response<ResBody> {
    let resp = routes.oneshot(req).await.unwrap_or_else(|e| match e {});
    resp.map(|b| b.map_err(Into::into).boxed_unsync())
}

/// Translate a grpc-webnext unary request into a native gRPC call to the router
/// and write the `[len|message][len|trailer]` Fetch response body. When `json`,
/// the request/response messages are transcoded JSON<->protobuf.
async fn unary(
    routes: Routes,
    config: ServerConfig,
    req: Request<Incoming>,
    resp_ct: String,
) -> Response<ResBody> {
    let json = is_json_ct(&resp_ct);
    let ct = resp_ct.as_str();
    let (parts, body) = req.into_parts();
    let path = match parts.uri.path_and_query().cloned() {
        Some(p) => p,
        None => return text_response(StatusCode::BAD_REQUEST, "missing method path"),
    };

    let mut message = match body.collect().await {
        Ok(c) => c.to_bytes(),
        Err(e) => return text_response(StatusCode::BAD_REQUEST, format!("read body: {e}")),
    };
    if message.len() > config.max_message_bytes {
        return text_response(StatusCode::PAYLOAD_TOO_LARGE, "request message exceeds size limit");
    }

    // JSON request -> binary protobuf for the router.
    if json {
        match config.transcoder.as_ref().unwrap().request_json_to_proto(path.path(), &message) {
            Ok(proto) => message = proto.into(),
            Err(e) => return webnext_error(ct, Code::InvalidArgument, &format!("bad json request: {e}")),
        }
    }

    // Build a native gRPC request into the router: reframe body, force content-type.
    let mut builder = Request::builder().method(http::Method::POST).uri(path.clone());
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
    let mut out_message = deframe_all(&body_bytes).into_iter().next().unwrap_or_default();

    let (status_code, status_message) = read_status(&trailer_headers, &resp_parts.headers);

    // Binary protobuf response -> JSON (only for a successful message).
    if json && status_code == 0 && !out_message.is_empty() {
        match config.transcoder.as_ref().unwrap().response_proto_to_json(path.path(), &out_message) {
            Ok(j) => out_message = j.into(),
            Err(e) => return webnext_error(ct, Code::Internal, &format!("bad json response: {e}")),
        }
    }

    if json {
        // Native JSON: bare JSON message body, status + metadata in HTTP headers.
        return json_fetch_response(
            ct,
            out_message,
            status_code,
            &status_message,
            &resp_parts.headers,
            &trailer_headers,
        );
    }

    // Binary proto: `[len|message][len|trailer]` body, initial metadata in headers.
    let trailer = Trailer {
        stream_id: 0,
        status_code,
        status_message,
        trailers: metadata::metadata_to_vec(&tonic::metadata::MetadataMap::from_headers(trailer_headers)),
    };
    let framed = encode_response_body(&out_message, &trailer);
    let mut response = Response::builder().status(StatusCode::OK).header(http::header::CONTENT_TYPE, ct);
    if let Some(headers) = response.headers_mut() {
        metadata::merge_metadata_into_headers(
            &tonic::metadata::MetadataMap::from_headers(resp_parts.headers),
            headers,
        );
    }
    response.body(boxed_full(Full::new(framed))).expect("valid response")
}

/// Build a native-JSON Fetch response: the JSON message is the bare body; gRPC
/// status and metadata (initial + trailing) travel in HTTP response headers.
fn json_fetch_response(
    content_type: &str,
    message: Bytes,
    status_code: u32,
    status_message: &str,
    initial: &HeaderMap,
    trailing: &HeaderMap,
) -> Response<ResBody> {
    let mut builder = Response::builder()
        .status(StatusCode::OK)
        .header(http::header::CONTENT_TYPE, content_type)
        .header("grpc-status", status_code.to_string());
    if !status_message.is_empty() {
        if let Ok(v) = http::HeaderValue::from_str(&percent_encode(status_message)) {
            builder = builder.header("grpc-message", v);
        }
    }
    if let Some(headers) = builder.headers_mut() {
        let md = |h: &HeaderMap| tonic::metadata::MetadataMap::from_headers(h.clone());
        metadata::merge_metadata_into_headers(&md(initial), headers);
        metadata::merge_metadata_into_headers(&md(trailing), headers);
    }
    // On error there is no message body.
    let body = if status_code == 0 { message } else { Bytes::new() };
    builder.body(boxed_full(Full::new(body))).expect("valid response")
}

/// Minimal percent-encoding for a `grpc-message` header value.
fn percent_encode(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        if b.is_ascii_alphanumeric() || matches!(b, b' ' | b'-' | b'_' | b'.' | b'/' | b':') {
            out.push(b as char);
        } else {
            out.push_str(&format!("%{b:02X}"));
        }
    }
    out
}

/// A grpc-webnext error response: native-JSON (status in headers) for `+json`,
/// otherwise a framed empty-message + status-trailer body.
fn webnext_error(content_type: &str, code: Code, message: &str) -> Response<ResBody> {
    if is_json_ct(content_type) {
        return json_fetch_response(
            content_type,
            Bytes::new(),
            code as u32,
            message,
            &HeaderMap::new(),
            &HeaderMap::new(),
        );
    }
    let trailer = Trailer {
        stream_id: 0,
        status_code: code as u32,
        status_message: message.to_string(),
        trailers: Vec::new(),
    };
    let framed = encode_response_body(&[], &trailer);
    Response::builder()
        .status(StatusCode::OK)
        .header(http::header::CONTENT_TYPE, content_type)
        .body(boxed_full(Full::new(framed)))
        .expect("valid response")
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
