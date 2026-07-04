//! grpc-webnext proxy library.
//!
//! On one port, in front of any upstream gRPC server:
//!   * native `application/grpc` — forwarded to the upstream untouched (README #9),
//!   * grpc-webnext unary over Fetch — translated into a native gRPC call,
//!   * grpc-webnext streaming over WebSocket — translated per stream.
//!
//! Binary `+proto` is schema-agnostic: message bytes are forwarded opaquely, streamed
//! without buffering. `+json` is transcoded to/from the upstream's binary protobuf
//! when a descriptor source is configured (upstream reflection or a bundled
//! `FileDescriptorSet`, see [`SchemaSource`]); with no source it stays `UNIMPLEMENTED`.
//! Because both surfaces reuse the same core transcoder as the native library, a client
//! can't tell a proxied `+json` response from a native one.
//!
//! Deadlines are enforced locally (the upstream call is dropped on expiry) *and*
//! forwarded downstream as `grpc-timeout`; client cancellation (Reset / disconnect)
//! propagates to the upstream by dropping the call.

use std::convert::Infallible;
use std::net::SocketAddr;
use std::time::Duration;

use bytes::Bytes;
use grpc_webnext_core::pb::Trailer;
use grpc_webnext_core::{
    deframe_all, encode_response_body, encode_trailer_block, grpc_frame, Transcoder,
    EMPTY_MESSAGE_BLOCK, LEN_PREFIX,
};
use http::uri::PathAndQuery;
use http::{HeaderMap, Request, Response, StatusCode};
use http_body_util::combinators::UnsyncBoxBody;
use http_body_util::{BodyExt, Full, StreamBody};
use hyper::body::{Frame as BodyFrame, Incoming};
use hyper_util::rt::{TokioExecutor, TokioIo};
use tokio::net::TcpListener;
use tonic::body::Body as TonicBody;
use tonic::metadata::MetadataMap;
use tonic::transport::Channel;
use tonic::Code;
use tower::ServiceExt;

pub mod metadata;
mod reflect;
mod schema;
pub mod ws;

pub use schema::{Schema, SchemaSource};

pub const CT_PROTO: &str = "application/grpc-webnext+proto";
pub const CT_JSON: &str = "application/grpc-webnext+json";
const CT_GRPC: &str = "application/grpc";

/// The proxy owns the client-facing deadline: it drops the call at the exact
/// deadline (surfacing DEADLINE_EXCEEDED) and forwards `grpc-timeout` downstream
/// with this grace so the upstream's own enforcement is a later backstop rather
/// than racing the local timer.
pub const DEADLINE_GRACE: Duration = Duration::from_millis(500);

pub type BoxError = Box<dyn std::error::Error + Send + Sync>;
pub type ResBody = UnsyncBoxBody<Bytes, BoxError>;

#[derive(Clone)]
pub struct ProxyConfig {
    /// Upstream gRPC endpoint (e.g. `http://127.0.0.1:50051`).
    pub upstream: http::Uri,
    /// Max bytes to buffer for a single request message (the response is streamed).
    pub max_message_bytes: usize,
    /// Max concurrent logical streams per WebSocket connection.
    pub max_concurrent_streams: usize,
    /// Interval between WebSocket keepalive pings on an open streaming connection.
    /// A native ping (RFC 6455 §5.5.2) is control-frame traffic the peer auto-answers,
    /// so it keeps idle-timeout proxies/LBs from dropping a quiet stream. `None`
    /// disables keepalive (the default).
    pub ws_keepalive: Option<Duration>,
    /// How long to wait for a peer's pong (or any frame) after a keepalive ping before
    /// declaring the connection dead and dropping it — the gRPC `keepalive_timeout`
    /// analogue. Only applies when `ws_keepalive` is set. Defaults to 20s (gRPC's
    /// default); a peer silent for `ws_keepalive + ws_keepalive_timeout` is dropped.
    pub ws_keepalive_timeout: Duration,
    /// Descriptor source for `+json` termination. `None` (the default) keeps the proxy
    /// binary-only (`+json` -> UNIMPLEMENTED); `Reflection` transcodes using descriptors
    /// fetched from the upstream's reflection service; `Bundled` uses a supplied
    /// `FileDescriptorSet`. The binary `+proto` path is unaffected either way.
    pub schema: SchemaSource,
    /// How often to refresh the reflection descriptor snapshot (only for
    /// `SchemaSource::Reflection`). Defaults to 4h. A forced reload via
    /// `admin_reload_path` is independent of this interval.
    pub reflection_ttl: Duration,
    /// Optional management endpoint that forces an immediate reflection reload. When
    /// `Some(path)`, a `POST` to that exact path reloads and returns 200/503; all other
    /// methods on it return 405. `None` (the default) disables it. Restrict network
    /// access to this path — it is unauthenticated.
    pub admin_reload_path: Option<String>,
    /// Accept plain `application/json` (or a blank content-type) on a *main* gRPC method
    /// path — i.e. treat it as `+json`. Off by default: plain JSON is then only accepted
    /// on REST-annotated URLs, and a main path requires `application/grpc-webnext+json`.
    /// REST annotation routes work regardless of this flag.
    pub allow_implicit_codec: bool,
}

impl Default for ProxyConfig {
    fn default() -> Self {
        Self {
            upstream: http::Uri::default(),
            max_message_bytes: 4 * 1024 * 1024,
            max_concurrent_streams: 100,
            ws_keepalive: None,
            ws_keepalive_timeout: Duration::from_secs(20),
            schema: SchemaSource::None,
            reflection_ttl: Duration::from_secs(4 * 60 * 60),
            admin_reload_path: None,
            allow_implicit_codec: false,
        }
    }
}

// Retry is intentionally NOT implemented in the proxy. This is a protocol-level
// wire proxy, not an application gateway: retries belong in the client (gRPC service
// config), which can back off per-client. A proxy fans many clients into one upstream,
// so proxy-side retry amplifies load exactly when the upstream is failing (retry
// storms) and compounds with client retries. See doc/BACKLOG.md.

#[derive(Clone)]
struct Proxy {
    config: ProxyConfig,
    channel: Channel,
    schema: Schema,
}

/// Serve the proxy on `listener` until the process ends.
pub async fn serve(listener: TcpListener, config: ProxyConfig) -> std::io::Result<()> {
    // Lazy connect: the upstream need not be up when the proxy starts.
    let channel = Channel::builder(config.upstream.clone()).connect_lazy();
    // A bad bundled descriptor set is a config error — surface it at startup.
    let schema = Schema::build(config.schema.clone(), channel.clone(), config.reflection_ttl)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidInput, e.to_string()))?;
    // Kick off the eager reflection load + TTL refresh (no-op for None/Bundled).
    schema.start();
    let proxy = Proxy { config, channel, schema };

    loop {
        let (stream, _peer) = listener.accept().await?;
        let io = TokioIo::new(stream);
        let proxy = proxy.clone();

        tokio::spawn(async move {
            let service = hyper::service::service_fn(move |req| {
                let proxy = proxy.clone();
                async move { proxy.handle(req).await }
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

impl Proxy {
    async fn handle(&self, mut req: Request<Incoming>) -> Result<Response<ResBody>, Infallible> {
        // Management endpoint: force a reflection reload. Matched before all routing so
        // it can't collide with a gRPC method path.
        if let Some(admin) = &self.config.admin_reload_path {
            if req.uri().path() == admin {
                return Ok(self.handle_admin_reload(req.method()).await);
            }
        }

        // WebSocket streaming path: hijack the connection and serve frames.
        if hyper_tungstenite::is_upgrade_request(&req) {
            // Parse the offered subprotocols. The proxy is proto-only; it just needs
            // multiplexing (many streams per socket) vs single-stream (the method is
            // the WS URL). The credential/codec details are the native server's job.
            let subs: Vec<String> = req
                .headers()
                .get(http::header::SEC_WEBSOCKET_PROTOCOL)
                .and_then(|v| v.to_str().ok())
                .map(|s| s.split(',').map(|t| t.trim().to_string()).collect())
                .unwrap_or_default();
            let has = |s: &str| subs.iter().any(|p| p == s);
            let mut multi = has("grpc-webnext+proto+multi") || has("grpc-webnext+json+multi");
            let mut json = has("grpc-webnext+json+multi") || has("grpc-webnext+json");
            // Echo whichever recognized subprotocol was offered (REST clients may offer
            // `application/json`).
            let echo = [
                "grpc-webnext+proto+multi",
                "grpc-webnext+json+multi",
                "grpc-webnext+proto",
                "grpc-webnext+json",
                "grpc-webnext",
                "application/json",
            ]
            .into_iter()
            .find(|&p| has(p));
            // Single-stream: the method is the URL path; multiplexed: from each Subscribe.
            let mut method = (!multi).then(|| req.uri().path().to_string());

            // REST annotation? A matching WS URL binds to a gRPC method; the route is
            // single-stream JSON, method from the binding, requests built from the URL.
            let annotation = if self.schema.enabled() {
                match self.schema.transcoder_any().await {
                    Ok(tc) => tc.match_ws(req.uri().path(), req.uri().query()).map(std::sync::Arc::new),
                    Err(_) => None,
                }
            } else {
                None
            };
            if let Some(ann) = &annotation {
                multi = false;
                json = true;
                method = Some(ann.grpc_method().to_string());
            }

            return Ok(match hyper_tungstenite::upgrade(&mut req, None) {
                Ok((mut response, websocket)) => {
                    if let Some(p) = echo {
                        response.headers_mut().insert(
                            http::header::SEC_WEBSOCKET_PROTOCOL,
                            http::HeaderValue::from_static(p),
                        );
                    }
                    let channel = self.channel.clone();
                    let schema = self.schema.clone();
                    let max_streams = self.config.max_concurrent_streams;
                    let keepalive = self.config.ws_keepalive;
                    let keepalive_timeout = self.config.ws_keepalive_timeout;
                    tokio::spawn(async move {
                        ws::serve(channel, schema, annotation, websocket, max_streams, multi, json, method, keepalive, keepalive_timeout).await
                    });
                    response.map(boxed_full)
                }
                Err(e) => text_response(StatusCode::BAD_REQUEST, format!("upgrade failed: {e}")),
            });
        }

        let content_type = req
            .headers()
            .get(http::header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .unwrap_or("")
            .to_string();

        Ok(if content_type == CT_PROTO {
            self.handle_unary(req).await
        } else if content_type == CT_JSON {
            // SDK `+json`: try a REST annotation binding first, then the main method path.
            self.handle_json_fetch(req, true).await
        } else if content_type.starts_with(CT_GRPC) {
            // Native gRPC: forward to the upstream untouched (same-port passthrough).
            self.passthrough(req).await
        } else if (content_type == "application/json" || content_type.is_empty())
            && self.schema.enabled()
        {
            // Plain REST/JSON: only meaningful with a schema. REST-annotated URLs are
            // transcoded; a main path needs `allow_implicit_codec`.
            self.handle_json_fetch(req, false).await
        } else {
            text_response(
                StatusCode::UNSUPPORTED_MEDIA_TYPE,
                "expected application/grpc-webnext+proto or application/grpc",
            )
        })
    }

    /// Force an immediate reflection reload (management hook). `POST` only.
    async fn handle_admin_reload(&self, method: &http::Method) -> Response<ResBody> {
        if method != http::Method::POST {
            return text_response(StatusCode::METHOD_NOT_ALLOWED, "use POST to force a reflection reload");
        }
        match self.schema.reload().await {
            Ok(()) => text_response(StatusCode::OK, "reflection reloaded"),
            Err(status) => {
                // Nothing-to-reload (None/Bundled) is a client mistake; a failed fetch is
                // an upstream problem.
                let http = if status.code() == Code::FailedPrecondition {
                    StatusCode::CONFLICT
                } else {
                    StatusCode::SERVICE_UNAVAILABLE
                };
                text_response(http, format!("reflection reload failed: {}", status.message()))
            }
        }
    }

    /// Forward a native gRPC request to the upstream unchanged. The channel's
    /// origin layer rewrites the authority to the upstream endpoint.
    async fn passthrough(&self, req: Request<Incoming>) -> Response<ResBody> {
        let (parts, body) = req.into_parts();
        let req = Request::from_parts(parts, TonicBody::new(body));
        match self.channel.clone().oneshot(req).await {
            Ok(resp) => resp.map(|b| b.map_err(Into::into).boxed_unsync()),
            Err(e) => text_response(StatusCode::BAD_GATEWAY, format!("upstream: {e}")),
        }
    }

    /// Unary over Fetch: forward the single request message to the upstream and
    /// **stream** the `[len|message][len|trailer]` response body back. The upstream's
    /// gRPC response frame is `[1-byte flag][u32 len][message]`, so dropping the flag
    /// byte yields our message block verbatim — the proxy pipes it straight through
    /// (opaque, no decode) without buffering the possibly-large message, then appends
    /// the trailer block. The deadline is forwarded downstream (with grace) and enforced
    /// locally: if the upstream doesn't respond in time the call is dropped (cancelling
    /// it upstream) and DEADLINE_EXCEEDED is returned. No retry — see the note above.
    async fn handle_unary(&self, req: Request<Incoming>) -> Response<ResBody> {
        let Some(path) = req.uri().path_and_query().cloned() else {
            return text_response(StatusCode::BAD_REQUEST, "missing method path");
        };
        let deadline = metadata::parse_grpc_timeout(req.headers());
        let (parts, body) = req.into_parts();

        // Stream the length-prefixed request body straight into the upstream gRPC frame —
        // opaque, no buffering. The length comes from the client's prefix.
        let grpc_body = match frame_upstream_request(body, self.config.max_message_bytes).await {
            Ok(b) => b,
            Err(resp) => return resp,
        };

        // Build the upstream gRPC request: forward metadata (minus the hop-by-hop
        // denylist), force content-type, and forward grpc-timeout with grace so the
        // upstream's own enforcement is a backstop to our local timer.
        let mut builder = Request::builder().method(http::Method::POST).uri(path.clone());
        for (name, value) in parts.headers.iter() {
            if !metadata::is_denied(name) {
                builder = builder.header(name.clone(), value.clone());
            }
        }
        builder = builder
            .header(http::header::CONTENT_TYPE, CT_GRPC)
            .header("te", "trailers");
        if let Some(d) = deadline {
            builder = builder.header("grpc-timeout", metadata::format_grpc_timeout(d + DEADLINE_GRACE));
        }
        let grpc_req = builder.body(grpc_body).expect("valid request");

        // Establish the call, bounded by the local deadline. Timing out here drops the
        // future, which cancels the upstream RPC.
        let channel = self.channel.clone();
        let established = match deadline {
            Some(d) => match tokio::time::timeout(d, channel.oneshot(grpc_req)).await {
                Ok(r) => r,
                Err(_) => return fetch_status(Code::DeadlineExceeded, "proxy deadline exceeded"),
            },
            None => channel.oneshot(grpc_req).await,
        };
        let resp = match established {
            Ok(resp) => resp,
            Err(e) => return fetch_status(Code::Unavailable, &format!("upstream: {e}")),
        };
        let (resp_parts, mut resp_body) = resp.into_parts();

        // Initial metadata -> response headers, written before the streamed body.
        let mut out = Response::builder().status(StatusCode::OK).header(http::header::CONTENT_TYPE, CT_PROTO);
        if let Some(headers) = out.headers_mut() {
            metadata::merge_metadata_into_headers(&MetadataMap::from_headers(resp_parts.headers.clone()), headers);
        }

        let resp_headers = resp_parts.headers;
        let deadline_at = deadline.map(|d| tokio::time::Instant::now() + d);
        let stream = async_stream::try_stream! {
            let mut skip = 1usize; // drop the gRPC compression-flag byte
            let mut saw_message = false;
            let mut trailer_headers = HeaderMap::new();
            let mut timed_out = false;
            loop {
                let next = match deadline_at {
                    Some(at) => match tokio::time::timeout_at(at, resp_body.frame()).await {
                        Ok(f) => f,
                        Err(_) => { timed_out = true; break; }
                    },
                    None => resp_body.frame().await,
                };
                let Some(frame) = next else { break };
                let frame = frame.map_err(|e| -> BoxError { format!("upstream body: {e}").into() })?;
                match frame.into_data() {
                    Ok(mut data) => {
                        while skip > 0 && !data.is_empty() {
                            let n = skip.min(data.len());
                            let _ = data.split_to(n);
                            skip -= n;
                        }
                        if !data.is_empty() {
                            saw_message = true;
                            yield BodyFrame::data(data);
                        }
                    }
                    Err(frame) => {
                        if let Ok(t) = frame.into_trailers() {
                            trailer_headers = t;
                        }
                    }
                }
            }
            // A deadline firing mid-message can't be signalled cleanly (the message block
            // already promised a length we won't fulfil) — stop, letting the client see a
            // truncated body. Before any message we can still emit a clean status.
            if timed_out && saw_message {
                return;
            }
            if !saw_message {
                yield BodyFrame::data(Bytes::copy_from_slice(&EMPTY_MESSAGE_BLOCK));
            }
            let (status_code, status_message) = if timed_out {
                (Code::DeadlineExceeded as u32, "proxy deadline exceeded".to_string())
            } else {
                metadata::read_status(&trailer_headers, &resp_headers)
            };
            let trailer = Trailer {
                stream_id: 0,
                status_code,
                status_message,
                trailers: metadata::metadata_to_vec(&MetadataMap::from_headers(trailer_headers)),
            };
            yield BodyFrame::data(encode_trailer_block(&trailer));
        };

        out.body(StreamBody::new(stream).boxed_unsync()).expect("valid response")
    }

    /// Unary over Fetch with the `+json` codec, including REST annotation routing. A
    /// `google.api.http` binding is tried first (the URL is a REST pattern; its
    /// path/query/body build the request message); otherwise the URL is the gRPC method
    /// path — `sdk_json` (the `+json` contract) is always allowed there, while plain JSON
    /// needs `allow_implicit_codec`. Either way JSON<->proto is transcoded around a binary
    /// upstream call, with the native library's response shape.
    async fn handle_json_fetch(&self, req: Request<Incoming>, sdk_json: bool) -> Response<ResBody> {
        let tc = match self.schema.transcoder_any().await {
            Ok(tc) => tc,
            Err(s) => return json_error(s.code(), s.message()),
        };
        let http_method = req.method().clone();
        let Some(pq) = req.uri().path_and_query().cloned() else {
            return json_error(Code::Internal, "missing request path");
        };
        let deadline = metadata::parse_grpc_timeout(req.headers());
        let (parts, body) = req.into_parts();
        let body_bytes = match collect_bounded(body, self.config.max_message_bytes).await {
            Ok(b) => b,
            Err(resp) => return resp,
        };

        // 1) REST annotation binding? Path/query/body build the method's request message.
        match tc.transcode_http_request(http_method.as_str(), pq.path(), pq.query(), &body_bytes) {
            Ok(Some(call)) => {
                let Ok(grpc_path) = call.grpc_method.parse::<PathAndQuery>() else {
                    return json_error(Code::Internal, "bad transcoded method path");
                };
                return self
                    .unary_json_upstream(&tc, grpc_path, call.message.into(), &parts.headers, deadline)
                    .await;
            }
            Ok(None) => {} // not a REST URL — fall through to the main method path
            Err(e) => return json_error(Code::InvalidArgument, &format!("bad REST request: {e}")),
        }

        // 2) Main gRPC method path (the URL is `/pkg.Service/Method`).
        if !sdk_json && !self.config.allow_implicit_codec {
            return json_error(
                Code::Unimplemented,
                "this path requires content-type application/grpc-webnext+json (or set allow_implicit_codec)",
            );
        }
        if !tc.has_method(pq.path()) {
            return json_error(Code::Unimplemented, &format!("no descriptor for method {}", pq.path()));
        }
        let proto = match tc.request_json_to_proto(pq.path(), &body_bytes) {
            Ok(p) => p,
            Err(e) => return json_error(Code::InvalidArgument, &format!("bad json request: {e}")),
        };
        self.unary_json_upstream(&tc, pq, proto.into(), &parts.headers, deadline).await
    }

    /// Call the upstream with an already-encoded binary request message and render the
    /// binary response as native JSON (bare body, status in `grpc-status`/`grpc-message`
    /// headers). Shared by the SDK `+json` and REST annotation paths. JSON necessarily
    /// buffers both messages (the transform is whole-message); the request was bounded by
    /// `max_message_bytes` upstream of here.
    async fn unary_json_upstream(
        &self,
        tc: &Transcoder,
        grpc_path: PathAndQuery,
        message: Bytes,
        req_headers: &HeaderMap,
        deadline: Option<Duration>,
    ) -> Response<ResBody> {
        let mut builder = Request::builder().method(http::Method::POST).uri(grpc_path.clone());
        for (name, value) in req_headers.iter() {
            if !metadata::is_denied(name) {
                builder = builder.header(name.clone(), value.clone());
            }
        }
        builder = builder.header(http::header::CONTENT_TYPE, CT_GRPC).header("te", "trailers");
        if let Some(d) = deadline {
            builder = builder.header("grpc-timeout", metadata::format_grpc_timeout(d + DEADLINE_GRACE));
        }
        let grpc_req =
            builder.body(TonicBody::new(Full::new(grpc_frame(&message)))).expect("valid request");

        // Establish + collect under the local deadline (dropping the future cancels the
        // upstream RPC). JSON must buffer the response to transcode it anyway.
        let channel = self.channel.clone();
        let call = async move {
            let resp = channel.oneshot(grpc_req).await.map_err(|e| format!("upstream: {e}"))?;
            let (parts, body) = resp.into_parts();
            let collected = body.collect().await.map_err(|e| format!("upstream body: {e}"))?;
            Ok::<_, String>((parts, collected))
        };
        let (resp_parts, collected) = match deadline {
            Some(d) => match tokio::time::timeout(d, call).await {
                Ok(Ok(v)) => v,
                Ok(Err(e)) => return json_error(Code::Unavailable, &e),
                Err(_) => return json_error(Code::DeadlineExceeded, "proxy deadline exceeded"),
            },
            None => match call.await {
                Ok(v) => v,
                Err(e) => return json_error(Code::Unavailable, &e),
            },
        };

        let trailer_headers = collected.trailers().cloned().unwrap_or_default();
        let body_bytes = collected.to_bytes();
        let out_message = deframe_all(&body_bytes).into_iter().next().unwrap_or_default();
        let (status_code, status_message) =
            metadata::read_status(&trailer_headers, &resp_parts.headers);

        let json_body = if status_code == 0 && !out_message.is_empty() {
            match tc.response_proto_to_json(grpc_path.path(), &out_message) {
                Ok(j) => Bytes::from(j),
                Err(e) => return json_error(Code::Internal, &format!("bad json response: {e}")),
            }
        } else {
            Bytes::new()
        };
        json_fetch_response(
            json_body,
            status_code,
            &status_message,
            &resp_parts.headers,
            &trailer_headers,
        )
    }
}

/// Buffer a request body into memory, bounded by `max`. Used by the `+json` path,
/// which must hold the whole message to transcode it (binary `+proto` streams instead).
async fn collect_bounded(mut body: Incoming, max: usize) -> Result<Bytes, Response<ResBody>> {
    let mut buf = bytes::BytesMut::new();
    while let Some(frame) = body.frame().await {
        let frame = frame.map_err(|e| json_error(Code::Internal, &format!("read body: {e}")))?;
        if let Ok(data) = frame.into_data() {
            if buf.len() + data.len() > max {
                return Err(json_error(Code::ResourceExhausted, "request message exceeds size limit"));
            }
            buf.extend_from_slice(&data);
        }
    }
    Ok(buf.freeze())
}

/// Render a native-JSON Fetch response: bare JSON message body (empty on error) with
/// the status in `grpc-status`/`grpc-message` and metadata in headers. Mirrors the
/// native server's `json_fetch_response` so the two surfaces are indistinguishable.
fn json_fetch_response(
    message: Bytes,
    status_code: u32,
    status_message: &str,
    initial: &HeaderMap,
    trailing: &HeaderMap,
) -> Response<ResBody> {
    let mut builder = Response::builder()
        .status(StatusCode::OK)
        .header(http::header::CONTENT_TYPE, CT_JSON)
        .header("grpc-status", status_code.to_string());
    if !status_message.is_empty() {
        if let Ok(v) = http::HeaderValue::from_str(&metadata::percent_encode(status_message)) {
            builder = builder.header("grpc-message", v);
        }
    }
    if let Some(headers) = builder.headers_mut() {
        let md = |h: &HeaderMap| MetadataMap::from_headers(h.clone());
        metadata::merge_metadata_into_headers(&md(initial), headers);
        metadata::merge_metadata_into_headers(&md(trailing), headers);
    }
    // On error there is no message body.
    let body = if status_code == 0 { message } else { Bytes::new() };
    builder.body(boxed_full(Full::new(body))).expect("valid response")
}

/// A `+json` Fetch error response (empty body, status in headers).
fn json_error(code: Code, message: &str) -> Response<ResBody> {
    json_fetch_response(Bytes::new(), code as u32, message, &HeaderMap::new(), &HeaderMap::new())
}

/// Turn a length-prefixed `+proto` Fetch request body (`[u32 len | message]`) into a
/// streaming gRPC request body: peek the length prefix to enforce the size limit, then
/// emit the `[1-byte flag]` + the client's block verbatim. A large upload is piped
/// straight upstream without being buffered to measure — the length comes from the
/// client's prefix.
async fn frame_upstream_request(
    mut body: Incoming,
    max: usize,
) -> Result<TonicBody, Response<ResBody>> {
    // Peek the 4-byte length prefix (it may span read chunks) — one chunk, not the
    // whole message.
    let mut lead = bytes::BytesMut::new();
    while lead.len() < LEN_PREFIX {
        match body.frame().await {
            Some(Ok(frame)) => {
                if let Ok(data) = frame.into_data() {
                    lead.extend_from_slice(&data);
                }
            }
            Some(Err(e)) => return Err(text_response(StatusCode::BAD_REQUEST, format!("read body: {e}"))),
            None => break,
        }
    }
    if lead.len() < LEN_PREFIX {
        return Err(text_response(StatusCode::BAD_REQUEST, "request body missing length prefix"));
    }
    let declared = u32::from_be_bytes([lead[0], lead[1], lead[2], lead[3]]) as usize;
    if declared > max {
        return Err(text_response(StatusCode::PAYLOAD_TOO_LARGE, "request message exceeds size limit"));
    }
    let lead = lead.freeze();
    let stream = async_stream::try_stream! {
        yield BodyFrame::data(Bytes::from_static(&[0u8])); // gRPC compression flag (uncompressed)
        yield BodyFrame::data(lead);                        // [u32 len | leading message bytes]
        while let Some(frame) = body.frame().await {
            let frame = frame.map_err(|e| -> BoxError { format!("read body: {e}").into() })?;
            if let Ok(data) = frame.into_data() {
                yield BodyFrame::data(data);
            }
        }
    };
    // Annotate the boxed body to pin the stream's error type to BoxError.
    let body: UnsyncBoxBody<Bytes, BoxError> = StreamBody::new(stream).boxed_unsync();
    Ok(TonicBody::new(body))
}

/// A buffered Fetch response carrying only a status (empty message block + trailer),
/// for failures that happen before any response body is available.
fn fetch_status(code: Code, message: &str) -> Response<ResBody> {
    let trailer = Trailer {
        stream_id: 0,
        status_code: code as u32,
        status_message: message.to_string(),
        trailers: Vec::new(),
    };
    Response::builder()
        .status(StatusCode::OK)
        .header(http::header::CONTENT_TYPE, CT_PROTO)
        .body(boxed_full(Full::new(encode_response_body(&[], &trailer))))
        .expect("valid response")
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

/// Bind an ephemeral local address and serve; returns the bound address and a
/// handle. Convenience for tests and simple mains.
pub async fn bind_and_serve(
    config: ProxyConfig,
) -> std::io::Result<(SocketAddr, tokio::task::JoinHandle<std::io::Result<()>>)> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let handle = tokio::spawn(serve(listener, config));
    Ok((addr, handle))
}
