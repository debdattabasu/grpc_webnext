//! grpc-webnext proxy library.
//!
//! On one port, in front of any upstream gRPC server:
//!   * native `application/grpc` — forwarded to the upstream untouched (README #9),
//!   * grpc-webnext unary over Fetch — translated into a native gRPC call,
//!   * grpc-webnext streaming over WebSocket — translated per stream.
//!
//! Binary-only for grpc-webnext (`+json` -> UNIMPLEMENTED). Deadlines are
//! enforced locally (the upstream call is dropped on expiry) *and* forwarded
//! downstream as `grpc-timeout`; client cancellation (Reset / disconnect)
//! propagates to the upstream by dropping the call.

use std::convert::Infallible;
use std::net::SocketAddr;
use std::time::Duration;

use bytes::Bytes;
use grpc_webnext_core::pb::Trailer;
use grpc_webnext_core::{encode_response_body, BytesCodec};
use http::{Request, Response, StatusCode};
use http_body_util::combinators::UnsyncBoxBody;
use http_body_util::{BodyExt, Full};
use hyper::body::Incoming;
use hyper_util::rt::{TokioExecutor, TokioIo};
use tokio::net::TcpListener;
use tonic::body::Body as TonicBody;
use tonic::metadata::MetadataMap;
use tonic::transport::Channel;
use tonic::Status;
use tower::ServiceExt;

pub mod metadata;
pub mod ws;

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
    /// Max bytes to buffer for a single request/response message.
    pub max_message_bytes: usize,
    /// Retry policy for unary calls to the upstream.
    pub retry: RetryPolicy,
    /// Max concurrent logical streams per WebSocket connection.
    pub max_concurrent_streams: usize,
}

impl Default for ProxyConfig {
    fn default() -> Self {
        Self {
            upstream: http::Uri::default(),
            max_message_bytes: 4 * 1024 * 1024,
            retry: RetryPolicy::default(),
            max_concurrent_streams: 100,
        }
    }
}

/// gRPC-style retry policy applied to unary upstream calls. Retries are bounded
/// by `max_attempts`, only fire for `retryable_codes`, use exponential backoff
/// with full jitter, and never outlive the call deadline.
#[derive(Clone)]
pub struct RetryPolicy {
    /// Total attempts including the first. `1` disables retry.
    pub max_attempts: u32,
    pub initial_backoff: Duration,
    pub max_backoff: Duration,
    pub backoff_multiplier: f64,
    pub retryable_codes: Vec<tonic::Code>,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_attempts: 1, // retry off by default (opt-in, like gRPC service config)
            initial_backoff: Duration::from_millis(50),
            max_backoff: Duration::from_secs(1),
            backoff_multiplier: 2.0,
            retryable_codes: vec![tonic::Code::Unavailable],
        }
    }
}

#[derive(Clone)]
struct Proxy {
    config: ProxyConfig,
    channel: Channel,
}

/// Serve the proxy on `listener` until the process ends.
pub async fn serve(listener: TcpListener, config: ProxyConfig) -> std::io::Result<()> {
    // Lazy connect: the upstream need not be up when the proxy starts.
    let channel = Channel::builder(config.upstream.clone()).connect_lazy();
    let proxy = Proxy { config, channel };

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
        // WebSocket streaming path: hijack the connection and serve frames.
        if hyper_tungstenite::is_upgrade_request(&req) {
            return Ok(match hyper_tungstenite::upgrade(&mut req, None) {
                Ok((response, websocket)) => {
                    let channel = self.channel.clone();
                    let max_streams = self.config.max_concurrent_streams;
                    tokio::spawn(async move { ws::serve(channel, websocket, max_streams).await });
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
            text_response(
                StatusCode::NOT_IMPLEMENTED,
                "proxy is binary-only; +json is served by the native library",
            )
        } else if content_type.starts_with(CT_GRPC) {
            // Native gRPC: forward to the upstream untouched (same-port passthrough).
            self.passthrough(req).await
        } else {
            text_response(
                StatusCode::UNSUPPORTED_MEDIA_TYPE,
                "expected application/grpc-webnext+proto or application/grpc",
            )
        })
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

    /// Unary over Fetch: forward the single request message and write the
    /// `[len|message][len|trailer]` response body. The deadline is forwarded
    /// downstream and enforced locally (the call is dropped on expiry).
    async fn handle_unary(&self, req: Request<Incoming>) -> Response<ResBody> {
        let Some(path) = req.uri().path_and_query().cloned() else {
            return text_response(StatusCode::BAD_REQUEST, "missing method path");
        };

        let metadata = metadata::request_headers_to_metadata(req.headers());
        let deadline = metadata::parse_grpc_timeout(req.headers());

        let body = match req.into_body().collect().await {
            Ok(c) => c.to_bytes(),
            Err(e) => return text_response(StatusCode::BAD_REQUEST, format!("read body: {e}")),
        };
        if body.len() > self.config.max_message_bytes {
            return text_response(StatusCode::PAYLOAD_TOO_LARGE, "request message exceeds size limit");
        }

        // Local deadline enforcement wraps all retry attempts; on expiry the
        // in-flight call future is dropped, cancelling the upstream RPC.
        let forward_deadline = deadline.map(|d| d + DEADLINE_GRACE);
        let call = self.unary_with_retry(&path, &metadata, &body, forward_deadline);
        let response = match deadline {
            Some(d) => match tokio::time::timeout(d, call).await {
                Ok(r) => r,
                Err(_) => Err(Status::deadline_exceeded("proxy deadline exceeded")),
            },
            None => call.await,
        };

        let (message, trailer, response_metadata) = match response {
            Ok(resp) => {
                let meta = resp.metadata().clone();
                let msg = resp.into_inner();
                let trailer = Trailer {
                    stream_id: 0,
                    status_code: 0,
                    status_message: String::new(),
                    trailers: Vec::new(),
                };
                (msg, trailer, meta)
            }
            Err(status) => {
                let trailer = Trailer {
                    stream_id: 0,
                    status_code: status.code() as u32,
                    status_message: status.message().to_string(),
                    trailers: metadata::metadata_to_vec(status.metadata()),
                };
                (Bytes::new(), trailer, MetadataMap::new())
            }
        };

        let framed = encode_response_body(&message, &trailer);
        let mut builder = Response::builder()
            .status(StatusCode::OK)
            .header(http::header::CONTENT_TYPE, CT_PROTO);
        if let Some(headers) = builder.headers_mut() {
            metadata::merge_metadata_into_headers(&response_metadata, headers);
        }
        builder.body(boxed_full(Full::new(framed))).expect("valid response")
    }

    /// Forward a unary call to the upstream, retrying per the configured policy.
    /// Each attempt rebuilds the request; retries stop at `max_attempts`, on a
    /// non-retryable code, or on success. The caller's deadline bounds the whole
    /// loop (including backoff sleeps).
    async fn unary_with_retry(
        &self,
        path: &http::uri::PathAndQuery,
        metadata: &MetadataMap,
        body: &Bytes,
        forward_deadline: Option<std::time::Duration>,
    ) -> Result<tonic::Response<Bytes>, Status> {
        let policy = &self.config.retry;
        let mut backoff = policy.initial_backoff;
        let mut attempt = 0;

        loop {
            attempt += 1;

            let mut request =
                tonic::Request::from_parts(metadata.clone(), Default::default(), body.clone());
            if let Some(d) = forward_deadline {
                request.set_timeout(d);
            }

            let mut client = tonic::client::Grpc::new(self.channel.clone());
            let result = match client.ready().await {
                Ok(()) => client.unary::<Bytes, Bytes, _>(request, path.clone(), BytesCodec).await,
                Err(e) => Err(Status::unavailable(format!("upstream unready: {e}"))),
            };

            let status = match result {
                Ok(resp) => return Ok(resp),
                Err(status) => status,
            };

            let can_retry =
                attempt < policy.max_attempts && policy.retryable_codes.contains(&status.code());
            if !can_retry {
                return Err(status);
            }

            // Exponential backoff with full jitter: sleep in [0, backoff).
            let jittered = backoff.mul_f64(fastrand::f64());
            tokio::time::sleep(jittered).await;
            backoff = backoff
                .mul_f64(policy.backoff_multiplier)
                .min(policy.max_backoff);
        }
    }
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
