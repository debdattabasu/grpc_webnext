//! grpc-webnext proxy library.
//!
//! Terminates grpc-webnext (Fetch unary now; WebSocket streaming next) and
//! forwards `+proto` payloads opaquely to an upstream gRPC server via
//! [`grpc_webnext_core::BytesCodec`]. `+json` is answered with UNIMPLEMENTED.

use std::convert::Infallible;
use std::net::SocketAddr;

use bytes::Bytes;
use grpc_webnext_core::pb::Trailer;
use grpc_webnext_core::{encode_response_body, BytesCodec};
use http::{Request, Response, StatusCode};
use http_body_util::{BodyExt, Full};
use hyper::body::Incoming;
use hyper_util::rt::{TokioExecutor, TokioIo};
use tokio::net::TcpListener;
use tonic::metadata::MetadataMap;
use tonic::transport::Channel;

pub mod metadata;
pub mod ws;

pub const CT_PROTO: &str = "application/grpc-webnext+proto";
pub const CT_JSON: &str = "application/grpc-webnext+json";

#[derive(Clone)]
pub struct ProxyConfig {
    /// Upstream gRPC endpoint (e.g. `http://127.0.0.1:50051`).
    pub upstream: http::Uri,
    /// Max bytes to buffer for a single request/response message.
    pub max_message_bytes: usize,
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
            // serve_connection_with_upgrades so the WebSocket path can hijack later.
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
    async fn handle(&self, mut req: Request<Incoming>) -> Result<Response<Full<Bytes>>, Infallible> {
        // WebSocket streaming path: hijack the connection and serve frames.
        if hyper_tungstenite::is_upgrade_request(&req) {
            match hyper_tungstenite::upgrade(&mut req, None) {
                Ok((response, websocket)) => {
                    let channel = self.channel.clone();
                    tokio::spawn(async move { ws::serve(channel, websocket).await });
                    return Ok(response);
                }
                Err(e) => {
                    return Ok(HttpError::status(
                        StatusCode::BAD_REQUEST,
                        format!("websocket upgrade failed: {e}"),
                    )
                    .into_response());
                }
            }
        }

        let content_type = req
            .headers()
            .get(http::header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .unwrap_or("")
            .to_string();

        let result = match content_type.as_str() {
            CT_PROTO => self.handle_unary(req).await,
            CT_JSON => Err(HttpError::status(
                StatusCode::NOT_IMPLEMENTED,
                "proxy is binary-only; +json is served by the native library",
            )),
            _ => Err(HttpError::status(
                StatusCode::UNSUPPORTED_MEDIA_TYPE,
                "expected content-type application/grpc-webnext+proto",
            )),
        };

        Ok(result.unwrap_or_else(|e| e.into_response()))
    }

    /// Unary over Fetch: forward the single request message and write the
    /// `[len|message][len|trailer]` response body.
    async fn handle_unary(&self, req: Request<Incoming>) -> Result<Response<Full<Bytes>>, HttpError> {
        let path = req
            .uri()
            .path_and_query()
            .cloned()
            .ok_or_else(|| HttpError::status(StatusCode::BAD_REQUEST, "missing method path"))?;

        // Request metadata + optional deadline from grpc-timeout.
        let metadata = metadata::request_headers_to_metadata(req.headers());
        let timeout = metadata::parse_grpc_timeout(req.headers());

        // Buffer the request message (bounded).
        let body = req
            .into_body()
            .collect()
            .await
            .map_err(|e| HttpError::status(StatusCode::BAD_REQUEST, format!("read body: {e}")))?
            .to_bytes();
        if body.len() > self.config.max_message_bytes {
            return Err(HttpError::status(
                StatusCode::PAYLOAD_TOO_LARGE,
                "request message exceeds size limit",
            ));
        }

        let mut grpc_request = tonic::Request::from_parts(metadata, Default::default(), body);
        if let Some(d) = timeout {
            grpc_request.set_timeout(d);
        }

        // Forward opaquely to the upstream.
        let mut client = tonic::client::Grpc::new(self.channel.clone());
        client
            .ready()
            .await
            .map_err(|e| HttpError::status(StatusCode::BAD_GATEWAY, format!("upstream unready: {e}")))?;

        let response = client.unary::<Bytes, Bytes, _>(grpc_request, path, BytesCodec).await;

        // Map success or gRPC error to a grpc-webnext framed response (always HTTP 200).
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

        let body = encode_response_body(&message, &trailer);
        let mut builder = Response::builder()
            .status(StatusCode::OK)
            .header(http::header::CONTENT_TYPE, CT_PROTO);

        // Initial response metadata -> HTTP response headers.
        if let Some(headers) = builder.headers_mut() {
            metadata::merge_metadata_into_headers(&response_metadata, headers);
        }

        builder
            .body(Full::new(body))
            .map_err(|e| HttpError::status(StatusCode::INTERNAL_SERVER_ERROR, format!("build response: {e}")))
    }
}

/// A simple non-gRPC HTTP error (used only for transport-level failures like a
/// missing/incorrect content-type, before we're in gRPC-status land).
struct HttpError {
    status: StatusCode,
    message: String,
}

impl HttpError {
    fn status(status: StatusCode, message: impl Into<String>) -> Self {
        Self { status, message: message.into() }
    }
    fn into_response(self) -> Response<Full<Bytes>> {
        Response::builder()
            .status(self.status)
            .body(Full::new(Bytes::from(self.message)))
            .unwrap()
    }
}

/// Bind an ephemeral local address and serve; returns the bound address and a
/// handle. Convenience for tests and simple mains.
pub async fn bind_and_serve(config: ProxyConfig) -> std::io::Result<(SocketAddr, tokio::task::JoinHandle<std::io::Result<()>>)> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let handle = tokio::spawn(serve(listener, config));
    Ok((addr, handle))
}
