//! grpc-webnext: serve the grpc-webnext wire protocol (unary over Fetch, streaming over
//! WebSocket, plus `+json` / REST transcoding) over any gRPC service.
//!
//! All the inbound protocol translation is shared; the only thing that varies is where the
//! translated gRPC call lands — a local in-process [`tonic::service::Routes`] you own
//! ([`serve_in_process`], the "native server" / wrap mode) or a remote upstream over an
//! HTTP/2 channel ([`serve_proxy`], the standalone binary proxy). Both are the same
//! [`Backend`]; the two entry points build one and run the identical handlers, so a client
//! can't tell a wrapped response from a proxied one.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use http::HeaderMap;
use http_body_util::combinators::UnsyncBoxBody;
use hyper::body::Incoming;
use hyper::Request;
use hyper_util::rt::{TokioExecutor, TokioIo};
use tokio::net::TcpListener;
use tonic::metadata::MetadataMap;
use tonic::service::Routes;
use tonic::transport::Channel;
use tonic::Status;

// Inbound protocol translation.
pub mod backend;
mod fetch;
mod reflect;
pub mod schema;
mod ws;

// Wire codec + gRPC-semantics core: generated types, frame codec, Fetch-response framing,
// metadata, HTTP-rule transcoding.
pub mod pb {
    include!(concat!(env!("OUT_DIR"), "/grpc.webnext.v1.rs"));
}
pub mod codec;
pub mod frame;
mod framing;
pub mod grpc_framing;
pub mod httprule;
pub mod json_frame;
pub mod metadata;
pub mod transcode;

pub use backend::Backend;
pub use schema::{Schema, SchemaSource};

pub use codec::BytesCodec;
pub use frame::{decode_frame, encode_frame, FrameError};
pub use framing::{
    decode_response_body, encode_request_body, encode_response_body, encode_trailer_block,
    FetchError, EMPTY_MESSAGE_BLOCK, LEN_PREFIX,
};
pub use grpc_framing::{deframe_all, frame as grpc_frame, Deframer};
pub use httprule::{HttpCall, HttpRouter, WsBinding};
pub use transcode::{TranscodeError, Transcoder};

// --- Wire constants ---------------------------------------------------------

pub const CT_PROTO: &str = "application/grpc-webnext+proto";
pub const CT_JSON: &str = "application/grpc-webnext+json";
pub(crate) const CT_GRPC: &str = "application/grpc";

/// Base subprotocol; a client offers it plus a codec/credential entry.
pub const WS_SUBPROTOCOL: &str = "grpc-webnext";
pub const WS_SUBPROTOCOL_JSON: &str = "grpc-webnext+json";
pub const WS_SUBPROTOCOL_PROTO: &str = "grpc-webnext+proto";
pub const WS_SUBPROTOCOL_JSON_MULTI: &str = "grpc-webnext+json+multi";
pub const WS_SUBPROTOCOL_PROTO_MULTI: &str = "grpc-webnext+proto+multi";

/// The proxy owns the client-facing deadline: it drops the call at the deadline
/// (surfacing DEADLINE_EXCEEDED) and forwards `grpc-timeout` downstream with this grace so
/// the callee's own enforcement is a later backstop rather than racing the local timer.
pub const DEADLINE_GRACE: Duration = Duration::from_millis(500);

pub type BoxError = Box<dyn std::error::Error + Send + Sync>;
pub type ResBody = UnsyncBoxBody<Bytes, BoxError>;

// --- Auth hooks (in-process surface) ----------------------------------------

/// Authorize a WebSocket connection at handshake time (see the connection-gate docs in
/// `doc/PROTOCOL.md`). Given the gRPC method the credential is scoped to and the request
/// headers; `Err(status)` closes the accepted socket with a `4000 + code` close frame.
pub type ConnectAuthFn = Arc<dyn Fn(&str, &HeaderMap) -> Result<(), Status> + Send + Sync>;

/// Authorize a single stream from its method path and request metadata. `Err(status)`
/// answers that call with the status (a WS `Reset` / a Fetch `grpc-status`). The
/// authoritative, gRPC-faithful check, run on every grpc-webnext stream on both transports.
pub type StreamAuthFn = Arc<dyn Fn(&str, &MetadataMap) -> Result<(), Status> + Send + Sync>;

// --- Public configs ---------------------------------------------------------

/// Configuration for [`serve_in_process`] — wrap an in-process tonic service.
#[derive(Clone)]
pub struct ServerConfig {
    pub max_message_bytes: usize,
    /// Descriptor-based JSON<->proto transcoder. When set, `+json`/REST requests are
    /// transcoded to the router's binary protobuf and back. `None` ⇒ `+json` is
    /// `UNIMPLEMENTED`.
    pub transcoder: Option<Arc<Transcoder>>,
    /// Optional connection-level WebSocket handshake gate.
    pub connect_auth: Option<ConnectAuthFn>,
    /// Optional per-stream authorization (every grpc-webnext stream, both transports).
    pub stream_auth: Option<StreamAuthFn>,
    /// Allow plain `application/json` / blank content-type to reach *main* gRPC paths
    /// (and blank WS subprotocols to infer their codec). Off by default.
    pub allow_implicit_codec: bool,
    /// WebSocket keepalive ping interval (`None` disables).
    pub ws_keepalive: Option<Duration>,
    /// Dead-peer timeout after a keepalive ping (gRPC's `keepalive_timeout`, default 20s).
    pub ws_keepalive_timeout: Duration,
    /// Max concurrent logical streams per WebSocket connection. Defaults to `usize::MAX`
    /// (no cap — a wrapped service serves one client per connection).
    pub max_concurrent_streams: usize,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            max_message_bytes: 4 * 1024 * 1024,
            transcoder: None,
            connect_auth: None,
            stream_auth: None,
            allow_implicit_codec: false,
            ws_keepalive: None,
            ws_keepalive_timeout: Duration::from_secs(20),
            max_concurrent_streams: usize::MAX,
        }
    }
}

/// Configuration for [`serve_proxy`] — front a remote upstream gRPC server.
#[derive(Clone)]
pub struct ProxyConfig {
    /// Upstream gRPC endpoint (e.g. `http://127.0.0.1:50051`).
    pub upstream: http::Uri,
    pub max_message_bytes: usize,
    /// Max concurrent logical streams per WebSocket connection (default 100).
    pub max_concurrent_streams: usize,
    pub ws_keepalive: Option<Duration>,
    pub ws_keepalive_timeout: Duration,
    /// Descriptor source for `+json` termination (`None` ⇒ binary-only).
    pub schema: SchemaSource,
    /// Reflection snapshot refresh interval (default 4h).
    pub reflection_ttl: Duration,
    /// Optional management endpoint: `POST` to this exact path forces a reflection reload.
    pub admin_reload_path: Option<String>,
    /// Accept plain `application/json` / blank on *main* gRPC paths (off by default).
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

// --- Internal runtime -------------------------------------------------------

/// The per-connection knobs the shared handlers read, independent of which surface built
/// them.
pub(crate) struct RunConfig {
    pub max_message_bytes: usize,
    pub max_concurrent_streams: usize,
    pub ws_keepalive: Option<Duration>,
    pub ws_keepalive_timeout: Duration,
    pub allow_implicit_codec: bool,
    pub connect_auth: Option<ConnectAuthFn>,
    pub stream_auth: Option<StreamAuthFn>,
    pub admin_reload_path: Option<String>,
}

/// Everything a connection needs: where to dispatch, how to transcode, and the policy.
/// Cheap to clone.
#[derive(Clone)]
pub(crate) struct Runtime {
    pub backend: Backend,
    pub schema: Schema,
    pub cfg: Arc<RunConfig>,
}

// --- Entry points -----------------------------------------------------------

/// Serve grpc-webnext + native gRPC for an in-process `routes` on `listener`.
pub async fn serve_in_process(
    listener: TcpListener,
    routes: Routes,
    config: ServerConfig,
) -> std::io::Result<()> {
    let schema = Schema::from_transcoder(config.transcoder.clone());
    let cfg = Arc::new(RunConfig {
        max_message_bytes: config.max_message_bytes,
        max_concurrent_streams: config.max_concurrent_streams,
        ws_keepalive: config.ws_keepalive,
        ws_keepalive_timeout: config.ws_keepalive_timeout,
        allow_implicit_codec: config.allow_implicit_codec,
        connect_auth: config.connect_auth,
        stream_auth: config.stream_auth,
        admin_reload_path: None,
    });
    run(listener, Runtime { backend: Backend::InProcess(routes), schema, cfg }).await
}

/// Serve grpc-webnext + native gRPC passthrough in front of an upstream gRPC server.
pub async fn serve_proxy(listener: TcpListener, config: ProxyConfig) -> std::io::Result<()> {
    // Lazy connect: the upstream need not be up when we start.
    let channel = Channel::builder(config.upstream.clone()).connect_lazy();
    // A bad bundled descriptor set is a config error — surface it at startup.
    let schema = Schema::build(config.schema.clone(), channel.clone(), config.reflection_ttl)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidInput, e.to_string()))?;
    // Eager reflection load + TTL refresh (no-op for None/Bundled).
    schema.start();
    let cfg = Arc::new(RunConfig {
        max_message_bytes: config.max_message_bytes,
        max_concurrent_streams: config.max_concurrent_streams,
        ws_keepalive: config.ws_keepalive,
        ws_keepalive_timeout: config.ws_keepalive_timeout,
        allow_implicit_codec: config.allow_implicit_codec,
        connect_auth: None,
        stream_auth: None,
        admin_reload_path: config.admin_reload_path,
    });
    run(listener, Runtime { backend: Backend::Upstream(channel), schema, cfg }).await
}

/// The connection accept loop, shared by both surfaces.
async fn run(listener: TcpListener, rt: Runtime) -> std::io::Result<()> {
    loop {
        let (stream, _peer) = listener.accept().await?;
        let io = TokioIo::new(stream);
        let rt = rt.clone();
        tokio::spawn(async move {
            let service = hyper::service::service_fn(move |req: Request<Incoming>| {
                let rt = rt.clone();
                async move { fetch::handle(&rt, req).await }
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

/// Bind an ephemeral local address, serve an in-process `routes`, and return the bound
/// address + task handle. For tests and simple mains.
pub async fn bind_and_serve_in_process(
    routes: Routes,
    config: ServerConfig,
) -> std::io::Result<(SocketAddr, tokio::task::JoinHandle<std::io::Result<()>>)> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let handle = tokio::spawn(serve_in_process(listener, routes, config));
    Ok((addr, handle))
}

/// Bind an ephemeral local address, serve a proxy, and return the bound address + handle.
pub async fn bind_and_serve_proxy(
    config: ProxyConfig,
) -> std::io::Result<(SocketAddr, tokio::task::JoinHandle<std::io::Result<()>>)> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let handle = tokio::spawn(serve_proxy(listener, config));
    Ok((addr, handle))
}

// --- WebSocket handshake helpers (public: used inside a ConnectAuthFn) -------

/// Parse the `Sec-WebSocket-Protocol` request header into its comma-separated tokens.
pub fn ws_subprotocols(headers: &HeaderMap) -> Vec<String> {
    headers
        .get(http::header::SEC_WEBSOCKET_PROTOCOL)
        .and_then(|v| v.to_str().ok())
        .map(|s| s.split(',').map(|t| t.trim().to_string()).filter(|t| !t.is_empty()).collect())
        .unwrap_or_default()
}

/// Extract a `bearer.<token>` credential a client placed in the WebSocket subprotocol list.
pub fn ws_bearer_token(headers: &HeaderMap) -> Option<String> {
    ws_subprotocols(headers)
        .into_iter()
        .find_map(|p| p.strip_prefix("bearer.").map(|t| t.to_string()))
}

/// Read a single query parameter's (percent-decoded) value.
pub(crate) fn query_param(query: Option<&str>, key: &str) -> Option<String> {
    query?.split('&').find_map(|kv| {
        let (k, v) = kv.split_once('=')?;
        (k == key).then(|| crate::metadata::percent_decode(v))
    })
}
