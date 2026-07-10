//! Binary path over **real HTTP/2**, tunneled through a WebSocket by
//! [h2ts](https://github.com/debdattabasu/h2ts).
//!
//! When a WebSocket handshake offers the `h2ts` subprotocol the client is speaking real
//! gRPC (HTTP/2 with trailers) over the tunnel — not the custom `Frame` protocol. So there
//! is nothing to translate:
//!
//! * **in-process** — hand the tunnel straight to the tonic [`Routes`](tonic::service::Routes)
//!   via [`serve_h2`](h2ts_server::serve_h2); tonic serves real gRPC over it.
//! * **proxy** — [`bridge`](h2ts_server::bridge) the tunnel's bytes to the h2c upstream;
//!   the proxy never parses gRPC (schema-agnostic, incremental — wslay streams sub-frame).
//!
//! This is the `{ encoding: proto, unary: h2ts, streaming: h2ts }` default. The `+json`
//! and Fetch/custom-`Frame` paths are untouched and live elsewhere.

use std::convert::Infallible;

use bytes::Bytes;
use http::{Request, Response};
use http_body_util::{BodyExt, Empty};
use hyper::body::Incoming;
use tokio::net::TcpStream;
use tonic::body::Body as TonicBody;
use tower::ServiceExt as _;

use crate::{Backend, BoxError, ResBody, Runtime};

/// Complete the h2ts handshake and, once upgraded, serve real gRPC over the tunnel
/// (in-process) or byte-pump it to the upstream (proxy). Returns the `101` immediately;
/// the tunnel runs on a spawned task. Only called when the client offered `h2ts`.
pub(crate) fn serve(rt: &Runtime, req: &mut Request<Incoming>) -> Response<ResBody> {
    let (response, ws_fut) = match h2ts_server::accept(req) {
        Ok(pair) => pair,
        Err(e) => return e.rejection_response().map(box_empty),
    };

    let rt = rt.clone();
    tokio::spawn(async move {
        let ws = match ws_fut.await {
            Ok(ws) => ws,
            Err(e) => {
                tracing::debug!("h2ts upgrade failed: {e}");
                return;
            }
        };
        match &rt.backend {
            // Real gRPC, served directly by the in-process router — no translation.
            Backend::InProcess(routes) => {
                let routes = routes.clone();
                let service = hyper::service::service_fn(move |req: Request<Incoming>| {
                    let routes = routes.clone();
                    async move { routes.oneshot(req.map(TonicBody::new)).await }
                });
                if let Err(e) = h2ts_server::serve_h2(ws, service).await {
                    tracing::debug!("h2ts serve_h2 ended: {e}");
                }
            }
            // Byte-transparent tunnel to the h2c upstream — no gRPC parsing.
            Backend::Upstream(_) => {
                let Some(authority) = rt.cfg.upstream_authority.clone() else {
                    tracing::debug!("h2ts proxy: no upstream authority configured");
                    return;
                };
                match TcpStream::connect(&authority).await {
                    Ok(tcp) => {
                        let _ = h2ts_server::bridge(ws, tcp).await;
                    }
                    Err(e) => tracing::debug!("h2ts proxy connect {authority} failed: {e}"),
                }
            }
        }
    });

    response.map(box_empty)
}

/// The `101` (and any rejection) response body is `Empty`; box it into the crate's `ResBody`.
fn box_empty(body: Empty<Bytes>) -> ResBody {
    body.map_err(|e: Infallible| -> BoxError { match e {} }).boxed_unsync()
}
