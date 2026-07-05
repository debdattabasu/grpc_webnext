//! The gRPC dispatch target.
//!
//! Everything else in this crate is inbound protocol translation (Fetch + WebSocket ⇄
//! gRPC); the only thing that varies between "wrap an in-process service" and "proxy an
//! upstream" is where the translated gRPC call lands. Both destinations are the same
//! interface — `tower::Service<http::Request, Response = http::Response>` reduced to
//! `.oneshot(req)` — so the difference is one enum.

use http::{Request, Response};
use http_body_util::BodyExt;
use tonic::body::Body as TonicBody;
use tonic::service::Routes;
use tonic::transport::Channel;
use tower::ServiceExt;

use crate::{BoxError, ResBody};

/// Where a translated gRPC request is dispatched.
#[derive(Clone)]
pub enum Backend {
    /// In-process: call the local tonic `Routes` (wrap a service you own).
    InProcess(Routes),
    /// Upstream: forward to a remote gRPC server over an HTTP/2 `Channel` (proxy mode).
    Upstream(Channel),
}

impl Backend {
    /// Dispatch a gRPC-framed HTTP request and return the HTTP response. The two backends'
    /// response bodies differ in type; both are boxed into one `ResBody` here, so every
    /// caller downstream is monomorphic. `InProcess` is infallible (its `Service::Error`
    /// is `Infallible`); `Upstream` surfaces the transport error for the caller to map to a
    /// gRPC status (`UNAVAILABLE` / `BAD_GATEWAY`).
    pub async fn call(&self, req: Request<TonicBody>) -> Result<Response<ResBody>, BoxError> {
        match self {
            Backend::InProcess(routes) => {
                let resp = routes.clone().oneshot(req).await.unwrap_or_else(|e| match e {});
                Ok(resp.map(|b| b.map_err(Into::into).boxed_unsync()))
            }
            Backend::Upstream(channel) => {
                let resp = channel.clone().oneshot(req).await.map_err(|e| Box::new(e) as BoxError)?;
                Ok(resp.map(|b| b.map_err(Into::into).boxed_unsync()))
            }
        }
    }

    /// Whether this backend forwards to a remote upstream (proxy mode). Used to pick
    /// policies that only make sense across the network — enforcing the client deadline
    /// locally, forwarding `grpc-timeout` with grace, capping concurrent streams.
    pub fn is_upstream(&self) -> bool {
        matches!(self, Backend::Upstream(_))
    }
}
