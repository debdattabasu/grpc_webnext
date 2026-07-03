//! Test-only Echo gRPC server used as the upstream in proxy/server tests.

use std::net::SocketAddr;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;

use futures::StreamExt;
use tokio::net::TcpListener;
use tokio::sync::mpsc::{self, UnboundedReceiver, UnboundedSender};
use tokio_stream::wrappers::TcpListenerStream;
use tonic::{Request, Response, Status, Streaming};

pub mod pb {
    tonic::include_proto!("echo.v1");
}

/// Compiled `FileDescriptorSet` for echo.proto, for building a JSON transcoder.
pub const FILE_DESCRIPTOR_SET: &[u8] =
    include_bytes!(concat!(env!("OUT_DIR"), "/echo_descriptor.bin"));

use pb::echo_server::{Echo, EchoServer};
use pb::{EchoRequest, EchoResponse, SleepRequest};

#[derive(Default)]
pub struct EchoSvc {
    /// If set, the `Hang` handler signals here when it is dropped (i.e. the RPC
    /// was cancelled by the client).
    cancel_tx: Option<UnboundedSender<()>>,
    /// Number of times `FlakyUnary` still fails before it starts succeeding.
    flaky_remaining: Arc<AtomicU32>,
}

impl EchoSvc {
    /// An EchoSvc whose `Hang` handler signals on the returned receiver when
    /// cancelled. For in-process use (native server tests).
    pub fn with_cancel() -> (Self, UnboundedReceiver<()>) {
        let (tx, rx) = mpsc::unbounded_channel();
        (EchoSvc { cancel_tx: Some(tx), ..Default::default() }, rx)
    }
}

/// Fires `cancel_tx` when dropped — dropped when the `Hang` stream is cancelled.
struct CancelGuard(Option<UnboundedSender<()>>);

impl Drop for CancelGuard {
    fn drop(&mut self) {
        if let Some(tx) = &self.0 {
            let _ = tx.send(());
        }
    }
}

#[tonic::async_trait]
impl Echo for EchoSvc {
    async fn unary(
        &self,
        request: Request<EchoRequest>,
    ) -> Result<Response<EchoResponse>, Status> {
        let message = request.into_inner().message;
        Ok(Response::new(EchoResponse { message }))
    }

    type StreamStream =
        std::pin::Pin<Box<dyn futures::Stream<Item = Result<EchoResponse, Status>> + Send>>;

    async fn stream(
        &self,
        request: Request<Streaming<EchoRequest>>,
    ) -> Result<Response<Self::StreamStream>, Status> {
        let mut inbound = request.into_inner();
        let output = async_stream::stream! {
            while let Some(req) = inbound.next().await {
                match req {
                    Ok(EchoRequest { message }) => yield Ok(EchoResponse { message }),
                    Err(e) => { yield Err(e); break; }
                }
            }
        };
        Ok(Response::new(Box::pin(output)))
    }

    async fn sleep(
        &self,
        request: Request<SleepRequest>,
    ) -> Result<Response<EchoResponse>, Status> {
        let millis = request.into_inner().millis;
        tokio::time::sleep(std::time::Duration::from_millis(u64::from(millis))).await;
        Ok(Response::new(EchoResponse { message: "awake".into() }))
    }

    async fn flaky_unary(
        &self,
        request: Request<EchoRequest>,
    ) -> Result<Response<EchoResponse>, Status> {
        // Decrement-and-fail while there are failures remaining.
        let should_fail = self
            .flaky_remaining
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |v| (v > 0).then(|| v - 1))
            .is_ok();
        if should_fail {
            return Err(Status::unavailable("flaky: transient failure"));
        }
        Ok(Response::new(EchoResponse { message: request.into_inner().message }))
    }

    type HangStream =
        std::pin::Pin<Box<dyn futures::Stream<Item = Result<EchoResponse, Status>> + Send>>;

    async fn hang(
        &self,
        _request: Request<EchoRequest>,
    ) -> Result<Response<Self::HangStream>, Status> {
        let guard = CancelGuard(self.cancel_tx.clone());
        let output = async_stream::stream! {
            // The guard lives inside the stream future; dropping the stream
            // (client cancel / disconnect) drops the guard and signals.
            let _guard = guard;
            yield Ok(EchoResponse { message: "started".into() });
            futures::future::pending::<()>().await;
        };
        Ok(Response::new(Box::pin(output)))
    }
}

async fn serve(svc: EchoSvc) -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let incoming = TcpListenerStream::new(listener);
    tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(EchoServer::new(svc))
            .serve_with_incoming(incoming)
            .await
            .unwrap();
    });
    addr
}

/// Spawn the Echo server on an ephemeral port; returns its address.
pub async fn spawn() -> SocketAddr {
    serve(EchoSvc::default()).await
}

/// Spawn the Echo server with cancellation observation: the returned receiver
/// fires once the `Hang` RPC is cancelled by the client.
pub async fn spawn_with_cancel() -> (SocketAddr, UnboundedReceiver<()>) {
    let (tx, rx) = mpsc::unbounded_channel();
    let addr = serve(EchoSvc { cancel_tx: Some(tx), ..Default::default() }).await;
    (addr, rx)
}

/// Spawn the Echo server whose `FlakyUnary` fails `fail_times` before succeeding.
pub async fn spawn_flaky(fail_times: u32) -> SocketAddr {
    serve(EchoSvc {
        flaky_remaining: Arc::new(AtomicU32::new(fail_times)),
        ..Default::default()
    })
    .await
}
