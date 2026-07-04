//! Test-only Echo gRPC server used as the upstream in proxy/server tests.

use std::net::SocketAddr;
use std::pin::Pin;
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

/// Generated types for a minimal server-reflection service (v1).
pub mod reflect_pb {
    #![allow(clippy::enum_variant_names)]
    tonic::include_proto!("grpc.reflection.v1");
}

/// Compiled `FileDescriptorSet` for echo.proto, for building a JSON transcoder.
pub const FILE_DESCRIPTOR_SET: &[u8] =
    include_bytes!(concat!(env!("OUT_DIR"), "/echo_descriptor.bin"));

use pb::echo_server::{Echo, EchoServer};
use pb::{EchoRequest, EchoResponse, RepeatRequest, SleepRequest};

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

    /// An EchoSvc whose `FlakyUnary` fails `fail_times` times before succeeding.
    /// For in-process use (native server tests) — mirrors [`spawn_flaky`].
    pub fn flaky(fail_times: u32) -> Self {
        EchoSvc { flaky_remaining: Arc::new(AtomicU32::new(fail_times)), ..Default::default() }
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

    type ChatStream =
        std::pin::Pin<Box<dyn futures::Stream<Item = Result<EchoResponse, Status>> + Send>>;

    async fn chat(
        &self,
        request: Request<Streaming<EchoRequest>>,
    ) -> Result<Response<Self::ChatStream>, Status> {
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

    type RepeatStream =
        std::pin::Pin<Box<dyn futures::Stream<Item = Result<EchoResponse, Status>> + Send>>;

    async fn repeat(
        &self,
        request: Request<RepeatRequest>,
    ) -> Result<Response<Self::RepeatStream>, Status> {
        let RepeatRequest { message, count } = request.into_inner();
        let output = async_stream::stream! {
            for _ in 0..count {
                yield Ok(EchoResponse { message: message.clone() });
            }
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

// --- Test-only server-reflection that preserves raw descriptor bytes ---------

use reflect_pb::server_reflection_request::MessageRequest;
use reflect_pb::server_reflection_response::MessageResponse;
use reflect_pb::server_reflection_server::{ServerReflection, ServerReflectionServer};
use reflect_pb::{
    ErrorResponse, FileDescriptorResponse, ListServiceResponse, ServerReflectionRequest,
    ServerReflectionResponse, ServiceResponse,
};

/// Split a serialized `FileDescriptorSet` (`repeated file = 1`) into each file's raw
/// bytes, verbatim — preserving custom options that a prost round-trip would drop.
fn split_files(mut set: &[u8]) -> Vec<Vec<u8>> {
    let mut out = Vec::new();
    while !set.is_empty() {
        assert_eq!(set[0], 0x0A, "expected FileDescriptorSet field 1 tag");
        set = &set[1..];
        let mut len = 0u64;
        let mut shift = 0;
        loop {
            let b = set[0];
            set = &set[1..];
            len |= u64::from(b & 0x7F) << shift;
            if b & 0x80 == 0 {
                break;
            }
            shift += 7;
        }
        let len = len as usize;
        out.push(set[..len].to_vec());
        set = &set[len..];
    }
    out
}

/// A minimal reflection service that answers with the *raw* descriptor bytes (options
/// intact), mimicking a compliant reflection server — unlike tonic-reflection, which
/// strips custom options like `google.api.http`.
struct RawReflection {
    files: Vec<Vec<u8>>,
    services: Vec<String>,
}

#[tonic::async_trait]
impl ServerReflection for RawReflection {
    type ServerReflectionInfoStream =
        Pin<Box<dyn futures::Stream<Item = Result<ServerReflectionResponse, Status>> + Send>>;

    async fn server_reflection_info(
        &self,
        request: Request<Streaming<ServerReflectionRequest>>,
    ) -> Result<Response<Self::ServerReflectionInfoStream>, Status> {
        let mut inbound = request.into_inner();
        let files = self.files.clone();
        let services = self.services.clone();
        let output = async_stream::stream! {
            while let Some(req) = inbound.next().await {
                let req = match req { Ok(r) => r, Err(e) => { yield Err(e); break; } };
                let message_response = Some(match req.message_request {
                    Some(MessageRequest::ListServices(_)) => {
                        MessageResponse::ListServicesResponse(ListServiceResponse {
                            service: services.iter().map(|n| ServiceResponse { name: n.clone() }).collect(),
                        })
                    }
                    // Any file request returns the full set — the client dedups by name.
                    Some(MessageRequest::FileContainingSymbol(_))
                    | Some(MessageRequest::FileByFilename(_)) => {
                        MessageResponse::FileDescriptorResponse(FileDescriptorResponse {
                            file_descriptor_proto: files.clone(),
                        })
                    }
                    _ => MessageResponse::ErrorResponse(ErrorResponse {
                        error_code: 12,
                        error_message: "unimplemented".into(),
                    }),
                });
                yield Ok(ServerReflectionResponse {
                    valid_host: String::new(),
                    original_request: None,
                    message_response,
                });
            }
        };
        Ok(Response::new(Box::pin(output)))
    }
}

/// Spawn the Echo server with a raw-preserving reflection service (annotations intact),
/// so REST/`google.api.http` routing can be exercised over reflection.
pub async fn spawn_with_reflection() -> SocketAddr {
    let reflection = RawReflection {
        files: split_files(FILE_DESCRIPTOR_SET),
        services: vec!["echo.v1.Echo".to_string()],
    };
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let incoming = TcpListenerStream::new(listener);
    tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(EchoServer::new(EchoSvc::default()))
            .add_service(ServerReflectionServer::new(reflection))
            .serve_with_incoming(incoming)
            .await
            .unwrap();
    });
    addr
}
