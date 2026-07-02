//! Test-only Echo gRPC server used as the upstream in proxy integration tests.

use std::net::SocketAddr;

use futures::StreamExt;
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::{Request, Response, Status, Streaming};

pub mod pb {
    tonic::include_proto!("echo.v1");
}

use pb::echo_server::{Echo, EchoServer};
use pb::{EchoRequest, EchoResponse};

#[derive(Default)]
struct EchoSvc;

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
}

/// Spawn the Echo server on an ephemeral port; returns its address.
pub async fn spawn() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let incoming = TcpListenerStream::new(listener);

    tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(EchoServer::new(EchoSvc))
            .serve_with_incoming(incoming)
            .await
            .unwrap();
    });

    addr
}
