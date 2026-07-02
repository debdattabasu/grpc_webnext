//! Example Greeter server, served over grpc-webnext (Fetch + WebSocket) and
//! native gRPC on the same port via the native server library.

use std::io::Write;
use std::pin::Pin;

use futures::{Stream, StreamExt};
use grpc_webnext_server::{bind_and_serve, ServerConfig};
use tonic::{Request, Response, Status, Streaming};

pub mod pb {
    tonic::include_proto!("greeter.v1");
}

use pb::greeter_server::{Greeter, GreeterServer};
use pb::{ChatMessage, CountdownRequest, HelloReply, HelloRequest, Tick};
use tonic::service::Routes;

#[derive(Default)]
struct GreeterSvc;

#[tonic::async_trait]
impl Greeter for GreeterSvc {
    async fn say_hello(&self, req: Request<HelloRequest>) -> Result<Response<HelloReply>, Status> {
        let name = req.into_inner().name;
        Ok(Response::new(HelloReply {
            message: format!("Hello, {name}!"),
        }))
    }

    type CountdownStream = Pin<Box<dyn Stream<Item = Result<Tick, Status>> + Send>>;

    async fn countdown(
        &self,
        req: Request<CountdownRequest>,
    ) -> Result<Response<Self::CountdownStream>, Status> {
        let from = req.into_inner().from;
        let output = async_stream::stream! {
            for value in (0..=from).rev() {
                yield Ok(Tick { value });
            }
        };
        Ok(Response::new(Box::pin(output)))
    }

    type ChatStream = Pin<Box<dyn Stream<Item = Result<ChatMessage, Status>> + Send>>;

    async fn chat(
        &self,
        req: Request<Streaming<ChatMessage>>,
    ) -> Result<Response<Self::ChatStream>, Status> {
        let mut inbound = req.into_inner();
        let output = async_stream::stream! {
            while let Some(msg) = inbound.next().await {
                match msg {
                    Ok(m) => yield Ok(ChatMessage { text: format!("echo: {}", m.text) }),
                    Err(e) => { yield Err(e); break; }
                }
            }
        };
        Ok(Response::new(Box::pin(output)))
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let routes = Routes::new(GreeterServer::new(GreeterSvc::default()));
    let (addr, handle) = bind_and_serve(routes, ServerConfig::default()).await?;

    // Print readiness for the demo harness, then serve until killed.
    let mut stdout = std::io::stdout();
    writeln!(stdout, "LISTENING http://{addr}")?;
    stdout.flush()?;

    handle.await??;
    Ok(())
}
