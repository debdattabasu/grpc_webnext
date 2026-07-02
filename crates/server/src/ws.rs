//! WebSocket streaming path for the native server.
//!
//! Each `Subscribe` becomes a native gRPC call into the inner tonic `Routes`:
//! the request body is a live stream of gRPC-framed messages fed from WS
//! `Message` frames; the response body is de-framed back into `Message` frames.

use std::collections::HashMap;
use std::convert::Infallible;

use bytes::Bytes;
use futures::{SinkExt, StreamExt};
use grpc_webnext_core::pb::{frame::Kind, Frame, Header, Message as WsMessage, Reset, Trailer};
use grpc_webnext_core::{decode_frame, encode_frame, grpc_frame, metadata, Deframer};
use http::uri::PathAndQuery;
use http::{HeaderMap, Request};
use http_body_util::{BodyExt, StreamBody};
use hyper::body::Frame as BodyFrame;
use hyper_tungstenite::tungstenite::Message as TungMessage;
use hyper_tungstenite::HyperWebsocket;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::body::Body as TonicBody;
use tonic::service::Routes;
use tonic::Code;

use crate::{ServerConfig, CT_GRPC};

struct StreamState {
    /// Feeds request messages into the gRPC request body; `None` after half-close.
    req_tx: Option<mpsc::Sender<Bytes>>,
    task: tokio::task::JoinHandle<()>,
}

pub async fn serve(routes: Routes, websocket: HyperWebsocket, _config: ServerConfig) {
    let ws = match websocket.await {
        Ok(ws) => ws,
        Err(e) => {
            tracing::debug!("ws upgrade failed: {e}");
            return;
        }
    };
    let (mut ws_sink, mut ws_stream) = ws.split();

    let (outbound_tx, mut outbound_rx) = mpsc::channel::<Frame>(64);
    let (done_tx, mut done_rx) = mpsc::channel::<u32>(64);

    let writer = tokio::spawn(async move {
        while let Some(frame) = outbound_rx.recv().await {
            if ws_sink.send(TungMessage::Binary(encode_frame(&frame))).await.is_err() {
                break;
            }
        }
    });

    let mut streams: HashMap<u32, StreamState> = HashMap::new();

    loop {
        tokio::select! {
            Some(stream_id) = done_rx.recv() => { streams.remove(&stream_id); }
            incoming = ws_stream.next() => {
                let Some(incoming) = incoming else { break };
                let msg = match incoming { Ok(m) => m, Err(_) => break };
                match msg {
                    TungMessage::Binary(data) => {
                        let Ok(frame) = decode_frame(&data) else { continue };
                        handle_frame(frame, &routes, &outbound_tx, &done_tx, &mut streams).await;
                    }
                    TungMessage::Close(_) => break,
                    _ => {}
                }
            }
        }
    }

    for (_, state) in streams.drain() {
        state.task.abort();
    }
    drop(outbound_tx);
    let _ = writer.await;
}

async fn handle_frame(
    frame: Frame,
    routes: &Routes,
    outbound_tx: &mpsc::Sender<Frame>,
    done_tx: &mpsc::Sender<u32>,
    streams: &mut HashMap<u32, StreamState>,
) {
    match frame.kind {
        Some(Kind::Subscribe(sub)) => {
            let stream_id = sub.stream_id;
            if streams.contains_key(&stream_id) {
                send_reset(outbound_tx, stream_id, Code::InvalidArgument, "stream_id in use").await;
                return;
            }
            let path: PathAndQuery = match sub.method.parse() {
                Ok(p) => p,
                Err(_) => {
                    send_reset(outbound_tx, stream_id, Code::InvalidArgument, "invalid method").await;
                    return;
                }
            };

            let (req_tx, req_rx) = mpsc::channel::<Bytes>(16);
            if !sub.initial_payload.is_empty() {
                let _ = req_tx.send(Bytes::from(sub.initial_payload)).await;
            }

            let headers = metadata::metadata_vec_to_metadata(&sub.headers).into_headers();
            let timeout = metadata::grpc_timeout_from_millis(sub.timeout_millis);

            let task = tokio::spawn(run_stream(
                routes.clone(),
                path,
                headers,
                timeout,
                req_rx,
                stream_id,
                outbound_tx.clone(),
                done_tx.clone(),
            ));
            streams.insert(stream_id, StreamState { req_tx: Some(req_tx), task });
        }
        Some(Kind::Message(msg)) => {
            if let Some(state) = streams.get(&msg.stream_id) {
                if let Some(tx) = &state.req_tx {
                    let _ = tx.send(Bytes::from(msg.payload)).await;
                }
            }
        }
        Some(Kind::HalfClose(hc)) => {
            if let Some(state) = streams.get_mut(&hc.stream_id) {
                state.req_tx = None;
            }
        }
        Some(Kind::Reset(rst)) => {
            if let Some(state) = streams.remove(&rst.stream_id) {
                state.task.abort();
            }
        }
        _ => {}
    }
}

#[allow(clippy::too_many_arguments)]
async fn run_stream(
    routes: Routes,
    path: PathAndQuery,
    headers: HeaderMap,
    timeout: Option<std::time::Duration>,
    req_rx: mpsc::Receiver<Bytes>,
    stream_id: u32,
    outbound_tx: mpsc::Sender<Frame>,
    done_tx: mpsc::Sender<u32>,
) {
    // Request body: a live stream of gRPC-framed messages.
    let body_stream = ReceiverStream::new(req_rx)
        .map(|payload| Ok::<_, Infallible>(BodyFrame::data(grpc_frame(&payload))));
    let req_body = TonicBody::new(StreamBody::new(body_stream));

    let mut builder = Request::builder().method(http::Method::POST).uri(path);
    for (name, value) in headers.iter() {
        builder = builder.header(name.clone(), value.clone());
    }
    builder = builder.header(http::header::CONTENT_TYPE, CT_GRPC).header("te", "trailers");
    if let Some(d) = timeout {
        builder = builder.header("grpc-timeout", metadata::format_grpc_timeout(d));
    }
    let request = builder.body(req_body).expect("valid request");

    let resp = match tower::ServiceExt::oneshot(routes, request).await {
        Ok(r) => r,
        Err(e) => match e {},
    };
    let (parts, mut body) = resp.into_parts();

    // Initial response metadata.
    let header = Header {
        stream_id,
        headers: metadata::metadata_to_vec(&tonic::metadata::MetadataMap::from_headers(parts.headers.clone())),
    };
    let _ = outbound_tx.send(Frame { kind: Some(Kind::Header(header)) }).await;

    let mut deframer = Deframer::new();
    let mut trailers = HeaderMap::new();

    while let Some(frame) = body.frame().await {
        let frame = match frame {
            Ok(f) => f,
            Err(_) => break,
        };
        if frame.is_data() {
            let data = frame.into_data().unwrap_or_default();
            deframer.push(&data);
            while let Some(msg) = deframer.next_message() {
                let out = Frame {
                    kind: Some(Kind::Message(WsMessage { stream_id, payload: msg.to_vec() })),
                };
                if outbound_tx.send(out).await.is_err() {
                    let _ = done_tx.send(stream_id).await;
                    return;
                }
            }
        } else if frame.is_trailers() {
            if let Ok(t) = frame.into_trailers() {
                trailers = t;
            }
        }
    }

    let (status_code, status_message) = crate::read_status(&trailers, &parts.headers);
    let trailer = Trailer {
        stream_id,
        status_code,
        status_message,
        trailers: metadata::metadata_to_vec(&tonic::metadata::MetadataMap::from_headers(trailers)),
    };
    let _ = outbound_tx.send(Frame { kind: Some(Kind::Trailer(trailer)) }).await;
    let _ = done_tx.send(stream_id).await;
}

async fn send_reset(outbound_tx: &mpsc::Sender<Frame>, stream_id: u32, code: Code, message: &str) {
    let frame = Frame {
        kind: Some(Kind::Reset(Reset {
            stream_id,
            status_code: code as u32,
            status_message: message.to_string(),
        })),
    };
    let _ = outbound_tx.send(frame).await;
}
