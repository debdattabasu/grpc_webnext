//! WebSocket streaming path.
//!
//! One WebSocket carries one or more logical gRPC streams, keyed by `stream_id`
//! (multiplexing). Each `Subscribe` opens an upstream bidi streaming call via
//! the passthrough [`BytesCodec`]; every gRPC cardinality (server/client/bidi)
//! is a special case of bidi at the wire level.
//!
//! Framing rule: exactly one `Frame` per WebSocket message, no fragmentation.

use std::collections::HashMap;

use bytes::Bytes;
use futures::{SinkExt, StreamExt};
use grpc_webnext_core::pb::{frame::Kind, Frame, Header, Message as WsMessage, Reset, Trailer};
use grpc_webnext_core::{decode_frame, encode_frame, BytesCodec};
use hyper_tungstenite::tungstenite::Message as TungMessage;
use hyper_tungstenite::HyperWebsocket;
use http::uri::PathAndQuery;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::Channel;
use tonic::Code;

use crate::metadata;

/// Per-stream state held by the connection's read loop.
struct StreamState {
    /// Sends request messages into the upstream request stream.
    /// `None` after the client has half-closed.
    req_tx: Option<mpsc::Sender<Bytes>>,
    /// The response-pumping task; aborted on Reset.
    task: tokio::task::JoinHandle<()>,
}

/// Serve one upgraded WebSocket connection.
pub async fn serve(channel: Channel, websocket: HyperWebsocket, max_streams: usize) {
    let ws = match websocket.await {
        Ok(ws) => ws,
        Err(e) => {
            tracing::debug!("websocket upgrade failed: {e}");
            return;
        }
    };
    let (mut ws_sink, mut ws_stream) = ws.split();

    // All outbound frames (from every stream task) funnel through one channel so
    // writes to the socket are serialized.
    let (outbound_tx, mut outbound_rx) = mpsc::channel::<Frame>(64);
    // Stream tasks signal completion so the read loop can drop their state.
    let (done_tx, mut done_rx) = mpsc::channel::<u32>(64);

    let writer = tokio::spawn(async move {
        while let Some(frame) = outbound_rx.recv().await {
            if ws_sink
                .send(TungMessage::Binary(encode_frame(&frame)))
                .await
                .is_err()
            {
                break;
            }
        }
    });

    let mut streams: HashMap<u32, StreamState> = HashMap::new();

    loop {
        tokio::select! {
            // A stream task finished; drop its bookkeeping.
            Some(stream_id) = done_rx.recv() => {
                streams.remove(&stream_id);
            }
            incoming = ws_stream.next() => {
                let Some(incoming) = incoming else { break };
                let msg = match incoming {
                    Ok(m) => m,
                    Err(e) => { tracing::debug!("ws read error: {e}"); break; }
                };
                match msg {
                    TungMessage::Binary(data) => {
                        let frame = match decode_frame(&data) {
                            Ok(f) => f,
                            Err(e) => { tracing::debug!("bad frame: {e}"); continue; }
                        };
                        handle_frame(frame, &channel, &outbound_tx, &done_tx, &mut streams, max_streams).await;
                    }
                    TungMessage::Close(_) => break,
                    // Ping/Pong handled by tungstenite; ignore Text.
                    _ => {}
                }
            }
        }
    }

    // Connection closing: abort any live stream tasks.
    for (_, state) in streams.drain() {
        state.task.abort();
    }
    drop(outbound_tx);
    let _ = writer.await;
}

async fn handle_frame(
    frame: Frame,
    channel: &Channel,
    outbound_tx: &mpsc::Sender<Frame>,
    done_tx: &mpsc::Sender<u32>,
    streams: &mut HashMap<u32, StreamState>,
    max_streams: usize,
) {
    match frame.kind {
        Some(Kind::Subscribe(sub)) => {
            let stream_id = sub.stream_id;
            if streams.contains_key(&stream_id) {
                send_reset(outbound_tx, stream_id, Code::InvalidArgument, "stream_id already in use").await;
                return;
            }
            if streams.len() >= max_streams {
                send_reset(outbound_tx, stream_id, Code::ResourceExhausted, "too many concurrent streams").await;
                return;
            }
            let path: PathAndQuery = match sub.method.parse() {
                Ok(p) => p,
                Err(_) => {
                    send_reset(outbound_tx, stream_id, Code::InvalidArgument, "invalid method path").await;
                    return;
                }
            };

            // Request message channel -> upstream request stream.
            let (req_tx, req_rx) = mpsc::channel::<Bytes>(16);
            if !sub.initial_payload.is_empty() {
                let _ = req_tx.send(Bytes::from(sub.initial_payload)).await;
            }

            let metadata = metadata::metadata_vec_to_metadata(&sub.headers);
            let timeout = metadata::grpc_timeout_from_metadatum(&sub.timeout_millis);

            let task = tokio::spawn(run_stream(
                channel.clone(),
                path,
                metadata,
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
            // Drop the request sender so the upstream request stream ends.
            if let Some(state) = streams.get_mut(&hc.stream_id) {
                state.req_tx = None;
            }
        }
        Some(Kind::Reset(rst)) => {
            if let Some(state) = streams.remove(&rst.stream_id) {
                state.task.abort();
            }
        }
        // Server never receives Header/Trailer/Pong from the client in v1; ignore.
        _ => {}
    }
}

/// Drive one upstream gRPC stream: forward request messages, pump responses
/// back as frames, and finish with a `Trailer`.
async fn run_stream(
    channel: Channel,
    path: PathAndQuery,
    metadata: tonic::metadata::MetadataMap,
    timeout: Option<std::time::Duration>,
    req_rx: mpsc::Receiver<Bytes>,
    stream_id: u32,
    outbound_tx: mpsc::Sender<Frame>,
    done_tx: mpsc::Sender<u32>,
) {
    let request_stream = ReceiverStream::new(req_rx);
    let mut request = tonic::Request::from_parts(metadata, Default::default(), request_stream);
    if let Some(d) = timeout {
        request.set_timeout(d + crate::DEADLINE_GRACE); // forwarded as a backstop
    }

    // Establish the upstream call and pump responses to WS frames. On deadline
    // expiry this future is dropped, cancelling the upstream RPC.
    let pump = async {
        let mut client = tonic::client::Grpc::new(channel);
        if let Err(e) = client.ready().await {
            send_reset(&outbound_tx, stream_id, Code::Unavailable, &format!("upstream unready: {e}")).await;
            return;
        }
        let mut response = match client.streaming(request, path, BytesCodec).await {
            Ok(r) => r,
            Err(status) => {
                send_trailer(&outbound_tx, stream_id, &status).await;
                return;
            }
        };

        let header = Header {
            stream_id,
            headers: metadata::metadata_to_vec(response.metadata()),
        };
        let _ = outbound_tx.send(Frame { kind: Some(Kind::Header(header)) }).await;

        let stream = response.get_mut();
        loop {
            match stream.message().await {
                Ok(Some(payload)) => {
                    let frame = Frame {
                        kind: Some(Kind::Message(WsMessage { stream_id, payload: payload.to_vec() })),
                    };
                    if outbound_tx.send(frame).await.is_err() {
                        break;
                    }
                }
                Ok(None) => {
                    let trailers = stream.trailers().await.ok().flatten().unwrap_or_default();
                    let frame = Frame {
                        kind: Some(Kind::Trailer(Trailer {
                            stream_id,
                            status_code: 0,
                            status_message: String::new(),
                            trailers: metadata::metadata_to_vec(&trailers),
                        })),
                    };
                    let _ = outbound_tx.send(frame).await;
                    break;
                }
                Err(status) => {
                    send_trailer(&outbound_tx, stream_id, &status).await;
                    break;
                }
            }
        }
    };

    // Local deadline enforcement.
    match timeout {
        Some(d) => {
            if tokio::time::timeout(d, pump).await.is_err() {
                let frame = Frame {
                    kind: Some(Kind::Trailer(Trailer {
                        stream_id,
                        status_code: Code::DeadlineExceeded as u32,
                        status_message: "proxy deadline exceeded".into(),
                        trailers: vec![],
                    })),
                };
                let _ = outbound_tx.send(frame).await;
            }
        }
        None => pump.await,
    }

    let _ = done_tx.send(stream_id).await;
}

async fn send_trailer(outbound_tx: &mpsc::Sender<Frame>, stream_id: u32, status: &tonic::Status) {
    let frame = Frame {
        kind: Some(Kind::Trailer(Trailer {
            stream_id,
            status_code: status.code() as u32,
            status_message: status.message().to_string(),
            trailers: metadata::metadata_to_vec(status.metadata()),
        })),
    };
    let _ = outbound_tx.send(frame).await;
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
