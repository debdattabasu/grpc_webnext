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
use grpc_webnext_core::{BytesCodec, WsBinding};
use grpc_webnext_transport::{decode_binary, decode_text, keepalive_interval, next_tick, sleep_until, to_tung};
use std::sync::Arc;
use hyper_tungstenite::tungstenite::Message as TungMessage;
use hyper_tungstenite::HyperWebsocket;
use http::uri::PathAndQuery;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::Channel;
use tonic::Code;

use crate::metadata;
use crate::Schema;

/// Per-stream state held by the connection's read loop.
struct StreamState {
    /// Sends request messages into the upstream request stream.
    /// `None` after the client has half-closed.
    req_tx: Option<mpsc::Sender<Bytes>>,
    /// The response-pumping task; aborted on Reset.
    task: tokio::task::JoinHandle<()>,
}

/// Serve one upgraded WebSocket connection. `multi` = multiplexed (streams carry
/// `stream_id`/`method`); otherwise single-stream, with the method taken from
/// `method` (the WS URL path) and the one stream normalized to id 1. `json` selects
/// the codec (text/JSON vs binary/proto) up front from the negotiated subprotocol;
/// JSON streams transcode each message via `schema`.
#[allow(clippy::too_many_arguments)]
pub async fn serve(
    channel: Channel,
    schema: Schema,
    annotation: Option<Arc<WsBinding>>,
    websocket: HyperWebsocket,
    max_streams: usize,
    max_message_bytes: usize,
    multi: bool,
    json: bool,
    method: Option<String>,
    keepalive: Option<std::time::Duration>,
    keepalive_timeout: std::time::Duration,
) {
    let method_url = method.unwrap_or_default();
    // Max silence tolerated before the peer is declared dead: one ping interval to
    // provoke a pong plus the ack timeout. `None` when keepalive is off.
    let max_silence = keepalive.map(|iv| iv + keepalive_timeout);
    let mut opened = false;
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
        // Optional keepalive: emit a WebSocket ping every `keepalive` so idle-timeout
        // proxies/LBs see traffic on a quiet stream. The peer (browser or tungstenite)
        // answers pings with pongs automatically, so nothing else is needed either side.
        let mut ping = keepalive.map(keepalive_interval);
        loop {
            tokio::select! {
                frame = outbound_rx.recv() => {
                    let Some(frame) = frame else { break };
                    if ws_sink.send(to_tung(&frame, json, multi)).await.is_err() {
                        break;
                    }
                }
                _ = next_tick(ping.as_mut()) => {
                    if ws_sink.send(TungMessage::Ping(Bytes::new())).await.is_err() {
                        break;
                    }
                }
            }
        }
    });

    let mut streams: HashMap<u32, StreamState> = HashMap::new();
    // Keepalive liveness: any inbound frame (in particular the auto-pong to our
    // keepalive ping) proves the peer is alive and pushes the deadline out. If nothing
    // arrives within `max_silence`, the peer is gone — drop the connection (gRPC-style).
    let mut deadline = max_silence.map(|d| tokio::time::Instant::now() + d);

    loop {
        tokio::select! {
            // A stream task finished; drop its bookkeeping.
            Some(stream_id) = done_rx.recv() => {
                streams.remove(&stream_id);
            }
            _ = sleep_until(deadline) => {
                tracing::debug!("ws keepalive: no pong within timeout; dropping connection");
                break;
            }
            incoming = ws_stream.next() => {
                let Some(incoming) = incoming else { break };
                let msg = match incoming {
                    Ok(m) => m,
                    Err(e) => { tracing::debug!("ws read error: {e}"); break; }
                };
                if let Some(d) = max_silence {
                    deadline = Some(tokio::time::Instant::now() + d);
                }
                let frame = match msg {
                    // The codec is pinned by the subprotocol, so a frame of the wrong
                    // kind (binary on a JSON socket or vice versa) is simply ignored.
                    TungMessage::Binary(data) if !json => decode_binary(&data, multi, &method_url, &mut opened),
                    TungMessage::Text(text) if json => decode_text(&text, multi, &method_url, &mut opened),
                    TungMessage::Close(_) => break,
                    // Pong (the peer's reply to our keepalive ping) needs no action;
                    // an inbound Ping is auto-answered by tungstenite.
                    _ => continue,
                };
                let Some(frame) = frame else { continue };
                handle_frame(frame, json, &channel, &schema, &annotation, &outbound_tx, &done_tx, &mut streams, max_streams, max_message_bytes).await;
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

#[allow(clippy::too_many_arguments)]
async fn handle_frame(
    frame: Frame,
    json: bool,
    channel: &Channel,
    schema: &Schema,
    annotation: &Option<Arc<WsBinding>>,
    outbound_tx: &mpsc::Sender<Frame>,
    done_tx: &mpsc::Sender<u32>,
    streams: &mut HashMap<u32, StreamState>,
    max_streams: usize,
    max_message_bytes: usize,
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
            // An opening frame may carry the first message inline (`initial_payload`);
            // hold it to the same size limit as any later message.
            if sub.initial_payload.len() > max_message_bytes {
                send_reset(outbound_tx, stream_id, Code::ResourceExhausted, "request message exceeds size limit").await;
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
                let _ = req_tx.send(sub.initial_payload).await;
            }

            // A REST annotation route with no body (GET-style server-stream) takes its one
            // request entirely from the URL: inject an empty payload (the binding fills it
            // from path/query) and half-close.
            let req_tx_state = if annotation.as_ref().is_some_and(|a| !a.has_body()) {
                let _ = req_tx.send(Bytes::new()).await;
                None
            } else {
                Some(req_tx)
            };

            let metadata = metadata::metadata_vec_to_metadata(&sub.headers);
            let timeout = metadata::grpc_timeout_from_metadatum(&sub.timeout_millis);

            let task = tokio::spawn(run_stream(
                channel.clone(),
                schema.clone(),
                json,
                annotation.clone(),
                path,
                metadata,
                timeout,
                req_rx,
                stream_id,
                outbound_tx.clone(),
                done_tx.clone(),
            ));

            streams.insert(stream_id, StreamState { req_tx: req_tx_state, task });
        }
        Some(Kind::Message(msg)) => {
            if msg.payload.len() > max_message_bytes {
                // Oversized message terminates the stream (gRPC RESOURCE_EXHAUSTED),
                // matching the Fetch size limit.
                let stream_id = msg.stream_id;
                send_reset(outbound_tx, stream_id, Code::ResourceExhausted, "request message exceeds size limit").await;
                if let Some(state) = streams.remove(&stream_id) {
                    state.task.abort();
                }
                return;
            }
            if let Some(state) = streams.get(&msg.stream_id) {
                if let Some(tx) = &state.req_tx {
                    let _ = tx.send(msg.payload).await;
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
        // Client never sends Header/Trailer frames in v1; ignore.
        _ => {}
    }
}

/// Drive one upstream gRPC stream: forward request messages, pump responses
/// back as frames, and finish with a `Trailer`.
#[allow(clippy::too_many_arguments)]
async fn run_stream(
    channel: Channel,
    schema: Schema,
    json: bool,
    annotation: Option<Arc<WsBinding>>,
    path: PathAndQuery,
    metadata: tonic::metadata::MetadataMap,
    timeout: Option<std::time::Duration>,
    req_rx: mpsc::Receiver<Bytes>,
    stream_id: u32,
    outbound_tx: mpsc::Sender<Frame>,
    done_tx: mpsc::Sender<u32>,
) {
    let method_path = path.path().to_string();

    // Resolve the transcoder for this method up front (JSON only). A first hit for a
    // service may do a reflection round-trip, but it runs in this per-stream task, so it
    // never blocks the connection read loop or other multiplexed streams.
    let transcoder = if json {
        match schema.transcoder(&method_path).await {
            Ok(tc) => Some(tc),
            Err(status) => {
                // Capability gap (no schema / unknown method), rejected before the
                // upstream RPC starts — a `Reset`, matching the native server's
                // no-transcoder path. (Upstream-returned statuses below are `Trailer`s.)
                send_reset(&outbound_tx, stream_id, status.code(), status.message()).await;
                let _ = done_tx.send(stream_id).await;
                return;
            }
        }
    } else {
        None
    };

    // Request messages become protobuf: a REST annotation builds each from the URL +
    // body; a plain `+json` stream transcodes each JSON message; proto passes through.
    // The `BytesCodec` frames each one on the wire.
    let req_tc = transcoder.clone();
    let req_ann = annotation.clone();
    let req_path = method_path.clone();
    let request_stream = ReceiverStream::new(req_rx).map(move |payload| {
        // A transcode failure yields empty bytes, which the upstream rejects.
        if let Some(ann) = &req_ann {
            ann.build_message(&payload).map(Bytes::from).unwrap_or_default()
        } else if let Some(tc) = &req_tc {
            tc.request_json_to_proto(&req_path, &payload).map(Bytes::from).unwrap_or_default()
        } else {
            payload
        }
    });
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
                    // Proto response message: transcoded to JSON when `json`, otherwise
                    // forwarded as `Bytes` without copying.
                    let payload = match &transcoder {
                        Some(tc) => match tc.response_proto_to_json(&method_path, &payload) {
                            Ok(j) => Bytes::from(j),
                            Err(e) => {
                                send_reset(&outbound_tx, stream_id, Code::Internal, &format!("json encode: {e}")).await;
                                break;
                            }
                        },
                        None => payload,
                    };
                    let frame = Frame {
                        kind: Some(Kind::Message(WsMessage { stream_id, payload })),
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
