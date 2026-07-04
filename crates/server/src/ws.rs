//! WebSocket streaming path for the native server.
//!
//! Each `Subscribe` becomes a native gRPC call into the inner tonic `Routes`:
//! the request body is a live stream of gRPC-framed messages fed from WS
//! `Message` frames; the response body is de-framed back into `Message` frames.

use std::collections::HashMap;
use std::convert::Infallible;

use bytes::Bytes;
use futures::{SinkExt, StreamExt};
use std::sync::Arc;

use grpc_webnext_core::json_frame::{
    decode_json_frame, encode_json_frame, json_frame_to_proto, json_open_to_subscribe, proto_frame_to_json,
};
use grpc_webnext_core::pb::{frame::Kind, Frame, Header, Message as WsMessage, Reset, Trailer};
use grpc_webnext_core::{decode_frame, encode_frame, grpc_frame, metadata, Deframer, Transcoder, WsBinding};
use http::uri::PathAndQuery;
use http::{HeaderMap, Request};
use http_body_util::{BodyExt, StreamBody};
use hyper::body::Frame as BodyFrame;
use hyper_tungstenite::tungstenite::protocol::frame::coding::CloseCode;
use hyper_tungstenite::tungstenite::protocol::CloseFrame;
use hyper_tungstenite::tungstenite::Message as TungMessage;
use hyper_tungstenite::HyperWebsocket;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::body::Body as TonicBody;
use tonic::service::Routes;
use tonic::{Code, Status};

use crate::{ServerConfig, StreamAuthFn, CT_GRPC};

struct StreamState {
    /// Feeds request messages into the gRPC request body; `None` after half-close.
    req_tx: Option<mpsc::Sender<Bytes>>,
    task: tokio::task::JoinHandle<()>,
}

/// Per-connection WebSocket parameters resolved from the handshake.
pub struct WsParams {
    /// `Some(true)` = JSON/text, `Some(false)` = proto/binary, `None` = infer from
    /// the first frame (only reachable with implicit codecs).
    pub codec: Option<bool>,
    /// Multiplexing enabled (`+multi` subprotocol): streams carry `stream_id`/`method`.
    pub multi: bool,
    /// Single-stream mode only: the gRPC method taken from the WS URL path.
    pub method: Option<String>,
    /// Annotation route: the WS URL matched a `google.api.http` binding, so the method
    /// comes from the binding and each request message is built from path/query + body.
    pub annotation: Option<Arc<WsBinding>>,
}

pub async fn serve(
    routes: Routes,
    websocket: HyperWebsocket,
    config: ServerConfig,
    reject: Option<Status>,
    params: WsParams,
) {
    let transcoder = config.transcoder.clone();
    let stream_auth = config.stream_auth.clone();
    let keepalive = config.ws_keepalive;
    // Max silence tolerated before the peer is declared dead: one ping interval to
    // provoke a pong plus the ack timeout. `None` when keepalive is off.
    let max_silence = keepalive.map(|iv| iv + config.ws_keepalive_timeout);
    let ws = match websocket.await {
        Ok(ws) => ws,
        Err(e) => {
            tracing::debug!("ws upgrade failed: {e}");
            return;
        }
    };
    let (mut ws_sink, mut ws_stream) = ws.split();

    // Connection gate rejected this handshake: hand the client a readable gRPC
    // status via a private close code, then drop the socket without ever reading
    // a frame or creating a stream.
    if let Some(status) = reject {
        let _ = ws_sink.send(close_for_status(&status)).await;
        let _ = ws_sink.close().await;
        return;
    }

    // The outbound channel carries ready-to-send WebSocket messages so each
    // stream can pick its codec (text JSON vs binary protobuf).
    let (outbound_tx, mut outbound_rx) = mpsc::channel::<TungMessage>(64);
    let (done_tx, mut done_rx) = mpsc::channel::<u32>(64);

    let writer = tokio::spawn(async move {
        // Optional keepalive: emit a WebSocket ping every `keepalive` so idle-timeout
        // proxies/LBs see traffic on a quiet stream. The peer (browser or tungstenite)
        // answers pings with pongs automatically, so nothing else is needed either side.
        let mut ping = keepalive.map(keepalive_interval);
        loop {
            tokio::select! {
                msg = outbound_rx.recv() => {
                    let Some(msg) = msg else { break };
                    if ws_sink.send(msg).await.is_err() {
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

    let multi = params.multi;
    let annotation = params.annotation;
    let method_url = params.method.unwrap_or_default();
    let mut streams: HashMap<u32, StreamState> = HashMap::new();
    // Connection codec (true = JSON/text, false = proto/binary). A codec subprotocol
    // pins it up front; otherwise (implicit) it locks to the first frame's type.
    let mut codec: Option<bool> = params.codec;
    // Single-stream mode: whether the one stream has been opened by a first frame.
    let mut opened = false;
    // Keepalive liveness: any inbound frame (in particular the auto-pong to our
    // keepalive ping) proves the peer is alive and pushes the deadline out. If nothing
    // arrives within `max_silence`, the peer is gone — drop the connection (gRPC-style).
    let mut deadline = max_silence.map(|d| tokio::time::Instant::now() + d);

    loop {
        tokio::select! {
            Some(stream_id) = done_rx.recv() => { streams.remove(&stream_id); }
            _ = sleep_until(deadline) => {
                tracing::debug!("ws keepalive: no pong within timeout; dropping connection");
                break;
            }
            incoming = ws_stream.next() => {
                let Some(incoming) = incoming else { break };
                let msg = match incoming { Ok(m) => m, Err(_) => break };
                if let Some(d) = max_silence {
                    deadline = Some(tokio::time::Instant::now() + d);
                }
                let decoded = match msg {
                    TungMessage::Binary(data) => {
                        if *codec.get_or_insert(false) {
                            continue; // locked to JSON/text
                        }
                        decode_binary(&data, multi, &method_url, &mut opened).map(|f| (f, false))
                    }
                    TungMessage::Text(text) => {
                        if !*codec.get_or_insert(true) {
                            continue; // locked to proto/binary
                        }
                        decode_text(&text, multi, &method_url, &mut opened).map(|f| (f, true))
                    }
                    TungMessage::Close(_) => break,
                    _ => continue,
                };
                let Some((frame, json)) = decoded else { continue };
                handle_frame(frame, json, multi, &annotation, &routes, &transcoder, &stream_auth, &outbound_tx, &done_tx, &mut streams).await;
            }
        }
    }

    for (_, state) in streams.drain() {
        state.task.abort();
    }
    drop(outbound_tx);
    let _ = writer.await;
}

/// Decode an inbound binary (proto) frame into an internal `Frame`. In single-stream
/// mode the stream is normalized to id 1 and the `Subscribe` method comes from the
/// URL; a non-`Subscribe` before the stream opens is dropped.
fn decode_binary(data: &[u8], multi: bool, method_url: &str, opened: &mut bool) -> Option<Frame> {
    let mut frame = decode_frame(data).ok()?;
    if !multi {
        match frame.kind.as_mut()? {
            Kind::Subscribe(s) => {
                s.stream_id = 1;
                s.method = method_url.to_string();
                *opened = true;
            }
            _ if !*opened => return None, // must open with a Subscribe first
            Kind::Message(m) => m.stream_id = 1,
            Kind::HalfClose(h) => h.stream_id = 1,
            Kind::Reset(r) => r.stream_id = 1,
            _ => {}
        }
    }
    Some(frame)
}

/// Decode an inbound text (JSON) frame. In single-stream mode the first frame opens
/// the one stream (method from the URL); later frames are messages/half-close/reset.
fn decode_text(text: &str, multi: bool, method_url: &str, opened: &mut bool) -> Option<Frame> {
    let jf = decode_json_frame(text).ok()?;
    if multi {
        return Some(json_frame_to_proto(jf, 0));
    }
    if !*opened {
        *opened = true;
        let sub = json_open_to_subscribe(jf, method_url.to_string(), 1);
        Some(Frame { kind: Some(Kind::Subscribe(sub)) })
    } else {
        Some(json_frame_to_proto(jf, 1))
    }
}

/// Build the WebSocket message for a frame in the stream's codec (`multi` controls
/// whether JSON frames carry `streamId`).
fn to_tung(frame: &Frame, json: bool, multi: bool) -> TungMessage {
    if json {
        if let Some(jf) = proto_frame_to_json(frame, multi) {
            return TungMessage::Text(encode_json_frame(&jf).into());
        }
    }
    TungMessage::Binary(encode_frame(frame))
}

/// A close frame that carries a gRPC status to browser JS: private close code
/// `4000 + code` (gRPC codes are 0..=16, so 4000..=4016) and the message as the
/// reason (WebSocket caps the reason at 123 bytes).
fn close_for_status(status: &Status) -> TungMessage {
    let code = 4000u16 + status.code() as u16;
    TungMessage::Close(Some(CloseFrame {
        code: CloseCode::from(code),
        reason: truncate_utf8(status.message(), 123).to_string().into(),
    }))
}

/// A keepalive ticker whose first tick is one full period out (not immediate) and
/// that skips missed ticks rather than bursting catch-up pings after a busy period.
fn keepalive_interval(period: std::time::Duration) -> tokio::time::Interval {
    let mut i = tokio::time::interval_at(tokio::time::Instant::now() + period, period);
    i.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    i
}

/// Await the next keepalive tick, or never resolve when keepalive is disabled — so
/// the writer's `select!` simply has no ping arm in that case.
async fn next_tick(interval: Option<&mut tokio::time::Interval>) {
    match interval {
        Some(i) => {
            i.tick().await;
        }
        None => std::future::pending().await,
    }
}

/// Resolve at `deadline`, or never when it is `None` (keepalive off) — so the read
/// loop's `select!` simply has no liveness-timeout arm in that case.
async fn sleep_until(deadline: Option<tokio::time::Instant>) {
    match deadline {
        Some(d) => tokio::time::sleep_until(d).await,
        None => std::future::pending().await,
    }
}

/// Truncate to at most `max` bytes on a char boundary.
fn truncate_utf8(s: &str, max: usize) -> &str {
    if s.len() <= max {
        return s;
    }
    let mut end = max;
    while end > 0 && !s.is_char_boundary(end) {
        end -= 1;
    }
    &s[..end]
}

#[allow(clippy::too_many_arguments)]
#[allow(clippy::too_many_arguments)]
async fn handle_frame(
    frame: Frame,
    json: bool,
    multi: bool,
    annotation: &Option<Arc<WsBinding>>,
    routes: &Routes,
    transcoder: &Option<Arc<Transcoder>>,
    stream_auth: &Option<StreamAuthFn>,
    outbound_tx: &mpsc::Sender<TungMessage>,
    done_tx: &mpsc::Sender<u32>,
    streams: &mut HashMap<u32, StreamState>,
) {
    match frame.kind {
        Some(Kind::Subscribe(sub)) => {
            let stream_id = sub.stream_id;
            if streams.contains_key(&stream_id) {
                send_reset(outbound_tx, stream_id, json, multi, Code::InvalidArgument, "stream_id in use").await;
                return;
            }
            if json && transcoder.is_none() {
                send_reset(outbound_tx, stream_id, json, multi, Code::Unimplemented, "+json needs a transcoder").await;
                return;
            }
            let path: PathAndQuery = match sub.method.parse() {
                Ok(p) => p,
                Err(_) => {
                    send_reset(outbound_tx, stream_id, json, multi, Code::InvalidArgument, "invalid method").await;
                    return;
                }
            };

            // Per-stream authorization from the Subscribe metadata.
            let md = metadata::metadata_vec_to_metadata(&sub.headers);
            if let Some(auth) = stream_auth {
                if let Err(status) = auth(&sub.method, &md) {
                    send_reset(outbound_tx, stream_id, json, multi, status.code(), status.message()).await;
                    return;
                }
            }

            let (req_tx, req_rx) = mpsc::channel::<Bytes>(16);
            if !sub.initial_payload.is_empty() {
                let _ = req_tx.send(Bytes::from(sub.initial_payload)).await;
            }

            // An annotation route with no body (GET-style server-stream) takes its
            // single request entirely from the URL: inject one empty payload (the
            // binding fills it from path/query) and half-close.
            let mut req_tx_state = Some(req_tx.clone());
            if let Some(ann) = annotation {
                if !ann.has_body() {
                    let _ = req_tx.send(Bytes::new()).await;
                    req_tx_state = None;
                }
            }

            let headers = md.into_headers();
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
                json,
                multi,
                transcoder.clone(),
                annotation.clone(),
            ));
            streams.insert(stream_id, StreamState { req_tx: req_tx_state, task });
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
    outbound_tx: mpsc::Sender<TungMessage>,
    done_tx: mpsc::Sender<u32>,
    json: bool,
    multi: bool,
    transcoder: Option<Arc<Transcoder>>,
    annotation: Option<Arc<WsBinding>>,
) {
    let method_path = path.path().to_string();

    // Request body: a live stream of gRPC-framed messages. Annotation routes build
    // each message from the URL path/query + body; plain `+json` transcodes each
    // JSON message; proto passes through.
    let req_tc = transcoder.clone();
    let req_ann = annotation.clone();
    let req_path = method_path.clone();
    let body_stream = ReceiverStream::new(req_rx).map(move |payload| {
        // A transcode failure yields empty bytes, which the router rejects.
        let proto = if let Some(ann) = &req_ann {
            ann.build_message(&payload).map(Bytes::from).unwrap_or_default()
        } else if json {
            req_tc
                .as_ref()
                .unwrap()
                .request_json_to_proto(&req_path, &payload)
                .map(Bytes::from)
                .unwrap_or_default()
        } else {
            payload
        };
        Ok::<_, Infallible>(BodyFrame::data(grpc_frame(&proto)))
    });
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
    let _ = outbound_tx.send(to_tung(&Frame { kind: Some(Kind::Header(header)) }, json, multi)).await;

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
                // Transcode each protobuf response message to JSON when `json`.
                let payload = if json {
                    match transcoder.as_ref().unwrap().response_proto_to_json(&method_path, &msg) {
                        Ok(j) => j,
                        Err(e) => {
                            send_reset(&outbound_tx, stream_id, json, multi, Code::Internal, &format!("json encode: {e}")).await;
                            let _ = done_tx.send(stream_id).await;
                            return;
                        }
                    }
                } else {
                    msg.to_vec()
                };
                let out = Frame { kind: Some(Kind::Message(WsMessage { stream_id, payload })) };
                if outbound_tx.send(to_tung(&out, json, multi)).await.is_err() {
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
    let _ = outbound_tx.send(to_tung(&Frame { kind: Some(Kind::Trailer(trailer)) }, json, multi)).await;
    let _ = done_tx.send(stream_id).await;
}

async fn send_reset(
    outbound_tx: &mpsc::Sender<TungMessage>,
    stream_id: u32,
    json: bool,
    multi: bool,
    code: Code,
    message: &str,
) {
    let frame = Frame {
        kind: Some(Kind::Reset(Reset {
            stream_id,
            status_code: code as u32,
            status_message: message.to_string(),
        })),
    };
    let _ = outbound_tx.send(to_tung(&frame, json, multi)).await;
}
