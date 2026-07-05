//! Shared transport glue for the grpc-webnext WebSocket surface.
//!
//! The native server (`grpc-webnext-server`) and the schema-agnostic proxy
//! (`grpc-webnext-proxy`) translate the *same* wire protocol; historically each kept its
//! own copy of the frame-codec and keepalive helpers, and those copies drifted (see
//! `doc/UNIFICATION.md`). This crate is the single home for that glue so the two surfaces
//! stay byte-identical by construction.
//!
//! Phase 0 (this module): the WebSocket frame codec (`decode_binary`/`decode_text`/
//! `to_tung`) and the keepalive timing helpers, which were character-for-character
//! duplicates in both crates.

use grpc_webnext_core::json_frame::{
    decode_json_frame, encode_json_frame, json_frame_to_proto, json_open_to_subscribe,
    proto_frame_to_json,
};
use grpc_webnext_core::pb::{frame::Kind, Frame};
use grpc_webnext_core::{decode_frame, encode_frame};
use hyper_tungstenite::tungstenite::Message as TungMessage;

/// Decode an inbound binary (proto) WebSocket frame into an internal `Frame`. In
/// single-stream mode (`!multi`) the stream is normalized to id `1` and a `Subscribe`'s
/// method is taken from the WS URL (`method_url`); a non-`Subscribe` frame arriving before
/// the stream has opened is dropped. `opened` tracks whether that first `Subscribe` has
/// been seen. In multiplexed mode frames pass through unchanged (they carry their own ids).
pub fn decode_binary(data: &[u8], multi: bool, method_url: &str, opened: &mut bool) -> Option<Frame> {
    let mut frame = decode_frame(data).ok()?;
    if multi {
        return Some(frame);
    }
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
    Some(frame)
}

/// Decode an inbound text (JSON) WebSocket frame into an internal `Frame`. In single-stream
/// mode the first frame opens the one stream (method from the URL); later frames are
/// messages/half-close/reset. In multiplexed mode the frame carries its own `streamId`.
/// (The message payload stays as JSON bytes; it is transcoded to protobuf per-message
/// downstream.)
pub fn decode_text(text: &str, multi: bool, method_url: &str, opened: &mut bool) -> Option<Frame> {
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

/// Encode an outbound internal `Frame` as a WebSocket message in the stream's codec: a JSON
/// text frame when `json` (frames with no JSON form fall back to binary), otherwise a binary
/// proto frame. `multi` controls whether JSON frames carry `streamId`.
pub fn to_tung(frame: &Frame, json: bool, multi: bool) -> TungMessage {
    if json {
        if let Some(jf) = proto_frame_to_json(frame, multi) {
            return TungMessage::Text(encode_json_frame(&jf).into());
        }
    }
    TungMessage::Binary(encode_frame(frame))
}

/// A keepalive ticker whose first tick is one full period out (not immediate) and that
/// skips missed ticks rather than bursting catch-up pings after a busy period.
pub fn keepalive_interval(period: std::time::Duration) -> tokio::time::Interval {
    let mut i = tokio::time::interval_at(tokio::time::Instant::now() + period, period);
    i.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    i
}

/// Await the next keepalive tick, or never resolve when keepalive is disabled — so a
/// writer's `select!` simply has no ping arm in that case.
pub async fn next_tick(interval: Option<&mut tokio::time::Interval>) {
    match interval {
        Some(i) => {
            i.tick().await;
        }
        None => std::future::pending().await,
    }
}

/// Resolve at `deadline`, or never when it is `None` (keepalive off) — so a read loop's
/// `select!` simply has no liveness-timeout arm in that case.
pub async fn sleep_until(deadline: Option<tokio::time::Instant>) {
    match deadline {
        Some(d) => tokio::time::sleep_until(d).await,
        None => std::future::pending().await,
    }
}
