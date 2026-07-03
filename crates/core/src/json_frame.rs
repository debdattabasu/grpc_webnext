//! Native-JSON WebSocket frame format for the `+json` codec.
//!
//! In JSON mode, WebSocket messages are **text** frames carrying a flat object
//! keyed by `streamId`; you read which field is present:
//!
//! ```jsonc
//! { "streamId": 1, "method": "/pkg.Svc/M", "metadata": {…} } // open (has method)
//! { "streamId": 1, "message": {…} }                          // data message
//! { "streamId": 1, "halfClose": true }                       // client done sending
//! { "streamId": 1, "metadata": {…} }                         // initial response metadata
//! { "streamId": 1, "status": { "code": 0, "message": "" } }  // terminal (trailer/reset)
//! ```
//!
//! The application `message` is a *native* JSON value (not base64 bytes). Proto
//! mode uses binary frames (`crate::frame`); the WebSocket text-vs-binary type
//! selects the codec.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Metadata as a JSON object (single value per key).
pub type JsonMeta = BTreeMap<String, String>;

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct JsonStatus {
    pub code: u32,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub message: String,
}

/// One WebSocket text frame. Exactly one of `method` / `message` / `half_close`
/// / `status` (or a bare `metadata`) determines the frame kind.
#[derive(Debug, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct JsonFrame {
    pub stream_id: u32,
    /// Set → this is an open (subscribe). Carries the method path.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub method: Option<String>,
    /// Request/response metadata (on open) or initial response metadata (header)
    /// or trailing metadata (with `status`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metadata: Option<JsonMeta>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timeout_millis: Option<u32>,
    /// A data message (native JSON). On an open frame, the optional first message.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message: Option<Value>,
    /// Set → the client is done sending.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub half_close: Option<bool>,
    /// Set → terminal status (trailer, or a reset/cancel).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub status: Option<JsonStatus>,
}

/// Parse a WebSocket text frame.
pub fn decode_json_frame(text: &str) -> Result<JsonFrame, serde_json::Error> {
    serde_json::from_str(text)
}

/// Serialize a `JsonFrame` to a WebSocket text payload.
pub fn encode_json_frame(frame: &JsonFrame) -> String {
    serde_json::to_string(frame).expect("JsonFrame serializes")
}

// --- Conversions to/from the internal protobuf `Frame` (server-side only) -----

use crate::pb::{frame::Kind, metadatum, Frame, HalfClose, Message, Metadatum, Reset, Subscribe};

fn meta_vec_to_json(items: &[Metadatum]) -> Option<JsonMeta> {
    let map: JsonMeta = items
        .iter()
        .filter_map(|m| match &m.value {
            Some(metadatum::Value::AsciiValue(s)) => Some((m.key.clone(), s.clone())),
            _ => None, // binary metadata is omitted from JSON frames
        })
        .collect();
    (!map.is_empty()).then_some(map)
}

fn json_to_meta_vec(meta: &Option<JsonMeta>) -> Vec<Metadatum> {
    meta.iter()
        .flatten()
        .map(|(k, v)| Metadatum {
            key: k.clone(),
            value: Some(metadatum::Value::AsciiValue(v.clone())),
        })
        .collect()
}

/// Convert a client `JsonFrame` (WS text) into the internal `Frame`. The frame
/// kind is chosen by which field is present. Message payloads become JSON bytes.
pub fn json_frame_to_proto(f: JsonFrame) -> Frame {
    let stream_id = f.stream_id;
    let to_bytes = |v: &Value| serde_json::to_vec(v).unwrap_or_default();

    let kind = if let Some(method) = f.method {
        Kind::Subscribe(Subscribe {
            stream_id,
            method,
            headers: json_to_meta_vec(&f.metadata),
            timeout_millis: f.timeout_millis.unwrap_or(0),
            initial_payload: f.message.as_ref().map(to_bytes).unwrap_or_default(),
            json: true,
        })
    } else if let Some(status) = f.status {
        // A terminal status from the client is a cancel/reset.
        Kind::Reset(Reset { stream_id, status_code: status.code, status_message: status.message })
    } else if f.half_close == Some(true) {
        Kind::HalfClose(HalfClose { stream_id })
    } else if let Some(message) = f.message {
        Kind::Message(Message { stream_id, payload: to_bytes(&message) })
    } else {
        // Bare `{streamId}` is treated as half-close.
        Kind::HalfClose(HalfClose { stream_id })
    };
    Frame { kind: Some(kind) }
}

/// Convert an internal server `Frame` into a `JsonFrame` for a WS text message.
/// Returns `None` for kinds with no JSON form (ping/pong, or client-only kinds).
pub fn proto_frame_to_json(frame: &Frame) -> Option<JsonFrame> {
    let from_bytes = |b: &[u8]| serde_json::from_slice::<Value>(b).unwrap_or(Value::Null);
    Some(match frame.kind.as_ref()? {
        Kind::Message(m) => JsonFrame {
            stream_id: m.stream_id,
            message: Some(from_bytes(&m.payload)),
            ..Default::default()
        },
        Kind::Header(h) => JsonFrame {
            stream_id: h.stream_id,
            metadata: Some(meta_vec_to_json(&h.headers).unwrap_or_default()),
            ..Default::default()
        },
        Kind::Trailer(t) => JsonFrame {
            stream_id: t.stream_id,
            status: Some(JsonStatus { code: t.status_code, message: t.status_message.clone() }),
            metadata: meta_vec_to_json(&t.trailers),
            ..Default::default()
        },
        Kind::Reset(r) => JsonFrame {
            stream_id: r.stream_id,
            status: Some(JsonStatus { code: r.status_code, message: r.status_message.clone() }),
            ..Default::default()
        },
        _ => return None,
    })
}
