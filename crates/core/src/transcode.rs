//! JSON <-> protobuf transcoding for the `+json` codec.
//!
//! grpc-webnext carries opaque message bytes; the envelope (frames, trailers)
//! is always protobuf, but the *application message* may be JSON. Converting
//! JSON to the binary protobuf that a gRPC handler expects (and back) needs the
//! message descriptors, so a `Transcoder` is built from a compiled
//! `FileDescriptorSet` (e.g. `protoc --descriptor_set_out` /
//! `prost_build ... file_descriptor_set_path`).

use prost_reflect::prost::Message;
use prost_reflect::{DescriptorPool, DynamicMessage, MessageDescriptor};
use serde::Serialize;

use crate::httprule::HttpRouter;
pub use crate::httprule::{HttpCall, WsBinding};

#[derive(Debug, thiserror::Error)]
pub enum TranscodeError {
    #[error("failed to load descriptor set: {0}")]
    Descriptor(String),
    #[error("unknown method: {0}")]
    UnknownMethod(String),
    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("protobuf decode error: {0}")]
    Decode(String),
    #[error("http transcoding: {0}")]
    Http(String),
}

/// Transcodes application messages between JSON and binary protobuf, keyed by
/// gRPC method path (`/pkg.Service/Method`), and maps `google.api.http` REST
/// bindings onto gRPC methods.
#[derive(Clone)]
pub struct Transcoder {
    pool: DescriptorPool,
    router: HttpRouter,
}

impl Transcoder {
    /// Build from an encoded `FileDescriptorSet`.
    pub fn from_file_descriptor_set(bytes: &[u8]) -> Result<Self, TranscodeError> {
        let pool = DescriptorPool::decode(bytes)
            .map_err(|e| TranscodeError::Descriptor(e.to_string()))?;
        let router = HttpRouter::from_pool(&pool);
        Ok(Self { pool, router })
    }

    /// Whether any `google.api.http` REST bindings were found.
    pub fn has_http_rules(&self) -> bool {
        !self.router.is_empty()
    }

    /// Map a REST request `(method, path, query, body)` onto a gRPC call, or
    /// `Ok(None)` if no HTTP binding matches.
    pub fn transcode_http_request(
        &self,
        method: &str,
        path: &str,
        query: Option<&str>,
        body: &[u8],
    ) -> Result<Option<HttpCall>, TranscodeError> {
        self.router.transcode(method, path, query, body)
    }

    /// Resolve a WebSocket annotation route from its upgrade path, or `None` if the
    /// path matches no binding.
    pub fn match_ws(&self, path: &str, query: Option<&str>) -> Option<WsBinding> {
        self.router.match_ws(path, query)
    }

    /// Whether a gRPC method has any HTTP annotation (used to keep an annotated
    /// RPC's main route off the plain-HTTP surface).
    pub fn is_annotated_method(&self, grpc_method: &str) -> bool {
        self.router.is_annotated(grpc_method)
    }

    /// Resolve `(request_type, response_type)` for a method path.
    fn io_types(&self, path: &str) -> Result<(MessageDescriptor, MessageDescriptor), TranscodeError> {
        let (service, method) = path
            .trim_start_matches('/')
            .split_once('/')
            .ok_or_else(|| TranscodeError::UnknownMethod(path.to_string()))?;
        let svc = self
            .pool
            .get_service_by_name(service)
            .ok_or_else(|| TranscodeError::UnknownMethod(path.to_string()))?;
        let m = svc
            .methods()
            .find(|m| m.name() == method)
            .ok_or_else(|| TranscodeError::UnknownMethod(path.to_string()))?;
        Ok((m.input(), m.output()))
    }

    /// Whether `path` (`/pkg.Service/Method`) resolves to a known method — i.e. this
    /// transcoder can handle it. Callers use it to distinguish "unknown method"
    /// (UNIMPLEMENTED) from a genuine transcode/validation failure.
    pub fn has_method(&self, path: &str) -> bool {
        self.io_types(path).is_ok()
    }

    /// JSON request message -> binary protobuf. Empty input is the default message.
    pub fn request_json_to_proto(&self, path: &str, json: &[u8]) -> Result<Vec<u8>, TranscodeError> {
        let (input, _) = self.io_types(path)?;
        Ok(self.json_to_proto(input, json)?.encode_to_vec())
    }

    /// Binary protobuf response message -> JSON.
    pub fn response_proto_to_json(&self, path: &str, proto: &[u8]) -> Result<Vec<u8>, TranscodeError> {
        let (_, output) = self.io_types(path)?;
        self.proto_to_json(output, proto)
    }

    fn json_to_proto(
        &self,
        desc: MessageDescriptor,
        json: &[u8],
    ) -> Result<DynamicMessage, TranscodeError> {
        if json.is_empty() {
            return Ok(DynamicMessage::new(desc));
        }
        let mut de = serde_json::Deserializer::from_slice(json);
        let msg = DynamicMessage::deserialize(desc, &mut de)?;
        de.end()?;
        Ok(msg)
    }

    fn proto_to_json(&self, desc: MessageDescriptor, proto: &[u8]) -> Result<Vec<u8>, TranscodeError> {
        let msg =
            DynamicMessage::decode(desc, proto).map_err(|e| TranscodeError::Decode(e.to_string()))?;
        let mut buf = Vec::new();
        let mut ser = serde_json::Serializer::new(&mut buf);
        msg.serialize(&mut ser)?;
        Ok(buf)
    }
}
