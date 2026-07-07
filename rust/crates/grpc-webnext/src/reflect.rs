//! Fetch message descriptors from an upstream's gRPC server-reflection service and
//! assemble them into a [`Transcoder`] for `+json` termination.
//!
//! The upstream is queried lazily, once per service, with `file_containing_symbol`.
//! A well-behaved reflection server returns the requested file *and* its transitive
//! dependencies in one shot, but we don't rely on that: any `dependency` we haven't
//! seen is chased down with `file_by_filename` until the closure is complete, then
//! the files are topologically sorted and handed to the descriptor pool.
//!
//! v1 is tried first; an upstream that only implements the older `v1alpha` service
//! answers the retry (v1 responds `Unimplemented` when the service isn't registered).

use std::collections::{HashMap, HashSet};

use crate::Transcoder;
use prost::Message as _;
use prost_types::FileDescriptorProto;
use tonic::transport::Channel;
use tonic::{Code, Status};

/// A reflected proto file kept as its **original** serialized bytes plus its parsed
/// dependency list. We hold the raw bytes rather than a decoded `FileDescriptorProto`
/// because prost drops unknown fields on decode — which would strip custom method
/// options such as the `google.api.http` extension that REST annotation routing needs.
struct RawFile {
    bytes: Vec<u8>,
    deps: Vec<String>,
}

#[allow(clippy::enum_variant_names)] // generated: oneof variants all end in `Response`
pub mod v1 {
    tonic::include_proto!("grpc.reflection.v1");
}
#[allow(clippy::enum_variant_names)] // generated: oneof variants all end in `Response`
pub mod v1alpha {
    tonic::include_proto!("grpc.reflection.v1alpha");
}

/// Which reflection service version answered.
#[derive(Clone, Copy)]
enum Version {
    V1,
    V1alpha,
}

/// Enumerate *every* service the upstream exposes via reflection and assemble one
/// transcoder covering all of them. This is the proxy's whole `+json` schema: one
/// snapshot, loaded eagerly and refreshed on a TTL, rather than per-method lazily.
///
/// v1 is tried first; an upstream that only implements v1alpha answers the retry
/// (v1 responds `Unimplemented` when its reflection service isn't registered). A
/// service whose descriptors can't be fetched is skipped (logged), not fatal.
pub async fn load_all(channel: &Channel) -> Result<Transcoder, Status> {
    let (services, version) = match list_services_v1(channel.clone()).await {
        Err(e) if e.code() == Code::Unimplemented => {
            (list_services_v1alpha(channel.clone()).await?, Version::V1alpha)
        }
        other => (other?, Version::V1),
    };

    let mut files: HashMap<String, RawFile> = HashMap::new();
    for service in services {
        let fetched = match version {
            Version::V1 => fetch_v1(channel.clone(), &service).await,
            Version::V1alpha => fetch_v1alpha(channel.clone(), &service).await,
        };
        match fetched {
            Ok(svc_files) => files.extend(svc_files),
            Err(e) => tracing::warn!("reflection: skipping service {service}: {e}"),
        }
    }

    if files.is_empty() {
        return Err(Status::unavailable("upstream reflection returned no descriptors"));
    }
    // Frame the raw file bytes (dependency-ordered) into a FileDescriptorSet.
    let mut set = Vec::new();
    for raw in topo_sort(files) {
        push_file_descriptor(&mut set, &raw);
    }
    Transcoder::from_file_descriptor_set(&set)
        .map_err(|e| Status::internal(format!("build descriptor pool: {e}")))
}

/// Read a file's `name` and `dependency` list without keeping the decoded message —
/// enough to drive the dependency walk and topo-sort. The raw bytes are kept separately.
fn file_meta(bytes: &[u8]) -> Result<(String, Vec<String>), Status> {
    let fd = FileDescriptorProto::decode(bytes)
        .map_err(|e| Status::internal(format!("bad descriptor: {e}")))?;
    Ok((fd.name().to_string(), fd.dependency))
}

/// Order files so each file's dependencies precede it — the order the descriptor pool
/// expects — and return their raw bytes.
fn topo_sort(files: HashMap<String, RawFile>) -> Vec<Vec<u8>> {
    fn visit(
        name: &str,
        files: &HashMap<String, RawFile>,
        seen: &mut HashSet<String>,
        out: &mut Vec<Vec<u8>>,
    ) {
        if !seen.insert(name.to_string()) {
            return;
        }
        if let Some(f) = files.get(name) {
            for dep in &f.deps {
                visit(dep, files, seen, out);
            }
            out.push(f.bytes.clone());
        }
    }

    let mut seen = HashSet::new();
    let mut out = Vec::with_capacity(files.len());
    let mut names: Vec<String> = files.keys().cloned().collect();
    names.sort(); // deterministic output regardless of HashMap iteration order
    for name in names {
        visit(&name, &files, &mut seen, &mut out);
    }
    out
}

/// Append `raw` (a serialized `FileDescriptorProto`) as `repeated file = 1` of a
/// `FileDescriptorSet`: tag byte `0x0A` (field 1, length-delimited) + a varint length +
/// the bytes verbatim. Framing the *original* bytes is what preserves custom options.
fn push_file_descriptor(buf: &mut Vec<u8>, raw: &[u8]) {
    buf.push(0x0A);
    let mut len = raw.len() as u64;
    loop {
        let mut byte = (len & 0x7F) as u8;
        len >>= 7;
        if len != 0 {
            byte |= 0x80;
        }
        buf.push(byte);
        if len == 0 {
            break;
        }
    }
    buf.extend_from_slice(raw);
}

// The two versions share the descriptor-chasing logic but not the generated stream
// types (they live under different packages), so the drivers are kept small and the
// closure bookkeeping is expressed inline against each.
macro_rules! reflection_driver {
    ($fn_name:ident, $module:ident) => {
        async fn $fn_name(
            channel: Channel,
            symbol: &str,
        ) -> Result<HashMap<String, RawFile>, Status> {
            use $module::server_reflection_client::ServerReflectionClient;
            use $module::server_reflection_request::MessageRequest;
            use $module::server_reflection_response::MessageResponse;
            use $module::ServerReflectionRequest;

            let make = |req: MessageRequest| ServerReflectionRequest {
                host: String::new(),
                message_request: Some(req),
            };

            let (tx, rx) = tokio::sync::mpsc::channel::<ServerReflectionRequest>(16);
            let mut client = ServerReflectionClient::new(channel);
            let mut inbound = client
                .server_reflection_info(tokio_stream::wrappers::ReceiverStream::new(rx))
                .await?
                .into_inner();

            let send = |req| {
                let tx = tx.clone();
                async move {
                    tx.send(req).await.map_err(|_| Status::internal("reflection request channel closed"))
                }
            };

            send(make(MessageRequest::FileContainingSymbol(symbol.to_string()))).await?;

            let mut files: HashMap<String, RawFile> = HashMap::new();
            let mut requested: HashSet<String> = HashSet::new();
            let mut pending: usize = 1;

            while pending > 0 {
                let Some(resp) = inbound.message().await? else { break };
                pending -= 1;
                match resp.message_response {
                    Some(MessageResponse::FileDescriptorResponse(fdr)) => {
                        for raw in fdr.file_descriptor_proto {
                            let (name, deps) = file_meta(&raw)?;
                            for dep in &deps {
                                if !files.contains_key(dep) && requested.insert(dep.clone()) {
                                    send(make(MessageRequest::FileByFilename(dep.clone()))).await?;
                                    pending += 1;
                                }
                            }
                            files.insert(name, RawFile { bytes: raw, deps });
                        }
                    }
                    Some(MessageResponse::ErrorResponse(e)) => {
                        return Err(Status::new(Code::from(e.error_code), e.error_message));
                    }
                    _ => {}
                }
            }
            drop(tx);
            Ok(files)
        }
    };
}

reflection_driver!(fetch_v1, v1);
reflection_driver!(fetch_v1alpha, v1alpha);

// `list_services` is a single request/response, so its driver is much smaller.
macro_rules! list_services_driver {
    ($fn_name:ident, $module:ident) => {
        async fn $fn_name(channel: Channel) -> Result<Vec<String>, Status> {
            use $module::server_reflection_client::ServerReflectionClient;
            use $module::server_reflection_request::MessageRequest;
            use $module::server_reflection_response::MessageResponse;
            use $module::ServerReflectionRequest;

            let (tx, rx) = tokio::sync::mpsc::channel::<ServerReflectionRequest>(4);
            let mut client = ServerReflectionClient::new(channel);
            let mut inbound = client
                .server_reflection_info(tokio_stream::wrappers::ReceiverStream::new(rx))
                .await?
                .into_inner();
            tx.send(ServerReflectionRequest {
                host: String::new(),
                message_request: Some(MessageRequest::ListServices(String::new())),
            })
            .await
            .map_err(|_| Status::internal("reflection request channel closed"))?;
            let resp = inbound
                .message()
                .await?
                .ok_or_else(|| Status::internal("reflection stream closed before response"))?;
            drop(tx);
            match resp.message_response {
                Some(MessageResponse::ListServicesResponse(r)) => {
                    Ok(r.service.into_iter().map(|s| s.name).collect())
                }
                Some(MessageResponse::ErrorResponse(e)) => {
                    Err(Status::new(Code::from(e.error_code), e.error_message))
                }
                _ => Err(Status::internal("unexpected reflection response to list_services")),
            }
        }
    };
}

list_services_driver!(list_services_v1, v1);
list_services_driver!(list_services_v1alpha, v1alpha);
