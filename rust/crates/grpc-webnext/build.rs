use std::path::PathBuf;

fn main() {
    // Generate a client stub for the gRPC server-reflection service (v1 + the older
    // v1alpha fallback) so the proxy backend can fetch message descriptors from an
    // upstream at runtime and transcode `+json`. Client-only: we never *serve* reflection.
    let protos = ["proto/reflection_v1.proto", "proto/reflection_v1alpha.proto"];
    for p in protos {
        println!("cargo:rerun-if-changed={p}");
    }

    let out = PathBuf::from(std::env::var("OUT_DIR").unwrap());
    tonic_prost_build::configure()
        .build_server(false)
        .build_client(true)
        .out_dir(&out)
        .compile_protos(&protos, &["proto"])
        .expect("failed to compile reflection protos");

    // The grpc-webnext wire types (frames, metadata, status). The canonical proto lives at
    // the repo root (`/proto`), shared across the Rust/Go/Node implementations. In-workspace
    // we compile that source of truth directly; a crate published to crates.io has no repo
    // root, so we fall back to the vendored copy under `proto/` (kept identical by the
    // `vendored_proto` test). Three levels up from this crate reaches the repo root.
    let (webnext, includes) = if std::path::Path::new("../../../proto/grpc_webnext.proto").exists() {
        ("../../../proto/grpc_webnext.proto", "../../../proto")
    } else {
        ("proto/grpc_webnext.proto", "proto")
    };
    println!("cargo:rerun-if-changed={webnext}");
    prost_build::Config::new()
        .out_dir(&out)
        // Decode `bytes` fields (message payloads, initial_payload, bin metadata) as `Bytes`
        // rather than `Vec<u8>`, so the WS path slices payloads instead of copying them.
        .bytes(["."])
        .compile_protos(&[webnext], &[includes])
        .expect("failed to compile grpc_webnext.proto");
}
