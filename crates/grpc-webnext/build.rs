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
}
