use std::path::PathBuf;

fn main() {
    let out = PathBuf::from(std::env::var("OUT_DIR").unwrap());
    tonic_prost_build::configure()
        .build_server(true)
        .build_client(true)
        .file_descriptor_set_path(out.join("echo_descriptor.bin"))
        .compile_protos(&["proto/echo.proto"], &["proto"])
        .expect("failed to compile echo.proto");

    // A separate compile (no descriptor set) for a test-only server-reflection server
    // that returns raw descriptor bytes, so custom options like `google.api.http`
    // survive — unlike tonic-reflection, which round-trips through prost and strips them.
    tonic_prost_build::configure()
        .build_server(true)
        .build_client(false)
        .compile_protos(&["proto/reflection_v1.proto"], &["proto"])
        .expect("failed to compile reflection_v1.proto");
}
