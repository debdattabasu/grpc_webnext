use std::path::PathBuf;

fn main() {
    let out = PathBuf::from(std::env::var("OUT_DIR").unwrap());
    tonic_prost_build::configure()
        .build_server(true)
        .build_client(true)
        .file_descriptor_set_path(out.join("echo_descriptor.bin"))
        .compile_protos(&["proto/echo.proto"], &["proto"])
        .expect("failed to compile echo.proto");
}
