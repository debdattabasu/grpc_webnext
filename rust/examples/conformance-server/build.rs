use std::path::PathBuf;

fn main() {
    let out = PathBuf::from(std::env::var("OUT_DIR").unwrap());
    tonic_prost_build::configure()
        .build_server(true)
        .build_client(false)
        .file_descriptor_set_path(out.join("conformance_descriptor.bin"))
        .compile_protos(
            &["../../../conformance/proto/conformance.proto"],
            &["../../../conformance/proto"],
        )
        .expect("failed to compile conformance.proto");
}
