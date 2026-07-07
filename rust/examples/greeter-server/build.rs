use std::path::PathBuf;

fn main() {
    let out = PathBuf::from(std::env::var("OUT_DIR").unwrap());
    tonic_prost_build::configure()
        .build_server(true)
        .build_client(false)
        .file_descriptor_set_path(out.join("greeter_descriptor.bin"))
        .compile_protos(&["../greeter.proto"], &[".."])
        .expect("failed to compile greeter.proto");
}
