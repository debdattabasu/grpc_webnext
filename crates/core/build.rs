use std::path::PathBuf;

fn main() {
    let proto = "../../proto/grpc_webnext.proto";
    println!("cargo:rerun-if-changed={proto}");

    let out = PathBuf::from(std::env::var("OUT_DIR").unwrap());
    prost_build::Config::new()
        .out_dir(&out)
        .compile_protos(&[proto], &["../../proto"])
        .expect("failed to compile grpc_webnext.proto");
}
