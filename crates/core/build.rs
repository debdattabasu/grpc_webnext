use std::path::PathBuf;

fn main() {
    let proto = "../../proto/grpc_webnext.proto";
    println!("cargo:rerun-if-changed={proto}");

    let out = PathBuf::from(std::env::var("OUT_DIR").unwrap());
    prost_build::Config::new()
        .out_dir(&out)
        // Decode `bytes` fields (message payloads, initial_payload, bin metadata) as
        // `Bytes` rather than `Vec<u8>`, so the WS path slices payloads instead of
        // copying them frame by frame. Wire format is unchanged.
        .bytes(["."])
        .compile_protos(&[proto], &["../../proto"])
        .expect("failed to compile grpc_webnext.proto");
}
