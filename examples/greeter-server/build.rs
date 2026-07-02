fn main() {
    tonic_prost_build::configure()
        .build_server(true)
        .build_client(false)
        .compile_protos(&["../greeter.proto"], &[".."])
        .expect("failed to compile greeter.proto");
}
