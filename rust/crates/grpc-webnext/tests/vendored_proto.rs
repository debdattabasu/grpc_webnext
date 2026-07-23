//! The crate ships a vendored copy of the shared wire proto (`proto/grpc_webnext.proto`)
//! so it builds standalone from crates.io, where the repo root isn't present. In-workspace
//! that copy must stay byte-identical to the canonical source of truth at the repo root —
//! this test is the drift guard. On a published crate (no repo root) it is a no-op.

#[test]
fn vendored_proto_matches_repo_root() {
    let root = std::path::Path::new("../../../proto/grpc_webnext.proto");
    if !root.exists() {
        // Built outside the workspace (e.g. from a crates.io download): nothing to compare.
        return;
    }
    let canonical = std::fs::read_to_string(root).expect("read repo-root proto");
    let vendored =
        std::fs::read_to_string("proto/grpc_webnext.proto").expect("read vendored proto");
    assert_eq!(
        canonical, vendored,
        "vendored proto/grpc_webnext.proto drifted from the repo-root source of truth; \
         re-copy it: `cp proto/grpc_webnext.proto rust/crates/grpc-webnext/proto/`",
    );
}
