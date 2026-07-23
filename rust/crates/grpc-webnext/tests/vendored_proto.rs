//! The crate ships a vendored copy of the shared wire proto (`proto/grpc_webnext.proto`)
//! so it builds standalone from crates.io, where the repo root isn't present. In-workspace
//! `build.rs` refreshes that copy from the canonical `/proto/grpc_webnext.proto` on every
//! build, so it should always match the source of truth — this test is the backstop for the
//! case the refresh couldn't run (e.g. a read-only tree). On a published crate it is a no-op.

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
        "vendored proto/grpc_webnext.proto is stale vs the repo-root source of truth. \
         build.rs auto-refreshes it from /proto on any in-workspace build — run `cargo build` \
         and commit the updated copy. Always edit the proto at the repo root, never the copy.",
    );
}
