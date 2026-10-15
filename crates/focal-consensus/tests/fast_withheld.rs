//! The fast track is withheld outside this crate's own tests until the shared crates' fix for
//! releasing a held fast vote is taken (09, 2026-10-05): a member configured for it is refused
//! before anything is written.
#![allow(clippy::unwrap_used, clippy::panic, clippy::disallowed_macros)]

use focal_consensus::{ConsensusError, DurableNode, NodeConfig};
use focal_log::{SharedWal, WalIdentity, WalOptions};

#[test]
fn a_member_configured_for_the_fast_track_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    let options = WalOptions::new(WalIdentity {
        cluster: [1; 16],
        node: 1,
        stream: 0,
    });
    let shared = SharedWal::open(dir.path(), options).unwrap();
    let mut config = NodeConfig::single(1, [1; 16], [2; 16]);
    config.fast = true;
    match DurableNode::open_on_wal(config, shared.clone()) {
        Err(ConsensusError::Configuration(_)) => {}
        Err(other) => panic!("refused for another reason: {other}"),
        Ok(_) => panic!("a fast-track member opened"),
    }
    let config = NodeConfig::single(1, [1; 16], [2; 16]);
    assert!(DurableNode::open_on_wal(config, shared).is_ok());
}
