//! Offline native "publications": reconstruct a stopped node's committed native
//! prefix from its durable directory, with no live server.
//!
//! A node's unified consensus log confirms a decoder *pair* (managed + native),
//! so the native prefix cannot be recovered by a bare native session (that
//! refuses with `DecoderMismatch`); it is recovered by reopening the whole
//! [`Session`] the way the node does. A single-voter reopen re-establishes
//! leadership to apply its own committed log (a restart, inherently — a new-term
//! no-op may be appended, which advances Raft only, never native state), then
//! the committed prefix is read from recovered state as ordered outcomes (this
//! survives a checkpoint, which folds the prefix into state rather than
//! re-delivering it).
//!
//! This is the authoritative, client-independent publication order the R11 §1
//! black-box history checker verifies client observations against, and the basis
//! of a `focal ledger publications` reader.

use crate::embedded::NodeIdentity;
use focal_consensus::{DurableNode, NodeConfig};
use focal_core::native::NativeOutcome;
use focal_ledger::{Session, SessionLimits};
use focal_log::{SharedWal, WalIdentity, WalOptions};
use std::path::Path;

/// A generous cap on drive polls; a single-voter node becomes authoritative and
/// applies its committed tail in a handful, but a fixed bound prevents spinning.
const MAX_DRIVE_POLLS: usize = 4096;
/// Extra drains after leadership to let the committed tail finish applying.
const SETTLE_POLLS: usize = 8;

/// Any failure reopening the durable directory or applying its committed tail.
#[derive(Debug, thiserror::Error)]
pub enum PublicationsError {
    #[error(transparent)]
    Log(#[from] focal_log::LogError),
    #[error(transparent)]
    Content(#[from] focal_evidence::ContentError),
    #[error(transparent)]
    Consensus(#[from] focal_consensus::ConsensusError),
    #[error(transparent)]
    Ledger(#[from] focal_ledger::LedgerError),
    #[error("the reopened node did not become authoritative within the poll bound")]
    NotAuthoritative,
}

/// The committed native prefix of the stopped node rooted at `root`, as ordered
/// outcomes — one per committed record, in native-sequence order (each carries
/// its `invocation`, `sequence` and `intent`). The node must not be running (its
/// WAL lock must be free); reopening replays committed history without admitting
/// new work.
pub fn offline_native_publications(
    root: &Path,
    identity: &NodeIdentity,
) -> Result<Vec<NativeOutcome>, PublicationsError> {
    let wal = SharedWal::open(
        root.join("wal"),
        WalOptions::new(WalIdentity {
            cluster: identity.cluster,
            node: identity.node,
            stream: 0,
        }),
    )?;
    let config = NodeConfig::single(identity.node, identity.cluster, identity.ledger.session.0);
    let consensus = DurableNode::open_on_wal(config, wal.clone())?;
    let hosting = crate::network_service::native_hosting(root, identity, wal.disk_budget())?;
    let mut session = Session::from_node_hosted(
        identity.ledger,
        consensus,
        SessionLimits::default(),
        hosting,
    )?;
    session.campaign()?;
    let mut authoritative = false;
    for _ in 0..MAX_DRIVE_POLLS {
        session.poll()?;
        if session.is_authoritative() {
            authoritative = true;
            break;
        }
        session.tick()?;
    }
    if !authoritative {
        return Err(PublicationsError::NotAuthoritative);
    }
    // The checkpoint-restored prefix is already in committed state; a few more
    // drains apply any uncheckpointed committed tail before it is read.
    for _ in 0..SETTLE_POLLS {
        session.poll()?;
    }
    Ok(session.native_committed_outcomes()?)
}
