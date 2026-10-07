//! The node's hyper-log configuration, from the device and the node (27 §15.3): every field by
//! `hyper_log::Config::derive` (hyper-raft `docs/durable.md` §6), from what focal already states.
//! No field is chosen here, and no constant of the log is copied: a group's bounds are the ones
//! its core runs by (`core_state::limits`), and the device's block is what the system reports.
use std::path::Path;

use hyper_block::buf::Alignment;
use hyper_log::Facts;
use hyper_raft::wire::ENTRY_FIXED_BYTES;

use crate::{ConsensusError, MAX_ENTRY_BYTES, NodeConfig};

/// What the node states of its log, beside the groups' own settings.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct NodeLog {
    /// The device's write unit: `hyper_block::file::preferred_block` of the log's file.
    pub block: usize,
    /// Bytes the node's disk budget gives the log (`DiskKind::Wal`'s quota), its persist area
    /// included.
    pub disk_bytes: u64,
    /// The most groups the node may be placed: its admission bound, never its current placement.
    pub max_groups: usize,
    /// The longest checkpoint cadence among the node's owners, in entries: the control host's
    /// `checkpoint_interval`, a session's `checkpoint_after_entries`.
    pub cadence_entries: u64,
    /// Bytes of recent entries the node gives the log's cache across its groups, charged to its
    /// memory budget where the log is opened.
    pub cache_bytes: u64,
}

/// The log's configuration for a node whose groups run at most `groups`' settings, as `log`
/// states the node:
/// - a frame holds the largest entry any group may take, with the shell's record around it;
/// - a group retains two checkpoint cadences and what may be uncommitted past them: a cadence's
///   entries at their largest, and the core's uncommitted bytes with one entry past them, at
///   least `ENTRY_FIXED_BYTES` each, as the core's own bounds count them;
/// - the queue admits every group's writes, and the file what the disk budget holds.
///
/// Facts that cannot hold the log are refused, typed (`ConsensusError::LogFacts`), never clamped.
pub fn config(groups: &NodeConfig, log: &NodeLog) -> Result<hyper_log::Config, ConsensusError> {
    let unfit =
        || ConsensusError::Configuration("the node's log facts past what this machine addresses");
    let align = Alignment::new(log.block)
        .map_err(|_| ConsensusError::Configuration("the device's block is not a power of two"))?;
    let entry = MAX_ENTRY_BYTES
        .checked_add(ENTRY_FIXED_BYTES)
        .ok_or_else(unfit)?;
    let entry_bytes = u64::try_from(entry).map_err(|_| unfit())?;
    let largest = MAX_ENTRY_BYTES
        .checked_add(hyper_durable::ENTRY_OVERHEAD)
        .ok_or_else(unfit)?;
    let uncommitted_bytes = groups
        .max_uncommitted_bytes
        .checked_add(u64::try_from(groups.max_entry_bytes).map_err(|_| unfit())?)
        .ok_or_else(unfit)?;
    let uncommitted_entries = uncommitted_bytes
        .checked_div(u64::try_from(ENTRY_FIXED_BYTES).map_err(|_| unfit())?)
        .ok_or_else(unfit)?;
    let cadence_bytes = log
        .cadence_entries
        .checked_mul(entry_bytes)
        .ok_or_else(unfit)?;
    hyper_log::Config::derive(&Facts {
        align,
        sealed: false,
        largest_entry: largest,
        disk_bytes: log.disk_bytes,
        max_groups: log.max_groups,
        cadence_entries: log.cadence_entries,
        cadence_bytes,
        uncommitted_entries,
        uncommitted_bytes,
        cache_bytes: log.cache_bytes,
    })
    .map_err(|error| match error {
        hyper_log::LogError::Unfit(unfit) => ConsensusError::LogFacts(unfit),
        _ => ConsensusError::Configuration("the node's log facts give no configuration"),
    })
}

/// The device's write unit for the log's file at `path`, which must exist: what the system reports
/// as the size it writes whole (`hyper_block::file::preferred_block`).
pub fn device_block(path: &Path) -> Result<usize, ConsensusError> {
    let file = std::fs::File::open(path)
        .map_err(|error| ConsensusError::Log(focal_log::LogError::Io(error)))?;
    hyper_block::file::preferred_block(&file, path)
        .map_err(|_| ConsensusError::Configuration("the device reports no block size"))
}

#[cfg(test)]
#[cfg_attr(
    test,
    allow(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::panic,
        clippy::indexing_slicing,
        clippy::arithmetic_side_effects
    )
)]
#[path = "node_log_tests.rs"]
mod tests;
