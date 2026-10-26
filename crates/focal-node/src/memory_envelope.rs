//! The memory a node holds and the memory a session it founds is allowed.
//!
//! A node's budget is what its operator sets (`node.memory_bytes`) or three
//! quarters of what the process may use: the least of physical memory and its
//! control group's limit (`focal_platform::memory_limit`), the share .NET's
//! GC takes of a container's limit, leaving the rest to the allocator, the
//! runtime and the kernel's page cache the log reads through. Every tenant and
//! session it hosts is a child of that budget, which arbitrates their live use.
//!
//! A session's allowance is one value every replica shares: the leader admits
//! work under it and the followers replay what it admitted, so a replica with
//! less would refuse what the session committed. The node that founds a
//! session allows it half of its own envelope; that allowance is the session's
//! committed memory requirement (its placement policy's `required_memory`), and
//! placement seats a copy only on a node that can fund it.
use crate::config::Settings;
use focal_ledger::SessionLimits;
use focal_memory::{MemoryBudget, MemoryError};

/// The least a node's budget may be.
pub(crate) const MIN_NODE_BYTES: u64 = 64 << 20;
/// A node that cannot read its machine's memory keeps the envelope nodes held
/// before they read it.
const UNKNOWN_NODE_BYTES: u64 = 1 << 30;

/// The bytes this node's budget holds.
pub(crate) fn node_bytes(settings: &Settings) -> usize {
    let bytes = settings
        .node
        .memory_bytes
        .unwrap_or_else(|| focal_platform::memory_envelope().unwrap_or(UNKNOWN_NODE_BYTES))
        .max(MIN_NODE_BYTES);
    usize::try_from(bytes).unwrap_or(usize::MAX)
}

/// This node's budget: its envelope, a quarter of it reserved for completing
/// admitted work (the 256 MiB of the 1 GiB a node held before).
pub(crate) fn node_budget(settings: &Settings) -> Result<MemoryBudget, MemoryError> {
    let bytes = node_bytes(settings);
    MemoryBudget::new(bytes, bytes / 4)
}

/// The allowance of a session this node founds: half its envelope, at least
/// half the least envelope a node holds (`MIN_NODE_BYTES`). It was once never
/// below the standard 256 MiB, more than a 256 MB container's whole envelope:
/// no node of such a cluster could seat a second copy, so no session there
/// could survive a failure.
pub(crate) fn founded_session_bytes(settings: &Settings) -> usize {
    node_bytes(settings) / 2
}

/// A session's allowance from its committed requirement. A requirement of
/// zero is none committed (a session registered before sessions carried
/// one), and such a session has the standard allowance it was founded with.
pub(crate) fn session_bytes(required_memory: u64) -> usize {
    if required_memory == 0 {
        return SessionLimits::default().memory_bytes;
    }
    usize::try_from(required_memory).unwrap_or(usize::MAX)
}

/// Where the node keeps the allowance of the session it founded: one of its small files, sealed from
/// the at-rest level (29 §12.10).
const FOUNDED_FILE: &str = "SESSION.memory";

/// Format: the most bytes the founded allowance's file holds: a `u64` in decimal (twenty digits at
/// most) and its line's end.
pub(crate) const FOUNDED_BYTES: usize = 21;

/// The limits of the session this node founded, as it was founded: the
/// allowance recorded when its log was new, or the standard allowance for a
/// session founded before allowances were recorded. Read-only.
pub(crate) fn founded_limits(
    files: &crate::node_files::NodeFiles,
) -> Result<SessionLimits, crate::embedded::NodeError> {
    match files.read(FOUNDED_FILE, FOUNDED_BYTES)? {
        Some(bytes) => Ok(SessionLimits::within(session_bytes(parse(&bytes)?))),
        None => Ok(SessionLimits::default()),
    }
}

/// Whether `bytes` are the founded allowance's file's, as the node reads it: what a conversion of
/// the node's files checks before it seals one.
pub(crate) fn check(bytes: &[u8]) -> Result<(), crate::embedded::NodeError> {
    parse(bytes).map(|_| ())
}

/// The limits of the session this node founds over `consensus`: a log no
/// entry was ever written to is a new session, whose allowance (half this
/// node's envelope) is recorded before its first entry, so that every later
/// open, and every replica the session's committed requirement seats, holds
/// the same allowance. An existing session keeps the one it was founded with.
pub(crate) fn found_limits(
    files: &crate::node_files::NodeFiles,
    settings: &Settings,
    consensus: &focal_consensus::DurableNode,
) -> Result<SessionLimits, crate::embedded::NodeError> {
    let status = consensus.status();
    if status.term == 0 && status.committed_index == 0 && !files.root().join(FOUNDED_FILE).exists()
    {
        let bytes = founded_session_bytes(settings);
        files.write(FOUNDED_FILE, format!("{bytes}\n").as_bytes())?;
    }
    founded_limits(files)
}

fn parse(bytes: &[u8]) -> Result<u64, crate::embedded::NodeError> {
    std::str::from_utf8(bytes)
        .ok()
        .and_then(|text| text.trim().parse().ok())
        .ok_or(crate::embedded::NodeError::Corrupt(
            "session memory allowance",
        ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_operator_setting_wins_and_a_session_takes_half_however_small() {
        let mut settings = Settings::default();
        settings.node.memory_bytes = Some(4 << 30);
        assert_eq!(node_bytes(&settings), 4 << 30);
        assert_eq!(founded_session_bytes(&settings), 2 << 30);
        // A 256 MB container's envelope founds a session another such node
        // can seat.
        settings.node.memory_bytes = Some(192 << 20);
        assert_eq!(founded_session_bytes(&settings), 96 << 20);
        settings.node.memory_bytes = Some(MIN_NODE_BYTES);
        assert_eq!(founded_session_bytes(&settings), 32 << 20);
        // Below the least envelope, the least envelope.
        settings.node.memory_bytes = Some(MIN_NODE_BYTES / 2);
        assert_eq!(
            node_bytes(&settings),
            usize::try_from(MIN_NODE_BYTES).unwrap()
        );
        assert_eq!(session_bytes(0), SessionLimits::default().memory_bytes);
        assert_eq!(session_bytes(96 << 20), 96 << 20);
        assert_eq!(session_bytes(3 << 30), 3 << 30);
    }

    #[test]
    fn unset_reads_the_machine() {
        let bytes = node_bytes(&Settings::default());
        let limit = focal_platform::memory_limit().unwrap();
        assert_eq!(
            u64::try_from(bytes).unwrap(),
            (limit / 4 * 3).max(MIN_NODE_BYTES)
        );
    }
}
