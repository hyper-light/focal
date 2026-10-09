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
        .unwrap_or_else(|| {
            focal_platform::memory_limit().map_or(UNKNOWN_NODE_BYTES, |limit| {
                limit.checked_div(4).unwrap_or(0).saturating_mul(3)
            })
        })
        .max(MIN_NODE_BYTES);
    usize::try_from(bytes).unwrap_or(usize::MAX)
}

/// This node's budget: its envelope, a quarter of it reserved for completing
/// admitted work (the 256 MiB of the 1 GiB a node held before).
pub(crate) fn node_budget(settings: &Settings) -> Result<MemoryBudget, MemoryError> {
    let bytes = node_bytes(settings);
    MemoryBudget::new(bytes, bytes / 4)
}

/// The allowance of a session this node founds: half its envelope, never
/// below the standard session allowance.
pub(crate) fn founded_session_bytes(settings: &Settings) -> usize {
    (node_bytes(settings) / 2).max(SessionLimits::default().memory_bytes)
}

/// A session's allowance from its committed requirement: a requirement below
/// the standard allowance (none was committed before sessions carried one)
/// is the standard allowance.
pub(crate) fn session_bytes(required_memory: u64) -> usize {
    usize::try_from(required_memory)
        .unwrap_or(usize::MAX)
        .max(SessionLimits::default().memory_bytes)
}

/// Where the node keeps the allowance of the session it founded.
const FOUNDED_FILE: &str = "SESSION.memory";

/// The limits of the session this node founded, as it was founded: the
/// allowance recorded when its log was new, or the standard allowance for a
/// session founded before allowances were recorded. Read-only.
pub(crate) fn founded_limits(
    root: &std::path::Path,
) -> Result<SessionLimits, crate::embedded::NodeError> {
    let path = root.join(FOUNDED_FILE);
    match std::fs::read(&path) {
        Ok(bytes) => Ok(SessionLimits::within(session_bytes(parse(&bytes)?))),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(SessionLimits::default()),
        Err(error) => Err(error.into()),
    }
}

/// The limits of the session this node founds over `consensus`: a log no
/// entry was ever written to is a new session, whose allowance (half this
/// node's envelope) is recorded before its first entry, so that every later
/// open, and every replica the session's committed requirement seats, holds
/// the same allowance. An existing session keeps the one it was founded with.
pub(crate) fn found_limits(
    root: &std::path::Path,
    settings: &Settings,
    consensus: &focal_consensus::DurableNode,
) -> Result<SessionLimits, crate::embedded::NodeError> {
    let path = root.join(FOUNDED_FILE);
    let status = consensus.status();
    if status.term == 0 && status.committed_index == 0 && !path.exists() {
        let bytes = founded_session_bytes(settings);
        crate::embedded::atomic_file(&path, format!("{bytes}\n").as_bytes())?;
    }
    founded_limits(root)
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
    fn an_operator_setting_wins_and_a_session_takes_half_never_below_standard() {
        let mut settings = Settings::default();
        settings.node.memory_bytes = Some(4 << 30);
        assert_eq!(node_bytes(&settings), 4 << 30);
        assert_eq!(founded_session_bytes(&settings), 2 << 30);
        settings.node.memory_bytes = Some(MIN_NODE_BYTES);
        assert_eq!(
            founded_session_bytes(&settings),
            SessionLimits::default().memory_bytes
        );
        assert_eq!(session_bytes(0), SessionLimits::default().memory_bytes);
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
