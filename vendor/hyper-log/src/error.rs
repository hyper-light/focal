//! What the log fails with.

use hyper_block::DiskError;

/// What the log fails with.
#[derive(Debug, thiserror::Error)]
pub enum LogError {
    /// The device refused an operation.
    #[error(transparent)]
    Disk(#[from] DiskError),
    /// A write or flush failed; nothing more is taken until the log is reopened.
    #[error("the log is fenced after a failed write or flush")]
    Fenced,
    /// The log has closed, or the thread that answers it has ended.
    #[error("the log is closed")]
    Closed,
    /// The queue holds all the writer takes; the submitter retries.
    #[error("the log's queue is full")]
    Busy,
    /// Every segment holds live records and the file is at its quota: groups must compact.
    #[error("the log is full of live records")]
    Full,
    /// A submission whose records take more than one frame holds.
    #[error("a submission of {0} bytes is more than one frame holds")]
    TooLarge(usize),
    /// A new group past the log's bound.
    #[error("the log holds {0} groups already")]
    TooManyGroups(usize),
    /// The group would retain more entries than its bound: it compacts first.
    #[error("group {0:032x} would retain more than its bound")]
    Backlog(u128),
    /// An update the group as it stands cannot take.
    #[error("group {group:032x}: {reason}")]
    Invalid {
        /// The group.
        group: u128,
        /// What the update breaks.
        reason: &'static str,
    },
    /// An entry the group does not hold.
    #[error("group {group:032x} holds no entry {index}")]
    Unavailable {
        /// The group.
        group: u128,
        /// The entry asked for.
        index: u64,
    },
    /// An entry before the group's first.
    #[error("group {group:032x} compacted entries before {first}")]
    Compacted {
        /// The group.
        group: u128,
        /// The first entry it holds.
        first: u64,
    },
    /// An entry read back does not verify as the one asked for.
    #[error("entry {index} of group {group:032x} does not verify")]
    Corrupt {
        /// The group.
        group: u128,
        /// The entry read.
        index: u64,
    },
    /// State the log acknowledged is damaged: the replicas on this log recover from peers.
    #[error("the log's acknowledged state is damaged: {0}")]
    Damaged(&'static str),
    /// The file holds no log, another log, or another geometry.
    #[error("the file is not this log: {0}")]
    Foreign(&'static str),
    /// The group's handle sent this write before it heard that an earlier one was refused: it
    /// is refused too, changing nothing, so a group's writes never apply out of the order they
    /// were sent (a later write taken after an earlier one refused would leave the group with
    /// the later and without the earlier). The handle sends what it still needs again.
    #[error("group {0:032x}: a write sent behind a refused one")]
    Behind(u128),
    /// The group has a handle (`Log::group`), through which alone it is written.
    #[error("group {0:032x} is written through its handle")]
    Claimed(u128),
    /// A configuration the log cannot run with.
    #[error("the log's configuration is invalid: {0}")]
    Config(&'static str),
    /// A sealed log's bytes whose CRC held and whose MAC or tag does not: changed by someone who
    /// could recompute a CRC, never a torn write (hyper-raft docs/seal.md §5.1). The log serves
    /// nothing from such a file.
    #[error("the sealed log was tampered with: {0}")]
    Tampered(&'static str),
    /// A sealed log's key could not be made, wrapped or unwrapped.
    #[error(transparent)]
    Seal(#[from] hyper_seal::SealError),
}
