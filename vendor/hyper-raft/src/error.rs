//! What the core refuses, and why. Nothing here unwinds: what another core
//! asserts is an error of one of three kinds. A **refusal** changed nothing
//! and the caller may go on. A **violation** is a peer's message that
//! contradicts what this member holds; the message is dropped and nothing
//! changed. A **fatal** error means the member's own state no longer adds
//! up, or an operation stopped half way: the replica stops and is reopened
//! from its durable state.
use crate::ConfigurationError;

/// What [`crate::Storage`] answers when it cannot give what the core asked
/// for.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub enum StorageError {
    /// The index asked for lies before the first entry storage still holds:
    /// a snapshot covers it.
    #[error("the log is compacted behind the index asked for")]
    Compacted,
    /// The index asked for lies beyond the last entry storage holds.
    #[error("the log does not hold the index asked for")]
    Unavailable,
    /// No snapshot covers the index asked for yet; a leader tries again
    /// later and sends nothing meanwhile.
    #[error("no snapshot to send yet")]
    SnapshotTemporarilyUnavailable,
    /// The entries exist but are not in memory yet; a leader sends nothing
    /// to that member until they are.
    #[error("the entries are not read yet")]
    LogTemporarilyUnavailable,
    /// Any other failure of storage, named.
    #[error("storage: {0}")]
    Other(&'static str),
}

/// Everything the core refuses or reports: a refusal, a violation or a
/// fatal error, as the module states; [`Error::is_fatal`] tells the last
/// apart.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub enum Error {
    /// Storage could not give what the core read.
    #[error("storage: {0}")]
    Storage(#[from] StorageError),
    /// A refusal: a message of a kind only a member sends itself was handed
    /// in as one from a peer.
    #[error("a message a member sends itself arrived from the network")]
    StepLocalMessage,
    /// A refusal: an answer came from one this member does not track.
    #[error("an answer from one that is no member")]
    StepPeerNotFound,
    /// A refusal: a member that is no voter was asked to campaign.
    #[error("only a voter campaigns")]
    NotPromotable,
    /// A refusal: this member's log may lack entries it acknowledged
    /// ([`crate::raft::Lost`]), and the others are no quorum without it (or
    /// the group has the fast track), so it may not campaign.
    #[error("the log may lack what this member acknowledged")]
    Lost,
    /// A refusal: the proposal is dropped, for the cause it names
    /// ([`Dropped`]).
    #[error("the proposal is dropped: {0}")]
    ProposalDropped(Dropped),
    /// A refusal: a read is asked of a member that knows no leader to ask
    /// (a follower that heard none, or a candidate).
    #[error("the read is dropped: no leader to ask")]
    ReadDropped,
    /// A refusal: a snapshot is asked for while this member leads, knows no
    /// leader, holds a snapshot or a request already, or has no entry of
    /// its term.
    #[error("the snapshot request is dropped")]
    RequestSnapshotDropped,
    /// A refusal: the configuration change is not one that may be made.
    #[error("configuration: {0}")]
    Configuration(#[from] ConfigurationError),
    /// A refusal: the [`crate::Config`] does not describe a member that can
    /// run.
    #[error("settings: {0}")]
    Settings(&'static str),
    /// A refusal: a bound of [`crate::Limits`] is reached.
    #[error("capacity: {0}")]
    Capacity(&'static str),
    /// A violation: a peer's message contradicts what this member holds;
    /// it is dropped and nothing changed.
    #[error("a peer's message contradicts this member: {0}")]
    Violation(&'static str),
    /// Fatal: the member's own state no longer adds up.
    #[error("the member's state is inconsistent: {0}")]
    Invariant(&'static str),
    /// Fatal: an operation already begun could not reserve the memory to
    /// finish.
    #[error("no memory to finish an operation already begun")]
    Memory,
}
/// Why a proposal was dropped. Two are the caller's bugs, a proposal no member could take
/// ([`Dropped::is_callers_bug`]); the others are ordinary refusals of a member that cannot take it
/// now, to retry when a leader is known or redirect to it (`docs/raft.md` §3.3).
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub enum Dropped {
    /// No entry to propose, or a fast proposal of no data. The caller's bug.
    #[error("it states nothing")]
    Empty,
    /// A change of the configuration that does not decode. The caller's bug.
    #[error("its change does not decode")]
    Malformed,
    /// A follower that knows no leader to forward it to, or a candidate whose election is under
    /// way: retried once a leader is known.
    #[error("no leader is known")]
    NoLeader,
    /// The leader is handing its leadership over: retried at the next leader.
    #[error("the leader is transferring its leadership")]
    Transferring,
    /// The leader leads a configuration that names it no member, until that configuration is
    /// committed (`docs/raft.md` §3.4): retried at the next leader.
    #[error("the leader is no member of the configuration it leads")]
    NotMember,
    /// What the leader holds uncommitted is at its bound (`Config::max_uncommitted_size`):
    /// retried once entries commit.
    #[error("the leader holds as much uncommitted as it may")]
    Uncommitted,
}
impl Dropped {
    /// Whether the proposal is one no member could take: the caller's bug, never to retry.
    pub fn is_callers_bug(self) -> bool {
        matches!(self, Self::Empty | Self::Malformed)
    }
}
impl Error {
    /// Whether the replica must stop and be reopened.
    pub fn is_fatal(&self) -> bool {
        matches!(self, Self::Invariant(_) | Self::Memory)
    }
}
/// What the core's operations return.
pub type Result<T> = std::result::Result<T, Error>;
