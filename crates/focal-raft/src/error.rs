//! What the core refuses, and why. Nothing here unwinds: what another core
//! asserts is an error of one of three kinds. A **refusal** changed nothing
//! and the caller may go on. A **violation** is a peer's message that
//! contradicts what this member holds; the message is dropped and nothing
//! changed. A **fatal** error means the member's own state no longer adds
//! up, or an operation stopped half way: the replica stops and is reopened
//! from its durable state.
use crate::ConfigurationError;

#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub enum StorageError {
    #[error("the log is compacted behind the index asked for")]
    Compacted,
    #[error("the log does not hold the index asked for")]
    Unavailable,
    #[error("no snapshot to send yet")]
    SnapshotTemporarilyUnavailable,
    #[error("the entries are not read yet")]
    LogTemporarilyUnavailable,
    #[error("storage: {0}")]
    Other(&'static str),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub enum Error {
    #[error("storage: {0}")]
    Storage(#[from] StorageError),
    #[error("a message a member sends itself arrived from the network")]
    StepLocalMessage,
    #[error("an answer from one that is no member")]
    StepPeerNotFound,
    #[error("only a voter campaigns")]
    NotPromotable,
    #[error("the proposal is dropped")]
    ProposalDropped,
    #[error("the snapshot request is dropped")]
    RequestSnapshotDropped,
    #[error("configuration: {0}")]
    Configuration(#[from] ConfigurationError),
    #[error("settings: {0}")]
    Settings(&'static str),
    #[error("capacity: {0}")]
    Capacity(&'static str),
    #[error("a peer's message contradicts this member: {0}")]
    Violation(&'static str),
    #[error("the member's state is inconsistent: {0}")]
    Invariant(&'static str),
    #[error("no memory to finish an operation already begun")]
    Memory,
}
impl Error {
    /// Whether the replica must stop and be reopened.
    pub fn is_fatal(&self) -> bool {
        matches!(self, Self::Invariant(_) | Self::Memory)
    }
}
pub type Result<T> = std::result::Result<T, Error>;
