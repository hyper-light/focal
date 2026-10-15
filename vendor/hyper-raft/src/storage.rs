//! What is durable, as the core reads it. The core never writes: it says
//! what to persist ([`crate::Ready`]) and is told when that is done.
use crate::{
    error::StorageError,
    proto::{ConfState, Entry, HardState, Snapshot},
};

/// What storage holds when a member opens: where it voted and what it
/// committed, its configuration, and the proposals it approved.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct InitialState {
    /// The term, the vote and the commit index last persisted.
    pub hard_state: HardState,
    /// The configuration of the latest snapshot or applied change.
    pub configuration: ConfState,
    /// What this member approved by itself and storage holds
    /// ([`crate::fast`]), in any order: every proposal a `Ready` gave that no
    /// later `Ready` released ([`crate::Ready::released`]), whatever the log
    /// holds.
    pub proposals: Vec<Entry>,
    /// The greatest index a `Ready` released that storage holds durably, or
    /// zero: the member knew its log committed through it by a classic
    /// quorum, and knows it again when it opens.
    pub released: u64,
}

/// The durable log and state of one member, as the core reads them.
pub trait Storage {
    /// What the member held when it stopped.
    fn initial_state(&self) -> Result<InitialState, StorageError>;
    /// The entries of `[low, high)` in order, appended to `into`: as many
    /// as `max_bytes` of their encoding admit, and one at least. The page
    /// is chosen before it is copied: what is appended is reserved for
    /// exactly, so that a page's spare capacity never holds a place for
    /// the entries behind it.
    fn entries(
        &self,
        low: u64,
        high: u64,
        max_bytes: u64,
        into: &mut Vec<Entry>,
    ) -> Result<(), StorageError>;
    /// Whether an entry of `[low, high)` satisfies `predicate`, in order
    /// and without copying any: the first that does ends the walk.
    fn any_entry(
        &self,
        low: u64,
        high: u64,
        predicate: &mut dyn FnMut(&Entry) -> bool,
    ) -> Result<bool, StorageError>;
    /// The term of the entry at `index`, which is in `[first_index - 1,
    /// last_index]`: the index before the first is the snapshot's.
    fn term(&self, index: u64) -> Result<u64, StorageError>;
    /// The index of the first entry held, one past the snapshot's.
    fn first_index(&self) -> Result<u64, StorageError>;
    /// The index of the last entry held, or the snapshot's if none is.
    fn last_index(&self) -> Result<u64, StorageError>;
    /// A snapshot at `request_index` or later, for the member `to`.
    fn snapshot(&self, request_index: u64, to: u64) -> Result<Snapshot, StorageError>;
}
