//! What is durable, as the core reads it. The core never writes: it says
//! what to persist ([`crate::Ready`]) and is told when that is done.
use crate::{
    error::StorageError,
    proto::{ConfState, Entry, HardState, Snapshot},
};

#[derive(Clone, Debug, Default, PartialEq)]
pub struct InitialState {
    pub hard_state: HardState,
    pub configuration: ConfState,
}

pub trait Storage {
    fn initial_state(&self) -> Result<InitialState, StorageError>;
    /// The entries of `[low, high)` in order, appended to `into`: as many
    /// as `max_bytes` of their encoding admit, and one at least.
    fn entries(
        &self,
        low: u64,
        high: u64,
        max_bytes: u64,
        into: &mut Vec<Entry>,
    ) -> Result<(), StorageError>;
    /// The term of the entry at `index`, which is in `[first_index - 1,
    /// last_index]`: the index before the first is the snapshot's.
    fn term(&self, index: u64) -> Result<u64, StorageError>;
    fn first_index(&self) -> Result<u64, StorageError>;
    fn last_index(&self) -> Result<u64, StorageError>;
    /// A snapshot at `request_index` or later, for the member `to`.
    fn snapshot(&self, request_index: u64, to: u64) -> Result<Snapshot, StorageError>;
}
