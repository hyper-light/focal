//! The core's storage: the group's durable log as its [`LogStore`] answers for it, with what the
//! replica keeps beside it — the configuration it opened at and the snapshot it prepared for
//! members that lag behind the log's start.
use std::cell::RefCell;

use hyper_raft::proto::{ConfState, Entry, HardState, Snapshot};
use hyper_raft::{InitialState, Storage, StorageError};

use crate::store::LogStore;

/// The core's view of what is durable.
pub struct Held<L> {
    pub(crate) log: L,
    /// The configuration the state machine opened at.
    pub(crate) configuration: ConfState,
    /// The hard state the log held when the member opened.
    pub(crate) opened: HardState,
    /// The snapshot prepared for a member that needs entries the log no longer holds.
    pub(crate) snapshot: Option<Snapshot>,
    /// The bytes of one page the core walks storage by: the store reads at most this much at a
    /// time when the core looks at entries without taking them.
    pub(crate) page: u64,
    /// The entry the core's walks are shown, its buffers reused from one entry to the next, so
    /// a walk copies bytes and allocates nothing once warm.
    scratch: RefCell<Entry>,
}

impl<L: LogStore> Held<L> {
    pub(crate) fn new(log: L, configuration: ConfState, opened: HardState, page: u64) -> Self {
        Self {
            log,
            configuration,
            opened,
            snapshot: None,
            page,
            scratch: RefCell::new(Entry::default()),
        }
    }

    /// The group's log.
    pub fn log(&self) -> &L {
        &self.log
    }
}

impl<L: LogStore> Storage for Held<L> {
    fn initial_state(&self) -> Result<InitialState, StorageError> {
        let mut proposals = Vec::new();
        self.log.proposals(&mut proposals)?;
        Ok(InitialState {
            hard_state: self.opened,
            configuration: self.configuration.clone(),
            proposals,
        })
    }

    fn entries(
        &self,
        low: u64,
        high: u64,
        max_bytes: u64,
        into: &mut Vec<Entry>,
    ) -> Result<(), StorageError> {
        self.log.entries(low, high, max_bytes, into)
    }

    fn any_entry(
        &self,
        low: u64,
        high: u64,
        predicate: &mut dyn FnMut(&Entry) -> bool,
    ) -> Result<bool, StorageError> {
        let mut scratch = self
            .scratch
            .try_borrow_mut()
            .map_err(|_| StorageError::Other("a walk of the log inside another"))?;
        let mut found = false;
        self.log.visit(low, high, self.page, &mut |entry| {
            entry.copy_into(&mut scratch);
            found = predicate(&scratch);
            found
        })?;
        Ok(found)
    }

    fn term(&self, index: u64) -> Result<u64, StorageError> {
        self.log.term(index)
    }

    fn first_index(&self) -> Result<u64, StorageError> {
        self.log
            .bounds()?
            .0
            .index
            .checked_add(1)
            .ok_or(StorageError::Other("an index past u64"))
    }

    fn last_index(&self) -> Result<u64, StorageError> {
        Ok(self.log.bounds()?.1)
    }

    fn snapshot(&self, request_index: u64, _to: u64) -> Result<Snapshot, StorageError> {
        match &self.snapshot {
            Some(snapshot) if hyper_raft::proto::snapshot_index(snapshot) >= request_index => {
                Ok(snapshot.clone())
            }
            _ => Err(StorageError::SnapshotTemporarilyUnavailable),
        }
    }
}
