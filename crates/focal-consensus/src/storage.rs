use super::{ConsensusError, NodeConfig};
use crate::PbMessageExt;
use crate::memory::{entry_bytes, reserve, snapshot_bytes};
use focal_memory::{Allocation, BudgetKind, BudgetLane, MemoryBudget};
use raft::{
    GetEntriesContext, RaftState, Storage, StorageError,
    eraftpb::{ConfState, Entry, HardState, Snapshot},
};
use std::collections::VecDeque;

/// Owned retained buffers; payload charges leave with truncation/compaction.
/// Capacity for entry/permit deques remains charged even after removing entries.
pub(super) struct RamLog {
    pub hard_state: HardState,
    pub conf_state: ConfState,
    pub entries: VecDeque<Entry>,
    pub snapshot: Snapshot,
    charges: VecDeque<Allocation>,
    entry_bytes: usize,
    snapshot_charge: Option<Allocation>,
    slots: Option<Allocation>,
    metadata: Allocation,
    budget: MemoryBudget,
}
pub(super) struct PreparedSnapshot {
    snapshot: Snapshot,
    conf: ConfState,
    allocation: Allocation,
}
pub(super) struct PreparedUpdate {
    snapshot: Option<PreparedSnapshot>,
    entries: Vec<(Entry, Allocation)>,
}
impl RamLog {
    pub fn new(config: &NodeConfig, budget: MemoryBudget) -> Result<Self, ConsensusError> {
        let amount = config
            .voters
            .len()
            .checked_add(config.learners.len())
            .and_then(|n| n.checked_mul(64))
            .and_then(|n| n.checked_add(4096))
            .ok_or(ConsensusError::Capacity)?;
        let metadata = reserve(&budget, BudgetKind::Control, BudgetLane::Completion, amount)?;
        Ok(Self {
            hard_state: HardState::default(),
            conf_state: ConfState {
                voters: config.voters.clone(),
                learners: config.learners.clone(),
                ..Default::default()
            },
            entries: VecDeque::new(),
            snapshot: Snapshot::default(),
            charges: VecDeque::new(),
            entry_bytes: 0,
            snapshot_charge: None,
            slots: None,
            metadata,
            budget,
        })
    }
    pub fn resident_bytes(&self) -> Result<usize, ConsensusError> {
        self.metadata
            .bytes()
            .checked_add(self.entry_bytes)
            .and_then(|n| n.checked_add(self.snapshot_charge.as_ref().map_or(0, Allocation::bytes)))
            .and_then(|n| n.checked_add(self.slots.as_ref().map_or(0, Allocation::bytes)))
            .ok_or(ConsensusError::Capacity)
    }
    /// The index of the stored snapshot the log is compacted behind; zero
    /// while the log is complete from its first entry.
    pub fn snapshot_index(&self) -> u64 {
        self.snapshot.get_metadata().index
    }
    pub fn validate(&self) -> Result<(), ConsensusError> {
        super::validate_conf_state(&self.conf_state)?;
        if self.hard_state.term == u64::MAX
            || self.last_index()? == u64::MAX
            || self.snapshot.get_metadata().term == u64::MAX
        {
            return Err(ConsensusError::Corruption("exhausted Raft term or index"));
        }
        if self.hard_state.commit > self.last_index()?
            || self.hard_state.commit < self.snapshot.get_metadata().index
        {
            return Err(ConsensusError::Corruption(
                "commit outside retained snapshot/log",
            ));
        }
        Ok(())
    }
    fn reserve_slots(&mut self, additional: usize) -> Result<(), ConsensusError> {
        let needed = self
            .entries
            .len()
            .checked_add(additional)
            .ok_or(ConsensusError::Capacity)?;
        if needed <= self.entries.capacity() && needed <= self.charges.capacity() {
            return Ok(());
        }
        let capacity = needed
            .checked_next_power_of_two()
            .ok_or(ConsensusError::Capacity)?;
        let amount = capacity
            .checked_mul(
                std::mem::size_of::<Entry>().saturating_add(std::mem::size_of::<Allocation>()),
            )
            .ok_or(ConsensusError::Capacity)?;
        let mut allocation = reserve(
            &self.budget,
            BudgetKind::Index,
            BudgetLane::Completion,
            amount,
        )?;
        let result = self
            .entries
            .try_reserve_exact(additional)
            .and_then(|()| self.charges.try_reserve_exact(additional));
        let actual = self
            .entries
            .capacity()
            .checked_mul(std::mem::size_of::<Entry>())
            .and_then(|n| {
                self.charges
                    .capacity()
                    .checked_mul(std::mem::size_of::<Allocation>())
                    .and_then(|m| n.checked_add(m))
            })
            .ok_or(ConsensusError::Capacity)?;
        allocation
            .shrink_to(actual)
            .map_err(|_| ConsensusError::Capacity)?;
        self.slots = Some(allocation);
        result.map_err(|_| ConsensusError::Capacity)
    }
    pub fn prepare_snapshot(
        &self,
        snapshot: &Snapshot,
    ) -> Result<PreparedSnapshot, ConsensusError> {
        super::validate_conf_state(snapshot.get_metadata().get_conf_state())?;
        if snapshot.get_metadata().index < self.snapshot.get_metadata().index {
            return Err(ConsensusError::Corruption("snapshot regression"));
        }
        let allocation = reserve(
            &self.budget,
            BudgetKind::Payload,
            BudgetLane::Completion,
            snapshot_bytes(snapshot)?,
        )?;
        Ok(PreparedSnapshot {
            snapshot: snapshot.clone(),
            conf: snapshot.get_metadata().get_conf_state().clone(),
            allocation,
        })
    }
    pub fn prepare(
        &mut self,
        entries: &[Entry],
        snapshot: Option<&Snapshot>,
    ) -> Result<PreparedUpdate, ConsensusError> {
        let snapshot = snapshot
            .map(|value| self.prepare_snapshot(value))
            .transpose()?;
        let snapshot_index = snapshot
            .as_ref()
            .map_or(self.snapshot.get_metadata().index, |s| {
                s.snapshot.get_metadata().index
            });
        let mut last = if snapshot.is_some() {
            snapshot_index
        } else {
            self.last_index()?
        };
        let mut previous = None;
        let mut prepared = Vec::new();
        prepared
            .try_reserve_exact(entries.len())
            .map_err(|_| ConsensusError::Capacity)?;
        for entry in entries {
            if entry.index <= snapshot_index {
                continue;
            }
            if entry.index <= self.hard_state.commit {
                if self.term(entry.index)? != entry.term {
                    return Err(ConsensusError::Corruption("committed log overwrite"));
                }
                continue;
            }
            if previous.is_some_and(|index: u64| index.checked_add(1) != Some(entry.index))
                || entry.index > last.checked_add(1).ok_or(ConsensusError::Capacity)?
            {
                return Err(ConsensusError::Corruption("gap in Raft suffix"));
            }
            let allocation = reserve(
                &self.budget,
                BudgetKind::Payload,
                BudgetLane::Completion,
                entry_bytes(entry)?,
            )?;
            prepared.push((entry.clone(), allocation));
            previous = Some(entry.index);
            last = entry.index;
        }
        let added = prepared
            .iter()
            .try_fold(0usize, |n, (_, a)| n.checked_add(a.bytes()))
            .ok_or(ConsensusError::Capacity)?;
        self.entry_bytes
            .checked_add(added)
            .ok_or(ConsensusError::Capacity)?;
        self.reserve_slots(prepared.len())?;
        Ok(PreparedUpdate {
            snapshot,
            entries: prepared,
        })
    }
    pub fn publish(&mut self, update: PreparedUpdate) -> Result<(), ConsensusError> {
        if let Some(snapshot) = update.snapshot {
            self.entries.clear();
            self.charges.clear();
            self.entry_bytes = 0;
            self.conf_state = snapshot.conf;
            self.hard_state.commit = self
                .hard_state
                .commit
                .max(snapshot.snapshot.get_metadata().index);
            self.snapshot = snapshot.snapshot;
            self.snapshot_charge = Some(snapshot.allocation);
        }
        for (entry, allocation) in update.entries {
            while self
                .entries
                .back()
                .is_some_and(|old| old.index >= entry.index)
            {
                self.entries.pop_back();
                let old = self
                    .charges
                    .pop_back()
                    .ok_or(ConsensusError::Corruption("entry accounting mismatch"))?;
                self.entry_bytes = self
                    .entry_bytes
                    .checked_sub(old.bytes())
                    .ok_or(ConsensusError::Capacity)?;
            }
            self.entry_bytes = self
                .entry_bytes
                .checked_add(allocation.bytes())
                .ok_or(ConsensusError::Capacity)?;
            self.entries.push_back(entry);
            self.charges.push_back(allocation);
        }
        Ok(())
    }
    pub fn append(&mut self, entries: &[Entry]) -> Result<(), ConsensusError> {
        let update = self.prepare(entries, None)?;
        self.publish(update)
    }
    pub fn install_snapshot(&mut self, snapshot: Snapshot) -> Result<(), ConsensusError> {
        let update = self.prepare(&[], Some(&snapshot))?;
        self.publish(update)
    }
    pub fn compact_prepared(&mut self, prepared: PreparedSnapshot) -> Result<(), ConsensusError> {
        let index = prepared.snapshot.get_metadata().index;
        if self.term(index)? != prepared.snapshot.get_metadata().term {
            return Err(ConsensusError::Corruption("checkpoint term mismatch"));
        }
        while self
            .entries
            .front()
            .is_some_and(|entry| entry.index <= index)
        {
            self.entries.pop_front();
            let old = self
                .charges
                .pop_front()
                .ok_or(ConsensusError::Corruption("entry accounting mismatch"))?;
            self.entry_bytes = self
                .entry_bytes
                .checked_sub(old.bytes())
                .ok_or(ConsensusError::Capacity)?;
        }
        self.snapshot = prepared.snapshot;
        self.snapshot_charge = Some(prepared.allocation);
        self.conf_state = prepared.conf;
        Ok(())
    }
}
impl Storage for RamLog {
    fn initial_state(&self) -> raft::Result<RaftState> {
        Ok(RaftState {
            hard_state: self.hard_state.clone(),
            conf_state: self.conf_state.clone(),
        })
    }
    fn entries(
        &self,
        low: u64,
        high: u64,
        max_size: impl Into<Option<u64>>,
        _context: GetEntriesContext,
    ) -> raft::Result<Vec<Entry>> {
        if low < self.first_index()? {
            return Err(StorageError::Compacted.into());
        }
        if low > high || high > self.last_index()?.saturating_add(1) {
            return Err(StorageError::Unavailable.into());
        }
        let limit = max_size.into().unwrap_or(u64::MAX);
        let mut size = 0u64;
        let mut result = Vec::new();
        let offset = usize::try_from(
            low.checked_sub(self.first_index()?)
                .ok_or(StorageError::Unavailable)?,
        )
        .map_err(|_| raft::Error::Store(StorageError::Unavailable))?;
        for entry in self.entries.iter().skip(offset).take(
            usize::try_from(high.checked_sub(low).ok_or(StorageError::Unavailable)?)
                .map_err(|_| StorageError::Unavailable)?,
        ) {
            let bytes = entry.compute_size() as u64;
            if !result.is_empty() && size.saturating_add(bytes) > limit {
                break;
            }
            size = size.saturating_add(bytes);
            result.push(entry.clone());
        }
        Ok(result)
    }
    fn term(&self, index: u64) -> raft::Result<u64> {
        let snapshot = self.snapshot.get_metadata();
        if index == snapshot.index {
            return Ok(snapshot.term);
        }
        if index < snapshot.index {
            return Err(StorageError::Compacted.into());
        }
        let offset = usize::try_from(
            index
                .checked_sub(snapshot.index)
                .and_then(|value| value.checked_sub(1))
                .ok_or(StorageError::Unavailable)?,
        )
        .map_err(|_| raft::Error::Store(StorageError::Unavailable))?;
        self.entries
            .get(offset)
            .filter(|entry| entry.index == index)
            .map(|entry| entry.term)
            .ok_or(StorageError::Unavailable.into())
    }
    fn first_index(&self) -> raft::Result<u64> {
        self.snapshot
            .get_metadata()
            .index
            .checked_add(1)
            .ok_or(StorageError::Unavailable.into())
    }
    fn last_index(&self) -> raft::Result<u64> {
        Ok(self
            .entries
            .back()
            .map_or(self.snapshot.get_metadata().index, |entry| entry.index))
    }
    fn snapshot(&self, request_index: u64, _to: u64) -> raft::Result<Snapshot> {
        if self.snapshot.is_empty() || self.snapshot.get_metadata().index < request_index {
            return Err(StorageError::SnapshotTemporarilyUnavailable.into());
        }
        Ok(self.snapshot.clone())
    }
}
