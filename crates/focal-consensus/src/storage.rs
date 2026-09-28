use super::{ConsensusError, NodeConfig};
use crate::memory::{entry_bytes, reserve, snapshot_bytes};
use focal_memory::{Allocation, BudgetKind, BudgetLane, MemoryBudget};
use focal_raft::{
    InitialState, Storage, StorageError,
    proto::{self, ConfState, Entry, HardState, Snapshot},
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
        proto::snapshot_index(&self.snapshot)
    }
    pub fn validate(&self) -> Result<(), ConsensusError> {
        super::validate_conf_state(&self.conf_state)?;
        if self.hard_state.term == u64::MAX
            || self.last_index()? == u64::MAX
            || proto::snapshot_term(&self.snapshot) == u64::MAX
        {
            return Err(ConsensusError::Corruption("exhausted Raft term or index"));
        }
        if self.hard_state.commit > self.last_index()?
            || self.hard_state.commit < proto::snapshot_index(&self.snapshot)
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
        let conf = snapshot
            .metadata
            .as_ref()
            .and_then(|metadata| metadata.conf_state.as_ref())
            .ok_or(ConsensusError::Corruption(
                "snapshot without its configuration",
            ))?;
        super::validate_conf_state(conf)?;
        if proto::snapshot_index(snapshot) < proto::snapshot_index(&self.snapshot) {
            return Err(ConsensusError::Corruption("snapshot regression"));
        }
        let conf = conf.clone();
        let allocation = reserve(
            &self.budget,
            BudgetKind::Payload,
            BudgetLane::Completion,
            snapshot_bytes(snapshot)?,
        )?;
        Ok(PreparedSnapshot {
            snapshot: snapshot.clone(),
            conf,
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
            .map_or(proto::snapshot_index(&self.snapshot), |s| {
                proto::snapshot_index(&s.snapshot)
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
                .max(proto::snapshot_index(&snapshot.snapshot));
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
        let index = proto::snapshot_index(&prepared.snapshot);
        if self.term(index)? != proto::snapshot_term(&prepared.snapshot) {
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
    fn initial_state(&self) -> Result<InitialState, StorageError> {
        Ok(InitialState {
            hard_state: self.hard_state.clone(),
            configuration: self.conf_state.clone(),
        })
    }
    fn entries(
        &self,
        low: u64,
        high: u64,
        max_bytes: u64,
        into: &mut Vec<Entry>,
    ) -> Result<(), StorageError> {
        if low < self.first_index()? {
            return Err(StorageError::Compacted);
        }
        if low > high || high > self.last_index()?.saturating_add(1) {
            return Err(StorageError::Unavailable);
        }
        let mut size = 0u64;
        let want = usize::try_from(high.checked_sub(low).ok_or(StorageError::Unavailable)?)
            .map_err(|_| StorageError::Unavailable)?;
        // The upper bound (high - low) is known; reserve it so replication and
        // read fetches never reallocate the entry spine (max_bytes only trims).
        into.try_reserve_exact(want)
            .map_err(|_| StorageError::Unavailable)?;
        let offset = usize::try_from(
            low.checked_sub(self.first_index()?)
                .ok_or(StorageError::Unavailable)?,
        )
        .map_err(|_| StorageError::Unavailable)?;
        let mut taken = 0usize;
        for entry in self.entries.iter().skip(offset).take(want) {
            let bytes = proto::encoded_bytes(entry);
            if taken > 0 && size.saturating_add(bytes) > max_bytes {
                break;
            }
            size = size.saturating_add(bytes);
            into.push(entry.clone());
            taken = taken.saturating_add(1);
        }
        Ok(())
    }
    fn term(&self, index: u64) -> Result<u64, StorageError> {
        let (snapshot_index, snapshot_term) = (
            proto::snapshot_index(&self.snapshot),
            proto::snapshot_term(&self.snapshot),
        );
        if index == snapshot_index {
            return Ok(snapshot_term);
        }
        if index < snapshot_index {
            return Err(StorageError::Compacted);
        }
        let offset = usize::try_from(
            index
                .checked_sub(snapshot_index)
                .and_then(|value| value.checked_sub(1))
                .ok_or(StorageError::Unavailable)?,
        )
        .map_err(|_| StorageError::Unavailable)?;
        self.entries
            .get(offset)
            .filter(|entry| entry.index == index)
            .map(|entry| entry.term)
            .ok_or(StorageError::Unavailable)
    }
    fn first_index(&self) -> Result<u64, StorageError> {
        proto::snapshot_index(&self.snapshot)
            .checked_add(1)
            .ok_or(StorageError::Unavailable)
    }
    fn last_index(&self) -> Result<u64, StorageError> {
        Ok(self
            .entries
            .back()
            .map_or(proto::snapshot_index(&self.snapshot), |entry| entry.index))
    }
    fn snapshot(&self, request_index: u64, _to: u64) -> Result<Snapshot, StorageError> {
        if proto::snapshot_is_empty(&self.snapshot)
            || proto::snapshot_index(&self.snapshot) < request_index
        {
            return Err(StorageError::SnapshotTemporarilyUnavailable);
        }
        Ok(self.snapshot.clone())
    }
}
