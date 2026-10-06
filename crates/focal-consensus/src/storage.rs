use super::{ConsensusError, NodeConfig};
use crate::memory::{entry_bytes, reserve, snapshot_bytes};
use focal_memory::{Allocation, BudgetKind, BudgetLane, MemoryBudget};
use hyper_raft::{
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
    /// What this member approved by itself (27 §4), each with its charge,
    /// held until a `Ready` releases its index (`Ready::released`), whatever
    /// the log holds there: the core asks for them back at every open and
    /// holds them until it knows that index committed by a classic quorum.
    /// At most what the core holds (`Limits::proposals`).
    pub proposals: Vec<(Entry, Allocation)>,
    /// The greatest index a `Ready` released in this process. Not written:
    /// focal's log keeps no record of a release, so a member opens with
    /// zero, is given back every proposal its log still holds, and holds
    /// them until it learns their commit again, which costs the core's
    /// bounded room and never safety (hyper-raft `InitialState::released`).
    released: u64,
    charges: VecDeque<Allocation>,
    /// The bytes of the retained entries, running: through each entry,
    /// from an origin that moves with compaction, so the bytes of any
    /// range are a subtraction (`bytes_between`) and never a walk.
    cumulative: VecDeque<u64>,
    /// The running total before the first retained entry.
    origin: u64,
    entry_bytes: usize,
    snapshot_charge: Option<Allocation>,
    slots: Option<Allocation>,
    /// Held for the log's life: the charge of its identity and configuration.
    _metadata: Allocation,
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
    proposals: Vec<(Entry, Allocation)>,
    /// The index the `Ready` released (`Ready::released`), if it moved.
    released: Option<u64>,
}
impl RamLog {
    pub fn new(config: &NodeConfig, budget: MemoryBudget) -> Result<Self, ConsensusError> {
        // The log's own state and its copy of the configuration: each
        // member's id once, in the voters or the learners, with the
        // bookkeeping of the four member lists.
        let amount = config
            .voters
            .len()
            .checked_add(config.learners.len())
            .and_then(|n| n.checked_mul(std::mem::size_of::<u64>()))
            .and_then(|n| n.checked_add(std::mem::size_of::<Self>()))
            .and_then(|n| n.checked_add(4 * focal_memory::ALLOCATOR_OVERHEAD))
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
            proposals: Vec::new(),
            released: 0,
            charges: VecDeque::new(),
            cumulative: VecDeque::new(),
            origin: 0,
            entry_bytes: 0,
            snapshot_charge: None,
            slots: None,
            _metadata: metadata,
            budget,
        })
    }
    /// The index of the stored snapshot the log is compacted behind; zero
    /// while the log is complete from its first entry.
    pub fn snapshot_index(&self) -> u64 {
        proto::snapshot_index(&self.snapshot)
    }
    /// The bytes the retained entries of `[low, high)` hold — the first
    /// `max_entries` of them at most, and `cap` at most — from the running
    /// totals kept beside the entries, so that asking about a range costs
    /// nothing whatever its length. Indexes behind the first retained
    /// entry hold nothing here (they are the snapshot's).
    pub fn bytes_between(
        &self,
        low: u64,
        high: u64,
        max_entries: usize,
        cap: usize,
    ) -> Result<usize, ConsensusError> {
        let low = low.max(self.first_index()?);
        let high = high
            .min(self.last_index()?.saturating_add(1))
            .min(low.saturating_add(u64::try_from(max_entries).unwrap_or(u64::MAX)));
        if low >= high {
            return Ok(0);
        }
        let from = self.position(low)?;
        let through = self.position(high.saturating_sub(1))?;
        let before = match from.checked_sub(1) {
            Some(previous) => self.total_through(previous)?,
            None => self.origin,
        };
        let bytes = self
            .total_through(through)?
            .checked_sub(before)
            .ok_or(ConsensusError::Corruption("entry accounting mismatch"))?;
        Ok(usize::try_from(bytes).unwrap_or(usize::MAX).min(cap))
    }
    /// The running total through the retained entry at `position`.
    fn total_through(&self, position: usize) -> Result<u64, ConsensusError> {
        self.cumulative
            .get(position)
            .copied()
            .ok_or(ConsensusError::Corruption("entry accounting mismatch"))
    }
    /// The position of `index` among the retained entries.
    fn position(&self, index: u64) -> Result<usize, StorageError> {
        usize::try_from(
            index
                .checked_sub(self.first_index()?)
                .ok_or(StorageError::Unavailable)?,
        )
        .map_err(|_| StorageError::Unavailable)
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
        let running = self
            .cumulative
            .back()
            .map_or(0, |through| through.saturating_sub(self.origin));
        if self.cumulative.len() != self.entries.len()
            || running != u64::try_from(self.entry_bytes).unwrap_or(u64::MAX)
        {
            return Err(ConsensusError::Corruption("entry accounting mismatch"));
        }
        Ok(())
    }
    fn reserve_slots(&mut self, additional: usize) -> Result<(), ConsensusError> {
        let needed = self
            .entries
            .len()
            .checked_add(additional)
            .ok_or(ConsensusError::Capacity)?;
        if needed <= self.entries.capacity()
            && needed <= self.charges.capacity()
            && needed <= self.cumulative.capacity()
        {
            return Ok(());
        }
        let capacity = needed
            .checked_next_power_of_two()
            .ok_or(ConsensusError::Capacity)?;
        let slot = std::mem::size_of::<Entry>()
            .saturating_add(std::mem::size_of::<Allocation>())
            .saturating_add(std::mem::size_of::<u64>());
        let amount = capacity.checked_mul(slot).ok_or(ConsensusError::Capacity)?;
        let mut allocation = reserve(
            &self.budget,
            BudgetKind::Index,
            BudgetLane::Completion,
            amount,
        )?;
        // Grow to the power of two the budget was just charged for, not by
        // the addition alone: reserving exactly what was asked moved the
        // whole retained log on every commit (two reallocations a commit,
        // hundreds of kilobytes each before a checkpoint) for the same
        // charge.
        let result = self
            .entries
            .try_reserve_exact(capacity.saturating_sub(self.entries.len()))
            .and_then(|()| {
                self.charges
                    .try_reserve_exact(capacity.saturating_sub(self.charges.len()))
            })
            .and_then(|()| {
                self.cumulative
                    .try_reserve_exact(capacity.saturating_sub(self.cumulative.len()))
            });
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
            .and_then(|n| {
                self.cumulative
                    .capacity()
                    .checked_mul(std::mem::size_of::<u64>())
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
        self.prepare_with(entries, snapshot, &[], None)
    }
    /// As `prepare`, with what the member approved by itself and the index
    /// through which the `Ready` released what it approved before.
    pub fn prepare_with(
        &mut self,
        entries: &[Entry],
        snapshot: Option<&Snapshot>,
        proposals: &[Entry],
        released: Option<u64>,
    ) -> Result<PreparedUpdate, ConsensusError> {
        let mut held = Vec::new();
        held.try_reserve_exact(proposals.len())
            .map_err(|_| ConsensusError::Capacity)?;
        for proposal in proposals {
            let allocation = reserve(
                &self.budget,
                BudgetKind::Payload,
                BudgetLane::Completion,
                entry_bytes(proposal)?,
            )?;
            held.push((proposal.clone(), allocation));
        }
        self.proposals
            .try_reserve(held.len())
            .map_err(|_| ConsensusError::Capacity)?;
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
            proposals: held,
            released,
        })
    }
    pub fn publish(&mut self, update: PreparedUpdate) -> Result<(), ConsensusError> {
        if let Some(snapshot) = update.snapshot {
            self.entries.clear();
            self.charges.clear();
            self.cumulative.clear();
            self.origin = 0;
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
                self.cumulative
                    .pop_back()
                    .ok_or(ConsensusError::Corruption("entry accounting mismatch"))?;
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
            let through = self
                .cumulative
                .back()
                .copied()
                .unwrap_or(self.origin)
                .checked_add(
                    u64::try_from(allocation.bytes()).map_err(|_| ConsensusError::Capacity)?,
                )
                .ok_or(ConsensusError::Capacity)?;
            self.cumulative.push_back(through);
            self.entries.push_back(entry);
            self.charges.push_back(allocation);
        }
        // What the `Ready` released is dropped before its own proposals are
        // taken (hyper-raft `Ready::released`).
        if let Some(released) = update.released {
            self.released = self.released.max(released);
            self.release_proposals();
        }
        for held in update.proposals {
            // One entry an index: what storage holds there it keeps.
            if !self
                .proposals
                .iter()
                .any(|(entry, _)| entry.index == held.0.index)
            {
                self.proposals.push(held);
            }
        }
        Ok(())
    }
    /// What a `Ready` released is held no more. Nothing else drops a
    /// proposal: not the log reaching its index, not a snapshot, not a
    /// compaction (hyper-raft `Ready::released`).
    fn release_proposals(&mut self) {
        let released = self.released;
        self.proposals.retain(|(entry, _)| entry.index > released);
    }
    /// A proposal as the log of writes states it, at opening.
    pub fn hold_proposal(&mut self, entry: Entry) -> Result<(), ConsensusError> {
        let update = self.prepare_with(&[], None, std::slice::from_ref(&entry), None)?;
        self.publish(update)
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
            self.origin = self
                .cumulative
                .pop_front()
                .ok_or(ConsensusError::Corruption("entry accounting mismatch"))?;
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
        // A compaction drops no proposal: only a `Ready`'s release does.
        Ok(())
    }
}
impl Storage for RamLog {
    fn initial_state(&self) -> Result<InitialState, StorageError> {
        Ok(InitialState {
            hard_state: self.hard_state,
            configuration: self.conf_state.clone(),
            proposals: self
                .proposals
                .iter()
                .map(|(entry, _)| entry.clone())
                .collect(),
            released: self.released,
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
        let want = usize::try_from(high.checked_sub(low).ok_or(StorageError::Unavailable)?)
            .map_err(|_| StorageError::Unavailable)?;
        let offset = self.position(low)?;
        // The page is chosen before any of it is copied: the longest
        // prefix of the range whose encoded bytes fit, and one entry at
        // least. Only that many are reserved for and cloned, so a page
        // costs what it carries, never what lies behind it.
        let taken = if max_bytes == u64::MAX {
            want
        } else {
            let mut size = 0u64;
            let mut taken = 0usize;
            for entry in self.entries.iter().skip(offset).take(want) {
                size = size.saturating_add(proto::encoded_bytes(entry));
                if taken > 0 && size > max_bytes {
                    break;
                }
                taken = taken.saturating_add(1);
            }
            taken
        };
        into.try_reserve_exact(taken)
            .map_err(|_| StorageError::LogTemporarilyUnavailable)?;
        for entry in self.entries.iter().skip(offset).take(taken) {
            let mut copy = Entry {
                entry_type: entry.entry_type,
                term: entry.term,
                index: entry.index,
                ..Entry::default()
            };
            copy.data
                .try_reserve_exact(entry.data.len())
                .map_err(|_| StorageError::LogTemporarilyUnavailable)?;
            copy.data.extend_from_slice(&entry.data);
            copy.context
                .try_reserve_exact(entry.context.len())
                .map_err(|_| StorageError::LogTemporarilyUnavailable)?;
            copy.context.extend_from_slice(&entry.context);
            into.push(copy);
        }
        Ok(())
    }
    fn any_entry(
        &self,
        low: u64,
        high: u64,
        predicate: &mut dyn FnMut(&Entry) -> bool,
    ) -> Result<bool, StorageError> {
        if low < self.first_index()? {
            return Err(StorageError::Compacted);
        }
        if low > high || high > self.last_index()?.saturating_add(1) {
            return Err(StorageError::Unavailable);
        }
        let want = usize::try_from(high.checked_sub(low).ok_or(StorageError::Unavailable)?)
            .map_err(|_| StorageError::Unavailable)?;
        let offset = self.position(low)?;
        Ok(self.entries.iter().skip(offset).take(want).any(predicate))
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tests::config;

    /// The budget is charged for the next power of two of what the log
    /// needs, so the slots grow to that: one reallocation per doubling, not
    /// one per commit moving the whole log each time.
    #[test]
    fn slots_grow_to_the_power_of_two_the_budget_was_charged_for() {
        let budget = MemoryBudget::new(64 * 1024 * 1024, 0).unwrap();
        let mut log = RamLog::new(&config(1), budget).unwrap();
        let mut capacities = Vec::new();
        for _ in 0..1_000 {
            log.reserve_slots(1).unwrap();
            let capacity = log.entries.capacity();
            if capacities.last() != Some(&capacity) {
                capacities.push(capacity);
            }
            assert!(capacity.is_power_of_two(), "{capacity}");
            assert!(log.charges.capacity() >= capacity);
            // What the slots hold is what the budget holds for them.
            let charged = log.slots.as_ref().map(|slots| slots.bytes()).unwrap_or(0);
            assert!(
                charged >= capacity * std::mem::size_of::<Entry>(),
                "{charged} charged for {capacity} slots"
            );
            log.entries.push_back(Entry::default());
            log.charges.push_back(
                reserve(&log.budget, BudgetKind::Payload, BudgetLane::Completion, 1).unwrap(),
            );
        }
        // Doublings only: 1, 2, 4, ..., 1024.
        assert!(capacities.len() <= 11, "{capacities:?}");
    }

    /// The running totals say what a walk of the entries says, through
    /// appends, a replaced suffix, compaction and a snapshot; and they say
    /// it in one subtraction, whatever the range.
    #[test]
    fn the_running_totals_say_what_a_walk_of_the_entries_says() {
        let budget = MemoryBudget::new(64 * 1024 * 1024, 0).unwrap();
        let mut log = RamLog::new(&config(1), budget).unwrap();
        let entry = |index: u64, bytes: usize| Entry {
            index,
            term: 1,
            data: vec![index as u8; bytes],
            ..Entry::default()
        };
        let walk = |log: &RamLog, low: u64, high: u64, max_entries: usize, cap: usize| {
            log.entries
                .iter()
                .filter(|entry| entry.index >= low && entry.index < high)
                .take(max_entries)
                .map(|entry| entry_bytes(entry).unwrap())
                .sum::<usize>()
                .min(cap)
        };
        let agree = |log: &RamLog| {
            let first = log.first_index().unwrap();
            let last = log.last_index().unwrap();
            for low in first.saturating_sub(2)..=last + 2 {
                for high in low..=last + 2 {
                    for (max_entries, cap) in [
                        (usize::MAX, usize::MAX),
                        (2, usize::MAX),
                        (usize::MAX, 300),
                        (1, 10),
                    ] {
                        assert_eq!(
                            log.bytes_between(low, high, max_entries, cap).unwrap(),
                            walk(log, low, high, max_entries, cap),
                            "[{low}, {high}) of {max_entries} entries under {cap}"
                        );
                    }
                }
            }
            log.validate().unwrap();
        };
        log.append(
            &(1..=8)
                .map(|index| entry(index, 100 * index as usize))
                .collect::<Vec<_>>(),
        )
        .unwrap();
        log.hard_state.commit = 4;
        agree(&log);
        // A suffix replaced: the totals of what it replaced leave with it.
        log.append(&[entry(6, 7), entry(7, 9)]).unwrap();
        assert_eq!(log.last_index().unwrap(), 7);
        agree(&log);
        // Compacted behind a checkpoint: the origin moves.
        let snapshot = Snapshot {
            metadata: Some(hyper_raft::proto::SnapshotMetadata {
                conf_state: Some(log.conf_state.clone()),
                index: 4,
                term: 1,
            }),
            ..Snapshot::default()
        };
        let prepared = log.prepare_snapshot(&snapshot).unwrap();
        log.compact_prepared(prepared).unwrap();
        assert_eq!(log.first_index().unwrap(), 5);
        agree(&log);
        log.append(&[entry(8, 1000), entry(9, 1)]).unwrap();
        agree(&log);
        // A snapshot installed: the log begins again.
        let installed = Snapshot {
            metadata: Some(hyper_raft::proto::SnapshotMetadata {
                conf_state: Some(log.conf_state.clone()),
                index: 20,
                term: 1,
            }),
            ..Snapshot::default()
        };
        log.install_snapshot(installed).unwrap();
        assert_eq!(
            log.bytes_between(1, 100, usize::MAX, usize::MAX).unwrap(),
            0
        );
        log.append(&[entry(21, 50)]).unwrap();
        agree(&log);
    }
}
