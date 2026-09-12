//! One outstanding durable Ready per group. The pinned RawNode::ready contract
//! forbids mutation before advance; other groups remain independently runnable.
use super::*;
use focal_log::WalAppend;
use raft::{LightReady, Ready};
use storage::PreparedUpdate;

pub(super) struct PendingDrain {
    events: NodeEvents,
    delivered: u64,
    phase: Phase,
}
enum Phase {
    Start,
    Ready(Box<ReadyPhase>),
    Light(Box<LightPhase>),
}
struct LightPhase {
    light: LightReady,
    hard_state: HardState,
    record: Option<Record>,
    receipt: Option<WalAppend>,
}
// Only an outstanding Ready allocates this owned slot. Its size and payload
// copies are covered by the pre-reserved staging permit, keeping idle nodes small.
struct ReadyPhase {
    ready: Ready,
    prepared: PreparedUpdate,
    records: Vec<Record>,
    receipt: Option<WalAppend>,
}
impl DurableNode {
    pub fn shared_wal(&self) -> SharedWal {
        self.wal.shared_wal()
    }
    /// True from Ready acquisition until its full output prefix is released.
    /// Mutations return PersistencePending in this state; no Raft input is lost.
    pub fn persistence_pending(&self) -> bool {
        self.persistence.is_some() || self.checkpoint.is_some() || self.decoder_write.is_some()
    }
    /// Read-only owner wake predicate, including a retained WAL receipt and
    /// newly queued Ready work that has not yet started persistence.
    pub fn has_ready(&self) -> bool {
        self.persistence.is_some()
            || self.checkpoint.is_some()
            || self.decoder_write.is_some()
            || self.raw.has_ready()
            || self.recovered_events.is_some()
            || self.recovered_snapshot.is_some()
    }

    /// Queue/poll one group's durability without waiting for the physical writer.
    /// None retains the exact Ready, preparation, output and accounting permit.
    /// The caller can poll other groups on the same owner before trying again.
    pub fn try_drain(&mut self) -> Result<Option<NodeEvents>, ConsensusError> {
        self.poll_drain(false)
    }

    /// Synchronous compatibility path over the same persistence state machine.
    /// Waits on each exact receipt without a timer, spin loop, or runtime bridge.
    pub fn drain(&mut self) -> Result<NodeEvents, ConsensusError> {
        self.poll_drain(true)?
            .ok_or(ConsensusError::PersistencePending)
    }

    fn poll_drain(&mut self, blocking: bool) -> Result<Option<NodeEvents>, ConsensusError> {
        self.check()?;
        if self.decoder_write.is_some() && !self.poll_decoder_floor(blocking)? {
            return Ok(None);
        }
        if self.checkpoint.is_some() {
            return Err(ConsensusError::PersistencePending);
        }
        if self.persistence.is_none() {
            let membership_pending = self
                .raw
                .raft
                .raft_log
                .unstable
                .entries
                .iter()
                .rev()
                .take_while(|entry| entry.index > self.delivered_index)
                .any(|entry| entry.get_entry_type() != EntryType::EntryNormal)
                || self
                    .raw
                    .store()
                    .entries
                    .iter()
                    .rev()
                    .take_while(|entry| entry.index > self.delivered_index)
                    .any(|entry| entry.get_entry_type() != EntryType::EntryNormal);
            let bytes = memory::staging_bytes(
                &self.raw,
                &self.config,
                0,
                if membership_pending { 1024 } else { 0 },
            )?;
            // Nothing has been taken from Raft yet: a refused staging reservation
            // leaves the replica exactly as it was, so the caller retries once
            // memory returns instead of losing the node to a transient shortage.
            self.active_allocation = Some(memory::reserve(
                &self.budget,
                BudgetKind::Pending,
                BudgetLane::Completion,
                bytes,
            )?);
            let events = self.recovered_events.take().unwrap_or_else(|| NodeEvents {
                snapshot: self.recovered_snapshot.take(),
                allocation: self.recovered_allocation.take(),
                ..Default::default()
            });
            self.persistence = Some(PendingDrain {
                events,
                delivered: self.delivered_index,
                phase: Phase::Start,
            });
        }
        let result = catch_unwind(AssertUnwindSafe(|| self.drain_progress(blocking)));
        match result {
            Ok(Ok(events)) => Ok(events),
            Ok(Err(error)) => {
                self.failed = true;
                Err(error)
            }
            Err(_) => {
                self.failed = true;
                Err(ConsensusError::DependencyFailure)
            }
        }
    }

    fn drain_progress(&mut self, blocking: bool) -> Result<Option<NodeEvents>, ConsensusError> {
        let mut pending = self.persistence.take().ok_or(ConsensusError::Failed)?;
        loop {
            match pending.phase {
                Phase::Start => {
                    if !self.raw.has_ready() {
                        pending.events.applied_index = pending.delivered;
                        let bytes = memory::events_bytes(&pending.events)?;
                        pending.events.allocation = Some(
                            self.active_allocation
                                .as_mut()
                                .ok_or(ConsensusError::Failed)?
                                .split_off(bytes)
                                .map_err(|_| ConsensusError::Capacity)?,
                        );
                        let retained = memory::raw_bytes(&self.raw)?;
                        let mut allocation = self
                            .active_allocation
                            .take()
                            .ok_or(ConsensusError::Failed)?;
                        if allocation.shrink_to(retained).is_err() {
                            self.active_allocation = Some(allocation);
                            return Err(ConsensusError::Capacity);
                        }
                        self.raw_allocation = Some(allocation);
                        self.delivered_index = pending.delivered;
                        return Ok(Some(pending.events));
                    }
                    let ready = self.raw.ready();
                    let mut records = Vec::new();
                    // The record count is known: entries plus an optional snapshot
                    // and hard state. Reserve once so the ready cycle never grows.
                    records
                        .try_reserve_exact(ready.entries().len().saturating_add(2))
                        .map_err(|_| ConsensusError::Capacity)?;
                    if !ready.snapshot().is_empty() {
                        records.push(proto_record(
                            self.config.group_id,
                            RecordKind::Snapshot,
                            ready.snapshot().get_metadata().index,
                            ready.snapshot().get_metadata().term,
                            ready.snapshot(),
                        )?);
                    }
                    for entry in ready.entries() {
                        records.push(proto_record(
                            self.config.group_id,
                            RecordKind::Entry,
                            entry.index,
                            entry.term,
                            entry,
                        )?);
                    }
                    if let Some(hs) = ready.hs() {
                        records.push(proto_record(
                            self.config.group_id,
                            RecordKind::HardState,
                            hs.commit,
                            hs.term,
                            hs,
                        )?);
                    }
                    let snapshot = (!ready.snapshot().is_empty()).then_some(ready.snapshot());
                    self.wal.validate_append(&records)?;
                    let prepared = self.raw.mut_store().prepare(ready.entries(), snapshot)?;
                    pending.phase = Phase::Ready(Box::new(ReadyPhase {
                        ready,
                        prepared,
                        records,
                        receipt: None,
                    }));
                }
                Phase::Ready(mut work) => {
                    if !work.records.is_empty() && work.receipt.is_none() {
                        match self
                            .wal
                            .append_async_in(&work.records, BudgetLane::Completion)
                        {
                            Ok(receipt) => {
                                work.records.clear();
                                work.receipt = Some(receipt);
                                if !blocking {
                                    pending.phase = Phase::Ready(work);
                                    self.persistence = Some(pending);
                                    return Ok(None);
                                }
                            }
                            Err(focal_log::LogError::Capacity) => {
                                pending.phase = Phase::Ready(work);
                                self.persistence = Some(pending);
                                return Ok(None);
                            }
                            Err(error) => return Err(error.into()),
                        }
                    }
                    if let Some(ticket) = work.receipt.as_mut() {
                        let completed = if blocking {
                            Some(ticket.wait_blocking())
                        } else {
                            ticket.try_complete()
                        };
                        match completed {
                            None => {
                                pending.phase = Phase::Ready(work);
                                self.persistence = Some(pending);
                                return Ok(None);
                            }
                            Some(result) => {
                                result?;
                            }
                        }
                    }
                    let ReadyPhase {
                        mut ready,
                        prepared,
                        ..
                    } = *work;
                    self.raw.mut_store().publish(prepared)?;
                    if !ready.snapshot().is_empty() {
                        pending.delivered = ready.snapshot().get_metadata().index;
                        pending.events.committed.clear();
                        pending.events.membership.clear();
                        pending.events.snapshot = Some(snapshot_event(ready.snapshot()));
                    }
                    if let Some(hs) = ready.hs() {
                        self.raw.mut_store().hard_state = hs.clone();
                    }
                    pending.events.messages.extend(ready.take_messages());
                    pending
                        .events
                        .messages
                        .extend(ready.take_persisted_messages());
                    pending
                        .events
                        .read_states
                        .extend(
                            ready
                                .take_read_states()
                                .into_iter()
                                .map(|read| ReadBarrier {
                                    index: read.index,
                                    context: read.request_ctx.to_vec(),
                                }),
                        );
                    self.apply_entries(
                        ready.take_committed_entries(),
                        &mut pending.events,
                        &mut pending.delivered,
                    )?;
                    let light = self.raw.advance_append(ready);
                    if let Some(commit) = light.commit_index() {
                        let mut hard_state = self.raw.store().hard_state.clone();
                        hard_state.commit = commit;
                        let record = proto_record(
                            self.config.group_id,
                            RecordKind::HardState,
                            commit,
                            hard_state.term,
                            &hard_state,
                        )?;
                        self.wal.validate_append(std::slice::from_ref(&record))?;
                        pending.phase = Phase::Light(Box::new(LightPhase {
                            light,
                            hard_state,
                            record: Some(record),
                            receipt: None,
                        }));
                    } else {
                        self.finish_light(light, &mut pending.events, &mut pending.delivered)?;
                        pending.phase = Phase::Start;
                    }
                }
                Phase::Light(mut work) => {
                    if let Some(encoded) = work.record.as_ref() {
                        match self
                            .wal
                            .append_async_in(std::slice::from_ref(encoded), BudgetLane::Completion)
                        {
                            Ok(ticket) => {
                                work.record = None;
                                work.receipt = Some(ticket);
                                if !blocking {
                                    pending.phase = Phase::Light(work);
                                    self.persistence = Some(pending);
                                    return Ok(None);
                                }
                            }
                            Err(focal_log::LogError::Capacity) => {
                                pending.phase = Phase::Light(work);
                                self.persistence = Some(pending);
                                return Ok(None);
                            }
                            Err(error) => return Err(error.into()),
                        }
                    }
                    let ticket = work.receipt.as_mut().ok_or(ConsensusError::Failed)?;
                    let completed = if blocking {
                        Some(ticket.wait_blocking())
                    } else {
                        ticket.try_complete()
                    };
                    match completed {
                        None => {
                            pending.phase = Phase::Light(work);
                            self.persistence = Some(pending);
                            return Ok(None);
                        }
                        Some(result) => {
                            result?;
                        }
                    }
                    self.raw.mut_store().hard_state = work.hard_state;
                    self.finish_light(work.light, &mut pending.events, &mut pending.delivered)?;
                    pending.phase = Phase::Start;
                }
            }
        }
    }
    fn finish_light(
        &mut self,
        mut light: LightReady,
        events: &mut NodeEvents,
        delivered: &mut u64,
    ) -> Result<(), ConsensusError> {
        events.messages.extend(light.take_messages());
        self.apply_entries(light.take_committed_entries(), events, delivered)?;
        self.raw.advance_apply_to(*delivered);
        Ok(())
    }
}
