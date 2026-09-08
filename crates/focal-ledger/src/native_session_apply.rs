use super::*;
struct Output {
    committed: Vec<NativeCommit>,
    reads: Vec<NativeReadBoundary>,
    allocation: Allocation,
}
pub(super) struct Delivery {
    events: NodeEvents,
    output: Option<Output>,
    snapshot: bool,
    entry: usize,
    membership: usize,
    read: usize,
}
impl Delivery {
    fn new(events: NodeEvents) -> Self {
        Self { events, output: None, snapshot: false, entry: 0, membership: 0, read: 0 }
    }
}
fn readiness(term: u64) -> [u8; 16] {
    let mut value = [0; 16];
    if let Some(prefix) = value.get_mut(..8) { prefix.copy_from_slice(b"FCNREADY"); }
    if let Some(suffix) = value.get_mut(8..) { suffix.copy_from_slice(&term.to_le_bytes()); }
    value
}
pub(super) fn retryable(error: &NativeSessionError) -> bool {
    matches!(error, NativeSessionError::Capacity | NativeSessionError::Memory(_)
        | NativeSessionError::Native(NativeError::Memory(_) | NativeError::Capacity(_))
        | NativeSessionError::Owner(NativeOwnerError::Native(NativeError::Memory(_) | NativeError::Capacity(_)))
        | NativeSessionError::Consensus(ConsensusError::Capacity | ConsensusError::PersistencePending))
}
impl<S: NativeSchemaVerifier> NativeSession<S> {
    pub fn poll(&mut self) -> Result<NativeSessionEvents, NativeSessionError> {
        self.check()?;
        if self.delivery.is_none() {
            if self.consensus.checkpoint_pending() { self.consensus.finish_checkpoint()?; }
            self.delivery = Some(Delivery::new(self.consensus.drain()?));
        }
        self.deliver()
    }
    pub fn try_poll(&mut self) -> Result<Option<NativeSessionEvents>, NativeSessionError> {
        self.check()?;
        if self.delivery.is_none() {
            if self.consensus.checkpoint_pending() && !self.consensus.try_finish_checkpoint()? { return Ok(None); }
            let Some(events) = self.consensus.try_drain()? else { return Ok(None); };
            self.delivery = Some(Delivery::new(events));
        }
        self.deliver().map(Some)
    }
    fn deliver(&mut self) -> Result<NativeSessionEvents, NativeSessionError> {
        let mut delivery = self.delivery.take().ok_or(NativeSessionError::Failed)?;
        match self.advance_delivery(&mut delivery) {
            Ok(()) => {
                let output = delivery.output.ok_or(NativeSessionError::Failed)?;
                if self.is_authoritative() {
                    if let Err(error) = self.flush_proposals() {
                        if !retryable(&error) { self.failed = true; return Err(error); }
                    }
                }
                Ok(NativeSessionEvents { consensus: delivery.events, committed: output.committed,
                    read_boundaries: output.reads, _allocation: output.allocation })
            }
            Err(error) => {
                if !retryable(&error) { self.failed = true; }
                self.delivery = Some(delivery);
                Err(error)
            }
        }
    }
    /// Only a snapshot, conflicting committed prefix, or fully applied
    /// current-term barrier may resolve the speculative suffix.
    pub(super) fn demote_resolved(&mut self) -> Result<(), NativeSessionError> {
        self.ready_term = None;
        self.pending.clear();
        let domain = self.domain.take().ok_or(NativeSessionError::Failed)?;
        let core = match domain {
            Domain::Passive(core) => core,
            Domain::Active(mut owner) => {
                owner.discard_all();
                match owner.into_committed_core() {
                    Ok(core) => core,
                    Err(refused) => {
                        self.domain = Some(Domain::Active(refused.owner));
                        return Err(NativeSessionError::Failed);
                    }
                }
            }
        };
        self.domain = Some(Domain::Passive(core));
        Ok(())
    }
    fn promote(&mut self, term: u64) -> Result<(), NativeSessionError> {
        if self.reconstruction_needed { self.demote_resolved()?; }
        let domain = self.domain.take().ok_or(NativeSessionError::Failed)?;
        self.domain = Some(match domain {
            Domain::Active(owner) => Domain::Active(owner),
            Domain::Passive(core) => match NativeOwner::with_record_buffers(core, &self.schemas, self.limits.encoding) {
                Ok(owner) => Domain::Active(owner),
                Err(refused) => {
                    self.domain = Some(Domain::Passive(refused.core));
                    return Err(refused.error.into());
                }
            }
        });
        self.reconstruction_needed = false;
        self.ready_term = Some(term);
        Ok(())
    }
    fn advance_delivery(&mut self, delivery: &mut Delivery) -> Result<(), NativeSessionError> {
        let status = self.status();
        let leader = status.role == StateRole::Leader;
        if status.term != self.observed_term || leader != self.observed_leader {
            self.ready_term = None;
            self.readiness_requested = None;
            self.reconstruction_needed = true;
            self.observed_term = status.term;
            self.observed_leader = leader;
            // Role change alone leaves unresolved candidates and grants intact.
            if self.pending.is_empty() { self.demote_resolved()?; }
        }
        if delivery.output.is_none() {
            let charge = add(array::<NativeCommit>(delivery.events.committed.len())?, array::<NativeReadBoundary>(delivery.events.read_states.len())?)?;
            let permit = self.budget.reserve(BudgetKind::Pending, BudgetLane::Completion, charge)?;
            delivery.output = Some(Output { committed: reserved(delivery.events.committed.len())?, reads: reserved(delivery.events.read_states.len())?, allocation: permit.commit() });
        }
        if !delivery.snapshot {
            if let Some(snapshot) = &delivery.events.snapshot { self.restore_snapshot(snapshot)?; }
            delivery.snapshot = true;
        }
        while let Some(membership) = delivery.events.membership.get(delivery.membership) {
            if membership.index <= self.configuration_index || membership.index > delivery.events.applied_index { return Err(NativeSessionError::Corrupt); }
            self.configuration_index = membership.index;
            delivery.membership = add(delivery.membership, 1)?;
        }
        while let Some(entry) = delivery.events.committed.get(delivery.entry) {
            if entry.index <= self.applied_raft || entry.index > delivery.events.applied_index { return Err(NativeSessionError::Corrupt); }
            if !entry.data.starts_with(&record::MAGIC) { return Err(NativeSessionError::Legacy); }
            let record = record::StructuralRecord::inspect(&entry.data, self.limits.inspection)?;
            let header = record.header();
            if header.ledger != self.ledger || header.profile != self.profile || header.range.0 == 0
                || header.base != self.sequence()? || entry.term == 0 || entry.term < self.recording_term
                || (entry.term == self.recording_term && self.recording_range != Some(header.range))
                || (self.recording_range.is_none() && header.base != SessionSeq(0)) { return Err(NativeSessionError::Corrupt); }
            let matches = self.pending.front().is_some_and(|pending| pending.submitted && pending.hash == Some(header.hash) && pending.outcome == header.outcome);
            let outcome = if matches {
                let pending = self.pending.front().ok_or(NativeSessionError::Corrupt)?;
                let Some(Domain::Active(owner)) = self.domain.as_mut() else { return Err(NativeSessionError::Corrupt); };
                let outcome = owner.publish_after_durable(pending.candidate)?;
                self.pending.pop_front();
                outcome
            } else {
                self.demote_resolved()?;
                let Some(Domain::Passive(core)) = self.domain.as_mut() else { return Err(NativeSessionError::Failed); };
                let expected_range = if entry.term == self.recording_term { self.recording_range.ok_or(NativeSessionError::Corrupt)? } else { header.range };
                let prepared = record::replay::prepare(core, &record, expected_range, self.limits.recovery, &self.store, &self.schemas)?;
                match core.publish_native(prepared) { Ok(outcome) => outcome, Err(refused) => return Err(refused.error.into()) }
            };
            let output = delivery.output.as_mut().ok_or(NativeSessionError::Failed)?;
            if output.committed.len() == output.committed.capacity() { return Err(NativeSessionError::Capacity); }
            output.committed.push(NativeCommit { raft_index: entry.index, raft_term: entry.term, record_hash: header.hash, outcome });
            self.applied_raft = entry.index;
            self.recording_range = Some(header.range);
            self.recording_term = entry.term;
            delivery.entry = add(delivery.entry, 1)?;
        }
        if delivery.events.applied_index < self.applied_raft { return Err(NativeSessionError::Corrupt); }
        self.applied_raft = delivery.events.applied_index;
        while let Some(barrier) = delivery.events.read_states.get(delivery.read) {
            if barrier.index > self.applied_raft { return Err(NativeSessionError::Corrupt); }
            if leader && barrier.context == readiness(status.term) { self.promote(status.term)?; }
            let boundary = NativeReadBoundary { raft_index: barrier.index, native_sequence: self.sequence()? };
            let output = delivery.output.as_mut().ok_or(NativeSessionError::Failed)?;
            if output.reads.len() == output.reads.capacity() { return Err(NativeSessionError::Capacity); }
            output.reads.push(boundary);
            delivery.read = add(delivery.read, 1)?;
        }
        if !leader && self.reconstruction_needed && self.consensus.has_committed_current_term()
            && self.applied_raft != 0 && self.consensus.published_term(self.applied_raft)? == status.term { self.demote_resolved()?; }
        if leader && self.consensus.has_committed_current_term() && self.ready_term != Some(status.term) && self.readiness_requested != Some(status.term) {
            self.request_read(&readiness(status.term))?;
            self.readiness_requested = Some(status.term);
        }
        Ok(())
    }
    fn request_read(&mut self, context: &[u8]) -> Result<(), NativeSessionError> {
        if context.is_empty() || context.len() > 1024 { return Err(NativeSessionError::Capacity); }
        let _permit = self.budget.reserve(BudgetKind::Pending, BudgetLane::Completion, array::<u8>(context.len())?)?;
        let mut copied = reserved(context.len())?;
        copied.extend_from_slice(context);
        self.consensus.read_index(copied)?;
        Ok(())
    }
    pub fn read_index(&mut self, context: &[u8]) -> Result<(), NativeSessionError> {
        self.require_authority()?;
        if context.starts_with(b"FCNREADY") { return Err(NativeSessionError::Corrupt); }
        self.request_read(context)
    }
}
