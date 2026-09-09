//! Committed delivery for the native engine: genesis, records, membership,
//! snapshots and read barriers in one Raft order, with the delivery retained
//! across retryable refusals and every suffix disposition backed by evidence.
use super::engine::{Domain, NativeEngine};
use super::*;
use focal_consensus::AppliedMembership;

const READINESS: &[u8; 8] = b"FCNREADY";
const CORRELATION: &[u8; 8] = b"FCNREAD1";

/// Committed native output of one delivery, funded before any entry applies.
pub(crate) struct NativeOutput {
    pub(crate) committed: Vec<NativeCommit>,
    pub(crate) reads: Vec<NativeReadBoundary>,
    allocation: Allocation,
}
impl NativeOutput {
    pub(crate) fn reserve(
        budget: &MemoryBudget,
        commits: usize,
        reads: usize,
    ) -> Result<Self, NativeSessionError> {
        let charge = add(
            array::<NativeCommit>(commits)?,
            array::<NativeReadBoundary>(reads)?,
        )?;
        let permit = budget.reserve(BudgetKind::Pending, BudgetLane::Completion, charge)?;
        Ok(Self {
            committed: reserved(commits)?,
            reads: reserved(reads)?,
            allocation: permit.commit(),
        })
    }
    pub(crate) fn into_parts(self) -> (Vec<NativeCommit>, Vec<NativeReadBoundary>, Allocation) {
        (self.committed, self.reads, self.allocation)
    }
}
pub(super) struct Delivery {
    events: NodeEvents,
    output: Option<NativeOutput>,
    snapshot: bool,
    entry: usize,
    membership: usize,
    read: usize,
}
impl Delivery {
    fn new(events: NodeEvents) -> Self {
        Self {
            events,
            output: None,
            snapshot: false,
            entry: 0,
            membership: 0,
            read: 0,
        }
    }
}
fn readiness(term: u64) -> [u8; 16] {
    let mut value = [0; 16];
    if let Some(prefix) = value.get_mut(..8) {
        prefix.copy_from_slice(READINESS);
    }
    if let Some(suffix) = value.get_mut(8..) {
        suffix.copy_from_slice(&term.to_le_bytes());
    }
    value
}
/// Proof that an unresolved speculative suffix can never commit.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum SuffixEvidence {
    ConflictingCommittedPrefix,
    InstalledSnapshot,
    NewerTermBarrier,
}
impl<S: NativeSchemaVerifier> NativeEngine<S> {
    pub(crate) fn poll(
        &mut self,
        consensus: &mut DurableNode,
    ) -> Result<NativeSessionEvents, NativeSessionError> {
        self.check()?;
        if self.delivery.is_none() {
            if consensus.checkpoint_pending() {
                consensus.finish_checkpoint()?;
            }
            self.delivery = Some(Delivery::new(consensus.drain()?));
        }
        self.deliver(consensus)
    }
    pub(crate) fn try_poll(
        &mut self,
        consensus: &mut DurableNode,
    ) -> Result<Option<NativeSessionEvents>, NativeSessionError> {
        self.check()?;
        if self.delivery.is_none() {
            if consensus.checkpoint_pending() && !consensus.try_finish_checkpoint()? {
                return Ok(None);
            }
            let Some(events) = consensus.try_drain()? else {
                return Ok(None);
            };
            self.delivery = Some(Delivery::new(events));
        }
        self.deliver(consensus).map(Some)
    }
    fn deliver(
        &mut self,
        consensus: &mut DurableNode,
    ) -> Result<NativeSessionEvents, NativeSessionError> {
        let mut delivery = self.delivery.take().ok_or(NativeSessionError::Failed)?;
        match self.advance_delivery(consensus, &mut delivery) {
            Ok(()) => {
                let (committed, read_boundaries, allocation) = delivery
                    .output
                    .ok_or(NativeSessionError::Failed)?
                    .into_parts();
                // Committed output is delivered even when a later proposal flush
                // fails; a fatal flush stops admission without asserting that any
                // in-flight operation definitely did not commit.
                let flush_refusal = self.flush_after_delivery(consensus);
                Ok(NativeSessionEvents {
                    consensus: delivery.events,
                    committed,
                    read_boundaries,
                    flush_refusal,
                    _allocation: allocation,
                })
            }
            Err(error) => {
                if error.class() == FailureClass::FailClosed {
                    self.failed = true;
                }
                self.delivery = Some(delivery);
                Err(error)
            }
        }
    }
    /// Discard every unresolved candidate and return to a passive Core. Only the
    /// listed evidence may call this; a role change alone is not evidence.
    pub(super) fn resolve_suffix(
        &mut self,
        _evidence: SuffixEvidence,
    ) -> Result<(), NativeSessionError> {
        self.ready_term = None;
        self.pending.clear();
        let domain = self.domain.take().ok_or(NativeSessionError::Failed)?;
        let core = match domain {
            Domain::Passive(core) => core,
            Domain::Active(mut owner, permit) => {
                owner.discard_all();
                match (*owner).into_committed_core() {
                    Ok(core) => core,
                    Err(refused) => {
                        self.domain = Some(Domain::Active(Box::new(refused.owner), permit));
                        return Err(NativeSessionError::Failed);
                    }
                }
            }
        };
        self.domain = Some(Domain::Passive(core));
        Ok(())
    }
    /// Convert an idle active owner back to a passive Core for replay of a
    /// committed entry this session did not author. Pending candidates must
    /// already be resolved; this is reconstruction, not disposition.
    fn passive_for_replay(&mut self) -> Result<(), NativeSessionError> {
        if !self.pending.is_empty() {
            return Err(NativeSessionError::Corrupt);
        }
        self.resolve_suffix(SuffixEvidence::ConflictingCommittedPrefix)
    }
    /// Reconstruct the active owner at this term's committed readiness barrier.
    pub(crate) fn promote(
        &mut self,
        term: u64,
        consensus: &DurableNode,
    ) -> Result<(), NativeSessionError> {
        if self.pending.iter().any(|pending| pending.term != term) {
            // The readiness barrier of this term lies at or beyond the applied
            // current-term entry; older-term candidates can no longer commit.
            if self.applied_raft == 0 || consensus.published_term(self.applied_raft)? != term {
                return Err(NativeSessionError::Corrupt);
            }
            self.resolve_suffix(SuffixEvidence::NewerTermBarrier)?;
        }
        let domain = self.domain.take().ok_or(NativeSessionError::Failed)?;
        self.domain = Some(match domain {
            Domain::Active(owner, permit) => Domain::Active(owner, permit),
            Domain::Passive(core) => {
                match NativeOwner::with_record_buffers(core, &self.schemas, self.limits.encoding) {
                    Ok(owner) => {
                        // The boxed owner is an explicitly charged session allocation;
                        // a refused charge returns the reconciled Core untouched.
                        let permit = self.budget.reserve(
                            BudgetKind::Pending,
                            BudgetLane::Completion,
                            array::<NativeOwner>(1)?,
                        );
                        match permit {
                            Ok(permit) => Domain::Active(Box::new(owner), permit.commit()),
                            Err(error) => {
                                self.domain = Some(Domain::Passive(
                                    owner
                                        .into_committed_core()
                                        .map_err(|_| NativeSessionError::Failed)?,
                                ));
                                return Err(error.into());
                            }
                        }
                    }
                    Err(refused) => {
                        self.domain = Some(Domain::Passive(refused.core));
                        return Err(refused.error.into());
                    }
                }
            }
        });
        self.reconstruction_needed = false;
        self.ready_term = Some(term);
        Ok(())
    }
    pub(crate) fn apply_membership(
        &mut self,
        membership: &AppliedMembership,
        applied_index: u64,
    ) -> Result<(), NativeSessionError> {
        if membership.index <= self.configuration_index
            || membership.index > applied_index
            || membership.index <= self.applied_raft
        {
            return Err(NativeSessionError::Corrupt);
        }
        self.configuration_index = membership.index;
        self.applied_raft = membership.index;
        Ok(())
    }
    fn apply_genesis(
        &mut self,
        data: &[u8],
        index: u64,
        consensus: &DurableNode,
    ) -> Result<(), NativeSessionError> {
        let record = genesis::Genesis::decode(data)?;
        let expected = genesis::Genesis::derive(
            consensus.cluster_id(),
            consensus.group_id(),
            self.ledger,
            self.profile,
            crate::native_checkpoint::format_hash(),
        );
        if record != expected {
            return Err(NativeSessionError::Corrupt);
        }
        match self.genesis {
            // A duplicate identical genesis from a concurrent leader is inert.
            Some(existing) if existing == record.genesis => Ok(()),
            Some(_) => Err(NativeSessionError::Corrupt),
            None => {
                self.genesis = Some(record.genesis);
                // A hosted engine already carries its activation record's
                // index; the standalone engine descends from its genesis entry.
                if self.activation_index == 0 {
                    self.activation_index = index;
                }
                Ok(())
            }
        }
    }
    fn propose_genesis(&mut self, consensus: &mut DurableNode) -> Result<(), NativeSessionError> {
        let record = genesis::Genesis::derive(
            consensus.cluster_id(),
            consensus.group_id(),
            self.ledger,
            self.profile,
            crate::native_checkpoint::format_hash(),
        );
        let mut bytes = [0u8; genesis::BYTES];
        record.write_into(&mut bytes);
        let _permit = self.budget.reserve(
            BudgetKind::Pending,
            BudgetLane::Completion,
            array::<u8>(genesis::BYTES)?,
        )?;
        consensus.propose_borrowed_in(&bytes, BudgetLane::Completion)?;
        Ok(())
    }
    /// Term and role observation. A change alone leaves unresolved candidates
    /// and grants intact; only committed evidence may discard them.
    pub(crate) fn observe(&mut self, status: &NodeStatus) {
        let leader = status.role == StateRole::Leader;
        if status.term != self.observed_term || leader != self.observed_leader {
            self.ready_term = None;
            self.readiness_requested = None;
            self.reconstruction_needed = true;
            self.genesis_proposed = false;
            self.observed_term = status.term;
            self.observed_leader = leader;
        }
    }
    /// Entries this engine applies: the committed genesis and native records.
    pub(crate) fn is_native_entry(data: &[u8]) -> bool {
        data.starts_with(&genesis::MAGIC) || data.starts_with(&record::MAGIC)
    }
    /// Caller-issued read barriers carry this correlation namespace.
    pub(crate) fn is_correlated_read(context: &[u8]) -> bool {
        context.starts_with(CORRELATION)
    }
    /// The Raft prefix and configuration a hosting Session had already applied
    /// when this engine was activated inside it.
    pub(crate) fn adopt_prefix(&mut self, applied_raft: u64, configuration_index: u64) {
        self.applied_raft = applied_raft;
        self.configuration_index = configuration_index;
    }
    /// Adopt the translated legacy prefix as native prefix one (23 §5.1). No
    /// record produced it, so no recording range exists; the owner is rebuilt
    /// from this core at the next readiness barrier like a restored checkpoint.
    pub(crate) fn install_imported(
        &mut self,
        core: Core<NativeState>,
        applied_raft: u64,
        configuration_index: u64,
    ) -> Result<(), NativeSessionError> {
        if core.native_sequence() != SessionSeq(1) || !self.pending.is_empty() {
            return Err(NativeSessionError::Corrupt);
        }
        self.reconstruction_needed = true;
        self.domain = Some(Domain::Passive(core));
        self.recording_range = None;
        self.recording_term = 0;
        self.records_floor = SessionSeq(1);
        self.applied_raft = applied_raft;
        self.configuration_index = configuration_index;
        Ok(())
    }
    /// Apply one committed genesis or native record in Raft order.
    pub(crate) fn apply_entry(
        &mut self,
        entry: &focal_consensus::CommittedEntry,
        applied_index: u64,
        consensus: &DurableNode,
        output: &mut NativeOutput,
    ) -> Result<(), NativeSessionError> {
        if entry.index <= self.applied_raft || entry.index > applied_index {
            return Err(NativeSessionError::Corrupt);
        }
        if entry.data.starts_with(&genesis::MAGIC) {
            self.apply_genesis(&entry.data, entry.index, consensus)?;
            self.applied_raft = entry.index;
            return Ok(());
        }
        if !entry.data.starts_with(&record::MAGIC) {
            return Err(NativeSessionError::Legacy);
        }
        if self.genesis.is_none() {
            return Err(NativeSessionError::Corrupt);
        }
        let record = record::StructuralRecord::inspect(&entry.data, self.limits.inspection)?;
        let header = record.header();
        if header.ledger != self.ledger
            || header.profile != self.profile
            || header.range.0 == 0
            || header.base != self.sequence()?
            || entry.term == 0
            || entry.term < self.recording_term
            || (entry.term == self.recording_term && self.recording_range != Some(header.range))
            || (self.recording_range.is_none() && header.base != self.records_floor)
        {
            return Err(NativeSessionError::Corrupt);
        }
        let matches = self.pending.front().is_some_and(|pending| {
            pending.submitted
                && pending.hash == Some(header.hash)
                && pending.outcome == header.outcome
                && header.range == self.range
        });
        let outcome = if matches {
            let candidate = self
                .pending
                .front()
                .ok_or(NativeSessionError::Corrupt)?
                .candidate;
            let Some(Domain::Active(owner, _)) = self.domain.as_mut() else {
                return Err(NativeSessionError::Corrupt);
            };
            let outcome = owner.publish_after_durable(candidate)?;
            self.pending.pop_front();
            outcome
        } else {
            if !self.pending.is_empty() {
                // A different record at our committed prefix: the whole suffix
                // is conflicting speculation.
                self.resolve_suffix(SuffixEvidence::ConflictingCommittedPrefix)?;
            } else if matches!(self.domain, Some(Domain::Active(..))) {
                self.passive_for_replay()?;
            }
            let Some(Domain::Passive(core)) = self.domain.as_mut() else {
                return Err(NativeSessionError::Failed);
            };
            // Within a recording term the producer range is bound; a new term
            // may introduce a new producer only through this committed entry.
            let expected_range = if entry.term == self.recording_term {
                self.recording_range.ok_or(NativeSessionError::Corrupt)?
            } else {
                header.range
            };
            let prepared = record::replay::prepare(
                core,
                &record,
                expected_range,
                self.limits.recovery,
                &self.reader,
                &self.schemas,
            )?;
            match core.publish_native(prepared) {
                Ok(outcome) => outcome,
                Err(refused) => return Err(refused.error.into()),
            }
        };
        if output.committed.len() == output.committed.capacity() {
            return Err(NativeSessionError::Capacity);
        }
        output.committed.push(NativeCommit {
            raft_index: entry.index,
            raft_term: entry.term,
            record_hash: header.hash,
            outcome,
        });
        self.applied_raft = entry.index;
        self.recording_range = Some(header.range);
        self.recording_term = entry.term;
        Ok(())
    }
    /// Every entry of the delivery has been applied; the Raft prefix includes
    /// entries this engine never sees (no-ops, ancillary metadata).
    pub(crate) fn finish_entries(&mut self, applied_index: u64) -> Result<(), NativeSessionError> {
        if applied_index < self.applied_raft {
            return Err(NativeSessionError::Corrupt);
        }
        self.applied_raft = applied_index;
        Ok(())
    }
    /// A completed caller-correlated read barrier becomes a boundary naming the
    /// applied Raft index and native sequence at which the read may be served.
    pub(crate) fn apply_correlated_read(
        &mut self,
        barrier: &focal_consensus::ReadBarrier,
        output: &mut NativeOutput,
    ) -> Result<(), NativeSessionError> {
        if barrier.index > self.applied_raft {
            return Err(NativeSessionError::Corrupt);
        }
        let correlation = barrier
            .context
            .strip_prefix(CORRELATION.as_slice())
            .and_then(|bytes| <[u8; 16]>::try_from(bytes).ok())
            .ok_or(NativeSessionError::Corrupt)?;
        let boundary = NativeReadBoundary {
            correlation: ReadCorrelation(correlation),
            raft_index: barrier.index,
            native_sequence: self.sequence()?,
        };
        if output.reads.len() == output.reads.capacity() {
            return Err(NativeSessionError::Capacity);
        }
        output.reads.push(boundary);
        Ok(())
    }
    /// After a delivery: a fully applied newer-term entry proves older-term
    /// candidates dead, an idle follower returns to a passive Core, and a ready
    /// leader without a genesis proposes it.
    pub(crate) fn settle(
        &mut self,
        status: &NodeStatus,
        consensus: &mut DurableNode,
    ) -> Result<(), NativeSessionError> {
        let leader = status.role == StateRole::Leader;
        if self
            .pending
            .iter()
            .any(|pending| pending.term != status.term)
            && self.applied_raft != 0
            && consensus.has_committed_current_term()
            && consensus.published_term(self.applied_raft)? == status.term
        {
            self.resolve_suffix(SuffixEvidence::NewerTermBarrier)?;
        }
        if !leader
            && self.reconstruction_needed
            && self.pending.is_empty()
            && matches!(self.domain, Some(Domain::Active(..)))
        {
            self.passive_for_replay()?;
        }
        if leader
            && self.ready_term == Some(status.term)
            && self.genesis.is_none()
            && !self.genesis_proposed
        {
            self.propose_genesis(consensus)?;
            self.genesis_proposed = true;
        }
        Ok(())
    }
    /// Flush admitted candidates once a delivery completes on an authority. A
    /// fatal refusal stops admission without asserting that any in-flight
    /// operation definitely did not commit; the refusal is reported, not lost.
    pub(crate) fn flush_after_delivery(
        &mut self,
        consensus: &mut DurableNode,
    ) -> Option<NativeSessionError> {
        if !self.is_authoritative(&consensus.status()) {
            return None;
        }
        match self.flush_proposals(consensus) {
            Ok(()) => None,
            Err(error) => {
                if error.class() == FailureClass::FailClosed {
                    self.failed = true;
                }
                Some(error)
            }
        }
    }
    fn advance_delivery(
        &mut self,
        consensus: &mut DurableNode,
        delivery: &mut Delivery,
    ) -> Result<(), NativeSessionError> {
        let status = consensus.status();
        let leader = status.role == StateRole::Leader;
        self.observe(&status);
        if delivery.output.is_none() {
            delivery.output = Some(NativeOutput::reserve(
                &self.budget,
                delivery.events.committed.len(),
                delivery.events.read_states.len(),
            )?);
        }
        if !delivery.snapshot {
            if let Some(snapshot) = &delivery.events.snapshot {
                self.restore_snapshot(snapshot, consensus)?;
            }
            delivery.snapshot = true;
        }
        let applied_index = delivery.events.applied_index;
        while let Some(entry) = delivery.events.committed.get(delivery.entry) {
            // Membership and native entries share one Raft order.
            while let Some(membership) = delivery.events.membership.get(delivery.membership)
                && membership.index < entry.index
            {
                self.apply_membership(membership, applied_index)?;
                delivery.membership = add(delivery.membership, 1)?;
            }
            let output = delivery.output.as_mut().ok_or(NativeSessionError::Failed)?;
            self.apply_entry(entry, applied_index, consensus, output)?;
            delivery.entry = add(delivery.entry, 1)?;
        }
        while let Some(membership) = delivery.events.membership.get(delivery.membership) {
            self.apply_membership(membership, applied_index)?;
            delivery.membership = add(delivery.membership, 1)?;
        }
        self.finish_entries(applied_index)?;
        while let Some(barrier) = delivery.events.read_states.get(delivery.read) {
            if barrier.index > self.applied_raft {
                return Err(NativeSessionError::Corrupt);
            }
            if barrier.context == readiness(status.term) {
                if leader {
                    self.promote(status.term, consensus)?;
                }
            } else {
                let output = delivery.output.as_mut().ok_or(NativeSessionError::Failed)?;
                self.apply_correlated_read(barrier, output)?;
            }
            delivery.read = add(delivery.read, 1)?;
        }
        self.settle(&status, consensus)?;
        if leader
            && consensus.has_committed_current_term()
            && self.ready_term != Some(status.term)
            && self.readiness_requested != Some(status.term)
        {
            self.request_read(consensus, &readiness(status.term))?;
            self.readiness_requested = Some(status.term);
        }
        Ok(())
    }
    fn request_read(
        &mut self,
        consensus: &mut DurableNode,
        context: &[u8],
    ) -> Result<(), NativeSessionError> {
        if context.is_empty() || context.len() > 1024 {
            return Err(NativeSessionError::Capacity);
        }
        let _permit = self.budget.reserve(
            BudgetKind::Pending,
            BudgetLane::Completion,
            array::<u8>(context.len())?,
        )?;
        let mut copied = reserved(context.len())?;
        copied.extend_from_slice(context);
        consensus.read_index(copied)?;
        Ok(())
    }
    /// Request a quorum read barrier tagged with the caller's correlation. The
    /// completion arrives in a later poll as a `NativeReadBoundary`; the readiness
    /// namespace is internal and cannot be requested here.
    pub(crate) fn read_index(
        &mut self,
        consensus: &mut DurableNode,
        correlation: ReadCorrelation,
    ) -> Result<(), NativeSessionError> {
        self.require_authority(&consensus.status())?;
        let mut context = [0u8; 24];
        if let Some(prefix) = context.get_mut(..8) {
            prefix.copy_from_slice(CORRELATION);
        }
        if let Some(suffix) = context.get_mut(8..) {
            suffix.copy_from_slice(&correlation.0);
        }
        self.request_read(consensus, &context)
    }
}
