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
    /// A committed layout record precedes the candidates in the log; each
    /// holds fragments of the layout it was prepared against, so none can
    /// be published as prepared. A record of theirs that still commits is
    /// replayed from its bytes, and an exact retry finds it.
    LayoutChanged,
    /// A committed retirement record precedes the candidates in the log;
    /// each was prepared against rows that leave with the family, so none
    /// can be published as prepared. A record of theirs that still commits
    /// is replayed from its bytes, and an exact retry finds it.
    Retired,
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
                // The origin member takes its genesis-derived identity so
                // every replica names it alike (25 §4).
                let origin = range::origin_member(&record.genesis)?;
                match self.domain.as_mut() {
                    Some(Domain::Passive(core)) => core.rename_native_member(0, origin)?,
                    Some(Domain::Active(owner, _)) => owner.rename_native_member(0, origin)?,
                    None => return Err(NativeSessionError::Failed),
                }
                // The movement map starts from the genesis layout: every
                // member held by the voters at range epoch one (25 §6).
                if self.movement.is_none() {
                    let map = super::movement::map_from_layout(
                        self.ledger,
                        self.committed_core()?.native_layout().boundaries(),
                        self.limits.ranges,
                    )?;
                    self.movement = Some(super::movement::Movement::new(
                        record.genesis,
                        map,
                        self.limits.ranges,
                        self.budget.clone(),
                    )?);
                }
                Ok(())
            }
        }
    }
    /// Apply a committed layout record: inert when its epoch has passed,
    /// otherwise the same split or merge on this replica's group. A refusal
    /// here means this replica cannot hold the committed layout (its member
    /// bound is lower than the authority's) and is fail-closed. On an
    /// authority the record ends every pending candidate first.
    fn apply_layout(&mut self, data: &[u8]) -> Result<(), NativeSessionError> {
        let record = range::LayoutRecord::decode(data)?;
        if record.ledger != self.ledger {
            return Err(NativeSessionError::Corrupt);
        }
        if self.layout_change == Some(record) {
            self.layout_change = None;
        }
        if self.committed_core()?.native_layout().epoch() != record.expected_epoch {
            return Ok(());
        }
        // A layout change committed behind a transfer's begin is inert on
        // every replica alike: the map and the layout never diverge (25 §6).
        if self
            .movement
            .as_ref()
            .is_some_and(|movement| movement.pending().is_some())
        {
            return Ok(());
        }
        if !self.pending.is_empty() {
            self.resolve_suffix(SuffixEvidence::LayoutChanged)?;
        }
        let max = self.limits.recovery.native.max_ranges;
        let applied = match self.domain.as_mut() {
            Some(Domain::Passive(core)) => apply_layout_to(core, record.operation, max),
            Some(Domain::Active(owner, _)) => match record.operation {
                range::LayoutOperation::Split { at, id } => owner
                    .split_native_range(at, id)
                    .map_err(NativeSessionError::from),
                range::LayoutOperation::Merge { left } => {
                    let index = owner.native_layout().check_merge(left)?;
                    owner
                        .merge_native_range(index)
                        .map_err(NativeSessionError::from)
                }
            },
            None => return Err(NativeSessionError::Failed),
        };
        applied.map_err(|_| NativeSessionError::Corrupt)?;
        if let Some(movement) = self.movement.as_mut() {
            match record.operation {
                range::LayoutOperation::Split { at, id } => movement.split(at, id)?,
                range::LayoutOperation::Merge { left } => movement.merge(left)?,
            }
        }
        Ok(())
    }
    /// Apply a committed retirement record (26 §4): inert when the prefix
    /// it named has passed, a movement is pending, or the committed state
    /// refuses the family; otherwise the same family leaves this replica
    /// alike behind its continuation, and the record counts. On an
    /// authority the record ends every pending candidate first; the owner
    /// is reconstructed at the next readiness barrier, as after any record
    /// it did not author.
    fn apply_retirement(&mut self, data: &[u8]) -> Result<(), NativeSessionError> {
        let record = retirement::RetirementRecord::decode(data)?;
        if record.ledger != self.ledger {
            return Err(NativeSessionError::Corrupt);
        }
        if self.retirement == Some(record) {
            self.retirement = None;
        }
        if self.sequence()? != record.expected_prefix {
            return Ok(());
        }
        if self
            .movement
            .as_ref()
            .is_some_and(|movement| movement.pending().is_some())
        {
            return Ok(());
        }
        if !self.pending.is_empty() {
            self.resolve_suffix(SuffixEvidence::Retired)?;
        } else if matches!(self.domain, Some(Domain::Active(..))) {
            self.passive_for_replay()?;
        }
        // An authority applied this through its committed core: it asks for
        // a fresh readiness barrier and reconstructs its owner there.
        self.readiness_requested = None;
        self.reconstruction_needed = true;
        let Some(Domain::Passive(core)) = self.domain.as_mut() else {
            return Err(NativeSessionError::Failed);
        };
        let Ok(family) = core.retirement_family(record.root) else {
            return Ok(());
        };
        if record.through < family.through {
            return Ok(());
        }
        match core.retire_native_family(&family, record.bundle, record.bytes, record.through) {
            Ok(_) => {}
            Err(error @ (NativeError::Memory(_) | NativeError::Capacity(_))) => {
                return Err(error.into());
            }
            Err(_) => return Err(NativeSessionError::Corrupt),
        }
        self.retired_families = self.retired_families.saturating_add(1);
        Ok(())
    }
    /// Apply a committed movement record (25 §6): the coordinator's step at
    /// the current native prefix under a proof minted from the entry; inert
    /// when the committed state refuses it, the same on every replica.
    fn apply_movement(
        &mut self,
        data: &[u8],
        index: u64,
        term: u64,
    ) -> Result<(), NativeSessionError> {
        let record = movement::MovementRecord::decode(data)?;
        if record.ledger != self.ledger {
            return Err(NativeSessionError::Corrupt);
        }
        let sequence = self.sequence()?;
        let movement = self.movement.as_mut().ok_or(NativeSessionError::Corrupt)?;
        if movement.apply(&record, sequence, index, term)? {
            self.adopt_map_identities()?;
        }
        Ok(())
    }
    /// Name the layout's members as the map names them (25 §6): a
    /// transfer's replacement carries a fresh identity once it is
    /// activated, and the core's layout follows in the same apply, so
    /// records, checkpoints, layout changes and the directory all name one
    /// member alike. Members are in key order on both sides; a differing
    /// count is a divergence no replica may serve from.
    fn adopt_map_identities(&mut self) -> Result<(), NativeSessionError> {
        let Some(movement) = self.movement.as_ref() else {
            return Ok(());
        };
        let ranges = movement.coordinator.map().ranges();
        let count = ranges.len();
        for position in 0..count {
            let wanted = ranges.get(position).map(|range| range.id);
            let current = match self.domain.as_ref() {
                Some(Domain::Passive(core)) => core.native_layout(),
                Some(Domain::Active(owner, _)) => owner.native_layout(),
                None => return Err(NativeSessionError::Failed),
            };
            if current.len() != count {
                return Err(NativeSessionError::Corrupt);
            }
            let (Some(wanted), Some(held)) = (wanted, current.ids().nth(position)) else {
                return Err(NativeSessionError::Corrupt);
            };
            if wanted == held {
                continue;
            }
            match self.domain.as_mut() {
                Some(Domain::Passive(core)) => core.rename_native_member(position, wanted)?,
                Some(Domain::Active(owner, _)) => owner.rename_native_member(position, wanted)?,
                None => return Err(NativeSessionError::Failed),
            }
        }
        Ok(())
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
            self.layout_change = None;
            self.retirement = None;
            if let Some(movement) = self.movement.as_mut() {
                movement.in_flight = None;
            }
            self.observed_term = status.term;
            self.observed_leader = leader;
        }
    }
    /// Entries this engine applies: the committed genesis and native records.
    pub(crate) fn is_native_entry(data: &[u8]) -> bool {
        data.starts_with(&genesis::MAGIC)
            || data.starts_with(&record::MAGIC)
            || data.starts_with(&range::MAGIC)
            || data.starts_with(&movement::MAGIC)
            || data.starts_with(&retirement::MAGIC)
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
        if entry.data.starts_with(&range::MAGIC) {
            if self.genesis.is_none() {
                return Err(NativeSessionError::Corrupt);
            }
            self.apply_layout(&entry.data)?;
            self.applied_raft = entry.index;
            return Ok(());
        }
        if entry.data.starts_with(&movement::MAGIC) {
            if self.genesis.is_none() {
                return Err(NativeSessionError::Corrupt);
            }
            self.apply_movement(&entry.data, entry.index, entry.term)?;
            self.applied_raft = entry.index;
            return Ok(());
        }
        if entry.data.starts_with(&retirement::MAGIC) {
            if self.genesis.is_none() {
                return Err(NativeSessionError::Corrupt);
            }
            self.apply_retirement(&entry.data)?;
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
            let started = std::time::Instant::now();
            let reader = RecordingReader::new(&self.reader);
            let prepared = record::replay::prepare(
                core,
                &record,
                expected_range,
                self.limits.recovery,
                &reader,
                &self.schemas,
            );
            let missing = reader.take_missing();
            self.pending_custody = if missing.is_empty() {
                None
            } else {
                Some(PendingCustody::new(missing, &self.budget)?)
            };
            let prepared = prepared?;
            let Some(Domain::Passive(core)) = self.domain.as_mut() else {
                return Err(NativeSessionError::Failed);
            };
            let outcome = match core.publish_native(prepared) {
                Ok(outcome) => outcome,
                Err(refused) => return Err(refused.error.into()),
            };
            let elapsed = u64::try_from(started.elapsed().as_micros()).unwrap_or(u64::MAX);
            self.materializer.serial_records = self.materializer.serial_records.saturating_add(1);
            self.materializer.serial_micros =
                self.materializer.serial_micros.saturating_add(elapsed);
            outcome
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
    /// How many consecutive native records from the delivery's current entry
    /// a batch may take: none while this session has unresolved candidates
    /// (the head may be its own), none when one worker replays serially, and
    /// never across a membership entry.
    fn record_run(&self, delivery: &Delivery, first_index: u64) -> usize {
        if self.limits.materializer.max_workers <= 1 || !self.pending.is_empty() {
            return 0;
        }
        let next_membership = delivery
            .events
            .membership
            .get(delivery.membership)
            .map(|membership| membership.index);
        let mut expected = first_index;
        delivery
            .events
            .committed
            .iter()
            .skip(delivery.entry)
            .take(self.limits.materializer.max_batch)
            .take_while(|entry| {
                let native = entry.data.starts_with(&record::MAGIC)
                    && entry.index == expected
                    && next_membership.is_none_or(|index| entry.index < index);
                expected = expected.saturating_add(1);
                native
            })
            .count()
    }
    /// Materialize a run of committed records this session did not author.
    /// Returns how many were applied (a prefix, in order) and the failure that
    /// stopped the run, if any; the delivery resumes at the failed entry.
    fn apply_run(
        &mut self,
        entries: &[focal_consensus::CommittedEntry],
        applied_index: u64,
        output: &mut NativeOutput,
    ) -> (usize, Option<NativeSessionError>) {
        match self.materialize_run(entries, applied_index, output) {
            Ok((applied, failure)) => (applied, failure),
            Err(error) => (0, Some(error)),
        }
    }
    #[allow(
        clippy::type_complexity,
        reason = "the applied prefix length and the refusal that ended the run"
    )]
    fn materialize_run(
        &mut self,
        entries: &[focal_consensus::CommittedEntry],
        applied_index: u64,
        output: &mut NativeOutput,
    ) -> Result<(usize, Option<NativeSessionError>), NativeSessionError> {
        if self.genesis.is_none() || !self.pending.is_empty() {
            return Err(NativeSessionError::Corrupt);
        }
        // The same header rules `apply_entry` applies to one record, walked
        // ahead over the run so every record's producer range is known.
        let mut records = Vec::new();
        records
            .try_reserve_exact(entries.len())
            .map_err(|_| NativeSessionError::Capacity)?;
        let mut sequence = self.sequence()?;
        let mut recording_range = self.recording_range;
        let mut recording_term = self.recording_term;
        let mut expected_index = self.applied_raft;
        for entry in entries {
            expected_index = expected_index.saturating_add(1);
            if entry.index <= self.applied_raft
                || entry.index > applied_index
                || entry.index != expected_index
                || !entry.data.starts_with(&record::MAGIC)
            {
                return Err(NativeSessionError::Corrupt);
            }
            let record = record::StructuralRecord::inspect(&entry.data, self.limits.inspection)?;
            let header = record.header();
            if header.ledger != self.ledger
                || header.profile != self.profile
                || header.range.0 == 0
                || header.base != sequence
                || entry.term == 0
                || entry.term < recording_term
                || (entry.term == recording_term && recording_range != Some(header.range))
                || (recording_range.is_none() && header.base != self.records_floor)
            {
                return Err(NativeSessionError::Corrupt);
            }
            let expected_range = if entry.term == recording_term {
                recording_range.ok_or(NativeSessionError::Corrupt)?
            } else {
                header.range
            };
            records.push((record, expected_range));
            sequence = header.outcome.sequence;
            recording_range = Some(header.range);
            recording_term = entry.term;
        }
        if matches!(self.domain, Some(Domain::Active(..))) {
            self.passive_for_replay()?;
        }
        let Some(Domain::Passive(core)) = self.domain.as_mut() else {
            return Err(NativeSessionError::Failed);
        };
        let started = std::time::Instant::now();
        let reader = RecordingReader::new(&self.reader);
        let batch = record::materialize::materialize_batch(
            core,
            &records,
            self.limits.recovery,
            &reader,
            &self.schemas,
            self.limits.materializer,
        );
        let missing = reader.take_missing();
        self.pending_custody = if missing.is_empty() {
            None
        } else {
            Some(PendingCustody::new(missing, &self.budget)?)
        };
        let elapsed = u64::try_from(started.elapsed().as_micros()).unwrap_or(u64::MAX);
        let stats = &mut self.materializer;
        stats.micros = stats.micros.saturating_add(elapsed);
        stats.batches = stats.batches.saturating_add(1);
        stats.records = stats
            .records
            .saturating_add(u64::try_from(batch.report.applied).unwrap_or(u64::MAX));
        stats.waves = stats
            .waves
            .saturating_add(u64::try_from(batch.report.waves).unwrap_or(u64::MAX));
        stats.violations = stats
            .violations
            .saturating_add(u64::try_from(batch.report.violations).unwrap_or(u64::MAX));
        if batch.report.max_parallel > 1 {
            stats.parallel_batches = stats.parallel_batches.saturating_add(1);
        }
        if batch.report.serial_fallback {
            stats.serial_fallbacks = stats.serial_fallbacks.saturating_add(1);
        }
        let mut applied = 0usize;
        for (outcome, entry) in batch.outcomes.into_iter().zip(entries) {
            let record = record::StructuralRecord::inspect(&entry.data, self.limits.inspection)?;
            let header = record.header();
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
            applied = applied.saturating_add(1);
        }
        Ok((
            applied,
            batch
                .failure
                .map(|(_, error)| NativeSessionError::from(error)),
        ))
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
            // Consecutive records this session did not author, up to the next
            // membership entry, are materialized as one batch (doc 25 §2).
            let run = self.record_run(delivery, entry.index);
            let output = delivery.output.as_mut().ok_or(NativeSessionError::Failed)?;
            if run >= 2 {
                let entries = delivery
                    .events
                    .committed
                    .get(delivery.entry..add(delivery.entry, run)?)
                    .ok_or(NativeSessionError::Corrupt)?;
                let (applied, failure) = self.apply_run(entries, applied_index, output);
                delivery.entry = add(delivery.entry, applied)?;
                if let Some(error) = failure {
                    return Err(error);
                }
                continue;
            }
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

/// The same split or merge on a passive core.
fn apply_layout_to(
    core: &mut Core<NativeState>,
    operation: range::LayoutOperation,
    max: usize,
) -> Result<(), NativeSessionError> {
    match operation {
        range::LayoutOperation::Split { at, id } => {
            core.native_layout().check_split(at, id, max)?;
            Ok(core.split_native_range(at, id)?)
        }
        range::LayoutOperation::Merge { left } => {
            let index = core.native_layout().check_merge(left)?;
            Ok(core.merge_native_range(index)?)
        }
    }
}
