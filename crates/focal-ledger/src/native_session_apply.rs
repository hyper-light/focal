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
        let permit = budget.reserve(BudgetKind::Pending, BudgetLane::Committed, charge)?;
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
                    self.fail_closed(&error);
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
        self.encoded = 0;
        self.submitted = 0;
        self.flush_stalled = false;
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
        #[cfg(test)]
        if self.refuse_reconstructions > 0 {
            self.refuse_reconstructions = self.refuse_reconstructions.saturating_sub(1);
            return Err(NativeSessionError::Memory(
                focal_memory::MemoryError::Capacity {
                    requested: 1,
                    available: 0,
                },
            ));
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
        // What decides whether this replica can hold the committed layout is
        // checked first, against its committed layout and its member bound:
        // a refusal there means it is configured below the authority, and it
        // fails closed. What can fail after it is memory for the boundary
        // pages the split copies: a resource the delivery waits for, retried
        // at the next poll (the record is not applied until it succeeds).
        let layout = self.committed_core()?.native_layout();
        let checked = match record.operation {
            range::LayoutOperation::Split { at, id } => layout.check_split(at, id, max),
            range::LayoutOperation::Merge { left } => layout.check_merge(left).map(|_| ()),
        };
        if checked.is_err() {
            return Err(NativeSessionError::Corrupt);
        }
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
        applied?;
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
    /// refuses the family — the same on every replica, and counted
    /// (`retirements_inert`); otherwise the same family leaves this replica
    /// alike behind its continuation, and the record counts. One refusal
    /// is not inert: a record that carries the outcome bound the authority
    /// checked it against, whose outcome this replica's own bound cannot
    /// hold, fails closed (`OutcomeBound`) — this replica is configured
    /// below the authority, and applying nothing where every other replica
    /// retired would diverge silently, while applying it would make a
    /// state its own checkpoint could not restore. A version-1 record
    /// carries no bound (it was proposed without the check): where it does
    /// not fit it is inert and counted, as any refused family. On an
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
            self.retirements_inert = self.retirements_inert.saturating_add(1);
            return Ok(());
        }
        if self
            .movement
            .as_ref()
            .is_some_and(|movement| movement.pending().is_some())
        {
            self.retirements_inert = self.retirements_inert.saturating_add(1);
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
        let local = u64::try_from(self.limits.recovery.native.outcomes).unwrap_or(u64::MAX);
        let Some(Domain::Passive(core)) = self.domain.as_mut() else {
            return Err(NativeSessionError::Failed);
        };
        // A family is derived through the event index (doc 31 M2); one a restore could not
        // fund is built now.
        core.build_event_index();
        let family = match core.retirement_family(record.root) {
            Ok(family) => family,
            Err(focal_core::native::retirement::RetirementRefusal::OutcomeCapacity) => {
                if let Some(committed) = record.outcome_limit {
                    return Err(NativeSessionError::OutcomeBound { committed, local });
                }
                self.retirements_inert = self.retirements_inert.saturating_add(1);
                return Ok(());
            }
            Err(_) => {
                self.retirements_inert = self.retirements_inert.saturating_add(1);
                return Ok(());
            }
        };
        if record.through < family.through {
            self.retirements_inert = self.retirements_inert.saturating_add(1);
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
        self.sweep_payloads_if_due();
        Ok(())
    }
    /// Apply a committed seal record (F12): inert when the prefix it named
    /// has passed or a movement is pending — the same on every replica, and
    /// counted (`seals_inert`); otherwise the same plan is derived from the
    /// committed state under the floors and the bound the record names, and
    /// the same rows leave this replica alike behind the seal's row. The
    /// floors are the pressure floors of the prefix: a replica that derives
    /// others under another resident outcome bound than the authority's is
    /// configured apart from it and fails closed by name (`OutcomeBound`),
    /// as for a retirement; under the same bound they are a divergence
    /// (`Corrupt`), as is a plan whose count is not the record's. On an
    /// authority the record ends every pending candidate first; the owner
    /// is reconstructed at the next readiness barrier.
    fn apply_seal(&mut self, data: &[u8]) -> Result<(), NativeSessionError> {
        let record = seal::SealRecord::decode(data)?;
        if record.ledger != self.ledger {
            return Err(NativeSessionError::Corrupt);
        }
        if self.seal.as_ref() == Some(&record) {
            self.seal = None;
        }
        if self.sequence()? != record.expected_prefix {
            self.seals_inert = self.seals_inert.saturating_add(1);
            return Ok(());
        }
        if self
            .movement
            .as_ref()
            .is_some_and(|movement| movement.pending().is_some())
        {
            self.seals_inert = self.seals_inert.saturating_add(1);
            return Ok(());
        }
        if !self.pending.is_empty() {
            self.resolve_suffix(SuffixEvidence::Retired)?;
        } else if matches!(self.domain, Some(Domain::Active(..))) {
            self.passive_for_replay()?;
        }
        self.readiness_requested = None;
        self.reconstruction_needed = true;
        let local = u64::try_from(self.limits.recovery.native.outcomes).unwrap_or(u64::MAX);
        let Some(Domain::Passive(core)) = self.domain.as_mut() else {
            return Err(NativeSessionError::Failed);
        };
        let floors = core
            .pressure_floors(record.floors.len())
            .map_err(NativeSessionError::Native)?;
        if floors != record.floors {
            if record.outcome_limit != local {
                return Err(NativeSessionError::OutcomeBound {
                    committed: record.outcome_limit,
                    local,
                });
            }
            return Err(NativeSessionError::Corrupt);
        }
        let applied = core.apply_seal(focal_core::native::seal::SealRecord {
            floors: &record.floors,
            bound: record.bound,
            bundle: record.bundle,
            bytes: record.bytes,
            count: record.count,
            fold: record.fold,
        });
        match applied {
            Ok(_) => {}
            Err(error @ (NativeError::Memory(_) | NativeError::Capacity(_))) => {
                return Err(error.into());
            }
            Err(_) => return Err(NativeSessionError::Corrupt),
        }
        self.seals_applied = self.seals_applied.saturating_add(1);
        Ok(())
    }
    /// Apply a committed anchored seal record (F12, `SealRule::Anchored`):
    /// inert when the prefix it named has passed or a movement is pending,
    /// as a derived seal's; otherwise the floors are the pressure's here (a
    /// replica under another resident outcome bound fails closed by name,
    /// `OutcomeBound`; under the same bound they are a divergence), and the
    /// plan the record names is derived again and published
    /// (`Core::apply_anchored`). The authority proposed it with nothing
    /// pending, and admission held behind it, so the state it applies to is
    /// the state it was proposed against: a plan or an ordinal that does
    /// not hold is a divergence. An owner with nothing pending takes it in
    /// place and goes on (`NativeOwner::apply_anchored`); any other replica
    /// applies it to its committed rows and rebuilds its owner at the next
    /// readiness barrier. A closing seal is counted apart.
    fn apply_anchored_seal(&mut self, data: &[u8]) -> Result<(), NativeSessionError> {
        let record = seal::AnchoredSealRecord::decode(data)?;
        if record.ledger != self.ledger {
            return Err(NativeSessionError::Corrupt);
        }
        if self.anchored.as_ref() == Some(&record) {
            self.anchored = None;
        }
        if self.sequence()? != record.expected_prefix {
            self.seals_inert = self.seals_inert.saturating_add(1);
            return Ok(());
        }
        if self
            .movement
            .as_ref()
            .is_some_and(|movement| movement.pending().is_some())
        {
            self.seals_inert = self.seals_inert.saturating_add(1);
            return Ok(());
        }
        if !self.pending.is_empty() {
            self.resolve_suffix(SuffixEvidence::Retired)?;
        }
        self.close_check_at = 0;
        let local = u64::try_from(self.limits.recovery.native.outcomes).unwrap_or(u64::MAX);
        let floors = self
            .committed_core()?
            .close_floors(record.floors.len())
            .map_err(NativeSessionError::Native)?;
        if floors != record.floors {
            if record.outcome_limit != local {
                return Err(NativeSessionError::OutcomeBound {
                    committed: record.outcome_limit,
                    local,
                });
            }
            return Err(NativeSessionError::Corrupt);
        }
        let anchored = || focal_core::native::seal::AnchoredRecord {
            floors: &record.floors,
            ordinal: record.ordinal,
            through: record.through,
            principals: &record.principals,
            count: record.count,
            bundle: record.bundle,
            bytes: record.bytes,
            fold: record.fold,
        };
        let refused = |error: NativeError| -> NativeSessionError {
            match error {
                error @ (NativeError::Memory(_) | NativeError::Capacity(_)) => error.into(),
                _ => NativeSessionError::Corrupt,
            }
        };
        let in_place = match self.domain.as_mut() {
            Some(Domain::Active(owner, _)) => {
                owner.apply_anchored(anchored()).map_err(refused)?.is_some()
            }
            _ => false,
        };
        if !in_place {
            if matches!(self.domain, Some(Domain::Active(..))) {
                self.passive_for_replay()?;
            }
            self.readiness_requested = None;
            self.reconstruction_needed = true;
            let Some(Domain::Passive(core)) = self.domain.as_mut() else {
                return Err(NativeSessionError::Failed);
            };
            core.apply_anchored(anchored()).map_err(refused)?;
        }
        if record.is_closing() {
            self.closes_applied = self.closes_applied.saturating_add(1);
        } else {
            self.seals_applied = self.seals_applied.saturating_add(1);
        }
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
    pub(crate) fn observe(&mut self, status: &focal_consensus::NodeScalars) {
        self.begin_read_term(status.term);
        let leader = status.role == StateRole::Leader;
        if status.term != self.observed_term || leader != self.observed_leader {
            self.ready_term = None;
            self.readiness_requested = None;
            self.reconstruction_needed = true;
            self.genesis_proposed = false;
            self.layout_change = None;
            self.retirement = None;
            self.seal = None;
            self.anchored = None;
            self.waiting_seal = None;
            self.close_wanted = false;
            self.close_check_at = 0;
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
            || data.starts_with(&record::RETIRE_MAGIC)
            || data.starts_with(&range::MAGIC)
            || data.starts_with(&movement::MAGIC)
            || data.starts_with(&retirement::MAGIC)
            || data.starts_with(&seal::MAGIC)
            || data.starts_with(&seal::ANCHORED_MAGIC)
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
        if entry.data.starts_with(&seal::MAGIC) {
            if self.genesis.is_none() {
                return Err(NativeSessionError::Corrupt);
            }
            self.apply_seal(&entry.data)?;
            self.applied_raft = entry.index;
            return Ok(());
        }
        if entry.data.starts_with(&seal::ANCHORED_MAGIC) {
            if self.genesis.is_none() {
                return Err(NativeSessionError::Corrupt);
            }
            self.apply_anchored_seal(&entry.data)?;
            self.applied_raft = entry.index;
            return Ok(());
        }
        if entry.data.starts_with(&record::RETIRE_MAGIC) {
            if self.genesis.is_none() {
                return Err(NativeSessionError::Corrupt);
            }
            return self.apply_retire_record(entry, output);
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
            self.submitted > 0
                && pending.hash == Some(header.hash)
                && pending.outcome == header.outcome
                && header.range == self.range
        });
        if output.committed.len() == output.committed.capacity() {
            return Err(NativeSessionError::Capacity("committed output"));
        }
        let outcome = if matches {
            self.publish_own_head()?
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
    /// The cold batch of a cold retirement record (doc 31 M2, A2), from the committed rows at its
    /// base, each family's frame the one the record names: a replica whose rows make another is
    /// diverged, and one that keeps no cold streams cannot follow the log.
    fn cold_batch(
        &self,
        record: &record::RetireRecord,
    ) -> Result<focal_core::native::retirement::ColdBatch, NativeSessionError> {
        if !self.cold.is_kept() {
            return Err(NativeSessionError::Stopped(Box::from(
                "a cold retirement on a replica that keeps no cold streams",
            )));
        }
        let core = match self.domain.as_ref() {
            Some(Domain::Active(owner, _)) => owner.committed_core(),
            Some(Domain::Passive(core)) => core,
            None => return Err(NativeSessionError::Failed),
        };
        let retired = record.retired();
        let mut roots = Vec::new();
        roots
            .try_reserve_exact(retired.len())
            .map_err(|_| NativeSessionError::Capacity("cold batch"))?;
        roots.extend(retired.iter().map(|one| one.root));
        // Written at the record's own sequence, the prefix every frame claims.
        let batch = core.cold_batch(
            &roots,
            record.header().outcome.sequence,
            self.limits.encoding,
            &self.budget,
            BudgetLane::Committed,
        )?;
        let alike = batch.entries.len() == retired.len()
            && batch.entries.iter().zip(retired).all(|(entry, one)| {
                entry.root == one.root
                    && entry.digest == one.bundle
                    && u64::from(entry.length) == one.bytes
            });
        if !alike {
            return Err(NativeSessionError::Native(
                focal_model::lifecycle::ContractError::InvalidManifest.into(),
            ));
        }
        Ok(batch)
    }
    /// Publish this leader's own oldest candidate, which the committed entry carries.
    fn publish_own_head(&mut self) -> Result<NativeOutcome, NativeSessionError> {
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
        self.encoded = self.encoded.saturating_sub(1);
        self.submitted = self.submitted.saturating_sub(1);
        self.shrink_pending();
        Ok(outcome)
    }
    /// Apply a committed retirement batch (doc 31 M2), in Raft order with the native records
    /// around it and under the same header rules: this leader's own candidate published where
    /// the entry carries it, else the families derived from the committed rows at the record's
    /// place and retired alike. Rows that cannot derive them alike are a divergence that stops
    /// this replica (`InvalidManifest`), never a retirement skipped.
    fn apply_retire_record(
        &mut self,
        entry: &focal_consensus::CommittedEntry,
        output: &mut NativeOutput,
    ) -> Result<(), NativeSessionError> {
        let record = record::RetireRecord::inspect(
            &entry.data,
            self.limits.inspection,
            focal_core::native::retirement_batch::MAX_RETIRE_FAMILIES,
        )?;
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
            self.submitted > 0
                && pending.hash == Some(header.hash)
                && pending.outcome == header.outcome
                && header.range == self.range
        });
        // A cold record's batch (doc 31 M2, A2): encoded here from the committed rows at the
        // record's base, the leader's and a follower's alike, checked family by family against
        // the record, and appended to the cold streams once the record applies. What it takes of
        // the streams is promised first: a refusal (no room, no memory) leaves the record to be
        // applied again, nothing changed, and once it applies its batch fails only on IO.
        let cold = if record.version() == focal_core::native::retirement_batch::RETIRE_COLD {
            let batch = self.cold_batch(&record)?;
            let room = self
                .cold
                .writer(self.cold_source.as_ref(), &self.budget)
                .map_err(super::cold::cold_error)?
                .ok_or(NativeSessionError::Failed)?
                .room(batch.bytes.len())
                .map_err(super::cold::cold_error)?;
            Some((batch, room))
        } else {
            None
        };
        if output.committed.len() == output.committed.capacity() {
            return Err(NativeSessionError::Capacity("committed output"));
        }
        let outcome = if matches {
            self.publish_own_head()?
        } else {
            if !self.pending.is_empty() {
                self.resolve_suffix(SuffixEvidence::ConflictingCommittedPrefix)?;
            } else if matches!(self.domain, Some(Domain::Active(..))) {
                self.passive_for_replay()?;
            }
            let Some(Domain::Passive(core)) = self.domain.as_mut() else {
                return Err(NativeSessionError::Failed);
            };
            core.build_event_index();
            core.apply_retirement_record(&record)?
        };
        if let Some((batch, room)) = cold {
            let appended = match self.cold.writer(self.cold_source.as_ref(), &self.budget) {
                Ok(Some(writer)) => writer.append(outcome.sequence, &batch.bytes, room),
                Ok(None) => Err(focal_evidence::ContentError::Failed),
                Err(error) => Err(error),
            };
            // The record applied and its batch is not in the streams: this replica stops, and a
            // reopen restores the streams from its checkpoint and appends the batch again as it
            // replays the record.
            if let Err(error) = appended {
                return Err(NativeSessionError::Stopped(Box::from(format!(
                    "a cold batch could not be appended after its record applied: {error}"
                ))));
            }
        }
        output.committed.push(NativeCommit {
            raft_index: entry.index,
            raft_term: entry.term,
            record_hash: header.hash,
            outcome,
        });
        self.retired_families = self
            .retired_families
            .saturating_add(u64::try_from(record.retired().len()).unwrap_or(u64::MAX));
        self.sweep_payloads_if_due();
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
            .map_err(|_| NativeSessionError::Capacity("replayed records"))?;
        let mut sequence = self.sequence()?;
        let mut recording_range = self.recording_range;
        let mut recording_term = self.recording_term;
        // Anchor consecutiveness on the run's own first index, not applied_raft +
        // 1: consensus drops empty term-start no-ops from the committed stream, so
        // the first record of a run can sit at applied_raft + 2 (or further). The
        // single-entry path tolerates that same gap (it checks only
        // entry.index <= applied_raft); record_run guarantees the batch is
        // internally contiguous, so only successors must be strictly consecutive.
        let mut expected_index: Option<u64> = None;
        for entry in entries {
            let want = match expected_index {
                Some(previous) => previous.checked_add(1).ok_or(NativeSessionError::Corrupt)?,
                None => entry.index,
            };
            if entry.index != want
                || entry.index <= self.applied_raft
                || entry.index > applied_index
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
            expected_index = Some(entry.index);
        }
        // Room for every commit the run reports, before any record publishes.
        if output
            .committed
            .capacity()
            .saturating_sub(output.committed.len())
            < entries.len()
        {
            return Err(NativeSessionError::Capacity("committed output"));
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
        // Each record as the walk above inspected it.
        for ((outcome, entry), (record, _)) in batch.outcomes.into_iter().zip(entries).zip(&records)
        {
            let header = record.header();
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
    /// After a delivery: a fully applied newer-term entry proves older-term
    /// candidates dead, an idle follower returns to a passive Core, and a ready
    /// leader without a genesis proposes it.
    pub(crate) fn settle(
        &mut self,
        status: &focal_consensus::NodeScalars,
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
            // Past the entry the term began with: in a fast group what the new leader recovered
            // is of its term too, and precedes that entry, so a candidate restamped there is
            // published before it, never discarded (hyper-raft S-4).
            && consensus.term_began_by(self.applied_raft)?
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
        if !self.is_authoritative(&consensus.scalars()) {
            return None;
        }
        // A host taking a batch proposes its candidates when the batch ends
        // (`flush_deferred`), not at a poll inside it; a seal held for the
        // drain is proposed once nothing is pending, batch or not.
        let flushed = if self.defer_proposals {
            Ok(())
        } else {
            self.flush_proposals(consensus)
        };
        match flushed.and_then(|()| self.propose_wanted(consensus)) {
            Ok(()) => None,
            Err(error) => {
                if error.class() == FailureClass::FailClosed {
                    self.fail_closed(&error);
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
        let status = consensus.scalars();
        let leader = status.role == StateRole::Leader;
        self.observe(&status);
        if delivery.output.is_none() {
            delivery.output = Some(NativeOutput::reserve(
                &self.budget,
                delivery.events.committed.len(),
                delivery
                    .events
                    .read_states
                    .len()
                    .saturating_add(self.parked_reads.len())
                    .saturating_add(self.read_rounds.held()),
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
        // Barriers parked by an earlier delivery whose index this one reached.
        let bound = consensus.pending_reads();
        for barrier in self.take_applied_parked_reads() {
            self.apply_read_barrier(&barrier, leader, &status, consensus)?;
        }
        while let Some(barrier) = delivery.events.read_states.get(delivery.read) {
            // A caller's read answers its round, which is served once this copy has applied
            // the index it names (`serve_rounds`), never parked on its own.
            if Self::is_correlated_read(&barrier.context) {
                let barrier = barrier.clone();
                self.answer_round(&barrier, consensus)?;
                delivery.read = add(delivery.read, 1)?;
                continue;
            }
            if barrier.index > self.applied_raft {
                // Answered by a leader ahead of this copy: the read waits
                // for the entries it names (27 §5, follower reads).
                match self.park_read(barrier, bound) {
                    Ok(()) => {}
                    // The parked set is full: the barrier is dropped and
                    // counted, never held back with the delivery — the
                    // delivery carries the entries the parked reads wait
                    // for; new reads are refused at the request.
                    Err(NativeSessionError::Capacity(_)) => {
                        self.reads_dropped = self.reads_dropped.saturating_add(1);
                    }
                    Err(error) => return Err(error),
                }
                delivery.read = add(delivery.read, 1)?;
                continue;
            }
            let barrier = barrier.clone();
            self.apply_read_barrier(&barrier, leader, &status, consensus)?;
            delivery.read = add(delivery.read, 1)?;
        }
        let output = delivery.output.as_mut().ok_or(NativeSessionError::Failed)?;
        self.serve_rounds(status.term, output)?;
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
    /// One barrier this copy has applied up to. Classified by its 8-byte
    /// magic, not by full equality with the current term: READINESS and
    /// CORRELATION share their first seven bytes, so a readiness barrier
    /// confirmed in a prior term and drained after a term bump would
    /// otherwise be misread as a correlated read and fail closed. A
    /// readiness barrier for the current term promotes; a stale-term one is
    /// a benign internal barrier and is ignored.
    fn apply_read_barrier(
        &mut self,
        barrier: &focal_consensus::ReadBarrier,
        leader: bool,
        status: &focal_consensus::NodeScalars,
        consensus: &mut DurableNode,
    ) -> Result<(), NativeSessionError> {
        if barrier.context.starts_with(READINESS.as_slice()) {
            if leader && barrier.context == readiness(status.term) {
                self.promote(status.term, consensus)?;
            }
        } else if Self::is_correlated_read(&barrier.context) {
            self.answer_round(barrier, consensus)?;
        } else {
            return Err(NativeSessionError::Corrupt);
        }
        Ok(())
    }
    fn request_read(
        &mut self,
        consensus: &mut DurableNode,
        context: &[u8],
    ) -> Result<(), NativeSessionError> {
        request_barrier(&self.budget, consensus, context)
    }
    /// Request a quorum read barrier for the caller's correlation. The completion arrives in a
    /// later poll as a `NativeReadBoundary`; the readiness namespace is internal and cannot be
    /// requested here. One barrier is out at a time, confirming every read held when it was
    /// asked: a read asked while one is out waits for the next (`read_rounds`).
    pub(crate) fn read_index(
        &mut self,
        consensus: &mut DurableNode,
        correlation: ReadCorrelation,
    ) -> Result<(), NativeSessionError> {
        let status = consensus.scalars();
        self.require_reader(&status)?;
        self.begin_read_term(status.term);
        // A copy whose barriers wait at their bound, out or answered ahead of what it has
        // applied, is too far behind to take another read: refused here, typed, not dropped
        // when answered.
        if self
            .read_rounds
            .rounds()
            .saturating_add(self.parked_reads.len())
            >= consensus.pending_reads()
        {
            return Err(NativeSessionError::Capacity("parked reads"));
        }
        self.grow_read_room()?;
        let window = read_window(consensus);
        let budget = &self.budget;
        self.read_rounds.hold(correlation, window, |barrier| {
            request_barrier(budget, consensus, &correlated(barrier))
        })
    }
    /// Room for one more read held: grown by doubling as reads come, the room's bytes charged
    /// before it is taken, and the charge for the room it replaces let go.
    fn grow_read_room(&mut self) -> Result<(), NativeSessionError> {
        let held = self.read_rounds.held();
        if held < self.read_rounds.room() {
            return Ok(());
        }
        let room = held.saturating_mul(2).max(1);
        let permit = self.budget.reserve(
            BudgetKind::Pending,
            BudgetLane::Completion,
            super::read_rounds::ReadRounds::bytes_for(room),
        )?;
        if !self.read_rounds.reserve(room) {
            return Err(NativeSessionError::Capacity("reads held"));
        }
        self.read_rounds_charge = Some(permit.commit());
        Ok(())
    }
    /// The reads held for `term`: those of an earlier term are given up with it, as their
    /// askers' waits are.
    fn begin_read_term(&mut self, term: u64) {
        let given = self.read_rounds.begin_term(term);
        self.count_given_up(given);
    }
    /// Reads given up unanswered, counted.
    fn count_given_up(&mut self, given: usize) {
        self.reads_dropped = self
            .reads_dropped
            .saturating_add(u64::try_from(given).unwrap_or(u64::MAX));
    }
    /// One tick of the copy's clock for its reads: a barrier out asked again, or given up, once
    /// it has waited the core's election timeout in ticks unanswered (`read_rounds`).
    pub(crate) fn tick_reads(&mut self, consensus: &mut DurableNode) {
        self.begin_read_term(consensus.scalars().term);
        let window = read_window(consensus);
        let patience = consensus.election_tick();
        let budget = &self.budget;
        let given = self.read_rounds.tick(patience, window, |barrier| {
            request_barrier(budget, consensus, &correlated(barrier))
        });
        self.count_given_up(given);
    }
    /// A caller's barrier answered: its round is served once this copy has applied the index
    /// it names, and the reads behind it are asked for.
    pub(crate) fn answer_round(
        &mut self,
        barrier: &focal_consensus::ReadBarrier,
        consensus: &mut DurableNode,
    ) -> Result<(), NativeSessionError> {
        let correlation = barrier
            .context
            .strip_prefix(CORRELATION.as_slice())
            .and_then(|bytes| <[u8; 16]>::try_from(bytes).ok())
            .ok_or(NativeSessionError::Corrupt)?;
        // Answered by a leader ahead of this copy (27 §5, F55): the round waits for the entries
        // its answer names, counted as a parked barrier was.
        if barrier.index > self.applied_raft {
            self.reads_parked = self.reads_parked.saturating_add(1);
        }
        let window = read_window(consensus);
        let budget = &self.budget;
        let given = self.read_rounds.answered(
            ReadCorrelation(correlation),
            barrier.index,
            window,
            |next| request_barrier(budget, consensus, &correlated(next)),
        );
        self.count_given_up(given);
        Ok(())
    }
    /// The reads held: waiting on a barrier, or answered and waiting to be applied here.
    pub(crate) fn reads_held(&self) -> usize {
        self.read_rounds.held()
    }
    /// Read barriers answered above this copy's applied index and held until it caught up.
    pub(crate) fn reads_parked(&self) -> u64 {
        self.reads_parked
    }
    /// Reads given up unanswered: their barrier refused or lost, or their term ended.
    pub(crate) fn reads_dropped(&self) -> u64 {
        self.reads_dropped
    }
    /// Every read of `term` whose round this copy has now applied, as a boundary naming the
    /// applied Raft index and native sequence it may be served at, into the room the output
    /// holds; a read of an earlier term is given up with it.
    pub(crate) fn serve_rounds(
        &mut self,
        term: u64,
        output: &mut NativeOutput,
    ) -> Result<(), NativeSessionError> {
        self.begin_read_term(term);
        let native_sequence = self.sequence()?;
        let room = output.reads.capacity().saturating_sub(output.reads.len());
        let reads = &mut output.reads;
        self.read_rounds
            .take_served(self.applied_raft, room, |correlation, raft_index| {
                reads.push(NativeReadBoundary {
                    correlation,
                    raft_index,
                    native_sequence,
                });
            });
        Ok(())
    }
}

/// Asks consensus for a read barrier under `context`, its bytes held while it is asked.
fn request_barrier(
    budget: &MemoryBudget,
    consensus: &mut DurableNode,
    context: &[u8],
) -> Result<(), NativeSessionError> {
    if context.is_empty() || context.len() > 1024 {
        return Err(NativeSessionError::Capacity("read context"));
    }
    let _permit = budget.reserve(
        BudgetKind::Pending,
        BudgetLane::Completion,
        array::<u8>(context.len())?,
    )?;
    let mut copied = reserved(context.len())?;
    copied.extend_from_slice(context);
    consensus.read_index(copied)?;
    Ok(())
}

/// The barriers a copy keeps out at once: its share of the reads its leader may hold
/// (`DurableNode::pending_reads`), divided among the members that may ask, voters and learners,
/// so that every copy at its share is still within the leader's bound (`read_rounds`).
fn read_window(consensus: &DurableNode) -> usize {
    let members = consensus.membership();
    let askers = members
        .voters
        .len()
        .saturating_add(members.learners.len())
        .max(1);
    consensus
        .pending_reads()
        .checked_div(askers)
        .unwrap_or(1)
        .max(1)
}

/// A caller's read barrier's context: the correlation namespace and the correlation.
fn correlated(correlation: ReadCorrelation) -> [u8; 24] {
    let mut context = [0u8; 24];
    if let Some(prefix) = context.get_mut(..8) {
        prefix.copy_from_slice(CORRELATION);
    }
    if let Some(suffix) = context.get_mut(8..) {
        suffix.copy_from_slice(&correlation.0);
    }
    context
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
