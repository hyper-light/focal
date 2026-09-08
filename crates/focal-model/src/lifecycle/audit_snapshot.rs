//! Native restoration views, distinct from the frozen V1 wire/application
//! formats. Hydration preserves frozen audit values without rerunning current
//! actor, receipt, clock or report authority. Definition stamps and canonical
//! order derive from actual retained declarations, never supplied digests.
//!
//! These checks establish intrinsic coherence and contiguous accepted history.
//! The importer must authenticate the containing records, prove complete sealed
//! registry membership and original publication positions, check immutable
//! evidence and restore local custody. A result testament's content also binds
//! owner publication witnesses; this model preserves that content identity but
//! cannot authenticate it without those owner records.
use super::*;
use crate::lifecycle::memory as bytes;
use crate::lifecycle::validation::{
    AcceptedResultSnapshotV1, Declaration, FenceReason, ProgramView, TargetDeclaration,
};
use crate::{ContentHash, ReceiptFence, ValidationMode, VerdictValue};

const ALLOCATION: usize = 4 * std::mem::size_of::<usize>();
const MEMBER_VISITS: usize = 96;

/// Every retained member coordinate, excluding the definition stamp and the
/// mechanically derived parts of the canonical order key.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AuditMemberSnapshotV1 {
    pub key: EvaluationKey,
    pub declaration_index: u32,
    pub binding: Binding,
    pub receipt: Option<ReceiptFence>,
    pub begun: bool,
    pub state: State,
    pub suppression: Option<Suppression>,
    pub fence: Option<AuthorityFence>,
    pub last_result: Option<AcceptedResultSnapshotV1>,
    pub sealed: Option<ContentHash>,
}

/// Scalar header. Export members and results through their snapshot getters;
/// no temporary array or owned definition is required for export. Counts and
/// the reserved late-result capacity use explicit, checked scalar widths.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AuditCohortSnapshotV1 {
    pub claim: Binding,
    pub issuer: ParticipantId,
    pub sequence: SessionSeq,
    pub members: u64,
    pub results: u64,
    pub result_capacity: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ResultTestamentSnapshotV1 {
    pub binding: Binding,
    pub state: ResultTestamentState,
    pub cohort: AuditCohortSnapshotV1,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ResultArtifactSnapshotV1 {
    pub result: AcceptedResultSnapshotV1,
    pub artifact: ArtifactRef,
    pub producer: ParticipantId,
}

/// Allocation-free preparation over immutable borrowed rows. Declaration order
/// must match canonical member order. Source arrays and decoder workspace are
/// separately owned and bounded by the importer. Hold construction_charge
/// before build: it includes inline storage, both full buffer capacities and
/// conservative allocator headers, including unused late-result slots.
#[derive(Debug)]
pub struct AuditCohortHydrationPlan<'a, 'definition> {
    snapshot: AuditCohortSnapshotV1,
    members: &'a [AuditMemberSnapshotV1],
    results: &'a [AcceptedResultSnapshotV1],
    declarations: &'a [&'definition Declaration],
    capacity: usize,
    heap: usize,
    allocations: usize,
    charge: usize,
    preparation_visits: usize,
    construction_visits: usize,
    complete: bool,
}

#[derive(Debug)]
pub struct ResultTestamentHydrationPlan<'a, 'definition> {
    snapshot: ResultTestamentSnapshotV1,
    cohort: AuditCohortHydrationPlan<'a, 'definition>,
    charge: usize,
}

struct Visits {
    remaining: usize,
    used: usize,
}
impl Visits {
    fn take(&mut self, count: usize) -> Result<(), ContractError> {
        self.remaining = self
            .remaining
            .checked_sub(count)
            .ok_or(ContractError::Capacity)?;
        self.used = bytes::add(self.used, count)?;
        Ok(())
    }
}

impl AuditMember {
    pub fn snapshot_v1(&self) -> AuditMemberSnapshotV1 {
        AuditMemberSnapshotV1 {
            key: self.key,
            declaration_index: self.order.declaration,
            binding: self.binding,
            receipt: self.receipt,
            begun: self.begun,
            state: self.state,
            suppression: self.suppression,
            fence: self.fence,
            last_result: self.last_result.map(AcceptedResult::snapshot_v1),
            sealed: self.sealed,
        }
    }

    /// Fixed member checks plus the bounded last-result policy scan, if present.
    pub fn hydration_visits(
        declaration: &Declaration,
        snapshot: AuditMemberSnapshotV1,
    ) -> Result<usize, ContractError> {
        bytes::add(
            MEMBER_VISITS,
            if snapshot.last_result.is_some() {
                AcceptedResult::hydration_visits(declaration)?
            } else {
                0
            },
        )
    }

    /// Restore an audit projection, not a live evaluation cursor. The actual
    /// evaluation and complete sealed membership remain importer cross-checks.
    pub fn hydrate_v1(
        declaration: &Declaration,
        snapshot: AuditMemberSnapshotV1,
        max_visits: usize,
    ) -> Result<Self, ContractError> {
        bytes::fits(Self::hydration_visits(declaration, snapshot)?, max_visits)?;
        identity(snapshot.binding, declaration.binding())?;
        require(
            snapshot.key.validation.0 == declaration.binding().object.0
                && snapshot.declaration_index == declaration.declaration_index(),
        )?;
        if snapshot.key.generation == 0 {
            return Err(ContractError::StaleEvaluation);
        }
        target(declaration, snapshot.key.target)?;
        match (declaration.target(), snapshot.receipt) {
            (TargetDeclaration::Admission, None) => {}
            (_, Some(receipt))
                if declaration.target() != TargetDeclaration::Admission
                    && !receipt.receipt.is_zero()
                    && receipt.epoch != 0 => {}
            _ => return Err(ContractError::StaleReceipt),
        }
        if snapshot.sealed == Some(ContentHash([0; 32])) {
            return Err(ContractError::InvalidCut);
        }
        if let Some(AuthorityFence {
            reason: FenceReason::Deadline(deadline),
            ..
        }) = snapshot.fence
            && deadline != declaration.deadline()
        {
            return Err(ContractError::InvalidCut);
        }
        let last = snapshot
            .last_result
            .map(|value| {
                AcceptedResult::hydrate_v1(
                    declaration,
                    value,
                    AcceptedResult::hydration_visits(declaration)?,
                )
            })
            .transpose()?;
        if let Some(last) = last {
            identity(snapshot.binding, last.binding())?;
            require(
                result_key(last) == snapshot.key
                    && last.receipt() == snapshot.receipt
                    && last.resulting_state() == snapshot.state,
            )?;
        }
        member_state(declaration, snapshot, last)?;
        Ok(Self {
            definition: declaration.definition_stamp(),
            key: snapshot.key,
            order: order(
                snapshot.key.target,
                declaration.declaration_index(),
                snapshot.key.generation,
                snapshot.key.validation,
            ),
            binding: snapshot.binding,
            receipt: snapshot.receipt,
            begun: snapshot.begun,
            state: snapshot.state,
            suppression: snapshot.suppression,
            fence: snapshot.fence,
            last_result: last,
            sealed: snapshot.sealed,
        })
    }
}

fn identity(current: Binding, original: Binding) -> Result<(), ContractError> {
    Binding {
        revision: original.revision,
        ..current
    }
    .check(&original)?;
    if current.revision < original.revision {
        return Err(ContractError::StaleRevision);
    }
    Ok(())
}

fn require(valid: bool) -> Result<(), ContractError> {
    if valid {
        Ok(())
    } else {
        Err(ContractError::InvalidManifest)
    }
}

fn target(declaration: &Declaration, target: Target) -> Result<(), ContractError> {
    let valid = |binding: Binding| {
        binding.ledger == declaration.binding().ledger && !binding.object.is_zero()
    };
    let own_claim = |binding: Binding| valid(binding) && binding.object.0 == declaration.claim().0;
    let matches = match (declaration.target(), target) {
        (
            TargetDeclaration::WholeWorkSlot { index, .. },
            Target::Artifact {
                response,
                slot,
                artifact,
            },
        ) => index == slot && valid(response) && valid(artifact),
        (
            TargetDeclaration::WholeWorkSlot { index, .. },
            Target::MissingSlot { response, slot },
        ) => index == slot && valid(response),
        (TargetDeclaration::Delivery, Target::Delivery { response }) => valid(response),
        (TargetDeclaration::Admission, Target::Admission { claim }) => own_claim(claim),
        (TargetDeclaration::Increment, Target::Increment { claim, artifact }) => {
            own_claim(claim) && valid(artifact)
        }
        _ => false,
    };
    if matches {
        Ok(())
    } else {
        Err(ContractError::InvalidTarget)
    }
}

fn member_state(
    declaration: &Declaration,
    snapshot: AuditMemberSnapshotV1,
    last: Option<AcceptedResult>,
) -> Result<(), ContractError> {
    if snapshot.state.is_terminal() {
        require(last.is_some() && snapshot.fence.is_none() && snapshot.suppression.is_none())?;
    } else {
        // Audit cohorts only contain recorded sealed or already terminal rows.
        require(snapshot.sealed.is_some())?;
    }
    if snapshot.begun {
        require(
            snapshot.suppression.is_none()
                && !matches!(
                    snapshot.key.target,
                    Target::Delivery { .. } | Target::MissingSlot { .. }
                ),
        )?;
        if let Some(last) = last {
            require(last.attempt().is_some())?;
        } else {
            require(
                snapshot.state
                    == match declaration.program() {
                        ProgramView::Programmatic { .. } => State::Validating,
                        ProgramView::Agentic { .. } => State::ValidatingQualityBar,
                        ProgramView::Delivery => return Err(ContractError::InvalidTransition),
                    },
            )?;
        }
    } else if snapshot.state == State::Ready {
        require(last.is_none() && snapshot.suppression.is_some())?;
    } else {
        require(snapshot.sealed.is_none())?;
        let last = last.ok_or(ContractError::InvalidManifest)?;
        require(matches!(
            (snapshot.state, last.phase()),
            (State::Validated, Phase::Delivery)
                | (State::ValidationIncomplete, Phase::MissingTarget)
        ))?;
    }
    match snapshot.suppression {
        Some(Suppression::MissingTarget) => require(
            matches!(snapshot.key.target, Target::MissingSlot { .. })
                && declaration.mode() == ValidationMode::Observe,
        )?,
        Some(Suppression::ArtifactFailure(_)) => {
            require(matches!(snapshot.key.target, Target::Artifact { .. }))?
        }
        Some(Suppression::ParentFailure(_)) => {
            require(!matches!(snapshot.key.target, Target::Delivery { .. }))?
        }
        Some(Suppression::CohortSealed(_)) | None => {}
    }
    if snapshot.binding.revision.0
        < declaration
            .binding()
            .revision
            .0
            .checked_add(1)
            .ok_or(ContractError::Capacity)?
    {
        return Err(ContractError::StaleRevision);
    }
    Ok(())
}

impl AuditCohort {
    pub fn snapshot_v1(&self) -> Result<AuditCohortSnapshotV1, ContractError> {
        Ok(AuditCohortSnapshotV1 {
            claim: self.claim,
            issuer: self.issuer,
            sequence: self.sequence,
            members: u64::try_from(self.members.len()).map_err(|_| ContractError::Capacity)?,
            results: u64::try_from(self.results.len()).map_err(|_| ContractError::Capacity)?,
            result_capacity: u64::try_from(self.result_capacity)
                .map_err(|_| ContractError::Capacity)?,
        })
    }

    /// `claim` is the actual retained claim, possibly at a later revision than
    /// the frozen cohort. Its immutable identity, issuer and original seal must
    /// agree. Definitions are the actual retained bodies in canonical member
    /// order; full membership and publication provenance remain importer work.
    /// `max_visits` covers preparation and subsequent construction together.
    #[allow(clippy::too_many_arguments)]
    pub fn prepare_hydration_v1<'a, 'definition>(
        claim: &ClaimState,
        snapshot: AuditCohortSnapshotV1,
        members: &'a [AuditMemberSnapshotV1],
        results: &'a [AcceptedResultSnapshotV1],
        declarations: &'a [&'definition Declaration],
        limits: Limits,
        max_bytes: usize,
        max_visits: usize,
    ) -> Result<AuditCohortHydrationPlan<'a, 'definition>, ContractError> {
        let capacity =
            usize::try_from(snapshot.result_capacity).map_err(|_| ContractError::Capacity)?;
        bytes::fits(members.len(), limits.evaluations)?;
        bytes::fits(results.len(), limits.results)?;
        bytes::fits(capacity, limits.results)?;
        require(
            u64::try_from(members.len()).ok() == Some(snapshot.members)
                && u64::try_from(results.len()).ok() == Some(snapshot.results)
                && members.len() == declarations.len(),
        )?;
        bytes::fits(results.len(), capacity)?;
        let heap = bytes::add(
            bytes::array::<AuditMember>(members.len())?,
            bytes::array::<AcceptedResult>(capacity)?,
        )?;
        let allocations = bytes::add(
            bytes::allocation::<AuditMember>(members.len()),
            bytes::allocation::<AcceptedResult>(capacity),
        )?;
        let charge = charge::<AuditCohort>(heap, allocations)?;
        bytes::fits(charge, max_bytes)?;
        let mut visits = Visits {
            remaining: max_visits,
            used: 0,
        };
        visits.take(16)?;
        identity(claim.binding(), snapshot.claim)?;
        if snapshot.issuer != claim.issuer() {
            return Err(ContractError::WrongActor);
        }
        if snapshot.sequence.0 == 0 || claim.local_sealed_at() != Some(snapshot.sequence) {
            return Err(ContractError::InvalidCut);
        }
        let mut offset = 0usize;
        let mut previous_order = None;
        let mut full_capacity = 0usize;
        let mut construction_visits = 0usize;
        let mut complete = true;
        for (value, declaration) in members.iter().zip(declarations) {
            visits.take(8)?;
            if declaration.binding().ledger != snapshot.claim.ledger {
                return Err(ContractError::WrongLedger);
            }
            if declaration.claim().0 != snapshot.claim.object.0 {
                return Err(ContractError::WrongObject);
            }
            if declaration.issuer() != snapshot.issuer {
                return Err(ContractError::WrongActor);
            }
            let work = AuditMember::hydration_visits(declaration, *value)?;
            visits.take(work)?;
            let member = AuditMember::hydrate_v1(declaration, *value, work)?;
            construction_visits = bytes::add(construction_visits, bytes::add(work, 1)?)?;
            if previous_order.is_some_and(|previous| previous >= member.order) {
                return Err(ContractError::InvalidManifest);
            }
            previous_order = Some(member.order);
            complete &= member.complete();
            let bound = usize::try_from(declaration.attempt_bound().max(1))
                .map_err(|_| ContractError::Capacity)?;
            full_capacity = bytes::add(full_capacity, bound)?;
            let mut last = None;
            let mut count = 0usize;
            loop {
                // Charge the terminal probe too, even for an empty group.
                visits.take(1)?;
                let Some(value) = results.get(offset).copied() else {
                    break;
                };
                if snapshot_key(value) != member.key {
                    break;
                }
                visits.take(12)?;
                let work = AcceptedResult::hydration_visits(declaration)?;
                visits.take(work)?;
                let result = AcceptedResult::hydrate_v1(declaration, value, work)?;
                construction_visits = bytes::add(construction_visits, bytes::add(work, 1)?)?;
                Self::check_result(snapshot.claim, &member, result)?;
                history_step(last, result, count)?;
                count = bytes::add(count, 1)?;
                bytes::fits(count, bound)?;
                last = Some(result);
                offset = bytes::add(offset, 1)?;
            }
            require(last == member.last_result)?;
        }
        visits.take(4)?;
        require(offset == results.len())?;
        // Open cohorts retain the entire admitted attempt promise. Completed
        // native cohorts are either frozen exactly or retain their original
        // open-cohort capacity; restoration never silently shrinks either.
        require(capacity == full_capacity || (complete && capacity == results.len()))?;
        construction_visits = bytes::add(construction_visits, bytes::add(members.len(), 8)?)?;
        bytes::fits(construction_visits, visits.remaining)?;
        Ok(AuditCohortHydrationPlan {
            snapshot,
            members,
            results,
            declarations,
            capacity,
            heap,
            allocations,
            charge,
            preparation_visits: visits.used,
            construction_visits,
            complete,
        })
    }
}

fn snapshot_key(value: AcceptedResultSnapshotV1) -> EvaluationKey {
    EvaluationKey {
        validation: value.validation,
        target: value.target,
        generation: value.generation,
    }
}

fn history_step(
    previous: Option<AcceptedResult>,
    next: AcceptedResult,
    count: usize,
) -> Result<(), ContractError> {
    match next.attempt() {
        Some(attempt) if usize::try_from(attempt).ok() == Some(count) => {}
        None if count == 0 && matches!(next.phase(), Phase::Delivery | Phase::MissingTarget) => {}
        _ => return Err(ContractError::InvalidManifest),
    }
    if let Some(previous) = previous {
        require(!previous.is_terminal() && previous.binding().revision < next.binding().revision)?;
        match previous.verdict() {
            VerdictValue::Error => {
                require(previous.phase() == next.phase())?;
                if previous.phase() == Phase::Quality {
                    require(previous.programmatic_evidence() == next.programmatic_evidence())?;
                }
            }
            VerdictValue::Pass => require(
                previous.phase() == Phase::Programmatic
                    && next.phase() == Phase::Quality
                    && next.programmatic_evidence() == previous.evidence(),
            )?,
            VerdictValue::Fail | VerdictValue::Incomplete => {
                return Err(ContractError::InvalidManifest);
            }
        }
    }
    Ok(())
}

fn charge<T>(heap: usize, allocations: usize) -> Result<usize, ContractError> {
    bytes::add(
        bytes::total::<T>(heap)?,
        allocations
            .checked_mul(ALLOCATION)
            .ok_or(ContractError::Capacity)?,
    )
}

impl AuditCohortHydrationPlan<'_, '_> {
    pub fn construction_charge(&self) -> usize {
        self.charge
    }
    pub fn heap_bytes(&self) -> usize {
        self.heap
    }
    pub fn heap_allocations(&self) -> usize {
        self.allocations
    }
    pub fn preparation_visits(&self) -> usize {
        self.preparation_visits
    }
    pub fn construction_visits(&self) -> usize {
        self.construction_visits
    }
    pub fn visits(&self) -> Result<usize, ContractError> {
        bytes::add(self.preparation_visits, self.construction_visits)
    }

    pub fn build(self) -> Result<AuditCohort, ContractError> {
        let mut members = bytes::reserve::<AuditMember>(self.members.len())?;
        bytes::fits(members.capacity(), self.members.len())?;
        let mut results = bytes::reserve::<AcceptedResult>(self.capacity)?;
        bytes::fits(results.capacity(), self.capacity)?;
        let mut visits = Visits {
            remaining: self.construction_visits,
            used: 0,
        };
        let mut offset = 0usize;
        for (value, declaration) in self.members.iter().zip(self.declarations) {
            let work = AuditMember::hydration_visits(declaration, *value)?;
            visits.take(bytes::add(work, 1)?)?;
            members.push(AuditMember::hydrate_v1(declaration, *value, work)?);
            loop {
                visits.take(1)?;
                let Some(result) = self.results.get(offset).copied() else {
                    break;
                };
                if snapshot_key(result) != value.key {
                    break;
                }
                let work = AcceptedResult::hydration_visits(declaration)?;
                visits.take(work)?;
                results.push(AcceptedResult::hydrate_v1(declaration, result, work)?);
                offset = bytes::add(offset, 1)?;
            }
        }
        visits.take(8)?;
        require(offset == self.results.len())?;
        let cohort = AuditCohort {
            claim: self.snapshot.claim,
            issuer: self.snapshot.issuer,
            sequence: self.snapshot.sequence,
            members,
            results,
            result_capacity: self.capacity,
        };
        bytes::fits(
            charge::<AuditCohort>(cohort.retained_heap_bytes()?, cohort.heap_allocations()?)?,
            self.charge,
        )?;
        Ok(cohort)
    }
}

impl ResultTestament {
    pub fn snapshot_v1(&self) -> Result<ResultTestamentSnapshotV1, ContractError> {
        Ok(ResultTestamentSnapshotV1 {
            binding: self.binding,
            state: self.state,
            cohort: self.cohort.snapshot_v1()?,
        })
    }

    /// `generated` is the actual original generation binding from retained
    /// history, not inferred from the current row. Posting advances it once.
    /// The lower model permits other starting revisions and even zero content;
    /// native import separately enforces canonical generation and its original
    /// content/publication witnesses. Neither actor transition is replayed here.
    #[allow(clippy::too_many_arguments)]
    pub fn prepare_hydration_v1<'a, 'definition>(
        claim: &ClaimState,
        generated: Binding,
        snapshot: ResultTestamentSnapshotV1,
        members: &'a [AuditMemberSnapshotV1],
        results: &'a [AcceptedResultSnapshotV1],
        declarations: &'a [&'definition Declaration],
        limits: Limits,
        max_bytes: usize,
        max_visits: usize,
    ) -> Result<ResultTestamentHydrationPlan<'a, 'definition>, ContractError> {
        let remaining_visits = max_visits.checked_sub(8).ok_or(ContractError::Capacity)?;
        if generated.ledger != snapshot.cohort.claim.ledger {
            return Err(ContractError::WrongLedger);
        }
        if generated.object.is_zero() {
            return Err(ContractError::InvalidTarget);
        }
        snapshot.binding.check(&match snapshot.state {
            ResultTestamentState::Generated => generated,
            ResultTestamentState::Posted => generated.next()?,
        })?;
        let cohort = AuditCohort::prepare_hydration_v1(
            claim,
            snapshot.cohort,
            members,
            results,
            declarations,
            limits,
            max_bytes,
            remaining_visits,
        )?;
        if !cohort.complete {
            return Err(ContractError::InvalidTransition);
        }
        let charge = charge::<ResultTestament>(cohort.heap, cohort.allocations)?;
        bytes::fits(charge, max_bytes)?;
        Ok(ResultTestamentHydrationPlan {
            snapshot,
            cohort,
            charge,
        })
    }
}

impl ResultTestamentHydrationPlan<'_, '_> {
    pub fn construction_charge(&self) -> usize {
        self.charge
    }
    pub fn heap_bytes(&self) -> usize {
        self.cohort.heap_bytes()
    }
    pub fn heap_allocations(&self) -> usize {
        self.cohort.heap_allocations()
    }
    pub fn preparation_visits(&self) -> Result<usize, ContractError> {
        bytes::add(self.cohort.preparation_visits(), 8)
    }
    pub fn construction_visits(&self) -> usize {
        self.cohort.construction_visits()
    }
    pub fn visits(&self) -> Result<usize, ContractError> {
        bytes::add(self.preparation_visits()?, self.construction_visits())
    }
    pub fn build(self) -> Result<ResultTestament, ContractError> {
        let result = ResultTestament {
            binding: self.snapshot.binding,
            state: self.snapshot.state,
            cohort: self.cohort.build()?,
        };
        bytes::fits(
            charge::<ResultTestament>(result.retained_heap_bytes()?, result.heap_allocations()?)?,
            self.charge,
        )?;
        Ok(result)
    }
}

impl ResultArtifact {
    pub fn snapshot_v1(&self) -> ResultArtifactSnapshotV1 {
        ResultArtifactSnapshotV1 {
            result: self.result.snapshot_v1(),
            artifact: self.artifact,
            producer: self.producer,
        }
    }
    pub fn hydration_visits(declaration: &Declaration) -> Result<usize, ContractError> {
        bytes::add(AcceptedResult::hydration_visits(declaration)?, 8)
    }
    pub fn hydrate_v1(
        declaration: &Declaration,
        snapshot: ResultArtifactSnapshotV1,
        max_visits: usize,
    ) -> Result<Self, ContractError> {
        bytes::fits(Self::hydration_visits(declaration)?, max_visits)?;
        let result = AcceptedResult::hydrate_v1(
            declaration,
            snapshot.result,
            AcceptedResult::hydration_visits(declaration)?,
        )?;
        let artifact = Self::from_result(result)?;
        if artifact.artifact != snapshot.artifact || artifact.producer != snapshot.producer {
            return Err(ContractError::InvalidManifest);
        }
        Ok(artifact)
    }
}
