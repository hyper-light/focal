//! Recompute original aggregate decisions from the complete recovered prefix.
//! The model orders original publications, freezes first terminal decisions,
//! and checks complete cohorts. Later Observe reports therefore remain history
//! rather than changing an earlier response, artifact or claimant outcome.
use super::read_validate::{ValidationRead, invalid};
use super::*;
use focal_memory::{BudgetKind, BudgetLane};
use focal_model::lifecycle::aggregation::{
    self, AggregateOutcome, ArtifactOutcome, ProjectionLimits, ProjectionShape,
    PublicationPosition, PublishedResponse, PublishedResult, RegisteredEvaluation, ResponseOutcome,
    WholeWorkView,
};
use focal_model::lifecycle::claim::ClaimTerminalCut;
use focal_model::lifecycle::evidence;
use std::cell::Cell;

fn add(a: usize, b: usize) -> Result<usize, NativeError> {
    a.checked_add(b).ok_or(ContractError::Capacity.into())
}
fn mul(a: usize, b: usize) -> Result<usize, NativeError> {
    a.checked_mul(b).ok_or(ContractError::Capacity.into())
}
fn levels(count: usize) -> Result<usize, NativeError> {
    usize::try_from(
        usize::BITS
            .checked_sub(count.leading_zeros())
            .ok_or(ContractError::Capacity)?,
    )
    .map_err(|_| ContractError::Capacity.into())
}
fn require(value: bool) -> Result<(), NativeError> {
    if value { Ok(()) } else { Err(invalid()) }
}
fn model(error: NativeError) -> ContractError {
    match error {
        NativeError::Contract(error) => error,
        NativeError::Memory(_) | NativeError::Capacity(_) => ContractError::Capacity,
        _ => ContractError::InvalidManifest,
    }
}

struct Rows<'a, 'v, 'r> {
    read: &'a ValidationRead<'v, 'r>,
    claim: ClaimId,
    // WholeWorkView has Option-returning lookups. Preserve their error before
    // the model can reinterpret exhausted work as absent participant evidence.
    failure: Cell<Option<ContractError>>,
}
/// Shared funded projection boundary for complete checkpoints and bounded
/// recorded-successor overlays. Sources expose actual owner indices only.
pub(super) trait ProjectionSource: WholeWorkView {
    fn limits(&self) -> NativeLimits;
    fn budget(&self) -> &MemoryBudget;
    fn charge(&self, visits: usize) -> Result<(), NativeError>;
    fn check(&self) -> Result<(), NativeError>;
}
impl ProjectionSource for Rows<'_, '_, '_> {
    fn limits(&self) -> NativeLimits {
        self.read.limits
    }
    fn budget(&self) -> &MemoryBudget {
        self.read.budget
    }
    fn charge(&self, visits: usize) -> Result<(), NativeError> {
        self.read.charge(visits)
    }
    fn check(&self) -> Result<(), NativeError> {
        Rows::check(self)
    }
}
impl Rows<'_, '_, '_> {
    fn retain<T>(&self, value: Result<T, NativeError>) -> Option<T> {
        match value {
            Ok(value) => Some(value),
            Err(error) => {
                self.failure.set(Some(model(error)));
                None
            }
        }
    }
    fn check(&self) -> Result<(), NativeError> {
        self.failure.get().map_or(Ok(()), |error| Err(error.into()))
    }
    fn get(&self, key: Key) -> Option<&Row> {
        if self.failure.get().is_some() {
            return None;
        }
        self.retain(self.read.get(key)).flatten()
    }
}
impl WholeWorkView for Rows<'_, '_, '_> {
    fn prefix(&self) -> SessionSeq {
        self.read.prefix
    }
    fn declaration(&self, id: ValidationId) -> Option<&validation::Declaration> {
        as_definition(self.get(Key::Definition(id)))
    }
    fn evaluation(&self, registered: RegisteredEvaluation) -> Option<&validation::EvaluationState> {
        as_evaluation(self.get(Key::Evaluation(transactions::key_for_registered(
            self.claim, registered,
        ))))
    }
    fn accepted(&self, result: &validation::AcceptedResult) -> Option<PublishedResult<'_>> {
        let key = NativeResultKey::of(*result);
        let (result, sequence, ordinal) = match result.phase() {
            validation::Phase::Delivery => {
                let value = response_reads::as_delivery(self.get(Key::DeliveryResult(key)))?;
                (value.result_ref(), value.sequence(), value.ordinal())
            }
            validation::Phase::MissingTarget => {
                let value = response_reads::as_missing(self.get(Key::MissingResult(key)))?;
                (value.result_ref(), value.sequence(), value.ordinal())
            }
            validation::Phase::Programmatic | validation::Phase::Quality => {
                let value = as_result(self.get(Key::Accepted(key)))?;
                (value.result_ref(), value.sequence(), value.ordinal())
            }
        };
        Some(PublishedResult {
            result,
            position: PublicationPosition { sequence, ordinal },
        })
    }
    fn response(&self, id: TestamentId) -> Option<PublishedResponse<'_>> {
        let value = response_reads::as_response_record(self.get(Key::Response(id)))?;
        Some(PublishedResponse {
            response: value.response(),
            received: value.received(),
            entered: value.entered(),
        })
    }
    fn work(&self, id: ArtifactId) -> Option<&evidence::WorkArtifact> {
        as_work(self.get(Key::Work(id))).map(|row| &row.state)
    }
    fn works(
        &self,
        claim: ClaimId,
    ) -> impl Iterator<Item = Result<&evidence::WorkArtifact, ContractError>> {
        // The Cycle namespace starts with ClaimId. One bounded seek visits only
        // this claim's actual cycles, including abandoned old receipts; it never
        // scans other claims or selects only successful/closed responses.
        let start = Key::Cycle(NativeCycleKey {
            claim,
            receipt: ReceiptId([0; 16]),
            epoch: 0,
            cycle: 0,
        });
        let seek = (|| {
            require(claim == self.claim)?;
            self.check()?;
            self.read.charge(mul(add(usize::BITS as usize, 1)?, 64)?)
        })();
        let mut failed = seek.err().map(model);
        let mut cycles = if failed.is_none() {
            Some(self.read.root.entries_from(&start, false))
        } else {
            None
        };
        let mut current: Option<NativeCycleKey> = None;
        let mut next = None;
        let mut remaining = 0usize;
        let mut cycle_count = 0usize;
        let mut work_count = 0usize;
        let mut finished = false;
        std::iter::from_fn(move || {
            if finished {
                return None;
            }
            if let Some(error) = failed.take() {
                finished = true;
                return Some(Err(error));
            }
            let result = (|| {
                loop {
                    self.read.charge(128)?;
                    if let Some(id) = next {
                        remaining = remaining.checked_sub(1).ok_or_else(invalid)?;
                        work_count = add(work_count, 1)?;
                        if work_count > self.read.limits.plan_edges {
                            return Err(ContractError::Capacity.into());
                        }
                        let key = current.ok_or_else(invalid)?;
                        let row =
                            as_work(Some(self.read.require(Key::Work(id))?)).ok_or_else(invalid)?;
                        let work = &row.state;
                        require(
                            work.reference().id == id
                                && work.claim() == claim
                                && work.receipt().receipt == key.receipt
                                && work.receipt().epoch == key.epoch
                                && work.cycle() == key.cycle,
                        )?;
                        next = row.next;
                        require((remaining == 0) == next.is_none())?;
                        return Ok(Some(work));
                    }
                    require(remaining == 0)?;
                    let Some(entry) = cycles.as_mut().and_then(|values| values.next()) else {
                        return Ok(None);
                    };
                    let Key::Cycle(key) = entry.key else {
                        return Ok(None);
                    };
                    if key.claim != claim {
                        return Ok(None);
                    }
                    cycle_count = add(cycle_count, 1)?;
                    if cycle_count > self.read.limits.plan_edges {
                        return Err(ContractError::Capacity.into());
                    }
                    let Row::Cycle(cycle) = &entry.value else {
                        return Err(invalid());
                    };
                    if cycle.work_count > self.read.limits.work_artifacts_per_cycle {
                        return Err(ContractError::Capacity.into());
                    }
                    require((cycle.work_count == 0) == cycle.work_head.is_none())?;
                    current = Some(key);
                    remaining = cycle.work_count;
                    next = cycle.work_head;
                }
            })();
            match result {
                Ok(Some(value)) => Some(Ok(value)),
                Ok(None) => {
                    finished = true;
                    None
                }
                Err(error) => {
                    finished = true;
                    let error = model(error);
                    self.failure.set(Some(error));
                    Some(Err(error))
                }
            }
        })
    }
}

fn policy_work(
    claim: &ClaimState,
    read: &impl ProjectionSource,
) -> Result<(usize, usize), NativeError> {
    let policy = claim.acceptance();
    let slots = policy.slot_count();
    let declarations = policy.declarations().len();
    if slots > read.limits().plan_edges || declarations > read.limits().definitions {
        return Err(ContractError::Capacity.into());
    }
    read.charge(add(slots, 1)?)?;
    let mut checks = 0usize;
    for slot in policy.slots() {
        checks = add(checks, slot.checks.len())?;
    }
    if checks > read.limits().plan_edges {
        return Err(ContractError::Capacity.into());
    }
    // Full policy intent hashing in RegistrationSet::check, plus the quote's
    // own d+s+k walk. Each immutable declaration needs <256 framed hash bytes,
    // slot <64 and check <64; fixed identity/header costs fit another 256.
    let hash = add(
        256,
        add(mul(declarations, 256)?, mul(add(slots, checks)?, 64)?)?,
    )?;
    read.charge(add(hash, add(declarations, add(slots, checks)?)?)?)?;
    Ok((checks, declarations))
}

/// One complete per-claim projection, after its retained rows have been built.
/// Model work is debited before invoking callbacks: its local allowance and
/// source lookups can never both spend the same shared remaining work budget.
pub(super) fn validate_claim(
    owned: &OwnedClaim,
    read: &ValidationRead<'_, '_>,
) -> Result<(), NativeError> {
    let claim = owned.claim().ok_or_else(invalid)?;
    let registry = owned.registrations().ok_or_else(invalid)?;
    let id = ClaimId(claim.binding().object.0);
    let rows = Rows {
        read,
        claim: id,
        failure: Cell::new(None),
    };
    validate_source(claim, registry, &rows)
}

pub(super) fn validate_source(
    claim: &ClaimState,
    registry: &RegistrationSet,
    rows: &impl ProjectionSource,
) -> Result<(), NativeError> {
    let id = ClaimId(claim.binding().object.0);
    let read = rows;
    let native = rows.limits();
    let mut works = 0usize;
    for work in rows.works(id) {
        work?;
        works = add(works, 1)?;
    }
    rows.check()?;
    // A legacy claim carries no native obligations, responses, work or
    // evaluations: its recorded status is the fact, nothing is derived (23 §5).
    if claim.origin() == focal_model::lifecycle::claim::ClaimOrigin::Legacy {
        if works != 0
            || claim.response_count() != 0
            || !registry.rows().is_empty()
            || claim.acceptance().slot_count() != 0
            || !claim.acceptance().declarations().is_empty()
        {
            return Err(invalid());
        }
        return Ok(());
    }
    let (checks, declarations) = policy_work(claim, read)?;
    let shape = ProjectionShape {
        responses: claim.response_count(),
        works,
        evaluations: registry.rows().len(),
    };
    let mut limits = ProjectionLimits {
        responses: native.responses,
        works: native.plan_edges,
        slots: native.plan_edges,
        evaluations: native.evaluations_per_claim,
        declarations: native.definitions,
        visits: native.plan_edges,
        bytes: native.preparation_bytes,
    };
    let quote = aggregation::quote_projection(claim, shape, limits)?;
    let counted = add(quote.inspection_visits(), quote.reduction_visits())?;
    let dimensions = [
        shape.responses,
        works,
        shape.evaluations,
        claim.acceptance().slot_count(),
        declarations,
        checks,
    ];
    let largest = dimensions.into_iter().max().unwrap_or(0);
    // Counted model loops include linear policy searches but omit fixed-record
    // comparisons, binary searches and sorting. At most one bounded binary
    // search is nested in a counted scalar helper; 512 bytes covers the largest
    // fixed target/result/claim comparison and its scalar guards. No descriptor
    // text or payload is hashed by these helpers.
    let helpers = mul(add(counted, 1)?, mul(512, add(levels(largest)?, 1)?)?)?;
    // Three in-place sorts: responses, chronological entry/result events, and
    // chosen delivery results. 64*n*(log2(n)+1) bounds their fixed key work.
    let mut sorts = 0usize;
    for count in [
        shape.responses,
        add(shape.responses, shape.evaluations)?,
        shape.evaluations,
    ] {
        sorts = add(sorts, mul(mul(count, add(levels(count)?, 1)?)?, 64)?)?;
    }
    read.charge(add(counted, add(helpers, sorts)?)?)?;
    limits.visits = quote.inspection_visits().max(quote.reduction_visits());
    let plan = aggregation::prepare_projection(claim, registry, rows, limits);
    rows.check()?;
    let plan = plan?;
    require(plan.construction_charge() <= quote.construction_charge())?;
    let reservation = read.budget().reserve(
        BudgetKind::Recovery,
        BudgetLane::Completion,
        plan.construction_charge(),
    )?;
    let projection = plan.build();
    rows.check()?;
    let projection = projection?;
    require(projection.construction_charge() <= quote.construction_charge())?;
    derived(claim, &projection, rows)?;
    seals(claim, registry, rows)?;
    drop(projection);
    drop(reservation);
    Ok(())
}

fn derived(
    claim: &ClaimState,
    projection: &aggregation::WholeWorkProjection<'_>,
    rows: &impl ProjectionSource,
) -> Result<(), NativeError> {
    let read = rows;
    read.charge(add(claim.response_count(), 1)?)?;
    for decision in projection.response_decisions() {
        read.charge(256)?;
        let decision = decision?;
        let response = rows.response(TestamentId(decision.response_binding().object.0));
        rows.check()?;
        let response = response.ok_or_else(invalid)?.response;
        let expected = match decision.response_outcome() {
            ResponseOutcome::Evaluating => None,
            outcome => Some(outcome),
        };
        require(response.terminal() == expected)?;
    }
    read.charge(add(projection.artifact_decisions().len(), 1)?)?;
    for decision in projection.artifact_decisions() {
        read.charge(256)?;
        let work = rows.work(decision.artifact().id);
        rows.check()?;
        let work = work.ok_or_else(invalid)?;
        let expected = match decision.outcome() {
            ArtifactOutcome::Pending => None,
            outcome => Some((decision.sequence(), outcome)),
        };
        require(work.terminal() == expected)?;
    }
    read.charge(256)?;
    match projection.claim_decision().outcome() {
        AggregateOutcome::Pending => require(!claim.local_complete()),
        AggregateOutcome::LocalComplete { sequence } => {
            require(claim.local_complete() && claim.local_sealed_at() == Some(sequence))
        }
        AggregateOutcome::Blocked(cut) => {
            require(claim.terminal_cut() == Some(ClaimTerminalCut::Required(cut)))
        }
    }
}

fn seals(
    claim: &ClaimState,
    registry: &RegistrationSet,
    rows: &impl ProjectionSource,
) -> Result<(), NativeError> {
    let read = rows;
    let cut = claim.local_sealed_at();
    let recorded = registry.snapshot_v1();
    require(
        recorded.sealed == cut.is_some()
            && recorded.sealed_at == cut
            && (cut.is_none() || recorded.increments_sealed),
    )?;
    read.charge(add(registry.rows().len(), 1)?)?;
    for registered in registry.rows() {
        // Original terminal members are unchanged by automatic sealing. A
        // later terminal Observe report must keep the stamp recorded while its
        // begun attempt was still outstanding at the original claim cut.
        read.charge(1024)?;
        let state = rows.evaluation(*registered);
        rows.check()?;
        let state = state.ok_or_else(invalid)?;
        let declaration = rows.declaration(ValidationId(registered.binding().object.0));
        rows.check()?;
        state.check_recorded_claim_seal(declaration.ok_or_else(invalid)?, claim)?;
        let mut before = false;
        if let Some(last) = state.last_result().filter(|result| result.is_terminal()) {
            let published = rows.accepted(&last);
            rows.check()?;
            let published = published.ok_or_else(invalid)?;
            before = cut.is_some_and(|cut| published.position.sequence <= cut);
        }
        require(state.sealed().is_some() == (cut.is_some() && !before))?;
    }
    Ok(())
}
