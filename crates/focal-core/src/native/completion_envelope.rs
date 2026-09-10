//! One evaluation generation's future RAM and record demand. The owner separately
//! retains the exact definition/schema contract, funds the envelope and journals
//! its exclusive loans. No reservation or authority is granted by this value.

use super::cohort_budget::CohortBudget;
use super::prepare::{ALLOCATION, add, array, containers, event_containers, heap, within};
use super::prepare_budget::ConstructionBudget;
use super::*;
use focal_memory::{RangeWriteEnvelope, RangeWriteLimits};
#[cfg(test)]
use focal_model::lifecycle::artifact_descriptor::PayloadSpec;
use focal_model::lifecycle::artifact_descriptor::{ArtifactDescriptor, Limits as ArtifactLimits};
use focal_model::lifecycle::scope::{OwnedChild, Scope, ScopeLimits};
use focal_model::{ObjectRef, ValidationMode, WaitPredicate};

#[path = "completion_admission_graph.rs"]
mod admission_graph;
#[path = "completion_graph.rs"]
mod graph;
#[path = "completion_work.rs"]
mod work;
use graph::CompletionGraph;

#[cfg(test)]
#[path = "completion_envelope_tests.rs"]
pub(super) mod tests;

/// Supplied by the owner's pinned verifier contract for every reachable schema.
/// Workspace excludes the separately retained verification capability. Both may
/// coexist with ingress and Core construction, so the envelope sums them.
#[derive(Debug, Clone, Copy)]
pub(super) struct EvidenceBounds {
    pub(super) workspace_bytes: usize,
    pub(super) retained_bytes: usize,
}

/// The one additional allowance belongs only to a checked Admission failure.
/// Other changed parent rows do not consume that target-specific credit.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum CompletionUse {
    Regular,
    AdmissionFailure,
}

/// Capture the actual report parent before its borrowed preparation frame is
/// consumed. No participant can select the completion-use classification.
#[derive(Debug, Clone, Copy)]
pub(super) struct ReportParent {
    binding: Binding,
    status: ClaimStatus,
}

impl ReportParent {
    pub(super) fn capture(parent: &ClaimState) -> Self {
        Self {
            binding: parent.binding(),
            status: parent.status(),
        }
    }

    pub(super) fn claim_id(self) -> ClaimId {
        ClaimId(self.binding.object.0)
    }

    /// A late report can contain cohort seals without failing its parent again.
    /// Only a real Posted-to-PostFailed change needs the original failure fact.
    pub(super) fn completion_use_recorded(
        self,
        prepared: &NativePrepared,
    ) -> Result<CompletionUse, NativeError> {
        let operation = prepared.outcome.operation;
        let changed = prepared.claim(self.claim_id());
        if operation != NativeOperation::ReportAdmission
            || self.status != ClaimStatus::Posted
            || changed.is_none_or(|row| row.status() != ClaimStatus::PostFailed)
        {
            return self.completion_use(operation, changed);
        }
        let event = super::completion_book::CompletionBook::admission_failure_event(prepared)?
            .ok_or(ContractError::InvalidCut)?;
        self.completion_use_prepared(operation, changed, Some(event))
    }

    /// Classify a prepared report using its actual original PostFailed fact.
    /// Later monitor releases may advance the same terminal row while its
    /// original Required cut and immutable identity remain unchanged.
    pub(super) fn completion_use_prepared(
        self,
        operation: NativeOperation,
        changed: Option<&ClaimState>,
        post_failed: Option<NativeClaimEvent>,
    ) -> Result<CompletionUse, NativeError> {
        let Some(event) = post_failed else {
            let usage = self.completion_use(operation, changed)?;
            return if usage == CompletionUse::AdmissionFailure {
                Err(ContractError::InvalidCut.into())
            } else {
                Ok(usage)
            };
        };
        let changed = changed.ok_or(ContractError::InvalidTransition)?;
        let failed = self.binding.next()?;
        if operation != NativeOperation::ReportAdmission
            || self.status != ClaimStatus::Posted
            || changed.status() != ClaimStatus::PostFailed
            || event.kind != NativeEventKind::PostFailed
            || event.before != Some(self.binding)
            || event.after != failed
            || event.status != ClaimStatus::PostFailed
            || event.owned_child.is_some()
            || changed.binding().revision < failed.revision
        {
            return Err(ContractError::InvalidTransition.into());
        }
        failed.check(&Binding {
            revision: failed.revision,
            ..changed.binding()
        })?;
        let Some(focal_model::lifecycle::claim::ClaimTerminalCut::Required(cut)) =
            changed.terminal_cut()
        else {
            return Err(ContractError::InvalidCut.into());
        };
        if cut.sequence().0 == 0
            || changed.local_sealed_at() != Some(cut.sequence())
            || cut.cause().key().target
                != focal_model::lifecycle::aggregation::CauseTarget::Admission
        {
            return Err(ContractError::InvalidCut.into());
        }
        Ok(CompletionUse::AdmissionFailure)
    }

    pub(super) fn completion_use(
        self,
        operation: NativeOperation,
        changed: Option<&ClaimState>,
    ) -> Result<CompletionUse, NativeError> {
        if operation != NativeOperation::ReportAdmission {
            return Ok(CompletionUse::Regular);
        }
        let Some(changed) = changed else {
            return Ok(CompletionUse::Regular);
        };
        if changed.binding() == self.binding && changed.status() == self.status {
            return Ok(CompletionUse::Regular);
        }
        self.binding.next()?.check(&changed.binding())?;
        if self.status != ClaimStatus::Posted || changed.status() != ClaimStatus::PostFailed {
            return Err(ContractError::InvalidTransition.into());
        }
        let Some(focal_model::lifecycle::claim::ClaimTerminalCut::Required(cut)) =
            changed.terminal_cut()
        else {
            return Err(ContractError::InvalidTransition.into());
        };
        if cut.sequence().0 == 0
            || cut.cause().key().target
                != focal_model::lifecycle::aggregation::CauseTarget::Admission
        {
            return Err(ContractError::InvalidCut.into());
        }
        Ok(CompletionUse::AdmissionFailure)
    }
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(super) struct CompletionSlots {
    pub(super) claims: usize,
    pub(super) definitions: usize,
    pub(super) evaluations: usize,
    pub(super) artifacts: usize,
    pub(super) identities: usize,
    pub(super) results: usize,
    pub(super) responses: usize,
    pub(super) receipts: usize,
    pub(super) result_testaments: usize,
    pub(super) monitors: usize,
    pub(super) monitor_links: usize,
    pub(super) outcomes: usize,
    pub(super) events: usize,
    pub(super) sequences: u64,
    /// New unique rows, including history and auxiliary records. Replaced rows
    /// consume range-write capacity without consuming a new-row slot.
    pub(super) new_rows: usize,
}

impl CompletionSlots {
    pub(super) fn checked_add(self, other: Self) -> Result<Self, NativeError> {
        Ok(Self {
            claims: add(self.claims, other.claims)?,
            definitions: add(self.definitions, other.definitions)?,
            evaluations: add(self.evaluations, other.evaluations)?,
            artifacts: add(self.artifacts, other.artifacts)?,
            identities: add(self.identities, other.identities)?,
            results: add(self.results, other.results)?,
            responses: add(self.responses, other.responses)?,
            receipts: add(self.receipts, other.receipts)?,
            result_testaments: add(self.result_testaments, other.result_testaments)?,
            monitors: add(self.monitors, other.monitors)?,
            monitor_links: add(self.monitor_links, other.monitor_links)?,
            outcomes: add(self.outcomes, other.outcomes)?,
            events: add(self.events, other.events)?,
            sequences: self
                .sequences
                .checked_add(other.sequences)
                .ok_or(NativeError::Capacity("completion sequence slots"))?,
            new_rows: add(self.new_rows, other.new_rows)?,
        })
    }

    pub(super) fn checked_sub(self, other: Self) -> Result<Self, NativeError> {
        let subtract = |left: usize, right: usize| {
            left.checked_sub(right)
                .ok_or(NativeError::Capacity("completion slot subtraction"))
        };
        Ok(Self {
            claims: subtract(self.claims, other.claims)?,
            definitions: subtract(self.definitions, other.definitions)?,
            evaluations: subtract(self.evaluations, other.evaluations)?,
            artifacts: subtract(self.artifacts, other.artifacts)?,
            identities: subtract(self.identities, other.identities)?,
            results: subtract(self.results, other.results)?,
            responses: subtract(self.responses, other.responses)?,
            receipts: subtract(self.receipts, other.receipts)?,
            result_testaments: subtract(self.result_testaments, other.result_testaments)?,
            monitors: subtract(self.monitors, other.monitors)?,
            monitor_links: subtract(self.monitor_links, other.monitor_links)?,
            outcomes: subtract(self.outcomes, other.outcomes)?,
            events: subtract(self.events, other.events)?,
            sequences: self
                .sequences
                .checked_sub(other.sequences)
                .ok_or(NativeError::Capacity("completion sequence slots"))?,
            new_rows: subtract(self.new_rows, other.new_rows)?,
        })
    }

    pub(super) fn checked_scale(self, reports: u32) -> Result<Self, NativeError> {
        let count =
            usize::try_from(reports).map_err(|_| NativeError::Capacity("completion slot count"))?;
        Ok(Self {
            claims: multiply(self.claims, count)?,
            definitions: multiply(self.definitions, count)?,
            evaluations: multiply(self.evaluations, count)?,
            artifacts: multiply(self.artifacts, count)?,
            identities: multiply(self.identities, count)?,
            results: multiply(self.results, count)?,
            responses: multiply(self.responses, count)?,
            receipts: multiply(self.receipts, count)?,
            result_testaments: multiply(self.result_testaments, count)?,
            monitors: multiply(self.monitors, count)?,
            monitor_links: multiply(self.monitor_links, count)?,
            outcomes: multiply(self.outcomes, count)?,
            events: multiply(self.events, count)?,
            sequences: self
                .sequences
                .checked_mul(u64::from(reports))
                .ok_or(NativeError::Capacity("completion sequence slots"))?,
            new_rows: multiply(self.new_rows, count)?,
        })
    }
}

/// Target-specific finite-record policy. A future WholeWork constructor may
/// price its complete worst-case aggregation on every attempt; the book never
/// infers an event count or auxiliary-row count from the number of reports.
#[derive(Debug, Clone, Copy)]
struct SlotDemand {
    per_report: CompletionSlots,
    failure: Option<CompletionSlots>,
}

impl SlotDemand {
    fn remaining(
        self,
        reports: u32,
        failure_available: bool,
    ) -> Result<CompletionSlots, NativeError> {
        if reports == 0 {
            return Ok(CompletionSlots::default());
        }
        let slots = self.per_report.checked_scale(reports)?;
        if failure_available {
            slots.checked_add(self.failure.ok_or(ContractError::InvalidTransition)?)
        } else {
            Ok(slots)
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub(super) struct CompletionEnvelope {
    record_buffers: Option<record_codec::EncodingLimits>,
    target: CompletionTarget,
    parent: Binding,
    policy: ContentHash,
    scope_limits: ScopeLimits,
    max_responses: u32,
    parent_heap: usize,
    registry_heap: usize,
    registrations: usize,
    max_registrations: usize,
    registry_limit: usize,
    descriptor: ArtifactLimits,
    input_heap: usize,
    reports: u32,
    ordinary_report: RangeWriteEnvelope,
    failed_report: Option<RangeWriteEnvelope>,
    // Mixed completion journals survive construction until their pending
    // candidate commits or rolls back, just like the candidate's range rows.
    ordinary_journal_bytes: usize,
    failed_journal_bytes: usize,
    retained: usize,
    workspace: usize,
    required: usize,
    slot_demand: SlotDemand,
    slots: CompletionSlots,
    cohort: CohortBudget,
    work: Option<work::WorkCompletion>,
    graph: Option<CompletionGraph>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CompletionTarget {
    Admission,
    Increment,
    Work,
}

impl CompletionTarget {
    fn of(declaration: &validation::Declaration) -> Result<Self, NativeError> {
        match declaration.target() {
            validation::TargetDeclaration::Admission => Ok(Self::Admission),
            validation::TargetDeclaration::Increment => Ok(Self::Increment),
            validation::TargetDeclaration::WholeWorkSlot { .. } => Ok(Self::Work),
            _ => Err(ContractError::InvalidTarget.into()),
        }
    }

    fn matches(self, target: validation::Target) -> bool {
        matches!(
            (self, target),
            (Self::Admission, validation::Target::Admission { .. })
                | (Self::Increment, validation::Target::Increment { .. })
                | (Self::Work, validation::Target::Artifact { .. })
        )
    }
}

/// Checked immutable-parent facts shared across one affected grant cohort.
/// Construction visits the actual policy and owned buffers once. The borrowed
/// lifetime prevents retaining these facts across mutation of either source;
/// every per-grant comparison below uses only fixed-size fields.
#[derive(Debug)]
pub(super) struct ParentFacts<'a> {
    binding: Binding,
    policy: ContentHash,
    scope_limits: ScopeLimits,
    max_responses: u32,
    heap: usize,
    registry_heap: usize,
    registrations: usize,
    registry_limit: usize,
    responses: usize,
    posted: bool,
    _sources: std::marker::PhantomData<(&'a ClaimState, &'a RegistrationSet)>,
}

impl<'a> ParentFacts<'a> {
    pub(super) fn new(
        claim: &'a ClaimState,
        registrations: &'a RegistrationSet,
    ) -> Result<Self, NativeError> {
        registrations.check(claim)?;
        Ok(Self {
            binding: claim.binding(),
            policy: claim.acceptance().intent_fingerprint(),
            scope_limits: claim.scopes().limits(),
            max_responses: claim.max_responses(),
            heap: heap(claim)?,
            registry_heap: transactions::registry_heap(registrations)?,
            registrations: registrations.rows().len(),
            registry_limit: registrations.max_rows(),
            responses: claim.response_count(),
            posted: claim.status() == ClaimStatus::Posted,
            _sources: std::marker::PhantomData,
        })
    }
}

fn multiply(left: usize, right: usize) -> Result<usize, NativeError> {
    left.checked_mul(right)
        .ok_or(NativeError::Capacity("completion multiplication"))
}

fn charged_heap(bytes: usize, allocations: usize) -> Result<usize, NativeError> {
    add(bytes, multiply(allocations, ALLOCATION)?)
}

/// One Admission target per declaration; one Delivery or WholeWork slot target
/// per response; one Increment target per possible work slot and response. The
/// current native owner permits at most one work artifact per slot/cycle. Future
/// target replacement requires a revised bound; retained prior-receipt
/// Increment registrations add conservative adoption-generation headroom. The
/// registry's immutable owner limit also bounds legal future registration.
pub(super) fn registry_bound(
    limits: NativeLimits,
    claim: &ClaimState,
    registrations: &RegistrationSet,
) -> Result<(usize, usize), NativeError> {
    use focal_model::lifecycle::aggregation::RegisteredEvaluation;
    let maximum =
        super::response_budget::retained_registration_bound(claim, registrations, limits)?
            .min(registrations.max_rows());
    within(registrations.rows().len(), maximum)?;
    let future = charged_heap(
        multiply(maximum, size_of::<RegisteredEvaluation>())?,
        usize::from(maximum != 0),
    )?;
    Ok((
        maximum,
        future.max(transactions::registry_heap(registrations)?),
    ))
}

pub(super) fn cohort_bound(
    limits: NativeLimits,
    claim: &ClaimState,
    registry: &RegistrationSet,
) -> Result<CohortBudget, NativeError> {
    let (rows, heap) = registry_bound(limits, claim, registry)?;
    CohortBudget::empty().add_claim(claim, registry, rows, heap)
}

/// A funded report has one live evaluation group in the completion journal.
/// Its immutable parent policy is fingerprinted once, its exact declaration is
/// resolved once, and all actual candidate events are visited. Reject Begin or
/// reconstruction if even that guaranteed final bookkeeping cannot fit. Future
/// multi-member seal writers must replace this bound with their complete cohort.
fn check_journal_visits(
    limits: NativeLimits,
    claim: &ClaimState,
    events: usize,
) -> Result<(), NativeError> {
    let visits = add(
        add(
            events,
            multiply(3, claim.acceptance().declarations().len())?,
        )?,
        add(claim.acceptance().slot_count(), 3)?,
    )?;
    if visits > limits.plan_edges {
        return Err(NativeError::Capacity("completion journal visits"));
    }
    Ok(())
}

fn check_cohort_visits(
    limits: NativeLimits,
    cohort: CohortBudget,
    claims: usize,
    extras: usize,
    original_events: usize,
) -> Result<(), NativeError> {
    if cohort.claims() == 0 {
        return Ok(());
    }
    if cohort.writer_visits(claims, extras)? > limits.plan_edges {
        return Err(NativeError::Capacity("cohort seal writer visits"));
    }
    if cohort.journal_visits(original_events, 1)? > limits.plan_edges {
        return Err(NativeError::Capacity("cohort completion journal visits"));
    }
    Ok(())
}

/// Prices full authored response-history and bounded future registration
/// capacity, retaining existing spare capacities as floors. Both must fit the
/// entry ceiling in full; only additional scope growth may be clipped to the
/// remaining space. Later parent checks enforce this fixed envelope.
pub(super) fn parent_bound(
    limits: NativeLimits,
    claim: &ClaimState,
    registrations: &RegistrationSet,
) -> Result<(usize, usize), NativeError> {
    registrations.check(claim)?;
    let response_limit = usize::try_from(claim.max_responses())
        .map_err(|_| NativeError::Capacity("response history count"))?;
    within(claim.response_count(), response_limit)?;
    let current_responses = charged_heap(
        claim.response_history_heap_bytes()?,
        claim.response_history_heap_allocations(),
    )?;
    let future_responses = charged_heap(
        claim.max_response_history_heap_bytes()?,
        usize::from(claim.max_responses() != 0),
    )?;
    let scopes = claim.scopes();
    let scope_limits = scopes.limits();
    let current_scopes = charged_heap(scopes.retained_heap_bytes()?, scopes.heap_allocations()?)?;
    let future_scopes = charged_heap(
        add(
            add(
                multiply(scope_limits.scopes, size_of::<Scope>())?,
                multiply(scope_limits.roots, size_of::<WaitPredicate>())?,
            )?,
            multiply(scope_limits.children, size_of::<OwnedChild>())?,
        )?,
        add(
            add(
                usize::from(scope_limits.scopes != 0),
                usize::from(scope_limits.children != 0),
            )?,
            scope_limits.scopes.min(scope_limits.roots),
        )?,
    )?;
    let immutable = heap(claim)?
        .checked_sub(add(current_scopes, current_responses)?)
        .ok_or(NativeError::Capacity("claim mutable heap accounting"))?;
    let (_, registry) = registry_bound(limits, claim, registrations)?;
    let row_heap = entry_heap(limits)?;
    let available_parent = row_heap
        .checked_sub(add(OwnedClaim::container_charge(), registry)?)
        .ok_or(NativeError::Capacity("future claim entry charge"))?;
    let history_base = add(immutable, future_responses.max(current_responses))?;
    // Do not silently reduce the authored closing-history capacity to fit the
    // entry. Such a Begin would make an otherwise legal later response refuse.
    within(add(history_base, current_scopes)?, available_parent)?;
    let scope_space = available_parent
        .checked_sub(history_base)
        .ok_or(NativeError::Capacity("future response entry charge"))?;
    let parent = add(
        history_base,
        future_scopes.max(current_scopes).min(scope_space),
    )?;
    within(heap(claim)?, parent)?;
    Ok((parent, registry))
}

pub(super) fn entry_heap(limits: NativeLimits) -> Result<usize, NativeError> {
    limits
        .range
        .max_entry_bytes
        .checked_sub(size_of::<focal_memory::Entry<Key, Row>>())
        .ok_or(NativeError::Capacity("native entry inline charge"))
}

fn result_containers() -> Result<usize, NativeError> {
    add(
        OwnedArtifact::container_charge(),
        add(
            OwnedEvaluation::container_charge(),
            OwnedAccepted::container_charge(),
        )?,
    )
}

fn failure_scratch(parent: usize, registry: usize) -> Result<usize, NativeError> {
    add(array::<ClaimState>(1)?, add(parent, registry)?)
}

/// A nonempty label needs a String slot as well as a byte allocation. Five other
/// possible buffers are kind, metadata, inline payload, inputs and label slots.
/// This conservative bound is valid even when a dimension's own ceiling is lower.
fn descriptor_charge(dynamic: usize) -> Result<usize, NativeError> {
    let labels = dynamic
        .checked_div(size_of::<String>())
        .ok_or(NativeError::Capacity("descriptor label size"))?;
    charged_heap(dynamic, add(5, labels)?)
}

pub(super) fn pinned_descriptor_charge(
    dynamic: usize,
    limits: ArtifactLimits,
) -> Result<usize, NativeError> {
    let mut allocations = 0;
    for possible in [
        limits.kind_bytes,
        limits.metadata_bytes,
        limits.inline_bytes,
        limits.inputs,
        limits.visibility_labels,
    ] {
        allocations = add(allocations, usize::from(possible != 0))?;
    }
    if limits.visibility_label_bytes != 0 {
        let labels = dynamic
            .checked_div(size_of::<String>())
            .ok_or(NativeError::Capacity("descriptor label size"))?;
        allocations = add(allocations, limits.visibility_labels.min(labels))?;
    }
    charged_heap(dynamic, allocations)
}

/// Select internal dimension caps from the complete report allowance. All byte
/// fields share one aggregate construction cap; their maxima need not coexist.
/// Large durable payloads may use a content pointer, independently of inline cap.
/// The most inputs one result artifact admitted under `descriptor` can
/// carry, and so the most `ArtifactInput` index rows its report writes: the
/// configured input limit, never above the model's fixed ceiling.
pub(super) fn input_bound(descriptor: ArtifactLimits) -> usize {
    descriptor.inputs.min(ArtifactLimits::MAX_INPUTS)
}

pub(super) fn descriptor_limits(
    limits: NativeLimits,
    claim: &ClaimState,
    registrations: &RegistrationSet,
) -> Result<ArtifactLimits, NativeError> {
    descriptor_limits_with_cohort(limits, claim, registrations, CohortBudget::empty())
}

fn descriptor_limits_with_cohort(
    limits: NativeLimits,
    claim: &ClaimState,
    registrations: &RegistrationSet,
    cohort: CohortBudget,
) -> Result<ArtifactLimits, NativeError> {
    let (parent, registry) = parent_bound(limits, claim, registrations)?;
    let fixed = add(
        cohort.construction_bytes()?,
        add(
            NativeArtifactInput::container_charge(),
            add(result_containers()?, failure_scratch(parent, registry)?)?,
        )?,
    )?;
    let available = limits
        .preparation_bytes
        .checked_sub(fixed)
        .ok_or(NativeError::Capacity("compound admission report scratch"))?;
    let entry = entry_heap(limits)?;
    for required in [
        OwnedEvaluation::container_charge(),
        OwnedAccepted::container_charge(),
        OwnedEvent::container_charge(),
    ] {
        within(required, entry)?;
    }
    let available = available.min(
        entry
            .checked_sub(OwnedArtifact::container_charge())
            .ok_or(NativeError::Capacity("artifact entry charge"))?,
    );
    // Binary search is bounded by usize width and allocates no buffer. Checking
    // overflow is equivalent to refusing an unrepresentable candidate charge.
    let mut low = 0usize;
    let mut high = available;
    while low < high {
        let span = high
            .checked_sub(low)
            .ok_or(NativeError::Capacity("descriptor interval"))?;
        let middle = add(low, add(span / 2, span % 2)?)?;
        if descriptor_charge(middle).is_ok_and(|charge| charge <= available) {
            low = middle;
        } else {
            high = middle
                .checked_sub(1)
                .ok_or(NativeError::Capacity("descriptor interval"))?;
        }
    }
    // Every non-Complete report must be able to describe durable diagnostics.
    // A content pointer needs no inline heap; kind="error" needs five bytes.
    if low < "error".len() {
        // A constrained owner may still support a minimal durable diagnostic
        // reference. Pin all optional heaps to zero rather than price nonexistent
        // buffers or silently admit an unaccounted inline payload.
        if charged_heap("error".len(), 1)? > available {
            return Err(NativeError::Capacity(
                "minimum admission diagnostic descriptor",
            ));
        }
        return Ok(ArtifactLimits {
            kind_bytes: "error".len(),
            metadata_bytes: 0,
            inline_bytes: 0,
            inputs: 0,
            visibility_labels: 0,
            visibility_label_bytes: 0,
            construction_bytes: add(size_of::<ArtifactDescriptor>(), "error".len())?,
        });
    }
    Ok(ArtifactLimits {
        kind_bytes: low,
        metadata_bytes: low,
        inline_bytes: low,
        inputs: super::index_rows::input_bound(limits)
            .min(limits.plan_edges)
            .min(
                low.checked_div(size_of::<ObjectRef>())
                    .ok_or(NativeError::Capacity("descriptor input size"))?,
            ),
        visibility_labels: low
            .checked_div(size_of::<String>())
            .ok_or(NativeError::Capacity("descriptor label size"))?,
        visibility_label_bytes: low,
        construction_bytes: add(size_of::<ArtifactDescriptor>(), low)?,
    })
}

impl CompletionEnvelope {
    #[allow(clippy::too_many_arguments)] // Private owner contract, not participant configuration.
    pub(super) fn derive(
        rows: &super::ranges::NativeRanges,
        limits: NativeLimits,
        claim: &ClaimState,
        registrations: &RegistrationSet,
        declaration: &validation::Declaration,
        mut descriptor: ArtifactLimits,
        evidence: EvidenceBounds,
    ) -> Result<Self, NativeError> {
        let target = CompletionTarget::of(declaration)?;
        // WholeWork needs the actual response, complete graph and future
        // projection. Its dedicated constructor cannot be bypassed here.
        if target == CompletionTarget::Work {
            return Err(ContractError::InvalidTarget.into());
        }
        if declaration.attempt_bound() == 0 {
            return Err(ContractError::InvalidTransition.into());
        }
        claim.acceptance().check_declaration(declaration)?;
        registrations.check(claim)?;
        if !registrations
            .rows()
            .iter()
            .any(|row| row.binding() == declaration.binding() && target.matches(row.target()))
        {
            return Err(ContractError::InvalidTarget.into());
        }
        let operation = match target {
            CompletionTarget::Admission => NativeOperation::ReportAdmission,
            CompletionTarget::Increment => NativeOperation::ReportIncrement,
            CompletionTarget::Work => return Err(ContractError::InvalidTarget.into()),
        };
        let failure_possible =
            target == CompletionTarget::Admission && declaration.mode() == ValidationMode::Required;
        let cohort = if failure_possible {
            cohort_bound(limits, claim, registrations)?
        } else {
            CohortBudget::empty()
        };
        let maximum_events = if target == CompletionTarget::Admission
            && declaration.mode() == ValidationMode::Required
        {
            4
        } else {
            3
        };
        check_journal_visits(limits, claim, maximum_events)?;
        check_cohort_visits(limits, cohort, 1, 4, maximum_events)?;
        if target == CompletionTarget::Admission && claim.status() == ClaimStatus::Posted {
            // Begin already checks the current projection through authority.
            // Reconstruction must retain that same future-work promise even
            // when this owner was created with smaller traversal limits.
            if focal_model::lifecycle::aggregation::admission_completion_visits(
                claim,
                registrations,
            )? > limits.plan_edges
            {
                return Err(NativeError::Capacity(
                    "admission completion projection visits",
                ));
            }
        }
        let construction =
            ConstructionBudget::for_operation(operation, limits)?.with_cohort(cohort, limits)?;
        let batch = limits.range.max_batch_entries;
        // The promised result artifact cites no more inputs than leave room in
        // one batch for the report's other rows: nine primary rows, the
        // artifact's four fixed index rows and its verdict, or for a failed
        // report eleven rows, the cohort's rows and every status move (22 §7).
        // The reported evaluation and every sealed cohort evaluation may
        // retire its due timer.
        let timers = crate::native::index_rows::timer_rows(0, add(1, cohort.evaluations())?, 0)?;
        let report_fixed = crate::native::index_rows::report_rows(0)?;
        let fixed_rows = if failure_possible {
            let moved = add(1, cohort.claims())?;
            add(
                add(
                    add(11, cohort.changed_keys())?,
                    add(report_fixed, add(moved, moved)?)?,
                )?,
                timers,
            )?
        } else {
            add(add(9, report_fixed)?, timers)?
        };
        descriptor.inputs =
            crate::native::index_rows::cap_inputs(input_bound(descriptor), fixed_rows, batch);
        let promised_index = |moved: usize| {
            add(
                crate::native::index_rows::report_rows(input_bound(descriptor))?,
                add(add(moved, moved)?, timers)?,
            )
        };
        match target {
            CompletionTarget::Admission => construction.check_counts(
                1,
                add(4, cohort.evaluations())?,
                add(4, cohort.events())?,
                promised_index(add(1, cohort.claims())?)?,
            )?,
            CompletionTarget::Increment => {
                construction.check_counts(0, 4, 3, promised_index(0)?)?
            }
            CompletionTarget::Work => return Err(ContractError::InvalidTarget.into()),
        }
        let (parent_heap, registry_heap) = parent_bound(limits, claim, registrations)?;
        let maximum = descriptor_limits_with_cohort(limits, claim, registrations, cohort)?;
        descriptor.construction_bytes = descriptor
            .construction_bytes
            .min(maximum.construction_bytes);
        descriptor.kind_bytes = descriptor.kind_bytes.min(maximum.kind_bytes);
        descriptor.metadata_bytes = descriptor.metadata_bytes.min(maximum.metadata_bytes);
        descriptor.inline_bytes = descriptor.inline_bytes.min(maximum.inline_bytes);
        descriptor.inputs = descriptor.inputs.min(maximum.inputs);
        descriptor.visibility_labels = descriptor.visibility_labels.min(maximum.visibility_labels);
        descriptor.visibility_label_bytes = descriptor
            .visibility_label_bytes
            .min(maximum.visibility_label_bytes);
        let dynamic = descriptor
            .construction_bytes
            .checked_sub(size_of::<ArtifactDescriptor>())
            .ok_or(NativeError::Capacity("descriptor construction bytes"))?;
        if descriptor.kind_bytes < "error".len() || dynamic < "error".len() {
            return Err(NativeError::Capacity(
                "minimum admission diagnostic descriptor",
            ));
        }
        let descriptor_heap = pinned_descriptor_charge(dynamic, descriptor)?;
        let input_heap = add(NativeArtifactInput::container_charge(), descriptor_heap)?;
        // Increment outcomes constrain later acceptance. They do not replace
        // the work or claim row, even when the check is Required and fails.
        let scratch = add(
            cohort.construction_bytes()?,
            add(
                add(input_heap, result_containers()?)?,
                match target {
                    CompletionTarget::Admission => failure_scratch(parent_heap, registry_heap)?,
                    CompletionTarget::Increment => 0,
                    CompletionTarget::Work => return Err(ContractError::InvalidTarget.into()),
                },
            )?,
        )?;
        within(scratch, construction.scratch_bytes)?;
        let regular_heap = add(
            add(descriptor_heap, result_containers()?)?,
            event_containers(3)?,
        )?;
        // A report indexes its result artifact and verdict; a failed report
        // also moves the parent and every sealed cohort claim between status
        // keys (doc 22 §7).
        let report_index = crate::native::index_rows::report_rows(input_bound(descriptor))?;
        let ordinary_keys = add(add(9, report_index)?, timers)?;
        let ordinary_report = rows.future_write_envelope(
            RangeWriteLimits {
                changed_keys: ordinary_keys,
                deleted_keys: timers,
                deleted_heap: 0,
                incoming_heap: regular_heap,
                input_capacity: ordinary_keys,
            },
            limits.max_ranges,
        )?;
        let failed_report = if failure_possible {
            let moved = add(1, cohort.claims())?;
            let changes = add(
                add(11, cohort.changed_keys())?,
                add(report_index, add(add(moved, moved)?, timers)?)?,
            )?;
            Some(rows.future_write_envelope(
                RangeWriteLimits {
                    changed_keys: changes,
                    deleted_keys: add(moved, timers)?,
                    deleted_heap: 0,
                    incoming_heap: add(
                        add(regular_heap, cohort.incoming_heap()?)?,
                        add(
                            add(containers(1)?, add(parent_heap, registry_heap)?)?,
                            event_containers(1)?,
                        )?,
                    )?,
                    input_capacity: changes,
                },
                limits.max_ranges,
            )?)
        } else {
            None
        };
        let reports = declaration.attempt_bound();
        let count = usize::try_from(reports).map_err(|_| NativeError::Capacity("report count"))?;
        let failed_journal_bytes = if cohort.claims() == 0 {
            0
        } else {
            super::completion_book::journal_bytes(add(1, cohort.evaluations())?)?
        };
        let regular_retained = crate::native::mutation::retained(ordinary_report)?;
        let retained = if let Some(failed) = failed_report {
            add(
                multiply(
                    count
                        .checked_sub(1)
                        .ok_or(NativeError::Capacity("report count"))?,
                    regular_retained,
                )?,
                add(
                    crate::native::mutation::retained(failed)?,
                    failed_journal_bytes,
                )?,
            )?
        } else {
            multiply(count, regular_retained)?
        };
        let transient = |envelope: RangeWriteEnvelope| {
            envelope
                .additional_peak_bytes()
                .checked_sub(envelope.additional_retained_bytes())
                .ok_or(NativeError::Capacity("range temporary charge"))
        };
        let range_workspace =
            transient(ordinary_report)?.max(failed_report.map(transient).transpose()?.unwrap_or(0));
        // The owner serializes verification/construction. Charge one workspace,
        // never one peak for every possible attempt. Ingress may have its own
        // permit while Core's conservative input precharges coexist with it.
        let workspace = add(
            add(
                input_heap,
                add(evidence.workspace_bytes, evidence.retained_bytes)?,
            )?,
            add(construction.temporary_bytes()?, range_workspace)?,
        )?;
        let failure_events = add(1, cohort.events())?;
        // A report's new rows: artifact, identity, accepted result, outcome,
        // three events, and the artifact's and verdict's index rows (22 §7).
        // A failed report moves status keys without adding rows.
        let slot_demand = SlotDemand {
            per_report: CompletionSlots {
                artifacts: 1,
                identities: 1,
                results: 1,
                outcomes: 1,
                events: 3,
                sequences: 1,
                new_rows: add(7, report_index)?,
                ..CompletionSlots::default()
            },
            failure: failed_report.map(|_| CompletionSlots {
                events: failure_events,
                new_rows: failure_events,
                ..CompletionSlots::default()
            }),
        };
        let slots = slot_demand.remaining(reports, failed_report.is_some())?;
        let result = Self {
            record_buffers: None,
            target,
            parent: claim.binding(),
            policy: claim.acceptance().intent_fingerprint(),
            scope_limits: claim.scopes().limits(),
            max_responses: claim.max_responses(),
            parent_heap,
            registry_heap,
            registrations: registrations.rows().len(),
            max_registrations: registry_bound(limits, claim, registrations)?.0,
            registry_limit: registrations.max_rows(),
            descriptor,
            input_heap,
            reports,
            ordinary_report,
            failed_report,
            ordinary_journal_bytes: 0,
            failed_journal_bytes,
            retained,
            workspace,
            required: add(retained, workspace)?,
            slot_demand,
            slots,
            cohort,
            work: None,
            graph: None,
        };
        result.check_parent(claim, registrations)?;
        Ok(result)
    }

    pub(super) fn descriptor_limits(self) -> ArtifactLimits {
        self.descriptor
    }
    pub(super) fn cohort(self) -> CohortBudget {
        self.cohort
    }
    pub(super) fn supports_target(self, target: EvaluationTarget) -> bool {
        matches!(
            (self.target, target),
            (CompletionTarget::Admission, EvaluationTarget::Admission)
                | (
                    CompletionTarget::Increment,
                    EvaluationTarget::Increment { .. }
                )
                | (CompletionTarget::Work, EvaluationTarget::Work { .. })
        )
    }
    pub(super) fn reports(self) -> u32 {
        self.reports
    }
    pub(super) fn slots(self) -> CompletionSlots {
        self.slots
    }
    pub(super) fn remaining_slots(
        self,
        remaining_reports: u32,
        failure_available: bool,
    ) -> Result<CompletionSlots, NativeError> {
        if remaining_reports > self.reports {
            return Err(ContractError::InvalidTransition.into());
        }
        self.slot_demand
            .remaining(remaining_reports, failure_available)
    }

    /// Exercise heterogeneous accounting before any new target is activated.
    #[cfg(test)]
    pub(super) fn with_slot_demand_for_test(
        mut self,
        per_report: CompletionSlots,
        failure: Option<CompletionSlots>,
    ) -> Result<Self, NativeError> {
        if failure.is_some() != self.failed_report.is_some() {
            return Err(ContractError::InvalidTransition.into());
        }
        self.slot_demand = SlotDemand {
            per_report,
            failure,
        };
        self.slots = self.remaining_slots(self.reports, failure.is_some())?;
        Ok(self)
    }
    pub(super) fn total_retained_bytes(self) -> usize {
        self.retained
    }
    pub(super) fn workspace_bytes(self) -> usize {
        self.workspace
    }
    pub(super) fn required_bytes(self) -> usize {
        self.required
    }
    pub(super) fn report_storage(
        self,
        usage: CompletionUse,
    ) -> Result<RangeWriteEnvelope, NativeError> {
        match usage {
            CompletionUse::AdmissionFailure => self
                .failed_report
                .ok_or(ContractError::InvalidTransition.into()),
            CompletionUse::Regular => Ok(self.ordinary_report),
        }
    }
    pub(super) fn per_report_retained_bytes(
        self,
        usage: CompletionUse,
    ) -> Result<usize, NativeError> {
        let storage = self.report_storage(usage)?;
        let records = self
            .record_buffers
            .map(|limits| record_codec::future_record_bytes(storage, limits))
            .transpose()?
            .unwrap_or(0);
        add(
            add(crate::native::mutation::retained(storage)?, records)?,
            match usage {
                CompletionUse::Regular => self.ordinary_journal_bytes,
                CompletionUse::AdmissionFailure => self.failed_journal_bytes,
            },
        )
    }

    pub(super) fn with_record_buffers(
        mut self,
        limits: record_codec::EncodingLimits,
    ) -> Result<Self, NativeError> {
        if self.record_buffers.is_some() {
            return Err(ContractError::InvalidTransition.into());
        }
        let regular = record_codec::future_record_bytes(self.ordinary_report, limits)?;
        let records = if let Some(failed) = self.failed_report {
            add(
                multiply(
                    usize::try_from(self.reports.checked_sub(1).ok_or(ContractError::Capacity)?)
                        .map_err(|_| ContractError::Capacity)?,
                    regular,
                )?,
                record_codec::future_record_bytes(failed, limits)?,
            )?
        } else {
            multiply(
                usize::try_from(self.reports).map_err(|_| ContractError::Capacity)?,
                regular,
            )?
        };
        self.retained = add(self.retained, records)?;
        self.required = add(self.required, records)?;
        self.record_buffers = Some(limits);
        Ok(self)
    }

    /// A due deadline substitutes a smaller authority-only write for the
    /// unfinished report chain. Prove this at Begin/reconstruction, before the
    /// owner promises completion, and repeat it when lending the held source.
    pub(super) fn deadline_storage(
        self,
        limits: NativeLimits,
    ) -> Result<RangeWriteEnvelope, NativeError> {
        let construction =
            ConstructionBudget::for_operation(NativeOperation::EvaluationDeadline, limits)?;
        within(4, construction.max_changes)?;
        within(1, construction.max_events)?;
        within(1, construction.extras_count)?;
        within(
            OwnedEvaluation::container_charge(),
            construction.scratch_bytes,
        )?;
        let storage = self.report_storage(CompletionUse::Regular)?;
        let dimensions = storage.limits();
        within(construction.max_changes, dimensions.changed_keys)?;
        within(construction.max_changes, dimensions.input_capacity)?;
        within(
            add(
                construction.scratch_bytes,
                event_containers(construction.max_events)?,
            )?,
            dimensions.incoming_heap,
        )?;
        let transient = storage
            .additional_peak_bytes()
            .checked_sub(storage.additional_retained_bytes())
            .ok_or(NativeError::Capacity("deadline range temporary charge"))?;
        within(
            add(construction.temporary_bytes()?, transient)?,
            self.workspace_bytes(),
        )?;
        self.remaining_slots(1, false)?
            .checked_sub(CompletionSlots {
                artifacts: 0,
                identities: 0,
                results: 0,
                outcomes: 1,
                events: 1,
                sequences: 1,
                new_rows: 2,
                ..CompletionSlots::default()
            })?;
        Ok(storage)
    }

    /// Apply before custody or construction. Schemas/provenance and semantic
    /// input visibility remain checked by the exact owner/definition contract.
    pub(super) fn check_descriptor(&self, input: &NativeArtifactInput) -> Result<(), NativeError> {
        let descriptor = input.get().ok_or(ContractError::MissingEvidence)?;
        // NativeArtifactInput's private fallible constructor retains exactly
        // one descriptor; ArtifactView prices that same singleton capacity.
        self.check_descriptor_view(descriptor)
    }

    pub(super) fn check_descriptor_view(
        &self,
        descriptor: &impl super::report_artifact::ArtifactView,
    ) -> Result<(), NativeError> {
        super::report_artifact::check_limits(
            descriptor,
            self.descriptor_limits(),
            self.input_heap,
            "pinned report descriptor dimensions",
        )
    }

    /// The owner must also apply this before publishing mutations that grow a
    /// protected parent. Checking only when a report arrives would be too late
    /// to prevent unrelated admission from invalidating its promised allowance.
    pub(super) fn check_parent(
        &self,
        claim: &ClaimState,
        registrations: &RegistrationSet,
    ) -> Result<(), NativeError> {
        self.check_parent_facts(&ParentFacts::new(claim, registrations)?)
    }

    /// Constant work per grant: no collection scan, hash, copy or allocation.
    /// The owner separately checks exact registration membership for this grant.
    pub(super) fn check_parent_facts(&self, facts: &ParentFacts<'_>) -> Result<(), NativeError> {
        if self.failed_report.is_some() && facts.posted {
            facts.binding.next()?;
        }
        Binding {
            revision: self.parent.revision,
            ..facts.binding
        }
        .check(&self.parent)?;
        if facts.binding.revision < self.parent.revision
            || facts.policy != self.policy
            || facts.scope_limits != self.scope_limits
            || facts.max_responses != self.max_responses
            || !u32::try_from(facts.responses).is_ok_and(|count| count <= self.max_responses)
            || facts.registry_limit != self.registry_limit
            || facts.registrations < self.registrations
            || facts.registrations > self.max_registrations
        {
            return Err(ContractError::InvalidPolicy.into());
        }
        within(facts.heap, self.parent_heap)?;
        within(facts.registry_heap, self.registry_heap)
    }
}
