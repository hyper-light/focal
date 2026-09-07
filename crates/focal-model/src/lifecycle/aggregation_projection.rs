//! Bounded, temporary qualification of the sole owner's immutable effective
//! prefix. Source lookups are trusted owner adapters, never participant filters.
//! This is neither a durable schema nor another independently mutable ledger.
use super::*;
use crate::lifecycle::claim::ClaimState;
use crate::lifecycle::evidence::{Response, WorkArtifact};
use crate::lifecycle::validation::{Declaration, EvaluationState};

#[path = "aggregation_projection_reduce.rs"]
mod reduce;
#[path = "aggregation_projection_source.rs"]
mod source;
#[cfg(test)]
#[path = "aggregation_projection_tests.rs"]
mod tests;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct PublicationPosition {
    pub sequence: SessionSeq,
    pub ordinal: u32,
}

#[derive(Debug, Clone, Copy)]
pub struct PublishedResponse<'a> {
    pub response: &'a Response,
    pub received: Option<PublicationPosition>,
    pub entered: Option<PublicationPosition>,
}

#[derive(Debug, Clone, Copy)]
pub struct PublishedResult<'a> {
    pub result: &'a AcceptedResult,
    pub position: PublicationPosition,
}

/// Every lookup must resolve the same actual effective prefix, including staged
/// rows, and remain stable for the plan/projection borrow. No authored selection
/// may remove a response, declaration, registration, work row or accepted fact.
pub trait WholeWorkView {
    fn prefix(&self) -> SessionSeq;
    fn declaration(&self, id: ValidationId) -> Option<&Declaration>;
    fn evaluation(&self, registered: RegisteredEvaluation) -> Option<&EvaluationState>;
    fn accepted(&self, result: &AcceptedResult) -> Option<PublishedResult<'_>>;
    fn response(&self, id: TestamentId) -> Option<PublishedResponse<'_>>;
    fn work(&self, id: ArtifactId) -> Option<&WorkArtifact>;
    /// Complete owner-maintained cycle/work membership, including unclosed work.
    /// The cursor must report broken links/counts as errors rather than truncate.
    fn works(&self, claim: ClaimId) -> impl Iterator<Item = Result<&WorkArtifact, ContractError>>;
}

#[derive(Debug, Clone, Copy)]
pub struct ProjectionLimits {
    pub responses: usize,
    pub works: usize,
    /// Maximum total response/declared-slot cells, including missing slots.
    pub slots: usize,
    pub evaluations: usize,
    pub declarations: usize,
    pub visits: usize,
    pub bytes: usize,
}

#[derive(Debug, Clone, Copy)]
struct Counts {
    responses: usize,
    slots: usize,
    results: usize,
    events: usize,
}

struct Visits {
    remaining: usize,
}
impl Visits {
    fn new(limit: usize) -> Self {
        Self { remaining: limit }
    }
    fn take(&mut self, count: usize) -> Result<(), ContractError> {
        self.remaining = self
            .remaining
            .checked_sub(count)
            .ok_or(ContractError::Capacity)?;
        Ok(())
    }
}

pub struct ProjectionPlan<'a, V: WholeWorkView> {
    claim: &'a ClaimState,
    registrations: &'a RegistrationSet,
    view: &'a V,
    limits: ProjectionLimits,
    counts: Counts,
    charge: usize,
}

pub(super) struct ProjectedWitness {
    pub(super) response: TestamentId,
    pub(super) slot: u32,
    pub(super) artifact: ArtifactRef,
    pub(super) checks: usize,
}

struct ProjectedResponse<'a> {
    source: PublishedResponse<'a>,
    active: bool,
    started: bool,
    outcome: ResponseOutcome,
    delivery_ready: bool,
    dirty: bool,
    artifacts_start: usize,
    artifacts_len: usize,
}

/// Checked proof buffers borrow the canonical definition/policy. All vectors
/// have one bounded construction owner; there is no per-response policy copy.
pub struct WholeWorkProjection<'a> {
    claim: &'a ClaimState,
    sequence: SessionSeq,
    responses: Vec<ProjectedResponse<'a>>,
    artifacts: Vec<ArtifactDecision>,
    witnesses: Vec<ProjectedWitness>,
    checks: Vec<CheckWitness>,
    nonartifact: Vec<NonArtifactWitness>,
    delivery: Option<DeliveryWitness>,
    delivery_results: Vec<AcceptedResult>,
    outcome: AggregateOutcome,
    increments_ready: bool,
    charge: usize,
}

pub fn prepare_projection<'a, V: WholeWorkView>(
    claim: &'a ClaimState,
    registrations: &'a RegistrationSet,
    view: &'a V,
    limits: ProjectionLimits,
) -> Result<ProjectionPlan<'a, V>, ContractError> {
    let counts = source::inspect(claim, registrations, view, limits)?;
    let charge = reduce::construction_charge(claim, counts)?;
    if charge > limits.bytes {
        return Err(ContractError::Capacity);
    }
    Ok(ProjectionPlan {
        claim,
        registrations,
        view,
        limits,
        counts,
        charge,
    })
}

impl<'a, V: WholeWorkView> ProjectionPlan<'a, V> {
    /// Additional peak including inline projection state and allocator metadata;
    /// existing source rows/pinned roots remain charged separately by the owner.
    pub fn construction_charge(&self) -> usize {
        self.charge
    }
    pub fn build(self) -> Result<WholeWorkProjection<'a>, ContractError> {
        reduce::build(self)
    }
}

impl WholeWorkProjection<'_> {
    pub fn claim_decision(&self) -> ClaimDecision<'_> {
        ClaimDecision {
            binding: self.claim.binding(),
            sequence: self.sequence,
            outcome: self.outcome,
            delivery: self.delivery,
            acceptance: self.claim.acceptance(),
            delivery_results: &self.delivery_results,
            nonartifact: &self.nonartifact,
            increments_ready: self.increments_ready,
            coverage: CoverageView::Projected(&self.witnesses, &self.checks),
        }
    }
    pub fn response_decision(&self, id: TestamentId) -> Option<ResponseDecision<'_>> {
        let row = self
            .responses
            .iter()
            .find(|row| row.source.response.identity().binding.object.0 == id.0)?;
        if !row.started {
            return None;
        }
        let end = row.artifacts_start.checked_add(row.artifacts_len)?;
        Some(ResponseDecision {
            claim: self.claim.binding(),
            response: row.source.response.identity().binding,
            receipt: row.source.response.identity().receipt,
            sequence: self.sequence,
            outcome: row.outcome,
            artifacts: self.artifacts.get(row.artifacts_start..end)?,
        })
    }
    pub fn construction_charge(&self) -> usize {
        self.charge
    }
}
