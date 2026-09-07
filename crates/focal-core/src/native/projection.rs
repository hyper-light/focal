//! Borrow the sole owner's actual effective prefix. Projection buffers live
//! under one query reservation; neither the source nor the proof escapes its
//! owner's borrow. Mutations must separately fund and publish all consequences.
use super::*;
use focal_memory::{BudgetKind, BudgetLane};
use focal_model::lifecycle::aggregation::{
    self, ProjectionLimits, PublicationPosition, PublishedResponse, PublishedResult,
    RegisteredEvaluation, WholeWorkProjection, WholeWorkView,
};

pub(super) struct ProjectionRows<'a, 'b> {
    pub(super) view: &'a View<'b>,
    pub(super) claim: ClaimId,
    pub(super) limits: NativeLimits,
    pub(super) staged: Option<&'a prepare::Extras>,
    pub(super) sequence: SessionSeq,
}
impl ProjectionRows<'_, '_> {
    fn get(&self, key: Key) -> Option<&Row> {
        self.staged
            .and_then(|extras| {
                extras
                    .rows
                    .iter()
                    .find(|row| row.key == key)
                    .map(|row| &row.row)
            })
            .or_else(|| self.view.get(key))
    }
}
impl WholeWorkView for ProjectionRows<'_, '_> {
    fn prefix(&self) -> SessionSeq {
        self.sequence
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
        match result.phase() {
            validation::Phase::Delivery => {
                let accepted =
                    super::response_reads::as_delivery(self.get(Key::DeliveryResult(key)))?;
                Some(PublishedResult {
                    result: accepted.result_ref(),
                    position: PublicationPosition {
                        sequence: accepted.sequence(),
                        ordinal: accepted.ordinal(),
                    },
                })
            }
            validation::Phase::MissingTarget => {
                let accepted =
                    super::response_reads::as_missing(self.get(Key::MissingResult(key)))?;
                Some(PublishedResult {
                    result: accepted.result_ref(),
                    position: PublicationPosition {
                        sequence: accepted.sequence(),
                        ordinal: accepted.ordinal(),
                    },
                })
            }
            validation::Phase::Programmatic | validation::Phase::Quality => {
                let accepted = as_result(self.get(Key::Accepted(key)))?;
                Some(PublishedResult {
                    result: accepted.result_ref(),
                    position: PublicationPosition {
                        sequence: accepted.sequence(),
                        ordinal: accepted.ordinal(),
                    },
                })
            }
        }
    }
    fn response(&self, id: TestamentId) -> Option<PublishedResponse<'_>> {
        let record = super::response_reads::as_response_record(self.get(Key::Response(id)))?;
        Some(PublishedResponse {
            response: record.response(),
            received: record.received(),
            entered: record.entered(),
        })
    }
    fn work(&self, id: ArtifactId) -> Option<&evidence::WorkArtifact> {
        as_work(self.get(Key::Work(id))).map(|row| &row.state)
    }
    fn works(
        &self,
        claim: ClaimId,
    ) -> impl Iterator<Item = Result<&evidence::WorkArtifact, ContractError>> {
        super::projection_work::works(self.view, claim, self.limits).map(|old| {
            let old = old?;
            self.work(old.reference().id)
                .ok_or(ContractError::MissingEvidence)
        })
    }
}

pub(super) fn with_projection<T>(
    view: &View<'_>,
    id: ClaimId,
    limits: NativeLimits,
    source: &MemoryBudget,
    project: impl FnOnce(&WholeWorkProjection<'_>) -> T,
) -> Result<T, NativeError> {
    let claim = view.claim(id).ok_or(ContractError::InvalidTarget)?;
    let registrations = view
        .owned_claim(id)?
        .registrations()
        .ok_or(ContractError::InvalidPolicy)?;
    let rows = ProjectionRows {
        view,
        claim: id,
        limits,
        staged: None,
        sequence: view.prefix(),
    };
    let plan = aggregation::prepare_projection(
        claim,
        registrations,
        &rows,
        ProjectionLimits {
            responses: limits.responses,
            slots: limits.plan_edges,
            works: limits.plan_edges,
            evaluations: limits.evaluations_per_claim,
            declarations: limits.definitions,
            visits: limits.plan_edges,
            bytes: limits.preparation_bytes,
        },
    )?;
    // The reservation precedes every projection allocation and outlives the
    // temporary proof buffers, including participant callback failure/unwind.
    let reservation = source.reserve(
        BudgetKind::Query,
        BudgetLane::Ordinary,
        plan.construction_charge(),
    )?;
    let projection = plan.build()?;
    let result = project(&projection);
    drop(projection);
    drop(reservation);
    Ok(result)
}

/// Internal mutation projection over already checked provisional rows. The
/// enclosing Pending/Completion allowance owns all scratch, so no Query lane or
/// unreserved allocation is introduced midway through an atomic mutation.
#[allow(clippy::too_many_arguments)]
pub(super) fn with_staged<T>(
    view: &View<'_>,
    claim: &ClaimState,
    registry: &RegistrationSet,
    staged: &prepare::Extras,
    sequence: SessionSeq,
    limits: NativeLimits,
    scratch: &mut prepare::Scratch,
    project: impl FnOnce(&WholeWorkProjection<'_>, &mut prepare::Scratch) -> Result<T, NativeError>,
) -> Result<T, NativeError> {
    let id = ClaimId(claim.binding().object.0);
    let old = view.claim(id).ok_or(ContractError::InvalidTarget)?;
    if sequence.0
        != view
            .prefix()
            .0
            .checked_add(1)
            .ok_or(ContractError::Capacity)?
        || claim.binding().revision < old.binding().revision
    {
        return Err(ContractError::InvalidCut.into());
    }
    Binding {
        revision: old.binding().revision,
        ..claim.binding()
    }
    .check(&old.binding())?;
    let rows = ProjectionRows {
        view,
        claim: id,
        limits,
        staged: Some(staged),
        sequence,
    };
    let plan = aggregation::prepare_projection(
        claim,
        registry,
        &rows,
        ProjectionLimits {
            responses: limits.responses,
            works: limits.plan_edges,
            slots: limits.plan_edges,
            evaluations: limits.evaluations_per_claim,
            declarations: limits.definitions,
            visits: limits.plan_edges,
            bytes: scratch.remaining()?,
        },
    )?;
    scratch.charge(plan.construction_charge())?;
    let projection = plan.build()?;
    project(&projection, scratch)
}
