//! Complete admission projection against independently restored facts. This
//! checks recorded requirements and canonical failure choice without executing
//! validators or granting present-day authority to a historical participant.
use super::*;
use focal_model::lifecycle::aggregation::{
    self, AdmissionOutcome, AdmissionView, PublishedAdmissionResult, RegisteredEvaluation,
};
use focal_model::lifecycle::claim::ClaimTerminalCut;
use std::cell::Cell;

struct Read<'a, 'v, 'r> {
    read: &'a ValidationRead<'v, 'r>,
    claim: ClaimId,
    failed: Cell<bool>,
}
impl Read<'_, '_, '_> {
    fn row(&self, key: Key) -> Option<&Row> {
        match self.read.get(key) {
            Ok(value) => value,
            Err(_) => {
                self.failed.set(true);
                None
            }
        }
    }
}
impl AdmissionView for Read<'_, '_, '_> {
    fn prefix(&self) -> SessionSeq {
        self.read.prefix
    }
    fn declaration(&self, id: ValidationId) -> Option<&validation::Declaration> {
        match self.row(Key::Definition(id))? {
            Row::Definition(value) => value.get(),
            _ => None,
        }
    }
    fn evaluation(&self, registered: RegisteredEvaluation) -> Option<&validation::EvaluationState> {
        let key = EvaluationKey {
            claim: self.claim,
            validation: ValidationId(registered.binding().object.0),
            target: EvaluationTarget::of(registered.target()),
            generation: registered.generation(),
        };
        match self.row(Key::Evaluation(key))? {
            Row::Evaluation(value) => value.get(),
            _ => None,
        }
    }
    fn accepted(
        &self,
        result: &validation::AcceptedResult,
    ) -> Option<PublishedAdmissionResult<'_>> {
        let Row::Accepted(value) = self.row(Key::Accepted(NativeResultKey::of(*result)))? else {
            return None;
        };
        let value = value.get()?;
        Some(PublishedAdmissionResult {
            result: value.result_ref(),
            sequence: value.sequence(),
            ordinal: value.ordinal(),
        })
    }
}
pub(super) fn validate(
    owned: &OwnedClaim,
    read: &ValidationRead<'_, '_>,
) -> Result<(), NativeError> {
    let claim = owned.claim().ok_or_else(invalid)?;
    let registrations = owned.registrations().ok_or_else(invalid)?;
    // The quote itself checks/hashes the immutable policy. Price its complete
    // bounded traversal before asking the model for the projection allowance.
    let mut fields = sum(
        sum(
            claim.acceptance().declarations().len(),
            registrations.rows().len(),
        )?,
        1,
    )?;
    let mut slots = claim.acceptance().slots();
    loop {
        read.charge(1)?;
        let Some(slot) = slots.next() else {
            break;
        };
        fields = sum(fields, sum(slot.checks.len(), 1)?)?;
    }
    read.charge(fields.checked_mul(1024).ok_or(ContractError::Capacity)?)?;
    let visits = aggregation::admission_completion_visits(claim, registrations)?;
    read.charge(visits)?;
    let view = Read {
        read,
        claim: ClaimId(claim.binding().object.0),
        failed: Cell::new(false),
    };
    let result = aggregation::project_admission(
        claim,
        registrations,
        &view,
        aggregation::AdmissionLimits {
            declarations: read.limits.definitions,
            evaluations: read.limits.evaluations_per_claim,
            visits,
        },
    );
    if view.failed.get() {
        return Err(ContractError::Capacity.into());
    }
    let decision = result?;
    match decision.outcome() {
        AdmissionOutcome::Passed => (),
        AdmissionOutcome::Pending => {
            if claim.receipt().is_some() {
                return Err(invalid());
            }
        }
        AdmissionOutcome::Blocked(required) => {
            if claim.receipt().is_some() {
                return Err(invalid());
            }
            let terminal = match claim.terminal_cut().ok_or_else(invalid)? {
                ClaimTerminalCut::Explicit(cut) => cut.position,
                ClaimTerminalCut::Required(cut) => cut.sequence(),
                ClaimTerminalCut::Graph(cut) => cut.sequence(),
            };
            if terminal > required.sequence() {
                return Err(invalid());
            }
        }
    }
    Ok(())
}
