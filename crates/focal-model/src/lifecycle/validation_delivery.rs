//! Pure Receipt owner facts from an actual observed respondent-authored response.
//! These checks neither run an evaluator nor manufacture an evidence artifact.
use super::*;
use crate::lifecycle::{
    claim::ClaimState,
    evidence::{Parent, Response, ResponseState},
};

#[cfg(test)]
#[path = "validation_delivery_tests.rs"]
mod tests;

fn received_target(claim: &ClaimState, response: &Response) -> Result<Target, ContractError> {
    let parent = Parent::from_claim(claim)?;
    parent.require_open_response()?;
    let identity = response.identity();
    if identity.claim != parent.claim {
        return Err(ContractError::WrongObject);
    }
    if response.state() != ResponseState::Received {
        return Err(ContractError::InvalidTransition);
    }
    claim.received_report(identity.binding, identity.receipt, response.report_stamp())?;
    Ok(Target::Delivery {
        response: identity.binding,
    })
}

impl<'a> Evaluation<'a> {
    /// Materialize the committed pure Receipt declaration only after the actual
    /// claimant receipt has been recorded on this claim and this exact response.
    /// The response cycle assigns the generation; it is not a participant option.
    pub fn materialize_delivery(
        principal: Principal,
        declaration: &'a Declaration,
        claim: &ClaimState,
        response: &Response,
    ) -> Result<Self, ContractError> {
        principal.require_actor(claim.issuer())?;
        claim.acceptance().check_declaration(declaration)?;
        if declaration.target() != TargetDeclaration::Delivery {
            return Err(ContractError::InvalidTarget);
        }
        let target = received_target(claim, response)?;
        Self::materialize(
            principal,
            declaration,
            Materialization {
                binding: declaration.binding(),
                target,
                slot_name: None,
                generation: u64::from(response.identity().cycle),
                receipt: Some(response.identity().receipt),
            },
        )
    }

    /// Recover the exact owner frame without caller-authored readiness or policy
    /// flags. `receive_delivery` still enforces the committed logical deadline;
    /// obtaining this frame alone does not mean a late receipt passed validation.
    pub fn delivery_owner(
        &self,
        claim: &ClaimState,
        response: &Response,
        logical_time: u64,
    ) -> Result<OwnerState, ContractError> {
        claim.acceptance().check_declaration(self.declaration)?;
        let target = received_target(claim, response)?;
        let identity = response.identity();
        if self.target != target || self.declaration.target() != TargetDeclaration::Delivery {
            return Err(ContractError::InvalidTarget);
        }
        if self.receipt != Some(identity.receipt) {
            return Err(ContractError::StaleReceipt);
        }
        if self.generation != u64::from(identity.cycle)
            || self.fence.is_some()
            || self.state != State::Ready
            || self.begun
            || self.phase != Phase::Delivery
            || self.suppression.is_some()
            || self.sealed.is_some()
        {
            return Err(ContractError::StaleEvaluation);
        }
        let readiness = ResponseReadiness::checked(
            identity.binding,
            identity.claim,
            identity.receipt,
            response.manifest(),
            target,
        )?;
        Ok(OwnerState {
            evaluation: self.binding,
            target,
            parent: ParentState::Open,
            readiness: Readiness::ResponseReceived(readiness),
            cohort: Cohort::Open,
            authority: Authority {
                evaluator: claim.issuer(),
                definition: self.declaration.binding().content,
                generation: self.generation,
                receipt: self.receipt,
                deadline: self.deadline(),
                policy_evidence: None,
                state: AuthorityState::Live,
            },
            logical_time,
        })
    }
}
