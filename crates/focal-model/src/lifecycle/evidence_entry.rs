//! One checked response entry authorizes its complete deterministic consequences.
//! This ephemeral capability is neither a participant identity nor a durable row.

use super::*;
use crate::lifecycle::{aggregation::ClaimDecision, claim::ClaimState};

/// Authority derived from an actual checked Received → Validating response plan.
///
/// The owner first advances the claim through its checked claimant or evaluator
/// path, refreshes the acceptance decision, and obtains this capability before
/// applying the response plan. All derived work and missing-target changes must
/// publish atomically with that exact claim and response entry. A capability
/// cannot authorize another response, receipt, cycle, or later claim revision.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ResponseEntry {
    claim: Binding,
    response: Binding,
    receipt: ReceiptFence,
    cycle: u32,
    respondent: ParticipantId,
    stamp: ReportStamp,
}

impl Response {
    pub fn entry_capability(
        &self,
        transition: &ResponseTransition,
        claim: &ClaimState,
        acceptance: &ClaimDecision<'_>,
    ) -> Result<ResponseEntry, ContractError> {
        let parent = self.check_acceptance(claim, acceptance)?;
        self.check(&transition.expected, &parent)?;
        parent.require_open_response()?;
        if claim.status() != ClaimStatus::Validating
            || self.state != ResponseState::Received
            || transition.before != ResponseState::Received
            || transition.next != ResponseState::Validating
            || transition.terminal.is_some()
        {
            return Err(ContractError::InvalidTransition);
        }
        if transition.stamp != self.stamp {
            return Err(ContractError::ContentConflict);
        }
        self.identity.binding.next()?.check(&transition.binding)?;
        if self.identity.cycle == 0
            || self.identity.cycle > claim.max_responses()
            || usize::try_from(self.identity.cycle).map_err(|_| ContractError::Capacity)?
                > claim.response_count()
            || self.respondent != parent.holder
        {
            return Err(ContractError::InvalidTarget);
        }
        Ok(ResponseEntry {
            claim: claim.binding(),
            response: transition.binding,
            receipt: self.identity.receipt,
            cycle: self.identity.cycle,
            respondent: self.respondent,
            stamp: self.stamp,
        })
    }
}

impl ResponseEntry {
    /// Validate the exact staged entry, including an entry with no work or
    /// missing checks. This exposes no constructor or mutable authority fields.
    pub fn check(&self, claim: &ClaimState, response: &Response) -> Result<(), ContractError> {
        self.claim.check(&claim.binding())?;
        self.response.check(&response.identity.binding)?;
        if response.stamp != self.stamp {
            return Err(ContractError::ContentConflict);
        }
        if claim.status() != ClaimStatus::Validating || response.state != ResponseState::Validating
        {
            return Err(ContractError::InvalidTransition);
        }
        let parent = Parent::from_claim(claim)?;
        parent.require_open_response()?;
        parent.check_identity(response.identity.binding.ledger, response.identity.claim)?;
        parent.check_receipt(self.receipt)?;
        if response.identity.receipt != self.receipt
            || response.identity.cycle != self.cycle
            || response.respondent != self.respondent
            || parent.holder != self.respondent
        {
            return Err(ContractError::InvalidTarget);
        }
        claim.received_report(response.identity.binding, self.receipt, self.stamp)?;
        Ok(())
    }
}

impl WorkArtifact {
    /// Apply response-entry consequences to any exact attached manifest member.
    /// The initiating evaluator need not be the evaluator of this particular
    /// artifact; this grants no authority to begin or report another check.
    pub fn begin_entered(
        &self,
        expected: &Binding,
        claim: &ClaimState,
        response: &Response,
        entry: &ResponseEntry,
    ) -> Result<Self, ContractError> {
        entry.check(claim, response)?;
        self.binding.check(expected)?;
        let parent = Parent::from_claim(claim)?;
        parent.check_identity(self.binding.ledger, self.claim)?;
        parent.check_receipt(self.receipt)?;
        if self.cycle != entry.cycle || self.producer != entry.respondent {
            return Err(ContractError::InvalidTarget);
        }
        self.enter_attached(response)
    }
}
