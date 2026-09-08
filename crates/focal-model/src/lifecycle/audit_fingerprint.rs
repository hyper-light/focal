//! Explicit native audit content identity. Tags and big-endian scalar encodings
//! below are independent of Rust enum layout and all frozen V1 formats. This
//! domain is not a durable native codec: the owner additionally binds original
//! publication witnesses when constructing its claimant bundle identity.
use super::*;
use crate::lifecycle::aggregation::CauseTarget;
use crate::lifecycle::validation::FenceReason;
use crate::{ContentHash, ReceiptFence, ValidationMode, VerdictValue};

struct Hash(blake3::Hasher);
impl Hash {
    fn bytes(&mut self, bytes: &[u8]) {
        self.0.update(bytes);
    }
    fn tag(&mut self, tag: u8) {
        self.bytes(&[tag]);
    }
    fn count(&mut self, count: usize) -> Result<(), ContractError> {
        self.bytes(
            &u64::try_from(count)
                .map_err(|_| ContractError::Capacity)?
                .to_be_bytes(),
        );
        Ok(())
    }
    fn binding(&mut self, binding: Binding) {
        self.bytes(&binding.ledger.tenant.0);
        self.bytes(&binding.ledger.session.0);
        self.bytes(&binding.object.0);
        self.bytes(&binding.content.0);
        self.bytes(&binding.revision.0.to_be_bytes());
    }
    fn target(&mut self, target: Target) {
        match target {
            Target::Artifact {
                response,
                slot,
                artifact,
            } => {
                self.tag(0);
                self.binding(response);
                self.bytes(&slot.to_be_bytes());
                self.binding(artifact);
            }
            Target::MissingSlot { response, slot } => {
                self.tag(1);
                self.binding(response);
                self.bytes(&slot.to_be_bytes());
            }
            Target::Delivery { response } => {
                self.tag(2);
                self.binding(response);
            }
            Target::Admission { claim } => {
                self.tag(3);
                self.binding(claim);
            }
            Target::Increment { claim, artifact } => {
                self.tag(4);
                self.binding(claim);
                self.binding(artifact);
            }
        }
    }
    fn receipt(&mut self, receipt: Option<ReceiptFence>) {
        match receipt {
            None => self.tag(0),
            Some(receipt) => {
                self.tag(1);
                self.bytes(&receipt.receipt.0);
                self.bytes(&receipt.epoch.to_be_bytes());
            }
        }
    }
    fn artifact(&mut self, artifact: Option<ArtifactRef>) {
        match artifact {
            None => self.tag(0),
            Some(artifact) => {
                self.tag(1);
                self.bytes(&artifact.id.0);
                self.bytes(&artifact.hash.0);
            }
        }
    }
    fn phase(&mut self, phase: Phase) {
        self.tag(match phase {
            Phase::Programmatic => 0,
            Phase::Quality => 1,
            Phase::Delivery => 2,
            Phase::MissingTarget => 3,
        });
    }
    fn state(&mut self, state: State) {
        self.tag(match state {
            State::Ready => 0,
            State::Validating => 1,
            State::ValidatingQualityBar => 2,
            State::Validated => 3,
            State::ValidationIncomplete => 4,
            State::ValidationFailed => 5,
            State::ValidationFailedNotRequired => 6,
            State::Errored => 7,
            State::ErroredNotRequired => 8,
            State::QualityBarValidationFailed => 9,
            State::QualityBarValidationFailedNotRequired => 10,
        });
    }
    fn suppression(&mut self, suppression: Option<Suppression>) {
        match suppression {
            None => self.tag(0),
            Some(Suppression::MissingTarget) => self.tag(1),
            Some(Suppression::ParentFailure(cause)) => {
                self.tag(2);
                self.bytes(&cause.0);
            }
            Some(Suppression::ArtifactFailure(cause)) => {
                self.tag(3);
                self.bytes(&cause.0);
            }
            Some(Suppression::CohortSealed(cause)) => {
                self.tag(4);
                self.bytes(&cause.0);
            }
        }
    }
    fn fence(&mut self, fence: Option<AuthorityFence>) {
        let Some(fence) = fence else {
            self.tag(0);
            return;
        };
        self.tag(1);
        match fence.reason {
            FenceReason::Cancellation => self.tag(0),
            FenceReason::Revocation => self.tag(1),
            FenceReason::Supersession => self.tag(2),
            FenceReason::Expiry => self.tag(3),
            FenceReason::ReceiptAdoption => self.tag(4),
            FenceReason::Evaluation => self.tag(5),
            FenceReason::Deadline(deadline) => {
                self.tag(6);
                self.bytes(&deadline.timer.0);
                self.bytes(&deadline.generation.to_be_bytes());
                self.bytes(&deadline.at.to_be_bytes());
            }
        }
        self.bytes(&fence.cause.0);
    }
    fn result(&mut self, result: AcceptedResult) {
        self.bytes(result.definition_stamp().as_bytes());
        self.binding(result.binding());
        self.bytes(&result.ledger().tenant.0);
        self.bytes(&result.ledger().session.0);
        self.bytes(&result.claim().0);
        self.target(result.target());
        self.bytes(&result.validation().0);
        self.bytes(&result.declaration_index().to_be_bytes());
        self.tag(match result.mode() {
            ValidationMode::Required => 0,
            ValidationMode::Observe => 1,
        });
        self.tag(match result.verdict() {
            VerdictValue::Pass => 0,
            VerdictValue::Fail => 1,
            VerdictValue::Incomplete => 2,
            VerdictValue::Error => 3,
        });
        self.phase(result.phase());
        match result.attempt() {
            None => self.tag(0),
            Some(attempt) => {
                self.tag(1);
                self.bytes(&attempt.to_be_bytes());
            }
        }
        self.bytes(&result.generation().to_be_bytes());
        self.receipt(result.receipt());
        self.artifact(result.evidence());
        self.artifact(result.programmatic_evidence());
        match result.reporter() {
            None => self.tag(0),
            Some(reporter) => {
                self.tag(1);
                self.bytes(&reporter.0);
            }
        }
        self.state(result.resulting_state());
    }
    fn member(&mut self, member: &AuditMember) {
        self.bytes(member.definition.as_bytes());
        self.bytes(&member.key.validation.0);
        self.target(member.key.target);
        self.bytes(&member.key.generation.to_be_bytes());
        match member.order.target {
            CauseTarget::Admission => self.tag(0),
            CauseTarget::Increment { artifact, content } => {
                self.tag(1);
                self.bytes(&artifact.0);
                self.bytes(&content.0);
            }
            CauseTarget::Response(response) => {
                self.tag(2);
                self.bytes(&response.0);
            }
        }
        self.bytes(&member.order.declaration.to_be_bytes());
        self.bytes(&member.order.generation.to_be_bytes());
        self.bytes(&member.order.validation.0);
        self.binding(member.binding);
        self.receipt(member.receipt);
        self.tag(u8::from(member.begun));
        self.state(member.state);
        self.suppression(member.suppression);
        self.fence(member.fence);
        match member.last_result {
            None => self.tag(0),
            Some(result) => {
                self.tag(1);
                self.result(result);
            }
        }
        match member.sealed {
            None => self.tag(0),
            Some(cause) => {
                self.tag(1);
                self.bytes(&cause.0);
            }
        }
    }
}

impl AuditCohort {
    pub(super) fn check_complete_order(&self) -> Result<(), ContractError> {
        let mut previous = None;
        for member in &self.members {
            if !member.complete() {
                return Err(ContractError::InvalidTransition);
            }
            if previous.is_some_and(|order| order >= member.order) {
                return Err(ContractError::InvalidManifest);
            }
            previous = Some(member.order);
        }
        let mut previous = None;
        for result in &self.results {
            let order = result_order(*result);
            if previous.is_some_and(|prior| prior >= order) {
                return Err(ContractError::InvalidManifest);
            }
            previous = Some(order);
        }
        Ok(())
    }

    /// Complete canonical native audit identity. No allocation or sorting is
    /// performed. Reserved capacities and arrival order are not content; every
    /// retained member and accepted result is. The owner's publication witness
    /// vector and new bundle identity are deliberately separate inputs there.
    pub fn content_fingerprint(&self) -> Result<ContentHash, ContractError> {
        let mut hash = Hash(blake3::Hasher::new_derive_key(
            "focal/native/audit-content/1",
        ));
        hash.tag(1);
        hash.binding(self.claim);
        hash.bytes(&self.issuer.0);
        hash.bytes(&self.sequence.0.to_be_bytes());
        hash.count(self.members.len())?;
        let mut previous = None;
        for member in &self.members {
            if !member.complete() {
                return Err(ContractError::InvalidTransition);
            }
            if previous.is_some_and(|order| order >= member.order) {
                return Err(ContractError::InvalidManifest);
            }
            previous = Some(member.order);
            hash.member(member);
        }
        hash.count(self.results.len())?;
        let mut previous = None;
        for result in &self.results {
            let order = result_order(*result);
            if previous.is_some_and(|prior| prior >= order) {
                return Err(ContractError::InvalidManifest);
            }
            previous = Some(order);
            hash.result(*result);
        }
        Ok(ContentHash(*hash.0.finalize().as_bytes()))
    }
}
