//! Immutable behavioral helpers used when admitting and replaying V1 operations.
//!
//! These rules preserve the original interpretation of stored V1 values, including
//! ordered relation selection. Relation lookups do not filter ledger IDs; domain
//! admission separately enforces namespace constraints.
//! A new protocol must introduce a separate behavioral boundary rather than
//! changing these functions. Authored content and specification identity encoders
//! are likewise immutable V1 contracts.
use crate::*;

/// The original schema interpreted by this behavioral boundary.
pub const SCHEMA: u16 = 1;

pub use crate::canonical::manifest_hash;

pub const fn is_terminal(status: ClaimStatus) -> bool {
    match status {
        ClaimStatus::Generated
        | ClaimStatus::Posted
        | ClaimStatus::Received
        | ClaimStatus::Progressed
        | ClaimStatus::TestamentGenerated
        | ClaimStatus::TestamentAcknowledged
        | ClaimStatus::Validating => false,
        ClaimStatus::Satisfied
        | ClaimStatus::PostFailed
        | ClaimStatus::ReceiptFailed
        | ClaimStatus::TestamentGenerationFailed
        | ClaimStatus::ValidationIncomplete
        | ClaimStatus::ValidationFailed
        | ClaimStatus::ValidationErrored
        | ClaimStatus::Cancelled
        | ClaimStatus::Expired
        | ClaimStatus::Revoked
        | ClaimStatus::Superseded
        | ClaimStatus::DependencyFailed
        | ClaimStatus::Deadlocked => true,
    }
}

pub const fn is_active(status: ClaimStatus) -> bool {
    !is_terminal(status)
}

pub const fn severity(value: VerdictValue) -> u8 {
    match value {
        VerdictValue::Pass => 0,
        VerdictValue::Incomplete => 1,
        VerdictValue::Error => 2,
        VerdictValue::Fail => 3,
    }
}

pub const fn lifecycle_action(status: ClaimStatus) -> LifecycleAction {
    match status {
        ClaimStatus::Generated => LifecycleAction::Generated,
        ClaimStatus::Posted => LifecycleAction::Posted,
        ClaimStatus::Received => LifecycleAction::Received,
        ClaimStatus::Progressed => LifecycleAction::Progressed,
        ClaimStatus::TestamentGenerated => LifecycleAction::TestamentGenerated,
        ClaimStatus::TestamentAcknowledged => LifecycleAction::TestamentAcknowledged,
        ClaimStatus::Validating => LifecycleAction::Validating,
        ClaimStatus::Satisfied => LifecycleAction::Satisfied,
        ClaimStatus::PostFailed => LifecycleAction::PostFailed,
        ClaimStatus::ReceiptFailed => LifecycleAction::ReceiptFailed,
        ClaimStatus::TestamentGenerationFailed => LifecycleAction::TestamentGenerationFailed,
        ClaimStatus::ValidationIncomplete => LifecycleAction::ValidationIncomplete,
        ClaimStatus::ValidationFailed => LifecycleAction::ValidationFailed,
        ClaimStatus::ValidationErrored => LifecycleAction::ValidationErrored,
        ClaimStatus::Cancelled => LifecycleAction::Cancelled,
        ClaimStatus::Expired => LifecycleAction::Expired,
        ClaimStatus::Revoked => LifecycleAction::Revoked,
        ClaimStatus::Superseded => LifecycleAction::Superseded,
        ClaimStatus::DependencyFailed => LifecycleAction::DependencyFailed,
        ClaimStatus::Deadlocked => LifecycleAction::Deadlocked,
    }
}

pub fn issuer(content: &ClaimContent) -> Option<ParticipantId> {
    party(content, RelationKind::Issuer)
}

pub fn subject(content: &ClaimContent) -> Option<ParticipantId> {
    party(content, RelationKind::Subject)
}

fn party(content: &ClaimContent, kind: RelationKind) -> Option<ParticipantId> {
    content
        .relations
        .iter()
        .find_map(|relation| match (relation.kind == kind, &relation.target) {
            (true, RelationTarget::Participant(participant)) => Some(*participant),
            _ => None,
        })
}

pub fn action(content: &ClaimContent) -> Option<ActionType> {
    content
        .relations
        .iter()
        .find_map(|relation| match relation {
            Relation {
                kind: RelationKind::ClaimAction,
                target: RelationTarget::Action(action),
            } => Some(*action),
            _ => None,
        })
}

pub fn cause(content: &ClaimContent) -> Option<Cause> {
    content
        .relations
        .iter()
        .find_map(|relation| match relation {
            Relation {
                kind: RelationKind::CausedBy,
                target: RelationTarget::Root(root),
            } => Some(Cause::Root(*root)),
            Relation {
                kind: RelationKind::CausedBy,
                target: RelationTarget::Object(object),
            } if object.kind == ObjectKind::Claim => Some(Cause::Claim(ClaimId(object.id.0))),
            _ => None,
        })
}

pub fn dependencies(
    content: &ClaimContent,
    kind: RelationKind,
) -> impl Iterator<Item = ClaimId> + '_ {
    content
        .relations
        .iter()
        .filter(move |relation| relation.kind == kind)
        .filter_map(|relation| match &relation.target {
            RelationTarget::Object(object) if object.kind == ObjectKind::Claim => {
                Some(ClaimId(object.id.0))
            }
            _ => None,
        })
}

/// Original revision target. Batch, verdict and monitor-rebinding operations do
/// not gain an implicit target even when their nested values contain claim IDs.
pub const fn claim_id(command: &Command) -> Option<ClaimId> {
    match command {
        Command::GenerateClaim { claim } => Some(claim.id),
        Command::SupersedeClaim { predecessor, .. } => Some(*predecessor),
        Command::PostClaim { claim }
        | Command::AcquireReceipt { claim, .. }
        | Command::AdoptReceipt { claim, .. }
        | Command::RecordProgress { claim, .. }
        | Command::BeginEvidenceSet { claim, .. }
        | Command::AttachArtifact { claim, .. }
        | Command::CloseTestament { claim, .. }
        | Command::AcknowledgeTestament { claim, .. }
        | Command::BeginWholeWorkValidation { claim }
        | Command::BeginIncrementValidation { claim, .. }
        | Command::CompleteWholeWork { claim }
        | Command::FailPost { claim, .. }
        | Command::FailReceipt { claim, .. }
        | Command::FailTestamentGeneration { claim, .. }
        | Command::CancelClaim { claim, .. }
        | Command::RevokeClaim { claim, .. }
        | Command::ExpireClaim { claim, .. }
        | Command::ReleaseScope { claim } => Some(*claim),
        Command::RegisterMonitor { owner, .. } => Some(*owner),
        Command::NegotiateEpoch { .. }
        | Command::AdvanceEpochFloor { .. }
        | Command::GenerateClaimBatch { .. }
        | Command::RecordValidationVerdict { .. }
        | Command::RebindMonitor { .. }
        | Command::RegisterArtifact { .. }
        | Command::ExpireMonitor { .. }
        | Command::RecordFencedValidationVerdict { .. } => None,
    }
}

pub fn request_stream_valid(stream: &RequestStreamIdentity) -> bool {
    stream.cluster != [0; 16]
        && !stream.ledger.tenant.is_zero()
        && !stream.ledger.session.is_zero()
        && !stream.principal.is_zero()
        && stream.generation > 0
}

pub fn request_key_valid(key: &ManagedRequestKey) -> bool {
    request_stream_valid(&key.stream) && key.ordinal > 0 && !key.id.is_zero()
}

#[cfg(test)]
#[path = "semantics_v1_tests.rs"]
mod tests;
