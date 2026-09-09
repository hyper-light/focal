//! Narrow participant ingress over the existing committed command encoding.
//! Protocol 3 changes admission, not reducer/replay semantics. The server records
//! the same trusted authority facts as its existing runtime bridge, after proving
//! this particular participant's standing. It never grants a Runtime credential.
use crate::*;
use focal_model::*;

pub const PEER_PROTOCOL_VERSION: u16 = 3;

pub const fn peer_command(command: &Command) -> bool {
    matches!(
        command,
        Command::AcknowledgeTestament { .. }
            | Command::BeginWholeWorkValidation { .. }
            | Command::BeginIncrementValidation { .. }
            | Command::CompleteWholeWork { .. }
            | Command::RegisterArtifact { .. }
            | Command::RecordFencedValidationVerdict { .. }
    )
}

pub const fn participant_protocol(operation: &Operation) -> u16 {
    match operation {
        Operation::Native { .. } | Operation::NativeRead(_) | Operation::NativeList(_) => {
            crate::NATIVE_PROTOCOL_VERSION
        }
        Operation::Submit { command, .. }
        | Operation::Managed {
            operation: ManagedOperation::Submit { command, .. },
            ..
        } if peer_command(command) => PEER_PROTOCOL_VERSION,
        Operation::Managed { .. }
        | Operation::RequestStreamControl { .. }
        | Operation::RequestStreamRead { .. }
        | Operation::ManagedSupport { .. } => MANAGED_PROTOCOL_VERSION,
        _ => PROTOCOL_VERSION,
    }
}

pub fn is_peer_request(request: &RequestEnvelope) -> bool {
    request.protocol == PEER_PROTOCOL_VERSION
        && matches!(&request.operation,
        Operation::Submit { command, .. }
        | Operation::Managed { operation: ManagedOperation::Submit { command, .. }, .. }
        if peer_command(command))
}

/// Trusted owner admission. `claim` must come from this owner's committed Core,
/// never from a client document or a stale projection. Revision is checked again
/// by Core against effective pending state, before any mutation is accepted.
/// Returning true authorizes ONLY this command's legacy runtime gate.
pub fn participant_authority(
    ledger: LedgerId,
    principal: ParticipantId,
    command: &Command,
    expected_revision: Option<ObjectRevision>,
    claim: Option<&Claim>,
) -> Result<bool, AccessError> {
    match command {
        Command::AcknowledgeTestament { .. }
        | Command::BeginWholeWorkValidation { .. }
        | Command::BeginIncrementValidation { .. }
        | Command::CompleteWholeWork { .. } => {
            let claim = claim.ok_or(AccessError::Unauthorized)?;
            if claim.content().ledger != ledger
                || claim.content().issuer() != Some(principal)
                || expected_revision.is_none()
            {
                return Err(AccessError::Unauthorized);
            }
            Ok(true)
        }
        Command::RegisterArtifact { artifact } => {
            if artifact.content.ledger != ledger
                || artifact.content.producer != principal
                || artifact.content.receipt.is_some()
                || expected_revision.is_some()
            {
                return Err(AccessError::Unauthorized);
            }
            Ok(true)
        }
        Command::RecordFencedValidationVerdict { verdict, .. } => {
            if verdict.evaluator != principal || expected_revision.is_some() {
                return Err(AccessError::Unauthorized);
            }
            // Core rechecks the actual requirement evaluator, run, handler,
            // attempt, target, manifest and receipt, including pending changes.
            Ok(false)
        }
        _ => Err(AccessError::Unauthorized),
    }
}
