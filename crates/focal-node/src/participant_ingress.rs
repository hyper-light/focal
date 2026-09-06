//! Command-specific participant standing, checked by the current ledger owner.
use crate::host::access;
use focal_ledger::Session;
use focal_model::*;
use focal_wire::*;

pub(crate) fn authority(
    session: &Session,
    protocol: u16,
    principal: ParticipantId,
    command: &Command,
    revision: Option<ObjectRevision>,
) -> Result<Option<bool>, AccessError> {
    if protocol != PEER_PROTOCOL_VERSION {
        return Ok(None);
    }
    let state = session.read_at_least(session.sequence()).map_err(access)?;
    let claim = command.claim_id().and_then(|id| state.claims.get(&id));
    let granted = participant_authority(session.ledger(), principal, command, revision, claim)?;
    if let Command::BeginIncrementValidation {
        claim: id,
        validation,
        target,
        manifest,
    } = command
    {
        let claim = claim.ok_or(AccessError::Unauthorized)?;
        let requirement = state
            .validations
            .get(validation)
            .ok_or(AccessError::InvalidRequest)?;
        let evidence = claim
            .lifecycle()
            .evidence_set
            .and_then(|id| state.evidence_sets.get(&id))
            .ok_or(AccessError::InvalidRequest)?;
        if requirement.content().claim != *id
            || requirement.content().phase != ValidationPhase::Increment
            || evidence.claim != *id
            || !evidence
                .artifacts
                .iter()
                .any(|artifact| artifact.hash == *target)
            || manifest_hash(&evidence.artifacts).map_err(|_| AccessError::Capacity)? != *manifest
        {
            return Err(AccessError::InvalidRequest);
        }
    }
    Ok(Some(granted))
}
