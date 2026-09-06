//! Current proposal policy, separate from historical domain execution.
use crate::*;

/// Additional admission policy for newly proposed work only. Historical intents
/// replay through the frozen reducer, preserving their original results.
pub(crate) fn validate_admission(
    state: &access::WriteState<'_>,
    limits: &Limits,
    principal: ParticipantId,
    authority: &AuthorityContext,
    command: &Command,
) -> Result<(), DomainOutcome> {
    fn validation(value: &ValidationContent) -> Result<(), DomainOutcome> {
        if value.kind == ValidationKind::Receipt
            && (value.phase != ValidationPhase::WholeWork
                || value.quality_bar.is_some()
                || !value.handlers.is_empty()
                || !value.evidence_schemas.is_empty())
        {
            return Err(refuse(
                ErrorCode::InvalidSchema,
                "receipt is whole-work delivery only; use a separate test, inspection or contract validation for evidence and quality",
            ));
        }
        Ok(())
    }
    fn claim(value: &NewClaim) -> Result<(), DomainOutcome> {
        value
            .validations
            .iter()
            .try_for_each(|v| validation(&v.content))
    }
    validate_report(state, principal, command)?;
    validate_diagnostic_headroom(state, limits, principal, authority, command)?;
    match command {
        Command::GenerateClaim { claim: value }
        | Command::SupersedeClaim {
            successor: value, ..
        } => claim(value),
        Command::GenerateClaimBatch { claims } => claims.iter().try_for_each(claim),
        Command::AcknowledgeTestament { claim, .. }
        | Command::BeginWholeWorkValidation { claim }
        | Command::CompleteWholeWork { claim } => {
            if let Some(value) = state.claims.get(claim) {
                for requirement in &value.content().requirements {
                    if let Some(value) = state.validations.get(&requirement.id) {
                        validation(value.content())?;
                    }
                }
            }
            Ok(())
        }
        _ => Ok(()),
    }
}

/// An unsuccessful account is still authored by the respondent. Its diagnostic
/// is evidence for the claimant's validators, not a requested claim verdict.
/// The overlay supplies both committed rows and earlier pending mutations.
fn validate_report(
    state: &access::WriteState<'_>,
    principal: ParticipantId,
    command: &Command,
) -> Result<(), DomainOutcome> {
    if matches!(command, Command::FailTestamentGeneration { .. }) {
        return Err(refuse(
            ErrorCode::InvalidSchema,
            "the receipt holder must submit the testament, including failures; runtime-generated failure testaments are historical only",
        ));
    }
    let Command::CloseTestament {
        claim,
        receipt,
        evidence_set,
        manifest,
        outcome,
        ..
    } = command
    else {
        return Ok(());
    };
    match outcome {
        OutcomeKind::Complete => return Ok(()),
        OutcomeKind::Partial
        | OutcomeKind::Refused
        | OutcomeKind::Impossible
        | OutcomeKind::Interrupted
        | OutcomeKind::Failed => {}
    }
    let parent = state
        .claims
        .get(claim)
        .ok_or_else(|| refuse(ErrorCode::UnknownObject, "unknown claim"))?;
    let entitlement = parent
        .lifecycle()
        .receipt
        .as_ref()
        .ok_or_else(|| refuse(ErrorCode::StaleReceipt, "no active receipt"))?;
    if entitlement.fence != *receipt {
        return Err(refuse(ErrorCode::StaleReceipt, "testament receipt changed"));
    }
    if entitlement.holder != principal {
        return Err(refuse(ErrorCode::WrongActor, "receipt holder must testify"));
    }
    let set = state
        .evidence_sets
        .get(evidence_set)
        .ok_or_else(|| refuse(ErrorCode::UnknownObject, "unknown evidence set"))?;
    if set.claim != *claim || set.receipt != *receipt || set.closed {
        return Err(refuse(
            ErrorCode::StaleReceipt,
            "evidence set closed or fenced",
        ));
    }
    if set.artifacts != *manifest {
        return Err(refuse(
            ErrorCode::InvalidManifest,
            "close must contain the exact staged ordered manifest",
        ));
    }
    for reference in manifest {
        let artifact = state
            .artifacts
            .get(&reference.id)
            .ok_or_else(|| refuse(ErrorCode::InvalidManifest, "unknown diagnostic artifact"))?;
        if artifact.content_hash() != reference.hash {
            return Err(refuse(ErrorCode::InvalidManifest, "artifact hash changed"));
        }
        if eligible_diagnostic(artifact, parent.content().ledger, principal, *receipt) {
            // Artifact insertion already required trusted durable, schema-valid
            // custody. Reading this exact row also records the planner's fence.
            return Ok(());
        }
    }
    Err(refuse(
        ErrorCode::EvidenceNotDurable,
        "non-complete testament requires the holder's durable typed error artifact in its exact manifest",
    ))
}

fn diagnostic_content(
    content: &ArtifactContent,
    ledger: LedgerId,
    principal: ParticipantId,
    receipt: ReceiptFence,
) -> bool {
    content.kind == "error"
        && content.ledger == ledger
        && content.schema == semantics_v1::SCHEMA
        && content.schema_hash != ContentHash::default()
        && content.receipt == Some(receipt)
        && content.producer == principal
}

fn eligible_diagnostic(
    artifact: &Artifact,
    ledger: LedgerId,
    principal: ParticipantId,
    receipt: ReceiptFence,
) -> bool {
    diagnostic_content(artifact.content(), ledger, principal, receipt)
        && artifact.lifecycle().custody_revision != 0
}

/// Preserve room to report a later work failure before newly admitted artifacts
/// fill the set. Existing full historical sets retain their exact bound; this
/// policy neither rewrites them nor invents an error to make a close pass.
fn validate_diagnostic_headroom(
    state: &access::WriteState<'_>,
    limits: &Limits,
    principal: ParticipantId,
    authority: &AuthorityContext,
    command: &Command,
) -> Result<(), DomainOutcome> {
    let Command::AttachArtifact {
        claim,
        receipt,
        evidence_set,
        artifact: candidate,
    } = command
    else {
        return Ok(());
    };
    let Some(set) = state.evidence_sets.get(evidence_set) else {
        // The frozen reducer supplies the ordinary missing-set refusal.
        return Ok(());
    };
    if set.artifacts.len() >= limits.max_artifacts_per_set
        || set.artifacts.len().checked_add(1) != Some(limits.max_artifacts_per_set)
    {
        return Ok(());
    }
    let parent = state
        .claims
        .get(claim)
        .ok_or_else(|| refuse(ErrorCode::UnknownObject, "unknown claim"))?;
    let entitlement = parent
        .lifecycle()
        .receipt
        .as_ref()
        .ok_or_else(|| refuse(ErrorCode::StaleReceipt, "no active receipt"))?;
    if entitlement.fence != *receipt || set.receipt != *receipt || set.claim != *claim || set.closed
    {
        return Err(refuse(
            ErrorCode::StaleReceipt,
            "evidence set closed or fenced",
        ));
    }
    if entitlement.holder != principal {
        return Err(refuse(
            ErrorCode::WrongActor,
            "receipt holder must supply evidence",
        ));
    }
    for reference in &set.artifacts {
        if let Some(artifact) = state.artifacts.get(&reference.id)
            && artifact.content_hash() == reference.hash
            && eligible_diagnostic(artifact, parent.content().ledger, principal, *receipt)
        {
            return Ok(());
        }
    }

    // Content identity can deduplicate an attachment to an already staged row,
    // including one presented under another candidate ID. Such a no-op consumes
    // no reserved slot. Hashing is needed only at this one-slot boundary.
    let hash = candidate.content.content_hash()?;
    if let Some(id) = state.identities.get(&(ObjectKind::Artifact, hash)) {
        let reference = ArtifactRef {
            id: ArtifactId(id.0),
            hash,
        };
        if set.artifacts.contains(&reference) {
            return Ok(());
        }
        // create_artifact keeps the original immutable custody row on a content
        // hit; a new positive attestation cannot repair a zero-custody old row.
        return match state.artifacts.get(&reference.id) {
            Some(artifact)
                if artifact.content_hash() == hash
                    && eligible_diagnostic(
                        artifact,
                        parent.content().ledger,
                        principal,
                        *receipt,
                    ) =>
            {
                Ok(())
            }
            _ => Err(refuse(
                ErrorCode::Capacity,
                "the final artifact slot is reserved for a durable current-holder error report",
            )),
        };
    }
    if !diagnostic_content(
        &candidate.content,
        parent.content().ledger,
        principal,
        *receipt,
    ) {
        return Err(refuse(
            ErrorCode::Capacity,
            "the final artifact slot is reserved for a durable current-holder error report",
        ));
    }
    // Match the frozen writer's first accepted attestation, rather than finding
    // a later positive revision after the writer would already choose zero.
    if authority
        .evidence
        .iter()
        .find(|evidence| {
            evidence.descriptor_hash == hash && evidence.durable && evidence.schema_valid
        })
        .is_none_or(|evidence| evidence.custody_revision == 0)
    {
        return Err(refuse(
            ErrorCode::EvidenceNotDurable,
            "reserved diagnostic slot requires verified durable custody",
        ));
    }
    Ok(())
}
