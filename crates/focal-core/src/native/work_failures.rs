//! Failed work remains evidence, independently of claim and response status.
//! Production failure reuses an actual respondent diagnostic; receipt rejection
//! stores a distinct claimant diagnostic tied to the exact rejected work product.

use super::prepare::{Extra, Extras, Scratch};
use super::work_artifacts::{cycle, empty_plan, observation, parent, store_artifact};
use super::work_owned::{NativeWork, OwnedWork};
use super::*;
use focal_model::{
    ArtifactRef, EvidenceAttestation,
    lifecycle::{
        artifact_descriptor::{ArtifactDescriptor, WorkProvenance, WorkRole},
        claim::ClaimCut,
        evidence::{Diagnostic, EvidenceFailure, WorkArtifact, WorkArtifactState},
    },
};

fn attestation(artifact: &NativeArtifact) -> EvidenceAttestation {
    EvidenceAttestation {
        descriptor_hash: artifact.descriptor().content_hash(),
        custody_revision: artifact.custody().local_revision(),
        durable: true,
        schema_valid: true,
    }
}

fn production(
    view: &View<'_>,
    context: NativeContext,
    claim: Binding,
    slot: u32,
    diagnostic: ArtifactRef,
    limits: NativeLimits,
) -> Result<(NativeCycleKey, NativeCycle, NativeWork), NativeError> {
    let parent = parent(view, claim)?;
    context.principal.require_actor(parent.holder)?;
    let mut cycle = cycle(view, &parent, limits)?;
    let key = NativeCycleKey::of(&parent);
    let claim = view
        .claim(parent.claim)
        .ok_or(ContractError::InvalidTarget)?;
    if !claim.acceptance().has_slot(slot) {
        return Err(ContractError::InvalidTarget.into());
    }
    let maximum = super::response_budget::work_limit(limits)?;
    if cycle.work_count >= maximum {
        return Err(NativeError::Capacity("work artifacts per response cycle"));
    }
    if view.get(Key::WorkSlot(key, slot)).is_some() || view.get(Key::Work(diagnostic.id)).is_some()
    {
        return Err(ContractError::InvalidManifest.into());
    }
    let Some(Row::Diagnostic(row)) = view.get(Key::Diagnostic(diagnostic.id)) else {
        return Err(ContractError::MissingEvidence.into());
    };
    let row = row.get().ok_or(ContractError::MissingEvidence)?;
    row.diagnostic.check_parent(&parent)?;
    if cycle.diagnostic_count == 0
        || row.diagnostic.artifact() != diagnostic
        || row.diagnostic.diagnostic().reason != EvidenceFailure::Production
    {
        return Err(ContractError::MissingEvidence.into());
    }
    let source = as_artifact(view.get(Key::Artifact(diagnostic.id)))
        .ok_or(ContractError::MissingEvidence)?;
    let descriptor = source.descriptor();
    if descriptor.id() != diagnostic.id
        || descriptor.content_hash() != diagnostic.hash
        || descriptor.ledger() != parent.ledger
        || descriptor.producer() != parent.holder
        || descriptor.receipt() != Some(parent.receipt)
        || descriptor.kind() != "error"
        || descriptor.work_provenance()
            != Some(WorkProvenance {
                claim: parent.claim,
                cycle: parent.next_cycle,
                role: WorkRole::Diagnostic {
                    reason: EvidenceFailure::Production,
                },
            })
    {
        return Err(ContractError::MissingEvidence.into());
    }
    // The failed object's immutable binding is the real diagnostic artifact.
    // No nonexistent product ID, placeholder payload, or attestation is invented.
    let state = WorkArtifact::generation_failed(
        descriptor.binding(),
        &parent,
        context.principal,
        slot,
        parent.receipt,
        Diagnostic {
            reason: EvidenceFailure::Production,
            artifact: diagnostic,
        },
        &attestation(source),
    )?;
    let work = NativeWork {
        state,
        next: cycle.work_head,
    };
    transactions::increment(
        &mut cycle.work_count,
        1,
        maximum,
        "work artifacts per response cycle",
    )?;
    cycle.work_head = Some(diagnostic.id);
    Ok((key, cycle, work))
}

fn inherits_visibility(
    source: &ArtifactDescriptor,
    descriptor: &ArtifactDescriptor,
    limits: NativeLimits,
) -> Result<(), NativeError> {
    let mut remaining = limits.plan_edges;
    let mut labels = descriptor.visibility();
    for required in source.visibility() {
        loop {
            remaining = remaining
                .checked_sub(1)
                .ok_or(NativeError::Capacity("rejected work visibility"))?;
            match labels.next() {
                Some(label) if label < required => continue,
                Some(label) if label == required => break,
                _ => return Err(ContractError::InvalidPolicy.into()),
            }
        }
    }
    Ok(())
}

fn rejection<'a>(
    view: &View<'_>,
    context: NativeContext,
    claim: Binding,
    expected: Binding,
    reason: EvidenceFailure,
    input: &'a NativeArtifactInput,
    limits: NativeLimits,
) -> Result<&'a ArtifactDescriptor, NativeError> {
    let (parent, old) = observation(view, context, claim, expected)?;
    if !matches!(
        reason,
        EvidenceFailure::Structure | EvidenceFailure::Metadata
    ) {
        return Err(ContractError::InvalidPolicy.into());
    }
    if !matches!(
        old.state.state(),
        WorkArtifactState::Generated | WorkArtifactState::Received
    ) || old.state.attachment().is_some()
    {
        return Err(ContractError::InvalidTransition.into());
    }
    old.state.binding().next()?;
    let descriptor = input.get().ok_or(ContractError::MissingEvidence)?;
    if descriptor.ledger() != parent.ledger {
        return Err(ContractError::WrongLedger.into());
    }
    if descriptor.producer() != parent.issuer {
        return Err(ContractError::WrongActor.into());
    }
    if descriptor.receipt() != Some(old.state.receipt()) {
        return Err(ContractError::StaleReceipt.into());
    }
    if descriptor.kind() != "error"
        || descriptor.result_provenance().is_some()
        || descriptor.work_provenance()
            != Some(WorkProvenance {
                claim: old.state.claim(),
                cycle: old.state.cycle(),
                role: WorkRole::ReceiptRejection {
                    artifact: old.state.reference(),
                    reason,
                },
            })
    {
        return Err(ContractError::MissingEvidence.into());
    }
    if view.get(Key::Artifact(descriptor.id())).is_some()
        || view
            .get(Key::ArtifactIdentity(descriptor.content_hash()))
            .is_some()
        || view.get(Key::Work(descriptor.id())).is_some()
        || view.get(Key::Diagnostic(descriptor.id())).is_some()
    {
        return Err(ContractError::ContentConflict.into());
    }
    let source = as_artifact(view.get(Key::Artifact(old.state.reference().id)))
        .ok_or(ContractError::MissingEvidence)?;
    inherits_visibility(source.descriptor(), descriptor, limits)?;
    super::reporting::check_inputs(descriptor, view, limits)?;
    let mut artifacts = view.meta().artifacts;
    transactions::increment(&mut artifacts, 1, limits.artifacts, "artifacts")?;
    Ok(descriptor)
}

pub(super) fn authorize<'a>(
    view: &View<'_>,
    context: NativeContext,
    command: &'a NativeCommand,
    limits: NativeLimits,
) -> Result<Option<&'a ArtifactDescriptor>, NativeError> {
    match command {
        NativeCommand::FailWorkProduction {
            claim,
            slot,
            diagnostic,
        } => {
            production(view, context, *claim, *slot, *diagnostic, limits)?;
            Ok(None)
        }
        NativeCommand::RejectWork {
            claim,
            expected,
            reason,
            artifact,
        } => rejection(view, context, *claim, *expected, *reason, artifact, limits).map(Some),
        _ => Ok(None),
    }
}

fn stage_work(
    work: NativeWork,
    before: Option<Binding>,
    extras: &mut Extras,
    scratch: &mut Scratch,
) -> Result<(), NativeError> {
    let binding = work.state.binding();
    let fact = NativeFact::Work {
        claim: work.state.claim(),
        before,
        after: binding,
        state: work.state.state(),
    };
    scratch.charge(OwnedWork::container_charge())?;
    let row = OwnedWork::new(work)?;
    let heap = row.heap_charge()?;
    extras.push(Extra {
        key: Key::Work(ArtifactId(binding.object.0)),
        row: Row::Work(row),
        heap,
        fact: Some(fact),
    })
}

#[allow(clippy::too_many_arguments)] // Exact internal owner transaction frame.
pub(super) fn prepare(
    command: NativeCommand,
    request: RequestKey,
    evidence: Option<&focal_evidence::VerifiedNativeArtifact>,
    context: NativeContext,
    _cut: ClaimCut,
    view: &View<'_>,
    limits: NativeLimits,
    meta: &mut Meta,
    extras: &mut Extras,
    scratch: &mut Scratch,
) -> Result<transactions::Plan, NativeError> {
    authorize(view, context, &command, limits)?;
    if !extras.rows.is_empty() {
        return Err(ContractError::InvalidTransition.into());
    }
    match command {
        NativeCommand::FailWorkProduction {
            claim,
            slot,
            diagnostic,
        } => {
            let (key, cycle, work) = production(view, context, claim, slot, diagnostic, limits)?;
            stage_work(work, None, extras, scratch)?;
            extras.push(Extra {
                key: Key::WorkSlot(key, slot),
                row: Row::WorkSlot(diagnostic.id),
                heap: 0,
                fact: None,
            })?;
            extras.push(Extra {
                key: Key::Cycle(key),
                row: Row::Cycle(cycle),
                heap: 0,
                fact: None,
            })?;
        }
        NativeCommand::RejectWork {
            claim,
            expected,
            reason,
            artifact,
        } => {
            let (parent, old) = observation(view, context, claim, expected)?;
            scratch.charge(artifact.heap_charge()?)?;
            let descriptor = artifact.get().ok_or(ContractError::MissingEvidence)?;
            let verified = evidence.ok_or(ContractError::MissingEvidence)?;
            verified.check(request, descriptor)?;
            let custody = verified.custody();
            let diagnostic = Diagnostic {
                reason,
                artifact: ArtifactRef {
                    id: descriptor.id(),
                    hash: descriptor.content_hash(),
                },
            };
            let attestation = EvidenceAttestation {
                descriptor_hash: descriptor.content_hash(),
                custody_revision: custody.local_revision(),
                durable: true,
                schema_valid: true,
            };
            let next = NativeWork {
                state: old.state.reject_receipt(
                    &expected,
                    &parent,
                    context.principal,
                    diagnostic,
                    &attestation,
                )?,
                next: old.next,
            };
            transactions::increment(&mut meta.artifacts, 1, limits.artifacts, "artifacts")?;
            store_artifact(
                artifact.into_descriptor()?,
                custody,
                request,
                extras,
                scratch,
            )?;
            stage_work(next, Some(expected), extras, scratch)?;
        }
        _ => return Err(ContractError::InvalidTransition.into()),
    }
    Ok(empty_plan())
}
