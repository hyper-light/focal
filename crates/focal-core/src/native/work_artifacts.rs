//! Receipt-bound peer evidence transactions. Submitting a work product or a
//! diagnostic neither creates testimony nor acknowledges claimant observation.
//! Every insertion edits one bounded cycle head and its exact slot membership.

use super::prepare::{Extra, Extras, Scratch};
use super::work_owned::{NativeDiagnostic, NativeWork, OwnedDiagnostic, OwnedWork};
use super::*;
use focal_model::{
    ArtifactRef, EvidenceAttestation,
    lifecycle::{
        artifact_descriptor::{ArtifactDescriptor, WorkProvenance, WorkRole},
        claim::ClaimCut,
        evidence::{Diagnostic, Parent, ResponseDiagnostic, WorkArtifact},
    },
};

pub(super) fn parent(view: &View<'_>, expected: Binding) -> Result<Parent, NativeError> {
    let claim = view
        .claim(ClaimId(expected.object.0))
        .ok_or(ContractError::InvalidTarget)?;
    claim.binding().check(&expected)?;
    let parent = Parent::from_claim(claim)?;
    parent.require_open_response()?;
    if parent.next_cycle > claim.max_responses() || parent.next_cycle.checked_add(1).is_none() {
        return Err(NativeError::Capacity("claim response cycles"));
    }
    Ok(parent)
}

pub(super) fn cycle(
    view: &View<'_>,
    parent: &Parent,
    limits: NativeLimits,
) -> Result<NativeCycle, NativeError> {
    let cycle = match view.get(Key::Cycle(NativeCycleKey::of(parent))) {
        None => NativeCycle::default(),
        Some(Row::Cycle(cycle)) => *cycle,
        Some(_) => return Err(ContractError::InvalidTarget.into()),
    };
    if cycle.response.is_some() {
        return Err(ContractError::InvalidTransition.into());
    }
    if cycle.work_head.is_some() != (cycle.work_count != 0)
        || cycle.diagnostic_head.is_some() != (cycle.diagnostic_count != 0)
        || cycle.work_count > limits.work_artifacts_per_cycle
        || cycle.diagnostic_count > limits.diagnostics_per_cycle
    {
        return Err(ContractError::InvalidManifest.into());
    }
    if let Some(head) = cycle.work_head {
        let work = work(view, head)?;
        if work.state.reference().id != head {
            return Err(ContractError::InvalidManifest.into());
        }
        check_work_parent(&work.state, parent)?;
    }
    if let Some(head) = cycle.diagnostic_head {
        let Some(Row::Diagnostic(row)) = view.get(Key::Diagnostic(head)) else {
            return Err(ContractError::InvalidManifest.into());
        };
        let diagnostic = row.get().ok_or(ContractError::InvalidManifest)?;
        if diagnostic.diagnostic.artifact().id != head {
            return Err(ContractError::InvalidManifest.into());
        }
        diagnostic.diagnostic.check_parent(parent)?;
    }
    Ok(cycle)
}

fn work<'a>(view: &View<'a>, id: ArtifactId) -> Result<&'a NativeWork, NativeError> {
    match view.get(Key::Work(id)) {
        Some(Row::Work(row)) => row.get().ok_or(ContractError::InvalidTarget.into()),
        _ => Err(ContractError::InvalidTarget.into()),
    }
}

fn check_work_parent(work: &WorkArtifact, parent: &Parent) -> Result<(), NativeError> {
    if work.binding().ledger != parent.ledger {
        return Err(ContractError::WrongLedger.into());
    }
    if work.claim() != parent.claim {
        return Err(ContractError::WrongObject.into());
    }
    if work.receipt() != parent.receipt {
        return Err(ContractError::StaleReceipt.into());
    }
    if work.cycle() != parent.next_cycle {
        return Err(ContractError::InvalidManifest.into());
    }
    if work.producer() != parent.holder {
        return Err(ContractError::WrongActor.into());
    }
    Ok(())
}

fn submission<'a>(
    view: &View<'_>,
    context: NativeContext,
    expected: Binding,
    role: WorkRole,
    artifact: &'a NativeArtifactInput,
    limits: NativeLimits,
) -> Result<&'a ArtifactDescriptor, NativeError> {
    let descriptor = artifact.get().ok_or(ContractError::MissingEvidence)?;
    submission_view(view, context, expected, role, descriptor, limits)?;
    Ok(descriptor)
}

/// Resolve the exact receipt, cycle, artifact identity and referenced sources
/// before the owner constructs a typed input or performs custody IO. A prepared
/// body is checked again by the owned writer after fallible construction.
pub(super) fn submission_view(
    view: &View<'_>,
    context: NativeContext,
    expected: Binding,
    role: WorkRole,
    descriptor: &impl super::report_artifact::ArtifactView,
    limits: NativeLimits,
) -> Result<(), NativeError> {
    let parent = parent(view, expected)?;
    context.principal.require_actor(parent.holder)?;
    let cycle = cycle(view, &parent, limits)?;
    // Evidence admission must leave a representable atomic response close, even
    // for a diagnostic-only cycle. This is derived from owner transaction bounds.
    let work_limit = super::response_budget::work_limit(limits)?;
    if cycle.work_count > work_limit {
        return Err(NativeError::Capacity("response close work bound"));
    }
    let fields = descriptor.fields();
    if fields.ledger != parent.ledger {
        return Err(ContractError::WrongLedger.into());
    }
    if fields.producer != parent.holder {
        return Err(ContractError::WrongActor.into());
    }
    if fields.receipt != Some(parent.receipt) {
        return Err(ContractError::StaleReceipt.into());
    }
    if fields.result.is_some()
        || fields.work
            != Some(WorkProvenance {
                claim: parent.claim,
                cycle: parent.next_cycle,
                role,
            })
    {
        return Err(ContractError::MissingEvidence.into());
    }
    match role {
        WorkRole::Output { slot } => {
            let claim = view
                .claim(parent.claim)
                .ok_or(ContractError::InvalidTarget)?;
            if !claim.acceptance().has_slot(slot) {
                return Err(ContractError::InvalidTarget.into());
            }
            let count = super::increments::count(claim, limits)?;
            super::response_budget::check_increment_shape(count, descriptor.input_count(), limits)?;
            if count != 0 {
                let registry = view
                    .owned_claim(parent.claim)?
                    .registrations()
                    .ok_or(ContractError::InvalidTarget)?;
                super::response_budget::check_registration_capacity_in(
                    view, claim, registry, limits,
                )?;
                if registry.is_sealed() || registry.increment_targets_sealed() {
                    return Err(ContractError::InvalidTransition.into());
                }
                registry.copy_with_additional_heap_bytes(count)?;
                // Do not expose Required targets whose mandatory restrictions
                // already make every eventual result impossible in this owner.
                if claim.acceptance().declarations().iter().any(|row| {
                    row.target() == focal_model::lifecycle::aggregation::ObligationTarget::Increment
                        && row.mode() == focal_model::ValidationMode::Required
                }) {
                    let report_limits =
                        super::completion_envelope::descriptor_limits(limits, claim, registry)?;
                    super::increment_authority::check_completion_visibility_view(
                        descriptor,
                        report_limits,
                        limits.plan_edges,
                    )?;
                }
                let mut evaluations = view.meta().evaluations;
                transactions::increment(
                    &mut evaluations,
                    count,
                    limits.evaluations,
                    "evaluations",
                )?;
            }
            if cycle.work_count >= work_limit {
                return Err(NativeError::Capacity("work artifacts per response cycle"));
            }
            if view
                .get(Key::WorkSlot(NativeCycleKey::of(&parent), slot))
                .is_some()
            {
                return Err(ContractError::InvalidManifest.into());
            }
        }
        WorkRole::Diagnostic { reason } => {
            if fields.kind != "error" {
                return Err(ContractError::MissingEvidence.into());
            }
            let maximum = limits.diagnostics_per_cycle.min(limits.plan_edges);
            if cycle.diagnostic_count >= maximum {
                return Err(NativeError::Capacity("diagnostics per response cycle"));
            }
            if reason != EvidenceFailure::Work {
                let claim = view
                    .claim(parent.claim)
                    .ok_or(ContractError::InvalidTarget)?;
                let (_, credit) = super::respondent_state::read(view, claim, limits)?
                    .ok_or(ContractError::InvalidTransition)?;
                // Extra diagnostics may not occupy the last slot still needed
                // for the respondent's own whole-cycle failure report.
                if credit.diagnostics == credit.closes
                    && cycle
                        .diagnostic_count
                        .checked_add(1)
                        .is_none_or(|next| next >= maximum)
                {
                    return Err(NativeError::Capacity("reserved respondent diagnostic slot"));
                }
            }
        }
        WorkRole::ReceiptRejection { .. } => return Err(ContractError::InvalidPolicy.into()),
    }
    if view.get(Key::Artifact(fields.id)).is_some()
        || view
            .get(Key::ArtifactIdentity(descriptor.content_hash()))
            .is_some()
        || view.get(Key::Work(fields.id)).is_some()
        || view.get(Key::Diagnostic(fields.id)).is_some()
    {
        return Err(ContractError::ContentConflict.into());
    }
    super::reporting::check_inputs_view(descriptor, view, limits)?;
    // An existing completion promise may reserve part of this allowance. The
    // publishing owner additionally checks its book before accepting the candidate.
    let mut artifacts = view.meta().artifacts;
    transactions::increment(&mut artifacts, 1, limits.artifacts, "artifacts")?;
    Ok(())
}

pub(super) fn observation<'a>(
    view: &View<'a>,
    context: NativeContext,
    claim: Binding,
    expected: Binding,
) -> Result<(Parent, &'a NativeWork), NativeError> {
    // Claimant observation follows the artifact's independent lifecycle. Its
    // original receipt/cycle remains inspectable after cancellation or adoption;
    // current open-work entitlement is required only for new submissions.
    let current = view
        .claim(ClaimId(claim.object.0))
        .ok_or(ContractError::InvalidTarget)?;
    current.binding().check(&claim)?;
    let parent = Parent::from_claim(current)?;
    context.principal.require_actor(parent.issuer)?;
    let id = ArtifactId(expected.object.0);
    let old = work(view, id)?;
    old.state.binding().check(&expected)?;
    if old.state.binding().ledger != parent.ledger {
        return Err(ContractError::WrongLedger.into());
    }
    if old.state.claim() != parent.claim {
        return Err(ContractError::WrongObject.into());
    }
    let cycle = NativeCycleKey {
        claim: old.state.claim(),
        receipt: old.state.receipt().receipt,
        epoch: old.state.receipt().epoch,
        cycle: old.state.cycle(),
    };
    match view.get(Key::Cycle(cycle)) {
        Some(Row::Cycle(cycle)) if cycle.work_count != 0 && cycle.work_head.is_some() => {}
        _ => return Err(ContractError::InvalidManifest.into()),
    }
    match view.get(Key::WorkSlot(cycle, old.state.slot())) {
        Some(Row::WorkSlot(artifact)) if *artifact == id => {}
        _ => return Err(ContractError::InvalidManifest.into()),
    }
    let artifact =
        as_artifact(view.get(Key::Artifact(id))).ok_or(ContractError::MissingEvidence)?;
    if artifact.descriptor().content_hash() != old.state.binding().content
        || artifact.descriptor().producer() != old.state.producer()
        || artifact.descriptor().receipt() != Some(old.state.receipt())
        || artifact.descriptor().work_provenance()
            != Some(WorkProvenance {
                claim: old.state.claim(),
                cycle: old.state.cycle(),
                role: WorkRole::Output {
                    slot: old.state.slot(),
                },
            })
    {
        return Err(ContractError::MissingEvidence.into());
    }
    Ok((parent, old))
}

fn received(
    view: &View<'_>,
    context: NativeContext,
    claim: Binding,
    expected: Binding,
    _limits: NativeLimits,
) -> Result<NativeWork, NativeError> {
    let (parent, old) = observation(view, context, claim, expected)?;
    Ok(NativeWork {
        state: old.state.receive(&expected, &parent, context.principal)?,
        next: old.next,
    })
}

/// Read-only actual-owner preflight. Call before schema verification or content
/// IO; returned participant descriptors confer no custody by themselves.
pub(super) fn authorize<'a>(
    view: &View<'_>,
    context: NativeContext,
    command: &'a NativeCommand,
    limits: NativeLimits,
) -> Result<Option<&'a ArtifactDescriptor>, NativeError> {
    match command {
        NativeCommand::FailWorkProduction { .. } | NativeCommand::RejectWork { .. } => {
            super::work_failures::authorize(view, context, command, limits)
        }
        NativeCommand::SubmitWork {
            claim,
            slot,
            artifact,
        } => submission(
            view,
            context,
            *claim,
            WorkRole::Output { slot: *slot },
            artifact,
            limits,
        )
        .map(Some),
        NativeCommand::SubmitDiagnostic {
            claim,
            reason,
            artifact,
        } => submission(
            view,
            context,
            *claim,
            WorkRole::Diagnostic { reason: *reason },
            artifact,
            limits,
        )
        .map(Some),
        NativeCommand::ReceiveWork { claim, expected } => {
            received(view, context, *claim, *expected, limits)?;
            Ok(None)
        }
        _ => Ok(None),
    }
}

pub(super) fn empty_plan() -> transactions::Plan {
    transactions::Plan {
        rows: Vec::new(),
        registry: transactions::RegistryOverrides::new(),
        created: 0,
    }
}

pub(super) fn store_artifact(
    descriptor: ArtifactDescriptor,
    custody: focal_evidence::NativeLocalCustody,
    request: RequestKey,
    extras: &mut Extras,
    scratch: &mut Scratch,
) -> Result<(), NativeError> {
    let binding = descriptor.binding();
    let id = descriptor.id();
    scratch.charge(OwnedArtifact::container_charge())?;
    let artifact = OwnedArtifact::new(NativeArtifact::from_work(descriptor, custody, request)?)?;
    let heap = artifact.heap_charge()?;
    extras.push(Extra {
        key: Key::Artifact(id),
        row: Row::Artifact(artifact),
        heap,
        fact: Some(NativeFact::Artifact { binding }),
    })?;
    extras.push(Extra {
        key: Key::ArtifactIdentity(binding.content),
        row: Row::ArtifactIdentity(id),
        heap: 0,
        fact: None,
    })
}

#[allow(clippy::too_many_arguments)] // Exact internal transaction frame, not a participant options surface.
pub(super) fn prepare(
    command: NativeCommand,
    request: RequestKey,
    evidence: Option<&focal_evidence::VerifiedNativeArtifact>,
    context: NativeContext,
    cut: ClaimCut,
    view: &View<'_>,
    limits: NativeLimits,
    meta: &mut Meta,
    extras: &mut Extras,
    scratch: &mut Scratch,
) -> Result<transactions::Plan, NativeError> {
    if matches!(
        &command,
        NativeCommand::FailWorkProduction { .. } | NativeCommand::RejectWork { .. }
    ) {
        return super::work_failures::prepare(
            command, request, evidence, context, cut, view, limits, meta, extras, scratch,
        );
    }
    authorize(view, context, &command, limits)?;
    if !extras.rows.is_empty() {
        return Err(ContractError::InvalidTransition.into());
    }
    let (claim, role, input) = match command {
        NativeCommand::SubmitWork {
            claim,
            slot,
            artifact,
        } => (claim, WorkRole::Output { slot }, artifact),
        NativeCommand::SubmitDiagnostic {
            claim,
            reason,
            artifact,
        } => (claim, WorkRole::Diagnostic { reason }, artifact),
        NativeCommand::ReceiveWork { claim, expected } => {
            let next = received(view, context, claim, expected, limits)?;
            let binding = next.state.binding();
            let state = next.state.state();
            scratch.charge(OwnedWork::container_charge())?;
            let row = OwnedWork::new(next)?;
            let heap = row.heap_charge()?;
            extras.push(Extra {
                key: Key::Work(ArtifactId(binding.object.0)),
                row: Row::Work(row),
                heap,
                fact: Some(NativeFact::Work {
                    claim: ClaimId(claim.object.0),
                    before: Some(expected),
                    after: binding,
                    state,
                }),
            })?;
            return Ok(empty_plan());
        }
        _ => return Err(ContractError::InvalidTransition.into()),
    };
    let parent = parent(view, claim)?;
    let key = NativeCycleKey::of(&parent);
    let mut cycle = cycle(view, &parent, limits)?;
    scratch.charge(input.heap_charge()?)?;
    let descriptor = input.get().ok_or(ContractError::MissingEvidence)?;
    let verified = evidence.ok_or(ContractError::MissingEvidence)?;
    verified.check(request, descriptor)?;
    let custody = verified.custody();
    let binding = descriptor.binding();
    let id = descriptor.id();
    let attestation = EvidenceAttestation {
        descriptor_hash: binding.content,
        custody_revision: custody.local_revision(),
        durable: true,
        schema_valid: true,
    };
    transactions::increment(&mut meta.artifacts, 1, limits.artifacts, "artifacts")?;
    let mut plan = empty_plan();
    match role {
        WorkRole::Output { slot } => {
            let state = WorkArtifact::generate(
                binding,
                &parent,
                context.principal,
                slot,
                parent.receipt,
                &attestation,
            )?;
            let next = NativeWork {
                state,
                next: cycle.work_head,
            };
            transactions::increment(
                &mut cycle.work_count,
                1,
                limits.work_artifacts_per_cycle,
                "work artifacts per response cycle",
            )?;
            cycle.work_head = Some(id);
            store_artifact(input.into_descriptor()?, custody, request, extras, scratch)?;
            scratch.charge(OwnedWork::container_charge())?;
            let row = OwnedWork::new(next)?;
            let heap = row.heap_charge()?;
            extras.push(Extra {
                key: Key::Work(id),
                row: Row::Work(row),
                heap,
                fact: Some(NativeFact::Work {
                    claim: parent.claim,
                    before: None,
                    after: binding,
                    state: state.state(),
                }),
            })?;
            extras.push(Extra {
                key: Key::WorkSlot(key, slot),
                row: Row::WorkSlot(id),
                heap: 0,
                fact: None,
            })?;
            let claim = view
                .claim(parent.claim)
                .ok_or(ContractError::InvalidTarget)?;
            let count = super::increments::count(claim, limits)?;
            if count != 0 {
                let source = view
                    .owned_claim(parent.claim)?
                    .registrations()
                    .ok_or(ContractError::InvalidTarget)?;
                let heap = source.copy_with_additional_heap_bytes(count)?;
                let charge = super::prepare::add(
                    heap,
                    if heap == 0 {
                        0
                    } else {
                        super::prepare::ALLOCATION
                    },
                )?;
                scratch.charge(charge)?;
                let mut registry = source
                    .try_copy_with_additional(count, source.copy_with_additional_charge(count)?)?;
                super::prepare::within(transactions::registry_heap(&registry)?, charge)?;
                super::increments::prepare(
                    context,
                    view,
                    claim,
                    &state,
                    limits,
                    meta,
                    &mut registry,
                    extras,
                    scratch,
                )?;
                plan.rows = scratch.reserve::<ClaimState>(1)?;
                scratch.charge(super::prepare::heap(claim)?)?;
                plan.rows.push(claim.try_copy(claim.retained_bytes()?)?);
                plan.registry
                    .insert(claim, registry, limits.plan_nodes, scratch)?;
            }
        }
        WorkRole::Diagnostic { reason } => {
            let diagnostic = ResponseDiagnostic::record_native(
                &parent,
                context.principal,
                parent.receipt,
                Diagnostic {
                    reason,
                    artifact: ArtifactRef {
                        id,
                        hash: binding.content,
                    },
                },
                descriptor,
                &attestation,
            )?;
            let next = NativeDiagnostic {
                diagnostic,
                next: cycle.diagnostic_head,
            };
            transactions::increment(
                &mut cycle.diagnostic_count,
                1,
                limits.diagnostics_per_cycle,
                "diagnostics per response cycle",
            )?;
            cycle.diagnostic_head = Some(id);
            store_artifact(input.into_descriptor()?, custody, request, extras, scratch)?;
            scratch.charge(OwnedDiagnostic::container_charge())?;
            let row = OwnedDiagnostic::new(next)?;
            let heap = row.heap_charge()?;
            extras.push(Extra {
                key: Key::Diagnostic(id),
                row: Row::Diagnostic(row),
                heap,
                fact: Some(NativeFact::Diagnostic {
                    claim: parent.claim,
                    binding,
                    reason,
                }),
            })?;
        }
        WorkRole::ReceiptRejection { .. } => return Err(ContractError::InvalidPolicy.into()),
    }
    extras.push(Extra {
        key: Key::Cycle(key),
        row: Row::Cycle(cycle),
        heap: 0,
        fact: (!plan.registry.is_empty()).then_some(NativeFact::Registrations { claim }),
    })?;
    Ok(plan)
}
