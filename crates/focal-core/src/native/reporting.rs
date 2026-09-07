//! Report one already-begun Admission/Increment attempt with immutable evidence.
//! All derived rows remain detached until the common owner publication.
use super::prepare::{Extra, Extras, Scratch, heap};
use super::*;
use focal_model::lifecycle::{
    aggregation, artifact_descriptor::ArtifactDescriptor, audit, claim::ClaimCut,
};
use focal_model::{ObjectKind, VerdictValue};

// Report facts precede their derived claim transition. This fixed ordering avoids
// inventing a result's publication coordinates from its eventual parent outcome.
const ACCEPTED_ORDINAL: u32 = 2;

pub(super) fn check_inputs(
    descriptor: &ArtifactDescriptor,
    view: &View<'_>,
    limits: NativeLimits,
) -> Result<(), NativeError> {
    if descriptor.inputs().len() > limits.plan_edges {
        return Err(NativeError::Capacity("artifact input visits"));
    }
    let mut visits = limits.plan_edges;
    for input in descriptor.inputs() {
        visit(&mut visits)?;
        if input.ledger != view.ledger() {
            return Err(ContractError::WrongLedger.into());
        }
        let exists = match input.kind {
            ObjectKind::Claim => view.claim(ClaimId(input.id.0)).is_some(),
            ObjectKind::Validation => {
                as_definition(view.get(Key::Definition(ValidationId(input.id.0)))).is_some()
            }
            ObjectKind::Artifact => {
                let source = as_artifact(view.get(Key::Artifact(ArtifactId(input.id.0))));
                if let Some(source) = source {
                    // Both descriptors have canonical sorted labels. Require
                    // inherited restrictions without allocating a union/set.
                    let mut labels = descriptor.visibility();
                    for required in source.descriptor().visibility() {
                        visit(&mut visits)?;
                        loop {
                            visit(&mut visits)?;
                            match labels.next() {
                                Some(label) if label < required => continue,
                                Some(label) if label == required => break,
                                _ => return Err(ContractError::InvalidPolicy.into()),
                            }
                        }
                    }
                }
                source.is_some()
            }
            ObjectKind::Testament => {
                as_response(view.get(Key::Response(TestamentId(input.id.0)))).is_some()
            }
        };
        if !exists {
            return Err(ContractError::InvalidTarget.into());
        }
    }
    Ok(())
}

fn visit(remaining: &mut usize) -> Result<(), NativeError> {
    *remaining = remaining
        .checked_sub(1)
        .ok_or(NativeError::Capacity("artifact input visits"))?;
    Ok(())
}

#[allow(clippy::too_many_arguments)] // One checked internal owner transaction, not participant configuration.
pub(super) fn prepare(
    claim: Binding,
    key: EvaluationKey,
    expected: Binding,
    report: validation::Report,
    artifact: NativeArtifactInput,
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
    let descriptor = artifact.get().ok_or(ContractError::MissingEvidence)?;
    let authorize = if matches!(key.target, EvaluationTarget::Increment { .. }) {
        super::increment_authority::report
    } else {
        super::admission_authority::report
    };
    let (registered, authorization) = authorize(
        view, context, claim, key, expected, report, descriptor, limits,
    )?;
    let parent = registered.parent;
    let definition = registered.definition;
    let old = registered.state;
    let registry = registered.registry;
    let evaluation = old.bind(definition)?;
    let owner = if matches!(key.target, EvaluationTarget::Increment { .. }) {
        super::increment_authority::report_owner(view, &registered, context.logical_time)?
    } else {
        evaluation.admission_report_owner(parent, context.logical_time)?
    };
    let attempt = authorization.attempt();
    scratch.charge(artifact.heap_charge()?)?;
    let verified = evidence.ok_or(ContractError::MissingEvidence)?;
    verified.check(request, descriptor)?;
    let custody = verified.custody();
    let facts = validation::EvidenceFacts {
        binding: descriptor.binding(),
        claim: key.claim,
        validation: key.validation,
        target: old.target(),
        generation: old.generation(),
        attempt,
        producer: descriptor.producer(),
        value: report.value,
        kind: match report.value {
            VerdictValue::Pass | VerdictValue::Fail => validation::EvidenceKind::Proof,
            VerdictValue::Incomplete | VerdictValue::Error => validation::EvidenceKind::Diagnostic,
        },
        schema: descriptor.schema_hash(),
        custody_revision: Some(custody.local_revision()),
    };
    let transition = evaluation.report(context.principal, &expected, &owner, report, &facts)?;
    let result = transition.result.ok_or(ContractError::MissingEvidence)?;
    let next = transition.next.into_state();
    let result_key = NativeResultKey::of(result);
    if view.get(Key::Accepted(result_key)).is_some() {
        return Err(ContractError::StaleEvaluation.into());
    }
    let accepted = NativeAccepted::new(
        result,
        attempt,
        audit::ResultArtifact::from_result(result)?,
        cut.position,
        ACCEPTED_ORDINAL,
    )?;
    let mut rows = Vec::new();
    // Admission determines whether a Posted claim can be received. Once receipt
    // or a terminal cut has committed, a begun report remains independent audit
    // evidence. Do not scan later response/Increment cohorts: that would make an
    // already funded report depend on unrelated, growing projection work.
    if key.target == EvaluationTarget::Admission && parent.status() == ClaimStatus::Posted {
        let projection = super::admission_view::AdmissionRows {
            view,
            sequence: cut.position,
            claim: key.claim,
            report: Some((&next, &accepted)),
        };
        let decision = aggregation::project_admission(
            parent,
            registry,
            &projection,
            aggregation::AdmissionLimits {
                declarations: limits.definitions,
                evaluations: limits.evaluations_per_claim,
                visits: limits.plan_edges,
            },
        )?;
        if matches!(
            decision.outcome(),
            aggregation::AdmissionOutcome::Blocked(_)
        ) {
            rows = scratch.reserve::<ClaimState>(1)?;
            scratch.charge(heap(parent)?)?;
            let mut changed = parent.try_copy(parent.retained_bytes()?)?;
            changed.apply_admission(&claim, &decision)?;
            rows.push(changed);
        }
    }
    transactions::increment(&mut meta.artifacts, 1, limits.artifacts, "artifacts")?;
    transactions::increment(&mut meta.results, 1, limits.results, "results")?;
    let descriptor = artifact.into_descriptor()?;
    let binding = descriptor.binding();
    let artifact_id = descriptor.id();
    scratch.charge(OwnedArtifact::container_charge())?;
    let artifact = OwnedArtifact::new(NativeArtifact::new(descriptor, custody, facts)?)?;
    let artifact_heap = artifact.heap_charge()?;
    if !extras.rows.is_empty() {
        return Err(ContractError::InvalidTransition.into());
    }
    extras.push(Extra {
        key: Key::Artifact(artifact_id),
        row: Row::Artifact(artifact),
        heap: artifact_heap,
        fact: Some(NativeFact::Artifact { binding }),
    })?;
    extras.push(Extra {
        key: Key::ArtifactIdentity(binding.content),
        row: Row::ArtifactIdentity(artifact_id),
        heap: 0,
        fact: None,
    })?;
    scratch.charge(OwnedEvaluation::container_charge())?;
    let next_row = OwnedEvaluation::new(next)?;
    let next_heap = next_row.heap_charge()?;
    extras.push(Extra {
        key: Key::Evaluation(key),
        row: Row::Evaluation(next_row),
        heap: next_heap,
        fact: Some(NativeFact::Evaluation {
            kind: NativeEvaluationEventKind::Reported,
            key,
            before: Some(old.binding()),
            after: next.binding(),
            state: next.state(),
            phase: next.phase(),
            attempt: Some(attempt),
            fence: next.fence(),
        }),
    })?;
    if extras.events()
        != usize::try_from(ACCEPTED_ORDINAL).map_err(|_| NativeError::Capacity("result ordinal"))?
    {
        return Err(ContractError::InvalidCut.into());
    }
    scratch.charge(OwnedAccepted::container_charge())?;
    let accepted = OwnedAccepted::new(accepted)?;
    let accepted_heap = accepted.heap_charge()?;
    extras.push(Extra {
        key: Key::Accepted(result_key),
        row: Row::Accepted(accepted),
        heap: accepted_heap,
        fact: Some(NativeFact::Accepted { key: result_key }),
    })?;
    Ok(transactions::Plan {
        rows,
        registry: None,
        created: 0,
    })
}
