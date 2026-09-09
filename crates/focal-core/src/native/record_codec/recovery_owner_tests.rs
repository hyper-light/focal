//! A restored checkpoint must recover promised completion capacity before new
//! traffic is admitted. These tests use actual disk-backed evidence and exhaust
//! the enclosing memory owner, including its ordinary and completion headroom.
use super::tests as checkpoint;
use super::*;
use crate::native::report_tests as f;
use focal_evidence::BuiltinNativeSchemas;
use focal_evidence::ContentStore;
use focal_memory::{Allocation, BudgetKind};
use focal_model::lifecycle::artifact_descriptor::{ResultProvenance, WorkProvenance, WorkRole};
use focal_model::lifecycle::evidence::EvidenceFailure;
use focal_model::{
    ArtifactRef, Confidence, ContentDomainId, OutcomeKind, ValidationMode, VerdictValue,
};

fn pressure(budget: &MemoryBudget) -> Allocation {
    budget
        .reserve(
            BudgetKind::Payload,
            BudgetLane::Completion,
            budget.limit() - budget.stats().used,
        )
        .unwrap()
        .commit()
}
fn restored(core: &Core<NativeState>, store: &ContentStore, range: u128) -> Core<NativeState> {
    let bytes = checkpoint::encode(core);
    let mut limits = checkpoint::limits(core.limits);
    limits.native.plan_edges = 65_536;
    limits.native.work_artifacts_per_cycle = 3;
    limits.native.diagnostics_per_cycle = 3;
    limits.native.response_summary_bytes = 256;
    restore(
        &checkpoint::inspect(&bytes),
        RangeId(range),
        limits,
        checkpoint::budget(),
        store,
        &BuiltinNativeSchemas,
    )
    .unwrap()
}
fn owner_after_refusal(core: Core<NativeState>) -> (NativeOwner, MemoryBudget) {
    let budget = core.state.budget.clone();
    let baseline = checkpoint::encode(&core);
    let before = budget.stats();
    let occupied = pressure(&budget);
    let full = budget.stats();
    let refusal = NativeOwner::with_schemas(core, &BuiltinNativeSchemas).unwrap_err();
    assert_eq!(budget.stats(), full);
    assert_eq!(checkpoint::encode(&refusal.core), baseline);
    drop(occupied);
    assert_eq!(budget.stats(), before);
    let owner = NativeOwner::with_schemas(refusal.core, &BuiltinNativeSchemas).unwrap();
    assert_eq!(owner.pending_len(), 0);
    (owner, budget)
}
fn stage(
    owner: &mut NativeOwner,
    input: NativeInput,
    store: &mut ContentStore,
) -> (NativeCandidate, NativeOutcome) {
    let time = owner.effective().logical_time() + 1;
    match owner
        .prepare_with_custody(
            f::context(input.request.principal, time),
            input,
            store,
            ContentDomainId::from_u128(93),
            &BuiltinNativeSchemas,
        )
        .unwrap()
    {
        NativeStaging::Prepared { candidate, outcome } => (candidate, outcome),
        other => panic!("expected a fresh completion, got {other:?}"),
    }
}
fn admission(owner: &NativeOwner, id: u128, value: VerdictValue) -> NativeInput {
    let key = f::key(1);
    let view = owner.effective();
    let state = view.evaluation(key).unwrap();
    let attempt = state
        .bind(view.definition(key.validation).unwrap())
        .unwrap()
        .current_attempt()
        .unwrap();
    let artifact = f::descriptor(f::artifact_spec(id, attempt.evaluator, value))
        .with_result_provenance(ResultProvenance {
            claim: key.claim,
            validation: key.validation,
            target: state.target(),
            generation: state.generation(),
            attempt,
            value,
        })
        .unwrap();
    let reference = ArtifactRef {
        id: artifact.id(),
        hash: artifact.content_hash(),
    };
    NativeInput {
        request: f::request(attempt.evaluator, id),
        command: NativeCommand::ReportAdmission {
            claim: view.claim(key.claim).unwrap().binding(),
            key,
            expected: state.binding(),
            report: validation::Report {
                generation: state.generation(),
                attempt,
                value,
                evidence: reference,
            },
            artifact: NativeArtifactInput::new(artifact).unwrap(),
        },
    }
}

#[test]
fn checkpoint_owner_restores_begun_retry_and_quality_reports_under_exhausted_parent_memory() {
    let original = f::running(&[(ValidationMode::Observe, true)]);
    let directory = tempfile::tempdir().unwrap();
    let mut store = checkpoint::store(directory.path());
    let core = restored(&original, &store, 12_001);
    let (mut owner, budget) = owner_after_refusal(core);
    let original_claim = owner.committed().claim(f::key(1).claim).unwrap().binding();
    let mut held = Vec::new();
    for (id, value) in [
        (13_001, VerdictValue::Error),
        (13_002, VerdictValue::Pass),
        (13_003, VerdictValue::Pass),
    ] {
        let input = admission(&owner, id, value);
        let retry = f::copy_report(&input);
        let committed_retry = f::copy_report(&input);
        held.push(pressure(&budget));
        assert_eq!(budget.stats().used, budget.limit());
        let (candidate, outcome) = stage(&mut owner, input, &mut store);
        assert!(owner.committed().recorded(outcome.invocation).is_none());
        assert_eq!(owner.pending_len(), 1);
        let before = budget.stats();
        assert!(
            matches!(owner.prepare_with_custody(f::context(retry.request.principal, 200), retry,
            &mut store, ContentDomainId::from_u128(93), &BuiltinNativeSchemas).unwrap(),
            NativeStaging::Existing { outcome: found, candidate: Some(found_candidate) }
                if found == outcome && found_candidate == candidate)
        );
        assert_eq!(budget.stats(), before);
        owner.publish_after_durable(candidate).unwrap();
        assert_eq!(
            owner.committed().recorded(outcome.invocation),
            Some(outcome)
        );
        assert!(
            matches!(owner.prepare_with_custody(f::context(committed_retry.request.principal, 200),
            committed_retry, &mut store, ContentDomainId::from_u128(93), &BuiltinNativeSchemas).unwrap(),
            NativeStaging::Existing { outcome: found, candidate: None } if found == outcome)
        );
    }
    assert_eq!(
        owner.committed().evaluation(f::key(1)).unwrap().state(),
        validation::State::Validated
    );
    assert_eq!(
        owner.committed().claim(f::key(1).claim).unwrap().binding(),
        original_claim
    );
    assert_eq!(owner.pending_len(), 0);
    drop(held);
    drop(owner);
    assert_eq!(budget.stats().used, 0);
}

#[test]
fn checkpoint_owner_recovers_pending_post_then_diagnostic_failed_close_and_post_capacity() {
    let (original, mut store, _directory) = super::super::evidence::tests::recovery_fixture(0);
    let core = restored(&original, &store, 12_002);
    let (mut owner, budget) = owner_after_refusal(core);
    let claim = ClaimId::from_u128(1);
    let first = TestamentId::from_u128(900);
    let mut held = Vec::new();
    let initial = owner
        .committed()
        .response(first)
        .unwrap()
        .identity()
        .binding;
    let post = NativeInput {
        request: f::request(f::SUBJECT, 14_001),
        command: NativeCommand::PostResponse {
            claim: owner.committed().claim(claim).unwrap().binding(),
            expected: initial,
        },
    };
    held.push(pressure(&budget));
    let (candidate, _) = stage(&mut owner, post, &mut store);
    owner.publish_after_durable(candidate).unwrap();
    assert_eq!(
        owner
            .committed()
            .response(first)
            .unwrap()
            .reported_outcome(),
        OutcomeKind::Failed
    );

    let parent = owner.committed().claim(claim).unwrap();
    let mut spec = f::artifact_spec(14_002, f::SUBJECT, VerdictValue::Error);
    spec.receipt = Some(parent.receipt().unwrap().fence);
    spec.work = Some(WorkProvenance {
        claim,
        cycle: 2,
        role: WorkRole::Diagnostic {
            reason: EvidenceFailure::Work,
        },
    });
    let descriptor = f::descriptor(spec);
    let diagnostic = ArtifactRef {
        id: descriptor.id(),
        hash: descriptor.content_hash(),
    };
    let input = NativeInput {
        request: f::request(f::SUBJECT, 14_002),
        command: NativeCommand::SubmitDiagnostic {
            claim: parent.binding(),
            reason: EvidenceFailure::Work,
            artifact: NativeArtifactInput::new(descriptor).unwrap(),
        },
    };
    held.push(pressure(&budget));
    let (candidate, _) = stage(&mut owner, input, &mut store);
    owner.publish_after_durable(candidate).unwrap();

    let second = TestamentId::from_u128(14_003);
    let input = NativeInput {
        request: f::request(f::SUBJECT, 14_003),
        command: NativeCommand::CloseResponse {
            claim: owner.committed().claim(claim).unwrap().binding(),
            response: f::binding(14_003),
            report: NativeResponseInput {
                summary: "Work failed after restart.".into(),
                confidence: Confidence::Tentative,
                outcome: OutcomeKind::Failed,
                manifest: vec![],
                diagnostics: vec![diagnostic],
            },
        },
    };
    held.push(pressure(&budget));
    let (candidate, _) = stage(&mut owner, input, &mut store);
    owner.publish_after_durable(candidate).unwrap();
    let expected = owner
        .committed()
        .response(second)
        .unwrap()
        .identity()
        .binding;
    let input = NativeInput {
        request: f::request(f::SUBJECT, 14_004),
        command: NativeCommand::PostResponse {
            claim: owner.committed().claim(claim).unwrap().binding(),
            expected,
        },
    };
    held.push(pressure(&budget));
    let (candidate, outcome) = stage(&mut owner, input, &mut store);
    owner.publish_after_durable(candidate).unwrap();
    let response = owner.committed().response(second).unwrap();
    assert_eq!(response.reported_outcome(), OutcomeKind::Failed);
    assert_eq!(
        response
            .diagnostics()
            .iter()
            .map(|item| item.artifact())
            .collect::<Vec<_>>(),
        vec![diagnostic]
    );
    assert!(owner.committed().artifact(diagnostic.id).is_some());
    assert_eq!(
        owner.committed().recorded(outcome.invocation),
        Some(outcome)
    );
    assert_eq!(owner.committed().claim(claim).unwrap().response_count(), 2);
    drop(held);
    drop(owner);
    assert_eq!(budget.stats().used, 0);
}
