use super::*;
use crate::native::report_tests::{ISSUER, core, post, request};
use focal_memory::{Allocation, BudgetKind};
use focal_model::lifecycle::artifact_descriptor::ResultProvenance;

fn admission(claim: u128, index: u128) -> EvaluationKey {
    EvaluationKey {
        claim: ClaimId::from_u128(claim),
        validation: ValidationId::from_u128(claim * 100 + index),
        target: EvaluationTarget::Admission,
        generation: 1,
    }
}

fn stage(owner: &mut NativeOwner, input: NativeInput) -> NativeCandidate {
    match owner
        .prepare(context(input.request.principal, 30), input, None)
        .unwrap()
    {
        NativeStaging::Prepared { candidate, .. } => candidate,
        existing => panic!("fresh operation: {existing:?}"),
    }
}

fn failure_input(owner: &NativeOwner, claim: u128) -> NativeInput {
    let key = admission(claim, 1);
    let view = owner.effective();
    let state = view.evaluation(key).unwrap();
    let attempt = state
        .bind(view.definition(key.validation).unwrap())
        .unwrap()
        .current_attempt()
        .unwrap();
    let artifact = descriptor(artifact_spec(
        40_000 + claim,
        attempt.evaluator,
        VerdictValue::Fail,
    ))
    .with_result_provenance(ResultProvenance {
        claim: key.claim,
        validation: key.validation,
        target: state.target(),
        generation: state.generation(),
        attempt,
        value: VerdictValue::Fail,
    })
    .unwrap();
    NativeInput {
        request: request(attempt.evaluator, 50_000 + claim),
        command: NativeCommand::ReportAdmission {
            claim: view.claim(key.claim).unwrap().binding(),
            key,
            expected: state.binding(),
            report: validation::Report {
                generation: state.generation(),
                attempt,
                value: VerdictValue::Fail,
                evidence: ArtifactRef {
                    id: artifact.id(),
                    hash: artifact.content_hash(),
                },
            },
            artifact: NativeArtifactInput::new(artifact).unwrap(),
        },
    }
}

fn fail(owner: &mut NativeOwner, store: &mut ContentStore, claim: u128) -> NativeCandidate {
    let input = failure_input(owner, claim);
    match owner
        .prepare_with_custody(
            context(input.request.principal, 100),
            input,
            store,
            ContentDomainId::from_u128(19_001),
            &BuiltinNativeSchemas,
        )
        .unwrap()
    {
        NativeStaging::Prepared { candidate, .. } => candidate,
        existing => panic!("fresh report: {existing:?}"),
    }
}

fn exhaust(owner: &NativeOwner) -> Allocation {
    let source = owner.budget_for_test();
    source
        .reserve(
            BudgetKind::Payload,
            BudgetLane::Completion,
            source.limit() - source.stats().used,
        )
        .unwrap()
        .commit()
}

#[test]
fn separate_pending_seal_journals_retain_and_refund_their_own_credit_under_pressure() {
    let mut core = core();
    for claim in [1, 2] {
        publish(
            &mut core,
            10,
            creation(
                10_000 + claim,
                claim,
                &[
                    (ValidationMode::Required, false),
                    (ValidationMode::Observe, false),
                ],
                None,
            ),
        );
        publish(&mut core, 10, post(11_000 + claim, binding(claim)));
    }
    let mut owner = NativeOwner::new(core).unwrap();
    for claim in [1, 2] {
        for index in [1, 2] {
            let key = admission(claim, index);
            let view = owner.effective();
            let input = NativeInput {
                request: request(EVALUATOR, 20_000 + claim * 100 + index),
                command: NativeCommand::BeginAdmission {
                    claim: view.claim(key.claim).unwrap().binding(),
                    key,
                    expected: view.evaluation(key).unwrap().binding(),
                },
            };
            let candidate = stage(&mut owner, input);
            owner.publish_after_durable(candidate).unwrap();
        }
    }
    let directory = tempfile::tempdir().unwrap();
    let mut store = ContentStore::open(
        directory.path(),
        StoreLimits {
            max_content_bytes: 2 * 1024 * 1024,
            max_staging_bytes: 4 * 1024 * 1024,
            max_uploads: 8,
            chunk_bytes: 17,
            max_manifest_bytes: 128 * 1024,
        },
    )
    .unwrap();
    let pressure = exhaust(&owner);
    let before = owner.budget_stats();
    let committed = owner.committed().sequence();
    let journal = crate::native::completion_book::journal_bytes(2).unwrap();
    // Each isolated Required grant now retains its one-member graph protection.
    // Pending retirement keeps that owned buffer until the report commits.
    let graph_members = crate::native::prepare::array::<ClaimId>(1).unwrap();
    assert!(journal > 0);

    let first = fail(&mut owner, &mut store, 1);
    let first_writes = owner
        .prepared_candidate(first)
        .unwrap()
        .mutation_heap_bytes();
    assert!(first_writes > 0);
    let after_first = owner.budget_stats();
    assert_eq!(after_first.used, before.used);
    assert_eq!(
        after_first.by_kind[BudgetKind::Pending as usize],
        before.by_kind[BudgetKind::Pending as usize] + journal + first_writes
    );
    let second = fail(&mut owner, &mut store, 2);
    let second_writes = owner
        .prepared_candidate(second)
        .unwrap()
        .mutation_heap_bytes();
    assert!(second_writes > 0);
    let after_second = owner.budget_stats();
    assert_eq!(after_second.used, before.used);
    assert_eq!(
        after_second.by_kind[BudgetKind::Pending as usize],
        before.by_kind[BudgetKind::Pending as usize] + 2 * journal + first_writes + second_writes
    );
    assert_eq!(owner.pending_len(), 2);
    assert_eq!(owner.committed().sequence(), committed);
    for claim in [1, 2] {
        assert_eq!(
            owner
                .effective()
                .claim(admission(claim, 1).claim)
                .unwrap()
                .status(),
            ClaimStatus::PostFailed
        );
        let observer = owner.effective().evaluation(admission(claim, 2)).unwrap();
        assert!(observer.has_begun());
        assert!(observer.sealed().is_some());
        assert!(!observer.state().is_terminal());
    }

    assert_eq!(owner.discard_from(second).unwrap(), 1);
    assert_eq!(owner.budget_stats(), after_first);
    assert!(
        owner
            .effective()
            .evaluation(admission(2, 2))
            .unwrap()
            .sealed()
            .is_none()
    );
    let second = fail(&mut owner, &mut store, 2);
    assert_eq!(owner.budget_stats(), after_second);
    owner.publish_after_durable(first).unwrap();
    assert_eq!(
        owner.budget_stats().by_kind[BudgetKind::Pending as usize],
        before.by_kind[BudgetKind::Pending as usize] + journal + second_writes - graph_members
    );
    assert!(
        owner
            .candidate(second)
            .unwrap()
            .evaluation(admission(2, 2))
            .unwrap()
            .sealed()
            .is_some()
    );
    owner.publish_after_durable(second).unwrap();
    assert_eq!(owner.pending_len(), 0);
    assert_eq!(
        owner.budget_stats().by_kind[BudgetKind::Pending as usize],
        before.by_kind[BudgetKind::Pending as usize] - 2 * graph_members
    );
    assert_eq!(
        owner
            .committed()
            .claim(admission(1, 1).claim)
            .unwrap()
            .issuer(),
        ISSUER
    );
    drop(pressure);
}
