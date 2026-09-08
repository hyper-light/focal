use super::*;
use crate::lifecycle::aggregation::RegistrationSet;
use crate::lifecycle::memory as bytes;

#[path = "audit_snapshot_tests.rs"]
mod snapshot_tests;

fn fixture() -> (
    Vec<v::Declaration>,
    ClaimState,
    RegistrationSet,
    [v::EvaluationState; 2],
) {
    let handler = HandlerRef {
        id: ValidatorId::from_u128(1),
        version: ContentHash([2; 32]),
        agentic: false,
    };
    let steps = [v::HandlerPolicy {
        handler: &handler,
        attempts: 2,
        proof_schema: ContentHash([3; 32]),
        diagnostic_schema: ContentHash([3; 32]),
    }];
    let declarations = vec![
        receipt(),
        declaration(2, &steps, ValidationMode::Required),
        declaration(3, &steps, ValidationMode::Observe),
    ];
    let mut c = claim(&declarations);
    let ready = evaluation(&declarations[1]);
    let other = evaluation(&declarations[2]);
    let running = other
        .begin(
            Principal::Actor(evaluator()),
            &other.binding(),
            &owner(&other),
        )
        .unwrap()
        .next;
    let mut registry = RegistrationSet::new(&c, 8, usize::MAX).unwrap();
    // Native traversal order is deliberately different from canonical audit order.
    registry.register(&c, &other, usize::MAX).unwrap();
    registry.register(&c, &ready, usize::MAX).unwrap();
    fail_claim(&mut c);
    registry.seal_targets(&c).unwrap();
    let running = running
        .into_state()
        .seal_claim(&declarations[2], &running.binding(), &c)
        .unwrap()
        .next();
    let ready = ready
        .into_state()
        .seal_claim(&declarations[1], &ready.binding(), &c)
        .unwrap()
        .next();
    (declarations, c, registry, [running, ready])
}

fn limits() -> Limits {
    Limits {
        evaluations: 8,
        results: 8,
    }
}

#[test]
fn native_bridge_uses_complete_stamped_membership_and_preserves_legacy_audit_order() {
    let (declarations, c, registry, states) = fixture();
    let evaluations = [
        states[0].bind(&declarations[2]).unwrap(),
        states[1].bind(&declarations[1]).unwrap(),
    ];
    let targets = registry.audit_targets(&c).unwrap();
    assert_eq!(targets.claim().binding(), c.binding());
    assert_eq!(targets.sealed_at(), SessionSeq(5));
    assert_eq!(targets.rows().len(), 2);
    let cohort =
        AuditCohort::prepare_native(targets, &evaluations, &[], limits(), usize::MAX, usize::MAX)
            .unwrap()
            .build()
            .unwrap();
    assert!(!cohort.complete());
    assert_eq!(cohort.claim_binding(), c.binding());
    assert_eq!(cohort.issuer(), issuer());
    assert_eq!(cohort.sealed_at(), SessionSeq(5));
    assert_eq!(cohort.result_capacity(), 4);
    assert_eq!(
        cohort.members()[0].key().validation,
        ValidationId::from_u128(2)
    );
    assert_eq!(
        cohort.members()[1].key().validation,
        ValidationId::from_u128(3)
    );

    let original = claim(&declarations);
    let mut legacy = EvaluationRegistry::new(&original, acceptance_limits()).unwrap();
    for definition in &declarations[1..] {
        legacy.register(&evaluation(definition)).unwrap();
    }
    let old = AuditCohort::seal(
        &c,
        &legacy.seal_targets(),
        SessionSeq(5),
        &evaluations,
        &[],
        limits(),
    )
    .unwrap();
    assert_eq!(cohort.members(), old.members());
    assert_eq!(cohort.results(), old.results());

    for changed in [
        vec![evaluations[0]],
        vec![evaluations[0], evaluations[0]],
        vec![evaluations[1], evaluations[0]],
    ] {
        assert!(
            AuditCohort::prepare_native(
                registry.audit_targets(&c).unwrap(),
                &changed,
                &[],
                limits(),
                usize::MAX,
                usize::MAX
            )
            .is_err()
        );
    }
    let copied = registry.try_copy(registry.copy_charge().unwrap()).unwrap();
    assert_eq!(copied.audit_targets(&c).unwrap().sealed_at(), SessionSeq(5));
}

#[test]
fn unstamped_legacy_seal_and_a_different_original_claim_cut_cannot_grant_native_audit_authority() {
    let (declarations, c, registry, _) = fixture();
    let mut original = claim(&declarations);
    let mut unstamped = RegistrationSet::new(&original, 8, usize::MAX).unwrap();
    unstamped.seal_targets(&original).unwrap();
    fail_claim(&mut original);
    assert_eq!(
        unstamped.audit_targets(&original).unwrap_err(),
        ContractError::InvalidCut
    );

    let mut later = claim(&declarations);
    let reference = ArtifactRef {
        id: ArtifactId::from_u128(999),
        hash: ContentHash([9; 32]),
    };
    later
        .report_boundary_failure(
            &later.binding(),
            Principal::Actor(issuer()),
            BoundaryFailure::Post,
            Diagnostic {
                reason: EvidenceFailure::Structure,
                artifact: reference,
            },
            &EvidenceAttestation {
                descriptor_hash: reference.hash,
                custody_revision: 1,
                durable: true,
                schema_valid: true,
            },
            ClaimCut {
                position: SessionSeq(6),
                cause: reference.hash,
            },
        )
        .unwrap();
    assert_eq!(later.binding(), c.binding());
    assert_eq!(
        registry.audit_targets(&later).unwrap_err(),
        ContractError::InvalidCut
    );
    assert_eq!(
        registry.audit_targets(&c).unwrap().sealed_at(),
        SessionSeq(5)
    );
}

#[test]
fn native_audit_preflight_prices_both_buffers_and_rejects_before_any_allocation() {
    let (declarations, c, registry, states) = fixture();
    let evaluations = [
        states[0].bind(&declarations[2]).unwrap(),
        states[1].bind(&declarations[1]).unwrap(),
    ];
    let quote = AuditCohort::prepare_native(
        registry.audit_targets(&c).unwrap(),
        &evaluations,
        &[],
        limits(),
        usize::MAX,
        usize::MAX,
    )
    .unwrap();
    let charge = quote.construction_charge();
    let visits = quote.visits();
    assert_eq!(quote.heap_allocations(), 2);
    assert_eq!(
        charge,
        std::mem::size_of::<AuditCohort>()
            + quote.heap_bytes()
            + 2 * 4 * std::mem::size_of::<usize>()
    );
    bytes::fail_after(0, || {
        for (byte_limit, visit_limit) in [(charge - 1, visits), (charge, visits - 1)] {
            assert!(matches!(
                AuditCohort::prepare_native(
                    registry.audit_targets(&c).unwrap(),
                    &evaluations,
                    &[],
                    limits(),
                    byte_limit,
                    visit_limit
                ),
                Err(ContractError::Capacity)
            ));
            assert_eq!(bytes::remaining_allocations(), Some(0));
        }
    });
    for allocation in 0..2 {
        bytes::fail_after(allocation, || {
            let plan = AuditCohort::prepare_native(
                registry.audit_targets(&c).unwrap(),
                &evaluations,
                &[],
                limits(),
                charge,
                visits,
            )
            .unwrap();
            assert!(matches!(plan.build(), Err(ContractError::Capacity)));
        });
        assert_eq!(registry.rows().len(), 2);
        assert_eq!(c.local_sealed_at(), Some(SessionSeq(5)));
    }
    let cohort = quote.build().unwrap();
    assert!(
        cohort.retained_bytes().unwrap()
            + cohort.heap_allocations().unwrap() * 4 * std::mem::size_of::<usize>()
            <= charge
    );
}

#[test]
fn fallible_audit_copy_keeps_late_capacity_and_independent_results_then_copies_posted_bundle() {
    let (declarations, c, registry, states) = fixture();
    let evaluations = [
        states[0].bind(&declarations[2]).unwrap(),
        states[1].bind(&declarations[1]).unwrap(),
    ];
    let original = AuditCohort::prepare_native(
        registry.audit_targets(&c).unwrap(),
        &evaluations,
        &[],
        limits(),
        usize::MAX,
        usize::MAX,
    )
    .unwrap()
    .build()
    .unwrap();
    for allocation in 0..2 {
        bytes::fail_after(allocation, || {
            assert!(matches!(
                original.try_copy(original.copy_charge().unwrap()),
                Err(ContractError::Capacity)
            ))
        });
    }
    bytes::fail_after(0, || {
        assert!(matches!(
            original.try_copy(original.copy_charge().unwrap() - 1),
            Err(ContractError::Capacity)
        ));
        assert_eq!(bytes::remaining_allocations(), Some(0));
    });
    let mut copied = original.try_copy(original.copy_charge().unwrap()).unwrap();
    assert_eq!(copied.result_capacity(), original.result_capacity());
    assert_ne!(copied.members().as_ptr(), original.members().as_ptr());
    assert_eq!(
        copied.copy_heap_bytes().unwrap(),
        original.copy_heap_bytes().unwrap()
    );
    let error = report(&evaluations[0], VerdictValue::Error, 20);
    let passed = report(&error.next, VerdictValue::Pass, 21);
    bytes::fail_after(0, || {
        copied.record(&error.next).unwrap();
        copied.record(&passed.next).unwrap();
        assert_eq!(bytes::remaining_allocations(), Some(0));
    });
    assert!(copied.complete());
    assert!(!original.complete());
    assert_eq!(original.result_count(), 0);
    assert_eq!(
        copied.results(),
        &[error.result.unwrap(), passed.result.unwrap()]
    );
    let mut bundle =
        ResultTestament::generate(binding(30), Principal::Actor(issuer()), copied).unwrap();
    bundle
        .post(Principal::Actor(issuer()), &bundle.binding())
        .unwrap();
    let owned = bundle.try_copy(bundle.copy_charge().unwrap()).unwrap();
    assert_eq!(owned.binding(), bundle.binding());
    assert_eq!(owned.state(), ResultTestamentState::Posted);
    assert_eq!(owned.members(), bundle.members());
    assert_eq!(owned.results(), bundle.results());
    assert_ne!(owned.results().as_ptr(), bundle.results().as_ptr());
}

#[test]
fn complete_native_freeze_uses_exact_history_capacity_and_rejects_reordered_or_missing_attempts() {
    let (declarations, c, registry, states) = fixture();
    let running = states[0].bind(&declarations[2]).unwrap();
    let error = report(&running, VerdictValue::Error, 20);
    let pass = report(&error.next, VerdictValue::Pass, 21);
    let evaluations = [pass.next, states[1].bind(&declarations[1]).unwrap()];
    let history = [error.result.unwrap(), pass.result.unwrap()];
    let exact = Limits {
        evaluations: 2,
        results: 2,
    };
    let cohort = AuditCohort::prepare_native(
        registry.audit_targets(&c).unwrap(),
        &evaluations,
        &history,
        exact,
        usize::MAX,
        usize::MAX,
    )
    .unwrap()
    .build()
    .unwrap();
    assert!(cohort.complete());
    assert_eq!(cohort.result_capacity(), history.len());
    assert_eq!(cohort.results(), &history);
    for changed in [
        vec![history[1]],
        vec![history[1], history[0]],
        vec![history[0], history[0]],
    ] {
        assert!(
            AuditCohort::prepare_native(
                registry.audit_targets(&c).unwrap(),
                &evaluations,
                &changed,
                exact,
                usize::MAX,
                usize::MAX
            )
            .is_err()
        );
    }
    let unfinished = [running, evaluations[1]];
    assert!(matches!(
        AuditCohort::prepare_native(
            registry.audit_targets(&c).unwrap(),
            &unfinished,
            &[],
            exact,
            usize::MAX,
            usize::MAX
        ),
        Err(ContractError::Capacity)
    ));

    // A terminal result may predate the claim seal. The automatic writer keeps
    // that exact terminal row, without requiring an invented seal revision.
    let ready = evaluation(&declarations[2]);
    let begun = ready
        .begin(
            Principal::Actor(evaluator()),
            &ready.binding(),
            &owner(&ready),
        )
        .unwrap()
        .next;
    let finished = report(&begun, VerdictValue::Pass, 22);
    assert!(finished.next.sealed().is_none());
    let terminal = [finished.next, evaluations[1]];
    let old_history = [finished.result.unwrap()];
    let terminal_cohort = AuditCohort::prepare_native(
        registry.audit_targets(&c).unwrap(),
        &terminal,
        &old_history,
        exact,
        usize::MAX,
        usize::MAX,
    )
    .unwrap()
    .build()
    .unwrap();
    assert!(terminal_cohort.complete());
    assert_eq!(terminal_cohort.results(), &old_history);
    assert_eq!(terminal_cohort.result_capacity(), 1);
}
