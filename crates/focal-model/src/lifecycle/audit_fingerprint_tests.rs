use super::*;
use crate::lifecycle::aggregation::{CauseTarget, RegistrationSet};
use crate::lifecycle::memory as bytes;

fn fixed_cohort() -> AuditCohort {
    let claim = Binding {
        revision: ObjectRevision(3),
        ..binding(1)
    };
    let key = EvaluationKey {
        validation: ValidationId::from_u128(2),
        target: Target::Admission { claim },
        generation: 1,
    };
    AuditCohort {
        claim,
        issuer: issuer(),
        sequence: SessionSeq(5),
        members: vec![AuditMember {
            definition: v::DefinitionStamp::fixture(),
            key,
            order: order(key.target, 2, 1, key.validation),
            binding: Binding {
                revision: ObjectRevision(2),
                ..binding(2)
            },
            receipt: None,
            begun: false,
            state: State::Ready,
            suppression: Some(Suppression::CohortSealed(ContentHash([9; 32]))),
            fence: None,
            last_result: None,
            sealed: Some(ContentHash([9; 32])),
        }],
        results: Vec::new(),
        result_capacity: 0,
    }
}

fn complete_in_order(arrival: [usize; 2]) -> AuditCohort {
    let handler = HandlerRef {
        id: ValidatorId::from_u128(1),
        version: ContentHash([2; 32]),
        agentic: false,
    };
    let steps = [v::HandlerPolicy {
        handler: &handler,
        attempts: 1,
        proof_schema: ContentHash([3; 32]),
        diagnostic_schema: ContentHash([3; 32]),
    }];
    let definitions = [
        receipt(),
        declaration(2, &steps, ValidationMode::Observe),
        declaration(3, &steps, ValidationMode::Observe),
    ];
    let mut c = claim(&definitions);
    let ready = [evaluation(&definitions[1]), evaluation(&definitions[2])];
    let mut registry = RegistrationSet::new(&c, 2, usize::MAX).unwrap();
    let mut begun = Vec::new();
    for evaluation in &ready {
        registry.register(&c, evaluation, usize::MAX).unwrap();
        begun.push(
            evaluation
                .begin(
                    Principal::Actor(evaluator()),
                    &evaluation.binding(),
                    &owner(evaluation),
                )
                .unwrap()
                .next,
        );
    }
    fail_claim(&mut c);
    registry.seal_targets(&c).unwrap();
    let sealed: Vec<_> = begun
        .iter()
        .zip(&definitions[1..])
        .map(|(evaluation, definition)| {
            evaluation
                .into_state()
                .seal_claim(definition, &evaluation.binding(), &c)
                .unwrap()
                .next()
                .bind(definition)
                .unwrap()
        })
        .collect();
    let mut cohort = AuditCohort::prepare_native(
        registry.audit_targets(&c).unwrap(),
        &sealed,
        &[],
        Limits {
            evaluations: 2,
            results: 2,
        },
        usize::MAX,
        usize::MAX,
    )
    .unwrap()
    .build()
    .unwrap();
    for (member, evaluation) in cohort.members().iter().zip(&sealed) {
        assert!(member.suppression().is_none());
        assert_eq!(member.seal_cause(), evaluation.sealed());
        assert!(member.seal_cause().is_some());
    }
    assert_eq!(
        cohort.content_fingerprint(),
        Err(ContractError::InvalidTransition)
    );
    let results = [
        report(&sealed[0], VerdictValue::Pass, 40),
        report(&sealed[1], VerdictValue::Fail, 41),
    ];
    bytes::fail_after(0, || {
        for index in arrival {
            cohort.record(&results[index].next).unwrap();
        }
        assert_eq!(bytes::remaining_allocations(), Some(0));
    });
    assert!(cohort.complete());
    cohort
}

#[test]
fn native_audit_content_has_a_fixed_layout_vector_without_allocation_or_capacity_identity() {
    let cohort = fixed_cohort();
    // Independently encoded 462-byte native v1 payload, checked with a reference
    // BLAKE3 single-chunk implementation (also checked against its empty vector).
    let expected = ContentHash([
        57, 54, 119, 205, 166, 148, 194, 243, 125, 135, 160, 215, 49, 199, 79, 254, 228, 171, 237,
        151, 126, 76, 45, 33, 103, 181, 38, 118, 71, 204, 139, 164,
    ]);
    bytes::fail_after(0, || {
        assert_eq!(cohort.content_fingerprint().unwrap(), expected);
        assert_eq!(bytes::remaining_allocations(), Some(0));
    });
    let mut copied = cohort.try_copy(cohort.copy_charge().unwrap()).unwrap();
    copied.members.reserve(20);
    copied.results.reserve(8);
    copied.result_capacity = 8;
    assert_ne!(
        copied.retained_bytes().unwrap(),
        cohort.retained_bytes().unwrap()
    );
    assert_eq!(copied.content_fingerprint().unwrap(), expected);
}

#[test]
fn every_retained_member_coordinate_and_control_cause_changes_native_content() {
    let mut source = fixed_cohort();
    source.members[0].fence = Some(AuthorityFence {
        reason: v::FenceReason::Evaluation,
        cause: ContentHash([11; 32]),
    });
    let original = source.content_fingerprint().unwrap();
    let result_cohort = complete_in_order([0, 1]);
    let result = result_cohort.results()[0];
    let stamp = result.definition_stamp();
    macro_rules! changed {
        ($edit:expr) => {{
            let mut value = source.try_copy(source.copy_charge().unwrap()).unwrap();
            $edit(&mut value);
            assert_ne!(value.content_fingerprint(), Ok(original));
        }};
    }
    changed!(|v: &mut AuditCohort| v.claim.ledger.tenant = TenantId::from_u128(2));
    changed!(|v: &mut AuditCohort| v.claim.ledger.session = SessionId::from_u128(2));
    changed!(|v: &mut AuditCohort| v.claim.object = ObjectId::from_u128(2));
    changed!(|v: &mut AuditCohort| v.claim.content = ContentHash([8; 32]));
    changed!(|v: &mut AuditCohort| v.claim.revision = ObjectRevision(4));
    changed!(|v: &mut AuditCohort| v.issuer = evaluator());
    changed!(|v: &mut AuditCohort| v.sequence = SessionSeq(6));
    changed!(|v: &mut AuditCohort| v.members[0].definition = stamp);
    changed!(|v: &mut AuditCohort| v.members[0].key.validation = ValidationId::from_u128(3));
    changed!(
        |v: &mut AuditCohort| v.members[0].key.target = Target::Admission { claim: binding(4) }
    );
    changed!(|v: &mut AuditCohort| v.members[0].key.generation = 2);
    changed!(|v: &mut AuditCohort| v.members[0].order.target =
        CauseTarget::Response(crate::TestamentId::from_u128(4)));
    changed!(|v: &mut AuditCohort| v.members[0].order.declaration = 3);
    changed!(|v: &mut AuditCohort| v.members[0].order.generation = 2);
    changed!(|v: &mut AuditCohort| v.members[0].order.validation = ValidationId::from_u128(3));
    changed!(|v: &mut AuditCohort| v.members[0].binding = binding(3));
    changed!(
        |v: &mut AuditCohort| v.members[0].receipt = Some(crate::ReceiptFence {
            receipt: crate::ReceiptId::from_u128(5),
            epoch: 1
        })
    );
    changed!(|v: &mut AuditCohort| v.members[0].begun = true);
    changed!(|v: &mut AuditCohort| v.members[0].state = State::Validated);
    changed!(|v: &mut AuditCohort| v.members[0].suppression = Some(Suppression::MissingTarget));
    changed!(|v: &mut AuditCohort| v.members[0].suppression =
        Some(Suppression::CohortSealed(ContentHash([8; 32]))));
    changed!(|v: &mut AuditCohort| v.members[0].fence = None);
    changed!(
        |v: &mut AuditCohort| v.members[0].fence.as_mut().unwrap().reason =
            v::FenceReason::Cancellation
    );
    changed!(
        |v: &mut AuditCohort| v.members[0].fence.as_mut().unwrap().cause = ContentHash([12; 32])
    );
    changed!(|v: &mut AuditCohort| v.members[0].last_result = Some(result));
    changed!(|v: &mut AuditCohort| v.members[0].sealed = None);
    changed!(|v: &mut AuditCohort| v.members[0].sealed = Some(ContentHash([10; 32])));
    for reason in [
        v::FenceReason::Deadline(deadline()),
        v::FenceReason::Deadline(Deadline {
            generation: 2,
            ..deadline()
        }),
        v::FenceReason::Deadline(Deadline {
            at: 101,
            ..deadline()
        }),
    ] {
        changed!(|v: &mut AuditCohort| v.members[0].fence.as_mut().unwrap().reason = reason);
    }
    changed!(|v: &mut AuditCohort| v.members.clear());
    changed!(|v: &mut AuditCohort| v.results.push(result));
}

#[test]
fn reversed_late_report_arrivals_produce_identical_complete_content_and_preserve_each_result() {
    let forward = complete_in_order([0, 1]);
    let reverse = complete_in_order([1, 0]);
    assert_eq!(forward.members(), reverse.members());
    assert_eq!(forward.results(), reverse.results());
    assert_eq!(forward.content_fingerprint(), reverse.content_fingerprint());
    assert_eq!(
        forward
            .results()
            .iter()
            .map(|r| r.validation())
            .collect::<Vec<_>>(),
        vec![ValidationId::from_u128(2), ValidationId::from_u128(3)]
    );
    let original = forward.content_fingerprint().unwrap();
    let mut omitted = forward.try_copy(forward.copy_charge().unwrap()).unwrap();
    omitted.results.remove(0);
    assert_ne!(omitted.content_fingerprint(), Ok(original));
    let mut reordered = forward.try_copy(forward.copy_charge().unwrap()).unwrap();
    reordered.results.reverse();
    assert_eq!(
        reordered.content_fingerprint(),
        Err(ContractError::InvalidManifest)
    );
    assert!(
        ResultTestament::generate_canonical(binding(90), Principal::Actor(issuer()), reordered)
            .is_err()
    );
    let mut duplicated = forward.try_copy(forward.copy_charge().unwrap()).unwrap();
    duplicated.results[1] = duplicated.results[0];
    assert_eq!(
        duplicated.content_fingerprint(),
        Err(ContractError::InvalidManifest)
    );
}

#[test]
fn canonical_bundle_consumes_without_sorting_and_preserves_frozen_content_across_posting() {
    let cohort = complete_in_order([1, 0]);
    let fingerprint = cohort.content_fingerprint().unwrap();
    for (binding, principal) in [
        (binding(90), Principal::Actor(evaluator())),
        (
            Binding {
                revision: ObjectRevision(2),
                ..binding(90)
            },
            Principal::Actor(issuer()),
        ),
        (
            Binding {
                object: ObjectId::from_u128(0),
                ..binding(90)
            },
            Principal::Actor(issuer()),
        ),
        (
            Binding {
                content: ContentHash([0; 32]),
                ..binding(90)
            },
            Principal::Actor(issuer()),
        ),
    ] {
        let copy = cohort.try_copy(cohort.copy_charge().unwrap()).unwrap();
        assert!(ResultTestament::generate_canonical(binding, principal, copy).is_err());
    }
    bytes::fail_after(0, || {
        let mut bundle =
            ResultTestament::generate_canonical(binding(90), Principal::Actor(issuer()), cohort)
                .unwrap();
        assert_eq!(bundle.issuer(), issuer());
        assert_eq!(bundle.sealed_at(), SessionSeq(5));
        assert_eq!(bundle.state(), ResultTestamentState::Generated);
        bundle
            .post(Principal::Actor(issuer()), &bundle.binding())
            .unwrap();
        assert_eq!(bundle.state(), ResultTestamentState::Posted);
        assert_eq!(bundle.cohort().content_fingerprint().unwrap(), fingerprint);
        assert_eq!(bytes::remaining_allocations(), Some(0));
    });
}
