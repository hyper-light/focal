use super::*;

fn snapshots(
    cohort: &AuditCohort,
) -> (
    AuditCohortSnapshotV1,
    Vec<AuditMemberSnapshotV1>,
    Vec<v::AcceptedResultSnapshotV1>,
) {
    (
        cohort.snapshot_v1().unwrap(),
        cohort
            .members()
            .iter()
            .map(AuditMember::snapshot_v1)
            .collect(),
        cohort
            .results()
            .iter()
            .copied()
            .map(v::AcceptedResult::snapshot_v1)
            .collect(),
    )
}

fn cohort(
    c: &ClaimState,
    registry: &RegistrationSet,
    evaluations: &[v::Evaluation<'_>],
    history: &[v::AcceptedResult],
) -> AuditCohort {
    AuditCohort::prepare_native(
        registry.audit_targets(c).unwrap(),
        evaluations,
        history,
        limits(),
        usize::MAX,
        usize::MAX,
    )
    .unwrap()
    .build()
    .unwrap()
}

#[test]
fn open_audit_restoration_preserves_reserved_late_attempts_and_every_frozen_coordinate() {
    let (declarations, c, registry, states) = fixture();
    let running = states[0].bind(&declarations[2]).unwrap();
    let ready = states[1].bind(&declarations[1]).unwrap();
    let original = cohort(&c, &registry, &[running, ready], &[]);
    let (header, members, results) = snapshots(&original);
    let definitions = [&declarations[1], &declarations[2]];
    let plan = bytes::fail_after(0, || {
        let plan = AuditCohort::prepare_hydration_v1(
            &c,
            header,
            &members,
            &results,
            &definitions,
            limits(),
            usize::MAX,
            usize::MAX,
        )
        .unwrap();
        assert_eq!(bytes::remaining_allocations(), Some(0));
        plan
    });
    let expected_heap = plan.heap_bytes();
    assert_eq!(plan.heap_allocations(), 2);
    let mut restored = plan.build().unwrap();
    assert_eq!(restored.snapshot_v1(), original.snapshot_v1());
    assert_eq!(restored.members(), original.members());
    assert_eq!(restored.results(), original.results());
    assert_eq!(restored.retained_heap_bytes().unwrap(), expected_heap);
    assert_eq!(restored.result_capacity(), 4);
    assert!(!restored.complete());
    assert_ne!(restored.members().as_ptr(), original.members().as_ptr());

    let error = report(&running, VerdictValue::Error, 20);
    let passed = report(&error.next, VerdictValue::Pass, 21);
    bytes::fail_after(0, || {
        restored.record(&error.next).unwrap();
        restored.record(&passed.next).unwrap();
        assert_eq!(bytes::remaining_allocations(), Some(0));
    });
    assert!(restored.complete());
    assert_eq!(restored.result_count(), 2);
    assert_eq!(restored.result_capacity(), 4);
    assert!(!original.complete());
    // A later complete cohort retains the original promise instead of silently
    // changing its capacity to the now-smaller immutable history length.
    let (header, members, results) = snapshots(&restored);
    let copy = AuditCohort::prepare_hydration_v1(
        &c,
        header,
        &members,
        &results,
        &definitions,
        limits(),
        usize::MAX,
        usize::MAX,
    )
    .unwrap()
    .build()
    .unwrap();
    assert_eq!(copy.result_capacity(), 4);
    assert_eq!(copy.members(), restored.members());
    assert_eq!(copy.results(), restored.results());
    assert_eq!(copy.content_fingerprint(), restored.content_fingerprint());
}

#[test]
fn native_result_testimony_restores_generated_and_posted_states_without_new_publication() {
    let (declarations, c, registry, states) = fixture();
    let running = states[0].bind(&declarations[2]).unwrap();
    let ready = states[1].bind(&declarations[1]).unwrap();
    let error = report(&running, VerdictValue::Error, 20);
    let passed = report(&error.next, VerdictValue::Pass, 21);
    let original = cohort(
        &c,
        &registry,
        &[passed.next, ready],
        &[error.result.unwrap(), passed.result.unwrap()],
    );
    assert_eq!(original.result_capacity(), 2);
    let fingerprint = original.content_fingerprint().unwrap();
    let mut testament =
        ResultTestament::generate_canonical(binding(30), Principal::Actor(issuer()), original)
            .unwrap();
    let definitions = [&declarations[1], &declarations[2]];
    for posted in [false, true] {
        if posted {
            testament
                .post(Principal::Actor(issuer()), &testament.binding())
                .unwrap();
        }
        let (_, members, results) = snapshots(testament.cohort());
        let header = testament.snapshot_v1().unwrap();
        let plan = ResultTestament::prepare_hydration_v1(
            &c,
            binding(30),
            header,
            &members,
            &results,
            &definitions,
            limits(),
            usize::MAX,
            usize::MAX,
        )
        .unwrap();
        let charge = plan.construction_charge();
        let work = plan.visits().unwrap();
        assert!(matches!(
            ResultTestament::prepare_hydration_v1(
                &c,
                binding(30),
                header,
                &members,
                &results,
                &definitions,
                limits(),
                charge - 1,
                work,
            ),
            Err(ContractError::Capacity)
        ));
        assert!(matches!(
            ResultTestament::prepare_hydration_v1(
                &c,
                binding(30),
                header,
                &members,
                &results,
                &definitions,
                limits(),
                charge,
                work - 1,
            ),
            Err(ContractError::Capacity)
        ));
        let restored = ResultTestament::prepare_hydration_v1(
            &c,
            binding(30),
            header,
            &members,
            &results,
            &definitions,
            limits(),
            charge,
            work,
        )
        .unwrap()
        .build()
        .unwrap();
        assert_eq!(restored.snapshot_v1(), testament.snapshot_v1());
        assert_eq!(restored.members(), testament.members());
        assert_eq!(restored.results(), testament.results());
        assert_eq!(restored.cohort().content_fingerprint(), Ok(fingerprint));
        assert_eq!(restored.binding().content, testament.binding().content);
        let mut wrong_revision = header;
        wrong_revision.binding.revision.0 += 1;
        assert!(matches!(
            ResultTestament::prepare_hydration_v1(
                &c,
                binding(30),
                wrong_revision,
                &members,
                &results,
                &definitions,
                limits(),
                usize::MAX,
                usize::MAX,
            ),
            Err(ContractError::StaleRevision)
        ));
    }
}

#[test]
fn hydration_refuses_corrupt_members_history_capacity_and_foreign_model_values() {
    let (declarations, c, registry, states) = fixture();
    let running = states[0].bind(&declarations[2]).unwrap();
    let ready = states[1].bind(&declarations[1]).unwrap();
    let error = report(&running, VerdictValue::Error, 20);
    let passed = report(&error.next, VerdictValue::Pass, 21);
    let original = cohort(
        &c,
        &registry,
        &[passed.next, ready],
        &[error.result.unwrap(), passed.result.unwrap()],
    );
    let (header, members, results) = snapshots(&original);
    let definitions = [&declarations[1], &declarations[2]];
    let refuse =
        |header, members: &[AuditMemberSnapshotV1], results: &[v::AcceptedResultSnapshotV1]| {
            bytes::fail_after(0, || {
                assert!(
                    AuditCohort::prepare_hydration_v1(
                        &c,
                        header,
                        members,
                        results,
                        &definitions,
                        limits(),
                        usize::MAX,
                        usize::MAX,
                    )
                    .is_err()
                );
                assert_eq!(bytes::remaining_allocations(), Some(0));
            });
        };
    for changed in [
        AuditCohortSnapshotV1 {
            members: 1,
            ..header
        },
        AuditCohortSnapshotV1 {
            results: 1,
            ..header
        },
        AuditCohortSnapshotV1 {
            result_capacity: 1,
            ..header
        },
        AuditCohortSnapshotV1 {
            result_capacity: 3,
            ..header
        },
        AuditCohortSnapshotV1 {
            result_capacity: u64::MAX,
            ..header
        },
        AuditCohortSnapshotV1 {
            sequence: SessionSeq(0),
            ..header
        },
        AuditCohortSnapshotV1 {
            sequence: SessionSeq(6),
            ..header
        },
        AuditCohortSnapshotV1 {
            issuer: evaluator(),
            ..header
        },
        AuditCohortSnapshotV1 {
            claim: binding(99),
            ..header
        },
    ] {
        refuse(changed, &members, &results);
    }
    let mut changed = members.clone();
    changed.reverse();
    refuse(header, &changed, &results);
    changed = members.clone();
    changed[1] = changed[0];
    refuse(header, &changed, &results);
    for value in [
        AuditMemberSnapshotV1 {
            declaration_index: 9,
            ..members[1]
        },
        AuditMemberSnapshotV1 {
            binding: binding(99),
            ..members[1]
        },
        AuditMemberSnapshotV1 {
            begun: false,
            ..members[1]
        },
        AuditMemberSnapshotV1 {
            state: State::Validating,
            ..members[1]
        },
        AuditMemberSnapshotV1 {
            last_result: None,
            ..members[1]
        },
        AuditMemberSnapshotV1 {
            sealed: Some(ContentHash([0; 32])),
            ..members[1]
        },
        AuditMemberSnapshotV1 {
            fence: Some(AuthorityFence {
                reason: v::FenceReason::Evaluation,
                cause: ContentHash([5; 32]),
            }),
            ..members[1]
        },
        AuditMemberSnapshotV1 {
            suppression: Some(Suppression::MissingTarget),
            ..members[1]
        },
    ] {
        changed = members.clone();
        changed[1] = value;
        refuse(header, &changed, &results);
    }
    let mut changed_results = results.clone();
    changed_results.reverse();
    refuse(header, &members, &changed_results);
    changed_results = results.clone();
    changed_results[1] = changed_results[0];
    refuse(header, &members, &changed_results);
    changed_results = results.clone();
    changed_results[0].attempt = Some(1);
    refuse(header, &members, &changed_results);
    changed_results = results.clone();
    changed_results[0].reporter = Some(issuer());
    refuse(header, &members, &changed_results);
    refuse(
        AuditCohortSnapshotV1 {
            results: 1,
            result_capacity: 1,
            ..header
        },
        &members,
        &results[1..],
    );
    assert!(
        AuditCohort::prepare_hydration_v1(
            &c,
            header,
            &members,
            &results,
            &[&declarations[2], &declarations[1]],
            limits(),
            usize::MAX,
            usize::MAX,
        )
        .is_err()
    );
}

#[test]
fn exact_work_and_byte_limits_cover_build_and_each_allocation_failure_is_retryable() {
    let (declarations, c, registry, states) = fixture();
    let evaluations = [
        states[0].bind(&declarations[2]).unwrap(),
        states[1].bind(&declarations[1]).unwrap(),
    ];
    let original = cohort(&c, &registry, &evaluations, &[]);
    let (header, members, results) = snapshots(&original);
    let definitions = [&declarations[1], &declarations[2]];
    let prepare = |max_bytes, max_visits| {
        AuditCohort::prepare_hydration_v1(
            &c,
            header,
            &members,
            &results,
            &definitions,
            limits(),
            max_bytes,
            max_visits,
        )
    };
    let plan = prepare(usize::MAX, usize::MAX).unwrap();
    let charge = plan.construction_charge();
    let work = plan.visits().unwrap();
    assert!(matches!(
        prepare(charge - 1, work),
        Err(ContractError::Capacity)
    ));
    assert!(matches!(
        prepare(charge, work - 1),
        Err(ContractError::Capacity)
    ));
    assert!(matches!(prepare(charge, 0), Err(ContractError::Capacity)));
    for failed_allocation in 0..2 {
        bytes::fail_after(failed_allocation, || {
            assert!(matches!(
                prepare(charge, work).unwrap().build(),
                Err(ContractError::Capacity)
            ));
        });
        let restored = prepare(charge, work).unwrap().build().unwrap();
        assert_eq!(restored.members(), original.members());
        assert_eq!(restored.result_capacity(), original.result_capacity());
    }
    // A ready suppressed row needs no nested declaration scan, but even its
    // scalar work is prepaid before a member is produced.
    let value = members[0];
    let work = AuditMember::hydration_visits(definitions[0], value).unwrap();
    assert!(matches!(
        AuditMember::hydrate_v1(definitions[0], value, work - 1),
        Err(ContractError::Capacity)
    ));
    assert_eq!(
        AuditMember::hydrate_v1(definitions[0], value, work).unwrap(),
        original.members()[0]
    );
}

#[test]
fn evaluator_result_artifact_restores_exact_error_and_success_evidence_roles() {
    let (declarations, _, _, states) = fixture();
    let running = states[0].bind(&declarations[2]).unwrap();
    let error = report(&running, VerdictValue::Error, 20);
    let passed = report(&error.next, VerdictValue::Pass, 21);
    for result in [error.result.unwrap(), passed.result.unwrap()] {
        let artifact = ResultArtifact::from_result(result).unwrap();
        let snapshot = artifact.snapshot_v1();
        let work = ResultArtifact::hydration_visits(&declarations[2]).unwrap();
        let restored = bytes::fail_after(0, || {
            ResultArtifact::hydrate_v1(&declarations[2], snapshot, work).unwrap()
        });
        assert_eq!(restored, artifact);
        assert_eq!(restored.snapshot_v1(), snapshot);
        assert!(matches!(
            ResultArtifact::hydrate_v1(&declarations[2], snapshot, work - 1),
            Err(ContractError::Capacity)
        ));
        for changed in [
            ResultArtifactSnapshotV1 {
                producer: issuer(),
                ..snapshot
            },
            ResultArtifactSnapshotV1 {
                artifact: ArtifactRef {
                    id: ArtifactId::from_u128(999),
                    ..snapshot.artifact
                },
                ..snapshot
            },
        ] {
            assert!(ResultArtifact::hydrate_v1(&declarations[2], changed, work).is_err());
        }
    }
}

#[test]
fn sealed_fences_and_empty_audits_restore_without_fabricating_results() {
    let (declarations, c, registry, states) = fixture();
    let running = states[0].bind(&declarations[2]).unwrap();
    let ready = states[1].bind(&declarations[1]).unwrap();
    let fenced_owner = v::OwnerState {
        authority: v::Authority {
            state: v::AuthorityState::Fenced(AuthorityFence {
                reason: v::FenceReason::ReceiptAdoption,
                cause: ContentHash([4; 32]),
            }),
            ..owner(&running).authority
        },
        ..owner(&running)
    };
    let fenced = running
        .record_fence(&running.binding(), &fenced_owner)
        .unwrap();
    let original = cohort(&c, &registry, &[fenced, ready], &[]);
    assert!(original.complete());
    let (header, members, results) = snapshots(&original);
    let restored = AuditCohort::prepare_hydration_v1(
        &c,
        header,
        &members,
        &results,
        &[&declarations[1], &declarations[2]],
        limits(),
        usize::MAX,
        usize::MAX,
    )
    .unwrap()
    .build()
    .unwrap();
    assert_eq!(restored.members(), original.members());
    assert_eq!(
        restored.content_fingerprint(),
        original.content_fingerprint()
    );
    assert!(restored.results().is_empty());

    let mut c = claim(&[receipt()]);
    let mut registry = RegistrationSet::new(&c, 1, usize::MAX).unwrap();
    fail_claim(&mut c);
    registry.seal_targets(&c).unwrap();
    let empty = cohort(&c, &registry, &[], &[]);
    let (header, members, results) = snapshots(&empty);
    let plan = AuditCohort::prepare_hydration_v1(
        &c,
        header,
        &members,
        &results,
        &[],
        limits(),
        usize::MAX,
        usize::MAX,
    )
    .unwrap();
    assert_eq!(plan.heap_bytes(), 0);
    assert_eq!(plan.heap_allocations(), 0);
    let restored = bytes::fail_after(0, || plan.build().unwrap());
    assert!(restored.complete());
    assert_eq!(restored.snapshot_v1(), empty.snapshot_v1());
    assert_eq!(restored.content_fingerprint(), empty.content_fingerprint());
}

#[test]
fn quality_history_retains_the_actual_early_programmatic_proof_across_retries() {
    let tool = HandlerRef {
        id: ValidatorId::from_u128(1),
        version: ContentHash([2; 32]),
        agentic: false,
    };
    let agent = HandlerRef {
        agentic: true,
        ..tool
    };
    let programmatic = [v::HandlerPolicy {
        handler: &tool,
        attempts: 2,
        proof_schema: ContentHash([3; 32]),
        diagnostic_schema: ContentHash([3; 32]),
    }];
    let agentic = [v::HandlerPolicy {
        handler: &agent,
        ..programmatic[0]
    }];
    let check = v::PhasePolicy {
        evaluator: evaluator(),
        definition: ContentHash([8; 32]),
        handlers: &programmatic,
        required_policy: None,
    };
    let definition = v::Declaration::new(
        Principal::Actor(issuer()),
        v::DeclarationSpec {
            binding: binding(2),
            claim: ClaimId::from_u128(1),
            issuer: issuer(),
            declaration_index: 2,
            kind: ValidationKind::Test,
            phase: ValidationPhase::Admission,
            mode: ValidationMode::Observe,
            target: v::TargetDeclaration::Admission,
            program: v::Program::Programmatic {
                check,
                quality: Some(v::PhasePolicy {
                    handlers: &agentic,
                    ..check
                }),
            },
            deadline: deadline(),
        },
        v::Limits {
            handlers: 2,
            attempts: 4,
            slot_bytes: 32,
        },
    )
    .unwrap();
    let declarations = [receipt(), definition];
    let mut c = claim(&declarations);
    let ready = evaluation(&declarations[1]);
    let mut registry = RegistrationSet::new(&c, 1, usize::MAX).unwrap();
    registry.register(&c, &ready, usize::MAX).unwrap();
    let running = ready
        .begin(
            Principal::Actor(evaluator()),
            &ready.binding(),
            &owner(&ready),
        )
        .unwrap()
        .next;
    fail_claim(&mut c);
    registry.seal_targets(&c).unwrap();
    let running = running
        .into_state()
        .seal_claim(&declarations[1], &running.binding(), &c)
        .unwrap()
        .next()
        .bind(&declarations[1])
        .unwrap();
    // Quality starts after attempt zero succeeds, before the spare
    // programmatic retry is exhausted.
    let passed = report(&running, VerdictValue::Pass, 20);
    let error = report(&passed.next, VerdictValue::Error, 21);
    let quality = report(&error.next, VerdictValue::Pass, 22);
    let original = cohort(
        &c,
        &registry,
        &[quality.next],
        &[
            passed.result.unwrap(),
            error.result.unwrap(),
            quality.result.unwrap(),
        ],
    );
    let (header, members, results) = snapshots(&original);
    let definitions = [&declarations[1]];
    let restored = AuditCohort::prepare_hydration_v1(
        &c,
        header,
        &members,
        &results,
        &definitions,
        limits(),
        usize::MAX,
        usize::MAX,
    )
    .unwrap()
    .build()
    .unwrap();
    assert_eq!(restored.members(), original.members());
    assert_eq!(restored.results(), original.results());
    assert_eq!(
        restored.content_fingerprint(),
        original.content_fingerprint()
    );

    // Each altered quality result is individually plausible. The complete
    // audit rejects swapping its inherited proof away from the actual first
    // successful programmatic attempt, even if the member's last result agrees.
    let foreign = ArtifactRef {
        id: ArtifactId::from_u128(99),
        hash: ContentHash([9; 32]),
    };
    let mut changed_results = results.clone();
    changed_results[1].programmatic_evidence = Some(foreign);
    changed_results[2].programmatic_evidence = Some(foreign);
    let mut changed_members = members.clone();
    changed_members[0].last_result = Some(changed_results[2]);
    for result in &changed_results {
        v::AcceptedResult::hydrate_v1(&declarations[1], *result, usize::MAX).unwrap();
    }
    assert!(matches!(
        AuditCohort::prepare_hydration_v1(
            &c,
            header,
            &changed_members,
            &changed_results,
            &definitions,
            limits(),
            usize::MAX,
            usize::MAX,
        ),
        Err(ContractError::InvalidManifest)
    ));
}

#[test]
fn lower_model_testimony_preserves_original_generation_revision_and_zero_content() {
    let (declarations, c, registry, states) = fixture();
    let running = states[0].bind(&declarations[2]).unwrap();
    let ready = states[1].bind(&declarations[1]).unwrap();
    let passed = report(&running, VerdictValue::Pass, 20);
    let original = cohort(
        &c,
        &registry,
        &[passed.next, ready],
        &[passed.result.unwrap()],
    );
    let (_, members, results) = snapshots(&original);
    let definitions = [&declarations[1], &declarations[2]];
    for revision in [0, 7, u64::MAX - 1] {
        let generated = Binding {
            revision: ObjectRevision(revision),
            content: ContentHash([0; 32]),
            ..binding(30)
        };
        let mut testament = ResultTestament::generate(
            generated,
            Principal::Actor(issuer()),
            original.try_copy(original.copy_charge().unwrap()).unwrap(),
        )
        .unwrap();
        for posted in [false, true] {
            if posted {
                testament
                    .post(Principal::Actor(issuer()), &testament.binding())
                    .unwrap();
            }
            let snapshot = testament.snapshot_v1().unwrap();
            let restored = ResultTestament::prepare_hydration_v1(
                &c,
                generated,
                snapshot,
                &members,
                &results,
                &definitions,
                limits(),
                usize::MAX,
                usize::MAX,
            )
            .unwrap()
            .build()
            .unwrap();
            assert_eq!(restored.snapshot_v1(), testament.snapshot_v1());
            assert_eq!(restored.members(), testament.members());
            assert_eq!(restored.results(), testament.results());
            assert_eq!(restored.binding().content, ContentHash([0; 32]));
            assert_eq!(restored.binding().revision.0, revision + u64::from(posted));
            assert!(
                ResultTestament::prepare_hydration_v1(
                    &c,
                    Binding {
                        content: ContentHash([1; 32]),
                        ..generated
                    },
                    snapshot,
                    &members,
                    &results,
                    &definitions,
                    limits(),
                    usize::MAX,
                    usize::MAX,
                )
                .is_err()
            );
        }
    }
}
