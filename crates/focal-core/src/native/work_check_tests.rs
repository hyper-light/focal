use super::super::report_tests::{EVALUATOR, QUALITY};
use super::*;
use focal_model::{
    Deadline, HandlerRef, ObjectRevision, TimerId, ValidationKind, ValidationPhase, ValidatorId,
    VerdictValue,
};

const REQUIREMENTS: [(u32, ValidationMode); 3] = [
    (0, ValidationMode::Required),
    (0, ValidationMode::Observe),
    (1, ValidationMode::Required),
];

pub(super) fn declaration(
    index: u32,
    slot: u32,
    mode: ValidationMode,
    deadline: u64,
) -> validation::Declaration {
    let handler = HandlerRef {
        id: ValidatorId::from_u128(77),
        version: ContentHash([77; 32]),
        agentic: mode == ValidationMode::Observe,
    };
    let handlers = [validation::HandlerPolicy {
        handler: &handler,
        attempts: 2,
        proof_schema: test_report_schema(),
        diagnostic_schema: error_report_schema(),
    }];
    let policy = validation::PhasePolicy {
        evaluator: if handler.agentic { QUALITY } else { EVALUATOR },
        definition: ContentHash([78; 32]),
        handlers: &handlers,
        required_policy: None,
    };
    validation::Declaration::new(
        Principal::Actor(ISSUER),
        validation::DeclarationSpec {
            binding: binding(300 + u128::from(index)),
            claim: ClaimId::from_u128(1),
            issuer: ISSUER,
            declaration_index: index,
            kind: ValidationKind::Test,
            phase: ValidationPhase::WholeWork,
            mode,
            target: validation::TargetDeclaration::WholeWorkSlot {
                index: slot,
                name: if slot == 0 { "primary" } else { "secondary" },
            },
            program: if handler.agentic {
                validation::Program::Agentic { check: policy }
            } else {
                validation::Program::Programmatic {
                    check: policy,
                    quality: None,
                }
            },
            deadline: Deadline {
                timer: TimerId::from_u128(300 + u128::from(index)),
                generation: 1,
                at: deadline,
            },
        },
        validation::Limits {
            handlers: 4,
            attempts: 8,
            slot_bytes: 64,
        },
    )
    .unwrap()
}

fn setup(
    requirements: &[(u32, ValidationMode)],
    deadline: u64,
    evaluations: usize,
) -> (Fixture, NativeCommand) {
    let core = Core::new_native(
        binding(1).ledger,
        RangeId(2781),
        NativeLimits {
            plan_nodes: 16,
            plan_edges: 2048,
            preparation_bytes: 1024 * 1024,
            evaluations,
            evaluations_per_claim: 64,
            range: RangeConfig {
                max_batch_entries: 128,
                page_entries: 4,
                ..RangeConfig::default()
            },
            ..NativeLimits::default()
        },
        MemoryBudget::new(128 * 1024 * 1024, 16 * 1024 * 1024).unwrap(),
    )
    .unwrap();
    let directory = tempfile::tempdir().unwrap();
    let store = ContentStore::open(
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
    let fixture = Fixture {
        owner: NativeOwner::new(core).unwrap(),
        store,
        _directory: directory,
        serial: 10,
    };
    let mut input = creation(1, 1, &[], None);
    let NativeCommand::Create {
        claims,
        declarations,
    } = &mut input.command
    else {
        panic!("create")
    };
    for (offset, &(slot, mode)) in requirements.iter().enumerate() {
        declarations.push(declaration(
            u32::try_from(offset + 1).unwrap(),
            slot,
            mode,
            deadline,
        ));
    }
    let checks: Vec<Vec<aggregation::CheckPolicy>> = (0..2)
        .map(|slot| {
            declarations.iter().filter_map(|declaration| {
                matches!(declaration.target(), validation::TargetDeclaration::WholeWorkSlot { index, .. } if index == slot)
                    .then_some(aggregation::CheckPolicy {
                        declaration_index: declaration.declaration_index(),
                        validation: ValidationId(declaration.binding().object.0),
                        mode: declaration.mode(),
                    })
            }).collect()
        })
        .collect();
    let policies: Vec<_> = checks
        .iter()
        .enumerate()
        .map(|(slot, checks)| aggregation::SlotPolicy {
            slot: u32::try_from(slot).unwrap(),
            missing_declaration_index: 20 + u32::try_from(slot).unwrap(),
            mode: if checks
                .iter()
                .any(|check| check.mode == ValidationMode::Required)
            {
                ValidationMode::Required
            } else {
                ValidationMode::Observe
            },
            checks,
        })
        .collect();
    claims[0].definition.acceptance = aggregation::AcceptancePolicy::new(
        binding(1),
        ISSUER,
        &policies,
        declarations,
        aggregation::Limits {
            max_slots: 8,
            max_checks: 16,
            max_results: 64,
            max_updates: 64,
        },
    )
    .unwrap();
    (fixture, input.command)
}

fn fixture(requirements: &[(u32, ValidationMode)], deadline: u64, evaluations: usize) -> Fixture {
    let (mut f, create) = setup(requirements, deadline, evaluations);
    f.commit(ISSUER, create);
    f.commit(
        ISSUER,
        NativeCommand::Post {
            expected: f.claim(),
        },
    );
    f.commit(
        SUBJECT,
        NativeCommand::AcquireReceipt {
            expected: f.claim(),
            receipt: ReceiptId::from_u128(701),
        },
    );
    f
}

fn post(f: &mut Fixture, response: u128, manifest: Vec<SlotBinding>) {
    f.commit(
        SUBJECT,
        f.close(response, OutcomeKind::Complete, manifest, vec![]),
    );
    f.commit(
        SUBJECT,
        NativeCommand::PostResponse {
            claim: f.claim(),
            expected: f.response(response),
        },
    );
}

fn key(
    index: u32,
    response: u128,
    cycle: u64,
    slot: u32,
    artifact: Option<ArtifactId>,
) -> EvaluationKey {
    EvaluationKey {
        claim: ClaimId::from_u128(1),
        validation: ValidationId::from_u128(300 + u128::from(index)),
        generation: cycle,
        target: match artifact {
            Some(artifact) => EvaluationTarget::Work {
                response: TestamentId::from_u128(response),
                slot,
                artifact,
            },
            None => EvaluationTarget::MissingSlot {
                response: TestamentId::from_u128(response),
                slot,
            },
        },
    }
}

fn receive(f: &mut Fixture, response: u128) -> NativeOutcome {
    f.commit(
        ISSUER,
        NativeCommand::ReceiveResponse {
            claim: f.claim(),
            expected: f.response(response),
        },
    )
}

fn delivery_key(response: u128, cycle: u64) -> EvaluationKey {
    EvaluationKey {
        claim: ClaimId::from_u128(1),
        validation: ValidationId::from_u128(100),
        target: EvaluationTarget::Delivery {
            response: TestamentId::from_u128(response),
        },
        generation: cycle,
    }
}

fn assert_ready(f: &Fixture, key: EvaluationKey, target: validation::Target, mode: ValidationMode) {
    let view = f.owner.effective();
    let state = view.evaluation(key).unwrap();
    assert_eq!(state.state(), validation::State::Ready);
    assert_eq!(state.binding().revision, ObjectRevision(1));
    assert_eq!(state.target(), target);
    assert_eq!(state.generation(), key.generation);
    assert_eq!(state.receipt(), Some(f.parent().receipt));
    assert!(!state.has_begun());
    assert_eq!(state.last_result(), None);
    assert_eq!(
        state
            .bind(view.definition(key.validation).unwrap())
            .unwrap()
            .mode(),
        mode
    );
    let rows = view.registrations(key.claim).unwrap().rows();
    assert_eq!(
        rows.iter()
            .filter(|row| row.binding().object.0 == key.validation.0 && row.target() == target)
            .count(),
        1
    );
    assert!(
        view.result(NativeResultKey {
            evaluation: key,
            revision: ObjectRevision(1)
        })
        .is_none()
    );
}

#[test]
fn receipt_materializes_every_required_and_observe_check_at_exact_attached_or_missing_target() {
    let mut f = fixture(&REQUIREMENTS, 1000, 128);
    let output = f.work(801, 0);
    let generated = f
        .owner
        .effective()
        .work(output.artifact.id)
        .unwrap()
        .state
        .binding();
    f.commit(
        ISSUER,
        NativeCommand::ReceiveWork {
            claim: f.claim(),
            expected: generated,
        },
    );
    post(&mut f, 900, vec![output]);
    let attached = f.owner.effective().work(output.artifact.id).unwrap().state;
    assert!(attached.binding().revision > generated.revision);
    let read = f.owner.pin(0, 100).unwrap();
    let outcome = receive(&mut f, 900);
    assert_eq!(
        (
            outcome.evaluations,
            outcome.results,
            outcome.artifacts,
            outcome.events
        ),
        (4, 1, 0, 7)
    );
    let response = f.response(900);
    for (index, mode) in [(1, ValidationMode::Required), (2, ValidationMode::Observe)] {
        let key = key(index, 900, 1, 0, Some(output.artifact.id));
        assert_ready(
            &f,
            key,
            validation::Target::Artifact {
                response,
                slot: 0,
                artifact: attached.binding(),
            },
            mode,
        );
        assert!(
            read.with_evaluation(key, 0, |row| row.state())
                .unwrap()
                .is_none()
        );
    }
    assert_ready(
        &f,
        key(3, 900, 1, 1, None),
        validation::Target::MissingSlot { response, slot: 1 },
        ValidationMode::Required,
    );
    let view = f.owner.effective();
    assert_eq!(
        view.registrations(ClaimId::from_u128(1))
            .unwrap()
            .rows()
            .len(),
        4
    );
    assert_eq!(view.work(output.artifact.id).unwrap().state, attached);
    assert_eq!(
        view.response(TestamentId::from_u128(900)).unwrap().state(),
        ResponseState::Received
    );
    assert_eq!(
        view.claim(ClaimId::from_u128(1)).unwrap().status(),
        ClaimStatus::TestamentAcknowledged
    );
    assert!(!view.claim(ClaimId::from_u128(1)).unwrap().local_complete());
    let materialized: Vec<_> = (0..outcome.events)
        .filter_map(
            |ordinal| match view.event(outcome.sequence, ordinal).unwrap().fact {
                NativeFact::Evaluation {
                    kind: NativeEvaluationEventKind::Materialized,
                    key,
                    state,
                    attempt,
                    ..
                } => Some((key, state, attempt)),
                _ => None,
            },
        )
        .collect();
    assert_eq!(materialized.len(), 4);
    assert!(
        materialized
            .iter()
            .all(|(_, state, attempt)| *state == validation::State::Ready && attempt.is_none())
    );
    let delivery = view.evaluation(delivery_key(900, 1)).unwrap();
    let result_key = NativeResultKey {
        evaluation: delivery_key(900, 1),
        revision: delivery.binding().revision,
    };
    let result = view.delivery_result(result_key).unwrap();
    assert_eq!(
        view.event(result.sequence(), result.ordinal())
            .unwrap()
            .fact,
        NativeFact::Delivery { key: result_key }
    );
}

#[test]
fn expired_whole_work_checks_stay_ready_and_do_not_change_independent_receipt_results() {
    for (time, delivery_passed) in [(100, true), (1000, false)] {
        let mut f = fixture(&REQUIREMENTS, 20, 128);
        post(&mut f, 900, vec![]);
        f.serial = time;
        let outcome = receive(&mut f, 900);
        assert_eq!(outcome.evaluations, 4);
        assert_eq!(outcome.results, u32::from(delivery_passed));
        for (offset, &(slot, mode)) in REQUIREMENTS.iter().enumerate() {
            assert_ready(
                &f,
                key(u32::try_from(offset + 1).unwrap(), 900, 1, slot, None),
                validation::Target::MissingSlot {
                    response: f.response(900),
                    slot,
                },
                mode,
            );
        }
        let delivery_key = delivery_key(900, 1);
        let view = f.owner.effective();
        let delivery = view.evaluation(delivery_key).unwrap();
        if delivery_passed {
            let result = view
                .delivery_result(NativeResultKey {
                    evaluation: delivery_key,
                    revision: delivery.binding().revision,
                })
                .unwrap();
            assert_eq!(result.result().verdict(), VerdictValue::Pass);
            assert_eq!(result.result().attempt(), None);
            assert_eq!(result.result().evidence(), None);
        } else {
            assert_eq!(delivery.state(), validation::State::Ready);
            assert_eq!(delivery.last_result(), None);
        }
        assert_eq!(outcome.artifacts, 0);
    }
}

#[test]
fn later_responses_create_independent_cohorts_even_when_received_in_reverse_order() {
    let mut f = fixture(&REQUIREMENTS, 1000, 128);
    let first = f.work(801, 0);
    post(&mut f, 900, vec![first]);
    let second = f.work(802, 1);
    post(&mut f, 901, vec![second]);
    receive(&mut f, 901);
    let saved = *f
        .owner
        .effective()
        .evaluation(key(3, 901, 2, 1, Some(second.artifact.id)))
        .unwrap();
    receive(&mut f, 900);
    for (id, cycle, present) in [(900, 1, first), (901, 2, second)] {
        for (offset, &(slot, mode)) in REQUIREMENTS.iter().enumerate() {
            let artifact = (slot == present.slot).then_some(present.artifact.id);
            let target = match artifact {
                Some(artifact_id) => validation::Target::Artifact {
                    response: f.response(id),
                    slot,
                    artifact: f
                        .owner
                        .effective()
                        .work(artifact_id)
                        .unwrap()
                        .state
                        .binding(),
                },
                None => validation::Target::MissingSlot {
                    response: f.response(id),
                    slot,
                },
            };
            assert_ready(
                &f,
                key(
                    u32::try_from(offset + 1).unwrap(),
                    id,
                    cycle,
                    slot,
                    artifact,
                ),
                target,
                mode,
            );
        }
    }
    assert_eq!(
        *f.owner
            .effective()
            .evaluation(key(3, 901, 2, 1, Some(second.artifact.id)))
            .unwrap(),
        saved
    );
    assert_eq!(
        f.owner
            .effective()
            .registrations(ClaimId::from_u128(1))
            .unwrap()
            .rows()
            .len(),
        8
    );
}

#[test]
fn unauthorized_failed_and_discarded_receipts_leave_the_whole_cohort_atomic() {
    let mut f = fixture(&REQUIREMENTS, 1000, 128);
    post(&mut f, 900, vec![]);
    let claim = f.claim();
    let expected = f.response(900);
    let original = f.owner.budget_stats();
    assert!(
        f.stage(SUBJECT, NativeCommand::ReceiveResponse { claim, expected })
            .is_err()
    );
    assert_eq!(f.claim(), claim);
    assert_eq!(f.response(900), expected);
    assert_eq!(f.owner.budget_stats(), original);
    let input = f.input(ISSUER, NativeCommand::ReceiveResponse { claim, expected });
    let request = input.request;
    let NativeStaging::Prepared { candidate, outcome } =
        f.owner.prepare(context(ISSUER, 90), input, None).unwrap()
    else {
        panic!("fresh")
    };
    assert_eq!(outcome.evaluations, 4);
    assert_eq!(
        f.owner
            .effective()
            .registrations(ClaimId::from_u128(1))
            .unwrap()
            .rows()
            .len(),
        4
    );
    assert!(
        f.owner
            .committed()
            .registrations(ClaimId::from_u128(1))
            .unwrap()
            .rows()
            .is_empty()
    );
    let retry = || NativeInput {
        request,
        command: NativeCommand::ReceiveResponse { claim, expected },
    };
    assert_eq!(
        f.owner.prepare(context(ISSUER, 0), retry(), None).unwrap(),
        NativeStaging::Existing {
            outcome,
            candidate: Some(candidate)
        }
    );
    assert_eq!(f.owner.discard_from(candidate).unwrap(), 1);
    assert_eq!(f.claim(), claim);
    assert_eq!(f.response(900), expected);
    assert!(
        f.owner
            .effective()
            .registrations(ClaimId::from_u128(1))
            .unwrap()
            .rows()
            .is_empty()
    );
    assert_eq!(f.owner.budget_stats(), original);
    let NativeStaging::Prepared { candidate, .. } =
        f.owner.prepare(context(ISSUER, 90), retry(), None).unwrap()
    else {
        panic!("fresh")
    };
    f.owner.publish_after_durable(candidate).unwrap();
    assert!(matches!(
        f.owner.prepare(context(ISSUER, 0), retry(), None).unwrap(),
        NativeStaging::Existing {
            candidate: None,
            ..
        }
    ));

    // Delivery staging succeeds first; the remaining complete cohort cannot fit.
    // Its later refusal must not publish that partial Delivery or response receipt.
    let mut limited = fixture(&REQUIREMENTS, 1000, 3);
    post(&mut limited, 900, vec![]);
    let before = limited.owner.budget_stats();
    let claim = limited.claim();
    let expected = limited.response(900);
    assert!(
        limited
            .stage(ISSUER, NativeCommand::ReceiveResponse { claim, expected })
            .is_err()
    );
    assert_eq!(limited.claim(), claim);
    assert_eq!(limited.response(900), expected);
    assert!(
        limited
            .owner
            .effective()
            .registrations(ClaimId::from_u128(1))
            .unwrap()
            .rows()
            .is_empty()
    );
    assert!(
        limited
            .owner
            .effective()
            .evaluation(delivery_key(900, 1))
            .is_none()
    );
    assert_eq!(limited.owner.budget_stats(), before);
}

#[test]
fn failed_optional_work_keeps_diagnostics_and_materializes_a_real_missing_slot_target() {
    let requirements = [(0, ValidationMode::Required), (1, ValidationMode::Observe)];
    let mut f = fixture(&requirements, 1000, 128);
    let output = f.work(801, 0);
    let artifact = f.artifact(
        802,
        WorkRole::Diagnostic {
            reason: EvidenceFailure::Production,
        },
    );
    let diagnostic = ArtifactRef {
        id: artifact.get().unwrap().id(),
        hash: artifact.get().unwrap().content_hash(),
    };
    f.commit(
        SUBJECT,
        NativeCommand::SubmitDiagnostic {
            claim: f.claim(),
            reason: EvidenceFailure::Production,
            artifact,
        },
    );
    f.commit(
        SUBJECT,
        NativeCommand::FailWorkProduction {
            claim: f.claim(),
            slot: 1,
            diagnostic,
        },
    );
    let failed = f.owner.effective().work(diagnostic.id).unwrap().state;
    f.commit(
        SUBJECT,
        f.close(900, OutcomeKind::Partial, vec![output], vec![diagnostic]),
    );
    f.commit(
        SUBJECT,
        NativeCommand::PostResponse {
            claim: f.claim(),
            expected: f.response(900),
        },
    );
    let outcome = receive(&mut f, 900);
    assert_eq!(
        (outcome.evaluations, outcome.results, outcome.artifacts),
        (3, 1, 0)
    );
    assert_ready(
        &f,
        key(2, 900, 1, 1, None),
        validation::Target::MissingSlot {
            response: f.response(900),
            slot: 1,
        },
        ValidationMode::Observe,
    );
    let view = f.owner.effective();
    let response = view.response(TestamentId::from_u128(900)).unwrap();
    assert_eq!(response.manifest(), &[output]);
    assert_eq!(response.failed_work().len(), 1);
    assert_eq!(response.failed_work()[0].binding(), failed.binding());
    assert_eq!(response.failed_work()[0].diagnostic().artifact, diagnostic);
    assert_eq!(view.work(diagnostic.id).unwrap().state, failed);
    assert_eq!(failed.state(), WorkArtifactState::GenerationFailed);
    assert_eq!(failed.attachment(), None);
    assert!(view.diagnostic(diagnostic.id).is_some());
    assert_eq!(response.reported_outcome(), OutcomeKind::Partial);
}

#[test]
fn an_omitted_immutable_check_definition_cannot_create_a_smaller_cohort() {
    let (mut f, mut command) = setup(&REQUIREMENTS, 1000, 128);
    let NativeCommand::Create { declarations, .. } = &mut command else {
        panic!("create")
    };
    let removed = declarations.pop().unwrap();
    assert_eq!(removed.declaration_index(), 3);
    let original = f.owner.budget_stats();
    assert!(f.stage(ISSUER, command).is_err());
    assert!(f.owner.effective().claim(ClaimId::from_u128(1)).is_none());
    assert!(
        f.owner
            .effective()
            .definition(ValidationId::from_u128(100))
            .is_none()
    );
    assert!(
        f.owner
            .effective()
            .definition(ValidationId::from_u128(301))
            .is_none()
    );
    assert_eq!(f.owner.budget_stats(), original);
}
