use super::super::report_tests::{EVALUATOR, QUALITY, artifact_spec};
use super::*;

#[path = "adoption_increment_tests.rs"]
mod adoption_tests;
use focal_memory::{BudgetKind, BudgetLane};
use focal_model::lifecycle::artifact_descriptor::ResultProvenance;
use focal_model::{
    Deadline, HandlerRef, TimerId, ValidationKind, ValidationPhase, ValidatorId, VerdictValue,
};

#[derive(Clone, Copy)]
enum Program {
    Direct,
    Programmatic,
    Chain,
}

fn declaration(index: u32, mode: ValidationMode, program: Program) -> validation::Declaration {
    let handler = |id, agentic| HandlerRef {
        id: ValidatorId::from_u128(id),
        version: ContentHash([id as u8; 32]),
        agentic,
    };
    let programs = [handler(31, false), handler(32, false)];
    let agents = [handler(41, true), handler(42, true)];
    let policy = |handler, attempts| validation::HandlerPolicy {
        handler,
        attempts,
        proof_schema: test_report_schema(),
        diagnostic_schema: error_report_schema(),
    };
    let program_steps = [policy(&programs[0], 2), policy(&programs[1], 1)];
    let agent_steps = [policy(&agents[0], 1), policy(&agents[1], 1)];
    let phase = |evaluator, handlers| validation::PhasePolicy {
        evaluator,
        definition: ContentHash([18; 32]),
        handlers,
        required_policy: None,
    };
    validation::Declaration::new(
        Principal::Actor(ISSUER),
        validation::DeclarationSpec {
            binding: binding(200 + u128::from(index)),
            claim: ClaimId::from_u128(1),
            issuer: ISSUER,
            declaration_index: index,
            kind: ValidationKind::Test,
            phase: ValidationPhase::Increment,
            mode,
            target: validation::TargetDeclaration::Increment,
            program: match program {
                Program::Direct => validation::Program::Agentic {
                    check: phase(QUALITY, &agent_steps[..1]),
                },
                Program::Programmatic => validation::Program::Programmatic {
                    check: phase(EVALUATOR, &program_steps[..1]),
                    quality: None,
                },
                Program::Chain => validation::Program::Programmatic {
                    check: phase(EVALUATOR, &program_steps),
                    quality: Some(phase(QUALITY, &agent_steps)),
                },
            },
            deadline: Deadline {
                timer: TimerId::from_u128(200 + u128::from(index)),
                generation: 1,
                at: 1000,
            },
        },
        validation::Limits {
            handlers: 8,
            attempts: 8,
            slot_bytes: 64,
        },
    )
    .unwrap()
}

fn with_budget(requirements: &[(ValidationMode, Program)]) -> (Fixture, MemoryBudget) {
    let budget = MemoryBudget::new(256 * 1024 * 1024, 32 * 1024 * 1024).unwrap();
    let core = Core::new_native(
        binding(1).ledger,
        RangeId(1781),
        NativeLimits {
            plan_nodes: 16,
            plan_edges: 1024,
            preparation_bytes: 1024 * 1024,
            evaluations_per_claim: 128,
            range: RangeConfig {
                max_batch_entries: 128,
                page_entries: 4,
                ..RangeConfig::default()
            },
            ..NativeLimits::default()
        },
        budget.clone(),
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
    let mut f = Fixture {
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
    for (offset, &(mode, program)) in requirements.iter().enumerate() {
        declarations.push(declaration(
            u32::try_from(offset + 1).unwrap(),
            mode,
            program,
        ));
    }
    claims[0].definition.acceptance = aggregation::AcceptancePolicy::new(
        binding(1),
        ISSUER,
        &[slot_policy(0), slot_policy(1)],
        declarations,
        aggregation::Limits {
            max_slots: 8,
            max_checks: 16,
            max_results: 128,
            max_updates: 128,
        },
    )
    .unwrap();
    f.commit(ISSUER, input.command);
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
    (f, budget)
}

fn fixture(requirements: &[(ValidationMode, Program)]) -> Fixture {
    with_budget(requirements).0
}

fn evaluation_key(f: &Fixture, index: u32, artifact: ArtifactId) -> EvaluationKey {
    let work = f.owner.effective().work(artifact).unwrap().state;
    EvaluationKey {
        claim: work.claim(),
        validation: ValidationId::from_u128(200 + u128::from(index)),
        target: EvaluationTarget::Increment { artifact },
        generation: u64::from(work.cycle()),
    }
}

fn begin(f: &Fixture, key: EvaluationKey) -> NativeCommand {
    NativeCommand::BeginIncrement {
        claim: f.claim(),
        key,
        expected: f.owner.effective().evaluation(key).unwrap().binding(),
    }
}

fn report(f: &Fixture, key: EvaluationKey, id: u128, value: VerdictValue) -> NativeCommand {
    let view = f.owner.effective();
    let state = view.evaluation(key).unwrap();
    let evaluation = state
        .bind(view.definition(key.validation).unwrap())
        .unwrap();
    let attempt = evaluation.current_attempt().unwrap();
    let mut spec = artifact_spec(id, attempt.evaluator, value);
    spec.receipt = state.receipt();
    spec.visibility = &[];
    spec.result = Some(ResultProvenance {
        claim: key.claim,
        validation: key.validation,
        target: state.target(),
        generation: state.generation(),
        attempt,
        value,
    });
    let artifact = descriptor(spec);
    NativeCommand::ReportIncrement {
        claim: f.claim(),
        key,
        expected: state.binding(),
        report: validation::Report {
            generation: state.generation(),
            attempt,
            value,
            evidence: ArtifactRef {
                id: artifact.id(),
                hash: artifact.content_hash(),
            },
        },
        artifact: NativeArtifactInput::new(artifact).unwrap(),
    }
}

fn report_actor(command: &NativeCommand) -> ParticipantId {
    let NativeCommand::ReportIncrement { report, .. } = command else {
        panic!("report")
    };
    report.attempt.evaluator
}

fn prepared(staging: NativeStaging) -> (NativeCandidate, NativeOutcome) {
    let NativeStaging::Prepared { candidate, outcome } = staging else {
        panic!("fresh")
    };
    (candidate, outcome)
}

#[test]
fn submit_work_exposes_complete_ready_increment_cohort_at_one_effective_prefix() {
    let mut f = fixture(&[
        (ValidationMode::Required, Program::Programmatic),
        (ValidationMode::Observe, Program::Direct),
    ]);
    let claim = f.claim();
    let artifact = f.artifact(801, WorkRole::Output { slot: 0 });
    let artifact_binding = artifact.get().unwrap().binding();
    let (candidate, outcome) = prepared(
        f.stage(
            SUBJECT,
            NativeCommand::SubmitWork {
                claim,
                slot: 0,
                artifact,
            },
        )
        .unwrap(),
    );
    assert_eq!(outcome.evaluations, 2);
    assert_eq!(f.claim(), claim);
    assert!(
        f.owner
            .committed()
            .work(ArtifactId::from_u128(801))
            .is_none()
    );
    for index in [1, 2] {
        let key = evaluation_key(&f, index, ArtifactId::from_u128(801));
        assert!(f.owner.committed().evaluation(key).is_none());
        let ready = f.owner.effective().evaluation(key).unwrap();
        assert_eq!(ready.state(), validation::State::Ready);
        assert!(!ready.has_begun());
        assert_eq!(ready.receipt(), Some(f.parent().receipt));
        assert_eq!(
            ready.target(),
            validation::Target::Increment {
                claim,
                artifact: artifact_binding
            }
        );
        assert_eq!(ready.generation(), 1);
    }
    let registrations = (0..outcome.events).filter(|ordinal| matches!(f.owner.effective().event(outcome.sequence, *ordinal).unwrap().fact, NativeFact::Registrations { claim: actual } if actual == claim)).count();
    assert_eq!(registrations, 1);
    f.owner.publish_after_durable(candidate).unwrap();
    let other = f.work(802, 1);
    for index in [1, 2] {
        let first = evaluation_key(&f, index, ArtifactId::from_u128(801));
        let second = evaluation_key(&f, index, other.artifact.id);
        assert_ne!(first, second);
        assert!(f.owner.committed().evaluation(first).is_some());
        assert!(f.owner.committed().evaluation(second).is_some());
    }
}

#[test]
fn required_increment_failure_is_evidence_and_does_not_terminalize_claim_or_work() {
    let mut f = fixture(&[(ValidationMode::Required, Program::Programmatic)]);
    let output = f.work(801, 0);
    let key = evaluation_key(&f, 1, output.artifact.id);
    let claim = f.claim();
    let work = f.owner.committed().work(output.artifact.id).unwrap().state;
    f.commit(EVALUATOR, begin(&f, key));
    let command = report(&f, key, 901, VerdictValue::Fail);
    let NativeCommand::ReportIncrement {
        report: expected, ..
    } = &command
    else {
        panic!("report")
    };
    let expected = *expected;
    let outcome = f.commit(EVALUATOR, command);
    assert_eq!(outcome.changed, 0);
    assert_eq!(outcome.events, 3);
    assert_eq!(f.claim(), claim);
    assert_eq!(
        f.owner.committed().work(output.artifact.id).unwrap().state,
        work
    );
    let state = f.owner.committed().evaluation(key).unwrap();
    assert_eq!(state.state(), validation::State::ValidationFailed);
    let accepted = f
        .owner
        .committed()
        .result(NativeResultKey {
            evaluation: key,
            revision: state.binding().revision,
        })
        .unwrap();
    assert_eq!(accepted.result().verdict(), VerdictValue::Fail);
    assert_eq!(accepted.attempt(), expected.attempt);
    assert_eq!(accepted.ordinal(), 2);
    assert_eq!(accepted.sequence(), outcome.sequence);
    assert_eq!(accepted.result().receipt(), Some(work.receipt()));
    assert!(
        f.owner
            .committed()
            .response(TestamentId::from_u128(900))
            .is_none()
    );
}

#[test]
fn complete_retry_fallback_and_quality_chain_uses_funded_capacity_under_full_parent_pressure() {
    let (mut f, budget) = with_budget(&[(ValidationMode::Required, Program::Chain)]);
    let output = f.work(801, 0);
    let key = evaluation_key(&f, 1, output.artifact.id);
    let parent = f.claim();
    f.commit(EVALUATOR, begin(&f, key));
    let pin = f.owner.pin(0, 100).unwrap();
    let pressure = budget
        .reserve(
            BudgetKind::Payload,
            BudgetLane::Completion,
            budget.limit() - budget.stats().used,
        )
        .unwrap()
        .commit();
    let mut results = Vec::new();
    for (index, value) in [
        VerdictValue::Error,
        VerdictValue::Error,
        VerdictValue::Pass,
        VerdictValue::Error,
        VerdictValue::Pass,
    ]
    .into_iter()
    .enumerate()
    {
        let command = report(&f, key, 901 + index as u128, value);
        let NativeCommand::ReportIncrement {
            report: expected, ..
        } = &command
        else {
            panic!("report")
        };
        assert_eq!(expected.attempt.index, index as u32);
        let expected_handler = [31, 31, 32, 41, 42][index];
        assert_eq!(
            expected.attempt.handler,
            ValidatorId::from_u128(expected_handler)
        );
        let actor = report_actor(&command);
        let outcome = f.commit(actor, command);
        assert_eq!(outcome.events, 3);
        assert_eq!(outcome.changed, 0);
        let state = f.owner.committed().evaluation(key).unwrap();
        let result_key = NativeResultKey {
            evaluation: key,
            revision: state.binding().revision,
        };
        results.push(result_key);
        assert_eq!(
            f.owner
                .committed()
                .result(result_key)
                .unwrap()
                .result()
                .verdict(),
            value
        );
        assert_eq!(f.claim(), parent);
    }
    assert_eq!(
        f.owner.committed().evaluation(key).unwrap().state(),
        validation::State::Validated
    );
    for result in results {
        assert!(f.owner.committed().result(result).is_some());
    }
    assert_eq!(
        pin.with_evaluation(key, 0, |state| state.state()).unwrap(),
        Some(validation::State::Validating)
    );
    drop(pressure);
    f.owner.release(&pin).unwrap();
}

#[test]
fn direct_agentic_increment_starts_quality_without_synthetic_programmatic_proof() {
    let mut f = fixture(&[(ValidationMode::Observe, Program::Direct)]);
    let output = f.work(801, 0);
    let key = evaluation_key(&f, 1, output.artifact.id);
    assert!(f.stage(EVALUATOR, begin(&f, key)).is_err());
    f.commit(QUALITY, begin(&f, key));
    assert_eq!(
        f.owner.committed().evaluation(key).unwrap().state(),
        validation::State::ValidatingQualityBar
    );
    f.commit(QUALITY, report(&f, key, 901, VerdictValue::Pass));
    let state = f.owner.committed().evaluation(key).unwrap();
    assert_eq!(state.state(), validation::State::Validated);
    let result = f
        .owner
        .committed()
        .result(NativeResultKey {
            evaluation: key,
            revision: state.binding().revision,
        })
        .unwrap();
    assert_eq!(result.result().programmatic_evidence(), None);
    assert_eq!(result.attempt().evaluator, QUALITY);
}

#[test]
fn begun_report_after_response_close_post_and_receipt_keeps_original_target_cycle_and_receipt() {
    let mut f = fixture(&[(ValidationMode::Required, Program::Programmatic)]);
    let output = f.work(801, 0);
    let key = evaluation_key(&f, 1, output.artifact.id);
    f.commit(EVALUATOR, begin(&f, key));
    let original = *f.owner.committed().evaluation(key).unwrap();
    f.commit(
        SUBJECT,
        f.close(900, OutcomeKind::Complete, vec![output], vec![]),
    );
    f.commit(
        SUBJECT,
        NativeCommand::PostResponse {
            claim: f.claim(),
            expected: f.response(900),
        },
    );
    f.commit(
        ISSUER,
        NativeCommand::ReceiveResponse {
            claim: f.claim(),
            expected: f.response(900),
        },
    );
    let claim = f.claim();
    let work = f.owner.committed().work(output.artifact.id).unwrap().state;
    assert_eq!(work.state(), WorkArtifactState::Attached);
    assert_eq!(f.parent().next_cycle, 2);
    f.commit(EVALUATOR, report(&f, key, 901, VerdictValue::Pass));
    let accepted = f.owner.committed().evaluation(key).unwrap();
    assert_eq!(accepted.target(), original.target());
    assert_eq!(accepted.receipt(), original.receipt());
    assert_eq!(accepted.generation(), original.generation());
    assert_eq!(f.claim(), claim);
    assert_eq!(
        f.owner.committed().work(output.artifact.id).unwrap().state,
        work
    );
    let later = f.work(802, 0);
    let next = evaluation_key(&f, 1, later.artifact.id);
    assert_eq!(next.generation, 2);
    assert_ne!(key, next);
    assert_eq!(
        f.owner.committed().evaluation(next).unwrap().state(),
        validation::State::Ready
    );
    assert_eq!(
        f.owner.committed().evaluation(key).unwrap().state(),
        validation::State::Validated
    );
}

#[test]
fn increment_refuses_other_command_family_actor_attempt_receipt_and_result_provenance() {
    let mut f = fixture(&[(ValidationMode::Required, Program::Programmatic)]);
    let output = f.work(801, 0);
    let key = evaluation_key(&f, 1, output.artifact.id);
    let ready = *f.owner.committed().evaluation(key).unwrap();
    assert!(f.stage(SUBJECT, begin(&f, key)).is_err());
    assert!(
        f.stage(
            EVALUATOR,
            NativeCommand::BeginAdmission {
                claim: f.claim(),
                key,
                expected: ready.binding()
            }
        )
        .is_err()
    );
    let mut wrong = begin(&f, key);
    let NativeCommand::BeginIncrement { key, .. } = &mut wrong else {
        panic!("begin")
    };
    key.generation += 1;
    assert!(f.stage(EVALUATOR, wrong).is_err());
    let key = evaluation_key(&f, 1, output.artifact.id);
    f.commit(EVALUATOR, begin(&f, key));
    let running = *f.owner.committed().evaluation(key).unwrap();
    let prefix = f.owner.committed().sequence();
    for scenario in 0..9 {
        let mut command = report(&f, key, 901, VerdictValue::Pass);
        let mut actor = EVALUATOR;
        let NativeCommand::ReportIncrement {
            expected,
            report,
            artifact,
            ..
        } = &mut command
        else {
            panic!("report")
        };
        match scenario {
            0 => actor = SUBJECT,
            1 => report.generation += 1,
            2 => report.attempt.index += 1,
            3 => *expected = expected.next().unwrap(),
            4 => report.evidence.hash = ContentHash([66; 32]),
            5..=8 => {
                let mut spec = artifact_spec(901, EVALUATOR, VerdictValue::Pass);
                spec.visibility = &[];
                spec.receipt = running.receipt();
                spec.result = artifact.get().unwrap().result_provenance();
                match scenario {
                    5 => {
                        spec.producer = SUBJECT;
                        spec.result.as_mut().unwrap().attempt.evaluator = SUBJECT;
                    }
                    6 => spec.receipt.as_mut().unwrap().epoch += 1,
                    7 => {
                        spec.result.as_mut().unwrap().target = validation::Target::Increment {
                            claim: f.claim(),
                            artifact: binding(999),
                        }
                    }
                    8 => spec.result.as_mut().unwrap().generation += 1,
                    _ => unreachable!(),
                }
                let changed = descriptor(spec);
                report.evidence = ArtifactRef {
                    id: changed.id(),
                    hash: changed.content_hash(),
                };
                *artifact = NativeArtifactInput::new(changed).unwrap();
            }
            _ => unreachable!(),
        }
        assert!(f.stage(actor, command).is_err(), "scenario {scenario}");
        assert_eq!(f.owner.effective().sequence(), prefix);
        assert_eq!(f.owner.effective().evaluation(key).unwrap(), &running);
        assert!(
            f.owner
                .effective()
                .artifact(ArtifactId::from_u128(901))
                .is_none()
        );
        assert_eq!(f.owner.pending_len(), 0);
    }
    let NativeCommand::ReportIncrement {
        claim,
        key,
        expected,
        report: result,
        artifact,
    } = report(&f, key, 901, VerdictValue::Pass)
    else {
        panic!("report")
    };
    assert!(
        f.stage(
            EVALUATOR,
            NativeCommand::ReportAdmission {
                claim,
                key,
                expected,
                report: result,
                artifact
            }
        )
        .is_err()
    );
    f.commit(EVALUATOR, report(&f, key, 901, VerdictValue::Pass));
}

#[test]
fn cancellation_fences_all_begun_increment_grants_and_ready_targets_without_new_results() {
    let mut f = fixture(&[
        (ValidationMode::Required, Program::Programmatic),
        (ValidationMode::Observe, Program::Direct),
    ]);
    let output = f.work(801, 0);
    let first = evaluation_key(&f, 1, output.artifact.id);
    let second = evaluation_key(&f, 2, output.artifact.id);
    f.commit(EVALUATOR, begin(&f, first));
    let command = report(&f, first, 901, VerdictValue::Pass);
    let work = f.owner.committed().work(output.artifact.id).unwrap().state;
    let (_, outcome) = prepared(
        f.stage(
            ISSUER,
            NativeCommand::Cancel {
                expected: f.claim(),
            },
        )
        .unwrap(),
    );
    assert!(f.stage(EVALUATOR, command).is_err());
    assert!(f.stage(QUALITY, begin(&f, second)).is_err());
    for key in [first, second] {
        assert!(
            f.owner
                .effective()
                .evaluation(key)
                .unwrap()
                .fence()
                .is_some()
        );
        assert!(
            f.owner
                .committed()
                .evaluation(key)
                .unwrap()
                .fence()
                .is_none()
        );
    }
    assert!(
        f.owner
            .effective()
            .artifact(ArtifactId::from_u128(901))
            .is_none()
    );
    assert_eq!(outcome.artifacts, 0);
    assert_eq!(outcome.results, 0);
    assert_eq!(
        f.owner.effective().work(output.artifact.id).unwrap().state,
        work
    );
    assert_eq!(f.owner.discard_all(), 1);
    f.commit(EVALUATOR, report(&f, first, 901, VerdictValue::Pass));
    assert_eq!(
        f.owner.committed().evaluation(first).unwrap().state(),
        validation::State::Validated
    );
}

#[test]
fn pending_work_begin_and_report_discard_restore_full_registry_and_reserved_credit() {
    let mut f = fixture(&[(ValidationMode::Required, Program::Programmatic)]);
    let parent = f.claim();
    let base = f.owner.budget_stats();
    let registry = f
        .owner
        .committed()
        .registrations(ClaimId::from_u128(1))
        .unwrap()
        .rows()
        .len();
    let artifact = f.artifact(801, WorkRole::Output { slot: 0 });
    let (work, work_outcome) = prepared(
        f.stage(
            SUBJECT,
            NativeCommand::SubmitWork {
                claim: parent,
                slot: 0,
                artifact,
            },
        )
        .unwrap(),
    );
    let key = evaluation_key(&f, 1, ArtifactId::from_u128(801));
    let before_begin = f.owner.budget_stats();
    let (beginning, _) = prepared(f.stage(EVALUATOR, begin(&f, key)).unwrap());
    let (_, report_outcome) = prepared(
        f.stage(EVALUATOR, report(&f, key, 901, VerdictValue::Error))
            .unwrap(),
    );
    assert_eq!(
        f.owner.effective().evaluation(key).unwrap().state(),
        validation::State::Validating
    );
    assert!(f.owner.committed().evaluation(key).is_none());
    assert_eq!(f.owner.discard_from(beginning).unwrap(), 2);
    assert_eq!(f.owner.budget_stats(), before_begin);
    assert_eq!(
        f.owner.effective().evaluation(key).unwrap().state(),
        validation::State::Ready
    );
    assert!(
        f.owner
            .effective()
            .artifact(ArtifactId::from_u128(901))
            .is_none()
    );
    assert!(
        f.owner
            .effective()
            .recorded(report_outcome.invocation)
            .is_none()
    );
    assert_eq!(f.owner.discard_from(work).unwrap(), 1);
    assert_eq!(f.owner.budget_stats(), base);
    assert_eq!(f.claim(), parent);
    assert!(f.owner.effective().evaluation(key).is_none());
    assert!(
        f.owner
            .effective()
            .work(ArtifactId::from_u128(801))
            .is_none()
    );
    assert!(
        f.owner
            .effective()
            .recorded(work_outcome.invocation)
            .is_none()
    );
    assert_eq!(
        f.owner
            .effective()
            .registrations(ClaimId::from_u128(1))
            .unwrap()
            .rows()
            .len(),
        registry
    );
    let output = f.work(801, 0);
    let restored = evaluation_key(&f, 1, output.artifact.id);
    f.commit(EVALUATOR, begin(&f, restored));
    f.commit(EVALUATOR, report(&f, restored, 901, VerdictValue::Pass));
}

#[test]
fn exact_pending_and_committed_result_retry_needs_no_second_custody_or_completion_credit() {
    let mut f = fixture(&[(ValidationMode::Observe, Program::Direct)]);
    let output = f.work(801, 0);
    let key = evaluation_key(&f, 1, output.artifact.id);
    f.commit(QUALITY, begin(&f, key));
    let input = f.input(QUALITY, report(&f, key, 901, VerdictValue::Pass));
    let copy = |input: &NativeInput| {
        let NativeCommand::ReportIncrement {
            claim,
            key,
            expected,
            report,
            artifact,
        } = &input.command
        else {
            panic!("report")
        };
        NativeInput {
            request: input.request,
            command: NativeCommand::ReportIncrement {
                claim: *claim,
                key: *key,
                expected: *expected,
                report: *report,
                artifact: artifact.copy().unwrap(),
            },
        }
    };
    let retry = copy(&input);
    let second_retry = copy(&input);
    let (candidate, outcome) = prepared(
        f.owner
            .prepare_with_custody(
                context(QUALITY, 100),
                input,
                &mut f.store,
                ContentDomainId::from_u128(93),
                &BuiltinNativeSchemas,
            )
            .unwrap(),
    );
    let budget = f.owner.budget_stats();
    assert_eq!(
        f.owner
            .prepare(context(QUALITY, 2000), retry, None)
            .unwrap(),
        NativeStaging::Existing {
            outcome,
            candidate: Some(candidate)
        }
    );
    assert_eq!(f.owner.budget_stats(), budget);
    f.owner.publish_after_durable(candidate).unwrap();
    assert_eq!(
        f.owner
            .prepare(context(QUALITY, 2001), second_retry, None)
            .unwrap(),
        NativeStaging::Existing {
            outcome,
            candidate: None
        }
    );
}

#[test]
fn claimant_seals_increment_targets_without_revoking_held_grants_or_closing_response() {
    let mut f = fixture(&[(ValidationMode::Required, Program::Programmatic)]);
    let output = f.work(801, 0);
    let key = evaluation_key(&f, 1, output.artifact.id);
    f.commit(EVALUATOR, begin(&f, key));
    let parent = f.claim();
    let command = || NativeCommand::SealIncrementTargets { claim: parent };
    assert!(f.stage(SUBJECT, command()).is_err());
    let input = f.input(ISSUER, command());
    let request_key = input.request;
    let baseline = f.owner.budget_stats();
    let (candidate, outcome) =
        prepared(f.owner.prepare(context(ISSUER, 100), input, None).unwrap());
    assert_eq!(f.claim(), parent);
    assert!(
        f.owner
            .effective()
            .registrations(key.claim)
            .unwrap()
            .increment_targets_sealed()
    );
    assert!(
        !f.owner
            .committed()
            .registrations(key.claim)
            .unwrap()
            .increment_targets_sealed()
    );
    assert_eq!(
        f.owner
            .prepare(
                context(ISSUER, 101),
                NativeInput {
                    request: request_key,
                    command: command()
                },
                None
            )
            .unwrap(),
        NativeStaging::Existing {
            outcome,
            candidate: Some(candidate)
        }
    );
    assert_eq!(f.owner.discard_from(candidate).unwrap(), 1);
    assert_eq!(f.owner.budget_stats(), baseline);
    assert!(
        !f.owner
            .effective()
            .registrations(key.claim)
            .unwrap()
            .increment_targets_sealed()
    );
    let (candidate, outcome) = prepared(
        f.owner
            .prepare(
                context(ISSUER, 100),
                NativeInput {
                    request: request_key,
                    command: command(),
                },
                None,
            )
            .unwrap(),
    );
    f.owner.publish_after_durable(candidate).unwrap();
    assert_eq!(
        f.owner
            .prepare(
                context(ISSUER, 101),
                NativeInput {
                    request: request_key,
                    command: command()
                },
                None
            )
            .unwrap(),
        NativeStaging::Existing {
            outcome,
            candidate: None
        }
    );
    f.serial = 102;
    f.commit(ISSUER, command());
    let artifact = f.artifact(802, WorkRole::Output { slot: 1 });
    assert!(
        f.stage(
            SUBJECT,
            NativeCommand::SubmitWork {
                claim: f.claim(),
                slot: 1,
                artifact
            }
        )
        .is_err()
    );
    assert!(
        f.owner
            .effective()
            .work(ArtifactId::from_u128(802))
            .is_none()
    );
    let diagnostic = f.diagnostic(803);
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
    f.commit(
        ISSUER,
        NativeCommand::ReceiveResponse {
            claim: f.claim(),
            expected: f.response(900),
        },
    );
    f.commit(EVALUATOR, report(&f, key, 901, VerdictValue::Pass));
    assert_eq!(
        f.owner.committed().evaluation(key).unwrap().state(),
        validation::State::Validated
    );
    assert!(
        f.owner
            .committed()
            .registrations(key.claim)
            .unwrap()
            .increment_targets_sealed()
    );
}

#[test]
fn descriptor_rejects_an_internally_conflicting_result_producer_before_native_admission() {
    use focal_model::lifecycle::artifact_descriptor::{ArtifactDescriptor, Limits};
    let mut f = fixture(&[(ValidationMode::Required, Program::Programmatic)]);
    let output = f.work(801, 0);
    let key = evaluation_key(&f, 1, output.artifact.id);
    f.commit(EVALUATOR, begin(&f, key));
    let NativeCommand::ReportIncrement { artifact, .. } = report(&f, key, 901, VerdictValue::Pass)
    else {
        panic!("report")
    };
    let mut spec = artifact_spec(901, SUBJECT, VerdictValue::Pass);
    spec.receipt = artifact.get().unwrap().receipt();
    spec.result = artifact.get().unwrap().result_provenance();
    assert!(matches!(
        ArtifactDescriptor::prepare(
            spec,
            Limits {
                kind_bytes: 128,
                metadata_bytes: 1024,
                inline_bytes: 65536,
                inputs: 16,
                visibility_labels: 16,
                visibility_label_bytes: 128,
                construction_bytes: 128 * 1024,
            }
        ),
        Err(ContractError::WrongActor)
    ));
}

#[test]
fn inherited_visibility_checks_exact_descriptor_heap_count_label_and_visit_boundaries() {
    use super::super::increment_authority::check_completion_visibility;
    use focal_model::lifecycle::artifact_descriptor::{ArtifactDescriptor, Limits};
    let labels = ["alpha", "restricted"];
    let mut spec = artifact_spec(801, EVALUATOR, VerdictValue::Pass);
    spec.visibility = &labels;
    let source = descriptor(spec);
    let minimum = size_of::<ArtifactDescriptor>()
        + "error".len()
        + labels.len() * size_of::<String>()
        + labels.iter().map(|label| label.len()).sum::<usize>();
    let exact = Limits {
        kind_bytes: 5,
        metadata_bytes: 0,
        inline_bytes: 0,
        inputs: 0,
        visibility_labels: 2,
        visibility_label_bytes: "restricted".len(),
        construction_bytes: minimum,
    };
    check_completion_visibility(&source, exact, 2).unwrap();
    assert!(check_completion_visibility(&source, exact, 1).is_err());
    for changed in [
        Limits {
            kind_bytes: 4,
            ..exact
        },
        Limits {
            visibility_labels: 1,
            ..exact
        },
        Limits {
            visibility_label_bytes: "restricted".len() - 1,
            ..exact
        },
        Limits {
            construction_bytes: minimum - 1,
            ..exact
        },
    ] {
        assert!(check_completion_visibility(&source, changed, 2).is_err());
    }
    spec.visibility = &[];
    let unlabelled = descriptor(spec);
    check_completion_visibility(
        &unlabelled,
        Limits {
            visibility_labels: 0,
            visibility_label_bytes: 0,
            construction_bytes: size_of::<ArtifactDescriptor>() + "error".len(),
            ..exact
        },
        0,
    )
    .unwrap();
}

#[test]
fn submission_refuses_unreportable_visibility_before_custody_or_required_evaluation_creation() {
    use crate::native::fixtures::SyncCell as Cell;
    use focal_evidence::{BuiltinSchemaError, NativeSchemaVerifier};
    use focal_model::lifecycle::artifact_descriptor::{ArtifactDescriptor, Limits};

    struct Schemas {
        quotes: Cell<usize>,
        verifications: Cell<usize>,
    }
    impl NativeSchemaVerifier for Schemas {
        fn maximum_bytes(&self, schema: ContentHash) -> Result<usize, BuiltinSchemaError> {
            self.quotes.set(self.quotes.get() + 1);
            BuiltinNativeSchemas.maximum_bytes(schema)
        }
        fn verify(&self, schema: ContentHash, bytes: &[u8]) -> Result<(), BuiltinSchemaError> {
            self.verifications.set(self.verifications.get() + 1);
            BuiltinNativeSchemas.verify(schema, bytes)
        }
    }

    let mut f = fixture(&[(ValidationMode::Required, Program::Programmatic)]);
    let owned: Vec<_> = (0..1025)
        .map(|index| format!("restricted-{index:04}"))
        .collect();
    let labels: Vec<_> = owned.iter().map(String::as_str).collect();
    let parent = f.parent();
    let mut spec = artifact_spec(801, SUBJECT, VerdictValue::Pass);
    spec.receipt = Some(parent.receipt);
    spec.visibility = &labels;
    spec.work = Some(WorkProvenance {
        claim: parent.claim,
        cycle: parent.next_cycle,
        role: WorkRole::Output { slot: 0 },
    });
    let artifact = NativeArtifactInput::new(
        ArtifactDescriptor::prepare(
            spec,
            Limits {
                kind_bytes: 128,
                metadata_bytes: 1024,
                inline_bytes: 65536,
                inputs: 16,
                visibility_labels: 1025,
                visibility_label_bytes: 128,
                construction_bytes: 128 * 1024,
            },
        )
        .unwrap()
        .build()
        .unwrap(),
    )
    .unwrap();
    let before = f.owner.budget_stats();
    let prefix = f.owner.committed().sequence();
    let registrations = f
        .owner
        .committed()
        .registrations(parent.claim)
        .unwrap()
        .rows()
        .len();
    let input = f.input(
        SUBJECT,
        NativeCommand::SubmitWork {
            claim: f.claim(),
            slot: 0,
            artifact,
        },
    );
    let request_key = input.request;
    let schemas = Schemas {
        quotes: Cell::new(0),
        verifications: Cell::new(0),
    };
    assert!(
        f.owner
            .prepare_with_custody(
                context(SUBJECT, f.serial as u64),
                input,
                &mut f.store,
                ContentDomainId::from_u128(93),
                &schemas,
            )
            .is_err()
    );
    assert_eq!(schemas.quotes.get(), 0);
    assert_eq!(schemas.verifications.get(), 0);
    assert_eq!(f.owner.budget_stats(), before);
    assert_eq!(f.owner.effective().sequence(), prefix);
    assert_eq!(f.owner.pending_len(), 0);
    assert!(
        f.owner
            .effective()
            .artifact(ArtifactId::from_u128(801))
            .is_none()
    );
    assert!(
        f.owner
            .effective()
            .work(ArtifactId::from_u128(801))
            .is_none()
    );
    assert!(f.owner.effective().recorded(request_key).is_none());
    assert!(
        f.owner
            .effective()
            .evaluation(EvaluationKey {
                claim: parent.claim,
                validation: ValidationId::from_u128(201),
                target: EvaluationTarget::Increment {
                    artifact: ArtifactId::from_u128(801)
                },
                generation: 1,
            })
            .is_none()
    );
    assert_eq!(
        f.owner
            .effective()
            .registrations(parent.claim)
            .unwrap()
            .rows()
            .len(),
        registrations
    );
    // Refusing this output does not strand the current respondent's failure report.
    let diagnostic = f.diagnostic(802);
    f.commit(
        SUBJECT,
        f.close(900, OutcomeKind::Failed, vec![], vec![diagnostic]),
    );
    let response = f
        .owner
        .committed()
        .response(TestamentId::from_u128(900))
        .unwrap();
    assert!(response.manifest().is_empty());
    assert_eq!(response.diagnostics()[0].artifact(), diagnostic);
}

#[test]
fn observe_only_output_can_publish_but_unreportable_begin_acquires_no_responsibility() {
    use focal_model::lifecycle::artifact_descriptor::{ArtifactDescriptor, Limits};
    let mut f = fixture(&[(ValidationMode::Observe, Program::Programmatic)]);
    let owned: Vec<_> = (0..1025)
        .map(|index| format!("restricted-{index:04}"))
        .collect();
    let labels: Vec<_> = owned.iter().map(String::as_str).collect();
    let parent = f.parent();
    let mut spec = artifact_spec(801, SUBJECT, VerdictValue::Pass);
    spec.receipt = Some(parent.receipt);
    spec.visibility = &labels;
    spec.work = Some(WorkProvenance {
        claim: parent.claim,
        cycle: parent.next_cycle,
        role: WorkRole::Output { slot: 0 },
    });
    let descriptor = ArtifactDescriptor::prepare(
        spec,
        Limits {
            kind_bytes: 128,
            metadata_bytes: 1024,
            inline_bytes: 65536,
            inputs: 16,
            visibility_labels: 1025,
            visibility_label_bytes: 128,
            construction_bytes: 128 * 1024,
        },
    )
    .unwrap()
    .build()
    .unwrap();
    let source = ArtifactRef {
        id: descriptor.id(),
        hash: descriptor.content_hash(),
    };
    f.commit(
        SUBJECT,
        NativeCommand::SubmitWork {
            claim: f.claim(),
            slot: 0,
            artifact: NativeArtifactInput::new(descriptor).unwrap(),
        },
    );
    let key = evaluation_key(&f, 1, source.id);
    let before = f.owner.budget_stats();
    let prefix = f.owner.committed().sequence();
    assert!(f.stage(EVALUATOR, begin(&f, key)).is_err());
    assert_eq!(f.owner.budget_stats(), before);
    assert_eq!(f.owner.effective().sequence(), prefix);
    assert_eq!(f.owner.pending_len(), 0);
    let evaluation = f.owner.effective().evaluation(key).unwrap();
    assert_eq!(evaluation.state(), validation::State::Ready);
    assert!(!evaluation.has_begun());
    f.commit(
        SUBJECT,
        f.close(
            900,
            OutcomeKind::Complete,
            vec![SlotBinding {
                slot: 0,
                artifact: source,
            }],
            vec![],
        ),
    );
    assert_eq!(
        f.owner
            .committed()
            .response(TestamentId::from_u128(900))
            .unwrap()
            .manifest(),
        &[SlotBinding {
            slot: 0,
            artifact: source
        }]
    );
}

#[test]
fn reconstruction_refunds_refusal_then_funds_live_increment_after_received_response() {
    use super::super::report_tests::{prepared as core_prepared, publish};
    let budget = MemoryBudget::new(256 * 1024 * 1024, 32 * 1024 * 1024).unwrap();
    let mut core = Core::new_native(
        binding(1).ledger,
        RangeId(2781),
        NativeLimits {
            plan_nodes: 16,
            plan_edges: 1024,
            preparation_bytes: 1024 * 1024,
            evaluations_per_claim: 128,
            range: RangeConfig {
                page_entries: 4,
                max_batch_entries: 128,
                ..RangeConfig::default()
            },
            ..NativeLimits::default()
        },
        budget.clone(),
    )
    .unwrap();
    let mut input = creation(1, 1, &[], None);
    let NativeCommand::Create {
        claims,
        declarations,
    } = &mut input.command
    else {
        panic!("create")
    };
    declarations.push(declaration(
        1,
        ValidationMode::Required,
        Program::Programmatic,
    ));
    claims[0].definition.acceptance = aggregation::AcceptancePolicy::new(
        binding(1),
        ISSUER,
        &[slot_policy(0)],
        declarations,
        aggregation::Limits {
            max_slots: 8,
            max_checks: 16,
            max_results: 128,
            max_updates: 128,
        },
    )
    .unwrap();
    publish(&mut core, 10, input);
    let claim = core.native_claim(ClaimId::from_u128(1)).unwrap().binding();
    publish(
        &mut core,
        20,
        NativeInput {
            request: request(ISSUER, 2),
            command: NativeCommand::Post { expected: claim },
        },
    );
    let claim = core.native_claim(ClaimId::from_u128(1)).unwrap().binding();
    publish(
        &mut core,
        30,
        NativeInput {
            request: request(SUBJECT, 3),
            command: NativeCommand::AcquireReceipt {
                expected: claim,
                receipt: ReceiptId::from_u128(701),
            },
        },
    );
    let parent =
        evidence::Parent::from_claim(core.native_claim(ClaimId::from_u128(1)).unwrap()).unwrap();
    let mut spec = artifact_spec(801, SUBJECT, VerdictValue::Pass);
    spec.visibility = &[];
    spec.receipt = Some(parent.receipt);
    spec.work = Some(WorkProvenance {
        claim: parent.claim,
        cycle: parent.next_cycle,
        role: WorkRole::Output { slot: 0 },
    });
    let artifact = descriptor(spec);
    let source = ArtifactRef {
        id: artifact.id(),
        hash: artifact.content_hash(),
    };
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
    let input = NativeInput {
        request: request(SUBJECT, 4),
        command: NativeCommand::SubmitWork {
            claim: core.native_claim(parent.claim).unwrap().binding(),
            slot: 0,
            artifact: NativeArtifactInput::new(artifact).unwrap(),
        },
    };
    let NativeCommand::SubmitWork { artifact, .. } = &input.command else {
        panic!("work")
    };
    let custody = store
        .verify_native_artifact(
            input.request,
            artifact.get().unwrap(),
            ContentDomainId::from_u128(93),
            &budget,
            &BuiltinNativeSchemas,
        )
        .unwrap();
    let next = core_prepared(core.prepare_native_evidenced(
        context(SUBJECT, 40),
        input,
        &[],
        Some(&custody),
    ));
    core.publish_native(next).unwrap();
    drop(custody);
    let key = EvaluationKey {
        claim: parent.claim,
        validation: ValidationId::from_u128(201),
        target: EvaluationTarget::Increment {
            artifact: source.id,
        },
        generation: 1,
    };
    let expected = core.native_evaluation(key).unwrap().binding();
    let claim = core.native_claim(parent.claim).unwrap().binding();
    publish(
        &mut core,
        50,
        NativeInput {
            request: request(EVALUATOR, 5),
            command: NativeCommand::BeginIncrement {
                claim,
                key,
                expected,
            },
        },
    );
    publish(
        &mut core,
        60,
        NativeInput {
            request: request(SUBJECT, 6),
            command: NativeCommand::CloseResponse {
                claim,
                response: binding(900),
                report: NativeResponseInput {
                    summary: "Respondent completed the output.".into(),
                    confidence: Confidence::Committed,
                    outcome: OutcomeKind::Complete,
                    manifest: vec![SlotBinding {
                        slot: 0,
                        artifact: source,
                    }],
                    diagnostics: vec![],
                },
            },
        },
    );
    let claim = core.native_claim(parent.claim).unwrap().binding();
    let response = core
        .native_response(TestamentId::from_u128(900))
        .unwrap()
        .identity()
        .binding;
    publish(
        &mut core,
        70,
        NativeInput {
            request: request(SUBJECT, 7),
            command: NativeCommand::PostResponse {
                claim,
                expected: response,
            },
        },
    );
    let claim = core.native_claim(parent.claim).unwrap().binding();
    let response = core
        .native_response(TestamentId::from_u128(900))
        .unwrap()
        .identity()
        .binding;
    publish(
        &mut core,
        80,
        NativeInput {
            request: request(ISSUER, 8),
            command: NativeCommand::ReceiveResponse {
                claim,
                expected: response,
            },
        },
    );
    let original = *core.native_evaluation(key).unwrap();
    let claim = core.native_claim(parent.claim).unwrap().binding();
    let pressure = budget
        .reserve(
            BudgetKind::Payload,
            BudgetLane::Completion,
            budget.limit() - budget.stats().used,
        )
        .unwrap()
        .commit();
    let before = budget.stats();
    let refusal = NativeOwner::new(core).unwrap_err();
    assert_eq!(budget.stats(), before);
    assert_eq!(refusal.core.native_evaluation(key).unwrap(), &original);
    drop(pressure);
    let mut f = Fixture {
        owner: NativeOwner::new(refusal.core).unwrap(),
        store,
        _directory: directory,
        serial: 100,
    };
    let pressure = budget
        .reserve(
            BudgetKind::Payload,
            BudgetLane::Completion,
            budget.limit() - budget.stats().used,
        )
        .unwrap()
        .commit();
    f.commit(EVALUATOR, report(&f, key, 901, VerdictValue::Error));
    f.commit(EVALUATOR, report(&f, key, 902, VerdictValue::Pass));
    assert_eq!(f.claim(), claim);
    assert_eq!(
        f.owner.committed().evaluation(key).unwrap().target(),
        original.target()
    );
    assert_eq!(
        f.owner.committed().evaluation(key).unwrap().receipt(),
        original.receipt()
    );
    assert_eq!(
        f.owner.committed().evaluation(key).unwrap().state(),
        validation::State::Validated
    );
    drop(pressure);
}

#[test]
fn rejection_before_increment_begin_preserves_failed_work_while_evaluator_reports_actual_result() {
    let mut f = fixture(&[(ValidationMode::Required, Program::Programmatic)]);
    let output = f.work(801, 0);
    let key = evaluation_key(&f, 1, output.artifact.id);
    let original = f.owner.committed().work(output.artifact.id).unwrap().state;
    let mut spec = artifact_spec(802, ISSUER, VerdictValue::Error);
    spec.visibility = &[];
    spec.receipt = Some(original.receipt());
    spec.work = Some(WorkProvenance {
        claim: original.claim(),
        cycle: original.cycle(),
        role: WorkRole::ReceiptRejection {
            artifact: output.artifact,
            reason: EvidenceFailure::Structure,
        },
    });
    let diagnostic = descriptor(spec);
    let rejection = ArtifactRef {
        id: diagnostic.id(),
        hash: diagnostic.content_hash(),
    };
    f.commit(
        ISSUER,
        NativeCommand::RejectWork {
            claim: f.claim(),
            expected: original.binding(),
            reason: EvidenceFailure::Structure,
            artifact: NativeArtifactInput::new(diagnostic).unwrap(),
        },
    );
    let failed = f.owner.committed().work(output.artifact.id).unwrap().state;
    assert_eq!(failed.state(), WorkArtifactState::ReceiptFailed);
    let before = f.owner.committed().evaluation(key).unwrap();
    assert_eq!(before.state(), validation::State::Ready);
    assert!(before.last_result().is_none());
    f.commit(EVALUATOR, begin(&f, key));
    f.commit(EVALUATOR, report(&f, key, 901, VerdictValue::Pass));
    assert_eq!(
        f.owner.committed().evaluation(key).unwrap().state(),
        validation::State::Validated
    );
    assert_eq!(
        f.owner.committed().work(output.artifact.id).unwrap().state,
        failed
    );
    let respondent = f.diagnostic(803);
    f.commit(
        SUBJECT,
        f.close(900, OutcomeKind::Failed, vec![], vec![respondent]),
    );
    let response = f
        .owner
        .committed()
        .response(TestamentId::from_u128(900))
        .unwrap();
    assert!(response.manifest().is_empty());
    assert_eq!(response.failed_work().len(), 1);
    assert_eq!(response.failed_work()[0].diagnostic().artifact, rejection);
    assert_eq!(response.diagnostics()[0].artifact(), respondent);
    assert_eq!(
        f.owner.committed().work(output.artifact.id).unwrap().state,
        failed
    );
}
