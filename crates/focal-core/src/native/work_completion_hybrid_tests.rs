use super::*;
use crate::native::report_tests::{self as reports, EVALUATOR, QUALITY};
use focal_memory::{Allocation, BudgetKind, BudgetLane};
use focal_model::lifecycle::artifact_descriptor::ResultProvenance;
use focal_model::{Deadline, HandlerRef, TimerId, ValidationKind, ValidationPhase, ValidatorId};

fn hybrid_declaration() -> validation::Declaration {
    let programmatic = HandlerRef {
        id: ValidatorId::from_u128(77),
        version: ContentHash([77; 32]),
        agentic: false,
    };
    let agentic = HandlerRef {
        id: ValidatorId::from_u128(78),
        version: ContentHash([78; 32]),
        agentic: true,
    };
    let programmatic_handlers = [validation::HandlerPolicy {
        handler: &programmatic,
        attempts: 2,
        proof_schema: test_report_schema(),
        diagnostic_schema: error_report_schema(),
    }];
    let quality_handlers = [validation::HandlerPolicy {
        handler: &agentic,
        attempts: 1,
        proof_schema: test_report_schema(),
        diagnostic_schema: error_report_schema(),
    }];
    validation::Declaration::new(
        Principal::Actor(ISSUER),
        validation::DeclarationSpec {
            binding: binding(301),
            claim: CLAIM,
            issuer: ISSUER,
            declaration_index: 1,
            kind: ValidationKind::Test,
            phase: ValidationPhase::WholeWork,
            mode: ValidationMode::Required,
            target: validation::TargetDeclaration::WholeWorkSlot {
                index: 0,
                name: "primary",
            },
            program: validation::Program::Programmatic {
                check: validation::PhasePolicy {
                    evaluator: EVALUATOR,
                    definition: ContentHash([80; 32]),
                    handlers: &programmatic_handlers,
                    required_policy: None,
                },
                quality: Some(validation::PhasePolicy {
                    evaluator: QUALITY,
                    definition: ContentHash([81; 32]),
                    handlers: &quality_handlers,
                    required_policy: None,
                }),
            },
            deadline: Deadline {
                timer: TimerId::from_u128(301),
                generation: 1,
                at: 1000,
            },
        },
        validation::Limits {
            handlers: 2,
            attempts: 3,
            slot_bytes: 64,
        },
    )
    .unwrap()
}

fn report(
    f: &Fixture,
    key: EvaluationKey,
    id: u128,
    value: VerdictValue,
) -> (validation::Attempt, ArtifactRef, NativeCommand) {
    let view = f.owner.effective();
    let state = view.evaluation(key).unwrap();
    let attempt = state
        .bind(view.definition(key.validation).unwrap())
        .unwrap()
        .current_attempt()
        .unwrap();
    let mut spec = reports::artifact_spec(id, attempt.evaluator, value);
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
    let descriptor = reports::descriptor(spec);
    let reference = ArtifactRef {
        id: descriptor.id(),
        hash: descriptor.content_hash(),
    };
    (
        attempt,
        reference,
        NativeCommand::ReportWork {
            claim: f.claim(),
            key,
            expected: state.binding(),
            report: validation::Report {
                generation: state.generation(),
                attempt,
                value,
                evidence: reference,
            },
            artifact: NativeArtifactInput::new(descriptor).unwrap(),
        },
    )
}

fn exhaust(f: &Fixture) -> Allocation {
    let source = f.owner.budget_for_test();
    let pressure = source
        .reserve(
            BudgetKind::Payload,
            BudgetLane::Completion,
            source.limit() - source.stats().used,
        )
        .unwrap()
        .commit();
    assert_eq!(source.stats().used, source.limit());
    pressure
}

fn assert_pending_acceptance(f: &Fixture) {
    let view = f.owner.committed();
    let claim = view.claim(CLAIM).unwrap();
    assert_eq!(claim.status(), ClaimStatus::Validating);
    assert!(!claim.local_complete());
    assert!(claim.terminal_cut().is_none());
    assert_eq!(
        view.response(RESPONSE).unwrap().state(),
        ResponseState::Validating
    );
    assert_eq!(
        view.work(ArtifactId::from_u128(801)).unwrap().state.state(),
        WorkArtifactState::Validating
    );
}

#[test]
fn programmatic_retry_and_distinct_agentic_quality_complete_under_exhausted_ancestor_ram() {
    let declaration = hybrid_declaration();
    assert_eq!(declaration.attempt_bound(), 3);
    let mut f = checked_slot_fixture_with_definition(ValidationMode::Required, 65_536, declaration);
    complete_response(&mut f, 900, 801);
    let key = EvaluationKey {
        claim: CLAIM,
        validation: ValidationId::from_u128(301),
        target: EvaluationTarget::Work {
            response: RESPONSE,
            slot: 0,
            artifact: ArtifactId::from_u128(801),
        },
        generation: 1,
    };
    let target = f.owner.effective().evaluation(key).unwrap().target();
    let receipt = f.owner.effective().evaluation(key).unwrap().receipt();
    let expected = f.owner.effective().evaluation(key).unwrap().binding();
    f.commit(
        EVALUATOR,
        NativeCommand::BeginWork {
            claim: f.claim(),
            key,
            expected,
        },
    );
    assert_pending_acceptance(&f);
    let original = f.owner.committed().evaluation(key).unwrap().binding();
    let pin = f.owner.pin(0, 10_000).unwrap();

    let pressure_programmatic = exhaust(&f);
    let (attempt0, error_reference, command) = report(&f, key, 1901, VerdictValue::Error);
    assert_eq!(
        (attempt0.phase, attempt0.index, attempt0.evaluator),
        (validation::Phase::Programmatic, 0, EVALUATOR)
    );
    let error_outcome = f.commit(EVALUATOR, command);
    assert_pending_acceptance(&f);
    let retry_result = f
        .owner
        .committed()
        .evaluation(key)
        .unwrap()
        .last_result()
        .unwrap();
    assert!(!retry_result.is_terminal());
    assert_eq!(retry_result.evidence(), Some(error_reference));
    assert_eq!(retry_result.programmatic_evidence(), None);

    // A completed report may return conservative unused credit. Consume that
    // newly available ancestor capacity too before testing the next phase.
    let pressure_retry = exhaust(&f);
    let (attempt1, programmatic_reference, command) = report(&f, key, 1902, VerdictValue::Pass);
    assert_eq!(
        (attempt1.phase, attempt1.index, attempt1.evaluator),
        (validation::Phase::Programmatic, 1, EVALUATOR)
    );
    let programmatic_outcome = f.commit(EVALUATOR, command);
    assert!(programmatic_outcome.sequence > error_outcome.sequence);
    assert_pending_acceptance(&f);
    let programmatic_result = f
        .owner
        .committed()
        .evaluation(key)
        .unwrap()
        .last_result()
        .unwrap();
    assert!(!programmatic_result.is_terminal());
    assert_eq!(
        programmatic_result.resulting_state(),
        validation::State::ValidatingQualityBar
    );
    assert_eq!(
        programmatic_result.programmatic_evidence(),
        Some(programmatic_reference)
    );

    let pressure_quality = exhaust(&f);
    let (quality_attempt, _, wrong_actor_command) = report(&f, key, 1903, VerdictValue::Pass);
    assert_eq!(
        (
            quality_attempt.phase,
            quality_attempt.index,
            quality_attempt.evaluator
        ),
        (validation::Phase::Quality, 2, QUALITY)
    );
    assert_eq!(quality_attempt.handler, ValidatorId::from_u128(78));
    assert_eq!(quality_attempt.definition, ContentHash([81; 32]));
    assert_refused_unchanged(&mut f, EVALUATOR, wrong_actor_command);
    assert!(
        f.owner
            .effective()
            .artifact(ArtifactId::from_u128(1903))
            .is_none()
    );
    assert_pending_acceptance(&f);
    let (actual_quality, quality_reference, command) = report(&f, key, 1904, VerdictValue::Pass);
    assert_eq!(actual_quality, quality_attempt);
    let complete = f.commit(QUALITY, command);
    assert!(complete.sequence > programmatic_outcome.sequence);
    let view = f.owner.committed();
    let state = view.evaluation(key).unwrap();
    assert_eq!(state.state(), validation::State::Validated);
    assert_eq!(state.binding().revision.0, original.revision.0 + 3);
    assert_eq!(state.target(), target);
    assert_eq!(state.receipt(), receipt);
    let terminal = state.last_result().unwrap();
    assert!(terminal.is_terminal());
    assert_eq!(terminal.evidence(), Some(quality_reference));
    assert_eq!(
        terminal.programmatic_evidence(),
        Some(programmatic_reference)
    );
    assert_eq!(terminal.reporter(), Some(QUALITY));
    assert_eq!(view.claim(CLAIM).unwrap().status(), ClaimStatus::Satisfied);
    assert_eq!(
        view.response(RESPONSE).unwrap().state(),
        ResponseState::Validated
    );
    assert_eq!(
        view.work(ArtifactId::from_u128(801)).unwrap().state.state(),
        WorkArtifactState::Validated
    );
    for (result, attempt, evidence, outcome) in [
        (retry_result, attempt0, error_reference, error_outcome),
        (
            programmatic_result,
            attempt1,
            programmatic_reference,
            programmatic_outcome,
        ),
        (terminal, quality_attempt, quality_reference, complete),
    ] {
        let retained = view.result(NativeResultKey::of(result)).unwrap();
        assert_eq!(retained.result(), result);
        assert_eq!(retained.attempt(), attempt);
        assert_eq!(retained.sequence(), outcome.sequence);
        let artifact = view.artifact(evidence.id).unwrap();
        assert_eq!(artifact.descriptor().content_hash(), evidence.hash);
        assert_eq!(artifact.descriptor().producer(), attempt.evaluator);
        assert_eq!(
            artifact.descriptor().result_provenance().unwrap().attempt,
            attempt
        );
        assert!(artifact.custody().local_revision() > 0);
    }
    assert_eq!(
        pin.with_evaluation(key, 0, |row| row.state()).unwrap(),
        Some(validation::State::Validating)
    );
    assert_eq!(
        pin.with_response_record(RESPONSE, 0, |row| row.response().state())
            .unwrap(),
        Some(ResponseState::Validating)
    );
    drop((pressure_programmatic, pressure_retry, pressure_quality));
}
