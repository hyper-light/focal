use super::super::read_source::Meter;
use super::*;
use crate::native::report_tests as f;
use focal_model::lifecycle::{
    Principal,
    validation::{
        self, Authority, AuthorityState, Cohort, EvidenceFacts, EvidenceKind, Materialization,
        OwnerState, ParentState, PhasePolicy, Program, Readiness, Report,
    },
};
use focal_model::{ArtifactId, HandlerRef, TimerId, ValidationKind, ValidationPhase, ValidatorId};
use std::cell::Cell;

const PROGRAM: HandlerRef = HandlerRef {
    id: ValidatorId::from_u128(10),
    version: ContentHash([10; 32]),
    agentic: false,
};
const FALLBACK: HandlerRef = HandlerRef {
    id: ValidatorId::from_u128(11),
    version: ContentHash([11; 32]),
    agentic: false,
};
const QUALITY: HandlerRef = HandlerRef {
    id: ValidatorId::from_u128(12),
    version: ContentHash([12; 32]),
    agentic: true,
};
const PROGRAM_STEPS: [HandlerPolicy<'static>; 2] = [
    HandlerPolicy {
        handler: &PROGRAM,
        attempts: 2,
        proof_schema: ContentHash([21; 32]),
        diagnostic_schema: ContentHash([22; 32]),
    },
    HandlerPolicy {
        handler: &FALLBACK,
        attempts: 1,
        proof_schema: ContentHash([23; 32]),
        diagnostic_schema: ContentHash([24; 32]),
    },
];
const QUALITY_STEPS: [HandlerPolicy<'static>; 1] = [HandlerPolicy {
    handler: &QUALITY,
    attempts: 2,
    proof_schema: ContentHash([25; 32]),
    diagnostic_schema: ContentHash([26; 32]),
}];
struct Reader {
    work: Meter,
    schema: Cell<Option<(ArtifactRef, ContentHash)>>,
}
impl Reader {
    fn new(visits: usize) -> Self {
        Self {
            work: Meter::new(visits),
            schema: Cell::new(None),
        }
    }
}
impl Read for Reader {
    fn charge(&self, visits: usize) -> Result<(), NativeError> {
        self.work
            .charge(visits)
            .map_err(super::super::read_evidence::codec)
    }
    fn schema(&self, artifact: ArtifactRef) -> Result<ContentHash, NativeError> {
        self.charge(1)?;
        self.schema
            .get()
            .filter(|(reference, _)| *reference == artifact)
            .map(|(_, schema)| schema)
            .ok_or_else(invalid)
    }
}
#[derive(Clone, Copy)]
struct Step {
    kind: NativeEvaluationEventKind,
    attempt: Option<Attempt>,
    state: State,
    phase: Phase,
    fence: Option<AuthorityFence>,
    result: Option<AcceptedResult>,
    schema: Option<(ArtifactRef, ContentHash)>,
}
impl Step {
    fn apply(self, cursor: &mut AttemptCursor<'_>, read: &Reader) -> Result<(), NativeError> {
        read.schema.set(self.schema);
        cursor.event(
            self.kind,
            self.attempt,
            self.state,
            self.phase,
            self.fence,
            self.result,
            read,
        )
    }
}
fn declaration(mode: ValidationMode, path: u8) -> Declaration {
    let quality = PhasePolicy {
        evaluator: f::QUALITY,
        definition: ContentHash([41; 32]),
        handlers: &QUALITY_STEPS,
        required_policy: None,
    };
    let program = match path {
        2 => Program::Agentic { check: quality },
        _ => Program::Programmatic {
            check: PhasePolicy {
                evaluator: f::EVALUATOR,
                definition: ContentHash([40; 32]),
                handlers: &PROGRAM_STEPS,
                required_policy: None,
            },
            quality: (path == 1).then_some(quality),
        },
    };
    Declaration::new(
        Principal::Actor(f::ISSUER),
        validation::DeclarationSpec {
            binding: f::binding(100),
            claim: ClaimId::from_u128(200),
            issuer: f::ISSUER,
            declaration_index: 4,
            kind: ValidationKind::Inspection,
            phase: ValidationPhase::Admission,
            mode,
            target: validation::TargetDeclaration::Admission,
            program,
            deadline: Deadline {
                timer: TimerId::from_u128(20),
                generation: 1,
                at: 100,
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
fn policy<'a>(declaration: &'a Declaration, phase: Phase) -> PhasePolicyView<'a> {
    match (declaration.program(), phase) {
        (ProgramView::Programmatic { check, .. }, Phase::Programmatic)
        | (
            ProgramView::Programmatic {
                quality: Some(check),
                ..
            },
            Phase::Quality,
        )
        | (ProgramView::Agentic { check }, Phase::Quality) => check,
        _ => panic!("external phase"),
    }
}
fn owner(evaluation: &validation::Evaluation<'_>, declaration: &Declaration) -> OwnerState {
    let policy = policy(declaration, evaluation.current_phase());
    OwnerState {
        evaluation: evaluation.binding(),
        target: evaluation.target(),
        parent: ParentState::Open,
        readiness: Readiness::AdmissionPosted,
        cohort: Cohort::Open,
        authority: Authority {
            evaluator: policy.evaluator(),
            definition: policy.definition(),
            generation: evaluation.generation(),
            receipt: None,
            deadline: declaration.deadline(),
            policy_evidence: None,
            state: AuthorityState::Live,
        },
        logical_time: 1,
    }
}
fn ready(declaration: &Declaration) -> validation::Evaluation<'_> {
    validation::Evaluation::materialize(
        Principal::Actor(f::ISSUER),
        declaration,
        Materialization {
            binding: declaration.binding(),
            target: Target::Admission {
                claim: f::binding(200),
            },
            slot_name: None,
            generation: 7,
            receipt: None,
        },
    )
    .unwrap()
}
fn marker(kind: NativeEvaluationEventKind, evaluation: validation::Evaluation<'_>) -> Step {
    Step {
        kind,
        attempt: if evaluation.has_begun() && !evaluation.state().is_terminal() {
            Some(evaluation.current_attempt().unwrap())
        } else {
            None
        },
        state: evaluation.state(),
        phase: evaluation.current_phase(),
        fence: evaluation.fence(),
        result: None,
        schema: None,
    }
}
fn begun(declaration: &Declaration) -> (validation::Evaluation<'_>, Vec<Step>) {
    let ready = ready(declaration);
    let materialized = marker(NativeEvaluationEventKind::Materialized, ready);
    let evaluation = ready
        .begin(
            Principal::Actor(ready.evaluator().unwrap()),
            &ready.binding(),
            &owner(&ready, declaration),
        )
        .unwrap()
        .next;
    (
        evaluation,
        vec![
            materialized,
            marker(NativeEvaluationEventKind::Begun, evaluation),
        ],
    )
}
fn reported<'a>(
    evaluation: validation::Evaluation<'a>,
    declaration: &'a Declaration,
    verdict: VerdictValue,
) -> (validation::Evaluation<'a>, Step) {
    let attempt = evaluation.current_attempt().unwrap();
    let handler = policy(declaration, attempt.phase)
        .handlers()
        .find(|step| step.handler.id == attempt.handler)
        .unwrap();
    let (kind, schema) = match verdict {
        VerdictValue::Pass | VerdictValue::Fail => (EvidenceKind::Proof, handler.proof_schema),
        VerdictValue::Incomplete | VerdictValue::Error => {
            (EvidenceKind::Diagnostic, handler.diagnostic_schema)
        }
    };
    let evidence_binding = f::binding(1000 + u128::from(attempt.index));
    let reference = ArtifactRef {
        id: ArtifactId(evidence_binding.object.0),
        hash: evidence_binding.content,
    };
    let report = Report {
        generation: evaluation.generation(),
        attempt,
        value: verdict,
        evidence: reference,
    };
    let facts = EvidenceFacts {
        binding: evidence_binding,
        claim: evaluation.claim(),
        validation: evaluation.validation(),
        target: evaluation.target(),
        generation: evaluation.generation(),
        attempt,
        producer: attempt.evaluator,
        value: verdict,
        kind,
        schema,
        custody_revision: Some(1),
    };
    let transition = evaluation
        .report(
            Principal::Actor(attempt.evaluator),
            &evaluation.binding(),
            &owner(&evaluation, declaration),
            report,
            &facts,
        )
        .unwrap();
    let next = transition.next;
    (
        next,
        Step {
            kind: NativeEvaluationEventKind::Reported,
            attempt: Some(attempt),
            state: next.state(),
            phase: next.current_phase(),
            fence: next.fence(),
            result: transition.result,
            schema: Some((reference, schema)),
        },
    )
}
fn check(
    declaration: &Declaration,
    steps: &[Step],
    final_state: &EvaluationState,
    read: &Reader,
) -> Result<(), NativeError> {
    let mut cursor = AttemptCursor::new(declaration);
    for step in steps {
        step.apply(&mut cursor, read)?;
    }
    cursor.finish(final_state, read)
}

#[test]
fn actual_retry_fallback_early_quality_agentic_and_terminal_histories_match_every_retained_cursor()
{
    for mode in [ValidationMode::Required, ValidationMode::Observe] {
        for path in 0..3 {
            let declaration = declaration(mode, path);
            for preceding_errors in 0..3 {
                if path != 1 && preceding_errors != 0 {
                    continue;
                }
                for verdict in [
                    VerdictValue::Pass,
                    VerdictValue::Fail,
                    VerdictValue::Incomplete,
                    VerdictValue::Error,
                ] {
                    let (mut evaluation, mut steps) = begun(&declaration);
                    check(
                        &declaration,
                        &steps,
                        &evaluation.into_state(),
                        &Reader::new(1_000_000),
                    )
                    .unwrap();
                    if path == 1 {
                        for _ in 0..preceding_errors {
                            let (next, step) =
                                reported(evaluation, &declaration, VerdictValue::Error);
                            evaluation = next;
                            steps.push(step);
                            check(
                                &declaration,
                                &steps,
                                &evaluation.into_state(),
                                &Reader::new(1_000_000),
                            )
                            .unwrap();
                        }
                        let (next, step) = reported(evaluation, &declaration, VerdictValue::Pass);
                        evaluation = next;
                        steps.push(step);
                        assert_eq!(evaluation.current_phase(), Phase::Quality);
                        assert_eq!(
                            evaluation.current_attempt().unwrap().index,
                            preceding_errors + 1
                        );
                    }
                    for _ in 0..declaration.attempt_bound() {
                        let (next, step) = reported(evaluation, &declaration, verdict);
                        evaluation = next;
                        steps.push(step);
                        check(
                            &declaration,
                            &steps,
                            &evaluation.into_state(),
                            &Reader::new(1_000_000),
                        )
                        .unwrap();
                        if evaluation.state().is_terminal() {
                            break;
                        }
                        assert_eq!(verdict, VerdictValue::Error);
                    }
                    assert!(evaluation.state().is_terminal());
                    let read = Reader::new(1_000_000);
                    check(&declaration, &steps, &evaluation.into_state(), &read).unwrap();
                    let used = 1_000_000 - read.work.remaining();
                    check(
                        &declaration,
                        &steps,
                        &evaluation.into_state(),
                        &Reader::new(used),
                    )
                    .unwrap();
                    assert!(
                        check(
                            &declaration,
                            &steps,
                            &evaluation.into_state(),
                            &Reader::new(used - 1)
                        )
                        .is_err()
                    );
                }
            }
        }
    }
}

#[test]
fn skipped_retry_forged_handler_wrong_schema_and_hidden_quality_entry_cannot_match_valid_final_rows()
 {
    let declaration = declaration(ValidationMode::Required, 1);
    let (mut evaluation, mut steps) = begun(&declaration);
    for verdict in [
        VerdictValue::Error,
        VerdictValue::Error,
        VerdictValue::Pass,
        VerdictValue::Pass,
    ] {
        let (next, step) = reported(evaluation, &declaration, verdict);
        evaluation = next;
        steps.push(step);
    }
    let final_state = evaluation.into_state();
    check(&declaration, &steps, &final_state, &Reader::new(1_000_000)).unwrap();
    for defect in 0..8 {
        let mut changed = steps.clone();
        match defect {
            0 => {
                changed.remove(2);
            }
            1 => changed[2].attempt.as_mut().unwrap().index = 1,
            2 => changed[2].attempt.as_mut().unwrap().handler = FALLBACK.id,
            3 => changed[2].attempt.as_mut().unwrap().version = ContentHash([99; 32]),
            4 => changed[2].attempt.as_mut().unwrap().evaluator = f::QUALITY,
            5 => changed[2].attempt.as_mut().unwrap().definition = ContentHash([99; 32]),
            6 => changed[2].schema.as_mut().unwrap().1 = PROGRAM_STEPS[0].proof_schema,
            _ => changed[4].phase = Phase::Programmatic,
        }
        assert!(
            check(
                &declaration,
                &changed,
                &final_state,
                &Reader::new(1_000_000)
            )
            .is_err(),
            "defect {defect}"
        );
    }
    // Correct final state alone does not prove that the successful programmatic
    // attempt was actually delivered before quality began.
    let mut changed = steps.clone();
    changed.remove(4);
    assert!(
        check(
            &declaration,
            &changed,
            &final_state,
            &Reader::new(1_000_000)
        )
        .is_err()
    );
    // Scalar hydration cannot prove which prior artifact actually passed. An
    // internally coherent final quality row can pin a different retained
    // artifact; the complete historical cursor must still reject that swap.
    let wrong_proof = steps[2].result.unwrap().evidence().unwrap();
    let mut snapshot = final_state.snapshot_v1().unwrap();
    snapshot.programmatic_evidence = Some(wrong_proof);
    snapshot.last_result.as_mut().unwrap().programmatic_evidence = Some(wrong_proof);
    let forged = EvaluationState::hydrate_v1(&declaration, snapshot, usize::MAX).unwrap();
    let mut changed = steps.clone();
    changed.last_mut().unwrap().result = forged.last_result();
    assert!(check(&declaration, &changed, &forged, &Reader::new(1_000_000)).is_err());
}

#[test]
fn begun_sealed_chains_continue_but_a_recorded_fence_stops_external_reports() {
    let declaration = declaration(ValidationMode::Required, 1);
    let (evaluation, mut steps) = begun(&declaration);
    let mut sealed_owner = owner(&evaluation, &declaration);
    sealed_owner.cohort = Cohort::Sealed {
        cause: ContentHash([61; 32]),
    };
    let sealed = evaluation
        .record_seal(&evaluation.binding(), &sealed_owner)
        .unwrap();
    steps.push(marker(NativeEvaluationEventKind::Sealed, sealed));
    check(
        &declaration,
        &steps,
        &sealed.into_state(),
        &Reader::new(1_000_000),
    )
    .unwrap();
    let (passed, report) = reported(sealed, &declaration, VerdictValue::Pass);
    let mut continued = steps.clone();
    continued.push(report);
    check(
        &declaration,
        &continued,
        &passed.into_state(),
        &Reader::new(1_000_000),
    )
    .unwrap();
    let mut fenced_owner = owner(&sealed, &declaration);
    fenced_owner.authority.state = AuthorityState::Fenced(AuthorityFence {
        reason: FenceReason::Evaluation,
        cause: ContentHash([62; 32]),
    });
    let fenced = sealed
        .record_fence(&sealed.binding(), &fenced_owner)
        .unwrap();
    steps.push(marker(NativeEvaluationEventKind::AuthorityFenced, fenced));
    check(
        &declaration,
        &steps,
        &fenced.into_state(),
        &Reader::new(1_000_000),
    )
    .unwrap();
    steps.push(report);
    assert!(
        check(
            &declaration,
            &steps,
            &passed.into_state(),
            &Reader::new(1_000_000)
        )
        .is_err()
    );
    let mut erased = continued;
    erased.remove(2);
    assert!(
        check(
            &declaration,
            &erased,
            &passed.into_state(),
            &Reader::new(1_000_000)
        )
        .is_err()
    );
}

#[test]
fn actual_native_delivery_and_missing_results_keep_unbegun_cursors_independent() {
    let (core, _store, _directory) = super::super::evidence::tests::recovery_fixture(3);
    let read = Reader::new(1_000_000);
    let mut deliveries = 0;
    let mut missing = 0;
    for entry in core.state.rows.entries() {
        let (Key::Evaluation(key), Row::Evaluation(owned)) = (entry.key, &entry.value) else {
            continue;
        };
        if !matches!(
            key.target,
            EvaluationTarget::Delivery { .. } | EvaluationTarget::MissingSlot { .. }
        ) {
            continue;
        }
        let declaration = core.native_definition(key.validation).unwrap();
        let state = owned.get().unwrap();
        let mut cursor = AttemptCursor::new(declaration);
        for entry in core.state.rows.entries() {
            let Row::Event(event) = &entry.value else {
                continue;
            };
            match event.get().unwrap().expand(core.state.ledger).fact {
                NativeFact::Evaluation {
                    kind,
                    key: recorded,
                    after,
                    state,
                    phase,
                    attempt,
                    fence,
                    ..
                } if recorded == key => {
                    let result = if kind == NativeEvaluationEventKind::MissingTarget {
                        let key = NativeResultKey {
                            evaluation: key,
                            revision: after.revision,
                        };
                        match core.state.rows.get(&Key::MissingResult(key)) {
                            Some(Row::MissingResult(value)) => Some(value.get().unwrap().result()),
                            _ => None,
                        }
                    } else {
                        None
                    };
                    cursor
                        .event(kind, attempt, state, phase, fence, result, &read)
                        .unwrap();
                }
                NativeFact::Delivery { key: result_key } if result_key.evaluation == key => {
                    let Some(Row::DeliveryResult(value)) =
                        core.state.rows.get(&Key::DeliveryResult(result_key))
                    else {
                        panic!("delivery result");
                    };
                    cursor
                        .delivery(value.get().unwrap().result(), &read)
                        .unwrap();
                    deliveries += 1;
                }
                _ => (),
            }
        }
        cursor.finish(state, &read).unwrap();
        AttemptCursor::resume(declaration, state, &read)
            .unwrap()
            .finish(state, &read)
            .unwrap();
        assert!(!state.has_begun());
        if matches!(key.target, EvaluationTarget::MissingSlot { .. }) {
            missing += 1;
        }
    }
    assert!(deliveries > 0 && missing > 0);
}

#[test]
fn suppressed_begin_stays_unbegun_and_exhausted_program_never_enters_optional_quality() {
    let declaration = declaration(ValidationMode::Required, 1);
    let evaluation = ready(&declaration);
    let mut frame = owner(&evaluation, &declaration);
    frame.parent = ParentState::Failed {
        cause: ContentHash([81; 32]),
    };
    let suppressed = evaluation
        .begin(
            Principal::Actor(evaluation.evaluator().unwrap()),
            &evaluation.binding(),
            &frame,
        )
        .unwrap()
        .next;
    assert_eq!(suppressed.state(), State::Ready);
    assert!(!suppressed.has_begun());
    let mut steps = vec![
        marker(NativeEvaluationEventKind::Materialized, evaluation),
        marker(NativeEvaluationEventKind::Begun, suppressed),
    ];
    check(
        &declaration,
        &steps,
        &suppressed.into_state(),
        &Reader::new(1_000_000),
    )
    .unwrap();
    let mut seal = owner(&suppressed, &declaration);
    seal.cohort = Cohort::Sealed {
        cause: ContentHash([82; 32]),
    };
    let sealed = suppressed
        .record_seal(&suppressed.binding(), &seal)
        .unwrap();
    steps.push(marker(NativeEvaluationEventKind::Sealed, sealed));
    check(
        &declaration,
        &steps,
        &sealed.into_state(),
        &Reader::new(1_000_000),
    )
    .unwrap();
    let (mut evaluation, mut steps) = begun(&declaration);
    for _ in 0..3 {
        let (next, step) = reported(evaluation, &declaration, VerdictValue::Error);
        evaluation = next;
        steps.push(step);
    }
    assert_eq!(evaluation.state(), State::Errored);
    assert_eq!(evaluation.current_phase(), Phase::Programmatic);
    check(
        &declaration,
        &steps,
        &evaluation.into_state(),
        &Reader::new(1_000_000),
    )
    .unwrap();
}

#[test]
fn replay_resume_accepts_every_actual_retry_quality_and_terminal_suffix_without_old_artifacts() {
    for mode in [ValidationMode::Required, ValidationMode::Observe] {
        for path in 0..3 {
            let declaration = declaration(mode, path);
            for verdict in [
                VerdictValue::Pass,
                VerdictValue::Fail,
                VerdictValue::Incomplete,
                VerdictValue::Error,
            ] {
                let (mut evaluation, mut steps) = begun(&declaration);
                let mut predecessors = vec![
                    (ready(&declaration).into_state(), 1),
                    (evaluation.into_state(), 2),
                ];
                if path == 1 {
                    // Exercise the second attempt, fallback handler and exact
                    // successful programmatic proof before entering quality.
                    for value in [VerdictValue::Error, VerdictValue::Error, VerdictValue::Pass] {
                        let (next, step) = reported(evaluation, &declaration, value);
                        evaluation = next;
                        steps.push(step);
                        predecessors.push((evaluation.into_state(), steps.len()));
                    }
                }
                for _ in 0..declaration.attempt_bound() {
                    let (next, step) = reported(evaluation, &declaration, verdict);
                    evaluation = next;
                    steps.push(step);
                    predecessors.push((evaluation.into_state(), steps.len()));
                    if evaluation.state().is_terminal() {
                        break;
                    }
                    assert_eq!(verdict, VerdictValue::Error);
                }
                let final_state = evaluation.into_state();
                check(&declaration, &steps, &final_state, &Reader::new(1_000_000)).unwrap();
                for (predecessor, offset) in predecessors {
                    let read = Reader::new(1_000_000);
                    // Reader has no historical artifacts at resume time.
                    let mut cursor =
                        AttemptCursor::resume(&declaration, &predecessor, &read).unwrap();
                    let used = 1_000_000 - read.work.remaining();
                    AttemptCursor::resume(&declaration, &predecessor, &Reader::new(used)).unwrap();
                    assert!(
                        AttemptCursor::resume(&declaration, &predecessor, &Reader::new(used - 1))
                            .is_err()
                    );
                    cursor.finish(&predecessor, &read).unwrap();
                    for step in &steps[offset..] {
                        step.apply(&mut cursor, &read).unwrap();
                    }
                    cursor.finish(&final_state, &read).unwrap();
                }
            }
        }
    }
}

#[test]
fn replay_resume_preserves_exact_suppression_and_seal_and_rejects_a_foreign_definition() {
    let declaration = declaration(ValidationMode::Required, 1);
    let ready = ready(&declaration);
    let mut frame = owner(&ready, &declaration);
    frame.parent = ParentState::Failed {
        cause: ContentHash([91; 32]),
    };
    let suppressed = ready
        .begin(
            Principal::Actor(ready.evaluator().unwrap()),
            &ready.binding(),
            &frame,
        )
        .unwrap()
        .next;
    let mut seal_frame = owner(&suppressed, &declaration);
    seal_frame.cohort = Cohort::Sealed {
        cause: ContentHash([92; 32]),
    };
    let sealed = suppressed
        .record_seal(&suppressed.binding(), &seal_frame)
        .unwrap();
    let read = Reader::new(1_000_000);
    let mut cursor = AttemptCursor::resume(&declaration, &suppressed.into_state(), &read).unwrap();
    marker(NativeEvaluationEventKind::Sealed, sealed)
        .apply(&mut cursor, &read)
        .unwrap();
    cursor.finish(&sealed.into_state(), &read).unwrap();
    let predecessor = sealed.into_state();
    let cursor = AttemptCursor::resume(&declaration, &predecessor, &read).unwrap();
    cursor.finish(&predecessor, &read).unwrap();
    for change_seal in [false, true] {
        let mut changed = predecessor.snapshot_v1().unwrap();
        if change_seal {
            changed.sealed = Some(ContentHash([93; 32]));
        } else {
            changed.suppression = Some(Suppression::ParentFailure(ContentHash([93; 32])));
        }
        // Intrinsic hydration permits either cause; replay must retain the
        // particular cause that was present in the validated predecessor.
        let changed = EvaluationState::hydrate_v1(&declaration, changed, usize::MAX).unwrap();
        assert!(cursor.finish(&changed, &read).is_err());
    }
    let mut fence_frame = owner(&sealed, &declaration);
    fence_frame.authority.state = AuthorityState::Fenced(AuthorityFence {
        reason: FenceReason::Evaluation,
        cause: ContentHash([94; 32]),
    });
    let fenced = sealed
        .record_fence(&sealed.binding(), &fence_frame)
        .unwrap();
    let cursor = AttemptCursor::resume(&declaration, &fenced.into_state(), &read).unwrap();
    cursor.finish(&fenced.into_state(), &read).unwrap();
    assert!(cursor.finish(&sealed.into_state(), &read).is_err());

    let different = self::declaration(ValidationMode::Observe, 1);
    assert_eq!(different.binding(), declaration.binding());
    assert!(AttemptCursor::resume(&different, &predecessor, &read).is_err());
}
