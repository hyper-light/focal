use super::super::tests::{
    ISSUER, binding, declaration, limits, owner_for, programmatic, ready, report_parts,
    report_value, specification,
};
use super::*;
use crate::lifecycle::memory;
use crate::{ObjectId, ObjectRevision, ReceiptId};

fn agentic() -> Program<'static> {
    match programmatic(true) {
        Program::Programmatic {
            quality: Some(check),
            ..
        } => Program::Agentic { check },
        _ => unreachable!(),
    }
}

fn begin(declaration: &Declaration) -> Evaluation<'_> {
    let evaluation = ready(declaration);
    evaluation
        .begin(
            Principal::Actor(evaluation.evaluator().unwrap()),
            &evaluation.binding(),
            &owner_for(&evaluation),
        )
        .unwrap()
        .next
}

fn round_trip(declaration: &Declaration, evaluation: Evaluation<'_>) {
    let original = evaluation.into_state();
    let snapshot = original.snapshot_v1().unwrap();
    let restored = EvaluationState::hydrate_v1(
        declaration,
        snapshot,
        EvaluationState::hydration_visits(declaration).unwrap(),
    )
    .unwrap();
    assert_eq!(restored, original);
    assert_eq!(restored.snapshot_v1().unwrap(), snapshot);
    assert_eq!(
        restored.bind(declaration).unwrap().current_attempt(),
        evaluation.current_attempt()
    );
    if let Some(result) = original.last_result() {
        assert_eq!(
            AcceptedResult::hydrate_v1(
                declaration,
                result.snapshot_v1(),
                AcceptedResult::hydration_visits(declaration).unwrap(),
            )
            .unwrap(),
            result
        );
    }
}

fn refuse_rows(declaration: &Declaration, rows: &[(&str, EvaluationSnapshotV1)]) {
    let visits = EvaluationState::hydration_visits(declaration).unwrap();
    for (label, row) in rows {
        assert!(
            EvaluationState::hydrate_v1(declaration, *row, visits).is_err(),
            "{label}"
        );
    }
}

fn refuse_results(declaration: &Declaration, rows: &[(&str, AcceptedResultSnapshotV1)]) {
    let visits = AcceptedResult::hydration_visits(declaration).unwrap();
    for (label, row) in rows {
        assert!(
            AcceptedResult::hydrate_v1(declaration, *row, visits).is_err(),
            "{label}"
        );
    }
}

#[test]
fn every_terminal_verdict_and_mode_restores_from_actual_reports() {
    for mode in [ValidationMode::Required, ValidationMode::Observe] {
        for path in 0..3 {
            let definition = declaration(
                mode,
                match path {
                    0 => programmatic(false),
                    1 => agentic(),
                    _ => programmatic(true),
                },
            );
            for verdict in [
                VerdictValue::Pass,
                VerdictValue::Fail,
                VerdictValue::Incomplete,
                VerdictValue::Error,
            ] {
                let mut evaluation = begin(&definition);
                if path == 2 {
                    evaluation = report_value(&evaluation, VerdictValue::Pass).next;
                }
                round_trip(&definition, evaluation);
                // Error is terminal only after the real policy exhausts retries
                // and fallback handlers. All intermediate results are retained.
                for _ in 0..definition.attempt_bound() {
                    evaluation = report_value(&evaluation, verdict).next;
                    round_trip(&definition, evaluation);
                    if evaluation.state().is_terminal() {
                        break;
                    }
                    assert_eq!(verdict, VerdictValue::Error);
                }
                let expected = match (verdict, path == 0, mode) {
                    (VerdictValue::Pass, _, _) => State::Validated,
                    (VerdictValue::Incomplete, _, _) => State::ValidationIncomplete,
                    (VerdictValue::Error, _, ValidationMode::Required) => State::Errored,
                    (VerdictValue::Error, _, ValidationMode::Observe) => State::ErroredNotRequired,
                    (VerdictValue::Fail, true, ValidationMode::Required) => State::ValidationFailed,
                    (VerdictValue::Fail, true, ValidationMode::Observe) => {
                        State::ValidationFailedNotRequired
                    }
                    (VerdictValue::Fail, false, ValidationMode::Required) => {
                        State::QualityBarValidationFailed
                    }
                    (VerdictValue::Fail, false, ValidationMode::Observe) => {
                        State::QualityBarValidationFailedNotRequired
                    }
                };
                assert_eq!(evaluation.state(), expected);
                assert_eq!(
                    evaluation.last_result().unwrap().reporter(),
                    Some(evaluation.evaluator().unwrap())
                );
            }
        }
    }
}

#[test]
fn early_and_fallback_quality_entry_preserve_local_cursors_and_historical_results() {
    let definition = declaration(ValidationMode::Required, programmatic(true));
    for preceding_errors in [0, 1, 2] {
        let mut evaluation = begin(&definition);
        for _ in 0..preceding_errors {
            evaluation = report_value(&evaluation, VerdictValue::Error).next;
            round_trip(&definition, evaluation);
        }
        let passing = report_value(&evaluation, VerdictValue::Pass);
        let historical = passing.result.unwrap();
        evaluation = passing.next;
        let snapshot = evaluation.into_state().snapshot_v1().unwrap();
        assert_eq!((snapshot.handler, snapshot.handler_attempt), (0, 0));
        assert_eq!(snapshot.attempt, preceding_errors + 1);
        assert_eq!(snapshot.phase, Phase::Quality);
        assert_eq!(historical.phase(), Phase::Programmatic);
        assert_eq!(snapshot.programmatic_evidence, historical.evidence());
        round_trip(&definition, evaluation);
        evaluation = report_value(&evaluation, VerdictValue::Error).next;
        round_trip(&definition, evaluation);
        evaluation = report_value(&evaluation, VerdictValue::Pass).next;
        round_trip(&definition, evaluation);
        assert_eq!(
            evaluation.last_result().unwrap().programmatic_evidence(),
            historical.evidence()
        );
        // A separately retained historical result describes its own transition,
        // even after the current row advances to a later terminal state.
        let restored =
            AcceptedResult::hydrate_v1(&definition, historical.snapshot_v1(), usize::MAX).unwrap();
        assert_eq!(restored, historical);
        assert_eq!(restored.resulting_state(), State::ValidatingQualityBar);
        assert_eq!(evaluation.state(), State::Validated);
    }
}

#[test]
fn real_seals_fences_and_late_sealed_reports_preserve_prior_results() {
    let definition = declaration(ValidationMode::Required, programmatic(true));
    let active = begin(&definition);
    let retry = report_value(&active, VerdictValue::Error).next;
    let old_result = retry.last_result().unwrap();
    for reason in [
        FenceReason::Cancellation,
        FenceReason::Revocation,
        FenceReason::Supersession,
        FenceReason::Expiry,
        FenceReason::ReceiptAdoption,
        FenceReason::Evaluation,
        FenceReason::Deadline(retry.deadline()),
    ] {
        // General owner fences historically permit a zero cause.
        let fence = AuthorityFence {
            reason,
            cause: ContentHash([0; 32]),
        };
        let mut owner = owner_for(&retry);
        owner.authority.state = AuthorityState::Fenced(fence);
        owner.logical_time = retry.deadline().at;
        let fenced = retry.record_fence(&retry.binding(), &owner).unwrap();
        assert_eq!(fenced.last_result(), Some(old_result));
        assert!(fenced.binding().revision > old_result.binding().revision);
        round_trip(&definition, fenced);
        let sealed_owner = OwnerState {
            cohort: Cohort::Sealed {
                cause: ContentHash([73; 32]),
            },
            ..owner_for(&fenced)
        };
        round_trip(
            &definition,
            fenced
                .record_seal(&fenced.binding(), &sealed_owner)
                .unwrap(),
        );
    }
    let cause = ContentHash([74; 32]);
    let owner = OwnerState {
        parent: ParentState::Failed { cause },
        cohort: Cohort::Sealed { cause },
        ..owner_for(&retry)
    };
    let mut sealed = retry.record_seal(&retry.binding(), &owner).unwrap();
    assert_eq!(sealed.last_result(), Some(old_result));
    round_trip(&definition, sealed);
    for verdict in [VerdictValue::Pass, VerdictValue::Error, VerdictValue::Pass] {
        let owner = OwnerState {
            parent: ParentState::Failed { cause },
            cohort: Cohort::Sealed { cause },
            ..owner_for(&sealed)
        };
        let (report, evidence) = report_parts(&sealed, verdict);
        sealed = sealed
            .report(
                Principal::Actor(report.attempt.evaluator),
                &sealed.binding(),
                &owner,
                report,
                &evidence,
            )
            .unwrap()
            .next;
        assert_eq!(sealed.sealed(), Some(cause));
        round_trip(&definition, sealed);
    }
    assert_eq!(sealed.state(), State::Validated);
}

#[test]
fn delivery_missing_target_and_suppressed_rows_restore_without_invented_attempts() {
    let delivery = Declaration::new(
        Principal::Actor(ISSUER),
        DeclarationSpec {
            kind: ValidationKind::Receipt,
            target: TargetDeclaration::Delivery,
            ..specification(ValidationMode::Required, Program::Delivery)
        },
        limits(),
    )
    .unwrap();
    let waiting = ready(&delivery);
    round_trip(&delivery, waiting);
    let received = waiting
        .receive_delivery(
            Principal::Actor(ISSUER),
            &waiting.binding(),
            &owner_for(&waiting),
        )
        .unwrap()
        .next;
    assert!(!received.has_begun());
    assert_eq!(received.last_result().unwrap().phase(), Phase::Delivery);
    round_trip(&delivery, received);
    for mode in [ValidationMode::Required, ValidationMode::Observe] {
        for program in [programmatic(false), agentic()] {
            let definition = declaration(mode, program);
            let ordinary = ready(&definition);
            let missing = Evaluation::materialize(
                Principal::Actor(ISSUER),
                &definition,
                Materialization {
                    binding: ordinary.binding(),
                    target: Target::MissingSlot {
                        response: binding(300),
                        slot: 0,
                    },
                    slot_name: Some("output"),
                    generation: ordinary.generation(),
                    receipt: ordinary.receipt(),
                },
            )
            .unwrap();
            round_trip(&definition, missing);
            let assessed = missing
                .begin(
                    Principal::Actor(ISSUER),
                    &missing.binding(),
                    &owner_for(&missing),
                )
                .unwrap()
                .next;
            assert!(!assessed.has_begun());
            assert_eq!(assessed.attempt_index(), None);
            assert_eq!(assessed.current_phase(), ordinary.current_phase());
            if mode == ValidationMode::Required {
                let result = assessed.last_result().unwrap();
                assert_eq!(result.phase(), Phase::MissingTarget);
                assert_eq!(
                    (result.attempt(), result.evidence(), result.reporter()),
                    (None, None, None)
                );
                let snapshot = result.snapshot_v1();
                refuse_results(
                    &definition,
                    &[
                        (
                            "structural reporter",
                            AcceptedResultSnapshotV1 {
                                reporter: Some(ISSUER),
                                ..snapshot
                            },
                        ),
                        (
                            "structural attempt",
                            AcceptedResultSnapshotV1 {
                                attempt: Some(0),
                                ..snapshot
                            },
                        ),
                    ],
                );
            } else {
                assert_eq!(assessed.suppression(), Some(Suppression::MissingTarget));
                assert!(assessed.last_result().is_none());
            }
            round_trip(&definition, assessed);
        }
    }
    let definition = declaration(ValidationMode::Observe, programmatic(false));
    let waiting = ready(&definition);
    // These are real lower-model suppression values, including historically
    // accepted zero hashes. Snapshot hydration must preserve their reachability.
    let cause = ContentHash([0; 32]);
    for owner in [
        OwnerState {
            parent: ParentState::Failed { cause },
            ..owner_for(&waiting)
        },
        OwnerState {
            readiness: Readiness::ArtifactFailed {
                artifact: binding(400),
                cause,
            },
            ..owner_for(&waiting)
        },
        OwnerState {
            cohort: Cohort::Sealed { cause },
            ..owner_for(&waiting)
        },
    ] {
        let suppressed = waiting
            .begin(
                Principal::Actor(waiting.evaluator().unwrap()),
                &waiting.binding(),
                &owner,
            )
            .unwrap()
            .next;
        assert!(suppressed.suppression().is_some());
        round_trip(&definition, suppressed);
        let owner = OwnerState {
            cohort: Cohort::Sealed {
                cause: ContentHash([75; 32]),
            },
            ..owner_for(&suppressed)
        };
        round_trip(
            &definition,
            suppressed
                .record_seal(&suppressed.binding(), &owner)
                .unwrap(),
        );
    }
    let owner = OwnerState {
        cohort: Cohort::Sealed {
            cause: ContentHash([76; 32]),
        },
        ..owner_for(&waiting)
    };
    round_trip(
        &definition,
        waiting.record_seal(&waiting.binding(), &owner).unwrap(),
    );
}

#[test]
fn malformed_frame_and_handler_cursor_values_are_refused() {
    let definition = declaration(ValidationMode::Required, programmatic(true));
    let active = begin(&definition);
    let row = active.into_state().snapshot_v1().unwrap();
    for binding in [
        binding(101),
        Binding {
            content: ContentHash([99; 32]),
            ..definition.binding()
        },
    ] {
        let other = Declaration::new(
            Principal::Actor(ISSUER),
            DeclarationSpec {
                binding,
                ..specification(ValidationMode::Required, programmatic(true))
            },
            limits(),
        )
        .unwrap();
        refuse_rows(&other, &[("different retained declaration", row)]);
    }
    refuse_rows(
        &definition,
        &[
            (
                "zero generation",
                EvaluationSnapshotV1 {
                    generation: 0,
                    ..row
                },
            ),
            (
                "wrong object",
                EvaluationSnapshotV1 {
                    binding: Binding {
                        object: ObjectId::from_u128(999),
                        ..row.binding
                    },
                    ..row
                },
            ),
            (
                "wrong content",
                EvaluationSnapshotV1 {
                    binding: Binding {
                        content: ContentHash([99; 32]),
                        ..row.binding
                    },
                    ..row
                },
            ),
            (
                "stale revision",
                EvaluationSnapshotV1 {
                    binding: Binding {
                        revision: ObjectRevision(0),
                        ..row.binding
                    },
                    ..row
                },
            ),
            (
                "missing begin revision",
                EvaluationSnapshotV1 {
                    binding: definition.binding(),
                    ..row
                },
            ),
            (
                "missing receipt",
                EvaluationSnapshotV1 {
                    receipt: None,
                    ..row
                },
            ),
            (
                "zero receipt",
                EvaluationSnapshotV1 {
                    receipt: Some(ReceiptFence {
                        receipt: ReceiptId::from_u128(0),
                        epoch: 3,
                    }),
                    ..row
                },
            ),
            (
                "zero epoch",
                EvaluationSnapshotV1 {
                    receipt: Some(ReceiptFence {
                        epoch: 0,
                        ..row.receipt.unwrap()
                    }),
                    ..row
                },
            ),
            (
                "wrong target family",
                EvaluationSnapshotV1 {
                    target: Target::Admission {
                        claim: binding(200),
                    },
                    ..row
                },
            ),
            (
                "wrong slot",
                EvaluationSnapshotV1 {
                    target: Target::Artifact {
                        response: binding(300),
                        slot: 1,
                        artifact: binding(400),
                    },
                    ..row
                },
            ),
            (
                "zero artifact",
                EvaluationSnapshotV1 {
                    target: Target::Artifact {
                        response: binding(300),
                        slot: 0,
                        artifact: binding(0),
                    },
                    ..row
                },
            ),
            (
                "absent handler",
                EvaluationSnapshotV1 {
                    handler: u64::MAX,
                    ..row
                },
            ),
            (
                "exhausted local attempt",
                EvaluationSnapshotV1 {
                    handler_attempt: 2,
                    ..row
                },
            ),
            (
                "global cursor mismatch",
                EvaluationSnapshotV1 { attempt: 1, ..row },
            ),
            (
                "wrong handler offset",
                EvaluationSnapshotV1 { handler: 1, ..row },
            ),
            (
                "unbegun active",
                EvaluationSnapshotV1 {
                    begun: false,
                    ..row
                },
            ),
            (
                "begun ready",
                EvaluationSnapshotV1 {
                    state: State::Ready,
                    ..row
                },
            ),
            (
                "structural phase",
                EvaluationSnapshotV1 {
                    phase: Phase::Delivery,
                    ..row
                },
            ),
            (
                "unproved quality entry",
                EvaluationSnapshotV1 {
                    phase: Phase::Quality,
                    ..row
                },
            ),
            (
                "begun suppression",
                EvaluationSnapshotV1 {
                    suppression: Some(Suppression::ParentFailure(ContentHash([1; 32]))),
                    ..row
                },
            ),
            (
                "zero seal",
                EvaluationSnapshotV1 {
                    sealed: Some(ContentHash([0; 32])),
                    ..row
                },
            ),
            (
                "wrong deadline fence",
                EvaluationSnapshotV1 {
                    fence: Some(AuthorityFence {
                        reason: FenceReason::Deadline(Deadline {
                            at: active.deadline().at + 1,
                            ..active.deadline()
                        }),
                        cause: ContentHash([1; 32]),
                    }),
                    ..row
                },
            ),
        ],
    );
    let quality = report_value(&active, VerdictValue::Pass).next;
    let row = quality.into_state().snapshot_v1().unwrap();
    let other_index = Declaration::new(
        Principal::Actor(ISSUER),
        DeclarationSpec {
            declaration_index: 5,
            ..specification(ValidationMode::Required, programmatic(true))
        },
        limits(),
    )
    .unwrap();
    refuse_rows(
        &other_index,
        &[("different retained declaration index", row)],
    );
    refuse_rows(
        &definition,
        &[
            (
                "quality zero global offset",
                EvaluationSnapshotV1 { attempt: 0, ..row },
            ),
            (
                "quality outside total",
                EvaluationSnapshotV1 {
                    attempt: definition.attempt_bound(),
                    ..row
                },
            ),
            (
                "missing programmatic proof",
                EvaluationSnapshotV1 {
                    programmatic_evidence: None,
                    ..row
                },
            ),
            (
                "entry cannot start mid handler",
                EvaluationSnapshotV1 {
                    handler_attempt: 1,
                    ..row
                },
            ),
            (
                "missing prior pass",
                EvaluationSnapshotV1 {
                    last_result: None,
                    ..row
                },
            ),
        ],
    );
}

#[test]
fn malformed_results_and_nested_history_links_are_refused() {
    let definition = declaration(ValidationMode::Required, programmatic(false));
    let active = begin(&definition);
    let terminal = report_value(&active, VerdictValue::Pass).next;
    let result = terminal.last_result().unwrap().snapshot_v1();
    refuse_results(
        &definition,
        &[
            (
                "wrong claim",
                AcceptedResultSnapshotV1 {
                    claim: ClaimId::from_u128(999),
                    ..result
                },
            ),
            (
                "wrong validation",
                AcceptedResultSnapshotV1 {
                    validation: ValidationId::from_u128(999),
                    ..result
                },
            ),
            (
                "wrong index",
                AcceptedResultSnapshotV1 {
                    declaration_index: result.declaration_index + 1,
                    ..result
                },
            ),
            (
                "wrong mode",
                AcceptedResultSnapshotV1 {
                    mode: ValidationMode::Observe,
                    ..result
                },
            ),
            (
                "wrong ledger",
                AcceptedResultSnapshotV1 {
                    ledger: LedgerId {
                        session: crate::SessionId::from_u128(999),
                        ..result.ledger
                    },
                    ..result
                },
            ),
            (
                "missing reporter",
                AcceptedResultSnapshotV1 {
                    reporter: None,
                    ..result
                },
            ),
            (
                "wrong reporter",
                AcceptedResultSnapshotV1 {
                    reporter: Some(ISSUER),
                    ..result
                },
            ),
            (
                "missing evidence",
                AcceptedResultSnapshotV1 {
                    evidence: None,
                    ..result
                },
            ),
            (
                "missing attempt",
                AcceptedResultSnapshotV1 {
                    attempt: None,
                    ..result
                },
            ),
            (
                "out of range attempt",
                AcceptedResultSnapshotV1 {
                    attempt: Some(definition.attempt_bound()),
                    ..result
                },
            ),
            (
                "wrong state",
                AcceptedResultSnapshotV1 {
                    resulting_state: State::ValidationFailed,
                    ..result
                },
            ),
            (
                "missing programmatic proof",
                AcceptedResultSnapshotV1 {
                    programmatic_evidence: None,
                    ..result
                },
            ),
            (
                "structural result with report fields",
                AcceptedResultSnapshotV1 {
                    phase: Phase::MissingTarget,
                    ..result
                },
            ),
            (
                "insufficient report revision",
                AcceptedResultSnapshotV1 {
                    binding: active.binding(),
                    ..result
                },
            ),
        ],
    );
    let row = terminal.into_state().snapshot_v1().unwrap();
    refuse_rows(
        &definition,
        &[
            (
                "terminal missing result",
                EvaluationSnapshotV1 {
                    last_result: None,
                    ..row
                },
            ),
            (
                "terminal with fence",
                EvaluationSnapshotV1 {
                    fence: Some(AuthorityFence {
                        reason: FenceReason::Evaluation,
                        cause: ContentHash([1; 32]),
                    }),
                    ..row
                },
            ),
            (
                "nested result newer than row",
                EvaluationSnapshotV1 {
                    last_result: Some(AcceptedResultSnapshotV1 {
                        binding: result.binding.next().unwrap(),
                        ..result
                    }),
                    ..row
                },
            ),
            (
                "nested generation mismatch",
                EvaluationSnapshotV1 {
                    last_result: Some(AcceptedResultSnapshotV1 {
                        generation: result.generation + 1,
                        ..result
                    }),
                    ..row
                },
            ),
            (
                "nested receipt mismatch",
                EvaluationSnapshotV1 {
                    last_result: Some(AcceptedResultSnapshotV1 {
                        receipt: Some(ReceiptFence {
                            epoch: 4,
                            ..result.receipt.unwrap()
                        }),
                        ..result
                    }),
                    ..row
                },
            ),
            (
                "nested target mismatch",
                EvaluationSnapshotV1 {
                    last_result: Some(AcceptedResultSnapshotV1 {
                        target: Target::Artifact {
                            response: binding(301),
                            slot: 0,
                            artifact: binding(400),
                        },
                        ..result
                    }),
                    ..row
                },
            ),
            (
                "nested state mismatch",
                EvaluationSnapshotV1 {
                    state: State::ValidationIncomplete,
                    ..row
                },
            ),
        ],
    );
    let retry = report_value(&active, VerdictValue::Error).next;
    let retry_result = retry.last_result().unwrap().snapshot_v1();
    refuse_results(
        &definition,
        &[(
            "premature terminal error",
            AcceptedResultSnapshotV1 {
                resulting_state: State::Errored,
                ..retry_result
            },
        )],
    );
    let row = retry.into_state().snapshot_v1().unwrap();
    refuse_rows(
        &definition,
        &[(
            "retry result skips attempt",
            EvaluationSnapshotV1 {
                last_result: Some(AcceptedResultSnapshotV1 {
                    attempt: Some(1),
                    ..retry_result
                }),
                ..row
            },
        )],
    );
    let fallback = report_value(&retry, VerdictValue::Error).next;
    let exhausted = report_value(&fallback, VerdictValue::Error).next;
    let result = exhausted.last_result().unwrap().snapshot_v1();
    refuse_results(
        &definition,
        &[(
            "retry after exhaustion",
            AcceptedResultSnapshotV1 {
                resulting_state: State::Validating,
                ..result
            },
        )],
    );
}

#[test]
fn exact_hydration_quotes_cover_handler_target_families_without_allocations() {
    for target in [
        TargetDeclaration::WholeWorkSlot {
            index: 0,
            name: "output",
        },
        TargetDeclaration::Admission,
        TargetDeclaration::Increment,
    ] {
        let definition = Declaration::new(
            Principal::Actor(ISSUER),
            DeclarationSpec {
                target,
                phase: match target {
                    TargetDeclaration::WholeWorkSlot { .. } | TargetDeclaration::Delivery => {
                        ValidationPhase::WholeWork
                    }
                    TargetDeclaration::Admission => ValidationPhase::Admission,
                    TargetDeclaration::Increment => ValidationPhase::Increment,
                },
                ..specification(ValidationMode::Required, programmatic(true))
            },
            limits(),
        )
        .unwrap();
        let initial = ready(&definition);
        let active = begin(&definition);
        let quality = report_value(&active, VerdictValue::Pass).next;
        let terminal = report_value(&quality, VerdictValue::Fail).next;
        let evaluation_visits = EvaluationState::hydration_visits(&definition).unwrap();
        let result_visits = AcceptedResult::hydration_visits(&definition).unwrap();
        memory::fail_after(0, || {
            for evaluation in [initial, active, quality, terminal] {
                let row = evaluation.into_state().snapshot_v1().unwrap();
                assert_eq!(
                    EvaluationState::hydrate_v1(&definition, row, evaluation_visits).unwrap(),
                    evaluation.into_state()
                );
                assert_eq!(
                    EvaluationState::hydrate_v1(&definition, row, evaluation_visits - 1),
                    Err(ContractError::Capacity)
                );
                if let Some(result) = evaluation.last_result() {
                    assert_eq!(
                        AcceptedResult::hydrate_v1(
                            &definition,
                            result.snapshot_v1(),
                            result_visits
                        )
                        .unwrap(),
                        result
                    );
                    assert_eq!(
                        AcceptedResult::hydrate_v1(
                            &definition,
                            result.snapshot_v1(),
                            result_visits - 1
                        ),
                        Err(ContractError::Capacity)
                    );
                }
            }
            assert_eq!(memory::remaining_allocations(), Some(0));
        });
    }
}
