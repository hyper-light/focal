use super::*;
use crate::native::{
    object_journal,
    prepare::{Extras, Scratch},
};

impl object_journal::Source for NativeView<'_> {
    fn claim(&self, id: ClaimId) -> Option<&ClaimState> {
        self.claim(id)
    }
    fn definition(&self, id: ValidationId) -> Option<&validation::Declaration> {
        self.definition(id)
    }
    fn work(&self, id: ArtifactId) -> Option<&NativeWork> {
        self.work(id)
    }
    fn response(&self, id: TestamentId) -> Option<&NativeResponseRecord> {
        self.response_record(id)
    }
    fn evaluation(&self, key: EvaluationKey) -> Option<&validation::EvaluationState> {
        self.evaluation(key)
    }
    fn missing(&self, key: NativeResultKey) -> Option<&NativeMissingResult> {
        self.missing_result(key)
    }
    fn artifact(&self, id: ArtifactId) -> Option<&NativeArtifact> {
        self.artifact(id)
    }
    fn accepted(&self, key: NativeResultKey) -> Option<&NativeAccepted> {
        self.result(key)
    }
}

fn staged(missing: bool) -> (Fixture, NativeOutcome, Extras) {
    staged_with_mode(missing, ValidationMode::Required)
}

fn staged_with_mode(missing: bool, mode: ValidationMode) -> (Fixture, NativeOutcome, Extras) {
    let mut f = checked_slot_fixture_with_mode(mode);
    if missing {
        f.commit(SUBJECT, f.close(900, OutcomeKind::Complete, vec![], vec![]));
        deliver(&mut f, 900);
    } else {
        complete_response(&mut f, 900, 801);
    }
    let command = enter(&f, 900);
    let (outcome, extras) = stage_original(&mut f, ISSUER, command, 2048);
    (f, outcome, extras)
}

// Keep adversarial object checks scoped to the original immutable plan. Replay
// the same typed command against the actual borrowed source, rather than trying
// to rewind an evaluation after the candidate's automatic seal suffix.
fn copy_command(command: &NativeCommand) -> NativeCommand {
    match command {
        NativeCommand::EnterWholeWork { claim, expected } => NativeCommand::EnterWholeWork {
            claim: *claim,
            expected: *expected,
        },
        NativeCommand::BeginWork {
            claim,
            key,
            expected,
        } => NativeCommand::BeginWork {
            claim: *claim,
            key: *key,
            expected: *expected,
        },
        NativeCommand::ReportWork {
            claim,
            key,
            expected,
            report,
            artifact,
        } => {
            let descriptor = artifact.get().unwrap();
            NativeCommand::ReportWork {
                claim: *claim,
                key: *key,
                expected: *expected,
                report: *report,
                artifact: NativeArtifactInput::new(
                    descriptor
                        .try_copy(descriptor.retained_bytes().unwrap())
                        .unwrap(),
                )
                .unwrap(),
            }
        }
        _ => panic!("object-journal fixture command"),
    }
}

fn stage_original(
    f: &mut Fixture,
    actor: ParticipantId,
    command: NativeCommand,
    visits: usize,
) -> (NativeOutcome, Extras) {
    let copied = copy_command(&command);
    let NativeStaging::Prepared { candidate, outcome } = f.stage(actor, command).unwrap() else {
        panic!("fresh object transaction");
    };
    let source = f.owner.committed();
    let view = source.source_view();
    let NativeInvocation::Request(request) = outcome.invocation else {
        panic!("actor fixture");
    };
    let input = NativeInput {
        request,
        command: copied,
    };
    assert_eq!(
        intent::fingerprint(source.ledger(), &input).unwrap(),
        outcome.intent
    );
    assert_eq!(source.sequence().0 + 1, outcome.sequence.0);
    let evidence = if let NativeCommand::ReportWork { artifact, .. } = &input.command {
        Some(
            f.store
                .verify_native_artifact(
                    request,
                    artifact.get().unwrap(),
                    ContentDomainId::from_u128(93),
                    &view.state.budget,
                    &BuiltinNativeSchemas,
                )
                .unwrap(),
        )
    } else {
        None
    };
    let limits = NativeLimits {
        plan_nodes: 16,
        plan_edges: visits,
        preparation_bytes: 1024 * 1024,
        evaluations_per_claim: 32,
        range: RangeConfig {
            max_batch_entries: 128,
            page_entries: 4,
            ..RangeConfig::default()
        },
        ..NativeLimits::default()
    };
    let mut meta = view.meta();
    meta.logical_time = outcome.logical_time;
    meta.outcomes += 1;
    let mut scratch = Scratch {
        used: 0,
        max: limits.preparation_bytes,
    };
    let mut extras = Extras::new(limits.range.max_batch_entries, limits.preparation_bytes).unwrap();
    let plan = transactions::prepare(
        input.command,
        request,
        evidence.as_ref(),
        context(actor, outcome.logical_time),
        focal_model::lifecycle::claim::ClaimCut {
            position: outcome.sequence,
            cause: outcome.intent,
        },
        view,
        limits,
        &mut meta,
        &mut extras,
        &mut scratch,
    )
    .unwrap();
    let original = extras.journal.as_ref().unwrap();
    let original_events = u32::try_from(original.len()).unwrap();
    let actual = f.owner.candidate(candidate).unwrap();
    for (ordinal, fact) in original.iter().enumerate() {
        let event = actual
            .event(outcome.sequence, u32::try_from(ordinal).unwrap())
            .unwrap();
        assert_eq!(event.invocation, outcome.invocation);
        assert_eq!(event.fact, *fact, "original event {ordinal}");
    }
    let mut evaluation_keys = extras
        .rows
        .iter()
        .filter_map(|extra| match extra.key {
            Key::Evaluation(key) => Some(key),
            _ => None,
        })
        .collect::<std::collections::BTreeSet<_>>();
    let original_evaluations = u32::try_from(evaluation_keys.len()).unwrap();
    let mut registrations = false;
    let mut sealed_keys = std::collections::BTreeSet::new();
    let mut sealed_claims = std::collections::BTreeSet::new();
    for ordinal in original_events..outcome.events {
        let event = actual.event(outcome.sequence, ordinal).unwrap();
        assert_eq!(
            (event.invocation, event.sequence, event.ordinal),
            (outcome.invocation, outcome.sequence, ordinal)
        );
        match event.fact {
            NativeFact::Evaluation {
                kind: NativeEvaluationEventKind::Sealed,
                key,
                before: Some(before),
                after,
                ..
            } => {
                assert!(
                    !registrations,
                    "seal evaluations precede registry suffix events"
                );
                assert!(sealed_keys.insert(key));
                evaluation_keys.insert(key);
                let previous = extras
                    .rows
                    .iter()
                    .find_map(|extra| {
                        if extra.key != Key::Evaluation(key) {
                            return None;
                        }
                        match &extra.row {
                            Row::Evaluation(row) => row.get(),
                            _ => None,
                        }
                    })
                    .or_else(|| source.evaluation(key))
                    .unwrap();
                let token = previous
                    .seal_claim(
                        source.definition(key.validation).unwrap(),
                        &previous.binding(),
                        actual.claim(key.claim).unwrap(),
                    )
                    .unwrap();
                assert!(token.changed());
                assert_eq!(token.before(), before);
                assert_eq!(token.next().binding(), after);
                assert_eq!(actual.evaluation(key), Some(&token.next()));
            }
            NativeFact::Registrations { claim } => {
                registrations = true;
                let id = ClaimId(claim.object.0);
                assert!(sealed_claims.insert(id));
                assert_eq!(actual.claim(id).unwrap().binding(), claim);
                assert_eq!(source.claim(id).unwrap().local_sealed_at(), None);
                assert_eq!(
                    actual.claim(id).unwrap().local_sealed_at(),
                    Some(outcome.sequence)
                );
                assert!(actual.registrations(id).unwrap().is_sealed());
            }
            _ => panic!("unexpected appended object-journal fact"),
        }
    }
    assert_eq!(
        outcome.events,
        original_events + u32::try_from(sealed_keys.len() + sealed_claims.len()).unwrap()
    );
    assert_eq!(
        outcome.evaluations,
        u32::try_from(evaluation_keys.len()).unwrap()
    );
    assert_eq!(outcome.changed, u32::try_from(plan.rows.len()).unwrap());
    // The checker below receives the original event and row counts. Accepted
    // and missing-result positions still use their actual original coordinates.
    let original_outcome = NativeOutcome {
        events: original_events,
        evaluations: original_evaluations,
        ..outcome
    };
    (original_outcome, extras)
}

#[test]
fn work_entry_cannot_precede_response_or_claim_entry() {
    let (f, outcome, mut extras) = staged(false);
    checked(&f, outcome, &mut extras).unwrap();
    let journal = extras.journal.as_mut().unwrap();
    let claim = journal
        .iter()
        .position(|fact| {
            matches!(fact,
        NativeFact::Claim(event) if event.kind == NativeEventKind::Validating)
        })
        .unwrap();
    let work = journal
        .iter()
        .position(|fact| {
            matches!(
                fact,
                NativeFact::Work {
                    state: WorkArtifactState::Validating,
                    ..
                }
            )
        })
        .unwrap();
    journal.swap(claim, work);
    assert!(checked(&f, outcome, &mut extras).is_err());
}

#[test]
fn observe_missing_requires_entry_order_even_without_an_accepted_result() {
    let (f, outcome, mut extras) = staged_with_mode(true, ValidationMode::Observe);
    assert!(
        !extras
            .rows
            .iter()
            .any(|extra| matches!(extra.key, Key::MissingResult(_)))
    );
    checked(&f, outcome, &mut extras).unwrap();
    let journal = extras.journal.as_mut().unwrap();
    let claim = journal
        .iter()
        .position(|fact| {
            matches!(fact,
        NativeFact::Claim(event) if event.kind == NativeEventKind::Validating)
        })
        .unwrap();
    let evaluation = journal
        .iter()
        .position(|fact| {
            matches!(
                fact,
                NativeFact::Evaluation {
                    kind: NativeEvaluationEventKind::MissingTarget,
                    ..
                }
            )
        })
        .unwrap();
    journal.swap(claim, evaluation);
    assert!(checked(&f, outcome, &mut extras).is_err());
}

fn checked(f: &Fixture, outcome: NativeOutcome, extras: &mut Extras) -> Result<(), NativeError> {
    object_journal::check(
        extras,
        &f.owner.committed(),
        outcome.operation,
        outcome.sequence,
        NativeLimits {
            plan_edges: 2048,
            ..NativeLimits::default()
        },
        &mut Scratch {
            used: 0,
            max: 1024 * 1024,
        },
    )
}

#[test]
fn actual_entry_journal_matches_every_final_row_and_missing_publication() {
    for missing in [false, true] {
        let (f, outcome, mut extras) = staged(missing);
        checked(&f, outcome, &mut extras).unwrap();
        assert_eq!(
            f.owner.committed().response(RESPONSE).unwrap().state(),
            ResponseState::Received
        );
    }
}

#[test]
fn missing_or_duplicated_object_events_and_unjournaled_rows_are_refused() {
    for mutation in 0..5 {
        let (f, outcome, mut extras) = staged(false);
        let journal = extras.journal.as_mut().unwrap();
        let at = journal
            .iter()
            .position(|fact| matches!(fact, NativeFact::Work { .. }))
            .unwrap();
        match mutation {
            0 => {
                journal.remove(at);
            }
            1 => {
                let fact = journal[at];
                journal.insert(at, fact);
            }
            2 => {
                if let NativeFact::Work { before, .. } = &mut journal[at] {
                    *before = None;
                }
            }
            3 => {
                if let NativeFact::Work { after, .. } = &mut journal[at] {
                    after.content = ContentHash([99; 32]);
                }
            }
            4 => {
                extras
                    .rows
                    .retain(|extra| !matches!(extra.key, Key::Work(_)));
            }
            _ => unreachable!(),
        }
        assert!(
            checked(&f, outcome, &mut extras).is_err(),
            "mutation {mutation}"
        );
    }
}

#[test]
fn missing_result_requires_exact_row_evaluation_and_publication_order() {
    for mutation in 0..6 {
        let (f, outcome, mut extras) = staged(true);
        match mutation {
            0 => extras
                .rows
                .retain(|extra| !matches!(extra.key, Key::MissingResult(_))),
            1 => extras
                .rows
                .retain(|extra| !matches!(extra.key, Key::Evaluation(_))),
            2 => extras
                .journal
                .as_mut()
                .unwrap()
                .retain(|fact| !matches!(fact, NativeFact::Missing { .. })),
            3 => {
                let extra = extras
                    .rows
                    .iter_mut()
                    .find(|extra| matches!(extra.key, Key::MissingResult(_)))
                    .unwrap();
                let Row::MissingResult(row) = &extra.row else {
                    panic!("missing");
                };
                let old = *row.get().unwrap();
                let changed =
                    NativeMissingResult::new(old.result(), old.sequence(), old.ordinal() + 1)
                        .unwrap();
                extra.row = Row::MissingResult(OwnedMissingResult::new(changed).unwrap());
            }
            4 => {
                let journal = extras.journal.as_mut().unwrap();
                let at = journal
                    .iter()
                    .position(|fact| matches!(fact, NativeFact::Missing { .. }))
                    .unwrap();
                let before = journal
                    .iter()
                    .position(|fact| matches!(fact, NativeFact::Evaluation { .. }))
                    .unwrap();
                journal.swap(at, before);
            }
            5 => {
                let extra = extras
                    .rows
                    .iter_mut()
                    .find(|extra| matches!(extra.key, Key::Evaluation(_)))
                    .unwrap();
                let Key::Evaluation(key) = extra.key else {
                    panic!("evaluation");
                };
                extra.row = Row::Evaluation(
                    OwnedEvaluation::new(*f.owner.committed().evaluation(key).unwrap()).unwrap(),
                );
            }
            _ => unreachable!(),
        }
        assert!(
            checked(&f, outcome, &mut extras).is_err(),
            "mutation {mutation}"
        );
    }
}

#[test]
fn response_entry_position_cannot_shift_or_skip_a_revision() {
    for mutation in 0..3 {
        let (f, outcome, mut extras) = staged(false);
        let journal = extras.journal.as_mut().unwrap();
        let at = journal
            .iter()
            .position(|fact| {
                matches!(
                    fact,
                    NativeFact::Response {
                        state: ResponseState::Validating,
                        ..
                    }
                )
            })
            .unwrap();
        match mutation {
            0 => {
                journal.remove(at);
            }
            1 => {
                let fact = journal.remove(at);
                journal.push(fact);
            }
            2 => {
                if let NativeFact::Response { after, .. } = &mut journal[at] {
                    after.revision.0 += 1;
                }
            }
            _ => unreachable!(),
        }
        assert!(
            checked(&f, outcome, &mut extras).is_err(),
            "mutation {mutation}"
        );
    }
}

#[test]
fn journal_index_requires_precharged_bytes_and_bounded_events() {
    let (f, outcome, mut extras) = staged(false);
    let source = f.owner.committed();
    assert!(
        object_journal::check(
            &mut extras,
            &source,
            outcome.operation,
            outcome.sequence,
            NativeLimits::default(),
            &mut Scratch { used: 0, max: 0 }
        )
        .is_err()
    );
    assert!(
        object_journal::check(
            &mut extras,
            &source,
            outcome.operation,
            outcome.sequence,
            NativeLimits {
                plan_edges: 0,
                ..NativeLimits::default()
            },
            &mut Scratch {
                used: 0,
                max: usize::MAX
            }
        )
        .is_err()
    );
    checked(&f, outcome, &mut extras).unwrap();
}

fn work_key() -> EvaluationKey {
    EvaluationKey {
        claim: CLAIM,
        validation: ValidationId::from_u128(301),
        target: EvaluationTarget::Work {
            response: RESPONSE,
            slot: 0,
            artifact: ArtifactId::from_u128(801),
        },
        generation: 1,
    }
}

fn work_begin(f: &Fixture) -> (ParticipantId, NativeCommand) {
    let key = work_key();
    let view = f.owner.committed();
    let old = view.evaluation(key).unwrap();
    let actor = old
        .bind(view.definition(key.validation).unwrap())
        .unwrap()
        .evaluator()
        .unwrap();
    (
        actor,
        NativeCommand::BeginWork {
            claim: f.claim(),
            key,
            expected: old.binding(),
        },
    )
}

fn staged_begin(already_entered: bool) -> (Fixture, NativeOutcome, Extras) {
    let mut f = checked_slot_fixture_with_visits(ValidationMode::Required, 64 * 1024);
    complete_response(&mut f, 900, 801);
    if already_entered {
        f.commit(ISSUER, enter(&f, 900));
    }
    let (actor, command) = work_begin(&f);
    let (outcome, extras) = stage_original(&mut f, actor, command, 64 * 1024);
    (f, outcome, extras)
}

fn staged_report(mode: ValidationMode, value: VerdictValue) -> (Fixture, NativeOutcome, Extras) {
    let mut f = checked_slot_fixture_with_visits(mode, 64 * 1024);
    complete_response(&mut f, 900, 801);
    let (actor, command) = work_begin(&f);
    f.commit(actor, command);
    let key = work_key();
    let view = f.owner.committed();
    let state = view.evaluation(key).unwrap();
    let attempt = state
        .bind(view.definition(key.validation).unwrap())
        .unwrap()
        .current_attempt()
        .unwrap();
    let mut spec = crate::native::report_tests::artifact_spec(1801, attempt.evaluator, value);
    spec.receipt = state.receipt();
    spec.visibility = &[];
    spec.result = Some(
        focal_model::lifecycle::artifact_descriptor::ResultProvenance {
            claim: key.claim,
            validation: key.validation,
            target: state.target(),
            generation: state.generation(),
            attempt,
            value,
        },
    );
    let artifact = descriptor(spec);
    let command = NativeCommand::ReportWork {
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
    };
    let (outcome, extras) = stage_original(&mut f, attempt.evaluator, command, 64 * 1024);
    (f, outcome, extras)
}

#[test]
fn actual_begin_journals_cover_first_entry_and_existing_entry() {
    for already_entered in [false, true] {
        let (f, outcome, mut extras) = staged_begin(already_entered);
        checked(&f, outcome, &mut extras).unwrap();
        assert!(matches!(
            extras.journal.as_ref().unwrap().first(),
            Some(NativeFact::Evaluation {
                kind: NativeEvaluationEventKind::Begun,
                attempt: Some(_),
                ..
            })
        ));
        if already_entered {
            assert_eq!(extras.rows.len(), 1);
            assert_eq!(extras.journal.as_ref().unwrap().len(), 1);
            assert!(
                f.owner
                    .committed()
                    .response_record(RESPONSE)
                    .unwrap()
                    .entered()
                    .unwrap()
                    .sequence
                    < outcome.sequence
            );
        }
    }
}

#[test]
fn begun_history_requires_its_actual_attempt_and_final_row() {
    for mutation in 0..5 {
        let (f, outcome, mut extras) = staged_begin(false);
        match mutation {
            0 => {
                extras.journal.as_mut().unwrap().remove(0);
            }
            1 => {
                extras
                    .rows
                    .retain(|extra| !matches!(extra.key, Key::Evaluation(_)));
            }
            2 => {
                extras.journal.as_mut().unwrap().swap(0, 1);
            }
            3 => {
                if let NativeFact::Evaluation {
                    attempt: Some(attempt),
                    ..
                } = &mut extras.journal.as_mut().unwrap()[0]
                {
                    attempt.index += 1;
                }
            }
            4 => {
                if let NativeFact::Evaluation { kind, .. } =
                    &mut extras.journal.as_mut().unwrap()[0]
                {
                    *kind = NativeEvaluationEventKind::Reported;
                }
            }
            _ => unreachable!(),
        }
        assert!(
            checked(&f, outcome, &mut extras).is_err(),
            "mutation {mutation}"
        );
    }
}

#[test]
fn actual_reports_preserve_original_acceptance_coordinates_and_retry_history() {
    for value in [VerdictValue::Pass, VerdictValue::Error, VerdictValue::Fail] {
        let (f, outcome, mut extras) = staged_report(ValidationMode::Required, value);
        checked(&f, outcome, &mut extras).unwrap();
        let journal = extras.journal.as_ref().unwrap();
        assert!(matches!(journal[0], NativeFact::Artifact { .. }));
        assert!(matches!(
            journal[1],
            NativeFact::Evaluation {
                kind: NativeEvaluationEventKind::Reported,
                ..
            }
        ));
        let NativeFact::Accepted { key } = journal[2] else {
            panic!("accepted");
        };
        let result = extras
            .rows
            .iter()
            .find_map(|extra| match &extra.row {
                Row::Accepted(row) => row.get(),
                _ => None,
            })
            .unwrap();
        assert_eq!((result.sequence(), result.ordinal()), (outcome.sequence, 2));
        assert_eq!(NativeResultKey::of(result.result()), key);
        assert_eq!(result.result().verdict(), value);
    }
}

#[test]
fn late_observe_report_requires_no_rewritten_terminal_work_response_or_claim() {
    let (f, outcome, mut extras) = staged_report(ValidationMode::Observe, VerdictValue::Pass);
    assert_eq!(
        f.owner.committed().claim(CLAIM).unwrap().status(),
        ClaimStatus::Satisfied
    );
    assert_eq!(
        f.owner.committed().response(RESPONSE).unwrap().state(),
        ResponseState::Validated
    );
    assert_eq!(
        f.owner
            .committed()
            .work(ArtifactId::from_u128(801))
            .unwrap()
            .state
            .state(),
        WorkArtifactState::Validated
    );
    assert_eq!(extras.rows.len(), 4);
    assert_eq!(extras.journal.as_ref().unwrap().len(), 3);
    checked(&f, outcome, &mut extras).unwrap();
}

#[test]
fn report_journal_refuses_omitted_reordered_or_substituted_evidence() {
    for mutation in 0..10 {
        let (f, outcome, mut extras) = staged_report(ValidationMode::Required, VerdictValue::Pass);
        match mutation {
            0 => extras
                .rows
                .retain(|extra| !matches!(extra.key, Key::Artifact(_))),
            1 => extras
                .rows
                .retain(|extra| !matches!(extra.key, Key::ArtifactIdentity(_))),
            2 => extras
                .rows
                .retain(|extra| !matches!(extra.key, Key::Accepted(_))),
            3 => extras
                .rows
                .retain(|extra| !matches!(extra.key, Key::Evaluation(_))),
            4 => extras.journal.as_mut().unwrap().swap(0, 1),
            5 => extras.journal.as_mut().unwrap().swap(1, 2),
            6 => {
                let extra = extras
                    .rows
                    .iter_mut()
                    .find(|extra| matches!(extra.key, Key::Accepted(_)))
                    .unwrap();
                let Row::Accepted(row) = &extra.row else {
                    panic!("accepted");
                };
                let old = *row.get().unwrap();
                let changed = NativeAccepted::new(
                    old.result(),
                    old.attempt(),
                    old.artifact(),
                    old.sequence(),
                    3,
                )
                .unwrap();
                extra.row = Row::Accepted(OwnedAccepted::new(changed).unwrap());
            }
            7 => {
                if let NativeFact::Evaluation {
                    attempt: Some(attempt),
                    ..
                } = &mut extras.journal.as_mut().unwrap()[1]
                {
                    attempt.definition = ContentHash([99; 32]);
                }
            }
            8 => {
                let extra = extras
                    .rows
                    .iter_mut()
                    .find(|extra| matches!(extra.key, Key::Evaluation(_)))
                    .unwrap();
                extra.row = Row::Evaluation(
                    OwnedEvaluation::new(*f.owner.committed().evaluation(work_key()).unwrap())
                        .unwrap(),
                );
            }
            9 => {
                let extra = extras
                    .rows
                    .iter_mut()
                    .find(|extra| matches!(extra.key, Key::ArtifactIdentity(_)))
                    .unwrap();
                extra.row = Row::ArtifactIdentity(ArtifactId::from_u128(9999));
            }
            _ => unreachable!(),
        }
        assert!(
            checked(&f, outcome, &mut extras).is_err(),
            "mutation {mutation}"
        );
    }
}

#[test]
fn first_entry_cannot_omit_both_a_consequence_row_and_its_history() {
    for missing in [false, true] {
        let (f, outcome, mut extras) = staged(missing);
        if missing {
            extras
                .rows
                .retain(|extra| !matches!(extra.key, Key::Evaluation(_) | Key::MissingResult(_)));
            extras.journal.as_mut().unwrap().retain(|fact| {
                !matches!(
                    fact,
                    NativeFact::Evaluation { .. } | NativeFact::Missing { .. }
                )
            });
        } else {
            let id = ArtifactId::from_u128(801);
            extras.rows.retain(|extra| extra.key != Key::Work(id));
            extras.journal.as_mut().unwrap().retain(
                |fact| !matches!(fact, NativeFact::Work { after, .. } if after.object.0 == id.0),
            );
        }
        assert!(checked(&f, outcome, &mut extras).is_err());
    }
}
