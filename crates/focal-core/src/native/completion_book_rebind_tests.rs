//! Collector/accounting fixtures only. Native automatic cohort construction is
//! deliberately not supplied by these tests: each added row is an actual model
//! seal transition over a native report's checked claim and registered state.
use super::*;
use crate::native::prepare::{Extras, Scratch};
use focal_memory::{Change as RangeChange, Entry};
use focal_model::lifecycle::claim::ClaimCut;
use focal_model::{ClaimStatus, VerdictValue};

#[path = "completion_composition_tests.rs"]
mod composition_tests;

fn copy_row(row: &Row) -> Result<Row, MemoryError> {
    match row {
        Row::Meta(value) => Ok(Row::Meta(*value)),
        Row::Claim(value) => value.copy().map(Row::Claim),
        Row::Definition(value) => value.copy().map(Row::Definition),
        Row::Evaluation(value) => value.copy().map(Row::Evaluation),
        Row::Artifact(value) => value.copy().map(Row::Artifact),
        Row::ArtifactIdentity(value) => Ok(Row::ArtifactIdentity(*value)),
        Row::Accepted(value) => value.copy().map(Row::Accepted),
        Row::Outcome(value) => Ok(Row::Outcome(*value)),
        Row::Event(value) => value.copy().map(Row::Event),
        Row::Index => Ok(Row::Index),
        _ => panic!("unexpected row in Admission-only book fixture"),
    }
}

fn input(
    core: &Core<NativeState>,
    tail: Option<&NativePrepared>,
    id: u128,
    index: u32,
    verdict: VerdictValue,
) -> NativeInput {
    let view = View {
        state: &core.state,
        tail,
    };
    let key = fixture::key(index);
    let actor = view
        .evaluation(key)
        .unwrap()
        .bind(view.definition(key.validation).unwrap())
        .unwrap()
        .current_attempt()
        .unwrap()
        .evaluator;
    fixture::report_for(
        core,
        tail,
        id,
        index,
        verdict,
        fixture::descriptor(fixture::artifact_spec(id, actor, verdict)),
    )
}

fn with_seals(
    core: &Core<NativeState>,
    tail: Option<&NativePrepared>,
    input: NativeInput,
    custody: &focal_evidence::VerifiedNativeArtifact,
    members: &[u32],
) -> (
    NativePrepared,
    ReportAdvance,
    Vec<validation::SealTransition>,
) {
    let pending = tail.into_iter().collect::<Vec<_>>();
    let ordinary = fixture::report(core, fixture::copy_report(&input), &pending, custody);
    let mut base_outcome = ordinary.outcome();
    drop(ordinary);
    let view = View {
        state: &core.state,
        tail,
    };
    let NativeCommand::ReportAdmission { key, expected, .. } = &input.command else {
        panic!("Admission fixture")
    };
    let key = *key;
    let before = *expected;
    let parent =
        crate::native::completion_envelope::ReportParent::capture(view.claim(key.claim).unwrap());
    let mut meta = view.meta();
    meta.logical_time = base_outcome.logical_time;
    meta.outcomes += 1;
    let mut scratch = Scratch {
        used: 0,
        max: core.limits.preparation_bytes,
    };
    let mut extras = Extras::new(32, core.limits.preparation_bytes).unwrap();
    let plan = transactions::prepare(
        input.command,
        input.request,
        Some(custody),
        fixture::context(input.request.principal, 100),
        ClaimCut {
            position: base_outcome.sequence,
            cause: base_outcome.intent,
        },
        &view,
        core.limits,
        &mut meta,
        &mut extras,
        &mut scratch,
    )
    .unwrap();
    // This collector fixture deliberately controls its own seal suffix. Recover
    // counts from the actual original transaction, before adding those seals;
    // ordinary preparation now includes the automatic production suffix.
    base_outcome.events = u32::try_from(if extras.journal.is_some() {
        extras.events()
    } else {
        claim_changes::event_count(&plan.rows, &view, base_outcome.operation).unwrap()
            + extras.events()
    })
    .unwrap();
    base_outcome.evaluations = u32::try_from(
        extras
            .rows
            .iter()
            .filter(|row| matches!(row.key, Key::Evaluation(_)))
            .count(),
    )
    .unwrap();
    let changed_claim = plan
        .rows
        .iter()
        .find(|row| row.binding().object.0 == key.claim.0);
    let usage = parent
        .completion_use_prepared(
            NativeOperation::ReportAdmission,
            changed_claim,
            extras
                .admission_graph
                .as_ref()
                .map(crate::native::admission_graph::Proof::event),
        )
        .unwrap();
    let claim = changed_claim.unwrap_or_else(|| view.claim(key.claim).unwrap());
    let mut seals = members
        .iter()
        .map(|&index| {
            let key = fixture::key(index);
            let state = extras
                .rows
                .iter()
                .find(|row| row.key == Key::Evaluation(key))
                .and_then(|row| as_evaluation(Some(&row.row)))
                .unwrap_or_else(|| view.evaluation(key).unwrap());
            let seal = state
                .seal_claim(
                    view.definition(key.validation).unwrap(),
                    &state.binding(),
                    claim,
                )
                .unwrap();
            assert!(seal.changed());
            seal
        })
        .collect::<Vec<_>>();
    meta.events += usize::try_from(base_outcome.events).unwrap();
    let mut changes = claim_changes::changes(
        plan,
        extras,
        meta,
        base_outcome,
        &view,
        core.limits,
        core.limits.preparation_bytes,
        &mut scratch,
    )
    .unwrap();
    let mut outcome = base_outcome;
    for seal in &seals {
        let state = seal.next();
        let key = EvaluationKey::of(key.claim, &state);
        let row = OwnedEvaluation::new(state).unwrap();
        let heap = row.heap_charge().unwrap();
        let replacement =
            RangeChange::Put(Entry::new(Key::Evaluation(key), Row::Evaluation(row), heap));
        match changes
            .iter_mut()
            .find(|row| *row.key() == Key::Evaluation(key))
        {
            Some(existing) => *existing = replacement,
            None => {
                changes.push(replacement);
                outcome.evaluations += 1;
            }
        }
        let evaluation = state
            .bind(view.definition(key.validation).unwrap())
            .unwrap();
        let event = OwnedEvent::new(
            StoredEvent::pack(NativeEvent {
                invocation: outcome.invocation,
                sequence: outcome.sequence,
                ordinal: outcome.events,
                fact: NativeFact::Evaluation {
                    kind: NativeEvaluationEventKind::Sealed,
                    key,
                    before: Some(seal.before()),
                    after: state.binding(),
                    state: state.state(),
                    phase: state.phase(),
                    attempt: state
                        .has_begun()
                        .then(|| evaluation.current_attempt().unwrap()),
                    fence: state.fence(),
                },
            })
            .unwrap(),
        )
        .unwrap();
        let heap = event.heap_charge().unwrap();
        changes.push(RangeChange::Put(Entry::new(
            Key::Event(outcome.sequence, outcome.events),
            Row::Event(event),
            heap,
        )));
        outcome.events += 1;
        meta.events += 1;
    }
    for change in &mut changes {
        if let RangeChange::Put(entry) = change {
            match &mut entry.value {
                Row::Outcome(value) => *value = outcome,
                Row::Meta(value) => *value = meta,
                _ => {}
            }
        }
    }
    let range = match tail {
        Some(tail) => core.state.rows.plan_after(
            &core.state.budget,
            &tail.fragments,
            outcome.sequence.0,
            changes,
            BudgetLane::Completion,
            usize::MAX,
        ),
        None => core.state.rows.plan_batch(
            &core.state.budget,
            outcome.sequence.0,
            changes,
            BudgetLane::Completion,
            usize::MAX,
        ),
    }
    .unwrap()
    .build_in_with(&core.state.budget, copy_row)
    .unwrap();
    // Events above retain their original member order. This fixture has distinct
    // Admission definitions, so the complete before binding orders its tokens.
    assert!(seals.iter().all(|seal| matches!(
        seal.previous().target(),
        validation::Target::Admission { .. }
    )));
    seals.sort_unstable_by_key(|seal| {
        let binding = seal.before();
        (
            binding.ledger,
            binding.object,
            binding.content,
            binding.revision,
        )
    });
    assert!(
        seals
            .windows(2)
            .all(|pair| pair[0].before().object != pair[1].before().object)
    );
    (
        NativePrepared {
            fragments: range,
            outcome,
            writes: crate::native::mutation::WriteSet::unrecorded(),
        },
        ReportAdvance { key, before, usage },
        seals,
    )
}

fn installed(core: &Core<NativeState>, source: &MemoryBudget, count: u32) -> CompletionBook {
    let mut book = CompletionBook::new(source, core.limits).unwrap();
    for index in 1..=count {
        let journal = install(&mut book, core, source, index);
        book.commit(journal).unwrap();
    }
    book
}

#[test]
fn mixed_retirement_and_live_seals_commit_after_a_younger_report_and_rollback_exactly() {
    let mut core = fixture::running(&[
        (ValidationMode::Required, false),
        (ValidationMode::Observe, true),
        (ValidationMode::Observe, false),
    ]);
    let source = source();
    let mut book = installed(&core, &source, 3);
    let original_totals = book.totals;
    let original_credit = book.grant(fixture::key(2)).unwrap().credit;
    let original_weight = book.entries.maximum();
    let original_stats = source.stats();
    let mut custody = fixture::Custody::new();
    let request = input(&core, None, 901, 1, VerdictValue::Fail);
    let verified = fixture::verified(&mut custody, &request);
    let (prepared, report, seals) = with_seals(&core, None, request, &verified, &[2, 3]);
    assert_eq!(
        prepared.claim(fixture::key(1).claim).unwrap().status(),
        ClaimStatus::PostFailed
    );
    let view = View {
        state: &core.state,
        tail: None,
    };
    let journal = book
        .apply_prepared(
            &view,
            &prepared,
            Some(report),
            None,
            &seals,
            JournalFunding::External {
                source: &source,
                lane: BudgetLane::Completion,
            },
        )
        .unwrap();
    assert!(matches!(journal.change, Change::Many { .. }));
    assert_eq!(book.remaining_reports(fixture::key(1)), Some(0));
    assert_eq!(
        book.remaining_reports(fixture::key(2)),
        Some(original_credit.remaining_reports)
    );
    let sealed_credit = book.grant(fixture::key(2)).unwrap().credit;
    assert_eq!(sealed_credit.binding, seals[0].next().binding());
    let after_seal = book.totals;

    let request = input(&core, Some(&prepared), 902, 2, VerdictValue::Error);
    let verified = fixture::verified(&mut custody, &request);
    let later = fixture::report(&core, request, &[&prepared], &verified);
    let view = View {
        state: &core.state,
        tail: Some(&prepared),
    };
    let later_journal = book
        .apply_prepared(
            &view,
            &later,
            Some(ReportAdvance {
                key: fixture::key(2),
                before: sealed_credit.binding,
                usage: CompletionUse::Regular,
            }),
            None,
            &[],
            JournalFunding::External {
                source: &source,
                lane: BudgetLane::Completion,
            },
        )
        .unwrap();
    assert_eq!(
        book.remaining_reports(fixture::key(2)),
        Some(original_credit.remaining_reports - 1)
    );
    drop(later);
    book.rollback(later_journal).unwrap();
    assert_eq!(book.totals, after_seal);
    assert_eq!(book.grant(fixture::key(2)).unwrap().credit, sealed_credit);
    drop(prepared);
    book.rollback(journal).unwrap();
    assert_eq!(book.totals, original_totals);
    assert_eq!(book.grant(fixture::key(2)).unwrap().credit, original_credit);
    assert_eq!(book.entries.maximum(), original_weight);
    assert_eq!(source.stats(), original_stats);

    let request = input(&core, None, 903, 1, VerdictValue::Fail);
    let verified = fixture::verified(&mut custody, &request);
    let (prepared, report, seals) = with_seals(&core, None, request, &verified, &[2, 3]);
    let view = View {
        state: &core.state,
        tail: None,
    };
    let journal = book
        .apply_prepared(
            &view,
            &prepared,
            Some(report),
            None,
            &seals,
            JournalFunding::External {
                source: &source,
                lane: BudgetLane::Completion,
            },
        )
        .unwrap();
    let request = input(&core, Some(&prepared), 904, 2, VerdictValue::Pass);
    let verified = fixture::verified(&mut custody, &request);
    let later = fixture::report(&core, request, &[&prepared], &verified);
    let sealed = prepared.evaluation(fixture::key(2)).unwrap().binding();
    let view = View {
        state: &core.state,
        tail: Some(&prepared),
    };
    let later_journal = book
        .apply_prepared(
            &view,
            &later,
            Some(ReportAdvance {
                key: fixture::key(2),
                before: sealed,
                usage: CompletionUse::Regular,
            }),
            None,
            &[],
            JournalFunding::External {
                source: &source,
                lane: BudgetLane::Completion,
            },
        )
        .unwrap();
    let advanced = book.grant(fixture::key(2)).unwrap().credit;
    core.publish_native(prepared).unwrap();
    book.commit(journal).unwrap();
    assert_eq!(book.len(), 2);
    assert!(book.grant(fixture::key(1)).is_err());
    assert_eq!(book.grant(fixture::key(2)).unwrap().credit, advanced);
    assert_eq!(
        book.grant(fixture::key(3)).unwrap().credit.binding,
        seals[1].next().binding()
    );
    drop(later);
    book.rollback(later_journal).unwrap();
    assert_eq!(book.grant(fixture::key(2)).unwrap().credit.binding, sealed);
    assert_eq!(
        book.grant(fixture::key(2))
            .unwrap()
            .credit
            .remaining_reports,
        original_credit.remaining_reports
    );
    assert_eq!(book.journals, 0);
}

#[test]
fn same_report_and_seal_debit_once_and_preserve_the_accepted_revision() {
    let mut core = fixture::running(&[
        (ValidationMode::Required, false),
        (ValidationMode::Observe, true),
    ]);
    let source = source();
    let mut book = installed(&core, &source, 2);
    let mut custody = fixture::Custody::new();
    let request = input(&core, None, 911, 1, VerdictValue::Fail);
    let verified = fixture::verified(&mut custody, &request);
    let (failed, _, _) = with_seals(&core, None, request, &verified, &[]);
    let view = View {
        state: &core.state,
        tail: None,
    };
    let journal = book
        .apply_prepared(
            &view,
            &failed,
            Some(ReportAdvance {
                key: fixture::key(1),
                before: core.native_evaluation(fixture::key(1)).unwrap().binding(),
                usage: CompletionUse::AdmissionFailure,
            }),
            None,
            &[],
            JournalFunding::External {
                source: &source,
                lane: BudgetLane::Completion,
            },
        )
        .unwrap();
    core.publish_native(failed).unwrap();
    book.commit(journal).unwrap();

    let before = book.grant(fixture::key(2)).unwrap().credit;
    let before_totals = book.totals;
    let request = input(&core, None, 912, 2, VerdictValue::Pass);
    let verified = fixture::verified(&mut custody, &request);
    let (prepared, report, seals) = with_seals(&core, None, request, &verified, &[2]);
    let accepted = prepared
        .evaluation(fixture::key(2))
        .unwrap()
        .last_result()
        .unwrap();
    assert_eq!(
        seals[0].previous().state(),
        validation::State::ValidatingQualityBar
    );
    assert_eq!(accepted.binding(), before.binding.next().unwrap());
    assert_eq!(
        prepared.evaluation(fixture::key(2)).unwrap().binding(),
        accepted.binding().next().unwrap()
    );
    assert_eq!(
        prepared
            .result(NativeResultKey::of(accepted))
            .unwrap()
            .ordinal(),
        2
    );
    let view = View {
        state: &core.state,
        tail: None,
    };
    let journal = book
        .apply_prepared(
            &view,
            &prepared,
            Some(report),
            None,
            &seals,
            JournalFunding::External {
                source: &source,
                lane: BudgetLane::Completion,
            },
        )
        .unwrap();
    assert_eq!(
        book.remaining_reports(fixture::key(2)),
        Some(before.remaining_reports - 1)
    );
    assert_eq!(
        book.grant(fixture::key(2))
            .unwrap()
            .credit
            .failure_available,
        before.failure_available
    );
    assert_eq!(
        book.grant(fixture::key(2)).unwrap().credit.binding,
        seals[0].next().binding()
    );
    drop(prepared);
    book.rollback(journal).unwrap();
    assert_eq!(book.grant(fixture::key(2)).unwrap().credit, before);
    assert_eq!(book.totals, before_totals);
}

#[test]
fn incomplete_duplicate_or_reversed_seal_proofs_and_refused_funding_leave_book_exact() {
    let core = fixture::running(&[
        (ValidationMode::Required, false),
        (ValidationMode::Observe, true),
        (ValidationMode::Observe, false),
    ]);
    let source = source();
    let mut book = installed(&core, &source, 3);
    let original = book.totals;
    let credits = [1, 2, 3].map(|index| book.grant(fixture::key(index)).unwrap().credit);
    let revision = book.revision;
    let stats = source.stats();
    let mut custody = fixture::Custody::new();
    let request = input(&core, None, 921, 1, VerdictValue::Fail);
    let verified = fixture::verified(&mut custody, &request);
    let (prepared, report, seals) = with_seals(&core, None, request, &verified, &[2, 3]);
    let view = View {
        state: &core.state,
        tail: None,
    };
    for supplied in [
        &[][..],
        &seals[..1],
        &[seals[0], seals[0]][..],
        &[seals[1], seals[0]][..],
    ] {
        assert!(
            book.apply_prepared(
                &view,
                &prepared,
                Some(report),
                None,
                supplied,
                JournalFunding::External {
                    source: &source,
                    lane: BudgetLane::Completion,
                },
            )
            .is_err()
        );
        assert_eq!(book.totals, original);
        assert_eq!(book.revision, revision);
        assert_eq!(book.journals, 0);
        assert_eq!(source.stats(), stats);
        assert_eq!(
            [1, 2, 3].map(|index| book.grant(fixture::key(index)).unwrap().credit),
            credits
        );
    }
    let denied = source.child(1, 0).unwrap();
    assert!(
        book.apply_prepared(
            &view,
            &prepared,
            Some(report),
            None,
            &seals,
            JournalFunding::External {
                source: &denied,
                lane: BudgetLane::Ordinary,
            },
        )
        .is_err()
    );
    assert_eq!(book.totals, original);
    assert_eq!(book.revision, revision);
    assert_eq!(book.journals, 0);
    assert_eq!(source.stats(), stats);
    assert_eq!(denied.stats().used, 0);
    let journal = book
        .apply_prepared(
            &view,
            &prepared,
            Some(report),
            None,
            &seals,
            JournalFunding::External {
                source: &source,
                lane: BudgetLane::Completion,
            },
        )
        .unwrap();
    drop(prepared);
    book.rollback(journal).unwrap();
    assert_eq!(book.totals, original);
    assert_eq!(source.stats(), stats);
}

#[test]
fn a_valid_ready_seal_cannot_authorize_an_evaluation_omitted_from_the_source_registry() {
    let mut core = fixture::core();
    fixture::publish(
        &mut core,
        10,
        fixture::creation(
            1,
            1,
            &[
                (ValidationMode::Required, false),
                (ValidationMode::Observe, true),
                (ValidationMode::Observe, false),
            ],
            None,
        ),
    );
    fixture::publish(&mut core, 20, fixture::post(2, fixture::binding(1)));

    // Deliberately construct the malformed membership before terminalization:
    // its two retained rows are exact original registrations, but Ready key 3
    // is absent. This fixture is used only to prove refusal below.
    let claim = core.native_claim(fixture::key(1).claim).unwrap();
    let mut incomplete = RegistrationSet::new(claim, 16, usize::MAX).unwrap();
    for index in [1, 2] {
        let key = fixture::key(index);
        let evaluation = core
            .native_evaluation(key)
            .unwrap()
            .bind(core.native_definition(key.validation).unwrap())
            .unwrap();
        incomplete.register(claim, &evaluation, usize::MAX).unwrap();
    }
    for index in [1, 2] {
        let claim = core
            .native_claim(fixture::key(index).claim)
            .unwrap()
            .binding();
        let expected = core
            .native_evaluation(fixture::key(index))
            .unwrap()
            .binding();
        fixture::publish(
            &mut core,
            30,
            fixture::begin(10 + u128::from(index), claim, index, expected),
        );
    }
    let source = source();
    let mut book = installed(&core, &source, 2);
    let mut custody = fixture::Custody::new();
    let request = input(&core, None, 931, 1, VerdictValue::Fail);
    let verified = fixture::verified(&mut custody, &request);
    let (failed, _, _) = with_seals(&core, None, request, &verified, &[]);
    let view = View {
        state: &core.state,
        tail: None,
    };
    let retired = book
        .apply_prepared(
            &view,
            &failed,
            Some(ReportAdvance {
                key: fixture::key(1),
                before: core.native_evaluation(fixture::key(1)).unwrap().binding(),
                usage: CompletionUse::AdmissionFailure,
            }),
            None,
            &[],
            JournalFunding::External {
                source: &source,
                lane: BudgetLane::Completion,
            },
        )
        .unwrap();
    core.publish_native(failed).unwrap();
    book.commit(retired).unwrap();
    assert_eq!(
        core.native_claim(fixture::key(1).claim).unwrap().status(),
        ClaimStatus::PostFailed
    );
    assert_eq!(
        core.native_evaluation(fixture::key(3)).unwrap().state(),
        validation::State::Ready
    );
    assert!(book.grant(fixture::key(3)).is_err());

    // The same model operation and complete native registry are admissible.
    let request = input(&core, None, 932, 2, VerdictValue::Error);
    let verified = fixture::verified(&mut custody, &request);
    let (prepared, report, seals) = with_seals(&core, None, request, &verified, &[3]);
    let view = View {
        state: &core.state,
        tail: None,
    };
    let journal = book
        .apply_prepared(
            &view,
            &prepared,
            Some(report),
            None,
            &seals,
            JournalFunding::External {
                source: &source,
                lane: BudgetLane::Completion,
            },
        )
        .unwrap();
    drop(prepared);
    book.rollback(journal).unwrap();

    // Publish only the malformed membership through storage, preserving the
    // actual evaluation, declaration, claim cut, and live grant coordinates.
    let claim = core.native_claim(fixture::key(1).claim).unwrap();
    incomplete.check(claim).unwrap();
    let row = OwnedClaim::new(
        claim.try_copy(claim.retained_bytes().unwrap()).unwrap(),
        incomplete,
    )
    .unwrap();
    let heap = row.heap_charge().unwrap();
    let malformed = core
        .state
        .rows
        .prepare_batch_with(
            core.state.rows.prefix() + 1,
            vec![RangeChange::Put(Entry::new(
                Key::Claim(fixture::key(1).claim),
                Row::Claim(row),
                heap,
            ))],
            BudgetLane::Ordinary,
            copy_row,
        )
        .unwrap();
    core.state.rows.publish(malformed).unwrap();
    let before_state = *core.native_evaluation(fixture::key(3)).unwrap();
    let before_credit = book.grant(fixture::key(2)).unwrap().credit;
    let before_totals = book.totals;
    let before_revision = book.revision;
    let before_stats = source.stats();
    let request = input(&core, None, 933, 2, VerdictValue::Error);
    let verified = fixture::verified(&mut custody, &request);
    let (prepared, report, seals) = with_seals(&core, None, request, &verified, &[3]);
    seals[0].check(&before_state, &seals[0].next()).unwrap();
    let view = View {
        state: &core.state,
        tail: None,
    };
    assert!(
        book.apply_prepared(
            &view,
            &prepared,
            Some(report),
            None,
            &seals,
            JournalFunding::External {
                source: &source,
                lane: BudgetLane::Completion,
            },
        )
        .is_err()
    );
    assert_eq!(book.grant(fixture::key(2)).unwrap().credit, before_credit);
    assert_eq!(book.totals, before_totals);
    assert_eq!(book.revision, before_revision);
    assert_eq!(book.journals, 0);
    assert_eq!(source.stats(), before_stats);
    assert_eq!(core.native_evaluation(fixture::key(3)), Some(&before_state));
}

#[test]
fn mixed_journal_sort_and_group_validation_share_one_nonrenewable_visit_budget() {
    let core = fixture::running(&[
        (ValidationMode::Required, false),
        (ValidationMode::Observe, true),
        (ValidationMode::Observe, false),
    ]);
    let source = source();
    let mut book = installed(&core, &source, 3);
    let before = book.totals;
    let credits = [1, 2, 3].map(|index| book.grant(fixture::key(index)).unwrap().credit);
    let before_stats = source.stats();
    let mut custody = fixture::Custody::new();
    let request = input(&core, None, 941, 1, VerdictValue::Fail);
    let verified = fixture::verified(&mut custody, &request);
    let (prepared, report, seals) = with_seals(&core, None, request, &verified, &[3, 2]);
    let event_keys = (0..prepared.outcome.events)
        .filter_map(
            |ordinal| match CompletionBook::event(&prepared, ordinal).unwrap().fact {
                NativeFact::Evaluation {
                    kind: NativeEvaluationEventKind::Sealed,
                    key,
                    ..
                } => Some(key),
                _ => None,
            },
        )
        .collect::<Vec<_>>();
    assert_eq!(event_keys, [fixture::key(3), fixture::key(2)]);
    assert_eq!(
        seals
            .iter()
            .map(|seal| EvaluationKey::of(fixture::key(1).claim, &seal.next()))
            .collect::<Vec<_>>(),
        [fixture::key(2), fixture::key(3)]
    );
    let view = View {
        state: &core.state,
        tail: None,
    };
    let generous = book.limits.plan_edges;
    let mut minimum = None;
    for allowed in 0..=generous {
        book.limits.plan_edges = allowed;
        let revision = book.revision;
        match book.apply_prepared(
            &view,
            &prepared,
            Some(report),
            None,
            &seals,
            JournalFunding::External {
                source: &source,
                lane: BudgetLane::Completion,
            },
        ) {
            Ok(journal) => {
                book.rollback(journal).unwrap();
                minimum = Some(allowed);
                break;
            }
            Err(NativeError::Capacity(_)) => {
                assert_eq!(book.totals, before);
                assert_eq!(book.revision, revision);
                assert_eq!(book.journals, 0);
                assert_eq!(source.stats(), before_stats);
                assert_eq!(
                    [1, 2, 3].map(|index| book.grant(fixture::key(index)).unwrap().credit),
                    credits
                );
            }
            other => panic!("unexpected collector outcome: {other:?}"),
        }
    }
    let minimum = minimum.expect("the generous valid candidate must be accepted");
    assert!(minimum > usize::try_from(prepared.outcome.events).unwrap() * 2);
    book.limits.plan_edges = minimum - 1;
    assert!(matches!(
        book.apply_prepared(
            &view,
            &prepared,
            Some(report),
            None,
            &seals,
            JournalFunding::External {
                source: &source,
                lane: BudgetLane::Completion,
            },
        ),
        Err(NativeError::Capacity(_))
    ));
    assert_eq!(book.totals, before);
    assert_eq!(source.stats(), before_stats);
    book.limits.plan_edges = generous;
    let journal = book
        .apply_prepared(
            &view,
            &prepared,
            Some(report),
            None,
            &seals,
            JournalFunding::External {
                source: &source,
                lane: BudgetLane::Completion,
            },
        )
        .unwrap();
    drop(prepared);
    book.rollback(journal).unwrap();
    assert_eq!(book.totals, before);
    assert_eq!(source.stats(), before_stats);
}
