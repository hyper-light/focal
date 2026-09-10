use super::*;
use crate::native::completion_book::{CandidateJournal, CompletionBook, JournalFunding};
use crate::native::completion_envelope::{CompletionEnvelope, EvidenceBounds, descriptor_limits};
use crate::native::completion_schemas::SchemaSet;
use crate::native::report_tests as fixture;
use focal_evidence::BuiltinNativeSchemas;
use focal_memory::Entry;
use focal_model::ValidationMode;

fn prefixes() -> (Core<NativeState>, NativePrepared, NativePrepared) {
    let core = fixture::core();
    let created = fixture::prepared(core.prepare_native(
        fixture::context(fixture::ISSUER, 1),
        fixture::creation(1, 1, &[(ValidationMode::Required, false)], None),
        &[],
    ));
    let posted = fixture::prepared(core.prepare_native(
        fixture::context(fixture::ISSUER, 2),
        fixture::post(2, fixture::binding(1)),
        &[&created],
    ));
    (core, created, posted)
}

fn fresh<'a>(
    core: &'a Core<NativeState>,
    created: &'a NativePrepared,
    posted: &'a NativePrepared,
    request: u128,
    time: u64,
) -> Fresh<'a> {
    let key = fixture::key(1);
    let Checked::Fresh(fresh) = core
        .check_native_chain(
            fixture::context(fixture::EVALUATOR, time),
            fixture::begin(
                request,
                posted.claim(key.claim).unwrap().binding(),
                1,
                posted.evaluation(key).unwrap().binding(),
            ),
            [created, posted].into_iter(),
        )
        .unwrap()
    else {
        panic!("fresh checked Begin")
    };
    fresh
}

fn transition<'a>(fresh: &Fresh<'a>) -> BeginTransition<'a> {
    let Some(Admission::Begin {
        transition,
        active: true,
        ..
    }) = fresh.authorize_admission().unwrap()
    else {
        panic!("actual active Begin authority")
    };
    transition
}

#[test]
fn begin_proof_outlives_consumed_fresh_and_rejects_other_request_and_time() {
    let (core, created, posted) = prefixes();
    let before = core.native_budget();
    let checked = fresh(&core, &created, &posted, 3, 3);
    let proof = transition(&checked);
    assert_eq!(core.native_budget(), before);
    let original = checked.build(&core.state.budget, None).unwrap();
    proof.check(&original).unwrap();
    assert_eq!(posted.evaluation(proof.key()), Some(&proof.previous()));
    assert!(!proof.previous().has_begun());
    assert_eq!(original.evaluation(proof.key()), Some(&proof.next()));
    assert!(proof.next().has_begun());
    let other_request = fresh(&core, &created, &posted, 4, 3)
        .build(&core.state.budget, None)
        .unwrap();
    assert_eq!(
        other_request.evaluation(proof.key()),
        original.evaluation(proof.key())
    );
    assert_ne!(
        other_request.outcome().invocation,
        original.outcome().invocation
    );
    assert!(proof.check(&other_request).is_err());
    let other_time = fresh(&core, &created, &posted, 3, 4)
        .build(&core.state.budget, None)
        .unwrap();
    assert_eq!(
        other_time.outcome().invocation,
        original.outcome().invocation
    );
    assert_eq!(
        other_time.evaluation(proof.key()),
        original.evaluation(proof.key())
    );
    assert!(proof.check(&other_time).is_err());
    proof.check(&original).unwrap();
}

#[test]
fn equal_bindings_and_prefixes_do_not_replace_the_exact_pending_source_or_owner() {
    let (core, created, posted) = prefixes();
    let checked = fresh(&core, &created, &posted, 3, 3);
    let proof = transition(&checked);
    let original = checked.build(&core.state.budget, None).unwrap();
    let sibling_post = fixture::prepared(core.prepare_native(
        fixture::context(fixture::ISSUER, 2),
        fixture::post(2, fixture::binding(1)),
        &[&created],
    ));
    assert_eq!(sibling_post.outcome(), posted.outcome());
    let sibling = fresh(&core, &created, &sibling_post, 3, 3)
        .build(&core.state.budget, None)
        .unwrap();
    assert_eq!(sibling.outcome(), original.outcome());
    assert_eq!(
        sibling.evaluation(proof.key()),
        original.evaluation(proof.key())
    );
    assert!(matches!(
        proof.check(&sibling),
        Err(NativeError::Memory(MemoryError::WrongRange))
    ));
    let (other_core, other_created, other_posted) = prefixes();
    let foreign = fresh(&other_core, &other_created, &other_posted, 3, 3)
        .build(&other_core.state.budget, None)
        .unwrap();
    assert_eq!(foreign.outcome(), original.outcome());
    assert_eq!(
        foreign.evaluation(proof.key()),
        original.evaluation(proof.key())
    );
    assert!(matches!(
        proof.check(&foreign),
        Err(NativeError::Memory(MemoryError::WrongRange))
    ));
    proof.check(&original).unwrap();
}

fn copy_row(row: &Row) -> Result<Row, MemoryError> {
    match row {
        Row::Meta(value) => Ok(Row::Meta(*value)),
        Row::Claim(value) => value.copy().map(Row::Claim),
        Row::Definition(value) => value.copy().map(Row::Definition),
        Row::Evaluation(value) => value.copy().map(Row::Evaluation),
        Row::Outcome(value) => Ok(Row::Outcome(*value)),
        Row::Event(value) => value.copy().map(Row::Event),
        // The claim's other rows share its span under the storage layout.
        other => crate::native::prepare::copy(other),
    }
}

// Adversarial candidate rows still use the exact real parent Range root. This
// isolates checked event/state completeness from the separate source check.
fn altered(
    core: &Core<NativeState>,
    source: &NativePrepared,
    original: &NativePrepared,
    events: Vec<NativeEvent>,
    state: validation::EvaluationState,
) -> NativePrepared {
    let mut outcome = original.outcome();
    outcome.events = u32::try_from(events.len()).unwrap();
    let Some(Row::Meta(meta)) = original.fragments.get(&Key::Meta) else {
        panic!("metadata")
    };
    let mut meta = *meta;
    let Some(Row::Meta(source_meta)) = source.fragments.get(&Key::Meta) else {
        panic!("metadata")
    };
    meta.events = source_meta.events + events.len();
    let row = OwnedEvaluation::new(state).unwrap();
    let heap = row.heap_charge().unwrap();
    let mut changes = vec![
        Change::Put(Entry::new(Key::Meta, Row::Meta(meta), 0)),
        Change::Put(Entry::new(
            Key::Outcome(outcome.invocation),
            Row::Outcome(outcome),
            0,
        )),
        Change::Put(Entry::new(
            Key::Evaluation(fixture::key(1)),
            Row::Evaluation(row),
            heap,
        )),
    ];
    for (index, event) in events.into_iter().enumerate() {
        let row = OwnedEvent::new(StoredEvent::pack(event).unwrap()).unwrap();
        let heap = row.heap_charge().unwrap();
        changes.push(Change::Put(Entry::new(
            Key::Event(outcome.sequence, u32::try_from(index).unwrap()),
            Row::Event(row),
            heap,
        )));
    }
    let range = core
        .state
        .rows
        .plan_after(
            &core.state.budget,
            &source.fragments,
            outcome.sequence.0,
            changes,
            BudgetLane::Completion,
            usize::MAX,
        )
        .unwrap()
        .build_in_with(&core.state.budget, copy_row)
        .unwrap();
    NativePrepared {
        fragments: range,
        outcome,
        writes: crate::native::mutation::WriteSet::unrecorded(),
    }
}

#[test]
fn collector_requires_complete_begun_history_and_exact_authorized_final_state() {
    let (core, created, posted) = prefixes();
    let fresh = fresh(&core, &created, &posted, 3, 3);
    let view = fresh.publication_source();
    let proof = transition(&fresh);
    let original = fresh.build(&core.state.budget, None).unwrap();
    let registered = crate::native::admission_authority::registered_any(
        &view,
        posted.claim(proof.key().claim).unwrap().binding(),
        proof.key(),
    )
    .unwrap();
    let schemas = SchemaSet::new(
        registered.definition,
        &core.state.budget,
        &BuiltinNativeSchemas,
        core.limits.plan_edges,
    )
    .unwrap();
    let evidence = EvidenceBounds {
        workspace_bytes: schemas.workspace_bytes() - schemas.custody_bytes(),
        retained_bytes: schemas.custody_bytes(),
    };
    let envelope = CompletionEnvelope::derive(
        &core.state.rows,
        core.limits,
        registered.parent,
        registered.registry,
        registered.definition,
        descriptor_limits(core.limits, registered.parent, registered.registry).unwrap(),
        evidence,
    )
    .unwrap();
    let mut book = CompletionBook::new(&core.state.budget, core.limits).unwrap();
    let begin = book
        .install_begin_with_members(
            proof.key(),
            proof.next().binding(),
            envelope,
            schemas,
            registered.registration_index,
            None,
        )
        .unwrap();
    let expected_reports = book.remaining_reports(proof.key());
    let capacity = book.funded_capacity();
    let Some(Row::Event(event)) = original
        .fragments
        .get(&Key::Event(original.outcome().sequence, 0))
    else {
        panic!("actual Begun event")
    };
    let original_event = event.get().unwrap().expand(original.outcome().ledger);
    assert!(matches!(
        original_event.fact,
        NativeFact::Evaluation {
            kind: NativeEvaluationEventKind::Begun,
            ..
        }
    ));
    let mut duplicate = original_event;
    duplicate.ordinal = 1;
    let mut misplaced = original_event;
    misplaced.ordinal = 1;
    let mut unrelated = original_event;
    if let NativeFact::Evaluation { key, .. } = &mut unrelated.fact {
        *key = fixture::key(2);
    }
    let baseline = core.native_budget();
    assert!(
        book.apply_prepared(
            &view,
            &original,
            None,
            None,
            &[],
            JournalFunding::External {
                source: &core.state.budget,
                lane: BudgetLane::Ordinary
            },
        )
        .is_err()
    );
    assert_eq!(core.native_budget(), baseline);
    assert_eq!(book.remaining_reports(proof.key()), expected_reports);
    for (events, state) in [
        (vec![], proof.next()),
        (vec![original_event, duplicate], proof.next()),
        (vec![misplaced], proof.next()),
        (vec![unrelated], proof.next()),
        (vec![original_event], proof.previous()),
    ] {
        let malformed = altered(&core, &posted, &original, events, state);
        proof.check(&malformed).unwrap();
        let baseline = core.native_budget();
        assert!(
            book.apply_prepared(
                &view,
                &malformed,
                None,
                Some(&proof),
                &[],
                JournalFunding::External {
                    source: &core.state.budget,
                    lane: BudgetLane::Ordinary
                },
            )
            .is_err()
        );
        assert_eq!(core.native_budget(), baseline);
        assert_eq!(book.remaining_reports(proof.key()), expected_reports);
        assert_eq!(book.funded_capacity(), capacity);
    }
    let update = book
        .apply_prepared(
            &view,
            &original,
            None,
            Some(&proof),
            &[],
            JournalFunding::External {
                source: &core.state.budget,
                lane: BudgetLane::Ordinary,
            },
        )
        .unwrap();
    book.check_begin_composition(&begin, &update).unwrap();
    drop(original);
    book.rollback_candidate(CandidateJournal::begin(begin, update))
        .unwrap();
    assert_eq!(book.remaining_reports(proof.key()), None);
}

#[test]
fn a_fake_handler_free_begun_fact_cannot_hide_behind_an_unbegun_final_row() {
    let (core, created, posted) = prefixes();
    let fresh = fresh(&core, &created, &posted, 3, 3);
    let view = fresh.publication_source();
    let proof = transition(&fresh);
    let original = fresh.build(&core.state.budget, None).unwrap();
    let Some(Row::Event(event)) = original
        .fragments
        .get(&Key::Event(original.outcome().sequence, 0))
    else {
        panic!("actual Begun event")
    };
    let mut fake = event.get().unwrap().expand(original.outcome().ledger);
    let NativeFact::Evaluation {
        after,
        state,
        attempt,
        ..
    } = &mut fake.fact
    else {
        panic!("evaluation event")
    };
    *after = proof.previous().binding();
    *state = validation::State::Ready;
    *attempt = None;
    let malformed = altered(&core, &posted, &original, vec![fake], proof.previous());
    // The exact source and invocation are real. Only the invented handler-free
    // history and unbegun final row differ; neither grants suppression authority.
    proof.check(&malformed).unwrap();
    let mut book = CompletionBook::new(&core.state.budget, core.limits).unwrap();
    let budget = core.native_budget();
    let capacity = book.funded_capacity();
    assert!(matches!(
        book.apply_prepared(
            &view,
            &malformed,
            None,
            None,
            &[],
            JournalFunding::External {
                source: &core.state.budget,
                lane: BudgetLane::Ordinary,
            },
        ),
        Err(NativeError::Contract(ContractError::InvalidTransition))
    ));
    assert_eq!(core.native_budget(), budget);
    assert_eq!(book.funded_capacity(), capacity);
    assert_eq!(book.remaining_reports(proof.key()), None);
    assert_eq!(posted.evaluation(proof.key()), Some(&proof.previous()));
    assert_eq!(original.evaluation(proof.key()), Some(&proof.next()));
}
