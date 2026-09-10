use super::*;
use crate::native::report_tests as fixture;
use focal_memory::{Change as RangeChange, Entry};
use focal_model::ValidationMode;

const CLAIM: ClaimId = ClaimId::from_u128(1);
const RESPONSE: TestamentId = TestamentId::from_u128(900);

fn input(claim: Binding, response: Binding) -> NativeInput {
    NativeInput {
        request: fixture::request(fixture::ISSUER, 9001),
        command: NativeCommand::EnterWholeWork {
            claim,
            expected: response,
        },
    }
}

fn build(core: &Core<NativeState>) -> prepare::BuiltNative {
    let checked = core
        .check_native_chain(
            fixture::context(fixture::ISSUER, 200),
            input(
                core.native_claim(CLAIM).unwrap().binding(),
                core.native_response(RESPONSE).unwrap().identity().binding,
            ),
            std::iter::empty(),
        )
        .unwrap();
    let prepare::Checked::Fresh(fresh) = checked else {
        panic!("fresh entry")
    };
    fresh
        .build_recorded(&core.state.budget, None, None, None)
        .unwrap()
}

fn events(prepared: &NativePrepared) -> Vec<NativeEvent> {
    (0..prepared.outcome.events)
        .map(|ordinal| CompletionBook::event(prepared, ordinal).unwrap())
        .collect()
}

fn observer(events: &[NativeEvent]) -> (usize, EvaluationKey) {
    events
        .iter()
        .enumerate()
        .find_map(|(ordinal, event)| match event.fact {
            NativeFact::Evaluation {
                key,
                kind: NativeEvaluationEventKind::MissingTarget,
                state: validation::State::Ready,
                ..
            } => Some((ordinal, key)),
            _ => None,
        })
        .unwrap()
}

// Retain every genuine final row and exact source-root provenance. Only the
// supplied history is altered, so refusal cannot hide behind a foreign branch.
fn altered(
    core: &Core<NativeState>,
    original: &NativePrepared,
    mut events: Vec<NativeEvent>,
) -> NativePrepared {
    for (ordinal, event) in events.iter_mut().enumerate() {
        event.ordinal = u32::try_from(ordinal).unwrap();
    }
    assert_eq!(
        events.len(),
        usize::try_from(original.outcome.events).unwrap()
    );
    let changes = original
        .fragments
        .entries()
        .map(|entry| {
            let (row, heap) = match entry.key {
                Key::Event(sequence, ordinal) if sequence == original.outcome.sequence => {
                    let event = *events.get(usize::try_from(ordinal).unwrap()).unwrap();
                    let row = OwnedEvent::new(StoredEvent::pack(event).unwrap()).unwrap();
                    let heap = row.heap_charge().unwrap();
                    (Row::Event(row), heap)
                }
                _ => (prepare::copy(&entry.value).unwrap(), entry.heap_bytes),
            };
            RangeChange::Put(Entry::new(entry.key, row, heap))
        })
        .collect::<Vec<_>>();
    assert!(changes.len() <= core.limits.range.max_batch_entries);
    let range = core
        .state
        .rows
        .prepare_batch_with(
            original.outcome.sequence.0,
            changes,
            BudgetLane::Ordinary,
            prepare::copy,
        )
        .unwrap();
    NativePrepared {
        fragments: range,
        outcome: original.outcome,
        writes: crate::native::mutation::WriteSet::unrecorded(),
    }
}

#[test]
fn actual_owner_settles_missing_required_and_observe_before_sealing_and_rolls_back_exactly() {
    let core = work_authority::history_fixture(false, true);
    let mut owner = NativeOwner::new(core).unwrap();
    let before_budget = owner.budget_stats();
    let before_range = owner.range_stats();
    let before_sequence = owner.effective().sequence();
    let before_claim = owner.effective().claim(CLAIM).unwrap().binding();
    let before_response = owner
        .effective()
        .response(RESPONSE)
        .unwrap()
        .identity()
        .binding;
    let before = owner
        .effective()
        .registrations(CLAIM)
        .unwrap()
        .rows()
        .iter()
        .map(|registered| {
            let key = transactions::key_for_registered(CLAIM, *registered);
            (key, *owner.effective().evaluation(key).unwrap())
        })
        .collect::<Vec<_>>();
    for publish in [false, true] {
        let NativeStaging::Prepared { candidate, outcome } = owner
            .prepare(
                fixture::context(fixture::ISSUER, 200),
                input(before_claim, before_response),
                None,
            )
            .unwrap()
        else {
            panic!("actual entry")
        };
        assert_eq!(outcome.operation, NativeOperation::EnterWholeWork);
        assert_eq!((outcome.artifacts, outcome.results), (0, 1));
        let view = owner.candidate(candidate).unwrap();
        let entry = view.response_record(RESPONSE).unwrap().entered().unwrap();
        let mut structural_members = 0;
        for (key, old) in &before {
            let next = view.evaluation(*key).unwrap();
            let bound = next.bind(view.definition(key.validation).unwrap()).unwrap();
            assert!(!next.has_begun());
            assert_eq!(next.phase(), old.phase());
            assert_eq!(next.receipt(), old.receipt());
            assert_eq!(owner.committed().evaluation(*key), Some(old));
            if old.state().is_terminal() {
                assert_eq!(next, old);
                assert!(
                    matches!(key.target, EvaluationTarget::Delivery { response } if response == RESPONSE)
                );
                let result = old.last_result().unwrap();
                assert_eq!(result.phase(), validation::Phase::Delivery);
                let result_key = NativeResultKey::of(result);
                let original = owner.committed().delivery_result(result_key).unwrap();
                let retained = view.delivery_result(result_key).unwrap();
                assert_eq!(
                    (retained.result(), retained.sequence(), retained.ordinal()),
                    (original.result(), original.sequence(), original.ordinal()),
                );
                assert!(original.sequence() < outcome.sequence);
                assert!(!(0..outcome.events).any(|ordinal| matches!(
                    view.event(outcome.sequence, ordinal).unwrap().fact,
                    NativeFact::Evaluation { key: changed, .. } if changed == *key
                )));
                continue;
            }
            assert!(
                matches!(key.target, EvaluationTarget::MissingSlot { response, .. } if response == RESPONSE)
            );
            structural_members += 1;
            match bound.mode() {
                ValidationMode::Required => {
                    assert!(next.state().is_terminal());
                    assert_eq!(next.binding(), old.binding().next().unwrap());
                    let result = next.last_result().unwrap();
                    assert_eq!(result.attempt(), None);
                    assert_eq!(result.evidence(), None);
                    assert_eq!(result.phase(), validation::Phase::MissingTarget);
                    let missing = view.missing_result(NativeResultKey::of(result)).unwrap();
                    assert_eq!(missing.sequence(), outcome.sequence);
                    assert!(entry.ordinal < missing.ordinal());
                }
                ValidationMode::Observe => {
                    assert_eq!(
                        bound.suppression(),
                        Some(validation::Suppression::MissingTarget)
                    );
                    assert_eq!(next.state(), validation::State::Ready);
                    assert_eq!(next.last_result(), None);
                    assert!(next.sealed().is_some());
                    assert_eq!(
                        next.binding(),
                        old.binding().next().unwrap().next().unwrap()
                    );
                    let history = (0..outcome.events)
                        .filter_map(|ordinal| {
                            match view.event(outcome.sequence, ordinal).unwrap().fact {
                                NativeFact::Evaluation {
                                    key: recorded,
                                    kind,
                                    before,
                                    after,
                                    ..
                                } if recorded == *key => Some((ordinal, kind, before, after)),
                                _ => None,
                            }
                        })
                        .collect::<Vec<_>>();
                    assert_eq!(history.len(), 2);
                    assert!(entry.ordinal < history[0].0);
                    assert_eq!(history[0].1, NativeEvaluationEventKind::MissingTarget);
                    assert_eq!(history[0].2, Some(old.binding()));
                    assert_eq!(history[1].1, NativeEvaluationEventKind::Sealed);
                    assert_eq!(history[1].2, Some(history[0].3));
                    assert_eq!(history[1].3, next.binding());
                }
            }
        }
        assert_eq!(structural_members, 2);
        if publish {
            owner.publish_after_durable(candidate).unwrap();
            assert!(owner.committed().registrations(CLAIM).unwrap().is_sealed());
            assert_eq!(owner.committed().sequence(), outcome.sequence);
        } else {
            assert_eq!(owner.discard_from(candidate).unwrap(), 1);
            assert_eq!(owner.budget_stats(), before_budget);
            assert_eq!(owner.range_stats(), before_range);
            assert_eq!(owner.effective().sequence(), before_sequence);
            assert_eq!(
                owner.effective().claim(CLAIM).unwrap().binding(),
                before_claim
            );
            assert!(!owner.effective().registrations(CLAIM).unwrap().is_sealed());
            for (key, old) in &before {
                assert_eq!(owner.effective().evaluation(*key), Some(old));
            }
        }
    }
}

#[test]
fn collector_requires_the_structural_fact_and_entry_order_without_creating_report_credit() {
    let core = work_authority::history_fixture(false, true);
    let built = build(&core);
    let prepared = built.prepared();
    let source = View {
        state: &core.state,
        tail: None,
    };
    let mut book = CompletionBook::new(&core.state.budget, core.limits).unwrap();
    let before = core.native_budget();
    let original = events(prepared);
    let (missing, key) = observer(&original);
    let entry = crate::native::response_reads::as_response_record(
        prepared.fragments.get(&Key::Response(RESPONSE)),
    )
    .unwrap()
    .entered()
    .unwrap();
    let seal = original
        .iter()
        .position(|event| {
            matches!(event.fact,
            NativeFact::Evaluation { key: recorded, kind: NativeEvaluationEventKind::Sealed, .. }
                if recorded == key)
        })
        .unwrap();
    for mutation in 0..8 {
        let mut changed = original.clone();
        match mutation {
            0 => {
                changed[missing].fact = NativeFact::Registrations {
                    claim: prepared.claim(CLAIM).unwrap().binding(),
                }
            }
            1..=5 => {
                let NativeFact::Evaluation {
                    kind,
                    before,
                    phase,
                    ..
                } = &mut changed[missing].fact
                else {
                    panic!("structural event")
                };
                match mutation {
                    1 => *phase = validation::Phase::Programmatic,
                    2 => *kind = NativeEvaluationEventKind::Begun,
                    3 => *kind = NativeEvaluationEventKind::Reported,
                    4 => *before = None,
                    5 => *before = Some(prepared.evaluation(key).unwrap().binding()),
                    _ => unreachable!(),
                }
            }
            6 => changed.swap(missing, usize::try_from(entry.ordinal).unwrap()),
            7 => changed.swap(missing, seal),
            _ => unreachable!(),
        }
        let candidate = altered(&core, prepared, changed);
        assert!(
            book.apply_prepared(
                &source,
                &candidate,
                None,
                None,
                built.seals(),
                JournalFunding::External {
                    source: &core.state.budget,
                    lane: BudgetLane::Ordinary
                },
            )
            .is_err(),
            "mutation {mutation}"
        );
        assert_eq!(book.totals, Totals::default());
        assert_eq!((book.len(), book.journals, book.revision), (0, 0, 0));
        assert_eq!(book.remaining_reports(key), None);
        drop(candidate);
        assert_eq!(core.native_budget(), before);
    }
    let journal = book
        .apply_prepared(
            &source,
            prepared,
            None,
            None,
            built.seals(),
            JournalFunding::External {
                source: &core.state.budget,
                lane: BudgetLane::Ordinary,
            },
        )
        .unwrap();
    assert!(journal.owner.is_none());
    book.commit(journal).unwrap();
    assert_eq!(book.totals, Totals::default());
    assert_eq!((book.len(), book.journals, book.revision), (0, 0, 0));
    assert_eq!(core.native_budget(), before);
}

#[test]
fn structural_entry_checks_charge_visits_before_reads_and_preserve_state_phase() {
    let core = work_authority::history_fixture(false, true);
    let built = build(&core);
    let prepared = built.prepared();
    let source = View {
        state: &core.state,
        tail: None,
    };
    let history = events(prepared);
    let (ordinal, key) = observer(&history);
    let event = frame(prepared, u32::try_from(ordinal).unwrap(), None)
        .unwrap()
        .unwrap();
    let old = *source.evaluation(key).unwrap();
    let next = built
        .seals()
        .iter()
        .find(|seal| seal.before() == event.after)
        .unwrap()
        .previous();
    let definition = source.definition(key.validation).unwrap();
    assert_ne!(next.phase(), validation::Phase::MissingTarget);
    assert_eq!(old.phase(), next.phase());
    check_frame(event, old, next, definition).unwrap();
    assert!(matches!(
        check_missing(&source, prepared, event, old, next, definition, &mut 3),
        Err(NativeError::Capacity("completion event visits"))
    ));
    let mut remaining = 4;
    check_missing(
        &source,
        prepared,
        event,
        old,
        next,
        definition,
        &mut remaining,
    )
    .unwrap();
    assert_eq!(remaining, 0);
    assert_eq!(source.evaluation(key).unwrap(), &old);
}
