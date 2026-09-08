use super::*;
use crate::native::prepare::{Extra, Extras, Scratch};

fn staged() -> (NativeOwner, NativeOutcome, Vec<ClaimState>, Extras) {
    let core = authored_posted(&[(
        1,
        500,
        &[
            (ValidationMode::Observe, true),
            (ValidationMode::Observe, true),
        ],
        &[],
    )]);
    let mut owner = NativeOwner::new(core).unwrap();
    begin_claim(&mut owner, 1, 1, 31);
    begin_claim(&mut owner, 1, 2, 32);
    let timer = input(&owner, 1);
    let (candidate, outcome) = fire(&mut owner, timer, 500);
    let actual = owner.candidate(candidate).unwrap();
    let mut rows = Vec::new();
    let mut extras = Extras::new(128, usize::MAX).unwrap();
    extras
        .begin_journal(
            128,
            &mut Scratch {
                used: 0,
                max: usize::MAX,
            },
        )
        .unwrap();
    let mut suffix = false;
    let mut seals = Vec::new();
    let mut registration_facts = 0;
    for ordinal in 0..outcome.events {
        let fact = actual.event(outcome.sequence, ordinal).unwrap().fact;
        match fact {
            NativeFact::Claim(event) => {
                assert!(!suffix);
                extras.record(fact).unwrap();
                let row = actual.claim(ClaimId(event.after.object.0)).unwrap();
                rows.push(row.try_copy(row.retained_bytes().unwrap()).unwrap());
            }
            NativeFact::Evaluation {
                key,
                kind: NativeEvaluationEventKind::AuthorityFenced,
                ..
            } => {
                assert!(!suffix);
                let previous = *owner.committed().evaluation(key).unwrap();
                let definition = actual.definition(key.validation).unwrap();
                // The original deadline journal precedes the shared seal suffix.
                // Derive its real intermediate row from the same actual source,
                // then prove that the retained final row is exactly its seal.
                let fenced = previous
                    .expire_claim(definition, actual.claim(key.claim).unwrap())
                    .unwrap();
                assert_eq!(
                    fact,
                    NativeFact::Evaluation {
                        kind: NativeEvaluationEventKind::AuthorityFenced,
                        key,
                        before: Some(previous.binding()),
                        after: fenced.binding(),
                        state: fenced.state(),
                        phase: fenced.phase(),
                        attempt: Some(fenced.bind(definition).unwrap().current_attempt().unwrap()),
                        fence: fenced.fence(),
                    }
                );
                assert_sealed(&owner, outcome, key, fenced);
                extras.record(fact).unwrap();
                let row = OwnedEvaluation::new(fenced).unwrap();
                extras
                    .push(Extra {
                        key: Key::Evaluation(key),
                        heap: row.heap_charge().unwrap(),
                        row: Row::Evaluation(row),
                        fact: None,
                    })
                    .unwrap();
            }
            NativeFact::Evaluation {
                key,
                kind: NativeEvaluationEventKind::Sealed,
                ..
            } => {
                suffix = true;
                seals.push(key);
            }
            NativeFact::Registrations { claim } => {
                assert!(suffix);
                assert_eq!(
                    claim,
                    actual.claim(ClaimId::from_u128(1)).unwrap().binding()
                );
                registration_facts += 1;
            }
            _ => panic!("unexpected claim deadline or automatic seal fact"),
        }
    }
    assert_eq!(seals, vec![claim_key(1, 1), claim_key(1, 2)]);
    assert_eq!(registration_facts, 1);
    (owner, outcome, rows, extras)
}

fn check(
    owner: &NativeOwner,
    outcome: NativeOutcome,
    rows: &[ClaimState],
    extras: &mut Extras,
) -> Result<(), NativeError> {
    crate::native::claim_deadlines::check_journal(
        rows,
        extras,
        &owner.committed().0,
        outcome,
        owner.core.limits,
        0,
    )
}

#[test]
fn expiry_journal_requires_every_actual_registry_fence_even_if_row_and_event_are_both_omitted() {
    let (owner, outcome, rows, mut extras) = staged();
    check(&owner, outcome, &rows, &mut extras).unwrap();
    let omitted = claim_key(1, 2);
    extras
        .rows
        .retain(|extra| extra.key != Key::Evaluation(omitted));
    extras
        .journal
        .as_mut()
        .unwrap()
        .retain(|fact| !matches!(fact, NativeFact::Evaluation { key, .. } if *key == omitted));
    assert!(check(&owner, outcome, &rows, &mut extras).is_err());
    assert!(
        owner
            .committed()
            .evaluation(omitted)
            .unwrap()
            .fence()
            .is_none()
    );
}

#[test]
fn expiry_journal_rejects_reordered_or_duplicate_fences_and_false_lifecycle_events() {
    for mutation in 0..3 {
        let (owner, outcome, rows, mut extras) = staged();
        check(&owner, outcome, &rows, &mut extras).unwrap();
        let journal = extras.journal.as_mut().unwrap();
        let first = journal
            .iter()
            .position(|fact| matches!(fact, NativeFact::Evaluation { .. }))
            .unwrap();
        match mutation {
            0 => journal.swap(first, first + 1),
            1 => journal.push(journal[first]),
            2 => {
                let NativeFact::Evaluation { kind, .. } = &mut journal[first] else {
                    panic!("fence event");
                };
                *kind = NativeEvaluationEventKind::Begun;
            }
            _ => unreachable!(),
        }
        assert!(check(&owner, outcome, &rows, &mut extras).is_err());
    }
}

#[test]
fn expiry_journal_rejects_extra_rows_and_changed_timer_or_publication_identity() {
    let (owner, outcome, rows, mut extras) = staged();
    check(&owner, outcome, &rows, &mut extras).unwrap();
    for changed in [
        NativeOutcome {
            logical_time: 499,
            ..outcome
        },
        NativeOutcome {
            sequence: SessionSeq(outcome.sequence.0 + 1),
            ..outcome
        },
        NativeOutcome {
            intent: ContentHash([99; 32]),
            ..outcome
        },
        NativeOutcome {
            invocation: NativeInvocation::Request(request(ISSUER, 500)),
            ..outcome
        },
    ] {
        assert!(check(&owner, changed, &rows, &mut extras).is_err());
    }
    let key = claim_key(1, 1);
    let row = OwnedEvaluation::new(*owner.effective().evaluation(key).unwrap()).unwrap();
    // Bypass the builder's duplicate guard to exercise the journal's own check.
    extras.rows.push(Extra {
        key: Key::Evaluation(key),
        heap: row.heap_charge().unwrap(),
        row: Row::Evaluation(row),
        fact: None,
    });
    assert!(check(&owner, outcome, &rows, &mut extras).is_err());
}
