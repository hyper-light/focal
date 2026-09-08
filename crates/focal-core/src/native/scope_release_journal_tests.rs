use super::*;
use crate::native::prepare::{Extra, Extras, Scratch};
use focal_model::lifecycle::claim::ClaimCut;
use focal_model::{Deadline, MonitorId, TimerId, WaitPredicate};

fn prepared_plan(
    owner: &NativeOwner,
    expected: Binding,
    outcome: NativeOutcome,
) -> (transactions::Plan, Extras, NativeLimits, usize) {
    let mut limits = f::core().limits;
    limits.plan_edges = 65_536;
    let source = owner.committed();
    let mut extras = Extras::new(limits.range.max_batch_entries, limits.preparation_bytes).unwrap();
    let mut scratch = Scratch {
        used: 0,
        max: limits.preparation_bytes,
    };
    let plan = crate::native::scope_release::prepare(
        expected,
        f::context(f::ISSUER, outcome.logical_time),
        ClaimCut {
            position: outcome.sequence,
            cause: outcome.intent,
        },
        source.source_view(),
        limits,
        &mut extras,
        &mut scratch,
    )
    .unwrap();
    let index_rows = extras.rows.len();
    crate::native::scope_release::check_journal_with_monitors(
        &plan.rows,
        &extras,
        source.source_view(),
        outcome,
        limits,
        index_rows,
    )
    .unwrap();
    (plan, extras, limits, index_rows)
}

#[test]
fn release_journal_requires_original_unique_owner_fact_and_ordered_actual_graph_consequences() {
    let mut owner = tree();
    for id in [4, 5] {
        commit(&mut owner, f::creation(50 + id, id, &[], None));
        let expected = binding(&owner, id);
        commit(
            &mut owner,
            NativeInput {
                request: f::request(f::ISSUER, 60 + id),
                command: NativeCommand::RegisterMonitor {
                    expected,
                    receipt: None,
                    id: MonitorId::from_u128(70 + id),
                    roots: vec![WaitPredicate::Released(ClaimId::from_u128(3))],
                    deadline: Deadline {
                        timer: TimerId::from_u128(80 + id),
                        generation: 1,
                        at: 1000,
                    },
                },
            },
        );
    }
    let expected = binding(&owner, 1);
    commit(
        &mut owner,
        NativeInput {
            request: f::request(f::ISSUER, 2),
            command: NativeCommand::Cancel { expected },
        },
    );
    assert_eq!(
        owner
            .committed()
            .claim(ClaimId::from_u128(4))
            .unwrap()
            .status(),
        ClaimStatus::Generated
    );
    // Cancellation settles dependency failures immediately. These explicit
    // Released predicates remain pending until the terminal leaf is released.
    for id in [4, 5] {
        assert!(
            owner
                .committed()
                .claim(ClaimId::from_u128(id))
                .unwrap()
                .scopes()
                .monitor(MonitorId::from_u128(70 + id))
                .unwrap()
                .active()
        );
    }
    let expected = binding(&owner, 3);
    let (candidate, outcome) = stage(&mut owner, release(expected, f::ISSUER, 51));
    let source = owner.committed();
    let view = source.source_view();
    let (healthy, extras, limits, _) = prepared_plan(&owner, expected, outcome);
    let facts = extras.journal.as_ref().unwrap();
    assert_eq!(healthy.rows.len(), 3);
    assert_eq!(facts.len(), 3);
    assert!(matches!(
        facts[0],
        NativeFact::Claim(NativeClaimEvent {
            kind: NativeEventKind::OwnerReleased,
            ..
        })
    ));
    assert!(matches!(
        facts[1],
        NativeFact::Claim(NativeClaimEvent {
            kind: NativeEventKind::Monitor(NativeMonitorEvent::Released { .. }),
            ..
        })
    ));
    assert!(matches!(
        facts[2],
        NativeFact::Claim(NativeClaimEvent {
            kind: NativeEventKind::Monitor(NativeMonitorEvent::Released { .. }),
            ..
        })
    ));
    for (ordinal, fact) in facts.iter().enumerate() {
        assert_eq!(
            owner
                .candidate(candidate)
                .unwrap()
                .event(outcome.sequence, u32::try_from(ordinal).unwrap())
                .unwrap()
                .fact,
            *fact
        );
    }
    for row in &healthy.rows {
        assert_eq!(
            owner
                .candidate(candidate)
                .unwrap()
                .claim(ClaimId(row.binding().object.0)),
            Some(row)
        );
    }
    let budget = owner.budget_stats();
    let range = owner.range_stats();
    for corruption in 0..11 {
        let (mut plan, mut extras, _, index_rows) = prepared_plan(&owner, expected, outcome);
        let mut altered = outcome;
        match corruption {
            0 => extras.journal = None,
            1 => {
                extras.journal.as_mut().unwrap().remove(0);
            }
            2 => extras.journal.as_mut().unwrap().swap(0, 1),
            3 => {
                let NativeFact::Claim(first) = &mut extras.journal.as_mut().unwrap()[0] else {
                    panic!("owner fact")
                };
                first.kind = NativeEventKind::Cancelled;
            }
            4 => {
                let NativeInvocation::Request(mut request) = altered.invocation else {
                    panic!("actor request")
                };
                request.principal = f::SUBJECT;
                altered.invocation = NativeInvocation::Request(request);
            }
            5 => altered.intent = ContentHash([89; 32]),
            6 => {
                let NativeFact::Claim(first) = &mut extras.journal.as_mut().unwrap()[0] else {
                    panic!("owner fact")
                };
                first.after.content = ContentHash([88; 32]);
            }
            7 => {
                let unrelated = source.claim(ROOT).unwrap();
                plan.rows.push(
                    unrelated
                        .try_copy(unrelated.copy_charge().unwrap())
                        .unwrap(),
                );
            }
            8 => {
                let unrelated = source.claim(ROOT).unwrap();
                let registry = source.registrations(ROOT).unwrap();
                let claim = unrelated
                    .try_copy(unrelated.copy_charge().unwrap())
                    .unwrap();
                let registry = registry
                    .try_copy(registry.retained_bytes().unwrap())
                    .unwrap();
                let row = OwnedClaim::new(claim, registry).unwrap();
                let heap = row.heap_charge().unwrap();
                extras
                    .push(Extra {
                        key: Key::Claim(ROOT),
                        row: Row::Claim(row),
                        heap,
                        fact: None,
                    })
                    .unwrap();
            }
            9 => {
                let repeated = extras.journal.as_ref().unwrap()[1];
                extras.journal.as_mut().unwrap().push(repeated);
                let row = plan
                    .rows
                    .iter()
                    .find(|row| row.binding().object.0 == ClaimId::from_u128(4).0)
                    .unwrap();
                plan.rows
                    .push(row.try_copy(row.copy_charge().unwrap()).unwrap());
            }
            10 => {
                let repeated = extras.journal.as_ref().unwrap()[1];
                extras.journal.as_mut().unwrap()[2] = repeated;
            }
            _ => unreachable!(),
        }
        assert!(
            crate::native::scope_release::check_journal_with_monitors(
                &plan.rows, &extras, view, altered, limits, index_rows
            )
            .is_err(),
            "corruption {corruption}"
        );
    }
    assert_eq!(owner.budget_stats(), budget);
    assert_eq!(owner.range_stats(), range);
    assert_eq!(owner.pending_len(), 1);
    assert!(
        !owner
            .committed()
            .claim(ClaimId::from_u128(3))
            .unwrap()
            .released()
    );
    assert!(
        owner
            .candidate(candidate)
            .unwrap()
            .claim(ClaimId::from_u128(3))
            .unwrap()
            .released()
    );
}
