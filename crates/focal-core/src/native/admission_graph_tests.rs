//! Corrupt detached candidates produced by an actual verified Admission report.
use super::*;
use crate::native::{prepare::Extra, report_tests as f};
use focal_model::{ValidationMode, VerdictValue, lifecycle::claim::ClaimIntent};

struct Staged {
    rows: Vec<ClaimState>,
    extras: Extras,
    outcome: NativeOutcome,
}

fn source() -> Core<NativeState> {
    let mut core = f::running(&[(ValidationMode::Required, false)]);
    // The direct transaction fixture has no owner grants. Add an actual indexed
    // dependent so the report checker also sees a real graph-derived row.
    let mut input = f::creation(40, 2, &[], None);
    let NativeCommand::Create { claims, .. } = &mut input.command else {
        panic!("creation")
    };
    claims[0].definition.graph = focal_model::lifecycle::graph::Declaration::new(
        &[focal_model::lifecycle::graph::Obligation {
            kind: focal_model::lifecycle::graph::Kind::DependsOn,
            target: ClaimId::from_u128(1),
        }],
        4,
    )
    .unwrap();
    f::publish(&mut core, 40, input);
    core
}
fn view(core: &Core<NativeState>) -> View<'_> {
    View {
        state: &core.state,
        tail: None,
    }
}
fn stage(core: &Core<NativeState>) -> Staged {
    let mut custody = f::Custody::new();
    let artifact = f::descriptor(f::artifact_spec(500, f::EVALUATOR, VerdictValue::Fail));
    let input = f::report_for(core, None, 41, 1, VerdictValue::Fail, artifact);
    let evidence = f::verified(&mut custody, &input);
    let view = view(core);
    let intent = intent::fingerprint(view.ledger(), &input).unwrap();
    let sequence = SessionSeq(view.prefix().0 + 1);
    let mut meta = view.meta();
    meta.logical_time = 100;
    meta.outcomes += 1;
    let mut extras = Extras::new(
        core.limits.range.max_batch_entries,
        core.limits.preparation_bytes,
    )
    .unwrap();
    let mut scratch = Scratch {
        used: 0,
        max: core.limits.preparation_bytes,
    };
    let mut plan = transactions::prepare(
        input.command,
        input.request,
        Some(&evidence),
        f::context(input.request.principal, 100),
        ClaimCut {
            position: sequence,
            cause: intent,
        },
        &view,
        core.limits,
        &mut meta,
        &mut extras,
        &mut scratch,
    )
    .unwrap();
    plan.rows.sort_unstable_by_key(|row| row.binding().object);
    let outcome = NativeOutcome {
        ledger: view.ledger(),
        invocation: input.request.into(),
        sequence,
        logical_time: 100,
        operation: NativeOperation::ReportAdmission,
        intent,
        created: 0,
        changed: u32::try_from(plan.rows.len()).unwrap(),
        definitions: 0,
        evaluations: 1,
        artifacts: 1,
        results: 1,
        receipts: 0,
        responses: 0,
        result_testaments: 0,
        events: u32::try_from(extras.events()).unwrap(),
    };
    Staged {
        rows: plan.rows,
        extras,
        outcome,
    }
}
fn checked(core: &Core<NativeState>, staged: &Staged) -> Result<(), NativeError> {
    check(
        &staged.rows,
        &staged.extras,
        &view(core),
        staged.outcome,
        core.limits,
        0,
    )
}

#[test]
fn original_report_coordinates_and_graph_kinds_cannot_be_rewritten() {
    let core = source();
    let mut staged = stage(&core);
    checked(&core, &staged).unwrap();
    let proof = staged.extras.admission_graph.unwrap();
    let original = staged.extras.journal.as_ref().unwrap().clone();
    assert_eq!(&original[..4], &proof.prefix());
    assert!(matches!(original[4], NativeFact::Claim(event)
        if event.kind == NativeEventKind::DependencyFailed));
    assert_eq!(staged.rows.len(), 2);
    let stats = core.native_stats();
    let budget = core.state.budget.stats();
    for corruption in 0..5 {
        let journal = staged.extras.journal.as_mut().unwrap();
        journal.clone_from(&original);
        match corruption {
            0 => journal.swap(1, 2),
            1 => {
                journal.remove(2);
            }
            2 => {
                journal.insert(3, original[2]);
            }
            3 => {
                // Satisfied cannot be justified by the root's Required failure
                // cut, even if the fabricated event uses the exact final row.
                journal[4] = NativeFact::Claim(NativeClaimEvent {
                    kind: NativeEventKind::Satisfied,
                    ..proof.event()
                });
            }
            4 => {
                journal[4] = NativeFact::Claim(NativeClaimEvent {
                    kind: NativeEventKind::PostFailed,
                    ..proof.event()
                });
            }
            _ => unreachable!(),
        }
        assert!(checked(&core, &staged).is_err(), "corruption {corruption}");
        assert_eq!(core.native_stats(), stats);
        assert_eq!(core.state.budget.stats(), budget);
        assert_eq!(
            core.native_claim(f::key(1).claim).unwrap().status(),
            ClaimStatus::Posted
        );
    }
    staged.extras.journal = Some(original);
    checked(&core, &staged).unwrap();
}

#[test]
fn exact_result_state_and_independent_peer_terminal_cut_are_required() {
    let core = source();
    let mut staged = stage(&core);
    let proof = staged.extras.admission_graph.unwrap();
    let result_key = NativeResultKey::of(proof.accepted.result());
    let accepted_index = staged
        .extras
        .rows
        .iter()
        .position(|row| row.key == Key::Accepted(result_key))
        .unwrap();
    let wrong = NativeAccepted::new(
        proof.accepted.result(),
        proof.accepted.attempt(),
        proof.accepted.artifact(),
        proof.accepted.sequence(),
        1,
    )
    .unwrap();
    let old = std::mem::replace(
        &mut staged.extras.rows[accepted_index].row,
        Row::Accepted(OwnedAccepted::new(wrong).unwrap()),
    );
    assert!(matches!(
        checked(&core, &staged),
        Err(NativeError::Contract(ContractError::MissingEvidence))
    ));
    staged.extras.rows[accepted_index].row = old;

    let eval_index = staged
        .extras
        .rows
        .iter()
        .position(|row| row.key == Key::Evaluation(proof.key))
        .unwrap();
    let old = std::mem::replace(
        &mut staged.extras.rows[eval_index].row,
        Row::Evaluation(OwnedEvaluation::new(proof.previous).unwrap()),
    );
    assert!(matches!(
        checked(&core, &staged),
        Err(NativeError::Contract(ContractError::MissingEvidence))
    ));
    staged.extras.rows[eval_index].row = old;

    let peer = core.native_claim(ClaimId::from_u128(2)).unwrap();
    let mut cancelled = peer.try_copy(peer.retained_bytes().unwrap()).unwrap();
    cancelled
        .apply(
            &peer.binding(),
            Principal::Actor(f::ISSUER),
            ClaimIntent::Cancel {
                cut: ClaimCut {
                    position: staged.outcome.sequence,
                    cause: ContentHash([81; 32]),
                },
            },
        )
        .unwrap();
    let old = std::mem::replace(&mut staged.rows[1], cancelled);
    assert!(matches!(
        checked(&core, &staged),
        Err(NativeError::Contract(ContractError::InvalidCut))
    ));
    staged.rows[1] = old;

    // No index suffix can silently alter a stored scope or replace this exact
    // evidence with a second row under the same key.
    let duplicate = OwnedAccepted::new(proof.accepted).unwrap();
    staged.extras.rows.push(Extra {
        key: Key::Accepted(result_key),
        heap: duplicate.heap_charge().unwrap(),
        row: Row::Accepted(duplicate),
        fact: None,
    });
    assert!(matches!(
        checked(&core, &staged),
        Err(NativeError::Contract(ContractError::InvalidManifest))
    ));
    staged.extras.rows.pop();
    checked(&core, &staged).unwrap();
}

#[test]
fn preflight_visit_quote_covers_actual_report_checker_without_allocating() {
    let core = source();
    let staged = stage(&core);
    let claims: Vec<_> = staged
        .rows
        .iter()
        .map(|row| core.native_claim(ClaimId(row.binding().object.0)).unwrap())
        .collect();
    let before = core.state.budget.stats();
    let quoted =
        checker_visits_bound(&claims, staged.extras.rows.len(), staged.extras.events()).unwrap();
    let mut limits = core.limits;
    limits.plan_edges = quoted;
    check(
        &staged.rows,
        &staged.extras,
        &view(&core),
        staged.outcome,
        limits,
        0,
    )
    .unwrap();
    limits.plan_edges = 0;
    assert!(matches!(
        check(
            &staged.rows,
            &staged.extras,
            &view(&core),
            staged.outcome,
            limits,
            0
        ),
        Err(NativeError::Capacity("admission graph history visits"))
    ));
    assert!(checker_visits_bound(&claims, usize::MAX, staged.extras.events()).is_err());
    assert!(checker_visits_bound(&claims, staged.extras.rows.len(), 3).is_err());
    assert_eq!(core.state.budget.stats(), before);
}
