use super::*;
use crate::native::report_tests as fixture;
use focal_memory::{BudgetKind, BudgetLane, Change, Entry};
use focal_model::{ValidationMode, VerdictValue};

fn meta(core: &Core<NativeState>, tail: Option<&NativePrepared>) -> Meta {
    View {
        state: &core.state,
        tail,
    }
    .meta()
}
fn begin_input(core: &Core<NativeState>, tail: Option<&NativePrepared>) -> NativeInput {
    let view = View {
        state: &core.state,
        tail,
    };
    fixture::begin(
        3,
        view.claim(ClaimId::from_u128(1)).unwrap().binding(),
        1,
        view.evaluation(fixture::key(1)).unwrap().binding(),
    )
}
fn assert_refused_without_change(
    core: &Core<NativeState>,
    input: NativeInput,
    pending: &[&NativePrepared],
    evidence: Option<&focal_evidence::VerifiedNativeArtifact>,
) {
    let stats = core.native_stats();
    let budget = core.state.budget.stats();
    let events = meta(core, None).events;
    let pending_events: Vec<_> = pending
        .iter()
        .map(|tail| (tail.outcome(), meta(core, Some(tail)).events))
        .collect();
    let request = input.request;
    assert!(matches!(
        core.prepare_native_evidenced(
            fixture::context(request.principal, 100),
            input,
            pending,
            evidence
        ),
        Err(NativeError::Capacity("events"))
    ));
    assert_eq!(core.native_stats(), stats);
    assert_eq!(core.state.budget.stats(), budget);
    assert_eq!(meta(core, None).events, events);
    assert!(core.native_outcome(request).is_none());
    for (tail, before) in pending.iter().zip(pending_events) {
        assert_eq!((tail.outcome(), meta(core, Some(tail)).events), before);
        assert!(tail.recorded(request).is_none());
    }
}
fn counted_events(core: &Core<NativeState>, outcomes: &[NativeOutcome]) -> usize {
    outcomes
        .iter()
        .map(|outcome| fixture::events(core, *outcome).len())
        .sum()
}

#[test]
fn cumulative_create_post_begin_limits_refuse_one_short_and_accept_exact_totals() {
    let mut core = fixture::core();
    core.limits.events = 2;
    assert_refused_without_change(
        &core,
        fixture::creation(1, 1, &[(ValidationMode::Required, false)], None),
        &[],
        None,
    );
    assert!(core.native_claim(ClaimId::from_u128(1)).is_none());
    core.limits.events = 3;
    let created = fixture::publish(
        &mut core,
        10,
        fixture::creation(1, 1, &[(ValidationMode::Required, false)], None),
    );
    assert_eq!(created.events, 3);
    assert_eq!(meta(&core, None).events, 3);

    core.limits.events = 4;
    assert_refused_without_change(&core, fixture::post(2, fixture::binding(1)), &[], None);
    assert_eq!(
        core.native_claim(ClaimId::from_u128(1)).unwrap().status(),
        ClaimStatus::Generated
    );
    core.limits.events = 5;
    let posted = fixture::publish(&mut core, 20, fixture::post(2, fixture::binding(1)));
    assert_eq!(posted.events, 2);
    assert_eq!(meta(&core, None).events, 5);

    assert_refused_without_change(&core, begin_input(&core, None), &[], None);
    assert!(!core.native_evaluation(fixture::key(1)).unwrap().has_begun());
    core.limits.events = 6;
    let input = begin_input(&core, None);
    let began = fixture::publish(&mut core, 30, input);
    assert_eq!(began.events, 1);
    assert_eq!(meta(&core, None).events, 6);
    assert_eq!(counted_events(&core, &[created, posted, began]), 6);
}

#[test]
fn verified_required_failure_counts_history_and_registry_seal_and_retry_needs_no_capacity() {
    let mut core = fixture::running(&[(ValidationMode::Required, false)]);
    assert_eq!(meta(&core, None).events, 6);
    let old = *core.native_evaluation(fixture::key(1)).unwrap();
    let mut store = fixture::Custody::new();
    let artifact = fixture::descriptor(fixture::artifact_spec(
        500,
        fixture::EVALUATOR,
        VerdictValue::Fail,
    ));
    let input = fixture::report_for(&core, None, 4, 1, VerdictValue::Fail, artifact);
    let evidence = fixture::verified(&mut store, &input);
    let retry = fixture::copy_report(&input);
    core.limits.events = 10;
    assert_refused_without_change(&core, fixture::copy_report(&input), &[], Some(&evidence));
    assert_eq!(core.native_evaluation(fixture::key(1)), Some(&old));
    assert!(core.native_artifact(ArtifactId::from_u128(500)).is_none());
    assert_eq!(
        core.native_claim(ClaimId::from_u128(1)).unwrap().status(),
        ClaimStatus::Posted
    );

    core.limits.events = 11;
    let prepared = fixture::report(&core, input, &[], &evidence);
    let outcome = prepared.outcome();
    assert_eq!(outcome.events, 5);
    assert_eq!(meta(&core, Some(&prepared)).events, 11);
    assert_eq!(meta(&core, None).events, 6);
    core.publish_native(prepared).unwrap();
    assert_eq!(meta(&core, None).events, 11);
    assert_eq!(fixture::events(&core, outcome).len(), 5);
    assert!(matches!(
        core.native_event(outcome.sequence, 3).unwrap().fact,
        NativeFact::Claim(NativeClaimEvent {
            kind: NativeEventKind::PostFailed,
            ..
        })
    ));
    assert_eq!(
        core.native_event(outcome.sequence, 4).unwrap().fact,
        NativeFact::Registrations {
            claim: core.native_claim(fixture::key(1).claim).unwrap().binding(),
        },
    );
    let outcomes: Vec<_> = [
        fixture::request(fixture::ISSUER, 1).into(),
        fixture::request(fixture::ISSUER, 2).into(),
        fixture::request(fixture::EVALUATOR, 11).into(),
        outcome.invocation,
    ]
    .into_iter()
    .map(|request| core.native_outcome(request).unwrap())
    .collect();
    assert_eq!(counted_events(&core, &outcomes), meta(&core, None).events);

    let budget = core.state.budget.clone();
    let pressure = budget
        .reserve(
            BudgetKind::Query,
            BudgetLane::Completion,
            budget.limit() - budget.stats().used,
        )
        .unwrap();
    let occupied = budget.stats();
    // The old expected claim/evaluation revisions now precede the terminal
    // result. Exact request recovery still succeeds before authority or funding.
    let existing = core
        .prepare_native(fixture::context(fixture::EVALUATOR, 0), retry, &[])
        .unwrap();
    assert!(
        matches!(existing, NativePreparation::Existing { outcome: old, committed: true } if old == outcome)
    );
    assert_eq!(budget.stats(), occupied);
    assert_eq!(meta(&core, None).events, 11);
    drop(pressure);
}

#[test]
fn observe_report_counts_three_events_and_does_not_reserve_a_nonexistent_claim_transition() {
    let mut core = fixture::running(&[(ValidationMode::Observe, false)]);
    let before = meta(&core, None).events;
    let mut store = fixture::Custody::new();
    let input = fixture::report_for(
        &core,
        None,
        4,
        1,
        VerdictValue::Fail,
        fixture::descriptor(fixture::artifact_spec(
            501,
            fixture::EVALUATOR,
            VerdictValue::Fail,
        )),
    );
    let evidence = fixture::verified(&mut store, &input);
    core.limits.events = before + 2;
    assert_refused_without_change(&core, fixture::copy_report(&input), &[], Some(&evidence));
    core.limits.events = before + 3;
    let prepared = fixture::report(&core, input, &[], &evidence);
    let outcome = prepared.outcome();
    assert_eq!((outcome.events, outcome.changed), (3, 0));
    core.publish_native(prepared).unwrap();
    assert_eq!(meta(&core, None).events, before + 3);
    assert_eq!(fixture::events(&core, outcome).len(), 3);
    assert_eq!(
        core.native_claim(ClaimId::from_u128(1)).unwrap().status(),
        ClaimStatus::Posted
    );
}

#[test]
fn pending_chain_event_totals_are_effective_and_pending_exact_retry_is_free() {
    let mut core = fixture::core();
    let created = fixture::publish(
        &mut core,
        10,
        fixture::creation(1, 1, &[(ValidationMode::Required, false)], None),
    );
    core.limits.events = 5;
    let posted = fixture::prepared(core.prepare_native(
        fixture::context(fixture::ISSUER, 20),
        fixture::post(2, fixture::binding(1)),
        &[],
    ));
    assert_eq!(meta(&core, Some(&posted)).events, 5);
    assert_refused_without_change(&core, begin_input(&core, Some(&posted)), &[&posted], None);
    core.limits.events = 6;
    let retry = begin_input(&core, Some(&posted));
    let began = fixture::prepared(core.prepare_native(
        fixture::context(fixture::EVALUATOR, 30),
        begin_input(&core, Some(&posted)),
        &[&posted],
    ));
    assert_eq!(meta(&core, None).events, 3);
    assert_eq!(meta(&core, Some(&began)).events, 6);
    let before = core.state.budget.stats();
    let existing = core
        .prepare_native(
            fixture::context(fixture::EVALUATOR, 0),
            retry,
            &[&posted, &began],
        )
        .unwrap();
    assert!(
        matches!(existing, NativePreparation::Existing { outcome, committed: false } if outcome == began.outcome())
    );
    assert_eq!(core.state.budget.stats(), before);
    let outcomes = [created, posted.outcome(), began.outcome()];
    core.publish_native(posted).unwrap();
    core.publish_native(began).unwrap();
    assert_eq!(meta(&core, None).events, counted_events(&core, &outcomes));
    assert_eq!(meta(&core, None).events, 6);
}

#[test]
fn zero_event_configuration_and_counter_overflow_fail_closed() {
    let budget = MemoryBudget::new(16 * 1024 * 1024, 0).unwrap();
    assert!(matches!(
        Core::new_native(
            fixture::binding(1).ledger,
            RangeId(892),
            NativeLimits {
                events: 0,
                ..NativeLimits::default()
            },
            budget.clone()
        ),
        Err(NativeError::Memory(MemoryError::InvalidConfiguration(_)))
    ));
    assert_eq!(budget.stats().used, 0);

    let mut core = fixture::core();
    core.limits.events = usize::MAX;
    // Test-only accounting corruption exercises checked arithmetic; this row
    // is not presented as a valid lifecycle history or fabricated result proof.
    let exhausted = core
        .state
        .rows
        .prepare_batch_with(
            1,
            vec![Change::Put(Entry::new(
                Key::Meta,
                Row::Meta(Meta {
                    events: usize::MAX,
                    ..Meta::default()
                }),
                0,
            ))],
            BudgetLane::Completion,
            |_| panic!("empty range has no retained values to copy"),
        )
        .unwrap();
    core.state.rows.publish(exhausted).unwrap();
    let before = (core.native_stats(), core.state.budget.stats());
    assert!(matches!(
        core.prepare_native(
            fixture::context(fixture::ISSUER, 10),
            fixture::creation(1, 1, &[(ValidationMode::Required, false)], None),
            &[]
        ),
        Err(NativeError::Capacity(_))
    ));
    assert_eq!((core.native_stats(), core.state.budget.stats()), before);
    assert_eq!(meta(&core, None).events, usize::MAX);
    assert!(core.native_claim(ClaimId::from_u128(1)).is_none());
    assert!(
        core.native_outcome(fixture::request(fixture::ISSUER, 1))
            .is_none()
    );
}
