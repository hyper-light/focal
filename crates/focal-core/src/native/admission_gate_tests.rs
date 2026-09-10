use super::super::report_tests::{
    ISSUER, binding, context, core, creation, post, prepared, request,
};
use super::*;
use focal_model::ValidationMode;

#[test]
fn checked_pending_input_builds_from_selected_funding_after_parent_exhaustion() {
    let mut core = core();
    let first = prepared(core.prepare_native(
        context(ISSUER, 1),
        creation(1, 1, &[(ValidationMode::Required, false)], None),
        &[],
    ));
    let parent = core.state.budget.clone();
    let pool = parent
        .funded_child(BudgetLane::Ordinary, 8 * 1024 * 1024)
        .unwrap();
    let pressure = parent
        .reserve(
            BudgetKind::Payload,
            BudgetLane::Completion,
            parent.limit() - parent.stats().used,
        )
        .unwrap();
    let before = parent.stats();
    let local_before = pool.stats();
    let checked = core
        .check_native_chain(
            context(ISSUER, 2),
            post(2, binding(1)),
            std::iter::once(&first),
        )
        .unwrap();
    assert_eq!(parent.stats(), before);
    assert_eq!(pool.stats(), local_before);
    let Checked::Fresh(fresh) = checked else {
        panic!("expected a fresh request")
    };
    let second = fresh.build(&pool, None).unwrap();
    assert_eq!(second.outcome().operation, NativeOperation::Post);
    assert_eq!(second.outcome().sequence, SessionSeq(2));
    assert_eq!(parent.stats().used, before.used);
    assert_eq!(parent.stats().ordinary_used, before.ordinary_used);
    assert!(pool.stats().used > local_before.used);
    core.state
        .rows
        .validate_chain([&first.fragments, &second.fragments].into_iter())
        .unwrap();
    core.publish_native(first).unwrap();
    let outcome = core.publish_native(second).unwrap();
    assert_eq!(core.native_outcome(request(ISSUER, 2)), Some(outcome));
    assert_eq!(
        core.native_claim(ClaimId::from_u128(1)).unwrap().status(),
        ClaimStatus::Posted
    );
    drop(pressure);
    drop(core);
    drop(pool);
    assert_eq!(parent.stats().used, 0);
}

#[test]
fn checked_fresh_source_refusal_retains_base_and_does_not_spend_either_budget() {
    let core = core();
    let parent = core.state.budget.clone();
    let unrelated = MemoryBudget::new(8 * 1024 * 1024, 0).unwrap();
    let before = parent.stats();
    let other_before = unrelated.stats();
    let Checked::Fresh(fresh) = core
        .check_native_chain(
            context(ISSUER, 1),
            creation(1, 1, &[], None),
            std::iter::empty(),
        )
        .unwrap()
    else {
        panic!("expected a fresh request")
    };
    assert_eq!(parent.stats(), before);
    assert!(matches!(
        fresh.build(&unrelated, None),
        Err(NativeError::Memory(MemoryError::InvalidConfiguration(_)))
    ));
    assert_eq!(parent.stats(), before);
    assert_eq!(unrelated.stats(), other_before);
    assert_eq!(core.native_sequence(), SessionSeq(0));
    assert!(core.native_claim(ClaimId::from_u128(1)).is_none());
}

#[test]
fn checked_retries_resolve_before_full_queue_memory_and_new_logical_time() {
    let mut core = core();
    core.limits.pending = 1;
    let first = prepared(core.prepare_native(context(ISSUER, 5), creation(1, 1, &[], None), &[]));
    let outcome = first.outcome();
    let parent = core.state.budget.clone();
    let pressure = parent
        .reserve(
            BudgetKind::Payload,
            BudgetLane::Completion,
            parent.limit() - parent.stats().used,
        )
        .unwrap();
    let before = parent.stats();
    assert!(matches!(core.check_native_chain(
        context(ISSUER, 0), creation(1, 1, &[], None), std::iter::once(&first),
    ).unwrap(), Checked::Existing { outcome: found, committed: false } if found == outcome));
    assert_eq!(parent.stats(), before);
    assert!(matches!(
        core.check_native_chain(
            context(ISSUER, 6),
            creation(2, 2, &[], None),
            std::iter::once(&first),
        ),
        Err(NativeError::Capacity("pending candidates"))
    ));
    assert_eq!(parent.stats(), before);
    core.publish_native(first).unwrap();
    let committed = parent.stats();
    assert!(matches!(core.check_native_chain(
        context(ISSUER, 0), creation(1, 1, &[], None), std::iter::empty(),
    ).unwrap(), Checked::Existing { outcome: found, committed: true } if found == outcome));
    assert_eq!(parent.stats(), committed);
    drop(pressure);
    drop(core);
    assert_eq!(parent.stats().used, 0);
}

#[test]
fn begin_uses_its_small_construction_shape_under_parent_pressure() {
    use super::super::report_tests::{EVALUATOR, begin, key};
    let mut core = core();
    let old_scratch_floor = core.limits.preparation_bytes;
    let first = prepared(core.prepare_native(
        context(ISSUER, 1),
        creation(1, 1, &[(ValidationMode::Required, false)], None),
        &[],
    ));
    core.publish_native(first).unwrap();
    let posted = prepared(core.prepare_native(context(ISSUER, 2), post(2, binding(1)), &[]));
    core.publish_native(posted).unwrap();
    let parent = core.state.budget.clone();
    let claim = core.native_claim(key(1).claim).unwrap().binding();
    let evaluation = core.native_evaluation(key(1)).unwrap().binding();
    let free = old_scratch_floor / 2;
    let pressure = parent
        .reserve(
            BudgetKind::Payload,
            BudgetLane::Completion,
            parent.limit() - parent.stats().used - free,
        )
        .unwrap();
    let before = parent.stats();
    // The low-level four-row construction fits below the former all-operation
    // scratch floor. This API does not promise completion: NativeOwner separately
    // funds the full report chain before it accepts responsibility.
    assert!(parent.limit() - before.used < old_scratch_floor);
    let candidate =
        prepared(core.prepare_native(context(EVALUATOR, 3), begin(3, claim, 1, evaluation), &[]));
    let outcome = candidate.outcome();
    assert_eq!(outcome.changed, 0);
    assert_eq!(outcome.evaluations, 1);
    assert_eq!(outcome.events, 1);
    assert!(candidate.evaluation(key(1)).unwrap().has_begun());
    assert!(!core.native_evaluation(key(1)).unwrap().has_begun());
    assert!(parent.stats().used - before.used < free);
    core.publish_native(candidate).unwrap();
    assert!(core.native_evaluation(key(1)).unwrap().has_begun());
    drop(pressure);
    drop(core);
    assert_eq!(parent.stats().used, 0);
}
