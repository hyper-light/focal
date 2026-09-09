use super::super::prepare::Scratch;
use super::*;

fn limits(batch: usize, nodes: usize, scratch: usize) -> NativeLimits {
    let mut limits = NativeLimits {
        plan_nodes: nodes,
        preparation_bytes: scratch,
        ..NativeLimits::default()
    };
    limits.range.max_batch_entries = batch;
    limits
}

#[test]
fn begin_charge_is_independent_of_unrelated_global_capacity() {
    let small = ConstructionBudget::for_operation(
        NativeOperation::BeginAdmission,
        limits(4, 1, OwnedEvaluation::container_charge()),
    )
    .unwrap();
    let large = ConstructionBudget::for_operation(
        NativeOperation::BeginAdmission,
        limits(usize::MAX, usize::MAX, usize::MAX),
    )
    .unwrap();
    assert_eq!(
        small.pending_bytes().unwrap(),
        large.pending_bytes().unwrap()
    );
    assert_eq!(large.max_changes, 4);
    assert_eq!(large.max_claim_rows, 0);
    assert_eq!(large.extras_count, 1);
    assert_eq!(large.max_events, 1);
    assert_eq!(large.scratch_bytes, OwnedEvaluation::container_charge());
    large.check_counts(0, 1, 1, 0).unwrap();
    assert!(large.check_counts(1, 1, 1, 0).is_err());
    assert!(large.check_counts(0, 2, 1, 0).is_err());
    assert!(large.check_counts(0, 1, 2, 0).is_err());
}

#[test]
fn begin_preserves_a_tighter_scratch_limit_instead_of_widening_it() {
    let required = OwnedEvaluation::container_charge();
    for scratch_bytes in [0, required - 1, required, required + 1] {
        let budget = ConstructionBudget::for_operation(
            NativeOperation::BeginAdmission,
            limits(4, 1, scratch_bytes),
        )
        .unwrap();
        let mut scratch = Scratch {
            used: 0,
            max: budget.scratch_bytes,
        };
        assert_eq!(scratch.charge(required).is_ok(), scratch_bytes >= required);
        assert_eq!(
            scratch.used,
            if scratch_bytes >= required {
                required
            } else {
                0
            }
        );
    }
}

#[test]
fn report_accepts_the_actual_nine_row_shape_below_its_eleven_row_ceiling() {
    for batch in 0..=12 {
        let budget = ConstructionBudget::for_operation(
            NativeOperation::ReportAdmission,
            limits(batch, 1, 4096),
        )
        .unwrap();
        // Four extra rows: artifact, content identity, evaluation and accepted
        // result. Three of them emit history. Meta and outcome add two rows.
        assert_eq!(budget.check_counts(0, 4, 3, 0).is_ok(), batch >= 9);
        // First blocking result additionally changes the claim and emits its
        // terminal transition. The smaller owner still accepts the first shape.
        assert_eq!(budget.check_counts(1, 4, 4, 0).is_ok(), batch >= 11);
        // The ceiling also funds the report's index rows: the artifact's
        // three fixed rows and sixteen input rows, its verdict, and the
        // claim's status move (doc 22 §7).
        let index = crate::native::index_rows::report_rows(16).unwrap()
            + crate::native::index_rows::STATUS_ROWS;
        assert_eq!(index, 22);
        assert_eq!(budget.max_index_rows, batch.min(index));
        assert_eq!(budget.max_changes, batch.min(11 + index));
        assert_eq!(budget.scratch_bytes, 4096);
    }
}

#[test]
fn report_shape_limits_reject_each_independently_oversized_component() {
    let budget =
        ConstructionBudget::for_operation(NativeOperation::ReportAdmission, limits(100, 100, 4096))
            .unwrap();
    for claims in 0..=2 {
        for extras in 0..=5 {
            for events in 0..=5 {
                let expected = claims <= 1
                    && extras <= 4
                    && events <= 4
                    && claims + extras + events + 2 <= budget.max_changes;
                assert_eq!(
                    budget.check_counts(claims, extras, events, 0).is_ok(),
                    expected,
                    "claims={claims}, extras={extras}, events={events}"
                );
            }
        }
    }
    // An owner with no admitted claim-plan rows can still report without a
    // derived claim mutation; it cannot silently allocate the missing slot.
    let no_claim_rows =
        ConstructionBudget::for_operation(NativeOperation::ReportAdmission, limits(11, 0, 4096))
            .unwrap();
    no_claim_rows.check_counts(0, 4, 3, 0).unwrap();
    assert!(no_claim_rows.check_counts(1, 4, 4, 0).is_err());
}

#[test]
fn report_retains_existing_scratch_contract_but_not_global_array_sizes() {
    // Thirty-four changes: the eleven-change report shape, its twenty-two
    // possible index rows and the reported evaluation's due timer.
    let small =
        ConstructionBudget::for_operation(NativeOperation::ReportAdmission, limits(34, 1, 8192))
            .unwrap();
    let large = ConstructionBudget::for_operation(
        NativeOperation::ReportAdmission,
        limits(usize::MAX, usize::MAX, 8192),
    )
    .unwrap();
    assert_eq!(
        small.pending_bytes().unwrap(),
        large.pending_bytes().unwrap()
    );
    let wider =
        ConstructionBudget::for_operation(NativeOperation::ReportAdmission, limits(34, 1, 16384))
            .unwrap();
    assert_eq!(
        wider.pending_bytes().unwrap() - small.pending_bytes().unwrap(),
        8192
    );
    assert_eq!(wider.changes_bytes, small.changes_bytes);
    assert_eq!(wider.extras_bytes, small.extras_bytes);
}

#[test]
fn extras_allowance_covers_live_replacement_and_original_buffers() {
    let budget =
        ConstructionBudget::for_operation(NativeOperation::ReportAdmission, limits(11, 1, 4096))
            .unwrap();
    let mut old = Vec::<Extra>::new();
    for capacity in [1, 2, 4] {
        let mut replacement = Vec::<Extra>::new();
        replacement.try_reserve_exact(capacity).unwrap();
        let old_bytes = array::<Extra>(old.capacity()).unwrap();
        let replacement_bytes = array::<Extra>(replacement.capacity()).unwrap();
        assert!(old_bytes + replacement_bytes <= budget.extras_bytes);
        if capacity == 4 {
            assert!(old_bytes + replacement_bytes > replacement_bytes);
        }
        // Keep both buffers alive through the accounting assertion, matching
        // Extras::push's replacement-before-drop growth order.
        drop(old);
        old = replacement;
    }
}

#[test]
fn change_allowance_covers_simultaneous_vectors_and_owned_containers() {
    for (operation, claims, extras, events) in [
        (NativeOperation::BeginAdmission, 0, 1, 1),
        (NativeOperation::ReportAdmission, 0, 4, 3),
        (NativeOperation::ReportAdmission, 1, 4, 4),
    ] {
        let budget = ConstructionBudget::for_operation(operation, limits(11, 1, 4096)).unwrap();
        budget.check_counts(claims, extras, events, 0).unwrap();
        let mut changes = Vec::<Change<Key, Row>>::new();
        changes
            .try_reserve_exact(claims + extras + events + 2)
            .unwrap();
        let mut history = Vec::<claim_changes::History>::new();
        history.try_reserve_exact(claims).unwrap();
        let actual = array::<Change<Key, Row>>(changes.capacity()).unwrap()
            + array::<claim_changes::History>(history.capacity()).unwrap()
            + containers(claims).unwrap()
            + event_containers(events).unwrap();
        assert!(actual <= budget.changes_bytes, "{operation:?}");
        assert_eq!(
            budget.pending_bytes().unwrap(),
            budget.scratch_bytes
                + budget.changes_bytes
                + budget.extras_bytes
                + crate::native::mutation::bytes(budget.max_changes).unwrap()
        );
    }
}

#[test]
fn creation_prices_derived_index_rows_while_other_operations_keep_their_allowances() {
    for operation in [
        NativeOperation::Create,
        NativeOperation::Cancel,
        NativeOperation::Post,
        NativeOperation::AcquireReceipt,
    ] {
        for (batch, nodes, scratch) in [(0, 0, 0), (9, 3, 41), (64, 32, 65536)] {
            let budget =
                ConstructionBudget::for_operation(operation, limits(batch, nodes, scratch))
                    .unwrap();
            let extras = if operation == NativeOperation::Create {
                batch
            } else {
                batch / 2
            };
            let original_extras = 2 * array::<Extra>(extras).unwrap();
            // Creation may fill the batch with index rows; every other
            // operation moves at most each changed claim between status keys
            // and changes the due timers it can (doc 22 §7): a cancellation
            // retires one per extra row and event, a post registers one per
            // admission evaluation among its extras, and a receipt settles
            // none.
            let index_rows = match operation {
                NativeOperation::Create => batch,
                NativeOperation::Cancel => (2 * nodes + batch / 2 + batch / 2).min(batch),
                NativeOperation::Post => (2 * nodes + batch / 2).min(batch),
                _ => (2 * nodes).min(batch),
            };
            let original_changes = array::<Change<Key, Row>>(batch).unwrap()
                + array::<claim_changes::History>(nodes).unwrap()
                + containers(nodes).unwrap()
                + event_containers(batch).unwrap()
                + array::<crate::native::index_rows::IndexChange>(index_rows).unwrap();
            assert_eq!(
                budget.pending_bytes().unwrap(),
                scratch
                    + original_changes
                    + original_extras
                    + crate::native::mutation::bytes(batch).unwrap()
            );
            assert_eq!(budget.max_index_rows, index_rows);
            assert_eq!(budget.max_changes, batch);
            assert_eq!(budget.max_claim_rows, nodes);
            assert_eq!(budget.max_events, batch);
            assert_eq!(budget.extras_count, extras);
        }
    }
    // One new claim with three outgoing targets adds three links, three heads
    // and one definition, then its own six index rows (issuer, subject,
    // status, creation, action and the definition's evaluator). Index rows
    // have no separate event: all eighteen rows fit, one more does not.
    let creation =
        ConstructionBudget::for_operation(NativeOperation::Create, limits(18, 7, 65536)).unwrap();
    creation.check_counts(1, 7, 2, 6).unwrap();
    assert!(creation.check_counts(1, 8, 2, 6).is_err());
    assert!(creation.check_counts(1, 7, 2, 7).is_err());
}

#[test]
fn construction_quotes_reject_overflow_without_building_buffers() {
    for operation in [
        NativeOperation::Create,
        NativeOperation::Cancel,
        NativeOperation::Post,
        NativeOperation::AcquireReceipt,
    ] {
        assert!(ConstructionBudget::for_operation(operation, limits(usize::MAX, 1, 1)).is_err());
        assert!(ConstructionBudget::for_operation(operation, limits(4, usize::MAX, 1)).is_err());
    }
    assert!(
        ConstructionBudget::for_operation(
            NativeOperation::ReportAdmission,
            limits(11, 1, usize::MAX),
        )
        .is_err()
    );
    let budget =
        ConstructionBudget::for_operation(NativeOperation::ReportAdmission, limits(11, 1, 4096))
            .unwrap();
    for (claims, extras, events) in [(usize::MAX, 0, 0), (0, usize::MAX, 0), (0, 0, usize::MAX)] {
        assert!(budget.check_counts(claims, extras, events, 0).is_err());
    }
}
