use super::*;
use crate::native::report_tests as fixture;
use focal_evidence::BuiltinNativeSchemas;
use focal_memory::{BudgetKind, BudgetLane, Entry};
use focal_model::{ValidationMode, VerdictValue};

fn new_claim(core: &Core<NativeState>) -> Result<NativePreparation, NativeError> {
    core.prepare_native(
        fixture::context(fixture::ISSUER, 1),
        fixture::creation(1, 1, &[(ValidationMode::Required, false)], None),
        &[],
    )
}

#[test]
fn real_mutations_capture_every_write_and_leave_unchanged_content_out() {
    let mut core = fixture::core();
    let initial = fixture::prepared(new_claim(&core));
    assert_eq!(
        initial.content_profile(),
        NativeContentProfile::ProjectionOnly
    );
    // Before creation the only row is Meta, itself updated by this command.
    assert_eq!(initial.mutation_count(), initial.fragments.len());
    assert_eq!(
        initial.mutation_heap_bytes(),
        bytes(initial.mutation_count()).unwrap()
    );
    assert!(
        initial
            .writes
            .keys
            .windows(2)
            .all(|pair| pair[0].key < pair[1].key)
    );
    assert!(
        initial
            .writes
            .keys
            .iter()
            .any(|item| matches!(item.key, Key::Definition(_)))
    );
    assert!(
        initial
            .writes
            .keys
            .iter()
            .any(|item| matches!(item.key, Key::Outcome(_)))
    );
    initial.writes.check(&initial.fragments).unwrap();
    core.publish_native(initial).unwrap();
    let posted = fixture::prepared(core.prepare_native(
        fixture::context(fixture::ISSUER, 2),
        fixture::post(2, fixture::binding(1)),
        &[],
    ));
    posted.writes.check(&posted.fragments).unwrap();
    assert!(posted.mutation_count() < posted.fragments.len());
    assert!(
        !posted
            .writes
            .keys
            .iter()
            .any(|item| matches!(item.key, Key::Definition(_)))
    );
    assert_eq!(
        posted
            .writes
            .keys
            .iter()
            .filter(|item| matches!(item.key, Key::Event(..)))
            .count(),
        posted.outcome.events as usize
    );
    assert!(posted.writes.keys.iter().any(|item| item.key == Key::Meta));
    assert!(
        posted
            .writes
            .keys
            .iter()
            .any(|item| item.key == Key::Outcome(posted.outcome.invocation))
    );
    // A wrong-root publication returns the same funded write set for retry.
    let charge = posted.mutation_heap_bytes();
    let mut foreign = fixture::core();
    let refused = foreign.publish_native(posted).unwrap_err().prepared;
    assert_eq!(refused.mutation_heap_bytes(), charge);
    refused.writes.check(&refused.fragments).unwrap();
    core.publish_native(refused).unwrap();
}

#[test]
fn malformed_or_unfunded_capture_refuses_and_returns_its_complete_debit() {
    let source = MemoryBudget::new(1 << 20, 0).unwrap();
    let meta = || Change::Put(Entry::new(Key::Meta, Row::Meta(Meta::default()), 0));
    for changes in [
        vec![],
        vec![meta(), meta()],
        vec![Change::Delete(Key::End)],
        vec![Change::Put(Entry::new(
            Key::ArtifactIdentity(ContentHash([1; 32])),
            Row::Meta(Meta::default()),
            0,
        ))],
        vec![Change::Delete(Key::Claim(ClaimId::from_u128(1))), meta()],
    ] {
        let before = source.stats();
        let funding = source
            .reserve(
                BudgetKind::Pending,
                BudgetLane::Ordinary,
                bytes(changes.len()).unwrap(),
            )
            .unwrap()
            .commit();
        assert!(
            WriteSet::capture(
                NativeContentProfile::ProjectionOnly,
                changes.len(),
                changes.iter(),
                usize::MAX,
                funding
            )
            .is_err()
        );
        assert_eq!(source.stats(), before);
    }
    let changes = [meta()];
    let bound = bytes(1).unwrap();
    let before = source.stats();
    let funding = source
        .reserve(BudgetKind::Pending, BudgetLane::Ordinary, bound)
        .unwrap()
        .commit();
    assert!(
        WriteSet::capture(
            NativeContentProfile::ProjectionOnly,
            changes.len(),
            changes.iter(),
            bound - 1,
            funding
        )
        .is_err()
    );
    assert_eq!(source.stats(), before);
    let funding = source
        .reserve(BudgetKind::Pending, BudgetLane::Ordinary, bound)
        .unwrap()
        .commit();
    let writes = WriteSet::capture(
        NativeContentProfile::ProjectionOnly,
        changes.len(),
        changes.iter(),
        bound,
        funding,
    )
    .unwrap();
    assert_eq!(writes.heap_bytes(), bound);
    assert_eq!(source.stats().used, before.used + bound);
    drop(writes);
    assert_eq!(source.stats(), before);
}

#[test]
fn canonical_plan_capture_preserves_explicit_deletion_without_scanning_the_root() {
    let mut core = fixture::core();
    let key = Key::ArtifactIdentity(ContentHash([1; 32]));
    let inserted = core
        .state
        .rows
        .prepare_batch_with(
            1,
            vec![Change::Put(Entry::new(
                key,
                Row::ArtifactIdentity(ArtifactId::from_u128(1)),
                0,
            ))],
            BudgetLane::Ordinary,
            prepare::copy,
        )
        .unwrap();
    core.state.rows.publish(inserted).unwrap();
    let plan = core
        .state
        .rows
        .plan_batch(
            &core.state.budget,
            2,
            vec![Change::Delete(key)],
            BudgetLane::Ordinary,
            usize::MAX,
        )
        .unwrap();
    let source = &core.state.budget;
    let funding = source
        .reserve(BudgetKind::Pending, BudgetLane::Ordinary, bytes(1).unwrap())
        .unwrap()
        .commit();
    let writes = WriteSet::capture(
        NativeContentProfile::ProjectionOnly,
        plan.changes_len(),
        plan.changes(),
        bytes(1).unwrap(),
        funding,
    )
    .unwrap();
    let deleted = plan
        .build_in_with(&core.state.budget, prepare::copy)
        .unwrap();
    assert_eq!(writes.keys, [MutationKey { key, deleted: true }]);
    writes.check(&deleted).unwrap();
    assert!(deleted.get(&key).is_none());
    assert!(core.state.rows.get(&key).is_some());
}

#[test]
fn capture_allocation_failure_drops_the_candidate_and_allows_the_same_request() {
    let mut core = fixture::core();
    let before = core.native_budget();
    FAIL_CAPTURE.with(|flag| flag.set(true));
    assert!(matches!(
        new_claim(&core),
        Err(NativeError::Memory(MemoryError::AllocationFailed))
    ));
    assert_eq!(core.native_budget(), before);
    assert_eq!(core.native_sequence(), SessionSeq(0));
    let candidate = fixture::prepared(new_claim(&core));
    candidate.writes.check(&candidate.fragments).unwrap();
    core.publish_native(candidate).unwrap();
    assert!(matches!(
        new_claim(&core).unwrap(),
        NativePreparation::Existing {
            committed: true,
            ..
        }
    ));
}

#[test]
fn held_reports_retain_write_sets_through_pending_and_release_them_on_rollback() {
    let core = fixture::running(&[(ValidationMode::Required, false)]);
    let source = core.state.budget.clone();
    let report = fixture::report_for(
        &core,
        None,
        930,
        1,
        VerdictValue::Pass,
        fixture::descriptor(fixture::artifact_spec(
            9930,
            fixture::EVALUATOR,
            VerdictValue::Pass,
        )),
    );
    let mut custody = fixture::Custody::new();
    let proof = fixture::verified(&mut custody, &report);
    let mut owner = NativeOwner::new(core).unwrap();
    let _pressure = source
        .reserve(
            BudgetKind::Payload,
            BudgetLane::Completion,
            source.limit() - source.stats().used,
        )
        .unwrap()
        .commit();
    let before = owner.budget_stats();
    let NativeStaging::Prepared { candidate, .. } = owner
        .prepare_evidenced_with_schemas(
            fixture::context(fixture::EVALUATOR, 100),
            report,
            Some(&proof),
            &BuiltinNativeSchemas,
        )
        .unwrap()
    else {
        panic!("fresh report")
    };
    let prepared = owner.prepared_candidate(candidate).unwrap();
    assert!(prepared.mutation_heap_bytes() > 0);
    prepared.writes.check(&prepared.fragments).unwrap();
    assert!(
        prepared
            .writes
            .keys
            .iter()
            .any(|item| matches!(item.key, Key::ArtifactIdentity(_)))
    );
    owner.discard_from(candidate).unwrap();
    assert!(owner.prepared_candidate(candidate).is_err());
    assert_eq!(owner.budget_stats(), before);
}
