use super::report_tests::{
    EVALUATOR, ISSUER, QUALITY, SUBJECT, artifact_spec, context, copy_report, core, creation,
    descriptor, key, prepared, report_for, request, running,
};
use super::*;
use focal_evidence::{BuiltinNativeSchemas, ContentStore, StoreLimits, VerifiedNativeArtifact};
use focal_memory::{BudgetKind, BudgetLane};
use focal_model::{ContentDomainId, ValidationMode, VerdictValue};

fn store(path: &std::path::Path) -> ContentStore {
    ContentStore::open(
        path,
        StoreLimits {
            max_content_bytes: 2 * 1024 * 1024,
            max_staging_bytes: 4 * 1024 * 1024,
            max_uploads: 8,
            chunk_bytes: 17,
            max_manifest_bytes: 128 * 1024,
        },
    )
    .unwrap()
}

fn verify(
    store: &mut ContentStore,
    input: &NativeInput,
    source: &MemoryBudget,
) -> VerifiedNativeArtifact {
    let NativeCommand::ReportAdmission { artifact, .. } = &input.command else {
        panic!("expected report")
    };
    store
        .verify_native_artifact(
            input.request,
            artifact.get().unwrap(),
            ContentDomainId::from_u128(93),
            source,
            &BuiltinNativeSchemas,
        )
        .unwrap()
}

#[test]
fn one_ordinary_funded_owner_pool_carries_custody_retry_quality_and_receipt_under_pressure() {
    let mut core = running(&[(ValidationMode::Required, true)]);
    let owner = core.state.budget.clone();
    let pool = owner
        .funded_child(BudgetLane::Ordinary, 20 * 1024 * 1024)
        .unwrap();
    // Keep every original, ordinarily funded page alive throughout publication.
    let read = core.pin_native(0, 1000).unwrap();
    let pressure = owner
        .reserve(
            BudgetKind::Payload,
            BudgetLane::Completion,
            owner.stats().limit - owner.stats().used,
        )
        .unwrap();
    let parent = owner.stats();
    assert_eq!(parent.used, parent.limit);
    let directory = tempfile::tempdir().unwrap();
    let mut custody = store(directory.path());

    for (index, (value, evaluator)) in [
        (VerdictValue::Error, EVALUATOR),
        (VerdictValue::Pass, EVALUATOR),
        (VerdictValue::Pass, QUALITY),
    ]
    .into_iter()
    .enumerate()
    {
        let input = report_for(
            &core,
            None,
            100 + index as u128,
            1,
            value,
            descriptor(artifact_spec(800 + index as u128, evaluator, value)),
        );
        let retry = copy_report(&input);
        let before = pool.stats();
        let verified = verify(&mut custody, &input, &pool);
        assert_eq!(pool.stats().used, before.used + verified.retained_bytes());
        assert_eq!(owner.stats().used, parent.used);
        assert_eq!(owner.stats().ordinary_used, parent.ordinary_used);
        assert!(
            owner
                .reserve(BudgetKind::Pending, BudgetLane::Completion, 1)
                .is_err()
        );
        let next = prepared(core.prepare_native_in(
            &pool,
            context(evaluator, 100 + index as u64),
            input,
            &[],
            Some(&verified),
        ));
        let outcome = next.outcome();
        assert_eq!(outcome.results, 1);
        assert_eq!(outcome.artifacts, 1);
        assert_eq!(owner.stats().used, parent.used);
        let no_free_credit = pool
            .reserve(
                BudgetKind::Pending,
                BudgetLane::Completion,
                pool.stats().limit - pool.stats().used,
            )
            .unwrap();
        let exhausted = pool.stats();
        assert!(matches!(
            core.prepare_native_in(
                &pool,
                context(evaluator, 900),
                copy_report(&retry),
                &[&next],
                None,
            ).unwrap(),
            NativePreparation::Existing { outcome: existing, committed: false }
                if existing == outcome
        ));
        assert_eq!(pool.stats(), exhausted);
        core.publish_native(next).unwrap();
        let published = pool.stats();
        assert!(matches!(
            core.prepare_native_in(&pool, context(evaluator, 900), retry, &[], None).unwrap(),
            NativePreparation::Existing { outcome: existing, committed: true }
                if existing == outcome
        ));
        assert_eq!(pool.stats(), published);
        drop(no_free_credit);
        drop(verified);
        assert_eq!(owner.stats().used, parent.used);
        assert_eq!(owner.stats().ordinary_used, parent.ordinary_used);
    }

    assert_eq!(
        core.native_claim(key(1).claim).unwrap().status(),
        ClaimStatus::Posted
    );
    let expected = core.native_claim(key(1).claim).unwrap().binding();
    let received = prepared(core.prepare_native_in(
        &pool,
        context(SUBJECT, 200),
        NativeInput {
            request: request(SUBJECT, 120),
            command: NativeCommand::AcquireReceipt {
                expected,
                receipt: ReceiptId::from_u128(900),
            },
        },
        &[],
        None,
    ));
    core.publish_native(received).unwrap();
    let claim = core.native_claim(key(1).claim).unwrap();
    assert_eq!(claim.status(), ClaimStatus::Received);
    assert_eq!(claim.response_count(), 0);
    assert_eq!(owner.stats().used, parent.used);
    assert_eq!(owner.stats().ordinary_used, parent.ordinary_used);
    assert!(
        read.with_claim(key(1).claim, 0, |old| old.receipt())
            .unwrap()
            .unwrap()
            .is_none()
    );
    core.release_native(&read).unwrap();
    drop(read);
    drop(core);
    assert_eq!(pool.stats().used, 0);
    drop(pool);
    drop(pressure);
    assert_eq!(owner.stats().used, 0);
}

#[test]
fn failed_funded_report_returns_scratch_and_pages_without_losing_custody_or_exact_retry() {
    let core = running(&[(ValidationMode::Required, false)]);
    let owner = core.state.budget.clone();
    let pool = owner
        .funded_child(BudgetLane::Ordinary, 20 * 1024 * 1024)
        .unwrap();
    let _pressure = owner
        .reserve(
            BudgetKind::Payload,
            BudgetLane::Completion,
            owner.stats().limit - owner.stats().used,
        )
        .unwrap();
    let directory = tempfile::tempdir().unwrap();
    let mut custody = store(directory.path());
    let input = report_for(
        &core,
        None,
        130,
        1,
        VerdictValue::Pass,
        descriptor(artifact_spec(910, EVALUATOR, VerdictValue::Pass)),
    );
    let verified = verify(&mut custody, &input, &pool);
    let before = pool.stats();
    let parent = owner.stats();
    let sequence = core.native_sequence();
    let failed = super::prepare::fail_copies_after(0, || {
        core.prepare_native_in(
            &pool,
            context(EVALUATOR, 100),
            copy_report(&input),
            &[],
            Some(&verified),
        )
    });
    assert!(matches!(
        failed,
        Err(NativeError::Memory(MemoryError::AllocationFailed))
    ));
    assert_eq!(pool.stats(), before);
    assert_eq!(owner.stats(), parent);
    assert_eq!(core.native_sequence(), sequence);
    assert_eq!(core.native_outcome(input.request), None);

    let independent = MemoryBudget::new(64 * 1024 * 1024, 0).unwrap();
    assert!(matches!(
        core.prepare_native_in(
            &independent,
            context(EVALUATOR, 100),
            copy_report(&input),
            &[],
            Some(&verified),
        ),
        Err(NativeError::Memory(MemoryError::InvalidConfiguration(_)))
    ));
    assert_eq!(independent.stats().used, 0);
    assert_eq!(pool.stats(), before);
    let retry = prepared(core.prepare_native_in(
        &pool,
        context(EVALUATOR, 100),
        input,
        &[],
        Some(&verified),
    ));
    assert_eq!(retry.outcome().results, 1);
    drop(retry);
    assert_eq!(pool.stats(), before);
    assert_eq!(owner.stats(), parent);
    drop(verified);
    assert_eq!(pool.stats().used, 0);
}

#[test]
fn completion_funding_cannot_bypass_native_ordinary_admission() {
    let core = core();
    let pool = core
        .state
        .budget
        .funded_child(BudgetLane::Completion, 8 * 1024 * 1024)
        .unwrap();
    let before = core.native_budget();
    let refused = core.prepare_native_in(
        &pool,
        context(ISSUER, 10),
        creation(1, 1, &[], None),
        &[],
        None,
    );
    assert!(matches!(
        refused,
        Err(NativeError::Memory(MemoryError::InvalidConfiguration(_)))
    ));
    assert_eq!(pool.stats().used, 0);
    assert_eq!(core.native_budget(), before);
    assert_eq!(core.native_sequence(), SessionSeq(0));
    assert!(core.native_claim(key(1).claim).is_none());
}
