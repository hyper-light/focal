use super::report_tests::{
    Custody, EVALUATOR, ISSUER, artifact_spec, begin, binding, context, creation, descriptor, key,
    post, prepared, publish, report_for, request, verified,
};
use super::*;
use focal_memory::{Entry, MemoryError};
use focal_model::{ValidationMode, VerdictValue};

fn limits(page_bytes: usize) -> NativeLimits {
    NativeLimits {
        range: RangeConfig {
            page_entries: 128,
            page_bytes,
            max_batch_entries: 128,
            ..RangeConfig::default()
        },
        plan_nodes: 16,
        plan_edges: 256,
        preparation_bytes: 1024 * 1024,
        evaluations_per_claim: 16,
        ..NativeLimits::default()
    }
}

fn new_core(limits: NativeLimits) -> Core<NativeState> {
    Core::new_native(
        binding(1).ledger,
        RangeId(901),
        limits,
        MemoryBudget::new(64 * 1024 * 1024, 8 * 1024 * 1024).unwrap(),
    )
    .unwrap()
}

fn entry_bytes(entry: &Entry<Key, Row>) -> usize {
    size_of::<Entry<Key, Row>>() + entry.heap_bytes
}

fn check_entries(core: &Core<NativeState>) {
    assert!(core.limits.range.page_bytes <= 64 * 1024);
    assert_ne!(core.limits.range.max_entry_bytes, usize::MAX);
    for entry in core.state.rows.entries() {
        assert!(entry_bytes(entry) <= core.limits.range.max_entry_bytes);
    }
    assert_eq!(
        core.native_stats().entries,
        core.state.rows.entries().count()
    );
}

#[test]
fn native_default_layout_has_finite_page_and_complete_owned_entry_caps() {
    let original = NativeLimits::default();
    assert_eq!(original.range.page_bytes, usize::MAX);
    assert_eq!(original.range.max_entry_bytes, usize::MAX);
    let expected = original.preparation_bytes
        + OwnedClaim::container_charge()
        + OwnedEvent::container_charge()
        + size_of::<Entry<Key, Row>>();
    let core = new_core(original);
    assert_eq!(core.limits.range.page_bytes, 64 * 1024);
    assert_eq!(core.limits.range.max_entry_bytes, expected);
    assert_eq!(core.native_sequence(), SessionSeq(0));
    check_entries(&core);
}

#[test]
fn tighter_supplied_page_and_entry_caps_are_preserved_while_wider_caps_are_clamped() {
    for page_bytes in [2048, 4096, 64 * 1024, usize::MAX] {
        let mut config = limits(page_bytes);
        config.range.max_entry_bytes = 32 * 1024;
        let mut core = new_core(config);
        assert_eq!(core.limits.range.page_bytes, page_bytes.min(64 * 1024));
        assert_eq!(core.limits.range.max_entry_bytes, 32 * 1024);
        publish(&mut core, 10, creation(1, 1, &[], None));
        check_entries(&core);
    }
}

#[test]
fn real_pending_claim_admission_and_custody_report_publish_with_small_byte_bound_pages() {
    for page_bytes in [2048, 4096] {
        let mut core = new_core(limits(page_bytes));
        let empty = core.pin_native(0, 1000).unwrap();
        let created = prepared(core.prepare_native(
            context(ISSUER, 10),
            creation(1, 1, &[(ValidationMode::Required, false)], None),
            &[],
        ));
        let posted =
            prepared(core.prepare_native(context(ISSUER, 20), post(2, binding(1)), &[&created]));
        let claim = posted.claim(key(1).claim).unwrap().binding();
        let evaluation = posted.evaluation(key(1)).unwrap().binding();
        let begun = prepared(core.prepare_native(
            context(EVALUATOR, 30),
            begin(3, claim, 1, evaluation),
            &[&created, &posted],
        ));
        let input = report_for(
            &core,
            Some(&begun),
            4,
            1,
            VerdictValue::Pass,
            descriptor(artifact_spec(904, EVALUATOR, VerdictValue::Pass)),
        );
        let artifact = match &input.command {
            NativeCommand::ReportAdmission { report, .. } => report.evidence,
            _ => panic!("expected actual Admission report"),
        };
        let mut custody = Custody::new();
        let token = verified(&mut custody, &input);
        let reported = prepared(core.prepare_native_evidenced(
            context(EVALUATOR, 40),
            input,
            &[&created, &posted, &begun],
            Some(&token),
        ));
        assert_eq!(core.native_sequence(), SessionSeq(0));
        assert!(core.native_artifact(artifact.id).is_none());
        assert_eq!(
            reported
                .artifact(artifact.id)
                .unwrap()
                .descriptor()
                .content_hash(),
            artifact.hash
        );
        let result_key =
            NativeResultKey::of(reported.evaluation(key(1)).unwrap().last_result().unwrap());
        assert_eq!(
            reported.result(result_key).unwrap().result().verdict(),
            VerdictValue::Pass
        );
        let outcomes = [
            created.outcome(),
            posted.outcome(),
            begun.outcome(),
            reported.outcome(),
        ];
        for candidate in [created, posted, begun, reported] {
            core.publish_native(candidate).unwrap();
            check_entries(&core);
        }
        assert_eq!(core.native_sequence(), SessionSeq(4));
        assert!(core.native_stats().pages > 1);
        assert_eq!(
            core.native_evaluation(key(1)).unwrap().state(),
            validation::State::Validated
        );
        assert_eq!(
            core.native_result(result_key).unwrap().sequence(),
            SessionSeq(4)
        );
        assert_eq!(core.native_result(result_key).unwrap().ordinal(), 2);
        assert_eq!(core.native_claim(key(1).claim).unwrap().response_count(), 0);
        assert_eq!(
            empty
                .with_claim(key(1).claim, 1, |claim| claim.status())
                .unwrap(),
            None
        );
        for outcome in outcomes {
            assert_eq!(core.native_outcome(outcome.request), Some(outcome));
            for ordinal in 0..outcome.events {
                assert_eq!(
                    core.native_event(outcome.sequence, ordinal)
                        .unwrap()
                        .request,
                    outcome.request
                );
            }
        }
    }
}

#[test]
fn large_native_claim_remains_at_the_same_owned_address_when_an_adjacent_claim_is_inserted() {
    let mut core = new_core(limits(2048));
    let requirements = [(ValidationMode::Observe, false); 15];
    publish(&mut core, 10, creation(1, 1, &requirements, None));
    let entry = core
        .state
        .rows
        .get_entry(&Key::Claim(key(1).claim))
        .unwrap();
    assert!(
        entry_bytes(entry) > core.limits.range.page_bytes,
        "fixture must exceed the whole-page byte bound even before page metadata"
    );
    let pointer = std::ptr::from_ref(core.native_claim(key(1).claim).unwrap());
    let definition = core.native_definition(key(1).validation).unwrap().binding();
    let read = core.pin_native(0, 1000).unwrap();
    let candidate =
        prepared(core.prepare_native(context(ISSUER, 20), creation(2, 2, &[], None), &[]));
    assert_eq!(
        std::ptr::from_ref(candidate.claim(key(1).claim).unwrap()),
        pointer
    );
    assert_eq!(
        candidate.definition(key(1).validation).unwrap().binding(),
        definition
    );
    assert_eq!(
        candidate.claim(key(1).claim).unwrap().status(),
        ClaimStatus::Generated
    );
    core.publish_native(candidate).unwrap();
    assert_eq!(
        std::ptr::from_ref(core.native_claim(key(1).claim).unwrap()),
        pointer
    );
    assert_eq!(
        read.with_claim(key(1).claim, 1, std::ptr::from_ref)
            .unwrap(),
        Some(pointer)
    );
    assert!(core.native_claim(ClaimId::from_u128(2)).is_some());
    check_entries(&core);
}

#[test]
fn inline_only_entry_allowance_refuses_claim_heap_without_publishing_any_partial_fact() {
    let mut config = limits(2048);
    config.range.max_entry_bytes = size_of::<Entry<Key, Row>>();
    let core = new_core(config);
    let before = core.native_budget();
    let result = core.prepare_native(context(ISSUER, 100), creation(1, 1, &[], None), &[]);
    assert!(matches!(
        result,
        Err(NativeError::Memory(MemoryError::ItemTooLarge { .. }))
    ));
    assert_eq!(core.native_budget(), before);
    assert_eq!(core.native_sequence(), SessionSeq(0));
    assert_eq!(core.native_stats().entries, 0);
    assert!(core.native_claim(key(1).claim).is_none());
    assert!(
        core.native_definition(focal_model::ValidationId::from_u128(100))
            .is_none()
    );
    assert!(core.native_outcome(request(ISSUER, 1)).is_none());
    assert!(core.native_event(SessionSeq(1), 0).is_none());
}

#[test]
fn strict_entry_rejection_preserves_existing_rows_and_allows_smaller_same_request_retry() {
    let mut reference = new_core(limits(2048));
    publish(&mut reference, 10, creation(1, 1, &[], None));
    let accepted_maximum = reference
        .state
        .rows
        .entries()
        .map(entry_bytes)
        .max()
        .unwrap();
    let mut config = limits(2048);
    config.range.max_entry_bytes = accepted_maximum;
    let mut core = new_core(config);
    publish(&mut core, 10, creation(1, 1, &[], None));
    let read = core.pin_native(0, 1000).unwrap();
    let before = core.native_budget();
    let original = core.native_claim(key(1).claim).unwrap().binding();
    let refused = core.prepare_native(
        context(ISSUER, 900),
        creation(2, 2, &[(ValidationMode::Required, false); 15], None),
        &[],
    );
    assert!(matches!(
        refused,
        Err(NativeError::Memory(MemoryError::ItemTooLarge { .. }))
    ));
    assert_eq!(core.native_budget(), before);
    assert_eq!(core.native_sequence(), SessionSeq(1));
    assert_eq!(core.native_claim(key(1).claim).unwrap().binding(), original);
    assert!(core.native_claim(ClaimId::from_u128(2)).is_none());
    assert!(core.native_outcome(request(ISSUER, 2)).is_none());
    assert!(core.native_event(SessionSeq(2), 0).is_none());
    let accepted = publish(&mut core, 20, creation(2, 2, &[], None));
    assert_eq!(accepted.sequence, SessionSeq(2));
    assert_eq!(accepted.logical_time, 20);
    assert_eq!(
        read.with_claim(ClaimId::from_u128(2), 1, |claim| claim.status())
            .unwrap(),
        None
    );
    assert_eq!(core.native_outcome(request(ISSUER, 2)), Some(accepted));
    check_entries(&core);
}
