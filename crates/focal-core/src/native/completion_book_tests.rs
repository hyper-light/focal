use super::*;
use crate::native::completion_envelope::descriptor_limits;
use crate::native::report_tests as fixture;
use focal_evidence::BuiltinNativeSchemas;
use focal_model::ValidationMode;

#[path = "completion_book_rebind_tests.rs"]
mod rebind_tests;
#[path = "completion_slot_tests.rs"]
mod slot_tests;

fn source() -> MemoryBudget {
    // Accounting capacity only: construction allocates small bounded buffers.
    MemoryBudget::new(usize::MAX / 16, 0).unwrap()
}

fn parts(
    core: &Core<NativeState>,
    source: &MemoryBudget,
    index: u32,
) -> (Binding, CompletionEnvelope, SchemaSet) {
    let key = fixture::key(index);
    let Some(Row::Claim(owner)) = core.state.row(&Key::Claim(key.claim)) else {
        panic!("actual owner claim");
    };
    let parent = owner.claim().unwrap();
    let registrations = owner.registrations().unwrap();
    let declaration = core.native_definition(key.validation).unwrap();
    let schemas = SchemaSet::new(declaration, source, &BuiltinNativeSchemas, 16).unwrap();
    let envelope = CompletionEnvelope::derive(
        &core.state.rows,
        core.limits,
        parent,
        registrations,
        declaration,
        descriptor_limits(core.limits, parent, registrations).unwrap(),
    )
    .unwrap();
    (
        core.native_evaluation(key).unwrap().binding(),
        envelope,
        schemas,
    )
}

fn install(
    book: &mut CompletionBook,
    core: &Core<NativeState>,
    source: &MemoryBudget,
    index: u32,
) -> Journal {
    let (binding, envelope, schemas) = parts(core, source, index);
    book.install_begin(
        fixture::key(index),
        binding,
        envelope,
        schemas,
        registration(core, index),
    )
    .unwrap()
}

fn registration(core: &Core<NativeState>, index: u32) -> usize {
    let key = fixture::key(index);
    let Some(Row::Claim(claim)) = core.state.row(&Key::Claim(key.claim)) else {
        panic!("claim")
    };
    claim
        .registrations()
        .unwrap()
        .rows()
        .iter()
        .position(|row| transactions::key_for_registered(key.claim, *row) == key)
        .unwrap()
}

#[test]
fn tail_begin_restores_exact_grant_buffer_with_older_pending() {
    let core = fixture::running(&[(ValidationMode::Observe, false); 2]);
    let source = source();
    let mut book = CompletionBook::new(&source, core.limits).unwrap();
    let empty = source.stats();
    let first = install(&mut book, &core, &source, 1);
    let before_second = source.stats();
    let capacity = book.entries.capacity();
    let second = install(&mut book, &core, &source, 2);
    assert!(book.entries.capacity() > capacity);
    assert_eq!(book.journals, 2);

    book.rollback(second).unwrap();
    assert_eq!(source.stats(), before_second);
    assert_eq!(book.entries.capacity(), capacity);
    assert_eq!(book.len(), 1);

    book.rollback(first).unwrap();
    assert_eq!(source.stats(), empty);
    assert_eq!(book.entries.capacity(), 0);
    drop(book);
    assert_eq!(source.stats().used, 0);
}

#[test]
fn committing_begin_head_preserves_later_update_and_rollback_restores_credit() {
    let core = fixture::running(&[(ValidationMode::Observe, false)]);
    let source = source();
    let mut book = CompletionBook::new(&source, core.limits).unwrap();
    let begin = install(&mut book, &core, &source, 1);
    let key = fixture::key(1);
    let before = core.native_evaluation(key).unwrap().binding();
    let report = book
        .advance(
            key,
            before,
            before.next().unwrap(),
            false,
            CompletionUse::Regular,
        )
        .unwrap();
    assert_eq!(book.remaining_reports(key), Some(1));
    book.commit(begin).unwrap();
    assert_eq!(book.remaining_reports(key), Some(1));
    assert_eq!(book.journals, 1);
    book.rollback(report).unwrap();
    assert_eq!(book.remaining_reports(key), Some(2));
    assert_eq!(book.grant(key).unwrap().credit.binding, before);
    assert_eq!(book.journals, 0);
}

#[test]
fn published_terminal_head_and_discarded_replacement_leave_no_grant_or_charge() {
    let core = fixture::running(&[(ValidationMode::Observe, false); 2]);
    let source = source();
    let mut book = CompletionBook::new(&source, core.limits).unwrap();
    let begin = install(&mut book, &core, &source, 1);
    book.commit(begin).unwrap();
    let first_key = fixture::key(1);
    let binding = core.native_evaluation(first_key).unwrap().binding();
    let report = book
        .advance(
            first_key,
            binding,
            binding.next().unwrap(),
            true,
            CompletionUse::Regular,
        )
        .unwrap();
    let second = install(&mut book, &core, &source, 2);
    book.commit(report).unwrap();
    assert!(book.remaining_reports(first_key).is_none());
    book.rollback(second).unwrap();
    assert_eq!(book.len(), 0);
    drop(book);
    assert_eq!(source.stats().used, 0);
}

#[test]
fn foreign_and_non_tail_journals_are_refused_without_changing_credits() {
    let core = fixture::running(&[(ValidationMode::Observe, false); 2]);
    let source = source();
    let mut left = CompletionBook::new(&source, core.limits).unwrap();
    let mut right = CompletionBook::new(&source, core.limits).unwrap();
    let foreign = install(&mut left, &core, &source, 1);
    assert!(right.rollback(foreign).is_err());
    assert_eq!(left.remaining_reports(fixture::key(1)), Some(2));
    assert_eq!(right.len(), 0);
    // Journals are private linear owner capabilities. Refusal is an internal
    // owner-order violation; the public owner checks tickets before this call.
    let first = install(&mut right, &core, &source, 1);
    let second = install(&mut right, &core, &source, 2);
    assert!(right.rollback(first).is_err());
    assert_eq!(right.len(), 2);
    assert_eq!(right.remaining_reports(fixture::key(1)), Some(2));
    assert_eq!(right.remaining_reports(fixture::key(2)), Some(2));
    drop(second);
    drop(left);
    drop(right);
    assert_eq!(source.stats().used, 0);
}

#[test]
fn failed_index_growth_restores_existing_metadata_and_parent_exactly() {
    let core = fixture::running(&[(ValidationMode::Observe, false)]);
    let source = source();
    let mut book = CompletionBook::new(&source, core.limits).unwrap();
    let baseline = source.stats();
    let (binding, envelope, schemas) = parts(&core, &source, 1);
    // Leave one byte less than the grant vector's growth: the install is
    // refused at its only allocation, nothing installed or charged.
    let metadata = CompletionIndex::<Grant>::slot_charge(1).unwrap();
    let pressure = source
        .reserve(
            BudgetKind::Index,
            BudgetLane::Ordinary,
            source.reservation_limit(BudgetLane::Ordinary) - source.stats().used - metadata + 1,
        )
        .unwrap()
        .commit();
    let before = source.stats().used - schemas.retained_bytes();
    assert!(
        book.install_begin(
            fixture::key(1),
            binding,
            envelope,
            schemas,
            registration(&core, 1)
        )
        .is_err()
    );
    assert_eq!(source.stats().used, before);
    assert_eq!(book.entries.capacity(), 0);
    assert_eq!(book.journals, 0);
    drop(pressure);
    assert_eq!(source.stats(), baseline);
}

#[test]
fn serial_margin_includes_reports_and_outcomes_one_control() {
    let core = fixture::running(&[(ValidationMode::Observe, false)]);
    let source = source();
    let mut book = CompletionBook::new(&source, core.limits).unwrap();
    let begin = install(&mut book, &core, &source, 1);
    assert!(book.check_serial(u64::MAX - 2).is_err());
    book.check_serial(u64::MAX - 3).unwrap();
    // Resident outcomes keep one for an authority control while a grant lives,
    // nothing for the reports it may yet make.
    let mut meta = Meta {
        outcomes: book.limits.outcomes - 1,
        ..Meta::default()
    };
    book.check_slots(meta, SessionSeq(u64::MAX - 1), 0, BudgetLane::Ordinary)
        .unwrap();
    meta.outcomes += 1;
    assert!(
        book.check_slots(meta, SessionSeq(0), 0, BudgetLane::Completion)
            .is_err()
    );
    book.rollback(begin).unwrap();
    book.check_serial(u64::MAX).unwrap();
}

#[test]
fn increment_grant_funds_exact_target_family_and_journals_terminal_report_without_claim_failure() {
    let (core, registry, state) = super::super::completion_envelope::tests::increment_fixture();
    let source = source();
    let mut book = CompletionBook::new(&source, core.limits).unwrap();
    let parent = core.native_claim(fixture::key(1).claim).unwrap();
    let key = EvaluationKey::of(fixture::key(1).claim, &state);
    let declaration = core.native_definition(key.validation).unwrap();
    let pins = || SchemaSet::new(declaration, &source, &BuiltinNativeSchemas, 16).unwrap();
    let schemas = pins();
    let envelope = CompletionEnvelope::derive(
        &core.state.rows,
        core.limits,
        parent,
        &registry,
        declaration,
        descriptor_limits(core.limits, parent, &registry).unwrap(),
    )
    .unwrap();
    drop(schemas);
    let position = registry
        .rows()
        .iter()
        .position(|row| transactions::key_for_registered(key.claim, *row) == key)
        .unwrap();
    let baseline = source.stats();
    assert!(
        book.install_begin(
            EvaluationKey {
                target: EvaluationTarget::Admission,
                ..key
            },
            state.binding(),
            envelope,
            pins(),
            position,
        )
        .is_err()
    );
    assert_eq!(source.stats(), baseline);
    assert_eq!(book.len(), 0);
    let begin = book
        .install_begin(key, state.binding(), envelope, pins(), position)
        .unwrap();
    assert_eq!(
        book.remaining_reports(key),
        Some(declaration.attempt_bound())
    );
    assert!(!book.grant(key).unwrap().credit.failure_available);
    let before_report = source.stats();
    assert!(
        book.advance(
            key,
            state.binding(),
            state.binding().next().unwrap(),
            true,
            CompletionUse::AdmissionFailure
        )
        .is_err()
    );
    assert_eq!(source.stats(), before_report);
    let report = book
        .advance(
            key,
            state.binding(),
            state.binding().next().unwrap(),
            true,
            CompletionUse::Regular,
        )
        .unwrap();
    assert_eq!(book.remaining_reports(key), Some(0));
    book.commit(begin).unwrap();
    book.rollback(report).unwrap();
    assert_eq!(book.remaining_reports(key), Some(1));
    assert_eq!(book.grant(key).unwrap().credit.binding, state.binding());
    assert!(!book.grant(key).unwrap().credit.failure_available);
    let report = book
        .advance(
            key,
            state.binding(),
            state.binding().next().unwrap(),
            true,
            CompletionUse::Regular,
        )
        .unwrap();
    book.commit(report).unwrap();
    assert_eq!(book.len(), 0);
    drop(book);
    assert_eq!(source.stats().used, 0);
}

#[test]
fn admission_envelope_cannot_fund_an_increment_identity_even_with_the_same_definition_id() {
    let core = fixture::running(&[(ValidationMode::Required, false)]);
    let source = source();
    let mut book = CompletionBook::new(&source, core.limits).unwrap();
    let baseline = source.stats();
    let (binding, envelope, schemas) = parts(&core, &source, 1);
    assert!(
        book.install_begin(
            EvaluationKey {
                target: EvaluationTarget::Increment {
                    artifact: focal_model::ArtifactId::from_u128(880)
                },
                ..fixture::key(1)
            },
            binding,
            envelope,
            schemas,
            registration(&core, 1),
        )
        .is_err()
    );
    assert_eq!(source.stats(), baseline);
    assert_eq!(book.entries.capacity(), 0);
}

#[test]
fn recovered_partial_credit_counts_only_its_remaining_reports() {
    let core = fixture::running(&[(ValidationMode::Required, true)]);
    let source = source();
    let mut book = CompletionBook::new(&source, core.limits).unwrap();
    let (binding, envelope, schemas) = parts(&core, &source, 1);
    assert_eq!(envelope.reports(), 3);
    book.install_recovered(
        fixture::key(1),
        binding,
        1,
        envelope,
        schemas,
        registration(&core, 1),
    )
    .unwrap();
    assert_eq!(book.totals.reports, 1);
}
