use super::*;
use crate::native::completion_envelope::{EvidenceBounds, descriptor_limits};
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
    let Some(Row::Claim(owner)) = core.state.rows.get(&Key::Claim(key.claim)) else {
        panic!("actual owner claim");
    };
    let parent = owner.claim().unwrap();
    let registrations = owner.registrations().unwrap();
    let declaration = core.native_definition(key.validation).unwrap();
    let schemas = SchemaSet::new(declaration, source, &BuiltinNativeSchemas, 16).unwrap();
    let evidence = EvidenceBounds {
        workspace_bytes: schemas
            .workspace_bytes()
            .checked_sub(schemas.custody_bytes())
            .unwrap(),
        retained_bytes: schemas.custody_bytes(),
    };
    let envelope = CompletionEnvelope::derive(
        &core.state.rows,
        core.limits,
        parent,
        registrations,
        declaration,
        descriptor_limits(core.limits, parent, registrations).unwrap(),
        evidence,
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
    let Some(Row::Claim(claim)) = core.state.rows.get(&Key::Claim(key.claim)) else {
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
fn tail_begin_restores_exact_grant_buffer_and_added_funding_with_older_pending() {
    let core = fixture::running(&[(ValidationMode::Observe, false); 2]);
    let source = source();
    let mut book = CompletionBook::new(&source, core.limits).unwrap();
    let empty = source.stats();
    let first = install(&mut book, &core, &source, 1);
    let before_second = source.stats();
    let capacity = book.entries.capacity();
    let backing = book.funded_capacity();
    let second = install(&mut book, &core, &source, 2);
    assert!(book.entries.capacity() > capacity);
    assert!(book.funded_capacity() > backing);
    assert_eq!(book.journals, 2);

    book.rollback(second).unwrap();
    assert_eq!(source.stats(), before_second);
    assert_eq!(book.entries.capacity(), capacity);
    assert_eq!(book.funded_capacity(), backing);
    assert_eq!(book.len(), 1);
    assert!(book.trim_idle().is_err());

    book.rollback(first).unwrap();
    assert_eq!(source.stats(), empty);
    assert_eq!(book.entries.capacity(), 0);
    assert_eq!(book.funded_capacity(), 0);
    book.trim_idle().unwrap();
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
    book.trim_idle().unwrap();
}

#[test]
fn published_terminal_head_and_discarded_replacement_preserve_issued_pages() {
    let core = fixture::running(&[(ValidationMode::Observe, false); 2]);
    let source = source();
    let mut book = CompletionBook::new(&source, core.limits).unwrap();
    let begin = install(&mut book, &core, &source, 1);
    book.commit(begin).unwrap();
    let first_key = fixture::key(1);
    let binding = core.native_evaluation(first_key).unwrap().binding();
    let retained = book
        .source()
        .reserve(BudgetKind::Pages, BudgetLane::Completion, 128)
        .unwrap()
        .commit();
    let report = book
        .advance(
            first_key,
            binding,
            binding.next().unwrap(),
            true,
            CompletionUse::Regular,
        )
        .unwrap();
    assert_eq!(book.totals.workspace, 0);
    let before_second_capacity = book.funded_capacity();
    let second = install(&mut book, &core, &source, 2);
    let added = match &second.change {
        Change::Begin { added_funding, .. } => *added_funding,
        _ => panic!("begin"),
    };
    // The second generation can reuse previously promised idle credit; only
    // actual issued pages can force any small additional contribution.
    assert!(added <= retained.bytes());
    book.commit(report).unwrap();
    assert!(book.remaining_reports(first_key).is_none());
    book.rollback(second).unwrap();
    assert_eq!(book.funded_capacity(), before_second_capacity);
    assert_eq!(book.source().stats().used, retained.bytes());
    assert_eq!(book.len(), 0);
    book.trim_idle().unwrap();
    assert_eq!(book.funded_capacity(), retained.bytes());
    drop(retained);
    book.trim_idle().unwrap();
    assert_eq!(book.funded_capacity(), 0);
    drop(book);
    assert_eq!(source.stats().used, 0);
}

#[test]
fn trim_waits_for_pending_report_rollback_and_never_refunds_issued_debit() {
    let core = fixture::running(&[(ValidationMode::Observe, false)]);
    let source = source();
    let mut book = CompletionBook::new(&source, core.limits).unwrap();
    let begin = install(&mut book, &core, &source, 1);
    book.commit(begin).unwrap();
    let key = fixture::key(1);
    let binding = core.native_evaluation(key).unwrap().binding();
    let backing = book.funded_capacity();
    let allocation = book
        .source()
        .reserve(BudgetKind::Pages, BudgetLane::Completion, 256)
        .unwrap()
        .commit();
    let report = book
        .advance(
            key,
            binding,
            binding.next().unwrap(),
            true,
            CompletionUse::Regular,
        )
        .unwrap();
    assert!(book.trim_idle().is_err());
    assert_eq!(book.funded_capacity(), backing);
    // Owner ordering: dispose the candidate's pages before restoring credit.
    drop(allocation);
    book.rollback(report).unwrap();
    assert_eq!(book.remaining_reports(key), Some(2));
    assert_eq!(book.funded_capacity(), backing);
    book.trim_idle().unwrap();
    assert_eq!(book.funded_capacity(), backing);
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
fn failed_pool_growth_restores_existing_metadata_and_parent_exactly() {
    let core = fixture::running(&[(ValidationMode::Observe, false)]);
    let source = source();
    let mut book = CompletionBook::new(&source, core.limits).unwrap();
    let baseline = source.stats();
    let (binding, envelope, schemas) = parts(&core, &source, 1);
    // Leave exactly the grant-vector allocation available, forcing failure only
    // after new metadata was allocated and before the grant was installed.
    let metadata = CompletionIndex::<Grant>::slot_charge(1).unwrap();
    let pressure = source
        .reserve(
            BudgetKind::Index,
            BudgetLane::Ordinary,
            source.reservation_limit(BudgetLane::Ordinary) - source.stats().used - metadata,
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
    assert_eq!(book.funded_capacity(), 0);
    assert_eq!(book.journals, 0);
    drop(pressure);
    assert_eq!(source.stats(), baseline);
}

#[test]
fn serial_and_outcome_margins_include_reports_and_one_control() {
    let core = fixture::running(&[(ValidationMode::Observe, false)]);
    let source = source();
    let mut book = CompletionBook::new(&source, core.limits).unwrap();
    let begin = install(&mut book, &core, &source, 1);
    assert!(book.check_serial(u64::MAX - 2).is_err());
    book.check_serial(u64::MAX - 3).unwrap();
    let mut meta = Meta {
        outcomes: book.limits.outcomes - 3,
        ..Meta::default()
    };
    book.check_slots(meta, SessionSeq(u64::MAX - 3), 0).unwrap();
    meta.outcomes += 1;
    assert!(book.check_slots(meta, SessionSeq(0), 0).is_err());
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
        EvidenceBounds {
            workspace_bytes: schemas.workspace_bytes() - schemas.custody_bytes(),
            retained_bytes: schemas.custody_bytes(),
        },
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
    assert_eq!(book.totals.slots.events, 3);
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
    book.trim_idle().unwrap();
    assert_eq!(book.len(), 0);
    assert_eq!(book.funded_capacity(), 0);
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
    assert_eq!(book.funded_capacity(), 0);
}

#[test]
fn unequal_envelope_demands_survive_partial_reports_head_commits_and_tail_rollback() {
    // Synthetic finite-record profiles exercise the accounting seam; no new
    // WholeWork target or mismatched physical report is admitted by this test.
    let core = fixture::running(&[
        (ValidationMode::Required, false),
        (ValidationMode::Observe, true),
    ]);
    let source = source();
    let mut book = CompletionBook::new(&source, core.limits).unwrap();
    let (a_binding, a, a_schemas) = parts(&core, &source, 1);
    let a = a
        .with_slot_demand_for_test(
            CompletionSlots {
                artifacts: 2,
                identities: 2,
                results: 2,
                outcomes: 1,
                events: 11,
                sequences: 1,
                new_rows: 20,
                ..CompletionSlots::default()
            },
            Some(CompletionSlots {
                events: 3,
                new_rows: 4,
                ..CompletionSlots::default()
            }),
        )
        .unwrap();
    let (b_binding, b, b_schemas) = parts(&core, &source, 2);
    let b = b
        .with_slot_demand_for_test(
            CompletionSlots {
                artifacts: 1,
                identities: 1,
                results: 1,
                outcomes: 1,
                events: 6,
                sequences: 1,
                new_rows: 10,
                ..CompletionSlots::default()
            },
            None,
        )
        .unwrap();
    let a_begin = book
        .install_begin(
            fixture::key(1),
            a_binding,
            a,
            a_schemas,
            registration(&core, 1),
        )
        .unwrap();
    let b_begin = book
        .install_begin(
            fixture::key(2),
            b_binding,
            b,
            b_schemas,
            registration(&core, 2),
        )
        .unwrap();
    let full = book.totals;
    assert_eq!(
        full.slots,
        CompletionSlots {
            artifacts: 7,
            identities: 7,
            results: 7,
            outcomes: 5,
            events: 43,
            sequences: 5,
            new_rows: 74,
            ..CompletionSlots::default()
        }
    );
    assert_eq!(full.reports, 5);
    let first = book
        .advance(
            fixture::key(1),
            a_binding,
            a_binding.next().unwrap(),
            false,
            CompletionUse::Regular,
        )
        .unwrap();
    let after_first = book.totals;
    assert_eq!(
        after_first.slots,
        CompletionSlots {
            artifacts: 5,
            identities: 5,
            results: 5,
            outcomes: 4,
            events: 32,
            sequences: 4,
            new_rows: 54,
            ..CompletionSlots::default()
        }
    );
    let second = book
        .advance(
            fixture::key(2),
            b_binding,
            b_binding.next().unwrap(),
            false,
            CompletionUse::Regular,
        )
        .unwrap();
    assert_eq!(
        book.totals.slots,
        CompletionSlots {
            artifacts: 4,
            identities: 4,
            results: 4,
            outcomes: 3,
            events: 26,
            sequences: 3,
            new_rows: 44,
            ..CompletionSlots::default()
        }
    );
    book.commit(a_begin).unwrap();
    book.commit(b_begin).unwrap();
    book.rollback(second).unwrap();
    assert_eq!(book.totals, after_first);
    book.rollback(first).unwrap();
    assert_eq!(book.totals, full);
    let failed = book
        .advance(
            fixture::key(1),
            a_binding,
            a_binding.next().unwrap(),
            true,
            CompletionUse::AdmissionFailure,
        )
        .unwrap();
    assert_eq!(book.totals.slots, b.slots());
    book.commit(failed).unwrap();
    book.check_slots(Meta::default(), SessionSeq(0), 0).unwrap();
    let done = book
        .advance(
            fixture::key(2),
            b_binding,
            b_binding.next().unwrap(),
            true,
            CompletionUse::Regular,
        )
        .unwrap();
    book.commit(done).unwrap();
    assert_eq!(book.totals.slots, CompletionSlots::default());
    assert_eq!(book.totals.reports, 0);
    book.trim_idle().unwrap();
    assert_eq!(book.funded_capacity(), 0);
}

#[test]
fn recovered_partial_credit_uses_its_envelope_profile_instead_of_full_original_count() {
    let core = fixture::running(&[(ValidationMode::Required, true)]);
    let source = source();
    let mut book = CompletionBook::new(&source, core.limits).unwrap();
    let (binding, envelope, schemas) = parts(&core, &source, 1);
    let envelope = envelope
        .with_slot_demand_for_test(
            CompletionSlots {
                artifacts: 1,
                identities: 1,
                results: 2,
                outcomes: 1,
                events: 9,
                sequences: 1,
                new_rows: 15,
                ..CompletionSlots::default()
            },
            Some(CompletionSlots {
                events: 2,
                new_rows: 3,
                ..CompletionSlots::default()
            }),
        )
        .unwrap();
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
    assert_eq!(
        book.totals.slots,
        CompletionSlots {
            artifacts: 1,
            identities: 1,
            results: 2,
            outcomes: 1,
            events: 11,
            sequences: 1,
            new_rows: 18,
            ..CompletionSlots::default()
        }
    );
    assert_eq!(book.totals.reports, 1);
    let mut meta = Meta {
        events: book.limits.events - 11,
        ..Meta::default()
    };
    book.check_slots(meta, SessionSeq(0), 0).unwrap();
    meta.events += 1;
    assert!(book.check_slots(meta, SessionSeq(0), 0).is_err());
}

#[test]
fn summed_slot_overflow_refuses_before_metadata_growth_or_funding_changes() {
    let core = fixture::running(&[
        (ValidationMode::Required, false),
        (ValidationMode::Observe, false),
    ]);
    let source = source();
    let mut book = CompletionBook::new(&source, core.limits).unwrap();
    let (binding, envelope, schemas) = parts(&core, &source, 1);
    let envelope = envelope
        .with_slot_demand_for_test(
            CompletionSlots {
                artifacts: 1,
                identities: 1,
                results: 1,
                outcomes: 1,
                events: usize::MAX / 2,
                sequences: 1,
                new_rows: 7,
                ..CompletionSlots::default()
            },
            Some(CompletionSlots {
                events: 1,
                new_rows: 1,
                ..CompletionSlots::default()
            }),
        )
        .unwrap();
    let first = book
        .install_begin(
            fixture::key(1),
            binding,
            envelope,
            schemas,
            registration(&core, 1),
        )
        .unwrap();
    assert_eq!(book.totals.slots.events, usize::MAX);
    let before = source.stats();
    let totals = book.totals;
    let capacity = book.entries.capacity();
    let funding = book.funded_capacity();
    let (binding, envelope, schemas) = parts(&core, &source, 2);
    assert!(
        book.install_begin(
            fixture::key(2),
            binding,
            envelope,
            schemas,
            registration(&core, 2)
        )
        .is_err()
    );
    assert_eq!(source.stats(), before);
    assert_eq!(book.totals, totals);
    assert_eq!(book.entries.capacity(), capacity);
    assert_eq!(book.funded_capacity(), funding);
    assert_eq!(book.len(), 1);
    assert_eq!(book.journals, 1);
    book.rollback(first).unwrap();
    assert_eq!(book.totals.slots, CompletionSlots::default());
    assert_eq!(book.funded_capacity(), 0);
}
