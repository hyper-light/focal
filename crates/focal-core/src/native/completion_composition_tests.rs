//! Book accounting over actual report/seal candidates. Grant installation is
//! exercised separately from its native Begin command, as in the parent tests.
use super::*;

#[test]
fn empty_following_update_has_no_phantom_revision_or_retained_resources() {
    let core = fixture::running(&[(ValidationMode::Observe, false)]);
    let source = source();
    let mut book = CompletionBook::new(&source, core.limits).unwrap();
    let baseline = source.stats();
    let begin = install(&mut book, &core, &source, 1);
    let revision = book.revision;
    let empty = Journal::empty();
    book.check_begin_composition(&begin, &empty).unwrap();
    let candidate = CandidateJournal::begin(begin, empty);
    assert_eq!(book.revision, revision);
    assert_eq!(book.journals, 1);
    book.rollback_candidate(candidate).unwrap();
    assert_eq!(book.journals, 0);
    assert_eq!(book.revision, 0);
    assert_eq!(source.stats(), baseline);

    let begin = install(&mut book, &core, &source, 1);
    book.check_begin_composition(&begin, &Journal::empty())
        .unwrap();
    book.commit_candidate(CandidateJournal::begin(begin, Journal::empty()))
        .unwrap();
    assert_eq!(book.journals, 0);
    assert_eq!(book.len(), 1);
}

#[test]
fn begin_and_mixed_update_rollback_restore_the_exact_pre_begin_pool_and_index() {
    let core = fixture::running(&[
        (ValidationMode::Required, false),
        (ValidationMode::Observe, true),
        (ValidationMode::Observe, false),
    ]);
    let source = source();
    let mut book = CompletionBook::new(&source, core.limits).unwrap();
    for index in [1, 3] {
        let older = install(&mut book, &core, &source, index);
        book.commit(older).unwrap();
    }
    let baseline = source.stats();
    let capacity = book.entries.capacity();
    let funding = book.funded_capacity();
    let totals = book.totals;
    let revision = book.revision;
    let old = [1, 3].map(|index| book.grant(fixture::key(index)).unwrap().credit);
    let begin = install(&mut book, &core, &source, 2);
    assert!(book.entries.capacity() > capacity);
    let mut custody = fixture::Custody::new();
    let request = input(&core, None, 951, 1, VerdictValue::Fail);
    let verified = fixture::verified(&mut custody, &request);
    let (prepared, report, seals) = with_seals(&core, None, request, &verified, &[2, 3]);
    let view = View {
        state: &core.state,
        tail: None,
    };
    let update = book
        .apply_prepared(
            &view,
            &prepared,
            Some(report),
            None,
            &seals,
            JournalFunding::External {
                source: &source,
                lane: BudgetLane::Completion,
            },
        )
        .unwrap();
    assert!(matches!(update.change, Change::Many { .. }));
    book.check_begin_composition(&begin, &update).unwrap();
    let candidate = CandidateJournal::begin(begin, update);
    assert_eq!(book.journals, 2);
    assert_eq!(book.remaining_reports(fixture::key(1)), Some(0));
    assert_eq!(
        book.grant(fixture::key(2)).unwrap().credit.binding,
        seals[0].next().binding()
    );
    drop(prepared);
    book.rollback_candidate(candidate).unwrap();
    assert_eq!(book.journals, 0);
    assert_eq!(book.revision, revision);
    assert_eq!(book.totals, totals);
    assert_eq!(book.entries.capacity(), capacity);
    assert_eq!(book.funded_capacity(), funding);
    assert_eq!(source.stats(), baseline);
    assert!(book.grant(fixture::key(2)).is_err());
    assert_eq!(
        [1, 3].map(|index| book.grant(fixture::key(index)).unwrap().credit),
        old
    );
}

#[test]
fn composed_head_commit_preserves_the_new_grants_younger_pending_report() {
    let mut core = fixture::running(&[
        (ValidationMode::Required, false),
        (ValidationMode::Observe, true),
        (ValidationMode::Observe, false),
    ]);
    let source = source();
    let mut book = CompletionBook::new(&source, core.limits).unwrap();
    for index in [1, 3] {
        let older = install(&mut book, &core, &source, index);
        book.commit(older).unwrap();
    }
    let begin = install(&mut book, &core, &source, 2);
    let mut custody = fixture::Custody::new();
    let request = input(&core, None, 952, 1, VerdictValue::Fail);
    let verified = fixture::verified(&mut custody, &request);
    let (prepared, report, seals) = with_seals(&core, None, request, &verified, &[2, 3]);
    let view = View {
        state: &core.state,
        tail: None,
    };
    let update = book
        .apply_prepared(
            &view,
            &prepared,
            Some(report),
            None,
            &seals,
            JournalFunding::External {
                source: &source,
                lane: BudgetLane::Completion,
            },
        )
        .unwrap();
    book.check_begin_composition(&begin, &update).unwrap();
    let candidate = CandidateJournal::begin(begin, update);
    let sealed = book.grant(fixture::key(2)).unwrap().credit;
    let request = input(&core, Some(&prepared), 953, 2, VerdictValue::Error);
    let verified = fixture::verified(&mut custody, &request);
    let later = fixture::report(&core, request, &[&prepared], &verified);
    let view = View {
        state: &core.state,
        tail: Some(&prepared),
    };
    let later_journal = book
        .apply_prepared(
            &view,
            &later,
            Some(ReportAdvance {
                key: fixture::key(2),
                before: sealed.binding,
                usage: CompletionUse::Regular,
            }),
            None,
            &[],
            JournalFunding::HeldCompletion,
        )
        .unwrap();
    let advanced = book.grant(fixture::key(2)).unwrap().credit;
    let totals = book.totals;
    core.publish_native(prepared).unwrap();
    book.commit_candidate(candidate).unwrap();
    assert_eq!(book.journals, 1);
    assert_eq!(book.totals, totals);
    assert_eq!(book.grant(fixture::key(2)).unwrap().credit, advanced);
    assert!(book.grant(fixture::key(1)).is_err());
    assert_eq!(
        book.grant(fixture::key(3)).unwrap().credit.binding,
        seals[1].next().binding()
    );
    drop(later);
    book.rollback_candidate(CandidateJournal::single(later_journal))
        .unwrap();
    assert_eq!(book.grant(fixture::key(2)).unwrap().credit, sealed);
    assert_eq!(book.journals, 0);
}

#[test]
fn foreign_nonadjacent_and_reversed_composition_refusals_retain_both_journals() {
    let core = fixture::running(&[(ValidationMode::Observe, true); 2]);
    let source = source();
    let mut left = CompletionBook::new(&source, core.limits).unwrap();
    let mut right = CompletionBook::new(&source, core.limits).unwrap();
    let first = install(&mut left, &core, &source, 1);
    let intervening = install(&mut left, &core, &source, 2);
    let key = fixture::key(1);
    let binding = left.grant(key).unwrap().credit.binding;
    let later = left
        .advance(
            key,
            binding,
            binding.next().unwrap(),
            false,
            CompletionUse::Regular,
        )
        .unwrap();
    let foreign_begin = install(&mut right, &core, &source, 1);
    let binding = right.grant(key).unwrap().credit.binding;
    let foreign = right
        .advance(
            key,
            binding,
            binding.next().unwrap(),
            false,
            CompletionUse::Regular,
        )
        .unwrap();
    let baseline = source.stats();
    let left_state = (left.totals, left.revision, left.journals);
    let right_state = (right.totals, right.revision, right.journals);
    assert!(left.check_begin_composition(&first, &later).is_err());
    assert!(left.check_begin_composition(&later, &first).is_err());
    assert!(left.check_begin_composition(&first, &foreign).is_err());
    assert!(right.check_begin_composition(&first, &foreign).is_err());
    assert_eq!((left.totals, left.revision, left.journals), left_state);
    assert_eq!((right.totals, right.revision, right.journals), right_state);
    assert_eq!(source.stats(), baseline);
    left.rollback(later).unwrap();
    left.rollback(intervening).unwrap();
    left.rollback(first).unwrap();
    right.rollback(foreign).unwrap();
    right.rollback(foreign_begin).unwrap();
    assert_eq!(left.journals, 0);
    assert_eq!(right.journals, 0);
}
