//! Actual Core receipt/response history drives the shared accounting journal.
//! These tests inspect private credit only after checked native preparation.
use super::*;
use crate::native::completion_envelope::{EvidenceBounds, descriptor_limits};
use crate::native::report_tests::{self as fixture, ISSUER, SUBJECT};
use focal_evidence::BuiltinNativeSchemas;
use focal_model::{Confidence, OutcomeKind};

fn core(count: u128) -> Core<NativeState> {
    let limits = NativeLimits {
        plan_nodes: 16,
        plan_edges: 65_536,
        preparation_bytes: 16 * 1024 * 1024,
        diagnostics_per_cycle: 4,
        response_summary_bytes: 128,
        range: RangeConfig {
            max_batch_entries: 256,
            page_entries: 4,
            ..RangeConfig::default()
        },
        ..NativeLimits::default()
    };
    let mut core = Core::new_native(
        fixture::binding(1).ledger,
        RangeId(928),
        limits,
        MemoryBudget::new(256 * 1024 * 1024, 16 * 1024 * 1024).unwrap(),
    )
    .unwrap();
    for id in 1..=count {
        fixture::publish(
            &mut core,
            (id * 3) as u64,
            fixture::creation(id * 3, id, &[], None),
        );
        fixture::publish(
            &mut core,
            (id * 3 + 1) as u64,
            NativeInput {
                request: fixture::request(ISSUER, id * 3 + 1),
                command: NativeCommand::Post {
                    expected: fixture::binding(id),
                },
            },
        );
        let expected = core.native_claim(ClaimId::from_u128(id)).unwrap().binding();
        fixture::publish(
            &mut core,
            (id * 3 + 2) as u64,
            NativeInput {
                request: fixture::request(SUBJECT, id * 3 + 2),
                command: NativeCommand::AcquireReceipt {
                    expected,
                    receipt: ReceiptId::from_u128(700 + id),
                },
            },
        );
    }
    core
}
fn source() -> MemoryBudget {
    MemoryBudget::new(usize::MAX / 16, 0).unwrap()
}
fn view(core: &Core<NativeState>) -> View<'_> {
    View {
        state: &core.state,
        tail: None,
    }
}
fn quote(
    view: &View<'_>,
    id: u128,
    limits: NativeLimits,
) -> (RespondentEnvelope, NativeVerificationBudget) {
    let claim = view.claim(ClaimId::from_u128(id)).unwrap();
    let registry = view
        .owned_claim(ClaimId::from_u128(id))
        .unwrap()
        .registrations()
        .unwrap();
    let verification = NativeVerificationBudget::for_schema(
        focal_evidence::error_report_schema(),
        &BuiltinNativeSchemas,
    )
    .unwrap();
    let envelope = RespondentEnvelope::derive(
        view,
        claim,
        limits,
        descriptor_limits(limits, claim, registry).unwrap(),
        EvidenceBounds {
            workspace_bytes: verification.peak_bytes() - verification.retained_bytes(),
            retained_bytes: verification.retained_bytes(),
        },
    )
    .unwrap();
    (envelope, verification)
}
fn install(book: &mut CompletionBook, view: &View<'_>, id: u128) -> Journal {
    let (envelope, verification) = quote(view, id, book.limits);
    book.install_respondent(
        view,
        view.claim(ClaimId::from_u128(id)).unwrap(),
        &envelope,
        verification,
    )
    .unwrap()
}
fn prepared(
    core: &Core<NativeState>,
    pending: &[&NativePrepared],
    id: u128,
    actor: ParticipantId,
    command: NativeCommand,
) -> NativePrepared {
    let NativePreparation::Prepared(row) = core
        .prepare_native(
            fixture::context(actor, id as u64),
            NativeInput {
                request: fixture::request(actor, id),
                command,
            },
            pending,
        )
        .unwrap()
    else {
        panic!("fresh")
    };
    row
}
fn close(claim: Binding, id: u128) -> NativeCommand {
    NativeCommand::CloseResponse {
        claim,
        response: fixture::binding(id),
        report: NativeResponseInput {
            summary: "The cycle is complete.".into(),
            confidence: Confidence::Committed,
            outcome: OutcomeKind::Complete,
            manifest: Vec::new(),
            diagnostics: Vec::new(),
        },
    }
}
fn credit(book: &CompletionBook, key: RespondentKey) -> RespondentCredit {
    book.respondents.get(key).unwrap().credit
}

#[test]
fn receipt_install_rollback_and_parent_refusal_restore_exact_index_and_backing() {
    let core = core(1);
    let view = view(&core);
    let source = source();
    let mut book = CompletionBook::new(&source, core.limits).unwrap();
    let baseline = source.stats();
    let journal = install(&mut book, &view, 1);
    assert_eq!(book.respondents.len(), 1);
    assert_eq!(book.totals.reports, 12);
    assert_eq!(book.totals.slots.responses, 4);
    assert!(source.stats().used > baseline.used);
    book.rollback(journal).unwrap();
    assert_eq!(book.respondents.capacity(), 0);
    assert_eq!(book.totals, Totals::default());
    assert_eq!(source.stats(), baseline);

    let limited = MemoryBudget::new(1024 * 1024, 0).unwrap();
    let mut refused = CompletionBook::new(&limited, core.limits).unwrap();
    let before = limited.stats();
    let (envelope, verification) = quote(&view, 1, core.limits);
    assert!(
        refused
            .install_respondent(
                &view,
                view.claim(ClaimId::from_u128(1)).unwrap(),
                &envelope,
                verification
            )
            .is_err()
    );
    assert_eq!(refused.respondents.capacity(), 0);
    assert_eq!(refused.totals, Totals::default());
    assert_eq!(limited.stats(), before);
    book.install_recovered_respondent(
        &view,
        view.claim(ClaimId::from_u128(1)).unwrap(),
        &envelope,
        verification,
    )
    .unwrap();
    assert_eq!(book.journals, 0);
    assert_eq!(book.totals.reports, 12);
}

#[test]
fn generated_post_credit_survives_older_close_commit_and_younger_tail_rollback() {
    let mut core = core(1);
    let source = source();
    let mut book = CompletionBook::new(&source, core.limits).unwrap();
    let installed = install(&mut book, &view(&core), 1);
    book.commit(installed).unwrap();
    let key = key(core.native_claim(ClaimId::from_u128(1)).unwrap()).unwrap();
    let initial = credit(&book, key);
    let close = prepared(
        &core,
        &[],
        100,
        SUBJECT,
        close(core.native_claim(key.claim).unwrap().binding(), 900),
    );
    let first = book
        .apply_respondents(&view(&core), &close, JournalFunding::HeldCompletion)
        .unwrap();
    let after_close = credit(&book, key);
    assert_eq!(
        after_close,
        RespondentCredit {
            diagnostics: 3,
            closes: 3,
            posts: 4
        }
    );
    let tail = View {
        state: &core.state,
        tail: Some(&close),
    };
    let response = super::super::super::response_reads::as_response(
        tail.get(Key::Response(TestamentId::from_u128(900))),
    )
    .unwrap();
    let post = prepared(
        &core,
        &[&close],
        101,
        SUBJECT,
        NativeCommand::PostResponse {
            claim: tail.claim(key.claim).unwrap().binding(),
            expected: response.identity().binding,
        },
    );
    let second = book
        .apply_respondents(&tail, &post, JournalFunding::HeldCompletion)
        .unwrap();
    let advanced = credit(&book, key);
    assert_eq!(advanced.posts, 3);
    let totals = book.totals;
    core.publish_native(close).unwrap();
    book.commit_candidate(CandidateJournal::single(first))
        .unwrap();
    assert_eq!(credit(&book, key), advanced);
    assert_eq!(book.totals, totals);
    drop(post);
    book.rollback_candidate(CandidateJournal::single(second))
        .unwrap();
    assert_eq!(credit(&book, key), after_close);
    assert_eq!(book.totals.reports, 10);
    assert!(book.totals.reports < initial.actions().unwrap());
    book.respondent_contract(
        key,
        RespondentSpend::Post,
        &view(&core),
        &BuiltinNativeSchemas,
    )
    .unwrap();
}

#[test]
fn adoption_composes_retirement_with_exact_new_receipt_install_and_refunds_tail() {
    let core = core(1);
    let source = source();
    let mut book = CompletionBook::new(&source, core.limits).unwrap();
    let installed = install(&mut book, &view(&core), 1);
    book.commit(installed).unwrap();
    let claim = core.native_claim(ClaimId::from_u128(1)).unwrap();
    let old = key(claim).unwrap();
    let before = (
        book.totals,
        source.stats(),
        book.respondents.capacity(),
        book.funded_capacity(),
    );
    let adopted = prepared(
        &core,
        &[],
        102,
        ISSUER,
        NativeCommand::AdoptReceipt {
            expected: claim.binding(),
            previous: claim.receipt().unwrap().fence,
            receipt: ReceiptId::from_u128(702),
            holder: ParticipantId::from_u128(85),
        },
    );
    let update = book
        .apply_respondents(
            &view(&core),
            &adopted,
            JournalFunding::External {
                source: &source,
                lane: BudgetLane::Ordinary,
            },
        )
        .unwrap();
    assert_eq!(credit(&book, old), RespondentCredit::default());
    let candidate = View {
        state: &core.state,
        tail: Some(&adopted),
    };
    let new = key(candidate.claim(old.claim).unwrap()).unwrap();
    let install = install(&mut book, &candidate, 1);
    assert_eq!(book.respondents.len(), 2);
    assert_eq!(credit(&book, new).actions().unwrap(), 12);
    let empty = CandidateJournal::single(Journal::empty());
    book.check_respondent_composition(&empty, &update, &install)
        .unwrap();
    let combined = empty.with_respondents(update, install);
    let totals = book.totals;
    let budget = source.stats();
    // An already-applied successor cannot masquerade as this candidate's base.
    assert!(
        book.apply_respondents(&candidate, &adopted, JournalFunding::HeldCompletion)
            .is_err()
    );
    assert_eq!(book.totals, totals);
    assert_eq!(source.stats(), budget);
    drop(adopted);
    book.rollback_candidate(combined).unwrap();
    assert_eq!(
        (
            book.totals,
            source.stats(),
            book.respondents.capacity(),
            book.funded_capacity()
        ),
        before
    );
    assert_eq!(credit(&book, old).actions().unwrap(), 12);
    assert!(book.respondents.get(new).is_none());
}

#[test]
fn actual_batch_supersession_retires_multiple_receipts_with_a_refundable_held_journal() {
    use focal_model::lifecycle::succession::{Correction, CorrectionKind, Lineage};
    use focal_model::{Cause, ObjectRef, RootCommandId};
    let core = core(2);
    let source = source();
    let mut book = CompletionBook::new(&source, core.limits).unwrap();
    for id in [1, 2] {
        let journal = install(&mut book, &view(&core), id);
        book.commit(journal).unwrap();
    }
    let mut claims = Vec::new();
    let mut declarations = Vec::new();
    for (id, predecessor) in [(3, 1), (4, 2)] {
        let NativeCommand::Create {
            claims: mut authored,
            declarations: definitions,
        } = fixture::creation(110, id, &[], None).command
        else {
            panic!("creation")
        };
        authored[0].definition.lineage = Lineage::new(
            fixture::binding(id),
            Cause::Root(RootCommandId::from_u128(id)),
            &[Correction {
                kind: CorrectionKind::Supersedes,
                predecessor: ObjectRef::claim(
                    fixture::binding(1).ledger,
                    ClaimId::from_u128(predecessor),
                ),
            }],
            1,
        )
        .unwrap();
        claims.extend(authored);
        declarations.extend(definitions);
    }
    let candidate = prepared(
        &core,
        &[],
        110,
        ISSUER,
        NativeCommand::Create {
            claims,
            declarations,
        },
    );
    for id in [1, 2] {
        assert_eq!(
            candidate.claim(ClaimId::from_u128(id)).unwrap().status(),
            ClaimStatus::Superseded
        );
    }
    let baseline = (book.totals, source.stats(), book.source().stats());
    let configured = book.limits.plan_edges;
    book.limits.plan_edges = 0;
    assert!(matches!(
        book.apply_respondents(&view(&core), &candidate, JournalFunding::HeldCompletion),
        Err(NativeError::Capacity("respondent journal visits"))
    ));
    book.limits.plan_edges = configured;
    assert_eq!(
        (book.totals, source.stats(), book.source().stats()),
        baseline
    );
    let blocked = book
        .source()
        .reserve(
            BudgetKind::Pending,
            BudgetLane::Completion,
            book.pool.available(),
        )
        .unwrap()
        .commit();
    let pressured = source.stats();
    assert!(
        book.apply_respondents(&view(&core), &candidate, JournalFunding::HeldCompletion)
            .is_err()
    );
    assert_eq!(book.totals, baseline.0);
    assert_eq!(source.stats(), pressured);
    drop(blocked);
    let journal = book
        .apply_respondents(&view(&core), &candidate, JournalFunding::HeldCompletion)
        .unwrap();
    assert!(matches!(journal.change, Change::RespondentMany { .. }));
    assert_eq!(book.totals, Totals::default());
    assert_eq!(book.respondents.len(), 2);
    assert!(book.source().stats().used > baseline.2.used);
    drop(candidate);
    book.rollback(journal).unwrap();
    assert_eq!(
        (book.totals, source.stats(), book.source().stats()),
        baseline
    );
}
