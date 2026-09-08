//! Actual native histories establish every respondent credit; corruptions are
//! confined to copies of already published rows and never participant commands.
use super::*;
use crate::native::respondent_state::{
    self as state, RespondentCredit, RespondentKey, RespondentSpend,
};
use focal_memory::{BudgetLane, Change, Entry};

fn limits() -> NativeLimits {
    NativeLimits {
        plan_nodes: 16,
        plan_edges: 65_536,
        // These histories need two work slots and three diagnostics, not the
        // default large per-action receipt allowance. Establish limits before
        // receipt so these are actual fundable promises, not runtime changes.
        preparation_bytes: 2 * 1024 * 1024,
        evaluations_per_claim: 32,
        work_artifacts_per_cycle: 2,
        diagnostics_per_cycle: 3,
        response_summary_bytes: 256,
        range: RangeConfig {
            max_batch_entries: 32,
            page_entries: 4,
            page_bytes: 4096,
            max_entry_bytes: 64 * 1024,
            ..RangeConfig::default()
        },
        ..NativeLimits::default()
    }
}
fn credit(view: &NativeView<'_>) -> Option<(RespondentKey, RespondentCredit)> {
    state::read(
        view.source_view(),
        view.claim(ClaimId::from_u128(1)).unwrap(),
        limits(),
    )
    .unwrap()
}
fn expected(diagnostics: u32, closes: u32, posts: u32) -> RespondentCredit {
    RespondentCredit {
        diagnostics,
        closes,
        posts,
    }
}
fn selected(
    f: &Fixture,
    actor: ParticipantId,
    command: &NativeCommand,
) -> Result<Option<(RespondentKey, RespondentSpend)>, NativeError> {
    let view = f.owner.effective();
    state::spend(
        view.source_view(),
        view.claim(ClaimId::from_u128(1)).unwrap(),
        context(actor, 100),
        command,
        limits(),
    )
}
fn diagnostic(f: &mut Fixture, id: u128, reason: EvidenceFailure) -> ArtifactRef {
    let artifact = f.artifact(id, WorkRole::Diagnostic { reason });
    let reference = ArtifactRef {
        id: artifact.get().unwrap().id(),
        hash: artifact.get().unwrap().content_hash(),
    };
    f.commit(
        SUBJECT,
        NativeCommand::SubmitDiagnostic {
            claim: f.claim(),
            reason,
            artifact,
        },
    );
    reference
}

#[test]
fn only_first_actual_work_diagnostic_reduces_credit_and_close_discard_restores_it() {
    let mut f = Fixture::with_limits(limits());
    let initial = credit(&f.owner.committed()).unwrap();
    assert_eq!(initial.1, expected(4, 4, 4));
    assert_eq!(initial.1.actions().unwrap(), 12);
    let production = diagnostic(&mut f, 80_001, EvidenceFailure::Production);
    assert_eq!(credit(&f.owner.committed()), Some(initial));
    let first = NativeCommand::SubmitDiagnostic {
        claim: f.claim(),
        reason: EvidenceFailure::Work,
        artifact: f.artifact(
            80_002,
            WorkRole::Diagnostic {
                reason: EvidenceFailure::Work,
            },
        ),
    };
    assert_eq!(
        selected(&f, SUBJECT, &first).unwrap(),
        Some((initial.0, RespondentSpend::Diagnostic))
    );
    assert!(selected(&f, ISSUER, &first).is_err());
    f.commit(SUBJECT, first);
    assert_eq!(credit(&f.owner.committed()).unwrap().1, expected(3, 4, 4));
    let second = NativeCommand::SubmitDiagnostic {
        claim: f.claim(), reason: EvidenceFailure::Work,
        artifact: NativeArtifactInput::new(descriptor(ArtifactSpec {
            ledger: f.parent().ledger,
            id: ArtifactId::from_u128(80_003),
            schema: 1,
            kind: "error",
            schema_hash: error_report_schema(),
            metadata: b"{}",
            payload: PayloadSpec::Inline(br#"{"code":"another_failure","message":"A separate optional error was observed."}"#),
            producer: SUBJECT,
            receipt: Some(f.parent().receipt),
            result: None,
            work: Some(WorkProvenance {
                claim: f.parent().claim,
                cycle: f.parent().next_cycle,
                role: WorkRole::Diagnostic { reason: EvidenceFailure::Work },
            }),
            inputs: &[],
            visibility: &[],
        })).unwrap(),
    };
    assert_eq!(selected(&f, SUBJECT, &second).unwrap(), None);
    f.commit(SUBJECT, second);
    let view = f.owner.effective();
    let first_ref = view
        .diagnostic(ArtifactId::from_u128(80_002))
        .unwrap()
        .diagnostic
        .artifact();
    let second_ref = view
        .diagnostic(ArtifactId::from_u128(80_003))
        .unwrap()
        .diagnostic
        .artifact();
    let close = f.close(
        80_004,
        OutcomeKind::Failed,
        vec![],
        vec![production, first_ref, second_ref],
    );
    assert_eq!(
        selected(&f, SUBJECT, &close).unwrap(),
        Some((initial.0, RespondentSpend::Close))
    );
    let before = f.owner.budget_stats();
    assert_eq!(credit(&f.owner.committed()).unwrap().1, expected(3, 4, 4));
    assert_eq!(f.owner.budget_stats(), before);
    let NativeStaging::Prepared { candidate, .. } = f.stage(SUBJECT, close).unwrap() else {
        panic!("close")
    };
    assert_eq!(credit(&f.owner.effective()).unwrap().1, expected(3, 3, 4));
    assert_eq!(credit(&f.owner.committed()).unwrap().1, expected(3, 4, 4));
    assert_eq!(f.owner.discard_from(candidate).unwrap(), 1);
    assert_eq!(credit(&f.owner.effective()).unwrap().1, expected(3, 4, 4));
    f.commit(
        SUBJECT,
        f.close(
            80_004,
            OutcomeKind::Failed,
            vec![],
            vec![production, first_ref, second_ref],
        ),
    );
    let post = NativeCommand::PostResponse {
        claim: f.claim(),
        expected: f.response(80_004),
    };
    assert_eq!(
        selected(&f, SUBJECT, &post).unwrap(),
        Some((initial.0, RespondentSpend::Post))
    );
    f.commit(SUBJECT, post);
    assert_eq!(credit(&f.owner.committed()).unwrap().1, expected(3, 3, 3));
}

#[test]
fn full_response_allowance_keeps_all_generated_posts_until_the_actual_holder_posts_them() {
    let mut f = Fixture::with_limits(limits());
    for index in 0..4 {
        f.commit(
            SUBJECT,
            f.close(81_000 + index, OutcomeKind::Complete, vec![], vec![]),
        );
    }
    let key = credit(&f.owner.committed()).unwrap().0;
    assert_eq!(credit(&f.owner.committed()).unwrap().1, expected(0, 0, 4));
    let close = f.close(81_100, OutcomeKind::Complete, vec![], vec![]);
    assert_eq!(selected(&f, SUBJECT, &close).unwrap(), None);
    for (remaining, id) in [(3, 81_002), (2, 81_000), (1, 81_003), (0, 81_001)] {
        let post = NativeCommand::PostResponse {
            claim: f.claim(),
            expected: f.response(id),
        };
        assert_eq!(
            selected(&f, SUBJECT, &post).unwrap(),
            Some((key, RespondentSpend::Post))
        );
        f.commit(SUBJECT, post);
        assert_eq!(
            credit(&f.owner.committed()).unwrap().1,
            expected(0, 0, remaining)
        );
    }
}

#[test]
fn adoption_replaces_credit_without_adopting_old_generated_testimony_or_diagnostics() {
    let mut f = Fixture::with_limits(limits());
    f.commit(
        SUBJECT,
        f.close(82_000, OutcomeKind::Complete, vec![], vec![]),
    );
    diagnostic(&mut f, 82_001, EvidenceFailure::Work);
    let old = credit(&f.owner.committed()).unwrap();
    assert_eq!(old.1, expected(2, 3, 4));
    let replacement = ParticipantId::from_u128(85);
    let command = NativeCommand::AdoptReceipt {
        expected: f.claim(),
        previous: f.parent().receipt,
        receipt: ReceiptId::from_u128(702),
        holder: replacement,
    };
    let NativeStaging::Prepared { candidate, .. } = f.stage(ISSUER, command).unwrap() else {
        panic!("adoption")
    };
    let adopted = credit(&f.owner.effective()).unwrap();
    assert_eq!(
        adopted.0,
        RespondentKey {
            claim: old.0.claim,
            receipt: ReceiptId::from_u128(702),
            epoch: 2
        }
    );
    assert_eq!(adopted.1, expected(3, 3, 3));
    let old_post = NativeCommand::PostResponse {
        claim: f.claim(),
        expected: f.response(82_000),
    };
    assert!(selected(&f, SUBJECT, &old_post).is_err());
    assert!(selected(&f, replacement, &old_post).is_err());
    assert_eq!(credit(&f.owner.committed()), Some(old));
    f.owner.discard_from(candidate).unwrap();
    assert_eq!(credit(&f.owner.effective()), Some(old));
    f.commit(
        ISSUER,
        NativeCommand::Cancel {
            expected: f.claim(),
        },
    );
    assert_eq!(credit(&f.owner.committed()), None);
}

fn copied(f: &Fixture) -> Core<NativeState> {
    let source = f.owner.committed();
    let mut core = Core::new_native(
        source.ledger(),
        RangeId(92_100),
        limits(),
        MemoryBudget::new(128 * 1024 * 1024, 16 * 1024 * 1024).unwrap(),
    )
    .unwrap();
    core.state.rows = RangeStore::new(
        RangeId(92_100),
        source.sequence().0 - 1,
        core.limits.range,
        core.state.budget.clone(),
    )
    .unwrap();
    let changes = source
        .source_view()
        .state
        .rows
        .entries()
        .map(|entry| {
            Change::Put(Entry::new(
                entry.key,
                prepare::copy(&entry.value).unwrap(),
                entry.heap_bytes,
            ))
        })
        .collect();
    let prepared = core
        .state
        .rows
        .prepare_batch_with(
            source.sequence().0,
            changes,
            BudgetLane::Ordinary,
            prepare::copy,
        )
        .unwrap();
    core.state.rows.publish(prepared).unwrap();
    core
}
fn altered(core: &mut Core<NativeState>, changes: Vec<Change<Key, Row>>) {
    let prepared = core
        .state
        .rows
        .prepare_batch_with(
            core.native_sequence().0 + 1,
            changes,
            BudgetLane::Ordinary,
            prepare::copy,
        )
        .unwrap();
    core.state.rows.publish(prepared).unwrap();
}
fn from_core(
    core: &Core<NativeState>,
    configured: NativeLimits,
) -> Result<Option<(RespondentKey, RespondentCredit)>, NativeError> {
    let view = View {
        state: &core.state,
        tail: None,
    };
    state::read(
        &view,
        core.native_claim(ClaimId::from_u128(1)).unwrap(),
        configured,
    )
}

#[test]
fn bounded_reader_refuses_broken_diagnostic_membership_and_substituted_response_identity() {
    let mut f = Fixture::with_limits(limits());
    let diagnostic = diagnostic(&mut f, 83_001, EvidenceFailure::Work);
    let mut core = copied(&f);
    let mut broken = *core.native_diagnostic(diagnostic.id).unwrap();
    broken.next = Some(diagnostic.id);
    let row = crate::native::work_owned::OwnedDiagnostic::new(broken).unwrap();
    let heap = row.heap_charge().unwrap();
    altered(
        &mut core,
        vec![Change::Put(Entry::new(
            Key::Diagnostic(diagnostic.id),
            Row::Diagnostic(row),
            heap,
        ))],
    );
    assert!(from_core(&core, limits()).is_err());
    let mut core = copied(&f);
    altered(
        &mut core,
        vec![Change::Delete(Key::ArtifactIdentity(diagnostic.hash))],
    );
    assert!(from_core(&core, limits()).is_err());
    f.commit(
        SUBJECT,
        f.close(83_002, OutcomeKind::Failed, vec![], vec![diagnostic]),
    );
    f.commit(
        SUBJECT,
        f.close(83_003, OutcomeKind::Complete, vec![], vec![]),
    );
    let mut core = copied(&f);
    let replacement = prepare::copy(
        core.state
            .rows
            .get(&Key::Response(TestamentId::from_u128(83_002)))
            .unwrap(),
    )
    .unwrap();
    let Row::Response(ref row) = replacement else {
        panic!("response")
    };
    let heap = row.heap_charge().unwrap();
    altered(
        &mut core,
        vec![Change::Put(Entry::new(
            Key::Response(TestamentId::from_u128(83_003)),
            replacement,
            heap,
        ))],
    );
    assert!(from_core(&core, limits()).is_err());
    let core = copied(&f);
    let mut low = limits();
    low.plan_edges = 2;
    assert!(from_core(&core, low).is_err());
    assert_eq!(
        from_core(&core, limits()).unwrap().unwrap().1,
        expected(2, 2, 4)
    );
}

#[test]
fn posted_claim_without_responsibility_has_no_respondent_credit() {
    let core = crate::native::report_tests::running(&[]);
    assert_eq!(from_core(&core, limits()).unwrap(), None);
}
