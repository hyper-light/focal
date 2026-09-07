use super::*;

fn reference(input: &NativeArtifactInput) -> ArtifactRef {
    let source = input.get().unwrap();
    ArtifactRef {
        id: source.id(),
        hash: source.content_hash(),
    }
}

fn production_diagnostic(f: &mut Fixture, id: u128) -> ArtifactRef {
    let artifact = f.artifact(
        id,
        WorkRole::Diagnostic {
            reason: EvidenceFailure::Production,
        },
    );
    let source = reference(&artifact);
    let outcome = f.commit(
        SUBJECT,
        NativeCommand::SubmitDiagnostic {
            claim: f.claim(),
            reason: EvidenceFailure::Production,
            artifact,
        },
    );
    assert_eq!(outcome.artifacts, 1);
    source
}

fn rejection_changed(
    f: &Fixture,
    id: u128,
    target: ArtifactRef,
    reason: EvidenceFailure,
    change: impl FnOnce(&mut ArtifactSpec<'_>),
) -> NativeArtifactInput {
    let work = f.owner.effective().work(target.id).unwrap().state;
    let mut spec = ArtifactSpec {
        ledger: work.binding().ledger,
        id: ArtifactId::from_u128(id),
        schema: 1,
        kind: "error",
        schema_hash: error_report_schema(),
        metadata: b"{}",
        payload: PayloadSpec::Inline(
            br#"{"code":"unusable_output","message":"The supplied output is missing required fields."}"#,
        ),
        producer: ISSUER,
        receipt: Some(work.receipt()),
        result: None,
        work: Some(WorkProvenance {
            claim: work.claim(),
            cycle: work.cycle(),
            role: WorkRole::ReceiptRejection {
                artifact: target,
                reason,
            },
        }),
        inputs: &[],
        visibility: &[],
    };
    change(&mut spec);
    NativeArtifactInput::new(descriptor(spec)).unwrap()
}

fn reject(f: &Fixture, id: u128, target: ArtifactRef, reason: EvidenceFailure) -> NativeCommand {
    NativeCommand::RejectWork {
        claim: f.claim(),
        expected: f.owner.effective().work(target.id).unwrap().state.binding(),
        reason,
        artifact: rejection_changed(f, id, target, reason, |_| {}),
    }
}

fn fresh(staging: NativeStaging) -> (NativeCandidate, NativeOutcome) {
    let NativeStaging::Prepared { candidate, outcome } = staging else {
        panic!("expected a new candidate")
    };
    (candidate, outcome)
}

#[test]
fn production_failure_reuses_actual_diagnostic_without_inventing_an_artifact() {
    let mut f = Fixture::new();
    let diagnostic = production_diagnostic(&mut f, 801);
    let source = f.owner.committed().artifact(diagnostic.id).unwrap();
    let original = source.descriptor().binding();
    let custody = source.custody().local_revision();
    let parent = f.claim();
    let outcome = f.commit(
        SUBJECT,
        NativeCommand::FailWorkProduction {
            claim: parent,
            slot: 0,
            diagnostic,
        },
    );
    assert_eq!(outcome.artifacts, 0);
    assert_eq!(outcome.responses, 0);
    assert_eq!(outcome.events, 1);
    assert_eq!(f.claim(), parent);
    let failed = f.owner.committed().work(diagnostic.id).unwrap().state;
    assert_eq!(failed.binding(), original);
    assert_eq!(failed.state(), WorkArtifactState::GenerationFailed);
    assert_eq!(failed.attachment(), None);
    assert_eq!(failed.diagnostic().unwrap().artifact, diagnostic);
    assert_eq!(
        failed.diagnostic().unwrap().reason,
        EvidenceFailure::Production
    );
    let source = f.owner.committed().artifact(diagnostic.id).unwrap();
    assert_eq!(source.descriptor().binding(), original);
    assert_eq!(source.custody().local_revision(), custody);
    assert_eq!(
        f.owner
            .committed()
            .diagnostic(diagnostic.id)
            .unwrap()
            .diagnostic
            .artifact(),
        diagnostic
    );
    assert!(matches!(
        f.owner.committed().event(outcome.sequence, 0).unwrap().fact,
        NativeFact::Work { before: None, after, state: WorkArtifactState::GenerationFailed, .. }
            if after == original
    ));

    let close = f.commit(
        SUBJECT,
        f.close(900, OutcomeKind::Failed, vec![], vec![diagnostic]),
    );
    assert_eq!(close.artifacts, 0);
    assert_eq!(close.responses, 1);
    let response = f
        .owner
        .committed()
        .response(TestamentId::from_u128(900))
        .unwrap();
    assert!(response.manifest().is_empty());
    assert_eq!(response.failed_work().len(), 1);
    let retained = response.failed_work()[0];
    assert_eq!(retained.binding(), original);
    assert_eq!(retained.slot(), 0);
    assert_eq!(retained.state(), WorkArtifactState::GenerationFailed);
    assert_eq!(retained.diagnostic(), failed.diagnostic().unwrap());
    assert_eq!(response.diagnostics()[0].artifact(), diagnostic);
    assert_eq!(
        f.owner.committed().work(diagnostic.id).unwrap().state,
        failed
    );
}

#[test]
fn production_failure_requires_holder_exact_current_diagnostic_and_unused_declared_slot() {
    let mut f = Fixture::new();
    let diagnostic = production_diagnostic(&mut f, 801);
    let work_diagnostic = f.diagnostic(802);
    let prefix = f.owner.committed().sequence();
    for (actor, slot, source) in [
        (ISSUER, 0, diagnostic),
        (SUBJECT, 2, diagnostic),
        (SUBJECT, 0, work_diagnostic),
        (
            SUBJECT,
            0,
            ArtifactRef {
                id: ArtifactId::from_u128(999),
                ..diagnostic
            },
        ),
        (
            SUBJECT,
            0,
            ArtifactRef {
                hash: ContentHash([91; 32]),
                ..diagnostic
            },
        ),
    ] {
        assert!(
            f.stage(
                actor,
                NativeCommand::FailWorkProduction {
                    claim: f.claim(),
                    slot,
                    diagnostic: source,
                }
            )
            .is_err()
        );
        assert_eq!(f.owner.effective().sequence(), prefix);
        assert!(f.owner.effective().work(diagnostic.id).is_none());
        assert_eq!(f.owner.pending_len(), 0);
    }
    let output = f.work(803, 0);
    assert!(
        f.stage(
            SUBJECT,
            NativeCommand::FailWorkProduction {
                claim: f.claim(),
                slot: 0,
                diagnostic,
            }
        )
        .is_err()
    );
    f.commit(
        SUBJECT,
        NativeCommand::FailWorkProduction {
            claim: f.claim(),
            slot: 1,
            diagnostic,
        },
    );
    assert!(
        f.stage(
            SUBJECT,
            NativeCommand::FailWorkProduction {
                claim: f.claim(),
                slot: 1,
                diagnostic,
            }
        )
        .is_err()
    );
    f.commit(
        SUBJECT,
        f.close(
            900,
            OutcomeKind::Failed,
            vec![output],
            vec![diagnostic, work_diagnostic],
        ),
    );
    let old = f.owner.committed().work(diagnostic.id).unwrap().state;
    assert!(
        f.stage(
            SUBJECT,
            NativeCommand::FailWorkProduction {
                claim: f.claim(),
                slot: 0,
                diagnostic,
            }
        )
        .is_err()
    );
    assert_eq!(f.owner.committed().work(diagnostic.id).unwrap().state, old);
}

#[test]
fn rejection_of_generated_or_received_work_preserves_output_and_needs_respondent_report() {
    for received in [false, true] {
        let mut f = Fixture::new();
        let output = f.work(801, 0);
        let original = f
            .owner
            .committed()
            .artifact(output.artifact.id)
            .unwrap()
            .descriptor()
            .binding();
        if received {
            f.commit(
                ISSUER,
                NativeCommand::ReceiveWork {
                    claim: f.claim(),
                    expected: original,
                },
            );
        }
        let before = f
            .owner
            .committed()
            .work(output.artifact.id)
            .unwrap()
            .state
            .binding();
        let command = reject(&f, 802, output.artifact, EvidenceFailure::Structure);
        let NativeCommand::RejectWork { artifact, .. } = &command else {
            panic!("rejection")
        };
        let claimant_diagnostic = reference(artifact);
        let outcome = f.commit(ISSUER, command);
        assert_eq!(outcome.artifacts, 1);
        assert_eq!(outcome.responses, 0);
        let failed = f.owner.committed().work(output.artifact.id).unwrap().state;
        assert_eq!(failed.binding(), before.next().unwrap());
        assert_eq!(failed.state(), WorkArtifactState::ReceiptFailed);
        assert_eq!(failed.attachment(), None);
        assert_eq!(failed.diagnostic().unwrap().artifact, claimant_diagnostic);
        assert_eq!(
            f.owner
                .committed()
                .artifact(output.artifact.id)
                .unwrap()
                .descriptor()
                .binding(),
            original
        );
        assert!(
            f.owner
                .committed()
                .diagnostic(claimant_diagnostic.id)
                .is_none()
        );
        assert_eq!(
            f.owner
                .committed()
                .artifact(claimant_diagnostic.id)
                .unwrap()
                .descriptor()
                .producer(),
            ISSUER
        );
        for diagnostics in [vec![], vec![claimant_diagnostic]] {
            assert!(
                f.stage(
                    SUBJECT,
                    f.close(900, OutcomeKind::Failed, vec![], diagnostics)
                )
                .is_err()
            );
        }
        let respondent_diagnostic = f.diagnostic(803);
        assert!(
            f.stage(
                SUBJECT,
                f.close(
                    900,
                    OutcomeKind::Failed,
                    vec![output],
                    vec![respondent_diagnostic]
                )
            )
            .is_err()
        );
        f.commit(
            SUBJECT,
            f.close(
                900,
                OutcomeKind::Failed,
                vec![],
                vec![respondent_diagnostic],
            ),
        );
        let response = f
            .owner
            .committed()
            .response(TestamentId::from_u128(900))
            .unwrap();
        assert!(response.manifest().is_empty());
        assert_eq!(response.failed_work().len(), 1);
        assert_eq!(response.failed_work()[0].binding(), failed.binding());
        assert_eq!(
            response.failed_work()[0].diagnostic().artifact,
            claimant_diagnostic
        );
        assert_eq!(response.diagnostics().len(), 1);
        assert_eq!(response.diagnostics()[0].artifact(), respondent_diagnostic);
        assert_eq!(response.diagnostics()[0].producer(), SUBJECT);
        assert_eq!(
            f.owner.committed().work(output.artifact.id).unwrap().state,
            failed
        );
    }
}

#[test]
fn rejection_refuses_wrong_writer_reason_revision_target_cycle_and_reused_id() {
    let mut f = Fixture::new();
    let output = f.work(801, 0);
    let second = f.work(802, 1);
    let original = f.owner.committed().work(output.artifact.id).unwrap().state;
    let prefix = f.owner.committed().sequence();
    for scenario in 0..9 {
        let mut command = reject(&f, 803, output.artifact, EvidenceFailure::Metadata);
        let mut actor = ISSUER;
        let NativeCommand::RejectWork {
            expected,
            reason,
            artifact,
            ..
        } = &mut command
        else {
            panic!("rejection")
        };
        match scenario {
            0 => actor = SUBJECT,
            1 => *reason = EvidenceFailure::Work,
            2 => *reason = EvidenceFailure::Production,
            3 => *expected = expected.next().unwrap(),
            4 => {
                *artifact = rejection_changed(
                    &f,
                    803,
                    output.artifact,
                    EvidenceFailure::Metadata,
                    |spec| {
                        spec.work.as_mut().unwrap().role = WorkRole::ReceiptRejection {
                            artifact: second.artifact,
                            reason: EvidenceFailure::Metadata,
                        };
                    },
                )
            }
            5 => {
                *artifact = rejection_changed(
                    &f,
                    803,
                    output.artifact,
                    EvidenceFailure::Metadata,
                    |spec| {
                        spec.work.as_mut().unwrap().cycle += 1;
                    },
                )
            }
            6 => {
                *artifact =
                    rejection_changed(&f, 801, output.artifact, EvidenceFailure::Metadata, |_| {})
            }
            7 => {
                *artifact = rejection_changed(
                    &f,
                    803,
                    output.artifact,
                    EvidenceFailure::Metadata,
                    |spec| {
                        spec.producer = SUBJECT;
                    },
                )
            }
            8 => {
                *artifact = rejection_changed(
                    &f,
                    803,
                    output.artifact,
                    EvidenceFailure::Metadata,
                    |spec| {
                        spec.work.as_mut().unwrap().role = WorkRole::ReceiptRejection {
                            artifact: ArtifactRef {
                                hash: ContentHash([77; 32]),
                                ..output.artifact
                            },
                            reason: EvidenceFailure::Metadata,
                        };
                    },
                )
            }
            _ => unreachable!(),
        }
        assert!(f.stage(actor, command).is_err(), "scenario {scenario}");
        assert_eq!(f.owner.effective().sequence(), prefix);
        assert_eq!(
            f.owner.effective().work(output.artifact.id).unwrap().state,
            original
        );
        assert!(
            f.owner
                .effective()
                .artifact(ArtifactId::from_u128(803))
                .is_none()
        );
        assert_eq!(f.owner.pending_len(), 0);
    }
    f.commit(
        ISSUER,
        reject(&f, 803, output.artifact, EvidenceFailure::Metadata),
    );
    let rejected = f.owner.committed().work(output.artifact.id).unwrap().state;
    assert!(
        f.stage(
            ISSUER,
            reject(&f, 804, output.artifact, EvidenceFailure::Structure)
        )
        .is_err()
    );
    assert_eq!(
        f.owner.committed().work(output.artifact.id).unwrap().state,
        rejected
    );
}

#[test]
fn rejection_diagnostic_requires_actual_schema_verified_custody_before_state_changes() {
    let mut f = Fixture::new();
    let output = f.work(801, 0);
    let original = f.owner.committed().work(output.artifact.id).unwrap().state;
    let prefix = f.owner.committed().sequence();
    let mut command = reject(&f, 802, output.artifact, EvidenceFailure::Metadata);
    let NativeCommand::RejectWork { artifact, .. } = &mut command else {
        panic!("rejection")
    };
    *artifact = rejection_changed(
        &f,
        802,
        output.artifact,
        EvidenceFailure::Metadata,
        |spec| {
            spec.payload = PayloadSpec::Inline(br#"{"code":"missing_message"}"#);
        },
    );
    assert!(f.stage(ISSUER, command).is_err());
    let command = reject(&f, 802, output.artifact, EvidenceFailure::Metadata);
    let input = f.input(ISSUER, command);
    assert!(f.owner.prepare(context(ISSUER, 90), input, None).is_err());
    assert_eq!(f.owner.effective().sequence(), prefix);
    assert_eq!(
        f.owner.effective().work(output.artifact.id).unwrap().state,
        original
    );
    assert!(
        f.owner
            .effective()
            .artifact(ArtifactId::from_u128(802))
            .is_none()
    );
    f.commit(
        ISSUER,
        reject(&f, 802, output.artifact, EvidenceFailure::Metadata),
    );
    assert_eq!(
        f.owner
            .committed()
            .work(output.artifact.id)
            .unwrap()
            .state
            .state(),
        WorkArtifactState::ReceiptFailed
    );
}

#[test]
fn attached_output_cannot_be_rejected_or_replaced_by_a_production_failure() {
    let mut f = Fixture::new();
    let output = f.work(801, 0);
    let production = production_diagnostic(&mut f, 802);
    f.commit(
        SUBJECT,
        f.close(900, OutcomeKind::Partial, vec![output], vec![production]),
    );
    let attached = f.owner.committed().work(output.artifact.id).unwrap().state;
    assert_eq!(attached.attachment(), Some(TestamentId::from_u128(900)));
    assert!(
        f.stage(
            ISSUER,
            reject(&f, 803, output.artifact, EvidenceFailure::Structure)
        )
        .is_err()
    );
    assert!(
        f.stage(
            SUBJECT,
            NativeCommand::FailWorkProduction {
                claim: f.claim(),
                slot: 0,
                diagnostic: production
            }
        )
        .is_err()
    );
    assert_eq!(
        f.owner.committed().work(output.artifact.id).unwrap().state,
        attached
    );
    assert!(
        f.owner
            .committed()
            .artifact(ArtifactId::from_u128(803))
            .is_none()
    );
    assert!(
        f.owner
            .committed()
            .response(TestamentId::from_u128(900))
            .unwrap()
            .failed_work()
            .is_empty()
    );
}

#[test]
fn issuer_can_reject_unattached_work_after_parent_cancellation_without_repainting_parent() {
    let mut f = Fixture::new();
    let output = f.work(801, 0);
    let original = f.owner.committed().work(output.artifact.id).unwrap().state;
    f.commit(
        ISSUER,
        NativeCommand::Cancel {
            expected: f.claim(),
        },
    );
    let terminal = f.claim();
    f.commit(
        ISSUER,
        reject(&f, 802, output.artifact, EvidenceFailure::Metadata),
    );
    let rejected = f.owner.committed().work(output.artifact.id).unwrap().state;
    assert_eq!(rejected.state(), WorkArtifactState::ReceiptFailed);
    assert_eq!(rejected.cycle(), original.cycle());
    assert_eq!(rejected.receipt(), original.receipt());
    assert_eq!(f.claim(), terminal);
    assert!(
        f.owner
            .committed()
            .response(TestamentId::from_u128(900))
            .is_none()
    );
    assert!(
        f.stage(SUBJECT, f.close(900, OutcomeKind::Failed, vec![], vec![]))
            .is_err()
    );
}

#[test]
fn pending_diagnostic_failure_and_close_share_one_discardable_effective_chain() {
    let mut f = Fixture::new();
    let base = f.owner.committed().sequence();
    let parent = f.claim();
    let read = f.owner.pin(0, 100).unwrap();
    let budget = f.owner.budget_stats();
    let artifact = f.artifact(
        801,
        WorkRole::Diagnostic {
            reason: EvidenceFailure::Production,
        },
    );
    let diagnostic = reference(&artifact);
    let (first, first_outcome) = fresh(
        f.stage(
            SUBJECT,
            NativeCommand::SubmitDiagnostic {
                claim: parent,
                reason: EvidenceFailure::Production,
                artifact,
            },
        )
        .unwrap(),
    );
    let (_, failed_outcome) = fresh(
        f.stage(
            SUBJECT,
            NativeCommand::FailWorkProduction {
                claim: parent,
                slot: 0,
                diagnostic,
            },
        )
        .unwrap(),
    );
    let (_, closed_outcome) = fresh(
        f.stage(
            SUBJECT,
            f.close(900, OutcomeKind::Failed, vec![], vec![diagnostic]),
        )
        .unwrap(),
    );
    assert_eq!(f.owner.pending_len(), 3);
    assert!(f.owner.committed().artifact(diagnostic.id).is_none());
    assert!(f.owner.committed().work(diagnostic.id).is_none());
    assert!(
        f.owner
            .committed()
            .response(TestamentId::from_u128(900))
            .is_none()
    );
    assert_eq!(
        f.owner
            .effective()
            .response(TestamentId::from_u128(900))
            .unwrap()
            .failed_work()
            .len(),
        1
    );
    assert_eq!(
        read.with_work(diagnostic.id, 0, |row| row.state.state())
            .unwrap(),
        None
    );
    assert_eq!(f.owner.discard_from(first).unwrap(), 3);
    assert_eq!(f.owner.committed().sequence(), base);
    assert_eq!(f.claim(), parent);
    assert_eq!(f.owner.budget_stats(), budget);
    for outcome in [first_outcome, failed_outcome, closed_outcome] {
        assert!(f.owner.effective().recorded(outcome.request).is_none());
        assert!(f.owner.effective().event(outcome.sequence, 0).is_none());
    }
    assert!(f.owner.effective().artifact(diagnostic.id).is_none());
    assert!(f.owner.effective().diagnostic(diagnostic.id).is_none());
    assert!(f.owner.effective().work(diagnostic.id).is_none());
    assert!(
        f.owner
            .effective()
            .response(TestamentId::from_u128(900))
            .is_none()
    );
    f.owner.release(&read).unwrap();
    // Custody may remain on disk; the discarded ledger IDs and slot are reusable.
    assert_eq!(production_diagnostic(&mut f, 801), diagnostic);
    f.commit(
        SUBJECT,
        NativeCommand::FailWorkProduction {
            claim: f.claim(),
            slot: 0,
            diagnostic,
        },
    );
    f.commit(
        SUBJECT,
        f.close(900, OutcomeKind::Failed, vec![], vec![diagnostic]),
    );
}

#[test]
fn pending_rejection_close_discard_restores_original_work_and_removes_both_diagnostics() {
    let mut f = Fixture::new();
    let output = f.work(801, 0);
    let original = f.owner.committed().work(output.artifact.id).unwrap().state;
    let parent = f.claim();
    let read = f.owner.pin(0, 100).unwrap();
    let budget = f.owner.budget_stats();
    let (first, rejection) = fresh(
        f.stage(
            ISSUER,
            reject(&f, 802, output.artifact, EvidenceFailure::Metadata),
        )
        .unwrap(),
    );
    let artifact = f.artifact(
        803,
        WorkRole::Diagnostic {
            reason: EvidenceFailure::Work,
        },
    );
    let diagnostic = reference(&artifact);
    let (_, respondent) = fresh(
        f.stage(
            SUBJECT,
            NativeCommand::SubmitDiagnostic {
                claim: f.claim(),
                reason: EvidenceFailure::Work,
                artifact,
            },
        )
        .unwrap(),
    );
    let (_, close) = fresh(
        f.stage(
            SUBJECT,
            f.close(900, OutcomeKind::Failed, vec![], vec![diagnostic]),
        )
        .unwrap(),
    );
    assert_eq!(
        f.owner
            .effective()
            .response(TestamentId::from_u128(900))
            .unwrap()
            .failed_work()[0]
            .state(),
        WorkArtifactState::ReceiptFailed
    );
    assert_eq!(
        f.owner.committed().work(output.artifact.id).unwrap().state,
        original
    );
    assert_eq!(
        read.with_work(output.artifact.id, 0, |row| row.state)
            .unwrap(),
        Some(original)
    );
    assert_eq!(f.owner.discard_from(first).unwrap(), 3);
    assert_eq!(f.claim(), parent);
    assert_eq!(f.owner.budget_stats(), budget);
    assert_eq!(
        f.owner.effective().work(output.artifact.id).unwrap().state,
        original
    );
    for id in [802, 803] {
        assert!(
            f.owner
                .effective()
                .artifact(ArtifactId::from_u128(id))
                .is_none()
        );
        assert!(
            f.owner
                .effective()
                .diagnostic(ArtifactId::from_u128(id))
                .is_none()
        );
    }
    for outcome in [rejection, respondent, close] {
        assert!(f.owner.effective().recorded(outcome.request).is_none());
    }
    assert!(
        f.owner
            .effective()
            .response(TestamentId::from_u128(900))
            .is_none()
    );
    f.owner.release(&read).unwrap();
    f.commit(
        ISSUER,
        reject(&f, 802, output.artifact, EvidenceFailure::Metadata),
    );
    let diagnostic = f.diagnostic(803);
    f.commit(
        SUBJECT,
        f.close(900, OutcomeKind::Failed, vec![], vec![diagnostic]),
    );
}

#[test]
fn mixed_success_and_failed_slots_close_at_admitted_cycle_cap_and_stay_in_separate_manifests() {
    let mut f = Fixture::with_limits(NativeLimits {
        work_artifacts_per_cycle: 2,
        plan_nodes: 16,
        plan_edges: 256,
        preparation_bytes: 1024 * 1024,
        evaluations_per_claim: 16,
        range: RangeConfig {
            max_batch_entries: 128,
            page_entries: 4,
            ..RangeConfig::default()
        },
        ..NativeLimits::default()
    });
    let failed = f.work(801, 0);
    let success = f.work(802, 1);
    f.commit(
        ISSUER,
        reject(&f, 803, failed.artifact, EvidenceFailure::Structure),
    );
    let failed_state = f.owner.committed().work(failed.artifact.id).unwrap().state;
    let diagnostic = f.diagnostic(804);
    f.commit(
        SUBJECT,
        f.close(900, OutcomeKind::Partial, vec![success], vec![diagnostic]),
    );
    let response = f
        .owner
        .committed()
        .response(TestamentId::from_u128(900))
        .unwrap();
    assert_eq!(response.manifest(), &[success]);
    assert_eq!(response.failed_work().len(), 1);
    assert_eq!(response.failed_work()[0].binding(), failed_state.binding());
    assert_eq!(response.failed_work()[0].slot(), 0);
    assert_eq!(
        f.owner.committed().work(failed.artifact.id).unwrap().state,
        failed_state
    );
    assert_eq!(
        f.owner
            .committed()
            .work(success.artifact.id)
            .unwrap()
            .state
            .attachment(),
        Some(TestamentId::from_u128(900))
    );
    let next = f.work(805, 0);
    f.commit(
        SUBJECT,
        f.close(901, OutcomeKind::Complete, vec![next], vec![]),
    );
    let original = f
        .owner
        .committed()
        .response(TestamentId::from_u128(900))
        .unwrap();
    assert_eq!(original.manifest(), &[success]);
    assert_eq!(original.failed_work()[0].binding(), failed_state.binding());
}
