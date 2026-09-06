use super::*;
use crate::{ContentHash, ObjectId, ObjectRevision, ReceiptId};

fn binding(id: u128) -> Binding {
    Binding {
        ledger: LedgerId {
            tenant: crate::TenantId::from_u128(1),
            session: crate::SessionId::from_u128(1),
        },
        object: ObjectId::from_u128(id),
        content: ContentHash([7; 32]),
        revision: ObjectRevision(1),
    }
}
fn parent() -> Parent {
    Parent {
        ledger: LedgerId {
            tenant: crate::TenantId::from_u128(1),
            session: crate::SessionId::from_u128(1),
        },
        claim: ClaimId::from_u128(1),
        issuer: ParticipantId::from_u128(10),
        holder: ParticipantId::from_u128(11),
        receipt: ReceiptFence {
            receipt: ReceiptId::from_u128(1),
            epoch: 1,
        },
        status: ClaimStatus::Received,
        local_complete: false,
        latest_response: None,
        next_cycle: 1,
    }
}
fn custody(hash: ContentHash) -> EvidenceAttestation {
    EvidenceAttestation {
        descriptor_hash: hash,
        custody_revision: 1,
        durable: true,
        schema_valid: true,
    }
}
fn artifact(id: u128, slot: u32) -> WorkArtifact {
    let p = parent();
    WorkArtifact::generate(
        binding(id),
        &p,
        Principal::Actor(p.holder),
        slot,
        p.receipt,
        &custody(binding(id).content),
    )
    .unwrap()
}
fn report(max_artifacts: usize) -> CloseReport<'static> {
    CloseReport {
        summary: "The respondent completed this work cycle.",
        confidence: crate::Confidence::Committed,
        outcome: crate::OutcomeKind::Complete,
        diagnostics: &[],
        limits: ResponseLimits {
            artifacts: max_artifacts,
            diagnostics: 8,
            summary_bytes: 1024,
            construction_bytes: 64 * 1024,
        },
    }
}
fn close(current: &[WorkArtifact]) -> ClosePlan {
    let p = parent();
    let manifest: Vec<_> = current
        .iter()
        .map(|a| SlotBinding {
            slot: a.slot(),
            artifact: a.reference(),
        })
        .collect();
    Response::close(
        ResponseIdentity {
            binding: binding(100),
            claim: p.claim,
            receipt: p.receipt,
            cycle: 1,
            prior: None,
        },
        &p,
        Principal::Actor(p.holder),
        current,
        &manifest,
        report(10),
    )
    .unwrap()
}
fn actors(p: &Parent) -> [Principal; 4] {
    [
        Principal::Actor(p.issuer),
        Principal::Actor(p.holder),
        Principal::Actor(ParticipantId::from_u128(12)),
        Principal::Node(p.issuer),
    ]
}

#[test]
fn artifact_arrival_does_not_assess_missing_slots_and_both_receipt_close_orders_work() {
    let p = parent();
    let a = artifact(2, 0);
    let received = a
        .receive(&a.binding(), &p, Principal::Actor(p.issuer))
        .unwrap();
    assert_eq!(received.state(), WorkArtifactState::Received);
    let b = artifact(3, 1);
    let plan = close(&[received, b]);
    assert_eq!(plan.response.state(), ResponseState::Generated);
    assert_eq!(plan.attachments.len(), 2);
    for item in &plan.attachments {
        assert_eq!(item.state(), WorkArtifactState::Attached);
        assert_eq!(item.attachment(), Some(TestamentId::from_u128(100)));
        assert_eq!(
            item.receive(&item.binding(), &p, Principal::Actor(p.issuer)),
            Err(ContractError::InvalidTransition)
        );
    }
    assert_eq!(plan.attachments[0].binding().revision, ObjectRevision(3));
    assert_eq!(plan.attachments[1].binding().revision, ObjectRevision(2));
    assert_eq!(a.state(), WorkArtifactState::Generated);
}

#[test]
fn every_public_evidence_writer_requires_its_exact_actor_role() {
    let p = parent();
    let a = artifact(2, 0);
    let expected_manifest = [SlotBinding {
        slot: 0,
        artifact: a.reference(),
    }];
    let identity = ResponseIdentity {
        binding: binding(100),
        claim: p.claim,
        receipt: p.receipt,
        cycle: 1,
        prior: None,
    };
    for actor in actors(&p) {
        let generated = WorkArtifact::generate(
            binding(2),
            &p,
            actor,
            0,
            p.receipt,
            &custody(binding(2).content),
        );
        assert_eq!(generated.is_ok(), actor == Principal::Actor(p.holder));
        assert_eq!(
            a.receive(&a.binding(), &p, actor).is_ok(),
            actor == Principal::Actor(p.issuer)
        );
        assert_eq!(
            Response::close(identity, &p, actor, &[a], &expected_manifest, report(1)).is_ok(),
            actor == Principal::Actor(p.holder)
        );
        let mut response = close(&[a]).response;
        assert_eq!(
            response.plan_post(&identity.binding, &p, actor).is_ok(),
            actor == Principal::Actor(p.holder)
        );
        response
            .apply(
                response
                    .plan_post(&identity.binding, &p, Principal::Actor(p.holder))
                    .unwrap(),
            )
            .unwrap();
        assert_eq!(
            response
                .plan_receive(&response.identity().binding, &p, actor)
                .is_ok(),
            actor == Principal::Actor(p.issuer)
        );
        response
            .apply(
                response
                    .plan_receive(&response.identity().binding, &p, Principal::Actor(p.issuer))
                    .unwrap(),
            )
            .unwrap();
        assert_eq!(
            response
                .plan_begin_fixture(&response.identity().binding, &p, actor)
                .is_ok(),
            actor == Principal::Actor(p.issuer)
        );
    }
}

#[test]
fn response_delivery_and_artifact_begin_are_separate_fenced_transitions() {
    let p = parent();
    let plan = close(&[artifact(2, 0)]);
    let a = plan.attachments[0];
    let mut response = plan.response;
    assert!(response.evaluation().is_err());
    assert_eq!(
        response.plan_receive(&response.identity().binding, &p, Principal::Actor(p.issuer)),
        Err(ContractError::InvalidTransition)
    );
    let posted = response
        .plan_post(&response.identity().binding, &p, Principal::Actor(p.holder))
        .unwrap();
    response.apply(posted).unwrap();
    assert_eq!(response.apply(posted), Err(ContractError::StaleRevision));
    assert!(response.evaluation().is_err());
    response
        .apply(
            response
                .plan_receive(&response.identity().binding, &p, Principal::Actor(p.issuer))
                .unwrap(),
        )
        .unwrap();
    assert!(
        a.begin(&a.binding(), &p, Principal::Actor(p.issuer), &response)
            .is_err()
    );
    response
        .apply(
            response
                .plan_begin_fixture(&response.identity().binding, &p, Principal::Actor(p.issuer))
                .unwrap(),
        )
        .unwrap();
    let proof = response.evaluation().unwrap();
    assert_eq!(proof.claim(), p.claim);
    assert_eq!(proof.manifest()[0].artifact, a.reference());
    let validating = a
        .begin(&a.binding(), &p, Principal::Actor(p.issuer), &response)
        .unwrap();
    assert_eq!(validating.state(), WorkArtifactState::Validating);
    let adopted = Parent {
        receipt: ReceiptFence {
            epoch: 2,
            ..p.receipt
        },
        ..p
    };
    assert_eq!(
        a.begin(
            &a.binding(),
            &adopted,
            Principal::Actor(p.issuer),
            &response
        ),
        Err(ContractError::StaleReceipt)
    );
}

#[test]
fn closing_checks_complete_manifest_lineage_bounds_and_all_rows_before_returning_a_plan() {
    let p = parent();
    let a = artifact(2, 0);
    let b = artifact(3, 1);
    let identity = ResponseIdentity {
        binding: binding(100),
        claim: p.claim,
        receipt: p.receipt,
        cycle: 1,
        prior: None,
    };
    let manifest = [
        SlotBinding {
            slot: 0,
            artifact: a.reference(),
        },
        SlotBinding {
            slot: 1,
            artifact: b.reference(),
        },
    ];
    let run = |id, rows: &[WorkArtifact], slots: &[SlotBinding], limit| {
        Response::close(
            id,
            &p,
            Principal::Actor(p.holder),
            rows,
            slots,
            report(limit),
        )
    };
    assert_eq!(
        run(identity, &[a, b], &manifest, 1),
        Err(ContractError::Capacity)
    );
    assert_eq!(
        run(identity, &[a, b], &manifest[..1], 2),
        Err(ContractError::InvalidManifest)
    );
    assert_eq!(
        run(identity, &[a, a], &[manifest[0], manifest[0]], 2),
        Err(ContractError::InvalidManifest)
    );
    assert_eq!(
        run(
            ResponseIdentity {
                prior: Some(TestamentId::from_u128(9)),
                ..identity
            },
            &[a, b],
            &manifest,
            2
        ),
        Err(ContractError::InvalidManifest)
    );
    let exhausted = WorkArtifact {
        binding: Binding {
            revision: ObjectRevision(u64::MAX),
            ..b.binding()
        },
        ..b
    };
    assert_eq!(
        run(identity, &[a, exhausted], &manifest, 2),
        Err(ContractError::Capacity)
    );
    assert_eq!(a.state(), WorkArtifactState::Generated);
    assert_eq!(a.attachment(), None);
}

#[test]
fn failure_requires_real_durable_diagnostic_and_cannot_be_attached_or_repainted() {
    let p = parent();
    let a = artifact(2, 0);
    let diagnostic = Diagnostic {
        reason: EvidenceFailure::Structure,
        artifact: ArtifactRef {
            id: crate::ArtifactId::from_u128(3),
            hash: ContentHash([8; 32]),
        },
    };
    let evidence = custody(diagnostic.artifact.hash);
    for actor in actors(&p) {
        assert_eq!(
            a.reject_receipt(&a.binding(), &p, actor, diagnostic, &evidence)
                .is_ok(),
            actor == Principal::Actor(p.issuer)
        );
    }
    let rejected = a
        .reject_receipt(
            &a.binding(),
            &p,
            Principal::Actor(p.issuer),
            diagnostic,
            &evidence,
        )
        .unwrap();
    assert_eq!(rejected.state(), WorkArtifactState::ReceiptFailed);
    assert_eq!(rejected.diagnostic(), Some(diagnostic));
    assert!(
        rejected
            .receive(&rejected.binding(), &p, Principal::Actor(p.issuer))
            .is_err()
    );
    assert!(
        a.reject_receipt(
            &a.binding(),
            &p,
            Principal::Actor(p.issuer),
            diagnostic,
            &EvidenceAttestation {
                durable: false,
                ..evidence
            }
        )
        .is_err()
    );
    let id = ResponseIdentity {
        binding: binding(100),
        claim: p.claim,
        receipt: p.receipt,
        cycle: 1,
        prior: None,
    };
    assert_eq!(
        Response::close(
            id,
            &p,
            Principal::Actor(p.holder),
            &[rejected],
            &[SlotBinding {
                slot: 0,
                artifact: a.reference()
            }],
            report(1)
        ),
        Err(ContractError::InvalidTransition)
    );
    let failed = WorkArtifact::generation_failed(
        binding(4),
        &p,
        Principal::Actor(p.holder),
        1,
        p.receipt,
        Diagnostic {
            reason: EvidenceFailure::Production,
            ..diagnostic
        },
        &evidence,
    )
    .unwrap();
    assert_eq!(failed.state(), WorkArtifactState::GenerationFailed);
    assert_eq!(failed.attachment(), None);
}

#[test]
fn terminality_is_explicit_for_every_evidence_state() {
    assert_eq!(WorkArtifactState::ALL.len(), 8);
    assert_eq!(ResponseState::ALL.len(), 8);
    assert_eq!(
        WorkArtifactState::ALL
            .iter()
            .filter(|s| s.is_terminal())
            .count(),
        4
    );
    assert_eq!(
        ResponseState::ALL
            .iter()
            .filter(|s| s.is_terminal())
            .count(),
        4
    );
    let a = artifact(2, 0);
    let p = parent();
    for state in WorkArtifactState::ALL
        .iter()
        .copied()
        .filter(|s| s.is_terminal())
    {
        let terminal = WorkArtifact { state, ..a };
        assert!(
            terminal
                .receive(&terminal.binding(), &p, Principal::Actor(p.issuer))
                .is_err()
        );
    }
    for status in [
        ClaimStatus::Cancelled,
        ClaimStatus::Expired,
        ClaimStatus::Satisfied,
    ] {
        let terminal = Parent { status, ..p };
        assert!(
            WorkArtifact::generate(
                binding(5),
                &terminal,
                Principal::Actor(p.holder),
                2,
                p.receipt,
                &custody(binding(5).content)
            )
            .is_err()
        );
    }
}

#[test]
fn generated_objects_and_diagnostics_require_nonzero_identity_and_custody() {
    let p = parent();
    assert!(
        WorkArtifact::generate(
            binding(0),
            &p,
            Principal::Actor(p.holder),
            0,
            p.receipt,
            &custody(binding(0).content)
        )
        .is_err()
    );
    assert!(
        WorkArtifact::generate(
            binding(2),
            &p,
            Principal::Actor(p.holder),
            0,
            p.receipt,
            &EvidenceAttestation {
                custody_revision: 0,
                ..custody(binding(2).content)
            }
        )
        .is_err()
    );
    assert!(
        Response::close(
            ResponseIdentity {
                binding: binding(0),
                claim: p.claim,
                receipt: p.receipt,
                cycle: 1,
                prior: None
            },
            &p,
            Principal::Actor(p.holder),
            &[],
            &[],
            report(1)
        )
        .is_err()
    );
}

fn response_identity() -> ResponseIdentity {
    let p = parent();
    ResponseIdentity {
        binding: binding(100),
        claim: p.claim,
        receipt: p.receipt,
        cycle: p.next_cycle,
        prior: p.latest_response,
    }
}

fn diagnostic_source(p: &Parent, id: u128) -> crate::Artifact {
    use crate::CanonicalContent;
    let content = crate::ArtifactContent {
        ledger: p.ledger,
        schema: 1,
        kind: "error".into(),
        schema_hash: ContentHash([9; 32]),
        metadata: vec![],
        payload: crate::ArtifactPayload::Inline(id.to_be_bytes().to_vec()),
        producer: p.holder,
        receipt: Some(p.receipt),
        inputs: std::collections::BTreeSet::new(),
        visibility: std::collections::BTreeSet::new(),
    };
    let hash = content.content_hash().unwrap();
    crate::Artifact::new(
        content,
        hash,
        crate::ArtifactLifecycle {
            created: crate::SessionSeq(1),
            custody_revision: 1,
        },
    )
}

fn report_diagnostic(id: u128) -> ResponseDiagnostic {
    let p = parent();
    let source = diagnostic_source(&p, id);
    let diagnostic = Diagnostic {
        reason: EvidenceFailure::Work,
        artifact: ArtifactRef {
            id: crate::ArtifactId::from_u128(id),
            hash: source.content_hash(),
        },
    };
    ResponseDiagnostic::record(
        &p,
        Principal::Actor(p.holder),
        p.receipt,
        diagnostic,
        (diagnostic.artifact.id, &source),
        &custody(diagnostic.artifact.hash),
    )
    .unwrap()
}

#[test]
fn all_reported_outcomes_follow_explicit_authoring_posting_and_receipt_without_a_verdict() {
    use crate::{Confidence, OutcomeKind};
    let p = parent();
    for outcome in [
        OutcomeKind::Complete,
        OutcomeKind::Partial,
        OutcomeKind::Refused,
        OutcomeKind::Impossible,
        OutcomeKind::Interrupted,
        OutcomeKind::Failed,
    ] {
        let (mut response, charge) = {
            let summary = String::from("I ended this work cycle. See the diagnostic artifact.");
            let diagnostics = [report_diagnostic(200)];
            let prepared = Response::prepare_close(
                response_identity(),
                &p,
                Principal::Actor(p.holder),
                &[],
                &[],
                CloseReport {
                    summary: &summary,
                    confidence: Confidence::Tentative,
                    outcome,
                    diagnostics: &diagnostics,
                    ..report(0)
                },
            )
            .unwrap();
            let charge = prepared.construction_charge();
            let built = prepared.build().unwrap();
            assert!(built.attachments.is_empty());
            assert!(built.retained_bytes().unwrap() >= charge);
            (built.response, charge)
        };
        // Construction inputs have gone away; response owns exactly the report.
        assert_eq!(response.reported_outcome(), outcome);
        assert_eq!(response.confidence(), Confidence::Tentative);
        assert_eq!(response.respondent(), p.holder);
        assert_eq!(
            response.diagnostics()[0].artifact().id,
            crate::ArtifactId::from_u128(200)
        );
        assert_eq!(response.state(), ResponseState::Generated);
        assert_eq!(response.terminal(), None);
        assert!(!response.summary().is_empty());
        assert!(charge > response.summary().len());
        let summary_address = response.summary().as_ptr();
        let diagnostic_address = response.diagnostics().as_ptr();
        assert!(
            response
                .plan_receive(&response.identity().binding, &p, Principal::Actor(p.issuer))
                .is_err()
        );
        let posted = response
            .plan_post(&response.identity().binding, &p, Principal::Actor(p.holder))
            .unwrap();
        response.apply(posted).unwrap();
        assert_eq!(response.state(), ResponseState::Posted);
        assert!(response.evaluation().is_err());
        let received = response
            .plan_receive(&response.identity().binding, &p, Principal::Actor(p.issuer))
            .unwrap();
        response.apply(received).unwrap();
        assert_eq!(response.state(), ResponseState::Received);
        assert_eq!(response.terminal(), None);
        assert_eq!(response.reported_outcome(), outcome);
        assert_eq!(response.summary().as_ptr(), summary_address);
        assert_eq!(response.diagnostics().as_ptr(), diagnostic_address);
    }
}

#[test]
fn diagnostic_only_failure_remains_inspectable_and_never_supplies_a_missing_work_slot() {
    use crate::lifecycle::validation::{ResponseReadiness, Target};
    let p = parent();
    let diagnostic = report_diagnostic(200);
    let failed_product = WorkArtifact::generation_failed(
        binding(2),
        &p,
        Principal::Actor(p.holder),
        0,
        p.receipt,
        Diagnostic {
            reason: EvidenceFailure::Production,
            artifact: diagnostic.artifact(),
        },
        &custody(diagnostic.artifact().hash),
    )
    .unwrap();
    let plan = Response::close(
        response_identity(),
        &p,
        Principal::Actor(p.holder),
        &[],
        &[],
        CloseReport {
            summary: "The compiler failed; no requested binary was produced.",
            outcome: crate::OutcomeKind::Failed,
            diagnostics: &[diagnostic],
            ..report(0)
        },
    )
    .unwrap();
    assert!(plan.attachments.is_empty());
    let mut response = plan.response;
    assert!(response.manifest().is_empty());
    assert_eq!(
        response.diagnostics()[0].artifact(),
        failed_product.diagnostic().unwrap().artifact
    );
    assert_eq!(failed_product.state(), WorkArtifactState::GenerationFailed);
    for (actor, next) in [
        (Principal::Actor(p.holder), ResponseState::Posted),
        (Principal::Actor(p.issuer), ResponseState::Received),
        (Principal::Actor(p.issuer), ResponseState::Validating),
    ] {
        let transition = match next {
            ResponseState::Posted => response.plan_post(&response.identity().binding, &p, actor),
            ResponseState::Received => {
                response.plan_receive(&response.identity().binding, &p, actor)
            }
            _ => response.plan_begin_fixture(&response.identity().binding, &p, actor),
        }
        .unwrap();
        response.apply(transition).unwrap();
    }
    let view = response.evaluation().unwrap();
    assert!(
        ResponseReadiness::from_evaluation(
            &view,
            Target::MissingSlot {
                response: response.identity().binding,
                slot: 0
            },
        )
        .is_ok()
    );
    assert_eq!(
        ResponseReadiness::from_evaluation(
            &view,
            Target::Artifact {
                response: response.identity().binding,
                slot: 0,
                artifact: binding(200)
            },
        ),
        Err(ContractError::InvalidManifest)
    );
    assert_eq!(response.state(), ResponseState::Validating);
    assert_eq!(response.terminal(), None);
}

#[test]
fn diagnostics_require_real_custody_and_exact_respondent_receipt_cycle() {
    let p = parent();
    let source = report_diagnostic(200).diagnostic();
    let stored = diagnostic_source(&p, 200);
    let valid = custody(source.artifact.hash);
    for actor in actors(&p) {
        assert_eq!(
            ResponseDiagnostic::record(
                &p,
                actor,
                p.receipt,
                source,
                (source.artifact.id, &stored),
                &valid
            )
            .is_ok(),
            actor == Principal::Actor(p.holder)
        );
    }
    for evidence in [
        EvidenceAttestation {
            durable: false,
            ..valid.clone()
        },
        EvidenceAttestation {
            schema_valid: false,
            ..valid.clone()
        },
        EvidenceAttestation {
            custody_revision: 0,
            ..valid.clone()
        },
        EvidenceAttestation {
            descriptor_hash: ContentHash([99; 32]),
            ..valid.clone()
        },
    ] {
        assert_eq!(
            ResponseDiagnostic::record(
                &p,
                Principal::Actor(p.holder),
                p.receipt,
                source,
                (source.artifact.id, &stored),
                &evidence
            ),
            Err(ContractError::MissingEvidence)
        );
    }
    assert_eq!(
        ResponseDiagnostic::record(
            &p,
            Principal::Actor(p.holder),
            ReceiptFence {
                epoch: 2,
                ..p.receipt
            },
            source,
            (source.artifact.id, &stored),
            &valid
        ),
        Err(ContractError::StaleReceipt)
    );
    for foreign in [
        Parent {
            claim: ClaimId::from_u128(9),
            ..p
        },
        Parent { next_cycle: 2, ..p },
        Parent {
            holder: ParticipantId::from_u128(12),
            ..p
        },
        Parent {
            receipt: ReceiptFence {
                epoch: 2,
                ..p.receipt
            },
            ..p
        },
    ] {
        let row = diagnostic_source(&foreign, 200);
        let foreign_diagnostic = Diagnostic {
            artifact: ArtifactRef {
                hash: row.content_hash(),
                ..source.artifact
            },
            ..source
        };
        let diagnostic = ResponseDiagnostic::record(
            &foreign,
            Principal::Actor(foreign.holder),
            foreign.receipt,
            foreign_diagnostic,
            (source.artifact.id, &row),
            &custody(row.content_hash()),
        )
        .unwrap();
        assert!(
            Response::close(
                response_identity(),
                &p,
                Principal::Actor(p.holder),
                &[],
                &[],
                CloseReport {
                    diagnostics: &[diagnostic],
                    outcome: crate::OutcomeKind::Failed,
                    ..report(0)
                }
            )
            .is_err()
        );
    }
}

#[test]
fn all_noncomplete_reports_need_diagnostics_and_preflight_checks_bounds_before_ownership() {
    use crate::OutcomeKind;
    let p = parent();
    let diagnostics = [report_diagnostic(200)];
    for outcome in [
        OutcomeKind::Partial,
        OutcomeKind::Refused,
        OutcomeKind::Impossible,
        OutcomeKind::Interrupted,
        OutcomeKind::Failed,
    ] {
        assert_eq!(
            Response::close(
                response_identity(),
                &p,
                Principal::Actor(p.holder),
                &[],
                &[],
                CloseReport {
                    outcome,
                    ..report(0)
                }
            ),
            Err(ContractError::MissingEvidence)
        );
    }
    let input = CloseReport {
        outcome: OutcomeKind::Failed,
        diagnostics: &diagnostics,
        ..report(0)
    };
    let charge = Response::prepare_close(
        response_identity(),
        &p,
        Principal::Actor(p.holder),
        &[],
        &[],
        input,
    )
    .unwrap()
    .construction_charge();
    for limits in [
        ResponseLimits {
            construction_bytes: charge - 1,
            ..input.limits
        },
        ResponseLimits {
            summary_bytes: input.summary.len() - 1,
            ..input.limits
        },
        ResponseLimits {
            diagnostics: 0,
            ..input.limits
        },
    ] {
        assert_eq!(
            Response::close(
                response_identity(),
                &p,
                Principal::Actor(p.holder),
                &[],
                &[],
                CloseReport { limits, ..input }
            ),
            Err(ContractError::Capacity)
        );
    }
    let exact = Response::prepare_close(
        response_identity(),
        &p,
        Principal::Actor(p.holder),
        &[],
        &[],
        CloseReport {
            limits: ResponseLimits {
                construction_bytes: charge,
                summary_bytes: input.summary.len(),
                diagnostics: 1,
                ..input.limits
            },
            ..input
        },
    )
    .unwrap();
    assert_eq!(exact.construction_charge(), charge);
    assert_eq!(
        exact.build().unwrap().response.reported_outcome(),
        OutcomeKind::Failed
    );
    assert_eq!(
        Response::close(
            response_identity(),
            &p,
            Principal::Actor(p.holder),
            &[],
            &[],
            CloseReport {
                summary: " \n\t",
                ..input
            }
        ),
        Err(ContractError::InvalidManifest)
    );
    for malformed in [
        vec![diagnostics[0], diagnostics[0]],
        vec![report_diagnostic(201), diagnostics[0]],
    ] {
        assert_eq!(
            Response::close(
                response_identity(),
                &p,
                Principal::Actor(p.holder),
                &[],
                &[],
                CloseReport {
                    diagnostics: &malformed,
                    ..input
                }
            ),
            Err(ContractError::InvalidManifest)
        );
    }
    let work = artifact(200, 0);
    assert_eq!(
        Response::close(
            response_identity(),
            &p,
            Principal::Actor(p.holder),
            &[work],
            &[SlotBinding {
                slot: 0,
                artifact: work.reference()
            }],
            CloseReport {
                limits: ResponseLimits {
                    artifacts: 1,
                    ..input.limits
                },
                ..input
            }
        ),
        Err(ContractError::InvalidManifest)
    );
    assert_eq!(work.state(), WorkArtifactState::Generated);
    assert_eq!(work.attachment(), None);
}

#[test]
fn a_transition_cannot_substitute_authored_content_under_the_same_supplied_binding() {
    let p = parent();
    let diagnostics = [report_diagnostic(200)];
    let input = CloseReport {
        diagnostics: &diagnostics,
        ..report(0)
    };
    let original = Response::close(
        response_identity(),
        &p,
        Principal::Actor(p.holder),
        &[],
        &[],
        input,
    )
    .unwrap()
    .response;
    let posted = original
        .plan_post(&original.identity().binding, &p, Principal::Actor(p.holder))
        .unwrap();
    for different in [
        CloseReport {
            summary: "A different assertion",
            ..input
        },
        CloseReport {
            confidence: crate::Confidence::Hint,
            ..input
        },
        CloseReport {
            outcome: crate::OutcomeKind::Failed,
            ..input
        },
        CloseReport {
            diagnostics: &[],
            ..input
        },
    ] {
        let mut substitute = Response::close(
            response_identity(),
            &p,
            Principal::Actor(p.holder),
            &[],
            &[],
            different,
        )
        .unwrap()
        .response;
        assert_eq!(substitute.identity(), original.identity());
        assert_eq!(
            substitute.apply(posted),
            Err(ContractError::ContentConflict)
        );
        assert_eq!(substitute.state(), ResponseState::Generated);
    }
    let mut reconstructed = Response::close(
        response_identity(),
        &p,
        Principal::Actor(p.holder),
        &[],
        &[],
        input,
    )
    .unwrap()
    .response;
    reconstructed.apply(posted).unwrap();
    assert_eq!(reconstructed.state(), ResponseState::Posted);
}

#[test]
fn diagnostic_tokens_validate_the_actual_immutable_artifact_descriptor() {
    use crate::CanonicalContent;
    let p = parent();
    let source = diagnostic_source(&p, 200);
    let reference = ArtifactRef {
        id: crate::ArtifactId::from_u128(200),
        hash: source.content_hash(),
    };
    let diagnostic = Diagnostic {
        reason: EvidenceFailure::Work,
        artifact: reference,
    };
    let content = source.content();
    for (content, error) in [
        (
            crate::ArtifactContent {
                producer: p.issuer,
                ..content.clone()
            },
            ContractError::WrongActor,
        ),
        (
            crate::ArtifactContent {
                ledger: LedgerId {
                    session: crate::SessionId::from_u128(99),
                    ..p.ledger
                },
                ..content.clone()
            },
            ContractError::WrongLedger,
        ),
        (
            crate::ArtifactContent {
                receipt: None,
                ..content.clone()
            },
            ContractError::StaleReceipt,
        ),
        (
            crate::ArtifactContent {
                receipt: Some(ReceiptFence {
                    epoch: 2,
                    ..p.receipt
                }),
                ..content.clone()
            },
            ContractError::StaleReceipt,
        ),
        (
            crate::ArtifactContent {
                kind: "document".into(),
                ..content.clone()
            },
            ContractError::MissingEvidence,
        ),
        (
            crate::ArtifactContent {
                schema: 0,
                ..content.clone()
            },
            ContractError::MissingEvidence,
        ),
        (
            crate::ArtifactContent {
                schema_hash: ContentHash([0; 32]),
                ..content.clone()
            },
            ContractError::MissingEvidence,
        ),
    ] {
        let hash = content.content_hash().unwrap();
        let stored = crate::Artifact::new(content, hash, source.lifecycle().clone());
        let claimed = Diagnostic {
            artifact: ArtifactRef { hash, ..reference },
            ..diagnostic
        };
        assert_eq!(
            ResponseDiagnostic::record(
                &p,
                Principal::Actor(p.holder),
                p.receipt,
                claimed,
                (reference.id, &stored),
                &custody(hash)
            ),
            Err(error)
        );
    }
    assert_eq!(
        ResponseDiagnostic::record(
            &p,
            Principal::Actor(p.holder),
            p.receipt,
            diagnostic,
            (crate::ArtifactId::from_u128(201), &source),
            &custody(reference.hash)
        ),
        Err(ContractError::MissingEvidence)
    );
    assert_eq!(
        ResponseDiagnostic::record(
            &p,
            Principal::Actor(p.holder),
            p.receipt,
            Diagnostic {
                artifact: ArtifactRef {
                    hash: ContentHash([33; 32]),
                    ..reference
                },
                ..diagnostic
            },
            (reference.id, &source),
            &custody(reference.hash)
        ),
        Err(ContractError::MissingEvidence)
    );
    for lifecycle in [
        crate::ArtifactLifecycle {
            created: crate::SessionSeq(0),
            custody_revision: 1,
        },
        crate::ArtifactLifecycle {
            created: crate::SessionSeq(1),
            custody_revision: 2,
        },
    ] {
        let stored = crate::Artifact::new(content.clone(), reference.hash, lifecycle);
        assert_eq!(
            ResponseDiagnostic::record(
                &p,
                Principal::Actor(p.holder),
                p.receipt,
                diagnostic,
                (reference.id, &stored),
                &custody(reference.hash)
            ),
            Err(ContractError::MissingEvidence)
        );
    }
}
