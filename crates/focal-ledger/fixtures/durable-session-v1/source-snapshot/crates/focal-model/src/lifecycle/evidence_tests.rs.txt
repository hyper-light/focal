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
        10,
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
            Response::close(identity, &p, actor, &[a], &expected_manifest, 1).is_ok(),
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
        Response::close(id, &p, Principal::Actor(p.holder), rows, slots, limit)
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
            1
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
            1
        )
        .is_err()
    );
}
