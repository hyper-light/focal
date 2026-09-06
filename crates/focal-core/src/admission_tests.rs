//! Current authoring policy is tested separately from historical execution.
use crate::tests::{EVALUATOR, ISSUER, WORKER, input, ledger, new_claim, setup};
use crate::*;

const CLAIM: ClaimId = ClaimId::from_u128(700);
const SET: EvidenceSetId = EvidenceSetId::from_u128(701);
const TESTAMENT: TestamentId = TestamentId::from_u128(702);
const RECEIPT: ReceiptFence = ReceiptFence {
    receipt: ReceiptId::from_u128(703),
    epoch: 1,
};
const BYTES: usize = 16 * 1024 * 1024;
const NONCOMPLETE: [OutcomeKind; 5] = [
    OutcomeKind::Partial,
    OutcomeKind::Refused,
    OutcomeKind::Impossible,
    OutcomeKind::Interrupted,
    OutcomeKind::Failed,
];

fn apply(core: &mut Core, request: AuthenticatedInput) -> ApplyResult {
    let prepared = core.prepare(&request).unwrap();
    core.apply(SessionSeq(core.sequence().0 + 1), prepared)
        .unwrap()
}

fn received() -> Core {
    let mut core = setup();
    apply(
        &mut core,
        input(
            1,
            ISSUER,
            Command::GenerateClaim {
                claim: new_claim(700),
            },
        ),
    );
    apply(
        &mut core,
        input(2, ISSUER, Command::PostClaim { claim: CLAIM }),
    );
    apply(
        &mut core,
        input(
            3,
            WORKER,
            Command::AcquireReceipt {
                claim: CLAIM,
                receipt: RECEIPT.receipt,
                epoch: RECEIPT.epoch,
            },
        ),
    );
    core
}

fn opened() -> Core {
    let mut core = received();
    apply(
        &mut core,
        input(
            4,
            WORKER,
            Command::BeginEvidenceSet {
                claim: CLAIM,
                receipt: RECEIPT,
                evidence_set: SET,
            },
        ),
    );
    core
}

fn artifact(kind: &str) -> NewArtifact {
    NewArtifact {
        id: ArtifactId::from_u128(704),
        content: ArtifactContent {
            ledger: ledger(),
            schema: 1,
            kind: kind.into(),
            schema_hash: ContentHash([17; 32]),
            metadata: Vec::new(),
            payload: ArtifactPayload::Inline(br#"{"reason":"work could not finish"}"#.to_vec()),
            producer: WORKER,
            receipt: Some(RECEIPT),
            inputs: BTreeSet::from([ObjectRef::claim(ledger(), CLAIM)]),
            visibility: BTreeSet::new(),
        },
    }
}

fn attach(value: NewArtifact, custody_revision: u64) -> (AuthenticatedInput, ArtifactRef) {
    let reference = ArtifactRef {
        id: value.id,
        hash: value.content.content_hash().unwrap(),
    };
    let mut request = input(
        5,
        WORKER,
        Command::AttachArtifact {
            claim: CLAIM,
            receipt: RECEIPT,
            evidence_set: SET,
            artifact: value,
        },
    );
    request.authority.evidence.push(EvidenceAttestation {
        descriptor_hash: reference.hash,
        custody_revision,
        durable: true,
        schema_valid: true,
    });
    (request, reference)
}

fn close(outcome: OutcomeKind, manifest: Vec<ArtifactRef>) -> AuthenticatedInput {
    let mut request = input(
        6,
        WORKER,
        Command::CloseTestament {
            claim: CLAIM,
            receipt: RECEIPT,
            testament: TESTAMENT,
            evidence_set: SET,
            manifest,
            summary: "Respondent's explicit account".into(),
            confidence: Confidence::Tentative,
            outcome,
        },
    );
    request.authority.runtime = false;
    request
}

fn managed(value: AuthenticatedInput) -> ManagedAuthenticatedInput {
    ManagedAuthenticatedInput {
        key: ManagedRequestKey {
            stream: RequestStreamIdentity {
                cluster: [9; 16],
                ledger: value.ledger,
                principal: value.principal,
                slot: 0,
                generation: 1,
            },
            ordinal: 1,
            id: value.request_id,
        },
        expected_revision: value.expected_revision,
        authority: value.authority,
        command: value.command,
    }
}

fn refuse_both(core: &Core, request: AuthenticatedInput, code: ErrorCode) {
    let before = core.encode_checkpoint().unwrap();
    assert!(
        matches!(core.prepare(&request), Err(DomainOutcome::Refuse { code: actual, .. }) if actual == code)
    );
    assert!(matches!(
        core.stage_managed_pending_bounded(&PendingState::new(), &managed(request), BYTES),
        Err(StagingError::Domain(DomainOutcome::Refuse { code: actual, .. })) if actual == code
    ));
    assert_eq!(core.encode_checkpoint().unwrap(), before);
}

#[test]
fn receiving_work_creates_only_receipt_until_the_respondent_explicitly_testifies() {
    let core = received();
    assert!(core.snapshot().testaments.is_empty());
    assert!(core.snapshot().artifacts.is_empty());
    assert!(core.snapshot().evidence_sets.is_empty());
    let claim = &core.snapshot().claims[&CLAIM];
    assert_eq!(claim.lifecycle().status, ClaimStatus::Received);
    assert_eq!(claim.lifecycle().receipt.as_ref().unwrap().fence, RECEIPT);
    assert!(!claim.lifecycle().local_complete);
}

#[test]
fn every_noncomplete_account_requires_diagnostic_evidence_in_both_admission_paths() {
    for outcome in NONCOMPLETE {
        let mut core = opened();
        refuse_both(&core, close(outcome, vec![]), ErrorCode::EvidenceNotDurable);
        let (request, reference) = attach(artifact("test-report"), 1);
        apply(&mut core, request);
        refuse_both(
            &core,
            close(outcome, vec![reference]),
            ErrorCode::EvidenceNotDurable,
        );
    }
}

#[test]
fn each_explicit_account_is_preserved_without_acknowledging_or_satisfying_work() {
    for outcome in NONCOMPLETE.into_iter().chain([OutcomeKind::Complete]) {
        let mut legacy = opened();
        let (request, reference) = attach(artifact("error"), 1);
        apply(&mut legacy, request);
        let mut scoped = legacy.clone();
        let authored = close(outcome, vec![reference]);
        let accepted = apply(&mut legacy, authored.clone());
        assert_eq!(
            accepted.receipt.outcome,
            CommandResult::Testament(TESTAMENT)
        );
        let candidate = scoped
            .stage_managed_pending_bounded(&PendingState::new(), &managed(authored), BYTES)
            .unwrap();
        scoped
            .audit_managed_pending_stage(&PendingState::new(), &candidate, BYTES)
            .unwrap();
        scoped.publish_managed(candidate).unwrap();
        for core in [&legacy, &scoped] {
            let testament = &core.snapshot().testaments[&TESTAMENT];
            assert_eq!(testament.content().outcome, outcome);
            assert_eq!(testament.content().confidence, Confidence::Tentative);
            assert_eq!(testament.content().summary, "Respondent's explicit account");
            assert_eq!(testament.content().artifacts, [reference]);
            assert_eq!(testament.lifecycle().acknowledged, None);
            let claim = &core.snapshot().claims[&CLAIM];
            assert_eq!(claim.lifecycle().status, ClaimStatus::TestamentGenerated);
            assert!(!claim.lifecycle().local_complete);
            assert!(core.snapshot().runs.is_empty());
        }
        assert_eq!(legacy.snapshot().testaments, scoped.snapshot().testaments);
    }
    let mut core = opened();
    apply(&mut core, close(OutcomeKind::Complete, vec![]));
    assert_eq!(
        core.snapshot().claims[&CLAIM].lifecycle().status,
        ClaimStatus::TestamentGenerated
    );
}

#[test]
fn noncomplete_report_requires_current_holder_receipt_and_exact_manifest() {
    let mut core = opened();
    let (request, reference) = attach(artifact("error"), 1);
    apply(&mut core, request);
    let authored = close(OutcomeKind::Failed, vec![reference]);
    let mut other = authored.clone();
    other.principal = ISSUER;
    refuse_both(&core, other, ErrorCode::WrongActor);
    let mut stale = authored.clone();
    if let Command::CloseTestament { receipt, .. } = &mut stale.command {
        receipt.epoch += 1;
    }
    refuse_both(&core, stale, ErrorCode::StaleReceipt);
    for manifest in [
        vec![],
        vec![ArtifactRef {
            hash: ContentHash([99; 32]),
            ..reference
        }],
    ] {
        refuse_both(
            &core,
            close(OutcomeKind::Failed, manifest),
            ErrorCode::InvalidManifest,
        );
    }
}

#[test]
fn claimed_diagnostic_must_have_schema_and_durable_custody() {
    for (schema, custody) in [(ContentHash::default(), 1), (ContentHash([17; 32]), 0)] {
        let mut core = opened();
        let mut value = artifact("error");
        value.content.schema_hash = schema;
        let (request, reference) = attach(value, custody);
        // These historical artifact shapes remain representable; they cannot
        // become the diagnostic proof for a newly admitted noncomplete report.
        apply(&mut core, request);
        refuse_both(
            &core,
            close(OutcomeKind::Failed, vec![reference]),
            ErrorCode::EvidenceNotDurable,
        );
    }
    for (durable, schema_valid) in [(false, true), (true, false)] {
        let core = opened();
        let (mut request, _) = attach(artifact("error"), 1);
        request.authority.evidence[0].durable = durable;
        request.authority.evidence[0].schema_valid = schema_valid;
        refuse_both(&core, request, ErrorCode::EvidenceNotDurable);
        assert!(core.snapshot().artifacts.is_empty());
    }
}

#[test]
fn adopted_holder_cannot_reuse_the_previous_respondents_diagnostic() {
    let mut core = opened();
    let (request, reference) = attach(artifact("error"), 1);
    apply(&mut core, request);
    let replacement = ReceiptFence {
        receipt: ReceiptId::from_u128(705),
        epoch: 2,
    };
    apply(
        &mut core,
        input(
            8,
            ISSUER,
            Command::AdoptReceipt {
                claim: CLAIM,
                previous: RECEIPT,
                receipt: replacement.receipt,
                holder: EVALUATOR,
                epoch: replacement.epoch,
            },
        ),
    );
    let mut request = close(OutcomeKind::Failed, vec![reference]);
    request.principal = EVALUATOR;
    if let Command::CloseTestament { receipt, .. } = &mut request.command {
        *receipt = replacement;
    }
    refuse_both(&core, request, ErrorCode::EvidenceNotDurable);
    assert_eq!(core.snapshot().evidence_sets[&SET].receipt, replacement);
    assert_eq!(
        core.snapshot().artifacts[&reference.id].content().receipt,
        Some(RECEIPT)
    );
    assert!(core.snapshot().testaments.is_empty());
}

#[test]
fn diagnostic_lookup_observes_unpublished_artifacts_and_pending_receipt_fences() {
    let core = opened();
    let before = core.encode_checkpoint().unwrap();
    let (request, reference) = attach(artifact("error"), 1);
    let mut pending = PendingState::new();
    pending.reserve(3).unwrap();
    let stage = core.stage_pending(&pending, &request).unwrap();
    let (prepared, _) = pending.accept(&core, stage).unwrap();
    assert!(core.snapshot().artifacts.is_empty());
    let authored = close(OutcomeKind::Failed, vec![reference]);
    let legacy = core.stage_pending(&pending, &authored).unwrap();
    core.audit_pending_stage(&pending, &legacy, EpochLimits::default())
        .unwrap();
    let scoped = core
        .stage_managed_pending_bounded(&pending, &managed(authored.clone()), BYTES)
        .unwrap();
    core.audit_managed_pending_stage(&pending, &scoped, BYTES)
        .unwrap();
    assert_eq!(scoped.result().outcome, legacy.result().receipt.outcome);
    let mut published = core.clone();
    published
        .apply(SessionSeq(core.sequence().0 + 1), prepared)
        .unwrap();
    published
        .apply(
            SessionSeq(published.sequence().0 + 1),
            legacy.prepared().clone(),
        )
        .unwrap();
    assert_eq!(
        published.snapshot().testaments[&TESTAMENT]
            .content()
            .outcome,
        OutcomeKind::Failed
    );
    let adopt = input(
        8,
        ISSUER,
        Command::AdoptReceipt {
            claim: CLAIM,
            previous: RECEIPT,
            receipt: ReceiptId::from_u128(705),
            holder: EVALUATOR,
            epoch: 2,
        },
    );
    let stage = core.stage_pending(&pending, &adopt).unwrap();
    pending.accept(&core, stage).unwrap();
    assert!(matches!(
        core.stage_pending(&pending, &authored),
        Err(DomainOutcome::Refuse {
            code: ErrorCode::StaleReceipt,
            ..
        })
    ));
    assert!(matches!(
        core.stage_managed_pending_bounded(&pending, &managed(authored), BYTES),
        Err(StagingError::Domain(DomainOutcome::Refuse {
            code: ErrorCode::StaleReceipt,
            ..
        }))
    ));
    assert_eq!(core.encode_checkpoint().unwrap(), before);
}

fn exceptional() -> AuthenticatedInput {
    let error = artifact("error");
    let hash = error.content.content_hash().unwrap();
    let mut request = input(
        7,
        ISSUER,
        Command::FailTestamentGeneration {
            claim: CLAIM,
            testament: TESTAMENT,
            evidence_set: SET,
            error,
            summary: "Historical runtime closing failure".into(),
        },
    );
    request.authority.evidence.push(EvidenceAttestation {
        descriptor_hash: hash,
        custody_revision: 1,
        durable: true,
        schema_valid: true,
    });
    request
}

#[test]
fn runtime_authority_cannot_submit_new_generated_failure_testaments() {
    let core = opened();
    refuse_both(&core, exceptional(), ErrorCode::InvalidSchema);
    assert!(core.snapshot().testaments.is_empty());
    assert!(core.snapshot().artifacts.is_empty());
    assert!(!core.snapshot().evidence_sets[&SET].closed);
}

fn recorded(core: &Core, input: AuthenticatedInput) -> PreparedMutation {
    PreparedMutation {
        schema: 1,
        base: core.sequence(),
        command_hash: command_hash(&input).unwrap(),
        footprint: Footprint {
            ledger: input.ledger,
            session_exclusive: true,
        },
        input,
    }
}

#[test]
fn original_failure_rules_replay_and_retained_legacy_retries_bypass_current_policy() {
    // These are explicit valid historical intents, not current authoring or a
    // synthetic outcome tombstone. Both execute the retained V1 reducer.
    for authored in [exceptional(), close(OutcomeKind::Failed, vec![])] {
        let mut direct = opened();
        let before = direct.encode_checkpoint().unwrap();
        let prepared = recorded(&direct, authored.clone());
        let bytes = prepared.encode_v1().unwrap();
        let decoded = PreparedMutation::decode_v1(&bytes).unwrap();
        let mut epoch = Core::decode_checkpoint(&before).unwrap();
        let expected = direct
            .apply(SessionSeq(direct.sequence().0 + 1), decoded.clone())
            .unwrap();
        let output = epoch
            .plan_epoch(vec![decoded], EpochLimits::default())
            .unwrap()
            .execute(&epoch)
            .unwrap();
        assert_eq!(
            epoch.publish_epoch(output).unwrap().as_slice(),
            std::slice::from_ref(&expected)
        );
        assert_eq!(
            epoch.encode_checkpoint().unwrap(),
            direct.encode_checkpoint().unwrap()
        );
        let restored = Core::decode_checkpoint(&direct.encode_checkpoint().unwrap()).unwrap();
        assert_eq!(
            restored.prepare(&authored),
            Err(DomainOutcome::Duplicate(Box::new(expected.receipt)))
        );
        let mut changed = authored.clone();
        changed.request_id = RequestId::from_u128(88);
        assert!(restored.prepare(&changed).is_err());
        let testament = &restored.snapshot().testaments[&TESTAMENT];
        assert_eq!(testament.content().outcome, OutcomeKind::Failed);
        assert_eq!(
            testament.lifecycle().acknowledged.is_some(),
            matches!(authored.command, Command::FailTestamentGeneration { .. })
        );
    }
}

#[test]
fn committed_managed_failure_intents_replay_without_readmitting_current_policy() {
    for authored in [exceptional(), close(OutcomeKind::Interrupted, vec![])] {
        let mut core = opened();
        let request = managed(authored);
        assert!(
            core.stage_managed_pending_bounded(&PendingState::new(), &request, BYTES)
                .is_err()
        );
        let prepared = PreparedManagedMutation {
            schema: 1,
            base: core.sequence(),
            command_hash: managed_command_hash(&request).unwrap(),
            footprint: Footprint {
                ledger: request.key.stream.ledger,
                session_exclusive: true,
            },
            input: request,
        };
        let original = core.encode_checkpoint().unwrap();
        let body = prepared.encode_v1().unwrap();
        let decoded = PreparedManagedMutation::decode_v1(&body).unwrap();
        let replay = core.replay_managed_bounded(&decoded, BYTES).unwrap();
        core.audit_managed_pending_stage(&PendingState::new(), &replay, BYTES)
            .unwrap();
        let expected = core.publish_managed(replay).unwrap();
        let mut restored = Core::decode_checkpoint(&original).unwrap();
        let replay = restored.replay_managed_bounded(&decoded, BYTES).unwrap();
        assert_eq!(restored.publish_managed(replay).unwrap(), expected);
        assert_eq!(
            restored.encode_checkpoint().unwrap(),
            core.encode_checkpoint().unwrap()
        );
    }
}

fn attachment_at(
    kind: &str,
    artifact_id: u128,
    request_id: u128,
    custody_revision: u64,
) -> (AuthenticatedInput, ArtifactRef) {
    let mut value = artifact(kind);
    value.id = ArtifactId::from_u128(artifact_id);
    value.content.metadata = artifact_id.to_be_bytes().to_vec();
    let (mut request, reference) = attach(value, custody_revision);
    request.request_id = RequestId::from_u128(request_id);
    (request, reference)
}

#[test]
fn the_final_free_manifest_slot_remains_available_for_an_actual_failure_report() {
    let mut core = opened();
    core.limits.max_artifacts_per_set = 2;
    let (work, work_ref) = attachment_at("document", 710, 10, 1);
    apply(&mut core, work.clone());
    let (too_much_work, _) = attachment_at("document", 711, 11, 1);
    refuse_both(&core, too_much_work, ErrorCode::Capacity);

    // A fresh request deduplicating to the already staged artifact consumes no
    // slot, even when its proposed object ID differs from the canonical ID.
    let mut duplicate = work.clone();
    duplicate.request_id = RequestId::from_u128(12);
    if let Command::AttachArtifact { artifact, .. } = &mut duplicate.command {
        artifact.id = ArtifactId::from_u128(799);
    }
    apply(&mut core, duplicate);
    assert_eq!(core.snapshot().evidence_sets[&SET].artifacts, [work_ref]);
    assert!(matches!(
        core.prepare(&work),
        Err(DomainOutcome::Duplicate(_))
    ));

    let (error, error_ref) = attachment_at("error", 712, 13, 1);
    apply(&mut core, error);
    let full_manifest = vec![work_ref, error_ref];
    assert_eq!(core.snapshot().evidence_sets[&SET].artifacts, full_manifest);
    apply(&mut core, close(OutcomeKind::Failed, full_manifest.clone()));
    let report = &core.snapshot().testaments[&TESTAMENT];
    assert_eq!(report.content().artifacts, full_manifest);
    assert_eq!(report.content().outcome, OutcomeKind::Failed);
    assert_eq!(report.lifecycle().acknowledged, None);
    assert_eq!(
        core.snapshot().claims[&CLAIM].lifecycle().status,
        ClaimStatus::TestamentGenerated
    );

    // An already retained eligible diagnostic removes the reservation; ordinary
    // work may use the last free slot because failure testimony is still possible.
    let mut core = opened();
    core.limits.max_artifacts_per_set = 2;
    let (error, error_ref) = attachment_at("error", 713, 14, 1);
    apply(&mut core, error);
    let (work, work_ref) = attachment_at("document", 714, 15, 1);
    apply(&mut core, work);
    apply(
        &mut core,
        close(OutcomeKind::Partial, vec![error_ref, work_ref]),
    );
}

#[test]
fn the_reserved_slot_requires_the_custody_record_the_frozen_writer_will_actually_store() {
    let mut core = opened();
    core.limits.max_artifacts_per_set = 1;
    let (work, _) = attachment_at("document", 710, 10, 1);
    refuse_both(&core, work, ErrorCode::Capacity);
    let (zero_custody, _) = attachment_at("error", 711, 11, 0);
    refuse_both(&core, zero_custody.clone(), ErrorCode::EvidenceNotDurable);
    // A later positive duplicate attestation must not hide the first matching
    // zero revision that the historical writer chooses.
    let mut conflicting = zero_custody;
    conflicting.authority.evidence.push(EvidenceAttestation {
        custody_revision: 1,
        ..conflicting.authority.evidence[0].clone()
    });
    refuse_both(&core, conflicting, ErrorCode::EvidenceNotDurable);
    let mut empty_schema = artifact("error");
    empty_schema.content.schema_hash = ContentHash::default();
    let (empty_schema, _) = attach(empty_schema, 1);
    refuse_both(&core, empty_schema, ErrorCode::Capacity);
    let (valid, reference) = attachment_at("error", 712, 12, 1);
    apply(&mut core, valid);
    apply(&mut core, close(OutcomeKind::Interrupted, vec![reference]));
}

#[test]
fn pending_work_and_diagnostic_rows_drive_the_same_headroom_policy_in_both_admission_paths() {
    let mut core = opened();
    core.limits.max_artifacts_per_set = 2;
    let before = core.encode_checkpoint().unwrap();
    let mut pending = PendingState::new();
    pending.reserve(3).unwrap();
    let (work, work_ref) = attachment_at("document", 710, 10, 1);
    let staged = core.stage_pending(&pending, &work).unwrap();
    let (work_prepared, _) = pending.accept(&core, staged).unwrap();
    let (blocked, _) = attachment_at("document", 711, 11, 1);
    assert!(matches!(
        core.stage_pending(&pending, &blocked),
        Err(DomainOutcome::Refuse {
            code: ErrorCode::Capacity,
            ..
        })
    ));
    assert!(matches!(
        core.stage_managed_pending_bounded(&pending, &managed(blocked), BYTES),
        Err(StagingError::Domain(DomainOutcome::Refuse {
            code: ErrorCode::Capacity,
            ..
        }))
    ));

    let (diagnostic, error_ref) = attachment_at("error", 712, 12, 1);
    let legacy = core.stage_pending(&pending, &diagnostic).unwrap();
    let scoped = core
        .stage_managed_pending_bounded(&pending, &managed(diagnostic), BYTES)
        .unwrap();
    assert_eq!(legacy.result().receipt.outcome, scoped.result().outcome);
    core.audit_pending_stage(&pending, &legacy, EpochLimits::default())
        .unwrap();
    core.audit_managed_pending_stage(&pending, &scoped, BYTES)
        .unwrap();
    let (error_prepared, _) = pending.accept(&core, legacy).unwrap();
    let authored = close(OutcomeKind::Failed, vec![work_ref, error_ref]);
    let closing = core.stage_pending(&pending, &authored).unwrap();
    assert!(
        core.stage_managed_pending_bounded(&pending, &managed(authored), BYTES)
            .is_ok()
    );
    assert_eq!(core.encode_checkpoint().unwrap(), before);
    for prepared in [work_prepared, error_prepared, closing.prepared().clone()] {
        core.apply(SessionSeq(core.sequence().0 + 1), prepared)
            .unwrap();
    }
    assert_eq!(
        core.snapshot().testaments[&TESTAMENT].content().outcome,
        OutcomeKind::Failed
    );

    // The already-staged diagnostic is visible when deciding whether a new work
    // artifact may consume the final slot, before either row is published.
    let mut core = opened();
    core.limits.max_artifacts_per_set = 2;
    let mut pending = PendingState::new();
    pending.reserve(1).unwrap();
    let (diagnostic, _) = attachment_at("error", 713, 13, 1);
    let staged = core.stage_pending(&pending, &diagnostic).unwrap();
    pending.accept(&core, staged).unwrap();
    let (work, _) = attachment_at("document", 714, 14, 1);
    assert!(core.stage_pending(&pending, &work).is_ok());
    assert!(
        core.stage_managed_pending_bounded(&pending, &managed(work), BYTES)
            .is_ok()
    );
}

#[test]
fn historical_full_sets_replay_unchanged_without_forging_new_failure_evidence() {
    let mut core = opened();
    core.limits.max_artifacts_per_set = 1;
    let (work, reference) = attachment_at("document", 710, 10, 1);
    refuse_both(&core, work.clone(), ErrorCode::Capacity);
    let original = recorded(&core, work.clone());
    let bytes = original.encode_v1().unwrap();
    let result = core
        .apply(
            SessionSeq(core.sequence().0 + 1),
            PreparedMutation::decode_v1(&bytes).unwrap(),
        )
        .unwrap();
    let restored = Core::decode_checkpoint(&core.encode_checkpoint().unwrap()).unwrap();
    assert_eq!(
        restored.prepare(&work),
        Err(DomainOutcome::Duplicate(Box::new(result.receipt)))
    );
    assert_eq!(
        restored.snapshot().evidence_sets[&SET].artifacts,
        [reference]
    );
    let (error, _) = attachment_at("error", 711, 11, 1);
    refuse_both(&restored, error, ErrorCode::Capacity);
    refuse_both(
        &restored,
        close(OutcomeKind::Failed, vec![reference]),
        ErrorCode::EvidenceNotDurable,
    );
    assert!(restored.snapshot().testaments.is_empty());
}

#[test]
fn a_positive_new_attestation_cannot_repair_deduplicated_historical_zero_custody() {
    let mut core = opened();
    core.limits.max_artifacts_per_set = 1;
    let mut value = artifact("error");
    value.id = ArtifactId::from_u128(710);
    let hash = value.content.content_hash().unwrap();
    let mut registered = input(
        10,
        WORKER,
        Command::RegisterArtifact {
            artifact: value.clone(),
        },
    );
    registered.authority.evidence.push(EvidenceAttestation {
        descriptor_hash: hash,
        custody_revision: 0,
        durable: true,
        schema_valid: true,
    });
    // Original row identity/custody is preserved; the new headroom rule applies
    // to attaching it, not to rewriting historical artifact content or metadata.
    let prepared = recorded(&core, registered);
    core.apply(SessionSeq(core.sequence().0 + 1), prepared)
        .unwrap();
    let (mut candidate, _) = attach(value, 1);
    candidate.request_id = RequestId::from_u128(11);
    refuse_both(&core, candidate, ErrorCode::Capacity);
    assert!(core.snapshot().evidence_sets[&SET].artifacts.is_empty());
    assert_eq!(
        core.snapshot().artifacts[&ArtifactId::from_u128(710)]
            .lifecycle()
            .custody_revision,
        0
    );
}
