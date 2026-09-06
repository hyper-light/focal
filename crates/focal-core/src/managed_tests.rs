use super::*;
use crate::tests::{ISSUER, WORKER, input, ledger, new_claim, setup};

const BYTES: usize = 16 * 1024 * 1024;

fn managed(legacy: AuthenticatedInput) -> ManagedAuthenticatedInput {
    ManagedAuthenticatedInput {
        key: ManagedRequestKey {
            stream: RequestStreamIdentity {
                cluster: [9; 16],
                ledger: legacy.ledger,
                principal: legacy.principal,
                slot: 0,
                generation: 1,
            },
            ordinal: u64::try_from(u128::from_be_bytes(legacy.request_id.0)).unwrap(),
            id: legacy.request_id,
        },
        expected_revision: legacy.expected_revision,
        authority: legacy.authority,
        command: legacy.command,
    }
}

fn stage(core: &Core, input: &ManagedAuthenticatedInput) -> StagedManagedMutation {
    core.stage_managed_pending_bounded(&PendingState::new(), input, BYTES)
        .unwrap()
}

#[test]
fn managed_complete_workflow_matches_legacy_domain_state_and_replays_exactly() {
    let mut legacy = setup();
    let mut scoped = legacy.clone();
    let mut replay = scoped.clone();
    let initial_receipts = scoped.snapshot().receipts.clone();
    let initial_epochs = scoped.snapshot().epochs.clone();
    let claim = new_claim(100);
    let id = claim.id;
    let receipt = ReceiptFence {
        receipt: ReceiptId::from_u128(100),
        epoch: 1,
    };
    let testament = TestamentId::from_u128(104);
    let evidence_set = EvidenceSetId::from_u128(103);
    let commands = [
        (ISSUER, Command::GenerateClaim { claim }),
        (ISSUER, Command::PostClaim { claim: id }),
        (
            WORKER,
            Command::AcquireReceipt {
                claim: id,
                receipt: receipt.receipt,
                epoch: 1,
            },
        ),
        (
            WORKER,
            Command::BeginEvidenceSet {
                claim: id,
                receipt,
                evidence_set,
            },
        ),
        (
            WORKER,
            Command::CloseTestament {
                claim: id,
                receipt,
                testament,
                evidence_set,
                manifest: vec![],
                summary: "finished".into(),
                confidence: Confidence::Committed,
                outcome: OutcomeKind::Complete,
            },
        ),
        (
            ISSUER,
            Command::AcknowledgeTestament {
                claim: id,
                testament,
            },
        ),
        (ISSUER, Command::BeginWholeWorkValidation { claim: id }),
        (ISSUER, Command::CompleteWholeWork { claim: id }),
    ];
    for (offset, (actor, command)) in commands.into_iter().enumerate() {
        let ordinary = input(1000 + offset as u128, actor, command);
        let scoped_input = managed(ordinary.clone());
        let legacy_prepared = legacy.prepare(&ordinary).unwrap();
        let old = legacy
            .apply(SessionSeq(legacy.sequence().0 + 1), legacy_prepared)
            .unwrap();
        let staged = stage(&scoped, &scoped_input);
        let row_ids = staged
            .patch()
            .claims()
            .map(|(id, _)| *id)
            .collect::<Vec<_>>();
        assert!(!row_ids.is_empty());
        scoped
            .audit_managed_pending_stage(&PendingState::new(), &staged, BYTES)
            .unwrap();
        assert_eq!(
            scoped.validate_managed(&staged).unwrap(),
            old.receipt.sequence
        );
        let log = postcard::to_stdvec(staged.prepared()).unwrap();
        let prepared: PreparedManagedMutation = postcard::from_bytes(&log).unwrap();
        let restored = replay.replay_managed_bounded(&prepared, BYTES).unwrap();
        let actual = scoped.publish_managed(staged).unwrap();
        assert_eq!(replay.publish_managed(restored).unwrap(), actual);
        assert_eq!(actual.key, scoped_input.key);
        assert_eq!(actual.sequence, old.receipt.sequence);
        assert_eq!(actual.command_hash, old.receipt.command_hash);
        assert_eq!(actual.outcome, old.receipt.outcome);
        assert_eq!(actual.deltas, old.deltas);
        assert_eq!(actual.effects, old.effects);
        assert_eq!(scoped.snapshot().receipts, initial_receipts);
        assert_eq!(scoped.snapshot().epochs, initial_epochs);
        let mut expected = legacy.snapshot().clone();
        expected.receipts = initial_receipts.clone();
        assert_eq!(&expected, scoped.snapshot());
        assert_eq!(
            replay.encode_checkpoint().unwrap(),
            scoped.encode_checkpoint().unwrap()
        );
        replay = Core::decode_checkpoint(&replay.encode_checkpoint().unwrap()).unwrap();
    }
    assert_eq!(
        scoped.snapshot().claims[&id].lifecycle().status,
        ClaimStatus::Satisfied
    );
}

#[test]
fn managed_admission_preserves_auth_revision_and_resource_checks_without_legacy_keys() {
    let mut core = Core::new(
        ledger(),
        Limits {
            max_requests: 1,
            ..Limits::default()
        },
    );
    let negotiation = input(
        1,
        ISSUER,
        Command::NegotiateEpoch {
            epoch: RequestEpoch(1),
        },
    );
    core.apply(SessionSeq(1), core.prepare(&negotiation).unwrap())
        .unwrap();
    let request = managed(input(
        2,
        ISSUER,
        Command::GenerateClaim {
            claim: new_claim(100),
        },
    ));
    assert!(matches!(
        core.prepare(&input(2, ISSUER, request.command.clone())),
        Err(DomainOutcome::Refuse {
            code: ErrorCode::Capacity,
            ..
        })
    ));
    assert!(matches!(
        core.stage_managed_pending_bounded(&PendingState::new(), &request, 1),
        Err(StagingError::Capacity)
    ));
    let before = core.encode_checkpoint().unwrap();
    let mut wrong_actor = request.clone();
    wrong_actor.key.stream.principal = WORKER;
    wrong_actor.authority.runtime = false;
    assert!(matches!(
        core.stage_managed_pending_bounded(&PendingState::new(), &wrong_actor, BYTES),
        Err(StagingError::Domain(DomainOutcome::Refuse {
            code: ErrorCode::WrongActor,
            ..
        }))
    ));
    for command in [
        Command::NegotiateEpoch {
            epoch: RequestEpoch(2),
        },
        Command::AdvanceEpochFloor {
            minimum: RequestEpoch(1),
        },
    ] {
        let denied = ManagedAuthenticatedInput {
            command,
            ..request.clone()
        };
        assert!(matches!(
            core.stage_managed_pending_bounded(&PendingState::new(), &denied, BYTES),
            Err(StagingError::Domain(DomainOutcome::Refuse {
                code: ErrorCode::InvalidEpoch,
                ..
            }))
        ));
    }
    for field in 0..5 {
        let mut invalid = request.clone();
        match field {
            0 => invalid.key.stream.cluster = [0; 16],
            1 => invalid.key.stream.principal = ParticipantId::from_u128(0),
            2 => invalid.key.stream.generation = 0,
            3 => invalid.key.ordinal = 0,
            _ => invalid.key.id = RequestId::from_u128(0),
        }
        assert!(matches!(
            core.stage_managed_pending_bounded(&PendingState::new(), &invalid, BYTES),
            Err(StagingError::Domain(DomainOutcome::Refuse {
                code: ErrorCode::InvalidSchema,
                ..
            }))
        ));
    }
    let wrong_ledger = ManagedAuthenticatedInput {
        key: ManagedRequestKey {
            stream: RequestStreamIdentity {
                ledger: LedgerId {
                    session: SessionId::from_u128(500),
                    ..ledger()
                },
                ..request.key.stream
            },
            ..request.key
        },
        ..request.clone()
    };
    assert!(matches!(
        core.stage_managed_pending_bounded(&PendingState::new(), &wrong_ledger, BYTES),
        Err(StagingError::Domain(DomainOutcome::Refuse {
            code: ErrorCode::InvalidNamespace,
            ..
        }))
    ));
    assert_eq!(core.encode_checkpoint().unwrap(), before);
    core.publish_managed(stage(&core, &request)).unwrap();
    let mut stale = managed(input(
        3,
        ISSUER,
        Command::PostClaim {
            claim: ClaimId::from_u128(100),
        },
    ));
    stale.expected_revision = Some(ObjectRevision(99));
    assert!(matches!(
        core.stage_managed_pending_bounded(&PendingState::new(), &stale, BYTES),
        Err(StagingError::Domain(DomainOutcome::Refuse {
            code: ErrorCode::RevisionConflict,
            ..
        }))
    ));
    assert_eq!(core.snapshot().receipts.len(), 1);
    assert_eq!(core.snapshot().epochs.len(), 1);
    assert_eq!(core.sequence(), SessionSeq(2));
}

#[test]
fn managed_rows_are_visible_to_later_managed_and_legacy_pending_commands() {
    let mut core = setup();
    let mut pending = PendingState::new();
    let generate = managed(input(
        1000,
        ISSUER,
        Command::GenerateClaim {
            claim: new_claim(100),
        },
    ));
    let staged = stage(&core, &generate);
    assert!(matches!(
        pending.validate_managed_next(&core, &staged),
        Err(CoreError::Exhausted)
    ));
    pending.reserve(3).unwrap();
    let (first, _) = pending.accept_managed(&core, staged).unwrap();
    assert_eq!(core.snapshot().claims.len(), 0);
    assert!(
        pending
            .view(&core)
            .unwrap()
            .claim(&ClaimId::from_u128(100))
            .is_some()
    );
    let post = input(
        1001,
        ISSUER,
        Command::PostClaim {
            claim: ClaimId::from_u128(100),
        },
    );
    let staged = core.stage_pending_bounded(&pending, &post, BYTES).unwrap();
    let (second, _) = pending.accept(&core, staged).unwrap();
    let acquire = managed(input(
        1002,
        WORKER,
        Command::AcquireReceipt {
            claim: ClaimId::from_u128(100),
            receipt: ReceiptId::from_u128(100),
            epoch: 1,
        },
    ));
    let staged = core
        .stage_managed_pending_bounded(&pending, &acquire, BYTES)
        .unwrap();
    core.audit_managed_pending_stage(&pending, &staged, BYTES)
        .unwrap();
    assert_eq!(
        staged
            .view_after(&core, &pending)
            .unwrap()
            .claim(&ClaimId::from_u128(100))
            .unwrap()
            .lifecycle()
            .status,
        ClaimStatus::Received
    );
    let (third, _) = pending.accept_managed(&core, staged).unwrap();
    let restored = core.replay_managed_bounded(&first, BYTES).unwrap();
    core.publish_managed(restored).unwrap();
    pending.drop_prefix(1, &core).unwrap();
    core.apply(SessionSeq(core.sequence().0 + 1), second)
        .unwrap();
    pending.drop_prefix(1, &core).unwrap();
    core.publish_managed(core.replay_managed_bounded(&third, BYTES).unwrap())
        .unwrap();
    pending.drop_prefix(1, &core).unwrap();
    assert!(pending.is_empty());
    assert_eq!(core.snapshot().receipts.len(), 4);
    assert_eq!(core.sequence(), SessionSeq(6));
}

#[test]
fn managed_provenance_and_actual_accesses_never_alias_legacy_history() {
    let mut core = setup();
    let request = managed(input(
        1000,
        ISSUER,
        Command::GenerateClaim {
            claim: new_claim(100),
        },
    ));
    let recorder = Recorder::new(ledger(), core.sequence(), 65_536);
    let staged = core
        .stage_managed_recorded(&request, core.view(), &recorder)
        .unwrap();
    let accesses = recorder.finish();
    assert!(!accesses.session_exclusive);
    assert!(
        accesses
            .writes
            .contains(&AccessKey::Claim(ClaimId::from_u128(100)))
    );
    assert!(
        accesses
            .reads
            .iter()
            .chain(&accesses.writes)
            .all(|key| !matches!(key.table(), Some(StateTable::Epochs | StateTable::Receipts)))
    );
    let before = core.encode_checkpoint().unwrap();
    let mut tampered = stage(&core, &request);
    tampered.result.key.ordinal = 99;
    assert!(core.publish_managed(tampered).is_err());
    let mut tampered = stage(&core, &request);
    tampered.version.rows.claims.clear();
    assert!(core.publish_managed(tampered).is_err());
    let mut prepared = staged.prepared().clone();
    prepared.command_hash = ContentHash([55; 32]);
    assert!(matches!(
        core.replay_managed_bounded(&prepared, BYTES),
        Err(CoreError::Checksum)
    ));
    assert_eq!(core.encode_checkpoint().unwrap(), before);
    core.publish_managed(staged).unwrap();
    assert!(matches!(
        core.replay_managed_bounded(&prepared, BYTES),
        Err(CoreError::StalePreparation)
    ));
}
