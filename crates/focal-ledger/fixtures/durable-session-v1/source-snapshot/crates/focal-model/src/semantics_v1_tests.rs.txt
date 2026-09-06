use super::*;
use std::collections::BTreeSet;

fn ledger(tenant: u128) -> LedgerId {
    LedgerId {
        tenant: TenantId::from_u128(tenant),
        session: SessionId::from_u128(1),
    }
}

fn content(relations: impl IntoIterator<Item = Relation>) -> ClaimContent {
    ClaimContent {
        ledger: ledger(5),
        // Relation interpretation historically did not perform admission checks.
        schema: 0,
        occurrence: OccurrenceId::default(),
        description: String::new(),
        relations: relations.into_iter().collect(),
        scopes: BTreeSet::new(),
        requirements: Vec::new(),
        deadline: None,
    }
}

fn relation(kind: RelationKind, target: RelationTarget) -> Relation {
    Relation { kind, target }
}

#[test]
fn original_status_terminality_activity_and_lifecycle_actions_are_fixed() {
    use ClaimStatus as S;
    use LifecycleAction as A;
    let rows = [
        (S::Generated, false, A::Generated),
        (S::Posted, false, A::Posted),
        (S::Received, false, A::Received),
        (S::Progressed, false, A::Progressed),
        (S::TestamentGenerated, false, A::TestamentGenerated),
        (S::TestamentAcknowledged, false, A::TestamentAcknowledged),
        (S::Validating, false, A::Validating),
        (S::Satisfied, true, A::Satisfied),
        (S::PostFailed, true, A::PostFailed),
        (S::ReceiptFailed, true, A::ReceiptFailed),
        (
            S::TestamentGenerationFailed,
            true,
            A::TestamentGenerationFailed,
        ),
        (S::ValidationIncomplete, true, A::ValidationIncomplete),
        (S::ValidationFailed, true, A::ValidationFailed),
        (S::ValidationErrored, true, A::ValidationErrored),
        (S::Cancelled, true, A::Cancelled),
        (S::Expired, true, A::Expired),
        (S::Revoked, true, A::Revoked),
        (S::Superseded, true, A::Superseded),
        (S::DependencyFailed, true, A::DependencyFailed),
        (S::Deadlocked, true, A::Deadlocked),
    ];
    assert_eq!(rows.len(), 20);
    assert_eq!(S::ALL.len(), rows.len());
    for (status, terminal, expected_action) in rows {
        assert_eq!(is_terminal(status), terminal, "{status:?}");
        assert_eq!(is_active(status), !terminal, "{status:?}");
        assert_eq!(lifecycle_action(status), expected_action, "{status:?}");
        assert_eq!(status.is_terminal(), terminal);
        assert_eq!(status.is_active(), !terminal);
        assert_eq!(LifecycleAction::from(status), expected_action);
    }
}

#[test]
fn verdict_severity_is_not_the_serialized_variant_order() {
    let rows = [
        (VerdictValue::Pass, 0),
        (VerdictValue::Fail, 3),
        (VerdictValue::Incomplete, 1),
        (VerdictValue::Error, 2),
    ];
    assert_eq!(VerdictValue::ALL.len(), rows.len());
    for (value, expected) in rows {
        assert_eq!(severity(value), expected);
        assert_eq!(value.severity(), expected);
    }
}

#[test]
fn every_original_relation_kind_rejects_incompatible_target_shapes() {
    let participant = ParticipantId::from_u128(7);
    let root = RootCommandId::from_u128(8);
    let claim = ClaimId::from_u128(9);
    let object = |kind| {
        RelationTarget::Object(ObjectRef {
            ledger: ledger(1),
            kind,
            id: ObjectId(claim.0),
        })
    };
    // Target, party, action, cause and dependency interpretations for one target.
    let targets = [
        (
            RelationTarget::Participant(participant),
            Some(participant),
            None,
            None,
            None,
        ),
        (
            object(ObjectKind::Claim),
            None,
            None,
            Some(Cause::Claim(claim)),
            Some(claim),
        ),
        (object(ObjectKind::Testament), None, None, None, None),
        (object(ObjectKind::Validation), None, None, None, None),
        (object(ObjectKind::Artifact), None, None, None, None),
        (
            RelationTarget::Action(ActionType::Challenge),
            None,
            Some(ActionType::Challenge),
            None,
            None,
        ),
        (
            RelationTarget::Root(root),
            None,
            None,
            Some(Cause::Root(root)),
            None,
        ),
    ];
    let kinds = [
        RelationKind::Issuer,
        RelationKind::Subject,
        RelationKind::Evaluator,
        RelationKind::ClaimAction,
        RelationKind::Supersedes,
        RelationKind::DependsOn,
        RelationKind::Awaits,
        RelationKind::CausedBy,
        RelationKind::Refines,
        RelationKind::ConflictsWith,
        RelationKind::DerivedFrom,
        RelationKind::Reviews,
        RelationKind::Amends,
        RelationKind::ContributedBy,
        RelationKind::Invalidates,
    ];
    assert_eq!(RelationKind::ALL.len(), kinds.len());
    for kind in kinds {
        for (target, party, target_action, target_cause, dependency) in &targets {
            let claim = content([relation(kind, target.clone())]);
            let expected_issuer = if kind == RelationKind::Issuer {
                *party
            } else {
                None
            };
            let expected_subject = if kind == RelationKind::Subject {
                *party
            } else {
                None
            };
            let expected_action = if kind == RelationKind::ClaimAction {
                *target_action
            } else {
                None
            };
            let expected_cause = if kind == RelationKind::CausedBy {
                target_cause.clone()
            } else {
                None
            };
            assert_eq!(issuer(&claim), expected_issuer, "{kind:?} {target:?}");
            assert_eq!(subject(&claim), expected_subject, "{kind:?} {target:?}");
            assert_eq!(action(&claim), expected_action, "{kind:?} {target:?}");
            assert_eq!(cause(&claim), expected_cause, "{kind:?} {target:?}");
            assert_eq!(claim.issuer(), expected_issuer);
            assert_eq!(claim.subject(), expected_subject);
            assert_eq!(claim.action(), expected_action);
            assert_eq!(claim.cause(), expected_cause);
            let expected_dependencies: Vec<_> = dependency.iter().copied().collect();
            assert_eq!(
                dependencies(&claim, kind).collect::<Vec<_>>(),
                expected_dependencies
            );
            assert_eq!(
                claim.dependencies(kind).collect::<Vec<_>>(),
                expected_dependencies
            );
            let other = if kind == RelationKind::DependsOn {
                RelationKind::Awaits
            } else {
                RelationKind::DependsOn
            };
            assert_eq!(dependencies(&claim, other).next(), None);
        }
    }
}

#[test]
fn all_ten_original_action_targets_are_preserved() {
    let actions = [
        ActionType::Work,
        ActionType::Consultation,
        ActionType::Challenge,
        ActionType::Feedback,
        ActionType::Approval,
        ActionType::Summon,
        ActionType::Handoff,
        ActionType::Evaluation,
        ActionType::Correction,
        ActionType::Teardown,
    ];
    assert_eq!(ActionType::ALL.len(), actions.len());
    for expected in actions {
        let claim = content([relation(
            RelationKind::ClaimAction,
            RelationTarget::Action(expected),
        )]);
        assert_eq!(action(&claim), Some(expected));
        assert_eq!(claim.action(), Some(expected));
    }
}

#[test]
fn ordered_selection_and_cross_ledger_dependencies_keep_original_behavior() {
    let object = |tenant, id| {
        RelationTarget::Object(ObjectRef::claim(ledger(tenant), ClaimId::from_u128(id)))
    };
    let claim = content([
        relation(
            RelationKind::Issuer,
            RelationTarget::Participant(ParticipantId::from_u128(9)),
        ),
        relation(
            RelationKind::Issuer,
            RelationTarget::Participant(ParticipantId::from_u128(2)),
        ),
        relation(
            RelationKind::Subject,
            RelationTarget::Participant(ParticipantId::from_u128(8)),
        ),
        relation(
            RelationKind::Subject,
            RelationTarget::Participant(ParticipantId::from_u128(3)),
        ),
        relation(
            RelationKind::ClaimAction,
            RelationTarget::Action(ActionType::Teardown),
        ),
        relation(
            RelationKind::ClaimAction,
            RelationTarget::Action(ActionType::Work),
        ),
        relation(
            RelationKind::CausedBy,
            RelationTarget::Root(RootCommandId::from_u128(1)),
        ),
        relation(RelationKind::CausedBy, object(1, 9)),
        relation(RelationKind::CausedBy, object(5, 2)),
        relation(RelationKind::DependsOn, object(9, 9)),
        relation(RelationKind::DependsOn, object(5, 8)),
        relation(RelationKind::DependsOn, object(5, 2)),
        relation(RelationKind::DependsOn, object(1, 9)),
        relation(RelationKind::Awaits, object(0, 1)),
    ]);
    assert_eq!(issuer(&claim), Some(ParticipantId::from_u128(2)));
    assert_eq!(subject(&claim), Some(ParticipantId::from_u128(3)));
    assert_eq!(action(&claim), Some(ActionType::Work));
    // Object targets sort before Root targets; the first claim can be foreign.
    assert_eq!(cause(&claim), Some(Cause::Claim(ClaimId::from_u128(9))));
    // Preserve object ordering, foreign-ledger IDs and duplicate resulting IDs.
    assert_eq!(
        dependencies(&claim, RelationKind::DependsOn).collect::<Vec<_>>(),
        [9, 2, 8, 9].map(ClaimId::from_u128)
    );
    assert_eq!(
        dependencies(&claim, RelationKind::Awaits).collect::<Vec<_>>(),
        [ClaimId::from_u128(1)]
    );
    let roots = content([
        relation(
            RelationKind::CausedBy,
            RelationTarget::Root(RootCommandId::from_u128(8)),
        ),
        relation(
            RelationKind::CausedBy,
            RelationTarget::Root(RootCommandId::from_u128(1)),
        ),
    ]);
    assert_eq!(
        cause(&roots),
        Some(Cause::Root(RootCommandId::from_u128(1)))
    );
    let empty = content([]);
    assert_eq!(
        (
            issuer(&empty),
            subject(&empty),
            action(&empty),
            cause(&empty)
        ),
        (None, None, None, None)
    );
    assert_eq!(dependencies(&empty, RelationKind::DependsOn).next(), None);
}

#[test]
fn all_29_original_commands_keep_their_revision_target() {
    let bytes = include_bytes!("../../focal-core/fixtures/durable-v1-inputs/commands.rows");
    let (durable_v1::Value(commands), rest): (durable_v1::Value<Vec<Command>>, _) =
        postcard::take_from_bytes(bytes).unwrap();
    assert!(rest.is_empty());
    assert_eq!(commands.len(), 58);
    let existing = Some(ClaimId::from_u128(0xc10000000000000000000000000000ff));
    // Each explicit row corresponds to the original frozen command ordinal.
    let mut expected = [
        None,                           // NegotiateEpoch
        None,                           // AdvanceEpochFloor
        Some(ClaimId::from_u128(1001)), // GenerateClaim
        None,                           // GenerateClaimBatch, including a nonempty batch
        existing,                       // PostClaim
        existing,                       // AcquireReceipt
        existing,                       // AdoptReceipt
        existing,                       // RecordProgress
        existing,                       // BeginEvidenceSet
        existing,                       // AttachArtifact
        existing,                       // CloseTestament
        existing,                       // AcknowledgeTestament
        existing,                       // BeginWholeWorkValidation
        existing,                       // BeginIncrementValidation
        None,                           // RecordValidationVerdict
        existing,                       // CompleteWholeWork
        existing,                       // FailPost
        existing,                       // FailReceipt
        existing,                       // FailTestamentGeneration
        existing,                       // CancelClaim
        existing,                       // RevokeClaim
        existing,                       // ExpireClaim
        existing,                       // SupersedeClaim uses predecessor, not successor
        existing,                       // RegisterMonitor uses owner
        None,                           // RebindMonitor has no revision target
        existing,                       // ReleaseScope
        None,                           // RegisterArtifact
        None,                           // ExpireMonitor
        None,                           // RecordFencedValidationVerdict
    ];
    for (round, rows) in commands.chunks_exact(29).enumerate() {
        if round == 1 {
            expected[2] = Some(ClaimId::from_u128(1000));
        }
        for (index, (command, target)) in rows.iter().zip(expected).enumerate() {
            assert_eq!(usize::from(command.code()), index + 1);
            assert_eq!(claim_id(command), target, "round {round}, command {index}");
            assert_eq!(command.claim_id(), target, "round {round}, command {index}");
        }
    }
}

#[test]
fn managed_stream_and_request_key_boundaries_are_fixed() {
    let stream = RequestStreamIdentity {
        cluster: [1; 16],
        ledger: ledger(1),
        principal: ParticipantId::from_u128(1),
        slot: 0,
        generation: 1,
    };
    let invalid = [
        RequestStreamIdentity {
            cluster: [0; 16],
            ..stream
        },
        RequestStreamIdentity {
            ledger: LedgerId {
                tenant: TenantId::default(),
                ..stream.ledger
            },
            ..stream
        },
        RequestStreamIdentity {
            ledger: LedgerId {
                session: SessionId::default(),
                ..stream.ledger
            },
            ..stream
        },
        RequestStreamIdentity {
            principal: ParticipantId::default(),
            ..stream
        },
        RequestStreamIdentity {
            generation: 0,
            ..stream
        },
    ];
    for invalid in invalid {
        assert!(!request_stream_valid(&invalid));
        assert!(!invalid.is_valid());
        let key = ManagedRequestKey {
            stream: invalid,
            ordinal: u64::MAX,
            id: RequestId([0xff; 16]),
        };
        assert!(!request_key_valid(&key));
        assert!(!key.is_valid());
    }
    for slot in [0, 1, u32::MAX] {
        for (generation, stream_valid) in [(0, false), (1, true), (u64::MAX, true)] {
            let stream = RequestStreamIdentity {
                slot,
                generation,
                ..stream
            };
            assert_eq!(request_stream_valid(&stream), stream_valid);
            assert_eq!(stream.is_valid(), stream_valid);
            for (ordinal, ordinal_valid) in [(0, false), (1, true), (u64::MAX, true)] {
                for (id, id_valid) in [
                    (RequestId::default(), false),
                    (RequestId::from_u128(1), true),
                    (RequestId([0xff; 16]), true),
                ] {
                    let key = ManagedRequestKey {
                        stream,
                        ordinal,
                        id,
                    };
                    let expected = stream_valid && ordinal_valid && id_valid;
                    assert_eq!(request_key_valid(&key), expected);
                    assert_eq!(key.is_valid(), expected);
                }
            }
        }
    }
    for byte in 0..16 {
        let mut cluster = [0; 16];
        cluster[byte] = 1;
        assert!(request_stream_valid(&RequestStreamIdentity {
            cluster,
            ..stream
        }));
    }
}

fn hex_bytes(value: &str) -> Vec<u8> {
    assert_eq!(value.len() % 2, 0);
    value
        .as_bytes()
        .chunks_exact(2)
        .map(|pair| u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16).unwrap())
        .collect()
}

#[test]
fn original_manifest_header_order_duplicates_and_hashes_are_fixed() {
    // Captured before extraction from release model SHA-256
    // a9c8f0b37204106e8cc25e88cddf94fdf7fc0bd27c221af9455534cabf3a7517.
    // Original preimages use domain, big-endian u16 schema and u32 count,
    // followed by each 16-byte ID and 32-byte content hash in caller order.
    let first = ArtifactRef {
        id: ArtifactId([0x11; 16]),
        hash: ContentHash([0x22; 32]),
    };
    let second = ArtifactRef {
        id: ArtifactId([0x33; 16]),
        hash: ContentHash([0x44; 32]),
    };
    let empty_bytes = hex_bytes("666f63616c2e65766964656e63652d6d616e696665737400000100000000");
    let ordered_bytes = hex_bytes(concat!(
        "666f63616c2e65766964656e63652d6d616e696665737400000100000002",
        "11111111111111111111111111111111",
        "2222222222222222222222222222222222222222222222222222222222222222",
        "33333333333333333333333333333333",
        "4444444444444444444444444444444444444444444444444444444444444444",
    ));
    assert_eq!(SCHEMA, 1);
    for (artifacts, bytes, expected) in [
        (
            vec![],
            empty_bytes,
            "31cb33ab5dbce16c2023a9648231ef80c007490fae2c6f336ed75f5e5891f567",
        ),
        (
            vec![first, second],
            ordered_bytes,
            "44c30714402e926c2a947969f42519eb69f51d272db203f85c23a4b15b30e6f3",
        ),
    ] {
        let expected_hash = ContentHash(hex_bytes(expected).try_into().unwrap());
        assert_eq!(ContentHash(*blake3::hash(&bytes).as_bytes()), expected_hash);
        assert_eq!(manifest_hash(&artifacts).unwrap(), expected_hash);
        assert_eq!(crate::manifest_hash(&artifacts).unwrap(), expected_hash);
    }
    assert_eq!(
        manifest_hash(&[second, first]).unwrap().to_string(),
        "1c3c991d878c0a370c053ef8fc6935ab2dd32949567a31ba0b74f0d48d6251a1"
    );
    assert_eq!(
        manifest_hash(&[first, first]).unwrap().to_string(),
        "2cae1b0b73ec5063a3a13ff7081ecc4d6029a1249d89696dd9e132a3363626fb"
    );
}
