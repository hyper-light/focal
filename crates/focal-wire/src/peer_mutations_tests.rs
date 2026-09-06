use super::*;

#[test]
fn participant_handshake_requires_explicit_owner_support_and_never_downgrades() {
    let hello = Hello {
        versions: vec![PEER_PROTOCOL_VERSION],
        max_frame_bytes: 4096,
        max_items: 8,
    };
    assert_eq!(
        limits().negotiate_profiles(&hello, true, false),
        Err(AccessError::UnsupportedProtocol)
    );
    assert_eq!(
        limits().negotiate_profiles(&hello, false, true),
        Err(AccessError::UnsupportedProtocol)
    );
    let negotiated = limits().negotiate_profiles(&hello, true, true).unwrap();
    for protocol in [
        PROTOCOL_VERSION,
        MANAGED_PROTOCOL_VERSION,
        PEER_PROTOCOL_VERSION,
    ] {
        assert!(negotiated.accepts_protocol(protocol));
    }
    assert!(!negotiated.accepts_protocol(4));
    let old = Negotiated {
        protocol: MANAGED_PROTOCOL_VERSION,
        ..negotiated
    };
    assert!(!old.accepts_protocol(PEER_PROTOCOL_VERSION));
}

fn authority() -> AuthorityContext {
    AuthorityContext {
        runtime: false,
        cause: Cause::Root(RootCommandId::from_u128(4)),
        policy_revision: 1,
        logical_time: 10,
        evidence: vec![],
    }
}
fn claim() -> Claim {
    Claim::new(
        ClaimContent {
            ledger: ledger(),
            schema: SCHEMA_MAJOR,
            occurrence: OccurrenceId::from_u128(10),
            description: "peer standing".into(),
            relations: BTreeSet::from([Relation {
                kind: RelationKind::Issuer,
                target: RelationTarget::Participant(grant().principal),
            }]),
            scopes: BTreeSet::new(),
            requirements: vec![],
            deadline: None,
        },
        ContentHash([1; 32]),
        ClaimLifecycle {
            status: ClaimStatus::Posted,
            revision: ObjectRevision(2),
            created: SessionSeq(1),
            history: vec![],
            receipt: None,
            evidence_set: None,
            testament: None,
            local_complete: false,
            released: false,
            terminal_witness: None,
        },
    )
}

#[test]
fn peer_ingress_never_grants_blanket_runtime_or_changes_legacy_admission() {
    let peer = AuthenticatedPeer::local(grant()).unwrap();
    for command in [
        Command::AcknowledgeTestament {
            claim: ClaimId::from_u128(10),
            testament: TestamentId::from_u128(11),
        },
        Command::BeginWholeWorkValidation {
            claim: ClaimId::from_u128(10),
        },
        Command::CompleteWholeWork {
            claim: ClaimId::from_u128(10),
        },
    ] {
        let mut envelope = request(90);
        envelope.operation = Operation::Submit {
            expected_revision: Some(ObjectRevision(2)),
            command: command.clone(),
        };
        assert!(matches!(
            verify_request(peer.clone(), envelope.clone(), &limits()),
            Err(AccessError::Unauthorized)
        ));
        envelope.protocol = participant_protocol(&envelope.operation);
        assert_eq!(envelope.protocol, PEER_PROTOCOL_VERSION);
        let input = verify_request(peer.clone(), envelope.clone(), &limits())
            .unwrap()
            .into_authenticated(authority())
            .unwrap();
        assert!(
            !input.authority.runtime,
            "authentication alone must not grant Runtime"
        );
        let committed = claim();
        assert_eq!(
            participant_authority(
                ledger(),
                peer.principal(),
                &command,
                input.expected_revision,
                Some(&committed)
            ),
            Ok(true)
        );
        for (principal, revision, stored) in [
            (
                ParticipantId::from_u128(99),
                input.expected_revision,
                Some(&committed),
            ),
            (peer.principal(), None, Some(&committed)),
            (peer.principal(), input.expected_revision, None),
        ] {
            assert_eq!(
                participant_authority(ledger(), principal, &command, revision, stored),
                Err(AccessError::Unauthorized)
            );
        }
        let mut foreign = ledger();
        foreign.session = SessionId::from_u128(99);
        assert_eq!(
            participant_authority(
                foreign,
                peer.principal(),
                &command,
                input.expected_revision,
                Some(&committed)
            ),
            Err(AccessError::Unauthorized)
        );
        let node = AuthenticatedPeer::local(PeerGrant {
            role: PeerRole::Node { node_id: 1 },
            ..grant()
        })
        .unwrap();
        assert!(matches!(
            verify_request(node, envelope, &limits()),
            Err(AccessError::Unauthorized)
        ));
    }
    let mut forged = request(91);
    forged.protocol = PEER_PROTOCOL_VERSION;
    forged.operation = Operation::Submit {
        expected_revision: None,
        command: Command::RevokeClaim {
            claim: ClaimId::from_u128(10),
            reason: "forged privilege".into(),
        },
    };
    assert!(matches!(
        verify_request(peer, forged, &limits()),
        Err(AccessError::UnsupportedProtocol)
    ));
}

#[test]
fn managed_peer_profile_preserves_identity_and_negative_receipt_fences() {
    let peer = AuthenticatedPeer::local(grant()).unwrap();
    let key = ManagedRequestKey {
        stream: RequestStreamIdentity {
            cluster: [7; 16],
            ledger: ledger(),
            principal: peer.principal(),
            slot: 0,
            generation: 1,
        },
        ordinal: 2,
        id: RequestId::from_u128(88),
    };
    let mut envelope = request(88);
    envelope.protocol = PEER_PROTOCOL_VERSION;
    envelope.operation = Operation::Managed {
        key,
        operation: ManagedOperation::Submit {
            expected_revision: Some(ObjectRevision(2)),
            command: Command::BeginWholeWorkValidation {
                claim: ClaimId::from_u128(10),
            },
        },
    };
    let (found, _, _) = managed_request_identity(&envelope).unwrap();
    assert_eq!(found, key);
    verify_request(peer.clone(), envelope.clone(), &limits()).unwrap();
    for (through, valid) in [(1, false), (2, true)] {
        let reply = envelope.reply(Response::Error(AccessError::ManagedRetired { through }));
        assert_eq!(
            validate_response(&envelope, &reply, Some(peer.principal()), &limits()).is_ok(),
            valid
        );
    }
    envelope.protocol = MANAGED_PROTOCOL_VERSION;
    assert!(matches!(
        verify_request(peer, envelope, &limits()),
        Err(AccessError::Unauthorized)
    ));
}

#[test]
fn standalone_artifact_and_verdict_require_the_actual_producer_and_evaluator() {
    let principal = grant().principal;
    let artifact = NewArtifact {
        id: ArtifactId::from_u128(12),
        content: ArtifactContent {
            ledger: ledger(),
            schema: SCHEMA_MAJOR,
            kind: "text".into(),
            schema_hash: ContentHash([2; 32]),
            producer: principal,
            receipt: None,
            metadata: vec![],
            payload: ArtifactPayload::Inline(b"proof".to_vec()),
            inputs: BTreeSet::new(),
            visibility: BTreeSet::new(),
        },
    };
    let mut command = Command::RegisterArtifact { artifact };
    assert_eq!(
        participant_authority(ledger(), principal, &command, None, None),
        Ok(true)
    );
    assert_eq!(
        participant_authority(ledger(), ParticipantId::from_u128(99), &command, None, None),
        Err(AccessError::Unauthorized)
    );
    if let Command::RegisterArtifact { artifact } = &mut command {
        artifact.content.receipt = Some(ReceiptFence {
            receipt: ReceiptId::from_u128(8),
            epoch: 1,
        });
    }
    assert_eq!(
        participant_authority(ledger(), principal, &command, None, None),
        Err(AccessError::Unauthorized)
    );
    let verdict = Command::RecordFencedValidationVerdict {
        receipt: None,
        verdict: VerdictRecord {
            run: ValidationRunId {
                validation: ValidationId::from_u128(7),
                target_hash: ContentHash([3; 32]),
                phase: ValidationPhase::WholeWork,
                epoch: 1,
            },
            evaluator: principal,
            handler: HandlerRef {
                id: ValidatorId::from_u128(6),
                version: ContentHash([4; 32]),
                agentic: false,
            },
            attempt: 0,
            manifest: ContentHash([5; 32]),
            value: VerdictValue::Pass,
            evidence: vec![],
        },
    };
    assert_eq!(
        participant_authority(ledger(), principal, &verdict, None, None),
        Ok(false)
    );
    assert_eq!(
        participant_authority(ledger(), ParticipantId::from_u128(99), &verdict, None, None),
        Err(AccessError::Unauthorized)
    );
}
