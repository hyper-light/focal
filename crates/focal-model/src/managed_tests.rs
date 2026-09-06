use super::*;

fn key() -> ManagedRequestKey {
    ManagedRequestKey {
        stream: RequestStreamIdentity {
            cluster: [1; 16],
            ledger: LedgerId {
                tenant: TenantId::from_u128(2),
                session: SessionId::from_u128(3),
            },
            principal: ParticipantId::from_u128(4),
            slot: 0,
            generation: 1,
        },
        ordinal: 1,
        id: RequestId::from_u128(5),
    }
}

fn original_digest(domain: &[u8], encoded: &[u8]) -> ContentHash {
    let mut bytes = domain.to_vec();
    bytes.extend_from_slice(&1_u16.to_be_bytes());
    bytes.extend_from_slice(encoded);
    ContentHash(*blake3::hash(&bytes).as_bytes())
}

#[test]
fn managed_domain_hash_preserves_exact_legacy_command_encoding() {
    let key = key();
    let legacy = AuthenticatedInput {
        ledger: key.stream.ledger,
        principal: key.stream.principal,
        request_epoch: RequestEpoch(77),
        request_id: key.id,
        expected_revision: Some(ObjectRevision(9)),
        authority: AuthorityContext {
            runtime: true,
            cause: Cause::Root(RootCommandId::from_u128(6)),
            policy_revision: 1,
            logical_time: 10,
            evidence: Vec::new(),
        },
        command: Command::PostClaim {
            claim: ClaimId::from_u128(8),
        },
    };
    // Reconstruct the original canonical fields independently of the new helper.
    let mut original = b"focal.command\0".to_vec();
    original.extend_from_slice(&1_u16.to_be_bytes());
    original.extend_from_slice(&legacy.ledger.tenant.0);
    original.extend_from_slice(&legacy.ledger.session.0);
    original.extend_from_slice(&legacy.principal.0);
    original.extend_from_slice(&5_u16.to_be_bytes());
    let body = postcard::to_allocvec(&(legacy.expected_revision, &legacy.command)).unwrap();
    original.extend_from_slice(&(body.len() as u32).to_be_bytes());
    original.extend_from_slice(&body);
    let expected = ContentHash(*blake3::hash(&original).as_bytes());
    assert_eq!(command_hash(&legacy).unwrap(), expected);
    let mut managed = ManagedAuthenticatedInput {
        key,
        expected_revision: legacy.expected_revision,
        authority: legacy.authority,
        command: legacy.command,
    };
    assert_eq!(managed_command_hash(&managed).unwrap(), expected);
    managed.key.stream.slot = 7;
    managed.key.stream.generation = 99;
    managed.key.ordinal = 400;
    managed.key.id = RequestId::from_u128(33);
    managed.authority.logical_time = 999;
    assert_eq!(managed_command_hash(&managed).unwrap(), expected);
    assert_eq!(
        managed_command_parts_hash(
            managed.key.stream.ledger,
            managed.key.stream.principal,
            &managed.expected_revision,
            &managed.command
        )
        .unwrap(),
        expected
    );
    managed.key.stream.principal = ParticipantId::from_u128(22);
    assert_ne!(managed_command_hash(&managed).unwrap(), expected);
}

#[test]
fn streamed_acknowledgment_hash_commits_complete_outcome_and_scope() {
    let receipt = ManagedReceipt {
        key: key(),
        sequence: SessionSeq(7),
        raft_index: 9,
        intent_hash: ContentHash([0; 32]),
        outcome: ManagedReceiptOutcome::Cursor {
            revision: 2,
            floor: SessionSeq(3),
            record: None,
        },
    };
    let expected = original_digest(
        b"focal.managed-receipt\0",
        &postcard::to_allocvec(&receipt).unwrap(),
    );
    assert_eq!(receipt.content_hash().unwrap(), expected);
    let mut variants = vec![receipt.clone(); 9];
    variants[0].key.stream.cluster = [2; 16];
    variants[1].key.stream.slot = 2;
    variants[2].key.stream.generation = 2;
    variants[3].key.ordinal = 2;
    variants[4].key.id = RequestId::from_u128(7);
    variants[5].sequence = SessionSeq(8);
    variants[6].raft_index = 10;
    variants[7].intent_hash = ContentHash([3; 32]);
    variants[8].outcome = ManagedReceiptOutcome::Sealed {
        family: ManagedRequestFamily::Cursor,
    };
    for changed in variants {
        assert_ne!(changed.content_hash().unwrap(), expected);
    }
    let control = RequestStreamControlInput {
        cluster: receipt.key.stream.cluster,
        ledger: receipt.key.stream.ledger,
        principal: receipt.key.stream.principal,
        id: RequestId::from_u128(10),
        command: RequestStreamCommand::Acknowledge {
            stream: receipt.key.stream,
            expected_revision: 3,
            through: 1,
            receipts: vec![ManagedReceiptAck {
                key: receipt.key,
                receipt_hash: expected,
            }],
        },
    };
    let bytes = postcard::to_allocvec(&(
        control.cluster,
        control.ledger,
        control.principal,
        &control.command,
    ))
    .unwrap();
    assert_eq!(
        control.intent_hash().unwrap(),
        original_digest(b"focal.request-stream-control\0", &bytes)
    );
    assert_eq!(
        control.intent_hash().unwrap(),
        request_stream_control_hash(
            control.cluster,
            control.ledger,
            control.principal,
            &control.command
        )
        .unwrap()
    );
    let mut different_id = control.clone();
    different_id.id = RequestId::from_u128(11);
    assert_eq!(
        control.intent_hash().unwrap(),
        different_id.intent_hash().unwrap()
    );
    if let RequestStreamCommand::Acknowledge { receipts, .. } = &mut different_id.command {
        receipts[0].receipt_hash = ContentHash([0; 32]);
    }
    assert_ne!(
        control.intent_hash().unwrap(),
        different_id.intent_hash().unwrap()
    );
}

#[test]
fn managed_identity_and_frozen_legacy_key_receipt_bytes_are_disjoint() {
    let managed = key();
    assert!(managed.is_valid()); // slot zero is a real stream slot
    let mut invalid = managed;
    invalid.stream.generation = 0;
    assert!(!invalid.is_valid());
    invalid = managed;
    invalid.ordinal = 0;
    assert!(!invalid.is_valid());
    invalid = managed;
    invalid.id = RequestId::from_u128(0);
    assert!(!invalid.is_valid());
    let key = RequestKey {
        principal: managed.stream.principal,
        epoch: RequestEpoch(7),
        id: managed.id,
    };
    let mut key_bytes = key.principal.0.to_vec();
    key_bytes.push(7);
    key_bytes.extend_from_slice(&key.id.0);
    assert_eq!(postcard::to_allocvec(&key).unwrap(), key_bytes);
    assert_eq!(postcard::from_bytes::<RequestKey>(&key_bytes).unwrap(), key);
    let receipt = MutationReceipt {
        ledger: managed.stream.ledger,
        key,
        sequence: SessionSeq(11),
        command_hash: ContentHash([9; 32]),
        outcome: CommandResult::Noop,
    };
    let mut receipt_bytes = receipt.ledger.tenant.0.to_vec();
    receipt_bytes.extend_from_slice(&receipt.ledger.session.0);
    receipt_bytes.extend_from_slice(&key_bytes);
    receipt_bytes.push(11);
    receipt_bytes.extend_from_slice(&[9; 32]);
    receipt_bytes.push(11); // original CommandResult::Noop variant
    assert_eq!(postcard::to_allocvec(&receipt).unwrap(), receipt_bytes);
    assert_eq!(
        postcard::from_bytes::<MutationReceipt>(&receipt_bytes).unwrap(),
        receipt
    );
    let mut value = serde_json::to_value(managed).unwrap();
    value["legacy_epoch"] = serde_json::Value::from(1);
    assert!(serde_json::from_value::<ManagedRequestKey>(value).is_err());
    assert_eq!(
        postcard::to_allocvec(&ManagedRequestFamily::Domain).unwrap(),
        [0]
    );
    assert_eq!(
        postcard::to_allocvec(&ManagedRequestFamily::Cursor).unwrap(),
        [1]
    );
    assert_eq!(
        postcard::to_allocvec(&ManagedReceiptResolution::Unknown).unwrap(),
        [2]
    );
}
