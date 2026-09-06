use super::*;
fn envelope(operation: Operation) -> RequestEnvelope {
    let mut value = super::request(100);
    value.operation = operation;
    value
}
fn key() -> ManagedRequestKey {
    ManagedRequestKey {
        stream: RequestStreamIdentity {
            cluster: [7; 16],
            ledger: ledger(),
            principal: actor().principal(),
            slot: 0,
            generation: 1,
        },
        ordinal: 2,
        id: RequestId::from_u128(111),
    }
}
fn actor() -> AuthenticatedPeer {
    AuthenticatedPeer::local(PeerGrant {
        principal: ParticipantId::from_u128(9),
        tenants: [ledger().tenant].into_iter().collect(),
        role: PeerRole::Actor,
    })
    .unwrap()
}
fn mutation() -> RequestEnvelope {
    let mut request = envelope(Operation::Managed {
        key: key(),
        operation: ManagedOperation::Submit {
            expected_revision: None,
            command: Command::PostClaim {
                claim: ClaimId::from_u128(1),
            },
        },
    });
    request.protocol = MANAGED_PROTOCOL_VERSION;
    request.request_id = key().id;
    request
}
#[test]
fn managed_identity_authentication_and_negative_fences_bind_full_request() {
    let limits = WireLimits::default();
    let request = mutation();
    let (identity, family, intent) = managed_request_identity(&request).unwrap();
    assert_eq!(identity, key());
    assert_eq!(family, ManagedRequestFamily::Domain);
    verify_request(actor(), request.clone(), &limits).unwrap();
    let mut foreign = request.clone();
    if let Operation::Managed { key, .. } = &mut foreign.operation {
        key.stream.principal = ParticipantId::from_u128(12);
    }
    assert!(matches!(
        verify_request(actor(), foreign, &limits),
        Err(AccessError::Unauthorized)
    ));
    for change in 0..4 {
        let mut malformed = request.clone();
        match change {
            0 => malformed.protocol = PROTOCOL_VERSION,
            1 => malformed.request_epoch = RequestEpoch(2),
            2 => malformed.request_id = RequestId::from_u128(1),
            _ => malformed.ledger.session = SessionId::from_u128(9),
        };
        assert!(managed_request_identity(&malformed).is_err());
    }
    for (error, valid) in [
        (AccessError::ManagedRetired { through: 1 }, false),
        (AccessError::ManagedRetired { through: 2 }, true),
        (AccessError::ManagedClosed { generation: 0 }, false),
        (AccessError::ManagedClosed { generation: 1 }, true),
    ] {
        let response = request.reply(Response::Error(error));
        assert_eq!(
            validate_response(&request, &response, Some(actor().principal()), &limits).is_ok(),
            valid
        );
    }
    let receipt = ManagedReceipt {
        key: identity,
        sequence: SessionSeq(0),
        raft_index: 4,
        intent_hash: intent,
        outcome: ManagedReceiptOutcome::Sealed { family },
    };
    validate_managed_receipt(&receipt, &identity, family, intent, &limits).unwrap();
    let mut wrong = receipt.clone();
    wrong.key.ordinal += 1;
    assert!(validate_managed_receipt(&wrong, &identity, family, intent, &limits).is_err());
    let raw = envelope(Operation::Read(ReadRequest {
        consistency: ReadConsistency::Linearizable,
        query: ReadQuery::Scan { after: None },
        max_items: 1,
    }));
    assert!(
        validate_response(
            &raw,
            &raw.reply(Response::Error(AccessError::ManagedClosed {
                generation: 1
            })),
            None,
            &limits
        )
        .is_err()
    );
}
#[test]
fn managed_cursor_hash_matches_existing_bytes_and_control_receipt_is_exact() {
    let limits = WireLimits::default();
    let key = key();
    let stream = StreamRequest::Open {
        consumer: ConsumerId([1; 16]),
        filter: DeltaFilter::All,
        start: None,
        seed: false,
        credits: Credits {
            items: 1,
            bytes: 512,
        },
    };
    let old = postcard::to_stdvec(&(ledger(), actor().principal(), &stream)).unwrap();
    assert_eq!(
        cursor_request_intent(ledger(), actor().principal(), &stream).unwrap(),
        ContentHash(blake3::derive_key("focal.stream.intent.v1", &old))
    );
    assert_eq!(
        managed_intent(&key, &ManagedOperation::Cursor(stream))
            .unwrap()
            .1,
        ManagedRequestFamily::Cursor
    );
    let command = RequestStreamCommand::Register {
        slot: 0,
        expected_generation: 0,
        owner: RequestId([2; 16]),
        window: 4,
    };
    let mut request = envelope(Operation::RequestStreamControl {
        cluster: [7; 16],
        command: command.clone(),
    });
    request.protocol = MANAGED_PROTOCOL_VERSION;
    let receipt = RequestStreamControlReceipt {
        cluster: [7; 16],
        ledger: ledger(),
        principal: actor().principal(),
        id: request.request_id,
        intent_hash: request_stream_control_hash([7; 16], ledger(), actor().principal(), &command)
            .unwrap(),
        raft_index: 7,
        outcome: RequestStreamControlOutcome::Registered(RequestStreamState::Active {
            stream: key.stream,
            owner: RequestId([2; 16]),
            revision: 1,
            window: 4,
            acknowledged_through: 0,
        }),
    };
    let reply = RequestStreamControlReply {
        token: ReadToken {
            ledger: ledger(),
            sequence: SessionSeq(0),
            route_epoch: request.route_epoch,
        },
        receipt,
    };
    validate_response(
        &request,
        &request.reply(Response::RequestStreamControlled(reply.clone())),
        Some(actor().principal()),
        &limits,
    )
    .unwrap();
    let mut changed = reply;
    changed.receipt.intent_hash = ContentHash([1; 32]);
    assert!(
        validate_response(
            &request,
            &request.reply(Response::RequestStreamControlled(changed)),
            Some(actor().principal()),
            &limits
        )
        .is_err()
    );
    let mut legacy = request.clone();
    legacy.protocol = PROTOCOL_VERSION;
    assert!(matches!(
        verify_request(actor(), legacy, &limits),
        Err(AccessError::UnsupportedProtocol)
    ));
}
