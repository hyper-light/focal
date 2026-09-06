#![allow(
    clippy::panic,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::unreachable,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros
)]
use focal_model::*;
use focal_wire::*;
use std::collections::BTreeSet;

fn principal() -> ParticipantId {
    ParticipantId::from_u128(3)
}
fn request() -> RequestEnvelope {
    RequestEnvelope {
        protocol: PROTOCOL_VERSION,
        ledger: LedgerId {
            tenant: TenantId::from_u128(1),
            session: SessionId::from_u128(2),
        },
        route_epoch: RouteEpoch(4),
        request_epoch: RequestEpoch(90),
        request_id: RequestId::from_u128(91),
        operation: Operation::Reconcile(ReconcileQuery::Receipt {
            epoch: RequestEpoch(3),
            request: RequestId::from_u128(6),
        }),
    }
}
fn peer(role: PeerRole) -> AuthenticatedPeer {
    AuthenticatedPeer::local(PeerGrant {
        principal: principal(),
        tenants: BTreeSet::from([request().ledger.tenant]),
        role,
    })
    .unwrap()
}
fn epoch() -> EpochReconciliation {
    EpochReconciliation {
        epoch: RequestEpoch(3),
        minimum: Some(RequestEpoch(2)),
        latest_admitted: Some(RequestEpoch(5)),
        admitted: true,
    }
}
fn reply() -> ReconcileReply {
    let request = request();
    let key = RequestKey {
        principal: principal(),
        epoch: RequestEpoch(3),
        id: RequestId::from_u128(6),
    };
    ReconcileReply {
        applied_index: 20,
        token: ReadToken {
            ledger: request.ledger,
            sequence: SessionSeq(12),
            route_epoch: request.route_epoch,
        },
        page: ReconcilePage {
            schema: RECONCILE_SCHEMA,
            ledger: request.ledger,
            principal: principal(),
            sequence: SessionSeq(12),
            result: ReconcileResult::Receipt {
                key,
                epoch: epoch(),
                resolution: ReceiptResolution::Committed(Box::new(MutationReceipt {
                    ledger: request.ledger,
                    key,
                    sequence: SessionSeq(10),
                    command_hash: ContentHash([8; 32]),
                    outcome: CommandResult::Noop,
                })),
            },
        },
    }
}
fn validate(reply: ReconcileReply) -> Result<(), WireError> {
    let request = request();
    validate_response(
        &request,
        &request.reply(Response::Reconciled(reply)),
        Some(principal()),
        &WireLimits::default(),
    )
}

fn cursor_reply() -> ReconcileReply {
    let mut reply = reply();
    let ReconcileResult::Receipt {
        key, resolution, ..
    } = &mut reply.page.result
    else {
        panic!()
    };
    *resolution = ReceiptResolution::CommittedCursor(Box::new(CursorMutationReceipt {
        ledger: reply.page.ledger,
        key: *key,
        intent_hash: ContentHash([8; 32]),
        revision: 2,
        domain_sequence: SessionSeq(10),
        raft_index: 16,
        floor: SessionSeq(5),
        record: Some(CursorRecordSnapshot {
            token: CursorTokenSnapshot {
                key: CursorConsumerKeySnapshot {
                    ledger: reply.page.ledger,
                    consumer: [9; 16],
                },
                generation: 2,
                scope: ContentHash([10; 32]),
                position: CursorPositionSnapshot {
                    ledger: reply.page.ledger,
                    sequence: SessionSeq(8),
                    offset: CursorPositionOffsetSnapshot::Delta(3),
                },
            },
            filter: CursorFilterSnapshot::Claims(vec![
                ClaimId::from_u128(1),
                ClaimId::from_u128(2),
            ]),
            expires_at: 200,
            mode: CursorModeSnapshot::Live,
        }),
    }));
    reply
}

#[test]
fn cursor_receipts_preserve_metadata_outcomes_and_bind_both_publication_prefixes() {
    let original = cursor_reply();
    validate(original.clone()).unwrap();
    let encoded = encode_payload(&original, 4096).unwrap();
    assert_eq!(
        decode_payload::<ReconcileReply>(&encoded).unwrap(),
        original
    );
    for case in 0..8 {
        let mut bad = cursor_reply();
        let ReconcileResult::Receipt {
            resolution: ReceiptResolution::CommittedCursor(receipt),
            ..
        } = &mut bad.page.result
        else {
            panic!()
        };
        let record = receipt.record.as_mut().unwrap();
        match case {
            0 => receipt.raft_index = 21,
            1 => receipt.domain_sequence = SessionSeq(13),
            2 => record.token.position.sequence = SessionSeq(11), // not merely <= page sequence
            3 => record.token.position.ledger.session = SessionId::from_u128(90),
            4 => receipt.floor = SessionSeq(11),
            5 => {
                record.filter =
                    CursorFilterSnapshot::Claims(vec![ClaimId::from_u128(2), ClaimId::from_u128(2)])
            }
            6 => {
                record.filter =
                    CursorFilterSnapshot::Claims(vec![ClaimId::from_u128(2), ClaimId::from_u128(1)])
            }
            _ => record.mode = CursorModeSnapshot::Protected, // missing the permanent lease sentinel
        }
        assert!(validate(bad).is_err(), "cursor case {case}");
    }
    let mut oversized = cursor_reply();
    let ReconcileResult::Receipt {
        resolution: ReceiptResolution::CommittedCursor(receipt),
        ..
    } = &mut oversized.page.result
    else {
        panic!()
    };
    receipt.record.as_mut().unwrap().filter =
        CursorFilterSnapshot::Claims(vec![
            ClaimId::from_u128(1);
            WireLimits::default().max_items as usize + 1
        ]);
    assert!(matches!(validate(oversized), Err(WireError::Limit)));
    let mut old = cursor_reply();
    let ReconcileResult::Receipt {
        epoch,
        resolution: ReceiptResolution::CommittedCursor(receipt),
        ..
    } = &mut old.page.result
    else {
        panic!()
    };
    epoch.minimum = Some(RequestEpoch(4));
    epoch.admitted = false;
    receipt.record = None; // floor-only metadata commands still have committed outcomes
    validate(old).unwrap();
    // The historical trusted cursor API permits these scalar values. Reading
    // its retained receipt must preserve them, not apply newer actor-ingress
    // restrictions retroactively.
    let mut historical_scalars = cursor_reply();
    let ReconcileResult::Receipt {
        resolution: ReceiptResolution::CommittedCursor(receipt),
        ..
    } = &mut historical_scalars.page.result
    else {
        panic!()
    };
    let record = receipt.record.as_mut().unwrap();
    record.token.key.consumer = [0; 16];
    record.token.scope = ContentHash([0; 32]);
    record.filter = CursorFilterSnapshot::Claims(vec![ClaimId::default(), ClaimId::from_u128(1)]);
    validate(historical_scalars).unwrap();
    let mut zero_prefix = cursor_reply();
    zero_prefix.applied_index = 0;
    assert!(validate(zero_prefix).is_err());
    assert_eq!(
        postcard::to_stdvec(&ReceiptResolution::Unknown).unwrap(),
        vec![2]
    );
    assert_eq!(
        postcard::to_stdvec(&ReceiptResolution::BelowFloor {
            minimum: RequestEpoch(7)
        })
        .unwrap(),
        vec![1, 7]
    );
    let ReconcileResult::Receipt { resolution, .. } = original.page.result else {
        panic!()
    };
    assert_eq!(postcard::to_stdvec(&resolution).unwrap()[0], 3);
}

#[test]
fn reconciliation_is_read_only_own_principal_actor_or_runtime_and_strictly_decoded() {
    for role in [PeerRole::Actor, PeerRole::Runtime] {
        let verified = verify_request(peer(role), request(), &WireLimits::default()).unwrap();
        assert_eq!(verified.peer().principal(), principal());
        assert!(!verified.request().operation.is_mutation());
    }
    for role in [PeerRole::Evaluator, PeerRole::Node { node_id: 1 }] {
        assert_eq!(
            verify_request(peer(role), request(), &WireLimits::default()).unwrap_err(),
            AccessError::Unauthorized
        );
    }
    let mut foreign = request();
    foreign.ledger.tenant = TenantId::from_u128(99);
    assert_eq!(
        verify_request(peer(PeerRole::Runtime), foreign, &WireLimits::default()).unwrap_err(),
        AccessError::Unauthorized
    );
    for query in [
        ReconcileQuery::Epoch {
            epoch: RequestEpoch(0),
        },
        ReconcileQuery::Receipt {
            epoch: RequestEpoch(1),
            request: RequestId::default(),
        },
    ] {
        let mut invalid = request();
        invalid.operation = Operation::Reconcile(query);
        assert_eq!(
            verify_request(peer(PeerRole::Actor), invalid, &WireLimits::default()).unwrap_err(),
            AccessError::InvalidRequest
        );
    }
    let mut forged = serde_json::to_value(request()).unwrap();
    forged["operation"]["Reconcile"]["Receipt"]["principal"] =
        serde_json::to_value(principal()).unwrap();
    assert!(serde_json::from_value::<RequestEnvelope>(forged).is_err());
}

#[test]
fn reconciled_receipts_bind_the_queried_key_principal_route_and_published_prefix() {
    validate(reply()).unwrap();
    let mut bad = reply();
    bad.page.principal = ParticipantId::from_u128(42);
    assert!(validate(bad).is_err());
    let mut bad = reply();
    bad.page.ledger.session = SessionId::from_u128(42);
    assert!(validate(bad).is_err());
    let mut bad = reply();
    bad.page.sequence = SessionSeq(11);
    assert!(validate(bad).is_err());
    let mut bad = reply();
    bad.token.route_epoch = RouteEpoch(5);
    assert!(validate(bad).is_err());
    let mut bad = reply();
    bad.page.schema = 2;
    assert!(validate(bad).is_err());
    let mut bad = reply();
    let ReconcileResult::Receipt { epoch, .. } = &mut bad.page.result else {
        panic!()
    };
    epoch.admitted = false;
    assert!(validate(bad).is_err());
    for edit in 0..6 {
        let mut bad = reply();
        let ReconcileResult::Receipt {
            key,
            epoch,
            resolution,
        } = &mut bad.page.result
        else {
            panic!()
        };
        let ReceiptResolution::Committed(receipt) = resolution else {
            panic!()
        };
        match edit {
            0 => key.id = RequestId::from_u128(70),
            1 => receipt.key.epoch = RequestEpoch(90), // envelope key is not the queried key
            2 => receipt.key.principal = ParticipantId::from_u128(77),
            3 => receipt.sequence = SessionSeq(13),
            4 => epoch.epoch = RequestEpoch(4),
            _ => receipt.sequence = SessionSeq(0),
        }
        assert!(validate(bad).is_err(), "edit {edit}");
    }
    let mut bad = reply();
    let ReconcileResult::Receipt {
        resolution: ReceiptResolution::Committed(receipt),
        ..
    } = &mut bad.page.result
    else {
        panic!()
    };
    receipt.outcome = CommandResult::Generated(vec![
        ClaimId::from_u128(9);
        WireLimits::default().max_items as usize + 1
    ]);
    assert!(matches!(validate(bad), Err(WireError::Limit)));
    let mut envelope = request().reply(Response::Reconciled(reply()));
    envelope.route_epoch = RouteEpoch(5);
    assert!(
        validate_response(
            &request(),
            &envelope,
            Some(principal()),
            &WireLimits::default()
        )
        .is_err()
    );
}

#[test]
fn retained_receipt_hashes_are_opaque_values_not_reserved_zero_sentinels() {
    for mut historical in [reply(), cursor_reply()] {
        let ReconcileResult::Receipt { resolution, .. } = &mut historical.page.result else {
            panic!()
        };
        match resolution {
            ReceiptResolution::Committed(receipt) => receipt.command_hash = ContentHash([0; 32]),
            ReceiptResolution::CommittedCursor(receipt) => {
                receipt.intent_hash = ContentHash([0; 32])
            }
            _ => panic!(),
        }
        // CursorInput and its persisted receipt do not reserve zero; the
        // canonical domain hash likewise returns the full 256-bit hash without
        // a sentinel mapping. Reconciliation has no authored command from which
        // to recompute either value and must preserve the retained bytes.
        let encoded = encode_payload(&historical, 4096).unwrap();
        let decoded = decode_payload::<ReconcileReply>(&encoded).unwrap();
        assert_eq!(decoded, historical);
        validate(decoded.clone()).unwrap();
        let mut wrong_key = decoded;
        let ReconcileResult::Receipt { key, .. } = &mut wrong_key.page.result else {
            panic!()
        };
        key.id = RequestId::from_u128(700);
        assert!(validate(wrong_key).is_err());
    }
}

#[test]
fn retained_below_floor_receipt_is_valid_but_missing_history_and_unknown_stay_distinct() {
    let mut historical = reply();
    let ReconcileResult::Receipt { epoch: status, .. } = &mut historical.page.result else {
        panic!()
    };
    status.minimum = Some(RequestEpoch(4));
    status.admitted = false;
    validate(historical.clone()).unwrap(); // actual retained receipt wins
    let ReconcileResult::Receipt { resolution, .. } = &mut historical.page.result else {
        panic!()
    };
    *resolution = ReceiptResolution::BelowFloor {
        minimum: RequestEpoch(4),
    };
    validate(historical.clone()).unwrap();
    let ReconcileResult::Receipt { resolution, .. } = &mut historical.page.result else {
        panic!()
    };
    *resolution = ReceiptResolution::Unknown;
    assert!(validate(historical).is_err());
    let mut unknown = reply();
    let ReconcileResult::Receipt { resolution, .. } = &mut unknown.page.result else {
        panic!()
    };
    *resolution = ReceiptResolution::Unknown;
    validate(unknown).unwrap();
    let request = RequestEnvelope {
        operation: Operation::Reconcile(ReconcileQuery::Epoch {
            epoch: RequestEpoch(3),
        }),
        ..request()
    };
    let mut page = reply();
    page.page.result = ReconcileResult::Epoch(epoch());
    validate_response(
        &request,
        &request.reply(Response::Reconciled(page.clone())),
        Some(principal()),
        &WireLimits::default(),
    )
    .unwrap();
    let ReconcileResult::Epoch(status) = &mut page.page.result else {
        panic!()
    };
    status.minimum = None;
    assert!(
        validate_response(
            &request,
            &request.reply(Response::Reconciled(page)),
            Some(principal()),
            &WireLimits::default()
        )
        .is_err()
    );
}

#[test]
fn reconciliation_appends_frozen_ordinals_and_round_trips_without_mutating_request_identity() {
    assert_eq!(
        postcard::to_stdvec(&Operation::OpenEpoch {
            epoch: RequestEpoch(9)
        })
        .unwrap(),
        vec![4, 9]
    );
    assert_eq!(
        postcard::to_stdvec(&Response::PeerAccepted).unwrap(),
        vec![3]
    );
    let query = Operation::Reconcile(ReconcileQuery::Epoch {
        epoch: RequestEpoch(7),
    });
    assert_eq!(query.registered_tag(), 15);
    assert_eq!(postcard::to_stdvec(&query).unwrap(), vec![14, 0, 7]);
    assert_eq!(
        postcard::from_bytes::<Operation>(&[4, 9]).unwrap(),
        Operation::OpenEpoch {
            epoch: RequestEpoch(9)
        }
    );
    let bytes = encode_payload(&request(), 4096).unwrap();
    assert_eq!(
        decode_payload::<RequestEnvelope>(&bytes).unwrap(),
        request()
    );
    let response = request().reply(Response::Reconciled(reply()));
    let encoded = encode_payload(&response, 4096).unwrap();
    assert_eq!(
        decode_payload::<ResponseEnvelope>(&encoded).unwrap(),
        response
    );
    assert_eq!(postcard::to_stdvec(&response.result).unwrap()[0], 11);
    let mut trailing = encoded;
    trailing.push(0);
    assert!(decode_payload::<ResponseEnvelope>(&trailing).is_err());
}
