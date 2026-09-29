use super::*;
use std::collections::BTreeSet;

fn ledger() -> LedgerId {
    LedgerId {
        tenant: TenantId::from_u128(7),
        session: SessionId::from_u128(8),
    }
}
fn key() -> RequestKey {
    RequestKey {
        principal: ParticipantId::from_u128(11),
        epoch: RequestEpoch(3),
        id: RequestId::from_u128(12),
    }
}
/// A request frame header exactly as the input format lays it out (21 §3).
fn frame(profile: u8, namespace: u8, ledger: LedgerId, key: RequestKey, command: u8) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(96);
    bytes.extend_from_slice(&NATIVE_FRAME_MAGIC);
    bytes.extend_from_slice(&NATIVE_FRAME_VERSION.to_le_bytes());
    bytes.push(profile);
    bytes.push(namespace);
    bytes.extend_from_slice(&ledger.tenant.0);
    bytes.extend_from_slice(&ledger.session.0);
    bytes.extend_from_slice(&key.principal.0);
    bytes.extend_from_slice(&key.epoch.0.to_le_bytes());
    bytes.extend_from_slice(&key.id.0);
    bytes.push(command);
    bytes.extend_from_slice(&[0xAB; 11]);
    bytes
}
fn peer(role: PeerRole, principal: ParticipantId) -> AuthenticatedPeer {
    AuthenticatedPeer::local(PeerGrant {
        principal,
        tenants: BTreeSet::from([ledger().tenant]),
        role,
    })
    .unwrap()
}
fn envelope(protocol: u16, operation: Operation) -> RequestEnvelope {
    RequestEnvelope {
        protocol,
        ledger: ledger(),
        route_epoch: RouteEpoch(1),
        request_epoch: key().epoch,
        request_id: key().id,
        operation,
    }
}
fn read_request() -> NativeReadRequest {
    NativeReadRequest {
        consistency: ReadConsistency::Linearizable,
        query: NativeReadQuery::Claim {
            id: ClaimId::from_u128(5),
            expand: NativeClaimExpand::default(),
        },
        max_items: 8,
    }
}
fn list_request() -> NativeListRequest {
    NativeListRequest {
        filter: NativeListFilter::Claims {
            issuer: Some(ParticipantId::from_u128(11)),
            subject: None,
            status: None,
            action: None,
            scope: None,
            relation: None,
            created_after: None,
        },
        cursor: None,
        max_items: 16,
        max_visits: 64,
    }
}

#[test]
fn fixed_header_inspection_reads_exactly_the_registered_layout() {
    let header = inspect_native_frame(&frame(1, 0, ledger(), key(), 27)).unwrap();
    assert_eq!(
        header,
        NativeFrameHeader {
            profile: NativeProfile::AuthoredV1,
            namespace: 0,
            ledger: ledger(),
            key: key(),
            command: 27,
        }
    );
    // Timer namespaces parse; admissibility refuses them.
    assert_eq!(
        inspect_native_frame(&frame(0, 2, ledger(), key(), 0))
            .unwrap()
            .namespace,
        2
    );
    let mut wrong_magic = frame(0, 0, ledger(), key(), 0);
    wrong_magic[0] = b'X';
    let mut wrong_version = frame(0, 0, ledger(), key(), 0);
    wrong_version[8] = 2;
    let mut wrong_profile = frame(0, 0, ledger(), key(), 0);
    wrong_profile[10] = 2;
    let short = frame(0, 0, ledger(), key(), 0)[..NATIVE_ACTOR_HEADER_BYTES].to_vec();
    for bytes in [
        wrong_magic,
        wrong_version,
        wrong_profile,
        short,
        frame(0, 0, ledger(), key(), NATIVE_COMMAND_TAGS),
        Vec::new(),
    ] {
        assert!(matches!(
            inspect_native_frame(&bytes),
            Err(AccessError::InvalidRequest)
        ));
    }
}

#[test]
fn admissibility_binds_the_frame_to_the_authenticated_envelope() {
    let actor = peer(PeerRole::Actor, key().principal);
    let request = envelope(NATIVE_PROTOCOL_VERSION, Operation::Summary);
    let bytes = frame(0, 0, ledger(), key(), 5);
    assert_eq!(
        native_frame_admissible(&bytes, &actor, &request)
            .unwrap()
            .command,
        5
    );
    // Evaluator and runtime peers submit their own frames; nodes never do.
    for role in [PeerRole::Evaluator, PeerRole::Runtime] {
        assert!(native_frame_admissible(&bytes, &peer(role, key().principal), &request).is_ok());
    }
    assert!(matches!(
        native_frame_admissible(
            &bytes,
            &peer(PeerRole::Node { node_id: 4 }, key().principal),
            &request
        ),
        Err(AccessError::Unauthorized)
    ));
    // Another principal, another ledger or a timer namespace is unauthorized.
    assert!(matches!(
        native_frame_admissible(
            &bytes,
            &peer(PeerRole::Actor, ParticipantId::from_u128(99)),
            &request
        ),
        Err(AccessError::Unauthorized)
    ));
    let other_ledger = LedgerId {
        tenant: ledger().tenant,
        session: SessionId::from_u128(80),
    };
    assert!(matches!(
        native_frame_admissible(&frame(0, 0, other_ledger, key(), 5), &actor, &request),
        Err(AccessError::Unauthorized)
    ));
    for namespace in 1..=3 {
        assert!(matches!(
            native_frame_admissible(&frame(0, namespace, ledger(), key(), 5), &actor, &request),
            Err(AccessError::Unauthorized)
        ));
    }
    // A request identity that is not the envelope's is malformed, not unauthorized.
    let mut other_epoch = request.clone();
    other_epoch.request_epoch = RequestEpoch(4);
    assert!(matches!(
        native_frame_admissible(&bytes, &actor, &other_epoch),
        Err(AccessError::InvalidRequest)
    ));
    let mut other_id = request.clone();
    other_id.request_id = RequestId::from_u128(13);
    assert!(matches!(
        native_frame_admissible(&bytes, &actor, &other_id),
        Err(AccessError::InvalidRequest)
    ));
    // A zero epoch is malformed even when the envelope agrees with the frame.
    let mut zero_envelope = request.clone();
    zero_envelope.request_epoch = RequestEpoch(0);
    assert!(matches!(
        native_frame_admissible(
            &frame(
                0,
                0,
                ledger(),
                RequestKey {
                    epoch: RequestEpoch(0),
                    ..key()
                },
                5
            ),
            &actor,
            &zero_envelope
        ),
        Err(AccessError::InvalidRequest)
    ));
}

#[test]
fn native_operations_carry_registered_tags_actor_capability_and_mutation_class() {
    let native = Operation::Native {
        frame: vec![1, 2, 3],
    };
    let read = Operation::NativeRead(read_request());
    let list = Operation::NativeList(list_request());
    assert_eq!(native.registered_tag(), 25);
    assert_eq!(read.registered_tag(), 26);
    assert_eq!(list.registered_tag(), 27);
    assert!(native.is_mutation());
    assert!(!read.is_mutation());
    assert!(!list.is_mutation());
    for operation in [&native, &read, &list] {
        assert_eq!(capability(operation), Capability::Actor);
        assert!(is_native_operation(operation));
        assert!(native_profile_operation(operation));
        assert_eq!(participant_protocol(operation), NATIVE_PROTOCOL_VERSION);
    }
    // Every registered operation kind has a distinct tag and name.
    let tags: BTreeSet<u8> = NativeOperationKind::ALL
        .iter()
        .map(|kind| kind.registered_tag())
        .collect();
    let names: BTreeSet<&str> = NativeOperationKind::ALL
        .iter()
        .map(|kind| kind.name())
        .collect();
    assert_eq!(tags.len(), NativeOperationKind::ALL.len());
    assert_eq!(names.len(), NativeOperationKind::ALL.len());
    assert_eq!(tags.iter().max(), Some(&31));
    assert_eq!(
        NativeOperationKind::ALL
            .iter()
            .filter(|kind| !kind.participant_authored())
            .count(),
        5
    );
    assert_eq!(
        NativeProfile::from_registered(1),
        Some(NativeProfile::AuthoredV1)
    );
    assert_eq!(NativeProfile::AuthoredV1.registered_tag(), 1);
    assert_eq!(NativeProfile::from_registered(2), None);
}

#[test]
fn the_native_profile_is_required_for_native_operations_and_admits_a_closed_set() {
    let actor = peer(PeerRole::Actor, key().principal);
    let limits = WireLimits::default();
    let native = || Operation::Native {
        frame: frame(0, 0, ledger(), key(), 5),
    };
    for protocol in [
        PROTOCOL_VERSION,
        MANAGED_PROTOCOL_VERSION,
        PEER_PROTOCOL_VERSION,
    ] {
        for operation in [
            native(),
            Operation::NativeRead(read_request()),
            Operation::NativeList(list_request()),
        ] {
            assert!(matches!(
                verify_request(actor.clone(), envelope(protocol, operation), &limits),
                Err(AccessError::UnsupportedProtocol)
            ));
        }
    }
    for operation in [
        native(),
        Operation::NativeRead(read_request()),
        Operation::NativeList(list_request()),
        Operation::Summary,
        Operation::Reconcile(ReconcileQuery::Epoch {
            epoch: RequestEpoch(3),
        }),
        Operation::Download {
            content: ContentRef {
                domain: ContentDomainId::from_u128(1),
                root: ContentHash([1; 32]),
                length: 1,
                class: ContentClass::Document,
            },
            offset: 0,
            max_bytes: 16,
        },
    ] {
        assert!(
            verify_request(
                actor.clone(),
                envelope(NATIVE_PROTOCOL_VERSION, operation),
                &limits
            )
            .is_ok()
        );
    }
    // Legacy typed submissions and legacy reads never ride the native profile.
    for operation in [
        Operation::OpenEpoch {
            epoch: RequestEpoch(3),
        },
        Operation::Read(ReadRequest {
            consistency: ReadConsistency::Linearizable,
            query: ReadQuery::Objects(vec![]),
            max_items: 1,
        }),
        Operation::Monitor {
            id: MonitorId::from_u128(1),
        },
    ] {
        assert!(matches!(
            verify_request(
                actor.clone(),
                envelope(NATIVE_PROTOCOL_VERSION, operation),
                &limits
            ),
            Err(AccessError::UnsupportedProtocol)
        ));
    }
    // Node peers hold no actor capability for native operations.
    assert!(matches!(
        verify_request(
            peer(PeerRole::Node { node_id: 1 }, key().principal),
            envelope(NATIVE_PROTOCOL_VERSION, native()),
            &limits
        ),
        Err(AccessError::Unauthorized)
    ));
}

#[test]
fn negotiation_offers_the_native_profile_only_with_managed_participant_and_native_support() {
    let limits = WireLimits::default();
    let hello = Hello {
        versions: vec![
            NATIVE_PROTOCOL_VERSION,
            PEER_PROTOCOL_VERSION,
            MANAGED_PROTOCOL_VERSION,
            PROTOCOL_VERSION,
        ],
        max_frame_bytes: 1 << 20,
        max_items: 64,
    };
    assert_eq!(
        limits
            .negotiate_native(&hello, true, true, true)
            .unwrap()
            .protocol,
        NATIVE_PROTOCOL_VERSION
    );
    for (managed, participant, native, expected) in [
        (true, true, false, PEER_PROTOCOL_VERSION),
        (true, false, true, MANAGED_PROTOCOL_VERSION),
        (false, true, true, PROTOCOL_VERSION),
    ] {
        assert_eq!(
            limits
                .negotiate_native(&hello, managed, participant, native)
                .unwrap()
                .protocol,
            expected,
            "{managed} {participant} {native}"
        );
    }
    assert_eq!(
        limits
            .negotiate_profiles(&hello, true, true)
            .unwrap()
            .protocol,
        PEER_PROTOCOL_VERSION
    );
    let negotiated = Negotiated {
        protocol: NATIVE_PROTOCOL_VERSION,
        max_frame_bytes: 1024,
        max_items: 1,
    };
    for requested in [
        PROTOCOL_VERSION,
        MANAGED_PROTOCOL_VERSION,
        PEER_PROTOCOL_VERSION,
        NATIVE_PROTOCOL_VERSION,
    ] {
        assert!(negotiated.accepts_protocol(requested));
    }
    assert!(!negotiated.accepts_protocol(5));
    let peer_only = Negotiated {
        protocol: PEER_PROTOCOL_VERSION,
        ..negotiated
    };
    assert!(!peer_only.accepts_protocol(NATIVE_PROTOCOL_VERSION));
}

#[test]
fn read_and_list_requests_validate_bounds_identities_and_cursors() {
    let limits = WireLimits {
        max_items: 16,
        ..WireLimits::default()
    };
    assert!(read_request().validate(&limits).is_ok());
    let too_many = NativeReadRequest {
        max_items: 17,
        ..read_request()
    };
    let zero = NativeReadRequest {
        max_items: 0,
        ..read_request()
    };
    let empty = NativeReadRequest {
        query: NativeReadQuery::Objects(vec![]),
        ..read_request()
    };
    let zero_claim = NativeReadRequest {
        query: NativeReadQuery::Claim {
            id: ClaimId::from_u128(0),
            expand: NativeClaimExpand::default(),
        },
        ..read_request()
    };
    let events = NativeReadRequest {
        query: NativeReadQuery::Events {
            after: None,
            limit: 9,
        },
        ..read_request()
    };
    for bad in [too_many, zero, empty, zero_claim, events] {
        assert!(matches!(
            bad.validate(&limits),
            Err(AccessError::InvalidRequest)
        ));
    }
    assert!(
        NativeReadRequest {
            query: NativeReadQuery::Objects(vec![
                NativeObjectRef::Claim(ClaimId::from_u128(1)),
                NativeObjectRef::Outcome(NativeInvocationRef::Import),
            ]),
            ..read_request()
        }
        .validate(&limits)
        .is_ok()
    );
    assert!(list_request().validate(&limits).is_ok());
    let visits_below_items = NativeListRequest {
        max_visits: 15,
        ..list_request()
    };
    let visits_unbounded = NativeListRequest {
        max_visits: MAX_NATIVE_LIST_VISITS + 1,
        ..list_request()
    };
    let long_cursor = NativeListRequest {
        cursor: Some(NativeListCursor(vec![0; MAX_NATIVE_LIST_CURSOR_BYTES + 1])),
        ..list_request()
    };
    let empty_cursor = NativeListRequest {
        cursor: Some(NativeListCursor(vec![])),
        ..list_request()
    };
    let zero_holder = NativeListRequest {
        filter: NativeListFilter::Receipts {
            holder: Some(ParticipantId::from_u128(0)),
            claim: None,
        },
        ..list_request()
    };
    let empty_kind = NativeListRequest {
        filter: NativeListFilter::Artifacts {
            producer: None,
            kind: Some(String::new()),
            schema: None,
            input: None,
        },
        ..list_request()
    };
    for bad in [
        visits_below_items,
        visits_unbounded,
        long_cursor,
        empty_cursor,
        zero_holder,
        empty_kind,
    ] {
        assert!(matches!(
            bad.validate(&limits),
            Err(AccessError::InvalidRequest)
        ));
    }
}

#[test]
fn native_envelopes_and_replies_round_trip_with_frozen_bytes() {
    let native = envelope(
        NATIVE_PROTOCOL_VERSION,
        Operation::Native {
            frame: frame(1, 0, ledger(), key(), 27),
        },
    );
    let read = envelope(
        NATIVE_PROTOCOL_VERSION,
        Operation::NativeRead(read_request()),
    );
    let list = envelope(
        NATIVE_PROTOCOL_VERSION,
        Operation::NativeList(list_request()),
    );
    let limit = WireLimits::default().max_frame_bytes;
    let mut hashes = Vec::new();
    for request in [&native, &read, &list] {
        let bytes = encode_payload(request, limit).unwrap();
        assert_eq!(decode_payload::<RequestEnvelope>(&bytes).unwrap(), *request);
        hashes.push(blake3::hash(&bytes).to_hex().to_string());
    }
    // Registered encodings of the three native operations; a change here is a
    // wire format change and must be recorded in doc 19.
    assert_eq!(
        hashes,
        [
            "48dd17b32e540837c507bb4d6c5bfdbdad5f1344bc0714fd84652a3ed8b25084",
            "a7f793ac79ad32d46e33f933d89058e11fdc1fc0bdf548bfce14b62cabb1c57f",
            "551ba3a5622360547dd0e1d999472cb23d777bad70d203433a22365f03c0693a",
        ]
    );
    let receipt = NativeReceipt {
        invocation: NativeInvocationRef::Request(key()),
        sequence: SessionSeq(9),
        logical_time: 1_700_000_000_000,
        operation: NativeOperationKind::CloseResponse,
        intent: ContentHash([3; 32]),
        counts: NativeOutcomeCounts {
            created: 1,
            changed: 2,
            definitions: 0,
            evaluations: 1,
            artifacts: 2,
            results: 0,
            receipts: 0,
            responses: 1,
            result_testaments: 0,
            events: 4,
        },
    };
    let replies = [
        Response::Native(NativeMutationReply::Committed(receipt)),
        Response::Native(NativeMutationReply::Pending(NativeTicket {
            key: key(),
            intent: ContentHash([3; 32]),
        })),
        Response::Native(NativeMutationReply::Refused(NativeRefusal {
            kind: NativeRefusalKind::Stale {
                binding: NativeBinding {
                    object: ObjectId::from_u128(5),
                    content: ContentHash([5; 32]),
                    revision: ObjectRevision(2),
                },
            },
            detail: "claim revision moved".into(),
        })),
        Response::NativeRead(NativeReadPage {
            token: ReadToken {
                ledger: ledger(),
                sequence: SessionSeq(9),
                route_epoch: RouteEpoch(1),
            },
            native_sequence: SessionSeq(9),
            logical_time: 5,
            objects: vec![
                NativeObject::Missing(NativeObjectRef::Claim(ClaimId::from_u128(5))),
                NativeObject::Receipt(NativeReceiptRecord {
                    claim: ClaimId::from_u128(5),
                    fence: ReceiptFence {
                        receipt: ReceiptId::from_u128(6),
                        epoch: 1,
                    },
                    holder: key().principal,
                    acquired: SessionSeq(4),
                }),
                NativeObject::Standing(NativeStanding {
                    principal: key().principal,
                    role: NativePeerRole::Actor,
                    profile: NativeProfile::AuthoredV1,
                    native_sequence: SessionSeq(9),
                    logical_time: 5,
                }),
            ],
            next: Some(NativeContinuation::Events {
                sequence: SessionSeq(9),
                ordinal: 3,
            }),
            visited: 3,
        }),
        Response::NativeListed(NativeListPage {
            token: ReadToken {
                ledger: ledger(),
                sequence: SessionSeq(9),
                route_epoch: RouteEpoch(1),
            },
            native_sequence: SessionSeq(9),
            objects: vec![],
            next: Some(NativeListCursor(vec![7; 40])),
            visited: 64,
        }),
    ];
    for reply in replies {
        let envelope = native.reply(reply);
        let bytes = encode_payload(&envelope, limit).unwrap();
        assert_eq!(
            decode_payload::<ResponseEnvelope>(&bytes).unwrap(),
            envelope
        );
    }
}

#[test]
fn native_replies_are_validated_against_the_request_identity_and_bounds() {
    let limits = WireLimits::default();
    let principal = Some(key().principal);
    let native = envelope(
        NATIVE_PROTOCOL_VERSION,
        Operation::Native {
            frame: frame(0, 0, ledger(), key(), 5),
        },
    );
    let receipt = |invocation| NativeReceipt {
        invocation,
        sequence: SessionSeq(4),
        logical_time: 9,
        operation: NativeOperationKind::AcquireReceipt,
        intent: ContentHash([3; 32]),
        counts: NativeOutcomeCounts {
            created: 0,
            changed: 1,
            definitions: 0,
            evaluations: 0,
            artifacts: 0,
            results: 0,
            receipts: 1,
            responses: 0,
            result_testaments: 0,
            events: 1,
        },
    };
    let committed = native.reply(Response::Native(NativeMutationReply::Committed(receipt(
        NativeInvocationRef::Request(key()),
    ))));
    assert!(validate_response(&native, &committed, principal, &limits).is_ok());
    let other_key = RequestKey {
        id: RequestId::from_u128(99),
        ..key()
    };
    let foreign = native.reply(Response::Native(NativeMutationReply::Committed(receipt(
        NativeInvocationRef::Request(other_key),
    ))));
    let timer = native.reply(Response::Native(NativeMutationReply::Committed(receipt(
        NativeInvocationRef::Import,
    ))));
    let mut zero = receipt(NativeInvocationRef::Request(key()));
    zero.sequence = SessionSeq(0);
    let unsequenced = native.reply(Response::Native(NativeMutationReply::Committed(zero)));
    let other_principal = native.reply(Response::Native(NativeMutationReply::Pending(
        NativeTicket {
            key: RequestKey {
                principal: ParticipantId::from_u128(42),
                ..key()
            },
            intent: ContentHash([1; 32]),
        },
    )));
    let mismatched_kind = native.reply(Response::NativeRead(NativeReadPage {
        token: ReadToken {
            ledger: ledger(),
            sequence: SessionSeq(4),
            route_epoch: RouteEpoch(1),
        },
        native_sequence: SessionSeq(4),
        logical_time: 0,
        objects: vec![],
        next: None,
        visited: 0,
    }));
    for bad in [
        foreign,
        timer,
        unsequenced,
        other_principal,
        mismatched_kind,
    ] {
        assert!(validate_response(&native, &bad, principal, &limits).is_err());
    }
    let read = envelope(
        NATIVE_PROTOCOL_VERSION,
        Operation::NativeRead(read_request()),
    );
    let page = |sequence: u64, objects: Vec<NativeObject>, visited: u32| {
        read.reply(Response::NativeRead(NativeReadPage {
            token: ReadToken {
                ledger: ledger(),
                sequence: SessionSeq(sequence),
                route_epoch: RouteEpoch(1),
            },
            native_sequence: SessionSeq(4),
            logical_time: 0,
            objects,
            next: None,
            visited,
        }))
    };
    let standing = NativeObject::Standing(NativeStanding {
        principal: key().principal,
        role: NativePeerRole::Actor,
        profile: NativeProfile::AuthoredV1,
        native_sequence: SessionSeq(4),
        logical_time: 0,
    });
    assert!(
        validate_response(
            &read,
            &page(4, vec![standing.clone()], 1),
            principal,
            &limits
        )
        .is_ok()
    );
    // A token that disagrees with the page prefix, more objects than visits, or
    // more objects than requested is not a valid page.
    assert!(
        validate_response(
            &read,
            &page(3, vec![standing.clone()], 1),
            principal,
            &limits
        )
        .is_err()
    );
    assert!(
        validate_response(
            &read,
            &page(4, vec![standing.clone()], 0),
            principal,
            &limits
        )
        .is_err()
    );
    assert!(
        validate_response(
            &read,
            &page(4, vec![standing.clone(); 9], 9),
            principal,
            &limits
        )
        .is_err()
    );
    // Single-object queries answer with exactly one object.
    let outcome = envelope(
        NATIVE_PROTOCOL_VERSION,
        Operation::NativeRead(NativeReadRequest {
            query: NativeReadQuery::Standing,
            ..read_request()
        }),
    );
    let two = outcome.reply(Response::NativeRead(NativeReadPage {
        token: ReadToken {
            ledger: ledger(),
            sequence: SessionSeq(4),
            route_epoch: RouteEpoch(1),
        },
        native_sequence: SessionSeq(4),
        logical_time: 0,
        objects: vec![standing.clone(), standing],
        next: None,
        visited: 2,
    }));
    assert!(validate_response(&outcome, &two, principal, &limits).is_err());
    let list = envelope(
        NATIVE_PROTOCOL_VERSION,
        Operation::NativeList(list_request()),
    );
    let listed = |visited: u32, next: Option<NativeListCursor>| {
        list.reply(Response::NativeListed(NativeListPage {
            token: ReadToken {
                ledger: ledger(),
                sequence: SessionSeq(4),
                route_epoch: RouteEpoch(1),
            },
            native_sequence: SessionSeq(4),
            objects: vec![],
            next,
            visited,
        }))
    };
    assert!(
        validate_response(
            &list,
            &listed(3, Some(NativeListCursor(vec![1; 8]))),
            principal,
            &limits
        )
        .is_ok()
    );
    assert!(validate_response(&list, &listed(65, None), principal, &limits).is_err());
    assert!(
        validate_response(
            &list,
            &listed(0, Some(NativeListCursor(vec![1; 8]))),
            principal,
            &limits
        )
        .is_err()
    );
    assert!(
        validate_response(
            &list,
            &listed(3, Some(NativeListCursor(vec![]))),
            principal,
            &limits
        )
        .is_err()
    );
}

#[cfg(unix)]
struct NativeEcho {
    native: bool,
}
#[cfg(unix)]
impl RequestHandler for NativeEcho {
    fn supports_managed_requests(&self) -> bool {
        true
    }
    fn supports_participant_requests(&self) -> bool {
        true
    }
    fn supports_native_requests(&self) -> bool {
        self.native
    }
    fn handle<'a>(&'a self, request: &'a VerifiedRequest) -> HandlerFuture<'a> {
        Box::pin(async move {
            let envelope = request.request();
            let result = match &envelope.operation {
                Operation::Native { frame } => {
                    match native_frame_admissible(frame, request.peer(), envelope) {
                        Ok(header) => {
                            Response::Native(NativeMutationReply::Refused(NativeRefusal {
                                kind: NativeRefusalKind::Refused(
                                    NativeErrorCode::InvalidTransition,
                                ),
                                detail: format!("command {}", header.command),
                            }))
                        }
                        Err(error) => Response::Error(error),
                    }
                }
                Operation::NativeRead(_) => Response::NativeRead(NativeReadPage {
                    token: ReadToken {
                        ledger: envelope.ledger,
                        sequence: SessionSeq(2),
                        route_epoch: envelope.route_epoch,
                    },
                    native_sequence: SessionSeq(2),
                    logical_time: 7,
                    objects: vec![NativeObject::Missing(NativeObjectRef::Claim(
                        ClaimId::from_u128(5),
                    ))],
                    next: None,
                    visited: 1,
                }),
                _ => Response::Error(AccessError::UnsupportedOperation),
            };
            envelope.reply(result)
        })
    }
}

#[cfg(unix)]
#[tokio::test]
async fn the_native_profile_negotiates_over_the_local_socket_only_where_the_handler_admits_it() {
    let root = tempfile::tempdir().unwrap();
    let limits = WireLimits::default();
    let grant = PeerGrant {
        principal: key().principal,
        tenants: BTreeSet::from([ledger().tenant]),
        role: PeerRole::Actor,
    };
    for native in [true, false] {
        let socket = root.path().join(format!("native-{native}.sock"));
        let server =
            std::sync::Arc::new(UnixServer::bind(&socket, grant.clone(), limits.clone()).unwrap());
        let serving = server.clone();
        let handler: std::sync::Arc<dyn RequestHandler> =
            std::sync::Arc::new(NativeEcho { native });
        let task = tokio::spawn(async move { serving.serve(handler).await });
        let remote = UnixRemote::new(&socket, limits.clone()).unwrap();
        let submit = envelope(
            NATIVE_PROTOCOL_VERSION,
            Operation::Native {
                frame: frame(0, 0, ledger(), key(), 5),
            },
        );
        let read = envelope(
            NATIVE_PROTOCOL_VERSION,
            Operation::NativeRead(read_request()),
        );
        let legacy = envelope(PROTOCOL_VERSION, Operation::Summary);
        if native {
            let reply = remote.request(&submit).await.unwrap();
            assert!(matches!(
                reply.result,
                Response::Native(NativeMutationReply::Refused(NativeRefusal {
                    kind: NativeRefusalKind::Refused(NativeErrorCode::InvalidTransition),
                    ref detail,
                })) if detail == "command 5"
            ));
            let page = remote.request(&read).await.unwrap();
            assert!(matches!(page.result, Response::NativeRead(ref page) if page.visited == 1));
        } else {
            // Without native support the profile is rejected at negotiation;
            // the request never reaches the handler.
            assert!(matches!(
                remote.request(&submit).await,
                Err(WireError::Access(AccessError::UnsupportedProtocol))
            ));
        }
        // Earlier profiles keep working on the same socket either way.
        assert!(matches!(
            remote.request(&legacy).await.unwrap().result,
            Response::Error(AccessError::UnsupportedOperation)
        ));
        task.abort();
        let _ = task.await;
        drop(server);
    }
}
