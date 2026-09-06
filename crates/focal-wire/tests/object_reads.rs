#![allow(
    clippy::panic,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::unreachable,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros
)]
//! A framed response must answer the requested object keys and read prefix.
use focal_model::*;
use focal_wire::*;
use std::collections::BTreeSet;

fn ledger() -> LedgerId {
    LedgerId {
        tenant: TenantId::from_u128(1),
        session: SessionId::from_u128(2),
    }
}
fn reference(kind: ObjectKind, id: u128) -> ObjectRef {
    ObjectRef {
        ledger: ledger(),
        kind,
        id: ObjectId::from_u128(id),
    }
}
fn token() -> ReadToken {
    ReadToken {
        ledger: ledger(),
        sequence: SessionSeq(20),
        route_epoch: RouteEpoch(3),
    }
}
fn request(references: Vec<ObjectRef>) -> RequestEnvelope {
    RequestEnvelope {
        protocol: PROTOCOL_VERSION,
        ledger: ledger(),
        route_epoch: RouteEpoch(3),
        request_epoch: RequestEpoch(1),
        request_id: RequestId::from_u128(4),
        operation: Operation::Read(ReadRequest {
            consistency: ReadConsistency::Linearizable,
            query: ReadQuery::Objects(references),
            max_items: 16,
        }),
    }
}
fn object(kind: ObjectKind, id: u128) -> ReadObject {
    match kind {
        ObjectKind::Claim => {
            let content = ClaimContent {
                ledger: ledger(),
                schema: 1,
                occurrence: OccurrenceId::from_u128(8),
                description: "claim descriptor".into(),
                relations: BTreeSet::new(),
                scopes: BTreeSet::new(),
                requirements: vec![],
                deadline: None,
            };
            let hash = content.content_hash().unwrap();
            ReadObject::Claim {
                id: ClaimId::from_u128(id),
                value: Claim::new(
                    content,
                    hash,
                    ClaimLifecycle {
                        status: ClaimStatus::Generated,
                        revision: ObjectRevision(1),
                        created: SessionSeq(1),
                        history: vec![StatusFact {
                            status: ClaimStatus::Generated,
                            sequence: SessionSeq(1),
                        }],
                        receipt: None,
                        evidence_set: None,
                        testament: None,
                        local_complete: false,
                        released: false,
                        terminal_witness: None,
                    },
                ),
            }
        }
        ObjectKind::Testament => {
            let content = TestamentContent {
                ledger: ledger(),
                schema: 1,
                claim: ClaimId::from_u128(5),
                receipt: ReceiptFence {
                    receipt: ReceiptId::from_u128(6),
                    epoch: 1,
                },
                evidence_set: EvidenceSetId::from_u128(7),
                artifacts: vec![],
                summary: "closed proof".into(),
                confidence: Confidence::Tentative,
                outcome: OutcomeKind::Interrupted,
            };
            let hash = content.content_hash().unwrap();
            ReadObject::Testament {
                id: TestamentId::from_u128(id),
                value: Testament::new(
                    content,
                    hash,
                    TestamentLifecycle {
                        created: SessionSeq(1),
                        acknowledged: None,
                    },
                ),
            }
        }
        ObjectKind::Artifact => {
            let content = ArtifactContent {
                ledger: ledger(),
                schema: 1,
                kind: "text".into(),
                schema_hash: ContentHash([9; 32]),
                metadata: vec![],
                payload: ArtifactPayload::Inline(b"proof".to_vec()),
                producer: ParticipantId::from_u128(6),
                receipt: None,
                inputs: BTreeSet::new(),
                visibility: BTreeSet::new(),
            };
            let hash = content.content_hash().unwrap();
            ReadObject::Artifact {
                id: ArtifactId::from_u128(id),
                value: Artifact::new(
                    content,
                    hash,
                    ArtifactLifecycle {
                        created: SessionSeq(1),
                        custody_revision: 1,
                    },
                ),
            }
        }
        ObjectKind::Validation => {
            let content = ValidationContent {
                ledger: ledger(),
                schema: 1,
                claim: ClaimId::from_u128(5),
                kind: ValidationKind::Receipt,
                phase: ValidationPhase::WholeWork,
                mode: ValidationMode::Required,
                description: "receipt".into(),
                quality_bar: None,
                evaluator: ParticipantId::from_u128(6),
                handlers: vec![],
                evidence_schemas: BTreeSet::new(),
                contributed_by: BTreeSet::new(),
                policy_revision: 1,
            };
            let hash = content.content_hash().unwrap();
            ReadObject::Validation {
                id: ValidationId::from_u128(id),
                value: Validation::new(
                    content,
                    hash,
                    ValidationLifecycle {
                        created: SessionSeq(1),
                        latest_epoch: 0,
                    },
                ),
            }
        }
    }
}
fn reply(request: &RequestEnvelope, objects: Vec<ReadObject>) -> ResponseEnvelope {
    request.reply(Response::Read(ReadPage {
        token: token(),
        objects,
        next: None,
    }))
}
fn valid(request: &RequestEnvelope, response: &ResponseEnvelope) -> bool {
    validate_response(request, response, None, &WireLimits::default()).is_ok()
}

#[test]
fn exact_objects_accept_only_requested_keys_and_ordered_missing_subsets() {
    let kinds = [
        ObjectKind::Claim,
        ObjectKind::Testament,
        ObjectKind::Artifact,
        ObjectKind::Validation,
    ];
    let references = kinds.into_iter().map(|kind| reference(kind, 10)).collect();
    let request = request(references);
    assert!(valid(&request, &reply(&request, vec![])));
    let full = kinds
        .into_iter()
        .map(|kind| object(kind, 10))
        .collect::<Vec<_>>();
    assert!(valid(&request, &reply(&request, full.clone())));
    // Different families may deliberately share the same object ID.
    assert!(valid(
        &request,
        &reply(&request, vec![full[1].clone(), full[3].clone()])
    ));
    for objects in [
        vec![object(ObjectKind::Claim, 99)],
        vec![full[0].clone(), full[0].clone()],
        vec![full[2].clone(), full[1].clone()],
        vec![full[0].clone(), object(ObjectKind::Artifact, 99)],
    ] {
        assert!(!valid(&request, &reply(&request, objects)));
    }
    for kind in kinds {
        let request = request_for_one(kind);
        assert!(valid(&request, &reply(&request, vec![object(kind, 10)])));
        let wrong_kind = if kind == ObjectKind::Claim {
            ObjectKind::Artifact
        } else {
            ObjectKind::Claim
        };
        assert!(!valid(
            &request,
            &reply(&request, vec![object(wrong_kind, 10)])
        ));
        assert!(!valid(&request, &reply(&request, vec![object(kind, 11)])));
    }
}
fn request_for_one(kind: ObjectKind) -> RequestEnvelope {
    request(vec![reference(kind, 10)])
}

#[test]
fn explicit_repeated_references_keep_occurrence_semantics_without_allowing_extra_duplicates() {
    let request = request(vec![
        reference(ObjectKind::Claim, 10),
        reference(ObjectKind::Artifact, 11),
        reference(ObjectKind::Claim, 10),
    ]);
    let claim = object(ObjectKind::Claim, 10);
    assert!(valid(
        &request,
        &reply(&request, vec![claim.clone(), claim.clone()])
    ));
    assert!(valid(
        &request,
        &reply(
            &request,
            vec![
                claim.clone(),
                object(ObjectKind::Artifact, 11),
                claim.clone()
            ]
        )
    ));
    assert!(!valid(
        &request,
        &reply(&request, vec![claim.clone(), claim.clone(), claim])
    ));
}

#[test]
fn exact_objects_reject_foreign_prefixes_routes_and_synthetic_continuations() {
    let mut request = request_for_one(ObjectKind::Claim);
    let response = reply(&request, vec![object(ObjectKind::Claim, 10)]);
    for index in 0..4 {
        let mut forged = response.clone();
        let Response::Read(page) = &mut forged.result else {
            panic!("read")
        };
        match index {
            0 => page.token.ledger.session = SessionId::from_u128(99),
            1 => page.token.route_epoch = RouteEpoch(4),
            2 => {
                page.next = Some(ObjectKey {
                    kind: ObjectKind::Claim,
                    id: ObjectId::from_u128(10),
                })
            }
            3 => forged.route_epoch = RouteEpoch(4),
            _ => unreachable!(),
        }
        assert!(!valid(&request, &forged));
    }
    if let Operation::Read(read) = &mut request.operation {
        read.consistency = ReadConsistency::Exact(token());
    }
    assert!(valid(&request, &response));
    let mut later = response.clone();
    let Response::Read(page) = &mut later.result else {
        panic!("read")
    };
    page.token.sequence = SessionSeq(21);
    assert!(!valid(&request, &later));
    if let Operation::Read(read) = &mut request.operation {
        read.consistency = ReadConsistency::AtLeast(token());
    }
    assert!(valid(&request, &later));
    let mut older = response.clone();
    let Response::Read(page) = &mut older.result else {
        panic!("read")
    };
    page.token.sequence = SessionSeq(19);
    assert!(!valid(&request, &older));
    if let Operation::Read(read) = &mut request.operation {
        let mut foreign = token();
        foreign.ledger.tenant = TenantId::from_u128(99);
        read.consistency = ReadConsistency::AtLeast(foreign);
    }
    assert!(!valid(&request, &response));
    let mut foreign = request_for_one(ObjectKind::Claim);
    if let Operation::Read(ReadRequest {
        query: ReadQuery::Objects(references),
        ..
    }) = &mut foreign.operation
    {
        references[0].ledger.tenant = TenantId::from_u128(99);
    }
    assert!(!valid(&foreign, &reply(&foreign, vec![])));
}
