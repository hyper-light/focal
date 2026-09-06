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

fn ledger() -> LedgerId {
    LedgerId {
        tenant: TenantId::from_u128(1),
        session: SessionId::from_u128(2),
    }
}
fn id() -> ValidationId {
    ValidationId::from_u128(3)
}
fn position() -> ValidationResultPosition {
    ValidationResultPosition {
        run: ValidationRunId {
            validation: id(),
            target_hash: ContentHash([4; 32]),
            phase: ValidationPhase::WholeWork,
            epoch: 1,
        },
        attempt: None,
    }
}
fn token() -> ReadToken {
    ReadToken {
        ledger: ledger(),
        sequence: SessionSeq(10),
        route_epoch: RouteEpoch(1),
    }
}
fn request(after: Option<ValidationResultPosition>) -> RequestEnvelope {
    RequestEnvelope {
        protocol: PROTOCOL_VERSION,
        ledger: ledger(),
        route_epoch: RouteEpoch(1),
        request_epoch: RequestEpoch(1),
        request_id: RequestId::from_u128(5),
        operation: Operation::Read(ReadRequest {
            consistency: ReadConsistency::Exact(token()),
            query: ReadQuery::ValidationResults { id: id(), after },
            max_items: 1,
        }),
    }
}
fn peer() -> AuthenticatedPeer {
    AuthenticatedPeer::local(PeerGrant {
        principal: ParticipantId::from_u128(6),
        tenants: BTreeSet::from([ledger().tenant]),
        role: PeerRole::Actor,
    })
    .unwrap()
}
fn object() -> ReadObject {
    let content = ValidationContent {
        ledger: ledger(),
        schema: 1,
        claim: ClaimId::from_u128(7),
        kind: ValidationKind::Receipt,
        phase: ValidationPhase::WholeWork,
        mode: ValidationMode::Required,
        description: "delivery".into(),
        quality_bar: None,
        evaluator: ParticipantId::from_u128(8),
        handlers: vec![],
        evidence_schemas: BTreeSet::new(),
        contributed_by: BTreeSet::new(),
        policy_revision: 1,
    };
    let hash = content.content_hash().unwrap();
    let record = ValidationResult {
        position: position(),
        value: ValidationResultValue::Run(ValidationRunSummary {
            id: position().run,
            claim: content.claim,
            evaluator: content.evaluator,
            manifest: ContentHash([9; 32]),
            handler_index: 0,
            quality_phase: false,
            attempt_count: 1,
            final_verdict: Some(VerdictValue::Pass),
        }),
    };
    ReadObject::ValidationResults {
        id: id(),
        value: Validation::new(
            content,
            hash,
            ValidationLifecycle {
                created: SessionSeq(1),
                latest_epoch: 1,
            },
        ),
        records: vec![record],
        next: Some(position()),
    }
}
fn reply(request: &RequestEnvelope, object: ReadObject) -> ResponseEnvelope {
    request.reply(Response::Read(ReadPage {
        token: token(),
        objects: vec![object],
        next: None,
    }))
}

#[test]
fn appended_query_preserves_existing_query_ordinals_and_rejects_cross_scope_cursors() {
    let limits = WireLimits::default();
    assert_eq!(
        encode_payload(&ReadQuery::Objects(vec![]), 1024).unwrap(),
        vec![0, 0]
    );
    assert_eq!(
        encode_payload(&ReadQuery::Scan { after: None }, 1024).unwrap(),
        vec![1, 0]
    );
    assert_eq!(
        encode_payload(
            &ReadQuery::Traverse {
                roots: vec![],
                depth: 0
            },
            1024
        )
        .unwrap(),
        vec![2, 0, 0]
    );
    assert!(verify_request(peer(), request(None), &limits).is_ok());
    assert!(verify_request(peer(), request(Some(position())), &limits).is_ok());
    let mut foreign = position();
    foreign.run.validation = ValidationId::from_u128(90);
    assert!(verify_request(peer(), request(Some(foreign)), &limits).is_err());
    let mut unpinned = request(Some(position()));
    if let Operation::Read(read) = &mut unpinned.operation {
        read.consistency = ReadConsistency::Linearizable;
    }
    assert!(verify_request(peer(), unpinned, &limits).is_err());
    let mut wrong_tenant = request(None);
    wrong_tenant.ledger.tenant = TenantId::from_u128(99);
    assert!(verify_request(peer(), wrong_tenant, &limits).is_err());
}

#[test]
fn nested_result_validation_enforces_prefix_shape_bounds_and_progress() {
    let limits = WireLimits::default();
    let request = request(None);
    assert!(validate_response(&request, &reply(&request, object()), None, &limits).is_ok());
    let mut duplicate = object();
    if let ReadObject::ValidationResults { records, .. } = &mut duplicate {
        records.push(records[0].clone());
    }
    assert!(validate_response(&request, &reply(&request, duplicate), None, &limits).is_err());
    let mut cross = object();
    if let ReadObject::ValidationResults { records, .. } = &mut cross {
        records[0].position.run.validation = ValidationId::from_u128(100);
    }
    assert!(validate_response(&request, &reply(&request, cross), None, &limits).is_err());
    let mut future = object();
    if let ReadObject::ValidationResults { value, .. } = &mut future {
        *value = value.with_lifecycle(ValidationLifecycle {
            created: SessionSeq(1),
            latest_epoch: 0,
        });
    }
    assert!(validate_response(&request, &reply(&request, future), None, &limits).is_err());
    let resumed = super_request(Some(position()));
    assert!(validate_response(&resumed, &reply(&resumed, object()), None, &limits).is_err());
    let mut wrong_kind = request.clone();
    if let Operation::Read(read) = &mut wrong_kind.operation {
        read.query = ReadQuery::Objects(vec![]);
    }
    assert!(validate_response(&wrong_kind, &reply(&wrong_kind, object()), None, &limits).is_err());
    let mut bad_cursor = object();
    if let ReadObject::ValidationResults { next, .. } = &mut bad_cursor {
        next.as_mut().unwrap().attempt = Some(40);
    }
    assert!(validate_response(&request, &reply(&request, bad_cursor), None, &limits).is_err());
}
fn super_request(after: Option<ValidationResultPosition>) -> RequestEnvelope {
    request(after)
}

fn completed_quality_page() -> (RequestEnvelope, ReadObject) {
    let mut request = request(None);
    if let Operation::Read(read) = &mut request.operation {
        read.max_items = 3;
    }
    let ReadObject::ValidationResults { value, .. } = object() else {
        panic!("requirement")
    };
    let mut spec = value.content().clone();
    spec.kind = ValidationKind::Test;
    spec.quality_bar = Some("Review quality after tests pass".into());
    spec.handlers = (0_u128..4)
        .map(|index| HandlerRef {
            id: ValidatorId::from_u128(20 + index),
            version: ContentHash([11; 32]),
            agentic: index == 3,
        })
        .collect();
    let hash = spec.content_hash().unwrap();
    let manifest = ContentHash([9; 32]);
    let header = ValidationRunSummary {
        id: position().run,
        claim: spec.claim,
        evaluator: spec.evaluator,
        manifest,
        handler_index: 3,
        quality_phase: true,
        attempt_count: 2,
        final_verdict: Some(VerdictValue::Pass),
    };
    let records = vec![
        ValidationResult {
            position: position(),
            value: ValidationResultValue::Run(header),
        },
        ValidationResult {
            position: ValidationResultPosition {
                attempt: Some(0),
                ..position()
            },
            value: ValidationResultValue::Attempt(VerdictRecord {
                run: position().run,
                evaluator: spec.evaluator,
                handler: spec.handlers[0].clone(),
                attempt: 0,
                manifest,
                value: VerdictValue::Pass,
                evidence: vec![],
            }),
        },
        ValidationResult {
            position: ValidationResultPosition {
                attempt: Some(1),
                ..position()
            },
            value: ValidationResultValue::Attempt(VerdictRecord {
                run: position().run,
                evaluator: spec.evaluator,
                handler: spec.handlers[3].clone(),
                attempt: 1,
                manifest,
                value: VerdictValue::Pass,
                evidence: vec![],
            }),
        },
    ];
    (
        request,
        ReadObject::ValidationResults {
            id: id(),
            value: Validation::new(spec, hash, value.lifecycle().clone()),
            records,
            next: None,
        },
    )
}
#[test]
fn results_bind_pinned_phase_and_handlers_while_allowing_skipped_fallbacks() {
    let limits = WireLimits::default();
    let (request, valid) = completed_quality_page();
    assert!(validate_response(&request, &reply(&request, valid.clone()), None, &limits).is_ok());
    for scenario in 0..7 {
        let mut forged = valid.clone();
        let ReadObject::ValidationResults { records, .. } = &mut forged else {
            panic!("results")
        };
        match scenario {
            0 => {
                for record in records {
                    record.position.run.phase = ValidationPhase::Admission;
                    match &mut record.value {
                        ValidationResultValue::Run(run) => {
                            run.id.phase = ValidationPhase::Admission
                        }
                        ValidationResultValue::Attempt(attempt) => {
                            attempt.run.phase = ValidationPhase::Admission
                        }
                    }
                }
            }
            1 => {
                if let ValidationResultValue::Attempt(attempt) = &mut records[1].value {
                    attempt.handler.version = ContentHash([99; 32]);
                }
            }
            2 => {
                if let ValidationResultValue::Attempt(attempt) = &mut records[2].value {
                    attempt.attempt = 99;
                }
            }
            3 => {
                if let ValidationResultValue::Run(run) = &mut records[0].value {
                    run.handler_index = 99;
                }
            }
            4 => {
                if let ValidationResultValue::Run(run) = &mut records[0].value {
                    run.attempt_count = 99;
                }
            }
            5 => {
                if let ValidationResultValue::Attempt(attempt) = &mut records[1].value {
                    attempt.value = VerdictValue::Fail;
                }
            }
            6 => {
                if let ValidationResultValue::Attempt(attempt) = &mut records[2].value {
                    attempt.manifest = ContentHash([99; 32]);
                }
            }
            _ => unreachable!(),
        }
        assert!(
            validate_response(&request, &reply(&request, forged), None, &limits).is_err(),
            "forgery {scenario}"
        );
    }
    // A continuation can start at the quality attempt. Its pinned slot is 3,
    // while its committed execution ordinal is 1.
    let mut resumed = request.clone();
    let ReadObject::ValidationResults { records, .. } = &valid else {
        panic!("results")
    };
    if let Operation::Read(read) = &mut resumed.operation {
        read.query = ReadQuery::ValidationResults {
            id: id(),
            after: Some(records[1].position),
        };
    }
    let mut suffix = valid;
    if let ReadObject::ValidationResults { records, .. } = &mut suffix {
        records.drain(..2);
    }
    assert!(validate_response(&resumed, &reply(&resumed, suffix), None, &limits).is_ok());
}
