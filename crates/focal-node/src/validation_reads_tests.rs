use super::*;
use crate::{config::Settings, embedded::EmbeddedNode};
use focal_ledger::Submission;

fn submit(node: &mut EmbeddedNode, label: &str, actor: ParticipantId, command: Command) {
    let input = crate::demo::request(&node.identity, label, actor, command, vec![]);
    assert!(matches!(
        node.session.submit_local(&input).unwrap(),
        Submission::Committed(_)
    ));
}
fn read_page(
    views: &mut ReadViews,
    node: &mut EmbeddedNode,
    id: ValidationId,
    consistency: ReadConsistency,
    after: Option<ValidationResultPosition>,
    limit: u32,
) -> ReadPage {
    let read = ReadRequest {
        consistency,
        query: ReadQuery::ValidationResults { id, after },
        max_items: limit,
    };
    let page = views
        .read(
            &mut node.session,
            node.identity.issuer,
            &read,
            RequestId::from_u128(111),
            &WireLimits::default(),
        )
        .unwrap();
    let request = RequestEnvelope {
        protocol: PROTOCOL_VERSION,
        ledger: page.token.ledger,
        route_epoch: page.token.route_epoch,
        request_epoch: RequestEpoch(1),
        request_id: RequestId::from_u128(111),
        operation: Operation::Read(read),
    };
    validate_response(
        &request,
        &request.reply(Response::Read(page.clone())),
        Some(node.identity.issuer),
        &WireLimits::default(),
    )
    .unwrap();
    page
}
fn results(
    page: &ReadPage,
) -> (
    &Validation,
    &[ValidationResult],
    Option<ValidationResultPosition>,
) {
    match &page.objects[0] {
        ReadObject::ValidationResults {
            value,
            records,
            next,
            ..
        } => (value, records, *next),
        _ => panic!("validation results"),
    }
}

#[test]
fn exact_validation_results_pin_publication_and_recover_from_wal_and_checkpoint() {
    let directory = tempfile::tempdir().unwrap();
    let mut settings = Settings::default();
    settings.node.data_dir = Some(directory.path().to_owned());
    let mut node = EmbeddedNode::open(&settings).unwrap();
    let identity = node.identity.clone();
    for (name, actor) in [
        ("result-issuer", identity.issuer),
        ("result-worker", identity.worker),
        ("result-evaluator", identity.evaluator),
    ] {
        submit(
            &mut node,
            name,
            actor,
            Command::NegotiateEpoch {
                epoch: RequestEpoch(1),
            },
        );
    }
    let claim_id = ClaimId::from_u128(500);
    let mut claim = crate::demo::claim(&identity, claim_id).unwrap();
    let test = claim
        .validations
        .iter_mut()
        .find(|v| v.content.kind == ValidationKind::Test)
        .unwrap();
    let test_id = test.id;
    test.content.evidence_schemas.clear();
    let first_handler = test.content.handlers[0].clone();
    let fallback = HandlerRef {
        id: ValidatorId::from_u128(600),
        version: ContentHash([7; 32]),
        agentic: false,
    };
    test.content.handlers.push(fallback.clone());
    claim.content.requirements = claim
        .validations
        .iter()
        .map(|v| RequirementRef {
            id: v.id,
            specification: v.content.specification_hash().unwrap(),
        })
        .collect();
    let validation = claim
        .validations
        .iter()
        .find(|v| v.content.kind == ValidationKind::Receipt)
        .unwrap()
        .id;
    submit(
        &mut node,
        "result-claim",
        identity.issuer,
        Command::GenerateClaim { claim },
    );
    let mut views = ReadViews::new();
    let no_execution = read_page(
        &mut views,
        &mut node,
        validation,
        ReadConsistency::Linearizable,
        None,
        16,
    );
    assert_eq!(results(&no_execution).0.lifecycle().latest_epoch, 0);
    assert!(results(&no_execution).1.is_empty());
    assert_eq!(results(&no_execution).2, None);
    submit(
        &mut node,
        "result-post",
        identity.issuer,
        Command::PostClaim { claim: claim_id },
    );
    let receipt = ReceiptFence {
        receipt: ReceiptId::from_u128(501),
        epoch: 1,
    };
    submit(
        &mut node,
        "result-receive",
        identity.worker,
        Command::AcquireReceipt {
            claim: claim_id,
            receipt: receipt.receipt,
            epoch: 1,
        },
    );
    let evidence_set = EvidenceSetId::from_u128(502);
    submit(
        &mut node,
        "result-open",
        identity.worker,
        Command::BeginEvidenceSet {
            claim: claim_id,
            receipt,
            evidence_set,
        },
    );
    let testament = TestamentId::from_u128(503);
    submit(
        &mut node,
        "result-close",
        identity.worker,
        Command::CloseTestament {
            claim: claim_id,
            receipt,
            testament,
            evidence_set,
            manifest: vec![],
            summary: "closed delivery".into(),
            confidence: Confidence::Tentative,
            outcome: OutcomeKind::Interrupted,
        },
    );
    let before_ack = read_page(
        &mut views,
        &mut node,
        validation,
        ReadConsistency::Linearizable,
        None,
        16,
    );
    assert!(results(&before_ack).1.is_empty());
    submit(
        &mut node,
        "result-ack",
        identity.issuer,
        Command::AcknowledgeTestament {
            claim: claim_id,
            testament,
        },
    );
    let current = read_page(
        &mut views,
        &mut node,
        validation,
        ReadConsistency::Linearizable,
        None,
        16,
    );
    let (requirement, records, next) = results(&current);
    assert_eq!(requirement.lifecycle().latest_epoch, 1);
    assert_eq!(records.len(), 2);
    assert!(next.is_none());
    let ValidationResultValue::Run(run) = &records[0].value else {
        panic!("run header")
    };
    assert_eq!(run.final_verdict, Some(VerdictValue::Pass));
    assert_eq!(run.attempt_count, 1);
    let ValidationResultValue::Attempt(verdict) = &records[1].value else {
        panic!("attempt")
    };
    assert_eq!(verdict.value, VerdictValue::Pass);
    assert_eq!(verdict.run, run.id);
    assert_eq!(records[1].position.attempt, Some(0));
    let header_frame = postcard::experimental::serialized_size(requirement).unwrap()
        + postcard::experimental::serialized_size(&records[0]).unwrap()
        + 512;
    let mut byte_limits = WireLimits {
        max_frame_bytes: u32::try_from(header_frame).unwrap(),
        ..WireLimits::default()
    };
    let byte_request = ReadRequest {
        consistency: ReadConsistency::Exact(current.token),
        query: ReadQuery::ValidationResults {
            id: validation,
            after: None,
        },
        max_items: 16,
    };
    let limited = views
        .read(
            &mut node.session,
            identity.issuer,
            &byte_request,
            RequestId::from_u128(222),
            &byte_limits,
        )
        .unwrap();
    assert_eq!(results(&limited).1, &records[..1]);
    assert_eq!(results(&limited).2, Some(records[0].position));
    byte_limits.max_frame_bytes -= 1;
    assert_eq!(
        views.read(
            &mut node.session,
            identity.issuer,
            &byte_request,
            RequestId::from_u128(223),
            &byte_limits
        ),
        Err(AccessError::Capacity)
    );
    // Cached heap work is admitted before serialization or cloning. An exact
    // budget for the requirement and header returns a continuation; one byte
    // less cannot return a false empty result or traverse the attempt row.
    let now = views.advance(&mut node.session).unwrap();
    let view = views
        .views
        .get(&(identity.issuer, current.token.sequence))
        .unwrap();
    let requirement_cost = view
        .project_object(
            ObjectRef {
                ledger: current.token.ledger,
                kind: ObjectKind::Validation,
                id: ObjectId(validation.0),
            },
            now,
            |_, cost| cost,
        )
        .unwrap()
        .unwrap();
    let header_cost = view
        .next_validation_result(validation, None, now)
        .unwrap()
        .unwrap()
        .bytes;
    let mut cost_limits = WireLimits {
        max_frame_bytes: 1024,
        max_cost: u64::try_from(requirement_cost + header_cost).unwrap(),
        ..WireLimits::default()
    };
    cost_limits.validate().unwrap();
    let limited = views
        .read(
            &mut node.session,
            identity.issuer,
            &byte_request,
            RequestId::from_u128(224),
            &cost_limits,
        )
        .unwrap();
    assert_eq!(results(&limited).1, &records[..1]);
    assert_eq!(results(&limited).2, Some(records[0].position));
    for max_cost in [cost_limits.max_cost - 1, requirement_cost as u64 - 1] {
        cost_limits.max_cost = max_cost;
        assert_eq!(
            views.read(
                &mut node.session,
                identity.issuer,
                &byte_request,
                RequestId::from_u128(225),
                &cost_limits,
            ),
            Err(AccessError::Capacity)
        );
    }
    assert_eq!(
        read_page(
            &mut views,
            &mut node,
            validation,
            ReadConsistency::Exact(no_execution.token),
            None,
            16
        ),
        no_execution
    );
    assert_eq!(
        read_page(
            &mut views,
            &mut node,
            validation,
            ReadConsistency::Exact(before_ack.token),
            None,
            16
        ),
        before_ack
    );
    let first = read_page(
        &mut views,
        &mut node,
        validation,
        ReadConsistency::Exact(current.token),
        None,
        1,
    );
    let cursor = results(&first).2.unwrap();
    let second = read_page(
        &mut views,
        &mut node,
        validation,
        ReadConsistency::Exact(current.token),
        Some(cursor),
        1,
    );
    assert_eq!(results(&first).1, &records[..1]);
    assert_eq!(results(&second).1, &records[1..]);
    assert_eq!(results(&second).2, None);
    let unknown = read_page(
        &mut views,
        &mut node,
        ValidationId::from_u128(999),
        ReadConsistency::Exact(current.token),
        None,
        1,
    );
    assert!(unknown.objects.is_empty());
    // A non-receipt run updates its summary and adds attempts at three distinct
    // committed prefixes. The old summary must not borrow a newer live run.
    submit(
        &mut node,
        "result-schedule",
        identity.issuer,
        Command::BeginWholeWorkValidation { claim: claim_id },
    );
    let scheduled = read_page(
        &mut views,
        &mut node,
        test_id,
        ReadConsistency::Linearizable,
        None,
        16,
    );
    let ValidationResultValue::Run(run) = &results(&scheduled).1[0].value else {
        panic!("scheduled run")
    };
    let run = run.clone();
    assert_eq!(run.attempt_count, 0);
    assert_eq!(run.final_verdict, None);
    submit(
        &mut node,
        "result-error",
        identity.evaluator,
        Command::RecordFencedValidationVerdict {
            receipt: Some(receipt),
            verdict: VerdictRecord {
                run: run.id,
                evaluator: identity.evaluator,
                handler: first_handler,
                attempt: 0,
                manifest: run.manifest,
                value: VerdictValue::Error,
                evidence: vec![],
            },
        },
    );
    let error = read_page(
        &mut views,
        &mut node,
        test_id,
        ReadConsistency::Linearizable,
        None,
        16,
    );
    let ValidationResultValue::Run(header) = &results(&error).1[0].value else {
        panic!("fallback run")
    };
    assert_eq!(header.handler_index, 1);
    assert_eq!(header.attempt_count, 1);
    assert_eq!(header.final_verdict, None);
    submit(
        &mut node,
        "result-fail",
        identity.evaluator,
        Command::RecordFencedValidationVerdict {
            receipt: Some(receipt),
            verdict: VerdictRecord {
                run: run.id,
                evaluator: identity.evaluator,
                handler: fallback,
                attempt: 1,
                manifest: run.manifest,
                value: VerdictValue::Fail,
                evidence: vec![],
            },
        },
    );
    let final_results = read_page(
        &mut views,
        &mut node,
        test_id,
        ReadConsistency::Linearizable,
        None,
        16,
    );
    let ValidationResultValue::Run(header) = &results(&final_results).1[0].value else {
        panic!("final run")
    };
    assert_eq!(header.final_verdict, Some(VerdictValue::Fail));
    assert_eq!(header.attempt_count, 2);
    assert_eq!(results(&final_results).1.len(), 3);
    assert_eq!(
        read_page(
            &mut views,
            &mut node,
            test_id,
            ReadConsistency::Exact(scheduled.token),
            None,
            16
        ),
        scheduled
    );
    assert_eq!(
        read_page(
            &mut views,
            &mut node,
            test_id,
            ReadConsistency::Exact(error.token),
            None,
            16
        ),
        error
    );
    let current = read_page(
        &mut views,
        &mut node,
        validation,
        ReadConsistency::Linearizable,
        None,
        16,
    );
    drop(views);
    drop(node);
    // No checkpoint was requested: restored results come from actual WAL replay.
    let mut reopened = EmbeddedNode::open(&settings).unwrap();
    let mut fresh = ReadViews::new();
    let replayed = read_page(
        &mut fresh,
        &mut reopened,
        validation,
        ReadConsistency::Linearizable,
        None,
        16,
    );
    assert_eq!(replayed, current);
    assert_eq!(
        read_page(
            &mut fresh,
            &mut reopened,
            test_id,
            ReadConsistency::Linearizable,
            None,
            16
        ),
        final_results
    );
    reopened.session.checkpoint().unwrap();
    drop(fresh);
    drop(reopened);
    let mut reopened = EmbeddedNode::open(&settings).unwrap();
    assert_eq!(
        read_page(
            &mut ReadViews::new(),
            &mut reopened,
            validation,
            ReadConsistency::Linearizable,
            None,
            16
        ),
        current
    );
    assert_eq!(
        read_page(
            &mut ReadViews::new(),
            &mut reopened,
            test_id,
            ReadConsistency::Linearizable,
            None,
            16
        ),
        final_results
    );
}

#[test]
fn result_cursors_cannot_cross_validation_or_skip_to_unpinned_state() {
    let directory = tempfile::tempdir().unwrap();
    let mut settings = Settings::default();
    settings.node.data_dir = Some(directory.path().to_owned());
    let mut node = EmbeddedNode::open(&settings).unwrap();
    let id = ValidationId::from_u128(700);
    let position = ValidationResultPosition {
        run: ValidationRunId {
            validation: id,
            target_hash: ContentHash([1; 32]),
            phase: ValidationPhase::WholeWork,
            epoch: 1,
        },
        attempt: None,
    };
    let mut views = ReadViews::new();
    let request = ReadRequest {
        consistency: ReadConsistency::StaleProjection,
        query: ReadQuery::ValidationResults {
            id,
            after: Some(position),
        },
        max_items: 1,
    };
    assert_eq!(
        views.read(
            &mut node.session,
            node.identity.issuer,
            &request,
            RequestId::from_u128(1),
            &WireLimits::default()
        ),
        Err(AccessError::InvalidRequest)
    );
}
