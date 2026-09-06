use super::*;
use crate::{config::Settings, reads::ReadViews, streams::Streams};

fn peer(node: &EmbeddedNode, principal: ParticipantId, role: PeerRole) -> AuthenticatedPeer {
    AuthenticatedPeer::local(PeerGrant {
        principal,
        tenants: [node.identity.ledger.tenant].into_iter().collect(),
        role,
    })
    .unwrap()
}
fn envelope(ledger: LedgerId, id: u128, operation: Operation) -> RequestEnvelope {
    RequestEnvelope {
        protocol: PROTOCOL_VERSION,
        ledger,
        route_epoch: RouteEpoch(1),
        request_epoch: RequestEpoch(1),
        request_id: RequestId::from_u128(id),
        operation,
    }
}
fn reconcile(query: ReconcileQuery) -> Operation {
    Operation::Reconcile(query)
}
fn receipt(id: u128) -> Operation {
    reconcile(ReconcileQuery::Receipt {
        epoch: RequestEpoch(1),
        request: RequestId::from_u128(id),
    })
}
fn response(
    node: &mut EmbeddedNode,
    principal: AuthenticatedPeer,
    request: RequestEnvelope,
) -> ResponseEnvelope {
    let limits = WireLimits::default();
    let verified = verify_request(principal.clone(), request.clone(), &limits)
        .unwrap_or_else(|error| panic!("{error:?}: {request:?}"));
    let response = super::dispatch(
        node,
        &mut ReadViews::new(),
        &mut Streams::new().unwrap(),
        verified,
        &limits,
    );
    validate_response(&request, &response, Some(principal.principal()), &limits)
        .unwrap_or_else(|error| panic!("{error:?}: {request:?} => {response:?}"));
    response
}
fn page(response: ResponseEnvelope) -> ReconcileReply {
    match response.result {
        Response::Reconciled(page) => page,
        other => panic!("{other:?}"),
    }
}
fn same_page(actual: ReconcileReply, before: &ReconcileReply) {
    assert_eq!(actual.page, before.page);
    assert_eq!(actual.token, before.token);
    assert!(actual.applied_index >= before.applied_index);
}

#[test]
fn local_reconciliation_reads_committed_core_without_writes_and_preserves_floor_on_restart() {
    let root = tempfile::tempdir().unwrap();
    let mut settings = Settings::default();
    settings.node.data_dir = Some(root.path().to_owned());
    let mut node = EmbeddedNode::open(&settings).unwrap();
    let ledger = node.identity.ledger;
    let issuer = node.identity.issuer;
    let actor = peer(&node, issuer, PeerRole::Actor);
    let runtime = peer(&node, issuer, PeerRole::Runtime);
    let stranger = peer(&node, ParticipantId::from_u128(909), PeerRole::Actor);
    let committed = response(
        &mut node,
        actor.clone(),
        envelope(
            ledger,
            10,
            Operation::OpenEpoch {
                epoch: RequestEpoch(1),
            },
        ),
    );
    let Response::Submitted(MutationReply::Committed(expected)) = committed.result else {
        panic!("{committed:?}")
    };
    let before = node.session.status().applied_index;
    let sequence = node.session.sequence();
    let found = page(response(
        &mut node,
        actor.clone(),
        envelope(ledger, 20, receipt(10)),
    ));
    assert_eq!(found.token.sequence, sequence);
    assert!(
        matches!(found.page.result, ReconcileResult::Receipt { resolution: ReceiptResolution::Committed(ref actual),.. } if **actual == expected)
    );
    let hidden = page(response(
        &mut node,
        stranger.clone(),
        envelope(ledger, 21, receipt(10)),
    ));
    assert_eq!(hidden.page.principal, stranger.principal());
    assert!(matches!(
        hidden.page.result,
        ReconcileResult::Receipt {
            epoch: EpochReconciliation { minimum: None, .. },
            resolution: ReceiptResolution::Unknown,
            ..
        }
    ));
    let absent = page(response(
        &mut node,
        actor.clone(),
        envelope(ledger, 22, receipt(999)),
    ));
    assert!(matches!(
        absent.page.result,
        ReconcileResult::Receipt {
            resolution: ReceiptResolution::Unknown,
            ..
        }
    ));
    let epoch = page(response(
        &mut node,
        actor.clone(),
        envelope(
            ledger,
            23,
            reconcile(ReconcileQuery::Epoch {
                epoch: RequestEpoch(1),
            }),
        ),
    ));
    assert!(matches!(
        epoch.page.result,
        ReconcileResult::Epoch(EpochReconciliation {
            admitted: true,
            minimum: Some(RequestEpoch(1)),
            ..
        })
    ));
    assert_eq!(
        node.session.status().applied_index,
        before,
        "ReadIndex must not append or apply a command"
    );
    assert_eq!(node.session.sequence(), sequence);
    for id in [20, 21, 22, 23] {
        assert!(
            node.session
                .receipt(&RequestKey {
                    principal: issuer,
                    epoch: RequestEpoch(1),
                    id: RequestId::from_u128(id)
                })
                .is_none()
        );
    }
    let mut opening = envelope(
        ledger,
        30,
        Operation::OpenEpoch {
            epoch: RequestEpoch(2),
        },
    );
    opening.request_epoch = RequestEpoch(2);
    let opened = response(&mut node, actor.clone(), opening);
    assert!(matches!(
        opened.result,
        Response::Submitted(MutationReply::Committed(_))
    ));
    let mut floor = envelope(
        ledger,
        31,
        Operation::Submit {
            command: Command::AdvanceEpochFloor {
                minimum: RequestEpoch(2),
            },
            expected_revision: None,
        },
    );
    floor.request_epoch = RequestEpoch(2);
    assert!(matches!(
        response(&mut node, runtime, floor).result,
        Response::Submitted(MutationReply::Committed(_))
    ));
    let below = page(response(
        &mut node,
        actor.clone(),
        envelope(ledger, 32, receipt(999)),
    ));
    assert!(matches!(
        below.page.result,
        ReconcileResult::Receipt {
            resolution: ReceiptResolution::BelowFloor {
                minimum: RequestEpoch(2)
            },
            ..
        }
    ));
    let retained = page(response(
        &mut node,
        actor.clone(),
        envelope(ledger, 33, receipt(10)),
    ));
    assert!(
        matches!(retained.page.result, ReconcileResult::Receipt { resolution:ReceiptResolution::Committed(ref actual),.. } if **actual == expected)
    );
    node.checkpoint().unwrap();
    drop(node);
    let mut node = EmbeddedNode::open(&settings).unwrap();
    same_page(
        page(response(
            &mut node,
            actor.clone(),
            envelope(ledger, 34, receipt(999)),
        )),
        &below,
    );
    same_page(
        page(response(
            &mut node,
            actor.clone(),
            envelope(ledger, 35, receipt(10)),
        )),
        &retained,
    );
    let mut wrong_route = envelope(ledger, 36, receipt(10));
    wrong_route.route_epoch = RouteEpoch(2);
    assert!(matches!(
        response(&mut node, actor.clone(), wrong_route).result,
        Response::Error(AccessError::Unavailable)
    ));
    let mut wrong_ledger = envelope(ledger, 37, receipt(10));
    wrong_ledger.ledger.session = SessionId::from_u128(9876);
    assert!(matches!(
        response(&mut node, actor, wrong_ledger).result,
        Response::Error(AccessError::Unauthorized)
    ));
}

#[tokio::test]
async fn local_reconciliation_response_permit_survives_owner_shutdown() {
    let root = tempfile::tempdir().unwrap();
    let mut settings = Settings::default();
    settings.node.data_dir = Some(root.path().to_owned());
    let node = EmbeddedNode::open(&settings).unwrap();
    let actor = peer(&node, node.identity.issuer, PeerRole::Actor);
    let request = envelope(node.identity.ledger, 40, receipt(10));
    let limits = WireLimits::default();
    let (host, owner) = LocalHost::spawn(node, limits.clone()).unwrap();
    let reply = focal_wire::dispatch_accounted(&host, actor, request, &limits).await;
    assert!(matches!(reply.envelope().result, Response::Reconciled(_)));
    let held = host.budget.stats().used;
    assert!(held > 0 && held < 1024 * 1024);
    host.stop().await.unwrap();
    owner.join().unwrap();
    assert_eq!(host.budget.stats().used, held);
    drop(reply);
    assert_eq!(host.budget.stats().used, 0);
}

#[test]
fn cursor_reconciliation_keeps_original_metadata_prefix_principal_and_restart_receipt() {
    let root = tempfile::tempdir().unwrap();
    let mut settings = Settings::default();
    settings.node.data_dir = Some(root.path().to_owned());
    let mut node = EmbeddedNode::open(&settings).unwrap();
    let ledger = node.identity.ledger;
    let issuer = node.identity.issuer;
    let actor = peer(&node, issuer, PeerRole::Actor);
    let runtime = peer(&node, issuer, PeerRole::Runtime);
    response(
        &mut node,
        actor.clone(),
        envelope(
            ledger,
            60,
            Operation::OpenEpoch {
                epoch: RequestEpoch(1),
            },
        ),
    );
    let before_sequence = node.session.sequence();
    let before_index = node.session.status().applied_index;
    let stream = Operation::Stream(StreamRequest::Open {
        consumer: focal_stream::ConsumerId::from_u128(600),
        filter: focal_stream::DeltaFilter::Claims(
            [ClaimId::from_u128(1), ClaimId::from_u128(2)]
                .into_iter()
                .collect(),
        ),
        start: None,
        seed: false,
        credits: Credits {
            items: 1,
            bytes: 65536,
        },
    });
    assert!(matches!(
        response(&mut node, runtime, envelope(ledger, 61, stream)).result,
        Response::Stream(_)
    ));
    let original = node
        .session
        .cursor_receipt(&RequestKey {
            principal: issuer,
            epoch: RequestEpoch(1),
            id: RequestId::from_u128(61),
        })
        .unwrap()
        .clone();
    assert!(original.raft_index > before_index);
    assert_eq!(node.session.sequence(), before_sequence);
    let after_cursor = node.session.status().applied_index;
    let found = page(response(
        &mut node,
        actor.clone(),
        envelope(ledger, 62, receipt(61)),
    ));
    assert_eq!(found.page.sequence, before_sequence);
    assert_eq!(found.applied_index, after_cursor);
    let ReconcileResult::Receipt {
        resolution: ReceiptResolution::CommittedCursor(ref actual),
        ..
    } = found.page.result
    else {
        panic!("{found:?}")
    };
    assert_eq!(
        postcard::to_allocvec(actual).unwrap(),
        postcard::to_allocvec(&original).unwrap()
    );
    assert_eq!(node.session.status().applied_index, after_cursor);
    let small = WireLimits {
        max_items: 1,
        ..WireLimits::default()
    };
    assert_eq!(
        crate::reconciliation::local(
            &mut node.session,
            issuer,
            &ReconcileQuery::Receipt {
                epoch: RequestEpoch(1),
                request: RequestId::from_u128(61)
            },
            RequestId::from_u128(65),
            RouteEpoch(1),
            &small
        ),
        Err(AccessError::Capacity)
    );
    assert_eq!(node.session.status().applied_index, after_cursor);
    let stranger = peer(&node, ParticipantId::from_u128(909), PeerRole::Actor);
    assert!(matches!(
        page(response(
            &mut node,
            stranger,
            envelope(ledger, 63, receipt(61))
        ))
        .page
        .result,
        ReconcileResult::Receipt {
            resolution: ReceiptResolution::Unknown,
            ..
        }
    ));
    node.checkpoint().unwrap();
    drop(node);
    let mut node = EmbeddedNode::open(&settings).unwrap();
    same_page(
        page(response(
            &mut node,
            actor,
            envelope(ledger, 64, receipt(61)),
        )),
        &found,
    );
}

#[tokio::test]
async fn scalar_epoch_reconciliation_admits_under_pressure_without_a_full_frame_reservation() {
    let root = tempfile::tempdir().unwrap();
    let mut settings = Settings::default();
    settings.node.data_dir = Some(root.path().to_owned());
    let node = EmbeddedNode::open(&settings).unwrap();
    let actor = peer(&node, node.identity.issuer, PeerRole::Actor);
    let query = envelope(
        node.identity.ledger,
        70,
        reconcile(ReconcileQuery::Epoch {
            epoch: RequestEpoch(1),
        }),
    );
    let limits = WireLimits::default();
    let (host, owner) = LocalHost::spawn(node, limits.clone()).unwrap();
    let stats = host.budget.stats();
    let pressure = host
        .budget
        .reserve(
            BudgetKind::Payload,
            BudgetLane::Ordinary,
            stats.limit - stats.completion_reserve - stats.ordinary_used - 128 * 1024,
        )
        .unwrap();
    let reply = focal_wire::dispatch_accounted(&host, actor, query, &limits).await;
    assert!(
        matches!(reply.envelope().result, Response::Reconciled(_)),
        "{:?}",
        reply.envelope()
    );
    drop(reply);
    drop(pressure);
    host.stop().await.unwrap();
    owner.join().unwrap();
    assert_eq!(host.budget.stats().used, 0);
}
