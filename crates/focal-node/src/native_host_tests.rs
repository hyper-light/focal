//! The native wire profile through the embedded owner: offline activation,
//! frame admission with exact retry, closed refusals and fixed-prefix reads.
use crate::{config::Settings, embedded::EmbeddedNode, host::LocalHost, native_activation};
use focal_core::native::{NativeCommand, NativeInput, input_codec};
use focal_ledger::{LedgerActivation, NativeContentProfile};
use focal_model::lifecycle::{
    Binding, Principal, aggregation,
    claim::ClaimDefinition,
    creation::{Owner, Proposal},
    graph, scope,
    succession::Lineage,
    validation,
};
use focal_model::*;
use focal_wire::*;

fn settings(root: &std::path::Path) -> Settings {
    let mut settings = Settings::default();
    settings.node.data_dir = Some(root.to_owned());
    settings
}
fn binding(ledger: LedgerId, id: u128) -> Binding {
    Binding {
        ledger,
        object: ObjectId::from_u128(id),
        content: ContentHash([7; 32]),
        revision: ObjectRevision(1),
    }
}
fn definition(issuer: ParticipantId, binding: Binding) -> validation::Declaration {
    let claim_id = u128::from_be_bytes(binding.object.0);
    validation::Declaration::new(
        Principal::Actor(issuer),
        validation::DeclarationSpec {
            binding: Binding {
                object: ObjectId::from_u128(claim_id.checked_add(10_000).unwrap()),
                ..binding
            },
            claim: ClaimId(binding.object.0),
            issuer,
            declaration_index: 900,
            kind: ValidationKind::Receipt,
            phase: ValidationPhase::WholeWork,
            mode: ValidationMode::Required,
            target: validation::TargetDeclaration::Delivery,
            program: validation::Program::Delivery,
            deadline: Deadline {
                timer: TimerId::from_u128(1),
                generation: 1,
                at: 100,
            },
        },
        validation::Limits {
            handlers: 4,
            attempts: 8,
            slot_bytes: 64,
        },
    )
    .unwrap()
}
fn proposal(ledger: LedgerId, issuer: ParticipantId, subject: ParticipantId, id: u128) -> Proposal {
    let binding = binding(ledger, id);
    Proposal {
        definition: ClaimDefinition {
            binding,
            issuer,
            subject,
            deadline: None,
            max_responses: 4,
            created: SessionSeq(999),
            graph: graph::Declaration::empty(),
            lineage: Lineage::root(binding, RootCommandId::from_u128(1)).unwrap(),
            acceptance: aggregation::AcceptancePolicy::new(
                binding,
                issuer,
                &[],
                &[definition(issuer, binding)],
                aggregation::Limits {
                    max_slots: 8,
                    max_checks: 16,
                    max_results: 32,
                    max_updates: 8,
                },
            )
            .unwrap(),
            scope_limits: scope::ScopeLimits {
                scopes: 8,
                roots: 32,
                children: 16,
            },
        },
        owner: None,
    }
}
fn create(node: &EmbeddedNode, request: u128, id: u128) -> NativeInput {
    let ledger = node.identity.ledger;
    let proposals = vec![proposal(
        ledger,
        node.identity.issuer,
        node.identity.worker,
        id,
    )];
    let declarations = proposals
        .iter()
        .map(|p| definition(node.identity.issuer, p.definition.binding))
        .collect();
    NativeInput {
        request: RequestKey {
            principal: node.identity.issuer,
            epoch: RequestEpoch(1),
            id: RequestId::from_u128(request),
        },
        command: NativeCommand::Create {
            claims: proposals,
            declarations,
        },
    }
}
fn frame(ledger: LedgerId, input: &NativeInput) -> Vec<u8> {
    let plan = input_codec::EncodingPlan::prepare(
        input_codec::InputFrame::Request {
            ledger,
            profile: NativeContentProfile::ProjectionOnly,
            input,
        },
        input_codec::EncodingLimits {
            bytes: 1 << 20,
            visits: 1 << 28,
        },
    )
    .unwrap();
    let mut bytes = vec![0; plan.quote().bytes];
    plan.write_into(&mut bytes).unwrap();
    bytes
}
fn peer(node: &EmbeddedNode, principal: ParticipantId, role: PeerRole) -> AuthenticatedPeer {
    AuthenticatedPeer::local(PeerGrant {
        principal,
        tenants: [node.identity.ledger.tenant].into_iter().collect(),
        role,
    })
    .unwrap()
}
fn envelope(
    node: &EmbeddedNode,
    protocol: u16,
    request: u128,
    operation: Operation,
) -> RequestEnvelope {
    RequestEnvelope {
        protocol,
        ledger: node.identity.ledger,
        route_epoch: RouteEpoch(1),
        request_epoch: RequestEpoch(1),
        request_id: RequestId::from_u128(request),
        operation,
    }
}
async fn call(host: &LocalHost, peer: AuthenticatedPeer, request: RequestEnvelope) -> Response {
    dispatch(host, peer, request, &WireLimits::default())
        .await
        .result
}
fn read(query: NativeReadQuery) -> Operation {
    Operation::NativeRead(NativeReadRequest {
        consistency: ReadConsistency::Linearizable,
        query,
        max_items: 16,
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_embedded_node_activates_offline_admits_frames_exactly_once_and_serves_native_reads() {
    let root = tempfile::tempdir().unwrap();
    // Before activation the host does not advertise the native profile at all.
    {
        let node = EmbeddedNode::open(&settings(root.path())).unwrap();
        assert_eq!(node.session.activation(), LedgerActivation::V1);
        let issuer = node.identity.issuer;
        let bytes = frame(node.identity.ledger, &create(&node, 1, 100));
        let request = envelope(
            &node,
            NATIVE_PROTOCOL_VERSION,
            1,
            Operation::Native { frame: bytes },
        );
        let actor = peer(&node, issuer, PeerRole::Actor);
        let (host, owner) = LocalHost::spawn(node, WireLimits::default()).unwrap();
        assert!(!host.supports_native_requests());
        assert_eq!(
            call(&host, actor, request).await,
            Response::Error(AccessError::UnsupportedProtocol)
        );
        host.stop().await.unwrap();
        owner.join_async().await.unwrap();
    }
    // Offline activation commits the genesis record and retains it.
    let activation = native_activation::activate_local(
        &settings(root.path()),
        NativeContentProfile::ProjectionOnly,
    )
    .unwrap();
    assert!(activation.proposed);
    assert!(activation.activation.is_native());
    let again = native_activation::activate_local(
        &settings(root.path()),
        NativeContentProfile::ProjectionOnly,
    )
    .unwrap();
    assert!(!again.proposed);
    assert_eq!(again.activation, activation.activation);

    let node = EmbeddedNode::open(&settings(root.path())).unwrap();
    assert!(node.session.activation().is_native());
    let ledger = node.identity.ledger;
    let issuer = node.identity.issuer;
    let worker = node.identity.worker;
    let bytes = frame(ledger, &create(&node, 1, 100));
    let other = frame(ledger, &create(&node, 1, 101));
    let second = frame(ledger, &create(&node, 2, 102));
    let actor = peer(&node, issuer, PeerRole::Actor);
    let stranger = peer(&node, worker, PeerRole::Actor);
    let request = |request: u128, frame: Vec<u8>| {
        envelope(
            &node,
            NATIVE_PROTOCOL_VERSION,
            request,
            Operation::Native { frame },
        )
    };
    let native_request = request(1, bytes.clone());
    let conflicting = request(1, other);
    let second_request = request(2, second);
    let wrong_principal = request(1, bytes.clone());
    let legacy_profile = envelope(
        &node,
        PROTOCOL_VERSION,
        1,
        Operation::Native {
            frame: bytes.clone(),
        },
    );
    let claim_read = envelope(
        &node,
        NATIVE_PROTOCOL_VERSION,
        3,
        read(NativeReadQuery::Claim {
            id: ClaimId::from_u128(100),
            expand: NativeClaimExpand {
                evaluations: true,
                ..NativeClaimExpand::default()
            },
        }),
    );
    let outcome_read = envelope(
        &node,
        NATIVE_PROTOCOL_VERSION,
        4,
        read(NativeReadQuery::Objects(vec![
            NativeObjectRef::Outcome(NativeInvocationRef::Request(RequestKey {
                principal: issuer,
                epoch: RequestEpoch(1),
                id: RequestId::from_u128(1),
            })),
            NativeObjectRef::Claim(ClaimId::from_u128(999)),
            NativeObjectRef::Definition(ValidationId::from_u128(10_100)),
        ])),
    );
    let events_read = envelope(
        &node,
        NATIVE_PROTOCOL_VERSION,
        5,
        read(NativeReadQuery::Events {
            after: None,
            limit: 8,
        }),
    );
    let standing_read = envelope(
        &node,
        NATIVE_PROTOCOL_VERSION,
        6,
        read(NativeReadQuery::Standing),
    );
    let stale_read = envelope(
        &node,
        NATIVE_PROTOCOL_VERSION,
        7,
        Operation::NativeRead(NativeReadRequest {
            consistency: ReadConsistency::Exact(ReadToken {
                ledger,
                sequence: SessionSeq(1),
                route_epoch: RouteEpoch(1),
            }),
            query: NativeReadQuery::Standing,
            max_items: 1,
        }),
    );
    let list = envelope(
        &node,
        NATIVE_PROTOCOL_VERSION,
        8,
        Operation::NativeList(NativeListRequest {
            filter: NativeListFilter::Claims {
                issuer: Some(issuer),
                subject: None,
                status: None,
                action: None,
                scope: None,
                relation: None,
                created_after: None,
            },
            cursor: None,
            max_items: 8,
            max_visits: 64,
        }),
    );
    let (host, owner) = LocalHost::spawn(node, WireLimits::default()).unwrap();
    assert!(host.supports_native_requests());

    // A fresh frame commits on the single-node authority.
    let Response::Native(NativeMutationReply::Committed(receipt)) =
        call(&host, actor.clone(), native_request.clone()).await
    else {
        panic!("fresh frame commits");
    };
    assert_eq!(receipt.operation, NativeOperationKind::Create);
    assert_eq!(receipt.sequence, SessionSeq(1));
    assert_eq!(receipt.counts.created, 1);
    assert_eq!(
        receipt.invocation,
        NativeInvocationRef::Request(RequestKey {
            principal: issuer,
            epoch: RequestEpoch(1),
            id: RequestId::from_u128(1),
        })
    );
    // The identical frame is the same committed outcome; another intent under
    // the same key is a closed conflict, never a second claim.
    assert_eq!(
        call(&host, actor.clone(), native_request).await,
        Response::Native(NativeMutationReply::Committed(receipt))
    );
    assert!(matches!(
        call(&host, actor.clone(), conflicting).await,
        Response::Native(NativeMutationReply::Refused(NativeRefusal {
            kind: NativeRefusalKind::Conflict,
            ..
        }))
    ));
    // Identity is bound to the authenticated peer and to the native profile.
    assert_eq!(
        call(&host, stranger, wrong_principal).await,
        Response::Error(AccessError::Unauthorized)
    );
    assert_eq!(
        call(&host, actor.clone(), legacy_profile).await,
        Response::Error(AccessError::UnsupportedProtocol)
    );
    let Response::Native(NativeMutationReply::Committed(second_receipt)) =
        call(&host, actor.clone(), second_request).await
    else {
        panic!("second frame commits");
    };
    assert_eq!(second_receipt.sequence, SessionSeq(2));

    // Reads project the committed rows at a fixed prefix.
    let Response::NativeRead(page) = call(&host, actor.clone(), claim_read).await else {
        panic!("claim read");
    };
    assert_eq!(page.native_sequence, SessionSeq(2));
    assert_eq!(page.token.sequence, SessionSeq(2));
    let NativeObject::Claim(claim) = &page.objects[0] else {
        panic!("claim document first: {:?}", page.objects);
    };
    assert_eq!(claim.issuer, issuer);
    assert_eq!(claim.subject, worker);
    assert_eq!(claim.status, ClaimStatus::Generated);
    assert_eq!(claim.origin, NativeClaimOrigin::Native);
    assert_eq!(claim.acceptance.len(), 0);
    assert!(claim.content.is_none());
    let Response::NativeRead(page) = call(&host, actor.clone(), outcome_read).await else {
        panic!("outcome read");
    };
    assert_eq!(page.visited, 3);
    assert!(
        matches!(&page.objects[0], NativeObject::Outcome(outcome) if outcome.sequence == SessionSeq(1))
    );
    assert_eq!(
        page.objects[1],
        NativeObject::Missing(NativeObjectRef::Claim(ClaimId::from_u128(999)))
    );
    let NativeObject::Definition(definition) = &page.objects[2] else {
        panic!("definition document: {:?}", page.objects[2]);
    };
    assert_eq!(definition.claim, ClaimId::from_u128(100));
    assert_eq!(definition.kind, ValidationKind::Receipt);
    assert_eq!(definition.target, NativeTargetDeclaration::Delivery);
    assert!(definition.content.is_none());
    let Response::NativeRead(page) = call(&host, actor.clone(), events_read).await else {
        panic!("events read");
    };
    assert!(!page.objects.is_empty());
    assert!(
        page.objects
            .iter()
            .all(|object| matches!(object, NativeObject::Event(_)))
    );
    let Response::NativeRead(page) = call(&host, actor.clone(), standing_read).await else {
        panic!("standing read");
    };
    assert_eq!(
        page.objects,
        vec![NativeObject::Standing(NativeStanding {
            principal: issuer,
            role: NativePeerRole::Actor,
            profile: NativeProfile::ProjectionOnly,
            native_sequence: SessionSeq(2),
            logical_time: page.logical_time,
        })]
    );
    assert_eq!(
        call(&host, actor.clone(), stale_read).await,
        Response::Error(AccessError::SnapshotExpired)
    );
    // The issuer index lists both committed claims in key order from the
    // committed prefix; two rows visited, nothing left to continue.
    let Response::NativeListed(listed) = call(&host, actor.clone(), list).await else {
        panic!("a bounded list page");
    };
    assert_eq!(listed.native_sequence, SessionSeq(2));
    assert_eq!(listed.visited, 2);
    assert!(listed.next.is_none());
    let mut ids: Vec<_> = listed
        .objects
        .iter()
        .map(|object| match object {
            NativeObject::Claim(claim) => claim.binding.object,
            other => panic!("{other:?}"),
        })
        .collect();
    ids.sort();
    assert_eq!(ids, [ObjectId::from_u128(100), ObjectId::from_u128(102)]);
    host.stop().await.unwrap();
    owner.join_async().await.unwrap();

    // The committed prefix and the activation survive a restart.
    let node = EmbeddedNode::open(&settings(root.path())).unwrap();
    assert!(node.session.activation().is_native());
    assert_eq!(node.session.native_sequence().unwrap(), SessionSeq(2));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn native_ledgers_stream_only_under_the_native_profile() {
    let root = tempfile::tempdir().unwrap();
    native_activation::activate_local(&settings(root.path()), NativeContentProfile::ProjectionOnly)
        .unwrap();
    let node = EmbeddedNode::open(&settings(root.path())).unwrap();
    assert!(node.session.activation().is_native());
    let actor = peer(&node, node.identity.issuer, PeerRole::Actor);
    let open = |protocol: u16, request: u128| {
        envelope(
            &node,
            protocol,
            request,
            Operation::Stream(StreamRequest::Open {
                consumer: focal_stream::ConsumerId::from_u128(1),
                filter: focal_stream::DeltaFilter::All,
                start: None,
                seed: false,
                credits: Credits {
                    items: 8,
                    bytes: 8192,
                },
            }),
        )
    };
    let legacy = open(PROTOCOL_VERSION, 1);
    let native = open(NATIVE_PROTOCOL_VERSION, 2);
    let (host, owner) = LocalHost::spawn(node, WireLimits::default()).unwrap();
    // A legacy-profile consumer cannot decode schema-2 deltas: refused at the
    // door, before any cursor is registered.
    assert_eq!(
        call(&host, actor.clone(), legacy).await,
        Response::Error(AccessError::UnsupportedProtocol)
    );
    // The native profile passes the gate. A raw legacy request key has no
    // admitted request epoch on a native ledger, so this cursor is refused as
    // an invalid request; durable watches use the managed cursor stream.
    assert_eq!(
        call(&host, actor, native).await,
        Response::Error(AccessError::InvalidRequest)
    );
    host.stop().await.unwrap();
    owner.join_async().await.unwrap();
}

/// A child proposal citing `parent` as its cause, with the owner binding the
/// request pins (23 §5, 13 P17.12); `owner_ledger` lets a test forge a
/// foreign parent while the frame itself stays on this ledger.
fn child_proposal(
    ledger: LedgerId,
    issuer: ParticipantId,
    subject: ParticipantId,
    id: u128,
    parent: Binding,
    receipt: Option<ReceiptFence>,
    owner_ledger: LedgerId,
) -> Proposal {
    let mut proposal = proposal(ledger, issuer, subject, id);
    proposal.definition.lineage = Lineage::new(
        proposal.definition.binding,
        Cause::Claim(ClaimId(parent.object.0)),
        &[],
        0,
    )
    .unwrap();
    proposal.owner = Some(Owner {
        expected: Binding {
            ledger: owner_ledger,
            ..parent
        },
        receipt,
    });
    proposal
}
fn create_from(
    ledger: LedgerId,
    principal: ParticipantId,
    issuer: ParticipantId,
    request: u128,
    proposals: Vec<Proposal>,
) -> NativeInput {
    let declarations = proposals
        .iter()
        .map(|p| definition(issuer, p.definition.binding))
        .collect();
    let _ = ledger;
    NativeInput {
        request: RequestKey {
            principal,
            epoch: RequestEpoch(1),
            id: RequestId::from_u128(request),
        },
        command: NativeCommand::Create {
            claims: proposals,
            declarations,
        },
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn child_causes_are_refused_for_foreign_stale_and_unauthorized_parentage() {
    let root = tempfile::tempdir().unwrap();
    native_activation::activate_local(&settings(root.path()), NativeContentProfile::ProjectionOnly)
        .unwrap();
    let node = EmbeddedNode::open(&settings(root.path())).unwrap();
    let ledger = node.identity.ledger;
    let issuer = node.identity.issuer;
    let worker = node.identity.worker;
    let foreign = LedgerId {
        tenant: ledger.tenant,
        session: SessionId::from_u128(0xfeed),
    };
    let parent = binding(ledger, 300);
    let native = |principal: ParticipantId, request: u128, input: &NativeInput| {
        envelope(
            &node,
            NATIVE_PROTOCOL_VERSION,
            request,
            Operation::Native {
                frame: frame(ledger, input),
            },
        )
        .pipe(|request| (peer(&node, principal, PeerRole::Actor), request))
    };
    let creation = native(issuer, 1, &create(&node, 1, 300));
    let child_by_issuer = native(
        issuer,
        2,
        &create_from(
            ledger,
            issuer,
            issuer,
            2,
            vec![child_proposal(
                ledger, issuer, worker, 301, parent, None, ledger,
            )],
        ),
    );
    // The subject holds no receipt: neither the parent's issuer nor its
    // current receipt holder, so it cannot cite the parent as a cause.
    let child_by_stranger = native(
        worker,
        3,
        &create_from(
            ledger,
            worker,
            worker,
            3,
            vec![child_proposal(
                ledger, worker, issuer, 302, parent, None, ledger,
            )],
        ),
    );
    // A foreign-ledger parent binding is refused before any lookup.
    let child_foreign = native(
        issuer,
        4,
        &create_from(
            ledger,
            issuer,
            issuer,
            4,
            vec![child_proposal(
                ledger, issuer, worker, 303, parent, None, foreign,
            )],
        ),
    );
    // A parent binding at revision one after the parent changed is stale.
    let child_stale = native(
        issuer,
        6,
        &create_from(
            ledger,
            issuer,
            issuer,
            6,
            vec![child_proposal(
                ledger, issuer, worker, 304, parent, None, ledger,
            )],
        ),
    );
    let (host, owner) = LocalHost::spawn(node, WireLimits::default()).unwrap();
    let Response::Native(NativeMutationReply::Committed(_)) =
        call(&host, creation.0, creation.1).await
    else {
        panic!("parent creation commits");
    };
    // Refusals that never touch the parent come first: the parent's binding
    // is still at revision one for them.
    let stranger = call(&host, child_by_stranger.0, child_by_stranger.1).await;
    assert!(
        matches!(
            stranger,
            Response::Native(NativeMutationReply::Refused(NativeRefusal {
                kind: NativeRefusalKind::Refused(NativeErrorCode::WrongActor),
                ..
            }))
        ),
        "{stranger:?}"
    );
    let foreign_reply = call(&host, child_foreign.0, child_foreign.1).await;
    assert!(
        matches!(
            foreign_reply,
            Response::Native(NativeMutationReply::Refused(NativeRefusal {
                kind: NativeRefusalKind::Refused(
                    NativeErrorCode::InvalidTarget | NativeErrorCode::WrongLedger
                ),
                ..
            }))
        ),
        "{foreign_reply:?}"
    );
    let Response::Native(NativeMutationReply::Committed(receipt)) =
        call(&host, child_by_issuer.0, child_by_issuer.1).await
    else {
        panic!("the issuer's child commits");
    };
    assert_eq!(receipt.counts.created, 1);
    // The issuer's child registration advanced the parent's revision, so a
    // request pinning the original binding is stale.
    let stale = call(&host, child_stale.0, child_stale.1).await;
    assert!(
        matches!(
            stale,
            Response::Native(NativeMutationReply::Refused(NativeRefusal {
                kind: NativeRefusalKind::Stale { .. }
                    | NativeRefusalKind::Refused(NativeErrorCode::StaleRevision),
                ..
            }))
        ),
        "{stale:?}"
    );
    host.stop().await.unwrap();
    owner.join_async().await.unwrap();
}
trait Pipe: Sized {
    fn pipe<T>(self, f: impl FnOnce(Self) -> T) -> T {
        f(self)
    }
}
impl<T> Pipe for T {}
