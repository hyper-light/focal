#![allow(
    clippy::panic,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::unreachable,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros
)]
use focal_client::managed_store::{ManagedOperationStore, ManagedStoreError, ManagedStoreLimits};
use focal_client::operation_store::OperationIntent;
use focal_client::pending::OperationContext;
use focal_client::{Client, ClientError, EmbeddedTransport, RetryPolicy};
use focal_model::*;
use focal_node::{
    config::Settings,
    embedded::{EmbeddedNode, NodeIdentity},
    host::{HostOwner, LocalHost},
};
use focal_wire::*;
use std::{collections::BTreeSet, time::Duration};
use tokio::runtime::Runtime;

const CLAIM: ClaimId = ClaimId::from_u128(700);
const CONSUMER: ConsumerId = ConsumerId::from_u128(701);

struct Harness {
    identity: NodeIdentity,
    client: Client<EmbeddedTransport<LocalHost>>,
    host: LocalHost,
    owner: HostOwner,
}
impl Harness {
    fn start(node: EmbeddedNode) -> Self {
        let identity = node.identity.clone();
        let limits = WireLimits::default();
        let (host, owner) = LocalHost::spawn(node, limits.clone()).unwrap();
        let peer = AuthenticatedPeer::local(PeerGrant {
            principal: identity.issuer,
            tenants: BTreeSet::from([identity.ledger.tenant]),
            role: PeerRole::Runtime,
        })
        .unwrap();
        let client = Client::new(
            EmbeddedTransport::new(peer, host.clone(), limits.clone()).unwrap(),
            RetryPolicy {
                max_attempts: 1,
                max_elapsed: Duration::from_secs(3),
                ..RetryPolicy::default()
            },
            limits,
            1,
        )
        .unwrap();
        Self {
            identity,
            client,
            host,
            owner,
        }
    }
    fn context(&self) -> OperationContext {
        OperationContext {
            cluster: self.identity.cluster,
            ledger: self.identity.ledger,
            principal: self.identity.issuer,
        }
    }
    /// Drop ingress without Stop: the WAL, not a graceful checkpoint, must
    /// recover the acknowledged commits and the caller's unknown replies.
    fn drop_without_checkpoint(self) {
        drop(self.client);
        drop(self.host);
        self.owner.join().unwrap();
    }
    fn checkpoint_and_stop(self, runtime: &Runtime) {
        runtime.block_on(self.host.stop()).unwrap();
        drop(self.client);
        drop(self.host);
        self.owner.join().unwrap();
    }
}

fn envelope(context: OperationContext, id: RequestId, operation: Operation) -> RequestEnvelope {
    RequestEnvelope {
        protocol: MANAGED_PROTOCOL_VERSION,
        ledger: context.ledger,
        route_epoch: RouteEpoch(1),
        request_epoch: RequestEpoch(1),
        request_id: id,
        operation,
    }
}
fn control_request(input: &RequestStreamControlInput) -> RequestEnvelope {
    envelope(
        OperationContext {
            cluster: input.cluster,
            ledger: input.ledger,
            principal: input.principal,
        },
        input.id,
        Operation::RequestStreamControl {
            cluster: input.cluster,
            command: input.command.clone(),
        },
    )
}
fn control(
    runtime: &Runtime,
    harness: &Harness,
    input: &RequestStreamControlInput,
) -> RequestStreamControlReceipt {
    runtime
        .block_on(
            harness
                .client
                .request_stream_control(control_request(input), harness.context()),
        )
        .unwrap()
        .receipt
}
fn read(runtime: &Runtime, harness: &Harness, query: RequestStreamQuery) -> RequestStreamRead {
    runtime
        .block_on(harness.client.request_stream_read(
            envelope(
                harness.context(),
                RequestId::from_u128(9000),
                Operation::RequestStreamRead {
                    cluster: harness.context().cluster,
                    query,
                },
            ),
            harness.context(),
        ))
        .unwrap()
        .page
}
fn assert_legacy(runtime: &Runtime, harness: &Harness, expected: &MutationReceipt) {
    let mut request = envelope(
        harness.context(),
        RequestId::from_u128(9001),
        Operation::Reconcile(ReconcileQuery::Receipt {
            epoch: expected.key.epoch,
            request: expected.key.id,
        }),
    );
    request.protocol = PROTOCOL_VERSION;
    let reply = runtime
        .block_on(harness.client.reconcile(request, harness.identity.issuer))
        .unwrap();
    assert!(matches!(reply.page.result, ReconcileResult::Receipt {
        resolution: ReceiptResolution::Committed(ref actual), ..
    } if actual.as_ref() == expected));
}
fn new_claim(identity: &NodeIdentity) -> NewClaim {
    let validation = NewValidation {
        id: ValidationId::from_u128(702),
        content: ValidationContent {
            ledger: identity.ledger,
            schema: 1,
            claim: CLAIM,
            kind: ValidationKind::Receipt,
            phase: ValidationPhase::WholeWork,
            mode: ValidationMode::Required,
            description: "receipt proof".into(),
            quality_bar: None,
            evaluator: identity.evaluator,
            handlers: Vec::new(),
            evidence_schemas: BTreeSet::new(),
            contributed_by: BTreeSet::from([identity.issuer]),
            policy_revision: 1,
        },
    };
    NewClaim {
        id: CLAIM,
        content: ClaimContent {
            ledger: identity.ledger,
            schema: 1,
            occurrence: OccurrenceId::from_u128(703),
            description: "managed namespace survives durable reply loss".into(),
            relations: BTreeSet::from([
                Relation {
                    kind: RelationKind::Issuer,
                    target: RelationTarget::Participant(identity.issuer),
                },
                Relation {
                    kind: RelationKind::Subject,
                    target: RelationTarget::Participant(identity.worker),
                },
                Relation {
                    kind: RelationKind::ClaimAction,
                    target: RelationTarget::Action(ActionType::Work),
                },
                Relation {
                    kind: RelationKind::CausedBy,
                    target: RelationTarget::Root(identity.root),
                },
            ]),
            scopes: BTreeSet::new(),
            requirements: vec![RequirementRef {
                id: validation.id,
                specification: validation.content.specification_hash().unwrap(),
            }],
            deadline: None,
        },
        validations: vec![validation],
    }
}
fn registration(context: OperationContext, generation: u64, id: u128) -> RequestStreamControlInput {
    RequestStreamControlInput {
        cluster: context.cluster,
        ledger: context.ledger,
        principal: context.principal,
        id: RequestId::from_u128(id),
        command: RequestStreamCommand::Register {
            slot: 0,
            expected_generation: generation,
            owner: RequestId::from_u128(id + 1),
            window: 2,
        },
    }
}
fn limits() -> ManagedStoreLimits {
    ManagedStoreLimits {
        window: 2,
        ..ManagedStoreLimits::default()
    }
}

#[test]
fn actual_client_store_reconciles_lost_replies_retires_both_families_and_keeps_legacy_receipts() {
    // Store fsyncs run on this synchronous caller; only transport waits enter the runtime.
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let temp = tempfile::tempdir().unwrap();
    let mut settings = Settings::default();
    settings.node.data_dir = Some(temp.path().join("node"));
    let harness = Harness::start(EmbeddedNode::open(&settings).unwrap());
    let context = harness.context();
    let original_identity = harness.identity.clone();
    let mut legacy_request = envelope(
        context,
        RequestId::from_u128(1000),
        Operation::OpenEpoch {
            epoch: RequestEpoch(1),
        },
    );
    legacy_request.protocol = PROTOCOL_VERSION;
    let MutationReply::Committed(legacy) = runtime
        .block_on(harness.client.submit(legacy_request))
        .unwrap()
    else {
        panic!("legacy receipt")
    };

    let store_root = temp.path().join("managed");
    let register = registration(context, 0, 2000);
    let store =
        ManagedOperationStore::create(&store_root, context, limits(), register.clone()).unwrap();
    let original_registration = control(&runtime, &harness, &store.registration().unwrap());
    // Registration committed, but its response never entered the private journal.
    drop(store);
    harness.drop_without_checkpoint();
    let harness = Harness::start(EmbeddedNode::open(&settings).unwrap());
    assert_eq!(harness.identity, original_identity);
    let store = ManagedOperationStore::open(&store_root, context, limits()).unwrap();
    assert!(!store.status().unwrap().registered);
    let retried_registration = control(&runtime, &harness, &store.registration().unwrap());
    assert_eq!(retried_registration, original_registration);
    store.record_registration(retried_registration).unwrap();

    // Same RequestId as the legacy receipt is a distinct real scoped key.
    let domain = store.reserve(RequestId::from_u128(1000)).unwrap();
    let authored = new_claim(&harness.identity);
    let domain_intent = OperationIntent {
        name: "claim.submit",
        version: 1,
        canonical: br#"{"description":"durable managed request"}"#,
    };
    let prepared = store
        .prepare(domain, domain_intent, |key| {
            Ok(envelope(
                context,
                key.id,
                Operation::Managed {
                    key,
                    operation: ManagedOperation::Submit {
                        expected_revision: None,
                        command: Command::GenerateClaim { claim: authored },
                    },
                },
            ))
        })
        .unwrap();
    let exact_domain_request = prepared.request;
    let original_domain = runtime
        .block_on(
            harness
                .client
                .submit_managed(exact_domain_request.clone(), context),
        )
        .unwrap()
        .receipt;
    assert_eq!(original_domain.sequence, SessionSeq(legacy.sequence.0 + 1));
    assert!(
        matches!(&original_domain.outcome, ManagedReceiptOutcome::Domain(CommandResult::Generated(ids)) if ids == &[CLAIM])
    );
    assert_eq!(store.receipt(domain).unwrap(), None);
    drop(store);
    harness.drop_without_checkpoint();

    let node = EmbeddedNode::open(&settings).unwrap();
    assert_eq!(node.session.sequence(), original_domain.sequence);
    assert_eq!(
        node.session
            .read_at_least(original_domain.sequence)
            .unwrap()
            .claims
            .len(),
        1
    );
    let harness = Harness::start(node);
    let store = ManagedOperationStore::open(&store_root, context, limits()).unwrap();
    let retried = store
        .prepare(domain, domain_intent, |_| {
            panic!("unknown operation expanded twice")
        })
        .unwrap();
    assert_eq!(retried.request, exact_domain_request);
    let recovered = runtime
        .block_on(harness.client.submit_managed(retried.request, context))
        .unwrap()
        .receipt;
    assert_eq!(recovered, original_domain);
    store.record_receipt(domain, &recovered).unwrap();

    let cursor = store.reserve(RequestId::from_u128(1001)).unwrap();
    let prepared = store
        .prepare(
            cursor,
            OperationIntent {
                name: "stream.open",
                version: 1,
                canonical: br#"{"filter":"all","seed":false}"#,
            },
            |key| {
                Ok(envelope(
                    context,
                    key.id,
                    Operation::Managed {
                        key,
                        operation: ManagedOperation::Cursor(StreamRequest::Open {
                            consumer: CONSUMER,
                            filter: DeltaFilter::All,
                            start: None,
                            seed: false,
                            credits: Credits {
                                items: 16,
                                bytes: 64 * 1024,
                            },
                        }),
                    },
                ))
            },
        )
        .unwrap();
    let cursor_request = prepared.request;
    let opened = runtime
        .block_on(
            harness
                .client
                .submit_managed(cursor_request.clone(), context),
        )
        .unwrap();
    let cursor_delivery = opened.stream.unwrap();
    assert_eq!(
        opened.receipt.sequence, original_domain.sequence,
        "cursor metadata is not a domain mutation"
    );
    assert!(opened.receipt.raft_index > original_domain.raft_index);
    assert!(matches!(
        opened.receipt.outcome,
        ManagedReceiptOutcome::Cursor {
            record: Some(_),
            ..
        }
    ));
    assert_eq!(
        cursor_delivery.acknowledged.position,
        Position::origin(context.ledger)
    );
    store.record_receipt(cursor, &opened.receipt).unwrap();
    assert!(matches!(
        store.reserve(RequestId::from_u128(1002)),
        Err(ManagedStoreError::Capacity)
    ));
    let before_ack = read(&runtime, &harness, RequestStreamQuery::Slot { slot: 0 });
    assert!(
        matches!(
            before_ack.result,
            RequestStreamReadResult::Slot(RequestStreamState::Active {
                revision: 1,
                acknowledged_through: 0,
                ..
            })
        ),
        "ordinary outcomes do not change the control CAS revision"
    );

    let acknowledgment = store
        .prepare_acknowledgment(RequestId::from_u128(2002), 2)
        .unwrap();
    let RequestStreamCommand::Acknowledge {
        through, receipts, ..
    } = &acknowledgment.command
    else {
        panic!("ack")
    };
    assert_eq!(*through, 2);
    assert_eq!(
        receipts,
        &vec![
            ManagedReceiptAck {
                key: original_domain.key,
                receipt_hash: original_domain.content_hash().unwrap()
            },
            ManagedReceiptAck {
                key: opened.receipt.key,
                receipt_hash: opened.receipt.content_hash().unwrap()
            },
        ]
    );
    let original_ack = control(&runtime, &harness, &acknowledgment);
    drop(store);
    harness.drop_without_checkpoint();
    let node = EmbeddedNode::open(&settings).unwrap();
    assert_eq!(node.session.sequence(), original_domain.sequence);
    assert_eq!(
        node.session.cursor(CONSUMER).unwrap().token.position,
        cursor_delivery.acknowledged.position,
        "request-history ACK must never acknowledge consumer delta progress"
    );
    let harness = Harness::start(node);
    let store = ManagedOperationStore::open(&store_root, context, limits()).unwrap();
    assert_eq!(store.status().unwrap().retired_through, 0);
    assert_eq!(
        store.pending_control().unwrap(),
        Some(acknowledgment.clone())
    );
    let retried_ack = control(&runtime, &harness, &acknowledgment);
    assert_eq!(retried_ack, original_ack);
    store.record_control(retried_ack).unwrap();
    assert_eq!(store.status().unwrap().retired_through, 2);
    assert!(!store_root.join("0000000000000001.prepared").exists());
    assert!(!store_root.join("0000000000000002.receipt").exists());
    assert!(matches!(
        store.prepare(domain, domain_intent, |_| panic!(
            "retired identity expanded"
        )),
        Err(ManagedStoreError::Retired)
    ));
    for (request, key) in [
        (exact_domain_request.clone(), original_domain.key),
        (cursor_request, opened.receipt.key),
    ] {
        assert!(matches!(
            runtime.block_on(harness.client.submit_managed(request, context)),
            Err(ClientError::Access(AccessError::ManagedRetired {
                through: 2
            }))
        ));
        assert!(matches!(
            read(&runtime, &harness, RequestStreamQuery::Receipt { key }).result,
            RequestStreamReadResult::Receipt {
                resolution: ManagedReceiptResolution::Retired { through: 2 },
                ..
            }
        ));
    }
    assert_legacy(&runtime, &harness, &legacy);
    assert_eq!(store.stop_issuance().unwrap(), 2);
    let close = store.prepare_close(RequestId::from_u128(2003)).unwrap();
    store
        .record_control(control(&runtime, &harness, &close))
        .unwrap();
    assert!(store.status().unwrap().closed);
    harness.checkpoint_and_stop(&runtime);

    let node = EmbeddedNode::open(&settings).unwrap();
    assert_eq!(node.session.sequence(), original_domain.sequence);
    assert_eq!(
        node.session.cursor(CONSUMER).unwrap().token.position,
        Position::origin(context.ledger)
    );
    let harness = Harness::start(node);
    assert_legacy(&runtime, &harness, &legacy);
    assert!(matches!(
        read(
            &runtime,
            &harness,
            RequestStreamQuery::Receipt {
                key: original_domain.key
            }
        )
        .result,
        RequestStreamReadResult::Receipt {
            resolution: ManagedReceiptResolution::StreamClosed { generation: 1 },
            ..
        }
    ));
    let next = ManagedOperationStore::create(
        temp.path().join("next"),
        context,
        limits(),
        registration(context, 1, 2010),
    )
    .unwrap();
    next.record_registration(control(&runtime, &harness, &next.registration().unwrap()))
        .unwrap();
    assert!(matches!(
        next.request(domain),
        Err(ManagedStoreError::Retired)
    ));
    assert!(matches!(
        runtime.block_on(harness.client.submit_managed(exact_domain_request, context)),
        Err(ClientError::Access(AccessError::ManagedClosed {
            generation: 1
        }))
    ));
    let next_id = next.reserve(RequestId::from_u128(1000)).unwrap();
    assert_eq!(next_id.key(context).ordinal, 1);
    assert_eq!(next_id.key(context).stream.generation, 2);
    let prepared = next
        .prepare(
            next_id,
            OperationIntent {
                name: "claim.post",
                version: 1,
                canonical: b"post existing claim",
            },
            |key| {
                Ok(envelope(
                    context,
                    key.id,
                    Operation::Managed {
                        key,
                        operation: ManagedOperation::Submit {
                            expected_revision: None,
                            command: Command::PostClaim { claim: CLAIM },
                        },
                    },
                ))
            },
        )
        .unwrap();
    let posted = runtime
        .block_on(harness.client.submit_managed(prepared.request, context))
        .unwrap()
        .receipt;
    assert_eq!(posted.sequence, SessionSeq(original_domain.sequence.0 + 1));
    assert!(matches!(
        posted.outcome,
        ManagedReceiptOutcome::Domain(CommandResult::Claim {
            claim: CLAIM,
            status: ClaimStatus::Posted
        })
    ));
    next.record_receipt(next_id, &posted).unwrap();
    assert_legacy(&runtime, &harness, &legacy);
    harness.checkpoint_and_stop(&runtime);
}
