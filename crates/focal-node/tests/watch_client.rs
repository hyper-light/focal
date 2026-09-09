#![allow(
    clippy::panic,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros
)]
use focal_client::{Client, EmbeddedTransport, RetryPolicy, pending::OperationContext, watch::*};
use focal_core::native::{NativeCommand, NativeInput, input_codec};
use focal_ledger::NativeContentProfile;
use focal_model::lifecycle::{
    Binding, Principal, aggregation, claim::ClaimDefinition, creation::Proposal, graph, scope,
    succession::Lineage, validation,
};
use focal_model::*;
use focal_node::{
    config::Settings,
    embedded::EmbeddedNode,
    host::{HostOwner, LocalHost},
};
use focal_wire::*;
use std::{collections::BTreeSet, os::unix::fs::PermissionsExt};
fn random() -> Result<[u8; 16], focal_client::input::InputError> {
    let mut bytes = [0; 16];
    getrandom::fill(&mut bytes).unwrap();
    Ok(bytes)
}
struct Fixture {
    client: Client<EmbeddedTransport<LocalHost>>,
    host: LocalHost,
    owner: HostOwner,
    context: OperationContext,
    worker: ParticipantId,
}
impl Fixture {
    fn start(root: &std::path::Path) -> Self {
        let mut settings = Settings::default();
        settings.node.data_dir = Some(root.into());
        let node = EmbeddedNode::open(&settings).unwrap();
        let context = OperationContext {
            cluster: node.identity.cluster,
            ledger: node.identity.ledger,
            principal: node.identity.issuer,
        };
        let worker = node.identity.worker;
        let limits = WireLimits::default();
        let (host, owner) = LocalHost::spawn(node, limits.clone()).unwrap();
        let peer = AuthenticatedPeer::local(PeerGrant {
            principal: context.principal,
            tenants: BTreeSet::from([context.ledger.tenant]),
            role: PeerRole::Runtime,
        })
        .unwrap();
        let client = Client::new(
            EmbeddedTransport::new(peer, host.clone(), limits.clone()).unwrap(),
            RetryPolicy::default(),
            limits,
            1,
        )
        .unwrap();
        Self {
            client,
            host,
            owner,
            context,
            worker,
        }
    }
    fn stop(self) {
        drop(self.client);
        drop(self.host);
        self.owner.join().unwrap();
    }
}
fn delivery(
    runtime: &tokio::runtime::Runtime,
    client: &Client<EmbeddedTransport<LocalHost>>,
    journal: &mut WatchJournal,
) -> WatchDelivery {
    for _ in 0..64 {
        match journal.next_action(&mut random).unwrap() {
            WatchAction::Delivery => return journal.delivery().unwrap().clone(),
            WatchAction::Request(action) => {
                let reply = runtime
                    .block_on(client.request(action.request.clone()))
                    .unwrap_or_else(|error| {
                        panic!("watch request {:?}: {error:?}", action.request.operation)
                    });
                journal.accept(action, reply).unwrap();
            }
        }
    }
    panic!("watch did not reach delivery")
}
#[test]
fn lost_delivery_and_lost_ack_recover_exactly_without_advancing_unconsumed_data() {
    let root = tempfile::tempdir().unwrap();
    let local = tempfile::tempdir().unwrap();
    std::fs::set_permissions(local.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let fixture = Fixture::start(root.path());
    let store = WatchStore::open(local.path(), fixture.context).unwrap();
    let mut watch = store
        .create(
            "changes",
            WatchOptions {
                seed: false,
                max_items: 1,
                ..Default::default()
            },
        )
        .unwrap();
    let first = delivery(&runtime, &fixture.client, &mut watch);
    assert_eq!(first.number, 1);
    assert_eq!(watch.status().acknowledged, 0);
    assert!(matches!(
        watch.acknowledge(ContentHash([1; 32])),
        Err(WatchError::DeliveryMismatch)
    ));
    drop(watch);
    drop(store);
    let context = fixture.context;
    fixture.stop();
    let fixture = Fixture::start(root.path());
    let store = WatchStore::open(local.path(), context).unwrap();
    let mut watch = store.resume("changes").unwrap();
    assert_eq!(watch.delivery(), Some(&first));
    assert!(matches!(
        watch.next_action(&mut random).unwrap(),
        WatchAction::Delivery
    ));
    watch.acknowledge(first.id).unwrap();
    drop(watch);
    let mut watch = store.resume("changes").unwrap();
    watch.acknowledge(first.id).unwrap();
    assert_eq!(watch.status().acknowledged, 1);
    let next = delivery(&runtime, &fixture.client, &mut watch);
    assert_eq!(next.number, 2);
    assert_ne!(next.id, first.id);
    // More than the four-slot window can pass only after each consumed page's
    // exact receipt is retired. There is no manual epoch/floor ceremony.
    for _ in 0..12 {
        let id = watch.delivery().unwrap().id;
        watch.acknowledge(id).unwrap();
        let _ = delivery(&runtime, &fixture.client, &mut watch);
    }
    drop(watch);
    fixture.stop();
}
#[test]
fn lost_network_reply_keeps_exact_request_and_missing_journal_cannot_recreate() {
    let root = tempfile::tempdir().unwrap();
    let local = tempfile::tempdir().unwrap();
    std::fs::set_permissions(local.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let fixture = Fixture::start(root.path());
    let store = WatchStore::open(local.path(), fixture.context).unwrap();
    let mut watch = store
        .create(
            "unknown",
            WatchOptions {
                seed: false,
                ..Default::default()
            },
        )
        .unwrap();
    let original = loop {
        let WatchAction::Request(action) = watch.next_action(&mut random).unwrap() else {
            panic!("request expected")
        };
        let reply = runtime
            .block_on(fixture.client.request(action.request.clone()))
            .unwrap();
        if matches!(action.request.operation, Operation::Managed { .. }) {
            break action.request;
        }
        watch.accept(action, reply).unwrap();
    };
    drop(watch);
    let mut watch = store.resume("unknown").unwrap();
    let WatchAction::Request(action) = watch.next_action(&mut random).unwrap() else {
        panic!("pending expected")
    };
    assert_eq!(action.request, original);
    let reply = runtime
        .block_on(fixture.client.request(action.request.clone()))
        .unwrap();
    watch.accept(action, reply).unwrap();
    assert!(watch.delivery().is_some());
    drop(watch);
    let body = std::fs::read_dir(local.path())
        .unwrap()
        .map(|e| e.unwrap().path())
        .find(|p| {
            p.file_name()
                .unwrap()
                .to_string_lossy()
                .starts_with("watch-")
                && p.extension().is_some_and(|e| e == "watch-owner")
        })
        .unwrap();
    std::fs::remove_file(body).unwrap();
    assert!(store.resume("unknown").is_err());
    fixture.stop();
}

#[test]
fn seed_pages_survive_client_restart_and_expired_source_never_skips_to_tail() {
    let root = tempfile::tempdir().unwrap();
    let local = tempfile::tempdir().unwrap();
    std::fs::set_permissions(local.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
    let mut settings = Settings::default();
    settings.node.data_dir = Some(root.path().into());
    let mut node = EmbeddedNode::open(&settings).unwrap();
    let report = focal_node::demo::run(&mut node).unwrap();
    drop(node);
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let fixture = Fixture::start(root.path());
    let store = WatchStore::open(local.path(), fixture.context).unwrap();
    let options = WatchOptions {
        claims: vec![report.claim],
        max_items: 1,
        ..Default::default()
    };
    let mut watch = store.create("seed", options).unwrap();
    let first = delivery(&runtime, &fixture.client, &mut watch);
    let WatchPage::Seed { page } = &first.page else {
        panic!("seed")
    };
    let token = page.token;
    assert!(page.next.is_some());
    watch.acknowledge(first.id).unwrap();
    drop(watch);
    let mut watch = store.resume("seed").unwrap();
    let second = delivery(&runtime, &fixture.client, &mut watch);
    let WatchPage::Seed { page } = &second.page else {
        panic!("seed")
    };
    assert_eq!(page.token, token);
    assert!(page.next.is_some());
    drop(watch);
    fixture.stop();
    let fixture = Fixture::start(root.path());
    let mut watch = store.resume("seed").unwrap();
    assert_eq!(watch.delivery(), Some(&second));
    watch.acknowledge(second.id).unwrap();
    let mut expired = false;
    for _ in 0..32 {
        let WatchAction::Request(action) = watch.next_action(&mut random).unwrap() else {
            panic!("expired seed must not invent data")
        };
        let response = runtime.block_on(fixture.client.request(action.request.clone()));
        match response {
            Ok(response) => match watch.accept(action, response) {
                Ok(()) => {}
                Err(WatchError::Remote(_)) => {
                    expired = true;
                    break;
                }
                Err(error) => panic!("{error}"),
            },
            Err(focal_client::ClientError::Access(_)) => {
                assert!(
                    matches!(action.request.operation,Operation::Read(ReadRequest{consistency:ReadConsistency::Exact(actual),..}) if actual==token)
                );
                expired = true;
                break;
            }
            Err(error) => panic!("{error}"),
        }
    }
    assert!(expired);
    assert!(watch.status().seeding);
    assert!(watch.status().pending);
    assert_eq!(watch.status().acknowledged, 2);
    drop(watch);
    fixture.stop();
}

#[test]
fn large_seed_rows_use_server_byte_budget_and_all_continuations_finish() {
    let root = tempfile::tempdir().unwrap();
    let local = tempfile::tempdir().unwrap();
    std::fs::set_permissions(local.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
    let mut settings = Settings::default();
    settings.node.data_dir = Some(root.path().into());
    let mut node = EmbeddedNode::open(&settings).unwrap();
    focal_node::demo::run(&mut node).unwrap();
    for index in 0..6u128 {
        let artifact = NewArtifact {
            id: ArtifactId::from_u128(80000 + index),
            content: ArtifactContent {
                ledger: node.identity.ledger,
                schema: SCHEMA_MAJOR,
                kind: format!("large-seed-row-{index}"),
                schema_hash: ContentHash([94; 32]),
                metadata: vec![1; 15000],
                payload: ArtifactPayload::Inline(vec![2; 15000]),
                producer: node.identity.issuer,
                receipt: None,
                inputs: BTreeSet::new(),
                visibility: BTreeSet::new(),
            },
        };
        let hash = artifact.content.content_hash().unwrap();
        let input = AuthenticatedInput {
            ledger: node.identity.ledger,
            principal: node.identity.issuer,
            request_epoch: RequestEpoch(1),
            request_id: RequestId::from_u128(90000 + index),
            expected_revision: None,
            authority: AuthorityContext {
                runtime: true,
                cause: Cause::Root(node.identity.root),
                policy_revision: 1,
                logical_time: 0,
                evidence: vec![EvidenceAttestation {
                    descriptor_hash: hash,
                    custody_revision: 1,
                    durable: true,
                    schema_valid: true,
                }],
            },
            command: Command::RegisterArtifact { artifact },
        };
        let result = node.session.submit_local(&input).unwrap();
        assert!(
            matches!(result, focal_ledger::Submission::Committed(_)),
            "{result:?}"
        );
    }
    drop(node);
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let fixture = Fixture::start(root.path());
    let store = WatchStore::open(local.path(), fixture.context).unwrap();
    let mut watch = store.create("large", WatchOptions::default()).unwrap();
    let mut count = 0;
    let mut pages = 0;
    loop {
        let item = delivery(&runtime, &fixture.client, &mut watch);
        assert!(
            postcard::experimental::serialized_size(&item.page).unwrap() <= MAX_WATCH_PAGE_BYTES
        );
        let WatchPage::Seed { page } = &item.page else {
            break;
        };
        count += page.objects.len();
        pages += 1;
        watch.acknowledge(item.id).unwrap();
        if pages == 1 {
            drop(watch);
            watch = store.resume("large").unwrap();
        }
    }
    assert_eq!(count, 11);
    assert!(pages >= 3);
    drop(watch);
    fixture.stop();
}

// Native claim frames, shaped as in the node's own native host tests.
fn native_binding(ledger: LedgerId, id: u128) -> Binding {
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
    let binding = native_binding(ledger, id);
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

fn create(
    ledger: LedgerId,
    issuer: ParticipantId,
    worker: ParticipantId,
    request: u128,
    id: u128,
) -> NativeInput {
    let proposals = vec![proposal(ledger, issuer, worker, id)];
    let declarations = proposals
        .iter()
        .map(|p| definition(issuer, p.definition.binding))
        .collect();
    NativeInput {
        request: RequestKey {
            principal: issuer,
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
/// One committed native creation through the in-process client.
fn commit_native(
    runtime: &tokio::runtime::Runtime,
    fixture: &Fixture,
    request: u128,
    claim: u128,
) -> NativeReceipt {
    let ledger = fixture.context.ledger;
    let input = create(
        ledger,
        fixture.context.principal,
        fixture.worker,
        request,
        claim,
    );
    let envelope = RequestEnvelope {
        protocol: NATIVE_PROTOCOL_VERSION,
        ledger,
        route_epoch: RouteEpoch(1),
        request_epoch: RequestEpoch(1),
        request_id: RequestId::from_u128(request),
        operation: Operation::Native {
            frame: frame(ledger, &input),
        },
    };
    match runtime
        .block_on(fixture.client.request(envelope))
        .unwrap()
        .result
    {
        Response::Native(NativeMutationReply::Committed(receipt)) => receipt,
        other => panic!("native creation: {other:?}"),
    }
}
#[test]
fn native_watches_seed_through_native_reads_and_stream_schema_two_deltas_in_process() {
    let root = tempfile::tempdir().unwrap();
    let local = tempfile::tempdir().unwrap();
    std::fs::set_permissions(local.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let mut settings = Settings::default();
    settings.node.data_dir = Some(root.path().into());
    focal_node::native_activation::activate_local(&settings, NativeContentProfile::ProjectionOnly)
        .unwrap();
    let fixture = Fixture::start(root.path());
    let first = commit_native(&runtime, &fixture, 1, 100);
    assert_eq!(first.sequence, SessionSeq(1));
    let store = WatchStore::open(local.path(), fixture.context).unwrap();

    // A seeded claim watch: the claim is read with its evaluations at a
    // prefix no older than the snapshot, then the seed completes and the
    // tail polls.
    let mut seeded = store
        .create(
            "seeded",
            WatchOptions {
                engine: WatchEngine::Native,
                claims: vec![ClaimId::from_u128(100)],
                seed: true,
                max_items: 8,
                ..Default::default()
            },
        )
        .unwrap();
    let page = delivery(&runtime, &fixture.client, &mut seeded);
    let WatchPage::NativeSeed {
        token,
        objects,
        next,
    } = &page.page
    else {
        panic!("native seed page: {page:?}");
    };
    assert_eq!(*next, NativeSeedNext::Complete);
    assert!(token.sequence >= SessionSeq(1));
    assert!(
        objects.iter().any(|object| matches!(object, NativeObject::Claim(claim) if claim.binding.object == ObjectId::from_u128(100))),
        "{objects:?}"
    );
    assert!(seeded.status().seeding);
    seeded.acknowledge(page.id).unwrap();
    let tail = delivery(&runtime, &fixture.client, &mut seeded);
    let WatchPage::Events { page: events } = &tail.page else {
        panic!("tail page: {tail:?}");
    };
    assert!(
        events
            .events
            .iter()
            .all(|event| !matches!(event, StreamEvent::Delta { .. })),
        "the seed's own facts are not replayed: {events:?}"
    );
    assert!(!seeded.status().seeding);
    seeded.acknowledge(tail.id).unwrap();

    // A second record: the seeded watch receives only its claim's facts and
    // an unseeded watch of everything replays both records from the origin.
    let second = commit_native(&runtime, &fixture, 2, 101);
    assert_eq!(second.sequence, SessionSeq(2));
    let mut everything = store
        .create(
            "everything",
            WatchOptions {
                engine: WatchEngine::Native,
                seed: false,
                max_items: 64,
                ..Default::default()
            },
        )
        .unwrap();
    let history = delivery(&runtime, &fixture.client, &mut everything);
    let WatchPage::Events { page: events } = &history.page else {
        panic!("history page: {history:?}");
    };
    let deltas: Vec<&Delta> = events
        .events
        .iter()
        .filter_map(|event| match event {
            StreamEvent::Delta { delta, .. } => Some(delta),
            _ => None,
        })
        .collect();
    assert!(
        deltas
            .iter()
            .all(|delta| delta.schema == NATIVE_DELTA_SCHEMA),
        "{deltas:?}"
    );
    assert!(
        deltas
            .iter()
            .all(|delta| delta.actor == fixture.context.principal)
    );
    let creations: Vec<ClaimId> = deltas
        .iter()
        .filter(|delta| {
            matches!(
                &delta.fact,
                DeltaFact::Native(record) if matches!(
                    record.fact,
                    NativeFactRecord::Claim(NativeClaimEventRecord { kind: NativeEventKindRecord::Created, .. })
                )
            )
        })
        .filter_map(|delta| delta.claim)
        .collect();
    assert_eq!(
        creations,
        [ClaimId::from_u128(100), ClaimId::from_u128(101)],
        "{deltas:?}"
    );
    assert!(
        deltas
            .iter()
            .any(|delta| delta.id.sequence == SessionSeq(1))
    );
    assert!(
        deltas
            .iter()
            .any(|delta| delta.id.sequence == SessionSeq(2))
    );
    for delta in &deltas {
        let DeltaFact::Native(record) = &delta.fact else {
            panic!("{delta:?}");
        };
        assert_eq!(record.sequence, delta.id.sequence);
        assert_eq!(record.ordinal, delta.id.ordinal);
    }
    everything.acknowledge(history.id).unwrap();
    let after = delivery(&runtime, &fixture.client, &mut seeded);
    let WatchPage::Events { page: events } = &after.page else {
        panic!("{after:?}");
    };
    assert!(
        events
            .events
            .iter()
            .all(|event| !matches!(event, StreamEvent::Delta { .. })),
        "the claim filter keeps another claim's creation out: {events:?}"
    );
    seeded.acknowledge(after.id).unwrap();

    // A watch of a family seeds through its list and restarts intact.
    let mut definitions = store
        .create(
            "definitions",
            WatchOptions {
                engine: WatchEngine::Native,
                family: Some(ObjectKind::Validation),
                max_items: 8,
                ..Default::default()
            },
        )
        .unwrap();
    let listed = delivery(&runtime, &fixture.client, &mut definitions);
    let WatchPage::NativeSeed { objects, next, .. } = &listed.page else {
        panic!("{listed:?}");
    };
    assert_eq!(*next, NativeSeedNext::Complete);
    assert_eq!(objects.len(), 2, "{objects:?}");
    assert!(
        objects
            .iter()
            .all(|object| matches!(object, NativeObject::Definition(_)))
    );
    drop(definitions);
    drop(seeded);
    drop(everything);
    drop(store);
    let context = fixture.context;
    fixture.stop();
    let fixture = Fixture::start(root.path());
    let store = WatchStore::open(local.path(), context).unwrap();
    let mut definitions = store.resume("definitions").unwrap();
    assert_eq!(definitions.delivery(), Some(&listed));
    definitions.acknowledge(listed.id).unwrap();
    let tail = delivery(&runtime, &fixture.client, &mut definitions);
    assert!(matches!(tail.page, WatchPage::Events { .. }), "{tail:?}");
    let mut everything = store.resume("everything").unwrap();
    let again = delivery(&runtime, &fixture.client, &mut everything);
    assert!(matches!(again.page, WatchPage::Events { .. }));
    drop(definitions);
    drop(everything);
    fixture.stop();
}
