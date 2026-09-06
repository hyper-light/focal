#![allow(
    clippy::panic,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros
)]
use focal_client::{Client, EmbeddedTransport, RetryPolicy, pending::OperationContext, watch::*};
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
