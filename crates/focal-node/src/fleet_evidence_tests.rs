use super::*;
use focal_consensus::{DurableNode, NodeConfig};
use focal_directory::{
    DurabilityIntent, FailureClass, Placement, PlacementPolicy, PlacementSpec, SessionFenceKind,
};
use focal_ledger::{SessionLimits, SessionPlacementRequest};
use focal_log::{SharedWal, WalIdentity, WalOptions, WalWriterLimits};

const CLUSTER: [u8; 16] = [147; 16];
fn ledger(index: u128) -> LedgerId {
    LedgerId {
        tenant: TenantId::from_u128(147),
        session: SessionId::from_u128(index),
    }
}
fn peer() -> AuthenticatedPeer {
    AuthenticatedPeer::local(PeerGrant {
        principal: ParticipantId::from_u128(147),
        tenants: [ledger(1).tenant].into_iter().collect(),
        role: PeerRole::Actor,
    })
    .unwrap()
}
fn request(index: u128, operation: Operation) -> RequestEnvelope {
    RequestEnvelope {
        protocol: PROTOCOL_VERSION,
        ledger: ledger(index),
        route_epoch: RouteEpoch(1),
        request_epoch: RequestEpoch(1),
        request_id: RequestId::from_u128(1),
        operation,
    }
}
struct Fixture {
    hosts: std::collections::BTreeMap<LedgerId, ReplicaHost>,
    owner: ReplicaOwner,
    outgoing: FleetReplication,
    wals: Vec<SharedWal>,
    budget: MemoryBudget,
}
fn fixture(path: &std::path::Path) -> Fixture {
    let budget = MemoryBudget::new(768 * 1024 * 1024, 256 * 1024 * 1024).unwrap();
    let tenant = budget.child(384 * 1024 * 1024, 128 * 1024 * 1024).unwrap();
    let mut wals = Vec::new();
    let mut replicas = Vec::new();
    for index in 1..=2 {
        let wal = SharedWal::open_with_budget(
            path.join(index.to_string()),
            WalOptions::new(WalIdentity {
                cluster: CLUSTER,
                node: 1,
                stream: index,
            }),
            WalWriterLimits::default(),
            budget.child(128 * 1024 * 1024, 32 * 1024 * 1024).unwrap(),
        )
        .unwrap();
        let node = DurableNode::open_on_wal_in(
            NodeConfig::single(1, CLUSTER, ledger(u128::from(index)).session.0),
            wal.clone(),
            &tenant,
        )
        .unwrap();
        let mut session = Session::from_node_in(
            ledger(u128::from(index)),
            node,
            SessionLimits::default(),
            &tenant,
        )
        .unwrap();
        session.campaign().unwrap();
        for _ in 0..4 {
            session.poll().unwrap();
        }
        session
            .propose_placement(&SessionPlacementRequest {
                expected_index: 0,
                expected_configuration_index: session.membership().unwrap().configuration_index,
                operation: focal_directory::OperationId::from_u128(147),
                kind: SessionFenceKind::Created,
                from_route: RouteEpoch(0),
                to_route: RouteEpoch(1),
                membership_epoch: 1,
                placement_epoch: 1,
                placement: PlacementSpec {
                    policy: PlacementPolicy {
                        durability: DurabilityIntent {
                            survive: FailureClass::Node,
                            max_failures: 0,
                        },
                        residency: Default::default(),
                        home_regions: Default::default(),
                        required_memory: 0,
                    },
                    placement: Placement {
                        preferred_leader: 1,
                        voters: [(1, 1)].into_iter().collect(),
                        materializers: [(1, 1)].into_iter().collect(),
                        content_copies: [(1, 1)].into_iter().collect(),
                    },
                },
            })
            .unwrap();
        for _ in 0..4 {
            session.poll().unwrap();
        }
        let mut config = ReplicaConfig::new(RootCommandId::from_u128(147));
        config.tick = Duration::from_secs(1);
        config.request_timeout = Duration::from_millis(500);
        replicas.push(FleetReplica { session, config });
        wals.push(wal);
    }
    let (hosts, owner, outgoing) = ReplicaFleet::spawn(
        1,
        replicas,
        vec![FleetTenant {
            tenant: ledger(1).tenant,
            weight: 1,
            budget: tenant,
        }],
        budget.clone(),
        ReplicaHost::wire_limits(),
    )
    .unwrap();
    Fixture {
        hosts,
        owner,
        outgoing,
        wals,
        budget,
    }
}
async fn read(fixture: &Fixture, index: u128) {
    let response = tokio::time::timeout(
        Duration::from_millis(300),
        dispatch(
            &fixture.hosts[&ledger(index)],
            peer(),
            request(
                index,
                Operation::Read(ReadRequest {
                    consistency: ReadConsistency::Linearizable,
                    query: ReadQuery::Objects(Vec::new()),
                    max_items: 1,
                }),
            ),
            &ReplicaHost::wire_limits(),
        ),
    )
    .await
    .unwrap();
    assert!(matches!(response.result, Response::Read(_)), "{response:?}");
}
async fn pending_export(
    fixture: &Fixture,
    ttl: Duration,
) -> tokio::task::JoinHandle<Result<focal_ledger::DurableEvidenceSnapshot, LedgerError>> {
    let host = fixture.hosts[&ledger(1)].clone();
    let mut progress = host.progress.clone();
    progress.borrow_and_update();
    let export = tokio::spawn(async move { host.checkpoint_evidence(ttl).await });
    tokio::time::timeout(Duration::from_millis(250), progress.changed())
        .await
        .unwrap()
        .unwrap();
    assert!(
        !export.is_finished(),
        "paused physical writer must not return a checkpoint witness"
    );
    export
}
async fn shutdown(fixture: Fixture) {
    for host in fixture.hosts.values() {
        if !host.progress().stopped {
            host.stop().await.unwrap();
        }
    }
    fixture.owner.join().unwrap();
    drop(fixture.hosts);
    drop(fixture.outgoing);
    drop(fixture.wals);
    assert_eq!(fixture.budget.stats().used, 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn checkpoint_waits_for_exact_writer_fence_while_another_writer_commits_and_reopens() {
    let directory = tempfile::tempdir().unwrap();
    let fixture = fixture(directory.path());
    read(&fixture, 1).await;
    read(&fixture, 2).await;
    let pause = fixture.wals[0].pause_for_test().unwrap();
    let export = pending_export(&fixture, Duration::from_secs(5)).await;
    let committed = tokio::time::timeout(
        Duration::from_millis(300),
        dispatch(
            &fixture.hosts[&ledger(2)],
            peer(),
            request(
                2,
                Operation::OpenEpoch {
                    epoch: RequestEpoch(1),
                },
            ),
            &ReplicaHost::wire_limits(),
        ),
    )
    .await
    .unwrap();
    assert!(
        matches!(
            committed.result,
            Response::Submitted(MutationReply::Committed(_))
        ),
        "{committed:?}"
    );
    assert!(!export.is_finished());
    pause.resume().unwrap();
    let snapshot = export.await.unwrap().unwrap();
    assert_eq!(snapshot.prefix().ledger, ledger(1));
    assert_eq!(snapshot.prefix().sequence, SessionSeq(0));
    assert_eq!(
        snapshot.prefix().checkpoint,
        ContentHash(*blake3::hash(snapshot.checkpoint()).as_bytes())
    );
    assert!(
        snapshot
            .artifact_after(None, snapshot.elapsed_clock().unwrap())
            .unwrap()
            .is_none()
    );
    let prefix = snapshot.prefix().clone();
    drop(snapshot);
    shutdown(fixture).await;
    let wal = SharedWal::open(
        directory.path().join("1"),
        WalOptions::new(WalIdentity {
            cluster: CLUSTER,
            node: 1,
            stream: 1,
        }),
    )
    .unwrap();
    let node =
        DurableNode::open_on_wal(NodeConfig::single(1, CLUSTER, ledger(1).session.0), wal).unwrap();
    let mut restored = Session::from_node(ledger(1), node, SessionLimits::default()).unwrap();
    restored.poll().unwrap();
    assert_eq!(restored.sequence(), prefix.sequence);
    assert_eq!(restored.status().applied_index, prefix.index.0);
    assert_eq!(restored.placement().unwrap().to_route, prefix.route);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn expired_cancelled_and_timed_out_exports_release_interest_without_releasing_the_disk_gate()
{
    let directory = tempfile::tempdir().unwrap();
    let fixture = fixture(directory.path());
    read(&fixture, 1).await;
    read(&fixture, 2).await;
    let pause = fixture.wals[0].pause_for_test().unwrap();
    let export = pending_export(&fixture, Duration::from_millis(30)).await;
    tokio::time::sleep(Duration::from_millis(60)).await;
    read(&fixture, 2).await;
    pause.resume().unwrap();
    assert!(matches!(
        export.await.unwrap(),
        Err(LedgerError::Graph(focal_graph::GraphError::Memory(
            focal_memory::MemoryError::LeaseExpired
        )))
    ));
    read(&fixture, 1).await;

    let pause = fixture.wals[0].pause_for_test().unwrap();
    let export = pending_export(&fixture, Duration::from_secs(5)).await;
    export.abort();
    assert!(matches!(export.await, Err(error) if error.is_cancelled()));
    read(&fixture, 2).await;
    pause.resume().unwrap();
    read(&fixture, 1).await;

    let pause = fixture.wals[0].pause_for_test().unwrap();
    let export = pending_export(&fixture, Duration::from_secs(5)).await;
    assert!(matches!(
        export.await.unwrap(),
        Err(LedgerError::OutcomeUnknown)
    ));
    read(&fixture, 2).await;
    pause.resume().unwrap();
    read(&fixture, 1).await;
    shutdown(fixture).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn checkpoint_stop_deadline_leaves_other_sessions_available_until_writer_recovery() {
    let directory = tempfile::tempdir().unwrap();
    let fixture = fixture(directory.path());
    read(&fixture, 1).await;
    read(&fixture, 2).await;
    let pause = fixture.wals[0].pause_for_test().unwrap();
    let export = pending_export(&fixture, Duration::from_secs(5)).await;
    let stopped =
        tokio::time::timeout(Duration::from_millis(800), fixture.hosts[&ledger(1)].stop())
            .await
            .unwrap();
    assert!(matches!(stopped, Err(LedgerError::OutcomeUnknown)));
    assert!(matches!(
        export.await.unwrap(),
        Err(LedgerError::OutcomeUnknown)
    ));
    read(&fixture, 2).await;
    pause.resume().unwrap();
    shutdown(fixture).await;
}
