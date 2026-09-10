use super::*;
use focal_consensus::{DurableNode, NodeConfig};
use focal_ledger::SessionLimits;
use focal_log::{
    LogicalLogId, Record, RecordKind, SharedWal, WalIdentity, WalLease, WalOptions, WalWriterLimits,
};

const CLUSTER: [u8; 16] = [119; 16];
fn ledger(index: u128) -> LedgerId {
    LedgerId {
        tenant: TenantId::from_u128(119),
        session: SessionId::from_u128(index),
    }
}
fn peer() -> AuthenticatedPeer {
    AuthenticatedPeer::local(PeerGrant {
        principal: ParticipantId::from_u128(119),
        tenants: [ledger(1).tenant].into_iter().collect(),
        role: PeerRole::Actor,
    })
    .unwrap()
}
fn envelope(index: u128, operation: Operation) -> RequestEnvelope {
    RequestEnvelope {
        protocol: PROTOCOL_VERSION,
        ledger: ledger(index),
        route_epoch: RouteEpoch(1),
        request_epoch: RequestEpoch(1),
        request_id: RequestId::from_u128(1),
        operation,
    }
}
fn read(index: u128) -> RequestEnvelope {
    envelope(
        index,
        Operation::Read(ReadRequest {
            consistency: ReadConsistency::Linearizable,
            query: ReadQuery::Objects(Vec::new()),
            max_items: 1,
        }),
    )
}
struct Fixture {
    hosts: std::collections::BTreeMap<LedgerId, ReplicaHost>,
    owner: ReplicaOwner,
    outgoing: FleetReplication,
    wal: SharedWal,
    wal_budget: MemoryBudget,
    budget: MemoryBudget,
}
fn fixture(path: &std::path::Path, count: u128) -> Fixture {
    let budget = MemoryBudget::new(512 * 1024 * 1024, 128 * 1024 * 1024).unwrap();
    let tenant = budget.child(256 * 1024 * 1024, 64 * 1024 * 1024).unwrap();
    let wal_budget = budget.child(128 * 1024 * 1024, 32 * 1024 * 1024).unwrap();
    let wal = SharedWal::open_with_budget(
        path,
        WalOptions::new(WalIdentity {
            cluster: CLUSTER,
            node: 1,
            stream: 0,
        }),
        WalWriterLimits::default(),
        wal_budget.clone(),
    )
    .unwrap();
    let replicas = (1..=count)
        .map(|index| {
            let node = DurableNode::open_on_wal_in(
                NodeConfig::single(1, CLUSTER, ledger(index).session.0),
                wal.clone(),
                &tenant,
            )
            .unwrap();
            let mut session =
                Session::from_node_in(ledger(index), node, SessionLimits::default(), &tenant)
                    .unwrap();
            session.campaign().unwrap();
            for _ in 0..3 {
                session.poll().unwrap();
            }
            let mut config = ReplicaConfig::new(RootCommandId::from_u128(119));
            config.tick = Duration::from_secs(1);
            config.request_timeout = Duration::from_millis(500);
            FleetReplica { session, config }
        })
        .collect();
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
        wal,
        wal_budget,
        budget,
    }
}
async fn settle(fixture: &Fixture) {
    for (id, host) in &fixture.hosts {
        let result = dispatch(
            host,
            peer(),
            RequestEnvelope {
                ledger: *id,
                ..read(1)
            },
            &ReplicaHost::wire_limits(),
        )
        .await;
        assert!(matches!(result.result, Response::Read(_)), "{result:?}");
    }
}
fn blocker(wal: &SharedWal) -> WalLease {
    let mut lease = wal.lease(LogicalLogId([250; 16])).unwrap();
    lease
        .append(
            &(1..=3)
                .map(|index| Record {
                    log: LogicalLogId([250; 16]),
                    kind: RecordKind::Entry,
                    index,
                    term: 1,
                    payload: vec![1],
                })
                .collect::<Vec<_>>(),
        )
        .unwrap();
    lease
}
// Bounded replay backpressure holds the actual disk writer; no artificial
// append completion or successful durability acknowledgment is substituted.
fn pause(lease: WalLease) -> (mpsc::SyncSender<()>, JoinHandle<WalLease>) {
    let (entered, entry) = mpsc::sync_channel(1);
    let (resume, resumed) = mpsc::sync_channel(1);
    let worker = std::thread::spawn(move || {
        let mut first = true;
        lease
            .replay(|_| {
                if first {
                    first = false;
                    entered.send(()).unwrap();
                    resumed.recv().unwrap();
                }
                Ok(())
            })
            .unwrap();
        lease
    });
    entry.recv().unwrap();
    (resume, worker)
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
    drop(fixture.wal);
    assert_eq!(fixture.budget.stats().used, 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shared_owner_queues_covering_flush_and_serves_another_group_while_disk_waits() {
    let path = tempfile::tempdir().unwrap();
    let fixture = fixture(path.path(), 4);
    settle(&fixture).await;
    let lease = blocker(&fixture.wal);
    let before = fixture.wal.stats().unwrap();
    let (resume, disk) = pause(lease);
    let mut observations = Vec::new();
    let mut writes = Vec::new();
    for index in 1..=3 {
        let host = fixture.hosts[&ledger(index)].clone();
        let mut observation = host.progress.clone();
        observation.borrow_and_update();
        observations.push(observation);
        writes.push(tokio::spawn(async move {
            dispatch(
                &host,
                peer(),
                envelope(
                    index,
                    Operation::OpenEpoch {
                        epoch: RequestEpoch(1),
                    },
                ),
                &ReplicaHost::wire_limits(),
            )
            .await
        }));
    }
    let independent = tokio::time::timeout(Duration::from_millis(250), async {
        // Every group has reached its nonblocking drain and published progress;
        // the physical writer is still stopped before all of their fences.
        for observation in &mut observations {
            observation.changed().await.unwrap();
        }
        dispatch(
            &fixture.hosts[&ledger(4)],
            peer(),
            read(4),
            &ReplicaHost::wire_limits(),
        )
        .await
    })
    .await;
    let all_unacknowledged = writes.iter().all(|write| !write.is_finished());
    resume.send(()).unwrap();
    drop(disk.join().unwrap());
    // Every request commits exactly once. A write whose first answer was
    // not its commit (the owner refused it for capacity under load, or
    // reported it accepted before its fence) is resent as the exact same
    // request, which finds the committed outcome by identity or commits
    // it once; the assertion is never loosened to accept anything else.
    let mut committed = true;
    for (position, write) in writes.into_iter().enumerate() {
        let index = position as u128 + 1;
        let mut answer = write.await.unwrap().result;
        for _ in 0..40 {
            if matches!(answer, Response::Submitted(MutationReply::Committed(_))) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
            answer = dispatch(
                &fixture.hosts[&ledger(index)],
                peer(),
                envelope(
                    index,
                    Operation::OpenEpoch {
                        epoch: RequestEpoch(1),
                    },
                ),
                &ReplicaHost::wire_limits(),
            )
            .await
            .result;
        }
        committed &= matches!(answer, Response::Submitted(MutationReply::Committed(_)));
    }
    let after = fixture.wal.stats().unwrap();
    let coalesced = after.group_commits - before.group_commits < 6;
    shutdown(fixture).await;
    assert!(matches!(independent.unwrap().result, Response::Read(_)));
    assert!(all_unacknowledged);
    assert!(committed);
    assert!(
        coalesced,
        "the three initial Ready batches did not share a covering flush"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn host_stop_reaches_retained_ready_and_expires_unknown_under_pinned_wal_budget() {
    let path = tempfile::tempdir().unwrap();
    let fixture = fixture(path.path(), 2);
    settle(&fixture).await;
    let pressure = fixture
        .wal_budget
        .reserve(
            BudgetKind::Payload,
            BudgetLane::Completion,
            fixture.wal_budget.stats().limit - fixture.wal_budget.stats().used,
        )
        .unwrap();
    let host = fixture.hosts[&ledger(1)].clone();
    let mut observation = host.progress.clone();
    observation.borrow_and_update();
    let write = tokio::spawn(async move {
        dispatch(
            &host,
            peer(),
            envelope(
                1,
                Operation::OpenEpoch {
                    epoch: RequestEpoch(1),
                },
            ),
            &ReplicaHost::wire_limits(),
        )
        .await
    });
    tokio::time::timeout(Duration::from_millis(250), observation.changed())
        .await
        .unwrap()
        .unwrap();
    let stopped =
        tokio::time::timeout(Duration::from_secs(2), fixture.hosts[&ledger(1)].stop()).await;
    let result = write.await.unwrap();
    drop(pressure);
    let other = dispatch(
        &fixture.hosts[&ledger(2)],
        peer(),
        read(2),
        &ReplicaHost::wire_limits(),
    )
    .await;
    shutdown(fixture).await;
    assert!(matches!(stopped.unwrap(), Err(LedgerError::OutcomeUnknown)));
    assert!(matches!(
        result.result,
        Response::Error(AccessError::OutcomeUnknown)
    ));
    assert!(matches!(other.result, Response::Read(_)));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stopping_last_session_on_a_stalled_writer_does_not_join_it_on_the_fleet_worker() {
    let path = tempfile::tempdir().unwrap();
    let budget = MemoryBudget::new(512 * 1024 * 1024, 128 * 1024 * 1024).unwrap();
    let tenant = budget.child(256 * 1024 * 1024, 64 * 1024 * 1024).unwrap();
    let mut replicas = Vec::new();
    let mut to_pause = None;
    for index in 1..=2 {
        let wal = SharedWal::open_with_budget(
            path.path().join(index.to_string()),
            WalOptions::new(WalIdentity {
                cluster: CLUSTER,
                node: 1,
                stream: 0,
            }),
            WalWriterLimits::default(),
            budget.child(64 * 1024 * 1024, 16 * 1024 * 1024).unwrap(),
        )
        .unwrap();
        let node = DurableNode::open_on_wal_in(
            NodeConfig::single(1, CLUSTER, ledger(index).session.0),
            wal.clone(),
            &tenant,
        )
        .unwrap();
        let mut session =
            Session::from_node_in(ledger(index), node, SessionLimits::default(), &tenant).unwrap();
        session.campaign().unwrap();
        for _ in 0..3 {
            session.poll().unwrap();
        }
        let mut config = ReplicaConfig::new(RootCommandId::from_u128(119));
        config.tick = Duration::from_secs(1);
        config.request_timeout = Duration::from_millis(400);
        replicas.push(FleetReplica { session, config });
        if index == 1 {
            to_pause = Some(wal);
        }
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
    for index in 1..=2 {
        assert!(matches!(
            dispatch(
                &hosts[&ledger(index)],
                peer(),
                read(index),
                &ReplicaHost::wire_limits()
            )
            .await
            .result,
            Response::Read(_)
        ));
    }
    let wal = to_pause.take().unwrap();
    let paused = wal.pause_for_test().unwrap();
    drop(wal); // No fixture, replay lease, or returned pause guard owns the writer.
    let host = hosts[&ledger(1)].clone();
    let mut observation = host.progress.clone();
    observation.borrow_and_update();
    let write = tokio::spawn(async move {
        dispatch(
            &host,
            peer(),
            envelope(
                1,
                Operation::OpenEpoch {
                    epoch: RequestEpoch(1),
                },
            ),
            &ReplicaHost::wire_limits(),
        )
        .await
    });
    let pending = tokio::time::timeout(Duration::from_millis(250), observation.changed()).await;
    let stopped = tokio::time::timeout(Duration::from_secs(2), hosts[&ledger(1)].stop()).await;
    let other = tokio::time::timeout(
        Duration::from_millis(250),
        dispatch(
            &hosts[&ledger(2)],
            peer(),
            read(2),
            &ReplicaHost::wire_limits(),
        ),
    )
    .await;
    paused.resume().unwrap();
    let write = write.await.unwrap();
    for host in hosts.values() {
        if !host.progress().stopped {
            host.stop().await.unwrap();
        }
    }
    owner.join().unwrap();
    drop(hosts);
    drop(outgoing);
    assert!(pending.is_ok());
    assert!(matches!(stopped.unwrap(), Err(LedgerError::OutcomeUnknown)));
    assert!(matches!(other.unwrap().result, Response::Read(_)));
    assert!(matches!(
        write.result,
        Response::Error(AccessError::OutcomeUnknown)
    ));
    assert_eq!(budget.stats().used, 0);
}
