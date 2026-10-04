#![allow(
    clippy::panic,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::unreachable,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros
)]
use focal_consensus::{DurableNode, NodeConfig};
use focal_ledger::{Session, SessionLimits};
use focal_log::{SharedWal, WalIdentity, WalOptions, WalWriterLimits};
use focal_memory::{BudgetKind, BudgetLane, MemoryBudget};
use focal_model::*;
use focal_node::fleet::*;
use focal_wire::*;
use std::{
    collections::BTreeMap,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

const CLUSTER: [u8; 16] = [83; 16];
fn ledger(index: u128) -> LedgerId {
    LedgerId {
        tenant: TenantId::from_u128(800 + index % 2),
        session: SessionId::from_u128(1000 + index),
    }
}
fn actor(ledger: LedgerId) -> AuthenticatedPeer {
    AuthenticatedPeer::local(PeerGrant {
        principal: ParticipantId::from_u128(900),
        tenants: [ledger.tenant].into_iter().collect(),
        role: PeerRole::Actor,
    })
    .unwrap()
}
fn request(ledger: LedgerId, id: u128, operation: Operation) -> RequestEnvelope {
    RequestEnvelope {
        protocol: PROTOCOL_VERSION,
        ledger,
        route_epoch: RouteEpoch(1),
        request_epoch: RequestEpoch(1),
        request_id: RequestId::from_u128(id),
        operation,
    }
}
fn epoch(ledger: LedgerId, id: u128) -> RequestEnvelope {
    request(
        ledger,
        id,
        Operation::OpenEpoch {
            epoch: RequestEpoch(1),
        },
    )
}
fn read(ledger: LedgerId) -> RequestEnvelope {
    request(
        ledger,
        10000,
        Operation::Read(ReadRequest {
            consistency: ReadConsistency::Linearizable,
            query: ReadQuery::Objects(Vec::new()),
            max_items: 1,
        }),
    )
}
struct Group {
    hosts: BTreeMap<LedgerId, ReplicaHost>,
    owner: ReplicaOwner,
    outgoing: Option<FleetReplication>,
    wal: SharedWal,
    budget: MemoryBudget,
    tenants: Vec<MemoryBudget>,
}
fn open(path: &std::path::Path, node: u64, voters: &[u64], count: usize) -> Group {
    let budget = MemoryBudget::new(1024 * 1024 * 1024, 256 * 1024 * 1024).unwrap();
    let tenants: Vec<_> = (0..2)
        .map(|_| budget.child(384 * 1024 * 1024, 64 * 1024 * 1024).unwrap())
        .collect();
    let wal = SharedWal::open_with_budget(
        path,
        WalOptions::new(WalIdentity {
            cluster: CLUSTER,
            node,
            stream: 0,
        }),
        WalWriterLimits::default(),
        budget.child(256 * 1024 * 1024, 64 * 1024 * 1024).unwrap(),
    )
    .unwrap();
    let baseline = budget.stats().used;
    let replicas = (0..count)
        .map(|index| {
            let ledger = ledger(index as u128);
            let mut config = NodeConfig::single(node, CLUSTER, ledger.session.0);
            config.voters = voters.to_vec();
            let consensus =
                DurableNode::open_on_wal_in(config, wal.clone(), &tenants[index % 2]).unwrap();
            let session = Session::from_node_in(
                ledger,
                consensus,
                SessionLimits::default(),
                &tenants[index % 2],
            )
            .unwrap();
            let mut config = ReplicaConfig::new(RootCommandId::from_u128(901));
            config.tick = Duration::from_millis(20);
            config.request_timeout = Duration::from_millis(750);
            FleetReplica { session, config }
        })
        .collect();
    if count >= 32 {
        assert!(
            budget.stats().used - baseline < count * 128 * 1024,
            "idle sessions preallocated active publication capacity: {}",
            budget.stats().used - baseline
        );
    }
    let (hosts, owner, outgoing) = ReplicaFleet::spawn(
        node,
        replicas,
        tenants
            .iter()
            .enumerate()
            .map(|(i, budget)| FleetTenant {
                tenant: ledger(i as u128).tenant,
                weight: if i == 0 { 2 } else { 1 },
                budget: budget.clone(),
            })
            .collect(),
        budget.clone(),
        ReplicaHost::wire_limits(),
    )
    .unwrap();
    Group {
        hosts,
        owner,
        outgoing: Some(outgoing),
        wal,
        budget,
        tenants,
    }
}
async fn ready(
    hosts: &[&BTreeMap<LedgerId, ReplicaHost>],
    ledger: LedgerId,
    excluded: Option<usize>,
) -> usize {
    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            for (i, hosts) in hosts.iter().enumerate() {
                if Some(i) == excluded {
                    continue;
                }
                let host = &hosts[&ledger];
                let status = host.progress();
                if status.node == status.leader && status.term > 0 {
                    let reply = dispatch(
                        host,
                        actor(ledger),
                        read(ledger),
                        &ReplicaHost::wire_limits(),
                    )
                    .await;
                    if matches!(reply.result, Response::Read(_)) {
                        return i;
                    }
                }
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("no authoritative session leader")
}
async fn stop(hosts: &BTreeMap<LedgerId, ReplicaHost>) {
    for host in hosts.values() {
        host.stop().await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn idle_sessions_share_one_owner_and_tenant_pressure_does_not_block_another_tenant() {
    let directory = tempfile::tempdir().unwrap();
    let group = open(directory.path(), 1, &[1], 48);
    ready(&[&group.hosts], ledger(0), None).await;
    ready(&[&group.hosts], ledger(1), None).await;
    let available = {
        let stats = group.tenants[0].stats();
        stats.limit - stats.completion_reserve - stats.ordinary_used
    };
    let pressure = group.tenants[0]
        .reserve(BudgetKind::Payload, BudgetLane::Ordinary, available)
        .unwrap();
    let refused = dispatch(
        &group.hosts[&ledger(0)],
        actor(ledger(0)),
        epoch(ledger(0), 1),
        &ReplicaHost::wire_limits(),
    )
    .await;
    assert!(matches!(
        refused.result,
        Response::Error(AccessError::Capacity)
    ));
    let reply = dispatch(
        &group.hosts[&ledger(1)],
        actor(ledger(1)),
        epoch(ledger(1), 1),
        &ReplicaHost::wire_limits(),
    )
    .await;
    assert!(
        matches!(
            reply.result,
            Response::Submitted(MutationReply::Committed(_))
        ),
        "{reply:?}"
    );
    assert!(!group.hosts[&ledger(0)].progress().stopped);
    drop(pressure);
    let success = dispatch(
        &group.hosts[&ledger(0)],
        actor(ledger(0)),
        epoch(ledger(0), 1),
        &ReplicaHost::wire_limits(),
    )
    .await;
    assert!(
        matches!(
            success.result,
            Response::Submitted(MutationReply::Committed(_))
        ),
        "{success:?}"
    );
    group.hosts[&ledger(2)].stop().await.unwrap();
    let still_serving = dispatch(
        &group.hosts[&ledger(1)],
        actor(ledger(1)),
        read(ledger(1)),
        &ReplicaHost::wire_limits(),
    )
    .await;
    assert!(matches!(still_serving.result, Response::Read(_)));
    for (id, host) in &group.hosts {
        if *id != ledger(2) {
            host.stop().await.unwrap();
        }
    }
    group.owner.join().unwrap();
    drop(group.wal);
    let stopped_used = group.budget.stats().used;
    assert!(
        stopped_used > 0,
        "reachable channel backing lost its reservation"
    );
    drop(group.hosts);
    assert_eq!(
        group.budget.stats().used,
        stopped_used,
        "outbound receiver lost shared backing"
    );
    drop(group.outgoing);
    assert_eq!(
        group.budget.stats().used,
        0,
        "detached node/session allocations leaked"
    );
}

#[tokio::test]
async fn retained_host_keeps_channel_backing_after_owner_and_receiver_close() {
    let directory = tempfile::tempdir().unwrap();
    let group = open(directory.path(), 1, &[1], 1);
    let retained = group.hosts[&ledger(0)].clone();
    stop(&group.hosts).await;
    group.owner.join().unwrap();
    drop(group.outgoing);
    drop(group.hosts);
    drop(group.wal);
    assert!(
        group.budget.stats().used > 0,
        "live host lost channel backing"
    );
    drop(retained);
    assert_eq!(group.budget.stats().used, 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn multiple_session_quorums_share_node_workers_and_wal_through_leader_loss_and_restart() {
    let roots: Vec<_> = (0..3).map(|_| tempfile::tempdir().unwrap()).collect();
    let mut groups: Vec<_> = roots
        .iter()
        .enumerate()
        .map(|(i, root)| open(root.path(), i as u64 + 1, &[1, 2, 3], 4))
        .collect();
    let isolated = Arc::new(AtomicU64::new(0));
    let targets: Vec<_> = groups.iter().map(|group| group.hosts.clone()).collect();
    let mut pumps = Vec::new();
    for (index, group) in groups.iter_mut().enumerate() {
        let mut outgoing = group.outgoing.take().unwrap();
        let targets = targets.clone();
        let isolated = isolated.clone();
        pumps.push(tokio::spawn(async move {
            while let Some(frame) = outgoing.recv().await {
                let source = index as u64 + 1;
                if isolated.load(Ordering::Acquire) == source
                    || isolated.load(Ordering::Acquire) == frame.target
                {
                    continue;
                }
                let ledger = frame.request.ledger;
                let peer = AuthenticatedPeer::local(PeerGrant {
                    principal: ParticipantId::from_u128(source as u128),
                    tenants: [ledger.tenant].into_iter().collect(),
                    role: PeerRole::Node { node_id: source },
                })
                .unwrap();
                let _ = dispatch(
                    &targets[frame.target as usize - 1][&ledger],
                    peer,
                    frame.request.clone(),
                    &ReplicaHost::wire_limits(),
                )
                .await;
            }
        }));
    }
    let hosts: Vec<_> = groups.iter().map(|group| &group.hosts).collect();
    let mut receipts = BTreeMap::new();
    for index in 0..4 {
        let ledger = ledger(index);
        let leader = ready(&hosts, ledger, None).await;
        let reply = dispatch(
            &hosts[leader][&ledger],
            actor(ledger),
            epoch(ledger, 1),
            &ReplicaHost::wire_limits(),
        )
        .await;
        assert!(
            matches!(
                reply.result,
                Response::Submitted(MutationReply::Committed(_))
            ),
            "{reply:?}"
        );
        receipts.insert(ledger, reply);
    }
    let leader = ready(&hosts, ledger(0), None).await;
    isolated.store(leader as u64 + 1, Ordering::Release);
    let uncertain = dispatch(
        &hosts[leader][&ledger(0)],
        actor(ledger(0)),
        epoch(ledger(0), 2),
        &ReplicaHost::wire_limits(),
    )
    .await;
    assert!(matches!(
        uncertain.result,
        Response::Error(AccessError::OutcomeUnknown | AccessError::Unavailable)
    ));
    let replacement = ready(&hosts, ledger(0), Some(leader)).await;
    let committed = dispatch(
        &hosts[replacement][&ledger(0)],
        actor(ledger(0)),
        epoch(ledger(0), 2),
        &ReplicaHost::wire_limits(),
    )
    .await;
    assert!(
        matches!(
            committed.result,
            Response::Submitted(MutationReply::Committed(_))
        ),
        "{committed:?}"
    );
    isolated.store(0, Ordering::Release);
    tokio::time::timeout(Duration::from_secs(15), async {
        while groups.iter().any(|group| {
            group.hosts.iter().any(|(id, host)| {
                host.progress().sequence < SessionSeq(if *id == ledger(0) { 2 } else { 1 })
            })
        }) {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    for group in &groups {
        stop(&group.hosts).await;
    }
    for pump in pumps {
        pump.abort();
        let _ = pump.await;
    }
    drop(hosts);
    drop(targets);
    for group in groups {
        group.owner.join().unwrap();
        drop(group.hosts);
        drop(group.wal);
        assert_eq!(group.budget.stats().used, 0);
    }
    // Reopen each physical WAL once and inspect every recovered session prefix.
    for (index, root) in roots.iter().enumerate() {
        let group = open(root.path(), index as u64 + 1, &[1, 2, 3], 4);
        for (ledger, receipt) in &receipts {
            assert!(
                group.hosts[ledger].progress().sequence
                    >= match &receipt.result {
                        Response::Submitted(MutationReply::Committed(receipt)) => receipt.sequence,
                        _ => unreachable!(),
                    }
            );
        }
        stop(&group.hosts).await;
        group.owner.join().unwrap();
    }
}

/// A session's queued proposals share its writes (27 §9): sixteen clients
/// ask one session of three grouped owners for entries one after another,
/// each the moment its last was answered, and the leader's log takes them
/// in fewer writes than entries. Before, an owner that shares its thread
/// started a write for each proposal as it came and dispatched nothing to a
/// session with a write out — one proposal a write. What the burst took and
/// how many entries a write carried is printed (the measurement of the
/// change); the test claims the burst commits.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_sessions_queued_proposals_share_its_writes() {
    const CLIENTS: u128 = 16;
    const EACH: u128 = 40;
    let roots: Vec<_> = (0..3).map(|_| tempfile::tempdir().unwrap()).collect();
    let mut groups: Vec<_> = roots
        .iter()
        .enumerate()
        .map(|(i, root)| open(root.path(), i as u64 + 1, &[1, 2, 3], 1))
        .collect();
    let targets: Vec<_> = groups.iter().map(|group| group.hosts.clone()).collect();
    let mut pumps = Vec::new();
    // Each frame is carried on a task of its own, as a peer connection
    // carries each on its own stream: a pump that waited for every frame's
    // answer — given once its receiver persisted it — would carry one frame
    // at a time and make the followers' writes the burst's pace. As many
    // at once as the consensus window lets a peer have in flight.
    let window = focal_consensus::DEFAULT_INFLIGHT_WINDOW;
    for (index, group) in groups.iter_mut().enumerate() {
        let mut outgoing = group.outgoing.take().unwrap();
        let targets = targets.clone();
        let lanes = Arc::new(tokio::sync::Semaphore::new(window * 2));
        pumps.push(tokio::spawn(async move {
            while let Some(frame) = outgoing.recv().await {
                let Ok(lane) = lanes.clone().acquire_owned().await else {
                    break;
                };
                let targets = targets.clone();
                tokio::spawn(async move {
                    let source = index as u64 + 1;
                    let ledger = frame.request.ledger;
                    let peer = AuthenticatedPeer::local(PeerGrant {
                        principal: ParticipantId::from_u128(source as u128),
                        tenants: [ledger.tenant].into_iter().collect(),
                        role: PeerRole::Node { node_id: source },
                    })
                    .unwrap();
                    let _ = dispatch(
                        &targets[frame.target as usize - 1][&ledger],
                        peer,
                        frame.request.clone(),
                        &ReplicaHost::wire_limits(),
                    )
                    .await;
                    drop(lane);
                });
            }
        }));
    }
    let hosts: Vec<_> = groups.iter().map(|group| &group.hosts).collect();
    let session = ledger(0);
    let leader = ready(&hosts, session, None).await;
    let wal = &groups[leader].wal;
    let before = wal.stats().unwrap();
    let started = std::time::Instant::now();
    let policy = focal_client::RetryPolicy::default();
    let policy = &policy;
    let hosts_ref = &hosts;
    futures_util::future::join_all((0..CLIENTS).map(|client| async move {
        for each in 0..EACH {
            let envelope = epoch(session, 20_000 + client * EACH + each);
            // Charged to the group's commits: only a stall spends it.
            let periods = || -> Vec<u64> {
                hosts_ref
                    .iter()
                    .map(|hosts| hosts[&session].periods())
                    .collect()
            };
            let committed = || {
                hosts_ref
                    .iter()
                    .map(|hosts| hosts[&session].progress().sequence)
                    .max()
                    .unwrap_or_default()
            };
            let budget = focal_timing::ProgressDeadline::periods(
                Duration::from_secs(10),
                Duration::from_millis(20),
            );
            let frozen = Duration::from_secs(60);
            let mut wait = focal_timing::ProgressDeadline::begin(&periods(), budget, frozen);
            let mut seen = committed();
            let mut backoffs = 0u32;
            loop {
                // The replica that leads at the time.
                let leading = hosts_ref
                    .iter()
                    .position(|hosts| {
                        let progress = hosts[&session].progress();
                        progress.node == progress.leader
                    })
                    .unwrap_or(leader);
                let reply = dispatch(
                    &hosts_ref[leading][&session],
                    actor(session),
                    envelope.clone(),
                    &ReplicaHost::wire_limits(),
                )
                .await;
                match reply.result {
                    Response::Submitted(MutationReply::Committed(_)) => break,
                    Response::Error(
                        refused @ (AccessError::Unavailable
                        | AccessError::OutcomeUnknown
                        | AccessError::Capacity),
                    ) => {
                        let now = committed();
                        if now > seen {
                            seen = now;
                            wait =
                                focal_timing::ProgressDeadline::begin(&periods(), budget, frozen);
                        }
                        if let Err(spent) = wait.check(&periods()) {
                            panic!("{envelope:?}: {refused:?} after {spent}");
                        }
                        if !matches!(refused, AccessError::OutcomeUnknown) {
                            tokio::time::sleep(policy.pause(backoffs)).await;
                            backoffs = backoffs.saturating_add(1);
                        }
                    }
                    other => panic!("{envelope:?}: {other:?}"),
                }
            }
        }
    }))
    .await;
    let elapsed = started.elapsed();
    let after = wal.stats().unwrap();
    let entries = u64::try_from(CLIENTS * EACH).unwrap();
    let writes = after.group_commits - before.group_commits;
    println!(
        "queued proposals: {entries} entries in {elapsed:?} ({:.0} a second); the leader's log took them in {writes} writes ({:.2} entries a write, {} records)",
        entries as f64 / elapsed.as_secs_f64(),
        entries as f64 / writes.max(1) as f64,
        after.appended_records - before.appended_records
    );
    for group in &groups {
        stop(&group.hosts).await;
    }
    for pump in pumps {
        pump.abort();
        let _ = pump.await;
    }
    drop(hosts);
    drop(targets);
    for group in groups {
        group.owner.join().unwrap();
    }
}
