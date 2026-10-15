use super::*;
use focal_consensus::{DurableNode, NodeConfig};
use focal_ledger::SessionLimits;
use focal_log::{
    LogicalLogId, Record, RecordKind, SharedWal, WalIdentity, WalLease, WalOptions, WalWriterLimits,
};

const CLUSTER: [u8; 16] = [119; 16];
/// The replicas' configured tick in these fixtures: what a wait's
/// allowance is counted in (`crate::test_waits`).
const TICK: Duration = Duration::from_secs(1);
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
    fixture_stretching(path, count, Duration::from_secs(2))
}
/// Owners whose period may be stretched to `ceiling` by the paths they are
/// told of (`ReplicaHost::pace`).
fn fixture_stretching(path: &std::path::Path, count: u128, ceiling: Duration) -> Fixture {
    fixture_with(path, count, ceiling, FirstTerm::Durable, |_| {}, |_| ()).0
}
/// What a fixture's sessions have made of their first term when their owners take them.
#[derive(Clone, Copy, PartialEq, Eq)]
enum FirstTerm {
    /// Each campaigned and made its term's first entry durable: it leads, ready in its term.
    Durable,
    /// Each campaigned, its first entry not yet written: it leads a term it is not yet ready in.
    Campaigned,
}
/// A fixture whose sessions, configured by `configure`, have done `first` of their first term,
/// and what `before_spawn` made of its log before their owners took them.
fn fixture_with<R>(
    path: &std::path::Path,
    count: u128,
    ceiling: Duration,
    first: FirstTerm,
    configure: impl Fn(&mut NodeConfig),
    before_spawn: impl FnOnce(&SharedWal) -> R,
) -> (Fixture, R) {
    // A session with a write out holds some nine megabytes reserved (its
    // request, its staging, its reply): the tenant has room for every
    // session to have one out at once.
    let sessions = usize::try_from(count).unwrap();
    let tenant_bytes = (256 * 1024 * 1024).max(sessions * 12 * 1024 * 1024);
    let budget = MemoryBudget::new(tenant_bytes + 256 * 1024 * 1024, 128 * 1024 * 1024).unwrap();
    let tenant = budget.child(tenant_bytes, 64 * 1024 * 1024).unwrap();
    let wal_budget = budget.child(128 * 1024 * 1024, 32 * 1024 * 1024).unwrap();
    let wal = SharedWal::open_with_budget(
        path,
        WalOptions::new(WalIdentity {
            cluster: CLUSTER,
            node: 1,
            stream: 0,
        }),
        // A queue with a place for every session's write, so that as many
        // sessions as the test has can each have one out at once — up to the
        // most a writer admits, past which the sessions' writes take turns.
        WalWriterLimits {
            queue_items: WalWriterLimits::default()
                .queue_items
                .max(usize::try_from(count).unwrap().saturating_mul(4))
                .min(focal_log::MAX_QUEUE_ITEMS),
            ..WalWriterLimits::default()
        },
        wal_budget.clone(),
    )
    .unwrap();
    let replicas = (1..=count)
        .map(|index| {
            let mut node_config = NodeConfig::single(1, CLUSTER, ledger(index).session.0);
            configure(&mut node_config);
            let node = DurableNode::open_on_wal_in(node_config, wal.clone(), &tenant).unwrap();
            let mut session =
                Session::from_node_in(ledger(index), node, SessionLimits::default(), &tenant)
                    .unwrap();
            session.campaign().unwrap();
            if first == FirstTerm::Durable {
                for _ in 0..3 {
                    session.poll().unwrap();
                }
            }
            let mut config = ReplicaConfig::new(RootCommandId::from_u128(119));
            config.tick = TICK;
            config.tick_ceiling = ceiling;
            config.request_timeout = Duration::from_millis(500);
            FleetReplica { session, config }
        })
        .collect();
    let held = before_spawn(&wal);
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
    (
        Fixture {
            hosts,
            owner,
            outgoing,
            wal,
            wal_budget,
            budget,
        },
        held,
    )
}
/// [`fixture`]'s `count` sessions over `shards` owner shards.
fn fixture_on_shards(path: &std::path::Path, count: u128, shards: usize) -> Fixture {
    let sessions = usize::try_from(count).unwrap();
    let tenant_bytes = (256 * 1024 * 1024).max(sessions * 12 * 1024 * 1024);
    let budget = MemoryBudget::new(tenant_bytes + 256 * 1024 * 1024, 128 * 1024 * 1024).unwrap();
    let tenant = budget.child(tenant_bytes, 64 * 1024 * 1024).unwrap();
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
            config.tick = TICK;
            config.request_timeout = Duration::from_millis(500);
            FleetReplica { session, config }
        })
        .collect();
    let (hosts, owner, outgoing) = ReplicaFleet::spawn_sharded(
        1,
        replicas,
        vec![FleetTenant {
            tenant: ledger(1).tenant,
            weight: 1,
            budget: tenant,
        }],
        budget.clone(),
        ReplicaHost::wire_limits(),
        shards,
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

#[test]
fn shared_owner_queues_covering_flush_and_serves_another_group_while_disk_waits() {
    focal_rt::block_on(async move {
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
            writes.push(
                focal_rt::spawn(async move {
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
                })
                .unwrap(),
            );
        }
        // Charged to the owners' periods, two steps of them (the drains, then
        // the read): a group the paused writer held would answer in none.
        let observed: Vec<&ReplicaHost> = fixture.hosts.values().collect();
        let steps = TICK * (2 * crate::test_waits::STEP);
        let independent = crate::test_waits::within(&observed, steps, TICK, async {
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
                focal_rt::time::sleep(Duration::from_millis(50))
                    .await
                    .unwrap();
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
    })
    .unwrap()
}

#[test]
fn host_stop_reaches_retained_ready_and_expires_unknown_under_pinned_wal_budget() {
    focal_rt::block_on(async move {
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
        let write = focal_rt::spawn(async move {
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
        })
        .unwrap();
        let observed: Vec<&ReplicaHost> = fixture.hosts.values().collect();
        let step = TICK * crate::test_waits::STEP;
        crate::test_waits::within(&observed, step, TICK, observation.changed())
            .await
            .expect("the session's progress changed")
            .unwrap();
        let stopped =
            crate::test_waits::within(&observed, step, TICK, fixture.hosts[&ledger(1)].stop())
                .await;
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
    })
    .unwrap()
}

#[test]
fn stopping_last_session_on_a_stalled_writer_does_not_join_it_on_the_fleet_worker() {
    focal_rt::block_on(async move {
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
                Session::from_node_in(ledger(index), node, SessionLimits::default(), &tenant)
                    .unwrap();
            session.campaign().unwrap();
            for _ in 0..3 {
                session.poll().unwrap();
            }
            let mut config = ReplicaConfig::new(RootCommandId::from_u128(119));
            config.tick = TICK;
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
        let write = focal_rt::spawn(async move {
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
        })
        .unwrap();
        // Each a step of the owners' periods (`test_waits::STEP`).
        let observed: Vec<&ReplicaHost> = hosts.values().collect();
        let step = TICK * crate::test_waits::STEP;
        let pending = crate::test_waits::within(&observed, step, TICK, observation.changed()).await;
        let stopped =
            crate::test_waits::within(&observed, step, TICK, hosts[&ledger(1)].stop()).await;
        let other = crate::test_waits::within(
            &observed,
            step,
            TICK,
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
    })
    .unwrap()
}

/// The owner that shares a thread among sessions is told by the log when a
/// session's write is answered, and asks nothing meanwhile (the audit's
/// F45: it asked every millisecond, for every session with a write out).
/// With the log held and one session, a hundred and a thousand, each with a
/// write out:
/// the owner asks about each write as it queues it and not again; and when
/// the log answers, its answer wakes every session that waited on it — each
/// one's count of the log's answers grows, or the sweep an answer's signal
/// that found the owner's signals full asked for looks at it — and every
/// write is committed.
/// The owners' ticks are stretched to ten seconds here, the longest an
/// owner's may be, so that a wake the log's answer failed to deliver is
/// left to a tick. A first statement held the commits to five seconds of
/// the clock, which three suites at once passed (2026-10-03). The first
/// tick after a write still comes at the cadence it was set at, and one that
/// comes as the log answers drains the session before the answer's signal
/// is taken: the signal is counted all the same, and published at the
/// session's next step (`Owner::count_answered`; unpublished, it read as a
/// session never woken: 17 of 100 on a loaded workstation, 2026-10-11).
#[test]
fn a_held_log_is_asked_nothing_and_its_answer_wakes_every_session_that_waits() {
    focal_rt::block_on(async move {
        for sessions in [1u128, 100, 1_000] {
            let path = tempfile::tempdir().unwrap();
            let ceiling = Duration::from_secs(10);
            let fixture = fixture_stretching(path.path(), sessions, ceiling);
            settle(&fixture).await;
            // Every owner is told of a path a minute long, and is in its
            // stretched period once it has ticked: its next tick is ten
            // seconds away.
            let mut far = focal_timing::PathRtt::default();
            far.on_sample(60_000_000_000);
            let mut ticked = Vec::new();
            for host in fixture.hosts.values() {
                assert_eq!(host.pace([&far]).period, ceiling);
                ticked.push(host.periods());
            }
            for (host, before) in fixture.hosts.values().zip(&ticked) {
                // At most the second its unstretched period still had to run.
                let mut waited = 0u32;
                while host.periods() == *before {
                    waited += 1;
                    assert!(waited < 1_000, "an owner did not tick in ten seconds");
                    focal_rt::time::sleep(Duration::from_millis(10))
                        .await
                        .unwrap();
                }
            }
            let asked_before: u64 = fixture
                .hosts
                .values()
                .map(|host| host.progress().waits_asked)
                .sum();
            let (resume, disk) = pause(blocker(&fixture.wal));
            // One session after another, so that each write reaches its
            // session: the owner admits a tenant's requests a few at a time,
            // and a hundred sent at once are most of them refused the room
            // before any session sees them.
            let mut writes = Vec::new();
            for index in 1..=sessions {
                let host = fixture.hosts[&ledger(index)].clone();
                let mut observation = host.progress.clone();
                observation.borrow_and_update();
                writes.push(
                    focal_rt::spawn(async move {
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
                    })
                    .unwrap(),
                );
                // The session has queued its write and said so.
                observation.changed().await.unwrap();
            }
            let queued: u64 = fixture
                .hosts
                .values()
                .map(|host| host.progress().waits_asked)
                .sum();
            // The log stays held for many times the millisecond at which every
            // session was asked before.
            focal_rt::time::sleep(Duration::from_millis(250))
                .await
                .unwrap();
            let held: u64 = fixture
                .hosts
                .values()
                .map(|host| host.progress().waits_asked)
                .sum();
            // No write is acknowledged while the log is held.
            let waiting: Vec<bool> = writes.iter().map(|write| !write.is_finished()).collect();
            let unanswered = waiting.iter().filter(|waits| **waits).count();
            assert_eq!(unanswered as u128, sessions);
            // What each session had heard from the log before it answers, and
            // how often a sweep had looked at it.
            // A session's count of the log's answers, of sweeps that looked at it, and of wakes
            // handed to it.
            type Woken = (u64, u64, u64);
            let answered_before: Vec<Woken> = (1..=sessions)
                .map(|index| {
                    let progress = fixture.hosts[&ledger(index)].progress();
                    (
                        progress.waits_answered,
                        progress.waits_swept,
                        progress.waits_handed,
                    )
                })
                .collect();
            resume.send(()).unwrap();
            drop(disk.join().unwrap());
            // Those that waited on the held log are answered as it answers.
            let mut answers = Vec::new();
            let mut refused = Vec::new();
            for (write, waited) in writes.into_iter().zip(&waiting) {
                if *waited {
                    answers.push(Some(write.await.unwrap().result));
                } else {
                    answers.push(None);
                    refused.push(write);
                }
            }
            // An answer that is not yet a commit is asked again, the wait charged
            // to the commits as they come: it ends when they stop coming.
            let mut refused = refused.into_iter();
            let mut wait = focal_timing::ProgressDeadline::begin(
                &[0],
                u64::try_from(sessions).unwrap(),
                crate::network_service::tests::FROZEN,
            );
            for (position, answer) in answers.into_iter().enumerate() {
                let index = position as u128 + 1;
                // The commits before this one.
                let committed = u64::try_from(position).unwrap();
                let mut answer = match answer {
                    Some(answer) => answer,
                    None => refused.next().unwrap().await.unwrap().result,
                };
                while !matches!(answer, Response::Submitted(MutationReply::Committed(_))) {
                    if let Err(spent) = wait.check(&[committed]) {
                        panic!("session {index} was not committed: {spent}: {answer:?}");
                    }
                    focal_rt::time::sleep(Duration::from_millis(50))
                        .await
                        .unwrap();
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
            }
            // The log's answer woke every session that waited on it: its signal;
            // or — where a write answered more sessions than the owner's signals
            // hold and an answer's signal found them full — the sweep that
            // signal's loss asked for (before the sweep, such a session waited for
            // its tick: PR #4's macOS run, one of a thousand); or — where the log
            // had no room to tell the session of its write — another session's
            // answered write, which gave the room back (`waits_handed`; uncounted,
            // it read as a session never woken: CI run 38079021596, macOS, one of
            // a hundred).
            let mut swept = 0;
            let mut handed = 0;
            let unwoken: Vec<(u128, Woken, Woken)> = (1..=sessions)
                .filter_map(|index| {
                    let progress = fixture.hosts[&ledger(index)].progress();
                    let before = answered_before[usize::try_from(index - 1).unwrap()];
                    let after = (
                        progress.waits_answered,
                        progress.waits_swept,
                        progress.waits_handed,
                    );
                    if after.1 > before.1 {
                        swept += 1;
                    }
                    if after.2 > before.2 {
                        handed += 1;
                    }
                    (after.0 <= before.0 && after.1 <= before.1 && after.2 <= before.2)
                        .then_some((index, before, after))
                })
                .collect();
            println!("{sessions} sessions: {swept} looked at by a sweep, {handed} handed a wake");
            shutdown(fixture).await;
            assert!(
                unwoken.is_empty(),
                "{} of {sessions} sessions were not woken by the log's answer \
                 (session, before and after: answered, swept, handed): {unwoken:?}",
                unwoken.len()
            );
            // While the log was held nothing was asked: a session asks once as
            // it queues its write (a second time if a request arrived for it
            // meanwhile), never at intervals.
            assert_eq!(
                held,
                queued,
                "{sessions} sessions asked {} times of a held log",
                held - queued
            );
            assert!(
                queued - asked_before <= 2 * sessions as u64,
                "{sessions} sessions asked {} times as they queued",
                queued - asked_before
            );
        }
    })
    .unwrap()
}

/// The audit's F26 acceptance: 4,096 sessions in one grouped owner, the only
/// stopped one placed past the first 512 by key. The node's aggregates count
/// it in the first round; the page lists it in the first round, flagged,
/// and in every round after; every healthy session is listed within
/// ⌈healthy / (room − flagged)⌉ rounds; and the rounds keep no more than
/// the hosted set. The process's resident memory is printed beside what the
/// fleet's budget accounts, for the audit's comparison.
#[test]
fn the_only_stopped_session_past_the_first_512_shows_in_the_first_round() {
    focal_rt::block_on(async move {
        use crate::metrics::{MetricLabels, rounds::Rounds};
        use std::collections::BTreeSet;
        let count = 4096u128;
        let path = tempfile::tempdir().unwrap();
        let fixture = fixture(path.path(), count);
        settle(&fixture).await;
        let stopped = ledger(3000);
        assert!(
            fixture
                .hosts
                .keys()
                .position(|ledger| *ledger == stopped)
                .unwrap()
                >= 512
        );
        fixture.hosts[&stopped].stop().await.unwrap();
        let labels = MetricLabels {
            node: 1,
            cluster: "0".repeat(32),
            region: None,
            zone: None,
            role: "host",
        };
        let memory = MemoryBudget::new(1 << 30, 1 << 20).unwrap();
        let mut rounds = Rounds::new(&labels, memory.clone()).unwrap();
        let hosted: Vec<(LedgerId, &ReplicaHost)> = fixture
            .hosts
            .iter()
            .map(|(ledger, host)| (*ledger, host))
            .collect();
        let survey = |rounds: &mut Rounds| {
            rounds
                .survey(
                    |visit: &mut dyn FnMut(LedgerId, u64, &ReplicaHost)| {
                        for (index, (ledger, host)) in hosted.iter().enumerate() {
                            visit(*ledger, u64::try_from(index).unwrap(), host);
                        }
                    },
                    hosted.len(),
                    1,
                )
                .unwrap()
        };
        let first = survey(&mut rounds);
        assert_eq!(first.aggregates.hosted, 4096);
        assert_eq!(first.aggregates.stopped, 1);
        assert_eq!(first.aggregates.leading, 4095);
        let flagged: Vec<LedgerId> = first
            .flags
            .iter()
            .filter(|(_, flagged)| *flagged)
            .map(|(ledger, _)| *ledger)
            .collect();
        assert_eq!(flagged, [stopped]);
        let room = rounds.budget().capacities([hosted.len(), 0, 0, 0])[0];
        assert!(room >= 2, "room {room}");
        let mut seen = BTreeSet::new();
        let mut listed = 0usize;
        let rounds_bound = 4095usize.div_ceil(room - 1);
        let mut survey_next = Some(first);
        while seen.len() < hosted.len() {
            let current = survey_next.take().unwrap_or_else(|| survey(&mut rounds));
            let chosen = rounds.choose_sessions(&current.flags, room).unwrap();
            assert!(chosen.contains(&stopped), "round {listed}");
            assert_eq!(rounds.kept().0, hosted.len());
            seen.extend(chosen);
            listed += 1;
            assert!(
                listed <= rounds_bound,
                "{} of 4096 after {listed}",
                seen.len()
            );
        }
        let resident = std::process::Command::new("ps")
            .args(["-o", "rss=", "-p", &std::process::id().to_string()])
            .output()
            .ok()
            .and_then(|output| String::from_utf8(output.stdout).ok())
            .map(|text| text.trim().to_owned())
            .unwrap_or_else(|| "unknown".into());
        println!(
            "4096 sessions: {room} listed a round, every one within {listed} rounds; \
             resident {resident} KiB, fleet budget used {} bytes, rounds kept {} bytes",
            fixture.budget.stats().used,
            rounds.kept().1,
        );
    })
    .unwrap()
}

/// A session holds a participant's write that would wait past its delay bound — once its
/// window's worth is queued for its pipeline, the next would be taken only after a bound — at
/// its door, charged, and serves it once the owner takes what it stood behind: held, counted
/// held, never refused for the wait. A write held its whole request time with the door never
/// opening is refused then, counted at the delay. Once its queue drains the session takes
/// writes at once again.
#[test]
fn a_session_holds_what_would_wait_past_its_delay_bound_and_serves_it() {
    focal_rt::block_on(async move {
        let path = tempfile::tempdir().unwrap();
        let fixture = fixture(path.path(), 1);
        settle(&fixture).await;
        let host = fixture.hosts[&ledger(1)].clone();
        let write = |id: u128| RequestEnvelope {
            request_id: RequestId::from_u128(id),
            ..envelope(
                1,
                Operation::OpenEpoch {
                    epoch: RequestEpoch(1),
                },
            )
        };
        let lease = blocker(&fixture.wal);
        let (resume, disk) = pause(lease);
        host.pace.forget_service();
        // One write takes the session's write out; the paused writer holds it.
        let mut observation = host.progress.clone();
        observation.borrow_and_update();
        let mut answers = Vec::new();
        let first = host.clone();
        answers.push(
            focal_rt::spawn(async move {
                dispatch(&first, peer(), write(100), &ReplicaHost::wire_limits()).await
            })
            .unwrap(),
        );
        let steps = TICK * (2 * crate::test_waits::STEP);
        crate::test_waits::within(&[&host], steps, TICK, observation.changed())
            .await
            .unwrap()
            .unwrap();
        // With no cycle measured nothing is refused for its delay: the next writes queue
        // behind the write out, the initial window's worth of them.
        for id in 0..crate::pace::INITIAL_WINDOW {
            let queued = host.clone();
            let id = 200 + u128::try_from(id).unwrap();
            answers.push(
                focal_rt::spawn(async move {
                    dispatch(&queued, peer(), write(id), &ReplicaHost::wire_limits()).await
                })
                .unwrap(),
            );
        }
        let window = u64::try_from(crate::pace::INITIAL_WINDOW).unwrap();
        crate::test_waits::within(&[&host], steps, TICK, async {
            while host.pace.queued(true) < window {
                focal_rt::time::sleep(Duration::from_millis(1))
                    .await
                    .unwrap();
            }
        })
        .await
        .unwrap();
        assert!(host.pace.queueing_delay(true) > Duration::ZERO);
        // Then the session measures a cycle of a microsecond: its window is the initial ten,
        // and with ten writes queued for it the next would wait past its bound: its door shuts.
        host.pace.served(Duration::from_micros(1));
        crate::test_waits::within(&[&host], steps, TICK, async {
            while !host.pace.delay_exceeded(true) {
                focal_rt::time::sleep(Duration::from_millis(1))
                    .await
                    .unwrap();
            }
        })
        .await
        .unwrap();
        // The next write is held at the door, not refused.
        let held = host.clone();
        let held = focal_rt::spawn(async move {
            dispatch(&held, peer(), write(300), &ReplicaHost::wire_limits()).await
        })
        .unwrap();
        crate::test_waits::within(&[&host], steps, TICK, async {
            while host.input_refusals().requests_held < 1 {
                focal_rt::time::sleep(Duration::from_millis(1))
                    .await
                    .unwrap();
            }
        })
        .await
        .unwrap();
        assert!(!held.is_finished(), "held while the door is shut");
        assert_eq!(
            host.input_refusals().requests_refused,
            Default::default(),
            "nothing refused"
        );
        answers.push(held);
        resume.send(()).unwrap();
        drop(disk.join().unwrap());
        // The write out is answered, the queued ones are served however long they waited, and
        // the held one with them: nothing is refused.
        for answer in answers {
            let result = answer.await.unwrap().result;
            assert!(
                !matches!(result, Response::Error(AccessError::Capacity)),
                "a write queued or held is served: {result:?}"
            );
        }
        assert_eq!(
            host.input_refusals().requests_refused,
            Default::default(),
            "nothing refused"
        );
        assert_eq!(host.input_refusals().requests_held, 1);
        // The queue drained: with a cycle the queue can be taken within, the session takes
        // writes again.
        assert_eq!(host.pace.queueing_delay(true), Duration::ZERO);
        host.pace.forget_service();
        host.pace.served(Duration::from_millis(100));
        assert!(!host.pace.delay_exceeded(true));
        let again = dispatch(&host, peer(), write(400), &ReplicaHost::wire_limits()).await;
        assert!(!matches!(
            again.result,
            Response::Error(AccessError::Capacity)
        ));
        drop(host);
        shutdown(fixture).await;
    })
    .unwrap()
}

/// A write held at a shut door its whole request time, the session taking nothing of its kind in
/// all of it, is refused then, nothing done, and counted at the delay; the writes queued before
/// it are served once the session moves again.
#[test]
fn a_write_held_its_whole_request_time_is_refused_and_counted_at_the_delay() {
    focal_rt::block_on(async move {
        let path = tempfile::tempdir().unwrap();
        let fixture = fixture(path.path(), 1);
        settle(&fixture).await;
        let host = fixture.hosts[&ledger(1)].clone();
        let write = |id: u128| RequestEnvelope {
            request_id: RequestId::from_u128(id),
            ..envelope(
                1,
                Operation::OpenEpoch {
                    epoch: RequestEpoch(1),
                },
            )
        };
        let lease = blocker(&fixture.wal);
        let (resume, disk) = pause(lease);
        host.pace.forget_service();
        let mut observation = host.progress.clone();
        observation.borrow_and_update();
        let mut answers = Vec::new();
        let first = host.clone();
        answers.push(
            focal_rt::spawn(async move {
                dispatch(&first, peer(), write(100), &ReplicaHost::wire_limits()).await
            })
            .unwrap(),
        );
        let steps = TICK * (2 * crate::test_waits::STEP);
        crate::test_waits::within(&[&host], steps, TICK, observation.changed())
            .await
            .unwrap()
            .unwrap();
        for id in 0..crate::pace::INITIAL_WINDOW {
            let queued = host.clone();
            let id = 200 + u128::try_from(id).unwrap();
            answers.push(
                focal_rt::spawn(async move {
                    dispatch(&queued, peer(), write(id), &ReplicaHost::wire_limits()).await
                })
                .unwrap(),
            );
        }
        let window = u64::try_from(crate::pace::INITIAL_WINDOW).unwrap();
        crate::test_waits::within(&[&host], steps, TICK, async {
            while host.pace.queued(true) < window {
                focal_rt::time::sleep(Duration::from_millis(1))
                    .await
                    .unwrap();
            }
        })
        .await
        .unwrap();
        host.pace.served(Duration::from_micros(1));
        assert!(host.pace.delay_exceeded(true));
        // The disk stays paused past the request time: the held write's door never opens.
        let refused = dispatch(&host, peer(), write(300), &ReplicaHost::wire_limits()).await;
        assert!(matches!(
            refused.result,
            Response::Error(AccessError::Capacity)
        ));
        let counted = host.input_refusals();
        assert_eq!(
            counted.requests_refused,
            crate::metrics::RefusedBy {
                delay: 1,
                ..Default::default()
            },
            "counted at the delay"
        );
        assert_eq!(counted.requests_held, 1);
        resume.send(()).unwrap();
        drop(disk.join().unwrap());
        for answer in answers {
            let result = answer.await.unwrap().result;
            assert!(
                !matches!(result, Response::Error(AccessError::Capacity)),
                "a write queued before it is served: {result:?}"
            );
        }
        drop(host);
        shutdown(fixture).await;
    })
    .unwrap()
}

/// A session whose write is in flight is handed no work: what comes for it
/// waits in its queue, a participant's within the participants' share, and
/// what finds no room is refused — counted where it was refused, a
/// participant's request apart from a peer's frame (`InputRefusals`), never
/// a silent drop.
#[test]
fn a_session_with_a_write_out_refuses_what_its_queue_has_no_room_for_and_counts_it() {
    focal_rt::block_on(async move {
        let path = tempfile::tempdir().unwrap();
        let fixture = fixture(path.path(), 1);
        settle(&fixture).await;
        let host = fixture.hosts[&ledger(1)].clone();
        let write = |id: u128| RequestEnvelope {
            request_id: RequestId::from_u128(id),
            ..envelope(
                1,
                Operation::OpenEpoch {
                    epoch: RequestEpoch(1),
                },
            )
        };
        let lease = blocker(&fixture.wal);
        let (resume, disk) = pause(lease);
        // The queue's own room is what is tested: the delay its session measured would refuse
        // the writes behind a paused disk before the room is full.
        host.pace.forget_service();
        // One write takes the session's write out; the paused writer holds it.
        let mut observation = host.progress.clone();
        observation.borrow_and_update();
        let mut answers = Vec::new();
        let first = host.clone();
        answers.push(
            focal_rt::spawn(async move {
                dispatch(&first, peer(), write(100), &ReplicaHost::wire_limits()).await
            })
            .unwrap(),
        );
        let steps = TICK * (2 * crate::test_waits::STEP);
        crate::test_waits::within(&[&host], steps, TICK, observation.changed())
            .await
            .unwrap()
            .unwrap();
        // The participants' share of the session's queue, and one more.
        let share = grouped::PARTICIPANT_SLOTS;
        for id in 0..=share {
            let host = host.clone();
            let request = write(200 + id as u128);
            answers.push(
                focal_rt::spawn(async move {
                    dispatch(&host, peer(), request, &ReplicaHost::wire_limits()).await
                })
                .unwrap(),
            );
        }
        let refused = crate::test_waits::within(&[&host], steps, TICK, async {
            while host.input_refusals().requests_refused.total() == 0 {
                focal_rt::time::sleep(Duration::from_millis(1))
                    .await
                    .unwrap();
            }
        })
        .await;
        resume.send(()).unwrap();
        drop(disk.join().unwrap());
        let mut capacity = 0;
        for answer in answers {
            if matches!(
                answer.await.unwrap().result,
                Response::Error(AccessError::Capacity)
            ) {
                capacity += 1;
            }
        }
        let counted = host.input_refusals();
        drop(host);
        shutdown(fixture).await;
        refused.unwrap();
        assert_eq!(capacity, 1, "one write found no room");
        assert_eq!(
            counted.requests_refused,
            crate::metrics::RefusedBy {
                queue: 1,
                ..Default::default()
            },
            "and was counted at its queue"
        );
        assert_eq!(counted.frames_refused.total(), 0);
        assert_eq!(counted.requests_dropped, 0);
    })
    .unwrap()
}

/// A peer's frame is queued in the room its session's peer reserve gives,
/// apart from the participants' queue (F56): with the session's write out
/// and the participants' share of its queue full, a frame is not refused —
/// it waits, and is answered once the write is durable — and only one past
/// the room is refused, and counted (27 §12). Before, a frame took one of
/// the participants' places, and with them full every frame was refused
/// and its peer counted it lost.
#[test]
fn a_peers_frame_waits_in_its_own_room_while_the_participants_queue_is_full() {
    focal_rt::block_on(async move {
        let path = tempfile::tempdir().unwrap();
        let fixture = fixture(path.path(), 1);
        settle(&fixture).await;
        let host = fixture.hosts[&ledger(1)].clone();
        let write = |id: u128| RequestEnvelope {
            request_id: RequestId::from_u128(id),
            ..envelope(
                1,
                Operation::OpenEpoch {
                    epoch: RequestEpoch(1),
                },
            )
        };
        let lease = blocker(&fixture.wal);
        let (resume, disk) = pause(lease);
        // The queue's own room is what is tested: the delay its session measured would refuse
        // the writes behind a paused disk before the room is full.
        host.pace.forget_service();
        // One write takes the session's write out; the paused writer holds it.
        let mut observation = host.progress.clone();
        observation.borrow_and_update();
        let mut writes = Vec::new();
        let first = host.clone();
        writes.push(
            focal_rt::spawn(async move {
                dispatch(&first, peer(), write(100), &ReplicaHost::wire_limits()).await
            })
            .unwrap(),
        );
        let steps = TICK * (2 * crate::test_waits::STEP);
        crate::test_waits::within(&[&host], steps, TICK, observation.changed())
            .await
            .unwrap()
            .unwrap();
        // The participants' share of the session's queue, and one more.
        let share = grouped::PARTICIPANT_SLOTS;
        for id in 0..=share {
            let host = host.clone();
            let request = write(200 + id as u128);
            writes.push(
                focal_rt::spawn(async move {
                    dispatch(&host, peer(), request, &ReplicaHost::wire_limits()).await
                })
                .unwrap(),
            );
        }
        crate::test_waits::within(&[&host], steps, TICK, async {
            while host.input_refusals().requests_refused.total() == 0 {
                focal_rt::time::sleep(Duration::from_millis(1))
                    .await
                    .unwrap();
            }
        })
        .await
        .unwrap();
        // The peers' room: this lone voter's reserve, one in-flight window.
        let (room, queued) = host.pace.frame_room();
        assert_eq!(room, focal_consensus::DEFAULT_INFLIGHT_WINDOW);
        assert_eq!(queued, 0);
        let node = AuthenticatedPeer::local(PeerGrant {
            principal: ParticipantId::from_u128(2),
            tenants: [ledger(1).tenant].into_iter().collect(),
            role: PeerRole::Node { node_id: 2 },
        })
        .unwrap();
        let mut frames = Vec::new();
        for id in 0..=room {
            let host = host.clone();
            let node = node.clone();
            let request = RequestEnvelope {
                request_id: RequestId::from_u128(1000 + id as u128),
                ..envelope(
                    1,
                    Operation::Raft {
                        group: ledger(1).session.0,
                        message: vec![7, 8, 9],
                    },
                )
            };
            frames.push(
                focal_rt::spawn(async move {
                    dispatch(&host, node, request, &ReplicaHost::wire_limits()).await
                })
                .unwrap(),
            );
        }
        crate::test_waits::within(&[&host], steps, TICK, async {
            while host.input_refusals().frames_refused.total() == 0 {
                focal_rt::time::sleep(Duration::from_millis(1))
                    .await
                    .unwrap();
            }
        })
        .await
        .unwrap();
        assert_eq!(
            host.pace.frame_room(),
            (room, room),
            "every place in the room is held while the write is out"
        );
        resume.send(()).unwrap();
        drop(disk.join().unwrap());
        let mut refused = 0;
        for frame in frames {
            match frame.await.unwrap().result {
                Response::Error(AccessError::Capacity) => refused += 1,
                // This node is no member of the lone voter's group: the owner
                // answers the frame once the write is durable, and it is not
                // refused for room.
                Response::Error(AccessError::Unauthorized) => {}
                other => panic!("{other:?}"),
            }
        }
        let mut capacity = 0;
        for answer in writes {
            if matches!(
                answer.await.unwrap().result,
                Response::Error(AccessError::Capacity)
            ) {
                capacity += 1;
            }
        }
        let counted = host.input_refusals();
        assert_eq!(
            host.pace.frame_room().1,
            0,
            "every place was given back as its frame was taken"
        );
        drop(host);
        shutdown(fixture).await;
        assert_eq!(refused, 1, "only the frame past the room is refused");
        assert_eq!(capacity, 1, "one write found no room");
        let queue = crate::metrics::RefusedBy {
            queue: 1,
            ..Default::default()
        };
        assert_eq!(counted.frames_refused, queue, "and was counted at its room");
        assert_eq!(counted.requests_refused, queue);
        assert_eq!(counted.frames_dropped, 0);
        assert_eq!(counted.requests_dropped, 0);
    })
    .unwrap()
}
/// A session that leads a term it is not yet ready in does not serve: its progress names it leader,
/// not authoritative (`ReplicaProgress::serves`), until the term's first entry is durable and the
/// barrier asked behind it answered. A write sent meanwhile waits for the term, held by its owner
/// while the entry persists, and commits. A node's Ready waits on the same fact: said on a known
/// leader alone, it was said before the term was ready in 11 of about 140 starts under load
/// (2026-10-12).
#[test]
fn a_leader_serves_once_its_term_is_ready() {
    focal_rt::block_on(async move {
        let path = tempfile::tempdir().unwrap();
        // The paused writer holds the campaign's first entry: the session leads, not ready.
        let (fixture, (resume, disk)) = fixture_with(
            path.path(),
            1,
            Duration::from_secs(2),
            FirstTerm::Campaigned,
            |_| {},
            |wal| pause(blocker(wal)),
        );
        let host = fixture.hosts[&ledger(1)].clone();
        let progress = host.progress();
        assert_eq!(progress.leader, progress.node, "{progress:?}");
        assert!(
            !progress.authoritative && !progress.serves(),
            "{progress:?}"
        );
        let writer = host.clone();
        let write = focal_rt::spawn(async move {
            dispatch(
                &writer,
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
        })
        .unwrap();
        let steps = TICK * (2 * crate::test_waits::STEP);
        crate::test_waits::within(&[&host], steps, TICK, async {
            while host.pace.queued(true) < 1 {
                focal_rt::time::sleep(Duration::from_millis(1))
                    .await
                    .unwrap();
            }
        })
        .await
        .unwrap();
        resume.send(()).unwrap();
        let lease = disk.join().unwrap();
        let reply = crate::test_waits::within(&[&host], steps, TICK, write)
            .await
            .unwrap()
            .unwrap();
        assert!(
            matches!(
                reply.result,
                Response::Submitted(MutationReply::Committed(_))
            ),
            "{reply:?}"
        );
        let progress = host.progress();
        assert!(progress.authoritative && progress.serves(), "{progress:?}");
        drop(lease);
        drop(host);
        shutdown(fixture).await;
    })
    .unwrap()
}
/// One of a group's other voters, played by the test: a session of its own on its own disk, its
/// messages stepped and its answers carried by hand.
fn voter(path: &std::path::Path, node: u64) -> focal_ledger::Session {
    let mut config = NodeConfig::single(node, CLUSTER, ledger(1).session.0);
    config.voters = vec![1, 2, 3];
    focal_ledger::Session::open(
        path.join(node.to_string()),
        ledger(1),
        config,
        SessionLimits::default(),
    )
    .unwrap()
}
fn node_peer(node: u64) -> AuthenticatedPeer {
    AuthenticatedPeer::local(PeerGrant {
        principal: ParticipantId::from_u128(u128::from(node)),
        tenants: [ledger(1).tenant].into_iter().collect(),
        role: PeerRole::Node { node_id: node },
    })
    .unwrap()
}
/// What the voters the test plays send the leader, and how they take its answer.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Carry {
    /// Every answer is carried, and accepted.
    All,
    /// Their answers to its appends are held; the rest are carried, and accepted.
    HoldingAppendAnswers,
    /// The leader stops: what it is sent may be refused as it does.
    Stopping,
}
/// Carries what the leader sent to the voters the test plays and their answers back, once each;
/// an answer to an append is kept in `held` while they are held. Whether anything moved.
async fn carry(
    host: &ReplicaHost,
    outgoing: &mut FleetReplication,
    voters: &mut [focal_ledger::Session; 2],
    how: Carry,
    held: &mut Vec<(u64, focal_consensus::Message)>,
) -> bool {
    let mut moved = false;
    while let Some(frame) = outgoing.try_recv() {
        let (Operation::Raft { message, .. } | Operation::RaftOrdered { message, .. }) =
            &frame.request.operation
        else {
            continue;
        };
        let Some(voter) = frame
            .target
            .checked_sub(2)
            .and_then(|index| voters.get_mut(usize::try_from(index).unwrap()))
        else {
            continue;
        };
        let message = focal_consensus::decode_message_in(message, voter.wire()).unwrap();
        voter.step(message).unwrap();
        moved = true;
    }
    for (index, voter) in voters.iter_mut().enumerate() {
        let from = u64::try_from(index).unwrap() + 2;
        let wire = voter.wire();
        for message in voter.poll().unwrap().messages {
            moved = true;
            if how == Carry::HoldingAppendAnswers
                && message.msg_type == focal_consensus::MessageType::MsgAppendResponse
            {
                held.push((from, message));
                continue;
            }
            send(host, wire, from, &message, how != Carry::Stopping).await;
        }
    }
    moved
}
/// One voter's message to the leader, as its node sends it; `accepted`, the leader must take it.
async fn send(
    host: &ReplicaHost,
    wire: focal_consensus::Wire,
    from: u64,
    message: &focal_consensus::Message,
    accepted: bool,
) {
    let reply = dispatch(
        host,
        node_peer(from),
        RequestEnvelope {
            protocol: PROTOCOL_VERSION,
            ledger: ledger(1),
            route_epoch: RouteEpoch(1),
            request_epoch: RequestEpoch(1),
            request_id: RequestId::from_u128((u128::from(from) << 64) | u128::from(message.index)),
            operation: Operation::Raft {
                group: ledger(1).session.0,
                message: focal_consensus::encode_message_in(message, wire).unwrap(),
            },
        },
        &ReplicaHost::wire_limits(),
    )
    .await;
    assert!(
        matches!(reply.result, Response::PeerAccepted)
            || (!accepted
                && matches!(
                    reply.result,
                    Response::Error(AccessError::Unavailable | AccessError::OutcomeUnknown)
                )),
        "{:?}",
        reply.result
    );
}
/// A new leader of three voters that has written its term's first entry, its followers' answers
/// to it not yet come, is not ready in its term: a participant's write sent then waits for the
/// term and commits. Taken, it was refused `NotReady` (the participant told `Unavailable`): at
/// every change of leader, each write that came before the term's first entry committed and its
/// readiness round came back.
#[test]
fn a_new_leader_of_three_holds_a_write_until_its_term_is_ready() {
    focal_rt::block_on(async move {
        let path = tempfile::tempdir().unwrap();
        let (fixture, ()) = fixture_with(
            path.path(),
            1,
            Duration::from_secs(2),
            FirstTerm::Campaigned,
            |config| config.voters = vec![1, 2, 3],
            |_| (),
        );
        let Fixture {
            hosts,
            owner,
            mut outgoing,
            wal,
            wal_budget,
            budget,
        } = fixture;
        let host = hosts[&ledger(1)].clone();
        let mut voters = [voter(path.path(), 2), voter(path.path(), 3)];
        let mut held = Vec::new();
        let steps = TICK * (4 * crate::test_waits::STEP);
        // The votes are carried; the answers to the leader's first entry are held.
        crate::test_waits::within(&[&host], steps, TICK, async {
            loop {
                let progress = host.progress();
                if progress.leader == progress.node && held.len() >= 2 {
                    break;
                }
                if !carry(
                    &host,
                    &mut outgoing,
                    &mut voters,
                    Carry::HoldingAppendAnswers,
                    &mut held,
                )
                .await
                {
                    focal_rt::time::sleep(Duration::from_millis(1))
                        .await
                        .unwrap();
                }
            }
        })
        .await
        .unwrap();
        let progress = host.progress();
        assert!(
            !progress.authoritative && !progress.serves(),
            "{progress:?}"
        );
        let writer = host.clone();
        let mut write = focal_rt::spawn(async move {
            dispatch(
                &writer,
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
        })
        .unwrap();
        // The write reaches the session's queue and waits there for the term, its followers'
        // answers held, while the owner runs two periods.
        crate::test_waits::within(&[&host], steps, TICK, async {
            while host.pace.queued(true) < 1 && !write.is_finished() {
                focal_rt::time::sleep(Duration::from_millis(1))
                    .await
                    .unwrap();
            }
        })
        .await
        .unwrap();
        let before = host.periods();
        crate::test_waits::within(&[&host], steps, TICK, async {
            while host.periods() < before + 2 && !write.is_finished() {
                carry(
                    &host,
                    &mut outgoing,
                    &mut voters,
                    Carry::HoldingAppendAnswers,
                    &mut held,
                )
                .await;
                focal_rt::time::sleep(Duration::from_millis(1))
                    .await
                    .unwrap();
            }
        })
        .await
        .unwrap();
        assert!(
            !write.is_finished(),
            "the write was answered before its term was ready: {:?}",
            (&mut write).await.unwrap().result
        );
        // The answers come: the term's first entry commits, its readiness round is carried, and
        // the write commits.
        for (from, message) in std::mem::take(&mut held) {
            send(&host, voters[0].wire(), from, &message, true).await;
        }
        let reply = crate::test_waits::within(&[&host], steps, TICK, async {
            loop {
                if write.is_finished() {
                    break (&mut write).await.unwrap();
                }
                if !carry(&host, &mut outgoing, &mut voters, Carry::All, &mut held).await {
                    focal_rt::time::sleep(Duration::from_millis(1))
                        .await
                        .unwrap();
                }
            }
        })
        .await
        .unwrap();
        assert!(
            matches!(
                reply.result,
                Response::Submitted(MutationReply::Committed(_))
            ),
            "{:?}",
            reply.result
        );
        let progress = host.progress();
        assert!(progress.authoritative && progress.serves(), "{progress:?}");
        // A leader's stop hands its log off to a voter: what it sends them is carried until it
        // stops.
        let stopper = host.clone();
        let mut stopping = focal_rt::spawn(async move { stopper.stop().await }).unwrap();
        crate::test_waits::within(&[&host], steps, TICK, async {
            while !stopping.is_finished() {
                if !carry(
                    &host,
                    &mut outgoing,
                    &mut voters,
                    Carry::Stopping,
                    &mut held,
                )
                .await
                {
                    focal_rt::time::sleep(Duration::from_millis(1))
                        .await
                        .unwrap();
                }
            }
        })
        .await
        .unwrap();
        (&mut stopping).await.unwrap().unwrap();
        drop(host);
        shutdown(Fixture {
            hosts,
            owner,
            outgoing,
            wal,
            wal_budget,
            budget,
        })
        .await;
    })
    .unwrap()
}

/// A shard held up holds up only its own sessions (doc 31 §4.4): with one owner shard's thread
/// held, the other shard's session commits, and the held shard's session commits once it is let
/// go. On one owner thread a node, either session's work waited for the other's.
#[test]
fn a_held_shard_holds_up_only_its_own_sessions() {
    focal_rt::block_on(async move {
        let path = tempfile::tempdir().unwrap();
        let fixture = fixture_on_shards(path.path(), 2, 2);
        // Sessions 1 and 2 differ in their last bit: the shard rule puts them on different shards.
        assert_ne!(
            grouped::shard_of(ledger(1), 2),
            grouped::shard_of(ledger(2), 2)
        );
        settle(&fixture).await;
        let held_host = fixture.hosts[&ledger(1)].clone();
        let HostSender::Group { sender, .. } = &held_host.sender else {
            panic!("a fleet's host sends to its shard");
        };
        let (held, holding) = mpsc::sync_channel(1);
        let (release, released) = mpsc::sync_channel(1);
        sender
            .send(grouped::FleetInput::Shard(grouped::ShardWork::Hold {
                held,
                release: released,
            }))
            .unwrap();
        holding.recv().unwrap();
        let write = |index: u128| {
            envelope(
                index,
                Operation::OpenEpoch {
                    epoch: RequestEpoch(1),
                },
            )
        };
        let other = fixture.hosts[&ledger(2)].clone();
        let observed = [&other];
        let steps = TICK * (2 * crate::test_waits::STEP);
        let committed = crate::test_waits::within(&observed, steps, TICK, async {
            dispatch(&other, peer(), write(2), &ReplicaHost::wire_limits()).await
        })
        .await
        .expect("the other shard's session answered while this one was held");
        assert!(
            matches!(
                committed.result,
                Response::Submitted(MutationReply::Committed(_))
            ),
            "{committed:?}"
        );
        let waiting = {
            let host = held_host.clone();
            focal_rt::spawn(async move {
                dispatch(&host, peer(), write(1), &ReplicaHost::wire_limits()).await
            })
            .unwrap()
        };
        release.send(()).unwrap();
        let answered = waiting.await.unwrap();
        assert!(
            matches!(
                answered.result,
                Response::Submitted(MutationReply::Committed(_))
            ),
            "{answered:?}"
        );
        // A host's clone holds its share of the fleet's lease: these go before the fleet's
        // charges are counted.
        drop((held_host, other));
        shutdown(fixture).await;
    })
    .unwrap()
}

/// Measurement, not a check: commits a second of `FOCAL_MEASURE_SESSIONS` sessions (16 by
/// default) writing at once, each `FOCAL_MEASURE_WRITES` entries (200) one after another, on one
/// owner shard and on `FOCAL_MEASURE_SHARDS` (one a core by default), over one shared log. Run
/// with `--release --ignored --nocapture`.
#[test]
#[ignore = "measurement"]
fn measure_commits_over_owner_shards() {
    let number = |name: &str, default: u64| -> u64 {
        std::env::var(name)
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(default)
    };
    let sessions = u128::from(number("FOCAL_MEASURE_SESSIONS", 16));
    let writes = number("FOCAL_MEASURE_WRITES", 200);
    let many = usize::try_from(number(
        "FOCAL_MEASURE_SHARDS",
        u64::try_from(grouped::default_owner_shards()).unwrap(),
    ))
    .unwrap();
    for shards in [1, many] {
        focal_rt::block_on(async move {
            let path = tempfile::tempdir().unwrap();
            let fixture = fixture_on_shards(path.path(), sessions, shards);
            settle(&fixture).await;
            let started = std::time::Instant::now();
            let mut tasks = Vec::new();
            for index in 1..=sessions {
                let host = fixture.hosts[&ledger(index)].clone();
                tasks.push(
                    focal_rt::spawn(async move {
                        for n in 0..writes {
                            let mut request = envelope(
                                index,
                                Operation::OpenEpoch {
                                    epoch: RequestEpoch(1),
                                },
                            );
                            request.request_id = RequestId::from_u128(u128::from(n) + 1_000);
                            let answer = dispatch(&host, peer(), request, &ReplicaHost::wire_limits())
                                .await;
                            assert!(
                                matches!(
                                    answer.result,
                                    Response::Submitted(MutationReply::Committed(_))
                                ),
                                "{answer:?}"
                            );
                        }
                    })
                    .unwrap(),
                );
            }
            for task in tasks {
                task.await.unwrap();
            }
            let seconds = started.elapsed().as_secs_f64();
            let commits = f64::from(u32::try_from(sessions).unwrap()) * writes as f64;
            println!(
                "{shards} shard(s), {sessions} sessions x {writes} writes: {commits:.0} commits in {seconds:.2} s, {:.0}/s",
                commits / seconds
            );
            shutdown(fixture).await;
        })
        .unwrap();
    }
}
