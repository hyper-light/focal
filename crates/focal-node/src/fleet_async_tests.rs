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
            config.tick_ceiling = ceiling;
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
    let observed: Vec<&ReplicaHost> = fixture.hosts.values().collect();
    let step = TICK * crate::test_waits::STEP;
    crate::test_waits::within(&observed, step, TICK, observation.changed())
        .await
        .expect("the session's progress changed")
        .unwrap();
    let stopped =
        crate::test_waits::within(&observed, step, TICK, fixture.hosts[&ledger(1)].stop()).await;
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
    // Each a step of the owners' periods (`test_waits::STEP`).
    let observed: Vec<&ReplicaHost> = hosts.values().collect();
    let step = TICK * crate::test_waits::STEP;
    let pending = crate::test_waits::within(&observed, step, TICK, observation.changed()).await;
    let stopped = crate::test_waits::within(&observed, step, TICK, hosts[&ledger(1)].stop()).await;
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
/// the clock, which three suites at once passed (2026-10-03).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_held_log_is_asked_nothing_and_its_answer_wakes_every_session_that_waits() {
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
                tokio::time::sleep(Duration::from_millis(10)).await;
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
        tokio::time::sleep(Duration::from_millis(250)).await;
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
        let answered_before: Vec<(u64, u64)> = (1..=sessions)
            .map(|index| {
                let progress = fixture.hosts[&ledger(index)].progress();
                (progress.waits_answered, progress.waits_swept)
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
        }
        // The log's answer woke every session that waited on it: its signal,
        // or — where a write answered more sessions than the owner's signals
        // hold and an answer's signal found them full — the sweep that
        // signal's loss asked for. Before the sweep, such a session waited for
        // its tick (PR #4's macOS run: one of a thousand).
        let mut swept = 0;
        let unwoken: Vec<u128> = (1..=sessions)
            .filter(|index| {
                let progress = fixture.hosts[&ledger(*index)].progress();
                let (answered, looked) = answered_before[usize::try_from(*index - 1).unwrap()];
                if progress.waits_swept > looked {
                    swept += 1;
                }
                progress.waits_answered <= answered && progress.waits_swept <= looked
            })
            .collect();
        println!("{sessions} sessions: {swept} looked at by a sweep");
        shutdown(fixture).await;
        assert!(
            unwoken.is_empty(),
            "{} of {sessions} sessions were not woken by the log's answer: {unwoken:?}",
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
}

/// The audit's F26 acceptance: 4,096 sessions in one grouped owner, the only
/// stopped one placed past the first 512 by key. The node's aggregates count
/// it in the first round; the page lists it in the first round, flagged,
/// and in every round after; every healthy session is listed within
/// ⌈healthy / (room − flagged)⌉ rounds; and the rounds keep no more than
/// the hosted set. The process's resident memory is printed beside what the
/// fleet's budget accounts, for the audit's comparison.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_only_stopped_session_past_the_first_512_shows_in_the_first_round() {
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
}

/// A session whose write is in flight is handed no work: what comes for it
/// waits in its queue, a participant's within the participants' share, and
/// what finds no room is refused — counted where it was refused, a
/// participant's request apart from a peer's frame (`InputRefusals`), never
/// a silent drop.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_session_with_a_write_out_refuses_what_its_queue_has_no_room_for_and_counts_it() {
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
    // One write takes the session's write out; the paused writer holds it.
    let mut observation = host.progress.clone();
    observation.borrow_and_update();
    let mut answers = Vec::new();
    let first = host.clone();
    answers.push(tokio::spawn(async move {
        dispatch(&first, peer(), write(100), &ReplicaHost::wire_limits()).await
    }));
    let steps = TICK * (2 * crate::test_waits::STEP);
    crate::test_waits::within(&[&host], steps, TICK, observation.changed())
        .await
        .unwrap()
        .unwrap();
    // The participants' share of the session's queue, and one more.
    let queue = ReplicaConfig::new(RootCommandId::from_u128(119)).queue_items;
    let share = queue - queue / 4;
    for id in 0..=share {
        let host = host.clone();
        let request = write(200 + id as u128);
        answers.push(tokio::spawn(async move {
            dispatch(&host, peer(), request, &ReplicaHost::wire_limits()).await
        }));
    }
    let refused = crate::test_waits::within(&[&host], steps, TICK, async {
        while host.input_refusals().requests_refused == 0 {
            tokio::time::sleep(Duration::from_millis(1)).await;
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
    assert_eq!(counted.requests_refused, 1, "and was counted");
    assert_eq!(counted.frames_refused, 0);
    assert_eq!(counted.requests_dropped, 0);
}

/// A peer's frame reaches its session's owner as it comes, while the
/// session's write is in flight too: the owner steps it or keeps it, and
/// answers at once a frame it refuses — here one from a node the session
/// does not name. Queued behind the write, a frame waited for it, holding a
/// place of the queue participants' work shares (`GroupOwner::enqueue`).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_peers_frame_reaches_its_owner_while_the_sessions_write_is_out() {
    let path = tempfile::tempdir().unwrap();
    let fixture = fixture(path.path(), 1);
    settle(&fixture).await;
    let host = fixture.hosts[&ledger(1)].clone();
    let node = AuthenticatedPeer::local(PeerGrant {
        principal: ParticipantId::from_u128(2),
        tenants: [ledger(1).tenant].into_iter().collect(),
        role: PeerRole::Node { node_id: 2 },
    })
    .unwrap();
    let lease = blocker(&fixture.wal);
    let (resume, disk) = pause(lease);
    // One write takes the session's write out; the paused writer holds it.
    let mut observation = host.progress.clone();
    observation.borrow_and_update();
    let first = host.clone();
    let write = tokio::spawn(async move {
        dispatch(
            &first,
            peer(),
            RequestEnvelope {
                request_id: RequestId::from_u128(300),
                ..envelope(
                    1,
                    Operation::OpenEpoch {
                        epoch: RequestEpoch(1),
                    },
                )
            },
            &ReplicaHost::wire_limits(),
        )
        .await
    });
    let steps = TICK * (2 * crate::test_waits::STEP);
    crate::test_waits::within(&[&host], steps, TICK, observation.changed())
        .await
        .unwrap()
        .unwrap();
    let frame = RequestEnvelope {
        request_id: RequestId::from_u128(301),
        ..envelope(
            1,
            Operation::Raft {
                group: ledger(1).session.0,
                message: vec![1],
            },
        )
    };
    let answered = crate::test_waits::within(
        &[&host],
        steps,
        TICK,
        dispatch(&host, node, frame, &ReplicaHost::wire_limits()),
    )
    .await;
    let write_out = !write.is_finished();
    resume.send(()).unwrap();
    drop(disk.join().unwrap());
    write.await.unwrap();
    drop(host);
    shutdown(fixture).await;
    let answered = answered.expect("the frame waited for the session's write");
    assert!(
        matches!(answered.result, Response::Error(AccessError::Unauthorized)),
        "{answered:?}"
    );
    assert!(write_out, "answered while the session's write was out");
}
