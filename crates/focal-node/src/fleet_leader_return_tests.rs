//! Leadership goes back to the placement's preferred leader (27 §5): by the
//! replica that leads in its place, once, and only to a member that is
//! there to take it.
use super::*;
use focal_directory::{DurabilityIntent, FailureClass, Placement, PlacementPolicy, PlacementSpec};
use focal_ledger::{OperationId, SessionFenceKind};

fn created(preferred: u64, configuration: u64) -> SessionPlacementRequest {
    let members = std::collections::BTreeMap::from([(1, 1), (2, 1), (3, 1)]);
    SessionPlacementRequest {
        expected_index: 0,
        expected_configuration_index: configuration,
        operation: OperationId::from_u128(41),
        kind: SessionFenceKind::Created,
        from_route: RouteEpoch(0),
        to_route: RouteEpoch(1),
        membership_epoch: 1,
        placement_epoch: 1,
        placement: PlacementSpec {
            policy: PlacementPolicy {
                durability: DurabilityIntent {
                    survive: FailureClass::Node,
                    max_failures: 1,
                },
                residency: Default::default(),
                home_regions: Default::default(),
                required_memory: 0,
            },
            placement: Placement {
                preferred_leader: preferred,
                voters: members.clone(),
                materializers: members.clone(),
                content_copies: members,
            },
        },
    }
}
impl Fleet {
    /// Commit the placement that prefers `preferred`, at whoever leads.
    async fn prefer(&self, preferred: u64) {
        let mut wait = self.deadline();
        loop {
            let leader = self.leader(None).await;
            let host = &self.hosts[leader];
            let placed = match host.membership().await {
                Ok(reply) => {
                    host.propose_placement(created(preferred, reply.view().configuration_index))
                        .await
                }
                Err(error) => Err(error),
            };
            match placed {
                Ok(_) => return,
                Err(error) => {
                    if let Err(spent) = wait.check(&self.periods()) {
                        panic!("the placement was never committed: {error:?}; {spent}");
                    }
                }
            }
            tokio::time::sleep(TICK).await;
        }
    }
    /// Every replica follows `node`, which serves.
    async fn led_by(&self, node: u64) {
        self.led_among(node, 0).await;
    }
    /// Every replica but `away` follows `node`, which serves.
    async fn led_among(&self, node: u64, away: u64) {
        let mut wait = self.deadline();
        loop {
            let agreed = self
                .hosts
                .iter()
                .filter(|host| host.progress().node != away)
                .all(|host| host.progress().leader == node);
            let excluding = (away != 0).then(|| away as usize - 1);
            if agreed && self.leader(excluding).await as u64 + 1 == node {
                return;
            }
            if let Err(spent) = wait.check(&self.periods()) {
                // Each replica's progress, and its owner's periods: run, run
                // without the replica's tick, and the longest.
                panic!(
                    "leadership never reached {node}: {spent}; {:?}",
                    self.hosts
                        .iter()
                        .map(|host| (
                            host.progress(),
                            host.periods(),
                            host.refused_periods(),
                            host.longest_period()
                        ))
                        .collect::<Vec<_>>()
                );
            }
            tokio::time::sleep(TICK).await;
        }
    }
    /// `leader` hands leadership to its successor, once: until its term
    /// has ended.
    async fn leave(&self, leader: u64) {
        let host = &self.hosts[leader as usize - 1];
        let term = host.progress().term;
        let mut wait = self.deadline();
        while host.progress().term == term {
            match host.transfer_leader(leader % 3 + 1).await {
                Ok(())
                | Err(focal_ledger::LedgerError::Consensus(
                    focal_consensus::ConsensusError::NotLeader { .. },
                )) => {}
                Err(error) => panic!("transfer from {leader}: {error:?}"),
            }
            if let Err(spent) = wait.check(&self.periods()) {
                panic!("leadership never left {leader}: {spent}");
            }
            tokio::time::sleep(TICK).await;
        }
    }
    fn returns(&self) -> (u64, u64) {
        self.hosts
            .iter()
            .map(|host| host.progress().returns)
            .fold((0, 0), |(asked, failed), stats| {
                (asked + stats.asked, failed + stats.failed)
            })
    }
    /// Every owner runs `count` more periods.
    async fn run(&self, count: u64) {
        let from = self.periods();
        let mut wait = ProgressDeadline::begin(&from, count.saturating_mul(64), FROZEN);
        loop {
            let now = self.periods();
            if now.iter().zip(&from).all(|(now, from)| now - from >= count) {
                return;
            }
            if let Err(spent) = wait.check(&now) {
                panic!("the owners stopped: {spent}");
            }
            tokio::time::sleep(TICK).await;
        }
    }
}

impl Fleet {
    /// The highest term a replica knows.
    fn term(&self) -> u64 {
        self.hosts
            .iter()
            .map(|host| host.progress().term)
            .max()
            .unwrap_or(0)
    }
    /// While no election is held, no hand-over is asked for: `count`
    /// periods pass, and where the term they began in still stands, so
    /// does what was asked.
    async fn asks_nothing(&self, count: u64) {
        let (term, asked) = (self.term(), self.returns().0);
        self.run(count).await;
        if self.term() == term {
            assert_eq!(self.returns().0, asked, "asked with nothing to ask for");
        }
    }
}

/// The starved owners of a loaded machine lose leadership they were handed,
/// and a preferred leader is then as often elected as it is handed
/// leadership. What is claimed is what holds whatever the machine does:
/// leadership ends at the preferred leader, nothing is asked of a member
/// that is away or while no election is held, and no replica asks more
/// often than its rest allows. That a hand-over is asked for when it is
/// due, and only then, is what `leader_return` tests by itself.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn leadership_returns_to_the_preferred_leader_once_and_only_when_it_is_there() {
    let directory = tempfile::tempdir().unwrap();
    let fleet = Fleet::open(directory.path());
    let first = fleet.leader(None).await;
    // The placement prefers a member that does not lead.
    let preferred = (first as u64 + 1) % 3 + 1;
    let election = fleet.hosts[first].election_periods();
    let began = fleet.periods();
    fleet.prefer(preferred).await;
    fleet.led_by(preferred).await;
    // Where it is preferred it stays.
    fleet.asks_nothing(election * 12).await;
    fleet.led_by(preferred).await;

    // Moved away by hand, it comes back. It comes back within a few
    // election timeouts, so the move is judged by the term it ended, not
    // by who is found leading afterwards.
    fleet.leave(preferred).await;
    fleet.led_by(preferred).await;

    // A preferred leader that cannot be reached is asked nothing: the
    // others elect one of themselves and keep it.
    fleet.isolated.store(preferred as u8, Ordering::SeqCst);
    let stand_in = fleet.leader(Some(preferred as usize - 1)).await;
    assert_ne!(stand_in as u64 + 1, preferred);
    let asked = fleet.returns().0;
    fleet.run(election * 12).await;
    assert_eq!(
        fleet.returns().0,
        asked,
        "nothing is asked of a member away"
    );
    let reply = fleet
        .retry_exact(
            fleet.leader(Some(preferred as usize - 1)).await,
            &request(
                7_001,
                Operation::Read(ReadRequest {
                    consistency: ReadConsistency::Linearizable,
                    query: ReadQuery::Objects(vec![]),
                    max_items: 1,
                }),
            ),
        )
        .await;
    assert!(matches!(reply.result, Response::Read(_)), "{reply:?}");

    // Back and current, it leads again.
    fleet.isolated.store(0, Ordering::SeqCst);
    fleet.led_by(preferred).await;
    fleet.asks_nothing(election * 12).await;

    // No storm: a replica asks once, and again after its rest at the
    // soonest.
    let rest = election * u64::from(crate::leader_return::REST);
    for (host, began) in fleet.hosts.iter().zip(began) {
        let progress = host.progress();
        let periods = host.periods() - began;
        assert!(
            progress.returns.asked <= 1 + periods / rest,
            "{progress:?} in {periods} periods"
        );
        assert!(progress.returns.failed <= progress.returns.asked);
    }
    fleet.stop().await;
}

/// The voters in the preferred leader's zone outrank the rest: one of them
/// leads while the preferred leader is away, elected or handed leadership
/// by the voter that was, and the preferred leader leads again once it is
/// back.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_voter_in_the_preferred_leaders_zone_leads_in_its_place() {
    let directory = tempfile::tempdir().unwrap();
    let fleet = Fleet::open(directory.path());
    let first = fleet.leader(None).await as u64 + 1;
    // The one that leads is preferred, the next is in its zone.
    let (preferred, near, far) = (first, first % 3 + 1, (first + 1) % 3 + 1);
    fleet.prefer(preferred).await;
    for host in &fleet.hosts {
        // What is said of the zone is not said of the leader itself.
        assert!(matches!(
            host.admit(
                vec![1, 2, 3],
                Near {
                    leader: preferred,
                    voters: vec![preferred],
                },
            )
            .await,
            Err(focal_ledger::LedgerError::Capacity)
        ));
        host.admit(
            vec![1, 2, 3],
            Near {
                leader: preferred,
                voters: vec![near],
            },
        )
        .await
        .unwrap();
        assert_eq!(host.progress().near.voters, vec![near]);
    }
    let ranks = |fleet: &Fleet| -> Vec<(u64, i64)> {
        fleet
            .hosts
            .iter()
            .map(|host| (host.progress().node, host.progress().priority))
            .collect()
    };
    let mut expected = vec![
        (preferred, PREFERRED_LEADER_PRIORITY),
        (near, ZONE_PRIORITY),
        (far, VOTER_PRIORITY),
    ];
    expected.sort_unstable();
    let mut wait = fleet.deadline();
    while ranks(&fleet) != expected {
        if let Err(spent) = wait.check(&fleet.periods()) {
            panic!("the ranks never followed: {spent}; {:?}", ranks(&fleet));
        }
        tokio::time::sleep(TICK).await;
    }
    fleet.led_by(preferred).await;

    // Away: whoever is elected, the voter of its zone leads.
    fleet.isolated.store(preferred as u8, Ordering::SeqCst);
    fleet.led_among(near, preferred).await;
    // It has no one to hand to: the preferred leader is asked nothing
    // while it is away.
    let election = fleet.hosts[0].election_periods();
    let asked = fleet.hosts[near as usize - 1].progress().returns.asked;
    fleet.run(election * 12).await;
    fleet.led_among(near, preferred).await;
    assert_eq!(
        fleet.hosts[near as usize - 1].progress().returns.asked,
        asked,
        "nothing is asked of a member away"
    );

    // Back: the voter of its zone hands leadership to it.
    fleet.isolated.store(0, Ordering::SeqCst);
    fleet.led_by(preferred).await;

    // What the directory said of another leader ranks no one.
    for host in &fleet.hosts {
        host.admit(
            vec![1, 2, 3],
            Near {
                leader: far,
                voters: vec![near],
            },
        )
        .await
        .unwrap();
    }
    let mut expected = vec![
        (preferred, PREFERRED_LEADER_PRIORITY),
        (near, VOTER_PRIORITY),
        (far, VOTER_PRIORITY),
    ];
    expected.sort_unstable();
    let mut wait = fleet.deadline();
    while ranks(&fleet) != expected {
        if let Err(spent) = wait.check(&fleet.periods()) {
            panic!("the ranks never followed: {spent}; {:?}", ranks(&fleet));
        }
        tokio::time::sleep(TICK).await;
    }
    fleet.stop().await;
}
