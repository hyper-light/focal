//! Elections over a modelled network (27 §6, stage B): real replicas on real
//! logs, their messages carried by `focal_sim::path` at LAN, regional and
//! geographic profiles, in virtual time. The tick period is the one derived
//! from the round trips each replica measured (27 §3.1 P2).
use crate::{
    DurableNode, StateRole,
    tests::config,
    timing::{PathRtt, TickPace},
};
use focal_sim::path::{Fabric, Fate, Loss, Path};
use raft::prelude::Message;
use std::time::Duration;

const CONFIGURED: Duration = Duration::from_millis(100);
const CEILING: Duration = Duration::from_secs(2);

struct Replica {
    node: DurableNode,
    _dir: tempfile::TempDir,
    period: u64,
    next_tick: u64,
    applied: Vec<Vec<u8>>,
    down: bool,
}
struct Sim {
    fabric: Fabric<Message>,
    replicas: Vec<Replica>,
    /// Every `(term, leader)` any replica reported, in order of appearance.
    leaders: Vec<(u64, u64)>,
    derive: bool,
}
impl Sim {
    fn new(seed: u64, path: Path, derive: bool) -> Self {
        let mut fabric = Fabric::new(seed, 1 << 16, 1 << 28);
        fabric.set_path(path);
        let replicas = (1..=3u64)
            .map(|id| {
                let dir = tempfile::tempdir().unwrap();
                let mut cfg = config(id);
                cfg.voters = vec![1, 2, 3];
                let mut paths = [PathRtt::default(); 3];
                // What a replica's transport has measured before its group
                // elects: probes, at the path's round trip.
                let mut probe = focal_sim::Seeded::new(seed ^ id);
                for estimator in &mut paths {
                    for _ in 0..32 {
                        let jitter = path.jitter_ns().saturating_mul(2).saturating_add(1);
                        let one_way = |random: &mut focal_sim::Seeded| {
                            path.one_way_ns()
                                .saturating_sub(path.jitter_ns())
                                .saturating_add(random.below(jitter))
                        };
                        let round = one_way(&mut probe) + one_way(&mut probe);
                        estimator.on_sample(round.max(1));
                    }
                }
                let period = if derive {
                    let others: Vec<&PathRtt> = paths
                        .iter()
                        .enumerate()
                        .filter(|(index, _)| *index as u64 + 1 != id)
                        .map(|(_, estimator)| estimator)
                        .collect();
                    TickPace::derive(CONFIGURED, CEILING, cfg.election_tick, others).period
                } else {
                    CONFIGURED
                };
                let period = period.as_nanos() as u64;
                Replica {
                    node: DurableNode::open(cfg, dir.path()).unwrap(),
                    _dir: dir,
                    period,
                    // Replicas do not start in step.
                    next_tick: period / 3 * id,
                    applied: Vec::new(),
                    down: false,
                }
            })
            .collect();
        Self {
            fabric,
            replicas,
            leaders: Vec::new(),
            derive,
        }
    }
    fn election_timeout(&self) -> u64 {
        self.replicas
            .iter()
            .map(|replica| replica.period * replica.node.election_tick() as u64)
            .max()
            .unwrap()
    }
    fn flush(&mut self, index: usize) {
        let replica = &mut self.replicas[index];
        let events = replica.node.drain().unwrap();
        replica
            .applied
            .extend(events.committed.into_iter().map(|entry| entry.data));
        for message in events.messages {
            use raft::protocompat::PbMessageExt as _;
            let bytes = message.compute_size() as usize;
            let (from, to) = (message.from, message.to);
            let _: Fate = self.fabric.send(from, to, message, bytes);
        }
        let status = self.replicas[index].node.status();
        if status.role == StateRole::Leader
            && self.leaders.last() != Some(&(status.term, status.node_id))
            && !self.leaders.contains(&(status.term, status.node_id))
        {
            self.leaders.push((status.term, status.node_id));
        }
    }
    /// Run until virtual time `until`, or until `done` holds.
    fn run(&mut self, until: u64, mut done: impl FnMut(&Self) -> bool) -> bool {
        loop {
            if done(self) {
                return true;
            }
            let tick = self
                .replicas
                .iter()
                .filter(|replica| !replica.down)
                .map(|replica| replica.next_tick)
                .min();
            let next = [tick, self.fabric.next_arrival()]
                .into_iter()
                .flatten()
                .min()
                .unwrap();
            if next > until {
                return done(self);
            }
            self.fabric.advance_to(next.max(self.fabric.now())).unwrap();
            while let Some(delivery) = self.fabric.receive() {
                let index = (delivery.to - 1) as usize;
                if self.replicas[index].down {
                    continue;
                }
                // A message from a node outside the configuration, or one a
                // replica refuses for room, is dropped as a transport would.
                let _ = self.replicas[index].node.step(delivery.message);
                self.flush(index);
            }
            let now = self.fabric.now();
            for index in 0..self.replicas.len() {
                if !self.replicas[index].down && self.replicas[index].next_tick <= now {
                    self.replicas[index].node.tick().unwrap();
                    self.replicas[index].next_tick = now + self.replicas[index].period;
                    self.flush(index);
                }
            }
        }
    }
    fn leader(&self) -> Option<u64> {
        let live: Vec<_> = self
            .replicas
            .iter()
            .filter(|replica| !replica.down)
            .collect();
        let leader = live
            .iter()
            .find(|replica| replica.node.status().role == StateRole::Leader)?
            .node
            .status();
        live.iter()
            .all(|replica| {
                let status = replica.node.status();
                status.leader_id == leader.node_id && status.term == leader.term
            })
            .then_some(leader.node_id)
    }
    fn stop(&mut self, node: u64) {
        self.replicas[(node - 1) as usize].down = true;
        for other in 1..=3 {
            self.fabric.partition(node, other, true);
            self.fabric.partition(other, node, true);
        }
    }
    fn terms(&self) -> usize {
        self.leaders.len()
    }
}

/// At every profile a group elects one leader within a few election
/// timeouts, keeps it for a hundred more without a single new term, commits
/// through it, and elects again within a few timeouts when it is lost.
fn elects_keeps_and_replaces(path: Path, seed: u64) {
    let mut sim = Sim::new(seed, path, true);
    let timeout = sim.election_timeout();
    assert!(
        sim.run(8 * timeout, |sim| sim.leader().is_some()),
        "no leader within eight election timeouts of {timeout} ns"
    );
    let first = sim.leader().unwrap();
    let terms = sim.terms();
    let settled = sim.fabric.now();
    sim.run(settled + 100 * timeout, |_| false);
    assert_eq!(
        sim.leader(),
        Some(first),
        "the leader changed: {:?}",
        sim.leaders
    );
    assert_eq!(sim.terms(), terms, "a term was spent: {:?}", sim.leaders);
    // It commits at the speed of the path: within a few round trips.
    let index = (first - 1) as usize;
    sim.replicas[index].node.propose(b"entry".to_vec()).unwrap();
    sim.flush(index);
    let proposed = sim.fabric.now();
    let round = 2 * (path.one_way_ns() + path.jitter_ns());
    assert!(
        sim.run(proposed + 4 * round + 2 * timeout, |sim| sim
            .replicas
            .iter()
            .all(|replica| replica
                .applied
                .iter()
                .any(|entry| entry == b"entry"))),
        "the entry did not reach every replica"
    );
    // The leader is lost; the two that remain elect within a few timeouts.
    sim.stop(first);
    let lost = sim.fabric.now();
    assert!(
        sim.run(lost + 8 * timeout, |sim| sim
            .leader()
            .is_some_and(|leader| leader != first)),
        "no successor within eight election timeouts: {:?}",
        sim.leaders
    );
    let mut seen = std::collections::BTreeMap::new();
    for (term, leader) in &sim.leaders {
        assert_eq!(
            *seen.entry(*term).or_insert(*leader),
            *leader,
            "two leaders in term {term}: {:?}",
            sim.leaders
        );
    }
    assert!(sim.derive);
}
#[test]
fn a_lan_group_elects_keeps_and_replaces_its_leader() {
    for seed in 1..=4 {
        elects_keeps_and_replaces(Path::LAN, seed);
    }
}
#[test]
fn a_regional_group_elects_keeps_and_replaces_its_leader() {
    for seed in 1..=4 {
        elects_keeps_and_replaces(Path::REGIONAL, seed);
    }
}
#[test]
fn a_geographic_group_elects_keeps_and_replaces_its_leader() {
    for seed in 1..=4 {
        elects_keeps_and_replaces(Path::GEOGRAPHIC, seed);
    }
}

/// The class 27 §3.3 names "round expires inside the WAN round trip": on a
/// path whose round trip is longer than the election timeout the configured
/// period gives, a candidate times out before its votes return, and the
/// group never has a leader. At the derived pace the same group elects and
/// keeps its leader.
#[test]
fn a_group_whose_round_trip_exceeds_the_configured_timeout_elects_only_at_the_derived_pace() {
    // 1.2 s one way: a round trip of 2.4 s, against an election timeout of
    // one to two seconds at the configured 100 ms period.
    let path = Path::in_order(1_200_000_000, 100_000_000);
    for seed in 1..=3 {
        let mut fixed = Sim::new(seed, path, false);
        let timeout = fixed.election_timeout();
        assert!(2 * (path.one_way_ns() - path.jitter_ns()) > 2 * timeout);
        fixed.run(200 * timeout, |_| false);
        assert!(
            fixed.leaders.is_empty() && fixed.leader().is_none(),
            "a leader was elected inside a round trip: {:?}",
            fixed.leaders
        );
        let mut derived = Sim::new(seed, path, true);
        let timeout = derived.election_timeout();
        assert!(timeout >= 10 * 2 * path.one_way_ns().min(CEILING.as_nanos() as u64 / 2));
        assert!(
            derived.run(8 * timeout, |sim| sim.leader().is_some()),
            "no leader at the derived pace"
        );
        let first = derived.leader();
        let settled = derived.fabric.now();
        derived.run(settled + 50 * timeout, |_| false);
        assert_eq!(derived.leader(), first, "{:?}", derived.leaders);
        assert_eq!(derived.terms(), 1, "{:?}", derived.leaders);
    }
}

/// Loss in bursts on a regional path: the group keeps electing at most one
/// leader a term, and what it commits reaches every replica.
#[test]
fn a_regional_group_under_bursty_loss_stays_safe_and_commits() {
    for seed in 1..=4 {
        // Bursts of five messages on average, entered once in fifty: about
        // nine in a hundred messages lost, in runs.
        let path = Path::REGIONAL.with_loss(Loss::bursty(20_000, 200_000, 1_000_000));
        let mut sim = Sim::new(seed, path, true);
        let timeout = sim.election_timeout();
        assert!(sim.run(20 * timeout, |sim| sim.leader().is_some()));
        // A client: an entry a leader admitted is not committed until it is
        // applied, and a leader lost in between may take it with it. Each
        // entry is proposed again, to whoever leads, until it is applied.
        let end = sim.fabric.now() + 400 * timeout;
        let mut terms_proposed_in = 0usize;
        for entry in 0..20u8 {
            let mut asked: Option<(u64, u64)> = None;
            loop {
                let applied = sim
                    .replicas
                    .iter()
                    .any(|replica| replica.applied.iter().any(|data| data == &[entry]));
                if applied {
                    break;
                }
                assert!(
                    sim.fabric.now() < end,
                    "entry {entry} never committed: {:?}",
                    sim.leaders
                );
                if let Some(leader) = sim.leader() {
                    let index = (leader - 1) as usize;
                    let term = sim.replicas[index].node.status().term;
                    if asked != Some((leader, term))
                        && sim.replicas[index].node.propose(vec![entry]).is_ok()
                    {
                        asked = Some((leader, term));
                        terms_proposed_in += 1;
                        sim.flush(index);
                    }
                }
                let step = sim.fabric.now() + timeout / 4;
                sim.run(step, |_| false);
            }
        }
        assert!(terms_proposed_in >= 20);
        let settle = sim.fabric.now() + 40 * timeout;
        let everywhere = |sim: &Sim| {
            sim.replicas.iter().all(|replica| {
                (0..20u8).all(|entry| replica.applied.iter().any(|data| data == &[entry]))
            })
        };
        assert!(
            sim.run(settle, everywhere),
            "committed entries did not reach every replica: {:?}",
            sim.replicas
                .iter()
                .map(|replica| replica.applied.len())
                .collect::<Vec<_>>()
        );
        // Entries commit in the order they were asked for: an entry is
        // proposed only once the one before it is applied.
        let mut order: Vec<u8> = Vec::new();
        for data in &sim.replicas[0].applied {
            if let [entry] = data.as_slice()
                && !order.contains(entry)
            {
                order.push(*entry);
            }
        }
        assert_eq!(order, (0..20u8).collect::<Vec<_>>());
        // Every replica applied the same entries in the same order.
        let first = sim.replicas[0].applied.clone();
        for replica in &sim.replicas {
            assert_eq!(replica.applied, first);
        }
        assert!(sim.fabric.stats().dropped_loss > 0, "the path lost nothing");
        let mut seen = std::collections::BTreeMap::new();
        for (term, leader) in &sim.leaders {
            assert_eq!(*seen.entry(*term).or_insert(*leader), *leader);
        }
    }
}

/// Priority over a regional path: with the leader lost, the member of
/// higher priority leads whichever of the two times out first.
#[test]
fn the_preferred_member_leads_after_a_loss_over_a_regional_path() {
    for seed in 1..=6 {
        let mut sim = Sim::new(seed, Path::REGIONAL, true);
        let timeout = sim.election_timeout();
        assert!(sim.run(8 * timeout, |sim| sim.leader().is_some()));
        let first = sim.leader().unwrap();
        let mut others = [1u64, 2, 3].into_iter().filter(|node| *node != first);
        let (preferred, other) = (others.next().unwrap(), others.next().unwrap());
        sim.replicas[(preferred - 1) as usize]
            .node
            .set_priority(2)
            .unwrap();
        sim.replicas[(other - 1) as usize]
            .node
            .set_priority(1)
            .unwrap();
        sim.stop(first);
        let lost = sim.fabric.now();
        assert!(
            sim.run(lost + 12 * timeout, |sim| sim.leader().is_some()),
            "no successor: {:?}",
            sim.leaders
        );
        assert_eq!(sim.leader(), Some(preferred), "{:?}", sim.leaders);
        assert!(
            !sim.leaders.iter().any(|(_, leader)| *leader == other),
            "the member of lower priority led: {:?}",
            sim.leaders
        );
    }
}
