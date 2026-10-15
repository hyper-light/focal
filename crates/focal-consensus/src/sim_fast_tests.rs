//! What the fast track is for, measured (27 §6, stage E): how long a member
//! that does not lead waits from proposing an entry until it applies it, by
//! the fast track and by the classic one, over `focal_sim::path` with none
//! to a tenth of the messages lost. Real replicas on real logs, in virtual
//! time.
//!
//! By the classic track the member sends its proposal to the leader, which
//! is what a node does for a client today. By the fast track it sends it to
//! every voter.
use crate::{DurableNode, Message, StateRole, tests::config};
use focal_sim::path::{Fabric, Fate, Loss, Path};

const PERIOD: u64 = 100_000_000;

#[derive(Clone)]
enum Carried {
    Raft(Box<Message>),
    /// A proposal on its way to the leader.
    Forward(Vec<u8>),
}
struct Replica {
    node: DurableNode,
    _dir: tempfile::TempDir,
    next_tick: u64,
    applied: Vec<Vec<u8>>,
    displaced: Vec<Vec<u8>>,
}
struct Sim {
    fabric: Fabric<Carried>,
    replicas: Vec<Replica>,
}
#[derive(Clone, Copy, Debug, PartialEq)]
enum Track {
    Classic,
    Fast,
}
#[derive(Debug)]
struct Measured {
    mean: u64,
    median: u64,
    tail: u64,
    /// Proposals made again: nothing came of the one before in time.
    again: usize,
    by_fast_quorum: u64,
}

impl Sim {
    fn new(seed: u64, members: u64, path: Path) -> Self {
        let mut fabric = Fabric::new(seed, 1 << 16, 1 << 28);
        fabric.set_path(path);
        let replicas = (1..=members)
            .map(|id| {
                let dir = tempfile::tempdir().unwrap();
                let mut cfg = config(id);
                cfg.voters = (1..=members).collect();
                cfg.fast = true;
                Replica {
                    node: DurableNode::open(cfg, dir.path()).unwrap(),
                    _dir: dir,
                    next_tick: PERIOD / members * id,
                    applied: Vec::new(),
                    displaced: Vec::new(),
                }
            })
            .collect();
        Self { fabric, replicas }
    }
    fn flush(&mut self, index: usize) {
        let replica = &mut self.replicas[index];
        let events = replica.node.drain().unwrap();
        replica
            .applied
            .extend(events.committed.into_iter().map(|entry| entry.data));
        replica
            .displaced
            .extend(events.displaced.into_iter().map(|entry| entry.data));
        for message in events.messages {
            let bytes = crate::envelope::message_len(&message).unwrap();
            let (from, to) = (message.from, message.to);
            let _: Fate = self
                .fabric
                .send(from, to, Carried::Raft(Box::new(message)), bytes);
        }
    }
    fn leader(&self) -> Option<usize> {
        self.replicas
            .iter()
            .position(|replica| replica.node.status().role == StateRole::Leader)
    }
    /// Runs until `done` holds or virtual time reaches `until`.
    fn run(&mut self, until: u64, mut done: impl FnMut(&Self) -> bool) -> bool {
        loop {
            if done(self) {
                return true;
            }
            let tick = self.replicas.iter().map(|replica| replica.next_tick).min();
            let next = [tick, self.fabric.next_arrival()]
                .into_iter()
                .flatten()
                .min()
                .unwrap();
            if next > until {
                self.fabric
                    .advance_to(until.max(self.fabric.now()))
                    .unwrap();
                return done(self);
            }
            self.fabric.advance_to(next.max(self.fabric.now())).unwrap();
            while let Some(delivery) = self.fabric.receive() {
                let index = (delivery.to - 1) as usize;
                match delivery.message {
                    Carried::Raft(message) => {
                        let _ = self.replicas[index].node.step(*message);
                    }
                    // One that does not lead any more drops it; its sender
                    // sends it again.
                    Carried::Forward(data) => {
                        let _ = self.replicas[index].node.propose(data);
                    }
                }
                self.flush(index);
            }
            let now = self.fabric.now();
            for index in 0..self.replicas.len() {
                if self.replicas[index].next_tick <= now {
                    self.replicas[index].node.tick().unwrap();
                    self.replicas[index].next_tick = now + PERIOD;
                    self.flush(index);
                }
            }
        }
    }
    fn propose(&mut self, track: Track, from: usize, data: &[u8]) {
        match track {
            Track::Fast => {
                // Refused while the member knows no leader, or holds all it
                // may: it is proposed again.
                let _ = self.replicas[from].node.propose_fast(data.to_vec());
                self.flush(from);
            }
            Track::Classic => {
                let leader = self.replicas[from].node.status().leader_id;
                if leader != 0 {
                    let _: Fate = self.fabric.send(
                        from as u64 + 1,
                        leader,
                        Carried::Forward(data.to_vec()),
                        data.len(),
                    );
                }
            }
        }
    }
}

/// How long the last member waits for each of `count` entries it proposes,
/// one after another.
fn measure(track: Track, members: u64, path: Path, count: usize, seed: u64) -> Measured {
    let mut sim = Sim::new(seed, members, path);
    sim.replicas[0].node.campaign().unwrap();
    sim.flush(0);
    let round = 2 * (path.one_way_ns() + path.jitter_ns());
    let settle = 40 * PERIOD + 8 * round;
    assert!(
        sim.run(settle, |sim| {
            sim.leader().is_some_and(|leader| {
                sim.replicas[leader].node.has_committed_current_term()
                    && sim
                        .replicas
                        .iter()
                        .all(|replica| replica.node.status().leader_id == leader as u64 + 1)
            })
        }),
        "no leader"
    );
    let from = (members - 1) as usize;
    // What a proposer waits before it proposes again: what the path takes
    // to carry its proposal there and the answer back, twice over.
    let patience = (4 * round).max(3 * PERIOD);
    let mut waits = Vec::with_capacity(count);
    let mut again = 0;
    for number in 0..count {
        let data = format!("entry {number:08}").into_bytes();
        let proposed = sim.fabric.now();
        let mut applied = false;
        for _ in 0..200 {
            sim.propose(track, from, &data);
            let deadline = sim.fabric.now() + patience;
            applied = sim.run(deadline, |sim| {
                sim.replicas[from]
                    .applied
                    .last()
                    .is_some_and(|last| *last == data)
                    || sim.replicas[from]
                        .applied
                        .iter()
                        .rev()
                        .take(4)
                        .any(|held| *held == data)
            });
            if applied {
                break;
            }
            again += 1;
        }
        assert!(applied, "entry {number} was never applied");
        waits.push(sim.fabric.now() - proposed);
    }
    // Every member applies what the proposer applied, once the path is
    // given the time.
    let last = sim.replicas[from].applied.last().unwrap().clone();
    let until = sim.fabric.now() + settle;
    assert!(sim.run(until, |sim| {
        sim.replicas.iter().all(|replica| {
            replica
                .applied
                .iter()
                .rev()
                .take(8)
                .any(|held| *held == last)
        })
    }));
    // What one member applied is the beginning of what the member that
    // applied most did: an entry proposed again may be committed twice, and
    // not every member has heard of the last of it.
    let longest = sim
        .replicas
        .iter()
        .map(|replica| &replica.applied)
        .max_by_key(|applied| applied.len())
        .unwrap();
    for (member, replica) in sim.replicas.iter().enumerate() {
        for (position, (applied, reference)) in replica.applied.iter().zip(longest).enumerate() {
            assert_eq!(
                applied,
                reference,
                "member {} applied another entry at position {position}",
                member + 1
            );
        }
    }
    waits.sort_unstable();
    Measured {
        mean: waits.iter().sum::<u64>() / waits.len() as u64,
        median: waits[waits.len() / 2],
        tail: waits[waits.len() * 99 / 100],
        again,
        by_fast_quorum: sim
            .replicas
            .iter()
            .map(|replica| replica.node.fast_stats().committed)
            .sum(),
    }
}

fn millis(ns: u64) -> f64 {
    ns as f64 / 1e6
}

/// With nothing lost the fast track takes three of the classic track's four
/// trips; with messages lost it is not the slower: the leader sends what it
/// took to its members whether or not the fast quorum comes.
///
/// `FOCAL_FAST_ENTRIES` sets how many entries each case proposes, and
/// `FOCAL_FAST_FULL` runs every case (09 records a run of it); without them
/// the cases are the ones the gate has the time for.
#[test]
fn the_fast_track_is_faster_where_nothing_is_lost_and_never_slower() {
    let full = std::env::var_os("FOCAL_FAST_FULL").is_some();
    // One case: `path:members:loss`.
    let only = std::env::var("FOCAL_FAST_CASE").ok();
    let count = std::env::var("FOCAL_FAST_ENTRIES")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(if full { 120usize } else { 32 });
    let cases: Vec<(&str, Path, u64, u32)> = if full {
        [("regional", Path::REGIONAL), ("lan", Path::LAN)]
            .into_iter()
            .flat_map(|(name, path)| {
                [3u64, 5].into_iter().flat_map(move |members| {
                    [0u32, 1, 2, 5, 10]
                        .into_iter()
                        .map(move |loss| (name, path, members, loss))
                })
            })
            .collect()
    } else {
        vec![
            ("regional", Path::REGIONAL, 3, 0),
            ("regional", Path::REGIONAL, 5, 0),
            ("regional", Path::REGIONAL, 5, 5),
            ("regional", Path::REGIONAL, 3, 10),
            ("lan", Path::LAN, 3, 0),
        ]
    };
    println!(
        "{:<10} {:>7} {:>6}  {:>26}  {:>26}  {:>6}  fast quorum",
        "path", "members", "loss", "classic mean/p50/p99 ms", "fast mean/p50/p99 ms", "ratio"
    );
    for (name, path, members, loss) in cases {
        if only
            .as_ref()
            .is_some_and(|only| *only != format!("{name}:{members}:{loss}"))
        {
            continue;
        }
        let lossy = path.with_loss(Loss::random(loss * 10_000));
        let classic = measure(Track::Classic, members, lossy, count, 7);
        let fast = measure(Track::Fast, members, lossy, count, 7);
        let ratio = fast.mean as f64 / classic.mean as f64;
        println!(
            "{name:<10} {members:>7} {loss:>5}%  {:>8.1}/{:>8.1}/{:>8.1}  {:>8.1}/{:>8.1}/{:>8.1}  {ratio:>6.2}  {} of {count} ({} and {} proposed again)",
            millis(classic.mean),
            millis(classic.median),
            millis(classic.tail),
            millis(fast.mean),
            millis(fast.median),
            millis(fast.tail),
            fast.by_fast_quorum,
            classic.again,
            fast.again,
        );
        assert_eq!(classic.by_fast_quorum, 0);
        if loss == 0 {
            assert!(
                fast.by_fast_quorum as usize >= count * 9 / 10,
                "{name} {members}: the fast quorum committed {} of {count}",
                fast.by_fast_quorum
            );
            assert!(
                ratio < 0.85,
                "{name} {members}: the fast track took {ratio:.2} of the classic"
            );
        }
        // Where messages are lost a few entries wait for a heartbeat to
        // send them again, by either track, and the mean of a run is
        // theirs: the track is judged by the entry in the middle.
        assert!(
            fast.median as f64 <= classic.median as f64 * 1.05,
            "{name} {members} at {loss}%: the fast track's median is {} and the classic's {}",
            fast.median,
            classic.median
        );
    }
}
