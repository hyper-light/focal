//! A group of members on a network the schedule owns: it delivers, loses,
//! repeats and holds back messages, stops and reopens members, and compacts
//! their logs. What a schedule does is decided from what the members are,
//! so two groups that are alike are scheduled alike.
use std::collections::BTreeMap;

use focal_raft::proto::{
    ConfChangeSingle, ConfChangeTransition, ConfChangeType, ConfChangeV2, ConfState, Message,
    MessageType,
};

use super::{Output, Replica, Said, Seeded, Settings, Store, View, members, votes};

/// The most messages the network holds; the oldest is lost for a new one.
const NETWORK: usize = 2048;

#[derive(Clone, Debug)]
pub enum Op {
    Deliver {
        at: usize,
        keep: bool,
        lose: bool,
    },
    Tick(u64),
    Propose(u64, Vec<u8>),
    /// By the fast track.
    Fast(u64, Vec<u8>),
    Change(u64, ConfChangeV2),
    Transfer(u64, u64),
    Read(u64, Vec<u8>),
    /// Several reads asked of one member before it is asked what there is
    /// to do: this core sends one round for them.
    Reads(u64, Vec<Vec<u8>>),
    Restart(u64),
    Compact(u64),
    Block(u64, u64),
    Heal,
    Priority(u64, i64),
    /// The member is told what the path to another carries before it
    /// answers.
    Window(u64, u64, u64),
    Campaign(u64),
    Unreachable(u64, u64),
    Ping(u64),
}

/// What one member did in an operation.
#[derive(Debug, PartialEq)]
pub struct Report {
    pub member: u64,
    pub accepted: Option<bool>,
    pub output: Output,
    pub view: View,
}

/// What a schedule may do, in parts of a hundred of its steps.
#[derive(Clone, Copy, Debug)]
pub struct Mix {
    pub changes: bool,
    /// Whether a change may remove or demote the member that leads.
    pub leader_leaves: bool,
    pub restarts: bool,
    pub compaction: bool,
    pub partitions: bool,
    pub priorities: bool,
    /// Whether the bytes a member is sent ahead of its answers change
    /// while a schedule runs. Where the cores are compared they do not:
    /// `raft-rs` has no such bound.
    pub windows: bool,
    /// Whether reads are asked several at a time. Where the cores are
    /// compared they are not: `raft-rs` sends a round for each.
    pub bursts: bool,
    /// Of a hundred proposals, how many go by the fast track.
    pub fast: u64,
    pub lose: u64,
    pub repeat: u64,
}
impl Mix {
    pub fn everything() -> Self {
        Self {
            changes: true,
            leader_leaves: false,
            restarts: true,
            compaction: true,
            partitions: true,
            priorities: true,
            windows: false,
            bursts: false,
            fast: 0,
            lose: 8,
            repeat: 5,
        }
    }
}

pub struct Cluster<R> {
    pub nodes: Vec<Option<R>>,
    pub stores: Vec<Store>,
    pub net: Vec<Message>,
    pub blocked: Vec<(u64, u64)>,
    pub settings: Settings,
    pub seed: u64,
    /// What was committed at each index, by whoever committed it first.
    pub chosen: BTreeMap<u64, Said>,
    /// Who led each term.
    pub leaders: BTreeMap<u64, u64>,
    /// How often a member that led a group it was no voter of was stopped
    /// for it. `raft-rs` leads on when a change it applies leaves it none,
    /// and its group follows it and commits nothing.
    pub deposed: u64,
    /// Whether such a member is stopped. Where the cores are compared it
    /// is not: the comparison ends there.
    pub stop_who_left: bool,
    reads: u64,
    /// For each read that waits, the highest index any member had
    /// committed when it was asked: what it is answered with is no less.
    asked: BTreeMap<Vec<u8>, u64>,
    /// How many reads were answered.
    pub answered: u64,
    opened: u64,
}

impl<R: Replica> Cluster<R> {
    pub fn new(count: u64, voters: &[u64], settings: Settings, seed: u64) -> Self {
        let boot = ConfState {
            voters: voters.to_vec(),
            ..ConfState::default()
        };
        let stores: Vec<Store> = (0..count).map(|_| Store::new(boot.clone())).collect();
        let mut cluster = Self {
            nodes: Vec::new(),
            stores,
            net: Vec::new(),
            blocked: Vec::new(),
            settings,
            seed,
            chosen: BTreeMap::new(),
            leaders: BTreeMap::new(),
            deposed: 0,
            stop_who_left: false,
            reads: 0,
            asked: BTreeMap::new(),
            answered: 0,
            opened: 0,
        };
        for id in 1..=count {
            let node = cluster.open(id);
            cluster.nodes.push(Some(node));
        }
        cluster
    }
    fn open(&mut self, id: u64) -> R {
        self.opened += 1;
        R::open(
            id,
            self.stores[(id - 1) as usize].clone(),
            &self.settings,
            self.seed
                .wrapping_mul(1_000_003)
                .wrapping_add(id * 7919 + self.opened),
        )
    }
    pub fn node(&mut self, id: u64) -> Option<&mut R> {
        self.nodes.get_mut((id - 1) as usize)?.as_mut()
    }
    pub fn peek(&self, id: u64) -> Option<&R> {
        self.nodes.get((id - 1) as usize)?.as_ref()
    }
    pub fn ids(&self) -> Vec<u64> {
        (1..=self.nodes.len() as u64).collect()
    }
    pub fn up(&self) -> Vec<u64> {
        self.ids()
            .into_iter()
            .filter(|id| self.peek(*id).is_some())
            .collect()
    }
    pub fn leaders_now(&self) -> Vec<u64> {
        self.up()
            .into_iter()
            .filter(|id| self.peek(*id).is_some_and(|node| node.view().role == 2))
            .collect()
    }
    fn is_blocked(&self, from: u64, to: u64) -> bool {
        self.blocked.contains(&(from, to))
    }

    /// What the member did, recorded and checked against what the group
    /// holds true whatever the schedule.
    fn report(&mut self, member: u64, accepted: Option<bool>) -> Report {
        let node = self.node(member).expect("a member that is up");
        let output = node.drain();
        let view = node.view();
        for committed in &output.committed {
            // What an entry states, and not the term it bears: by the fast
            // track the leader that took an entry and the leader that took
            // it again at its election each gave it its own.
            let stated = |said: &Said| (said.0, said.2, said.3.clone());
            match self.chosen.get(&committed.0) {
                Some(chosen) => assert_eq!(
                    stated(chosen),
                    stated(committed),
                    "seed {}: member {member} committed another entry at {}",
                    self.seed,
                    committed.0
                ),
                None => {
                    self.chosen.insert(committed.0, committed.clone());
                }
            }
        }
        for (index, context) in &output.reads {
            // A read sees what was committed before it was asked, whoever
            // leads by the time it is answered: a leader that was deposed
            // meanwhile answers nothing.
            if let Some(floor) = self.asked.remove(context) {
                assert!(
                    *index >= floor,
                    "seed {}: member {member} answered a read at {index}, asked when {floor} was committed",
                    self.seed
                );
                self.answered += 1;
            }
        }
        if view.role == 2 {
            let leader = *self.leaders.entry(view.term).or_insert(member);
            assert_eq!(
                leader, member,
                "seed {}: two leaders of term {}",
                self.seed, view.term
            );
        }
        for message in &output.messages {
            if self.net.len() >= NETWORK {
                self.net.remove(0);
            }
            self.net.push(message.clone());
        }
        let conf = self.stores[(member - 1) as usize].0.borrow().conf.clone();
        if self.stop_who_left && view.role == 2 && !votes(&conf, member) {
            // It leads a group it is no voter of, and unwinds when it next
            // commits. Its owner stops it, and it opens as what it is.
            self.deposed += 1;
            self.nodes[(member - 1) as usize] = None;
            let node = self.open(member);
            self.nodes[(member - 1) as usize] = Some(node);
        }
        Report {
            member,
            accepted,
            output,
            view,
        }
    }

    pub fn act(&mut self, op: &Op) -> Vec<Report> {
        let mut reports = Vec::new();
        match op {
            Op::Deliver { at, keep, lose } => {
                let message = if *keep {
                    self.net[*at].clone()
                } else {
                    self.net.remove(*at)
                };
                let snapshot = message.msg_type == MessageType::MsgSnapshot as i32;
                let (from, to) = (message.from, message.to);
                let arrives = !*lose && !self.is_blocked(from, to) && self.peek(to).is_some();
                if arrives {
                    let accepted = self.node(to).map(|node| node.step(message));
                    reports.push(self.report(to, accepted));
                }
                // Who sent a snapshot is told what became of it.
                if snapshot && !*keep && self.peek(from).is_some() {
                    if let Some(node) = self.node(from) {
                        node.snapshot_status(to, arrives);
                    }
                    reports.push(self.report(from, None));
                }
            }
            Op::Tick(id) => {
                let accepted = self.node(*id).map(|node| node.tick());
                reports.push(self.report(*id, accepted));
            }
            Op::Propose(id, data) => {
                let accepted = self.node(*id).map(|node| node.propose(data.clone()));
                reports.push(self.report(*id, accepted));
            }
            Op::Fast(id, data) => {
                let accepted = self
                    .node(*id)
                    .map(|node| node.propose_fast(data.clone()).is_some());
                reports.push(self.report(*id, accepted));
            }
            Op::Change(id, change) => {
                let accepted = self.node(*id).map(|node| node.propose_change(change));
                reports.push(self.report(*id, accepted));
            }
            Op::Transfer(id, to) => {
                if let Some(node) = self.node(*id) {
                    node.transfer(*to);
                }
                reports.push(self.report(*id, None));
            }
            Op::Read(id, context) => {
                let floor = self.chosen.keys().next_back().copied().unwrap_or(0);
                self.asked.insert(context.clone(), floor);
                if let Some(node) = self.node(*id) {
                    node.read(context.clone());
                }
                reports.push(self.report(*id, None));
            }
            Op::Reads(id, contexts) => {
                let floor = self.chosen.keys().next_back().copied().unwrap_or(0);
                for context in contexts {
                    self.asked.insert(context.clone(), floor);
                    if let Some(node) = self.node(*id) {
                        node.read(context.clone());
                    }
                }
                reports.push(self.report(*id, None));
            }
            Op::Restart(id) => {
                // What was not durable is gone; what was is what it opens on.
                self.nodes[(*id - 1) as usize] = None;
                let node = self.open(*id);
                self.nodes[(*id - 1) as usize] = Some(node);
                reports.push(self.report(*id, None));
            }
            Op::Compact(id) => {
                let accepted = self.node(*id).map(|node| node.compact());
                reports.push(self.report(*id, accepted));
            }
            Op::Block(from, to) => {
                if !self.is_blocked(*from, *to) {
                    self.blocked.push((*from, *to));
                }
            }
            Op::Heal => self.blocked.clear(),
            Op::Priority(id, priority) => {
                if let Some(node) = self.node(*id) {
                    node.set_priority(*priority);
                }
                reports.push(self.report(*id, None));
            }
            Op::Window(id, member, bytes) => {
                if let Some(node) = self.node(*id) {
                    node.set_window(*member, *bytes);
                }
                reports.push(self.report(*id, None));
            }
            Op::Campaign(id) => {
                let accepted = self.node(*id).map(|node| node.campaign());
                reports.push(self.report(*id, accepted));
            }
            Op::Unreachable(id, member) => {
                if let Some(node) = self.node(*id) {
                    node.unreachable(*member);
                }
                reports.push(self.report(*id, None));
            }
            Op::Ping(id) => {
                if let Some(node) = self.node(*id) {
                    node.ping();
                }
                reports.push(self.report(*id, None));
            }
        }
        reports
    }

    fn change(&self, rng: &mut Seeded, leader: u64, mix: &Mix) -> ConfChangeV2 {
        let conf = self.stores[(leader - 1) as usize].0.borrow().conf.clone();
        let joint = !conf.voters_outgoing.is_empty();
        if joint && rng.chance(70) {
            // Out of the joint configuration.
            return ConfChangeV2::default();
        }
        if rng.chance(4) {
            // A change that leaves no voter is committed and never applied.
            return ConfChangeV2 {
                changes: conf
                    .voters
                    .iter()
                    .map(|voter| ConfChangeSingle {
                        change_type: ConfChangeType::RemoveNode as i32,
                        node_id: *voter,
                    })
                    .collect(),
                ..ConfChangeV2::default()
            };
        }
        let held = members(&conf);
        let leading = self.leaders_now();
        let spared = |id: &u64| mix.leader_leaves || (*id != leader && !leading.contains(id));
        let outside: Vec<u64> = self
            .ids()
            .into_iter()
            .filter(|id| !held.contains(id))
            .collect();
        let voters: Vec<u64> = conf.voters.iter().copied().filter(spared).collect();
        let inside: Vec<u64> = held.iter().copied().filter(spared).collect();
        let one = |rng: &mut Seeded| -> Option<ConfChangeSingle> {
            let (kind, member) = match rng.below(6) {
                0 => (ConfChangeType::AddLearnerNode, rng.pick(&outside)?),
                1 => (ConfChangeType::AddNode, rng.pick(&outside)?),
                2 => (ConfChangeType::AddNode, rng.pick(&conf.learners)?),
                3 => (ConfChangeType::RemoveNode, rng.pick(&inside)?),
                4 => (ConfChangeType::AddLearnerNode, rng.pick(&voters)?),
                _ => (ConfChangeType::RemoveNode, rng.pick(&conf.learners)?),
            };
            Some(ConfChangeSingle {
                change_type: kind as i32,
                node_id: member,
            })
        };
        let count = if rng.chance(60) { 1 } else { 1 + rng.below(3) };
        let mut changes = Vec::new();
        for _ in 0..count * 4 {
            if changes.len() as u64 == count {
                break;
            }
            if let Some(change) = one(rng) {
                changes.push(change);
            }
        }
        let transition = match rng.below(10) {
            0 => ConfChangeTransition::Implicit,
            1 => ConfChangeTransition::Explicit,
            _ => ConfChangeTransition::Auto,
        };
        ConfChangeV2 {
            transition: transition as i32,
            changes,
            context: vec![],
        }
    }

    /// The next step of the schedule.
    pub fn choose(&mut self, rng: &mut Seeded, mix: &Mix) -> Op {
        let up = self.up();
        let all = self.ids();
        let leaders = self.leaders_now();
        let any = |rng: &mut Seeded| rng.pick(&up).unwrap_or(1);
        // Where a leader is wanted and none is known, any member is asked:
        // what one that does not lead does with it is compared as well.
        let leader = |rng: &mut Seeded| {
            if rng.chance(90) {
                rng.pick(&leaders).unwrap_or_else(|| any(rng))
            } else {
                any(rng)
            }
        };
        for _ in 0..16 {
            let drawn = rng.below(100);
            match drawn {
                0..=49 if !self.net.is_empty() => {
                    return Op::Deliver {
                        at: rng.below(self.net.len() as u64) as usize,
                        keep: rng.chance(mix.repeat),
                        lose: rng.chance(mix.lose),
                    };
                }
                0..=74 if !up.is_empty() => return Op::Tick(any(rng)),
                75..=84 if !up.is_empty() => {
                    let size = 1 + rng.below(48) as usize;
                    let data = (0..size).map(|_| rng.next() as u8).collect();
                    if rng.chance(mix.fast) {
                        // From any member: the fast track is for those
                        // that do not lead.
                        return Op::Fast(any(rng), data);
                    }
                    return Op::Propose(leader(rng), data);
                }
                85..=87 if mix.changes && !up.is_empty() => {
                    let at = leader(rng);
                    return Op::Change(at, self.change(rng, at, mix));
                }
                88..=89 if !up.is_empty() => {
                    return Op::Transfer(leader(rng), rng.pick(&all).unwrap_or(1));
                }
                90..=91 if !up.is_empty() => {
                    if mix.bursts && rng.chance(50) {
                        let contexts = (0..2 + rng.below(7))
                            .map(|_| {
                                self.reads += 1;
                                self.reads.to_le_bytes().to_vec()
                            })
                            .collect();
                        return Op::Reads(leader(rng), contexts);
                    }
                    self.reads += 1;
                    return Op::Read(leader(rng), self.reads.to_le_bytes().to_vec());
                }
                92 if mix.restarts => return Op::Restart(rng.pick(&all).unwrap_or(1)),
                93 if mix.compaction && !up.is_empty() => return Op::Compact(any(rng)),
                94 if mix.partitions => {
                    let from = rng.pick(&all).unwrap_or(1);
                    let to = rng.pick(&all).unwrap_or(1);
                    if from != to {
                        return Op::Block(from, to);
                    }
                }
                95 if mix.partitions => return Op::Heal,
                96 if mix.windows && !up.is_empty() && rng.chance(50) => {
                    // From less than one entry to more than a schedule
                    // ever has in flight.
                    let bytes = [1, 24, 96, 512, 1 << 20][rng.below(5) as usize];
                    return Op::Window(leader(rng), rng.pick(&all).unwrap_or(1), bytes);
                }
                96 if mix.priorities && !up.is_empty() => {
                    return Op::Priority(any(rng), rng.below(4) as i64);
                }
                97 if !up.is_empty() => return Op::Campaign(any(rng)),
                98 if !up.is_empty() => {
                    return Op::Unreachable(leader(rng), rng.pick(&all).unwrap_or(1));
                }
                99 if !up.is_empty() => return Op::Ping(leader(rng)),
                _ => {}
            }
        }
        Op::Heal
    }

    /// With the network whole and every member up, the group elects and
    /// commits: within `budget` rounds of ticks and deliveries a proposal
    /// is applied by every member of the configuration.
    pub fn settles(&mut self, budget: usize) -> bool {
        self.blocked.clear();
        for id in self.ids() {
            if self.peek(id).is_none() {
                self.act(&Op::Restart(id));
            }
        }
        let mut proposed = None;
        for round in 0..budget {
            // A member that joined after the leader's snapshot was taken is
            // not named by it and discards it: it is seeded by a snapshot
            // taken since, which is the owner's to take.
            if round % 16 == 15 {
                for leader in self.leaders_now() {
                    self.act(&Op::Compact(leader));
                }
            }

            while !self.net.is_empty() {
                self.act(&Op::Deliver {
                    at: 0,
                    keep: false,
                    lose: false,
                });
            }
            if let Some(index) = proposed {
                let leader = self.leaders_now().into_iter().next();
                let conf =
                    leader.map(|leader| self.stores[(leader - 1) as usize].0.borrow().conf.clone());
                if let Some(conf) = conf
                    && members(&conf).iter().all(|member| {
                        self.peek(*member)
                            .is_some_and(|node| node.app().index >= index)
                    })
                    && members(&conf).iter().any(|member| votes(&conf, *member))
                {
                    return true;
                }
            } else if let Some(leader) = self
                .leaders_now()
                .into_iter()
                .max_by_key(|id| self.peek(*id).map_or(0, |node| node.view().term))
            {
                let reports = self.act(&Op::Propose(leader, b"settled".to_vec()));
                if reports.iter().any(|report| report.accepted == Some(true)) {
                    proposed = self.peek(leader).map(|node| node.view().last_index);
                }
            }
            for id in self.ids() {
                self.act(&Op::Tick(id));
            }
        }
        for id in self.ids() {
            let view = self.peek(id).map(|node| node.view());
            let conf = self.stores[(id - 1) as usize].0.borrow().conf.clone();
            println!("member {id}: proposed {proposed:?} {conf:?}\n  {view:?}");
        }
        false
    }
}
