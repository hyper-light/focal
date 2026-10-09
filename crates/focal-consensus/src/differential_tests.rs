//! The differential (27 §15.10): one input stream goes to both backends, focal-log's and
//! hyper-durable's shell, and what their owners see must be the same.
//!
//! The draws are the same: each member's election seed is given (`NodeConfig::election_seed`, a
//! test's, never serialized). The owner drains to quiescence after every input, each drain waiting
//! for its write before anything is delivered, so both backends take the same `Ready`s in the same
//! order. After every round of drains, for each member, the backends must agree on what the owner
//! was handed: the committed entries and the changes of membership in order, the messages in order,
//! the snapshot, the applied index, and the read states as the set the round gave (the shell gives
//! a read once its apply reaches the index, where focal-log may give it a drain earlier for the
//! owner to park). After a restart they must agree on the member's scalars and its log's last
//! index. A round whose events differ fails, naming the member, the round and what differs.
use super::*;
use focal_memory::DiskBudgetConfig;
use hyper_block::buf::Alignment;
use hyper_block::file::{CachingRequest, DeviceFile};
use hyper_log::{Config as LogConfig, Log, Waits};
use std::collections::BTreeSet;
use tempfile::TempDir;

const CLUSTER: [u8; 16] = [5; 16];
const GROUP: [u8; 16] = [6; 16];
const LOG_ID: u128 = 0x0066_6f63_616c_6469_6666;
/// The drains a member may take to settle, and the rounds a cluster may take to go quiet: far
/// more than any test here needs, and one that keeps going past them is a fault the test names.
const DRAINS: usize = 1_000;
/// The largest entry the groups take: a frame of the shell's log holds it.
const ENTRY: usize = 1 << 20;
/// Each member's election seed: its id times this, so no two members draw alike.
const SEED: u64 = 0x9e37_79b9_7f4a_7c15;

/// The shell's log, as `shell_node_tests` sizes it: a frame holds an entry of `ENTRY`, and it
/// never waits for more submitters, each test driving its members' writes one at a time.
fn log_config() -> LogConfig {
    LogConfig {
        segment_bytes: 1024 * 4096,
        max_segments: 16,
        max_groups: 4,
        group_entries: 1 << 12,
        group_bytes: 8 << 20,
        group_cache: 1 << 16,
        queue_submissions: 64,
        waits: Waits::Never,
    }
}

fn no_needs(_: &[u8]) -> Option<[u8; 32]> {
    None
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Backend {
    Log,
    Shell,
}

/// One member on one backend: its node, then the shell's log if it has one, then its directory,
/// dropped in that order.
struct Member {
    backend: Backend,
    node: Option<DurableNode>,
    log: Option<Log<DeviceFile>>,
    dir: TempDir,
    config: NodeConfig,
    budget: MemoryBudget,
}

impl Member {
    fn new(backend: Backend, config: NodeConfig) -> Self {
        let mut member = Self {
            backend,
            node: None,
            log: None,
            dir: tempfile::tempdir().unwrap(),
            config,
            budget: MemoryBudget::new(256 * 1024 * 1024, 64 * 1024 * 1024).unwrap(),
        };
        member.open();
        member
    }

    /// Opens the member over its files, made the first time.
    fn open(&mut self) {
        match self.backend {
            Backend::Log => {
                self.node = Some(DurableNode::open(self.config.clone(), self.dir.path()).unwrap());
            }
            Backend::Shell => {
                let path = self.dir.path().join("raft.log");
                let fresh = !path.exists();
                let align = Alignment::new(4096).unwrap();
                let file =
                    DeviceFile::open(&path, true, CachingRequest::PreferDirect, align).unwrap();
                let log = if fresh {
                    Log::create(file, log_config(), LOG_ID).unwrap()
                } else {
                    Log::open(file, log_config(), LOG_ID).unwrap().0
                };
                let disk = DiskBudget::new(DiskBudgetConfig::unbounded()).unwrap();
                let node = DurableNode::open_on_shell(
                    self.config.clone(),
                    self.dir.path(),
                    &log,
                    &self.budget,
                    disk,
                    no_needs,
                )
                .unwrap();
                self.node = Some(node);
                self.log = Some(log);
            }
        }
    }

    /// Lets the member go and opens it again from its files, as a restart does.
    fn restart(&mut self) {
        self.node = None;
        self.log = None;
        self.open();
    }

    fn node(&mut self) -> &mut DurableNode {
        self.node.as_mut().unwrap()
    }

    /// Drains until nothing more is due, each drain waiting for its write, and then once more:
    /// an owner that owes every request its drain (focal's `Owner::drain_owed`) drains a member
    /// with nothing ready, and is given nothing.
    fn settle(&mut self) -> Vec<NodeEvents> {
        let mut drained = Vec::new();
        for _ in 0..DRAINS {
            if !self.node().has_ready() {
                let (backend, id) = (self.backend, self.config.node_id);
                let idle = self.node().drain().unwrap_or_else(|error| {
                    panic!("{backend:?} member {id}: a drain with nothing ready: {error:?}")
                });
                drained.push(idle);
                return drained;
            }
            drained.push(self.node().drain().unwrap());
        }
        panic!(
            "{:?} member {} never settled",
            self.backend, self.config.node_id
        );
    }
}

/// What one round of drains handed a member's owner, as the backends must agree on it.
#[derive(Debug, Default, PartialEq, Eq)]
struct Seen {
    committed: Vec<CommittedEntry>,
    membership: Vec<AppliedMembership>,
    messages: Vec<Message>,
    snapshots: Vec<AppliedSnapshot>,
    applied: u64,
    reads: BTreeSet<(u64, Vec<u8>)>,
}

impl Seen {
    fn of(drained: Vec<NodeEvents>) -> Self {
        let mut seen = Self::default();
        for events in drained {
            seen.committed.extend(events.committed.iter().cloned());
            seen.membership.extend(events.membership.iter().cloned());
            seen.messages.extend(events.messages.iter().cloned());
            seen.snapshots.extend(events.snapshot.clone());
            seen.applied = seen.applied.max(events.applied_index);
            seen.reads.extend(
                events
                    .read_states
                    .iter()
                    .map(|read| (read.index, read.context.clone())),
            );
        }
        seen
    }

    /// The first part the two differ in, named.
    fn difference(&self, other: &Self) -> Option<String> {
        let parts: [(&str, String, String); 6] = [
            (
                "committed",
                format!("{:?}", self.committed),
                format!("{:?}", other.committed),
            ),
            (
                "membership",
                format!("{:?}", self.membership),
                format!("{:?}", other.membership),
            ),
            (
                "messages",
                format!("{:?}", self.messages),
                format!("{:?}", other.messages),
            ),
            (
                "snapshots",
                format!("{:?}", self.snapshots),
                format!("{:?}", other.snapshots),
            ),
            (
                "applied",
                format!("{:?}", self.applied),
                format!("{:?}", other.applied),
            ),
            (
                "reads",
                format!("{:?}", self.reads),
                format!("{:?}", other.reads),
            ),
        ];
        parts
            .into_iter()
            .find(|(_, log, shell)| log != shell)
            .map(|(part, log, shell)| format!("{part}: focal-log {log}, shell {shell}"))
    }
}

/// The same members on both backends, driven by one input stream.
struct Twin {
    log: Vec<Member>,
    shell: Vec<Member>,
    /// The members cut off: what they send and what is sent to them is lost.
    cut: BTreeSet<u64>,
    rounds: usize,
}

impl Twin {
    /// `count` voters, seeded alike on both backends.
    fn new(count: u64) -> Self {
        let voters: Vec<u64> = (1..=count).collect();
        let members = |backend| {
            voters
                .iter()
                .map(|&id| {
                    let mut config = NodeConfig::single(id, CLUSTER, GROUP);
                    config.voters = voters.clone();
                    config.max_entry_bytes = ENTRY;
                    config.election_seed = Some(id.wrapping_mul(SEED));
                    Member::new(backend, config)
                })
                .collect()
        };
        Self {
            log: members(Backend::Log),
            shell: members(Backend::Shell),
            cut: BTreeSet::new(),
            rounds: 0,
        }
    }

    /// `act` on member `at` (from zero) of both backends; their answers must agree.
    fn on<R: std::fmt::Debug + PartialEq>(
        &mut self,
        at: usize,
        act: impl Fn(&mut DurableNode) -> R,
    ) -> R {
        let log = act(self.log[at].node());
        let shell = act(self.shell[at].node());
        assert_eq!(log, shell, "member {}: the backends answered apart", at + 1);
        log
    }

    /// `act` on every member of both backends.
    fn each(&mut self, act: impl Fn(&mut DurableNode)) {
        for member in self.log.iter_mut().chain(self.shell.iter_mut()) {
            act(member.node());
        }
    }

    /// Settles every member and delivers what each sent, round after round until a round sends
    /// nothing; after every round the backends must agree on what each member's owner was handed.
    fn pump(&mut self) {
        for _ in 0..DRAINS {
            self.rounds += 1;
            let (mut log_sent, mut shell_sent) = (Vec::new(), Vec::new());
            for at in 0..self.log.len() {
                let log = Seen::of(self.log[at].settle());
                let shell = Seen::of(self.shell[at].settle());
                if let Some(difference) = log.difference(&shell) {
                    panic!("round {}, member {}: {difference}", self.rounds, at + 1);
                }
                log_sent.extend(log.messages);
                shell_sent.extend(shell.messages);
            }
            if log_sent.is_empty() {
                return;
            }
            let cut = self.cut.clone();
            let delivered = |members: &mut Vec<Member>, sent: Vec<Message>| {
                for message in sent {
                    if cut.contains(&message.from) || cut.contains(&message.to) {
                        continue;
                    }
                    let to = usize::try_from(message.to).unwrap() - 1;
                    members[to].node().step(message).unwrap();
                }
            };
            delivered(&mut self.log, log_sent);
            delivered(&mut self.shell, shell_sent);
        }
        panic!("the clusters never went quiet");
    }

    /// Restarts member `at` on both backends; they must open to the same scalars and last index.
    fn restart(&mut self, at: usize) {
        self.log[at].restart();
        self.shell[at].restart();
        self.on(at, |node| format!("{:?}", node.scalars()));
        self.on(at, |node| {
            node.last_index().map_err(|error| format!("{error:?}"))
        })
        .unwrap();
    }

    /// One tick on every member, then the round.
    fn tick(&mut self) {
        self.each(|node| node.tick().unwrap());
        self.pump();
    }

    /// The member every member names leader, agreed on by both backends.
    fn leader(&mut self) -> Option<u64> {
        let all: Vec<usize> = (0..self.log.len()).collect();
        self.leader_among(&all)
    }

    /// The member the members at `ats` (from zero) all name leader, agreed on by both backends: a
    /// leader cut off still names itself, so the others are asked alone.
    fn leader_among(&mut self, ats: &[usize]) -> Option<u64> {
        let leaders: BTreeSet<u64> = ats
            .iter()
            .map(|at| self.on(*at, |node| node.status().leader_id))
            .collect();
        (leaders.len() == 1)
            .then(|| leaders.into_iter().next())
            .flatten()
            .filter(|id| *id != 0)
    }
}

/// An election, entries replicated and a follower's read: every round alike on both backends.
#[test]
fn both_backends_elect_replicate_and_read_alike() {
    let mut twin = Twin::new(3);
    twin.on(0, |node| node.campaign().map_err(|e| format!("{e:?}")))
        .unwrap();
    twin.pump();
    assert_eq!(twin.leader(), Some(1));
    for data in [b"a".as_slice(), b"bb", b"ccc", b"dddd"] {
        twin.on(0, |node| {
            node.propose(data.to_vec()).map_err(|e| format!("{e:?}"))
        })
        .unwrap();
        twin.pump();
    }
    twin.on(1, |node| {
        node.read_index(b"read".to_vec())
            .map_err(|e| format!("{e:?}"))
    })
    .unwrap();
    twin.pump();
}

/// Elections by timeout: the seeded draws give both backends the same candidates, and a leader
/// cut off is replaced and, healed, follows.
#[test]
fn both_backends_elect_by_timeout_alike_across_a_partition() {
    let mut twin = Twin::new(3);
    for _ in 0..200 {
        if twin.leader().is_some() {
            break;
        }
        twin.tick();
    }
    let first = twin.leader().expect("a leader by timeout");
    for data in [b"before".as_slice(), b"the cut"] {
        twin.on(usize::try_from(first).unwrap() - 1, |node| {
            node.propose(data.to_vec()).map_err(|e| format!("{e:?}"))
        })
        .unwrap();
        twin.pump();
    }
    twin.cut.insert(first);
    let uncut: Vec<usize> = (1..=3u64)
        .filter(|id| *id != first)
        .map(|id| usize::try_from(id).unwrap() - 1)
        .collect();
    // The members left name the cut leader until their timeouts run out, then one of their own.
    let mut replaced = None;
    for _ in 0..200 {
        replaced = twin.leader_among(&uncut).filter(|leader| *leader != first);
        if replaced.is_some() {
            break;
        }
        twin.tick();
    }
    assert!(
        replaced.is_some(),
        "the members left elect a leader of their own"
    );
    twin.cut.clear();
    for _ in 0..20 {
        twin.tick();
    }
    assert!(twin.leader().is_some(), "one leader once healed");
}

/// A member restarted between entries opens to the same state on both backends, and the group
/// goes on alike.
#[test]
fn both_backends_restart_to_the_same_state() {
    let mut twin = Twin::new(3);
    twin.on(0, |node| node.campaign().map_err(|e| format!("{e:?}")))
        .unwrap();
    twin.pump();
    for data in [b"one".as_slice(), b"two"] {
        twin.on(0, |node| {
            node.propose(data.to_vec()).map_err(|e| format!("{e:?}"))
        })
        .unwrap();
        twin.pump();
    }
    twin.restart(1);
    twin.pump();
    twin.on(0, |node| {
        node.propose(b"three".to_vec())
            .map_err(|e| format!("{e:?}"))
    })
    .unwrap();
    twin.pump();
    twin.restart(0);
    twin.pump();
}

/// A checkpoint of a prefix captured earlier, made after the replica went on, is taken at the
/// captured point with the entries after it kept, on both backends; once a later checkpoint
/// supersedes it, the old point is refused. (The restart from a deferred image and its replay
/// of the entries after it: `a_deferred_seeded_checkpoint_lands_at_its_point_while_the_replica_goes_on`.)
#[test]
fn both_backends_checkpoint_a_captured_prefix_after_going_on() {
    for backend in [Backend::Log, Backend::Shell] {
        let mut config = NodeConfig::single(1, CLUSTER, GROUP);
        config.max_entry_bytes = ENTRY;
        let mut member = Member::new(backend, config);
        member.node().campaign().unwrap();
        member.settle();
        for data in [b"one".as_slice(), b"two"] {
            member.node().propose(data.to_vec()).unwrap();
            member.settle();
        }
        let point = member.node().checkpoint_point().unwrap();
        let captured = point.index;
        // The replica goes on while the captured state is made durable elsewhere.
        for data in [b"three".as_slice(), b"four"] {
            member.node().propose(data.to_vec()).unwrap();
            member.settle();
        }
        let image = b"state through two".to_vec();
        let allocation = member
            .budget
            .reserve(BudgetKind::Recovery, BudgetLane::Completion, 4096)
            .unwrap()
            .commit();
        member
            .node()
            .begin_checkpoint_from_funded(point.clone(), image.clone(), allocation)
            .unwrap_or_else(|error| panic!("{backend:?}: {error:?}"));
        member.node().finish_checkpoint().unwrap();
        member.settle();
        // Superseded once a later checkpoint lands: the old point is refused, retryably.
        member.node().propose(b"five".to_vec()).unwrap();
        member.settle();
        let later = member.node().checkpoint_point().unwrap();
        let allocation = member
            .budget
            .reserve(BudgetKind::Recovery, BudgetLane::Completion, 4096)
            .unwrap()
            .commit();
        member
            .node()
            .begin_checkpoint_from_funded(later, b"state through five".to_vec(), allocation)
            .unwrap();
        member.node().finish_checkpoint().unwrap();
        member.settle();
        let allocation = member
            .budget
            .reserve(BudgetKind::Recovery, BudgetLane::Completion, 4096)
            .unwrap()
            .commit();
        assert!(matches!(
            member
                .node()
                .begin_checkpoint_from_funded(point, image.clone(), allocation),
            Err(ConsensusError::CheckpointIndex)
        ));
        member.restart();
        let seen = Seen::of(member.settle());
        let snapshot = seen
            .snapshots
            .first()
            .unwrap_or_else(|| panic!("{backend:?}: no image on restart: {seen:?}"));
        assert!(snapshot.index > captured, "{backend:?}");
        assert_eq!(snapshot.data, b"state through five", "{backend:?}");
        let after: Vec<&[u8]> = seen
            .committed
            .iter()
            .filter(|entry| !entry.data.is_empty())
            .map(|entry| entry.data.as_slice())
            .collect();
        assert!(after.is_empty(), "{backend:?}: {after:?}");
    }
}
