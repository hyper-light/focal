//! Members on the shell, end to end: each member's log a hyper-log log on a real file of its own,
//! its group's records and image in its own directory, opened again from both as a restart does.
use super::*;
use focal_memory::DiskBudgetConfig;
use hyper_block::buf::Alignment;
use hyper_block::file::CachingRequest;
use hyper_log::Log;
use hyper_log::{Config as LogConfig, Waits};
use tempfile::TempDir;

const CLUSTER: [u8; 16] = [3; 16];
const GROUP: [u8; 16] = [4; 16];
const LOG_ID: u128 = 0x0066_6f63_616c_5f6c_6f67;
/// A decoder the tests' managed entries need.
const MANAGED: [u8; 32] = [7; 32];
/// The drains a test lets settle take at most: far more than any test here needs, and a
/// member that keeps giving past it is a fault the test names.
const DRAINS: usize = 1_000;

/// The largest entry the tests' groups take: a frame of their log holds it.
const ENTRY: usize = 1 << 20;

/// A log whose frame holds an entry of `ENTRY`, which never waits for more submitters between
/// frames: each test drives its members' writes one at a time.
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

fn managed(data: &[u8]) -> Option<[u8; 32]> {
    data.starts_with(b"managed").then_some(MANAGED)
}

/// One member: its node, then the log it writes, then the directory both live in, dropped in
/// that order.
struct Member {
    node: Option<DurableNode>,
    log: Option<Log<DeviceFile>>,
    dir: TempDir,
    config: NodeConfig,
    budget: MemoryBudget,
    needs: Needs,
}

impl Member {
    fn new(config: NodeConfig, needs: Needs) -> Self {
        let budget = MemoryBudget::new(256 * 1024 * 1024, 64 * 1024 * 1024).unwrap();
        Self::within(config, needs, budget)
    }

    fn within(config: NodeConfig, needs: Needs, budget: MemoryBudget) -> Self {
        let mut member = Self {
            node: None,
            log: None,
            dir: tempfile::tempdir().unwrap(),
            config,
            budget,
            needs,
        };
        member.open();
        member
    }

    /// Opens the log on its file, made the first time, and the member over it.
    fn open(&mut self) {
        let path = self.dir.path().join("raft.log");
        let fresh = !path.exists();
        let align = Alignment::new(4096).unwrap();
        let file = DeviceFile::open(&path, true, CachingRequest::PreferDirect, align).unwrap();
        let log = if fresh {
            Log::create(file, log_config(), LOG_ID).unwrap()
        } else {
            Log::open(file, log_config(), LOG_ID).unwrap().0
        };
        let disk = DiskBudget::new(DiskBudgetConfig::unbounded()).unwrap();
        let node = DurableNode::open_on_shell(
            self.config.clone(),
            self.dir.path(),
            &log.opener(),
            &self.budget,
            disk,
            self.needs,
        )
        .unwrap();
        self.node = Some(node);
        self.log = Some(log);
    }

    /// Lets the member go and opens it again from its log and its files.
    fn restart(&mut self) {
        self.node = None;
        self.log = None;
        self.open();
    }

    fn node(&mut self) -> &mut DurableNode {
        self.node.as_mut().unwrap()
    }

    /// Drains until nothing more is due, every drain's events gathered in order.
    fn settle(&mut self) -> Vec<NodeEvents> {
        let mut drained = Vec::new();
        for _ in 0..DRAINS {
            if !self.node().has_ready() {
                return drained;
            }
            drained.push(self.node().drain().unwrap());
        }
        panic!("the member never settled");
    }
}

/// What the owner was handed to apply, in order: each committed entry's index and data.
fn committed(drained: &[NodeEvents]) -> Vec<(u64, Vec<u8>)> {
    drained
        .iter()
        .flat_map(|events| events.committed.iter())
        .map(|entry| (entry.index, entry.data.clone()))
        .collect()
}

/// Member `id` of a group whose entries are at most `ENTRY`.
fn config(id: u64) -> NodeConfig {
    let mut config = NodeConfig::single(id, CLUSTER, GROUP);
    config.max_entry_bytes = ENTRY;
    config
}

fn sole(needs: Needs) -> Member {
    Member::new(config(1), needs)
}

/// A sole member elects itself: its campaign, then the drains that make its term and its empty
/// entry durable.
fn elected(member: &mut Member) {
    member.node().campaign().unwrap();
    member.settle();
    assert_eq!(member.node().scalars().role, StateRole::Leader);
}

/// Contract (a): what a group commits reaches the owner in index order and once, and a restart
/// hands over again exactly what is committed above the group's image (none here), in order.
#[test]
fn a_sole_member_hands_over_in_order_and_once_and_replays_after_a_restart() {
    let mut member = sole(no_needs);
    elected(&mut member);
    for data in [b"a", b"b", b"c"] {
        member.node().propose(data.to_vec()).unwrap();
    }
    let drained = member.settle();
    let expected = vec![(2, b"a".to_vec()), (3, b"b".to_vec()), (4, b"c".to_vec())];
    assert_eq!(committed(&drained), expected);
    assert_eq!(member.node().scalars().applied_index, 4);
    assert!(committed(&member.settle()).is_empty(), "handed over once");
    member.restart();
    let drained = member.settle();
    assert!(drained.iter().all(|events| events.snapshot.is_none()));
    assert_eq!(committed(&drained), expected);
    assert_eq!(member.node().scalars().applied_index, 4);
}

/// Contract (e): a checkpoint is the image a restart opens at. The owner is handed it as the
/// state it restores, then what was committed after it, and the log no longer starts before it.
#[test]
fn a_checkpoint_is_the_image_a_restart_opens_at() {
    let mut member = sole(no_needs);
    elected(&mut member);
    for data in [b"a", b"b", b"c"] {
        member.node().propose(data.to_vec()).unwrap();
    }
    member.settle();
    assert!(matches!(
        member.node().checkpoint(3, b"stale".to_vec()),
        Err(ConsensusError::CheckpointIndex)
    ));
    member.node().checkpoint(4, b"state".to_vec()).unwrap();
    assert_eq!(member.node().snapshot_index(), 4);
    member.node().propose(b"d".to_vec()).unwrap();
    assert_eq!(committed(&member.settle()), vec![(5, b"d".to_vec())]);
    member.restart();
    let drained = member.settle();
    let snapshot = drained
        .first()
        .and_then(|events| events.snapshot.clone())
        .unwrap();
    assert_eq!(
        (snapshot.index, snapshot.data.as_slice()),
        (4, &b"state"[..])
    );
    assert_eq!(
        snapshot.configuration,
        MembershipConfiguration::from_conf(&ConfState {
            voters: vec![1],
            ..ConfState::default()
        })
    );
    assert_eq!(committed(&drained), vec![(5, b"d".to_vec())]);
    assert_eq!(
        member.node().published_term(5).unwrap(),
        member.node().scalars().term
    );
}

/// Three members, each on its own log and files.
struct Cluster {
    members: Vec<Member>,
    applied: Vec<Vec<(u64, Vec<u8>)>>,
}

impl Cluster {
    fn new() -> Self {
        let members = (1..=3)
            .map(|id| {
                let mut config = config(id);
                config.voters = vec![1, 2, 3];
                Member::new(config, no_needs)
            })
            .collect();
        Self {
            members,
            applied: vec![Vec::new(); 3],
        }
    }

    /// Drains every member and delivers what each sends, until a whole round sends nothing.
    fn pump(&mut self) -> Vec<ReadBarrier> {
        let mut reads = Vec::new();
        for _ in 0..DRAINS {
            let mut messages = Vec::new();
            for (at, member) in self.members.iter_mut().enumerate() {
                for events in member.settle() {
                    self.applied[at].extend(
                        events
                            .committed
                            .iter()
                            .map(|entry| (entry.index, entry.data.clone())),
                    );
                    reads.extend(events.read_states.iter().cloned());
                    messages.extend(events.messages);
                }
            }
            if messages.is_empty() {
                return reads;
            }
            for message in messages {
                let to = usize::try_from(message.to).unwrap() - 1;
                self.members[to].node().step(message).unwrap();
            }
        }
        panic!("the cluster never went quiet");
    }
}

/// A group of three elects, replicates and serves a follower's read through its leader, every
/// member handed the same entries in the same order.
#[test]
fn three_members_on_the_shell_elect_replicate_and_read() {
    let mut cluster = Cluster::new();
    cluster.members[0].node().campaign().unwrap();
    cluster.pump();
    assert_eq!(cluster.members[0].node().scalars().role, StateRole::Leader);
    for data in [b"x", b"y"] {
        cluster.members[0].node().propose(data.to_vec()).unwrap();
    }
    cluster.pump();
    let expected = vec![(2, b"x".to_vec()), (3, b"y".to_vec())];
    for applied in &cluster.applied {
        assert_eq!(applied, &expected);
    }
    cluster.members[1]
        .node()
        .read_index(b"read".to_vec())
        .unwrap();
    let reads = cluster.pump();
    assert_eq!(
        reads,
        vec![ReadBarrier {
            index: 3,
            context: b"read".to_vec()
        }]
    );
}

/// Contract (f): a write holding an entry that needs a decoder the group's records do not state
/// waits, whole, and the member refuses input as focal-log refuses while its floor write is
/// pending; once the record is durable the write goes out, and a restart requires the decoder.
#[test]
fn a_write_needing_an_unstated_decoder_waits_for_its_record() {
    let mut member = sole(managed);
    elected(&mut member);
    member.node().propose(b"managed 1".to_vec()).unwrap();
    assert!(committed(&member.settle()).is_empty(), "held, not written");
    assert!(matches!(
        member.node().propose(b"managed 2".to_vec()),
        Err(ConsensusError::PersistencePending)
    ));
    member.node().confirm_decoder(MANAGED).unwrap();
    member.node().begin_decoder_floor(MANAGED).unwrap();
    member.node().finish_decoder_floor().unwrap();
    assert!(member.node().decoder_floor_ready(MANAGED));
    assert_eq!(
        committed(&member.settle()),
        vec![(2, b"managed 1".to_vec())]
    );
    member.restart();
    assert_eq!(member.node().required_decoder(), Some(MANAGED));
    assert!(matches!(
        member.node().drain(),
        Err(ConsensusError::DecoderUnconfirmed)
    ));
    member.node().confirm_decoder(MANAGED).unwrap();
    assert_eq!(
        committed(&member.settle()),
        vec![(2, b"managed 1".to_vec())]
    );
}

/// Contract (g): what a drive hands over is reserved before it. A budget that cannot hold it
/// refuses the drain with `Capacity`, nothing taken; once memory returns, the drain hands over
/// all of it.
#[test]
fn a_drain_the_budget_cannot_hold_is_refused_and_nothing_is_lost() {
    let parent = MemoryBudget::new(256 * 1024 * 1024, 64 * 1024 * 1024).unwrap();
    let mut member = Member::within(config(1), no_needs, parent.clone());
    elected(&mut member);
    let data = vec![9; ENTRY];
    member.node().propose(data.clone()).unwrap();
    // Everything the budget has left, taken by another owner.
    let stats = parent.stats();
    let left = stats.limit - stats.used;
    let taken = parent
        .reserve(BudgetKind::Pending, BudgetLane::Completion, left)
        .unwrap();
    assert!(matches!(
        member.node().drain(),
        Err(ConsensusError::Capacity)
    ));
    assert!(!member.node().failed());
    drop(taken);
    assert_eq!(committed(&member.settle()), vec![(2, data)]);
}

/// The shell refuses what is focal-log's alone, typed, and a fast group at open.
#[test]
fn what_is_focal_logs_alone_is_refused_on_the_shell() {
    let mut member = sole(no_needs);
    assert!(matches!(
        member.node().shared_wal(),
        Err(ConsensusError::Configuration(_))
    ));
    assert!(matches!(
        member.node().inject_fault_once(FaultPoint::AfterDataSync),
        Err(ConsensusError::Configuration(_))
    ));
    let mut fast = config(1);
    fast.group_id = [5; 16];
    fast.fast = true;
    let budget = MemoryBudget::new(64 * 1024 * 1024, 16 * 1024 * 1024).unwrap();
    let disk = DiskBudget::new(DiskBudgetConfig::unbounded()).unwrap();
    let log = member.log.as_ref().unwrap();
    assert!(matches!(
        DurableNode::open_on_shell(
            fast,
            member.dir.path(),
            &log.opener(),
            &budget,
            disk.clone(),
            no_needs
        ),
        Err(ConsensusError::Configuration(_))
    ));
    // A group whose largest entry a frame of the log cannot hold would fence at its first such
    // write: it is refused at open instead.
    let mut large = config(1);
    large.group_id = [6; 16];
    large.max_entry_bytes = 4 * ENTRY;
    assert!(matches!(
        DurableNode::open_on_shell(
            large,
            member.dir.path(),
            &log.opener(),
            &budget,
            disk,
            no_needs
        ),
        Err(ConsensusError::Configuration(
            "the node's log cannot hold the group's largest entry in one frame"
        ))
    ));
}

/// The owners' protocol over the shell: a drain that gives nothing while the write is out is
/// `None`, the owner waits for that write (`wait_persisted`), and the next drain gives what it
/// made durable; with nothing out there is nothing to wait for.
#[test]
fn an_owner_polls_waits_for_the_write_out_and_drains_after() {
    let mut member = sole(no_needs);
    elected(&mut member);
    assert!(!member.node().wait_persisted().unwrap(), "nothing out");
    member.node().propose(b"a".to_vec()).unwrap();
    let mut gave = Vec::new();
    for _ in 0..DRAINS {
        match member.node().try_drain().unwrap() {
            Some(events) => {
                gave.extend(events.committed.iter().map(|e| (e.index, e.data.clone())));
                if !member.node().has_ready() {
                    break;
                }
            }
            None => assert!(member.node().wait_persisted().unwrap()),
        }
    }
    assert_eq!(gave, vec![(2, b"a".to_vec())]);
    assert!(!member.node().persistence_pending());
}

/// An owner of many groups is told when a write of this one is answered: the signal it set makes
/// what the log calls as it answers, and a drain after the call gives what became durable.
#[test]
fn the_owners_signal_is_called_when_the_log_answers_a_write() {
    let mut member = sole(no_needs);
    elected(&mut member);
    let (tell, told) = std::sync::mpsc::sync_channel::<()>(64);
    member.node().notify_persisted(Some(Box::new(move || {
        let tell = tell.clone();
        Box::new(move || {
            let _ = tell.try_send(());
        })
    })));
    member.node().propose(b"a".to_vec()).unwrap();
    let mut gave = Vec::new();
    for _ in 0..DRAINS {
        match member.node().try_drain().unwrap() {
            Some(events) => {
                gave.extend(events.committed.iter().map(|e| (e.index, e.data.clone())));
                if gave.len() == 1 && !member.node().has_ready() {
                    break;
                }
            }
            // The wake is the fact waited on: buffered when it came first.
            None => told.recv().unwrap(),
        }
    }
    assert_eq!(gave, vec![(2, b"a".to_vec())]);
}
