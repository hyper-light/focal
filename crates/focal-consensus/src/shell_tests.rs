use super::*;
use focal_sim::disk::Disk;
use hyper_durable::{Entries, Output, RamStore, Replica, ReplicaError, Settings, Unbounded};
use hyper_raft::proto::{ConfChangeSingle, ConfChangeTransition, ConfChangeType};
use hyper_raft::{Config, Elections};
use std::path::Path;
use std::time::Duration;

const BOUND: usize = 1 << 16;
const MANAGED: [u8; 32] = [7; 32];

fn voters(ids: &[u64]) -> ConfState {
    ConfState {
        voters: ids.to_vec(),
        ..ConfState::default()
    }
}

fn dir() -> PathBuf {
    Path::new("/data/raft/groups/g").to_path_buf()
}

fn handover(disk: Disk, control: bool) -> HandOver<Disk> {
    HandOver::open(disk, dir(), control, BOUND, voters(&[1])).unwrap()
}

/// What the tests' entries need: a managed entry needs `MANAGED`.
fn managed(data: &[u8]) -> Option<[u8; 32]> {
    data.starts_with(b"managed").then_some(MANAGED)
}

/// Each entry needs the decoder its first byte names.
fn by_first_byte(data: &[u8]) -> Option<[u8; 32]> {
    data.first().map(|&byte| [byte; 32])
}

type Sole = Replica<FloorStore<RamStore>, HandOver<Disk>, Unbounded>;

/// What these tests state of their voter (`hyper_raft::Limits::derive`): a message of the pages
/// they send and a few entries more, focal's members (`MAX_MEMBERS`, which the learners the tests
/// add join), and queues of 1 MiB each, past anything a test here holds.
fn limits() -> hyper_raft::Limits {
    hyper_raft::Limits::derive(hyper_raft::Stated {
        message: 1 << 17,
        members: crate::MAX_MEMBERS,
        memory: 1 << 20,
        depth: 1,
    })
    .unwrap()
}

fn settings() -> Settings {
    Settings {
        core: Config {
            max_size_per_msg: 1 << 16,
            max_inflight_msgs: 8,
            max_committed_size_per_ready: 1 << 16,
            ..Config::new(1, limits())
        },
        elections: Elections::Ticks,
        quiet: Duration::from_millis(50),
    }
}

/// A sole voter on `store`, its machine on `disk`, elected.
fn sole(store: RamStore, disk: Disk) -> Sole {
    let store = FloorStore::new(store, managed, None, None);
    let mut r = Replica::open(&settings(), store, handover(disk, false), Unbounded).unwrap();
    r.campaign().unwrap();
    pump(&mut r);
    assert!(r.is_leader());
    r
}

/// Drives `r` until nothing is out and nothing more is due, on a clock that only moves forward.
fn pump(r: &mut Sole) -> Option<Fault> {
    let mut out = Output::default();
    for now in 1..=10_000 {
        out.clear();
        let driven = r.drive(now, Waker::noop(), &mut out).unwrap();
        if driven.stalled.is_some() {
            return driven.stalled;
        }
        if !driven.more && driven.out == 0 {
            return None;
        }
    }
    panic!("the replica never rested");
}

fn learner(id: u64, context: &[u8]) -> ConfChangeV2 {
    ConfChangeV2 {
        transition: ConfChangeTransition::Auto,
        changes: vec![ConfChangeSingle {
            change_type: ConfChangeType::AddLearnerNode,
            node_id: id,
        }],
        context: context.to_vec(),
    }
}

fn drained(r: &mut Sole) -> NodeEvents {
    let mut events = NodeEvents::default();
    r.machine_mut().take(&mut events).unwrap();
    events
}

/// Contract (a): what a group commits reaches the owner in order and once, as over focal-log: a
/// normal entry with data as a committed entry, a change with the context its entry stated and
/// the configurations before and after it, a leader's empty entry only in the applied index.
#[test]
fn entries_and_changes_are_handed_over_in_order_and_once() {
    let mut r = sole(RamStore::new(), Disk::new(1 << 20));
    let elected = drained(&mut r);
    assert!(elected.committed.is_empty(), "the leader's empty entry");
    assert_eq!(elected.applied_index, 1);
    for data in [b"a", b"b", b"c"] {
        r.propose(Vec::new(), data.to_vec()).unwrap();
    }
    r.change(Vec::new(), &learner(2, b"where 2 listens"))
        .unwrap();
    pump(&mut r);
    let events = drained(&mut r);
    let committed: Vec<_> = events
        .committed
        .iter()
        .map(|e| (e.index, e.data.clone()))
        .collect();
    assert_eq!(
        committed,
        vec![(2, b"a".to_vec()), (3, b"b".to_vec()), (4, b"c".to_vec())]
    );
    assert_eq!(events.membership.len(), 1);
    let change = &events.membership[0];
    assert_eq!(
        (change.index, change.context.as_slice()),
        (5, &b"where 2 listens"[..])
    );
    assert_eq!(
        change.before,
        MembershipConfiguration::from_conf(&voters(&[1]))
    );
    assert_eq!(
        change.after,
        MembershipConfiguration::from_conf(&ConfState {
            voters: vec![1],
            learners: vec![2],
            ..ConfState::default()
        })
    );
    assert_eq!(events.applied_index, 5);
    let again = drained(&mut r);
    assert!(again.committed.is_empty() && again.membership.is_empty());
    assert_eq!(again.applied_index, 5, "handed over once");
}

/// Contract (e): a checkpoint is durable when it returns, a restart opens at it with the
/// configuration it was taken under, and the image a leader serves is that one, verified from
/// its file, with that configuration: never one applied since.
#[test]
fn a_restart_opens_at_the_checkpoint_and_a_leader_serves_it_as_written() {
    let mut r = sole(RamStore::new(), Disk::new(1 << 20));
    for data in [b"a", b"b"] {
        r.propose(Vec::new(), data.to_vec()).unwrap();
    }
    pump(&mut r);
    drained(&mut r);
    let at = r.machine().applied();
    let configuration = r.configuration().clone();
    r.machine_mut()
        .checkpoint(at, &configuration, b"state at three")
        .unwrap();
    r.change(Vec::new(), &learner(2, b"")).unwrap();
    pump(&mut r);
    assert_eq!(r.configuration().learners, vec![2]);

    let mut image = Vec::new();
    let (point, served) = r.machine_mut().image(&mut image).unwrap();
    assert_eq!((point, image.as_slice()), (at, &b"state at three"[..]));
    assert_eq!(
        served,
        voters(&[1]),
        "the configuration at the image's point"
    );

    let mut disk = r.into_machine().medium;
    disk.crash();
    let reopened = handover(disk, false);
    assert_eq!(reopened.durable(), at);
    assert_eq!(reopened.configuration(), &voters(&[1]));
    assert_eq!(reopened.applied(), at);
}

/// A restart replays what the log holds past the checkpoint, and hands the owner only that: the
/// owner restored the checkpoint, and nothing it covers comes again.
#[test]
fn a_restart_hands_over_only_what_its_checkpoint_does_not_cover() {
    let mut r = sole(RamStore::new(), Disk::new(1 << 20));
    for data in [b"a", b"b", b"c"] {
        r.propose(Vec::new(), data.to_vec()).unwrap();
    }
    pump(&mut r);
    drained(&mut r);
    let at = Point {
        index: 3,
        term: r.term(),
    };
    r.machine_mut()
        .checkpoint(at, &voters(&[1]), b"through b")
        .unwrap();
    let store = r.log_mut().inner_mut().clone();
    let disk = r.into_machine().medium;

    let mut r = sole(store, disk);
    let events = drained(&mut r);
    let committed: Vec<_> = events.committed.iter().map(|e| e.index).collect();
    assert_eq!(committed, vec![4], "c, and nothing before it");
}

/// An install is durable before it returns (O3), replaces what waited for the owner, and hands
/// the owner the image with its configuration.
#[test]
fn an_install_is_durable_when_it_returns_and_replaces_what_waited() {
    let mut machine = handover(Disk::new(1 << 20), false);
    let entry = Entry {
        index: 1,
        term: 1,
        data: b"behind the image".to_vec(),
        ..Entry::default()
    };
    machine
        .apply(&EntryRef::of(&entry), &mut Vec::new())
        .unwrap();
    let configuration = voters(&[1, 2]);
    let at = Point { index: 10, term: 2 };
    machine.install(b"image", at, &configuration).unwrap();
    machine.medium.crash();
    let (point, bytes) = group_files::read_image(&machine.medium, &dir(), BOUND)
        .unwrap()
        .unwrap();
    assert_eq!((point.index, point.term), (10, 2));
    assert_eq!(point.configuration, configuration);
    assert_eq!(bytes, b"image");

    let mut events = NodeEvents::default();
    machine.take(&mut events).unwrap();
    assert!(events.committed.is_empty(), "what the image covers");
    let snapshot = events.snapshot.unwrap();
    assert_eq!((snapshot.index, snapshot.term), (10, 2));
    assert_eq!(snapshot.data, b"image");
    assert_eq!(
        snapshot.configuration,
        MembershipConfiguration::from_conf(&configuration)
    );
    assert_eq!(events.applied_index, 10);
    assert_eq!(machine.durable(), at);
}

/// Contract (d): a control group's every entry is acted on at its members' next start.
#[test]
fn a_control_group_acts_at_start_on_every_entry() {
    let entry = Entry {
        index: 1,
        term: 1,
        data: b"x".to_vec(),
        ..Entry::default()
    };
    let entry = EntryRef::of(&entry);
    assert!(handover(Disk::new(1 << 20), true).acts_at_start(&entry));
    assert!(!handover(Disk::new(1 << 20), false).acts_at_start(&entry));
}

/// Contract (f): a write carrying an entry that needs a decoder the records do not state durable
/// waits whole, the replica refusing what it is given, until the owner states it and releases
/// the hold; then the entry is applied and handed over.
#[test]
fn a_write_needing_a_floor_not_durable_waits_whole_until_released() {
    let mut r = sole(RamStore::new(), Disk::new(1 << 20));
    r.propose(Vec::new(), b"plain".to_vec()).unwrap();
    assert_eq!(pump(&mut r), None);
    r.propose(Vec::new(), b"managed entry".to_vec()).unwrap();
    assert_eq!(pump(&mut r), Some(Fault::Held));
    assert_eq!(r.held(), Some(&MANAGED));
    assert_eq!(
        r.propose(Vec::new(), b"more".to_vec()),
        Err(ReplicaError::Stalled)
    );
    let before = drained(&mut r);
    assert!(before.committed.iter().all(|e| e.data != b"managed entry"));

    r.release(&MANAGED);
    assert_eq!(pump(&mut r), None);
    let after = drained(&mut r);
    assert!(after.committed.iter().any(|e| e.data == b"managed entry"));
    assert_eq!(r.held(), None);
}

fn write_of(entries: &[Entry]) -> Write<'_> {
    Write {
        entries: entries.first().map(|first| Entries {
            first: first.index,
            entries,
        }),
        ..Write::default()
    }
}

fn entry(index: u64, data: &[u8]) -> Entry {
    Entry {
        index,
        term: 1,
        data: data.to_vec(),
        ..Entry::default()
    }
}

/// A held write's followers are refused behind it, as hyper-log refuses them; the records state
/// two decoders at most, so a third released is never taken for durable and its write stays
/// held.
#[test]
fn writes_behind_a_held_one_are_refused_and_a_third_decoder_is_never_durable() {
    let mut store = FloorStore::new(RamStore::new(), by_first_byte, Some([b'a'; 32]), None);
    let waker = Waker::noop();
    let first = [entry(1, b"a: needs the floor")];
    assert_eq!(store.submit(&write_of(&first), waker), Ok(()));
    assert_eq!(store.poll(), Some(Ok(())), "the store's depth is one");
    let second = [entry(2, b"b: needs the successor")];
    assert_eq!(store.submit(&write_of(&second), waker), Err(Fault::Held));
    assert_eq!(store.held(), Some(&[b'b'; 32]));
    let behind = [entry(3, b"a")];
    assert_eq!(store.submit(&write_of(&behind), waker), Err(Fault::Behind));

    store.release(&[b'b'; 32]);
    assert_eq!(store.held(), None);
    assert_eq!(store.submit(&write_of(&second), waker), Ok(()));

    let third = [entry(3, b"c: no record states it")];
    assert_eq!(store.submit(&write_of(&third), waker), Err(Fault::Held));
    store.release(&[b'c'; 32]);
    assert_eq!(store.held(), Some(&[b'c'; 32]), "never taken for durable");
    assert_eq!(store.submit(&write_of(&third), waker), Err(Fault::Behind));
}
