// Dependency-free bench: a plain `harness = false` binary, like focal-log's.
// A measurement tool, not a pass/fail test.
#![allow(
    clippy::unwrap_used,
    clippy::disallowed_macros,
    clippy::cast_precision_loss,
    clippy::arithmetic_side_effects,
    clippy::indexing_slicing
)]
//! What a commit waits for (the audit's F17). A member that alone decides
//! proposes entries one at a time; then three members, a thread and a log
//! each on this host's disk, commit entries one at a time and then as fast
//! as the leader takes them. Reported: how long a proposal takes to be
//! given back committed at its leader, and the flushes each member's log
//! made for it. Every flush is a real disk sync, so the latencies are the
//! disk's: a same-host signal.
//!
//! `FOCAL_BENCH_OWNER=settle` drives each member with the drain that waits
//! and gives everything at once; the default drives it as its owners do: a
//! leader's messages are sent while its write is in flight.
//!
//! `FOCAL_BENCH_BACKEND=shell` runs every member over hyper-durable's shell, its
//! log a hyper-log of its own on the same disk (27 §15.10: the shell replaces
//! focal-log only where it is at least as fast); the default is focal-log. On
//! the shell each member's frames, updates and flushes per entry of the
//! one-at-a-time phase are reported from its log's statistics, and
//! `FOCAL_BENCH_WAITS=never` makes the log's writer never wait between frames.
use focal_consensus::{DurableNode, Message, NodeConfig, StateRole};
use focal_memory::{DiskBudget, DiskBudgetConfig, MemoryBudget};
use hyper_block::buf::Alignment;
use hyper_block::file::{CachingRequest, DeviceFile};
use hyper_log::{Config as LogConfig, Log, Waits};
use std::{
    path::Path,
    sync::mpsc::{self, Receiver, Sender},
    time::{Duration, Instant},
};

/// A member's storage: focal-log's WAL in its directory, or a hyper-log of its own there.
enum Store {
    Wal,
    Shell(Log<DeviceFile>),
}

impl Store {
    /// Flushes the member's log has made so far.
    fn flushes(&self, node: &DurableNode) -> u64 {
        match self {
            Store::Wal => node.shared_wal().unwrap().stats().unwrap().group_commits,
            Store::Shell(log) => log.stats(None).unwrap().flushes,
        }
    }
}

fn shell() -> bool {
    std::env::var("FOCAL_BENCH_BACKEND").as_deref() == Ok("shell")
}

/// The member's log as the node runs it: it waits between frames as measured.
fn log_config() -> LogConfig {
    LogConfig {
        segment_bytes: 64 * 1024 * 1024,
        max_segments: 16,
        max_groups: 4,
        group_entries: 1 << 16,
        group_bytes: 256 << 20,
        group_cache: 8 << 20,
        queue_submissions: 64,
        waits: if std::env::var("FOCAL_BENCH_WAITS").as_deref() == Ok("never") {
            Waits::Never
        } else {
            Waits::Measured
        },
    }
}

fn no_needs(_: &[u8]) -> Option<[u8; 32]> {
    None
}

/// Opens member `config` in `dir` on the backend the bench runs.
fn open(config: NodeConfig, dir: &Path) -> (Store, DurableNode) {
    if !shell() {
        return (Store::Wal, DurableNode::open(config, dir).unwrap());
    }
    let align = Alignment::new(4096).unwrap();
    let file = DeviceFile::open(
        &dir.join("raft.log"),
        true,
        CachingRequest::PreferDirect,
        align,
    )
    .unwrap();
    let log = Log::create(file, log_config(), 0x0062_656e_6368).unwrap();
    let budget = MemoryBudget::new(512 * 1024 * 1024, 128 * 1024 * 1024).unwrap();
    let disk = DiskBudget::new(DiskBudgetConfig::unbounded()).unwrap();
    let node = DurableNode::open_on_shell(config, dir, &log, &budget, disk, no_needs).unwrap();
    (Store::Shell(log), node)
}

const PAYLOAD: usize = 256;
const ALONE: usize = 200;
const ONE_AT_A_TIME: usize = 200;
const PIPELINED: usize = 4000;

enum Input {
    Message(Box<Message>),
    Propose(Vec<u8>),
    Stop,
}

fn config(node: u64, voters: &[u64]) -> NodeConfig {
    let mut config = NodeConfig::single(node, [7; 16], [9; 16]);
    config.voters = voters.to_vec();
    config
}
fn percentile(sorted: &[Duration], per_mille: usize) -> Duration {
    sorted[(sorted.len() * per_mille / 1000).min(sorted.len() - 1)]
}
fn report(name: &str, mut latencies: Vec<Duration>) {
    latencies.sort();
    println!(
        "{name}: median {:.3} ms, p99 {:.3} ms, least {:.3} ms",
        percentile(&latencies, 500).as_secs_f64() * 1e3,
        percentile(&latencies, 990).as_secs_f64() * 1e3,
        latencies[0].as_secs_f64() * 1e3,
    );
}

fn alone() {
    let dir = tempfile::tempdir().unwrap();
    let (store, mut node) = open(config(1, &[1]), dir.path());
    node.campaign().unwrap();
    while !node.has_committed_current_term() {
        drop(node.drain().unwrap());
    }
    let before = store.flushes(&node);
    let mut latencies = Vec::with_capacity(ALONE);
    for round in 0..ALONE {
        let began = Instant::now();
        node.propose(vec![round as u8; PAYLOAD]).unwrap();
        let mut given = 0;
        while given < 1 {
            given += node.drain().unwrap().committed.len();
        }
        latencies.push(began.elapsed());
    }
    let flushes = store.flushes(&node) - before;
    report("one voter, an entry at a time", latencies);
    println!(
        "one voter: {:.2} flushes a commit",
        flushes as f64 / ALONE as f64
    );
}

/// One member on its thread: it takes what comes, drains until nothing is
/// left to persist, sends what its drain gives, and tells the client of
/// every entry it gives back committed.
fn member(
    mut node: DurableNode,
    store: &Store,
    inbox: Receiver<Input>,
    peers: Vec<(u64, Sender<Input>)>,
    committed: Option<Sender<usize>>,
    staged: bool,
) -> u64 {
    let send = |messages: Vec<Message>| {
        for message in messages {
            if let Some((_, peer)) = peers.iter().find(|(id, _)| *id == message.to) {
                let _ = peer.send(Input::Message(Box::new(message)));
            }
        }
    };
    let before = store.flushes(&node);
    'serve: loop {
        // Everything that waits is taken before the next drain: what came
        // while the last write was in flight is one batch. A member with
        // work due drives again without waiting for input: over the shell a
        // drain may give what is ready while later writes are still out.
        let mut first = if node.has_ready() {
            None
        } else {
            Some(match inbox.recv() {
                Ok(input) => input,
                Err(_) => break,
            })
        };
        while let Some(input) = first.take().or_else(|| inbox.try_recv().ok()) {
            match input {
                Input::Message(message) => {
                    let _ = node.step(*message);
                }
                Input::Propose(data) => node.propose(data).unwrap(),
                Input::Stop => break 'serve,
            }
        }
        let events = if staged {
            loop {
                match node.try_drain().unwrap() {
                    Some(events) => break events,
                    None => {
                        if let Some(early) = node.sendable().unwrap() {
                            send(early.messages);
                        }
                        if !node.wait_persisted().unwrap() {
                            break node.drain().unwrap();
                        }
                    }
                }
            }
        } else {
            node.drain().unwrap()
        };
        if let Some(committed) = &committed
            && !events.committed.is_empty()
        {
            let _ = committed.send(events.committed.len());
        }
        send(events.messages);
    }
    store.flushes(&node) - before
}

fn three(staged: bool) {
    let dirs: Vec<_> = (0..3).map(|_| tempfile::tempdir().unwrap()).collect();
    let (stores, mut nodes): (Vec<_>, Vec<_>) = dirs
        .iter()
        .enumerate()
        .map(|(at, dir)| open(config(at as u64 + 1, &[1, 2, 3]), dir.path()))
        .unzip();
    // The election, before the members take their threads: messages are
    // carried by hand until the first member has committed in its term.
    nodes[0].campaign().unwrap();
    while !(nodes[0].status().role == StateRole::Leader && nodes[0].has_committed_current_term()) {
        let mut messages = Vec::new();
        for node in &mut nodes {
            messages.extend(node.drain().unwrap().messages);
        }
        for message in messages {
            let to = message.to as usize - 1;
            let _ = nodes[to].step(message);
        }
    }
    for node in &mut nodes {
        drop(node.drain().unwrap());
    }
    let (senders, inboxes): (Vec<_>, Vec<_>) = (0..3).map(|_| mpsc::channel::<Input>()).unzip();
    let (committed, commits) = mpsc::channel::<usize>();
    let flushes = std::thread::scope(|scope| {
        let mut threads = Vec::new();
        for (at, ((node, inbox), store)) in nodes.drain(..).zip(inboxes).zip(&stores).enumerate() {
            let peers: Vec<_> = senders
                .iter()
                .enumerate()
                .filter(|(peer, _)| *peer != at)
                .map(|(peer, sender)| (peer as u64 + 1, sender.clone()))
                .collect();
            let committed = (at == 0).then(|| committed.clone());
            threads.push(scope.spawn(move || member(node, store, inbox, peers, committed, staged)));
        }
        let leader = &senders[0];
        // The leader's first entry of its term: the group is ready after it.
        leader.send(Input::Propose(vec![0; PAYLOAD])).unwrap();
        let mut seen = 0;
        while seen < 1 {
            seen += commits.recv().unwrap();
        }
        let counts = |stores: &[Store]| -> Vec<(u64, u64, u64)> {
            stores
                .iter()
                .map(|store| match store {
                    Store::Shell(log) => {
                        let s = log.stats(None).unwrap();
                        (s.frames, s.updates, s.flushes)
                    }
                    Store::Wal => (0, 0, 0),
                })
                .collect()
        };
        let before_counts = counts(&stores);
        let mut latencies = Vec::with_capacity(ONE_AT_A_TIME);
        for round in 0..ONE_AT_A_TIME {
            let began = Instant::now();
            leader
                .send(Input::Propose(vec![round as u8; PAYLOAD]))
                .unwrap();
            let mut seen = 0;
            while seen < 1 {
                seen += commits.recv().unwrap();
            }
            latencies.push(began.elapsed());
        }
        report("three voters, an entry at a time", latencies);
        let after_counts = counts(&stores);
        for (at, (b, a)) in before_counts.iter().zip(&after_counts).enumerate() {
            if matches!(stores[at], Store::Wal) {
                continue;
            }
            println!(
                "member {}: per entry frames {:.2} updates {:.2} flushes {:.2}",
                at + 1,
                (a.0 - b.0) as f64 / ONE_AT_A_TIME as f64,
                (a.1 - b.1) as f64 / ONE_AT_A_TIME as f64,
                (a.2 - b.2) as f64 / ONE_AT_A_TIME as f64
            );
        }
        let began = Instant::now();
        for round in 0..PIPELINED {
            leader
                .send(Input::Propose(vec![round as u8; PAYLOAD]))
                .unwrap();
        }
        let mut seen = 0;
        while seen < PIPELINED {
            seen += commits.recv().unwrap();
        }
        let took = began.elapsed();
        println!(
            "three voters, {PIPELINED} entries as fast as the leader takes them: {:.0} entries/s",
            PIPELINED as f64 / took.as_secs_f64()
        );
        for sender in &senders {
            let _ = sender.send(Input::Stop);
        }
        threads
            .into_iter()
            .map(|thread| thread.join().unwrap())
            .collect::<Vec<_>>()
    });
    let entries = 1 + ONE_AT_A_TIME + PIPELINED;
    println!("three voters: flushes by member {flushes:?} for {entries} entries");
}

fn main() {
    let staged = std::env::var("FOCAL_BENCH_OWNER").as_deref() != Ok("settle");
    println!(
        "backend: {}",
        if shell() {
            "hyper-durable's shell over hyper-log"
        } else {
            "focal-log"
        }
    );
    println!(
        "owner: {}",
        if staged {
            "sends a leader's messages while its write is in flight"
        } else {
            "waits for every write before it sends"
        }
    );
    alone();
    three(staged);
}
