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
use focal_consensus::{DurableNode, Message, NodeConfig, StateRole};
use std::{
    sync::mpsc::{self, Receiver, Sender},
    time::{Duration, Instant},
};

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
    let mut node = DurableNode::open(config(1, &[1]), dir.path()).unwrap();
    node.campaign().unwrap();
    drop(node.drain().unwrap());
    let wal = node.shared_wal().unwrap();
    let before = wal.stats().unwrap().group_commits;
    let mut latencies = Vec::with_capacity(ALONE);
    for round in 0..ALONE {
        let began = Instant::now();
        node.propose(vec![round as u8; PAYLOAD]).unwrap();
        let events = node.drain().unwrap();
        assert_eq!(events.committed.len(), 1);
        latencies.push(began.elapsed());
    }
    let flushes = wal.stats().unwrap().group_commits - before;
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
    let wal = node.shared_wal().unwrap();
    let before = wal.stats().unwrap().group_commits;
    'serve: loop {
        // Everything that waits is taken before the next drain: what came
        // while the last write was in flight is one batch.
        let mut first = Some(match inbox.recv() {
            Ok(input) => input,
            Err(_) => break,
        });
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
    wal.stats().unwrap().group_commits - before
}

fn three(staged: bool) {
    let dirs: Vec<_> = (0..3).map(|_| tempfile::tempdir().unwrap()).collect();
    let mut nodes: Vec<_> = dirs
        .iter()
        .enumerate()
        .map(|(at, dir)| DurableNode::open(config(at as u64 + 1, &[1, 2, 3]), dir.path()).unwrap())
        .collect();
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
        for (at, (node, inbox)) in nodes.drain(..).zip(inboxes).enumerate() {
            let peers: Vec<_> = senders
                .iter()
                .enumerate()
                .filter(|(peer, _)| *peer != at)
                .map(|(peer, sender)| (peer as u64 + 1, sender.clone()))
                .collect();
            let committed = (at == 0).then(|| committed.clone());
            threads.push(scope.spawn(move || member(node, inbox, peers, committed, staged)));
        }
        let leader = &senders[0];
        // The leader's first entry of its term: the group is ready after it.
        leader.send(Input::Propose(vec![0; PAYLOAD])).unwrap();
        let mut seen = 0;
        while seen < 1 {
            seen += commits.recv().unwrap();
        }
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
