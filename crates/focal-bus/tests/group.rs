//! Three bus nodes replicating one part, and a client, over real UDP sockets on loopback, their four
//! endpoints driven by one task on one hyper-rt shard, each node's log a sealed hyper-log file in a
//! directory of its own (doc 34, step 3b.3).
//!
//! The nodes elect a leader by their liveness stream; a client publishes to whichever node it knows,
//! follows each redirect to the leader the node names, and every batch it publishes is
//! acknowledged, persisted, at the offsets the part gave it, in order. A subscriber of the leader is
//! delivered every batch, in order. The leader is killed: the two others elect one of themselves,
//! the client follows, sends again what was not acknowledged, and is acknowledged once for each
//! batch: every batch acknowledged before the kill is in the new leader's part, at the offsets it
//! was acknowledged at, and a batch sent again is recognised and not appended twice.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros,
    clippy::cast_possible_truncation,
    clippy::too_many_lines
)]

use std::collections::BTreeMap;
use std::future::{Future, poll_fn};
use std::net::SocketAddr;
use std::path::Path;
use std::pin::pin;
use std::task::{Poll, Waker};
use std::time::{Duration, Instant};

use focal_bus::batch::{BatchBuilder, Producing};
use focal_bus::budget::BusBudget;
use focal_bus::client::{Client, Delivery};
use focal_bus::directory::BusDirectory;
use focal_bus::limits::IO;
use focal_bus::member::{Member, MemberSettings};
use focal_bus::node::{Node, NodeConfig};
use focal_bus::wire::{self, Bus, Durability, Role};
use focal_memory::MemoryBudget;
use focal_timing::ProgressDeadline;
use hyper_rt::runtime::{LocalRuntime, RuntimeConfig};
use hyper_rt_transport::Driver;
use hyper_transport::tls::{CertificateDer, Credentials, PrivateKeyDer, PrivatePkcs8KeyDer, Tls};
use hyper_transport::{Config, Endpoint, Event, Refusal};

type Drv = Driver<Bus, BusBudget, BusDirectory>;

const NODES: [u64; 3] = [1, 2, 3];
const CLIENT: u64 = 10;
const STREAM: u64 = 5;
const PRODUCER: u64 = 77;
/// A wait fails only when what it waits on did not move for this long: a wedge, not slowness.
const FROZEN: Duration = Duration::from_secs(30);
/// Each node's memory: its part's segments, its transport's buffers, its tables, its replica.
const NODE_MEMORY: u64 = 96 << 20;

struct Pki {
    issuer: rcgen::Issuer<'static, rcgen::KeyPair>,
    root: Vec<u8>,
}

impl Pki {
    fn new() -> Self {
        let mut params = rcgen::CertificateParams::new(Vec::<String>::new()).unwrap();
        params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
        let key = rcgen::KeyPair::generate().unwrap();
        let root = params.self_signed(&key).unwrap().der().to_vec();
        Self {
            issuer: rcgen::Issuer::new(params, key),
            root,
        }
    }

    fn issue(&self, name: &str) -> (Vec<u8>, Vec<u8>) {
        let params = rcgen::CertificateParams::new(vec![name.to_owned()]).unwrap();
        let key = rcgen::KeyPair::generate().unwrap();
        let certificate = params.signed_by(&key, &self.issuer).unwrap();
        (certificate.der().to_vec(), key.serialize_der())
    }

    fn credentials(&self, own: &(Vec<u8>, Vec<u8>)) -> Credentials {
        Credentials {
            chain: vec![CertificateDer::from(own.0.clone())],
            key: PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(own.1.clone())),
            roots: vec![CertificateDer::from(self.root.clone())],
        }
    }
}

fn node_name(id: u64) -> String {
    format!("focal-bus-node-{id}")
}

fn runtime() -> RuntimeConfig {
    RuntimeConfig {
        shards: 1,
        tasks_per_shard: 64,
        timers_per_shard: 64,
        interests_per_shard: 64,
        ring_entries: 64,
        step_budget_ns: 1_000_000,
        timer_tick_ns: 100_000,
        batch: 64,
        pin: false,
        cores: Vec::new(),
        page_bytes: 4096,
        spin_ns: 0,
        wake_tracking: None,
        latency_objective_ns: None,
    }
}

/// The identities: each node's, the client's.
struct Identities {
    pki: Pki,
    nodes: Vec<(Vec<u8>, Vec<u8>)>,
    client: (Vec<u8>, Vec<u8>),
}

impl Identities {
    fn new() -> Self {
        let pki = Pki::new();
        let nodes = NODES.iter().map(|id| pki.issue(&node_name(*id))).collect();
        let client = pki.issue("focal-bus-client-1");
        Self { pki, nodes, client }
    }

    /// The endpoint of node `id`, knowing the other nodes and the client.
    fn node(&self, id: u64, budget: &MemoryBudget) -> Endpoint<Bus, BusBudget, BusDirectory> {
        let mut directory = BusDirectory::new(8);
        for (at, peer) in NODES.iter().enumerate() {
            if *peer != id {
                directory
                    .enroll(&self.nodes[at].0, *peer, Role::Node, &node_name(*peer))
                    .unwrap();
            }
        }
        directory
            .enroll(&self.client.0, CLIENT, Role::Client, "focal-bus-client-1")
            .unwrap();
        let own = &self.nodes[NODES.iter().position(|n| *n == id).unwrap()];
        Endpoint::new(
            Config {
                tls: Tls::mutual(&self.pki.credentials(own), true).unwrap(),
                role: Role::Node,
                limits: focal_bus::limits::node(1, NODES.len() - 1).unwrap(),
                congestion: Default::default(),
            },
            BusBudget::new(budget.clone()),
            directory,
            Instant::now(),
        )
        .unwrap()
    }

    /// The client's endpoint, knowing every node.
    fn client(&self, budget: &MemoryBudget) -> Endpoint<Bus, BusBudget, BusDirectory> {
        let mut directory = BusDirectory::new(8);
        for (at, peer) in NODES.iter().enumerate() {
            directory
                .enroll(&self.nodes[at].0, *peer, Role::Node, &node_name(*peer))
                .unwrap();
        }
        Endpoint::new(
            Config {
                tls: Tls::mutual(&self.pki.credentials(&self.client), false).unwrap(),
                role: Role::Client,
                limits: focal_bus::limits::client_of(NODES.len()).unwrap(),
                congestion: Default::default(),
            },
            BusBudget::new(budget.clone()),
            directory,
            Instant::now(),
        )
        .unwrap()
    }
}

fn batch(builder: &mut BatchBuilder, sequence: u32) -> Vec<u8> {
    sized_batch(builder, sequence, 0)
}

/// A batch of one record: its sequence's eight bytes, then filler to `payload` bytes where that is
/// more.
fn sized_batch(builder: &mut BatchBuilder, sequence: u32, payload: usize) -> Vec<u8> {
    builder.begin(&Producing {
        stream: STREAM,
        producer: PRODUCER,
        sequence,
        flags: 0,
        timestamp: 1_792_022_400_000_000,
    });
    let mut record = u64::from(sequence).to_le_bytes().to_vec();
    record.resize(payload.max(record.len()), 0x5a);
    builder.push(1_792_022_400_000_000, &record).unwrap();
    builder.finish().unwrap().to_vec()
}

/// One node: its endpoint's driver and the member it serves.
struct Running {
    driver: Drv,
    member: Member,
}

/// What the client has: the batches it published and not yet had acknowledged, the offsets each
/// acknowledged batch took, and what a subscription was delivered.
#[derive(Default)]
struct Seen {
    acked: BTreeMap<u32, (u64, u16)>,
    again: u64,
    delivered: Vec<(u64, u32)>,
    subscribed: bool,
    redirects: u64,
    /// Answers that the part waits for an earlier sequence of the session (KIP-98's
    /// OUT_OF_ORDER_SEQUENCE_NUMBER): every batch not yet acknowledged goes again at the next step.
    out_of_order: u64,
}

struct World {
    nodes: Vec<Option<Running>>,
    addresses: Vec<SocketAddr>,
    client: Drv,
    /// The node the client believes leads, and the term it believes so in.
    leader: u64,
    term: u64,
    /// Every batch published and not yet acknowledged, by sequence.
    unacked: BTreeMap<u32, Vec<u8>>,
    seen: Seen,
    waker: Waker,
    builder: BatchBuilder,
    sent: u32,
    events: u64,
    subscribe_at: Option<u64>,
    /// The class every acknowledgement states.
    class: Durability,
    /// Each batch's record's bytes (`sized_batch`).
    payload: usize,
}

impl World {
    fn running(&mut self) -> impl Iterator<Item = &mut Running> {
        self.nodes.iter_mut().flatten()
    }

    fn leader_now(&self) -> Option<u64> {
        self.nodes
            .iter()
            .flatten()
            .find(|running| running.member.is_leader())
            .map(|running| running.member.id())
    }

    fn terms(&self) -> u64 {
        self.nodes
            .iter()
            .flatten()
            .map(|running| running.member.term())
            .sum()
    }

    /// What a stalled run leaves to read: the client's view and each node's role, term and
    /// statistics, its log's, window's, parked and owed among them.
    fn diagnose(&self) -> String {
        let mut out = format!(
            "client: leader {} term {}, {} acknowledged, {} unacknowledged, {} sent",
            self.leader,
            self.term,
            self.seen.acked.len(),
            self.unacked.len(),
            self.sent
        );
        for running in self.nodes.iter().flatten() {
            let member = &running.member;
            out.push_str(&format!(
                "\nnode {}: leads {}, term {}: {:?}",
                member.id(),
                member.is_leader(),
                member.term(),
                member.stats()
            ));
        }
        out
    }

    /// Handles every event that is ready, then gives each node its turn; waits at most until the
    /// soonest of the nodes' deadlines, or a tick.
    async fn step(&mut self) {
        let soonest = self
            .nodes
            .iter()
            .flatten()
            .filter_map(|running| running.member.deadline())
            .min();
        let wait_ns = soonest
            .map(|at| at.saturating_duration_since(Instant::now()))
            .unwrap_or(Duration::from_millis(1))
            .min(Duration::from_millis(1));
        let mut tick = pin!(hyper_rt::futures::sleep(
            u64::try_from(wait_ns.as_nanos()).unwrap().max(1)
        ));
        let began = Instant::now();
        let nodes = &mut self.nodes;
        let client = &mut self.client;
        let found = poll_fn(|context| {
            let mut found: Vec<(Option<usize>, Event<Bus>)> = Vec::new();
            for (at, running) in nodes.iter_mut().enumerate() {
                if let Some(running) = running
                    && let Poll::Ready(event) = running.driver.poll_event(context)
                {
                    found.push((Some(at), event.unwrap()));
                }
            }
            if let Poll::Ready(event) = client.poll_event(context) {
                found.push((None, event.unwrap()));
            }
            if !found.is_empty() || tick.as_mut().poll(context).is_ready() {
                return Poll::Ready(found);
            }
            Poll::Pending
        })
        .await;
        let now = Instant::now();
        // Each node is told how late it came to its stream's wake, before anything the wait
        // brought is fed (`Member::waited`).
        for running in self.nodes.iter_mut().flatten() {
            running.member.waited(began, now);
        }
        for (whose, event) in found {
            self.events += 1;
            match whose {
                Some(at) => self.node_event(at, event, now),
                None => self.client_event(event),
            }
        }
        for at in 0..self.nodes.len() {
            if let Some(running) = self.nodes[at].as_mut() {
                while let Some(event) = running.driver.endpoint().poll_event() {
                    running
                        .member
                        .on_event(running.driver.endpoint(), event, now);
                    self.events += 1;
                }
                let waker = self.waker.clone();
                running
                    .member
                    .turn(running.driver.endpoint(), &waker, now, &mut |_| {});
            }
        }
        while let Some(event) = self.client.endpoint().poll_event() {
            self.client_event(event);
        }
    }

    fn node_event(&mut self, at: usize, event: Event<Bus>, now: Instant) {
        if let Some(running) = self.nodes[at].as_mut() {
            running
                .member
                .on_event(running.driver.endpoint(), event, now);
        }
    }

    fn client_event(&mut self, event: Event<Bus>) {
        let mut client = Client::new(self.leader);
        let mut redirected = Vec::new();
        let seen = &mut self.seen;
        let unacked = &mut self.unacked;
        let class = self.class;
        client.on_event(self.client.endpoint(), event, |delivery| match delivery {
            Delivery::Acked(ack) if ack.class == wire::Ack::OUT_OF_ORDER => seen.out_of_order += 1,
            Delivery::Acked(ack) => {
                assert_eq!(ack.stream, STREAM);
                assert_eq!(ack.producer, PRODUCER);
                assert_eq!(Durability::of(ack.class), Some(class), "{ack:?}");
                unacked.remove(&ack.sequence);
                if ack.count == 0 || seen.acked.contains_key(&ack.sequence) {
                    seen.again += 1;
                }
                if ack.count > 0 {
                    let first = seen.acked.insert(ack.sequence, (ack.base, ack.count));
                    if let Some(first) = first {
                        assert_eq!(first, (ack.base, ack.count), "acknowledged at one offset");
                    }
                }
            }
            Delivery::Redirected(redirect) => {
                seen.redirects += 1;
                redirected.push(redirect);
            }
            Delivery::Batch(batch) => seen.delivered.push((batch.base_offset(), batch.sequence())),
            Delivery::Subscribed { .. } => seen.subscribed = true,
            Delivery::SubscribeRedirected { .. }
            | Delivery::Gap(_)
            | Delivery::Refused(_)
            | Delivery::Unsubscribed { .. }
            | Delivery::Lost(_) => {}
        });
        for redirect in redirected {
            self.follow(redirect.leader);
        }
    }

    /// Takes a leader a node named, where its term is past what the client knew (KIP-951's rule).
    fn follow(&mut self, leader: wire::Leader) {
        if let Some(node) = leader.node
            && leader.term > self.term
        {
            self.leader = node;
            self.term = leader.term;
            let at = NODES.iter().position(|n| *n == node).unwrap();
            let _ = self
                .client
                .endpoint()
                .connect(Instant::now(), node, self.addresses[at]);
        }
    }

    /// Sends the leader the client knows every batch not yet acknowledged, and new ones up to
    /// `until`.
    fn publish(&mut self, until: u32) {
        while self.sent < until {
            let bytes = sized_batch(&mut self.builder, self.sent, self.payload);
            self.unacked.insert(self.sent, bytes);
            self.sent += 1;
        }
        let client = Client::new(self.leader);
        for bytes in self.unacked.values() {
            match client.publish(self.client.endpoint(), bytes) {
                Ok(()) | Err(Refusal::NotConnected) => {}
                Err(Refusal::LaneFull) => return,
                Err(other) => panic!("publish refused: {other:?}"),
            }
        }
    }
}

fn open_node(
    ids: &Identities,
    id: u64,
    dir: &Path,
    addresses: &[SocketAddr],
    driver: Drv,
    fates: &[(u64, u64)],
    memory: u64,
) -> Running {
    let budget = focal_bus::limits::group_budget(usize::try_from(memory).unwrap()).unwrap();
    let node = Node::new(
        NodeConfig {
            segment_bytes: NodeConfig::SEGMENT_BYTES,
            streams: 64,
            subscriptions: 4,
            producers: 64,
        },
        budget.clone(),
    )
    .unwrap();
    let peers = NODES
        .iter()
        .zip(addresses)
        .filter(|(peer, _)| **peer != id)
        .map(|(peer, address)| (*peer, *address))
        .collect();
    let data = dir.join(format!("node-{id}"));
    std::fs::create_dir_all(&data).unwrap();
    let keys = dir.join("keys");
    std::fs::create_dir_all(&keys).unwrap();
    let member = Member::open(
        MemberSettings {
            id,
            peers,
            data,
            key_file: keys.join(format!("node-{id}.key")),
            memory_bytes: memory,
            fates: fates.to_vec(),
            spanning: 2,
        },
        node,
        NODES.to_vec(),
        1,
        64 * 27,
        budget,
        Instant::now(),
    )
    .unwrap();
    let _ = ids;
    Running { driver, member }
}

/// Persisted: the group states no fates, and acknowledges a publish once a majority's disks hold
/// it.
#[test]
fn a_part_replicated_by_three_nodes_acknowledges_what_it_persisted_and_keeps_it_past_its_leader() {
    group_keeps_what_it_acknowledged(&[], Durability::Persisted);
}

/// Replicated (core step R-8): each node its own fate, a publish acknowledged once a majority holds
/// it across two fates, followers in memory under their durable promises; every acknowledged
/// batch is in the new leader's part after the old one is killed.
#[test]
fn a_part_held_across_three_fates_acknowledges_replicated_and_keeps_it_past_its_leader() {
    group_keeps_what_it_acknowledged(&[(1, 1), (2, 2), (3, 3)], Durability::Replicated);
}

/// A steady stream of publishes is written with its commits riding its entries: each node's commit
/// that moved alone waits for its next write within the node's period, the interval its liveness
/// stream sends at (`Member::restate_period`), and is written alone only once a period passed with
/// none. At a period of zero every commit was written alone the moment no write was out, as many
/// writes of the commit alone as of entries (on CI's disk, two log frames for each entry).
#[test]
fn a_steady_stream_writes_its_commits_with_its_entries() {
    const BATCHES: u32 = 200;
    let ids = Identities::new();
    let dir = tempfile::tempdir().unwrap();
    let mut runtime = LocalRuntime::new(&runtime()).unwrap();
    runtime
        .block_on(async move {
            let mut world =
                open_world(&ids, dir.path(), &[], Durability::Persisted, 64 << 20, 128).await;
            let mut wait = ProgressDeadline::begin(&[0], u64::MAX, FROZEN);
            while world.seen.acked.len() < BATCHES as usize {
                let acked = u32::try_from(world.seen.acked.len()).unwrap();
                world.publish((acked + 4).min(BATCHES));
                world.step().await;
                wait.check(&[world.seen.acked.len() as u64])
                    .expect("the stream was not acknowledged");
            }
            for running in world.running() {
                let stats = running.member.stats();
                assert!(
                    stats.writes.quiet * 2 < stats.writes.readies,
                    "node {} wrote its commit alone {} times beside {} writes of entries: {stats:?}",
                    running.member.id(),
                    stats.writes.quiet,
                    stats.writes.readies
                );
            }
        })
        .unwrap();
}

/// A part whose publishes pass what its log may hold keeps taking them, and every node comes to
/// hold every batch. Each node has 24 MiB, so its log holds about twice that and a mebibyte
/// (`hyper_log::Config` from the part's facts), and its window gives memory back a 5 MiB segment
/// at a time; 110 batches of most of a mebibyte, four in flight, are past twice the log's bound.
/// It holds the bus to what each of these let it reach: the log let go of what the window let go
/// (`Member::compact_behind_window`; without it the log filled and the group took nothing more,
/// every closed-loop round on fedora, 2026-10-19), a batch sent again answered once
/// (`Member::publish`), the window giving the replica memory (`Member::relieve_window`), a debt
/// held as soon as memory came back (`MemberBudget::settle_owed`), a dropped Raft message told
/// unreachable, and applying on the committed lane (`limits::GROUP_APPLY_HOLD`): each, missing,
/// stopped this test for good in some of its runs.
#[test]
fn a_part_whose_publishes_pass_its_logs_bound_lets_its_log_go_and_takes_them() {
    const MEMORY: u64 = 24 << 20;
    const BATCHES: u32 = 110;
    let payload = usize::try_from(wire::BATCH_BOUND).unwrap() - 4096;
    let ids = Identities::new();
    let dir = tempfile::tempdir().unwrap();
    let mut runtime = LocalRuntime::new(&runtime()).unwrap();
    runtime
        .block_on(async move {
            let mut world = open_world(
                &ids,
                dir.path(),
                &[],
                Durability::Persisted,
                MEMORY,
                payload,
            )
            .await;
            // Charged to the acknowledgements alone: a group that stops taking writes and elects
            // again and again redirects its publisher every time, which is no progress.
            let mut wait = ProgressDeadline::begin(&[0], u64::MAX, FROZEN);
            while world.seen.acked.len() < BATCHES as usize {
                // A publisher's window of four batches in flight, as a client holds its lane's.
                let acked = u32::try_from(world.seen.acked.len()).unwrap();
                world.publish((acked + 4).min(BATCHES));
                world.step().await;
                if let Err(frozen) = wait.check(&[world.seen.acked.len() as u64]) {
                    panic!(
                        "the batches past the log's bound were not acknowledged: {frozen:?}\n{}",
                        world.diagnose()
                    );
                }
            }
            for (sequence, (base, count)) in &world.seen.acked {
                assert_eq!(
                    (*base, *count),
                    (u64::from(*sequence), 1),
                    "in order, each once"
                );
            }
            // Every node's part comes to hold every batch: a node behind the start of a log its
            // leader compacted is given the leader's image and the entries after it.
            let ends = |world: &World| -> Vec<u64> {
                world
                    .nodes
                    .iter()
                    .flatten()
                    .map(|running| {
                        running
                            .member
                            .node()
                            .part()
                            .bounds(STREAM)
                            .map_or(0, |bounds| bounds.1)
                    })
                    .collect()
            };
            let mut wait = ProgressDeadline::begin(&ends(&world), u64::MAX, FROZEN);
            while ends(&world).iter().any(|end| *end < u64::from(BATCHES)) {
                world.step().await;
                if let Err(frozen) = wait.check(&ends(&world)) {
                    panic!(
                        "a node never came to hold every batch: {frozen:?}\n{}",
                        world.diagnose()
                    );
                }
            }
            for running in world.running() {
                let stats = running.member.stats();
                assert!(
                    stats.compactions > 0,
                    "node {} never let its log go: {stats:?}",
                    running.member.id()
                );
            }
        })
        .unwrap();
}

/// Three nodes of `memory` each stating `fates`, every acknowledgement of `class`, the client
/// connected to the first and a leader elected; each batch's record of `payload` bytes.
async fn open_world(
    ids: &Identities,
    dir: &Path,
    fates: &[(u64, u64)],
    class: Durability,
    memory: u64,
    payload: usize,
) -> World {
    let client_budget = MemoryBudget::new(64 << 20, 0).unwrap();
    let waker = poll_fn(|context| Poll::Ready(context.waker().clone())).await;
    let mut drivers = Vec::new();
    for id in NODES {
        let budget = MemoryBudget::new(64 << 20, 0).unwrap();
        let endpoint = ids.node(id, &budget);
        let driver = Driver::bind(endpoint, "127.0.0.1:0".parse().unwrap(), IO).unwrap();
        drivers.push(driver);
    }
    let addresses: Vec<SocketAddr> = drivers.iter().map(|d| d.local_addr().unwrap()).collect();
    let nodes = NODES
        .iter()
        .zip(drivers)
        .map(|(id, driver)| Some(open_node(ids, *id, dir, &addresses, driver, fates, memory)))
        .collect();
    let client = Driver::bind(
        ids.client(&client_budget),
        "127.0.0.1:0".parse().unwrap(),
        IO,
    )
    .unwrap();
    let mut world = World {
        nodes,
        addresses: addresses.clone(),
        client,
        leader: NODES[0],
        term: 0,
        unacked: BTreeMap::new(),
        seen: Seen::default(),
        waker,
        builder: BatchBuilder::new(),
        sent: 0,
        events: 0,
        subscribe_at: None,
        class,
        payload,
    };
    let now = Instant::now();
    for running in world.running() {
        running.member.connect(running.driver.endpoint(), now);
    }
    world
        .client
        .endpoint()
        .connect(now, NODES[0], addresses[0])
        .unwrap();
    // A leader is elected: the wait is charged to the terms the nodes reach.
    let mut wait = ProgressDeadline::begin(&[world.terms()], u64::MAX, FROZEN);
    while world.leader_now().is_none() {
        world.step().await;
        wait.check(&[world.terms()]).expect("no leader was elected");
    }
    world
}

/// Three nodes stating `fates`, every acknowledgement of `class` (module docs).
fn group_keeps_what_it_acknowledged(fates: &[(u64, u64)], class: Durability) {
    // The runtime's task owns what it uses.
    let fates = fates.to_vec();
    let ids = Identities::new();
    let dir = tempfile::tempdir().unwrap();
    let mut runtime = LocalRuntime::new(&runtime()).unwrap();
    runtime
        .block_on(async move {
            let mut world = open_world(&ids, dir.path(), &fates, class, NODE_MEMORY, 0).await;
            let addresses = world.addresses.clone();

            // A hundred batches, each acknowledged persisted at the offsets the part gave it, in
            // order; the client follows the redirect of a node that does not lead.
            let mut wait = ProgressDeadline::begin(&[0], u64::MAX, FROZEN);
            while world.seen.acked.len() < 100 {
                world.publish(100);
                world.step().await;
                wait.check(&[world.seen.acked.len() as u64 + world.seen.redirects])
                    .expect("the batches were not acknowledged");
            }
            for (sequence, (base, count)) in &world.seen.acked {
                assert_eq!(
                    (*base, *count),
                    (u64::from(*sequence), 1),
                    "in order, each once"
                );
            }

            // A subscriber of the leader is delivered every batch, in order.
            let leader = world.leader;
            let subscribing = Client::new(leader);
            let mut wait = ProgressDeadline::begin(&[0], u64::MAX, FROZEN);
            while world.seen.delivered.len() < 100 {
                if world.subscribe_at.is_none() {
                    let asked = subscribing.subscribe(
                        world.client.endpoint(),
                        Instant::now(),
                        STREAM,
                        wire::From::Offset(0),
                        focal_bus::limits::SERVE_PERIOD,
                    );
                    if asked.is_ok() {
                        world.subscribe_at = Some(0);
                    }
                }
                world.step().await;
                wait.check(&[world.seen.delivered.len() as u64])
                    .expect("the subscriber was not delivered");
            }
            for (at, (base, sequence)) in world.seen.delivered.iter().enumerate() {
                assert_eq!((*base, *sequence), (at as u64, at as u32));
            }

            // The leader is killed. The two others elect one of themselves; the client follows the
            // redirects and sends again what was not acknowledged.
            let killed = NODES.iter().position(|n| *n == leader).unwrap();
            world.nodes[killed] = None;
            world.client.endpoint().disconnect(Instant::now(), leader);
            let survivor = NODES.iter().copied().find(|n| *n != leader).unwrap();
            world.leader = survivor;
            let at = NODES.iter().position(|n| *n == survivor).unwrap();
            world
                .client
                .endpoint()
                .connect(Instant::now(), survivor, addresses[at])
                .unwrap();
            let mut wait = ProgressDeadline::begin(&[world.terms()], u64::MAX, FROZEN);
            while world.leader_now().is_none_or(|l| l == leader) {
                world.step().await;
                wait.check(&[world.terms()])
                    .expect("no leader was elected after the kill");
            }

            // A hundred more, every one acknowledged; the batches acknowledged before the kill keep
            // their offsets, and the next batch's offset follows them.
            let mut wait = ProgressDeadline::begin(&[0], u64::MAX, FROZEN);
            while world.seen.acked.len() < 200 {
                world.publish(200);
                world.step().await;
                wait.check(&[world.seen.acked.len() as u64 + world.seen.redirects])
                    .expect("the batches after the kill were not acknowledged");
            }
            for (sequence, (base, count)) in &world.seen.acked {
                assert_eq!(
                    (*base, *count),
                    (u64::from(*sequence), 1),
                    "nothing acknowledged lost"
                );
            }

            // The newest batch sent again is recognised: acknowledged at its offset, not appended.
            let again_before = world.seen.again;
            let last = batch(&mut world.builder, 199);
            world.unacked.insert(199, last);
            let mut wait = ProgressDeadline::begin(&[0], u64::MAX, FROZEN);
            while world.seen.again == again_before {
                world.publish(200);
                world.step().await;
                wait.check(&[world.seen.again])
                    .expect("the batch sent again was not acknowledged");
            }
            let new_leader = world.leader_now().unwrap();
            let at = NODES.iter().position(|n| *n == new_leader).unwrap();
            let bounds = world.nodes[at]
                .as_ref()
                .unwrap()
                .member
                .node()
                .part()
                .bounds(STREAM)
                .unwrap();
            assert_eq!(bounds.1, 200, "two hundred records, none twice");
        })
        .unwrap();
}
