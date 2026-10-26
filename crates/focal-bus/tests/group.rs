//! Three bus nodes replicating one part, and a client, over real UDP sockets on loopback, their four
//! endpoints driven by one task on one hyper-rt shard, each node's log a sealed hyper-log file in a
//! directory of its own (doc 34, step 3b.3).
//!
//! The nodes elect a leader by their liveness stream; the client is focal-bus's client of the group
//! (`Client::group`): it publishes to whichever node it knows, follows each redirect to the leader the
//! node names, and every batch it publishes is acknowledged, persisted, at the offsets the part gave
//! it, in order. A subscriber of the leader is delivered every batch, in order. The leader is killed,
//! its socket gone with nothing said, as a kill -9 leaves it: the two others elect one of
//! themselves, and the client, told by its endpoint that it cannot hear the leader, asks the others
//! until one names the new leader, follows it, sends again what was not acknowledged, and is
//! acknowledged once for each batch: every batch acknowledged before the kill is in the new leader's
//! part, at the offsets it was acknowledged at, and a batch sent again is recognised and not
//! appended twice. Nothing tells the client where the leader went but the nodes.

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

use std::collections::{BTreeMap, BTreeSet};
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

/// The identities: the authority's certificate, each node's, the client's.
struct Identities {
    root: Vec<u8>,
    nodes: Vec<(Vec<u8>, Vec<u8>)>,
    client: (Vec<u8>, Vec<u8>),
}

impl Identities {
    fn new() -> Self {
        let pki = Pki::new();
        let nodes = NODES.iter().map(|id| pki.issue(&node_name(*id))).collect();
        let client = pki.issue("focal-bus-client-1");
        Self {
            root: pki.root,
            nodes,
            client,
        }
    }

    /// The identities of the bus's identity directory `dir`, read as a node's service reads them as
    /// it starts (`pki::load_or_make`), which seals a directory an earlier binary kept in the clear.
    fn load(dir: &Path) -> Self {
        let local = focal_bus::pki::load_or_make(dir, None, NODES.len(), 1).unwrap();
        let pair = |identity: &focal_bus::pki::Identity| {
            (identity.certificate.clone(), identity.key.clone())
        };
        Self {
            root: local.root.clone(),
            nodes: local.nodes.iter().map(pair).collect(),
            client: pair(&local.clients[0]),
        }
    }

    fn credentials(&self, own: &(Vec<u8>, Vec<u8>)) -> Credentials {
        Credentials {
            chain: vec![CertificateDer::from(own.0.clone())],
            key: PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(own.1.clone())),
            roots: vec![CertificateDer::from(self.root.clone())],
        }
    }

    /// The endpoint of node `id` on a socket bound to `bound`, knowing the other nodes and the
    /// client, keyed as a node's is across its restarts at that address
    /// (`focal_bus::serve::restart_key`).
    fn node(
        &self,
        id: u64,
        budget: &MemoryBudget,
        bound: SocketAddr,
    ) -> Endpoint<Bus, BusBudget, BusDirectory> {
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
        Endpoint::new_restartable(
            Config {
                tls: Tls::mutual(&self.credentials(own), true).unwrap(),
                role: Role::Node,
                limits: focal_bus::limits::node(1, NODES.len() - 1).unwrap(),
                congestion: Default::default(),
            },
            BusBudget::new(budget.clone()),
            directory,
            Instant::now(),
            &focal_bus::serve::restart_key(&own.1, bound).unwrap(),
            None,
        )
        .unwrap()
    }

    /// Node `id`'s driver on a socket bound to `listen`, as a node's service binds it.
    fn bind_node(&self, id: u64, budget: &MemoryBudget, listen: SocketAddr) -> Drv {
        let (socket, bound) = focal_bus::serve::node_socket(listen).unwrap();
        Driver::new(self.node(id, budget, bound), socket, IO).unwrap()
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
                tls: Tls::mutual(&self.credentials(&self.client), false).unwrap(),
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
    /// Batches acknowledged as taken already without their offsets: a batch sent again that was not
    /// its session's newest (doc 34 P6, Kafka's `DUPLICATE_SEQUENCE_NUMBER`), as one a failover left
    /// unacknowledged is.
    taken: BTreeSet<u32>,
    again: u64,
    delivered: Vec<(u64, u32)>,
    subscribed: bool,
    /// Subscribes that ended without a subscription or ended one: redirected, or refused.
    subscription_ends: u64,
    /// Gaps told: offsets a node let go before the subscriber took them.
    gaps: u64,
    /// Each moved subscription's next offset, as its node told it (doc 34 P7).
    moves: Vec<u64>,
    redirects: u64,
    /// Times the client sent to another node (`Delivery::Rerouted`).
    reroutes: u64,
    /// Each node whose connection the client was told ended (`Delivery::Lost`), in order: what a
    /// subscription there was is gone.
    lost: Vec<u64>,
    /// Answers that the part waits for an earlier sequence of the session (KIP-98's
    /// OUT_OF_ORDER_SEQUENCE_NUMBER): every batch not yet acknowledged goes again at the next step.
    out_of_order: u64,
}

impl Seen {
    /// The batches acknowledged, at their offsets or as taken already.
    fn acknowledged(&self) -> usize {
        self.acked.len()
            + self
                .taken
                .iter()
                .filter(|sequence| !self.acked.contains_key(sequence))
                .count()
    }
}

struct World {
    nodes: Vec<Option<Running>>,
    addresses: Vec<SocketAddr>,
    client: Drv,
    /// The client of the group: the node it sends to, the leader it follows and the term it was
    /// named at, as its own events taught it.
    bus: Client,
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
    /// The node whose loop stalls, if one does: not polled, so its socket goes unread, and nothing
    /// of it turns.
    paused: Option<usize>,
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
        let (leader, term) = self.bus.leader();
        let mut out = format!(
            "client: sends to {}, leader {leader} term {term}, {} reroutes, {} acknowledged, {} \
             unacknowledged, {} sent; {} delivered, subscribed {}, {} subscriptions ended, {} of \
             them moved, {} gaps",
            self.bus.node(),
            self.seen.reroutes,
            self.seen.acked.len(),
            self.unacked.len(),
            self.sent,
            self.seen.delivered.len(),
            self.seen.subscribed,
            self.seen.subscription_ends,
            self.seen.moves.len(),
            self.seen.gaps
        );
        let unacked: Vec<u32> = self.unacked.keys().copied().take(8).collect();
        out.push_str(&format!(
            "\nclient: unacknowledged {unacked:?}, {} taken without offsets, {} out of order, lost \
             {:?}",
            self.seen.taken.len(),
            self.seen.out_of_order,
            self.seen.lost
        ));
        for running in self.nodes.iter().flatten() {
            let member = &running.member;
            let part = member.node().part();
            let verdicts: Vec<String> = unacked
                .iter()
                .map(|sequence| {
                    let verdict = part.judge(STREAM, CLIENT, PRODUCER, u64::from(*sequence), 1);
                    format!("{sequence}: {verdict:?}")
                })
                .collect();
            out.push_str(&format!(
                "\nnode {}: leads {}, term {}: {:?}\n  its part: {:?}\n  its sessions judge \
                 the unacknowledged: {verdicts:?}",
                member.id(),
                member.is_leader(),
                member.term(),
                member.stats(),
                member.node().stats()
            ));
        }
        out
    }

    /// Handles every event that is ready, then gives each node its turn; waits at most until the
    /// soonest of the nodes' deadlines, or a tick.
    async fn step(&mut self) {
        let paused = self.paused;
        let soonest = self
            .nodes
            .iter()
            .enumerate()
            .filter(|(at, _)| Some(*at) != paused)
            .filter_map(|(_, running)| running.as_ref())
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
                if Some(at) != paused
                    && let Some(running) = running
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
        for (at, running) in self.nodes.iter_mut().enumerate() {
            if Some(at) != paused
                && let Some(running) = running
            {
                running.member.waited(began, now);
            }
        }
        for (whose, event) in found {
            self.events += 1;
            match whose {
                Some(at) => self.node_event(at, event, now),
                None => self.client_event(event),
            }
        }
        for at in 0..self.nodes.len() {
            if Some(at) != paused
                && let Some(running) = self.nodes[at].as_mut()
            {
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
        let seen = &mut self.seen;
        let unacked = &mut self.unacked;
        let class = self.class;
        let endpoint = self.client.endpoint();
        self.bus
            .on_event(endpoint, event, Instant::now(), |delivery| match delivery {
                Delivery::Acked(ack) if ack.class == wire::Ack::OUT_OF_ORDER => {
                    seen.out_of_order += 1
                }
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
                    } else {
                        seen.taken.insert(ack.sequence);
                    }
                }
                Delivery::Redirected(_) => seen.redirects += 1,
                Delivery::Rerouted { .. } => seen.reroutes += 1,
                Delivery::Batch(batch) => {
                    seen.delivered.push((batch.base_offset(), batch.sequence()))
                }
                Delivery::Subscribed { .. } => seen.subscribed = true,
                Delivery::SubscribeRedirected { .. } | Delivery::Unsubscribed { .. } => {
                    seen.subscribed = false;
                    seen.subscription_ends += 1;
                }
                Delivery::Moved(moved) => {
                    assert_eq!(moved.stream, STREAM);
                    seen.subscribed = false;
                    seen.subscription_ends += 1;
                    seen.moves.push(moved.next);
                }
                Delivery::Gap(_) => seen.gaps += 1,
                Delivery::Lost { node, .. } => seen.lost.push(node),
                Delivery::Refused(_) => {}
            });
    }

    /// Sends the node the client sends to every batch not yet acknowledged, and new ones up to
    /// `until`, unless the client's back-off holds them; a node not connected is dialed.
    fn publish(&mut self, until: u32) {
        while self.sent < until {
            let bytes = sized_batch(&mut self.builder, self.sent, self.payload);
            self.unacked.insert(self.sent, bytes);
            self.sent += 1;
        }
        let now = Instant::now();
        if !self.bus.ready(now) {
            return;
        }
        // While the client asks a node about the leader, only the oldest batch owed goes.
        let sends = if self.bus.asking() {
            1
        } else {
            self.unacked.len()
        };
        for bytes in self.unacked.values().take(sends) {
            match self.bus.publish(self.client.endpoint(), bytes) {
                Ok(()) => {}
                Err(Refusal::NotConnected) => {
                    let _ = self.bus.dial(self.client.endpoint(), now);
                    return;
                }
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
    let member = open_member(id, dir, addresses, fates, memory).unwrap();
    let _ = ids;
    Running { driver, member }
}

/// Node `id`'s member opened on its data directory in `dir`, or the reason it is refused.
fn open_member(
    id: u64,
    dir: &Path,
    addresses: &[SocketAddr],
    fates: &[(u64, u64)],
    memory: u64,
) -> Result<Member, focal_bus::BusError> {
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
    Member::open(
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

/// Do: with the leader above a follower, stop driving that follower, as a node whose loop stalled:
/// its socket goes unread and its lane from the leader fills until the leader's outbox for it is at
/// its bound. Then kill it, reopen it on its directory and publish on. Expect: the reopened
/// follower comes to hold every batch. It dials its leader (the lesser of a pair dials), so its new
/// connection takes the old one's place and no close of the old is told; what waited for the
/// stalled process's lane holds neither the probe the leader sends the new one nor any frame after
/// it (found by Quiver's crash test on its lab branch, kiln's CP-T2.2, Focal's condition 3).
#[test]
fn a_follower_reopened_after_its_lane_filled_comes_to_hold_every_batch() {
    const PAYLOAD: usize = 64 << 10;
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
                NODE_MEMORY,
                PAYLOAD,
            )
            .await;
            let reopen = |world: &mut World, at: usize| {
                let id = NODES[at];
                world.nodes[at] = None;
                let budget = MemoryBudget::new(64 << 20, 0).unwrap();
                let driver = ids.bind_node(id, &budget, world.addresses[at]);
                let mut running = open_node(
                    &ids,
                    id,
                    dir.path(),
                    &world.addresses,
                    driver,
                    &[],
                    NODE_MEMORY,
                );
                running
                    .member
                    .connect(running.driver.endpoint(), Instant::now());
                world.nodes[at] = Some(running);
            };
            let mut until = 20;
            publish_until(&mut world, until).await;
            // The lowest node follows: a leader there is killed and reopened until another leads.
            while world.leader_now() == Some(NODES[0]) {
                world.nodes[0] = None;
                let mut wait = ProgressDeadline::begin(&[world.terms()], u64::MAX, FROZEN);
                while world.leader_now().is_none_or(|l| l == NODES[0]) {
                    world.step().await;
                    wait.check(&[world.terms()])
                        .expect("no leader was elected after the kill");
                }
                reopen(&mut world, 0);
                until += 10;
                publish_until(&mut world, until).await;
            }
            let leader = world.leader_now().unwrap();
            let leader_at = NODES.iter().position(|n| *n == leader).unwrap();
            let outboxed = |world: &World| {
                world.nodes[leader_at]
                    .as_ref()
                    .map_or(0, |running| running.member.stats().outboxed)
            };

            // The follower's loop stalls: publishing goes on, acknowledged on the leader and the
            // other follower, until a frame for the stalled one waits in the leader's outbox. Its
            // lane fills within the window the stalled node last granted, at most the transport's
            // stream window ceiling: that many batches of this payload, and the one its sending
            // side holds beyond, bound the wait.
            world.paused = Some(0);
            let before = outboxed(&world);
            let most =
                usize::try_from(hyper_transport::stream_window_ceiling()).unwrap() / PAYLOAD + 2;
            for _ in 0..most {
                if outboxed(&world) > before {
                    break;
                }
                until += 1;
                publish_until(&mut world, until).await;
            }
            assert!(
                outboxed(&world) > before,
                "no frame for the stalled follower waited in {most} batches\n{}",
                world.diagnose()
            );

            // Killed and reopened on its directory: it comes to hold every batch.
            world.paused = None;
            reopen(&mut world, 0);
            until += 20;
            publish_until(&mut world, until).await;
            let mut wait = ProgressDeadline::begin(&ends(&world), u64::MAX, FROZEN);
            loop {
                let reached = ends(&world);
                if reached.iter().all(|end| *end >= u64::from(until)) {
                    break;
                }
                world.step().await;
                if let Err(frozen) = wait.check(&ends(&world)) {
                    panic!(
                        "the reopened follower never came to hold every batch: {frozen:?}\n{}",
                        world.diagnose()
                    );
                }
            }
        })
        .unwrap();
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
            while world.seen.acknowledged() < BATCHES as usize {
                let acked = u32::try_from(world.seen.acknowledged()).unwrap();
                world.publish((acked + 4).min(BATCHES));
                world.step().await;
                wait.check(&[world.seen.acknowledged() as u64])
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
            while world.seen.acknowledged() < BATCHES as usize {
                // A publisher's window of four batches in flight, as a client holds its lane's.
                let acked = u32::try_from(world.seen.acknowledged()).unwrap();
                world.publish((acked + 4).min(BATCHES));
                world.step().await;
                if let Err(frozen) = wait.check(&[world.seen.acknowledged() as u64]) {
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
        let driver = ids.bind_node(id, &budget, "127.0.0.1:0".parse().unwrap());
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
    let group: Vec<(u64, SocketAddr)> = NODES.iter().copied().zip(addresses.clone()).collect();
    let mut world = World {
        nodes,
        addresses: addresses.clone(),
        client,
        bus: Client::group(&group).unwrap(),
        unacked: BTreeMap::new(),
        seen: Seen::default(),
        waker,
        builder: BatchBuilder::new(),
        sent: 0,
        events: 0,
        subscribe_at: None,
        class,
        payload,
        paused: None,
    };
    let now = Instant::now();
    for running in world.running() {
        running.member.connect(running.driver.endpoint(), now);
    }
    world.bus.dial(world.client.endpoint(), now).unwrap();
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

            // A hundred batches, each acknowledged persisted at the offsets the part gave it, in
            // order; the client follows the redirect of a node that does not lead.
            let mut wait = ProgressDeadline::begin(&[0], u64::MAX, FROZEN);
            while world.seen.acknowledged() < 100 {
                world.publish(100);
                world.step().await;
                wait.check(&[world.seen.acknowledged() as u64 + world.seen.redirects])
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
            let leader = world.bus.node();
            assert_eq!(
                Some(leader),
                world.leader_now(),
                "the client learned the leader"
            );
            let mut wait = ProgressDeadline::begin(&[0], u64::MAX, FROZEN);
            while world.seen.delivered.len() < 100 {
                if world.subscribe_at.is_none() {
                    let asked = world.bus.subscribe(
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

            // The leader is killed: its socket goes with nothing said, as a kill -9 leaves it, and
            // its connection to the client stays open on the client's side. The two others elect one
            // of themselves; the client, told its path to the leader is degrading, asks them, follows
            // the leader they name and sends again what was not acknowledged. Nothing else tells it.
            let killed = NODES.iter().position(|n| *n == leader).unwrap();
            world.nodes[killed] = None;
            let mut wait = ProgressDeadline::begin(&[world.terms()], u64::MAX, FROZEN);
            while world.leader_now().is_none_or(|l| l == leader) {
                world.step().await;
                wait.check(&[world.terms()])
                    .expect("no leader was elected after the kill");
            }

            // A hundred more, every one acknowledged; the batches acknowledged before the kill keep
            // their offsets, and the next batch's offset follows them. Charged to what the client
            // hears and does: acknowledgements, redirects, and the nodes it turns to.
            let killed_at = Instant::now();
            let mut wait = ProgressDeadline::begin(&[0], u64::MAX, FROZEN);
            while world.seen.acknowledged() < 200 {
                world.publish(200);
                world.step().await;
                wait.check(&[world.seen.acknowledged() as u64
                    + world.seen.redirects
                    + world.seen.reroutes])
                    .expect("the batches after the kill were not acknowledged");
            }
            let new_leader = world.leader_now().unwrap();
            assert_eq!(
                world.bus.node(),
                new_leader,
                "the client sends to the new leader\n{}",
                world.diagnose()
            );
            assert!(
                world.seen.reroutes > 0,
                "the client turned away from the killed leader"
            );
            eprintln!(
                "acknowledged again {:?} after the kill: {} reroutes, {} redirects",
                killed_at.elapsed(),
                world.seen.reroutes,
                world.seen.redirects
            );
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

/// Node `id`'s data directory in `dir`, and the keys its files are sealed under (29 §12.9).
fn node_files(dir: &Path, id: u64) -> (std::path::PathBuf, focal_seal::DataKeys) {
    let data = dir.join(format!("node-{id}"));
    let keys = focal_seal::DataKeys::new(&data, dir.join("keys").join(format!("node-{id}.key")));
    (data, keys)
}

/// Node `id`'s run and part image made what an earlier binary kept, the node down: in the clear,
/// its directory unmarked. Its log stays as it is.
fn unseal_node(dir: &Path, id: u64) {
    let (data, keys) = node_files(dir, id);
    let group = keys.key(focal_seal::Store::Group).unwrap();
    for (name, most) in [("part.image", 64usize << 20), ("run", 8)] {
        let path = data.join(name);
        let bytes = std::fs::read(&path).unwrap();
        let plain = focal_seal::file::open(&group, bytes, most).unwrap();
        std::fs::write(&path, &*plain).unwrap();
    }
    std::fs::remove_file(data.join("SEALED")).unwrap();
}

/// Node `id`'s run and image sealed and its directory marked so.
fn assert_node_sealed(dir: &Path, id: u64) {
    let (data, _) = node_files(dir, id);
    assert!(data.join("SEALED").is_file(), "node {id} is marked sealed");
    assert_ne!(
        std::fs::read(data.join("run")).unwrap().len(),
        8,
        "node {id}'s run is sealed"
    );
    let image = std::fs::read(data.join("part.image")).unwrap();
    assert!(!image.starts_with(b"FBPI"), "node {id}'s image is sealed");
}

/// Writes `bytes` to `path` owner-only, as the identity directory's files are written.
fn write_owner_only(path: &Path, bytes: &[u8]) {
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create(true).truncate(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    std::io::Write::write_all(&mut options.open(path).unwrap(), bytes).unwrap();
}

/// The stems of the identity directory's files: each node's, then the client's.
fn identity_stems() -> impl Iterator<Item = String> {
    NODES
        .iter()
        .map(|id| format!("node-{id}"))
        .chain(std::iter::once("client-1".to_owned()))
}

/// The bus's identity directory `dir` holding `ids` as a binary before 29 §12.9 kept it: each key a
/// DER document in the clear, the authority written last, no keys of the directory's own and no
/// mark.
fn identities_in_the_clear(dir: &Path, ids: &Identities) {
    std::fs::create_dir_all(dir).unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(dir, std::fs::Permissions::from_mode(0o700)).unwrap();
    }
    for name in ["SEALED", "SEAL.node", "SEAL.root"] {
        let _ = std::fs::remove_file(dir.join(name));
    }
    let pairs = ids.nodes.iter().chain(std::iter::once(&ids.client));
    for (stem, (certificate, key)) in identity_stems().zip(pairs) {
        write_owner_only(&dir.join(format!("{stem}.der")), certificate);
        write_owner_only(&dir.join(format!("{stem}.key")), key);
    }
    write_owner_only(&dir.join("root.der"), &ids.root);
}

/// The identity directory `dir` sealed: marked so, and no key file a DER document or holding any
/// of its key's bytes.
fn assert_identities_sealed(dir: &Path, ids: &Identities) {
    assert!(
        dir.join("SEALED").is_file(),
        "the identity directory is marked sealed"
    );
    let pairs = ids.nodes.iter().chain(std::iter::once(&ids.client));
    for (stem, (_, key)) in identity_stems().zip(pairs) {
        let file = std::fs::read(dir.join(format!("{stem}.key"))).unwrap();
        assert_ne!(file.first(), Some(&0x30), "{stem}.key lies in the clear");
        assert!(
            !file
                .windows(key.len())
                .any(|window| window == key.as_slice()),
            "{stem}.key holds its key's bytes"
        );
    }
}

/// Node `id`'s stream floor: the first offset its part still holds.
fn floor_of(world: &World, id: u64) -> u64 {
    let at = NODES.iter().position(|n| *n == id).unwrap();
    world.nodes[at]
        .as_ref()
        .and_then(|running| running.member.node().part().bounds(STREAM))
        .map_or(0, |bounds| bounds.0)
}

/// A subscriber of the stream: the node it is subscribed at, the offset it is delivered next, and
/// how many of the client's deliveries it has read.
struct Consumer {
    at: Option<u64>,
    next: u64,
    read: usize,
    /// How many of the client's moved subscriptions it has read.
    moves: usize,
    /// How many of the client's lost connections it has read.
    lost: usize,
}

/// Publishes up to `until` with four batches in flight, as [`publish_until`] does, while the
/// consumer is delivered every offset below `until`, each once and in order, subscribed at the
/// leader from the offset it expects next whenever it is subscribed nowhere; charged to the
/// acknowledgements and the offsets delivered.
async fn serve_until(world: &mut World, consumer: &mut Consumer, until: u32) {
    let progress =
        |world: &World, consumer: &Consumer| [world.seen.acknowledged() as u64, consumer.next];
    let mut wait = ProgressDeadline::begin(&progress(world, consumer), u64::MAX, FROZEN);
    let mut ends = world.seen.subscription_ends;
    while world.seen.acknowledged() < until as usize || consumer.next < u64::from(until) {
        let acked = u32::try_from(world.seen.acknowledged()).unwrap();
        if acked < until {
            world.publish((acked + 4).min(until));
        }
        // A subscription whose connection went (its node restarted or killed, or given up by the
        // client as degrading) is gone with it: the consumer subscribes again.
        for node in &world.seen.lost[consumer.lost..] {
            if consumer.at == Some(*node) {
                eprintln!(
                    "the connection to node {node} holding the subscription went: subscribing \
                     again from {}",
                    consumer.next
                );
                consumer.at = None;
            }
        }
        consumer.lost = world.seen.lost.len();
        // A subscription its node moved as it stopped leading (doc 34 P7), or a subscribe
        // redirected or refused, is no subscription: the consumer subscribes again at the leader.
        if world.seen.subscription_ends != ends {
            ends = world.seen.subscription_ends;
            eprintln!(
                "the subscription at node {:?} ended (moved, redirected or refused): subscribing \
                 again from {}",
                consumer.at, consumer.next
            );
            consumer.at = None;
        }
        assert_eq!(
            world.seen.gaps,
            0,
            "a node let offsets go before the subscriber took them\n{}",
            world.diagnose()
        );
        // Where the client sends, as its own events taught it: the nodes' redirects and moves.
        let now = Instant::now();
        if consumer.at.is_none() && world.bus.ready(now) {
            let asked = world.bus.subscribe(
                world.client.endpoint(),
                now,
                STREAM,
                wire::From::Offset(consumer.next),
                focal_bus::limits::SERVE_PERIOD,
            );
            if asked.is_ok() {
                consumer.at = Some(world.bus.node());
            } else {
                let _ = world.bus.dial(world.client.endpoint(), now);
            }
        }
        world.step().await;
        for (base, sequence) in &world.seen.delivered[consumer.read..] {
            assert_eq!(
                (*base, u64::from(*sequence)),
                (consumer.next, consumer.next),
                "delivered once and in order"
            );
            consumer.next += 1;
        }
        consumer.read = world.seen.delivered.len();
        // A node that stopped leading names the offset after every one it delivered: the lane is
        // ordered, so the consumer has taken each of them.
        for next in &world.seen.moves[consumer.moves..] {
            assert_eq!(*next, consumer.next, "a moved subscription's next offset");
        }
        consumer.moves = world.seen.moves.len();
        if let Err(frozen) = wait.check(&progress(world, consumer)) {
            panic!(
                "the batches were not acknowledged and delivered: {frozen:?}, consumer at {:?} \
                 next {}\n{}",
                consumer.at,
                consumer.next,
                world.diagnose()
            );
        }
    }
}

/// Publishes up to `until` with four batches in flight, as a client holds its lane's, until every
/// batch to it is acknowledged; charged to the acknowledgements.
async fn publish_until(world: &mut World, until: u32) {
    let mut wait = ProgressDeadline::begin(&[0], u64::MAX, FROZEN);
    while world.seen.acknowledged() < until as usize {
        let acked = u32::try_from(world.seen.acknowledged()).unwrap();
        world.publish((acked + 4).min(until));
        world.step().await;
        if let Err(frozen) = wait.check(&[world.seen.acknowledged() as u64]) {
            panic!(
                "the batches were not acknowledged: {frozen:?}\n{}",
                world.diagnose()
            );
        }
    }
}

/// How far each running node's stream reaches.
fn ends(world: &World) -> Vec<u64> {
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
}

/// Do: leave in a node's data directory what an earlier binary's first image install left when cut
/// before its rename (its temporary, the image in the clear, and no image), then open the node.
/// Expect: the node opens, marked sealed, with no temporary left: nothing else would have replaced
/// it before the node's next image (29 §12.9).
#[test]
fn a_node_whose_first_image_install_was_cut_keeps_nothing_of_it_in_the_clear() {
    let dir = tempfile::tempdir().unwrap();
    let (data, _) = node_files(dir.path(), NODES[0]);
    std::fs::create_dir_all(&data).unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&data, std::fs::Permissions::from_mode(0o700)).unwrap();
    }
    let temporary = data.join("part.part");
    let mut clear = b"FBPI".to_vec();
    clear.resize(4096, 0x5a);
    std::fs::write(&temporary, &clear).unwrap();
    let addresses: Vec<SocketAddr> = NODES
        .iter()
        .map(|id| SocketAddr::from(([127, 0, 0, 1], 40_000 + *id as u16)))
        .collect();
    let mut runtime = LocalRuntime::new(&runtime()).unwrap();
    runtime
        .block_on(async move {
            let member = open_member(NODES[0], dir.path(), &addresses, &[], NODE_MEMORY).unwrap();
            assert!(data.join("SEALED").is_file(), "the node is marked sealed");
            assert!(
                !temporary.exists(),
                "the cut install's temporary lies in the clear"
            );
            drop(member);
        })
        .unwrap();
}

/// Do: make the group's identity directory as an earlier binary kept it, in the clear, and start a
/// group of three on it whose window lets its batches go, so each node keeps an image; turn the
/// directory back to the clear and restart the nodes one by one, each first given its run and image
/// as an earlier binary kept them (in the clear, unmarked) and reading its identity from the
/// directory as its service does, publishing and consuming between the restarts; then, a node down,
/// plant its image in the clear after its mark, flip a byte of its sealed image, and put another
/// node's image in its place, and do the same to a key of the identity directory (29 §12.9).
/// Expect: the directory is sealed by the first start and again by the first restart, every key
/// what it was; each node seals its run and image as it opens and marks itself so, and reopens at
/// its image's floor; the group acknowledges every batch at the offset it gave it, in order, each
/// once, the producer's session carrying on through every restart (a batch sent again is
/// recognised); a subscriber following the leader (it first asks a node that does not lead, and
/// subscribes again after each redirect and each subscription a node that stops leading moves, doc
/// 34 P7) is delivered every batch from where it began, once and in order, each move naming the
/// offset it had reached; every node's stream comes to reach every batch; the planted, flipped and
/// carried images and keys are each refused.
#[test]
fn a_group_kept_in_the_clear_seals_its_files_as_its_nodes_restart() {
    const MEMORY: u64 = 24 << 20;
    let payload = usize::try_from(wire::BATCH_BOUND).unwrap() - 4096;
    let dir = tempfile::tempdir().unwrap();
    // The group's identity directory as an earlier binary made it, sealed by the first start.
    let pki = dir.path().join("pki");
    identities_in_the_clear(&pki, &Identities::new());
    let ids = Identities::load(&pki);
    assert_identities_sealed(&pki, &ids);
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
            let began = Instant::now();
            publish_until(&mut world, 60).await;
            // What each phase took, for the record (29 §12.9): no wait is judged by it.
            eprintln!("60 batches before any restart: {:?}", began.elapsed());
            // Each node keeps an image: its window let batches go. Charged to how far each reaches.
            let imaged = |dir: &Path| {
                NODES
                    .iter()
                    .all(|id| node_files(dir, *id).0.join("part.image").is_file())
            };
            let mut wait = ProgressDeadline::begin(&ends(&world), u64::MAX, FROZEN);
            while !imaged(dir.path()) {
                world.step().await;
                wait.check(&ends(&world))
                    .expect("a node never kept an image");
            }
            // A subscriber from the stream's next offset, delivered every batch from there through
            // every restart.
            let mut consumer = Consumer {
                at: None,
                next: 60,
                read: world.seen.delivered.len(),
                moves: world.seen.moves.len(),
                lost: world.seen.lost.len(),
            };
            // It first asks a node that does not lead, as a client that knows no leader does: the
            // node redirects it, and it subscribes again at the leader (`serve_until`). Unless that
            // node has come to lead meanwhile, and takes it.
            let leader = world.leader_now().expect("a leader before the restarts");
            let follower = NODES.iter().copied().find(|n| *n != leader).unwrap();
            let follower_at = NODES.iter().position(|n| *n == follower).unwrap();
            let ended = world.seen.subscription_ends;
            let mut wait = ProgressDeadline::begin(&[0], u64::MAX, FROZEN);
            while world.seen.subscription_ends == ended && !world.seen.subscribed {
                if consumer.at.is_none() {
                    let asked = Client::new(follower).subscribe(
                        world.client.endpoint(),
                        Instant::now(),
                        STREAM,
                        wire::From::Offset(consumer.next),
                        focal_bus::limits::SERVE_PERIOD,
                    );
                    if asked.is_ok() {
                        consumer.at = Some(follower);
                    } else {
                        let _ = world.client.endpoint().connect(
                            Instant::now(),
                            follower,
                            world.addresses[follower_at],
                        );
                    }
                }
                world.step().await;
                wait.check(&[world.seen.subscription_ends])
                    .expect("a subscribe at a node that does not lead was never answered");
            }
            if world.seen.subscription_ends != ended {
                consumer.at = None;
            }
            // The identity directory as the earlier binary left it: the first node restarted
            // seals it again.
            identities_in_the_clear(&pki, &ids);
            let mut until = 60;
            for id in NODES {
                let at = NODES.iter().position(|n| *n == id).unwrap();
                let floor = floor_of(&world, id);
                assert!(floor > 0, "node {id}'s window let batches go");
                // The node goes down with nothing said; the client and the consumer learn it from
                // their endpoint (the restarted node resets what its last run held, or its path is
                // degrading first) and from the nodes.
                world.nodes[at] = None;
                unseal_node(dir.path(), id);
                // The node starts as its service does: its identity read from the directory.
                let loaded = Identities::load(&pki);
                assert_eq!((&loaded.nodes, &loaded.client), (&ids.nodes, &ids.client));
                assert_identities_sealed(&pki, &ids);
                let budget = MemoryBudget::new(64 << 20, 0).unwrap();
                let driver = loaded.bind_node(id, &budget, world.addresses[at]);
                let mut running = open_node(
                    &loaded,
                    id,
                    dir.path(),
                    &world.addresses,
                    driver,
                    &[],
                    MEMORY,
                );
                running
                    .member
                    .connect(running.driver.endpoint(), Instant::now());
                world.nodes[at] = Some(running);
                assert_node_sealed(dir.path(), id);
                // Its floor is its image's: none lost, and none it had not reached.
                let reopened = floor_of(&world, id);
                assert!(
                    reopened > 0 && reopened <= floor,
                    "node {id} reopened at floor {reopened}, where it stood at {floor}"
                );
                until += 20;
                let began = Instant::now();
                serve_until(&mut world, &mut consumer, until).await;
                eprintln!(
                    "20 batches after node {id} restarted: {:?}",
                    began.elapsed()
                );
            }
            for (sequence, (base, count)) in &world.seen.acked {
                assert_eq!(
                    (*base, *count),
                    (u64::from(*sequence), 1),
                    "in order, each once"
                );
            }
            // The producer's session carried on: its newest batch sent again is recognised.
            let again = world.seen.again;
            let last = world.sent - 1;
            let bytes = sized_batch(&mut world.builder, last, world.payload);
            world.unacked.insert(last, bytes);
            let mut wait = ProgressDeadline::begin(&[0], u64::MAX, FROZEN);
            while world.seen.again == again {
                world.publish(world.sent);
                world.step().await;
                wait.check(&[world.seen.again])
                    .expect("the batch sent again was not recognised");
            }
            // Every node's stream comes to reach every batch.
            let mut wait = ProgressDeadline::begin(&ends(&world), u64::MAX, FROZEN);
            while ends(&world).iter().any(|end| *end < u64::from(until)) {
                world.step().await;
                if let Err(frozen) = wait.check(&ends(&world)) {
                    panic!(
                        "a node never reached every batch: {frozen:?}\n{}",
                        world.diagnose()
                    );
                }
            }
            // A node down: an image in the clear after its mark, a sealed image changed, and
            // another node's image are each refused at its open.
            let id = NODES[0];
            world.nodes[0] = None;
            let (data, keys) = node_files(dir.path(), id);
            let image = data.join("part.image");
            let sealed = std::fs::read(&image).unwrap();
            let group = keys.key(focal_seal::Store::Group).unwrap();
            let clear = focal_seal::file::open(&group, sealed.clone(), 64 << 20).unwrap();
            std::fs::write(&image, &*clear).unwrap();
            assert!(open_member(id, dir.path(), &world.addresses, &[], MEMORY).is_err());
            let mut flipped = sealed.clone();
            let at = flipped.len() / 2;
            flipped[at] ^= 0x40;
            std::fs::write(&image, &flipped).unwrap();
            assert!(open_member(id, dir.path(), &world.addresses, &[], MEMORY).is_err());
            std::fs::copy(
                node_files(dir.path(), NODES[1]).0.join("part.image"),
                &image,
            )
            .unwrap();
            assert!(open_member(id, dir.path(), &world.addresses, &[], MEMORY).is_err());
            std::fs::write(&image, &sealed).unwrap();
            assert!(open_member(id, dir.path(), &world.addresses, &[], MEMORY).is_ok());
            // The identity directory: a key planted in the clear after its mark, a sealed key
            // changed, and another directory's key are each refused as a node reads it.
            let key = pki.join("node-2.key");
            let sealed = std::fs::read(&key).unwrap();
            let refused =
                |pki: &Path| focal_bus::pki::load_or_make(pki, None, NODES.len(), 1).is_err();
            write_owner_only(&key, &ids.nodes[1].1);
            assert!(refused(&pki), "a key planted in the clear was read");
            let mut flipped = sealed.clone();
            let at = flipped.len() / 2;
            flipped[at] ^= 0x40;
            write_owner_only(&key, &flipped);
            assert!(refused(&pki), "a changed key was read");
            let other = dir.path().join("other-pki");
            focal_bus::pki::load_or_make(&other, None, NODES.len(), 1).unwrap();
            write_owner_only(&key, &std::fs::read(other.join("node-2.key")).unwrap());
            assert!(refused(&pki), "another directory's key was read");
            write_owner_only(&key, &sealed);
            assert!(!refused(&pki));
        })
        .unwrap();
}
