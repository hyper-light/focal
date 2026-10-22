//! A bus node's service: its endpoint driven on the caller's hyper-rt shard, its part, until an
//! interrupt or a termination stops it.
//!
//! The node runs to completion on its shard: a datagram is read, decrypted, its publish appended
//! and delivered to every subscriber's lane, and the datagrams that carry those deliveries are
//! sent, in one turn of the loop, with no handoff between threads (doc 32 D8; on one CPU every
//! handoff is a context switch).

use std::future::{Future, poll_fn};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::pin::pin;
use std::task::Poll;
use std::time::Instant;

use focal_memory::{BudgetKind, BudgetLane, MemoryBudget};
use hyper_rt::signal::{self, Signal};
use hyper_rt_transport::Driver;
use hyper_transport::tls::{CertificateDer, Credentials, PrivateKeyDer, PrivatePkcs8KeyDer, Tls};
use hyper_transport::{Config, Endpoint};

use crate::BusError;
use crate::budget::BusBudget;
use crate::directory::BusDirectory;
use crate::member::{Member, MemberSettings};
use crate::node::{Node, NodeConfig, NodeStats};
use crate::pki::{self, Local};
use crate::wire::{Bus, Role};

/// What a node's service runs with.
#[derive(Clone, Debug)]
pub struct ServeConfig {
    /// The address it listens on.
    pub listen: SocketAddr,
    /// The directory of its local identities (`pki`).
    pub pki: PathBuf,
    /// Client identities it admits.
    pub clients: usize,
    /// Its part's bounds.
    pub node: NodeConfig,
    /// The bytes the node may hold: its segments, tables and transport.
    pub memory_bytes: usize,
    /// The group the node's part is replicated by (doc 34), none for a lone node.
    pub group: Option<GroupServe>,
}

/// A node of a part's group: which node it is, the others, and where it keeps its log.
#[derive(Clone, Debug)]
pub struct GroupServe {
    /// This node (from 1), as the local directory names it.
    pub node: u64,
    /// Every other node of the group, and where it listens.
    pub peers: Vec<(u64, SocketAddr)>,
    /// The directory the node keeps its log, its part's image and its run in.
    pub data: PathBuf,
    /// The root key file the node's log's keys are kept under, outside `data`.
    pub key_file: PathBuf,
    /// Each node's fate, the same at every node; none for a group that commits on its disks alone.
    pub fates: Vec<(u64, u64)>,
    /// The distinct fates a commit on entries held in memory must span.
    pub spanning: usize,
}

/// The most client identities a node of `memory_bytes` admits: each holds its connections
/// ([`crate::limits::CONNECTIONS_PER_CLIENT`]), and each of those a receive window of at least the
/// transport's initial window (RFC 9002 §7.2 at QUIC's least datagram), so past this the node could
/// not open them all.
pub fn most_clients(memory_bytes: usize) -> usize {
    let per_client = usize::try_from(hyper_transport::initial_window(
        hyper_transport::MIN_DATAGRAM,
    ))
    .unwrap_or(usize::MAX)
    .saturating_mul(crate::limits::CONNECTIONS_PER_CLIENT)
    .max(1);
    memory_bytes.checked_div(per_client).unwrap_or(0)
}

fn credentials(local: &Local, node: u64) -> Result<Credentials, BusError> {
    let identity = local
        .node(node)
        .ok_or(BusError::Configuration("no identity for this node"))?;
    Ok(Credentials {
        chain: vec![CertificateDer::from(identity.certificate.clone())],
        key: PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(identity.key.clone())),
        roots: vec![CertificateDer::from(local.root.clone())],
    })
}

/// What a node did, as it stops: its own account, its memory, its transport's buffers, the rooms
/// its sockets told, and its shard's loop (none off a shard).
pub type Served = (
    NodeStats,
    focal_memory::BudgetStats,
    crate::budget::BusBudgetStats,
    hyper_rt_transport::Rooms,
    Option<hyper_rt::shard_loop::Counters>,
    // A node of a part's group: what its member did (its elections, its answers that waited).
    Option<crate::member::MemberStats>,
);

/// Runs a node until it is told to stop, calling `ready` with the address it listens on once it
/// does, and `tell` with each event of its group's member as it happens; what it did, when it
/// stops.
pub async fn serve(
    config: ServeConfig,
    ready: impl FnOnce(SocketAddr),
    tell: impl FnMut(&crate::member::GroupEvent),
) -> Result<Served, BusError> {
    if config.clients > most_clients(config.memory_bytes) {
        return Err(BusError::Configuration(
            "more client identities than the node's memory admits",
        ));
    }
    let (this, peers) = match &config.group {
        Some(group) => (group.node, group.peers.len()),
        None => (1, 0),
    };
    let local = pki::load_or_make(&config.pki, peers.saturating_add(1), config.clients)?;
    // A node of a part's group applies what its group committed within all of its memory, and
    // admits inputs within less (`limits::group_budget`).
    let budget = if config.group.is_some() {
        crate::limits::group_budget(config.memory_bytes)?
    } else {
        MemoryBudget::new(config.memory_bytes, 0).map_err(BusError::Memory)?
    };
    let mut directory = BusDirectory::new(config.clients.saturating_add(peers));
    for (index, client) in local.clients.iter().enumerate() {
        let peer = local
            .client_peer(index)
            .ok_or(BusError::Configuration("clients"))?;
        let name = format!("focal-bus-client-{}", index.saturating_add(1));
        directory.enroll(&client.certificate, peer, Role::Client, &name)?;
    }
    if let Some(group) = &config.group {
        for (peer, _) in &group.peers {
            let identity = local.node(*peer).ok_or(BusError::Configuration(
                "no identity for a node of the group",
            ))?;
            directory.enroll(
                &identity.certificate,
                *peer,
                Role::Node,
                &pki::node_name(*peer),
            )?;
        }
    }
    let tls = Tls::mutual(&credentials(&local, this)?, true).map_err(BusError::Transport)?;
    let limits = crate::limits::node(config.clients, peers)?;
    let lane_window = limits.lane_window;
    let endpoint_config = Config {
        tls,
        role: Role::Node,
        limits,
        congestion: Default::default(),
    };
    let endpoint: Endpoint<Bus, BusBudget, BusDirectory> = Endpoint::new(
        endpoint_config,
        BusBudget::new(budget.clone()),
        directory,
        Instant::now(),
    )
    .map_err(BusError::Transport)?;
    // What the driver holds for its sockets as it is made, charged for as long as it is bound:
    // its receive ring at one buffer and its listener's socket (`hyper_rt_transport::least_bytes`).
    // What the ring grows by as its drains need it, the driver charges to the endpoint's budget as
    // it grows (hyper-raft docs/transport.md §4aa).
    let _socket = budget
        .reserve(
            BudgetKind::Pending,
            BudgetLane::Ordinary,
            hyper_rt_transport::least_bytes(crate::limits::IO),
        )
        .map_err(BusError::Memory)?
        .commit();
    let mut driver = Driver::bind(endpoint, config.listen, crate::limits::IO)
        .map_err(|_| BusError::Configuration("the listen address could not be bound"))?;
    let address = driver
        .local_addr()
        .map_err(|_| BusError::Configuration("the listen address"))?;
    let mut node = Node::new(config.node, budget.clone())?;
    let signals = signal::subscribe(&[Signal::Interrupt, Signal::Terminate])
        .map_err(|_| BusError::Configuration("signals could not be subscribed"))?;
    if let Some(group) = config.group {
        let memory_bytes = u64::try_from(config.memory_bytes)
            .map_err(|_| BusError::Configuration("the node's memory"))?;
        let mut members: Vec<u64> = group.peers.iter().map(|(peer, _)| *peer).collect();
        members.push(group.node);
        members.sort_unstable();
        let now = driver
            .clock()
            .map_err(|_| BusError::Configuration("the shard's clock"))?;
        let member = Member::open(
            MemberSettings {
                id: group.node,
                peers: group.peers,
                data: group.data,
                key_file: group.key_file,
                memory_bytes,
                fates: group.fates,
                spanning: group.spanning,
            },
            node,
            members,
            config.clients,
            lane_window,
            budget,
            now,
        )?;
        ready(address);
        return serve_member(driver, member, signals, tell).await;
    }
    ready(address);
    // The transport's reservations given back as the node last looked: a delivery that found no
    // memory is asked again once some come back.
    let mut returns = driver.endpoint().budget().stats().returns;
    loop {
        let mut stop = pin!(signals.recv());
        let woke = poll_fn(|context| {
            if stop.as_mut().poll(context).is_ready() {
                return Poll::Ready(Woke::Stop);
            }
            if let Poll::Ready(event) = driver.poll_event(context) {
                return Poll::Ready(Woke::Event(event));
            }
            // A frame sent and answered gives its buffer back as the driver reads the answer, and
            // no event of the node's need follow: a starved delivery is asked again then.
            if node.starving() && driver.endpoint().budget().stats().returns != returns {
                return Poll::Ready(Woke::Freed);
            }
            Poll::Pending
        })
        .await;
        match woke {
            Woke::Stop => {
                return Ok((
                    node.stats(),
                    node.memory(),
                    driver.endpoint().budget().stats(),
                    driver.rooms(),
                    hyper_rt::futures::shard_counters(),
                    None,
                ));
            }
            Woke::Event(event) => {
                let event =
                    event.map_err(|_| BusError::Configuration("the endpoint's socket failed"))?;
                node.on_event(driver.endpoint(), event);
                while let Some(event) = driver.endpoint().poll_event() {
                    node.on_event(driver.endpoint(), event);
                }
            }
            Woke::Freed | Woke::Due => {}
        }
        returns = driver.endpoint().budget().stats().returns;
        node.relieve_transport(driver.endpoint());
    }
}

/// The loop of a node of a part's group (doc 34): the member's events, its turns and its waits, until
/// an interrupt or a termination. A wake that brings no event of the endpoint's, no signal and no
/// timer is the log's: a write the replica gave it is durable, and the turn's drive takes it.
async fn serve_member(
    mut driver: Driver<Bus, BusBudget, BusDirectory>,
    mut member: Member,
    signals: signal::SignalStream,
    mut tell: impl FnMut(&crate::member::GroupEvent),
) -> Result<Served, BusError> {
    let clock = |driver: &Driver<Bus, BusBudget, BusDirectory>| {
        driver
            .clock()
            .map_err(|_| BusError::Configuration("the shard's clock"))
    };
    let waker = poll_fn(|context| Poll::Ready(context.waker().clone())).await;
    let now = clock(&driver)?;
    member.connect(driver.endpoint(), now);
    let mut returns = driver.endpoint().budget().stats().returns;
    loop {
        let began = clock(&driver)?;
        let sleep_ns = member.deadline().map(|at| {
            u64::try_from(at.saturating_duration_since(began).as_nanos())
                .unwrap_or(u64::MAX)
                .max(1)
        });
        let mut sleep = pin!(sleep_ns.map(hyper_rt::futures::sleep));
        let mut stop = pin!(signals.recv());
        let mut first = true;
        let woke = poll_fn(|context| {
            if stop.as_mut().poll(context).is_ready() {
                return Poll::Ready(Woke::Stop);
            }
            if let Poll::Ready(event) = driver.poll_event(context) {
                return Poll::Ready(Woke::Event(event));
            }
            if let Some(sleep) = sleep.as_mut().as_pin_mut()
                && sleep.poll(context).is_ready()
            {
                return Poll::Ready(Woke::Due);
            }
            if member.node().starving() && driver.endpoint().budget().stats().returns != returns {
                return Poll::Ready(Woke::Freed);
            }
            // Polled again with nothing else ready: the log woke the task.
            if !std::mem::take(&mut first) {
                return Poll::Ready(Woke::Due);
            }
            Poll::Pending
        })
        .await;
        let now = clock(&driver)?;
        member.waited(began, now);
        match woke {
            Woke::Stop => {
                return Ok((
                    member.node().stats(),
                    member.node().memory(),
                    driver.endpoint().budget().stats(),
                    driver.rooms(),
                    hyper_rt::futures::shard_counters(),
                    Some(member.stats()),
                ));
            }
            Woke::Event(event) => {
                let event =
                    event.map_err(|_| BusError::Configuration("the endpoint's socket failed"))?;
                member.on_event(driver.endpoint(), event, now);
                while let Some(event) = driver.endpoint().poll_event() {
                    member.on_event(driver.endpoint(), event, now);
                }
            }
            Woke::Freed | Woke::Due => {}
        }
        member.turn(driver.endpoint(), &waker, now, &mut tell);
        if member.fenced() {
            return Err(BusError::Fenced);
        }
        returns = driver.endpoint().budget().stats().returns;
        member.node_mut().relieve_transport(driver.endpoint());
    }
}

/// What woke the node's loop.
enum Woke<E> {
    /// An interrupt or a termination.
    Stop,
    /// The endpoint's event.
    Event(E),
    /// The transport gave memory back while a delivery waited for some.
    Freed,
    /// A node of a group has work: its deadline came, or its log answered a write.
    Due,
}
