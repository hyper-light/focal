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

fn credentials(local: &Local) -> Credentials {
    Credentials {
        chain: vec![CertificateDer::from(local.node.certificate.clone())],
        key: PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(local.node.key.clone())),
        roots: vec![CertificateDer::from(local.root.clone())],
    }
}

/// What a node did, as it stops: its own account, its memory, its transport's buffers, the rooms
/// its sockets told, and its shard's loop (none off a shard).
pub type Served = (
    NodeStats,
    focal_memory::BudgetStats,
    crate::budget::BusBudgetStats,
    hyper_rt_transport::Rooms,
    Option<hyper_rt::shard_loop::Counters>,
);

/// Runs a node until it is told to stop, calling `ready` with the address it listens on once it
/// does; what it did, when it stops.
pub async fn serve(
    config: ServeConfig,
    ready: impl FnOnce(SocketAddr),
) -> Result<Served, BusError> {
    if config.clients > most_clients(config.memory_bytes) {
        return Err(BusError::Configuration(
            "more client identities than the node's memory admits",
        ));
    }
    let local = pki::load_or_make(&config.pki, config.clients)?;
    let budget = MemoryBudget::new(config.memory_bytes, 0).map_err(BusError::Memory)?;
    let mut directory = BusDirectory::new(config.clients);
    for (index, client) in local.clients.iter().enumerate() {
        let peer = Local::client_peer(index).ok_or(BusError::Configuration("clients"))?;
        let name = format!("focal-bus-client-{}", index.saturating_add(1));
        directory.enroll(&client.certificate, peer, Role::Client, &name)?;
    }
    let tls = Tls::mutual(&credentials(&local), true).map_err(BusError::Transport)?;
    let endpoint_config = Config {
        tls,
        role: Role::Node,
        limits: crate::limits::node(config.clients, 0)?,
        congestion: Default::default(),
    };
    let endpoint: Endpoint<Bus, BusBudget, BusDirectory> = Endpoint::new(
        endpoint_config,
        BusBudget::new(budget.clone()),
        directory,
        Instant::now(),
    )
    .map_err(BusError::Transport)?;
    // The socket's buffers, charged for as long as it is bound: its receive places and its outbox
    // (`Io::held_bound`).
    let _socket = budget
        .reserve(
            BudgetKind::Pending,
            BudgetLane::Ordinary,
            crate::limits::IO.held_bound(),
        )
        .map_err(BusError::Memory)?
        .commit();
    let mut driver = Driver::bind(endpoint, config.listen, crate::limits::IO)
        .map_err(|_| BusError::Configuration("the listen address could not be bound"))?;
    let address = driver
        .local_addr()
        .map_err(|_| BusError::Configuration("the listen address"))?;
    let mut node = Node::new(config.node, budget)?;
    let signals = signal::subscribe(&[Signal::Interrupt, Signal::Terminate])
        .map_err(|_| BusError::Configuration("signals could not be subscribed"))?;
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
            Woke::Freed => {}
        }
        returns = driver.endpoint().budget().stats().returns;
        node.relieve_transport(driver.endpoint());
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
}
