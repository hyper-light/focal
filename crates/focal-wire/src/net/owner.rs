//! The network owner: one hyper-rt task per endpoint (27 §15). It holds the endpoint's driver
//! (`hyper_rt_transport::Driver`) and everything that moves on it — the dials and their greetings,
//! the exchanges this side asks and the ones it serves — and is reached through bounded channels:
//! a [`Network`] handle sends it [`Command`]s and awaits their answers on one-shots. Nothing is
//! shared with it and nothing is locked; it never blocks, and every table it keeps is bounded by
//! the endpoint's own (`hyper_transport::Limits`), refused typed past it.
//!
//! A request a peer sends is read whole into a reservation of the node's budget and handed, with
//! what its sender authenticated as, to a task of its own on the owner's shard ([`Serve`]); the
//! task's answer comes back on the owner's own queue and is written as the exchange's reply. The
//! owner ends, closing its connections, once every handle to it is gone: nothing else holds its
//! queue of commands open (a registry wakes it through a watched word, its tasks answer on a queue
//! of its own). A request is
//! admitted against its sender's share of the ingress before any of its body is reserved (the
//! audit's F03): the lane's bytes among the peers with a connection up, one frame at least.

use std::collections::BTreeMap;
use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;
use std::time::{Duration, Instant};

use hyper_rt::combine::{Either3, race3};
use hyper_rt::sync::{
    ChannelReceiver, OneshotSender, Sender as CommandSender, WatchReceiver, channel, oneshot,
};
use hyper_rt::{RtError, TaskId};
use hyper_transport::tls::Tls;
use hyper_transport::{
    Classes, Epoch, Event, ExchangeId, PathFacts, PeerId, Progress, Refusal, Reservation,
};

use super::budget::{NetBudget, NetBudgetStats};
use super::classes::{FocalClasses, Kind, Sender};
use super::directory::{Inbound, NetDirectory};
use crate::{
    AccessError, Hello, HelloReply, Negotiated, TrafficClass, WireError, WireLimits,
    decode_payload, encode_payload,
};

/// The driver an owner holds.
pub(crate) type Driver = hyper_rt_transport::Driver<FocalClasses, NetBudget, NetDirectory>;

/// The most a greeting and its answer carry, bytes: focal-wire's greeting frame bound (sixteen
/// profiles of two bytes and two bounds, encoded, far within it).
pub const HELLO_BYTES: u32 = 4096;

/// What a served request's handler is told: what was asked, by whom, on what profile.
pub struct Served {
    /// The request's kind.
    pub kind: Kind,
    /// Its class, as its kind and its sender's role gave it.
    pub class: TrafficClass,
    /// Its body, read whole and verified.
    pub body: Vec<u8>,
    /// What its sender authenticated as.
    pub from: Inbound,
    /// The profile its connection negotiated, once its greeting was answered.
    pub negotiated: Option<Negotiated>,
    /// The round trip its connection measures now.
    pub path: Duration,
}

/// A served request's answer: the reply, or `None` to refuse the exchange (its stream is reset,
/// which its asker reads as the exchange refused by the peer).
pub type ServeFuture = Pin<Box<dyn Future<Output = Option<Reply>> + 'static>>;

/// A served request's reply: its body, and the charge its handler made for it, kept until the
/// endpoint has taken the body's last byte or the exchange ends. A reply whose peer reads it
/// slowly stays charged while the owner holds it.
pub struct Reply {
    pub body: Vec<u8>,
    pub charge: Option<crate::ResponseCharge>,
}
impl Reply {
    /// A reply whose body is all it holds.
    pub fn uncharged(body: Vec<u8>) -> Self {
        Self { body, charge: None }
    }
}

/// What an endpoint serves: its peers' greetings and requests. Installed on a listening endpoint's
/// owner once ([`Command::Serve`]); until then every request is refused.
pub trait Serve: Send + 'static {
    /// The profiles this side answers a greeting with ([`WireLimits::negotiate_ordered`]'s
    /// capabilities: managed, participant, native, ordered).
    fn capabilities(&self) -> [bool; 4];
    /// The answer to one request, on a task of its own.
    fn serve(&self, request: Served) -> ServeFuture;
}

/// A connection this side dialed and greeted.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Linked {
    pub(crate) peer: PeerId,
    pub(crate) epoch: Epoch,
    pub(crate) negotiated: Negotiated,
}

/// A request's answer, and what the path to its peer measured when it came.
#[derive(Debug)]
pub(crate) struct Answer {
    pub(crate) body: Vec<u8>,
    pub(crate) path: Option<PathFacts>,
}

/// Why an exchange this side asked ended without an answer, and whether its connection is gone.
#[derive(Debug)]
pub(crate) struct Failed {
    pub(crate) error: WireError,
    pub(crate) closed: bool,
}

/// What an owner holds and what it refused.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct NetStats {
    /// The endpoint's own (`hyper_transport::Stats`).
    pub endpoint: hyper_transport::Stats,
    /// The budget it holds.
    pub budget: NetBudgetStats,
    /// Requests refused for their sender's share of the ingress.
    pub refused_ingress: u64,
    /// Requests refused because their task could not be spawned.
    pub refused_tasks: u64,
    /// Requests refused before their greeting, or past their negotiated bound.
    pub refused_unnegotiated: u64,
    /// Exchanges asked and answered.
    pub answered: u64,
    /// Dials started: a route's first, and each after its connection went. A caller that joins a
    /// dial in flight, or finds the route's connection up, starts none.
    pub dials: u64,
}

/// What a handle asks of the owner.
pub(crate) enum Command {
    /// Dial `name` at `address` (its certificate pinned to `pin` when given), greet it offering
    /// `offers`, and answer once the connection is greeted or failed. A route dialed before is
    /// the same peer: a greeted connection to it answers at once, one being dialed answers when
    /// it does.
    Connect {
        address: SocketAddr,
        name: String,
        pin: Option<[u8; 32]>,
        offers: Vec<u16>,
        reply: OneshotSender<Result<Linked, WireError>>,
    },
    /// Ask `peer`, on the connection of `epoch`, a request of `kind` whose body is `body`, its
    /// peer given `period` to answer once it has it.
    Ask {
        peer: PeerId,
        epoch: Epoch,
        kind: Kind,
        body: Vec<u8>,
        period: Duration,
        reply: OneshotSender<Result<Answer, Failed>>,
    },
    /// Close every connection to `peer`.
    Disconnect { peer: PeerId },
    /// Run the connections made from now on under `tls`.
    SetTls {
        tls: Box<Tls>,
        reply: OneshotSender<Result<(), WireError>>,
    },
    /// What the owner holds.
    Stats { reply: OneshotSender<NetStats> },
    /// Serve the endpoint's peers with `serve`, and tell `done` when the owner ends.
    Serve {
        serve: Box<dyn Serve>,
        done: OneshotSender<()>,
    },
    /// What the path to `peer` measures now.
    Path {
        peer: PeerId,
        reply: OneshotSender<Option<PathFacts>>,
    },
    /// Stop: every connection is closed and the owner ends.
    Close,
}

/// A handle to an endpoint's owner.
#[derive(Clone)]
pub(crate) struct Network {
    commands: CommandSender<Command>,
}

impl Network {
    /// Starts the owner of `driver` on the current shard, negotiating under `limits`; commands wait
    /// in a queue of `queue`, past which a handle's send waits its turn, and the answers of the
    /// requests it serves in one of their own as long. `revocations` is the registry's revision,
    /// watched, for a listener: the owner closes the connections a withdrawal no longer grants as
    /// it moves. The owner serves nothing until it is given what to serve ([`Command::Serve`]).
    pub(crate) fn start(
        driver: Driver,
        limits: WireLimits,
        queue: usize,
        revocations: Option<WatchReceiver>,
    ) -> Result<Self, WireError> {
        let (commands, receiver) = channel(queue.max(1)).map_err(|_| WireError::Connection)?;
        let (answers, answered) = channel(queue.max(1)).map_err(|_| WireError::Connection)?;
        let owner = Owner::new(driver, limits, answers);
        hyper_rt::futures::spawn_detached(owner.run(receiver, answered, revocations))
            .map_err(|_| WireError::Connection)?;
        Ok(Self { commands })
    }

    /// Sends `command`, waiting its turn on the queue: `Connection` when the owner has ended, or
    /// when this is not a shard's task (a wait it cannot keep: refused before anything is queued).
    pub(crate) async fn send(&self, command: Command) -> Result<(), WireError> {
        crate::require_runtime()?;
        self.commands
            .send(command)
            .await
            .map_err(|_| WireError::Connection)
    }

    /// Sends `command` without waiting: `Limit` when the queue is full, `Connection` when the
    /// owner has ended.
    pub(crate) fn try_send(&self, command: Command) -> Result<(), WireError> {
        self.commands
            .try_send(command)
            .map_err(|error| match error {
                hyper_rt::sync::SyncError::Full(_) => WireError::Limit,
                _ => WireError::Connection,
            })
    }

    /// Asks the owner with the command `make` builds around a one-shot, and awaits its answer.
    pub(crate) async fn call<T: 'static>(
        &self,
        make: impl FnOnce(OneshotSender<T>) -> Command,
    ) -> Result<T, WireError> {
        let (sender, receiver) = oneshot().map_err(|_| WireError::Connection)?;
        self.send(make(sender)).await?;
        receiver.await.map_err(|_| WireError::Connection)
    }
}

/// A peer of the endpoint, as the owner keeps it.
struct PeerState {
    /// Its connection now, while one is up.
    epoch: Option<Epoch>,
    /// What its connection negotiated: this side's answer to its greeting, or the answer to this
    /// side's.
    negotiated: Option<Negotiated>,
    /// This side dialed it: the route it was dialed for, the profiles it is offered, its greeting
    /// in flight, and who waits for the connection.
    dialed: Option<DialState>,
}

struct DialState {
    route: (SocketAddr, String),
    offers: Vec<u16>,
    /// The endpoint is dialing it: from the dial's start to its connection up or its failure.
    dialing: bool,
    hello: Option<ExchangeId>,
    waiting: Vec<OneshotSender<Result<Linked, WireError>>>,
}

/// An exchange this side asked.
struct Asking {
    peer: PeerId,
    class: TrafficClass,
    body: Vec<u8>,
    written: usize,
    /// The answer's body, as it is read.
    reading: Option<Reservation>,
    /// Who waits for the answer; `None` for this side's own greeting.
    reply: Option<OneshotSender<Result<Answer, Failed>>>,
}

/// An exchange a peer asked.
struct Serving {
    peer: PeerId,
    kind: Kind,
    class: TrafficClass,
    /// The request's body, as it is read, and the bytes of the sender's share it holds.
    reading: Option<Reservation>,
    share: u64,
    /// The task answering it.
    task: Option<TaskId>,
    /// The answer, as it is written: its charge held until its last byte is taken.
    answer: Option<(Reply, usize)>,
    /// The request read whole before the endpoint was given what to serve: it waits for it.
    parked: Option<Vec<u8>>,
}

struct Owner {
    driver: Driver,
    /// The last instant the shard's clock gave: what an endpoint call is stamped with should the
    /// clock be unreadable (an owner whose shard is ending).
    last: Instant,
    limits: WireLimits,
    serve: Option<Box<dyn Serve>>,
    /// Who waits for the owner to end.
    done: Vec<OneshotSender<()>>,
    /// Where the tasks serving requests send their answers.
    answers: CommandSender<(ExchangeId, Option<Reply>)>,
    peers: BTreeMap<PeerId, PeerState>,
    routes: BTreeMap<(SocketAddr, String), PeerId>,
    asking: BTreeMap<ExchangeId, Asking>,
    serving: BTreeMap<ExchangeId, Serving>,
    /// Each peer's share of the ingress in use, by lane: ordinary and completion.
    ingress: BTreeMap<PeerId, [u64; 2]>,
    stats: NetStats,
    closing: bool,
    /// The registry revision last acted on: a withdrawal since closes what it no longer grants.
    revision: Option<u64>,
}

/// The lane a class's bodies are funded from: 1 for the completion lane, 0 for the ordinary.
fn lane_of(class: TrafficClass) -> usize {
    usize::from(class == TrafficClass::Control)
}

/// The error a refusal of an exchange this side asked is.
fn refused(refusal: Refusal) -> WireError {
    match refusal {
        Refusal::Stalled => WireError::Timeout,
        Refusal::FrameBound => WireError::Limit,
        Refusal::Corrupt => WireError::InvalidFrame,
        Refusal::Identity => WireError::Authentication,
        Refusal::Kind => WireError::Access(AccessError::Unauthorized),
        Refusal::Budget
        | Refusal::Exchanges
        | Refusal::Lanes
        | Refusal::LaneFull
        | Refusal::Pending
        | Refusal::Identities
        | Refusal::Connections
        | Refusal::Busy
        | Refusal::Events => WireError::Access(AccessError::Capacity),
        Refusal::Closed
        | Refusal::NotConnected
        | Refusal::UnknownExchange
        | Refusal::Order
        | Refusal::Exhausted
        | Refusal::Quic
        | Refusal::Configuration
        | Refusal::Unknown => WireError::Connection,
    }
}

/// The error a dial that failed, or a connection closed before its greeting, ends in.
fn unreachable(refusal: Option<Refusal>) -> WireError {
    match refusal {
        Some(Refusal::Identity) => WireError::Authentication,
        Some(refusal) => match refused(refusal) {
            WireError::Access(error) => WireError::Access(error),
            _ => WireError::Connection,
        },
        None => WireError::Connection,
    }
}

impl Owner {
    fn new(
        driver: Driver,
        limits: WireLimits,
        answers: CommandSender<(ExchangeId, Option<Reply>)>,
    ) -> Self {
        let last = driver.clock().unwrap_or_else(|_| Instant::now());
        Self {
            driver,
            last,
            limits,
            serve: None,
            done: Vec::new(),
            answers,
            peers: BTreeMap::new(),
            routes: BTreeMap::new(),
            asking: BTreeMap::new(),
            serving: BTreeMap::new(),
            ingress: BTreeMap::new(),
            stats: NetStats::default(),
            closing: false,
            revision: None,
        }
    }

    fn now(&mut self) -> Instant {
        // The shard's clock is the endpoint's; one that cannot be read is an owner that is ending.
        if let Ok(now) = self.driver.clock() {
            self.last = now;
        }
        self.last
    }

    async fn run(
        mut self,
        mut commands: ChannelReceiver<Command>,
        mut answers: ChannelReceiver<(ExchangeId, Option<Reply>)>,
        mut revocations: Option<WatchReceiver>,
    ) {
        // Whether a handle may still send: once every one is gone the queue is read no more.
        let mut asked = true;
        loop {
            // What the endpoint has first, then the answers of this owner's own tasks, then what
            // it is asked, then the registry's word.
            let others = async {
                let commands = async {
                    if asked {
                        Some(commands.recv().await)
                    } else {
                        std::future::pending().await
                    }
                };
                let revoked = async {
                    match revocations.as_mut() {
                        Some(watched) => watched.changed().await.is_ok(),
                        None => std::future::pending().await,
                    }
                };
                hyper_rt::combine::race2(commands, revoked).await
            };
            let next = race3(self.driver.event(), answers.recv(), others).await;
            match next {
                Either3::First(Ok(event)) => self.on_event(event),
                Either3::First(Err(_)) => break,
                Either3::Second(Ok((exchange, reply))) => self.answer(exchange, reply),
                // The owner holds a sender of its own: never closed while it runs.
                Either3::Second(Err(_)) => break,
                Either3::Third(hyper_rt::combine::Either::First(Some(Ok(command)))) => {
                    self.on_command(command)
                }
                // Every handle is gone: nothing more will be asked.
                Either3::Third(hyper_rt::combine::Either::First(_)) => {
                    asked = false;
                    self.closing = true;
                }
                Either3::Third(hyper_rt::combine::Either::Second(true)) => self.revoked(),
                // The registry is gone: its revision is read as the owner moves, as before.
                Either3::Third(hyper_rt::combine::Either::Second(false)) => revocations = None,
            }
            // The rest of what is ready, a bounded turn of it, before the next wait.
            for _ in 0..hyper_rt_transport::TURNS {
                let Some(event) = self.driver.endpoint().poll_event() else {
                    break;
                };
                self.on_event(event);
            }
            // A withdrawal whose wake this owner missed (its queue was full) is seen here.
            let revision = self.driver.endpoint().directory().revision();
            if revision != self.revision {
                self.revision = revision;
                self.revoked();
            }
            let _ = self.driver.flush();
            if self.closing && self.asking.is_empty() && self.serving.is_empty() {
                break;
            }
        }
        self.shut();
    }

    /// Ends every exchange and connection, telling everyone who waits.
    fn shut(&mut self) {
        let now = self.now();
        for (_, asking) in std::mem::take(&mut self.asking) {
            if let Some(reply) = asking.reply {
                let _ = reply.send(Err(Failed {
                    error: WireError::Connection,
                    closed: true,
                }));
            }
            if let Some(reading) = asking.reading {
                self.driver.endpoint().release(reading);
            }
        }
        for (_, serving) in std::mem::take(&mut self.serving) {
            if let Some(task) = serving.task {
                let _ = hyper_rt::futures::cancel(task);
            }
            if let Some(reading) = serving.reading {
                self.driver.endpoint().release(reading);
            }
        }
        let peers: Vec<PeerId> = self.peers.keys().copied().collect();
        for peer in peers {
            self.fail_waiting(peer, WireError::Connection);
            self.driver.endpoint().disconnect(now, peer);
        }
        let _ = self.driver.flush();
        for done in self.done.drain(..) {
            let _ = done.send(());
        }
    }

    // ------------------------------------------------------------------------------- commands

    fn on_command(&mut self, command: Command) {
        match command {
            Command::Connect {
                address,
                name,
                pin,
                offers,
                reply,
            } => self.connect(address, name, pin, offers, reply),
            Command::Ask {
                peer,
                epoch,
                kind,
                body,
                period,
                reply,
            } => self.ask(peer, epoch, kind, body, period, reply),
            Command::Disconnect { peer } => {
                let now = self.now();
                self.driver.endpoint().disconnect(now, peer);
            }
            Command::SetTls { tls, reply } => {
                let set = self
                    .driver
                    .endpoint()
                    .set_tls(*tls)
                    .map_err(|_| WireError::Authentication);
                let _ = reply.send(set);
            }
            Command::Stats { reply } => {
                let stats = self.stats();
                let _ = reply.send(stats);
            }
            Command::Serve { serve, done } => {
                self.serve = Some(serve);
                // One serve a listener; a second waits for the same end.
                if self.done.len() < 2 {
                    self.done.push(done);
                }
                // What arrived before it was waiting for it.
                let parked: Vec<ExchangeId> = self
                    .serving
                    .iter()
                    .filter(|(_, serving)| serving.parked.is_some())
                    .map(|(exchange, _)| *exchange)
                    .collect();
                for exchange in parked {
                    let Some(serving) = self.serving.get_mut(&exchange) else {
                        continue;
                    };
                    let Some(body) = serving.parked.take() else {
                        continue;
                    };
                    let (peer, kind, class) = (serving.peer, serving.kind, serving.class);
                    self.handle(exchange, peer, kind, class, body);
                }
            }
            Command::Path { peer, reply } => {
                let facts = self.driver.endpoint().path(peer);
                let _ = reply.send(facts);
            }
            Command::Close => self.closing = true,
        }
    }

    fn stats(&mut self) -> NetStats {
        let endpoint = self.driver.endpoint();
        NetStats {
            endpoint: endpoint.stats(),
            budget: endpoint.budget().stats(),
            ..self.stats
        }
    }

    fn connect(
        &mut self,
        address: SocketAddr,
        name: String,
        pin: Option<[u8; 32]>,
        offers: Vec<u16>,
        reply: OneshotSender<Result<Linked, WireError>>,
    ) {
        if self.closing {
            let _ = reply.send(Err(WireError::Connection));
            return;
        }
        // A socket is of one family (hyper-rt's IPv6 sockets are IPv6-only): an address of the other
        // is never reached from it, and is refused now rather than dialed to its idle timeout.
        let family = self
            .driver
            .local_addr()
            .map(|local| local.is_ipv4() == address.is_ipv4());
        if family != Ok(true) {
            let _ = reply.send(Err(WireError::Connection));
            return;
        }
        let route = (address, name);
        if let Some(peer) = self.routes.get(&route).copied() {
            if self.peers.contains_key(&peer) {
                self.rejoin(peer, address, offers, reply);
                return;
            }
            self.routes.remove(&route);
        }
        let Some(peer) = self.driver.endpoint().directory_mut().dial(&route.1, pin) else {
            let _ = reply.send(Err(WireError::Access(AccessError::Capacity)));
            return;
        };
        self.routes
            .retain(|_, routed| self.peers.contains_key(routed));
        if self.routes.len() >= self.limits.max_connections.max(1) {
            let _ = reply.send(Err(WireError::Access(AccessError::Capacity)));
            self.driver.endpoint().directory_mut().forget(peer);
            return;
        }
        self.routes.insert(route.clone(), peer);
        self.peers.insert(
            peer,
            PeerState {
                epoch: None,
                negotiated: None,
                dialed: Some(DialState {
                    route,
                    offers,
                    dialing: false,
                    hello: None,
                    waiting: vec![reply],
                }),
            },
        );
        self.dial(peer, address);
    }

    /// Starts a dial of `peer` at `address`; a refused one fails who waits for it.
    fn dial(&mut self, peer: PeerId, address: SocketAddr) {
        let now = self.now();
        match self.driver.endpoint().connect(now, peer, address) {
            Ok(()) => {
                if let Some(dial) = self
                    .peers
                    .get_mut(&peer)
                    .and_then(|state| state.dialed.as_mut())
                {
                    dial.dialing = true;
                }
                self.stats.dials = self.stats.dials.saturating_add(1);
            }
            Err(refusal) => self.fail_waiting(peer, refused(refusal)),
        }
    }

    /// The dial of `peer` is over, its connection up or failed.
    fn dialed(&mut self, peer: PeerId) {
        if let Some(dial) = self
            .peers
            .get_mut(&peer)
            .and_then(|state| state.dialed.as_mut())
        {
            dial.dialing = false;
        }
    }

    /// A call for a route dialed before: the greeted connection, or a wait on its dial (the
    /// audit's F60: concurrent cold calls dial once). A route's waiters are bounded by the
    /// endpoint's connections, each its own caller's.
    fn rejoin(
        &mut self,
        peer: PeerId,
        address: SocketAddr,
        offers: Vec<u16>,
        reply: OneshotSender<Result<Linked, WireError>>,
    ) {
        let most = self.limits.max_connections.max(1);
        let Some(state) = self.peers.get_mut(&peer) else {
            let _ = reply.send(Err(WireError::Connection));
            return;
        };
        if let (Some(epoch), Some(negotiated)) = (state.epoch, state.negotiated) {
            let _ = reply.send(Ok(Linked {
                peer,
                epoch,
                negotiated,
            }));
            return;
        }
        let Some(dial) = state.dialed.as_mut() else {
            let _ = reply.send(Err(WireError::Connection));
            return;
        };
        if dial.waiting.len() >= most {
            let _ = reply.send(Err(WireError::Access(AccessError::Capacity)));
            return;
        }
        dial.offers = offers;
        dial.waiting.push(reply);
        // A dial in progress or a connection being greeted is joined; a route whose connection
        // went is dialed again, once.
        let idle = !dial.dialing && state.epoch.is_none();
        if idle {
            self.dial(peer, address);
        }
    }

    /// Everyone waiting for `peer`'s connection is told `error`.
    fn fail_waiting(&mut self, peer: PeerId, error: WireError) {
        let Some(dial) = self
            .peers
            .get_mut(&peer)
            .and_then(|state| state.dialed.as_mut())
        else {
            return;
        };
        for waiter in dial.waiting.drain(..) {
            let _ = waiter.send(Err(clone_error(&error)));
        }
    }

    fn ask(
        &mut self,
        peer: PeerId,
        epoch: Epoch,
        kind: Kind,
        body: Vec<u8>,
        period: Duration,
        reply: OneshotSender<Result<Answer, Failed>>,
    ) {
        let current = self.peers.get(&peer).and_then(|state| state.epoch);
        if current != Some(epoch) || self.closing {
            let _ = reply.send(Err(Failed {
                error: WireError::Connection,
                closed: true,
            }));
            return;
        }
        let Some(class) = FocalClasses::class_of(kind, Sender::Node) else {
            let _ = reply.send(Err(Failed {
                error: WireError::InvalidFrame,
                closed: false,
            }));
            return;
        };
        match self.open(peer, kind, class, body, period, Some(reply)) {
            Ok(_) => {}
            Err((refusal, reply)) => {
                if let Some(reply) = reply {
                    let _ = reply.send(Err(Failed {
                        error: refused(refusal),
                        closed: refusal == Refusal::NotConnected,
                    }));
                }
            }
        }
    }

    /// Opens an exchange asking `peer` `kind` with `body`, and writes as much of it as credit
    /// takes; the refusal, and who was to be answered, when it cannot be opened.
    #[allow(clippy::type_complexity)]
    fn open(
        &mut self,
        peer: PeerId,
        kind: Kind,
        class: TrafficClass,
        body: Vec<u8>,
        period: Duration,
        reply: Option<OneshotSender<Result<Answer, Failed>>>,
    ) -> Result<ExchangeId, (Refusal, Option<OneshotSender<Result<Answer, Failed>>>)> {
        let progress = match Progress::new(period) {
            Ok(progress) => progress,
            Err(refusal) => return Err((refusal, reply)),
        };
        let now = self.now();
        let length = u64::try_from(body.len()).unwrap_or(u64::MAX);
        let exchange =
            match self
                .driver
                .endpoint()
                .open(now, peer, kind, &[], Some(length), progress)
            {
                Ok(exchange) => exchange,
                Err(refusal) => return Err((refusal, reply)),
            };
        self.asking.insert(
            exchange,
            Asking {
                peer,
                class,
                body,
                written: 0,
                reading: None,
                reply,
            },
        );
        self.write_ask(exchange);
        Ok(exchange)
    }

    fn write_ask(&mut self, exchange: ExchangeId) {
        let Some(asking) = self.asking.get_mut(&exchange) else {
            return;
        };
        let rest = asking.body.get(asking.written..).unwrap_or(&[]);
        if rest.is_empty() {
            return;
        }
        match self.driver.endpoint().write_body(exchange, rest) {
            Ok(taken) => asking.written = asking.written.saturating_add(taken),
            Err(refusal) => self.fail_ask(exchange, refused(refusal), false),
        }
    }

    fn fail_ask(&mut self, exchange: ExchangeId, error: WireError, closed: bool) {
        let Some(asking) = self.asking.remove(&exchange) else {
            return;
        };
        if let Some(reading) = asking.reading {
            self.driver.endpoint().release(reading);
        }
        self.driver.endpoint().end(exchange);
        match asking.reply {
            Some(reply) => {
                let _ = reply.send(Err(Failed { error, closed }));
            }
            // This side's greeting: the connection's dial fails with it.
            None => {
                self.fail_waiting(asking.peer, error);
                if let Some(dial) = self
                    .peers
                    .get_mut(&asking.peer)
                    .and_then(|state| state.dialed.as_mut())
                {
                    dial.hello = None;
                }
                let now = self.now();
                self.driver.endpoint().disconnect(now, asking.peer);
            }
        }
    }

    fn answer(&mut self, exchange: ExchangeId, reply: Option<Reply>) {
        let Some(serving) = self.serving.get_mut(&exchange) else {
            // Refused while its task ran: nothing waits for the answer.
            return;
        };
        serving.task = None;
        let Some(reply) = reply else {
            self.end_serving(exchange);
            return;
        };
        let length = u64::try_from(reply.body.len()).unwrap_or(u64::MAX);
        if self
            .driver
            .endpoint()
            .reply(exchange, &[], Some(length))
            .is_err()
        {
            self.end_serving(exchange);
            return;
        }
        if let Some(serving) = self.serving.get_mut(&exchange) {
            serving.answer = Some((reply, 0));
        }
        self.write_answer(exchange);
    }

    fn write_answer(&mut self, exchange: ExchangeId) {
        let Some(serving) = self.serving.get_mut(&exchange) else {
            return;
        };
        let Some((reply, written)) = serving.answer.as_mut() else {
            return;
        };
        let rest = reply.body.get(*written..).unwrap_or(&[]);
        let done = match self.driver.endpoint().write_body(exchange, rest) {
            Ok(taken) => {
                *written = written.saturating_add(taken);
                *written >= reply.body.len()
            }
            Err(_) => true,
        };
        if done {
            self.end_serving(exchange);
        }
    }

    /// A served exchange ends: its share and its reservation go back, and its task is stopped.
    fn end_serving(&mut self, exchange: ExchangeId) {
        let Some(serving) = self.serving.remove(&exchange) else {
            return;
        };
        if let Some(task) = serving.task {
            let _ = hyper_rt::futures::cancel(task);
        }
        if let Some(reading) = serving.reading {
            self.driver.endpoint().release(reading);
        }
        self.give_back(serving.peer, serving.class, serving.share);
        self.driver.endpoint().end(exchange);
    }

    fn give_back(&mut self, peer: PeerId, class: TrafficClass, share: u64) {
        if let Some(held) = self
            .ingress
            .get_mut(&peer)
            .and_then(|lanes| lanes.get_mut(lane_of(class)))
        {
            *held = held.saturating_sub(share);
        }
        if self
            .ingress
            .get(&peer)
            .is_some_and(|lanes| lanes.iter().all(|held| *held == 0))
        {
            self.ingress.remove(&peer);
        }
    }

    /// Closes the connections of every peer whose grant was withdrawn, telling each it was refused
    /// for its identity: what the peer's dial reports as its certificate refused.
    fn revoked(&mut self) {
        let now = self.now();
        let revoked = self.driver.endpoint().directory().revoked();
        for peer in revoked {
            self.driver.endpoint().refuse(now, peer, Refusal::Identity);
        }
    }

    // --------------------------------------------------------------------------------- events

    fn on_event(&mut self, event: Event<FocalClasses>) {
        match event {
            Event::Connected { peer, role, epoch } => self.connected(peer, role, epoch),
            Event::Closed {
                peer,
                epoch,
                refusal,
            } => self.closed(peer, epoch, refusal),
            Event::Unreachable { peer, refusal } => self.unreached(peer, refusal),
            Event::Request {
                exchange,
                peer,
                kind,
                class,
                body,
            } => self.request(exchange, peer, kind, class, body),
            Event::Reply { exchange, body } => self.reply(exchange, body),
            Event::BodyReady { exchange } => {
                if self.asking.contains_key(&exchange) {
                    self.read_answer(exchange);
                } else {
                    self.read_request(exchange);
                }
            }
            Event::Writable { exchange } => {
                if self.asking.contains_key(&exchange) {
                    self.write_ask(exchange);
                } else {
                    self.write_answer(exchange);
                }
            }
            Event::Frame { frame, .. } => self.driver.endpoint().release(frame),
            Event::Refused {
                exchange, refusal, ..
            } => {
                if self.asking.contains_key(&exchange) {
                    let closed = refusal == Refusal::Closed;
                    // The exchange is gone: nothing of it is ended again.
                    if let Some(asking) = self.asking.remove(&exchange) {
                        if let Some(reading) = asking.reading {
                            self.driver.endpoint().release(reading);
                        }
                        match asking.reply {
                            Some(reply) => {
                                let _ = reply.send(Err(Failed {
                                    error: refused(refusal),
                                    closed,
                                }));
                            }
                            // This side's greeting. Refused with its connection gone, the
                            // lifecycle event that follows says why the connection went (its
                            // certificate refused by the listener, RFC 8446 §4.4.4, or the
                            // listener full) and fails the dial with that; refused on a
                            // connection still up, the dial fails with the refusal, and the
                            // connection, which no greeting will now negotiate, is closed.
                            None => {
                                if let Some(dial) = self
                                    .peers
                                    .get_mut(&asking.peer)
                                    .and_then(|state| state.dialed.as_mut())
                                {
                                    dial.hello = None;
                                }
                                let gone =
                                    closed && self.driver.endpoint().path(asking.peer).is_none();
                                if !gone {
                                    self.fail_waiting(asking.peer, refused(refusal));
                                    let now = self.now();
                                    self.driver.endpoint().disconnect(now, asking.peer);
                                }
                            }
                        }
                    }
                } else if let Some(serving) = self.serving.remove(&exchange) {
                    if let Some(task) = serving.task {
                        let _ = hyper_rt::futures::cancel(task);
                    }
                    if let Some(reading) = serving.reading {
                        self.driver.endpoint().release(reading);
                    }
                    self.give_back(serving.peer, serving.class, serving.share);
                }
            }
        }
    }

    fn connected(&mut self, peer: PeerId, _role: Sender, epoch: Epoch) {
        self.dialed(peer);
        self.driver.endpoint().directory_mut().set_live(peer, true);
        let state = self.peers.entry(peer).or_insert(PeerState {
            epoch: None,
            negotiated: None,
            dialed: None,
        });
        state.epoch = Some(epoch);
        state.negotiated = None;
        let Some(dial) = state.dialed.as_mut() else {
            return;
        };
        let hello = Hello {
            versions: dial.offers.clone(),
            max_frame_bytes: self.limits.max_frame_bytes,
            max_items: self.limits.max_items,
        };
        let period = self.limits.request_timeout;
        let body = match encode_payload(&hello, HELLO_BYTES) {
            Ok(body) => body,
            Err(error) => {
                self.fail_waiting(peer, error);
                return;
            }
        };
        match self.open(peer, Kind::Hello, TrafficClass::Control, body, period, None) {
            Ok(exchange) => {
                if let Some(dial) = self
                    .peers
                    .get_mut(&peer)
                    .and_then(|state| state.dialed.as_mut())
                {
                    dial.hello = Some(exchange);
                }
            }
            Err((refusal, _)) => self.fail_waiting(peer, refused(refusal)),
        }
    }

    fn closed(&mut self, peer: PeerId, epoch: Epoch, refusal: Option<Refusal>) {
        let Some(state) = self.peers.get_mut(&peer) else {
            return;
        };
        if state.epoch != Some(epoch) {
            return;
        }
        state.epoch = None;
        state.negotiated = None;
        self.driver.endpoint().directory_mut().set_live(peer, false);
        // A dial whose connection closed before its greeting was answered: its certificate
        // refused by the listener (RFC 8446 §4.4.4), or the listener full.
        self.fail_waiting(peer, unreachable(refusal));
        let inbound = self
            .peers
            .get(&peer)
            .is_some_and(|state| state.dialed.is_none());
        if inbound {
            self.peers.remove(&peer);
        }
    }

    fn unreached(&mut self, peer: PeerId, refusal: Option<Refusal>) {
        self.dialed(peer);
        let unpinned = self.driver.endpoint().directory_mut().take_unpinned(peer);
        let error = if unpinned {
            WireError::Authentication
        } else {
            unreachable(refusal)
        };
        self.fail_waiting(peer, error);
        // A route that was never reached is forgotten, so its next dial starts afresh.
        let never = self
            .peers
            .get(&peer)
            .is_some_and(|state| state.epoch.is_none() && state.negotiated.is_none());
        if never && let Some(state) = self.peers.remove(&peer) {
            if let Some(dial) = state.dialed {
                self.routes.remove(&dial.route);
            }
            self.driver.endpoint().directory_mut().forget(peer);
        }
    }

    /// A peer's share of a lane's bytes: the lane's capacity among the peers with a connection
    /// up, and one negotiated frame at least.
    fn share(&mut self, class: TrafficClass, frame: u64) -> u64 {
        let live = self
            .peers
            .values()
            .filter(|state| state.epoch.is_some())
            .count();
        self.driver
            .endpoint()
            .budget()
            .capacity(class)
            .checked_div(u64::try_from(live.max(1)).unwrap_or(u64::MAX))
            .unwrap_or(0)
            .max(frame)
    }

    fn request(
        &mut self,
        exchange: ExchangeId,
        peer: PeerId,
        kind: Kind,
        class: TrafficClass,
        body: Option<u64>,
    ) {
        let negotiated = self.peers.get(&peer).and_then(|state| state.negotiated);
        // What the body may be: a greeting's bound, an enrollment message's, or the profile the
        // connection negotiated; a request before its connection's greeting is refused.
        let bound = match kind {
            Kind::Hello => u64::from(HELLO_BYTES),
            Kind::Enrollment(_) => u64::from(self.limits.max_frame_bytes),
            Kind::Operation(_) => match negotiated {
                Some(negotiated) => u64::from(negotiated.max_frame_bytes),
                None => {
                    self.stats.refused_unnegotiated =
                        self.stats.refused_unnegotiated.saturating_add(1);
                    self.driver
                        .endpoint()
                        .refuse_exchange(exchange, Refusal::Kind);
                    return;
                }
            },
        };
        let Some(length) = body.filter(|length| *length <= bound) else {
            self.stats.refused_unnegotiated = self.stats.refused_unnegotiated.saturating_add(1);
            self.driver
                .endpoint()
                .refuse_exchange(exchange, Refusal::FrameBound);
            return;
        };
        // The sender's share of the ingress, before any of the body is reserved (F03).
        let share = self.share(class, bound);
        let held = self
            .ingress
            .get(&peer)
            .and_then(|lanes| lanes.get(lane_of(class)))
            .copied()
            .unwrap_or(0);
        // Refused past it typed, as the budget's own refusal is: the sender reads the peer's
        // capacity, never its connection failing.
        if held.saturating_add(length) > share {
            self.stats.refused_ingress = self.stats.refused_ingress.saturating_add(1);
            self.driver
                .endpoint()
                .refuse_exchange(exchange, Refusal::Budget);
            return;
        }
        let reading = match self.driver.endpoint().reserve(class, length) {
            Ok(reading) => reading,
            Err(refusal) => {
                self.driver.endpoint().refuse_exchange(exchange, refusal);
                return;
            }
        };
        if let Some(lane) = self
            .ingress
            .entry(peer)
            .or_insert([0; 2])
            .get_mut(lane_of(class))
        {
            *lane = lane.saturating_add(length);
        }
        self.serving.insert(
            exchange,
            Serving {
                peer,
                kind,
                class,
                reading: Some(reading),
                share: length,
                task: None,
                answer: None,
                parked: None,
            },
        );
        self.read_request(exchange);
    }

    fn read_request(&mut self, exchange: ExchangeId) {
        let Some(serving) = self.serving.get_mut(&exchange) else {
            return;
        };
        let Some(reading) = serving.reading.as_mut() else {
            return;
        };
        if self.driver.endpoint().read_body(exchange, reading).is_err() {
            self.end_serving(exchange);
            return;
        }
        if !self.driver.endpoint().body_complete(exchange) {
            return;
        }
        let Some(serving) = self.serving.get_mut(&exchange) else {
            return;
        };
        let Some(reading) = serving.reading.take() else {
            return;
        };
        let (peer, kind, class) = (serving.peer, serving.kind, serving.class);
        // The body leaves the reservation for its handler; its bytes go back to the budget, and
        // the sender's share stays held until the exchange ends.
        let granted = reading.granted();
        let tag = reading.tag();
        let body = reading.into_buffer();
        self.driver
            .endpoint()
            .release(Reservation::tagged(granted, Vec::new(), tag));
        if self.serve.is_none() {
            // Bounded by the exchange table: each waits in its own exchange's slot.
            if let Some(serving) = self.serving.get_mut(&exchange) {
                serving.parked = Some(body);
            }
            return;
        }
        self.handle(exchange, peer, kind, class, body);
    }

    /// A request read whole: a greeting is answered here, the rest go to their tasks.
    fn handle(
        &mut self,
        exchange: ExchangeId,
        peer: PeerId,
        kind: Kind,
        class: TrafficClass,
        body: Vec<u8>,
    ) {
        match kind {
            Kind::Hello => self.greet(exchange, peer, &body),
            Kind::Operation(_) | Kind::Enrollment(_) => {
                self.dispatch(exchange, peer, kind, class, body)
            }
        }
    }

    /// Answers a peer's greeting with the profile both speak.
    fn greet(&mut self, exchange: ExchangeId, peer: PeerId, body: &[u8]) {
        let reply = match decode_payload::<Hello>(body) {
            Ok(hello) => {
                let [managed, participant, native, ordered] = self
                    .serve
                    .as_ref()
                    .map_or([false; 4], |serve| serve.capabilities());
                match self
                    .limits
                    .negotiate_ordered(&hello, managed, participant, native, ordered)
                {
                    Ok(negotiated) => {
                        if let Some(state) = self.peers.get_mut(&peer) {
                            state.negotiated = Some(negotiated);
                        }
                        HelloReply::Accepted(negotiated)
                    }
                    Err(error) => HelloReply::Rejected(error),
                }
            }
            Err(_) => HelloReply::Rejected(AccessError::InvalidRequest),
        };
        let body = encode_payload(&reply, HELLO_BYTES).ok();
        self.answer(exchange, body.map(Reply::uncharged));
    }

    /// Hands a request to a task of its own; its answer comes back as a command.
    fn dispatch(
        &mut self,
        exchange: ExchangeId,
        peer: PeerId,
        kind: Kind,
        class: TrafficClass,
        body: Vec<u8>,
    ) {
        let Some(from) = self.driver.endpoint().directory().inbound(peer).cloned() else {
            self.end_serving(exchange);
            return;
        };
        let path = self
            .driver
            .endpoint()
            .path(peer)
            .map_or(Duration::ZERO, |facts| facts.rtt);
        let negotiated = self.peers.get(&peer).and_then(|state| state.negotiated);
        let served = Served {
            kind,
            class,
            body,
            from,
            negotiated,
            path,
        };
        let Some(serve) = self.serve.as_ref() else {
            self.end_serving(exchange);
            return;
        };
        let work = serve.serve(served);
        let answers = self.answers.clone();
        let task = hyper_rt::futures::spawn_detached(async move {
            let reply = work.await;
            // The owner reads its queue until it ends; one that ended has nothing to write to.
            let _ = answers.send((exchange, reply)).await;
        });
        match task {
            Ok(task) => {
                if let Some(serving) = self.serving.get_mut(&exchange) {
                    serving.task = Some(task);
                }
            }
            Err(_) => {
                self.stats.refused_tasks = self.stats.refused_tasks.saturating_add(1);
                self.end_serving(exchange);
            }
        }
    }

    fn reply(&mut self, exchange: ExchangeId, body: Option<u64>) {
        let Some(asking) = self.asking.get(&exchange) else {
            self.driver.endpoint().end(exchange);
            return;
        };
        let class = asking.class;
        let bound = if asking.reply.is_none() {
            u64::from(HELLO_BYTES)
        } else {
            u64::from(self.limits.max_frame_bytes)
        };
        let Some(length) = body.filter(|length| *length <= bound) else {
            self.fail_ask(exchange, WireError::Limit, false);
            return;
        };
        let reading = match self.driver.endpoint().reserve(class, length) {
            Ok(reading) => reading,
            Err(refusal) => {
                self.fail_ask(exchange, refused(refusal), false);
                return;
            }
        };
        if let Some(asking) = self.asking.get_mut(&exchange) {
            asking.reading = Some(reading);
        }
        self.read_answer(exchange);
    }

    fn read_answer(&mut self, exchange: ExchangeId) {
        let Some(asking) = self.asking.get_mut(&exchange) else {
            return;
        };
        let Some(reading) = asking.reading.as_mut() else {
            return;
        };
        if let Err(refusal) = self.driver.endpoint().read_body(exchange, reading) {
            self.fail_ask(exchange, refused(refusal), false);
            return;
        }
        if !self.driver.endpoint().body_complete(exchange) {
            return;
        }
        let Some(asking) = self.asking.remove(&exchange) else {
            return;
        };
        let peer = asking.peer;
        let path = self.driver.endpoint().path(peer);
        self.driver.endpoint().end(exchange);
        let body = match asking.reading {
            Some(reading) => {
                let (granted, tag) = (reading.granted(), reading.tag());
                let body = reading.into_buffer();
                self.driver
                    .endpoint()
                    .release(Reservation::tagged(granted, Vec::new(), tag));
                body
            }
            None => Vec::new(),
        };
        match asking.reply {
            Some(reply) => {
                self.stats.answered = self.stats.answered.saturating_add(1);
                let _ = reply.send(Ok(Answer { body, path }));
            }
            None => self.greeted(peer, &body),
        }
    }

    /// This side's greeting was answered: the connection is linked, or refused.
    fn greeted(&mut self, peer: PeerId, body: &[u8]) {
        let Some(state) = self.peers.get_mut(&peer) else {
            return;
        };
        let Some(epoch) = state.epoch else {
            return;
        };
        let (max_frame_bytes, max_items) = (self.limits.max_frame_bytes, self.limits.max_items);
        let outcome = match decode_payload::<HelloReply>(body) {
            Ok(HelloReply::Accepted(negotiated))
                if negotiated.max_frame_bytes <= max_frame_bytes
                    && negotiated.max_items <= max_items
                    && state
                        .dialed
                        .as_ref()
                        .is_some_and(|dial| dial.offers.contains(&negotiated.protocol)) =>
            {
                Ok(negotiated)
            }
            Ok(HelloReply::Accepted(_)) | Err(_) => Err(WireError::InvalidFrame),
            Ok(HelloReply::Rejected(error)) => Err(WireError::Access(error)),
        };
        let Some(dial) = state.dialed.as_mut() else {
            return;
        };
        dial.hello = None;
        match outcome {
            Ok(negotiated) => {
                state.negotiated = Some(negotiated);
                let linked = Linked {
                    peer,
                    epoch,
                    negotiated,
                };
                for waiter in dial.waiting.drain(..) {
                    let _ = waiter.send(Ok(linked));
                }
            }
            Err(error) => {
                self.fail_waiting(peer, error);
                let now = self.now();
                self.driver.endpoint().disconnect(now, peer);
            }
        }
    }
}

/// A copy of `error` for each of several waiters: the variants waiters are told carry no
/// payload that cannot be copied; an I/O error is told as the connection failing.
pub(crate) fn clone_error(error: &WireError) -> WireError {
    match error {
        WireError::Io(_) => WireError::Connection,
        WireError::Limit => WireError::Limit,
        WireError::Allocation => WireError::Allocation,
        WireError::InvalidFrame => WireError::InvalidFrame,
        WireError::Timeout => WireError::Timeout,
        WireError::Authentication => WireError::Authentication,
        WireError::Connection => WireError::Connection,
        WireError::Access(access) => WireError::Access(access.clone()),
    }
}

/// An owner's failure to start: the runtime refused a channel or a task.
impl From<RtError> for WireError {
    fn from(_: RtError) -> Self {
        WireError::Connection
    }
}
