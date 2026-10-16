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

use std::collections::{BTreeMap, VecDeque};
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
    Classes, Endpoint, Epoch, Event, ExchangeId, PathFacts, PeerId, Progress, Refusal, Reservation,
};

use super::budget::{NetBudget, NetBudgetStats};
use super::classes::{FocalClasses, Kind, Sender};
use super::directory::{Inbound, LeafPins, NetDirectory};
use crate::{
    AccessError, Hello, HelloReply, Negotiated, TrafficClass, WireError, WireLimits,
    decode_payload, encode_payload,
};

/// The driver an owner holds.
pub(crate) type Driver = hyper_rt_transport::Driver<FocalClasses, NetBudget, NetDirectory>;

/// The waits for an owner's end it keeps: a server's serve, and a shutdown that awaits the end.
const ENDED_WAITERS: usize = 4;

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
    /// When its head arrived, on the shard's clock (nanoseconds): its answer's time is counted
    /// from then, however long it waited for its body and its task.
    pub arrived_ns: u64,
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

/// A frame a peer sent on one of its replication lanes (hyper-transport's lanes): one-way, in the
/// order its lane carries, answered by nothing ([`crate::LANED_PROTOCOL_VERSION`]).
pub struct Framed {
    /// The frame's kind.
    pub kind: Kind,
    /// Its class, as its kind and its sender's role gave it.
    pub class: TrafficClass,
    /// Its bytes, read whole.
    pub body: Vec<u8>,
    /// What its sender authenticated as.
    pub from: Inbound,
    /// The profile its sender's connection negotiated, once its greeting was answered.
    pub negotiated: Option<Negotiated>,
    /// The round trip its connection measures now.
    pub path: Duration,
}

/// What an endpoint serves: its peers' greetings, requests and lane frames. Installed on a
/// listening endpoint's owner once ([`Command::Serve`]); until then every request is refused and
/// every frame let go.
pub trait Serve: Send + 'static {
    /// The profiles this side answers a greeting with ([`WireLimits::negotiate_laned`]'s
    /// capabilities: managed, participant, native, ordered, laned).
    fn capabilities(&self) -> [bool; 5];
    /// The answer to one request, on a task of its own.
    fn serve(&self, request: Served) -> ServeFuture;
    /// One frame a peer sent on a lane, taken at once; whether it was. What serves no lanes
    /// lets it go (it answers no greeting with the laned profile, so no peer sends it one).
    fn frame(&self, framed: Framed) -> bool {
        let _ = framed;
        false
    }
}

/// What a frame queued on a lane is told: queued, with what its path measures then; or why not.
pub(crate) type LaneReply = OneshotSender<Result<Option<PathFacts>, WireError>>;

/// A frame that found its lane full: it waits, behind the frames of its lane that came before it,
/// until the lane has room (`Owner::flush_parked`), as an exchange waits for a stream; its sender
/// waits with it, and with it the frame's charge. Refused when its connection goes, when its
/// sender gives up waiting, or when as many wait as the lane itself holds.
struct Parked {
    epoch: Epoch,
    kind: Kind,
    body: Vec<u8>,
    reply: LaneReply,
}

/// A connection this side dialed: up, and greeted when its dial greets.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Linked {
    pub(crate) peer: PeerId,
    pub(crate) epoch: Epoch,
    /// The profile its greeting negotiated; `None` on a connection its dial does not greet.
    pub(crate) negotiated: Option<Negotiated>,
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
    /// Frames that found their lane full and waited for room on it.
    pub lane_frames_parked: u64,
    /// Frames queued on a peer's lane, frames a lane refused (full, or its connection gone), and
    /// frames taken from peers' lanes and handed to what serves them.
    pub lane_frames_sent: u64,
    pub lane_frames_refused: u64,
    pub lane_frames_received: u64,
    /// Frames from peers' lanes let go: nothing served them, their sender is not known, or what
    /// serves them refused them (no room, not a node's replication, not verified).
    pub lane_frames_dropped: u64,
    /// Dials started: a route's first, and each after its connection went. A caller that joins a
    /// dial in flight, or finds the route's connection up, starts none.
    pub dials: u64,
    /// What the endpoint's socket did: datagrams and the system calls that moved them, and
    /// whether the kernel segments its sends and coalesces its receives (`UDP_SEGMENT`,
    /// `UDP_GRO`).
    pub io: hyper_rt::udp::IoStats,
}

/// What a handle asks of the owner.
pub(crate) enum Command {
    /// Dial `name` at `address` (its leaf one `pin` accepts when given), greet it offering
    /// `offers`, and answer once the connection is greeted or failed. A route dialed before is
    /// the same peer: a greeted connection to it answers at once, one being dialed answers when
    /// it does.
    Connect {
        address: SocketAddr,
        name: String,
        pin: Option<LeafPins>,
        offers: Vec<u16>,
        /// A connection of the caller's alone: never a route's, never shared, forgotten as it
        /// closes (a probe of exactly an address, 24 §24).
        alone: bool,
        /// Greet it: the data protocol's connections are. One of the enrollment protocol, whose
        /// joiner may send the enrollment kinds alone, is answered as it comes up.
        greet: bool,
        reply: OneshotSender<Result<Linked, WireError>>,
    },
    /// Ask `peer`, on the connection of `epoch`, a request of `kind` whose body is `body`, its
    /// peer given `period` to answer once it has it.
    Ask {
        peer: PeerId,
        epoch: Epoch,
        kind: Kind,
        body: Asked,
        period: Duration,
        reply: OneshotSender<Result<Answer, Failed>>,
    },
    /// Queue `body`, a message of `kind`, on `peer`'s lane `lane` of the connection of `epoch`:
    /// after the lane's earlier frames, answered by nothing. Told once it is queued, with what the
    /// path to `peer` measures then, as an answer brings it; or why not. A lane that is full
    /// holds the frame until it has room (`Parked`).
    Lane {
        peer: PeerId,
        epoch: Epoch,
        lane: hyper_transport::LaneId,
        kind: Kind,
        body: Vec<u8>,
        reply: LaneReply,
    },
    /// Close every connection to `peer`.
    Disconnect { peer: PeerId },
    /// Size the stream pool the endpoint's participants share (hyper-transport's
    /// `Endpoint::set_stream_pool`).
    StreamPool { streams: u32 },
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
    /// Tell `done` when the owner has ended, its endpoint and socket gone.
    Ended { done: OneshotSender<()> },
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
    /// it moves. `enrollment` is the most a message of the enrollment protocol carries, for a
    /// listener that serves it; elsewhere its kinds are refused. The owner serves nothing until it
    /// is given what to serve ([`Command::Serve`]).
    /// The owner of `driver`, whose endpoint sends as `role` (what its identity is granted as).
    pub(crate) fn start(
        (driver, role): (Driver, Sender),
        limits: WireLimits,
        queue: usize,
        revocations: Option<WatchReceiver>,
        enrollment: Option<u32>,
    ) -> Result<Self, WireError> {
        let (commands, receiver) = channel(queue.max(1)).map_err(|_| WireError::Connection)?;
        let (answers, answered) = channel(queue.max(1)).map_err(|_| WireError::Connection)?;
        let owner = Owner::new((driver, role), limits, answers, enrollment);
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

    /// Queues `command` if the queue has room now: a test's way to fill it.
    #[cfg(test)]
    pub(crate) fn try_queue(&self, command: Command) -> bool {
        self.commands.try_send(command).is_ok()
    }

    /// Sends `command` without waiting for the queue: now when it has room, else by a task of its
    /// own on this shard that waits its turn (a plain thread waits for room itself). Refused only
    /// when the owner has ended (`Connection`) or neither can take it — the shard's tasks all
    /// taken (`Limit`): a command is never dropped unseen, as one a full queue refused was.
    pub(crate) fn send_now(&self, command: Command) -> Result<(), WireError> {
        match self.commands.try_send(command) {
            Ok(()) => Ok(()),
            Err(hyper_rt::sync::SyncError::Full(command)) => {
                if hyper_rt::futures::shard_id().is_some() {
                    let commands = self.commands.clone();
                    hyper_rt::futures::spawn_detached(async move {
                        // The owner reads its queue until it ends; one that ended has nothing to
                        // close.
                        let _ = commands.send(command).await;
                    })
                    .map(|_| ())
                    .map_err(|_| WireError::Limit)
                } else {
                    self.commands
                        .blocking_send(command)
                        .map_err(|_| WireError::Connection)
                }
            }
            Err(_) => Err(WireError::Connection),
        }
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
    /// Its connections up now, each with what its greeting negotiated (this side's answer to the
    /// peer's greeting, or the answer to this side's): the one a dial of this side's holds, or as
    /// many as the peer's identity is admitted (`AdmissionLimits`), each greeted on its own. The
    /// newest is the current one, which a dial links to.
    connections: BTreeMap<Epoch, Option<Negotiated>>,
    /// This side dialed it: the route it was dialed for, the profiles it is offered, its greeting
    /// in flight, and who waits for the connection.
    dialed: Option<DialState>,
}

struct DialState {
    route: (SocketAddr, String),
    offers: Vec<u16>,
    /// A connection of its caller's alone: in no route, forgotten as it closes.
    alone: bool,
    /// Its connection is greeted as it comes up.
    greet: bool,
    /// The endpoint is dialing it: from the dial's start to its connection up or its failure.
    dialing: bool,
    hello: Option<ExchangeId>,
    waiting: Vec<OneshotSender<Result<Linked, WireError>>>,
}

/// What an exchange this side asks sends: wiped when it goes if it is a message of the enrollment
/// protocol, which carries an invitation's secret (focal-enrollment's frames were `Zeroizing`),
/// however the exchange ends — answered, refused, or never opened.
pub(crate) struct Asked {
    bytes: Vec<u8>,
    secret: bool,
}
/// Lets go what `state` keeps for `peer`'s connections that are no longer up. Only a peer's current
/// connection's end is an event, so an older one's goes the next time the peer's connections change
/// (hyper-transport's `connection_live`): what is kept stays within the connections the peer's
/// identity is admitted, and one more.
fn keep_live(
    endpoint: &Endpoint<FocalClasses, NetBudget, NetDirectory>,
    peer: PeerId,
    state: &mut PeerState,
) {
    state
        .connections
        .retain(|epoch, _| endpoint.connection_live(peer, *epoch));
}

impl PeerState {
    /// The current connection: the newest up, and what it negotiated.
    fn current(&self) -> Option<(Epoch, Option<Negotiated>)> {
        self.connections
            .last_key_value()
            .map(|(epoch, negotiated)| (*epoch, *negotiated))
    }
    /// What the connection of `epoch` negotiated, once its greeting was answered.
    fn negotiated(&self, epoch: Epoch) -> Option<Negotiated> {
        self.connections.get(&epoch).copied().flatten()
    }
}

impl Asked {
    /// The body of a request of `kind`.
    pub(crate) fn of(kind: Kind, bytes: Vec<u8>) -> Self {
        Self {
            bytes,
            secret: matches!(kind, Kind::Enrollment(_)),
        }
    }
}
impl Drop for Asked {
    fn drop(&mut self) {
        if self.secret {
            zeroize::Zeroize::zeroize(&mut self.bytes);
        }
    }
}

/// An exchange this side asked.
struct Asking {
    peer: PeerId,
    class: TrafficClass,
    body: Asked,
    written: usize,
    /// The answer's body, as it is read.
    reading: Option<Reservation>,
    /// Who waits for the answer; `None` for this side's own greeting.
    reply: Option<OneshotSender<Result<Answer, Failed>>>,
}

/// An exchange a peer asked.
struct Serving {
    peer: PeerId,
    /// The connection the request came on.
    epoch: Epoch,
    /// When its head arrived, on the shard's clock.
    arrived_ns: u64,
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
    /// What the endpoint's identity is granted as: with a message's kind, the class it is asked in,
    /// as its peer gives it.
    role: Sender,
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
    /// Frames that wait for room on their lanes, by peer and lane, each lane's in the order they
    /// came (`Parked`): at most a lane's window of them a lane.
    parked: BTreeMap<(PeerId, hyper_transport::LaneId), VecDeque<Parked>>,
    closing: bool,
    /// The registry revision last acted on: a withdrawal since closes what it no longer grants.
    revision: Option<u64>,
    /// The most a message of the enrollment protocol carries, on a listener that serves it.
    enrollment: Option<u64>,
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
        // Never sent: its peer granted no stream for a period, so the request was not taken.
        Refusal::Unsent
        | Refusal::Budget
        | Refusal::Exchanges
        | Refusal::Lanes
        | Refusal::LaneFull
        | Refusal::Pending
        | Refusal::Identities
        | Refusal::Connections
        | Refusal::Busy
        | Refusal::ReceiveBuffer
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
        (driver, role): (Driver, Sender),
        limits: WireLimits,
        answers: CommandSender<(ExchangeId, Option<Reply>)>,
        enrollment: Option<u32>,
    ) -> Self {
        let last = driver.clock().unwrap_or_else(|_| Instant::now());
        Self {
            driver,
            role,
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
            parked: BTreeMap::new(),
            closing: false,
            revision: None,
            enrollment: enrollment.map(u64::from),
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
            // What waited for room on a lane takes what the turn freed.
            self.flush_parked();
            let _ = self.driver.flush();
            if self.closing && self.asking.is_empty() && self.serving.is_empty() {
                break;
            }
        }
        let done = self.shut();
        // The endpoint and its socket go before anyone waiting for the end hears of it: an address
        // the endpoint held is free again by then.
        drop(self);
        for done in done {
            let _ = done.send(());
        }
    }

    /// Ends every exchange and connection, telling everyone who waits for one; who waits for the
    /// owner's own end is handed back, to be told once it has gone.
    fn shut(&mut self) -> Vec<OneshotSender<()>> {
        let now = self.now();
        for (_, waiting) in std::mem::take(&mut self.parked) {
            for frame in waiting {
                let _ = frame.reply.send(Err(WireError::Connection));
            }
        }
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
        std::mem::take(&mut self.done)
    }

    // ------------------------------------------------------------------------------- commands

    /// `done` is told when the owner ends; past [`ENDED_WAITERS`] it is dropped now, which its
    /// waiter reads as the end it waits for.
    fn wait_for_end(&mut self, done: OneshotSender<()>) {
        if self.done.len() < ENDED_WAITERS {
            self.done.push(done);
        }
    }

    fn on_command(&mut self, command: Command) {
        match command {
            Command::Connect {
                address,
                name,
                pin,
                offers,
                alone,
                greet,
                reply,
            } => self.connect(address, name, pin, offers, alone, greet, reply),
            Command::Ask {
                peer,
                epoch,
                kind,
                body,
                period,
                reply,
            } => self.ask(peer, epoch, kind, body, period, reply),
            Command::Lane {
                peer,
                epoch,
                lane,
                kind,
                body,
                reply,
            } => self.lane(
                peer,
                Parked {
                    epoch,
                    kind,
                    body,
                    reply,
                },
                lane,
            ),
            Command::Disconnect { peer } => {
                let now = self.now();
                self.driver.endpoint().disconnect(now, peer);
            }
            Command::StreamPool { streams } => {
                // Refused only under fixed credit, which focal's endpoints never run.
                let _ = self.driver.endpoint().set_stream_pool(streams);
            }
            Command::SetTls { tls, reply } => {
                let set = self
                    .driver
                    .endpoint()
                    .set_tls(*tls)
                    // The endpoint's configuration slots full is its capacity reached, not a
                    // credential refused; anything else is the TLS the caller built.
                    .map_err(|refusal| match refusal {
                        Refusal::Quic => WireError::Access(AccessError::Capacity),
                        _ => WireError::Authentication,
                    });
                let _ = reply.send(set);
            }
            Command::Stats { reply } => {
                let stats = self.stats();
                let _ = reply.send(stats);
            }
            Command::Ended { done } => self.wait_for_end(done),
            Command::Serve { serve, done } => {
                self.serve = Some(serve);
                // One serve a listener; a second waits for the same end.
                self.wait_for_end(done);
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
                    let (peer, epoch) = (serving.peer, serving.epoch);
                    let (kind, class) = (serving.kind, serving.class);
                    self.handle(exchange, (peer, epoch), (kind, class), body);
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
            io: self.driver.stats(),
            ..self.stats
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn connect(
        &mut self,
        address: SocketAddr,
        name: String,
        pin: Option<LeafPins>,
        offers: Vec<u16>,
        alone: bool,
        greet: bool,
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
        if let Some(peer) = self.routes.get(&route).copied().filter(|_| !alone) {
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
        // A connection of its caller's alone is in no route: no other call finds it.
        if !alone {
            self.routes.insert(route.clone(), peer);
        }
        self.peers.insert(
            peer,
            PeerState {
                connections: BTreeMap::new(),
                dialed: Some(DialState {
                    route,
                    offers,
                    alone,
                    greet,
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
        if let Some((epoch, Some(negotiated))) = state.current() {
            let _ = reply.send(Ok(Linked {
                peer,
                epoch,
                negotiated: Some(negotiated),
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
        let idle = !dial.dialing && state.connections.is_empty();
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
        body: Asked,
        period: Duration,
        reply: OneshotSender<Result<Answer, Failed>>,
    ) {
        let current = self
            .peers
            .get(&peer)
            .and_then(|state| state.current())
            .map(|(current, _)| current);
        if current != Some(epoch) || self.closing {
            let _ = reply.send(Err(Failed {
                error: WireError::Connection,
                closed: true,
            }));
            return;
        }
        let Some(class) = FocalClasses::class_of(kind, self.role) else {
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
        body: Asked,
        period: Duration,
        reply: Option<OneshotSender<Result<Answer, Failed>>>,
    ) -> Result<ExchangeId, (Refusal, Option<OneshotSender<Result<Answer, Failed>>>)> {
        // The peer is given `period` to answer once it has the request, and its answer the path's
        // probe timeout to arrive (RFC 9002 §6.2.1): judged at the period alone, an answer sent a
        // moment before it was late by its own flight (the 1 MB seal across 128 kbit/s, two runs
        // of three, 2026-10-10). The peer answers "not yet" within the period by its own margin.
        let delivery = self
            .driver
            .endpoint()
            .path(peer)
            .map_or(Duration::ZERO, |facts| facts.probe_timeout);
        let progress = match Progress::new(period.saturating_add(delivery)) {
            Ok(progress) => progress,
            Err(refusal) => return Err((refusal, reply)),
        };
        let now = self.now();
        let length = u64::try_from(body.bytes.len()).unwrap_or(u64::MAX);
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
        let rest = asking.body.bytes.get(asking.written..).unwrap_or(&[]);
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
                epoch,
                kind,
                class,
                body,
            } => self.request((exchange, peer, epoch), kind, class, body),
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
            Event::Frame {
                peer, kind, frame, ..
            } => self.frame(peer, kind, frame),
            // A lane that refused a frame has room: what waits for it goes now.
            Event::LaneRoom { .. } => self.flush_parked(),
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

    /// Queues a frame on `peer`'s lane on the connection of its epoch, behind the lane's earlier
    /// frames, those that wait for room among them: refused when that connection is gone (its
    /// lanes' frames went with it, and a frame queued on its successor would be taken for one of
    /// them); held while the lane is full.
    fn lane(&mut self, peer: PeerId, frame: Parked, lane: hyper_transport::LaneId) {
        if self.closing || !self.driver.endpoint().connection_live(peer, frame.epoch) {
            self.lane_refused(frame.reply, WireError::Connection);
            return;
        }
        if self
            .parked
            .get(&(peer, lane))
            .is_some_and(|waiting| !waiting.is_empty())
        {
            self.park(peer, lane, frame);
            return;
        }
        match self
            .driver
            .endpoint()
            .send_frame(peer, lane, frame.kind, &frame.body)
        {
            Ok(()) => self.lane_queued(peer, frame.reply),
            Err(Refusal::LaneFull) => self.park(peer, lane, frame),
            Err(refusal) => self.lane_refused(frame.reply, refused(refusal)),
        }
    }

    /// `frame` waits for room on `peer`'s lane, or is refused for the room where as many wait as
    /// the lane itself holds.
    fn park(&mut self, peer: PeerId, lane: hyper_transport::LaneId, frame: Parked) {
        let bound = crate::transport::lane_window(&self.limits);
        let waiting = self.parked.entry((peer, lane)).or_default();
        if waiting.len() >= bound || waiting.try_reserve(1).is_err() {
            self.lane_refused(frame.reply, WireError::Access(AccessError::Capacity));
            return;
        }
        waiting.push_back(frame);
        self.stats.lane_frames_parked = self.stats.lane_frames_parked.saturating_add(1);
    }

    /// The frames that wait for room on their lanes, in each lane's order, while it has room;
    /// those whose connection went, or whose sender stopped waiting, go.
    fn flush_parked(&mut self) {
        if self.parked.is_empty() {
            return;
        }
        let mut parked = std::mem::take(&mut self.parked);
        for ((peer, lane), waiting) in &mut parked {
            // Each turn one frame leaves, or the lane is full and the rest wait.
            for _ in 0..waiting.len() {
                let Some(front) = waiting.front() else {
                    break;
                };
                let outcome = if front.reply.is_closed() {
                    None
                } else if self.closing
                    || !self.driver.endpoint().connection_live(*peer, front.epoch)
                {
                    Some(Err(WireError::Connection))
                } else {
                    match self
                        .driver
                        .endpoint()
                        .send_frame(*peer, *lane, front.kind, &front.body)
                    {
                        Ok(()) => Some(Ok(())),
                        Err(Refusal::LaneFull) => break,
                        Err(refusal) => Some(Err(refused(refusal))),
                    }
                };
                let Some(frame) = waiting.pop_front() else {
                    break;
                };
                match outcome {
                    Some(Ok(())) => self.lane_queued(*peer, frame.reply),
                    Some(Err(error)) => self.lane_refused(frame.reply, error),
                    // Its sender gave up waiting: nothing to tell.
                    None => {}
                }
            }
        }
        parked.retain(|_, waiting| !waiting.is_empty());
        self.parked = parked;
    }

    fn lane_queued(&mut self, peer: PeerId, reply: LaneReply) {
        self.stats.lane_frames_sent = self.stats.lane_frames_sent.saturating_add(1);
        let _ = reply.send(Ok(self.driver.endpoint().path(peer)));
    }

    fn lane_refused(&mut self, reply: LaneReply, error: WireError) {
        self.stats.lane_frames_refused = self.stats.lane_frames_refused.saturating_add(1);
        let _ = reply.send(Err(error));
    }

    /// A frame from `peer`'s lane: its bytes taken whole and its grant given back, as an answer's
    /// are, then handed to what serves frames, with what its sender authenticated as.
    fn frame(&mut self, peer: PeerId, kind: Kind, frame: Reservation) {
        let (granted, tag) = (frame.granted(), frame.tag());
        let body = frame.into_buffer();
        self.driver
            .endpoint()
            .release(Reservation::tagged(granted, Vec::new(), tag));
        let from = self.driver.endpoint().directory().inbound(peer).cloned();
        let sender = match &from {
            Some(Inbound::Data { peer, .. }) => Some(Sender::of(peer.role())),
            Some(Inbound::Joiner) | None => None,
        };
        let class = sender
            .and_then(|sender| <super::classes::FocalClasses as Classes>::class_of(kind, sender));
        let (Some(from), Some(class), Some(serve)) = (from, class, self.serve.as_ref()) else {
            self.stats.lane_frames_dropped = self.stats.lane_frames_dropped.saturating_add(1);
            return;
        };
        // The peer's newest connection is the one its lanes come on.
        let negotiated = self
            .peers
            .get(&peer)
            .and_then(|state| state.connections.values().next_back().copied().flatten());
        let path = self
            .driver
            .endpoint()
            .path(peer)
            .map_or(Duration::ZERO, |facts| facts.rtt);
        let taken = serve.frame(Framed {
            kind,
            class,
            body,
            from,
            negotiated,
            path,
        });
        if taken {
            self.stats.lane_frames_received = self.stats.lane_frames_received.saturating_add(1);
        } else {
            self.stats.lane_frames_dropped = self.stats.lane_frames_dropped.saturating_add(1);
        }
    }

    fn connected(&mut self, peer: PeerId, _role: Sender, epoch: Epoch) {
        self.dialed(peer);
        self.driver.endpoint().directory_mut().set_live(peer, true);
        let state = self.peers.entry(peer).or_insert(PeerState {
            connections: BTreeMap::new(),
            dialed: None,
        });
        // Its own, until its greeting is answered; the peer's other connections keep theirs, those
        // still up.
        state.connections.insert(epoch, None);
        keep_live(self.driver.endpoint(), peer, state);
        let Some(dial) = state.dialed.as_mut() else {
            return;
        };
        if !dial.greet {
            let linked = Linked {
                peer,
                epoch,
                negotiated: None,
            };
            for waiter in dial.waiting.drain(..) {
                let _ = waiter.send(Ok(linked));
            }
            return;
        }
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
        let body = Asked::of(Kind::Hello, body);
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
        state.connections.remove(&epoch);
        keep_live(self.driver.endpoint(), peer, state);
        if !state.connections.is_empty() {
            // Not its last: the peer is up on another.
            return;
        }
        self.driver.endpoint().directory_mut().set_live(peer, false);
        // A dial whose connection closed before its greeting was answered: its certificate
        // refused by the listener (RFC 8446 §4.4.4), or the listener full.
        self.fail_waiting(peer, unreachable(refusal));
        // An inbound peer, or a connection of its caller's alone, is forgotten as it closes.
        let forgotten = self
            .peers
            .get(&peer)
            .is_some_and(|state| state.dialed.as_ref().is_none_or(|dial| dial.alone));
        if forgotten {
            let alone = self
                .peers
                .remove(&peer)
                .is_some_and(|state| state.dialed.is_some());
            if alone {
                self.driver.endpoint().directory_mut().forget(peer);
            }
        }
    }

    fn unreached(&mut self, peer: PeerId, refusal: Option<Refusal>) {
        self.dialed(peer);
        let unpinned = self.driver.endpoint().directory_mut().take_unpinned(peer);
        let error = if unpinned {
            WireError::Unpinned
        } else {
            unreachable(refusal)
        };
        self.fail_waiting(peer, error);
        // A route that was never reached is forgotten, so its next dial starts afresh.
        let never = self
            .peers
            .get(&peer)
            .is_some_and(|state| state.connections.is_empty());
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
            .filter(|state| !state.connections.is_empty())
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
        (exchange, peer, epoch): (ExchangeId, PeerId, Epoch),
        kind: Kind,
        class: TrafficClass,
        body: Option<u64>,
    ) {
        // The profile of the connection the request came on: a peer's connections are greeted
        // each on its own.
        let negotiated = self
            .peers
            .get(&peer)
            .and_then(|state| state.negotiated(epoch));
        // What the body may be: a greeting's bound, an enrollment message's, or the profile the
        // connection negotiated; a request before its connection's greeting is refused, as is a
        // message of the enrollment protocol where it is not served.
        let bound = match kind {
            Kind::Hello => u64::from(HELLO_BYTES),
            Kind::Enrollment(_) => match self.enrollment {
                Some(bound) => bound,
                None => {
                    self.driver
                        .endpoint()
                        .refuse_exchange(exchange, Refusal::Kind);
                    return;
                }
            },
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
                epoch,
                arrived_ns: hyper_rt::futures::now_ns(),
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
        let (peer, epoch) = (serving.peer, serving.epoch);
        let (kind, class) = (serving.kind, serving.class);
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
        self.handle(exchange, (peer, epoch), (kind, class), body);
    }

    /// A request read whole: a greeting is answered here, the rest go to their tasks.
    fn handle(
        &mut self,
        exchange: ExchangeId,
        (peer, epoch): (PeerId, Epoch),
        (kind, class): (Kind, TrafficClass),
        body: Vec<u8>,
    ) {
        match kind {
            Kind::Hello => self.greet(exchange, (peer, epoch), &body),
            Kind::Operation(_) | Kind::Enrollment(_) => {
                self.dispatch(exchange, (peer, epoch), (kind, class), body)
            }
        }
    }

    /// Answers a peer's greeting with the profile both speak.
    fn greet(&mut self, exchange: ExchangeId, (peer, epoch): (PeerId, Epoch), body: &[u8]) {
        let reply = match decode_payload::<Hello>(body) {
            Ok(hello) => {
                let capabilities = self
                    .serve
                    .as_ref()
                    .map_or([false; 5], |serve| serve.capabilities());
                match self.limits.negotiate_laned(&hello, capabilities) {
                    Ok(negotiated) => {
                        // The connection it came on is greeted, told or not: a peer's lifecycle
                        // tells its current connection alone, and waits behind the requests
                        // that came before it, so a greeting may be the first this side hears of
                        // its peer. The peer's other connections are as they were, those still up.
                        let state = self.peers.entry(peer).or_insert(PeerState {
                            connections: BTreeMap::new(),
                            dialed: None,
                        });
                        state.connections.insert(epoch, Some(negotiated));
                        keep_live(self.driver.endpoint(), peer, state);
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
        (peer, epoch): (PeerId, Epoch),
        (kind, class): (Kind, TrafficClass),
        body: Vec<u8>,
    ) {
        let arrived_ns = self
            .serving
            .get(&exchange)
            .map_or_else(hyper_rt::futures::now_ns, |serving| serving.arrived_ns);
        let Some(from) = self.driver.endpoint().directory().inbound(peer).cloned() else {
            self.end_serving(exchange);
            return;
        };
        let path = self
            .driver
            .endpoint()
            .path(peer)
            .map_or(Duration::ZERO, |facts| facts.rtt);
        let negotiated = self
            .peers
            .get(&peer)
            .and_then(|state| state.negotiated(epoch));
        let served = Served {
            kind,
            class,
            body,
            from,
            negotiated,
            path,
            arrived_ns,
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
        // This side's greeting went on its dial's connection, the current one.
        let Some((epoch, _)) = state.current() else {
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
                if let Some(greeted) = state.connections.get_mut(&epoch) {
                    *greeted = Some(negotiated);
                }
                let linked = Linked {
                    peer,
                    epoch,
                    negotiated: Some(negotiated),
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
        WireError::Unpinned => WireError::Unpinned,
        WireError::Connection => WireError::Connection,
        WireError::Access(access) => WireError::Access(access.clone()),
        WireError::SocketPath { path, most } => WireError::SocketPath {
            path: path.clone(),
            most: *most,
        },
    }
}

/// An owner's failure to start: the runtime refused a channel or a task.
impl From<RtError> for WireError {
    fn from(_: RtError) -> Self {
        WireError::Connection
    }
}
