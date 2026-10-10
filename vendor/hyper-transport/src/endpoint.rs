//! The endpoint: one per socket, one owner (mantle note 32 §3.4).
//!
//! It owns hyper-quic's endpoint and every connection, each boxed in a table indexed by the QUIC
//! layer's handle, and every exchange in a generational table. A connection is taken out of its
//! slot while it is driven and put back after, so the code that drives it reaches the rest of the
//! endpoint without sharing anything.
//!
//! **The events the owner has not polled** are bounded by the endpoint's limits
//! ([`Limits::event_bound`], `docs/transport.md` §4a). Every event waits in a seat of a bounded
//! table, held until the owner polls it:
//! - an exchange's events hold its slot in the exchange table, removed or not; each kind waits at
//!   most once ([`EXCHANGE_EVENTS`]): its head, `BodyReady` and `Writable` (a later one while one
//!   waits says nothing the waiting one does not), and `Refused`;
//! - a lane's frames are counted against the lane, at most the core's window of them, and against
//!   its peer, at most its lanes' windows; a lane at either bound is not read, so its bytes wait
//!   in QUIC, whose flow control holds the sender (RFC 9000 §4.1), and it is read again as the
//!   owner polls the peer's frames;
//! - a peer's lifecycle waits as one event, its latest state with its epoch: a change while it
//!   waits replaces it and moves it to the back, after the refusals of the exchanges a closed
//!   connection carried; its entry in the peer table is held until the owner polls it.
//!
//! A table at its bound refuses, typed, the stream, the connection or the dial that would need one
//! more seat ([`Refusal::Exchanges`], [`Refusal::Events`]); no event is ever dropped, and nothing
//! refuses a datagram, whose acknowledgements every connection's loss detection needs (RFC 9002
//! §6).

use std::collections::VecDeque;
use std::marker::PhantomData;
use std::net::SocketAddr;
use std::time::{Duration, Instant};

use hyper_quic::congestion::Congestion;
use hyper_quic::rustls::pki_types::CertificateDer;
use hyper_quic::{
    ClientConfigHandle, Connection, ConnectionError, DatagramEvent, Dir, EcnCodepoint,
    EndpointConfig, Event as QuicEvent, IdleTimeout, Incoming as Attempt, StreamEvent, StreamId,
    Transmit, TransportConfig, TransportErrorCode, VarInt,
};

use crate::admission::{Admission, AdmissionLimits, AdmissionStats};
use crate::budget::{Budget, Lane, Reservation};
use crate::credit::{
    K_GRANULARITY, MIN_DATAGRAM, Window, class_reserve, initial_window, stream_window_ceiling,
};
use crate::exchange::{End, Exchange, In, Incoming, Out, Outgoing, Pushed, abandon, pull, push};
use crate::frame::{PREFIX_BYTES, Prefix};
use crate::lane::{LaneIn, LaneOut, OPENER_BYTES, Reading, opened};
use crate::progress::{Carry, Delivered, Moved, Progress};
use crate::receive::Receive;
use crate::tally::{Table, Tally};
use crate::timing::PeerTiming;
use crate::tls::Tls;
use crate::{Authenticated, Classes, Directory, Epoch, Event, ExchangeId, PeerId, Refusal};

/// The bounds an endpoint keeps; every one is the owner's configuration, derived by its own law
/// (CLAUDE.md §1: no arbitrary numbers).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Limits {
    /// Admission of connections by identity.
    pub admission: AdmissionLimits,
    /// The exchanges a peer may have open on one connection at once (QUIC's bidirectional stream
    /// limit, RFC 9000 §4.6). An exchange this side opens past the peer's limit waits its turn.
    pub streams_per_connection: u32,
    /// The exchanges open on the endpoint at once, both directions.
    pub exchanges: usize,
    /// The lanes a peer may open to this node (QUIC's unidirectional stream limit).
    pub lanes_per_peer: u32,
    /// The frames a lane queues: the core's window (T37).
    pub lane_window: usize,
    /// The longest head a message may carry.
    pub max_head: u32,
    /// The most a connection's receive window may grow to: the peer's share of the node's
    /// transport budget. It also bounds what QUIC retains of this side's own writes.
    pub window_ceiling: u64,
    /// The period an exchange a peer opened is judged at while its request arrives and its reply
    /// is carried.
    pub serve_period: Duration,
    /// QUIC's idle timeout (RFC 9000 §10.1: at least three probe timeouts).
    pub idle_timeout: Duration,
    /// QUIC keep-alive, from the path's measured NAT lifetime; `None` sends none.
    pub keep_alive: Option<Duration>,
    /// The endpoint's own responses (version negotiation, retries, refusals, stateless resets)
    /// queued for sending; one past the bound is dropped and counted, as RFC 9000 lets an endpoint
    /// leave any of them unsent.
    pub max_responses: usize,
    /// The chunks of [`crate::RECEIVE_CHUNK`] bytes received datagrams are cut from, each reserved
    /// from the budget: the chunk memory a peer can pin is at most this many chunks, whatever it
    /// sends; past them a datagram is copied into a buffer of its own (`crate::receive`). A full
    /// receive window of unread data, packed, occupies `window_ceiling / RECEIVE_CHUNK` chunks; the
    /// owner sets this from that and its budget.
    pub receive_chunks: usize,
}

impl Limits {
    /// The most events the endpoint holds for its owner at once: [`EXCHANGE_EVENTS`] for every
    /// exchange the table holds, and, for every entry of the peer table (the larger of the
    /// identity and connection bounds), its one lifecycle event and its lanes' windows of frames.
    /// The queue never holds more (the module's documentation says how).
    pub fn event_bound(&self) -> usize {
        let peers = self.admission.identities.max(self.admission.connections);
        let lanes = usize::try_from(self.lanes_per_peer).unwrap_or(usize::MAX);
        let frames = lanes.saturating_mul(self.lane_window);
        EXCHANGE_EVENTS
            .saturating_mul(self.exchanges)
            .saturating_add(peers.saturating_mul(frames.saturating_add(1)))
    }

    fn validate(&self) -> Result<(), Refusal> {
        let zero = self.streams_per_connection == 0
            || self.exchanges == 0
            || self.lanes_per_peer == 0
            || self.lane_window == 0
            || self.max_responses == 0
            || self.serve_period.is_zero()
            || self.idle_timeout.is_zero();
        if zero || self.window_ceiling < initial_window(MIN_DATAGRAM) {
            return Err(Refusal::Configuration);
        }
        Ok(())
    }
    /// The QUIC transport both sides of every connection run with: the stream limits above, the
    /// initial receive window and the reserve on top of it ([`crate::credit`]), a stream window
    /// at quinn's assembler ceiling or the share, whichever is less.
    fn transport(&self, ranks: u8, congestion: &Congestion) -> Result<TransportConfig, Refusal> {
        let reserve = class_reserve(ranks.saturating_sub(1));
        let window = initial_window(MIN_DATAGRAM).saturating_add(reserve);
        let stream = stream_window_ceiling().min(self.window_ceiling);
        let varint = |value: u64| VarInt::from_u64(value).map_err(|_| Refusal::Configuration);
        let idle = IdleTimeout::try_from(self.idle_timeout).map_err(|_| Refusal::Configuration)?;
        let mut transport = TransportConfig::default();
        transport
            .max_concurrent_bidi_streams(VarInt::from_u32(self.streams_per_connection))
            .max_concurrent_uni_streams(VarInt::from_u32(self.lanes_per_peer))
            .stream_receive_window(varint(stream)?)
            .receive_window(varint(window)?)
            .send_window(self.window_ceiling)
            .max_idle_timeout(Some(idle))
            .keep_alive_interval(self.keep_alive)
            .congestion(congestion.clone());
        Ok(transport)
    }
}

/// What an endpoint is built from.
pub struct Config<R> {
    /// The TLS its connections run under: what it presents and how it verifies its peers, and
    /// whether it accepts connections (a server side) or only dials ([`Tls::mutual`] from
    /// credentials, or the owner's own configurations).
    pub tls: Tls,
    /// The node's role as a sender: with a message's kind it gives the message's class.
    pub role: R,
    /// The endpoint's bounds.
    pub limits: Limits,
    /// The congestion controller every connection's paths run: one of hyper-quic's reviewed set
    /// (`congestion::Congestion`; CUBIC by default, focal runs Copa).
    pub congestion: Congestion,
}

/// What the path to a peer measures now.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PathFacts {
    /// The smoothed round trip (RFC 9002 §5.3).
    pub rtt: Duration,
    /// The least round trip seen.
    pub min_rtt: Duration,
    /// The congestion window, bytes.
    pub cwnd: u64,
    /// The largest UDP payload the path carries now.
    pub max_datagram: u16,
    /// The rate the congestion window allows over the round trip, bytes a second.
    pub delivery_rate: u64,
    /// The receive window this side advertises to the peer now, reserve excluded.
    pub receive_window: u64,
    /// The rate the congestion controller paces at, bytes a second, when it states one (Copa does,
    /// once it has a round trip; NewReno, CUBIC and BBR keep the connection's rule).
    pub pacing_rate: Option<u64>,
    /// The controller's slow-start threshold, bytes, when it keeps one.
    pub ssthresh: Option<u64>,
}

/// What the endpoint holds and what it refused.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Stats {
    /// Admission.
    pub admission: AdmissionStats,
    /// Exchanges open.
    pub exchanges: usize,
    /// Connections held, in any state.
    pub connections: usize,
    /// Events not yet polled.
    pub events: usize,
    /// Lane frames skipped because their class or the budget could not take them.
    pub frames_dropped: u64,
    /// Endpoint responses dropped at their bound.
    pub responses_dropped: u64,
    /// Streams a peer opened past a bound, refused.
    pub streams_refused: u64,
    /// Receive chunks held, each charged to the budget.
    pub receive_chunks: usize,
    /// Datagrams copied into buffers of their own because no chunk could take them.
    pub receive_copied: u64,
}

/// The QUIC application error code a connection closes with when it ends without a fault.
const CLOSE_NORMAL: u32 = 0;
/// The kinds of event an exchange can have waiting at once, each at most once: its head
/// (`Request` or `Reply`, once in its life), `BodyReady` and `Writable` (each replaced by none
/// while one waits), and `Refused` (once, as it ends).
const EXCHANGE_EVENTS: usize = 4;
/// A message's incoming states, each of which a step leaves or stops at: the bound on the steps
/// one read takes.
const IN_STATES: usize = 6;

/// What the owner asks with, for [`Core::open`].
struct Ask<'a, K> {
    peer: PeerId,
    kind: u16,
    class: K,
    header: &'a [u8],
    body: Option<u64>,
    deadline: Progress,
}

struct Conn<R> {
    quic: Connection,
    /// The endpoint's key for it: the QUIC layer's handle, which a later connection may reuse.
    key: usize,
    /// It among every connection the endpoint has held, counted: what tells it from a later one
    /// under the same key.
    serial: u64,
    /// The peer this side dialed, if it dialed.
    dialed: Option<PeerId>,
    /// Whether an inbound handshake still holds its pending place.
    pending: bool,
    peer: Option<(PeerId, R)>,
    epoch: Epoch,
    /// Its exchanges, oldest first.
    exchanges: Vec<u64>,
    /// Its exchanges with a stream, as `(stream, exchange)` sorted by stream: what a stream event is matched to by a
    /// binary search. It was a scan of `exchanges` with an arena lookup each, per event, so a connection with hundreds of
    /// exchanges open paid hundreds of lookups per Readable, Stopped or Writable event (slates measured the same shape
    /// in its own fetch worker, 2026-10-05: a 64 MiB pull of 1,024 chunks did not finish in 9 minutes of CPU until each
    /// turn visited only the exchanges that moved). Bounded by `limits.streams_per_connection`, as `exchanges` is, and
    /// reserved to it when the connection is made, so no insert allocates.
    by_stream: Vec<(StreamId, u64)>,
    /// When this connection's credit queue last advanced (policy C): an exchange was given a stream or finished, or
    /// the peer raised the streams it lets this side open. An exchange waiting for a stream is judged on this.
    credit_moved: Option<Instant>,
    lanes_out: Vec<LaneOut>,
    lanes_in: Vec<LaneIn>,
    window: Window,
    /// The stream bytes read on it, by class.
    delivered: Delivered,
    /// The budget's grants for the receive window: the initial one and one per growth.
    grants: Vec<Reservation>,
    /// The connection was lost and its owner told.
    lost: bool,
}

/// A peer's lifecycle as the owner hears it: its newest connection up, that connection closed,
/// or a dial to it failed with no connection up since; a close and a failed dial with why, when
/// either side said.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Lifecycle {
    Connected(Epoch),
    Closed(Epoch, Option<Refusal>),
    Unreachable(Option<Refusal>),
}

#[derive(Debug)]
struct PeerEntry<R> {
    peer: PeerId,
    timing: PeerTiming,
    epoch: Epoch,
    /// The established connection exchanges to the peer are opened on: its newest.
    connection: Option<usize>,
    /// Its role, as its newest connection's certificate names it.
    role: Option<R>,
    used: u64,
    /// The connections identified as the peer and not lost, its newest or older ones not yet
    /// gone.
    held: u32,
    /// A dial to it is in progress.
    dialing: bool,
    /// The last dial to it failed, and no connection to it has come up since: why, when either
    /// side said.
    unreachable: Option<Option<Refusal>>,
    /// Why its newest connection closed, when either side said.
    closed: Option<Refusal>,
    /// The state the owner was last told, by the lifecycle event it polled last.
    reported: Option<Lifecycle>,
    /// The state of the lifecycle event waiting for the owner, if one does.
    waiting: Option<Lifecycle>,
    /// Frames of its lanes waiting for the owner, on any of its connections, live or gone.
    frames: usize,
    /// Its lanes not read for want of a seat for their next frame.
    paused: u32,
}

impl<R> PeerEntry<R> {
    fn new(peer: PeerId, used: u64) -> Self {
        Self {
            peer,
            timing: PeerTiming::default(),
            epoch: 0,
            connection: None,
            role: None,
            used,
            held: 0,
            dialing: false,
            unreachable: None,
            closed: None,
            reported: None,
            waiting: None,
            frames: 0,
            paused: 0,
        }
    }
    /// The peer's state now: `None` before it ever connected or a dial to it failed.
    fn lifecycle(&self) -> Option<Lifecycle> {
        if self.connection.is_some() {
            Some(Lifecycle::Connected(self.epoch))
        } else if let Some(refusal) = self.unreachable {
            Some(Lifecycle::Unreachable(refusal))
        } else {
            (self.epoch > 0).then_some(Lifecycle::Closed(self.epoch, self.closed))
        }
    }
    /// Whether the entry must stay: a connection or a dial needs it, or events wait in it.
    fn pinned(&self) -> bool {
        self.held > 0 || self.dialing || self.waiting.is_some() || self.frames > 0
    }
}

/// An event waiting for the owner. The seat it holds until it is polled is the one it names (its
/// exchange's slot, its peer's entry), and for a frame also the lane it came on: the key and serial
/// of its connection, and its stream.
struct Waiting<C: Classes> {
    event: Event<C>,
    lane: Option<(usize, u64, StreamId)>,
}

impl<C: Classes> Waiting<C> {
    /// Whether this is a lifecycle event of `peer`.
    fn lifecycle_of(&self, peer: PeerId) -> bool {
        matches!(self.event,
            Event::Connected { peer: of, .. } | Event::Closed { peer: of, .. } | Event::Unreachable { peer: of, .. }
                if of == peer)
    }
}

/// The endpoint: see the crate's documentation.
pub struct Endpoint<C: Classes, B: Budget<C::Class>, D: Directory<Role = C::Role>> {
    quic: hyper_quic::Endpoint,
    client: ClientConfigHandle,
    conns: Vec<Option<Box<Conn<C::Role>>>>,
    core: Core<C, B, D>,
    receive: Receive,
    scratch: Vec<u8>,
    responses: VecDeque<(Transmit, Vec<u8>)>,
    spare: Vec<Vec<u8>>,
    next: usize,
    /// The connections held so far: the next one's serial.
    serials: u64,
    /// A peer's connections, gathered to read its paused lanes again.
    keys: Vec<usize>,
}

/// What the code driving one connection reaches besides it.
struct Core<C: Classes, B, D> {
    classes: PhantomData<C>,
    budget: B,
    directory: D,
    role: C::Role,
    limits: Limits,
    /// The congestion controller connections are made with.
    congestion: Congestion,
    /// The deadline an exchange a peer opened is judged by.
    serve: Progress,
    admission: Admission,
    exchanges: Table<C::Class>,
    /// Whether every settled sum is compared with the fold it replaced (`oracle` feature).
    #[cfg(feature = "oracle")]
    oracle: bool,
    /// Settled sums that differed from their fold.
    #[cfg(feature = "oracle")]
    mismatches: u64,
    /// The events the owner has not polled, at most [`Limits::event_bound`].
    events: VecDeque<Waiting<C>>,
    peers: Vec<PeerEntry<C::Role>>,
    /// The last `now` the endpoint was given; operations the owner calls between driving calls
    /// take it as theirs.
    now: Instant,
    /// The owner's timer granularity, as it last reported it: what receive windows are tuned under.
    granularity: Duration,
    tick: u64,
    dialing: usize,
    ids: Vec<u64>,
    stats: Stats,
}

/// What `conn` has moved, as the wait of an exchange of `rank` reads it.
fn moved<R>(conn: &Conn<R>, rank: u8) -> Moved {
    let stats = conn.quic.stats();
    Moved {
        sent: stats.udp_tx.bytes.saturating_sub(stats.path.lost_bytes),
        received: stats.udp_rx.bytes,
        delivered: conn.delivered.from(rank),
    }
}

fn length(bytes: usize) -> u64 {
    u64::try_from(bytes).unwrap_or(u64::MAX)
}

/// The code range of a TLS alert carried as a QUIC CRYPTO_ERROR (RFC 9000 §20.1: `0x0100` to
/// `0x01ff`, the alert in the low byte).
const CRYPTO_ERRORS: std::ops::RangeInclusive<u64> = 0x0100..=0x01ff;

/// What a connection's loss says of why it ended, when either side said: a TLS alert from either
/// side is a certificate refused ([`Refusal::Identity`]); the peer's CONNECTION_REFUSED (RFC 9000
/// §20.1), sent for an attempt its pending places or its budget could not take, is its pending
/// bound ([`Refusal::Pending`]); the peer's application close carries its refusal's code
/// ([`Refusal::code`]). A close that names no refusal, a timeout, a reset and this side's own close
/// say nothing.
fn refusal_of(reason: &ConnectionError) -> Option<Refusal> {
    match reason {
        ConnectionError::TransportError(error)
            if CRYPTO_ERRORS.contains(&u64::from(error.code)) =>
        {
            Some(Refusal::Identity)
        }
        ConnectionError::ConnectionClosed(close)
            if CRYPTO_ERRORS.contains(&u64::from(close.error_code)) =>
        {
            Some(Refusal::Identity)
        }
        ConnectionError::ConnectionClosed(close)
            if close.error_code == TransportErrorCode::CONNECTION_REFUSED =>
        {
            Some(Refusal::Pending)
        }
        ConnectionError::ApplicationClosed(close) => {
            match Refusal::from_code(close.error_code.into_inner()) {
                Refusal::Unknown | Refusal::Closed => None,
                refusal => Some(refusal),
            }
        }
        _ => None,
    }
}

impl<C: Classes, B: Budget<C::Class>, D: Directory<Role = C::Role>> Endpoint<C, B, D> {
    /// An endpoint for `config`, holding its bytes against `budget` and naming peers by
    /// `directory`. `now` is the caller's clock.
    pub fn new(
        config: Config<C::Role>,
        budget: B,
        directory: D,
        now: Instant,
    ) -> Result<Self, Refusal> {
        config.limits.validate()?;
        let transport = config.limits.transport(C::RANKS, &config.congestion)?;
        let (server, client) = config.tls.prepare(&transport)?;
        let mut quic = hyper_quic::Endpoint::new(EndpointConfig::default(), server, true, None)
            .map_err(|_| Refusal::Quic)?;
        let client = quic
            .insert_client_config(client)
            .map_err(|_| Refusal::Quic)?;
        let limits = config.limits;
        let peers = limits
            .admission
            .identities
            .max(limits.admission.connections);
        Ok(Self {
            quic,
            client,
            conns: Vec::new(),
            core: Core {
                classes: PhantomData,
                budget,
                directory,
                role: config.role,
                limits,
                congestion: config.congestion,
                serve: Progress::new(limits.serve_period)?,
                admission: Admission::new(limits.admission, limits.serve_period)?,
                exchanges: Table::new(limits.exchanges, C::RANKS),
                #[cfg(feature = "oracle")]
                oracle: false,
                #[cfg(feature = "oracle")]
                mismatches: 0,
                events: VecDeque::new(),
                peers: Vec::with_capacity(peers),
                now,
                granularity: K_GRANULARITY,
                tick: 0,
                dialing: 0,
                ids: Vec::new(),
                stats: Stats::default(),
            },
            receive: Receive::new(limits.receive_chunks),
            scratch: Vec::new(),
            responses: VecDeque::with_capacity(limits.max_responses),
            spare: Vec::with_capacity(limits.max_responses),
            next: 0,
            serials: 0,
            keys: Vec::with_capacity(limits.admission.per_identity),
        })
    }

    /// A datagram arrived from `from`.
    pub fn handle_datagram(
        &mut self,
        now: Instant,
        from: SocketAddr,
        ecn: Option<EcnCodepoint>,
        bytes: &[u8],
    ) {
        self.core.now = now;
        let datagram = self.receive.take(bytes, &mut self.core.budget);
        let mut buffer = std::mem::take(&mut self.scratch);
        buffer.clear();
        match self
            .quic
            .handle(now, from, None, ecn, datagram, &mut buffer)
        {
            Some(DatagramEvent::NewConnection(attempt)) => self.attempt(now, attempt, &mut buffer),
            Some(DatagramEvent::ConnectionEvent(handle, event)) => {
                if let Some(Some(conn)) = self.conns.get_mut(handle.0) {
                    conn.quic.handle_event(event, self.quic.configs_mut());
                }
                self.pump(now, handle.0);
            }
            Some(DatagramEvent::Response(transmit)) => self.respond(transmit, &buffer),
            None => {}
        }
        self.scratch = buffer;
    }

    /// The next datagram to send, written into `out` (cleared first).
    pub fn poll_transmit(&mut self, now: Instant, out: &mut Vec<u8>) -> Option<Transmit> {
        self.core.now = now;
        out.clear();
        if let Some((transmit, bytes)) = self.responses.pop_front() {
            out.extend_from_slice(&bytes);
            if self.spare.len() < self.core.limits.max_responses {
                self.spare.push(bytes);
            }
            return Some(transmit);
        }
        let count = self.conns.len();
        for step in 0..count {
            let key = self.next.wrapping_add(step).checked_rem(count).unwrap_or(0);
            let Some(Some(conn)) = self.conns.get_mut(key) else {
                continue;
            };
            if let Some(transmit) = conn.quic.poll_transmit(now, 1, out, self.quic.configs()) {
                self.next = key.wrapping_add(1);
                self.endpoint_events(key);
                return Some(transmit);
            }
        }
        None
    }

    /// When [`Endpoint::handle_timeout`] is next due.
    pub fn poll_timeout(&mut self) -> Option<Instant> {
        let connections = self
            .conns
            .iter_mut()
            .flatten()
            .filter_map(|conn| conn.quic.poll_timeout())
            .min();
        let exchanges = self
            .core
            .exchanges
            .values()
            .filter_map(|exchange| exchange.carry.due())
            .min();
        match (connections, exchanges) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (a, b) => a.or(b),
        }
    }

    /// Time has reached `now`: fire every timer due.
    pub fn handle_timeout(&mut self, now: Instant) {
        self.core.now = now;
        for key in 0..self.conns.len() {
            if let Some(Some(conn)) = self.conns.get_mut(key)
                && conn.quic.poll_timeout().is_some_and(|due| due <= now)
            {
                conn.quic.handle_timeout(now);
            }
            self.pump(now, key);
        }
    }

    /// The next event for the owner. Polling it gives back the seat it held (the module's
    /// documentation): an exchange's slot, a peer's entry, or a seat of a lane's window, and a
    /// lane paused for want of one is read again.
    pub fn poll_event(&mut self) -> Option<Event<C>> {
        let Waiting { event, lane } = self.core.events.pop_front()?;
        match event {
            Event::Request { exchange, .. }
            | Event::Reply { exchange, .. }
            | Event::Refused { exchange, .. } => self.core.exchanges.release(exchange.0),
            Event::BodyReady { exchange } => self
                .core
                .exchanges
                .release_with(exchange.0, |waiting| waiting.ready_waiting = false),
            Event::Writable { exchange } => self
                .core
                .exchanges
                .release_with(exchange.0, |waiting| waiting.writable_waiting = false),
            Event::Connected { peer, .. }
            | Event::Closed { peer, .. }
            | Event::Unreachable { peer, .. } => {
                self.core.lifecycle_polled(peer);
            }
            Event::Frame { peer, .. } => {
                if let Some((key, serial, stream)) = lane {
                    self.frame_polled(peer, (key, serial), stream);
                }
            }
        }
        Some(event)
    }

    /// Dial `peer` at `address`, unless a connection to it exists or is being dialed: concurrent
    /// cold calls to one peer dial once and share the connection (T42, focal's F60). The dial
    /// holds the peer's entry for the lifecycle event it ends in; [`Refusal::Events`] when every
    /// entry is held by events the owner has not polled.
    pub fn connect(
        &mut self,
        now: Instant,
        peer: PeerId,
        address: SocketAddr,
    ) -> Result<(), Refusal> {
        self.core.now = now;
        let known = self.conns.iter().flatten().any(|conn| {
            !conn.lost && (conn.dialed == Some(peer) || conn.peer.is_some_and(|(id, _)| id == peer))
        });
        if known {
            return Ok(());
        }
        self.core.admission.may_dial(self.core.dialing)?;
        let dialing = self.core.entry(peer).ok_or(Refusal::Events)?;
        dialing.dialing = true;
        let dialed = self.dial(now, peer, address);
        if dialed.is_err()
            && let Some(entry) = self.core.entry(peer)
        {
            entry.dialing = false;
        }
        dialed
    }

    /// The dial of [`Endpoint::connect`], once the peer's entry holds it.
    fn dial(&mut self, now: Instant, peer: PeerId, address: SocketAddr) -> Result<(), Refusal> {
        let grant = self.core.window_grant()?;
        let Some(name) = self.core.directory.server_name(peer) else {
            self.core.budget.release(grant);
            return Err(Refusal::Identity);
        };
        let dialed = self.quic.connect(now, self.client, address, name, None);
        let (handle, quic) = match dialed {
            Ok(dialed) => dialed,
            Err(_) => {
                self.core.budget.release(grant);
                return Err(Refusal::Quic);
            }
        };
        self.core.dialing = self.core.dialing.saturating_add(1);
        self.store(handle.0, quic, Some(peer), grant);
        Ok(())
    }

    /// Close every connection to `peer` (T43: a retired route closes its connection); its
    /// exchanges are refused as the connections close.
    pub fn disconnect(&mut self, now: Instant, peer: PeerId) {
        self.close_peer(now, peer, CLOSE_NORMAL);
    }

    /// Close every connection to `peer`, telling it why: a peer whose identity no longer
    /// authenticates here (its grant withdrawn) is refused [`Refusal::Identity`], which its side
    /// reports as it reports a dial whose certificate was refused at its handshake. Its exchanges
    /// are refused as the connections close.
    pub fn refuse(&mut self, now: Instant, peer: PeerId, refusal: Refusal) {
        self.close_peer(now, peer, refusal.code());
    }

    fn close_peer(&mut self, now: Instant, peer: PeerId, code: u32) {
        self.core.now = now;
        for key in 0..self.conns.len() {
            let of = self
                .conns
                .get(key)
                .and_then(Option::as_ref)
                .is_some_and(|conn| {
                    conn.dialed == Some(peer) || conn.peer.is_some_and(|(id, _)| id == peer)
                });
            if of {
                self.close(now, key, code);
            }
        }
    }

    /// Ask `peer` a message of `kind` with `header`, and a body of `body` bytes to follow through
    /// [`Endpoint::write_body`]. The exchange is judged by `deadline` (T39). Past the peer's
    /// stream limit it waits its turn, judged by its deadline all the same (T37).
    pub fn open(
        &mut self,
        now: Instant,
        peer: PeerId,
        kind: C::Kind,
        header: &[u8],
        body: Option<u64>,
        deadline: Progress,
    ) -> Result<ExchangeId, Refusal> {
        self.core.now = now;
        let class = C::class_of(kind, self.core.role).ok_or(Refusal::Kind)?;
        let key = self.core.connection_of(peer).ok_or(Refusal::NotConnected)?;
        let mut conn = self.take(key).ok_or(Refusal::NotConnected)?;
        // A period that hears nothing ends an exchange whose request is being sent, so a period
        // no longer than the peer's acknowledgement delay could end it while the peer lives
        // (`progress.rs`): refused before anything is sent.
        if deadline.period() <= conn.quic.peer_max_ack_delay() {
            self.put(key, conn);
            return Err(Refusal::Configuration);
        }
        let ask = Ask {
            peer,
            kind: C::kind_code(kind),
            class,
            header,
            body,
            deadline,
        };
        let opened = self.core.open(now, key, &mut conn, ask);
        self.put(key, conn);
        opened
    }

    /// Run the connections made from now on under `tls`: a renewed certificate, roots that
    /// changed, a protocol added or taken away. A connection already up keeps the TLS it was
    /// made with until it closes; a configuration replaced goes once the last connection made
    /// under it has drained. A `tls` without a server side stops accepting connections, and one
    /// with it starts. [`Refusal::Configuration`] for a configuration [`Endpoint::new`] would
    /// refuse, [`Refusal::Quic`] when every configuration slot still serves a connection; either
    /// way the endpoint keeps the TLS it had.
    pub fn set_tls(&mut self, tls: Tls) -> Result<(), Refusal> {
        let transport = self
            .core
            .limits
            .transport(C::RANKS, &self.core.congestion)?;
        let (server, client) = tls.prepare(&transport)?;
        let client = self
            .quic
            .insert_client_config(client)
            .map_err(|_| Refusal::Quic)?;
        if self.quic.set_server_config(server).is_err() {
            self.quic.retire_client_config(client);
            return Err(Refusal::Quic);
        }
        let replaced = std::mem::replace(&mut self.client, client);
        self.quic.retire_client_config(replaced);
        Ok(())
    }

    /// The head of the message the peer sent on `exchange`, once it has arrived.
    pub fn head(&self, exchange: ExchangeId) -> Option<&[u8]> {
        let exchange = self.core.exchanges.get(exchange.0)?;
        if matches!(exchange.incoming.state, In::Prefix | In::Head) {
            return None;
        }
        exchange.incoming.head.as_ref().map(Reservation::bytes)
    }

    /// Write the next bytes of `exchange`'s body; returns how many were taken, fewer than offered
    /// when credit ran out ([`Event::Writable`] says when more can be taken). The last byte
    /// written writes the body's checksum and finishes the message.
    pub fn write_body(&mut self, exchange: ExchangeId, from: &[u8]) -> Result<usize, Refusal> {
        self.with_exchange(exchange, |core, conn, id| core.write_body(conn, id, from))
    }

    /// Read the next bytes of the peer's body on `exchange` into `into`, as many as it has room
    /// for and have arrived; [`Event::BodyReady`] says when more have. Once the last byte is read
    /// the body's checksum is verified ([`Endpoint::body_complete`]).
    pub fn read_body(
        &mut self,
        exchange: ExchangeId,
        into: &mut Reservation,
    ) -> Result<usize, Refusal> {
        self.with_exchange(exchange, |core, conn, id| core.read_body(conn, id, into))
    }

    /// Whether the peer's whole message on `exchange` has been read and verified.
    pub fn body_complete(&self, exchange: ExchangeId) -> bool {
        self.core
            .exchanges
            .get(exchange.0)
            .is_some_and(|exchange| exchange.incoming.whole())
    }

    /// Answer the request on `exchange` with `header`, and a body of `body` bytes to follow
    /// through [`Endpoint::write_body`].
    pub fn reply(
        &mut self,
        exchange: ExchangeId,
        header: &[u8],
        body: Option<u64>,
    ) -> Result<(), Refusal> {
        self.with_exchange(exchange, |core, conn, id| {
            core.reply(conn, id, header, body)
        })
    }

    /// End `exchange`: its reservations go back to the budget, and a half not yet complete is
    /// reset, which the peer sees as [`Refusal::Closed`]. A message whose every body byte was
    /// written is complete to its owner: the trailer the endpoint adds is still sent, after
    /// which the exchange goes. An exchange is the owner's until it ends it or it is refused.
    pub fn end(&mut self, exchange: ExchangeId) {
        let _ = self.with_exchange(exchange, |core, conn, id| {
            core.end(conn, id);
            Ok(())
        });
    }

    /// End `exchange`, refusing it for `refusal`: a half not yet complete is reset with the
    /// refusal's code, so its peer hears why — an owner holding a sender to its share of what it
    /// reads refuses past it `Budget`, which the asker reads as the peer's refusal and not as its
    /// connection failing. The owner, which refused it, hears nothing more of it.
    pub fn refuse_exchange(&mut self, exchange: ExchangeId, refusal: Refusal) {
        let _ = self.with_exchange(exchange, |core, conn, id| {
            core.refuse(conn, id, refusal);
            Ok(())
        });
    }

    /// Bytes for a message of `class`, from the endpoint's budget: what the owner reads bodies
    /// into.
    pub fn reserve(&mut self, class: C::Class, bytes: u64) -> Result<Reservation, Refusal> {
        self.core.budget.reserve(bytes, Lane::Class(class))
    }

    /// Give a reservation back to the budget: a frame's, or one the owner reserved.
    pub fn release(&mut self, reservation: Reservation) {
        self.core.budget.release(reservation);
    }

    /// Queue `frame` of `kind` on `peer`'s lane `lane`, in order after the lane's earlier frames.
    pub fn send_frame(
        &mut self,
        peer: PeerId,
        lane: crate::LaneId,
        kind: C::Kind,
        frame: &[u8],
    ) -> Result<(), Refusal> {
        let class = C::class_of(kind, self.core.role).ok_or(Refusal::Kind)?;
        if length(frame.len()) > C::frame_bound(class) {
            return Err(Refusal::FrameBound);
        }
        let key = self.core.connection_of(peer).ok_or(Refusal::NotConnected)?;
        let mut conn = self.take(key).ok_or(Refusal::NotConnected)?;
        let queued = self
            .core
            .send_frame(&mut conn, lane, (C::kind_code(kind), class), frame);
        self.put(key, conn);
        queued
    }

    /// Keying material from the TLS session of the connection to `peer` (RFC 5705, RFC 8446
    /// §7.5), for the datagram plane's keys, and the epoch it belongs to.
    pub fn export_keying_material(
        &self,
        peer: PeerId,
        label: &[u8],
        context: &[u8],
        out: &mut [u8],
    ) -> Result<Epoch, Refusal> {
        let key = self.core.connection_of(peer).ok_or(Refusal::NotConnected)?;
        let conn = self
            .conns
            .get(key)
            .and_then(Option::as_ref)
            .ok_or(Refusal::NotConnected)?;
        conn.quic
            .crypto_session()
            .export_keying_material(out, label, context)
            .map_err(|_| Refusal::Configuration)?;
        Ok(conn.epoch)
    }

    /// What the path to `peer` measures now.
    pub fn path(&self, peer: PeerId) -> Option<PathFacts> {
        let key = self.core.connection_of(peer)?;
        let conn = self.conns.get(key)?.as_ref()?;
        let stats = conn.quic.stats();
        let rtt = stats.path.rtt;
        let nanos = u128::from(stats.path.cwnd).saturating_mul(1_000_000_000);
        let rate = nanos.checked_div(rtt.as_nanos().max(1)).unwrap_or(0);
        let metrics = conn.quic.congestion_state().metrics();
        Some(PathFacts {
            rtt,
            min_rtt: stats.path.min_rtt,
            cwnd: stats.path.cwnd,
            max_datagram: stats.path.current_mtu,
            delivery_rate: u64::try_from(rate).unwrap_or(u64::MAX),
            receive_window: conn.window.window(),
            // qlog's bits a second, in bytes.
            pacing_rate: metrics.pacing_rate.and_then(|bits| bits.checked_div(8)),
            ssthresh: metrics.ssthresh,
        })
    }

    /// The connection credit a message of `class` to `peer` may spend now: what QUIC would take
    /// on the peer's connection, less the reserve the class leaves for the classes above it.
    pub fn credit(&mut self, peer: PeerId, class: C::Class) -> Option<u64> {
        let key = self.core.connection_of(peer)?;
        let conn = self.conns.get_mut(key)?.as_mut()?;
        Some(credit(&mut conn.quic, C::rank(class)))
    }

    /// What an exchange with `peer` is expected to take, its work included: the tail of the
    /// exchanges it answered over the caller's measured timer `granularity`
    /// (`hyper_timing::Lateness`), doubled for each given up on since (T40). `None` while it has
    /// answered none.
    pub fn exchange_tail(&self, peer: PeerId, granularity: Duration) -> Option<Duration> {
        let at = self
            .core
            .peers
            .binary_search_by_key(&peer, |entry| entry.peer)
            .ok()?;
        self.core.peers.get(at)?.timing.tail(granularity)
    }

    /// The owner's timer granularity `G` moved: the mean lateness of its timed waits
    /// (`hyper_timing::Lateness`, `docs/timing.md` §2.4). A round trip shorter than it is not
    /// resolved when a receive window is tuned; until the first report RFC 9002's `kGranularity`
    /// stands in ([`crate::K_GRANULARITY`]). A zero granularity, a clock that resolved nothing
    /// finer, is ignored.
    pub fn set_granularity(&mut self, granularity: Duration) {
        if !granularity.is_zero() {
            self.core.granularity = granularity;
        }
    }

    /// QUIC's counters for the connection to `peer` (its path's round trip, congestion window,
    /// losses and frames), or `None` without one: what a refused exchange is diagnosed from.
    pub fn connection_stats(&self, peer: PeerId) -> Option<hyper_quic::ConnectionStats> {
        let key = self.core.connection_of(peer)?;
        Some(self.conns.get(key)?.as_ref()?.quic.stats())
    }

    /// Compares every running sum the scheduling core reads with the fold over the connection's
    /// exchanges it replaced (src/tally.rs), counting differences in [`Self::oracle_mismatches`].
    /// A test switch: the fold is what the sums exist to avoid.
    #[cfg(feature = "oracle")]
    pub fn set_oracle(&mut self, on: bool) {
        self.core.oracle = on;
    }

    /// Running sums that differed from their fold while the oracle was on.
    #[cfg(feature = "oracle")]
    pub fn oracle_mismatches(&self) -> u64 {
        self.core.mismatches
    }

    /// How many times the running sums were settled: the oracle's non-vacuity counter.
    #[cfg(feature = "oracle")]
    pub fn oracle_settles(&self) -> u64 {
        self.core.exchanges.settled()
    }

    /// What the endpoint holds and what it refused.
    pub fn stats(&self) -> Stats {
        Stats {
            admission: self.core.admission.stats(),
            exchanges: self.core.exchanges.len(),
            connections: self.conns.iter().flatten().count(),
            events: self.core.events.len(),
            receive_chunks: self.receive.chunks(),
            receive_copied: self.receive.copied(),
            ..self.core.stats
        }
    }

    /// The budget, for the owner's accounting.
    pub fn budget(&self) -> &B {
        &self.core.budget
    }

    /// The directory, for the owner's reads.
    pub fn directory(&self) -> &D {
        &self.core.directory
    }

    /// The directory, for the owner to change between driving calls: a peer it is about to dial
    /// (its server name), an identity granted or revoked. A change applies to the handshakes that
    /// complete after it; a connection already identified keeps its peer and role until the
    /// owner closes it ([`Endpoint::disconnect`]).
    pub fn directory_mut(&mut self) -> &mut D {
        &mut self.core.directory
    }

    fn take(&mut self, key: usize) -> Option<Box<Conn<C::Role>>> {
        self.conns.get_mut(key).and_then(Option::take)
    }

    fn put(&mut self, key: usize, conn: Box<Conn<C::Role>>) {
        if let Some(slot) = self.conns.get_mut(key) {
            *slot = Some(conn);
        }
    }

    /// The owner polled a frame of `peer`'s lane on the connection `key` held under `serial`:
    /// its seats go back, and the peer's lanes paused for want of one are read again.
    fn frame_polled(&mut self, peer: PeerId, (key, serial): (usize, u64), stream: StreamId) {
        let paused = self.core.peer_entry_mut(peer).is_some_and(|entry| {
            entry.frames = entry.frames.saturating_sub(1);
            entry.paused > 0
        });
        if let Some(Some(conn)) = self.conns.get_mut(key)
            && conn.serial == serial
            && let Some(lane) = conn.lanes_in.iter_mut().find(|lane| lane.stream == stream)
        {
            lane.queued = lane.queued.saturating_sub(1);
        }
        if !paused {
            return;
        }
        let mut keys = std::mem::take(&mut self.keys);
        keys.clear();
        keys.extend(self.core.admission.connections_of(peer));
        for &key in &keys {
            let Some(mut conn) = self.take(key) else {
                continue;
            };
            // From the last: reading a lane removes at most that lane.
            for at in (0..conn.lanes_in.len()).rev() {
                let paused = conn.lanes_in.get(at).is_some_and(|lane| lane.paused);
                if paused && self.core.frame_room(&conn, at) {
                    self.core.resume_lane(&mut conn, at);
                }
            }
            self.put(key, conn);
        }
        self.keys = keys;
    }

    fn with_exchange<T>(
        &mut self,
        exchange: ExchangeId,
        work: impl FnOnce(&mut Core<C, B, D>, &mut Conn<C::Role>, u64) -> Result<T, Refusal>,
    ) -> Result<T, Refusal> {
        let key = self
            .core
            .exchanges
            .get(exchange.0)
            .map(|exchange| exchange.connection)
            .ok_or(Refusal::UnknownExchange)?;
        let mut conn = self.take(key).ok_or(Refusal::UnknownExchange)?;
        let done = work(&mut self.core, &mut conn, exchange.0);
        self.core.progress(self.core.now, &mut conn);
        self.put(key, conn);
        done
    }

    fn store(&mut self, key: usize, quic: Connection, dialed: Option<PeerId>, grant: Reservation) {
        let limits = self.core.limits;
        let initial = initial_window(MIN_DATAGRAM);
        let grants = vec![grant];
        let serial = self.serials;
        self.serials = serial.saturating_add(1);
        let conn = Conn {
            quic,
            key,
            serial,
            dialed,
            pending: dialed.is_none(),
            peer: None,
            epoch: 0,
            exchanges: Vec::with_capacity(
                usize::try_from(limits.streams_per_connection).unwrap_or(0),
            ),
            by_stream: Vec::with_capacity(
                usize::try_from(limits.streams_per_connection).unwrap_or(0),
            ),
            credit_moved: None,
            lanes_out: Vec::new(),
            lanes_in: Vec::new(),
            window: Window::new(initial, limits.window_ceiling),
            delivered: Delivered::new(C::RANKS),
            grants,
            lost: false,
        };
        if self.conns.len() <= key {
            self.conns.resize_with(key.saturating_add(1), || None);
        }
        self.put(key, Box::new(conn));
    }

    fn attempt(&mut self, now: Instant, attempt: Attempt, buffer: &mut Vec<u8>) {
        let admission = self.core.admission.stats();
        let crowded = admission.pending.saturating_mul(2) >= self.core.admission.limits().pending;
        // A source that has not proven its address takes no place while half of them are taken
        // (RFC 9000 §8.1.2, Retry under load; focal `validate_address`).
        if crowded && !attempt.remote_address_validated() && attempt.may_retry() {
            buffer.clear();
            if let Ok(transmit) = self.quic.retry(attempt, buffer) {
                self.respond(transmit, buffer);
            }
            return;
        }
        let grant = match self
            .core
            .admission
            .begin()
            .and_then(|()| self.core.window_grant())
        {
            Ok(grant) => grant,
            Err(refusal) => {
                if refusal != Refusal::Pending {
                    self.core.admission.end_pending();
                }
                buffer.clear();
                if let Some(transmit) = self.quic.refuse(attempt, buffer) {
                    self.respond(transmit, buffer);
                }
                return;
            }
        };
        buffer.clear();
        match self.quic.accept(attempt, now, buffer, None, None) {
            Ok((handle, quic)) => {
                self.store(handle.0, quic, None, grant);
                self.pump(now, handle.0);
            }
            Err(error) => {
                self.core.admission.end_pending();
                self.core.budget.release(grant);
                if let Some(transmit) = error.response {
                    self.respond(transmit, buffer);
                }
            }
        }
    }

    fn respond(&mut self, transmit: Transmit, bytes: &[u8]) {
        if self.responses.len() >= self.core.limits.max_responses {
            self.core.stats.responses_dropped = self.core.stats.responses_dropped.saturating_add(1);
            return;
        }
        let mut copy = self.spare.pop().unwrap_or_default();
        copy.clear();
        copy.extend_from_slice(bytes.get(..transmit.size).unwrap_or(bytes));
        self.responses.push_back((transmit, copy));
    }

    fn endpoint_events(&mut self, key: usize) {
        let Some(Some(conn)) = self.conns.get_mut(key) else {
            return;
        };
        while let Some(event) = conn.quic.poll_endpoint_events() {
            if let Some(event) = self
                .quic
                .handle_event(hyper_quic::ConnectionHandle(key), event)
            {
                conn.quic.handle_event(event, self.quic.configs_mut());
            }
        }
    }

    /// Drive connection `key` after anything reached it.
    fn pump(&mut self, now: Instant, key: usize) {
        self.endpoint_events(key);
        let Some(mut conn) = self.take(key) else {
            return;
        };
        while let Some(event) = conn.quic.poll() {
            if let Some(replaced) = self.core.on_event(now, key, &mut conn, event) {
                self.close(now, replaced, Refusal::Connections.code());
            }
        }
        self.core.progress(now, &mut conn);
        self.put(key, conn);
        self.endpoint_events(key);
        let drained = self
            .conns
            .get(key)
            .and_then(Option::as_ref)
            .is_some_and(|conn| conn.quic.is_drained());
        if drained && let Some(conn) = self.take(key) {
            self.core.gone(key, conn);
        }
    }

    /// Close connection `key` with `code`: its exchanges are refused and its owner told now, not
    /// when the close has drained.
    fn close(&mut self, now: Instant, key: usize, code: u32) {
        let Some(mut conn) = self.take(key) else {
            return;
        };
        conn.quic
            .close(now, VarInt::from_u32(code), bytes::Bytes::new());
        self.core.lost(key, &mut conn);
        self.put(key, conn);
        self.endpoint_events(key);
    }
}

impl<C: Classes, B: Budget<C::Class>, D: Directory<Role = C::Role>> Core<C, B, D> {
    fn window_grant(&mut self) -> Result<Reservation, Refusal> {
        let reserve = class_reserve(C::RANKS.saturating_sub(1));
        let bytes = initial_window(MIN_DATAGRAM).saturating_add(reserve);
        self.budget.reserve(bytes, Lane::Window)
    }

    fn connection_of(&self, peer: PeerId) -> Option<usize> {
        let at = self
            .peers
            .binary_search_by_key(&peer, |entry| entry.peer)
            .ok()?;
        self.peers.get(at)?.connection
    }

    /// The entry of `peer`, made if it has none; past the bound the entry used longest ago that
    /// nothing pins ([`PeerEntry::pinned`]) makes room, and with none such there is no entry.
    fn entry(&mut self, peer: PeerId) -> Option<&mut PeerEntry<C::Role>> {
        self.tick = self.tick.saturating_add(1);
        let at = match self.peers.binary_search_by_key(&peer, |entry| entry.peer) {
            Ok(at) => at,
            Err(_) => self.insert_entry(peer)?,
        };
        let tick = self.tick;
        let entry = self.peers.get_mut(at)?;
        entry.used = tick;
        Some(entry)
    }

    fn insert_entry(&mut self, peer: PeerId) -> Option<usize> {
        let bound = self
            .limits
            .admission
            .identities
            .max(self.limits.admission.connections);
        if self.peers.len() >= bound {
            let evict = self
                .peers
                .iter()
                .enumerate()
                .filter(|(_, entry)| !entry.pinned())
                .min_by_key(|(_, entry)| entry.used)
                .map(|(at, _)| at)?;
            self.peers.remove(evict);
        }
        let at = self
            .peers
            .binary_search_by_key(&peer, |entry| entry.peer)
            .err()?;
        self.peers.insert(at, PeerEntry::new(peer, self.tick));
        Some(at)
    }

    /// The entry of `peer`, if it has one; none is made.
    fn peer_entry_mut(&mut self, peer: PeerId) -> Option<&mut PeerEntry<C::Role>> {
        let at = self
            .peers
            .binary_search_by_key(&peer, |entry| entry.peer)
            .ok()?;
        self.peers.get_mut(at)
    }

    /// The peer's lifecycle may have changed: the event of it waiting for the owner, if any, is
    /// replaced by its state now and moved to the back, after every event queued before the
    /// change; a state the owner was already told waits as nothing.
    fn report_lifecycle(&mut self, peer: PeerId) {
        let Ok(at) = self.peers.binary_search_by_key(&peer, |entry| entry.peer) else {
            return;
        };
        let Some(entry) = self.peers.get_mut(at) else {
            return;
        };
        let state = entry.lifecycle();
        if state == entry.waiting {
            return;
        }
        if entry.waiting.take().is_some()
            && let Some(index) = self
                .events
                .iter()
                .position(|waiting| waiting.lifecycle_of(peer))
        {
            let _ = self.events.remove(index);
        }
        let event = match state {
            Some(state) if Some(state) == entry.reported => return,
            Some(Lifecycle::Connected(epoch)) => match entry.role {
                Some(role) => Event::Connected { peer, role, epoch },
                None => return,
            },
            Some(Lifecycle::Closed(epoch, refusal)) => Event::Closed {
                peer,
                epoch,
                refusal,
            },
            Some(Lifecycle::Unreachable(refusal)) => Event::Unreachable { peer, refusal },
            None => return,
        };
        entry.waiting = state;
        self.events.push_back(Waiting { event, lane: None });
    }

    /// The owner polled `peer`'s lifecycle event: what it was told is the state that waited.
    fn lifecycle_polled(&mut self, peer: PeerId) {
        if let Some(entry) = self.peer_entry_mut(peer) {
            entry.reported = entry.waiting.take().or(entry.reported);
        }
    }

    /// Queue an event of exchange `id`, holding its slot; nothing when `id` names no exchange.
    fn push_exchange(&mut self, id: u64, event: Event<C>) {
        if self.exchanges.hold(id) {
            self.events.push_back(Waiting { event, lane: None });
        }
    }

    /// Queue a `BodyReady` of exchange `id`, unless one waits: it says nothing more.
    fn push_body_ready(&mut self, id: u64) {
        let fresh = self.exchanges.hold_if(id, |exchange| {
            !std::mem::replace(&mut exchange.ready_waiting, true)
        });
        if fresh {
            self.events.push_back(Waiting {
                event: Event::BodyReady {
                    exchange: ExchangeId(id),
                },
                lane: None,
            });
        }
    }

    fn on_event(
        &mut self,
        now: Instant,
        key: usize,
        conn: &mut Conn<C::Role>,
        event: QuicEvent,
    ) -> Option<usize> {
        match event {
            QuicEvent::Connected => return self.connected(now, key, conn),
            QuicEvent::ConnectionLost { reason } => {
                let refusal = refusal_of(&reason);
                self.lost_for(key, conn, refusal);
            }
            QuicEvent::Stream(event) => self.on_stream(now, key, conn, event),
            QuicEvent::HandshakeDataReady
            | QuicEvent::DatagramReceived
            | QuicEvent::DatagramsUnblocked => {}
        }
        None
    }

    fn on_stream(
        &mut self,
        now: Instant,
        key: usize,
        conn: &mut Conn<C::Role>,
        event: StreamEvent,
    ) {
        match event {
            StreamEvent::Opened { dir } => self.accept_streams(now, key, conn, dir),
            StreamEvent::Readable { id } => self.readable(now, conn, id),
            StreamEvent::Stopped { id, error_code } => {
                let refusal = Refusal::from_code(error_code.into_inner());
                if let Some(exchange) = self.exchange_on(conn, id) {
                    self.finish_exchange(conn, exchange, Some((refusal, true)));
                } else if let Some(at) = conn
                    .lanes_out
                    .iter()
                    .position(|lane| lane.stream == Some(id))
                {
                    let mut lane = conn.lanes_out.remove(at);
                    for (_, frame) in lane.queue.drain(..) {
                        self.budget.release(frame);
                    }
                }
            }
            StreamEvent::Writable { id } => {
                if let Some(exchange) = self
                    .exchange_on(conn, id)
                    .and_then(|exchange| self.exchanges.get_mut(exchange))
                {
                    exchange.stream_blocked = false;
                }
            }
            StreamEvent::Available { dir: Dir::Bi } => conn.credit_moved = Some(now),
            StreamEvent::Finished { .. } | StreamEvent::Available { .. } => {}
        }
    }

    /// The application protocol the handshake negotiated.
    fn protocol(conn: &Conn<C::Role>) -> Option<Vec<u8>> {
        let data = conn.quic.crypto_session().handshake_data()?;
        data.downcast::<hyper_quic::crypto::rustls::HandshakeData>()
            .ok()?
            .protocol
    }

    /// The certificate the peer authenticated with, its end-entity first.
    fn certificate(conn: &Conn<C::Role>) -> Option<Vec<u8>> {
        let identity = conn.quic.crypto_session().peer_identity()?;
        let chain = identity.downcast::<Vec<CertificateDer<'static>>>().ok()?;
        chain
            .first()
            .map(|certificate| certificate.as_ref().to_vec())
    }

    /// The handshake completed: identify the peer and admit the connection. Returns a connection
    /// this one replaces, for the caller to close.
    fn connected(&mut self, now: Instant, key: usize, conn: &mut Conn<C::Role>) -> Option<usize> {
        if conn.pending {
            conn.pending = false;
            self.admission.end_pending();
        }
        if conn.dialed.is_some() {
            self.dialing = self.dialing.saturating_sub(1);
        }
        let certificate = Self::certificate(conn);
        // A connection negotiates one of the server's protocols or none is up (RFC 9001 §8.1;
        // `Tls::prepare` requires each side to name one).
        let identified = Self::protocol(conn).and_then(|protocol| {
            self.directory.identify(&Authenticated {
                certificate: certificate.as_deref(),
                protocol: &protocol,
                dialed: conn.dialed,
            })
        });
        let Some((peer, role)) =
            identified.filter(|(peer, _)| conn.dialed.is_none_or(|dialed| dialed == *peer))
        else {
            self.admission.unidentified();
            return self.refuse_connection(now, conn, Refusal::Identity);
        };
        // The entry its lifecycle events and frames wait in, before anything is charged: a table
        // whose every entry waits for the owner takes no new peer.
        if self.entry(peer).is_none() {
            return self.refuse_connection(now, conn, Refusal::Events);
        }
        self.tick = self.tick.saturating_add(1);
        let replaced = match self.admission.admit(peer, key, self.tick, now) {
            Ok(replaced) => replaced,
            Err(refusal) => return self.refuse_connection(now, conn, refusal),
        };
        conn.peer = Some((peer, role));
        let dialed = conn.dialed.is_some();
        let epoch = self.entry(peer).map_or(0, |entry| {
            entry.epoch = entry.epoch.saturating_add(1);
            entry.connection = Some(key);
            entry.role = Some(role);
            entry.held = entry.held.saturating_add(1);
            // A connection the peer opened leaves a dial to it in progress, and its entry held.
            entry.dialing &= !dialed;
            entry.unreachable = None;
            entry.epoch
        });
        conn.epoch = epoch;
        self.report_lifecycle(peer);
        self.accept_streams(now, key, conn, Dir::Bi);
        self.accept_streams(now, key, conn, Dir::Uni);
        replaced
    }

    fn refuse_connection(
        &mut self,
        now: Instant,
        conn: &mut Conn<C::Role>,
        refusal: Refusal,
    ) -> Option<usize> {
        conn.quic
            .close(now, VarInt::from_u32(refusal.code()), bytes::Bytes::new());
        if let Some(peer) = conn.dialed {
            conn.lost = true;
            self.dial_failed(peer, Some(refusal));
        }
        None
    }

    /// A dial to `peer` ended before a connection to it was up, for `refusal` when either side
    /// said why: unreachable, unless a connection to it came up meanwhile.
    fn dial_failed(&mut self, peer: PeerId, refusal: Option<Refusal>) {
        if let Some(entry) = self.peer_entry_mut(peer) {
            entry.dialing = false;
            entry.unreachable = entry.connection.is_none().then_some(refusal);
        }
        self.report_lifecycle(peer);
    }

    /// The connection was lost: every exchange on it is refused and its owner told, once.
    fn lost(&mut self, key: usize, conn: &mut Conn<C::Role>) {
        self.lost_for(key, conn, None);
    }

    /// [`Core::lost`], with what the peer or the handshake said when it ended (`refusal_of`): a
    /// dial that ends before its peer is identified reports it.
    fn lost_for(&mut self, key: usize, conn: &mut Conn<C::Role>, refusal: Option<Refusal>) {
        if conn.lost {
            return;
        }
        conn.lost = true;
        self.ids.clear();
        let mut ids = std::mem::take(&mut self.ids);
        ids.extend_from_slice(&conn.exchanges);
        // Refused in the order they were made, as the list kept them before removal reordered it.
        ids.sort_unstable_by_key(|id| self.exchanges.get(*id).map_or(0, |exchange| exchange.order));
        for id in &ids {
            self.finish_exchange(conn, *id, Some((Refusal::Closed, false)));
        }
        self.ids = ids;
        self.release_lanes(conn);
        match (conn.peer, conn.dialed) {
            (Some((peer, _)), _) => {
                self.admission.release(peer, key);
                if let Some(entry) = self.peer_entry_mut(peer) {
                    entry.held = entry.held.saturating_sub(1);
                    if entry.connection == Some(key) {
                        entry.connection = None;
                        entry.closed = refusal;
                    }
                }
                self.report_lifecycle(peer);
            }
            (None, Some(peer)) => {
                self.dialing = self.dialing.saturating_sub(1);
                self.dial_failed(peer, refusal);
            }
            (None, None) => {}
        }
        if conn.pending {
            conn.pending = false;
            self.admission.end_pending();
        }
    }

    /// The connection drained: what it held goes back.
    fn gone(&mut self, key: usize, mut conn: Box<Conn<C::Role>>) {
        self.lost(key, &mut conn);
        for grant in conn.grants.drain(..) {
            self.budget.release(grant);
        }
    }

    fn release_lanes(&mut self, conn: &mut Conn<C::Role>) {
        for mut lane in conn.lanes_out.drain(..) {
            for (_, frame) in lane.queue.drain(..) {
                self.budget.release(frame);
            }
        }
        let peer = conn.peer.map(|(peer, _)| peer);
        for lane in conn.lanes_in.drain(..) {
            self.drop_lane_in(peer, lane);
        }
    }

    fn exchange_on(&self, conn: &Conn<C::Role>, stream: StreamId) -> Option<u64> {
        let at = conn
            .by_stream
            .binary_search_by_key(&stream, |(held, _)| *held)
            .ok()?;
        conn.by_stream.get(at).map(|(_, id)| *id).filter(|id| {
            self.exchanges
                .get(*id)
                .is_some_and(|exchange| exchange.stream == Some(stream))
        })
    }

    fn accept_streams(&mut self, now: Instant, key: usize, conn: &mut Conn<C::Role>, dir: Dir) {
        if conn.peer.is_none() {
            return;
        }
        while let Some(stream) = conn.quic.streams().accept(dir) {
            match dir {
                Dir::Bi => self.served(now, key, conn, stream),
                Dir::Uni => self.lane_from(conn, stream),
            }
        }
    }

    /// A peer opened an exchange.
    fn served(&mut self, now: Instant, key: usize, conn: &mut Conn<C::Role>, stream: StreamId) {
        let Some((peer, _)) = conn.peer else {
            return;
        };
        let mut carry = Carry::idle(self.serve, now, moved(conn, 0));
        let backlog = self.backlog(conn, 0);
        carry.answering(now, moved(conn, 0), length(PREFIX_BYTES), backlog);
        let exchange = Exchange {
            credit_seen: None,
            counted: crate::tally::Counted::default(),
            touched: false,
            order: 0,
            active: false,
            at: 0,
            deadline: None,
            connection: key,
            peer,
            stream: Some(stream),
            opened: false,
            class: None,
            rank: 0,
            out: Outgoing::nothing(),
            incoming: Incoming::new(),
            carry,
            began: now,
            answered: false,
            ready_sent: false,
            ready_waiting: false,
            wants_write: false,
            writable_sent: false,
            writable_waiting: false,
            stream_blocked: false,
            starved: false,
            ended: false,
        };
        match self.exchanges.insert(exchange) {
            Ok(id) => {
                self.exchanges.place(id, conn.exchanges.len());
                conn.exchanges.push(id);
                bind_stream(&mut conn.by_stream, stream, id);
                self.begun(now, key, peer);
                self.read_exchange(now, conn, id);
            }
            Err(refusal) => {
                self.stats.streams_refused = self.stats.streams_refused.saturating_add(1);
                abandon(&mut conn.quic, stream, refusal.code());
            }
        }
    }

    fn readable(&mut self, now: Instant, conn: &mut Conn<C::Role>, stream: StreamId) {
        if stream.dir() == Dir::Uni {
            if let Some(at) = conn.lanes_in.iter().position(|lane| lane.stream == stream) {
                self.read_lane(conn, at);
            }
            return;
        }
        if let Some(id) = self.exchange_on(conn, stream) {
            self.read_exchange(now, conn, id);
        }
    }

    /// Advance the incoming message of exchange `id` as far as what has arrived allows.
    fn read_exchange(&mut self, now: Instant, conn: &mut Conn<C::Role>, id: u64) {
        for _ in 0..IN_STATES {
            match self.read_step(now, conn, id) {
                Ok(true) => {}
                Ok(false) => return,
                Err(failure) => {
                    self.finish_exchange(conn, id, Some(failure));
                    return;
                }
            }
        }
    }

    /// One step of reading: whether the state advanced.
    fn read_step(
        &mut self,
        now: Instant,
        conn: &mut Conn<C::Role>,
        id: u64,
    ) -> Result<bool, (Refusal, bool)> {
        let Some(exchange) = self.exchanges.get_mut(id) else {
            return Ok(false);
        };
        match exchange.incoming.state {
            In::Prefix => self.read_prefix(now, conn, id),
            In::Head => self.read_head(now, conn, id),
            In::Body => {
                if !exchange.ready_sent {
                    exchange.ready_sent = true;
                    self.push_body_ready(id);
                }
                Ok(false)
            }
            In::Trailer => self.read_trailer(conn, id),
            In::Whole => self.read_end(conn, id),
            In::Finished => Ok(false),
        }
    }

    fn read_prefix(
        &mut self,
        now: Instant,
        conn: &mut Conn<C::Role>,
        id: u64,
    ) -> Result<bool, (Refusal, bool)> {
        let exchange = self
            .exchanges
            .get_mut(id)
            .ok_or((Refusal::UnknownExchange, false))?;
        let stream = exchange.stream.ok_or((Refusal::Order, false))?;
        let incoming = &mut exchange.incoming;
        let want = PREFIX_BYTES.saturating_sub(incoming.filled);
        let (got, end) = pull(&mut conn.quic, stream, want, |bytes| {
            let at = incoming.filled;
            if let Some(into) = incoming.prefix.get_mut(at..at.saturating_add(bytes.len())) {
                into.copy_from_slice(bytes);
            }
            incoming.filled = at.saturating_add(bytes.len());
        });
        conn.window.consumed(length(got));
        conn.delivered.add(exchange.rank, length(got));
        ended(end, incoming.filled < PREFIX_BYTES)?;
        if incoming.filled < PREFIX_BYTES {
            return Ok(false);
        }
        let prefix = Prefix::decode(&incoming.prefix).map_err(|refusal| (refusal, false))?;
        self.admit_prefix(now, conn, id, prefix)
            .map_err(|refusal| (refusal, false))?;
        Ok(true)
    }

    /// Check a message's prefix against this side's bounds for its class, and reserve its head.
    fn admit_prefix(
        &mut self,
        now: Instant,
        conn: &mut Conn<C::Role>,
        id: u64,
        prefix: Prefix,
    ) -> Result<(), Refusal> {
        let exchange = self.exchanges.get_mut(id).ok_or(Refusal::UnknownExchange)?;
        let class = if exchange.opened {
            if prefix.kind != 0 {
                return Err(Refusal::Corrupt);
            }
            exchange.class.ok_or(Refusal::Order)?
        } else {
            let role = conn.peer.map(|(_, role)| role).ok_or(Refusal::Identity)?;
            let kind = C::kind_of(prefix.kind).ok_or(Refusal::Kind)?;
            C::class_of(kind, role).ok_or(Refusal::Kind)?
        };
        if prefix.head > self.limits.max_head || prefix.length() > C::frame_bound(class) {
            return Err(Refusal::FrameBound);
        }
        let head = if prefix.head == 0 {
            Reservation::default()
        } else {
            self.budget
                .reserve(u64::from(prefix.head), Lane::Class(class))?
        };
        let exchange = self.exchanges.get_mut(id).ok_or(Refusal::UnknownExchange)?;
        exchange.class = Some(class);
        exchange.rank = C::rank(class);
        exchange.incoming.decoded = Some(prefix);
        exchange.incoming.head = Some(head);
        exchange.incoming.state = In::Head;
        let rank = exchange.rank;
        let (at, backlog) = (moved(conn, rank), self.backlog(conn, rank));
        let exchange = self.exchanges.get_mut(id).ok_or(Refusal::UnknownExchange)?;
        exchange.carry.answering(now, at, prefix.length(), backlog);
        Ok(())
    }

    fn read_head(
        &mut self,
        now: Instant,
        conn: &mut Conn<C::Role>,
        id: u64,
    ) -> Result<bool, (Refusal, bool)> {
        let exchange = self
            .exchanges
            .get_mut(id)
            .ok_or((Refusal::UnknownExchange, false))?;
        let stream = exchange.stream.ok_or((Refusal::Order, false))?;
        let prefix = exchange.incoming.decoded.ok_or((Refusal::Order, false))?;
        let head = exchange
            .incoming
            .head
            .as_mut()
            .ok_or((Refusal::Order, false))?;
        let want = usize::try_from(head.room()).unwrap_or(usize::MAX);
        let (got, end) = pull(&mut conn.quic, stream, want, |bytes| head.fill(bytes));
        conn.window.consumed(length(got));
        conn.delivered.add(exchange.rank, length(got));
        ended(end, head.room() > 0)?;
        if head.room() > 0 {
            return Ok(false);
        }
        prefix
            .verify(&exchange.incoming.prefix, head.bytes())
            .map_err(|refusal| (refusal, false))?;
        self.headed(now, conn, id, prefix);
        Ok(true)
    }

    /// The head arrived whole: the owner hears of the request or the reply.
    fn headed(&mut self, now: Instant, conn: &mut Conn<C::Role>, id: u64, prefix: Prefix) {
        let rank = self.rank_of(id);
        // Taken before the state moves on: the head is in, and its body and trailer still owed.
        let (at, backlog) = (moved(conn, rank), self.backlog(conn, rank));
        let Some(exchange) = self.exchanges.get_mut(id) else {
            return;
        };
        exchange.incoming.state = match prefix.body {
            Some(0) => In::Trailer,
            Some(_) => In::Body,
            None => In::Whole,
        };
        exchange.incoming.left = prefix.body.unwrap_or(0);
        exchange.ready_sent = true;
        match prefix.body {
            Some(body) if body > 0 => exchange.carry.answering(now, at, body, backlog),
            _ => exchange.carry.rest(),
        }
        let (opened, peer, class) = (exchange.opened, exchange.peer, exchange.class);
        let taken = now.saturating_duration_since(exchange.began);
        exchange.answered = opened;
        if opened {
            if let Some(entry) = self.entry(peer) {
                entry.timing.answered(taken);
            }
            self.push_exchange(
                id,
                Event::Reply {
                    exchange: ExchangeId(id),
                    body: prefix.body,
                },
            );
        } else if let (Some(kind), Some(class)) = (C::kind_of(prefix.kind), class) {
            self.push_exchange(
                id,
                Event::Request {
                    exchange: ExchangeId(id),
                    peer,
                    kind,
                    class,
                    body: prefix.body,
                },
            );
        }
    }

    fn read_trailer(&mut self, conn: &mut Conn<C::Role>, id: u64) -> Result<bool, (Refusal, bool)> {
        let exchange = self
            .exchanges
            .get_mut(id)
            .ok_or((Refusal::UnknownExchange, false))?;
        let stream = exchange.stream.ok_or((Refusal::Order, false))?;
        let incoming = &mut exchange.incoming;
        let want = crate::frame::TRAILER_BYTES.saturating_sub(incoming.trailer_filled);
        let (got, end) = pull(&mut conn.quic, stream, want, |bytes| {
            let at = incoming.trailer_filled;
            if let Some(into) = incoming.trailer.get_mut(at..at.saturating_add(bytes.len())) {
                into.copy_from_slice(bytes);
            }
            incoming.trailer_filled = at.saturating_add(bytes.len());
        });
        conn.window.consumed(length(got));
        conn.delivered.add(exchange.rank, length(got));
        let short = incoming.trailer_filled < crate::frame::TRAILER_BYTES;
        ended(end, short)?;
        if short {
            return Ok(false);
        }
        incoming
            .sum
            .verify(incoming.trailer)
            .map_err(|refusal| (refusal, false))?;
        incoming.state = In::Whole;
        exchange.carry.rest();
        self.push_body_ready(id);
        Ok(true)
    }

    /// The message is whole: read the stream's end, so that QUIC frees the stream. A byte past
    /// the message is corrupt.
    fn read_end(&mut self, conn: &mut Conn<C::Role>, id: u64) -> Result<bool, (Refusal, bool)> {
        let exchange = self
            .exchanges
            .get_mut(id)
            .ok_or((Refusal::UnknownExchange, false))?;
        let stream = exchange.stream.ok_or((Refusal::Order, false))?;
        let (got, end) = pull(&mut conn.quic, stream, 1, |_| {});
        if got > 0 {
            return Err((Refusal::Corrupt, false));
        }
        match end {
            End::Finished | End::Closed => {
                exchange.incoming.state = In::Finished;
                Ok(true)
            }
            End::Reset(code) => Err((Refusal::from_code(code), true)),
            End::Open | End::Blocked => Ok(false),
        }
    }

    fn read_body(
        &mut self,
        conn: &mut Conn<C::Role>,
        id: u64,
        into: &mut Reservation,
    ) -> Result<usize, Refusal> {
        let exchange = self.exchanges.get_mut(id).ok_or(Refusal::UnknownExchange)?;
        exchange.ready_sent = false;
        if exchange.incoming.state != In::Body {
            return match exchange.incoming.state {
                In::Prefix | In::Head | In::Trailer | In::Whole | In::Finished => Ok(0),
                In::Body => Err(Refusal::Order),
            };
        }
        let stream = exchange.stream.ok_or(Refusal::Order)?;
        let incoming = &mut exchange.incoming;
        let room = into.room().min(incoming.left);
        let (got, end) = pull(
            &mut conn.quic,
            stream,
            usize::try_from(room).unwrap_or(usize::MAX),
            |bytes| {
                into.fill(bytes);
                incoming.sum.fold(bytes);
            },
        );
        let taken = length(got);
        exchange.starved = got < usize::try_from(room).unwrap_or(usize::MAX);
        incoming.left = incoming.left.saturating_sub(taken);
        exchange.carry.arrived(taken);
        conn.window.consumed(taken);
        conn.delivered.add(exchange.rank, taken);
        let failed = ended(end, incoming.left > 0).err();
        if let Some(failure) = failed {
            self.finish_exchange(conn, id, Some(failure));
            return Err(failure.0);
        }
        if incoming.left == 0 {
            incoming.state = In::Trailer;
            self.read_exchange(self.now, conn, id);
        }
        Ok(got)
    }

    fn open(
        &mut self,
        now: Instant,
        key: usize,
        conn: &mut Conn<C::Role>,
        ask: Ask<'_, C::Class>,
    ) -> Result<ExchangeId, Refusal> {
        let Ask {
            peer,
            kind,
            class,
            header,
            body,
            deadline,
        } = ask;
        if conn.peer.is_none() || conn.lost {
            return Err(Refusal::NotConnected);
        }
        let message = self.message(class, kind, header, body)?;
        let mut out = Outgoing::nothing();
        out.state = Out::Head;
        out.message = Some(message);
        out.left = body.unwrap_or(0);
        out.has_body = body.is_some();
        let exchange = Exchange {
            credit_seen: None,
            counted: crate::tally::Counted::default(),
            touched: false,
            order: 0,
            active: false,
            at: 0,
            deadline: None,
            connection: key,
            peer,
            stream: None,
            opened: true,
            class: Some(class),
            rank: C::rank(class),
            out,
            incoming: Incoming::new(),
            carry: Carry::idle(deadline, now, moved(conn, C::rank(class))),
            began: now,
            answered: false,
            ready_sent: false,
            ready_waiting: false,
            wants_write: false,
            writable_sent: false,
            writable_waiting: false,
            stream_blocked: false,
            starved: false,
            ended: false,
        };
        let id = self.exchanges.insert(exchange)?;
        self.exchanges.place(id, conn.exchanges.len());
        conn.exchanges.push(id);
        let held = self.held(conn);
        let at = moved(conn, C::rank(class));
        if let Some(exchange) = self.exchanges.get_mut(id) {
            exchange.carry.asking(now, at, held);
        }
        self.start(conn, id);
        self.begun(now, key, peer);
        Ok(ExchangeId(id))
    }

    /// An exchange began on connection `key` of `peer`: its identity's most recently used, and
    /// held from replacement until the exchange ends ([`Core::finish_exchange`]).
    fn begun(&mut self, now: Instant, key: usize, peer: PeerId) {
        self.tick = self.tick.saturating_add(1);
        self.admission.begun(peer, key, self.tick, now);
    }

    /// A message's prefix and head, checked against the class's bounds, in one reservation.
    fn message(
        &mut self,
        class: C::Class,
        kind: u16,
        header: &[u8],
        body: Option<u64>,
    ) -> Result<Reservation, Refusal> {
        let head = length(header.len());
        if head > u64::from(self.limits.max_head)
            || head.saturating_add(body.unwrap_or(0)) > C::frame_bound(class)
        {
            return Err(Refusal::FrameBound);
        }
        let prefix = Prefix::encode(kind, header, body)?;
        let mut message = self.budget.reserve(
            length(PREFIX_BYTES).saturating_add(head),
            Lane::Class(class),
        )?;
        message.fill(&prefix);
        message.fill(header);
        Ok(message)
    }

    /// What the exchanges on `conn` have to send.
    fn held(&mut self, conn: &Conn<C::Role>) -> u64 {
        let held = self.exchanges.tally(conn.key).map_or(0, Tally::held);
        #[cfg(feature = "oracle")]
        self.check(held, |core| {
            conn.exchanges
                .iter()
                .filter_map(|id| core.exchanges.get(*id))
                .fold(0u64, |held, exchange| {
                    held.saturating_add(exchange.out.pending())
                })
        });
        held
    }

    /// What the peer has declared on `conn` and not yet delivered, of the classes of `rank` and
    /// the less urgent ones: what the answering wait of a class of `rank` may come after.
    fn backlog(&mut self, conn: &Conn<C::Role>, rank: u8) -> u64 {
        let owed = self
            .exchanges
            .tally(conn.key)
            .map_or(0, |tally| tally.owed_from(rank));
        #[cfg(feature = "oracle")]
        self.check(owed, |core| {
            conn.exchanges
                .iter()
                .filter_map(|id| core.exchanges.get(*id))
                .filter(|exchange| exchange.rank >= rank)
                .fold(0u64, |owed, exchange| {
                    owed.saturating_add(exchange.incoming.owed())
                })
        });
        owed
    }

    /// With the oracle on, counts a visit set differing from the exchanges of `conn` that
    /// `visits` picks, in the order they were made.
    #[cfg(feature = "oracle")]
    fn check_visits(&mut self, conn: &Conn<C::Role>, visited: &[u64]) {
        if !self.oracle {
            return;
        }
        let mut expected: Vec<(u64, u64)> = conn
            .exchanges
            .iter()
            .filter_map(|id| self.exchanges.get(*id).map(|exchange| (exchange, *id)))
            .filter(|(exchange, _)| crate::tally::visits(exchange))
            .map(|(exchange, id)| (exchange.order, id))
            .collect();
        expected.sort_unstable();
        if !expected.iter().map(|(_, id)| id).eq(visited.iter()) {
            self.mismatches = self.mismatches.saturating_add(1);
        }
    }

    /// With the oracle on, counts a due set differing from the exchanges of `conn` whose wait is
    /// due at `now`, in the order they were made.
    #[cfg(feature = "oracle")]
    fn check_due(&mut self, conn: &Conn<C::Role>, now: Instant, due: &[u64]) {
        if !self.oracle {
            return;
        }
        let mut expected: Vec<(u64, u64)> = conn
            .exchanges
            .iter()
            .filter_map(|id| self.exchanges.get(*id).map(|exchange| (exchange, *id)))
            .filter(|(exchange, _)| exchange.carry.due().is_some_and(|at| at <= now))
            .map(|(exchange, id)| (exchange.order, id))
            .collect();
        expected.sort_unstable();
        if !expected.iter().map(|(_, id)| id).eq(due.iter()) {
            self.mismatches = self.mismatches.saturating_add(1);
        }
    }

    /// With the oracle on, counts `tallied` differing from what `fold` finds over every exchange.
    #[cfg(feature = "oracle")]
    fn check(&mut self, tallied: u64, fold: impl FnOnce(&Self) -> u64) {
        if self.oracle && fold(self) != tallied {
            self.mismatches = self.mismatches.saturating_add(1);
        }
    }

    /// Takes exchange `id`, at index `at`, out of `conn`'s list in constant time: the last takes
    /// its place (visits keep the order exchanges were made by their own record of it).
    fn unlist(&mut self, conn: &mut Conn<C::Role>, id: u64, at: usize) {
        if conn.exchanges.get(at) != Some(&id) {
            return;
        }
        conn.exchanges.swap_remove(at);
        if let Some(moved) = conn.exchanges.get(at) {
            self.exchanges.place(*moved, at);
        }
    }

    /// Give exchange `id` a stream if it has none and one is free, then write what it can.
    fn start(&mut self, conn: &mut Conn<C::Role>, id: u64) {
        let Some(exchange) = self.exchanges.get_mut(id) else {
            return;
        };
        if exchange.stream.is_none() {
            let Some(stream) = conn.quic.streams().open(Dir::Bi) else {
                return;
            };
            exchange.stream = Some(stream);
            bind_stream(&mut conn.by_stream, stream, id);
            conn.credit_moved = Some(self.now);
            // Its stream is the queue's advance for it too: the wait runs from the grant, as one given a stream at once.
            exchange.credit_seen = None;
            exchange.carry.hold(self.now, moved(conn, exchange.rank), 0);
            let _ = conn
                .quic
                .send_stream(stream)
                .set_priority(priority::<C>(exchange.rank));
        }
        if let Err(failure) = self.flush(conn, id) {
            self.finish_exchange(conn, id, Some(failure));
        }
    }

    /// The bytes that exchanges and lanes of classes more urgent than `rank` on `conn` still have
    /// to send: what they declared and have not written, whether or not their owner has offered
    /// it yet. Strict priority among classes (T15) is kept where the credit is taken, not only
    /// where QUIC orders what is buffered: a lower class takes only the credit these leave. An
    /// earlier rule counted a more urgent exchange as waiting only once its owner had been refused
    /// a write, so an owner that wrote its bulk body first took the credit its requests were about
    /// to need (`requests_behind_a_bulk_body_take_credit_first_from_a_slow_owner`).
    fn demand_above(&mut self, conn: &Conn<C::Role>, rank: u8) -> u64 {
        let exchanges = self
            .exchanges
            .tally(conn.key)
            .map_or(0, |tally| tally.streamed_above(rank));
        #[cfg(feature = "oracle")]
        self.check(exchanges, |core| {
            conn.exchanges
                .iter()
                .filter_map(|id| core.exchanges.get(*id))
                .filter(|exchange| exchange.rank < rank && exchange.stream.is_some())
                .fold(0u64, |sum, exchange| {
                    sum.saturating_add(exchange.out.pending())
                })
        });
        conn.lanes_out
            .iter()
            .flat_map(|lane| {
                let written = lane.written;
                lane.queue
                    .iter()
                    .enumerate()
                    .map(move |(at, (front, frame))| {
                        let unsent = if at == 0 {
                            frame.bytes().len().saturating_sub(written)
                        } else {
                            frame.bytes().len()
                        };
                        if *front < rank { length(unsent) } else { 0 }
                    })
            })
            .fold(exchanges, u64::saturating_add)
    }

    /// The credit a class of `rank` may take on `conn` now: what QUIC would take, less the reserve
    /// for the classes above and less what those classes still have to send.
    fn allowed(&mut self, conn: &mut Conn<C::Role>, rank: u8) -> u64 {
        let demand = self.demand_above(conn, rank);
        credit(&mut conn.quic, rank).saturating_sub(demand)
    }

    fn rank_of(&self, id: u64) -> u8 {
        self.exchanges.get(id).map_or(0, |exchange| exchange.rank)
    }

    /// Write what exchange `id` has buffered, within the credit its class may spend.
    fn flush(&mut self, conn: &mut Conn<C::Role>, id: u64) -> Result<(), (Refusal, bool)> {
        let allowed = self.allowed(conn, self.rank_of(id));
        let Some(exchange) = self.exchanges.get_mut(id) else {
            return Ok(());
        };
        let Some(stream) = exchange.stream else {
            return Ok(());
        };
        if exchange.out.state == Out::Head {
            let message = exchange
                .out
                .message
                .as_ref()
                .map_or(&[][..], Reservation::bytes);
            let total = message.len();
            let rest = message.get(exchange.out.written..).unwrap_or(&[]);
            let took = pushed(push(&mut conn.quic, stream, rest, allowed))?;
            exchange.out.written = exchange.out.written.saturating_add(took);
            if exchange.out.written < total {
                return Ok(());
            }
            if let Some(message) = exchange.out.message.take() {
                self.budget.release(message);
            }
            let exchange = self
                .exchanges
                .get_mut(id)
                .ok_or((Refusal::UnknownExchange, false))?;
            exchange.out.state = match (exchange.out.has_body, exchange.out.left) {
                (true, 0) => Out::Trailer,
                (true, _) => Out::Body,
                (false, _) => {
                    finish(conn, stream)?;
                    Out::Finished
                }
            };
        }
        self.flush_trailer(conn, id)
    }

    fn flush_trailer(&mut self, conn: &mut Conn<C::Role>, id: u64) -> Result<(), (Refusal, bool)> {
        let allowed = self.allowed(conn, self.rank_of(id));
        let Some(exchange) = self.exchanges.get_mut(id) else {
            return Ok(());
        };
        let Some(stream) = exchange.stream else {
            return Ok(());
        };
        if exchange.out.state != Out::Trailer {
            return Ok(());
        }
        let trailer = exchange.out.sum.trailer();
        let rest = trailer.get(exchange.out.trailer_written..).unwrap_or(&[]);
        let took = pushed(push(&mut conn.quic, stream, rest, allowed))?;
        exchange.out.trailer_written = exchange.out.trailer_written.saturating_add(took);
        if exchange.out.trailer_written >= trailer.len() {
            finish(conn, stream)?;
            exchange.out.state = Out::Finished;
            if !exchange.opened {
                exchange.carry.rest();
            }
            if exchange.ended {
                self.finish_exchange(conn, id, None);
            }
        }
        Ok(())
    }

    fn write_body(
        &mut self,
        conn: &mut Conn<C::Role>,
        id: u64,
        from: &[u8],
    ) -> Result<usize, Refusal> {
        let allowed = self.allowed(conn, self.rank_of(id));
        let exchange = self.exchanges.get_mut(id).ok_or(Refusal::UnknownExchange)?;
        match exchange.out.state {
            Out::Head => {
                exchange.wants_write = true;
                return Ok(0);
            }
            Out::Body => {}
            Out::Nothing | Out::Trailer | Out::Finished => return Err(Refusal::Order),
        }
        if length(from.len()) > exchange.out.left {
            return Err(Refusal::Order);
        }
        let stream = exchange.stream.ok_or(Refusal::Order)?;
        let took = match pushed(push(&mut conn.quic, stream, from, allowed)) {
            Ok(took) => took,
            Err(failure) => {
                self.finish_exchange(conn, id, Some(failure));
                return Err(failure.0);
            }
        };
        exchange.out.sum.fold(from.get(..took).unwrap_or(&[]));
        exchange.out.left = exchange.out.left.saturating_sub(length(took));
        exchange.wants_write = took < from.len();
        exchange.writable_sent = false;
        // Fewer taken than the credit allowed: the stream's own window refused the rest.
        exchange.stream_blocked = took < from.len() && length(took) < allowed;
        if exchange.out.left == 0 {
            exchange.out.state = Out::Trailer;
            if let Err(failure) = self.flush_trailer(conn, id) {
                self.finish_exchange(conn, id, Some(failure));
                return Err(failure.0);
            }
        }
        Ok(took)
    }

    fn reply(
        &mut self,
        conn: &mut Conn<C::Role>,
        id: u64,
        header: &[u8],
        body: Option<u64>,
    ) -> Result<(), Refusal> {
        let exchange = self.exchanges.get(id).ok_or(Refusal::UnknownExchange)?;
        let arrived = !matches!(exchange.incoming.state, In::Prefix | In::Head);
        if exchange.opened || exchange.out.state != Out::Nothing || !arrived {
            return Err(Refusal::Order);
        }
        let class = exchange.class.ok_or(Refusal::Order)?;
        let message = self.message(class, 0, header, body)?;
        let now = self.now;
        let exchange = self.exchanges.get_mut(id).ok_or(Refusal::UnknownExchange)?;
        exchange.out.state = Out::Head;
        exchange.out.message = Some(message);
        exchange.out.left = body.unwrap_or(0);
        exchange.out.has_body = body.is_some();
        let held = self.held(conn);
        let at = moved(conn, C::rank(class));
        if let Some(exchange) = self.exchanges.get_mut(id) {
            exchange.carry.asking(now, at, held);
        }
        if let Err(failure) = self.flush(conn, id) {
            self.finish_exchange(conn, id, Some(failure));
            return Err(failure.0);
        }
        Ok(())
    }

    /// The owner ends exchange `id`. When the peer's message is whole and all this side has left
    /// to send is its trailer, which credit did not take when the body's last byte was written,
    /// the exchange waits for the trailer to go; resetting it would refuse the peer a message its
    /// owner wrote whole.
    fn end(&mut self, conn: &mut Conn<C::Role>, id: u64) {
        let trailer_left = self.exchanges.get_mut(id).is_some_and(|exchange| {
            exchange.ended = exchange.out.state == Out::Trailer
                && matches!(exchange.incoming.state, In::Whole | In::Finished);
            exchange.ended
        });
        if trailer_left {
            if let Some(exchange) = self.exchanges.get_mut(id)
                && let Some(head) = exchange.incoming.head.take()
            {
                self.budget.release(head);
            }
            if let Err(failure) = self.flush_trailer(conn, id) {
                self.finish_exchange(conn, id, Some(failure));
            }
        } else {
            self.finish_exchange(conn, id, None);
        }
    }

    /// End exchange `id` refused for `refusal`, by its owner: the halves not yet complete are reset
    /// with the refusal's code, and the owner, which decided it, is told nothing (an exchange its
    /// owner ended raises no event).
    fn refuse(&mut self, conn: &mut Conn<C::Role>, id: u64, refusal: Refusal) {
        if let Some(exchange) = self.exchanges.get_mut(id) {
            exchange.ended = true;
        }
        self.finish_exchange(conn, id, Some((refusal, false)));
    }

    /// End exchange `id`: with `failure`, the owner hears it was refused. A half not yet complete
    /// is reset with the refusal's code, so the peer hears it too.
    fn finish_exchange(
        &mut self,
        conn: &mut Conn<C::Role>,
        id: u64,
        failure: Option<(Refusal, bool)>,
    ) {
        // The refusal's seat is the exchange's slot, held before the exchange is removed from it.
        let refused = failure.filter(|_| {
            self.exchanges
                .get(id)
                .is_some_and(|exchange| !exchange.ended)
        });
        let held = refused.is_some() && self.exchanges.hold(id);
        let Some(exchange) = self.exchanges.remove(id) else {
            return;
        };
        self.admission
            .ended(exchange.peer, exchange.connection, self.now);
        self.unlist(conn, id, exchange.at);
        // Only an exchange that held a stream frees credit; one refused while queued moves no queue.
        if exchange.stream.is_some() {
            conn.credit_moved = Some(self.now);
        }
        if let Some(stream) = exchange.stream
            && let Ok(at) = conn
                .by_stream
                .binary_search_by_key(&stream, |(held, _)| *held)
        {
            conn.by_stream.remove(at);
        }
        let code = failure
            .map_or(Refusal::Closed, |(refusal, _)| refusal)
            .code();
        if let Some(stream) = exchange.stream {
            if exchange.out.state != Out::Finished {
                let _ = conn.quic.send_stream(stream).reset(VarInt::from_u32(code));
            }
            if exchange.incoming.state != In::Finished {
                let _ = conn.quic.recv_stream(stream).stop(VarInt::from_u32(code));
            }
        }
        if exchange.opened
            && !exchange.answered
            && let Some(entry) = self.entry(exchange.peer)
        {
            entry.timing.abandoned();
        }
        for reservation in [exchange.out.message, exchange.incoming.head]
            .into_iter()
            .flatten()
        {
            self.budget.release(reservation);
        }
        if let Some((refusal, by_peer)) = refused
            && held
        {
            self.events.push_back(Waiting {
                event: Event::Refused {
                    exchange: ExchangeId(id),
                    refusal,
                    by_peer,
                },
                lane: None,
            });
        }
    }

    /// After anything reached `conn`: start waiting exchanges, write what is buffered, wake
    /// writers that can write again, judge deadlines, write lanes, and tune the window.
    fn progress(&mut self, now: Instant, conn: &mut Conn<C::Role>) {
        if conn.peer.is_none() || conn.lost {
            return;
        }
        let mut ids = std::mem::take(&mut self.ids);
        ids.clear();
        // Only the exchanges `start` or `wake` can move (src/tally.rs `visits`), in the order they
        // were made: for every other, both do nothing.
        self.exchanges.active(conn.key, &mut ids);
        #[cfg(feature = "oracle")]
        self.check_visits(conn, &ids);
        // Streams are given in that order: once one is refused, every later one would be.
        let mut streams_left = true;
        for id in &ids {
            let queued = |core: &Self| {
                core.exchanges
                    .get(*id)
                    .is_some_and(|exchange| exchange.stream.is_none())
            };
            if !streams_left && queued(self) {
                continue;
            }
            self.start(conn, *id);
            if queued(self) {
                streams_left = false;
            }
            self.wake(conn, *id);
        }
        ids.clear();
        // Only the exchanges whose wait is due, from the connection's deadline heap, in the order
        // they were made: `judge` does nothing for any other.
        self.exchanges.due(conn.key, now, &mut ids);
        #[cfg(feature = "oracle")]
        self.check_due(conn, now, &ids);
        for id in &ids {
            self.judge(now, conn, *id);
        }
        self.ids = ids;
        self.flush_lanes(conn);
        self.tune(now, conn);
    }

    fn wake(&mut self, conn: &mut Conn<C::Role>, id: u64) {
        let wanted = self.exchanges.get(id).is_some_and(|exchange| {
            exchange.wants_write
                && !exchange.writable_sent
                && !exchange.stream_blocked
                && exchange.out.state == Out::Body
        });
        if !wanted || self.allowed(conn, self.rank_of(id)) == 0 {
            return;
        }
        let fresh = self.exchanges.hold_if(id, |exchange| {
            exchange.writable_sent = true;
            !std::mem::replace(&mut exchange.writable_waiting, true)
        });
        if fresh {
            self.events.push_back(Waiting {
                event: Event::Writable {
                    exchange: ExchangeId(id),
                },
                lane: None,
            });
        }
    }

    fn judge(&mut self, now: Instant, conn: &mut Conn<C::Role>, id: u64) {
        let due = self
            .exchanges
            .get(id)
            .and_then(|exchange| exchange.carry.due())
            .is_some_and(|due| due <= now);
        if !due {
            return;
        }
        let rank = self.rank_of(id);
        let held = self.held(conn);
        let backlog = self.backlog(conn, rank);
        let at = moved(conn, rank);
        // What the peer's credit would take of this exchange's class now, before this side's own
        // classes are served: none means the peer is not taking what the exchange has to send.
        let peer_takes = credit(&mut conn.quic, rank);
        let credit_moved = conn.credit_moved;
        let judged = self.exchanges.get_mut(id).map_or(Ok(()), |exchange| {
            if exchange.stream.is_none() {
                // Queued for a stream (policy C): judged on whether the connection's credit queue moves, not on its
                // own age. A further advance restarts the wait from it; none for a whole period refuses it.
                return match credit_moved {
                    Some(advanced) if exchange.credit_seen != Some(advanced) => {
                        exchange.credit_seen = Some(advanced);
                        exchange.carry.hold_from(advanced, at, backlog);
                        Ok(())
                    }
                    _ => Err(Refusal::Stalled),
                };
            }
            if Self::ours_to_move(exchange, peer_takes) {
                exchange.carry.hold(now, at, backlog);
                return Ok(());
            }
            exchange.carry.judge(now, at, held, backlog)
        });
        if let Err(refusal) = judged {
            self.finish_exchange(conn, id, Some((refusal, false)));
        }
    }

    /// Whether a period that moved nothing for `exchange` is this side's doing, not the peer's,
    /// and so no evidence against the peer: its owner is not reading the body it is answered with,
    /// or it has bytes to send that the peer's credit would take (`peer_takes`) and this side has
    /// not sent, because its owner has not offered them or a more urgent class of this side's
    /// took the credit first. Only a peer that stops taking or answering ends an exchange.
    fn ours_to_move(exchange: &Exchange<C::Class>, peer_takes: u64) -> bool {
        let unread = exchange.incoming.state == In::Body && !exchange.starved;
        let sending = matches!(exchange.out.state, Out::Head | Out::Body | Out::Trailer);
        let unsent = sending && exchange.stream.is_some() && peer_takes > 0;
        unread || unsent
    }

    /// Grow the receive window if the owner consumed a whole one within two round trips and the
    /// budget funds the growth: the window is `min(BDP, share)` (node.md §3.3).
    fn tune(&mut self, now: Instant, conn: &mut Conn<C::Role>) {
        let Some(grown) = conn.window.tune(now, conn.quic.rtt(), self.granularity) else {
            return;
        };
        let more = grown.saturating_sub(conn.window.window());
        let Ok(grant) = self.budget.reserve(more, Lane::Window) else {
            return;
        };
        conn.grants.push(grant);
        conn.window.grew(grown);
        let reserve = class_reserve(C::RANKS.saturating_sub(1));
        if let Ok(window) = VarInt::from_u64(grown.saturating_add(reserve)) {
            conn.quic.set_receive_window(window);
        }
    }

    fn send_frame(
        &mut self,
        conn: &mut Conn<C::Role>,
        lane: crate::LaneId,
        (kind, class): (u16, C::Class),
        frame: &[u8],
    ) -> Result<(), Refusal> {
        if conn.peer.is_none() || conn.lost {
            return Err(Refusal::NotConnected);
        }
        let at = match conn.lanes_out.iter().position(|out| out.lane == lane) {
            Some(at) => at,
            None => {
                let bound = usize::try_from(self.limits.lanes_per_peer).unwrap_or(usize::MAX);
                if conn.lanes_out.len() >= bound {
                    return Err(Refusal::Lanes);
                }
                conn.lanes_out
                    .push(LaneOut::new(lane, self.limits.lane_window));
                conn.lanes_out.len().saturating_sub(1)
            }
        };
        let full = conn
            .lanes_out
            .get(at)
            .is_none_or(|out| out.queue.len() >= self.limits.lane_window);
        if full {
            return Err(Refusal::LaneFull);
        }
        let prefix = Prefix::encode(kind, frame, None)?;
        let mut bytes = self.budget.reserve(
            length(PREFIX_BYTES).saturating_add(length(frame.len())),
            Lane::Class(class),
        )?;
        bytes.fill(&prefix);
        bytes.fill(frame);
        if let Some(out) = conn.lanes_out.get_mut(at) {
            out.queue.push_back((C::rank(class), bytes));
        }
        // A connection carrying frames is in use as one carrying exchanges is.
        if let Some((peer, _)) = conn.peer {
            self.admission.carried(peer, conn.key, self.now);
        }
        self.flush_lane(conn, at);
        Ok(())
    }

    fn flush_lanes(&mut self, conn: &mut Conn<C::Role>) {
        for at in 0..conn.lanes_out.len() {
            self.flush_lane(conn, at);
        }
    }

    /// Write lane `at`'s opening and as many of its frames as credit allows, in order.
    fn flush_lane(&mut self, conn: &mut Conn<C::Role>, at: usize) {
        let Some(out) = conn.lanes_out.get_mut(at) else {
            return;
        };
        if out.queue.is_empty() {
            return;
        }
        let stream = match out.stream {
            Some(stream) => stream,
            None => {
                let Some(stream) = conn.quic.streams().open(Dir::Uni) else {
                    return;
                };
                out.stream = Some(stream);
                stream
            }
        };
        let opener = crate::lane::opener(out.lane);
        if out.opener_written < opener.len() {
            let rest = opener.get(out.opener_written..).unwrap_or(&[]);
            let Pushed::Took(took) = push(&mut conn.quic, stream, rest, u64::MAX) else {
                return;
            };
            out.opener_written = out.opener_written.saturating_add(took);
            if out.opener_written < opener.len() {
                return;
            }
        }
        self.flush_frames(conn, at, stream);
    }

    /// Write lane `at`'s frames in order while credit allows; each step writes a frame whole or
    /// stops, so the steps are bounded by the lane's window.
    fn flush_frames(&mut self, conn: &mut Conn<C::Role>, at: usize, stream: StreamId) {
        for _ in 0..self.limits.lane_window {
            let front = conn
                .lanes_out
                .get(at)
                .and_then(|out| out.queue.front().map(|(rank, _)| *rank));
            let Some(rank) = front else {
                return;
            };
            let allowed = self.allowed(conn, rank);
            let Some(out) = conn.lanes_out.get_mut(at) else {
                return;
            };
            let Some((_, frame)) = out.queue.front() else {
                return;
            };
            let _ = conn
                .quic
                .send_stream(stream)
                .set_priority(priority::<C>(rank));
            let rest = frame.bytes().get(out.written..).unwrap_or(&[]);
            let whole = rest.len();
            let Pushed::Took(took) = push(&mut conn.quic, stream, rest, allowed) else {
                return;
            };
            if took < whole {
                out.written = out.written.saturating_add(took);
                return;
            }
            out.written = 0;
            if let Some((_, frame)) = out.queue.pop_front() {
                self.budget.release(frame);
            }
        }
    }

    /// A peer opened a lane.
    fn lane_from(&mut self, conn: &mut Conn<C::Role>, stream: StreamId) {
        let bound = usize::try_from(self.limits.lanes_per_peer).unwrap_or(usize::MAX);
        if conn.lanes_in.len() >= bound {
            self.stats.streams_refused = self.stats.streams_refused.saturating_add(1);
            let _ = conn
                .quic
                .recv_stream(stream)
                .stop(VarInt::from_u32(Refusal::Lanes.code()));
            return;
        }
        conn.lanes_in.push(LaneIn::new(stream));
        let at = conn.lanes_in.len().saturating_sub(1);
        self.read_lane(conn, at);
    }
}

impl<C: Classes, B: Budget<C::Class>, D: Directory<Role = C::Role>> Core<C, B, D> {
    /// Read lane `at` as far as what has arrived allows. Every step consumes a byte or stops, and
    /// what is buffered is bounded by the stream window, so the steps are too.
    fn read_lane(&mut self, conn: &mut Conn<C::Role>, at: usize) {
        for _ in 0..stream_window_ceiling() {
            match self.lane_step(conn, at) {
                Ok(true) => {}
                Ok(false) => return,
                Err(refusal) => {
                    if at < conn.lanes_in.len() {
                        let lane = conn.lanes_in.remove(at);
                        let _ = conn
                            .quic
                            .recv_stream(lane.stream)
                            .stop(VarInt::from_u32(refusal.code()));
                        self.drop_lane_in(conn.peer.map(|(peer, _)| peer), lane);
                    }
                    return;
                }
            }
        }
    }

    /// One step of a lane: whether it advanced.
    fn lane_step(&mut self, conn: &mut Conn<C::Role>, at: usize) -> Result<bool, Refusal> {
        let state = conn
            .lanes_in
            .get(at)
            .map(|lane| lane.state)
            .ok_or(Refusal::Lanes)?;
        match state {
            Reading::Opener => lane_opener(conn, at),
            Reading::Prefix if self.frame_room(conn, at) => self.lane_prefix(conn, at),
            Reading::Prefix => {
                self.pause_lane(conn, at);
                Ok(false)
            }
            Reading::Frame => self.lane_frame(conn, at),
            Reading::Skipping => self.lane_skip(conn, at),
        }
    }

    /// Whether lane `at` of `conn` has seats for its next frame: one of its window, the core's
    /// (T37), and one of its peer's, every lane the peer may open at the core's window.
    fn frame_room(&self, conn: &Conn<C::Role>, at: usize) -> bool {
        let (Some(lane), Some((peer, _))) = (conn.lanes_in.get(at), conn.peer) else {
            return false;
        };
        let Ok(index) = self.peers.binary_search_by_key(&peer, |entry| entry.peer) else {
            return false;
        };
        let lanes = usize::try_from(self.limits.lanes_per_peer).unwrap_or(usize::MAX);
        let peer_frames = lanes.saturating_mul(self.limits.lane_window);
        lane.queued < self.limits.lane_window
            && self
                .peers
                .get(index)
                .is_some_and(|entry| entry.frames < peer_frames)
    }

    /// Lane `at` of `conn` waits for a seat: it is not read, and its bytes wait in QUIC, until
    /// the owner polls one of its peer's frames.
    fn pause_lane(&mut self, conn: &mut Conn<C::Role>, at: usize) {
        let Some(lane) = conn.lanes_in.get_mut(at) else {
            return;
        };
        if std::mem::replace(&mut lane.paused, true) {
            return;
        }
        if let Some((peer, _)) = conn.peer
            && let Some(entry) = self.peer_entry_mut(peer)
        {
            entry.paused = entry.paused.saturating_add(1);
        }
    }

    /// Lane `at` of `conn` has its seats again: it is read.
    fn resume_lane(&mut self, conn: &mut Conn<C::Role>, at: usize) {
        let resumed = conn
            .lanes_in
            .get_mut(at)
            .is_some_and(|lane| std::mem::replace(&mut lane.paused, false));
        if resumed
            && let Some((peer, _)) = conn.peer
            && let Some(entry) = self.peer_entry_mut(peer)
        {
            entry.paused = entry.paused.saturating_sub(1);
        }
        self.read_lane(conn, at);
    }

    /// A lane from `peer` is gone: its frame in progress goes back, and its pause with it.
    fn drop_lane_in(&mut self, peer: Option<PeerId>, lane: LaneIn) {
        if let Some(frame) = lane.frame {
            self.budget.release(frame);
        }
        if lane.paused
            && let Some(entry) = peer.and_then(|peer| self.peer_entry_mut(peer))
        {
            entry.paused = entry.paused.saturating_sub(1);
        }
    }

    fn lane_prefix(&mut self, conn: &mut Conn<C::Role>, at: usize) -> Result<bool, Refusal> {
        let lane = conn.lanes_in.get_mut(at).ok_or(Refusal::Lanes)?;
        let want = PREFIX_BYTES.saturating_sub(lane.filled);
        let (got, end) = pull(&mut conn.quic, lane.stream, want, |bytes| {
            let from = lane.filled;
            if let Some(into) = lane.prefix.get_mut(from..from.saturating_add(bytes.len())) {
                into.copy_from_slice(bytes);
            }
            lane.filled = from.saturating_add(bytes.len());
        });
        conn.window.consumed(length(got));
        conn.delivered.add(0, length(got));
        lane_ended(end)?;
        if lane.filled < PREFIX_BYTES {
            return Ok(false);
        }
        lane.filled = 0;
        let prefix = Prefix::decode(&lane.prefix)?;
        if prefix.body.is_some() {
            return Err(Refusal::Corrupt);
        }
        lane.decoded = Some(prefix);
        lane.kind = prefix.kind;
        lane.left = u64::from(prefix.head);
        let role = conn.peer.map(|(_, role)| role).ok_or(Refusal::Identity)?;
        let class = C::kind_of(prefix.kind).and_then(|kind| C::class_of(kind, role));
        let fits = class.filter(|class| prefix.length() <= C::frame_bound(*class));
        let frame = fits.and_then(|class| {
            self.budget
                .reserve(prefix.length(), Lane::Class(class))
                .ok()
        });
        let lane = conn.lanes_in.get_mut(at).ok_or(Refusal::Lanes)?;
        lane.rank = fits.map_or(0, C::rank);
        lane.state = if frame.is_some() {
            Reading::Frame
        } else {
            Reading::Skipping
        };
        lane.frame = frame;
        Ok(true)
    }

    fn lane_frame(&mut self, conn: &mut Conn<C::Role>, at: usize) -> Result<bool, Refusal> {
        let lane = conn.lanes_in.get_mut(at).ok_or(Refusal::Lanes)?;
        let frame = lane.frame.as_mut().ok_or(Refusal::Order)?;
        let want = usize::try_from(lane.left).unwrap_or(usize::MAX);
        let (got, end) = pull(&mut conn.quic, lane.stream, want, |bytes| frame.fill(bytes));
        conn.window.consumed(length(got));
        conn.delivered.add(lane.rank, length(got));
        lane.left = lane.left.saturating_sub(length(got));
        lane_ended(end)?;
        if lane.left > 0 {
            return Ok(false);
        }
        let frame = lane.frame.take().ok_or(Refusal::Order)?;
        lane.state = Reading::Prefix;
        let verified = lane
            .decoded
            .ok_or(Refusal::Order)?
            .verify(&lane.prefix, frame.bytes());
        let (peer, kind) = (conn.peer.map(|(peer, _)| peer), C::kind_of(lane.kind));
        let (lane_id, stream) = (lane.lane, lane.stream);
        match (verified, peer, kind) {
            (Ok(()), Some(peer), Some(kind)) => {
                // Its seats were found free before its prefix was read (`lane_step`).
                lane.queued = lane.queued.saturating_add(1);
                if let Some(entry) = self.peer_entry_mut(peer) {
                    entry.frames = entry.frames.saturating_add(1);
                }
                self.admission.carried(peer, conn.key, self.now);
                self.events.push_back(Waiting {
                    event: Event::Frame {
                        peer,
                        lane: lane_id,
                        kind,
                        frame,
                    },
                    lane: Some((conn.key, conn.serial, stream)),
                });
                Ok(true)
            }
            _ => {
                self.budget.release(frame);
                Err(Refusal::Corrupt)
            }
        }
    }

    fn lane_skip(&mut self, conn: &mut Conn<C::Role>, at: usize) -> Result<bool, Refusal> {
        let lane = conn.lanes_in.get_mut(at).ok_or(Refusal::Lanes)?;
        let want = usize::try_from(lane.left).unwrap_or(usize::MAX);
        let (got, end) = pull(&mut conn.quic, lane.stream, want, |_| {});
        conn.window.consumed(length(got));
        conn.delivered.add(0, length(got));
        lane.left = lane.left.saturating_sub(length(got));
        lane_ended(end)?;
        if lane.left > 0 {
            return Ok(false);
        }
        lane.state = Reading::Prefix;
        self.stats.frames_dropped = self.stats.frames_dropped.saturating_add(1);
        Ok(true)
    }
}

fn lane_opener<R>(conn: &mut Conn<R>, at: usize) -> Result<bool, Refusal> {
    let lane = conn.lanes_in.get_mut(at).ok_or(Refusal::Lanes)?;
    let want = OPENER_BYTES.saturating_sub(lane.filled);
    let (got, end) = pull(&mut conn.quic, lane.stream, want, |bytes| {
        let from = lane.filled;
        if let Some(into) = lane.opener.get_mut(from..from.saturating_add(bytes.len())) {
            into.copy_from_slice(bytes);
        }
        lane.filled = from.saturating_add(bytes.len());
    });
    conn.window.consumed(length(got));
    conn.delivered.add(0, length(got));
    lane_ended(end)?;
    if lane.filled < OPENER_BYTES {
        return Ok(false);
    }
    lane.lane = opened(lane.opener)?;
    lane.filled = 0;
    lane.state = Reading::Prefix;
    Ok(true)
}

/// A lane ends when its peer finishes or resets it; until then it stops only for want of bytes.
fn lane_ended(end: End) -> Result<(), Refusal> {
    match end {
        End::Open | End::Blocked => Ok(()),
        End::Finished | End::Closed => Err(Refusal::Closed),
        End::Reset(code) => Err(Refusal::from_code(code)),
    }
}

/// What a read's end says about the message: an error when the peer reset the stream, or when
/// the stream ended or vanished while `short` of what the message declared.
fn ended(end: End, short: bool) -> Result<(), (Refusal, bool)> {
    match end {
        End::Reset(code) => Err((Refusal::from_code(code), true)),
        End::Finished | End::Closed if short => Err((Refusal::Corrupt, false)),
        End::Open | End::Blocked | End::Finished | End::Closed => Ok(()),
    }
}

fn pushed(pushed: Pushed) -> Result<usize, (Refusal, bool)> {
    match pushed {
        Pushed::Took(took) => Ok(took),
        Pushed::Stopped(code) => Err((Refusal::from_code(code), true)),
        Pushed::Closed => Err((Refusal::Closed, false)),
    }
}

fn finish(conn: &mut Conn<impl Sized>, stream: StreamId) -> Result<(), (Refusal, bool)> {
    conn.quic
        .send_stream(stream)
        .finish()
        .map_err(|_| (Refusal::Closed, false))
}

/// The connection credit a class of `rank` may spend now: what QUIC would take, less the reserve
/// it leaves for the classes above it.
fn credit(connection: &mut Connection, rank: u8) -> u64 {
    connection
        .streams()
        .write_limit()
        .saturating_sub(class_reserve(rank))
}

/// QUIC's stream priority for a class of `rank`: higher is sent first, so the most urgent class
/// is the highest (quinn's `set_priority`).
fn priority<C: Classes>(rank: u8) -> i32 {
    i32::from(C::RANKS)
        .saturating_sub(1)
        .saturating_sub(i32::from(rank))
}

/// Records that exchange `id` holds `stream`, keeping `by_stream` sorted by stream (a stream is bound once).
fn bind_stream(by_stream: &mut Vec<(StreamId, u64)>, stream: StreamId, id: u64) {
    if let Err(at) = by_stream.binary_search_by_key(&stream, |(held, _)| *held) {
        by_stream.insert(at, (stream, id));
    }
}
