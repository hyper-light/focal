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

use std::collections::{HashSet, VecDeque};
use std::marker::PhantomData;
use std::net::SocketAddr;
use std::num::NonZeroUsize;
use std::time::{Duration, Instant};

use hyper_quic::congestion::Congestion;
use hyper_quic::rustls::pki_types::CertificateDer;
use hyper_quic::{
    ClientConfigHandle, Connection, ConnectionError, ConnectionId, DatagramEvent, Dir,
    EcnCodepoint, EndpointConfig, Event as QuicEvent, IdleTimeout, Incoming as Attempt,
    MtuDiscoveryConfig, ServerConfig, StreamEvent, StreamId, Transmit, TransportConfig,
    TransportErrorCode, VarInt,
};

use crate::admission::{Admission, AdmissionLimits, AdmissionStats};
use crate::budget::{Budget, Lane, Reservation};
use crate::charge::{Unprompted, probes_within};
use crate::credit::{
    K_GRANULARITY, MIN_DATAGRAM, Room, Window, class_reserve, initial_window, stream_window_ceiling,
};
use crate::deadlines;
use crate::exchange::{End, Exchange, In, Incoming, Out, Outgoing, Pushed, abandon, pull, push};
use crate::frame::{PREFIX_BYTES, Prefix};
use crate::lane::{LaneIn, LaneOut, OPENER_BYTES, Reading, opened};
use crate::pool::{Pool, PoolStats, Share};
use crate::progress::{Carry, Delivered, LEAST_PROGRESS, Moved, Progress};
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
    /// How a connection is given the exchanges its peer may open at once: every one from its
    /// handshake, or as its peer wants them from a pool the owner sizes.
    pub stream_credit: StreamCredit,
    /// How many of the exchanges this side asks may wait for a stream on one connection.
    pub ask_queue: AskQueue,
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
    /// leave any of them unsent. Each answers one datagram the endpoint took since its transmits
    /// were last polled, so an owner that polls them after each of its receives states what one
    /// receive hands the endpoint, and none of a burst's is dropped: quinn sends each as its
    /// datagram is handled, dropping it only for a full socket. A dial whose answer is dropped
    /// hears nothing until its probe timeout, and dials begun together probe again together:
    /// eight joiners of an enrollment sponsor with one handshake place, answered one at a time
    /// from a bound of one, failed their joins after 10 s of probes (focal, 2026-10-19). The
    /// queue grows as responses come and is never laid out at the bound.
    pub max_responses: usize,
    /// The chunks of [`crate::RECEIVE_CHUNK`] bytes received datagrams are cut from, each reserved
    /// from the budget: the chunk memory a peer can pin is at most this many chunks, whatever it
    /// sends; past them a datagram is copied into a buffer of its own (`crate::receive`). A full
    /// receive window of unread data, packed, occupies `window_ceiling / RECEIVE_CHUNK` chunks; the
    /// owner sets this from that and its budget.
    pub receive_chunks: usize,
}

/// How connections are given the exchanges their peers may open at once (QUIC's bidirectional
/// stream credit, RFC 9000 §4.6; docs/transport.md §4g).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StreamCredit {
    /// Every connection holds [`Limits::streams_per_connection`] from its handshake on.
    Fixed,
    /// The connections of a pooled role ([`Classes::pooled`]) share a pool the owner sizes
    /// ([`Endpoint::set_stream_pool`]): each is granted streams as its peer is blocked at its grant,
    /// so a peer holds back what the endpoint cannot take where it sent it to be refused (admission
    /// credit as Breakwater's, `crate::pool`). A connection's handshake grants `initial`, at most
    /// the streams per connection: a connection that is not pooled is raised to those once its
    /// role is known.
    Pooled {
        /// What every connection's handshake grants: what a new peer may open before it is heard.
        initial: u32,
    },
}

/// What bounds the exchanges this side asks that wait for a stream on a connection (docs/transport.md
/// §4g).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AskQueue {
    /// The exchange table alone ([`Limits::exchanges`]): an exchange waits for a stream as long as
    /// its connection's credit queue moves (policy C).
    Table,
    /// One round of the streams the peer grants the connection: those this side holds and those
    /// it may still open. An exchange asked while the connection already waits for that many is
    /// refused [`Refusal::Unsent`] at once, nothing sent. A peer that grants streams as wanted
    /// ([`StreamCredit::Pooled`]) holds back what it cannot take; past a round the asker's queue
    /// would only wait longer, as the peer's own admission refuses a queue past what it answers in
    /// its bound, so the wait an asked exchange meets is held to about a round of its credit.
    Credit,
}

impl Limits {
    /// The most events the endpoint holds for its owner at once: [`EXCHANGE_EVENTS`] for every
    /// exchange the table holds, and, for every entry of the peer table (the larger of the
    /// identity and connection bounds), its one lifecycle event, its lanes' windows of frames and
    /// one [`Event::LaneRoom`] a lane.
    /// The queue never holds more (the module's documentation says how).
    pub fn event_bound(&self) -> usize {
        let peers = self.admission.identities.max(self.admission.connections);
        let lanes = usize::try_from(self.lanes_per_peer).unwrap_or(usize::MAX);
        let frames = lanes.saturating_mul(self.lane_window);
        // A peer's lifecycle event, its lanes' frames, and a room event of each lane to it.
        EXCHANGE_EVENTS
            .saturating_mul(self.exchanges)
            .saturating_add(peers.saturating_mul(frames.saturating_add(lanes).saturating_add(1)))
    }

    fn validate(&self) -> Result<(), Refusal> {
        if let StreamCredit::Pooled { initial } = self.stream_credit
            && initial > self.streams_per_connection
        {
            return Err(Refusal::Configuration);
        }
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
    /// at quinn's assembler ceiling or the share, whichever is less, and acknowledgements
    /// decimated as Google QUIC's are (`TransportConfig::decimate_acks`): every peer is
    /// hyper-quic, which takes the extension.
    fn transport(&self, ranks: u8, congestion: &Congestion) -> Result<TransportConfig, Refusal> {
        let reserve = class_reserve(ranks.saturating_sub(1));
        let window = initial_window(MIN_DATAGRAM).saturating_add(reserve);
        let stream = stream_window_ceiling().min(self.window_ceiling);
        let varint = |value: u64| VarInt::from_u64(value).map_err(|_| Refusal::Configuration);
        let idle = IdleTimeout::try_from(self.idle_timeout).map_err(|_| Refusal::Configuration)?;
        let mut transport = TransportConfig::default();
        let handshake = match self.stream_credit {
            StreamCredit::Fixed => self.streams_per_connection,
            StreamCredit::Pooled { initial } => initial,
        };
        transport
            .max_concurrent_bidi_streams(VarInt::from_u32(handshake))
            .max_concurrent_uni_streams(VarInt::from_u32(self.lanes_per_peer))
            .stream_receive_window(varint(stream)?)
            .receive_window(varint(window)?)
            .send_window(self.window_ceiling)
            .max_idle_timeout(Some(idle))
            .keep_alive_interval(self.keep_alive)
            .congestion(congestion.clone())
            .decimate_acks();
        // MTU discovery searches up to what the peer takes and the path carries, as the path's
        // socket tells it (`Endpoint::set_path_ceiling`), not to quinn's 1,452-byte default: a
        // jumbo-frame path carried 8,954-byte datagrams, and a bus node at a million messages a
        // second on a busy core used 2.24 times less time a message for it (docs/transport.md).
        let mut search = MtuDiscoveryConfig::default();
        search.upper_bound(u16::try_from(crate::RECEIVE_CHUNK).unwrap_or(u16::MAX));
        transport.mtu_discovery_config(Some(search));
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
    /// The bytes the path's warm-up has put on it, its datagrams' (hyper-quic's
    /// `PathStats::sent_warm_up_bytes`): what a count of the bytes that crossed the path takes out to
    /// see the application's own
    pub warm_up_bytes: u64,
    /// Whether the path's warm-up is yet to end (hyper-quic's `PathStats::warm_up_pending`): once
    /// false, the connection sends no more of its padding
    pub warming_up: bool,
    /// The path's probe timeout before backoff (RFC 9002 §6.2.1): the smoothed round trip, four
    /// times its variation and the peer's acknowledgement delay, how long what is sent now may take
    /// to be known delivered or lost. An asker that gives its peer a time to answer waits this on
    /// top of it, for an answer sent at the deadline to arrive.
    pub probe_timeout: Duration,
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
    /// Lane frames written straight from their sender's bytes into an idle lane's stream, never
    /// copied into a buffer of their own.
    pub frames_direct: u64,
    /// Lanes whose next frame, of a lossless class, waits for the budget to hold it, and that
    /// their owner does not hold: a held lane's frame waits for its owner, and no memory moves it.
    pub frames_reserving: usize,
    /// Endpoint responses dropped at their bound.
    pub responses_dropped: u64,
    /// Streams a peer opened past a bound, refused.
    pub streams_refused: u64,
    /// Lanes stopped at a frame refused: corrupt (its prefix's or its own checksum failed), or a
    /// lane past its bounds.
    pub lanes_refused: u64,
    /// Receive chunks held, each charged to the budget.
    pub receive_chunks: usize,
    /// What the socket's receive queue holds of the connections' windows, payload bytes
    /// ([`Endpoint::set_receive_capacity`]; `u64::MAX` until the owner says).
    pub receive_capacity: u64,
    /// The windows granted, summed: what the peers may send unread.
    pub receive_granted: u64,
    /// Connections refused for the queue holding no more first windows.
    pub receive_refused: u64,
    /// Windows set back to their first because their connections were idle when another window's
    /// growth, or a connection's first window, was refused for what they held (`credit`'s window
    /// set back).
    pub windows_set_back: u64,
    /// Connections closed because they stayed idle for a quiet period after their windows were
    /// set back, owing what was wanted again ([`Refusal::ReceiveBuffer`]).
    pub windows_closed: u64,
    /// Connections that receive on a socket of their own ([`Endpoint::set_peer_room`]).
    pub receive_rooms: usize,
    /// Datagrams copied into buffers of their own because no chunk could take them.
    pub receive_copied: u64,
    /// The stream pool ([`StreamCredit::Pooled`]).
    pub pool: PoolStats,
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

/// What a connection holds of its endpoint's memory beside its windows, the messages it carries and
/// its exchange tables ([`state_charge`]): its QUIC state (`hyper_quic::Connection`, 8,128 bytes
/// inline, and what that allocates), its stream tables and its rows in the endpoint's own, which
/// grow with the connections held. Charged to the budget as the connection is made, beside its
/// first window, and given back as it goes, so a node's connections are bounded by its memory as
/// their windows are, not by a count stated apart from it. An idle connection was measured to hold
/// 27.3 to 29.1 KB of its server's heap on macOS and Linux, its exchange tables for 16 streams
/// among it (`docs/benchmarks.md`, "What a server holds for each connection");
/// hyper-rt-transport's `connection_memory` test holds the measure to the charge.
pub const CONNECTION_STATE: u64 = 30 * 1024;

/// The bytes a connection's exchange tables hold for `streams` exchanges: each exchange's id and its
/// stream's entry (`Conn::exchanges`, `Conn::by_stream`), reserved to the streams the connection
/// may have at once as it is made, so no insert allocates.
fn exchange_tables(streams: u32) -> u64 {
    let entry = size_of::<u64>().saturating_add(size_of::<(StreamId, u64)>());
    u64::from(streams).saturating_mul(u64::try_from(entry).unwrap_or(u64::MAX))
}

/// What a connection's own state is charged under `limits` as it is made: [`CONNECTION_STATE`] and
/// its exchange tables, which `limits.streams_per_connection` sizes.
pub fn state_charge(limits: &Limits) -> u64 {
    CONNECTION_STATE.saturating_add(exchange_tables(limits.streams_per_connection))
}

/// What a connection of classes `C` is charged under `limits` as it is made: its first window, the
/// reserve on top of it, and its state ([`state_charge`]). An owner that states its connections'
/// bound from its memory divides what it gives its endpoint by this.
pub fn connection_charge<C: Classes>(limits: &Limits) -> u64 {
    let reserve = class_reserve(C::RANKS.saturating_sub(1));
    initial_window(MIN_DATAGRAM)
        .saturating_add(reserve)
        .saturating_add(state_charge(limits))
}

/// A connection's first grants: its window's, and its state's ([`state_charge`]).
struct Grant {
    window: Reservation,
    state: Reservation,
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
    /// An inbound handshake's origin: the connection ID its client first sent to, and whether the
    /// client proved its address with a retry's token, which names that ID (RFC 9000 §8.1.2,
    /// §7.3). What tells a handshake its client gave up for a retry from the one it went on with.
    origin: Option<(ConnectionId, bool)>,
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
    /// Its exchanges a judgement found unread (`Exchange::unread_since`): while any is, the
    /// connection's credit it holds is the peer's to give back, and an exchange waiting only for
    /// that credit is not judged.
    unread: u32,
    /// When the last unread exchange ended: the credit it held comes back from then, and an
    /// exchange that waited for it through a window that saw it end is given a window afresh.
    unread_ended: Option<Instant>,
    lanes_out: Vec<LaneOut>,
    lanes_in: Vec<LaneIn>,
    window: Window,
    /// The stream bytes read on it, by class.
    delivered: Delivered,
    /// The budget's grants for the receive window: the initial one and one per growth.
    grants: Vec<Reservation>,
    /// What the connection's own state takes of the budget ([`CONNECTION_STATE`]), given back as
    /// it goes.
    state: Option<Reservation>,
    /// When its window was set back, while its growth's grants go back as its peer's reads pay
    /// what QUIC still owes of the credit given (`Core::repay`).
    set_back: Option<Instant>,
    /// It waits in `Core::grown`, once.
    listed: bool,
    /// The connection was lost and its owner told.
    lost: bool,
    /// The room of the socket it receives on alone, once its driver connected one to its peer
    /// ([`Endpoint::set_peer_room`]); until then its windows are the shared socket's.
    room: Option<Room>,
    /// Its part of the stream pool ([`StreamCredit::Pooled`]).
    share: Share,
    /// The exchanges its peer opened that are open: what a pooled connection's grant serves.
    served: u32,
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
    /// The lanes of its the owner holds ([`Endpoint::hold_lane`]): at most its lanes' bound.
    holds: Vec<crate::LaneId>,
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
            holds: Vec::new(),
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
    /// The ceiling every connection's MTU search starts under (`set_default_path_ceiling`)
    path_ceiling: Option<u16>,
    responses: VecDeque<(Transmit, Vec<u8>)>,
    spare: Vec<Vec<u8>>,
    /// By connection key: whether it waits in `touched`, in `rearm` or in `sending`.
    marks: Vec<Marks>,
    /// The connections an event or a call changed since the endpoint last read them
    /// (`read_touched`): each is given its turn to transmit and its deadline is placed again. A
    /// connection that nothing touched has nothing new to send and no deadline but the one placed,
    /// so the endpoint visits what its events reach, never every connection it holds (`benches/
    /// connections.rs` in hyper-rt-transport).
    touched: Vec<usize>,
    /// The connections whose deadline is to be placed again though nothing changed what they
    /// would send: one given its turn that had nothing to send, its timers moved all the same.
    rearm: Vec<usize>,
    /// The connections that may have a datagram to send, each given its turn in order: one that
    /// sends goes to the back, as it may have more; one with nothing leaves until touched again.
    sending: VecDeque<usize>,
    /// The connections by their next deadline, the earliest first (src/deadlines.rs, by key): the
    /// connection's QUIC timer or its exchanges' earliest wait, whichever comes first.
    deadlines: Vec<(Instant, u64)>,
    /// Each connection's index in `deadlines`, by key.
    positions: Vec<Option<usize>>,
    /// The connections a timeout found due, gathered before they are driven.
    due: Vec<u64>,
    /// The frontier of a walk of `deadlines` for the due ones.
    walk: Vec<usize>,
    /// The connections held so far: the next one's serial.
    serials: u64,
    /// A peer's connections, gathered to read its paused lanes again.
    keys: Vec<usize>,
}

/// What a window's growth or a connection's first window was refused for since the endpoint last
/// set back idle windows: room in the shared socket's queue, or the budget's bytes.
#[derive(Clone, Copy, Debug, Default)]
struct Wanted {
    room: bool,
    budget: bool,
}

/// Where a connection waits to be read again (`Endpoint::touched`, `rearm`, `sending`).
#[derive(Clone, Copy, Debug, Default)]
struct Marks {
    touched: bool,
    rearm: bool,
    sending: bool,
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
    /// The streams pooled connections share ([`StreamCredit::Pooled`]); until the owner sizes it,
    /// one connection's streams.
    pool: Pool,
    /// What the socket's receive queue holds of the windows granted ([`Room`]).
    room: Room,
    /// The room a handshake's peer's unprompted messages take, as last said
    /// ([`Endpoint::set_receive_capacity`]): what a first window is taken with.
    handshake_room: u64,
    /// The connections whose windows grew, each by its key and serial, once: what an idle one's
    /// window is set back from when another wants what it holds. Bounded by the connections held;
    /// one that goes leaves it.
    grown: VecDeque<(usize, u64)>,
    /// What a refused growth or first window wanted since the last pass over `grown`.
    wanted: Wanted,
    /// Moves whenever a connection is identified or goes ([`Endpoint::connection_generation`]).
    generation: u64,
    /// Lanes whose next frame waits for the budget ([`Reading::Reserving`]) and that their owner
    /// does not hold: a held lane's frame waits for its owner, and no memory moves it.
    reserving: usize,
    /// By connection key, its lanes counted in `reserving`: what a retry visits, so a budget that
    /// holds more asks again of the connections with a frame waiting, never every connection.
    reserving_at: Vec<u32>,
    /// The inbound handshakes that hold a pending place, by key: no more than the places.
    pending_keys: Vec<usize>,
    /// The attempts the endpoint refused, whose copies are ignored.
    refused: Refused,
}

/// The attempts an endpoint refused (its places taken, or a bound it holds), each by its client's
/// address, the connection ID its client first sent to and whether it carried a retry's token,
/// oldest first. A copy of one, which its client sent a copy spacing behind it (hyper-quic's
/// handshake copies) and which may come once a place is free, is ignored: its client was told the
/// attempt was refused and gave it up, and admitted the copy held its place until its idle timeout
/// (an enrollment joiner's copy of its token's attempt, 10 s in one run of six under CPU
/// contention, focal 2026-10-19). Its token's flag keeps a refused copy of a ClientHello sent before
/// a retry from matching the attempt its client made with the token. Kept while a token is valid
/// (`ServerConfig::retry_token_validity`), after which a copy that carries one is refused as
/// expired, and as many as the endpoint answers in one turn (`Limits::max_responses`), the oldest
/// let go first.
#[derive(Debug, Default)]
struct Refused {
    order: VecDeque<(Instant, (SocketAddr, ConnectionId, bool))>,
    keys: HashSet<(SocketAddr, ConnectionId, bool)>,
    /// None for an endpoint that takes no connection.
    lifetime: Option<Duration>,
}

impl Refused {
    /// Lets go of the records past the lifetime.
    fn expire(&mut self, now: Instant) {
        let Some(lifetime) = self.lifetime else {
            return;
        };
        while let Some(&(at, key)) = self.order.front() {
            if now.saturating_duration_since(at) <= lifetime {
                return;
            }
            self.order.pop_front();
            self.keys.remove(&key);
        }
    }

    fn holds(&self, key: &(SocketAddr, ConnectionId, bool)) -> bool {
        self.keys.contains(key)
    }

    /// Records a refusal, letting go of the oldest past `bound`: a record that finds no room is
    /// not kept, and its copy is judged as any attempt is.
    fn remember(&mut self, now: Instant, key: (SocketAddr, ConnectionId, bool), bound: usize) {
        if self.lifetime.is_none() || self.keys.contains(&key) {
            return;
        }
        while self.order.len() >= bound {
            let Some((_, oldest)) = self.order.pop_front() else {
                return;
            };
            self.keys.remove(&oldest);
        }
        if self.order.try_reserve(1).is_err() || self.keys.try_reserve(1).is_err() {
            return;
        }
        self.order.push_back((now, key));
        self.keys.insert(key);
    }
}

/// What `conn` has moved, as the wait of an exchange of `rank` reads it.
fn moved<R>(conn: &Conn<R>, rank: u8) -> Moved {
    let stats = conn.quic.stats();
    Moved {
        sent: stats.udp_tx.bytes.saturating_sub(stats.path.lost_bytes),
        acked: stats.path.acked_bytes,
        received: stats.udp_rx.bytes,
        delivered: conn.delivered.from(rank),
    }
}

fn length(bytes: usize) -> u64 {
    u64::try_from(bytes).unwrap_or(u64::MAX)
}

/// What `quic`'s peer may send unprompted by any window while this side does not read: an
/// acknowledgement for each of this side's ack-eliciting packets in flight to it, and `probes`
/// loss probes, those of the idle timeout ([`crate::probes_within`]). While the connection
/// handshakes its peer sends nothing larger: an inbound handshake waits in an admission place
/// ([`Endpoint::unprompted_shared`]), so this is a connection this side dialed, whose server is
/// established only after it. Once established, the peer sends two datagrams of up to the path's
/// MTU beside them: the rest of the packet its first probe timeout repeats, and an MTU probe
/// ([`Unprompted::large`]).
fn unprompted_by(quic: &Connection, probes: u64) -> Unprompted {
    Unprompted {
        acknowledgements: quic.ack_eliciting_in_flight(),
        probes,
        large: if quic.is_handshaking() { 0 } else { 2 },
    }
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

/// The configuration slots an endpoint under `admission` needs, so that replacing its TLS
/// ([`Endpoint::set_tls`]) is never refused for the configurations it still holds: one current
/// server and one current client configuration, and while they are replaced their replacements,
/// four; beside them each replaced configuration still held has a holder, a server's an inbound
/// handshake begun under it (`pending` at most), a client's a dial or a connection made with it
/// (within `connections`). A connection a server accepted holds its configuration no longer than
/// its handshake (hyper-quic's `Configs`).
fn config_slots(admission: &AdmissionLimits) -> Result<NonZeroUsize, Refusal> {
    admission
        .connections
        .checked_add(admission.pending)
        .and_then(|held| held.checked_add(4))
        .and_then(NonZeroUsize::new)
        .ok_or(Refusal::Configuration)
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
        Self::build(config, budget, directory, now, None)
    }

    /// [`Endpoint::new`], with the QUIC layer's own randomness drawn from `seed` where the
    /// system's would be: its connection identifiers, reset tokens and the reserved transport
    /// parameter whose random length moves a handshake's sizes (hyper-quic
    /// `ReservedTransportParameter::random`). For a simulation whose run must be the same from
    /// one seed (`docs/sim.md` §3.9); TLS draws its own, which move no size where its signatures
    /// are of fixed length.
    pub fn new_seeded(
        config: Config<C::Role>,
        budget: B,
        directory: D,
        now: Instant,
        seed: [u8; 32],
    ) -> Result<Self, Refusal> {
        Self::build(config, budget, directory, now, Some(seed))
    }

    fn build(
        config: Config<C::Role>,
        budget: B,
        directory: D,
        now: Instant,
        seed: Option<[u8; 32]>,
    ) -> Result<Self, Refusal> {
        config.limits.validate()?;
        let transport = config.limits.transport(C::RANKS, &config.congestion)?;
        let (server, client) = config.tls.prepare(&transport)?;
        let retry_lifetime = server.as_ref().map(ServerConfig::retry_token_validity);
        let mut endpoint = EndpointConfig::default();
        // Peers may send any datagram the receive chunks hold, where quinn's default told them
        // Ethernet's 1,472 bytes whatever the path carried.
        endpoint
            .max_udp_payload_size(u16::try_from(crate::RECEIVE_CHUNK).unwrap_or(u16::MAX))
            .map_err(|_| Refusal::Configuration)?;
        endpoint.config_slots(config_slots(&config.limits.admission)?);
        let mut quic =
            hyper_quic::Endpoint::new(endpoint, server, true, seed).map_err(|_| Refusal::Quic)?;
        let client = quic
            .insert_client_config(client)
            .map_err(|_| Refusal::Quic)?;
        let limits = config.limits;
        // The tables a connection or an identity has a row in start empty and grow with what the
        // endpoint holds (`Endpoint::reserve_marks`), each connection's rows charged with its state
        // (`CONNECTION_STATE`): sized to what admission could hold before any came, they held about
        // 650 bytes for each connection admitted (docs/benchmarks.md), charged to nothing.
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
                peers: Vec::new(),
                now,
                granularity: K_GRANULARITY,
                tick: 0,
                dialing: 0,
                ids: Vec::new(),
                stats: Stats::default(),
                pool: Pool::new(u64::from(limits.streams_per_connection)),
                room: Room::unbounded(),
                handshake_room: 0,
                grown: VecDeque::new(),
                wanted: Wanted::default(),
                generation: 0,
                reserving: 0,
                reserving_at: Vec::new(),
                pending_keys: Vec::new(),
                refused: Refused {
                    lifetime: retry_lifetime,
                    ..Refused::default()
                },
            },
            receive: Receive::new(limits.receive_chunks),
            scratch: Vec::new(),
            path_ceiling: None,
            responses: VecDeque::new(),
            spare: Vec::new(),
            marks: Vec::new(),
            touched: Vec::new(),
            rearm: Vec::new(),
            sending: VecDeque::new(),
            deadlines: Vec::new(),
            positions: Vec::new(),
            due: Vec::new(),
            walk: Vec::new(),
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
        self.poll_transmit_batch(now, 1, out)
    }

    /// The next datagrams to send, written into `out` (cleared first): up to `max_datagrams` of one
    /// connection's, to one destination, each but the last [`Transmit::segment_size`] bytes, for a
    /// socket that sends them in one segmented call (`UDP_SEGMENT`) or splits them. A driver that
    /// asked for one at a time made a call, and a slot of its outbox, a datagram.
    pub fn poll_transmit_batch(
        &mut self,
        now: Instant,
        max_datagrams: usize,
        out: &mut Vec<u8>,
    ) -> Option<Transmit> {
        self.core.now = now;
        out.clear();
        if let Some((transmit, bytes)) = self.responses.pop_front() {
            out.extend_from_slice(&bytes);
            // A buffer for a later response, as many kept as the handshakes waiting at once:
            // a burst past them takes its buffers as it comes and gives them up after.
            if self.spare.len() < self.core.limits.admission.pending
                && self.spare.try_reserve(1).is_ok()
            {
                self.spare.push(bytes);
            }
            return Some(transmit);
        }
        self.read_touched();
        while let Some(key) = self.sending.pop_front() {
            if let Some(marks) = self.marks.get_mut(key) {
                marks.sending = false;
            }
            let Some(Some(conn)) = self.conns.get_mut(key) else {
                continue;
            };
            let sent = conn
                .quic
                .poll_transmit(now, max_datagrams.max(1), out, self.quic.configs());
            // Sending, or finding nothing to send, moves the connection's timers.
            self.rearm_later(key);
            if let Some(transmit) = sent {
                self.endpoint_events(key);
                self.queue_sending(key);
                return Some(transmit);
            }
        }
        None
    }

    /// When [`Endpoint::handle_timeout`] is next due.
    pub fn poll_timeout(&mut self) -> Option<Instant> {
        self.read_touched();
        let first = self.deadlines.first().map(|(due, _)| *due);
        #[cfg(feature = "oracle")]
        self.check_deadlines(first);
        first
    }

    /// Time has reached `now`: fire every timer due.
    pub fn handle_timeout(&mut self, now: Instant) {
        self.core.now = now;
        self.retry_reserving();
        self.read_touched();
        let mut due = std::mem::take(&mut self.due);
        due.clear();
        deadlines::due(&self.deadlines, now, &mut self.walk, &mut due);
        for &id in &due {
            let Ok(key) = usize::try_from(id) else {
                continue;
            };
            if let Some(Some(conn)) = self.conns.get_mut(key)
                && conn.quic.poll_timeout().is_some_and(|at| at <= now)
            {
                conn.quic.handle_timeout(now);
            }
            self.pump(now, key);
            // Driven or gone, its deadline is placed again at the next poll.
            self.touch(key);
        }
        due.clear();
        self.due = due;
    }

    /// Connection `key` changed: it is given its turn to transmit, and its deadline is placed
    /// again, at the endpoint's next poll.
    fn touch(&mut self, key: usize) {
        self.reserve_marks(key);
        if let Some(marks) = self.marks.get_mut(key)
            && !marks.touched
        {
            marks.touched = true;
            self.touched.push(key);
        }
    }

    /// Connection `key`'s deadline is placed again at the endpoint's next poll.
    fn rearm_later(&mut self, key: usize) {
        self.reserve_marks(key);
        if let Some(marks) = self.marks.get_mut(key)
            && !marks.rearm
        {
            marks.rearm = true;
            self.rearm.push(key);
        }
    }

    /// Grants the pool's spare streams to the connections waiting for them, longest waiting first
    /// (`crate::pool`): each granted is touched, so it sends the peer its new limit at this poll.
    /// One gone, or whose peer is not blocked now, is passed.
    fn grant_pool(&mut self) {
        let most = self.core.limits.streams_per_connection;
        loop {
            let Some((key, serial)) = self.core.pool.next() else {
                // Nothing spare while one waits: what an idle connection holds is taken back for
                // it, the pool overcommitted by the streams its peer was told of (`crate::pool`).
                let Some((key, serial)) = self.core.pool.next_idle() else {
                    break;
                };
                if let Some(Some(conn)) = self.conns.get_mut(key)
                    && conn.serial == serial
                    && let Some(granted) = self.core.pool.reclaim(&mut conn.share, conn.served)
                {
                    conn.quic
                        .set_max_concurrent_streams(Dir::Bi, VarInt::from_u32(granted));
                }
                continue;
            };
            let granted = match self.conns.get_mut(key) {
                Some(Some(conn)) if conn.serial == serial => {
                    if conn.lost || !conn.quic.peer_streams_blocked(Dir::Bi) {
                        self.core.pool.passed(&mut conn.share);
                        false
                    } else {
                        let granted = self.core.pool.grant(&mut conn.share, most);
                        conn.quic
                            .set_max_concurrent_streams(Dir::Bi, VarInt::from_u32(granted));
                        true
                    }
                }
                // Gone, or its key now another connection's: that one waits under its own serial.
                _ => false,
            };
            if granted {
                self.touch(key);
            }
        }
    }

    /// Takes back what idle connections' grown windows hold, once a window's growth or a
    /// connection's first window was refused for it (`credit`'s window set back): the shared
    /// socket's room, which only connections receiving on it hold, or the budget, which every one
    /// does. An idle connection whose window grew is set back to its first. One set back that has
    /// moved nothing since, for a quiet period, still owing what is wanted, is closed
    /// ([`Refusal::ReceiveBuffer`]): its peer sent nothing that would pay it, and QUIC takes back no
    /// credit it gave (RFC 9000 §4.1). Idle is admission's rule: no exchange under way and nothing
    /// moved for its quiet period, the endpoint's serve period. A connection not idle, or holding
    /// none of what was wanted, keeps its place; one whose growth all went back leaves the list,
    /// and one gone, or whose key is another's now, is passed.
    fn take_back_windows(&mut self) {
        let wanted = std::mem::take(&mut self.core.wanted);
        if !(wanted.room || wanted.budget) {
            return;
        }
        let now = self.core.now;
        let quiet = self.core.admission.quiet();
        for _ in 0..self.core.grown.len() {
            let Some((key, serial)) = self.core.grown.pop_front() else {
                break;
            };
            let close = match self.conns.get_mut(key) {
                Some(Some(conn)) if conn.serial == serial => {
                    if !conn.window.grown() && conn.set_back.is_none() {
                        conn.listed = false;
                        continue;
                    }
                    let since = conn
                        .peer
                        .and_then(|(peer, _)| self.core.admission.quiet_since(peer, key));
                    let idle =
                        since.is_some_and(|since| now.saturating_duration_since(since) >= quiet);
                    let holds = wanted.budget || (wanted.room && conn.room.is_none());
                    match (conn.set_back, since) {
                        _ if !(idle && holds) || conn.lost => false,
                        // Moved strictly before it was set back: a move at the instant of the
                        // set back may have come after it, and earns a quiet period afresh.
                        (Some(at), Some(since)) if since < at => {
                            now.saturating_duration_since(at) >= quiet
                        }
                        _ => {
                            self.core.set_back(conn, now);
                            false
                        }
                    }
                }
                _ => continue,
            };
            self.core.grown.push_back((key, serial));
            if close {
                self.core.stats.windows_closed = self.core.stats.windows_closed.saturating_add(1);
                self.close(now, key, Refusal::ReceiveBuffer.code());
            }
        }
    }

    /// Connection `key` waits for its turn to transmit, once.
    fn queue_sending(&mut self, key: usize) {
        if let Some(marks) = self.marks.get_mut(key)
            && !marks.sending
        {
            marks.sending = true;
            self.sending.push_back(key);
        }
    }

    /// Room for key `key`'s marks and heap index: the keys are the QUIC endpoint's connection
    /// handles, bounded by the connections it holds at once. The lists a turn fills with keys grow
    /// with them, so a turn that lists every key allocates nothing; a reservation the allocator
    /// refuses leaves a list to grow as it is filled.
    fn reserve_marks(&mut self, key: usize) {
        if self.marks.len() <= key {
            self.marks.resize(key.saturating_add(1), Marks::default());
            let keys = self.marks.len();
            for list in [&mut self.touched, &mut self.rearm, &mut self.walk] {
                let _ = list.try_reserve(keys.saturating_sub(list.len()));
            }
            let _ = self.due.try_reserve(keys.saturating_sub(self.due.len()));
            let _ = self
                .sending
                .try_reserve(keys.saturating_sub(self.sending.len()));
            let _ = self
                .deadlines
                .try_reserve(keys.saturating_sub(self.deadlines.len()));
        }
        if self.positions.len() <= key {
            self.positions.resize(key.saturating_add(1), None);
        }
    }

    /// Reads again the connections touched since the last poll: each is queued for its turn to
    /// transmit and placed again by its deadline, and those only to be re-armed are placed again.
    fn read_touched(&mut self) {
        self.grant_pool();
        self.take_back_windows();
        let mut touched = std::mem::take(&mut self.touched);
        for &key in &touched {
            if let Some(marks) = self.marks.get_mut(key) {
                marks.touched = false;
            }
            if self.conns.get(key).is_some_and(Option::is_some) {
                self.queue_sending(key);
            }
            self.place(key);
        }
        touched.clear();
        self.touched = touched;
        let mut rearm = std::mem::take(&mut self.rearm);
        for &key in &rearm {
            if let Some(marks) = self.marks.get_mut(key) {
                marks.rearm = false;
            }
            self.place(key);
        }
        rearm.clear();
        self.rearm = rearm;
    }

    /// Places connection `key` in `deadlines` by its QUIC timer or its exchanges' earliest wait,
    /// whichever comes first; a connection gone, or with neither, leaves them.
    fn place(&mut self, key: usize) {
        let due = match self.conns.get_mut(key) {
            Some(Some(conn)) => {
                let quic = conn.quic.poll_timeout();
                let waits = self.core.exchanges.first_due(key);
                match (quic, waits) {
                    (Some(a), Some(b)) => Some(a.min(b)),
                    (a, b) => a.or(b),
                }
            }
            _ => None,
        };
        let Ok(id) = u64::try_from(key) else {
            return;
        };
        deadlines::set(&mut self.deadlines, &mut self.positions, id, due);
    }

    /// With the oracle on, the earliest deadline placed equals the one a visit of every connection
    /// and every exchange finds (what `poll_timeout` was): a connection changed and not touched
    /// would differ.
    #[cfg(feature = "oracle")]
    fn check_deadlines(&mut self, first: Option<Instant>) {
        if !self.core.oracle {
            return;
        }
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
        let visited = match (connections, exchanges) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (a, b) => a.or(b),
        };
        if visited != first {
            self.core.mismatches = self.core.mismatches.saturating_add(1);
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
            Event::LaneRoom { .. } => {
                if let Some((key, serial, stream)) = lane
                    && let Some(Some(conn)) = self.conns.get_mut(key)
                    && conn.serial == serial
                    && let Some(out) = conn
                        .lanes_out
                        .iter_mut()
                        .find(|out| out.stream == Some(stream))
                {
                    out.room_waiting = false;
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
            self.core.release_window(grant);
            return Err(Refusal::Identity);
        };
        let dialed = self.quic.connect(now, self.client, address, name, None);
        let (handle, quic) = match dialed {
            Ok(dialed) => dialed,
            Err(_) => {
                self.core.release_window(grant);
                return Err(Refusal::Quic);
            }
        };
        self.core.dialing = self.core.dialing.saturating_add(1);
        self.store(handle.0, quic, Some(peer), None, grant);
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
        let retry_lifetime = server.as_ref().map(ServerConfig::retry_token_validity);
        let client = self
            .quic
            .insert_client_config(client)
            .map_err(|_| Refusal::Quic)?;
        if self.quic.set_server_config(server).is_err() {
            self.quic.retire_client_config(client);
            return Err(Refusal::Quic);
        }
        // Tokens the earlier configuration issued are still checked by its lifetime, and a record
        // is kept for the longer of the two.
        self.core.refused.lifetime = match (self.core.refused.lifetime, retry_lifetime) {
            (Some(was), Some(now)) => Some(was.max(now)),
            (was, now) => was.or(now),
        };
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

    /// Give a reservation back to the budget: a frame's, or one the owner reserved. A lossless frame
    /// that waited for the budget is reserved again now.
    pub fn release(&mut self, reservation: Reservation) {
        self.core.budget.release(reservation);
        self.retry_reserving();
    }

    /// The budget may hold more than it did (the owner gave bytes back to it elsewhere): each lane
    /// whose lossless frame waits for it ([`Classes::lossless`]) asks again, and is read on if its
    /// frame is reserved. Only the connections with such a lane are visited, and a lane its owner
    /// holds waits for its owner. Nothing waits, nothing is done.
    pub fn retry_reserving(&mut self) {
        if self.core.reserving == 0 {
            return;
        }
        // Bounded: one turn for each connection key the count has seen.
        for key in 0..self.core.reserving_at.len() {
            let waits = self
                .core
                .reserving_at
                .get(key)
                .is_some_and(|count| *count > 0);
            if !waits {
                continue;
            }
            let Some(mut conn) = self.take(key) else {
                continue;
            };
            // From the last: reading a lane removes at most that lane.
            for at in (0..conn.lanes_in.len()).rev() {
                let waiting = conn
                    .lanes_in
                    .get(at)
                    .is_some_and(|lane| lane.state == Reading::Reserving && !lane.held);
                if waiting {
                    self.core.read_lane(&mut conn, at);
                }
            }
            self.put(key, conn);
            if self.core.reserving == 0 {
                break;
            }
        }
    }

    /// Lanes whose next frame, of a lossless class, waits for the budget to hold it, their owner
    /// not holding them: what [`Endpoint::retry_reserving`] moves once the budget holds more.
    /// [`Stats::frames_reserving`] without the visit of every connection [`Endpoint::stats`] makes.
    pub fn frames_reserving(&self) -> usize {
        self.core.reserving
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
        let queued = self.core.send_frame(
            &mut conn,
            lane,
            (C::kind_code(kind), class, C::embedded_checksum(kind)),
            frame,
        );
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

    /// Whether the connection of `epoch` to `peer` is up. A peer may hold several connections at
    /// once (its identity's, up to its admission bound), and only its current one's lifecycle is
    /// an event ([`Event::Connected`], [`Event::Closed`]): an owner that keeps something for each
    /// connection a request came on ([`Event::Request`]'s epoch) lets an older one's go by this.
    pub fn connection_live(&self, peer: PeerId, epoch: Epoch) -> bool {
        self.conns.iter().flatten().any(|conn| {
            !conn.lost && conn.epoch == epoch && conn.peer.is_some_and(|(of, _)| of == peer)
        })
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
            warm_up_bytes: stats.path.sent_warm_up_bytes,
            warming_up: stats.path.warm_up_pending,
            probe_timeout: conn.quic.probe_timeout(),
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
            pool: self.core.pool.stats(),
            receive_capacity: self.core.room.capacity(),
            receive_granted: self.core.room.granted(),
            receive_refused: self.core.room.refused(),
            frames_reserving: self.core.reserving,
            receive_rooms: self
                .conns
                .iter()
                .flatten()
                .filter(|conn| conn.room.is_some())
                .count(),
            ..self.core.stats
        }
    }

    /// What the endpoint's windows may grant at most, summed, payload bytes: for every connection
    /// admission holds and every handshake it lets wait beside them, a window at its ceiling and the
    /// reserve on top. What its socket's receive queue should hold for no window to be held below
    /// its ceiling by the queue ([`Endpoint::set_receive_capacity`]).
    pub fn receive_demand(&self) -> u64 {
        let admission = self.core.limits.admission;
        let held = admission.connections.saturating_add(admission.pending);
        u64::try_from(held)
            .unwrap_or(u64::MAX)
            .saturating_mul(self.peer_demand())
    }

    /// What one connection's windows may grant at most: its window at the ceiling and the reserve
    /// on top. What a socket it receives on alone should hold ([`Endpoint::set_peer_room`]).
    pub fn peer_demand(&self) -> u64 {
        let reserve = class_reserve(C::RANKS.saturating_sub(1));
        self.core
            .limits
            .window_ceiling
            .max(initial_window(MIN_DATAGRAM))
            .saturating_add(reserve)
    }

    /// The socket's receive queue holds `bytes` of its peers' stream data, the unprompted messages
    /// of every connection on it counted ([`Endpoint::unprompted_shared`]): what its driver's
    /// [`crate::ReceiveCharge`] tells of the buffer the socket reports, or before anything is
    /// measured [`Endpoint::unmeasured_room`]. `handshake` is the room one handshake more would
    /// take for its peer's ([`Endpoint::unprompted_handshake`]; nothing before a measurement). From
    /// now the windows granted, summed, never pass it: a connection is taken only with room for its
    /// first window and its peer's unprompted messages, else refused [`Refusal::ReceiveBuffer`],
    /// and a window grows only while what is left holds both for each handshake admission may
    /// still let wait (`credit`'s socket room). Windows granted already stand until they are given
    /// back.
    pub fn set_receive_capacity(&mut self, bytes: u64, handshake: u64) {
        self.core.room.set_capacity(bytes);
        self.core.handshake_room = handshake;
    }

    /// The owner holds `peer`'s lane `lane`: nothing more of it is read until the owner lets it go
    /// ([`Endpoint::release_lane`]), so what the peer sends on it waits in QUIC, and QUIC's flow
    /// control holds the peer at the window this side granted (the socket's room). A consumer's
    /// backpressure: a node whose subscribers cannot take more holds the publishers that feed them,
    /// rather than queueing what they send. A lane the peer has not yet opened is held from its
    /// opening, on this connection or a later one. Refused `Lanes` past the lanes a peer may open.
    pub fn hold_lane(&mut self, peer: PeerId, lane: crate::LaneId) -> Result<(), Refusal> {
        let bound = usize::try_from(self.core.limits.lanes_per_peer).unwrap_or(usize::MAX);
        let entry = self
            .core
            .peer_entry_mut(peer)
            .ok_or(Refusal::NotConnected)?;
        if !entry.holds.contains(&lane) {
            if entry.holds.len() >= bound {
                return Err(Refusal::Lanes);
            }
            entry.holds.push(lane);
        }
        self.mark_lanes(peer, lane, true);
        Ok(())
    }

    /// The owner lets `peer`'s lane `lane` go: it is read again from where it stopped.
    pub fn release_lane(&mut self, peer: PeerId, lane: crate::LaneId) -> Result<(), Refusal> {
        let entry = self
            .core
            .peer_entry_mut(peer)
            .ok_or(Refusal::NotConnected)?;
        entry.holds.retain(|held| *held != lane);
        self.mark_lanes(peer, lane, false);
        Ok(())
    }

    /// Marks `peer`'s open lanes named `lane` held or not, reading on those let go.
    fn mark_lanes(&mut self, peer: PeerId, lane: crate::LaneId, held: bool) {
        let mut keys = std::mem::take(&mut self.keys);
        keys.clear();
        keys.extend(self.core.admission.connections_of(peer));
        for &key in &keys {
            let Some(mut conn) = self.take(key) else {
                continue;
            };
            // From the last: reading a lane removes at most that lane.
            for at in (0..conn.lanes_in.len()).rev() {
                let matches = conn
                    .lanes_in
                    .get(at)
                    .is_some_and(|open| open.state != Reading::Opener && open.lane == lane);
                if !matches {
                    continue;
                }
                let Some(open) = conn.lanes_in.get_mut(at) else {
                    continue;
                };
                let was = std::mem::replace(&mut open.held, held);
                // A frame waiting for the budget waits for its owner while held, and for the
                // budget again once let go.
                if open.state == Reading::Reserving && was != held {
                    if held {
                        self.core.waited_for_budget(key);
                    } else {
                        self.core.waits_for_budget(key);
                    }
                }
                if !held {
                    self.core.read_lane(&mut conn, at);
                }
            }
            self.put(key, conn);
        }
        self.keys = keys;
    }

    /// A count that moves whenever a connection is identified or goes: an owner that keeps
    /// something for each connection (a driver's socket for each peer) looks again
    /// ([`Endpoint::established`]) only when it moved.
    pub fn connection_generation(&self) -> u64 {
        self.core.generation
    }

    /// Every identified connection's peer and the address it is reached at now, into `out`
    /// (cleared first; at most the admission's bound on connections).
    pub fn established(&self, out: &mut Vec<(PeerId, SocketAddr)>) {
        out.clear();
        for conn in self.conns.iter().flatten() {
            if let Some((peer, _)) = conn.peer
                && !conn.lost
            {
                out.push((peer, conn.quic.remote_address()));
            }
        }
    }

    /// The connection window `peer` has consumed, summed over its streams as flow control counts
    /// it: what a message read from its socket carried, as the difference across the read
    /// (hyper-quic patch Q20). `None` for a peer with no connection.
    pub fn stream_bytes_received(&self, peer: PeerId) -> Option<u64> {
        let key = self.core.connection_of(peer)?;
        let conn = self.conns.get(key).and_then(Option::as_ref)?;
        Some(conn.quic.stream_bytes_received())
    }

    /// The connection window consumed, summed over the connections that receive on the shared
    /// socket (those without a socket of their own, [`Endpoint::set_peer_room`]).
    pub fn stream_bytes_received_shared(&self) -> u64 {
        self.conns
            .iter()
            .flatten()
            .filter(|conn| conn.room.is_none())
            .fold(0u64, |sum, conn| {
                sum.saturating_add(conn.quic.stream_bytes_received())
            })
    }

    /// What `peer` may send its socket unprompted by any window while this side does not read
    /// ([`unprompted_by`]). `None` for a peer with no connection.
    pub fn unprompted(&self, peer: PeerId) -> Option<Unprompted> {
        let key = self.core.connection_of(peer)?;
        let conn = self.conns.get(key).and_then(Option::as_ref)?;
        Some(unprompted_by(
            &conn.quic,
            probes_within(self.core.limits.idle_timeout),
        ))
    }

    /// What the shared socket's peers may send it unprompted: each connection without a socket of
    /// its own, as [`Endpoint::unprompted`], a handshake waiting in an admission place as
    /// [`Endpoint::unprompted_handshake`] with its acknowledgements. Only the places taken count:
    /// a place no handshake holds has no peer to send, and a handshake taken later is taken with
    /// room for its own ([`Endpoint::set_receive_capacity`]). Every place was reserved, taken or
    /// not, and a handshake in one counted again as a connection: on Linux's default buffer
    /// focal's 32 places took 860 KB of a 319 KB queue, and every first window was refused.
    pub fn unprompted_shared(&self) -> Unprompted {
        let probes = probes_within(self.core.limits.idle_timeout);
        self.conns
            .iter()
            .flatten()
            .filter(|conn| conn.room.is_none())
            .fold(Unprompted::default(), |sum, conn| {
                sum.plus(if conn.pending {
                    Unprompted {
                        acknowledgements: conn.quic.ack_eliciting_in_flight(),
                        ..self.unprompted_handshake()
                    }
                } else {
                    unprompted_by(&conn.quic, probes)
                })
            })
    }

    /// What a handshake waiting in an admission place may send unprompted besides its
    /// acknowledgements: its loss probes, and one MTU probe. A client's handshake is done once it
    /// sends its Finished, its server's only once that is read (RFC 9001 §4.1.1), so the client
    /// may be established and probing while its server waits.
    pub fn unprompted_handshake(&self) -> Unprompted {
        Unprompted {
            acknowledgements: 0,
            probes: probes_within(self.core.limits.idle_timeout),
            large: 1,
        }
    }

    /// The room the shared socket must hold before anything is measured: the first window of every
    /// connection and handshake admission lets in at once. What each sends before this side first
    /// acknowledges it is bounded by the protocol (RFC 9000 §8.1's three times what it was sent
    /// before its address is validated, RFC 9002 §7.2's initial window), and the first datagram the
    /// socket reads is the first sample.
    pub fn unmeasured_room(&self) -> u64 {
        let admission = self.core.limits.admission;
        let held = admission.connections.saturating_add(admission.pending);
        u64::try_from(held)
            .unwrap_or(u64::MAX)
            .saturating_mul(Core::<C, B, D>::first_window())
    }

    /// One connection's first window, the room a socket of its own must hold before anything is
    /// measured ([`Endpoint::unmeasured_room`]).
    pub fn unmeasured_peer_room(&self) -> u64 {
        Core::<C, B, D>::first_window()
    }

    /// The largest UDP payload the local stack carries on any path, where the driver knows it before
    /// it can read a path's own (a simulation's links, a host's interfaces): every connection made
    /// from now on begins its MTU search under it, its first probe never past it, and a path's own
    /// ceiling (`set_path_ceiling`) replaces it once read. A probe past a path's MTU is lost, and in
    /// flight until it is found lost: a first probe of 33,363 bytes on a 1,500-byte path held a
    /// new connection's data behind its window for a round trip.
    pub fn set_default_path_ceiling(&mut self, payload: u16) {
        self.path_ceiling = Some(payload);
    }

    /// The largest UDP payload the local stack carries on the path to `peer`, as its kernel knows
    /// the path's MTU (its driver read it from the socket it connected to the peer, hyper-rt's
    /// `UdpSocket::path_payload`): the connection's MTU discovery searches no further, and none of
    /// its datagrams is larger. Refused `NotConnected` for a peer with no connection.
    pub fn set_path_ceiling(&mut self, peer: PeerId, payload: u16) -> Result<(), Refusal> {
        let key = self.core.connection_of(peer).ok_or(Refusal::NotConnected)?;
        let now = self.core.now;
        let conn = self
            .conns
            .get_mut(key)
            .and_then(Option::as_mut)
            .ok_or(Refusal::NotConnected)?;
        conn.quic.set_path_ceiling(now, payload);
        self.touch(key);
        Ok(())
    }

    /// `peer`'s connection receives on a socket of its own from now, whose queue holds `bytes` of
    /// datagram payload (its driver connected a socket to the peer): its windows leave the shared
    /// socket's room for a room of their own, in which they grow alone, so each connection a node
    /// holds brings the queue its windows need, as a TCP connection's socket does. Said again, the
    /// room is resized. Refused `NotConnected` for a peer with no connection.
    pub fn set_peer_room(&mut self, peer: PeerId, bytes: u64) -> Result<(), Refusal> {
        let key = self.core.connection_of(peer).ok_or(Refusal::NotConnected)?;
        let conn = self
            .conns
            .get_mut(key)
            .and_then(Option::as_mut)
            .ok_or(Refusal::NotConnected)?;
        if let Some(room) = conn.room.as_mut() {
            room.set_capacity(bytes);
            return Ok(());
        }
        let held = conn
            .grants
            .iter()
            .map(Reservation::granted)
            .fold(0, u64::saturating_add);
        self.core.room.release(held);
        let mut room = Room::unbounded();
        room.set_capacity(bytes);
        room.hold(held);
        conn.room = Some(room);
        Ok(())
    }

    /// Sizes the stream pool ([`StreamCredit::Pooled`]): the exchanges pooled peers may have open
    /// at once, all connections' together. Credit it gains goes to the connections waiting at the
    /// next poll; credit it loses comes back as their exchanges end. Refused `Configuration` under
    /// [`StreamCredit::Fixed`].
    pub fn set_stream_pool(&mut self, streams: u32) -> Result<(), Refusal> {
        if self.core.limits.stream_credit == StreamCredit::Fixed {
            return Err(Refusal::Configuration);
        }
        self.core.pool.resize(u64::from(streams));
        Ok(())
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
        self.touch(key);
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

    fn store(
        &mut self,
        key: usize,
        mut quic: Connection,
        dialed: Option<PeerId>,
        origin: Option<(ConnectionId, bool)>,
        grant: Grant,
    ) {
        if let Some(ceiling) = self.path_ceiling {
            quic.set_path_ceiling(self.core.now, ceiling);
        }
        let limits = self.core.limits;
        let initial = initial_window(MIN_DATAGRAM);
        let Grant { window, state } = grant;
        let grants = vec![window];
        let serial = self.serials;
        self.serials = serial.saturating_add(1);
        let conn = Conn {
            quic,
            key,
            serial,
            dialed,
            pending: dialed.is_none(),
            origin,
            peer: None,
            epoch: 0,
            exchanges: Vec::with_capacity(
                usize::try_from(limits.streams_per_connection).unwrap_or(0),
            ),
            by_stream: Vec::with_capacity(
                usize::try_from(limits.streams_per_connection).unwrap_or(0),
            ),
            credit_moved: None,
            unread: 0,
            unread_ended: None,
            lanes_out: Vec::new(),
            lanes_in: Vec::new(),
            window: Window::new(initial, limits.window_ceiling),
            delivered: Delivered::new(C::RANKS),
            grants,
            state: Some(state),
            set_back: None,
            listed: false,
            lost: false,
            room: None,
            share: Share::default(),
            served: 0,
        };
        if self.conns.len() <= key {
            self.conns.resize_with(key.saturating_add(1), || None);
        }
        self.put(key, Box::new(conn));
    }

    fn attempt(&mut self, now: Instant, attempt: Attempt, buffer: &mut Vec<u8>) {
        let remote = attempt.remote_address();
        let origin = (*attempt.orig_dst_cid(), attempt.remote_address_validated());
        // A client that took a retry may have sent its ClientHello again under the connection ID
        // it first used before the retry reached it (a probe timeout on a slow path, a client
        // held off its CPU), and one of the two copies may come while every place is taken and
        // the other once one is free. Admitted, the copy its client gave up holds its place until
        // its idle timeout, and the client's own attempt with the token is refused for that place:
        // eight joiners of an enrollment sponsor with one place waited 10 s behind such a copy in
        // 15 of 15 runs under CPU contention (focal, 2026-10-19). A token names the ID its client
        // used before the retry, and binds its address (RFC 9000 §8.1.4), so the attempt it
        // carries closes a handshake pending under that ID from that address without a token, and
        // an attempt without one under the ID of a handshake pending with a token is that copy.
        let superseded = self
            .core
            .pending_with(remote, origin.0, !origin.1, &self.conns);
        if !origin.1 && superseded.is_some() {
            // Answered nothing: its client goes on with the handshake its token began.
            self.quic.ignore(attempt);
            return;
        }
        if let Some(key) = superseded {
            self.close(now, key, Refusal::Closed.code());
        }
        let record = (remote, origin.0, origin.1);
        self.core.refused.expire(now);
        if self.core.refused.holds(&record) {
            // A copy of an attempt its client was told was refused, and gave up.
            self.quic.ignore(attempt);
            return;
        }
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
                let bound = self.core.limits.max_responses;
                self.core.refused.remember(now, record, bound);
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
                self.store(handle.0, quic, None, Some(origin), grant);
                self.core.pending_keys.push(handle.0);
                self.pump(now, handle.0);
            }
            Err(error) => {
                self.core.admission.end_pending();
                self.core.release_window(grant);
                if let Some(transmit) = error.response {
                    self.respond(transmit, buffer);
                }
            }
        }
    }

    fn respond(&mut self, transmit: Transmit, bytes: &[u8]) {
        let response = bytes.get(..transmit.size).unwrap_or(bytes);
        if self.responses.len() >= self.core.limits.max_responses
            || self.responses.try_reserve(1).is_err()
        {
            self.core.stats.responses_dropped = self.core.stats.responses_dropped.saturating_add(1);
            return;
        }
        let mut copy = self.spare.pop().unwrap_or_default();
        copy.clear();
        if copy.try_reserve(response.len()).is_err() {
            self.core.stats.responses_dropped = self.core.stats.responses_dropped.saturating_add(1);
            return;
        }
        copy.extend_from_slice(response);
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
            // Gone: its deadline leaves at the next poll.
            self.touch(key);
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
    /// The handshake pending from `remote` under `id`, admitted with a token if `validated`: one
    /// at most, the client's one attempt under that ID. A look at the pending places alone.
    fn pending_with(
        &self,
        remote: SocketAddr,
        id: ConnectionId,
        validated: bool,
        conns: &[Option<Box<Conn<C::Role>>>],
    ) -> Option<usize> {
        self.pending_keys.iter().copied().find(|&key| {
            conns.get(key).and_then(Option::as_ref).is_some_and(|conn| {
                conn.origin == Some((id, validated)) && conn.quic.remote_address() == remote
            })
        })
    }

    /// A connection's first window, with the reserve on top of it: room in the socket's queue
    /// first, then the budget's bytes.
    fn window_grant(&mut self) -> Result<Grant, Refusal> {
        let bytes = Self::first_window();
        if let Err(refusal) = self.room.first(bytes, self.handshake_room) {
            self.wanted.room = true;
            return Err(refusal);
        }
        let window = match self.budget.reserve(bytes, Lane::Window) {
            Ok(grant) => grant,
            Err(refusal) => {
                self.room.release(bytes);
                self.wanted.budget = true;
                return Err(refusal);
            }
        };
        match self
            .budget
            .reserve(state_charge(&self.limits), Lane::Window)
        {
            Ok(state) => Ok(Grant { window, state }),
            Err(refusal) => {
                self.room.release(window.granted());
                self.budget.release(window);
                self.wanted.budget = true;
                Err(refusal)
            }
        }
    }

    /// The bytes of a connection's first window and the reserve on top of it.
    fn first_window() -> u64 {
        let reserve = class_reserve(C::RANKS.saturating_sub(1));
        initial_window(MIN_DATAGRAM).saturating_add(reserve)
    }

    /// The room a window's growth leaves: for each handshake admission may still let wait at once,
    /// its first window and the room its peer's unprompted messages take.
    fn kept_for_handshakes(&self) -> u64 {
        let limit = self.limits.admission.pending;
        let waiting = self.admission.stats().pending;
        let more = u64::try_from(limit.saturating_sub(waiting)).unwrap_or(u64::MAX);
        more.saturating_mul(Self::first_window().saturating_add(self.handshake_room))
    }

    /// A connection's first grants given back before it was made: its window's room in the
    /// socket's queue and its bytes, and its state's.
    fn release_window(&mut self, grant: Grant) {
        self.room.release(grant.window.granted());
        self.budget.release(grant.window);
        self.budget.release(grant.state);
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
            // Every byte of the message on the stream acknowledged: the peer has it.
            StreamEvent::Finished { id } => {
                if let Some(exchange) = self
                    .exchange_on(conn, id)
                    .and_then(|exchange| self.exchanges.get_mut(exchange))
                {
                    exchange.out.acked = true;
                }
            }
            StreamEvent::Available { .. } => {}
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
            self.pending_keys.retain(|&held| held != key);
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
        if let StreamCredit::Pooled { initial } = self.limits.stream_credit {
            if C::pooled(role) {
                self.pool
                    .join(&mut conn.share, initial, conn.key, conn.serial);
                if let Some(granted) = self
                    .pool
                    .welcome(&mut conn.share, self.limits.streams_per_connection)
                {
                    conn.quic
                        .set_max_concurrent_streams(Dir::Bi, VarInt::from_u32(granted));
                }
            } else {
                // A role outside the pool holds every stream a connection may from here on.
                conn.quic.set_max_concurrent_streams(
                    Dir::Bi,
                    VarInt::from_u32(self.limits.streams_per_connection),
                );
            }
        }
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
        self.generation = self.generation.wrapping_add(1);
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
        self.pool.leave(&mut conn.share);
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
            self.pending_keys.retain(|&held| held != key);
        }
    }

    /// The connection drained: what it held goes back.
    fn gone(&mut self, key: usize, mut conn: Box<Conn<C::Role>>) {
        self.lost(key, &mut conn);
        if conn.listed {
            let serial = conn.serial;
            self.grown
                .retain(|&(listed, of)| !(listed == key && of == serial));
        }
        // A connection's windows go back to the room that held them: the shared socket's, or the
        // room of its own, which goes with it.
        let shared = conn.room.is_none();
        for grant in conn.grants.drain(..) {
            if shared {
                self.room.release(grant.granted());
            }
            self.budget.release(grant);
        }
        if let Some(state) = conn.state.take() {
            self.budget.release(state);
        }
        self.generation = self.generation.wrapping_add(1);
    }

    fn release_lanes(&mut self, conn: &mut Conn<C::Role>) {
        for mut lane in conn.lanes_out.drain(..) {
            for (_, frame) in lane.queue.drain(..) {
                self.budget.release(frame);
            }
        }
        let peer = conn.peer.map(|(peer, _)| peer);
        let key = conn.key;
        for lane in conn.lanes_in.drain(..) {
            self.drop_lane_in(key, peer, lane);
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
            unread_since: None,
            starved: false,
            ended: false,
            credit_settled: false,
        };
        match self.exchanges.insert(exchange) {
            Ok(id) => {
                self.exchanges.place(id, conn.exchanges.len());
                conn.exchanges.push(id);
                conn.served = conn.served.saturating_add(1);
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
                    epoch: conn.epoch,
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
        if self.limits.ask_queue == AskQueue::Credit && self.asks_past_credit(conn) {
            return Err(Refusal::Unsent);
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
            unread_since: None,
            starved: false,
            ended: false,
            credit_settled: false,
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

    /// Whether `conn` already waits for as many streams for its asked exchanges as its peer grants
    /// it, those this side holds and those it may still open, one at least ([`AskQueue::Credit`]).
    fn asks_past_credit(&mut self, conn: &mut Conn<C::Role>) -> bool {
        let (streamed, queued) = self
            .exchanges
            .tally(conn.key)
            .map_or((0, 0), crate::tally::Tally::asked);
        #[cfg(feature = "oracle")]
        self.check(queued, |core| {
            conn.exchanges
                .iter()
                .filter_map(|id| core.exchanges.get(*id))
                .filter(|exchange| exchange.opened && exchange.stream.is_none())
                .fold(0u64, |queued, _| queued.saturating_add(1))
        });
        let credit = streamed.saturating_add(conn.quic.streams().remaining(Dir::Bi));
        queued >= credit.max(1)
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
            let (has_body, left) = (exchange.out.has_body, exchange.out.left);
            if !has_body {
                self.settle_credit(conn, id);
                finish(conn, stream)?;
            }
            let exchange = self
                .exchanges
                .get_mut(id)
                .ok_or((Refusal::UnknownExchange, false))?;
            exchange.out.state = match (has_body, left) {
                (true, 0) => Out::Trailer,
                (true, _) => Out::Body,
                (false, _) => Out::Finished,
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
            self.settle_credit(conn, id);
            finish(conn, stream)?;
            let exchange = self
                .exchanges
                .get_mut(id)
                .ok_or((Refusal::UnknownExchange, false))?;
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
    /// An exchange on `conn` ended, this side's own if `opened`. A pooled connection's grant
    /// serves the exchanges its peer opens, so one of those ended may give streams back to the
    /// pool (`crate::pool`).
    fn exchange_ended(&mut self, conn: &mut Conn<C::Role>, opened: bool) {
        if opened {
            return;
        }
        conn.served = conn.served.saturating_sub(1);
        if let Some(granted) = self
            .pool
            .ended(&mut conn.share, conn.served, conn.key, conn.serial)
        {
            conn.quic
                .set_max_concurrent_streams(Dir::Bi, VarInt::from_u32(granted));
        }
    }

    /// A peer's exchange settles its stream's credit with the pool as its reply is finished, before
    /// the finish: QUIC gives a stream's credit back to its peer once its request is read and its
    /// reply finished, not once the reply is acknowledged (hyper-quic `VENDORED.md` §29), so the
    /// connection's grant must be the pool's by then, or the stream goes back to the peer past it.
    fn settle_credit(&mut self, conn: &mut Conn<C::Role>, id: u64) {
        let Some(exchange) = self.exchanges.get_mut(id) else {
            return;
        };
        if exchange.opened || exchange.credit_settled {
            return;
        }
        exchange.credit_settled = true;
        self.exchange_ended(conn, false);
    }

    /// An exchange that ended without its credit settled as its reply finished (one this side
    /// opened, or one refused) settles it now.
    fn unsettled_ended(&mut self, conn: &mut Conn<C::Role>, exchange: &Exchange<C::Class>) {
        if !exchange.credit_settled {
            self.exchange_ended(conn, exchange.opened);
        }
    }

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
        if exchange.unread_since.is_some() {
            conn.unread = conn.unread.saturating_sub(1);
            conn.unread_ended = Some(self.now);
        }
        self.admission
            .ended(exchange.peer, exchange.connection, self.now);
        self.unlist(conn, id, exchange.at);
        // Only an exchange that held a stream frees credit; one refused while queued moves no queue.
        if exchange.stream.is_some() {
            conn.credit_moved = Some(self.now);
        }
        self.unsettled_ended(conn, &exchange);
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
        // A pooled peer blocked at its grant with none more on its way waits its turn for more,
        // granted as the endpoint next polls (`Endpoint::grant_pool`).
        if conn.share.pooled && !conn.share.wanting && conn.quic.peer_streams_blocked(Dir::Bi) {
            self.pool.wants(&mut conn.share, conn.key, conn.serial);
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
        let floor = conn.quic.probe_timeout();
        // Out of the peer's credit — its stream's own, or the connection's with this side's bytes
        // to send — with at least a datagram of its own sent and all of that acknowledged: the
        // peer holds its bytes and gives back no credit for them. An exchange that wrote less is
        // one waiting behind such a one, not one. What its stream has written goes with it: written
        // past what it had when it was found unread, the peer gave credit back.
        let unread_now = self.exchanges.get(id).and_then(|exchange| {
            let blocked =
                exchange.stream_blocked || (peer_takes == 0 && exchange.out.pending() > 0);
            if !blocked {
                return None;
            }
            let stream = conn.quic.send_stream(exchange.stream?);
            let written = stream.written().ok()?;
            (written >= LEAST_PROGRESS && stream.sent_acknowledged().unwrap_or(false))
                .then_some(written)
        });
        let held_unread = conn.unread;
        let unread_ended = conn.unread_ended;
        let mut unread_change = 0i8;
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
                    _ => Err(Refusal::Unsent),
                };
            }
            // A stream its peer does not read ends by its own bytes, whatever else the connection
            // carries (focal's audit F38): found unread, it is given a window more to be read, and
            // its reset gives back the connection's credit it holds.
            match (unread_now, exchange.unread_since) {
                (Some(written), None) => {
                    exchange.unread_since = Some((now, written));
                    unread_change = 1;
                }
                // Written past what it had when it was found unread: the peer gave credit back,
                // reading, however often a judgement finds the stream out of credit (a sender held
                // to the peer's pace is out of credit at nearly every instant, its bytes taken as
                // they go). The wait starts again from here; judged on the state alone, a transfer
                // the peer read steadily was refused once it outlasted a window (8 MiB on a loaded
                // host, in 12 of 14 runs under two dozen busy loops).
                (Some(written), Some((_, mark))) if written > mark => {
                    exchange.unread_since = Some((now, written));
                }
                (Some(_), Some((since, _)))
                    if now.saturating_duration_since(since) >= exchange.carry.window(floor) =>
                {
                    return Err(Refusal::Stalled);
                }
                (None, Some(_)) => {
                    exchange.unread_since = None;
                    unread_change = -1;
                }
                _ => {}
            }
            if Self::ours_to_move(exchange, peer_takes) {
                exchange.carry.hold(now, at, backlog);
                return Ok(());
            }
            // Waiting only for the connection's credit, which an unread exchange of this side
            // holds or held in this window: no evidence against the peer's taking this one, and
            // the unread one ends within its window and its credit comes back.
            let starved_by_unread = held_unread > 0
                || unread_ended.is_some_and(|ended| ended >= exchange.carry.started());
            if peer_takes == 0
                && starved_by_unread
                && exchange.unread_since.is_none()
                && exchange.out.pending() > 0
            {
                exchange.carry.hold(now, at, backlog);
                return Ok(());
            }
            let acknowledged = exchange.out.acked;
            exchange
                .carry
                .judge(now, at, held, backlog, acknowledged, floor)
        });
        match unread_change {
            1 => conn.unread = conn.unread.saturating_add(1),
            -1 => conn.unread = conn.unread.saturating_sub(1),
            _ => {}
        }
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
        if conn.set_back.is_some() {
            self.repay(conn);
        }
        let Some(grown) = conn.window.tune(now, conn.quic.rtt(), self.granularity) else {
            return;
        };
        let more = grown.saturating_sub(conn.window.window());
        // A connection on a socket of its own grows in its own room, which no handshake shares;
        // one on the shared socket leaves the first windows of the handshakes that may come.
        let grew = match conn.room.as_mut() {
            Some(room) => room.grow(more, 0),
            None => {
                let kept = self.kept_for_handshakes();
                self.room.grow(more, kept)
            }
        };
        if !grew {
            // What idle connections hold of the shared room is set back for it; a room of its
            // own no other connection holds.
            self.wanted.room |= conn.room.is_none();
            return;
        }
        let Ok(grant) = self.budget.reserve(more, Lane::Window) else {
            match conn.room.as_mut() {
                Some(room) => room.release(more),
                None => self.room.release(more),
            }
            self.wanted.budget = true;
            return;
        };
        conn.grants.push(grant);
        conn.window.grew(grown);
        if !conn.listed {
            conn.listed = true;
            self.grown.push_back((conn.key, conn.serial));
        }
        let reserve = class_reserve(C::RANKS.saturating_sub(1));
        if let Ok(window) = VarInt::from_u64(grown.saturating_add(reserve)) {
            conn.quic.set_receive_window(window);
        }
    }

    /// Sets `conn`'s window back to its first (`credit`'s window set back). QUIC keeps the credit
    /// its peer was given (RFC 9000 §4.1), so the window shrinks as the peer's reads pay it
    /// (hyper-quic's debt, `Connection::receive_window_debt`), and what the growth holds goes back
    /// as they do ([`Core::repay`]).
    fn set_back(&mut self, conn: &mut Conn<C::Role>, now: Instant) {
        conn.set_back = Some(now);
        if !conn.window.grown() {
            return;
        }
        conn.window.set_back();
        let reserve = class_reserve(C::RANKS.saturating_sub(1));
        if let Ok(window) = VarInt::from_u64(conn.window.window().saturating_add(reserve)) {
            conn.quic.set_receive_window(window);
        }
        self.stats.windows_set_back = self.stats.windows_set_back.saturating_add(1);
        self.repay(conn);
    }

    /// The grants of a window set back, the latest first, go back to its room and the budget
    /// while what stays still holds what its peer may send: the window and the reserve on top of
    /// it, and what QUIC still owes of the credit given before the window was set back. A grant
    /// goes back whole, so what is held never falls below that.
    fn repay(&mut self, conn: &mut Conn<C::Role>) {
        let reserve = class_reserve(C::RANKS.saturating_sub(1));
        let debt = conn.quic.receive_window_debt();
        let owed = conn
            .window
            .window()
            .saturating_add(reserve)
            .saturating_add(debt);
        let mut held = conn
            .grants
            .iter()
            .map(Reservation::granted)
            .fold(0u64, u64::saturating_add);
        while conn.grants.len() > 1 {
            let Some(last) = conn.grants.last().map(Reservation::granted) else {
                break;
            };
            let rest = held.saturating_sub(last);
            if rest < owed {
                break;
            }
            let Some(grant) = conn.grants.pop() else {
                break;
            };
            held = rest;
            match conn.room.as_mut() {
                Some(room) => room.release(grant.granted()),
                None => self.room.release(grant.granted()),
            }
            self.budget.release(grant);
        }
        if debt == 0 {
            conn.set_back = None;
        }
    }

    fn send_frame(
        &mut self,
        conn: &mut Conn<C::Role>,
        lane: crate::LaneId,
        (kind, class, embedded): (u16, C::Class, Option<crate::Embedded>),
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
        // A lossless class's lane holds one frame beyond what QUIC took. QUIC's stream holds what
        // the peer's credit lets it, as a socket's send buffer does, whose writer waits once it is
        // full: a frame left waiting says the peer, the connection's credit or the class's share
        // takes no more, and the sender is held, not its queue grown. The lane held a congestion
        // window of bytes, as Linux's TCP Small Queues bound what waits below a socket (Dumazet,
        // 2012), but a loss-based law's window on a path that loses nothing grows without end:
        // CUBIC's on loopback reached 1.55 GB, and a bus publisher the node held queued 470 ms of
        // messages in its own lane. A window of frames of the class's bound (the core's, T37) is
        // otherwise megabytes deep.
        let full = conn.lanes_out.get(at).is_none_or(|out| {
            out.queue.len() >= self.limits.lane_window
                || (C::lossless(class) && !out.queue.is_empty())
        });
        if full {
            // Said once the lane has room again (`flush_frames`).
            if let Some(out) = conn.lanes_out.get_mut(at) {
                out.refused = true;
            }
            return Err(Refusal::LaneFull);
        }
        // A frame of its own checksum is covered by the prefix only before it: its owner summed
        // the rest, and its receiver verifies that sum.
        let prefix = match embedded {
            Some(embedded) => {
                Prefix::encode_covering(kind, frame.len(), embedded.head(frame)?, None)?
            }
            None => Prefix::encode(kind, frame, None)?,
        };
        let mut bytes = self.budget.reserve(
            length(PREFIX_BYTES).saturating_add(length(frame.len())),
            Lane::Class(class),
        )?;
        // A connection carrying frames is in use as one carrying exchanges is.
        if let Some((peer, _)) = conn.peer {
            self.admission.carried(peer, conn.key, self.now);
        }
        // An idle lane writes the frame straight into its stream: what QUIC takes is copied once,
        // from the caller's bytes, and only what it leaves is copied into the frame's buffer to
        // wait in the lane, behind nothing. The buffer was reserved first, so a stream that takes
        // part of the frame always leaves room for the rest; one that takes it all gives it back.
        let taken = self.write_idle(conn, at, C::rank(class), &prefix, frame);
        if taken >= PREFIX_BYTES.saturating_add(frame.len()) {
            self.budget.release(bytes);
            self.stats.frames_direct = self.stats.frames_direct.saturating_add(1);
            return Ok(());
        }
        bytes.fill(prefix.get(taken..).unwrap_or(&[]));
        bytes.fill(
            frame
                .get(taken.saturating_sub(PREFIX_BYTES)..)
                .unwrap_or(&[]),
        );
        if let Some(out) = conn.lanes_out.get_mut(at) {
            out.queue.push_back((C::rank(class), bytes));
        }
        self.flush_lane(conn, at);
        Ok(())
    }

    /// Writes `prefix` and then `frame` to lane `at`'s stream while the lane holds no frame and its
    /// opening is written, within what the class may spend; how many of their bytes QUIC took.
    fn write_idle(
        &mut self,
        conn: &mut Conn<C::Role>,
        at: usize,
        rank: u8,
        prefix: &[u8],
        frame: &[u8],
    ) -> usize {
        let Some(stream) = conn.lanes_out.get(at).and_then(|out| {
            (out.queue.is_empty() && out.opener_written >= crate::lane::OPENER_BYTES)
                .then_some(out.stream)
                .flatten()
        }) else {
            return 0;
        };
        let allowed = self.allowed(conn, rank);
        let _ = conn
            .quic
            .send_stream(stream)
            .set_priority(priority::<C>(rank));
        let Pushed::Took(head) = push(&mut conn.quic, stream, prefix, allowed) else {
            return 0;
        };
        if head < prefix.len() {
            return head;
        }
        let left = allowed.saturating_sub(length(head));
        match push(&mut conn.quic, stream, frame, left) {
            Pushed::Took(body) => head.saturating_add(body),
            Pushed::Stopped(_) | Pushed::Closed => head,
        }
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
        let (peer, epoch, key, serial) = (
            conn.peer.map(|(peer, _)| peer),
            conn.epoch,
            conn.key,
            conn.serial,
        );
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
            // A lane that refused a frame has room again: its owner is told, once.
            if out.refused
                && !out.room_waiting
                && out.queue.len() < self.limits.lane_window
                && let Some(peer) = peer
            {
                out.refused = false;
                out.room_waiting = true;
                self.events.push_back(Waiting {
                    event: Event::LaneRoom {
                        peer,
                        epoch,
                        lane: out.lane,
                    },
                    lane: Some((key, serial, stream)),
                });
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
                    self.stats.lanes_refused = self.stats.lanes_refused.saturating_add(1);
                    if at < conn.lanes_in.len() {
                        let lane = conn.lanes_in.remove(at);
                        let _ = conn
                            .quic
                            .recv_stream(lane.stream)
                            .stop(VarInt::from_u32(refusal.code()));
                        self.drop_lane_in(conn.key, conn.peer.map(|(peer, _)| peer), lane);
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
        // A lane its owner holds is not read past its opening: what its peer sends waits in QUIC.
        if state != Reading::Opener && conn.lanes_in.get(at).is_some_and(|lane| lane.held) {
            return Ok(false);
        }
        match state {
            Reading::Opener => {
                let advanced = lane_opener(conn, at)?;
                // The lane named itself: held from now if its owner holds its peer's lane of that name.
                let opened = conn
                    .lanes_in
                    .get(at)
                    .filter(|lane| lane.state != Reading::Opener)
                    .map(|lane| lane.lane);
                if let (Some(id), Some((peer, _))) = (opened, conn.peer) {
                    let held = self
                        .peer_entry_mut(peer)
                        .is_some_and(|entry| entry.holds.contains(&id));
                    if let Some(lane) = conn.lanes_in.get_mut(at) {
                        lane.held = held;
                    }
                }
                Ok(advanced)
            }
            Reading::Prefix if self.frame_room(conn, at) => self.lane_prefix(conn, at),
            Reading::Prefix => {
                self.pause_lane(conn, at);
                Ok(false)
            }
            Reading::Frame => self.lane_frame(conn, at),
            Reading::Skipping => self.lane_skip(conn, at),
            Reading::Reserving => self.lane_reserve(conn, at),
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

    /// A lane from `peer` on the connection `key` is gone: its frame in progress goes back, and its
    /// pause and its wait for the budget with it.
    fn drop_lane_in(&mut self, key: usize, peer: Option<PeerId>, lane: LaneIn) {
        if let Some(frame) = lane.frame {
            self.budget.release(frame);
        }
        if lane.state == Reading::Reserving && !lane.held {
            self.waited_for_budget(key);
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
        lane.state = match (&frame, fits) {
            (Some(_), _) => Reading::Frame,
            // A lossless frame the budget cannot hold now waits for it, its lane unread.
            (None, Some(class)) if C::lossless(class) => Reading::Reserving,
            (None, _) => Reading::Skipping,
        };
        lane.frame = frame;
        // Read, so not held: it waits for the budget alone.
        if lane.state == Reading::Reserving {
            self.waits_for_budget(conn.key);
            return Ok(false);
        }
        Ok(true)
    }

    /// A lane of the connection `key` waits for the budget, its owner not holding it.
    fn waits_for_budget(&mut self, key: usize) {
        self.reserving = self.reserving.saturating_add(1);
        if self.reserving_at.len() <= key {
            // Keys are the QUIC endpoint's connection handles, bounded by the connections it holds.
            self.reserving_at.resize(key.saturating_add(1), 0);
        }
        if let Some(count) = self.reserving_at.get_mut(key) {
            *count = count.saturating_add(1);
        }
    }

    /// A lane of the connection `key` counted in `reserving` waits for the budget no more: its
    /// frame reserved, its owner holding it, or the lane gone.
    fn waited_for_budget(&mut self, key: usize) {
        self.reserving = self.reserving.saturating_sub(1);
        if let Some(count) = self.reserving_at.get_mut(key) {
            *count = count.saturating_sub(1);
        }
    }

    /// A lossless frame's bytes ask the budget again: read on once it holds them, else the lane
    /// stays unread.
    fn lane_reserve(&mut self, conn: &mut Conn<C::Role>, at: usize) -> Result<bool, Refusal> {
        let role = conn.peer.map(|(_, role)| role).ok_or(Refusal::Identity)?;
        let lane = conn.lanes_in.get_mut(at).ok_or(Refusal::Lanes)?;
        let prefix = lane.decoded.ok_or(Refusal::Order)?;
        let class = C::kind_of(lane.kind)
            .and_then(|kind| C::class_of(kind, role))
            .ok_or(Refusal::Kind)?;
        let Ok(frame) = self.budget.reserve(prefix.length(), Lane::Class(class)) else {
            return Ok(false);
        };
        lane.frame = Some(frame);
        lane.state = Reading::Frame;
        self.waited_for_budget(conn.key);
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
        let decoded = lane.decoded.ok_or(Refusal::Order)?;
        let (peer, kind) = (conn.peer.map(|(peer, _)| peer), C::kind_of(lane.kind));
        // A frame of its own checksum: the prefix's covers what that does not, and that the rest.
        let verified = match kind.and_then(C::embedded_checksum) {
            Some(embedded) => embedded
                .head(frame.bytes())
                .and_then(|head| decoded.verify(&lane.prefix, head))
                .and_then(|()| embedded.verify(frame.bytes())),
            None => decoded.verify(&lane.prefix, frame.bytes()),
        };
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
