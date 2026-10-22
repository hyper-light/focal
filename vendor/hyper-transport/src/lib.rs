//! The application layer over hyper-quic, sans-io (mantle note 32 §3.4, step T-1).
//!
//! mantle's application protocol in slates' shape (node.md §3): one connection per peer,
//! bidirectional exchanges of a request and a reply, classes with a credit reserve for the classes
//! above, absolute credits, typed refusals, streaming bodies through reservations, and the class
//! decided by the message's kind and the sender's role, never by anything a peer chooses. Its
//! generic core is focal-wire's (`transport.rs`, `peers.rs`, `admission.rs`, `frame.rs`,
//! `round.rs`), ported from an async crate over quinn into a state machine over hyper-quic
//! (`ORIGIN.md`).
//!
//! The [`Endpoint`] owns hyper-quic's endpoint and every connection, and is driven by its caller:
//! it is fed `now`, datagrams and timeouts, and it returns datagrams, a timeout and [`Event`]s. It
//! never spawns, never reads a clock and never opens a socket.
//!
//! What a project supplies:
//! - [`Classes`]: its message kinds, the class each sender role gives each kind, and each class's
//!   frame bound;
//! - [`Budget`]: the bytes the node may hold for the transport, over its own accountant;
//! - [`Directory`]: which peer, in which role, a certificate names.

#![cfg_attr(
    test,
    allow(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::panic,
        clippy::indexing_slicing,
        clippy::arithmetic_side_effects,
        clippy::disallowed_macros,
        clippy::cognitive_complexity,
        clippy::cast_possible_truncation
    )
)]

mod admission;
mod arena;
mod budget;
mod charge;
mod crc;
mod credit;
mod deadlines;
mod endpoint;
mod error;
mod exchange;
mod frame;
mod lane;
mod pool;
mod progress;
mod receive;
mod round;
mod state;
mod tally;
mod timing;
pub mod tls;

pub use admission::{AdmissionLimits, AdmissionStats};
pub use budget::{Budget, Fixed, Lane, Reservation};
pub use charge::{Account, Charged, Queue, ReceiveCharge, SocketCharge, Unprompted, probes_within};
pub use credit::{
    K_GRANULARITY, MIN_DATAGRAM, STREAM_BYTES_PER_PACKET, class_reserve, initial_window,
    socket_buffer_ask, stream_window_ceiling,
};
pub use endpoint::{
    AskQueue, Config, Endpoint, Limits, PathFacts, Stats, StreamCredit, connection_charge,
    state_charge,
};
pub use error::Refusal;
pub use frame::Embedded;
/// The congestion controllers a connection may run ([`Config::congestion`]).
pub use hyper_quic::congestion;
pub use hyper_quic::{EcnCodepoint, Transmit};
/// The fold an owner measures its timer granularity `G` with, for [`Endpoint::set_granularity`]
/// and [`Endpoint::exchange_tail`].
pub use hyper_timing::Lateness;
pub use pool::PoolStats;
pub use progress::{LEAST_PROGRESS, Progress};
pub use receive::RECEIVE_CHUNK;
pub use round::{Round, RoundEnd};
pub use state::StateStats;
pub use timing::spread;

use std::fmt::Debug;

/// A peer, as the [`Directory`] names it.
pub type PeerId = u64;
/// A replication lane to a peer: one long-lived stream, frames in order (node.md §3.2).
pub type LaneId = u32;
/// A connection to a peer, counted: each new connection to a peer is a new epoch, and keying
/// material exported under one epoch is never valid under another.
pub type Epoch = u64;

/// One exchange of a request and its reply.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ExchangeId(pub(crate) u64);

/// What a project tells the transport about its messages. A project's class set is its own (slates:
/// control, metadata, bulk; focal: its traffic classes; mantle: control, replication, request,
/// bulk); the transport orders them and keeps the reserve.
pub trait Classes {
    /// A class of message; lower is more urgent.
    type Class: Copy + Ord + Debug;
    /// A kind of message.
    type Kind: Copy + Debug;
    /// A sender's role.
    type Role: Copy + Debug;
    /// How many classes there are: each class's [`Classes::rank`] is below it.
    const RANKS: u8;
    /// A class's rank: 0 for the most urgent, one more for each class above it.
    fn rank(class: Self::Class) -> u8;
    /// The class of a message of `kind` from a sender of `role`, or `None` when that role may not
    /// send that kind (audit §13.3, node.md §3.6).
    fn class_of(kind: Self::Kind, role: Self::Role) -> Option<Self::Class>;
    /// The largest message, head and body, a class carries; checked from the prefix before any
    /// byte past it is read.
    fn frame_bound(class: Self::Class) -> u64;
    /// The kind's code on the wire.
    fn kind_code(kind: Self::Kind) -> u16;
    /// The kind a code names, or `None` for a code this side does not know.
    fn kind_of(code: u16) -> Option<Self::Kind>;
    /// Whether a peer of `role` shares the stream pool ([`StreamCredit::Pooled`]): granted the
    /// exchanges it may open as it wants them. A role outside it holds every stream a connection
    /// may. None is, unless a side says so.
    fn pooled(role: Self::Role) -> bool {
        let _ = role;
        false
    }
    /// Whether an exchange of `kind` waits on an event its owner awaits rather than on the
    /// endpoint's service: a read held until something arrives for it or its wait ends (a long
    /// poll). One a pooled peer opens is parked as its head arrives, holding none of the pool's
    /// credit while it waits: its connection is granted a stream of its own for it, within the
    /// streams one connection may hold (`crate::pool`, docs/transport.md §4ab). None does, unless a
    /// side says so.
    fn waits(kind: Self::Kind) -> bool {
        let _ = kind;
        false
    }
    /// Whether a lane frame of `class` is never skipped. A frame whose class bound or budget cannot
    /// take it is skipped and counted, so one full range never stalls a lane (`lane`'s module doc):
    /// right for a class whose frames the protocol above sends again (consensus messages). A class
    /// whose frames are their sender's only copy (a bus's messages) is lossless: a frame of it the
    /// budget cannot take now waits, its lane unread so QUIC's flow control holds its sender, until
    /// the budget can ([`Endpoint::retry_reserving`]). One past its class's bound is still refused,
    /// as it never fits. None is, unless a side says so.
    fn lossless(class: Self::Class) -> bool {
        let _ = class;
        false
    }
    /// Where a lane frame of `kind` carries its own CRC-32C, which its receiver's protocol checks
    /// end to end ([`Embedded`]): the transport's prefix checksum then covers the frame only before
    /// it, and the transport verifies the frame's own on receipt, so a frame is summed once, not
    /// twice. Each frame of such a kind must carry it; one whose own checksum fails is refused as
    /// corrupt, as one whose prefix checksum fails is. None does, unless a side says so.
    fn embedded_checksum(kind: Self::Kind) -> Option<Embedded> {
        let _ = kind;
        None
    }
}

/// The bytes an endpoint asks its budget for to queue an outgoing lane frame of `length` bytes: the
/// frame and the transport's prefix before it. An owner whose budget hands out buffers by size sizes
/// its largest from this of its largest [`Classes::frame_bound`], so no frame its classes allow is
/// refused for want of a buffer; a constant of its own would drift from the transport's.
pub fn frame_grant(length: u64) -> u64 {
    length.saturating_add(u64::try_from(frame::PREFIX_BYTES).unwrap_or(u64::MAX))
}

/// What a connection's handshake established, for [`Directory::identify`]: everything the
/// directory names the peer and its role by.
#[derive(Clone, Copy, Debug)]
pub struct Authenticated<'a> {
    /// The peer's end-entity certificate (DER), verified by the TLS configuration's verifier, or
    /// `None` when the peer presented none: a server whose client verifier lets a client
    /// authenticate otherwise (an enrolling node, a guest) accepts it without one, and only the
    /// directory decides what such a peer may send (its role, through [`Classes::class_of`]).
    pub certificate: Option<&'a [u8]>,
    /// The application protocol the handshake negotiated (RFC 7301): one of the server
    /// configuration's, so one endpoint can serve several protocols' peers under one certificate
    /// each and name each by its own.
    pub protocol: &'a [u8],
    /// The peer this side dialed, or `None` for a connection the peer opened. A dialed connection
    /// is admitted only as the peer dialed: the directory names it, or refuses it.
    pub dialed: Option<PeerId>,
}

/// Which peer a connection names: the node's view of its membership and its clients.
pub trait Directory {
    /// A sender's role, as [`Classes::Role`].
    type Role: Copy + Debug;
    /// The peer and role a connection's handshake names, or `None`: the connection is refused
    /// ([`Refusal::Identity`]) and charged to no identity.
    fn identify(&mut self, authenticated: &Authenticated<'_>) -> Option<(PeerId, Self::Role)>;
    /// The name a peer's certificate is checked against when this node dials it.
    fn server_name(&self, peer: PeerId) -> Option<&str>;
}

/// What the endpoint tells its owner.
pub enum Event<C: Classes> {
    /// A connection to `peer` authenticated; `epoch` counts the connections to it.
    Connected {
        /// The peer.
        peer: PeerId,
        /// Its role, as the directory names it.
        role: C::Role,
        /// The connection's epoch.
        epoch: Epoch,
    },
    /// A peer asked: its head is read ([`Endpoint::head`]) and its body, if any, is read through
    /// reservations ([`Endpoint::read_body`]).
    Request {
        /// The exchange.
        exchange: ExchangeId,
        /// Who asked.
        peer: PeerId,
        /// The epoch of the connection it came on: a peer may hold more than one connection to
        /// this side at once (its identity's, up to its admission bound), and what an owner keeps
        /// for a connection is found by it.
        epoch: Epoch,
        /// What was asked.
        kind: C::Kind,
        /// The class the kind and the asker's role give.
        class: C::Class,
        /// The body's length, if a body follows.
        body: Option<u64>,
    },
    /// The peer replied to an exchange this side opened.
    Reply {
        /// The exchange.
        exchange: ExchangeId,
        /// The reply body's length, if a body follows.
        body: Option<u64>,
    },
    /// More of a body can be read, or its checksum verified.
    BodyReady {
        /// The exchange.
        exchange: ExchangeId,
    },
    /// A body that could not be written whole can take more.
    Writable {
        /// The exchange.
        exchange: ExchangeId,
    },
    /// A lane that refused a frame for being full ([`Refusal::LaneFull`]) has room again: what
    /// waited for it may be queued. Said once a refusal, and not again while it waits to be
    /// polled. A core within its window never finds its lane full; an owner whose lane carries
    /// many groups' frames waits for this instead of losing what found it full.
    LaneRoom {
        /// The peer.
        peer: PeerId,
        /// The connection the lane is on.
        epoch: Epoch,
        /// The lane.
        lane: LaneId,
    },
    /// A frame arrived on a peer's lane; give the reservation back with [`Endpoint::release`].
    Frame {
        /// The peer.
        peer: PeerId,
        /// The lane.
        lane: LaneId,
        /// The frame's kind.
        kind: C::Kind,
        /// The frame, read into a reservation from the budget.
        frame: Reservation,
    },
    /// An exchange ended without completing. It is gone: its identifier names nothing now.
    Refused {
        /// The exchange.
        exchange: ExchangeId,
        /// Why.
        refusal: Refusal,
        /// Whether the peer refused it (a stream reset carrying the refusal) or this side did.
        by_peer: bool,
    },
    /// The connection of `epoch` to `peer` closed; every exchange on it was refused first. A
    /// newer connection to the peer, one that replaced it, is unaffected.
    Closed {
        /// The peer.
        peer: PeerId,
        /// The epoch of the connection that closed.
        epoch: Epoch,
        /// Why, when either side said, as [`Event::Unreachable`] tells it: a TLS client's
        /// handshake is complete before the server has verified its certificate (RFC 8446 §4.4.4,
        /// RFC 9001 §4.1.2), so a dial whose certificate the peer refuses, by TLS or by its
        /// directory, is up and then closed with [`Refusal::Identity`]; a connection the peer's
        /// admission replaced closes with its bound ([`Refusal::Connections`]).
        refusal: Option<Refusal>,
    },
    /// A dial to `peer` ended before it authenticated, or authenticated as someone else. Told for
    /// every dial that ends so with no connection to the peer up, even when the last event the
    /// owner polled said the same: an owner that dials again on it hears of each dial's end.
    Unreachable {
        /// The peer.
        peer: PeerId,
        /// Why, when either side said: [`Refusal::Identity`] for a certificate refused on either
        /// side (a TLS alert, RFC 9000 §20.1's crypto codes, or the peer's directory or this one
        /// naming no peer, or another than the one dialed), a bound of the peer's admission
        /// ([`Refusal::Pending`], [`Refusal::Identities`], [`Refusal::Connections`],
        /// [`Refusal::Events`]) when its endpoint said it was full; `None` when nothing answered
        /// (the path, a timeout, a reset).
        refusal: Option<Refusal>,
    },
}

impl<C: Classes> Debug for Event<C> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Connected { peer, role, epoch } => {
                write!(
                    formatter,
                    "Connected {{ peer: {peer}, role: {role:?}, epoch: {epoch} }}"
                )
            }
            Self::Request {
                exchange,
                peer,
                epoch,
                kind,
                class,
                body,
            } => write!(
                formatter,
                "Request {{ exchange: {exchange:?}, peer: {peer}, epoch: {epoch}, kind: {kind:?}, class: {class:?}, body: {body:?} }}"
            ),
            Self::Reply { exchange, body } => {
                write!(
                    formatter,
                    "Reply {{ exchange: {exchange:?}, body: {body:?} }}"
                )
            }
            Self::BodyReady { exchange } => {
                write!(formatter, "BodyReady {{ exchange: {exchange:?} }}")
            }
            Self::Writable { exchange } => {
                write!(formatter, "Writable {{ exchange: {exchange:?} }}")
            }
            Self::LaneRoom { peer, epoch, lane } => {
                write!(
                    formatter,
                    "LaneRoom {{ peer: {peer:?}, epoch: {epoch:?}, lane: {lane:?} }}"
                )
            }
            Self::Frame {
                peer,
                lane,
                kind,
                frame,
            } => write!(
                formatter,
                "Frame {{ peer: {peer}, lane: {lane}, kind: {kind:?}, bytes: {} }}",
                frame.bytes().len()
            ),
            Self::Refused {
                exchange,
                refusal,
                by_peer,
            } => write!(
                formatter,
                "Refused {{ exchange: {exchange:?}, refusal: {refusal:?}, by_peer: {by_peer} }}"
            ),
            Self::Closed {
                peer,
                epoch,
                refusal,
            } => write!(
                formatter,
                "Closed {{ peer: {peer}, epoch: {epoch}, refusal: {refusal:?} }}"
            ),
            Self::Unreachable { peer, refusal } => {
                write!(
                    formatter,
                    "Unreachable {{ peer: {peer}, refusal: {refusal:?} }}"
                )
            }
        }
    }
}
