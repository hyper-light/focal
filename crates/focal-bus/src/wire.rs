//! The bus's messages on hyper-transport: what each is, who may send it, how urgent it is and how
//! large it may be, and the heads of its exchanges.
//!
//! - A **publish** is a lane frame from a client: one batch (`batch`), appended by the node to the
//!   batch's stream. A frame has no reply: at most once by itself; the classes that promise more
//!   acknowledge in their own frames.
//! - A **delivery** is a lane frame from the node: one stored batch, the same bytes the publish
//!   carried with the base offset and `next` stamped, which the receiver checks against its
//!   producer's CRC.
//! - A **gap** is a lane frame from the node: a subscriber that fell behind the node's retained
//!   window is told the offsets it missed, `[from, to)`, and goes on from `to`.
//! - A **subscribe** is an exchange from a client: the stream and the offset to start at; its reply
//!   states the stream's retained window, `[floor, next)`, or, from a node that does not lead the
//!   part, the leader it knows ([`Reply`]).
//! - An **acknowledgement** is a lane frame from the node: a publish its part's group committed and
//!   its leader applied, with the offsets it took and the class it achieved ([`Ack`]; doc 34 P5).
//! - A **redirect** is a lane frame from a node that does not lead the part: the publish it did not
//!   take, and the leader it knows, by node and term, which a client takes only where the term
//!   advances what it knew ([`Redirect`]; doc 34 P2, after Kafka's KIP-951).
//! - A **raft message** is a lane frame between nodes: one of hyper-raft's wire records, a part's
//!   group's append, answer, vote or heartbeat (doc 34 P8).
//! - A **heartbeat** is a lane frame between nodes on their liveness lane: hyper-liveness's node
//!   pair stream, which judges each peer node for every group the pair shares (doc 33 §4.1).
//!
//! The kind and the sender's role give the class, never the sender: a client cannot send a
//! delivery, a node no publish, a client no raft message. Subscribes and heartbeats are control,
//! ahead of the flow they start or judge; a group's messages go ahead of deliveries, which are of
//! what the group committed; deliveries go ahead of publishes, so a node drains to its subscribers
//! before it takes more.

use hyper_transport::Classes;

use crate::BusError;

/// A message's urgency, the most urgent first.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Class {
    /// Subscriptions and their answers, and the nodes' heartbeats.
    Control,
    /// A part's group's messages between its nodes.
    Replicate,
    /// Deliveries, gap notices, acknowledgements and redirects.
    Deliver,
    /// Publishes.
    Publish,
}

/// What a message is.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Kind {
    /// A client asks to receive a stream from an offset.
    Subscribe,
    /// A client's batch for a stream.
    Publish,
    /// A node's stored batch for a subscriber.
    Deliver,
    /// A node tells a subscriber the offsets it missed.
    Gap,
    /// A part's group's message, from one of its nodes to another.
    Raft,
    /// A node acknowledges a publish its part's group committed.
    Ack,
    /// A node that does not lead the part names the leader it knows.
    Redirect,
    /// A node's heartbeat to another, on their liveness lane.
    Heartbeat,
}

/// A sender's role, as the directory names it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Role {
    /// A bus node.
    Node,
    /// A client: an agent's process, a claimant or a respondent.
    Client,
}

/// The lane every client publishes on and every node delivers on: one ordered lane each way a
/// connection. Between nodes, the lane a part's group's messages travel on.
pub const LANE: hyper_transport::LaneId = 0;
/// The lane two nodes' heartbeats travel on, apart from their groups' messages: a heartbeat waits
/// behind no append (QUIC's streams are ordered each on its own, RFC 9000 §2.2).
pub const LIVENESS_LANE: hyper_transport::LaneId = 1;

/// Format: the largest exchange head or reply, a subscribe's head or its reply
/// ([`Subscribe::BYTES`], [`Reply::BYTES`]); a control exchange carries nothing else
/// (`Limits::max_head` is this bound).
pub const HEAD_BOUND: u64 = {
    let largest = if Subscribe::BYTES > Reply::BYTES {
        Subscribe::BYTES
    } else {
        Reply::BYTES
    };
    largest as u64
};
/// Format: the largest control message: an exchange's head or reply ([`HEAD_BOUND`]), or a
/// heartbeat, `hyper_liveness`'s largest.
pub const CONTROL_BOUND: u64 = {
    let heartbeat = hyper_liveness::MAX_BYTES as u64;
    if heartbeat > HEAD_BOUND {
        heartbeat
    } else {
        HEAD_BOUND
    }
};
/// Cited: the largest batch the bus carries, the bound NATS and Kafka ship with: 1 MiB, nats-server's
/// `MAX_PAYLOAD_SIZE` ("should be using something different if > 1MB payloads are needed"), and
/// Kafka's `message.max.bytes`, the largest record batch a broker takes, 1 MiB and its 12-byte log
/// overhead (`ServerLogConfigs.MAX_MESSAGE_BYTES_DEFAULT`). Every message larger is an artifact,
/// referenced (doc 32 §1.1). A part's segment holds [`SEGMENT_BATCHES`] of them
/// (`NodeConfig::SEGMENT_BYTES`).
pub const BATCH_BOUND: u64 = 1 << 20;
/// Derived: the bytes of a group's entry that holds the largest batch, as hyper-raft's wire writes
/// it: the entry's fixed bytes and the batch, the context empty (a part's entries carry none). An
/// append carries entries of at most these bytes, or one entry (`Config::max_size_per_msg`).
pub const ENTRY_BOUND: u64 = (hyper_raft::wire::ENTRY_FIXED_BYTES as u64) + BATCH_BOUND;
/// Derived: the largest of a group's messages, a leader's append of [`ENTRY_BOUND`] bytes of
/// entries and its fixed record (`hyper_raft::wire::MESSAGE_RECORD_FIXED_BYTES`, which no message
/// of the core's exceeds): what hyper-raft is told its transport carries (`Stated::message`).
pub const RAFT_BOUND: u64 = (hyper_raft::wire::MESSAGE_RECORD_FIXED_BYTES as u64) + ENTRY_BOUND;
/// Cited: the largest batches a segment holds. A batch is never split, so a segment's tail leaves
/// unused at most what the next batch needs; five of the largest hold that to a fifth of the
/// segment, the most the allocator itself wastes rounding a request up: jemalloc's four size
/// classes a doubling "limit internal fragmentation to approximately 20%" (jemalloc(3)).
pub const SEGMENT_BATCHES: u64 = 5;

const KINDS: [Kind; 8] = [
    Kind::Subscribe,
    Kind::Publish,
    Kind::Deliver,
    Kind::Gap,
    Kind::Raft,
    Kind::Ack,
    Kind::Redirect,
    Kind::Heartbeat,
];

/// The bus's classes for hyper-transport.
#[derive(Clone, Copy, Debug)]
pub struct Bus;

impl Classes for Bus {
    type Class = Class;
    type Kind = Kind;
    type Role = Role;
    const RANKS: u8 = 4;

    fn rank(class: Class) -> u8 {
        match class {
            Class::Control => 0,
            Class::Replicate => 1,
            Class::Deliver => 2,
            Class::Publish => 3,
        }
    }

    fn class_of(kind: Kind, role: Role) -> Option<Class> {
        match (kind, role) {
            (Kind::Subscribe, Role::Client) | (Kind::Heartbeat, Role::Node) => Some(Class::Control),
            (Kind::Raft, Role::Node) => Some(Class::Replicate),
            (Kind::Publish, Role::Client) => Some(Class::Publish),
            (Kind::Deliver | Kind::Gap | Kind::Ack | Kind::Redirect, Role::Node) => {
                Some(Class::Deliver)
            }
            _ => None,
        }
    }

    fn frame_bound(class: Class) -> u64 {
        match class {
            Class::Control => CONTROL_BOUND,
            Class::Replicate => RAFT_BOUND,
            Class::Deliver | Class::Publish => BATCH_BOUND,
        }
    }

    fn kind_code(kind: Kind) -> u16 {
        match kind {
            Kind::Subscribe => 1,
            Kind::Publish => 2,
            Kind::Deliver => 3,
            Kind::Gap => 4,
            Kind::Raft => 5,
            Kind::Ack => 6,
            Kind::Redirect => 7,
            Kind::Heartbeat => 8,
        }
    }

    fn kind_of(code: u16) -> Option<Kind> {
        KINDS.get(usize::from(code).checked_sub(1)?).copied()
    }

    /// A publish and a delivery are their sender's only copy of a batch, and a group's message
    /// its leader's word in order: a lane that cannot hold one now waits for room, its sender held
    /// by flow control, and never skips it. A heartbeat is not: one that waits is a late one, and
    /// the stream judges lateness.
    fn lossless(class: Class) -> bool {
        matches!(class, Class::Publish | Class::Deliver | Class::Replicate)
    }

    /// A publish and a delivery are one batch each, which carries its producer's CRC
    /// ([`crate::batch::EMBEDDED`]): the transport covers the rest and checks that, so a node sums
    /// each batch once, not three times (its prefix's, the batch's, and its prefix's again to
    /// deliver it), and a subscriber once, not twice. A group's message carries its record's own
    /// CRC, which the transport's covers again.
    fn embedded_checksum(kind: Kind) -> Option<hyper_transport::Embedded> {
        matches!(kind, Kind::Publish | Kind::Deliver).then_some(crate::batch::EMBEDDED)
    }
}

fn u64_at(bytes: &[u8], at: usize) -> Option<u64> {
    bytes
        .get(at..at.checked_add(8)?)
        .and_then(|field| field.try_into().ok())
        .map(u64::from_le_bytes)
}

/// Where a subscription starts.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum From {
    /// At this offset, or the stream's floor if it is below it.
    Offset(u64),
    /// At the stream's next offset: what is published from now on.
    Tail,
}

/// A subscribe's head: the stream and where to start.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Subscribe {
    /// The stream.
    pub stream: u64,
    /// Where to start.
    pub from: From,
}

impl Subscribe {
    /// The head's bytes: 17, the stream, a tag and the offset.
    pub const BYTES: usize = 17;
    const TAIL: u8 = 1;

    /// The head as sent.
    pub fn encode(&self) -> [u8; Self::BYTES] {
        let mut out = [0; Self::BYTES];
        let (tag, offset) = match self.from {
            From::Offset(offset) => (0, offset),
            From::Tail => (Self::TAIL, 0),
        };
        for (slot, byte) in out.iter_mut().zip(
            self.stream
                .to_le_bytes()
                .into_iter()
                .chain([tag])
                .chain(offset.to_le_bytes()),
        ) {
            *slot = byte;
        }
        out
    }

    /// The head received.
    pub fn decode(bytes: &[u8]) -> Result<Self, BusError> {
        if bytes.len() != Self::BYTES {
            return Err(BusError::Protocol("a subscribe's head"));
        }
        let stream = u64_at(bytes, 0).ok_or(BusError::Protocol("a subscribe's stream"))?;
        let offset = u64_at(bytes, 9).ok_or(BusError::Protocol("a subscribe's offset"))?;
        let from = match bytes.get(8) {
            Some(0) => From::Offset(offset),
            Some(&Self::TAIL) => From::Tail,
            _ => return Err(BusError::Protocol("a subscribe's start")),
        };
        Ok(Self { stream, from })
    }
}

/// A subscribe's reply: the stream's retained window, `[floor, next)`, and where the subscription
/// starts.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Subscribed {
    /// The first offset the node still holds.
    pub floor: u64,
    /// The offset the stream's next record takes.
    pub next: u64,
    /// The offset the first delivery holds.
    pub start: u64,
}

impl Subscribed {
    /// The reply's bytes.
    pub const BYTES: usize = 24;

    /// The reply as sent.
    pub fn encode(&self) -> [u8; Self::BYTES] {
        let mut out = [0; Self::BYTES];
        for (slot, byte) in out.iter_mut().zip(
            self.floor
                .to_le_bytes()
                .into_iter()
                .chain(self.next.to_le_bytes())
                .chain(self.start.to_le_bytes()),
        ) {
            *slot = byte;
        }
        out
    }

    /// The reply received.
    pub fn decode(bytes: &[u8]) -> Result<Self, BusError> {
        if bytes.len() != Self::BYTES {
            return Err(BusError::Protocol("a subscribe's reply"));
        }
        let field = |at| u64_at(bytes, at).ok_or(BusError::Protocol("a subscribe's reply"));
        Ok(Self {
            floor: field(0)?,
            next: field(8)?,
            start: field(16)?,
        })
    }
}

/// A gap notice: `stream`'s offsets `[from, to)` were let go before the subscriber took them.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Gap {
    /// The stream.
    pub stream: u64,
    /// The first offset missed.
    pub from: u64,
    /// The offset delivery goes on from.
    pub to: u64,
}

impl Gap {
    /// The notice's bytes.
    pub const BYTES: usize = 24;

    /// The notice as sent.
    pub fn encode(&self) -> [u8; Self::BYTES] {
        let mut out = [0; Self::BYTES];
        for (slot, byte) in out.iter_mut().zip(
            self.stream
                .to_le_bytes()
                .into_iter()
                .chain(self.from.to_le_bytes())
                .chain(self.to.to_le_bytes()),
        ) {
            *slot = byte;
        }
        out
    }

    /// The notice received.
    pub fn decode(bytes: &[u8]) -> Result<Self, BusError> {
        if bytes.len() != Self::BYTES {
            return Err(BusError::Protocol("a gap notice"));
        }
        let field = |at| u64_at(bytes, at).ok_or(BusError::Protocol("a gap notice"));
        Ok(Self {
            stream: field(0)?,
            from: field(8)?,
            to: field(16)?,
        })
    }
}

/// A field of `bytes` at `at`, little-endian.
fn u32_at(bytes: &[u8], at: usize) -> Option<u32> {
    bytes
        .get(at..at.checked_add(4)?)
        .and_then(|field| field.try_into().ok())
        .map(u32::from_le_bytes)
}

fn u16_at(bytes: &[u8], at: usize) -> Option<u16> {
    bytes
        .get(at..at.checked_add(2)?)
        .and_then(|field| field.try_into().ok())
        .map(u16::from_le_bytes)
}

/// Writes `fields`' bytes into `out` in order: each a little-endian integer's bytes.
fn put(out: &mut [u8], fields: &[&[u8]]) {
    let mut slots = out.iter_mut();
    for field in fields {
        // The field's bytes lead: `zip` takes from its left first, so with the slots on the left
        // it would take one slot past each field and drop it.
        for (byte, slot) in field.iter().zip(slots.by_ref()) {
            *slot = *byte;
        }
    }
}

/// The node a part's leader is, as another node knows it, and the term it leads in: a client takes
/// it only where the term is past what it knew (Kafka's KIP-951 takes a new leader only where its
/// epoch advances the client's view). A node that knows no leader names none.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Leader {
    /// The leader's node, as the directory names it; none known when the node knows no leader.
    pub node: Option<u64>,
    /// The term the node knows the part in.
    pub term: u64,
}

impl Leader {
    /// Its bytes: the node (zero for none) and the term.
    pub const BYTES: usize = 16;

    fn encode_into(&self, out: &mut [u8]) {
        put(
            out,
            &[
                &self.node.unwrap_or(0).to_le_bytes(),
                &self.term.to_le_bytes(),
            ],
        );
    }

    fn decode_at(bytes: &[u8], at: usize) -> Option<Self> {
        let node = u64_at(bytes, at)?;
        let term = u64_at(bytes, at.checked_add(8)?)?;
        Some(Self {
            node: (node != 0).then_some(node),
            term,
        })
    }
}

/// A subscribe's reply: where the subscription starts, or the leader of the part, from a node that
/// does not lead it (doc 34 P7).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Reply {
    /// The subscription is made.
    Subscribed(Subscribed),
    /// The node does not lead the part: subscribe to the leader it names.
    Redirected(Leader),
}

impl Reply {
    /// The reply's bytes: a tag, then a [`Subscribed`] or a [`Leader`] and zeros to the length.
    pub const BYTES: usize = 1 + Subscribed::BYTES;
    const SUBSCRIBED: u8 = 0;
    const REDIRECTED: u8 = 1;

    /// The reply as sent.
    pub fn encode(&self) -> [u8; Self::BYTES] {
        let mut out = [0; Self::BYTES];
        match self {
            Self::Subscribed(subscribed) => {
                put(&mut out, &[&[Self::SUBSCRIBED], &subscribed.encode()]);
            }
            Self::Redirected(leader) => {
                put(&mut out, &[&[Self::REDIRECTED]]);
                if let Some(body) = out.get_mut(1..) {
                    leader.encode_into(body);
                }
            }
        }
        out
    }

    /// The reply received.
    pub fn decode(bytes: &[u8]) -> Result<Self, BusError> {
        if bytes.len() != Self::BYTES {
            return Err(BusError::Protocol("a subscribe's reply"));
        }
        let body = bytes
            .get(1..)
            .ok_or(BusError::Protocol("a subscribe's reply"))?;
        match bytes.first() {
            Some(&Self::SUBSCRIBED) => Subscribed::decode(body).map(Self::Subscribed),
            Some(&Self::REDIRECTED) => Leader::decode_at(body, 0)
                .map(Self::Redirected)
                .ok_or(BusError::Protocol("a subscribe's redirect")),
            _ => Err(BusError::Protocol("a subscribe's reply")),
        }
    }
}

/// A publish's acknowledgement: its batch, named by its stream, producer and sequence, was
/// committed by its part's group and applied by the leader at `base`, its `count` records taking
/// `[base, base + count)`, in the class `class` states (doc 34 P5; doc 33 §4.8). A batch the part
/// had already applied (its producer sent it again) is acknowledged with the offsets it took then,
/// or with none where the part no longer says (`count` zero); one past a gap in its session's order
/// is refused [`Ack::OUT_OF_ORDER`], `base` the sequence its session waits for (doc 34 P6).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Ack {
    /// The batch's stream.
    pub stream: u64,
    /// Its producer.
    pub producer: u64,
    /// Its producer's sequence of its first record.
    pub sequence: u32,
    /// The offset of its first record.
    pub base: u64,
    /// Its records.
    pub count: u16,
    /// What its commit achieved: [`Durability`]'s code.
    pub class: u8,
}

impl Ack {
    /// The acknowledgement's bytes.
    pub const BYTES: usize = 8 + 8 + 4 + 8 + 2 + 1;
    /// The class of a batch the part refused alike at every node (no batch the part takes, a
    /// stream past its bound): its producer must not send it again.
    pub const REFUSED: u8 = u8::MAX;
    /// The class of a batch past a gap in its session's order, not appended: its producer sends
    /// again, in order, from the sequence `base` states (Kafka's `OUT_OF_ORDER_SEQUENCE_NUMBER`).
    pub const OUT_OF_ORDER: u8 = u8::MAX - 1;

    /// The acknowledgement as sent.
    pub fn encode(&self) -> [u8; Self::BYTES] {
        let mut out = [0; Self::BYTES];
        put(
            &mut out,
            &[
                &self.stream.to_le_bytes(),
                &self.producer.to_le_bytes(),
                &self.sequence.to_le_bytes(),
                &self.base.to_le_bytes(),
                &self.count.to_le_bytes(),
                &[self.class],
            ],
        );
        out
    }

    /// The acknowledgement received.
    pub fn decode(bytes: &[u8]) -> Result<Self, BusError> {
        let field = |found: Option<u64>| found.ok_or(BusError::Protocol("an acknowledgement"));
        if bytes.len() != Self::BYTES {
            return Err(BusError::Protocol("an acknowledgement"));
        }
        Ok(Self {
            stream: field(u64_at(bytes, 0))?,
            producer: field(u64_at(bytes, 8))?,
            sequence: u32_at(bytes, 16).ok_or(BusError::Protocol("an acknowledgement"))?,
            base: field(u64_at(bytes, 20))?,
            count: u16_at(bytes, 28).ok_or(BusError::Protocol("an acknowledgement"))?,
            class: *bytes
                .get(30)
                .ok_or(BusError::Protocol("an acknowledgement"))?,
        })
    }
}

/// What a commit achieved (doc 32 D2, doc 33 §4.2): the code an [`Ack`] carries.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Durability {
    /// Held by its node alone.
    Ephemeral,
    /// Held by a majority, followers in memory under their promises, across the fates the stream
    /// asks.
    Replicated,
    /// Held by a majority on stable media.
    Persisted,
}

impl Durability {
    /// The code an acknowledgement carries.
    pub const fn code(self) -> u8 {
        match self {
            Self::Ephemeral => 0,
            Self::Replicated => 1,
            Self::Persisted => 2,
        }
    }

    /// The durability of `code`.
    pub const fn of(code: u8) -> Option<Self> {
        match code {
            0 => Some(Self::Ephemeral),
            1 => Some(Self::Replicated),
            2 => Some(Self::Persisted),
            _ => None,
        }
    }
}

/// A publish a node did not take, for it does not lead the batch's part, and the leader it knows
/// (doc 34 P2): the client sends it again to that leader.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Redirect {
    /// The batch's stream.
    pub stream: u64,
    /// Its producer.
    pub producer: u64,
    /// Its producer's sequence of its first record.
    pub sequence: u32,
    /// The leader the node knows.
    pub leader: Leader,
}

impl Redirect {
    /// The redirect's bytes.
    pub const BYTES: usize = 8 + 8 + 4 + Leader::BYTES;

    /// The redirect as sent.
    pub fn encode(&self) -> [u8; Self::BYTES] {
        let mut out = [0; Self::BYTES];
        put(
            &mut out,
            &[
                &self.stream.to_le_bytes(),
                &self.producer.to_le_bytes(),
                &self.sequence.to_le_bytes(),
            ],
        );
        if let Some(leader) = out.get_mut(20..) {
            self.leader.encode_into(leader);
        }
        out
    }

    /// The redirect received.
    pub fn decode(bytes: &[u8]) -> Result<Self, BusError> {
        if bytes.len() != Self::BYTES {
            return Err(BusError::Protocol("a redirect"));
        }
        let refused = || BusError::Protocol("a redirect");
        Ok(Self {
            stream: u64_at(bytes, 0).ok_or_else(refused)?,
            producer: u64_at(bytes, 8).ok_or_else(refused)?,
            sequence: u32_at(bytes, 16).ok_or_else(refused)?,
            leader: Leader::decode_at(bytes, 20).ok_or_else(refused)?,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn heads_round_trip_and_a_wrong_length_is_refused() {
        for from in [From::Offset(0), From::Offset(u64::MAX - 1), From::Tail] {
            let head = Subscribe { stream: 77, from };
            assert_eq!(Subscribe::decode(&head.encode()).unwrap(), head);
        }
        let reply = Subscribed {
            floor: 3,
            next: 9,
            start: 4,
        };
        assert_eq!(Subscribed::decode(&reply.encode()).unwrap(), reply);
        let gap = Gap {
            stream: 5,
            from: 10,
            to: 20,
        };
        assert_eq!(Gap::decode(&gap.encode()).unwrap(), gap);
        assert!(Subscribe::decode(&[0; 16]).is_err());
        let mut bad = Subscribe {
            stream: 1,
            from: From::Tail,
        }
        .encode();
        bad[8] = 9;
        assert!(Subscribe::decode(&bad).is_err());
    }

    #[test]
    fn only_a_client_publishes_or_subscribes_and_only_a_node_delivers() {
        assert_eq!(
            Bus::class_of(Kind::Publish, Role::Client),
            Some(Class::Publish)
        );
        assert_eq!(Bus::class_of(Kind::Publish, Role::Node), None);
        assert_eq!(Bus::class_of(Kind::Deliver, Role::Client), None);
        assert_eq!(Bus::class_of(Kind::Gap, Role::Node), Some(Class::Deliver));
        for kind in KINDS {
            assert_eq!(Bus::kind_of(Bus::kind_code(kind)), Some(kind));
        }
        assert_eq!(Bus::kind_of(0), None);
        assert_eq!(Bus::kind_of(9), None);
    }

    #[test]
    fn only_a_node_sends_a_groups_messages_heartbeats_acknowledgements_and_redirects() {
        assert_eq!(
            Bus::class_of(Kind::Raft, Role::Node),
            Some(Class::Replicate)
        );
        assert_eq!(Bus::class_of(Kind::Raft, Role::Client), None);
        assert_eq!(
            Bus::class_of(Kind::Heartbeat, Role::Node),
            Some(Class::Control)
        );
        assert_eq!(Bus::class_of(Kind::Heartbeat, Role::Client), None);
        for kind in [Kind::Ack, Kind::Redirect] {
            assert_eq!(Bus::class_of(kind, Role::Node), Some(Class::Deliver));
            assert_eq!(Bus::class_of(kind, Role::Client), None);
        }
        // A group's messages go ahead of deliveries, and wait for room rather than be skipped.
        assert!(Bus::rank(Class::Replicate) < Bus::rank(Class::Deliver));
        assert!(Bus::lossless(Class::Replicate));
        assert!(!Bus::lossless(Class::Control));
        // The largest append a part's group sends fits its frame, and a heartbeat fits control's.
        assert_eq!(Bus::frame_bound(Class::Replicate), RAFT_BOUND);
        const { assert!(RAFT_BOUND > BATCH_BOUND) };
        const { assert!(CONTROL_BOUND >= hyper_liveness::MAX_BYTES as u64) };
    }

    #[test]
    fn replies_acknowledgements_and_redirects_round_trip_and_a_wrong_length_is_refused() {
        let subscribed = Reply::Subscribed(Subscribed {
            floor: 1,
            next: 2,
            start: 1,
        });
        assert_eq!(Reply::decode(&subscribed.encode()).unwrap(), subscribed);
        for node in [None, Some(3)] {
            let redirected = Reply::Redirected(Leader { node, term: 7 });
            assert_eq!(Reply::decode(&redirected.encode()).unwrap(), redirected);
        }
        let mut bad = subscribed.encode();
        bad[0] = 2;
        assert!(Reply::decode(&bad).is_err());
        assert!(Reply::decode(&[0; Reply::BYTES - 1]).is_err());

        let ack = Ack {
            stream: u64::MAX,
            producer: 9,
            sequence: u32::MAX,
            base: 1 << 40,
            count: u16::MAX,
            class: Durability::Persisted.code(),
        };
        assert_eq!(Ack::decode(&ack.encode()).unwrap(), ack);
        assert!(Ack::decode(&[0; Ack::BYTES + 1]).is_err());
        for durability in [
            Durability::Ephemeral,
            Durability::Replicated,
            Durability::Persisted,
        ] {
            assert_eq!(Durability::of(durability.code()), Some(durability));
        }
        assert_eq!(Durability::of(3), None);

        let redirect = Redirect {
            stream: 4,
            producer: 5,
            sequence: 6,
            leader: Leader {
                node: Some(2),
                term: 11,
            },
        };
        assert_eq!(Redirect::decode(&redirect.encode()).unwrap(), redirect);
        assert!(Redirect::decode(&[0; Redirect::BYTES - 1]).is_err());
    }
}
