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
//!   states the stream's retained window, `[floor, next)`.
//!
//! The kind and the sender's role give the class, never the sender: a client cannot send a
//! delivery, a node no publish. Subscribes are control, ahead of the flow they start; deliveries go
//! ahead of publishes, so a node drains to its subscribers before it takes more.

use hyper_transport::Classes;

use crate::BusError;

/// A message's urgency, the most urgent first.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Class {
    /// Subscriptions and their answers.
    Control,
    /// Deliveries and gap notices.
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
/// connection.
pub const LANE: hyper_transport::LaneId = 0;

/// Format: the largest control message, a subscribe's head or its reply's ([`Subscribe::BYTES`],
/// [`Subscribed::BYTES`]); a control exchange carries nothing else (`Limits::max_head` is the
/// same bound).
pub const CONTROL_BOUND: u64 = {
    let largest = if Subscribe::BYTES > Subscribed::BYTES {
        Subscribe::BYTES
    } else {
        Subscribed::BYTES
    };
    largest as u64
};
/// Cited: the largest batch the bus carries, the bound NATS and Kafka ship with: 1 MiB, nats-server's
/// `MAX_PAYLOAD_SIZE` ("should be using something different if > 1MB payloads are needed"), and
/// Kafka's `message.max.bytes`, the largest record batch a broker takes, 1 MiB and its 12-byte log
/// overhead (`ServerLogConfigs.MAX_MESSAGE_BYTES_DEFAULT`). Every message larger is an artifact,
/// referenced (doc 32 §1.1). A part's segment holds [`SEGMENT_BATCHES`] of them
/// (`NodeConfig::SEGMENT_BYTES`).
pub const BATCH_BOUND: u64 = 1 << 20;
/// Cited: the largest batches a segment holds. A batch is never split, so a segment's tail leaves
/// unused at most what the next batch needs; five of the largest hold that to a fifth of the
/// segment, the most the allocator itself wastes rounding a request up: jemalloc's four size
/// classes a doubling "limit internal fragmentation to approximately 20%" (jemalloc(3)).
pub const SEGMENT_BATCHES: u64 = 5;

const KINDS: [Kind; 4] = [Kind::Subscribe, Kind::Publish, Kind::Deliver, Kind::Gap];

/// The bus's classes for hyper-transport.
#[derive(Clone, Copy, Debug)]
pub struct Bus;

impl Classes for Bus {
    type Class = Class;
    type Kind = Kind;
    type Role = Role;
    const RANKS: u8 = 3;

    fn rank(class: Class) -> u8 {
        match class {
            Class::Control => 0,
            Class::Deliver => 1,
            Class::Publish => 2,
        }
    }

    fn class_of(kind: Kind, role: Role) -> Option<Class> {
        match (kind, role) {
            (Kind::Subscribe, Role::Client) => Some(Class::Control),
            (Kind::Publish, Role::Client) => Some(Class::Publish),
            (Kind::Deliver | Kind::Gap, Role::Node) => Some(Class::Deliver),
            _ => None,
        }
    }

    fn frame_bound(class: Class) -> u64 {
        match class {
            Class::Control => CONTROL_BOUND,
            Class::Deliver | Class::Publish => BATCH_BOUND,
        }
    }

    fn kind_code(kind: Kind) -> u16 {
        match kind {
            Kind::Subscribe => 1,
            Kind::Publish => 2,
            Kind::Deliver => 3,
            Kind::Gap => 4,
        }
    }

    fn kind_of(code: u16) -> Option<Kind> {
        KINDS.get(usize::from(code).checked_sub(1)?).copied()
    }

    /// A publish and a delivery are their sender's only copy of a batch: a lane that cannot hold
    /// one now waits for room, its sender held by flow control, and never skips it.
    fn lossless(class: Class) -> bool {
        matches!(class, Class::Publish | Class::Deliver)
    }

    /// A publish and a delivery are one batch each, which carries its producer's CRC
    /// ([`crate::batch::EMBEDDED`]): the transport covers the rest and checks that, so a node sums
    /// each batch once, not three times (its prefix's, the batch's, and its prefix's again to
    /// deliver it), and a subscriber once, not twice.
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
        assert_eq!(Bus::kind_of(5), None);
    }
}
