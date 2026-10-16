//! Every way the transport says no, typed. A refusal that crosses the wire travels as the
//! application error code of a stream reset (RFC 9000 §19.4), so the side that asked learns which
//! bound it met, not merely that its stream ended.

/// Why the transport refused an operation, or why an exchange ended without completing.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub enum Refusal {
    /// The sender's role may not send a message of this kind (audit §13.3, node.md §3.6).
    #[error("the sender's role may not send this kind of message")]
    Kind,
    /// The message is longer than its class's frame bound, or its head longer than the head
    /// bound; checked from the prefix, before any byte past it is read.
    #[error("the message is past the bound of its class")]
    FrameBound,
    /// The node's budget cannot fund the bytes.
    #[error("the budget cannot fund the bytes")]
    Budget,
    /// The handshakes in progress are at their bound.
    #[error("too many handshakes are in progress")]
    Pending,
    /// The identities holding connections are at their bound.
    #[error("too many identities hold connections")]
    Identities,
    /// The connections held in all are at their bound, after the replacement rule.
    #[error("too many connections are held")]
    Connections,
    /// The identity holds every connection it may and none of them is idle: each carries an
    /// exchange or carried something within the quiet period, so none is replaced.
    #[error("the identity holds every connection it may, and is using each")]
    Busy,
    /// The peer's certificate names no peer of the directory, or not the one dialed.
    #[error("the certificate names no peer the directory knows")]
    Identity,
    /// The exchange table is at its bound.
    #[error("too many exchanges are open")]
    Exchanges,
    /// The lane's queue holds a whole window of the core's frames already.
    #[error("the lane holds the core's whole window")]
    LaneFull,
    /// The lanes to or from a peer are at their bound.
    #[error("too many lanes")]
    Lanes,
    /// There is no established connection to the peer.
    #[error("no connection to the peer")]
    NotConnected,
    /// No exchange has this identifier, or it has ended.
    #[error("no such exchange")]
    UnknownExchange,
    /// The exchange cannot do this now: a reply on an exchange this side opened, a body longer
    /// than it declared, a second reply.
    #[error("the exchange does not allow this now")]
    Order,
    /// A prefix, head, body or frame did not verify: a malformed prefix or a CRC-32C mismatch.
    #[error("a checksum or a prefix did not verify")]
    Corrupt,
    /// A period of the exchange's progress deadline moved less than a datagram (T39).
    #[error("a period moved less than a datagram")]
    Stalled,
    /// The exchange waited for a stream its peer did not grant for a whole period of its progress
    /// deadline (policy C): none of it was sent, so the peer never saw it. Distinct from
    /// [`Refusal::Stalled`], whose exchange may have reached the peer, so an asker knows a request
    /// refused so was not taken, as a peer granting streams as wanted (`StreamCredit::Pooled`)
    /// holds requests back while it is busy.
    #[error("the exchange waited a period for a stream it was not granted: nothing was sent")]
    Unsent,
    /// The connection closed, or the exchange was abandoned by its owner.
    #[error("the connection closed")]
    Closed,
    /// Exchange identifiers are exhausted (T4): the table's slot generations ran out.
    #[error("exchange identifiers are exhausted")]
    Exhausted,
    /// The QUIC layer refused the operation.
    #[error("the QUIC layer refused")]
    Quic,
    /// The configuration is invalid.
    #[error("invalid configuration")]
    Configuration,
    /// Every entry of the peer table is held by events the owner has not polled, or by the
    /// connections and dials that will end in them: no new peer is taken until it polls them
    /// (`Limits::event_bound`).
    #[error("the owner's unpolled events hold every peer entry")]
    Events,
    /// The peer refused with a code this side does not know.
    #[error("the peer refused with an unknown code")]
    Unknown,
    /// The socket's receive queue cannot hold another connection's window: what the peers may send
    /// unread, summed, never passes what the queue holds (`Endpoint::set_receive_capacity`).
    #[error("the socket's receive queue holds no more windows")]
    ReceiveBuffer,
}

/// The refusals that cross the wire, in the order of their codes; a code is its index plus one,
/// so that zero, QUIC's conventional "no error", is never a refusal. A refusal is only ever added
/// at the end, so a code keeps its meaning between versions. The admission bounds a connection is
/// closed at (codes 10 to 14, and the receive queue's, 15) cross as the connection's close code,
/// so a dialer learns that the peer was full rather than gone (`Event::Unreachable`).
const WIRE: [Refusal; 15] = [
    Refusal::Kind,
    Refusal::FrameBound,
    Refusal::Budget,
    Refusal::Corrupt,
    Refusal::Stalled,
    Refusal::Closed,
    Refusal::Exchanges,
    Refusal::Lanes,
    Refusal::Identity,
    Refusal::Pending,
    Refusal::Identities,
    Refusal::Connections,
    Refusal::Events,
    Refusal::Busy,
    Refusal::ReceiveBuffer,
];

impl Refusal {
    /// The application error code a stream reset carries for this refusal.
    /// A refusal that does not cross the wire resets as a closed exchange.
    pub(crate) fn code(self) -> u32 {
        let index = |refusal: Self| WIRE.iter().position(|wire| *wire == refusal);
        let index = index(self).or_else(|| index(Self::Closed)).unwrap_or(0);
        u32::try_from(index).unwrap_or(0).saturating_add(1)
    }
    /// The refusal a peer's stream reset carries.
    pub(crate) fn from_code(code: u64) -> Self {
        code.checked_sub(1)
            .and_then(|index| usize::try_from(index).ok())
            .and_then(|index| WIRE.get(index).copied())
            .unwrap_or(Self::Unknown)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_wire_refusal_survives_its_code() {
        for refusal in WIRE {
            assert_eq!(Refusal::from_code(u64::from(refusal.code())), refusal);
        }
        assert_eq!(Refusal::from_code(0), Refusal::Unknown);
        assert_eq!(Refusal::from_code(u64::MAX), Refusal::Unknown);
        // A refusal that never crosses the wire resets as a closed exchange.
        assert_eq!(
            Refusal::from_code(u64::from(Refusal::Order.code())),
            Refusal::Closed
        );
    }

    #[test]
    fn a_refusal_keeps_its_code_as_refusals_are_added() {
        // Appended, never renumbered: what an older peer sends means what it meant.
        let codes: Vec<u32> = WIRE.iter().map(|refusal| refusal.code()).collect();
        assert_eq!(codes, (1..=15).collect::<Vec<u32>>());
        assert_eq!(Refusal::Kind.code(), 1);
        assert_eq!(Refusal::Identity.code(), 9);
        assert_eq!(Refusal::Pending.code(), 10);
        assert_eq!(Refusal::Events.code(), 13);
        assert_eq!(Refusal::Busy.code(), 14);
        assert_eq!(Refusal::ReceiveBuffer.code(), 15);
    }
}
