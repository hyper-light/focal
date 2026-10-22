//! The bus's bounds on hyper-transport, a node's and a client's: each is the bus's own protocol,
//! the transport's own ceiling, or a cited source, as hyper-transport asks of its owner ("every one
//! is the owner's configuration, derived by its own law", `Limits`). A node and a client take them
//! from here; neither states a number of its own.

use std::time::Duration;

use hyper_rt_transport::Io;
use hyper_transport::{AdmissionLimits, AskQueue, Limits, StreamCredit};

use crate::BusError;
use crate::batch::MIN_BATCH;
use crate::wire::{Gap, HEAD_BOUND};

/// Cited: the datagrams one system call carries each way: the packets one NAPI poll hands the
/// kernel's network stack at most, the default poll weight drivers "are strongly advised to not
/// use bigger" (`NAPI_POLL_WEIGHT`, include/linux/netdevice.h). A receive call then takes a poll's
/// burst whole, and one that fills fewer places than it offers says the socket is drained
/// (hyper-rt's batched receive).
pub const IO: Io = Io { batch: 64 };

/// Derived: what a node of a part's group keeps of its admitted memory from what inputs reserve,
/// for what its replica comes to hold past their reservations, which cannot be refused: one entry
/// (`wire::ENTRY_BOUND`), the most one call grows by (hyper-durable's charge).
pub const GROUP_COMPLETION_RESERVE: usize = if crate::wire::ENTRY_BOUND > usize::MAX as u64 {
    usize::MAX
} else {
    crate::wire::ENTRY_BOUND as usize
};

/// Derived: what applying a committed entry may take past what a node of a part's group admits,
/// within its memory: the window's segment (`NodeConfig::SEGMENT_BYTES`), taken on the committed
/// lane ([`focal_memory::BudgetLane::Committed`]: what a committed fact requires where it
/// applies, held to the hold limit, doc 31 M3). Applying gives back the entries the replica kept,
/// and the window recycles its oldest segments as the tail fills, so what was admitted falls back
/// within its bound. Without it, a follower that took appends while it could hold them had no
/// segment left to apply them into, the entries it held were given back only by applying them,
/// and its part fenced the replica for want of memory for a committed batch (2026-10-19, a 20 MiB
/// node under contention).
pub const GROUP_APPLY_HOLD: usize = crate::node::NodeConfig::SEGMENT_BYTES;

/// The memory a node of a part's group of `memory_bytes` draws on: inputs, the transport's
/// included, admitted within its memory less [`GROUP_APPLY_HOLD`] and
/// [`GROUP_COMPLETION_RESERVE`], its replica's own growth within its memory less the hold, and
/// applying committed entries within all of it. A node whose memory holds no more than these and
/// a window segment serves nothing.
pub fn group_budget(memory_bytes: usize) -> Result<focal_memory::MemoryBudget, BusError> {
    let least = GROUP_APPLY_HOLD
        .saturating_add(GROUP_COMPLETION_RESERVE)
        .saturating_add(crate::node::NodeConfig::SEGMENT_BYTES);
    if memory_bytes <= least {
        return Err(BusError::Configuration(
            "a group node's memory holds no window segment past its apply's hold and reserve",
        ));
    }
    let admitted = memory_bytes.saturating_sub(GROUP_APPLY_HOLD);
    focal_memory::MemoryBudget::new(memory_bytes, 0)
        .and_then(|root| root.child_holding(admitted, GROUP_COMPLETION_RESERVE, memory_bytes))
        .map_err(BusError::Memory)
}

/// Derived: the connections a client identity holds at once: the one it is served on, and the one
/// that replaces it after a reconnect, admitted while the old one is held until it has been quiet
/// for the serve period (hyper-transport's admission replaces only a quiet connection).
pub const CONNECTIONS_PER_CLIENT: usize = 2;

/// Format: the lanes a client opens, the protocol's one each way ([`crate::wire::LANE`]).
const LANES_PER_CLIENT: u32 = 1;
/// Format: the lanes a node of a group opens to another: its groups' messages' and its heartbeats'
/// ([`crate::wire::LANE`], [`crate::wire::LIVENESS_LANE`]), so a heartbeat waits behind no append.
const LANES_PER_NODE: u32 = 2;

/// Derived: the subscribes a connection has open at once. A node answers each in the event its
/// request arrives (`node::Node::on_event`), so a connection's next one waits at most the round trip
/// before it, its turn kept by the asker's queue ([`AskQueue::Table`]). A subscriber of many streams
/// waits a round trip a stream (open: a subscribe carried as a lane frame would wait none).
const STREAMS_PER_CONNECTION: u32 = 1;

/// Cited: QUIC's idle timeout, two minutes: the least a NAT may keep a UDP mapping (RFC 4787
/// REQ-5, "MUST NOT expire in less than two minutes"; RFC 8085 §3.5). A connection silent longer
/// has lost its path's mapping on any compliant NAT; a shorter one would close on a peer that only
/// stalled, which a busy machine's scheduler does.
pub const IDLE_TIMEOUT: Duration = Duration::from_secs(120);

/// Cited: the keep-alive interval, the shortest RFC 8085 allows ("SHOULD NOT transmit them more
/// frequently than once every 15 seconds", §3.5): a quiet connection keeps its mapping through the
/// middleboxes that time out sooner than RFC 4787 asks, and a live peer is heard eight times in each
/// idle timeout.
pub const KEEP_ALIVE: Duration = Duration::from_secs(15);

/// The period an exchange is judged at, the idle timeout: an exchange whose peer sends nothing for
/// a period ends (hyper-transport `progress`), so a shorter one would end a stalled peer's subscribe
/// sooner than its connection would end, which is the transport's whole judgement of a peer. The
/// transport never judges one sooner than the path's probe timeout.
pub const SERVE_PERIOD: Duration = IDLE_TIMEOUT;

/// Derived: the frames a lane queues, those one receive call can bring it at the least datagram
/// every QUIC path carries: [`IO`]'s datagrams of `MIN_DATAGRAM` bytes (1,200, RFC 9000 §14), each
/// holding as many of the smallest frames as fit (a gap notice, or a batch of one empty record, with
/// the transport's prefix). Past it a lane waits, its bytes in QUIC, until its owner takes a frame;
/// a lossless lane's sending side holds one frame beyond what QUIC took, whatever its window.
fn lane_window() -> Result<usize, BusError> {
    let smallest = u64::try_from(Gap::BYTES.min(MIN_BATCH))
        .map(hyper_transport::frame_grant)
        .map_err(|_| BusError::Configuration("the smallest frame"))?;
    let per_datagram = hyper_transport::MIN_DATAGRAM
        .checked_div(smallest)
        .ok_or(BusError::Configuration("the smallest frame"))?;
    usize::try_from(per_datagram)
        .ok()
        .and_then(|frames| frames.checked_mul(IO.batch))
        .ok_or(BusError::Configuration("the lane window"))
}

/// Derived: the most a connection's receive window grows to: the largest stream window the
/// transport keeps (`stream_window_ceiling`, about 2.3 MB) for each stream a peer sends on at once,
/// its lane and its subscribe. Windows grow only as they are used (the transport's auto-tuning,
/// Chromium QUIC's) and are charged as they grow; this bounds what one peer may pin.
fn window_ceiling(lanes: u32) -> u64 {
    hyper_transport::stream_window_ceiling()
        .saturating_mul(u64::from(lanes.saturating_add(STREAMS_PER_CONNECTION)))
}

/// The bounds for an endpoint admitting `identities` peers' identities, of
/// [`CONNECTIONS_PER_CLIENT`] connections each, each opening `lanes` lanes.
fn limits(identities: usize, lanes: u32) -> Result<Limits, BusError> {
    let identities = identities.max(1);
    let connections = identities
        .checked_mul(CONNECTIONS_PER_CLIENT)
        .ok_or(BusError::Configuration("connections"))?;
    let exchanges = usize::try_from(STREAMS_PER_CONNECTION)
        .ok()
        .and_then(|streams| connections.checked_mul(streams))
        .ok_or(BusError::Configuration("exchanges"))?;
    let window_ceiling = window_ceiling(lanes);
    // A full receive window of unread data, packed, and the chunk being cut (focal-wire's rule).
    let receive_chunks = usize::try_from(window_ceiling)
        .ok()
        .and_then(|window| window.checked_div(hyper_transport::RECEIVE_CHUNK))
        .and_then(|chunks| chunks.checked_add(1))
        .ok_or(BusError::Configuration("the receive window"))?;
    Ok(Limits {
        admission: AdmissionLimits {
            // Every seat's handshake may be under way at once.
            pending: connections,
            identities,
            connections,
            per_identity: CONNECTIONS_PER_CLIENT,
        },
        streams_per_connection: STREAMS_PER_CONNECTION,
        stream_credit: StreamCredit::Fixed,
        ask_queue: AskQueue::Table,
        exchanges,
        lanes_per_peer: lanes,
        lane_window: lane_window()?,
        max_head: u32::try_from(HEAD_BOUND)
            .map_err(|_| BusError::Configuration("the head bound"))?,
        window_ceiling,
        serve_period: SERVE_PERIOD,
        idle_timeout: IDLE_TIMEOUT,
        keep_alive: Some(KEEP_ALIVE),
        // What one receive of the driver hands the endpoint, each datagram answered at most once
        // (a refusal, a retry) before its next send: focal-wire's rule.
        max_responses: hyper_rt_transport::TURNS.saturating_mul(IO.batch),
        receive_chunks,
        // Every seat's handshake may be under way at once, the places the seats: the rule of half
        // of them holds.
        retry_bytes: None,
    })
}

/// A node's bounds: the client identities its directory holds, and the other nodes of its group,
/// none for a lone node. A node of a group opens a second lane to each other node, for heartbeats.
pub fn node(clients: usize, peers: usize) -> Result<Limits, BusError> {
    let identities = clients
        .checked_add(peers)
        .ok_or(BusError::Configuration("identities"))?;
    let lanes = if peers == 0 {
        LANES_PER_CLIENT
    } else {
        LANES_PER_NODE
    };
    limits(identities, lanes)
}

/// A client's bounds: the one node it dials.
pub fn client() -> Result<Limits, BusError> {
    client_of(1)
}

/// A client's bounds where the stream's part is replicated by a group of `nodes` nodes: it dials
/// whichever leads, and the one it is redirected to while the one it left is still held (doc 34
/// P2).
pub fn client_of(nodes: usize) -> Result<Limits, BusError> {
    limits(nodes, LANES_PER_CLIENT)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_bounds_are_the_transport_s_to_take() {
        let lone = node(2, 0).unwrap();
        assert_eq!(lone.admission.connections, 4);
        assert_eq!(lone.lanes_per_peer, 1);
        // A subscribe's reply, its tag and the retained window, is the largest head.
        assert_eq!(lone.max_head, 25);
        // A node of a group of three admits its two peers beside its clients, on two lanes each.
        let grouped = node(2, 2).unwrap();
        assert_eq!(grouped.admission.identities, 4);
        assert_eq!(grouped.lanes_per_peer, 2);
        // 1,200-byte datagrams of 44-byte frames (a 24-byte gap notice and its 20-byte prefix).
        assert_eq!(lone.lane_window, 64 * (1_200 / 44));
        assert!(lone.window_ceiling >= hyper_transport::initial_window(1_200));
        assert!(lone.keep_alive.unwrap() < lone.idle_timeout);
        let client = client().unwrap();
        assert_eq!(client.admission.identities, 1);
    }
}
