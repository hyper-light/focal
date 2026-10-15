//! Bounded egress from the session owner to authenticated peer connections.
use crate::control_host::{ControlReplicationFrame, DirectoryReplication};
use crate::fleet::{FleetReplication, ReplicationFrame};
use focal_wire::{Delivery, PeerConnectionPool, PeerSendError};
use futures_util::FutureExt;
use hyper_rt::sync::ChannelReceiver;
use std::collections::{BTreeMap, VecDeque};
use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::pin::pin;
use std::task::{Context, Poll};

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ReplicationReport {
    pub attempted: u64,
    /// Handed to a send: every frame attempted is sent or refused, and
    /// every frame sent is accepted, lost or refused at the pool's bound.
    pub sent: u64,
    /// Accepted by remote ingress; this is never a Raft/quorum acknowledgment.
    pub accepted: u64,
    pub lost: u64,
    pub saturated: u64,
    /// Given up before they were sent, for the room: a frame for a peer
    /// that held more than its share when the driver was full. Told to
    /// their owners, as every frame that is not accepted is.
    pub refused: u64,
    pub peak_inflight: usize,
    /// The most frames that waited for their peers' lanes at once.
    pub peak_waiting: usize,
}
#[derive(Debug, thiserror::Error)]
pub enum ReplicationDriverError {
    #[error("replication driver capacity must be between one and 65536")]
    Configuration,
    #[error("replication send task failed")]
    Worker,
    /// The set of sends under way could not be reserved, or the owner's
    /// channel refused the driver's wait (off a shard).
    #[error("replication driver refused: {0}")]
    Runtime(#[from] focal_rt::RtError),
}

/// Drain the host's bounded channel, holding at most `max_inflight` frames:
/// those being sent and those that wait for their peer's lane.
///
/// A frame is sent only when its peer has a place for it — as many at once
/// as the pool's lane to one peer holds — so a send under way never waits
/// behind another to the same peer, and a peer that stopped answering holds
/// its own lane and nothing else (the audit's F42: it held the whole
/// driver, and the frames of healthy peers waited in the owner's channel
/// behind it). The frames a peer's lane has no place for wait their turn in
/// that peer's own queue, in the order they came, what a group cannot do
/// without (`Urgent`) before its entries.
///
/// The driver never stops receiving: a frame it left in its owner's channel
/// would hold back every frame behind it, whoever they are for. It holds
/// `max_inflight` frames, sending and waiting — what the pool itself admits,
/// every connection's lane at once (`PeerPoolLimits::for_consensus`), so
/// that a burst from many groups to one peer waits here and is carried.
/// When it holds all it may, the frame that came is taken if its peer holds
/// less waiting than the peer that holds the most, whose newest waiting
/// frame is given up for it; otherwise the frame that came is given up. So
/// the room is shared by the peers that need it, and none can take
/// another's by being slow.
///
/// A frame that is not accepted — lost, refused by the peer, or given up
/// for the room — is told to its owner, whose core then probes the member
/// instead of streaming to it: nothing is dropped untold.
///
/// Closing the receiver's sender drains accepted frames; cancelling this
/// future drops the owned futures and frames and releases their allocations
/// immediately. The pool is borrowed; no task is spawned.
pub async fn drive_replication(
    receiver: ChannelReceiver<ReplicationFrame>,
    pool: &PeerConnectionPool,
    max_inflight: usize,
) -> Result<ReplicationReport, ReplicationDriverError> {
    drive(Receiver::Single(receiver), pool, max_inflight).await
}

/// The grouped receiver retains its channel accounting through all outstanding
/// sends, including after every logical session has stopped. The pool remains
/// borrowed; this adapter adds no task, queue, or ownership wrapper.
pub async fn drive_fleet_replication(
    receiver: FleetReplication,
    pool: &PeerConnectionPool,
    max_inflight: usize,
) -> Result<ReplicationReport, ReplicationDriverError> {
    drive(Receiver::Fleet(receiver), pool, max_inflight).await
}

pub async fn drive_control_replication(
    receiver: ChannelReceiver<ControlReplicationFrame>,
    pool: &PeerConnectionPool,
    max_inflight: usize,
) -> Result<ReplicationReport, ReplicationDriverError> {
    drive(Receiver::Control(receiver), pool, max_inflight).await
}

/// Directory startup retains its fixed channel allowance through this owned
/// receiver and all outstanding sends, without an extra shared wrapper.
pub async fn drive_directory_replication(
    receiver: DirectoryReplication,
    pool: &PeerConnectionPool,
    max_inflight: usize,
) -> Result<ReplicationReport, ReplicationDriverError> {
    drive(Receiver::Directory(receiver), pool, max_inflight).await
}

/// What carries a frame to its peer: the node's connection pool, or what
/// stands for it where the driver itself is tested. Both live on the
/// driver's shard: the sends are futures of the driver's own task.
trait Carrier {
    /// The exchanges with one peer the carrier has under way at once.
    fn lane(&self) -> usize;
    fn carry<'a>(
        &'a self,
        target: u64,
        request: &'a focal_wire::RequestEnvelope,
        delivery: Delivery,
    ) -> impl Future<Output = Result<(), PeerSendError>> + 'a;
}
impl Carrier for PeerConnectionPool {
    fn lane(&self) -> usize {
        self.limits().per_peer_inflight
    }
    fn carry<'a>(
        &'a self,
        target: u64,
        request: &'a focal_wire::RequestEnvelope,
        delivery: Delivery,
    ) -> impl Future<Output = Result<(), PeerSendError>> + 'a {
        self.send_as(target, request, delivery)
    }
}

enum Frame {
    Session(ReplicationFrame),
    Control(ControlReplicationFrame),
}
impl Frame {
    fn target(&self) -> u64 {
        match self {
            Self::Session(frame) => frame.target,
            Self::Control(frame) => frame.target,
        }
    }
    fn complete(&mut self, accepted: bool) {
        match self {
            Self::Session(frame) => frame.report_snapshot(accepted),
            Self::Control(frame) => frame.report_snapshot(accepted),
        }
    }
    fn lost(&mut self) {
        match self {
            Self::Session(frame) => frame.lost(),
            Self::Control(frame) => frame.lost(),
        }
    }
    fn request(&self) -> &focal_wire::RequestEnvelope {
        match self {
            Self::Session(frame) => &frame.request,
            Self::Control(frame) => &frame.request,
        }
    }
    fn delivery(&self) -> Delivery {
        match self {
            Self::Session(frame) => frame.delivery(),
            Self::Control(frame) => frame.delivery(),
        }
    }
    fn urgent(&self) -> bool {
        match self {
            Self::Session(frame) => frame.urgent,
            Self::Control(frame) => frame.urgent,
        }
    }
    /// The frame did not reach its peer: what a snapshot waits on is
    /// answered, and the owner is told.
    fn give_up(mut self) {
        self.complete(false);
        self.lost();
    }
}
/// The frames that wait for one peer's lane, and how many of its frames
/// are being sent.
#[derive(Default)]
struct Waiting {
    /// What a group cannot do without — heartbeats, votes, answers — in the
    /// order it came.
    urgent: VecDeque<Frame>,
    /// Entries and snapshots, in the order they came.
    bulk: VecDeque<Frame>,
    sending: usize,
}
impl Waiting {
    fn len(&self) -> usize {
        self.urgent.len().saturating_add(self.bulk.len())
    }
    /// The frame waits its turn. One there is no memory to queue is given
    /// back, to be given up.
    fn push(&mut self, frame: Frame) -> Option<Frame> {
        let queue = if frame.urgent() {
            &mut self.urgent
        } else {
            &mut self.bulk
        };
        if queue.try_reserve(1).is_err() {
            return Some(frame);
        }
        queue.push_back(frame);
        None
    }
    /// The next to send: what is urgent first.
    fn next(&mut self) -> Option<Frame> {
        self.urgent.pop_front().or_else(|| self.bulk.pop_front())
    }
    /// The newest that waits, entries before what is urgent: what is given
    /// up when the peer holds more than its share.
    fn newest(&mut self) -> Option<Frame> {
        self.bulk.pop_back().or_else(|| self.urgent.pop_back())
    }
}

enum Receiver {
    Single(ChannelReceiver<ReplicationFrame>),
    Fleet(FleetReplication),
    Control(ChannelReceiver<ControlReplicationFrame>),
    Directory(DirectoryReplication),
}
impl Receiver {
    /// The next frame; `None` once every sender is gone and the channel
    /// is drained. A wait the channel refuses (off a shard) is an error.
    fn poll_recv(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Result<Option<Frame>, focal_rt::RtError>> {
        let polled = match self {
            Self::Single(receiver) => pin!(receiver.recv())
                .poll(cx)
                .map(|r| r.map(Frame::Session)),
            Self::Fleet(receiver) => receiver.poll_recv(cx).map(|r| r.map(Frame::Session)),
            Self::Control(receiver) => pin!(receiver.recv())
                .poll(cx)
                .map(|r| r.map(Frame::Control)),
            Self::Directory(receiver) => receiver.poll_recv(cx).map(|r| r.map(Frame::Control)),
        };
        polled.map(|received| match received {
            Ok(frame) => Ok(Some(frame)),
            Err(hyper_rt::sync::SyncError::Closed(())) => Ok(None),
            Err(_) => Err(focal_rt::RtError::NotOnShardThread),
        })
    }
}

/// What the driver learnt: a frame from the owner (in the turn's slot, so
/// a turn is a word and a frame is moved once), the end of the owner's
/// egress, or a send that ended.
enum Event<T> {
    Received,
    Ended,
    Sent(Option<T>),
}

async fn drive<C: Carrier>(
    mut receiver: Receiver,
    pool: &C,
    max_inflight: usize,
) -> Result<ReplicationReport, ReplicationDriverError> {
    if !(1..=65536).contains(&max_inflight) {
        return Err(ReplicationDriverError::Configuration);
    }
    if hyper_rt::futures::shard_id().is_none() {
        return Err(ReplicationDriverError::Worker);
    }
    // As many to one peer at once as the pool's lane to it holds: a send
    // under way never waits for that lane behind another of this driver's.
    let lane = pool.lane().clamp(1, max_inflight);
    // The sends under way: at most `max_inflight`, the most the driver
    // holds, so a push is refused only past that bound.
    let mut tasks = focal_rt::Concurrent::new(max_inflight)?;
    let mut peers: BTreeMap<u64, Waiting> = BTreeMap::new();
    let mut waiting = 0usize;
    let mut receiving = true;
    let mut report = ReplicationReport::default();
    let send = |mut frame: Frame| {
        AssertUnwindSafe(async move {
            let target = frame.target();
            let result = pool.carry(target, frame.request(), frame.delivery()).await;
            frame.complete(result.is_ok());
            // Whatever kept it from the peer, the frame's owner is told,
            // and its core probes the member instead of streaming into a
            // void or a lane that is full (27 §3.3).
            if result.is_err() {
                frame.lost();
            }
            // Keep the whole frame, especially its Allocation, alive
            // across connection setup, retries and response receipt.
            drop(frame);
            (target, result)
        })
        .catch_unwind()
    };
    // What waits is sent only as a send to its peer ends, so nothing waits
    // once nothing is being sent.
    while receiving || !tasks.is_empty() {
        // A send that ended is taken before the next frame: its place is
        // free for the frame, which is then given up only when the driver
        // is full. The owner's channel is never left waiting for longer
        // than the ends already in hand take, each of them bounded by the
        // frames the driver holds.
        let mut slot = None;
        let event = std::future::poll_fn(|cx| {
            if !tasks.is_empty()
                && let Poll::Ready(ended) = tasks.poll_next(cx)
            {
                return Poll::Ready(Ok(Event::Sent(ended)));
            }
            if receiving && let Poll::Ready(received) = receiver.poll_recv(cx) {
                return Poll::Ready(received.map(|frame| match frame {
                    Some(frame) => {
                        slot = Some(frame);
                        Event::Received
                    }
                    None => Event::Ended,
                }));
            }
            Poll::Pending
        })
        .await?;
        match event {
            Event::Ended => receiving = false,
            Event::Received => {
                let Some(frame) = slot.take() else {
                    continue;
                };
                report.attempted = report.attempted.saturating_add(1);
                let target = frame.target();
                let held = peers.get(&target).map_or(0, Waiting::len);
                if tasks.len().saturating_add(waiting) >= max_inflight {
                    // Full. Of the other peers, the one that holds the most
                    // waiting gives up its newest for this frame, unless
                    // this frame's own peer holds more than it; then this
                    // frame is given up. On a tie the other's goes: it is
                    // the older of the two, and of a group's frames the
                    // newer carries the more (an append supersedes the
                    // appends before it; a heartbeat, the heartbeats). It
                    // was this frame that went on a tie, and the test of a
                    // dead peer beside a live one hung on it: with the
                    // live peer's five frames arriving before the driver
                    // saw any of them carried, the fifth found the two
                    // peers holding two each and was given up. Nothing
                    // being sent is given up: where nothing waits there is
                    // no room to make.
                    let most = peers
                        .iter()
                        .filter(|(peer, _)| **peer != target)
                        .map(|(peer, held)| (held.len(), *peer))
                        .max();
                    let made = match most {
                        Some((length, peer)) if length >= held && length > 0 => {
                            peers.get_mut(&peer).and_then(Waiting::newest)
                        }
                        _ => None,
                    };
                    match made {
                        Some(given_up) => {
                            waiting = waiting.saturating_sub(1);
                            report.refused = report.refused.saturating_add(1);
                            given_up.give_up();
                        }
                        None => {
                            report.refused = report.refused.saturating_add(1);
                            frame.give_up();
                            continue;
                        }
                    }
                }
                let peer = peers.entry(target).or_default();
                if peer.sending < lane {
                    peer.sending = peer.sending.saturating_add(1);
                    report.sent = report.sent.saturating_add(1);
                    tasks.push(send(frame))?;
                    report.peak_inflight = report.peak_inflight.max(tasks.len());
                    continue;
                }
                match peer.push(frame) {
                    None => {
                        waiting = waiting.saturating_add(1);
                        report.peak_waiting = report.peak_waiting.max(waiting);
                    }
                    Some(frame) => {
                        report.refused = report.refused.saturating_add(1);
                        frame.give_up();
                    }
                }
            }
            Event::Sent(completed) => {
                let (target, result) = completed
                    .ok_or(ReplicationDriverError::Worker)?
                    .map_err(|_| ReplicationDriverError::Worker)?;
                match result {
                    Ok(()) => report.accepted = report.accepted.saturating_add(1),
                    Err(PeerSendError::Busy) => {
                        report.saturated = report.saturated.saturating_add(1)
                    }
                    Err(_) => report.lost = report.lost.saturating_add(1),
                }
                // The peer's place is free: its next frame takes it.
                let mut idle = false;
                if let Some(peer) = peers.get_mut(&target) {
                    match peer.next() {
                        Some(frame) => {
                            waiting = waiting.saturating_sub(1);
                            report.sent = report.sent.saturating_add(1);
                            tasks.push(send(frame))?;
                        }
                        None => {
                            peer.sending = peer.sending.saturating_sub(1);
                            idle = peer.sending == 0;
                        }
                    }
                }
                if idle {
                    peers.remove(&target);
                }
            }
        }
    }
    // What still waited for a peer's lane when the owner's egress ended is
    // given up as every frame that is not sent is: counted, and its owner
    // told — never dropped in silence, so every frame attempted is
    // accepted, lost, refused at the pool's bound or given up.
    for (_, mut peer) in peers {
        while let Some(frame) = peer.next() {
            report.refused = report.refused.saturating_add(1);
            frame.give_up();
        }
    }
    Ok(report)
}

#[cfg(test)]
#[path = "replication_tests.rs"]
mod tests;
