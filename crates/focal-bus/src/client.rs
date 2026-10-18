//! A bus client: publishes batches, subscribes to streams, and takes what the node delivers.
//!
//! Every delivery is checked whole against its producer's CRC before it is handed on: what a node,
//! a disk or a copy between them changed is refused here, end to end, whatever the transport's own
//! integrity said about the last hop. A gap notice is handed on as it is: the subscriber learns
//! exactly which offsets it missed.

use std::time::{Duration, Instant};

use hyper_transport::{
    Budget, Classes, Directory, Endpoint, Event, ExchangeId, PeerId, Progress, Refusal,
};

use crate::BusError;
use crate::batch::{Batch, BatchError};
use crate::wire::{
    self, Ack, Bus, Gap, Kind, LANE, Leader, Redirect, Reply, Subscribe, Subscribed,
};

/// What a client is handed.
#[derive(Debug)]
pub enum Delivery<'a> {
    /// A stored batch, its CRC checked.
    Batch(Batch<'a>),
    /// Offsets of a stream the node let go before this client took them.
    Gap(Gap),
    /// A subscription's start: the stream's retained window when it was made.
    Subscribed {
        /// The exchange that asked.
        exchange: ExchangeId,
        /// The reply.
        subscribed: Subscribed,
    },
    /// A subscribe the node did not take, for it does not lead the stream's part: the leader it
    /// knows (doc 34 P7).
    SubscribeRedirected {
        /// The exchange that asked.
        exchange: ExchangeId,
        /// The leader the node knows.
        leader: Leader,
    },
    /// A publish its part's group committed (doc 34 P5).
    Acked(Ack),
    /// A publish the node did not take, for it does not lead the batch's part, and the leader it
    /// knows (doc 34 P2).
    Redirected(Redirect),
    /// A delivery that did not pass its producer's CRC, or a frame that was no delivery.
    Refused(BatchError),
    /// A subscribe the node or the transport refused, and why.
    Unsubscribed {
        /// The exchange that asked.
        exchange: ExchangeId,
        /// Why.
        refusal: Refusal,
    },
    /// The node's connection ended, or the node could not be reached: nothing more comes over
    /// it. Why, when either side said.
    Lost(Option<Refusal>),
}

/// A client of one node.
#[derive(Debug)]
pub struct Client {
    node: PeerId,
}

impl Client {
    /// A client of the node the directory names `node`.
    pub const fn new(node: PeerId) -> Self {
        Self { node }
    }

    /// Queues `batch` for the node; `LaneFull` while the lane has no room, after which the
    /// endpoint says `LaneRoom`.
    pub fn publish<B, D>(
        &self,
        endpoint: &mut Endpoint<Bus, B, D>,
        batch: &[u8],
    ) -> Result<(), Refusal>
    where
        B: Budget<<Bus as Classes>::Class>,
        D: Directory<Role = <Bus as Classes>::Role>,
    {
        endpoint.send_frame(self.node, LANE, Kind::Publish, batch)
    }

    /// Asks for `stream` from `from`, judged at `period`.
    pub fn subscribe<B, D>(
        &self,
        endpoint: &mut Endpoint<Bus, B, D>,
        now: Instant,
        stream: u64,
        from: wire::From,
        period: Duration,
    ) -> Result<ExchangeId, Refusal>
    where
        B: Budget<<Bus as Classes>::Class>,
        D: Directory<Role = <Bus as Classes>::Role>,
    {
        let head = Subscribe { stream, from }.encode();
        endpoint.open(
            now,
            self.node,
            Kind::Subscribe,
            &head,
            None,
            Progress::new(period)?,
        )
    }

    /// Acts on one of the endpoint's events, handing what it delivers to `take`.
    pub fn on_event<B, D>(
        &mut self,
        endpoint: &mut Endpoint<Bus, B, D>,
        event: Event<Bus>,
        mut take: impl FnMut(Delivery<'_>),
    ) where
        B: Budget<<Bus as Classes>::Class>,
        D: Directory<Role = <Bus as Classes>::Role>,
    {
        match event {
            Event::Frame {
                kind: Kind::Deliver,
                frame,
                ..
            } => {
                let limit = usize::try_from(wire::BATCH_BOUND).unwrap_or(usize::MAX);
                // The transport checked the batch's CRC where the wire says it carries one.
                let parse = if Bus::embedded_checksum(Kind::Deliver).is_some() {
                    Batch::parse_verified
                } else {
                    Batch::parse
                };
                match parse(frame.bytes(), limit) {
                    Ok(batch) => take(Delivery::Batch(batch)),
                    Err(refused) => take(Delivery::Refused(refused)),
                }
                endpoint.release(frame);
            }
            Event::Frame {
                kind: Kind::Gap,
                frame,
                ..
            } => {
                match Gap::decode(frame.bytes()) {
                    Ok(gap) => take(Delivery::Gap(gap)),
                    Err(_) => take(Delivery::Refused(BatchError::Short)),
                }
                endpoint.release(frame);
            }
            Event::Frame {
                kind: Kind::Ack,
                frame,
                ..
            } => {
                match Ack::decode(frame.bytes()) {
                    Ok(ack) => take(Delivery::Acked(ack)),
                    Err(_) => take(Delivery::Refused(BatchError::Short)),
                }
                endpoint.release(frame);
            }
            Event::Frame {
                kind: Kind::Redirect,
                frame,
                ..
            } => {
                match Redirect::decode(frame.bytes()) {
                    Ok(redirect) => take(Delivery::Redirected(redirect)),
                    Err(_) => take(Delivery::Refused(BatchError::Short)),
                }
                endpoint.release(frame);
            }
            Event::Frame { frame, .. } => endpoint.release(frame),
            Event::Reply { exchange, .. } => {
                match endpoint.head(exchange).map(Reply::decode) {
                    Some(Ok(Reply::Subscribed(subscribed))) => take(Delivery::Subscribed {
                        exchange,
                        subscribed,
                    }),
                    Some(Ok(Reply::Redirected(leader))) => {
                        take(Delivery::SubscribeRedirected { exchange, leader });
                    }
                    _ => {}
                }
                endpoint.end(exchange);
            }
            Event::Refused {
                exchange, refusal, ..
            } => take(Delivery::Unsubscribed { exchange, refusal }),
            Event::Closed { peer, refusal, .. } | Event::Unreachable { peer, refusal }
                if peer == self.node =>
            {
                take(Delivery::Lost(refusal));
            }
            _ => {}
        }
    }
}

impl From<Refusal> for BusError {
    fn from(refusal: Refusal) -> Self {
        Self::Transport(refusal)
    }
}
