//! The datagram plane's socket: its own, beside the endpoint's (mantle note 32 §3.5). Each datagram it
//! opens comes with when it arrived on the shard's clock, the kernel's receive stamp where the platform
//! gives one: what a node-pair heartbeat is judged by (hyper-raft docs/timing.md §2.4).

use std::future::{Future, poll_fn};
use std::net::SocketAddr;
use std::pin::Pin;
use std::task::{Context, Poll};

use hyper_datagram::{Fence, Opened, PeerId, Plane, Refusal};
use hyper_rt::RtError;
use hyper_rt::readiness::Ready;
use hyper_rt::udp::{Arrival, Batched, Io, IoStats};

use crate::TURNS;

/// Carries a [`Plane`]'s datagrams on a batched UDP socket on a shard. The owner keeps the plane and
/// lends it to each call; the socket keeps only its bounded outbox and receive buffers.
#[derive(Debug)]
pub struct PlaneSocket {
    socket: Batched,
    readable: Option<Ready>,
    writable: Option<Ready>,
}

impl PlaneSocket {
    /// A socket bound to `address`, with the kernel's receive stamps asked for.
    pub fn bind(address: SocketAddr, io: Io) -> Result<Self, RtError> {
        Ok(Self::new(Batched::bind(address, io, true)?))
    }

    /// The plane's socket on `socket`.
    pub fn new(socket: Batched) -> Self {
        Self {
            socket,
            readable: None,
            writable: None,
        }
    }

    /// The address the socket is bound to.
    pub fn local_addr(&self) -> Result<SocketAddr, RtError> {
        self.socket.local_addr()
    }

    /// What the socket has done.
    pub fn stats(&self) -> IoStats {
        self.socket.stats()
    }

    /// Seals what `plane` has queued and sends it, each peer's datagram to the address `route` names; a
    /// peer `route` does not know, or one the plane refuses to seal for, goes to `refused` (the plane
    /// retransmits nothing). Past the outbox's bound, with the socket full, a datagram is dropped and
    /// counted.
    pub fn flush(
        &mut self,
        plane: &mut Plane,
        mut route: impl FnMut(PeerId) -> Option<SocketAddr>,
        mut refused: impl FnMut(PeerId, Option<Refusal>),
    ) {
        let _ = self.socket.send();
        let socket = &mut self.socket;
        plane.flush(|peer, sealed| match sealed {
            Err(refusal) => refused(peer, Some(refusal)),
            Ok(datagram) => match route(peer) {
                None => refused(peer, None),
                Some(to) => {
                    if socket.slot().is_none() {
                        let _ = socket.send();
                    }
                    socket.queue(to, datagram);
                }
            },
        });
        let _ = self.socket.send();
    }

    /// Waits for datagrams and opens one batch with `plane` under `fence`, handing each result to
    /// `deliver` with its arrival; returns how many. Sends what the outbox holds when the socket takes it.
    /// Cancel-safe: a batch is taken and delivered without an await between.
    pub async fn receive(
        &mut self,
        plane: &mut Plane,
        fence: &dyn Fence,
        mut deliver: impl FnMut(Arrival, Result<Opened<'_>, Refusal>),
    ) -> Result<usize, RtError> {
        poll_fn(|context| self.poll_receive(context, plane, fence, &mut deliver)).await
    }

    /// Opens every datagram already queued on the socket, without waiting (hyper-rt asks the kernel on
    /// every receive, so a datagram that arrived through a stop of the process is taken with its stamp).
    /// At most [`TURNS`] batches; returns how many datagrams.
    pub fn receive_ready(
        &mut self,
        plane: &mut Plane,
        fence: &dyn Fence,
        mut deliver: impl FnMut(Arrival, Result<Opened<'_>, Refusal>),
    ) -> Result<usize, RtError> {
        let mut total = 0usize;
        for _ in 0..TURNS {
            let count = self.socket.receive(|arrival, datagram| {
                deliver(arrival, plane.open(datagram, fence));
            })?;
            if count == 0 {
                break;
            }
            total = total.saturating_add(count);
        }
        Ok(total)
    }

    fn poll_receive(
        &mut self,
        context: &mut Context<'_>,
        plane: &mut Plane,
        fence: &dyn Fence,
        deliver: &mut impl FnMut(Arrival, Result<Opened<'_>, Refusal>),
    ) -> Poll<Result<usize, RtError>> {
        for _ in 0..TURNS {
            if self.socket.pending() {
                let _ = self.socket.send();
            }
            let count = match self.socket.receive(|arrival, datagram| {
                deliver(arrival, plane.open(datagram, fence));
            }) {
                Ok(count) => count,
                Err(error) => return Poll::Ready(Err(error)),
            };
            if count > 0 {
                return Poll::Ready(Ok(count));
            }
            let readable = self.readable.get_or_insert_with(|| self.socket.readable());
            if let Poll::Ready(outcome) = Pin::new(readable).poll(context) {
                self.readable = None;
                if let Err(error) = outcome {
                    return Poll::Ready(Err(error));
                }
                continue;
            }
            if self.socket.pending() {
                let writable = self.writable.get_or_insert_with(|| self.socket.writable());
                if let Poll::Ready(outcome) = Pin::new(writable).poll(context) {
                    self.writable = None;
                    if let Err(error) = outcome {
                        return Poll::Ready(Err(error));
                    }
                    continue;
                }
            }
            return Poll::Pending;
        }
        context.waker().wake_by_ref();
        Poll::Pending
    }
}
