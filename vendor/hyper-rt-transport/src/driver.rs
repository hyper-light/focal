//! The endpoint's driver: one owner, one socket, one timer.

use std::future::{Future, poll_fn};
use std::net::SocketAddr;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use hyper_rt::RtError;
use hyper_rt::futures::{Sleep, sleep};
use hyper_rt::readiness::Ready;
use hyper_rt::udp::{Batched, Io, IoStats, Sent};
use hyper_transport::{Budget, Classes, Directory, Endpoint, Event, Lateness};

/// Derived: the turns one poll of [`Driver::event`] takes before it yields to the shard, the bound
/// hyper-tokio takes from tokio's cooperative budget (128 operations a task a poll,
/// `tokio::task::coop`), kept so a consumer moving between the two drivers keeps one step's work.
pub const TURNS: usize = 128;

/// The shard's clock now, nanoseconds.
fn shard_now() -> Result<u64, RtError> {
    hyper_rt::registry::with_current(|context| context.now_ns()).ok_or(RtError::NotOnShardThread)
}

/// Drives an [`Endpoint`] on a hyper-rt shard (module doc of the crate).
pub struct Driver<C: Classes, B: Budget<C::Class>, D: Directory<Role = C::Role>> {
    endpoint: Endpoint<C, B, D>,
    socket: Batched,
    /// The shard clock's reading and the instant anchored to it.
    anchor: (u64, Instant),
    /// The armed readiness waits, kept between polls (a fired one is dropped and made again).
    readable: Option<Ready>,
    writable: Option<Ready>,
    /// The timer, and the deadline it is armed for.
    timer: Option<(Instant, Sleep)>,
    /// How late the timer fires: the owner's granularity `G` (hyper-raft docs/timing.md §2.4).
    late: Lateness,
}

impl<C: Classes, B: Budget<C::Class>, D: Directory<Role = C::Role>> Driver<C, B, D> {
    /// A driver for `endpoint` on a socket bound to `address`. Called on a shard thread.
    pub fn bind(endpoint: Endpoint<C, B, D>, address: SocketAddr, io: Io) -> Result<Self, RtError> {
        Self::new(endpoint, Batched::bind(address, io, false)?)
    }

    /// A driver for `endpoint` on `socket`. Called on a shard thread.
    #[allow(
        clippy::disallowed_methods,
        reason = "the runtime adapter anchors the endpoint's Instant to the shard's clock once"
    )]
    pub fn new(endpoint: Endpoint<C, B, D>, socket: Batched) -> Result<Self, RtError> {
        let anchor = (shard_now()?, Instant::now());
        let resolution = Duration::from_nanos(hyper_rt::machine::clock::resolution_ns());
        Ok(Self {
            endpoint,
            socket,
            anchor,
            readable: None,
            writable: None,
            timer: None,
            late: Lateness::new(resolution),
        })
    }

    /// The instant of the shard clock's reading `ns`.
    fn instant(&self, ns: u64) -> Instant {
        let (anchor_ns, anchor) = self.anchor;
        anchor
            .checked_add(Duration::from_nanos(ns.saturating_sub(anchor_ns)))
            .unwrap_or(anchor)
    }

    /// The shard clock's reading of `at`.
    fn ns(&self, at: Instant) -> u64 {
        let (anchor_ns, anchor) = self.anchor;
        let since =
            u64::try_from(at.saturating_duration_since(anchor).as_nanos()).unwrap_or(u64::MAX);
        anchor_ns.saturating_add(since)
    }

    /// Now, as an instant on the shard's clock.
    fn now(&self) -> Result<Instant, RtError> {
        Ok(self.instant(shard_now()?))
    }

    /// The endpoint, for the owner's calls.
    pub fn endpoint(&mut self) -> &mut Endpoint<C, B, D> {
        &mut self.endpoint
    }

    /// The address the socket is bound to.
    pub fn local_addr(&self) -> Result<SocketAddr, RtError> {
        self.socket.local_addr()
    }

    /// What the socket has done.
    pub fn stats(&self) -> IoStats {
        self.socket.stats()
    }

    /// Now on the endpoint's clock: the instant the owner passes to the endpoint's calls (`connect`,
    /// `ask`), on the shard's clock as the driver's own are.
    pub fn clock(&self) -> Result<Instant, RtError> {
        self.now()
    }

    /// Sends what the endpoint has to send now, as far as the socket takes it without waiting.
    pub fn flush(&mut self) -> Result<(), RtError> {
        let now = self.now()?;
        let _ = self.send(now);
        Ok(())
    }

    /// The endpoint's next event (hyper-tokio's `Driver::event`). Cancel-safe.
    pub async fn event(&mut self) -> Result<Event<C>, RtError> {
        poll_fn(|context| self.poll_event(context)).await
    }

    /// [`Driver::event`] as a poll: an event already queued goes at once; otherwise each turn takes what
    /// arrived, fires the timers due, sends, and hands out the next event, or arms the socket's readiness
    /// and the timer and parks.
    pub fn poll_event(&mut self, context: &mut Context<'_>) -> Poll<Result<Event<C>, RtError>> {
        if let Some(event) = self.endpoint.poll_event() {
            return Poll::Ready(Ok(event));
        }
        for _ in 0..TURNS {
            if let Err(error) = self.drain() {
                return Poll::Ready(Err(error));
            }
            let now = match self.now() {
                Ok(now) => now,
                Err(error) => return Poll::Ready(Err(error)),
            };
            if self.endpoint.poll_timeout().is_some_and(|due| due <= now) {
                self.endpoint.handle_timeout(now);
            }
            let more = self.send(now);
            if let Some(event) = self.endpoint.poll_event() {
                return Poll::Ready(Ok(event));
            }
            if more {
                continue;
            }
            match self.poll_ready(context) {
                Ok(true) => continue,
                Ok(false) => {}
                Err(error) => return Poll::Ready(Err(error)),
            }
            let Some(due) = self.endpoint.poll_timeout() else {
                return Poll::Pending;
            };
            match self.poll_timer(context, due, now) {
                Ok(true) => {}
                Ok(false) => return Poll::Pending,
                Err(error) => return Poll::Ready(Err(error)),
            }
        }
        // The budget is spent with work still to do: come back after the shard's other tasks.
        context.waker().wake_by_ref();
        Poll::Pending
    }

    /// Polls the socket's readiness waits, arming them as needed; whether one fired.
    fn poll_ready(&mut self, context: &mut Context<'_>) -> Result<bool, RtError> {
        let readable = self.readable.get_or_insert_with(|| self.socket.readable());
        if let Poll::Ready(outcome) = Pin::new(readable).poll(context) {
            self.readable = None;
            outcome?;
            return Ok(true);
        }
        if self.socket.pending() {
            let writable = self.writable.get_or_insert_with(|| self.socket.writable());
            if let Poll::Ready(outcome) = Pin::new(writable).poll(context) {
                self.writable = None;
                outcome?;
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// Polls the timer for `due`, arming it anew when the deadline moved; whether it fired.
    fn poll_timer(
        &mut self,
        context: &mut Context<'_>,
        due: Instant,
        now: Instant,
    ) -> Result<bool, RtError> {
        if self.timer.as_ref().is_none_or(|(armed, _)| *armed != due) {
            let wait =
                u64::try_from(due.saturating_duration_since(now).as_nanos()).unwrap_or(u64::MAX);
            self.timer = Some((due, sleep(wait)));
        }
        let Some((_, timer)) = self.timer.as_mut() else {
            return Ok(false);
        };
        match Pin::new(timer).poll(context) {
            Poll::Ready(outcome) => {
                self.timer = None;
                outcome?;
                self.fired(due)?;
                Ok(true)
            }
            Poll::Pending => Ok(false),
        }
    }

    /// The timer armed for `due` fired: its lateness folded into `G`, and the endpoint told.
    fn fired(&mut self, due: Instant) -> Result<(), RtError> {
        let woke = shard_now()?;
        // A full fold keeps the mean it has: 2^64 waits are past any process.
        let _ = self.late.on_wait(self.ns(due), woke);
        if let Some(granularity) = self.late.granularity() {
            self.endpoint.set_granularity(granularity);
        }
        Ok(())
    }

    /// Takes the datagrams that have arrived, a batch at a time, until none or [`TURNS`] batches.
    fn drain(&mut self) -> Result<(), RtError> {
        for _ in 0..TURNS {
            let (anchor_ns, anchor) = self.anchor;
            let endpoint = &mut self.endpoint;
            let taken = self.socket.receive(|arrival, bytes| {
                let at = anchor
                    .checked_add(Duration::from_nanos(
                        arrival.at_ns.saturating_sub(anchor_ns),
                    ))
                    .unwrap_or(anchor);
                endpoint.handle_datagram(at, arrival.from, None, bytes);
            })?;
            if taken == 0 {
                return Ok(());
            }
        }
        Ok(())
    }

    /// Moves what the endpoint transmits into the outbox and sends it; whether the outbox filled, so the
    /// endpoint may have more.
    fn send(&mut self, now: Instant) -> bool {
        if self.socket.pending() && self.socket.send() == Sent::Blocked {
            return false;
        }
        let mut filled = true;
        while let Some(slot) = self.socket.slot() {
            let Some(transmit) = self.endpoint.poll_transmit(now, &mut slot.bytes) else {
                filled = false;
                break;
            };
            slot.bytes.truncate(transmit.size);
            slot.to = transmit.destination;
            self.socket.commit();
        }
        self.socket.send() == Sent::Drained && filled
    }
}
