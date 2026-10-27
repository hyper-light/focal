//! What a `deliver` run's two sides wait to see in the ledger's events, and the tails that see
//! it. The consumer learns of a claim posted for it, and the issuer of a testament posted for
//! its claim, only by reading its recipient inbox a page at a time from where it last read, as a
//! consumer of a log learns of a message by fetching it: neither side is told by the other. A
//! message registers what it waits for before it sends the write that makes it, so no event it
//! waits for can come before its waiter; an event for a claim nobody waits on is not this run's
//! (or was waited for and given up), and is passed over.
use crate::driver::Conn;
use crate::error::LoadError;
use crate::progress::PROGRESS;
use focal_client::ClientError;
use focal_model::native_event::{
    NativeEvaluationEventKind, NativeEventRecord, NativeFactRecord, NativeResponseState,
    NativeValidationState,
};
use focal_model::{
    ClaimStatus, LedgerId, ParticipantId, RequestEpoch, RequestId, RouteEpoch, SessionSeq,
};
use focal_wire::{
    AccessError, NativeListFilter, NativeListRequest, NativeObject, NativeWaitRequest, Operation,
    RequestEnvelope, Response,
};
use hyper_rt::combine::{Either, race2};
use hyper_rt::sync::{OneshotReceiver, OneshotSender, oneshot};
use std::cell::{Cell, RefCell};
use std::collections::BTreeMap;
use std::time::Duration;

/// How long a side waits for the other's step to show in the events before it counts the
/// message undelivered: the client's own bound on one request's answer, twice.
pub const WAIT: Duration = Duration::from_secs(120);
/// Events one page of a tail holds at most: the native client's page of objects.
const TAIL_ITEMS: u32 = 256;
/// Indexed delivery positions one page visits at most.
const TAIL_VISITS: u32 = 4096;

/// Which side a tail reads for.
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum Side {
    /// The consumer: claims posted for it.
    Consumer,
    /// The issuer: testaments posted for its claims.
    Issuer,
    /// An agent of a run among agents: both, claims posted for it and testaments posted for the
    /// claims it issued, in its one inbox.
    Agent,
}

/// What a tail read, for the report.
#[derive(Clone, Copy, Default)]
pub struct TailCounts {
    /// Pages asked for.
    pub polls: u64,
    /// Events read.
    pub events: u64,
    /// Pages refused or failed.
    pub failed: u64,
    /// Pages the node held until a delivery came or the wait ended (the waiting profile).
    pub waited: u64,
}

/// The waiters of every message under way, by claim, at most `bound` of each kind.
pub struct Watch {
    /// Claims whose post the consumer waits to see: the sequence it committed at.
    posted: RefCell<BTreeMap<u128, OneshotSender<SessionSeq>>>,
    /// Claims whose testament the issuer waits to see: the testament and the sequence its post
    /// committed at.
    answered: RefCell<BTreeMap<u128, OneshotSender<(u128, SessionSeq)>>>,
    /// Work claims whose whole-work evaluation the evaluator waits to see due: the validation
    /// it judges and the sequence the evaluation was materialized at.
    evaluations: RefCell<BTreeMap<u128, OneshotSender<(u128, SessionSeq)>>>,
    /// Work claims whose end the issuer waits to see: the terminal status and the sequence it
    /// was reached at.
    settled: RefCell<BTreeMap<u128, OneshotSender<(ClaimStatus, SessionSeq)>>>,
    bound: usize,
    stopped: Cell<bool>,
    consumer: Cell<TailCounts>,
    issuer: Cell<TailCounts>,
    /// Every agent's tail together.
    agents: Cell<TailCounts>,
    /// The tails that have taken their starting place in their inboxes, and the run waiting for
    /// them all to have ([`Watch::all_positioned`]).
    positioned: Cell<usize>,
    positioning: RefCell<Option<std::task::Waker>>,
    /// The wakers of the tasks the tails run in, each once, woken as the run stops: a tail lets go
    /// of the read it has out at the node then, where it waited for the read's answer, up to a
    /// whole inbox wait ([`Watch::stopping`]). Each tail runs as a task of its own, so each is
    /// woken; at most the tails the run reserved for ([`Watch::reserve_tails`]).
    stopping: RefCell<Vec<std::task::Waker>>,
}

impl Watch {
    /// A watch for at most `bound` messages under way at once.
    pub fn new(bound: usize) -> Self {
        Self {
            posted: RefCell::new(BTreeMap::new()),
            answered: RefCell::new(BTreeMap::new()),
            evaluations: RefCell::new(BTreeMap::new()),
            settled: RefCell::new(BTreeMap::new()),
            bound,
            stopped: Cell::new(false),
            consumer: Cell::new(TailCounts::default()),
            issuer: Cell::new(TailCounts::default()),
            agents: Cell::new(TailCounts::default()),
            positioned: Cell::new(0),
            positioning: RefCell::new(None),
            stopping: RefCell::new(Vec::new()),
        }
    }
    /// Room for `tails` tails' tasks to wait for the run to stop, each once, reserved before any
    /// waits.
    pub fn reserve_tails(&self, tails: usize) -> Result<(), LoadError> {
        self.stopping
            .borrow_mut()
            .try_reserve_exact(tails)
            .map_err(|_| LoadError::Bound("tails waiting for the run to stop"))
    }
    /// Ends once the run stops ([`Watch::stop`]).
    fn stopping(&self) -> impl std::future::Future<Output = ()> + '_ {
        std::future::poll_fn(|cx| {
            if self.stopped.get() {
                return std::task::Poll::Ready(());
            }
            let mut waiting = self.stopping.borrow_mut();
            // Each task once, within the room reserved for the tails: a tail past it is let go at
            // its read's answer, up to an inbox wait, as every tail was before the room.
            if !waiting.iter().any(|waker| waker.will_wake(cx.waker()))
                && waiting.len() < waiting.capacity()
            {
                waiting.push(cx.waker().clone());
            }
            std::task::Poll::Pending
        })
    }
    /// A tail took its starting place: every delivery committed from here on is in what it reads.
    fn take_position(&self) {
        self.positioned.set(self.positioned.get().saturating_add(1));
        if let Some(waker) = self.positioning.borrow_mut().take() {
            waker.wake();
        }
    }
    /// Waits until `tails` tails have taken their starting place in their inboxes: a tail starts
    /// past every delivery committed before its first page, as a consumer joins a log at its end,
    /// so a run that begins before its tails have would deliver to an agent that never sees it
    /// (two claims of a thousand agents' run waited at their posts for good, CI 39870118553).
    /// Bounded by the tails' progress: refused once a whole [`WAIT`] passes with none more taking
    /// its place.
    pub async fn all_positioned(&self, tails: usize) -> Result<(), LoadError> {
        loop {
            let seen = self.positioned.get();
            if seen >= tails {
                return Ok(());
            }
            let more = std::future::poll_fn(|cx| {
                if self.positioned.get() != seen || self.stopped.get() {
                    return std::task::Poll::Ready(());
                }
                *self.positioning.borrow_mut() = Some(cx.waker().clone());
                std::task::Poll::Pending
            });
            match race2(more, focal_rt::time::sleep(WAIT)).await {
                Either::First(()) if self.stopped.get() => {
                    return Err(LoadError::Tails {
                        positioned: self.positioned.get(),
                        tails,
                    });
                }
                Either::First(()) => {}
                Either::Second(slept) => {
                    slept.map_err(LoadError::Runtime)?;
                    return Err(LoadError::Tails {
                        positioned: self.positioned.get(),
                        tails,
                    });
                }
            }
        }
    }
    /// Waits for `claim`'s post to show: registered before the post is sent.
    pub fn expect_posted(&self, claim: u128) -> Result<OneshotReceiver<SessionSeq>, LoadError> {
        expect(&self.posted, self.bound, claim)
    }
    /// Waits for a testament of `claim` to show posted: registered before its post is sent.
    pub fn expect_testament(
        &self,
        claim: u128,
    ) -> Result<OneshotReceiver<(u128, SessionSeq)>, LoadError> {
        expect(&self.answered, self.bound, claim)
    }
    /// Waits for `claim`'s whole-work evaluation to show due to its evaluator: registered before
    /// the write that enters it is sent.
    pub fn expect_evaluation(
        &self,
        claim: u128,
    ) -> Result<OneshotReceiver<(u128, SessionSeq)>, LoadError> {
        expect(&self.evaluations, self.bound, claim)
    }
    /// Waits for `claim` to show at a terminal status: registered before the write that settles
    /// it is sent.
    pub fn expect_settled(
        &self,
        claim: u128,
    ) -> Result<OneshotReceiver<(ClaimStatus, SessionSeq)>, LoadError> {
        expect(&self.settled, self.bound, claim)
    }
    /// Lets go of whatever waits on `claim`: its write was never committed, or its wait ended.
    pub fn forget(&self, claim: u128) {
        self.posted.borrow_mut().remove(&claim);
        self.answered.borrow_mut().remove(&claim);
        self.evaluations.borrow_mut().remove(&claim);
        self.settled.borrow_mut().remove(&claim);
    }
    /// What `receiver` waits for, or `None` past [`WAIT`] or once its tail stopped.
    pub async fn wait<T>(&self, receiver: OneshotReceiver<T>) -> Result<Option<T>, LoadError> {
        match race2(receiver, focal_rt::time::sleep(WAIT)).await {
            Either::First(Ok(value)) => Ok(Some(value)),
            // The sender went without sending: the run stopped its tails.
            Either::First(Err(_)) => Ok(None),
            Either::Second(slept) => {
                slept.map_err(LoadError::Runtime)?;
                Ok(None)
            }
        }
    }
    /// Ends the tails at their next page, and every wait with them.
    pub fn stop(&self) {
        self.stopped.set(true);
        if let Some(waker) = self.positioning.borrow_mut().take() {
            waker.wake();
        }
        for waker in self.stopping.borrow_mut().drain(..) {
            waker.wake();
        }
        self.posted.borrow_mut().clear();
        self.answered.borrow_mut().clear();
        self.evaluations.borrow_mut().clear();
        self.settled.borrow_mut().clear();
    }
    /// What the tail of `side` read.
    pub fn counts(&self, side: Side) -> TailCounts {
        self.cell(side).get()
    }
    fn cell(&self, side: Side) -> &Cell<TailCounts> {
        match side {
            Side::Consumer => &self.consumer,
            Side::Issuer => &self.issuer,
            Side::Agent => &self.agents,
        }
    }
    /// One event a tail of `side` read: a waiter it answers is told.
    fn saw(&self, side: Side, record: &NativeEventRecord) {
        match (side, &record.fact) {
            (Side::Consumer | Side::Agent, NativeFactRecord::Claim(claim))
                if claim.status == ClaimStatus::Posted =>
            {
                let id = u128::from_be_bytes(claim.after.object.0);
                if let Some(sender) = self.posted.borrow_mut().remove(&id) {
                    // A waiter gone since is a message given up: nothing to tell.
                    let _ = sender.send(record.sequence);
                }
            }
            (
                Side::Issuer | Side::Agent,
                NativeFactRecord::Response {
                    claim,
                    after,
                    state: NativeResponseState::Posted,
                    ..
                },
            ) => {
                let id = u128::from_be_bytes(claim.0);
                if let Some(sender) = self.answered.borrow_mut().remove(&id) {
                    let _ = sender.send((u128::from_be_bytes(after.object.0), record.sequence));
                }
            }
            // An evaluation materialized ready for its evaluator: the work claim's whole work is
            // entered and its evaluator may begin.
            (
                Side::Agent,
                NativeFactRecord::Evaluation {
                    kind: NativeEvaluationEventKind::Materialized,
                    key,
                    state: NativeValidationState::Ready,
                    ..
                },
            ) => {
                let id = u128::from_be_bytes(key.claim.0);
                if let Some(sender) = self.evaluations.borrow_mut().remove(&id) {
                    let _ = sender.send((u128::from_be_bytes(key.validation.0), record.sequence));
                }
            }
            (Side::Issuer | Side::Agent, NativeFactRecord::Claim(claim))
                if focal_model::semantics_v1::is_terminal(claim.status) =>
            {
                let id = u128::from_be_bytes(claim.after.object.0);
                if let Some(sender) = self.settled.borrow_mut().remove(&id) {
                    let _ = sender.send((claim.status, record.sequence));
                }
            }
            _ => {}
        }
    }
    fn count(&self, side: Side, change: impl FnOnce(&mut TailCounts)) {
        let cell = self.cell(side);
        let mut counts = cell.get();
        change(&mut counts);
        cell.set(counts);
    }
}

fn expect<T>(
    waiters: &RefCell<BTreeMap<u128, OneshotSender<T>>>,
    bound: usize,
    claim: u128,
) -> Result<OneshotReceiver<T>, LoadError> {
    let mut waiters = waiters.borrow_mut();
    if waiters.len() >= bound {
        return Err(LoadError::Bound("messages waited on"));
    }
    let (sender, receiver) = oneshot().map_err(LoadError::Runtime)?;
    waiters.insert(claim, sender);
    Ok(receiver)
}

/// The deliveries of `side`'s recipient from the committed prefix on, a page at a time over `conn` in
/// `ledger`, each page's request id from `ids`, waiting `poll` after a page with nothing new,
/// until the watch stops.
pub async fn tail(
    watch: &Watch,
    side: Side,
    (poll, wait_ms): (Duration, u32),
    conn: &Conn,
    ledger: LedgerId,
    recipient: ParticipantId,
    mut ids: impl FnMut() -> Result<u128, LoadError>,
) -> Result<(), LoadError> {
    // Where the tail starts: after every event the prefix holds now, as a consumer joins a log
    // at its end.
    let mut after = None;
    let mut started = false;
    let mut cursor = None;
    // Whether the node holds a page until a delivery comes: until it says it cannot.
    let mut waits = wait_ms > 0;
    while !watch.stopped.get() {
        let (max_items, max_visits) = if started {
            (TAIL_ITEMS, TAIL_VISITS)
        } else {
            (1, 1)
        };
        let list = NativeListRequest {
            filter: NativeListFilter::Deliveries { recipient, after },
            cursor: cursor.take(),
            max_items,
            max_visits,
        };
        // A page that goes on from a cursor has more to read now; one from the inbox's end
        // waits at the node for the next delivery.
        let waiting = started && waits && list.cursor.is_none();
        let (protocol, operation) = if waiting {
            (
                focal_wire::WAITING_PROTOCOL_VERSION,
                Operation::NativeWait(NativeWaitRequest { list, wait_ms }),
            )
        } else {
            (
                focal_wire::NATIVE_PROTOCOL_VERSION,
                Operation::NativeList(list),
            )
        };
        let envelope = RequestEnvelope {
            protocol,
            ledger,
            route_epoch: RouteEpoch(1),
            request_epoch: RequestEpoch(1),
            request_id: RequestId::from_u128(ids()?),
            operation,
        };
        // The read is let go once the run stops: its node answers a read no one waits for into
        // a stream its client has reset.
        let reply = match race2(conn.request(envelope), watch.stopping()).await {
            Either::First(reply) => reply,
            Either::Second(()) => break,
        };
        watch.count(side, |counts| counts.polls = counts.polls.saturating_add(1));
        PROGRESS.tail_poll();
        let result = reply.map(|envelope| envelope.result);
        if waiting
            && matches!(
                result,
                Err(ClientError::Access(AccessError::UnsupportedProtocol))
                    | Ok(Response::Error(AccessError::UnsupportedProtocol))
            )
        {
            // A node, or a connection, that does not hold an inbox read: polled from here on.
            waits = false;
            continue;
        }
        if waiting {
            watch.count(side, |counts| {
                counts.waited = counts.waited.saturating_add(1)
            });
            PROGRESS.tail_wait();
        }
        let page = match result {
            Ok(Response::NativeListed(page)) => page,
            _ => {
                watch.count(side, |counts| {
                    counts.failed = counts.failed.saturating_add(1)
                });
                PROGRESS.tail_failed();
                focal_rt::time::sleep(poll)
                    .await
                    .map_err(LoadError::Runtime)?;
                continue;
            }
        };
        if !started {
            // Every event at or before the prefix passed over: the next page starts past the
            // prefix's last record.
            after = Some((page.native_sequence, u32::MAX));
            started = true;
            watch.take_position();
            continue;
        }
        let mut read = 0u64;
        for object in &page.objects {
            if let NativeObject::Event(record) = object {
                after = Some((record.sequence, record.ordinal));
                read = read.saturating_add(1);
                watch.saw(side, record);
            }
        }
        watch.count(side, |counts| {
            counts.events = counts.events.saturating_add(read)
        });
        PROGRESS.tail_events(read);
        if read == 0 {
            // A page that reached the prefix waits for more: at the node where it holds the
            // read, here otherwise.
            cursor = page.next;
            if cursor.is_none() && !waiting {
                focal_rt::time::sleep(poll)
                    .await
                    .map_err(LoadError::Runtime)?;
            }
        }
    }
    Ok(())
}

#[cfg(test)]
#[cfg_attr(test, allow(clippy::unwrap_used, clippy::panic))]
mod tests {
    use super::*;

    /// The run begins once every tail has taken its place, whichever order they take them in.
    #[test]
    fn the_run_waits_for_every_tail_to_take_its_place() {
        focal_rt::block_on(async {
            let watch = Watch::new(4);
            let take = |after| {
                let watch = &watch;
                async move {
                    focal_rt::time::sleep(Duration::from_millis(after))
                        .await
                        .unwrap();
                    watch.take_position();
                }
            };
            let ((positioned, ()), ((), ())) = hyper_rt::combine::join2(
                hyper_rt::combine::join2(watch.all_positioned(3), take(30)),
                hyper_rt::combine::join2(take(10), take(20)),
            )
            .await;
            positioned.unwrap();
            assert_eq!(watch.positioned.get(), 3);
        })
        .unwrap();
    }

    /// A run stopped while its tails take their places ends the wait at once, refused.
    #[test]
    fn a_stop_ends_the_wait_for_the_tails() {
        focal_rt::block_on(async {
            let watch = Watch::new(4);
            let stop = async {
                focal_rt::time::sleep(Duration::from_millis(10))
                    .await
                    .unwrap();
                watch.take_position();
                watch.stop();
            };
            let (positioned, ()) = hyper_rt::combine::join2(watch.all_positioned(2), stop).await;
            assert!(matches!(
                positioned,
                Err(LoadError::Tails {
                    positioned: 1,
                    tails: 2
                })
            ));
        })
        .unwrap();
    }
}
