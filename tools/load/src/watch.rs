//! What a `deliver` run's two sides wait to see in the ledger's events, and the tails that see
//! it. The consumer learns of a claim posted for it, and the issuer of a testament posted for
//! its claim, only by reading its recipient inbox a page at a time from where it last read, as a
//! consumer of a log learns of a message by fetching it: neither side is told by the other. A
//! message registers what it waits for before it sends the write that makes it, so no event it
//! waits for can come before its waiter; an event for a claim nobody waits on is not this run's
//! (or was waited for and given up), and is passed over.
use crate::driver::Conn;
use crate::error::LoadError;
use focal_model::native_event::{NativeEventRecord, NativeFactRecord, NativeResponseState};
use focal_model::{
    ClaimStatus, LedgerId, ParticipantId, RequestEpoch, RequestId, RouteEpoch, SessionSeq,
};
use focal_wire::{
    NativeListFilter, NativeListRequest, NativeObject, Operation, RequestEnvelope, Response,
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
}

/// The waiters of every message under way, by claim, at most `bound` of each kind.
pub struct Watch {
    /// Claims whose post the consumer waits to see: the sequence it committed at.
    posted: RefCell<BTreeMap<u128, OneshotSender<SessionSeq>>>,
    /// Claims whose testament the issuer waits to see: the testament and the sequence its post
    /// committed at.
    answered: RefCell<BTreeMap<u128, OneshotSender<(u128, SessionSeq)>>>,
    bound: usize,
    stopped: Cell<bool>,
    consumer: Cell<TailCounts>,
    issuer: Cell<TailCounts>,
}

impl Watch {
    /// A watch for at most `bound` messages under way at once.
    pub fn new(bound: usize) -> Self {
        Self {
            posted: RefCell::new(BTreeMap::new()),
            answered: RefCell::new(BTreeMap::new()),
            bound,
            stopped: Cell::new(false),
            consumer: Cell::new(TailCounts::default()),
            issuer: Cell::new(TailCounts::default()),
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
    /// Lets go of whatever waits on `claim`: its write was never committed, or its wait ended.
    pub fn forget(&self, claim: u128) {
        self.posted.borrow_mut().remove(&claim);
        self.answered.borrow_mut().remove(&claim);
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
        self.posted.borrow_mut().clear();
        self.answered.borrow_mut().clear();
    }
    /// What the tail of `side` read.
    pub fn counts(&self, side: Side) -> TailCounts {
        self.cell(side).get()
    }
    fn cell(&self, side: Side) -> &Cell<TailCounts> {
        match side {
            Side::Consumer => &self.consumer,
            Side::Issuer => &self.issuer,
        }
    }
    /// One event a tail of `side` read: a waiter it answers is told.
    fn saw(&self, side: Side, record: &NativeEventRecord) {
        match (side, &record.fact) {
            (Side::Consumer, NativeFactRecord::Claim(claim))
                if claim.status == ClaimStatus::Posted =>
            {
                let id = u128::from_be_bytes(claim.after.object.0);
                if let Some(sender) = self.posted.borrow_mut().remove(&id) {
                    // A waiter gone since is a message given up: nothing to tell.
                    let _ = sender.send(record.sequence);
                }
            }
            (
                Side::Issuer,
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
    poll: Duration,
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
    while !watch.stopped.get() {
        let (max_items, max_visits) = if started {
            (TAIL_ITEMS, TAIL_VISITS)
        } else {
            (1, 1)
        };
        let envelope = RequestEnvelope {
            protocol: focal_wire::NATIVE_PROTOCOL_VERSION,
            ledger,
            route_epoch: RouteEpoch(1),
            request_epoch: RequestEpoch(1),
            request_id: RequestId::from_u128(ids()?),
            operation: Operation::NativeList(NativeListRequest {
                filter: NativeListFilter::Deliveries { recipient, after },
                cursor: cursor.take(),
                max_items,
                max_visits,
            }),
        };
        let reply = conn.request(envelope).await;
        watch.count(side, |counts| counts.polls = counts.polls.saturating_add(1));
        let page = match reply.map(|envelope| envelope.result) {
            Ok(Response::NativeListed(page)) => page,
            _ => {
                watch.count(side, |counts| {
                    counts.failed = counts.failed.saturating_add(1)
                });
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
        if read == 0 {
            // A page that reached the prefix waits for more.
            cursor = page.next;
            if cursor.is_none() {
                focal_rt::time::sleep(poll)
                    .await
                    .map_err(LoadError::Runtime)?;
            }
        }
    }
    Ok(())
}
