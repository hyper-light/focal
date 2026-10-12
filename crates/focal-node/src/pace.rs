//! An owner's tick period, shared between the owner thread and whoever
//! derives it from measured round trips (27 §3.1 P2).
use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
    time::Duration,
};

/// Zero means "as configured". The owner reads the period once per tick and
/// clamps it between its configured period and its ceiling, so nothing
/// written here can make it tick faster than configured or slower than its
/// ceiling. The handle is shared because the owner is a thread of its own
/// and the derivation runs beside the transport that measures.
#[derive(Clone, Default)]
pub(crate) struct TickPeriod(Arc<TickShared>);
#[derive(Default)]
struct TickShared {
    period_ns: AtomicU64,
    election_tick: AtomicUsize,
    /// The periods the owner has run: its progress, which a wait is
    /// charged in (27 §3.1 P8).
    periods: AtomicU64,
    /// The periods in which the replica was not ticked, because it was
    /// refused the room or still persisted what the tick before had left.
    refused: AtomicU64,
    /// When the owner last ran a period, as nanoseconds since the process
    /// began; and the longest a period took, from one to the next. An owner
    /// that stalls in a period (a disk that took long, a thread that was
    /// not scheduled) is seen here and nowhere else: its replica missed
    /// its heartbeats and its followers may have campaigned.
    last_advance_ns: AtomicU64,
    longest_ns: AtomicU64,
    /// The longest a period took beyond what the owner meant it to be: a
    /// stall, which the replica's patience covers ([`Self::patience`]) and
    /// which is remembered, with when it was seen, for the margin the
    /// paths are covered by times its own length (`ELECTION_MARGIN`): a
    /// stall that lasted a second stands for ten, and a longer one takes
    /// its place at once. The excess and not the period: a period is what
    /// the pace made it, and a pace fed its own periods would hold itself
    /// wherever it was.
    stall_ns: AtomicU64,
    stall_seen_ns: AtomicU64,
    /// The quorum's exchange tail with this group's other voters, in
    /// nanoseconds, zero while not enough of them are measured
    /// ([`quorum_tail`]): how late a commit's answers may come from
    /// followers whose owners stall, which the owner's own periods do not
    /// show (27 §8.4).
    quorum_tail_ns: AtomicU64,
    /// The last derivation as one value, for observers.
    derived: Mutex<Option<focal_timing::TickPace>>,
    /// What the session turned away for room, counted where it was turned
    /// away (`InputRefusals`): beside its periods because its host and its
    /// owner, on threads of their own, both turn input away and both hold
    /// this, and the node's metrics read it in place.
    frames_refused: RefusalCounts,
    requests_refused: RefusalCounts,
    frames_dropped: AtomicU64,
    requests_dropped: AtomicU64,
    /// The room the peers' frames have on their way to the owner: how many
    /// may be queued for it at once, which the owner states — its peer
    /// reserve (`Owner::peer_reserve`) — and how many are, each holding its
    /// place (`FrameTicket`). A peer's frame is queued in this room, beside
    /// the participants' queue and never in it (F56).
    frame_room: AtomicUsize,
    frames_queued: AtomicUsize,
    /// The participants' requests queued for the owner and not yet taken,
    /// each holding its place ([`QueueTicket`]); since when the queue has
    /// held one without emptying (the admission of the one that ended its
    /// last empty spell); when the owner last took one, and how long that
    /// one had waited: what the session's queueing delay is read from
    /// ([`TickPeriod::queueing_delay`]), in nanoseconds since the process
    /// began.
    requests_queued: AtomicU64,
    busy_since_ns: AtomicU64,
    taken_ns: AtomicU64,
    sojourn_ns: AtomicU64,
    /// The least time the owner took to answer a participant's request
    /// once it took it, over the last [`BASE_WINDOW`], and when that least
    /// was measured: the session's write cycle uncongested — what its
    /// queueing delay is judged by ([`TickPeriod::delay_bound`]). Zero until
    /// one is measured ([`TickPeriod::served`]).
    base_service_ns: AtomicU64,
    base_seen_ns: AtomicU64,
    /// When the oldest write the owner took and has not answered was admitted, in nanoseconds
    /// since the process began, zero while it holds none ([`TickPeriod::set_oldest_write`]): the
    /// age of the oldest request in the session's pipeline, Breakwater's signal (the oldest
    /// timestamp a queue holds, Cho et al., OSDI '20, §3.1) taken over the queue and the
    /// pipeline both. A request the owner took at once and then held behind a deep pipeline waits
    /// there, not in the queue, and its wait is seen only here; and as the session refuses, its
    /// oldest writes are answered and the age falls, so a refusal never keeps itself in force.
    oldest_write_ns: AtomicU64,
}
/// The bound a session's host turned an input away at, before it was
/// queued ([`TickPeriod::refuse_input`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum InputBound {
    /// The node's memory budget had no room for what the input holds on
    /// its way: its frame, its reply's room and its place.
    Memory,
    /// The runtime had no reply cell for it.
    Reply,
    /// Its queue was full: the participants' queue for a request, the room
    /// the owner's peer reserve gives for a peer's frame.
    Queue,
    /// Its session's queueing delay was past the bound the session's own
    /// service time gives it ([`TickPeriod::delay_bound`]): a request
    /// queued then would wait beyond what the session serves in, so it is
    /// refused before it is charged or queued, as Breakwater's AQM drops
    /// one (Cho et al., OSDI '20, §3.2.3).
    Delay,
}
/// One kind of input's refusals, a count for each bound.
#[derive(Default)]
struct RefusalCounts {
    memory: AtomicU64,
    reply: AtomicU64,
    queue: AtomicU64,
    delay: AtomicU64,
}
impl RefusalCounts {
    fn count(&self, bound: InputBound) {
        match bound {
            InputBound::Memory => &self.memory,
            InputBound::Reply => &self.reply,
            InputBound::Queue => &self.queue,
            InputBound::Delay => &self.delay,
        }
        .fetch_add(1, Ordering::Relaxed);
    }
    fn read(&self) -> crate::metrics::RefusedBy {
        crate::metrics::RefusedBy {
            memory: self.memory.load(Ordering::Relaxed),
            reply: self.reply.load(Ordering::Relaxed),
            queue: self.queue.load(Ordering::Relaxed),
            delay: self.delay.load(Ordering::Relaxed),
        }
    }
}
/// One of the places the owner's peer reserve gives the peers' frames
/// queued for it (`TickPeriod::queue_frame`), held by the frame until the
/// owner takes it, or it is dropped, and given back then.
pub(crate) struct FrameTicket(TickPeriod);
impl Drop for FrameTicket {
    fn drop(&mut self) {
        let _ =
            (self.0)
                .0
                .frames_queued
                .fetch_update(Ordering::AcqRel, Ordering::Acquire, |queued| {
                    queued.checked_sub(1)
                });
    }
}
/// A participant's request's place in the queue to its owner
/// ([`TickPeriod::queue_request`]), held from its admission until the owner
/// takes it ([`QueueTicket::taken`]) or it is dropped, which gives the place
/// back: the queue's count is right whatever ends the request's wait.
pub(crate) struct QueueTicket {
    pace: TickPeriod,
    admitted_ns: u64,
}
impl QueueTicket {
    /// The owner takes the request: how long it waited is the session's
    /// latest sojourn, and now is when the owner last took one. The wait,
    /// returned, is the first part of the request's time to its answer.
    pub(crate) fn taken(self) -> Duration {
        let now = now_ns();
        let shared = &self.pace.0;
        let waited = now.saturating_sub(self.admitted_ns);
        shared.sojourn_ns.store(waited, Ordering::Relaxed);
        shared.taken_ns.store(now, Ordering::Relaxed);
        Duration::from_nanos(waited)
    }
}
impl Drop for QueueTicket {
    fn drop(&mut self) {
        let _ = self.pace.0.requests_queued.fetch_update(
            Ordering::AcqRel,
            Ordering::Acquire,
            |queued| queued.checked_sub(1),
        );
    }
}
/// When the owner took a participant's request and how long it had waited to
/// be taken, for its service time and its time to its answer once it is
/// answered ([`TickPeriod::served`]); held while the request is in service,
/// and given back however it ends.
pub(crate) struct ServiceClock {
    pace: TickPeriod,
    taken: std::time::Instant,
    queued: Duration,
    /// Whether the pipeline held no other write as this one was taken: its
    /// service is the session's uncongested time, the only kind that may set
    /// the least ([`TickPeriod::base_service`]).
    alone: bool,
}
impl ServiceClock {
    /// The request taken at this clock's start is answered now.
    pub(crate) fn answered(self) {
        if self.alone {
            self.pace.served(self.taken.elapsed());
        }
    }
    /// When the request was admitted, in nanoseconds since the process began:
    /// taken less what it waited to be taken.
    pub(crate) fn admitted_ns(&self) -> u64 {
        let taken = u64::try_from(self.taken.saturating_duration_since(began()).as_nanos())
            .unwrap_or(u64::MAX);
        taken.saturating_sub(u64::try_from(self.queued.as_nanos()).unwrap_or(u64::MAX))
    }
}
/// Now, in nanoseconds since the process began.
fn now_ns() -> u64 {
    u64::try_from(began().elapsed().as_nanos()).unwrap_or(u64::MAX)
}
/// How long the least service time a session measured stands before a
/// newer, longer one replaces it: BBR's min-RTT filter (Cardwell et al.,
/// "BBR: Congestion-Based Congestion Control", ACM Queue 2016;
/// draft-cardwell-iccrg-bbr-congestion-control, MinRTTFilterLen = 10 s). A
/// least taken over a window and not a mean: a mean of the time a request
/// takes to be answered grows with the queue a congested pipeline holds it
/// in, and a bound that grows with the congestion it bounds holds nothing
/// (measured 2026-10-12: a bound of eight mean service times let the p99
/// reach 2.9 s at 1,000 writes a second).
const BASE_WINDOW: Duration = Duration::from_secs(10);
/// The multiple of a session's least service time a request may wait queued
/// before it is refused: Breakwater's AQM threshold, twice its target delay,
/// the target being 40% of an SLO of ten times a request's service and round
/// trip (Cho et al., OSDI '20, §3.2.3 and §5.1: AQM at 2·d_t, d_t = 0.4·SLO,
/// SLO = 10 × (service + RTT)); 2 × 0.4 × 10 = 8. A session's service and
/// round trip together are the time it takes to answer a request it took,
/// read uncongested (its least over [`BASE_WINDOW`]). Two such times were
/// measured too few: a session on focal-log takes no input while its write
/// is out, so a request queued behind a busy write cycle waited 15–70 ms
/// against a least of 12 ms at 1,000 writes a second, uncongested, and was
/// refused (2026-10-12).
const DELAY_BOUND_CYCLES: u32 = 8;
/// What a session turned away for room, of peers' frames and participants'
/// requests apart: refused by its host before it was queued — the room the
/// peers' frames have, the participants' queue or its memory full — and
/// dropped by its fleet's owner once queued: a participant's request at the
/// scheduler's quota, a peer's frame where its session's queue could not
/// grow. A peer counts a refused frame lost; a dropped one it asks again
/// once, then counts lost.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct InputRefusals {
    pub(crate) frames_refused: crate::metrics::RefusedBy,
    pub(crate) requests_refused: crate::metrics::RefusedBy,
    pub(crate) frames_dropped: u64,
    pub(crate) requests_dropped: u64,
}
/// When this process began, for the owners' periods to be timed against.
fn began() -> std::time::Instant {
    static BEGAN: std::sync::OnceLock<std::time::Instant> = std::sync::OnceLock::new();
    *BEGAN.get_or_init(std::time::Instant::now)
}
impl TickPeriod {
    #[cfg(test)]
    pub(crate) fn set(&self, period: Duration) {
        self.store(period);
    }
    fn store(&self, period: Duration) {
        self.0.period_ns.store(
            u64::try_from(period.as_nanos()).unwrap_or(u64::MAX),
            Ordering::Relaxed,
        );
    }
    /// Put a derivation in force from the owner's next tick.
    pub(crate) fn publish(&self, pace: focal_timing::TickPace) {
        self.store(pace.period);
        if let Ok(mut derived) = self.0.derived.lock() {
            *derived = Some(pace);
        }
    }
    pub(crate) fn derived(&self) -> Option<focal_timing::TickPace> {
        self.0.derived.lock().ok().and_then(|derived| *derived)
    }
    /// Whether the period in force is longer than the configured one.
    pub(crate) fn stretched(&self, configured: Duration, ceiling: Duration) -> bool {
        self.get(configured, ceiling) > configured
    }
    pub(crate) fn get(&self, configured: Duration, ceiling: Duration) -> Duration {
        Duration::from_nanos(self.0.period_ns.load(Ordering::Relaxed))
            .clamp(configured, ceiling.max(configured))
    }
    /// The owner ran one period, which it meant to be `intended` long.
    pub(crate) fn advance(&self, intended: Duration) {
        self.0.periods.fetch_add(1, Ordering::Relaxed);
        let now = u64::try_from(began().elapsed().as_nanos()).unwrap_or(u64::MAX);
        let last = self.0.last_advance_ns.swap(now, Ordering::Relaxed);
        if last != 0 {
            let took = now.saturating_sub(last);
            self.0.longest_ns.fetch_max(took, Ordering::Relaxed);
            let meant = u64::try_from(intended.as_nanos()).unwrap_or(u64::MAX);
            let excess = took.saturating_sub(meant);
            if excess >= self.stall_at(now) {
                self.0.stall_ns.store(excess, Ordering::Relaxed);
                self.0.stall_seen_ns.store(now, Ordering::Relaxed);
            }
        }
    }
    /// The stall remembered at `now`: none once it has stood for the
    /// margin times its length.
    fn stall_at(&self, now: u64) -> u64 {
        let stall = self.0.stall_ns.load(Ordering::Relaxed);
        let seen = self.0.stall_seen_ns.load(Ordering::Relaxed);
        let held = now.saturating_sub(seen);
        if held >= focal_timing::ELECTION_MARGIN.saturating_mul(stall) {
            0
        } else {
            stall
        }
    }
    /// The longest a period took, from one to the next.
    pub(crate) fn longest(&self) -> Duration {
        Duration::from_nanos(self.0.longest_ns.load(Ordering::Relaxed))
    }
    /// The stall the owner remembers now, in nanoseconds.
    pub(crate) fn stall(&self) -> u64 {
        self.stall_at(u64::try_from(began().elapsed().as_nanos()).unwrap_or(u64::MAX))
    }
    /// The ticks the stall is covered by: what the longest stall the owner
    /// remembers took, and a tail of the paths after it, in periods of the
    /// pace in force. The replica waits as many beyond its election
    /// timeout before it campaigns (`hyper_raft::Raft::set_patience`): a
    /// node that stalls cannot tell a leader that stalls as it does from
    /// one that died, and a leader's heartbeat leaves its tick, so the one
    /// after a stall takes a tail to arrive. In ticks and not in the
    /// period (27 §3.1 P2): what stretches is the election timeout, and
    /// nothing else that is counted in periods.
    pub(crate) fn patience(&self, configured: Duration, ceiling: Duration) -> usize {
        let stall = self.stall();
        if stall == 0 {
            return 0;
        }
        let tail = self.derived().map_or(0, |pace| pace.broadcast_tail_ns);
        let period = u64::try_from(self.get(configured, ceiling).as_nanos())
            .unwrap_or(u64::MAX)
            .max(1);
        usize::try_from(stall.saturating_add(tail).div_ceil(period)).unwrap_or(usize::MAX)
    }
    /// Put the quorum's exchange tail in force ([`quorum_tail`]); none
    /// while not enough voters are measured.
    pub(crate) fn publish_quorum_tail(&self, tail: Option<Duration>) {
        let ns = tail.map_or(0, |tail| u64::try_from(tail.as_nanos()).unwrap_or(u64::MAX));
        self.0.quorum_tail_ns.store(ns, Ordering::Relaxed);
    }
    /// The ticks the quorum's exchange tail takes, in periods of the pace
    /// in force: what a leader's followers may answer beyond its own
    /// periods. At most an election timeout at the tick ceiling, the
    /// longest a dead leader goes unnoticed; zero while not enough voters
    /// are measured. What a request waits for a commit is given these
    /// beyond its time.
    pub(crate) fn quorum_ticks(&self, configured: Duration, ceiling: Duration) -> usize {
        let tail = self.0.quorum_tail_ns.load(Ordering::Relaxed);
        if tail == 0 {
            return 0;
        }
        let period = u64::try_from(self.get(configured, ceiling).as_nanos())
            .unwrap_or(u64::MAX)
            .max(1);
        let most = u64::try_from(ceiling.max(configured).as_nanos())
            .unwrap_or(u64::MAX)
            .saturating_mul(u64::try_from(self.election_tick().max(1)).unwrap_or(u64::MAX));
        usize::try_from(tail.min(most).div_ceil(period)).unwrap_or(usize::MAX)
    }
    /// The period passed without a tick.
    pub(crate) fn refuse(&self) {
        self.0.refused.fetch_add(1, Ordering::Relaxed);
    }
    /// The host refused a peer's frame (`frame`) or a participant's request
    /// for room before it was queued, at `bound`.
    pub(crate) fn refuse_input(&self, frame: bool, bound: InputBound) {
        if frame {
            self.0.frames_refused.count(bound);
        } else {
            self.0.requests_refused.count(bound);
        }
    }
    /// The owner states the room its peers' frames have on their way to it:
    /// its peer reserve (`Owner::peer_reserve`). A room made smaller takes
    /// no frame until as many as are over it have been taken.
    pub(crate) fn set_frame_room(&self, room: usize) {
        self.0.frame_room.store(room, Ordering::Relaxed);
    }
    /// A place for one more of the peers' frames queued for the owner,
    /// while the room the owner states has one; none before it states it.
    pub(crate) fn queue_frame(&self) -> Option<FrameTicket> {
        let room = self.0.frame_room.load(Ordering::Relaxed);
        self.0
            .frames_queued
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |queued| {
                if queued < room {
                    queued.checked_add(1)
                } else {
                    None
                }
            })
            .ok()?;
        Some(FrameTicket(self.clone()))
    }
    /// A place in the queue to the owner for a participant's request
    /// admitted now. The first to find the queue empty starts its busy
    /// spell: the sojourn the last spell ended with says nothing of this one.
    pub(crate) fn queue_request(&self) -> QueueTicket {
        let admitted_ns = now_ns();
        if self.0.requests_queued.fetch_add(1, Ordering::AcqRel) == 0 {
            self.0.sojourn_ns.store(0, Ordering::Relaxed);
            self.0.busy_since_ns.store(admitted_ns, Ordering::Relaxed);
        }
        QueueTicket {
            pace: self.clone(),
            admitted_ns,
        }
    }
    /// How long the oldest of the participants' requests queued now has
    /// waited, as near as the queue's own marks tell it: none while none is
    /// queued; otherwise at least since the owner last took one or since
    /// the queue last stopped being empty, whichever is later (no request
    /// queued now came before that), and at least as long as the one taken
    /// last had waited (each was queued no later than the one after it in
    /// its class). Breakwater's congestion signal (Cho et al., OSDI '20,
    /// §3.1): the delay a request is kept, which a load's variance in
    /// service time does not blur as a queue's length does.
    pub(crate) fn queueing_delay(&self) -> Duration {
        if self.0.requests_queued.load(Ordering::Acquire) == 0 {
            return Duration::ZERO;
        }
        let now = now_ns();
        let since = self
            .0
            .taken_ns
            .load(Ordering::Relaxed)
            .max(self.0.busy_since_ns.load(Ordering::Relaxed));
        Duration::from_nanos(
            now.saturating_sub(since)
                .max(self.0.sojourn_ns.load(Ordering::Relaxed)),
        )
    }
    /// A clock started as the owner takes a participant's request that waited
    /// `queued` to be taken, read when the request is answered
    /// ([`ServiceClock::answered`]); the request is in service while it is held.
    pub(crate) fn service_clock(&self, queued: Duration) -> ServiceClock {
        ServiceClock {
            pace: self.clone(),
            taken: std::time::Instant::now(),
            queued,
            alone: self.0.oldest_write_ns.load(Ordering::Relaxed) == 0,
        }
    }
    /// The owner answered, `service` after taking it, a participant's request it
    /// took with no other write in its pipeline: uncongested. The least such
    /// time stands for [`BASE_WINDOW`]: a sample no longer replaces it until
    /// then (BBR's min-RTT update: a sample at or under the least, or a least
    /// that expired). Only uncongested samples: under a load that never empties
    /// the pipeline every request waits behind others, and a least taken from
    /// those rose with the congestion it bounds (BBR drains its pipe to
    /// re-measure, ProbeRTT; a session needs no drain, as a request that finds
    /// its pipeline empty is the measurement). Only the owner thread answers, so
    /// these marks have one writer.
    pub(crate) fn served(&self, service: Duration) {
        let sample = u64::try_from(service.as_nanos()).unwrap_or(u64::MAX).max(1);
        let now = now_ns();
        let base = self.0.base_service_ns.load(Ordering::Relaxed);
        let seen = self.0.base_seen_ns.load(Ordering::Relaxed);
        let window = u64::try_from(BASE_WINDOW.as_nanos()).unwrap_or(u64::MAX);
        if base == 0 || sample <= base || now.saturating_sub(seen) > window {
            self.0.base_service_ns.store(sample, Ordering::Relaxed);
            self.0.base_seen_ns.store(now, Ordering::Relaxed);
        }
    }
    /// The owner states when the oldest write it holds unanswered was admitted
    /// ([`ServiceClock::admitted_ns`]), or none.
    pub(crate) fn set_oldest_write(&self, admitted_ns: Option<u64>) {
        self.0
            .oldest_write_ns
            .store(admitted_ns.unwrap_or(0), Ordering::Relaxed);
    }
    /// How long the session's pipeline has held its oldest write past the time
    /// one takes uncongested; none while it holds none.
    pub(crate) fn service_delay(&self) -> Duration {
        let oldest = self.0.oldest_write_ns.load(Ordering::Relaxed);
        if oldest == 0 {
            return Duration::ZERO;
        }
        Duration::from_nanos(
            now_ns()
                .saturating_sub(oldest)
                .saturating_sub(self.0.base_service_ns.load(Ordering::Relaxed)),
        )
    }
    /// The session's least service time over the last [`BASE_WINDOW`]: its
    /// write cycle uncongested; none until one is measured.
    pub(crate) fn base_service(&self) -> Option<Duration> {
        match self.0.base_service_ns.load(Ordering::Relaxed) {
            0 => None,
            ns => Some(Duration::from_nanos(ns)),
        }
    }
    /// The longest a request may wait queued before the session refuses
    /// more ([`InputBound::Delay`]): [`DELAY_BOUND_CYCLES`] of its write
    /// cycle uncongested. None until the session has served a request: what
    /// it refuses is judged by what it measured, never by a number picked
    /// for it.
    pub(crate) fn delay_bound(&self) -> Option<Duration> {
        self.base_service()
            .map(|base| base.saturating_mul(DELAY_BOUND_CYCLES))
    }
    /// Whether a participant's request is to be refused now for the delay
    /// it would be queued behind ([`Self::delay_bound`]).
    pub(crate) fn delay_exceeded(&self) -> bool {
        self.delay_bound()
            .is_some_and(|bound| self.queueing_delay() > bound || self.service_delay() > bound)
    }
    /// The room the owner last stated, and the frames queued in it now.
    #[cfg(test)]
    pub(crate) fn frame_room(&self) -> (usize, usize) {
        (
            self.0.frame_room.load(Ordering::Relaxed),
            self.0.frames_queued.load(Ordering::Relaxed),
        )
    }
    /// The fleet's owner dropped a peer's frame (`frame`) or a
    /// participant's request once queued.
    pub(crate) fn drop_input(&self, frame: bool) {
        let counter = if frame {
            &self.0.frames_dropped
        } else {
            &self.0.requests_dropped
        };
        counter.fetch_add(1, Ordering::Relaxed);
    }
    pub(crate) fn input_refusals(&self) -> InputRefusals {
        InputRefusals {
            frames_refused: self.0.frames_refused.read(),
            requests_refused: self.0.requests_refused.read(),
            frames_dropped: self.0.frames_dropped.load(Ordering::Relaxed),
            requests_dropped: self.0.requests_dropped.load(Ordering::Relaxed),
        }
    }
    pub(crate) fn refused(&self) -> u64 {
        self.0.refused.load(Ordering::Relaxed)
    }
    pub(crate) fn periods(&self) -> u64 {
        self.0.periods.load(Ordering::Relaxed)
    }
    /// The owner states its replica's election ticks once it has opened it.
    pub(crate) fn announce(&self, election_tick: usize) {
        self.0.election_tick.store(election_tick, Ordering::Relaxed);
    }
    /// Zero until the owner has opened its replica.
    pub(crate) fn election_tick(&self) -> usize {
        self.0.election_tick.load(Ordering::Relaxed)
    }
}
/// The exchange tail a commit waits on among a group of `voters` voters, this
/// one among them, from the `tails` measured with the others: a commit
/// needs a majority, this voter and the `voters / 2` others that answer
/// first, so the tail is the `voters / 2`-th smallest. None where fewer of
/// the others are measured, or where this voter is a majority alone: then
/// nothing is known, or nothing waited on.
pub(crate) fn quorum_tail(mut tails: Vec<Duration>, voters: usize) -> Option<Duration> {
    let needed = voters.checked_div(2)?;
    if needed == 0 {
        return None;
    }
    tails.sort_unstable();
    tails.get(needed.checked_sub(1)?).copied()
}
impl PartialEq for TickPeriod {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}
impl Eq for TickPeriod {}
impl std::fmt::Debug for TickPeriod {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("TickPeriod")
            .field("derived", &self.derived())
            .finish()
    }
}

#[cfg(test)]
#[cfg_attr(
    test,
    allow(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::panic,
        clippy::indexing_slicing,
        clippy::arithmetic_side_effects
    )
)]
mod quorum_tests {
    use super::*;

    fn ms(n: u64) -> Duration {
        Duration::from_millis(n)
    }

    /// A commit waits on the majority that answers first: of three voters
    /// the faster other, of five the second fastest of four, of four the
    /// second fastest of three (a majority of four is three).
    #[test]
    fn a_commit_waits_on_the_majority_that_answers_first() {
        assert_eq!(quorum_tail(vec![ms(30), ms(5)], 3), Some(ms(5)));
        assert_eq!(
            quorum_tail(vec![ms(40), ms(9), ms(700), ms(3)], 5),
            Some(ms(9))
        );
        assert_eq!(quorum_tail(vec![ms(40), ms(9), ms(700)], 4), Some(ms(40)));
    }

    /// Too few others measured says nothing, and a voter that is a
    /// majority alone waits on no one.
    #[test]
    fn too_few_measured_or_a_lone_voter_waits_on_nothing() {
        assert_eq!(quorum_tail(vec![], 3), None);
        assert_eq!(quorum_tail(vec![ms(5)], 5), None);
        assert_eq!(quorum_tail(vec![], 1), None);
        assert_eq!(quorum_tail(vec![ms(5)], 0), None);
    }

    /// The tail in ticks of the pace in force, rounded up, never past an
    /// election timeout at the ceiling, and none while unmeasured.
    #[test]
    fn the_quorum_tail_in_ticks_is_bounded_by_an_election_timeout_at_the_ceiling() {
        let pace = TickPeriod::default();
        pace.announce(10);
        let (tick, ceiling) = (ms(25), ms(100));
        assert_eq!(pace.quorum_ticks(tick, ceiling), 0);
        pace.publish_quorum_tail(Some(ms(301)));
        assert_eq!(pace.quorum_ticks(tick, ceiling), 13);
        pace.publish_quorum_tail(Some(Duration::from_secs(3600)));
        assert_eq!(
            pace.quorum_ticks(tick, ceiling),
            40,
            "ten ticks at 100 ms, in 25 ms ticks"
        );
        pace.publish_quorum_tail(None);
        assert_eq!(pace.quorum_ticks(tick, ceiling), 0);
    }
}

#[cfg(test)]
#[cfg_attr(
    test,
    allow(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::panic,
        clippy::indexing_slicing,
        clippy::arithmetic_side_effects
    )
)]
mod frame_room_tests {
    use super::*;

    /// The peers' frames are queued in the room the owner states and no
    /// more; a frame taken or dropped gives its place back, and a room made
    /// smaller takes none until as many as are over it are gone.
    #[test]
    fn a_peers_frame_is_queued_in_the_room_the_owner_states() {
        let pace = TickPeriod::default();
        assert!(pace.queue_frame().is_none(), "no room before it is stated");
        pace.set_frame_room(2);
        let first = pace.queue_frame().unwrap();
        let second = pace.queue_frame().unwrap();
        assert!(pace.queue_frame().is_none(), "the room is full");
        assert_eq!(pace.frame_room(), (2, 2));
        drop(first);
        let third = pace.queue_frame().unwrap();
        pace.set_frame_room(1);
        drop(second);
        assert!(pace.queue_frame().is_none(), "one is over the smaller room");
        drop(third);
        assert_eq!(pace.frame_room(), (1, 0));
        let fourth = pace.queue_frame().unwrap();
        assert!(pace.queue_frame().is_none());
        drop(fourth);
        assert_eq!(pace.frame_room(), (1, 0));
    }
}
