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
    /// writes and the rest apart ([`QueueMarks`]): a session holds its writes
    /// for its pipeline ([`TickPeriod::pipeline_open`]) while it answers its
    /// reads, so each kind's delay is its own.
    writes: QueueMarks,
    others: QueueMarks,
    /// The least time a write took from its taking to its answer with no
    /// other write in its pipeline, in the term the session leads in: its
    /// write cycle uncongested, what every delay is judged by
    /// ([`TickPeriod::delay_bound`]). Zero until one is measured
    /// ([`TickPeriod::served`]); only falls within a term
    /// ([`TickPeriod::reset_base`]).
    base_service_ns: AtomicU64,
    /// The writes the owner answered within one bound, counted interval by
    /// interval as they are answered: when the current interval began and
    /// how many it has answered; and the most any interval answered over the
    /// last [`BASE_WINDOW`], and when that was. What the pipeline answers
    /// within a bound is its window by Little's law ([`TickPeriod::write_window`]),
    /// counted directly and not as a rate times the bound: a delivery-rate
    /// sample taken as each write is answered read the pipeline's bursts of
    /// answers — 20,000 writes a second where it answered 5,000 (three local
    /// nodes, 2026-10-12) — and a window built of them bounded nothing.
    interval_began_ns: AtomicU64,
    interval_answered: AtomicU64,
    answered_most: AtomicU64,
    answered_most_seen_ns: AtomicU64,
    /// When the oldest write the owner took and has not answered was admitted, in nanoseconds
    /// since the process began, zero while it holds none ([`TickPeriod::set_oldest_write`]): the
    /// age of the oldest request in the session's pipeline, Breakwater's signal (the oldest
    /// timestamp a queue holds, Cho et al., OSDI '20, §3.1) taken over the queue and the
    /// pipeline both. A request the owner took at once and then held behind a deep pipeline waits
    /// there, not in the queue, and its wait is seen only here; and as the session refuses, its
    /// oldest writes are answered and the age falls, so a refusal never keeps itself in force.
    oldest_write_ns: AtomicU64,
}
/// One kind of the participants' requests queued for the owner: how many
/// wait, each holding its place ([`QueueTicket`]); since when the queue has
/// held one without emptying (the admission of the one that ended its last
/// empty spell); when the owner last took one, and how long that one had
/// waited, in nanoseconds since the process began: what its queueing delay
/// is read from ([`QueueMarks::delay_at`]). And since when that delay has
/// been past its bound without a break, zero while it is not: CoDel's
/// `first_above_time` (RFC 8289 §5.5).
#[derive(Default)]
struct QueueMarks {
    queued: AtomicU64,
    busy_since_ns: AtomicU64,
    taken_ns: AtomicU64,
    sojourn_ns: AtomicU64,
    above_since_ns: AtomicU64,
}
impl QueueMarks {
    /// One more is queued at `now`. The first to find the queue empty
    /// starts its busy spell: the sojourn the last spell ended with says
    /// nothing of this one.
    fn enter(&self, now: u64) {
        if self.queued.fetch_add(1, Ordering::AcqRel) == 0 {
            self.sojourn_ns.store(0, Ordering::Relaxed);
            self.busy_since_ns.store(now, Ordering::Relaxed);
        }
    }
    /// One left the queue, taken or dropped.
    fn leave(&self) {
        let _ = self
            .queued
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |queued| {
                queued.checked_sub(1)
            });
    }
    /// The owner took one admitted at `admitted` now: how long it waited.
    fn taken(&self, admitted: u64, now: u64) -> u64 {
        let waited = now.saturating_sub(admitted);
        self.sojourn_ns.store(waited, Ordering::Relaxed);
        self.taken_ns.store(now, Ordering::Relaxed);
        waited
    }
    /// How long the oldest queued now has waited, as near as the marks tell
    /// it: none while none is queued; otherwise at least since the owner
    /// last took one or since the queue last stopped being empty, whichever
    /// is later (none queued now came before that), and at least as long as
    /// the one taken last had waited (each was queued no later than the one
    /// after it). Breakwater's congestion signal (Cho et al., OSDI '20,
    /// §3.1): the delay a request is kept, which a load's variance in
    /// service time does not blur as a queue's length does.
    fn delay_at(&self, now: u64) -> Duration {
        if self.queued.load(Ordering::Acquire) == 0 {
            return Duration::ZERO;
        }
        let since = self
            .taken_ns
            .load(Ordering::Relaxed)
            .max(self.busy_since_ns.load(Ordering::Relaxed));
        Duration::from_nanos(
            now.saturating_sub(since)
                .max(self.sojourn_ns.load(Ordering::Relaxed)),
        )
    }
    /// CoDel's test (RFC 8289 §5.5): whether the delay, past `bound` now, has
    /// stayed past it a whole `interval` without a break. A delay at or
    /// under the bound even once starts the interval again.
    fn standing_at(&self, now: u64, bound: Duration, interval: Duration) -> bool {
        if self.delay_at(now) <= bound {
            self.above_since_ns.store(0, Ordering::Relaxed);
            return false;
        }
        // Zero is "not above": a delay first past its bound as the process began is marked a
        // nanosecond later.
        let since = match self.above_since_ns.compare_exchange(
            0,
            now.max(1),
            Ordering::AcqRel,
            Ordering::Acquire,
        ) {
            Ok(_) => return false,
            Err(since) => since,
        };
        now.saturating_sub(since) >= u64::try_from(interval.as_nanos()).unwrap_or(u64::MAX)
    }
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
    write: bool,
}
impl QueueTicket {
    /// The owner takes the request: how long it waited is its kind's latest
    /// sojourn, and now is when the owner last took one of that kind. The
    /// wait, returned, is the first part of the request's time to its
    /// answer.
    pub(crate) fn taken(self) -> Duration {
        let marks = self.pace.marks(self.write);
        Duration::from_nanos(marks.taken(self.admitted_ns, now_ns()))
    }
}
impl Drop for QueueTicket {
    fn drop(&mut self) {
        self.pace.marks(self.write).leave();
    }
}
/// When the owner took a participant's request and how long it had waited to
/// be taken, for its service time and its time to its answer once it is
/// answered ([`TickPeriod::served`]); held while the request is in service,
/// and given back however it ends.
pub(crate) struct ServiceClock {
    pace: TickPeriod,
    taken: std::time::Instant,
    /// Whether it is a write: its answer is counted in its interval, and its
    /// service is a sample of the session's cycle ([`TickPeriod::served`]).
    write: bool,
}
impl ServiceClock {
    /// The request taken at this clock's start is answered now.
    pub(crate) fn answered(self) {
        if self.write {
            self.pace.delivered();
            self.pace.served(self.taken.elapsed());
        }
    }
    /// When the owner took the request, in nanoseconds since the process
    /// began: its time in the pipeline starts here.
    pub(crate) fn taken_ns(&self) -> u64 {
        u64::try_from(self.taken.saturating_duration_since(began()).as_nanos()).unwrap_or(u64::MAX)
    }
}
/// Now, in nanoseconds since the process began.
fn now_ns() -> u64 {
    u64::try_from(began().elapsed().as_nanos()).unwrap_or(u64::MAX)
}
/// How long the most writes a session answered in one bound stands before a
/// lower count replaces it: BBR's bandwidth filter, over a window as its min
/// RTT's (Cardwell et al., "BBR: Congestion-Based Congestion Control", ACM
/// Queue 2016; draft-cardwell-iccrg-bbr-congestion-control, MinRTTFilterLen =
/// 10 s).
const BASE_WINDOW: Duration = Duration::from_secs(10);
/// The multiple of a session's uncongested write cycle a write may take in its
/// pipeline, and wait in its queue, before the session holds the next: the
/// latency it gives for its throughput. A pipeline batches: a cycle carrying
/// `n` writes takes about the cycle uncongested, `b`, and a cost `c` for each
/// write, so it answers `n / (b + c·n)` a second, approaching `1/c`, and a
/// batch of `k·b/c` writes takes `(k + 1)·b` to answer `k / (k + 1)` of that
/// most. A bound of `m` cycles therefore buys `1 - 1/m` of the session's
/// greatest throughput, whatever its costs, and doubling the pipeline from
/// `m` cycles takes it to `2m - 1`: from 2.5 cycles a doubling buys a quarter
/// more, from four a seventh, at twice the latency. A quarter is where BBR
/// judges a pipe full (Cardwell et al., ACM Queue 2016: the delivery rate grew
/// less than 25% over three rounds of doubling), so the bound is four cycles,
/// three quarters of the most. Ten bought the last part at the latency it
/// costs: on 1-CPU CI nodes at 4,000 writes a second, about a tenth more
/// committed than land-14's fixed 32 writes in flight, at three times its
/// client p50 (130 ms against 45) and refusals answered in 50 ms where
/// land-14's took 10 (A/B run 38287321502). Two (Breakwater's AQM at twice
/// its target, Cho et al., OSDI '20) bought half: on three local nodes at
/// 16,000 writes a second offered it held 2,000 a second where the pipeline
/// unbounded held 4,500 (2026-10-12).
const DELAY_BOUND_CYCLES: u32 = 4;
/// The bounds a queue's delay must stay past its bound for before it is a
/// standing queue ([`TickPeriod::standing_interval`]): two, the bound already
/// being four cycles — a burst that drains within twice what a write may take
/// is a good queue (RFC 8289 §2), where CoDel's own ratio (its target 5 to
/// 10% of its interval, §4.4) is set for a target well under a round trip.
const STANDING_BOUNDS: u32 = 2;
/// The write window before a rate is measured, and its least: TCP's initial
/// window (RFC 6928, ten segments), so a session that has not yet answered
/// a write a cycle's worth takes ten.
pub(crate) const INITIAL_WINDOW: usize = 10;
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
    /// The marks of one kind of participants' request: writes, or the rest.
    fn marks(&self, write: bool) -> &QueueMarks {
        if write {
            &self.0.writes
        } else {
            &self.0.others
        }
    }
    /// A place in the queue to the owner for a participant's request
    /// admitted now, a `write` or not.
    pub(crate) fn queue_request(&self, write: bool) -> QueueTicket {
        let admitted_ns = now_ns();
        self.marks(write).enter(admitted_ns);
        QueueTicket {
            pace: self.clone(),
            admitted_ns,
            write,
        }
    }
    /// How long the oldest queued request of its kind has waited
    /// ([`QueueMarks::delay_at`]).
    #[cfg(test)]
    pub(crate) fn queueing_delay(&self, write: bool) -> Duration {
        self.marks(write).delay_at(now_ns())
    }
    /// How many requests of its kind are queued.
    #[cfg(test)]
    pub(crate) fn queued(&self, write: bool) -> u64 {
        self.marks(write).queued.load(Ordering::Acquire)
    }
    /// A clock started as the owner takes a participant's request, read when
    /// the request is answered
    /// ([`ServiceClock::answered`]); the request is in service while it is held.
    /// Only a `write` measures the session's cycle: a read's answer waits on a
    /// barrier and not a write, and a least taken from reads would hold the
    /// writes to a cycle they never take.
    pub(crate) fn service_clock(&self, write: bool) -> ServiceClock {
        ServiceClock {
            pace: self.clone(),
            taken: std::time::Instant::now(),
            write,
        }
    }
    /// A write is answered now: counted in its interval, and an interval
    /// that has run a bound is closed, its count taken into the windowed
    /// most ([`TickPeriod::write_window`]).
    fn delivered(&self) {
        self.delivered_at(now_ns());
    }
    fn delivered_at(&self, now: u64) {
        let Some(bound) = self.delay_bound() else {
            return;
        };
        let bound = u64::try_from(bound.as_nanos()).unwrap_or(u64::MAX);
        let began = self.0.interval_began_ns.load(Ordering::Relaxed);
        if began == 0 {
            self.0
                .interval_began_ns
                .store(now.max(1), Ordering::Relaxed);
            self.0.interval_answered.store(1, Ordering::Relaxed);
            return;
        }
        let answered = self
            .0
            .interval_answered
            .load(Ordering::Relaxed)
            .saturating_add(1);
        if now.saturating_sub(began) < bound {
            self.0.interval_answered.store(answered, Ordering::Relaxed);
            return;
        }
        // The interval ran its bound: what it answered is a sample, and this
        // answer begins the next.
        let most = self.0.answered_most.load(Ordering::Relaxed);
        let seen = self.0.answered_most_seen_ns.load(Ordering::Relaxed);
        let window = u64::try_from(BASE_WINDOW.as_nanos()).unwrap_or(u64::MAX);
        let sample = answered.saturating_sub(1);
        if sample >= most || now.saturating_sub(seen) > window {
            self.0.answered_most.store(sample, Ordering::Relaxed);
            self.0.answered_most_seen_ns.store(now, Ordering::Relaxed);
        }
        self.0
            .interval_began_ns
            .store(now.max(1), Ordering::Relaxed);
        self.0.interval_answered.store(1, Ordering::Relaxed);
    }
    /// The writes the session may hold in its pipeline, and in their queue
    /// for it: by Little's law what it answers within its bound — the most
    /// writes it answered in one bound ([`Self::delay_bound`]) over the
    /// window, and at least [`INITIAL_WINDOW`]. Taken in to that depth, a
    /// write waits for the pipeline at most a bound, and queued to it, at most
    /// a bound to be taken; past it, a write adds latency, not throughput
    /// (CoDel's bad queue, RFC 8289 §2). The bound's cycles are its window's
    /// gain, as BBR's cwnd_gain over its BDP: a pipeline that answers in one
    /// cycle what it took grows its window by them each cycle, so it grows
    /// with the batches a deeper pipeline makes until its cycle reaches the
    /// bound — where its answers in one least cycle would hold it at whatever
    /// it began with (the learned window of 2026-10-11 settled at two
    /// writes). None until the session has served a write.
    pub(crate) fn write_window(&self) -> Option<usize> {
        self.delay_bound()?;
        let most = self.0.answered_most.load(Ordering::Relaxed);
        Some(
            usize::try_from(most)
                .unwrap_or(usize::MAX)
                .max(INITIAL_WINDOW),
        )
    }
    /// The owner answered a write, `service` after taking it. Any write's
    /// service is at least the cycle of the batch it was proposed in, which is
    /// at least the session's uncongested cycle, so the least of every write's
    /// is that cycle from above: a sample at or under the least replaces it,
    /// and a longer one never does while the session leads in the term it
    /// measured it in ([`Self::reset_base`]). Taken from writes taken alone
    /// only, the least was what the first such write took and nothing after:
    /// under an overload that never empties the pipeline, a session whose
    /// first writes were slow kept a bound of 3 s for a whole run (A/B run
    /// 38287321502, 4,000 writes a second on 1-CPU nodes, client p50 370 ms
    /// where the other round's was 130). Only the owner thread answers, so
    /// these marks have one writer.
    pub(crate) fn served(&self, service: Duration) {
        let sample = u64::try_from(service.as_nanos()).unwrap_or(u64::MAX).max(1);
        let base = self.0.base_service_ns.load(Ordering::Relaxed);
        if base == 0 || sample <= base {
            self.0.base_service_ns.store(sample, Ordering::Relaxed);
        }
    }
    /// The session leads in a new term: what it measured of its cycle and its
    /// rate under the last is forgotten, and measured again. Within a term
    /// the least only falls: a write taken while a load runs waits on
    /// the work the load left (its followers still replaying the last batch,
    /// its owner's queue), so a least that expired into such samples, or
    /// that a probe re-measured under load (BBR's ProbeRTT), rose with the
    /// load (22 ms to 124 ms within seconds, and a probe's 91 ms, at 16,000
    /// writes a second
    /// on three local nodes, 2026-10-12), and its bound let the pipeline
    /// hold every request its clients had out. A leader's paths are what a
    /// term changes: a new leader has new followers to reach.
    pub(crate) fn reset_base(&self) {
        self.0.base_service_ns.store(0, Ordering::Relaxed);
        self.0.interval_began_ns.store(0, Ordering::Relaxed);
        self.0.interval_answered.store(0, Ordering::Relaxed);
        self.0.answered_most.store(0, Ordering::Relaxed);
        self.0.answered_most_seen_ns.store(0, Ordering::Relaxed);
    }
    /// The owner states when it took the oldest write it holds unanswered
    /// ([`ServiceClock::taken_ns`]), or none: its time in the pipeline, apart
    /// from what it waited in its queue, which that queue's delay judges.
    /// Judged from its admission, a write the queue had held past the bound
    /// closed the pipeline the moment it was taken, and the pipeline took
    /// one write a cycle (2026-10-12).
    pub(crate) fn set_oldest_write(&self, taken_ns: Option<u64>) {
        // Zero is none: a write taken as the process began is marked a nanosecond later.
        self.0
            .oldest_write_ns
            .store(taken_ns.map_or(0, |taken| taken.max(1)), Ordering::Relaxed);
    }
    /// Whether the owner may take another participant's write into its
    /// pipeline, which holds `in_flight` now: while it holds none, and
    /// otherwise while it holds fewer than its window ([`Self::write_window`])
    /// and its oldest was taken within the bound ([`Self::delay_bound`]). A
    /// write past the window, or behind one that has waited past the bound,
    /// waits in its queue, where its delay is judged, and the session's
    /// other work goes on.
    pub(crate) fn pipeline_open(&self, in_flight: usize) -> bool {
        self.pipeline_open_at(in_flight, now_ns())
    }
    fn pipeline_open_at(&self, in_flight: usize, now: u64) -> bool {
        let oldest = self.0.oldest_write_ns.load(Ordering::Relaxed);
        if oldest == 0 {
            return true;
        }
        let (Some(bound), Some(window)) = (self.delay_bound(), self.write_window()) else {
            return true;
        };
        in_flight < window && Duration::from_nanos(now.saturating_sub(oldest)) <= bound
    }
    /// The session's least write cycle uncongested; none until one is
    /// measured.
    pub(crate) fn base_service(&self) -> Option<Duration> {
        match self.0.base_service_ns.load(Ordering::Relaxed) {
            0 => None,
            ns => Some(Duration::from_nanos(ns)),
        }
    }
    /// The longest a write may wait, in its queue or in the pipeline, before
    /// the session judges it congested: [`DELAY_BOUND_CYCLES`] of its write
    /// cycle uncongested. None until the session has served a write: what it
    /// refuses is judged by what it measured, never by a number picked for
    /// it.
    pub(crate) fn delay_bound(&self) -> Option<Duration> {
        self.base_service()
            .map(|base| base.saturating_mul(DELAY_BOUND_CYCLES))
    }
    /// Forgets the session's measured service time, as one that has served nothing yet: a test
    /// of a queue's own room sets the delay bound aside with it.
    #[cfg(test)]
    pub(crate) fn forget_service(&self) {
        self.reset_base();
        self.0.writes.above_since_ns.store(0, Ordering::Relaxed);
        self.0.others.above_since_ns.store(0, Ordering::Relaxed);
    }
    /// How long a queue's delay must stay past its bound before it is a
    /// standing queue: [`STANDING_BOUNDS`] bounds. A burst that drains within
    /// it is a good queue and is refused nothing; one that stands is a bad
    /// queue. Of the bound, which only falls within a term, and not of the
    /// slowest uncongested write over a window: that rose with the load too
    /// (to 494 ms at 16,000 writes a second locally), and a queue stood that
    /// long before it was refused for. None until the session has served a
    /// write.
    pub(crate) fn standing_interval(&self) -> Option<Duration> {
        self.delay_bound()
            .map(|bound| bound.saturating_mul(STANDING_BOUNDS))
    }
    /// Whether a participant's request (a `write` or not) is to be refused now
    /// for the delay it would be queued behind: CoDel's test (Nichols and
    /// Jacobson, RFC 8289 §5.5; Facebook's server queues refuse so, Maurer,
    /// "Fail at Scale", ACM Queue 2015) on its kind's queue. The delay past
    /// its bound ([`Self::delay_bound`]) is a standing queue once it has
    /// stayed there a whole interval ([`Self::standing_interval`]) without a
    /// break; a delay that falls to the bound even once starts the interval
    /// again. Judged at once, a single write the disk was slow to flush
    /// refused what came behind it: at 100 writes a second on macOS, whose
    /// flush time varies several-fold, a quarter of all writes were refused
    /// with nothing queued (2026-10-12).
    pub(crate) fn delay_exceeded(&self, write: bool) -> bool {
        self.delay_exceeded_at(write, now_ns())
    }
    /// [`Self::delay_exceeded`] as of `now`, in nanoseconds since the process began. A write is
    /// refused once its queue holds its window: taken in at most a window a bound
    /// ([`Self::write_window`]), the next would wait past the bound to be taken — Little's law
    /// read forward, as no interval need pass to know it.
    fn delay_exceeded_at(&self, write: bool, now: u64) -> bool {
        if write {
            return self.write_window().is_some_and(|window| {
                usize::try_from(self.0.writes.queued.load(Ordering::Acquire)).unwrap_or(usize::MAX)
                    >= window
            });
        }
        let (Some(bound), Some(interval)) = (self.delay_bound(), self.standing_interval()) else {
            return false;
        };
        self.marks(write).standing_at(now, bound, interval)
    }
    /// Whether the owner drops a write it takes now after `waited` queued,
    /// without serving it: it waited past the bound. CoDel drops at the head
    /// (RFC 8289 §5), as Facebook's servers drop a request queued past its
    /// target ("Fail at Scale"): the door keeps the queue within its window,
    /// and this catches what the window let in before the rate fell, at the
    /// cost of a dequeue, where served it would cost a preparation its answer
    /// comes too late to be worth.
    pub(crate) fn head_drop(&self, waited: Duration) -> bool {
        self.delay_bound().is_some_and(|bound| waited > bound)
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
mod admission_tests {
    use super::*;

    fn ms(n: u64) -> Duration {
        Duration::from_millis(n)
    }
    fn ns(duration: Duration) -> u64 {
        u64::try_from(duration.as_nanos()).unwrap()
    }
    /// Waits until the process has run `at_least`: the marks these tests set in the past are
    /// times since it began.
    fn running_for(at_least: Duration) {
        let at_least = ns(at_least);
        loop {
            let now = now_ns();
            if now >= at_least {
                return;
            }
            std::thread::sleep(Duration::from_nanos(at_least - now));
        }
    }
    /// The cycle these tests measure: its bound, [`DELAY_BOUND_CYCLES`] of it, is 50 ms.
    const CYCLE: Duration = Duration::from_micros(12_500);
    /// A pace whose cycle is `base` and that answered at most `per_bound` writes in one bound.
    fn measured(base: Duration, per_bound: u64) -> TickPeriod {
        let pace = TickPeriod::default();
        pace.served(base);
        pace.0.answered_most.store(per_bound, Ordering::Relaxed);
        pace.0
            .answered_most_seen_ns
            .store(now_ns(), Ordering::Relaxed);
        pace
    }

    /// The uncongested cycle is the least a write took with no other write in its pipeline: a
    /// shorter sample replaces it at once and a longer one never does in the term, however long
    /// it stands; a new term forgets it, and nothing is bounded before one is measured.
    #[test]
    fn the_uncongested_cycle_only_falls_within_a_term() {
        let pace = TickPeriod::default();
        assert_eq!(pace.base_service(), None);
        assert_eq!(pace.delay_bound(), None);
        assert_eq!(pace.write_window(), None);
        assert!(
            !pace.delay_exceeded(true),
            "nothing is refused before a cycle is measured"
        );
        assert!(!pace.head_drop(ms(10_000)));
        assert!(pace.pipeline_open(1_000_000));
        pace.served(ms(9));
        pace.served(ms(4));
        pace.served(ms(7));
        assert_eq!(pace.base_service(), Some(ms(4)));
        assert_eq!(pace.delay_bound(), Some(ms(4) * DELAY_BOUND_CYCLES));
        // A load's longer cycles never raise it within the term.
        pace.served(ms(400));
        assert_eq!(pace.base_service(), Some(ms(4)));
        pace.reset_base();
        assert_eq!(pace.base_service(), None);
        pace.served(ms(6));
        assert_eq!(pace.base_service(), Some(ms(6)));
    }

    /// Every write answered measures the cycle, a write taken behind another as well: its
    /// service is at least its batch's cycle, and the least of all of them is the cycle from
    /// above. A read never measures it.
    #[test]
    fn every_write_measures_the_cycle_and_a_read_none() {
        let pace = TickPeriod::default();
        pace.service_clock(false).answered();
        assert_eq!(pace.base_service(), None, "a read measures nothing");
        let first = pace.service_clock(true);
        pace.set_oldest_write(Some(first.taken_ns()));
        let behind = pace.service_clock(true);
        behind.answered();
        let measured = pace
            .base_service()
            .expect("a write behind another measures the cycle");
        // Taken first and answered last, the first write took longer: the least stands.
        first.answered();
        assert_eq!(pace.base_service(), Some(measured), "the least only falls");
        // A slow first sample does not hold the bound: a quicker write lowers it.
        pace.reset_base();
        pace.served(ms(300));
        pace.served(ms(13));
        assert_eq!(pace.base_service(), Some(ms(13)));
    }

    /// The window is the most writes answered in one bound over the window, counted interval
    /// by interval as they are answered, and at least the initial window; a burst of answers
    /// counts as many as it answered within its bound, never as a rate it ran at.
    #[test]
    fn the_write_window_is_what_one_bound_answered() {
        running_for(ms(2_000));
        // 12.5 ms a cycle, bound 50 ms: 100 answered in a bound hold 100 in the pipeline.
        let pace = measured(CYCLE, 100);
        assert_eq!(pace.delay_bound(), Some(ms(50)));
        assert_eq!(pace.write_window(), Some(100));
        assert_eq!(measured(CYCLE, 3).write_window(), Some(INITIAL_WINDOW));
        // Counted: 150 answered at once in the first interval, 40 in the next two; the first
        // interval's count is taken once the next answer finds it past its bound.
        let pace = measured(CYCLE, 0);
        let began = now_ns() - ns(ms(1_000));
        for _ in 0..150 {
            pace.delivered_at(began);
        }
        assert_eq!(
            pace.write_window(),
            Some(INITIAL_WINDOW),
            "an interval not yet closed"
        );
        for at in 0..40 {
            pace.delivered_at(began + ns(ms(50)) + ns(ms(at)));
        }
        assert_eq!(pace.write_window(), Some(150));
        pace.delivered_at(began + ns(ms(150)));
        assert_eq!(
            pace.write_window(),
            Some(150),
            "a lower interval leaves the most"
        );
    }

    /// The pipeline takes a write while it holds none, and otherwise while it holds fewer than
    /// its window and its oldest was taken within the bound.
    #[test]
    fn the_pipeline_takes_writes_under_its_window_and_bound() {
        running_for(ms(200));
        let pace = measured(CYCLE, 100);
        assert!(pace.pipeline_open(500), "an empty pipeline takes one");
        let now = now_ns();
        pace.set_oldest_write(Some(now - ns(ms(10))));
        assert!(pace.pipeline_open_at(99, now));
        assert!(!pace.pipeline_open_at(100, now), "at its window");
        pace.set_oldest_write(Some(now - ns(ms(51))));
        assert!(!pace.pipeline_open_at(1, now), "its oldest past the bound");
        pace.set_oldest_write(None);
        assert!(pace.pipeline_open_at(100, now));
    }

    /// A write is refused at the door once its queue holds its window, and one taken after
    /// waiting past the bound is dropped at the head; the rest's queue is judged apart, so
    /// writes held for the pipeline never refuse a read.
    #[test]
    fn writes_are_refused_at_their_window_and_dropped_past_their_bound() {
        let pace = measured(CYCLE, 100);
        let mut queued: Vec<QueueTicket> = (0..99).map(|_| pace.queue_request(true)).collect();
        assert!(!pace.delay_exceeded(true));
        queued.push(pace.queue_request(true));
        assert!(
            pace.delay_exceeded(true),
            "its window queued: the next waits past the bound"
        );
        assert!(!pace.delay_exceeded(false), "the reads' queue is apart");
        assert_eq!(pace.queueing_delay(false), Duration::ZERO);
        drop(queued.pop());
        assert!(!pace.delay_exceeded(true));
        assert!(!pace.head_drop(ms(50)));
        assert!(pace.head_drop(ms(51)));
    }

    /// The rest's queue is CoDel's: a delay past its bound is a standing queue only once it
    /// has stayed past it a whole interval, two bounds; one that falls to the bound once starts
    /// the interval again, and one that stands is refused for until it falls.
    #[test]
    fn the_rests_delay_is_refused_for_only_once_it_stands_an_interval() {
        running_for(ms(2_000));
        let pace = measured(CYCLE, 0);
        assert_eq!(pace.delay_bound(), Some(ms(50)));
        assert_eq!(pace.standing_interval(), Some(ms(100)));
        let first = pace.queue_request(false);
        let busy = now_ns() - ns(ms(1_000));
        pace.0.others.busy_since_ns.store(busy, Ordering::Relaxed);
        let at = |after: u64| busy + ns(ms(after));
        assert!(
            !pace.delay_exceeded_at(false, at(51)),
            "first past its bound"
        );
        assert!(!pace.delay_exceeded_at(false, at(150)), "99 ms past it");
        assert!(
            pace.delay_exceeded_at(false, at(151)),
            "an interval past it"
        );
        assert!(
            pace.delay_exceeded_at(false, at(400)),
            "and while it stands"
        );
        // The owner takes it: the delay falls, and the interval starts again.
        drop(first);
        assert!(!pace.delay_exceeded_at(false, at(401)));
        let _second = pace.queue_request(false);
        pace.0
            .others
            .busy_since_ns
            .store(at(401), Ordering::Relaxed);
        assert!(
            !pace.delay_exceeded_at(false, at(452)),
            "first seen past its bound"
        );
        assert!(!pace.delay_exceeded_at(false, at(551)));
        assert!(pace.delay_exceeded_at(false, at(552)));
    }

    /// The queueing delay is none while nothing is queued; while the queue holds a request it
    /// is at least the time since the queue last stopped being empty or the owner last took
    /// one, whichever is later, and at least what the one taken last had waited.
    #[test]
    fn the_queueing_delay_is_the_oldest_wait_the_queue_marks_tell() {
        running_for(ms(40));
        let pace = measured(CYCLE, 0);
        assert_eq!(pace.queueing_delay(true), Duration::ZERO);
        let mut first = pace.queue_request(true);
        let second = pace.queue_request(true);
        // The queue has been busy for 30 ms with nothing taken.
        pace.0
            .writes
            .busy_since_ns
            .store(now_ns().saturating_sub(ns(ms(30))), Ordering::Relaxed);
        assert!(pace.queueing_delay(true) >= ms(30));
        // The owner takes the first, which had waited 30 ms: the one behind it waited at
        // least as long.
        first.admitted_ns = now_ns().saturating_sub(ns(ms(30)));
        assert!(first.taken() >= ms(30));
        assert!(pace.queueing_delay(true) >= ms(30));
        // Once the queue empties, nothing is queued and no delay is read.
        drop(second);
        assert_eq!(pace.queueing_delay(true), Duration::ZERO);
        // The next to find it empty starts a new spell: the last spell's sojourn says
        // nothing of it.
        let _third = pace.queue_request(true);
        assert!(pace.queueing_delay(true) < ms(30));
    }
}
