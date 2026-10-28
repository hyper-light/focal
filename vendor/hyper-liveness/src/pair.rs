//! One node pair: the stream this node sends the peer and the detector it runs on the peer's.

use std::time::Duration;

use hyper_timing::{
    Arrivals, Configuration, Costs, Event, ExchangeRtt, Exposure, LinkEstimator, Trust,
    arrival_detector_at, configure_arrivals, lateness_bound,
};

use crate::bound::Offset;
use crate::codec::{Echo, Heartbeat, MAX_BYTES};
use crate::{Change, Last, PairReport, PeerId, Refusal, Suspicion};

/// `duration` in nanoseconds, saturating at `u64::MAX` (584 years).
pub(crate) fn nanos(duration: Duration) -> u64 {
    u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX)
}

/// Format: RFC 8085 §3.1.3 (BCP 145), its second case, nanoseconds: an application that "cannot
/// maintain an RTT estimate for a destination, because the destination does not send return
/// traffic", "SHOULD NOT send more than one UDP datagram every 3 seconds". A stream to a peer from
/// which nothing has come is such a sender, and backs off to it (`Pair::backed_off`).
const UNANSWERED_NS: u64 = 3_000_000_000;

/// The longest gap a peer that has heard nothing from this node may leave after a heartbeat
/// stating `interval_ns` and `floor_ns`, nanoseconds: it backs off by twice the gap it stated, never
/// below its floor, to [`UNANSWERED_NS`] or its floor past it (`Pair::backed_off`). An upper bound
/// on that rule's next gap for any interval its floor and the ask give: exact while they hold, the
/// floor read where it rose.
fn unanswered_next(interval_ns: u64, floor_ns: u64) -> u64 {
    interval_ns
        .saturating_mul(2)
        .min(UNANSWERED_NS)
        .max(floor_ns)
        .max(interval_ns)
}

/// What the node gives every pair's sender at a poll.
pub(crate) struct Sender {
    pub(crate) local_run: u64,
    pub(crate) floor: Option<Duration>,
    pub(crate) granularity: Option<Duration>,
    pub(crate) durable_count: u64,
    pub(crate) durable_ns: Option<u64>,
    /// This node's run announced its end (`Liveness::stop`): a pair is sent the announcement again
    /// where it is due, and nothing else.
    pub(crate) stopped: bool,
    /// The node judges each peer's process apart from its disk (`Liveness::set_disk_apart`): a
    /// heartbeat due leaves whatever a flush is doing.
    pub(crate) apart: bool,
    /// In that mode, its own disk is slow (`Liveness::own_disk_slow`): its heartbeats say so.
    pub(crate) disk_slow: bool,
    /// When the oldest write on the node's log not yet durable was submitted, if one is
    /// (`Liveness::on_pending`): in the disk mode its heartbeats' flush age is that write's age,
    /// zero when none, a disk with nothing to do being no slow one (Focal's 5a).
    pub(crate) pending_ns: Option<u64>,
}

/// What a judge knows of its own durability (`docs/timing.md` §2.8, "A judge whose own proof
/// stalls"): its latest durable completion, and the latest stretch its stream waited for a proof it
/// lacked, from when it first wanted one to the completion that ended it, if one has.
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct Local {
    pub(crate) durable_ns: Option<u64>,
    pub(crate) stall: Option<(u64, Option<u64>)>,
}

/// When a freshness point that has passed is judged, given the judge's own durability.
pub(crate) enum Held {
    /// At this time: the point itself, or later where the judge's own proof stalled over it.
    At(u64),
    /// Not until the judge's own proof comes; `probe` when none is outstanding and nothing is durable
    /// since the point, so the judge's next completion, of any write, is what it waits for.
    Waiting { probe: bool },
}

/// The flush age a heartbeat sent at `sent_ns` carries: in the disk mode the age of the node's
/// oldest write not yet durable, zero when none (Focal's 5a); in today's, the age of its latest
/// durable completion, `durable_ns`, which the flush proof is read from.
fn flush_age(sender: &Sender, sent_ns: u64, durable_ns: u64) -> u64 {
    if sender.apart {
        sender
            .pending_ns
            .map_or(0, |since| sent_ns.saturating_sub(since))
    } else {
        sent_ns.saturating_sub(durable_ns)
    }
}

impl Local {
    /// What a node that judges its peers apart from its disk judges with (`Liveness::set_disk_apart`):
    /// nothing of its own holds a point, and it waits for no completion of its own.
    pub(crate) fn apart() -> Self {
        Self {
            durable_ns: Some(u64::MAX),
            stall: None,
        }
    }

    /// When a point that has passed, a peer's expected arrival plus `margin_ns`, is judged. A judge
    /// whose own proof is outstanding waits for it: its peers' proofs may be stalled with it, as on
    /// a volume they share, and a judge whose own stream stopped judges by a proof it cannot
    /// produce. Once it comes, a stall at least as long as the margin it holds the peer to gives the
    /// peer that margin again from its end, since the peer's stream resumes no sooner; a shorter one
    /// proves nothing past its end. A judge with nothing durable since the point, and no proof
    /// outstanding, waits for its next completion and judges on it: what it judges by is its own
    /// device's progress since the peer was due. Lifeguard's local health (Dadgar, Phillips and
    /// Currey 2018) applied to the flush proof.
    pub(crate) fn judge(&self, point: u64, margin_ns: u64) -> Held {
        let expected = point.saturating_sub(margin_ns);
        match self.stall {
            Some((_, None)) => return Held::Waiting { probe: false },
            Some((since, Some(end)))
                if end >= expected && end.saturating_sub(since) >= margin_ns =>
            {
                return Held::At(point.max(end.saturating_add(margin_ns)));
            }
            // A shorter wait proves nothing past its end: an ordinary flush that ended after the
            // peer's expected arrival but before its point, the journal stalling after it, held a
            // judge to the point it then could not judge by (macOS seed 254 of 256 of
            // `flushes_stalled_together_...`).
            _ => {}
        }
        // Its device proved itself since the point: the peer's silence is not one it shares. A
        // completion after the expected arrival but before the point does not prove it, since a
        // shared journal can stall after it with the peer's flush, asked earlier, still in it
        // (`flushes_stalled_together_...`, macOS seed 254 of 256).
        if self.durable_ns.is_some_and(|durable| durable >= point) {
            Held::At(point)
        } else {
            Held::Waiting { probe: true }
        }
    }
}

/// What the node gives a pair's receiver with a heartbeat.
pub(crate) struct Context<'a> {
    pub(crate) granularity: Option<Duration>,
    /// The node's failure evidence, whose MTBF is read only where a configuration needs it: a
    /// float division and a conversion, at every heartbeat it was read for none.
    pub(crate) exposure: &'a Exposure,
    /// What the node measured of its links (`Liveness::renew_evidence`), read only while this pair
    /// has no configuration of its own (`docs/timing.md` §3, item 10).
    pub(crate) evidence: Option<&'a Arrivals>,
    /// The node's own durability, which a point that passed is judged with.
    pub(crate) local: Local,
    /// The node judges each peer's process apart from its disk (`Liveness::set_disk_apart`): a
    /// heartbeat is taken whether or not a flush proves it.
    pub(crate) apart: bool,
}

/// What a heartbeat taken did, for the node.
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct Taken {
    /// It began a new run of the peer: the peer restarted.
    pub(crate) restarted: bool,
    /// Its lateness past the expected arrival its predecessor's freshness point was set from,
    /// nanoseconds: what the node's pool is fed, while the pool or this pair needs it.
    pub(crate) lateness: Option<i64>,
    /// Whether the pair judges by a configuration of its own.
    pub(crate) own: bool,
    /// Whether it configured the pair's detector anew.
    pub(crate) configured: bool,
    /// The run before the one it began had announced its stop ([`Heartbeat::stopping`]): the
    /// restart is no failure.
    pub(crate) after_stop: bool,
    /// It was the peer's announcement that its run stops, not a heartbeat taken.
    pub(crate) stopping: bool,
}

/// A peer's disk as its heartbeats say it is, judged apart from its process (kiln's CP-T2.2): slow
/// while they carry its own verdict (`Heartbeat::disk_slow`), from its own measured flushes, and
/// caught up once one doesn't.
#[derive(Debug, Default)]
struct Disk {
    slow: bool,
}

impl Disk {
    /// The verdict `beat` moves the peer's disk to, if it moves it: `Some(true)` slow, `Some(false)`
    /// caught up.
    fn judge(&mut self, beat: &Heartbeat) -> Option<bool> {
        (beat.disk_slow != self.slow).then(|| {
            self.slow = beat.disk_slow;
            self.slow
        })
    }
}

/// The wider of two behaviours, each measure the larger: an upper bound on both. The bound on a
/// lateness past a margin, `u + (1 − u)·V/(V + (α − μ)²)`, grows with the unseen share `u`, with
/// the variance `V` and with the mean `μ` at every margin (its derivatives are
/// `1 − V/(V + x²)`, `(1 − u)x²/(V + x²)²` and `(1 − u)·2Vx/(V + x²)²` in them, `x = α − μ`, none
/// negative), so a margin configured from the wider promises no less than one from either would.
pub(crate) fn wider(a: Option<Arrivals>, b: Option<Arrivals>) -> Option<Arrivals> {
    match (a, b) {
        (Some(a), Some(b)) => Some(Arrivals {
            unseen: a.unseen.max(b.unseen),
            lateness: a.lateness.max(b.lateness),
            deviation: a.deviation.max(b.deviation),
            mean_delay: a.mean_delay.max(b.mean_delay),
        }),
        (one, None) | (None, one) => one,
    }
}

/// The arrivals the node measured of its links, for a link whose expected arrival is averaged over
/// a window of `window` heartbeats: the deviation scaled by `√(1 + 1/n)`. A lateness is measured
/// against the expected arrival, whose error for independent delays adds `V(D)/n` to the delay's
/// own `V(D)`, and the latenesses the node measured, the pool's or a configured link's, carry their
/// own windows' error on top of `V(D)`, so their variance is at least `V(D)`: the scaled deviation
/// bounds the link's from above, which is the side Cantelli's inequality may err on (a larger
/// variance only loosens the bound).
pub(crate) fn scaled(pool: &Arrivals, window: u64) -> Arrivals {
    // u64 → f64 rounds only past 2⁵³, far past any window (`hyper_timing::WINDOW_LIMIT`).
    let n = window.max(1) as f64;
    Arrivals {
        deviation: pool.deviation.mul_f64((1.0 + 1.0 / n).sqrt()),
        ..*pool
    }
}

/// What a poll's send did.
pub(crate) enum Sent {
    /// A heartbeat of this many bytes is in the node's buffer, and its lateness past the slot after
    /// the previous heartbeat sent the peer, nanoseconds: what the peer will take it as, but the
    /// path's delay (`Stream::expected_ns`); `None` for the first.
    Message(usize, Option<i64>),
    /// A heartbeat is due and no flush proves it.
    NeedsFlush,
    /// Nothing due.
    Nothing,
}

/// This node's stream to the peer.
#[derive(Debug, Default)]
struct Stream {
    /// The next heartbeat's number and when it is due; `None` before the first.
    next: Option<(u64, u64)>,
    /// The interval the next heartbeat is due at, after the one before it; zero before the first.
    interval_ns: u64,
    /// The interval the floor and the peer's ask give it (`Pair::interval`): the one it sends at
    /// once the peer is heard, and the one it backs off from while nothing has come
    /// (`Pair::backed_off`); zero before the first.
    base_ns: u64,
    /// The heartbeats sent while nothing has come from the peer: the doublings its interval is
    /// backed off by, none once it is heard.
    unanswered: u32,
    /// The interval the peer asked for.
    asked_ns: u64,
    /// The durable count the latest heartbeat carried: the next must carry more.
    proof: u64,
    /// The flushes `E[flush]` was the mean of when the floor last gave the interval: the floor
    /// renews it, down as well as up, once they have doubled (`Pair::interval`).
    floor_from: u64,
    /// The latest heartbeat's number sent.
    sent: Option<u64>,
    /// When the latest heartbeat sent went, on this node's clock: this run's announcement goes after
    /// it, so that an echo of the announcement names it alone.
    sent_ns: u64,
    /// The slot after the latest heartbeat sent, when it was due: the arrival the peer expects of
    /// the next, but the path's delay. A heartbeat sent past it, a skipped slot's or its own
    /// lateness, is late by as much at the peer (`docs/timing.md` §2.2).
    expected_ns: Option<u64>,
}

impl Stream {
    /// The peer was heard for the first time, at `now_ns`: a stream backed off while it said nothing
    /// sends its next heartbeat at once, and at the interval the floor and the ask give from then
    /// on, so the peer, which judges this node from its own start, waits for no backed-off slot.
    /// That heartbeat starts the schedule anew: it states the interval the stream sends at from
    /// then on, and a send late past it, waiting on its flush, is late by as much, never slots
    /// skipped. Stated as its spacing from the one before, a peer heard just after a heartbeat went
    /// made the slot that short: heard 10 µs past it, an answer proven 500 µs later skipped 50 slots
    /// (`a_stream_never_heard_backs_off_to_three_seconds_and_answers_its_peer_at_once`).
    fn heard(&mut self, now_ns: u64) {
        if std::mem::take(&mut self.unanswered) == 0 {
            return;
        }
        if let Some((seq, due)) = self.next
            && due > now_ns
        {
            self.next = Some((seq, now_ns));
            self.interval_ns = self.base_ns.max(1);
            self.expected_ns = Some(now_ns);
        }
    }

    /// The stream sends its next number at once, at `now_ns`, the schedule starting anew there:
    /// the peer started its next run after announcing the end of the last, and was sent nothing
    /// since, where the slots of the stop would have been skipped, each the next heartbeat's
    /// lateness at a peer that judges this node from its own start; or the peer's announcement is
    /// to be answered (`Pair::take_ended`).
    fn resume(&mut self, now_ns: u64) {
        if self.next.is_some() {
            let seq = self.sent.map_or(0, |sent| sent.saturating_add(1));
            self.next = Some((seq, now_ns));
            self.expected_ns = Some(now_ns);
        }
    }
}

/// How a peer from which no heartbeat has come is judged: from when the node first attached the
/// pair, one interval and the pool's margin at it (`docs/timing.md` §3, item 10).
#[derive(Clone, Copy, Debug, Default)]
struct Unheard {
    /// When the node first polled with the pair attached.
    since_ns: Option<u64>,
    /// The freshness point of the first heartbeat, once the pool can give a margin.
    until_ns: Option<u64>,
    /// The margin of that point past the first heartbeat's expected arrival.
    margin_ns: u64,
    /// The bound the node's evidence put on the first heartbeat's coming past it: what the
    /// allowance is charged when it comes.
    bound: Option<f64>,
    suspected: bool,
}

/// The estimator of the peer's stream, boxed: its Allan levels are most of a kilobyte, and the
/// pair's other fields are read every poll.
#[derive(Debug)]
struct Link {
    estimator: LinkEstimator,
    granularity_ns: u64,
}

/// What this node holds of the peer's stream. The link's history outlives the peer's runs: the
/// delays it measures are the hosts' and the path's (`docs/timing.md` §2.6), so a restarted peer is
/// judged at once by the detector in force, its heartbeats numbered on from the last run's
/// (`base`).
#[derive(Debug, Default)]
struct Received {
    /// The latest run of the peer taken: heartbeats of earlier runs are refused.
    run: Option<u64>,
    link: Option<Box<Link>>,
    /// What the run's numbers are offset by in the estimator: one past the last run's latest.
    base: u64,
    /// The durable count of the latest heartbeat taken from the run.
    flushes: u64,
    /// The peer's stability floor, as it last said.
    floor_ns: u64,
    last: Option<Last>,
    /// The estimator's number of the latest heartbeat taken.
    last_mapped: Option<u64>,
    /// The next heartbeat begins a new run's schedule: the estimator is anchored anew.
    reanchor: bool,
    /// The latest heartbeat to echo back: its send and lateness on the peer's clock, and its
    /// arrival on this node's.
    echo: Option<(u64, u64, u64)>,
    /// The clocks' offset, bounded by the echoes the run's heartbeats carried (the `bound`
    /// module): what a suspicion states its bound from.
    offset: Offset,
    configuration: Option<Configuration>,
    /// The interval the link was at when the configuration was made: a link that moved since is
    /// configured again at its new interval.
    configured_interval: Option<Duration>,
    round_trip: ExchangeRtt,
    /// While the pair has no configuration of its own: the arrivals its margin was imposed from,
    /// the node's evidence scaled to the link's window and widened by the link's own, the
    /// heartbeats taken then, and whether that evidence was measured: one that was not is renewed
    /// as soon as the node's is.
    pooled: Option<(Arrivals, u64, bool)>,
    /// The interval asked of the peer for the margin of the node's evidence, nanoseconds, where at
    /// the link's own interval that margin promised nothing (`pool_margin`); zero otherwise, and
    /// from the link's first configuration on.
    pooled_ask_ns: u64,
    /// The longest gap the peer may leave before its next heartbeat when its latest echoed nothing,
    /// nanoseconds ([`unanswered_next`]); zero once it echoes this node.
    unanswered_next_ns: u64,
    /// The run taken last announced its stop ([`Heartbeat::stopping`]): it is suspected, judged
    /// by no freshness point and sent nothing but the answer to its announcement, a heartbeat of it
    /// after the announcement is refused, and its next run's start is no failure.
    stopped: bool,
    /// The announcement taken last is owed its answer: the next heartbeat sent the stopped run,
    /// which echoes it.
    answer: bool,
    /// The latest heartbeat of the peer spoke the other disk mode ([`Refusal::OtherMode`]): the
    /// pair judges it by no freshness point until one of this node's mode comes.
    other_mode: bool,
}

/// This node's run announced its end to the peer ([`Pair::goodbye`]): when the first announcement
/// went, and whether the peer took one, as a heartbeat of the peer's that echoes one says.
#[derive(Clone, Copy, Debug)]
struct Announced {
    first_ns: u64,
    taken: bool,
}

/// What a pair counts: the counters of its [`PairReport`], the rest of which is read from the
/// pair when a report is made. Kept whole, the report carried two intervals and three flags a
/// poll's walk of the pairs moved through and nothing read (`docs/benchmarks.md`, "The node's
/// evidence, kept").
#[derive(Clone, Copy, Debug, Default)]
struct Counts {
    sent: u64,
    skipped: u64,
    taken: u64,
    unproven: u64,
    other_mode: u64,
    configurations: u64,
    suspicions: u64,
    allowance: f64,
}

/// One pair.
#[derive(Debug)]
pub(crate) struct Pair {
    pub(crate) groups: u32,
    pub(crate) election: Option<Duration>,
    stream: Stream,
    received: Received,
    unheard: Unheard,
    /// Whether the owner was last told the peer is suspected: a change is reported only where
    /// what the owner was told differs, and the owner trusts a peer until told otherwise.
    told: bool,
    /// A passed freshness point the node's own stall held later (`Local::judge`), and the time it
    /// is judged at: once held, it is held no further, whatever the node's own durability does
    /// after. Restated at every wait, a judge whose every write was slower than the margin held its
    /// point past each next one and suspected a dead peer never
    /// (`a_judge_whose_writes_are_all_slow_still_suspects_a_dead_peer`). One for a point other
    /// than the pair's present one is spent.
    held: Option<(u64, u64)>,
    counts: Counts,
    /// This node's run announced its end to the peer, if it did.
    announced: Option<Announced>,
    /// In the disk mode, the peer's disk as its heartbeats' flush ages show it.
    disk: Disk,
}

impl Pair {
    pub(crate) fn new() -> Self {
        Self {
            groups: 0,
            election: None,
            stream: Stream::default(),
            received: Received::default(),
            unheard: Unheard::default(),
            told: false,
            held: None,
            counts: Counts::default(),
            announced: None,
            disk: Disk::default(),
        }
    }

    pub(crate) fn trust(&self) -> Trust {
        if self.received.stopped {
            return Trust::Suspected;
        }
        match self.received.link.as_ref() {
            Some(link) => link.estimator.trust(),
            None if self.unheard.suspected => Trust::Suspected,
            None => match self.unheard.until_ns {
                Some(until_ns) => Trust::Trusted { until_ns },
                None => Trust::Unconfigured,
            },
        }
    }

    /// The node polled with the pair attached at `now_ns`: a peer from which nothing has come is
    /// judged from the first such poll.
    pub(crate) fn attached(&mut self, now_ns: u64) {
        self.unheard.since_ns.get_or_insert(now_ns);
    }

    /// Whether the pair has heard its peer and judges it by no margin yet: the node's evidence's is
    /// imposed at the next poll, so a link whose peer stopped before its own evidence is judged all the
    /// same (`docs/timing.md` §3, item 10).
    pub(crate) fn wants_pool_margin(&self) -> bool {
        self.received.configuration.is_none()
            && self.received.pooled.is_none()
            && self.received.link.is_some()
    }

    /// Whether the pair waits for the node's evidence's margin for a peer it has not heard from.
    pub(crate) fn wants_unheard_margin(&self) -> bool {
        self.received.link.is_none() && self.unheard.until_ns.is_none()
    }

    /// The freshness point of a peer from which no heartbeat has come: one interval past the first
    /// poll with the pair attached, at this node's own floor (the interval the peer starts at
    /// is its floor, and the pool's premise is that the stalls are the hosts', so a host's floor is
    /// the measure of its peers' before they say theirs), plus the margin the node's evidence gives
    /// at it for a window of one.
    pub(crate) fn judge_unheard(
        &mut self,
        pool: &Arrivals,
        floor: Duration,
        granularity: Duration,
        mtbf: Option<Duration>,
    ) {
        let (Some(since), Some(election), Some(mtbf)) =
            (self.unheard.since_ns, self.election, mtbf)
        else {
            return;
        };
        let interval = floor.max(granularity);
        let costs = Costs { election, mtbf };
        let behaviour = scaled(pool, 1);
        if let Some(detector) = arrival_detector_at(&behaviour, &costs, granularity, interval) {
            self.unheard.until_ns = Some(
                since
                    .saturating_add(nanos(interval))
                    .saturating_add(nanos(detector.margin)),
            );
            self.unheard.margin_ns = nanos(detector.margin);
            self.unheard.bound = Some(lateness_bound(&behaviour, detector.margin));
        }
    }

    /// The interval this node's heartbeats to the peer are due at, once the first is scheduled.
    pub(crate) fn sending_interval(&self) -> Option<Duration> {
        (self.stream.interval_ns != 0).then(|| Duration::from_nanos(self.stream.interval_ns))
    }

    /// The interval the peer's heartbeats come at, once one has come.
    pub(crate) fn heard_interval(&self) -> Option<Duration> {
        self.received
            .link
            .as_ref()
            .map(|link| link.estimator.interval())
    }

    pub(crate) fn configuration(&self) -> Option<Configuration> {
        self.received.configuration
    }

    pub(crate) fn round_trip(&self) -> &ExchangeRtt {
        &self.received.round_trip
    }

    pub(crate) fn report(&self) -> PairReport {
        let counts = self.counts;
        PairReport {
            groups: self.groups,
            sent: counts.sent,
            skipped: counts.skipped,
            taken: counts.taken,
            unproven: counts.unproven,
            other_mode: counts.other_mode,
            configured: self.received.configuration.is_some(),
            judged: !matches!(self.trust(), Trust::Unconfigured),
            interval: self
                .received
                .link
                .as_ref()
                .map(|link| link.estimator.next_interval()),
            freshness: self.received.link.as_ref().and_then(|link| {
                Some(
                    link.estimator
                        .next_interval()
                        .saturating_add(link.estimator.margin()?),
                )
            }),
            sending: (self.stream.interval_ns != 0)
                .then(|| Duration::from_nanos(self.stream.interval_ns)),
            sending_due_ns: self.next_due(),
            configurations: counts.configurations,
            suspicions: counts.suspicions,
            allowance: counts.allowance,
        }
    }

    /// When the next heartbeat to the peer is due.
    pub(crate) fn next_due(&self) -> Option<u64> {
        if self.stop_taken() || (self.received.stopped && !self.received.answer) {
            return None;
        }
        self.stream.next.map(|(_, due)| due)
    }

    /// Whether the peer's run taken last announced its stop.
    pub(crate) fn stopped(&self) -> bool {
        self.received.stopped
    }

    /// Whether the peer took this node's run's announcement that it ends.
    pub(crate) fn stop_taken(&self) -> bool {
        self.announced.is_some_and(|announced| announced.taken)
    }

    /// Whether this node's announcement is due to go again: it went, the peer has not answered,
    /// and a stream's interval has passed since.
    pub(crate) fn goodbye_due(&self, now_ns: u64) -> bool {
        self.groups > 0
            && self.announced.is_some_and(|announced| !announced.taken)
            && self.stream.next.is_some_and(|(_, due)| due <= now_ns)
    }

    /// The peer's freshness point, while trusted: read at every poll of every pair.
    fn point(&self) -> Option<u64> {
        match self.received.link.as_ref() {
            Some(link) => link.estimator.deadline(),
            None => self.unheard.until_ns.filter(|_| !self.unheard.suspected),
        }
    }

    /// The margin of the freshness point past the peer's expected arrival: read only for a point
    /// that has passed, since it is a conversion through a `Duration` every pair's poll paid for
    /// nothing (`docs/benchmarks.md`).
    fn margin_ns(&self) -> u64 {
        match self.received.link.as_ref() {
            Some(link) => link.estimator.margin().map_or(0, nanos),
            None => self.unheard.margin_ns,
        }
    }

    /// How a passed `point` is judged: at the time it was held to, if it was, or as the node's own
    /// durability holds it now.
    fn hold(&self, point: u64, local: &Local) -> Held {
        match self.held {
            Some((held, at)) if held == point => Held::At(at),
            _ => local.judge(point, self.margin_ns()),
        }
    }

    /// When the peer is next judged, while trusted, at `now_ns` and with the node's own
    /// durability: its freshness point, or for one that has passed the time `Local::judge` holds
    /// it to; none while that waits for the node's own proof, whose completion polls the stream.
    /// Read without building the trust, since every wake asked reads it of every pair.
    pub(crate) fn deadline(&self, local: &Local, now_ns: u64) -> Option<u64> {
        // Judged by no point: a run that announced its stop.
        if self.received.stopped {
            return None;
        }
        let point = self.point()?;
        if point > now_ns {
            return Some(point);
        }
        match self.hold(point, local) {
            Held::At(at) => Some(at),
            Held::Waiting { .. } => None,
        }
    }

    /// The trust the owner is shown at `now_ns`: [`trust`](Self::trust), with a point that has
    /// passed restated to the later time the node's own durability holds it to (`Local::judge`).
    /// While that waits for the node's own proof, the pair shows its own trust, to the point that
    /// passed, as before the rule: shown judged by no point (`Trust::Unconfigured`, a pair's state
    /// before its first margin), every judged link read as unjudged to an owner through a start
    /// whose first flushes took seconds (mantle's real processes on windows-x86_64 at `b96bed9`).
    pub(crate) fn trust_at(&self, local: &Local, now_ns: u64) -> Trust {
        match (self.trust(), self.point()) {
            (Trust::Trusted { until_ns }, Some(point)) if until_ns <= now_ns => {
                match self.hold(point, local) {
                    Held::At(at) => Trust::Trusted { until_ns: at },
                    Held::Waiting { .. } => Trust::Trusted { until_ns },
                }
            }
            (trust, _) => trust,
        }
    }

    /// The interval to send at, and whether the floor gave it: the one the peer asked, where its
    /// floor allows it; where the floor binds (before the peer asks, or past what it asked), the
    /// floor. A sender must keep its interval above its floor to be stable (Lindley 1952), so a
    /// floor that rose past the interval moves it at once. A floor that fell moves it once the
    /// flushes it is the mean of have doubled since the floor last gave the interval, when the
    /// interval is as stale as the mean is uncertain (`docs/research/timing.md`, "When to renew an
    /// estimate's configuration"). Followed down at every sample, a mean moving with its samples
    /// started the peer's estimator again at each move (`LinkEstimator::retime`), and a link at its
    /// floor went unconfigured for as long as its flushes kept moving it (`docs/timing.md` §2.9);
    /// never followed down, the interval kept the first flushes of a new log, seconds on a Windows
    /// runner whose floor then fell to 33 to 118 ms, and the links' evidence came at it for
    /// minutes (§2.8). The peer asks from what this node's floor was, so once it asks past it the
    /// interval is the peer's again. A change smaller than `G`, the configurator's resolution, is
    /// none.
    fn interval(&self, sender: &Sender) -> Option<(u64, u64, bool)> {
        let floor = nanos(sender.floor?);
        let asked = self.stream.asked_ns;
        let current = self.stream.base_ns;
        let renewed = sender.durable_count >= self.stream.floor_from.saturating_mul(2);
        let (wanted, from_floor) = if asked >= floor {
            (asked, false)
        } else if renewed || floor > current {
            (floor, true)
        } else {
            (current, false)
        };
        let resolution = sender.granularity.map_or(0, nanos);
        let base = if current != 0 && wanted.abs_diff(current) <= resolution {
            current
        } else {
            wanted
        };
        Some((self.backed_off(base), base, from_floor))
    }

    /// The interval to send at to a peer from which nothing has come: `base` at the first
    /// heartbeat, then twice the gap the heartbeat about to go states, the back-off of a sender
    /// whose datagrams go unanswered (RFC 8085 §3.1.3, its first case: "a predetermined
    /// transmission interval that is exponentially backed off"; RFC 6298 §5.5's doubling), up to
    /// 3 s, the most such a sender may send at (that section's second case, [`UNANSWERED_NS`]), and
    /// never below `base`, which a floor past 3 s keeps (Lindley 1952). Once the peer is heard,
    /// `base`. Doubled from the stated gap, not from `base`, the rule is one the peer reads off each
    /// heartbeat ([`unanswered_next`]): a peer that hears this node while this node hears it not
    /// (its datagrams lost, or sent where this node no longer listens) expects each doubling, where
    /// a gap it could not foresee came a whole gap past its freshness point and was suspected, at
    /// every doubling, of a node alive and heard (`a_restarted_node_that_hears_no_peer_is_not_suspected_for_its_back_off`). At the floor alone, a node whose peers
    /// were never heard, started before them or alone, or restarted before it learned where they
    /// listen, flushed its log for each heartbeat for as long as they said nothing: ten thousand
    /// times a second at an idle NVMe's floor of 80 to 115 µs (found by mantle).
    fn backed_off(&self, base: u64) -> u64 {
        if self.received.run.is_some() {
            return base;
        }
        // Twice the gap the heartbeat about to go states (`unanswered_next`, which the peer reads
        // off it), from `base` at the first.
        let doubled = if self.stream.unanswered == 0 {
            base
        } else {
            self.stream.interval_ns.saturating_mul(2).max(base)
        };
        doubled.min(UNANSWERED_NS.max(base))
    }

    /// Sends the heartbeat due, if a flush proves it: one made durable after the previous
    /// heartbeat was due, and newer than the one the previous heartbeat carried; or, where the node
    /// judges each peer's process apart from its disk, whatever a flush is doing, carrying the age
    /// of its oldest write not yet durable, zero when none (`Liveness::set_disk_apart`,
    /// `Liveness::on_pending`), and asking no flush of its own for it (Focal's 5a). A sender behind
    /// its schedule sends the latest heartbeat due; the ones it skipped are never sent, and the
    /// peer takes them as that heartbeat's lateness, the stall's delay (`docs/timing.md` §2.2).
    pub(crate) fn send(&mut self, sender: &Sender, now_ns: u64, out: &mut [u8; MAX_BYTES]) -> Sent {
        // A peer whose run announced its stop is sent nothing until its next run is heard, but the
        // one heartbeat that answers the announcement: no one else listens, and each heartbeat
        // would ask a flush of its own.
        if self.groups == 0 || (self.received.stopped && !self.received.answer) {
            return Sent::Nothing;
        }
        // Not due: every poll asks every pair, and most have nothing due. A heartbeat is scheduled
        // only once a floor is measured, and a measured floor stays measured, so nothing below
        // could be asked of a pair with one scheduled and not yet due.
        if self.stream.next.is_some_and(|(_, due)| due > now_ns) {
            return Sent::Nothing;
        }
        let Some((interval, base, from_floor)) = self.interval(sender) else {
            // No flush measured yet: the first proves the first heartbeat and gives the floor.
            return Sent::NeedsFlush;
        };
        let (seq, due) = match self.stream.next {
            None => (0, now_ns),
            Some((seq, due)) if due <= now_ns => {
                let step = self.stream.interval_ns.max(1);
                let behind = now_ns.saturating_sub(due).checked_div(step).unwrap_or(0);
                (
                    seq.saturating_add(behind),
                    due.saturating_add(behind.saturating_mul(step)),
                )
            }
            Some(_) => return Sent::Nothing,
        };
        // A schedule keeps the spacing it was given; a stream that has none yet takes the interval
        // it has now, and never nothing. Its first floor can be nothing: a flush answered at the
        // instant it was asked (a simulated disk's cut, a clock that did not move), its timer not
        // yet measured. Kept at nothing, every heartbeat came due at the instant it was asked, no
        // flush fell after the one before, and the stream proved none, sent none and asked no wake,
        // so its timer was never measured either: a restarted node its peers suspected for good
        // (Quiver's simulation, kiln's CP-T2.2 step 6). One nanosecond and no more: a flush proves
        // a heartbeat only if it falls strictly after the one before, and strictly after takes one
        // positive step of the clock; a wider floor would only hold the first heartbeats back.
        let spacing = if self.stream.next.is_none() || self.stream.interval_ns == 0 {
            interval.max(1)
        } else {
            self.stream.interval_ns
        };
        let previous_due = due.saturating_sub(spacing);
        let proven = sender.apart
            || (sender.durable_count > self.stream.proof
                && sender
                    .durable_ns
                    .is_some_and(|at| at > previous_due && at <= now_ns));
        let Some(durable_ns) = sender.durable_ns.filter(|_| proven) else {
            self.stream.next = Some((seq, due));
            if self.stream.interval_ns == 0 {
                self.stream.interval_ns = spacing;
                self.stream.base_ns = base;
            }
            return Sent::NeedsFlush;
        };
        // A heartbeat goes out after the one it echoes came. An owner that reads its clock before
        // its socket, so that every datagram stamped before its poll is read, can take one stamped
        // after the `now` it then sends at. Sent at that `now`, the echo's hold was negative,
        // clamped to zero, and the peer's lower bound on the clocks' offset passed what any clock
        // allows by as much, so its suspicions stated bounds short of the time they measured
        // (`bound`; 51.6 to 245 µs on Linux CI).
        let sent_ns = self
            .received
            .echo
            .map_or(now_ns, |(_, _, arrival_ns)| now_ns.max(arrival_ns));
        let echo = self
            .received
            .echo
            .map(|(echoed_ns, late_ns, arrival_ns)| Echo {
                sent_ns: echoed_ns,
                late_ns,
                hold_ns: sent_ns.saturating_sub(arrival_ns),
            });
        let ask_ns = self
            .received
            .configuration
            .map_or(0, |configured| nanos(configured.best.interval))
            .max(self.received.pooled_ask_ns);
        if let Some(link) = self.received.link.as_mut() {
            // The peer moves to what this heartbeat asks, never below its floor, from its next
            // heartbeat on: this node expects it so, not suspecting it for the move; and a peer
            // that has heard nothing from this node backs off, which it is expected to as well.
            link.estimator.expect_interval(Duration::from_nanos(
                ask_ns
                    .max(self.received.floor_ns)
                    .max(self.received.unanswered_next_ns),
            ));
        }
        let beat = Heartbeat {
            run: sender.local_run,
            seq,
            interval_ns: spacing,
            floor_ns: sender.floor.map_or(0, nanos),
            ask_ns,
            sent_ns,
            late_ns: sent_ns.saturating_sub(due),
            flushes: sender.durable_count,
            flush_age_ns: flush_age(sender, sent_ns, durable_ns),
            apart: sender.apart,
            disk_slow: sender.apart && sender.disk_slow,
            echo,
            stopping: false,
        };
        let length = beat.encode(out).len();
        let own = self
            .stream
            .expected_ns
            .map(|expected| i64::try_from(sent_ns.saturating_sub(expected)).unwrap_or(i64::MAX));
        // The slots between the latest heartbeat sent and this one were due while this node was
        // behind and are never sent: the peer takes them as this one's lateness.
        let skipped = self
            .stream
            .sent
            .map_or(0, |previous| seq.saturating_sub(previous).saturating_sub(1));
        self.counts.skipped = self.counts.skipped.saturating_add(skipped);
        self.stream.sent = Some(seq);
        self.stream.sent_ns = sent_ns;
        self.received.answer = false;
        self.stream.proof = sender.durable_count;
        // Never nothing, which says no heartbeat was ever sent (`Pair::report`'s `sending`): at
        // least one nanosecond, the one positive step of the clock that strictly after needs.
        self.stream.interval_ns = interval.max(1);
        self.stream.base_ns = base;
        if self.received.run.is_none() {
            self.stream.unanswered = self.stream.unanswered.saturating_add(1);
        }
        if from_floor {
            self.stream.floor_from = sender.durable_count;
        }
        self.stream.next = Some((seq.saturating_add(1), due.saturating_add(interval)));
        self.stream.expected_ns = Some(due.saturating_add(interval));
        self.counts.sent = self.counts.sent.saturating_add(1);
        // Judged apart from its disk the heartbeat left whatever this node's flushes are doing. A
        // probe is asked where nothing became durable in this heartbeat's slot, never holding one:
        // the node's own wait on its disk is measured from it (`Liveness::own_disk_slow`), and the
        // next heartbeat is fresh on a disk that answers.
        Sent::Message(length, own)
    }

    /// The heartbeat that ends this node's run, in order ([`Heartbeat::stopping`]), written into
    /// `out`: its bytes, or none where the pair shares no group or the peer took one already.
    /// Numbered after the latest sent and stated at the stream's interval; no flush proves it, for
    /// it claims no life. Stamped after every heartbeat sent before it, so that the peer's answer,
    /// which echoes it, names an announcement alone, and due again a stream's interval on until the
    /// answer comes: a lost announcement, or a lost answer, is asked again as a heartbeat would be.
    /// A stream that never sent the peer anything was never taken, and is not asked again.
    pub(crate) fn goodbye(
        &mut self,
        sender: &Sender,
        now_ns: u64,
        out: &mut [u8; MAX_BYTES],
    ) -> usize {
        if self.groups == 0 || self.stop_taken() {
            return 0;
        }
        let seq = self.stream.sent.map_or(0, |sent| sent.saturating_add(1));
        let sent_ns = now_ns.max(self.stream.sent_ns.saturating_add(1));
        let beat = Heartbeat {
            run: sender.local_run,
            seq,
            interval_ns: self.stream.interval_ns,
            floor_ns: sender.floor.map_or(0, nanos),
            ask_ns: 0,
            sent_ns,
            late_ns: 0,
            flushes: sender.durable_count,
            flush_age_ns: sender
                .durable_ns
                .map_or(0, |durable| flush_age(sender, sent_ns, durable)),
            apart: sender.apart,
            disk_slow: sender.apart && sender.disk_slow,
            echo: None,
            stopping: true,
        };
        self.stream.sent = Some(seq);
        self.stream.sent_ns = sent_ns;
        self.stream.next = (self.stream.interval_ns > 0).then(|| {
            (
                seq.saturating_add(1),
                sent_ns.saturating_add(self.stream.interval_ns),
            )
        });
        self.announced.get_or_insert(Announced {
            first_ns: sent_ns,
            taken: false,
        });
        self.counts.sent = self.counts.sent.saturating_add(1);
        beat.encode(out).len()
    }

    /// The peer's freshness at `now_ns`, with the node's own durability: a suspicion when its point
    /// passed and `Local::judge` holds it no longer, told at the time it holds it to; and whether the
    /// node must ask for a proof of its own first.
    pub(crate) fn judge(
        &mut self,
        peer: PeerId,
        now_ns: u64,
        local: &Local,
    ) -> (Option<Change>, bool) {
        // A run that announced its end is suspected from the announcement until the next run is
        // heard: nothing of it is left to judge.
        if self.received.stopped {
            return (None, false);
        }
        let at = match self.point() {
            // Not passed: every poll of every pair, so nothing more is read.
            Some(point) if now_ns < point => return (None, false),
            Some(point) => match self.hold(point, local) {
                Held::At(at) if now_ns >= at => Some(at),
                Held::At(at) => {
                    // Held later: held there, and no further.
                    self.held = Some((point, at));
                    return (None, false);
                }
                Held::Waiting { probe } => return (None, probe),
            },
            None => None,
        };
        (self.judge_at(peer, now_ns, at), false)
    }

    /// The suspicion at `now_ns`, of a point that passed and is judged at `at`, if any.
    fn judge_at(&mut self, peer: PeerId, now_ns: u64, at: Option<u64>) -> Option<Change> {
        let Some(link) = self.received.link.as_mut() else {
            // Nothing heard: suspected once its first freshness point passes.
            let until = at?;
            if self.unheard.suspected {
                return None;
            }
            self.unheard.suspected = true;
            return self.tell_suspected(peer, until, now_ns);
        };
        let until = match link.estimator.deadline() {
            Some(_) => {
                let at = at?;
                if link.estimator.poll(now_ns) != Some(Event::Suspected) {
                    return None;
                }
                at
            }
            // Suspected with no freshness point passing at a poll, and not told: a margin imposed
            // at a poll (the node's evidence's, `pool_margin`) found the latest heartbeat already
            // past the next freshness point. Told as any suspicion, from that point; untold, a
            // peer that died young was suspected and never reported.
            None if !self.told && link.estimator.trust() == Trust::Suspected => {
                link.estimator.freshness()?
            }
            None => return None,
        };
        self.tell_suspected(peer, until, now_ns)
    }

    /// The suspicion to tell the owner, unless it was told already.
    fn tell_suspected(&mut self, peer: PeerId, at_ns: u64, noticed_ns: u64) -> Option<Change> {
        if self.told {
            return None;
        }
        self.told = true;
        self.counts.suspicions = self.counts.suspicions.saturating_add(1);
        Some(Change::Suspected(self.suspicion(peer, at_ns, noticed_ns)))
    }

    /// What the owner is to be told after a heartbeat taken at `at_ns`: the trust it now has,
    /// where it differs from what the owner was told. A peer no margin judges
    /// (`Trust::Unconfigured`: its first heartbeat came with no evidence of the node's to judge it
    /// by, the evidence gone with a detach, or no margin found at its interval) is one the owner
    /// trusts by default, so a suspicion told before is withdrawn as for a trusted one.
    fn settle(&mut self, peer: PeerId, at_ns: u64) -> Option<Change> {
        match self.trust() {
            Trust::Trusted { .. } | Trust::Unconfigured if self.told => {
                self.told = false;
                Some(Change::Trusted { peer, at_ns })
            }
            Trust::Suspected => self.tell_suspected(peer, at_ns, at_ns),
            _ => None,
        }
    }

    fn suspicion(&self, peer: PeerId, at_ns: u64, noticed_ns: u64) -> Suspicion {
        let unheard = self
            .unheard
            .since_ns
            .filter(|_| self.received.link.is_none())
            .map(|since| Duration::from_nanos(at_ns.saturating_sub(since)));
        let detection = unheard.or_else(|| {
            let last = self.received.last?;
            self.received.offset.detection(at_ns, last.due_ns)
        });
        Suspicion {
            peer,
            at_ns,
            noticed_ns,
            last: self.received.last,
            detection,
            detector: self
                .received
                .configuration
                .map(|configured| configured.current),
        }
    }

    /// The bound the freshness point in force put on the next heartbeat's coming past it, from the
    /// arrivals as they stand: the margin in force, on the link's own arrivals where it has a
    /// configuration (as they stand, or as configured while they are refused, a moved link's levels
    /// being started again), and on the behaviour the margin was imposed from where the node's
    /// evidence judges it. `None` while no margin judges. Charged to the allowance
    /// once a heartbeat taken ends the gap it judged: each heartbeat taken is the one mistake its
    /// predecessor's freshness point can make (`docs/timing.md` §2.2), so the allowance is the
    /// bound the detector keeps as it runs, whatever the configuration assumed.
    fn promise(&self) -> Option<f64> {
        let link = self.received.link.as_ref()?;
        let margin = link.estimator.margin()?;
        let behaviour = match self.received.pooled {
            Some((pooled, ..)) if self.received.configuration.is_none() => pooled,
            _ => link.estimator.arrivals().ok().or(self
                .received
                .configuration
                .map(|configured| configured.link))?,
        };
        Some(lateness_bound(&behaviour, margin))
    }

    /// Where a heartbeat of run `run` stands against the latest run taken from the peer: the same
    /// run, or a later one, the peer restarted (or this is its first), whose numbers, proofs and
    /// echo start again, the link's history staying and its schedule anchored anew at the run's
    /// first heartbeat. Whether the peer restarted; a heartbeat of an earlier run, a superseded
    /// run's delivered after the new run's first, is refused as stale and starts nothing.
    fn begin_run(&mut self, run: u64) -> Result<bool, Refusal> {
        match self.received.run {
            Some(latest) if run == latest => return Ok(false),
            Some(latest) if run < latest => return Err(Refusal::Stale),
            _ => {}
        }
        let restarted = self.received.run.is_some();
        self.received.stopped = false;
        self.received.answer = false;
        if restarted && let Some(link) = self.received.link.as_mut() {
            link.estimator.forget_expected();
        }
        self.received.run = Some(run);
        self.received.base = self
            .received
            .last_mapped
            .map_or(0, |seq| seq.saturating_add(1));
        self.received.flushes = 0;
        self.received.last = None;
        self.received.echo = None;
        // A new run's clock is its host's, which a restart may have changed.
        self.received.offset = Offset::default();
        self.received.reanchor = restarted;
        Ok(restarted)
    }

    /// Takes heartbeat `beat` from `peer`, received at `arrival_ns`, after judging the peer at
    /// that arrival: a freshness point that passed before the heartbeat came is a suspicion
    /// whatever order the owner fed them in. The changes are, in order, a suspicion at the
    /// arrival, the peer's restart or its announced stop, and the trust the heartbeat leaves.
    pub(crate) fn take(
        &mut self,
        peer: PeerId,
        beat: &Heartbeat,
        arrival_ns: u64,
        context: &Context<'_>,
        changes: &mut [Option<Change>; 4],
        taken: &mut Taken,
    ) -> Result<(), Refusal> {
        changes[0] = self.judge(peer, arrival_ns, &context.local).0;
        let other_mode = self.take_mode(peer, beat, arrival_ns, context, changes);
        if let Some(ended) = self.take_ended(peer, beat, arrival_ns, changes, taken) {
            return ended;
        }
        let first_heard = self.received.run.is_none();
        let after_stop = self.received.stopped;
        if self.begin_run(beat.run)? {
            taken.after_stop = after_stop;
            if after_stop {
                // A run started after its peer's announced stop, which was sent nothing since:
                // the stream goes on from its next number at once, skipping no slot of the stop.
                self.stream.resume(arrival_ns);
            }
            // A new incarnation, which the owner's core trusts and holds to lead nothing it led.
            self.told = false;
            taken.restarted = true;
            changes[1] = Some(Change::Restarted {
                peer,
                at_ns: arrival_ns,
            });
        }
        if first_heard {
            self.stream.heard(arrival_ns);
        }
        if self.received.last.is_some_and(|last| beat.seq <= last.seq) {
            return Err(Refusal::Stale);
        }
        // A heartbeat of the other disk mode is judged apart from its sender's disk: its flush
        // proof, which that mode means otherwise, is not read (`take_mode`).
        self.prove(beat, context.apart || other_mode)?;
        self.keep_said(beat, arrival_ns);
        let granularity = context.granularity.ok_or(Refusal::Unmeasured)?;
        let mapped = self
            .received
            .base
            .checked_add(beat.seq)
            .ok_or(Refusal::OutOfRange)?;
        // The bound the freshness point in force put on this heartbeat's coming past it, as the
        // evidence stood before it came: a peer's first heartbeat is the end of the gap its
        // judgment from the attach judged.
        let first = self.received.link.is_none();
        let promise = if first {
            self.unheard
                .bound
                .filter(|_| self.unheard.until_ns.is_some())
        } else {
            self.promise()
        };
        // A heartbeat that echoes nothing comes from a peer that has taken none of this node's, and
        // that backs its interval off (`backed_off`): its next gap is expected as long as that
        // rule makes it, or each doubling would come a whole gap past the point and be suspected.
        let unanswered = if beat.echo.is_none() {
            unanswered_next(beat.interval_ns, beat.floor_ns)
        } else {
            0
        };
        self.received.unanswered_next_ns = unanswered;
        let link = self.link(beat.interval_ns, granularity)?;
        link.estimator
            .on_heartbeat(mapped, arrival_ns)
            .map_err(|_| Refusal::OutOfRange)?;
        if unanswered > 0 {
            let next = link
                .estimator
                .next_interval()
                .max(Duration::from_nanos(unanswered));
            link.estimator.expect_interval(next);
        }
        let lateness = link.estimator.latest_lateness();
        self.received.last = Some(Last {
            seq: beat.seq,
            arrival_ns,
            due_ns: beat.sent_ns.saturating_sub(beat.late_ns),
            sent_ns: beat.sent_ns,
        });
        self.received.last_mapped = Some(mapped);
        // Taken, whatever its mode: one of the other mode is counted besides as refused for its
        // mode (`PairReport::other_mode`), and an owner that waits on heartbeats taken (to charge
        // its election costs, or to know a peer heard) waits on it as on any.
        self.counts.taken = self.counts.taken.saturating_add(1);
        // A heartbeat that ends a gap a freshness point judged is the one mistake that point can
        // make: its bound is charged, once. A peer's restart ends no gap a live peer left.
        if let Some(promise) = promise.filter(|_| first || lateness.is_some()) {
            self.counts.allowance += promise.clamp(0.0, 1.0);
        }
        if self.renewal_due() {
            taken.configured = self.configure(context, granularity);
        }
        // The MTBF is a float division: read only where the margin is renewed.
        if self.received.configuration.is_none()
            && let Some(evidence) = context.evidence
            && self.pool_margin_due(true)
        {
            self.pool_margin(evidence, true, granularity, context.exposure.mtbf());
        }
        // Its lateness feeds the node's pool as any heartbeat's does: a node alone in its mode has
        // no other evidence, and the node's evidence is never narrower than its widest configured
        // link (`renew_evidence`).
        taken.lateness = lateness;
        taken.own = self.received.configuration.is_some();
        changes[2] = self.settle(peer, arrival_ns);
        if other_mode {
            return Err(Refusal::OtherMode);
        }
        Ok(())
    }

    /// Whether a heartbeat's flush proof holds, judged with today's mode (`apart` false): a flush
    /// counted past the last the peer proved, fresh within the heartbeat's interval and lateness.
    /// Judged apart from the sender's disk, any heartbeat proves its process alone.
    fn prove(&mut self, beat: &Heartbeat, apart: bool) -> Result<(), Refusal> {
        let fresh_flush = beat.flush_age_ns <= beat.late_ns.saturating_add(beat.interval_ns);
        if !apart && (beat.flushes <= self.received.flushes || !fresh_flush) {
            self.counts.unproven = self.counts.unproven.saturating_add(1);
            return Err(Refusal::Unproven);
        }
        self.received.flushes = self.received.flushes.max(beat.flushes);
        Ok(())
    }

    /// A heartbeat's disk mode (kiln's CP-T2.2, Focal's condition 1): whether it is of the other
    /// mode than this node's. One of the other mode is judged apart from its sender's disk, taken
    /// for its sender's process (its run, its arrival, its round trip) and refused for its disk
    /// (`Refusal::OtherMode`): neither its flush proof nor its disk verdict is read, each of which
    /// that mode means otherwise. So a peer of the other mode is never suspected for its disk by a
    /// judge of today's mode, nor misread by one of the disk mode, and one that dies is suspected,
    /// as every crashed process must be (completeness), so a group whose leader it was replaces
    /// it. The owner is told once as the mismatch begins (`Change::ModeDiffers`) and once as it
    /// ends (`Change::ModeAgrees`).
    fn take_mode(
        &mut self,
        peer: PeerId,
        beat: &Heartbeat,
        arrival_ns: u64,
        context: &Context<'_>,
        changes: &mut [Option<Change>; 4],
    ) -> bool {
        let other = beat.apart != context.apart;
        if other {
            self.counts.other_mode = self.counts.other_mode.saturating_add(1);
        }
        if other != self.received.other_mode {
            self.received.other_mode = other;
            changes[3] = Some(if other {
                Change::ModeDiffers {
                    peer,
                    at_ns: arrival_ns,
                }
            } else {
                Change::ModeAgrees {
                    peer,
                    at_ns: arrival_ns,
                }
            });
        }
        other
    }

    /// A heartbeat that announces the end of the peer's run, or one of a run that announced its
    /// end: what taking it comes to; `None` for any other.
    fn take_ended(
        &mut self,
        peer: PeerId,
        beat: &Heartbeat,
        arrival_ns: u64,
        changes: &mut [Option<Change>; 4],
        taken: &mut Taken,
    ) -> Option<Result<(), Refusal>> {
        let current = self.received.run == Some(beat.run);
        if beat.stopping {
            // The run the peer stops, if it is the one taken: no evidence of life, which changes
            // nothing the detector holds, and the peer's end, told at once. Suspected only as its
            // freshness point passed, it was trusted for as long as the detector's margin after
            // its last heartbeat, and a remaining voter that wrongly suspected its leader
            // meanwhile counted it in a quorum it trusted and asked for votes (mantle's roll; the
            // window `SuspicionStopUnannounced.cfg` finds). An announcement of a run not taken
            // announces nothing known: the restart after it is judged by the run that was.
            taken.stopping = true;
            if current {
                if !self.received.stopped {
                    self.received.stopped = true;
                    self.told = true;
                    changes[1] = Some(Change::Stopped {
                        peer,
                        at_ns: arrival_ns,
                    });
                }
                // Answered once, by the next heartbeat, which echoes it and is proven as any: the
                // peer learns its announcement was taken, and may hand over what it leads knowing
                // this node holds it out. Each announcement taken is answered, so one whose answer
                // was lost, asked again, is answered again.
                self.received.echo = Some((beat.sent_ns, beat.late_ns, arrival_ns));
                self.received.answer = true;
                self.stream.resume(arrival_ns);
            }
            return Some(Ok(()));
        }
        // A heartbeat of a run that announced its end, overtaken by the announcement: no evidence
        // that the run lives, which ended.
        (current && self.received.stopped).then_some(Err(Refusal::Stale))
    }

    /// In the disk mode, the verdict `beat`, taken, moves the peer's disk to (`Disk::judge`):
    /// `Some(true)` slow, `Some(false)` caught up.
    pub(crate) fn judge_disk(&mut self, beat: &Heartbeat) -> Option<bool> {
        self.disk.judge(beat)
    }

    /// In the disk mode, whether the peer's disk is judged slow by what its heartbeats say.
    pub(crate) fn disk_slow(&self) -> bool {
        self.disk.slow
    }

    /// How long past the peer's latest heartbeat this pair trusts it, `η + α`, once its detector
    /// has a margin: what a stall of the peer's would have been suspected after.
    pub(crate) fn freshness(&self) -> Option<Duration> {
        let link = self.received.link.as_ref()?;
        Some(
            link.estimator
                .next_interval()
                .saturating_add(link.estimator.margin()?),
        )
    }

    /// Whether the margin of the node's evidence is due, on the configuration's doubling schedule:
    /// never imposed, or the heartbeats taken have doubled since it was; or at once, where it was
    /// imposed from evidence that was not measured and the node's now is.
    fn pool_margin_due(&self, measured: bool) -> bool {
        self.received.pooled.is_none_or(|(_, at, was)| {
            (measured && !was) || self.counts.taken >= at.saturating_mul(2)
        })
    }

    /// While the pair has no configuration of its own, the margin the node's evidence configures
    /// for it, imposed on its estimator (`docs/timing.md` §3, item 10): what the node measured of
    /// its links (`Liveness::renew_evidence`) scaled to the link's window (`scaled`), widened by what the
    /// link's own latenesses show so far, at its costs and its floors, put in force as its own
    /// configuration would be (`LinkEstimator::configure`): the best at the link's interval while
    /// its unavailability is below one; where it is one or more, which promises nothing, the best
    /// over every interval the floors allow, that interval asked of the peer; none where that is
    /// one or more too. Imposed at the link's interval alone, one in about a thousand of the
    /// simulation's margins (8 of 6,786) had an unavailability past one. Renewed on the
    /// configuration's doubling schedule: at the first, and once the heartbeats taken have doubled
    /// since.
    pub(crate) fn pool_margin(
        &mut self,
        pool: &Arrivals,
        measured: bool,
        granularity: Duration,
        mtbf: Option<Duration>,
    ) {
        let (Some(election), Some(mtbf)) = (self.election, mtbf) else {
            return;
        };
        if !self.pool_margin_due(measured) {
            return;
        }
        let taken = self.counts.taken;
        // The floors the link's own configuration would be searched over: the peer's floor and
        // this node's `G` (`configure`).
        let floor = Duration::from_nanos(self.received.floor_ns).max(granularity);
        let Some(link) = self.received.link.as_mut() else {
            return;
        };
        // Its own latenesses are at its own window already; before its `τ_int` is measured they
        // have no count for an unseen share to stand on, which the node's evidence carries.
        let shown = link.estimator.arrivals_seen();
        let window = link.estimator.estimates().window.length;
        let Some(behaviour) = wider(Some(scaled(pool, window)), shown) else {
            return;
        };
        let costs = Costs { election, mtbf };
        let Some(at) =
            arrival_detector_at(&behaviour, &costs, granularity, link.estimator.interval())
        else {
            return;
        };
        let (detector, ask_ns) = if at.unavailability < 1.0 {
            (at, 0)
        } else {
            match configure_arrivals(&behaviour, &costs, granularity, floor) {
                Some(best) if best.unavailability < 1.0 => (best, nanos(best.interval)),
                _ => return,
            }
        };
        link.estimator.impose(detector.margin);
        self.received.pooled = Some((behaviour, taken, measured));
        self.received.pooled_ask_ns = ask_ns;
    }

    /// What a heartbeat taken says of the peer and of this node's stream: the peer's floor, its
    /// ask, the heartbeat to echo, and the round trip its echo closes.
    ///
    /// The interval the peer asks of this node's stream is its detector's choice from that
    /// stream's heartbeats, and a run that has taken none of them (its heartbeat echoes none) chose
    /// from nothing of it: its ask is taken once it has. A peer started again at an address its
    /// peers did not yet know asked no interval for as long as it heard none of theirs, and each
    /// stream to it, taking that, went to its floor, a flush a heartbeat sent where no one listened
    /// (mantle's roll: 40 to 200 liveness writes a second against 1 to 2;
    /// `a_restarted_runs_ask_is_taken_once_it_has_taken_a_heartbeat`). The stream keeps the
    /// interval it was at; a first run's stream starts from no ask, its floor, as before. An echo
    /// of this run's announcement that it ends answers it: the peer took it, and it goes no more.
    fn keep_said(&mut self, beat: &Heartbeat, arrival_ns: u64) {
        self.received.floor_ns = beat.floor_ns;
        if beat.echo.is_some() {
            self.stream.asked_ns = beat.ask_ns;
        }
        if let Some(announced) = self.announced.as_mut()
            && beat
                .echo
                .is_some_and(|echo| echo.sent_ns >= announced.first_ns)
        {
            announced.taken = true;
            self.stream.next = None;
        }
        self.received.echo = Some((beat.sent_ns, beat.late_ns, arrival_ns));
        if let Some(sum) = self.round_trip_sum(beat, arrival_ns) {
            self.received
                .offset
                .echoed(arrival_ns, beat.sent_ns.saturating_sub(beat.late_ns), sum);
        }
    }

    /// The delay sum of `beat`'s echo (the `bound` module): the round trip on this node's clock,
    /// with each side's lateness past its schedule added back. The network round trip is a sample
    /// of the pair's path. `None` without an echo, or when the echo says the heartbeat arrived
    /// before this node sent the one it echoes, which no clock can make true.
    fn round_trip_sum(&mut self, beat: &Heartbeat, arrival_ns: u64) -> Option<u64> {
        let echo = beat.echo?;
        let network = arrival_ns
            .checked_sub(echo.sent_ns)?
            .checked_sub(echo.hold_ns)?;
        self.received.round_trip.on_sample(network);
        network.checked_add(echo.late_ns)?.checked_add(beat.late_ns)
    }

    /// The link at the peer's `interval`, built on the first heartbeat and started again at a new
    /// interval or a new run's schedule: one allocation each.
    fn link(&mut self, interval_ns: u64, granularity: Duration) -> Result<&mut Link, Refusal> {
        let interval = Duration::from_nanos(interval_ns);
        let reanchor = std::mem::take(&mut self.received.reanchor);
        match self.received.link.as_mut() {
            None => {
                let estimator = LinkEstimator::new(interval, granularity, None)
                    .map_err(|_| Refusal::Malformed)?;
                self.received.link = Some(Box::new(Link {
                    estimator,
                    granularity_ns: nanos(granularity),
                }));
            }
            Some(link) => {
                if link.granularity_ns != nanos(granularity) {
                    link.granularity_ns = nanos(granularity);
                    link.estimator.set_granularity(granularity);
                }
                if reanchor || link.estimator.interval() != interval {
                    link.estimator
                        .retime(interval, None)
                        .map_err(|_| Refusal::Malformed)?;
                }
            }
        }
        self.received.link.as_deref_mut().ok_or(Refusal::Malformed)
    }

    /// Whether the detector is to be configured again (Chen et al.'s adaptive detector, which
    /// reconfigures as its estimates move, §6): never configured; at an interval the configuration
    /// was not made at (the peer moved to the one asked); or once the latenesses have doubled since
    /// the configurator last ran on them (`LinkEstimator::reconfigure_due`), when the estimate has
    /// moved by as much as it is uncertain (`docs/research/timing.md`, "When to renew an
    /// estimate's configuration"). No rule on `β` is kept beside it: the one before (`β` doubled
    /// past the configured) had no derivation, and in the simulation's worlds that change the
    /// detectors kept their allowance without it (`docs/timing.md` §3, item 11). A configurator
    /// that found no availability at all is asked again on the same schedule.
    fn renewal_due(&self) -> bool {
        let Some(link) = self.received.link.as_ref() else {
            return false;
        };
        (self.received.configuration.is_some()
            && self.received.configured_interval != Some(link.estimator.interval()))
            || link.estimator.reconfigure_due()
    }

    /// Configures the detector: the configurator over the link's arrivals, this node's `G`, the
    /// peer's floor and the costs, any margin admitted, the bound one Cantelli factor on an
    /// arrival's lateness, which assumes no independence between heartbeats (`docs/timing.md`
    /// §2.2). A refusal leaves the detector in force (`LinkEstimator::configure`). Whether it
    /// configured.
    fn configure(&mut self, context: &Context<'_>, granularity: Duration) -> bool {
        let floor_ns = self.received.floor_ns;
        let Some(link) = self.received.link.as_mut() else {
            return false;
        };
        // The arrivals are asked for before any cost is read, as the estimator's `configure` would
        // refuse without them: a link refused for want of `τ_int` waits at its interval until a longer
        // Allan level measures it (`docs/timing.md` §2.8, "The interval the evidence needs"), judged
        // by its node's evidence meanwhile, and a configured one by its configuration.
        if link.estimator.arrivals().is_err() {
            return false;
        }
        let Some(costs) = self
            .election
            .zip(context.exposure.mtbf())
            .map(|(election, mtbf)| Costs { election, mtbf })
        else {
            return false;
        };
        let floor = Duration::from_nanos(floor_ns).max(granularity);
        match link.estimator.configure(&costs, granularity, floor) {
            Ok(configured) => {
                self.received.pooled_ask_ns = 0;
                self.received.configuration = Some(configured);
                self.received.configured_interval = Some(link.estimator.interval());
                self.counts.configurations = self.counts.configurations.saturating_add(1);
                true
            }
            Err(_) => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The rule a point that passed is judged by, given the judge's own durability, branch by
    /// branch (`Local::judge`): a point at 100 ms, a margin of 20 ms, the peer expected at 80 ms.
    #[test]
    fn a_judge_whose_own_proof_stalls_holds_its_peers_to_it() {
        const POINT: u64 = 100 * 1_000_000;
        const MARGIN: u64 = 20 * 1_000_000;
        let at = |local: Local| match local.judge(POINT, MARGIN) {
            Held::At(at) => Some(at),
            Held::Waiting { probe } => {
                assert!(probe || matches!(local.stall, Some((_, None))));
                None
            }
        };
        let ms = |n: u64| n * 1_000_000;
        // Its device made something durable since the point: judged at the point.
        assert_eq!(
            at(Local {
                durable_ns: Some(ms(101)),
                stall: None
            }),
            Some(POINT)
        );
        // Nothing durable since the point, nothing asked: it asks first.
        assert!(matches!(
            Local {
                durable_ns: Some(ms(99)),
                stall: None
            }
            .judge(POINT, MARGIN),
            Held::Waiting { probe: true }
        ));
        // Its own proof outstanding: it waits.
        assert!(matches!(
            Local {
                durable_ns: Some(ms(50)),
                stall: Some((ms(60), None))
            }
            .judge(POINT, MARGIN),
            Held::Waiting { probe: false }
        ));
        // A wait as long as the margin, ended after the expected arrival: the margin again from
        // its end.
        assert_eq!(
            at(Local {
                durable_ns: Some(ms(150)),
                stall: Some((ms(90), Some(ms(150))))
            }),
            Some(ms(170))
        );
        // A shorter wait, ended before the point: proves nothing past its end, so it asks first.
        assert!(matches!(
            Local {
                durable_ns: Some(ms(95)),
                stall: Some((ms(90), Some(ms(95))))
            }
            .judge(POINT, MARGIN),
            Held::Waiting { probe: true }
        ));
        // A long wait that ended before the peer was expected: none of the peer's silence.
        assert_eq!(
            at(Local {
                durable_ns: Some(ms(101)),
                stall: Some((ms(10), Some(ms(70))))
            }),
            Some(POINT)
        );
    }

    /// A judge whose own device works: everything durable, no proof outstanding.
    fn steady() -> Local {
        Local {
            durable_ns: Some(u64::MAX),
            stall: None,
        }
    }
    use hyper_timing::Exposure;

    const MS: u64 = 1_000_000;

    /// The node's evidence a test judges by: arrivals of a millisecond's deviation, one in a
    /// thousand past every one seen.
    fn evidence() -> Arrivals {
        Arrivals {
            unseen: 0.001,
            lateness: Duration::ZERO,
            deviation: Duration::from_millis(1),
            mean_delay: Duration::ZERO,
        }
    }

    /// A pair sharing a group whose elections cost a millisecond.
    fn pair() -> Pair {
        let mut pair = Pair::new();
        pair.groups = 1;
        pair.election = Some(Duration::from_millis(1));
        pair
    }

    /// An hour of node time watched, so the MTBF is measured.
    fn exposure() -> Exposure {
        let mut exposure = Exposure::new();
        exposure.on_exposure(Duration::from_secs(3_600));
        exposure
    }

    /// Heartbeat `seq` of run `run` at a 10 ms interval, sent on time with a fresh flush.
    fn beat(run: u64, seq: u64) -> Heartbeat {
        Heartbeat {
            run,
            seq,
            interval_ns: 10 * MS,
            floor_ns: MS,
            ask_ns: 0,
            sent_ns: seq * 10 * MS,
            late_ns: 0,
            flushes: seq + 1,
            flush_age_ns: 0,
            apart: false,
            disk_slow: false,
            echo: None,
            stopping: false,
        }
    }

    /// A peer suspected before any heartbeat came from it, whose first heartbeat leaves no margin
    /// to judge it by (the node's evidence went, with the pairs whose configurations it was), is no
    /// longer suspected: the owner, told it was, is told it is trusted, its default for a peer no
    /// detector judges.
    #[test]
    fn a_suspicion_told_is_withdrawn_when_a_heartbeat_leaves_the_peer_unjudged() {
        let mut pair = pair();
        let exposure = exposure();
        let granularity = Duration::from_micros(50);
        pair.attached(0);
        pair.judge_unheard(
            &evidence(),
            Duration::from_millis(10),
            granularity,
            exposure.mtbf(),
        );
        let until = pair.deadline(&steady(), 0).expect("judged from the attach");
        assert!(matches!(
            pair.judge(2, until, &steady()).0,
            Some(Change::Suspected(_))
        ));
        let context = Context {
            granularity: Some(granularity),
            exposure: &exposure,
            evidence: None,
            local: steady(),
            apart: false,
        };
        let (mut changes, mut taken) = ([None, None, None, None], Taken::default());
        let arrival = until + MS;
        pair.take(2, &beat(7, 0), arrival, &context, &mut changes, &mut taken)
            .unwrap();
        assert_eq!(pair.trust(), Trust::Unconfigured);
        assert_eq!(
            changes,
            [
                None,
                None,
                Some(Change::Trusted {
                    peer: 2,
                    at_ns: arrival
                }),
                None
            ]
        );
    }

    /// A suspicion states its bound once any heartbeat of the run has carried an echo, whatever the
    /// rest carried: here a peer whose first heartbeats came before it had heard from this node
    /// (no echo), then one echoing a heartbeat of this node's, then nothing. The suspicion at the
    /// next freshness point states the time from the peer's last schedule plus the echo's slack and
    /// the drift, on one clock never below the time itself. Bounded by the mean of the echoed sums
    /// over the expected arrival's window, it stated none while a heartbeat in the window had no
    /// echo: a survivor's suspicion of a stalled member, once in twenty runs at one CPU.
    #[test]
    fn a_suspicion_states_its_bound_once_any_heartbeat_was_echoed() {
        let exposure = exposure();
        let granularity = Duration::from_micros(50);
        let context = Context {
            granularity: Some(granularity),
            exposure: &exposure,
            evidence: None,
            local: steady(),
            apart: false,
        };
        let mut pair = pair();
        let mut latest = 0;
        for seq in 0..6 {
            let mut heartbeat = beat(7, seq);
            // Delays of 1 ms on one clock; the last heartbeat, sent at 50 ms, echoes one this node
            // sent at 20 ms, 1 ms late, which reached the peer at 21 ms.
            let arrival = seq * 10 * MS + MS;
            if seq == 5 {
                heartbeat.echo = Some(Echo {
                    sent_ns: 20 * MS,
                    late_ns: MS,
                    hold_ns: 50 * MS - 21 * MS,
                });
            }
            let (mut changes, mut taken) = ([None, None, None, None], Taken::default());
            pair.take(2, &heartbeat, arrival, &context, &mut changes, &mut taken)
                .unwrap();
            latest = arrival;
        }
        pair.pool_margin(&evidence(), true, granularity, exposure.mtbf());
        let until = pair
            .deadline(&steady(), 0)
            .expect("judged by the node's evidence");
        assert!(until > latest);
        let Some(Change::Suspected(suspicion)) = pair.judge(2, until, &steady()).0 else {
            panic!("suspected at the freshness point");
        };
        let last = suspicion.last.expect("heard from");
        let bound = suspicion.detection.expect("a bound is stated");
        assert!(Duration::from_nanos(suspicion.at_ns - last.due_ns) <= bound);
    }

    /// A heartbeat goes out no earlier than the heartbeat it echoes came, and the peer's bound on
    /// the clocks' offset then holds on one clock. Here this node took the peer's heartbeat, sent
    /// 10 µs after this node's `now` and stamped 30 µs after it, and sent its own at that `now`
    /// (an owner that reads its clock before its socket). Sent at `now`, the echo's hold was
    /// negative, clamped to zero, and the peer's suspicions stated bounds 10 µs short of the time
    /// they measured: 51.6, 95 and 245 µs on Linux CI, where reads take longer.
    #[test]
    fn a_heartbeat_is_sent_no_earlier_than_the_one_it_echoes_came() {
        let exposure = exposure();
        let granularity = Duration::from_micros(50);
        let context = Context {
            granularity: Some(granularity),
            exposure: &exposure,
            evidence: None,
            local: steady(),
            apart: false,
        };
        let us = 1_000;
        let now = 10 * MS;
        let mut node = pair();
        let mut theirs = beat(7, 0);
        theirs.sent_ns = now + 10 * us;
        let arrival = now + 30 * us;
        let (mut changes, mut taken) = ([None, None, None, None], Taken::default());
        node.take(2, &theirs, arrival, &context, &mut changes, &mut taken)
            .unwrap();
        let sender = Sender {
            local_run: 1,
            floor: Some(Duration::from_millis(1)),
            granularity: Some(granularity),
            durable_count: 1,
            durable_ns: Some(now),
            stopped: false,
            apart: false,
            disk_slow: false,
            pending_ns: None,
        };
        let mut out = [0; MAX_BYTES];
        let Sent::Message(length, ..) = node.send(&sender, now, &mut out) else {
            panic!("due, and a flush proves it");
        };
        let ours = Heartbeat::decode(out.get(..length).unwrap()).unwrap();
        let echo = ours.echo.expect("it echoes the peer's");
        assert_eq!(ours.sent_ns, arrival, "after the echoed heartbeat came");
        assert_eq!(echo.hold_ns, 0);
        assert_eq!(ours.late_ns, 30 * us, "its schedule was `now`");
        // The peer takes it 20 µs later, and suspects 15 ms after that.
        let mut peer = pair();
        let landed = ours.sent_ns + 20 * us;
        let (mut changes, mut taken) = ([None, None, None, None], Taken::default());
        peer.take(1, &ours, landed, &context, &mut changes, &mut taken)
            .unwrap();
        let due = ours.sent_ns - ours.late_ns;
        let at = landed + 15 * MS;
        let bound = peer.received.offset.detection(at, due).unwrap();
        assert!(
            Duration::from_nanos(at - due) <= bound,
            "{} ns measured, {bound:?} stated",
            at - due
        );
    }

    /// A stream whose floor is nothing (a flush answered at the instant it was asked, its timer
    /// not yet measured) sends once a flush proves a heartbeat, the spacing it keeps never nothing.
    /// It kept nothing: every heartbeat came due at the instant it was asked, no flush fell after
    /// the one before, and it sent none (Quiver's simulation, kiln's CP-T2.2 step 6: a restarted
    /// node its peers suspected for good).
    #[test]
    fn a_stream_whose_floor_is_nothing_sends_once_a_flush_proves_it() {
        let sender = |floor: Duration, durable_count: u64, durable_ns: u64| Sender {
            local_run: 1,
            floor: Some(floor),
            granularity: None,
            durable_count,
            durable_ns: Some(durable_ns),
            stopped: false,
            apart: false,
            disk_slow: false,
            pending_ns: None,
        };
        let mut out = [0; MAX_BYTES];
        let now = 10_000 * MS;
        for floor in [Duration::ZERO, Duration::from_millis(1)] {
            let mut node = pair();
            // Due at once, no flush past the one before: one is asked, at a floor of nothing.
            let first = node.send(&sender(Duration::ZERO, 1, now - 1), now, &mut out);
            assert!(matches!(first, Sent::NeedsFlush), "a flush is asked");
            // The flush asked, answered at the same instant, the floor still nothing or measured
            // since: it proves the heartbeat.
            let proved = node.send(&sender(floor, 2, now), now, &mut out);
            assert!(matches!(proved, Sent::Message(..)), "floor {floor:?}: sent");
            assert!(node.stream.interval_ns > 0, "floor {floor:?}");
            assert!(node.report().sending.is_some(), "floor {floor:?}");
        }
    }

    /// A stream to a peer never heard backs off (`Pair::backed_off`): its interval doubles from the
    /// floor's at each heartbeat sent, to 3 s and no further; the peer's first heartbeat taken ends
    /// it, and the next heartbeat is due at once, numbered next and stating the floor's interval,
    /// however soon after the one before the peer was heard. Do: a floor of 1 ms, eighteen
    /// heartbeats sent unanswered, each proven as it falls due; the peer's first heartbeat taken
    /// 10 µs past the last one's due, and the answer proven 500 µs later, within the floor. Expect:
    /// intervals of 1, 2, 4 ... ms to 3 s and then 3 s; the answer numbered next, no slot skipped,
    /// stating 1 ms, and the stream at 1 ms after it. With the answer's spacing stated from the one
    /// before, 10 µs, the same answer skipped 50 slots.
    #[test]
    fn a_stream_never_heard_backs_off_to_three_seconds_and_answers_its_peer_at_once() {
        let granularity = Duration::from_micros(50);
        let floor = Duration::from_millis(1);
        let mut node = pair();
        let mut now = 10_000 * MS;
        let mut durable = 0u64;
        let sent = |node: &mut Pair, now: u64, durable: &mut u64| {
            *durable += 1;
            let sender = Sender {
                local_run: 1,
                floor: Some(floor),
                granularity: Some(granularity),
                durable_count: *durable,
                durable_ns: Some(now),
                stopped: false,
                apart: false,
                disk_slow: false,
                pending_ns: None,
            };
            let mut out = [0; MAX_BYTES];
            match node.send(&sender, now, &mut out) {
                Sent::Message(length, ..) => Heartbeat::decode(&out[..length]).unwrap(),
                _ => panic!("due and proven at {now}"),
            }
        };
        let mut stated = Vec::new();
        for _ in 0..18 {
            let beat = sent(&mut node, now, &mut durable);
            stated.push(beat.interval_ns);
            now = node.stream.next.map(|(_, due)| due).unwrap();
        }
        let expected: Vec<u64> = (0..18u32)
            .map(|n| {
                // The first states the floor's interval; each after it the spacing it was due at.
                let doublings = n.saturating_sub(1);
                (MS << doublings).min(UNANSWERED_NS)
            })
            .collect();
        assert_eq!(stated, expected, "the back-off");
        assert_eq!(node.stream.interval_ns, UNANSWERED_NS);
        let last_seq = node.stream.sent.unwrap();
        let last_due = now - UNANSWERED_NS;
        let skipped = node.counts.skipped;
        // The peer's first heartbeat, 10 µs past the last one's due.
        let exposure = exposure();
        let context = Context {
            granularity: Some(granularity),
            exposure: &exposure,
            evidence: None,
            local: steady(),
            apart: false,
        };
        let (mut changes, mut taken) = ([None, None, None, None], Taken::default());
        let heard = last_due + 10_000;
        node.take(2, &beat(1, 0), heard, &context, &mut changes, &mut taken)
            .unwrap();
        assert_eq!(node.next_due(), Some(heard), "the answer due at once");
        let answer = sent(&mut node, heard + MS / 2, &mut durable);
        assert_eq!(
            (answer.seq, answer.interval_ns, node.counts.skipped),
            (last_seq + 1, MS, skipped),
            "the answer numbered next, at the floor's interval, no slot skipped"
        );
        assert_eq!(node.stream.interval_ns, MS, "at the floor after it");
    }

    /// A new log's first flushes are slow and its later ones fast (on a windows-2025 runner the
    /// first took 0.7 to 3.1 s and the floor then fell to 33 to 118 ms): the interval the floor
    /// gave at the first flush is renewed from the floor each time the flushes double, down as well
    /// as up, so it comes down to the floor in one move a doubling. Never followed down, it stayed
    /// at the first flush's two seconds. The peer is one the node hears: a stream to one never heard
    /// backs off from that interval (`Pair::backed_off`).
    #[test]
    fn a_floor_that_fell_is_followed_down_once_its_flushes_have_doubled() {
        let granularity = Duration::from_micros(50);
        let (first, later) = (2_000 * MS, 50 * MS);
        // `E[flush] + G` after `n` flushes: the first and `n - 1` later ones.
        let floor = |n: u64| (first + later * (n - 1)) / n + nanos(granularity);
        let mut node = pair();
        node.received.run = Some(1);
        let mut now = 10_000 * MS;
        let (mut moves, mut last) = (0, 0);
        let flushes = 256u64;
        for n in 1..=flushes {
            let sender = Sender {
                local_run: 1,
                floor: Some(Duration::from_nanos(floor(n))),
                granularity: Some(granularity),
                durable_count: n,
                durable_ns: Some(now),
                stopped: false,
                apart: false,
                disk_slow: false,
                pending_ns: None,
            };
            let mut out = [0; MAX_BYTES];
            assert!(
                matches!(node.send(&sender, now, &mut out), Sent::Message(..)),
                "flush {n}: due and proven"
            );
            // The floor at the latest doubling of the flushes, at or before this one.
            let renewed_at = 1u64 << (u64::BITS - 1 - n.leading_zeros());
            let interval = node.stream.interval_ns;
            assert_eq!(interval, floor(renewed_at), "flush {n}");
            if interval != last {
                moves += 1;
                last = interval;
            }
            now = node.stream.next.map(|(_, due)| due).unwrap();
        }
        assert_eq!(
            moves,
            u64::BITS - flushes.leading_zeros(),
            "one move a doubling, from the first flush"
        );
    }

    /// A stop announced outside the disk mode says no disk verdict, as a heartbeat doesn't: a node
    /// whose disk was slow while it was judged apart, then set to today's mode, stops before a poll
    /// renews its verdict. A verdict outside the disk mode is malformed, and every peer refused the
    /// announcement as one (Focal's condition 1, the switch).
    #[test]
    fn a_stop_announced_outside_the_disk_mode_says_no_disk_verdict() {
        for apart in [false, true] {
            let mut node = pair();
            let sender = Sender {
                local_run: 1,
                floor: None,
                granularity: None,
                durable_count: 1,
                durable_ns: Some(0),
                stopped: true,
                apart,
                disk_slow: true,
                pending_ns: None,
            };
            let mut out = [0; MAX_BYTES];
            let length = node.goodbye(&sender, 10 * MS, &mut out);
            let beat = Heartbeat::decode(&out[..length]);
            assert_eq!(
                beat.map(|beat| (beat.stopping, beat.apart, beat.disk_slow)),
                Ok((true, apart, apart)),
                "judged apart: {apart}"
            );
        }
    }

    /// A young link whose latest heartbeat came later than the freshness point of the one after it,
    /// judged at a poll by the margin of the node's evidence once the node has some, is suspected,
    /// and the owner is told so, from that freshness point. Untold, the detector held the peer
    /// suspected while the owner trusted it: a peer that died young was never reported.
    #[test]
    fn a_young_link_suspected_by_a_margin_imposed_at_a_poll_is_told() {
        let exposure = exposure();
        let granularity = Duration::from_micros(50);
        let context = Context {
            granularity: Some(granularity),
            exposure: &exposure,
            evidence: None,
            local: steady(),
            apart: false,
        };
        let mut pair = pair();
        let mut latest = 0;
        // A heartbeat on its schedule and the next half a second late at a 10 ms interval: one
        // lateness, too few for the link's own to widen the node's evidence, which needs two, and
        // past the margin that evidence gives (a millisecond's deviation, an hour's exposure).
        for seq in 0..=1 {
            let arrival = seq * 10 * MS + if seq == 1 { 500 * MS } else { 0 };
            let (mut changes, mut taken) = ([None, None, None, None], Taken::default());
            pair.take(
                2,
                &beat(7, seq),
                arrival,
                &context,
                &mut changes,
                &mut taken,
            )
            .unwrap();
            assert_eq!(changes, [None, None, None, None]);
            latest = arrival;
        }
        assert_eq!(pair.trust(), Trust::Unconfigured);
        // The node's evidence comes, and the next poll imposes its margin and judges.
        pair.pool_margin(&evidence(), true, granularity, exposure.mtbf());
        assert_eq!(pair.trust(), Trust::Suspected, "the margin finds it late");
        let Some(Change::Suspected(suspicion)) = pair.judge(2, latest + MS, &steady()).0 else {
            panic!("the suspicion is told");
        };
        assert!(
            suspicion.at_ns <= latest,
            "from the point before the late heartbeat"
        );
        assert_eq!(suspicion.last.map(|last| last.seq), Some(1));
        assert_eq!(
            pair.judge(2, latest + 2 * MS, &steady()).0,
            None,
            "told once"
        );
    }

    /// The node's evidence's margin is put in force as a configuration of the link's own is: where
    /// at the link's interval it promises nothing (an unavailability of one or more), the best over
    /// every interval the floors allow, that interval asked of the peer. Here elections cost a
    /// second and the node's evidence leaves one lateness in fifty unseen, so at the link's 10 ms
    /// every margin's mistakes cost more election than there is time (`T_E·β/η ≥ 1·0.02/0.01`).
    /// Imposed at the link's interval alone, that margin was in force and no interval was asked.
    #[test]
    fn a_margin_of_the_nodes_evidence_that_promises_nothing_at_the_links_interval_is_not_its_margin()
     {
        let exposure = exposure();
        let granularity = Duration::from_micros(50);
        let context = Context {
            granularity: Some(granularity),
            exposure: &exposure,
            evidence: None,
            local: steady(),
            apart: false,
        };
        let mut pair = pair();
        pair.election = Some(Duration::from_secs(1));
        for seq in 0..2 {
            let (mut changes, mut taken) = ([None, None, None, None], Taken::default());
            pair.take(
                2,
                &beat(7, seq),
                seq * 10 * MS,
                &context,
                &mut changes,
                &mut taken,
            )
            .unwrap();
        }
        let evidence = Arrivals {
            unseen: 0.02,
            ..evidence()
        };
        pair.pool_margin(&evidence, true, granularity, exposure.mtbf());
        let (behaviour, ..) = pair.received.pooled.expect("a margin is in force");
        let costs = Costs {
            election: Duration::from_secs(1),
            mtbf: exposure.mtbf().unwrap(),
        };
        let at = arrival_detector_at(&behaviour, &costs, granularity, Duration::from_millis(10))
            .unwrap();
        assert!(at.unavailability >= 1.0, "{at:?}");
        let floor = Duration::from_millis(1).max(granularity);
        let best = configure_arrivals(&behaviour, &costs, granularity, floor).unwrap();
        assert!(best.unavailability < 1.0, "{best:?}");
        let link = pair.received.link.as_ref().unwrap();
        assert_eq!(link.estimator.margin(), Some(best.margin));
        assert_eq!(pair.received.pooled_ask_ns, nanos(best.interval));
        assert!(best.interval > Duration::from_millis(10));
    }
}
