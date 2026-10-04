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
fn nanos(duration: Duration) -> u64 {
    u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX)
}

/// What the node gives every pair's sender at a poll.
pub(crate) struct Sender {
    pub(crate) local_run: u64,
    pub(crate) floor: Option<Duration>,
    pub(crate) granularity: Option<Duration>,
    pub(crate) durable_count: u64,
    pub(crate) durable_ns: Option<u64>,
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
    /// A heartbeat of this many bytes is in the node's buffer.
    Message(usize),
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
    /// The interval the peer asked for.
    asked_ns: u64,
    /// The durable count the latest heartbeat carried: the next must carry more.
    proof: u64,
    /// The latest heartbeat's number sent.
    sent: Option<u64>,
}

/// How a peer from which no heartbeat has come is judged: from when the node first attached the
/// pair, one interval and the pool's margin at it (`docs/timing.md` §3, item 10).
#[derive(Clone, Copy, Debug, Default)]
struct Unheard {
    /// When the node first polled with the pair attached.
    since_ns: Option<u64>,
    /// The freshness point of the first heartbeat, once the pool can give a margin.
    until_ns: Option<u64>,
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
    /// The interval the link's own evidence needs before its first configuration, nanoseconds: the
    /// longest its estimator has said its heartbeats would be independent at, at an interval too
    /// correlated to measure (`LinkEstimator::independent_interval`); zero while it has said none,
    /// and from the first configuration on. Asked of the peer while it stands, so a link with no
    /// configuration moves to where its history can measure itself (`docs/timing.md` §2.8). A
    /// configured link keeps its configuration through a refusal and follows its best: held past
    /// the first configuration, the longest estimate every later refusal drew, a maximum of noisy
    /// estimates that grows with the refusals sampled, held a link at 31 s against its best of
    /// 0.57 s in a simulated world whose host freezes begin once its links configure
    /// (`docs/benchmarks.md`, "The detector model, at its causes").
    evidence_ns: u64,
    /// While the pair has no configuration of its own: the arrivals its margin was imposed from,
    /// the node's evidence scaled to the link's window and widened by the link's own, and the
    /// heartbeats taken then.
    pooled: Option<(Arrivals, u64)>,
    /// The interval asked of the peer for the margin of the node's evidence, nanoseconds, where at
    /// the link's own interval that margin promised nothing (`pool_margin`); zero otherwise, and
    /// from the link's first configuration on.
    pooled_ask_ns: u64,
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
    counts: Counts,
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
            counts: Counts::default(),
        }
    }

    pub(crate) fn trust(&self) -> Trust {
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
            self.unheard.bound = Some(lateness_bound(&behaviour, detector.margin));
        }
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
            configurations: counts.configurations,
            suspicions: counts.suspicions,
            allowance: counts.allowance,
        }
    }

    /// When the next heartbeat to the peer is due.
    pub(crate) fn next_due(&self) -> Option<u64> {
        self.stream.next.map(|(_, due)| due)
    }

    /// The peer's freshness point, while trusted: [`trust`](Self::trust)'s, read without
    /// building it, since every wake asked reads it of every pair.
    pub(crate) fn deadline(&self) -> Option<u64> {
        match self.received.link.as_ref() {
            Some(link) => link.estimator.deadline(),
            None => self.unheard.until_ns.filter(|_| !self.unheard.suspected),
        }
    }

    /// The interval to send at: the one the peer asked, where its floor allows it; where the floor
    /// binds (before the peer asks, or past what it asked), the floor, which the interval follows up
    /// and not down. A sender must keep its interval above its floor to be stable (Lindley 1952), so
    /// a floor that rose past the interval moves it; a floor that fell is a mean that moved with a
    /// sample, and following it would start the peer's estimator again at each
    /// (`LinkEstimator::retime`), which is how a link at its floor could go unconfigured for as long
    /// as its flushes kept moving their mean (`docs/timing.md` §2.9). The peer asks from what this
    /// node's floor was, so once it asks past it the interval is the peer's again. A change smaller
    /// than `G`, the configurator's resolution, is none.
    fn interval(&self, sender: &Sender) -> Option<u64> {
        let floor = nanos(sender.floor?);
        let asked = self.stream.asked_ns;
        let wanted = if asked >= floor {
            asked
        } else {
            floor.max(self.stream.interval_ns)
        };
        let current = self.stream.interval_ns;
        let resolution = sender.granularity.map_or(0, nanos);
        Some(if current != 0 && wanted.abs_diff(current) <= resolution {
            current
        } else {
            wanted
        })
    }

    /// Sends the heartbeat due, if a flush proves it: one made durable after the previous
    /// heartbeat was due, and newer than the one the previous heartbeat carried. A sender behind
    /// its schedule sends the latest heartbeat due; the ones it skipped are never sent, and the
    /// peer takes them as that heartbeat's lateness, the stall's delay (`docs/timing.md` §2.2).
    pub(crate) fn send(&mut self, sender: &Sender, now_ns: u64, out: &mut [u8; MAX_BYTES]) -> Sent {
        if self.groups == 0 {
            return Sent::Nothing;
        }
        // Not due: every poll asks every pair, and most have nothing due. A heartbeat is scheduled
        // only once a floor is measured, and a measured floor stays measured, so nothing below
        // could be asked of a pair with one scheduled and not yet due.
        if self.stream.next.is_some_and(|(_, due)| due > now_ns) {
            return Sent::Nothing;
        }
        let Some(interval) = self.interval(sender) else {
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
        let spacing = if self.stream.next.is_none() {
            interval
        } else {
            self.stream.interval_ns
        };
        let previous_due = due.saturating_sub(spacing);
        let proven = sender.durable_count > self.stream.proof
            && sender
                .durable_ns
                .is_some_and(|at| at > previous_due && at <= now_ns);
        let Some(durable_ns) = sender.durable_ns.filter(|_| proven) else {
            self.stream.next = Some((seq, due));
            if self.stream.interval_ns == 0 {
                self.stream.interval_ns = spacing;
            }
            return Sent::NeedsFlush;
        };
        let echo = self
            .received
            .echo
            .map(|(sent_ns, late_ns, arrival_ns)| Echo {
                sent_ns,
                late_ns,
                hold_ns: now_ns.saturating_sub(arrival_ns),
            });
        let ask_ns = self
            .received
            .configuration
            .map_or(0, |configured| nanos(configured.best.interval))
            .max(self.received.evidence_ns)
            .max(self.received.pooled_ask_ns);
        if let Some(link) = self.received.link.as_mut() {
            // The peer moves to what this heartbeat asks, never below its floor, from its next
            // heartbeat on: this node expects it so, not suspecting it for the move.
            link.estimator
                .expect_interval(Duration::from_nanos(ask_ns.max(self.received.floor_ns)));
        }
        let beat = Heartbeat {
            run: sender.local_run,
            seq,
            interval_ns: spacing,
            floor_ns: sender.floor.map_or(0, nanos),
            ask_ns,
            sent_ns: now_ns,
            late_ns: now_ns.saturating_sub(due),
            flushes: sender.durable_count,
            flush_age_ns: now_ns.saturating_sub(durable_ns),
            echo,
        };
        let length = beat.encode(out).len();
        // The slots between the latest heartbeat sent and this one were due while this node was
        // behind and are never sent: the peer takes them as this one's lateness.
        let skipped = self
            .stream
            .sent
            .map_or(0, |previous| seq.saturating_sub(previous).saturating_sub(1));
        self.counts.skipped = self.counts.skipped.saturating_add(skipped);
        self.stream.sent = Some(seq);
        self.stream.proof = sender.durable_count;
        self.stream.interval_ns = interval;
        self.stream.next = Some((seq.saturating_add(1), due.saturating_add(interval)));
        self.counts.sent = self.counts.sent.saturating_add(1);
        Sent::Message(length)
    }

    /// The peer's freshness at `now_ns`: a suspicion when it passed.
    pub(crate) fn judge(&mut self, peer: PeerId, now_ns: u64) -> Option<Change> {
        let Some(link) = self.received.link.as_mut() else {
            // Nothing heard: suspected once its first freshness point passes.
            let until = self.unheard.until_ns.filter(|until| now_ns >= *until)?;
            if self.unheard.suspected {
                return None;
            }
            self.unheard.suspected = true;
            return self.tell_suspected(peer, until, now_ns);
        };
        let until = match link.estimator.deadline() {
            Some(until) => {
                if link.estimator.poll(now_ns) != Some(Event::Suspected) {
                    return None;
                }
                until
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
            Some((pooled, _)) if self.received.configuration.is_none() => pooled,
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
    /// arrival, the peer's restart, and the trust the heartbeat leaves.
    pub(crate) fn take(
        &mut self,
        peer: PeerId,
        beat: &Heartbeat,
        arrival_ns: u64,
        context: &Context<'_>,
        changes: &mut [Option<Change>; 3],
        taken: &mut Taken,
    ) -> Result<(), Refusal> {
        changes[0] = self.judge(peer, arrival_ns);
        if self.begin_run(beat.run)? {
            // A new incarnation, which the owner's core trusts and holds to lead nothing it led.
            self.told = false;
            taken.restarted = true;
            changes[1] = Some(Change::Restarted {
                peer,
                at_ns: arrival_ns,
            });
        }
        if self.received.last.is_some_and(|last| beat.seq <= last.seq) {
            return Err(Refusal::Stale);
        }
        let fresh_flush = beat.flush_age_ns <= beat.late_ns.saturating_add(beat.interval_ns);
        if beat.flushes <= self.received.flushes || !fresh_flush {
            self.counts.unproven = self.counts.unproven.saturating_add(1);
            return Err(Refusal::Unproven);
        }
        self.received.flushes = beat.flushes;
        self.received.floor_ns = beat.floor_ns;
        self.stream.asked_ns = beat.ask_ns;
        self.received.echo = Some((beat.sent_ns, beat.late_ns, arrival_ns));
        if let Some(sum) = self.round_trip_sum(beat, arrival_ns) {
            self.received
                .offset
                .echoed(arrival_ns, beat.sent_ns.saturating_sub(beat.late_ns), sum);
        }
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
        let link = self.link(beat.interval_ns, granularity)?;
        link.estimator
            .on_heartbeat(mapped, arrival_ns)
            .map_err(|_| Refusal::OutOfRange)?;
        let lateness = link.estimator.latest_lateness();
        self.received.last = Some(Last {
            seq: beat.seq,
            arrival_ns,
            due_ns: beat.sent_ns.saturating_sub(beat.late_ns),
            sent_ns: beat.sent_ns,
        });
        self.received.last_mapped = Some(mapped);
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
            && self.pool_margin_due()
        {
            self.pool_margin(evidence, granularity, context.exposure.mtbf());
        }
        taken.lateness = lateness;
        taken.own = self.received.configuration.is_some();
        changes[2] = self.settle(peer, arrival_ns);
        Ok(())
    }

    /// Whether the margin of the node's evidence is due, on the configuration's doubling schedule:
    /// never imposed, or the heartbeats taken have doubled since it was.
    fn pool_margin_due(&self) -> bool {
        self.received
            .pooled
            .is_none_or(|(_, at)| self.counts.taken >= at.saturating_mul(2))
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
        granularity: Duration,
        mtbf: Option<Duration>,
    ) {
        let (Some(election), Some(mtbf)) = (self.election, mtbf) else {
            return;
        };
        if !self.pool_margin_due() {
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
        self.received.pooled = Some((behaviour, taken));
        self.received.pooled_ask_ns = ask_ns;
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
        // The evidence is asked for only where it is wanting: refused for want of `τ_int`, or no
        // cost to configure by yet. The estimator says what it wants before any cost is read, as
        // its `configure` would refuse: a link that moved is configured again at each heartbeat
        // until its levels measure `τ_int` at the new interval, and the costs and floors were
        // built for every one of those refusals. Only a link with no configuration is moved for
        // its evidence (`evidence_ns`); a configured one is judged by its configuration meanwhile.
        match link.estimator.arrivals() {
            Err(hyper_timing::Refusal::CorrelationUnmeasured) => {
                if self.received.configuration.is_none()
                    && let Some(next) = link.estimator.independent_interval()
                {
                    self.received.evidence_ns = self.received.evidence_ns.max(nanos(next));
                }
                return false;
            }
            Err(_) => return false,
            Ok(_) => {}
        }
        // Measured, so no interval is wanting for evidence (`independent_interval` says none).
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
                self.received.evidence_ns = 0;
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
            echo: None,
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
        let until = pair.deadline().expect("judged from the attach");
        assert!(matches!(pair.judge(2, until), Some(Change::Suspected(_))));
        let context = Context {
            granularity: Some(granularity),
            exposure: &exposure,
            evidence: None,
        };
        let (mut changes, mut taken) = ([None, None, None], Taken::default());
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
                })
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
            let (mut changes, mut taken) = ([None, None, None], Taken::default());
            pair.take(2, &heartbeat, arrival, &context, &mut changes, &mut taken)
                .unwrap();
            latest = arrival;
        }
        pair.pool_margin(&evidence(), granularity, exposure.mtbf());
        let until = pair.deadline().expect("judged by the node's evidence");
        assert!(until > latest);
        let Some(Change::Suspected(suspicion)) = pair.judge(2, until) else {
            panic!("suspected at the freshness point");
        };
        let last = suspicion.last.expect("heard from");
        let bound = suspicion.detection.expect("a bound is stated");
        assert!(Duration::from_nanos(suspicion.at_ns - last.due_ns) <= bound);
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
        };
        let mut pair = pair();
        let mut latest = 0;
        // A heartbeat on its schedule and the next half a second late at a 10 ms interval: one
        // lateness, too few for the link's own to widen the node's evidence, which needs two, and
        // past the margin that evidence gives (a millisecond's deviation, an hour's exposure).
        for seq in 0..=1 {
            let arrival = seq * 10 * MS + if seq == 1 { 500 * MS } else { 0 };
            let (mut changes, mut taken) = ([None, None, None], Taken::default());
            pair.take(
                2,
                &beat(7, seq),
                arrival,
                &context,
                &mut changes,
                &mut taken,
            )
            .unwrap();
            assert_eq!(changes, [None, None, None]);
            latest = arrival;
        }
        assert_eq!(pair.trust(), Trust::Unconfigured);
        // The node's evidence comes, and the next poll imposes its margin and judges.
        pair.pool_margin(&evidence(), granularity, exposure.mtbf());
        assert_eq!(pair.trust(), Trust::Suspected, "the margin finds it late");
        let Some(Change::Suspected(suspicion)) = pair.judge(2, latest + MS) else {
            panic!("the suspicion is told");
        };
        assert!(
            suspicion.at_ns <= latest,
            "from the point before the late heartbeat"
        );
        assert_eq!(suspicion.last.map(|last| last.seq), Some(1));
        assert_eq!(pair.judge(2, latest + 2 * MS), None, "told once");
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
        };
        let mut pair = pair();
        pair.election = Some(Duration::from_secs(1));
        for seq in 0..2 {
            let (mut changes, mut taken) = ([None, None, None], Taken::default());
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
        pair.pool_margin(&evidence, granularity, exposure.mtbf());
        let (behaviour, _) = pair.received.pooled.expect("a margin is in force");
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
