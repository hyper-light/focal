//! One node pair: the stream this node sends the peer and the detector it runs on the peer's.

use std::time::Duration;

use hyper_timing::{
    Configuration, Costs, Event, ExchangeRtt, Exposure, Floors, LinkBehaviour, LinkEstimator,
    Trust, detector_at, mistake_bound,
};

use crate::bound::Sums;
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
    pub(crate) evidence: Option<&'a LinkBehaviour>,
}

/// What a heartbeat taken did, for the node.
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct Taken {
    /// It began a new run of the peer: the peer restarted.
    pub(crate) restarted: bool,
    /// Its prediction error, nanoseconds, and the heartbeats its number says were due since the
    /// latest taken (one, or more where some were lost): what the node's pool is fed, while the
    /// pool or this pair needs it.
    pub(crate) error: Option<(u64, i64)>,
    /// Whether the pair judges by a configuration of its own.
    pub(crate) own: bool,
    /// Whether it configured the pair's detector anew.
    pub(crate) configured: bool,
}

/// The wider of two behaviours, each measure the larger: an upper bound on both. Theorem 7's `β`
/// grows with the loss and with the variance at every margin (each factor `(V + p·x²)/(V + x²)`
/// has derivative `x²/(V + x²)` in `p` and `x²(1 − p)/(V + x²)²` in `V`, neither negative), so a
/// margin configured from the wider promises no less than one from either would.
pub(crate) fn wider(a: Option<LinkBehaviour>, b: Option<LinkBehaviour>) -> Option<LinkBehaviour> {
    match (a, b) {
        (Some(a), Some(b)) => Some(LinkBehaviour {
            loss: a.loss.max(b.loss),
            mean_delay: a.mean_delay.max(b.mean_delay),
            delay_deviation: a.delay_deviation.max(b.delay_deviation),
        }),
        (one, None) | (None, one) => one,
    }
}

/// The behaviour the node measured of its links, for a link whose prediction errors are over a
/// window of `window` heartbeats: the deviation scaled by `√(1 + 1/n)`. For independent delays the
/// prediction errors' variance at a window of `n` is `V(D)(1 + 1/n)`, and the variance of the
/// errors the node measured, the pool's or a configured link's, is at least `V(D)`: so the scaled
/// deviation bounds the link's from above, which is the side Cantelli's inequality may err on (a
/// larger variance only loosens the bound).
pub(crate) fn scaled(pool: &LinkBehaviour, window: u64) -> LinkBehaviour {
    // u64 → f64 rounds only past 2⁵³, far past any window (`hyper_timing::WINDOW_LIMIT`).
    let n = window.max(1) as f64;
    LinkBehaviour {
        delay_deviation: pool.delay_deviation.mul_f64((1.0 + 1.0 / n).sqrt()),
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
}

/// How a peer from which no heartbeat has come is judged: from when the node first attached the
/// pair, one interval and the pool's margin at it (`docs/timing.md` §3, item 10).
#[derive(Clone, Copy, Debug, Default)]
struct Unheard {
    /// When the node first polled with the pair attached.
    since_ns: Option<u64>,
    /// The freshness point of the first heartbeat, once the pool can give a margin.
    until_ns: Option<u64>,
    suspected: bool,
}

/// The estimator of the peer's stream and the ring of its delay sums, boxed together: the
/// estimator's Allan levels are most of a kilobyte, and the pair's other fields are read every
/// poll.
#[derive(Debug)]
struct Link {
    estimator: LinkEstimator,
    sums: Sums,
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
    configuration: Option<Configuration>,
    /// The latest heartbeat number whose freshness point the allowance counts.
    accounted: Option<u64>,
    /// When the detector was last configured: the heartbeats taken then, and `β` at its margin.
    renewed: Option<(u64, f64)>,
    round_trip: ExchangeRtt,
    /// The interval the link's own evidence needs, nanoseconds: the longest its estimator has
    /// said its heartbeats would be independent at, at an interval too correlated to measure
    /// (`LinkEstimator::independent_interval`); zero while it has said none. Asked of the peer,
    /// whatever the configuration's best, so the link is never asked back to an interval its
    /// estimator showed it cannot measure.
    evidence_ns: u64,
    /// While the pair has no configuration of its own: the behaviour its margin was imposed from,
    /// the node's evidence scaled to the link's window and widened by the link's own, and the
    /// heartbeats taken then.
    pooled: Option<(LinkBehaviour, u64)>,
}

/// What a pair counts: the counters of its [`PairReport`], the rest of which is read from the
/// pair when a report is made. Kept whole, the report carried two intervals and three flags a
/// poll's walk of the pairs moved through and nothing read (`docs/benchmarks.md`, "The node's
/// evidence, kept").
#[derive(Clone, Copy, Debug, Default)]
struct Counts {
    sent: u64,
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
        pool: &LinkBehaviour,
        floor: Duration,
        granularity: Duration,
        mtbf: Option<Duration>,
    ) {
        let (Some(since), Some(election), Some(mtbf)) =
            (self.unheard.since_ns, self.election, mtbf)
        else {
            return;
        };
        let floors = Floors {
            granularity,
            sender: floor.max(granularity),
            correlation: Duration::MAX,
        };
        let costs = Costs { election, mtbf };
        if let Some(detector) = detector_at(&scaled(pool, 1), &costs, &floors, floors.sender) {
            self.unheard.until_ns = Some(
                since
                    .saturating_add(nanos(floors.sender))
                    .saturating_add(nanos(detector.margin)),
            );
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
    /// its schedule sends the latest heartbeat due; the ones it skipped are lost to the peer, which
    /// is what they are.
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
            .max(self.received.evidence_ns);
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
        // The freshness point of the heartbeat after the latest passed: one more point judged.
        if let Some(next) = self.received.last_mapped.map(|seq| seq.saturating_add(1)) {
            let beta = self.beta_now();
            self.account(next, beta);
        }
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
            self.received.link.as_ref().and_then(|link| {
                let margin = link.estimator.margin()?;
                link.sums.detection(
                    link.estimator.estimates().window.length,
                    link.estimator.next_interval(),
                    margin,
                )
            })
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

    /// Theorem 7's `β` at the margin and interval in force, from the estimates as they stand: the
    /// bound on a mistake at each freshness point now, whatever the configuration assumed. `None`
    /// before a margin is configured.
    fn beta_now(&self) -> Option<f64> {
        let link = self.received.link.as_ref()?;
        let margin = link.estimator.margin()?;
        let behaviour = match self.received.pooled {
            // Judged by the node's evidence: the bound its margin promised, from that behaviour.
            Some((pooled, _)) if self.received.configuration.is_none() => pooled,
            _ => link.estimator.behaviour().ok().or(self
                .received
                .configuration
                .map(|configured| configured.link))?,
        };
        let variance = behaviour.delay_deviation.as_secs_f64().powi(2);
        Some(mistake_bound(
            behaviour.loss,
            variance,
            link.estimator.interval().as_secs_f64(),
            margin.as_secs_f64(),
        ))
    }

    /// Counts the freshness points through the estimator's heartbeat `seq` into the allowance, each
    /// at `β` (`beta_now`), while a margin is in force: the allowance is the bound the detector
    /// keeps as it runs, so a configuration older than the estimates does not understate it.
    fn account(&mut self, seq: u64, beta: Option<f64>) {
        let points = match self.received.accounted {
            Some(accounted) if seq > accounted => seq.saturating_sub(accounted),
            Some(_) => return,
            None => 1,
        };
        if let Some(beta) = beta {
            // u64 → f64 rounds only past 2⁵³ heartbeats.
            self.counts.allowance += beta.clamp(0.0, 1.0) * points as f64;
        }
        self.received.accounted = Some(seq);
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
        self.received.run = Some(run);
        self.received.base = self
            .received
            .last_mapped
            .map_or(0, |seq| seq.saturating_add(1));
        self.received.flushes = 0;
        self.received.last = None;
        self.received.echo = None;
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
        let sum = self.round_trip_sum(beat, arrival_ns);
        let granularity = context.granularity.ok_or(Refusal::Unmeasured)?;
        let mapped = self
            .received
            .base
            .checked_add(beat.seq)
            .ok_or(Refusal::OutOfRange)?;
        let previous = self.received.last_mapped;
        let link = self.link(beat.interval_ns, granularity)?;
        link.estimator
            .on_heartbeat(mapped, arrival_ns)
            .map_err(|_| Refusal::OutOfRange)?;
        link.sums.push(sum);
        let error = link.estimator.latest_error();
        self.received.last = Some(Last {
            seq: beat.seq,
            arrival_ns,
            due_ns: beat.sent_ns.saturating_sub(beat.late_ns),
            sent_ns: beat.sent_ns,
        });
        self.received.last_mapped = Some(mapped);
        self.counts.taken = self.counts.taken.saturating_add(1);
        let beta = self.beta_now();
        self.account(mapped, beta);
        if self.renewal_due(beta) {
            taken.configured = self.configure(context, granularity);
        }
        // The MTBF is a float division: read only where the margin is renewed.
        if self.received.configuration.is_none()
            && let Some(evidence) = context.evidence
            && self.pool_margin_due()
        {
            self.pool_margin(evidence, granularity, context.exposure.mtbf());
        }
        let due = previous.map_or(1, |previous| mapped.saturating_sub(previous).max(1));
        taken.error = error.map(|error| (due, error));
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
    /// link's own prediction errors and losses show so far, at the link's interval, its costs and
    /// its floors, as its own configuration would be. Renewed on the configuration's doubling
    /// schedule: at the first, and once the heartbeats taken have doubled since.
    pub(crate) fn pool_margin(
        &mut self,
        pool: &LinkBehaviour,
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
        let floor_ns = self.received.floor_ns;
        let Some(link) = self.received.link.as_mut() else {
            return;
        };
        let own = link.estimator.estimates();
        // Its own errors are at its own window already, and its loss is Jeffreys' over what it
        // was sent: before its `τ_int` is measured, the unseen term has no count to stand on.
        let shown = own.delay_deviation.map(|delay_deviation| LinkBehaviour {
            loss: own.loss,
            mean_delay: own.mean_delay.unwrap_or(Duration::ZERO),
            delay_deviation,
        });
        let Some(behaviour) = wider(Some(scaled(pool, own.window.length)), shown) else {
            return;
        };
        let floors = Floors {
            granularity,
            sender: Duration::from_nanos(floor_ns).max(granularity),
            correlation: Duration::MAX,
        };
        let costs = Costs { election, mtbf };
        if let Some(detector) = detector_at(&behaviour, &costs, &floors, link.estimator.interval())
        {
            link.estimator.impose(detector.margin);
            self.received.pooled = Some((behaviour, taken));
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
                let sums = Sums::new(estimator.estimates().window.drift);
                self.received.link = Some(Box::new(Link {
                    estimator,
                    sums,
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
                    link.sums.restart(link.estimator.estimates().window.drift);
                }
            }
        }
        self.received.link.as_deref_mut().ok_or(Refusal::Malformed)
    }

    /// Whether the detector is to be configured again (Chen et al.'s adaptive detector, which
    /// reconfigures as its estimates move, §6): never configured; at an interval the configuration
    /// was not made for (the peer moved to the one asked); once the configuration is as stale as
    /// the estimates are uncertain; or once `β` at the margin in force has doubled past the
    /// configured one, the link having got worse.
    ///
    /// Staleness: the inputs are estimated over the link's whole history (the prediction errors'
    /// variance, the loss, `τ_int`'s levels, the exposure the MTBF is), and an estimate over `n`
    /// heartbeats that the one over the first `n_k` is nested in has moved since by a variance of
    /// `σ²(1/n_k − 1/n)` (`σ²` its variance from one heartbeat, `τ_int` folded in alike), while its
    /// own is `σ²/n`. The two meet at `n = 2·n_k`: renewed sooner, a configuration follows moves
    /// smaller than the estimates' own error; later, it lags the evidence by more than that error.
    /// The rule asks nothing of the delays' distribution, so a link is configured
    /// `O(log heartbeats)` times, where the estimator's window cadence configured every few
    /// heartbeats and spent five times the stream's own work on the configurator
    /// (`docs/benchmarks.md`, "hyper-liveness"). The second rule's factor is not derived: a link
    /// that changes is `docs/timing.md` §3, item 11.
    fn renewal_due(&self, beta: Option<f64>) -> bool {
        let Some(configured) = self.received.configuration else {
            return true;
        };
        let interval = self
            .received
            .link
            .as_ref()
            .map(|link| link.estimator.interval());
        let Some((taken, renewed_beta)) = self.received.renewed else {
            return true;
        };
        interval != Some(configured.current.interval)
            || self.counts.taken >= taken.saturating_mul(2)
            || beta.is_some_and(|now| now >= 2.0 * renewed_beta)
    }

    /// Configures the detector: the configurator over this node's `G`, the peer's floor and the
    /// costs, with one heartbeat in the margin (`α < η`), whose Theorem 7 bound is a single
    /// Cantelli factor and assumes no independence between heartbeats. The traces' correlation
    /// time `T_c` is the spacing past the longest stall a run saw (`docs/timing.md` §2.6, item 6),
    /// which a young link has not seen: taking it as `τ_int·η` from the link's own history let the
    /// product of a many-heartbeat margin promise a mistake rate that six runs in ten broke under a
    /// one-CPU throttle, whose stalls the history had not yet held (`docs/benchmarks.md`,
    /// "hyper-liveness"). hyper-swim judges each probe on its own for the same reason (§2.7). A
    /// refusal leaves the detector in force (`LinkEstimator::configure`). Whether it configured.
    fn configure(&mut self, context: &Context<'_>, granularity: Duration) -> bool {
        let floor_ns = self.received.floor_ns;
        let Some(link) = self.received.link.as_mut() else {
            return false;
        };
        // The evidence is asked for only where it is wanting: refused for want of `τ_int`, or no
        // cost to configure by yet. The estimator says what it wants before any cost is read, as
        // its `configure` would refuse: a link that moved is configured again at each heartbeat
        // until its levels measure `τ_int` at the new interval, and the costs and floors were
        // built for every one of those refusals.
        match link.estimator.behaviour() {
            Err(hyper_timing::Refusal::CorrelationUnmeasured) => {
                if let Some(next) = link.estimator.independent_interval() {
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
        let floors = Floors {
            granularity,
            sender: Duration::from_nanos(floor_ns).max(granularity),
            correlation: Duration::MAX,
        };
        let Ok(configured) = link.estimator.configure(&costs, &floors) else {
            return false;
        };
        self.received.configuration = Some(configured);
        self.counts.configurations = self.counts.configurations.saturating_add(1);
        let beta = self.beta_now().unwrap_or(1.0);
        self.received.renewed = Some((self.counts.taken, beta));
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use hyper_timing::Exposure;

    const MS: u64 = 1_000_000;

    /// The node's evidence a test judges by: a link of a millisecond's deviation that loses
    /// one heartbeat in a thousand.
    fn evidence() -> LinkBehaviour {
        LinkBehaviour {
            loss: 0.001,
            mean_delay: Duration::ZERO,
            delay_deviation: Duration::from_millis(1),
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
        // Ten heartbeats on their schedule, and the eleventh 50 ms late at a 10 ms interval.
        for seq in 0..=10 {
            let arrival = seq * 10 * MS + if seq == 10 { 50 * MS } else { 0 };
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
        assert_eq!(suspicion.last.map(|last| last.seq), Some(10));
        assert_eq!(pair.judge(2, latest + 2 * MS), None, "told once");
    }
}
