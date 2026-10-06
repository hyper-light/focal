//! Node-pair liveness: one heartbeat stream per pair of nodes that share a consensus group, shared
//! by every group they share, each heartbeat proving a recent durable flush of the sender's log
//! (`docs/timing.md` §2.1, §2.8; step L-3).
//!
//! **What it replaces.** A Raft group's leader heartbeats each follower every `heartbeat_tick`,
//! and each follower runs an election timer: per-group messages and timers that grow with the
//! groups, not the machines, each a constant someone picked. Here a node keeps one stream to each
//! node it shares a group with ([`Liveness::attach`] counts the groups), sends one heartbeat on it
//! every `η`, and judges the peer's stream with NFD-E (Chen, Toueg and Aguilera 2002) through
//! `hyper_timing::LinkEstimator`, configured by `hyper_timing::configure_arrivals` from measured
//! floors. A group
//! sends nothing of its own for liveness; its core asks this crate whether the leader's node is
//! suspected (L-2). A pair with no group in common sends nothing at all.
//!
//! **The flush proof** (CockroachDB's store liveness, `docs/research/timing.md`). A heartbeat leaves
//! only once the sender's log has made a write durable after the previous heartbeat to that peer
//! was due: the owner reports each durable completion ([`Liveness::on_durable`]), and when none
//! came in time the stream asks for one ([`Output::flush`]) and sends on its completion. A node
//! whose disk stalls therefore stops heartbeating and is suspected as a crashed one is. Each
//! heartbeat carries the sender's count of durable writes and the age of the latest; a receiver
//! takes a heartbeat only when the count moved and the flush came after the previous heartbeat was
//! due, so a sender that heartbeats without flushing is not trusted either.
//!
//! **Timing, all measured.** The sender's interval is the one the receiver's configurator chose
//! (Chen et al.'s adaptive scheme, the receiver asking in its own heartbeats), never shorter than
//! the sender's stability floor `E[flush] + G` (Lindley 1952; `docs/timing.md` §2.6); before the
//! receiver has chosen, the floor. `G` is the mean lateness of the owner's waits for the stream's
//! wakes, each begun before its deadline and ended at or past it, whatever ended it
//! ([`Liveness::on_wait`]): how late past its wakes the stream is polled while its owner waits for
//! them, not what the owner's own work adds. `E[flush]` the mean of the
//! durable completions reported, the margin
//! bounded by one Cantelli factor on each heartbeat's lateness past its expected arrival (a slot
//! the sender skipped or the network lost being the next one's lateness), which assumes no
//! independence between heartbeats (`docs/timing.md` §2.2), the MTBF the Jeffreys posterior over
//! the pairs watched and the restarts seen (`hyper_timing::Exposure`), and the election cost the
//! owner's (`Liveness::set_election`, the election law's `T_E`). A configuration whose
//! unavailability is one or more promises nothing and is not made.
//!
//! **Every link judged** (`docs/timing.md` §2.8–§2.9, §3 item 10). A link whose heartbeats are too
//! correlated at its interval for its estimator to measure its evidence asks the interval its Allan
//! levels say they would be independent at (`LinkEstimator::independent_interval`), and a sender
//! follows its floor up, not down, so the receiver's evidence is not started again at each move of
//! a mean. Until a link configures, it is judged by the margin what its node measured of its links
//! configures for it: the wider of its pool's measure and the widest of its configured links', and
//! of what the link's own latenesses show; a peer from which nothing has come is judged from the
//! attach. A receiver expects
//! the interval it asked (`LinkEstimator::expect_interval`), and a peer's new run is reported
//! ([`Change::Restarted`]).
//!
//! **The bound.** Each suspicion states when the sender's last heartbeat was due on the sender's
//! clock and the bound past it within which NFD-E suspected: the difference of the two clocks'
//! readings less their offset, which the echoed round trips bound from below and the clocks' drift
//! ages (`bound`); no clock synchronization or path symmetry enters it.
//!
//! **Sans-io.** The crate is fed `now`, messages with their kernel receive stamps (hyper-tokio's
//! `PlaneSocket`), and durable completions, and returns heartbeats to queue on the datagram plane,
//! flush requests, trust changes and the time to be polled next. It never reads a clock, spawns or
//! opens a socket. Once each pair is configured, a heartbeat sent and one received allocate
//! nothing (`benches/allocs.rs`).

#![cfg_attr(
    test,
    allow(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::panic,
        clippy::indexing_slicing,
        clippy::arithmetic_side_effects,
        clippy::disallowed_macros,
        clippy::cast_possible_truncation,
        clippy::cognitive_complexity
    )
)]

mod bound;
mod codec;
mod pair;

use std::collections::BTreeMap;
use std::time::Duration;

pub use codec::{Echo, Heartbeat, KIND, MAX_BYTES, VERSION, is_liveness};
use hyper_timing::{
    Arrivals, Configuration, Detector, ExchangeRtt, Exposure, Flushes, Lateness, LinkEstimator,
    Trust,
};
use pair::Pair;

/// A node's identity, as the owner names it (the datagram plane's `PeerId`).
pub type PeerId = u64;

/// Why the crate refused: every failure is one of these, never a panic.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Refusal {
    /// A limit given to [`Liveness::new`] is zero.
    Limits,
    /// The node already keeps [`Settings::max_peers`] pairs.
    TooManyPeers,
    /// A pair cannot count more groups than a `u32` holds.
    TooManyGroups,
    /// No pair with this peer: nothing attached it.
    UnknownPeer,
    /// The peer is this node.
    FromSelf,
    /// The message is shorter than a heartbeat.
    Truncated,
    /// The message is not a liveness message.
    NotLiveness,
    /// A wire version this build does not read.
    BadVersion,
    /// The message does not parse.
    Malformed,
    /// A heartbeat no newer than the latest taken from the peer's run, or of an earlier run than
    /// the latest taken: a superseded run's.
    Stale,
    /// A heartbeat whose flush proof does not hold: its count of durable writes did not move, or
    /// its latest flush is older than the previous heartbeat's schedule.
    Unproven,
    /// The receiver's timer granularity is not measured yet, so no estimator can be built; the
    /// heartbeat's echo is kept.
    Unmeasured,
    /// A heartbeat too far from its run's schedule for the estimator to sum: a sender that
    /// restarted its schedule.
    OutOfRange,
}

impl std::fmt::Display for Refusal {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Debug::fmt(self, formatter)
    }
}

impl std::error::Error for Refusal {}

/// A node's liveness settings.
#[derive(Clone, Copy, Debug)]
pub struct Settings {
    /// This node.
    pub local: PeerId,
    /// This run of the node: a count it keeps durably and raises at every start, before the
    /// stream's first heartbeat, so each run's is greater than every earlier run's (the owner's
    /// record, written whole with the platform's full flush and its directory's: `docs/timing.md`
    /// §2.8, "A restart"). Runs are ordered by it: a heartbeat of a later run than the latest taken
    /// from the peer is the peer's restart, and one of an earlier run is refused
    /// ([`Refusal::Stale`]), as a superseded run's heartbeat delivered after the new run's first
    /// is (the plane keeps two epochs a peer). A value with no order, a boot nonce or a process id,
    /// made such a heartbeat a restart back to the old run and the new run's next another, and a
    /// value reused across runs made the new run's heartbeats, numbered from zero again, stale.
    pub run: u64,
    /// The most pairs the node keeps: the nodes placement lets it share groups with. Past it,
    /// [`Liveness::attach`] refuses.
    pub max_peers: usize,
    /// The fleet's failure history so far, the MTBF's prior evidence ([`Exposure::new`] for a
    /// fleet with none).
    pub history: Exposure,
    /// The resolution of the clock the owner reads `now`, its waits and the receive stamps on:
    /// the least step its readings take. A wait read exactly on time was late by less than it, so
    /// the timer's granularity `G` is never stated below it (`hyper_timing::Lateness`,
    /// `docs/timing.md` §2.4). hyper-tokio's `Clock::resolution` states the host's monotonic
    /// clock's; a simulation's stamps are whole nanoseconds, one.
    pub resolution: Duration,
}

/// Which write became durable.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Write {
    /// A log write the node made for its groups: the shell's completion.
    Log,
    /// The write [`Output::flush`] asked for.
    Liveness,
}

/// A suspicion of a peer: who, when, and the bound it kept.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Suspicion {
    /// The suspected node.
    pub peer: PeerId,
    /// The freshness point that passed with no newer heartbeat, on this node's clock.
    pub at_ns: u64,
    /// When the owner's poll noticed it, on this node's clock.
    pub noticed_ns: u64,
    /// The latest heartbeat taken from the peer: its number, its arrival (the kernel's stamp) on
    /// this node's clock, and when it was due and sent on the peer's.
    pub last: Option<Last>,
    /// The bound on the time from the last heartbeat's schedule, on the peer's clock, to `at_ns`, on
    /// this node's: their difference less the clocks' offset, bounded from below by the echoed round
    /// trips of the run's heartbeats and aged by the clocks' drift (the `bound` module); `None`
    /// before any heartbeat of the run carried an echo. For a peer from which no heartbeat came
    /// (`last` is `None`), the time from the node's first poll with the pair attached, an interval
    /// and the margin of the node's evidence (`docs/timing.md` §3, item 10).
    pub detection: Option<Duration>,
    /// The detector in force when it suspected, if configured (the pair's configuration's
    /// `current`, [`Liveness::configuration`]).
    pub detector: Option<Detector>,
}

/// The latest heartbeat taken from a peer.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Last {
    /// Its number in the peer's run.
    pub seq: u64,
    /// Its kernel receive stamp, on this node's clock.
    pub arrival_ns: u64,
    /// When it was due, on the peer's clock.
    pub due_ns: u64,
    /// When it was sent, on the peer's clock.
    pub sent_ns: u64,
}

/// A change in what this node believes of a peer.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Change {
    /// The peer's freshness passed: suspected.
    Suspected(Suspicion),
    /// A fresh heartbeat came from a peer the owner was told is suspected, and left it trusted or
    /// judged by no margin: a peer no detector judges is one the owner trusts.
    Trusted {
        /// The peer.
        peer: PeerId,
        /// The heartbeat's arrival.
        at_ns: u64,
    },
    /// A heartbeat came from a later run of the peer: it restarted, a new incarnation, which the
    /// owner's core trusts and holds to lead nothing it led before (`RawNode::restarted`). What the
    /// heartbeat leaves the peer's trust at follows, if it is not trusted.
    Restarted {
        /// The peer.
        peer: PeerId,
        /// The new run's first heartbeat's arrival.
        at_ns: u64,
    },
}

impl Change {
    /// The peer the change is about.
    pub fn peer(&self) -> PeerId {
        match self {
            Self::Suspected(suspicion) => suspicion.peer,
            Self::Trusted { peer, .. } | Self::Restarted { peer, .. } => *peer,
        }
    }
}

/// What a pair has done and promised, for the owner and its tests.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct PairReport {
    /// Groups the pair shares.
    pub groups: u32,
    /// Heartbeats sent to the peer.
    pub sent: u64,
    /// Slots of the stream to the peer that came due while this node was behind and were never
    /// sent: the peer takes each as the next heartbeat's lateness, not as a loss.
    pub skipped: u64,
    /// Heartbeats taken from the peer.
    pub taken: u64,
    /// Heartbeats refused for their flush proof.
    pub unproven: u64,
    /// Whether a configured detector of the pair's own judges the peer.
    pub configured: bool,
    /// Whether a margin judges the peer: its own configuration's, or, while it has none, the one
    /// what its node measured of its links configures for it (`docs/timing.md` §3, item 10).
    pub judged: bool,
    /// The interval the peer's heartbeats come at, or a longer one this node asked it to move to,
    /// once one has come, whether or not a margin judges: how long the evidence a pair is judged
    /// from may go without moving, which an owner waiting on that evidence must wait past.
    pub interval: Option<Duration>,
    /// The interval the peer's heartbeats come at, or a longer one this node asked it to move to,
    /// and the margin in force, `η + α`: how long past a heartbeat's expected arrival less its
    /// delay the peer is trusted, the election law's base (`docs/timing.md` §2.3); `None` while no
    /// margin judges.
    pub freshness: Option<Duration>,
    /// Configurations made.
    pub configurations: u64,
    /// Suspicions of the peer.
    pub suspicions: u64,
    /// The allowance for them: over every heartbeat taken that ended a gap a freshness point
    /// judged, the bound that point put on the heartbeat's coming past it, at the margin in force
    /// on the arrivals as they stood (the node's evidence, scaled to the link's window and widened
    /// by its own, while the margin was the evidence's): the expected number of suspicions were the
    /// peer alive throughout, since each heartbeat taken is the one mistake its predecessor's
    /// freshness point can make (`docs/timing.md` §2.2).
    pub allowance: f64,
}

/// Where the crate's work goes: the owner queues heartbeats on the plane, makes the liveness
/// write, and acts on trust changes.
pub trait Output {
    /// A heartbeat to queue on the plane for `peer`, now.
    fn heartbeat(&mut self, peer: PeerId, message: &[u8]);
    /// Make a write on the log device and flush it; report it with [`Write::Liveness`].
    fn flush(&mut self);
    /// A change in trust.
    fn change(&mut self, change: Change);
}

/// The durable writes seen: their count and the latest one's completion.
#[derive(Clone, Copy, Debug, Default)]
struct Durable {
    count: u64,
    latest_ns: Option<u64>,
}

/// A node's pairs: what it sends each peer and what it believes of each.
pub struct Liveness {
    local: PeerId,
    run: u64,
    max_peers: usize,
    pairs: BTreeMap<PeerId, Pair>,
    /// `G`: the lateness of the timed waits the owner reports ([`on_wait`](Self::on_wait)).
    timer: Lateness,
    /// The wake the latest poll asked of the owner, and the most the owner has been past one: what
    /// a bound the owner states adds (Lifeguard's local health, measured: `docs/timing.md` §2.7),
    /// its own stalls and all.
    asked: Option<u64>,
    late_most: u64,
    flushes: Flushes,
    durable: Durable,
    /// Whether a liveness write is out.
    flushing: bool,
    exposure: Exposure,
    last_poll_ns: Option<u64>,
    /// The earliest heartbeat due and freshness point, as the last poll left them: what
    /// [`wake`](Self::wake) answers without walking the pairs, which an owner asks after every call.
    next_wake: Option<u64>,
    message: [u8; MAX_BYTES],
    /// The node's pool of its links (`docs/timing.md` §3, item 10): one more estimator, fed the
    /// latenesses of every link that has no configuration of its own, the links it exists to
    /// judge, and of every link until it has its evidence (hyper-swim's rule, §2.7), one an
    /// arrival. Fed by the young links alone, a pool whose links configured before it could
    /// measure would never measure, and a peer never heard from would never be judged. Built on
    /// the first lateness fed: one allocation, boxed with its ring.
    pool: Option<Box<LinkEstimator>>,
    pool_seq: u64,
    /// What the pool measured, as of the latest lateness fed that it could say it from. A refusal
    /// leaves it in force, as `LinkEstimator::configure` leaves its margin and hyper-swim's pool
    /// its verdict (`docs/timing.md` §2.7): more of a stall can make the pool's `τ_int` unmeasured
    /// again, and a pair judged by its margin would then judge nothing, a peer it had suspected
    /// before it was heard never trusted again on its heartbeats.
    pool_measured: Option<Arrivals>,
    /// The widest arrivals the node's links configured their own detectors from, each measure
    /// the largest over the pairs that have a configuration: kept at each configuration made and
    /// at each pair let go, so it is read without walking the pairs.
    configured: Option<Arrivals>,
    /// What the node has measured of its links, the wider of `pool_measured` and `configured`
    /// ([`renew_evidence`](Self::renew_evidence)): kept where either changes, so a heartbeat and a
    /// poll read it, and only for a pair with no configuration of its own. Computed at every
    /// heartbeat, it was three maxima a heartbeat that every configured pair threw away
    /// (`docs/benchmarks.md`, "The node's evidence, kept").
    evidence: Option<Arrivals>,
}

/// The margins a pair that waits for one takes from its node's pool: its own link's, or, for a
/// peer it has not heard from, the first freshness point's. The MTBF is read once a poll, and only
/// for such a pair.
fn pool_margins(
    pair: &mut Pair,
    pool: &Arrivals,
    granularity: Duration,
    floor: Option<Duration>,
    mtbf: &mut Option<Option<Duration>>,
    exposure: &Exposure,
) {
    if pair.wants_pool_margin() {
        let mtbf = *mtbf.get_or_insert_with(|| exposure.mtbf());
        pair.pool_margin(pool, granularity, mtbf);
    }
    if let Some(floor) = floor
        && pair.wants_unheard_margin()
    {
        let mtbf = *mtbf.get_or_insert_with(|| exposure.mtbf());
        pair.judge_unheard(pool, floor, granularity, mtbf);
    }
}

/// The earlier of `wake` and `pair`'s next heartbeat due after `now_ns` and freshness point.
fn earliest(wake: Option<u64>, pair: &Pair, now_ns: u64) -> Option<u64> {
    [
        wake,
        pair.next_due().filter(|due| *due > now_ns),
        pair.deadline(),
    ]
    .into_iter()
    .flatten()
    .min()
}

impl Liveness {
    /// A node with no pairs.
    pub fn new(settings: Settings) -> Result<Self, Refusal> {
        if settings.max_peers == 0 {
            return Err(Refusal::Limits);
        }
        Ok(Self {
            local: settings.local,
            run: settings.run,
            max_peers: settings.max_peers,
            pairs: BTreeMap::new(),
            timer: Lateness::new(settings.resolution),
            asked: None,
            late_most: 0,
            flushes: Flushes::new(),
            durable: Durable::default(),
            flushing: false,
            exposure: settings.history,
            last_poll_ns: None,
            next_wake: None,
            message: [0; MAX_BYTES],
            pool: None,
            pool_seq: 0,
            pool_measured: None,
            configured: None,
            evidence: None,
        })
    }

    /// What the node has measured of its links, which judges a link with no configuration of its
    /// own (`docs/timing.md` §2.8, "Judged before its own evidence"), renewed after either of its
    /// parts moved: the wider of its pool's measure and the widest configured link's
    /// (`pair::wider`). A configured link measured its own behaviour at one interval with its
    /// `τ_int` within Madras and Sokal's window, the evidence its estimator refuses to configure
    /// without; the pool, fed by the same links' latenesses, mixes intervals and links, and once
    /// the links configure it is fed at the intervals they asked, seconds apart on a coarse timer.
    /// Under the pool's premise, that the stalls are the hosts' (§2.6), the widest configured link
    /// bounds what the pool stands for: the pool's latenesses are a mixture of its links', so their
    /// chance of passing a margin is a weighted mean of the links' chances, each at most its own
    /// link's bound and so at most the widest's, the bound growing with each measure.
    fn renew_evidence(&mut self) {
        self.evidence = pair::wider(self.pool_measured, self.configured);
    }

    /// The widest arrivals the pairs' configurations were made from.
    fn widest_configured(&self) -> Option<Arrivals> {
        self.pairs
            .values()
            .filter_map(|pair| pair.configuration().map(|configured| configured.link))
            .fold(None, |widest, link| pair::wider(widest, Some(link)))
    }

    /// Feeds the pool a link's lateness.
    fn feed_pool(&mut self, lateness: i64, interval: Duration) {
        let Some(granularity) = self.granularity() else {
            return;
        };
        if self.pool.is_none() {
            self.pool = LinkEstimator::new(interval, granularity, None)
                .ok()
                .map(Box::new);
        }
        let Some(pool) = self.pool.as_mut() else {
            return;
        };
        pool.set_granularity(granularity);
        self.pool_seq = self.pool_seq.saturating_add(1);
        // A lateness past what a window can sum is refused, as any estimator refuses such an
        // offset.
        let _ = pool.on_lateness(self.pool_seq, lateness);
        if let Ok(measured) = pool.arrivals() {
            self.pool_measured = Some(measured);
            self.renew_evidence();
        }
    }

    /// One more group shared with `peer`: the pair's stream starts with its first.
    pub fn attach(&mut self, peer: PeerId) -> Result<(), Refusal> {
        if peer == self.local {
            return Err(Refusal::FromSelf);
        }
        if !self.pairs.contains_key(&peer) && self.pairs.len() >= self.max_peers {
            return Err(Refusal::TooManyPeers);
        }
        let pair = self.pairs.entry(peer).or_insert_with(Pair::new);
        pair.groups = pair.groups.checked_add(1).ok_or(Refusal::TooManyGroups)?;
        Ok(())
    }

    /// One group fewer shared with `peer`: with its last the pair's stream ends and its state
    /// goes. A peer let go while suspected counts as a failure in the MTBF's evidence.
    pub fn detach(&mut self, peer: PeerId) -> Result<(), Refusal> {
        let pair = self.pairs.get_mut(&peer).ok_or(Refusal::UnknownPeer)?;
        pair.groups = pair.groups.saturating_sub(1);
        if pair.groups == 0 {
            if pair.trust() == Trust::Suspected {
                self.exposure.on_failure();
            }
            self.pairs.remove(&peer);
            // Its configuration may have been the widest.
            self.configured = self.widest_configured();
            self.renew_evidence();
            // Its wake may have been the earliest.
            self.next_wake = self.wake_after(self.last_poll_ns.unwrap_or(0));
        }
        Ok(())
    }

    /// The expected time from a suspicion of `peer` to a new leader, `T_E`, which its detector is
    /// configured to charge each election (`hyper_timing::Costs::election`): the election law's
    /// span over the groups whose leader that node is (the mean over them minimizes their summed
    /// unavailability, which is linear in `T_E`).
    pub fn set_election(&mut self, peer: PeerId, election: Duration) -> Result<(), Refusal> {
        let pair = self.pairs.get_mut(&peer).ok_or(Refusal::UnknownPeer)?;
        pair.election = Some(election);
        Ok(())
    }

    /// A write on this node's log became durable: started at `started_ns`, durable at
    /// `durable_ns`. Every durable completion is evidence a heartbeat may carry, and its time
    /// feeds `E[flush]`, the sender's floor.
    pub fn on_durable(&mut self, write: Write, started_ns: u64, durable_ns: u64) {
        self.durable.count = self.durable.count.saturating_add(1);
        self.durable.latest_ns = Some(
            self.durable
                .latest_ns
                .map_or(durable_ns, |l| l.max(durable_ns)),
        );
        // A full fold keeps its mean.
        let _ = self.flushes.on_flush(started_ns, durable_ns);
        if write == Write::Liveness {
            self.flushing = false;
        }
    }

    /// A liveness message from `from`, received at `arrival_ns` on this node's clock (the kernel's
    /// stamp where the owner has one). The peer is judged at the arrival first, so a freshness
    /// point that passed before the message came is a suspicion in whatever order messages and
    /// polls are fed; trust changes go to `out`. A heartbeat from a later run of the peer counts
    /// as a failure in the MTBF's evidence, the peer having restarted, and one from an earlier
    /// run than the latest taken is refused as stale.
    pub fn on_heartbeat(
        &mut self,
        from: PeerId,
        message: &[u8],
        arrival_ns: u64,
        out: &mut impl Output,
    ) -> Result<(), Refusal> {
        if from == self.local {
            return Err(Refusal::FromSelf);
        }
        let beat = Heartbeat::decode(message)?;
        let granularity = self.granularity();
        let pair = self.pairs.get_mut(&from).ok_or(Refusal::UnknownPeer)?;
        let context = pair::Context {
            granularity,
            exposure: &self.exposure,
            evidence: self.evidence.as_ref(),
        };
        let mut changes = [None, None, None];
        let mut taken = pair::Taken::default();
        let outcome = pair.take(from, &beat, arrival_ns, &context, &mut changes, &mut taken);
        for change in changes.into_iter().flatten() {
            out.change(change);
        }
        // The heartbeat moved this pair's freshness point; the others are as the last poll left
        // them. The earliest of the last poll's wake and this pair's is never past the earliest of
        // them all, so an owner that asks before it polls is woken in time, if early.
        self.next_wake = [self.next_wake, pair.deadline()]
            .into_iter()
            .flatten()
            .min();
        if taken.restarted {
            self.exposure.on_failure();
        }
        if taken.configured {
            self.configured = self.widest_configured();
            self.renew_evidence();
        }
        if let Some(lateness) = taken.lateness
            && (!taken.own || self.pool_measured.is_none())
        {
            self.feed_pool(lateness, Duration::from_nanos(beat.interval_ns));
        }
        outcome
    }

    /// Advances to `now_ns`: suspects peers whose freshness passed, sends the heartbeats due that
    /// a flush proves, and asks for a flush where none does. Call it at every [`wake`](Self::wake)
    /// and after each message or completion fed in. Every message stamped before `now_ns` must be
    /// fed first, or a peer whose heartbeat came is suspected: the owner reads its clock for
    /// `now_ns`, then reads its socket, which holds by then every datagram stamped before it, then
    /// polls. Read the other way round, a stop between the socket and the clock leaves the
    /// datagrams of the stop unread (`tests/processes.rs` traced one, a stopped member's).
    pub fn poll(&mut self, now_ns: u64, out: &mut impl Output) {
        if let Some(at) = self.asked
            && now_ns >= at
        {
            self.late_most = self.late_most.max(now_ns.saturating_sub(at));
            self.asked = None;
        }
        self.expose(now_ns);
        let mut wants_flush = false;
        let granularity = self.granularity();
        let floor = self.floor_at(granularity);
        let sender = pair::Sender {
            local_run: self.run,
            floor,
            granularity,
            durable_count: self.durable.count,
            durable_ns: self.durable.latest_ns,
        };
        // The MTBF only for a pair that waits for a margin from the node's evidence, which a
        // configured node has none of: read at every poll, it was a float division and a
        // conversion a poll.
        let mut mtbf = None;
        // The wake to ask, gathered in the same walk: each pair is final once it has been judged
        // and has sent, and a second walk of the map was a tenth of a poll.
        let mut wake: Option<u64> = None;
        for (&peer, pair) in &mut self.pairs {
            pair.attached(now_ns);
            if let (Some(evidence), Some(granularity)) = (&self.evidence, granularity) {
                pool_margins(
                    pair,
                    evidence,
                    granularity,
                    floor,
                    &mut mtbf,
                    &self.exposure,
                );
            }
            if let Some(change) = pair.judge(peer, now_ns) {
                out.change(change);
            }
            match pair.send(&sender, now_ns, &mut self.message) {
                pair::Sent::Message(length) => {
                    if let Some(message) = self.message.get(..length) {
                        out.heartbeat(peer, message);
                    }
                }
                pair::Sent::NeedsFlush => wants_flush = true,
                pair::Sent::Nothing => {}
            }
            wake = earliest(wake, pair, now_ns);
        }
        if wants_flush && !self.flushing {
            self.flushing = true;
            out.flush();
        }
        self.asked = wake;
        self.next_wake = if self.last_poll_ns == Some(now_ns) {
            wake
        } else {
            // A poll fed a time before an earlier one's: the wake is past the latest.
            self.wake_after(self.last_poll_ns.unwrap_or(0))
        };
    }

    /// The node time watched since the last poll: each pair's peer, for the time between polls.
    fn expose(&mut self, now_ns: u64) {
        if let Some(last) = self.last_poll_ns {
            let peers = u32::try_from(self.pairs.len()).unwrap_or(u32::MAX);
            self.exposure.on_exposure(
                Duration::from_nanos(now_ns.saturating_sub(last)).saturating_mul(peers),
            );
        }
        self.last_poll_ns = Some(self.last_poll_ns.map_or(now_ns, |last| last.max(now_ns)));
    }

    /// When to [`poll`](Self::poll) next: the earliest heartbeat due and freshness point. A
    /// heartbeat waiting on a flush is sent when the flush is reported. Kept by each poll, and by
    /// each heartbeat taken no later than its pair's new freshness point, so it costs nothing to
    /// ask; an owner that polls after every message, as [`poll`](Self::poll) asks, gets the
    /// earliest exactly.
    pub fn wake(&self) -> Option<u64> {
        self.next_wake
    }

    fn wake_after(&self, now_ns: u64) -> Option<u64> {
        self.pairs
            .values()
            .flat_map(|pair| [pair.next_due().filter(|due| *due > now_ns), pair.deadline()])
            .flatten()
            .min()
    }

    /// The sender's stability floor, `E[flush] + G`: `None` before a flush is measured.
    pub fn floor(&self) -> Option<Duration> {
        self.floor_at(self.granularity())
    }

    /// The floor at the granularity `G` already read.
    fn floor_at(&self, granularity: Option<Duration>) -> Option<Duration> {
        let flush = self.flushes.mean()?;
        Some(flush.saturating_add(granularity.unwrap_or(Duration::ZERO)))
    }

    /// `E[flush]`, the mean time from a log write's start to its durability, once one is reported:
    /// the vote round's flush in the election law's ballot (`hyper_timing::Ballot::measure`).
    pub fn flush_mean(&self) -> Option<Duration> {
        self.flushes.mean()
    }

    /// A wait the owner began before `deadline_ns`, the stream's [`wake`](Self::wake), that ended
    /// at `woke_ns`, at or past it, on the clock the stream is polled by, whatever ended it: the
    /// deadline, or a message, a completion or a command that came after it. Reported as the wait
    /// ends, before anything it brought is fed. `G` is the mean lateness of these waits
    /// (`docs/timing.md` §2.4): how late past its wakes the stream is polled while its owner waits
    /// for them, a stop or a frozen host included, and nothing the owner's own work adds. A wait
    /// that ended before its deadline reached nothing, and a wake the owner came to late because
    /// its thread was in its own work began no wait before it: neither is a sample. Taken from
    /// every poll past a wake, such wakes made `G` the owner's stalls, 4.8–7.4 s in a run whose
    /// timer was late by milliseconds; counted only where the deadline ended them, the waits of an
    /// owner woken past its wakes by messages before its timer fired counted nothing, and its
    /// stream refused every heartbeat as unmeasured (§2.9). A wait for a deadline other than the
    /// wake asked is not the stream's and measures nothing.
    pub fn on_wait(&mut self, deadline_ns: u64, woke_ns: u64) {
        if self.next_wake == Some(deadline_ns) {
            // A full fold keeps its mean: `G` stands as measured.
            let _ = self.timer.on_wait(deadline_ns, woke_ns);
        }
    }

    /// `G`, the mean lateness of the owner's waits for the stream's wakes
    /// ([`on_wait`](Self::on_wait)), once one is measured, and never below the clock's resolution
    /// ([`Settings::resolution`]).
    pub fn granularity(&self) -> Option<Duration> {
        self.timer.granularity()
    }

    /// The latest the owner has polled past a wake asked, or is past one at `now_ns`.
    pub fn latest_wake(&self, now_ns: u64) -> Duration {
        let past = self.asked.map_or(0, |at| now_ns.saturating_sub(at));
        Duration::from_nanos(past.max(self.late_most))
    }

    /// What this node believes of `peer`, if they share a group.
    pub fn trust(&self, peer: PeerId) -> Option<Trust> {
        self.pairs.get(&peer).map(Pair::trust)
    }

    /// The peers this node suspects.
    pub fn suspected(&self) -> impl Iterator<Item = PeerId> + '_ {
        self.pairs
            .iter()
            .filter(|(_, pair)| pair.trust() == Trust::Suspected)
            .map(|(peer, _)| *peer)
    }

    /// The detector judging `peer` (the election law's base is its `current.interval +
    /// current.margin`, `hyper_timing::ElectionTiming::derive`).
    pub fn configuration(&self, peer: PeerId) -> Option<Configuration> {
        self.pairs.get(&peer).and_then(Pair::configuration)
    }

    /// The round trip to `peer` the echoes measure, network and kernel only (each side's
    /// schedule and flush taken out): a path for the election law's ballot.
    pub fn round_trip(&self, peer: PeerId) -> Option<&ExchangeRtt> {
        self.pairs.get(&peer).map(Pair::round_trip)
    }

    /// What the pair with `peer` has done and promised.
    pub fn report(&self, peer: PeerId) -> Option<PairReport> {
        self.pairs.get(&peer).map(Pair::report)
    }

    /// The MTBF the detectors are configured with, once there is exposure.
    pub fn mtbf(&self) -> Option<Duration> {
        self.exposure.mtbf()
    }
}
