//! The network (`docs/sim.md` §3.4, step S-2): one model for the crates' typed messages and for
//! their datagrams. A payload states its size, so byte bounds and serialization times apply to
//! both.
//!
//! Per directed pair, a [`Path`]: propagation delay with jitter, in order or reordering; a
//! Gilbert–Elliott loss process per flow; a bottleneck [`Link`] with a drop-tail queue and an active
//! queue manager, shared by the flows through it; a path MTU; and a [`Nat`] whose mapping expires.
//! The model is focal's path model (focal 27 §3.1 P7, itself slates' `SimPath`), carried into the
//! world. Every draw is from a named stream of the flow it is for, so adding a flow leaves every
//! other flow's draws unchanged (§3.1), and every arrival is an event of the world, which its
//! strategy chooses like any other.
//!
//! **In flight.** The network holds what is in flight in an arena of [`NetLimits::messages`] slots
//! and [`NetLimits::bytes`] bytes, the capacity `C` of §7; the world holds an event naming the slot
//! ([`Ticket`]). Past the capacity the oldest message in flight is lost and counted, as slates' and
//! hyper-raft's harnesses lose it: a loss the protocol must tolerate (Raft thesis §3.3). A test
//! whose overflow losses pass its drawn losses has stated `C` too small for its schedule.
//!
//! A message is **duplicated** by being delivered and kept: it stays in its slot and arrives again
//! after a fresh propagation delay (hyper-raft's `keep`), so a duplicate adds nothing to the bound.
//! A **partition** cuts at the send and at the arrival: a message in flight when its pair is cut is
//! lost when it arrives (focal's network).
//!
//! Every probability is in parts per million, so a profile is exact and replayable.

use std::collections::{BTreeMap, BTreeSet, VecDeque};

use crate::clock::PPM;
use crate::error::SimError;
use crate::world::{NodeId, StreamId, World};

/// Nanoseconds in a second.
const NANOS_PER_SECOND: u128 = 1_000_000_000;
/// Bits in a byte.
const BITS_PER_BYTE: u128 = 8;
/// A millisecond in nanoseconds, the unit of the named profiles.
const MILLISECOND: u64 = 1_000_000;

/// The two-state Gilbert–Elliott channel (Gilbert, BSTJ 1960; Elliott, BSTJ 1963): independent loss
/// with one state, bursty loss with a bad state entered and left per message or in time
/// (`docs/research/burst-loss.md`). Its state is kept per directed flow.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Loss {
    chain: Chain,
    good_loss_ppm: u32,
    bad_loss_ppm: u32,
}

/// How the channel moves between its states.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Chain {
    /// One step a message: the bad state entered with `good_to_bad_ppm` and left with
    /// `bad_to_good_ppm`, whatever the time between messages.
    PerMessage {
        good_to_bad_ppm: u32,
        bad_to_good_ppm: u32,
    },
    /// In time: a stay in the good state lasts `mean_good_ns` on average, in the bad state
    /// `mean_bad_ns`, one step a nanosecond, so whether a message finds a burst depends on the time
    /// since the flow's last message.
    InTime { mean_good_ns: u64, mean_bad_ns: u64 },
}

impl Default for Loss {
    fn default() -> Self {
        Self::NONE
    }
}

/// One in fixed point with 64 fractional bits, the probabilities of the chain in time.
const Q64_ONE: u128 = 1 << 64;
/// The bits of the draw that decides the state in time: a probability in 32 fractional bits, finer
/// than the parts per million of the other draws, so a copy microseconds behind its original is
/// told from one sent with it.
const STATE_BITS: u32 = 32;

impl Loss {
    /// No loss.
    pub const NONE: Self = Self::random(0);

    /// Independent (Bernoulli) loss of `loss_ppm` a message.
    pub const fn random(loss_ppm: u32) -> Self {
        Self {
            chain: Chain::PerMessage {
                good_to_bad_ppm: 0,
                bad_to_good_ppm: PPM,
            },
            good_loss_ppm: loss_ppm,
            bad_loss_ppm: loss_ppm,
        }
    }

    /// A burst is entered with `enter_ppm` a message and left with `leave_ppm`, so it lasts
    /// `PPM / leave_ppm` messages on average; it loses `burst_loss_ppm` of what is sent inside it
    /// and nothing outside.
    pub const fn bursty(enter_ppm: u32, leave_ppm: u32, burst_loss_ppm: u32) -> Self {
        Self {
            chain: Chain::PerMessage {
                good_to_bad_ppm: enter_ppm,
                bad_to_good_ppm: leave_ppm,
            },
            good_loss_ppm: 0,
            bad_loss_ppm: burst_loss_ppm,
        }
    }

    /// Bursts in time: a burst lasts `mean_burst_ns` on average and the gap between bursts
    /// `mean_gap_ns`, and a burst loses `burst_loss_ppm` of what is sent inside it and nothing
    /// outside. A flow's first message finds a burst with the chain's stationary probability,
    /// `burst / (burst + gap)`; a later one, `δ` after the flow's previous message, with
    /// `π_b + (s − π_b)·(1 − 1/gap − 1/burst)^δ` where `s` is 1 in a burst and 0 outside
    /// (`docs/research/burst-loss.md` §2). Either mean zero is no process, refused.
    pub fn bursty_in_time(
        mean_burst_ns: u64,
        mean_gap_ns: u64,
        burst_loss_ppm: u32,
    ) -> Result<Self, SimError> {
        if mean_burst_ns == 0 || mean_gap_ns == 0 {
            return Err(SimError::NotALossProcess);
        }
        Ok(Self {
            chain: Chain::InTime {
                mean_good_ns: mean_gap_ns,
                mean_bad_ns: mean_burst_ns,
            },
            good_loss_ppm: 0,
            bad_loss_ppm: burst_loss_ppm,
        })
    }

    /// A lossless path draws nothing.
    const fn is_lossless(&self) -> bool {
        self.good_loss_ppm == 0 && self.bad_loss_ppm == 0
    }
}

/// The chance, in [`STATE_BITS`] fractional bits, that the chain in time is in its bad state
/// `elapsed` nanoseconds after it was in `was_bad` (or at its stationary probability with no
/// previous message): `π_b + (s − π_b)·λ^δ`, with `λ = 1 − 1/G − 1/B` a nanosecond, in 64
/// fractional bits, raised by squaring. Every step is integer arithmetic, so a run replays on every
/// host.
fn bad_in_time(mean_good_ns: u64, mean_bad_ns: u64, previous: Option<(bool, u64)>) -> u64 {
    let good = u128::from(mean_good_ns.max(1));
    let bad = u128::from(mean_bad_ns.max(1));
    // π_b = B / (G + B), in 64 fractional bits.
    let stationary = bad
        .checked_shl(64)
        .and_then(|scaled| scaled.checked_div(good.saturating_add(bad)))
        .unwrap_or(0);
    let chance = match previous {
        None => stationary,
        Some((was_bad, elapsed)) => {
            let rate = Q64_ONE
                .checked_div(good)
                .unwrap_or(0)
                .saturating_add(Q64_ONE.checked_div(bad).unwrap_or(0));
            let decay = power(Q64_ONE.saturating_sub(rate), elapsed);
            if was_bad {
                // π_b + (1 − π_b)·λ^δ
                let rest = Q64_ONE.saturating_sub(stationary);
                stationary.saturating_add(multiply(rest, decay))
            } else {
                // π_b·(1 − λ^δ)
                multiply(stationary, Q64_ONE.saturating_sub(decay))
            }
        }
    };
    chance
        .checked_shr(64_u32.saturating_sub(STATE_BITS))
        .and_then(|bits| u64::try_from(bits).ok())
        .unwrap_or(1 << STATE_BITS)
}

/// The product of two numbers in 64 fractional bits, at most one each, truncated.
fn multiply(a: u128, b: u128) -> u128 {
    // Below 2^64 · 2^64 unless both are one, whose product is one.
    a.checked_mul(b)
        .and_then(|product| product.checked_shr(64))
        .unwrap_or(Q64_ONE)
}

/// `base^exponent` in 64 fractional bits, `base` at most one: by squaring, one step a bit of the
/// exponent, at most 64.
fn power(mut base: u128, mut exponent: u64) -> u128 {
    let mut result = Q64_ONE;
    while exponent != 0 {
        if exponent & 1 == 1 {
            result = multiply(result, base);
        }
        exponent = exponent.checked_shr(1).unwrap_or(0);
        if exponent != 0 {
            base = multiply(base, base);
        }
    }
    result
}

/// What a link's queue manager does with a message that finds the queue long, below the capacity
/// that drops it (RFC 7567): one that is ECN-capable is marked Congestion Experienced and goes on,
/// one that is not is dropped where it would be marked (RFC 3168 §5).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Marking {
    /// Drop-tail alone.
    #[default]
    Off,
    /// A step at a threshold of the queue, as DCTCP's switches mark (RFC 8257 §3.1): a message
    /// that finds more than `threshold_bytes` ahead of it.
    Step {
        /// The backlog past which a message is marked.
        threshold_bytes: u64,
    },
    /// CoDel (RFC 8289) marking where it would drop: once the messages' sojourn has stayed above
    /// `target_ns` for an `interval_ns`, one is marked, and the next at `interval/√count` apart
    /// while it stays above.
    CoDel {
        /// The sojourn CoDel holds the queue to.
        target_ns: u64,
        /// How long the sojourn may stay above the target before a mark.
        interval_ns: u64,
    },
}

/// A bottleneck: messages are serialized at its rate one after another and wait in its drop-tail
/// queue while it is busy. Several paths may name one link, so their flows compete for it (the
/// dumbbell of RFC 5166).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Link {
    /// The rate the link serializes at.
    pub rate_bits_per_second: u64,
    /// A message arriving when the backlog plus itself exceeds this is dropped.
    pub queue_bytes: u64,
    /// What the queue's manager does below that capacity.
    pub marking: Marking,
}

impl Link {
    /// A link whose queue only drops what overflows it.
    pub const fn drop_tail(rate_bits_per_second: u64, queue_bytes: u64) -> Self {
        Self {
            rate_bits_per_second,
            queue_bytes,
            marking: Marking::Off,
        }
    }
}

/// CoDel's state on one link (RFC 8289 §5). A FIFO link's messages leave in the order they came and
/// each one's start of transmission is known when it is queued, so the state advances at each
/// message's dequeue time, in order, as the queue is entered.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct CoDel {
    first_above: Option<u64>,
    marking: bool,
    next: u64,
    count: u64,
    last_count: u64,
}

/// What CoDel is asked of one message.
#[derive(Clone, Copy, Debug)]
struct Sojourn {
    /// When it leaves the queue.
    dequeue: u64,
    /// How long it waited.
    waited: u64,
    /// The bytes ahead of it.
    ahead: u64,
    /// Its own size: the one message a queue may hold without being long (RFC 8289 §5.2's "queue
    /// holds more than one MTU").
    bytes: u64,
}

impl CoDel {
    /// Whether the sojourn has stayed above the target for an interval (RFC 8289 §5.2).
    fn above(&mut self, sojourn: Sojourn, target_ns: u64, interval_ns: u64) -> bool {
        if sojourn.waited < target_ns || sojourn.ahead <= sojourn.bytes {
            self.first_above = None;
            return false;
        }
        match self.first_above {
            None => {
                self.first_above = Some(sojourn.dequeue.saturating_add(interval_ns));
                false
            }
            Some(first) => sojourn.dequeue >= first,
        }
    }

    /// Whether the message leaving at `sojourn.dequeue` is marked.
    fn judge(&mut self, sojourn: Sojourn, target_ns: u64, interval_ns: u64) -> bool {
        let above = self.above(sojourn, target_ns, interval_ns);
        if self.marking {
            return self.while_marking(above, sojourn.dequeue, interval_ns);
        }
        if !above {
            return false;
        }
        self.marking = true;
        // RFC 8289 §5.5: a state left recently resumes near its rate.
        let delta = self.count.saturating_sub(self.last_count);
        let recent = sojourn.dequeue.saturating_sub(self.next) < interval_ns.saturating_mul(16);
        self.count = if delta > 1 && recent { delta } else { 1 };
        self.next = control_law(sojourn.dequeue, self.count, interval_ns);
        self.last_count = self.count;
        true
    }

    /// In the marking state: marks at the control law's times while the sojourn stays above.
    fn while_marking(&mut self, above: bool, dequeue: u64, interval_ns: u64) -> bool {
        if !above {
            self.marking = false;
            return false;
        }
        if dequeue < self.next {
            return false;
        }
        self.count = self.count.saturating_add(1);
        self.next = control_law(self.next, self.count, interval_ns);
        true
    }
}

/// `t + interval/√count` (RFC 8289 §5.3), the root in 16 fractional bits: `√(count·2³²) = √count·2¹⁶`.
fn control_law(at: u64, count: u64, interval_ns: u64) -> u64 {
    let step = u128::from(count.max(1))
        .checked_shl(32)
        .map(u128::isqrt)
        .zip(u128::from(interval_ns).checked_shl(16))
        .and_then(|(root, scaled)| scaled.checked_div(root))
        .and_then(|step| u64::try_from(step).ok())
        .unwrap_or(interval_ns);
    at.saturating_add(step)
}

/// A link of the network, by its place.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct LinkId(usize);

/// A message on a link: when its transmission starts and ends, and its size.
#[derive(Clone, Copy, Debug)]
struct Queued {
    starts: u64,
    departs: u64,
    bytes: u64,
}

/// A link and what it is sending.
#[derive(Clone, Debug)]
struct LinkState {
    link: Link,
    busy_until: u64,
    /// What the transmitter has accepted and not finished sending, in order. Each keeps the timing
    /// of the rate it was accepted at.
    queued: VecDeque<Queued>,
    codel: CoDel,
}

impl LinkState {
    /// Rounded up, so a message always takes time on a finite link.
    fn serialization_ns(&self, bytes: u64) -> u64 {
        let rate = u128::from(self.link.rate_bits_per_second.max(1));
        let bits = u128::from(bytes).saturating_mul(BITS_PER_BYTE);
        u64::try_from(bits.saturating_mul(NANOS_PER_SECOND).div_ceil(rate)).unwrap_or(u64::MAX)
    }

    /// What the transmitter still has to send at `now`: every message waiting, and the part of the
    /// one being sent that has not left.
    fn backlog_bytes(&mut self, now: u64) -> u64 {
        while self.queued.front().is_some_and(|head| head.departs <= now) {
            self.queued.pop_front();
        }
        self.queued.iter().fold(0_u64, |backlog, message| {
            let whole = message.departs.saturating_sub(message.starts);
            let left = message.departs.saturating_sub(now.max(message.starts));
            let bytes = u128::from(message.bytes)
                .saturating_mul(u128::from(left))
                .checked_div(u128::from(whole))
                .unwrap_or(0);
            backlog.saturating_add(u64::try_from(bytes).unwrap_or(u64::MAX))
        })
    }

    /// Whether the queue's manager finds the queue long for a message of `bytes` that would start
    /// at `starts` behind `backlog`.
    fn congested(&mut self, now: u64, starts: u64, backlog: u64, bytes: u64) -> bool {
        match self.link.marking {
            Marking::Off => false,
            Marking::Step { threshold_bytes } => backlog > threshold_bytes,
            Marking::CoDel {
                target_ns,
                interval_ns,
            } => {
                let sojourn = Sojourn {
                    dequeue: starts,
                    waited: starts.saturating_sub(now),
                    ahead: backlog,
                    bytes,
                };
                self.codel.judge(sojourn, target_ns, interval_ns)
            }
        }
    }
}

/// A delay measured on a host: its quantiles at probabilities in parts per million, from zero to
/// [`PPM`] ascending, drawn from by the inverse transform, linear between neighbouring points
/// (hyper-timing-trace's grid; hyper-liveness's measured worlds). The draw is one integer below
/// `PPM` and the interpolation is exact in integers, so a run replays on every host.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Measured {
    grid: &'static [u32],
    values: &'static [u64],
}

impl Measured {
    /// The quantiles `values`, nanoseconds, at the probabilities `grid`, parts per million: as
    /// many of each, at least two, the grid from zero to `PPM` and rising, the values never
    /// falling. Anything else is no distribution, refused.
    pub fn new(grid: &'static [u32], values: &'static [u64]) -> Result<Self, SimError> {
        let rising = grid
            .windows(2)
            .all(|pair| matches!(pair, [low, high] if low < high));
        let ordered = values
            .windows(2)
            .all(|pair| matches!(pair, [low, high] if low <= high));
        if grid.len() != values.len()
            || grid.len() < 2
            || grid.first() != Some(&0)
            || grid.last() != Some(&PPM)
            || !rising
            || !ordered
        {
            return Err(SimError::NotADistribution);
        }
        Ok(Self { grid, values })
    }

    /// The value at probability `u` parts per million, below `PPM`: linear between the grid's
    /// neighbouring points, rounded to the nearest nanosecond. A harness drawing a measured
    /// quantity of its own (a flush, a timer's lateness) draws `u` from its stream and reads it
    /// here.
    pub fn at(&self, u: u32) -> u64 {
        let segment = self
            .grid
            .partition_point(|point| *point <= u)
            .saturating_sub(1);
        let (Some(&low), Some(&high)) = (
            self.grid.get(segment),
            self.grid.get(segment.saturating_add(1)),
        ) else {
            return self.values.last().copied().unwrap_or(0);
        };
        let (Some(&from), Some(&to)) = (
            self.values.get(segment),
            self.values.get(segment.saturating_add(1)),
        ) else {
            return self.values.last().copied().unwrap_or(0);
        };
        let span = u128::from(high.saturating_sub(low));
        let share = u128::from(u.saturating_sub(low));
        let rise = u128::from(to.saturating_sub(from));
        // Rounded half up: (rise·share + span/2) / span.
        let step = rise
            .saturating_mul(share)
            .saturating_add(span / 2)
            .checked_div(span)
            .unwrap_or(0);
        from.saturating_add(u64::try_from(step).unwrap_or(u64::MAX))
    }
}

/// One directed path.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Path {
    one_way_ns: u64,
    /// Each message propagates in `one_way ± jitter`.
    jitter_ns: u64,
    /// Or as measured, in place of `one_way ± jitter`.
    measured: Option<Measured>,
    /// Whether a message may overtake an earlier one on its flow.
    reorders: bool,
    loss: Loss,
    /// A longer message is dropped: the black hole of RFC 8899.
    mtu: Option<usize>,
    link: Option<LinkId>,
}

impl Default for Path {
    fn default() -> Self {
        Self::NONE
    }
}

impl Path {
    /// Delivers at once and loses nothing.
    pub const NONE: Self = Self::in_order(0, 0);
    /// One switch apart: 0.2 ms ± 0.1 ms one way.
    pub const LAN: Self = Self::in_order(200_000, 100_000);
    /// One continent: 80 ms ± 20 ms one way.
    pub const REGIONAL: Self = Self::in_order(80 * MILLISECOND, 20 * MILLISECOND);
    /// Across the planet on a poor route: 500 ms ± 100 ms one way.
    pub const GEOGRAPHIC: Self = Self::in_order(500 * MILLISECOND, 100 * MILLISECOND);

    /// A path that keeps each flow's order.
    pub const fn in_order(one_way_ns: u64, jitter_ns: u64) -> Self {
        Self {
            one_way_ns,
            jitter_ns,
            measured: None,
            reorders: false,
            loss: Loss::NONE,
            mtu: None,
            link: None,
        }
    }

    /// A path whose messages may overtake one another.
    pub const fn reordering(one_way_ns: u64, jitter_ns: u64) -> Self {
        Self {
            reorders: true,
            ..Self::in_order(one_way_ns, jitter_ns)
        }
    }

    /// A path whose messages propagate as `measured`, keeping each flow's order.
    pub const fn measured(measured: Measured) -> Self {
        Self {
            measured: Some(measured),
            ..Self::in_order(0, 0)
        }
    }

    /// The path with `loss`.
    pub const fn with_loss(self, loss: Loss) -> Self {
        Self { loss, ..self }
    }

    /// The path with an MTU of `mtu` bytes.
    pub const fn with_mtu(self, mtu: usize) -> Self {
        Self {
            mtu: Some(mtu),
            ..self
        }
    }

    /// The path through `link`.
    pub const fn through(self, link: LinkId) -> Self {
        Self {
            link: Some(link),
            ..self
        }
    }

    /// The mean one-way delay.
    pub const fn one_way_ns(&self) -> u64 {
        self.one_way_ns
    }

    /// The jitter either side of it.
    pub const fn jitter_ns(&self) -> u64 {
        self.jitter_ns
    }
}

/// A NAT in front of a node (RFC 4787, endpoint-independent mapping): what is sent to the node
/// arrives only while its mapping is alive, which each message the node sends refreshes. After an
/// expiry the node is unreachable until it sends again: the rebinding of RFC 9000 §9.3 as its peers
/// see it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Nat {
    /// How long a mapping lives without a message from the node.
    pub idle_timeout_ns: u64,
}

/// How a drawn partition divides the nodes (TigerBeetle's packet simulator, `partition_mode`;
/// `docs/research/sim.md`, [PS]).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Split {
    /// A side of a size drawn uniformly from one to all but one, its members drawn.
    UniformSize,
    /// Each node on either side with even chance; a side that comes out empty or whole cuts
    /// nothing.
    UniformPartition,
    /// One node, drawn, alone.
    IsolateSingle,
}

/// Partitions drawn as a run goes (`docs/sim.md` §3.4, VOPR's shapes): at each
/// [`churn`](Net::churn) the harness calls, a partition starts or heals with its stated chance once
/// the present state has lasted its stability. A drawn partition is held apart from the cuts a test
/// makes itself ([`Net::partition`]), so healing one leaves the other.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Partitions {
    /// How the nodes are divided.
    pub split: Split,
    /// Both directions cut, or only those from the drawn side to the rest: a partial partition is
    /// an asymmetric one.
    pub symmetric: bool,
    /// At a churn with no partition, the chance one starts.
    pub start_ppm: u32,
    /// At a churn with a partition, the chance it heals.
    pub heal_ppm: u32,
    /// The least time a partition, or its absence, lasts before it may change.
    pub stable_ns: u64,
}

/// Why a message did not arrive.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Dropped {
    /// Longer than its path's MTU.
    Mtu,
    /// Its link's queue was full, or would have marked a message that cannot be marked.
    Queue,
    /// Its path's loss process lost it.
    Loss,
    /// Its pair was cut, at the send or at the arrival.
    Partition,
    /// The network's capacity: its flows, or what it holds in flight.
    Capacity,
    /// Its receiver's NAT mapping had expired.
    Nat,
}

/// What the network did with a message at its send.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Fate {
    /// It is in flight, to arrive at `at`.
    Arrives {
        /// When, in the world's nanoseconds.
        at: u64,
    },
    /// It was dropped at the send.
    Dropped(Dropped),
}

/// The non-vacuity counters of a run: a test of loss recovery asserts that `dropped_loss` moved, of
/// congestion `dropped_queue`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct NetStats {
    /// Messages sent.
    pub sent: u64,
    /// Messages delivered, duplicates included.
    pub delivered: u64,
    /// Deliveries that kept their message for another.
    pub duplicated: u64,
    /// Dropped for their path's MTU.
    pub dropped_mtu: u64,
    /// Dropped by a link's queue.
    pub dropped_queue: u64,
    /// Lost by a path's loss process.
    pub dropped_loss: u64,
    /// Cut by a partition, at the send or the arrival.
    pub dropped_partition: u64,
    /// Lost to the network's capacity: a flow it had no room for, or the oldest in flight.
    pub dropped_capacity: u64,
    /// Dropped at an expired NAT mapping.
    pub dropped_nat: u64,
    /// The longest backlog any link held.
    pub peak_queue_bytes: u64,
    /// ECN-capable messages a queue manager marked Congestion Experienced.
    pub marked: u64,
}

/// How much a network models and holds (`docs/sim.md` §7). A run that names more is refused;
/// nothing here grows with what is sent.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct NetLimits {
    /// Directed flows with state: their paths, loss states, order and streams.
    pub flows: usize,
    /// Links.
    pub links: usize,
    /// NATs.
    pub nats: usize,
    /// Messages one link holds, whatever their size.
    pub link_messages: usize,
    /// Messages in flight: the capacity `C`.
    pub messages: usize,
    /// Bytes in flight.
    pub bytes: usize,
}

/// A message in flight, by its slot and its send: an event of the world carries it to
/// [`Net::deliver`]. A ticket whose message was lost to the capacity since names nothing.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Ticket {
    slot: u32,
    send: u64,
}

/// A message delivered.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Delivery<P> {
    /// Who sent it.
    pub from: NodeId,
    /// Who it is for.
    pub to: NodeId,
    /// What it carries.
    pub payload: P,
}

/// A directed flow's state.
#[derive(Clone, Copy, Debug, Default)]
struct Flow {
    /// Whether the loss channel is in its bad state.
    bad: bool,
    /// When the loss channel in time last drew its state.
    last_loss_draw: Option<u64>,
    last_arrival: Option<u64>,
    delay: Option<StreamId>,
    loss: Option<StreamId>,
    duplicate: Option<StreamId>,
}

/// A message held in flight.
#[derive(Clone, Debug)]
struct Held<P> {
    send: u64,
    from: NodeId,
    to: NodeId,
    bytes: usize,
    payload: P,
}

/// The messages in flight: slots reused once delivered, at most `messages` of them and `bytes`.
#[derive(Clone, Debug)]
struct Flight<P> {
    slots: Vec<Option<Held<P>>>,
    free: Vec<u32>,
    held: usize,
    bytes: usize,
}

impl<P> Flight<P> {
    fn new() -> Self {
        Self {
            slots: Vec::new(),
            free: Vec::new(),
            held: 0,
            bytes: 0,
        }
    }

    /// The message `ticket` names, if it is still held.
    fn get(&self, ticket: Ticket) -> Option<&Held<P>> {
        let slot = usize::try_from(ticket.slot).ok()?;
        self.slots
            .get(slot)?
            .as_ref()
            .filter(|held| held.send == ticket.send)
    }

    /// Takes the message `ticket` names out of flight.
    fn take(&mut self, ticket: Ticket) -> Option<Held<P>> {
        self.get(ticket)?;
        let slot = usize::try_from(ticket.slot).ok()?;
        let held = self.slots.get_mut(slot)?.take()?;
        self.free.push(ticket.slot);
        self.held = self.held.saturating_sub(1);
        self.bytes = self.bytes.saturating_sub(held.bytes);
        Some(held)
    }

    /// The oldest message in flight: the one with the least send number.
    fn oldest(&self) -> Option<Ticket> {
        self.slots
            .iter()
            .enumerate()
            .filter_map(|(slot, held)| Some((slot, held.as_ref()?.send)))
            .min_by_key(|(_, send)| *send)
            .and_then(|(slot, send)| {
                Some(Ticket {
                    slot: u32::try_from(slot).ok()?,
                    send,
                })
            })
    }

    /// Holds `held`, which there is room for.
    fn hold(&mut self, held: Held<P>) -> Option<Ticket> {
        let send = held.send;
        let bytes = held.bytes;
        let slot = match self.free.pop() {
            Some(slot) => {
                *self.slots.get_mut(usize::try_from(slot).ok()?)? = Some(held);
                slot
            }
            None => {
                let slot = u32::try_from(self.slots.len()).ok()?;
                self.slots.push(Some(held));
                slot
            }
        };
        self.held = self.held.saturating_add(1);
        self.bytes = self.bytes.saturating_add(bytes);
        Some(Ticket { slot, send })
    }
}

/// The network: paths, links, NATs, partitions and what is in flight. `P` is what it carries.
#[derive(Clone, Debug)]
pub struct Net<P> {
    limits: NetLimits,
    default_path: Path,
    pair_paths: BTreeMap<(NodeId, NodeId), Path>,
    links: Vec<LinkState>,
    flows: BTreeMap<(NodeId, NodeId), Flow>,
    /// Each NAT and when the node behind it last sent.
    nats: BTreeMap<NodeId, (Nat, Option<u64>)>,
    blocked: BTreeSet<(NodeId, NodeId)>,
    /// The cuts of the drawn partition in force, if any.
    drawn: BTreeSet<(NodeId, NodeId)>,
    /// When the drawn partition last started or healed.
    changed_at: u64,
    /// The drawn side of a partition being made, held so a churn allocates nothing once grown.
    side: Vec<NodeId>,
    partitions: Option<StreamId>,
    flight: Flight<P>,
    /// Sends so far: each message's number, which orders them for the capacity's rule.
    sends: u64,
    duplicate_ppm: u32,
    stats: NetStats,
}

/// Adds one to a counter, which saturates at `u64::MAX`: past it more changes nothing measurable.
fn bump(counter: &mut u64) {
    *counter = counter.saturating_add(1);
}

/// What one send carries, once its fate is drawn.
struct Departure {
    at: u64,
    path: Path,
    /// Whether a queue's manager marked it Congestion Experienced.
    marked: bool,
}

impl<P: Clone> Net<P> {
    /// A network whose every pair has the zero path, within `limits`.
    pub fn new(limits: NetLimits) -> Self {
        Self {
            limits,
            default_path: Path::NONE,
            pair_paths: BTreeMap::new(),
            links: Vec::new(),
            flows: BTreeMap::new(),
            nats: BTreeMap::new(),
            blocked: BTreeSet::new(),
            drawn: BTreeSet::new(),
            changed_at: 0,
            side: Vec::new(),
            partitions: None,
            flight: Flight::new(),
            sends: 0,
            duplicate_ppm: 0,
            stats: NetStats::default(),
        }
    }

    /// The path of every directed pair without one of its own.
    pub fn set_path(&mut self, path: Path) {
        self.default_path = path;
    }

    /// The path from `from` to `to`.
    pub fn set_pair_path(&mut self, from: NodeId, to: NodeId, path: Path) -> Result<(), SimError> {
        if !self.pair_paths.contains_key(&(from, to)) && self.pair_paths.len() >= self.limits.flows
        {
            return Err(SimError::Full {
                what: "paths",
                bound: self.limits.flows,
            });
        }
        self.pair_paths.insert((from, to), path);
        Ok(())
    }

    /// A bottleneck paths may go through.
    pub fn add_link(&mut self, link: Link) -> Result<LinkId, SimError> {
        if self.links.len() >= self.limits.links {
            return Err(SimError::Full {
                what: "links",
                bound: self.limits.links,
            });
        }
        let id = LinkId(self.links.len());
        self.links.push(LinkState {
            link,
            busy_until: 0,
            queued: VecDeque::new(),
            codel: CoDel::default(),
        });
        Ok(id)
    }

    /// A capacity that drops or recovers mid-run; what is queued drains at the old timing.
    pub fn set_link(&mut self, id: LinkId, link: Link) {
        if let Some(state) = self.links.get_mut(id.0) {
            state.link = link;
        }
    }

    /// A NAT in front of `node`, its mapping alive from `now`, as if the node had just sent.
    pub fn set_nat(&mut self, node: NodeId, nat: Nat, now: u64) -> Result<(), SimError> {
        if !self.nats.contains_key(&node) && self.nats.len() >= self.limits.nats {
            return Err(SimError::Full {
                what: "nats",
                bound: self.limits.nats,
            });
        }
        self.nats.insert(node, (nat, Some(now)));
        Ok(())
    }

    /// Expires `node`'s mapping now.
    pub fn rebind(&mut self, node: NodeId) {
        if let Some((_, last)) = self.nats.get_mut(&node) {
            *last = None;
        }
    }

    /// Each delivery keeps its message for another with probability `ppm`.
    pub fn set_duplicate_ppm(&mut self, ppm: u32) {
        self.duplicate_ppm = ppm.min(PPM);
    }

    /// Cuts the directed pair from `from` to `to`, or heals it.
    pub fn partition(&mut self, from: NodeId, to: NodeId, blocked: bool) {
        if blocked {
            self.blocked.insert((from, to));
        } else {
            self.blocked.remove(&(from, to));
        }
    }

    /// Whether the pair from `from` to `to` is cut, by the test or by a drawn partition.
    pub fn cut(&self, from: NodeId, to: NodeId) -> bool {
        self.blocked.contains(&(from, to)) || self.drawn.contains(&(from, to))
    }

    /// Heals every cut the test made; a drawn partition heals at a churn.
    pub fn heal(&mut self) {
        self.blocked.clear();
    }

    /// Whether a drawn partition is in force.
    pub fn partitioned(&self) -> bool {
        !self.drawn.is_empty()
    }

    /// At the world's present, among `nodes`: a drawn partition heals with `plan.heal_ppm`, or one
    /// starts with `plan.start_ppm`, once the present state has lasted `plan.stable_ns`. Whether it
    /// changed.
    pub fn churn<E>(
        &mut self,
        world: &mut World<E>,
        nodes: &[NodeId],
        plan: Partitions,
    ) -> Result<bool, SimError> {
        let now = world.now();
        if now.saturating_sub(self.changed_at) < plan.stable_ns {
            return Ok(false);
        }
        let stream = Self::stream(
            world,
            &mut self.partitions,
            "net.partition",
            (NodeId(0), NodeId(0)),
        )?;
        if self.partitioned() {
            if !world.chance(stream, plan.heal_ppm)? {
                return Ok(false);
            }
            self.drawn.clear();
            self.changed_at = now;
            return Ok(true);
        }
        if !world.chance(stream, plan.start_ppm)? {
            return Ok(false);
        }
        self.draw_side(world, stream, nodes, plan.split)?;
        self.cut_side(nodes, plan.symmetric);
        self.changed_at = now;
        Ok(self.partitioned())
    }

    /// Draws the side of a partition into `self.side`.
    fn draw_side<E>(
        &mut self,
        world: &mut World<E>,
        stream: StreamId,
        nodes: &[NodeId],
        split: Split,
    ) -> Result<(), SimError> {
        self.side.clear();
        let count = u64::try_from(nodes.len()).unwrap_or(u64::MAX);
        match split {
            Split::UniformSize if count >= 2 => {
                let size = world
                    .below(stream, count.saturating_sub(1))?
                    .saturating_add(1);
                self.side.extend_from_slice(nodes);
                // The first `size` of a partial Fisher–Yates shuffle.
                for at in 0..size {
                    let span = count.saturating_sub(at);
                    let pick = at.saturating_add(world.below(stream, span)?);
                    let (at, pick) = (usize::try_from(at), usize::try_from(pick));
                    if let (Ok(at), Ok(pick)) = (at, pick) {
                        self.side.swap(at, pick);
                    }
                }
                self.side.truncate(usize::try_from(size).unwrap_or(0));
            }
            Split::UniformPartition => {
                for node in nodes {
                    if world.chance(stream, PPM / 2)? {
                        self.side.push(*node);
                    }
                }
            }
            Split::IsolateSingle if count >= 1 => {
                let pick = usize::try_from(world.below(stream, count)?).unwrap_or(0);
                self.side.extend(nodes.get(pick).copied());
            }
            _ => {}
        }
        Ok(())
    }

    /// Cuts every pair from the drawn side to the rest, and back where `symmetric`.
    fn cut_side(&mut self, nodes: &[NodeId], symmetric: bool) {
        for inside in &self.side {
            for outside in nodes.iter().filter(|node| !self.side.contains(node)) {
                self.drawn.insert((*inside, *outside));
                if symmetric {
                    self.drawn.insert((*outside, *inside));
                }
            }
        }
    }

    /// Forgets `node`: its paths, its flows' state and its NAT. What it has in flight still
    /// arrives, and its flows' streams are named again if it sends again.
    pub fn forget(&mut self, node: NodeId) {
        self.pair_paths
            .retain(|(from, to), _| *from != node && *to != node);
        self.flows
            .retain(|(from, to), _| *from != node && *to != node);
        self.nats.remove(&node);
    }

    /// The counters so far.
    pub fn stats(&self) -> NetStats {
        self.stats
    }

    /// Messages and bytes in flight.
    pub fn in_flight(&self) -> (usize, usize) {
        (self.flight.held, self.flight.bytes)
    }

    fn path(&self, from: NodeId, to: NodeId) -> Path {
        self.pair_paths
            .get(&(from, to))
            .copied()
            .unwrap_or(self.default_path)
    }

    /// The flow's state, made at its first message if there is room for it.
    fn flow(&mut self, from: NodeId, to: NodeId) -> Option<Flow> {
        if let Some(flow) = self.flows.get(&(from, to)) {
            return Some(*flow);
        }
        (self.flows.len() < self.limits.flows).then(Flow::default)
    }

    /// The flow's stream for `what`, named at its first draw.
    fn stream<E>(
        world: &mut World<E>,
        slot: &mut Option<StreamId>,
        what: &'static str,
        (from, to): (NodeId, NodeId),
    ) -> Result<StreamId, SimError> {
        if let Some(stream) = *slot {
            return Ok(stream);
        }
        let stream = world.stream(what, &[u64::from(from.0), u64::from(to.0)])?;
        *slot = Some(stream);
        Ok(stream)
    }

    /// Whether the flow's loss process loses this message: a transition of its channel, then a
    /// draw in the state it is in. A lossless path draws nothing.
    fn lose<E>(
        world: &mut World<E>,
        flow: &mut Flow,
        pair: (NodeId, NodeId),
        loss: Loss,
    ) -> Result<bool, SimError> {
        if loss.is_lossless() {
            return Ok(false);
        }
        let stream = Self::stream(world, &mut flow.loss, "net.loss", pair)?;
        flow.bad = match loss.chain {
            Chain::PerMessage {
                good_to_bad_ppm,
                bad_to_good_ppm,
            } => {
                if flow.bad {
                    !world.chance(stream, bad_to_good_ppm)?
                } else {
                    world.chance(stream, good_to_bad_ppm)?
                }
            }
            Chain::InTime {
                mean_good_ns,
                mean_bad_ns,
            } => {
                let now = world.now();
                let previous = flow
                    .last_loss_draw
                    .map(|at| (flow.bad, now.saturating_sub(at)));
                flow.last_loss_draw = Some(now);
                let chance = bad_in_time(mean_good_ns, mean_bad_ns, previous);
                world.below(stream, 1 << STATE_BITS)? < chance
            }
        };
        let ppm = if flow.bad {
            loss.bad_loss_ppm
        } else {
            loss.good_loss_ppm
        };
        world.chance(stream, ppm)
    }

    /// `one_way − jitter + U[0, 2·jitter]`, or the measured delay at a drawn probability; nothing
    /// is drawn without jitter or a measured delay.
    fn propagation<E>(
        world: &mut World<E>,
        flow: &mut Flow,
        pair: (NodeId, NodeId),
        path: Path,
    ) -> Result<u64, SimError> {
        if let Some(measured) = path.measured {
            let stream = Self::stream(world, &mut flow.delay, "net.delay", pair)?;
            let u = u32::try_from(world.below(stream, u64::from(PPM))?).unwrap_or(0);
            return Ok(measured.at(u));
        }
        if path.jitter_ns == 0 {
            return Ok(path.one_way_ns);
        }
        let stream = Self::stream(world, &mut flow.delay, "net.delay", pair)?;
        let span = path.jitter_ns.saturating_mul(2).saturating_add(1);
        Ok(path
            .one_way_ns
            .saturating_sub(path.jitter_ns)
            .saturating_add(world.below(stream, span)?))
    }

    /// Through the path's link, if it has one: when the message leaves it and whether the queue's
    /// manager marked it (only an ECN-capable one, `ecn`), or why it is dropped.
    fn queue(
        &mut self,
        now: u64,
        path: Path,
        bytes: u64,
        ecn: bool,
    ) -> Result<(u64, bool), Dropped> {
        let link_messages = self.limits.link_messages;
        let Some(link) = path.link.and_then(|id| self.links.get_mut(id.0)) else {
            return Ok((now, false));
        };
        let backlog = link.backlog_bytes(now);
        if backlog.saturating_add(bytes) > link.link.queue_bytes
            || link.queued.len() >= link_messages
        {
            return Err(Dropped::Queue);
        }
        let starts = link.busy_until.max(now);
        let marked = link.congested(now, starts, backlog, bytes);
        if marked && !ecn {
            return Err(Dropped::Queue);
        }
        self.stats.peak_queue_bytes = self.stats.peak_queue_bytes.max(backlog);
        let departs = starts.saturating_add(link.serialization_ns(bytes));
        link.busy_until = departs;
        link.queued.push_back(Queued {
            starts,
            departs,
            bytes,
        });
        if marked {
            bump(&mut self.stats.marked);
        }
        Ok((departs, marked))
    }

    /// Counts a message dropped at its send.
    fn dropped(&mut self, why: Dropped) -> Fate {
        let counter = match why {
            Dropped::Mtu => &mut self.stats.dropped_mtu,
            Dropped::Queue => &mut self.stats.dropped_queue,
            Dropped::Loss => &mut self.stats.dropped_loss,
            Dropped::Partition => &mut self.stats.dropped_partition,
            Dropped::Capacity => &mut self.stats.dropped_capacity,
            Dropped::Nat => &mut self.stats.dropped_nat,
        };
        bump(counter);
        Fate::Dropped(why)
    }

    /// The departure of a message of `bytes` on the flow, or why it does not depart: its path's
    /// MTU, its link and its loss process, in that order.
    fn depart<E>(
        &mut self,
        world: &mut World<E>,
        pair: (NodeId, NodeId),
        flow: &mut Flow,
        bytes: usize,
        ecn: bool,
    ) -> Result<Result<Departure, Dropped>, SimError> {
        let path = self.path(pair.0, pair.1);
        if path.mtu.is_some_and(|mtu| bytes > mtu) {
            return Ok(Err(Dropped::Mtu));
        }
        let size = u64::try_from(bytes).unwrap_or(u64::MAX);
        let (departs, marked) = match self.queue(world.now(), path, size, ecn) {
            Ok(queued) => queued,
            Err(why) => return Ok(Err(why)),
        };
        if Self::lose(world, flow, pair, path.loss)? {
            return Ok(Err(Dropped::Loss));
        }
        Ok(Ok(Departure {
            at: departs,
            path,
            marked,
        }))
    }

    /// Makes room in flight for `bytes` more: the oldest messages are lost and counted until it
    /// fits. A message larger than the network holds in all does not fit.
    fn make_room(&mut self, bytes: usize) -> bool {
        if bytes > self.limits.bytes {
            return false;
        }
        while self.flight.held >= self.limits.messages
            || self.flight.bytes.saturating_add(bytes) > self.limits.bytes
        {
            let Some(oldest) = self.flight.oldest() else {
                return false;
            };
            self.flight.take(oldest);
            bump(&mut self.stats.dropped_capacity);
        }
        true
    }

    /// Sends `payload`, `bytes` long once encoded, from `from` to `to` at the world's present,
    /// not ECN-capable: a queue manager drops it where it would mark it. If it is to arrive, the
    /// world is given `arrival(ticket)` for `to` at its arrival, which the harness hands back to
    /// [`deliver`](Self::deliver).
    pub fn send<E>(
        &mut self,
        world: &mut World<E>,
        (from, to): (NodeId, NodeId),
        payload: P,
        bytes: usize,
        arrival: impl FnOnce(Ticket) -> E,
    ) -> Result<Fate, SimError> {
        self.send_with(
            world,
            (from, to),
            payload,
            bytes,
            arrival,
            None::<fn(&mut P)>,
        )
    }

    /// Sends an ECN-capable message (RFC 3168 §5): where a queue manager on its link marks it,
    /// `mark` marks what it carries and it goes on.
    pub fn send_ecn<E>(
        &mut self,
        world: &mut World<E>,
        (from, to): (NodeId, NodeId),
        payload: P,
        bytes: usize,
        arrival: impl FnOnce(Ticket) -> E,
        mark: impl FnOnce(&mut P),
    ) -> Result<Fate, SimError> {
        self.send_with(world, (from, to), payload, bytes, arrival, Some(mark))
    }

    fn send_with<E>(
        &mut self,
        world: &mut World<E>,
        pair: (NodeId, NodeId),
        mut payload: P,
        bytes: usize,
        arrival: impl FnOnce(Ticket) -> E,
        mark: Option<impl FnOnce(&mut P)>,
    ) -> Result<Fate, SimError> {
        let now = world.now();
        bump(&mut self.stats.sent);
        if let Some((_, last)) = self.nats.get_mut(&pair.0) {
            *last = Some(now);
        }
        if self.cut(pair.0, pair.1) {
            return Ok(self.dropped(Dropped::Partition));
        }
        let Some(mut flow) = self.flow(pair.0, pair.1) else {
            return Ok(self.dropped(Dropped::Capacity));
        };
        let departure = self.depart(world, pair, &mut flow, bytes, mark.is_some())?;
        let fate = match departure {
            Ok(departure) => {
                if departure.marked
                    && let Some(mark) = mark
                {
                    mark(&mut payload);
                }
                self.fly(world, pair, &mut flow, departure, (payload, bytes), arrival)?
            }
            Err(why) => self.dropped(why),
        };
        self.flows.insert(pair, flow);
        Ok(fate)
    }

    /// Puts a departed message in flight and tells the world when it arrives.
    fn fly<E>(
        &mut self,
        world: &mut World<E>,
        pair: (NodeId, NodeId),
        flow: &mut Flow,
        departure: Departure,
        (payload, bytes): (P, usize),
        arrival: impl FnOnce(Ticket) -> E,
    ) -> Result<Fate, SimError> {
        let propagation = Self::propagation(world, flow, pair, departure.path)?;
        let mut at = departure.at.saturating_add(propagation);
        if !departure.path.reorders
            && let Some(previous) = flow.last_arrival
        {
            at = at.max(previous);
        }
        if !self.make_room(bytes) {
            return Ok(self.dropped(Dropped::Capacity));
        }
        let send = self.sends;
        self.sends = self.sends.saturating_add(1);
        let held = Held {
            send,
            from: pair.0,
            to: pair.1,
            bytes,
            payload,
        };
        let Some(ticket) = self.flight.hold(held) else {
            return Ok(self.dropped(Dropped::Capacity));
        };
        if let Err(error) = world.schedule(at, pair.1, arrival(ticket)) {
            // Nothing will carry it: it is not in flight.
            self.flight.take(ticket);
            return Err(error);
        }
        flow.last_arrival = Some(at);
        Ok(Fate::Arrives { at })
    }

    /// The message `ticket` names, arriving now: `None` if it was lost to the capacity since it
    /// was sent (counted then), or is lost now to a cut of its pair or its receiver's expired NAT
    /// mapping (counted now). A delivery the duplication draw keeps leaves the message in flight
    /// and gives the world `again(ticket)` for its next arrival, after a fresh propagation delay.
    pub fn deliver<E>(
        &mut self,
        world: &mut World<E>,
        ticket: Ticket,
        again: impl FnOnce(Ticket) -> E,
    ) -> Result<Option<Delivery<P>>, SimError> {
        let Some(held) = self.flight.get(ticket) else {
            return Ok(None);
        };
        let pair = (held.from, held.to);
        let now = world.now();
        if self.cut(pair.0, pair.1) {
            self.flight.take(ticket);
            bump(&mut self.stats.dropped_partition);
            return Ok(None);
        }
        if !self.reachable(pair.1, now) {
            self.flight.take(ticket);
            bump(&mut self.stats.dropped_nat);
            return Ok(None);
        }
        if self.kept(world, pair)? {
            return self.redeliver(world, ticket, pair, again);
        }
        let Some(held) = self.flight.take(ticket) else {
            return Ok(None);
        };
        bump(&mut self.stats.delivered);
        Ok(Some(Delivery {
            from: held.from,
            to: held.to,
            payload: held.payload,
        }))
    }

    /// Whether `node`'s NAT mapping, if it has one, is alive at `now`.
    fn reachable(&self, node: NodeId, now: u64) -> bool {
        self.nats.get(&node).is_none_or(|(nat, last)| {
            last.is_some_and(|last| now.saturating_sub(last) <= nat.idle_timeout_ns)
        })
    }

    /// Whether this delivery keeps its message for another: the flow's duplication draw.
    fn kept<E>(&mut self, world: &mut World<E>, pair: (NodeId, NodeId)) -> Result<bool, SimError> {
        if self.duplicate_ppm == 0 {
            return Ok(false);
        }
        let mut flow = self.flows.get(&pair).copied().unwrap_or_default();
        let stream = Self::stream(world, &mut flow.duplicate, "net.duplicate", pair)?;
        let kept = world.chance(stream, self.duplicate_ppm)?;
        if self.flows.contains_key(&pair) || self.flows.len() < self.limits.flows {
            self.flows.insert(pair, flow);
        }
        Ok(kept)
    }

    /// Delivers a copy of the message `ticket` names and schedules its next arrival.
    fn redeliver<E>(
        &mut self,
        world: &mut World<E>,
        ticket: Ticket,
        pair: (NodeId, NodeId),
        again: impl FnOnce(Ticket) -> E,
    ) -> Result<Option<Delivery<P>>, SimError> {
        let Some(payload) = self.flight.get(ticket).map(|held| held.payload.clone()) else {
            return Ok(None);
        };
        let mut flow = self.flows.get(&pair).copied().unwrap_or_default();
        let path = self.path(pair.0, pair.1);
        let delay = Self::propagation(world, &mut flow, pair, path)?;
        if self.flows.contains_key(&pair) || self.flows.len() < self.limits.flows {
            self.flows.insert(pair, flow);
        }
        if let Err(error) = world.schedule(world.now().saturating_add(delay), pair.1, again(ticket))
        {
            // Nothing will carry it again: it leaves flight with this delivery.
            self.flight.take(ticket);
            return Err(error);
        }
        bump(&mut self.stats.delivered);
        bump(&mut self.stats.duplicated);
        Ok(Some(Delivery {
            from: pair.0,
            to: pair.1,
            payload,
        }))
    }
}

/// What the adversary of the sealed plane sends.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Attack {
    /// A datagram the network carried, again, from its sender.
    Replay,
    /// A datagram cut short, from the adversary's address.
    Truncate,
    /// A datagram with one byte changed, from the adversary's address.
    Forge,
}

/// The adversary of the sealed plane (`docs/sim.md` §3.4): it keeps the latest datagrams the
/// network carried and, when the harness asks, sends a replay of one, one cut short or one with a
/// byte changed, as hyper-datagram's E2E does on sockets, so that every seed of a run on the plane
/// tries them. It keeps at most `datagrams` of them and `bytes` bytes, the oldest given up first,
/// and reuses their buffers, so it allocates nothing once grown.
#[derive(Clone, Debug)]
pub struct Adversary {
    seen: VecDeque<(NodeId, NodeId, Vec<u8>)>,
    datagrams: usize,
    bytes: usize,
    held: usize,
    spare: Vec<Vec<u8>>,
    stream: Option<StreamId>,
}

impl Adversary {
    /// An adversary that keeps at most `datagrams` datagrams and `bytes` bytes of them.
    pub fn new(datagrams: usize, bytes: usize) -> Self {
        Self {
            seen: VecDeque::new(),
            datagrams,
            bytes,
            held: 0,
            spare: Vec::new(),
            stream: None,
        }
    }

    /// The datagrams kept.
    pub fn kept(&self) -> usize {
        self.seen.len()
    }

    /// Keeps a datagram the network carried from `from` to `to`; one longer than the adversary
    /// keeps in all is not kept.
    pub fn observe(&mut self, from: NodeId, to: NodeId, datagram: &[u8]) {
        if datagram.len() > self.bytes || self.datagrams == 0 {
            return;
        }
        while self.seen.len() >= self.datagrams
            || self.held.saturating_add(datagram.len()) > self.bytes
        {
            let Some((_, _, mut old)) = self.seen.pop_front() else {
                break;
            };
            self.held = self.held.saturating_sub(old.len());
            old.clear();
            self.spare.push(old);
        }
        let mut copy = self.spare.pop().unwrap_or_default();
        copy.extend_from_slice(datagram);
        self.held = self.held.saturating_add(copy.len());
        self.seen.push_back((from, to, copy));
    }

    /// Sends one attack, drawn, on a kept datagram drawn: its receiver gets a replay from the
    /// datagram's sender, or a truncated or forged copy from `adversary`. `None` while nothing is
    /// kept.
    pub fn attack<E>(
        &mut self,
        world: &mut World<E>,
        net: &mut Net<Vec<u8>>,
        adversary: NodeId,
        arrival: impl FnOnce(Ticket) -> E,
    ) -> Result<Option<(Attack, Fate)>, SimError> {
        let count = u64::try_from(self.seen.len()).unwrap_or(u64::MAX);
        if count == 0 {
            return Ok(None);
        }
        let stream = Net::<Vec<u8>>::stream(
            world,
            &mut self.stream,
            "net.adversary",
            (adversary, adversary),
        )?;
        let pick = usize::try_from(world.below(stream, count)?).unwrap_or(0);
        let Some((from, to, original)) = self.seen.get(pick) else {
            return Ok(None);
        };
        let (from, to) = (*from, *to);
        let mut datagram = self.spare.pop().unwrap_or_default();
        datagram.extend_from_slice(original);
        let attack = match world.below(stream, 3)? {
            0 => Attack::Replay,
            1 => Attack::Truncate,
            _ => Attack::Forge,
        };
        let sender = Self::alter(world, stream, attack, &mut datagram, (from, adversary))?;
        let bytes = datagram.len();
        let fate = net.send(world, (sender, to), datagram, bytes, arrival)?;
        Ok(Some((attack, fate)))
    }

    /// Makes `datagram` the attack's and gives the address it is sent from.
    fn alter<E>(
        world: &mut World<E>,
        stream: StreamId,
        attack: Attack,
        datagram: &mut Vec<u8>,
        (from, adversary): (NodeId, NodeId),
    ) -> Result<NodeId, SimError> {
        let length = u64::try_from(datagram.len()).unwrap_or(u64::MAX);
        match attack {
            Attack::Replay => return Ok(from),
            // Strictly shorter, down to nothing.
            Attack::Truncate => {
                let keep = world.below(stream, length.max(1))?;
                datagram.truncate(usize::try_from(keep).unwrap_or(0));
            }
            Attack::Forge => {
                if length > 0 {
                    let at = usize::try_from(world.below(stream, length)?).unwrap_or(0);
                    // A change of one to 255: never the byte it was.
                    let change =
                        u8::try_from(world.below(stream, 255)?.saturating_add(1)).unwrap_or(1);
                    if let Some(byte) = datagram.get_mut(at) {
                        *byte ^= change;
                    }
                }
            }
        }
        Ok(adversary)
    }
}

#[cfg(test)]
mod tests {
    use super::{Q64_ONE, STATE_BITS, bad_in_time, power};

    const MILLISECOND: u64 = 1_000_000;
    /// The measured condition of `docs/research/burst-loss.md` §4.
    const BURST: u64 = 36_800_000;
    const GAP: u64 = 700 * MILLISECOND;
    const CERTAIN: u64 = 1 << STATE_BITS;

    #[test]
    fn a_power_is_exact_where_its_terms_are() {
        assert_eq!(power(Q64_ONE / 2, 0), Q64_ONE);
        assert_eq!(power(Q64_ONE / 2, 1), Q64_ONE / 2);
        assert_eq!(power(Q64_ONE / 2, 3), Q64_ONE / 8);
        assert_eq!(power(Q64_ONE / 2, 64), 1);
        assert_eq!(power(Q64_ONE / 2, 65), 0);
        assert_eq!(power(Q64_ONE, u64::MAX), Q64_ONE);
    }

    #[test]
    fn no_time_leaves_the_state_where_it_was() {
        assert_eq!(bad_in_time(GAP, BURST, Some((true, 0))), CERTAIN);
        assert_eq!(bad_in_time(GAP, BURST, Some((false, 0))), 0);
    }

    #[test]
    fn a_flow_starts_and_ends_at_the_stationary_chance() {
        // ⌊2³²·B/(G+B)⌋: the stationary chance of a burst, 5% of the time here.
        let stationary =
            u64::try_from(u128::from(CERTAIN) * u128::from(BURST) / u128::from(GAP + BURST))
                .unwrap();
        assert_eq!(bad_in_time(GAP, BURST, None), stationary);
        // Long enough after that λ^δ is below 2⁻⁶⁴, the state is forgotten.
        let forgotten = 1_000 * GAP;
        assert_eq!(bad_in_time(GAP, BURST, Some((true, forgotten))), stationary);
        assert_eq!(
            bad_in_time(GAP, BURST, Some((false, forgotten))),
            stationary
        );
    }

    #[test]
    fn a_burst_fades_with_time_and_a_gap_fills() {
        let mut was_bad = CERTAIN;
        let mut was_good = 0;
        for elapsed in [1, 1_000, MILLISECOND, 10 * MILLISECOND, 100 * MILLISECOND] {
            let bad = bad_in_time(GAP, BURST, Some((true, elapsed)));
            let good = bad_in_time(GAP, BURST, Some((false, elapsed)));
            assert!(bad <= was_bad && good >= was_good, "at {elapsed} ns");
            assert!(
                good < bad,
                "a burst is likelier just after one, at {elapsed} ns"
            );
            (was_bad, was_good) = (bad, good);
        }
    }
}
