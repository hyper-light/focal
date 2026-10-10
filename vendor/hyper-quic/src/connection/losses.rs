//! What a path's losses say of their bursts, learned from the connection's own loss record
//! (`docs/research/burst-loss.md` §7).
//!
//! RFC 9002 declares each lost packet with its send time. For a lost ack-eliciting packet, the
//! next ack-eliciting packet its space sent is its neighbour: `gap` later, lost or acknowledged.
//! Under the two-state loss chain in time with correlation time `τ` and mean loss `r`, the
//! neighbour of a lost packet is lost with probability `r + (1 − r)·e^(−gap/τ)`; on a path whose
//! losses are independent, with `r`. Each neighbour's fate adds its log-likelihood under each of
//! three hypotheses: independent loss, and the two measured conditions, `τ` = 35.0 ms (Jiang and
//! Schulzrinne's trace 4) and 78.7 ms (Bolot's 200 ms column). A hypothesis replaces the configured
//! correlation time only once it is more likely than the 35 ms one by Wald's sequential ratio
//! for errors of 5% each way, `(1 − β)/α = 19` (Wald, "Sequential Tests of Statistical
//! Hypotheses", Ann. Math. Statist. 16(2), 1945): with too few losses the configuration
//! stands.
//!
//! The mean loss is Laplace's rule of succession over the fates counted, `(lost + 1)/(fates + 2)`.
//! Every step is integer arithmetic, so a schedule is the same on every host.
//!
//! A packet declared lost is counted only once a packet sent later by more than the path's delay
//! variation is acknowledged while it is not ([`Ledger`]): RACK's test of a later send delivered
//! (RFC 8985 §6.2), with the reordering window widened to what the delay varies by, four mean
//! deviations (RFC 6298 §2's `K`). RFC 9002 declares a packet lost on reordering as well as on loss
//! (§6.1), and a reordered packet whose neighbour arrived is no evidence that losses are
//! independent. One acknowledged after it was declared lost counts as delivered.
//!
//! An endpoint keeps a path's evidence per remote IP address in a [`LossMemory`], as it keeps
//! Careful Resume's measurements, so the next connection's handshake copies use it.

use std::collections::VecDeque;
use std::net::IpAddr;

use super::copies::log2_fixed;
use crate::packet::SpaceId;
use crate::{Duration, Instant};

/// The hypotheses weighed, as correlation times; zero is independent loss.
const HYPOTHESES: [Duration; 3] = [
    Duration::ZERO,
    // Jiang and Schulzrinne, NOSSDAV 2000, Table 1, trace 4: 30/ln((1 − 0.0282)/(0.441 − 0.0282))
    Duration::from_micros(35_000),
    // Bolot, SIGCOMM 1993, Table 3, δ = 200 ms: 200/ln((1 − 0.11)/(0.18 − 0.11))
    Duration::from_micros(78_700),
];

/// The hypothesis a measurement must beat: the configured default's, 35 ms.
const DEFAULT: usize = 1;

/// Fractional bits of the log-likelihoods, as of the logarithm they are summed from.
const FRACTION_BITS: u32 = 16;

/// Wald's ratio for errors of 5% each way, `(1 − β)/α` with α = β = 0.05.
const WALD_RATIO: u64 = 19;

/// The most likely a packet is lost right after a lost one on any path measured: 0.60, Bolot's
/// conditional loss at his shortest spacing, 8 ms (SIGCOMM 1993, Table 3), in 32 fractional bits.
/// The chain in time takes it to one as the gap closes; no trace measured that, and a single
/// neighbour delivered beside a loss, which a receiver's acknowledgement ranges can also make
/// (RFC 9000 §13.2.3 lets it drop old ranges), would otherwise refute bursts at once.
const NEIGHBOUR_CEILING: u64 = 2_576_980_378;

/// One in fixed point with 64 fractional bits.
const Q64_ONE: u128 = 1 << 64;

/// A path's evidence on its loss bursts: the fates counted, and each hypothesis's log2-likelihood
/// in [`FRACTION_BITS`] fractional bits.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct LossFit {
    /// Ack-eliciting packets declared lost.
    lost: u64,
    /// Ack-eliciting packets acknowledged.
    delivered: u64,
    /// The log2-likelihood of the neighbours' fates under each of [`HYPOTHESES`].
    evidence: [i64; 3],
    /// Neighbours of lost packets whose fate was counted.
    pairs: u64,
}

impl LossFit {
    /// An ack-eliciting packet's fate.
    pub(super) fn fate(&mut self, lost: bool) {
        if lost {
            self.lost = self.lost.saturating_add(1);
        } else {
            self.delivered = self.delivered.saturating_add(1);
        }
    }

    /// The fate of the packet sent `gap` after a lost one.
    pub(super) fn neighbour(&mut self, gap: Duration, lost: bool) {
        let rate = self.rate();
        for (evidence, tau) in self.evidence.iter_mut().zip(HYPOTHESES) {
            let chance = neighbour_lost(rate, gap, tau);
            let p = if lost {
                chance
            } else {
                (1_u64 << 32).saturating_sub(chance)
            };
            *evidence = evidence.saturating_add(log2_q32(p));
        }
        self.pairs = self.pairs.saturating_add(1);
    }

    /// The correlation time the evidence settles on, once one hypothesis is more likely than the
    /// configured default's by [`WALD_RATIO`]: zero for independent loss.
    pub(crate) fn tau(&self) -> Option<Duration> {
        let threshold = i64::try_from(log2_fixed(WALD_RATIO)).unwrap_or(i64::MAX);
        let base = self.evidence.get(DEFAULT).copied().unwrap_or(0);
        self.evidence
            .iter()
            .zip(HYPOTHESES)
            .enumerate()
            .filter(|(at, _)| *at != DEFAULT)
            .filter(|(_, (evidence, _))| evidence.saturating_sub(base) >= threshold)
            .max_by_key(|(_, (evidence, _))| **evidence)
            .map(|(_, (_, tau))| tau)
    }

    /// Adds another's evidence: an endpoint's memory of a path takes each connection's.
    pub(crate) fn add(&mut self, other: &Self) {
        self.lost = self.lost.saturating_add(other.lost);
        self.delivered = self.delivered.saturating_add(other.delivered);
        self.pairs = self.pairs.saturating_add(other.pairs);
        for (mine, theirs) in self.evidence.iter_mut().zip(other.evidence) {
            *mine = mine.saturating_add(theirs);
        }
    }

    /// Whether nothing was counted.
    pub(crate) fn is_empty(&self) -> bool {
        self.lost == 0 && self.delivered == 0 && self.pairs == 0
    }

    /// The mean loss by Laplace's rule of succession, in 32 fractional bits.
    fn rate(&self) -> u64 {
        let fates = u128::from(self.lost)
            .saturating_add(u128::from(self.delivered))
            .saturating_add(2);
        let lost = u128::from(self.lost).saturating_add(1);
        lost.checked_shl(32)
            .and_then(|scaled| scaled.checked_div(fates))
            .and_then(|rate| u64::try_from(rate).ok())
            .unwrap_or(1 << 32)
    }
}

/// The chance, in 32 fractional bits, that the packet `gap` after a lost one is lost too at mean
/// loss `rate` (32 fractional bits) under correlation time `tau`: `r + (1 − r)·e^(−gap/τ)`, the
/// exponential as `(1 − 1/τ)^gap` in nanoseconds, raised by squaring; `r` for independent loss.
fn neighbour_lost(rate: u64, gap: Duration, tau: Duration) -> u64 {
    let tau = tau.as_nanos();
    if tau == 0 {
        return rate;
    }
    let step = Q64_ONE.checked_div(tau).unwrap_or(0);
    let gap = u64::try_from(gap.as_nanos()).unwrap_or(u64::MAX);
    let decay = power(Q64_ONE.saturating_sub(step), gap);
    let rest = u128::from((1_u64 << 32).saturating_sub(rate));
    let tail = rest
        .checked_mul(decay)
        .and_then(|product| product.checked_shr(64))
        .and_then(|tail| u64::try_from(tail).ok())
        .unwrap_or(0);
    rate.saturating_add(tail).min(NEIGHBOUR_CEILING.max(rate))
}

/// The log2 of a probability in 32 fractional bits, in [`FRACTION_BITS`] fractional bits: at most
/// zero, and a probability of zero taken at its least step, `2^−32`.
fn log2_q32(p: u64) -> i64 {
    let bits = i64::try_from(log2_fixed(p.max(1))).unwrap_or(i64::MAX);
    bits.saturating_sub(32_i64 << FRACTION_BITS)
}

/// The product of two numbers in 64 fractional bits, at most one each, truncated.
fn multiply(a: u128, b: u128) -> u128 {
    a.checked_mul(b)
        .and_then(|product| product.checked_shr(64))
        .unwrap_or(Q64_ONE)
}

/// `base^exponent` in 64 fractional bits, `base` at most one: by squaring, at most 64 steps.
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

/// The lost packets a connection holds before it counts them: the initial window's packets
/// (RFC 9002 §7.2: ten), the evidence a handshake's flights make. Past it the oldest is dropped
/// uncounted: a sample lost, never a wrong count.
const PENDING: usize = 10;

/// What became of the next ack-eliciting packet after a lost one.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Neighbour {
    /// In flight: this number.
    Waiting(u64),
    /// Acknowledged.
    Delivered,
    /// Declared lost: this number, counted lost once it is.
    Lost(u64),
}

/// A packet declared lost, not yet counted.
#[derive(Clone, Copy, Debug)]
struct Pending {
    space: SpaceId,
    number: u64,
    /// When it was sent.
    sent: Instant,
    /// Its neighbour and the time between their sends.
    neighbour: Option<(Duration, Neighbour)>,
}

/// A connection's loss evidence: what it counted, what it has yet to tell its endpoint, and the
/// lost packets it waits to count.
#[derive(Debug, Default)]
pub(crate) struct Ledger {
    /// The endpoint's memory of the remote when the connection began, and what it counted since.
    fit: LossFit,
    /// What it counted since it last told its endpoint.
    untold: LossFit,
    pending: VecDeque<Pending>,
}

impl Ledger {
    /// A ledger that starts from the endpoint's memory of the remote.
    pub(crate) fn new(fit: LossFit) -> Self {
        Self {
            fit,
            ..Self::default()
        }
    }

    /// The correlation time the evidence settles on ([`LossFit::tau`]).
    pub(crate) fn tau(&self) -> Option<Duration> {
        self.fit.tau()
    }

    fn count(&mut self, count: impl Fn(&mut LossFit)) {
        count(&mut self.fit);
        count(&mut self.untold);
    }

    /// Packet `number` of `space` was acknowledged
    pub(super) fn delivered(&mut self, space: SpaceId, number: u64, ack_eliciting: bool) {
        if ack_eliciting {
            self.count(|fit| fit.fate(false));
        }
        self.resolve(space, Neighbour::Waiting(number), Neighbour::Delivered);
    }

    /// The pending packets of `space` whose neighbour is `from` learn it is `to`
    fn resolve(&mut self, space: SpaceId, from: Neighbour, to: Neighbour) {
        for pending in self
            .pending
            .iter_mut()
            .filter(|pending| pending.space == space)
        {
            if let Some((_, neighbour)) = &mut pending.neighbour
                && *neighbour == from
            {
                *neighbour = to;
            }
        }
    }

    /// Ack-eliciting packet `number` of `space`, sent at `sent`, was declared lost; `next` is its
    /// neighbour's number, the time between their sends, and whether the neighbour is still in
    /// flight (one no longer held was acknowledged: packets are declared lost in the order sent)
    pub(super) fn lost(
        &mut self,
        space: SpaceId,
        number: u64,
        sent: Instant,
        next: Option<(u64, Duration, bool)>,
    ) {
        self.resolve(space, Neighbour::Waiting(number), Neighbour::Lost(number));
        let neighbour = next.map(|(next, gap, in_flight)| {
            let fate = if in_flight {
                Neighbour::Waiting(next)
            } else {
                Neighbour::Delivered
            };
            (gap, fate)
        });
        if self.pending.len() >= PENDING {
            self.pending.pop_front();
        }
        self.pending.push_back(Pending {
            space,
            number,
            sent,
            neighbour,
        });
    }

    /// An acknowledgement of `space` names `low..=high`: a packet declared lost among them was
    /// reordered, not lost, and counts as delivered. Only the held packets are visited, never the
    /// range the peer chose.
    pub(super) fn acknowledged(&mut self, space: SpaceId, low: u64, high: u64) {
        while let Some(at) = self
            .pending
            .iter()
            .position(|pending| pending.space == space && (low..=high).contains(&pending.number))
        {
            let Some(reordered) = self.pending.remove(at) else {
                return;
            };
            self.count(|fit| fit.fate(false));
            self.resolve(
                space,
                Neighbour::Lost(reordered.number),
                Neighbour::Delivered,
            );
        }
    }

    /// Counts the lost packets of `space` sent more than `variation` before `delivered_sent`, the
    /// send time of a packet of the same space just acknowledged, with their neighbours' fates once
    /// those are known: a packet that late could not have been overtaken by reordering alone. Only
    /// the same space's acknowledgements settle: a peer that discarded a space's keys
    /// acknowledges nothing more of it (RFC 9001 §4.9), and its last packets may have arrived.
    pub(super) fn settle(&mut self, space: SpaceId, delivered_sent: Instant, variation: Duration) {
        let settled = |pending: &Pending| {
            pending.space == space
                && pending
                    .sent
                    .checked_add(variation)
                    .is_some_and(|after| after < delivered_sent)
        };
        let mut index = 0;
        while let Some(pending) = self.pending.get(index).copied() {
            if !settled(&pending) {
                index = index.saturating_add(1);
                continue;
            }
            let pair = match pending.neighbour {
                None => Some(None),
                Some((_, Neighbour::Waiting(_))) => None,
                Some((gap, Neighbour::Delivered)) => Some(Some((gap, false))),
                Some((gap, Neighbour::Lost(next))) => {
                    let unsettled = self.pending.iter().any(|other| {
                        other.space == pending.space && other.number == next && !settled(other)
                    });
                    (!unsettled).then_some(Some((gap, true)))
                }
            };
            let Some(pair) = pair else {
                index = index.saturating_add(1);
                continue;
            };
            self.pending.remove(index);
            self.count(|fit| fit.fate(true));
            if let Some((gap, lost)) = pair {
                self.count(|fit| fit.neighbour(gap, lost));
            }
        }
    }

    /// What was counted since the endpoint was last told, taken
    pub(super) fn untold(&mut self) -> Option<LossFit> {
        (!self.untold.is_empty()).then(|| std::mem::take(&mut self.untold))
    }
}

/// The evidence an endpoint holds on its paths' loss bursts, one per remote IP address, at most
/// `capacity`, the oldest replaced first; each younger than the lifetime it is read with.
pub(crate) struct LossMemory {
    entries: VecDeque<(IpAddr, LossFit, Instant)>,
    capacity: usize,
}

impl LossMemory {
    pub(crate) fn new(capacity: usize) -> Self {
        Self {
            entries: VecDeque::new(),
            capacity,
        }
    }

    /// The evidence on `remote`, if any younger than `lifetime` is held. It stays held: every
    /// connection to the remote starts from it, and adds to it.
    pub(crate) fn get(&self, remote: IpAddr, now: Instant, lifetime: Duration) -> LossFit {
        self.entries
            .iter()
            .find(|(ip, _, _)| *ip == remote)
            .filter(|(_, _, at)| now.saturating_duration_since(*at) <= lifetime)
            .map(|(_, fit, _)| *fit)
            .unwrap_or_default()
    }

    /// Adds a connection's new evidence on `remote`, renewing its age; evidence past `lifetime`
    /// is dropped first
    pub(crate) fn add(&mut self, remote: IpAddr, fit: &LossFit, now: Instant, lifetime: Duration) {
        if self.capacity == 0 || fit.is_empty() {
            return;
        }
        let mut held = LossFit::default();
        if let Some(at) = self.entries.iter().position(|(ip, _, _)| *ip == remote)
            && let Some((_, old, when)) = self.entries.remove(at)
            && now.saturating_duration_since(when) <= lifetime
        {
            held = old;
        }
        held.add(fit);
        while self.entries.len() >= self.capacity {
            self.entries.pop_front();
        }
        self.entries.push_back((remote, held, now));
    }

    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.entries.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const ONE: u64 = 1 << 32;

    #[test]
    fn independent_loss_is_the_mean_at_any_gap() {
        let rate = ONE / 20;
        assert_eq!(neighbour_lost(rate, Duration::ZERO, Duration::ZERO), rate);
        assert_eq!(
            neighbour_lost(rate, Duration::from_secs(1), Duration::ZERO),
            rate
        );
    }

    #[test]
    fn a_near_neighbour_is_lost_at_the_measured_ceiling_and_a_far_one_at_the_mean() {
        let rate = ONE / 20;
        let tau = Duration::from_millis(35);
        assert_eq!(neighbour_lost(rate, Duration::ZERO, tau), NEIGHBOUR_CEILING);
        // e^(−100) is below 2^−64: the mean alone
        assert_eq!(
            neighbour_lost(rate, Duration::from_millis(3_500), tau),
            rate
        );
        let near = neighbour_lost(rate, Duration::from_millis(30), tau);
        let far = neighbour_lost(rate, Duration::from_millis(100), tau);
        assert!(NEIGHBOUR_CEILING > near && near > far && far > rate);
    }

    #[test]
    fn with_no_evidence_the_configuration_stands() {
        assert_eq!(LossFit::default().tau(), None);
    }

    /// A fit at 5% loss, nineteen deliveries to each loss, after `pairs` lost packets whose
    /// neighbour, sent `gap` after, met `lost`.
    fn fit(pairs: u32, gap: Duration, lost: bool) -> LossFit {
        let mut fit = LossFit::default();
        for _ in 0..pairs {
            for _ in 0..19 {
                fit.fate(false);
            }
            fit.fate(true);
            fit.neighbour(gap, lost);
        }
        fit
    }

    /// Neighbours sent with lost packets and delivered: each is (1 − r)/(1 − 0.6) more likely
    /// under independent loss, and Wald's ratio of 19 needs four of them.
    #[test]
    fn four_neighbours_sent_with_lost_packets_and_delivered_are_independent_loss() {
        let gap = Duration::from_micros(10);
        assert_eq!(fit(3, gap, false).tau(), None);
        assert_eq!(fit(4, gap, false).tau(), Some(Duration::ZERO));
    }

    /// Neighbours lost with the packets they followed favour bursts, which the configured 35 ms
    /// already is: it stands, and evidence at short gaps never tells 78.7 ms from it.
    #[test]
    fn neighbours_lost_together_keep_the_configured_bursts() {
        assert_eq!(fit(8, Duration::from_micros(10), true).tau(), None);
    }

    /// A neighbour 100 ms after its lost packet and delivered weighs less than one sent with it.
    #[test]
    fn a_far_neighbour_weighs_less() {
        let gap = Duration::from_millis(100);
        assert_eq!(fit(4, gap, false).tau(), None);
        let needed = (5..64)
            .find(|pairs| fit(*pairs, gap, false).tau().is_some())
            .unwrap();
        assert_eq!(fit(needed, gap, false).tau(), Some(Duration::ZERO));
    }

    fn at(base: Instant, ms: u64) -> Instant {
        base + Duration::from_millis(ms)
    }

    /// A packet declared lost and then acknowledged was reordered: it counts as delivered, and
    /// as no evidence of a burst.
    #[test]
    fn a_packet_acknowledged_after_its_loss_was_declared_is_delivered() {
        let base = Instant::now();
        let mut ledger = Ledger::default();
        ledger.lost(
            SpaceId::Data,
            4,
            at(base, 0),
            Some((5, Duration::ZERO, false)),
        );
        ledger.acknowledged(SpaceId::Data, 3, 4);
        ledger.settle(SpaceId::Data, at(base, 10_000), Duration::from_millis(1));
        let told = ledger.untold().unwrap();
        assert_eq!((told.lost, told.delivered, told.pairs), (0, 1, 0));
    }

    /// A lost packet is counted only once a packet of its space sent more than the delay's
    /// variation after it is acknowledged, and its neighbour's fate with it once known.
    #[test]
    fn a_loss_counts_once_a_later_send_arrives_and_its_neighbour_is_known() {
        let base = Instant::now();
        let variation = Duration::from_millis(200);
        let mut ledger = Ledger::default();
        ledger.lost(
            SpaceId::Data,
            4,
            at(base, 0),
            Some((5, Duration::ZERO, true)),
        );
        // Another space's delivery, and one within the variation, settle nothing
        ledger.settle(SpaceId::Handshake, at(base, 10_000), variation);
        ledger.settle(SpaceId::Data, at(base, 200), variation);
        assert!(ledger.untold().is_none());
        // Its neighbour still in flight: it waits for its fate
        ledger.settle(SpaceId::Data, at(base, 201), variation);
        assert!(ledger.untold().is_none());
        ledger.lost(SpaceId::Data, 5, at(base, 0), None);
        ledger.settle(SpaceId::Data, at(base, 201), variation);
        let told = ledger.untold().unwrap();
        assert_eq!((told.lost, told.delivered, told.pairs), (2, 0, 1));
    }

    #[test]
    fn the_memory_keeps_one_fit_a_remote_and_its_bound() {
        let now = Instant::now();
        let hour = Duration::from_secs(3_600);
        let mut memory = LossMemory::new(2);
        let mut fit = LossFit::default();
        fit.fate(true);
        let a: IpAddr = [10, 0, 0, 1].into();
        let b: IpAddr = [10, 0, 0, 2].into();
        let c: IpAddr = [10, 0, 0, 3].into();
        memory.add(a, &fit, now, hour);
        memory.add(a, &fit, now, hour);
        assert_eq!(memory.len(), 1);
        assert_eq!(memory.get(a, now, hour).lost, 2);
        memory.add(b, &fit, now, hour);
        memory.add(c, &fit, now, hour);
        assert_eq!(memory.len(), 2);
        assert!(memory.get(a, now, hour).is_empty(), "the oldest replaced");
        let later = now + hour + Duration::from_secs(1);
        assert!(memory.get(b, later, hour).is_empty(), "past its lifetime");
        memory.add(b, &fit, later, hour);
        assert_eq!(
            memory.get(b, later, hour).lost,
            1,
            "an old fit is not added to"
        );
    }
}
