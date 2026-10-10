//! Waits charged to progress (mantle note 32 T39; focal `transport.rs` `carried` and `frame.rs`
//! `read_payload_arriving`, made sans-io).
//!
//! An exchange is not given a fixed wall time. It is judged once a period, and given up only when
//! a period moved less than a datagram ([`LEAST_PROGRESS`]), so "a megabyte crosses a path of
//! 4 Mbit/s ... between endpoints that give a request one second" (focal 27 §7), and an exchange
//! whose peer stopped taking it ends within a period or two on any path. Two phases:
//!
//! - **Asking**: the request is being carried and the reply's prefix has not arrived. A period is
//!   charged what the connection sent and did not lose in it. The exchange ends at the end of a
//!   period that sent less than a datagram, or in which the peer sent nothing at all, and at the
//!   end of one that began when all the
//!   exchanges on the connection had to send beside it, and its own bytes, had been sent: the peer
//!   had the request and a period to answer it. What was sent is progress only while the peer is
//!   heard: a live peer acknowledges what it receives within its `max_ack_delay` (RFC 9000 §13.2.1,
//!   25 ms by default, §18.2), so a period that heard nothing is silence, and what the sender put
//!   into it (the flight in the air, then the probe timeout's probes, which RFC 9002 §6.2.4 sends
//!   whether or not the peer lives) moved nothing. So a period must be longer than the peer's
//!   acknowledgement delay, and `Endpoint::open` refuses one that is not
//!   (`Refusal::Configuration`); focal's `carried` counted bytes sent alone.
//! - **Answering**: the reply's prefix arrived and its body is arriving (or, on the side that
//!   answers, the request is arriving). A period must bring a datagram's worth of the connection's
//!   received bytes, or the body's end. Its bytes may come after others the peer sends first: what
//!   a more urgent class has to send (strict priority, T15), and what the peer declared on the
//!   connection's other exchanges of its class or a less urgent one. So the wait is charged with
//!   the bytes of its own class and the less urgent ones that the connection delivered, against
//!   what the peer declared and has still to deliver of them, this body included; the body ends at
//!   the end of a period that began with all of that delivered, as the asking phase ends one that
//!   began with everything sent. A peer that withholds the body while it sends others is given up
//!   once it has sent everything it owed; one that sends nothing, within a period.
//!
//!   The wait used to be bounded instead by the body's residency, its bytes at two datagrams of
//!   the least size a round trip of the path (focal `frame.rs`), and a period at least. That prices
//!   a sender limited by its path alone, which holds where a payload is written whole before it is
//!   sent and has its stream to itself. Here the peer's owner writes the body as it has it, its
//!   exchanges share the connection, and both ends may be short of CPU, so the round trip QUIC
//!   measures (395 µs on loopback) says nothing of when the body ends: an 8 MiB bulk reply moving
//!   at 2.7 MB/s was refused at 5.5 MB read (windows-11-arm, 35d35d8), and a 64 KiB reply queued
//!   behind fifteen others at none read while its period brought 464 KB. A wall-clock bound
//!   measures the machine, not the peer (`hyper_timing::progress`); what a slow body holds here is
//!   bounded in bytes by the receive window the budget funds, not by time.
//! - **Queued**: the exchange is open and waits for a stream, the peer's concurrent-stream credit
//!   being taken. It is judged on whether the connection's credit queue moves, not on its own age:
//!   an exchange given a stream or one holding a stream finishing, or the peer raising the streams
//!   it lets this side open, is an advance, and each restarts the wait from it; a period with none
//!   refuses it `Stalled`. A healthy peer behind a full window keeps the queue moving, so nothing
//!   queued is refused; one that withholds credit stops it, and every queued exchange is refused,
//!   typed, within a period of the last advance. The stream's grant starts the asking phase afresh.
//!   Judged on its own age, the 1,001st exchange beside 1,000 held ones was refused while its peer
//!   was healthy (hyper-transport `benches/lookup.rs`, 2026-10-05); an exchange refused while
//!   queued freed no stream, so it is no advance.

use std::time::{Duration, Instant};

use hyper_timing::RoundBudget;

use crate::Refusal;
use crate::credit::MIN_DATAGRAM;

/// The least a period must move: one datagram of the smallest size a QUIC path carries
/// (RFC 9000 §14; focal `frame.rs`'s `LEAST_PROGRESS`).
pub const LEAST_PROGRESS: u64 = MIN_DATAGRAM;

/// A progress-charged deadline: the period at which an exchange is judged.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Progress {
    period: Duration,
}

impl Progress {
    /// A deadline judged every `period`, which is what the peer is given to answer once it has
    /// the request.
    pub fn new(period: Duration) -> Result<Self, Refusal> {
        if period.is_zero() {
            return Err(Refusal::Configuration);
        }
        Ok(Self { period })
    }
    /// The period a round's budget opens with (hyper-timing's law for focal and slates): the tail
    /// of the exchanges with these peers, or the ceiling while none is measured.
    pub fn from_budget(budget: &RoundBudget) -> Self {
        let period = Duration::from_nanos(budget.deadline_ns.max(1));
        Self { period }
    }
    /// The period.
    pub fn period(&self) -> Duration {
        self.period
    }
}

/// What the connection has moved so far, as the judgement reads it.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct Moved {
    /// Bytes the connection sent and did not find lost.
    pub(crate) sent: u64,
    /// Bytes the connection received.
    pub(crate) received: u64,
    /// Stream bytes the connection delivered of the exchange's class and the less urgent ones.
    pub(crate) delivered: u64,
}

#[derive(Clone, Copy, Debug)]
enum Phase {
    Asking {
        /// What the exchanges on the connection, this one included, had to send.
        owed: u64,
        /// What the periods so far were charged.
        charged: u64,
        /// Whether the period now running began with every owed byte charged.
        had: bool,
    },
    Answering {
        /// What the peer declared on the connection, of the exchange's class and the less urgent
        /// ones, and has still to deliver, this body included: the most it was found to be.
        owed: u64,
        /// What the periods so far delivered of those classes.
        charged: u64,
        /// Whether the period now running began with everything owed delivered.
        had: bool,
        /// The body's length still to arrive.
        remaining: u64,
    },
    /// Nothing is being waited on: a served exchange whose request has arrived and whose reply
    /// has not begun is the owner's to answer.
    Idle,
}

/// The stream bytes a connection delivered to this side's reading, by the rank of the class they
/// belong to: what the answering wait of a class is charged with. Bytes whose class is not known
/// yet (a request's prefix, a lane's opening, a skipped frame) count as the most urgent class's,
/// so they are charged to no wait but that class's.
#[derive(Clone, Debug)]
pub(crate) struct Delivered {
    /// One sum per rank: as many as the project's classes have ranks.
    by_rank: Vec<u64>,
}

impl Delivered {
    pub(crate) fn new(ranks: u8) -> Self {
        Self {
            by_rank: vec![0; usize::from(ranks.max(1))],
        }
    }
    /// `bytes` of a class of `rank` were read.
    pub(crate) fn add(&mut self, rank: u8, bytes: u64) {
        let last = self.by_rank.len().saturating_sub(1);
        if let Some(sum) = self.by_rank.get_mut(usize::from(rank).min(last)) {
            *sum = sum.saturating_add(bytes);
        }
    }
    /// What was read of the classes of `rank` and the less urgent ones.
    pub(crate) fn from(&self, rank: u8) -> u64 {
        self.by_rank
            .iter()
            .skip(usize::from(rank))
            .fold(0, |sum, bytes| sum.saturating_add(*bytes))
    }
}

/// One exchange's wait.
#[derive(Clone, Copy, Debug)]
pub(crate) struct Carry {
    period: Duration,
    next: Instant,
    before: Moved,
    phase: Phase,
}

impl Carry {
    /// A wait judged every period of `progress` from `now`, with nothing to wait on yet.
    pub(crate) fn idle(progress: Progress, now: Instant, moved: Moved) -> Self {
        Self {
            period: progress.period,
            next: now.checked_add(progress.period).unwrap_or(now),
            before: moved,
            phase: Phase::Idle,
        }
    }
    /// The exchange is asking: `owed` bytes are to be sent on the connection, its own included.
    pub(crate) fn asking(&mut self, now: Instant, moved: Moved, owed: u64) {
        self.restart(now, moved);
        self.phase = Phase::Asking {
            owed,
            charged: 0,
            had: false,
        };
    }
    /// The exchange is answered: `remaining` bytes of body are to arrive, and the peer has
    /// `backlog` to deliver of the exchange's class and the less urgent ones, this body included.
    pub(crate) fn answering(&mut self, now: Instant, moved: Moved, remaining: u64, backlog: u64) {
        self.restart(now, moved);
        self.phase = Phase::Answering {
            owed: backlog.max(remaining),
            charged: 0,
            had: false,
            remaining,
        };
    }
    /// Nothing is waited on until the exchange asks or is answered again.
    pub(crate) fn rest(&mut self) {
        self.phase = Phase::Idle;
    }
    /// `bytes` of the body arrived and were read.
    pub(crate) fn arrived(&mut self, bytes: u64) {
        if let Phase::Answering { remaining, .. } = &mut self.phase {
            *remaining = remaining.saturating_sub(bytes);
        }
    }
    /// The period was this side's doing (its owner is not reading the body it is answered with,
    /// or has not written what the peer would take): no evidence against the peer, so the wait
    /// starts afresh from `now`, an answer's charge anew against the `backlog` the peer has now.
    pub(crate) fn hold(&mut self, now: Instant, moved: Moved, backlog: u64) {
        self.restart(now, moved);
        if let Phase::Answering {
            owed,
            charged,
            had,
            remaining,
        } = &mut self.phase
        {
            *owed = backlog.max(*remaining);
            *charged = 0;
            *had = false;
        }
    }
    /// The wait of an exchange queued for a stream restarts from `since`, when its connection's credit queue last
    /// advanced (policy C: a queued exchange is judged on whether its queue moves, not on its own age).
    pub(crate) fn hold_from(&mut self, since: Instant, moved: Moved, backlog: u64) {
        self.hold(since, moved, backlog);
    }

    fn restart(&mut self, now: Instant, moved: Moved) {
        self.before = moved;
        self.next = now.checked_add(self.period).unwrap_or(now);
    }
    /// When the wait is next judged, if anything is waited on.
    pub(crate) fn due(&self) -> Option<Instant> {
        (!matches!(self.phase, Phase::Idle)).then_some(self.next)
    }
    /// Judge the wait at `now`, if a judgement is due: `moved` is what the connection has moved,
    /// `held` what its exchanges have to send now, `backlog` what the peer has declared and not
    /// yet delivered of the exchange's class and the less urgent ones.
    pub(crate) fn judge(
        &mut self,
        now: Instant,
        moved: Moved,
        held: u64,
        backlog: u64,
    ) -> Result<(), Refusal> {
        if now < self.next || matches!(self.phase, Phase::Idle) {
            return Ok(());
        }
        let before = std::mem::replace(&mut self.before, moved);
        self.next = now.checked_add(self.period).unwrap_or(now);
        match &mut self.phase {
            Phase::Asking { owed, charged, had } => {
                let sent = moved.sent.saturating_sub(before.sent);
                let heard = moved.received.saturating_sub(before.received);
                if *had || sent < LEAST_PROGRESS || heard == 0 {
                    return Err(Refusal::Stalled);
                }
                *charged = charged.saturating_add(sent);
                *owed = (*owed).max(held);
                *had = *charged >= *owed;
                Ok(())
            }
            Phase::Answering {
                owed,
                charged,
                had,
                remaining,
            } => {
                let received = moved.received.saturating_sub(before.received);
                if *had || received < LEAST_PROGRESS.min(*remaining) {
                    return Err(Refusal::Stalled);
                }
                *charged = charged.saturating_add(moved.delivered.saturating_sub(before.delivered));
                *owed = (*owed).max(backlog);
                *had = *charged >= *owed;
                Ok(())
            }
            Phase::Idle => Ok(()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const PERIOD: Duration = Duration::from_millis(100);
    fn moved(sent: u64, received: u64) -> Moved {
        Moved {
            sent,
            received,
            delivered: received,
        }
    }

    /// focal's `carried`: a megabyte over a path that carries a megabit a second is given its eight
    /// seconds and more; it is never cut off at a fixed time while it moves.
    #[test]
    fn a_slow_transfer_that_moves_is_never_cut_off() {
        let start = hyper_sim::Anchor::new().instant(0).unwrap();
        let mut carry = Carry::idle(Progress::new(PERIOD).unwrap(), start, moved(0, 0));
        carry.asking(start, moved(0, 0), 1_000_000);
        // 12,500 bytes a period: a megabit a second.
        for period in 1..=80u64 {
            let now = start + PERIOD * u32::try_from(period).unwrap();
            let sent = 12_500 * period;
            carry
                .judge(
                    now,
                    moved(sent, period * 100),
                    1_000_000 - sent.min(1_000_000),
                    0,
                )
                .unwrap();
        }
        // Everything was sent by the 80th period: the peer had the request and the 81st to answer.
        let now = start + PERIOD * 81;
        assert_eq!(
            carry.judge(now, moved(1_000_000 + 1_200, 8_200), 0, 0),
            Err(Refusal::Stalled)
        );
    }

    /// A peer that died leaves the sender sending into silence: the flight in the air, then the
    /// probe timeout's probes, which RFC 9002 §6.2.4 sends whether or not the peer lives, a
    /// datagram or two each, at a backoff that doubles. Counted as progress, they kept an exchange
    /// to a killed peer alive for as many periods as held a probe: two periods on one run of the
    /// end-to-end scenario, four on another.
    #[test]
    fn what_is_sent_into_silence_is_not_progress() {
        let start = hyper_sim::Anchor::new().instant(0).unwrap();
        let mut carry = Carry::idle(Progress::new(PERIOD).unwrap(), start, moved(0, 0));
        carry.asking(start, moved(0, 0), 1_000_000);
        assert_eq!(
            carry.judge(start + PERIOD, moved(13_776, 0), 1_000_000, 0),
            Err(Refusal::Stalled)
        );
    }

    #[test]
    fn a_period_that_moves_less_than_a_datagram_ends_the_exchange() {
        let start = hyper_sim::Anchor::new().instant(0).unwrap();
        let mut carry = Carry::idle(Progress::new(PERIOD).unwrap(), start, moved(0, 0));
        carry.asking(start, moved(500, 0), 10_000);
        // Not yet due: no judgement.
        carry
            .judge(start + PERIOD / 2, moved(500, 0), 10_000, 0)
            .unwrap();
        assert_eq!(
            carry.judge(start + PERIOD, moved(500 + 1_199, 300), 10_000, 0),
            Err(Refusal::Stalled)
        );
    }

    /// An answer is charged with what the connection delivered against what the peer owed: a
    /// body that takes many periods while bytes keep arriving is never cut off, however short the
    /// path's round trip; one that stops arriving ends at the period that brought less than a
    /// datagram.
    #[test]
    fn an_answer_is_charged_with_what_arrives_and_must_keep_arriving() {
        let start = hyper_sim::Anchor::new().instant(0).unwrap();
        let mut carry = Carry::idle(Progress::new(PERIOD).unwrap(), start, moved(0, 0));
        // 1 MB at 12,000 bytes a period: 84 periods.
        carry.answering(start, moved(0, 0), 1_000_000, 1_000_000);
        for period in 1..=83u32 {
            let arrived = 12_000 * u64::from(period);
            carry
                .judge(
                    start + PERIOD * period,
                    moved(0, arrived),
                    0,
                    1_000_000 - arrived,
                )
                .unwrap();
            carry.arrived(12_000);
        }
        // A body that stops arriving ends at the period that brought less than a datagram.
        let mut carry = Carry::idle(Progress::new(PERIOD).unwrap(), start, moved(0, 0));
        carry.answering(start, moved(0, 0), 24_000, 24_000);
        assert_eq!(
            carry.judge(start + PERIOD, moved(0, 100), 0, 24_000),
            Err(Refusal::Stalled)
        );
    }

    /// A body the peer withholds while it delivers others: once the connection has delivered
    /// everything the peer owed, a period more, and the exchange ends. What a more urgent class
    /// delivers is not charged (`delivered` does not count it).
    #[test]
    fn a_withheld_body_ends_a_period_after_everything_owed_was_delivered() {
        let start = hyper_sim::Anchor::new().instant(0).unwrap();
        let mut carry = Carry::idle(Progress::new(PERIOD).unwrap(), start, moved(0, 0));
        // This body of 10,000 and another of 50,000 owed.
        carry.answering(start, moved(0, 0), 10_000, 60_000);
        let busy = |received: u64, delivered: u64| Moved {
            sent: 0,
            received,
            delivered,
        };
        // A more urgent class delivers a megabyte: none of it charged.
        carry
            .judge(start + PERIOD, busy(1_000_000, 0), 0, 60_000)
            .unwrap();
        // The other body's 50,000 arrive, and 10,000 of a third declared meanwhile.
        carry
            .judge(start + PERIOD * 2, busy(1_050_000, 50_000), 0, 20_000)
            .unwrap();
        // The third's 10,000: everything owed delivered by this period's end.
        carry
            .judge(start + PERIOD * 3, busy(1_060_000, 60_000), 0, 10_000)
            .unwrap();
        // A period more, still busy, and still not this body.
        assert_eq!(
            carry.judge(start + PERIOD * 4, busy(2_000_000, 60_000), 0, 10_000),
            Err(Refusal::Stalled)
        );
    }

    #[test]
    fn an_idle_wait_is_never_due() {
        let start = hyper_sim::Anchor::new().instant(0).unwrap();
        let carry = Carry::idle(Progress::new(PERIOD).unwrap(), start, moved(0, 0));
        assert_eq!(carry.due(), None);
        assert_eq!(Progress::new(Duration::ZERO), Err(Refusal::Configuration));
    }
}
