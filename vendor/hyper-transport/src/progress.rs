//! Waits charged to progress (mantle note 32 T39; focal `transport.rs` `carried` and `frame.rs`
//! `read_payload_arriving`, made sans-io).
//!
//! An exchange is not given a fixed wall time. It is judged once a period, and given up only when
//! a period moved less than a datagram ([`LEAST_PROGRESS`]), so "a megabyte crosses a path of
//! 4 Mbit/s ... between endpoints that give a request one second" (focal 27 §7), and an exchange
//! whose peer stopped taking it ends within a period or two on any path. A window is never judged
//! sooner than the path's probe timeout (hyper-quic's `probe_timeout`, patch Q7; RFC 9002 §6.2.1):
//! what the peer acknowledges and answers takes a round trip to say, and where the round trip is
//! longer than the period — a bottleneck's queue under load, 64 kbit/s and a 500 ms period in
//! focal's run — a period judged the path's clock rather than the peer. Two phases:
//!
//! - **Asking**: the request is being carried and the reply's prefix has not arrived. While the
//!   exchanges on the connection have bytes to send beside it, its own among them, a period is
//!   charged what the connection sent and did not lose in it, or what the peer acknowledged if that
//!   is more (a sender held by the peer's flow control on a slow path sends nothing while the peer
//!   drains the bottleneck), and ends the exchange if that is less than a datagram. Once all of it
//!   is sent it is in the network, which may hold it longer than a
//!   period — a bottleneck's queue on a slow path holds a window for seconds — and a period is
//!   charged what the peer acknowledged of what was sent (hyper-quic's `acked_bytes`, patch Q6), and
//!   ends the exchange if that is less than a datagram, unless the peer acknowledged the request
//!   whole in it: a request smaller than a datagram moves no more. The peer's time to answer begins
//!   when it has the request: when QUIC reports the exchange's stream acknowledged whole
//!   (`StreamEvent::Finished`), and the exchange ends at the end of a period that began with it so
//!   (focal's audit F38: counted from the send, the period began while the last of a request was
//!   still on its way, and ended before the peer had it; charged a datagram, a smaller request was
//!   refused at the end of the period it was sent in). A period in which the peer sent nothing at
//!   all ends it whatever was charged. A stream its peer does not read ends by its own bytes, whatever else the connection
//!   carries (focal's audit F38): out of the peer's credit (its own, or the connection's with bytes
//!   to send), at least a datagram of it sent and all of that acknowledged, it is unread, and a
//!   window more with nothing written ends it, its reset giving back the credit it holds (hyper-quic
//!   patch Q8, `SendStream::sent_acknowledged`, `written`). Written past what it had when found
//!   unread, its peer gave credit back, and the window starts again: a stream held to its peer's
//!   pace is out of credit and acknowledged at nearly every judgement while its bytes are taken. An exchange waiting only for credit that an unread
//!   one holds, or held in its window, is not judged: the credit comes back as the unread one
//!   ends. What was sent is progress only while the peer is
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
    /// Bytes of what the connection sent that the peer acknowledged.
    pub(crate) acked: u64,
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
        /// What the periods so far sent of it.
        charged: u64,
        /// Whether the period now running began with the peer holding the whole request: its
        /// stream acknowledged whole.
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
    /// When the window being judged began: the wait's start, or its last judgement.
    since: Instant,
    next: Instant,
    before: Moved,
    phase: Phase,
}

impl Carry {
    /// A wait judged every period of `progress` from `now`, with nothing to wait on yet.
    pub(crate) fn idle(progress: Progress, now: Instant, moved: Moved) -> Self {
        Self {
            period: progress.period,
            since: now,
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
        self.since = now;
        self.next = now.checked_add(self.period).unwrap_or(now);
    }
    /// When the window being judged began.
    pub(crate) fn started(&self) -> Instant {
        self.since
    }
    /// The window a judgement covers on a path whose probe timeout is `floor`.
    pub(crate) fn window(&self, floor: Duration) -> Duration {
        self.period.max(floor)
    }
    /// When the wait is next judged, if anything is waited on.
    pub(crate) fn due(&self) -> Option<Instant> {
        (!matches!(self.phase, Phase::Idle)).then_some(self.next)
    }
    /// Judge the wait at `now`, if a judgement is due: `moved` is what the connection has moved,
    /// `held` what its exchanges have to send now, `backlog` what the peer has declared and not
    /// yet delivered of the exchange's class and the less urgent ones, `acknowledged` whether
    /// the peer has acknowledged the exchange's own message whole, and `floor` the path's probe
    /// timeout. A window shorter than the floor is not judged: what the peer acknowledges and
    /// answers takes a round trip to say, and on a path whose round trip is longer than the
    /// period — a bottleneck's queue under load — a period would judge the path's clock, not the
    /// peer. The judgement waits for the floor, the window kept whole (RFC 9002 §6.2.1 declares no
    /// loss sooner either).
    pub(crate) fn judge(
        &mut self,
        now: Instant,
        moved: Moved,
        held: u64,
        backlog: u64,
        acknowledged: bool,
        floor: Duration,
    ) -> Result<(), Refusal> {
        if now < self.next || matches!(self.phase, Phase::Idle) {
            return Ok(());
        }
        let ready = self.since.checked_add(floor).unwrap_or(now);
        if now < ready {
            self.next = ready;
            return Ok(());
        }
        let before = std::mem::replace(&mut self.before, moved);
        self.since = now;
        self.next = now.checked_add(self.period).unwrap_or(now);
        match &mut self.phase {
            Phase::Asking { owed, charged, had } => {
                let sent = moved.sent.saturating_sub(before.sent);
                let acked = moved.acked.saturating_sub(before.acked);
                let heard = moved.received.saturating_sub(before.received);
                // What reached the peer moves the request as what left this side does: a sender
                // held by the peer's flow control on a slow path sends nothing while the peer drains
                // the bottleneck, and acknowledges; once all of it is sent, only what the peer
                // acknowledges moves it.
                let carried = if *charged < *owed {
                    sent.max(acked)
                } else {
                    acked
                };
                // A message the peer acknowledged whole in the window moved all it had, however
                // little: one smaller than a datagram can move no more, and the peer, which has it,
                // is given the next window to answer. Charged a datagram all the same, it was refused
                // at the window's end, one period from the send rather than from when the peer had
                // it, and the peer's "not yet" for it, sent as its own period ended, lost the race
                // whenever it was late (focal's 128 kbit/s seal: 24 ms late on a loaded host).
                let whole = acknowledged && !*had;
                if *had || (carried < LEAST_PROGRESS && !whole) || heard == 0 {
                    return Err(Refusal::Stalled);
                }
                *charged = charged.saturating_add(sent);
                *owed = (*owed).max(held);
                *had = acknowledged;
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
    /// What the connection moved, everything it sent acknowledged as it went.
    fn moved(sent: u64, received: u64) -> Moved {
        Moved {
            sent,
            acked: sent,
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
                    sent >= 1_000_000,
                    Duration::ZERO,
                )
                .unwrap();
        }
        // Everything was sent, and acknowledged, by the 80th period: the peer had the request and
        // the 81st to answer.
        let now = start + PERIOD * 81;
        assert_eq!(
            carry.judge(
                now,
                moved(1_000_000 + 1_200, 8_200),
                0,
                0,
                false,
                Duration::ZERO
            ),
            Err(Refusal::Stalled)
        );
    }

    /// A request smaller than a datagram, acknowledged whole in the window it was sent in, has
    /// moved all it had: the peer, which has it, is given the next window to answer, and is given
    /// up at that window's end. Charged a datagram, it was refused at the end of the window it was
    /// sent in (focal's 128 kbit/s seal: 93 bytes owed, 475 sent and 310 acknowledged in the
    /// window, refused before the peer's "not yet" arrived). One the peer has not acknowledged by
    /// then is refused there, as before.
    #[test]
    fn a_request_smaller_than_a_datagram_is_given_a_window_once_the_peer_has_it() {
        let start = hyper_sim::Anchor::new().instant(0).unwrap();
        let mut carry = Carry::idle(Progress::new(PERIOD).unwrap(), start, moved(0, 0));
        carry.asking(start, moved(0, 0), 93);
        assert_eq!(
            carry.judge(start + PERIOD, moved(475, 378), 0, 0, true, Duration::ZERO),
            Ok(())
        );
        assert_eq!(
            carry.judge(
                start + PERIOD * 2,
                moved(475 + 120, 378 + 60),
                0,
                0,
                true,
                Duration::ZERO
            ),
            Err(Refusal::Stalled)
        );
        let mut unacknowledged = Carry::idle(Progress::new(PERIOD).unwrap(), start, moved(0, 0));
        unacknowledged.asking(start, moved(0, 0), 93);
        assert_eq!(
            unacknowledged.judge(
                start + PERIOD,
                moved(475, 378),
                93,
                0,
                false,
                Duration::ZERO
            ),
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
            carry.judge(
                start + PERIOD,
                moved(13_776, 0),
                1_000_000,
                0,
                false,
                Duration::ZERO
            ),
            Err(Refusal::Stalled)
        );
    }

    /// A request sent whole is in the network until the peer acknowledges it, which a bottleneck's
    /// queue may hold for many periods: each period that brings the peer's acknowledgement of a
    /// datagram's worth keeps it, the peer's time to answer begins when it has acknowledged it all,
    /// and a period that brings less ends it (focal's audit F38).
    #[test]
    fn a_request_in_the_network_is_waited_on_while_the_peer_acknowledges_it() {
        let start = hyper_sim::Anchor::new().instant(0).unwrap();
        let carried = |sent: u64, acked: u64, received: u64| Moved {
            sent,
            acked,
            received,
            delivered: 0,
        };
        let mut carry = Carry::idle(Progress::new(PERIOD).unwrap(), start, carried(0, 0, 0));
        carry.asking(start, carried(0, 0, 0), 10_000);
        // Sent whole in the first period, 2,000 of it acknowledged.
        carry
            .judge(
                start + PERIOD,
                carried(10_000, 2_000, 200),
                0,
                0,
                false,
                Duration::ZERO,
            )
            .unwrap();
        // Nothing more to send: the periods that bring acknowledgements keep it.
        carry
            .judge(
                start + PERIOD * 2,
                carried(10_000, 5_000, 400),
                0,
                0,
                false,
                Duration::ZERO,
            )
            .unwrap();
        carry
            .judge(
                start + PERIOD * 3,
                carried(10_000, 10_000, 600),
                0,
                0,
                true,
                Duration::ZERO,
            )
            .unwrap();
        // The peer had it whole for a period: its time to answer is over.
        assert_eq!(
            carry.judge(
                start + PERIOD * 4,
                carried(10_000, 10_000, 800),
                0,
                0,
                true,
                Duration::ZERO
            ),
            Err(Refusal::Stalled)
        );
        // A request in the network that the peer acknowledges less than a datagram of in a period
        // ends there, heard or not.
        let mut carry = Carry::idle(Progress::new(PERIOD).unwrap(), start, carried(0, 0, 0));
        carry.asking(start, carried(0, 0, 0), 10_000);
        carry
            .judge(
                start + PERIOD,
                carried(10_000, 2_000, 200),
                0,
                0,
                false,
                Duration::ZERO,
            )
            .unwrap();
        assert_eq!(
            carry.judge(
                start + PERIOD * 2,
                carried(10_000, 2_500, 400),
                0,
                0,
                false,
                Duration::ZERO
            ),
            Err(Refusal::Stalled)
        );
    }

    /// A sender held by the peer's flow control sends nothing while the peer drains a slow path's
    /// bottleneck and acknowledges: what the peer acknowledged moves the request as what was sent
    /// does. Neither, and the period ends it.
    #[test]
    fn a_request_held_by_the_peers_credit_moves_by_what_the_peer_acknowledges() {
        let start = hyper_sim::Anchor::new().instant(0).unwrap();
        let carried = |sent: u64, acked: u64, received: u64| Moved {
            sent,
            acked,
            received,
            delivered: 0,
        };
        let mut carry = Carry::idle(Progress::new(PERIOD).unwrap(), start, carried(0, 0, 0));
        carry.asking(start, carried(0, 0, 0), 30_000);
        let none = Duration::ZERO;
        carry
            .judge(
                start + PERIOD,
                carried(12_000, 0, 200),
                18_000,
                0,
                false,
                none,
            )
            .unwrap();
        // Nothing sent: the peer's credit is spent; 4,800 of it acknowledged.
        carry
            .judge(
                start + PERIOD * 2,
                carried(12_000, 4_800, 400),
                18_000,
                0,
                false,
                none,
            )
            .unwrap();
        assert_eq!(
            carry.judge(
                start + PERIOD * 3,
                carried(12_000, 4_900, 600),
                18_000,
                0,
                false,
                none
            ),
            Err(Refusal::Stalled)
        );
    }

    /// A window shorter than the path's probe timeout is not judged: on a path whose round trip is
    /// longer than the period, what the peer acknowledges and answers cannot come within one. The
    /// judgement waits for the floor with the window kept whole, and judges all of it.
    #[test]
    fn a_window_is_judged_no_sooner_than_the_probe_timeout() {
        let start = hyper_sim::Anchor::new().instant(0).unwrap();
        let floor = PERIOD * 3;
        let mut carry = Carry::idle(Progress::new(PERIOD).unwrap(), start, moved(0, 0));
        carry.asking(start, moved(0, 0), 10_000);
        // Due by the period, and nothing moved yet: not judged, but put off to the floor.
        carry
            .judge(start + PERIOD, moved(0, 0), 10_000, 0, false, floor)
            .unwrap();
        assert_eq!(carry.due(), Some(start + floor));
        // Judged at the floor on everything since the start: it moved.
        carry
            .judge(start + floor, moved(10_000, 300), 0, 0, false, floor)
            .unwrap();
        // The next window is the floor's too: nothing in it ends the exchange at its end.
        carry
            .judge(
                start + floor + PERIOD,
                moved(10_000, 300),
                0,
                0,
                false,
                floor,
            )
            .unwrap();
        assert_eq!(
            carry.judge(start + floor * 2, moved(10_000, 300), 0, 0, false, floor),
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
            .judge(
                start + PERIOD / 2,
                moved(500, 0),
                10_000,
                0,
                false,
                Duration::ZERO,
            )
            .unwrap();
        assert_eq!(
            carry.judge(
                start + PERIOD,
                moved(500 + 1_199, 300),
                10_000,
                0,
                false,
                Duration::ZERO
            ),
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
                    false,
                    Duration::ZERO,
                )
                .unwrap();
            carry.arrived(12_000);
        }
        // A body that stops arriving ends at the period that brought less than a datagram.
        let mut carry = Carry::idle(Progress::new(PERIOD).unwrap(), start, moved(0, 0));
        carry.answering(start, moved(0, 0), 24_000, 24_000);
        assert_eq!(
            carry.judge(
                start + PERIOD,
                moved(0, 100),
                0,
                24_000,
                false,
                Duration::ZERO
            ),
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
            acked: 0,
            received,
            delivered,
        };
        // A more urgent class delivers a megabyte: none of it charged.
        carry
            .judge(
                start + PERIOD,
                busy(1_000_000, 0),
                0,
                60_000,
                false,
                Duration::ZERO,
            )
            .unwrap();
        // The other body's 50,000 arrive, and 10,000 of a third declared meanwhile.
        carry
            .judge(
                start + PERIOD * 2,
                busy(1_050_000, 50_000),
                0,
                20_000,
                false,
                Duration::ZERO,
            )
            .unwrap();
        // The third's 10,000: everything owed delivered by this period's end.
        carry
            .judge(
                start + PERIOD * 3,
                busy(1_060_000, 60_000),
                0,
                10_000,
                false,
                Duration::ZERO,
            )
            .unwrap();
        // A period more, still busy, and still not this body.
        assert_eq!(
            carry.judge(
                start + PERIOD * 4,
                busy(2_000_000, 60_000),
                0,
                10_000,
                false,
                Duration::ZERO
            ),
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
