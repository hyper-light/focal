//! Careful Resume (RFC 9959): a connection starts from half the capacity an earlier connection to
//! the same remote measured, once its own first round trip confirms the path, and retreats on the
//! first congestion (`docs/research/quic-overhead.md` §1)
//!
//! A connection observes what it delivers each round trip (§3.1). At its close the endpoint keeps
//! the observation for the remote's IP address in a [`CongestionMemory`], and hands it to the next
//! connection to that address, one connection at a time. That connection runs the phases of §3.2
//! to §3.5 over its congestion controller's window.
//!
//! A connection to a remote the memory holds nothing for measures the path while it is idle (the
//! warm-up): ack-eliciting PING and PADDING packets of the path's MTU, under the window and the
//! pacer and only when nothing else is to be sent, until four initial windows are acknowledged in
//! one round trip (§3.1's floor), congestion is met, or its budget is spent. The measurement goes to
//! the endpoint as soon as it is made. Connections that carry only small exchanges never deliver
//! four initial windows a round trip, so without it a path between two nodes would never be
//! measured and every burst on it would wait on the initial window
//! (`docs/research/quic-overhead.md` §5).

use std::collections::VecDeque;
use std::net::IpAddr;

use super::RttEstimator;
use crate::congestion::Controller;
use crate::{Duration, Instant};

/// What a connection measured of its path, for a later connection to the same remote (§3.1)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Saved {
    /// The bytes acknowledged in one round trip, the most the connection saw: `saved_cwnd`
    pub(crate) cwnd: u64,
    /// The connection's minimum RTT: `saved_rtt`
    pub(crate) rtt: Duration,
    /// When the measurement was taken, from which its lifetime runs (§2.4)
    pub(crate) at: Instant,
}

/// What the memory holds for one remote
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Held {
    /// A measurement no connection is using
    Saved(Saved),
    /// A connection is using the measurement or measuring the path, and gives it back or replaces
    /// it at its close
    Out,
    /// A connection that held the remote from this time warmed the path up and closed with nothing
    /// to keep: its warm-up met congestion or spent its budget.
    /// No connection warms the path up again until the lifetime has passed, so a path the warm-up
    /// cannot measure costs one budget a lifetime, not one a connection.
    Tried(Instant),
}

/// What a new connection is given for its remote
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Grant {
    /// A measurement to resume from
    Resume(Saved),
    /// Nothing is known of the path and no other connection is measuring it: this one measures it
    Measure,
    /// Another connection holds the remote's measurement or is measuring its path, or one tried
    /// within the lifetime and kept nothing
    Neither,
}

/// The measurements an endpoint holds, one per remote IP address (§3.1: "A sender MUST NOT retain
/// more than one set of CC parameters for a Remote Endpoint"), at most `capacity` remotes, the
/// oldest replaced first; a remote whose measurement a connection holds counts toward the bound
pub(crate) struct CongestionMemory {
    entries: VecDeque<(IpAddr, Held)>,
    capacity: usize,
}

impl CongestionMemory {
    pub(crate) fn new(capacity: usize) -> Self {
        Self {
            entries: VecDeque::new(),
            capacity,
        }
    }

    /// What a new connection to `remote` is given. A measurement younger than `lifetime` is taken
    /// out of the memory while one connection uses it, so no second connection starts from it at
    /// once (§3.2); with none, the connection measures the path, and other connections to the
    /// remote meanwhile neither resume nor measure it again
    pub(crate) fn take(&mut self, remote: IpAddr, now: Instant, lifetime: Duration) -> Grant {
        if self.capacity == 0 {
            return Grant::Neither;
        }
        let held = self
            .entries
            .iter_mut()
            .find(|(ip, _)| *ip == remote)
            .map(|(_, held)| held);
        match held {
            Some(Held::Out) => Grant::Neither,
            Some(Held::Tried(at)) if now.saturating_duration_since(*at) <= lifetime => {
                Grant::Neither
            }
            Some(held) => {
                let grant = match *held {
                    Held::Saved(saved) if now.saturating_duration_since(saved.at) <= lifetime => {
                        Grant::Resume(saved)
                    }
                    _ => Grant::Measure,
                };
                *held = Held::Out;
                grant
            }
            None => {
                self.insert(remote, Held::Out);
                Grant::Measure
            }
        }
    }

    /// Keeps `saved` for `remote`, in place of anything held for it
    pub(crate) fn put(&mut self, remote: IpAddr, saved: Saved) {
        if self.capacity == 0 {
            return;
        }
        if let Some(at) = self.entries.iter().position(|(ip, _)| *ip == remote) {
            self.entries.remove(at);
        }
        self.insert(remote, Held::Saved(saved));
    }

    /// The connection that held `remote` closed with nothing to keep and without warming the path
    /// up: the next connection measures it
    pub(crate) fn release(&mut self, remote: IpAddr) {
        if let Some(at) = self
            .entries
            .iter()
            .position(|(ip, held)| *ip == remote && *held == Held::Out)
        {
            self.entries.remove(at);
        }
    }

    /// The connection that held `remote` from `since` warmed the path up and closed with nothing
    /// to keep
    pub(crate) fn tried(&mut self, remote: IpAddr, since: Instant) {
        if let Some(held) = self
            .entries
            .iter_mut()
            .find(|(ip, held)| *ip == remote && *held == Held::Out)
            .map(|(_, held)| held)
        {
            *held = Held::Tried(since);
        }
    }

    fn insert(&mut self, remote: IpAddr, held: Held) {
        while self.entries.len() >= self.capacity {
            self.entries.pop_front();
        }
        self.entries.push_back((remote, held));
    }

    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.entries.len()
    }
}

/// The bytes acknowledged in each round trip of a connection, and the most of them (§3.1: "This
/// could be computed by measuring the volume of data acknowledged in one RTT")
///
/// A round trip is counted by packet numbers, not by the clock: it begins at an acknowledgement and
/// ends with the first acknowledgement of a Data packet sent after it, a packet-timed round trip as
/// BBR counts them (draft-ietf-ccwg-bbr-06 §5.5.1). Counted by the smoothed RTT on the clock, a path
/// whose round trip is shorter than the gap between acknowledgements (a LAN, or loopback) closed a
/// count at every acknowledgement and never saw a window's worth delivered.
#[derive(Debug, Default)]
struct Observer {
    /// The largest Data packet sent or acknowledged when the current round trip began: an
    /// acknowledgement of a larger one ends it. None before the first acknowledgement.
    mark: Option<u64>,
    /// The bytes acknowledged in the current round trip, from after the acknowledgement that began
    /// it
    acked: u64,
    /// The most acknowledged in one round trip, and when that round trip ended
    best: Option<(u64, Instant)>,
}

impl Observer {
    fn on_ack(
        &mut self,
        now: Instant,
        bytes: u64,
        largest_acked: Option<u64>,
        largest_sent: Option<u64>,
    ) {
        // `largest_sent` is the largest ack-eliciting packet in flight's, none once all are
        // acknowledged: the largest acknowledged stands for it then
        let now_mark = largest_sent.max(largest_acked);
        let Some(mark) = self.mark else {
            self.mark = now_mark;
            return;
        };
        self.acked = self.acked.saturating_add(bytes);
        if largest_acked.is_some_and(|acked| acked > mark) {
            if self.best.is_none_or(|(best, _)| self.acked > best) {
                self.best = Some((self.acked, now));
            }
            self.mark = now_mark;
            self.acked = 0;
        }
    }
}

/// What a closing connection leaves in its endpoint's memory for its remote
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Leaves {
    /// A measurement to keep
    Keep(Saved),
    /// Nothing, from a connection that held the remote's mark from this time and warmed the path up
    Tried(Instant),
    /// Nothing, from a connection that held the remote's mark and did not warm the path up: the
    /// mark is released
    Release,
    /// Nothing, and the memory is left as it is
    Nothing,
}

/// A phase of RFC 9959 §3
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Phase {
    /// Normal congestion control: nothing saved to resume from, or done with it
    Off,
    /// §3.2: the initial window under normal congestion control, until the data sent in the first
    /// round trip is acknowledged without congestion. `initial_end` is set by the first
    /// acknowledgement in any space to the largest ack-eliciting Data packet sent before it, or
    /// none; `ready` that every such packet is acknowledged.
    Reconnaissance {
        initial_end: Option<Option<u64>>,
        ready: bool,
    },
    /// §3.3: the window at the jump. `first` is the first Data packet sent in the phase, `last`
    /// the last, `pipe` the PipeSize.
    Unvalidated {
        first: u64,
        last: Option<u64>,
        pipe: u64,
        entered: Instant,
    },
    /// §3.4: normal congestion control until `last`, the last packet sent while Unvalidated, is
    /// acknowledged
    Validating { last: u64, pipe: u64 },
    /// §3.5: half the PipeSize, not increased, until `last` is acknowledged
    Retreat { last: u64, pipe: u64 },
}

/// The warm-up's state
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WarmUp {
    /// Not measuring: the connection was given a measurement, another connection is measuring, or
    /// the warm-up ended
    Off,
    /// Measuring; `sent` the bytes its packets have carried
    Measuring { sent: u64 },
}

/// One connection's Careful Resume
#[derive(Debug)]
pub(crate) struct CarefulResume {
    phase: Phase,
    /// The measurement this connection was given, returned at its close unless it retreated
    given: Option<Saved>,
    /// When the endpoint's memory marked this connection's remote as held by it, a mark it
    /// replaces at its close; none if it holds no mark
    holds: Option<Instant>,
    /// The configured maximum jump (§2.4's `max_jump`)
    max_jump: u64,
    observer: Observer,
    warm_up: WarmUp,
    /// Whether the warm-up sent anything
    warmed: bool,
    /// Whether the warm-up ended without its measurement, on congestion or at its budget
    failed: bool,
}

/// RFC 9959 §3.1: a measurement below four initial windows "would not justify" Careful Resume, and
/// the sender "can choose to not save" it; hyper-quic does not. The warm-up measures to it and no
/// further: the least measurement Careful Resume keeps.
const SAVE_FLOOR_WINDOWS: u64 = 4;

/// The warm-up's budget, in multiples of its target. Slow start doubles the window a round trip, and
/// a round trip's count can straddle two of the sender's rounds, so the target is met for certain
/// once a window of twice the target has been sent; the rounds that grew the window to it carried
/// less than that window again (a geometric sum), so all of it is below four times the target.
const WARM_UP_BUDGET_TARGETS: u64 = 4;

/// RFC 9959 §3.2 and §4.2.1: a minimum RTT at or below half the saved RTT stops Careful Resume, and
/// one past ten times it "is indicative of a path change"
const RTT_TOO_SMALL_DIVISOR: u32 = 2;
/// See [`RTT_TOO_SMALL_DIVISOR`]
const RTT_PATH_CHANGE_FACTOR: u32 = 10;

/// RFC 9959 §3.3: "jump_cwnd MUST be no more than half of the saved_cwnd"
const JUMP_DIVISOR: u64 = 2;

/// RFC 9959 §3.5: on leaving Safe Retreat, "ssthresh MUST be set to no larger than the most
/// recently measured PipeSize * Beta", Beta 0.5 by default; and on entering it the window is "no
/// more than (PipeSize/2)"
const RETREAT_DIVISOR: u64 = 2;

impl CarefulResume {
    /// A connection given `grant` by its endpoint's memory at `now`; `warm_up` whether a connection
    /// told to measure its path warms it up (else it measures only what it carries)
    pub(crate) fn new(grant: Grant, max_jump: u64, warm_up: bool, now: Instant) -> Self {
        let given = match grant {
            Grant::Resume(saved) => Some(saved),
            Grant::Measure | Grant::Neither => None,
        };
        Self {
            phase: match given {
                Some(_) => Phase::Reconnaissance {
                    initial_end: None,
                    ready: false,
                },
                None => Phase::Off,
            },
            given,
            holds: (grant != Grant::Neither).then_some(now),
            max_jump,
            observer: Observer::default(),
            warm_up: match grant {
                Grant::Measure if warm_up => WarmUp::Measuring { sent: 0 },
                _ => WarmUp::Off,
            },
            warmed: false,
            failed: false,
        }
    }

    /// Whether a warm-up packet of `bytes` may go: the connection is measuring, the measurement is
    /// short of its target and the budget has room for it (the window and the pacer are the
    /// caller's to check). A budget spent ends the warm-up.
    pub(crate) fn warm_up_wants(&mut self, initial_window: u64, bytes: u64) -> bool {
        let WarmUp::Measuring { sent } = self.warm_up else {
            return false;
        };
        let target = initial_window.saturating_mul(SAVE_FLOOR_WINDOWS);
        let budget = target.saturating_mul(WARM_UP_BUDGET_TARGETS);
        if self.observer.best.is_some_and(|(best, _)| best >= target) {
            // Met: `measured` publishes it at the acknowledgement that met it
            return false;
        }
        if sent.saturating_add(bytes) > budget {
            self.warm_up = WarmUp::Off;
            self.failed = true;
            return false;
        }
        true
    }

    /// A warm-up packet of `bytes` was sent
    pub(crate) fn on_warm_up_sent(&mut self, bytes: u64) {
        if let WarmUp::Measuring { sent } = &mut self.warm_up {
            *sent = sent.saturating_add(bytes);
            self.warmed = true;
        }
    }

    /// The warm-up's measurement, once, as soon as it reaches its target: the endpoint keeps it at
    /// once, so the next connection to the remote resumes from it while this one is still open
    pub(crate) fn measured(&mut self, initial_window: u64, min_rtt: Duration) -> Option<Saved> {
        if !matches!(self.warm_up, WarmUp::Measuring { .. }) {
            return None;
        }
        let saved = self.own(initial_window, min_rtt)?;
        self.warm_up = WarmUp::Off;
        self.holds = None;
        Some(saved)
    }

    /// Whether the controller's window is held as it is, not grown by acknowledgements: in the
    /// Unvalidated Phase (the window is the jump's) and in Safe Retreat (§3.5: "The CWND MUST NOT
    /// be increased")
    pub(crate) fn holds_window(&self) -> bool {
        matches!(
            self.phase,
            Phase::Unvalidated { .. } | Phase::Retreat { .. }
        )
    }

    /// The pacing rate in the Unvalidated Phase, bytes a second: one window a smoothed round trip
    /// (§3.3: "All packets sent in the Unvalidated Phase MUST use pacing based on the current
    /// RTT"; §4.3's ITT = RTT × MPS / jump_cwnd)
    pub(crate) fn pacing_rate(&self, window: u64, rtt: Duration) -> Option<u64> {
        if !matches!(self.phase, Phase::Unvalidated { .. }) {
            return None;
        }
        let nanos = rtt.as_nanos().max(1);
        let rate = u128::from(window)
            .saturating_mul(1_000_000_000)
            .checked_div(nanos)?;
        Some(u64::try_from(rate).unwrap_or(u64::MAX))
    }

    /// An acknowledgement was processed: `bytes` newly acknowledged ack-eliciting bytes, the Data
    /// space's largest acknowledged packet and largest ack-eliciting packet sent, the bytes in
    /// flight after it
    pub(crate) fn on_ack(
        &mut self,
        now: Instant,
        bytes: u64,
        largest_data_acked: Option<u64>,
        data_sent: Option<u64>,
        in_flight: u64,
        rtt: &RttEstimator,
        controller: &mut dyn Controller,
    ) {
        self.observer
            .on_ack(now, bytes, largest_data_acked, data_sent);
        let acked_past = |pn: u64| largest_data_acked.is_some_and(|largest| largest >= pn);
        match &mut self.phase {
            Phase::Off => {}
            Phase::Reconnaissance { initial_end, ready } => {
                *ready |= match *initial_end.get_or_insert(data_sent) {
                    Some(end) => acked_past(end),
                    None => true,
                };
            }
            Phase::Unvalidated {
                first,
                last,
                pipe,
                entered,
            } => {
                *pipe = pipe.saturating_add(bytes);
                let (first, last, pipe) = (*first, *last, *pipe);
                if acked_past(first) || now.saturating_duration_since(*entered) > rtt.get() {
                    self.leave_unvalidated(last, pipe, in_flight, controller);
                }
            }
            Phase::Validating { last, pipe } => {
                *pipe = pipe.saturating_add(bytes);
                if acked_past(*last) {
                    self.phase = Phase::Off;
                }
            }
            Phase::Retreat { last, pipe } => {
                *pipe = pipe.saturating_add(bytes);
                if acked_past(*last) {
                    controller.set_ssthresh(*pipe / RETREAT_DIVISOR);
                    self.phase = Phase::Off;
                }
            }
        }
    }

    /// The congestion controller reacted to congestion (loss or ECN-CE): Reconnaissance stops using
    /// Careful Resume (§3.2), the Unvalidated and Validating Phases retreat (§3.3, §3.4)
    pub(crate) fn on_congestion(&mut self, controller: &mut dyn Controller) {
        // A warm-up measures the path as it was before congestion and goes no further (§4.1: an
        // overshoot is no basis for the capacity); what it had delivered is kept at the close
        if matches!(self.warm_up, WarmUp::Measuring { .. }) && self.warmed {
            self.failed = true;
        }
        self.warm_up = WarmUp::Off;
        match self.phase {
            Phase::Off | Phase::Retreat { .. } => {}
            Phase::Reconnaissance { .. } => self.phase = Phase::Off,
            Phase::Unvalidated {
                first, last, pipe, ..
            } => {
                // Nothing sent since the jump: nothing of it to wait for
                self.retreat(last.unwrap_or(first), pipe, controller);
            }
            Phase::Validating { last, pipe } => self.retreat(last, pipe, controller),
        }
    }

    /// §3.5: the saved measurement is deleted and the window halved from the PipeSize
    fn retreat(&mut self, last: u64, pipe: u64, controller: &mut dyn Controller) {
        self.given = None;
        let window = controller.window().min(pipe / RETREAT_DIVISOR);
        controller.set_window(window);
        self.phase = Phase::Retreat { last, pipe };
    }

    /// The window would hold back the next datagram. In Reconnaissance with the initial data
    /// acknowledged, this is the deferred jump (§3.2: "this transition MAY be deferred to the time
    /// at which more data is sent than would have been normally permitted by the CC algorithm"):
    /// returns whether the window grew
    ///
    /// The jump is taken only where it gains (§3.2 makes it a MAY). It holds the window for a round
    /// trip (§3.3), where slow start would double it: a jump below twice the window loses to slow
    /// start on any transfer longer than the round trip, and gains only if what is in flight and
    /// `waiting` (the streams' bytes never sent) all fit in it, so the burst ends this round trip.
    /// Measured: a mebibyte reply resumed from a measurement of 70.8 kB jumped from 29.8 kB to
    /// 35.4 kB and came 185 ms later than slow start.
    pub(crate) fn on_window_blocked(
        &mut self,
        now: Instant,
        next_data_pn: u64,
        in_flight: u64,
        waiting: u64,
        rtt: &RttEstimator,
        controller: &mut dyn Controller,
    ) -> bool {
        let (Phase::Reconnaissance { ready: true, .. }, Some(saved)) = (self.phase, self.given)
        else {
            return false;
        };
        // §3.2 and §4.2.1: the path must still be the saved one
        let too_small = saved
            .rtt
            .checked_div(RTT_TOO_SMALL_DIVISOR)
            .is_some_and(|half| rtt.min() <= half);
        let path_changed = rtt.get() > saved.rtt.saturating_mul(RTT_PATH_CHANGE_FACTOR);
        let jump = (saved.cwnd / JUMP_DIVISOR).min(self.max_jump);
        let window = controller.window();
        let slow_start = controller
            .metrics()
            .ssthresh
            .is_some_and(|ssthresh| window < ssthresh);
        let gains = jump > window
            && (!slow_start
                || jump >= window.saturating_mul(2)
                || in_flight.saturating_add(waiting) <= jump);
        if too_small || path_changed || !gains || !controller.set_window(jump) {
            self.phase = Phase::Off;
            return false;
        }
        self.phase = Phase::Unvalidated {
            first: next_data_pn,
            last: None,
            pipe: in_flight,
            entered: now,
        };
        true
    }

    /// A datagram whose last packet is Data packet `pn` was sent, leaving `in_flight` bytes in
    /// flight: §3.3's first exit, "when the flight_size equals the CWND"
    pub(crate) fn on_data_sent(
        &mut self,
        pn: u64,
        in_flight: u64,
        controller: &mut dyn Controller,
    ) {
        if let Phase::Unvalidated { last, pipe, .. } = &mut self.phase {
            *last = Some(pn);
            let pipe = *pipe;
            if in_flight >= controller.window() {
                self.leave_unvalidated(Some(pn), pipe, in_flight, controller);
            }
        }
    }

    /// §3.3, on any exit event: "if the flight_size is less than the IW or if the flight_size is
    /// less than or equal to the PipeSize", the window becomes the PipeSize (not below the IW) and
    /// Careful Resume ends; otherwise the window becomes the flight size and Validating follows
    fn leave_unvalidated(
        &mut self,
        last: Option<u64>,
        pipe: u64,
        in_flight: u64,
        controller: &mut dyn Controller,
    ) {
        let iw = controller.initial_window();
        match last {
            Some(last) if in_flight >= iw && in_flight > pipe => {
                controller.set_window(in_flight);
                self.phase = Phase::Validating { last, pipe };
            }
            _ => {
                controller.set_window(pipe.max(iw));
                self.phase = Phase::Off;
            }
        }
    }

    /// The path changed (a migration): Careful Resume and the warm-up stop (§3.2)
    pub(crate) fn on_path_change(&mut self) {
        self.phase = Phase::Off;
        self.given = None;
        self.observer = Observer::default();
        self.warm_up = WarmUp::Off;
    }

    /// What this connection leaves for the next one to its remote at its close: its own
    /// measurement if it reached four initial windows (§3.1), else the one it was given and did not
    /// retreat from; else, if the memory marks the remote as held by it, that its warm-up failed (on
    /// congestion or at its budget), or that it releases the mark: a connection closed before or
    /// during its warm-up spent at most one budget, and the next connection may measure
    pub(crate) fn observed(&self, initial_window: u64, min_rtt: Duration) -> Leaves {
        match (self.own(initial_window, min_rtt).or(self.given), self.holds) {
            (Some(saved), _) => Leaves::Keep(saved),
            (None, Some(since)) if self.failed => Leaves::Tried(since),
            (None, Some(_)) => Leaves::Release,
            (None, None) => Leaves::Nothing,
        }
    }

    /// This connection's own measurement, if it reached four initial windows (§3.1)
    fn own(&self, initial_window: u64, min_rtt: Duration) -> Option<Saved> {
        let floor = initial_window.saturating_mul(SAVE_FLOOR_WINDOWS);
        let (cwnd, at) = self.observer.best.filter(|(cwnd, _)| *cwnd >= floor)?;
        Some(Saved {
            cwnd,
            rtt: min_rtt,
            at,
        })
    }

    #[cfg(test)]
    pub(crate) fn phase_name(&self) -> &'static str {
        match self.phase {
            Phase::Off => "off",
            Phase::Reconnaissance { .. } => "reconnaissance",
            Phase::Unvalidated { .. } => "unvalidated",
            Phase::Validating { .. } => "validating",
            Phase::Retreat { .. } => "retreat",
        }
    }
}

#[cfg(test)]
mod tests {
    use std::net::Ipv4Addr;

    use super::*;
    use crate::congestion::{NewReno, NewRenoConfig};

    const SECOND: Duration = Duration::from_secs(1);
    /// NewReno's default initial window at QUIC's 1,200-byte minimum (RFC 9002 §7.2)
    const IW: u64 = 12_000;

    fn controller(now: Instant) -> NewReno {
        NewReno::new(NewRenoConfig::default(), now, 1200)
    }

    fn rtt(sample: Duration) -> RttEstimator {
        let mut rtt = RttEstimator::new(Duration::from_millis(333));
        rtt.update(Duration::ZERO, sample);
        rtt
    }

    fn saved(cwnd: u64, at: Instant) -> Saved {
        Saved {
            cwnd,
            rtt: SECOND,
            at,
        }
    }

    /// A connection given `cwnd` whose initial data (Data packet 0) is acknowledged
    fn ready(cwnd: u64, now: Instant, controller: &mut NewReno) -> CarefulResume {
        let mut resume = CarefulResume::new(Grant::Resume(saved(cwnd, now)), u64::MAX, false, now);
        resume.on_ack(now, 0, None, Some(0), 0, &rtt(SECOND), controller);
        assert_eq!(resume.phase_name(), "reconnaissance");
        resume.on_ack(now, 1_000, Some(0), Some(0), 0, &rtt(SECOND), controller);
        resume
    }

    #[test]
    fn the_memory_keeps_one_measurement_per_address_for_its_lifetime() {
        let now = Instant::now();
        let (a, b) = (
            IpAddr::from(Ipv4Addr::LOCALHOST),
            IpAddr::from([10, 0, 0, 1]),
        );
        let mut memory = CongestionMemory::new(1);
        memory.put(a, saved(1, now));
        memory.put(a, saved(2, now));
        assert_eq!(memory.len(), 1);
        // One past the bound replaces the oldest
        memory.put(b, saved(3, now));
        // A taken measurement is out until it is put back, and no other connection measures
        assert_eq!(memory.take(b, now, SECOND), Grant::Resume(saved(3, now)));
        assert_eq!(memory.take(b, now, SECOND), Grant::Neither);
        // Past its lifetime it is no measurement: the path is measured again
        memory.put(b, saved(4, now));
        assert_eq!(memory.take(b, now + 2 * SECOND, SECOND), Grant::Measure);
        assert_eq!(memory.take(b, now + 2 * SECOND, SECOND), Grant::Neither);
        assert_eq!(memory.len(), 1);
    }

    #[test]
    fn one_connection_measures_an_unknown_remote_and_a_failed_attempt_waits_a_lifetime() {
        let now = Instant::now();
        let a = IpAddr::from(Ipv4Addr::LOCALHOST);
        let mut memory = CongestionMemory::new(2);
        assert_eq!(memory.take(a, now, SECOND), Grant::Measure);
        // While it measures, others neither measure nor resume
        assert_eq!(memory.take(a, now, SECOND), Grant::Neither);
        // It kept nothing: no connection measures again within the lifetime of its attempt
        memory.tried(a, now);
        assert_eq!(memory.take(a, now + SECOND, SECOND), Grant::Neither);
        assert_eq!(memory.take(a, now + 2 * SECOND, SECOND), Grant::Measure);
        // A release lets the next connection measure at once
        assert_eq!(memory.take(a, now + 2 * SECOND, SECOND), Grant::Neither);
        memory.release(a);
        assert_eq!(memory.take(a, now + 2 * SECOND, SECOND), Grant::Measure);
        // A measurement replaces the mark, and a later attempt's end no longer touches it
        memory.put(a, saved(5, now));
        memory.tried(a, now);
        memory.release(a);
        assert_eq!(memory.take(a, now, SECOND), Grant::Resume(saved(5, now)));
        // A memory of no remotes grants nothing
        assert_eq!(
            CongestionMemory::new(0).take(a, now, SECOND),
            Grant::Neither
        );
    }

    #[test]
    fn the_warm_up_measures_to_four_initial_windows_and_publishes_once() {
        let now = Instant::now();
        let mut cc = controller(now);
        let mut resume = CarefulResume::new(Grant::Measure, u64::MAX, true, now);
        assert!(resume.warm_up_wants(IW, 1_200));
        resume.on_ack(now, 1_000, Some(0), Some(10), 0, &rtt(SECOND), &mut cc);
        // 47,000 bytes a round trip: short of the target, nothing to publish
        resume.on_ack(
            now + SECOND,
            47_000,
            Some(11),
            Some(20),
            0,
            &rtt(SECOND),
            &mut cc,
        );
        assert_eq!(resume.measured(IW, SECOND), None);
        assert!(resume.warm_up_wants(IW, 1_200));
        // 48,000, four initial windows: published at once, and the warm-up ends
        resume.on_ack(
            now + 2 * SECOND,
            48_000,
            Some(21),
            Some(30),
            0,
            &rtt(SECOND),
            &mut cc,
        );
        let published = Saved {
            cwnd: 48_000,
            rtt: SECOND,
            at: now + 2 * SECOND,
        };
        assert_eq!(resume.measured(IW, SECOND), Some(published));
        assert_eq!(resume.measured(IW, SECOND), None);
        assert!(!resume.warm_up_wants(IW, 1_200));
        // At the close it is kept again, the mark having been replaced by it
        assert_eq!(resume.observed(IW, SECOND), Leaves::Keep(published));
    }

    #[test]
    fn the_warm_up_stops_at_its_budget_and_at_congestion_and_only_when_asked() {
        let now = Instant::now();
        let mut cc = controller(now);
        // Four times four initial windows of budget
        let mut resume = CarefulResume::new(Grant::Measure, u64::MAX, true, now);
        resume.on_warm_up_sent(16 * IW - 1_200);
        assert!(resume.warm_up_wants(IW, 1_200));
        assert!(!resume.warm_up_wants(IW, 1_201));

        // Closed before its warm-up sent anything: the mark is released, no attempt recorded
        let resume = CarefulResume::new(Grant::Measure, u64::MAX, true, now);
        assert_eq!(resume.observed(IW, SECOND), Leaves::Release);
        let mut resume = CarefulResume::new(Grant::Measure, u64::MAX, true, now);
        resume.on_warm_up_sent(1_200);
        resume.on_congestion(&mut cc);
        assert!(!resume.warm_up_wants(IW, 1_200));
        // It warmed the path up and measured nothing: the attempt is recorded at the close
        assert_eq!(resume.observed(IW, SECOND), Leaves::Tried(now));

        // Told to measure without a warm-up, or given a measurement, or neither: no warm-up
        for (grant, warm_up) in [
            (Grant::Measure, false),
            (Grant::Resume(saved(100_000, now)), true),
            (Grant::Neither, true),
        ] {
            let mut resume = CarefulResume::new(grant, u64::MAX, warm_up, now);
            assert!(!resume.warm_up_wants(IW, 1_200), "{grant:?}");
        }
        assert_eq!(
            CarefulResume::new(Grant::Neither, u64::MAX, true, now).observed(IW, SECOND),
            Leaves::Nothing
        );
    }

    #[test]
    fn a_blocked_window_jumps_to_half_the_measurement_once_the_initial_data_is_acknowledged() {
        let now = Instant::now();
        let mut cc = controller(now);
        let mut resume =
            CarefulResume::new(Grant::Resume(saved(100_000, now)), u64::MAX, false, now);
        // Before the initial data is acknowledged, no jump
        assert!(!resume.on_window_blocked(now, 1, IW, 0, &rtt(SECOND), &mut cc));
        let mut resume = ready(100_000, now, &mut cc);
        assert!(resume.on_window_blocked(now, 10, IW, 0, &rtt(SECOND), &mut cc));
        assert_eq!(cc.window(), 50_000);
        assert_eq!(resume.phase_name(), "unvalidated");
        assert!(resume.holds_window());
        // Paced at one window a round trip
        assert_eq!(resume.pacing_rate(50_000, SECOND), Some(50_000));
    }

    #[test]
    fn a_jump_below_twice_the_window_is_taken_only_for_a_burst_that_fits_it() {
        let now = Instant::now();
        // A measurement of 36,000: a jump to 18,000 from the 12,000 window of slow start
        let mut cc = controller(now);
        let mut resume = ready(36_000, now, &mut cc);
        // A transfer past the jump: slow start's doubling beats it, no jump
        assert!(!resume.on_window_blocked(now, 10, IW, 100_000, &rtt(SECOND), &mut cc));
        assert_eq!(resume.phase_name(), "off");
        assert_eq!(cc.window(), IW);
        // A burst that fits it: the jump ends it this round trip
        let mut cc = controller(now);
        let mut resume = ready(36_000, now, &mut cc);
        assert!(resume.on_window_blocked(now, 10, IW, 6_000, &rtt(SECOND), &mut cc));
        assert_eq!(cc.window(), 18_000);
    }

    #[test]
    fn max_jump_bounds_the_jump() {
        let now = Instant::now();
        let mut cc = controller(now);
        let mut resume = ready(100_000, now, &mut cc);
        resume.max_jump = 30_000;
        assert!(resume.on_window_blocked(now, 10, IW, 0, &rtt(SECOND), &mut cc));
        assert_eq!(cc.window(), 30_000);
    }

    #[test]
    fn a_different_path_or_a_small_measurement_stops_careful_resume() {
        let now = Instant::now();
        for (sample, cwnd) in [
            // RTT at most half the saved RTT
            (Duration::from_millis(500), 100_000),
            // RTT past ten times the saved RTT
            (Duration::from_secs(11), 100_000),
            // Half the measurement no more than the window
            (SECOND, 2 * IW),
        ] {
            let mut cc = controller(now);
            let mut resume = ready(cwnd, now, &mut cc);
            assert!(!resume.on_window_blocked(now, 10, IW, 0, &rtt(sample), &mut cc));
            assert_eq!(resume.phase_name(), "off");
            assert_eq!(cc.window(), IW);
        }
    }

    #[test]
    fn congestion_before_the_jump_stops_and_after_it_retreats_to_half_the_pipe() {
        let now = Instant::now();
        let mut cc = controller(now);
        let mut resume =
            CarefulResume::new(Grant::Resume(saved(100_000, now)), u64::MAX, false, now);
        resume.on_congestion(&mut cc);
        assert_eq!(resume.phase_name(), "off");
        // The measurement it never used goes back
        assert_eq!(
            resume.observed(IW, SECOND),
            Leaves::Keep(saved(100_000, now))
        );

        let mut resume = ready(100_000, now, &mut cc);
        assert!(resume.on_window_blocked(now, 10, 20_000, 0, &rtt(SECOND), &mut cc));
        resume.on_data_sent(12, 30_000, &mut cc);
        resume.on_congestion(&mut cc);
        assert_eq!(resume.phase_name(), "retreat");
        assert_eq!(cc.window(), 10_000);
        assert!(resume.holds_window());
        // The measurement is deleted (§3.5), and the remote's mark released: no warm-up was spent
        assert_eq!(resume.observed(IW, SECOND), Leaves::Release);
        // The last packet sent while Unvalidated is acknowledged: ssthresh from the PipeSize
        resume.on_ack(now, 4_000, Some(12), Some(12), 0, &rtt(SECOND), &mut cc);
        assert_eq!(resume.phase_name(), "off");
        assert_eq!(cc.metrics().ssthresh, Some(12_000));
    }

    #[test]
    fn the_unvalidated_phase_ends_after_a_round_trip_at_the_pipe_or_the_flight() {
        let now = Instant::now();
        // Little in flight at the exit: the window becomes the PipeSize and Careful Resume ends
        let mut cc = controller(now);
        let mut resume = ready(100_000, now, &mut cc);
        assert!(resume.on_window_blocked(now, 10, 14_000, 0, &rtt(SECOND), &mut cc));
        resume.on_data_sent(11, 16_000, &mut cc);
        resume.on_ack(now, 8_000, Some(10), Some(11), 8_000, &rtt(SECOND), &mut cc);
        assert_eq!(resume.phase_name(), "off");
        assert_eq!(cc.window(), 22_000);

        // A full flight: the window becomes the flight and Validating follows until the last
        // packet sent while Unvalidated is acknowledged
        let mut cc = controller(now);
        let mut resume = ready(100_000, now, &mut cc);
        assert!(resume.on_window_blocked(now, 10, 12_000, 0, &rtt(SECOND), &mut cc));
        resume.on_data_sent(40, 50_000, &mut cc);
        assert_eq!(resume.phase_name(), "validating");
        assert_eq!(cc.window(), 50_000);
        assert!(!resume.holds_window());
        resume.on_ack(
            now,
            1_000,
            Some(39),
            Some(40),
            49_000,
            &rtt(SECOND),
            &mut cc,
        );
        assert_eq!(resume.phase_name(), "validating");
        resume.on_ack(
            now,
            1_000,
            Some(40),
            Some(40),
            48_000,
            &rtt(SECOND),
            &mut cc,
        );
        assert_eq!(resume.phase_name(), "off");
    }

    #[test]
    fn a_round_trip_is_counted_by_packet_numbers_whatever_the_clock() {
        let now = Instant::now();
        let mut cc = controller(now);
        let mut resume = CarefulResume::new(Grant::Neither, u64::MAX, false, now);
        // Every acknowledgement at one instant, as on a path whose round trip is below the clock's
        // resolution: the round trip still runs to the acknowledgement of a packet past 40, and
        // with nothing in flight (no ack-eliciting packet outstanding) the largest acknowledged
        // marks the next
        resume.on_ack(
            now,
            1_200,
            Some(0),
            Some(40),
            0,
            &rtt(Duration::ZERO),
            &mut cc,
        );
        for acked in [10, 20, 30, 41] {
            resume.on_ack(
                now,
                12_000,
                Some(acked),
                None,
                0,
                &rtt(Duration::ZERO),
                &mut cc,
            );
        }
        assert_eq!(
            resume.observed(IW, SECOND),
            Leaves::Keep(Saved {
                cwnd: 48_000,
                rtt: SECOND,
                at: now,
            })
        );
    }

    #[test]
    fn a_connection_saves_what_it_delivered_a_round_trip_from_four_initial_windows() {
        let now = Instant::now();
        let mut cc = controller(now);
        let mut resume = CarefulResume::new(Grant::Neither, u64::MAX, false, now);
        // The first acknowledgement begins a round trip, ended by an acknowledgement past packet 10
        resume.on_ack(now, 1_000, Some(0), Some(10), 0, &rtt(SECOND), &mut cc);
        // 47,000 bytes in it: below four initial windows, not saved
        resume.on_ack(
            now + SECOND,
            47_000,
            Some(11),
            Some(20),
            0,
            &rtt(SECOND),
            &mut cc,
        );
        assert_eq!(resume.observed(IW, SECOND), Leaves::Nothing);
        // 60,000 in the next: saved, with the minimum RTT and when it was measured
        resume.on_ack(
            now + 2 * SECOND,
            60_000,
            Some(21),
            Some(30),
            0,
            &rtt(SECOND),
            &mut cc,
        );
        assert_eq!(
            resume.observed(IW, Duration::from_millis(900)),
            Leaves::Keep(Saved {
                cwnd: 60_000,
                rtt: Duration::from_millis(900),
                at: now + 2 * SECOND,
            })
        );
    }
}
