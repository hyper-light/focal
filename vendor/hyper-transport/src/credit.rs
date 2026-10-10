//! Credits (node.md §3.3; mantle note 32 T16, T17, T33).
//!
//! QUIC's flow control is limit-based and quinn keeps one window per connection, so what a node
//! buffers for a peer is bounded by that connection's receive window, and the classes share it.
//! Three rules are kept here, each derived:
//!
//! - **The class reserve** (slates `connection.rs`, `class_credit_reserve`): a class leaves unspent
//!   one packet's stream bytes of the peer's connection credit for every class above it, so the
//!   first packet of a more urgent exchange finds credit waiting however much the classes below
//!   have queued. slates measured a control ping waiting 68 ms of a 40 ms path for credit a bulk
//!   stream had taken, before its reserve. The receiver advertises the largest reserve on top of
//!   its window, so the reserve is headroom, never a cut.
//! - **The window** starts at RFC 9002's initial congestion window, so the receiver never holds a
//!   new sender below what its controller may already send, and doubles whenever the application
//!   consumed a whole window in less than two round trips, the sign that the window and not the path
//!   limited the transfer (Chromium QUIC's `QuicFlowController::MaybeIncreaseMaxWindowSize`, slates
//!   `flow.rs`). Each growth is reserved from the node's budget first, so the window is
//!   `min(BDP, share)`: it follows the path's bandwidth-delay product while the budget funds it.
//! - **The stream window's ceiling** is quinn's 1,024-span assembler limit made explicit (patch Q5):
//!   a window `W` of frames of at least `d` bytes is left with at most `W / (2d)` spans when every
//!   other frame is lost, so a window of at most `2 · 1,024 · d` cannot close the connection with
//!   "too many gaps in stream buffer" (node.md §3.3; focal reached the limit with a 10 MiB window).

use std::time::{Duration, Instant};

use hyper_quic::MAX_STREAM_CHUNKS;

/// The smallest datagram a QUIC path carries (RFC 9000 §14: "a UDP payload size of at least
/// 1200 bytes").
pub const MIN_DATAGRAM: u64 = 1_200;
/// The most a 1-RTT packet's header takes: one flags byte, a destination connection ID of at most
/// 20 bytes (RFC 9000 §17.2) and a packet number of at most 4 (RFC 9000 §17.1).
const SHORT_HEADER_MAX: u64 = 1 + 20 + 4;
/// The AEAD tag every packet carries: 16 bytes for each of QUIC's AEADs (RFC 9001 §5.3).
const AEAD_TAG: u64 = 16;
/// The most a STREAM frame's header takes: its type byte and three variable-length integers of at
/// most 8 bytes each, the stream ID, the offset and the length (RFC 9000 §19.8, §16).
const STREAM_FRAME_HEADER_MAX: u64 = 1 + 8 + 8 + 8;
/// The least stream data one packet of the smallest datagram carries, `d` above:
/// 1,200 - 25 - 16 - 25 = 1,134 bytes.
pub const STREAM_BYTES_PER_PACKET: u64 =
    MIN_DATAGRAM - SHORT_HEADER_MAX - AEAD_TAG - STREAM_FRAME_HEADER_MAX;
/// The two round trips within which a consumed window says the window was the limit (Chromium
/// QUIC's auto-tuning trigger, slates `flow.rs` `AUTOTUNE_RTT_MULTIPLE`).
const AUTOTUNE_ROUND_TRIPS: u32 = 2;
/// RFC 9002's `kGranularity` (Appendix A.2: "Timer granularity. This is a system-dependent
/// value"; 1 ms, as §6.1.2 recommends): the granularity a window is
/// tuned under until the owner reports the one it measured (`Endpoint::set_granularity`, the mean
/// lateness of its timed waits, `hyper_timing::Lateness`; `docs/timing.md` §2.4). Below it a round
/// trip is not resolved: a loopback path's round trip of tens of microseconds would otherwise
/// leave no time in which a consumed window counts as quick, and the window would never grow.
pub const K_GRANULARITY: Duration = Duration::from_millis(1);
/// The window doubles at a growth (Chromium QUIC's auto-tuning step, slates `flow.rs`).
const AUTOTUNE_GROWTH: u64 = 2;
/// RFC 9002 §7.2's floor on the initial window, `max(14720, 2 * max_datagram_size)`'s constant.
const INITIAL_WINDOW_FLOOR: u64 = 14_720;
/// RFC 9002 §7.2's initial window is ten datagrams, `10 * max_datagram_size`.
const INITIAL_WINDOW_DATAGRAMS: u64 = 10;

/// The connection credit a class leaves unspent for the `above` classes more urgent than it: one
/// packet's stream bytes for each.
pub fn class_reserve(above: u8) -> u64 {
    u64::from(above).saturating_mul(STREAM_BYTES_PER_PACKET)
}

/// The largest stream window that quinn's assembler cannot be made to refuse: `2 · 1,024 · d`,
/// about 2.3 MB.
pub fn stream_window_ceiling() -> u64 {
    let spans = u64::try_from(MAX_STREAM_CHUNKS).unwrap_or(u64::MAX);
    spans
        .saturating_mul(2)
        .saturating_mul(STREAM_BYTES_PER_PACKET)
}

/// RFC 9002 §7.2's initial congestion window for datagrams of `max_datagram` bytes:
/// `min(10 * max_datagram, max(14720, 2 * max_datagram))`, 12,000 bytes at 1,200.
pub fn initial_window(max_datagram: u64) -> u64 {
    let ten = max_datagram.saturating_mul(INITIAL_WINDOW_DATAGRAMS);
    let two = max_datagram.saturating_mul(2);
    ten.min(INITIAL_WINDOW_FLOOR.max(two))
}

/// A connection's receive window, auto-tuned as the application consumes it.
#[derive(Clone, Copy, Debug)]
pub(crate) struct Window {
    window: u64,
    ceiling: u64,
    consumed: u64,
    /// The tuning epoch: when it began and what had been consumed then.
    epoch: Option<(Instant, u64)>,
}

impl Window {
    /// A window of `initial` bytes that may grow to `ceiling`.
    pub(crate) fn new(initial: u64, ceiling: u64) -> Self {
        Self {
            window: initial,
            ceiling: ceiling.max(initial),
            consumed: 0,
            epoch: None,
        }
    }
    /// The window now.
    pub(crate) fn window(&self) -> u64 {
        self.window
    }
    /// The application consumed `bytes` more.
    pub(crate) fn consumed(&mut self, bytes: u64) {
        self.consumed = self.consumed.saturating_add(bytes);
    }
    /// The window the next growth would give, if the application consumed a whole window within
    /// two round trips of `rtt`, a round trip under the owner's timer `granularity` counting as
    /// that, by `now`; `None` when the window stays. A new epoch begins once a whole window was
    /// consumed, whether or not the window grows.
    pub(crate) fn tune(
        &mut self,
        now: Instant,
        rtt: Duration,
        granularity: Duration,
    ) -> Option<u64> {
        let (began, consumed_then) = *self.epoch.get_or_insert((now, self.consumed));
        if self.consumed.saturating_sub(consumed_then) < self.window {
            return None;
        }
        self.epoch = Some((now, self.consumed));
        let rtt = rtt.max(granularity);
        let quick =
            now.saturating_duration_since(began) <= rtt.saturating_mul(AUTOTUNE_ROUND_TRIPS);
        let grown = self
            .window
            .saturating_mul(AUTOTUNE_GROWTH)
            .min(self.ceiling);
        (quick && grown > self.window).then_some(grown)
    }
    /// The window grew to `window`, its growth funded.
    pub(crate) fn grew(&mut self, window: u64) {
        self.window = window.min(self.ceiling);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A path whose round trip is under the owner's timer granularity is tuned under that
    /// granularity: on Windows' 15.6 ms timer a window consumed in 20 ms, past two round trips of
    /// a 400 µs path and past RFC 9002's 1 ms, is still within two of the owner's granularity, the
    /// finest time in which it sees the window consumed.
    #[test]
    fn a_round_trip_under_the_granularity_counts_as_the_granularity() {
        let start = hyper_sim::Anchor::new().instant(0).unwrap();
        let rtt = Duration::from_micros(400);
        let consumed_in = Duration::from_millis(20);
        let mut coarse = Window::new(1_000, 4_000);
        assert_eq!(coarse.tune(start, rtt, K_GRANULARITY), None);
        coarse.consumed(1_000);
        assert_eq!(coarse.tune(start + consumed_in, rtt, K_GRANULARITY), None);
        let mut measured = Window::new(1_000, 4_000);
        let windows = Duration::from_micros(15_625);
        assert_eq!(measured.tune(start, rtt, windows), None);
        measured.consumed(1_000);
        assert_eq!(
            measured.tune(start + consumed_in, rtt, windows),
            Some(2_000)
        );
    }

    #[test]
    fn the_derived_numbers() {
        assert_eq!(STREAM_BYTES_PER_PACKET, 1_134);
        assert_eq!(class_reserve(0), 0);
        assert_eq!(class_reserve(3), 3 * 1_134);
        assert_eq!(stream_window_ceiling(), 2 * 1_024 * 1_134);
        assert_eq!(initial_window(1_200), 12_000);
        assert_eq!(initial_window(1_472), 14_720);
        assert_eq!(initial_window(9_000), 18_000);
    }

    /// slates `flow.rs`'s autotune law: a window consumed within two round trips doubles, up to
    /// the ceiling; one consumed slower starts a new epoch at the same window.
    #[test]
    fn a_window_consumed_quickly_doubles_up_to_its_ceiling() {
        let start = hyper_sim::Anchor::new().instant(0).unwrap();
        let rtt = Duration::from_millis(10);
        let mut window = Window::new(1_000, 3_000);
        assert_eq!(window.tune(start, rtt, K_GRANULARITY), None);
        window.consumed(999);
        assert_eq!(
            window.tune(start + Duration::from_millis(5), rtt, K_GRANULARITY),
            None
        );
        window.consumed(1);
        let grown = window
            .tune(start + Duration::from_millis(15), rtt, K_GRANULARITY)
            .unwrap();
        assert_eq!(grown, 2_000);
        window.grew(grown);
        // Slowly: a whole window over three round trips is no reason to grow.
        window.consumed(2_000);
        assert_eq!(
            window.tune(start + Duration::from_millis(45), rtt, K_GRANULARITY),
            None
        );
        assert_eq!(window.window(), 2_000);
        // Quickly again: capped at the ceiling, and no growth past it.
        window.consumed(2_000);
        assert_eq!(
            window.tune(start + Duration::from_millis(50), rtt, K_GRANULARITY),
            Some(3_000)
        );
        window.grew(3_000);
        window.consumed(3_000);
        assert_eq!(
            window.tune(start + Duration::from_millis(51), rtt, K_GRANULARITY),
            None
        );
    }
}
