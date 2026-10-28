//! A connection's forward erasure correction (`crate::erasure`, `TransportConfig::fec_window`):
//! which of its packets are sources, when a repair goes, and the decoder its peer's repairs feed.
//!
//! A connection codes only while its path loses. It measures the loss of its ack-eliciting packets
//! by RFC 5348 §5.4's average loss interval and sends repairs at three times the rate at which the
//! path loses them, `3p/(1 − p)` of the sources sent (`REPAIRS_PER_LOSS`, measured): the repairs a
//! span receives then outnumber the sources it loses, which lets the code rebuild losses as they
//! come rather than wait for repairs to catch up with them. A repair combines the sources sent
//! since the last its peer acknowledged and no earlier than the connection's loss delay (RFC 9002
//! §6.1.2): an older source's loss is detected, and its retransmission is under way, before a
//! repair could rebuild it. And it combines at most the sources over which three repairs go at
//! that rate, so each source is in about three repairs' spans: a repair's work is its span times
//! a source's bytes, and the span stays bounded however fast the connection sends.
//!
//! And it codes only while it sends often enough for a repair to be due within each loss delay: a
//! repair that comes after a loss's retransmission rebuilds nothing the retransmission does not,
//! so a connection whose packets of stream data are too few for that (an asker's requests, a few
//! a second) leaves its losses to retransmission and keeps nothing for repairs.

use crate::erasure::{Decoder, Encoder, Span};
use crate::frame::{FEC_SOURCE_MAX, FecRepair};
use crate::{Duration, Instant};

/// RFC 5348 §5.4's weights of the last eight loss intervals, the most recent first, in tenths.
const WEIGHTS: [u64; 8] = [10, 10, 10, 10, 8, 6, 4, 2];

/// Parts per million.
const PPM: u64 = 1_000_000;

/// The repairs sent for each source the path is expected to lose, measured: on hyper-sim's network
/// at 80 ms one way, two followers of a leader sending 110 appends of 7.5 KB a second (focal's
/// replication load, `tests/replication.rs`), two repairs for each loss left 30, 21, 14 and 12%
/// of the losses to retransmission at 1, 3, 5 and 10% loss each way, three left 14, 7, 4.5 and
/// 3.8%, for repairs of 11 to 13% of the sources at 3% loss where two cost 7 to 10%; the commit's
/// median was the loss-free path's either way, its 99th percentile 282 to 295 ms at 3% where two
/// gave 321 to 332 (loss-free: 161 ms). Every unrebuilt loss stalls a stream a round trip.
const REPAIRS_PER_LOSS: u64 = 3;

/// The repairs each source is in the span of, at the rate repairs go (the module's
/// documentation): measured on the same runs, six spans a source in place of three rebuilt no
/// more and doubled each repair's work, and a window of 128 sources in place of 64 the same.
const SPANS_PER_SOURCE: u64 = 3;

/// The bytes a packet's other frames leave for a source to stay small enough for a repair of it to
/// fit a packet of its own: the source frame, a repair frame's header, and the three bytes by
/// which a packet number's encoding can grow between two packets (RFC 9000 §17.1).
pub(super) const SOURCE_RESERVE: usize = FEC_SOURCE_MAX + REPAIR_RESERVE;

/// The bytes a source's frames, its source frame among them, leave of a packet: a repair of it
/// then fits a packet of its own.
pub(super) const REPAIR_RESERVE: usize = FecRepair::OVERHEAD_MAX + 3;

/// The recent loss rate of a connection's ack-eliciting packets: RFC 5348 §5.4's average loss
/// interval over the last eight, the open interval counted where it raises the mean. TFRC counts
/// one loss event a round trip; the code repairs packets, so each lost packet ends an interval.
/// The connection's start bounds its first interval, as RFC 5348 §6.3.1 synthesizes one, so the
/// first loss after a thousand packets delivered is a rate of one in a thousand and one.
#[derive(Default)]
pub(super) struct LossRate {
    /// The packets of each of the last eight closed intervals, the most recent first: a loss and
    /// the packets delivered before the next.
    closed: [u64; 8],
    /// The closed intervals held, at most eight.
    count: usize,
    /// The packets delivered since the last loss, or since the connection began.
    open: u64,
}

impl LossRate {
    pub(super) fn delivered(&mut self) {
        self.open = self.open.saturating_add(1);
    }

    pub(super) fn lost(&mut self) {
        self.closed.rotate_right(1);
        if let Some(first) = self.closed.first_mut() {
            *first = self.open.saturating_add(1);
        }
        self.count = self.count.saturating_add(1).min(WEIGHTS.len());
        self.open = 0;
    }

    /// The packets lost per million: `1 / I_mean`, zero before any loss.
    pub(super) fn ppm(&self) -> u64 {
        let held = self.count;
        if held == 0 {
            return 0;
        }
        // I_0 is the open interval, the last loss and the packets delivered since
        let open = self.open.saturating_add(1);
        let mut with_open = open.saturating_mul(WEIGHTS.first().copied().unwrap_or(0));
        let mut closed = 0u64;
        let mut total_weight = 0u64;
        for (index, (interval, weight)) in self.closed.iter().zip(WEIGHTS).take(held).enumerate() {
            closed = closed.saturating_add(interval.saturating_mul(weight));
            total_weight = total_weight.saturating_add(weight);
            if let Some(next) = WEIGHTS.get(index.saturating_add(1))
                && index.saturating_add(1) < held
            {
                with_open = with_open.saturating_add(interval.saturating_mul(*next));
            }
        }
        let weighted = with_open.max(closed);
        total_weight
            .saturating_mul(PPM)
            .checked_div(weighted)
            .unwrap_or(0)
            .min(PPM)
    }
}

/// The sending side: the encoder, the loss rate it codes against and the repair owed.
pub(super) struct Sending {
    encoder: Encoder,
    losses: LossRate,
    /// Repairs owed, in millionths of one: each source sent adds the repair rate.
    credit: u64,
    /// The packets of stream data sent in the current loss delay and the last.
    meter: Meter,
    /// The most sources a repair combines: the smaller of the two endpoints' windows.
    limit: usize,
    /// The repair the sources sent called for, made as the last of them went, to go in the next
    /// packet: what it covers, its bytes in `scratch`.
    ready: Option<Span>,
    /// A repair's bytes, kept for the next.
    scratch: Vec<u8>,
}

impl Sending {
    pub(super) fn new(limit: usize) -> Self {
        Self {
            encoder: Encoder::new(limit),
            losses: LossRate::default(),
            credit: 0,
            meter: Meter::default(),
            limit: limit.max(1),
            ready: None,
            scratch: Vec::new(),
        }
    }

    /// The repairs per million sources: twice the losses, `2p / (1 − p)`; zero while the path
    /// loses nothing.
    fn repair_ppm(&self) -> u64 {
        let lost = self.losses.ppm();
        if lost == 0 {
            return 0;
        }
        let kept = PPM.saturating_sub(lost).max(1);
        REPAIRS_PER_LOSS
            .saturating_mul(lost)
            .saturating_mul(PPM)
            .checked_div(kept)
            .unwrap_or(PPM)
            .min(PPM)
    }

    /// The sources a repair combines at the repair rate: those over which `SPANS_PER_SOURCE`
    /// repairs go, so that each source is in that many repairs' spans; `None` while the path
    /// loses nothing, or more than the window holds.
    fn span(&self) -> Option<usize> {
        let span = SPANS_PER_SOURCE
            .saturating_mul(PPM)
            .div_ceil(self.repair_ppm().max(1));
        let span = usize::try_from(span).ok()?;
        (self.repair_ppm() > 0 && span <= self.limit).then_some(span.max(1))
    }

    /// Whether the connection codes now: its path loses, and often enough that a span covering
    /// each source as many times as `SPANS_PER_SOURCE` fits the window; and it sends packets of
    /// stream data often enough that a repair is due within each loss delay. A rarer loss, or a
    /// sparser connection, is left to retransmission: a code that sparse would rebuild few
    /// losses in time, and each source it marked would still give up its packet's reserve for a
    /// repair.
    pub(super) fn active(&self) -> bool {
        self.span().is_some() && self.meter.per_period().saturating_mul(self.repair_ppm()) >= PPM
    }

    /// A packet of stream data was sent at `now`, coded or not: its rate decides whether the
    /// connection codes.
    pub(super) fn stream_packet(&mut self, now: Instant, loss_delay: Duration) {
        self.meter.sent(now, loss_delay);
    }

    /// Whether a repair is due.
    pub(super) fn due(&self) -> bool {
        self.ready.is_some()
    }

    /// The number the next source takes.
    pub(super) fn next_id(&self) -> u64 {
        self.encoder.next_id()
    }

    /// A packet the connection measures, numbered `packet`, was acknowledged; a source among
    /// them leaves the repairs to come.
    pub(super) fn delivered(&mut self, packet: u64) {
        self.losses.delivered();
        self.encoder.acked(packet);
    }

    /// A packet the connection measures was declared lost.
    pub(super) fn lost(&mut self) {
        self.losses.lost();
    }

    /// The source in packet `packet`, its frames `payload`, was sent at `now`: kept for the
    /// repairs after it, and the repair it calls for made, over the sources since the oldest
    /// unacknowledged one sent within `loss_delay`, at most a span's.
    pub(super) fn sent(&mut self, payload: &[u8], packet: u64, now: Instant, loss_delay: Duration) {
        if !self.encoder.sent(payload, packet, now) {
            return;
        }
        self.credit = self.credit.saturating_add(self.repair_ppm());
        if self.credit < PPM || self.ready.is_some() {
            return;
        }
        self.credit = self.credit.saturating_sub(PPM);
        if let Some(before) = now.checked_sub(loss_delay) {
            self.encoder.prune(before);
        }
        let span = self.span().unwrap_or(self.limit);
        self.ready = self.encoder.repair(span, &mut self.scratch);
    }

    /// The repair due, if it fits a packet's `room`; it is no longer due either way. One that does
    /// not fit was made of sources cut for a larger packet than this one (the path's MTU fell):
    /// what they hold no longer has a repair that fits, and they are forgotten.
    pub(super) fn repair(&mut self, room: usize) -> Option<(Span, &[u8])> {
        let made = self.ready.take()?;
        if self.scratch.len().saturating_add(FecRepair::OVERHEAD_MAX) > room {
            self.encoder.forget_all();
            return None;
        }
        Some((made, &self.scratch))
    }

    /// Forgets the sources kept and the repair made of them: the connection codes no longer, or
    /// the memory is wanted. The loss rate is kept, the path's.
    pub(super) fn forget(&mut self) {
        self.encoder.forget_all();
        self.ready = None;
        self.credit = 0;
        self.scratch = Vec::new();
    }

    /// Whether sources are kept.
    pub(super) fn holds(&self) -> bool {
        self.encoder.held() > 0 || self.ready.is_some()
    }

    /// The bytes held for the sources kept.
    pub(super) fn held_bytes(&self) -> usize {
        self.encoder
            .held_bytes()
            .saturating_add(self.scratch.capacity())
    }
}

/// The packets of stream data a connection sent in its current loss delay and in the one before.
#[derive(Default)]
struct Meter {
    /// When the current period began.
    start: Option<Instant>,
    count: u64,
    /// The last whole period's count; none when it ended a period or more before the current one.
    last: u64,
}

impl Meter {
    fn sent(&mut self, now: Instant, period: Duration) {
        let elapsed = self.start.map(|start| now.saturating_duration_since(start));
        if elapsed.is_none_or(|elapsed| elapsed >= period) {
            self.last = match elapsed {
                Some(elapsed) if elapsed < period.saturating_mul(2) => self.count,
                _ => 0,
            };
            self.start = Some(now);
            self.count = 0;
        }
        self.count = self.count.saturating_add(1);
    }

    /// The packets a loss delay: the current period's or the last's, whichever is more.
    fn per_period(&self) -> u64 {
        self.count.max(self.last)
    }
}

/// A connection's coding: each side, where both endpoints state a window.
#[derive(Default)]
pub(super) struct Fec {
    pub(super) sending: Option<Sending>,
    pub(super) receiving: Option<Decoder>,
    /// The sources the last packet's frames rebuilt, to be read as packets' frames are.
    pub(super) rebuilt: Vec<Vec<u8>>,
}

impl Fec {
    /// The sending side codes now, and the packet being built is to be a source if it carries
    /// stream data.
    pub(super) fn sources_active(&self) -> bool {
        self.sending.as_ref().is_some_and(Sending::active)
    }

    /// A repair waits to be sent.
    pub(super) fn repair_due(&self) -> bool {
        self.sending.as_ref().is_some_and(Sending::due)
    }

    /// Forgets what both sides hold.
    pub(super) fn forget(&mut self) {
        if let Some(sending) = &mut self.sending {
            sending.forget();
        }
        if let Some(receiving) = &mut self.receiving {
            receiving.forget_all();
        }
        self.rebuilt = Vec::new();
    }

    /// The bytes held by both sides.
    pub(super) fn held_bytes(&self) -> usize {
        let sending = self.sending.as_ref().map_or(0, Sending::held_bytes);
        let receiving = self.receiving.as_ref().map_or(0, Decoder::held_bytes);
        let rebuilt = self.rebuilt.iter().fold(0usize, |sum, payload| {
            sum.saturating_add(payload.capacity())
        });
        sending.saturating_add(receiving).saturating_add(rebuilt)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn no_loss_is_no_rate_and_one_loss_in_a_thousand_and_one_is_its_rate() {
        let mut losses = LossRate::default();
        for _ in 0..1_000 {
            losses.delivered();
        }
        assert_eq!(losses.ppm(), 0);
        losses.lost();
        assert_eq!(losses.ppm(), PPM / 1_001);
        // The open interval raises the mean only once it is longer than the closed one
        for _ in 0..500 {
            losses.delivered();
        }
        assert_eq!(losses.ppm(), PPM / 1_001);
        for _ in 0..1_500 {
            losses.delivered();
        }
        assert_eq!(losses.ppm(), PPM / 2_001);
    }

    #[test]
    fn three_percent_loss_measures_as_three_percent() {
        // A loss every 33 packets, its deliveries between
        let mut losses = LossRate::default();
        for _ in 0..40 {
            for _ in 0..32 {
                losses.delivered();
            }
            losses.lost();
        }
        assert_eq!(losses.ppm(), PPM / 33);
        let sending = Sending {
            losses,
            ..Sending::new(64)
        };
        // Three times the losses, of what is kept: 3 · (1/33) / (32/33), three thirty-seconds,
        // in whole millionths of the rate measured
        assert_eq!(sending.repair_ppm(), 3 * 30_303 * PPM / (PPM - 30_303));
        assert_eq!(sending.repair_ppm(), 93_749);
        // Each source in three repairs' spans: 3 / 0.093749, 33 sources, within a window of 64
        assert_eq!(sending.span(), Some(33));
        // and coded only while a repair is due within each loss delay: one at 0.093749 a packet
        // takes 11 packets within it
        let mut sending = sending;
        let (now, delay) = (Instant::now(), Duration::from_millis(100));
        for packet in 0..10u32 {
            sending.stream_packet(now + Duration::from_millis(u64::from(packet)), delay);
        }
        assert!(
            !sending.active(),
            "10 packets a loss delay owe no repair within it"
        );
        sending.stream_packet(now + Duration::from_millis(10), delay);
        assert!(sending.active());
        // A period past, the count stays the last period's; two past, it is forgotten
        sending.stream_packet(now + Duration::from_millis(150), delay);
        assert!(sending.active());
        sending.stream_packet(now + Duration::from_millis(400), delay);
        assert!(!sending.active());
    }
}
