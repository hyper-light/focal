use std::fmt::{Debug, Display, Formatter};

use super::min_max::MinMax;
use crate::{Duration, Instant};

#[derive(Clone, Debug, Default)]
pub(crate) struct BandwidthEstimation {
    total_acked: u64,
    prev_total_acked: u64,
    acked_time: Option<Instant>,
    prev_acked_time: Option<Instant>,
    total_sent: u64,
    prev_total_sent: u64,
    sent_time: Option<Instant>,
    prev_sent_time: Option<Instant>,
    max_filter: MinMax,
    acked_at_last_window: u64,
}

impl BandwidthEstimation {
    pub(crate) fn on_sent(&mut self, now: Instant, bytes: u64) {
        // Byte counts: saturating holds them at a total no connection reaches
        self.prev_total_sent = self.total_sent;
        self.total_sent = self.total_sent.saturating_add(bytes);
        self.prev_sent_time = self.sent_time;
        self.sent_time = Some(now);
    }

    pub(crate) fn on_ack(
        &mut self,
        now: Instant,
        _sent: Instant,
        bytes: u64,
        round: u64,
        app_limited: bool,
    ) {
        self.prev_total_acked = self.total_acked;
        self.total_acked = self.total_acked.saturating_add(bytes);
        self.prev_acked_time = self.acked_time;
        self.acked_time = Some(now);

        let prev_sent_time = match self.prev_sent_time {
            Some(prev_sent_time) => prev_sent_time,
            None => return,
        };

        let send_rate = match self.sent_time {
            // Each previous total is the total before the latest bytes, so no difference is
            // negative
            Some(sent_time) if sent_time > prev_sent_time => Self::bw_from_delta(
                self.total_sent.saturating_sub(self.prev_total_sent),
                sent_time.saturating_duration_since(prev_sent_time),
            )
            .unwrap_or(0),
            _ => u64::MAX, // will take the min of send and ack, so this is just a skip
        };

        let ack_rate = match self.prev_acked_time {
            Some(prev_acked_time) => Self::bw_from_delta(
                self.total_acked.saturating_sub(self.prev_total_acked),
                now.saturating_duration_since(prev_acked_time),
            )
            .unwrap_or(0),
            None => 0,
        };

        let bandwidth = send_rate.min(ack_rate);
        if !app_limited && self.max_filter.get() < bandwidth {
            self.max_filter.update_max(round, bandwidth);
        }
    }

    pub(crate) fn bytes_acked_this_window(&self) -> u64 {
        self.total_acked.saturating_sub(self.acked_at_last_window)
    }

    pub(crate) fn end_acks(&mut self, _current_round: u64, _app_limited: bool) {
        self.acked_at_last_window = self.total_acked;
    }

    pub(crate) fn get_estimate(&self) -> u64 {
        self.max_filter.get()
    }

    /// Bytes per second; computed in u128, where upstream's u64 product overflowed past 18 GB
    /// and its divisor was truncated past 584 years, and saturating into a u64
    pub(crate) fn bw_from_delta(bytes: u64, delta: Duration) -> Option<u64> {
        /// Nanoseconds in a second
        const NANOS_PER_SEC: u128 = 1_000_000_000;
        let bytes_per_second = u128::from(bytes)
            .saturating_mul(NANOS_PER_SEC)
            .checked_div(delta.as_nanos())?;
        Some(u64::try_from(bytes_per_second).unwrap_or(u64::MAX))
    }
}

impl Display for BandwidthEstimation {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{:.3} MB/s",
            self.get_estimate() as f32 / (1024 * 1024) as f32
        )
    }
}
