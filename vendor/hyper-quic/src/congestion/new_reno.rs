use std::any::Any;

use super::{Controller, initial_window};
use crate::connection::RttEstimator;
use crate::{Instant, float};

/// A simple, standard congestion controller
#[derive(Debug, Clone)]
pub struct NewReno {
    config: NewRenoConfig,
    current_mtu: u64,
    /// Maximum number of bytes in flight that may be sent.
    window: u64,
    /// Slow start threshold in bytes. When the congestion window is below ssthresh, the mode is
    /// slow start and the window grows by the number of bytes acknowledged.
    ssthresh: u64,
    /// The time when QUIC first detects a loss, causing it to enter recovery. When a packet sent
    /// after this time is acknowledged, QUIC exits recovery.
    recovery_start_time: Instant,
    /// Bytes which had been acked by the peer since leaving slow start
    bytes_acked: u64,
}

impl NewReno {
    /// Construct a state using the given `config` and current time `now`
    pub fn new(config: NewRenoConfig, now: Instant, current_mtu: u16) -> Self {
        Self {
            window: config
                .initial_window
                .unwrap_or_else(|| initial_window(current_mtu.into())),
            ssthresh: u64::MAX,
            recovery_start_time: now,
            current_mtu: current_mtu as u64,
            config,
            bytes_acked: 0,
        }
    }

    fn minimum_window(&self) -> u64 {
        2u64.saturating_mul(self.current_mtu)
    }
}

impl Controller for NewReno {
    fn on_ack(
        &mut self,
        _now: Instant,
        sent: Instant,
        bytes: u64,
        app_limited: bool,
        _rtt: &RttEstimator,
    ) {
        if app_limited || sent <= self.recovery_start_time {
            return;
        }

        if self.window < self.ssthresh {
            // Slow start
            // Windows and byte counts saturate at a size no connection reaches
            self.window = self.window.saturating_add(bytes);

            if self.window >= self.ssthresh {
                // Exiting slow start
                // Initialize `bytes_acked` for congestion avoidance. The idea
                // here is that any bytes over `sshthresh` will already be counted
                // towards the congestion avoidance phase - independent of when
                // how close to `sshthresh` the `window` was when switching states,
                // and independent of datagram sizes.
                self.bytes_acked = self.window.saturating_sub(self.ssthresh);
            }
        } else {
            // Congestion avoidance
            // This implementation uses the method which does not require
            // floating point math, which also increases the window by 1 datagram
            // for every round trip.
            // This mechanism is called Appropriate Byte Counting in
            // https://tools.ietf.org/html/rfc3465
            self.bytes_acked = self.bytes_acked.saturating_add(bytes);

            if let Some(left) = self.bytes_acked.checked_sub(self.window) {
                self.bytes_acked = left;
                self.window = self.window.saturating_add(self.current_mtu);
            }
        }
    }

    fn on_congestion_event(
        &mut self,
        now: Instant,
        sent: Instant,
        is_persistent_congestion: bool,
        _lost_bytes: u64,
    ) {
        if sent <= self.recovery_start_time {
            return;
        }

        self.recovery_start_time = now;
        self.window = float::saturating_u64(f64::from(
            self.window as f32 * self.config.loss_reduction_factor,
        ));
        self.window = self.window.max(self.minimum_window());
        self.ssthresh = self.window;

        if is_persistent_congestion {
            self.window = self.minimum_window();
        }
    }

    /// RFC 9002 §7.2: "If the maximum datagram size changes during the connection, the initial
    /// congestion window SHOULD be recalculated with the new size." A window still at the initial
    /// one, with no congestion met (the slow-start threshold unset), becomes the new initial window;
    /// one slow start or a congestion response has moved is the controller's own.
    fn on_mtu_update(&mut self, new_mtu: u16) {
        let before = self.initial_window();
        self.current_mtu = new_mtu as u64;
        if self.window == before && self.ssthresh == u64::MAX {
            self.window = self.initial_window();
        }
        self.window = self.window.max(self.minimum_window());
    }

    fn window(&self) -> u64 {
        self.window
    }

    fn set_window(&mut self, window: u64) -> bool {
        self.window = window.max(self.minimum_window());
        true
    }

    fn set_ssthresh(&mut self, ssthresh: u64) {
        self.ssthresh = ssthresh.max(self.minimum_window());
    }

    fn metrics(&self) -> super::ControllerMetrics {
        super::ControllerMetrics {
            congestion_window: self.window(),
            ssthresh: Some(self.ssthresh),
            pacing_rate: None,
        }
    }

    fn clone_box(&self) -> Box<dyn Controller> {
        Box::new(self.clone())
    }

    fn initial_window(&self) -> u64 {
        self.config
            .initial_window
            .unwrap_or_else(|| initial_window(self.current_mtu))
    }

    fn into_any(self: Box<Self>) -> Box<dyn Any> {
        self
    }
}

/// Configuration for the `NewReno` congestion controller
#[derive(Debug, Clone)]
pub struct NewRenoConfig {
    /// `None`: RFC 9002 §7.2's, by the path's datagram size
    initial_window: Option<u64>,
    loss_reduction_factor: f32,
}

impl NewRenoConfig {
    /// Limit on the amount of outstanding data in bytes before any is acknowledged
    ///
    /// By default RFC 9002 §7.2's, `min(10 * max_datagram_size, max(2 * max_datagram_size, 14720))`,
    /// recalculated as the path's datagram size changes; a value set here is fixed.
    pub fn initial_window(&mut self, value: u64) -> &mut Self {
        self.initial_window = Some(value);
        self
    }

    /// Reduction in congestion window when a new loss event is detected.
    pub fn loss_reduction_factor(&mut self, value: f32) -> &mut Self {
        self.loss_reduction_factor = value;
        self
    }
}

impl Default for NewRenoConfig {
    fn default() -> Self {
        Self {
            initial_window: None,
            loss_reduction_factor: 0.5,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::super::BASE_DATAGRAM_SIZE;
    use super::*;

    #[test]
    fn the_initial_window_follows_the_datagram_size_until_the_window_moves() {
        let now = Instant::now();
        let mut reno = NewReno::new(NewRenoConfig::default(), now, BASE_DATAGRAM_SIZE as u16);
        // RFC 9002 §7.2 at 1,200 bytes: ten datagrams
        assert_eq!(reno.window(), 12_000);
        // Path discovery finds 1,452 bytes: ten of those, 14,520
        reno.on_mtu_update(1_452);
        assert_eq!(reno.window(), 14_520);
        assert_eq!(reno.initial_window(), 14_520);
        // Jumbo datagrams: limited to the larger of 14,720 and two datagrams
        reno.on_mtu_update(9_000);
        assert_eq!(reno.window(), 18_000);
        // Once slow start moves it, the window is the controller's own
        let rtt = RttEstimator::new(Duration::from_millis(100));
        let later = now + Duration::from_millis(10);
        reno.on_ack(later, later, 1_000, false, &rtt);
        assert_eq!(reno.window(), 19_000);
        reno.on_mtu_update(1_452);
        assert_eq!(reno.window(), 19_000);
        // So after congestion, even at the initial window's size
        let mut reno = NewReno::new(NewRenoConfig::default(), now, BASE_DATAGRAM_SIZE as u16);
        reno.on_congestion_event(later, later, false, 1_200);
        reno.window = 12_000;
        reno.on_mtu_update(1_452);
        assert_eq!(reno.window(), 12_000);
        // A window set in the configuration is fixed
        let mut config = NewRenoConfig::default();
        config.initial_window(20_000);
        let mut reno = NewReno::new(config, now, BASE_DATAGRAM_SIZE as u16);
        reno.on_mtu_update(1_452);
        assert_eq!(reno.window(), 20_000);
    }
}
