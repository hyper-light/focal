//! Logic for controlling the rate at which data is sent

use crate::connection::RttEstimator;
use crate::{Duration, Instant};
use std::any::Any;

mod bbr;
mod copa;
mod cubic;
mod new_reno;

pub use bbr::{Bbr, BbrConfig};
pub use copa::{Copa, CopaConfig};
pub use cubic::{Cubic, CubicConfig};
pub use new_reno::{NewReno, NewRenoConfig};

/// Common interface for different congestion controllers
pub trait Controller: Send + Sync {
    /// One or more packets were just sent
    #[allow(unused_variables)]
    fn on_sent(&mut self, now: Instant, bytes: u64, last_packet_number: u64) {}

    /// Packet deliveries were confirmed
    ///
    /// `app_limited` indicates whether the connection was blocked on outgoing
    /// application data prior to receiving these acknowledgements.
    #[allow(unused_variables)]
    fn on_ack(
        &mut self,
        now: Instant,
        sent: Instant,
        bytes: u64,
        app_limited: bool,
        rtt: &RttEstimator,
    ) {
    }

    /// What the acknowledgement being processed says of the path's delivery rate, once its
    /// packets are acknowledged (`on_ack`) and before `on_end_acks`: given when it acknowledged an
    /// in-flight packet of the current path
    #[allow(unused_variables)]
    fn on_delivered(&mut self, now: Instant, sample: &RateSample) {}

    /// Packets are acked in batches, all with the same `now` argument. This indicates one of those batches has completed.
    #[allow(unused_variables)]
    fn on_end_acks(
        &mut self,
        now: Instant,
        in_flight: u64,
        app_limited: bool,
        largest_packet_num_acked: Option<u64>,
    ) {
    }

    /// Packets were deemed lost or marked congested
    ///
    /// `in_persistent_congestion` indicates whether all packets sent within the persistent
    /// congestion threshold period ending when the most recent packet in this batch was sent were
    /// lost.
    /// `lost_bytes` indicates how many bytes were lost. This value will be 0 for ECN triggers.
    fn on_congestion_event(
        &mut self,
        now: Instant,
        sent: Instant,
        is_persistent_congestion: bool,
        lost_bytes: u64,
    );

    /// The known MTU for the current network path has been updated
    fn on_mtu_update(&mut self, new_mtu: u16);

    /// Number of ack-eliciting bytes that may be in flight
    fn window(&self) -> u64;

    /// Sets the window to `window` bytes, not below the controller's minimum, for Careful Resume
    /// (RFC 9959 §3.3 to §3.5); returns whether the controller supports it
    ///
    /// A controller whose window is not a byte count it may be told (BBR's and Copa's are derived
    /// from their models) returns `false`, and its connections start at the initial window.
    #[allow(unused_variables)]
    fn set_window(&mut self, window: u64) -> bool {
        false
    }

    /// Sets the slow-start threshold on leaving Careful Resume's Safe Retreat (RFC 9959 §3.5),
    /// not below the controller's minimum window
    #[allow(unused_variables)]
    fn set_ssthresh(&mut self, ssthresh: u64) {}

    /// The rate the path's pacer sends at, in bytes a second
    ///
    /// `None` keeps the connection's own rule, five quarters of the window a smoothed round trip
    /// (RFC 9002 §7.7). A law that states its pacing (Copa §2.1) states it here.
    fn pacing_rate(&self) -> Option<u64> {
        None
    }

    /// Retrieve implementation-specific metrics used to populate `qlog` traces when they are enabled
    fn metrics(&self) -> ControllerMetrics {
        ControllerMetrics {
            congestion_window: self.window(),
            ssthresh: None,
            pacing_rate: None,
        }
    }

    /// Duplicate the controller's state
    fn clone_box(&self) -> Box<dyn Controller>;

    /// Initial congestion window
    fn initial_window(&self) -> u64;

    /// Returns Self for use in down-casting to extract implementation details
    fn into_any(self: Box<Self>) -> Box<dyn Any>;
}

/// What an acknowledgement says of how fast its path delivers: a delivery-rate sample
/// (draft-cheng-iccrg-delivery-rate-estimation-02 §3.3, Linux's `tcp_rate.c`), taken from the
/// newest packet it acknowledged ([`Controller::on_delivered`])
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub struct RateSample {
    /// The bytes the path delivered between the newest packet's send, by what had been delivered
    /// then, and the acknowledgement
    pub delivered: u64,
    /// The time they took: the longer of the newest packet's flight's send phase and its
    /// acknowledgement phase (§3.3)
    pub interval: Duration,
    /// `delivered` over `interval`, bytes a second; `None` when the interval is shorter than the
    /// path's least round trip, which no flight is delivered faster than (§3.3)
    pub rate: Option<u64>,
    /// Whether the newest packet was sent in a phase the application limited (§3.4): its rate
    /// measures the application, a floor of the path's
    pub app_limited: bool,
    /// The newest packet's round trip as measured, the peer's ACK delay in it
    pub rtt: Duration,
}

/// Common congestion controller metrics
#[derive(Default)]
#[non_exhaustive]
pub struct ControllerMetrics {
    /// Congestion window (bytes)
    pub congestion_window: u64,
    /// Slow start threshold (bytes)
    pub ssthresh: Option<u64>,
    /// Pacing rate (bits/s)
    pub pacing_rate: Option<u64>,
}

/// The congestion controller a connection's paths run, with its configuration, carried by value
///
/// A closed set: each controller here is reviewed and measured, and a path builds its own from
/// this value, so no factory is shared between connections (docs/transport.md §3.1).
#[derive(Debug, Clone)]
pub enum Congestion {
    /// CUBIC (RFC 9438), quinn's default
    Cubic(CubicConfig),
    /// NewReno (RFC 9002 §7)
    NewReno(NewRenoConfig),
    /// BBR (version 1)
    Bbr(BbrConfig),
    /// Copa (Arun and Balakrishnan, NSDI 2018), with slates' and focal's measured changes
    Copa(CopaConfig),
}

impl Default for Congestion {
    fn default() -> Self {
        Self::Cubic(CubicConfig::default())
    }
}

impl Congestion {
    /// A fresh controller for one path
    pub(crate) fn build(&self, now: Instant, current_mtu: u16) -> Box<dyn Controller> {
        match self {
            Self::Cubic(config) => Box::new(Cubic::new(config.clone(), now, current_mtu)),
            Self::NewReno(config) => Box::new(NewReno::new(config.clone(), now, current_mtu)),
            Self::Bbr(config) => Box::new(Bbr::new(config.clone(), current_mtu)),
            Self::Copa(config) => Box::new(Copa::new(config.clone(), now, current_mtu)),
        }
    }
}

/// RFC 9002 §7.2's initial window for datagrams of `max_datagram_size` bytes: "ten times the
/// maximum datagram size ... while limiting the window to the larger of 14,720 bytes or twice the
/// maximum datagram size"
pub(crate) fn initial_window(max_datagram_size: u64) -> u64 {
    max_datagram_size
        .saturating_mul(10)
        .min(max_datagram_size.saturating_mul(2).max(14_720))
}

/// The smallest maximum datagram size QUIC allows, 1200 bytes (RFC 9000 §14)
#[cfg(test)]
const BASE_DATAGRAM_SIZE: u64 = 1200;
