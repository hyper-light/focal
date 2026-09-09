//! Node liveness: a SWIM failure detector with the Lifeguard enhancements,
//! under-load deadline extensions and Vivaldi coordinates, whose verdicts the
//! partition owner commits ([24](../../../docs/archictecutre/24-placement-execution-and-fleet-control.md) §12).
#[cfg(test)]
#[path = "liveness/algorithm_tests.rs"]
mod algorithm_tests;
#[path = "liveness/coordinates.rs"]
pub mod coordinates;
#[path = "liveness/driver.rs"]
pub mod driver;
#[path = "liveness/gossip.rs"]
pub mod gossip;
#[path = "liveness/health.rs"]
pub mod health;
#[path = "liveness/suspicion.rs"]
pub mod suspicion;
#[path = "liveness/wire.rs"]
pub mod wire;

pub use driver::{
    LivenessConfig, LivenessCounters, LivenessDriver, LivenessEvent, LivenessHandle, LivenessView,
    LocalFacts, MemberView, ProbeError,
};
pub use gossip::MemberStatus;
