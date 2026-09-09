//! Lifeguard's local health multiplier: a node that is itself slow to probe
//! and answer accuses nobody hastily. The score saturates and every timeout
//! the detector applies is multiplied by `1 + 0.25·score`.
use serde::{Deserialize, Serialize};

pub const MAX_SCORE: u8 = 8;
const WEIGHT: f64 = 0.25;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct LocalHealth {
    pub score: u8,
}
impl LocalHealth {
    fn raise(&mut self) {
        self.score = self.score.saturating_add(1).min(MAX_SCORE);
    }
    fn lower(&mut self) {
        self.score = self.score.saturating_sub(1);
    }
    /// A probe this node sent went unanswered.
    pub fn on_probe_timeout(&mut self) {
        self.raise();
    }
    /// This node learned it was suspected and had to refute.
    pub fn on_refutation_needed(&mut self) {
        self.raise();
    }
    /// The driver's own tick ran late: this node is slow to answer.
    pub fn on_late_tick(&mut self) {
        self.raise();
    }
    pub fn on_successful_probe(&mut self) {
        self.lower();
    }
    pub fn on_successful_answer(&mut self) {
        self.lower();
    }
    /// `1.0` when healthy, up to `3.0` at saturation.
    pub fn multiplier(&self) -> f64 {
        1.0 + f64::from(self.score.min(MAX_SCORE)) * WEIGHT
    }
}
