//! Vivaldi network coordinates: every node places itself in a small
//! Euclidean space with a height for its access link, learning from each
//! acknowledged probe's round trip against the peer's coordinate. The
//! estimated round trip to a peer, with an upper confidence bound from both
//! coordinates' errors, drives that peer's probe timeout, so distance is
//! never mistaken for death and nearby peers are not waited on for seconds.
use serde::{Deserialize, Serialize};

pub const DIMENSIONS: usize = 8;

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct VivaldiConfig {
    /// Learning rate of a coordinate update.
    pub ce: f64,
    pub error_decay: f64,
    /// Centering pull toward the origin, applied every update.
    pub gravity: f64,
    pub height_adjustment: f64,
    pub adjustment_smoothing: f64,
    pub min_error: f64,
    pub max_error: f64,
    /// Confidence multiplier of the error margin in a round-trip bound.
    pub k_sigma: f64,
    pub rtt_default_ms: f64,
    pub sigma_default_ms: f64,
    pub sigma_min_ms: f64,
    pub sigma_max_ms: f64,
    pub rtt_min_ms: f64,
    pub rtt_max_ms: f64,
    /// Samples before an estimate is trusted over the defaults.
    pub min_samples: u32,
}
impl Default for VivaldiConfig {
    fn default() -> Self {
        Self {
            ce: 0.25,
            error_decay: 0.25,
            gravity: 0.01,
            height_adjustment: 0.25,
            adjustment_smoothing: 0.05,
            min_error: 0.05,
            max_error: 10.0,
            k_sigma: 2.0,
            rtt_default_ms: 100.0,
            sigma_default_ms: 50.0,
            sigma_min_ms: 1.0,
            sigma_max_ms: 500.0,
            rtt_min_ms: 1.0,
            rtt_max_ms: 10_000.0,
            min_samples: 3,
        }
    }
}

/// One node's position; carried in every probe and acknowledgement.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct NetworkCoordinate {
    /// Milliseconds along each axis.
    pub vec: [f64; DIMENSIONS],
    /// The access link's own latency, in milliseconds, never negative.
    pub height: f64,
    /// A bounded correction learned from prediction residuals.
    pub adjustment: f64,
    /// Relative error of the position, `min_error..=max_error`.
    pub error: f64,
    pub samples: u32,
}
impl NetworkCoordinate {
    pub fn origin(config: &VivaldiConfig) -> Self {
        Self {
            vec: [0.0; DIMENSIONS],
            height: 0.1,
            adjustment: 0.0,
            error: config.max_error,
            samples: 0,
        }
    }
    /// A coordinate received from a peer is used only when every field is a
    /// finite, sane number; anything else falls back to the defaults.
    pub fn is_valid(&self, config: &VivaldiConfig) -> bool {
        self.vec
            .iter()
            .all(|value| value.is_finite() && value.abs() <= config.rtt_max_ms)
            && self.height.is_finite()
            && (0.0..=config.rtt_max_ms).contains(&self.height)
            && self.adjustment.is_finite()
            && (-1.0..=1.0).contains(&self.adjustment)
            && self.error.is_finite()
            && (config.min_error..=config.max_error).contains(&self.error)
    }
    /// The round trip the two positions predict, in milliseconds.
    pub fn distance_ms(&self, other: &Self) -> f64 {
        let mut sum = 0.0;
        for (a, b) in self.vec.iter().zip(&other.vec) {
            let delta = a - b;
            sum += delta * delta;
        }
        (sum.sqrt() + self.height + other.height + self.adjustment + other.adjustment).max(0.0)
    }
    /// Learn from one acknowledged probe: move toward or away from the peer
    /// along the line between the two positions by the prediction error,
    /// weighted by relative confidence, and decay the error estimate.
    pub fn update(&mut self, peer: &Self, rtt_ms: f64, config: &VivaldiConfig) {
        if !rtt_ms.is_finite() || rtt_ms <= 0.0 || !peer.is_valid(config) {
            return;
        }
        let predicted = self.distance_ms(peer);
        let diff = rtt_ms - predicted;
        let mut unit = [0.0; DIMENSIONS];
        let mut norm = 0.0;
        for (slot, (a, b)) in unit.iter_mut().zip(self.vec.iter().zip(&peer.vec)) {
            *slot = a - b;
            norm += *slot * *slot;
        }
        let norm = norm.sqrt();
        if norm > f64::EPSILON {
            for value in &mut unit {
                *value /= norm;
            }
        } else {
            // Coincident positions: step along a fixed axis so they separate.
            unit[0] = 1.0;
        }
        let weight = self.error / (self.error + peer.error).max(config.min_error);
        let step = config.ce * weight;
        for (value, direction) in self.vec.iter_mut().zip(&unit) {
            *value += step * diff * direction;
            *value *= 1.0 - config.gravity;
            *value = value.clamp(-config.rtt_max_ms, config.rtt_max_ms);
        }
        self.height =
            (self.height + config.height_adjustment * step * diff).clamp(0.0, config.rtt_max_ms);
        self.adjustment = (self.adjustment + config.adjustment_smoothing * diff).clamp(-1.0, 1.0);
        let relative = if predicted > f64::EPSILON {
            (diff / predicted).abs()
        } else {
            diff.abs()
        };
        self.error = (self.error + config.error_decay * (relative - self.error))
            .clamp(config.min_error, config.max_error);
        self.samples = self.samples.saturating_add(1);
    }
    /// A quality in `0..=1` from samples and error; below one the estimate is
    /// blended with the defaults by callers.
    pub fn quality(&self, config: &VivaldiConfig) -> f64 {
        if self.samples < config.min_samples {
            return 0.0;
        }
        (1.0 - self.error / config.max_error).clamp(0.0, 1.0)
    }
}
/// The round-trip upper confidence bound between two positions, in
/// milliseconds: `rtt̂ + k_σ·σ`, clamped; conservative defaults when either
/// side has too few samples.
pub fn rtt_ucb_ms(
    local: &NetworkCoordinate,
    remote: Option<&NetworkCoordinate>,
    config: &VivaldiConfig,
) -> f64 {
    let (rtt_hat, sigma) = match remote {
        Some(remote)
            if remote.is_valid(config)
                && local.samples >= config.min_samples
                && remote.samples >= config.min_samples =>
        {
            let rtt_hat = local.distance_ms(remote);
            let sigma = ((local.error + remote.error) * rtt_hat.max(config.rtt_min_ms))
                .clamp(config.sigma_min_ms, config.sigma_max_ms);
            (rtt_hat, sigma)
        }
        _ => (config.rtt_default_ms, config.sigma_default_ms),
    };
    (rtt_hat + config.k_sigma * sigma).clamp(config.rtt_min_ms, config.rtt_max_ms)
}
