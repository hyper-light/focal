//! One suspicion: its deadline shrinks from `max` toward `min` as
//! independent members confirm it, never rescheduled by repeated evidence,
//! and grows only by the bounded, decaying extensions a loaded host earns
//! with witnessed progress.
use std::collections::BTreeSet;

pub const MAX_CONFIRMERS: usize = 16;
pub const MAX_EXTENSIONS: u32 = 5;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExtensionDecision {
    Granted { millis: u64 },
    Denied(ExtensionDenial),
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExtensionDenial {
    /// The host reports its admission is refusing capacity: heal, not extend.
    Overloaded,
    /// No progress since the previous grant.
    NoProgress,
    /// Too soon after the previous grant.
    RateLimited,
    /// Every extension was used.
    Exhausted,
}
/// Logarithmically decaying grants, capped, witnessed by progress.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExtensionTracker {
    base_ms: u64,
    min_grant_ms: u64,
    min_interval_ms: u64,
    count: u32,
    last_witness: Option<u64>,
    last_grant_at_ms: Option<u64>,
    total_ms: u64,
}
impl ExtensionTracker {
    pub fn new(base_ms: u64, min_grant_ms: u64, min_interval_ms: u64) -> Self {
        Self {
            base_ms,
            min_grant_ms,
            min_interval_ms,
            count: 0,
            last_witness: None,
            last_grant_at_ms: None,
            total_ms: 0,
        }
    }
    pub fn total_ms(&self) -> u64 {
        self.total_ms
    }
    pub fn count(&self) -> u32 {
        self.count
    }
    /// Decide one request; a grant is committed here.
    pub fn request(&mut self, now_ms: u64, witness: u64, overloaded: bool) -> ExtensionDecision {
        if overloaded {
            return ExtensionDecision::Denied(ExtensionDenial::Overloaded);
        }
        if self.count >= MAX_EXTENSIONS {
            return ExtensionDecision::Denied(ExtensionDenial::Exhausted);
        }
        if self
            .last_grant_at_ms
            .is_some_and(|at| now_ms.saturating_sub(at) < self.min_interval_ms)
        {
            return ExtensionDecision::Denied(ExtensionDenial::RateLimited);
        }
        if self.last_witness.is_some_and(|last| witness <= last) {
            return ExtensionDecision::Denied(ExtensionDenial::NoProgress);
        }
        let grant = self
            .base_ms
            .checked_shr(self.count)
            .unwrap_or(0)
            .max(self.min_grant_ms);
        self.count = self.count.saturating_add(1);
        self.last_witness = Some(witness);
        self.last_grant_at_ms = Some(now_ms);
        self.total_ms = self.total_ms.saturating_add(grant);
        ExtensionDecision::Granted { millis: grant }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Suspicion {
    pub incarnation: u64,
    pub started_ms: u64,
    pub originator: u64,
    confirmers: BTreeSet<u64>,
    confirmations: u32,
    min_ms: u64,
    max_ms: u64,
    target: u32,
    pub extensions: ExtensionTracker,
}
impl Suspicion {
    /// `min_ms`/`max_ms` already carry the local health multiplier; `target`
    /// is the confirmation count at which the deadline reaches `min_ms`.
    pub fn new(
        incarnation: u64,
        started_ms: u64,
        originator: u64,
        min_ms: u64,
        max_ms: u64,
        target: u32,
        extensions: ExtensionTracker,
    ) -> Self {
        Self {
            incarnation,
            started_ms,
            originator,
            confirmers: BTreeSet::new(),
            confirmations: 0,
            min_ms,
            max_ms: max_ms.max(min_ms),
            target: target.max(1),
            extensions,
        }
    }
    /// An independent member confirmed the suspicion; the originator's own
    /// vote is the suspicion itself and never counts. Returns whether it was
    /// new.
    pub fn confirm(&mut self, from: u64) -> bool {
        if from == self.originator || self.confirmers.contains(&from) {
            return false;
        }
        if self.confirmers.len() < MAX_CONFIRMERS {
            self.confirmers.insert(from);
        }
        self.confirmations = self.confirmations.saturating_add(1);
        true
    }
    pub fn confirmations(&self) -> u32 {
        self.confirmations
    }
    /// `max − (max − min)·ln(C+1)/ln(K+1)`, never below `min`.
    pub fn timeout_ms(&self) -> u64 {
        let span = self.max_ms.saturating_sub(self.min_ms);
        let ratio =
            (f64::from(self.confirmations) + 1.0).ln() / (f64::from(self.target) + 1.0).ln();
        let ratio = if ratio.is_finite() {
            ratio.clamp(0.0, 1.0)
        } else {
            1.0
        };
        let reduced = (span as f64 * ratio).round();
        let reduced = if reduced.is_finite() && reduced >= 0.0 {
            // Bounded by `span`, which fits.
            (reduced.min(span as f64)) as u64
        } else {
            span
        };
        self.max_ms.saturating_sub(reduced).max(self.min_ms)
    }
    pub fn deadline_ms(&self) -> u64 {
        self.started_ms
            .saturating_add(self.timeout_ms())
            .saturating_add(self.extensions.total_ms())
    }
    pub fn expired(&self, now_ms: u64) -> bool {
        now_ms >= self.deadline_ms()
    }
}
