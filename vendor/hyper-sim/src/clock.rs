//! Each node's clocks as views of virtual time (`docs/sim.md` §3.2).
//!
//! A node's monotonic clock reads `offset + ⌊v · (10⁶ + rate) / 10⁶⌋` at virtual time `v`: an
//! offset, and a rate in parts per million that the harness keeps within the deployment's stated
//! drift bound (the quantity a lease rests on, Gray and Cheriton §5). It never goes backward. The
//! wall clock is the monotonic clock moved to the node's epoch, and may also be stepped forward or
//! back (mantle's ±2 s steps; Antithesis's clock skips).

use std::time::{Duration, Instant};

use crate::error::SimError;

/// Parts per million in one: the unit of a clock's rate and of every probability (focal's rule,
/// `docs/sim.md` §3.4).
pub const PPM: u32 = 1_000_000;

/// How late a node's timers fire: `floor` plus a uniform draw below `spread`, in nanoseconds, as
/// hyper-liveness's owners wake late and hyper-timing measures a timer's granularity.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Lateness {
    /// The least lateness.
    pub floor_ns: u64,
    /// The spread above it: the draw is below this.
    pub spread_ns: u64,
}

/// A node's clocks.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Clock {
    /// The monotonic clock's reading at virtual time 0.
    pub offset_ns: u64,
    /// How much faster than virtual time the node's clocks run, in parts per million; negative is
    /// slower. Above −10⁶.
    pub rate_ppm: i32,
    /// The wall clock's reading at virtual time 0, in nanoseconds since its epoch.
    pub wall_ns: u64,
    /// How late its timers fire.
    pub lateness: Lateness,
}

impl Clock {
    /// `10⁶ + rate`: how many local nanoseconds a million virtual ones make, positive.
    fn pace(&self) -> Result<u64, SimError> {
        let pace = i64::from(PPM)
            .checked_add(i64::from(self.rate_ppm))
            .ok_or(SimError::Rate(self.rate_ppm))?;
        match u64::try_from(pace) {
            Ok(pace) if pace > 0 => Ok(pace),
            _ => Err(SimError::Rate(self.rate_ppm)),
        }
    }

    /// Whether the rate keeps the clock moving forward.
    pub(crate) fn check(&self) -> Result<(), SimError> {
        self.pace().map(|_| ())
    }

    /// The monotonic clock at virtual time `virtual_ns`. In `u64` throughout: with
    /// `v = q · 10⁶ + r`, `⌊v · pace / 10⁶⌋ = q · pace + ⌊r · pace / 10⁶⌋`, and `r · pace` is below
    /// `10⁶ · (10⁶ + 2³¹)`, about 2⁵¹ — no 128-bit division on the path every timer takes.
    pub fn monotonic(&self, virtual_ns: u64) -> Result<u64, SimError> {
        let pace = self.pace()?;
        let ppm = u64::from(PPM);
        let scaled = if pace == ppm {
            Some(virtual_ns)
        } else {
            let (q, r) = (virtual_ns.checked_div(ppm), virtual_ns.checked_rem(ppm));
            q.zip(r)
                .and_then(|(q, r)| Some((q.checked_mul(pace)?, r.checked_mul(pace)?)))
                .and_then(|(whole, part)| whole.checked_add(part.checked_div(ppm)?))
        };
        scaled
            .and_then(|scaled| scaled.checked_add(self.offset_ns))
            .ok_or(SimError::TimeOverflow)
    }

    /// The first virtual time at which the monotonic clock reads at least `local_ns`:
    /// `⌈(local − offset) · 10⁶ / pace⌉`, or 0 if it already did at 0. Exact, because
    /// `⌊x / 10⁶⌋ ≥ k` holds exactly when `x ≥ k · 10⁶` for whole `k`. With
    /// `local − offset = q · pace + r`, it is `q · 10⁶ + ⌈r · 10⁶ / pace⌉`, all in `u64`.
    pub fn virtual_at(&self, local_ns: u64) -> Result<u64, SimError> {
        let Some(ahead) = local_ns.checked_sub(self.offset_ns) else {
            return Ok(0);
        };
        let pace = self.pace()?;
        let ppm = u64::from(PPM);
        if pace == ppm {
            return Ok(ahead);
        }
        let overflow = SimError::TimeOverflow;
        let (q, r) = (
            ahead.checked_div(pace).ok_or(overflow.clone())?,
            ahead.checked_rem(pace).ok_or(overflow.clone())?,
        );
        let part = r
            .checked_mul(ppm)
            .and_then(|n| n.checked_add(pace.checked_sub(1)?))
            .and_then(|n| n.checked_div(pace))
            .ok_or(overflow.clone())?;
        q.checked_mul(ppm)
            .and_then(|whole| whole.checked_add(part))
            .ok_or(overflow)
    }

    /// The wall clock at virtual time `virtual_ns`, with `stepped_ns` the sum of its steps so far.
    pub fn wall(&self, virtual_ns: u64, stepped_ns: i64) -> Result<u64, SimError> {
        let elapsed = self
            .monotonic(virtual_ns)?
            .checked_sub(self.offset_ns)
            .ok_or(SimError::TimeOverflow)?;
        let wall = i128::from(self.wall_ns)
            .checked_add(i128::from(elapsed))
            .and_then(|n| n.checked_add(i128::from(stepped_ns)))
            .ok_or(SimError::TimeOverflow)?;
        u64::try_from(wall).map_err(|_| SimError::TimeOverflow)
    }
}

/// Virtual time as `Instant`s, for crates whose `now` is a `std::time::Instant` (hyper-transport,
/// hyper-quic, hyper-timing's waits): one host clock read, taken when the anchor is made, plus
/// virtual nanoseconds. Only differences of the instants it gives may enter a decision
/// (`docs/sim.md` §3.2), and a test that only needs an epoch takes it here rather than reading the
/// host clock itself.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Anchor(Instant);

impl Anchor {
    /// The anchor: the host's monotonic clock, read once.
    #[allow(
        clippy::disallowed_methods,
        reason = "the one host clock read of simulated time, its Instant anchor (docs/sim.md §3.2)"
    )]
    pub fn new() -> Self {
        Self(Instant::now())
    }

    /// The instant `virtual_ns` after the anchor.
    pub fn instant(&self, virtual_ns: u64) -> Result<Instant, SimError> {
        self.0
            .checked_add(Duration::from_nanos(virtual_ns))
            .ok_or(SimError::TimeOverflow)
    }
}

impl Default for Anchor {
    fn default() -> Self {
        Self::new()
    }
}
