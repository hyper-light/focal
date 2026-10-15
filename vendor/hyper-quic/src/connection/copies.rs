//! How long a copy of the handshake's flights waits behind its original
//! (`docs/research/burst-loss.md` §5).
//!
//! Losses come in bursts: on every path measured, a packet sent soon after a lost one is lost far
//! more often than the mean (Bolot, SIGCOMM 1993, Table 3; Jiang and Schulzrinne, NOSSDAV 2000,
//! Table 1), and a copy sent with its original is lost with it. Under a two-state loss chain in
//! time with correlation time `τ`, a copy `s` behind a lost original is lost too with probability
//! `r + (1 − r)·e^(−s/τ)` at mean loss `r`; when both are lost the flight waits a probe timeout
//! `C`. The expected added delay `s + C·(r + (1 − r)·e^(−s/τ))` is least at
//! `s = τ·ln((1 − r)·C/τ)`, taken here with `1 − r = 1` since the sender does not know `r`: on a
//! path whose probe timeout is shorter than a burst, the copy goes at once.
//!
//! The logarithm is in fixed point with integer operations alone, so a schedule is the same on
//! every host (Turner, "A Fast Binary Logarithm Algorithm", IEEE Signal Processing Magazine 27(5),
//! 2010; hyper-swim's `fixed.rs` computes it the same way).

use crate::Duration;

/// Fractional bits of the fixed-point logarithm. A representation width, not a tuning value: the
/// mantissa stays below `2^(bits + 1)`, whose square fits a `u64` with room to spare.
const FRACTION_BITS: u32 = 16;

/// `ln 2` in [`FRACTION_BITS`] fractional bits: `round(0.693_147_18 · 2^16)`.
const LN_2: u128 = 45_426;

/// The time a copy waits behind its original: `τ·ln(C/τ)` for a correlation time `burst` and a
/// probe timeout `pto`, and zero where the probe timeout is no longer than the burst or `burst` is
/// zero.
pub(super) fn copy_spacing(burst: Duration, pto: Duration) -> Duration {
    let tau = burst.as_nanos();
    let cost = pto.as_nanos();
    if tau == 0 || cost <= tau {
        return Duration::ZERO;
    }
    // C/τ in FRACTION_BITS fractional bits, then log2 of that less the scale; a ratio past 2^48
    // is taken at it, a spacing of 33τ
    let ratio = cost
        .checked_shl(FRACTION_BITS)
        .and_then(|scaled| scaled.checked_div(tau))
        .map_or(u64::MAX, |ratio| u64::try_from(ratio).unwrap_or(u64::MAX));
    let scale = u128::from(FRACTION_BITS)
        .checked_shl(FRACTION_BITS)
        .unwrap_or(0);
    let log2 = u128::from(log2_fixed(ratio)).saturating_sub(scale);
    let nanos = tau
        .saturating_mul(log2)
        .saturating_mul(LN_2)
        .checked_shr(FRACTION_BITS.saturating_mul(2))
        .unwrap_or(0);
    Duration::from_nanos(u64::try_from(nanos).unwrap_or(u64::MAX))
}

/// `log2(x)` in [`FRACTION_BITS`] fractional bits, for `x ≥ 1`, by Turner's iterative squaring: one
/// fractional bit an iteration.
pub(super) fn log2_fixed(x: u64) -> u64 {
    if x <= 1 {
        return 0;
    }
    let integer_part = (u64::BITS.saturating_sub(1)).saturating_sub(x.leading_zeros());
    let mut result = u64::from(integer_part)
        .checked_shl(FRACTION_BITS)
        .unwrap_or(0);
    // The mantissa in [1, 2), with its leading one at bit FRACTION_BITS
    let mut mantissa = if integer_part >= FRACTION_BITS {
        x.checked_shr(integer_part.saturating_sub(FRACTION_BITS))
            .unwrap_or(0)
    } else {
        x.checked_shl(FRACTION_BITS.saturating_sub(integer_part))
            .unwrap_or(0)
    };
    let two = 2_u64.checked_shl(FRACTION_BITS).unwrap_or(u64::MAX);
    for bit in (0..FRACTION_BITS).rev() {
        mantissa = mantissa
            .saturating_mul(mantissa)
            .checked_shr(FRACTION_BITS)
            .unwrap_or(0);
        if mantissa >= two {
            mantissa = mantissa.checked_shr(1).unwrap_or(0);
            result |= 1_u64.checked_shl(bit).unwrap_or(0);
        }
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    const MS: u64 = 1_000_000;

    #[test]
    fn a_copy_goes_at_once_where_a_probe_costs_no_more_than_a_burst() {
        let burst = Duration::from_millis(35);
        assert_eq!(
            copy_spacing(burst, Duration::from_millis(35)),
            Duration::ZERO
        );
        assert_eq!(
            copy_spacing(burst, Duration::from_millis(3)),
            Duration::ZERO
        );
        assert_eq!(
            copy_spacing(Duration::ZERO, Duration::from_secs(1)),
            Duration::ZERO
        );
    }

    #[test]
    fn a_power_of_two_spacing_is_its_logarithm() {
        // C/τ = 2^k: s = τ·k·ln 2, with ln 2 at 16 fractional bits.
        let burst = Duration::from_millis(32);
        for k in 1..10_u32 {
            let pto = burst * 2_u32.pow(k);
            let expected = (32 * MS as u128 * u128::from(k) * LN_2) >> 16;
            assert_eq!(copy_spacing(burst, pto).as_nanos(), expected, "k = {k}");
        }
    }

    #[test]
    fn the_first_probe_timeout_spaces_a_copy_past_a_measured_burst() {
        // τ = 35 ms, C = 999 ms (RFC 9002's first PTO from kInitialRtt): 35·ln(999/35) = 117.3 ms.
        let spacing = copy_spacing(Duration::from_millis(35), Duration::from_millis(999));
        assert!(
            (117 * MS..118 * MS).contains(&(spacing.as_nanos() as u64)),
            "{spacing:?}"
        );
    }
}
