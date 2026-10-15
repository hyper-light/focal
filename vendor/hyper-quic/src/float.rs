//! Float-to-integer conversion with the meaning Rust's `as` gives it: the fraction dropped,
//! saturating at the integer's bounds, NaN to zero. Written without `as` so that the narrowing
//! is stated where it happens; the congestion controllers and the bloom filter's sizing compute
//! in `f64` (IEEE 754 binary64) and store integers.

use crate::Duration;

/// `d.mul_f32(factor)`, which panics where this saturates: a product past `Duration::MAX` is
/// `Duration::MAX`, and a negative or NaN one is zero.
pub(crate) fn scale_duration(d: Duration, factor: f32) -> Duration {
    let product = f64::from(factor) * d.as_secs_f64();
    Duration::try_from_secs_f64(product).unwrap_or(if product > 0.0 {
        Duration::MAX
    } else {
        Duration::ZERO
    })
}

/// `x as u64`.
pub(crate) fn saturating_u64(x: f64) -> u64 {
    /// 2^64, exactly representable in binary64.
    const TWO_POW_64: f64 = 18_446_744_073_709_551_616.0;
    /// The binary64 exponent bias (IEEE 754 §3.4).
    const BIAS: u64 = 1023;
    /// The bits of the binary64 significand's fraction (IEEE 754 §3.4).
    const FRACTION_BITS: u64 = 52;

    if x.is_nan() || x < 1.0 {
        return 0;
    }
    if x >= TWO_POW_64 {
        return u64::MAX;
    }
    let bits = x.to_bits();
    // 1 <= x < 2^64, so the unbiased exponent is 0..=63.
    let exponent = ((bits >> FRACTION_BITS) & 0x7ff).saturating_sub(BIAS);
    let significand = (bits & ((1 << FRACTION_BITS) - 1)) | (1 << FRACTION_BITS);
    match exponent.checked_sub(FRACTION_BITS) {
        Some(up) => significand << up,
        None => FRACTION_BITS
            .checked_sub(exponent)
            .map_or(0, |down| significand >> down),
    }
}

/// `x as u32`.
pub(crate) fn saturating_u32(x: f64) -> u32 {
    u32::try_from(saturating_u64(x)).unwrap_or(u32::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn matches_as() {
        for x in [
            f64::NAN,
            f64::NEG_INFINITY,
            -1.5,
            0.0,
            0.5,
            1.0,
            1.5,
            2.0,
            3.999_999,
            4_503_599_627_370_495.5,
            4_503_599_627_370_496.0,
            9_007_199_254_740_993.0,
            1e18,
            18_446_744_073_709_549_568.0,
            18_446_744_073_709_551_616.0,
            1e30,
            f64::INFINITY,
        ] {
            assert_eq!(saturating_u64(x), x as u64, "{x}");
            assert_eq!(saturating_u32(x), x as u32, "{x}");
        }
    }

    #[test]
    fn scales_as_mul_f32() {
        let d = Duration::from_millis(333);
        for factor in [0.0, 0.5, 1.125, 9.0 / 8.0, 3.0] {
            assert_eq!(scale_duration(d, factor), d.mul_f32(factor));
        }
        assert_eq!(scale_duration(Duration::MAX, 2.0), Duration::MAX);
        assert_eq!(scale_duration(d, -1.0), Duration::ZERO);
        assert_eq!(scale_duration(d, f32::NAN), Duration::ZERO);
    }
}
