//! Time, and the site's curves (hyperlight-site app/globals.css and
//! components/studies/use-study-motion.ts).
//!
//! A display keeps one [`Clock`]; everything it draws is a function of the clock's `t`,
//! as each study's `update(svg, t)` is. Under reduced motion the clock stands still and
//! transitions are already over: every piece shows its settled pose.

use std::time::Instant;

/// One display's time, in seconds.
#[derive(Debug, Clone, Copy)]
pub struct Clock {
    start: Instant,
    reduced: bool,
}

impl Clock {
    pub fn new(reduced: bool) -> Clock {
        Clock {
            start: Instant::now(),
            reduced,
        }
    }

    /// Seconds since the display began; [`SETTLED`], for ever, under reduced motion.
    pub fn t(&self) -> f64 {
        if self.reduced {
            SETTLED
        } else {
            self.start.elapsed().as_secs_f64()
        }
    }

    pub fn reduced(&self) -> bool {
        self.reduced
    }
}

/// The time a still is drawn at: the lenses spread and the beams met, as the study's
/// first frames settle.
pub const SETTLED: f64 = 2.4;

/// Whether motion is to be kept still: `FOCAL_MOTION=reduced` (or `off`, `none`), focal's
/// own setting, as the site honours `prefers-reduced-motion`.
pub fn reduced(var: impl Fn(&str) -> Option<String>) -> bool {
    var("FOCAL_MOTION").is_some_and(|v| matches!(v.as_str(), "reduced" | "off" | "none"))
}

/// The site's one easing curve, `cubic-bezier(0.16, 1, 0.3, 1)`: expo-out. `x` in [0, 1]
/// is time; the result is progress.
pub fn expo_out(x: f64) -> f64 {
    cubic_bezier(0.16, 1.0, 0.3, 1.0, x)
}

/// CSS's `cubic-bezier(x1, y1, x2, y2)` at time `x`: the curve's parameter found for
/// `x` by Newton's method, then by bisection where the slope is flat. Each search has its
/// own fixed count of steps.
pub fn cubic_bezier(x1: f64, y1: f64, x2: f64, y2: f64, x: f64) -> f64 {
    if x.is_nan() || x <= 0.0 {
        return 0.0;
    }
    if x >= 1.0 {
        return 1.0;
    }
    let curve = |a: f64, b: f64, s: f64| {
        let u = 1.0 - s;
        3.0 * u * u * s * a + 3.0 * u * s * s * b + s * s * s
    };
    let slope = |a: f64, b: f64, s: f64| {
        let u = 1.0 - s;
        3.0 * u * u * a + 6.0 * u * s * (b - a) + 3.0 * s * s * (1.0 - b)
    };
    let mut s = x;
    for _ in 0..8 {
        let err = curve(x1, x2, s) - x;
        if err.abs() < 1e-7 {
            return curve(y1, y2, s);
        }
        let d = slope(x1, x2, s);
        if d.abs() < 1e-6 {
            break;
        }
        s -= err / d;
    }
    let (mut lo, mut hi) = (0.0, 1.0);
    s = x;
    for _ in 0..40 {
        let v = curve(x1, x2, s);
        if (v - x).abs() < 1e-7 {
            break;
        }
        if v < x {
            lo = s;
        } else {
            hi = s;
        }
        s = (lo + hi) / 2.0;
    }
    curve(y1, y2, s)
}

/// The studies' smoothstep, `p²(3 − 2p)`, clamped.
pub fn smoothstep(p: f64) -> f64 {
    let p = if p.is_nan() { 0.0 } else { p.clamp(0.0, 1.0) };
    p * p * (3.0 - 2.0 * p)
}

/// A value following a target: `v + (target − v)(1 − e^(−rate·dt))`.
pub fn follow(v: f64, target: f64, rate: f64, dt: f64) -> f64 {
    v + (target - v) * (1.0 - (-rate * dt.max(0.0)).exp())
}

/// `x` there and back over each unit: 0 → 1 → 0, as `alternate` runs a keyframe.
pub fn ping_pong(x: f64) -> f64 {
    let x = if x.is_nan() { 0.0 } else { x.rem_euclid(2.0) };
    if x > 1.0 { 2.0 - x } else { x }
}

/// How much a glint at `center` lights `at`, both along [0, 1]: a Gaussian `width` wide.
pub fn glint(at: f64, center: f64, width: f64) -> f64 {
    if width <= 0.0 {
        return 0.0;
    }
    let d = (at - center) / width;
    (-d * d).exp()
}

/// A slow breath in [0, 1]: `0.5 + 0.5·sin(2πt/period + phase)`.
pub fn breath(t: f64, period: f64, phase: f64) -> f64 {
    if period <= 0.0 {
        return 0.5;
    }
    0.5 + 0.5 * (std::f64::consts::TAU * t / period + phase).sin()
}

/// Whether a dash pattern over a path of length `length` is lit at `along` (in the
/// path's own units): `dashes` alternate on and off as `stroke-dasharray` reads them,
/// shifted by `offset` as `stroke-dashoffset` shifts them.
pub fn dashed(along: f64, offset: f64, length: f64, dashes: &[f64]) -> bool {
    let period: f64 = dashes.iter().sum();
    if period.is_nan() || period <= 0.0 || length.is_nan() || length <= 0.0 {
        return true;
    }
    let mut at = (along + offset).rem_euclid(period);
    for (on, dash) in dashes.iter().enumerate() {
        if at < *dash {
            return on % 2 == 0;
        }
        at -= dash;
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn expo_out_is_the_sites_curve() {
        assert_eq!(expo_out(0.0), 0.0);
        assert_eq!(expo_out(1.0), 1.0);
        // At a quarter of the time, 0.825622 of the way: the curve sampled at 2 million
        // points, independently.
        let quarter = expo_out(0.25);
        assert!((quarter - 0.825_622).abs() < 1e-5, "{quarter}");
        let mut last = 0.0;
        for i in 1..=100 {
            let v = expo_out(f64::from(i) / 100.0);
            assert!(v >= last - 1e-9, "monotone at {i}: {v} < {last}");
            last = v;
        }
    }

    #[test]
    fn curves_hold_their_ends() {
        assert_eq!(smoothstep(-1.0), 0.0);
        assert_eq!(smoothstep(2.0), 1.0);
        assert_eq!(smoothstep(0.5), 0.5);
        assert_eq!(ping_pong(0.25), 0.25);
        assert_eq!(ping_pong(1.75), 0.25);
        assert!((follow(0.0, 1.0, 7.0, 10.0) - 1.0).abs() < 1e-9);
        assert_eq!(glint(0.5, 0.5, 0.1), 1.0);
        assert!(glint(0.9, 0.5, 0.1) < 1e-6);
        assert_eq!(breath(0.0, 0.0, 0.0), 0.5);
    }

    #[test]
    fn dashes_light_where_the_pattern_is_on() {
        let pattern = [60.0, 130.0, 10.0, 800.0];
        assert!(dashed(0.0, 0.0, 1000.0, &pattern));
        assert!(!dashed(100.0, 0.0, 1000.0, &pattern));
        assert!(dashed(195.0, 0.0, 1000.0, &pattern));
        assert!(!dashed(500.0, 0.0, 1000.0, &pattern));
        // Shifted: the first dash is lit 60 later.
        assert!(dashed(100.0, -60.0, 1000.0, &pattern));
        assert!(dashed(5.0, 0.0, 0.0, &pattern));
    }

    #[test]
    fn reduced_motion_stands_still() {
        let clock = Clock::new(true);
        assert_eq!(clock.t(), SETTLED);
        assert!(reduced(|_| Some("reduced".into())));
        assert!(!reduced(|_| None));
    }
}
