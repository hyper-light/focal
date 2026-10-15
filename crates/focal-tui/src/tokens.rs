//! hyperlight's colours (hyperlight-site app/globals.css), focal's own from its study
//! (components/studies/focal.tsx), and painting them on a terminal.

use std::fmt::Write as _;

pub type Rgb = (u8, u8, u8);

/// The page: the site's `--background`, which inks with an opacity are drawn over where
/// the terminal does not say its own.
pub const PAGE: Rgb = (0x08, 0x09, 0x0a);
pub const FOREGROUND: Rgb = (0xed, 0xed, 0xee);
/// The headline's highlight.
pub const BRIGHT: Rgb = (0xf0, 0xf0, 0xef);
pub const BODY: Rgb = (0xa3, 0xa4, 0xab);
pub const MUTED: Rgb = (0x97, 0x99, 0x9f);
/// Eyebrows.
pub const EYEBROW: Rgb = (0xa1, 0xa3, 0xab);
pub const SUBTLE: Rgb = (0x79, 0x7c, 0x84);
/// The tiny cross, and footnotes.
pub const FAINT: Rgb = (0x6d, 0x70, 0x78);
/// Hairlines.
pub const LINE: Rgb = (0x24, 0x26, 0x2a);
/// A card's border on hover: a hairline that is there.
pub const EDGE: Rgb = (0x47, 0x46, 0x4f);
/// `.status-available`: done, and well.
pub const SAGE: Rgb = (0xa5, 0xc8, 0xb3);
/// `.status-development`: under way.
pub const AMBER: Rgb = (0xc6, 0xb0, 0x8e);
/// The mark's refraction stroke, and the focus ring.
pub const LAVENDER: Rgb = (0xbc, 0xb1, 0xd8);
/// The mark's second refraction stroke.
pub const TEAL: Rgb = (0xaa, 0xcb, 0xd0);
/// The prism's rose: the site has no red, and an error is the one place for it.
pub const ROSE: Rgb = (0xd6, 0xa2, 0xaa);

/// `--prism`: mint, sky, lavender, rose, sand, at 0, 28, 53, 75 and 100%.
pub const PRISM: [(f64, Rgb); 5] = [
    (0.0, (0xb8, 0xd9, 0xd2)),
    (0.28, (0xa6, 0xc4, 0xed)),
    (0.53, (0xb8, 0xa6, 0xd5)),
    (0.75, (0xd6, 0xa2, 0xaa)),
    (1.0, (0xd8, 0xcb, 0xb0)),
];

/// The focal study's spectral gradient, which its sparkles and the glow of its beams are
/// stroked with: lime, teal, periwinkle, orchid, clay.
pub const SPECTRAL: [(f64, Rgb); 5] = [
    (0.0, (0xd1, 0xd8, 0xb1)),
    (0.25, (0xa3, 0xcb, 0xd0)),
    (0.5, (0xa3, 0xaf, 0xe0)),
    (0.74, (0xc3, 0xa3, 0xcc)),
    (1.0, (0xd8, 0xae, 0x9c)),
];

/// The glass's edge: the gradient its outlines and rings are stroked with, each stop at
/// its opacity of 255.
pub const GLASS_EDGE: [(f64, Rgb, u8); 5] = [
    (0.0, (0xc6, 0xd6, 0xdf), 166),
    (0.28, (0x77, 0x81, 0x8d), 56),
    (0.51, (0xc3, 0xc0, 0xcc), 140),
    (0.73, (0x7f, 0x87, 0x96), 51),
    (1.0, (0xe0, 0xdf, 0xeb), 163),
];

/// The three light paths' own strokes: blue, violet, rose.
pub const BEAMS: [Rgb; 3] = [(0xa4, 0xc6, 0xd1), (0xbf, 0xad, 0xdb), (0xd3, 0xb4, 0xb7)];

/// The focus: its bright point, and the haze around it.
pub const FOCUS: Rgb = (0xe0, 0xe7, 0xef);
pub const FOCUS_HAZE: Rgb = (0xac, 0xba, 0xdb);
/// The axis the light leaves along.
pub const AXIS: Rgb = (0xd4, 0xdf, 0xe9);

/// `a` mixed toward `b` by `k` in [0, 1].
pub fn mix(a: Rgb, b: Rgb, k: f64) -> Rgb {
    let k = if k.is_nan() { 0.0 } else { k.clamp(0.0, 1.0) };
    let m = |x: u8, y: u8| channel(f64::from(x) + (f64::from(y) - f64::from(x)) * k);
    (m(a.0, b.0), m(a.1, b.1), m(a.2, b.2))
}

/// A colour channel from a value in [0, 255], rounded: a value outside is clamped, and
/// not-a-number is 0, so the cast below only ever sees 0..=255.
pub fn channel(v: f64) -> u8 {
    if v.is_nan() {
        return 0;
    }
    v.round().clamp(0.0, 255.0) as u8
}

/// A gradient of `stops` at `x` in [0, 1].
pub fn gradient(stops: &[(f64, Rgb)], x: f64) -> Rgb {
    let x = if x.is_nan() { 0.0 } else { x.clamp(0.0, 1.0) };
    let Some(&first) = stops.first() else {
        return FOREGROUND;
    };
    let mut prev = first;
    for &stop in stops {
        if x <= stop.0 {
            let span = stop.0 - prev.0;
            let k = if span > 0.0 { (x - prev.0) / span } else { 0.0 };
            return mix(prev.1, stop.1, k);
        }
        prev = stop;
    }
    prev.1
}

/// The prism at `x` in [0, 1].
pub fn prism(x: f64) -> Rgb {
    gradient(&PRISM, x)
}

/// The focal study's spectral gradient at `x` in [0, 1].
pub fn spectral(x: f64) -> Rgb {
    gradient(&SPECTRAL, x)
}

/// The glass edge at `x` in [0, 1]: its colour and its opacity of 255.
pub fn glass_edge(x: f64) -> (Rgb, u8) {
    let x = if x.is_nan() { 0.0 } else { x.clamp(0.0, 1.0) };
    let [first, ..] = GLASS_EDGE;
    let mut prev = first;
    for stop in GLASS_EDGE {
        if x <= stop.0 {
            let span = stop.0 - prev.0;
            let k = if span > 0.0 { (x - prev.0) / span } else { 0.0 };
            let alpha = channel(f64::from(prev.2) + (f64::from(stop.2) - f64::from(prev.2)) * k);
            return (mix(prev.1, stop.1, k), alpha);
        }
        prev = stop;
    }
    (prev.1, prev.2)
}

/// The prism as `prism-current` moves it: the gradient drawn 2.5 times its width, its
/// position running there and back over `period` seconds, eased in and out. `x` in
/// [0, 1] across what it colours.
pub fn prism_current(x: f64, t: f64, period: f64) -> Rgb {
    let phase = crate::motion::ping_pong(if period > 0.0 { t / period } else { 0.0 });
    let eased = crate::motion::smoothstep(phase);
    prism((x + eased * 1.5) / 2.5)
}

/// How colour is written: 24-bit where the terminal says it can, xterm's 256 otherwise.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Paint {
    pub truecolor: bool,
    /// The terminal's background, which inks with an opacity are drawn over.
    pub page: Rgb,
}

impl Paint {
    /// Paint for a terminal that shows 24-bit colour or not, over the site's page until
    /// the terminal says its own.
    pub const fn new(truecolor: bool) -> Paint {
        Paint {
            truecolor,
            page: PAGE,
        }
    }

    /// `c` at `alpha` of 255 over the page, as the foreground.
    pub fn fg_over(&self, out: &mut String, c: Rgb, alpha: u8) {
        self.fg(out, mix(self.page, c, f64::from(alpha) / 255.0));
    }

    pub fn fg(&self, out: &mut String, c: Rgb) {
        if self.truecolor {
            let _ = write!(out, "\x1b[38;2;{};{};{}m", c.0, c.1, c.2);
        } else {
            let _ = write!(out, "\x1b[38;5;{}m", xterm256(c));
        }
    }

    /// `c` as the background.
    pub fn bg(&self, out: &mut String, c: Rgb) {
        if self.truecolor {
            let _ = write!(out, "\x1b[48;2;{};{};{}m", c.0, c.1, c.2);
        } else {
            let _ = write!(out, "\x1b[48;5;{}m", xterm256(c));
        }
    }

    pub fn bold(&self, out: &mut String, on: bool) {
        out.push_str(if on { "\x1b[1m" } else { "\x1b[22m" });
    }

    pub fn reset(&self, out: &mut String) {
        out.push_str("\x1b[0m");
    }
}

/// The nearest of xterm's 6×6×6 cube or its 24 greys.
pub fn xterm256(c: Rgb) -> u8 {
    const STEPS: [u8; 6] = [0, 95, 135, 175, 215, 255];
    let level = |v: u8| -> (u8, u8) {
        let mut best = (0u8, 0u8);
        let mut err = i32::MAX;
        for (i, s) in (0u8..).zip(STEPS) {
            let e = i32::from(s).saturating_sub(i32::from(v)).saturating_abs();
            if e < err {
                err = e;
                best = (i, s);
            }
        }
        best
    };
    let ((r, rv), (g, gv), (b, bv)) = (level(c.0), level(c.1), level(c.2));
    // r, g and b are each below 6: 16 + 36·5 + 6·5 + 5 = 231 fits.
    let cube = 16u8
        .saturating_add(r.saturating_mul(36))
        .saturating_add(g.saturating_mul(6))
        .saturating_add(b);
    let sum = u32::from(c.0)
        .saturating_add(u32::from(c.1))
        .saturating_add(u32::from(c.2));
    let grey = u8::try_from((sum / 3).saturating_sub(3) / 10)
        .unwrap_or(23)
        .min(23);
    let level_of_grey = 8u8.saturating_add(grey.saturating_mul(10));
    let grey_rgb = (level_of_grey, level_of_grey, level_of_grey);
    if dist(c, grey_rgb) < dist(c, (rv, gv, bv)) {
        232u8.saturating_add(grey)
    } else {
        cube
    }
}

fn dist(a: Rgb, b: Rgb) -> i32 {
    let d = |x: u8, y: u8| {
        let v = i32::from(x).saturating_sub(i32::from(y));
        v.saturating_mul(v)
    };
    d(a.0, b.0)
        .saturating_add(d(a.1, b.1))
        .saturating_add(d(a.2, b.2))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn gradients_run_through_their_stops() {
        for (at, c) in PRISM {
            assert_eq!(prism(at), c);
        }
        for (at, c) in SPECTRAL {
            assert_eq!(spectral(at), c);
        }
        assert_eq!(prism(-1.0), PRISM[0].1);
        assert_eq!(prism(2.0), PRISM[4].1);
        assert_eq!(prism(f64::NAN), PRISM[0].1);
        assert_eq!(gradient(&[], 0.5), FOREGROUND);
        assert_eq!(glass_edge(0.0), (GLASS_EDGE[0].1, GLASS_EDGE[0].2));
        assert_eq!(glass_edge(1.0), (GLASS_EDGE[4].1, GLASS_EDGE[4].2));
    }

    #[test]
    fn channels_hold_their_range() {
        assert_eq!(channel(-4.0), 0);
        assert_eq!(channel(300.0), 255);
        assert_eq!(channel(f64::NAN), 0);
        assert_eq!(mix((0, 0, 0), (255, 255, 255), 0.5), (128, 128, 128));
    }

    #[test]
    fn colours_fall_back_to_xterms_256() {
        assert_eq!(xterm256((0, 0, 0)), 16);
        assert_eq!(xterm256((255, 255, 255)), 231);
        assert!((232..=255).contains(&xterm256(PAGE)));
        let mut s = String::new();
        Paint::new(false).fg(&mut s, SAGE);
        assert!(s.starts_with("\x1b[38;5;"));
    }
}
