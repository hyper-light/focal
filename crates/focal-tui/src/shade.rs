//! Shaded drawing for a colour terminal: an image rendered at a supersampled
//! resolution, averaged down for anti-aliasing, and shown as half-block cells
//! (`▀`), each two pixels in true colour, so that glass and light have their
//! gradients rather than dots. The focal mark is drawn this way: the site's
//! prism (hyperlight-site components/project-mark.tsx) as lit glass, the glass
//! gradient of its study (components/studies/focal.tsx), its two rays in the
//! spectrum, a glint running its rim, and its focal point glowing.

use crate::tokens::{self, Paint, Rgb};

/// A premultiplied pixel: colour already weighted by its alpha, and the alpha.
#[derive(Clone, Copy, Default)]
struct Px {
    r: f64,
    g: f64,
    b: f64,
    a: f64,
}

/// An image of premultiplied pixels.
pub struct Image {
    w: usize,
    h: usize,
    px: Vec<Px>,
}

type Pt = (f64, f64);

fn rgb(c: Rgb) -> (f64, f64, f64) {
    (f64::from(c.0), f64::from(c.1), f64::from(c.2))
}

/// A float in pixels as an index, clamped to `[0, max]`; not-a-number is 0.
fn at(v: f64, max: usize) -> usize {
    if v.is_nan() || v <= 0.0 {
        return 0;
    }
    let max_f = max as f64;
    if v >= max_f { max } else { v as usize }
}

impl Image {
    fn new(w: usize, h: usize) -> Image {
        Image {
            w,
            h,
            px: vec![Px::default(); w.saturating_mul(h)],
        }
    }

    /// `c` at alpha `a` over the pixel at (`x`, `y`).
    fn over(&mut self, x: usize, y: usize, c: Rgb, a: f64) {
        if x >= self.w || a <= 0.0 || a.is_nan() {
            return;
        }
        let Some(p) = y
            .checked_mul(self.w)
            .and_then(|row| row.checked_add(x))
            .and_then(|i| self.px.get_mut(i))
        else {
            return;
        };
        let a = a.min(1.0);
        let (r, g, b) = rgb(c);
        p.r = r * a + p.r * (1.0 - a);
        p.g = g * a + p.g * (1.0 - a);
        p.b = b * a + p.b * (1.0 - a);
        p.a = a + p.a * (1.0 - a);
    }

    /// Fill a polygon, its colour and alpha by position.
    fn fill(&mut self, poly: &[Pt], shade: &dyn Fn(f64, f64) -> (Rgb, f64)) {
        let mut xs: Vec<f64> = Vec::with_capacity(poly.len());
        for y in 0..self.h {
            let yc = y as f64 + 0.5;
            xs.clear();
            for (i, &a) in poly.iter().enumerate() {
                let next = i.saturating_add(1);
                let b = poly
                    .get(next)
                    .or_else(|| poly.first())
                    .copied()
                    .unwrap_or(a);
                if (a.1 <= yc) != (b.1 <= yc) {
                    xs.push(a.0 + (yc - a.1) / (b.1 - a.1) * (b.0 - a.0));
                }
            }
            xs.sort_by(f64::total_cmp);
            for pair in xs.chunks_exact(2) {
                let (Some(&x0), Some(&x1)) = (pair.first(), pair.get(1)) else {
                    continue;
                };
                let (x0, x1) = (at(x0.round(), self.w), at(x1.round(), self.w));
                for x in x0..x1 {
                    let (c, a) = shade(x as f64 + 0.5, yc);
                    self.over(x, y, c, a);
                }
            }
        }
    }

    /// Stroke a polyline `width` wide, its ink by position along it in [0, 1].
    fn stroke(&mut self, pts: &[Pt], width: f64, ink: &dyn Fn(f64) -> (Rgb, f64)) {
        let total: f64 = pts
            .windows(2)
            .filter_map(|w| Some((*w.first()?, *w.get(1)?)))
            .map(|(a, b)| (b.0 - a.0).hypot(b.1 - a.1))
            .sum();
        let r = width / 2.0;
        let mut walked = 0.0;
        for (a, b) in pts
            .windows(2)
            .filter_map(|w| Some((*w.first()?, *w.get(1)?)))
        {
            let len = (b.0 - a.0).hypot(b.1 - a.1);
            let steps = at((len * 2.0).ceil().max(1.0), 1 << 16).max(1);
            for s in 0..=steps {
                let u = s as f64 / steps as f64;
                let (x, y) = (a.0 + (b.0 - a.0) * u, a.1 + (b.1 - a.1) * u);
                let along = if total > 0.0 {
                    (walked + len * u) / total
                } else {
                    0.0
                };
                let (c, alpha) = ink(along);
                // A disc a step wide: each pixel's coverage by distance, the
                // ink spread over the steps that overlap it.
                let weight = alpha / (width * 2.0).max(1.0) * 1.5;
                for yy in at(y - r - 1.0, self.h)..=at(y + r + 1.0, self.h) {
                    for xx in at(x - r - 1.0, self.w)..=at(x + r + 1.0, self.w) {
                        let d = ((xx as f64 + 0.5 - x).hypot(yy as f64 + 0.5 - y) - r).max(0.0);
                        let cover = (1.0 - d).clamp(0.0, 1.0);
                        if cover > 0.0 {
                            self.over(xx, yy, c, weight * cover);
                        }
                    }
                }
            }
            walked += len;
        }
    }

    /// A solid disc of radius `r`, anti-aliased.
    fn disc(&mut self, cx: f64, cy: f64, r: f64, c: Rgb, a: f64) {
        for y in at(cy - r - 2.0, self.h)..=at(cy + r + 2.0, self.h) {
            for x in at(cx - r - 2.0, self.w)..=at(cx + r + 2.0, self.w) {
                let d = (x as f64 + 0.5 - cx).hypot(y as f64 + 0.5 - cy);
                self.over(x, y, c, a * (r + 0.5 - d).clamp(0.0, 1.0));
            }
        }
    }

    /// A glow falling off from its centre over radius `r`.
    fn glow(&mut self, cx: f64, cy: f64, r: f64, c: Rgb, a: f64) {
        if r <= 0.0 {
            return;
        }
        for y in at(cy - r, self.h)..=at(cy + r, self.h) {
            for x in at(cx - r, self.w)..=at(cx + r, self.w) {
                let d = (x as f64 + 0.5 - cx).hypot(y as f64 + 0.5 - cy) / r;
                if d < 1.0 {
                    self.over(x, y, c, a * (1.0 - d).powi(2));
                }
            }
        }
    }

    /// The image averaged over `s`×`s` blocks: its anti-aliased pixels.
    fn down(&self, s: usize) -> Image {
        let s = s.max(1);
        let (w, h) = (self.w / s, self.h / s);
        let mut out = Image::new(w, h);
        let n = s.saturating_mul(s) as f64;
        for y in 0..h {
            for x in 0..w {
                let mut acc = Px::default();
                for yy in 0..s {
                    for xx in 0..s {
                        let i = y
                            .saturating_mul(s)
                            .saturating_add(yy)
                            .saturating_mul(self.w)
                            .saturating_add(x.saturating_mul(s).saturating_add(xx));
                        if let Some(p) = self.px.get(i) {
                            acc.r += p.r;
                            acc.g += p.g;
                            acc.b += p.b;
                            acc.a += p.a;
                        }
                    }
                }
                if let Some(slot) = out.px.get_mut(y.saturating_mul(w).saturating_add(x)) {
                    *slot = Px {
                        r: acc.r / n,
                        g: acc.g / n,
                        b: acc.b / n,
                        a: acc.a / n,
                    };
                }
            }
        }
        out
    }

    fn pixel(&self, x: usize, y: usize) -> Option<Px> {
        y.checked_mul(self.w)
            .and_then(|row| row.checked_add(x))
            .and_then(|i| self.px.get(i))
            .copied()
    }

    /// Row `row` of half-block cells (two pixel rows each), over the page:
    /// a pixel nothing covers is left the terminal's own background.
    pub fn row(&self, row: usize, paint: &Paint, out: &mut String) {
        let shown = |p: Option<Px>| -> Option<Rgb> {
            let p = p?;
            if p.a < 0.04 {
                return None;
            }
            let (pr, pg, pb) = rgb(paint.page);
            let rest = 1.0 - p.a.min(1.0);
            Some((
                tokens::channel(p.r + pr * rest),
                tokens::channel(p.g + pg * rest),
                tokens::channel(p.b + pb * rest),
            ))
        };
        let y = row.saturating_mul(2);
        for x in 0..self.w {
            let top = shown(self.pixel(x, y));
            let bottom = if y.saturating_add(1) < self.h {
                shown(self.pixel(x, y.saturating_add(1)))
            } else {
                None
            };
            match (top, bottom) {
                (None, None) => {
                    paint.reset(out);
                    out.push(' ');
                }
                (Some(t), None) => {
                    paint.reset(out);
                    paint.fg(out, t);
                    out.push('▀');
                }
                (None, Some(b)) => {
                    paint.reset(out);
                    paint.fg(out, b);
                    out.push('▄');
                }
                (Some(t), Some(b)) => {
                    paint.fg(out, t);
                    paint.bg(out, b);
                    out.push('▀');
                }
            }
        }
        paint.reset(out);
    }

    /// Rows of cells: half the pixel rows, rounded up.
    pub fn rows(&self) -> usize {
        self.h.div_ceil(2)
    }

    /// Cells across: one a pixel column.
    pub fn cols(&self) -> usize {
        self.w
    }
}

/// Samples a pixel is drawn from in each direction: 64 a pixel.
const SUPERSAMPLE: usize = 8;

/// The focal mark, `cols` cells wide (as many pixels across and down, half as
/// many rows), at time `t`: the prism as lit glass.
pub fn mark(cols: usize, t: f64) -> Image {
    let size = cols.saturating_mul(SUPERSAMPLE);
    let mut img = Image::new(size, size);
    let s = size as f64 / 32.0;
    let p = |x: f64, y: f64| (x * s, y * s);
    let (apex, right, left) = (p(16.0, 4.0), p(29.0, 28.0), p(3.0, 28.0));
    let focus = p(16.0, 17.0);
    // The study's ambient haze behind the glass.
    img.glow(focus.0, focus.1, 15.0 * s, (0x85, 0x97, 0xb3), 0.18);
    // The glass: the study's gradient, deepened so that it reads at this size.
    let glass = [
        (0.0, (0x62, 0x72, 0x7e), 0.30),
        (0.28, (0x1c, 0x24, 0x2d), 0.55),
        (0.54, (0xc4, 0xd5, 0xdf), 0.18),
        (0.8, (0x57, 0x67, 0x79), 0.32),
        (1.0, (0xb0, 0xb9, 0xcf), 0.28),
    ];
    img.fill(&[apex, right, left], &|x, y| {
        gradient(&glass, ((x / s - 6.0) * 0.6 + (y / s - 4.0) * 0.8) / 28.0)
    });
    // The rays, each the colour of its light in the spectrum, and the axis.
    let rays = [
        [p(4.0, 12.0), focus, p(28.0, 12.0)],
        [p(4.0, 21.0), focus, p(28.0, 21.0)],
    ];
    for (k, ray) in rays.iter().enumerate() {
        let c = tokens::spectral(0.25 + 0.5 * k as f64);
        img.stroke(ray, (0.9 * s).max(0.6), &|_| (c, 0.75));
    }
    img.stroke(&[apex, focus], (0.9 * s).max(0.6), &|_| {
        (tokens::LAVENDER, 0.6)
    });
    // The rim, its edge gradient, a glint travelling it.
    let edge = [
        (0.0, (0xc6, 0xd6, 0xdf), 0.9),
        (0.28, (0x77, 0x81, 0x8d), 0.5),
        (0.51, (0xc3, 0xc0, 0xcc), 0.85),
        (0.73, (0x7f, 0x87, 0x96), 0.45),
        (1.0, (0xe0, 0xdf, 0xeb), 0.9),
    ];
    let glint = (t * 0.21).rem_euclid(1.0);
    img.stroke(&[apex, right, left, apex], (1.4 * s).max(0.8), &|u| {
        let (c, a) = gradient(&edge, u);
        let d = (u - glint).abs();
        let g = crate::motion::glint(d.min(1.0 - d), 0.0, 0.05);
        (tokens::mix(c, tokens::BRIGHT, g), (a + g).min(1.0))
    });
    // The focal point, glowing and breathing.
    let breath = crate::motion::breath(t, 3.2, 0.0);
    img.glow(
        focus.0,
        focus.1,
        5.0 * s,
        tokens::FOCUS_HAZE,
        0.4 + 0.15 * breath,
    );
    img.disc(focus.0, focus.1, 2.0 * s, tokens::FOCUS, 1.0);
    img.down(SUPERSAMPLE)
}

/// A gradient of `(position, colour, alpha)` stops at `x` in [0, 1].
fn gradient(stops: &[(f64, Rgb, f64)], x: f64) -> (Rgb, f64) {
    let x = if x.is_nan() { 0.0 } else { x.clamp(0.0, 1.0) };
    let Some(&first) = stops.first() else {
        return (tokens::FOREGROUND, 0.0);
    };
    let mut prev = first;
    for &stop in stops {
        if x <= stop.0 {
            let span = stop.0 - prev.0;
            let k = if span > 0.0 { (x - prev.0) / span } else { 0.0 };
            return (
                tokens::mix(prev.1, stop.1, k),
                prev.2 + (stop.2 - prev.2) * k,
            );
        }
        prev = stop;
    }
    (prev.1, prev.2)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_mark_is_shaded_glass_at_any_size() {
        let paint = Paint::new(true);
        for cols in [12usize, 16, 24] {
            let img = mark(cols, 2.4);
            assert_eq!(img.rows(), cols / 2);
            let mut drawn = 0usize;
            for r in 0..img.rows() {
                let mut s = String::new();
                img.row(r, &paint, &mut s);
                let seen = crate::seen(&s);
                assert_eq!(seen.chars().count(), cols, "{seen:?}");
                drawn += seen.chars().filter(|c| *c != ' ').count();
            }
            // The prism covers about half its square.
            assert!(drawn > cols * cols / 6, "{cols}: {drawn}");
        }
    }

    #[test]
    fn the_mark_moves_and_holds_still_at_one_time() {
        let paint = Paint::new(true);
        let text = |t: f64| {
            let img = mark(16, t);
            let mut s = String::new();
            for r in 0..img.rows() {
                img.row(r, &paint, &mut s);
            }
            s
        };
        assert_ne!(text(0.0), text(1.7));
        assert_eq!(text(1.7), text(1.7));
    }
}
