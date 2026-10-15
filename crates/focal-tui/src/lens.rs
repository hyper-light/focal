//! focal's study (hyperlight-site components/studies/focal.tsx): five glass lenses change
//! their spacing while three light paths find a focus. The same geometry as the site's,
//! in its 640-unit view: each lens a disc of eleven rings seen in perspective, leaning
//! and turning slowly, its rim catching a spectral sparkle; three beams curving in from
//! the left through the lenses to one focal point, dashes of light travelling them, and
//! the light leaving along the axis.

use crate::canvas::{Canvas, Ink};
use crate::motion;
use crate::tokens::{self, Rgb};

type Pt = (f64, f64);

const COUNT: usize = 5;
const RINGS: usize = 11;
const SAMPLES: usize = 73;
/// The intervals between those samples.
const SAMPLES_SPAN: f64 = 72.0;
/// Each lens's radius, outermost to the middle and back.
const RADII: [f64; COUNT] = [153.0, 178.0, 193.0, 178.0, 153.0];
/// The focus, in the view's units.
const FOCUS: Pt = (322.0, 308.0);
/// The part of the view the study occupies, which is fitted to the canvas.
const VIEW: (Pt, Pt) = ((48.0, 92.0), (592.0, 528.0));
/// The edge gradient's axis and the spectral gradient's (`x1 y1 x2 y2`).
const EDGE_AXIS: (Pt, Pt) = ((148.0, 167.0), (477.0, 439.0));
const SPECTRAL_AXIS: (Pt, Pt) = ((155.0, 110.0), (480.0, 498.0));
/// Points sampled along each curve of a beam to measure where its dashes are.
const BEAM_SAMPLES: usize = 40;

/// Ranks, back to front: the axis, the rings, the inner rims, the beams, the outlines,
/// the sparkles, the beams' glow, the focus.
const AXIS_RANK: u8 = 1;
const RING_RANK: u8 = 2;
const INNER_RANK: u8 = 3;
const BEAM_RANK: u8 = 4;
const OUTLINE_RANK: u8 = 5;
const SPARK_RANK: u8 = 6;
const GLOW_RANK: u8 = 7;
const FOCUS_RANK: u8 = 8;

/// Opacities are the site's, lifted for a terminal: a Braille dot is a sparser stroke
/// than a hairline on a screen, so each ink is shown this many times as opaque, at most
/// fully.
const LIFT: f64 = 2.4;

/// A point of lens `layer`'s circle of `radius` at `angle`, at time `t`, `thickness`
/// along the optical axis: the site's `lensPoint`, the lens placed on the axis by its
/// spacing, leaned toward the viewer, seen in perspective and turned.
fn lens_point(layer: usize, radius: f64, angle: f64, t: f64, thickness: f64) -> Pt {
    let layer = layer as f64;
    let center = (layer - 2.0) * (32.0 + (t * 0.66).sin() * 11.0);
    let y = angle.cos() * radius;
    let z = angle.sin() * radius;
    let x = center + thickness;
    let lean = 0.83 + (t * 0.5 + layer * 0.17).sin() * 0.14;
    let px = x * lean.cos() + z * lean.sin();
    let depth = -x * lean.sin() + z * lean.cos();
    let scale = 960.0 / (960.0 - depth);
    let turn = -0.27 + (t * 0.41).sin() * 0.075;
    (
        321.0 + (px * turn.cos() - y * turn.sin()) * scale,
        308.0 + (px * turn.sin() + y * turn.cos()) * scale,
    )
}

/// Where `p` falls along the gradient axis `axis`, in [0, 1].
fn along(p: Pt, axis: (Pt, Pt)) -> f64 {
    let ((x1, y1), (x2, y2)) = axis;
    let (dx, dy) = (x2 - x1, y2 - y1);
    let len = dx * dx + dy * dy;
    if len <= 0.0 {
        return 0.0;
    }
    (((p.0 - x1) * dx + (p.1 - y1) * dy) / len).clamp(0.0, 1.0)
}

/// An opacity of the site's (0–1) lifted for the terminal, of 255.
fn alpha(opacity: f64) -> u8 {
    tokens::channel((opacity * LIFT).clamp(0.0, 1.0) * 255.0)
}

/// The view fitted to a canvas: dots are square, so one scale for both axes, centred.
#[derive(Clone, Copy)]
struct Fit {
    scale: f64,
    ox: f64,
    oy: f64,
}

impl Fit {
    fn of(canvas: &Canvas) -> Fit {
        let ((x0, y0), (x1, y1)) = VIEW;
        let (w, h) = (x1 - x0, y1 - y0);
        let scale = (canvas.width() / w).min(canvas.height() / h);
        Fit {
            scale,
            ox: (canvas.width() - w * scale) / 2.0 - x0 * scale,
            oy: (canvas.height() - h * scale) / 2.0 - y0 * scale,
        }
    }

    fn place(self, p: Pt) -> Pt {
        (self.ox + p.0 * self.scale, self.oy + p.1 * self.scale)
    }
}

/// A closed circle of lens `layer`, stroked with `ink(point, piece)` for each of its
/// pieces, numbered round it.
fn circle(
    canvas: &mut Canvas,
    fit: Fit,
    layer: usize,
    radius: f64,
    t: f64,
    thickness: f64,
    ink: impl Fn(Pt, usize) -> Ink,
) {
    let mut last = lens_point(layer, radius, 0.0, t, thickness);
    for i in 1..SAMPLES {
        let angle = (i as f64 / SAMPLES_SPAN) * std::f64::consts::TAU;
        let p = lens_point(layer, radius, angle, t, thickness);
        let pen = ink(p, i);
        canvas.line(fit.place(last), fit.place(p), |_| pen);
        last = p;
    }
}

/// The dots a ring must stand from the next one drawn for the glass to read as rings,
/// not as a fill: a Braille dot is a stroke as wide as the gap it leaves.
const RING_GAP_DOTS: f64 = 7.0;

/// Draws the study at time `t` across the whole of `canvas`.
pub fn draw(canvas: &mut Canvas, t: f64) {
    canvas.clear();
    let fit = Fit::of(canvas);
    // The light leaving along the axis.
    let (fx, fy) = FOCUS;
    canvas.line(fit.place(FOCUS), fit.place((566.0, fy)), |_| Ink {
        rank: AXIS_RANK,
        color: tokens::AXIS,
        alpha: alpha(0.43 * 0.6),
    });
    beams(canvas, fit, t, false);
    for (layer, &radius) in RADII.iter().enumerate() {
        // The rings, fainter toward the middle of the glass: as many as stand apart at
        // this size, every `stride`th, each dashed.
        let rings = RINGS as f64;
        let spacing = (radius - 44.0) / (rings - 1.0) * fit.scale;
        let stride = (RING_GAP_DOTS / spacing.max(0.01)).ceil().clamp(1.0, rings) as usize;
        for ring in (1..RINGS).step_by(stride) {
            let k = ring as f64;
            let r = 31.0 + (k * (radius - 44.0)) / (rings - 1.0);
            let thickness = ((k / rings) * std::f64::consts::PI).sin() * 6.0;
            let opacity = 0.09 + (k / rings) * 0.18;
            circle(canvas, fit, layer, r, t, thickness, |p, piece| {
                let (c, a) = tokens::glass_edge(along(p, EDGE_AXIS));
                Ink {
                    rank: RING_RANK,
                    color: c,
                    alpha: if piece.checked_rem(3) == Some(0) {
                        alpha(0.7 * opacity * f64::from(a) / 255.0)
                    } else {
                        0
                    },
                }
            });
        }
        circle(canvas, fit, layer, radius - 7.0, t, 4.0, |p, _| {
            let (c, a) = tokens::glass_edge(along(p, EDGE_AXIS));
            Ink {
                rank: INNER_RANK,
                color: c,
                alpha: alpha(0.4 * f64::from(a) / 255.0),
            }
        });
        let strong = if layer == 2 { 1.0 } else { 0.8 };
        circle(canvas, fit, layer, radius, t, 0.0, |p, _| {
            let (c, a) = tokens::glass_edge(along(p, EDGE_AXIS));
            Ink {
                rank: OUTLINE_RANK,
                color: c,
                alpha: alpha(strong * f64::from(a) / 255.0),
            }
        });
        // The sparkle: a spectral arc running round the rim.
        let opacity = if layer & 1 == 0 { 0.7 } else { 0.32 };
        let lf = layer as f64;
        let mut last: Option<Pt> = None;
        for i in 0..23usize {
            let angle = (i as f64 / 22.0) * std::f64::consts::PI * 0.85 + t * 0.7 + lf * 0.6;
            let p = lens_point(layer, radius - 2.0, angle, t, 0.0);
            if let Some(a) = last {
                let color = tokens::spectral(along(p, SPECTRAL_AXIS));
                canvas.line(fit.place(a), fit.place(p), |_| Ink {
                    rank: SPARK_RANK,
                    color,
                    alpha: alpha(opacity),
                });
            }
            last = Some(p);
        }
    }
    beams(canvas, fit, t, true);
    // The focus: its haze, then its bright point.
    let haze = (38.0 * fit.scale).max(1.0);
    let core = (2.0 * fit.scale).max(0.6);
    let (cx, cy) = fit.place((fx, fy));
    let breath = motion::breath(t, 4.0, 0.0);
    let steps = (haze * 2.0).ceil().clamp(1.0, 64.0) as usize;
    let n = steps as f64;
    for i in 0..=steps {
        for j in 0..=steps {
            let x = cx - haze + 2.0 * haze * (i as f64 / n);
            let y = cy - haze + 2.0 * haze * (j as f64 / n);
            let d = ((x - cx).powi(2) + (y - cy).powi(2)).sqrt();
            if d <= core + 0.3 {
                canvas.dot(x, y, Ink::solid(FOCUS_RANK, tokens::FOCUS));
            } else if d <= haze * 0.35 && (i ^ j) & 1 == 0 {
                // The haze, sparse: the glow the site blurs, its light falling off.
                let fall = 1.0 - d / (haze * 0.35);
                canvas.dot(
                    x,
                    y,
                    Ink {
                        rank: GLOW_RANK,
                        color: tokens::mix(tokens::FOCUS_HAZE, tokens::FOCUS, fall),
                        alpha: alpha((0.18 + 0.27 * fall) * (0.75 + 0.25 * breath)),
                    },
                );
            }
        }
    }
}

/// The three beams at time `t`: their own strokes, dashed and travelling, or (`glow`)
/// the short spectral glints that run over the glass.
fn beams(canvas: &mut Canvas, fit: Fit, t: f64, glow: bool) {
    let sway = (t * 0.7).sin() * 13.0;
    for (index, branch) in [-1.0f64, 0.0, 1.0].into_iter().enumerate() {
        let y = 308.0 + branch * (115.0 + sway);
        let first = [
            (74.0, y),
            (177.0 + sway, y - branch * 11.0),
            (195.0, 308.0 + branch * 24.0),
            FOCUS,
        ];
        let second = [FOCUS, (386.0, 308.0), (465.0, 308.0), (567.0, 308.0)];
        // The path's length, so that its dashes are laid as `pathLength="1000"` lays them.
        let [origin, ..] = first;
        let mut length = 0.0;
        let mut prev = origin;
        for curve in [&first, &second] {
            for s in 1..=BEAM_SAMPLES {
                let p = bezier(curve, s as f64 / BEAM_SAMPLES as f64);
                length += dist(prev, p);
                prev = p;
            }
        }
        if length.is_nan() || length <= 0.0 {
            continue;
        }
        let i = index as f64;
        let offset = -(-t * 118.0 + i * 170.0);
        let base = tokens::BEAMS
            .get(index)
            .copied()
            .unwrap_or(tokens::FOREGROUND);
        let mut walked = 0.0;
        let mut prev = origin;
        for curve in [&first, &second] {
            for s in 1..=BEAM_SAMPLES {
                let p = bezier(curve, s as f64 / BEAM_SAMPLES as f64);
                let span = dist(prev, p);
                let start = walked;
                canvas.line(fit.place(prev), fit.place(p), |u| {
                    let at = (start + u * span) / length * 1000.0;
                    if glow {
                        if motion::dashed(at, offset, 1000.0, &[35.0, 965.0]) {
                            Ink {
                                rank: GLOW_RANK,
                                color: tokens::mix(
                                    tokens::spectral(along(p, SPECTRAL_AXIS)),
                                    tokens::BRIGHT,
                                    0.35,
                                ),
                                alpha: alpha(0.6),
                            }
                        } else {
                            Ink {
                                rank: 0,
                                color: base,
                                alpha: 0,
                            }
                        }
                    } else if motion::dashed(at, offset, 1000.0, &[60.0, 130.0, 10.0, 800.0]) {
                        Ink {
                            rank: BEAM_RANK,
                            color: tokens::mix(base, tokens::BRIGHT, 0.25),
                            alpha: 255,
                        }
                    } else {
                        // Between dashes the path is still faintly there.
                        Ink {
                            rank: BEAM_RANK,
                            color: base,
                            alpha: alpha(0.16),
                        }
                    }
                });
                walked += span;
                prev = p;
            }
        }
    }
}

/// A cubic Bézier curve at `s` in [0, 1].
fn bezier(c: &[Pt; 4], s: f64) -> Pt {
    let [a, b, d, e] = *c;
    let u = 1.0 - s;
    let w = (u * u * u, 3.0 * u * u * s, 3.0 * u * s * s, s * s * s);
    (
        w.0 * a.0 + w.1 * b.0 + w.2 * d.0 + w.3 * e.0,
        w.0 * a.1 + w.1 * b.1 + w.2 * d.1 + w.3 * e.1,
    )
}

fn dist(a: Pt, b: Pt) -> f64 {
    ((b.0 - a.0).powi(2) + (b.1 - a.1).powi(2)).sqrt()
}

/// The study's light for text beside it, now: the spectrum, slowly.
pub fn hue(t: f64) -> Rgb {
    tokens::spectral(motion::ping_pong(t / 9.0))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tokens::Paint;

    fn seen(c: &Canvas) -> Vec<String> {
        (0..c.rows())
            .map(|r| {
                let mut s = String::new();
                c.row(r, &Paint::new(true), &mut s);
                crate::seen(&s)
            })
            .collect()
    }

    #[test]
    fn the_lenses_fill_their_canvas_at_any_size() {
        for (cols, rows) in [(20, 10), (48, 24), (90, 40)] {
            let mut c = Canvas::new(cols, rows);
            draw(&mut c, 1.0);
            let drawn = seen(&c);
            let inked: usize = drawn
                .iter()
                .map(|r| r.chars().filter(|&ch| ch != ' ').count())
                .sum();
            assert!(inked > cols * rows / 4, "{cols}x{rows}: {inked}");
            for r in &drawn {
                assert_eq!(r.chars().count(), cols);
            }
        }
    }

    #[test]
    fn the_lenses_move_and_hold_still_at_one_time() {
        let at = |t: f64| {
            let mut c = Canvas::new(48, 24);
            draw(&mut c, t);
            seen(&c)
        };
        assert_ne!(at(0.0), at(2.0));
        assert_eq!(at(2.0), at(2.0));
    }

    #[test]
    fn the_beams_meet_at_the_focus() {
        for branch in [-1.0, 0.0, 1.0] {
            let y = 308.0 + branch * 115.0;
            let c = [(74.0, y), (177.0, y), (195.0, 308.0), FOCUS];
            assert_eq!(bezier(&c, 1.0), FOCUS);
            assert_eq!(bezier(&c, 0.0), (74.0, y));
        }
    }
}
