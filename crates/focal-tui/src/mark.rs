//! The focal mark (hyperlight-site components/project-mark.tsx): a prism, a triangle in
//! a 32×32 box, with light entering at its left face, refracting through it to a focal
//! point, and leaving at its right; the lines at 55%, the point solid. Here it moves as
//! focal's study moves: the light along the rim runs the spectral gradient, a glint
//! travels the rim as the study's beams carry theirs, and the focal point breathes.

use crate::canvas::{Canvas, Ink};
use crate::motion;
use crate::tokens::{self, Rgb};

type Pt = (f64, f64);

/// The prism: apex, right foot, left foot (`m16 4 13 24H3L16 4Z`).
const PRISM: [Pt; 3] = [(16.0, 4.0), (29.0, 28.0), (3.0, 28.0)];
/// The refraction: two rays bending through the focal point, and the axis down from the
/// apex (`m4 12 12 5 12-5M4 21l12-4 12 4M16 4v13`).
const RAYS: [[Pt; 3]; 2] = [
    [(4.0, 12.0), (16.0, 17.0), (28.0, 12.0)],
    [(4.0, 21.0), (16.0, 17.0), (28.0, 21.0)],
];
const AXIS: [Pt; 2] = [(16.0, 4.0), (16.0, 17.0)];
/// The focal point, and its radius.
const FOCUS: Pt = (16.0, 17.0);
const FOCUS_RADIUS: f64 = 2.0;

/// Ranks: the focal point over the rim over the rays.
const RAY: u8 = 1;
const RIM: u8 = 2;
const GLINT: u8 = 3;
const POINT: u8 = 4;

/// The part of the 32-unit box the mark's strokes cover (the rays reach x = 4 and 28;
/// the prism spans 3 to 29 across and 4 to 28 down), which is fitted to the canvas.
const EXTENT: (Pt, Pt) = ((3.0, 4.0), (29.0, 28.0));

/// The mark fitted to `canvas`: one scale for both axes (dots are square), the box's
/// corners on whole dots, so that its straight strokes land on rows and columns.
fn fit(canvas: &Canvas) -> impl Fn(Pt) -> Pt + use<> {
    let ((x0, y0), (x1, y1)) = EXTENT;
    let (w, h) = (x1 - x0, y1 - y0);
    let scale = ((canvas.width() - 1.0) / w)
        .min((canvas.height() - 1.0) / h)
        .max(0.0);
    let ox = ((canvas.width() - 1.0 - w * scale) / 2.0).round();
    let oy = ((canvas.height() - 1.0 - h * scale) / 2.0).round();
    move |p: Pt| (ox + (p.0 - x0) * scale, oy + (p.1 - y0) * scale)
}

/// Draws the mark at time `t` across the whole of `canvas`, centred: the rim in the
/// prism, light running along it with a glint where the beam is now; the rays at the
/// mark's 55%, each the colour of its light; the focal point solid and breathing.
pub fn draw(canvas: &mut Canvas, t: f64) {
    canvas.clear();
    let place = fit(canvas);
    for (k, ray) in RAYS.iter().enumerate() {
        let hue = tokens::prism(0.2 + 0.55 * k as f64);
        let [a, b, c] = *ray;
        let ink = Ink {
            rank: RAY,
            color: hue,
            alpha: 170,
        };
        canvas.line(place(a), place(b), |_| ink);
        canvas.line(place(b), place(c), |_| ink);
    }
    let [top, foot] = AXIS;
    canvas.line(place(top), place(foot), |_| Ink {
        rank: RAY,
        color: tokens::LAVENDER,
        alpha: 170,
    });
    let [apex, right, left] = PRISM;
    let edges = [(apex, right), (right, left), (left, apex)];
    let length: f64 = edges.iter().map(|(a, b)| dist(*a, *b)).sum();
    let glint_at = (t * 0.21).rem_euclid(1.0);
    let mut walked = 0.0;
    for (a, b) in edges {
        let span = dist(a, b);
        let start = walked;
        canvas.line(place(a), place(b), |u| {
            let along = if length > 0.0 {
                (start + u * span) / length
            } else {
                0.0
            };
            let color = tokens::prism_current(along, t, 9.0);
            let d = (along - glint_at).abs();
            let d = d.min(1.0 - d);
            let light = motion::glint(d, 0.0, 0.05);
            if light > 0.4 {
                Ink::solid(GLINT, tokens::mix(color, tokens::BRIGHT, light))
            } else {
                Ink::solid(RIM, color)
            }
        });
        walked += span;
    }
    // The focal point: a solid disc of the mark's radius, breathing.
    let (fx, fy) = place(FOCUS);
    let (ex, _) = place((FOCUS.0 + FOCUS_RADIUS, FOCUS.1));
    let r = (ex - fx).max(0.75);
    let breath = motion::breath(t, 3.2, 0.0);
    let color = tokens::mix(tokens::FOCUS_HAZE, tokens::BRIGHT, 0.6 + 0.4 * breath);
    let reach = r.ceil().clamp(1.0, 8.0) as i32;
    for dy in reach.saturating_neg()..=reach {
        for dx in reach.saturating_neg()..=reach {
            let (x, y) = (fx.round() + f64::from(dx), fy.round() + f64::from(dy));
            if (x - fx).powi(2) + (y - fy).powi(2) <= r * r + 0.3 {
                canvas.dot(x, y, Ink::solid(POINT, color));
            }
        }
    }
}

fn dist(a: Pt, b: Pt) -> f64 {
    ((b.0 - a.0).powi(2) + (b.1 - a.1).powi(2)).sqrt()
}

/// The mark's colour, for text beside it: its light now.
pub fn hue(t: f64) -> Rgb {
    tokens::spectral((t * 0.05).rem_euclid(1.0))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tokens::Paint;

    fn rows(c: &Canvas) -> Vec<String> {
        (0..c.rows())
            .map(|r| {
                let mut s = String::new();
                c.row(r, &Paint::new(true), &mut s);
                crate::seen(&s)
            })
            .collect()
    }

    #[test]
    fn the_mark_draws_its_prism_at_any_size() {
        for (cols, rows_) in [(8, 4), (16, 8), (24, 12)] {
            let mut c = Canvas::new(cols, rows_);
            draw(&mut c, 0.0);
            let drawn = rows(&c);
            assert_eq!(drawn.len(), rows_);
            let inked: usize = drawn
                .iter()
                .map(|r| r.chars().filter(|&ch| ch != ' ').count())
                .sum();
            assert!(inked >= cols + rows_, "{cols}x{rows_}: {drawn:#?}");
            for r in &drawn {
                assert_eq!(r.chars().count(), cols);
            }
        }
    }

    #[test]
    fn the_mark_moves_and_holds_still_at_one_time() {
        let paint = Paint::new(true);
        let colours = |t: f64| {
            let mut c = Canvas::new(16, 8);
            draw(&mut c, t);
            let mut s = String::new();
            for r in 0..c.rows() {
                c.row(r, &paint, &mut s);
            }
            s
        };
        assert_ne!(colours(0.0), colours(1.7), "the light moves along the rim");
        assert_eq!(colours(1.7), colours(1.7));
    }
}
