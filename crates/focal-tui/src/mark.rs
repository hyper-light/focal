//! The focal mark (hyperlight-site components/project-mark.tsx): a prism, light entering
//! at its left face, refracting through it to a focal point, and leaving at its right.
//! Drawn as shards draws its crystals, in Braille dots (two by four a cell), and as a solid
//! of glass rather than an outline: at sixteen dots a side an outline's slanted strokes
//! step unevenly and its inside is empty, which reads as broken. Here the prism climbs
//! two rows for every dot, so its sides step evenly; its sides are two dots thick and its
//! base one, so its weight does not sink; its body is a half-tone of glass, lit toward the
//! focal point; the site's rays run through it brighter than the glass; and the focal
//! point is a bright core whose cells carry nothing else, so it reads as a point of light,
//! never a bar. The light disperses across it as the site's prism gradient does, cool
//! where it enters and warm where it leaves; a glint travels the rim as the study's
//! beams carry theirs, the apex holds a highlight, and the focal point breathes.

use crate::canvas::{Canvas, Ink};
use crate::motion;
use crate::tokens::{self, Rgb};

type Pt = (f64, f64);

/// Ranks: the focal point over the glint over the rim over the rays over the glass. A cell
/// takes the colour of its weightiest dot.
const GLASS: u8 = 1;
const RAY: u8 = 2;
const RIM: u8 = 3;
const GLINT: u8 = 4;
const CORE: u8 = 5;

/// Where round the rim the glint is at time zero: high on the left side, where the light
/// enters (the rim runs down the left side, along the base, up the right).
const GLINT_START: f64 = 0.12;
/// The glass's and the rays' opacity over the page, of 255.
const GLASS_ALPHA: u8 = 140;
const RAY_ALPHA: u8 = 225;
/// How near a dot is to a ray, in dots, to be drawn as the ray.
const RAY_REACH: f64 = 0.55;
/// The focal point's radius, in dots, and how far its glow carries into the glass.
const CORE_RADIUS: f64 = 1.2;
const GLOW_REACH: f64 = 3.5;

/// The prism on a canvas: its apex, height and the rays through its focal point, all in
/// dots, mapped from the site's 32-unit box (`m16 4 13 24H3L16 4Z`, the rays
/// `m4 12 12 5 12-5M4 21l12-4 12 4M16 4v13`, the focal point at 16, 17).
struct Prism {
    apex: Pt,
    height: usize,
    focus: Pt,
    rays: [(Pt, Pt); 5],
}

impl Prism {
    fn on(canvas: &Canvas) -> Prism {
        let (w, h) = (canvas.width(), canvas.height());
        // As tall as the canvas allows, and as wide as tall (two rows for every dot each
        // side); the apex on a whole dot at the middle.
        let height = w.min(h).max(1.0);
        let top = ((h - height) / 2.0).floor();
        let apex = ((w / 2.0).floor(), top);
        let half = height / 2.0;
        let focus = (apex.0, top + height * 13.0 / 24.0);
        let reach = half * 12.0 / 13.0;
        let upper = top + height * 8.0 / 24.0;
        let lower = top + height * 17.0 / 24.0;
        let rays = [
            ((apex.0 - reach, upper), focus),
            ((apex.0 + reach, upper), focus),
            ((apex.0 - reach, lower), focus),
            ((apex.0 + reach, lower), focus),
            (apex, focus),
        ];
        Prism {
            apex,
            height: height as usize,
            focus,
            rays,
        }
    }

    /// Row `i` from the apex: its row in dots and its first and last dot.
    fn row(&self, i: usize) -> (f64, f64, f64) {
        let half = (i / 2) as f64;
        (
            self.apex.1 + i as f64,
            self.apex.0 - half,
            self.apex.0 + half,
        )
    }

    /// Where light is across the prism, in [0, 1]: 0 where it enters (the left), 1 where
    /// it leaves.
    fn across(&self, x: f64) -> f64 {
        let half = self.height as f64 / 2.0;
        if half > 0.0 {
            ((x - (self.apex.0 - half)) / (2.0 * half)).clamp(0.0, 1.0)
        } else {
            0.5
        }
    }

    /// How far round the rim a dot on it is, in [0, 1]: down the left side, along the
    /// base, up the right.
    fn round(&self, x: f64, y: f64) -> f64 {
        let depth = ((y - self.apex.1) / self.height.max(1) as f64).clamp(0.0, 1.0);
        let last = self.height.saturating_sub(1) as f64;
        if (y - self.apex.1) >= last && last > 0.0 {
            1.0 / 3.0 + self.across(x) / 3.0
        } else if x <= self.apex.0 {
            depth / 3.0
        } else {
            1.0 - depth / 3.0
        }
    }
}

/// Draws the mark at time `t` across the whole of `canvas`, centred.
pub fn draw(canvas: &mut Canvas, t: f64) {
    canvas.clear();
    let prism = Prism::on(canvas);
    let core_cells = core_cells(&prism);
    // The glint starts on the face the light enters, high on the left side.
    let glint_at = (GLINT_START + t * 0.21).rem_euclid(1.0);
    // The dispersion shimmers a little as the light moves.
    let shimmer = 0.08 * (t * 0.5).sin();
    let breath = motion::breath(t, 3.2, 0.0);
    for i in 0..prism.height {
        let (y, first, last) = prism.row(i);
        let base = i.saturating_add(1) == prism.height;
        let width = (last - first).round() as usize;
        for k in 0..=width {
            let x = first + k as f64;
            let in_core_cell = core_cells.contains(&cell(x, y));
            // The sides two dots thick, the base one.
            let rim = base || k < 2 || width.saturating_sub(k) < 2;
            // The whole spectrum across the prism: cool where the light enters, warm
            // where it leaves.
            let light = tokens::prism((0.05 + 0.85 * prism.across(x) + shimmer).clamp(0.0, 1.0));
            if in_core_cell && !rim {
                let d = dist((x, y), prism.focus);
                if d <= CORE_RADIUS {
                    let core = tokens::mix(tokens::FOCUS, tokens::BRIGHT, 0.4 + 0.6 * breath);
                    canvas.dot(x, y, Ink::solid(CORE, core));
                }
                continue;
            }
            if rim {
                // A highlight at the apex, and the glint where the beam is now.
                let apex = (1.0 - (y - prism.apex.1) / 2.0).clamp(0.0, 1.0);
                let color = tokens::mix(light, tokens::BRIGHT, 0.5 * apex);
                let d = (prism.round(x, y) - glint_at).abs();
                let glint = motion::glint(d.min(1.0 - d), 0.0, 0.05);
                let ink = if glint > 0.4 {
                    Ink::solid(GLINT, tokens::mix(color, tokens::BRIGHT, glint))
                } else {
                    Ink::solid(RIM, color)
                };
                canvas.dot(x, y, ink);
                continue;
            }
            let near = (-(dist((x, y), prism.focus) / GLOW_REACH).powi(2)).exp();
            let glowing = tokens::mix(light, tokens::FOCUS, 0.45 * near);
            let on_ray = prism
                .rays
                .iter()
                .any(|(a, b)| to_segment((x, y), *a, *b) <= RAY_REACH);
            if on_ray {
                canvas.dot(
                    x,
                    y,
                    Ink {
                        rank: RAY,
                        color: glowing,
                        alpha: RAY_ALPHA,
                    },
                );
            } else if (x + y).rem_euclid(2.0) < 0.5 {
                // Glass: half the dots, a half-tone, brighter toward the focal point.
                canvas.dot(
                    x,
                    y,
                    Ink {
                        rank: GLASS,
                        color: glowing,
                        alpha: GLASS_ALPHA,
                    },
                );
            }
        }
    }
}

/// The cells the focal point falls in: they carry the point alone.
fn core_cells(prism: &Prism) -> Vec<(i64, i64)> {
    let mut cells = Vec::new();
    let (fx, fy) = prism.focus;
    let reach = CORE_RADIUS.ceil();
    let mut y = (fy - reach).floor();
    while y <= fy + reach {
        let mut x = (fx - reach).floor();
        while x <= fx + reach {
            if dist((x, y), prism.focus) <= CORE_RADIUS {
                let at = cell(x, y);
                if !cells.contains(&at) {
                    cells.push(at);
                }
            }
            x += 1.0;
        }
        y += 1.0;
    }
    cells
}

/// The cell a dot falls in.
fn cell(x: f64, y: f64) -> (i64, i64) {
    ((x / 2.0).floor() as i64, (y / 4.0).floor() as i64)
}

fn dist(a: Pt, b: Pt) -> f64 {
    ((b.0 - a.0).powi(2) + (b.1 - a.1).powi(2)).sqrt()
}

/// The distance from `p` to the segment from `a` to `b`.
fn to_segment(p: Pt, a: Pt, b: Pt) -> f64 {
    let (vx, vy) = (b.0 - a.0, b.1 - a.1);
    let length = vx * vx + vy * vy;
    let u = if length > 0.0 {
        (((p.0 - a.0) * vx + (p.1 - a.1) * vy) / length).clamp(0.0, 1.0)
    } else {
        0.0
    };
    dist(p, (a.0 + u * vx, a.1 + u * vy))
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

    /// The dots of a cell's Braille character, as (x, y) within it.
    fn dots(ch: char) -> Vec<(usize, usize)> {
        let bits = (ch as u32).saturating_sub(0x2800);
        [
            (0, 0, 0x01),
            (0, 1, 0x02),
            (0, 2, 0x04),
            (1, 0, 0x08),
            (1, 1, 0x10),
            (1, 2, 0x20),
            (0, 3, 0x40),
            (1, 3, 0x80),
        ]
        .into_iter()
        .filter(|(_, _, b)| bits & b != 0)
        .map(|(x, y, _)| (x, y))
        .collect()
    }

    /// The lit dots of a drawn canvas, as a grid.
    fn grid(c: &Canvas) -> Vec<Vec<bool>> {
        let drawn = rows(c);
        let (w, h) = (c.cols() * 2, c.rows() * 4);
        let mut g = vec![vec![false; w]; h];
        for (r, line) in drawn.iter().enumerate() {
            for (col, ch) in line.chars().enumerate() {
                for (x, y) in dots(ch) {
                    g[r * 4 + y][col * 2 + x] = true;
                }
            }
        }
        g
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

    /// The sides step evenly: each row's outermost lit dots are those of a prism that
    /// widens one dot a side every two rows, symmetric about its apex, so no side is
    /// crooked; and nothing is drawn outside it.
    #[test]
    fn the_prism_steps_evenly_and_nothing_strays_outside_it() {
        for (cols, rows_) in [(8, 4), (16, 8)] {
            let mut c = Canvas::new(cols, rows_);
            draw(&mut c, 0.0);
            let g = grid(&c);
            let apex = cols; // the middle dot of 2·cols
            let height = (cols * 2).min(rows_ * 4);
            for (y, row) in g.iter().enumerate() {
                let lit: Vec<usize> = (0..row.len()).filter(|&x| row[x]).collect();
                if y >= height {
                    assert!(lit.is_empty(), "row {y} beyond the prism: {lit:?}");
                    continue;
                }
                let half = y / 2;
                assert_eq!(
                    (lit.first().copied(), lit.last().copied()),
                    (Some(apex - half), Some(apex + half)),
                    "{cols}x{rows_} row {y}"
                );
            }
        }
    }

    /// The focal point is a point of light: the cells it falls in carry it alone, so the
    /// rays meeting there never draw a bar across the prism.
    #[test]
    fn the_focal_point_carries_its_cells_alone() {
        let mut c = Canvas::new(8, 4);
        draw(&mut c, 0.0);
        let g = grid(&c);
        let prism = Prism::on(&c);
        for (cx, cy) in core_cells(&prism) {
            for y in (cy * 4)..(cy * 4 + 4) {
                for x in (cx * 2)..(cx * 2 + 2) {
                    let lit = g[y as usize][x as usize];
                    let near = dist((x as f64, y as f64), prism.focus) <= CORE_RADIUS;
                    assert_eq!(lit, near, "dot ({x}, {y})");
                }
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
