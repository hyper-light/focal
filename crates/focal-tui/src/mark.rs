//! The focal mark (hyperlight-site components/project-mark.tsx): a prism, light entering
//! at its left face, refracting through it to a focal point, and leaving at its right.
//! Drawn as shards draws its crystals, in Braille dots (two by four a cell), as a solid of
//! glass: the prism climbs two rows for every dot, so its sides step evenly; its sides are
//! two dots thick and its base one; its body is a half-tone of glass, the site's rays and
//! the focal point lit through it.
//!
//! A cell shows one colour, so colour is drawn a cell at a time. A cell on the rim takes
//! the light where its rim dots are, cool where the light enters and warm where it leaves,
//! as the site's prism gradient runs; a cell of glass takes the same light, fainter. The
//! focal point clears no cell (cleared cells read as a hole in the glass): every cell is
//! drawn toward white by how near its lit dots lie to it, on average, so the light gathers
//! there and balances on the axis, though the axis runs down a column of dots and a cell
//! pairs two columns. The apex holds a highlight over the cells of its first rows alike, a
//! glint travels the rim from the face the light enters, and the focal point breathes.

use crate::canvas::{Canvas, Ink};
use crate::motion;
use crate::tokens::{self, Rgb};

type Pt = (f64, f64);

/// A cell's dots, as (x, y) within it, in dots.
const CELL: [Pt; 8] = [
    (0.0, 0.0),
    (1.0, 0.0),
    (0.0, 1.0),
    (1.0, 1.0),
    (0.0, 2.0),
    (1.0, 2.0),
    (0.0, 3.0),
    (1.0, 3.0),
];

/// Ranks: a cell on the rim over a cell the rays light over a cell of glass. The dots of a
/// cell share one ink, so ranks order nothing within a cell.
const GLASS: u8 = 1;
const RAY: u8 = 2;
const RIM: u8 = 3;

/// Where round the rim the glint is at time zero: high on the left side, where the light
/// enters (the rim runs down the left side, along the base, up the right).
const GLINT_START: f64 = 0.12;
/// The glint's spread, as a share of the way round the rim, and how far toward white it
/// draws the rim where it is: a sheen, since a cell is the least it can light, and a cell
/// of a prism twelve dots a side is an eighth of it.
const GLINT_SPREAD: f64 = 0.05;
const GLINT_LIGHT: f64 = 0.45;
/// The glass's and the rays' opacity over the page, of 255.
const GLASS_ALPHA: u8 = 140;
const RAY_ALPHA: u8 = 225;
/// How near a dot is to a ray, in dots, to be drawn as the ray.
const RAY_REACH: f64 = 0.55;
/// The focal point's radius, in dots: its dots are always lit.
const CORE_RADIUS: f64 = 1.2;
/// How far the focal point's light carries, in dots, and how far toward white it draws a
/// cell on the rim and a cell of glass where it is brightest.
const GLOW_REACH: f64 = 2.2;
const RIM_GLOW: f64 = 0.35;
const GLASS_GLOW: f64 = 0.6;
/// The apex's highlight, over the cells of its first four rows.
const APEX_ROWS: f64 = 4.0;
const APEX_HIGHLIGHT: f64 = 0.55;

/// What a lit dot of the prism is.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Part {
    Rim,
    Core,
    Ray,
    Glass,
}

/// The prism on a canvas: its apex, height and the rays through its focal point, all in
/// dots, mapped from the site's 32-unit box (`m16 4 13 24H3L16 4Z`, the rays
/// `m4 12 12 5 12-5M4 21l12-4 12 4M16 4v13`, the focal point at 16, 17).
struct Prism {
    apex: Pt,
    /// Its rows, a whole number.
    height: f64,
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
            height,
            focus,
            rays,
        }
    }

    /// What the dot at (`x`, `y`) is, or `None` where it is dark: outside the prism, or
    /// glass the half-tone leaves out.
    fn part(&self, x: f64, y: f64) -> Option<Part> {
        let i = y - self.apex.1;
        if i < 0.0 || i >= self.height {
            return None;
        }
        // Row `i` reaches `half` dots each side of the apex: a dot for every two rows.
        let half = (i / 2.0).floor();
        let k = x - (self.apex.0 - half);
        let width = 2.0 * half;
        if k < 0.0 || k > width {
            return None;
        }
        // The sides two dots thick, the base one.
        if i + 1.0 >= self.height || k < 2.0 || width - k < 2.0 {
            return Some(Part::Rim);
        }
        if dist((x, y), self.focus) <= CORE_RADIUS {
            return Some(Part::Core);
        }
        if self
            .rays
            .iter()
            .any(|(a, b)| to_segment((x, y), *a, *b) <= RAY_REACH)
        {
            return Some(Part::Ray);
        }
        // Glass: half the dots, a half-tone.
        ((x + y).rem_euclid(2.0) < 0.5).then_some(Part::Glass)
    }

    /// Where light is across the prism, in [0, 1]: 0 where it enters (the left), 1 where
    /// it leaves.
    fn across(&self, x: f64) -> f64 {
        let half = self.height / 2.0;
        if half > 0.0 {
            ((x - (self.apex.0 - half)) / (2.0 * half)).clamp(0.0, 1.0)
        } else {
            0.5
        }
    }

    /// How far round the rim a dot on it is, in [0, 1]: down the left side, along the
    /// base, up the right.
    fn round(&self, x: f64, y: f64) -> f64 {
        let depth = ((y - self.apex.1) / self.height.max(1.0)).clamp(0.0, 1.0);
        let last = (self.height - 1.0).max(0.0);
        if (y - self.apex.1) >= last && last > 0.0 {
            1.0 / 3.0 + self.across(x) / 3.0
        } else if x <= self.apex.0 {
            depth / 3.0
        } else {
            1.0 - depth / 3.0
        }
    }
}

/// The light at one time: where round the rim the glint is, the dispersion's shimmer, the
/// focal point's strength, and its white.
struct Light {
    glint_at: f64,
    shimmer: f64,
    glow: f64,
    white: Rgb,
}

impl Light {
    fn at(t: f64) -> Light {
        Light {
            // The glint starts on the face the light enters, high on the left side.
            glint_at: (GLINT_START + t * 0.21).rem_euclid(1.0),
            // The dispersion shimmers a little as the light moves.
            shimmer: 0.08 * (t * 0.5).sin(),
            // The focal point breathes about its still strength.
            glow: 0.8 + 0.4 * motion::breath(t, 3.2, 0.0),
            white: tokens::mix(tokens::FOCUS, tokens::BRIGHT, 0.35),
        }
    }

    /// The whole spectrum across the prism at `x`: cool where the light enters, warm where
    /// it leaves.
    fn spectrum(&self, prism: &Prism, x: f64) -> Rgb {
        tokens::prism((0.05 + 0.85 * prism.across(x) + self.shimmer).clamp(0.0, 1.0))
    }

    /// The ink of the cell whose top row is `top`, its lit dots `dots`; `None` for a cell
    /// with none.
    fn ink(&self, prism: &Prism, top: f64, dots: &[Option<(f64, f64, Part)>; 8]) -> Option<Ink> {
        let lit = || dots.iter().flatten();
        let n = lit().count() as f64;
        if n < 1.0 {
            return None;
        }
        // How near the cell's dots lie to the focal point, on average.
        let away = lit()
            .map(|(x, y, _)| dist((*x, *y), prism.focus))
            .sum::<f64>()
            / n;
        let near = (-(away / GLOW_REACH).powi(2)).exp() * self.glow;
        let rims = || lit().filter(|(_, _, part)| *part == Part::Rim);
        let on_rim = rims().count() as f64;
        if on_rim >= 1.0 {
            let x = rims().map(|(x, _, _)| *x).sum::<f64>() / on_rim;
            let mut color = self.spectrum(prism, x);
            // The apex's highlight, alike over every cell of its first rows.
            if top + 1.5 - prism.apex.1 < APEX_ROWS {
                color = tokens::mix(color, tokens::BRIGHT, APEX_HIGHLIGHT);
            }
            color = tokens::mix(color, self.white, RIM_GLOW * near);
            // The glint where the beam is now, by the cell's rim dot nearest it.
            let glint = rims()
                .map(|(x, y, _)| {
                    let d = (prism.round(*x, *y) - self.glint_at).abs();
                    motion::glint(d.min(1.0 - d), 0.0, GLINT_SPREAD)
                })
                .fold(0.0, f64::max);
            return Some(Ink::solid(
                RIM,
                tokens::mix(color, tokens::BRIGHT, GLINT_LIGHT * glint),
            ));
        }
        let x = lit().map(|(x, _, _)| *x).sum::<f64>() / n;
        let color = tokens::mix(self.spectrum(prism, x), self.white, GLASS_GLOW * near);
        let rayed = lit().any(|(_, _, part)| matches!(part, Part::Ray | Part::Core));
        Some(Ink {
            rank: if rayed { RAY } else { GLASS },
            color,
            alpha: if rayed { RAY_ALPHA } else { GLASS_ALPHA },
        })
    }
}

/// Draws the mark at time `t` across the whole of `canvas`, centred.
pub fn draw(canvas: &mut Canvas, t: f64) {
    canvas.clear();
    let prism = Prism::on(canvas);
    let light = Light::at(t);
    for row in 0..canvas.rows() {
        for col in 0..canvas.cols() {
            let (left, top) = (col as f64 * 2.0, row as f64 * 4.0);
            let dots = CELL.map(|(dx, dy)| {
                let (x, y) = (left + dx, top + dy);
                prism.part(x, y).map(|part| (x, y, part))
            });
            if let Some(ink) = light.ink(&prism, top, &dots) {
                for (x, y, _) in dots.iter().flatten() {
                    canvas.dot(*x, *y, ink);
                }
            }
        }
    }
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

    /// Whether the dot at (`x`, `y`) lies inside the prism drawn on `cols` by `rows_` cells.
    fn inside(cols: usize, rows_: usize, x: i64, y: i64) -> bool {
        let height = (cols * 2).min(rows_ * 4) as i64;
        let apex = cols as i64;
        (0..height).contains(&y) && (x - apex).abs() <= y / 2
    }

    #[test]
    fn the_mark_draws_its_prism_at_any_size() {
        for (cols, rows_) in [(6, 3), (8, 4), (16, 8), (24, 12)] {
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
        for (cols, rows_) in [(6, 3), (8, 4), (16, 8)] {
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

    /// The prism's dots are a mirror image about its apex: nothing leans.
    #[test]
    fn the_prism_is_drawn_alike_either_side_of_its_apex() {
        for (cols, rows_) in [(6, 3), (8, 4), (16, 8)] {
            for t in [0.0, 1.3, motion::SETTLED] {
                let mut c = Canvas::new(cols, rows_);
                draw(&mut c, t);
                let g = grid(&c);
                let apex = cols;
                for (y, row) in g.iter().enumerate() {
                    for x in 0..=apex.min(row.len().saturating_sub(1)) {
                        let mirror = 2 * apex - x;
                        assert_eq!(
                            row[x],
                            mirror < row.len() && row[mirror],
                            "{cols}x{rows_} at {t}: dot ({x}, {y}) against ({mirror}, {y})"
                        );
                    }
                }
            }
        }
    }

    /// The glass has no hole: a dark dot inside the prism has every neighbour inside it
    /// lit, so the half-tone never leaves two dark dots side by side, and the focal point
    /// clears nothing round it.
    #[test]
    fn the_glass_has_no_hole() {
        for (cols, rows_) in [(6, 3), (8, 4), (16, 8)] {
            let mut c = Canvas::new(cols, rows_);
            draw(&mut c, 0.0);
            let g = grid(&c);
            for (y, row) in g.iter().enumerate() {
                for (x, &on) in row.iter().enumerate() {
                    let (x, y) = (x as i64, y as i64);
                    if on || !inside(cols, rows_, x, y) {
                        continue;
                    }
                    for (nx, ny) in [(x - 1, y), (x + 1, y), (x, y - 1), (x, y + 1)] {
                        if inside(cols, rows_, nx, ny) {
                            assert!(
                                g[ny as usize][nx as usize],
                                "{cols}x{rows_}: dark ({x}, {y}) beside dark ({nx}, {ny})"
                            );
                        }
                    }
                }
            }
        }
    }

    /// The focal point is where the light gathers: its light draws the cell nearest it
    /// toward white, and leaves the prism's corners as the spectrum colours them.
    #[test]
    fn the_light_gathers_at_the_focal_point() {
        let c = Canvas::new(6, 3);
        let prism = Prism::on(&c);
        let lit = Light::at(0.0);
        let unlit = Light {
            glow: 0.0,
            ..Light::at(0.0)
        };
        let cell = |light: &Light, col: usize, row: usize| {
            let (left, top) = (col as f64 * 2.0, row as f64 * 4.0);
            let dots = CELL.map(|(dx, dy)| {
                let (x, y) = (left + dx, top + dy);
                prism.part(x, y).map(|part| (x, y, part))
            });
            light.ink(&prism, top, &dots).unwrap().color
        };
        let from_white = |c: Rgb| {
            let w = lit.white;
            let d = |a: u8, b: u8| (f64::from(a) - f64::from(b)).powi(2);
            (d(c.0, w.0) + d(c.1, w.1) + d(c.2, w.2)).sqrt()
        };
        let (focal, plain) = (cell(&lit, 3, 1), cell(&unlit, 3, 1));
        assert!(
            from_white(focal) + 10.0 < from_white(plain),
            "{focal:?} against {plain:?}"
        );
        for (col, row) in [(0, 2), (5, 2)] {
            assert_eq!(cell(&lit, col, row), cell(&unlit, col, row));
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
