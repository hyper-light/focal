//! Braille cells as a canvas: each cell a 2×4 grid of dots (U+2800–U+28FF), so a
//! terminal draws lines at four times its rows' resolution. Terminal cells are about
//! twice as tall as wide, so the dots come out square.

use crate::tokens::{Paint, Rgb};

/// A dot's ink: how much it matters (the highest wins a cell's one colour), its colour,
/// and its opacity over the page, of 255.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Ink {
    pub rank: u8,
    pub color: Rgb,
    pub alpha: u8,
}

impl Ink {
    /// `color`, opaque.
    pub const fn solid(rank: u8, color: Rgb) -> Ink {
        Ink {
            rank,
            color,
            alpha: 255,
        }
    }
}

/// A canvas of cells. Its dots are allocated once, when it is made or grows; drawing a
/// frame allocates nothing.
#[derive(Debug, Clone)]
pub struct Canvas {
    cols: usize,
    rows: usize,
    dots: Vec<Option<Ink>>,
}

/// The bit of a Braille cell's dot at column `x` (0–1) and row `y` (0–3).
fn bit(x: usize, y: usize) -> u32 {
    match (x, y) {
        (0, 0) => 0x01,
        (0, 1) => 0x02,
        (0, 2) => 0x04,
        (1, 0) => 0x08,
        (1, 1) => 0x10,
        (1, 2) => 0x20,
        (0, 3) => 0x40,
        (1, 3) => 0x80,
        _ => 0,
    }
}

/// The dots across `cols` cells and down `rows`: two across, four down each.
fn dots_of(cols: usize, rows: usize) -> usize {
    cols.saturating_mul(2)
        .saturating_mul(rows.saturating_mul(4))
}

impl Canvas {
    /// A canvas of `cols` × `rows` cells.
    pub fn new(cols: usize, rows: usize) -> Canvas {
        Canvas {
            cols,
            rows,
            dots: vec![None; dots_of(cols, rows)],
        }
    }

    /// The canvas at `cols` × `rows`, cleared; its storage reused when it already holds
    /// that many dots.
    pub fn resize(&mut self, cols: usize, rows: usize) {
        self.cols = cols;
        self.rows = rows;
        self.dots.clear();
        self.dots.resize(dots_of(cols, rows), None);
    }

    /// Dots across.
    pub fn width(&self) -> f64 {
        self.cols.saturating_mul(2) as f64
    }

    /// Dots down.
    pub fn height(&self) -> f64 {
        self.rows.saturating_mul(4) as f64
    }

    pub fn cols(&self) -> usize {
        self.cols
    }

    pub fn rows(&self) -> usize {
        self.rows
    }

    pub fn clear(&mut self) {
        self.dots.fill(None);
    }

    /// Inks the dot at (`x`, `y`), in dots, unless an ink that matters more is there. An
    /// ink with no opacity draws nothing: it is a gap in a dashed stroke.
    pub fn dot(&mut self, x: f64, y: f64, ink: Ink) {
        if ink.alpha == 0 {
            return;
        }
        if !(x >= -0.5 && y >= -0.5 && x < self.width() - 0.5 && y < self.height() - 0.5) {
            return;
        }
        // In range and not a number neither: both round to a dot on the canvas.
        let (x, y) = (x.round() as usize, y.round() as usize);
        let at = y
            .saturating_mul(self.cols.saturating_mul(2))
            .saturating_add(x);
        if let Some(slot) = self.dots.get_mut(at)
            && slot.is_none_or(|held| held.rank <= ink.rank)
        {
            *slot = Some(ink);
        }
    }

    /// A line from `a` to `b`, in dots, one dot thick: a dot for each step along its
    /// longer axis, as a digital differential analyser lays a line, so that no dot is
    /// inked twice and a diagonal reads as a stroke, not a smear. Its ink is chosen
    /// along it: `ink(u)` for `u` in [0, 1] from `a` to `b`. Steps are bounded by the
    /// canvas: a line that leaves it is walked at most twice the canvas's span.
    pub fn line(&mut self, a: (f64, f64), b: (f64, f64), ink: impl Fn(f64) -> Ink) {
        let span = (b.0 - a.0).abs().max((b.1 - a.1).abs());
        if !span.is_finite() {
            return;
        }
        let most = (self.width() + self.height()) * 2.0;
        let steps = span.round().clamp(1.0, most.max(1.0)) as usize;
        let n = steps as f64;
        for i in 0..=steps {
            let u = i as f64 / n;
            self.dot(a.0 + (b.0 - a.0) * u, a.1 + (b.1 - a.1) * u, ink(u));
        }
    }

    /// Row `row` of cells, each its Braille character in the colour of its weightiest
    /// ink; empty cells blank.
    pub fn row(&self, row: usize, paint: &Paint, out: &mut String) {
        let width = self.cols.saturating_mul(2);
        let mut last: Option<(Rgb, u8)> = None;
        for col in 0..self.cols {
            let mut bits = 0u32;
            let mut best: Option<Ink> = None;
            for y in 0..4usize {
                for x in 0..2usize {
                    let at = row
                        .saturating_mul(4)
                        .saturating_add(y)
                        .saturating_mul(width)
                        .saturating_add(col.saturating_mul(2))
                        .saturating_add(x);
                    if let Some(Some(ink)) = self.dots.get(at) {
                        bits |= bit(x, y);
                        if best.is_none_or(|b| b.rank < ink.rank) {
                            best = Some(*ink);
                        }
                    }
                }
            }
            match best {
                Some(ink) if bits != 0 => {
                    if last != Some((ink.color, ink.alpha)) {
                        paint.fg_over(out, ink.color, ink.alpha);
                        last = Some((ink.color, ink.alpha));
                    }
                    out.push(char::from_u32(0x2800 | bits).unwrap_or(' '));
                }
                _ => out.push(' '),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const INK: Ink = Ink {
        rank: 1,
        color: (255, 255, 255),
        alpha: 255,
    };

    fn text(c: &Canvas, row: usize) -> String {
        let mut s = String::new();
        c.row(row, &Paint::new(true), &mut s);
        crate::seen(&s)
    }

    #[test]
    fn dots_land_in_their_cells() {
        let mut c = Canvas::new(2, 1);
        c.dot(0.0, 0.0, INK);
        c.dot(3.0, 3.0, INK);
        assert_eq!(text(&c, 0), "⠁⢀");
        // Off the canvas, or not a number: nothing.
        c.dot(-1.0, 0.0, INK);
        c.dot(f64::NAN, 0.0, INK);
        c.dot(9.0, 0.0, INK);
        c.dot(0.0, f64::INFINITY, INK);
        assert_eq!(text(&c, 0), "⠁⢀");
    }

    #[test]
    fn a_line_inks_every_dot_on_its_way() {
        let mut c = Canvas::new(2, 1);
        c.line((0.0, 1.0), (3.0, 1.0), |_| INK);
        assert_eq!(text(&c, 0), "⠒⠒");
        // A diagonal is one dot thick: a dot in each column.
        let mut d = Canvas::new(2, 1);
        d.line((0.0, 0.0), (3.0, 3.0), |_| INK);
        assert_eq!(text(&d, 0), "⠑⢄");
        // A line to infinity draws nothing and ends.
        c.line((0.0, 0.0), (f64::INFINITY, 0.0), |_| INK);
        // One far beyond the canvas is walked a bounded way.
        c.line((0.0, 0.0), (1e18, 0.0), |_| INK);
    }

    #[test]
    fn the_weightier_ink_colours_the_cell() {
        let mut c = Canvas::new(1, 1);
        c.dot(0.0, 0.0, Ink::solid(2, (1, 2, 3)));
        c.dot(1.0, 0.0, Ink::solid(1, (9, 9, 9)));
        let mut s = String::new();
        c.row(0, &Paint::new(true), &mut s);
        assert!(s.contains("38;2;1;2;3"), "{s:?}");
    }

    #[test]
    fn a_canvas_resizes_in_place() {
        let mut c = Canvas::new(4, 2);
        c.dot(1.0, 1.0, INK);
        c.resize(2, 1);
        assert_eq!(text(&c, 0), "  ");
        assert_eq!((c.cols(), c.rows()), (2, 1));
    }
}
