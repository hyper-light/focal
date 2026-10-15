//! What changes as it is watched: hairline bars, their leading cell lit by how far into
//! it they are, and a history of a measure as a sparkline, kept in a ring of fixed size.

use crate::page::Line;
use crate::tokens::{self, Paint, Rgb};

/// The eighths of a cell a bar's leading edge can stand at.
const EIGHTHS: [char; 9] = [' ', '▏', '▎', '▍', '▌', '▋', '▊', '▉', '█'];
/// The heights of a sparkline's cell.
const LEVELS: [char; 8] = ['▁', '▂', '▃', '▄', '▅', '▆', '▇', '█'];

/// A bar `width` cells long, `fraction` of it full, in `color` over the hairline: the
/// full cells solid, the cell at the edge filled by eighths, the rest the hairline.
pub fn bar(l: &mut Line, p: &Paint, width: usize, fraction: f64, color: Rgb) {
    let f = if fraction.is_nan() {
        0.0
    } else {
        fraction.clamp(0.0, 1.0)
    };
    let eighths = (f * width as f64 * 8.0).round().max(0.0) as usize;
    let full = (eighths / 8).min(width);
    let part = eighths.checked_rem(8).unwrap_or(0);
    let mut s = String::with_capacity(width.saturating_mul(3));
    s.extend(std::iter::repeat_n('█', full));
    l.put(p, color, &s);
    let mut drawn = full;
    if drawn < width && part > 0 {
        let edge = EIGHTHS.get(part).copied().unwrap_or(' ');
        let mut glyph = [0u8; 4];
        l.put(p, color, edge.encode_utf8(&mut glyph));
        drawn = drawn.saturating_add(1);
    }
    let rest = width.saturating_sub(drawn);
    if rest > 0 {
        let line: String = std::iter::repeat_n('─', rest).collect();
        l.put(p, tokens::LINE, &line);
    }
}

/// A measure's recent history: the last `N` samples, oldest first, in a ring that never
/// grows.
#[derive(Debug, Clone)]
pub struct History<const N: usize> {
    samples: [f64; N],
    /// Where the next sample goes.
    next: usize,
    /// How many samples it holds, at most `N`.
    held: usize,
}

impl<const N: usize> Default for History<N> {
    fn default() -> Self {
        History {
            samples: [0.0; N],
            next: 0,
            held: 0,
        }
    }
}

impl<const N: usize> History<N> {
    pub fn push(&mut self, v: f64) {
        if N == 0 {
            return;
        }
        if let Some(slot) = self.samples.get_mut(self.next) {
            *slot = if v.is_finite() { v } else { 0.0 };
        }
        self.next = self.next.saturating_add(1).checked_rem(N).unwrap_or(0);
        self.held = self.held.saturating_add(1).min(N);
    }

    /// The samples, oldest first.
    pub fn iter(&self) -> impl Iterator<Item = f64> + '_ {
        let start = if self.held < N { 0 } else { self.next };
        (0..self.held).filter_map(move |i| {
            let at = start.saturating_add(i).checked_rem(N)?;
            self.samples.get(at).copied()
        })
    }

    pub fn last(&self) -> Option<f64> {
        if self.held == 0 {
            return None;
        }
        let at = self.next.checked_add(N)?.checked_sub(1)?.checked_rem(N)?;
        self.samples.get(at).copied()
    }

    pub fn max(&self) -> f64 {
        self.iter().fold(0.0, f64::max)
    }

    /// The last `width` samples as a sparkline scaled to the largest of them, its newest
    /// cell brightest; empty cells where there are fewer samples than cells.
    pub fn sparkline(&self, l: &mut Line, p: &Paint, width: usize, color: Rgb) {
        let top = self.max();
        let skip = self.held.saturating_sub(width);
        let shown = self.held.saturating_sub(skip);
        l.pad(width.saturating_sub(shown));
        let mut glyph = [0u8; 4];
        for (i, v) in self.iter().skip(skip).enumerate() {
            let level = if top > 0.0 {
                ((v / top) * 7.0).round().clamp(0.0, 7.0) as usize
            } else {
                0
            };
            let age = if shown > 1 {
                i as f64 / (shown.saturating_sub(1)) as f64
            } else {
                1.0
            };
            let c = tokens::mix(tokens::mix(color, tokens::LINE, 0.55), color, age);
            let ch = LEVELS.get(level).copied().unwrap_or('▁');
            l.put(p, c, ch.encode_utf8(&mut glyph));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_bar_is_exactly_its_width() {
        let p = Paint::new(true);
        for f in [0.0, 0.07, 0.5, 0.99, 1.0, 2.0, f64::NAN] {
            let mut l = Line::default();
            bar(&mut l, &p, 20, f, tokens::SAGE);
            assert_eq!(crate::seen(&l.s).chars().count(), 20, "{f}");
        }
    }

    #[test]
    fn a_history_keeps_its_last_samples_in_order() {
        let mut h = History::<4>::default();
        assert_eq!(h.last(), None);
        for v in 1..=6 {
            h.push(f64::from(v));
        }
        assert_eq!(h.iter().collect::<Vec<_>>(), [3.0, 4.0, 5.0, 6.0]);
        assert_eq!(h.last(), Some(6.0));
        assert_eq!(h.max(), 6.0);
        let mut l = Line::default();
        h.sparkline(&mut l, &Paint::new(true), 6, tokens::TEAL);
        assert_eq!(crate::seen(&l.s), "  ▅▆▇█");
    }
}
