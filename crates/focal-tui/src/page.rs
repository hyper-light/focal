//! focal's pages: the head (the mark beside the brand and the page's name in the site's
//! tracked capitals), sections under eyebrows, usage lines coloured by what their words
//! are, and entries in columns.

use crate::layout;
use crate::text;
use crate::tokens::{self, Paint, Rgb};

/// A line being made: its text, with colours, and its visible width.
#[derive(Default, Debug, Clone)]
pub struct Line {
    pub s: String,
    pub w: usize,
}

impl Line {
    pub fn pad(&mut self, n: usize) -> &mut Line {
        self.s.extend(std::iter::repeat_n(' ', n));
        self.w = self.w.saturating_add(n);
        self
    }

    /// Pads to column `col`.
    pub fn to(&mut self, col: usize) -> &mut Line {
        let n = col.saturating_sub(self.w);
        self.pad(n)
    }

    pub fn put(&mut self, p: &Paint, c: Rgb, text: &str) -> &mut Line {
        p.fg(&mut self.s, c);
        self.s.push_str(text);
        self.w = self.w.saturating_add(text.chars().count());
        self
    }

    pub fn bold(&mut self, p: &Paint, on: bool) -> &mut Line {
        p.bold(&mut self.s, on);
        self
    }
}

/// A page being made.
#[derive(Default, Debug)]
pub struct Page {
    pub lines: Vec<Line>,
    /// What `line` hands out were its just-pushed line not there: never, as it pushes
    /// first; kept so that the method needs neither a panic nor an option.
    spare: Line,
}

impl Page {
    pub fn new() -> Page {
        Page::default()
    }

    pub fn line(&mut self) -> &mut Line {
        self.lines.push(Line::default());
        let last = self.lines.len().saturating_sub(1);
        // Just pushed, so there; the spare is never taken.
        match self.lines.get_mut(last) {
            Some(line) => line,
            None => &mut self.spare,
        }
    }

    pub fn blank(&mut self) {
        self.lines.push(Line::default());
    }

    /// The page as text, every line reset at its end, and an empty line after it, as
    /// every screen ends.
    pub fn text(self, p: &Paint) -> String {
        let mut text = String::new();
        for mut l in self.lines {
            p.reset(&mut l.s);
            text.push_str(&l.s);
            text.push('\n');
        }
        text.push('\n');
        text
    }
}

/// The mark in the head, in Braille dots as shards draws its own, the site's geometry
/// (`components/project-mark.tsx`): six cells by three, a prism twelve dots a side,
/// beside the brand's two lines.
const MARK_COLS: usize = 6;
const MARK_ROWS: usize = 3;

/// The head: the mark (where there is room) beside the brand `F O C A L` in the prism,
/// `name` in tracked capitals after it, then `lines` under them.
pub fn head(page: &mut Page, p: &Paint, cols: usize, name: &str, lines: &[(Rgb, bool, &str)]) {
    let with_mark = cols >= 56;
    let mut mark = crate::canvas::Canvas::new(MARK_COLS, MARK_ROWS);
    if with_mark {
        // A still of the mark: a page is drawn once.
        crate::mark::draw(&mut mark, 0.0);
    }
    let room = cols.saturating_sub(if with_mark {
        MARK_COLS.saturating_add(3)
    } else {
        1
    });
    let mut info: Vec<Line> = Vec::new();
    let mut brand = Line::default();
    let letters = text::eyebrow("focal");
    let n = letters.chars().count().max(2).saturating_sub(1) as f64;
    brand.bold(p, true);
    let mut glyph = [0u8; 4];
    for (k, ch) in letters.chars().enumerate() {
        brand.put(p, tokens::prism(k as f64 / n), ch.encode_utf8(&mut glyph));
    }
    brand.bold(p, false);
    let brow = text::eyebrow(name);
    if !name.is_empty()
        && brand
            .w
            .saturating_add(5)
            .saturating_add(brow.chars().count())
            <= room
    {
        brand
            .put(p, tokens::SUBTLE, "  ·  ")
            .put(p, tokens::EYEBROW, &brow);
    }
    info.push(brand);
    for (c, bold, words) in lines {
        for (i, part) in layout::wrap(words, room).into_iter().enumerate() {
            let mut l = Line::default();
            l.bold(p, *bold && i == 0).put(p, *c, &part).bold(p, false);
            info.push(l);
        }
    }
    let rows = info.len().max(if with_mark { mark.rows() } else { 0 });
    let mut info = info.into_iter();
    for r in 0..rows {
        let l = page.line();
        l.pad(1);
        if with_mark {
            if r < mark.rows() {
                mark.row(r, p, &mut l.s);
                l.w = l.w.saturating_add(MARK_COLS);
            } else {
                l.pad(MARK_COLS);
            }
            l.pad(2);
        }
        if let Some(i) = info.next() {
            l.s.push_str(&i.s);
            l.w = l.w.saturating_add(i.w);
        }
    }
}

/// A section's heading: the site's eyebrow, in its grey.
pub fn heading(page: &mut Page, p: &Paint, words: &str) {
    page.blank();
    page.line()
        .pad(2)
        .put(p, tokens::EYEBROW, &words.to_uppercase());
}

/// A usage line, its words coloured by what they are: the command's own, its options,
/// and what it takes.
pub fn usage(page: &mut Page, p: &Paint, cols: usize, line: &str) {
    let l = page.line();
    l.pad(4);
    let mut first = true;
    for word in line.split(' ') {
        if !first {
            l.pad(1);
        }
        first = false;
        let c = if word.starts_with('[') && word.contains("OPTIONS") {
            tokens::SUBTLE
        } else if word.chars().any(|c| c.is_ascii_uppercase()) {
            tokens::TEAL
        } else {
            tokens::FOREGROUND
        };
        if l.w.saturating_add(word.chars().count()) > cols {
            break;
        }
        l.put(p, c, word);
    }
}

/// An entry of a list: a name (in the foreground), what it takes (teal), and what it
/// does (the body's grey), wrapped beside them.
pub struct Entry<'a> {
    pub name: &'a str,
    pub takes: &'a str,
    pub about: &'a str,
}

/// The column the abouts of `entries` start at, after the indent: past the longest name.
pub fn entries_pad(entries: &[Entry<'_>]) -> usize {
    entries
        .iter()
        .map(|e| {
            e.name
                .chars()
                .count()
                .saturating_add(usize::from(!e.takes.is_empty()))
                .saturating_add(e.takes.chars().count())
        })
        .max()
        .unwrap_or(0)
        .saturating_add(3)
}

/// Entries in columns, the abouts aligned past the longest name.
pub fn entries(page: &mut Page, p: &Paint, cols: usize, entries: &[Entry<'_>]) {
    entries_at(page, p, cols, entries, entries_pad(entries));
}

/// Entries in columns, the abouts aligned at `pad` after the indent, so that several
/// lists on one page share their column.
pub fn entries_at(page: &mut Page, p: &Paint, cols: usize, entries: &[Entry<'_>], pad: usize) {
    let pad = pad.max(entries_pad(entries));
    let about_w = cols.saturating_sub(pad.saturating_add(4));
    for e in entries {
        let parts = if about_w >= 20 {
            layout::wrap(e.about, about_w)
        } else {
            Vec::new()
        };
        let l = page.line();
        l.pad(4).put(p, tokens::FOREGROUND, e.name);
        if !e.takes.is_empty() {
            l.pad(1).put(p, tokens::TEAL, e.takes);
        }
        if let Some(first) = parts.first() {
            l.to(pad.saturating_add(4)).put(p, tokens::BODY, first);
        }
        for more in parts.iter().skip(1) {
            page.line()
                .pad(pad.saturating_add(4))
                .put(p, tokens::BODY, more);
        }
    }
}

/// An option: its short form (lavender), its long form, its value's kind (subtle), and
/// what it does, its default faint.
pub struct Opt<'a> {
    pub short: Option<char>,
    pub long: &'a str,
    pub value: &'a str,
    pub about: &'a str,
    pub default: Option<&'a str>,
}

/// Options in columns, what each does wrapped beside it, or under it where narrow.
pub fn options(page: &mut Page, p: &Paint, cols: usize, shown: &[Opt<'_>]) {
    let width = |o: &Opt<'_>| -> usize {
        8usize
            .saturating_add(o.long.chars().count())
            .saturating_add(if o.value.is_empty() {
                0
            } else {
                o.value.chars().count().saturating_add(1)
            })
    };
    let pad = shown.iter().map(width).max().unwrap_or(0).saturating_add(4);
    let beside = cols >= pad.saturating_add(28);
    let about_w = if beside {
        cols.saturating_sub(pad)
    } else {
        cols.saturating_sub(8)
    };
    for o in shown {
        let l = page.line();
        l.pad(4);
        match o.short {
            Some(s) => {
                l.put(p, tokens::LAVENDER, &format!("-{s}"));
                l.put(p, tokens::SUBTLE, ", ");
            }
            None => {
                l.pad(4);
            }
        }
        l.put(p, tokens::FOREGROUND, &format!("--{}", o.long));
        if !o.value.is_empty() {
            l.pad(1).put(p, tokens::SUBTLE, o.value);
        }
        let parts = layout::wrap(o.about, about_w.max(10));
        let default = o.default.map(|d| format!("default {d}"));
        if beside {
            if let Some(first) = parts.first() {
                l.to(pad).put(p, tokens::BODY, first);
            }
            for part in parts.iter().skip(1) {
                page.line().pad(pad).put(p, tokens::BODY, part);
            }
            if let Some(d) = &default {
                page.line().pad(pad).put(p, tokens::FAINT, d);
            }
        } else {
            for part in &parts {
                page.line().pad(8).put(p, tokens::BODY, part);
            }
            if let Some(d) = &default {
                page.line().pad(8).put(p, tokens::FAINT, d);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_head_names_focal_in_the_prism_beside_its_mark() {
        let p = Paint::new(true);
        for cols in [40usize, 80, 160] {
            let mut page = Page::new();
            head(
                &mut page,
                &p,
                cols,
                "post",
                &[(tokens::BRIGHT, true, "Post a claim")],
            );
            let text = crate::seen(&page.text(&p));
            assert!(text.contains("F O C A L"), "{text}");
            assert!(text.contains("Post a claim"), "{text}");
            if cols >= 60 {
                assert!(text.contains("P O S T"), "{text}");
                // The mark, in Braille dots.
                assert!(
                    text.chars().any(|c| ('\u{2801}'..='\u{28ff}').contains(&c)),
                    "{text}"
                );
            }
        }
    }

    #[test]
    fn entries_and_options_line_up() {
        let p = Paint::new(true);
        let mut page = Page::new();
        entries(
            &mut page,
            &p,
            80,
            &[
                Entry {
                    name: "post",
                    takes: "claim",
                    about: "Make a claim actionable",
                },
                Entry {
                    name: "acquire",
                    takes: "receipt",
                    about: "Take the work",
                },
            ],
        );
        options(
            &mut page,
            &p,
            80,
            &[Opt {
                short: Some('h'),
                long: "help",
                value: "",
                about: "Print help",
                default: None,
            }],
        );
        let text = crate::seen(&page.text(&p));
        let at = |needle: &str| {
            text.lines()
                .find_map(|l| l.find(needle))
                .unwrap_or(usize::MAX)
        };
        assert_eq!(at("Make a claim"), at("Take the work"));
        assert!(text.contains("-h, --help"));
    }
}
