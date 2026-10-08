//! How focal looks on a colour terminal: hyperlight's design (hyperlight-site), drawn
//! with text. A quiet near-black page, prismatic colour only where it counts, motion that
//! is slow, continuous and subordinate to what matters, focal's mark and its study.
//!
//! - `tokens`: the site's colours, its prism, focal's spectrum and glass, and painting
//!   them on 24-bit or 256-colour terminals.
//! - `motion`: one clock per display and the site's curves.
//! - `canvas`: Braille cells as a 2×4 dot canvas, for drawing at sub-cell resolution.
//! - `mark`: the focal mark, its prism lit along its rim, its focal point breathing.
//! - `lens`: focal's study, five lenses and three beams finding one focus.
//! - `text`, `layout`: eyebrows, sizes, counts and times; text wrapped at words.
//! - `page`: the head, sections, usage lines, entries and options of every page.
//! - `panel`: errors, in a hairline panel.
//! - `frame`: drawing a frame over the last, in place.
//! - `meter`: hairline bars and sparklines for what changes as it is watched.
//! - `terminal`: whether output is a colour terminal, its size and its background.
//!
//! Nothing here allocates per frame once a display has grown to its size, and nothing
//! reads the clock but [`motion::Clock`]: a frame is a function of time, which tests set.
#![cfg_attr(
    test,
    allow(
        clippy::panic,
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::indexing_slicing,
        clippy::arithmetic_side_effects,
        clippy::disallowed_macros
    )
)]

pub mod canvas;
pub mod frame;
pub mod layout;
pub mod lens;
pub mod mark;
pub mod meter;
pub mod motion;
pub mod page;
pub mod panel;
pub mod terminal;
pub mod text;
pub mod tokens;

/// What a reader sees of `s`: its characters, without its escape sequences.
pub fn seen(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut esc = false;
    for ch in s.chars() {
        match (esc, ch) {
            (false, '\x1b') => esc = true,
            (true, c) if c.is_ascii_alphabetic() => esc = false,
            (true, _) => {}
            (false, ch) => out.push(ch),
        }
    }
    out
}
