//! Drawing a display over its last frame, in place, writing only what changed.
//!
//! A frame is a list of lines. Drawn at the same size as the last, only the lines that
//! differ are written: the cursor goes up to the frame's top, steps over each run of
//! unchanged lines in one move, and rewrites the rest; a frame that changed nothing
//! writes nothing. When the width or the number of lines changed, the frame is drawn
//! whole: back up over the rows the last one takes at the terminal's width now, everything
//! below cleared, every line written.

use std::fmt::Write as _;
use std::io::Write;

/// What a frame's write begins and ends with: synchronized output on, the cursor hidden;
/// then both undone.
const BEGIN: &str = "\x1b[?2026h\x1b[?25l";
const END: &str = "\x1b[?25h\x1b[?2026l";

#[derive(Debug, Default)]
pub struct Frame {
    /// The last frame's lines, as written, and each one's visible width.
    drawn: Vec<(String, usize)>,
    /// The columns the last frame was drawn at.
    cols: usize,
    /// The frame being made; its strings kept between frames.
    next: Vec<(String, usize)>,
    buf: String,
    /// Lines made so far this frame.
    made: usize,
}

impl Frame {
    pub fn new() -> Frame {
        Frame::default()
    }

    /// Begins a frame.
    pub fn begin(&mut self) {
        self.made = 0;
    }

    /// The frame's next line: its text, with its colours, and its visible width. The
    /// line's storage is kept from frame to frame.
    pub fn put(&mut self, text: &str, width: usize) {
        match self.next.get_mut(self.made) {
            Some(slot) => {
                slot.0.clear();
                slot.0.push_str(text);
                slot.1 = width;
            }
            None => self.next.push((text.to_string(), width)),
        }
        self.made = self.made.saturating_add(1);
    }

    /// Writes what changed since the last frame, for a terminal `cols` wide, to `out`;
    /// returns the bytes written (none when nothing changed).
    pub fn end(&mut self, cols: usize, out: &mut impl Write) -> std::io::Result<usize> {
        let cols = cols.max(1);
        self.next.truncate(self.made);
        self.buf.clear();
        let whole = cols != self.cols || self.next.len() != self.drawn.len();
        if whole {
            let rows_at = |width: usize| -> usize {
                self.drawn
                    .iter()
                    .map(|(_, w)| w.div_ceil(width.max(1)).max(1))
                    .fold(0usize, usize::saturating_add)
            };
            // At the width now: lines the terminal rewrapped take more rows.
            let up = rows_at(self.cols).max(rows_at(cols));
            if up > 0 && !self.drawn.is_empty() {
                let _ = write!(self.buf, "\r\x1b[{up}A");
            }
            self.buf.push_str("\x1b[J");
            for (text, _) in &self.next {
                self.buf.push_str(text);
                self.buf.push_str("\x1b[0m\n");
            }
        } else {
            let first = self
                .next
                .iter()
                .zip(&self.drawn)
                .position(|(a, b)| a.0 != b.0);
            if let Some(first) = first {
                let up = self.drawn.len().saturating_sub(first);
                let _ = write!(self.buf, "\r\x1b[{up}A");
                let mut skip = 0usize;
                for (a, b) in self.next.iter().zip(&self.drawn).skip(first) {
                    if a.0 == b.0 {
                        skip = skip.saturating_add(1);
                        continue;
                    }
                    if skip > 0 {
                        let _ = write!(self.buf, "\x1b[{skip}B");
                        skip = 0;
                    }
                    self.buf.push_str("\r\x1b[2K");
                    self.buf.push_str(&a.0);
                    self.buf.push_str("\x1b[0m\n");
                }
                if skip > 0 {
                    let _ = write!(self.buf, "\x1b[{skip}B");
                }
                self.buf.push('\r');
            }
        }
        let written = self.buf.len();
        if written > 0 {
            // One update: the terminal shows the frame whole (synchronized output, DEC
            // mode 2026, which terminals without it ignore), and the cursor is hidden
            // while it moves over the lines redrawn, shown again in the same write.
            out.write_all(BEGIN.as_bytes())?;
            out.write_all(self.buf.as_bytes())?;
            out.write_all(END.as_bytes())?;
            out.flush()?;
        }
        std::mem::swap(&mut self.drawn, &mut self.next);
        self.cols = cols;
        Ok(written)
    }

    /// What follows is below the frame: nothing draws over it again.
    pub fn leave(&mut self) {
        self.drawn.clear();
        self.next.clear();
        self.cols = 0;
    }

    /// Erases the frame drawn last, leaving the cursor where it began.
    pub fn erase(&mut self, out: &mut impl Write) -> std::io::Result<()> {
        let rows = self
            .drawn
            .iter()
            .map(|(_, w)| w.div_ceil(self.cols.max(1)).max(1))
            .fold(0usize, usize::saturating_add);
        if rows > 0 {
            write!(out, "\r\x1b[{rows}A\x1b[J")?;
            out.flush()?;
        }
        self.leave();
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_what_changed_is_written() {
        let mut f = Frame::new();
        let mut out = Vec::new();
        f.begin();
        f.put("a", 1);
        f.put("b", 1);
        assert!(f.end(80, &mut out).unwrap() > 0);
        out.clear();
        f.begin();
        f.put("a", 1);
        f.put("b", 1);
        assert_eq!(f.end(80, &mut out).unwrap(), 0);
        assert!(out.is_empty());
        f.begin();
        f.put("a", 1);
        f.put("c", 1);
        f.end(80, &mut out).unwrap();
        let text = String::from_utf8(out).unwrap();
        assert!(text.contains('c') && !text.contains('a'), "{text:?}");
    }
}
