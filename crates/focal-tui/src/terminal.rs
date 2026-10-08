//! What the terminal is: whether output goes to one, whether it shows colour and how
//! much, how wide it is, and the colour of its background.

use std::io::IsTerminal as _;

use crate::tokens::{Paint, Rgb};

/// The stream a page is written to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Stream {
    Stdout,
    Stderr,
}

impl Stream {
    fn is_terminal(self) -> bool {
        match self {
            Stream::Stdout => std::io::stdout().is_terminal(),
            Stream::Stderr => std::io::stderr().is_terminal(),
        }
    }
}

/// Whether colour is wanted at all: not under `NO_COLOR` (no-color.org) nor a `dumb`
/// terminal.
pub fn colour_wanted(var: &dyn Fn(&str) -> Option<String>) -> bool {
    var("NO_COLOR").is_none_or(|v| v.is_empty()) && var("TERM").is_none_or(|t| t != "dumb")
}

/// Whether the terminal says it shows 24-bit colour (`COLORTERM`), as most do: the rest
/// get xterm's 256.
pub fn truecolor(var: &dyn Fn(&str) -> Option<String>) -> bool {
    var("COLORTERM").is_some_and(|v| v == "truecolor" || v == "24bit")
}

fn env(key: &str) -> Option<String> {
    std::env::var(key).ok()
}

/// The paint for `stream`, if it is a colour terminal: focal's pages are drawn there,
/// and plain text written everywhere else. The background is asked of the terminal only
/// when `ask_background`, as a page is drawn once; a display asks once, too.
pub fn styled(stream: Stream, ask_background: bool) -> Option<Paint> {
    if !(stream.is_terminal() && colour_wanted(&env)) {
        return None;
    }
    let mut paint = Paint::new(truecolor(&env));
    if ask_background && let Some(page) = background() {
        paint.page = page;
    }
    Some(paint)
}

/// The terminal's width in columns for `stream`, or 80 where it is none.
pub fn width(stream: Stream) -> usize {
    size(stream).map_or(80, |(cols, _)| cols)
}

/// The terminal's columns and rows for `stream`, if it is one.
pub fn size(stream: Stream) -> Option<(usize, usize)> {
    let measured = match stream {
        Stream::Stdout => terminal_size::terminal_size_of(std::io::stdout()),
        Stream::Stderr => terminal_size::terminal_size_of(std::io::stderr()),
    };
    measured
        .map(|(w, h)| (usize::from(w.0), usize::from(h.0)))
        .filter(|(c, r)| *c > 0 && *r > 0)
}

/// The colour of the terminal's background, as it answers xterm's OSC 11 query
/// (`ESC ] 11 ; ? ST`, answered `ESC ] 11 ; rgb:RRRR/GGGG/BBBB`). A terminal that does
/// not answer it is told apart without waiting on a timer: the query is followed by DA1
/// (`ESC [ c`), which every terminal answers, and an answer to DA1 with none to OSC 11
/// before it says there is none. Asked of the controlling terminal, not stdin, which may
/// be a pipe; `None` where there is none, or it says nothing in `PATIENCE`.
#[cfg(unix)]
pub fn background() -> Option<Rgb> {
    use rustix::termios::{self, LocalModes, OptionalActions, SpecialCodeIndex};
    use std::io::{Read as _, Write as _};
    const PATIENCE: std::time::Duration = std::time::Duration::from_millis(300);
    let mut tty = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open("/dev/tty")
        .ok()?;
    let saved = termios::tcgetattr(&tty).ok()?;
    let mut raw = saved.clone();
    raw.local_modes
        .remove(LocalModes::ECHO | LocalModes::ICANON);
    // Reads wait at most a tenth of a second each (VTIME), and return what there is.
    raw.special_codes[SpecialCodeIndex::VMIN] = 0;
    raw.special_codes[SpecialCodeIndex::VTIME] = 1;
    termios::tcsetattr(&tty, OptionalActions::Now, &raw).ok()?;
    let mut heard = Vec::new();
    let asked = tty
        .write_all(b"\x1b]11;?\x1b\\\x1b[c")
        .and_then(|()| tty.flush());
    let deadline = std::time::Instant::now().checked_add(PATIENCE);
    if asked.is_ok() {
        let mut buf = [0u8; 128];
        // Each read waits at most VTIME; the deadline bounds the reads.
        while deadline.is_some_and(|d| std::time::Instant::now() < d) && heard.len() < 512 {
            match tty.read(&mut buf) {
                Ok(n) => {
                    if let Some(got) = buf.get(..n) {
                        heard.extend_from_slice(got);
                    }
                }
                Err(_) => break,
            }
            // DA1's answer ends `c` after `ESC [ ?`: everything is in.
            if heard.windows(3).any(|w| w == b"\x1b[?") && heard.ends_with(b"c") {
                break;
            }
        }
    }
    let _ = termios::tcsetattr(&tty, OptionalActions::Now, &saved);
    parse_background(&heard)
}

#[cfg(not(unix))]
pub fn background() -> Option<Rgb> {
    None
}

/// The colour in an OSC 11 answer: `rgb:` and three hexadecimal channels of one to four
/// digits each, scaled to eight bits.
pub fn parse_background(heard: &[u8]) -> Option<Rgb> {
    let text = std::str::from_utf8(heard).ok()?;
    let at = text.find("rgb:")?;
    let rest = text.get(at.saturating_add(4)..)?;
    let end = rest
        .find(|c: char| !(c.is_ascii_hexdigit() || c == '/'))
        .unwrap_or(rest.len());
    let mut parts = rest.get(..end)?.split('/');
    let mut channel = || -> Option<u8> {
        let hex = parts.next()?;
        let digits = u32::try_from(hex.len())
            .ok()
            .filter(|d| (1..=4).contains(d))?;
        let value = u32::from_str_radix(hex, 16).ok()?;
        let max = 16u32.checked_pow(digits)?.checked_sub(1)?;
        let scaled = value
            .checked_mul(255)?
            .checked_add(max / 2)?
            .checked_div(max)?;
        u8::try_from(scaled).ok()
    };
    Some((channel()?, channel()?, channel()?))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_background_answer_is_read() {
        assert_eq!(
            parse_background(b"\x1b]11;rgb:0808/0909/0a0a\x1b\\\x1b[?62;c"),
            Some((8, 9, 10))
        );
        assert_eq!(
            parse_background(b"\x1b]11;rgb:ff/80/00\x07"),
            Some((255, 128, 0))
        );
        assert_eq!(parse_background(b"\x1b[?62;c"), None);
        assert_eq!(parse_background(b"rgb:12345/0/0"), None);
    }

    #[test]
    fn colour_follows_the_environment() {
        assert!(colour_wanted(&|_| None));
        assert!(!colour_wanted(&|k| (k == "NO_COLOR").then(|| "1".into())));
        assert!(!colour_wanted(&|k| (k == "TERM").then(|| "dumb".into())));
        assert!(truecolor(
            &|k| (k == "COLORTERM").then(|| "truecolor".into())
        ));
    }
}
