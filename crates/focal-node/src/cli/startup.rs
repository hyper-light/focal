//! What `focal start node` shows on a colour terminal: focal's study, five lenses finding
//! their focus, beside the node's status while it starts; then, once it is ready, the
//! lenses settled and the status as a card, redrawn in place whenever the node reports a
//! new one. Nothing moves once the node is ready: a node left in the foreground spends no
//! processor time on its display. Off a terminal the node prints its JSON, as always.
//!
//! The display is one thread that owns the terminal's frame; the node tells it what
//! changed over a channel, and the thread ends when the node drops its end.

use focal_tui::canvas::Canvas;
use focal_tui::frame::Frame;
use focal_tui::motion::{self, Clock};
use focal_tui::page::Line;
use focal_tui::terminal::{self, Stream};
use focal_tui::tokens::{self, Paint, Rgb};
use focal_tui::{layout, lens, text};
use std::sync::mpsc::{self, Receiver, RecvTimeoutError, Sender};
use std::thread::JoinHandle;
use std::time::Duration;

/// A frame each 33 ms while the lenses move: the site's studies run at the display's
/// rate, and thirty frames a second is smooth on a terminal at a tenth of the work.
const FRAME: Duration = Duration::from_millis(33);
/// The longest the lenses move while a node has not yet said it is ready. A node waiting
/// on a quorum that does not come may wait for good; its display stops moving and says
/// it is still starting, so that waiting costs no processor time either.
const MOST_MOTION: Duration = Duration::from_secs(600);
/// The narrowest terminal the lenses are drawn on, beside the card.
const WITH_LENSES: usize = 76;

/// What the node said about itself last.
#[derive(Debug, Clone, Default)]
pub(crate) struct Card {
    condition: String,
    rows: Vec<(&'static str, String)>,
    meaning: Option<String>,
}

impl Card {
    /// The card of a status the node printed as JSON: its condition, the facts a person
    /// reads (identities as hexadecimal), and what its durability means.
    pub(crate) fn from_json(value: &serde_json::Value) -> Card {
        let text = |key: &str| value.get(key).map(plain);
        let mut rows = Vec::new();
        for (label, key) in [
            ("node", "node"),
            ("listen", "listen"),
            ("advertise", "advertise"),
            ("socket", "socket"),
            ("admin", "admin_socket"),
        ] {
            if let Some(v) = text(key).filter(|v| !v.is_empty() && v != "null") {
                rows.push((label, v));
            }
        }
        if let Some(ledger) = value.get("ledger") {
            for (label, key) in [("tenant", "tenant"), ("session", "session")] {
                if let Some(v) = ledger.get(key) {
                    rows.push((label, plain(v)));
                }
            }
        }
        if let Some(durability) = value.get("durability") {
            let survive = durability.get("survive").map(plain).unwrap_or_default();
            let failures = durability
                .get("max_failures")
                .map(plain)
                .unwrap_or_default();
            rows.push(("survives", format!("{survive} loss, up to {failures}")));
        }
        Card {
            condition: text("condition").unwrap_or_default(),
            rows,
            meaning: text("meaning"),
        }
    }

    fn ready(&self) -> bool {
        self.condition == "Ready"
    }
}

/// A value as a person reads it: strings bare, byte arrays as hexadecimal.
fn plain(value: &serde_json::Value) -> String {
    match value {
        serde_json::Value::String(s) => s.clone(),
        serde_json::Value::Array(items)
            if !items.is_empty() && items.iter().all(serde_json::Value::is_u64) =>
        {
            items
                .iter()
                .filter_map(serde_json::Value::as_u64)
                .filter_map(|b| u8::try_from(b).ok())
                .map(|b| format!("{b:02x}"))
                .collect()
        }
        other => other.to_string(),
    }
}

enum Note {
    Status(Card),
    /// The node is done: the last card stays, and what follows prints below it.
    Finish,
}

/// The display, while the node runs.
pub(crate) struct Show {
    notes: Option<Sender<Note>>,
    thread: Option<JoinHandle<()>>,
}

impl Show {
    /// The display for `title`, if stdout is a colour terminal; none elsewhere, where
    /// the node prints JSON.
    pub(crate) fn begin(title: &'static str) -> Option<Show> {
        let paint = terminal::styled(Stream::Stdout, true)?;
        let (notes, inbox) = mpsc::channel();
        let reduced = motion::reduced(|k| std::env::var(k).ok());
        let thread = std::thread::Builder::new()
            .name("focal-display".into())
            .spawn(move || run(paint, title, inbox, Clock::new(reduced)))
            .ok()?;
        Some(Show {
            notes: Some(notes),
            thread: Some(thread),
        })
    }

    /// The node's status, for the card.
    pub(crate) fn status(&self, value: &serde_json::Value) {
        if let Some(notes) = &self.notes {
            // A display that ended is no reason for the node to stop.
            let _ = notes.send(Note::Status(Card::from_json(value)));
        }
    }

    /// The last status, and the display ends below it.
    pub(crate) fn finish(mut self, value: &serde_json::Value) {
        self.status(value);
        if let Some(notes) = &self.notes {
            let _ = notes.send(Note::Finish);
        }
        self.end();
    }

    /// Without a last status (the node failed: its error is printed next), the display
    /// erases itself, so that the error stands alone.
    fn end(&mut self) {
        self.notes = None;
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

impl Drop for Show {
    fn drop(&mut self) {
        self.end();
    }
}

/// The display's thread: frames while the lenses move, then a frame for each status.
fn run(paint: Paint, title: &'static str, inbox: Receiver<Note>, clock: Clock) {
    let mut frame = Frame::new();
    let mut canvas = Canvas::new(0, 0);
    let mut card = Card {
        condition: "Starting".into(),
        ..Card::default()
    };
    let mut out = std::io::stdout();
    loop {
        let moving = !card.ready() && !clock.reduced() && clock.t() < MOST_MOTION.as_secs_f64();
        let t = if moving { clock.t() } else { motion::SETTLED };
        draw(
            &mut frame,
            &mut canvas,
            &paint,
            title,
            &card,
            t,
            moving,
            &mut out,
        );
        let next = if moving {
            match inbox.recv_timeout(FRAME) {
                Ok(note) => Some(note),
                Err(RecvTimeoutError::Timeout) => continue,
                Err(RecvTimeoutError::Disconnected) => None,
            }
        } else {
            // Still: nothing to draw until the node says something new.
            inbox.recv().ok()
        };
        match next {
            Some(Note::Status(next)) => card = next,
            Some(Note::Finish) => {
                // The card as it stands, once more, and what follows below it.
                draw(
                    &mut frame,
                    &mut canvas,
                    &paint,
                    title,
                    &card,
                    motion::SETTLED,
                    false,
                    &mut out,
                );
                frame.leave();
                return;
            }
            None => {
                let _ = frame.erase(&mut out);
                return;
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn draw(
    frame: &mut Frame,
    canvas: &mut Canvas,
    paint: &Paint,
    title: &str,
    card: &Card,
    t: f64,
    moving: bool,
    out: &mut impl std::io::Write,
) {
    let cols = terminal::width(Stream::Stdout);
    let lenses = cols >= WITH_LENSES;
    // The study's view is 544 × 436 units; at two dots a column and four a row, its
    // rows are two fifths of its columns.
    let lens_cols = if lenses { (cols / 2).clamp(28, 44) } else { 0 };
    let lens_rows = lens_cols.saturating_mul(2) / 5;
    if canvas.cols() != lens_cols || canvas.rows() != lens_rows {
        canvas.resize(lens_cols, lens_rows);
    }
    if lenses {
        lens::draw(canvas, t);
    }
    let room = cols.saturating_sub(lens_cols.saturating_add(4)).max(20);
    let info = card_lines(paint, title, card, t, moving, room);
    let rows = info.len().max(lens_rows);
    frame.begin();
    let mut info = info.into_iter();
    for r in 0..rows {
        let mut l = Line::default();
        l.pad(1);
        if lenses {
            if r < lens_rows {
                canvas.row(r, paint, &mut l.s);
                l.w = l.w.saturating_add(lens_cols);
            } else {
                l.pad(lens_cols);
            }
            l.pad(2);
        }
        if let Some(i) = info.next() {
            l.s.push_str(&i.s);
            l.w = l.w.saturating_add(i.w);
        }
        paint.reset(&mut l.s);
        frame.put(&l.s, l.w);
    }
    frame.put("", 0);
    let _ = frame.end(cols, out);
}

/// The card's lines: the brand and the command, the condition, the facts, what the
/// durability means.
fn card_lines(
    paint: &Paint,
    title: &str,
    card: &Card,
    t: f64,
    moving: bool,
    room: usize,
) -> Vec<Line> {
    let mut lines = Vec::new();
    lines.push(Line::default());
    let mut brand = Line::default();
    let letters = text::eyebrow("focal");
    let n = letters.chars().count().max(2).saturating_sub(1) as f64;
    brand.bold(paint, true);
    let mut glyph = [0u8; 4];
    for (k, ch) in letters.chars().enumerate() {
        // The brand runs the prism as the site's current moves it, while the lenses do.
        let c = if moving {
            tokens::prism_current(k as f64 / n, t, 9.0)
        } else {
            tokens::prism(k as f64 / n)
        };
        brand.put(paint, c, ch.encode_utf8(&mut glyph));
    }
    brand.bold(paint, false);
    brand
        .put(paint, tokens::SUBTLE, "  ·  ")
        .put(paint, tokens::EYEBROW, &text::eyebrow(title));
    lines.push(brand);
    lines.push(Line::default());
    let (dot, colour, said): (&str, Rgb, String) = if card.ready() {
        ("● ", tokens::SAGE, "Ready".into())
    } else if card.condition == "Stopped" {
        ("○ ", tokens::SUBTLE, "Stopped".into())
    } else {
        // Under way: the site's amber, breathing while the lenses find their focus.
        let k = if moving {
            motion::breath(t, 2.4, 0.0)
        } else {
            1.0
        };
        (
            "● ",
            tokens::mix(tokens::LINE, tokens::AMBER, 0.45 + 0.55 * k),
            {
                let still = !moving && !card.ready();
                if still {
                    format!("{} — still starting", card.condition)
                } else {
                    card.condition.clone()
                }
            },
        )
    };
    let mut status = Line::default();
    status
        .put(paint, colour, dot)
        .bold(paint, true)
        .put(paint, tokens::BRIGHT, &said)
        .bold(paint, false);
    lines.push(status);
    lines.push(Line::default());
    let label_w = card
        .rows
        .iter()
        .map(|(k, _)| k.len())
        .max()
        .unwrap_or(0)
        .saturating_add(2);
    for (label, value) in &card.rows {
        let mut l = Line::default();
        l.put(paint, tokens::EYEBROW, label);
        l.to(label_w);
        let shown = layout::clip(value, room.saturating_sub(label_w));
        l.put(paint, tokens::FOREGROUND, &shown);
        lines.push(l);
    }
    if let Some(meaning) = &card.meaning {
        lines.push(Line::default());
        for part in layout::wrap(meaning, room) {
            let mut l = Line::default();
            l.put(paint, tokens::BODY, &part);
            lines.push(l);
        }
    }
    if card.ready() {
        lines.push(Line::default());
        let mut hint = Line::default();
        hint.put(paint, tokens::FAINT, "+ ")
            .put(paint, tokens::SUBTLE, "Ctrl-C stops the node; ")
            .put(paint, tokens::TEAL, "focal")
            .put(
                paint,
                tokens::SUBTLE,
                " in another terminal lists what to do",
            );
        lines.push(hint);
    }
    lines
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_status_reads_as_a_card() {
        let value = serde_json::json!({
            "condition": "Ready",
            "ledger": {"tenant": [1, 2, 255], "session": [0, 16]},
            "node": 42,
            "socket": "/tmp/focal.sock",
            "admin_socket": null,
            "durability": {"survive": "node", "max_failures": 0},
            "meaning": "Acknowledged writes are synced to this disk."
        });
        let card = Card::from_json(&value);
        assert!(card.ready());
        assert!(card.rows.contains(&("node", "42".into())));
        assert!(card.rows.contains(&("tenant", "0102ff".into())));
        assert!(
            card.rows
                .contains(&("survives", "node loss, up to 0".into()))
        );
        assert!(!card.rows.iter().any(|(k, _)| *k == "admin"));
        let lines = card_lines(&Paint::new(true), "start node", &card, 1.0, false, 60);
        let text: String = lines.iter().map(|l| focal_tui::seen(&l.s) + "\n").collect();
        assert!(text.contains("F O C A L"), "{text}");
        assert!(text.contains("Ready"), "{text}");
        assert!(text.contains("/tmp/focal.sock"), "{text}");
    }

    #[test]
    fn a_frame_draws_at_any_width() {
        let card = Card::from_json(&serde_json::json!({"condition": "Starting"}));
        for t in [0.0, 1.5] {
            let mut frame = Frame::new();
            let mut canvas = Canvas::new(0, 0);
            let mut out = Vec::new();
            draw(
                &mut frame,
                &mut canvas,
                &Paint::new(true),
                "start node",
                &card,
                t,
                true,
                &mut out,
            );
            assert!(!out.is_empty());
        }
    }
}
