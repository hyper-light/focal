//! Errors, in a hairline panel: rounded corners (the site's 7px radius), a rose eyebrow
//! (the prism's rose; the site has no red), the message wrapped to the width there is,
//! and hints after the site's tiny cross.

use crate::layout;
use crate::text;
use crate::tokens::{self, Paint};

/// The panel's lines for `width` columns: each its text with colours, and its visible
/// width. Narrow terminals get a panel without its border, its text still wrapped.
pub fn error(
    paint: &Paint,
    width: usize,
    title: &str,
    message: &str,
    hints: &[&str],
) -> Vec<(String, usize)> {
    let mut lines = Vec::new();
    let boxed = width >= 30;
    let inner = if boxed {
        width.min(92).saturating_sub(6)
    } else {
        width.saturating_sub(2).max(10)
    };
    let heading = if title.is_empty() {
        text::eyebrow("error")
    } else {
        format!("{}  {}", text::eyebrow("error"), title)
    };
    let edge = |s: &mut String| {
        if boxed {
            paint.fg(s, tokens::EDGE);
        }
    };
    let full = inner.saturating_add(6);
    if boxed {
        let mut s = String::from("  ");
        edge(&mut s);
        s.push('╭');
        s.push_str(&"─".repeat(inner.saturating_add(2)));
        s.push('╮');
        lines.push((s, full));
    }
    let mut row = |content: &dyn Fn(&mut String), shown: usize| {
        let mut s = String::from("  ");
        if boxed {
            edge(&mut s);
            s.push_str("│ ");
        }
        content(&mut s);
        if boxed {
            s.push_str(&" ".repeat(inner.saturating_sub(shown)));
            edge(&mut s);
            s.push_str(" │");
        }
        lines.push((s, if boxed { full } else { shown.saturating_add(2) }));
    };
    for (i, part) in layout::wrap(&heading, inner).iter().enumerate() {
        let n = part.chars().count();
        row(
            &|s: &mut String| {
                paint.fg(s, tokens::ROSE);
                paint.bold(s, i == 0);
                s.push_str(part);
                paint.bold(s, false);
            },
            n,
        );
    }
    for part in layout::wrap(message, inner) {
        let n = part.chars().count();
        row(
            &|s: &mut String| {
                paint.fg(s, tokens::FOREGROUND);
                s.push_str(&part);
            },
            n,
        );
    }
    for hint in hints {
        for (i, part) in layout::wrap(hint, inner.saturating_sub(2))
            .iter()
            .enumerate()
        {
            let n = part.chars().count().saturating_add(2);
            row(
                &|s: &mut String| {
                    paint.fg(s, tokens::FAINT);
                    s.push_str(if i == 0 { "+ " } else { "  " });
                    paint.fg(s, tokens::SUBTLE);
                    s.push_str(part);
                },
                n,
            );
        }
    }
    if boxed {
        let mut s = String::from("  ");
        edge(&mut s);
        s.push('╰');
        s.push_str(&"─".repeat(inner.saturating_add(2)));
        s.push('╯');
        lines.push((s, full));
    }
    lines
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_panel_fits_every_width_and_keeps_every_word() {
        let paint = Paint::new(true);
        let message = "the node at /var/lib/focal answered unavailable: no leader yet";
        for width in [12usize, 29, 30, 50, 80, 200] {
            let lines = error(&paint, width, "post claim", message, &["retry it"]);
            let mut words = String::new();
            for (s, w) in &lines {
                let text = crate::seen(s);
                assert_eq!(text.chars().count(), *w, "{width}: {text:?}");
                assert!(*w <= width.max(12), "{width}: {text:?}");
                words.push_str(&text);
                words.push(' ');
            }
            for word in message.split(' ') {
                let piece: String = word.chars().take(8).collect();
                assert!(words.contains(&piece), "{width}: {piece} in {words}");
            }
        }
    }
}
