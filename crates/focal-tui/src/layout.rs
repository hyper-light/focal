//! Text that fits its width by wrapping at words, never by being cut, except as a last
//! resort that says so.

/// `s` in at most `width` characters, its end replaced by `…` if it must be cut.
pub fn clip(s: &str, width: usize) -> String {
    if s.chars().count() <= width {
        return s.to_string();
    }
    let mut out: String = s.chars().take(width.saturating_sub(1)).collect();
    if width > 0 {
        out.push('…');
    }
    out
}

/// `text` wrapped at word boundaries into lines of at most `width` characters; a word
/// longer than a line is broken.
pub fn wrap(text: &str, width: usize) -> Vec<String> {
    let width = width.max(1);
    let mut lines = Vec::new();
    for para in text.split('\n') {
        let mut line = String::new();
        let mut have = 0usize;
        for word in para.split(' ') {
            let mut rest: &str = word;
            // Each turn either places the rest of the word, ends a line, or takes a
            // whole line's worth of a long word: bounded by the word's length.
            loop {
                let need = rest.chars().count().saturating_add(usize::from(have > 0));
                if have.saturating_add(need) <= width {
                    if have > 0 {
                        line.push(' ');
                    }
                    line.push_str(rest);
                    have = have.saturating_add(need);
                    break;
                }
                if have > 0 {
                    lines.push(std::mem::take(&mut line));
                    have = 0;
                    continue;
                }
                // A word longer than a line: as much as fits, the rest on the next.
                let cut = rest
                    .char_indices()
                    .nth(width)
                    .map_or(rest.len(), |(at, _)| at);
                let (head, tail) = rest.split_at(cut);
                lines.push(head.to_string());
                rest = tail;
                if rest.is_empty() {
                    break;
                }
            }
        }
        lines.push(line);
    }
    lines
}

/// The visible width of `s`: its characters, without its escape sequences.
pub fn visible(s: &str) -> usize {
    let mut n = 0usize;
    let mut esc = false;
    for ch in s.chars() {
        match (esc, ch) {
            (false, '\x1b') => esc = true,
            (true, c) if c.is_ascii_alphabetic() => esc = false,
            (true, _) => {}
            (false, _) => n = n.saturating_add(1),
        }
    }
    n
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn text_wraps_at_words_and_breaks_long_ones() {
        assert_eq!(wrap("one two three", 7), ["one two", "three"]);
        assert_eq!(wrap("abcdefghij", 4), ["abcd", "efgh", "ij"]);
        assert_eq!(wrap("a\nb", 10), ["a", "b"]);
        assert_eq!(wrap("", 10), [""]);
        assert_eq!(clip("focal ledger", 6), "focal…");
        assert_eq!(clip("focal", 6), "focal");
        assert_eq!(visible("\x1b[38;2;1;2;3mab\x1b[0mc"), 3);
    }
}
