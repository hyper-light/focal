//! The first pass over a JSON document, before anything is decoded from it: its values are
//! counted and its depth and duplicate keys checked without building a tree, by a scanner that
//! keeps the containers it is inside on an explicit stack (no recursion). The stack holds at most
//! `max_depth + 1` containers, since a value deeper than that is refused before it opens one, and
//! every key it remembers names a value the node bound counts.
//!
//! The scanner checks structure, not every byte: a number's digits and a string's escapes are the
//! decoder's to judge, as the second pass decodes the same bytes. Keys are decoded exactly, so two
//! spellings of one key (`"a"`, `"a"`) are the duplicate they are.

use std::collections::BTreeSet;

/// Why a document's shape was refused.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShapeError {
    /// More values than the bound.
    Nodes,
    /// A value deeper than the bound.
    Depth,
    /// An object names a key twice.
    DuplicateKey,
    /// Not a JSON document's structure.
    Syntax,
}

/// A container the scanner is inside, and what it expects next.
enum Open {
    /// An array: `true` once it holds a value, so a `,` or `]` comes next.
    Array(bool),
    /// An object: what comes next, and the keys it named.
    Object(Expect, BTreeSet<String>),
}

/// What an object expects next.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Expect {
    /// A key or `}`: the object was just opened.
    First,
    /// A key: a `,` came.
    Key,
    /// The `:` after a key.
    Colon,
    /// A `,` or `}`: a value came.
    Next,
}

/// Checks the document `bytes` holds at most `max_nodes` values, none deeper than `max_depth` (the
/// document itself at depth 0), and no object naming a key twice.
pub fn check(bytes: &[u8], max_nodes: usize, max_depth: usize) -> Result<(), ShapeError> {
    let mut at = 0usize;
    let mut left = max_nodes;
    let mut open: Vec<Open> = Vec::new();
    let mut value = true;
    loop {
        at = whitespace(bytes, at);
        if value {
            value = false;
            left = left.checked_sub(1).ok_or(ShapeError::Nodes)?;
            if open.len() > max_depth {
                return Err(ShapeError::Depth);
            }
            match bytes.get(at) {
                Some(b'{') => {
                    open.try_reserve(1).map_err(|_| ShapeError::Nodes)?;
                    open.push(Open::Object(Expect::First, BTreeSet::new()));
                    at = next(at)?;
                }
                Some(b'[') => {
                    open.try_reserve(1).map_err(|_| ShapeError::Nodes)?;
                    open.push(Open::Array(false));
                    at = next(at)?;
                }
                Some(b'"') => at = string_end(bytes, next(at)?)?,
                Some(b't') => at = literal(bytes, at, b"true")?,
                Some(b'f') => at = literal(bytes, at, b"false")?,
                Some(b'n') => at = literal(bytes, at, b"null")?,
                Some(b'-' | b'0'..=b'9') => at = number_end(bytes, at),
                _ => return Err(ShapeError::Syntax),
            }
            continue;
        }
        let Some(container) = open.last_mut() else {
            // The document's one value is done: nothing but whitespace may follow it.
            return if at == bytes.len() {
                Ok(())
            } else {
                Err(ShapeError::Syntax)
            };
        };
        let byte = *bytes.get(at).ok_or(ShapeError::Syntax)?;
        match container {
            Open::Array(holds) => match (byte, *holds) {
                (b']', _) => {
                    open.pop();
                    at = next(at)?;
                }
                (b',', true) => {
                    value = true;
                    at = next(at)?;
                }
                (_, false) => {
                    // The array's first value begins here.
                    *holds = true;
                    value = true;
                }
                _ => return Err(ShapeError::Syntax),
            },
            Open::Object(expect, keys) => match (*expect, byte) {
                (Expect::First, b'}') | (Expect::Next, b'}') => {
                    open.pop();
                    at = next(at)?;
                }
                (Expect::First | Expect::Key, b'"') => {
                    let (key, end) = key(bytes, next(at)?)?;
                    if !keys.insert(key) {
                        return Err(ShapeError::DuplicateKey);
                    }
                    *expect = Expect::Colon;
                    at = end;
                }
                (Expect::Colon, b':') => {
                    *expect = Expect::Next;
                    value = true;
                    at = next(at)?;
                }
                (Expect::Next, b',') => {
                    *expect = Expect::Key;
                    at = next(at)?;
                }
                _ => return Err(ShapeError::Syntax),
            },
        }
    }
}

fn next(at: usize) -> Result<usize, ShapeError> {
    at.checked_add(1).ok_or(ShapeError::Syntax)
}

/// The first index at or past `at` that is not JSON whitespace.
fn whitespace(bytes: &[u8], mut at: usize) -> usize {
    while let Some(b' ' | b'\t' | b'\n' | b'\r') = bytes.get(at) {
        at = at.saturating_add(1);
    }
    at
}

/// Just past `word`, which must begin at `at`.
fn literal(bytes: &[u8], at: usize, word: &[u8]) -> Result<usize, ShapeError> {
    let end = at.checked_add(word.len()).ok_or(ShapeError::Syntax)?;
    match bytes.get(at..end) {
        Some(found) if found == word => Ok(end),
        _ => Err(ShapeError::Syntax),
    }
}

/// Just past the number beginning at `at`: its characters, whose form the decoder judges.
fn number_end(bytes: &[u8], mut at: usize) -> usize {
    while let Some(b'-' | b'+' | b'.' | b'e' | b'E' | b'0'..=b'9') = bytes.get(at) {
        at = at.saturating_add(1);
    }
    at
}

/// Just past the closing quote of the string whose content begins at `at`; an escape's next
/// character is never a closing quote.
fn string_end(bytes: &[u8], mut at: usize) -> Result<usize, ShapeError> {
    loop {
        match bytes.get(at) {
            Some(b'"') => return next(at),
            Some(b'\\') => at = at.checked_add(2).ok_or(ShapeError::Syntax)?,
            Some(_) => at = next(at)?,
            None => return Err(ShapeError::Syntax),
        }
    }
}

/// The key whose content begins at `at`, decoded as the decoder decodes it, and the index just
/// past its closing quote.
fn key(bytes: &[u8], mut at: usize) -> Result<(String, usize), ShapeError> {
    let mut decoded: Vec<u8> = Vec::new();
    loop {
        let byte = *bytes.get(at).ok_or(ShapeError::Syntax)?;
        at = next(at)?;
        match byte {
            b'"' => {
                let key = String::from_utf8(decoded).map_err(|_| ShapeError::Syntax)?;
                return Ok((key, at));
            }
            b'\\' => {
                let escape = *bytes.get(at).ok_or(ShapeError::Syntax)?;
                at = next(at)?;
                let simple = match escape {
                    b'"' => Some(b'"'),
                    b'\\' => Some(b'\\'),
                    b'/' => Some(b'/'),
                    b'b' => Some(0x08),
                    b'f' => Some(0x0c),
                    b'n' => Some(b'\n'),
                    b'r' => Some(b'\r'),
                    b't' => Some(b'\t'),
                    b'u' => None,
                    _ => return Err(ShapeError::Syntax),
                };
                match simple {
                    Some(byte) => push(&mut decoded, &[byte])?,
                    None => {
                        let (character, end) = unicode_escape(bytes, at)?;
                        at = end;
                        let mut buffer = [0u8; 4];
                        push(&mut decoded, character.encode_utf8(&mut buffer).as_bytes())?;
                    }
                }
            }
            byte if byte < 0x20 => return Err(ShapeError::Syntax),
            byte => push(&mut decoded, &[byte])?,
        }
    }
}

fn push(decoded: &mut Vec<u8>, bytes: &[u8]) -> Result<(), ShapeError> {
    decoded
        .try_reserve(bytes.len())
        .map_err(|_| ShapeError::Nodes)?;
    decoded.extend_from_slice(bytes);
    Ok(())
}

/// The character a `\u` escape's hex digits at `at` name, a surrogate pair's two escapes joined,
/// and the index past them; a lone surrogate is refused, as the decoder refuses it.
fn unicode_escape(bytes: &[u8], at: usize) -> Result<(char, usize), ShapeError> {
    let (first, at) = hex4(bytes, at)?;
    if !(0xD800..=0xDBFF).contains(&first) {
        return char::from_u32(u32::from(first))
            .map(|character| (character, at))
            .ok_or(ShapeError::Syntax);
    }
    let after = literal(bytes, at, b"\\u")?;
    let (second, end) = hex4(bytes, after)?;
    if !(0xDC00..=0xDFFF).contains(&second) {
        return Err(ShapeError::Syntax);
    }
    let high = u32::from(first)
        .checked_sub(0xD800)
        .ok_or(ShapeError::Syntax)?;
    let low = u32::from(second)
        .checked_sub(0xDC00)
        .ok_or(ShapeError::Syntax)?;
    let code = high
        .checked_shl(10)
        .and_then(|high| high.checked_add(low))
        .and_then(|code| code.checked_add(0x1_0000))
        .ok_or(ShapeError::Syntax)?;
    char::from_u32(code)
        .map(|character| (character, end))
        .ok_or(ShapeError::Syntax)
}

/// Four hex digits at `at`, and the index past them.
fn hex4(bytes: &[u8], at: usize) -> Result<(u16, usize), ShapeError> {
    let end = at.checked_add(4).ok_or(ShapeError::Syntax)?;
    let digits = bytes.get(at..end).ok_or(ShapeError::Syntax)?;
    if !digits.iter().all(u8::is_ascii_hexdigit) {
        return Err(ShapeError::Syntax);
    }
    let text = std::str::from_utf8(digits).map_err(|_| ShapeError::Syntax)?;
    let value = u16::from_str_radix(text, 16).map_err(|_| ShapeError::Syntax)?;
    Ok((value, end))
}

#[cfg(test)]
#[path = "shape_tests.rs"]
mod tests;
