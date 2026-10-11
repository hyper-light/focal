//! The shape scanner against the serde visitor it replaced, kept here as the oracle: on every
//! well-formed document the two refuse for the same reason or accept alike.
use super::*;
use serde::de::{self, DeserializeSeed, MapAccess, SeqAccess, Visitor};
use std::fmt;

/// The serde first pass the scanner replaced (`decode.rs` before 2026-10-10), its bounds made
/// parameters.
struct Oracle<'a> {
    remaining: &'a mut usize,
    depth: usize,
    max_depth: usize,
}
impl<'de> DeserializeSeed<'de> for Oracle<'_> {
    type Value = ();
    fn deserialize<D: de::Deserializer<'de>>(self, deserializer: D) -> Result<(), D::Error> {
        *self.remaining = self
            .remaining
            .checked_sub(1)
            .ok_or_else(|| de::Error::custom("node limit"))?;
        if self.depth > self.max_depth {
            return Err(de::Error::custom("depth limit"));
        }
        deserializer.deserialize_any(self)
    }
}
impl<'de> Visitor<'de> for Oracle<'_> {
    type Value = ();
    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("bounded document")
    }
    fn visit_bool<E: de::Error>(self, _: bool) -> Result<(), E> {
        Ok(())
    }
    fn visit_i64<E: de::Error>(self, _: i64) -> Result<(), E> {
        Ok(())
    }
    fn visit_u64<E: de::Error>(self, _: u64) -> Result<(), E> {
        Ok(())
    }
    fn visit_f64<E: de::Error>(self, _: f64) -> Result<(), E> {
        Ok(())
    }
    fn visit_str<E: de::Error>(self, _: &str) -> Result<(), E> {
        Ok(())
    }
    fn visit_unit<E: de::Error>(self) -> Result<(), E> {
        Ok(())
    }
    fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<(), A::Error> {
        let depth = self.depth + 1;
        while seq
            .next_element_seed(Oracle {
                remaining: self.remaining,
                depth,
                max_depth: self.max_depth,
            })?
            .is_some()
        {}
        Ok(())
    }
    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<(), A::Error> {
        let depth = self.depth + 1;
        let mut keys = BTreeSet::new();
        while let Some(key) = map.next_key::<String>()? {
            if !keys.insert(key) {
                return Err(de::Error::custom("duplicate mapping key"));
            }
            map.next_value_seed(Oracle {
                remaining: self.remaining,
                depth,
                max_depth: self.max_depth,
            })?;
        }
        Ok(())
    }
}

fn oracle(bytes: &[u8], max_nodes: usize, max_depth: usize) -> Result<(), ShapeError> {
    let mut parser = serde_json::Deserializer::from_slice(bytes);
    let mut remaining = max_nodes;
    let checked = Oracle {
        remaining: &mut remaining,
        depth: 0,
        max_depth,
    }
    .deserialize(&mut parser)
    .and_then(|()| parser.end());
    checked.map_err(|error| {
        let text = error.to_string();
        if text.starts_with("node limit") {
            ShapeError::Nodes
        } else if text.starts_with("depth limit") {
            ShapeError::Depth
        } else if text.starts_with("duplicate mapping key") {
            ShapeError::DuplicateKey
        } else {
            ShapeError::Syntax
        }
    })
}

/// A small deterministic source of choices (xorshift64*).
struct Choices(u64);
impl Choices {
    fn next(&mut self, below: u64) -> u64 {
        self.0 ^= self.0 >> 12;
        self.0 ^= self.0 << 25;
        self.0 ^= self.0 >> 27;
        self.0.wrapping_mul(0x2545_F491_4F6C_DD1D) % below
    }
}

const KEYS: &[&str] = &[
    "a",
    "\\u0061",
    "b",
    "\\\"q\\\"",
    "\\ud83d\\ude00",
    "\u{1F600}",
    "[{",
    "\\\\",
    "\\n",
    "\\u000a",
];
const STRINGS: &[&str] = &["", "[[[", "]]}", "\\\"", "\\\\", "\\u0041", "x y", "\u{e9}"];
const NUMBERS: &[&str] = &["0", "-1", "3.5e10", "12", "-0.25E-3"];
const SPACES: &[&str] = &["", " ", "\n", "\t ", "\r\n"];

/// A well-formed document: containers nest at most `levels` deep, keys repeat at times.
fn document(choices: &mut Choices, levels: usize, out: &mut String) {
    let space = |choices: &mut Choices, out: &mut String| {
        out.push_str(SPACES[choices.next(SPACES.len() as u64) as usize]);
    };
    let kind = if levels == 0 {
        choices.next(3) + 2
    } else {
        choices.next(5)
    };
    match kind {
        0 => {
            out.push('{');
            let entries = choices.next(4);
            for entry in 0..entries {
                if entry > 0 {
                    out.push(',');
                }
                space(choices, out);
                out.push('"');
                out.push_str(KEYS[choices.next(KEYS.len() as u64) as usize]);
                out.push('"');
                space(choices, out);
                out.push(':');
                space(choices, out);
                document(choices, levels - 1, out);
                space(choices, out);
            }
            out.push('}');
        }
        1 => {
            out.push('[');
            let entries = choices.next(4);
            for entry in 0..entries {
                if entry > 0 {
                    out.push(',');
                }
                space(choices, out);
                document(choices, levels - 1, out);
                space(choices, out);
            }
            out.push(']');
        }
        2 => {
            out.push('"');
            out.push_str(STRINGS[choices.next(STRINGS.len() as u64) as usize]);
            out.push('"');
        }
        3 => out.push_str(NUMBERS[choices.next(NUMBERS.len() as u64) as usize]),
        _ => out.push_str(["true", "false", "null"][choices.next(3) as usize]),
    }
}

#[test]
fn the_scanner_refuses_for_the_reason_the_serde_pass_did_on_every_generated_document() {
    let mut checked = [0usize; 5];
    for seed in 1..6000u64 {
        let mut choices = Choices(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1);
        let mut text = String::new();
        let levels = choices.next(9) as usize;
        document(&mut choices, levels, &mut text);
        for (max_nodes, max_depth) in [(1000, 16), (12, 16), (1000, 3), (6, 2), (1, 0)] {
            let expected = oracle(text.as_bytes(), max_nodes, max_depth);
            let found = check(text.as_bytes(), max_nodes, max_depth);
            assert_eq!(
                found, expected,
                "{text:?} at {max_nodes} nodes, {max_depth} deep"
            );
            let class = match found {
                Ok(()) => 0,
                Err(ShapeError::Nodes) => 1,
                Err(ShapeError::Depth) => 2,
                Err(ShapeError::DuplicateKey) => 3,
                Err(ShapeError::Syntax) => 4,
            };
            checked[class] += 1;
        }
    }
    // Every refusal the bounds make was reached, and well-formed documents are never malformed.
    assert!(
        checked[0] > 0 && checked[1] > 0 && checked[2] > 0 && checked[3] > 0,
        "{checked:?}"
    );
    assert_eq!(checked[4], 0, "{checked:?}");
}

#[test]
fn keys_are_compared_as_decoded_and_strings_hide_their_brackets() {
    assert_eq!(
        check(br#"{"a":1,"\u0061":2}"#, 100, 4),
        Err(ShapeError::DuplicateKey)
    );
    assert_eq!(
        check("{\"\\ud83d\\ude00\":1,\"\u{1F600}\":2}".as_bytes(), 100, 4),
        Err(ShapeError::DuplicateKey)
    );
    assert_eq!(check(br#"["[[[[", "]]", "{\"x\":"]"#, 100, 1), Ok(()));
    assert_eq!(check(br#"{"a":{"a":1}}"#, 100, 4), Ok(()));
    assert_eq!(check(br#"[[[]]]"#, 100, 2), Ok(()));
    assert_eq!(check(br#"[[[[]]]]"#, 100, 2), Err(ShapeError::Depth));
    assert_eq!(check(br#"[1,2,3]"#, 3, 4), Err(ShapeError::Nodes));
    assert_eq!(check(br#"[1,2,3]"#, 4, 4), Ok(()));
}

#[test]
fn malformed_structure_is_refused() {
    for text in [
        &b"{} x"[..],
        b"[1,]",
        b"{\"a\":1,}",
        b"[,1]",
        b"{\"a\" 1}",
        b"{1:2}",
        b"[1 2]",
        b"\"open",
        b"tru",
        b"{\"\\ud83d\":1}",
        b"{\"\\u+0041\":1}",
        b"",
    ] {
        assert_eq!(check(text, 100, 8), Err(ShapeError::Syntax), "{text:?}");
    }
}
