//! Immutable built-in payload contracts. Shape admission supplies no work verdict.
use crate::{TestReport, test_report_schema};
use focal_model::ContentHash;
use serde::{Deserialize, Deserializer, de};
use std::fmt;

/// These exact bytes, including the limits, define the immutable schema hash.
pub const ERROR_REPORT_SCHEMA: &[u8] = br#"focal.error_report.v1:{code:string[utf8_bytes=1..128,nonblank],message:string[utf8_bytes=1..4096,nonblank],details?:string[utf8_bytes=0..32768]|null};blank_codepoints=0009-000d,0020,0085,00a0,1680,2000-200a,2028-2029,202f,205f,3000;json_bytes<=65536;deny_unknown_fields;deny_duplicate_fields"#;
pub const ERROR_REPORT_MAX_BYTES: usize = 64 * 1024;
pub const TEST_REPORT_MAX_BYTES: usize = 1024 * 1024;

pub fn error_report_schema() -> ContentHash {
    ContentHash(*blake3::hash(ERROR_REPORT_SCHEMA).as_bytes())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum BuiltinSchemaError {
    #[error("evidence schema is not a supported built-in contract")]
    Unsupported,
    #[error("evidence exceeds its built-in schema byte limit")]
    Capacity,
    #[error("evidence does not match its built-in schema")]
    Invalid,
}

/// Determine the bound before reading referenced content or parsing JSON.
pub fn builtin_schema_limit(schema: ContentHash) -> Result<usize, BuiltinSchemaError> {
    if schema == test_report_schema() {
        Ok(TEST_REPORT_MAX_BYTES)
    } else if schema == error_report_schema() {
        Ok(ERROR_REPORT_MAX_BYTES)
    } else {
        Err(BuiltinSchemaError::Unsupported)
    }
}

/// Verify complete JSON and bounded fields. Existing test-report parsing and its
/// original hash are unchanged. Neither report type implies success or failure
/// of a claim; designated evaluators interpret evidence under declared policy.
pub fn verify_builtin_schema(schema: ContentHash, bytes: &[u8]) -> Result<(), BuiltinSchemaError> {
    if bytes.len() > builtin_schema_limit(schema)? {
        return Err(BuiltinSchemaError::Capacity);
    }
    if schema == test_report_schema() {
        serde_json::from_slice::<TestReport>(bytes)
            .map(|_| ())
            .map_err(|_| BuiltinSchemaError::Invalid)
    } else {
        // Serde structs also support positional sequences in some formats. This
        // new JSON contract requires the named object fields; retain the older
        // test-report decoder's full historical behavior in its branch above.
        if bytes
            .iter()
            .find(|&&byte| !matches!(byte, b' ' | b'\t' | b'\r' | b'\n'))
            != Some(&b'{')
        {
            return Err(BuiltinSchemaError::Invalid);
        }
        serde_json::from_slice::<ErrorReportShape>(bytes)
            .map(|_| ())
            .map_err(|_| BuiltinSchemaError::Invalid)
    }
}

// Retain no field strings. JSON unescaping may use the deserializer's temporary
// scratch storage, bounded above by ERROR_REPORT_MAX_BYTES before parsing.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ErrorReportShape {
    #[serde(rename = "code")]
    _code: CheckedText<128, true>,
    #[serde(rename = "message")]
    _message: CheckedText<4096, true>,
    #[serde(rename = "details", default)]
    _details: Option<CheckedText<32768, false>>,
}

struct CheckedText<const MAX: usize, const NONBLANK: bool>;
impl<'de, const MAX: usize, const NONBLANK: bool> Deserialize<'de> for CheckedText<MAX, NONBLANK> {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct TextVisitor<const MAX: usize, const NONBLANK: bool>;
        impl<const MAX: usize, const NONBLANK: bool> de::Visitor<'_> for TextVisitor<MAX, NONBLANK> {
            type Value = CheckedText<MAX, NONBLANK>;
            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(formatter, "a string of at most {MAX} UTF-8 bytes")
            }
            fn visit_str<E: de::Error>(self, value: &str) -> Result<Self::Value, E> {
                if value.len() > MAX || (NONBLANK && value.chars().all(blank)) {
                    return Err(E::custom("text exceeds its bound or is blank"));
                }
                Ok(CheckedText)
            }
        }
        deserializer.deserialize_str(TextVisitor::<MAX, NONBLANK>)
    }
}

// The schema fixes this set instead of inheriting future Unicode table changes.
fn blank(value: char) -> bool {
    matches!(value, '\u{0009}'..='\u{000d}' | '\u{0020}' | '\u{0085}' | '\u{00a0}'
        | '\u{1680}' | '\u{2000}'..='\u{200a}' | '\u{2028}'..='\u{2029}'
        | '\u{202f}' | '\u{205f}' | '\u{3000}')
}

#[cfg(test)]
#[path = "schema_tests.rs"]
mod tests;
