use super::InputError;
use serde::de::{self, DeserializeOwned, DeserializeSeed, MapAccess, SeqAccess, Visitor};
use std::{collections::BTreeSet, fmt};

pub const MAX_INPUT_BYTES: usize = 256 * 1024;
const MAX_NODES: usize = 8192;
const MAX_DEPTH: usize = 16;
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InputFormat {
    Json,
    Yaml,
}

pub fn parse_document<T: DeserializeOwned>(
    bytes: &[u8],
    format: InputFormat,
) -> Result<T, InputError> {
    if bytes.is_empty() || bytes.len() > MAX_INPUT_BYTES {
        return Err(InputError::Capacity);
    }
    match format {
        InputFormat::Json => {
            let mut parser = serde_json::Deserializer::from_slice(bytes);
            let mut remaining = MAX_NODES;
            Shape {
                remaining: &mut remaining,
                depth: 0,
            }
            .deserialize(&mut parser)
            .map_err(error)?;
            parser.end().map_err(error)?;
            serde_json::from_slice(bytes).map_err(error)
        }
        InputFormat::Yaml => {
            let text = std::str::from_utf8(bytes).map_err(|_| InputError::Invalid("UTF-8"))?;
            let options = serde_saphyr::options! {
                budget: serde_saphyr::budget! {max_depth:16,max_events:16384,max_nodes:8192,max_total_scalar_bytes:MAX_INPUT_BYTES,max_aliases:0,max_anchors:0,max_documents:1,max_merge_keys:0},
                duplicate_keys: serde_saphyr::options::DuplicateKeyPolicy::Error,
                merge_keys: serde_saphyr::options::MergeKeyPolicy::Error,
                reject_unsupported_tags: true,
            };
            serde_saphyr::from_str_with_options(text, options).map_err(error)
        }
    }
}
fn error(value: impl fmt::Display) -> InputError {
    InputError::Decode(value.to_string().chars().take(512).collect())
}
/// First JSON pass validates structure and duplicates without constructing a
/// recursive Value tree. DTO decoding only occurs after depth/node admission.
struct Shape<'a> {
    remaining: &'a mut usize,
    depth: usize,
}
impl<'de> DeserializeSeed<'de> for Shape<'_> {
    type Value = ();
    fn deserialize<D: de::Deserializer<'de>>(self, deserializer: D) -> Result<(), D::Error> {
        *self.remaining = self
            .remaining
            .checked_sub(1)
            .ok_or_else(|| de::Error::custom("node limit"))?;
        if self.depth > MAX_DEPTH {
            return Err(de::Error::custom("depth limit"));
        }
        deserializer.deserialize_any(self)
    }
}
impl<'de> Visitor<'de> for Shape<'_> {
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
        let depth = self
            .depth
            .checked_add(1)
            .ok_or_else(|| de::Error::custom("depth limit"))?;
        while seq
            .next_element_seed(Shape {
                remaining: self.remaining,
                depth,
            })?
            .is_some()
        {}
        Ok(())
    }
    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<(), A::Error> {
        let depth = self
            .depth
            .checked_add(1)
            .ok_or_else(|| de::Error::custom("depth limit"))?;
        let mut keys = BTreeSet::new();
        while let Some(key) = map.next_key::<String>()? {
            if !keys.insert(key) {
                return Err(de::Error::custom("duplicate mapping key"));
            }
            map.next_value_seed(Shape {
                remaining: self.remaining,
                depth,
            })?;
        }
        Ok(())
    }
}
