use super::InputError;
use serde::de::DeserializeOwned;
use std::fmt;

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
        InputFormat::Json => match super::shape::check(bytes, MAX_NODES, MAX_DEPTH) {
            Ok(()) => serde_json::from_slice(bytes).map_err(error),
            // The decoder names what is malformed precisely; a document it would take that the
            // first pass could not follow is refused, never decoded past its bounds.
            Err(super::shape::ShapeError::Syntax) => match serde_json::from_slice::<T>(bytes) {
                Err(precise) => Err(error(precise)),
                Ok(_) => Err(shape_error(super::shape::ShapeError::Syntax)),
            },
            Err(refused) => Err(shape_error(refused)),
        },
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
/// The first JSON pass's refusal, as the decoder names its own.
fn shape_error(error: super::shape::ShapeError) -> InputError {
    use super::shape::ShapeError;
    InputError::Decode(
        match error {
            ShapeError::Nodes => "node limit",
            ShapeError::Depth => "depth limit",
            ShapeError::DuplicateKey => "duplicate mapping key",
            ShapeError::Syntax => "malformed document",
        }
        .to_owned(),
    )
}
