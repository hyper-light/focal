use crate::Limits;

/// A JSON document within `limits`: its shape checked first by focal-client's scanner (an
/// explicit stack, no recursion: values counted, depth and duplicate keys checked) before
/// anything is decoded from it.
pub(crate) fn parse(bytes: &[u8], limits: Limits) -> Result<serde_json::Value, ()> {
    focal_client::input::shape::check(bytes, limits.max_nodes, limits.max_depth).map_err(|_| ())?;
    serde_json::from_slice(bytes).map_err(|_| ())
}
