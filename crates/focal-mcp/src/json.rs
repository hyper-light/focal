use crate::Limits;

/// A JSON document within `limits`: its shape checked first by focal-client's scanner (an
/// explicit stack, no recursion: values counted, depth and duplicate keys checked) before
/// anything is decoded from it.
pub(crate) fn parse(bytes: &[u8], limits: Limits) -> Result<serde_json::Value, ()> {
    parse_counted(bytes, limits).map(|(value, _)| value)
}

/// [`parse`], with how many values the document holds: what its decoded tree is charged by.
pub(crate) fn parse_counted(
    bytes: &[u8],
    limits: Limits,
) -> Result<(serde_json::Value, usize), ()> {
    let nodes = focal_client::input::shape::count(bytes, limits.max_nodes, limits.max_depth)
        .map_err(|_| ())?;
    Ok((serde_json::from_slice(bytes).map_err(|_| ())?, nodes))
}
