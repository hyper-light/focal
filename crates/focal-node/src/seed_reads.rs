//! Claim seeds use the existing committed association index at one pinned prefix.
use super::*;
use focal_graph::GraphScan;

pub(super) fn read(
    view: &GraphSnapshot,
    claims: &[ClaimId],
    after: Option<ObjectKey>,
    max_items: u32,
    now: u64,
    limits: &WireLimits,
) -> Result<(Vec<ReadObject>, Option<ObjectKey>), AccessError> {
    if claims.len() > 256
        || claims.iter().any(|id| id.is_zero())
        || claims.windows(2).any(|pair| matches!(pair,[a,b] if a>=b))
    {
        return Err(AccessError::InvalidRequest);
    }
    let mut last = after.map(|key| GraphKey::Object(key.kind, key.id));
    let mut objects = Vec::new();
    let mut visited = 0u32;
    let mut output_bytes = 0usize;
    let output_limit = (limits.max_frame_bytes as usize)
        .checked_sub(512)
        .ok_or(AccessError::Capacity)?;
    let heap_limit = (limits.max_frame_bytes as usize)
        // The graph's cached row charge is its serialized bound times 64.
        // Stream staging and SeedScan ingress reserve this same allowance,
        // retained through reply delivery even with a small negotiated frame.
        .checked_mul(64)
        .ok_or(AccessError::Capacity)?;
    let mut output_heap = 0usize;
    let mut visited_bytes = 0usize;
    let visit_limit = usize::try_from(limits.max_cost).map_err(|_| AccessError::Capacity)?;
    loop {
        let Some(candidate) = view
            .next_candidate(&GraphScan::Objects(None), last.as_ref(), now)
            .map_err(graph_error)?
        else {
            return Ok((objects, None));
        };
        if visited == max_items {
            return Ok((objects, key(last)?));
        }
        // Cached row cost bounds all residual work before association probes or
        // variable-size serialization. Resume before an unaffordable candidate.
        let next_bytes = visited_bytes
            .checked_add(candidate.bytes)
            .ok_or(AccessError::Capacity)?;
        if next_bytes > visit_limit {
            return if visited == 0 {
                Err(AccessError::Capacity)
            } else {
                Ok((objects, key(last)?))
            };
        }
        let reference = candidate.object.ok_or(AccessError::Unavailable)?;
        let mut selected = claims.is_empty();
        for claim in claims {
            if view
                .belongs_to_claim(reference, *claim, now)
                .map_err(graph_error)?
            {
                selected = true;
                break;
            }
        }
        if selected {
            let row = view
                .project_object(
                    reference,
                    now,
                    |object, heap| -> Result<Option<(ReadObject, usize)>, AccessError> {
                        if output_heap
                            .checked_add(heap)
                            .is_none_or(|sum| sum > heap_limit)
                        {
                            return Ok(None);
                        }
                        let bytes = postcard::experimental::serialized_size(object)
                            .map_err(|_| AccessError::Capacity)?
                            .checked_add(128)
                            .ok_or(AccessError::Capacity)?;
                        if output_bytes
                            .checked_add(bytes)
                            .is_none_or(|sum| sum > output_limit)
                        {
                            return Ok(None);
                        }
                        objects.try_reserve(1).map_err(|_| AccessError::Capacity)?;
                        Ok(Some((
                            convert(reference.kind, reference.id, object)?,
                            bytes,
                        )))
                    },
                )
                .map_err(graph_error)?
                .ok_or(AccessError::Unavailable)??;
            let Some((object, bytes)) = row else {
                return if visited == 0 {
                    Err(AccessError::Capacity)
                } else {
                    Ok((objects, key(last)?))
                };
            };
            output_bytes = output_bytes
                .checked_add(bytes)
                .ok_or(AccessError::Capacity)?;
            output_heap = output_heap
                .checked_add(candidate.bytes)
                .ok_or(AccessError::Capacity)?;
            objects.push(object);
        }
        visited_bytes = next_bytes;
        visited = visited.checked_add(1).ok_or(AccessError::Capacity)?;
        last = Some(candidate.key);
    }
}
fn key(value: Option<GraphKey>) -> Result<Option<ObjectKey>, AccessError> {
    match value {
        Some(GraphKey::Object(kind, id)) => Ok(Some(ObjectKey { kind, id })),
        None => Ok(None),
        _ => Err(AccessError::Unavailable),
    }
}
