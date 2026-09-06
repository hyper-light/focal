use super::*;

pub(super) fn read(
    view: &GraphSnapshot,
    id: ValidationId,
    after: Option<ValidationResultPosition>,
    max_items: u32,
    now: u64,
    limits: &WireLimits,
) -> Result<Option<ReadObject>, AccessError> {
    if id.is_zero()
        || max_items == 0
        || max_items > limits.max_items
        || after.is_some_and(|position| position.run.validation != id)
    {
        return Err(AccessError::InvalidRequest);
    }
    let reference = ObjectRef {
        ledger: view.ledger(),
        kind: ObjectKind::Validation,
        id: ObjectId(id.0),
    };
    let mut bytes = 512usize;
    let mut visited_bytes = 0u64;
    let requirement = view
        .project_object(reference, now, |object, heap_bytes| {
            visited_bytes = u64::try_from(heap_bytes).map_err(|_| AccessError::Capacity)?;
            if visited_bytes > limits.max_cost {
                return Err(AccessError::Capacity);
            }
            let GraphObject::Validation(value) = object else {
                return Err(AccessError::Unavailable);
            };
            bytes = bytes
                .checked_add(
                    postcard::experimental::serialized_size(value)
                        .map_err(|_| AccessError::Capacity)?,
                )
                .ok_or(AccessError::Capacity)?;
            if bytes > limits.max_frame_bytes as usize {
                return Err(AccessError::Capacity);
            }
            Ok(value.clone())
        })
        .map_err(graph_error)?
        .transpose()?;
    let Some(value) = requirement else {
        return Ok(None);
    };
    if after.is_some_and(|position| position.run.epoch > value.lifecycle().latest_epoch) {
        return Err(AccessError::InvalidRequest);
    }
    // A cursor names an existing row in this principal's exact pinned snapshot.
    if let Some(position) = after
        && view
            .project_validation_result(position, now, |_, _| ())
            .map_err(graph_error)?
            .is_none()
    {
        return Err(AccessError::InvalidRequest);
    }
    let mut records = Vec::new();
    let mut cursor = after;
    let mut next = None;
    while let Some(candidate) = view
        .next_validation_result(id, cursor, now)
        .map_err(graph_error)?
    {
        if records.len() >= max_items as usize {
            next = cursor;
            break;
        }
        let total_cost = visited_bytes
            .checked_add(u64::try_from(candidate.bytes).map_err(|_| AccessError::Capacity)?)
            .ok_or(AccessError::Capacity)?;
        if total_cost > limits.max_cost {
            if records.is_empty() {
                return Err(AccessError::Capacity);
            }
            next = cursor;
            break;
        }
        visited_bytes = total_cost;
        let record = view
            .project_validation_result(candidate.position, now, |record, _| {
                let size = postcard::experimental::serialized_size(record)
                    .map_err(|_| AccessError::Capacity)?;
                let total = bytes.checked_add(size).ok_or(AccessError::Capacity)?;
                if total > limits.max_frame_bytes as usize {
                    return Ok(None);
                }
                bytes = total;
                records
                    .try_reserve_exact(1)
                    .map_err(|_| AccessError::Capacity)?;
                Ok(Some(record.clone()))
            })
            .map_err(graph_error)?
            .ok_or(AccessError::Unavailable)??;
        let Some(record) = record else {
            if records.is_empty() {
                return Err(AccessError::Capacity);
            }
            next = cursor;
            break;
        };
        cursor = Some(record.position);
        records.push(record);
    }
    Ok(Some(ReadObject::ValidationResults {
        id,
        value,
        records,
        next,
    }))
}

#[cfg(test)]
#[path = "validation_reads_tests.rs"]
mod tests;
