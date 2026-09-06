use crate::*;

pub struct ValidationResultCandidate {
    pub position: ValidationResultPosition,
    pub bytes: usize,
}
fn start(validation: ValidationId) -> ValidationResultPosition {
    ValidationResultPosition {
        run: ValidationRunId {
            validation,
            target_hash: ContentHash([0; 32]),
            phase: ValidationPhase::Admission,
            epoch: 0,
        },
        attempt: None,
    }
}
fn end(validation: ValidationId) -> GraphKey {
    u128::from_be_bytes(validation.0).checked_add(1).map_or(
        GraphKey::ValidationResultsEnd,
        |value| {
            let id = ValidationId::from_u128(value);
            GraphKey::ValidationResult(id, start(id))
        },
    )
}
impl GraphSnapshot {
    /// Scan one fixed-size candidate without cloning a verdict's evidence list.
    /// The caller reserves response capacity before materializing the record.
    pub fn next_validation_result(
        &self,
        validation: ValidationId,
        after: Option<ValidationResultPosition>,
        now: u64,
    ) -> Result<Option<ValidationResultCandidate>, GraphError> {
        if validation.is_zero()
            || after.is_some_and(|position| position.run.validation != validation)
        {
            return Err(GraphError::Prefix);
        }
        let key =
            GraphKey::ValidationResult(validation, after.unwrap_or_else(|| start(validation)));
        self.lease()
            .project_next(&key, after.is_some(), &end(validation), now, |entry| {
                let (
                    GraphKey::ValidationResult(id, position),
                    GraphValue::ValidationResult(result),
                ) = (&entry.key, &entry.value)
                else {
                    return Err(GraphError::IndexMismatch);
                };
                if *id != validation || result.position != *position {
                    return Err(GraphError::IndexMismatch);
                }
                Ok(ValidationResultCandidate {
                    position: *position,
                    bytes: entry
                        .heap_bytes
                        .checked_add(size_of::<Entry<GraphKey, GraphValue>>())
                        .ok_or(GraphError::Overflow)?,
                })
            })?
            .transpose()
    }
    /// Borrow from this exact snapshot. No access to a live Core or current run
    /// registry can mix a later verdict into the pinned requirement's prefix.
    pub fn project_validation_result<R>(
        &self,
        position: ValidationResultPosition,
        now: u64,
        project: impl FnOnce(&ValidationResult, usize) -> R,
    ) -> Result<Option<R>, GraphError> {
        let validation = position.run.validation;
        if validation.is_zero() {
            return Err(GraphError::Prefix);
        }
        let key = GraphKey::ValidationResult(validation, position);
        self.lease()
            .project_next(&key, false, &end(validation), now, |entry| {
                let GraphValue::ValidationResult(result) = &entry.value else {
                    return Err(GraphError::IndexMismatch);
                };
                if entry.key != key {
                    return Ok(None);
                }
                if result.position != position {
                    return Err(GraphError::IndexMismatch);
                }
                let bytes = entry
                    .heap_bytes
                    .checked_add(size_of::<Entry<GraphKey, GraphValue>>())
                    .ok_or(GraphError::Overflow)?;
                Ok(Some(project(result, bytes)))
            })?
            .transpose()
            .map(Option::flatten)
    }
}
