use super::super::read_source::{Meter, Span};
use super::*;
use crate::native::creation_result::{
    NativeCreatedFamily, NativeCreatedObject, NativeCreationResult, OwnedCreationResult,
};
use focal_model::{ContentHash, ObjectId};

pub(in crate::native::record_codec) struct CreationInput<'a> {
    entries: Span<'a>,
    meter: Meter,
}
pub(in crate::native::record_codec) struct CreationPlan<'s, 'a> {
    input: &'s CreationInput<'a>,
    quote: Quote,
    check_visits: usize,
}
pub(in crate::native::record_codec) fn creation<'a>(
    cursor: &mut Cursor<'a>,
    max_source_visits: usize,
) -> Result<CreationInput<'a>, NativeError> {
    Ok(CreationInput {
        entries: Span::read_fixed(cursor, 71).map_err(codec)?,
        meter: Meter::new(max_source_visits),
    })
}
/// The most a decode of a `len`-byte creation result's record encoding takes on the heap: its
/// entries, fewer than its bytes over their 71 each.
pub(in crate::native) fn creation_decode_bound(len: usize) -> Result<usize, MemoryError> {
    let entries = len
        .checked_div(71)
        .and_then(|entries| entries.checked_add(1))
        .ok_or(MemoryError::CounterExhausted("creation result decode"))?;
    NativeCreationResult::construction_heap(entries)
}
/// The creation result whose record encoding `bytes` are (a spilled row's, doc 31 M3), checked as
/// a replayed one is.
pub(in crate::native) fn decode_creation(
    bytes: &[u8],
) -> Result<NativeCreationResult, NativeError> {
    let visits = bytes
        .len()
        .checked_mul(256)
        .and_then(|n| n.checked_add(4096))
        .ok_or(NativeError::Capacity("creation result decode"))?;
    let mut cursor = Cursor::new(bytes, bytes.len(), visits).map_err(codec)?;
    let input = creation(&mut cursor, visits)?;
    cursor.finish().map_err(codec)?;
    let plan = input.prepare(input.entries.count, visits)?;
    let quote = plan.quote();
    match plan.build(quote.heap_bytes, quote.model_build_visits)? {
        Row::CreationResult(OwnedCreationResult::Held(result)) => Ok(result),
        _ => Err(ContractError::InvalidManifest.into()),
    }
}
fn entry(cursor: &mut Cursor<'_>) -> Result<NativeCreatedObject, Error> {
    Ok(NativeCreatedObject {
        ordinal: cursor.u32()?,
        family: match cursor.u8()? {
            0 => NativeCreatedFamily::Claim,
            1 => NativeCreatedFamily::Validation,
            _ => return Err(Error::InvalidTag("created family")),
        },
        schema: cursor.u16()?,
        content: ContentHash(cursor.fixed()?),
        requested: ObjectId(cursor.fixed()?),
        resolved: ObjectId(cursor.fixed()?),
    })
}
impl<'a> CreationInput<'a> {
    fn at(&self, index: usize) -> Result<NativeCreatedObject, NativeError> {
        self.meter.charge(4).map_err(codec)?;
        if index >= self.entries.count {
            return Err(ContractError::InvalidManifest.into());
        }
        let start = mul(index, 71)?;
        let bytes = self
            .entries
            .bytes
            .get(start..add(start, 71)?)
            .ok_or(ContractError::InvalidManifest)?;
        let (value, used) = self.meter.read(bytes, entry).map_err(codec)?;
        if used != 71 {
            return Err(ContractError::InvalidManifest.into());
        }
        Ok(value)
    }
    pub(in crate::native::record_codec) fn prepare(
        &self,
        max_objects: usize,
        max_model_visits: usize,
    ) -> Result<CreationPlan<'_, 'a>, NativeError> {
        let count = self.entries.count;
        if count == 0 || count > max_objects {
            return Err(NativeError::Capacity("creation result objects"));
        }
        let checks = NativeCreationResult::inspection_visits(count)?;
        // Six scalar fields cost 77 primitive units; each indexed lookup adds
        // four checked indexing units. Both raw passes share this same meter.
        let source_inspection = mul(checks, 81)?;
        let source_build = mul(count, 81)?;
        fits(
            add(source_inspection, source_build)?,
            self.meter.remaining(),
        )?;
        fits(checks, max_model_visits)?;
        let before = self.meter.remaining();
        for index in 0..count {
            let value = self.at(index)?;
            NativeCreationResult::check_recorded_entry(value, index)?;
            for previous in 0..index {
                if NativeCreationResult::recorded_entries_conflict(self.at(previous)?, value) {
                    return Err(ContractError::ContentConflict.into());
                }
            }
        }
        if before.checked_sub(self.meter.remaining()) != Some(source_inspection) {
            return Err(NativeError::Capacity("creation source quote"));
        }
        let quote = Quote {
            heap_bytes: NativeCreationResult::construction_heap(count)?,
            allocations: 1,
            model_inspection_visits: checks,
            model_build_visits: add(add(checks, count)?, 1)?,
            source_inspection_visits: source_inspection,
            source_build_visits: source_build,
        };
        Ok(CreationPlan {
            input: self,
            quote,
            check_visits: checks,
        })
    }
}
impl CreationPlan<'_, '_> {
    pub(in crate::native::record_codec) fn quote(&self) -> Quote {
        self.quote
    }
    pub(in crate::native::record_codec) fn build(
        self,
        max_bytes: usize,
        max_model_visits: usize,
    ) -> Result<Row, NativeError> {
        fits(self.quote.heap_bytes, max_bytes)?;
        fits(self.quote.model_build_visits, max_model_visits)?;
        fits(self.quote.source_build_visits, self.input.meter.remaining())?;
        let before = self.input.meter.remaining();
        let mut values = Vec::new();
        values
            .try_reserve_exact(self.input.entries.count)
            .map_err(|_| MemoryError::AllocationFailed)?;
        fits(
            NativeCreationResult::construction_heap(values.capacity())?,
            self.quote.heap_bytes,
        )?;
        for index in 0..self.input.entries.count {
            values.push(self.input.at(index)?);
        }
        if before.checked_sub(self.input.meter.remaining()) != Some(self.quote.source_build_visits)
        {
            return Err(NativeError::Capacity("creation source quote"));
        }
        let value = NativeCreationResult::from_owned(
            values,
            self.input.entries.count,
            self.check_visits,
            self.quote.heap_bytes,
        )?;
        let owned = OwnedCreationResult::new(value)?;
        let actual = owned.heap_charge()?;
        finish(Row::CreationResult(owned), actual, self.quote, max_bytes)
    }
}
