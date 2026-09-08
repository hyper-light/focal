//! Complete dormant ProjectionOnly creation decoding. Local preparation checks
//! all authored projections and definitions without allocating scratch rows.
//! Effective references, authorization and durable publication remain owner work.
use super::bytes::Cursor;
use super::creation_content::DeclarationBodyInput;
use super::source_bytes::{Meter, SourceCursor, Span, Values, model_error};
use super::*;
use focal_model::lifecycle::{aggregation, claim::ClaimDefinition, creation, graph, succession};
use focal_model::{Cause, ObjectId, ObjectKind, ObjectRef, ObjectRevision, RootCommandId};

#[path = "legacy_creation_cohort.rs"]
mod cohort;
#[path = "legacy_creation_fields.rs"]
mod fields;
#[path = "legacy_creation_plan.rs"]
mod plan;
#[cfg(test)]
#[path = "legacy_creation_tests.rs"]
mod tests;

/// Independent cumulative domains avoid a nested declaration check renewing
/// the acceptance algorithm's allowance. These are internal ingress limits.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LegacyCreationWork {
    pub parsing: usize,
    pub source: usize,
    pub declarations: usize,
    pub acceptance: usize,
    pub structure: usize,
}
impl LegacyCreationWork {
    fn zero() -> Self {
        Self {
            parsing: 0,
            source: 0,
            declarations: 0,
            acceptance: 0,
            structure: 0,
        }
    }
    fn add(self, other: Self) -> Result<Self, CodecError> {
        Ok(Self {
            parsing: sum(self.parsing, other.parsing)?,
            source: sum(self.source, other.source)?,
            declarations: sum(self.declarations, other.declarations)?,
            acceptance: sum(self.acceptance, other.acceptance)?,
            structure: sum(self.structure, other.structure)?,
        })
    }
    fn subtract(self, other: Self) -> Result<Self, CodecError> {
        Ok(Self {
            parsing: difference(self.parsing, other.parsing)?,
            source: difference(self.source, other.source)?,
            declarations: difference(self.declarations, other.declarations)?,
            acceptance: difference(self.acceptance, other.acceptance)?,
            structure: difference(self.structure, other.structure)?,
        })
    }
    fn multiply(self, count: usize) -> Result<Self, CodecError> {
        Ok(Self {
            parsing: product(self.parsing, count)?,
            source: product(self.source, count)?,
            declarations: product(self.declarations, count)?,
            acceptance: product(self.acceptance, count)?,
            structure: product(self.structure, count)?,
        })
    }
    fn fits(self, maximum: Self) -> Result<(), CodecError> {
        if self.parsing > maximum.parsing
            || self.source > maximum.source
            || self.declarations > maximum.declarations
            || self.acceptance > maximum.acceptance
            || self.structure > maximum.structure
        {
            return Err(CodecError::Capacity);
        }
        Ok(())
    }
}
#[derive(Debug, Clone, Copy)]
pub struct LegacyCreationLimits {
    pub declaration: validation::Limits,
    pub acceptance: aggregation::Limits,
    pub bytes: usize,
    pub work: LegacyCreationWork,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LegacyCreationQuote {
    /// Inline NativeInput, final vector capacities and allocator bookkeeping.
    pub bytes: usize,
    pub allocations: usize,
    pub preparation: LegacyCreationWork,
    /// Includes reparsing/preparation and construction of every final object.
    pub construction: LegacyCreationWork,
}
#[derive(Debug)]
pub struct LegacyCreationPlan<'a> {
    bytes: &'a [u8],
    header: InputHeader,
    native: NativeLimits,
    limits: LegacyCreationLimits,
    quote: LegacyCreationQuote,
    intent: ContentHash,
}
impl LegacyCreationPlan<'_> {
    pub fn header(&self) -> InputHeader {
        self.header
    }
    pub fn quote(&self) -> LegacyCreationQuote {
        self.quote
    }
    pub fn intent_fingerprint(&self) -> ContentHash {
        self.intent
    }
    pub fn build(
        self,
        max_bytes: usize,
        work: LegacyCreationWork,
    ) -> Result<NativeInput, DecodeError> {
        plan::build(self, max_bytes, work)
    }
}
impl<'a> StructuralInput<'a> {
    pub fn prepare_legacy_creation(
        &self,
        native: NativeLimits,
        limits: LegacyCreationLimits,
    ) -> Result<Option<LegacyCreationPlan<'a>>, DecodeError> {
        if self.header().kind != (FrameKind::Request { command: 0 }) {
            return Ok(None);
        }
        if self.header().profile != NativeContentProfile::ProjectionOnly {
            return Err(CodecError::InvalidTag("creation profile").into());
        }
        plan::prepare(self.bytes(), self.header(), native, limits).map(Some)
    }
}

struct Work {
    maximum: LegacyCreationWork,
    parsing: Meter,
    source: Meter,
    declarations: Meter,
    acceptance: Meter,
    structure: Meter,
}
impl Work {
    fn new(maximum: LegacyCreationWork) -> Self {
        Self {
            maximum,
            parsing: Meter::new(maximum.parsing),
            source: Meter::new(maximum.source),
            declarations: Meter::new(maximum.declarations),
            acceptance: Meter::new(maximum.acceptance),
            structure: Meter::new(maximum.structure),
        }
    }
    fn used(&self) -> Result<LegacyCreationWork, CodecError> {
        Ok(LegacyCreationWork {
            parsing: difference(self.maximum.parsing, self.parsing.remaining())?,
            source: difference(self.maximum.source, self.source.remaining())?,
            declarations: difference(self.maximum.declarations, self.declarations.remaining())?,
            acceptance: difference(self.maximum.acceptance, self.acceptance.remaining())?,
            structure: difference(self.maximum.structure, self.structure.remaining())?,
        })
    }
    fn cursor<'a, T>(
        &self,
        bytes: &'a [u8],
        action: impl FnOnce(&mut Cursor<'a>) -> Result<T, DecodeError>,
    ) -> Result<T, DecodeError> {
        let mut cursor = Cursor::new(bytes, bytes.len(), self.parsing.remaining())?;
        let result = action(&mut cursor);
        self.parsing.charge(cursor.visits_used())?;
        result
    }
    fn model<T>(
        &self,
        meter: &Meter,
        action: impl FnOnce(&mut graph::VisitBudget) -> Result<T, ContractError>,
    ) -> Result<T, DecodeError> {
        let available = meter.remaining();
        let mut budget = graph::VisitBudget::new(available);
        let result = action(&mut budget);
        meter.charge(difference(available, budget.remaining())?)?;
        Ok(result?)
    }
}
fn sum(left: usize, right: usize) -> Result<usize, CodecError> {
    left.checked_add(right).ok_or(CodecError::Capacity)
}
fn product(left: usize, right: usize) -> Result<usize, CodecError> {
    left.checked_mul(right).ok_or(CodecError::Capacity)
}
fn difference(left: usize, right: usize) -> Result<usize, CodecError> {
    left.checked_sub(right).ok_or(CodecError::Capacity)
}
const ALLOCATION: usize = super::super::prepare::ALLOCATION;
fn vector_bytes<T>(count: usize) -> Result<usize, CodecError> {
    sum(
        product(size_of::<T>(), count)?,
        if count == 0 { 0 } else { ALLOCATION },
    )
}
fn reserve<T>(count: usize) -> Result<Vec<T>, DecodeError> {
    let mut values = Vec::new();
    values
        .try_reserve_exact(count)
        .map_err(|_| CodecError::Allocation)?;
    if values.capacity() != count {
        return Err(CodecError::Capacity.into());
    }
    Ok(values)
}
