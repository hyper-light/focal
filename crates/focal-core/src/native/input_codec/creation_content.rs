//! Complete borrowed creation descriptor bodies. These plans validate local
//! content only. Cohort correspondence, request identity, source selection and
//! owner-funded admission remain responsibilities of the enclosing ingress.
use super::bytes::Cursor;
use super::claim_source::ClaimView;
use super::validation_source::{DeclarationView, ValidationView};
use super::*;
use focal_model::lifecycle::{
    claim_descriptor as claim, validation as declaration, validation_descriptor as validation,
};

#[cfg(test)]
#[path = "creation_content_tests.rs"]
mod tests;

#[derive(Debug, Clone, Copy)]
pub struct BodyInspectionLimits {
    pub bytes: usize,
    pub parse_visits: usize,
    /// Extra scalar parsing needed to delimit variable claim collections.
    pub source_visits: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BodyParseQuote {
    pub parse_visits: usize,
    pub source_visits: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BodyConstructionQuote {
    /// Inline descriptor, final heap capacities and allocator bookkeeping.
    pub bytes: usize,
    pub allocations: usize,
    pub model_inspection_visits: usize,
    pub model_build_visits: usize,
    /// One allowance covers source work during preparation AND construction.
    pub source_inspection_visits: usize,
    pub source_build_visits: usize,
}

fn quote(
    charge: usize,
    allocations: usize,
    model_inspection_visits: usize,
    model_build_visits: usize,
    source_visits: usize,
    remaining: usize,
    source_build_visits: usize,
) -> Result<BodyConstructionQuote, DecodeError> {
    let source_inspection_visits = source_visits
        .checked_sub(remaining)
        .ok_or(CodecError::Capacity)?;
    let bytes = allocations
        .checked_mul(super::super::prepare::ALLOCATION)
        .and_then(|n| n.checked_add(charge))
        .ok_or(CodecError::Capacity)?;
    Ok(BodyConstructionQuote {
        bytes,
        allocations,
        model_inspection_visits,
        model_build_visits,
        source_inspection_visits,
        source_build_visits,
    })
}
fn check_build(
    quote: BodyConstructionQuote,
    max_bytes: usize,
    max_visits: usize,
) -> Result<(), DecodeError> {
    if quote.bytes > max_bytes || quote.model_build_visits > max_visits {
        return Err(CodecError::Capacity.into());
    }
    Ok(())
}
fn check_source(before: usize, after: usize, expected: usize) -> Result<(), DecodeError> {
    if before.checked_sub(after) != Some(expected) {
        return Err(CodecError::Capacity.into());
    }
    Ok(())
}
fn require_source(remaining: usize, required: usize) -> Result<(), DecodeError> {
    if remaining < required {
        return Err(CodecError::Capacity.into());
    }
    Ok(())
}

#[derive(Debug)]
pub struct ClaimBodyInput<'a> {
    source: ClaimView<'a>,
    parse: BodyParseQuote,
}
#[derive(Debug)]
pub struct ClaimBodyPlan<'s, 'a> {
    source: &'s ClaimView<'a>,
    descriptor: claim::ClaimSourcePlan<'s, 'a, ClaimView<'a>>,
    quote: BodyConstructionQuote,
}
impl<'a> ClaimBodyInput<'a> {
    pub(in crate::native) fn ownership_visits(&self) -> Result<usize, DecodeError> {
        use claim::ClaimSource;
        self.source
            .scope_count()
            .checked_add(self.source.slot_count())
            .and_then(|n| n.checked_mul(16))
            .and_then(|n| n.checked_add(128))
            .ok_or_else(|| CodecError::Capacity.into())
    }
    pub fn inspect(bytes: &'a [u8], limits: BodyInspectionLimits) -> Result<Self, DecodeError> {
        let mut cursor = Cursor::new(bytes, limits.bytes, limits.parse_visits)?;
        let value = Self::read(&mut cursor, limits.source_visits)?;
        cursor.finish()?;
        Ok(value)
    }
    pub(in crate::native) fn read(
        cursor: &mut Cursor<'a>,
        max_source_visits: usize,
    ) -> Result<Self, DecodeError> {
        let before = cursor.visits_used();
        let source = ClaimView::read(cursor, max_source_visits)?;
        let parse = BodyParseQuote {
            parse_visits: cursor
                .visits_used()
                .checked_sub(before)
                .ok_or(CodecError::Capacity)?,
            source_visits: max_source_visits
                .checked_sub(source.remaining_visits())
                .ok_or(CodecError::Capacity)?,
        };
        Ok(Self { source, parse })
    }
    pub(super) fn source(&self) -> &ClaimView<'a> {
        &self.source
    }
    pub fn parse_quote(&self) -> BodyParseQuote {
        self.parse
    }
    pub fn prepare(
        &mut self,
        limits: claim::Limits,
        max_model_visits: usize,
        max_source_visits: usize,
    ) -> Result<ClaimBodyPlan<'_, 'a>, DecodeError> {
        let plan = self.prepare_for_frame(limits, max_model_visits, max_source_visits)?;
        require_source(
            plan.source.remaining_visits(),
            plan.quote.source_build_visits,
        )?;
        Ok(plan)
    }
    pub(super) fn prepare_for_frame(
        &mut self,
        limits: claim::Limits,
        max_model_visits: usize,
        max_source_visits: usize,
    ) -> Result<ClaimBodyPlan<'_, 'a>, DecodeError> {
        self.source.set_visits(max_source_visits);
        let descriptor =
            claim::ClaimDescriptor::prepare_source(&self.source, limits, max_model_visits)?;
        let quote = quote(
            descriptor.construction_charge(),
            descriptor.construction_heap_allocations(),
            descriptor.inspection_visits(),
            descriptor.build_visits(),
            max_source_visits,
            self.source.remaining_visits(),
            self.source.single_pass_visits(),
        )?;
        Ok(ClaimBodyPlan {
            source: &self.source,
            descriptor,
            quote,
        })
    }
}
impl ClaimBodyPlan<'_, '_> {
    pub(super) fn issuer(&self) -> ParticipantId {
        self.descriptor.issuer()
    }
    pub fn quote(&self) -> BodyConstructionQuote {
        self.quote
    }
    pub fn fields(&self) -> claim::ClaimFields<'_> {
        self.descriptor.fields()
    }
    pub fn content_hash(&self) -> ContentHash {
        self.descriptor.content_hash()
    }
    pub fn intent_fingerprint(&self) -> ContentHash {
        self.descriptor.intent_fingerprint()
    }
    pub fn build(
        self,
        max_bytes: usize,
        max_visits: usize,
    ) -> Result<claim::ClaimDescriptor, DecodeError> {
        check_build(self.quote, max_bytes, max_visits)?;
        let before = self.source.remaining_visits();
        require_source(before, self.quote.source_build_visits)?;
        let charge = self.descriptor.construction_charge();
        let value = self.descriptor.build(charge, max_visits)?;
        check_source(
            before,
            self.source.remaining_visits(),
            self.quote.source_build_visits,
        )?;
        Ok(value)
    }
}

#[derive(Debug)]
pub struct DeclarationBodyInput<'a> {
    source: DeclarationView<'a>,
    parse: BodyParseQuote,
}
#[derive(Debug)]
pub struct DeclarationBodyPlan<'s, 'a> {
    source: &'s DeclarationView<'a>,
    descriptor: declaration::DeclarationSourcePlan<'s, 'a, DeclarationView<'a>>,
    quote: BodyConstructionQuote,
}
impl<'a> DeclarationBodyInput<'a> {
    pub(in crate::native) fn fields(&self) -> declaration::DeclarationFields<'_> {
        use declaration::DeclarationSource;
        self.source.fields()
    }
    pub(super) fn source(&self) -> &DeclarationView<'a> {
        &self.source
    }
    pub fn inspect(bytes: &'a [u8], limits: BodyInspectionLimits) -> Result<Self, DecodeError> {
        let mut cursor = Cursor::new(bytes, limits.bytes, limits.parse_visits)?;
        let value = Self::read(&mut cursor)?;
        cursor.finish()?;
        Ok(value)
    }
    pub(in crate::native) fn read(cursor: &mut Cursor<'a>) -> Result<Self, DecodeError> {
        let before = cursor.visits_used();
        let source = DeclarationView::read(cursor)?;
        let parse = BodyParseQuote {
            parse_visits: cursor
                .visits_used()
                .checked_sub(before)
                .ok_or(CodecError::Capacity)?,
            source_visits: 0,
        };
        Ok(Self { source, parse })
    }
    pub fn parse_quote(&self) -> BodyParseQuote {
        self.parse
    }
    pub fn prepare(
        &mut self,
        principal: Principal,
        limits: declaration::Limits,
        max_model_visits: usize,
        max_source_visits: usize,
    ) -> Result<DeclarationBodyPlan<'_, 'a>, DecodeError> {
        let plan =
            self.prepare_for_frame(principal, limits, max_model_visits, max_source_visits)?;
        require_source(
            plan.source.remaining_visits(),
            plan.quote.source_build_visits,
        )?;
        Ok(plan)
    }
    pub(super) fn prepare_for_frame(
        &mut self,
        principal: Principal,
        limits: declaration::Limits,
        max_model_visits: usize,
        max_source_visits: usize,
    ) -> Result<DeclarationBodyPlan<'_, 'a>, DecodeError> {
        self.source.set_visits(max_source_visits);
        let descriptor = declaration::Declaration::prepare_source(
            principal,
            &self.source,
            limits,
            max_model_visits,
        )?;
        let quote = quote(
            descriptor.construction_charge(),
            descriptor.construction_heap_allocations(),
            descriptor.inspection_visits(),
            descriptor.build_visits(),
            max_source_visits,
            self.source.remaining_visits(),
            self.source.single_pass_visits()?,
        )?;
        Ok(DeclarationBodyPlan {
            source: &self.source,
            descriptor,
            quote,
        })
    }
}
impl DeclarationBodyPlan<'_, '_> {
    pub(super) fn checked_declaration(&self) -> declaration::CheckedDeclaration {
        self.descriptor.checked_declaration()
    }
    pub fn quote(&self) -> BodyConstructionQuote {
        self.quote
    }
    pub fn fields(&self) -> declaration::DeclarationFields<'_> {
        self.descriptor.fields()
    }
    pub fn intent_fingerprint(&self) -> ContentHash {
        self.descriptor.intent_fingerprint()
    }
    pub fn build(
        self,
        max_bytes: usize,
        max_visits: usize,
    ) -> Result<declaration::Declaration, DecodeError> {
        check_build(self.quote, max_bytes, max_visits)?;
        let before = self.source.remaining_visits();
        require_source(before, self.quote.source_build_visits)?;
        let charge = self.descriptor.construction_charge();
        let value = self.descriptor.build(charge, max_visits)?;
        check_source(
            before,
            self.source.remaining_visits(),
            self.quote.source_build_visits,
        )?;
        Ok(value)
    }
}

#[derive(Debug)]
pub struct ValidationBodyInput<'a> {
    source: ValidationView<'a>,
    parse: BodyParseQuote,
}
#[derive(Debug)]
pub struct ValidationBodyPlan<'s, 'a> {
    source: &'s ValidationView<'a>,
    descriptor: validation::ValidationSourcePlan<'s, 'a, ValidationView<'a>>,
    quote: BodyConstructionQuote,
}
impl<'a> ValidationBodyInput<'a> {
    pub(in crate::native) fn fields(&self) -> validation::ValidationFields<'a> {
        use validation::ValidationSource;
        self.source.fields()
    }
    pub fn inspect(bytes: &'a [u8], limits: BodyInspectionLimits) -> Result<Self, DecodeError> {
        let mut cursor = Cursor::new(bytes, limits.bytes, limits.parse_visits)?;
        let value = Self::read(&mut cursor)?;
        cursor.finish()?;
        Ok(value)
    }
    pub(in crate::native) fn read(cursor: &mut Cursor<'a>) -> Result<Self, DecodeError> {
        let before = cursor.visits_used();
        let source = ValidationView::read(cursor)?;
        let parse = BodyParseQuote {
            parse_visits: cursor
                .visits_used()
                .checked_sub(before)
                .ok_or(CodecError::Capacity)?,
            source_visits: 0,
        };
        Ok(Self { source, parse })
    }
    pub fn parse_quote(&self) -> BodyParseQuote {
        self.parse
    }
    pub fn prepare(
        &mut self,
        principal: Principal,
        limits: validation::Limits,
        max_model_visits: usize,
        max_source_visits: usize,
    ) -> Result<ValidationBodyPlan<'_, 'a>, DecodeError> {
        let plan =
            self.prepare_for_frame(principal, limits, max_model_visits, max_source_visits)?;
        require_source(
            plan.source.remaining_visits(),
            plan.quote.source_build_visits,
        )?;
        Ok(plan)
    }
    pub(super) fn prepare_for_frame(
        &mut self,
        principal: Principal,
        limits: validation::Limits,
        max_model_visits: usize,
        max_source_visits: usize,
    ) -> Result<ValidationBodyPlan<'_, 'a>, DecodeError> {
        self.source.set_visits(max_source_visits);
        let descriptor = validation::ValidationDescriptor::prepare_source(
            principal,
            &self.source,
            limits,
            max_model_visits,
        )?;
        let quote = quote(
            descriptor.construction_charge(),
            descriptor.construction_heap_allocations(),
            descriptor.inspection_visits(),
            descriptor.build_visits(),
            max_source_visits,
            self.source.remaining_visits(),
            self.source.build_visits()?,
        )?;
        Ok(ValidationBodyPlan {
            source: &self.source,
            descriptor,
            quote,
        })
    }
}
impl ValidationBodyPlan<'_, '_> {
    /// Consumes the body plan: cohort proof inspection has its own extra source
    /// allowance and cannot silently reduce the budget promised for body build.
    pub(super) fn into_checked_declaration(
        self,
        max_visits: usize,
    ) -> Result<(declaration::CheckedDeclaration, usize, usize), DecodeError> {
        let before = self.source.remaining_visits();
        let (value, visits) = self.descriptor.checked_declaration(max_visits)?;
        let source = before
            .checked_sub(self.source.remaining_visits())
            .ok_or(CodecError::Capacity)?;
        Ok((value, visits, source))
    }
    pub fn quote(&self) -> BodyConstructionQuote {
        self.quote
    }
    pub fn fields(&self) -> validation::ValidationFields<'_> {
        self.descriptor.fields()
    }
    pub fn content_hash(&self) -> ContentHash {
        self.descriptor.content_hash()
    }
    pub fn specification_hash(&self) -> ContentHash {
        self.descriptor.specification_hash()
    }
    pub fn intent_fingerprint(&self) -> ContentHash {
        self.descriptor.intent_fingerprint()
    }
    pub fn build(
        self,
        max_bytes: usize,
        max_visits: usize,
    ) -> Result<validation::ValidationDescriptor, DecodeError> {
        check_build(self.quote, max_bytes, max_visits)?;
        let before = self.source.remaining_visits();
        require_source(before, self.quote.source_build_visits)?;
        let charge = self.descriptor.construction_charge();
        let value = self.descriptor.build(charge, max_visits)?;
        check_source(
            before,
            self.source.remaining_visits(),
            self.quote.source_build_visits,
        )?;
        Ok(value)
    }
}
