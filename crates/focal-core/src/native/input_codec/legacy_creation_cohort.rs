use super::*;
use focal_model::lifecycle::validation::{CheckedDeclaration, DeclarationSource};

#[derive(Clone, Copy)]
pub(super) struct DeclarationInfo {
    pub(super) checked: CheckedDeclaration,
    pub(super) intent: ContentHash,
    pub(super) heap: usize,
    pub(super) allocations: usize,
    pub(super) model_build: usize,
    pub(super) source_build: usize,
}

pub(super) fn process(
    input: &mut DeclarationBodyInput<'_>,
    principal: Principal,
    limits: validation::Limits,
    work: &Work,
    build: bool,
) -> Result<(DeclarationInfo, Option<validation::Declaration>), DecodeError> {
    let plan = input.prepare_for_frame(
        principal,
        limits,
        work.declarations.remaining(),
        work.source.remaining(),
    )?;
    let quote = plan.quote();
    work.declarations.charge(quote.model_inspection_visits)?;
    work.source.charge(quote.source_inspection_visits)?;
    let info = DeclarationInfo {
        checked: plan.checked_declaration(),
        intent: plan.intent_fingerprint(),
        heap: difference(
            difference(quote.bytes, product(quote.allocations, ALLOCATION)?)?,
            size_of::<validation::Declaration>(),
        )?,
        allocations: quote.allocations,
        model_build: quote.model_build_visits,
        source_build: quote.source_build_visits,
    };
    let owned = if build {
        work.source.charge(quote.source_build_visits)?;
        work.declarations.charge(quote.model_build_visits)?;
        Some(plan.build(quote.bytes, quote.model_build_visits)?)
    } else {
        None
    };
    Ok((info, owned))
}

pub(super) struct Bodies<'w, 'a> {
    tail: &'a [u8],
    left: usize,
    work: &'w Work,
}
impl<'w, 'a> Bodies<'w, 'a> {
    pub(super) fn new(span: Span<'a>, work: &'w Work) -> Self {
        Self {
            tail: span.bytes,
            left: span.count,
            work,
        }
    }
}
impl<'a> Iterator for Bodies<'_, 'a> {
    type Item = Result<DeclarationBodyInput<'a>, DecodeError>;
    fn next(&mut self) -> Option<Self::Item> {
        if self.left == 0 {
            return if self.tail.is_empty() {
                None
            } else {
                self.tail = &[];
                Some(Err(CodecError::TrailingBytes.into()))
            };
        }
        let result = self.work.cursor(self.tail, |cursor| {
            self.work.structure.charge(1)?;
            let body = DeclarationBodyInput::read(cursor)?;
            self.tail = self
                .tail
                .get(cursor.offset()..)
                .ok_or(CodecError::Truncated)?;
            self.left = difference(self.left, 1)?;
            Ok(body)
        });
        if result.is_err() {
            self.left = 0;
            self.tail = &[];
        }
        Some(result)
    }
}
fn error(value: DecodeError) -> ContractError {
    match value {
        DecodeError::Codec(value) => model_error(value),
        DecodeError::Native(NativeError::Contract(error)) => error,
        _ => ContractError::Capacity,
    }
}
pub(super) struct CheckedValues<'w, 'a> {
    bodies: Bodies<'w, 'a>,
    claim: ClaimId,
    principal: Principal,
    limits: validation::Limits,
    failed: bool,
}
impl Iterator for CheckedValues<'_, '_> {
    type Item = Result<CheckedDeclaration, ContractError>;
    fn next(&mut self) -> Option<Self::Item> {
        if self.failed {
            return None;
        }
        loop {
            let next = self.bodies.next()?;
            let value = (|| {
                let mut body = next?;
                if body.source().fields().claim != self.claim {
                    return Ok(None);
                }
                process(
                    &mut body,
                    self.principal,
                    self.limits,
                    self.bodies.work,
                    false,
                )
                .map(|(value, _)| Some(value.checked))
            })();
            match value {
                Ok(None) => continue,
                Ok(Some(value)) => return Some(Ok(value)),
                Err(value) => {
                    self.failed = true;
                    return Some(Err(error(value)));
                }
            }
        }
    }
}

pub(super) struct Cohort<'w, 'a> {
    pub(super) slots: Span<'a>,
    pub(super) declarations: Span<'a>,
    pub(super) count: usize,
    pub(super) claim: ClaimId,
    pub(super) principal: Principal,
    pub(super) limits: validation::Limits,
    pub(super) work: &'w Work,
}
impl aggregation::AcceptanceSource for Cohort<'_, '_> {
    type Slot<'s>
        = super::super::claim_source::SlotView<'s, 's>
    where
        Self: 's;
    type Slots<'s>
        = Values<'s, 's, Self::Slot<'s>>
    where
        Self: 's;
    type Declarations<'s>
        = CheckedValues<'s, 's>
    where
        Self: 's;
    fn slot_count(&self) -> usize {
        self.slots.count
    }
    fn declaration_count(&self) -> usize {
        self.count
    }
    fn slots(&self) -> Self::Slots<'_> {
        Values::new(
            self.slots,
            &self.work.source,
            super::super::claim_source::slot,
        )
    }
    fn declarations(&self) -> Self::Declarations<'_> {
        CheckedValues {
            bodies: Bodies::new(self.declarations, self.work),
            claim: self.claim,
            principal: self.principal,
            limits: self.limits,
            failed: false,
        }
    }
}
