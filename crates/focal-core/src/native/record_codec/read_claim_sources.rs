use super::*;
use focal_model::lifecycle::claim_descriptor::{ClaimSlotFields, ClaimSlotSource};

pub(in crate::native::record_codec) struct Acceptance<'m, 'a, D: Objects + ?Sized> {
    pub(in crate::native::record_codec) slots: Span<'a>,
    pub(in crate::native::record_codec) declarations: Span<'a>,
    pub(in crate::native::record_codec) objects: &'m D,
    pub(in crate::native::record_codec) meter: &'m Meter,
}
pub(in crate::native::record_codec) struct SlotSource<'m, 'a> {
    raw: Slot<'a>,
    meter: &'m Meter,
}
pub(in crate::native::record_codec) struct Slots<'m, 'a> {
    values: Values<'m, 'a, Slot<'a>>,
    meter: &'m Meter,
}
impl<'m, 'a> Iterator for Slots<'m, 'a> {
    type Item = Result<SlotSource<'m, 'a>, ContractError>;
    fn next(&mut self) -> Option<Self::Item> {
        self.values.next().map(|v| {
            v.map(|raw| SlotSource {
                raw,
                meter: self.meter,
            })
        })
    }
}
impl ClaimSlotSource for SlotSource<'_, '_> {
    type Checks<'s>
        = Values<'s, 's, aggregation::CheckPolicy>
    where
        Self: 's;
    fn fields(&self) -> ClaimSlotFields {
        self.raw.fields
    }
    fn checks(&self) -> Self::Checks<'_> {
        Values::new(self.raw.checks, self.meter, check)
    }
}
pub(in crate::native::record_codec) struct Declarations<'m, 'a, D: Objects + ?Sized> {
    values: Values<'m, 'a, Declared>,
    objects: &'m D,
    meter: &'m Meter,
}
impl<D: Objects + ?Sized> Iterator for Declarations<'_, '_, D> {
    type Item = Result<validation::CheckedDeclaration, ContractError>;
    fn next(&mut self) -> Option<Self::Item> {
        self.values.next().map(|encoded| {
            let encoded = encoded?;
            self.meter.charge(8).map_err(model_error)?;
            let actual = self
                .objects
                .declaration(ValidationId(encoded.binding.object.0))?
                .checked_declaration();
            actual.binding().check(&encoded.binding)?;
            if actual.declaration_index() != encoded.index
                || actual.mode() != encoded.mode
                || actual.target() != encoded.target
            {
                return Err(ContractError::InvalidPolicy);
            }
            Ok(actual)
        })
    }
}
impl<D: Objects + ?Sized> aggregation::AcceptanceSource for Acceptance<'_, '_, D> {
    type Slot<'s>
        = SlotSource<'s, 's>
    where
        Self: 's;
    type Slots<'s>
        = Slots<'s, 's>
    where
        Self: 's;
    type Declarations<'s>
        = Declarations<'s, 's, D>
    where
        Self: 's;
    fn slot_count(&self) -> usize {
        self.slots.count
    }
    fn declaration_count(&self) -> usize {
        self.declarations.count
    }
    fn slots(&self) -> Self::Slots<'_> {
        Slots {
            values: Values::new(self.slots, self.meter, slot),
            meter: self.meter,
        }
    }
    fn declarations(&self) -> Self::Declarations<'_> {
        Declarations {
            values: Values::new(self.declarations, self.meter, declaration),
            objects: self.objects,
            meter: self.meter,
        }
    }
}

pub(in crate::native::record_codec) struct Responses<'m, 'a, D: Objects + ?Sized> {
    pub(in crate::native::record_codec) span: Span<'a>,
    pub(in crate::native::record_codec) objects: &'m D,
    pub(in crate::native::record_codec) meter: &'m Meter,
}
pub(in crate::native::record_codec) struct ResponseValues<'m, 'a, D: Objects + ?Sized> {
    values: Values<'m, 'a, claim::ClaimResponseSnapshotV1>,
    objects: &'m D,
    meter: &'m Meter,
}
impl<'m, D: Objects + ?Sized> Iterator for ResponseValues<'m, '_, D> {
    type Item = Result<claim::ClaimResponseValue<'m>, ContractError>;
    fn next(&mut self) -> Option<Self::Item> {
        self.values.next().map(|history| {
            let history = history?;
            self.meter.charge(1).map_err(model_error)?;
            Ok(claim::ClaimResponseValue {
                response: self.objects.response(history.link.testament)?,
                history,
            })
        })
    }
}
impl<D: Objects + ?Sized> claim::ClaimResponseSource for Responses<'_, '_, D> {
    type Responses<'a>
        = ResponseValues<'a, 'a, D>
    where
        Self: 'a;
    fn responses(&self) -> Self::Responses<'_> {
        ResponseValues {
            values: Values::new(self.span, self.meter, fields::claim_response),
            objects: self.objects,
            meter: self.meter,
        }
    }
}

pub(in crate::native::record_codec) struct Registrations<'m, 'a, D: Objects + ?Sized> {
    pub(in crate::native::record_codec) claim: ClaimId,
    pub(in crate::native::record_codec) span: Span<'a>,
    pub(in crate::native::record_codec) objects: &'m D,
    pub(in crate::native::record_codec) meter: &'m Meter,
}
pub(in crate::native::record_codec) struct Members<'m, 'a, D: Objects + ?Sized> {
    claim: ClaimId,
    values: Values<'m, 'a, aggregation::RegistrationMemberSnapshotV1>,
    objects: &'m D,
    meter: &'m Meter,
}
impl<'m, D: Objects + ?Sized> Iterator for Members<'m, '_, D> {
    type Item = Result<aggregation::RegistrationValue<'m>, ContractError>;
    fn next(&mut self) -> Option<Self::Item> {
        self.values.next().map(|member| {
            let member = member?;
            self.meter.charge(4).map_err(model_error)?;
            let declaration = self
                .objects
                .declaration(ValidationId(member.binding.object.0))?;
            let key = EvaluationKey {
                claim: self.claim,
                validation: ValidationId(member.binding.object.0),
                target: crate::native::EvaluationTarget::of(member.target),
                generation: member.generation,
            };
            let evaluation = *self.objects.evaluation(key)?;
            Ok(aggregation::RegistrationValue {
                member,
                declaration,
                evaluation,
            })
        })
    }
}
impl<D: Objects + ?Sized> aggregation::RegistrationSnapshotSource for Registrations<'_, '_, D> {
    type Rows<'a>
        = Members<'a, 'a, D>
    where
        Self: 'a;
    fn rows(&self) -> Self::Rows<'_> {
        Members {
            claim: self.claim,
            values: Values::new(self.span, self.meter, fields::registration_member),
            objects: self.objects,
            meter: self.meter,
        }
    }
}
