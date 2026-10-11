//! Checks complete actual declaration bodies without temporary typed cohorts.
use super::*;

fn model_error(error: DecodeError) -> ContractError {
    match error {
        DecodeError::Native(NativeError::Contract(error)) => error,
        DecodeError::Codec(CodecError::Capacity | CodecError::Allocation) => {
            ContractError::Capacity
        }
        DecodeError::Codec(_) => ContractError::InvalidManifest,
        _ => ContractError::Capacity,
    }
}
/// A validation body's checked declaration and its specification's hash.
#[derive(Clone, Copy)]
pub(super) struct Metadata {
    declaration: validation::CheckedDeclaration,
    specification: ContentHash,
}
/// The checked declaration of the body `plan` planned, charged as a cohort's
/// read charges it: the planning loop hands its cohort what it planned.
pub(super) fn checked(
    plan: ValidationBodyPlan<'_, '_>,
    budget: &Budget,
) -> Result<Metadata, DecodeError> {
    let specification = plan.specification_hash();
    let (declaration, visits, source) =
        plan.into_checked_declaration(budget.descriptor.remaining())?;
    budget.descriptor.charge(visits)?;
    budget.source.charge(source)?;
    Ok(Metadata {
        declaration,
        specification,
    })
}
fn metadata(
    reader: &mut Reader<'_, '_>,
    principal: Principal,
    limits: AuthoredCreationLimits,
) -> Result<Metadata, DecodeError> {
    let mut body = reader.validation()?;
    let plan = prepare_validation(&mut body, reader.budget, principal, limits)?;
    let specification = plan.specification_hash();
    let (declaration, visits, source) =
        plan.into_checked_declaration(reader.budget.descriptor.remaining())?;
    reader.budget.descriptor.charge(visits)?;
    reader.budget.source.charge(source)?;
    Ok(Metadata {
        declaration,
        specification,
    })
}

/// A read of a claim's declaration bodies from the frame.
struct Reads<'b, 'a> {
    reader: Reader<'b, 'a>,
    left: usize,
    principal: Principal,
    limits: &'b AuthoredCreationLimits,
}
impl Iterator for Reads<'_, '_> {
    type Item = Result<Metadata, ContractError>;
    fn next(&mut self) -> Option<Self::Item> {
        if self.left == 0 {
            return if self.reader.tail.is_empty() {
                None
            } else {
                self.reader.tail = &[];
                Some(Err(ContractError::InvalidManifest))
            };
        }
        let value = (|| {
            let value = metadata(&mut self.reader, self.principal, *self.limits)?;
            self.left = subtract(self.left, 1)?;
            Ok(value)
        })();
        if value.is_err() {
            self.left = 0;
            self.reader.tail = &[];
        }
        Some(value.map_err(model_error))
    }
}
/// A pass over a cohort's declarations: those it holds, or read from the frame.
enum Declarations<'s, 'b, 'a> {
    Held(&'s [Option<Metadata>]),
    Read(Reads<'b, 'a>),
}
impl Iterator for Declarations<'_, '_, '_> {
    type Item = Result<validation::CheckedDeclaration, ContractError>;
    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Held(values) => {
                let (first, rest) = values.split_first()?;
                *values = rest;
                Some(
                    first
                        .map(|value| value.declaration)
                        .ok_or(ContractError::InvalidManifest),
                )
            }
            Self::Read(values) => Some(values.next()?.map(|value| value.declaration)),
        }
    }
}

struct Cohort<'b, 'a> {
    claim: &'b ClaimView<'a>,
    declarations: &'a [u8],
    count: usize,
    budget: &'b Budget,
    principal: Principal,
    limits: AuthoredCreationLimits,
    held: super::held::Held<Metadata>,
}
impl<'a> Cohort<'_, 'a> {
    fn read(&self) -> Reads<'_, 'a> {
        Reads {
            reader: Reader {
                tail: self.declarations,
                budget: self.budget,
            },
            left: self.count,
            principal: self.principal,
            limits: &self.limits,
        }
    }
}
impl<'a> aggregation::AcceptanceSource for Cohort<'_, 'a> {
    type Slot<'s>
        = SlotView<'s, 'a>
    where
        Self: 's;
    type Slots<'s>
        = Values<'s, 'a, SlotView<'s, 'a>>
    where
        Self: 's;
    type Declarations<'s>
        = Declarations<'s, 's, 'a>
    where
        Self: 's;
    fn slot_count(&self) -> usize {
        self.claim.slot_count()
    }
    fn declaration_count(&self) -> usize {
        self.count
    }
    fn slots(&self) -> Self::Slots<'_> {
        self.claim.slots_from(&self.budget.source)
    }
    fn declarations(&self) -> Self::Declarations<'_> {
        match self.held.replay() {
            Some(values) => Declarations::Held(values),
            None => Declarations::Read(self.read()),
        }
    }
    /// A cohort that holds its declarations replays them; its slots are read
    /// from the borrowed frame, which does not change, by the same decoder.
    fn replays(&self) -> bool {
        self.held.replay().is_some()
    }
}

pub(super) fn cohort(
    group: &Group<'_>,
    claim: Binding,
    issuer: ParticipantId,
    principal: Principal,
    limits: AuthoredCreationLimits,
    budget: &Budget,
    planned: super::held::Held<Metadata>,
) -> Result<(), DecodeError> {
    let mut source = Cohort {
        claim: group.claim.source(),
        declarations: group.declarations,
        count: group.count,
        budget,
        principal,
        limits,
        held: super::held::Held::none(),
    };
    // Each body is checked and stamped once, for every pass after: as its
    // plan read it where the planning loop handed it over, else read here.
    source.held = match planned.replay() {
        Some(_) => planned,
        None => super::held::Held::read(source.read())?.0,
    };
    let acceptance = aggregation::AcceptancePolicy::prepare_source(
        claim,
        issuer,
        &source,
        limits.acceptance,
        budget.acceptance.remaining(),
    )?;
    budget.acceptance.charge(acceptance.inspection_visits())?;
    // The source checker proves complete Required Delivery/slot correspondence.
    // Requirements additionally pin each actual authored specification, with
    // authored requirement order independent of declaration storage order.
    for requirement in source.claim.requirements_from(&budget.source) {
        budget.native.charge(1)?;
        let requirement = requirement?;
        let mut found = false;
        let mut matches = |value: Metadata| {
            if value.declaration.binding().object.0 == requirement.id.0 {
                if value.specification != requirement.specification {
                    return Err(ContractError::ContentConflict);
                }
                found = true;
            }
            Ok(())
        };
        match source.held.replay() {
            Some(values) => {
                for value in values {
                    budget.native.charge(1)?;
                    matches(value.ok_or(ContractError::InvalidManifest)?)?;
                }
            }
            None => {
                let mut rows = Reader {
                    tail: group.declarations,
                    budget,
                };
                for _ in 0..group.count {
                    budget.native.charge(1)?;
                    matches(metadata(&mut rows, principal, limits)?)?;
                }
                rows.finish()?;
            }
        }
        if !found {
            return Err(ContractError::InvalidPolicy.into());
        }
    }
    Ok(())
}

/// Family-scoped IDs cannot be reused by different authored groups. Immutable
/// bytes have already passed the complete body/cohort checks; this pass only
/// reads exact authored IDs and never derives identity from a supplied hash.
pub(super) fn batch_ids(
    bytes: &[u8],
    count: usize,
    native: NativeLimits,
    budget: &Budget,
) -> Result<(), DecodeError> {
    let (mut outer, actual) = Reader::start(bytes, budget, native.plan_nodes)?;
    if actual != count {
        return Err(ContractError::InvalidPolicy.into());
    }
    for ordinal in 0..count {
        let current = outer.group(native.definitions)?;
        let current_id = current.claim.source().fields().id;
        let (mut previous, _) = Reader::start(bytes, budget, native.plan_nodes)?;
        for _ in 0..ordinal {
            let previous = previous.group(native.definitions)?;
            budget.native.charge(1)?;
            if previous.claim.source().fields().id == current_id {
                return Err(ContractError::InvalidPolicy.into());
            }
            let mut rows = Reader {
                tail: current.declarations,
                budget,
            };
            for _ in 0..current.count {
                let id = rows.validation()?.fields().id;
                let mut old = Reader {
                    tail: previous.declarations,
                    budget,
                };
                for _ in 0..previous.count {
                    budget.native.charge(1)?;
                    if old.validation()?.fields().id == id {
                        return Err(ContractError::InvalidPolicy.into());
                    }
                }
                old.finish()?;
            }
            rows.finish()?;
        }
    }
    outer.finish()
}
