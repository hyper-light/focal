//! Borrowed complete declaration/validation bodies for creation decoding.
//! These views grant no evaluator authority and never invoke a handler.
use super::bytes::Cursor;
use super::source_bytes::{Meter, SourceCursor, Span, Values};
use super::*;
use focal_model::lifecycle::{validation as model, validation_descriptor as authored};
use focal_model::{ValidationKind, ValidationMode, ValidationPhase, ValidatorId};

fn option(cursor: &mut Cursor<'_>) -> Result<bool, CodecError> {
    match cursor.u8()? {
        0 => Ok(false),
        1 => Ok(true),
        _ => Err(CodecError::InvalidTag("option")),
    }
}
fn mode(cursor: &mut Cursor<'_>) -> Result<ValidationMode, CodecError> {
    match cursor.u8()? {
        0 => Ok(ValidationMode::Required),
        1 => Ok(ValidationMode::Observe),
        _ => Err(CodecError::InvalidTag("validation mode")),
    }
}
fn handler(cursor: &mut SourceCursor<'_, '_>) -> Result<model::HandlerValue, CodecError> {
    let id = ValidatorId(cursor.fixed()?);
    let version = ContentHash(cursor.fixed()?);
    let agentic = match cursor.u8()? {
        0 => false,
        1 => true,
        _ => return Err(CodecError::InvalidTag("agentic flag")),
    };
    Ok(model::HandlerValue {
        id,
        version,
        agentic,
        attempts: cursor.u32()?,
        proof_schema: ContentHash(cursor.fixed()?),
        diagnostic_schema: ContentHash(cursor.fixed()?),
    })
}
fn contributor(cursor: &mut SourceCursor<'_, '_>) -> Result<ParticipantId, CodecError> {
    Ok(ParticipantId(cursor.fixed()?))
}

#[derive(Debug)]
struct Policies<'a> {
    check: Span<'a>,
    quality: Span<'a>,
    meter: Meter,
}
impl Policies<'_> {
    fn visits(&self) -> Result<usize, CodecError> {
        self.check
            .count
            .checked_add(self.quality.count)
            .and_then(|n| n.checked_mul(123))
            .ok_or(CodecError::Capacity)
    }
}
fn phase<'a>(cursor: &mut Cursor<'a>) -> Result<(model::PhaseFields, Span<'a>), CodecError> {
    let evaluator = ParticipantId(cursor.fixed()?);
    let definition = ContentHash(cursor.fixed()?);
    let required_policy = if option(cursor)? {
        Some(ContentHash(cursor.fixed()?))
    } else {
        None
    };
    let handlers = Span::read(cursor, 117)?;
    Ok((
        model::PhaseFields {
            evaluator,
            definition,
            required_policy,
            handlers: handlers.count,
        },
        handlers,
    ))
}
fn program<'a>(
    cursor: &mut Cursor<'a>,
) -> Result<(model::ProgramFields, Policies<'a>), CodecError> {
    let mut policies = Policies {
        check: Span::EMPTY,
        quality: Span::EMPTY,
        meter: Meter::new(0),
    };
    let program = match cursor.u8()? {
        0 => model::ProgramFields::Delivery,
        1 => {
            let (check, span) = phase(cursor)?;
            policies.check = span;
            let quality = if option(cursor)? {
                let (quality, span) = phase(cursor)?;
                policies.quality = span;
                Some(quality)
            } else {
                None
            };
            model::ProgramFields::Programmatic { check, quality }
        }
        2 => {
            let (check, span) = phase(cursor)?;
            policies.check = span;
            model::ProgramFields::Agentic { check }
        }
        _ => return Err(CodecError::InvalidTag("validation program")),
    };
    Ok((program, policies))
}
struct Common<'a> {
    claim: ClaimId,
    issuer: ParticipantId,
    declaration_index: u32,
    kind: ValidationKind,
    phase: ValidationPhase,
    mode: ValidationMode,
    target: model::TargetDeclaration<'a>,
    program: model::ProgramFields,
    deadline: Deadline,
}
fn common<'a>(cursor: &mut Cursor<'a>) -> Result<(Common<'a>, Policies<'a>), CodecError> {
    let claim = ClaimId(cursor.fixed()?);
    let issuer = ParticipantId(cursor.fixed()?);
    let declaration_index = cursor.u32()?;
    let kind = match cursor.u16()? {
        1 => ValidationKind::Receipt,
        2 => ValidationKind::Test,
        3 => ValidationKind::Inspection,
        4 => ValidationKind::Integration,
        5 => ValidationKind::Contract,
        6 => ValidationKind::Design,
        7 => ValidationKind::Regression,
        _ => return Err(CodecError::InvalidTag("validation kind")),
    };
    let phase = match cursor.u16()? {
        1 => ValidationPhase::Admission,
        2 => ValidationPhase::Increment,
        3 => ValidationPhase::WholeWork,
        _ => return Err(CodecError::InvalidTag("declared phase")),
    };
    let mode = mode(cursor)?;
    let target = match cursor.u8()? {
        0 => model::TargetDeclaration::WholeWorkSlot {
            index: cursor.u32()?,
            name: cursor.text(cursor.remaining())?,
        },
        1 => model::TargetDeclaration::Delivery,
        2 => model::TargetDeclaration::Admission,
        3 => model::TargetDeclaration::Increment,
        _ => return Err(CodecError::InvalidTag("target declaration")),
    };
    let (program, policies) = program(cursor)?;
    let deadline = fixed::deadline(cursor)?;
    Ok((
        Common {
            claim,
            issuer,
            declaration_index,
            kind,
            phase,
            mode,
            target,
            program,
            deadline,
        },
        policies,
    ))
}

#[derive(Debug)]
pub(super) struct DeclarationView<'a> {
    fields: model::DeclarationFields<'a>,
    policies: Policies<'a>,
}
#[derive(Debug)]
pub(super) struct ValidationView<'a> {
    fields: authored::ValidationFields<'a>,
    policies: Policies<'a>,
    contributors: Span<'a>,
}

impl model::PolicySource for DeclarationView<'_> {
    type Handlers<'s>
        = Values<'s, 's, model::HandlerValue>
    where
        Self: 's;
    fn handlers(&self, phase: model::PolicyPhase) -> Self::Handlers<'_> {
        let span = match phase {
            model::PolicyPhase::Check => self.policies.check,
            model::PolicyPhase::Quality => self.policies.quality,
        };
        Values::new(span, &self.policies.meter, handler)
    }
}
impl<'a> model::DeclarationSource<'a> for DeclarationView<'a> {
    fn fields(&self) -> model::DeclarationFields<'a> {
        self.fields
    }
}
impl model::PolicySource for ValidationView<'_> {
    type Handlers<'s>
        = Values<'s, 's, model::HandlerValue>
    where
        Self: 's;
    fn handlers(&self, phase: model::PolicyPhase) -> Self::Handlers<'_> {
        let span = match phase {
            model::PolicyPhase::Check => self.policies.check,
            model::PolicyPhase::Quality => self.policies.quality,
        };
        Values::new(span, &self.policies.meter, handler)
    }
}
impl<'a> authored::ValidationSource<'a> for ValidationView<'a> {
    type Contributors<'s>
        = Values<'s, 's, ParticipantId>
    where
        Self: 's;
    fn fields(&self) -> authored::ValidationFields<'a> {
        self.fields
    }
    fn contributor_count(&self) -> usize {
        self.contributors.count
    }
    fn contributors(&self) -> Self::Contributors<'_> {
        Values::new(self.contributors, &self.policies.meter, contributor)
    }
}
impl<'a> DeclarationView<'a> {
    pub(super) fn read(cursor: &mut Cursor<'a>) -> Result<Self, CodecError> {
        let binding = fixed::binding(cursor)?;
        let (body, policies) = common(cursor)?;
        Ok(Self {
            fields: model::DeclarationFields {
                binding,
                claim: body.claim,
                issuer: body.issuer,
                declaration_index: body.declaration_index,
                kind: body.kind,
                phase: body.phase,
                mode: body.mode,
                target: body.target,
                program: body.program,
                deadline: body.deadline,
            },
            policies,
        })
    }
    pub(super) fn set_visits(&mut self, visits: usize) {
        self.policies.meter.reset(visits);
    }
    pub(super) fn remaining_visits(&self) -> usize {
        self.policies.meter.remaining()
    }
    pub(super) fn single_pass_visits(&self) -> Result<usize, CodecError> {
        self.policies.visits()
    }
}
impl<'a> ValidationView<'a> {
    pub(super) fn read(cursor: &mut Cursor<'a>) -> Result<Self, CodecError> {
        let ledger = fixed::ledger(cursor)?;
        let id = ValidationId(cursor.fixed()?);
        let schema = cursor.u16()?;
        let (body, policies) = common(cursor)?;
        let description = cursor.text(cursor.remaining())?;
        let quality_bar = if option(cursor)? {
            Some(cursor.text(cursor.remaining())?)
        } else {
            None
        };
        let contributors = Span::read(cursor, 16)?;
        let policy_revision = cursor.u64()?;
        Ok(Self {
            fields: authored::ValidationFields {
                ledger,
                id,
                schema,
                claim: body.claim,
                issuer: body.issuer,
                declaration_index: body.declaration_index,
                kind: body.kind,
                phase: body.phase,
                mode: body.mode,
                target: body.target,
                program: body.program,
                deadline: body.deadline,
                description,
                quality_bar,
                policy_revision,
            },
            policies,
            contributors,
        })
    }
    pub(super) fn set_visits(&mut self, visits: usize) {
        self.policies.meter.reset(visits);
    }
    pub(super) fn remaining_visits(&self) -> usize {
        self.policies.meter.remaining()
    }
    pub(super) fn build_visits(&self) -> Result<usize, CodecError> {
        // The authored model rechecks the source, prepares/copies its underlying
        // declaration, and copies contributors before validating owned output.
        self.policies
            .visits()?
            .checked_mul(3)
            .and_then(|n| {
                self.contributors
                    .count
                    .checked_mul(34)
                    .and_then(|c| n.checked_add(c))
            })
            .ok_or(CodecError::Capacity)
    }
}
