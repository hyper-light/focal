//! Borrowed scalar declarations and repeatable ordered handler streams. Model
//! inspection and construction allocate no temporary policy/handler arrays.
use super::*;
use crate::lifecycle::{graph::VisitBudget, memory as bytes};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HandlerValue {
    pub id: ValidatorId,
    pub version: ContentHash,
    pub agentic: bool,
    pub attempts: u32,
    pub proof_schema: ContentHash,
    pub diagnostic_schema: ContentHash,
}
impl From<HandlerPolicy<'_>> for HandlerValue {
    fn from(value: HandlerPolicy<'_>) -> Self {
        Self {
            id: value.handler.id,
            version: value.handler.version,
            agentic: value.handler.agentic,
            attempts: value.attempts,
            proof_schema: value.proof_schema,
            diagnostic_schema: value.diagnostic_schema,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PhaseFields {
    pub evaluator: ParticipantId,
    pub definition: ContentHash,
    pub required_policy: Option<ContentHash>,
    pub handlers: usize,
}
impl From<PhasePolicy<'_>> for PhaseFields {
    fn from(value: PhasePolicy<'_>) -> Self {
        Self {
            evaluator: value.evaluator,
            definition: value.definition,
            required_policy: value.required_policy,
            handlers: value.handlers.len(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProgramFields {
    Delivery,
    Programmatic {
        check: PhaseFields,
        quality: Option<PhaseFields>,
    },
    Agentic {
        check: PhaseFields,
    },
}
impl From<Program<'_>> for ProgramFields {
    fn from(value: Program<'_>) -> Self {
        match value {
            Program::Delivery => Self::Delivery,
            Program::Programmatic { check, quality } => Self::Programmatic {
                check: check.into(),
                quality: quality.map(Into::into),
            },
            Program::Agentic { check } => Self::Agentic {
                check: check.into(),
            },
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PolicyPhase {
    Check,
    Quality,
}

/// Factories and each step must be bounded and allocation-free. Adapter parsing
/// has its own allowance; model visits cover checking, hashing and owned copies.
/// Every phase, including absent phases, is checked for exact declared cardinality.
pub trait PolicySource {
    type Handlers<'s>: Iterator<Item = Result<HandlerValue, ContractError>>
    where
        Self: 's;
    fn handlers(&self, phase: PolicyPhase) -> Self::Handlers<'_>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DeclarationFields<'a> {
    pub binding: Binding,
    pub claim: ClaimId,
    pub issuer: ParticipantId,
    pub declaration_index: u32,
    pub kind: ValidationKind,
    pub phase: ValidationPhase,
    pub mode: ValidationMode,
    pub target: TargetDeclaration<'a>,
    pub program: ProgramFields,
    pub deadline: Deadline,
}
pub trait DeclarationSource<'a>: PolicySource {
    fn fields(&self) -> DeclarationFields<'a>;
}

#[derive(Debug)]
pub struct DeclarationSourcePlan<'s, 'a, S: DeclarationSource<'a>> {
    source: &'s S,
    principal: Principal,
    fields: DeclarationFields<'a>,
    limits: Limits,
    shape: Shape,
}
impl<'s, 'a, S: DeclarationSource<'a>> DeclarationSourcePlan<'s, 'a, S> {
    pub fn fields(&self) -> DeclarationFields<'a> {
        self.fields
    }
    pub fn construction_charge(&self) -> usize {
        self.shape.charge
    }
    pub fn construction_heap_bytes(&self) -> usize {
        self.shape.heap
    }
    pub fn construction_heap_allocations(&self) -> usize {
        self.shape.allocations
    }
    pub fn attempt_bound(&self) -> u32 {
        self.shape.attempts
    }
    pub fn intent_fingerprint(&self) -> ContentHash {
        self.shape.stamp.intent_fingerprint(self.shape.attempts)
    }
    pub(in crate::lifecycle::validation) fn checked_stamp(&self) -> DefinitionStamp {
        self.shape.stamp
    }
    pub fn inspection_visits(&self) -> usize {
        self.shape.inspection_visits
    }
    pub fn build_visits(&self) -> usize {
        self.shape.build_visits
    }
    pub fn build(self, max_bytes: usize, max_visits: usize) -> Result<Declaration, ContractError> {
        build(
            self.source,
            self.principal,
            self.limits,
            self.shape,
            max_bytes,
            max_visits,
        )
    }
}
impl Declaration {
    pub fn prepare_source<'s, 'a, S: DeclarationSource<'a>>(
        principal: Principal,
        source: &'s S,
        limits: Limits,
        max_visits: usize,
    ) -> Result<DeclarationSourcePlan<'s, 'a, S>, ContractError> {
        let (fields, shape) = inspect(principal, source, limits, max_visits)?;
        Ok(DeclarationSourcePlan {
            source,
            principal,
            fields,
            limits,
            shape,
        })
    }
}

#[derive(Debug, Clone, Copy)]
pub(in crate::lifecycle) enum PolicyEvent {
    Program(ProgramFields),
    Phase(PhaseFields),
    Handler(PolicyPhase, HandlerValue),
    Quality(bool),
}
#[derive(Debug, Clone, Copy)]
pub(in crate::lifecycle) struct ProgramShape {
    pub heap: usize,
    pub allocations: usize,
    pub attempts: u32,
    pub handlers: usize,
}

fn multiply(left: usize, right: usize) -> Result<usize, ContractError> {
    left.checked_mul(right).ok_or(ContractError::Capacity)
}

pub(in crate::lifecycle) fn check_declaration_fields(
    principal: Principal,
    fields: DeclarationFields<'_>,
    limits: Limits,
    visits: &mut VisitBudget,
) -> Result<usize, ContractError> {
    let slots = match fields.target {
        TargetDeclaration::WholeWorkSlot { name, .. } => {
            if name.is_empty() || name.len() > limits.slot_bytes {
                return Err(ContractError::InvalidTarget);
            }
            name.len()
        }
        _ => 0,
    };
    visits.charge(bytes::add(4096, multiply(2, slots)?)?)?;
    principal.require_actor(fields.issuer)?;
    if fields.claim.is_zero()
        || fields.issuer.is_zero()
        || fields.binding.object.is_zero()
        || fields.binding.ledger.tenant.is_zero()
        || fields.binding.ledger.session.is_zero()
        || fields.deadline.timer.is_zero()
        || fields.deadline.generation == 0
    {
        return Err(ContractError::InvalidPolicy);
    }
    let phase = match fields.target {
        TargetDeclaration::WholeWorkSlot { .. } | TargetDeclaration::Delivery => {
            ValidationPhase::WholeWork
        }
        TargetDeclaration::Admission => ValidationPhase::Admission,
        TargetDeclaration::Increment => ValidationPhase::Increment,
    };
    if fields.phase != phase {
        return Err(ContractError::InvalidTarget);
    }
    match fields.program {
        ProgramFields::Delivery => {
            if fields.kind != ValidationKind::Receipt
                || fields.mode != ValidationMode::Required
                || fields.target != TargetDeclaration::Delivery
            {
                return Err(ContractError::InvalidPolicy);
            }
        }
        ProgramFields::Programmatic { .. } | ProgramFields::Agentic { .. } => {
            if fields.kind == ValidationKind::Receipt
                || fields.target == TargetDeclaration::Delivery
            {
                return Err(ContractError::InvalidPolicy);
            }
        }
    }
    Ok(slots)
}

fn phases(program: ProgramFields) -> (Option<(PhaseFields, bool)>, Option<PhaseFields>) {
    match program {
        ProgramFields::Delivery => (None, None),
        ProgramFields::Programmatic { check, quality } => (Some((check, false)), quality),
        ProgramFields::Agentic { check } => (Some((check, true)), None),
    }
}

fn program_shape(program: ProgramFields, limits: Limits) -> Result<ProgramShape, ContractError> {
    let (check, quality) = phases(program);
    let mut handlers = 0;
    let mut allocations = 0;
    for policy in [check.map(|(fields, _)| fields), quality]
        .into_iter()
        .flatten()
    {
        if policy.evaluator.is_zero() || policy.handlers == 0 {
            return Err(ContractError::InvalidPolicy);
        }
        let count = u32::try_from(policy.handlers).map_err(|_| ContractError::Capacity)?;
        if count > limits.handlers {
            return Err(ContractError::Capacity);
        }
        handlers = bytes::add(handlers, policy.handlers)?;
        allocations = bytes::add(allocations, 1)?;
    }
    Ok(ProgramShape {
        heap: bytes::array::<OwnedHandlerPolicy>(handlers)?,
        allocations,
        attempts: 0,
        handlers,
    })
}

fn next<T>(
    values: &mut impl Iterator<Item = Result<T, ContractError>>,
    visits: &mut VisitBudget,
) -> Result<T, ContractError> {
    visits.charge(1)?;
    values.next().ok_or(ContractError::InvalidPolicy)?
}
fn end<T>(
    values: &mut impl Iterator<Item = Result<T, ContractError>>,
    visits: &mut VisitBudget,
) -> Result<(), ContractError> {
    visits.charge(1)?;
    match values.next() {
        None => Ok(()),
        Some(Err(error)) => Err(error),
        Some(Ok(_)) => Err(ContractError::InvalidPolicy),
    }
}

/// Check actual handler values and send those same values to the hash consumer.
/// The fixed visit charges include both native descriptor identity hash streams;
/// no consumer may traverse a replacement handler source behind this visitor.
pub(in crate::lifecycle) fn visit_program<S: PolicySource>(
    program: ProgramFields,
    source: &S,
    limits: Limits,
    visits: &mut VisitBudget,
    mut visitor: impl FnMut(PolicyEvent) -> Result<(), ContractError>,
) -> Result<ProgramShape, ContractError> {
    visits.charge(1)?;
    let mut shape = program_shape(program, limits)?;
    visitor(PolicyEvent::Program(program))?;
    let (check, quality) = phases(program);
    for (phase, policy) in [
        (PolicyPhase::Check, check),
        (PolicyPhase::Quality, quality.map(|fields| (fields, true))),
    ] {
        if phase == PolicyPhase::Quality && matches!(program, ProgramFields::Programmatic { .. }) {
            visits.charge(1)?;
            visitor(PolicyEvent::Quality(policy.is_some()))?;
        }
        visits.charge(1)?;
        let mut values = source.handlers(phase);
        if let Some((fields, agentic)) = policy {
            visits.charge(512)?;
            visitor(PolicyEvent::Phase(fields))?;
            for _ in 0..fields.handlers {
                let value = next(&mut values, visits)?;
                visits.charge(512)?;
                if value.attempts == 0 || value.id.is_zero() || value.agentic != agentic {
                    return Err(ContractError::InvalidPolicy);
                }
                shape.attempts = shape
                    .attempts
                    .checked_add(value.attempts)
                    .ok_or(ContractError::Capacity)?;
                if shape.attempts > limits.attempts {
                    return Err(ContractError::Capacity);
                }
                visitor(PolicyEvent::Handler(phase, value))?;
            }
        }
        end(&mut values, visits)?;
    }
    Ok(shape)
}

#[derive(Debug, Clone, Copy)]
pub(super) struct Shape {
    pub(super) stamp: DefinitionStamp,
    pub(super) attempts: u32,
    pub(super) heap: usize,
    pub(super) allocations: usize,
    pub(super) charge: usize,
    pub(super) inspection_visits: usize,
    pub(super) build_visits: usize,
}

pub(super) fn inspect<'a, S: DeclarationSource<'a>>(
    principal: Principal,
    source: &S,
    limits: Limits,
    max_visits: usize,
) -> Result<(DeclarationFields<'a>, Shape), ContractError> {
    let mut visits = VisitBudget::new(max_visits);
    visits.charge(1)?;
    let fields = source.fields();
    let slots = check_declaration_fields(principal, fields, limits, &mut visits)?;
    let mut stamp = stamp_begin(fields)?;
    let program = visit_program(fields.program, source, limits, &mut visits, |event| {
        stamp.event(event)
    })?;
    let heap = bytes::add(slots, program.heap)?;
    let allocations = bytes::add(bytes::allocation::<u8>(slots), program.allocations)?;
    let inspection_visits = max_visits
        .checked_sub(visits.remaining())
        .ok_or(ContractError::Capacity)?;
    let build_visits = bytes::add(
        multiply(3, inspection_visits)?,
        bytes::add(
            multiply(2, heap)?,
            bytes::add(multiply(4, program.handlers)?, 32)?,
        )?,
    )?;
    Ok((
        fields,
        Shape {
            stamp: DefinitionStamp(*stamp.0.finalize().as_bytes()),
            attempts: program.attempts,
            heap,
            allocations,
            charge: bytes::total::<Declaration>(heap)?,
            inspection_visits,
            build_visits,
        },
    ))
}

fn empty_phase(fields: PhaseFields) -> Result<OwnedPhasePolicy, ContractError> {
    Ok(OwnedPhasePolicy {
        evaluator: fields.evaluator,
        definition: fields.definition,
        required_policy: fields.required_policy,
        handlers: construction_buffer(fields.handlers)?,
    })
}
fn empty_program(fields: ProgramFields) -> Result<OwnedProgram, ContractError> {
    Ok(match fields {
        ProgramFields::Delivery => OwnedProgram::Delivery,
        ProgramFields::Programmatic { check, quality } => OwnedProgram::Programmatic {
            check: empty_phase(check)?,
            quality: quality.map(empty_phase).transpose()?,
        },
        ProgramFields::Agentic { check } => OwnedProgram::Agentic {
            check: empty_phase(check)?,
        },
    })
}
fn append(
    program: &mut OwnedProgram,
    phase: PolicyPhase,
    value: HandlerValue,
) -> Result<(), ContractError> {
    let policy = match (program, phase) {
        (
            OwnedProgram::Programmatic { check, .. } | OwnedProgram::Agentic { check },
            PolicyPhase::Check,
        ) => check,
        (
            OwnedProgram::Programmatic {
                quality: Some(quality),
                ..
            },
            PolicyPhase::Quality,
        ) => quality,
        _ => return Err(ContractError::InvalidPolicy),
    };
    if policy.handlers.len() == policy.handlers.capacity() {
        return Err(ContractError::Capacity);
    }
    policy.handlers.push(OwnedHandlerPolicy {
        handler: HandlerRef {
            id: value.id,
            version: value.version,
            agentic: value.agentic,
        },
        attempts: value.attempts,
        proof_schema: value.proof_schema,
        diagnostic_schema: value.diagnostic_schema,
    });
    Ok(())
}

pub(super) fn build<'a, S: DeclarationSource<'a>>(
    source: &S,
    principal: Principal,
    limits: Limits,
    shape: Shape,
    max_bytes: usize,
    max_visits: usize,
) -> Result<Declaration, ContractError> {
    bytes::fits(shape.charge, max_bytes)?;
    bytes::fits(shape.build_visits, max_visits)?;
    let mut visits = VisitBudget::new(shape.build_visits);
    visits.charge(1)?;
    let fields = source.fields();
    let slots = check_declaration_fields(principal, fields, limits, &mut visits)?;
    let program = program_shape(fields.program, limits)?;
    let heap = bytes::add(slots, program.heap)?;
    bytes::fits(heap, shape.heap)?;
    bytes::fits(
        bytes::add(bytes::allocation::<u8>(slots), program.allocations)?,
        shape.allocations,
    )?;
    visits.charge(bytes::add(heap, 8)?)?;
    let target = OwnedTarget::build(fields.target)?;
    let mut program = empty_program(fields.program)?;
    let actual = visit_program(fields.program, source, limits, &mut visits, |event| {
        if let PolicyEvent::Handler(phase, value) = event {
            append(&mut program, phase, value)?;
        }
        Ok(())
    })?;
    let mut declaration = Declaration {
        spec: OwnedSpec {
            binding: fields.binding,
            claim: fields.claim,
            issuer: fields.issuer,
            declaration_index: fields.declaration_index,
            kind: fields.kind,
            phase: fields.phase,
            mode: fields.mode,
            target,
            program,
            deadline: fields.deadline,
        },
        attempts: actual.attempts,
        stamp: shape.stamp,
    };
    let (_, actual) = inspect(principal, &&declaration, limits, visits.remaining())?;
    visits.charge(actual.inspection_visits)?;
    if actual.stamp != shape.stamp || actual.attempts != shape.attempts {
        return Err(ContractError::ContentConflict);
    }
    visits.charge(16)?;
    bytes::fits(declaration.retained_bytes()?, shape.charge)?;
    bytes::fits(declaration.heap_allocations()?, shape.allocations)?;
    declaration.stamp = actual.stamp;
    Ok(declaration)
}

fn handler_result(value: &HandlerPolicy<'_>) -> Result<HandlerValue, ContractError> {
    Ok((*value).into())
}
impl<'a> PolicySource for DeclarationSpec<'a> {
    type Handlers<'s>
        = std::iter::Map<
        std::slice::Iter<'s, HandlerPolicy<'a>>,
        fn(&HandlerPolicy<'a>) -> Result<HandlerValue, ContractError>,
    >
    where
        Self: 's;
    fn handlers(&self, phase: PolicyPhase) -> Self::Handlers<'_> {
        let values = match (self.program, phase) {
            (
                Program::Programmatic { check, .. } | Program::Agentic { check },
                PolicyPhase::Check,
            ) => check.handlers,
            (
                Program::Programmatic {
                    quality: Some(quality),
                    ..
                },
                PolicyPhase::Quality,
            ) => quality.handlers,
            _ => &[],
        };
        values.iter().map(handler_result)
    }
}
impl<'a> DeclarationSource<'a> for DeclarationSpec<'a> {
    fn fields(&self) -> DeclarationFields<'a> {
        DeclarationFields {
            binding: self.binding,
            claim: self.claim,
            issuer: self.issuer,
            declaration_index: self.declaration_index,
            kind: self.kind,
            phase: self.phase,
            mode: self.mode,
            target: self.target,
            program: self.program.into(),
            deadline: self.deadline,
        }
    }
}

/// Opaque borrowed iterator; owned handler representation remains private.
pub struct DeclarationHandlers<'a> {
    values: std::slice::Iter<'a, OwnedHandlerPolicy>,
}
impl Iterator for DeclarationHandlers<'_> {
    type Item = Result<HandlerValue, ContractError>;
    fn next(&mut self) -> Option<Self::Item> {
        self.values.next().map(|value| {
            Ok(HandlerValue {
                id: value.handler.id,
                version: value.handler.version,
                agentic: value.handler.agentic,
                attempts: value.attempts,
                proof_schema: value.proof_schema,
                diagnostic_schema: value.diagnostic_schema,
            })
        })
    }
}
fn phase_fields(policy: &OwnedPhasePolicy) -> PhaseFields {
    PhaseFields {
        evaluator: policy.evaluator,
        definition: policy.definition,
        required_policy: policy.required_policy,
        handlers: policy.handlers.len(),
    }
}
impl PolicySource for &Declaration {
    type Handlers<'s>
        = DeclarationHandlers<'s>
    where
        Self: 's;
    fn handlers(&self, phase: PolicyPhase) -> Self::Handlers<'_> {
        let values = match (&self.spec.program, phase) {
            (
                OwnedProgram::Programmatic { check, .. } | OwnedProgram::Agentic { check },
                PolicyPhase::Check,
            ) => check.handlers.as_slice(),
            (
                OwnedProgram::Programmatic {
                    quality: Some(quality),
                    ..
                },
                PolicyPhase::Quality,
            ) => quality.handlers.as_slice(),
            _ => &[],
        };
        DeclarationHandlers {
            values: values.iter(),
        }
    }
}
impl<'a> DeclarationSource<'a> for &'a Declaration {
    fn fields(&self) -> DeclarationFields<'a> {
        let value: &'a Declaration = self;
        DeclarationFields {
            binding: value.binding(),
            claim: value.claim(),
            issuer: value.issuer(),
            declaration_index: value.declaration_index(),
            kind: value.kind(),
            phase: value.declared_phase(),
            mode: value.mode(),
            target: value.target(),
            deadline: value.deadline(),
            program: match &value.spec.program {
                OwnedProgram::Delivery => ProgramFields::Delivery,
                OwnedProgram::Programmatic { check, quality } => ProgramFields::Programmatic {
                    check: phase_fields(check),
                    quality: quality.as_ref().map(phase_fields),
                },
                OwnedProgram::Agentic { check } => ProgramFields::Agentic {
                    check: phase_fields(check),
                },
            },
        }
    }
}

#[cfg(test)]
#[path = "validation_definition_source_tests.rs"]
mod tests;
