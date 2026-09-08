//! Repeatable authored fields and ordered policy/contributor streams. Input
//! adapters account for their own decoding work in addition to model visits.
use super::*;
use crate::lifecycle::graph::VisitBudget;
use validation::{DeclarationFields, DeclarationSource, PolicyPhase, PolicySource, ProgramFields};

#[cfg(test)]
#[path = "validation_descriptor_source_tests.rs"]
mod tests;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ValidationFields<'a> {
    pub ledger: LedgerId,
    pub id: ValidationId,
    pub schema: u16,
    pub claim: ClaimId,
    pub issuer: ParticipantId,
    pub declaration_index: u32,
    pub kind: ValidationKind,
    pub phase: ValidationPhase,
    pub mode: ValidationMode,
    pub target: validation::TargetDeclaration<'a>,
    pub program: ProgramFields,
    pub deadline: Deadline,
    pub description: &'a str,
    pub quality_bar: Option<&'a str>,
    pub policy_revision: u64,
}

impl<'a> ValidationFields<'a> {
    pub(super) fn declaration(self, content: ContentHash) -> DeclarationFields<'a> {
        DeclarationFields {
            binding: Binding {
                ledger: self.ledger,
                object: ObjectId(self.id.0),
                content,
                revision: ObjectRevision(1),
            },
            claim: self.claim,
            issuer: self.issuer,
            declaration_index: self.declaration_index,
            kind: self.kind,
            phase: self.phase,
            mode: self.mode,
            target: self.target,
            program: self.program,
            deadline: self.deadline,
        }
    }
}

/// Factories and individual iterator steps must be bounded and allocation-free.
/// Every declared stream must produce exactly its count followed by `None`.
pub trait ValidationSource<'a>: PolicySource {
    type Contributors<'s>: Iterator<Item = Result<ParticipantId, ContractError>>
    where
        Self: 's;
    fn fields(&self) -> ValidationFields<'a>;
    fn contributor_count(&self) -> usize;
    fn contributors(&self) -> Self::Contributors<'_>;
}

impl<'a> PolicySource for ValidationSpec<'a> {
    type Handlers<'s>
        = std::iter::Map<
        std::iter::Copied<std::slice::Iter<'a, validation::HandlerPolicy<'a>>>,
        fn(validation::HandlerPolicy<'a>) -> Result<validation::HandlerValue, ContractError>,
    >
    where
        Self: 's;

    fn handlers(&self, phase: PolicyPhase) -> Self::Handlers<'_> {
        let values = match (self.program, phase) {
            (
                validation::Program::Programmatic { check, .. }
                | validation::Program::Agentic { check },
                PolicyPhase::Check,
            ) => check.handlers,
            (
                validation::Program::Programmatic {
                    quality: Some(quality),
                    ..
                },
                PolicyPhase::Quality,
            ) => quality.handlers,
            _ => &[],
        };
        values.iter().copied().map(|value| Ok(value.into()))
    }
}

impl<'a> ValidationSource<'a> for ValidationSpec<'a> {
    type Contributors<'s>
        = std::iter::Map<
        std::iter::Copied<std::slice::Iter<'a, ParticipantId>>,
        fn(ParticipantId) -> Result<ParticipantId, ContractError>,
    >
    where
        Self: 's;
    fn fields(&self) -> ValidationFields<'a> {
        ValidationFields {
            ledger: self.ledger,
            id: self.id,
            schema: self.schema,
            claim: self.claim,
            issuer: self.issuer,
            declaration_index: self.declaration_index,
            kind: self.kind,
            phase: self.phase,
            mode: self.mode,
            target: self.target,
            program: self.program.into(),
            deadline: self.deadline,
            description: self.description,
            quality_bar: self.quality_bar,
            policy_revision: self.policy_revision,
        }
    }
    fn contributor_count(&self) -> usize {
        self.contributed_by.len()
    }
    fn contributors(&self) -> Self::Contributors<'_> {
        self.contributed_by.iter().copied().map(Ok)
    }
}

struct DeclarationAdapter<'s, 'a, S> {
    source: &'s S,
    fields: DeclarationFields<'a>,
}
impl<S: PolicySource> PolicySource for DeclarationAdapter<'_, '_, S> {
    type Handlers<'s>
        = S::Handlers<'s>
    where
        Self: 's;
    fn handlers(&self, phase: PolicyPhase) -> Self::Handlers<'_> {
        self.source.handlers(phase)
    }
}
impl<'a, S: PolicySource> DeclarationSource<'a> for DeclarationAdapter<'_, 'a, S> {
    fn fields(&self) -> DeclarationFields<'a> {
        self.fields
    }
}

struct OwnedSource<'a> {
    descriptor: &'a ValidationDescriptor,
    declaration: &'a validation::Declaration,
}
impl<'a> PolicySource for OwnedSource<'a> {
    type Handlers<'s>
        = <&'a validation::Declaration as PolicySource>::Handlers<'s>
    where
        Self: 's;
    fn handlers(&self, phase: PolicyPhase) -> Self::Handlers<'_> {
        self.declaration.handlers(phase)
    }
}
impl<'a> ValidationSource<'a> for OwnedSource<'a> {
    type Contributors<'s>
        = std::iter::Map<
        std::iter::Copied<std::slice::Iter<'a, ParticipantId>>,
        fn(ParticipantId) -> Result<ParticipantId, ContractError>,
    >
    where
        Self: 's;
    fn fields(&self) -> ValidationFields<'a> {
        let fields = self.declaration.fields();
        ValidationFields {
            ledger: fields.binding.ledger,
            id: ValidationId(fields.binding.object.0),
            schema: self.descriptor.schema,
            claim: fields.claim,
            issuer: fields.issuer,
            declaration_index: fields.declaration_index,
            kind: fields.kind,
            phase: fields.phase,
            mode: fields.mode,
            target: fields.target,
            program: fields.program,
            deadline: fields.deadline,
            description: &self.descriptor.description,
            quality_bar: self.descriptor.quality_bar.as_deref(),
            policy_revision: self.descriptor.policy_revision,
        }
    }
    fn contributor_count(&self) -> usize {
        self.descriptor.contributed_by.len()
    }
    fn contributors(&self) -> Self::Contributors<'_> {
        self.descriptor.contributed_by.iter().copied().map(Ok)
    }
}

#[derive(Debug, Clone, Copy)]
pub(super) struct Shape {
    pub(super) content_hash: ContentHash,
    pub(super) specification_hash: ContentHash,
    pub(super) heap: usize,
    pub(super) allocations: usize,
    pub(super) charge: usize,
    pub(super) inspection_visits: usize,
    pub(super) build_visits: usize,
    checked_visits: usize,
    contributors: usize,
    attempts: u32,
}

#[derive(Debug)]
pub struct ValidationSourcePlan<'s, 'a, S: ValidationSource<'a>> {
    source: &'s S,
    principal: Principal,
    fields: ValidationFields<'a>,
    limits: Limits,
    shape: Shape,
}
impl<'s, 'a, S: ValidationSource<'a>> ValidationSourcePlan<'s, 'a, S> {
    pub fn fields(&self) -> ValidationFields<'a> {
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
    pub fn content_hash(&self) -> ContentHash {
        self.shape.content_hash
    }
    pub fn specification_hash(&self) -> ContentHash {
        self.shape.specification_hash
    }
    pub fn intent_fingerprint(&self) -> ContentHash {
        hash::intent(self.fields.declaration(self.shape.content_hash).binding)
    }
    pub fn attempt_bound(&self) -> u32 {
        self.shape.attempts
    }
    pub fn inspection_visits(&self) -> usize {
        self.shape.inspection_visits
    }
    /// Exact additional model allowance for deriving checked declaration
    /// metadata. The source adapter separately charges its parsing work.
    pub fn checked_declaration_visits(&self) -> usize {
        self.shape.checked_visits
    }

    pub(in crate::lifecycle) fn checked_declaration_parts(
        &self,
        max_visits: usize,
    ) -> Result<(DeclarationFields<'a>, validation::DefinitionStamp, usize), ContractError> {
        bytes::fits(self.shape.checked_visits, max_visits)?;
        let (fields, actual, stamp) = inspect_inner(
            self.principal,
            self.source,
            self.limits,
            self.shape.checked_visits,
            Some(self.shape.content_hash),
        )?;
        if hash::intent(fields.declaration(actual.content_hash).binding)
            != self.intent_fingerprint()
            || actual.specification_hash != self.shape.specification_hash
            || actual.heap != self.shape.heap
            || actual.allocations != self.shape.allocations
            || actual.contributors != self.shape.contributors
            || actual.attempts != self.shape.attempts
            || actual.checked_visits != self.shape.checked_visits
        {
            return Err(ContractError::ContentConflict);
        }
        Ok((
            fields.declaration(actual.content_hash),
            stamp.ok_or(ContractError::InvalidPolicy)?,
            actual.checked_visits,
        ))
    }
    /// Conservative complete model allowance, charged before construction.
    pub fn build_visits(&self) -> usize {
        self.shape.build_visits
    }
    pub fn build(
        self,
        max_bytes: usize,
        max_visits: usize,
    ) -> Result<ValidationDescriptor, ContractError> {
        build(
            self.source,
            self.principal,
            self.fields,
            self.limits,
            self.shape,
            max_bytes,
            max_visits,
        )
    }
}
impl ValidationDescriptor {
    pub fn prepare_source<'s, 'a, S: ValidationSource<'a>>(
        principal: Principal,
        source: &'s S,
        limits: Limits,
        max_visits: usize,
    ) -> Result<ValidationSourcePlan<'s, 'a, S>, ContractError> {
        let (fields, shape) = inspect(principal, source, limits, max_visits)?;
        Ok(ValidationSourcePlan {
            source,
            principal,
            fields,
            limits,
            shape,
        })
    }
}

fn multiply(left: usize, right: usize) -> Result<usize, ContractError> {
    left.checked_mul(right).ok_or(ContractError::Capacity)
}

fn text(value: &str, limit: usize) -> Result<(), ContractError> {
    bytes::fits(value.len(), limit)?;
    if value.trim().is_empty() || value.contains('\0') {
        return Err(ContractError::InvalidPolicy);
    }
    Ok(())
}

pub(super) fn inspect<'a, S: ValidationSource<'a>>(
    principal: Principal,
    source: &S,
    limits: Limits,
    max_visits: usize,
) -> Result<(ValidationFields<'a>, Shape), ContractError> {
    let (fields, shape, _) = inspect_inner(principal, source, limits, max_visits, None)?;
    Ok((fields, shape))
}

fn inspect_inner<'a, S: ValidationSource<'a>>(
    principal: Principal,
    source: &S,
    limits: Limits,
    max_visits: usize,
    stamp_content: Option<ContentHash>,
) -> Result<
    (
        ValidationFields<'a>,
        Shape,
        Option<validation::DefinitionStamp>,
    ),
    ContractError,
> {
    let mut visits = VisitBudget::new(max_visits);
    visits.charge(2)?;
    let fields = source.fields();
    let contributors = source.contributor_count();
    bytes::fits(fields.description.len(), limits.description_bytes)?;
    bytes::fits(
        fields.quality_bar.map_or(0, str::len),
        limits.quality_bar_bytes,
    )?;
    bytes::fits(contributors, limits.contributors)?;
    let text_bytes = bytes::add(
        fields.description.len(),
        fields.quality_bar.map_or(0, str::len),
    )?;
    let handlers = match fields.program {
        ProgramFields::Delivery => 0,
        ProgramFields::Agentic { check } => check.handlers,
        ProgramFields::Programmatic { check, quality } => {
            bytes::add(check.handlers, quality.map_or(0, |phase| phase.handlers))?
        }
    };
    let slot_bytes = match fields.target {
        validation::TargetDeclaration::WholeWorkSlot { name, .. } => name.len(),
        _ => 0,
    };
    let stamp_work = bytes::add(
        4096,
        bytes::add(multiply(2, slot_bytes)?, multiply(512, handlers)?)?,
    )?;
    let added_work = if stamp_content.is_some() {
        stamp_work
    } else {
        0
    };
    visits.charge(added_work)?;
    // The shared visitor covers base policy checking. This additional bound
    // covers both authored hashes, text scans and all contributor comparisons.
    let text_work = multiply(4, bytes::add(text_bytes, slot_bytes)?)?;
    let row_work = bytes::add(multiply(1024, handlers)?, multiply(128, contributors)?)?;
    visits.charge(bytes::add(4096, bytes::add(text_work, row_work)?)?)?;
    if fields.schema != 1 || fields.policy_revision == 0 {
        return Err(ContractError::InvalidPolicy);
    }
    text(fields.description, limits.description_bytes)?;
    if let Some(quality) = fields.quality_bar {
        text(quality, limits.quality_bar_bytes)?;
    }
    if matches!(fields.program, ProgramFields::Delivery) && fields.quality_bar.is_some() {
        return Err(ContractError::InvalidPolicy);
    }
    let slots = validation::check_declaration_fields(
        principal,
        fields.declaration(ContentHash([0; 32])),
        limits.declaration,
        &mut visits,
    )?;
    let mut hash = hash::Stream::new(fields)?;
    let mut stamp = stamp_content
        .map(|content| validation::stamp_begin(fields.declaration(content)))
        .transpose()?;
    let program = validation::visit_program(
        fields.program,
        source,
        limits.declaration,
        &mut visits,
        |event| {
            hash.policy(event)?;
            if let Some(stamp) = stamp.as_mut() {
                stamp.event(event)?;
            }
            Ok(())
        },
    )?;
    hash.authored(fields, contributors)?;
    visits.charge(1)?;
    let mut rows = source.contributors();
    let mut previous = None;
    for _ in 0..contributors {
        visits.charge(1)?;
        let participant = rows.next().ok_or(ContractError::InvalidManifest)??;
        if participant.is_zero() || previous.is_some_and(|previous| previous >= participant) {
            return Err(ContractError::InvalidPolicy);
        }
        hash.contributor(participant);
        previous = Some(participant);
    }
    visits.charge(1)?;
    match rows.next() {
        None => {}
        Some(Err(error)) => return Err(error),
        Some(Ok(_)) => return Err(ContractError::InvalidManifest),
    }
    let (content_hash, specification_hash) = hash.finish(fields.policy_revision);
    let heap = bytes::add(
        bytes::add(slots, program.heap)?,
        memory::authored_heap(
            fields.description.len(),
            fields.quality_bar.map_or(0, str::len),
            contributors,
        )?,
    )?;
    let allocations = bytes::add(
        bytes::add(bytes::allocation::<u8>(slots), program.allocations)?,
        memory::authored_allocations(
            fields.description.len(),
            fields.quality_bar.map_or(0, str::len),
            contributors,
        )?,
    )?;
    let charge = bytes::total::<ValidationDescriptor>(heap)?;
    bytes::fits(charge, limits.construction_bytes)?;
    let inspection_visits = max_visits
        .checked_sub(visits.remaining())
        .and_then(|used| used.checked_sub(added_work))
        .ok_or(ContractError::Capacity)?;
    let checked_visits = bytes::add(inspection_visits, stamp_work)?;
    let build_visits = bytes::add(
        multiply(16, inspection_visits)?,
        bytes::add(multiply(8, heap)?, 256)?,
    )?;
    Ok((
        fields,
        Shape {
            content_hash,
            specification_hash,
            heap,
            allocations,
            charge,
            inspection_visits,
            build_visits,
            checked_visits,
            contributors,
            attempts: program.attempts,
        },
        stamp.map(|stamp| stamp.finish()),
    ))
}

pub(super) fn build<'a, S: ValidationSource<'a>>(
    source: &S,
    principal: Principal,
    expected: ValidationFields<'a>,
    limits: Limits,
    shape: Shape,
    max_bytes: usize,
    max_visits: usize,
) -> Result<ValidationDescriptor, ContractError> {
    bytes::fits(shape.charge, max_bytes)?;
    bytes::fits(shape.build_visits, max_visits)?;
    let mut visits = VisitBudget::new(shape.build_visits);
    let (fields, actual) = inspect(principal, source, limits, visits.remaining())?;
    visits.charge(actual.inspection_visits)?;
    if hash::intent(fields.declaration(actual.content_hash).binding)
        != hash::intent(expected.declaration(shape.content_hash).binding)
        || actual.specification_hash != shape.specification_hash
        || actual.heap != shape.heap
        || actual.allocations != shape.allocations
        || actual.contributors != shape.contributors
    {
        return Err(ContractError::ContentConflict);
    }
    let adapter = DeclarationAdapter {
        source,
        fields: fields.declaration(shape.content_hash),
    };
    let declaration = validation::Declaration::prepare_source(
        principal,
        &adapter,
        limits.declaration,
        visits.remaining(),
    )?;
    visits.charge(declaration.inspection_visits())?;
    let declaration_bytes = declaration.construction_charge();
    let declaration_visits = declaration.build_visits();
    visits.charge(declaration_visits)?;
    let declaration = declaration.build(declaration_bytes, declaration_visits)?;
    visits.charge(bytes::add(
        16,
        multiply(
            2,
            memory::authored_heap(
                fields.description.len(),
                fields.quality_bar.map_or(0, str::len),
                shape.contributors,
            )?,
        )?,
    )?)?;
    let description = memory::string(fields.description)?;
    let quality_bar = fields.quality_bar.map(memory::string).transpose()?;
    let mut contributed_by = bytes::reserve::<ParticipantId>(shape.contributors)?;
    bytes::fits(contributed_by.capacity(), shape.contributors)?;
    visits.charge(1)?;
    let mut rows = source.contributors();
    for _ in 0..shape.contributors {
        visits.charge(1)?;
        let participant = rows.next().ok_or(ContractError::InvalidManifest)??;
        if contributed_by.len() == contributed_by.capacity() {
            return Err(ContractError::Capacity);
        }
        contributed_by.push(participant);
    }
    visits.charge(1)?;
    match rows.next() {
        None => {}
        Some(Err(error)) => return Err(error),
        Some(Ok(_)) => return Err(ContractError::InvalidManifest),
    }
    let descriptor = ValidationDescriptor {
        schema: fields.schema,
        declaration,
        description,
        quality_bar,
        contributed_by,
        policy_revision: fields.policy_revision,
        specification_hash: shape.specification_hash,
    };
    let owned = OwnedSource {
        declaration: &descriptor.declaration,
        descriptor: &descriptor,
    };
    let (copied, checked) = inspect(principal, &owned, limits, visits.remaining())?;
    visits.charge(checked.inspection_visits)?;
    if hash::intent(copied.declaration(checked.content_hash).binding)
        != hash::intent(expected.declaration(shape.content_hash).binding)
        || checked.specification_hash != shape.specification_hash
        || checked.attempts != shape.attempts
        || descriptor.binding() != copied.declaration(checked.content_hash).binding
    {
        return Err(ContractError::ContentConflict);
    }
    visits.charge(16)?;
    bytes::fits(descriptor.retained_bytes()?, shape.charge)?;
    bytes::fits(descriptor.retained_heap_bytes()?, shape.heap)?;
    bytes::fits(descriptor.heap_allocations()?, shape.allocations)?;
    Ok(descriptor)
}
