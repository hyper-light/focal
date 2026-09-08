//! Repeatable value sources for precharged artifact construction. Callers can
//! decode references and borrow labels directly from immutable input bytes.
use super::*;
use crate::lifecycle::graph::VisitBudget;

#[cfg(test)]
#[path = "artifact_descriptor_source_tests.rs"]
mod tests;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ArtifactFields<'a> {
    pub ledger: LedgerId,
    pub id: ArtifactId,
    pub schema: u16,
    pub kind: &'a str,
    pub schema_hash: ContentHash,
    pub metadata: &'a [u8],
    pub payload: PayloadSpec<'a>,
    pub producer: ParticipantId,
    pub receipt: Option<ReceiptFence>,
    pub result: Option<ResultProvenance>,
    pub work: Option<WorkProvenance>,
}

/// Factories and each iterator step must be bounded and allocation-free. A byte
/// adapter must also debit its own parsing work; model visits cover model work.
/// Counts are declarations, not trusted iterator size hints. Every pass requires
/// exactly that many successful values followed by `None`.
pub trait ArtifactSource<'a> {
    type Inputs<'s>: Iterator<Item = Result<ObjectRef, ContractError>>
    where
        Self: 's;
    type Visibility<'s>: Iterator<Item = Result<&'a str, ContractError>>
    where
        Self: 's;
    fn fields(&self) -> ArtifactFields<'a>;
    fn input_count(&self) -> usize;
    fn visibility_count(&self) -> usize;
    fn inputs(&self) -> Self::Inputs<'_>;
    fn visibility(&self) -> Self::Visibility<'_>;
}

#[derive(Debug)]
pub struct ArtifactSourcePlan<'s, 'a, S: ArtifactSource<'a>> {
    source: &'s S,
    fields: ArtifactFields<'a>,
    limits: Limits,
    shape: Shape,
}

impl<'s, 'a, S: ArtifactSource<'a>> ArtifactSourcePlan<'s, 'a, S> {
    pub fn fields(&self) -> ArtifactFields<'a> {
        self.fields
    }
    /// Counts from the successfully inspected body, not fresh source hints.
    pub fn input_count(&self) -> usize {
        self.shape.inputs
    }
    pub fn visibility_count(&self) -> usize {
        self.shape.visibility
    }
    /// Replay the original source without copying references. The caller must
    /// bound the replay by `input_count()` and charge its adapter's work. These
    /// values are not a new authority proof: build rechecks the complete owned
    /// body against this plan before returning it.
    pub fn inputs(&self) -> S::Inputs<'_> {
        self.source.inputs()
    }
    /// Replay borrowed labels under the same source allowance as preparation
    /// and build. As with inputs, callers must enforce the inspected count.
    pub fn visibility(&self) -> S::Visibility<'_> {
        self.source.visibility()
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
    pub fn intent_fingerprint(&self) -> ContentHash {
        intent_fingerprint(self.fields.id, self.shape.content_hash)
    }
    /// Model work only, separate from any work performed by a source adapter.
    pub fn inspection_visits(&self) -> usize {
        self.shape.inspection_visits
    }
    /// Complete model allowance for copy, owned validation/hash and reconciliation.
    pub fn build_visits(&self) -> usize {
        self.shape.build_visits
    }
    pub fn build(
        self,
        max_bytes: usize,
        max_visits: usize,
    ) -> Result<ArtifactDescriptor, ContractError> {
        build(
            self.source,
            self.fields.id,
            self.limits,
            self.shape,
            max_bytes,
            max_visits,
        )
    }
}

impl ArtifactDescriptor {
    pub fn prepare_source<'s, 'a, S: ArtifactSource<'a>>(
        source: &'s S,
        limits: Limits,
        max_visits: usize,
    ) -> Result<ArtifactSourcePlan<'s, 'a, S>, ContractError> {
        let (fields, shape) = inspect(source, limits, max_visits)?;
        Ok(ArtifactSourcePlan {
            source,
            fields,
            limits,
            shape,
        })
    }
}

impl<'a> ArtifactSource<'a> for ArtifactSpec<'a> {
    type Inputs<'s>
        = std::iter::Map<
        std::iter::Copied<std::slice::Iter<'a, ObjectRef>>,
        fn(ObjectRef) -> Result<ObjectRef, ContractError>,
    >
    where
        Self: 's;
    type Visibility<'s>
        = std::iter::Map<
        std::iter::Copied<std::slice::Iter<'a, &'a str>>,
        fn(&'a str) -> Result<&'a str, ContractError>,
    >
    where
        Self: 's;
    fn fields(&self) -> ArtifactFields<'a> {
        ArtifactFields {
            ledger: self.ledger,
            id: self.id,
            schema: self.schema,
            kind: self.kind,
            schema_hash: self.schema_hash,
            metadata: self.metadata,
            payload: self.payload,
            producer: self.producer,
            receipt: self.receipt,
            result: self.result,
            work: self.work,
        }
    }
    fn input_count(&self) -> usize {
        self.inputs.len()
    }
    fn visibility_count(&self) -> usize {
        self.visibility.len()
    }
    fn inputs(&self) -> Self::Inputs<'_> {
        self.inputs.iter().copied().map(Ok)
    }
    fn visibility(&self) -> Self::Visibility<'_> {
        self.visibility.iter().copied().map(Ok)
    }
}

pub(super) struct OwnedSource<'a>(pub(super) &'a ArtifactDescriptor);
impl<'a> ArtifactSource<'a> for OwnedSource<'a> {
    type Inputs<'s>
        = std::iter::Map<
        std::iter::Copied<std::slice::Iter<'a, ObjectRef>>,
        fn(ObjectRef) -> Result<ObjectRef, ContractError>,
    >
    where
        Self: 's;
    type Visibility<'s>
        = std::iter::Map<
        std::slice::Iter<'a, String>,
        fn(&'a String) -> Result<&'a str, ContractError>,
    >
    where
        Self: 's;
    fn fields(&self) -> ArtifactFields<'a> {
        let value = self.0;
        ArtifactFields {
            ledger: value.ledger,
            id: value.id,
            schema: value.schema,
            kind: &value.kind,
            schema_hash: value.schema_hash,
            metadata: &value.metadata,
            payload: value.payload(),
            producer: value.producer,
            receipt: value.receipt,
            result: value.result,
            work: value.work,
        }
    }
    fn input_count(&self) -> usize {
        self.0.inputs.len()
    }
    fn visibility_count(&self) -> usize {
        self.0.visibility.len()
    }
    fn inputs(&self) -> Self::Inputs<'_> {
        self.0.inputs.iter().copied().map(Ok)
    }
    fn visibility(&self) -> Self::Visibility<'_> {
        self.0.visibility.iter().map(|value| Ok(value.as_str()))
    }
}

#[derive(Debug, Clone, Copy)]
pub(super) struct Shape {
    pub(super) heap: usize,
    pub(super) allocations: usize,
    pub(super) charge: usize,
    pub(super) content_hash: ContentHash,
    pub(super) inspection_visits: usize,
    pub(super) build_visits: usize,
    inputs: usize,
    visibility: usize,
}

fn multiply(left: usize, right: usize) -> Result<usize, ContractError> {
    left.checked_mul(right).ok_or(ContractError::Capacity)
}

/// This fixed allowance covers scalar provenance checking and every fixed
/// hash field (including length framing). Variable byte work is added below.
const FIELD_WORK: usize = 4096;
const INPUT_WORK: usize = 256;

struct Base {
    heap: usize,
    allocations: usize,
}

fn check_fields(
    fields: ArtifactFields<'_>,
    inputs: usize,
    visibility: usize,
    limits: Limits,
    visits: &mut VisitBudget,
) -> Result<Base, ContractError> {
    // Bound byte loops before scanning kind or hashing any borrowed payload.
    let inline = match fields.payload {
        PayloadSpec::Inline(value) => value.len(),
        PayloadSpec::Content(_) => 0,
    };
    if fields.kind.len() > limits.kind_bytes
        || fields.metadata.len() > limits.metadata_bytes
        || inline > limits.inline_bytes
        || inputs > limits.inputs
        || visibility > limits.visibility_labels
    {
        return Err(ContractError::Capacity);
    }
    visits.charge(bytes::add(
        FIELD_WORK,
        bytes::add(
            multiply(2, fields.kind.len())?,
            bytes::add(fields.metadata.len(), inline)?,
        )?,
    )?)?;
    if fields.ledger.tenant.is_zero() || fields.ledger.session.is_zero() {
        return Err(ContractError::WrongLedger);
    }
    if fields.id.is_zero() || fields.producer.is_zero() {
        return Err(ContractError::InvalidTarget);
    }
    if fields.schema != 1
        || fields.schema_hash.0 == [0; 32]
        || fields.kind.is_empty()
        || !fields.kind.bytes().all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || b"_./-".contains(&byte)
        })
    {
        return Err(ContractError::InvalidPolicy);
    }
    if fields
        .receipt
        .is_some_and(|receipt| receipt.receipt.is_zero() || receipt.epoch == 0)
    {
        return Err(ContractError::StaleReceipt);
    }
    if let Some(result) = fields.result {
        result.check(fields.ledger, fields.producer)?;
    }
    if let Some(work) = fields.work {
        if fields.result.is_some() {
            return Err(ContractError::InvalidPolicy);
        }
        work.check(fields.receipt, fields.kind)?;
    }
    if let PayloadSpec::Content(pointer) = fields.payload
        && (pointer.domain.is_zero() || pointer.root.0 == [0; 32])
    {
        return Err(ContractError::InvalidTarget);
    }
    let heap = bytes::add(
        bytes::add(
            bytes::add(fields.kind.len(), fields.metadata.len())?,
            inline,
        )?,
        bytes::add(
            bytes::array::<ObjectRef>(inputs)?,
            bytes::array::<String>(visibility)?,
        )?,
    )?;
    let mut allocations = 0;
    for count in [
        bytes::allocation::<u8>(fields.kind.len()),
        bytes::allocation::<u8>(fields.metadata.len()),
        bytes::allocation::<u8>(inline),
        bytes::allocation::<ObjectRef>(inputs),
        bytes::allocation::<String>(visibility),
    ] {
        allocations = bytes::add(allocations, count)?;
    }
    bytes::fits(
        bytes::total::<ArtifactDescriptor>(heap)?,
        limits.construction_bytes,
    )?;
    Ok(Base { heap, allocations })
}

fn next<T>(
    values: &mut impl Iterator<Item = Result<T, ContractError>>,
    visits: &mut VisitBudget,
) -> Result<T, ContractError> {
    visits.charge(1)?;
    values.next().ok_or(ContractError::InvalidManifest)?
}
fn end<T>(
    values: &mut impl Iterator<Item = Result<T, ContractError>>,
    visits: &mut VisitBudget,
) -> Result<(), ContractError> {
    visits.charge(1)?;
    match values.next() {
        None => Ok(()),
        Some(Err(error)) => Err(error),
        Some(Ok(_)) => Err(ContractError::InvalidManifest),
    }
}

pub(super) fn inspect<'a, S: ArtifactSource<'a>>(
    source: &S,
    limits: Limits,
    max_visits: usize,
) -> Result<(ArtifactFields<'a>, Shape), ContractError> {
    let mut visits = VisitBudget::new(max_visits);
    visits.charge(3)?;
    let fields = source.fields();
    let inputs = source.input_count();
    let visibility = source.visibility_count();
    let mut base = check_fields(fields, inputs, visibility, limits, &mut visits)?;
    let mut hash = begin_hash(fields, inputs);
    visits.charge(1)?;
    let mut rows = source.inputs();
    let mut previous = None;
    for _ in 0..inputs {
        let input = next(&mut rows, &mut visits)?;
        visits.charge(INPUT_WORK)?;
        if input.ledger != fields.ledger {
            return Err(ContractError::WrongLedger);
        }
        if input.id.is_zero() {
            return Err(ContractError::InvalidTarget);
        }
        if previous.is_some_and(|previous| previous >= input) {
            return Err(ContractError::InvalidManifest);
        }
        previous = Some(input);
        hash.input(input);
    }
    end(&mut rows, &mut visits)?;
    hash.count(visibility);
    visits.charge(1)?;
    let mut labels = source.visibility();
    let mut previous = None;
    for _ in 0..visibility {
        let label = next(&mut labels, &mut visits)?;
        if label.len() > limits.visibility_label_bytes {
            return Err(ContractError::Capacity);
        }
        visits.charge(bytes::add(64, multiply(3, label.len())?)?)?;
        if previous.is_some_and(|previous| previous >= label) {
            return Err(ContractError::InvalidManifest);
        }
        previous = Some(label);
        base.heap = bytes::add(base.heap, label.len())?;
        base.allocations = bytes::add(base.allocations, bytes::allocation::<u8>(label.len()))?;
        bytes::fits(
            bytes::total::<ArtifactDescriptor>(base.heap)?,
            limits.construction_bytes,
        )?;
        hash.field(label.as_bytes());
    }
    end(&mut labels, &mut visits)?;
    let content_hash = finish_hash(hash, fields.result, fields.work);
    let inspection_visits = max_visits
        .checked_sub(visits.remaining())
        .ok_or(ContractError::Capacity)?;
    // Copying, two scalar inspections, the owned rehash and final capacity/
    // allocation reconciliation coexist. This is a finite conservative quote,
    // not a claim that every source callback has the same parsing cost.
    let build_visits = bytes::add(
        multiply(3, inspection_visits)?,
        bytes::add(
            multiply(2, base.heap)?,
            bytes::add(multiply(4, visibility)?, 32)?,
        )?,
    )?;
    Ok((
        fields,
        Shape {
            heap: base.heap,
            allocations: base.allocations,
            charge: bytes::total::<ArtifactDescriptor>(base.heap)?,
            content_hash,
            inspection_visits,
            build_visits,
            inputs,
            visibility,
        },
    ))
}

fn push<T>(values: &mut Vec<T>, value: T) -> Result<(), ContractError> {
    if values.len() == values.capacity() {
        return Err(ContractError::Capacity);
    }
    values.push(value);
    Ok(())
}

pub(super) fn build<'a, S: ArtifactSource<'a>>(
    source: &S,
    id: ArtifactId,
    limits: Limits,
    shape: Shape,
    max_bytes: usize,
    max_visits: usize,
) -> Result<ArtifactDescriptor, ContractError> {
    bytes::fits(shape.charge, max_bytes)?;
    bytes::fits(shape.build_visits, max_visits)?;
    let mut visits = VisitBudget::new(shape.build_visits);
    visits.charge(3)?;
    let fields = source.fields();
    let inputs = source.input_count();
    let visibility = source.visibility_count();
    if inputs != shape.inputs || visibility != shape.visibility || fields.id != id {
        return Err(ContractError::ContentConflict);
    }
    let mut base = check_fields(fields, inputs, visibility, limits, &mut visits)?;
    bytes::fits(base.heap, shape.heap)?;
    bytes::fits(base.allocations, shape.allocations)?;
    // Charge requested copy buffers before allocation; every reserve immediately
    // rejects capacity excess. Source values are copied once and then owned.
    visits.charge(bytes::add(base.heap, 8)?)?;
    let mut descriptor = ArtifactDescriptor {
        ledger: fields.ledger,
        id: fields.id,
        schema: fields.schema,
        kind: string(fields.kind)?,
        schema_hash: fields.schema_hash,
        metadata: copy(fields.metadata)?,
        payload: match fields.payload {
            PayloadSpec::Inline(value) => Payload::Inline(copy(value)?),
            PayloadSpec::Content(pointer) => Payload::Content(pointer),
        },
        producer: fields.producer,
        receipt: fields.receipt,
        result: fields.result,
        work: fields.work,
        inputs: reserve(inputs)?,
        visibility: reserve(visibility)?,
        content_hash: ContentHash([0; 32]),
    };
    visits.charge(1)?;
    let mut rows = source.inputs();
    for _ in 0..inputs {
        let value = next(&mut rows, &mut visits)?;
        push(&mut descriptor.inputs, value)?;
    }
    end(&mut rows, &mut visits)?;
    visits.charge(1)?;
    let mut labels = source.visibility();
    for _ in 0..visibility {
        let label = next(&mut labels, &mut visits)?;
        if label.len() > limits.visibility_label_bytes {
            return Err(ContractError::Capacity);
        }
        base.heap = bytes::add(base.heap, label.len())?;
        base.allocations = bytes::add(base.allocations, bytes::allocation::<u8>(label.len()))?;
        bytes::fits(base.heap, shape.heap)?;
        bytes::fits(base.allocations, shape.allocations)?;
        // copy plus String::from_utf8 validation, before either traverses bytes.
        visits.charge(bytes::add(8, multiply(2, label.len())?)?)?;
        push(&mut descriptor.visibility, string(label)?)?;
    }
    end(&mut labels, &mut visits)?;
    let (_, actual) = inspect(&OwnedSource(&descriptor), limits, visits.remaining())?;
    visits.charge(actual.inspection_visits)?;
    if intent_fingerprint(descriptor.id, actual.content_hash)
        != intent_fingerprint(id, shape.content_hash)
    {
        return Err(ContractError::ContentConflict);
    }
    visits.charge(bytes::add(16, multiply(2, visibility)?)?)?;
    bytes::fits(descriptor.retained_bytes()?, shape.charge)?;
    bytes::fits(descriptor.heap_allocations()?, shape.allocations)?;
    descriptor.content_hash = actual.content_hash;
    Ok(descriptor)
}
