//! A borrowed artifact body for owner preflight. Only an actual descriptor or
//! a successfully prepared model source plan can supply its identity/shape.
//! No scalar summary constructor can manufacture a checked content hash.
use super::prepare::{ALLOCATION, add, within};
use super::*;
use focal_model::ObjectRef;
use focal_model::lifecycle::artifact_descriptor::{
    ArtifactDescriptor, ArtifactFields, ArtifactSource, ArtifactSourcePlan, Limits, PayloadSpec,
};

mod sealed {
    use super::*;
    pub trait Sealed {}
    impl Sealed for ArtifactDescriptor {}
    impl<'a, S: ArtifactSource<'a>> Sealed for ArtifactSourcePlan<'_, 'a, S> {}
}

/// Borrowed source replays consume their adapter's one cumulative allowance.
/// The owner must fund these passes and build's remaining pass before building.
/// A source may drift; the model refuses such a build and the ordinary owned
/// authority path runs again before any custody or publication is accepted.
pub(in crate::native) trait ArtifactView: sealed::Sealed {
    fn fields(&self) -> ArtifactFields<'_>;
    fn content_hash(&self) -> ContentHash;
    fn input_count(&self) -> usize;
    fn visibility_count(&self) -> usize;
    fn inputs(&self) -> impl Iterator<Item = Result<ObjectRef, ContractError>>;
    fn visibility(&self) -> impl Iterator<Item = Result<&str, ContractError>>;
    fn retained_bytes(&self) -> Result<usize, NativeError>;
    /// Complete typed input charge, including its singleton and all allocators.
    fn heap_charge(&self) -> Result<usize, NativeError>;
}

impl ArtifactView for ArtifactDescriptor {
    fn fields(&self) -> ArtifactFields<'_> {
        ArtifactFields {
            ledger: self.ledger(),
            id: self.id(),
            schema: self.schema(),
            kind: self.kind(),
            schema_hash: self.schema_hash(),
            metadata: self.metadata(),
            payload: self.payload(),
            producer: self.producer(),
            receipt: self.receipt(),
            result: self.result_provenance(),
            work: self.work_provenance(),
        }
    }
    fn content_hash(&self) -> ContentHash {
        self.content_hash()
    }
    fn input_count(&self) -> usize {
        self.inputs().len()
    }
    fn visibility_count(&self) -> usize {
        self.visibility().len()
    }
    fn inputs(&self) -> impl Iterator<Item = Result<ObjectRef, ContractError>> {
        self.inputs().iter().copied().map(Ok)
    }
    fn visibility(&self) -> impl Iterator<Item = Result<&str, ContractError>> {
        self.visibility().map(Ok)
    }
    fn retained_bytes(&self) -> Result<usize, NativeError> {
        Ok(self.retained_bytes()?)
    }
    fn heap_charge(&self) -> Result<usize, NativeError> {
        input_heap(self.retained_heap_bytes()?, self.heap_allocations()?)
    }
}

impl<'a, S: ArtifactSource<'a>> ArtifactView for ArtifactSourcePlan<'_, 'a, S> {
    fn fields(&self) -> ArtifactFields<'_> {
        self.fields()
    }
    fn content_hash(&self) -> ContentHash {
        self.content_hash()
    }
    fn input_count(&self) -> usize {
        self.input_count()
    }
    fn visibility_count(&self) -> usize {
        self.visibility_count()
    }
    fn inputs(&self) -> impl Iterator<Item = Result<ObjectRef, ContractError>> {
        ExactValues::new(self.inputs(), self.input_count())
    }
    fn visibility<'v>(&'v self) -> impl Iterator<Item = Result<&'v str, ContractError>> {
        // The source's GAT yields the input-data lifetime. Map each value to
        // this shorter view borrow; an opaque associated iterator cannot be
        // covariantly shortened as a whole.
        ExactValues::new(self.visibility(), self.visibility_count())
            .map(|value| value.map(|label| -> &'v str { label }))
    }
    fn retained_bytes(&self) -> Result<usize, NativeError> {
        Ok(self.construction_charge())
    }
    fn heap_charge(&self) -> Result<usize, NativeError> {
        input_heap(
            self.construction_heap_bytes(),
            self.construction_heap_allocations(),
        )
    }
}

fn input_heap(heap: usize, allocations: usize) -> Result<usize, NativeError> {
    add(
        NativeArtifactInput::container_charge(),
        add(
            heap,
            allocations
                .checked_mul(ALLOCATION)
                .ok_or(NativeError::Capacity("artifact allocator charge"))?,
        )?,
    )
}

/// The plan's inspected count bounds even an inconsistent generic source.
/// A terminal probe executes once, under the adapter's own parsing allowance.
struct ExactValues<I> {
    values: I,
    remaining: usize,
    finished: bool,
}
impl<I> ExactValues<I> {
    fn new(values: I, remaining: usize) -> Self {
        Self {
            values,
            remaining,
            finished: false,
        }
    }
}
impl<T, I: Iterator<Item = Result<T, ContractError>>> Iterator for ExactValues<I> {
    type Item = Result<T, ContractError>;
    fn next(&mut self) -> Option<Self::Item> {
        if self.finished {
            return None;
        }
        if self.remaining == 0 {
            self.finished = true;
            return match self.values.next() {
                None => None,
                Some(Err(error)) => Some(Err(error)),
                Some(Ok(_)) => Some(Err(ContractError::InvalidManifest)),
            };
        }
        self.remaining = match self.remaining.checked_sub(1) {
            Some(remaining) => remaining,
            None => {
                self.finished = true;
                return Some(Err(ContractError::Capacity));
            }
        };
        match self.values.next() {
            Some(Ok(value)) => Some(Ok(value)),
            value => {
                self.finished = true;
                Some(Err(match value {
                    Some(Err(error)) => error,
                    _ => ContractError::InvalidManifest,
                }))
            }
        }
    }
}

pub(in crate::native) fn check_limits(
    descriptor: &impl ArtifactView,
    limits: Limits,
    max_heap: usize,
    dimension_error: &'static str,
) -> Result<(), NativeError> {
    let fields = descriptor.fields();
    if fields.kind.len() > limits.kind_bytes
        || fields.metadata.len() > limits.metadata_bytes
        || descriptor.input_count() > limits.inputs
        || descriptor.visibility_count() > limits.visibility_labels
    {
        return Err(NativeError::Capacity(dimension_error));
    }
    for label in descriptor.visibility() {
        if label?.len() > limits.visibility_label_bytes {
            return Err(NativeError::Capacity(dimension_error));
        }
    }
    if matches!(fields.payload, PayloadSpec::Inline(bytes) if bytes.len() > limits.inline_bytes) {
        return Err(NativeError::Capacity(dimension_error));
    }
    within(descriptor.retained_bytes()?, limits.construction_bytes)?;
    within(descriptor.heap_charge()?, max_heap)
}
