//! Independently owned immutable artifact facts. This native descriptor has no
//! artifact/response/evaluation lifecycle and confers no custody or authorization.
//! Core resolves input existence, inherited visibility, registered schemas and
//! actual storage/provenance before admission. V1 content and codecs are unchanged.
use super::{
    Binding, ContractError,
    evidence::EvidenceFailure,
    memory as bytes,
    validation::{Attempt, Phase, Target},
};
use crate::{
    ArtifactId, ArtifactRef, ClaimId, ContentClass, ContentDomainId, ContentHash, LedgerId,
    ObjectId, ObjectKind, ObjectRef, ObjectRevision, ParticipantId, ReceiptFence, ValidationId,
    VerdictValue,
};

#[path = "artifact_descriptor_source.rs"]
mod source;
pub use source::{ArtifactFields, ArtifactSource, ArtifactSourcePlan};

/// Immutable provenance of a participant-authored validation result artifact.
/// The owner must match this complete value to the actual retained evaluation
/// before accepting custody or a report; these fields confer no authority.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ResultProvenance {
    pub claim: ClaimId,
    pub validation: ValidationId,
    pub target: Target,
    pub generation: u64,
    pub attempt: Attempt,
    pub value: VerdictValue,
}

/// Immutable work-cycle identity. The owner checks it against the actual receipt
/// and current cycle; it records authorship without granting submission authority.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WorkProvenance {
    pub claim: ClaimId,
    pub cycle: u32,
    pub role: WorkRole,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkRole {
    Output {
        slot: u32,
    },
    Diagnostic {
        reason: EvidenceFailure,
    },
    /// A claimant's observation failure, tied to the exact immutable work product.
    ReceiptRejection {
        artifact: ArtifactRef,
        reason: EvidenceFailure,
    },
}

impl WorkProvenance {
    fn check(self, receipt: Option<ReceiptFence>, kind: &str) -> Result<(), ContractError> {
        if self.claim.is_zero() || self.cycle == 0 || self.cycle.checked_add(1).is_none() {
            return Err(ContractError::InvalidTarget);
        }
        if receipt.is_none_or(|receipt| receipt.receipt.is_zero() || receipt.epoch == 0) {
            return Err(ContractError::StaleReceipt);
        }
        if matches!(
            self.role,
            WorkRole::Diagnostic { .. } | WorkRole::ReceiptRejection { .. }
        ) && kind != "error"
        {
            return Err(ContractError::MissingEvidence);
        }
        if let WorkRole::ReceiptRejection { artifact, reason } = self.role {
            if artifact.id.is_zero() || artifact.hash.0 == [0; 32] {
                return Err(ContractError::InvalidTarget);
            }
            if !matches!(
                reason,
                EvidenceFailure::Structure | EvidenceFailure::Metadata
            ) {
                return Err(ContractError::InvalidPolicy);
            }
        }
        Ok(())
    }
}

impl ResultProvenance {
    fn check(self, ledger: LedgerId, producer: ParticipantId) -> Result<(), ContractError> {
        if self.claim.is_zero() || self.validation.is_zero() {
            return Err(ContractError::InvalidTarget);
        }
        if self.generation == 0 {
            return Err(ContractError::StaleEvaluation);
        }
        if self.attempt.evaluator != producer {
            return Err(ContractError::WrongActor);
        }
        if self.attempt.handler.is_zero()
            || self.attempt.version.0 == [0; 32]
            || self.attempt.definition.0 == [0; 32]
            || !matches!(self.attempt.phase, Phase::Programmatic | Phase::Quality)
        {
            return Err(ContractError::InvalidPolicy);
        }
        let binding = |target: Binding| {
            if target.ledger != ledger {
                Err(ContractError::WrongLedger)
            } else if target.object.is_zero()
                || target.content.0 == [0; 32]
                || target.revision.0 == 0
            {
                Err(ContractError::InvalidTarget)
            } else {
                Ok(())
            }
        };
        let claim = |target: Binding| {
            binding(target)?;
            if target.object.0 != self.claim.0 {
                Err(ContractError::InvalidTarget)
            } else {
                Ok(())
            }
        };
        match self.target {
            Target::Artifact {
                response, artifact, ..
            } => {
                binding(response)?;
                binding(artifact)
            }
            Target::MissingSlot { response, .. } | Target::Delivery { response } => {
                binding(response)?;
                // These outcomes are derived without an external evaluator
                // attempt. An authored artifact cannot invent that result role.
                Err(ContractError::InvalidTarget)
            }
            Target::Admission { claim: target } => claim(target),
            Target::Increment {
                claim: target,
                artifact,
            } => {
                claim(target)?;
                binding(artifact)
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ContentPointer {
    pub domain: ContentDomainId,
    pub root: ContentHash,
    pub length: u64,
    pub class: ContentClass,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PayloadSpec<'a> {
    Inline(&'a [u8]),
    Content(ContentPointer),
}

/// Internal descriptor IR: input references and visibility labels must already
/// be sorted and unique. Future authored-input builders sort fallibly before
/// this boundary; humans are not required to manually sort CLI/JSON/YAML input.
#[derive(Debug, Clone, Copy)]
pub struct ArtifactSpec<'a> {
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
    pub inputs: &'a [ObjectRef],
    pub visibility: &'a [&'a str],
}

#[derive(Debug, Clone, Copy)]
pub struct Limits {
    pub kind_bytes: usize,
    pub metadata_bytes: usize,
    pub inline_bytes: usize,
    pub inputs: usize,
    pub visibility_labels: usize,
    pub visibility_label_bytes: usize,
    pub construction_bytes: usize,
}

#[derive(Debug, PartialEq, Eq)]
enum Payload {
    Inline(Vec<u8>),
    Content(ContentPointer),
}

#[derive(Debug, PartialEq, Eq)]
pub struct ArtifactDescriptor {
    ledger: LedgerId,
    id: ArtifactId,
    schema: u16,
    kind: String,
    schema_hash: ContentHash,
    metadata: Vec<u8>,
    payload: Payload,
    producer: ParticipantId,
    receipt: Option<ReceiptFence>,
    result: Option<ResultProvenance>,
    work: Option<WorkProvenance>,
    inputs: Vec<ObjectRef>,
    visibility: Vec<String>,
    content_hash: ContentHash,
}

#[derive(Debug)]
pub struct ArtifactPlan<'a> {
    spec: ArtifactSpec<'a>,
    limits: Limits,
    shape: source::Shape,
}

fn string(value: &str) -> Result<String, ContractError> {
    String::from_utf8(copy(value.as_bytes())?).map_err(|_| ContractError::InvalidManifest)
}
fn reserve<T>(count: usize) -> Result<Vec<T>, ContractError> {
    let values = bytes::reserve::<T>(count)?;
    if size_of::<T>() != 0 && values.capacity() > count {
        return Err(ContractError::Capacity);
    }
    Ok(values)
}
fn copy<T: Copy>(values: &[T]) -> Result<Vec<T>, ContractError> {
    let mut owned = reserve(values.len())?;
    owned.extend_from_slice(values);
    Ok(owned)
}

impl ArtifactDescriptor {
    pub fn prepare(
        spec: ArtifactSpec<'_>,
        limits: Limits,
    ) -> Result<ArtifactPlan<'_>, ContractError> {
        let (_, shape) = source::inspect(&spec, limits, usize::MAX)?;
        Ok(ArtifactPlan {
            spec,
            limits,
            shape,
        })
    }

    pub fn id(&self) -> ArtifactId {
        self.id
    }
    pub fn ledger(&self) -> LedgerId {
        self.ledger
    }
    pub fn schema(&self) -> u16 {
        self.schema
    }
    pub fn kind(&self) -> &str {
        &self.kind
    }
    pub fn schema_hash(&self) -> ContentHash {
        self.schema_hash
    }
    pub fn metadata(&self) -> &[u8] {
        &self.metadata
    }
    pub fn payload(&self) -> PayloadSpec<'_> {
        match &self.payload {
            Payload::Inline(value) => PayloadSpec::Inline(value),
            Payload::Content(pointer) => PayloadSpec::Content(*pointer),
        }
    }
    pub fn producer(&self) -> ParticipantId {
        self.producer
    }
    pub fn receipt(&self) -> Option<ReceiptFence> {
        self.receipt
    }
    pub fn result_provenance(&self) -> Option<ResultProvenance> {
        self.result
    }
    pub fn work_provenance(&self) -> Option<WorkProvenance> {
        self.work
    }
    /// Bind this descriptor to the actual work cycle before custody. Existing
    /// provenance can only be repeated identically; buffers are moved unchanged.
    pub fn with_work_provenance(
        mut self,
        provenance: WorkProvenance,
    ) -> Result<Self, ContractError> {
        if self.result.is_some() {
            return Err(ContractError::InvalidPolicy);
        }
        if self.work.is_some_and(|existing| existing != provenance) {
            return Err(ContractError::ContentConflict);
        }
        provenance.check(self.receipt, &self.kind)?;
        if self.work.is_none() {
            self.work = Some(provenance);
            self.content_hash = content_hash(&self);
        }
        Ok(self)
    }
    /// Finish an unbound result descriptor before custody or publication. This
    /// consumes the descriptor without allocating or copying any owned buffer.
    /// An already declared role can only be supplied again identically.
    pub fn with_result_provenance(
        mut self,
        provenance: ResultProvenance,
    ) -> Result<Self, ContractError> {
        if self.work.is_some() {
            return Err(ContractError::InvalidPolicy);
        }
        if self.result.is_some_and(|existing| existing != provenance) {
            return Err(ContractError::ContentConflict);
        }
        provenance.check(self.ledger, self.producer)?;
        if self.result.is_none() {
            self.result = Some(provenance);
            self.content_hash = content_hash(&self);
        }
        Ok(self)
    }
    pub fn inputs(&self) -> &[ObjectRef] {
        &self.inputs
    }
    pub fn visibility(&self) -> impl ExactSizeIterator<Item = &str> {
        self.visibility.iter().map(String::as_str)
    }
    pub fn content_hash(&self) -> ContentHash {
        self.content_hash
    }
    /// Immutable descriptor revision; mutable lifecycle state is a separate row.
    pub fn binding(&self) -> Binding {
        Binding {
            ledger: self.ledger,
            object: ObjectId(self.id.0),
            content: self.content_hash,
            revision: ObjectRevision(1),
        }
    }

    /// Dynamic buffer bytes only; the owner separately reserves allocator
    /// metadata for `copy_heap_allocations()` allocations before copying.
    pub fn copy_heap_bytes(&self) -> Result<usize, ContractError> {
        self.heap_bytes(false)
    }
    /// Actual retained buffer capacity, excluding inline storage and allocator
    /// metadata. Use `heap_allocations()` for the latter owner's charge.
    pub fn retained_heap_bytes(&self) -> Result<usize, ContractError> {
        self.heap_bytes(true)
    }
    fn heap_bytes(&self, retained: bool) -> Result<usize, ContractError> {
        fn string_len(value: &String, retained: bool) -> usize {
            if retained {
                value.capacity()
            } else {
                value.len()
            }
        }
        fn vec_len<T>(value: &Vec<T>, retained: bool) -> usize {
            if retained {
                value.capacity()
            } else {
                value.len()
            }
        }
        let mut heap = bytes::add(
            string_len(&self.kind, retained),
            vec_len(&self.metadata, retained),
        )?;
        if let Payload::Inline(value) = &self.payload {
            heap = bytes::add(heap, vec_len(value, retained))?;
        }
        heap = bytes::add(
            heap,
            bytes::array::<ObjectRef>(vec_len(&self.inputs, retained))?,
        )?;
        heap = bytes::add(
            heap,
            bytes::array::<String>(vec_len(&self.visibility, retained))?,
        )?;
        for label in &self.visibility {
            heap = bytes::add(heap, string_len(label, retained))?;
        }
        Ok(heap)
    }
    pub fn copy_heap_allocations(&self) -> Result<usize, ContractError> {
        self.allocations(false)
    }
    pub fn heap_allocations(&self) -> Result<usize, ContractError> {
        self.allocations(true)
    }
    fn allocations(&self, retained: bool) -> Result<usize, ContractError> {
        let mut count = usize::from(if retained {
            self.kind.capacity() != 0
        } else {
            !self.kind.is_empty()
        });
        for capacity in [
            if retained {
                self.metadata.capacity()
            } else {
                self.metadata.len()
            },
            if retained {
                self.inputs.capacity()
            } else {
                self.inputs.len()
            },
            if retained {
                self.visibility.capacity()
            } else {
                self.visibility.len()
            },
        ] {
            count = bytes::add(count, usize::from(capacity != 0))?;
        }
        if let Payload::Inline(value) = &self.payload {
            count = bytes::add(
                count,
                usize::from(if retained {
                    value.capacity() != 0
                } else {
                    !value.is_empty()
                }),
            )?;
        }
        for label in &self.visibility {
            count = bytes::add(
                count,
                usize::from(if retained {
                    label.capacity() != 0
                } else {
                    !label.is_empty()
                }),
            )?;
        }
        Ok(count)
    }
    pub fn copy_charge(&self) -> Result<usize, ContractError> {
        bytes::total::<Self>(self.copy_heap_bytes()?)
    }
    pub fn retained_bytes(&self) -> Result<usize, ContractError> {
        bytes::total::<Self>(self.retained_heap_bytes()?)
    }
    pub fn try_copy(&self, max_bytes: usize) -> Result<Self, ContractError> {
        bytes::fits(self.copy_charge()?, max_bytes)?;
        let mut visibility = reserve(self.visibility.len())?;
        for label in &self.visibility {
            visibility.push(string(label)?);
        }
        let copied = Self {
            ledger: self.ledger,
            id: self.id,
            schema: self.schema,
            kind: string(&self.kind)?,
            schema_hash: self.schema_hash,
            metadata: copy(&self.metadata)?,
            payload: match &self.payload {
                Payload::Inline(value) => Payload::Inline(copy(value)?),
                Payload::Content(pointer) => Payload::Content(*pointer),
            },
            producer: self.producer,
            receipt: self.receipt,
            result: self.result,
            work: self.work,
            inputs: copy(&self.inputs)?,
            visibility,
            content_hash: self.content_hash,
        };
        bytes::fits(copied.retained_bytes()?, max_bytes)?;
        Ok(copied)
    }
    /// Address-sensitive native retry identity; immutable content identity above
    /// excludes this artifact's own allocated ID, as required by D-02.
    pub fn intent_fingerprint(&self) -> ContentHash {
        intent_fingerprint(self.id, self.content_hash)
    }
}

impl ArtifactPlan<'_> {
    /// Inline descriptor plus requested buffers, excluding allocator metadata.
    /// The effective owner reserves this and the allocation count before build.
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
        intent_fingerprint(self.spec.id, self.shape.content_hash)
    }
    pub fn build(self) -> Result<ArtifactDescriptor, ContractError> {
        source::build(
            &self.spec,
            self.spec.id,
            self.limits,
            self.shape,
            self.shape.charge,
            self.shape.build_visits,
        )
    }
}

fn intent_fingerprint(id: ArtifactId, content: ContentHash) -> ContentHash {
    let mut hash = blake3::Hasher::new_derive_key("focal/native/artifact-intent/1");
    hash.update(&id.0);
    hash.update(&content.0);
    ContentHash(*hash.finalize().as_bytes())
}

/// One hash engine supplies both checked value-source preparation and owned
/// artifact rehashing; the native content preimage remains unchanged.
struct Hash(blake3::Hasher);
impl Hash {
    fn field(&mut self, value: &[u8]) {
        self.0.update(&(value.len() as u128).to_be_bytes());
        self.0.update(value);
    }
    fn count(&mut self, value: usize) {
        self.field(&(value as u128).to_be_bytes());
    }
    fn binding(&mut self, binding: Binding) {
        self.field(&binding.ledger.tenant.0);
        self.field(&binding.ledger.session.0);
        self.field(&binding.object.0);
        self.field(&binding.content.0);
        self.field(&binding.revision.0.to_be_bytes());
    }
    fn target(&mut self, target: Target) {
        match target {
            Target::Artifact {
                response,
                slot,
                artifact,
            } => {
                self.field(b"artifact");
                self.binding(response);
                self.field(&slot.to_be_bytes());
                self.binding(artifact);
            }
            Target::MissingSlot { response, slot } => {
                self.field(b"missing-slot");
                self.binding(response);
                self.field(&slot.to_be_bytes());
            }
            Target::Delivery { response } => {
                self.field(b"delivery");
                self.binding(response);
            }
            Target::Admission { claim } => {
                self.field(b"admission");
                self.binding(claim);
            }
            Target::Increment { claim, artifact } => {
                self.field(b"increment");
                self.binding(claim);
                self.binding(artifact);
            }
        }
    }
}
fn begin_hash(fields: ArtifactFields<'_>, input_count: usize) -> Hash {
    let mut hash = Hash(blake3::Hasher::new_derive_key(
        "focal/native/artifact-content/1",
    ));
    hash.field(&fields.ledger.tenant.0);
    hash.field(&fields.ledger.session.0);
    hash.field(&fields.schema.to_be_bytes());
    hash.field(fields.kind.as_bytes());
    hash.field(&fields.schema_hash.0);
    hash.field(fields.metadata);
    match fields.payload {
        PayloadSpec::Inline(value) => {
            hash.field(b"inline");
            hash.field(value);
        }
        PayloadSpec::Content(pointer) => {
            hash.field(b"content");
            hash.field(&pointer.domain.0);
            hash.field(&pointer.root.0);
            hash.field(&pointer.length.to_be_bytes());
            hash.field(match pointer.class {
                ContentClass::Document => b"document",
                ContentClass::Evidence => b"evidence",
                ContentClass::Checkpoint => b"checkpoint",
            });
        }
    }
    hash.field(&fields.producer.0);
    match fields.receipt {
        None => hash.field(b"no-receipt"),
        Some(receipt) => {
            hash.field(b"receipt");
            hash.field(&receipt.receipt.0);
            hash.field(&receipt.epoch.to_be_bytes());
        }
    }
    hash.count(input_count);
    hash
}

impl Hash {
    fn input(&mut self, input: ObjectRef) {
        self.field(&input.ledger.tenant.0);
        self.field(&input.ledger.session.0);
        self.field(match input.kind {
            ObjectKind::Claim => b"claim",
            ObjectKind::Testament => b"testament",
            ObjectKind::Validation => b"validation",
            ObjectKind::Artifact => b"artifact",
        });
        self.field(&input.id.0);
    }
}

fn finish_hash(
    mut hash: Hash,
    result: Option<ResultProvenance>,
    work: Option<WorkProvenance>,
) -> ContentHash {
    match result {
        None => hash.field(b"no-result"),
        Some(result) => {
            hash.field(b"result");
            hash.field(&result.claim.0);
            hash.field(&result.validation.0);
            hash.target(result.target);
            hash.field(&result.generation.to_be_bytes());
            hash.field(match result.attempt.phase {
                Phase::Programmatic => b"programmatic",
                Phase::Quality => b"quality",
                Phase::Delivery => b"delivery",
                Phase::MissingTarget => b"missing-target",
            });
            hash.field(&result.attempt.index.to_be_bytes());
            hash.field(&result.attempt.handler.0);
            hash.field(&result.attempt.version.0);
            hash.field(&result.attempt.evaluator.0);
            hash.field(&result.attempt.definition.0);
            hash.field(match result.value {
                VerdictValue::Pass => b"pass",
                VerdictValue::Fail => b"fail",
                VerdictValue::Incomplete => b"incomplete",
                VerdictValue::Error => b"error",
            });
        }
    }
    // Descriptors without work provenance retain their original native hashes.
    // The optional role adds a domain-separated, length-framed suffix; repeated
    // payloads in distinct cycles no longer require invented metadata to differ.
    if let Some(work) = work {
        hash.field(b"focal/native/artifact-work/1");
        hash.field(&work.claim.0);
        hash.field(&work.cycle.to_be_bytes());
        match work.role {
            WorkRole::Output { slot } => {
                hash.field(b"output");
                hash.field(&slot.to_be_bytes());
            }
            WorkRole::Diagnostic { reason } => {
                hash.field(b"diagnostic");
                hash.field(match reason {
                    EvidenceFailure::Work => b"work",
                    EvidenceFailure::Production => b"production",
                    EvidenceFailure::Structure => b"structure",
                    EvidenceFailure::Metadata => b"metadata",
                });
            }
            WorkRole::ReceiptRejection { artifact, reason } => {
                hash.field(b"receipt-rejection");
                hash.field(&artifact.id.0);
                hash.field(&artifact.hash.0);
                hash.field(match reason {
                    EvidenceFailure::Work => b"work",
                    EvidenceFailure::Production => b"production",
                    EvidenceFailure::Structure => b"structure",
                    EvidenceFailure::Metadata => b"metadata",
                });
            }
        }
    }
    ContentHash(*hash.0.finalize().as_bytes())
}

fn content_hash(descriptor: &ArtifactDescriptor) -> ContentHash {
    let fields = source::OwnedSource(descriptor).fields();
    let mut hash = begin_hash(fields, descriptor.inputs.len());
    for input in &descriptor.inputs {
        hash.input(*input);
    }
    hash.count(descriptor.visibility.len());
    for label in descriptor.visibility() {
        hash.field(label.as_bytes());
    }
    finish_hash(hash, descriptor.result, descriptor.work)
}

#[cfg(test)]
#[path = "artifact_descriptor_tests.rs"]
mod tests;

#[cfg(test)]
#[path = "artifact_plan_identity_tests.rs"]
mod plan_identity_tests;
