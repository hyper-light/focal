//! Shared authored application operations. A descriptor is discovery metadata,
//! never server authority. Persist canonical intent before generating IDs, then
//! persist the complete planned mutation before transmission. Unknown outcomes
//! must reuse that saved command rather than invoking the builder again.
mod catalog;
mod documents;
mod inventory;
mod schema;
pub use catalog::*;
pub use documents::*;
pub use inventory::*;

use crate::input::*;
use focal_model::*;
use focal_wire::{ListRequest, Operation, ReadRequest};
use serde::Serialize;

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "operation", content = "input")]
pub enum AuthoredOperation {
    #[serde(rename = "claim.submit")]
    ClaimSubmit(ClaimDocument),
    #[serde(rename = "testament.submit")]
    TestamentSubmit(TestamentDocument),
    #[serde(rename = "artifact.submit")]
    ArtifactSubmit(ArtifactDocument),
    #[serde(rename = "claim.post")]
    ClaimPost(ClaimIdDocument),
    #[serde(rename = "claim.progress")]
    ClaimProgress(ProgressDocument),
    #[serde(rename = "claim.cancel")]
    ClaimCancel(CancelDocument),
    #[serde(rename = "receipt.acquire")]
    ReceiptAcquire(AcquireReceiptDocument),
    #[serde(rename = "evidence.begin")]
    EvidenceBegin(BeginEvidenceDocument),
    #[serde(rename = "claim.get")]
    ClaimGet(GetDocument),
    #[serde(rename = "testament.get")]
    TestamentGet(GetDocument),
    #[serde(rename = "artifact.get")]
    ArtifactGet(GetDocument),
    #[serde(rename = "validation.get")]
    ValidationGet(GetDocument),
    #[serde(rename = "claim.list")]
    ClaimList(ListDocument),
    #[serde(rename = "testament.list")]
    TestamentList(ListDocument),
    #[serde(rename = "artifact.list")]
    ArtifactList(ListDocument),
    #[serde(rename = "validation.list")]
    ValidationList(ListDocument),
    #[serde(rename = "request.epoch")]
    RequestEpoch(RequestEpochDocument),
    #[serde(rename = "request.status")]
    RequestStatus(RequestStatusDocument),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PlannedOperation {
    Mutation(Command),
    Read(ReadRequest),
    List(ListRequest),
    Reconcile(ReconcileQuery),
}
impl PlannedOperation {
    /// Request identity and optimistic revision belong to the adapter envelope,
    /// not authored domain input. A read cannot silently ignore a write fence.
    pub fn into_wire(
        self,
        expected_revision: Option<ObjectRevision>,
    ) -> Result<Operation, InputError> {
        match self {
            Self::Mutation(command) => Ok(Operation::Submit {
                expected_revision,
                command,
            }),
            Self::Read(read) if expected_revision.is_none() => Ok(Operation::Read(read)),
            Self::List(list) if expected_revision.is_none() => Ok(Operation::List(list)),
            Self::Reconcile(query) if expected_revision.is_none() => {
                Ok(Operation::Reconcile(query))
            }
            Self::Read(_) | Self::List(_) | Self::Reconcile(_) => {
                Err(InputError::Invalid("read has a mutation revision"))
            }
        }
    }
}
impl AuthoredOperation {
    pub fn descriptor(&self) -> &'static OperationDescriptor {
        match self {
            Self::ClaimSubmit(_) => &CLAIM_SUBMIT,
            Self::TestamentSubmit(_) => &TESTAMENT_SUBMIT,
            Self::ArtifactSubmit(_) => &ARTIFACT_SUBMIT,
            Self::ClaimPost(_) => &CLAIM_POST,
            Self::ClaimProgress(_) => &CLAIM_PROGRESS,
            Self::ClaimCancel(_) => &CLAIM_CANCEL,
            Self::ReceiptAcquire(_) => &RECEIPT_ACQUIRE,
            Self::EvidenceBegin(_) => &EVIDENCE_BEGIN,
            Self::ClaimGet(_) => &CLAIM_GET,
            Self::TestamentGet(_) => &TESTAMENT_GET,
            Self::ArtifactGet(_) => &ARTIFACT_GET,
            Self::ValidationGet(_) => &VALIDATION_GET,
            Self::ClaimList(_) => &CLAIM_LIST,
            Self::TestamentList(_) => &TESTAMENT_LIST,
            Self::ArtifactList(_) => &ARTIFACT_LIST,
            Self::ValidationList(_) => &VALIDATION_LIST,
            Self::RequestEpoch(_) => &REQUEST_EPOCH,
            Self::RequestStatus(_) => &REQUEST_STATUS,
        }
    }
    /// Stable field order and expanded serde defaults; no identity generation.
    /// Context/expected revision are separately bound by the operation journal.
    pub fn canonical_intent(&self) -> Result<Vec<u8>, InputError> {
        let mut output = LimitedJson(Vec::new());
        serde_json::to_writer(&mut output, self).map_err(|_| InputError::Capacity)?;
        Ok(output.0)
    }
    pub fn build(
        self,
        context: &BuildContext,
        ids: &mut impl IdGenerator,
    ) -> Result<PlannedOperation, InputError> {
        context.validate()?;
        // Also bounds directly constructed flag DTOs. Streaming serialization
        // stops at the cap instead of allocating an unbounded temporary string.
        self.canonical_intent()?;
        let command = match self {
            Self::RequestEpoch(document) => {
                return document.build().map(PlannedOperation::Reconcile);
            }
            Self::RequestStatus(document) => {
                return document.build().map(PlannedOperation::Reconcile);
            }
            Self::ClaimSubmit(document) => document.build(context, ids)?,
            Self::TestamentSubmit(document) => document.build(context, ids)?,
            Self::ArtifactSubmit(document) => document.build(context, ids)?,
            Self::ClaimPost(document) => Command::PostClaim {
                claim: ClaimId(parse_id(&document.claim)?),
            },
            Self::ClaimProgress(document) => {
                text(&document.message)?;
                Command::RecordProgress {
                    claim: ClaimId(parse_id(&document.claim)?),
                    receipt: document.receipt.build()?,
                    message: document.message,
                }
            }
            Self::ClaimCancel(document) => {
                text(&document.reason)?;
                Command::CancelClaim {
                    claim: ClaimId(parse_id(&document.claim)?),
                    reason: document.reason,
                }
            }
            Self::ReceiptAcquire(document) => {
                let claim = ClaimId(parse_id(&document.claim)?);
                if document.epoch == 0 {
                    return Err(InputError::Invalid("zero receipt epoch"));
                }
                Command::AcquireReceipt {
                    claim,
                    receipt: ReceiptId(allocated(document.id, ids)?),
                    epoch: document.epoch,
                }
            }
            Self::EvidenceBegin(document) => {
                let claim = ClaimId(parse_id(&document.claim)?);
                let receipt = document.receipt.build()?;
                Command::BeginEvidenceSet {
                    claim,
                    receipt,
                    evidence_set: EvidenceSetId(allocated(document.id, ids)?),
                }
            }
            Self::ClaimGet(document) => {
                return document
                    .build(ObjectKind::Claim, context)
                    .map(PlannedOperation::Read);
            }
            Self::TestamentGet(document) => {
                return document
                    .build(ObjectKind::Testament, context)
                    .map(PlannedOperation::Read);
            }
            Self::ArtifactGet(document) => {
                return document
                    .build(ObjectKind::Artifact, context)
                    .map(PlannedOperation::Read);
            }
            Self::ValidationGet(document) => {
                return document
                    .build(ObjectKind::Validation, context)
                    .map(PlannedOperation::Read);
            }
            Self::ClaimList(document) => {
                return document
                    .build(ObjectKind::Claim, context)
                    .map(PlannedOperation::List);
            }
            Self::TestamentList(document) => {
                return document
                    .build(ObjectKind::Testament, context)
                    .map(PlannedOperation::List);
            }
            Self::ArtifactList(document) => {
                return document
                    .build(ObjectKind::Artifact, context)
                    .map(PlannedOperation::List);
            }
            Self::ValidationList(document) => {
                return document
                    .build(ObjectKind::Validation, context)
                    .map(PlannedOperation::List);
            }
        };
        Ok(PlannedOperation::Mutation(command))
    }
}
struct LimitedJson(Vec<u8>);
impl std::io::Write for LimitedJson {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        let required = self
            .0
            .len()
            .checked_add(bytes.len())
            .ok_or_else(|| std::io::Error::other("authored input capacity"))?;
        if required > MAX_INPUT_BYTES {
            return Err(std::io::Error::other("authored input capacity"));
        }
        if required > self.0.capacity() {
            // Serde emits many tiny fragments. Grow geometrically while the
            // hard cap remains fixed, avoiding a reallocation per fragment.
            let target = self
                .0
                .capacity()
                .max(128)
                .saturating_mul(2)
                .max(required)
                .min(MAX_INPUT_BYTES);
            let additional = target
                .checked_sub(self.0.len())
                .ok_or_else(|| std::io::Error::other("authored input capacity"))?;
            self.0
                .try_reserve_exact(additional)
                .map_err(std::io::Error::other)?;
        }
        self.0.extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
fn text(value: &str) -> Result<(), InputError> {
    if value.len() > 16 * 1024 {
        return Err(InputError::Capacity);
    }
    // Lifecycle text historically accepts any bounded string in Core. Keep
    // that contract distinct from stricter authored claim/testament prose.
    Ok(())
}
fn allocated(value: Option<String>, ids: &mut impl IdGenerator) -> Result<[u8; 16], InputError> {
    match value {
        Some(value) => parse_id(&value),
        None => {
            let id = ids.next_id()?;
            if id == [0; 16] {
                return Err(InputError::Identity);
            }
            Ok(id)
        }
    }
}

#[cfg(test)]
mod tests;
