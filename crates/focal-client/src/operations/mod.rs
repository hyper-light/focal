//! Shared authored application operations. A descriptor is discovery metadata,
//! never server authority. Persist canonical intent before generating IDs, then
//! persist the complete planned mutation before transmission. Unknown outcomes
//! must reuse that saved command rather than invoking the builder again.
mod catalog;
mod claim_get;
mod documents;
mod inventory;
mod lifecycle;
mod monitors;
mod schema;
mod selection;
mod traversal;
mod validators;
mod wait;
pub use catalog::*;
pub use claim_get::*;
pub use documents::*;
pub use inventory::*;
pub use lifecycle::*;
pub use monitors::*;
pub use traversal::*;
pub use validators::*;
pub use wait::*;

use crate::input::*;
use focal_model::*;
use focal_wire::{ListRequest, Operation, ReadRequest};
use serde::Serialize;

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "operation", content = "input")]
pub enum AuthoredOperation {
    #[serde(rename = "monitor.register")]
    MonitorRegister(MonitorRegisterDocument),
    #[serde(rename = "monitor.get")]
    MonitorGet(MonitorGetDocument),
    #[serde(rename = "ledger.summary")]
    LedgerSummary(SummaryDocument),
    #[serde(rename = "validator.list")]
    ValidatorList(ValidatorDocument),
    #[serde(rename = "validator.get")]
    ValidatorGet(ValidatorDocument),
    #[serde(rename = "ledger.traverse")]
    LedgerTraverse(TraversalDocument),
    #[serde(rename = "claim.submit")]
    ClaimSubmit(ClaimDocument),
    #[serde(rename = "claim.submit_batch")]
    ClaimSubmitBatch(ClaimBatchDocument),
    #[serde(rename = "testament.submit")]
    TestamentSubmit(TestamentDocument),
    #[serde(rename = "artifact.submit")]
    ArtifactSubmit(ArtifactDocument),
    #[serde(rename = "artifact.register")]
    ArtifactRegister(RegisterArtifactDocument),
    #[serde(rename = "testament.receive")]
    TestamentReceive(ReceiveTestamentDocument),
    #[serde(rename = "validation.begin")]
    ValidationBegin(ClaimIdDocument),
    #[serde(rename = "validation.begin_increment")]
    ValidationBeginIncrement(IncrementValidationDocument),
    #[serde(rename = "validation.complete")]
    ValidationComplete(ClaimIdDocument),
    #[serde(rename = "validation.submit")]
    ValidationSubmit(ValidationVerdictDocument),
    #[serde(rename = "claim.supersede")]
    ClaimSupersede(SupersedeDocument),
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
    ClaimGet(ClaimGetDocument),
    #[serde(rename = "claim.wait")]
    ClaimWait(ClaimWaitDocument),
    #[serde(rename = "testament.get")]
    TestamentGet(GetDocument),
    #[serde(rename = "artifact.get")]
    ArtifactGet(GetDocument),
    #[serde(rename = "validation.get")]
    ValidationGet(GetDocument),
    #[serde(rename = "validation.context")]
    ValidationContext(GetDocument),
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
    Monitor(MonitorId),
    Summary,
    ClaimGet(ClaimSelector),
    ClaimWait {
        read: ReadRequest,
        until: crate::claim_wait::ClaimWaitUntil,
        timeout_ms: u32,
    },
    Validators(focal_wire::ValidatorRequest),
    Traverse(focal_wire::TraversalRequest),
    Mutation(Command),
    Read(ReadRequest),
    List(ListRequest),
    Selection(focal_wire::SelectionRequest),
    Reconcile(ReconcileQuery),
    /// A bounded composition of reads at one prefix; use Client::validation_context.
    ValidationContext(ReadRequest),
}
impl PlannedOperation {
    /// Request identity and optimistic revision belong to the adapter envelope,
    /// not authored domain input. A read cannot silently ignore a write fence.
    pub fn into_wire(
        self,
        expected_revision: Option<ObjectRevision>,
    ) -> Result<Operation, InputError> {
        match self {
            Self::Monitor(id) if expected_revision.is_none() => Ok(Operation::Monitor { id }),
            Self::Monitor(_) => Err(InputError::Invalid("read has a mutation revision")),
            Self::Summary if expected_revision.is_none() => Ok(Operation::Summary),
            Self::Summary => Err(InputError::Invalid("read has a mutation revision")),
            Self::Validators(query) if expected_revision.is_none() => {
                Ok(Operation::Validators(query))
            }
            Self::Validators(_) => Err(InputError::Invalid("read has a mutation revision")),
            Self::Traverse(query) if expected_revision.is_none() => Ok(Operation::Traverse(query)),
            Self::Traverse(_) => Err(InputError::Invalid("read has a mutation revision")),
            Self::Mutation(command) => Ok(Operation::Submit {
                expected_revision,
                command,
            }),
            Self::Read(read) if expected_revision.is_none() => Ok(Operation::Read(read)),
            Self::List(list) if expected_revision.is_none() => Ok(Operation::List(list)),
            Self::Selection(list) if expected_revision.is_none() => Ok(Operation::Select(list)),
            Self::Reconcile(query) if expected_revision.is_none() => {
                Ok(Operation::Reconcile(query))
            }
            Self::Read(_) | Self::List(_) | Self::Selection(_) | Self::Reconcile(_) => {
                Err(InputError::Invalid("read has a mutation revision"))
            }
            Self::ClaimGet(ClaimSelector::Exact(read)) if expected_revision.is_none() => {
                Ok(Operation::Read(read))
            }
            Self::ClaimGet(_) => Err(InputError::Invalid(
                "claim selection requires the composed client read",
            )),
            Self::ClaimWait { .. } => Err(InputError::Invalid(
                "claim wait requires the composed client read",
            )),
            Self::ValidationContext(_) => Err(InputError::Invalid(
                "validation context requires the composed client read",
            )),
        }
    }
}
impl AuthoredOperation {
    /// These command-specific peer grants are bound to a saved claim revision.
    pub fn revision_claim(&self) -> Result<Option<ClaimId>, InputError> {
        let claim = match self {
            Self::TestamentReceive(document) => &document.claim,
            Self::ValidationBeginIncrement(document) => &document.claim,
            Self::ValidationBegin(document) | Self::ValidationComplete(document) => &document.claim,
            _ => return Ok(None),
        };
        Ok(Some(ClaimId(parse_id(claim)?)))
    }
    pub fn descriptor(&self) -> &'static OperationDescriptor {
        match self {
            Self::LedgerSummary(_) => &LEDGER_SUMMARY,
            Self::MonitorRegister(_) => &MONITOR_REGISTER,
            Self::MonitorGet(_) => &MONITOR_GET,
            Self::ValidatorList(_) => &VALIDATOR_LIST,
            Self::ValidatorGet(_) => &VALIDATOR_GET,
            Self::LedgerTraverse(_) => &LEDGER_TRAVERSE,
            Self::ClaimSubmit(_) => &CLAIM_SUBMIT,
            Self::ClaimSubmitBatch(_) => &CLAIM_SUBMIT_BATCH,
            Self::TestamentSubmit(_) => &TESTAMENT_SUBMIT,
            Self::ArtifactSubmit(_) => &ARTIFACT_SUBMIT,
            Self::ArtifactRegister(_) => &ARTIFACT_REGISTER,
            Self::TestamentReceive(_) => &TESTAMENT_RECEIVE,
            Self::ValidationBegin(_) => &VALIDATION_BEGIN,
            Self::ValidationBeginIncrement(_) => &VALIDATION_BEGIN_INCREMENT,
            Self::ValidationComplete(_) => &VALIDATION_COMPLETE,
            Self::ValidationSubmit(_) => &VALIDATION_SUBMIT,
            Self::ClaimSupersede(_) => &CLAIM_SUPERSEDE,
            Self::ClaimPost(_) => &CLAIM_POST,
            Self::ClaimProgress(_) => &CLAIM_PROGRESS,
            Self::ClaimCancel(_) => &CLAIM_CANCEL,
            Self::ReceiptAcquire(_) => &RECEIPT_ACQUIRE,
            Self::EvidenceBegin(_) => &EVIDENCE_BEGIN,
            Self::ClaimGet(_) => &CLAIM_GET,
            Self::ClaimWait(_) => &CLAIM_WAIT,
            Self::TestamentGet(_) => &TESTAMENT_GET,
            Self::ArtifactGet(_) => &ARTIFACT_GET,
            Self::ValidationGet(_) => &VALIDATION_GET,
            Self::ValidationContext(_) => &VALIDATION_CONTEXT,
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
    /// Shared CLI/MCP binding, including the optimistic revision. Keep the
    /// field order unchanged: legacy MCP journals already persist these bytes.
    pub fn canonical_mutation_intent(
        &self,
        revision: Option<ObjectRevision>,
    ) -> Result<Vec<u8>, InputError> {
        #[derive(Serialize)]
        struct Intent<'a> {
            revision: Option<ObjectRevision>,
            authored: &'a AuthoredOperation,
        }
        let mut output = LimitedJson(Vec::new());
        serde_json::to_writer(
            &mut output,
            &Intent {
                revision,
                authored: self,
            },
        )
        .map_err(|_| InputError::Capacity)?;
        Ok(output.0)
    }
    /// Validate before creating a journal or reserving an operation. These
    /// throwaway IDs never leave this method; actual entropy is expanded only
    /// inside durable preparation. Excluding every exact authored hexadecimal ID
    /// prevents synthetic IDs from creating false self-lineage/duplicate errors.
    pub fn preflight(&self, context: &BuildContext) -> Result<(), InputError> {
        let excluded = authored_ids(&self.canonical_intent()?)?;
        let mut next = 0u128;
        let mut ids = || {
            for _ in 0..=excluded.len() {
                next = next.checked_add(1).ok_or(InputError::Capacity)?;
                let bytes = next.to_be_bytes();
                if excluded.binary_search(&bytes).is_err() {
                    return Ok(bytes);
                }
            }
            Err(InputError::Capacity)
        };
        self.clone().build(context, &mut ids)?;
        Ok(())
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
            Self::LedgerSummary(_) => return Ok(PlannedOperation::Summary),
            Self::MonitorRegister(document) => document.build(context, ids)?,
            Self::MonitorGet(document) => return document.build().map(PlannedOperation::Monitor),
            Self::ValidatorList(document) => {
                return document
                    .build(context, false)
                    .map(PlannedOperation::Validators);
            }
            Self::ValidatorGet(document) => {
                return document
                    .build(context, true)
                    .map(PlannedOperation::Validators);
            }
            Self::LedgerTraverse(document) => {
                return document.build(context).map(PlannedOperation::Traverse);
            }
            Self::RequestEpoch(document) => {
                return document.build().map(PlannedOperation::Reconcile);
            }
            Self::RequestStatus(document) => {
                return document.build().map(PlannedOperation::Reconcile);
            }
            Self::ClaimSubmit(document) => document.build(context, ids)?,
            Self::ClaimSubmitBatch(document) => document.build(context, ids)?,
            Self::TestamentSubmit(document) => document.build(context, ids)?,
            Self::ArtifactSubmit(document) => document.build(context, ids)?,
            Self::ArtifactRegister(document) => document.build(context, ids)?,
            Self::TestamentReceive(document) => Command::AcknowledgeTestament {
                claim: ClaimId(parse_id(&document.claim)?),
                testament: TestamentId(parse_id(&document.testament)?),
            },
            Self::ValidationBegin(document) => Command::BeginWholeWorkValidation {
                claim: ClaimId(parse_id(&document.claim)?),
            },
            Self::ValidationBeginIncrement(document) => Command::BeginIncrementValidation {
                claim: ClaimId(parse_id(&document.claim)?),
                validation: ValidationId(parse_id(&document.validation)?),
                target: parse_hash(&document.target_hash)?,
                manifest: parse_hash(&document.manifest)?,
            },
            Self::ValidationComplete(document) => Command::CompleteWholeWork {
                claim: ClaimId(parse_id(&document.claim)?),
            },
            Self::ValidationSubmit(document) => document.build(context)?,
            Self::ClaimSupersede(document) => document.build(context, ids)?,
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
            Self::ClaimWait(document) => {
                let (read, until, timeout_ms) = document.build(context)?;
                return Ok(PlannedOperation::ClaimWait {
                    read,
                    until,
                    timeout_ms,
                });
            }
            Self::ClaimGet(document) => {
                return document.build(context).map(PlannedOperation::ClaimGet);
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
            Self::ValidationContext(document) => {
                return document
                    .build(ObjectKind::Validation, context)
                    .map(PlannedOperation::ValidationContext);
            }
            Self::ClaimList(document) => {
                return document
                    .build_operation(ObjectKind::Claim, context)
                    .and_then(selection::planned);
            }
            Self::TestamentList(document) => {
                return document
                    .build_operation(ObjectKind::Testament, context)
                    .and_then(selection::planned);
            }
            Self::ArtifactList(document) => {
                return document
                    .build_operation(ObjectKind::Artifact, context)
                    .and_then(selection::planned);
            }
            Self::ValidationList(document) => {
                return document
                    .build_operation(ObjectKind::Validation, context)
                    .and_then(selection::planned);
            }
        };
        Ok(PlannedOperation::Mutation(command))
    }
}

// Canonical serde JSON leaves hexadecimal identities unescaped. Walk string
// boundaries once (respecting escaped quotes), then sort only exact 32-digit
// strings. Prose containing an ID is not an identity field. This bounds work by
// input bytes plus sorting/searching the IDs, even for many consecutive IDs.
fn authored_ids(canonical: &[u8]) -> Result<Vec<[u8; 16]>, InputError> {
    let mut ids = Vec::new();
    let mut start = None;
    let mut escaped = false;
    for (index, byte) in canonical.iter().copied().enumerate() {
        if let Some(begin) = start {
            if escaped {
                escaped = false;
            } else if byte == b'\\' {
                escaped = true;
            } else if byte == b'"' {
                let text = canonical.get(begin..index).ok_or(InputError::Capacity)?;
                if text.len() == 32
                    && text.iter().all(u8::is_ascii_hexdigit)
                    && let Ok(id) =
                        parse_id(std::str::from_utf8(text).map_err(|_| InputError::Capacity)?)
                {
                    ids.try_reserve(1).map_err(|_| InputError::Capacity)?;
                    ids.push(id);
                }
                start = None;
            }
        } else if byte == b'"' {
            start = Some(index.checked_add(1).ok_or(InputError::Capacity)?);
        }
    }
    ids.sort_unstable();
    ids.dedup();
    Ok(ids)
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

#[cfg(test)]
mod selection_tests;
