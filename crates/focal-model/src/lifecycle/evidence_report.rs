//! Participant-authored work reports. Construction is bounded and fallible; no
//! receipt event can construct a report, and no reported outcome is a verdict.
use super::*;
use crate::lifecycle::memory as bytes;
use crate::{Confidence, OutcomeKind};

#[path = "evidence_snapshot.rs"]
mod snapshot;
pub use snapshot::{
    FailedWorkSnapshotV1, ResponseArtifacts, ResponseDiagnosticSnapshotV1, ResponseHydrationPlan,
    ResponseSnapshotFieldsV1, ResponseSnapshotSource, ResponseSnapshotV1,
    ResponseTerminalSnapshotV1, WorkArtifactSnapshotV1, WorkTerminalSnapshotV1,
};

/// Limits are supplied by the effective owner, never by an untrusted request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ResponseLimits {
    pub artifacts: usize,
    pub diagnostics: usize,
    pub summary_bytes: usize,
    pub construction_bytes: usize,
}

/// A real diagnostic artifact authored under this work cycle's entitlement.
/// It is intentionally not a slot binding: an error report cannot stand in for a
/// missing work product merely because both are durable artifacts.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ResponseDiagnostic {
    ledger: LedgerId,
    claim: ClaimId,
    receipt: ReceiptFence,
    cycle: u32,
    producer: ParticipantId,
    diagnostic: Diagnostic,
}

impl ResponseDiagnostic {
    /// Record a native diagnostic from the exact descriptor and verified local
    /// custody resolved by the owner. Typed work provenance must name this
    /// exact claim, cycle and diagnostic reason. This creates a token for the
    /// effective entitlement and never converts validator results into work.
    pub fn record_native(
        parent: &Parent,
        principal: Principal,
        receipt: ReceiptFence,
        diagnostic: Diagnostic,
        source: &crate::lifecycle::artifact_descriptor::ArtifactDescriptor,
        custody: &EvidenceAttestation,
    ) -> Result<Self, ContractError> {
        use crate::lifecycle::artifact_descriptor::{WorkProvenance, WorkRole};
        parent.check_identity(parent.ledger, parent.claim)?;
        parent.check_receipt(receipt)?;
        parent.require_open_response()?;
        principal.require_actor(parent.holder)?;
        if source.id() != diagnostic.artifact.id
            || source.content_hash() != diagnostic.artifact.hash
        {
            return Err(ContractError::MissingEvidence);
        }
        if source.ledger() != parent.ledger {
            return Err(ContractError::WrongLedger);
        }
        if source.producer() != parent.holder {
            return Err(ContractError::WrongActor);
        }
        if source.receipt() != Some(receipt) {
            return Err(ContractError::StaleReceipt);
        }
        if source.kind() != "error"
            || source.schema() == 0
            || source.schema_hash() == crate::ContentHash([0; 32])
            || source.result_provenance().is_some()
            || source.work_provenance()
                != Some(WorkProvenance {
                    claim: parent.claim,
                    cycle: parent.next_cycle,
                    role: WorkRole::Diagnostic {
                        reason: diagnostic.reason,
                    },
                })
        {
            return Err(ContractError::MissingEvidence);
        }
        check_evidence(diagnostic.artifact, custody)?;
        Ok(Self {
            ledger: parent.ledger,
            claim: parent.claim,
            receipt,
            cycle: parent.next_cycle,
            producer: parent.holder,
            diagnostic,
        })
    }

    /// `source` is the exact immutable artifact row resolved by the owner, with
    /// its map key. Its canonical content hash and custody must already be
    /// verified; neither a request-provided descriptor nor an attestation alone
    /// is a substitute for that row. Payloads are not copied or rehashed here.
    pub fn record(
        parent: &Parent,
        principal: Principal,
        receipt: ReceiptFence,
        diagnostic: Diagnostic,
        source: (crate::ArtifactId, &crate::Artifact),
        custody: &EvidenceAttestation,
    ) -> Result<Self, ContractError> {
        parent.check_identity(parent.ledger, parent.claim)?;
        parent.check_receipt(receipt)?;
        parent.require_open_response()?;
        principal.require_actor(parent.holder)?;
        let content = source.1.content();
        if source.0 != diagnostic.artifact.id || source.1.content_hash() != diagnostic.artifact.hash
        {
            return Err(ContractError::MissingEvidence);
        }
        if content.ledger != parent.ledger {
            return Err(ContractError::WrongLedger);
        }
        if content.producer != parent.holder {
            return Err(ContractError::WrongActor);
        }
        if content.receipt != Some(receipt) {
            return Err(ContractError::StaleReceipt);
        }
        if content.kind != "error"
            || content.schema == 0
            || content.schema_hash == crate::ContentHash([0; 32])
            || source.1.lifecycle().created.0 == 0
            || source.1.lifecycle().custody_revision != custody.custody_revision
        {
            return Err(ContractError::MissingEvidence);
        }
        check_evidence(diagnostic.artifact, custody)?;
        Ok(Self {
            ledger: parent.ledger,
            claim: parent.claim,
            receipt,
            cycle: parent.next_cycle,
            producer: parent.holder,
            diagnostic,
        })
    }

    pub fn diagnostic(&self) -> Diagnostic {
        self.diagnostic
    }
    pub fn artifact(&self) -> ArtifactRef {
        self.diagnostic.artifact
    }
    pub fn producer(&self) -> ParticipantId {
        self.producer
    }
    pub fn receipt(&self) -> ReceiptFence {
        self.receipt
    }
    pub fn claim(&self) -> ClaimId {
        self.claim
    }
    pub fn cycle(&self) -> u32 {
        self.cycle
    }

    pub fn check_parent(&self, parent: &Parent) -> Result<(), ContractError> {
        parent.check_identity(self.ledger, self.claim)?;
        parent.check_receipt(self.receipt)?;
        if self.producer != parent.holder {
            return Err(ContractError::WrongActor);
        }
        if self.cycle != parent.next_cycle {
            return Err(ContractError::InvalidManifest);
        }
        Ok(())
    }
}

/// Every close supplies the respondent's actual report. There is deliberately
/// no default completion outcome. Diagnostics are ordered by artifact ID, while
/// the independent work manifest is ordered by declared slot index.
#[derive(Debug, Clone, Copy)]
pub struct CloseReport<'a> {
    pub summary: &'a str,
    pub confidence: Confidence,
    pub outcome: OutcomeKind,
    pub diagnostics: &'a [ResponseDiagnostic],
    pub limits: ResponseLimits,
}

#[derive(Debug, PartialEq, Eq)]
#[cfg_attr(test, derive(Clone))]
pub(super) struct ReportedWork {
    summary: String,
    confidence: Confidence,
    outcome: OutcomeKind,
    diagnostics: Vec<ResponseDiagnostic>,
}

/// Private semantic binding for native checked plans, not a persisted content
/// identity or wire tag. The successor durable format must define its own hash.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::lifecycle) struct ReportStamp([u8; 32]);
impl ReportStamp {
    pub(in crate::lifecycle) fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    #[cfg(test)]
    pub(in crate::lifecycle) fn fixture() -> Self {
        Self([0; 32])
    }
}

/// All authority, identities, content and bounds have been checked before this
/// allocation-free plan is returned. The owner reserves its construction charge
/// before building and publishes the response and all attachments atomically.
#[derive(Debug)]
pub struct ClosePreparation<'a> {
    identity: ResponseIdentity,
    respondent: ParticipantId,
    current: &'a [WorkArtifact],
    attachable: usize,
    failed: usize,
    report: CloseReport<'a>,
    stamp: ReportStamp,
    charge: usize,
}

impl ClosePreparation<'_> {
    /// Native plan/rows plus requested exact capacities. The storage owner
    /// separately accounts for allocator metadata before building this plan.
    pub fn construction_charge(&self) -> usize {
        self.charge
    }
    pub fn construction_heap_allocations(&self) -> Result<usize, ContractError> {
        add(
            bytes::allocation::<u8>(self.report.summary.len()),
            add(
                bytes::allocation::<SlotBinding>(self.attachable),
                add(
                    bytes::allocation::<WorkArtifact>(self.attachable),
                    add(
                        bytes::allocation::<FailedWork>(self.failed),
                        bytes::allocation::<ResponseDiagnostic>(self.report.diagnostics.len()),
                    )?,
                )?,
            )?,
        )
    }

    pub fn build(self) -> Result<ClosePlan, ContractError> {
        let summary = bytes::copy(self.report.summary.as_bytes())?;
        bytes::fits(summary.capacity(), self.report.summary.len())?;
        let mut manifest = bytes::reserve(self.attachable)?;
        bytes::fits(manifest.capacity(), self.attachable)?;
        let mut attachments = bytes::reserve(self.attachable)?;
        bytes::fits(attachments.capacity(), self.attachable)?;
        let mut failed_work = bytes::reserve(self.failed)?;
        bytes::fits(failed_work.capacity(), self.failed)?;
        let diagnostics = bytes::copy(self.report.diagnostics)?;
        bytes::fits(diagnostics.capacity(), self.report.diagnostics.len())?;
        for artifact in self.current {
            if let Some(failed) = FailedWork::from_artifact(artifact)? {
                failed_work.push(failed);
                continue;
            }
            manifest.push(SlotBinding {
                slot: artifact.slot,
                artifact: artifact.reference(),
            });
            attachments.push(WorkArtifact {
                binding: artifact.binding.next()?,
                state: WorkArtifactState::Attached,
                attachment: Some(self.identity.binding),
                ..*artifact
            });
        }
        let plan = ClosePlan {
            response: Response {
                identity: self.identity,
                respondent: self.respondent,
                state: ResponseState::Generated,
                manifest,
                failed_work,
                report: ReportedWork {
                    summary: String::from_utf8(summary)
                        .map_err(|_| ContractError::InvalidManifest)?,
                    confidence: self.report.confidence,
                    outcome: self.report.outcome,
                    diagnostics,
                },
                stamp: self.stamp,
                terminal: None,
            },
            attachments,
        };
        bytes::fits(plan.retained_bytes()?, self.charge)?;
        Ok(plan)
    }
}

impl Response {
    pub fn close(
        identity: ResponseIdentity,
        parent: &Parent,
        principal: Principal,
        current: &[WorkArtifact],
        expected_manifest: &[SlotBinding],
        report: CloseReport<'_>,
    ) -> Result<ClosePlan, ContractError> {
        Self::prepare_close(
            identity,
            parent,
            principal,
            current,
            expected_manifest,
            report,
        )?
        .build()
    }

    pub fn prepare_close<'a>(
        identity: ResponseIdentity,
        parent: &Parent,
        principal: Principal,
        current: &'a [WorkArtifact],
        expected_manifest: &[SlotBinding],
        report: CloseReport<'a>,
    ) -> Result<ClosePreparation<'a>, ContractError> {
        parent.check_identity(identity.binding.ledger, identity.claim)?;
        if identity.binding.object.is_zero() {
            return Err(ContractError::InvalidTarget);
        }
        parent.check_receipt(identity.receipt)?;
        parent.require_open_response()?;
        principal.require_actor(parent.holder)?;
        if identity.cycle != parent.next_cycle
            || identity.prior != parent.latest_response
            || identity.cycle.checked_add(1).is_none()
        {
            return Err(ContractError::InvalidManifest);
        }
        let limits = report.limits;
        if current.len() > limits.artifacts
            || expected_manifest.len() > limits.artifacts
            || report.diagnostics.len() > limits.diagnostics
            || report.summary.len() > limits.summary_bytes
        {
            return Err(ContractError::Capacity);
        }
        if report.summary.trim().is_empty() {
            return Err(ContractError::InvalidManifest);
        }
        if report.outcome != OutcomeKind::Complete && report.diagnostics.is_empty() {
            return Err(ContractError::MissingEvidence);
        }
        let mut attachable = 0;
        let mut failed = 0;
        let mut previous_slot = None;
        for (position, artifact) in current.iter().enumerate() {
            parent.check_identity(artifact.binding.ledger, artifact.claim)?;
            parent.check_receipt(artifact.receipt)?;
            if artifact.producer != parent.holder {
                return Err(ContractError::WrongActor);
            }
            if artifact.cycle != identity.cycle {
                return Err(ContractError::InvalidManifest);
            }
            if artifact.attachment.is_some() {
                return Err(ContractError::InvalidTransition);
            }
            if previous_slot.is_some_and(|prior| prior >= artifact.slot)
                || current
                    .iter()
                    .take(position)
                    .any(|old| old.reference().id == artifact.reference().id)
            {
                return Err(ContractError::InvalidManifest);
            }
            if FailedWork::from_artifact(artifact)?.is_some() {
                failed = add(failed, 1)?;
            } else {
                let expected = expected_manifest
                    .get(attachable)
                    .ok_or(ContractError::InvalidManifest)?;
                if artifact.slot != expected.slot || artifact.reference() != expected.artifact {
                    return Err(ContractError::InvalidManifest);
                }
                artifact.binding.next()?;
                attachable = add(attachable, 1)?;
            }
            previous_slot = Some(artifact.slot);
        }
        if attachable != expected_manifest.len() {
            return Err(ContractError::InvalidManifest);
        }
        let mut previous_diagnostic = None;
        for diagnostic in report.diagnostics {
            diagnostic.check_parent(parent)?;
            let id = diagnostic.artifact().id;
            if previous_diagnostic.is_some_and(|previous| previous >= id)
                || current.iter().any(|artifact| {
                    artifact.reference().id == id
                        && !(artifact.state == WorkArtifactState::GenerationFailed
                            && artifact.diagnostic == Some(diagnostic.diagnostic)
                            && artifact.reference() == diagnostic.artifact())
                })
            {
                return Err(ContractError::InvalidManifest);
            }
            previous_diagnostic = Some(id);
        }
        let charge = add(
            add(std::mem::size_of::<ClosePlan>(), report.summary.len())?,
            add(
                multiply(
                    attachable,
                    add(
                        std::mem::size_of::<SlotBinding>(),
                        std::mem::size_of::<WorkArtifact>(),
                    )?,
                )?,
                add(
                    bytes::array::<FailedWork>(failed)?,
                    multiply(
                        report.diagnostics.len(),
                        std::mem::size_of::<ResponseDiagnostic>(),
                    )?,
                )?,
            )?,
        )?;
        if charge > limits.construction_bytes {
            return Err(ContractError::Capacity);
        }
        let stamp = report_stamp(
            identity,
            parent.holder,
            expected_manifest,
            current,
            failed,
            report,
        )?;
        Ok(ClosePreparation {
            identity,
            respondent: parent.holder,
            current,
            attachable,
            failed,
            report,
            stamp,
            charge,
        })
    }

    pub fn respondent(&self) -> ParticipantId {
        self.respondent
    }
    pub fn summary(&self) -> &str {
        &self.report.summary
    }
    pub fn confidence(&self) -> Confidence {
        self.report.confidence
    }
    pub fn reported_outcome(&self) -> OutcomeKind {
        self.report.outcome
    }
    pub fn diagnostics(&self) -> &[ResponseDiagnostic] {
        &self.report.diagnostics
    }
    pub(in crate::lifecycle) fn report_stamp(&self) -> ReportStamp {
        self.stamp
    }

    /// Actual native row and owned buffer capacities, excluding allocator metadata.
    pub fn retained_bytes(&self) -> Result<usize, ContractError> {
        bytes::total::<Self>(self.retained_heap_bytes()?)
    }

    pub fn retained_heap_bytes(&self) -> Result<usize, ContractError> {
        add(
            self.report.summary.capacity(),
            add(
                bytes::array::<SlotBinding>(self.manifest.capacity())?,
                add(
                    bytes::array::<FailedWork>(self.failed_work.capacity())?,
                    bytes::array::<ResponseDiagnostic>(self.report.diagnostics.capacity())?,
                )?,
            )?,
        )
    }
    pub fn heap_allocations(&self) -> Result<usize, ContractError> {
        add(
            bytes::allocation::<u8>(self.report.summary.capacity()),
            add(
                bytes::allocation::<SlotBinding>(self.manifest.capacity()),
                add(
                    bytes::allocation::<FailedWork>(self.failed_work.capacity()),
                    bytes::allocation::<ResponseDiagnostic>(self.report.diagnostics.capacity()),
                )?,
            )?,
        )
    }
    pub fn copy_heap_bytes(&self) -> Result<usize, ContractError> {
        add(
            self.report.summary.len(),
            add(
                bytes::array::<SlotBinding>(self.manifest.len())?,
                add(
                    bytes::array::<FailedWork>(self.failed_work.len())?,
                    bytes::array::<ResponseDiagnostic>(self.report.diagnostics.len())?,
                )?,
            )?,
        )
    }
    pub fn copy_heap_allocations(&self) -> Result<usize, ContractError> {
        add(
            bytes::allocation::<u8>(self.report.summary.len()),
            add(
                bytes::allocation::<SlotBinding>(self.manifest.len()),
                add(
                    bytes::allocation::<FailedWork>(self.failed_work.len()),
                    bytes::allocation::<ResponseDiagnostic>(self.report.diagnostics.len()),
                )?,
            )?,
        )
    }
    pub fn copy_charge(&self) -> Result<usize, ContractError> {
        bytes::total::<Self>(self.copy_heap_bytes()?)
    }
    /// The owner reserves the complete charge before calling. Copying preserves
    /// immutable report identity, state and terminal cuts without readmission.
    /// Inline row size is included; allocator metadata is separately counted.
    pub fn try_copy(&self, max_bytes: usize) -> Result<Self, ContractError> {
        bytes::fits(self.copy_charge()?, max_bytes)?;
        let summary = bytes::copy(self.report.summary.as_bytes())?;
        bytes::fits(summary.capacity(), self.report.summary.len())?;
        let manifest = bytes::copy(&self.manifest)?;
        bytes::fits(manifest.capacity(), self.manifest.len())?;
        let failed_work = bytes::copy(&self.failed_work)?;
        bytes::fits(failed_work.capacity(), self.failed_work.len())?;
        let diagnostics = bytes::copy(&self.report.diagnostics)?;
        bytes::fits(diagnostics.capacity(), self.report.diagnostics.len())?;
        let copied = Self {
            identity: self.identity,
            respondent: self.respondent,
            state: self.state,
            manifest,
            failed_work,
            report: ReportedWork {
                summary: String::from_utf8(summary).map_err(|_| ContractError::InvalidManifest)?,
                confidence: self.report.confidence,
                outcome: self.report.outcome,
                diagnostics,
            },
            stamp: self.stamp,
            terminal: self.terminal,
        };
        bytes::fits(copied.retained_bytes()?, max_bytes)?;
        Ok(copied)
    }
}

impl ClosePlan {
    pub fn heap_allocations(&self) -> Result<usize, ContractError> {
        add(
            self.response.heap_allocations()?,
            bytes::allocation::<WorkArtifact>(self.attachments.capacity()),
        )
    }
    pub fn retained_bytes(&self) -> Result<usize, ContractError> {
        add(
            add(
                self.response.retained_bytes()?,
                std::mem::size_of::<Vec<WorkArtifact>>(),
            )?,
            multiply(
                self.attachments.capacity(),
                std::mem::size_of::<WorkArtifact>(),
            )?,
        )
    }
}

fn add(a: usize, b: usize) -> Result<usize, ContractError> {
    a.checked_add(b).ok_or(ContractError::Capacity)
}
fn multiply(a: usize, b: usize) -> Result<usize, ContractError> {
    a.checked_mul(b).ok_or(ContractError::Capacity)
}

fn report_stamp(
    identity: ResponseIdentity,
    respondent: ParticipantId,
    manifest: &[SlotBinding],
    current: &[WorkArtifact],
    failed_count: usize,
    report: CloseReport<'_>,
) -> Result<ReportStamp, ContractError> {
    report_stamp_body(
        identity,
        respondent,
        ReportBody {
            summary: report.summary,
            confidence: report.confidence,
            outcome: report.outcome,
            manifest_count: manifest.len(),
            manifest: manifest.iter().copied().map(Ok),
            failed_count,
            failed: current
                .iter()
                .filter_map(|artifact| FailedWork::from_artifact(artifact).transpose()),
            diagnostic_count: report.diagnostics.len(),
            diagnostics: report.diagnostics.iter().copied().map(Ok),
        },
    )
}

struct ReportBody<'a, M, F, D> {
    summary: &'a str,
    confidence: Confidence,
    outcome: OutcomeKind,
    manifest_count: usize,
    manifest: M,
    failed_count: usize,
    failed: F,
    diagnostic_count: usize,
    diagnostics: D,
}

fn report_stamp_body(
    identity: ResponseIdentity,
    respondent: ParticipantId,
    mut body: ReportBody<
        '_,
        impl Iterator<Item = Result<SlotBinding, ContractError>>,
        impl Iterator<Item = Result<FailedWork, ContractError>>,
        impl Iterator<Item = Result<ResponseDiagnostic, ContractError>>,
    >,
) -> Result<ReportStamp, ContractError> {
    struct Stamp(blake3::Hasher);
    impl Stamp {
        fn field(&mut self, bytes: &[u8]) -> Result<(), ContractError> {
            let len = u64::try_from(bytes.len()).map_err(|_| ContractError::Capacity)?;
            self.0.update(&len.to_be_bytes());
            self.0.update(bytes);
            Ok(())
        }
        fn count(&mut self, count: usize) -> Result<(), ContractError> {
            self.field(
                &u64::try_from(count)
                    .map_err(|_| ContractError::Capacity)?
                    .to_be_bytes(),
            )
        }
    }
    let mut stamp = Stamp(blake3::Hasher::new_derive_key(
        "focal native authored response binding",
    ));
    stamp.field(&identity.binding.ledger.tenant.0)?;
    stamp.field(&identity.binding.ledger.session.0)?;
    stamp.field(&identity.binding.object.0)?;
    stamp.field(&identity.binding.content.0)?;
    stamp.field(&identity.binding.revision.0.to_be_bytes())?;
    stamp.field(&identity.claim.0)?;
    stamp.field(&identity.receipt.receipt.0)?;
    stamp.field(&identity.receipt.epoch.to_be_bytes())?;
    stamp.field(&identity.cycle.to_be_bytes())?;
    match identity.prior {
        None => stamp.field(b"no-prior")?,
        Some(prior) => {
            stamp.field(b"prior")?;
            stamp.field(&prior.0)?;
        }
    }
    stamp.field(&respondent.0)?;
    stamp.field(body.summary.as_bytes())?;
    stamp.field(match body.confidence {
        Confidence::Hint => b"hint",
        Confidence::Tentative => b"tentative",
        Confidence::Committed => b"committed",
        Confidence::Consensus => b"consensus",
    })?;
    stamp.field(match body.outcome {
        OutcomeKind::Complete => b"complete",
        OutcomeKind::Partial => b"partial",
        OutcomeKind::Refused => b"refused",
        OutcomeKind::Impossible => b"impossible",
        OutcomeKind::Interrupted => b"interrupted",
        OutcomeKind::Failed => b"failed",
    })?;
    stamp.count(body.manifest_count)?;
    for _ in 0..body.manifest_count {
        let entry = body
            .manifest
            .next()
            .ok_or(ContractError::InvalidManifest)??;
        stamp.field(&entry.slot.to_be_bytes())?;
        stamp.field(&entry.artifact.id.0)?;
        stamp.field(&entry.artifact.hash.0)?;
    }
    if body.manifest.next().is_some() {
        return Err(ContractError::InvalidManifest);
    }
    // This is an in-memory guard, not a durable hash. Include the exact failed
    // row revision and diagnostic even though failed rows are never attached.
    stamp.count(body.failed_count)?;
    for _ in 0..body.failed_count {
        let failed = body.failed.next().ok_or(ContractError::InvalidManifest)??;
        stamp.field(&failed.binding.ledger.tenant.0)?;
        stamp.field(&failed.binding.ledger.session.0)?;
        stamp.field(&failed.binding.object.0)?;
        stamp.field(&failed.binding.content.0)?;
        stamp.field(&failed.binding.revision.0.to_be_bytes())?;
        stamp.field(&failed.slot.to_be_bytes())?;
        stamp.field(match failed.state {
            WorkArtifactState::GenerationFailed => b"generation-failed",
            WorkArtifactState::ReceiptFailed => b"receipt-failed",
            _ => return Err(ContractError::InvalidTransition),
        })?;
        stamp.field(&failed.diagnostic.artifact.id.0)?;
        stamp.field(&failed.diagnostic.artifact.hash.0)?;
        stamp.field(match failed.diagnostic.reason {
            EvidenceFailure::Work => b"work",
            EvidenceFailure::Production => b"production",
            EvidenceFailure::Structure => b"structure",
            EvidenceFailure::Metadata => b"metadata",
        })?;
    }
    if body.failed.next().is_some() {
        return Err(ContractError::InvalidManifest);
    }
    stamp.count(body.diagnostic_count)?;
    for _ in 0..body.diagnostic_count {
        let entry = body
            .diagnostics
            .next()
            .ok_or(ContractError::InvalidManifest)??;
        // Ledger/claim/receipt/cycle/producer are checked equal to the already
        // encoded response entitlement before this stamp is constructed.
        stamp.field(&entry.artifact().id.0)?;
        stamp.field(&entry.artifact().hash.0)?;
        stamp.field(match entry.diagnostic.reason {
            EvidenceFailure::Work => b"work",
            EvidenceFailure::Production => b"production",
            EvidenceFailure::Structure => b"structure",
            EvidenceFailure::Metadata => b"metadata",
        })?;
    }
    if body.diagnostics.next().is_some() {
        return Err(ContractError::InvalidManifest);
    }
    Ok(ReportStamp(*stamp.0.finalize().as_bytes()))
}
