//! Participant-authored work reports. Construction is bounded and fallible; no
//! receipt event can construct a report, and no reported outcome is a verdict.
use super::*;
use crate::{Confidence, OutcomeKind};

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

    pub(in crate::lifecycle) fn check_parent(&self, parent: &Parent) -> Result<(), ContractError> {
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
    report: CloseReport<'a>,
    stamp: ReportStamp,
    charge: usize,
}

impl ClosePreparation<'_> {
    /// Native plan/rows plus requested exact capacities; allocator metadata is
    /// excluded. This is not yet integrated with the storage MemoryBudget.
    pub fn construction_charge(&self) -> usize {
        self.charge
    }

    pub fn build(self) -> Result<ClosePlan, ContractError> {
        let mut summary = String::new();
        let mut manifest = Vec::new();
        let mut attachments = Vec::new();
        let mut diagnostics = Vec::new();
        summary
            .try_reserve_exact(self.report.summary.len())
            .map_err(|_| ContractError::Capacity)?;
        manifest
            .try_reserve_exact(self.current.len())
            .map_err(|_| ContractError::Capacity)?;
        attachments
            .try_reserve_exact(self.current.len())
            .map_err(|_| ContractError::Capacity)?;
        diagnostics
            .try_reserve_exact(self.report.diagnostics.len())
            .map_err(|_| ContractError::Capacity)?;
        summary.push_str(self.report.summary);
        diagnostics.extend_from_slice(self.report.diagnostics);
        for artifact in self.current {
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
        Ok(ClosePlan {
            response: Response {
                identity: self.identity,
                respondent: self.respondent,
                state: ResponseState::Generated,
                manifest,
                report: ReportedWork {
                    summary,
                    confidence: self.report.confidence,
                    outcome: self.report.outcome,
                    diagnostics,
                },
                stamp: self.stamp,
                terminal: None,
            },
            attachments,
        })
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
        if current.len() != expected_manifest.len() {
            return Err(ContractError::InvalidManifest);
        }
        let mut previous_slot = None;
        for (position, (artifact, expected)) in current.iter().zip(expected_manifest).enumerate() {
            parent.check_identity(artifact.binding.ledger, artifact.claim)?;
            parent.check_receipt(artifact.receipt)?;
            if artifact.producer != parent.holder {
                return Err(ContractError::WrongActor);
            }
            if artifact.cycle != identity.cycle {
                return Err(ContractError::InvalidManifest);
            }
            if !matches!(
                artifact.state,
                WorkArtifactState::Generated | WorkArtifactState::Received
            ) || artifact.attachment.is_some()
            {
                return Err(ContractError::InvalidTransition);
            }
            if artifact.slot != expected.slot
                || artifact.reference() != expected.artifact
                || previous_slot.is_some_and(|prior| prior >= artifact.slot)
                || current
                    .iter()
                    .take(position)
                    .any(|old| old.reference().id == artifact.reference().id)
            {
                return Err(ContractError::InvalidManifest);
            }
            artifact.binding.next()?;
            previous_slot = Some(artifact.slot);
        }
        let mut previous_diagnostic = None;
        for diagnostic in report.diagnostics {
            diagnostic.check_parent(parent)?;
            let id = diagnostic.artifact().id;
            if previous_diagnostic.is_some_and(|previous| previous >= id)
                || current.iter().any(|artifact| artifact.reference().id == id)
            {
                return Err(ContractError::InvalidManifest);
            }
            previous_diagnostic = Some(id);
        }
        let charge = add(
            add(std::mem::size_of::<ClosePlan>(), report.summary.len())?,
            add(
                multiply(
                    current.len(),
                    add(
                        std::mem::size_of::<SlotBinding>(),
                        std::mem::size_of::<WorkArtifact>(),
                    )?,
                )?,
                multiply(
                    report.diagnostics.len(),
                    std::mem::size_of::<ResponseDiagnostic>(),
                )?,
            )?,
        )?;
        if charge > limits.construction_bytes {
            return Err(ContractError::Capacity);
        }
        let stamp = report_stamp(identity, parent.holder, expected_manifest, report)?;
        Ok(ClosePreparation {
            identity,
            respondent: parent.holder,
            current,
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
        add(
            add(std::mem::size_of::<Self>(), self.report.summary.capacity())?,
            add(
                multiply(self.manifest.capacity(), std::mem::size_of::<SlotBinding>())?,
                multiply(
                    self.report.diagnostics.capacity(),
                    std::mem::size_of::<ResponseDiagnostic>(),
                )?,
            )?,
        )
    }
}

impl ClosePlan {
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
    report: CloseReport<'_>,
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
    stamp.field(report.summary.as_bytes())?;
    stamp.field(match report.confidence {
        Confidence::Hint => b"hint",
        Confidence::Tentative => b"tentative",
        Confidence::Committed => b"committed",
        Confidence::Consensus => b"consensus",
    })?;
    stamp.field(match report.outcome {
        OutcomeKind::Complete => b"complete",
        OutcomeKind::Partial => b"partial",
        OutcomeKind::Refused => b"refused",
        OutcomeKind::Impossible => b"impossible",
        OutcomeKind::Interrupted => b"interrupted",
        OutcomeKind::Failed => b"failed",
    })?;
    stamp.count(manifest.len())?;
    for entry in manifest {
        stamp.field(&entry.slot.to_be_bytes())?;
        stamp.field(&entry.artifact.id.0)?;
        stamp.field(&entry.artifact.hash.0)?;
    }
    stamp.count(report.diagnostics.len())?;
    for entry in report.diagnostics {
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
    Ok(ReportStamp(*stamp.0.finalize().as_bytes()))
}
