//! Local verified-through custody of an exact fsynced session checkpoint.
//! Verification targets never replace active ingress placement. The opaque
//! result is input to authority checking, not a self-authorizing ready fact.
use crate::custody::{CustodyScope, CustodyStore, content_error};
use focal_evidence::{CustodyRecordKind, MAX_TRANSFER_MANIFEST_BYTES, TransferManifest};
use focal_ledger::{CommittedPlacement, DurableEvidenceSnapshot, EvidencePrefix};
use focal_memory::{Allocation, BudgetKind, BudgetLane, MemoryBudget};
use focal_model::{ArtifactId, ContentHash};
use focal_wire::AccessError;
use serde::{Deserialize, Serialize};

const RESIDENT_BYTES: usize = 4 * MAX_TRANSFER_MANIFEST_BYTES + 16 * 1024;
const CHECKPOINT_SCRATCH: usize = 8 * 1024 * 1024;

/// Durable record, intentionally distinct from the non-deserializable witness.
/// Reopen can inspect/check this record, but must reverify the exact checkpoint
/// and referenced content before producing a fresh readiness witness.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CustodyPrefixManifest {
    pub schema: u16,
    pub prefix: EvidencePrefix,
    pub artifact_digest: ContentHash,
    pub verified_artifacts: u64,
    pub verified_content_references: u64,
    pub verified_content_bytes: u64,
}
pub struct VerifiedCustody {
    manifest: CustodyPrefixManifest,
    digest: ContentHash,
    placement: CommittedPlacement,
    _allocation: Allocation,
}
impl VerifiedCustody {
    pub fn manifest(&self) -> &CustodyPrefixManifest {
        &self.manifest
    }
    pub fn digest(&self) -> ContentHash {
        self.digest
    }
    pub fn placement(&self) -> &CommittedPlacement {
        &self.placement
    }
    /// Readiness for a move must cover its final committed cutover, including
    /// any artifacts added after an earlier checkpoint was exported. This is
    /// a local prefix check; the signer still checks current root authority.
    pub fn verify_cutover(&self, cutover: &CommittedPlacement) -> Result<(), AccessError> {
        let prefix = &self.manifest.prefix;
        let fence = cutover.fence();
        if fence.kind != focal_directory::SessionFenceKind::Cutover
            || prefix.cluster != cutover.cluster()
            || prefix.genesis != cutover.genesis()
            || prefix.ledger != fence.ledger
            || prefix.group != fence.log_group
            || prefix.operation != fence.operation
            || prefix.route != fence.to_route
            || prefix.placement_epoch != fence.placement_epoch
            || prefix.membership_epoch != fence.membership_epoch
            || prefix.placement_digest != fence.placement_digest
            || prefix.sequence < fence.sequence
            || prefix.index < fence.index
            || prefix.term < fence.term
        {
            return Err(AccessError::Unavailable);
        }
        Ok(())
    }
}

/// By-value continuation. It cannot be cloned, decoded, or supplied with an
/// arbitrary artifact list. Cancellation drops the owned buffers/permits; the
/// source session's bounded snapshot registry expires its page pin at the TTL.
pub struct CustodyVerification {
    snapshot: DurableEvidenceSnapshot,
    expected_active: Option<CustodyScope>,
    target: CustodyScope,
    checkpoint_installed: bool,
    after: Option<ArtifactId>,
    artifacts: u64,
    contents: u64,
    bytes: u64,
    digest: blake3::Hasher,
    transfer: Option<TransferManifest>,
    next_chunk: usize,
    content_digest: blake3::Hasher,
    _allocation: Allocation,
}
pub enum CustodyVerificationProgress {
    Pending(Box<CustodyVerification>),
    Complete(Box<VerifiedCustody>),
}
impl CustodyVerification {
    pub(crate) fn new(
        snapshot: DurableEvidenceSnapshot,
        expected_active: Option<CustodyScope>,
        budget: &MemoryBudget,
    ) -> Result<Self, AccessError> {
        let prefix = snapshot.prefix();
        let target = CustodyScope {
            ledger: prefix.ledger,
            route_epoch: prefix.route,
            policy_revision: prefix.placement_epoch,
        };
        let placement = &snapshot.placement().placement().placement;
        if prefix.index.0 == 0
            || prefix.term.0 == 0
            || prefix.route.0 == 0
            || prefix.placement_epoch == 0
            || (!placement.content_copies.contains_key(&prefix.node)
                && !placement.materializers.contains_key(&prefix.node)
                && !placement.voters.contains_key(&prefix.node))
            || expected_active.is_some_and(|active| {
                active.ledger != prefix.ledger
                    || active.route_epoch > target.route_epoch
                    || active.policy_revision > target.policy_revision
            })
        {
            return Err(AccessError::Unauthorized);
        }
        let allocation = budget
            .reserve(BudgetKind::Payload, BudgetLane::Completion, RESIDENT_BYTES)
            .map_err(|_| AccessError::Capacity)?
            .commit();
        Ok(Self {
            snapshot,
            expected_active,
            target,
            checkpoint_installed: false,
            after: None,
            artifacts: 0,
            contents: 0,
            bytes: 0,
            digest: blake3::Hasher::new_derive_key("focal.custody.artifacts.v1"),
            transfer: None,
            next_chunk: 0,
            content_digest: blake3::Hasher::new(),
            _allocation: allocation,
        })
    }
    fn check(&self, owner: &CustodyStore) -> Result<u64, AccessError> {
        if owner.config().node != self.snapshot.prefix().node
            || owner
                .installed(self.target.ledger)
                .map(|policy| policy.scope())
                != self.expected_active
        {
            return Err(AccessError::Unavailable);
        }
        self.snapshot.elapsed_clock().map_err(snapshot_error)
    }
    /// One bounded disk slice: install the <=8MiB checkpoint, read one <=1MiB
    /// chunk, or project one artifact. No operation scans the complete session.
    pub(crate) fn advance(
        mut self: Box<Self>,
        owner: &mut CustodyStore,
        budget: &MemoryBudget,
    ) -> Result<CustodyVerificationProgress, AccessError> {
        let now = self.check(owner)?;
        if !self.checkpoint_installed {
            let _scratch = budget
                .reserve(
                    BudgetKind::Payload,
                    BudgetLane::Completion,
                    CHECKPOINT_SCRATCH,
                )
                .map_err(|_| AccessError::Capacity)?;
            let digest = owner
                .content_mut()
                .install_custody_record(CustodyRecordKind::Checkpoint, self.snapshot.checkpoint())
                .map_err(content_error)?;
            if digest != self.snapshot.prefix().checkpoint {
                return Err(AccessError::InvalidRequest);
            }
            self.checkpoint_installed = true;
            return Ok(CustodyVerificationProgress::Pending(self));
        }
        if let Some(transfer) = &self.transfer {
            if self.next_chunk < transfer.chunks() {
                let _scratch = budget
                    .reserve(
                        BudgetKind::Payload,
                        BudgetLane::Completion,
                        owner.content().max_chunk_bytes(),
                    )
                    .map_err(|_| AccessError::Capacity)?;
                let bytes = owner
                    .content()
                    .read_transfer_chunk(transfer, self.next_chunk)
                    .map_err(content_error)?;
                self.content_digest.update(&bytes);
                self.next_chunk = self
                    .next_chunk
                    .checked_add(1)
                    .ok_or(AccessError::Capacity)?;
                return Ok(CustodyVerificationProgress::Pending(self));
            }
            if ContentHash(*self.content_digest.finalize().as_bytes()) != transfer.stream_digest() {
                return Err(AccessError::InvalidRequest);
            }
            self.transfer = None;
        }
        let next = self
            .snapshot
            .artifact_after(self.after, now)
            .map_err(snapshot_error)?;
        if let Some(artifact) = next {
            if self
                .after
                .is_some_and(|previous| artifact.artifact.id <= previous)
                || self.artifacts >= self.snapshot.prefix().artifacts
            {
                return Err(AccessError::InvalidRequest);
            }
            let mut buffer = [0u8; 256];
            let encoded = postcard::to_slice(&artifact, &mut buffer)
                .map_err(|_| AccessError::InvalidRequest)?;
            self.digest.update(encoded);
            self.after = Some(artifact.artifact.id);
            self.artifacts = self.artifacts.checked_add(1).ok_or(AccessError::Capacity)?;
            if let Some(reference) = artifact.content {
                if reference.domain != focal_model::ContentDomainId(self.target.ledger.tenant.0) {
                    return Err(AccessError::Unauthorized);
                }
                self.contents = self.contents.checked_add(1).ok_or(AccessError::Capacity)?;
                self.bytes = self
                    .bytes
                    .checked_add(reference.length)
                    .ok_or(AccessError::Capacity)?;
                self.transfer = Some(
                    owner
                        .content()
                        .export_manifest(&reference)
                        .map_err(content_error)?,
                );
                self.next_chunk = 0;
                self.content_digest = blake3::Hasher::new();
            }
            return Ok(CustodyVerificationProgress::Pending(self));
        }
        if self.artifacts != self.snapshot.prefix().artifacts {
            return Err(AccessError::InvalidRequest);
        }
        self.check(owner)?;
        let manifest = CustodyPrefixManifest {
            schema: 1,
            prefix: self.snapshot.prefix().clone(),
            artifact_digest: ContentHash(*self.digest.finalize().as_bytes()),
            verified_artifacts: self.artifacts,
            verified_content_references: self.contents,
            verified_content_bytes: self.bytes,
        };
        let mut bytes = [0u8; 4096];
        let encoded =
            postcard::to_slice(&manifest, &mut bytes).map_err(|_| AccessError::Capacity)?;
        let digest = owner
            .content_mut()
            .install_custody_record(CustodyRecordKind::Manifest, encoded)
            .map_err(content_error)?;
        let finished = self.check(owner)?;
        if self
            .snapshot
            .artifact_after(self.after, finished)
            .map_err(snapshot_error)?
            .is_some()
        {
            return Err(AccessError::InvalidRequest);
        }
        let Self {
            snapshot,
            mut _allocation,
            ..
        } = *self;
        let placement = snapshot.into_placement();
        _allocation
            .shrink_to(4096)
            .map_err(|_| AccessError::Capacity)?;
        Ok(CustodyVerificationProgress::Complete(Box::new(
            VerifiedCustody {
                manifest,
                digest,
                placement,
                _allocation,
            },
        )))
    }
}
fn snapshot_error(error: focal_ledger::LedgerError) -> AccessError {
    match error {
        focal_ledger::LedgerError::Graph(focal_graph::GraphError::Memory(
            focal_memory::MemoryError::LeaseExpired,
        )) => AccessError::SnapshotExpired,
        focal_ledger::LedgerError::Capacity | focal_ledger::LedgerError::Memory(_) => {
            AccessError::Capacity
        }
        _ => AccessError::Unavailable,
    }
}

#[cfg(test)]
#[path = "custody_prefix_tests.rs"]
mod tests;
