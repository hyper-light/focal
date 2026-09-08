//! Scalar restoration of original aggregate provenance. The importing native
//! history must prove the actual accepted result or structural absence and the
//! canonical chosen cause. These constructors do not grant reporting authority.
use super::*;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BlockingCauseSnapshotV1 {
    pub key: CauseKey,
    pub slot: Option<u32>,
    pub artifact: Option<ArtifactRef>,
    pub kind: BlockingKind,
    pub mode: ValidationMode,
    pub slot_mode: ValidationMode,
    pub evidence: Option<ArtifactRef>,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TerminalCutSnapshotV1 {
    pub sequence: SessionSeq,
    pub cause: BlockingCauseSnapshotV1,
}
impl BlockingCause {
    pub fn snapshot_v1(self) -> BlockingCauseSnapshotV1 {
        BlockingCauseSnapshotV1 {
            key: self.key,
            slot: self.slot,
            artifact: self.artifact,
            kind: self.kind,
            mode: self.mode,
            slot_mode: self.slot_mode,
            evidence: self.evidence,
        }
    }
    /// Constant-space intrinsic restoration; original result/manifest membership
    /// and publication provenance remain the enclosing importer's obligation.
    pub fn hydrate_v1(value: BlockingCauseSnapshotV1) -> Result<Self, ContractError> {
        match value.key.phase {
            CausePhase::Delivery => return Err(ContractError::InvalidTransition),
            CausePhase::MissingTarget => {
                if !matches!(value.key.target, CauseTarget::Response(id) if !id.is_zero())
                    || value.slot.is_none()
                    || value.artifact.is_some()
                    || value.evidence.is_some()
                    || value.key.generation.is_some()
                    || value.key.attempt.is_some()
                    || value.kind != BlockingKind::Incomplete
                    || value.mode != ValidationMode::Required
                {
                    return Err(ContractError::InvalidTransition);
                }
            }
            CausePhase::Programmatic | CausePhase::Quality => {
                if value
                    .key
                    .generation
                    .is_none_or(|generation| generation == 0)
                    || value.key.attempt.is_none()
                    || value.evidence.is_none()
                {
                    return Err(ContractError::InvalidTransition);
                }
                match value.key.target {
                    CauseTarget::Admission if value.slot.is_none() && value.artifact.is_none() => {}
                    CauseTarget::Increment { artifact, content }
                        if !artifact.is_zero()
                            && value.slot.is_none()
                            && value.artifact
                                == Some(ArtifactRef {
                                    id: artifact,
                                    hash: content,
                                }) => {}
                    CauseTarget::Response(response)
                        if !response.is_zero()
                            && value.slot.is_some()
                            && value
                                .artifact
                                .is_some_and(|artifact| !artifact.id.is_zero()) => {}
                    _ => return Err(ContractError::InvalidTarget),
                }
            }
        }
        Ok(Self {
            key: value.key,
            slot: value.slot,
            artifact: value.artifact,
            kind: value.kind,
            mode: value.mode,
            slot_mode: value.slot_mode,
            evidence: value.evidence,
        })
    }
}
impl TerminalCut {
    pub fn snapshot_v1(self) -> TerminalCutSnapshotV1 {
        TerminalCutSnapshotV1 {
            sequence: self.sequence,
            cause: self.cause.snapshot_v1(),
        }
    }
    pub fn hydrate_v1(value: TerminalCutSnapshotV1) -> Result<Self, ContractError> {
        if value.sequence.0 == 0 {
            return Err(ContractError::InvalidCut);
        }
        let cause = BlockingCause::hydrate_v1(value.cause)?;
        if !cause.blocks_parent() {
            return Err(ContractError::InvalidPolicy);
        }
        Ok(Self {
            sequence: value.sequence,
            cause,
        })
    }
}
