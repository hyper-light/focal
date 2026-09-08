//! Versioned scalar graph-cut provenance. Hydration never reruns SCC selection
//! or dependency propagation. The native importer must authenticate the exact
//! original source graph, witness fingerprint and canonical chosen origin.
use super::*;
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OriginSnapshotV1 {
    pub binding: Binding,
    pub created: SessionSeq,
    pub terminal: SessionSeq,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TerminalCutSnapshotV1 {
    pub sequence: SessionSeq,
    pub kind: FailureKind,
    pub origin: OriginSnapshotV1,
    pub fingerprint: ContentHash,
    pub deadline: Option<Deadline>,
    pub fired_at: Option<u64>,
}
impl Origin {
    pub fn snapshot_v1(self) -> OriginSnapshotV1 {
        OriginSnapshotV1 {
            binding: self.binding,
            created: self.created,
            terminal: self.terminal,
        }
    }
}
impl TerminalCut {
    pub fn snapshot_v1(self) -> TerminalCutSnapshotV1 {
        TerminalCutSnapshotV1 {
            sequence: self.sequence,
            kind: self.kind,
            origin: self.origin.snapshot_v1(),
            fingerprint: self.fingerprint,
            deadline: self.deadline,
            fired_at: self.fired_at,
        }
    }
    pub fn hydrate_v1(value: TerminalCutSnapshotV1) -> Result<Self, ContractError> {
        let origin = value.origin;
        if origin.binding.object.is_zero()
            || origin.binding.ledger.tenant.is_zero()
            || origin.binding.ledger.session.is_zero()
        {
            return Err(ContractError::InvalidTarget);
        }
        if origin.created.0 == 0
            || origin.terminal < origin.created
            || value.sequence < origin.terminal
            || value.fingerprint == ContentHash([0; 32])
        {
            return Err(ContractError::InvalidCut);
        }
        match (value.kind, value.deadline, value.fired_at) {
            (FailureKind::DependencyFailed, None, None) => {}
            (FailureKind::Deadlocked, Some(deadline), Some(fired))
                if !deadline.timer.is_zero()
                    && deadline.generation != 0
                    && fired >= deadline.at
                    && origin.terminal == value.sequence => {}
            _ => return Err(ContractError::InvalidCut),
        }
        Ok(Self {
            sequence: value.sequence,
            kind: value.kind,
            origin: Origin {
                binding: origin.binding,
                created: origin.created,
                terminal: origin.terminal,
            },
            fingerprint: value.fingerprint,
            deadline: value.deadline,
            fired_at: value.fired_at,
        })
    }
}
