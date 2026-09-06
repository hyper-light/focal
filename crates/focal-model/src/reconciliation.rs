use crate::{
    ClaimId, ContentHash, LedgerId, MutationReceipt, ParticipantId, RequestEpoch, RequestId,
    RequestKey, SessionSeq,
};
use serde::{Deserialize, Serialize};

pub const RECONCILE_SCHEMA: u16 = 1;

/// The authenticated host supplies the principal. Queries cannot impersonate one.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub enum ReconcileQuery {
    Epoch {
        epoch: RequestEpoch,
    },
    Receipt {
        epoch: RequestEpoch,
        request: RequestId,
    },
}
impl ReconcileQuery {
    pub fn epoch(self) -> RequestEpoch {
        match self {
            Self::Epoch { epoch } | Self::Receipt { epoch, .. } => epoch,
        }
    }
}

/// Scalar observation of one principal's epoch window; never a copy of its set.
/// None/None means no epoch admission has committed at the observed prefix.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EpochReconciliation {
    pub epoch: RequestEpoch,
    pub minimum: Option<RequestEpoch>,
    pub latest_admitted: Option<RequestEpoch>,
    pub admitted: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub enum ReceiptResolution {
    /// The retained exact outcome wins even when its epoch is below the floor.
    Committed(Box<MutationReceipt>),
    /// New admission is fenced at this committed prefix. This does NOT assert
    /// that the request never committed: historical results may be retired.
    BelowFloor { minimum: RequestEpoch },
    /// No retained outcome is visible. An earlier proposal can still commit;
    /// neither non-admission nor permission to replace the intent is implied.
    Unknown,
    /// Original cursor metadata outcome from the same authenticated request-key
    /// namespace. Its metadata revision does not advance the domain sequence.
    CommittedCursor(Box<CursorMutationReceipt>),
}

/// Read vocabulary for the original cursor outcome. Field and variant order
/// matches its persisted ledger representation. Keeping these owned read DTOs
/// here avoids coupling the domain model to the stream execution crate.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CursorMutationReceipt {
    pub ledger: LedgerId,
    pub key: RequestKey,
    pub intent_hash: ContentHash,
    pub revision: u64,
    pub domain_sequence: SessionSeq,
    pub raft_index: u64,
    pub floor: SessionSeq,
    pub record: Option<CursorRecordSnapshot>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CursorRecordSnapshot {
    pub token: CursorTokenSnapshot,
    pub filter: CursorFilterSnapshot,
    pub expires_at: u64,
    pub mode: CursorModeSnapshot,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CursorTokenSnapshot {
    pub key: CursorConsumerKeySnapshot,
    pub generation: u64,
    pub scope: ContentHash,
    pub position: CursorPositionSnapshot,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CursorConsumerKeySnapshot {
    pub ledger: LedgerId,
    pub consumer: [u8; 16],
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CursorPositionSnapshot {
    pub ledger: LedgerId,
    pub sequence: SessionSeq,
    pub offset: CursorPositionOffsetSnapshot,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub enum CursorPositionOffsetSnapshot {
    Delta(u32),
    Resolved,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub enum CursorFilterSnapshot {
    All,
    /// Canonically sorted, unique IDs. A vector permits fallible copy admission
    /// without building an extra tree for an immutable response.
    Claims(Vec<ClaimId>),
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub enum CursorModeSnapshot {
    Live,
    Seeding { snapshot: SessionSeq },
    Resync { reason: CursorResyncReason },
    Protected,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub enum CursorResyncReason {
    HistoryExpired,
    LeaseExpired,
    SlowConsumer,
    SnapshotExpired,
    ExplicitReset,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub enum ReconcileResult {
    Epoch(EpochReconciliation),
    Receipt {
        key: RequestKey,
        epoch: EpochReconciliation,
        resolution: ReceiptResolution,
    },
}

/// Read-only committed-state observation. The transport separately binds a fresh
/// quorum barrier and route to this actual applied domain prefix. This value by
/// itself is not proof that a caller obtained a quorum read.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReconcilePage {
    pub schema: u16,
    pub ledger: LedgerId,
    pub principal: ParticipantId,
    pub sequence: SessionSeq,
    pub result: ReconcileResult,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reconciliation_query_tags_are_explicitly_frozen_and_fields_are_strict() {
        assert_eq!(
            postcard::to_allocvec(&ReconcileQuery::Epoch {
                epoch: RequestEpoch(3)
            })
            .unwrap(),
            [0, 3]
        );
        let query = ReconcileQuery::Receipt {
            epoch: RequestEpoch(3),
            request: RequestId::from_u128(9),
        };
        let mut expected = vec![1, 3];
        expected.extend_from_slice(&9_u128.to_be_bytes());
        assert_eq!(postcard::to_allocvec(&query).unwrap(), expected);
        assert_eq!(
            postcard::to_allocvec(&ReceiptResolution::BelowFloor {
                minimum: RequestEpoch(2)
            })
            .unwrap(),
            [1, 2]
        );
        assert_eq!(
            postcard::to_allocvec(&ReceiptResolution::Unknown).unwrap(),
            [2]
        );
        for value in [
            r#"{"Epoch":{"epoch":1,"principal":"00000000000000000000000000000001"}}"#,
            r#"{"Receipt":{"epoch":1,"request":"00000000000000000000000000000001","extra":true}}"#,
        ] {
            assert!(serde_json::from_str::<ReconcileQuery>(value).is_err());
        }
        let json = serde_json::to_vec(&query).unwrap();
        assert_eq!(
            serde_json::from_slice::<ReconcileQuery>(&json).unwrap(),
            query
        );
    }
}
