use crate::CursorReceipt;
use focal_core::{ReconcileResultView, ReconciliationError};
use focal_model::*;
use focal_stream::{CursorMode, CursorRecord, DeltaFilter, PositionOffset, ResyncReason};

/// Borrowed committed-state observation over both families of the shared request
/// namespace. No result copy is made until the caller admits owned_bytes().
#[derive(Debug, Clone, Copy)]
pub struct ReconciliationView<'a> {
    domain: focal_core::ReconciliationView<'a>,
    cursor: Option<&'a CursorReceipt>,
}

impl ReconciliationView<'_> {
    /// Variable output items, inspected before any copy so ingress can enforce
    /// its item limit independently of the byte allowance.
    pub fn item_count(&self) -> usize {
        if let Some(cursor) = self.cursor {
            return match &cursor.record {
                Some(CursorRecord {
                    filter: DeltaFilter::Claims(claims),
                    ..
                }) => claims.len(),
                _ => 0,
            };
        }
        match self.domain.result {
            ReconcileResultView::Receipt {
                resolution: focal_core::ReceiptResolutionView::Committed(receipt),
                ..
            } => match &receipt.outcome {
                CommandResult::Generated(ids) | CommandResult::Existing(ids) => ids.len(),
                _ => 0,
            },
            _ => 0,
        }
    }

    pub fn owned_bytes(&self) -> Result<usize, ReconciliationError> {
        let mut bytes = self.domain.owned_bytes()?;
        if let Some(cursor) = self.cursor {
            bytes = bytes
                .checked_add(std::mem::size_of::<CursorMutationReceipt>())
                .ok_or(ReconciliationError::Capacity)?;
            if let Some(record) = &cursor.record
                && let DeltaFilter::Claims(claims) = &record.filter
            {
                bytes = claims
                    .len()
                    .checked_mul(std::mem::size_of::<ClaimId>())
                    .and_then(|heap| bytes.checked_add(heap))
                    .ok_or(ReconciliationError::Capacity)?;
            }
        }
        Ok(bytes)
    }

    /// Hold the admitted owned_bytes allowance until the result is consumed.
    /// This copies the original receipt, never the consumer's current state.
    pub fn to_owned(&self) -> Result<ReconcilePage, ReconciliationError> {
        let Some(cursor) = self.cursor else {
            return self.domain.to_owned();
        };
        let ReconcileResultView::Receipt { key, epoch, .. } = self.domain.result else {
            return Err(ReconciliationError::InvalidIdentity);
        };
        Ok(ReconcilePage {
            schema: RECONCILE_SCHEMA,
            ledger: self.domain.ledger,
            principal: self.domain.principal,
            sequence: self.domain.sequence,
            result: ReconcileResult::Receipt {
                key,
                epoch,
                resolution: ReceiptResolution::CommittedCursor(Box::new(copy_cursor(cursor)?)),
            },
        })
    }
}
impl<'a> ReconciliationView<'a> {
    pub(crate) fn new(
        domain: focal_core::ReconciliationView<'a>,
        cursor: Option<&'a CursorReceipt>,
    ) -> Self {
        Self { domain, cursor }
    }
}

fn copy_cursor(receipt: &CursorReceipt) -> Result<CursorMutationReceipt, ReconciliationError> {
    Ok(CursorMutationReceipt {
        ledger: receipt.ledger,
        key: receipt.key,
        intent_hash: receipt.intent_hash,
        revision: receipt.revision,
        domain_sequence: receipt.domain_sequence,
        raft_index: receipt.raft_index,
        floor: receipt.floor,
        record: receipt.record.as_ref().map(copy_record).transpose()?,
    })
}

pub(crate) fn copy_record(
    record: &CursorRecord,
) -> Result<CursorRecordSnapshot, ReconciliationError> {
    let filter = match &record.filter {
        DeltaFilter::All => CursorFilterSnapshot::All,
        DeltaFilter::Claims(claims) => {
            let mut ids = Vec::new();
            ids.try_reserve_exact(claims.len())
                .map_err(|_| ReconciliationError::Capacity)?;
            ids.extend(claims.iter().copied());
            CursorFilterSnapshot::Claims(ids)
        }
    };
    let mode = match record.mode {
        CursorMode::Live => CursorModeSnapshot::Live,
        CursorMode::Seeding { snapshot } => CursorModeSnapshot::Seeding { snapshot },
        CursorMode::Resync { reason } => CursorModeSnapshot::Resync {
            reason: match reason {
                ResyncReason::HistoryExpired => CursorResyncReason::HistoryExpired,
                ResyncReason::LeaseExpired => CursorResyncReason::LeaseExpired,
                ResyncReason::SlowConsumer => CursorResyncReason::SlowConsumer,
                ResyncReason::SnapshotExpired => CursorResyncReason::SnapshotExpired,
                ResyncReason::ExplicitReset => CursorResyncReason::ExplicitReset,
            },
        },
        CursorMode::Protected => CursorModeSnapshot::Protected,
    };
    Ok(CursorRecordSnapshot {
        token: CursorTokenSnapshot {
            key: CursorConsumerKeySnapshot {
                ledger: record.token.key.ledger,
                consumer: record.token.key.consumer.0,
            },
            generation: record.token.generation,
            scope: record.token.scope,
            position: CursorPositionSnapshot {
                ledger: record.token.position.ledger,
                sequence: record.token.position.sequence,
                offset: match record.token.position.offset {
                    PositionOffset::Delta(ordinal) => CursorPositionOffsetSnapshot::Delta(ordinal),
                    PositionOffset::Resolved => CursorPositionOffsetSnapshot::Resolved,
                },
            },
        },
        filter,
        expires_at: record.expires_at,
        mode,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use focal_stream::{ConsumerId, ConsumerKey, CursorToken, Position};
    use std::collections::BTreeSet;

    #[test]
    fn reconciliation_cursor_dto_matches_every_original_record_variant() {
        let ledger = LedgerId {
            tenant: TenantId::from_u128(1),
            session: SessionId::from_u128(2),
        };
        let mut receipt = CursorReceipt {
            ledger,
            key: RequestKey {
                principal: ParticipantId::from_u128(3),
                epoch: RequestEpoch(1),
                id: RequestId::from_u128(4),
            },
            intent_hash: ContentHash([5; 32]),
            revision: 6,
            domain_sequence: SessionSeq(7),
            raft_index: 8,
            floor: SessionSeq(2),
            record: None,
        };
        assert_eq!(
            postcard::to_stdvec(&receipt).unwrap(),
            postcard::to_stdvec(&copy_cursor(&receipt).unwrap()).unwrap()
        );
        let modes = [
            CursorMode::Live,
            CursorMode::Seeding {
                snapshot: SessionSeq(7),
            },
            CursorMode::Protected,
            CursorMode::Resync {
                reason: ResyncReason::HistoryExpired,
            },
            CursorMode::Resync {
                reason: ResyncReason::LeaseExpired,
            },
            CursorMode::Resync {
                reason: ResyncReason::SlowConsumer,
            },
            CursorMode::Resync {
                reason: ResyncReason::SnapshotExpired,
            },
            CursorMode::Resync {
                reason: ResyncReason::ExplicitReset,
            },
        ];
        for mode in modes {
            for offset in [PositionOffset::Delta(5), PositionOffset::Resolved] {
                for filter in [
                    DeltaFilter::All,
                    DeltaFilter::Claims(BTreeSet::from([
                        ClaimId::from_u128(11),
                        ClaimId::from_u128(10),
                    ])),
                ] {
                    receipt.record = Some(CursorRecord {
                        token: CursorToken {
                            key: ConsumerKey {
                                ledger,
                                consumer: ConsumerId::from_u128(12),
                            },
                            generation: 13,
                            scope: ContentHash([14; 32]),
                            position: Position {
                                ledger,
                                sequence: SessionSeq(7),
                                offset,
                            },
                        },
                        filter,
                        expires_at: 15,
                        mode: mode.clone(),
                    });
                    let copy = copy_cursor(&receipt).unwrap();
                    assert_eq!(
                        postcard::to_stdvec(&receipt).unwrap(),
                        postcard::to_stdvec(&copy).unwrap()
                    );
                    assert_eq!(
                        postcard::to_stdvec(&ReceiptResolution::CommittedCursor(Box::new(copy)))
                            .unwrap()[0],
                        3
                    );
                }
            }
        }
    }
}
