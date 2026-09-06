use crate::Core;
use focal_model::*;

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum ReconciliationError {
    #[error("reconciliation ledger does not match the committed owner")]
    LedgerMismatch,
    #[error("reconciliation requires nonzero authenticated identity and request ID")]
    InvalidIdentity,
    #[error("reconciliation requires a positive request epoch")]
    InvalidEpoch,
    #[error("reconciliation output exceeds capacity")]
    Capacity,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReceiptResolutionView<'a> {
    Committed(&'a MutationReceipt),
    BelowFloor { minimum: RequestEpoch },
    Unknown,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReconcileResultView<'a> {
    Epoch(EpochReconciliation),
    Receipt {
        key: RequestKey,
        epoch: EpochReconciliation,
        resolution: ReceiptResolutionView<'a>,
    },
}
/// Borrows the committed receipt until the owner has admitted its output copy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReconciliationView<'a> {
    pub ledger: LedgerId,
    pub principal: ParticipantId,
    pub sequence: SessionSeq,
    pub result: ReconcileResultView<'a>,
}
impl ReconciliationView<'_> {
    /// Conservative owned result residency, including the optional receipt box
    /// and the only variable-size CommandResult variants. Reserve before copying.
    pub fn owned_bytes(&self) -> Result<usize, ReconciliationError> {
        let mut size = std::mem::size_of::<ReconcilePage>();
        if let ReconcileResultView::Receipt {
            resolution: ReceiptResolutionView::Committed(receipt),
            ..
        } = self.result
        {
            size = size
                .checked_add(std::mem::size_of::<MutationReceipt>())
                .ok_or(ReconciliationError::Capacity)?;
            size = size
                .checked_add(outcome_heap_bytes(&receipt.outcome)?)
                .ok_or(ReconciliationError::Capacity)?;
        }
        size.checked_add(512).ok_or(ReconciliationError::Capacity)
    }
    /// The caller must hold the allowance returned by owned_bytes through reply
    /// encoding/consumption. The large receipt-vector allocation is fallible.
    pub fn to_owned(&self) -> Result<ReconcilePage, ReconciliationError> {
        let result = match self.result {
            ReconcileResultView::Epoch(epoch) => ReconcileResult::Epoch(epoch),
            ReconcileResultView::Receipt {
                key,
                epoch,
                resolution,
            } => ReconcileResult::Receipt {
                key,
                epoch,
                resolution: match resolution {
                    ReceiptResolutionView::Committed(receipt) => {
                        ReceiptResolution::Committed(Box::new(copy_receipt(receipt)?))
                    }
                    ReceiptResolutionView::BelowFloor { minimum } => {
                        ReceiptResolution::BelowFloor { minimum }
                    }
                    ReceiptResolutionView::Unknown => ReceiptResolution::Unknown,
                },
            },
        };
        Ok(ReconcilePage {
            schema: RECONCILE_SCHEMA,
            ledger: self.ledger,
            principal: self.principal,
            sequence: self.sequence,
            result,
        })
    }
}
impl Core {
    /// Local committed-state lookup only. A host requiring a linearizable answer
    /// obtains a fresh quorum barrier before invoking this method. Pending row
    /// overlays are deliberately excluded, even on a leader.
    pub fn reconcile(
        &self,
        ledger: LedgerId,
        principal: ParticipantId,
        query: &ReconcileQuery,
    ) -> Result<ReconciliationView<'_>, ReconciliationError> {
        if ledger != self.state.ledger {
            return Err(ReconciliationError::LedgerMismatch);
        }
        if principal.is_zero() || ledger.tenant.is_zero() || ledger.session.is_zero() {
            return Err(ReconciliationError::InvalidIdentity);
        }
        let epoch = query.epoch();
        if epoch.0 == 0 {
            return Err(ReconciliationError::InvalidEpoch);
        }
        let window = self.state.epochs.get(&principal);
        let observation = EpochReconciliation {
            epoch,
            minimum: window.map(|w| w.minimum),
            latest_admitted: window.and_then(|w| w.admitted.last().copied()),
            admitted: window.is_some_and(|w| w.admitted.contains(&epoch)),
        };
        let result = match *query {
            ReconcileQuery::Epoch { .. } => ReconcileResultView::Epoch(observation),
            ReconcileQuery::Receipt { request, .. } => {
                if request.is_zero() {
                    return Err(ReconciliationError::InvalidIdentity);
                }
                let key = RequestKey {
                    principal,
                    epoch,
                    id: request,
                };
                let resolution = if let Some(receipt) = self.state.receipts.get(&key) {
                    ReceiptResolutionView::Committed(receipt)
                } else if let Some(minimum) = observation.minimum.filter(|minimum| epoch < *minimum)
                {
                    ReceiptResolutionView::BelowFloor { minimum }
                } else {
                    ReceiptResolutionView::Unknown
                };
                ReconcileResultView::Receipt {
                    key,
                    epoch: observation,
                    resolution,
                }
            }
        };
        Ok(ReconciliationView {
            ledger,
            principal,
            sequence: self.state.sequence,
            result,
        })
    }
}
fn copy_receipt(receipt: &MutationReceipt) -> Result<MutationReceipt, ReconciliationError> {
    fn copy_ids(ids: &[ClaimId]) -> Result<Vec<ClaimId>, ReconciliationError> {
        let mut out = Vec::new();
        out.try_reserve_exact(ids.len())
            .map_err(|_| ReconciliationError::Capacity)?;
        out.extend_from_slice(ids);
        Ok(out)
    }
    let outcome = match &receipt.outcome {
        CommandResult::Generated(ids) => CommandResult::Generated(copy_ids(ids)?),
        CommandResult::Existing(ids) => CommandResult::Existing(copy_ids(ids)?),
        other @ (CommandResult::EpochAdmitted(_)
        | CommandResult::EpochFloorAdvanced(_)
        | CommandResult::Claim { .. }
        | CommandResult::Receipt { .. }
        | CommandResult::EvidenceSet(_)
        | CommandResult::Artifact(_)
        | CommandResult::Testament(_)
        | CommandResult::Validation(_)
        | CommandResult::Monitor(_)
        | CommandResult::Noop) => other.clone(),
    };
    Ok(MutationReceipt {
        ledger: receipt.ledger,
        key: receipt.key,
        sequence: receipt.sequence,
        command_hash: receipt.command_hash,
        outcome,
    })
}

fn outcome_heap_bytes(outcome: &CommandResult) -> Result<usize, ReconciliationError> {
    match outcome {
        CommandResult::Generated(ids) | CommandResult::Existing(ids) => ids
            .len()
            .checked_mul(std::mem::size_of::<ClaimId>())
            .ok_or(ReconciliationError::Capacity),
        CommandResult::EpochAdmitted(_)
        | CommandResult::EpochFloorAdvanced(_)
        | CommandResult::Claim { .. }
        | CommandResult::Receipt { .. }
        | CommandResult::EvidenceSet(_)
        | CommandResult::Artifact(_)
        | CommandResult::Testament(_)
        | CommandResult::Validation(_)
        | CommandResult::Monitor(_)
        | CommandResult::Noop => Ok(0),
    }
}

#[cfg(test)]
#[path = "reconciliation_tests.rs"]
mod tests;
