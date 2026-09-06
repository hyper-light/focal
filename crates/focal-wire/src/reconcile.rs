use crate::{Operation, ReadToken, RequestEnvelope, WireError, WireLimits};
use focal_model::*;
use serde::{Deserialize, Serialize};

/// The token and page describe the same committed prefix. The authenticated
/// owner supplies principal; no reconciliation query can select another actor.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReconcileReply {
    pub token: ReadToken,
    /// The actual applied Raft prefix also orders cursor metadata, which does
    /// not consume a domain SessionSeq.
    pub applied_index: u64,
    pub page: ReconcilePage,
}

pub(crate) fn validate_reconciliation(
    request: &RequestEnvelope,
    route: RouteEpoch,
    reply: &ReconcileReply,
    principal: Option<ParticipantId>,
    limits: &WireLimits,
) -> Result<(), WireError> {
    let Operation::Reconcile(query) = &request.operation else {
        return Err(WireError::InvalidFrame);
    };
    let page = &reply.page;
    if page.schema != 1
        || page.ledger != request.ledger
        || reply.token.ledger != page.ledger
        || reply.token.sequence != page.sequence
        || reply.token.route_epoch != route
        || route != request.route_epoch
        || route.0 == 0
        || reply.applied_index == 0
        || page.principal.is_zero()
        || principal.is_some_and(|expected| expected != page.principal)
    {
        return Err(WireError::InvalidFrame);
    }
    match (query, &page.result) {
        (ReconcileQuery::Epoch { epoch }, ReconcileResult::Epoch(status)) => {
            validate_epoch(*epoch, status, page.sequence)?;
        }
        (
            ReconcileQuery::Receipt { epoch, request },
            ReconcileResult::Receipt {
                key,
                epoch: status,
                resolution,
            },
        ) => {
            validate_epoch(*epoch, status, page.sequence)?;
            if key.principal != page.principal
                || key.epoch != *epoch
                || key.id != *request
                || request.is_zero()
            {
                return Err(WireError::InvalidFrame);
            }
            match resolution {
                ReceiptResolution::Committed(receipt) => {
                    if receipt.ledger != page.ledger
                        || receipt.key != *key
                        || receipt.sequence.0 == 0
                        || receipt.sequence > page.sequence
                        || status
                            .latest_admitted
                            .is_none_or(|latest| receipt.key.epoch > latest)
                    {
                        return Err(WireError::InvalidFrame);
                    }
                    if let CommandResult::Generated(ids) | CommandResult::Existing(ids) =
                        &receipt.outcome
                        && ids.len() > limits.max_items as usize
                    {
                        return Err(WireError::Limit);
                    }
                }
                ReceiptResolution::CommittedCursor(receipt) => {
                    if receipt.ledger != page.ledger
                        || receipt.key != *key
                        || receipt.revision == 0
                        || receipt.raft_index == 0
                        || receipt.raft_index > reply.applied_index
                        || receipt.floor > receipt.domain_sequence
                        || receipt.domain_sequence > page.sequence
                        || status
                            .latest_admitted
                            .is_none_or(|latest| receipt.key.epoch > latest)
                    {
                        return Err(WireError::InvalidFrame);
                    }
                    if let Some(record) = &receipt.record {
                        validate_cursor_record(record, receipt, limits)?;
                    }
                }
                ReceiptResolution::BelowFloor { minimum }
                    if status.minimum == Some(*minimum) && epoch < minimum => {}
                ReceiptResolution::Unknown
                    if status.minimum.is_none_or(|minimum| *epoch >= minimum) => {}
                _ => return Err(WireError::InvalidFrame),
            }
        }
        _ => return Err(WireError::InvalidFrame),
    }
    Ok(())
}

fn validate_cursor_record(
    record: &CursorRecordSnapshot,
    receipt: &CursorMutationReceipt,
    limits: &WireLimits,
) -> Result<(), WireError> {
    validate_cursor_record_at(record, receipt.ledger, receipt.domain_sequence, limits)
}
pub(crate) fn validate_cursor_record_at(
    record: &CursorRecordSnapshot,
    ledger: LedgerId,
    sequence: SessionSeq,
    limits: &WireLimits,
) -> Result<(), WireError> {
    let token = &record.token;
    if token.key.ledger != ledger
        || token.position.ledger != ledger
        || token.generation == 0
        || token.position.sequence > sequence
        || (token.position.sequence.0 == 0
            && token.position.offset != CursorPositionOffsetSnapshot::Resolved)
        || record.expires_at == 0
        || matches!(record.mode, CursorModeSnapshot::Protected) && record.expires_at != u64::MAX
    {
        return Err(WireError::InvalidFrame);
    }
    if let CursorFilterSnapshot::Claims(claims) = &record.filter {
        if claims.len() > limits.max_items as usize {
            return Err(WireError::Limit);
        }
        let mut previous = None;
        for claim in claims {
            if previous.is_some_and(|previous| previous >= *claim) {
                return Err(WireError::InvalidFrame);
            }
            previous = Some(*claim);
        }
    }
    if let CursorModeSnapshot::Seeding { snapshot } = record.mode
        && (snapshot > sequence
            || token.position.sequence != snapshot
            || token.position.offset != CursorPositionOffsetSnapshot::Resolved)
    {
        return Err(WireError::InvalidFrame);
    }
    Ok(())
}

fn validate_epoch(
    requested: RequestEpoch,
    status: &EpochReconciliation,
    sequence: SessionSeq,
) -> Result<(), WireError> {
    // The committed request-epoch protocol admits consecutive epochs and
    // retires a prefix. Its retained admission window cannot contain holes.
    let admitted = matches!((status.minimum, status.latest_admitted), (Some(minimum), Some(latest)) if minimum <= requested && requested <= latest);
    if requested.0 == 0
        || status.epoch != requested
        || status.minimum.is_some_and(|epoch| epoch.0 == 0)
        || status.latest_admitted.is_some_and(|epoch| epoch.0 == 0)
        || status.minimum.is_none() != status.latest_admitted.is_none()
        || (sequence.0 == 0 && status.minimum.is_some())
        || matches!((status.minimum, status.latest_admitted), (Some(minimum), Some(latest)) if minimum > latest)
        || status.admitted != admitted
    {
        return Err(WireError::InvalidFrame);
    }
    Ok(())
}
