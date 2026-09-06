use crate::*;
use focal_model::*;
use serde::{Deserialize, Serialize};

pub const MAX_MONITOR_ROOTS: usize = 256;

/// Current committed monitor at a fresh quorum prefix. `released` is the stored
/// fact; this read does not infer why release occurred or create a timer event.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MonitorPage {
    pub token: ReadToken,
    pub applied_index: u64,
    pub id: MonitorId,
    pub monitor: Option<Monitor>,
}

pub(crate) fn validate(
    request: &RequestEnvelope,
    response: &ResponseEnvelope,
    page: &MonitorPage,
    limits: &WireLimits,
) -> Result<(), WireError> {
    if !matches!(request.operation, Operation::Monitor { id } if id == page.id)
        || page.id.is_zero()
        || page.token.ledger != request.ledger
        || page.token.route_epoch != request.route_epoch
        || page.token.route_epoch != response.route_epoch
        || page.token.route_epoch.0 == 0
        || page.applied_index == 0
    {
        return Err(WireError::InvalidFrame);
    }
    if let Some(monitor) = &page.monitor
        && (monitor.id != page.id
            || monitor.owner.is_zero()
            || monitor.roots.is_empty()
            || monitor.roots.len() > MAX_MONITOR_ROOTS.min(limits.max_items as usize)
            || monitor.registered.0 == 0
            || monitor.registered > page.token.sequence
            || monitor.released.is_some_and(|released| {
                released < monitor.registered || released > page.token.sequence
            })
            || monitor.deadline.timer.is_zero()
            || monitor.roots.iter().any(|root| match root {
                WaitPredicate::Satisfied(id)
                | WaitPredicate::Terminal(id)
                | WaitPredicate::Released(id) => id.is_zero(),
            }))
    {
        return Err(WireError::InvalidFrame);
    }
    Ok(())
}

#[cfg(test)]
#[path = "monitor_tests.rs"]
mod tests;
