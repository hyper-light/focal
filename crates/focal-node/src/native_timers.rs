//! Trusted timer delivery from the node's clock (doc 17 §11, 22 §7). The
//! committed prefix keeps one due-timer index row per undelivered claim,
//! monitor and evaluation deadline; each tick scans the rows due at the
//! node's logical time, bounded by [`MAX_DELIVERIES`], reads each timer's
//! identity from its primary row and delivers it through the session. A
//! delivery consumes its row, so a redelivered timer is an exact retry that
//! resolves to its recorded outcome, and a restart needs no memory: the next
//! sweep rescans the index.
use crate::native_ingress::logical_time;
use focal_core::Core;
use focal_core::native::{
    NativeClaimDeadlineInput, NativeDeadlineInput, NativeIndexHit, NativeIndexScan,
    NativeMonitorDeadlineInput, NativeState, TimerTarget,
};
use focal_ledger::{LedgerError, NativeTimerInput, Session};

/// The most timers one sweep delivers; the rest wait for the next tick.
pub(crate) const MAX_DELIVERIES: usize = 64;

/// What one sweep did.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct Sweep {
    /// Timers the session admitted (committed or pending).
    pub delivered: usize,
    /// Timers the owner refused as contract errors; their rows stay until
    /// the primary row changes, so they are reported rather than retried
    /// in a loop within one sweep.
    pub refused: usize,
    /// Timers left for a later sweep because the session could not admit
    /// more now (capacity, authority or readiness).
    pub deferred: usize,
}

/// The delivery input of one due row, read from its primary row at the
/// committed prefix; absent when the row's primary no longer names it.
fn input(core: &Core<NativeState>, target: TimerTarget) -> Option<NativeTimerInput> {
    Some(match target {
        TimerTarget::Claim(claim) => NativeTimerInput::Claim(NativeClaimDeadlineInput {
            claim,
            deadline: core.native_claim(claim)?.deadline()?,
        }),
        TimerTarget::Evaluation(evaluation) => {
            let state = core.native_evaluation(evaluation)?;
            let declaration = core.native_definition(evaluation.validation)?;
            NativeTimerInput::Evaluation(NativeDeadlineInput {
                evaluation,
                deadline: state.bind(declaration).ok()?.deadline(),
            })
        }
        TimerTarget::Monitor(claim, monitor) => {
            let scope = core.native_claim(claim)?.scopes().monitor(monitor)?;
            NativeTimerInput::Monitor(NativeMonitorDeadlineInput {
                claim,
                monitor,
                deadline: scope.deadline(),
            })
        }
    })
}

/// A refusal the next sweep may succeed at: the session is out of admission
/// capacity, not the leader, or not ready. Everything else is a contract
/// refusal of this exact timer.
fn deferrable(error: &LedgerError) -> bool {
    matches!(
        error,
        LedgerError::Capacity
            | LedgerError::NotReady { .. }
            | LedgerError::Memory(_)
            | LedgerError::Consensus(_)
            | LedgerError::Retry
            | LedgerError::Behind
    )
}

/// Deliver every timer due at the node's logical time, at most
/// [`MAX_DELIVERIES`]. A ledger that is not native, or one this node cannot
/// write, delivers nothing.
pub(crate) fn sweep(session: &mut Session) -> Result<Sweep, LedgerError> {
    let Ok(core) = session.native_core() else {
        return Ok(Sweep::default());
    };
    let now = logical_time(session).map_err(|_| LedgerError::NotReady { leader: 0 })?;
    let mut inputs = Vec::new();
    inputs
        .try_reserve_exact(MAX_DELIVERIES)
        .map_err(|_| LedgerError::Capacity)?;
    for hit in core
        .native_index_scan(NativeIndexScan::Due { through: now }, None)
        .take(MAX_DELIVERIES)
    {
        let NativeIndexHit::Timer { target, .. } = hit else {
            continue;
        };
        if let Some(input) = input(core, target) {
            inputs.push(input);
        }
    }
    let mut result = Sweep::default();
    for input in inputs {
        match session.deliver_native_timer(input, now) {
            Ok(_) => result.delivered = result.delivered.saturating_add(1),
            Err(error) if deferrable(&error) => {
                result.deferred = result.deferred.saturating_add(1);
                break;
            }
            Err(_) => result.refused = result.refused.saturating_add(1),
        }
    }
    Ok(result)
}
