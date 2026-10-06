//! What the archive agent derives from a session's committed state, for
//! the fleet's replica owner and the embedded node alike (26 §4, and the
//! audit's F12): a released family's bundle, and the seal the closed
//! outcomes yield. Both are derived on the authority only, from the
//! committed core, charged to the caller's budget, and proposed by the
//! caller once the bundle is under custody.
use crate::fleet::{ArchivedFamily, SealedOutcomes};
use focal_core::native::seal::{SealBound, SealRefusal};
use focal_ledger::{LedgerError, Session};
use focal_memory::{BudgetKind, BudgetLane, MemoryBudget};
use focal_model::ClaimId;

/// One family's bundle from the committed core (26 §4): only on the
/// authority, only when the family is eligible, every registered consumer
/// has read past its last event, and that event settled at least
/// `min_age_ms` of the node's logical time ago (a finished claim stays
/// readable in the core for the grace the operator sets); the bytes are
/// charged to `budget`.
pub(crate) fn archived_family(
    session: &Session,
    budget: &MemoryBudget,
    root: ClaimId,
    min_age_ms: u64,
) -> Result<Option<ArchivedFamily>, LedgerError> {
    if !session.native_authoritative() {
        return Ok(None);
    }
    // What the proposal will ask short of the family: nothing in flight,
    // and the owner's outcome to spare beyond those it promised. A
    // bundle is not sealed for a family that cannot be proposed.
    if session.native_check_retirement().is_err() {
        return Ok(None);
    }
    let now = crate::native_ingress::logical_time(session)
        .map_err(|_| LedgerError::NotReady { leader: 0 })?;
    let report = session.native_retention()?;
    let limits = session.native_encoding_limits()?;
    let core = session.native_core()?;
    let Ok(family) = core.retirement_family(root) else {
        return Ok(None);
    };
    if !report.allows_family(family.through) {
        return Ok(None);
    }
    // The family's last event was published by the outcome at its
    // sequence; the outcome carries the logical time it settled at.
    let settled = core
        .native_event(family.through, 0)
        .and_then(|event| core.native_outcome(event.invocation))
        .map(|outcome| outcome.logical_time)
        .ok_or(LedgerError::Corrupt)?;
    if now.saturating_sub(settled) < min_age_ms {
        return Ok(None);
    }
    let through = core.native_sequence();
    let quote = core
        .archive_family_quote(&family, through, limits)
        .map_err(|error| LedgerError::Native(error.into()))?;
    let allocation = budget
        .reserve(BudgetKind::Payload, BudgetLane::Ordinary, quote.bytes)?
        .commit();
    let mut bundle = Vec::new();
    bundle
        .try_reserve_exact(quote.bytes)
        .map_err(|_| LedgerError::Capacity)?;
    bundle.resize(quote.bytes, 0);
    let digest = core
        .archive_family_into(&family, through, &mut bundle, quote.visits)
        .map_err(|error| LedgerError::Native(error.into()))?;
    let rows = family.rows();
    Ok(Some(ArchivedFamily {
        root,
        members: family.members,
        through,
        digest,
        rows,
        bundle,
        _allocation: allocation,
    }))
}
/// The seal the committed state yields now, when it is worth proposing
/// (F12): under pressure (the floors it forces are not empty), or when the
/// closed outcomes fill half a bundle at the read bound; a fold of the
/// oldest half of the seal rows when the next seal would reach the bound.
/// The bundle is written at the bound the read serves, the rows halved
/// until it fits.
pub(crate) fn sealed_outcomes(
    session: &Session,
    budget: &MemoryBudget,
) -> Result<Option<SealedOutcomes>, LedgerError> {
    if !session.native_authoritative() {
        return Ok(None);
    }
    if session.native_check_seal().is_err() {
        return Ok(None);
    }
    let mut limits = session.native_encoding_limits()?;
    limits.bytes = limits
        .bytes
        .min(focal_core::native::seal::SEAL_BUNDLE_BYTES);
    let core = session.native_core()?;
    let floors = core
        .pressure_floors(focal_ledger::native_session::seal::MAX_FLOORS)
        .map_err(|error| LedgerError::Native(error.into()))?;
    let mut bound = SealBound {
        principals: core.native_limits().principals,
        rows: focal_core::native::seal::DEFAULT_SEAL_ROWS_PER_BUNDLE,
    };
    let (plan, quote) = loop {
        let plan = match core.seal_plan(&floors, bound) {
            Ok(plan) => plan,
            Err(SealRefusal::Nothing) => return Ok(None),
            Err(SealRefusal::Capacity) => return Err(LedgerError::Capacity),
            Err(_) => return Err(LedgerError::Corrupt),
        };
        match core.seal_quote(&plan, limits) {
            Ok(quote) => break (plan, quote),
            Err(_) if bound.rows > 1 => bound.rows /= 2,
            Err(error) => return Err(LedgerError::Native(error.into())),
        }
    };
    let full = bound.rows / 2;
    if floors.is_empty() && plan.rows() < full.max(1) {
        return Ok(None);
    }
    // A fold when the seal rows would reach the bound: the oldest half
    // into the row keyed by the last of them.
    let seal_rows = core
        .native_seal_rows()
        .map_err(|error| LedgerError::Native(error.into()))?;
    let fold = if seal_rows.len().saturating_add(1) >= core.native_limits().seals {
        let half = seal_rows.len() / 2;
        match (seal_rows.first(), seal_rows.get(half)) {
            (Some((_, first_row)), Some((last, _))) if half >= 1 => {
                let plan = core
                    .fold_plan(first_row.first, *last)
                    .map_err(|_| LedgerError::Corrupt)?;
                let quote = core
                    .fold_quote(&plan, limits)
                    .map_err(|error| LedgerError::Native(error.into()))?;
                Some((plan, quote))
            }
            _ => None,
        }
    } else {
        None
    };
    let total = quote
        .bytes
        .checked_add(fold.as_ref().map_or(0, |(_, quote)| quote.bytes))
        .ok_or(LedgerError::Capacity)?;
    let allocation = budget
        .reserve(BudgetKind::Payload, BudgetLane::Ordinary, total)?
        .commit();
    let mut bundle = Vec::new();
    bundle
        .try_reserve_exact(quote.bytes)
        .map_err(|_| LedgerError::Capacity)?;
    bundle.resize(quote.bytes, 0);
    let digest = core
        .seal_into(&plan, &mut bundle, quote.visits)
        .map_err(|error| LedgerError::Native(error.into()))?;
    let fold = match fold {
        Some((plan, quote)) => {
            let mut bytes = Vec::new();
            bytes
                .try_reserve_exact(quote.bytes)
                .map_err(|_| LedgerError::Capacity)?;
            bytes.resize(quote.bytes, 0);
            let digest = core
                .fold_into(&plan, &mut bytes, quote.visits)
                .map_err(|error| LedgerError::Native(error.into()))?;
            Some((plan, bytes, digest))
        }
        None => None,
    };
    Ok(Some(SealedOutcomes {
        plan,
        bundle,
        digest,
        fold,
        _allocation: allocation,
    }))
}
