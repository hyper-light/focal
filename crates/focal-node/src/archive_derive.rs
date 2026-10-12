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
/// (F12): under pressure, or when the closed outcomes fill half a bundle at
/// the read bound — under the anchored rule, or half the window if that is
/// less; a fold of the oldest half of the seal rows when the next seal
/// would reach the bound. The bundle is written at the bound the read
/// serves, the rows halved until it fits. Derived rule: the plan is taken
/// under the pressure's floors, and it is proposable only at this prefix.
/// Anchored rule: the plan is what is closed now, no floors (the owner
/// forces those itself, and a later seal takes what they close), and it
/// stays proposable while the pipeline commits around its custody.
pub(crate) fn sealed_outcomes(
    session: &Session,
    budget: &MemoryBudget,
) -> Result<SealOffer, LedgerError> {
    if !session.native_authoritative() {
        return Ok(SealOffer::Nothing);
    }
    if session.native_check_seal().is_err() {
        return Ok(if session.native_seal_busy()? {
            SealOffer::Busy
        } else {
            SealOffer::Nothing
        });
    }
    let anchored = session.native_seal_rule()? == focal_ledger::SealRule::Anchored;
    let mut limits = session.native_encoding_limits()?;
    limits.bytes = limits
        .bytes
        .min(focal_core::native::seal::SEAL_BUNDLE_BYTES);
    let core = session.native_core()?;
    let floors = if anchored {
        Vec::new()
    } else {
        core.pressure_floors(focal_ledger::native_session::seal::MAX_FLOORS)
            .map_err(|error| LedgerError::Native(error.into()))?
    };
    let pressed = if anchored {
        core.close_pressure(0)
    } else {
        !floors.is_empty()
    };
    let window = core.native_limits().outcomes;
    let mut bound = SealBound {
        principals: core
            .native_limits()
            .principals
            .min(focal_ledger::native_session::seal::MAX_PRINCIPALS),
        rows: focal_core::native::seal::DEFAULT_SEAL_ROWS_PER_BUNDLE,
    };
    let (plan, quote) = loop {
        let plan = match core.seal_plan(&floors, bound) {
            Ok(plan) => plan,
            Err(SealRefusal::Nothing) => return Ok(SealOffer::Nothing),
            Err(SealRefusal::Capacity) => return Err(LedgerError::Capacity),
            Err(_) => return Err(LedgerError::Corrupt),
        };
        // Short of half a bundle and pressed by nothing, nothing is
        // proposed, and that is known from the plan: its bundle is not
        // encoded to be dropped. The quote of a plan short of it was
        // encoded at every round of the archive agent and dropped, on the
        // owner's thread, for as long as the outcomes awaiting a seal were
        // fewer than half a bundle (a stall every five seconds that grew
        // with them, 70 to 153 ms at 200 writes a second). The same rule
        // as when the quote came first: a plan short of half a bundle
        // fits the bundle, so its quote never halved the bound. Under the
        // anchored rule half the window, if less, is enough: a window
        // smaller than a bundle seals before it fills.
        let enough = if anchored {
            (bound.rows / 2).min(window / 2).max(1)
        } else {
            (bound.rows / 2).max(1)
        };
        if !pressed && plan.rows() < enough {
            return Ok(SealOffer::Nothing);
        }
        match core.seal_quote(&plan, limits) {
            Ok(quote) => break (plan, quote),
            Err(_) if bound.rows > 1 => bound.rows /= 2,
            Err(error) => return Err(LedgerError::Native(error.into())),
        }
    };
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
    Ok(SealOffer::Ready(Box::new(SealedOutcomes {
        plan,
        bundle,
        digest,
        fold,
        _allocation: allocation,
    })))
}
/// What the archive agent finds when it asks a replica for a seal (F12).
pub enum SealOffer {
    /// A seal is held for the pipeline to drain or in flight: the agent
    /// asks again soon, not at its next tick.
    Busy,
    /// Nothing worth proposing now.
    Nothing,
    /// A seal and its bundle, to put under custody and propose.
    Ready(Box<SealedOutcomes>),
}
