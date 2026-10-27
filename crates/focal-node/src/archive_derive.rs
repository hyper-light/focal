//! What the archive agent derives from a session's committed state, for
//! the fleet's replica owner and the embedded node alike (26 §4, and the
//! audit's F12): a released family's bundle, and the seal the closed
//! outcomes yield. Both are derived on the authority only, from the
//! committed core, charged to the caller's budget, and proposed by the
//! caller once the bundle is under custody.
use crate::fleet::{ArchivedFamily, SealedOutcomes};
use focal_core::native::seal::{SealBound, SealRefusal};
use focal_ledger::{LedgerError, Session};
use focal_memory::{Allocation, BudgetKind, BudgetLane, MemoryBudget};
use focal_model::{ClaimId, SessionSeq};
use std::collections::VecDeque;

/// The samples a session's [`Settled`] keeps at most.
pub(crate) const SETTLE_SAMPLES: usize = 1024;

/// When a session's committed records settled (26 §4): its committed sequence and the logical time
/// its core held there, sampled as the archive agent's walks come, oldest first. A record whose
/// logical time is below its core's is refused, so time never falls along the sequence, and the
/// first sample at or past a family's last event bounds from above when that event settled: a
/// family dated by it never retires before its grace, whether the outcome that settled it is
/// still in the core or a seal took it out (an anchored seal takes outcomes within seconds of
/// their close; a family was dated by its outcome alone, every walk past one sealed failed, and
/// none retired, a deliver run filling its session's 1 GiB). A sample is taken at most every
/// `grace / (SETTLE_SAMPLES / 2)`, those older than the newest at or before `now - grace` are
/// let go, and at most [`SETTLE_SAMPLES`] are kept, the oldest let go first, which only dates a
/// family later; what its buffer holds is charged as it grows. Kept on the authority alone and
/// never replicated: a replica that becomes the authority dates every family it holds by its
/// first sample, a grace after it.
#[derive(Default)]
pub(crate) struct Settled {
    samples: VecDeque<(SessionSeq, u64)>,
    charge: Option<Allocation>,
}
impl Settled {
    /// The committed core at `at` held logical time `time`, read at logical time `now` under a
    /// grace of `grace_ms`. A sample the buffer cannot be grown for is not taken: the families
    /// it would have dated are dated by a later one.
    pub(crate) fn sample(
        &mut self,
        budget: &MemoryBudget,
        at: SessionSeq,
        time: u64,
        now: u64,
        grace_ms: u64,
    ) {
        let cut = now.saturating_sub(grace_ms);
        while self.samples.get(1).is_some_and(|(_, time)| *time <= cut) {
            self.samples.pop_front();
        }
        let gap = grace_ms
            .checked_div(u64::try_from(SETTLE_SAMPLES / 2).unwrap_or(u64::MAX))
            .unwrap_or(0)
            .max(1);
        if self
            .samples
            .back()
            .is_some_and(|(last, last_time)| *last >= at || time < last_time.saturating_add(gap))
        {
            return;
        }
        if self.samples.len() >= SETTLE_SAMPLES {
            self.samples.pop_front();
        }
        if self.samples.len() == self.samples.capacity() && !self.grow(budget) {
            return;
        }
        self.samples.push_back((at, time));
    }
    /// The buffer doubled (to [`SETTLE_SAMPLES`] at most, 16 at least), its charge admitted first.
    fn grow(&mut self, budget: &MemoryBudget) -> bool {
        if self.charge.is_none() {
            match budget.reserve(BudgetKind::Index, BudgetLane::Ordinary, 0) {
                Ok(permit) => self.charge = Some(permit.commit()),
                Err(_) => return false,
            }
        }
        let Some(charge) = self.charge.as_mut() else {
            return false;
        };
        let have = self.samples.capacity();
        let want = have.saturating_mul(2).clamp(16, SETTLE_SAMPLES);
        let Some(more) = want
            .checked_sub(have)
            .filter(|more| *more > 0)
            .and_then(|more| more.checked_mul(size_of::<(SessionSeq, u64)>()))
        else {
            return false;
        };
        if charge.grow(more).is_err() {
            return false;
        }
        if self
            .samples
            .try_reserve_exact(want.saturating_sub(self.samples.len()))
            .is_err()
        {
            let held = charge.bytes().saturating_sub(more);
            // Given back exactly what was admitted for it.
            let _ = charge.shrink_to(held);
            return false;
        }
        true
    }
    /// The latest logical time the record at `through` can have settled at: the first sample at or
    /// past it, none before one is taken there.
    pub(crate) fn by(&self, through: SessionSeq) -> Option<u64> {
        let first = self.samples.partition_point(|(at, _)| *at < through);
        self.samples.get(first).map(|(_, time)| *time)
    }
    /// The last record a grace of `grace_ms` has passed for at logical time `now`, as [`Self::by`]
    /// dates records: a record is past its grace exactly when it is at or before it. The samples
    /// past their grace are the oldest ones (time never falls along them), so a walk of them
    /// stops at the first that is not; none before a sample is a grace old.
    pub(crate) fn due_through(&self, now: u64, grace_ms: u64) -> Option<SessionSeq> {
        self.samples
            .iter()
            .take_while(|(_, time)| now.saturating_sub(*time) >= grace_ms)
            .last()
            .map(|(at, _)| *at)
    }
}

/// One family's bundle from the committed core (26 §4): only on the
/// authority, only when the family is eligible, every registered consumer
/// has read past its last event, and that event settled at least
/// `min_age_ms` of the node's logical time ago (a finished claim stays
/// readable in the core for the grace the operator sets); the bytes are
/// charged to `budget`.
pub(crate) fn archived_family(
    session: &Session,
    budget: &MemoryBudget,
    settled: &mut Settled,
    root: ClaimId,
    min_age_ms: u64,
) -> Result<Option<ArchivedFamily>, LedgerError> {
    let Some(family) = eligible_family(session, budget, settled, root, min_age_ms)? else {
        return Ok(None);
    };
    let limits = session.native_encoding_limits()?;
    let core = session.native_core()?;
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
    let identity = focal_core::native::retirement_batch::family_digest(session.ledger(), &family)
        .map_err(|error| LedgerError::Native(error.into()))?;
    Ok(Some(ArchivedFamily {
        root,
        members: family.members,
        through,
        digest,
        family: identity,
        rows,
        bundle,
        _allocation: allocation,
    }))
}
/// The family rooted at `root` when it may retire now (26 §4): only on the authority, only when
/// it is eligible, every registered consumer has read past its last event, and that event settled
/// at least `min_age_ms` of the node's logical time ago as `settled` dates it, the committed
/// core sampled into it first.
fn eligible_family(
    session: &Session,
    budget: &MemoryBudget,
    settled: &mut Settled,
    root: ClaimId,
    min_age_ms: u64,
) -> Result<Option<focal_core::native::retirement::RetirementFamily>, LedgerError> {
    if !session.native_authoritative() {
        return Ok(None);
    }
    let now = crate::native_ingress::logical_time(session)
        .map_err(|_| LedgerError::NotReady { leader: 0 })?;
    let report = session.native_retention()?;
    let core = session.native_core()?;
    settled.sample(
        budget,
        core.native_sequence(),
        core.native_logical_time(),
        now,
        min_age_ms,
    );
    Ok(eligible_in(core, &report, now, root, min_age_ms, settled))
}
/// [`eligible_family`] on a core read once, under the session's retention report, logical time
/// and samples read once: what a walk asks of every family it visits.
fn eligible_in(
    core: &focal_core::Core<focal_core::native::NativeState>,
    report: &focal_ledger::RetentionReport,
    now: u64,
    root: ClaimId,
    min_age_ms: u64,
    settled: &Settled,
) -> Option<focal_core::native::retirement::RetirementFamily> {
    if !may_be_due(core, settled, now, min_age_ms) {
        return None;
    }
    let family = core.retirement_family(root).ok()?;
    if !report.allows_family(family.through) {
        return None;
    }
    // Its last event settled the grace ago, or its session is short of memory (doc 31 M3).
    core.retirement_due(settled.by(family.through), now, min_age_ms)
        .then_some(family)
}
/// Whether any family may be due at logical time `now` under a grace of `min_age_ms`
/// ([`focal_core::Core::retirement_due`]'s rule): the session is pressed for memory, which waives
/// the grace, or some record settled the grace ago. Neither, and no family is: none is derived to
/// learn so (a family's derivation walks its every member, row and event, and a walk derived one
/// a visit, 3.7% of a leader's processor at 100 work claims a second on compare CI, 2026-10-26,
/// where the grace is a day and no family is ever due).
fn may_be_due(
    core: &focal_core::Core<focal_core::native::NativeState>,
    settled: &Settled,
    now: u64,
    min_age_ms: u64,
) -> bool {
    core.native_memory_pressed() || settled.due_through(now, min_age_ms).is_some()
}
/// What one cold walk did (doc 31 M2, A2): how many families it retired, deferred and refused,
/// whether it found more than one batch of offers, whether it submitted a candidate, and where
/// the next walk resumes.
#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct ColdWalk {
    pub(crate) retired: usize,
    pub(crate) deferred: usize,
    pub(crate) refused: usize,
    pub(crate) full: bool,
    pub(crate) submitted: bool,
    pub(crate) next: Option<focal_core::native::retirement::RetirementCursor>,
}

/// One walk of the session's status index for a cold retirement (doc 31 M2, A2), on the
/// session's own thread: `visits` rows from `cursor`, every family that may retire now offered
/// ([`eligible_family`]'s rule, the retention report and logical time read once), and the offers
/// retired a batch at a time, each as consecutive candidates. A cold retirement needs nothing
/// from the node between a family's offer and its retirement, so one call does what took a call
/// a family; the work is bounded by `visits`. Only on the authority.
pub(crate) fn cold_walk(
    session: &mut Session,
    budget: &MemoryBudget,
    settled: &mut Settled,
    cursor: Option<focal_core::native::retirement::RetirementCursor>,
    min_age_ms: u64,
    visits: usize,
) -> Result<ColdWalk, LedgerError> {
    use focal_core::native::retirement_batch::{MAX_RETIRE_FAMILIES, RetireOffer, family_digest};
    if !session.native_authoritative() {
        return Ok(ColdWalk::default());
    }
    let now = crate::native_ingress::logical_time(session)
        .map_err(|_| LedgerError::NotReady { leader: 0 })?;
    let report = session.native_retention()?;
    let ledger = session.ledger();
    let core = session.native_core()?;
    settled.sample(
        budget,
        core.native_sequence(),
        core.native_logical_time(),
        now,
        min_age_ms,
    );
    // No family may be due: the pass is done, nothing visited.
    if !may_be_due(core, settled, now, min_age_ms) {
        return Ok(ColdWalk::default());
    }
    let candidates = core
        .retirement_candidates(cursor, visits)
        .map_err(|error| LedgerError::Native(error.into()))?;
    let mut offers = Vec::new();
    offers
        .try_reserve_exact(candidates.claims.len())
        .map_err(|_| LedgerError::Capacity)?;
    for root in &candidates.claims {
        let Some(family) = eligible_in(core, &report, now, *root, min_age_ms, settled) else {
            continue;
        };
        offers.push(RetireOffer {
            root: *root,
            family: family_digest(ledger, &family)
                .map_err(|error| LedgerError::Native(error.into()))?,
            bundle: focal_model::ContentHash([0; 32]),
            bytes: 0,
        });
    }
    let mut walk = ColdWalk {
        full: offers.len() > MAX_RETIRE_FAMILIES,
        next: candidates.next,
        ..ColdWalk::default()
    };
    let mut batches = offers.chunks(MAX_RETIRE_FAMILIES);
    for batch in batches.by_ref() {
        match session.native_retire(batch) {
            Ok((made, submission)) => {
                walk.retired = walk.retired.saturating_add(made.families.len());
                walk.deferred = walk.deferred.saturating_add(made.deferred.len());
                walk.refused = walk.refused.saturating_add(made.refused.len());
                walk.submitted |= submission.is_some();
                // Deferred for room (no candidate could take them) or a pending candidate:
                // the rest wait for the next walk as well.
                if !made.deferred.is_empty() {
                    break;
                }
            }
            Err(error) if walk.retired == 0 && walk.deferred == 0 => return Err(error),
            Err(_) => {
                walk.deferred = walk.deferred.saturating_add(batch.len());
                break;
            }
        }
    }
    walk.deferred = walk
        .deferred
        .saturating_add(batches.map(<[RetireOffer]>::len).sum::<usize>());
    Ok(walk)
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
    // Under the anchored rule half the window, if less, is enough: a window
    // smaller than a bundle seals before it fills.
    let enough = |bound: SealBound| {
        if anchored {
            (bound.rows / 2).min(window / 2).max(1)
        } else {
            (bound.rows / 2).max(1)
        }
    };
    // Short of half a bundle and pressed by nothing, nothing is planned, and
    // that is known from counters alone (`sealable_rows_bound`): deriving the
    // plan visits every outcome awaiting a seal, which grow with every write
    // between seals, and was done at every round of the archive agent on the
    // owner's thread only to be dropped (a stall every five seconds that grew
    // with them, 62 to 120 ms at 200 writes a second: run 38605001535, the
    // leader's `range: seal bundle` slow steps).
    if !pressed && core.sealable_rows_bound() < enough(bound) {
        return Ok(SealOffer::Nothing);
    }
    let (plan, quote) = loop {
        let plan = match core.seal_plan(&floors, bound) {
            Ok(plan) => plan,
            Err(SealRefusal::Nothing) => return Ok(SealOffer::Nothing),
            Err(SealRefusal::Capacity) => return Err(LedgerError::Capacity),
            Err(_) => return Err(LedgerError::Corrupt),
        };
        // The counters bound the plan from above; the plan itself decides.
        // A plan short of half a bundle is not encoded to be dropped (a
        // plan short of it fits the bundle, so its quote never halved the
        // bound).
        if !pressed && plan.rows() < enough(bound) {
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

#[cfg(test)]
#[path = "archive_derive_tests.rs"]
mod tests;
