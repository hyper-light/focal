//! Receipt-scoped respondent responsibility shares the completion pool, finite
//! counters and candidate journal with evaluator reports. Remaining credit is
//! reconstructed from real cycle/response records, never decremented from a
//! participant-provided action flag.
use super::*;
use crate::native::respondent_envelope::RespondentEnvelope;
use crate::native::respondent_state::{self, RespondentCredit, RespondentSpend};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct RespondentUpdate {
    pub(super) key: RespondentKey,
    pub(super) before: RespondentCredit,
    pub(super) after: RespondentCredit,
}

#[derive(Debug)]
pub(super) struct RespondentGrant {
    credit: RespondentCredit,
    envelope: RespondentEnvelope,
    verification: NativeVerificationBudget,
    // Reset for the complete affected set before each collection. These cells
    // only avoid repeated source traversal during this one candidate.
    seen: std::cell::Cell<bool>,
    update: std::cell::Cell<Option<RespondentUpdate>>,
}

/// What a receipt's action is checked against: its contract's bounds and its verification
/// budget. The action itself is charged to the state's budget, on the completion lane.
pub(in crate::native) struct RespondentLoan<'a> {
    envelope: &'a RespondentEnvelope,
    verification: &'a NativeVerificationBudget,
}
impl RespondentLoan<'_> {
    pub(in crate::native) fn envelope(&self) -> &RespondentEnvelope {
        self.envelope
    }
    pub(in crate::native) fn verification(&self) -> &NativeVerificationBudget {
        self.verification
    }
}

/// A respondent action affects one receipt and uses inline bookkeeping. A
/// graph/control candidate can retire several receipts; its own full envelope
/// retains this independently of the evaluation-update journal.
pub(in crate::native) fn respondent_journal_bytes(records: usize) -> Result<usize, NativeError> {
    if records <= 1 {
        Ok(0)
    } else {
        array::<RespondentUpdate>(records)
    }
}

/// Three complete event passes and two final update passes dominate the
/// collector's scalar work, including both source/grant lookups per owner fact.
/// Actual source-history validation has its separately checked bounded stage.
pub(in crate::native) fn respondent_journal_visits(
    events: usize,
    claims: usize,
) -> Result<usize, NativeError> {
    add(mul(events, 9)?, mul(claims, 2)?)
}

fn demand(envelope: RespondentEnvelope, credit: RespondentCredit) -> Result<Totals, NativeError> {
    if credit.actions()? == 0 {
        return Ok(Totals::default());
    }
    let demand = envelope.demand(credit)?;
    let actions = usize::try_from(demand.actions).map_err(|_| ContractError::Capacity)?;
    if actions != credit.actions()? {
        return Err(ContractError::InvalidManifest.into());
    }
    Ok(Totals {
        // Nothing is set aside for a receipt's future actions: each is charged what it holds as
        // it is written, on the completion lane (`respondent_owner::build`). Set aside for the
        // worst each could hold, a receipt took 29 MB of the session's share (12 actions, 1.2 to
        // 3.6 MB each), and about twenty messages in flight refused every other write
        // (deliveries at 200 a second, 2026-10-14).
        // This counter reserves candidate identities for all remaining funded
        // actions. Its original name is retained for evaluator-only callers.
        reports: actions,
        slots: demand.slots,
        live: 1,
        graphs: 0,
    })
}

fn key(claim: &ClaimState) -> Option<RespondentKey> {
    let receipt = claim.receipt()?.fence;
    Some(RespondentKey {
        claim: ClaimId(claim.binding().object.0),
        receipt: receipt.receipt,
        epoch: receipt.epoch,
    })
}

fn owner(fact: NativeFact) -> Option<ClaimId> {
    match fact {
        NativeFact::Claim(event) => Some(ClaimId(event.after.object.0)),
        NativeFact::Receipt { claim, .. }
        | NativeFact::ReceiptAdopted { claim, .. }
        | NativeFact::Registrations { claim } => Some(ClaimId(claim.object.0)),
        NativeFact::Work { claim, .. }
        | NativeFact::Diagnostic { claim, .. }
        | NativeFact::Response { claim, .. } => Some(claim),
        // Definitions exist before receipt or accompany a registry/claim fact.
        // Independent evaluator/results mutate no respondent source or heap.
        NativeFact::Definition { .. }
        | NativeFact::ResultTestament { .. }
        | NativeFact::Evaluation { .. }
        | NativeFact::Missing { .. }
        | NativeFact::Delivery { .. }
        | NativeFact::Accepted { .. }
        | NativeFact::Artifact { .. } => None,
    }
}

fn take(visits: &mut usize, count: usize) -> Result<(), NativeError> {
    *visits = visits
        .checked_sub(count)
        .ok_or(NativeError::Capacity("respondent journal visits"))?;
    Ok(())
}

impl CompletionBook {
    pub(in crate::native) fn respondent_contract<'a>(
        &'a self,
        key: RespondentKey,
        spend: RespondentSpend,
        view: &View<'_>,
        schemas: &impl NativeSchemaVerifier,
    ) -> Result<RespondentLoan<'a>, NativeError> {
        self.check_health()?;
        let grant = self
            .respondents
            .get(key)
            .ok_or(ContractError::StaleReceipt)?;
        let claim = view.claim(key.claim).ok_or(ContractError::InvalidTarget)?;
        if respondent_state::read(view, claim, self.limits)? != Some((key, grant.credit)) {
            return Err(ContractError::StaleReceipt.into());
        }
        let remaining = match spend {
            RespondentSpend::Diagnostic => grant.credit.diagnostics,
            RespondentSpend::Close => grant.credit.closes,
            RespondentSpend::Post => grant.credit.posts,
        };
        if remaining == 0 {
            return Err(ContractError::InvalidTransition.into());
        }
        grant.envelope.check_parent(view, claim, self.limits)?;
        if spend == RespondentSpend::Diagnostic {
            grant
                .verification
                .check_schema(focal_evidence::error_report_schema(), schemas)?;
        }
        Ok(RespondentLoan {
            envelope: &grant.envelope,
            verification: &grant.verification,
        })
    }

    pub(in crate::native) fn install_recovered_respondent(
        &mut self,
        view: &View<'_>,
        claim: &ClaimState,
        envelope: &RespondentEnvelope,
        verification: NativeVerificationBudget,
    ) -> Result<(), NativeError> {
        if self.journals != 0 {
            return Err(ContractError::InvalidTransition.into());
        }
        let journal = self.install_respondent(view, claim, envelope, verification)?;
        self.commit(journal)
    }

    pub(in crate::native) fn install_respondent(
        &mut self,
        view: &View<'_>,
        claim: &ClaimState,
        envelope: &RespondentEnvelope,
        verification: NativeVerificationBudget,
    ) -> Result<Journal, NativeError> {
        let recorded;
        let envelope = if let Some(limits) = self.record_buffers {
            recorded = envelope.with_record_buffers(limits)?;
            &recorded
        } else {
            envelope
        };
        self.check_health()?;
        let Some((key, credit)) = respondent_state::read(view, claim, self.limits)? else {
            return Ok(Journal::empty());
        };
        if credit.actions()? == 0 {
            return Ok(Journal::empty());
        }
        if self.respondents.get(key).is_some()
            || verification.schema() != focal_evidence::error_report_schema()
        {
            return Err(ContractError::InvalidTransition.into());
        }
        envelope.check_parent(view, claim, self.limits)?;
        let before = self.totals;
        let totals = before.replace(Totals::default(), demand(*envelope, credit)?)?;
        let journals = add(self.journals, 1)?;
        let revision = self.next_revision()?;
        let growth = self.respondents.grow(&self.parent, self.limits.receipts)?;
        if let Err(error) = self.respondents.insert(
            key,
            RespondentGrant {
                credit,
                envelope: *envelope,
                verification,
                seen: std::cell::Cell::new(false),
                update: std::cell::Cell::new(None),
            },
        ) {
            if let Some(growth) = growth {
                self.respondents.restore_growth(growth)?;
            }
            return Err(error);
        }
        self.totals = totals;
        self.journals = journals;
        let before_revision = self.revision;
        self.revision = revision;
        Ok(Journal {
            owner: Some(self.owner),
            before_revision,
            after_revision: revision,
            before,
            change: Change::RespondentInstall {
                key,
                credit,
                growth,
            },
        })
    }

    fn affected_respondents(
        &self,
        source: &View<'_>,
        prepared: &NativePrepared,
        visits: &mut usize,
        mut visit: impl FnMut(RespondentKey, &RespondentGrant) -> Result<(), NativeError>,
    ) -> Result<(), NativeError> {
        for ordinal in 0..prepared.outcome.events {
            take(visits, 1)?; // retained event
            let event = Self::event(prepared, ordinal, source.payloads())?;
            if event.sequence != prepared.outcome.sequence
                || event.ordinal != ordinal
                || event.invocation != prepared.outcome.invocation
            {
                return Err(ContractError::InvalidCut.into());
            }
            let Some(id) = owner(event.fact) else {
                continue;
            };
            take(visits, 2)?; // actual source owner and indexed grant
            let Some(key) = source.claim(id).and_then(key) else {
                continue;
            };
            if let Some(grant) = self.respondents.get(key) {
                visit(key, grant)?;
            }
        }
        self.check_health()
    }

    fn resolve_respondent(
        &self,
        source: &View<'_>,
        candidate: &View<'_>,
        key: RespondentKey,
        grant: &RespondentGrant,
    ) -> Result<Option<RespondentUpdate>, NativeError> {
        let before = source
            .claim(key.claim)
            .ok_or(ContractError::InvalidTarget)?;
        let expected = respondent_state::read(source, before, self.limits)?;
        if grant.credit.actions()? == 0 {
            // Earlier pending terminalization keeps its zero-credit key until
            // that exact journal commits. Later audit observations cannot revive it.
            if expected.is_some_and(|(_, credit)| credit != RespondentCredit::default()) {
                return Err(ContractError::StaleReceipt.into());
            }
            return Ok(None);
        }
        if expected != Some((key, grant.credit)) {
            return Err(ContractError::StaleReceipt.into());
        }
        let claim = candidate
            .claim(key.claim)
            .ok_or(ContractError::InvalidTarget)?;
        let next = respondent_state::read(candidate, claim, self.limits)?;
        let after = match next {
            Some((next_key, credit)) if next_key == key => {
                if credit.actions()? != 0 {
                    grant.envelope.check_parent(candidate, claim, self.limits)?;
                }
                credit
            }
            _ => RespondentCredit::default(),
        };
        if after.diagnostics > grant.credit.diagnostics
            || after.closes > grant.credit.closes
            || after.posts > grant.credit.posts
        {
            return Err(ContractError::InvalidTransition.into());
        }
        Ok((after != grant.credit).then_some(RespondentUpdate {
            key,
            before: grant.credit,
            after,
        }))
    }

    /// All source checks and allocations precede persistent mutation. Source
    /// marks are reset for every affected key before reuse after any refusal.
    /// The owner must drop prepared pages before rolling this journal back.
    pub(in crate::native) fn apply_respondents(
        &mut self,
        source: &View<'_>,
        prepared: &NativePrepared,
        funding: JournalFunding<'_>,
    ) -> Result<Journal, NativeError> {
        self.check_health()?;
        source.check_successor(prepared)?;
        if self.respondents.len() == 0 {
            return Ok(Journal::empty());
        }
        let candidate = source.with_own(prepared)?;
        let mut visits = self.limits.plan_edges;
        self.affected_respondents(source, prepared, &mut visits, |_, grant| {
            grant.seen.set(false);
            grant.update.set(None);
            Ok(())
        })?;
        let mut count = 0usize;
        let mut one = None;
        self.affected_respondents(source, prepared, &mut visits, |key, grant| {
            if grant.seen.replace(true) {
                return Ok(());
            }
            let update = self.resolve_respondent(source, &candidate, key, grant)?;
            if let Some(update) = update {
                count = add(count, 1)?;
                one = Some(update);
                grant.update.set(Some(update));
            }
            Ok(())
        })?;
        if count == 0 {
            return Ok(Journal::empty());
        }
        if count == 1 {
            return self.apply_respondent_one(one.ok_or(ContractError::InvalidTransition)?);
        }
        let bytes = respondent_journal_bytes(count)?;
        let JournalFunding::External {
            source: budget,
            lane,
        } = funding;
        let reservation = budget.reserve(BudgetKind::Pending, lane, bytes)?;
        let mut updates = Vec::new();
        updates
            .try_reserve_exact(count)
            .map_err(|_| MemoryError::AllocationFailed)?;
        within(array::<RespondentUpdate>(updates.capacity())?, bytes)?;
        self.affected_respondents(source, prepared, &mut visits, |_, grant| {
            if let Some(update) = grant.update.take() {
                if updates.len() == updates.capacity() {
                    return Err(ContractError::Capacity.into());
                }
                updates.push(update);
            }
            Ok(())
        })?;
        if updates.len() != count {
            return Err(ContractError::InvalidManifest.into());
        }
        let mut totals = self.totals;
        for update in &updates {
            take(&mut visits, 1)?;
            let grant = self
                .respondents
                .get(update.key)
                .ok_or(ContractError::StaleReceipt)?;
            if grant.credit != update.before {
                return Err(ContractError::StaleReceipt.into());
            }
            totals = totals.replace(
                demand(grant.envelope, update.before)?,
                demand(grant.envelope, update.after)?,
            )?;
        }
        let journals = add(self.journals, 1)?;
        let revision = self.next_revision()?;
        take(&mut visits, updates.len())?;
        let before = self.totals;
        for update in &updates {
            self.set_respondent(*update, false)?;
        }
        self.totals = totals;
        self.check_health()?;
        self.journals = journals;
        let before_revision = self.revision;
        self.revision = revision;
        Ok(Journal {
            owner: Some(self.owner),
            before_revision,
            after_revision: revision,
            before,
            change: Change::RespondentMany {
                updates,
                _allocation: reservation.commit(),
            },
        })
    }

    fn apply_respondent_one(&mut self, update: RespondentUpdate) -> Result<Journal, NativeError> {
        let grant = self
            .respondents
            .get(update.key)
            .ok_or(ContractError::StaleReceipt)?;
        if grant.credit != update.before {
            return Err(ContractError::StaleReceipt.into());
        }
        let totals = self.totals.replace(
            demand(grant.envelope, update.before)?,
            demand(grant.envelope, update.after)?,
        )?;
        let journals = add(self.journals, 1)?;
        let revision = self.next_revision()?;
        let before = self.totals;
        self.set_respondent(update, false)?;
        self.totals = totals;
        self.check_health()?;
        self.journals = journals;
        let before_revision = self.revision;
        self.revision = revision;
        Ok(Journal {
            owner: Some(self.owner),
            before_revision,
            after_revision: revision,
            before,
            change: Change::RespondentOne(update),
        })
    }

    fn set_respondent(
        &mut self,
        update: RespondentUpdate,
        restore: bool,
    ) -> Result<(), NativeError> {
        let credit = if restore { update.before } else { update.after };
        if self.respondents.get(update.key).is_none() {
            return Err(ContractError::StaleReceipt.into());
        }
        self.respondents
            .update(update.key, |grant| grant.credit = credit)
    }

    pub(super) fn restore_respondent(
        &mut self,
        update: RespondentUpdate,
    ) -> Result<(), NativeError> {
        self.set_respondent(update, true)
    }

    pub(super) fn check_respondent_update(
        &self,
        update: RespondentUpdate,
    ) -> Result<(), NativeError> {
        if self
            .respondents
            .get(update.key)
            .is_none_or(|grant| grant.credit != update.after)
        {
            return Err(ContractError::StaleReceipt.into());
        }
        Ok(())
    }

    pub(super) fn check_respondent_install(
        &self,
        key: RespondentKey,
        credit: RespondentCredit,
        growth: Option<&IndexGrowth<RespondentGrant, RespondentKey>>,
    ) -> Result<(), NativeError> {
        if self
            .respondents
            .get(key)
            .is_none_or(|grant| grant.credit != credit)
        {
            return Err(ContractError::StaleReceipt.into());
        }
        if let Some(growth) = growth {
            self.respondents.check_remove_restore_growth(growth, key)?;
        }
        Ok(())
    }
}

#[cfg(test)]
#[path = "respondent_book_tests.rs"]
mod tests;
