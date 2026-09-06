//! Admission projection over the sole owner's complete retained rows.
//! No aggregate, policy copy, result vector or independently mutable summary is
//! constructed. The owner supplies actual rows at one effective publication cut.
use super::*;
use crate::lifecycle::claim::{ClaimState, ClaimTerminalCut};
use crate::lifecycle::validation::{Declaration, EvaluationState};

#[derive(Debug, Clone, Copy)]
pub struct AdmissionLimits {
    pub declarations: usize,
    pub evaluations: usize,
    pub visits: usize,
}

/// Borrowed immutable history, including a result staged in the current owner
/// transaction. Only the owner assigns these coordinates. This is not an input
/// shape for participant-authored lifecycle or publication claims.
#[derive(Debug, Clone, Copy)]
pub struct PublishedAdmissionResult<'a> {
    pub result: &'a AcceptedResult,
    pub sequence: SessionSeq,
    pub ordinal: u32,
}

/// Trusted lookup into the actual effective owner prefix, including its pending
/// tail. Implementations must not filter this view using participant selections.
pub trait AdmissionView {
    fn prefix(&self) -> SessionSeq;
    fn declaration(&self, id: ValidationId) -> Option<&Declaration>;
    fn evaluation(&self, registered: RegisteredEvaluation) -> Option<&EvaluationState>;
    fn accepted(&self, result: &AcceptedResult) -> Option<PublishedAdmissionResult<'_>>;
}

struct Visits(usize);
impl Visits {
    fn take(&mut self, count: usize) -> Result<(), ContractError> {
        self.0 = self.0.checked_sub(count).ok_or(ContractError::Capacity)?;
        Ok(())
    }
}

fn publication<'a>(
    claim: &ClaimState,
    registered: RegisteredEvaluation,
    state: &EvaluationState,
    view: &'a impl AdmissionView,
    visits: &mut Visits,
) -> Result<Option<PublishedAdmissionResult<'a>>, ContractError> {
    let Some(last) = state.last_result() else {
        if state.state().is_terminal() {
            return Err(ContractError::MissingEvidence);
        }
        return Ok(None);
    };
    visits.take(1)?;
    let published = view.accepted(&last).ok_or(ContractError::MissingEvidence)?;
    if *published.result != last {
        return Err(ContractError::ContentConflict);
    }
    if !registered.matches(last)
        || last.binding().revision <= registered.binding().revision
        || last.binding().revision > state.binding().revision
    {
        return Err(ContractError::StaleEvaluation);
    }
    if published.sequence.0 == 0
        || published.sequence < claim.created()
        || published.sequence > view.prefix()
    {
        return Err(ContractError::InvalidCut);
    }
    if !state.has_begun()
        || last.is_terminal() != state.state().is_terminal()
        || (last.is_terminal() && last.resulting_state() != state.state())
    {
        return Err(ContractError::InvalidTransition);
    }
    visits.take(claim.acceptance().declarations().len())?;
    claim.acceptance().check_result(last)?;
    Ok(Some(published))
}

/// Derive receipt eligibility or the original Admission failure from complete
/// registration and retained typed result capabilities. Observe results remain
/// checked evidence but cannot delay or fail Required acceptance. Intermediate
/// Pass/Error attempts do not satisfy or fail a requirement.
pub fn project_admission<'a>(
    claim: &'a ClaimState,
    registrations: &RegistrationSet,
    view: &impl AdmissionView,
    limits: AdmissionLimits,
) -> Result<AdmissionDecision<'a>, ContractError> {
    if claim.created() > view.prefix() {
        return Err(ContractError::InvalidCut);
    }
    let policy = claim.acceptance();
    let declarations = policy.declarations();
    let rows = registrations.rows();
    if declarations.len() > limits.declarations || rows.len() > limits.evaluations {
        return Err(ContractError::Capacity);
    }
    let mut visits = Visits(limits.visits);
    visits.take(declarations.len())?;
    visits.take(policy.slots.len())?;
    for slot in &policy.slots {
        visits.take(slot.checks.len())?;
    }
    registrations.check(claim)?;
    let mut seen = 0usize;
    let mut all_passed = true;
    let mut first: Option<(TerminalCut, u32)> = None;
    // A Generated claim has no eligible Admission cohort yet. Once posted or
    // any member exists, every Admission declaration (including Observe) must
    // have its corresponding registration. Explicitly controlled pre-post claims
    // may legitimately have none; their phase independently forbids a receipt.
    visits.take(rows.len())?;
    let complete = matches!(
        claim.status(),
        ClaimStatus::Posted
            | ClaimStatus::Received
            | ClaimStatus::Progressed
            | ClaimStatus::TestamentGenerated
            | ClaimStatus::TestamentAcknowledged
            | ClaimStatus::Validating
    ) || rows
        .iter()
        .any(|row| matches!(row.target(), Target::Admission { .. }));
    for summary in declarations {
        visits.take(
            declarations
                .len()
                .checked_add(1)
                .ok_or(ContractError::Capacity)?,
        )?;
        let definition = view
            .declaration(ValidationId(summary.binding().object.0))
            .ok_or(ContractError::MissingEvidence)?;
        policy.check_declaration(definition)?;
        let admission = summary.target() == ObligationTarget::Admission;
        let mut members = 0usize;
        for (position, registered) in rows.iter().copied().enumerate() {
            visits.take(1)?;
            if registered.binding().object != summary.binding().object {
                continue;
            }
            seen = seen.checked_add(1).ok_or(ContractError::Capacity)?;
            visits.take(1)?;
            let state = view
                .evaluation(registered)
                .ok_or(ContractError::MissingEvidence)?;
            registered.check_state(*state, definition)?;
            if !admission {
                continue;
            }
            members = members.checked_add(1).ok_or(ContractError::Capacity)?;
            if members > 1 {
                return Err(ContractError::InvalidManifest);
            }
            let Target::Admission { claim: target } = state.target() else {
                return Err(ContractError::InvalidTarget);
            };
            same_content(claim.binding(), target)?;
            if target.revision > claim.binding().revision || registered.receipt().is_some() {
                return Err(ContractError::StaleEvaluation);
            }
            let published = publication(claim, registered, state, view, &mut visits)?;
            if let Some(published) = published {
                // A publication ordinal identifies one real event, even when
                // cause precedence within that mutation uses the canonical key.
                for earlier in rows.iter().copied().take(position) {
                    visits.take(1)?;
                    if !matches!(earlier.target(), Target::Admission { .. }) {
                        continue;
                    }
                    visits.take(1)?;
                    let state = view
                        .evaluation(earlier)
                        .ok_or(ContractError::MissingEvidence)?;
                    if let Some(last) = state.last_result() {
                        visits.take(1)?;
                        let old = view.accepted(&last).ok_or(ContractError::MissingEvidence)?;
                        if (old.sequence, old.ordinal) == (published.sequence, published.ordinal) {
                            return Err(ContractError::ConflictingCause);
                        }
                    }
                }
                if summary.mode() == ValidationMode::Required && published.result.is_terminal() {
                    if let Some(cause) = cause(published.result, ValidationMode::Required)? {
                        let cut = TerminalCut {
                            sequence: published.sequence,
                            cause,
                        };
                        if first.is_none_or(|(old, _)| {
                            (cut.sequence, cut.cause.key) < (old.sequence, old.cause.key)
                        }) {
                            first = Some((cut, published.ordinal));
                        }
                    }
                }
            }
            if summary.mode() == ValidationMode::Required
                && !published.is_some_and(|row| {
                    row.result.is_terminal() && row.result.verdict() == VerdictValue::Pass
                })
            {
                all_passed = false;
            }
        }
        if admission && members == 0 {
            if complete {
                return Err(ContractError::MissingEvidence);
            }
            if summary.mode() == ValidationMode::Required {
                all_passed = false;
            }
        }
    }
    if seen != rows.len() {
        return Err(ContractError::InvalidPolicy);
    }
    let outcome = match first {
        Some((cut, _)) => AdmissionOutcome::Blocked(cut),
        None if all_passed => AdmissionOutcome::Passed,
        None => AdmissionOutcome::Pending,
    };
    if let Some(ClaimTerminalCut::Required(original)) = claim.terminal_cut()
        && original.cause().key().target == CauseTarget::Admission
        && outcome != AdmissionOutcome::Blocked(original)
    {
        return Err(ContractError::ConflictingCause);
    }
    Ok(AdmissionDecision {
        binding: claim.binding(),
        acceptance: policy,
        outcome,
        blocking_publication: first.map(|(cut, ordinal)| (cut.sequence, ordinal)),
    })
}

#[cfg(test)]
#[path = "aggregation_admission_tests.rs"]
mod tests;
