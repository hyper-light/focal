//! One candidate can install/update evaluation responsibility, update respondent
//! credit, and install a replacement receipt. Four fixed ownership slots keep
//! these journals inline without a recursive journal or another allocation.
use super::*;

#[derive(Debug)]
pub(in crate::native) struct CandidateJournal {
    first: Journal,
    second: Option<Journal>,
    respondent: Option<Journal>,
    receipt: Option<Journal>,
}

impl CandidateJournal {
    pub(in crate::native) fn single(first: Journal) -> Self {
        Self {
            first,
            second: None,
            respondent: None,
            receipt: None,
        }
    }

    /// Call `check_begin_composition` before transferring either journal. A
    /// refusal then leaves both rollback owners in the caller's hands.
    pub(in crate::native) fn begin(first: Journal, second: Journal) -> Self {
        if matches!(second.change, Change::None) {
            Self::single(first)
        } else {
            Self {
                first,
                second: Some(second),
                respondent: None,
                receipt: None,
            }
        }
    }
    /// The caller first checks the borrowed journals, preserving all rollback
    /// ownership if composition is refused.
    pub(in crate::native) fn with_respondents(mut self, update: Journal, install: Journal) -> Self {
        if !matches!(update.change, Change::None) {
            self.respondent = Some(update);
        }
        if !matches!(install.change, Change::None) {
            self.receipt = Some(install);
        }
        self
    }

    fn journals(&self) -> impl Iterator<Item = &Journal> {
        [
            Some(&self.first),
            self.second.as_ref(),
            self.respondent.as_ref(),
            self.receipt.as_ref(),
        ]
        .into_iter()
        .flatten()
    }
}

fn updates(change: &Change) -> Result<&[Update], NativeError> {
    match change {
        Change::One(update) => Ok(std::slice::from_ref(update)),
        Change::Many { updates, .. } if !updates.is_empty() => Ok(updates),
        _ => Err(ContractError::InvalidTransition.into()),
    }
}

fn empty(journal: &Journal) -> bool {
    matches!(journal.change, Change::None)
        && journal.owner.is_none()
        && journal.before_revision == 0
        && journal.after_revision == 0
        && journal.before == Totals::default()
}

impl CompletionBook {
    pub(in crate::native) fn check_begin_composition(
        &self,
        begin: &Journal,
        update: &Journal,
    ) -> Result<(), NativeError> {
        self.check_health()?;
        self.check_journal(begin)?;
        let Change::Begin { key, credit, .. } = &begin.change else {
            return Err(ContractError::InvalidTransition.into());
        };
        if begin.before_revision.checked_add(1) != Some(begin.after_revision)
            || begin.after_revision > self.revision
        {
            return Err(ContractError::InvalidTransition.into());
        }
        if empty(update) {
            return Ok(());
        }
        self.check_journal(update)?;
        if begin.after_revision != update.before_revision
            || update.before_revision.checked_add(1) != Some(update.after_revision)
            || update.after_revision > self.revision
        {
            return Err(ContractError::InvalidTransition.into());
        }
        let grant = self.grant(*key)?;
        if begin
            .before
            .replace(Totals::default(), demand(grant.envelope, *credit)?)?
            != update.before
        {
            return Err(ContractError::InvalidTransition.into());
        }
        let rows = updates(&update.change)?;
        within(rows.len(), self.limits.plan_edges)?;
        let mut previous = None;
        for row in rows {
            if previous.is_some_and(|old| old >= row.key)
                || (row.key == *key && row.before != *credit)
            {
                return Err(ContractError::InvalidTransition.into());
            }
            previous = Some(row.key);
        }
        Ok(())
    }

    pub(in crate::native) fn check_respondent_composition(
        &self,
        candidate: &CandidateJournal,
        update: &Journal,
        install: &Journal,
    ) -> Result<(), NativeError> {
        if candidate.respondent.is_some() || candidate.receipt.is_some() {
            return Err(ContractError::InvalidTransition.into());
        }
        if !empty(update)
            && !matches!(
                update.change,
                Change::RespondentOne(_) | Change::RespondentMany { .. }
            )
        {
            return Err(ContractError::InvalidTransition.into());
        }
        if !empty(install) && !matches!(install.change, Change::RespondentInstall { .. }) {
            return Err(ContractError::InvalidTransition.into());
        }
        self.check_candidate_sequence(candidate.journals().chain([update, install]))
    }

    fn check_candidate_sequence<'a>(
        &self,
        journals: impl Iterator<Item = &'a Journal>,
    ) -> Result<(), NativeError> {
        self.check_health()?;
        let mut previous = None;
        let mut count = 0usize;
        for journal in journals {
            if empty(journal) {
                continue;
            }
            self.check_journal(journal)?;
            if journal.before_revision.checked_add(1) != Some(journal.after_revision)
                || journal.after_revision > self.revision
                || previous.is_some_and(|value| value != journal.before_revision)
            {
                return Err(ContractError::InvalidTransition.into());
            }
            previous = Some(journal.after_revision);
            count = add(count, 1)?;
        }
        if count > 4 || count > self.journals {
            return Err(ContractError::InvalidTransition.into());
        }
        Ok(())
    }

    fn check_candidate(
        &self,
        candidate: &CandidateJournal,
        rollback: bool,
    ) -> Result<(), NativeError> {
        self.check_candidate_sequence(candidate.journals())?;
        if let Some(second) = &candidate.second {
            self.check_begin_composition(&candidate.first, second)?;
        }
        let mut tail = None;
        for journal in candidate.journals() {
            if empty(journal) {
                continue;
            }
            tail = Some(journal.after_revision);
            match &journal.change {
                Change::Begin {
                    key,
                    credit,
                    growth,
                    ..
                } if rollback => {
                    let mut projected = self.grant(*key)?.credit;
                    if let Some(second) = &candidate.second {
                        for update in updates(&second.change)? {
                            if update.key == *key {
                                projected = update.before;
                            }
                        }
                    }
                    if projected != *credit {
                        return Err(ContractError::StaleEvaluation.into());
                    }
                    if let Some(growth) = growth {
                        self.entries.check_remove_restore_growth(growth, *key)?;
                    }
                }
                Change::One(update) if rollback || update.after.remaining_reports == 0 => {
                    self.check_update(*update)?
                }
                Change::Many { updates, .. } => {
                    for update in updates {
                        if rollback || update.after.remaining_reports == 0 {
                            self.check_update(*update)?;
                        }
                    }
                }
                Change::RespondentInstall {
                    key,
                    credit,
                    growth,
                    ..
                } if rollback => {
                    self.check_respondent_install(*key, *credit, growth.as_ref())?;
                }
                Change::RespondentOne(update) if rollback || update.after.actions()? == 0 => {
                    self.check_respondent_update(*update)?
                }
                Change::RespondentMany { updates, .. } => {
                    for update in updates {
                        if rollback || update.after.actions()? == 0 {
                            self.check_respondent_update(*update)?;
                        }
                    }
                }
                _ => {}
            }
        }
        if rollback && tail.is_some_and(|tail| tail != self.revision) {
            return Err(ContractError::InvalidTransition.into());
        }
        Ok(())
    }

    /// Prepared pages and all successors are dropped first. Check all four
    /// ownership slots before undoing any; release install backing last within
    /// its exact reverse publication order.
    pub(in crate::native) fn rollback_candidate(
        &mut self,
        candidate: CandidateJournal,
    ) -> Result<(), NativeError> {
        self.check_candidate(&candidate, true)?;
        let CandidateJournal {
            first,
            second,
            respondent,
            receipt,
        } = candidate;
        if let Some(receipt) = receipt {
            self.rollback(receipt)?;
        }
        if let Some(respondent) = respondent {
            self.rollback(respondent)?;
        }
        if let Some(second) = second {
            self.rollback(second)?;
        }
        self.rollback(first)
    }

    /// Live earlier grants may already have younger pending updates. Finalize
    /// only this candidate's retired grants; preserve every younger credit.
    pub(in crate::native) fn commit_candidate(
        &mut self,
        candidate: CandidateJournal,
    ) -> Result<(), NativeError> {
        self.check_candidate(&candidate, false)?;
        let CandidateJournal {
            first,
            second,
            respondent,
            receipt,
        } = candidate;
        self.commit(first)?;
        if let Some(second) = second {
            self.commit(second)?;
        }
        if let Some(respondent) = respondent {
            self.commit(respondent)?;
        }
        if let Some(receipt) = receipt {
            self.commit(receipt)?;
        }
        Ok(())
    }
}
