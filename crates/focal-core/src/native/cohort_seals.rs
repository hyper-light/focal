//! Detached, checked audit-cohort suffix. Original transaction rows and event
//! coordinates remain immutable until the original history has been emitted.
use super::claim_changes::OriginalPlan;
use super::cohort_budget::CohortBudget;
use super::prepare::{Extras, Scratch, add, array, within};
use super::*;
use focal_memory::{Change, Entry};
use focal_model::lifecycle::aggregation::RegisteredEvaluation;
use focal_model::lifecycle::validation::{Attempt, SealTransition};

#[derive(Clone, Copy)]
pub(super) struct SealUpdate {
    key: EvaluationKey,
    token_index: usize,
    original_extra: Option<usize>,
    attempt: Option<Attempt>,
}

pub(super) struct CohortSeals {
    tokens: Vec<SealTransition>,
    updates: Vec<SealUpdate>,
    registries: Vec<(ClaimId, RegistrationSet)>,
    additional_evaluations: usize,
    visits: Visits,
}

struct Visits {
    remaining: usize,
}
impl Visits {
    fn take(&mut self, count: usize) -> Result<(), NativeError> {
        self.remaining = self
            .remaining
            .checked_sub(count)
            .ok_or(NativeError::Capacity("cohort seal visits"))?;
        Ok(())
    }
}

impl CohortSeals {
    /// The sealed state of `key` if this suffix seals it. The updates are
    /// sorted by key; the search is bounded by their count.
    pub(super) fn sealed(&self, key: EvaluationKey) -> Option<validation::EvaluationState> {
        let index = self
            .updates
            .binary_search_by_key(&key, |update| update.key)
            .ok()?;
        let update = self.updates.get(index)?;
        Some(self.tokens.get(update.token_index)?.next())
    }
    /// Every sealed evaluation this suffix writes that the original plan did
    /// not stage itself.
    pub(super) fn for_each_new_sealed(
        &self,
        f: &mut dyn FnMut(EvaluationKey, validation::EvaluationState) -> Result<(), NativeError>,
    ) -> Result<(), NativeError> {
        for update in &self.updates {
            if update.original_extra.is_some() {
                continue;
            }
            let token = self
                .tokens
                .get(update.token_index)
                .ok_or(ContractError::InvalidManifest)?;
            f(update.key, token.next())?;
        }
        Ok(())
    }
}

fn levels(count: usize) -> Result<usize, NativeError> {
    usize::try_from(
        usize::BITS
            .checked_sub(count.leading_zeros())
            .ok_or(ContractError::Capacity)?,
    )
    .map(|levels| levels.max(1))
    .map_err(|_| NativeError::Capacity("cohort seal search"))
}

fn policy_visits(claim: &ClaimState) -> Result<usize, NativeError> {
    add(
        claim
            .acceptance()
            .declarations()
            .len()
            .checked_mul(2)
            .ok_or(NativeError::Capacity("cohort policy visits"))?,
        add(claim.acceptance().slot_count(), 1)?,
    )
}

fn selected(original: &OriginalPlan<'_>, claim: &ClaimState) -> Result<bool, NativeError> {
    let old = original
        .source()
        .claim(ClaimId(claim.binding().object.0))
        .and_then(ClaimState::local_sealed_at);
    match (old, claim.local_sealed_at()) {
        (Some(before), Some(after)) if before == after => Ok(false),
        (Some(_), _) => Err(ContractError::InvalidCut.into()),
        (None, None) => Ok(false),
        (None, Some(after)) if after == original.outcome().sequence => Ok(true),
        _ => Err(ContractError::InvalidCut.into()),
    }
}

fn registry<'a>(
    original: &'a OriginalPlan<'_>,
    claim: ClaimId,
    visits: &mut Visits,
) -> Result<&'a RegistrationSet, NativeError> {
    visits.take(levels(original.plan().registry.len())?)?;
    if let Some(registry) = original.plan().registry.get(claim) {
        return Ok(registry);
    }
    original
        .source()
        .owned_claim(claim)?
        .registrations()
        .ok_or(ContractError::InvalidTarget.into())
}

fn staged<'a>(
    original: &'a OriginalPlan<'_>,
    key: Key,
    visits: &mut Visits,
) -> Result<Option<(usize, &'a Row)>, NativeError> {
    let mut found = None;
    visits.take(1)?;
    for (index, extra) in original.extras().rows.iter().enumerate() {
        visits.take(1)?;
        if extra.key == key && found.replace((index, &extra.row)).is_some() {
            return Err(ContractError::InvalidTarget.into());
        }
    }
    Ok(found)
}

struct CheckedSeal {
    key: EvaluationKey,
    transition: SealTransition,
    original_extra: Option<usize>,
    attempt: Option<Attempt>,
}

fn member(
    original: &OriginalPlan<'_>,
    claim: &ClaimState,
    registered: RegisteredEvaluation,
    visits: &mut Visits,
) -> Result<CheckedSeal, NativeError> {
    let id = ClaimId(claim.binding().object.0);
    let key = transactions::key_for_registered(id, registered);
    let previous = staged(original, Key::Evaluation(key), visits)?;
    let (original_extra, state) = match previous {
        Some((index, Row::Evaluation(row))) => {
            (Some(index), row.get().ok_or(ContractError::InvalidTarget)?)
        }
        Some(_) => return Err(ContractError::InvalidTarget.into()),
        None => (None, original.source().evaluation(key)?),
    };
    let definition = match staged(original, Key::Definition(key.validation), visits)? {
        Some((_, Row::Definition(row))) => row.get().ok_or(ContractError::InvalidTarget)?,
        Some(_) => return Err(ContractError::InvalidTarget.into()),
        None => original.source().definition(key.validation)?,
    };
    // seal_claim checks the full immutable policy and declaration, while bind,
    // registered state and the current attempt compare fixed-size facts.
    visits.take(add(
        add(
            policy_visits(claim)?,
            claim.acceptance().declarations().len(),
        )?,
        4,
    )?)?;
    registered.check_state(*state, definition)?;
    let transition = state.seal_claim(definition, &state.binding(), claim)?;
    let next = transition.next();
    let attempt = if transition.changed() && next.has_begun() {
        Some(next.bind(definition)?.current_attempt()?)
    } else {
        None
    };
    Ok(CheckedSeal {
        key,
        transition,
        original_extra,
        attempt,
    })
}

fn reserve<T>(count: usize) -> Result<Vec<T>, NativeError> {
    let mut rows = Vec::new();
    rows.try_reserve_exact(count)
        .map_err(|_| MemoryError::AllocationFailed)?;
    within(array::<T>(rows.capacity())?, array::<T>(count)?)?;
    Ok(rows)
}

impl CohortSeals {
    pub(super) fn prepare(
        original: &OriginalPlan<'_>,
        scratch: &mut Scratch,
    ) -> Result<Self, NativeError> {
        let limits = original.limits();
        let mut visits = Visits {
            remaining: limits.plan_edges,
        };
        let mut profile = CohortBudget::empty();
        let mut changed = 0usize;
        let mut additional_evaluations = 0usize;
        let mut registry_count = 0usize;
        let mut registry_bytes = 0usize;
        for claim in &original.plan().rows {
            visits.take(1)?;
            if !selected(original, claim)? {
                continue;
            }
            let id = ClaimId(claim.binding().object.0);
            let registry = registry(original, id, &mut visits)?;
            // A full registry seal is published with the original local cut.
            // Seeing it for a newly sealed claim contradicts this writer's
            // original-prefix contract; never treat it as an unpriced cohort.
            if registry.is_sealed() {
                return Err(ContractError::InvalidTransition.into());
            }
            visits.take(policy_visits(claim)?)?;
            let heap = transactions::registry_heap(registry)?;
            profile = profile.add_claim(claim, registry, registry.rows().len(), heap)?;
            if !registry.is_sealed() {
                registry_count = add(registry_count, 1)?;
                registry_bytes = add(registry_bytes, heap)?;
            }
            for registered in registry.rows() {
                visits.take(1)?;
                let checked = member(original, claim, *registered, &mut visits)?;
                if checked.transition.changed() {
                    changed = add(changed, 1)?;
                    additional_evaluations = add(
                        additional_evaluations,
                        usize::from(checked.original_extra.is_none()),
                    )?;
                }
            }
        }
        let quoted_visits =
            profile.writer_visits(original.plan().rows.len(), original.extras().rows.len())?;
        if quoted_visits > limits.plan_edges {
            return Err(NativeError::Capacity("cohort seal visits"));
        }
        let consumed = limits
            .plan_edges
            .checked_sub(visits.remaining)
            .ok_or(NativeError::Capacity("cohort seal visits"))?;
        visits.remaining = quoted_visits
            .checked_sub(consumed)
            .ok_or(NativeError::Capacity("cohort seal visit quote"))?;
        let bytes = add(
            add(
                array::<SealTransition>(changed)?,
                array::<SealUpdate>(changed)?,
            )?,
            add(
                array::<(ClaimId, RegistrationSet)>(registry_count)?,
                add(
                    registry_bytes,
                    changed
                        .checked_mul(OwnedEvaluation::container_charge())
                        .ok_or(NativeError::Capacity("cohort evaluation containers"))?,
                )?,
            )?,
        )?;
        within(bytes, profile.construction_bytes()?)?;
        let events = add(changed, registry_count)?;
        let final_events = add(
            usize::try_from(original.outcome().events)
                .map_err(|_| NativeError::Capacity("cohort events"))?,
            events,
        )?;
        u32::try_from(final_events).map_err(|_| NativeError::Capacity("cohort events"))?;
        original
            .outcome()
            .evaluations
            .checked_add(
                u32::try_from(additional_evaluations)
                    .map_err(|_| NativeError::Capacity("cohort evaluations"))?,
            )
            .ok_or(NativeError::Capacity("cohort evaluations"))?;
        if add(original.meta().events, events)? > limits.events {
            return Err(NativeError::Capacity("events"));
        }
        let changes = add(
            add(original.plan().rows.len(), original.extras().rows.len())?,
            add(add(final_events, additional_evaluations)?, 2)?,
        )?;
        within(changes, limits.range.max_batch_entries)?;
        let mut charged = Scratch {
            used: scratch.used,
            max: scratch.max,
        };
        charged.charge(bytes)?;
        let mut suffix = Self {
            tokens: reserve(changed)?,
            updates: reserve(changed)?,
            registries: reserve(registry_count)?,
            additional_evaluations,
            visits,
        };
        for claim in &original.plan().rows {
            suffix.visits.take(1)?;
            if !selected(original, claim)? {
                continue;
            }
            let id = ClaimId(claim.binding().object.0);
            let registry = registry(original, id, &mut suffix.visits)?;
            suffix.visits.take(policy_visits(claim)?)?;
            if !registry.is_sealed() {
                suffix.visits.take(registry.rows().len())?;
                let heap = transactions::registry_heap(registry)?;
                let mut copied = registry.try_copy(registry.retained_bytes()?)?;
                within(transactions::registry_heap(&copied)?, heap)?;
                // This is the build pass's immutable registry/policy check.
                copied.seal_targets(claim)?;
                if suffix.registries.len() == suffix.registries.capacity() {
                    return Err(NativeError::Capacity("cohort registry replacements"));
                }
                suffix.registries.push((id, copied));
            } else {
                registry.check(claim)?;
            }
            for registered in registry.rows() {
                suffix.visits.take(1)?;
                let checked = member(original, claim, *registered, &mut suffix.visits)?;
                if checked.transition.changed() {
                    if suffix.tokens.len() == suffix.tokens.capacity()
                        || suffix.updates.len() == suffix.updates.capacity()
                    {
                        return Err(NativeError::Capacity("cohort seal transitions"));
                    }
                    suffix.updates.push(SealUpdate {
                        key: checked.key,
                        token_index: suffix.tokens.len(),
                        original_extra: checked.original_extra,
                        attempt: checked.attempt,
                    });
                    suffix.tokens.push(checked.transition);
                }
            }
        }
        if suffix.tokens.len() != changed || suffix.registries.len() != registry_count {
            return Err(ContractError::InvalidManifest.into());
        }
        suffix.canonicalize()?;
        *scratch = charged;
        Ok(suffix)
    }

    pub(super) fn events(&self) -> Result<usize, NativeError> {
        add(self.tokens.len(), self.registries.len())
    }

    pub(super) fn is_empty(&self) -> bool {
        self.tokens.is_empty() && self.registries.is_empty()
    }

    pub(super) fn additional_evaluations(&self) -> usize {
        self.additional_evaluations
    }

    pub(super) fn emit(
        &mut self,
        original: &OriginalPlan<'_>,
        mut emit: impl FnMut(NativeFact) -> Result<(), NativeError>,
    ) -> Result<(), NativeError> {
        for update in &self.updates {
            self.visits.take(1)?;
            let token = self
                .tokens
                .get(update.token_index)
                .ok_or(ContractError::InvalidManifest)?;
            let next = token.next();
            emit(NativeFact::Evaluation {
                kind: NativeEvaluationEventKind::Sealed,
                key: update.key,
                before: Some(token.before()),
                after: next.binding(),
                state: next.state(),
                phase: next.phase(),
                attempt: update.attempt,
                fence: next.fence(),
            })?;
        }
        let mut registries = self.registries.iter().peekable();
        for claim in &original.plan().rows {
            self.visits.take(1)?;
            if registries
                .peek()
                .is_some_and(|(id, _)| id.0 == claim.binding().object.0)
            {
                self.visits.take(1)?;
                emit(NativeFact::Registrations {
                    claim: claim.binding(),
                })?;
                registries.next();
            }
        }
        if registries.next().is_some() {
            return Err(ContractError::InvalidTarget.into());
        }
        Ok(())
    }

    /// Only the immutable-plan publisher calls this, after original and suffix
    /// history have both been emitted successfully. Each final key moves once.
    #[allow(clippy::too_many_arguments)] // Already checked detached publication payloads.
    pub(super) fn merge(
        mut self,
        plan: transactions::Plan,
        extras: Extras,
        source: &View<'_>,
        limits: NativeLimits,
        scratch: &mut Scratch,
        changes: &mut Vec<Change<Key, Row>>,
    ) -> Result<Vec<SealTransition>, NativeError> {
        let mut original_registries = plan.registry.into_iter().peekable();
        let mut sealed_registries = self.registries.into_iter().peekable();
        for claim in plan.rows {
            self.visits.take(1)?;
            let id = ClaimId(claim.binding().object.0);
            let original = if original_registries
                .peek()
                .is_some_and(|(owner, _)| *owner == id)
            {
                Some(
                    original_registries
                        .next()
                        .ok_or(ContractError::InvalidTarget)?
                        .1,
                )
            } else {
                None
            };
            let registrations = if sealed_registries
                .peek()
                .is_some_and(|(owner, _)| *owner == id)
            {
                sealed_registries
                    .next()
                    .ok_or(ContractError::InvalidTarget)?
                    .1
            } else if let Some(original) = original {
                original
            } else {
                transactions::copy_registry(source, &claim, limits, scratch)?
            };
            let owned = OwnedClaim::new(claim, registrations)?;
            let heap = owned.heap_charge()?;
            put(changes, Entry::new(Key::Claim(id), Row::Claim(owned), heap))?;
        }
        if original_registries.next().is_some() || sealed_registries.next().is_some() {
            return Err(ContractError::InvalidTarget.into());
        }
        for (index, extra) in extras.rows.into_iter().enumerate() {
            self.visits.take(1)?;
            if let Key::Evaluation(key) = extra.key
                && let Some(update) = find(&self.updates, key, &mut self.visits)?
            {
                if update.original_extra != Some(index) {
                    return Err(ContractError::InvalidTarget.into());
                }
                // The original event was emitted against this exact row. Only
                // now drop the old container and install the checked sealed row.
                continue;
            }
            put(changes, Entry::new(extra.key, extra.row, extra.heap))?;
        }
        for update in self.updates {
            self.visits.take(1)?;
            let next = self
                .tokens
                .get(update.token_index)
                .ok_or(ContractError::InvalidManifest)?
                .next();
            let owned = OwnedEvaluation::new(next)?;
            let heap = owned.heap_charge()?;
            within(heap, OwnedEvaluation::container_charge())?;
            put(
                changes,
                Entry::new(Key::Evaluation(update.key), Row::Evaluation(owned), heap),
            )?;
        }
        Ok(self.tokens)
    }
}

fn put(changes: &mut Vec<Change<Key, Row>>, entry: Entry<Key, Row>) -> Result<(), NativeError> {
    if changes.len() == changes.capacity() {
        return Err(NativeError::Capacity("cohort final changes"));
    }
    changes.push(Change::Put(entry));
    Ok(())
}

fn find<'a>(
    rows: &'a [SealUpdate],
    key: EvaluationKey,
    visits: &mut Visits,
) -> Result<Option<&'a SealUpdate>, NativeError> {
    let mut left = 0usize;
    let mut right = rows.len();
    while left < right {
        visits.take(1)?;
        let middle = add(
            left,
            right
                .checked_sub(left)
                .and_then(|n| n.checked_div(2))
                .ok_or(ContractError::Capacity)?,
        )?;
        let row = rows.get(middle).ok_or(ContractError::Capacity)?;
        match row.key.cmp(&key) {
            std::cmp::Ordering::Less => left = add(middle, 1)?,
            std::cmp::Ordering::Equal => return Ok(Some(row)),
            std::cmp::Ordering::Greater => right = middle,
        }
    }
    Ok(None)
}

#[path = "cohort_seals_sort.rs"]
mod sorting;

#[cfg(test)]
#[path = "cohort_seals_tests.rs"]
mod tests;
