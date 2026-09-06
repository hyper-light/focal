use super::*;
use focal_memory::{BudgetKind, BudgetLane, Change};
use focal_model::lifecycle::claim::ClaimCut;

#[cfg(test)]
#[path = "staging_tests.rs"]
mod staging_tests;

pub(super) const ALLOCATION: usize = 4 * size_of::<usize>();

pub(super) fn add(a: usize, b: usize) -> Result<usize, NativeError> {
    a.checked_add(b).ok_or(NativeError::Capacity("byte charge"))
}
pub(super) fn array<T>(count: usize) -> Result<usize, NativeError> {
    add(
        count
            .checked_mul(size_of::<T>())
            .ok_or(NativeError::Capacity("byte charge"))?,
        if count == 0 { 0 } else { ALLOCATION },
    )
}
pub(super) fn containers(count: usize) -> Result<usize, NativeError> {
    count
        .checked_mul(OwnedClaim::container_charge())
        .ok_or(NativeError::Capacity("claim containers"))
}
pub(super) fn event_containers(count: usize) -> Result<usize, NativeError> {
    count
        .checked_mul(OwnedEvent::container_charge())
        .ok_or(NativeError::Capacity("event containers"))
}
pub(super) fn heap(row: &ClaimState) -> Result<usize, NativeError> {
    add(
        row.retained_heap_bytes()?,
        row.heap_allocations()?
            .checked_mul(ALLOCATION)
            .ok_or(NativeError::Capacity("allocator charge"))?,
    )
}
pub(super) fn within(bytes: usize, max: usize) -> Result<(), NativeError> {
    if bytes > max {
        Err(NativeError::Capacity("preparation bytes"))
    } else {
        Ok(())
    }
}

/// Only retained neighboring rows in a touched page reach this copier. Storage
/// already owns their full precharge. A larger actual copy is refused, never
/// silently admitted or charged after publication.
fn copy(row: &Row) -> Result<Row, MemoryError> {
    match row {
        Row::Meta(meta) => Ok(Row::Meta(*meta)),
        Row::Outcome(outcome) => Ok(Row::Outcome(*outcome)),
        Row::Event(event) => {
            #[cfg(test)]
            copy_failure()?;
            event.copy().map(Row::Event)
        }
        Row::Definition(row) => {
            #[cfg(test)]
            copy_failure()?;
            row.copy().map(Row::Definition)
        }
        Row::Evaluation(row) => {
            #[cfg(test)]
            copy_failure()?;
            row.copy().map(Row::Evaluation)
        }
        Row::ArtifactIdentity(id) => Ok(Row::ArtifactIdentity(*id)),
        Row::Artifact(row) => {
            #[cfg(test)]
            copy_failure()?;
            row.copy().map(Row::Artifact)
        }
        Row::Accepted(row) => {
            #[cfg(test)]
            copy_failure()?;
            row.copy().map(Row::Accepted)
        }
        Row::Claim(claim) => {
            #[cfg(test)]
            copy_failure()?;
            claim.copy().map(Row::Claim)
        }
    }
}

#[cfg(test)]
std::thread_local! {
    static COPIES_REMAINING: std::cell::Cell<Option<usize>> = const { std::cell::Cell::new(None) };
}
#[cfg(test)]
fn copy_failure() -> Result<(), MemoryError> {
    COPIES_REMAINING.with(|remaining| match remaining.get() {
        Some(0) => Err(MemoryError::AllocationFailed),
        Some(count) => {
            remaining.set(Some(count - 1));
            Ok(())
        }
        None => Ok(()),
    })
}
#[cfg(test)]
pub(super) fn fail_copies_after<T>(count: usize, action: impl FnOnce() -> T) -> T {
    struct Restore(Option<usize>);
    impl Drop for Restore {
        fn drop(&mut self) {
            COPIES_REMAINING.with(|state| state.set(self.0));
        }
    }
    let _restore = Restore(COPIES_REMAINING.with(|state| state.replace(Some(count))));
    action()
}

/// Temporary native heaps share one pre-reserved allowance. Every allocation is
/// preflighted; allocator-reported capacities are reconciled before retention.
pub(super) struct Scratch {
    pub used: usize,
    pub max: usize,
}
impl Scratch {
    pub fn charge(&mut self, bytes: usize) -> Result<(), NativeError> {
        let next = add(self.used, bytes)?;
        within(next, self.max)?;
        self.used = next;
        Ok(())
    }
    pub fn remaining(&self) -> Result<usize, NativeError> {
        self.max
            .checked_sub(self.used)
            .ok_or(NativeError::Capacity("preparation bytes"))
    }
    pub fn reserve<T>(&mut self, count: usize) -> Result<Vec<T>, NativeError> {
        self.charge(array::<T>(count)?)?;
        let mut rows = Vec::new();
        rows.try_reserve_exact(count)
            .map_err(|_| MemoryError::AllocationFailed)?;
        self.charge(
            array::<T>(rows.capacity())?
                .checked_sub(array::<T>(count)?)
                .ok_or(NativeError::Capacity("allocator capacity"))?,
        )?;
        Ok(rows)
    }
}

/// Non-claim rows and their exact history are staged together. The vector is
/// charged separately from nested payloads, which consume Scratch.
pub(super) struct Extra {
    pub key: Key,
    pub row: Row,
    pub heap: usize,
    pub fact: Option<NativeFact>,
}
pub(super) struct Extras {
    pub rows: Vec<Extra>,
    max: usize,
    allowance: usize,
}
impl Extras {
    fn new(max: usize, allowance: usize) -> Result<Self, NativeError> {
        within(array::<Extra>(max)?, allowance)?;
        Ok(Self {
            rows: Vec::new(),
            max,
            allowance,
        })
    }
    pub fn events(&self) -> usize { self.rows.iter().filter(|row| row.fact.is_some()).count() }
    pub fn push(&mut self, row: Extra) -> Result<(), NativeError> {
        if self.rows.len() == self.max {
            return Err(NativeError::Capacity("native extra rows"));
        }
        if self.rows.iter().any(|old| old.key == row.key) {
            return Err(ContractError::InvalidTarget.into());
        }
        if self.rows.len() == self.rows.capacity() {
            let capacity = self
                .rows
                .capacity()
                .checked_mul(2)
                .unwrap_or(self.max)
                .max(1)
                .min(self.max);
            let old = array::<Extra>(self.rows.capacity())?;
            within(add(old, array::<Extra>(capacity)?)?, self.allowance)?;
            let mut replacement = Vec::new();
            replacement
                .try_reserve_exact(capacity)
                .map_err(|_| MemoryError::AllocationFailed)?;
            within(
                add(old, array::<Extra>(replacement.capacity())?)?,
                self.allowance,
            )?;
            replacement.append(&mut self.rows);
            self.rows = replacement;
        }
        self.rows.push(row);
        Ok(())
    }
    pub fn evaluation(
        &mut self,
        claim: ClaimId,
        declaration: &validation::Declaration,
        before: Option<Binding>,
        state: validation::EvaluationState,
        scratch: &mut Scratch,
    ) -> Result<(), NativeError> {
        let evaluation = state.bind(declaration)?;
        let fact = NativeFact::Evaluation {
            kind: if before.is_none() {
                NativeEvaluationEventKind::Materialized
            } else if state.fence().is_some() {
                NativeEvaluationEventKind::AuthorityFenced
            } else {
                NativeEvaluationEventKind::Begun
            },
            key: EvaluationKey::of(claim, &state),
            before,
            after: state.binding(),
            state: state.state(),
            phase: state.phase(),
            attempt: if state.has_begun() {
                Some(evaluation.current_attempt()?)
            } else {
                None
            },
            fence: state.fence(),
        };
        scratch.charge(OwnedEvaluation::container_charge())?;
        let row = OwnedEvaluation::new(state)?;
        let heap = row.heap_charge()?;
        self.push(Extra {
            key: Key::Evaluation(EvaluationKey::of(claim, &state)),
            row: Row::Evaluation(row),
            heap,
            fact: Some(fact),
        })
    }
}

impl Core<NativeState> {
    /// The full pending chain proves the source of every read and absent ID.
    /// Clock/identity are owner facts; participant intent cannot supply readiness
    /// or an evaluator grant. Retry returns the original accepted logical time.
    pub fn prepare_native(
        &self,
        context: NativeContext,
        input: NativeInput,
        pending: &[&NativePrepared],
    ) -> Result<NativePreparation, NativeError> {
        self.prepare_native_evidenced(context, input, pending, None)
    }

    /// A fresh report requires an actual verified local custody capability. An
    /// exact retry resolves its retained outcome before requiring evidence again.
    /// Native distributed ingress must additionally qualify installed placement.
    pub fn prepare_native_evidenced(
        &self,
        context: NativeContext,
        input: NativeInput,
        pending: &[&NativePrepared],
        evidence: Option<&focal_evidence::VerifiedNativeArtifact>,
    ) -> Result<NativePreparation, NativeError> {
        self.validate_native_chain(pending)?;
        context.principal.require_actor(input.request.principal)?;
        if input.request.principal.is_zero()
            || input.request.id.is_zero()
            || input.request.epoch.0 == 0
        {
            return Err(ContractError::InvalidTarget.into());
        }
        intent::bound_input(&input.command, self.limits)?;
        let intent = intent::fingerprint(self.state.ledger, &input)?;
        let view = View {
            state: &self.state,
            tail: pending.last().copied(),
        };
        if let Some(outcome) = as_outcome(view.get(Key::Outcome(input.request))) {
            return if outcome.intent == intent {
                Ok(NativePreparation::Existing {
                    outcome,
                    committed: outcome.sequence <= self.native_sequence(),
                })
            } else {
                Err(NativeError::RequestConflict)
            };
        }
        if pending.len() == self.limits.pending {
            return Err(NativeError::Capacity("pending candidates"));
        }
        let mut meta = view.meta();
        if context.logical_time < meta.logical_time {
            return Err(ContractError::InvalidCut.into());
        }
        meta.logical_time = context.logical_time;
        meta.outcomes = add(meta.outcomes, 1)?;
        if meta.outcomes > self.limits.outcomes {
            return Err(NativeError::Capacity("outcomes"));
        }
        let sequence = SessionSeq(
            view.prefix()
                .0
                .checked_add(1)
                .ok_or(NativeError::Capacity("sequence"))?,
        );
        let cut = ClaimCut {
            position: sequence,
            cause: intent,
        };
        let operation = match &input.command {
            NativeCommand::Create { .. } => NativeOperation::Create,
            NativeCommand::Cancel { .. } => NativeOperation::Cancel,
            NativeCommand::Post { .. } => NativeOperation::Post,
            NativeCommand::BeginAdmission { .. } => NativeOperation::BeginAdmission,
            NativeCommand::ReportAdmission { .. } => NativeOperation::ReportAdmission,
        };
        let lane = if matches!(operation, NativeOperation::Cancel | NativeOperation::ReportAdmission) {
            BudgetLane::Completion
        } else {
            BudgetLane::Ordinary
        };
        let extra_maximum = array::<Extra>(self.limits.range.max_batch_entries / 2)?;
        let extra_allowance = add(extra_maximum, extra_maximum)?;
        let changes_charge = add(
            add(
                array::<Change<Key, Row>>(self.limits.range.max_batch_entries)?,
                array::<claim_changes::History>(self.limits.plan_nodes)?,
            )?,
            add(
                containers(self.limits.plan_nodes)?,
                event_containers(self.limits.range.max_batch_entries)?,
            )?,
        )?;
        let reservation = self.state.budget.reserve(
            BudgetKind::Pending,
            lane,
            add(
                add(self.limits.preparation_bytes, changes_charge)?,
                extra_allowance,
            )?,
        )?;
        let mut scratch = Scratch {
            used: 0,
            max: self.limits.preparation_bytes,
        };
        let mut extras = Extras::new(self.limits.range.max_batch_entries / 2, extra_allowance)?;
        let plan = transactions::prepare(
            input.command,
            input.request,
            evidence,
            context,
            cut,
            &view,
            self.limits,
            &mut meta,
            &mut extras,
            &mut scratch,
        )?;
        let definitions = extras
            .rows
            .iter()
            .filter(|row| matches!(row.key, Key::Definition(_)))
            .count();
        let evaluations = extras.rows.iter().filter(|row| matches!(row.key, Key::Evaluation(_))).count();
        let artifacts = extras.rows.iter().filter(|row| matches!(row.key, Key::Artifact(_))).count();
        let results = extras.rows.iter().filter(|row| matches!(row.key, Key::Accepted(_))).count();
        let outcome = NativeOutcome {
            ledger: view.ledger(),
            request: input.request,
            sequence,
            logical_time: context.logical_time,
            operation,
            intent,
            created: u32::try_from(plan.created)
                .map_err(|_| NativeError::Capacity("created count"))?,
            changed: u32::try_from(plan.rows.len())
                .map_err(|_| NativeError::Capacity("changed count"))?,
            definitions: u32::try_from(definitions)
                .map_err(|_| NativeError::Capacity("definitions count"))?,
            evaluations: u32::try_from(evaluations)
                .map_err(|_| NativeError::Capacity("evaluations count"))?,
            artifacts: u32::try_from(artifacts).map_err(|_| NativeError::Capacity("artifacts count"))?,
            results: u32::try_from(results).map_err(|_| NativeError::Capacity("results count"))?,
            events: u32::try_from(add(
                claim_changes::event_count(&plan.rows, &view)?,
                extras.events(),
            )?)
            .map_err(|_| NativeError::Capacity("event count"))?,
        };
        let changes = claim_changes::changes(
            plan,
            extras,
            meta,
            outcome,
            &view,
            self.limits,
            changes_charge,
            &mut scratch,
        )?;
        let range = match view.tail {
            Some(tail) => {
                self.state
                    .rows
                    .prepare_after_with(&tail.range, sequence.0, changes, lane, copy)?
            }
            None => self
                .state
                .rows
                .prepare_batch_with(sequence.0, changes, lane, copy)?,
        };
        drop(reservation);
        Ok(NativePreparation::Prepared(NativePrepared {
            range,
            outcome,
        }))
    }
}
