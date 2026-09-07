use super::*;
#[cfg(test)]
use focal_memory::Change;
use focal_memory::{BudgetKind, BudgetLane};
use focal_model::lifecycle::claim::ClaimCut;

#[cfg(test)]
#[path = "staging_tests.rs"]
mod staging_tests;

#[cfg(test)]
#[path = "admission_gate_tests.rs"]
mod admission_gate_tests;

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
        Row::IncomingHead(row) => Ok(Row::IncomingHead(*row)),
        Row::IncomingLink(row) => Ok(Row::IncomingLink(*row)),
        Row::MissingResult(row) => row.copy().map(Row::MissingResult),
        Row::Meta(meta) => Ok(Row::Meta(*meta)),
        Row::Receipt(receipt) => Ok(Row::Receipt(*receipt)),
        Row::Cycle(cycle) => Ok(Row::Cycle(*cycle)),
        Row::WorkSlot(id) => Ok(Row::WorkSlot(*id)),
        Row::Work(row) => row.copy().map(Row::Work),
        Row::Diagnostic(row) => row.copy().map(Row::Diagnostic),
        Row::Response(row) => row.copy().map(Row::Response),
        Row::DeliveryResult(row) => row.copy().map(Row::DeliveryResult),
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
    pub journal: Option<Vec<NativeFact>>,
    max: usize,
    allowance: usize,
}
impl Extras {
    pub(super) fn new(max: usize, allowance: usize) -> Result<Self, NativeError> {
        within(array::<Extra>(max)?, allowance)?;
        Ok(Self {
            rows: Vec::new(),
            journal: None,
            max,
            allowance,
        })
    }
    pub fn events(&self) -> usize {
        match &self.journal {
            Some(events) => events.len(),
            None => self.rows.iter().filter(|row| row.fact.is_some()).count(),
        }
    }
    pub fn begin_journal(
        &mut self,
        capacity: usize,
        scratch: &mut Scratch,
    ) -> Result<(), NativeError> {
        if self.journal.is_some() || !self.rows.is_empty() {
            return Err(ContractError::InvalidTransition.into());
        }
        self.journal = Some(scratch.reserve(capacity)?);
        Ok(())
    }
    pub fn record(&mut self, fact: NativeFact) -> Result<(), NativeError> {
        let events = self
            .journal
            .as_mut()
            .ok_or(ContractError::InvalidTransition)?;
        if events.len() == events.capacity() {
            return Err(NativeError::Capacity("transaction history"));
        }
        events.push(fact);
        Ok(())
    }
    pub fn replace(&mut self, mut row: Extra) -> Result<(), NativeError> {
        if self.journal.is_none() {
            return Err(ContractError::InvalidTransition.into());
        }
        let index = self
            .rows
            .iter()
            .position(|old| old.key == row.key)
            .ok_or(ContractError::InvalidTarget)?;
        if let Some(fact) = row.fact.take() {
            self.record(fact)?;
        }
        *self
            .rows
            .get_mut(index)
            .ok_or(ContractError::InvalidTarget)? = row;
        Ok(())
    }
    pub fn push(&mut self, mut row: Extra) -> Result<(), NativeError> {
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
        if self.journal.is_some()
            && let Some(fact) = row.fact.take()
        {
            self.record(fact)?;
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
        self.prepare_native_in(&self.state.budget, context, input, pending, evidence)
    }

    /// Trusted owner allocation source, either the owner's budget or one of its
    /// descendants. A funded child lets planning and retained range pages spend
    /// the same held capacity as custody verification. Temporary failures return
    /// credit to that source; immutable published/pinned pages retain their debit.
    ///
    /// This does not establish an evaluation entitlement, reserve future row
    /// slots, or authorize two pending forks to spend the same promised share.
    /// Participant intent cannot select the source or widen the operation lane.
    pub fn prepare_native_in(
        &self,
        source: &MemoryBudget,
        context: NativeContext,
        input: NativeInput,
        pending: &[&NativePrepared],
        evidence: Option<&focal_evidence::VerifiedNativeArtifact>,
    ) -> Result<NativePreparation, NativeError> {
        self.prepare_native_chain(source, context, input, pending.iter().copied(), evidence)
    }

    /// The managed owner supplies its borrowed queue iterator directly. No
    /// temporary vector or reservation precedes exact retry lookup.
    pub(super) fn prepare_native_chain<'a>(
        &self,
        source: &MemoryBudget,
        context: NativeContext,
        input: NativeInput,
        pending: impl DoubleEndedIterator<Item = &'a NativePrepared> + ExactSizeIterator + Clone,
        evidence: Option<&focal_evidence::VerifiedNativeArtifact>,
    ) -> Result<NativePreparation, NativeError> {
        if !source.is_within(&self.state.budget) {
            return Err(MemoryError::InvalidConfiguration(
                "native allocation source is outside owner budget",
            )
            .into());
        }
        match self.check_native_chain(context, input, pending)? {
            Checked::Existing { outcome, committed } => {
                Ok(NativePreparation::Existing { outcome, committed })
            }
            Checked::Fresh(fresh) => fresh
                .build(source, evidence)
                .map(NativePreparation::Prepared),
        }
    }

    /// Resolve identity and the exact effective base before selecting resource
    /// ownership. This step neither reserves memory nor constructs domain rows.
    /// Its owned input and borrowed base cannot be replaced after checking.
    pub(super) fn check_native_chain<'a, 'p: 'a>(
        &'a self,
        context: NativeContext,
        input: NativeInput,
        mut pending: impl DoubleEndedIterator<Item = &'p NativePrepared> + ExactSizeIterator + Clone,
    ) -> Result<Checked<'a>, NativeError> {
        let pending_count = pending.len();
        if pending_count > self.limits.pending {
            return Err(NativeError::Capacity("pending candidates"));
        }
        self.state
            .rows
            .validate_chain(pending.clone().map(|item| &item.range))?;
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
            tail: pending.next_back(),
        };
        if let Some(outcome) = as_outcome(view.get(Key::Outcome(input.request))) {
            return if outcome.intent == intent {
                Ok(Checked::Existing {
                    outcome,
                    committed: outcome.sequence <= self.native_sequence(),
                })
            } else {
                Err(NativeError::RequestConflict)
            };
        }
        if pending_count == self.limits.pending {
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
            NativeCommand::EnterWholeWork { .. } => NativeOperation::EnterWholeWork,
            NativeCommand::SealIncrementTargets { .. } => NativeOperation::SealIncrementTargets,
            NativeCommand::BeginIncrement { .. } => NativeOperation::BeginIncrement,
            NativeCommand::ReportIncrement { .. } => NativeOperation::ReportIncrement,
            NativeCommand::FailWorkProduction { .. } => NativeOperation::FailWorkProduction,
            NativeCommand::RejectWork { .. } => NativeOperation::RejectWork,
            NativeCommand::SubmitWork { .. } => NativeOperation::SubmitWork,
            NativeCommand::SubmitDiagnostic { .. } => NativeOperation::SubmitDiagnostic,
            NativeCommand::ReceiveWork { .. } => NativeOperation::ReceiveWork,
            NativeCommand::CloseResponse { .. } => NativeOperation::CloseResponse,
            NativeCommand::PostResponse { .. } => NativeOperation::PostResponse,
            NativeCommand::ReceiveResponse { .. } => NativeOperation::ReceiveResponse,
            NativeCommand::Create { .. } => NativeOperation::Create,
            NativeCommand::AcquireReceipt { .. } => NativeOperation::AcquireReceipt,
            NativeCommand::Cancel { .. } => NativeOperation::Cancel,
            NativeCommand::Post { .. } => NativeOperation::Post,
            NativeCommand::BeginAdmission { .. } => NativeOperation::BeginAdmission,
            NativeCommand::ReportAdmission { .. } => NativeOperation::ReportAdmission,
        };
        let lane = if matches!(
            operation,
            NativeOperation::Cancel
                | NativeOperation::ReportAdmission
                | NativeOperation::ReportIncrement
        ) {
            BudgetLane::Completion
        } else {
            BudgetLane::Ordinary
        };
        Ok(Checked::Fresh(Fresh {
            input,
            context,
            view,
            meta,
            sequence,
            cut,
            intent,
            operation,
            lane,
            limits: self.limits,
        }))
    }
}

// The bounded native input stays on the stack. Boxing this capability would
// allocate before the owner can select and reserve its construction source.
#[allow(clippy::large_enum_variant)]
pub(super) enum Checked<'a> {
    Existing {
        outcome: NativeOutcome,
        committed: bool,
    },
    Fresh(Fresh<'a>),
}

/// Private admission capability tied to the actual committed/pending prefix.
/// Keeping it borrowed prevents publication while source selection is pending.
pub(super) struct Fresh<'a> {
    input: NativeInput,
    context: NativeContext,
    view: View<'a>,
    meta: Meta,
    sequence: SessionSeq,
    cut: ClaimCut,
    intent: ContentHash,
    operation: NativeOperation,
    lane: BudgetLane,
    limits: NativeLimits,
}

pub(super) enum Admission<'a> {
    Begin {
        key: EvaluationKey,
        registered: super::admission_authority::Registered<'a>,
        binding: Binding,
        active: bool,
    },
    Report {
        key: EvaluationKey,
        registered: super::admission_authority::Registered<'a>,
        authorization: validation::ReportAuthorization,
        artifact: &'a NativeArtifactInput,
    },
}

impl<'a> Fresh<'a> {
    pub(super) fn authorize_admission(&self) -> Result<Option<Admission<'_>>, NativeError> {
        match &self.input.command {
            NativeCommand::BeginAdmission {
                claim,
                key,
                expected,
            }
            | NativeCommand::BeginIncrement {
                claim,
                key,
                expected,
            } => {
                let begin = if matches!(&self.input.command, NativeCommand::BeginIncrement { .. }) {
                    super::increment_authority::begin
                } else {
                    super::admission_authority::begin
                };
                let begun = begin(
                    &self.view,
                    self.context,
                    *claim,
                    *key,
                    *expected,
                    self.limits,
                )?;
                Ok(Some(Admission::Begin {
                    key: *key,
                    binding: begun.next.binding(),
                    active: begun.next.has_begun(),
                    registered: begun.registered,
                }))
            }
            NativeCommand::ReportAdmission {
                claim,
                key,
                expected,
                report,
                artifact,
            }
            | NativeCommand::ReportIncrement {
                claim,
                key,
                expected,
                report,
                artifact,
            } => {
                let authorize =
                    if matches!(&self.input.command, NativeCommand::ReportIncrement { .. }) {
                        super::increment_authority::report
                    } else {
                        super::admission_authority::report
                    };
                let (registered, authorization) = authorize(
                    &self.view,
                    self.context,
                    *claim,
                    *key,
                    *expected,
                    *report,
                    artifact.get().ok_or(ContractError::MissingEvidence)?,
                    self.limits,
                )?;
                Ok(Some(Admission::Report {
                    key: *key,
                    registered,
                    authorization,
                    artifact,
                }))
            }
            _ => Ok(None),
        }
    }

    pub(super) fn view(&self) -> &View<'_> {
        &self.view
    }
    pub(super) fn limits(&self) -> NativeLimits {
        self.limits
    }
    pub(super) fn input(&self) -> &NativeInput {
        &self.input
    }
    pub(super) fn source(&self) -> &'a MemoryBudget {
        &self.view.state.budget
    }
    pub(super) fn authorize_work(
        &self,
    ) -> Result<Option<&focal_model::lifecycle::artifact_descriptor::ArtifactDescriptor>, NativeError>
    {
        super::work_artifacts::authorize(&self.view, self.context, &self.input.command, self.limits)
    }

    pub(super) fn build(
        self,
        source: &MemoryBudget,
        evidence: Option<&focal_evidence::VerifiedNativeArtifact>,
    ) -> Result<NativePrepared, NativeError> {
        self.build_with_completion(source, evidence, None)
    }

    pub(super) fn build_with_completion(
        self,
        source: &MemoryBudget,
        evidence: Option<&focal_evidence::VerifiedNativeArtifact>,
        completion: Option<&super::completion_envelope::CompletionEnvelope>,
    ) -> Result<NativePrepared, NativeError> {
        let Self {
            input,
            context,
            view,
            mut meta,
            sequence,
            cut,
            intent,
            operation,
            lane,
            limits,
        } = self;
        if !source.is_within(&view.state.budget) {
            return Err(MemoryError::InvalidConfiguration(
                "native allocation source is outside owner budget",
            )
            .into());
        }
        let construction =
            super::prepare_budget::ConstructionBudget::for_operation(operation, limits)?;
        // Bound every possible admitted Begin/Report write against immutable
        // storage limits. The bound is checked against the actual final plan;
        // it is not itself a reservation for future reports.
        let envelope = if matches!(
            operation,
            NativeOperation::BeginAdmission
                | NativeOperation::ReportAdmission
                | NativeOperation::BeginIncrement
                | NativeOperation::ReportIncrement
        ) {
            Some(
                view.state
                    .rows
                    .future_write_envelope(focal_memory::RangeWriteLimits {
                        changed_keys: construction.max_changes,
                        deleted_keys: 0,
                        incoming_heap: add(
                            construction.scratch_bytes,
                            add(
                                containers(construction.max_claim_rows)?,
                                event_containers(construction.max_events)?,
                            )?,
                        )?,
                        input_capacity: construction.max_changes,
                    })?,
            )
        } else {
            None
        };
        let reservation =
            source.reserve(BudgetKind::Pending, lane, construction.pending_bytes()?)?;
        let mut scratch = Scratch {
            used: 0,
            max: construction.scratch_bytes,
        };
        let mut extras = Extras::new(construction.extras_count, construction.extras_bytes)?;
        let plan = transactions::prepare(
            input.command,
            input.request,
            evidence,
            context,
            cut,
            &view,
            limits,
            &mut meta,
            &mut extras,
            &mut scratch,
        )?;
        let definitions = extras
            .rows
            .iter()
            .filter(|row| matches!(row.key, Key::Definition(_)))
            .count();
        let evaluations = extras
            .rows
            .iter()
            .filter(|row| matches!(row.key, Key::Evaluation(_)))
            .count();
        let artifacts = extras
            .rows
            .iter()
            .filter(|row| matches!(row.key, Key::Artifact(_)))
            .count();
        let results = extras
            .rows
            .iter()
            .filter(|row| {
                matches!(
                    row.key,
                    Key::Accepted(_) | Key::DeliveryResult(_) | Key::MissingResult(_)
                )
            })
            .count();
        let receipts = extras
            .rows
            .iter()
            .filter(|row| matches!(row.key, Key::Receipt(_)))
            .count();
        let responses = extras
            .rows
            .iter()
            .filter(|row| matches!(row.key, Key::Response(_)))
            .count();
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
            artifacts: u32::try_from(artifacts)
                .map_err(|_| NativeError::Capacity("artifacts count"))?,
            results: u32::try_from(results).map_err(|_| NativeError::Capacity("results count"))?,
            receipts: u32::try_from(receipts)
                .map_err(|_| NativeError::Capacity("receipt count"))?,
            responses: u32::try_from(responses)
                .map_err(|_| NativeError::Capacity("response count"))?,
            events: u32::try_from(add(
                claim_changes::event_count(&plan.rows, &view, operation)?,
                extras.events(),
            )?)
            .map_err(|_| NativeError::Capacity("event count"))?,
        };
        construction.check_counts(
            plan.rows.len(),
            extras.rows.len(),
            usize::try_from(outcome.events).map_err(|_| NativeError::Capacity("events"))?,
        )?;
        transactions::increment(
            &mut meta.events,
            usize::try_from(outcome.events).map_err(|_| NativeError::Capacity("events"))?,
            limits.events,
            "events",
        )?;
        let changes = claim_changes::changes(
            plan,
            extras,
            meta,
            outcome,
            &view,
            limits,
            construction.changes_bytes,
            &mut scratch,
        )?;
        let range_plan = match view.tail {
            Some(tail) => {
                view.state
                    .rows
                    .plan_after(&tail.range, sequence.0, changes, lane, usize::MAX)?
            }
            None => view
                .state
                .rows
                .plan_batch(sequence.0, changes, lane, usize::MAX)?,
        };
        if let Some(envelope) = envelope {
            envelope.check_plan(&range_plan)?;
        }
        if let Some(completion) = completion {
            completion
                .report_storage(outcome.changed != 0)?
                .check_plan(&range_plan)?;
        }
        let range = range_plan.build_in_with(source, copy)?;
        drop(reservation);
        Ok(NativePrepared { range, outcome })
    }
}
