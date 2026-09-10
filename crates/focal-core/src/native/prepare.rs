use super::*;
#[cfg(test)]
use focal_memory::Change;
use focal_memory::{Allocation, BudgetKind, BudgetLane};
use focal_model::lifecycle::claim::ClaimCut;

#[path = "monitor_ingress.rs"]
mod monitor_ingress;

#[cfg(test)]
#[path = "staging_tests.rs"]
mod staging_tests;

#[cfg(test)]
#[path = "admission_gate_tests.rs"]
mod admission_gate_tests;

#[cfg(test)]
#[path = "prepare_begin_tests.rs"]
mod begin_tests;

impl View<'_> {
    /// Tie a candidate to this exact immutable source root. The comparison
    /// borrows existing handles; it does not copy roots or reserve resources.
    pub(super) fn check_successor(&self, prepared: &NativePrepared) -> Result<(), NativeError> {
        match self.tail {
            Some(previous) => previous.fragments.validate_successor(&prepared.fragments)?,
            None => self
                .state
                .rows
                .validate_chain(std::iter::once(&prepared.fragments))?,
        }
        Ok(())
    }
}

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
pub(super) fn copy(row: &Row) -> Result<Row, MemoryError> {
    match row {
        Row::ClaimIdentity(id) => Ok(Row::ClaimIdentity(*id)),
        Row::DefinitionIdentity(id) => Ok(Row::DefinitionIdentity(*id)),
        Row::LegacyTestament(row) => row.copy().map(Row::LegacyTestament),
        Row::LegacyEvidenceSet(row) => row.copy().map(Row::LegacyEvidenceSet),
        Row::LegacyRun(row) => row.copy().map(Row::LegacyRun),
        Row::LegacyDefinition(row) => row.copy().map(Row::LegacyDefinition),
        Row::ClaimContent(row) => {
            #[cfg(test)]
            copy_failure()?;
            row.copy().map(Row::ClaimContent)
        }
        Row::CreationResult(row) => {
            #[cfg(test)]
            copy_failure()?;
            row.copy().map(Row::CreationResult)
        }
        Row::Monitor(row) => Ok(Row::Monitor(*row)),
        Row::MonitorHead(row) => Ok(Row::MonitorHead(*row)),
        Row::MonitorLink(row) => Ok(Row::MonitorLink(*row)),
        Row::IncomingHead(row) => Ok(Row::IncomingHead(*row)),
        Row::IncomingLink(row) => Ok(Row::IncomingLink(*row)),
        Row::MissingResult(row) => row.copy().map(Row::MissingResult),
        Row::Meta(meta) => Ok(Row::Meta(*meta)),
        Row::Receipt(receipt) => Ok(Row::Receipt(*receipt)),
        Row::Cycle(cycle) => Ok(Row::Cycle(*cycle)),
        Row::RetiredCycleHead(row) => Ok(Row::RetiredCycleHead(*row)),
        Row::Retired(row) => Ok(Row::Retired(*row)),
        Row::RetiredCycle(row) => Ok(Row::RetiredCycle(*row)),
        Row::WorkSlot(id) => Ok(Row::WorkSlot(*id)),
        Row::ClaimResultTestament(id) => Ok(Row::ClaimResultTestament(*id)),
        Row::ResultTestament(row) => {
            #[cfg(test)]
            copy_failure()?;
            row.copy().map(Row::ResultTestament)
        }
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
        Row::Index => Ok(Row::Index),
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
    pub(super) authored: Option<super::authored::Proof>,
    pub rows: Vec<Extra>,
    pub journal: Option<Vec<NativeFact>>,
    pub(super) control_graph: Option<super::control_graph::ControlGraphProof>,
    pub(super) admission_graph: Option<super::admission_graph::Proof>,
    max: usize,
    allowance: usize,
}
impl Extras {
    pub(super) fn new(max: usize, allowance: usize) -> Result<Self, NativeError> {
        within(array::<Extra>(max)?, allowance)?;
        Ok(Self {
            authored: None,
            rows: Vec::new(),
            journal: None,
            control_graph: None,
            admission_graph: None,
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
        pending: impl DoubleEndedIterator<Item = &'p NativePrepared> + ExactSizeIterator + Clone,
    ) -> Result<Checked<'a>, NativeError> {
        context.principal.require_actor(input.request.principal)?;
        if input.request.principal.is_zero()
            || input.request.id.is_zero()
            || input.request.epoch.0 == 0
        {
            return Err(ContractError::InvalidTarget.into());
        }
        intent::bound_input(&input.command, self.limits)?;
        super::authored::check_profile(self.state.profile, &input.command)?;
        let intent = intent::fingerprint(self.state.ledger, &input)?;
        let (view, meta, sequence, cut) =
            match self.check_request_identity_chain(context, input.request, intent, pending)? {
                RequestCheck::Existing { outcome, committed } => {
                    return Ok(Checked::Existing { outcome, committed });
                }
                RequestCheck::Fresh {
                    view,
                    meta,
                    sequence,
                    cut,
                } => (view, meta, sequence, cut),
            };
        let operation = match &input.command {
            NativeCommand::RegisterMonitor { .. } => NativeOperation::RegisterMonitor,
            NativeCommand::RebindMonitor { .. } => NativeOperation::RebindMonitor,
            NativeCommand::CancelMonitor { .. } => NativeOperation::CancelMonitor,
            NativeCommand::ReleaseScope { .. } => NativeOperation::ReleaseScope,
            NativeCommand::GenerateResultTestament { .. } => {
                NativeOperation::GenerateResultTestament
            }
            NativeCommand::PostResultTestament { .. } => NativeOperation::PostResultTestament,
            NativeCommand::BeginWork { .. } => NativeOperation::BeginWork,
            NativeCommand::ReportWork { .. } => NativeOperation::ReportWork,
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
            NativeCommand::Create { .. } | NativeCommand::CreateAuthored { .. } => {
                NativeOperation::Create
            }
            NativeCommand::AcquireReceipt { .. } => NativeOperation::AcquireReceipt,
            NativeCommand::AdoptReceipt { .. } => NativeOperation::AdoptReceipt,
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
                | NativeOperation::ReportWork
        ) {
            BudgetLane::Completion
        } else {
            BudgetLane::Ordinary
        };
        Ok(Checked::Fresh(Fresh {
            dispatch: Dispatch::Request { input, context },
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

    /// Separate trusted timer ingress. Exact typed retries precede current
    /// clock/eligibility checks and resource admission, including a full queue.
    pub(super) fn check_deadline_chain<'a, 'p: 'a>(
        &'a self,
        input: NativeDeadlineInput,
        logical_time: u64,
        mut pending: impl DoubleEndedIterator<Item = &'p NativePrepared> + ExactSizeIterator + Clone,
    ) -> Result<Checked<'a>, NativeError> {
        let pending_count = pending.len();
        if pending_count > self.limits.pending {
            return Err(NativeError::Capacity("pending candidates"));
        }
        self.state
            .rows
            .validate_chain(pending.clone().map(|item| &item.fragments))?;
        let intent = intent::deadline_fingerprint(self.state.ledger, input)?;
        let view = View {
            state: &self.state,
            tail: pending.next_back(),
        };
        if let Some(outcome) = as_outcome(view.get(Key::Outcome(input.key().into()))) {
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
        if logical_time < meta.logical_time {
            return Err(ContractError::InvalidCut.into());
        }
        meta.logical_time = logical_time;
        meta.outcomes = add(meta.outcomes, 1)?;
        within(meta.outcomes, self.limits.outcomes)?;
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
        let resolved = super::deadlines::resolve(&view, input, logical_time, cut, self.limits)?;
        let lane = if resolved.begun {
            BudgetLane::Completion
        } else {
            BudgetLane::Ordinary
        };
        Ok(Checked::Fresh(Fresh {
            dispatch: Dispatch::Deadline {
                input,
                logical_time,
                resolved,
            },
            view,
            meta,
            sequence,
            cut,
            intent,
            operation: NativeOperation::EvaluationDeadline,
            lane,
            limits: self.limits,
        }))
    }
}

/// Private common identity check for owned inputs and complete borrowed plans.
/// Callers derive the intent from actual checked content before using this seam.
pub(super) enum RequestCheck<'a> {
    Existing {
        outcome: NativeOutcome,
        committed: bool,
    },
    Fresh {
        view: View<'a>,
        meta: Meta,
        sequence: SessionSeq,
        cut: ClaimCut,
    },
}

impl Core<NativeState> {
    pub(super) fn check_request_identity_chain<'a, 'p: 'a>(
        &'a self,
        context: NativeContext,
        request: RequestKey,
        intent: ContentHash,
        mut pending: impl DoubleEndedIterator<Item = &'p NativePrepared> + ExactSizeIterator + Clone,
    ) -> Result<RequestCheck<'a>, NativeError> {
        let count = pending.len();
        if count > self.limits.pending {
            return Err(NativeError::Capacity("pending candidates"));
        }
        self.state
            .rows
            .validate_chain(pending.clone().map(|item| &item.fragments))?;
        context.principal.require_actor(request.principal)?;
        if request.principal.is_zero() || request.id.is_zero() || request.epoch.0 == 0 {
            return Err(ContractError::InvalidTarget.into());
        }
        let view = View {
            state: &self.state,
            tail: pending.next_back(),
        };
        if let Some(outcome) = as_outcome(view.get(Key::Outcome(request.into()))) {
            return if outcome.intent == intent {
                Ok(RequestCheck::Existing {
                    outcome,
                    committed: outcome.sequence <= self.native_sequence(),
                })
            } else {
                Err(NativeError::RequestConflict)
            };
        }
        if count == self.limits.pending {
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
        Ok(RequestCheck::Fresh {
            view,
            meta,
            sequence,
            cut,
        })
    }
}

impl Core<NativeState> {
    /// Claim timer retries use their own identity namespace. Discovery and SCC
    /// allocation happen only after the control construction allowance is held.
    pub(super) fn check_claim_deadline_chain<'a, 'p: 'a>(
        &'a self,
        input: NativeClaimDeadlineInput,
        logical_time: u64,
        mut pending: impl DoubleEndedIterator<Item = &'p NativePrepared> + ExactSizeIterator + Clone,
    ) -> Result<Checked<'a>, NativeError> {
        let pending_count = pending.len();
        if pending_count > self.limits.pending {
            return Err(NativeError::Capacity("pending candidates"));
        }
        self.state
            .rows
            .validate_chain(pending.clone().map(|item| &item.fragments))?;
        let intent = intent::claim_deadline_fingerprint(self.state.ledger, input)?;
        let view = View {
            state: &self.state,
            tail: pending.next_back(),
        };
        if let Some(outcome) = as_outcome(view.get(Key::Outcome(input.key().into()))) {
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
        if logical_time < meta.logical_time {
            return Err(ContractError::InvalidCut.into());
        }
        meta.logical_time = logical_time;
        meta.outcomes = add(meta.outcomes, 1)?;
        within(meta.outcomes, self.limits.outcomes)?;
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
        let resolved = super::claim_deadlines::resolve(&view, input, logical_time)?;
        Ok(Checked::Fresh(Fresh {
            dispatch: Dispatch::ClaimDeadline {
                input,
                logical_time,
                resolved,
            },
            view,
            meta,
            sequence,
            cut,
            intent,
            operation: NativeOperation::ClaimDeadline,
            lane: BudgetLane::Completion,
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
    dispatch: Dispatch,
    view: View<'a>,
    meta: Meta,
    sequence: SessionSeq,
    cut: ClaimCut,
    intent: ContentHash,
    operation: NativeOperation,
    lane: BudgetLane,
    limits: NativeLimits,
}

/// The actor and timer ingress types never share a forged principal or key.
#[allow(clippy::large_enum_variant)] // Owned input stays on the pre-admission stack.
enum Dispatch {
    Request {
        input: NativeInput,
        context: NativeContext,
    },
    Deadline {
        input: NativeDeadlineInput,
        logical_time: u64,
        resolved: super::deadlines::Resolved,
    },
    ClaimDeadline {
        input: NativeClaimDeadlineInput,
        logical_time: u64,
        resolved: super::claim_deadlines::Resolved,
    },
    MonitorDeadline {
        input: NativeMonitorDeadlineInput,
        logical_time: u64,
        resolved: super::monitor_deadlines::Resolved,
    },
}
impl Dispatch {
    fn invocation(&self) -> NativeInvocation {
        match self {
            Self::Request { input, .. } => input.request.into(),
            Self::Deadline { input, .. } => input.key().into(),
            Self::ClaimDeadline { input, .. } => input.key().into(),
            Self::MonitorDeadline { input, .. } => input.key().into(),
        }
    }
    fn logical_time(&self) -> u64 {
        match self {
            Self::Request { context, .. } => context.logical_time,
            Self::Deadline { logical_time, .. } => *logical_time,
            Self::ClaimDeadline { logical_time, .. } => *logical_time,
            Self::MonitorDeadline { logical_time, .. } => *logical_time,
        }
    }
}

/// Exact Begin authorized against the immutable source, retained without owning
/// or cloning its roots. Only Fresh can create this capability; the source
/// outlives Fresh so its consumed command cannot invalidate the proof.
pub(super) struct BeginTransition<'a> {
    source: View<'a>,
    invocation: NativeInvocation,
    intent: ContentHash,
    logical_time: u64,
    key: EvaluationKey,
    previous: &'a validation::EvaluationState,
    next: validation::EvaluationState,
}

impl BeginTransition<'_> {
    pub(super) fn key(&self) -> EvaluationKey {
        self.key
    }

    pub(super) fn previous(&self) -> validation::EvaluationState {
        *self.previous
    }

    pub(super) fn next(&self) -> validation::EvaluationState {
        self.next
    }

    pub(super) fn check(&self, prepared: &NativePrepared) -> Result<(), NativeError> {
        self.source.check_successor(prepared)?;
        if self.invocation != prepared.outcome.invocation
            || self.intent != prepared.outcome.intent
            || self.logical_time != prepared.outcome.logical_time
            || self.source.ledger() != prepared.outcome.ledger
            || self.source.prefix().0.checked_add(1) != Some(prepared.outcome.sequence.0)
            || self.source.evaluation(self.key)? != self.previous
            || self.previous.has_begun()
            || (!self.next.has_begun() && self.next.state() != validation::State::Ready)
            || self.next.fence().is_some()
            || self.next.state().is_terminal()
        {
            return Err(ContractError::InvalidTransition.into());
        }
        let operation = match self.key.target {
            EvaluationTarget::Admission => NativeOperation::BeginAdmission,
            EvaluationTarget::Increment { .. } => NativeOperation::BeginIncrement,
            EvaluationTarget::Work { .. } => NativeOperation::BeginWork,
            _ => return Err(ContractError::InvalidTarget.into()),
        };
        if prepared.outcome.operation != operation {
            return Err(ContractError::InvalidTransition.into());
        }
        self.previous
            .binding()
            .next()?
            .check(&self.next.binding())?;
        Ok(())
    }
}

#[allow(clippy::large_enum_variant)] // Fixed borrowed owner proof, before resource admission.
pub(super) enum Admission<'request, 'source> {
    Begin {
        key: EvaluationKey,
        registered: super::admission_authority::Registered<'request>,
        binding: Binding,
        active: bool,
        transition: BeginTransition<'source>,
    },
    Report {
        key: EvaluationKey,
        registered: super::admission_authority::Registered<'request>,
        authorization: validation::ReportAuthorization,
        artifact: &'request NativeArtifactInput,
    },
}

/// Prepared rows and their transient checked seal capabilities share the
/// original construction allowance. Only the token buffer remains charged
/// after construction; the owner releases it after validating its journal.
pub(super) struct BuiltNative {
    prepared: NativePrepared,
    seals: Vec<validation::SealTransition>,
    funding: Option<Allocation>,
}

impl BuiltNative {
    fn new(
        prepared: NativePrepared,
        seals: Vec<validation::SealTransition>,
        funding: Allocation,
    ) -> Result<Self, NativeError> {
        // Keep drop order explicit through the owned fields even on refusal:
        // pages and tokens are destroyed before their construction debit.
        let mut built = Self {
            prepared,
            seals,
            funding: Some(funding),
        };
        let bytes = array::<validation::SealTransition>(built.seals.capacity())?;
        built
            .funding
            .as_mut()
            .ok_or(ContractError::InvalidManifest)?
            .shrink_to(bytes)?;
        if bytes == 0 {
            drop(built.funding.take());
        }
        Ok(built)
    }

    pub(super) fn prepared(&self) -> &NativePrepared {
        &self.prepared
    }

    pub(super) fn seals(&self) -> &[validation::SealTransition] {
        &self.seals
    }

    pub(super) fn into_prepared(self) -> NativePrepared {
        let Self {
            prepared,
            seals,
            funding,
        } = self;
        drop(seals);
        drop(funding);
        prepared
    }
}

impl<'a> Fresh<'a> {
    fn begin_transition(
        &self,
        key: EvaluationKey,
        next: validation::EvaluationState,
    ) -> Result<BeginTransition<'a>, NativeError> {
        Ok(BeginTransition {
            source: self.publication_source(),
            invocation: self.dispatch.invocation(),
            intent: self.intent,
            logical_time: self.dispatch.logical_time(),
            key,
            previous: as_evaluation(self.view.get(Key::Evaluation(key)))
                .ok_or(ContractError::InvalidTarget)?,
            next,
        })
    }

    pub(super) fn authorize_admission(&self) -> Result<Option<Admission<'_, 'a>>, NativeError> {
        let Dispatch::Request { input, context } = &self.dispatch else {
            return Ok(None);
        };
        match &input.command {
            NativeCommand::BeginWork {
                claim,
                key,
                expected,
            } => {
                let begun = super::projection::with_projection(
                    &self.view,
                    key.claim,
                    self.limits,
                    &self.view.state.budget,
                    |projection| {
                        super::work_authority::begin(
                            &self.view,
                            *context,
                            *claim,
                            *key,
                            *expected,
                            self.limits,
                            &projection.claim_decision(),
                        )
                    },
                )??;
                Ok(Some(Admission::Begin {
                    key: *key,
                    binding: begun.next.binding(),
                    active: begun.next.has_begun(),
                    transition: self.begin_transition(*key, begun.next)?,
                    registered: begun.registered,
                }))
            }
            NativeCommand::ReportWork {
                claim,
                key,
                expected,
                report,
                artifact,
            } => {
                let (registered, authorization) = super::work_authority::report(
                    &self.view,
                    *context,
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
                let begin = if matches!(&input.command, NativeCommand::BeginIncrement { .. }) {
                    super::increment_authority::begin
                } else {
                    super::admission_authority::begin
                };
                let begun = begin(&self.view, *context, *claim, *key, *expected, self.limits)?;
                Ok(Some(Admission::Begin {
                    key: *key,
                    binding: begun.next.binding(),
                    active: begun.next.has_begun(),
                    transition: self.begin_transition(*key, begun.next)?,
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
                let authorize = if matches!(&input.command, NativeCommand::ReportIncrement { .. }) {
                    super::increment_authority::report
                } else {
                    super::admission_authority::report
                };
                let (registered, authorization) = authorize(
                    &self.view,
                    *context,
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
    /// Borrow the same immutable effective prefix across construction. This
    /// copies only the two references, never a root, row, grant or authority.
    pub(super) fn publication_source(&self) -> View<'a> {
        View {
            state: self.view.state,
            tail: self.view.tail,
        }
    }
    pub(super) fn lane(&self) -> BudgetLane {
        self.lane
    }
    pub(super) fn limits(&self) -> NativeLimits {
        self.limits
    }
    pub(super) fn input(&self) -> Result<&NativeInput, NativeError> {
        match &self.dispatch {
            Dispatch::Request { input, .. } => Ok(input),
            Dispatch::Deadline { .. }
            | Dispatch::ClaimDeadline { .. }
            | Dispatch::MonitorDeadline { .. } => Err(ContractError::WrongActor.into()),
        }
    }
    pub(super) fn authorize_deadline(
        &self,
    ) -> Result<Option<super::deadlines::Resolved>, NativeError> {
        match self.dispatch {
            Dispatch::Deadline { resolved, .. } => Ok(Some(resolved)),
            Dispatch::Request { .. }
            | Dispatch::ClaimDeadline { .. }
            | Dispatch::MonitorDeadline { .. } => Ok(None),
        }
    }
    pub(super) fn source(&self) -> &'a MemoryBudget {
        &self.view.state.budget
    }
    pub(super) fn authorize_work(
        &self,
    ) -> Result<Option<&focal_model::lifecycle::artifact_descriptor::ArtifactDescriptor>, NativeError>
    {
        match &self.dispatch {
            Dispatch::Request { input, context } => {
                super::work_artifacts::authorize(&self.view, *context, &input.command, self.limits)
            }
            Dispatch::Deadline { .. }
            | Dispatch::ClaimDeadline { .. }
            | Dispatch::MonitorDeadline { .. } => Ok(None),
        }
    }

    pub(super) fn authorize_respondent(
        &self,
    ) -> Result<
        Option<(
            super::respondent_state::RespondentKey,
            super::respondent_state::RespondentSpend,
        )>,
        NativeError,
    > {
        let Dispatch::Request { input, context } = &self.dispatch else {
            return Ok(None);
        };
        let binding = match &input.command {
            NativeCommand::SubmitDiagnostic { claim, .. }
            | NativeCommand::CloseResponse { claim, .. }
            | NativeCommand::PostResponse { claim, .. } => *claim,
            _ => return Ok(None),
        };
        // Work diagnostics authenticate provenance and caller before custody IO.
        self.authorize_work()?;
        let claim = self
            .view
            .claim(ClaimId(binding.object.0))
            .ok_or(ContractError::InvalidTarget)?;
        super::respondent_state::spend(&self.view, claim, *context, &input.command, self.limits)
    }

    pub(super) fn build_respondent(
        self,
        source: &MemoryBudget,
        evidence: Option<&focal_evidence::VerifiedNativeArtifact>,
        envelope: &super::respondent_envelope::RespondentEnvelope,
    ) -> Result<BuiltNative, NativeError> {
        self.build_inner(source, evidence, None, None, Some(envelope))
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
        self.build_recorded(source, evidence, completion, None)
            .map(BuiltNative::into_prepared)
    }

    pub(super) fn build_recorded(
        self,
        source: &MemoryBudget,
        evidence: Option<&focal_evidence::VerifiedNativeArtifact>,
        completion: Option<&super::completion_envelope::CompletionEnvelope>,
        deadline_envelope: Option<focal_memory::RangeWriteEnvelope>,
    ) -> Result<BuiltNative, NativeError> {
        if deadline_envelope.is_some() && !matches!(self.dispatch, Dispatch::Deadline { .. }) {
            return Err(ContractError::InvalidTransition.into());
        }
        self.build_inner(source, evidence, completion, deadline_envelope, None)
    }

    fn build_inner(
        self,
        source: &MemoryBudget,
        evidence: Option<&focal_evidence::VerifiedNativeArtifact>,
        completion: Option<&super::completion_envelope::CompletionEnvelope>,
        deadline_envelope: Option<focal_memory::RangeWriteEnvelope>,
        respondent: Option<&super::respondent_envelope::RespondentEnvelope>,
    ) -> Result<BuiltNative, NativeError> {
        let Self {
            dispatch,
            view,
            mut meta,
            sequence,
            cut,
            intent,
            operation,
            lane,
            limits,
        } = self;
        let invocation = dispatch.invocation();
        let logical_time = dispatch.logical_time();
        if !source.is_within(&view.state.budget) {
            return Err(MemoryError::InvalidConfiguration(
                "native allocation source is outside owner budget",
            )
            .into());
        }
        // Work responsibility can only be accepted through the managed owner
        // after it has installed the checked report contract. Direct Core calls
        // retain their existing families but cannot bypass this funding gate.
        if matches!(
            operation,
            NativeOperation::BeginWork | NativeOperation::ReportWork
        ) && completion.is_none()
        {
            return Err(ContractError::InvalidPolicy.into());
        }
        let cohort = if let Some(completion) = completion {
            completion.cohort()
        } else if let Dispatch::Request {
            input:
                NativeInput {
                    command: NativeCommand::ReportAdmission { key, .. },
                    ..
                },
            ..
        } = &dispatch
        {
            let parent = view.claim(key.claim).ok_or(ContractError::InvalidTarget)?;
            let definition = view.definition(key.validation)?;
            if parent.status() == ClaimStatus::Posted
                && definition.mode() == focal_model::ValidationMode::Required
            {
                let registry = view
                    .owned_claim(key.claim)?
                    .registrations()
                    .ok_or(ContractError::InvalidTarget)?;
                super::completion_envelope::cohort_bound(limits, parent, registry)?
            } else {
                super::cohort_budget::CohortBudget::empty()
            }
        } else {
            super::cohort_budget::CohortBudget::empty()
        };
        let construction = if let Some(respondent) = respondent {
            if completion.is_some() || deadline_envelope.is_some() {
                return Err(ContractError::InvalidTransition.into());
            }
            respondent.construction(operation, limits)?
        } else if operation == NativeOperation::ReportAdmission {
            if let Some(completion) = completion {
                completion.report_construction(limits)?
            } else {
                // Direct Core preparation has no held completion contract. It
                // reserves a bounded graph-capable stage before discovery;
                // managed owners use the exact pre-funded shape above.
                super::prepare_budget::ConstructionBudget::for_operation(
                    NativeOperation::ReportWork,
                    limits,
                )?
                .with_cohort(cohort, limits)?
            }
        } else {
            super::prepare_budget::ConstructionBudget::for_operation(operation, limits)?
                .with_cohort(cohort, limits)?
        };
        // Bound every possible admitted Begin/Report write against immutable
        // storage limits. The bound is checked against the actual final plan;
        // it is not itself a reservation for future reports.
        let envelope = if matches!(
            operation,
            NativeOperation::BeginAdmission
                | NativeOperation::ReportAdmission
                | NativeOperation::BeginIncrement
                | NativeOperation::ReportIncrement
                | NativeOperation::BeginWork
                | NativeOperation::ReportWork
        ) {
            Some(
                view.state.rows.future_write_envelope(
                    focal_memory::RangeWriteLimits {
                        changed_keys: construction.max_changes,
                        // Status rows of every moved claim and the due
                        // timers a report can retire (doc 22 §7).
                        deleted_keys: add(
                            construction.max_claim_rows,
                            construction.max_timer_rows,
                        )?
                        .min(construction.max_changes),
                        deleted_heap: 0,
                        incoming_heap: add(
                            construction.scratch_bytes,
                            add(
                                containers(construction.max_claim_rows)?,
                                event_containers(construction.max_events)?,
                            )?,
                        )?,
                        input_capacity: construction.max_changes,
                    },
                    limits.max_ranges,
                )?,
            )
        } else {
            None
        };
        let allocation = source
            .reserve(BudgetKind::Pending, lane, construction.temporary_bytes()?)?
            .commit();
        let mut scratch = Scratch {
            used: 0,
            max: construction.scratch_bytes,
        };
        let mut extras = Extras::new(construction.extras_count, construction.extras_bytes)?;
        let report_parent = match &dispatch {
            Dispatch::Request {
                input:
                    NativeInput {
                        command:
                            NativeCommand::ReportAdmission { claim, .. }
                            | NativeCommand::ReportIncrement { claim, .. },
                        ..
                    },
                ..
            } => Some(super::completion_envelope::ReportParent::capture(
                view.claim(ClaimId(claim.object.0))
                    .ok_or(ContractError::InvalidTarget)?,
            )),
            _ => None,
        };
        let plan = match dispatch {
            Dispatch::Request { input, context } => transactions::prepare(
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
            )?,
            Dispatch::Deadline { resolved, .. } => {
                if resolved.begun && deadline_envelope.is_none() {
                    return Err(ContractError::InvalidPolicy.into());
                }
                super::deadlines::prepare(&view, resolved, &mut extras, &mut scratch)?
            }
            Dispatch::ClaimDeadline {
                input,
                logical_time,
                resolved,
            } => super::claim_deadlines::prepare(
                &view,
                resolved,
                input,
                logical_time,
                cut,
                limits,
                &mut extras,
                &mut scratch,
            )?,
            Dispatch::MonitorDeadline {
                input,
                logical_time,
                resolved,
            } => super::monitor_deadlines::prepare(
                &view,
                resolved,
                input,
                logical_time,
                cut,
                limits,
                &mut extras,
                &mut scratch,
            )?,
        };
        let completion_use = match report_parent {
            Some(parent) => parent.completion_use_prepared(
                operation,
                plan.rows
                    .iter()
                    .find(|row| row.binding().object.0 == parent.claim_id().0),
                extras
                    .admission_graph
                    .as_ref()
                    .map(super::admission_graph::Proof::event),
            )?,
            None => super::completion_envelope::CompletionUse::Regular,
        };
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
        let result_testaments = extras
            .rows
            .iter()
            .filter(|row| matches!(row.key, Key::ResultTestament(_)))
            .count();
        let outcome = NativeOutcome {
            ledger: view.ledger(),
            invocation,
            sequence,
            logical_time,
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
            result_testaments: u32::try_from(result_testaments)
                .map_err(|_| NativeError::Capacity("result testament count"))?,
            events: u32::try_from(add(
                if extras.journal.is_some() {
                    0
                } else {
                    claim_changes::event_count(&plan.rows, &view, operation)?
                },
                extras.events(),
            )?)
            .map_err(|_| NativeError::Capacity("event count"))?,
        };
        construction.check_counts(
            plan.rows.len(),
            extras.rows.len(),
            usize::try_from(outcome.events).map_err(|_| NativeError::Capacity("events"))?,
            0,
        )?;
        transactions::increment(
            &mut meta.events,
            usize::try_from(outcome.events).map_err(|_| NativeError::Capacity("events"))?,
            limits.events,
            "events",
        )?;
        let original = claim_changes::OriginalPlan::check(
            plan,
            extras,
            meta,
            outcome,
            &view,
            limits,
            &mut scratch,
        )?;
        let claim_changes::SealedChanges {
            changes,
            outcome,
            meta: final_meta,
            seals,
            index,
        } = original
            .with_seals(&mut scratch)?
            .into_changes(construction.changes_bytes, &mut scratch)?;
        let changed =
            usize::try_from(outcome.changed).map_err(|_| NativeError::Capacity("changed count"))?;
        let events =
            usize::try_from(outcome.events).map_err(|_| NativeError::Capacity("event count"))?;
        let final_extras = changes
            .len()
            .checked_sub(add(add(changed, events)?, add(2, index)?)?)
            .ok_or(ContractError::InvalidManifest)?;
        construction.check_counts(changed, final_extras, events, index)?;
        if final_meta.events != add(view.meta().events, events)? {
            return Err(ContractError::InvalidManifest.into());
        }
        let range_plan = match view.tail {
            Some(tail) => view.state.rows.plan_after(
                source,
                &tail.fragments,
                sequence.0,
                changes,
                lane,
                usize::MAX,
            )?,
            None => view
                .state
                .rows
                .plan_batch(source, sequence.0, changes, lane, usize::MAX)?,
        };
        if let Some(envelope) = envelope {
            range_plan.check_envelope(&envelope)?;
        }
        if let Some(completion) = completion
            && operation != NativeOperation::BeginWork
        {
            range_plan.check_envelope(&completion.report_storage(completion_use)?)?;
        }
        if let Some(envelope) = deadline_envelope {
            range_plan.check_envelope(&envelope)?;
        }
        if let Some(respondent) = respondent {
            range_plan.check_envelope(&respondent.storage(operation)?)?;
        }
        // The actual shape is now fixed. Charging the maximum shape here would
        // exceed a regular report's smaller retained promise (e.g. 9 vs 11 rows).
        let mutation_bytes = super::mutation::bytes(range_plan.changes_len())?;
        within(mutation_bytes, construction.mutation_bytes()?)?;
        let writes = super::mutation::WriteSet::capture(
            view.state.profile,
            range_plan.changes_len(),
            range_plan.changes(),
            construction.mutation_bytes()?,
            source
                .reserve(BudgetKind::Pending, lane, mutation_bytes)?
                .commit(),
        )?;
        let fragments = range_plan.build_in_with(source, copy)?;
        writes.check(&fragments)?;
        BuiltNative::new(
            NativePrepared {
                fragments,
                outcome,
                writes,
            },
            seals,
            allocation,
        )
    }
}
