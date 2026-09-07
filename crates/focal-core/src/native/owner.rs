//! Exclusive ownership of the speculative native chain. Tickets identify a
//! candidate but cannot retain its pages, fork it, or transfer its resources.

use super::completion_book::{CompletionBook, Journal};
use super::completion_envelope::{CompletionEnvelope, EvidenceBounds, descriptor_limits};
use super::completion_schemas::SchemaSet;
use super::*;
use focal_evidence::{
    BuiltinNativeSchemas, ContentStore, NativeSchemaVerifier, VerifiedNativeArtifact,
};
use focal_memory::{Allocation, BudgetKind, BudgetLane, OwnerId};
use focal_model::ContentDomainId;
use focal_model::lifecycle::aggregation;
use std::collections::VecDeque;

fn check_revision_capacity(binding: Binding, reports: u32) -> Result<(), NativeError> {
    binding
        .revision
        .0
        .checked_add(u64::from(reports))
        .ok_or(NativeError::Capacity("completion revision margin"))?;
    Ok(())
}

fn completion_contract(
    view: &View<'_>,
    limits: NativeLimits,
    registered: &super::admission_authority::Registered<'_>,
    schemas: &impl NativeSchemaVerifier,
) -> Result<(CompletionEnvelope, SchemaSet), NativeError> {
    let state = view.state;
    // Required failure may advance the parent once. Ordinary mutations must
    // preserve this last revision while an active grant still needs it.
    if registered.definition.mode() == focal_model::ValidationMode::Required
        && registered.definition.target() == validation::TargetDeclaration::Admission
        && registered.parent.status() == focal_model::ClaimStatus::Posted
    {
        registered.parent.binding().next()?;
    }
    let pins = SchemaSet::new(
        registered.definition,
        &state.budget,
        schemas,
        limits.plan_edges,
    )?;
    let descriptor = descriptor_limits(limits, registered.parent, registered.registry)?;
    let evidence = EvidenceBounds {
        workspace_bytes: pins
            .workspace_bytes()
            .checked_sub(pins.custody_bytes())
            .ok_or(NativeError::Capacity("completion verification workspace"))?,
        retained_bytes: pins.custody_bytes(),
    };
    let envelope = CompletionEnvelope::derive(
        &state.rows,
        limits,
        registered.parent,
        registered.registry,
        registered.definition,
        descriptor,
        evidence,
    )?;
    if matches!(
        registered.state.target(),
        validation::Target::Increment { .. }
    ) {
        super::increment_authority::check_completion_target(view, registered, &envelope, limits)?;
    }
    Ok((envelope, pins))
}

fn build_funded(
    fresh: prepare::Fresh<'_>,
    book: &mut CompletionBook,
    evidence: Option<&VerifiedNativeArtifact>,
    custody: Option<(&mut ContentStore, ContentDomainId)>,
    schemas: &impl NativeSchemaVerifier,
) -> Result<(NativePrepared, Journal), NativeError> {
    let source = fresh.source();
    match fresh.authorize_admission()? {
        Some(prepare::Admission::Begin {
            key,
            registered,
            binding,
            active: true,
        }) => {
            check_revision_capacity(binding, registered.definition.attempt_bound())?;
            let (envelope, pins) =
                completion_contract(fresh.view(), fresh.limits(), &registered, schemas)?;
            let journal =
                book.install_begin(key, binding, envelope, pins, registered.registration_index)?;
            match fresh.build(source, evidence) {
                Ok(prepared) => Ok((prepared, journal)),
                Err(error) => {
                    book.rollback(journal)?;
                    Err(error)
                }
            }
        }
        Some(prepare::Admission::Report {
            key,
            registered,
            authorization,
            artifact,
        }) => {
            let before = registered.state.binding();
            let loan = book.report_contract(
                key,
                before,
                registered.parent,
                registered.registry,
                artifact,
                authorization.schema(),
                schemas,
            )?;
            let verified = if let Some((store, domain)) = custody {
                Some(store.verify_native_artifact_with_budget(
                    fresh.input().request,
                    artifact.get().ok_or(ContractError::MissingEvidence)?,
                    domain,
                    loan.source(),
                    schemas,
                    loan.verification(),
                )?)
            } else {
                None
            };
            let prepared = fresh.build_with_completion(
                loan.source(),
                verified.as_ref().or(evidence),
                Some(loan.envelope()),
            )?;
            drop(verified);
            let after = prepared
                .evaluation(key)
                .ok_or(ContractError::StaleEvaluation)?;
            let journal = book.advance(
                key,
                before,
                after.binding(),
                after.state().is_terminal() || after.fence().is_some(),
                prepared.outcome.changed != 0,
            )?;
            Ok((prepared, journal))
        }
        Some(prepare::Admission::Begin { .. }) | None => {
            let descriptor = fresh.authorize_work()?;
            let verified = if let (Some(descriptor), Some((store, domain))) = (descriptor, custody)
            {
                Some(store.verify_native_artifact(
                    fresh.input().request,
                    descriptor,
                    domain,
                    source,
                    schemas,
                )?)
            } else {
                None
            };
            let prepared = fresh.build(source, verified.as_ref().or(evidence))?;
            drop(verified);
            let journal = book.retire_prepared(&prepared)?;
            Ok((prepared, journal))
        }
    }
}

#[cfg(test)]
#[path = "owner_tests.rs"]
mod tests;

#[cfg(test)]
#[path = "owner_completion_tests.rs"]
mod completion_tests;

/// Process-local candidate identity. Neither this identity nor its owner stamp
/// is a durable receipt. A discarded serial is never reused in this owner.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NativeCandidate {
    owner: OwnerId,
    serial: u64,
}

/// Preparation does not acknowledge durability. A pending retry retains the
/// original ticket; a committed retry needs no candidate or additional charge.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NativeStaging {
    Prepared {
        candidate: NativeCandidate,
        outcome: NativeOutcome,
    },
    Existing {
        outcome: NativeOutcome,
        candidate: Option<NativeCandidate>,
    },
}

#[derive(Debug, thiserror::Error)]
pub enum NativeOwnerError {
    #[error(transparent)]
    Native(#[from] NativeError),
    #[error("candidate belongs to another native owner incarnation")]
    WrongOwner,
    #[error("native candidate is no longer pending")]
    UnknownCandidate,
    #[error("native candidates must publish in preparation order")]
    OutOfOrder,
}
impl From<MemoryError> for NativeOwnerError {
    fn from(error: MemoryError) -> Self {
        Self::Native(NativeError::Memory(error))
    }
}

/// Failed ownership transfer returns the original Core without cloning or
/// allocating. The caller can relieve pressure and retry the same transfer.
#[derive(Debug)]
pub struct NativeOwnerInitError {
    pub error: NativeOwnerError,
    pub core: Core<NativeState>,
}
impl std::fmt::Display for NativeOwnerInitError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.error.fmt(f)
    }
}
impl std::error::Error for NativeOwnerInitError {}

#[derive(Debug)]
struct Pending {
    candidate: NativeCandidate,
    prepared: NativePrepared,
    journal: Journal,
}

/// Owns the Core and every unpublished candidate in exactly one ordered chain.
/// The queue is bounded and charged before construction. Preparation requires
/// exclusive access; reads expose borrowed projections only. There is no Core,
/// mutable row, owned candidate, or allocation-source escape hatch.
///
/// This is an in-process resource ownership boundary. It performs no log IO and
/// protects bounded RAM/report slots; disk capacity and durable reconstruction
/// still require the enclosing log/custody owner.
/// A durable owner must retain it while append/commit status is unresolved.
pub struct NativeOwner {
    // Drop buffers and retained pages before releasing their queue allowance.
    pending: VecDeque<Pending>,
    core: Core<NativeState>,
    book: CompletionBook,
    faulted: bool,
    _queue_allocation: Allocation,
    incarnation: OwnerId,
    next_serial: u64,
}
impl std::fmt::Debug for NativeOwner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NativeOwner")
            .field("incarnation", &self.incarnation)
            .field("committed", &self.core.native_sequence())
            .field("pending", &self.pending.len())
            .finish_non_exhaustive()
    }
}
impl Drop for NativeOwner {
    fn drop(&mut self) {
        self.discard_all();
    }
}

impl NativeOwner {
    /// Takes exclusive ownership of a native Core. Previously issued snapshot
    /// leases remain valid; previously detached low-level candidates cannot be
    /// imported. Internal pending limits are derived from the node allowance.
    #[allow(clippy::result_large_err)] // Preserve the original Core without allocation on refusal.
    pub fn new(core: Core<NativeState>) -> Result<Self, NativeOwnerInitError> {
        Self::with_schemas(core, &BuiltinNativeSchemas)
    }

    /// Reconstruct and fund all already-begun, unfenced evaluations before
    /// admitting another mutation. Schema bounds are pinned by their immutable
    /// identity and checked again against the supplied registry on each report.
    #[allow(clippy::result_large_err)]
    pub fn with_schemas(
        core: Core<NativeState>,
        schemas: &impl NativeSchemaVerifier,
    ) -> Result<Self, NativeOwnerInitError> {
        let resources = (|| {
            let (incarnation, pending, allocation) = Self::allocate_queue(&core)?;
            let mut book = CompletionBook::new(&core.state.budget, core.limits)?;
            let view = View {
                state: &core.state,
                tail: None,
            };
            for entry in core.state.rows.entries() {
                let Key::Evaluation(key) = entry.key else {
                    continue;
                };
                let state = view.evaluation(key)?;
                if !state.has_begun() || state.state().is_terminal() || state.fence().is_some() {
                    continue;
                }
                let parent = view.claim(key.claim).ok_or(ContractError::InvalidTarget)?;
                let registered =
                    super::admission_authority::registered_any(&view, parent.binding(), key)?;
                let attempt = state.bind(registered.definition)?.current_attempt()?;
                let remaining = registered
                    .definition
                    .attempt_bound()
                    .checked_sub(attempt.index)
                    .filter(|remaining| *remaining != 0)
                    .ok_or(NativeError::Capacity("remaining completion reports"))?;
                check_revision_capacity(state.binding(), remaining)?;
                let (envelope, pins) =
                    completion_contract(&view, core.limits, &registered, schemas)?;
                book.install_recovered(
                    key,
                    state.binding(),
                    remaining,
                    envelope,
                    pins,
                    registered.registration_index,
                )?;
            }
            book.check_slots(view.meta(), view.prefix(), core.state.rows.len())?;
            Ok::<_, NativeError>((incarnation, pending, allocation, book))
        })();
        match resources {
            Ok((incarnation, pending, allocation, book)) => Ok(Self {
                pending,
                core,
                book,
                faulted: false,
                _queue_allocation: allocation,
                incarnation,
                next_serial: 0,
            }),
            Err(error) => Err(NativeOwnerInitError {
                error: error.into(),
                core,
            }),
        }
    }

    fn allocate_queue(
        core: &Core<NativeState>,
    ) -> Result<(OwnerId, VecDeque<Pending>, Allocation), NativeError> {
        let incarnation = OwnerId::new()?;
        let bytes = prepare::array::<Pending>(core.limits.pending)?;
        let reservation =
            core.state
                .budget
                .reserve(BudgetKind::Pending, BudgetLane::Ordinary, bytes)?;
        let mut pending = VecDeque::new();
        pending
            .try_reserve_exact(core.limits.pending)
            .map_err(|_| MemoryError::AllocationFailed)?;
        // Allocator rounding cannot silently widen the original precharge.
        prepare::within(prepare::array::<Pending>(pending.capacity())?, bytes)?;
        Ok((incarnation, pending, reservation.commit()))
    }

    pub fn pending_len(&self) -> usize {
        self.pending.len()
    }

    pub fn oldest(&self) -> Option<NativeCandidate> {
        self.pending.front().map(|row| row.candidate)
    }

    /// Stage against the entire retained chain. A fresh failure leaves both
    /// committed and pending roots intact. Exact retry lookup precedes queue
    /// capacity and memory admission, including retries of an earlier candidate.
    pub fn prepare(
        &mut self,
        context: NativeContext,
        input: NativeInput,
        evidence: Option<&VerifiedNativeArtifact>,
    ) -> Result<NativeStaging, NativeOwnerError> {
        self.prepare_using(context, input, evidence, None, &BuiltinNativeSchemas)
    }

    /// Check participant authority and the exact pending attempt before touching
    /// evidence storage. Verification and native construction use the same held
    /// owner capacity. Focal verifies submitted evidence; it does not run the
    /// participant's validator, tool, skill or agent.
    pub fn prepare_with_custody(
        &mut self,
        context: NativeContext,
        input: NativeInput,
        store: &mut ContentStore,
        inline_domain: ContentDomainId,
        schemas: &impl NativeSchemaVerifier,
    ) -> Result<NativeStaging, NativeOwnerError> {
        self.prepare_using(context, input, None, Some((store, inline_domain)), schemas)
    }

    /// A trusted embedding may provide custody it has already verified. The
    /// registry must still match the pinned completion contract for this report.
    pub fn prepare_evidenced_with_schemas(
        &mut self,
        context: NativeContext,
        input: NativeInput,
        evidence: Option<&VerifiedNativeArtifact>,
        schemas: &impl NativeSchemaVerifier,
    ) -> Result<NativeStaging, NativeOwnerError> {
        self.prepare_using(context, input, evidence, None, schemas)
    }

    fn prepare_using(
        &mut self,
        context: NativeContext,
        input: NativeInput,
        evidence: Option<&VerifiedNativeArtifact>,
        custody: Option<(&mut ContentStore, ContentDomainId)>,
        schemas: &impl NativeSchemaVerifier,
    ) -> Result<NativeStaging, NativeOwnerError> {
        let preparation = self.core.check_native_chain(
            context,
            input,
            self.pending.iter().map(|row| &row.prepared),
        )?;
        match preparation {
            prepare::Checked::Existing { outcome, committed } => {
                let candidate = if committed {
                    None
                } else {
                    Some(
                        self.pending
                            .iter()
                            .find(|row| row.prepared.outcome() == outcome)
                            .ok_or(NativeOwnerError::UnknownCandidate)?
                            .candidate,
                    )
                };
                Ok(NativeStaging::Existing { outcome, candidate })
            }
            prepare::Checked::Fresh(fresh) => {
                if self.faulted {
                    return Err(
                        NativeError::Capacity("completion owner requires reconstruction").into(),
                    );
                }
                if let Err(error) = self.book.check_health() {
                    self.faulted = true;
                    return Err(error.into());
                }
                let serial = self
                    .next_serial
                    .checked_add(1)
                    .ok_or(MemoryError::CounterExhausted("native candidate serial"))?;
                if self.pending.len() >= self.core.limits.pending
                    || self.pending.len() == self.pending.capacity()
                {
                    return Err(NativeError::Capacity("pending candidates").into());
                }
                let (prepared, journal) =
                    build_funded(fresh, &mut self.book, evidence, custody, schemas)?;
                let view = View {
                    state: &self.core.state,
                    tail: Some(&prepared),
                };
                let checked = self
                    .book
                    .check_slots(view.meta(), view.prefix(), prepared.range.len())
                    .and_then(|()| self.book.check_serial(serial))
                    .and_then(|()| self.book.check_parents(&prepared));
                if let Err(error) = checked {
                    drop(prepared);
                    if let Err(rollback) = self.book.rollback(journal) {
                        self.faulted = true;
                        return Err(rollback.into());
                    }
                    return Err(error.into());
                }
                let candidate = NativeCandidate {
                    owner: self.incarnation,
                    serial,
                };
                let outcome = prepared.outcome();
                self.pending.push_back(Pending {
                    candidate,
                    prepared,
                    journal,
                });
                self.next_serial = serial;
                Ok(NativeStaging::Prepared { candidate, outcome })
            }
        }
    }

    /// Publish the head only after the external log owner establishes its
    /// durability. A ticket alone is not proof of durability. Publication makes
    /// no allocation. A Core publication refusal retains the identical candidate
    /// and ticket. An internal bookkeeping failure after publication faults the
    /// owner; exact retry still exposes the committed outcome for reconciliation.
    /// Later candidates remain in the same chain after a successful publication.
    pub fn publish_after_durable(
        &mut self,
        candidate: NativeCandidate,
    ) -> Result<NativeOutcome, NativeOwnerError> {
        if self.position(candidate)? != 0 {
            return Err(NativeOwnerError::OutOfOrder);
        }
        let head = self
            .pending
            .pop_front()
            .ok_or(NativeOwnerError::UnknownCandidate)?;
        match self.core.publish_native(head.prepared) {
            Ok(outcome) => {
                if let Err(error) = self.book.commit(head.journal) {
                    self.faulted = true;
                    return Err(error.into());
                }
                if self.pending.is_empty()
                    && let Err(error) = self.book.trim_idle()
                {
                    self.faulted = true;
                    return Err(error.into());
                }
                Ok(outcome)
            }
            Err(refused) => {
                // pop_front retained this exact slot: restoration cannot grow
                // the buffer, allocate, change the ticket, or reorder the chain.
                self.pending.push_front(Pending {
                    candidate: head.candidate,
                    prepared: refused.prepared,
                    journal: head.journal,
                });
                Err(refused.error.into())
            }
        }
    }

    /// Discard the named candidate and every dependent successor, from tail to
    /// head. The external log owner must first know the suffix cannot commit;
    /// a client timeout or an unresolved append is not grounds for discarding it.
    /// Stale/foreign tickets leave the complete chain intact.
    pub fn discard_from(&mut self, candidate: NativeCandidate) -> Result<usize, NativeOwnerError> {
        let position = self.position(candidate)?;
        let count = self
            .pending
            .len()
            .checked_sub(position)
            .ok_or(NativeOwnerError::UnknownCandidate)?;
        while self.pending.len() > position {
            if let Some(row) = self.pending.pop_back() {
                drop(row.prepared);
                if let Err(error) = self.book.rollback(row.journal) {
                    self.faulted = true;
                    return Err(error.into());
                }
            }
        }
        if self.pending.is_empty()
            && let Err(error) = self.book.trim_idle()
        {
            self.faulted = true;
            return Err(error.into());
        }
        Ok(count)
    }

    /// Drop all pending candidates tail first. As with `discard_from`, the
    /// caller must resolve durability before using this to continue admission.
    pub fn discard_all(&mut self) -> usize {
        let count = self.pending.len();
        while let Some(row) = self.pending.pop_back() {
            drop(row.prepared);
            if self.book.rollback(row.journal).is_err() {
                self.faulted = true;
            }
        }
        if !self.faulted && self.book.trim_idle().is_err() {
            self.faulted = true;
        }
        count
    }

    fn position(&self, candidate: NativeCandidate) -> Result<usize, NativeOwnerError> {
        if candidate.owner != self.incarnation {
            return Err(NativeOwnerError::WrongOwner);
        }
        self.pending
            .iter()
            .position(|row| row.candidate == candidate)
            .ok_or(NativeOwnerError::UnknownCandidate)
    }

    pub fn committed(&self) -> NativeView<'_> {
        NativeView(View {
            state: &self.core.state,
            tail: None,
        })
    }

    /// Includes all unpublished work. These observations are provisional and
    /// must not be returned as durable participant acknowledgments.
    pub fn effective(&self) -> NativeView<'_> {
        NativeView(View {
            state: &self.core.state,
            tail: self.pending.back().map(|row| &row.prepared),
        })
    }

    pub fn candidate(
        &self,
        candidate: NativeCandidate,
    ) -> Result<NativeView<'_>, NativeOwnerError> {
        let row = self
            .pending
            .get(self.position(candidate)?)
            .ok_or(NativeOwnerError::UnknownCandidate)?;
        Ok(NativeView(View {
            state: &self.core.state,
            tail: Some(&row.prepared),
        }))
    }

    pub fn budget_stats(&self) -> BudgetStats {
        self.core.native_budget()
    }

    /// Inspect acceptance from the actual effective prefix, including this
    /// owner's pending tail. This read neither begins work nor publishes a
    /// verdict. Its bounded scratch allocation is released after the callback.
    pub fn with_effective_acceptance<T>(
        &self,
        claim: ClaimId,
        project: impl FnOnce(&aggregation::WholeWorkProjection<'_>) -> T,
    ) -> Result<T, NativeOwnerError> {
        Ok(super::projection::with_projection(
            &self.effective().0,
            claim,
            self.core.limits,
            &self.core.state.budget,
            project,
        )?)
    }

    pub fn range_stats(&self) -> RangeStats {
        self.core.native_stats()
    }

    /// Pins committed state only. Speculative pages cannot escape as read leases.
    pub fn pin(&mut self, now: u64, ttl: u64) -> Result<NativeRead, MemoryError> {
        self.core.pin_native(now, ttl)
    }

    pub fn release(&mut self, read: &NativeRead) -> Result<(), MemoryError> {
        self.core.release_native(read)
    }

    pub fn advance_clock(&mut self, now: u64) -> Result<usize, MemoryError> {
        self.core.advance_native_clock(now)
    }
}

/// Borrowed observations from one fixed committed or candidate prefix. The
/// borrow prevents publication/discard while rows are in use and carries no
/// independent allocation, root clone, or authority to prepare another branch.
pub struct NativeView<'a>(View<'a>);
impl<'a> NativeView<'a> {
    pub fn ledger(&self) -> LedgerId {
        self.0.ledger()
    }
    pub fn sequence(&self) -> SessionSeq {
        self.0.prefix()
    }
    pub fn logical_time(&self) -> u64 {
        self.0.meta().logical_time
    }
    pub fn claim(&self, id: ClaimId) -> Option<&'a ClaimState> {
        as_claim(self.0.get(Key::Claim(id)))
    }
    pub fn registrations(&self, id: ClaimId) -> Option<&'a RegistrationSet> {
        match self.0.get(Key::Claim(id)) {
            Some(Row::Claim(row)) => row.registrations(),
            _ => None,
        }
    }
    pub fn recorded(&self, request: RequestKey) -> Option<NativeOutcome> {
        as_outcome(self.0.get(Key::Outcome(request)))
    }
    pub fn definition(&self, id: ValidationId) -> Option<&'a validation::Declaration> {
        as_definition(self.0.get(Key::Definition(id)))
    }
    pub fn evaluation(&self, key: EvaluationKey) -> Option<&'a validation::EvaluationState> {
        as_evaluation(self.0.get(Key::Evaluation(key)))
    }
    pub fn receipt(&self, id: ReceiptId) -> Option<NativeReceipt> {
        as_receipt(self.0.get(Key::Receipt(id)))
    }
    pub fn artifact(&self, id: ArtifactId) -> Option<&'a NativeArtifact> {
        as_artifact(self.0.get(Key::Artifact(id)))
    }
    pub fn work(&self, id: ArtifactId) -> Option<&'a NativeWork> {
        as_work(self.0.get(Key::Work(id)))
    }
    pub fn diagnostic(&self, id: ArtifactId) -> Option<&'a NativeDiagnostic> {
        as_diagnostic(self.0.get(Key::Diagnostic(id)))
    }
    pub fn response(&self, id: TestamentId) -> Option<&'a Response> {
        as_response(self.0.get(Key::Response(id)))
    }
    pub fn missing_result(&self, key: NativeResultKey) -> Option<&'a NativeMissingResult> {
        super::response_reads::as_missing(self.0.get(Key::MissingResult(key)))
    }
    pub fn response_record(&self, id: TestamentId) -> Option<&'a NativeResponseRecord> {
        super::response_reads::as_response_record(self.0.get(Key::Response(id)))
    }
    pub fn delivery_result(&self, key: NativeResultKey) -> Option<&'a NativeDeliveryResult> {
        super::response_reads::as_delivery(self.0.get(Key::DeliveryResult(key)))
    }
    pub fn result(&self, key: NativeResultKey) -> Option<&'a NativeAccepted> {
        as_result(self.0.get(Key::Accepted(key)))
    }
    pub fn event(&self, sequence: SessionSeq, ordinal: u32) -> Option<NativeEvent> {
        match self.0.get(Key::Event(sequence, ordinal)) {
            Some(Row::Event(event)) => event.get().map(|row| row.expand(self.0.ledger())),
            _ => None,
        }
    }
}
