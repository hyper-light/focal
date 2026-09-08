//! Exclusive ownership of the speculative native chain. Tickets identify a
//! candidate but cannot retain its pages, fork it, or transfer its resources.

use super::completion_book::{
    CandidateJournal, CompletionBook, GraphMembers, JournalFunding, ReportAdvance,
};
use super::completion_envelope::{
    CompletionEnvelope, EvidenceBounds, ReportParent, descriptor_limits,
};
use super::completion_schemas::SchemaSet;
use super::*;
use focal_evidence::{
    BuiltinNativeSchemas, ContentStore, NativeSchemaVerifier, VerifiedNativeArtifact,
};
use focal_memory::{Allocation, BudgetKind, BudgetLane, OwnerId};
use focal_model::ContentDomainId;
use focal_model::lifecycle::aggregation;
use std::collections::VecDeque;

#[path = "owner_ingress.rs"]
mod ingress;
#[path = "respondent_owner.rs"]
mod respondent;

fn check_revision_capacity(
    binding: Binding,
    reports: u32,
    unsealed: bool,
) -> Result<(), NativeError> {
    binding
        .revision
        .0
        .checked_add(u64::from(reports))
        .and_then(|after_reports| after_reports.checked_add(u64::from(unsealed)))
        .ok_or(NativeError::Capacity("completion revision margin"))?;
    Ok(())
}

fn completion_contract(
    view: &View<'_>,
    limits: NativeLimits,
    registered: &super::admission_authority::Registered<'_>,
    schemas: &impl NativeSchemaVerifier,
) -> Result<(CompletionEnvelope, SchemaSet, Option<GraphMembers>), NativeError> {
    let state = view.state;
    // Required failure may advance the parent once. Ordinary mutations must
    // preserve this last revision while an active grant still needs it.
    if registered.definition.mode() == focal_model::ValidationMode::Required
        && registered.definition.target() == validation::TargetDeclaration::Admission
        && registered.parent.status() == focal_model::ClaimStatus::Posted
    {
        registered.parent.binding().next()?;
        super::cohort_budget::check_source(view, registered.parent, registered.registry, limits)?;
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
    let (envelope, members) = if matches!(
        registered.state.target(),
        validation::Target::Artifact { .. }
    ) {
        let (envelope, members) =
            CompletionEnvelope::derive_work(view, limits, registered, descriptor, evidence)?;
        super::work_authority::check_completion_target(view, registered, &envelope, limits)?;
        (envelope, Some(members))
    } else if matches!(
        registered.state.target(),
        validation::Target::Admission { .. }
    ) {
        CompletionEnvelope::derive_admission(view, limits, registered, descriptor, evidence)?
    } else {
        (
            CompletionEnvelope::derive(
                &state.rows,
                limits,
                registered.parent,
                registered.registry,
                registered.definition,
                descriptor,
                evidence,
            )?,
            None,
        )
    };
    if matches!(
        registered.state.target(),
        validation::Target::Increment { .. }
    ) {
        super::increment_authority::check_completion_target(view, registered, &envelope, limits)?;
    }
    envelope.deadline_storage(limits)?;
    Ok((envelope, pins, members))
}

fn build_funded(
    fresh: prepare::Fresh<'_>,
    book: &mut CompletionBook,
    evidence: Option<&VerifiedNativeArtifact>,
    custody: Option<(&mut ContentStore, ContentDomainId)>,
    schemas: &impl NativeSchemaVerifier,
) -> Result<(NativePrepared, CandidateJournal, bool), NativeError> {
    let source = fresh.source();
    let publication_source = fresh.publication_source();
    let lane = fresh.lane();
    if let Some((key, spend)) = respondent::select(&fresh, book, schemas)? {
        return respondent::build(fresh, book, evidence, custody, schemas, key, spend)
            .map(|(prepared, journal)| (prepared, journal, true));
    }
    if let Some(deadline) = fresh.authorize_deadline()? {
        let funding = if deadline.begun {
            JournalFunding::HeldCompletion
        } else {
            JournalFunding::External { source, lane }
        };
        let built = if deadline.begun {
            let loan = book.deadline_contract(deadline.key, deadline.binding)?;
            fresh.build_recorded(loan.source(), None, None, Some(loan.storage()))?
        } else {
            fresh.build_recorded(source, None, None, None)?
        };
        let journal = book.apply_prepared(
            &publication_source,
            built.prepared(),
            None,
            None,
            built.seals(),
            funding,
        )?;
        return Ok((built.into_prepared(), CandidateJournal::single(journal), deadline.begun));
    }
    match fresh.authorize_admission()? {
        Some(prepare::Admission::Begin {
            key,
            registered,
            binding,
            transition,
            active: true,
        }) => {
            check_revision_capacity(
                binding,
                registered.definition.attempt_bound(),
                registered.state.sealed().is_none(),
            )?;
            let (envelope, pins, members) =
                completion_contract(fresh.view(), fresh.limits(), &registered, schemas)?;
            let journal = book.install_begin_with_members(
                key,
                binding,
                envelope,
                pins,
                registered.registration_index,
                members,
            )?;
            let built = if envelope.is_work() {
                fresh.build_recorded(source, evidence, Some(&envelope), None)
            } else {
                fresh.build_recorded(source, evidence, None, None)
            };
            match built {
                Ok(built) => {
                    let update = match book.apply_prepared(
                        &publication_source,
                        built.prepared(),
                        None,
                        Some(&transition),
                        built.seals(),
                        JournalFunding::External { source, lane },
                    ) {
                        Ok(update) => update,
                        Err(error) => {
                            drop(built);
                            book.rollback(journal)?;
                            return Err(error);
                        }
                    };
                    if let Err(error) = book.check_begin_composition(&journal, &update) {
                        drop(built);
                        book.rollback(update)?;
                        book.rollback(journal)?;
                        return Err(error);
                    }
                    Ok((
                        built.into_prepared(),
                        CandidateJournal::begin(journal, update),
                        false,
                    ))
                }
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
            let parent = ReportParent::capture(registered.parent);
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
                    fresh.input()?.request,
                    artifact.get().ok_or(ContractError::MissingEvidence)?,
                    domain,
                    loan.source(),
                    schemas,
                    loan.verification(),
                )?)
            } else {
                None
            };
            let built = fresh.build_recorded(
                loan.source(),
                verified.as_ref().or(evidence),
                Some(loan.envelope()),
                None,
            )?;
            drop(verified);
            let prepared = built.prepared();
            let journal = book.apply_prepared(
                &publication_source,
                prepared,
                Some(ReportAdvance {
                    key,
                    before,
                    usage: parent.completion_use_recorded(prepared)?,
                }),
                None,
                built.seals(),
                JournalFunding::HeldCompletion,
            )?;
            Ok((built.into_prepared(), CandidateJournal::single(journal), true))
        }
        Some(prepare::Admission::Begin {
            transition,
            active: false,
            ..
        }) => {
            let built = fresh.build_recorded(source, evidence, None, None)?;
            let journal = book.apply_prepared(
                &publication_source,
                built.prepared(),
                None,
                Some(&transition),
                built.seals(),
                JournalFunding::External { source, lane },
            )?;
            Ok((built.into_prepared(), CandidateJournal::single(journal), false))
        }
        None => {
            let descriptor = fresh.authorize_work()?;
            let verified = if let (Some(descriptor), Some((store, domain))) = (descriptor, custody)
            {
                Some(store.verify_native_artifact(
                    fresh.input()?.request,
                    descriptor,
                    domain,
                    source,
                    schemas,
                )?)
            } else {
                None
            };
            let built = fresh.build_recorded(source, verified.as_ref().or(evidence), None, None)?;
            drop(verified);
            let journal = book.apply_prepared(
                &publication_source,
                built.prepared(),
                None,
                None,
                built.seals(),
                JournalFunding::External { source, lane },
            )?;
            Ok((built.into_prepared(), CandidateJournal::single(journal), false))
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

#[allow(clippy::large_enum_variant)] // Keep bounded input on stack before memory admission.
enum OwnerInput {
    Request(NativeContext, NativeInput),
    Deadline(NativeDeadlineInput, u64),
    ClaimDeadline(NativeClaimDeadlineInput, u64),
    MonitorDeadline(NativeMonitorDeadlineInput, u64),
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
    #[error("native input: {0}")]
    Input(#[from] super::input_codec::CodecError),
    #[error("native recorded mutation: {0}")]
    Record(#[source] super::record_codec::CodecError),
    #[error("candidate belongs to another native owner incarnation")]
    WrongOwner,
    #[error("native candidate is no longer pending")]
    UnknownCandidate,
    #[error("native candidates must publish in preparation order")]
    OutOfOrder,
    #[error("resolve pending native candidates before taking committed state")]
    PendingCandidates,
}
impl From<super::input_codec::DecodeError> for NativeOwnerError {
    fn from(error: super::input_codec::DecodeError) -> Self {
        match error {
            super::input_codec::DecodeError::Codec(error) => Self::Input(error),
            super::input_codec::DecodeError::Native(error) => Self::Native(error),
        }
    }
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

/// Refused ownership transfer keeps the identical owner and pending tickets.
#[derive(Debug)]
pub struct NativeOwnerIntoCoreError {
    pub error: NativeOwnerError,
    pub owner: NativeOwner,
}
impl std::fmt::Display for NativeOwnerIntoCoreError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { self.error.fmt(f) }
}
impl std::error::Error for NativeOwnerIntoCoreError {}

#[derive(Debug)]
struct Pending {
    candidate: NativeCandidate,
    record: record_codec::PendingRecord,
    prepared: NativePrepared,
    journal: CandidateJournal,
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
    record_buffers: Option<record_codec::EncodingLimits>,
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
// Whole-owner destruction needs no journal rollback: pending records/pages
// drop before Core and book, and every pool permit keeps its actual backing.
// Explicit suffix discard still rolls back journals before further admission.

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
        Self::with_record_profile(core, schemas, None)
    }

    /// Fund one retained encoded mutation buffer for each pending operation.
    /// Existing evaluator/respondent responsibilities additionally reserve their
    /// finite future record buffers before this owner accepts new traffic.
    /// Transport, consensus staging, WAL admission and disk capacity remain the
    /// enclosing service's separate obligations; refusal there retains tickets.
    pub fn with_record_buffers(
        core: Core<NativeState>,
        schemas: &impl NativeSchemaVerifier,
        encoding: record_codec::EncodingLimits,
    ) -> Result<Self, NativeOwnerInitError> {
        Self::with_record_profile(core, schemas, Some(encoding))
    }

    fn with_record_profile(
        core: Core<NativeState>,
        schemas: &impl NativeSchemaVerifier,
        record_buffers: Option<record_codec::EncodingLimits>,
    ) -> Result<Self, NativeOwnerInitError> {
        let resources = (|| {
            super::authored::check_storage(&core)?;
            let (incarnation, pending, allocation) = Self::allocate_queue(&core)?;
            let mut book = CompletionBook::new(&core.state.budget, core.limits)?;
            if let Some(limits) = record_buffers { book = book.with_record_buffers(limits); }
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
                check_revision_capacity(state.binding(), remaining, state.sealed().is_none())?;
                let (envelope, pins, members) =
                    completion_contract(&view, core.limits, &registered, schemas)?;
                book.install_recovered_with_members(
                    key,
                    state.binding(),
                    remaining,
                    envelope,
                    pins,
                    registered.registration_index,
                    members,
                )?;
            }
            for entry in core.state.rows.entries() {
                let Key::Claim(id) = entry.key else {
                    continue;
                };
                let claim = view.claim(id).ok_or(ContractError::InvalidTarget)?;
                let Some((_, credit)) = super::respondent_state::read(&view, claim, core.limits)?
                else {
                    continue;
                };
                if credit.actions()? != 0 {
                    let (envelope, verification) =
                        respondent::contract(&view, claim, core.limits, schemas)?;
                    book.install_recovered_respondent(&view, claim, &envelope, verification)?;
                }
            }
            book.check_slots(view.meta(), view.prefix(), core.state.rows.len())?;
            Ok::<_, NativeError>((incarnation, pending, allocation, book))
        })();
        match resources {
            Ok((incarnation, pending, allocation, book)) => Ok(Self {
                pending,
                core,
                book,
                record_buffers,
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
        self.prepare_using(
            OwnerInput::Request(context, input),
            evidence,
            None,
            &BuiltinNativeSchemas,
        )
    }

    /// Trusted timer ingress, separate from participant commands. The input
    /// identifies an authored timer; time comes from the publishing owner and
    /// cannot be decoded from an agent's request. The committed typed timer
    /// outcome also deduplicates firings after the evaluation has advanced.
    pub fn prepare_evaluation_deadline(
        &mut self,
        input: NativeDeadlineInput,
        logical_time: u64,
    ) -> Result<NativeStaging, NativeOwnerError> {
        self.prepare_using(
            OwnerInput::Deadline(input, logical_time),
            None,
            None,
            &BuiltinNativeSchemas,
        )
    }

    /// Trusted claim deadline ingress. The owner discovers the complete
    /// affected graph and applies canonical deadlock precedence before expiry.
    /// Participants cannot supply its clock, victim, peers or failure status.
    pub fn prepare_claim_deadline(
        &mut self,
        input: NativeClaimDeadlineInput,
        logical_time: u64,
    ) -> Result<NativeStaging, NativeOwnerError> {
        self.prepare_using(
            OwnerInput::ClaimDeadline(input, logical_time),
            None,
            None,
            &BuiltinNativeSchemas,
        )
    }

    /// Deliver the exact named monitor timer using owner-observed logical time.
    /// This cannot be requested by inventing a participant principal.
    pub fn prepare_monitor_deadline(
        &mut self,
        input: NativeMonitorDeadlineInput,
        logical_time: u64,
    ) -> Result<NativeStaging, NativeOwnerError> {
        self.prepare_using(
            OwnerInput::MonitorDeadline(input, logical_time),
            None,
            None,
            &BuiltinNativeSchemas,
        )
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
        self.prepare_using(
            OwnerInput::Request(context, input),
            None,
            Some((store, inline_domain)),
            schemas,
        )
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
        self.prepare_using(OwnerInput::Request(context, input), evidence, None, schemas)
    }

    fn prepare_using(
        &mut self,
        input: OwnerInput,
        evidence: Option<&VerifiedNativeArtifact>,
        custody: Option<(&mut ContentStore, ContentDomainId)>,
        schemas: &impl NativeSchemaVerifier,
    ) -> Result<NativeStaging, NativeOwnerError> {
        let pending = self.pending.iter().map(|row| &row.prepared);
        let preparation = match input {
            OwnerInput::Request(context, input) => {
                self.core.check_native_chain(context, input, pending)?
            }
            OwnerInput::Deadline(input, logical_time) => {
                self.core
                    .check_deadline_chain(input, logical_time, pending)?
            }
            OwnerInput::ClaimDeadline(input, logical_time) => self
                .core
                .check_claim_deadline_chain(input, logical_time, pending)?,
            OwnerInput::MonitorDeadline(input, logical_time) => self
                .core
                .check_monitor_deadline_chain(input, logical_time, pending)?,
        };
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
                let lane = fresh.lane();
                let (prepared, journal, held_record) =
                    build_funded(fresh, &mut self.book, evidence, custody, schemas)?;
                let source = View {
                    state: &self.core.state,
                    tail: self.pending.back().map(|row| &row.prepared),
                };
                let (prepared, journal) = respondent::attach(
                    &source,
                    prepared,
                    journal,
                    &mut self.book,
                    self.core.limits,
                    schemas,
                )?;
                let view = View {
                    state: &self.core.state,
                    tail: Some(&prepared),
                };
                let checked = self
                    .book
                    .check_slots(view.meta(), view.prefix(), prepared.range.len())
                    .and_then(|()| self.book.check_serial(serial))
                    .and_then(|()| self.book.check_parents(&view, &prepared))
                    .and_then(|()| self.book.check_graph_growth(&view, &prepared));
                if let Err(error) = checked {
                    drop(prepared);
                    if let Err(rollback) = self.book.rollback_candidate(journal) {
                        self.faulted = true;
                        return Err(rollback.into());
                    }
                    return Err(error.into());
                }
                let record_source = if held_record { self.book.source() } else { &self.core.state.budget };
                let record = match record_codec::PendingRecord::reserve(&prepared, record_source,
                    if held_record { BudgetLane::Completion } else { lane }, self.record_buffers) {
                    Ok(record) => record,
                    Err(error) => {
                        drop(prepared);
                        if let Err(rollback) = self.book.rollback_candidate(journal) {
                            self.faulted = true;
                            return Err(rollback.into());
                        }
                        return Err(error);
                    }
                };
                let candidate = NativeCandidate {
                    owner: self.incarnation,
                    serial,
                };
                let outcome = prepared.outcome();
                self.pending.push_back(Pending {
                    candidate,
                    record,
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
                drop(head.record);
                if let Err(error) = self.book.commit_candidate(head.journal) {
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
                    record: head.record,
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
                drop(row.record);
                drop(row.prepared);
                if let Err(error) = self.book.rollback_candidate(row.journal) {
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
            drop(row.record);
            drop(row.prepared);
            if self.book.rollback_candidate(row.journal).is_err() {
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

    /// Borrow the actual committed root for streaming checkpoint encoding.
    /// Pending rows remain isolated; this borrow prevents concurrent mutation.
    pub fn committed_core(&self) -> &Core<NativeState> { &self.core }

    /// Transfer a fully reconciled committed Core without allocating or copying
    /// rows. A pending or faulted owner is returned unchanged for reconciliation.
    pub fn into_committed_core(self) -> Result<Core<NativeState>, NativeOwnerIntoCoreError> {
        let error = if !self.pending.is_empty() { Some(NativeOwnerError::PendingCandidates) }
            else if self.faulted { Some(NativeError::Capacity("completion owner requires reconstruction").into()) }
            else { None };
        if let Some(error) = error { return Err(NativeOwnerIntoCoreError { error, owner: self }); }
        let Self { pending, core, book, _queue_allocation, .. } = self;
        drop(pending);
        drop(book);
        drop(_queue_allocation);
        Ok(core)
    }

    /// Encode the exact pending mutation using its preheld buffer permit. The
    /// cached bytes remain owned by this ticket through retries; this does not
    /// publish or acknowledge durability. A low work/byte cap or allocation
    /// failure leaves the candidate and its permit available for retry.
    pub fn encode_candidate(&mut self, candidate: NativeCandidate, limits: record_codec::EncodingLimits)
        -> Result<&record_codec::FundedRecord, NativeOwnerError>
    {
        let position = self.position(candidate)?;
        let pending = self.pending.get_mut(position).ok_or(NativeOwnerError::UnknownCandidate)?;
        pending.record.encode(&pending.prepared, limits)
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
        let prepared = self.prepared_candidate(candidate)?;
        Ok(NativeView(View {
            state: &self.core.state,
            tail: Some(prepared),
        }))
    }

    /// Borrow the exact unpublished mutation owned by this ticket. A future
    /// durable writer must encode this candidate and await its actual barrier;
    /// inspecting it does not publish or acknowledge any operation.
    pub fn prepared_candidate(
        &self,
        candidate: NativeCandidate,
    ) -> Result<&NativePrepared, NativeOwnerError> {
        self.pending
            .get(self.position(candidate)?)
            .map(|row| &row.prepared)
            .ok_or(NativeOwnerError::UnknownCandidate)
    }

    pub fn budget_stats(&self) -> BudgetStats {
        self.core.native_budget()
    }

    #[cfg(test)]
    pub(super) fn budget_for_test(&self) -> &MemoryBudget {
        &self.core.state.budget
    }

    #[cfg(test)]
    pub(super) fn core_for_test(&self) -> &Core<NativeState> {
        &self.core
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

    /// Inspect the complete audit at this owner's effective prefix. This read
    /// does not freeze or post a claimant result testament.
    pub fn with_effective_audit<T>(
        &self,
        claim: ClaimId,
        project: impl FnOnce(&NativeAudit) -> T,
    ) -> Result<T, NativeOwnerError> {
        Ok(super::audit::with_audit(
            &self.effective().0,
            claim,
            self.core.limits,
            project,
        )?)
    }

    /// Inspect committed audit history while preserving pending isolation.
    pub fn with_committed_audit<T>(
        &self,
        claim: ClaimId,
        project: impl FnOnce(&NativeAudit) -> T,
    ) -> Result<T, NativeOwnerError> {
        Ok(super::audit::with_audit(
            &self.committed().0,
            claim,
            self.core.limits,
            project,
        )?)
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
pub struct NativeView<'a>(pub(super) View<'a>);
impl<'a> NativeView<'a> {
    /// Borrow the exact source for isolated native transaction verification.
    /// Tests cannot mutate it or acquire an independently owned range root.
    #[cfg(test)]
    pub(super) fn source_view(&self) -> &View<'a> {
        &self.0
    }

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
    pub fn recorded(&self, invocation: impl Into<NativeInvocation>) -> Option<NativeOutcome> {
        as_outcome(self.0.get(Key::Outcome(invocation.into())))
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
    pub fn result_testament(&self, id: TestamentId) -> Option<&'a NativeResultTestament> {
        super::audit_bundle::as_result_testament(self.0.get(Key::ResultTestament(id)))
    }
    pub fn claim_result_testament(&self, claim: ClaimId) -> Option<&'a NativeResultTestament> {
        let id = super::audit_bundle::index(self.0.get(Key::ClaimResultTestament(claim)))?;
        self.result_testament(id)
            .filter(|row| row.testament().claim() == claim)
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
