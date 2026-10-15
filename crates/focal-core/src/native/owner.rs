//! Exclusive ownership of the speculative native chain. Tickets identify a
//! candidate but cannot retain its pages, fork it, or transfer its resources.

use super::completion_book::{
    CandidateJournal, CompletionBook, GraphMembers, JournalFunding, ReportAdvance,
};
use super::completion_envelope::{CompletionEnvelope, ReportParent, descriptor_limits};
use super::completion_schemas::SchemaSet;
use super::pending_index::{self, Chain, PendingIndex};
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
    let (envelope, members) = if matches!(
        registered.state.target(),
        validation::Target::Artifact { .. }
    ) {
        let (envelope, members) =
            CompletionEnvelope::derive_work(view, limits, registered, descriptor)?;
        super::work_authority::check_completion_target(view, registered, &envelope, limits)?;
        (envelope, Some(members))
    } else if matches!(
        registered.state.target(),
        validation::Target::Admission { .. }
    ) {
        CompletionEnvelope::derive_admission(view, limits, registered, descriptor)?
    } else {
        (
            CompletionEnvelope::derive(
                &state.rows,
                limits,
                registered.parent,
                registered.registry,
                registered.definition,
                descriptor,
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
        // A begun evaluation's due timer finishes work already taken: charged as it is written, on
        // the completion lane.
        let funding = if deadline.begun {
            JournalFunding::External {
                source,
                lane: BudgetLane::Completion,
            }
        } else {
            JournalFunding::External { source, lane }
        };
        let built = if deadline.begun {
            let loan = book.deadline_contract(deadline.key, deadline.binding)?;
            fresh.build_recorded(source, None, None, Some(loan.storage()))?
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
        return Ok((
            built.into_prepared(),
            CandidateJournal::single(journal),
            deadline.begun,
        ));
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
                    source,
                    schemas,
                    loan.verification(),
                )?)
            } else {
                None
            };
            let built = fresh.build_recorded(
                source,
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
                    usage: parent
                        .completion_use_recorded(prepared, publication_source.payloads())?,
                }),
                None,
                built.seals(),
                JournalFunding::External {
                    source,
                    lane: BudgetLane::Completion,
                },
            )?;
            Ok((
                built.into_prepared(),
                CandidateJournal::single(journal),
                true,
            ))
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
            Ok((
                built.into_prepared(),
                CandidateJournal::single(journal),
                false,
            ))
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
            Ok((
                built.into_prepared(),
                CandidateJournal::single(journal),
                false,
            ))
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
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.error.fmt(f)
    }
}
impl std::error::Error for NativeOwnerIntoCoreError {}

#[derive(Debug)]
pub(super) struct Pending {
    candidate: NativeCandidate,
    record: record_codec::PendingRecord,
    prepared: NativePrepared,
    journal: CandidateJournal,
}

impl Pending {
    pub(super) fn serial(&self) -> u64 {
        self.candidate.serial
    }
    pub(super) fn prepared(&self) -> &NativePrepared {
        &self.prepared
    }
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
/// [`NativeOwner::checkpoint_projection`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CheckpointProjection {
    pub rows: usize,
    pub bytes: usize,
}

pub struct NativeOwner {
    // Drop buffers and retained pages before releasing their queue allowance.
    pending: VecDeque<Pending>,
    /// Every pending candidate's changes by key: what a read during
    /// preparation finds before the committed rows (doc 30 §9).
    index: PendingIndex,
    /// The index's root node, which its candidates' charges do not cover.
    _index_root: Allocation,
    core: Core<NativeState>,
    book: CompletionBook,
    record_buffers: Option<record_codec::EncodingLimits>,
    faulted: bool,
    _queue_allocation: Allocation,
    incarnation: OwnerId,
    next_serial: u64,
    /// What the pending candidates add to a checkpoint, kept as the chain changes: their writes
    /// and their encoded bytes ([`NativeOwner::checkpoint_projection`]). Summed over the chain at
    /// each admission, it was a walk of the whole pipeline for every write, quadratic in its depth
    /// once the depth was the memory's to bound ([`NativeOwner::reserve_pending`]). Bounded by
    /// what the pending candidates are charged, so a sum never nears `usize::MAX`; it saturates
    /// rather than wrap, which could only overstate it and refuse admission.
    pending_rows: usize,
    pending_bytes: usize,
    /// The payload log's hold over what the newest candidate's preparation appended, while its
    /// admission is weighed, and the candidate's serial ([`NativeOwner::keep_appends`]).
    held: Option<(u64, super::payloads::PayloadHold)>,
    /// The last fresh request of each of a few commands that this owner's
    /// memory refused, while the headroom stays short of what it needed:
    /// another like it is refused from its header
    /// ([`NativeOwner::refuse_when_full`]).
    shortfalls: [Option<ingress::Shortfall>; ingress::SHORTFALLS],
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
/// The rows the pending candidates put: what a sweep keeps the segments of beside the committed
/// rows' (doc 31 M3).
fn pending_rows(pending: &VecDeque<Pending>) -> impl Iterator<Item = &Row> {
    pending.iter().flat_map(|row| {
        row.prepared
            .fragments
            .changes()
            .filter_map(|(_, _, change)| match change {
                super::ranges::RowChange::Put(entry) => Some(entry.value),
                super::ranges::RowChange::Delete(_) => None,
            })
    })
}

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
    #[allow(clippy::result_large_err)] // Preserve the original Core without allocation on refusal.
    pub fn with_record_buffers(
        core: Core<NativeState>,
        schemas: &impl NativeSchemaVerifier,
        encoding: record_codec::EncodingLimits,
    ) -> Result<Self, NativeOwnerInitError> {
        Self::with_record_profile(core, schemas, Some(encoding))
    }

    #[allow(clippy::result_large_err)] // Preserve the original Core without allocation on refusal.
    fn with_record_profile(
        core: Core<NativeState>,
        schemas: &impl NativeSchemaVerifier,
        record_buffers: Option<record_codec::EncodingLimits>,
    ) -> Result<Self, NativeOwnerInitError> {
        let resources = (|| {
            super::authored::check_storage(&core)?;
            let (incarnation, pending, allocation) = Self::allocate_queue(&core)?;
            let index_root = core
                .state
                .budget
                .reserve(
                    BudgetKind::Pending,
                    BudgetLane::Ordinary,
                    pending_index::root_charge(),
                )?
                .commit();
            let mut book = CompletionBook::new(&core.state.budget, core.limits)?;
            if let Some(limits) = record_buffers {
                book = book.with_record_buffers(limits);
            }
            let view = View::committed(&core.state);
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
                // A spilled claim waits to be delivered (`OwnedClaim::spill`): it holds no
                // receipt, so no respondent's work, and is not read back for none (doc 31 M3).
                if matches!(entry.value, Row::Claim(claim) if claim.logged().is_some()) {
                    continue;
                }
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
            // What is committed may stand anywhere within the bounds.
            book.check_slots(
                view.meta(),
                view.prefix(),
                core.state.rows.len(),
                BudgetLane::Completion,
            )?;
            Ok::<_, NativeError>((incarnation, pending, allocation, index_root, book))
        })();
        match resources {
            Ok((incarnation, pending, allocation, index_root, book)) => Ok(Self {
                pending,
                index: PendingIndex::default(),
                _index_root: index_root,
                core,
                book,
                record_buffers,
                faulted: false,
                _queue_allocation: allocation,
                incarnation,
                next_serial: 0,
                pending_rows: 0,
                pending_bytes: 0,
                held: None,
                shortfalls: [None; ingress::SHORTFALLS],
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

    /// Room for one more pending candidate: a full queue doubles its capacity, the growth charged
    /// to the core's budget before it is allocated. The candidates a leader holds in flight are as
    /// many as its memory funds, never a count picked for them: a queue fixed at
    /// `NativeLimits::pending` held a session on hyper-durable's shell at 355 writes a second
    /// against a 55 ms commit, every write past the 32nd in flight refused (Little's law,
    /// 2026-10-12). That limit remains the committed headroom a seal's outcome is checked
    /// against, which every replica replays alike; the depth of one leader's pipeline is its own.
    pub(super) fn reserve_pending(&mut self) -> Result<(), NativeError> {
        self.reserve_pending_on(BudgetLane::Ordinary)
    }
    /// [`Self::reserve_pending`] with the growth charged to `lane`.
    fn reserve_pending_on(&mut self, lane: BudgetLane) -> Result<(), NativeError> {
        if self.pending.len() < self.pending.capacity() {
            return Ok(());
        }
        let wanted = self
            .pending
            .capacity()
            .max(1)
            .checked_mul(2)
            .ok_or(MemoryError::CounterExhausted("native candidate queue"))?;
        let bytes = prepare::array::<Pending>(wanted)?;
        let reservation = self
            .core
            .state
            .budget
            .reserve(BudgetKind::Pending, lane, bytes)?;
        self.pending
            .try_reserve_exact(wanted.saturating_sub(self.pending.len()))
            .map_err(|_| MemoryError::AllocationFailed)?;
        // Allocator rounding cannot silently widen the charge.
        prepare::within(prepare::array::<Pending>(self.pending.capacity())?, bytes)?;
        self._queue_allocation = reservation.commit();
        Ok(())
    }

    /// Gives back what a burst grew the chain to once it has drained to a quarter of it: the chain
    /// moves into a buffer half as large, never below the committed headroom it began at, and its
    /// charge shrinks to match. Kept grown, every session that once ran deep would hold that
    /// memory from its rows for good. A buffer the allocator cannot give leaves the chain as it
    /// is, and a charge that cannot shrink stays as it was, more than the chain holds: neither
    /// is a failure of the publication that drained the chain.
    fn shrink_pending(&mut self) {
        let capacity = self.pending.capacity();
        let floor = self.core.limits.pending;
        if capacity <= floor || self.pending.len() > capacity / 4 {
            return;
        }
        let wanted = (capacity / 2).max(floor);
        let mut smaller = VecDeque::new();
        if smaller.try_reserve_exact(wanted).is_err() {
            return;
        }
        // Within the reserved capacity: no reallocation.
        smaller.extend(self.pending.drain(..));
        self.pending = smaller;
        if let Ok(bytes) = prepare::array::<Pending>(self.pending.capacity()) {
            let _ = self._queue_allocation.shrink_to(bytes);
        }
    }

    /// Adds `prepared` to what the pending candidates add to a checkpoint.
    fn count_pending(&mut self, prepared: &NativePrepared) {
        self.pending_rows = self.pending_rows.saturating_add(prepared.mutation_count());
        self.pending_bytes = self.pending_bytes.saturating_add(prepared.encoded_added);
    }

    /// Takes `prepared` from what the pending candidates add to a checkpoint.
    fn uncount_pending(&mut self, prepared: &NativePrepared) {
        self.pending_rows = self.pending_rows.saturating_sub(prepared.mutation_count());
        self.pending_bytes = self.pending_bytes.saturating_sub(prepared.encoded_added);
    }

    /// The newest candidate is admitted: what its preparation appended to the payload log is the
    /// log's ([`super::payloads::PayloadLog::hold`]). Its session says so once it has weighed it;
    /// another preparation, a retirement staged, a publication and the log detached each keep it
    /// first, so the log holds back only what a candidate still being weighed appended.
    pub fn keep_appends(&mut self) {
        if let Some((_, hold)) = self.held.take()
            && let Some(log) = self.core.state.payloads.as_deref()
        {
            log.keep(hold);
        }
    }

    /// What the newest candidate appended to the payload log, given back where it is among the
    /// candidates from serial `from` on that are discarded: the log as if it was never prepared.
    fn give_back_appends(&mut self, from: u64) {
        if let Some((serial, hold)) = self.held
            && serial >= from
        {
            self.held = None;
            if let Some(log) = self.core.state.payloads.as_deref() {
                log.give_back(hold);
            }
        }
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

    /// Prepare the retirement of the families `offers` name (doc 31 M2): planned
    /// ([`Self::plan_retirement`]) and its first candidate staged
    /// ([`Self::stage_retirement`]); every family past it is deferred. What a caller that
    /// stages one candidate at a time uses; a session stages them all while it has room.
    pub fn prepare_retirement(
        &mut self,
        offers: &[retirement_batch::RetireOffer],
        cold: Option<record_codec::EncodingLimits>,
    ) -> Result<(retirement_batch::RetireBatch, Option<NativeStaging>), NativeOwnerError> {
        let mut batch = self.plan_retirement(offers, cold)?;
        let staged = self.stage_retirement(&batch, 0)?;
        batch.keep(usize::from(staged.is_some()))?;
        Ok((batch, staged))
    }

    /// Plan the retirement of the families `offers` name (doc 31 M2) on the chain as it stands:
    /// each derived once, a family a pending candidate touches deferred and a refused one saying
    /// why, the rest packed into candidates each within the store's bound on a batch's changes.
    /// Nothing is staged; [`Self::stage_retirement`] stages each candidate in turn, ordered with
    /// every write before and after it, so nothing waits for it and it waits for nothing. Its
    /// record carries its families' roots and bundles, and every replica derives the rest.
    ///
    /// Under `cold` (doc 31 M2, A2), each candidate's record is a cold one: each family's bundle
    /// is its frame at its candidate's sequence (the chain's next for the first), encoded under
    /// those bounds, by its digest and length.
    pub fn plan_retirement(
        &mut self,
        offers: &[retirement_batch::RetireOffer],
        cold: Option<record_codec::EncodingLimits>,
    ) -> Result<retirement_batch::RetireBatch, NativeOwnerError> {
        // The first record's own sequence, the chain's next: the prefix its frames claim.
        let through = self.next_sequence()?;
        let plan = cold.map(|limits| retirement_batch::ColdPlan { through, limits });
        Ok(self.core.plan_retirement_batch(
            offers,
            Some(Chain::indexed(&self.index, &self.pending, u64::MAX)),
            plan,
        )?)
    }

    /// The sequence the next candidate claims: one past the chain's prefix.
    fn next_sequence(&self) -> Result<SessionSeq, NativeOwnerError> {
        Ok(View::over(
            &self.core.state,
            Some(Chain::indexed(&self.index, &self.pending, u64::MAX)),
        )
        .prefix()
        .0
        .checked_add(1)
        .map(SessionSeq)
        .ok_or(NativeError::Capacity("native prefix"))?)
    }

    /// Stage candidate `index` of `batch` ([`Self::plan_retirement`]) as the next candidate:
    /// none when it holds no family. A cold candidate is staged only at the sequence its frames
    /// were quoted at, the plan's candidates one after another with nothing between them.
    pub fn stage_retirement(
        &mut self,
        batch: &retirement_batch::RetireBatch,
        index: usize,
    ) -> Result<Option<NativeStaging>, NativeOwnerError> {
        self.keep_appends();
        let Some(families) = batch
            .candidate(index)
            .filter(|families| !families.is_empty())
        else {
            return Ok(None);
        };
        // A retirement gives back more than it takes once it applies: it is finishing work, staged
        // on the completion lane fresh writes cannot take, its room in the chain as well. Staged
        // on the ordinary lane, a session its writes had filled refused the candidates that would
        // have emptied it (1.4 to 2.9 MB of pages each, beside writes of a few kilobytes; a
        // 1 GiB session's walks failed hundreds of times a minute).
        let room = self.reserve_pending_on(BudgetLane::Completion).is_ok();
        if let Some(through) = batch.through {
            let quoted = u64::try_from(index)
                .ok()
                .and_then(|index| through.0.checked_add(index))
                .ok_or(NativeError::Capacity("native prefix"))?;
            if self.next_sequence()?.0 != quoted {
                return Err(NativeError::Capacity(
                    "a cold retirement staged at another sequence than its frames claim",
                )
                .into());
            }
        }
        if self.faulted {
            return Err(NativeError::Capacity("completion owner requires reconstruction").into());
        }
        if let Err(error) = self.book.check_health() {
            self.faulted = true;
            return Err(error.into());
        }
        let serial = self
            .next_serial
            .checked_add(1)
            .ok_or(MemoryError::CounterExhausted("native candidate serial"))?;
        if !room {
            return Err(NativeError::Capacity("pending candidates").into());
        }
        let lane = BudgetLane::Completion;
        let source = View::over(
            &self.core.state,
            Some(Chain::indexed(&self.index, &self.pending, u64::MAX)),
        );
        let prepared = self.core.prepare_retirement_batch(
            &source,
            families,
            if batch.through.is_some() {
                retirement_batch::RETIRE_COLD
            } else {
                retirement_batch::RETIRE_CUSTODY
            },
            &self.core.state.budget,
            lane,
        )?;
        let journal = self.book.apply_prepared(
            &source,
            &prepared,
            None,
            None,
            &[],
            JournalFunding::External {
                source: &self.core.state.budget,
                lane,
            },
        )?;
        let journal = CandidateJournal::single(journal);
        let checked = source.with_own(&prepared).and_then(|view| {
            self.book
                .check_slots(view.meta(), view.prefix(), prepared.fragments.len(), lane)
                .and_then(|()| self.book.check_serial(serial))
                .and_then(|()| self.book.check_parents(&view, &prepared))
                .and_then(|()| self.book.check_graph_growth(&view, &prepared))
        });
        let record = match checked {
            Ok(()) => record_codec::PendingRecord::reserve(
                &prepared,
                &self.core.state.budget,
                lane,
                self.record_buffers,
                self.core.state.payloads(),
            ),
            Err(error) => Err(error.into()),
        };
        let record = match record {
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
        self.index.add(serial, &prepared.fragments);
        self.count_pending(&prepared);
        self.pending.push_back(Pending {
            candidate,
            record,
            prepared,
            journal,
        });
        self.next_serial = serial;
        Ok(Some(NativeStaging::Prepared { candidate, outcome }))
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
        // A candidate before it that was not discarded was admitted. What this one's plan spills
        // is held until its session admits it: a refusal, here or there, gives it back.
        self.keep_appends();
        let hold = self
            .core
            .state
            .payloads
            .as_deref()
            .and_then(|log| log.hold());
        let staged = self.prepare_held(input, evidence, custody, schemas);
        if let Some(hold) = hold {
            match &staged {
                Ok(NativeStaging::Prepared { candidate, .. }) => {
                    self.held = Some((candidate.serial, hold));
                }
                // An exact retry appended nothing; a refused candidate leaves nothing.
                Ok(NativeStaging::Existing { .. }) | Err(_) => {
                    if let Some(log) = self.core.state.payloads.as_deref() {
                        log.give_back(hold);
                    }
                }
            }
        }
        staged
    }

    fn prepare_held(
        &mut self,
        input: OwnerInput,
        evidence: Option<&VerifiedNativeArtifact>,
        custody: Option<(&mut ContentStore, ContentDomainId)>,
        schemas: &impl NativeSchemaVerifier,
    ) -> Result<NativeStaging, NativeOwnerError> {
        // What earlier operations read back is theirs alone (doc 31 M3).
        self.core.state.reads.release();
        // Room for the candidate, made before its chain is borrowed: a retry is answered whatever
        // the memory says, a fresh request is refused only where no room could be funded.
        let room = self.reserve_pending().is_ok();
        let pending = Some(Chain::indexed(&self.index, &self.pending, u64::MAX));
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
        // A spilled row its checks could not take back was lent as it is stored: they stand only
        // where none failed (doc 31 M3).
        self.core.state.reads.failure()?;
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
                if !room {
                    return Err(NativeError::Capacity("pending candidates").into());
                }
                let lane = fresh.lane();
                let (prepared, journal, held_record) =
                    build_funded(fresh, &mut self.book, evidence, custody, schemas)?;
                let source = View::over(
                    &self.core.state,
                    Some(Chain::indexed(&self.index, &self.pending, u64::MAX)),
                );
                let (prepared, journal) = respondent::attach(
                    &source,
                    prepared,
                    journal,
                    &mut self.book,
                    self.core.limits,
                    schemas,
                )?;
                let checked = source.with_own(&prepared).and_then(|view| {
                    self.book
                        .check_slots(view.meta(), view.prefix(), prepared.fragments.len(), lane)
                        .and_then(|()| self.book.check_serial(serial))
                        .and_then(|()| self.book.check_parents(&view, &prepared))
                        .and_then(|()| self.book.check_graph_growth(&view, &prepared))
                        .and_then(|()| self.core.state.reads.failure())
                });
                if let Err(error) = checked {
                    drop(prepared);
                    if let Err(rollback) = self.book.rollback_candidate(journal) {
                        self.faulted = true;
                        return Err(rollback.into());
                    }
                    return Err(error.into());
                }
                // Completion work's record is charged on the completion lane, every record to the
                // state's budget: nothing was set aside for it.
                let record = match record_codec::PendingRecord::reserve(
                    &prepared,
                    &self.core.state.budget,
                    if held_record {
                        BudgetLane::Completion
                    } else {
                        lane
                    },
                    self.record_buffers,
                    self.core.state.payloads(),
                ) {
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
                self.index.add(serial, &prepared.fragments);
                self.count_pending(&prepared);
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
        // Published, a row's bytes must be in the log, and the publication may append.
        // A rejected ticket leaves the latest candidate's appends available to discard.
        self.keep_appends();
        let head = self
            .pending
            .pop_front()
            .ok_or(NativeOwnerError::UnknownCandidate)?;
        // Its changes are about to be the committed rows.
        self.index
            .remove_published(head.candidate.serial, &head.prepared.fragments);
        self.uncount_pending(&head.prepared);
        match self.core.publish_native(head.prepared) {
            Ok(outcome) => {
                drop(head.record);
                self.shrink_pending();
                if let Err(error) = self.book.commit_candidate(head.journal) {
                    self.faulted = true;
                    return Err(error.into());
                }
                Ok(outcome)
            }
            Err(refused) => {
                let Some(prepared) = refused.prepared else {
                    // Refused past its checks: a store is poisoned and the
                    // candidate is gone. Nothing published can be trusted.
                    self.faulted = true;
                    let _ = self.book.rollback_candidate(head.journal);
                    return Err(refused.error.into());
                };
                // pop_front retained this exact slot: restoration cannot grow
                // the buffer, allocate, change the ticket, or reorder the chain.
                self.index
                    .restore_oldest(head.candidate.serial, &prepared.fragments);
                self.count_pending(&prepared);
                self.pending.push_front(Pending {
                    candidate: head.candidate,
                    record: head.record,
                    prepared,
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
        self.give_back_appends(candidate.serial);
        let count = self
            .pending
            .len()
            .checked_sub(position)
            .ok_or(NativeOwnerError::UnknownCandidate)?;
        while self.pending.len() > position {
            if let Some(row) = self.pending.pop_back() {
                let pending = &self.pending;
                self.index
                    .remove_discarded(row.candidate.serial, &row.prepared.fragments, |key| {
                        pending_index::newest_among(pending, key)
                    });
                self.uncount_pending(&row.prepared);
                drop(row.record);
                drop(row.prepared);
                if let Err(error) = self.book.rollback_candidate(row.journal) {
                    self.faulted = true;
                    return Err(error.into());
                }
            }
        }
        Ok(count)
    }

    /// Drop all pending candidates tail first. As with `discard_from`, the
    /// caller must resolve durability before using this to continue admission.
    pub fn discard_all(&mut self) -> usize {
        self.give_back_appends(0);
        let count = self.pending.len();
        while let Some(row) = self.pending.pop_back() {
            let pending = &self.pending;
            self.index
                .remove_discarded(row.candidate.serial, &row.prepared.fragments, |key| {
                    pending_index::newest_among(pending, key)
                });
            self.uncount_pending(&row.prepared);
            drop(row.record);
            drop(row.prepared);
            if self.book.rollback_candidate(row.journal).is_err() {
                self.faulted = true;
            }
        }
        count
    }

    fn position(&self, candidate: NativeCandidate) -> Result<usize, NativeOwnerError> {
        if candidate.owner != self.incarnation {
            return Err(NativeOwnerError::WrongOwner);
        }
        // The chain is in serial order: each candidate takes the next serial at its back, the head
        // leaves at its front, and a discard takes a suffix. So a candidate is found by halving,
        // not by a walk, which for the candidate just prepared at the back was the whole chain.
        self.pending
            .binary_search_by_key(&candidate.serial, |row| row.candidate.serial)
            .ok()
            .filter(|&at| {
                self.pending
                    .get(at)
                    .is_some_and(|row| row.candidate == candidate)
            })
            .ok_or(NativeOwnerError::UnknownCandidate)
    }

    pub fn committed(&self) -> NativeView<'_> {
        NativeView(View::committed(&self.core.state))
    }

    /// Borrow the actual committed root for streaming checkpoint encoding.
    /// Pending rows remain isolated; this borrow prevents concurrent mutation.
    pub fn committed_core(&self) -> &Core<NativeState> {
        &self.core
    }

    /// The payload log the committed core spills to, given back (for a restored core).
    pub fn detach_payload_log(&mut self) -> Option<Box<dyn super::payloads::PayloadLog>> {
        // A candidate pending still names what it appended: the log keeps it.
        self.keep_appends();
        self.core.detach_payload_log()
    }
    /// Spill to `log` again, given back by a refused restore.
    pub fn attach_payload_log(&mut self, log: Box<dyn super::payloads::PayloadLog>) {
        self.core.attach_payload_log(log);
    }
    /// One step of the payload log's sweep (doc 31 M3, [`Core::sweep_payloads_step`]): the
    /// segments the pending candidates' rows name kept as the committed rows' are. How many
    /// segments it let go of when its pass ended, none while one goes on or none is due.
    pub fn sweep_payloads_step(&mut self, due: bool) -> Option<usize> {
        if !self.core.mark_payloads(due) {
            return None;
        }
        Some(self.core.sweep_marked(pending_rows(&self.pending)))
    }
    /// A whole pass of the payload log's sweep, now ([`Core::sweep_payloads`]), the pending
    /// candidates' segments kept. How many segments it let go of.
    pub fn sweep_payloads(&mut self) -> usize {
        let steps = super::payloads::pass_steps(self.core.state.rows.len());
        for _ in 0..steps {
            if self.core.mark_payloads(true) {
                return self.core.sweep_marked(pending_rows(&self.pending));
            }
        }
        0
    }

    /// The most a checkpoint of the committed root, with every pending
    /// candidate published, could take: its rows (the committed rows and
    /// every pending candidate's writes, each at most one new row) and an
    /// upper bound of its encoded bytes (a frame header, every row at its
    /// widest inline encoding, and the rows' heap at the codec's expansion). A session refuses fresh work whose
    /// projection its checkpoint could not hold, typed at admission, so that
    /// what it admitted it can always checkpoint (rule 2) instead of failing
    /// that checkpoint and stopping.
    pub fn checkpoint_projection(&self) -> Result<CheckpointProjection, NativeOwnerError> {
        let capacity =
            |_: ()| NativeOwnerError::Native(NativeError::Capacity("checkpoint projection"));
        let rows = self
            .core
            .state
            .rows
            .len()
            .checked_add(self.pending_rows)
            .ok_or_else(|| capacity(()))?;
        // What the committed root encodes to, exactly (its frame and the rows' running total,
        // `Core::checkpoint_bytes`), and every row a pending candidate puts, whole: what a
        // candidate replaces or deletes is not taken away, so with every candidate published
        // the checkpoint is at most this. The bound it replaced inferred the rows' bodies from
        // the memory their pages are charged and quadrupled it: twenty-two times what a
        // checkpoint of authored claims encodes to, and admission refused at a twentieth of
        // what a checkpoint holds.
        let bytes = self
            .core
            .checkpoint_bytes()?
            .checked_add(self.pending_bytes)
            .ok_or_else(|| capacity(()))?;
        Ok(CheckpointProjection { rows, bytes })
    }

    /// The range layout the committed rows are held in (25 §4).
    pub fn native_layout(&self) -> &ranges::RangeLayout {
        self.core.native_layout()
    }

    /// Add a range boundary at affinity `at` (see
    /// [`Core::split_native_range`]). Every pending candidate holds a
    /// fragment per member of the layout it was prepared against, so the
    /// layout changes only while nothing is pending and the owner is sound.
    pub fn split_native_range(
        &mut self,
        at: ranges::Affinity,
        id: RangeId,
    ) -> Result<(), NativeOwnerError> {
        self.check_layout_change()?;
        Ok(self.core.split_native_range(at, id)?)
    }

    /// Remove the boundary after member `index` (see
    /// [`Core::merge_native_range`]) under the same conditions as a split.
    pub fn merge_native_range(&mut self, index: usize) -> Result<(), NativeOwnerError> {
        self.check_layout_change()?;
        Ok(self.core.merge_native_range(index)?)
    }

    /// Give range member `index` the durable identity `id` (see
    /// [`Core::rename_native_member`]); identities are not part of any
    /// candidate's fragments, so pending work is unaffected.
    pub fn rename_native_member(
        &mut self,
        index: usize,
        id: RangeId,
    ) -> Result<(), NativeOwnerError> {
        Ok(self.core.rename_native_member(index, id)?)
    }

    /// Which owner this is: a rebuilt one is another.
    pub fn incarnation(&self) -> OwnerId {
        self.incarnation
    }

    /// Apply a committed anchored seal (F12) to the committed rows ([`Core::apply_anchored`]),
    /// the owner going on over them: what it keeps beside the rows (its completion book, whose
    /// promises name evaluations and claims and whose counts it reads from the rows as it checks
    /// them, and its queue, empty here) names nothing a seal takes, so it is the owner a rebuild
    /// over the sealed rows would make, without the rebuild's two walks of every row. `None`
    /// where it may not go on (something pending, or the owner unsound): the caller applies the
    /// seal to the committed rows and rebuilds the owner.
    pub fn apply_anchored(
        &mut self,
        record: super::seal::AnchoredRecord<'_>,
    ) -> Result<Option<usize>, NativeError> {
        if !self.pending.is_empty() || self.faulted {
            return Ok(None);
        }
        self.core.apply_anchored(record).map(Some)
    }

    fn check_layout_change(&self) -> Result<(), NativeOwnerError> {
        if !self.pending.is_empty() {
            return Err(NativeOwnerError::PendingCandidates);
        }
        if self.faulted {
            return Err(NativeError::Capacity("completion owner requires reconstruction").into());
        }
        Ok(())
    }

    /// Whether one family may retire now (26 §4): nothing pending, the
    /// owner sound, and the outcome and sequence the retirement publishes
    /// to spare beyond those the book holds for the reports it promised and
    /// the one control it keeps for an authority decision — the check every
    /// fresh candidate passes. [`Core::retirement_family`] checks the bound
    /// alone; this is the bound less what is promised, so a retirement never
    /// takes the outcome a live report was admitted against, and the owner
    /// rebuilt over the retired core admits every report it promised. A
    /// refusal of the book is `Capacity`, as for a candidate; a faulted
    /// owner refuses the same way, as it refuses every fresh candidate.
    pub fn check_retirement(&self) -> Result<(), NativeOwnerError> {
        self.check_layout_change()?;
        let view = View::committed(&self.core.state);
        let mut meta = view.meta();
        meta.outcomes = meta
            .outcomes
            .checked_add(1)
            .ok_or(NativeError::Capacity("outcomes"))?;
        let sequence = SessionSeq(
            view.prefix()
                .0
                .checked_add(1)
                .ok_or(NativeError::Capacity("sequence"))?,
        );
        // A retirement makes room: it is not fresh work.
        self.book.check_slots(
            meta,
            sequence,
            self.core.state.rows.len(),
            BudgetLane::Completion,
        )?;
        Ok(())
    }

    /// Transfer a fully reconciled committed Core without allocating or copying
    /// rows. A pending or faulted owner is returned unchanged for reconciliation.
    #[allow(clippy::result_large_err)] // Return the identical owner without allocation on refusal.
    pub fn into_committed_core(mut self) -> Result<Core<NativeState>, NativeOwnerIntoCoreError> {
        self.keep_appends();
        let error = if !self.pending.is_empty() {
            Some(NativeOwnerError::PendingCandidates)
        } else if self.faulted {
            Some(NativeError::Capacity("completion owner requires reconstruction").into())
        } else {
            None
        };
        if let Some(error) = error {
            return Err(NativeOwnerIntoCoreError { error, owner: self });
        }
        let Self {
            pending,
            core,
            book,
            _queue_allocation,
            ..
        } = self;
        drop(pending);
        drop(book);
        drop(_queue_allocation);
        Ok(core)
    }

    /// Encode the exact pending mutation using its preheld buffer permit. The
    /// cached bytes remain owned by this ticket through retries; this does not
    /// publish or acknowledge durability. A low work/byte cap or allocation
    /// failure leaves the candidate and its permit available for retry.
    pub fn encode_candidate(
        &mut self,
        candidate: NativeCandidate,
        limits: record_codec::EncodingLimits,
    ) -> Result<&record_codec::FundedRecord, NativeOwnerError> {
        let position = self.position(candidate)?;
        let payloads = self.core.state.payloads();
        let pending = self
            .pending
            .get_mut(position)
            .ok_or(NativeOwnerError::UnknownCandidate)?;
        pending.record.encode(&pending.prepared, limits, payloads)
    }

    /// The encoded records of `candidate` and of every candidate after it,
    /// in chain order: what a flush proposes at once. Refused unless each of
    /// them was encoded ([`Self::encode_candidate`]).
    pub fn encoded_from(
        &self,
        candidate: NativeCandidate,
    ) -> Result<impl Iterator<Item = &[u8]> + Clone + '_, NativeOwnerError> {
        let position = self.position(candidate)?;
        if self
            .pending
            .iter()
            .skip(position)
            .any(|row| row.record.encoded().is_none())
        {
            return Err(NativeError::Capacity("a candidate's record is not encoded").into());
        }
        Ok(self
            .pending
            .iter()
            .skip(position)
            .filter_map(|row| row.record.encoded().map(record_codec::FundedRecord::bytes)))
    }

    /// Includes all unpublished work. These observations are provisional and
    /// must not be returned as durable participant acknowledgments.
    pub fn effective(&self) -> NativeView<'_> {
        NativeView(View::over(
            &self.core.state,
            Some(Chain::indexed(&self.index, &self.pending, u64::MAX)),
        ))
    }

    /// The state as of `candidate`: the committed rows and every pending
    /// candidate up to and including it.
    pub fn candidate(
        &self,
        candidate: NativeCandidate,
    ) -> Result<NativeView<'_>, NativeOwnerError> {
        self.position(candidate)?;
        Ok(NativeView(View::over(
            &self.core.state,
            Some(Chain::indexed(&self.index, &self.pending, candidate.serial)),
        )))
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
    pub(super) fn book_for_test(&self) -> &CompletionBook {
        &self.book
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
    /// The outcome recorded for `invocation`, a spilled one read back: one that cannot be read
    /// is an error, never taken for none.
    pub fn recorded(
        &self,
        invocation: impl Into<NativeInvocation>,
    ) -> Result<Option<NativeOutcome>, NativeError> {
        self.0.outcome(invocation.into())
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
    /// Committed claims whose authored `kind` relation targets `target`, in
    /// identity order (the relation index of 22 §7): the corrections that
    /// invalidate a challenge, the follow-ups that refine a consultation.
    pub fn related_claims(
        &self,
        kind: focal_model::RelationKind,
        target: ClaimId,
    ) -> impl Iterator<Item = ClaimId> + use<'a> {
        self.0.relation_sources(kind, target)
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
            Some(Row::Event(event)) => event.get(self.0.ledger(), self.0.payloads()),
            _ => None,
        }
    }
}
