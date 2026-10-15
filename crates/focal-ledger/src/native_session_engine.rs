//! The native domain engine: pending candidate chain, active owner or passive
//! Core, native prefix and producer-range mapping, genesis and readiness state.
//! It never owns the consensus replica or the content writer; every operation
//! that touches Raft receives the replica explicitly, so the standalone native
//! session and the unified Session run the same code.
use super::*;
use focal_model::ContentDomainId;

const DISK_SAMPLE_INTERVAL: u32 = 32;
/// The most of a stopped session's cause it keeps: an error's text runs to a
/// line, its detail named once.
const FAILURE_TEXT: usize = 512;
/// The most steps a payload sweep whose pass fell short waits before the next may begin (doc 31
/// M3): the wait doubles from one step with each pass that falls short, so a sweep short of memory
/// tries again at a bounded cadence whatever the log does, and never walks the root back to back.
pub(crate) const SWEEP_RETRY_STEPS: u32 = 1024;

pub(super) struct Pending {
    pub(super) candidate: NativeCandidate,
    pub(super) outcome: NativeOutcome,
    pub(super) hash: Option<ContentHash>,
    pub(super) term: u64,
}
#[allow(clippy::large_enum_variant)] // One instance per session; variants move only at authority transitions.
pub(super) enum Domain {
    Passive(Core<NativeState>),
    Active(Box<NativeOwner>, Allocation),
}

pub(crate) struct NativeEngine<S: NativeSchemaVerifier> {
    pub(super) delivery: Option<super::apply::Delivery>,
    pub(super) pending: VecDeque<Pending>,
    /// While its host takes a batch of requests: each candidate is encoded
    /// as it is admitted, and every one is proposed together when the batch
    /// ends (`flush_proposals`), one append to each member, where a proposal
    /// a candidate sent each member an append, drew an answer and a commit's
    /// broadcast of its own.
    pub(super) defer_proposals: bool,
    /// The last flush of a batch left candidates it could not propose: the
    /// replica no longer leads, or the log or the memory has no room for
    /// them. No drain is owed them until a candidate is admitted; each drain
    /// tries them again all the same, and room is made by what a drain
    /// takes (a commit, an applied entry), authority returns only through a
    /// newer term's barrier, which lets them go. Owed, a host that drains
    /// at once what is owed (a grouped owner) ran the replica round after
    /// round, each drain proposing nothing.
    pub(super) flush_stalled: bool,
    /// How many of the chain's candidates, from its head, are encoded, and how many proposed:
    /// each a prefix of it, as candidates are encoded and proposed in chain order. Kept as the
    /// chain changes, so a flush starts where the last left off: walked from the head at each
    /// admission and each drain, the chain cost a session's owner work in its whole depth for
    /// every write (`flush_deferred_native_proposals`, 4.6% of the owner's own time at 10,000
    /// writes a second, 2026-10-12, and growing with the depth memory allows).
    pub(super) encoded: usize,
    pub(super) submitted: usize,
    pub(super) domain: Option<Domain>,
    pub(super) reader: ContentReader,
    /// Where checkpoint seeds are read from on install (25 §5); the writer
    /// belongs to whoever owns the session's physical resources.
    pub(super) seeds: SeedReader,
    /// The session's cold streams (doc 31 M2, A2), and where they live: every cold retirement
    /// this replica applies is appended to them, and its checkpoints name them.
    pub(super) cold: super::cold::ColdSlot,
    pub(super) cold_source: Option<super::cold::ColdSource>,
    /// The seeded checkpoint this replica cannot install until the chunks
    /// it names are local; the host pulls them and polls again.
    pub(super) pending_seed: Option<PendingSeed>,
    /// The objects a retained delivery could not read locally (24 §20).
    pub(super) pending_custody: Option<PendingCustody>,
    pub(super) schemas: S,
    pub(super) budget: MemoryBudget,
    pub(super) ledger: LedgerId,
    pub(super) profile: NativeContentProfile,
    pub(super) range: RangeId,
    pub(super) recording_range: Option<RangeId>,
    pub(super) recording_term: u64,
    /// The prefix that holds no native record yet: zero at genesis, one after an
    /// import, a checkpoint's prefix when it was installed without records.
    pub(super) records_floor: SessionSeq,
    /// Raft index of the committed activation record; zero until it applies.
    pub(super) activation_index: u64,
    pub(super) limits: NativeSessionLimits,
    pub(super) applied_raft: u64,
    pub(super) configuration_index: u64,
    pub(super) genesis: Option<ContentHash>,
    pub(super) genesis_proposed: bool,
    /// The layout record this authority proposed and has not seen applied;
    /// native admission waits for it, and a term change lets it go.
    pub(super) layout_change: Option<super::range::LayoutRecord>,
    /// The prefix the archive reports holding every proof through (26 §3).
    pub(super) archived_through: SessionSeq,
    /// The retirement record this authority proposed and has not seen
    /// applied (26 §4); native admission waits for it, and a term change
    /// lets it go.
    pub(super) retirement: Option<super::retirement::RetirementRecord>,
    /// Families retired through the applied prefix, counted from genesis or
    /// the checkpoint that seeded this replica.
    pub(super) retired_families: u64,
    /// Committed retirement records this replica applied nothing for since
    /// it opened; a diagnostic, never an input to state.
    pub(super) retirements_inert: u64,
    /// The segments the payload log held after its last sweep (doc 31 M3): another pass is due
    /// once the log has sealed two past them. A pass that fell short swept nothing and leaves it.
    pub(super) payload_segments_swept: usize,
    /// The steps the payload sweep waits before a pass may begin, after one fell short, and what
    /// it waited the last time ([`SWEEP_RETRY_STEPS`]).
    pub(super) payload_sweep_wait: u32,
    pub(super) payload_sweep_backoff: u32,
    /// Segments of this many bytes in the payload log the session opens, for the tests of a log
    /// at its segment cap.
    #[cfg(test)]
    pub(crate) payload_segment_bytes: Option<u64>,
    /// Where the replica keeps its payload log, and how many times it could not be opened
    /// there: a replica without one holds every row's bytes until it opens.
    pub(super) payload_source: Option<super::payloads::PayloadSource>,
    pub(super) payload_open_refusals: u64,
    /// The seal record this authority proposed and has not seen applied
    /// (F12); native admission waits for it, and a term change lets it go.
    pub(super) seal: Option<super::seal::SealRecord>,
    /// Seals applied through the applied prefix, from genesis or the
    /// checkpoint that seeded this replica; and the records applied nothing
    /// for since it opened (a diagnostic).
    pub(super) seals_applied: u64,
    pub(super) seals_inert: u64,
    /// The anchored seal this authority proposed and has not seen applied
    /// (F12, `SealRule::Anchored`); native admission waits for it, and a
    /// term change lets it go.
    pub(super) anchored: Option<super::seal::AnchoredSealRecord>,
    /// An anchored seal whose bundle is under custody, held for the
    /// pipeline to drain, and its charge.
    pub(super) waiting_seal: Option<(WaitingSeal, Allocation)>,
    /// The window came under the pressure that forces floors: admission
    /// holds for the pipeline to drain, and a seal carries them.
    pub(super) close_wanted: bool,
    /// The committed and pending resident outcomes at which admission asks
    /// the pressure rule again; zero asks at the next admission.
    pub(super) close_check_at: usize,
    /// Anchored seals let go before proposal (another seal took their
    /// ordinal, or what they name no longer awaits a seal) and closing
    /// seals applied, since this engine opened: diagnostics.
    pub(super) seals_dropped: u64,
    pub(super) closes_applied: u64,
    /// The chunks of this replica's latest checkpoint seed (25 §5): what
    /// its seed store must keep for peers that seed from it (26 §5).
    pub(super) seed_chunks: Vec<ContentHash>,
    /// The chunks of a checkpoint adopted but not yet the group's durable
    /// image: bounded as `seed_chunks` is, by the checkpoint's chunk limit.
    pub(super) staged_chunks: Vec<ContentHash>,
    /// The bytes of the latest checkpoint's root kept in its seed chunks.
    pub(super) seed_root_bytes: u64,
    /// The movement coordinator (25 §6), built once the genesis names the
    /// origin member and restored from a checkpoint's movement section.
    pub(super) movement: Option<super::movement::Movement>,
    pub(super) disk_sample: Option<u64>,
    pub(super) admissions_since_sample: u32,
    pub(super) ready_term: Option<u64>,
    pub(super) readiness_requested: Option<u64>,
    /// Read barriers the leader answered above this copy's applied index:
    /// a follower's reads wait for the entries, never the other way round.
    /// Bounded by the core's reads in flight; charged once, when first used.
    pub(super) parked_reads: Vec<focal_consensus::ReadBarrier>,
    pub(super) parked_charge: Option<Allocation>,
    /// Barriers parked, and barriers dropped at the parked bound, since
    /// this engine opened, for diagnostics.
    pub(super) reads_parked: u64,
    pub(super) reads_dropped: u64,
    pub(super) observed_term: u64,
    pub(super) observed_leader: bool,
    pub(super) reconstruction_needed: bool,
    /// Reconstructions the next promotions refuse as memory, for the tests
    /// of the session's handling of a refused reconstruction.
    #[cfg(test)]
    pub(crate) refuse_reconstructions: u32,
    pub(super) failed: bool,
    /// What the session first failed closed on, at most [`FAILURE_TEXT`]
    /// bytes of it: named by every refusal after (`check`).
    pub(super) failure: Option<Box<str>>,
    pub(super) materializer: super::MaterializerStats,
    pub(super) _pending_allocation: Allocation,
}

/// An anchored seal whose bundle is under custody, held for the pipeline to
/// drain (F12, `SealRule::Anchored`): what its record will name.
#[derive(Debug)]
pub(super) struct WaitingSeal {
    ordinal: u64,
    through: SessionSeq,
    principals: Vec<focal_core::native::seal::SealedPrincipal>,
    count: u64,
    bundle: ContentHash,
    bytes: u64,
    fold: Option<focal_core::native::seal::Fold>,
}
impl WaitingSeal {
    /// Whether the committed state still names this plan: its ordinal the
    /// next, every range awaiting a seal from its window's mark, its rows
    /// the same — what the apply derives again.
    fn holds(&self, core: &Core<NativeState>) -> bool {
        core.native_seal_ordinal() == Some(self.ordinal)
            && usize::try_from(self.count).is_ok_and(|rows| {
                core.anchored_plan(self.through, &self.principals, rows)
                    .is_ok_and(|plan| plan.rows() == rows)
            })
    }
}

/// What the engine reads from this node's disk beside the log: the content
/// tree custody verifies against and the seed store a seeded checkpoint is
/// assembled from (25 §5).
pub(crate) struct NativeSources {
    pub(crate) reader: ContentReader,
    pub(crate) seeds: SeedReader,
    /// Where the session keeps its cold streams (doc 31 M2, A2): every replica that can apply a
    /// cold retirement has them.
    pub(crate) cold: Option<super::cold::ColdSource>,
    /// Where the replica keeps its payload log (doc 31 M3): a replica without one holds every
    /// row's bytes.
    pub(crate) payloads: Option<super::payloads::PayloadSource>,
}

impl<S: NativeSchemaVerifier> NativeEngine<S> {
    /// An empty native domain at the group's origin. Startup delivery restores
    /// the committed prefix from consensus; nothing here reads the log.
    pub(crate) fn new(
        ledger: LedgerId,
        range: RangeId,
        profile: NativeContentProfile,
        limits: NativeSessionLimits,
        parent: &MemoryBudget,
        sources: NativeSources,
        schemas: S,
    ) -> Result<Self, NativeSessionError> {
        let NativeSources {
            reader,
            seeds,
            cold: cold_source,
            payloads,
        } = sources;
        if range.0 == 0
            || limits.content_domain.is_zero()
            || limits.recovery.native.pending == 0
            || limits.frame_bytes == 0
        {
            return Err(NativeSessionError::Capacity("session limits"));
        }
        // Validate the derived decode limits once; every frame reuses them.
        input_codec::NativeDecodeLimits::for_native(
            limits.recovery.native,
            limits.frame_bytes,
            limits.decode_work,
        )
        .map_err(NativeOwnerError::from)?;
        let budget = limits.budget_within(parent)?;
        let count = limits.recovery.native.pending;
        let permit = budget.reserve(
            BudgetKind::Pending,
            BudgetLane::Ordinary,
            array::<Pending>(count)?,
        )?;
        let mut pending = VecDeque::new();
        pending
            .try_reserve_exact(count)
            .map_err(|_| NativeSessionError::Capacity("pending candidates"))?;
        if pending.capacity() > count {
            return Err(NativeSessionError::Capacity("pending candidates"));
        }
        let mut core = match profile {
            NativeContentProfile::ProjectionOnly => {
                Core::new_native(ledger, range, limits.recovery.native, budget.clone())?
            }
            NativeContentProfile::AuthoredV1 => {
                Core::new_native_authored(ledger, range, limits.recovery.native, budget.clone())?
            }
        };
        // A log that cannot open (its directory, the volume) leaves the replica holding every
        // row's bytes, as one without a log does, until it opens (`open_payload_log`).
        let mut payload_open_refusals = 0u64;
        if let Some(source) = payloads.as_ref() {
            match super::payloads::FilePayloads::open(source, &budget) {
                Ok(log) => core.attach_payload_log(Box::new(log)),
                Err(_) => payload_open_refusals = 1,
            }
        }
        Ok(Self {
            delivery: None,
            pending,
            defer_proposals: false,
            flush_stalled: false,
            encoded: 0,
            submitted: 0,
            domain: Some(Domain::Passive(core)),
            reader,
            seeds,
            cold: if cold_source.is_some() {
                super::cold::ColdSlot::Closed(None)
            } else {
                super::cold::ColdSlot::None
            },
            cold_source,
            pending_seed: None,
            pending_custody: None,
            schemas,
            budget,
            ledger,
            profile,
            range,
            recording_range: None,
            recording_term: 0,
            records_floor: SessionSeq(0),
            activation_index: 0,
            limits,
            applied_raft: 0,
            configuration_index: 0,
            genesis: None,
            genesis_proposed: false,
            layout_change: None,
            archived_through: SessionSeq(0),
            retirement: None,
            retired_families: 0,
            retirements_inert: 0,
            payload_segments_swept: 0,
            payload_sweep_wait: 0,
            payload_sweep_backoff: 0,
            #[cfg(test)]
            payload_segment_bytes: None,
            payload_source: payloads,
            payload_open_refusals,
            seal: None,
            seals_applied: 0,
            seals_inert: 0,
            anchored: None,
            waiting_seal: None,
            close_wanted: false,
            close_check_at: 0,
            seals_dropped: 0,
            closes_applied: 0,
            seed_chunks: Vec::new(),
            staged_chunks: Vec::new(),
            seed_root_bytes: 0,
            movement: None,
            disk_sample: None,
            admissions_since_sample: 0,
            ready_term: None,
            readiness_requested: None,
            parked_reads: Vec::new(),
            parked_charge: None,
            reads_parked: 0,
            reads_dropped: 0,
            observed_term: 0,
            observed_leader: false,
            reconstruction_needed: true,
            #[cfg(test)]
            refuse_reconstructions: 0,
            failed: false,
            failure: None,
            materializer: super::MaterializerStats::default(),
            _pending_allocation: permit.commit(),
        })
    }

    pub(super) fn check(&self) -> Result<(), NativeSessionError> {
        if self.failed {
            return Err(match &self.failure {
                Some(cause) => NativeSessionError::Stopped(cause.clone()),
                None => NativeSessionError::Failed,
            });
        }
        if self.domain.is_none() {
            return Err(NativeSessionError::Failed);
        }
        Ok(())
    }
    /// The session stops on `error`, which is fail-closed: it keeps the first
    /// such error's text, at most [`FAILURE_TEXT`] bytes, to name when asked
    /// again. Written once, as the session stops.
    pub(super) fn fail_closed(&mut self, error: &NativeSessionError) {
        self.failed = true;
        if self.failure.is_none() {
            let mut text = error.to_string();
            text.truncate(text.floor_char_boundary(FAILURE_TEXT));
            self.failure = Some(text.into_boxed_str());
        }
    }
    pub(crate) fn activation_index(&self) -> u64 {
        self.activation_index
    }
    pub(crate) fn set_activation_index(&mut self, index: u64) {
        self.activation_index = index;
    }
    pub(crate) fn failed(&self) -> bool {
        self.failed
    }
    #[cfg(test)]
    pub(crate) fn budget(&self) -> &MemoryBudget {
        &self.budget
    }
    pub(crate) fn decode_limits(
        &self,
    ) -> Result<input_codec::NativeDecodeLimits, NativeSessionError> {
        Ok(input_codec::NativeDecodeLimits::for_native(
            self.limits.recovery.native,
            self.limits.frame_bytes,
            self.limits.decode_work,
        )
        .map_err(NativeOwnerError::from)?)
    }
    /// Current-term authority past its committed readiness barrier, with the
    /// genesis applied, the owner reconstructed and no delivery in flight.
    pub(crate) fn is_authoritative(&self, status: &focal_consensus::NodeScalars) -> bool {
        !self.failed
            && self.delivery.is_none()
            && status.role == StateRole::Leader
            && self.ready_term == Some(status.term)
            && self.genesis.is_some()
            && matches!(self.domain, Some(Domain::Active(..)))
    }
    pub(crate) fn genesis(&self) -> Option<ContentHash> {
        self.genesis
    }
    pub(crate) fn committed_core(&self) -> Result<&Core<NativeState>, NativeSessionError> {
        self.check()?;
        match self.domain.as_ref() {
            Some(Domain::Passive(core)) => Ok(core),
            Some(Domain::Active(owner, _)) => Ok(owner.committed_core()),
            None => Err(NativeSessionError::Failed),
        }
    }
    pub(crate) fn sequence(&self) -> Result<SessionSeq, NativeSessionError> {
        Ok(self.committed_core()?.native_sequence())
    }
    /// Propose one layout change as a session decision (25 §4). Only an
    /// authority with no pending candidate and no change in flight may; the
    /// change is checked against the committed layout before the record is
    /// proposed, so an applicable record is what the log carries.
    pub(crate) fn propose_layout(
        &mut self,
        consensus: &mut DurableNode,
        operation: super::range::LayoutOperation,
    ) -> Result<(), NativeSessionError> {
        let status = consensus.scalars();
        self.require_authority(&status)?;
        if self.layout_change.is_some() {
            return Err(NativeSessionError::LayoutChanging);
        }
        if self.retirement.is_some() {
            return Err(NativeSessionError::Retiring);
        }
        if self.sealing() {
            return Err(NativeSessionError::Sealing);
        }
        if self
            .movement
            .as_ref()
            .is_some_and(|movement| movement.pending().is_some() || movement.in_flight.is_some())
        {
            return Err(NativeSessionError::RangeMoving);
        }
        if !self.pending.is_empty() || self.delivery.is_some() {
            return Err(NativeSessionError::Capacity(
                "pending work before a movement",
            ));
        }
        let core = self.committed_core()?;
        let layout = core.native_layout();
        match operation {
            super::range::LayoutOperation::Split { at, id } => {
                layout.check_split(at, id, self.limits.recovery.native.max_ranges)?;
            }
            super::range::LayoutOperation::Merge { left } => {
                layout.check_merge(left)?;
            }
        }
        let record = super::range::LayoutRecord {
            ledger: self.ledger,
            expected_epoch: layout.epoch(),
            operation,
        };
        let mut bytes = [0u8; super::range::BYTES];
        record.write_into(&mut bytes);
        let _permit = self.budget.reserve(
            BudgetKind::Pending,
            BudgetLane::Completion,
            array::<u8>(super::range::BYTES)?,
        )?;
        consensus.propose_borrowed_in(&bytes, BudgetLane::Completion)?;
        self.layout_change = Some(record);
        Ok(())
    }
    pub(crate) fn archived_through(&self) -> SessionSeq {
        self.archived_through
    }
    /// The archive's report is monotone: it never takes back what it holds.
    pub(crate) fn note_archived(&mut self, through: SessionSeq) {
        self.archived_through = self.archived_through.max(through);
    }
    pub(crate) fn retirement_in_flight(&self) -> Option<super::retirement::RetirementRecord> {
        self.retirement
    }
    pub(crate) fn seal_in_flight(&self) -> Option<super::seal::SealRecord> {
        self.seal.clone()
    }
    pub(crate) fn seals_applied(&self) -> u64 {
        self.seals_applied
    }
    pub(crate) fn seals_inert(&self) -> u64 {
        self.seals_inert
    }
    /// Closing seals applied, and anchored seals let go before proposal,
    /// since this engine opened (F12, `SealRule::Anchored`).
    pub(crate) fn closes_applied(&self) -> u64 {
        self.closes_applied
    }
    pub(crate) fn seals_dropped(&self) -> u64 {
        self.seals_dropped
    }
    /// The anchored seal this authority proposed and has not seen applied.
    pub(crate) fn anchored_in_flight(&self) -> Option<&super::seal::AnchoredSealRecord> {
        self.anchored.as_ref()
    }
    /// How this session's authority seals (F12).
    pub(crate) fn seal_rule(&self) -> super::SealRule {
        self.limits.seal_rule
    }
    /// How this session's authority retires (doc 31 M2).
    pub(crate) fn retire_rule(&self) -> super::RetireRule {
        self.limits.retire_rule
    }
    /// A seal held for the drain or in flight: the archive agent looks
    /// again soon rather than at its next tick.
    pub(crate) fn seal_busy(&self) -> bool {
        self.sealing() || self.decision_wanted()
    }
    /// The owner must be reconstructed before this engine is authoritative
    /// again: a record it did not author through its owner applied through
    /// its committed core (a retirement, 26 §4).
    /// The active owner's incarnation, if there is one.
    #[cfg(test)]
    pub(crate) fn owner_incarnation(&self) -> Option<focal_memory::OwnerId> {
        match self.domain.as_ref() {
            Some(Domain::Active(owner, _)) => Some(owner.incarnation()),
            _ => None,
        }
    }
    pub(crate) fn reconstruction_needed(&self) -> bool {
        self.reconstruction_needed
    }
    pub(crate) fn retired_families(&self) -> u64 {
        self.retired_families
    }
    /// Committed retirement records this replica applied nothing for since
    /// it opened (26 §4): the prefix they named had passed, a movement was
    /// pending, or the committed state refused the family — a version-1
    /// record beyond this replica's outcome bound among them.
    pub(crate) fn retirements_inert(&self) -> u64 {
        self.retirements_inert
    }
    /// The payload log, taken from the core that holds it (doc 31 M3).
    pub(super) fn take_payload_log(
        &mut self,
    ) -> Option<Box<dyn focal_core::native::payloads::PayloadLog>> {
        match self.domain.as_mut() {
            Some(Domain::Passive(core)) => core.detach_payload_log(),
            Some(Domain::Active(owner, _)) => owner.detach_payload_log(),
            None => None,
        }
    }
    /// The payload log given back to the core it was taken from, whose spilled rows it holds.
    pub(super) fn give_back_payload_log(
        &mut self,
        log: Option<Box<dyn focal_core::native::payloads::PayloadLog>>,
    ) {
        let Some(log) = log else {
            return;
        };
        match self.domain.as_mut() {
            Some(Domain::Passive(core)) => core.attach_payload_log(log),
            Some(Domain::Active(owner, _)) => owner.attach_payload_log(log),
            None => {}
        }
    }
    /// Open the replica's payload log where its core has none: one that could not open before
    /// (the volume was full) is tried again. Never while a core holds spilled rows of another
    /// log: opening empties the directory, so only a core with no log at all takes one.
    pub(super) fn open_payload_log(&mut self) {
        let Some(source) = self.payload_source.as_ref() else {
            return;
        };
        let has_log = match self.domain.as_ref() {
            Some(Domain::Passive(core)) => core.payload_log().is_some(),
            Some(Domain::Active(owner, _)) => owner.committed_core().payload_log().is_some(),
            None => true,
        };
        if has_log {
            return;
        }
        #[cfg(test)]
        let opened = match self.payload_segment_bytes {
            Some(bytes) => super::payloads::FilePayloads::open_with(source, &self.budget, bytes),
            None => super::payloads::FilePayloads::open(source, &self.budget),
        };
        #[cfg(not(test))]
        let opened = super::payloads::FilePayloads::open(source, &self.budget);
        let log = match opened {
            Ok(log) => log,
            Err(_) => {
                self.payload_open_refusals = self.payload_open_refusals.saturating_add(1);
                return;
            }
        };
        match self.domain.as_mut() {
            Some(Domain::Passive(core)) => core.attach_payload_log(Box::new(log)),
            Some(Domain::Active(owner, _)) => owner.attach_payload_log(Box::new(log)),
            None => {}
        }
    }
    /// How many times the replica's payload log could not be opened.
    pub(crate) fn payload_open_refusals(&self) -> u64 {
        self.payload_open_refusals
    }
    /// The segments the replica's payload log holds: none without one.
    pub(crate) fn payload_segments(&self) -> usize {
        let core = match self.domain.as_ref() {
            Some(Domain::Passive(core)) => core,
            Some(Domain::Active(owner, _)) => owner.committed_core(),
            None => return 0,
        };
        core.payload_log()
            .map_or(0, focal_core::native::payloads::PayloadLog::held_segments)
    }
    /// What the payload log's sweep has done since the replica's core was made.
    pub(crate) fn payload_sweep(&self) -> focal_core::native::payloads::PayloadSweep {
        match self.domain.as_ref() {
            Some(Domain::Passive(core)) => core.payload_sweep(),
            Some(Domain::Active(owner, _)) => owner.committed_core().payload_sweep(),
            None => focal_core::native::payloads::PayloadSweep::default(),
        }
    }
    /// The payload bytes the replica's payload log holds: none without one.
    pub(crate) fn payload_bytes(&self) -> u64 {
        let core = match self.domain.as_ref() {
            Some(Domain::Passive(core)) => core,
            Some(Domain::Active(owner, _)) => owner.committed_core(),
            None => return 0,
        };
        core.payload_log()
            .map_or(0, focal_core::native::payloads::PayloadLog::held_bytes)
    }
    /// One step of the payload log's sweep (doc 31 M3, `payloads::Marks`): a pass over every
    /// committed row begins once the log has sealed another segment since the last sweep and
    /// goes on a bounded step at each call, and at its end the segments no row names are let
    /// go. A whole walk of the rows at once held the replica's thread for as long as the root
    /// took to walk.
    pub(super) fn sweep_payloads_if_due(&mut self) {
        self.open_payload_log();
        self.step_payload_sweep();
    }
    /// One step of the payload log's sweep ([`Self::sweep_payloads_if_due`]), the log as it is:
    /// what each delivery takes, never opening a log that could not open. A pass is due once the
    /// log has sealed two segments past the last sweep, or once rows an eighth of the root were
    /// written since the last pass that swept began
    /// (`focal_core::native::payloads::CHURN_SHARE`): a retirement or a deletion leaves spilled
    /// copies no row names without sealing a segment, and a log at its segment cap seals none. A
    /// pass that fell short for want of memory swept nothing and is not taken for a sweep: the next
    /// is as due, after a wait that doubles with each that falls short ([`SWEEP_RETRY_STEPS`]).
    /// Taken for one, a passing memory refusal left a log at its cap unswept for good, its
    /// segments counted swept and no two more ever to be sealed.
    pub(super) fn step_payload_sweep(&mut self) {
        use focal_core::native::payloads::SweepStep;
        let held = |core: &Core<NativeState>| {
            core.payload_log()
                .map_or(0, focal_core::native::payloads::PayloadLog::held_segments)
        };
        let waiting = self.payload_sweep_wait != 0;
        self.payload_sweep_wait = self.payload_sweep_wait.saturating_sub(1);
        let swept = self.payload_segments_swept;
        let due = |core: &Core<NativeState>| {
            !waiting && (held(core) > swept.saturating_add(1) || core.payload_churned())
        };
        let (step, after) = match self.domain.as_mut() {
            Some(Domain::Passive(core)) => {
                let due = due(core);
                (core.sweep_payloads_step(due), held(core))
            }
            Some(Domain::Active(owner, _)) => {
                let due = due(owner.committed_core());
                let step = owner.sweep_payloads_step(due);
                (step, held(owner.committed_core()))
            }
            None => return,
        };
        match step {
            SweepStep::Swept(_) => {
                self.payload_segments_swept = after;
                self.payload_sweep_backoff = 0;
            }
            SweepStep::Short => {
                self.payload_sweep_backoff = self
                    .payload_sweep_backoff
                    .saturating_mul(2)
                    .clamp(1, SWEEP_RETRY_STEPS);
                self.payload_sweep_wait = self.payload_sweep_backoff;
            }
            SweepStep::Idle | SweepStep::Going => {}
        }
    }
    /// A whole pass of the payload log's sweep, now: after a restore, whose rows spilled afresh
    /// to segments of their own, so what the core it replaced spilled is named by nothing.
    pub(super) fn sweep_payloads_now(&mut self) {
        self.open_payload_log();
        let held = |core: &Core<NativeState>| {
            core.payload_log()
                .map_or(0, focal_core::native::payloads::PayloadLog::held_segments)
        };
        let swept = match self.domain.as_mut() {
            Some(Domain::Passive(core)) => core.sweep_payloads().map(|_| held(core)),
            Some(Domain::Active(owner, _)) => {
                owner.sweep_payloads().map(|_| held(owner.committed_core()))
            }
            None => return,
        };
        // A pass that fell short swept nothing: the steps that follow try again.
        if let Some(held) = swept {
            self.payload_segments_swept = held;
        }
    }
    /// The chunks of the latest checkpoint seed, sorted.
    pub(crate) fn seed_chunks(&self) -> &[ContentHash] {
        &self.seed_chunks
    }
    /// The chunks of a checkpoint adopted but not yet durable as the group's.
    pub(crate) fn staged_seed_chunks(&self) -> &[ContentHash] {
        &self.staged_chunks
    }
    /// Record the seed chunks a freshly encoded or installed checkpoint
    /// names; an inline checkpoint names none.
    pub(super) fn note_seeds(&mut self, bytes: &[u8]) -> Result<(), NativeSessionError> {
        let (chunks, root_bytes) = self.describe_seeds(bytes)?;
        self.seed_chunks = chunks;
        self.seed_root_bytes = root_bytes;
        self.staged_chunks = Vec::new();
        Ok(())
    }
    /// Take a cold chunk a peer sent for the pending seed (doc 31 M2, A2): into the data stream
    /// (`data`) or its marks, verified against its hash and durable.
    pub(crate) fn install_cold_chunk(
        &mut self,
        data: bool,
        hash: ContentHash,
        bytes: &[u8],
    ) -> Result<(), NativeSessionError> {
        let source = self
            .cold_source
            .as_ref()
            .ok_or(NativeSessionError::Legacy)?;
        super::cold::NativeCold::install(source, data, hash, bytes).map_err(super::cold::cold_error)
    }
    /// The frame of digest `digest` in the cold batch of the record at `sequence` (doc 31 M2,
    /// A2): `None` when no cold record applied there, as for a family retired under custody.
    pub(crate) fn cold_frame(
        &mut self,
        sequence: SessionSeq,
        digest: ContentHash,
    ) -> Result<Option<super::cold::ColdFrame>, NativeSessionError> {
        match self
            .cold
            .writer(self.cold_source.as_ref(), &self.budget)
            .map_err(super::cold::cold_error)?
        {
            Some(cold) => cold
                .frame(sequence, digest, &self.budget)
                .map_err(super::cold::cold_error),
            None => Ok(None),
        }
    }
    /// The cold data stream as it stands, to be read away from the owner
    /// (`cold::ColdSlot::data`): none where this replica keeps no cold streams.
    pub(crate) fn cold_data(&self) -> Result<Option<super::cold::ColdData>, NativeSessionError> {
        self.cold
            .data(self.cold_source.as_ref(), &self.budget)
            .map_err(super::cold::cold_error)
    }
    /// The cold chunks the checkpoint `streams` names are durable: its encoding made them so
    /// before its bytes existed (doc 31 M2, A2), so they are pending no more.
    fn note_cold(
        &mut self,
        streams: Option<focal_evidence::ColdStreams>,
    ) -> Result<(), NativeSessionError> {
        if let Some(streams) = streams {
            self.cold
                .made_durable(&streams)
                .map_err(super::cold::cold_error)?;
        }
        Ok(())
    }
    /// The bytes of the root the latest checkpoint keeps in seed chunks,
    /// outside its envelope: zero for one whose root travels inline.
    pub(crate) fn seed_root_bytes(&self) -> u64 {
        self.seed_root_bytes
    }
    /// The chunks of a checkpoint whose image is adopted but not yet the
    /// group's durable one: kept with the durable image's until it is, for a
    /// restart may open at either.
    pub(super) fn stage_seeds(&mut self, bytes: &[u8]) -> Result<(), NativeSessionError> {
        self.staged_chunks = self.describe_seeds(bytes)?.0;
        Ok(())
    }
    /// The seed chunks the native section `bytes` names, sorted and distinct, and the bytes of
    /// the root they hold; the cold chunks it names are noted durable from the same parse.
    fn describe_seeds(
        &mut self,
        bytes: &[u8],
    ) -> Result<(Vec<ContentHash>, u64), NativeSessionError> {
        let mut chunks = Vec::new();
        let mut root_bytes = 0;
        let (manifest, cold) = crate::native_checkpoint::Checkpoint::describe_with_cold(
            bytes,
            self.limits.checkpoint,
        )?;
        self.note_cold(cold.map(|section| section.streams()).transpose()?)?;
        if let Some(manifest) = manifest {
            root_bytes = manifest.header().core_bytes;
            // Reserve the manifest's chunk count once instead of growing by one
            // per chunk (which was O(n^2) in reallocations).
            chunks
                .try_reserve_exact(manifest.len())
                .map_err(|_| NativeSessionError::Capacity("checkpoint chunks"))?;
            for chunk in manifest.chunks() {
                let chunk = chunk?;
                chunks.push(chunk.hash);
            }
        }
        chunks.sort();
        chunks.dedup();
        Ok((chunks, root_bytes))
    }
    /// The gates a retirement passes before its family is derived (26 §4):
    /// authority, no retirement, layout change or movement step in flight,
    /// no pending candidate and no delivery.
    fn retirement_gates(
        &self,
        status: &focal_consensus::NodeScalars,
    ) -> Result<(), NativeSessionError> {
        self.require_authority(status)?;
        if self.retirement.is_some() {
            return Err(NativeSessionError::Retiring);
        }
        if self.sealing() {
            return Err(NativeSessionError::Sealing);
        }
        if self.layout_change.is_some() {
            return Err(NativeSessionError::LayoutChanging);
        }
        if self
            .movement
            .as_ref()
            .is_some_and(|movement| movement.pending().is_some() || movement.in_flight.is_some())
        {
            return Err(NativeSessionError::RangeMoving);
        }
        if !self.pending.is_empty() || self.delivery.is_some() {
            return Err(NativeSessionError::Capacity(
                "pending work before a movement",
            ));
        }
        Ok(())
    }
    /// The core's outcome bound (`OutcomeCapacity`, the permanent one,
    /// named first), then the owner's word that the outcome a retirement
    /// publishes is not one it promised to a live report or keeps for the
    /// control (`OutcomesReserved`); the book's refusal is `Capacity`, as
    /// for a candidate, and is named here for what it holds.
    fn retirement_reservation(&self) -> Result<(), NativeSessionError> {
        let Some(Domain::Active(owner, _)) = self.domain.as_ref() else {
            return Err(NativeSessionError::Failed);
        };
        owner
            .committed_core()
            .check_retirement_outcome()
            .map_err(NativeSessionError::Retirement)?;
        owner.check_retirement().map_err(|error| match error {
            NativeOwnerError::PendingCandidates => {
                NativeSessionError::Capacity("pending candidates before retirement")
            }
            NativeOwnerError::Native(NativeError::Capacity(_)) => NativeSessionError::Retirement(
                focal_core::native::retirement::RetirementRefusal::OutcomesReserved,
            ),
            other => other.into(),
        })
    }
    /// Whether this authority could propose a retirement now (26 §4): the
    /// gates and the owner's reservation `propose_retirement` applies, short
    /// of the family itself. The archive agent asks before it seals a
    /// bundle, so nothing is sealed for a family that cannot be proposed.
    pub(crate) fn check_retirement(
        &self,
        status: &focal_consensus::NodeScalars,
    ) -> Result<(), NativeSessionError> {
        self.retirement_gates(status)?;
        self.retirement_reservation()
    }
    /// Propose one family's retirement as a session decision (26 §4). Only
    /// an authority with no pending candidate and nothing else in flight
    /// may; the family is derived from the committed state (which refuses
    /// one whose outcome would pass the core's bound), the owner's
    /// reservation of outcomes for the reports it promised is checked, and
    /// the bundle's claim is checked against the family, all before the
    /// record is proposed, so an applicable record is what the log carries
    /// and a refusal proposes and fences nothing. The record carries the
    /// bound the retirement was checked against.
    pub(crate) fn propose_retirement(
        &mut self,
        consensus: &mut DurableNode,
        root: focal_model::ClaimId,
        bundle: ContentHash,
        bytes: u64,
        through: SessionSeq,
    ) -> Result<(), NativeSessionError> {
        let status = consensus.scalars();
        self.retirement_gates(&status)?;
        let core = self.committed_core()?;
        let family = core
            .retirement_family(root)
            .map_err(NativeSessionError::Retirement)?;
        let prefix = core.native_sequence();
        self.retirement_reservation()?;
        if bundle.0 == [0; 32]
            || bytes == 0
            || through.0 == 0
            || through < family.through
            || through > prefix
        {
            return Err(NativeError::Contract(
                focal_model::lifecycle::ContractError::InvalidManifest,
            )
            .into());
        }
        let outcome_limit = u64::try_from(self.limits.recovery.native.outcomes)
            .map_err(|_| NativeSessionError::Capacity("outcome limit"))?;
        let record = super::retirement::RetirementRecord {
            ledger: self.ledger,
            expected_prefix: prefix,
            root,
            bundle,
            bytes,
            through,
            outcome_limit: Some(outcome_limit),
        };
        let mut encoded = [0u8; super::retirement::BYTES];
        record.write_into(&mut encoded)?;
        let _permit = self.budget.reserve(
            BudgetKind::Pending,
            BudgetLane::Completion,
            array::<u8>(super::retirement::BYTES)?,
        )?;
        consensus.propose_borrowed_in(&encoded, BudgetLane::Completion)?;
        self.retirement = Some(record);
        Ok(())
    }
    /// Whether this authority could propose a seal now (F12). Derived: the
    /// gates a retirement passes — no candidate pending — and room for the
    /// seal's own outcome within the bound and the committed headroom (a
    /// seal takes at least one outcome with it, so it never leaves the
    /// window fuller; held to the bound alone, a full window refused the
    /// seal that empties it, for good). Anchored: authority, and no seal in
    /// flight or held for the drain — its plan is derived from the
    /// committed core whatever the pipeline holds.
    pub(crate) fn check_seal(
        &self,
        status: &focal_consensus::NodeScalars,
    ) -> Result<(), NativeSessionError> {
        match self.limits.seal_rule {
            super::SealRule::Derived => {
                self.retirement_gates(status)?;
                self.committed_core()?
                    .check_seal_outcome()
                    .map_err(NativeSessionError::Seal)
            }
            super::SealRule::Anchored => {
                self.require_authority(status)?;
                if self.sealing() || self.waiting_seal.is_some() {
                    return Err(NativeSessionError::Sealing);
                }
                Ok(())
            }
        }
    }
    /// Propose one seal as a session decision (F12). Derived: only an
    /// authority with no pending candidate and nothing else in flight may;
    /// the plan must have been derived at the committed prefix (a plan
    /// derived earlier is refused, never proposed), and the core must hold
    /// the seal's outcome. Anchored: the plan must still be what the
    /// committed state names for it (its ordinal next, every range awaiting
    /// a seal from its window's mark, its rows the same); the seal is held
    /// for the pipeline to drain and proposed then (`propose_wanted`), with
    /// the floors the pressure forces there.
    pub(crate) fn propose_seal(
        &mut self,
        consensus: &mut DurableNode,
        plan: &focal_core::native::seal::SealPlan,
        bundle: ContentHash,
        bytes: u64,
        fold: Option<focal_core::native::seal::Fold>,
    ) -> Result<(), NativeSessionError> {
        if self.limits.seal_rule == super::SealRule::Anchored {
            return self.hold_seal(consensus, plan, bundle, bytes, fold);
        }
        let status = consensus.scalars();
        self.retirement_gates(&status)?;
        let core = self.committed_core()?;
        let prefix = core.native_sequence();
        if plan.through != prefix
            || bundle.0 == [0; 32]
            || bytes == 0
            || plan.rows() == 0
            || fold.is_some_and(|fold| fold.last >= plan.ordinal)
        {
            return Err(NativeError::Contract(
                focal_model::lifecycle::ContractError::InvalidManifest,
            )
            .into());
        }
        core.check_seal_outcome()
            .map_err(NativeSessionError::Seal)?;
        // The floors the plan was derived under are the pressure floors of
        // this prefix: the record names them, and apply derives them again.
        let floors = core
            .pressure_floors(super::seal::MAX_FLOORS)
            .map_err(NativeSessionError::Native)?;
        let record = super::seal::SealRecord {
            ledger: self.ledger,
            expected_prefix: prefix,
            through: plan.through,
            bundle,
            bytes,
            count: u64::try_from(plan.rows())
                .map_err(|_| NativeSessionError::Capacity("seal rows"))?,
            bound: focal_core::native::seal::SealBound {
                principals: plan.principals.len().max(1),
                rows: plan.rows(),
            },
            outcome_limit: u64::try_from(self.limits.recovery.native.outcomes)
                .map_err(|_| NativeSessionError::Capacity("outcome limit"))?,
            floors,
            fold,
        };
        let encoded = record.encode()?;
        let _permit = self.budget.reserve(
            BudgetKind::Pending,
            BudgetLane::Completion,
            array::<u8>(encoded.len())?,
        )?;
        consensus.propose_borrowed_in(&encoded, BudgetLane::Completion)?;
        self.seal = Some(record);
        Ok(())
    }
    /// Hold an anchored seal for the drain (F12): checked against the
    /// committed state as it stands, charged to the session while held, and
    /// proposed at once when nothing is pending.
    fn hold_seal(
        &mut self,
        consensus: &mut DurableNode,
        plan: &focal_core::native::seal::SealPlan,
        bundle: ContentHash,
        bytes: u64,
        fold: Option<focal_core::native::seal::Fold>,
    ) -> Result<(), NativeSessionError> {
        let status = consensus.scalars();
        self.check_seal(&status)?;
        let invalid = || -> NativeSessionError {
            NativeError::Contract(focal_model::lifecycle::ContractError::InvalidManifest).into()
        };
        if bundle.0 == [0; 32]
            || bytes == 0
            || plan.rows() == 0
            || plan.principals.len() > super::seal::MAX_PRINCIPALS
            || fold.is_some_and(|fold| fold.last >= plan.ordinal)
        {
            return Err(invalid());
        }
        let count =
            u64::try_from(plan.rows()).map_err(|_| NativeSessionError::Capacity("seal rows"))?;
        let mut principals =
            reserved::<focal_core::native::seal::SealedPrincipal>(plan.principals.len())?;
        principals.extend_from_slice(&plan.principals);
        let waiting = WaitingSeal {
            ordinal: plan.ordinal,
            through: plan.through,
            principals,
            count,
            bundle,
            bytes,
            fold,
        };
        // What it names is checked in full where it is proposed; here, that
        // no other seal took its ordinal while its bundle was under custody.
        if self.committed_core()?.native_seal_ordinal() != Some(waiting.ordinal) {
            return Err(invalid());
        }
        let charge = self
            .budget
            .reserve(
                BudgetKind::Pending,
                BudgetLane::Completion,
                array::<focal_core::native::seal::SealedPrincipal>(waiting.principals.len())?,
            )?
            .commit();
        self.waiting_seal = Some((waiting, charge));
        self.propose_wanted(consensus)
    }
    /// Propose the seal this authority holds for the drain (F12,
    /// `SealRule::Anchored`) once the pipeline has drained and nothing else
    /// is in flight: the held seal, if what it names still awaits a seal
    /// (else it is let go and counted, its bundle left to the collector),
    /// with the floors the pressure forces at this prefix; or, with no seal
    /// held, a closing seal of the floors alone. Waits, proposing nothing,
    /// while anything is pending or in flight; nothing to propose ends the
    /// want.
    pub(crate) fn propose_wanted(
        &mut self,
        consensus: &mut DurableNode,
    ) -> Result<(), NativeSessionError> {
        if self.limits.seal_rule != super::SealRule::Anchored || !self.decision_wanted() {
            return Ok(());
        }
        let status = consensus.scalars();
        if !self.is_authoritative(&status)
            || self.layout_change.is_some()
            || self.retirement.is_some()
            || self.sealing()
            || self.movement.as_ref().is_some_and(|movement| {
                movement.pending().is_some() || movement.in_flight.is_some()
            })
            || !self.pending.is_empty()
            || self.delivery.is_some()
        {
            return Ok(());
        }
        let waiting = self.waiting_seal.take();
        self.close_wanted = false;
        // What the seal changes moves the pressure: asked again at the next
        // admission.
        self.close_check_at = 0;
        let core = self.committed_core()?;
        let prefix = core.native_sequence();
        let ordinal = core
            .native_seal_ordinal()
            .ok_or(NativeSessionError::Capacity("seals"))?;
        let floors = core
            .close_floors(super::seal::MAX_ANCHORED_FLOORS)
            .map_err(NativeSessionError::Native)?;
        let held = waiting.is_some();
        let waiting = match waiting {
            Some((waiting, _charge)) if waiting.holds(core) => Some(waiting),
            _ => None,
        };
        core.check_seal_outcome()
            .map_err(NativeSessionError::Seal)?;
        if held && waiting.is_none() {
            self.seals_dropped = self.seals_dropped.saturating_add(1);
        }
        if waiting.is_none() && floors.is_empty() {
            return Ok(());
        }
        let outcome_limit = u64::try_from(self.limits.recovery.native.outcomes)
            .map_err(|_| NativeSessionError::Capacity("outcome limit"))?;
        let record = match waiting {
            Some(waiting) => super::seal::AnchoredSealRecord {
                ledger: self.ledger,
                expected_prefix: prefix,
                ordinal,
                through: waiting.through,
                bundle: waiting.bundle,
                bytes: waiting.bytes,
                count: waiting.count,
                outcome_limit,
                fold: waiting.fold,
                floors,
                principals: waiting.principals,
            },
            None => super::seal::AnchoredSealRecord {
                ledger: self.ledger,
                expected_prefix: prefix,
                ordinal,
                through: prefix,
                bundle: ContentHash([0; 32]),
                bytes: 0,
                count: 0,
                outcome_limit,
                fold: None,
                floors,
                principals: Vec::new(),
            },
        };
        let encoded = record.encode()?;
        let _permit = self.budget.reserve(
            BudgetKind::Pending,
            BudgetLane::Completion,
            array::<u8>(encoded.len())?,
        )?;
        consensus.propose_borrowed_in(&encoded, BudgetLane::Completion)?;
        self.anchored = Some(record);
        Ok(())
    }
    /// Propose one movement step (25 §6). The step is prepared against the
    /// committed coordinator state under the session verifier before the
    /// record is proposed, so an applicable record is what the log carries;
    /// `Cleanup` additionally waits for every read lease on the group.
    pub(crate) fn propose_range(
        &mut self,
        consensus: &mut DurableNode,
        operation: focal_ranges::RangeOperation,
    ) -> Result<(), NativeSessionError> {
        let status = consensus.scalars();
        self.require_authority(&status)?;
        if self.layout_change.is_some() {
            return Err(NativeSessionError::LayoutChanging);
        }
        if self.retirement.is_some() {
            return Err(NativeSessionError::Retiring);
        }
        if self.sealing() {
            return Err(NativeSessionError::Sealing);
        }
        if !self.pending.is_empty() || self.delivery.is_some() {
            return Err(NativeSessionError::Capacity("pending work before a seal"));
        }
        let sequence = self.sequence()?;
        let pinned = self.committed_core()?.native_stats().pinned_snapshots;
        let ledger = self.ledger;
        let movement = self.movement.as_mut().ok_or(NativeSessionError::Corrupt)?;
        if movement.in_flight.is_some() {
            return Err(NativeSessionError::RangeMoving);
        }
        if matches!(operation, focal_ranges::RangeOperation::Cleanup { .. }) && pinned > 0 {
            return Err(NativeSessionError::Range(focal_ranges::RangeError::Pinned));
        }
        let ordinal = movement
            .coordinator
            .checkpoint()
            .control_ordinal
            .checked_add(1)
            .ok_or(NativeSessionError::Capacity("control ordinal"))?;
        // Checked as an admission: the authority's own share bounds what it proposes.
        let prepared = movement.coordinator.prepare_in(
            BudgetLane::Completion,
            ordinal,
            sequence,
            operation.clone(),
            &movement.verifier,
        )?;
        let record = super::movement::MovementRecord {
            ledger,
            ordinal,
            operation,
        };
        let _permit = self.budget.reserve(
            BudgetKind::Pending,
            BudgetLane::Completion,
            array::<u8>(super::movement::MAX_RECORD_BYTES)?,
        )?;
        let bytes = record.encode()?;
        consensus.propose_borrowed_in(&bytes, BudgetLane::Completion)?;
        movement.in_flight = Some((ordinal, prepared.hash()));
        Ok(())
    }
    pub(crate) fn range_map(&self) -> Result<&focal_ranges::RangeMap, NativeSessionError> {
        self.check()?;
        Ok(self
            .movement
            .as_ref()
            .ok_or(NativeSessionError::Corrupt)?
            .coordinator
            .map())
    }
    pub(crate) fn movement_pending(
        &self,
    ) -> Result<Option<&focal_ranges::TransferState>, NativeSessionError> {
        self.check()?;
        Ok(self
            .movement
            .as_ref()
            .ok_or(NativeSessionError::Corrupt)?
            .pending())
    }
    pub(crate) fn movement_checkpoint(
        &self,
    ) -> Result<&focal_ranges::RangeCheckpoint, NativeSessionError> {
        self.check()?;
        Ok(self
            .movement
            .as_ref()
            .ok_or(NativeSessionError::Corrupt)?
            .coordinator
            .checkpoint())
    }
    pub(crate) fn movement_refusals(&self) -> u64 {
        self.movement
            .as_ref()
            .map_or(0, |movement| movement.refusals)
    }
    pub(crate) fn movement_in_flight(&self) -> bool {
        self.movement
            .as_ref()
            .is_some_and(|movement| movement.in_flight.is_some())
    }
    pub(crate) fn range_verifier(
        &self,
    ) -> Result<super::movement::LedgerRangeVerifier, NativeSessionError> {
        self.check()?;
        Ok(self
            .movement
            .as_ref()
            .ok_or(NativeSessionError::Corrupt)?
            .verifier)
    }
    /// The `Activate` step of the pending transfer over the proofs the
    /// state holds and the progress of the replica-held members that stay.
    pub(crate) fn range_activation_operation(
        &self,
        unchanged: Vec<focal_ranges::RangeProgress>,
    ) -> Result<focal_ranges::RangeOperation, NativeSessionError> {
        self.check()?;
        Ok(self
            .movement
            .as_ref()
            .ok_or(NativeSessionError::Corrupt)?
            .coordinator
            .activation_operation(unchanged)?)
    }
    pub(crate) fn range_activation(
        &self,
        operation: focal_ranges::TransferId,
    ) -> Option<&focal_ranges::ActivationCertificate> {
        self.movement
            .as_ref()
            .and_then(|movement| movement.coordinator.activation(operation))
    }
    pub(crate) fn applied_raft(&self) -> u64 {
        self.applied_raft
    }
    pub(crate) fn configuration_index(&self) -> u64 {
        self.configuration_index
    }
    /// What the engine uses of its share of the session's memory, and the share.
    pub(crate) fn memory_stats(&self) -> focal_memory::BudgetStats {
        self.budget.stats()
    }
    /// The seeded checkpoint waiting for its chunks, if any.
    pub(crate) fn pending_seed(&self) -> Option<&PendingSeed> {
        self.pending_seed.as_ref()
    }
    pub(crate) fn pending_custody(&self) -> Option<&PendingCustody> {
        self.pending_custody.as_ref()
    }
    pub(crate) fn take_pending_custody(&mut self) -> Option<PendingCustody> {
        self.pending_custody.take()
    }

    pub(crate) fn pending_count(&self) -> usize {
        self.pending.len()
    }
    /// This replica's producer incarnation: the configured range until an
    /// authoritative snapshot is installed, then a derived incarnation.
    pub(crate) fn range(&self) -> RangeId {
        self.range
    }
    pub(crate) fn recording_range(&self) -> Option<RangeId> {
        self.recording_range
    }
    pub(crate) fn outcome(
        &self,
        request: impl Into<NativeInvocation>,
    ) -> Result<Option<NativeOutcome>, NativeSessionError> {
        Ok(self.committed_core()?.native_outcome(request)?)
    }
    pub(crate) fn read_at_least(
        &self,
        boundary: NativeReadBoundary,
        status: &focal_consensus::NodeScalars,
    ) -> Result<&Core<NativeState>, NativeSessionError> {
        let core = self.committed_core()?;
        if self.applied_raft < boundary.raft_index
            || core.native_sequence() < boundary.native_sequence
        {
            return Err(NativeSessionError::NotReady {
                leader: status.leader_id,
            });
        }
        Ok(core)
    }
    pub(super) fn require_authority(
        &self,
        status: &focal_consensus::NodeScalars,
    ) -> Result<(), NativeSessionError> {
        self.check()?;
        if !self.is_authoritative(status) {
            return Err(NativeSessionError::NotReady {
                leader: status.leader_id,
            });
        }
        Ok(())
    }
    /// Whether this replica may serve a linearizable read: as the authority,
    /// or as a follower that knows its leader — the read's barrier goes to
    /// the leader through the core, and the answer waits for this copy to
    /// have applied the index it names (27 §5, follower reads).
    pub(crate) fn serves_reads(&self, status: &focal_consensus::NodeScalars) -> bool {
        self.is_authoritative(status)
            || (!self.failed
                && status.leader_id != 0
                && self.genesis.is_some()
                && self.domain.is_some())
    }
    pub(super) fn require_reader(
        &self,
        status: &focal_consensus::NodeScalars,
    ) -> Result<(), NativeSessionError> {
        self.check()?;
        if !self.serves_reads(status) {
            return Err(NativeSessionError::NotReady {
                leader: status.leader_id,
            });
        }
        Ok(())
    }
    /// Hold a barrier answered above the applied index until the entries it
    /// names have been applied; bounded by the reads the core holds in
    /// flight, so a follower far behind refuses new reads rather than grows.
    pub(super) fn park_read(
        &mut self,
        barrier: &focal_consensus::ReadBarrier,
        bound: usize,
    ) -> Result<(), NativeSessionError> {
        if self.parked_reads.len() >= bound {
            return Err(NativeSessionError::Capacity("parked reads"));
        }
        if self.parked_charge.is_none() {
            let permit = self.budget.reserve(
                BudgetKind::Pending,
                BudgetLane::Completion,
                array::<focal_consensus::ReadBarrier>(bound)?,
            )?;
            self.parked_reads
                .try_reserve_exact(bound)
                .map_err(|_| NativeSessionError::Capacity("parked reads"))?;
            self.parked_charge = Some(permit.commit());
        }
        let mut context = reserved(barrier.context.len())?;
        context.extend_from_slice(&barrier.context);
        self.parked_reads.push(focal_consensus::ReadBarrier {
            index: barrier.index,
            context,
        });
        self.reads_parked = self.reads_parked.saturating_add(1);
        Ok(())
    }
    /// The parked barriers this copy has now applied up to, in the order
    /// they were parked; the rest stay.
    pub(super) fn take_applied_parked_reads(&mut self) -> Vec<focal_consensus::ReadBarrier> {
        let applied = self.applied_raft;
        let mut ready = Vec::new();
        let mut index = 0;
        while index < self.parked_reads.len() {
            if self
                .parked_reads
                .get(index)
                .is_some_and(|barrier| barrier.index <= applied)
            {
                let barrier = self.parked_reads.remove(index);
                if ready.try_reserve_exact(1).is_err() {
                    self.parked_reads.insert(index, barrier);
                    break;
                }
                ready.push(barrier);
            } else {
                index = index.saturating_add(1);
            }
        }
        ready
    }

    /// Stage one input against the owner and submit the resulting candidate.
    /// An exact retry of committed work is answered from the root before any
    /// queue, disk or memory admission applies to fresh work.
    /// Whether a session decision is proposed and not yet applied: a layout change, a
    /// retirement or a seal, behind which no candidate is admitted (`admit` refuses it
    /// `LayoutChanging`, `Retiring` or `Sealing`).
    pub(crate) fn decision_in_flight(&self) -> bool {
        self.layout_change.is_some() || self.retirement.is_some() || self.sealing()
    }
    /// A seal of either kind proposed and not yet applied.
    pub(super) fn sealing(&self) -> bool {
        self.seal.is_some() || self.anchored.is_some()
    }
    /// A seal this authority holds for the pipeline to drain (F12,
    /// `SealRule::Anchored`): a bundle under custody, or floors the
    /// pressure forces. Admission holds behind it as behind a decision in
    /// flight, so the drain ends under any load.
    pub(crate) fn decision_wanted(&self) -> bool {
        self.close_wanted || self.waiting_seal.is_some()
    }
    pub(crate) fn admit(
        &mut self,
        consensus: &mut DurableNode,
        prepare: impl FnOnce(
            &mut NativeOwner,
            &S,
            ContentDomainId,
        ) -> Result<NativeStaging, NativeOwnerError>,
    ) -> Result<NativeSubmission, NativeSessionError> {
        let ((), submission) =
            self.admit_with(consensus, BudgetLane::Ordinary, |owner, schemas, domain| {
                prepare(owner, schemas, domain).map(|staged| ((), Some(staged)))
            })?;
        submission.ok_or(NativeSessionError::Failed)
    }
    /// Retire the families `offers` name as the leader's next candidate (doc 31 M2), admitted
    /// as any write is, so it takes its place among them: what the owner made of the offers,
    /// and the batch's submission, none when nothing retires.
    pub(crate) fn retire(
        &mut self,
        consensus: &mut DurableNode,
        offers: &[focal_core::native::retirement_batch::RetireOffer],
    ) -> Result<
        (
            focal_core::native::retirement_batch::RetireBatch,
            Option<NativeSubmission>,
        ),
        NativeSessionError,
    > {
        let cold = match self.limits.retire_rule {
            super::RetireRule::Exact => return Err(NativeSessionError::RetireExact),
            super::RetireRule::Pipeline => None,
            super::RetireRule::Cold => {
                // A cold batch is appended where it applies: this replica must keep the streams.
                if !self.cold.is_kept() {
                    return Err(NativeSessionError::RetireExact);
                }
                Some(self.limits.encoding)
            }
        };
        // Planned once, each offer's family derived once, then staged a candidate at a time
        // through every gate a write meets, while the session has room: a retirement takes all
        // its offers that may leave now, however few families one candidate holds.
        let (mut batch, mut submission) =
            self.admit_with(consensus, BudgetLane::Completion, |owner, _, _| {
                let batch = owner.plan_retirement(offers, cold)?;
                let staged = owner.stage_retirement(&batch, 0)?;
                Ok((batch, staged))
            })?;
        let mut staged = usize::from(submission.is_some());
        while staged > 0 && staged < batch.ends.len() {
            let next = self.admit_with(consensus, BudgetLane::Completion, |owner, _, _| {
                Ok(((), owner.stage_retirement(&batch, staged)?))
            });
            // A candidate refused (no room, the session busy) leaves the rest to a later offer.
            let Ok(((), Some(next))) = next else {
                break;
            };
            submission = Some(next);
            staged = staged.saturating_add(1);
        }
        batch.keep(staged)?;
        Ok((batch, submission))
    }
    /// Admit what `prepare` makes of the owner, a candidate or none, as a write is: every gate a
    /// write meets, the movement's fence, the session's room on `lane` (a retirement's on the
    /// completion lane, as the owner stages it), and the submission.
    fn admit_with<T>(
        &mut self,
        consensus: &mut DurableNode,
        lane: BudgetLane,
        prepare: impl FnOnce(
            &mut NativeOwner,
            &S,
            ContentDomainId,
        ) -> Result<(T, Option<NativeStaging>), NativeOwnerError>,
    ) -> Result<(T, Option<NativeSubmission>), NativeSessionError> {
        let status = consensus.scalars();
        self.require_authority(&status)?;
        if self.layout_change.is_some() {
            return Err(NativeSessionError::LayoutChanging);
        }
        if self.retirement.is_some() {
            return Err(NativeSessionError::Retiring);
        }
        // A seal held for the drain holds admission too: under any load the
        // pipeline then drains and the seal is proposed.
        if self.sealing() || self.decision_wanted() {
            return Err(NativeSessionError::Sealing);
        }
        // Room for the candidate, funded by the session's memory before it is prepared: the
        // candidates a leader holds in flight are as many as its memory funds (`reserve_pending`).
        let full = self.reserve_pending(lane).is_err();
        let domain = self.limits.content_domain;
        let Some(Domain::Active(owner, _)) = self.domain.as_mut() else {
            return Err(NativeSessionError::Failed);
        };
        let owner: &mut NativeOwner = owner;
        // An exact retry never needs a fresh slot, so the owner looks it up first
        // and refuses fresh work itself when its own queue is full.
        let (made, staged) = prepare(owner, &self.schemas, domain)?;
        let Some(staged) = staged else {
            return Ok((made, None));
        };
        // Between a transfer's barrier and its activation nothing lands on a
        // moving member (25 §6): the fresh candidate is discarded and the
        // caller retries after activation.
        if let NativeStaging::Prepared { candidate, .. } = staged
            && let Some(movement) = self.movement.as_ref()
        {
            let fenced = movement.fenced_members();
            if !fenced.is_empty() {
                let layout: Vec<RangeId> = owner.native_layout().ids().collect();
                let touches = owner
                    .prepared_candidate(candidate)?
                    .touched_members()
                    .filter_map(|position| layout.get(position).copied())
                    .any(|member| fenced.contains(&member));
                if touches {
                    owner.discard_from(candidate)?;
                    return Err(NativeSessionError::RangeMoving);
                }
            }
        }
        if let NativeStaging::Prepared { candidate, .. } = staged {
            // Sample disk headroom only for fresh work, and after prepare: a full
            // queue is an accounting inconsistency (the owner's queue is bounded
            // by the same limit) and missing headroom is ordinary pressure, but a
            // transient statfs error must refuse only this fresh candidate - never
            // an exact retry of committed work, which the owner already answered
            // above as Existing and which consumes no new disk. Discard the
            // candidate before returning any refusal.
            // `disk_headroom_ok` needs `&mut self`, so the `owner` borrow taken
            // above must end first (its last use is the movement fence). Discard
            // through a helper that re-borrows the owner rather than holding it
            // across the sample.
            let headroom = match self.disk_headroom_ok(consensus) {
                Ok(headroom) => headroom,
                Err(error) => {
                    self.discard_candidate(candidate)?;
                    return Err(error);
                }
            };
            let refused = if full {
                Some("pending candidates")
            } else if !headroom {
                Some("disk headroom")
            } else if !self.checkpointable()? {
                Some("checkpoint: the root with its candidates past what a checkpoint holds")
            } else {
                None
            };
            if let Some(bound) = refused {
                self.discard_candidate(candidate)?;
                return Err(NativeSessionError::Capacity(bound));
            }
        }
        let submission = self.submit_staged(consensus, staged, status.term);
        // Admitted, what its preparation appended to the payload log is the log's; a candidate
        // a deterministic refusal discarded gave it back.
        if let Some(Domain::Active(owner, _)) = self.domain.as_mut() {
            owner.keep_appends();
        }
        let submission = submission?;
        if matches!(submission, NativeSubmission::Pending { .. }) {
            self.note_pressure()?;
        }
        Ok((made, Some(submission)))
    }
    /// After a fresh admission under `SealRule::Anchored` (F12): once the
    /// committed and pending resident outcomes reach where the pressure rule
    /// could force a floor, ask it; when it would, hold admission for a seal
    /// that carries the floors (`propose_wanted`), else note how far the
    /// window may go before it could. O(1) below that point; the rule's
    /// count of what awaits a seal runs once per distance it grants.
    fn note_pressure(&mut self) -> Result<(), NativeSessionError> {
        if self.limits.seal_rule != super::SealRule::Anchored || self.close_wanted || self.sealing()
        {
            return Ok(());
        }
        let pending = self.pending.len();
        let core = self.committed_core()?;
        let resident = core.resident_outcomes();
        if resident.saturating_add(pending) < self.close_check_at {
            return Ok(());
        }
        let slack = core.close_slack()?;
        if slack == 0 {
            self.close_wanted = true;
        } else {
            self.close_check_at = resident.saturating_add(slack);
        }
        Ok(())
    }
    /// Room for one more submitted candidate: a full queue doubles its capacity, the growth
    /// charged to the session's budget before it is allocated, as the native owner's own queue
    /// grows (`NativeOwner::reserve_pending`). `NativeLimits::pending`, the count this queue was
    /// fixed at, remains the committed headroom of a seal's outcome; a leader's pipeline is as
    /// deep as its memory funds. The growth is charged to `lane`.
    fn reserve_pending(&mut self, lane: BudgetLane) -> Result<(), NativeSessionError> {
        if self.pending.len() < self.pending.capacity() {
            return Ok(());
        }
        let wanted = self
            .pending
            .capacity()
            .max(1)
            .checked_mul(2)
            .ok_or(NativeSessionError::Capacity("pending candidates"))?;
        let bytes = array::<Pending>(wanted)?;
        let permit = self.budget.reserve(BudgetKind::Pending, lane, bytes)?;
        self.pending
            .try_reserve_exact(wanted.saturating_sub(self.pending.len()))
            .map_err(|_| NativeSessionError::Capacity("pending candidates"))?;
        if array::<Pending>(self.pending.capacity())? > bytes {
            return Err(NativeSessionError::Capacity("pending candidates"));
        }
        self._pending_allocation = permit.commit();
        Ok(())
    }
    /// Gives back what a burst grew the chain to once it has drained to a quarter of it, as the
    /// native owner's chain does (`NativeOwner::shrink_pending`): the chain moves into a buffer
    /// half as large, never below the committed headroom it began at, its charge shrunk to match.
    /// A buffer the allocator cannot give leaves the chain as it is, and a charge that cannot
    /// shrink stays as it was, more than the chain holds.
    pub(super) fn shrink_pending(&mut self) {
        let capacity = self.pending.capacity();
        let floor = self.limits.recovery.native.pending;
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
        if let Ok(bytes) = array::<Pending>(self.pending.capacity()) {
            let _ = self._pending_allocation.shrink_to(bytes);
        }
    }
    /// Whether the committed root, with every pending candidate published,
    /// still fits a checkpoint (rule 2): what this session admits it must be
    /// able to checkpoint. A fresh candidate past the checkpoint's rows or
    /// assembled bytes is refused typed at admission, and the session keeps
    /// serving; never admitted and then a checkpoint failed and the session
    /// stopped (a ledger did, at 4,200 claims, before this check).
    fn checkpointable(&self) -> Result<bool, NativeSessionError> {
        let Some(Domain::Active(owner, _)) = self.domain.as_ref() else {
            return Err(NativeSessionError::Failed);
        };
        let projection = owner.checkpoint_projection()?;
        let (rows, bytes) = self.limits.admission_checkpoint();
        Ok(projection.rows <= rows && projection.bytes <= bytes)
    }
    /// Discard a prepared candidate, re-borrowing the active owner. Used when a
    /// refusal is decided after a `&mut self` call has ended the owner borrow.
    fn discard_candidate(&mut self, candidate: NativeCandidate) -> Result<(), NativeSessionError> {
        let Some(Domain::Active(owner, _)) = self.domain.as_mut() else {
            return Err(NativeSessionError::Failed);
        };
        owner.discard_from(candidate)?;
        Ok(())
    }
    /// Sample the WAL filesystem's free space when due and decide whether a
    /// fresh candidate may be admitted. Far above the watermark one sample
    /// covers a bounded run of admissions; near it every admission samples. A
    /// failed sample refuses fresh work rather than promising unknown space.
    fn disk_headroom_ok(&mut self, consensus: &DurableNode) -> Result<bool, NativeSessionError> {
        let headroom = self.limits.disk_headroom_bytes;
        if headroom == 0 {
            return Ok(true);
        }
        let due = match self.disk_sample {
            None => true,
            Some(available) => {
                self.admissions_since_sample >= DISK_SAMPLE_INTERVAL
                    || available < headroom.saturating_mul(2)
            }
        };
        if due {
            self.disk_sample = Some(
                consensus
                    .disk_available_bytes()
                    .map_err(|_| NativeSessionError::Capacity("disk sample"))?,
            );
            self.admissions_since_sample = 0;
        }
        self.admissions_since_sample = self.admissions_since_sample.saturating_add(1);
        Ok(self
            .disk_sample
            .is_some_and(|available| available >= headroom))
    }
    fn submit_staged(
        &mut self,
        consensus: &mut DurableNode,
        staged: NativeStaging,
        term: u64,
    ) -> Result<NativeSubmission, NativeSessionError> {
        let (candidate, outcome) = match staged {
            NativeStaging::Existing {
                outcome,
                candidate: None,
            } => return Ok(NativeSubmission::Committed(outcome)),
            NativeStaging::Existing {
                outcome,
                candidate: Some(candidate),
            } => (candidate, outcome),
            NativeStaging::Prepared { candidate, outcome } => {
                self.pending.push_back(Pending {
                    candidate,
                    outcome,
                    hash: None,
                    term,
                });
                // A candidate admitted is owed a drain, whatever the last
                // flush could not propose.
                self.flush_stalled = false;
                (candidate, outcome)
            }
        };
        // Temporary encoding or proposal pressure preserves the candidate: the
        // caller polls and resubmits the exact request without a second candidate.
        // In a batch, the candidate is encoded now (its refusal is its own) and
        // proposed with the batch's others when the batch ends.
        let flushed = if self.defer_proposals {
            self.encode_proposals().map(drop)
        } else {
            self.flush_proposals(consensus)
        };
        match flushed {
            Ok(()) => {}
            Err(error) => match error.class() {
                FailureClass::Retryable | FailureClass::Authority => {}
                FailureClass::Request => {
                    // A deterministic refusal discarded that candidate and its suffix.
                    if !self
                        .pending
                        .iter()
                        .any(|pending| pending.candidate == candidate)
                    {
                        return Err(error);
                    }
                }
                FailureClass::FailClosed => {
                    self.fail_closed(&error);
                    return Err(error);
                }
            },
        }
        Ok(NativeSubmission::Pending { candidate, outcome })
    }
    /// Whether candidates admitted from now on wait for [`Self::flush_deferred`].
    pub(crate) fn set_proposals_deferred(&mut self, deferred: bool) {
        self.defer_proposals = deferred;
    }
    /// Whether deferred candidates wait to be proposed: a drain is owed them,
    /// unless the last flush could propose none of what waits
    /// ([`Self::flush_stalled`]).
    pub(crate) fn has_deferred(&self) -> bool {
        self.defer_proposals && !self.flush_stalled && self.submitted < self.pending.len()
    }
    /// Propose every candidate the host deferred, together, on an authority;
    /// pressure, authority moving and a refused candidate keep or answer them
    /// as at admission. Only a refusal that stops the session is returned.
    pub(crate) fn flush_deferred(
        &mut self,
        consensus: &mut DurableNode,
    ) -> Option<NativeSessionError> {
        let refusal = if self.submitted >= self.pending.len()
            || !self.is_authoritative(&consensus.scalars())
        {
            None
        } else {
            match self.flush_proposals(consensus) {
                Err(error) if error.class() == FailureClass::FailClosed => {
                    self.fail_closed(&error);
                    Some(error)
                }
                Ok(()) | Err(_) => None,
            }
        };
        self.flush_stalled = self.submitted < self.pending.len();
        refusal
    }
    /// Encode every unsubmitted candidate in chain order, and name the first
    /// of them. Encoding refusals discard the candidate and its suffix.
    pub(super) fn encode_proposals(
        &mut self,
    ) -> Result<Option<NativeCandidate>, NativeSessionError> {
        let Some(Domain::Active(owner, _)) = self.domain.as_mut() else {
            return Err(NativeSessionError::Failed);
        };
        // Only what the last encoding left: the candidates before are encoded already.
        let mut position = self.encoded;
        while let Some(pending) = self.pending.get_mut(position) {
            let encoded = match owner.encode_candidate(pending.candidate, self.limits.encoding) {
                Ok(encoded) => encoded,
                Err(error) => {
                    let error = NativeSessionError::Owner(error);
                    if error.class() == FailureClass::Request {
                        let candidate = pending.candidate;
                        owner.discard_from(candidate)?;
                        self.pending.truncate(position);
                        self.submitted = self.submitted.min(position);
                    }
                    return Err(error);
                }
            };
            pending.hash = Some(encoded.hash());
            position = add(position, 1)?;
            self.encoded = position;
        }
        Ok(self
            .pending
            .get(self.submitted)
            .map(|pending| pending.candidate))
    }
    /// Encode and propose every unsubmitted candidate in chain order, in one
    /// proposal: the leader sends each member one append of them all.
    /// Encoding refusals discard the candidate and its suffix; proposal
    /// pressure keeps them for the next flush, and a batch the log has no
    /// room for whole is proposed a candidate at a time, as many as it has
    /// room for.
    pub(super) fn flush_proposals(
        &mut self,
        consensus: &mut DurableNode,
    ) -> Result<(), NativeSessionError> {
        let Some(first) = self.encode_proposals()? else {
            return Ok(());
        };
        let Some(Domain::Active(owner, _)) = self.domain.as_ref() else {
            return Err(NativeSessionError::Failed);
        };
        let records = owner
            .encoded_from(first)
            .map_err(NativeSessionError::Owner)?;
        // The unsubmitted candidates are the chain's suffix from the first of
        // them, in the owner's chain and in this one alike.
        let unsubmitted = self.pending.len().saturating_sub(self.submitted);
        if records.clone().count() != unsubmitted {
            return Err(NativeSessionError::Failed);
        }
        match consensus.propose_batch_borrowed_in(records, BudgetLane::Completion) {
            Ok(()) => {
                self.submitted = self.pending.len();
                Ok(())
            }
            Err(ConsensusError::Capacity) => self.propose_each(consensus),
            Err(error) => Err(error.into()),
        }
    }
    /// Propose every unsubmitted candidate, encoded, one at a time: as many
    /// as the log has room for; the rest wait for the next flush.
    fn propose_each(&mut self, consensus: &mut DurableNode) -> Result<(), NativeSessionError> {
        let Some(Domain::Active(owner, _)) = self.domain.as_mut() else {
            return Err(NativeSessionError::Failed);
        };
        while let Some(pending) = self.pending.get(self.submitted) {
            let encoded = owner
                .encode_candidate(pending.candidate, self.limits.encoding)
                .map_err(NativeSessionError::Owner)?;
            consensus.propose_borrowed_in(encoded.bytes(), BudgetLane::Completion)?;
            self.submitted = add(self.submitted, 1)?;
            self.encoded = self.encoded.max(self.submitted);
        }
        Ok(())
    }
}
