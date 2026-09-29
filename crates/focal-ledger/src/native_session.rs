//! Native-only durable genesis composition. This type is deliberately separate
//! from the live V1 Session; legacy migration and service selection are absent.
//! Followers own a passive Core. Admission credits are reconstructed once at a
//! current-term read barrier, never by replaying participant commands.
pub(crate) use apply::NativeOutput;
use engine::NativeEngine;
use focal_consensus::{
    ConsensusError, DurableNode, Message, NodeConfig, NodeEvents, NodeStatus, StateRole,
};
use focal_core::Core;
use focal_core::native::{
    NativeCandidate, NativeClaimDeadlineInput, NativeContentProfile, NativeContext,
    NativeDeadlineInput, NativeError, NativeInput, NativeInvocation, NativeMonitorDeadlineInput,
    NativeOutcome, NativeOwner, NativeOwnerError, NativeStaging, NativeState, input_codec,
    record_codec::{self as record, recovery},
};
use focal_evidence::{
    ContentReader, ContentStore, NativeEvidenceError, NativeSchemaVerifier, SeedReader, SeedStore,
};
use focal_memory::{Allocation, BudgetKind, BudgetLane, MemoryBudget, MemoryError, RangeId};
use focal_model::{ContentHash, LedgerId, SessionSeq};
use std::{collections::VecDeque, path::Path};

#[path = "native_session_apply.rs"]
mod apply;
#[path = "native_session_checkpoint.rs"]
mod checkpoint;
#[cfg(test)]
#[path = "native_session_cluster_tests.rs"]
mod cluster_tests;
#[path = "native_session_engine.rs"]
pub(crate) mod engine;
#[path = "native_session_genesis.rs"]
pub mod genesis;
#[path = "native_session_range.rs"]
pub mod range;
#[path = "native_session_retention.rs"]
pub mod retention;
#[path = "native_session_retirement.rs"]
pub mod retirement;
pub use range::LayoutOperation;
pub use retirement::RetirementRecord;
#[path = "native_session_movement.rs"]
pub mod movement;
pub use movement::{LedgerRangeVerifier, MovementRecord};
#[cfg(test)]
#[path = "native_session_tests.rs"]
pub(crate) mod tests;
#[cfg(test)]
#[path = "native_session_workflow_tests.rs"]
mod workflow_tests;

/// Host-supplied bounds, independent of participant-authored requirements.
#[derive(Debug, Clone, Copy)]
pub struct NativeSessionLimits {
    pub recovery: recovery::Limits,
    pub encoding: record::EncodingLimits,
    pub inspection: record::InspectionLimits,
    pub checkpoint: crate::native_checkpoint::Limits,
    /// Largest borrowed native input frame and its cumulative decode work.
    pub frame_bytes: usize,
    pub decode_work: input_codec::DecodeWork,
    pub memory_bytes: usize,
    pub completion_reserve_bytes: usize,
    pub content_domain: focal_model::ContentDomainId,
    /// Free bytes the WAL filesystem must keep before a fresh candidate is
    /// admitted; zero disables the watermark. Admission reserves memory for the
    /// report, its record buffer and retained pages, never disk, quorum or
    /// fan-out, so this refuses at the door instead of after an in-memory
    /// acknowledgement. Exact retries of committed work never need headroom.
    pub disk_headroom_bytes: u64,
    /// How committed records this session did not author are materialized
    /// (doc 25 §2): one worker replays them one at a time; more stage a
    /// delivery's consecutive records in dependency waves and install them
    /// in order, byte-identical to the serial replay.
    pub materializer: record::materialize::MaterializerLimits,
    /// Bounds on the movement coordinator (25 §6): members, historical maps,
    /// pins and the movement section of a checkpoint.
    pub ranges: focal_ranges::RangeLimits,
}
/// What the materializer has done since this session opened.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct MaterializerStats {
    /// Deliveries whose records were materialized as one batch.
    pub batches: u64,
    /// Records those batches applied.
    pub records: u64,
    /// Batches in which at least one wave staged several records at once.
    pub parallel_batches: u64,
    pub waves: u64,
    /// Reads the barrier found stale, each discarding a speculative suffix.
    pub violations: u64,
    pub serial_fallbacks: u64,
    /// Wall time spent materializing batches, in microseconds: a diagnostic
    /// for the operator and the measurement suite, never an input to state.
    pub micros: u64,
    /// Records replayed one at a time outside any batch, and their time.
    pub serial_records: u64,
    pub serial_micros: u64,
}
impl NativeSessionLimits {
    /// Production defaults for one hosted ledger: authored bodies up to the
    /// model maxima, 6 MiB records, 1 MiB frames, a 128 MiB engine budget and
    /// a 64 MiB free-space watermark on the WAL filesystem.
    /// The frame decode limits every host derives from these limits; the
    /// engine validates the same derivation once at construction.
    pub fn decode_limits(
        &self,
    ) -> Result<focal_core::native::input_codec::NativeDecodeLimits, NativeSessionError> {
        Ok(
            focal_core::native::input_codec::NativeDecodeLimits::for_native(
                self.recovery.native,
                self.frame_bytes,
                self.decode_work,
            )
            .map_err(focal_core::native::NativeOwnerError::from)?,
        )
    }
    pub fn standard(content_domain: focal_model::ContentDomainId) -> Self {
        use focal_model::lifecycle::{
            aggregation, artifact_descriptor, claim_descriptor, evidence::ResponseLimits,
            validation, validation_descriptor,
        };
        let declaration = validation::Limits {
            handlers: 32,
            attempts: 64,
            slot_bytes: 4096,
        };
        Self {
            recovery: recovery::Limits {
                // Bounded owner shapes: every completion-class admission funds
                // its future record buffer from these counts against the
                // encoding envelope below, so they must stay proportionate.
                native: focal_core::native::NativeLimits {
                    range: focal_memory::RangeConfig {
                        page_entries: 128,
                        max_batch_entries: 256,
                        ..focal_memory::RangeConfig::default()
                    },
                    plan_nodes: 64,
                    plan_edges: 65_536,
                    preparation_bytes: 1 << 20,
                    evaluations_per_claim: 64,
                    ..focal_core::native::NativeLimits::default()
                },
                acceptance: aggregation::Limits {
                    max_slots: 256,
                    max_checks: 4096,
                    max_results: 8192,
                    max_updates: 8192,
                },
                artifact: artifact_descriptor::Limits {
                    kind_bytes: 1024,
                    metadata_bytes: 65_536,
                    inline_bytes: 1024 * 1024,
                    inputs: 256,
                    visibility_labels: 256,
                    visibility_label_bytes: 4096,
                    construction_bytes: 4 * 1024 * 1024,
                },
                claim: claim_descriptor::Limits {
                    description_bytes: 65_536,
                    relations: 4096,
                    scopes: 256,
                    scope_key_bytes: 4096,
                    requirements: 4096,
                    slots: 256,
                    checks: 4096,
                    construction_bytes: 4 * 1024 * 1024,
                },
                declaration,
                validation: validation_descriptor::Limits {
                    declaration,
                    description_bytes: 65_536,
                    quality_bar_bytes: 65_536,
                    contributors: 256,
                    construction_bytes: 4 * 1024 * 1024,
                },
                response: ResponseLimits {
                    artifacts: 256,
                    diagnostics: 256,
                    summary_bytes: 65_536,
                    construction_bytes: 4 * 1024 * 1024,
                },
                creation_objects: 4096,
                work: recovery::Work {
                    parsing: 1 << 30,
                    source: 1 << 30,
                    model: 1 << 30,
                    lookup: 1 << 30,
                },
            },
            encoding: record::EncodingLimits {
                // One record: a preparation-sized body at the codec's fourfold
                // expansion plus every fixed row, index rows included.
                bytes: 6 << 20,
                visits: 1 << 30,
                rows: 100_000,
            },
            inspection: record::InspectionLimits {
                bytes: 6 << 20,
                visits: 1 << 30,
                rows: 100_000,
                row_bytes: 6 << 20,
            },
            checkpoint: crate::native_checkpoint::Limits::default(),
            frame_bytes: 1 << 20,
            decode_work: input_codec::DecodeWork {
                parse: 1 << 28,
                source: 1 << 28,
                model: 1 << 28,
                acceptance: 1 << 28,
                native: 1 << 28,
            },
            memory_bytes: 128 << 20,
            completion_reserve_bytes: 16 << 20,
            content_domain,
            disk_headroom_bytes: 64 << 20,
            materializer: record::materialize::MaterializerLimits::default(),
            ranges: focal_ranges::RangeLimits::default(),
        }
    }
}
/// Fresh admissions between free-space samples while far above the watermark.

#[derive(Debug, thiserror::Error)]
pub enum NativeSessionError {
    #[error(transparent)]
    Native(#[from] NativeError),
    #[error(transparent)]
    Owner(#[from] NativeOwnerError),
    #[error(transparent)]
    Consensus(#[from] ConsensusError),
    #[error(transparent)]
    Memory(#[from] MemoryError),
    #[error("native record: {0}")]
    Codec(#[from] record::CodecError),
    #[error("native session checkpoint: {0}")]
    Checkpoint(#[from] crate::native_checkpoint::Error),
    #[error("native session is not authoritative; known leader {leader}")]
    NotReady { leader: u64 },
    #[error("native session capacity exceeded")]
    Capacity,
    #[error("native session identity, profile or committed prefix mismatch")]
    Corrupt,
    #[error("native-only session refuses legacy history or migration")]
    Legacy,
    #[error("native session stopped after committed application failure; reopen to recover")]
    Failed,
    #[error("content named by a committed fact is not local yet; poll again once custody arrives")]
    CustodyPending,
    #[error("a committed layout change is in flight; propose again once it applies")]
    LayoutChanging,
    #[error("the member this mutation touches is moving; retry after activation")]
    RangeMoving,
    #[error("range movement: {0:?}")]
    Range(focal_ranges::RangeError),
    #[error("a committed retirement is in flight; propose again once it applies")]
    Retiring,
    #[error("retirement refused: {0:?}")]
    Retirement(focal_core::native::retirement::RetirementRefusal),
}
impl From<focal_ranges::RangeError> for NativeSessionError {
    fn from(error: focal_ranges::RangeError) -> Self {
        match error {
            focal_ranges::RangeError::Memory(memory) => Self::Memory(memory),
            other => Self::Range(other),
        }
    }
}

/// How the session treats a failure. Corruption is never retried; a recoverable
/// resource condition never stops the session permanently.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FailureClass {
    /// Memory, capacity or outstanding persistence: keep state, poll again.
    Retryable,
    /// Authority moved: candidates stay retained for evidence-based disposition.
    Authority,
    /// A refusal scoped to one request or candidate; the session continues.
    Request,
    /// Corruption or invariant failure: no further admission until reopen.
    FailClosed,
}
impl NativeSessionError {
    pub fn class(&self) -> FailureClass {
        use FailureClass::*;
        fn native(error: &NativeError) -> FailureClass {
            match error {
                NativeError::Memory(_) | NativeError::Capacity(_) => Retryable,
                NativeError::Evidence(NativeEvidenceError::Memory(_)) => Retryable,
                NativeError::Evidence(NativeEvidenceError::Content(content)) => match content {
                    focal_evidence::ContentError::Io(_)
                    | focal_evidence::ContentError::Locked
                    | focal_evidence::ContentError::Capacity => Retryable,
                    _ => FailClosed,
                },
                NativeError::Evidence(_) => Request,
                NativeError::Contract(_) | NativeError::RequestConflict => Request,
            }
        }
        match self {
            Self::Capacity
            | Self::Memory(_)
            | Self::CustodyPending
            | Self::LayoutChanging
            | Self::Retiring
            | Self::RangeMoving => Retryable,
            Self::Retirement(_) => Request,
            Self::Range(
                focal_ranges::RangeError::Capacity | focal_ranges::RangeError::Memory(_),
            ) => Retryable,
            Self::Range(_) => Request,
            Self::Native(error) => native(error),
            Self::Owner(NativeOwnerError::Native(error)) => native(error),
            Self::Owner(NativeOwnerError::Input(_) | NativeOwnerError::Record(_)) => Request,
            Self::Owner(_) => FailClosed,
            Self::Consensus(
                ConsensusError::Capacity
                | ConsensusError::PersistencePending
                | ConsensusError::CheckpointIndex,
            ) => Retryable,
            Self::Consensus(ConsensusError::NotLeader { .. }) | Self::NotReady { .. } => Authority,
            Self::Consensus(_)
            | Self::Codec(_)
            | Self::Checkpoint(_)
            | Self::Corrupt
            | Self::Legacy
            | Self::Failed => FailClosed,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NativeSubmission {
    Committed(NativeOutcome),
    Pending {
        candidate: NativeCandidate,
        outcome: NativeOutcome,
    },
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NativeCommit {
    pub raft_index: u64,
    pub raft_term: u64,
    pub record_hash: ContentHash,
    pub outcome: NativeOutcome,
}
/// Opaque caller correlation for a linearizable read barrier. It is not a
/// capability: a caller-constructed scalar cannot substitute for the barrier.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReadCorrelation(pub [u8; 16]);
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NativeReadBoundary {
    pub correlation: ReadCorrelation,
    pub raft_index: u64,
    pub native_sequence: SessionSeq,
}
/// Trusted timer delivery. Time comes from the publishing host, never from a
/// participant frame.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NativeTimerInput {
    Evaluation(NativeDeadlineInput),
    Claim(NativeClaimDeadlineInput),
    Monitor(NativeMonitorDeadlineInput),
}

/// Consensus buffers retain their original permit. Native summaries are copied
/// scalars with their own exact output allocation; neither exposes pending rows.
/// `flush_refusal` reports a proposal flush that failed after these commits were
/// delivered; the commits remain observable and reconcilable regardless.
pub struct NativeSessionEvents {
    pub consensus: NodeEvents,
    pub committed: Vec<NativeCommit>,
    pub read_boundaries: Vec<NativeReadBoundary>,
    pub flush_refusal: Option<NativeSessionError>,
    _allocation: Allocation,
}
/// A freshly opened session together with the output recovered during startup.
pub struct Opened<S: NativeSchemaVerifier> {
    pub session: NativeSession<S>,
    pub initial: NativeSessionEvents,
}

/// One durable native-only session over its own consensus replica and exclusive
/// content writer. The engine inside is the same code the unified Session hosts;
/// this wrapper only owns the physical resources and forwards to it.
pub struct NativeSession<S: NativeSchemaVerifier> {
    consensus: DurableNode,
    store: ContentStore,
    seeds: SeedStore,
    engine: NativeEngine<S>,
}

/// A seeded checkpoint a replica cannot install until every chunk it names
/// is local (25 §5): the snapshot's Raft coordinates and the chunks missing.
#[derive(Debug)]
pub struct PendingSeed {
    pub index: u64,
    pub term: u64,
    pub missing: Vec<ContentHash>,
    _allocation: Allocation,
}
impl PendingSeed {
    pub(crate) fn new(
        index: u64,
        term: u64,
        missing: Vec<ContentHash>,
        budget: &MemoryBudget,
    ) -> Result<Self, NativeSessionError> {
        let allocation = budget
            .reserve(
                BudgetKind::Recovery,
                BudgetLane::Completion,
                array::<ContentHash>(missing.capacity())?,
            )?
            .commit();
        Ok(Self {
            index,
            term,
            missing,
            _allocation: allocation,
        })
    }
    /// A copy of the missing chunks a host can carry away.
    pub fn missing_chunks(&self) -> Result<Vec<ContentHash>, NativeSessionError> {
        let mut chunks = Vec::new();
        chunks
            .try_reserve_exact(self.missing.len())
            .map_err(|_| NativeSessionError::Capacity)?;
        chunks.extend_from_slice(&self.missing);
        Ok(chunks)
    }
}

/// The content objects a retained delivery could not read locally (24 §20):
/// the artifacts a committed record or an installed checkpoint names whose
/// objects this replica does not hold yet. Hosts pull them from a required
/// copy and poll again; the delivery resumes once they are local.
#[derive(Debug)]
pub struct PendingCustody {
    pub missing: Vec<focal_model::ContentRef>,
    _allocation: Allocation,
}
/// The objects one retained delivery reports at most.
pub const MAX_PENDING_CUSTODY: usize = 64;
impl PendingCustody {
    pub(crate) fn new(
        missing: Vec<focal_model::ContentRef>,
        budget: &MemoryBudget,
    ) -> Result<Self, NativeSessionError> {
        let allocation = budget
            .reserve(
                BudgetKind::Recovery,
                BudgetLane::Completion,
                array::<focal_model::ContentRef>(missing.capacity())?,
            )?
            .commit();
        Ok(Self {
            missing,
            _allocation: allocation,
        })
    }
    /// A copy of the missing objects a host can carry away.
    pub fn missing_objects(&self) -> Result<Vec<focal_model::ContentRef>, NativeSessionError> {
        let mut objects = Vec::new();
        objects
            .try_reserve_exact(self.missing.len())
            .map_err(|_| NativeSessionError::Capacity)?;
        objects.extend(self.missing.iter().cloned());
        Ok(objects)
    }
}
/// A custody reader that remembers which objects the local store did not
/// hold, so a retained delivery can name what its host must pull.
pub(crate) struct RecordingReader<'a> {
    inner: &'a ContentReader,
    /// Materialization workers read through the same recorder; the lock is
    /// held only to note a missing object.
    missing: std::sync::Mutex<Vec<focal_model::ContentRef>>,
}
impl<'a> RecordingReader<'a> {
    pub(crate) fn new(inner: &'a ContentReader) -> Self {
        Self {
            inner,
            missing: std::sync::Mutex::new(Vec::new()),
        }
    }
    /// The objects the store lacked, bounded and without repeats; the
    /// recorder is empty afterwards.
    pub(crate) fn take_missing(&self) -> Vec<focal_model::ContentRef> {
        match self.missing.lock() {
            Ok(mut missing) => std::mem::take(&mut *missing),
            Err(poisoned) => std::mem::take(&mut *poisoned.into_inner()),
        }
    }
}
impl focal_evidence::NativeCustodyReader for RecordingReader<'_> {
    fn read_content(
        &self,
        reference: &focal_model::ContentRef,
        budget: usize,
    ) -> Result<Vec<u8>, focal_evidence::ContentError> {
        let result = self.inner.read_content(reference, budget);
        if let Err(focal_evidence::ContentError::Io(error)) = &result
            && error.kind() == std::io::ErrorKind::NotFound
            && let Ok(mut missing) = self.missing.lock()
            && missing.len() < MAX_PENDING_CUSTODY
            && !missing.iter().any(|known| known == reference)
            && missing.try_reserve(1).is_ok()
        {
            missing.push(reference.clone());
        }
        result
    }
}

/// The single durable capability identity: the enclosing checkpoint codec's
/// descriptor hash covers the session, input, mutation and root envelopes.
fn decoder() -> [u8; 32] {
    crate::native_checkpoint::format_hash().0
}
fn add(a: usize, b: usize) -> Result<usize, NativeSessionError> {
    a.checked_add(b).ok_or(NativeSessionError::Capacity)
}
fn array<T>(count: usize) -> Result<usize, NativeSessionError> {
    let bytes = count
        .checked_mul(size_of::<T>())
        .ok_or(NativeSessionError::Capacity)?;
    add(
        bytes,
        if count == 0 {
            0
        } else {
            focal_memory::ALLOCATOR_OVERHEAD
        },
    )
}
fn reserved<T>(count: usize) -> Result<Vec<T>, NativeSessionError> {
    let mut values = Vec::new();
    values
        .try_reserve_exact(count)
        .map_err(|_| NativeSessionError::Capacity)?;
    if values.capacity() > count {
        return Err(NativeSessionError::Capacity);
    }
    Ok(values)
}

impl<S: NativeSchemaVerifier> NativeSession<S> {
    pub fn decoder_hash() -> ContentHash {
        crate::native_checkpoint::format_hash()
    }

    #[allow(clippy::too_many_arguments)] // Explicit trusted physical identity and bounded custody.
    pub fn open(
        path: impl AsRef<Path>,
        config: NodeConfig,
        ledger: LedgerId,
        range: RangeId,
        profile: NativeContentProfile,
        limits: NativeSessionLimits,
        parent: &MemoryBudget,
        store: ContentStore,
        schemas: S,
    ) -> Result<Opened<S>, NativeSessionError> {
        Self::from_node(
            DurableNode::open_in(config, path, parent)?,
            ledger,
            range,
            profile,
            limits,
            parent,
            store,
            schemas,
        )
    }

    /// Recovered messages, read states and committed summaries are returned to
    /// the caller as the initial output; nothing observed at startup is dropped.
    #[allow(clippy::too_many_arguments)] // No hidden default or inferred recovery provenance.
    pub fn from_node(
        mut consensus: DurableNode,
        ledger: LedgerId,
        range: RangeId,
        profile: NativeContentProfile,
        limits: NativeSessionLimits,
        parent: &MemoryBudget,
        store: ContentStore,
        schemas: S,
    ) -> Result<Opened<S>, NativeSessionError> {
        if !consensus.is_budgeted_within(parent) {
            return Err(NativeSessionError::Capacity);
        }
        let is_new = consensus.required_decoder().is_none();
        if !is_new && consensus.required_decoder() != Some(decoder()) {
            return Err(NativeSessionError::Legacy);
        }
        consensus.confirm_decoder(decoder())?;
        if is_new {
            let initial = consensus.drain()?;
            if initial.applied_index != 0
                || !initial.committed.is_empty()
                || !initial.membership.is_empty()
                || initial.snapshot.is_some()
                || !initial.messages.is_empty()
            {
                return Err(NativeSessionError::Legacy);
            }
            consensus.begin_decoder_floor(decoder())?;
            consensus.finish_decoder_floor()?;
        }
        let reader = ContentReader::open(store.root())
            .map_err(|error| NativeSessionError::Native(NativeEvidenceError::from(error).into()))?;
        let seeds = SeedStore::open(store.root().join("seeds"), consensus.disk_budget())
            .map_err(|error| NativeSessionError::Native(NativeEvidenceError::from(error).into()))?;
        let engine = NativeEngine::new(
            ledger,
            range,
            profile,
            limits,
            parent,
            engine::NativeSources {
                reader,
                seeds: seeds.reader(),
            },
            schemas,
        )?;
        let mut session = Self {
            consensus,
            store,
            seeds,
            engine,
        };
        let initial = session.poll()?;
        Ok(Opened { session, initial })
    }

    pub fn status(&self) -> NodeStatus {
        self.consensus.status()
    }
    pub fn is_authoritative(&self) -> bool {
        self.engine.is_authoritative(&self.consensus.status())
    }
    pub fn genesis(&self) -> Option<ContentHash> {
        self.engine.genesis()
    }
    pub fn committed_core(&self) -> Result<&Core<NativeState>, NativeSessionError> {
        self.engine.committed_core()
    }
    pub fn sequence(&self) -> Result<SessionSeq, NativeSessionError> {
        self.engine.sequence()
    }
    pub fn applied_raft(&self) -> u64 {
        self.engine.applied_raft()
    }
    pub fn configuration_index(&self) -> u64 {
        self.engine.configuration_index()
    }
    pub fn pending_count(&self) -> usize {
        self.engine.pending_count()
    }
    pub fn range(&self) -> RangeId {
        self.engine.range()
    }
    pub fn recording_range(&self) -> Option<RangeId> {
        self.engine.recording_range()
    }
    /// What the materializer has done since this session opened.
    pub fn materializer_stats(&self) -> MaterializerStats {
        self.engine.materializer
    }
    /// A digest of the committed native rows at the current prefix: the
    /// checkpoint encoding's content hash, so two sessions at one prefix agree
    /// exactly when their rows are byte-identical.
    /// The committed range layout (25 §4): each member's durable identity
    /// and the affinity it starts at.
    pub fn native_layout(
        &self,
    ) -> Result<impl Iterator<Item = (RangeId, Option<[u8; 16]>)> + '_, NativeSessionError> {
        Ok(self.committed_core()?.native_layout().boundaries())
    }
    /// The epoch of the committed layout: zero at genesis, one more per
    /// applied layout record.
    pub fn native_layout_epoch(&self) -> Result<u64, NativeSessionError> {
        Ok(self.committed_core()?.native_layout().epoch())
    }
    /// Propose one layout change as a session decision (25 §4): the
    /// authority commits a layout record every replica applies between
    /// native records. The change is checked against the committed layout
    /// first; while it is in flight native proposals are refused with
    /// `LayoutChanging`, and it is refused itself while candidates are
    /// pending (they hold fragments of the current layout) or while another
    /// change is in flight.
    pub fn propose_layout(&mut self, operation: LayoutOperation) -> Result<(), NativeSessionError> {
        self.engine.propose_layout(&mut self.consensus, operation)
    }
    /// The prefix the archive reports holding, the retention floor's bound
    /// (26 §3); restored from this replica's checkpoints.
    pub fn archived_through(&self) -> SessionSeq {
        self.engine.archived_through()
    }
    /// The archive's report that it holds every proof through `through`.
    pub fn note_archived(&mut self, through: SessionSeq) {
        self.engine.note_archived(through);
    }
    /// Propose one family's retirement as a session decision (26 §4): the
    /// family is derived from the committed state first, so an applicable
    /// record is what the log carries. Refused while candidates are
    /// pending, a layout change, movement step or another retirement is in
    /// flight, or the family is ineligible.
    pub fn propose_retirement(
        &mut self,
        root: focal_model::ClaimId,
        bundle: ContentHash,
        bytes: u64,
        through: SessionSeq,
    ) -> Result<(), NativeSessionError> {
        self.engine
            .propose_retirement(&mut self.consensus, root, bundle, bytes, through)
    }
    /// The retirement this authority proposed and has not seen applied.
    pub fn retirement_in_flight(&self) -> Option<RetirementRecord> {
        self.engine.retirement_in_flight()
    }
    /// Families retired through this replica's applied prefix (26 §4),
    /// counted from genesis or the checkpoint that seeded it.
    pub fn retired_families(&self) -> u64 {
        self.engine.retired_families()
    }
    /// Propose one movement step as a session decision (25 §6): checked
    /// against the committed coordinator state first, refused while
    /// candidates are pending, a layout change or another step is in flight,
    /// and — for `Cleanup` — while any read lease pins the group.
    pub fn propose_range(
        &mut self,
        operation: focal_ranges::RangeOperation,
    ) -> Result<(), NativeSessionError> {
        self.engine.propose_range(&mut self.consensus, operation)
    }
    /// The committed movement map: every member's span, generation, holder
    /// and readers under the current range epoch.
    pub fn range_map(&self) -> Result<&focal_ranges::RangeMap, NativeSessionError> {
        self.engine.range_map()
    }
    pub fn range_epoch(&self) -> Result<focal_model::RouteEpoch, NativeSessionError> {
        Ok(self.range_map()?.epoch())
    }
    /// The transfer in progress, if any.
    pub fn movement_pending(
        &self,
    ) -> Result<Option<&focal_ranges::TransferState>, NativeSessionError> {
        self.engine.movement_pending()
    }
    /// The coordinator state as a checkpoint carries it.
    pub fn movement_checkpoint(
        &self,
    ) -> Result<&focal_ranges::RangeCheckpoint, NativeSessionError> {
        self.engine.movement_checkpoint()
    }
    /// Committed movement records the state refused (inert on every replica).
    pub fn movement_refusals(&self) -> u64 {
        self.engine.movement_refusals()
    }
    /// Whether this authority has a movement record proposed and not applied.
    pub fn movement_in_flight(&self) -> bool {
        self.engine.movement_in_flight()
    }
    /// The verifier this session applies movement records under; a test or
    /// host attests proofs with it before proposing them.
    pub fn range_verifier(&self) -> Result<LedgerRangeVerifier, NativeSessionError> {
        self.engine.range_verifier()
    }
    /// The `Activate` step of the pending transfer, built over the proofs
    /// the committed state holds.
    pub fn range_activation_operation(
        &self,
        unchanged: Vec<focal_ranges::RangeProgress>,
    ) -> Result<focal_ranges::RangeOperation, NativeSessionError> {
        self.engine.range_activation_operation(unchanged)
    }
    /// The activation certificate of a completed transfer, while its map is
    /// retained in history.
    pub fn range_activation(
        &self,
        operation: focal_ranges::TransferId,
    ) -> Option<&focal_ranges::ActivationCertificate> {
        self.engine.range_activation(operation)
    }
    pub fn native_state_digest(&self) -> Result<ContentHash, NativeSessionError> {
        let core = self.committed_core()?;
        let limits = self.engine.limits.checkpoint;
        Ok(record::checkpoint::rows_digest(
            core,
            record::EncodingLimits {
                bytes: limits.bytes,
                visits: limits.visits,
                rows: limits.rows,
            },
        )?)
    }
    pub fn outcome(
        &self,
        request: impl Into<NativeInvocation>,
    ) -> Result<Option<NativeOutcome>, NativeSessionError> {
        self.engine.outcome(request)
    }
    pub fn read_at_least(
        &self,
        boundary: NativeReadBoundary,
    ) -> Result<&Core<NativeState>, NativeSessionError> {
        self.engine
            .read_at_least(boundary, &self.consensus.status())
    }
    pub fn campaign(&mut self) -> Result<(), NativeSessionError> {
        self.engine.check()?;
        self.consensus.campaign()?;
        Ok(())
    }
    pub fn tick(&mut self) -> Result<(), NativeSessionError> {
        self.engine.check()?;
        self.consensus.tick()?;
        Ok(())
    }
    pub fn step_authenticated(
        &mut self,
        peer: u64,
        bytes: &[u8],
    ) -> Result<(), NativeSessionError> {
        self.engine.check()?;
        self.consensus.step_authenticated(peer, bytes)?;
        Ok(())
    }
    pub fn step(&mut self, message: Message) -> Result<(), NativeSessionError> {
        self.engine.check()?;
        self.consensus.step(message)?;
        Ok(())
    }
    /// Planned handover: the current authority asks `node` to campaign at once.
    /// Authority moves only through the committed readiness barrier of the new
    /// term; until then admission is refused on both nodes.
    pub fn transfer_leader(&mut self, node: u64) -> Result<(), NativeSessionError> {
        self.engine.check()?;
        self.consensus.transfer_leader(node)?;
        Ok(())
    }
    #[cfg(test)]
    pub(super) fn set_randomized_election_timeout(
        &mut self,
        ticks: usize,
    ) -> Result<(), NativeSessionError> {
        self.engine.check()?;
        self.consensus.set_randomized_election_timeout(ticks)?;
        Ok(())
    }
    /// Transport feedback for a snapshot sent to `node`; acceptance never proves
    /// installation, so consensus retries or re-probes as needed.
    pub fn report_snapshot(
        &mut self,
        node: u64,
        status: focal_consensus::SnapshotStatus,
    ) -> Result<(), NativeSessionError> {
        self.engine.check()?;
        self.consensus.report_snapshot(node, status)?;
        Ok(())
    }
    #[cfg(test)]
    pub(super) fn budget_for_test(&self) -> &MemoryBudget {
        self.engine.budget()
    }
    #[cfg(test)]
    pub(super) fn restore_snapshot_for_test(
        &mut self,
        snapshot: &focal_consensus::AppliedSnapshot,
    ) -> Result<(), NativeSessionError> {
        self.engine.restore_snapshot(snapshot, &self.consensus)
    }
    #[cfg(test)]
    pub(super) fn oldest_candidate_for_test(&self) -> Option<NativeCandidate> {
        self.engine.pending.front().map(|pending| pending.candidate)
    }
    #[cfg(test)]
    pub(super) fn store_for_test(&self) -> &ContentStore {
        &self.store
    }
    #[cfg(test)]
    pub(super) fn store_for_test_mut(&mut self) -> &mut ContentStore {
        &mut self.store
    }
    /// A fail-closed condition stopped admission and delivery; reopening the
    /// log is the only recovery.
    pub fn failed(&self) -> bool {
        self.engine.failed()
    }
    pub fn persistence_pending(&self) -> bool {
        self.consensus.persistence_pending()
    }
    pub fn checkpoint_pending(&self) -> bool {
        self.consensus.checkpoint_pending()
    }

    pub fn propose(
        &mut self,
        context: NativeContext,
        input: NativeInput,
    ) -> Result<NativeSubmission, NativeSessionError> {
        let store = &mut self.store;
        self.engine
            .admit(&mut self.consensus, |owner, schemas, domain| {
                owner.prepare_with_custody(context, input, store, domain, schemas)
            })
    }
    /// Borrowed native input frame from an authenticated participant. Timer
    /// namespaces are refused by the owner's actor ingress; the frame's authored
    /// principal must match the authenticated context.
    pub fn propose_native_frame(
        &mut self,
        context: NativeContext,
        frame: &[u8],
    ) -> Result<NativeSubmission, NativeSessionError> {
        let limits = self.engine.decode_limits()?;
        let store = &mut self.store;
        self.engine
            .admit(&mut self.consensus, |owner, schemas, domain| {
                owner.prepare_frame_with_custody(context, frame, limits, store, domain, schemas)
            })
    }
    /// Trusted timer delivery from the host's clock. Participants cannot reach
    /// this path; the owner deduplicates exact firings through the outcome row.
    pub fn deliver_native_timer(
        &mut self,
        input: NativeTimerInput,
        logical_time: u64,
    ) -> Result<NativeSubmission, NativeSessionError> {
        self.engine
            .admit(&mut self.consensus, |owner, _, _| match input {
                NativeTimerInput::Evaluation(input) => {
                    owner.prepare_evaluation_deadline(input, logical_time)
                }
                NativeTimerInput::Claim(input) => owner.prepare_claim_deadline(input, logical_time),
                NativeTimerInput::Monitor(input) => {
                    owner.prepare_monitor_deadline(input, logical_time)
                }
            })
    }
    pub fn poll(&mut self) -> Result<NativeSessionEvents, NativeSessionError> {
        self.engine.poll(&mut self.consensus)
    }
    pub fn try_poll(&mut self) -> Result<Option<NativeSessionEvents>, NativeSessionError> {
        self.engine.try_poll(&mut self.consensus)
    }
    /// Request a quorum read barrier tagged with the caller's correlation. The
    /// completion arrives in a later poll as a `NativeReadBoundary`; the readiness
    /// namespace is internal and cannot be requested here.
    pub fn read_index(&mut self, correlation: ReadCorrelation) -> Result<(), NativeSessionError> {
        self.engine.read_index(&mut self.consensus, correlation)
    }
    /// Encode the committed Core at the fully delivered prefix under a retained
    /// output permit and hand bytes and permit together to consensus. Completion
    /// requires the actual durable fence observed by `poll`/`try_poll`.
    pub fn begin_checkpoint(&mut self) -> Result<(), NativeSessionError> {
        self.engine
            .begin_checkpoint(&mut self.consensus, &mut self.seeds)
    }
    /// The seeded checkpoint this replica is waiting to install, if any.
    pub fn pending_seed(&self) -> Option<&PendingSeed> {
        self.engine.pending_seed()
    }
    /// The content objects a retained delivery is waiting for, if any.
    pub fn pending_custody(&self) -> Option<&PendingCustody> {
        self.engine.pending_custody()
    }
    /// Take one chunk of a pending seed from a peer: verified against its
    /// hash and sealed locally; the retained delivery installs the checkpoint
    /// at the next poll once every chunk is local.
    pub fn install_seed_chunk(
        &mut self,
        hash: ContentHash,
        bytes: &[u8],
    ) -> Result<(), NativeSessionError> {
        self.seeds
            .install_as(hash, bytes)
            .map_err(|error| NativeSessionError::Native(NativeEvidenceError::from(error).into()))
    }
    /// A read-only view of this replica's seeds, for serving peers.
    pub fn seed_reader(&self) -> SeedReader {
        self.seeds.reader()
    }
}
