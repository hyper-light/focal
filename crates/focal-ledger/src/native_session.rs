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
use focal_evidence::{ContentReader, ContentStore, NativeEvidenceError, NativeSchemaVerifier};
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
            Self::Capacity | Self::Memory(_) | Self::CustodyPending => Retryable,
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
    engine: NativeEngine<S>,
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
        let engine = NativeEngine::new(ledger, range, profile, limits, parent, reader, schemas)?;
        let mut session = Self {
            consensus,
            store,
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
        self.engine.begin_checkpoint(&mut self.consensus)
    }
}
