//! Native-only durable genesis composition. This type is deliberately separate
//! from the live V1 Session; legacy migration and service selection are absent.
//! Followers own a passive Core. Admission credits are reconstructed once at a
//! current-term read barrier, never by replaying participant commands.
use focal_consensus::{ConsensusError, DurableNode, Message, NodeConfig, NodeEvents, NodeStatus, StateRole};
use focal_core::{Core, native::{NativeCandidate, NativeContentProfile, NativeContext, NativeError,
    NativeInput, NativeInvocation, NativeOutcome, NativeOwner, NativeOwnerError, NativeStaging,
    NativeState, record_codec::{self as record, recovery}}};
use focal_evidence::{ContentStore, NativeSchemaVerifier};
use focal_memory::{Allocation, BudgetKind, BudgetLane, MemoryBudget, MemoryError, RangeId};
use focal_model::{ContentHash, LedgerId, SessionSeq};
use std::{collections::VecDeque, path::Path};

#[path = "native_session_apply.rs"]
mod apply;
#[path = "native_session_checkpoint.rs"]
mod checkpoint;
#[cfg(test)]
#[path = "native_session_tests.rs"]
mod tests;

/// Host-supplied bounds, independent of participant-authored requirements.
#[derive(Debug, Clone, Copy)]
pub struct NativeSessionLimits {
    pub recovery: recovery::Limits,
    pub encoding: record::EncodingLimits,
    pub inspection: record::InspectionLimits,
    pub checkpoint: crate::native_checkpoint::Limits,
    pub memory_bytes: usize,
    pub completion_reserve_bytes: usize,
    pub content_domain: focal_model::ContentDomainId,
}

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
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NativeSubmission {
    Committed(NativeOutcome),
    Pending { candidate: NativeCandidate, outcome: NativeOutcome },
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NativeCommit {
    pub raft_index: u64,
    pub raft_term: u64,
    pub record_hash: ContentHash,
    pub outcome: NativeOutcome,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NativeReadBoundary {
    pub raft_index: u64,
    pub native_sequence: SessionSeq,
}

/// Consensus buffers retain their original permit. Native summaries are copied
/// scalars with their own exact output allocation; neither exposes pending rows.
pub struct NativeSessionEvents {
    pub consensus: NodeEvents,
    pub committed: Vec<NativeCommit>,
    pub read_boundaries: Vec<NativeReadBoundary>,
    _allocation: Allocation,
}

struct Pending {
    candidate: NativeCandidate,
    outcome: NativeOutcome,
    hash: Option<ContentHash>,
    submitted: bool,
}
enum Domain {
    Passive(Core<NativeState>),
    Active(NativeOwner),
}

/// One native domain authority for one physical consensus group. This is not a
/// V1 decoder transition and does not provide placement or managed cursor APIs.
pub struct NativeSession<S: NativeSchemaVerifier> {
    delivery: Option<apply::Delivery>,
    pending: VecDeque<Pending>,
    domain: Option<Domain>,
    consensus: DurableNode,
    store: ContentStore,
    schemas: S,
    budget: MemoryBudget,
    ledger: LedgerId,
    profile: NativeContentProfile,
    range: RangeId,
    recording_range: Option<RangeId>,
    recording_term: u64,
    limits: NativeSessionLimits,
    applied_raft: u64,
    configuration_index: u64,
    ready_term: Option<u64>,
    readiness_requested: Option<u64>,
    observed_term: u64,
    observed_leader: bool,
    reconstruction_needed: bool,
    failed: bool,
    _pending_allocation: Allocation,
}

const DECODER: &[u8] = b"focal-native-session/genesis1;ancillary=native-only1;mutation=FCMUTATE2;checkpoint=FCNROOTS2;session=FCNSESS1;recorded-graph-capture;no-v1-import";
fn decoder() -> [u8; 32] { *blake3::hash(DECODER).as_bytes() }
fn add(a: usize, b: usize) -> Result<usize, NativeSessionError> {
    a.checked_add(b).ok_or(NativeSessionError::Capacity)
}
fn array<T>(count: usize) -> Result<usize, NativeSessionError> {
    let bytes = count.checked_mul(size_of::<T>()).ok_or(NativeSessionError::Capacity)?;
    add(bytes, if count == 0 { 0 } else { 4 * size_of::<usize>() })
}
fn reserved<T>(count: usize) -> Result<Vec<T>, NativeSessionError> {
    let mut values = Vec::new();
    values.try_reserve_exact(count).map_err(|_| NativeSessionError::Capacity)?;
    if values.capacity() > count { return Err(NativeSessionError::Capacity); }
    Ok(values)
}

impl<S: NativeSchemaVerifier> NativeSession<S> {
    pub fn decoder_hash() -> ContentHash { ContentHash(decoder()) }

    #[allow(clippy::too_many_arguments)] // Explicit trusted physical identity and bounded custody.
    pub fn open(
        path: impl AsRef<Path>, config: NodeConfig, ledger: LedgerId, range: RangeId,
        profile: NativeContentProfile, limits: NativeSessionLimits, parent: &MemoryBudget,
        store: ContentStore, schemas: S,
    ) -> Result<Self, NativeSessionError> {
        Self::from_node(DurableNode::open_in(config, path, parent)?, ledger, range,
            profile, limits, parent, store, schemas)
    }

    #[allow(clippy::too_many_arguments)] // No hidden default or inferred recovery provenance.
    pub fn from_node(
        mut consensus: DurableNode, ledger: LedgerId, range: RangeId,
        profile: NativeContentProfile, limits: NativeSessionLimits, parent: &MemoryBudget,
        store: ContentStore, schemas: S,
    ) -> Result<Self, NativeSessionError> {
        if !consensus.is_budgeted_within(parent) || range.0 == 0 || limits.content_domain.is_zero()
            || limits.recovery.native.pending == 0 {
            return Err(NativeSessionError::Capacity);
        }
        let is_new = consensus.required_decoder().is_none();
        if !is_new && consensus.required_decoder() != Some(decoder()) {
            return Err(NativeSessionError::Legacy);
        }
        consensus.confirm_decoder(decoder())?;
        if is_new {
            let initial = consensus.drain()?;
            if initial.applied_index != 0 || !initial.committed.is_empty()
                || !initial.membership.is_empty() || initial.snapshot.is_some()
                || !initial.messages.is_empty() {
                return Err(NativeSessionError::Legacy);
            }
            consensus.begin_decoder_floor(decoder())?;
            consensus.finish_decoder_floor()?;
        }
        let budget = parent.child(limits.memory_bytes, limits.completion_reserve_bytes)?;
        let count = limits.recovery.native.pending;
        let permit = budget.reserve(BudgetKind::Pending, BudgetLane::Ordinary, array::<Pending>(count)?)?;
        let mut pending = VecDeque::new();
        pending.try_reserve_exact(count).map_err(|_| NativeSessionError::Capacity)?;
        if pending.capacity() > count { return Err(NativeSessionError::Capacity); }
        let core = match profile {
            NativeContentProfile::ProjectionOnly => Core::new_native(ledger, range, limits.recovery.native, budget.clone())?,
            NativeContentProfile::AuthoredV1 => Core::new_native_authored(ledger, range, limits.recovery.native, budget.clone())?,
        };
        let mut session = Self {
            delivery: None, pending, domain: Some(Domain::Passive(core)), consensus, store, schemas,
            budget, ledger, profile, range, recording_range: None, recording_term: 0, limits,
            applied_raft: 0, configuration_index: 0, ready_term: None,
            readiness_requested: None, observed_term: 0, observed_leader: false,
            reconstruction_needed: true, failed: false, _pending_allocation: permit.commit(),
        };
        let _ = session.poll()?;
        Ok(session)
    }

    fn check(&self) -> Result<(), NativeSessionError> {
        if self.failed || self.domain.is_none() { Err(NativeSessionError::Failed) } else { Ok(()) }
    }
    pub fn status(&self) -> NodeStatus { self.consensus.status() }
    pub fn is_authoritative(&self) -> bool {
        let status = self.status();
        !self.failed && self.delivery.is_none() && status.role == StateRole::Leader && self.ready_term == Some(status.term)
            && matches!(self.domain, Some(Domain::Active(_)))
    }
    pub fn committed_core(&self) -> Result<&Core<NativeState>, NativeSessionError> {
        self.check()?;
        match self.domain.as_ref() {
            Some(Domain::Passive(core)) => Ok(core),
            Some(Domain::Active(owner)) => Ok(owner.committed_core()),
            None => Err(NativeSessionError::Failed),
        }
    }
    pub fn sequence(&self) -> Result<SessionSeq, NativeSessionError> { Ok(self.committed_core()?.native_sequence()) }
    pub fn applied_raft(&self) -> u64 { self.applied_raft }
    pub fn pending_count(&self) -> usize { self.pending.len() }
    pub fn outcome(&self, request: impl Into<NativeInvocation>) -> Result<Option<NativeOutcome>, NativeSessionError> {
        Ok(self.committed_core()?.native_outcome(request))
    }
    pub fn read_at_least(&self, boundary: NativeReadBoundary) -> Result<&Core<NativeState>, NativeSessionError> {
        let core = self.committed_core()?;
        if self.applied_raft < boundary.raft_index || core.native_sequence() < boundary.native_sequence {
            return Err(NativeSessionError::NotReady { leader: self.status().leader_id });
        }
        Ok(core)
    }
    pub fn campaign(&mut self) -> Result<(), NativeSessionError> { self.check()?; self.consensus.campaign()?; Ok(()) }
    pub fn tick(&mut self) -> Result<(), NativeSessionError> { self.check()?; self.consensus.tick()?; Ok(()) }
    pub fn step_authenticated(&mut self, peer: u64, bytes: &[u8]) -> Result<(), NativeSessionError> {
        self.check()?; self.consensus.step_authenticated(peer, bytes)?; Ok(())
    }
    /// Trusted in-process transport; network callers use step_authenticated.
    pub fn step(&mut self, message: Message) -> Result<(), NativeSessionError> {
        self.check()?; self.consensus.step(message)?; Ok(())
    }
    pub fn persistence_pending(&self) -> bool { self.consensus.persistence_pending() }

    pub fn propose(&mut self, context: NativeContext, input: NativeInput) -> Result<NativeSubmission, NativeSessionError> {
        self.require_authority()?;
        self.flush_proposals()?;
        let Some(Domain::Active(owner)) = self.domain.as_mut() else { return Err(NativeSessionError::Failed); };
        let staged = owner.prepare_with_custody(context, input, &mut self.store,
            self.limits.content_domain, &self.schemas)?;
        self.submit_staged(staged)
    }
    fn require_authority(&self) -> Result<(), NativeSessionError> {
        self.check()?;
        if !self.is_authoritative() { return Err(NativeSessionError::NotReady { leader: self.status().leader_id }); }
        Ok(())
    }
    fn submit_staged(&mut self, staged: NativeStaging) -> Result<NativeSubmission, NativeSessionError> {
        let (candidate, outcome) = match staged {
            NativeStaging::Existing { outcome, candidate: None } => return Ok(NativeSubmission::Committed(outcome)),
            NativeStaging::Existing { outcome, candidate: Some(candidate) } => (candidate, outcome),
            NativeStaging::Prepared { candidate, outcome } => {
                if self.pending.len() >= self.limits.recovery.native.pending || self.pending.len() == self.pending.capacity() {
                    self.failed = true;
                    return Err(NativeSessionError::Capacity);
                }
                self.pending.push_back(Pending { candidate, outcome, hash: None, submitted: false });
                (candidate, outcome)
            }
        };
        self.flush_proposals()?;
        Ok(NativeSubmission::Pending { candidate, outcome })
    }
    fn flush_proposals(&mut self) -> Result<(), NativeSessionError> {
        let Some(Domain::Active(owner)) = self.domain.as_mut() else { return Err(NativeSessionError::Failed); };
        for pending in &mut self.pending {
            if pending.submitted { continue; }
            let encoded = owner.encode_candidate(pending.candidate, self.limits.encoding)?;
            pending.hash = Some(encoded.hash());
            self.consensus.propose_borrowed_in(encoded.bytes(), BudgetLane::Completion)?;
            pending.submitted = true;
        }
        Ok(())
    }
}
