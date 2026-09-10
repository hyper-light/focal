//! Range movement through the replica host (25 §6): the operator's move and
//! the controller's steps cross the owner queue like every other trusted
//! control call; a replica states its own facts (readiness, seal, progress)
//! from its committed rows, and the operator's view names every member.
use super::*;
use focal_core::native::retirement::{RetirementCandidates, RetirementCursor};
use focal_core::native::{ContentRootsPage, NativeRowCursor, RetiredClaim};
use focal_ledger::LedgerRangeVerifier;
use focal_memory::RangeId;
use focal_model::{ClaimId, RaftTerm, RouteEpoch};
use focal_ranges::{
    DestinationReady, Holder, KeySpan, Placement, RangeDescriptor, RangeIntent, RangeOperation,
    RangeProgress, ReplicaId, SourceSealProof, StorageKey, TransferId,
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

/// One member of the committed movement map, for the operator and the
/// controller. Affinity bounds are the storage key's first component.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RangeMemberView {
    pub id: RangeId,
    pub generation: u64,
    pub start: Option<[u8; 16]>,
    pub end: Option<[u8; 16]>,
    /// The holding replica's node, `None` when the voters hold it.
    pub holder: Option<ReplicaId>,
    pub readers: Vec<ReplicaId>,
    /// Rows the member holds on this replica.
    pub entries: usize,
    /// The member's rows digest at the committed prefix, when asked for.
    pub digest: Option<ContentHash>,
}
/// The transfer in progress.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RangePendingView {
    pub operation: TransferId,
    pub old_epoch: RouteEpoch,
    pub seed: SessionSeq,
    pub sources: Vec<RangeId>,
    pub replacements: Vec<RangeMemberView>,
    pub snapshots: Vec<RangeId>,
    pub barrier: Option<SessionSeq>,
    pub seals: Vec<RangeId>,
    pub ready: Vec<RangeId>,
}
/// A retired map awaiting cleanup.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RangeHistoryView {
    pub operation: TransferId,
    pub epoch: RouteEpoch,
    pub proofs: ContentHash,
}
/// The committed movement state of one hosted session.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RangeView {
    pub epoch: RouteEpoch,
    pub ordinal: u64,
    pub prefix: SessionSeq,
    pub genesis: ContentHash,
    pub in_flight: bool,
    pub refusals: u64,
    pub members: Vec<RangeMemberView>,
    pub pending: Option<RangePendingView>,
    pub history: Vec<RangeHistoryView>,
}
/// One fact a replica states about a member of a transfer.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RangeFactRequest {
    Ready {
        operation: TransferId,
        range: RangeId,
    },
    Seal {
        operation: TransferId,
        range: RangeId,
    },
    Progress {
        range: RangeId,
    },
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum RangeFact {
    Ready(DestinationReady),
    Seal(SourceSealProof),
    Progress(RangeProgress),
}
/// The body of `Operation::RangeControl`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct RangeControlRequest {
    pub schema: u16,
    pub fact: RangeFactRequest,
}
pub const RANGE_CONTROL_SCHEMA: u16 = 1;
/// The reply carried in `Response::Control` for `RangeControl`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum RangeControlReply {
    Fact(Box<RangeFact>),
    Refused(focal_control::ControlFailure),
}

pub(super) enum RangeCall {
    View {
        digests: bool,
        reply: oneshot::Sender<Result<RangeView, LedgerError>>,
    },
    Propose {
        operation: Box<RangeOperation>,
        reply: oneshot::Sender<Result<(), LedgerError>>,
    },
    Move {
        member: RangeId,
        replica: ReplicaId,
        reply: oneshot::Sender<Result<TransferId, LedgerError>>,
    },
    Fact {
        request: RangeFactRequest,
        reply: oneshot::Sender<Result<RangeFact, LedgerError>>,
    },
    Activate {
        unchanged: Vec<RangeProgress>,
        reply: oneshot::Sender<Result<(), LedgerError>>,
    },
    Layout {
        operation: focal_ledger::LayoutOperation,
        reply: oneshot::Sender<Result<(), LedgerError>>,
    },
    SplitPoint {
        member: RangeId,
        reply: oneshot::Sender<Result<Option<[u8; 16]>, LedgerError>>,
    },
    /// Terminal, released claims from the status index (26 §4).
    Candidates {
        cursor: Option<RetirementCursor>,
        max_visits: usize,
        reply: oneshot::Sender<Result<RetirementCandidates, LedgerError>>,
    },
    /// One family's bundle, when it may leave now: eligible, past the
    /// retention floor, and settled at least `min_age_ms` ago.
    Archive {
        root: ClaimId,
        min_age_ms: u64,
        reply: oneshot::Sender<Result<Option<ArchivedFamily>, LedgerError>>,
    },
    /// Propose one family's retirement (26 §4).
    Retire {
        root: ClaimId,
        bundle: ContentHash,
        bytes: u64,
        through: SessionSeq,
        reply: oneshot::Sender<Result<(), LedgerError>>,
    },
    /// The continuation of a retired claim.
    Retired {
        claim: ClaimId,
        reply: oneshot::Sender<Result<Option<RetiredClaim>, LedgerError>>,
    },
    /// One page of the content roots the committed rows name (26 §5).
    ContentRoots {
        cursor: Option<NativeRowCursor>,
        max_visits: usize,
        reply: oneshot::Sender<Result<ContentRootsPage, LedgerError>>,
    },
    /// One bounded sweep of this replica's seed store (26 §5).
    CollectSeeds {
        grace_ms: u64,
        now_ms: u64,
        max_items: usize,
        reply: oneshot::Sender<Result<focal_evidence::SeedReport, LedgerError>>,
    },
}
/// One family's archive bundle as the committed core wrote it (26 §4):
/// what the archive agent seals as content before it proposes the
/// retirement record. The bytes stay charged to the replica's budget
/// until the agent is done with them.
pub struct ArchivedFamily {
    pub root: ClaimId,
    pub members: Vec<ClaimId>,
    /// The prefix the bundle claims.
    pub through: SessionSeq,
    /// The frame's own digest, verified again wherever the bundle is read.
    pub digest: ContentHash,
    pub rows: usize,
    pub bundle: Vec<u8>,
    pub(crate) _allocation: Allocation,
}

fn affinity_of(key: Option<StorageKey>) -> Option<[u8; 16]> {
    key.map(|key| key.affinity)
}
fn member_view(
    range: &RangeDescriptor,
    entries: usize,
    digest: Option<ContentHash>,
) -> RangeMemberView {
    RangeMemberView {
        id: range.id,
        generation: range.generation,
        start: affinity_of(range.span.start),
        end: affinity_of(range.span.end),
        holder: range.meta.owner.replica(),
        readers: range.meta.readers.iter().copied().collect(),
        entries,
        digest,
    }
}
/// The identity of a moved member: derived from the source, the destination
/// and the epoch, so a repeated request names the same transfer.
fn derive(
    domain: &str,
    ledger: LedgerId,
    member: RangeId,
    replica: ReplicaId,
    epoch: RouteEpoch,
) -> [u8; 16] {
    let mut hasher = blake3::Hasher::new_derive_key(domain);
    hasher.update(&ledger.tenant.0);
    hasher.update(&ledger.session.0);
    hasher.update(&member.0.to_le_bytes());
    hasher.update(&replica.node.to_le_bytes());
    hasher.update(&replica.generation.to_le_bytes());
    hasher.update(&epoch.0.to_le_bytes());
    let mut id = [0u8; 16];
    for (target, source) in id.iter_mut().zip(hasher.finalize().as_bytes()) {
        *target = *source;
    }
    id
}

impl ReplicaHost {
    async fn range_call<T>(
        &self,
        make: impl FnOnce(oneshot::Sender<Result<T, LedgerError>>) -> RangeCall,
    ) -> Result<T, LedgerError> {
        let charge = self
            .budget
            .reserve(BudgetKind::Control, BudgetLane::Completion, 256 * 1024)?
            .commit();
        let (send, receive) = oneshot::channel();
        self.sender
            .try_send(Work::Range(Box::new(make(send)), charge))
            .map_err(|error| match error {
                HostQueueError::Full => LedgerError::Capacity,
                HostQueueError::Disconnected => LedgerError::Failed,
            })?;
        receive.await.map_err(|_| LedgerError::OutcomeUnknown)?
    }
    /// The committed movement map and transfer state, with each member's
    /// rows digest when `digests` is asked for (a pass over the rows).
    pub async fn range_view(&self, digests: bool) -> Result<RangeView, LedgerError> {
        self.range_call(|reply| RangeCall::View { digests, reply })
            .await
    }
    /// Propose one movement step as a session decision (25 §6).
    pub async fn propose_range(&self, operation: RangeOperation) -> Result<(), LedgerError> {
        self.range_call(|reply| RangeCall::Propose {
            operation: Box::new(operation),
            reply,
        })
        .await
    }
    /// Begin moving `member` to `replica`: the intent is built from the
    /// committed map (the member's exact span and generation) and proposed;
    /// the transfer identity is derived so a repeat names the same one.
    pub async fn move_range(
        &self,
        member: RangeId,
        replica: ReplicaId,
    ) -> Result<TransferId, LedgerError> {
        self.range_call(|reply| RangeCall::Move {
            member,
            replica,
            reply,
        })
        .await
    }
    /// Propose the activation of the pending transfer over the proofs the
    /// committed state holds and the attested progress of the members that
    /// stay (25 §6).
    pub async fn activate_range(&self, unchanged: Vec<RangeProgress>) -> Result<(), LedgerError> {
        self.range_call(|reply| RangeCall::Activate { unchanged, reply })
            .await
    }
    /// Propose one layout change (a split at an affinity, or a merge) as a
    /// session decision (25 §4).
    pub async fn propose_layout(
        &self,
        operation: focal_ledger::LayoutOperation,
    ) -> Result<(), LedgerError> {
        self.range_call(|reply| RangeCall::Layout { operation, reply })
            .await
    }
    /// An affinity dividing `member` near its middle, if its rows allow one
    /// (25 §8).
    pub async fn split_point(&self, member: RangeId) -> Result<Option<[u8; 16]>, LedgerError> {
        self.range_call(|reply| RangeCall::SplitPoint { member, reply })
            .await
    }
    /// One fact this replica states about a member from its committed rows.
    pub async fn range_fact(&self, request: RangeFactRequest) -> Result<RangeFact, LedgerError> {
        self.range_call(|reply| RangeCall::Fact { request, reply })
            .await
    }
    /// Terminal, released claims from the status index, `max_visits` index
    /// rows from `cursor` on (26 §4).
    pub async fn retirement_candidates(
        &self,
        cursor: Option<RetirementCursor>,
        max_visits: usize,
    ) -> Result<RetirementCandidates, LedgerError> {
        self.range_call(|reply| RangeCall::Candidates {
            cursor,
            max_visits,
            reply,
        })
        .await
    }
    /// The bundle of `root`'s family when this replica is the authority,
    /// the family is eligible, the retention floor has passed its last
    /// event and that event settled at least `min_age_ms` of logical time
    /// ago; `None` otherwise.
    pub async fn archive_family(
        &self,
        root: ClaimId,
        min_age_ms: u64,
    ) -> Result<Option<ArchivedFamily>, LedgerError> {
        self.range_call(|reply| RangeCall::Archive {
            root,
            min_age_ms,
            reply,
        })
        .await
    }
    /// Propose one family's retirement as a session decision (26 §4).
    pub async fn propose_retirement(
        &self,
        root: ClaimId,
        bundle: ContentHash,
        bytes: u64,
        through: SessionSeq,
    ) -> Result<(), LedgerError> {
        self.range_call(|reply| RangeCall::Retire {
            root,
            bundle,
            bytes,
            through,
            reply,
        })
        .await
    }
    /// The continuation of a retired claim, if the claim retired.
    pub async fn retired(&self, claim: ClaimId) -> Result<Option<RetiredClaim>, LedgerError> {
        self.range_call(|reply| RangeCall::Retired { claim, reply })
            .await
    }
    /// One page of the content roots the committed rows name (26 §5).
    pub async fn content_roots(
        &self,
        cursor: Option<NativeRowCursor>,
        max_visits: usize,
    ) -> Result<ContentRootsPage, LedgerError> {
        self.range_call(|reply| RangeCall::ContentRoots {
            cursor,
            max_visits,
            reply,
        })
        .await
    }
    /// One bounded sweep of this replica's seed store (26 §5).
    pub async fn collect_seeds(
        &self,
        grace_ms: u64,
        now_ms: u64,
        max_items: usize,
    ) -> Result<focal_evidence::SeedReport, LedgerError> {
        self.range_call(|reply| RangeCall::CollectSeeds {
            grace_ms,
            now_ms,
            max_items,
            reply,
        })
        .await
    }
}

impl Owner {
    pub(super) fn accept_range(&mut self, call: RangeCall, charge: Allocation) {
        match call {
            RangeCall::View { digests, reply } => {
                let result = self.range_view(digests);
                drop(charge);
                let _ = reply.send(result);
            }
            RangeCall::Propose { operation, reply } => {
                let result = self.session.native_propose_range(*operation);
                drop(charge);
                let _ = reply.send(result);
            }
            RangeCall::Move {
                member,
                replica,
                reply,
            } => {
                let result = self.move_range(member, replica);
                drop(charge);
                let _ = reply.send(result);
            }
            RangeCall::Fact { request, reply } => {
                let result = self.range_fact(request);
                drop(charge);
                let _ = reply.send(result);
            }
            RangeCall::Activate { unchanged, reply } => {
                let result = self
                    .session
                    .native_range_activation_operation(unchanged)
                    .and_then(|operation| self.session.native_propose_range(operation));
                drop(charge);
                let _ = reply.send(result);
            }
            RangeCall::Layout { operation, reply } => {
                let result = self.session.native_propose_layout(operation);
                drop(charge);
                let _ = reply.send(result);
            }
            RangeCall::SplitPoint { member, reply } => {
                let result = self.session.native_range_map().and_then(|map| {
                    let index = map
                        .ranges()
                        .iter()
                        .position(|range| range.id == member)
                        .ok_or(LedgerError::PlacementConflict)?;
                    self.session.native_member_split_point(index)
                });
                drop(charge);
                let _ = reply.send(result);
            }
            RangeCall::Candidates {
                cursor,
                max_visits,
                reply,
            } => {
                let result = self.session.native_core().and_then(|core| {
                    core.retirement_candidates(cursor, max_visits)
                        .map_err(|error| LedgerError::Native(error.into()))
                });
                drop(charge);
                let _ = reply.send(result);
            }
            RangeCall::Archive {
                root,
                min_age_ms,
                reply,
            } => {
                let result = self.archive_family(root, min_age_ms);
                drop(charge);
                let _ = reply.send(result);
            }
            RangeCall::Retire {
                root,
                bundle,
                bytes,
                through,
                reply,
            } => {
                let result = self
                    .session
                    .native_propose_retirement(root, bundle, bytes, through);
                drop(charge);
                let _ = reply.send(result);
            }
            RangeCall::Retired { claim, reply } => {
                let result = self
                    .session
                    .native_core()
                    .map(|core| core.native_retired(claim).copied());
                drop(charge);
                let _ = reply.send(result);
            }
            RangeCall::ContentRoots {
                cursor,
                max_visits,
                reply,
            } => {
                let result = self.session.native_content_roots(cursor, max_visits);
                drop(charge);
                let _ = reply.send(result);
            }
            RangeCall::CollectSeeds {
                grace_ms,
                now_ms,
                max_items,
                reply,
            } => {
                let result = self
                    .session
                    .native_collect_seeds(grace_ms, now_ms, max_items);
                drop(charge);
                let _ = reply.send(result);
            }
        }
    }
    /// One family's bundle from the committed core (26 §4): only on the
    /// authority, only when the family is eligible, every registered
    /// consumer has read past its last event, and that event settled at
    /// least `min_age_ms` of the node's logical time ago (a finished claim
    /// stays readable in the core for the grace the operator sets); the
    /// bytes are charged here.
    fn archive_family(
        &self,
        root: ClaimId,
        min_age_ms: u64,
    ) -> Result<Option<ArchivedFamily>, LedgerError> {
        if !self.session.native_authoritative() {
            return Ok(None);
        }
        let now = crate::native_ingress::logical_time(&self.session)
            .map_err(|_| LedgerError::NotReady { leader: 0 })?;
        let report = self.session.native_retention()?;
        let limits = self.session.native_encoding_limits()?;
        let core = self.session.native_core()?;
        let Ok(family) = core.retirement_family(root) else {
            return Ok(None);
        };
        if !report.allows_family(family.through) {
            return Ok(None);
        }
        // The family's last event was published by the outcome at its
        // sequence; the outcome carries the logical time it settled at.
        let settled = core
            .native_event(family.through, 0)
            .and_then(|event| core.native_outcome(event.invocation))
            .map(|outcome| outcome.logical_time)
            .ok_or(LedgerError::Corrupt)?;
        if now.saturating_sub(settled) < min_age_ms {
            return Ok(None);
        }
        let through = core.native_sequence();
        let quote = core
            .archive_family_quote(&family, through, limits)
            .map_err(|error| LedgerError::Native(error.into()))?;
        let allocation = self
            .budget
            .reserve(BudgetKind::Payload, BudgetLane::Ordinary, quote.bytes)?
            .commit();
        let mut bundle = Vec::new();
        bundle
            .try_reserve_exact(quote.bytes)
            .map_err(|_| LedgerError::Capacity)?;
        bundle.resize(quote.bytes, 0);
        let digest = core
            .archive_family_into(&family, through, &mut bundle, quote.visits)
            .map_err(|error| LedgerError::Native(error.into()))?;
        let rows = family.rows();
        Ok(Some(ArchivedFamily {
            root,
            members: family.members,
            through,
            digest,
            rows,
            bundle,
            _allocation: allocation,
        }))
    }
    fn range_view(&self, digests: bool) -> Result<RangeView, LedgerError> {
        let map = self.session.native_range_map()?;
        let checkpoint = self.session.native_movement_checkpoint()?;
        let mut members = Vec::new();
        members
            .try_reserve_exact(map.ranges().len())
            .map_err(|_| LedgerError::Capacity)?;
        for (index, range) in map.ranges().iter().enumerate() {
            let digest = if digests {
                Some(self.session.native_member_digest(index)?)
            } else {
                None
            };
            let entries = self
                .session
                .native_member_stats(index)?
                .map_or(0, |stats| stats.entries);
            members.push(member_view(range, entries, digest));
        }
        let pending = checkpoint.pending.as_ref().map(|pending| RangePendingView {
            operation: pending.intent.operation,
            old_epoch: pending.intent.old_epoch,
            seed: pending.intent.seed,
            sources: pending.intent.sources.iter().copied().collect(),
            replacements: pending
                .intent
                .replacements
                .iter()
                .map(|range| member_view(range, 0, None))
                .collect(),
            snapshots: pending.snapshots.keys().copied().collect(),
            barrier: pending.barrier.as_ref().map(|proof| proof.sequence),
            seals: pending.source_seals.keys().copied().collect(),
            ready: pending.ready.keys().copied().collect(),
        });
        let mut history = Vec::new();
        history
            .try_reserve_exact(checkpoint.history.len())
            .map_err(|_| LedgerError::Capacity)?;
        for old in &checkpoint.history {
            history.push(RangeHistoryView {
                operation: old.activation.intent.operation,
                epoch: old.map.epoch(),
                proofs: old
                    .activation
                    .proof_digest()
                    .map_err(|_| LedgerError::Corrupt)?,
            });
        }
        Ok(RangeView {
            epoch: map.epoch(),
            ordinal: checkpoint.control_ordinal,
            prefix: self.session.native_sequence()?,
            genesis: self.session.native_range_verifier()?.genesis(),
            in_flight: self.session.native_movement_in_flight(),
            refusals: self.session.native_movement_refusals(),
            members,
            pending,
            history,
        })
    }
    fn move_range(
        &mut self,
        member: RangeId,
        replica: ReplicaId,
    ) -> Result<TransferId, LedgerError> {
        let map = self.session.native_range_map()?;
        let ledger = self.session.ledger();
        let source = map
            .get(member)
            .ok_or(LedgerError::PlacementConflict)?
            .clone();
        if source.meta.owner == Holder::Replica(replica) {
            return Err(LedgerError::PlacementConflict);
        }
        let epoch = map.epoch();
        let operation = TransferId(derive(
            "focal.range.move.operation.v1",
            ledger,
            member,
            replica,
            epoch,
        ));
        let id = RangeId(u128::from_le_bytes(derive(
            "focal.range.move.member.v1",
            ledger,
            member,
            replica,
            epoch,
        )));
        if id.0 == 0 || map.get(id).is_some() {
            return Err(LedgerError::PlacementConflict);
        }
        let intent = RangeIntent {
            ledger,
            operation,
            old_epoch: epoch,
            sources: BTreeSet::from([member]),
            replacements: vec![RangeDescriptor {
                id,
                generation: 1,
                span: KeySpan {
                    start: source.span.start,
                    end: source.span.end,
                },
                meta: Placement::replica(replica),
            }],
            seed: self.session.native_sequence()?,
        };
        // A pending transfer of the same intent is the same request.
        if let Some(pending) = self.session.native_movement_pending()? {
            return if pending.intent.operation == operation {
                Ok(operation)
            } else {
                Err(LedgerError::PlacementConflict)
            };
        }
        self.session
            .native_propose_range(RangeOperation::Begin(intent))?;
        Ok(operation)
    }
    /// The replica identity this host states its facts under.
    fn replica_id(&self) -> ReplicaId {
        ReplicaId {
            node: self.session.status().node_id,
            generation: self.config.node_generation,
        }
    }
    fn range_fact(&self, request: RangeFactRequest) -> Result<RangeFact, LedgerError> {
        let map = self.session.native_range_map()?;
        let ledger = self.session.ledger();
        let me = self.replica_id();
        let through = self.session.native_sequence()?;
        let digest_of_span = |start: Option<StorageKey>| -> Result<ContentHash, LedgerError> {
            let affinity = start.map_or([0; 16], |key| key.affinity);
            let (index, _) = self.session.native_member_at(affinity)?;
            self.session.native_member_digest(index)
        };
        match request {
            RangeFactRequest::Ready { operation, range } => {
                let pending = self
                    .session
                    .native_movement_pending()?
                    .ok_or(LedgerError::PlacementConflict)?;
                if pending.intent.operation != operation {
                    return Err(LedgerError::PlacementConflict);
                }
                let target = pending
                    .intent
                    .replacements
                    .iter()
                    .find(|item| item.id == range)
                    .ok_or(LedgerError::PlacementConflict)?;
                if target.meta.owner != Holder::Replica(me) {
                    return Err(LedgerError::PlacementConflict);
                }
                let snapshot =
                    pending
                        .snapshots
                        .get(&range)
                        .copied()
                        .ok_or(LedgerError::NotReady {
                            leader: self.session.status().leader_id,
                        })?;
                let state = digest_of_span(target.span.start)?;
                Ok(RangeFact::Ready(DestinationReady {
                    ledger,
                    operation,
                    new_epoch: RouteEpoch(map.epoch().0.saturating_add(1)),
                    range,
                    range_generation: target.generation,
                    replica: me,
                    seed: pending.intent.seed,
                    through,
                    snapshot,
                    state,
                    checkpoint: state,
                    attestation: ContentHash([0; 32]),
                }))
            }
            RangeFactRequest::Seal { operation, range } => {
                let pending = self
                    .session
                    .native_movement_pending()?
                    .ok_or(LedgerError::PlacementConflict)?;
                if pending.intent.operation != operation {
                    return Err(LedgerError::PlacementConflict);
                }
                let cut = pending.barrier.as_ref().map(|proof| proof.sequence).ok_or(
                    LedgerError::NotReady {
                        leader: self.session.status().leader_id,
                    },
                )?;
                let source = map.get(range).ok_or(LedgerError::PlacementConflict)?;
                if source.meta.owner != Holder::Replica(me) {
                    return Err(LedgerError::PlacementConflict);
                }
                let checkpoint = digest_of_span(source.span.start)?;
                Ok(RangeFact::Seal(SourceSealProof {
                    ledger,
                    operation,
                    old_epoch: map.epoch(),
                    range,
                    range_generation: source.generation,
                    replica: me,
                    cut,
                    checkpoint,
                    attestation: ContentHash([0; 32]),
                }))
            }
            RangeFactRequest::Progress { range } => {
                let member = map.get(range).ok_or(LedgerError::PlacementConflict)?;
                if member.meta.owner != Holder::Replica(me) {
                    return Err(LedgerError::PlacementConflict);
                }
                let root = digest_of_span(member.span.start)?;
                Ok(RangeFact::Progress(RangeProgress {
                    ledger,
                    epoch: map.epoch(),
                    range,
                    range_generation: member.generation,
                    replica: me,
                    through,
                    term: RaftTerm(self.session.status().term),
                    root,
                    attestation: ContentHash([0; 32]),
                }))
            }
        }
    }
    /// Whether this replica may serve a listing over the whole group: a
    /// voter always; a learner only when it holds every member.
    pub(super) fn serves_all_members(&self) -> bool {
        let status = self.session.status();
        if status.voters.contains(&status.node_id) {
            return true;
        }
        let me = self.replica_id();
        self.session.native_range_map().is_ok_and(|map| {
            map.ranges()
                .iter()
                .all(|range| range.meta.accepts_reader(me))
        })
    }
    /// Whether this replica may serve a native read for `location`: a voter
    /// holds every member; a learner serves only members it holds (25 §6).
    pub(super) fn serves_member(&self, location: focal_core::native::NativeLocation) -> bool {
        let status = self.session.status();
        if status.voters.contains(&status.node_id) {
            return true;
        }
        let Ok((_, id)) = self.session.native_member_for(location) else {
            return false;
        };
        let me = self.replica_id();
        self.session
            .native_range_map()
            .ok()
            .and_then(|map| map.get(id))
            .is_some_and(|range| range.meta.accepts_reader(me))
    }
}

/// Verify a fact a replica stated against the authority's committed state
/// (25 §6): the identity it claims is the authenticated peer, the member and
/// generation are the transfer's, and its digest equals the authority's own
/// digest of the frozen member. Returns the attested proof to propose.
pub(crate) fn verify_fact(
    verifier: &LedgerRangeVerifier,
    view: &RangeView,
    peer: u64,
    fact: RangeFact,
) -> Result<RangeOperation, LedgerError> {
    let pending = view
        .pending
        .as_ref()
        .ok_or(LedgerError::PlacementConflict)?;
    let barrier = pending.barrier.ok_or(LedgerError::NotReady { leader: 0 })?;
    let digest_of = |start: Option<[u8; 16]>, end: Option<[u8; 16]>| -> Option<ContentHash> {
        view.members
            .iter()
            .find(|member| member.start == start && member.end == end)
            .and_then(|member| member.digest)
    };
    match fact {
        RangeFact::Ready(mut proof) => {
            let target = pending
                .replacements
                .iter()
                .find(|item| item.id == proof.range)
                .ok_or(LedgerError::PlacementConflict)?;
            if proof.replica.node != peer
                || target.holder != Some(proof.replica)
                || proof.operation != pending.operation
                || proof.through < barrier
                || proof.seed != pending.seed
                || Some(proof.state) != digest_of(target.start, target.end)
            {
                return Err(LedgerError::PlacementConflict);
            }
            verifier.attest_ready(&mut proof)?;
            Ok(RangeOperation::Ready(proof))
        }
        RangeFact::Seal(mut proof) => {
            let source = view
                .members
                .iter()
                .find(|member| member.id == proof.range)
                .ok_or(LedgerError::PlacementConflict)?;
            if proof.replica.node != peer
                || source.holder != Some(proof.replica)
                || proof.operation != pending.operation
                || proof.cut != barrier
                || Some(proof.checkpoint) != source.digest
            {
                return Err(LedgerError::PlacementConflict);
            }
            verifier.attest_source_seal(&mut proof)?;
            Ok(RangeOperation::SourceSealed(proof))
        }
        RangeFact::Progress(_) => Err(LedgerError::PlacementConflict),
    }
}
/// Verify a progress fact of an unchanged replica-held member and attest it.
pub(crate) fn verify_progress(
    verifier: &LedgerRangeVerifier,
    view: &RangeView,
    peer: u64,
    mut proof: RangeProgress,
) -> Result<RangeProgress, LedgerError> {
    let member = view
        .members
        .iter()
        .find(|member| member.id == proof.range)
        .ok_or(LedgerError::PlacementConflict)?;
    let barrier = view
        .pending
        .as_ref()
        .and_then(|pending| pending.barrier)
        .ok_or(LedgerError::NotReady { leader: 0 })?;
    if proof.replica.node != peer
        || member.holder != Some(proof.replica)
        || proof.epoch != view.epoch
        || proof.through < barrier
        || Some(proof.root) != member.digest
    {
        return Err(LedgerError::PlacementConflict);
    }
    verifier.attest_progress(&mut proof)?;
    Ok(proof)
}
