//! Bounded evidence custody coordination outside the Raft owner. A content
//! reference is acknowledged only after every selected durable copy confirms it.
use crate::{
    config::Placement,
    content_host::ContentHost,
    custody::{CustodyPolicy, CustodyScope},
    placement::{self, NodeFacts, PlacementPlan},
};
use focal_memory::{Allocation, BudgetKind, BudgetLane, MemoryBudget};
use focal_model::*;
use focal_wire::*;
use futures_util::{FutureExt, StreamExt, stream::FuturesUnordered};
use std::{
    collections::{BTreeMap, BTreeSet},
    future::Future,
    pin::Pin,
};
use tokio::sync::{mpsc, oneshot};

const JOB_BYTES: usize = 8 * 1024 * 1024;

/// The custody an object owes under a placement (doc 04 §7, R8): the copies
/// the placement requires and those with a verified receipt at the current
/// scope. A phase that evaluates the object begins only when the two agree.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CustodyObligation {
    pub scope: CustodyScope,
    pub required: BTreeSet<u64>,
    pub held: BTreeSet<u64>,
}
impl CustodyObligation {
    pub fn satisfied(&self) -> bool {
        self.required.is_subset(&self.held)
    }
    pub fn missing(&self) -> impl Iterator<Item = u64> + '_ {
        self.required.difference(&self.held).copied()
    }
}
fn now_millis() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .ok()
        .and_then(|elapsed| u64::try_from(elapsed.as_millis()).ok())
        .unwrap_or(0)
}
/// One copy's receipt for one object under the scope, taken now.
fn receipt_for(
    scope: CustodyScope,
    node: u64,
    reference: &ContentRef,
) -> focal_evidence::CustodyReceipt {
    focal_evidence::CustodyReceipt::new(
        scope.ledger,
        reference.domain,
        reference.root,
        reference.length,
        node,
        scope.route_epoch,
        scope.policy_revision,
        now_millis(),
    )
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EvidencePlacement {
    scope: CustodyScope,
    voters: BTreeSet<u64>,
    copies: BTreeSet<u64>,
    /// The residency boundary every transfer is checked against (24 §22).
    fence: crate::placement_executor::ResidencyFence,
}
impl EvidencePlacement {
    /// Only a trusted committed placement owner may install this configuration.
    /// The solver independently checks residency and declared failure survival.
    pub fn verified(
        scope: CustodyScope,
        plan: &PlacementPlan,
        nodes: &[NodeFacts],
        placement: &Placement,
    ) -> Result<Self, AccessError> {
        if scope.ledger.tenant.is_zero()
            || scope.ledger.session.is_zero()
            || scope.route_epoch.0 == 0
            || scope.policy_revision == 0
        {
            return Err(AccessError::InvalidRequest);
        }
        placement::verify(plan, nodes, placement).map_err(|_| AccessError::Unavailable)?;
        if plan.voters.len() > 1024 || plan.content_copies.len() > 1024 {
            return Err(AccessError::Capacity);
        }
        let fence = crate::placement_executor::ResidencyFence::new(
            placement
                .residency
                .iter()
                .map(|label| crate::topology::region_id(label))
                .collect(),
            nodes
                .iter()
                .map(|node| (node.id, crate::topology::ids(&node.topology).0))
                .collect(),
        )
        .map_err(|_| AccessError::Capacity)?;
        Ok(Self {
            scope,
            voters: plan.voters.iter().copied().collect(),
            copies: plan.content_copies.iter().copied().collect(),
            fence,
        })
    }
    /// A placement the directory has committed and the agent installs on this
    /// node; the members are the committed voters and content copies.
    pub fn committed(
        scope: CustodyScope,
        voters: BTreeSet<u64>,
        copies: BTreeSet<u64>,
        fence: crate::placement_executor::ResidencyFence,
    ) -> Result<Self, AccessError> {
        if scope.ledger.tenant.is_zero()
            || scope.ledger.session.is_zero()
            || scope.route_epoch.0 == 0
            || scope.policy_revision == 0
            || voters.is_empty()
            || copies.is_empty()
        {
            return Err(AccessError::InvalidRequest);
        }
        if voters.len() > 1024 || copies.len() > 1024 {
            return Err(AccessError::Capacity);
        }
        Ok(Self {
            scope,
            voters,
            copies,
            fence,
        })
    }
    pub fn scope(&self) -> CustodyScope {
        self.scope
    }
    /// Whether a copy may move to `node` under the residency boundary
    /// (24 §22): refused before any byte moves.
    fn admits(&self, node: u64) -> Result<(), AccessError> {
        self.fence
            .check(node)
            .map_err(|_| AccessError::Unauthorized)
    }
    pub fn custody_policy(&self) -> CustodyPolicy {
        CustodyPolicy {
            ledger: self.scope.ledger,
            route_epoch: self.scope.route_epoch,
            policy_revision: self.scope.policy_revision,
            peers: self.voters.union(&self.copies).copied().collect(),
        }
    }
    fn bytes(&self) -> Result<usize, AccessError> {
        self.voters
            .len()
            .checked_add(self.copies.len())
            .and_then(|n| n.checked_mul(128))
            .and_then(|n| n.checked_add(512))
            .and_then(|n| n.checked_add(self.fence.bytes().ok()?))
            .ok_or(AccessError::Capacity)
    }
}

/// Cannot be decoded or supplied by a network caller. The replica rechecks its
/// request and placement fence immediately before proposing the artifact.
pub struct EvidenceWitness {
    scope: CustodyScope,
    request: RequestId,
    epoch: RequestEpoch,
    managed: Option<ManagedRequestKey>,
    principal: ParticipantId,
    descriptor: ContentHash,
    voters: BTreeSet<u64>,
    _allocation: Allocation,
}
pub struct EvidencedRequest {
    request: VerifiedRequest,
    witness: EvidenceWitness,
}
/// An artifact-bearing native frame whose inline payload the exclusive content
/// writer sealed and verified under the current placement.
pub struct NativeEvidencedRequest {
    pub(crate) request: VerifiedRequest,
    pub(crate) evidence: focal_evidence::VerifiedNativeArtifact,
}
impl EvidenceWitness {
    pub(crate) fn validate(
        &self,
        verified: &VerifiedRequest,
        scope: CustodyScope,
        voters: &[u64],
    ) -> Result<EvidenceAttestation, AccessError> {
        let request = verified.request();
        if self.scope != scope
            || request.ledger != scope.ledger
            || request.route_epoch != scope.route_epoch
            || self.request != request.request_id
            || self.epoch != request.request_epoch
            || self.managed != managed_key(&request.operation)
            || self.principal != verified.peer().principal()
            || self.voters.len() != voters.len()
            || voters.iter().any(|id| !self.voters.contains(id))
        {
            return Err(AccessError::Unavailable);
        }
        let artifact = artifact(&request.operation).ok_or(AccessError::InvalidRequest)?;
        if artifact
            .content
            .content_hash()
            .map_err(|_| AccessError::InvalidRequest)?
            != self.descriptor
        {
            return Err(AccessError::InvalidRequest);
        }
        Ok(EvidenceAttestation {
            descriptor_hash: self.descriptor,
            custody_revision: scope.policy_revision,
            durable: true,
            schema_valid: true,
        })
    }
}

enum JobKind {
    Seal(oneshot::Sender<Result<ContentRef, AccessError>>),
    Attest(oneshot::Sender<Result<EvidencedRequest, AccessError>>),
    AttestNative(oneshot::Sender<Result<NativeEvidencedRequest, AccessError>>),
    /// The custody obligation of one object, with the request handed back.
    Obligation(
        ContentRef,
        oneshot::Sender<Result<(CustodyObligation, VerifiedRequest), AccessError>>,
    ),
    /// Seal an archive bundle (26 §4) as content of the ledger under its
    /// current placement, replicate it to every required copy and report
    /// the obligation.
    Archive {
        bytes: Vec<u8>,
        reply: oneshot::Sender<Result<ArchiveOutcome, AccessError>>,
    },
    /// Re-verify every object the session's committed artifact projection
    /// names on this node, recopy what is missing from another required
    /// copy and complete the other required copies (24 §20).
    Repair {
        snapshot: Box<focal_ledger::DurableEvidenceSnapshot>,
        after: Option<ArtifactId>,
        limit: u32,
        reply: oneshot::Sender<Result<RepairReport, AccessError>>,
    },
}
/// A sealed archive bundle: the object that names it and which required
/// copies hold a receipt for it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArchiveOutcome {
    pub reference: ContentRef,
    pub obligation: CustodyObligation,
}
/// What one repair pass over a session's committed artifact projection
/// found and did on this node (24 §20). `next_after` names where a walk
/// that was bounded or outlived by its snapshot resumes; `complete` says
/// the projection was walked to its end.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RepairReport {
    pub sequence: SessionSeq,
    pub index: RaftIndex,
    pub artifacts: u64,
    pub objects: u64,
    pub verified: u64,
    pub repaired: u64,
    pub pushed: u64,
    pub unrecoverable: Vec<UnrecoverableObject>,
    pub unrecoverable_count: u64,
    pub complete: bool,
    pub next_after: Option<ArtifactId>,
}
/// An object no required copy could supply: the session needs a restore.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnrecoverableObject {
    pub artifact: ArtifactId,
    pub reference: ContentRef,
    pub asked: u32,
}
/// The objects one repair call examines at most.
pub const MAX_REPAIR_OBJECTS: u32 = 4096;
const MAX_UNRECOVERABLE_LISTED: usize = 64;
struct Job {
    /// The authenticated request a participant job serves; trusted node
    /// jobs carry none.
    request: Option<VerifiedRequest>,
    ledger: LedgerId,
    route: RouteEpoch,
    kind: JobKind,
    _allocation: Allocation,
}
struct PlacementRow {
    placement: EvidencePlacement,
    _allocation: Allocation,
}
struct Replacement {
    expected: Option<CustodyScope>,
    row: PlacementRow,
    reply: oneshot::Sender<Result<(), AccessError>>,
}
enum Completed {
    Seal {
        scope: Option<CustodyScope>,
        result: Result<ContentRef, AccessError>,
        reply: oneshot::Sender<Result<ContentRef, AccessError>>,
        _allocation: Allocation,
    },
    Attest {
        scope: Option<CustodyScope>,
        result: Box<Result<EvidencedRequest, AccessError>>,
        reply: oneshot::Sender<Result<EvidencedRequest, AccessError>>,
    },
    AttestNative {
        scope: Option<CustodyScope>,
        result: Box<Result<NativeEvidencedRequest, AccessError>>,
        reply: oneshot::Sender<Result<NativeEvidencedRequest, AccessError>>,
    },
    Obligation {
        scope: Option<CustodyScope>,
        result: Box<Result<(CustodyObligation, VerifiedRequest), AccessError>>,
        reply: oneshot::Sender<Result<(CustodyObligation, VerifiedRequest), AccessError>>,
    },
    Archive {
        scope: Option<CustodyScope>,
        result: Box<Result<ArchiveOutcome, AccessError>>,
        reply: oneshot::Sender<Result<ArchiveOutcome, AccessError>>,
    },
    Repair {
        scope: Option<CustodyScope>,
        result: Box<Result<RepairReport, AccessError>>,
        reply: oneshot::Sender<Result<RepairReport, AccessError>>,
    },
}
impl Completed {
    fn send(self, placements: &BTreeMap<LedgerId, PlacementRow>) {
        let current = |scope: Option<CustodyScope>| {
            scope.is_some_and(|scope| {
                placements
                    .get(&scope.ledger)
                    .is_some_and(|row| row.placement.scope == scope)
            })
        };
        match self {
            Self::Seal {
                scope,
                result,
                reply,
                _allocation,
            } => {
                let result = result.and_then(|value| {
                    if current(scope) {
                        Ok(value)
                    } else {
                        Err(AccessError::Unavailable)
                    }
                });
                let _ = reply.send(result);
            }
            Self::Attest {
                scope,
                result,
                reply,
            } => {
                let result = (*result).and_then(|value| {
                    if current(scope) {
                        Ok(value)
                    } else {
                        Err(AccessError::Unavailable)
                    }
                });
                let _ = reply.send(result);
            }
            Self::AttestNative {
                scope,
                result,
                reply,
            } => {
                let result = (*result).and_then(|value| {
                    if current(scope) {
                        Ok(value)
                    } else {
                        Err(AccessError::Unavailable)
                    }
                });
                let _ = reply.send(result);
            }
            Self::Obligation {
                scope,
                result,
                reply,
            } => {
                let result = (*result).and_then(|value| {
                    if current(scope) {
                        Ok(value)
                    } else {
                        Err(AccessError::Unavailable)
                    }
                });
                let _ = reply.send(result);
            }
            Self::Archive {
                scope,
                result,
                reply,
            } => {
                let result = (*result).and_then(|value| {
                    if current(scope) {
                        Ok(value)
                    } else {
                        Err(AccessError::Unavailable)
                    }
                });
                let _ = reply.send(result);
            }
            Self::Repair {
                scope,
                result,
                reply,
            } => {
                let result = (*result).and_then(|value| {
                    if current(scope) {
                        Ok(value)
                    } else {
                        Err(AccessError::Unavailable)
                    }
                });
                let _ = reply.send(result);
            }
        }
    }
}
#[derive(Clone)]
pub struct EvidenceCoordinator {
    sender: mpsc::Sender<Job>,
    control: mpsc::Sender<Replacement>,
    budget: MemoryBudget,
}
pub struct EvidenceDriver {
    receiver: mpsc::Receiver<Job>,
    control: mpsc::Receiver<Replacement>,
    content: ContentHost,
    node: u64,
    placements: BTreeMap<LedgerId, PlacementRow>,
    concurrency: usize,
    budget: MemoryBudget,
    _configuration: Allocation,
}
impl EvidenceCoordinator {
    /// The caller runs the returned driver alongside transport and Raft egress;
    /// both borrow one peer pool without another Arc wrapper or spawned task.
    pub fn channel(
        content: ContentHost,
        node: u64,
        placements: Vec<EvidencePlacement>,
        budget: MemoryBudget,
        concurrency: usize,
    ) -> Result<(Self, EvidenceDriver), AccessError> {
        if node == 0 || !(1..=64).contains(&concurrency) || placements.len() > 1024 {
            return Err(AccessError::InvalidRequest);
        }
        let configuration_bytes = placements
            .iter()
            .try_fold(0usize, |sum, placement| {
                placement
                    .voters
                    .len()
                    .checked_add(placement.copies.len())
                    .and_then(|n| n.checked_mul(128))
                    .and_then(|n| n.checked_add(512))
                    .and_then(|n| sum.checked_add(n))
            })
            .and_then(|n| {
                concurrency
                    .checked_mul(4096)
                    .and_then(|queue| n.checked_add(queue))
            })
            .ok_or(AccessError::Capacity)?;
        let mut configuration = budget
            .reserve(
                BudgetKind::Control,
                BudgetLane::Completion,
                configuration_bytes,
            )
            .map_err(|_| AccessError::Capacity)?
            .commit();
        let mut rows = BTreeMap::new();
        for placement in placements {
            if (!placement.voters.contains(&node) && !placement.copies.contains(&node))
                || rows.contains_key(&placement.scope.ledger)
                || placement.voters.union(&placement.copies).count() > 1024
            {
                return Err(AccessError::InvalidRequest);
            }
            let allocation = configuration
                .split_off(placement.bytes()?)
                .map_err(|_| AccessError::Capacity)?;
            rows.insert(
                placement.scope.ledger,
                PlacementRow {
                    placement,
                    _allocation: allocation,
                },
            );
        }
        let (sender, receiver) = mpsc::channel(concurrency);
        let (control, updates) = mpsc::channel(1);
        Ok((
            Self {
                sender,
                control,
                budget: budget.clone(),
            },
            EvidenceDriver {
                receiver,
                control: updates,
                content,
                node,
                placements: rows,
                concurrency,
                budget,
                _configuration: configuration,
            },
        ))
    }
    fn admit(&self, request: VerifiedRequest, kind: JobKind) -> Result<(), AccessError> {
        let bytes = postcard::experimental::serialized_size(request.request())
            .map_err(|_| AccessError::InvalidRequest)?
            .checked_mul(32)
            .and_then(|n| n.checked_add(JOB_BYTES))
            .ok_or(AccessError::Capacity)?;
        let allocation = self
            .budget
            .reserve(BudgetKind::Payload, BudgetLane::Ordinary, bytes)
            .map_err(|_| AccessError::Capacity)?
            .commit();
        let ledger = request.request().ledger;
        let route = request.request().route_epoch;
        self.sender
            .try_send(Job {
                request: Some(request),
                ledger,
                route,
                kind,
                _allocation: allocation,
            })
            .map_err(|error| match error {
                mpsc::error::TrySendError::Full(_) => AccessError::Capacity,
                mpsc::error::TrySendError::Closed(_) => AccessError::Unavailable,
            })
    }
    pub async fn seal(&self, request: VerifiedRequest) -> Result<ContentRef, AccessError> {
        let (send, receive) = oneshot::channel();
        self.admit(request, JobKind::Seal(send))?;
        receive.await.map_err(|_| AccessError::OutcomeUnknown)?
    }
    pub async fn attest(&self, request: VerifiedRequest) -> Result<EvidencedRequest, AccessError> {
        let (send, receive) = oneshot::channel();
        self.admit(request, JobKind::Attest(send))?;
        receive.await.map_err(|_| AccessError::OutcomeUnknown)?
    }
    /// Seal and verify the inline payload of an artifact-bearing native frame
    /// under the current placement; the owner still binds the evidence to the
    /// exact frame before admission.
    pub async fn attest_native(
        &self,
        request: VerifiedRequest,
    ) -> Result<NativeEvidencedRequest, AccessError> {
        let (send, receive) = oneshot::channel();
        self.admit(request, JobKind::AttestNative(send))?;
        receive.await.map_err(|_| AccessError::OutcomeUnknown)?
    }
    /// The custody obligation of one object under the request's placement:
    /// every required copy with a receipt at the current scope, else asked
    /// over its authenticated connection and recorded when it answers
    /// `Durable`; the request is handed back for admission.
    pub async fn obligation(
        &self,
        request: VerifiedRequest,
        reference: ContentRef,
    ) -> Result<(CustodyObligation, VerifiedRequest), AccessError> {
        let (send, receive) = oneshot::channel();
        self.admit(request, JobKind::Obligation(reference, send))?;
        receive.await.map_err(|_| AccessError::OutcomeUnknown)?
    }
    /// Seal an archive bundle (26 §4) as content of `ledger` under its
    /// current placement, replicate it to every required copy and report
    /// the custody obligation; the bytes are charged here until the job is
    /// done. A trusted node job: no participant request stands behind it.
    pub async fn archive(
        &self,
        ledger: LedgerId,
        route: RouteEpoch,
        bytes: Vec<u8>,
    ) -> Result<ArchiveOutcome, AccessError> {
        let charge = bytes
            .len()
            .checked_mul(2)
            .and_then(|n| n.checked_add(JOB_BYTES))
            .ok_or(AccessError::Capacity)?;
        let allocation = self
            .budget
            .reserve(BudgetKind::Payload, BudgetLane::Ordinary, charge)
            .map_err(|_| AccessError::Capacity)?
            .commit();
        let (reply, receive) = oneshot::channel();
        self.sender
            .try_send(Job {
                request: None,
                ledger,
                route,
                kind: JobKind::Archive { bytes, reply },
                _allocation: allocation,
            })
            .map_err(|error| match error {
                mpsc::error::TrySendError::Full(_) => AccessError::Capacity,
                mpsc::error::TrySendError::Closed(_) => AccessError::Unavailable,
            })?;
        receive.await.map_err(|_| AccessError::OutcomeUnknown)?
    }
    /// Repair one session's custody on this node (24 §20): a trusted node
    /// job under the session's current placement, walking the committed
    /// artifact projection the snapshot holds. `after` resumes a walk and
    /// `limit` bounds the objects one call examines.
    pub async fn repair(
        &self,
        snapshot: focal_ledger::DurableEvidenceSnapshot,
        after: Option<ArtifactId>,
        limit: u32,
    ) -> Result<RepairReport, AccessError> {
        if limit == 0 || limit > MAX_REPAIR_OBJECTS {
            return Err(AccessError::InvalidRequest);
        }
        let allocation = self
            .budget
            .reserve(BudgetKind::Payload, BudgetLane::Ordinary, JOB_BYTES)
            .map_err(|_| AccessError::Capacity)?
            .commit();
        let ledger = snapshot.prefix().ledger;
        let route = snapshot.prefix().route;
        let (reply, receive) = oneshot::channel();
        self.sender
            .try_send(Job {
                request: None,
                ledger,
                route,
                kind: JobKind::Repair {
                    snapshot: Box::new(snapshot),
                    after,
                    limit,
                    reply,
                },
                _allocation: allocation,
            })
            .map_err(|error| match error {
                mpsc::error::TrySendError::Full(_) => AccessError::Capacity,
                mpsc::error::TrySendError::Closed(_) => AccessError::Unavailable,
            })?;
        receive.await.map_err(|_| AccessError::OutcomeUnknown)?
    }
    /// Install a verified committed placement through the existing owner. The
    /// capacity-one completion mailbox remains available while data jobs stall.
    /// A canceled response is unknown; retry the complete target and expected
    /// scope to reconcile content installation before placement publication.
    pub async fn replace_placement(
        &self,
        expected: Option<CustodyScope>,
        placement: EvidencePlacement,
    ) -> Result<(), AccessError> {
        let allocation = self
            .budget
            .reserve(
                BudgetKind::Control,
                BudgetLane::Completion,
                placement
                    .bytes()?
                    .checked_mul(2)
                    .and_then(|n| n.checked_add(4096))
                    .ok_or(AccessError::Capacity)?,
            )
            .map_err(|_| AccessError::Capacity)?
            .commit();
        let (reply, receive) = oneshot::channel();
        self.control
            .try_send(Replacement {
                expected,
                row: PlacementRow {
                    placement,
                    _allocation: allocation,
                },
                reply,
            })
            .map_err(|error| match error {
                mpsc::error::TrySendError::Full(_) => AccessError::Capacity,
                mpsc::error::TrySendError::Closed(_) => AccessError::Unavailable,
            })?;
        receive.await.map_err(|_| AccessError::OutcomeUnknown)?
    }
}
impl EvidenceDriver {
    pub async fn run(self, pool: &PeerConnectionPool) -> Result<(), AccessError> {
        if tokio::runtime::Handle::try_current().is_err() {
            return Err(AccessError::Unavailable);
        }
        std::panic::AssertUnwindSafe(self.run_inner(pool))
            .catch_unwind()
            .await
            .map_err(|_| AccessError::Unavailable)?
    }
    async fn run_inner(mut self, pool: &PeerConnectionPool) -> Result<(), AccessError> {
        let content = self.content.clone();
        let mut tasks = FuturesUnordered::new();
        let mut receiving = true;
        let mut updates = true;
        while receiving || updates || !tasks.is_empty() {
            tokio::select! {
                biased;
                update = self.control.recv(), if updates => {
                    if let Some(update) = update {
                        let result = self.replace(update.expected, update.row).await;
                        let _ = update.reply.send(result);
                    } else {updates=false;}
                }
                job = self.receiver.recv(), if receiving && tasks.len() < self.concurrency => {
                    if let Some(job) = job {
                        let placement = self.snapshot(job.ledger, job.route);
                        tasks.push(process(&content, pool, self.node, placement, job));
                    } else { receiving = false; }
                }
                done = tasks.next(), if !tasks.is_empty() => {
                    if let Some(done) = done {done.send(&self.placements);}
                }
            }
        }
        Ok(())
    }
    fn snapshot(&self, ledger: LedgerId, route: RouteEpoch) -> Result<PlacementRow, AccessError> {
        let row = self
            .placements
            .get(&ledger)
            .filter(|row| row.placement.scope.route_epoch == route)
            .ok_or(AccessError::Unavailable)?;
        let allocation = self
            .budget
            .reserve(
                BudgetKind::Pending,
                BudgetLane::Ordinary,
                row.placement.bytes()?,
            )
            .map_err(|_| AccessError::Capacity)?
            .commit();
        Ok(PlacementRow {
            placement: row.placement.clone(),
            _allocation: allocation,
        })
    }
    async fn replace(
        &mut self,
        expected: Option<CustodyScope>,
        mut row: PlacementRow,
    ) -> Result<(), AccessError> {
        let next = &row.placement;
        if (!next.voters.contains(&self.node) && !next.copies.contains(&self.node))
            || next.voters.union(&next.copies).count() > 1024
        {
            return Err(AccessError::InvalidRequest);
        }
        if let Some(current) = self.placements.get(&next.scope.ledger) {
            // The same scope with the same members may change only its
            // residency fence (24 §22): the boundary or a node's region.
            let fence_only = next.scope == current.placement.scope
                && next.voters == current.placement.voters
                && next.copies == current.placement.copies;
            if current.placement != *next
                && !fence_only
                && (expected != Some(current.placement.scope)
                    || next.scope.route_epoch < current.placement.scope.route_epoch
                    || next.scope.policy_revision < current.placement.scope.policy_revision
                    || next.scope == current.placement.scope)
            {
                return Err(AccessError::Unavailable);
            }
        } else if expected.is_some() {
            return Err(AccessError::Unavailable);
        } else if self.placements.len() >= 1024 {
            return Err(AccessError::Capacity);
        }
        // The row and temporary policy are admitted before the content CAS.
        // Cancellation after its receipt leaves content ahead, which rejects old
        // jobs; an exact retry reconciles it. There is no two-owner transaction.
        let _staging = row
            ._allocation
            .split_off(
                next.bytes()?
                    .checked_add(4096)
                    .ok_or(AccessError::Capacity)?,
            )
            .map_err(|_| AccessError::Capacity)?;
        self.content
            .replace_policy(expected, next.custody_policy())
            .await?;
        self.placements.insert(row.placement.scope.ledger, row);
        Ok(())
    }
}

#[cfg(test)]
#[path = "evidence_service_tests.rs"]
mod tests;
async fn process(
    content: &ContentHost,
    pool: &PeerConnectionPool,
    node: u64,
    placement: Result<PlacementRow, AccessError>,
    job: Job,
) -> Completed {
    let scope = placement.as_ref().ok().map(|row| row.placement.scope);
    match job.kind {
        JobKind::Seal(response) => {
            let result = match (&placement, &job.request) {
                (Ok(row), Some(request)) => {
                    seal(content, pool, node, &row.placement, request).await
                }
                (Err(error), _) => Err(error.clone()),
                (Ok(_), None) => Err(AccessError::InvalidRequest),
            };
            drop(job.request);
            Completed::Seal {
                scope,
                result,
                reply: response,
                _allocation: job._allocation,
            }
        }
        JobKind::Attest(response) => {
            let result = match (&placement, job.request) {
                (Ok(row), Some(request)) => {
                    attest(
                        content,
                        pool,
                        node,
                        &row.placement,
                        request,
                        job._allocation,
                    )
                    .await
                }
                (Err(error), _) => Err(error.clone()),
                (Ok(_), None) => Err(AccessError::InvalidRequest),
            };
            Completed::Attest {
                scope,
                result: Box::new(result),
                reply: response,
            }
        }
        JobKind::AttestNative(response) => {
            let result = match (&placement, job.request) {
                (Ok(row), Some(request)) => {
                    attest_native(content, pool, node, &row.placement, request).await
                }
                (Err(error), _) => Err(error.clone()),
                (Ok(_), None) => Err(AccessError::InvalidRequest),
            };
            drop(job._allocation);
            Completed::AttestNative {
                scope,
                result: Box::new(result),
                reply: response,
            }
        }
        JobKind::Obligation(reference, response) => {
            let result = match (&placement, job.request) {
                (Ok(row), Some(request)) => {
                    obligation(content, pool, node, &row.placement, &reference)
                        .await
                        .map(|obligation| (obligation, request))
                }
                (Err(error), _) => Err(error.clone()),
                (Ok(_), None) => Err(AccessError::InvalidRequest),
            };
            drop(job._allocation);
            Completed::Obligation {
                scope,
                result: Box::new(result),
                reply: response,
            }
        }
        JobKind::Archive { bytes, reply } => {
            let result = match &placement {
                Ok(row) => archive(content, pool, node, &row.placement, bytes).await,
                Err(error) => Err(error.clone()),
            };
            drop(job._allocation);
            Completed::Archive {
                scope,
                result: Box::new(result),
                reply,
            }
        }
        JobKind::Repair {
            snapshot,
            after,
            limit,
            reply,
        } => {
            let result = match &placement {
                Ok(row) => {
                    repair(content, pool, node, &row.placement, *snapshot, after, limit).await
                }
                Err(error) => Err(error.clone()),
            };
            drop(job._allocation);
            Completed::Repair {
                scope,
                result: Box::new(result),
                reply,
            }
        }
    }
}
/// Walk the session's committed artifact projection from `after`, at most
/// `limit` objects (24 §20). An object this node holds and verifies counts
/// as verified; one it lacks or fails to verify is pulled, chunk by
/// verified chunk, from another required copy (the content copies first,
/// then the voters) and counts as repaired, or as unrecoverable when no
/// copy answers with it. Every other required copy is asked to verify the
/// object again, receipt or not, and failing that is given it. Missing data is
/// recopied under the same object identity, never replaced by a fresh one,
/// and nothing is inferred from a copy that cannot answer. A walk the
/// snapshot's lease outlives stops where it is and names where to resume.
async fn repair(
    content: &ContentHost,
    pool: &PeerConnectionPool,
    node: u64,
    placement: &EvidencePlacement,
    snapshot: focal_ledger::DurableEvidenceSnapshot,
    after: Option<ArtifactId>,
    limit: u32,
) -> Result<RepairReport, AccessError> {
    let scope = placement.scope;
    let prefix = snapshot.prefix();
    if prefix.ledger != scope.ledger
        || prefix.route != scope.route_epoch
        || prefix.placement_epoch != scope.policy_revision
        || prefix.node != node
    {
        return Err(AccessError::Unavailable);
    }
    let domain = ContentDomainId(scope.ledger.tenant.0);
    let mut report = RepairReport {
        sequence: prefix.sequence,
        index: prefix.index,
        artifacts: 0,
        objects: 0,
        verified: 0,
        repaired: 0,
        pushed: 0,
        unrecoverable: Vec::new(),
        unrecoverable_count: 0,
        complete: false,
        next_after: after,
    };
    // Where a missing object is asked for: the content copies, then the
    // voters, each once, never this node.
    let mut sources: Vec<u64> = Vec::new();
    for peer in placement.copies.iter().chain(placement.voters.iter()) {
        if *peer != node && !sources.contains(peer) && placement.fence.admits(*peer) {
            sources.try_reserve(1).map_err(|_| AccessError::Capacity)?;
            sources.push(*peer);
        }
    }
    let mut after = after;
    while report.objects < u64::from(limit) {
        let now = match snapshot.elapsed_clock() {
            Ok(now) => now,
            Err(error) => match crate::custody_prefix::snapshot_error(error) {
                AccessError::SnapshotExpired => break,
                error => return Err(error),
            },
        };
        let next = match snapshot.artifact_after(after, now) {
            Ok(next) => next,
            Err(error) => match crate::custody_prefix::snapshot_error(error) {
                AccessError::SnapshotExpired => break,
                error => return Err(error),
            },
        };
        let Some(artifact) = next else {
            report.complete = true;
            report.next_after = None;
            break;
        };
        if after.is_some_and(|previous| artifact.artifact.id <= previous) {
            return Err(AccessError::InvalidRequest);
        }
        after = Some(artifact.artifact.id);
        report.next_after = after;
        report.artifacts = report
            .artifacts
            .checked_add(1)
            .ok_or(AccessError::Capacity)?;
        let Some(reference) = artifact.content else {
            continue;
        };
        if reference.domain != domain {
            return Err(AccessError::Unauthorized);
        }
        report.objects = report.objects.checked_add(1).ok_or(AccessError::Capacity)?;
        let request = RequestId(
            reference
                .root
                .0
                .get(..16)
                .and_then(|bytes| bytes.try_into().ok())
                .ok_or(AccessError::InvalidRequest)?,
        );
        let transfer = transfer_id(scope, request, &reference);
        let held = matches!(
            local(
                content,
                node,
                scope,
                transfer,
                CustodyRequest::Verify {
                    policy_revision: scope.policy_revision,
                    content: reference.clone(),
                },
            )
            .await,
            Ok(CustodyReply::Durable { content: found, policy_revision })
                if found == reference && policy_revision == scope.policy_revision
        );
        if held {
            report.verified = report
                .verified
                .checked_add(1)
                .ok_or(AccessError::Capacity)?;
        } else {
            let mut asked = 0u32;
            let mut recovered = false;
            for peer in &sources {
                asked = asked.saturating_add(1);
                if pull(content, pool, node, *peer, scope, transfer, &reference)
                    .await
                    .is_ok()
                {
                    recovered = true;
                    break;
                }
            }
            if !recovered {
                report.unrecoverable_count = report
                    .unrecoverable_count
                    .checked_add(1)
                    .ok_or(AccessError::Capacity)?;
                if report.unrecoverable.len() < MAX_UNRECOVERABLE_LISTED {
                    report
                        .unrecoverable
                        .try_reserve(1)
                        .map_err(|_| AccessError::Capacity)?;
                    report.unrecoverable.push(UnrecoverableObject {
                        artifact: artifact.artifact.id,
                        reference: reference.clone(),
                        asked,
                    });
                }
                continue;
            }
            report.repaired = report
                .repaired
                .checked_add(1)
                .ok_or(AccessError::Capacity)?;
        }
        if placement.copies.contains(&node) {
            content
                .record_receipt(scope, receipt_for(scope, node, &reference))
                .await?;
        }
        // A repair re-asks every other required copy, receipt or not: a
        // receipt records an answer once given, not the bytes still held. A
        // copy outside the residency boundary is neither asked nor given.
        for peer in placement.copies.iter().filter(|peer| **peer != node) {
            if !placement.fence.admits(*peer) {
                continue;
            }
            let verified = remote(
                pool,
                *peer,
                scope,
                transfer,
                CustodyRequest::Verify {
                    policy_revision: scope.policy_revision,
                    content: reference.clone(),
                },
            )
            .await;
            if matches!(
                verified,
                Ok(CustodyReply::Durable { content: found, policy_revision })
                    if found == reference && policy_revision == scope.policy_revision
            ) {
                content
                    .record_receipt(scope, receipt_for(scope, *peer, &reference))
                    .await?;
                continue;
            }
            if push(content, pool, scope, transfer, *peer, &reference)
                .await
                .is_ok()
            {
                report.pushed = report.pushed.checked_add(1).ok_or(AccessError::Capacity)?;
            }
        }
    }
    Ok(report)
}
/// Give one required copy an object this node holds: open the transfer
/// with this node's manifest, send the chunks the copy lacks, seal, record
/// the copy's `Durable` answer as its receipt.
async fn push(
    content: &ContentHost,
    pool: &PeerConnectionPool,
    scope: CustodyScope,
    transfer: [u8; 16],
    peer: u64,
    reference: &ContentRef,
) -> Result<(), AccessError> {
    let manifest = content.export_manifest(scope, reference.clone()).await?;
    let opened = remote(
        pool,
        peer,
        scope,
        transfer,
        CustodyRequest::Open {
            transfer,
            policy_revision: scope.policy_revision,
            content: reference.clone(),
            manifest: manifest.value().encoded().to_vec(),
        },
    )
    .await?;
    let CustodyReply::Opened {
        chunks,
        next_missing,
    } = opened
    else {
        return Err(AccessError::InvalidRequest);
    };
    if chunks as usize != manifest.value().chunks() || next_missing > chunks {
        return Err(AccessError::InvalidRequest);
    }
    for index in next_missing..chunks {
        let bytes = content
            .read_transfer_chunk(scope, reference.clone(), index as usize)
            .await?;
        remote(
            pool,
            peer,
            scope,
            transfer,
            CustodyRequest::Chunk {
                transfer,
                index,
                bytes: bytes.value().clone(),
            },
        )
        .await?;
    }
    let reply = remote(
        pool,
        peer,
        scope,
        transfer,
        CustodyRequest::Seal { transfer },
    )
    .await?;
    if !matches!(reply, CustodyReply::Durable { content: found, policy_revision } if found == *reference && policy_revision == scope.policy_revision)
    {
        return Err(AccessError::InvalidRequest);
    }
    content
        .record_receipt(scope, receipt_for(scope, peer, reference))
        .await?;
    let _ = remote(
        pool,
        peer,
        scope,
        transfer,
        CustodyRequest::Cancel { transfer },
    )
    .await;
    Ok(())
}
/// Seal an archive bundle (26 §4) as an object of the ledger's tenant
/// domain, replicate it to every other required copy exactly as a sealed
/// upload is, and report which required copies hold a receipt for it. The
/// bundle names itself by its content root and length; the retirement
/// record carries both.
async fn archive(
    content: &ContentHost,
    pool: &PeerConnectionPool,
    node: u64,
    placement: &EvidencePlacement,
    bytes: Vec<u8>,
) -> Result<ArchiveOutcome, AccessError> {
    let scope = placement.scope;
    let (chunk_bytes, _) = content.import_chunking();
    let domain = ContentDomainId(scope.ledger.tenant.0);
    let reference = content
        .seal_import_inline(domain, bytes, chunk_bytes)
        .await?;
    let request = RequestId(
        reference
            .root
            .0
            .get(..16)
            .and_then(|bytes| bytes.try_into().ok())
            .ok_or(AccessError::InvalidRequest)?,
    );
    replicate(content, pool, node, placement, request, &reference).await?;
    let obligation = obligation(content, pool, node, placement, &reference).await?;
    Ok(ArchiveOutcome {
        reference,
        obligation,
    })
}
/// The obligation of one object: a receipt at the current scope counts; a
/// copy without one is asked to verify the object now (this node through
/// its own store) and its `Durable` answer is recorded as its receipt. A
/// copy that cannot answer is simply not held; nothing is ever inferred
/// from a request or an intent.
async fn obligation(
    content: &ContentHost,
    pool: &PeerConnectionPool,
    node: u64,
    placement: &EvidencePlacement,
    reference: &ContentRef,
) -> Result<CustodyObligation, AccessError> {
    let scope = placement.scope;
    let required = placement.copies.clone();
    let mut held = BTreeSet::new();
    let transfer = transfer_id(
        scope,
        RequestId(reference.root.0[..16].try_into().unwrap_or([0; 16])),
        reference,
    );
    for peer in &required {
        if !placement.fence.admits(*peer) {
            continue;
        }
        if content
            .receipt(scope, reference.root, *peer)
            .await?
            .is_some_and(|receipt| {
                receipt.route_epoch == scope.route_epoch
                    && receipt.policy_revision == scope.policy_revision
                    && receipt.length == reference.length
            })
        {
            held.insert(*peer);
            continue;
        }
        let durable = if *peer == node {
            content
                .export_manifest(scope, reference.clone())
                .await
                .is_ok()
        } else {
            matches!(
                remote(
                    pool,
                    *peer,
                    scope,
                    transfer,
                    CustodyRequest::Verify {
                        policy_revision: scope.policy_revision,
                        content: reference.clone(),
                    },
                )
                .await,
                Ok(CustodyReply::Durable { content: found, policy_revision })
                    if found == *reference && policy_revision == scope.policy_revision
            )
        };
        if durable {
            content
                .record_receipt(scope, receipt_for(scope, *peer, reference))
                .await?;
            held.insert(*peer);
        }
    }
    Ok(CustodyObligation {
        scope,
        required,
        held,
    })
}
/// Seal and verify an artifact-bearing native frame's inline payload, then
/// replicate the sealed bytes to every other required copy of the current
/// placement, exactly as a sealed upload is. A missing required copy refuses
/// the frame before admission; a follower that later leads or serves reads
/// therefore holds the payload under its own custody.
async fn attest_native(
    content: &ContentHost,
    pool: &PeerConnectionPool,
    node: u64,
    placement: &EvidencePlacement,
    request: VerifiedRequest,
) -> Result<NativeEvidencedRequest, AccessError> {
    let Operation::Native { frame } = &request.request().operation else {
        return Err(AccessError::InvalidRequest);
    };
    if request.request().ledger != placement.scope.ledger {
        return Err(AccessError::Unauthorized);
    }
    let domain = focal_model::ContentDomainId(placement.scope.ledger.tenant.0);
    let limits = focal_ledger::NativeSessionLimits::standard(domain);
    let (key, descriptor) = crate::native_ingress::artifact_of_frame(&limits, frame)?
        .ok_or(AccessError::InvalidRequest)?;
    if key.principal != request.peer().principal()
        || key.id != request.request().request_id
        || key.epoch != request.request().request_epoch
        || descriptor.ledger() != placement.scope.ledger
    {
        return Err(AccessError::Unauthorized);
    }
    let evidence = content
        .verify_native(crate::content_host::NativeVerification {
            scope: placement.scope,
            request: key,
            descriptor,
            domain,
        })
        .await?;
    let payload = evidence.custody().payload();
    let reference = ContentRef {
        domain: payload.domain,
        root: payload.root,
        length: payload.length,
        class: payload.class,
    };
    replicate(content, pool, node, placement, key.id, &reference).await?;
    content.check_policy(placement.scope).await?;
    Ok(NativeEvidencedRequest { request, evidence })
}
async fn seal(
    content: &ContentHost,
    pool: &PeerConnectionPool,
    node: u64,
    placement: &EvidencePlacement,
    request: &VerifiedRequest,
) -> Result<ContentRef, AccessError> {
    let reference = content
        .seal_upload(placement.scope, request.clone())
        .await?;
    replicate(
        content,
        pool,
        node,
        placement,
        request.request().request_id,
        &reference,
    )
    .await?;
    content.check_policy(placement.scope).await?;
    Ok(reference)
}
fn managed_key(operation: &Operation) -> Option<ManagedRequestKey> {
    match operation {
        Operation::Managed { key, .. } => Some(*key),
        _ => None,
    }
}
fn custody_request_id(request: &VerifiedRequest) -> Result<RequestId, AccessError> {
    let Some(key) = managed_key(&request.request().operation) else {
        return Ok(request.request().request_id);
    };
    let bytes = postcard::to_stdvec(&key).map_err(|_| AccessError::InvalidRequest)?;
    let hash = blake3::derive_key("focal.evidence.managed-transfer-request.v1", &bytes);
    let id: [u8; 16] = hash
        .get(..16)
        .ok_or(AccessError::InvalidRequest)?
        .try_into()
        .map_err(|_| AccessError::InvalidRequest)?;
    Ok(RequestId(id))
}
fn artifact(operation: &Operation) -> Option<&NewArtifact> {
    let command = match operation {
        Operation::Submit { command, .. }
        | Operation::Managed {
            operation: ManagedOperation::Submit { command, .. },
            ..
        } => command,
        _ => return None,
    };
    match command {
        Command::AttachArtifact { artifact, .. } | Command::RegisterArtifact { artifact } => {
            Some(artifact)
        }
        Command::FailTestamentGeneration { error, .. } => Some(error),
        _ => None,
    }
}
async fn attest(
    content: &ContentHost,
    pool: &PeerConnectionPool,
    node: u64,
    placement: &EvidencePlacement,
    request: VerifiedRequest,
    mut allocation: Allocation,
) -> Result<EvidencedRequest, AccessError> {
    let custody_id = custody_request_id(&request)?;
    let artifact = artifact(&request.request().operation).ok_or(AccessError::InvalidRequest)?;
    if artifact.content.ledger != placement.scope.ledger {
        return Err(AccessError::Unauthorized);
    }
    let maximum = focal_evidence::builtin_schema_limit(artifact.content.schema_hash)
        .map_err(|_| AccessError::UnsupportedOperation)?;
    match &artifact.content.payload {
        ArtifactPayload::Inline(bytes) if bytes.len() <= 16 * 1024 => {
            // Inline bytes are part of the command itself. The receiving Raft
            // owner checks this placement's voters and publishes only on quorum.
            focal_evidence::verify_builtin_schema(artifact.content.schema_hash, bytes)
                .map_err(|_| AccessError::InvalidRequest)?;
        }
        ArtifactPayload::Content(reference) => {
            if reference.length > u64::try_from(maximum).map_err(|_| AccessError::Capacity)? {
                return Err(AccessError::Capacity);
            }
            ensure_local(content, pool, node, placement, custody_id, reference).await?;
            replicate(content, pool, node, placement, custody_id, reference).await?;
            let bytes = content
                .read_bytes(placement.scope, reference.clone(), maximum)
                .await?;
            focal_evidence::verify_builtin_schema(artifact.content.schema_hash, bytes.value())
                .map_err(|_| AccessError::InvalidRequest)?;
        }
        _ => return Err(AccessError::Capacity),
    }
    content.check_policy(placement.scope).await?;
    let request_bytes = postcard::experimental::serialized_size(request.request())
        .map_err(|_| AccessError::InvalidRequest)?
        .checked_mul(32)
        .ok_or(AccessError::Capacity)?;
    let witness_bytes = placement
        .voters
        .len()
        .checked_mul(128)
        .and_then(|n| n.checked_add(std::mem::size_of::<EvidenceWitness>()))
        .and_then(|n| n.checked_add(request_bytes))
        .ok_or(AccessError::Capacity)?;
    allocation
        .shrink_to(witness_bytes)
        .map_err(|_| AccessError::Capacity)?;
    let witness = EvidenceWitness {
        scope: placement.scope,
        request: request.request().request_id,
        epoch: request.request().request_epoch,
        managed: managed_key(&request.request().operation),
        principal: request.peer().principal(),
        descriptor: artifact
            .content
            .content_hash()
            .map_err(|_| AccessError::InvalidRequest)?,
        voters: placement.voters.clone(),
        _allocation: allocation,
    };
    Ok(EvidencedRequest { request, witness })
}
fn transfer_id(scope: CustodyScope, request: RequestId, reference: &ContentRef) -> [u8; 16] {
    let mut hash = blake3::Hasher::new_derive_key("focal.evidence.custody-transfer.v1");
    hash.update(&scope.ledger.tenant.0);
    hash.update(&scope.ledger.session.0);
    hash.update(&scope.route_epoch.0.to_be_bytes());
    hash.update(&scope.policy_revision.to_be_bytes());
    hash.update(&request.0);
    hash.update(&reference.root.0);
    let mut id = [0; 16];
    for (target, source) in id.iter_mut().zip(hash.finalize().as_bytes()) {
        *target = *source;
    }
    id
}
/// Pull one seed chunk of a checkpoint (25 §5) from `peer`, verified
/// against its hash before it is returned.
pub(crate) async fn pull_seed(
    pool: &PeerConnectionPool,
    peer: u64,
    scope: CustodyScope,
    hash: focal_model::ContentHash,
) -> Result<Vec<u8>, AccessError> {
    let mut id = [0u8; 16];
    for (target, source) in id.iter_mut().zip(hash.0.iter()) {
        *target = *source;
    }
    let reply = remote(
        pool,
        peer,
        scope,
        id,
        CustodyRequest::SeedChunk {
            hash,
            max_bytes: u32::try_from(focal_evidence::SEED_CHUNK_BYTES)
                .map_err(|_| AccessError::Capacity)?,
        },
    )
    .await?;
    let CustodyReply::SeedChunk { hash: read, bytes } = reply else {
        return Err(AccessError::InvalidRequest);
    };
    if read != hash || focal_model::ContentHash(*blake3::hash(&bytes).as_bytes()) != hash {
        return Err(AccessError::InvalidRequest);
    }
    Ok(bytes)
}
/// Pull one content object this node lacks from `peer`, chunk by verified
/// chunk under the same object identity (24 §20): what a fresh copy of a
/// session does for the objects its retained delivery names.
pub(crate) async fn pull_object(
    content: &ContentHost,
    pool: &PeerConnectionPool,
    node: u64,
    peer: u64,
    scope: CustodyScope,
    reference: &ContentRef,
) -> Result<(), AccessError> {
    let request = RequestId(
        reference
            .root
            .0
            .get(..16)
            .and_then(|bytes| bytes.try_into().ok())
            .ok_or(AccessError::InvalidRequest)?,
    );
    let transfer = transfer_id(scope, request, reference);
    pull(content, pool, node, peer, scope, transfer, reference).await
}
fn envelope(scope: CustodyScope, id: [u8; 16], operation: CustodyRequest) -> RequestEnvelope {
    RequestEnvelope {
        protocol: PROTOCOL_VERSION,
        ledger: scope.ledger,
        route_epoch: scope.route_epoch,
        request_epoch: RequestEpoch(1),
        request_id: RequestId(id),
        operation: Operation::Custody(operation),
    }
}
async fn remote(
    pool: &PeerConnectionPool,
    peer: u64,
    scope: CustodyScope,
    transfer: [u8; 16],
    operation: CustodyRequest,
) -> Result<CustodyReply, AccessError> {
    pool.send_custody(peer, &envelope(scope, transfer, operation))
        .await
        .map_err(|error| match error {
            PeerSendError::Busy => AccessError::Capacity,
            PeerSendError::Rejected(error) => error,
            _ => AccessError::OutcomeUnknown,
        })
}
async fn local(
    content: &ContentHost,
    node: u64,
    scope: CustodyScope,
    transfer: [u8; 16],
    operation: CustodyRequest,
) -> Result<CustodyReply, AccessError> {
    let peer = AuthenticatedPeer::local(PeerGrant {
        principal: ParticipantId::from_u128(u128::from(node)),
        tenants: BTreeSet::from([scope.ledger.tenant]),
        role: PeerRole::Node { node_id: node },
    })?;
    let verified = verify_request(
        peer,
        envelope(scope, transfer, operation),
        &crate::fleet::ReplicaHost::wire_limits(),
    )?;
    let reply = content.request(verified).await?;
    // The coordinator's job reservation covers this owned response after the
    // actor's delivery permit is released; there is no unaccounted byte escape.
    match reply.value().result.clone() {
        Response::Custody(reply) => Ok(reply),
        Response::Error(error) => Err(error),
        _ => Err(AccessError::InvalidRequest),
    }
}
async fn replicate(
    content: &ContentHost,
    pool: &PeerConnectionPool,
    node: u64,
    placement: &EvidencePlacement,
    request: RequestId,
    reference: &ContentRef,
) -> Result<(), AccessError> {
    let scope = placement.scope;
    let transfer = transfer_id(scope, request, reference);
    let manifest = content.export_manifest(scope, reference.clone()).await?;
    // This node's own sealed object is its own receipt.
    if placement.copies.contains(&node) {
        content
            .record_receipt(scope, receipt_for(scope, node, reference))
            .await?;
    }
    for peer in &placement.copies {
        if *peer == node {
            continue;
        }
        placement.admits(*peer)?;
        if matches!(remote(pool, *peer, scope, transfer, CustodyRequest::Verify { policy_revision: scope.policy_revision, content: reference.clone() }).await,
            Ok(CustodyReply::Durable { content: found, policy_revision }) if found == *reference && policy_revision == scope.policy_revision)
        {
            content
                .record_receipt(scope, receipt_for(scope, *peer, reference))
                .await?;
            continue;
        }
        let opened = remote(
            pool,
            *peer,
            scope,
            transfer,
            CustodyRequest::Open {
                transfer,
                policy_revision: scope.policy_revision,
                content: reference.clone(),
                manifest: manifest.value().encoded().to_vec(),
            },
        )
        .await?;
        let CustodyReply::Opened {
            chunks,
            next_missing,
        } = opened
        else {
            return Err(AccessError::InvalidRequest);
        };
        if chunks as usize != manifest.value().chunks() || next_missing > chunks {
            return Err(AccessError::InvalidRequest);
        }
        for index in next_missing..chunks {
            let bytes = content
                .read_transfer_chunk(scope, reference.clone(), index as usize)
                .await?;
            remote(
                pool,
                *peer,
                scope,
                transfer,
                CustodyRequest::Chunk {
                    transfer,
                    index,
                    bytes: bytes.value().clone(),
                },
            )
            .await?;
        }
        let reply = remote(
            pool,
            *peer,
            scope,
            transfer,
            CustodyRequest::Seal { transfer },
        )
        .await?;
        if !matches!(reply, CustodyReply::Durable { content: found, policy_revision } if found == *reference && policy_revision == scope.policy_revision)
        {
            return Err(AccessError::InvalidRequest);
        }
        content
            .record_receipt(scope, receipt_for(scope, *peer, reference))
            .await?;
        let _ = remote(
            pool,
            *peer,
            scope,
            transfer,
            CustodyRequest::Cancel { transfer },
        )
        .await;
    }
    Ok(())
}
async fn ensure_local(
    content: &ContentHost,
    pool: &PeerConnectionPool,
    node: u64,
    placement: &EvidencePlacement,
    request: RequestId,
    reference: &ContentRef,
) -> Result<(), AccessError> {
    let scope = placement.scope;
    let transfer = transfer_id(scope, request, reference);
    if matches!(
        local(
            content,
            node,
            scope,
            transfer,
            CustodyRequest::Verify {
                policy_revision: scope.policy_revision,
                content: reference.clone()
            }
        )
        .await,
        Ok(CustodyReply::Durable { .. })
    ) {
        return Ok(());
    }
    for peer in placement.copies.iter().filter(|peer| **peer != node) {
        if !placement.fence.admits(*peer) {
            continue;
        }
        let result = pull(content, pool, node, *peer, scope, transfer, reference).await;
        if result.is_ok() {
            return Ok(());
        }
    }
    Err(AccessError::Unavailable)
}
async fn pull(
    content: &ContentHost,
    pool: &PeerConnectionPool,
    node: u64,
    peer: u64,
    scope: CustodyScope,
    transfer: [u8; 16],
    reference: &ContentRef,
) -> Result<(), AccessError> {
    let reply = remote(
        pool,
        peer,
        scope,
        transfer,
        CustodyRequest::Manifest {
            policy_revision: scope.policy_revision,
            content: reference.clone(),
            max_bytes: u32::try_from(focal_evidence::MAX_TRANSFER_MANIFEST_BYTES)
                .map_err(|_| AccessError::Capacity)?,
        },
    )
    .await?;
    let CustodyReply::Manifest {
        content: described,
        manifest,
    } = reply
    else {
        return Err(AccessError::InvalidRequest);
    };
    if described != *reference {
        return Err(AccessError::InvalidRequest);
    }
    let open = CustodyRequest::Open {
        transfer,
        policy_revision: scope.policy_revision,
        content: reference.clone(),
        manifest,
    };
    let opened = local(content, node, scope, transfer, open.clone()).await?;
    remote(pool, peer, scope, transfer, open).await?;
    let CustodyReply::Opened {
        chunks,
        next_missing,
    } = opened
    else {
        return Err(AccessError::InvalidRequest);
    };
    for index in next_missing..chunks {
        let reply = remote(
            pool,
            peer,
            scope,
            transfer,
            CustodyRequest::ReadChunk {
                transfer,
                index,
                max_bytes: u32::try_from(focal_evidence::MAX_TRANSFER_CHUNK_BYTES)
                    .map_err(|_| AccessError::Capacity)?,
            },
        )
        .await?;
        let CustodyReply::Chunk {
            index: found,
            bytes,
        } = reply
        else {
            return Err(AccessError::InvalidRequest);
        };
        if index != found {
            return Err(AccessError::InvalidRequest);
        }
        local(
            content,
            node,
            scope,
            transfer,
            CustodyRequest::Chunk {
                transfer,
                index,
                bytes,
            },
        )
        .await?;
    }
    let reply = local(
        content,
        node,
        scope,
        transfer,
        CustodyRequest::Seal { transfer },
    )
    .await?;
    if !matches!(reply, CustodyReply::Durable { content: found, policy_revision } if found == *reference && policy_revision == scope.policy_revision)
    {
        return Err(AccessError::InvalidRequest);
    }
    let _ = local(
        content,
        node,
        scope,
        transfer,
        CustodyRequest::Cancel { transfer },
    )
    .await;
    let _ = remote(
        pool,
        peer,
        scope,
        transfer,
        CustodyRequest::Cancel { transfer },
    )
    .await;
    Ok(())
}

#[derive(Clone)]
pub struct FleetService {
    pub replica: crate::fleet::ReplicaHost,
    pub content: ContentHost,
    pub evidence: EvidenceCoordinator,
}
enum Eligibility {
    Refused(NativeRefusal),
    Failed(AccessError),
}
impl FleetService {
    /// A phase that evaluates an artifact begins only when every required
    /// copy holds it (doc 04 §7, R8 instruction 1): the artifact's pointer
    /// is read from the committed prefix and its obligation asked of the
    /// evidence coordinator; a copy short of custody refuses the frame as a
    /// retryable capacity condition naming the copies, and an artifact the
    /// prefix does not hold is left for the owner to refuse.
    async fn eligible(&self, request: VerifiedRequest) -> Result<VerifiedRequest, Eligibility> {
        let Operation::Native { frame } = &request.request().operation else {
            return Ok(request);
        };
        let ledger = request.request().ledger;
        let limits = focal_ledger::NativeSessionLimits::standard(ContentDomainId(ledger.tenant.0));
        let artifact = crate::native_ingress::evaluation_artifact_of_frame(&limits, frame)
            .map_err(Eligibility::Failed)?;
        let Some(artifact) = artifact else {
            return Ok(request);
        };
        let pointer = self
            .replica
            .artifact_pointer(artifact)
            .await
            .map_err(|error| Eligibility::Failed(crate::host::access(error)))?;
        let Some(pointer) = pointer else {
            return Ok(request);
        };
        let reference = ContentRef {
            domain: pointer.domain,
            root: pointer.root,
            length: pointer.length,
            class: pointer.class,
        };
        let (obligation, request) = self
            .evidence
            .obligation(request, reference)
            .await
            .map_err(Eligibility::Failed)?;
        if obligation.satisfied() {
            return Ok(request);
        }
        let missing: Vec<String> = obligation.missing().map(|node| node.to_string()).collect();
        Err(Eligibility::Refused(NativeRefusal {
            kind: NativeRefusalKind::Capacity,
            detail: format!(
                "custody: {} of {} required copies hold the artifact; missing nodes {}",
                obligation.held.len(),
                obligation.required.len(),
                missing.join(",")
            ),
        }))
    }
}
impl RequestHandler for FleetService {
    fn supports_managed_requests(&self) -> bool {
        true
    }
    fn supports_participant_requests(&self) -> bool {
        true
    }
    fn supports_native_requests(&self) -> bool {
        true
    }
    fn handle(
        &self,
        request: VerifiedRequest,
    ) -> Pin<Box<dyn Future<Output = ResponseEnvelope> + Send + '_>> {
        Box::pin(async move { self.handle_accounted(request).await.into_envelope() })
    }
    fn handle_accounted(&self, request: VerifiedRequest) -> OwnedHandlerFuture<'_> {
        Box::pin(async move {
            let mut response = request
                .request()
                .reply(Response::Error(AccessError::Unavailable));
            if matches!(
                request.request().operation,
                Operation::Upload(UploadRequest::Seal { .. })
            ) {
                response.result = match self.evidence.seal(request).await {
                    Ok(reference) => Response::Upload(UploadReply::Sealed(reference)),
                    Err(error) => Response::Error(error),
                };
                OwnedResponse::new(response)
            } else if matches!(
                request.request().operation,
                Operation::Custody(_) | Operation::Upload(_) | Operation::Download { .. }
            ) {
                self.content.handle_accounted(request).await
            } else if let Operation::Native { frame } = &request.request().operation
                && inspect_native_frame(frame)
                    .is_ok_and(|header| crate::native_ingress::artifact_bearing(header.command))
            {
                match self.evidence.attest_native(request).await {
                    Ok(evidence) => {
                        self.replica
                            .submit_with_native_evidence(evidence.request, evidence.evidence)
                            .await
                    }
                    Err(error) => {
                        response.result = Response::Error(error);
                        OwnedResponse::new(response)
                    }
                }
            } else if let Operation::Native { frame } = &request.request().operation
                && inspect_native_frame(frame)
                    .is_ok_and(|header| crate::native_ingress::evaluates_artifact(header.command))
            {
                match self.eligible(request).await {
                    Ok(request) => self.replica.handle_accounted(request).await,
                    Err(Eligibility::Refused(refusal)) => {
                        response.result = Response::Native(NativeMutationReply::Refused(refusal));
                        OwnedResponse::new(response)
                    }
                    Err(Eligibility::Failed(error)) => {
                        response.result = Response::Error(error);
                        OwnedResponse::new(response)
                    }
                }
            } else if artifact(&request.request().operation).is_some() {
                let probe = match self.replica.probe_receipt(request).await {
                    Ok(probe) => probe,
                    Err(error) => {
                        response.result = Response::Error(error);
                        return OwnedResponse::new(response);
                    }
                };
                let crate::fleet::ReceiptProbe {
                    request,
                    known,
                    allocation,
                } = probe;
                if let Some(known) = known {
                    response.result = known;
                    drop(request);
                    return crate::host::finish_response(response, allocation);
                }
                let evidence = self.evidence.attest(*request).await;
                drop(allocation);
                match evidence {
                    Ok(evidence) => {
                        self.replica
                            .submit_with_evidence(evidence.request, evidence.witness)
                            .await
                    }
                    Err(error) => {
                        response.result = Response::Error(error);
                        OwnedResponse::new(response)
                    }
                }
            } else {
                self.replica.handle_accounted(request).await
            }
        })
    }
}
