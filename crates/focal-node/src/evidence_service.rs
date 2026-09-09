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

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EvidencePlacement {
    scope: CustodyScope,
    voters: BTreeSet<u64>,
    copies: BTreeSet<u64>,
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
        Ok(Self {
            scope,
            voters: plan.voters.iter().copied().collect(),
            copies: plan.content_copies.iter().copied().collect(),
        })
    }
    /// A placement the directory has committed and the agent installs on this
    /// node; the members are the committed voters and content copies.
    pub fn committed(
        scope: CustodyScope,
        voters: BTreeSet<u64>,
        copies: BTreeSet<u64>,
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
        })
    }
    pub fn scope(&self) -> CustodyScope {
        self.scope
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
}
struct Job {
    request: VerifiedRequest,
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
        self.sender
            .try_send(Job {
                request,
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
                        let placement = self.snapshot(job.request.request().ledger, job.request.request().route_epoch);
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
            if current.placement != *next
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
            let result = match &placement {
                Ok(row) => seal(content, pool, node, &row.placement, &job.request).await,
                Err(error) => Err(error.clone()),
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
            let result = match &placement {
                Ok(row) => {
                    attest(
                        content,
                        pool,
                        node,
                        &row.placement,
                        job.request,
                        job._allocation,
                    )
                    .await
                }
                Err(error) => Err(error.clone()),
            };
            Completed::Attest {
                scope,
                result: Box::new(result),
                reply: response,
            }
        }
        JobKind::AttestNative(response) => {
            let result = match &placement {
                Ok(row) => attest_native(content, pool, node, &row.placement, job.request).await,
                Err(error) => Err(error.clone()),
            };
            drop(job._allocation);
            Completed::AttestNative {
                scope,
                result: Box::new(result),
                reply: response,
            }
        }
    }
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
    for peer in &placement.copies {
        if *peer == node {
            continue;
        }
        if matches!(remote(pool, *peer, scope, transfer, CustodyRequest::Verify { policy_revision: scope.policy_revision, content: reference.clone() }).await,
            Ok(CustodyReply::Durable { content: found, policy_revision }) if found == *reference && policy_revision == scope.policy_revision)
        {
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
