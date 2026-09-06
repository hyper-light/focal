//! Bounded evidence custody coordination outside the Raft owner. A content
//! reference is acknowledged only after every selected durable copy confirms it.
use crate::{
    config::Placement,
    content_host::ContentHost,
    custody::CustodyScope,
    placement::{self, NodeFacts, PlacementPlan},
};
use focal_memory::{Allocation, BudgetKind, BudgetLane, MemoryBudget};
use focal_model::*;
use focal_wire::*;
use futures_util::{StreamExt, stream::FuturesUnordered};
use std::{
    collections::{BTreeMap, BTreeSet},
    future::Future,
    pin::Pin,
};
use tokio::sync::{mpsc, oneshot};

const MAX_REPORT_BYTES: usize = 1024 * 1024;
const JOB_BYTES: usize = 8 * 1024 * 1024;

#[derive(Clone)]
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
    pub fn scope(&self) -> CustodyScope {
        self.scope
    }
}

/// Cannot be decoded or supplied by a network caller. The replica rechecks its
/// request and placement fence immediately before proposing the artifact.
pub struct EvidenceWitness {
    scope: CustodyScope,
    request: RequestId,
    epoch: RequestEpoch,
    principal: ParticipantId,
    descriptor: ContentHash,
    voters: BTreeSet<u64>,
    _allocation: Allocation,
}
pub struct EvidencedRequest {
    request: VerifiedRequest,
    witness: EvidenceWitness,
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
}
struct Job {
    request: VerifiedRequest,
    kind: JobKind,
    _allocation: Allocation,
}
#[derive(Clone)]
pub struct EvidenceCoordinator {
    sender: mpsc::Sender<Job>,
    budget: MemoryBudget,
}
pub struct EvidenceDriver {
    receiver: mpsc::Receiver<Job>,
    content: ContentHost,
    node: u64,
    placements: BTreeMap<LedgerId, EvidencePlacement>,
    concurrency: usize,
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
        if node == 0
            || !(1..=64).contains(&concurrency)
            || placements.is_empty()
            || placements.len() > 1024
        {
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
        let configuration = budget
            .reserve(
                BudgetKind::Control,
                BudgetLane::Completion,
                configuration_bytes,
            )
            .map_err(|_| AccessError::Capacity)?
            .commit();
        let count = placements.len();
        let placements: BTreeMap<_, _> = placements
            .into_iter()
            .map(|p| (p.scope.ledger, p))
            .collect();
        if placements.len() != count
            || placements
                .values()
                .any(|p| !p.voters.contains(&node) && !p.copies.contains(&node))
        {
            return Err(AccessError::InvalidRequest);
        }
        let (sender, receiver) = mpsc::channel(concurrency);
        Ok((
            Self { sender, budget },
            EvidenceDriver {
                receiver,
                content,
                node,
                placements,
                concurrency,
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
}
impl EvidenceDriver {
    pub async fn run(mut self, pool: &PeerConnectionPool) -> Result<(), AccessError> {
        if tokio::runtime::Handle::try_current().is_err() {
            return Err(AccessError::Unavailable);
        }
        let mut tasks = FuturesUnordered::new();
        let mut receiving = true;
        while receiving || !tasks.is_empty() {
            tokio::select! {
                job = self.receiver.recv(), if receiving && tasks.len() < self.concurrency => {
                    if let Some(job) = job {
                        tasks.push(process(&self.content, pool, self.node, &self.placements, job));
                    } else { receiving = false; }
                }
                _ = tasks.next(), if !tasks.is_empty() => {}
            }
        }
        Ok(())
    }
}
async fn process(
    content: &ContentHost,
    pool: &PeerConnectionPool,
    node: u64,
    placements: &BTreeMap<LedgerId, EvidencePlacement>,
    job: Job,
) {
    let placement = placements
        .get(&job.request.request().ledger)
        .filter(|p| p.scope.route_epoch == job.request.request().route_epoch);
    match job.kind {
        JobKind::Seal(response) => {
            let result = match placement {
                Some(placement) => seal(content, pool, node, placement, &job.request).await,
                None => Err(AccessError::Unavailable),
            };
            let _ = response.send(result);
            drop(job._allocation);
        }
        JobKind::Attest(response) => {
            let result = match placement {
                Some(placement) => {
                    attest(content, pool, node, placement, job.request, job._allocation).await
                }
                None => Err(AccessError::Unavailable),
            };
            let _ = response.send(result);
        }
    }
}
async fn seal(
    content: &ContentHost,
    pool: &PeerConnectionPool,
    node: u64,
    placement: &EvidencePlacement,
    request: &VerifiedRequest,
) -> Result<ContentRef, AccessError> {
    let reference = content.seal_upload(request.clone()).await?;
    replicate(
        content,
        pool,
        node,
        placement,
        request.request().request_id,
        &reference,
    )
    .await?;
    Ok(reference)
}
fn artifact(operation: &Operation) -> Option<&NewArtifact> {
    match operation {
        Operation::Submit {
            command:
                Command::AttachArtifact { artifact, .. } | Command::RegisterArtifact { artifact },
            ..
        } => Some(artifact),
        Operation::Submit {
            command: Command::FailTestamentGeneration { error, .. },
            ..
        } => Some(error),
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
    let artifact = artifact(&request.request().operation).ok_or(AccessError::InvalidRequest)?;
    if artifact.content.ledger != placement.scope.ledger {
        return Err(AccessError::Unauthorized);
    }
    if artifact.content.schema_hash != focal_evidence::test_report_schema() {
        return Err(AccessError::UnsupportedOperation);
    }
    match &artifact.content.payload {
        ArtifactPayload::Inline(bytes) if bytes.len() <= 16 * 1024 => {
            // Inline bytes are part of the command itself. The receiving Raft
            // owner checks this placement's voters and publishes only on quorum.
            let _: focal_evidence::TestReport =
                serde_json::from_slice(bytes).map_err(|_| AccessError::InvalidRequest)?;
        }
        ArtifactPayload::Content(reference) => {
            ensure_local(
                content,
                pool,
                node,
                placement,
                request.request().request_id,
                reference,
            )
            .await?;
            replicate(
                content,
                pool,
                node,
                placement,
                request.request().request_id,
                reference,
            )
            .await?;
            let bytes = content
                .read_bytes(placement.scope, reference.clone(), MAX_REPORT_BYTES)
                .await?;
            let _: focal_evidence::TestReport =
                serde_json::from_slice(bytes.value()).map_err(|_| AccessError::InvalidRequest)?;
        }
        _ => return Err(AccessError::Capacity),
    }
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
