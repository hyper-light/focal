use crate::*;
use focal_model::*;
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{Arc, RwLock},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PeerRole {
    Actor,
    Evaluator,
    Runtime,
    Node { node_id: u64 },
}
#[derive(Debug, Clone)]
pub struct PeerGrant {
    pub principal: ParticipantId,
    pub tenants: BTreeSet<TenantId>,
    pub role: PeerRole,
}

/// This identity is never deserialized from the wire. A TLS certificate is
/// verified first; a server-owned grant then supplies its authority.
#[derive(Debug, Clone)]
pub struct AuthenticatedPeer {
    grant: PeerGrant,
    fingerprint: Option<[u8; 32]>,
}
impl AuthenticatedPeer {
    /// Trusted local ingress may use this only after its OS credential checks.
    pub fn local(grant: PeerGrant) -> Result<Self, AccessError> {
        validate_grant(&grant)?;
        Ok(Self {
            grant,
            fingerprint: None,
        })
    }
    /// Borrowed scope check for trusted service routers before ledger lookup.
    pub fn permits_tenant(&self, tenant: TenantId) -> bool {
        self.grant.tenants.contains(&tenant)
    }
    pub fn principal(&self) -> ParticipantId {
        self.grant.principal
    }
    pub fn role(&self) -> PeerRole {
        self.grant.role
    }
    pub fn certificate_fingerprint(&self) -> Option<[u8; 32]> {
        self.fingerprint
    }
}

/// Certificate grants and revocations are shared by active connection tasks.
/// Clones must observe the same revocation table, so this Arc is intentional.
#[derive(Clone)]
pub struct PeerRegistry {
    grants: Arc<RwLock<BTreeMap<[u8; 32], PeerGrant>>>,
    max_peers: usize,
}
impl PeerRegistry {
    /// Replace a complete server-owned grant projection atomically. Validate
    /// before locking; existing requests must never see a partial rebuild.
    pub fn replace_grants(&self, next: BTreeMap<[u8; 32], PeerGrant>) -> Result<(), AccessError> {
        if next.len() > self.max_peers {
            return Err(AccessError::Capacity);
        }
        for (fingerprint, grant) in &next {
            if *fingerprint == [0; 32] {
                return Err(AccessError::InvalidRequest);
            }
            validate_grant(grant)?;
        }
        let previous = {
            let mut grants = self.grants.write().map_err(|_| AccessError::Unavailable)?;
            std::mem::replace(&mut *grants, next)
        };
        drop(previous);
        Ok(())
    }
    pub fn new(max_peers: usize) -> Result<Self, AccessError> {
        if max_peers == 0 || max_peers > 1_000_000 {
            return Err(AccessError::Capacity);
        }
        Ok(Self {
            grants: Arc::new(RwLock::new(BTreeMap::new())),
            max_peers,
        })
    }
    pub fn register_certificate(
        &self,
        der: &[u8],
        grant: PeerGrant,
    ) -> Result<[u8; 32], AccessError> {
        validate_grant(&grant)?;
        let hash = certificate_fingerprint(der);
        let mut grants = self.grants.write().map_err(|_| AccessError::Unavailable)?;
        if !grants.contains_key(&hash) && grants.len() >= self.max_peers {
            return Err(AccessError::Capacity);
        }
        grants.insert(hash, grant);
        Ok(hash)
    }
    pub fn revoke(&self, fingerprint: [u8; 32]) -> Result<(), AccessError> {
        self.grants
            .write()
            .map_err(|_| AccessError::Unavailable)?
            .remove(&fingerprint);
        Ok(())
    }
    pub fn authenticate(&self, fingerprint: [u8; 32]) -> Result<AuthenticatedPeer, AccessError> {
        let grant = self
            .grants
            .read()
            .map_err(|_| AccessError::Unavailable)?
            .get(&fingerprint)
            .cloned()
            .ok_or(AccessError::Unauthorized)?;
        Ok(AuthenticatedPeer {
            grant,
            fingerprint: Some(fingerprint),
        })
    }
}
pub fn certificate_fingerprint(der: &[u8]) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new_derive_key("focal.transport.peer-certificate.v1");
    hasher.update(der);
    *hasher.finalize().as_bytes()
}
fn validate_grant(grant: &PeerGrant) -> Result<(), AccessError> {
    if grant.principal.is_zero()
        || grant.tenants.is_empty()
        || grant.tenants.len() > 1024
        || grant.tenants.iter().any(|t| t.is_zero())
        || matches!(grant.role, PeerRole::Node { node_id: 0 })
    {
        return Err(AccessError::InvalidRequest);
    }
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Capability {
    Actor,
    Runtime,
    Evaluator,
    Replication,
}
pub fn capability(operation: &Operation) -> Capability {
    match operation {
        Operation::Raft { .. }
        | Operation::EnrollmentControl { .. }
        | Operation::Custody(_)
        | Operation::PeerControl { .. }
        | Operation::NodeContact { .. } => Capability::Replication,
        Operation::Control { .. } => Capability::Runtime,
        Operation::Submit { command, .. } => match command {
            Command::NegotiateEpoch { .. }
            | Command::AdvanceEpochFloor { .. }
            | Command::AdoptReceipt { .. }
            | Command::AcknowledgeTestament { .. }
            | Command::BeginWholeWorkValidation { .. }
            | Command::BeginIncrementValidation { .. }
            | Command::CompleteWholeWork { .. }
            | Command::FailPost { .. }
            | Command::FailReceipt { .. }
            | Command::FailTestamentGeneration { .. }
            | Command::RevokeClaim { .. }
            | Command::ExpireClaim { .. }
            | Command::RebindMonitor { .. }
            | Command::ReleaseScope { .. }
            | Command::RegisterArtifact { .. }
            | Command::ExpireMonitor { .. } => Capability::Runtime,
            Command::RecordValidationVerdict { .. }
            | Command::RecordFencedValidationVerdict { .. } => Capability::Evaluator,
            _ => Capability::Actor,
        },
        _ => Capability::Actor,
    }
}

#[derive(Debug, Clone)]
pub struct VerifiedRequest {
    peer: AuthenticatedPeer,
    request: RequestEnvelope,
}
impl VerifiedRequest {
    pub fn peer(&self) -> &AuthenticatedPeer {
        &self.peer
    }
    pub fn request(&self) -> &RequestEnvelope {
        &self.request
    }
    pub fn into_parts(self) -> (AuthenticatedPeer, RequestEnvelope) {
        (self.peer, self.request)
    }
    /// Server-owned authority enters here, never through decoded client input.
    /// Even trusted composition cannot elevate an actor grant into runtime work.
    pub fn into_authenticated(
        self,
        mut authority: AuthorityContext,
    ) -> Result<AuthenticatedInput, AccessError> {
        let (expected_revision, command, epoch_allocation) = match self.request.operation {
            Operation::Submit {
                expected_revision,
                command,
            } => (expected_revision, command, false),
            Operation::OpenEpoch { epoch } => (None, Command::NegotiateEpoch { epoch }, true),
            _ => return Err(AccessError::InvalidRequest),
        };
        // The sole actor-triggered runtime intent is own-principal epoch admission;
        // no command, principal, cause, or extra authority can be supplied with it.
        authority.runtime = matches!(self.peer.role(), PeerRole::Runtime) || epoch_allocation;
        let verify_claim = |claim: &NewClaim| {
            if claim.content.ledger != self.request.ledger
                || claim.content.cause() != Some(authority.cause.clone())
                || (!authority.runtime && claim.content.issuer() != Some(self.peer.principal()))
            {
                Err(AccessError::Unauthorized)
            } else {
                Ok(())
            }
        };
        match &command {
            Command::GenerateClaim { claim } => verify_claim(claim)?,
            Command::GenerateClaimBatch { claims } => {
                for claim in claims {
                    verify_claim(claim)?;
                }
            }
            Command::SupersedeClaim { successor, .. } => verify_claim(successor)?,
            _ => {}
        }
        Ok(AuthenticatedInput {
            ledger: self.request.ledger,
            principal: self.peer.principal(),
            request_epoch: self.request.request_epoch,
            request_id: self.request.request_id,
            expected_revision,
            authority,
            command,
        })
    }
}

pub fn verify_request(
    peer: AuthenticatedPeer,
    request: RequestEnvelope,
    limits: &WireLimits,
) -> Result<VerifiedRequest, AccessError> {
    // Tenant authorization precedes all ledger-specific work and error disclosure.
    if !peer.grant.tenants.contains(&request.ledger.tenant) {
        return Err(AccessError::Unauthorized);
    }
    if request.protocol != PROTOCOL_VERSION {
        return Err(AccessError::UnsupportedProtocol);
    }
    if request.ledger.session.is_zero()
        || request.request_id.is_zero()
        || request.request_epoch.0 == 0
    {
        return Err(AccessError::InvalidRequest);
    }
    let allowed = match capability(&request.operation) {
        Capability::Actor => !matches!(peer.role(), PeerRole::Node { .. }),
        Capability::Runtime => matches!(peer.role(), PeerRole::Runtime),
        Capability::Evaluator => matches!(peer.role(), PeerRole::Runtime | PeerRole::Evaluator),
        Capability::Replication => matches!(peer.role(), PeerRole::Node { .. }),
    };
    if !allowed {
        return Err(AccessError::Unauthorized);
    }
    // Legacy verdicts remain decodable for durable replay. New remote outcomes
    // must carry the receipt fence, so adoption cannot race an evaluator reply.
    if matches!(
        request.operation,
        Operation::Submit {
            command: Command::RecordValidationVerdict { .. },
            ..
        }
    ) {
        return Err(AccessError::UnsupportedOperation);
    }
    let bytes = encode_payload(&request, limits.max_frame_bytes)
        .map_err(|_| AccessError::Capacity)?
        .len() as u64;
    let items = match &request.operation {
        Operation::Read(read) => {
            if read.max_items == 0 || read.max_items > limits.max_items {
                return Err(AccessError::Capacity);
            }
            match &read.consistency {
                ReadConsistency::AtLeast(token) | ReadConsistency::Exact(token)
                    if token.ledger != request.ledger =>
                {
                    return Err(AccessError::Unauthorized);
                }
                _ => {}
            }
            let refs = match &read.query {
                ReadQuery::Objects(objects) => objects.as_slice(),
                ReadQuery::Traverse { roots, depth } => {
                    if *depth > 64 {
                        return Err(AccessError::Capacity);
                    }
                    roots.as_slice()
                }
                ReadQuery::Scan { .. } => &[],
            };
            if refs.len() > limits.max_items as usize {
                return Err(AccessError::Capacity);
            }
            if refs.iter().any(|r| r.ledger != request.ledger) {
                return Err(AccessError::Unauthorized);
            }
            read.max_items as u64
        }
        Operation::Subscribe(sub) => {
            if sub.consumer == [0; 16]
                || sub.credits.items == 0
                || sub.credits.items > limits.max_items
                || sub.credits.bytes == 0
                || sub.credits.bytes > limits.max_frame_bytes
            {
                return Err(AccessError::Capacity);
            }
            if sub
                .after
                .iter()
                .chain(sub.acknowledged.iter())
                .any(|c| c.ledger != request.ledger)
            {
                return Err(AccessError::Unauthorized);
            }
            if sub
                .acknowledged
                .is_some_and(|ack| sub.after.is_none_or(|after| ack > after))
            {
                return Err(AccessError::InvalidRequest);
            }
            sub.credits.items as u64
        }
        Operation::Submit {
            command: Command::GenerateClaimBatch { claims },
            ..
        } => {
            if claims.len() > limits.max_items as usize {
                return Err(AccessError::Capacity);
            }
            claims.len() as u64
        }
        Operation::PeerControl { group, request } => {
            if *group == [0; 16] || request.is_empty() {
                return Err(AccessError::InvalidRequest);
            }
            if request.len() > MAX_PEER_CONTROL_REQUEST_BYTES {
                return Err(AccessError::Capacity);
            }
            1
        }
        Operation::NodeContact {
            group,
            sequence,
            acknowledged_through,
            advertise,
            ..
        } => {
            if *group == [0; 16]
                || *sequence == 0
                || *acknowledged_through >= *sequence
                || advertise.port() == 0
                || advertise.ip().is_unspecified()
                || advertise.ip().is_multicast()
                || matches!(advertise, std::net::SocketAddr::V4(address) if address.ip().is_broadcast())
                || matches!(advertise, std::net::SocketAddr::V6(address) if address.scope_id()!=0 || address.flowinfo()!=0)
            {
                return Err(AccessError::InvalidRequest);
            }
            // Trusted local Node grants have no certificate binding and cannot
            // announce a remotely routable identity.
            if peer.certificate_fingerprint().is_none() {
                return Err(AccessError::Unauthorized);
            }
            1
        }
        Operation::EnrollmentControl {
            group,
            genesis,
            request,
        } => {
            if *group == [0; 16] || *genesis == [0; 32] || request.is_empty() {
                return Err(AccessError::InvalidRequest);
            }
            if request.len() > MAX_ENROLLMENT_CONTROL_REQUEST_BYTES {
                return Err(AccessError::Capacity);
            }
            if peer.certificate_fingerprint().is_none() {
                return Err(AccessError::Unauthorized);
            }
            1
        }
        Operation::Control { group, request } => {
            if *group == [0; 16] || request.is_empty() {
                return Err(AccessError::InvalidRequest);
            }
            1
        }
        Operation::OpenEpoch { epoch } => {
            if *epoch != request.request_epoch {
                return Err(AccessError::InvalidRequest);
            }
            1
        }
        Operation::Stream(stream) => {
            let filter = stream.filter();
            if matches!(filter,DeltaFilter::Claims(claims) if claims.len()>limits.max_items as usize)
            {
                return Err(AccessError::Capacity);
            }
            let credits = stream.credits();
            if credits.items > limits.max_items || credits.bytes > limits.max_frame_bytes {
                return Err(AccessError::Capacity);
            }
            let scope = stream_scope(&peer, request.ledger, filter)?;
            let validate = |token: CursorToken| {
                if token.key.ledger != request.ledger
                    || token.position.ledger != request.ledger
                    || token.scope != scope
                {
                    return Err(AccessError::Unauthorized);
                }
                if token.generation == 0 || token.key.consumer.0 == [0; 16] {
                    return Err(AccessError::InvalidRequest);
                }
                Ok(())
            };
            match stream {
                StreamRequest::Open {
                    consumer, start, ..
                } => {
                    if consumer.0 == [0; 16] {
                        return Err(AccessError::InvalidRequest);
                    }
                    if start.is_some_and(|p| p.ledger != request.ledger) {
                        return Err(AccessError::Unauthorized);
                    }
                }
                StreamRequest::Poll {
                    cursor,
                    acknowledged,
                    ..
                } => {
                    validate(*cursor)?;
                    if let Some(ack) = acknowledged {
                        validate(*ack)?;
                        cursor
                            .same_stream(*ack)
                            .map_err(|_| AccessError::Unauthorized)?;
                        if ack.position > cursor.position {
                            return Err(AccessError::InvalidRequest);
                        }
                    }
                }
                StreamRequest::CompleteSeed {
                    cursor, snapshot, ..
                } => {
                    validate(*cursor)?;
                    if cursor.position != Position::resolved(request.ledger, *snapshot) {
                        return Err(AccessError::InvalidRequest);
                    }
                }
            }
            credits.items as u64
        }
        Operation::Upload(upload) => {
            if upload.upload() == [0; 16] {
                return Err(AccessError::InvalidRequest);
            }
            if let UploadRequest::Append { offset, bytes, .. } = upload
                && (bytes.is_empty() || offset.checked_add(bytes.len() as u64).is_none())
            {
                return Err(AccessError::InvalidRequest);
            }
            // Total content/staging quotas and authorized content domains belong
            // to the server-owned store. The frame cap bounds each append here.
            1
        }
        Operation::Custody(custody) => {
            match custody {
                CustodyRequest::Open {
                    transfer,
                    policy_revision,
                    content,
                    manifest,
                } => {
                    if *transfer == [0; 16] || *policy_revision == 0 || manifest.is_empty() {
                        return Err(AccessError::InvalidRequest);
                    }
                    if content.domain != ContentDomainId(request.ledger.tenant.0) {
                        return Err(AccessError::Unauthorized);
                    }
                }
                CustodyRequest::Verify {
                    policy_revision,
                    content,
                }
                | CustodyRequest::Manifest {
                    policy_revision,
                    content,
                    ..
                } => {
                    if *policy_revision == 0 {
                        return Err(AccessError::InvalidRequest);
                    }
                    if content.domain != ContentDomainId(request.ledger.tenant.0) {
                        return Err(AccessError::Unauthorized);
                    }
                }
                CustodyRequest::Chunk {
                    transfer, bytes, ..
                } => {
                    if *transfer == [0; 16] || bytes.is_empty() {
                        return Err(AccessError::InvalidRequest);
                    }
                }
                CustodyRequest::Seal { transfer }
                | CustodyRequest::Cancel { transfer }
                | CustodyRequest::ReadChunk { transfer, .. } => {
                    if *transfer == [0; 16] {
                        return Err(AccessError::InvalidRequest);
                    }
                }
            }
            if let CustodyRequest::Manifest { max_bytes, .. }
            | CustodyRequest::ReadChunk { max_bytes, .. } = custody
                && (*max_bytes == 0
                    || *max_bytes > limits.max_frame_bytes.saturating_sub(256)
                    || bytes
                        .checked_add(u64::from(*max_bytes))
                        .and_then(|n| n.checked_add(256))
                        .is_none_or(|n| n > limits.max_cost))
            {
                return Err(AccessError::Capacity);
            }
            1
        }
        Operation::Download {
            content,
            offset,
            max_bytes,
        } => {
            if content.domain.is_zero() || *offset > content.length {
                return Err(AccessError::InvalidRequest);
            }
            if *max_bytes == 0 || *max_bytes > limits.max_frame_bytes.saturating_sub(256) {
                return Err(AccessError::Capacity);
            }
            // Reserve output bytes as well as decoded request cost. Content-domain
            // authorization still runs in trusted ingress before opening storage.
            if bytes.saturating_add(*max_bytes as u64).saturating_add(256) > limits.max_cost {
                return Err(AccessError::Capacity);
            }
            1
        }
        _ => 1,
    };
    if bytes.saturating_add(items.saturating_mul(256)) > limits.max_cost {
        return Err(AccessError::Capacity);
    }
    Ok(VerifiedRequest { peer, request })
}

/// Opaque storage identity for an upload. Role changes do not change ownership;
/// principal, full ledger namespace, and client upload ID all participate.
pub fn upload_scope(peer: &AuthenticatedPeer, ledger: LedgerId, upload: [u8; 16]) -> [u8; 16] {
    let mut hash = blake3::Hasher::new_derive_key("focal.content.authenticated-upload.v1");
    hash.update(peer.principal().as_bytes());
    hash.update(ledger.tenant.as_bytes());
    hash.update(ledger.session.as_bytes());
    hash.update(&upload);
    let mut scoped = [0; 16];
    scoped.copy_from_slice(&hash.finalize().as_bytes()[..16]);
    scoped
}

/// Stable authenticated query-scope fence. Clients can carry the returned hash
/// but cannot substitute another principal's token when resuming or acknowledging.
pub fn stream_scope(
    peer: &AuthenticatedPeer,
    ledger: LedgerId,
    filter: &DeltaFilter,
) -> Result<ContentHash, AccessError> {
    let role = match peer.role() {
        PeerRole::Actor => 1u8,
        PeerRole::Evaluator => 2,
        PeerRole::Runtime => 3,
        PeerRole::Node { .. } => 4,
    };
    let bytes = postcard::to_stdvec(&(peer.principal(), role, ledger, filter))
        .map_err(|_| AccessError::InvalidRequest)?;
    let mut hash = blake3::Hasher::new_derive_key("focal.subscription.authenticated-scope.v1");
    hash.update(&bytes);
    Ok(ContentHash(*hash.finalize().as_bytes()))
}
