use crate::*;
/// The longest failure-domain label a node may announce (24 §22).
pub const MAX_TOPOLOGY_LABEL_BYTES: usize = 64;
/// The longest advertised name (`host:port`) a contact carries (24 §24):
/// a DNS name of at most 253 bytes, a colon and a port.
pub const MAX_ENDPOINT_NAME_BYTES: usize = 259;
/// An advertised name is `host:port` where the host is a DNS name (never an
/// address literal, which needs no resolution) and the port is nonzero.
pub fn valid_endpoint_name(name: &str) -> bool {
    if name.len() > MAX_ENDPOINT_NAME_BYTES {
        return false;
    }
    let Some((host, port)) = name.rsplit_once(':') else {
        return false;
    };
    if host.is_empty()
        || host.contains(':')
        || host.parse::<std::net::IpAddr>().is_ok()
        || !port.parse::<u16>().is_ok_and(|port| port != 0)
    {
        return false;
    }
    matches!(
        rustls::pki_types::ServerName::try_from(host.to_owned()),
        Ok(rustls::pki_types::ServerName::DnsName(_))
    )
}

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
    /// A node peer is infrastructure: it replicates, hosts and drives the
    /// sessions the directory assigns it across every tenant, and every
    /// operation its role may issue is validated against committed placement
    /// and enrollment facts, so its grant's tenants do not scope it. A
    /// client's grant does.
    pub fn permits_tenant(&self, tenant: TenantId) -> bool {
        matches!(self.grant.role, PeerRole::Node { .. }) || self.grant.tenants.contains(&tenant)
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
        Operation::ManagedSupport { .. }
        | Operation::Raft { .. }
        | Operation::EnrollmentControl { .. }
        | Operation::Custody(_)
        | Operation::PeerControl { .. }
        | Operation::PlacementControl { .. }
        | Operation::SessionSign { .. }
        | Operation::RangeControl { .. }
        | Operation::SessionControl { .. }
        | Operation::Probe { .. }
        | Operation::NodeContact { .. } => Capability::Replication,
        Operation::Control { .. } => Capability::Runtime,
        // Native frames carry the authenticated principal; timers never arrive
        // here, and the owner proves every role from its committed prefix.
        Operation::Native { .. } | Operation::NativeRead(_) | Operation::NativeList(_) => {
            Capability::Actor
        }
        Operation::Submit { command, .. }
        | Operation::Managed {
            operation: ManagedOperation::Submit { command, .. },
            ..
        } => match command {
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
        verify_authored_claims(
            &command,
            self.request.ledger,
            self.peer.principal(),
            &authority,
        )?;
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
    pub fn into_managed(
        self,
        mut authority: AuthorityContext,
    ) -> Result<ManagedAuthenticatedInput, AccessError> {
        let Operation::Managed {
            key,
            operation:
                ManagedOperation::Submit {
                    expected_revision,
                    command,
                },
        } = self.request.operation
        else {
            return Err(AccessError::InvalidRequest);
        };
        authority.runtime = matches!(self.peer.role(), PeerRole::Runtime);
        verify_authored_claims(
            &command,
            self.request.ledger,
            self.peer.principal(),
            &authority,
        )?;
        Ok(ManagedAuthenticatedInput {
            key,
            expected_revision,
            authority,
            command,
        })
    }
    pub fn into_request_stream_control(self) -> Result<RequestStreamControlInput, AccessError> {
        let Operation::RequestStreamControl { cluster, command } = self.request.operation else {
            return Err(AccessError::InvalidRequest);
        };
        Ok(RequestStreamControlInput {
            cluster,
            ledger: self.request.ledger,
            principal: self.peer.principal(),
            id: self.request.request_id,
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
    if !peer.permits_tenant(request.ledger.tenant) {
        return Err(AccessError::Unauthorized);
    }
    let managed = matches!(
        request.operation,
        Operation::Managed { .. }
            | Operation::RequestStreamControl { .. }
            | Operation::RequestStreamRead { .. }
            | Operation::ManagedSupport { .. }
    );
    let participant = is_peer_request(&request);
    let native = request.protocol == crate::NATIVE_PROTOCOL_VERSION;
    if native {
        if !crate::native_profile_operation(&request.operation) {
            return Err(AccessError::UnsupportedProtocol);
        }
    } else if crate::is_native_operation(&request.operation)
        || (!participant
            && ((managed && request.protocol != MANAGED_PROTOCOL_VERSION)
                || (!managed && request.protocol != PROTOCOL_VERSION)))
    {
        return Err(AccessError::UnsupportedProtocol);
    }
    if request.ledger.session.is_zero()
        || request.request_id.is_zero()
        || request.request_epoch.0 == 0
    {
        return Err(AccessError::InvalidRequest);
    }
    let allowed = if participant {
        !matches!(peer.role(), PeerRole::Node { .. })
    } else {
        match capability(&request.operation) {
            Capability::Actor => !matches!(peer.role(), PeerRole::Node { .. }),
            Capability::Runtime => matches!(peer.role(), PeerRole::Runtime),
            Capability::Evaluator => matches!(peer.role(), PeerRole::Runtime | PeerRole::Evaluator),
            Capability::Replication => matches!(peer.role(), PeerRole::Node { .. }),
        }
    };
    if !allowed {
        return Err(AccessError::Unauthorized);
    }
    if matches!(
        request.operation,
        Operation::Reconcile(_)
            | Operation::RequestStreamControl { .. }
            | Operation::RequestStreamRead { .. }
            | Operation::Managed {
                operation: ManagedOperation::Cursor(_),
                ..
            }
    ) && !matches!(peer.role(), PeerRole::Actor | PeerRole::Runtime)
    {
        return Err(AccessError::Unauthorized);
    }
    // Legacy verdicts remain decodable for durable replay. New remote outcomes
    // must carry the receipt fence, so adoption cannot race an evaluator reply.
    if matches!(
        request.operation,
        Operation::Submit {
            command: Command::RecordValidationVerdict { .. },
            ..
        } | Operation::Managed {
            operation: ManagedOperation::Submit {
                command: Command::RecordValidationVerdict { .. },
                ..
            },
            ..
        }
    ) {
        return Err(AccessError::UnsupportedOperation);
    }
    if managed && (request.request_epoch != RequestEpoch(1) || request.route_epoch.0 == 0) {
        return Err(AccessError::InvalidRequest);
    }
    if let Operation::Managed { key, .. } = &request.operation {
        managed_request_identity(&request).map_err(|error| match error {
            WireError::Access(error) => error,
            _ => AccessError::InvalidRequest,
        })?;
        if key.stream.principal != peer.principal() {
            return Err(AccessError::Unauthorized);
        }
    }
    request_shape(&request, limits, Some(&peer))?;
    Ok(VerifiedRequest { peer, request })
}

/// Local syntax and resource validation only. This neither authenticates a
/// caller nor authorizes an operation, validates committed state, or negotiates
/// a server capability. Managed ownership/control envelopes require their
/// dedicated authenticated workflow and are deliberately unsupported here.
pub fn check_request_shape(
    request: &RequestEnvelope,
    limits: &WireLimits,
) -> Result<(), AccessError> {
    limits.validate()?;
    if matches!(
        request.operation,
        Operation::Control { .. }
            | Operation::PeerControl { .. }
            | Operation::PlacementControl { .. }
            | Operation::SessionSign { .. }
            | Operation::RangeControl { .. }
            | Operation::SessionControl { .. }
            | Operation::Probe { .. }
            | Operation::NodeContact { .. }
            | Operation::EnrollmentControl { .. }
            | Operation::Raft { .. }
            | Operation::Custody(_)
            | Operation::Managed { .. }
            | Operation::RequestStreamControl { .. }
            | Operation::RequestStreamRead { .. }
            | Operation::ManagedSupport { .. }
    ) {
        return Err(AccessError::UnsupportedOperation);
    }
    if !is_peer_request(request) && request.protocol != PROTOCOL_VERSION {
        return Err(AccessError::UnsupportedProtocol);
    }
    if request.ledger.tenant.is_zero()
        || request.ledger.session.is_zero()
        || request.request_id.is_zero()
        || request.request_epoch.0 == 0
        || request.route_epoch.0 == 0
    {
        return Err(AccessError::InvalidRequest);
    }
    if matches!(
        &request.operation,
        Operation::Submit {
            command: Command::RecordValidationVerdict { .. },
            ..
        }
    ) {
        return Err(AccessError::UnsupportedOperation);
    }
    request_shape(request, limits, None)
}

fn request_shape(
    request: &RequestEnvelope,
    limits: &WireLimits,
    peer: Option<&AuthenticatedPeer>,
) -> Result<(), AccessError> {
    let bytes = crate::frame::payload_len(request, limits.max_frame_bytes)
        .map_err(|_| AccessError::Capacity)? as u64;
    let items = match &request.operation {
        Operation::RequestStreamControl { cluster, command } => {
            let principal = peer.ok_or(AccessError::UnsupportedOperation)?.principal();
            crate::managed::validate_control_request(
                *cluster,
                request.ledger,
                principal,
                command,
                limits.max_items,
            )?
        }
        Operation::RequestStreamRead { cluster, query } => {
            if *cluster == [0; 16] {
                return Err(AccessError::InvalidRequest);
            }
            if let RequestStreamQuery::Receipt { key } = query {
                if !key.is_valid() {
                    return Err(AccessError::InvalidRequest);
                }
                if key.stream.cluster != *cluster
                    || key.stream.ledger != request.ledger
                    || peer.is_some_and(|peer| key.stream.principal != peer.principal())
                {
                    return Err(AccessError::Unauthorized);
                }
            }
            1
        }
        Operation::ManagedSupport { group } => {
            if *group == [0; 16] {
                return Err(AccessError::InvalidRequest);
            }
            1
        }
        Operation::Reconcile(query) => {
            let epoch = match query {
                ReconcileQuery::Epoch { epoch } => epoch,
                ReconcileQuery::Receipt { epoch, request } => {
                    if request.is_zero() {
                        return Err(AccessError::InvalidRequest);
                    }
                    epoch
                }
            };
            if epoch.0 == 0 || request.route_epoch.0 == 0 {
                return Err(AccessError::InvalidRequest);
            }
            1
        }
        Operation::List(list) => {
            list.filter.validate()?;
            if list.max_items == 0
                || list.max_items > limits.max_items
                || list.max_visits == 0
                || list.max_visits > limits.max_items
                || list.cursor.as_ref().is_some_and(|cursor| {
                    cursor.bytes.is_empty() || cursor.bytes.len() > MAX_LIST_CURSOR_BYTES
                })
            {
                return Err(AccessError::Capacity);
            }
            u64::from(list.max_visits.max(list.max_items))
        }
        Operation::Traverse(query) => {
            query.validate(request.ledger, limits)?;
            u64::from(query.max_visits.max(query.max_items))
        }
        Operation::Monitor { id } => {
            if id.is_zero() || request.route_epoch.0 == 0 {
                return Err(AccessError::InvalidRequest);
            }
            MAX_MONITOR_ROOTS as u64
        }
        Operation::Summary => {
            if request.route_epoch.0 == 0 {
                return Err(AccessError::InvalidRequest);
            }
            1
        }
        Operation::Select(query) => {
            query.validate(request.ledger, limits)?;
            u64::from(query.query.max_visits.max(query.query.max_items))
        }
        Operation::Validators(query) => {
            query.validate(limits)?;
            u64::from(query.query.max_visits.max(query.query.max_items))
        }
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
                ReadQuery::SeedScan {
                    claims,
                    after,
                    max_bytes,
                } => {
                    if *max_bytes < 1024
                        || *max_bytes > 65536
                        || *max_bytes > limits.max_frame_bytes
                        || claims.len() > 256
                        || claims.iter().any(|id| id.is_zero())
                        || claims.windows(2).any(|pair| matches!(pair,[a,b] if a>=b))
                        || (after.is_some()
                            && !matches!(read.consistency, ReadConsistency::Exact(_)))
                    {
                        return Err(AccessError::InvalidRequest);
                    }
                    &[]
                }
                ReadQuery::ValidationResults { id, after } => {
                    if id.is_zero()
                        || after.is_some_and(|position| position.run.validation != *id)
                        || (after.is_some()
                            && !matches!(read.consistency, ReadConsistency::Exact(_)))
                    {
                        return Err(AccessError::InvalidRequest);
                    }
                    &[]
                }
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
        }
        | Operation::Managed {
            operation:
                ManagedOperation::Submit {
                    command: Command::GenerateClaimBatch { claims },
                    ..
                },
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
        Operation::PlacementControl { group, request } => {
            if *group == [0; 16] || request.is_empty() {
                return Err(AccessError::InvalidRequest);
            }
            if request.len() > MAX_PLACEMENT_CONTROL_REQUEST_BYTES {
                return Err(AccessError::Capacity);
            }
            // A node's own facts are signed with its certificate; a trusted
            // local Node grant has none and cannot speak for a remote identity.
            if peer.is_some_and(|peer| peer.certificate_fingerprint().is_none()) {
                return Err(AccessError::Unauthorized);
            }
            1
        }
        Operation::SessionSign { group, request } => {
            if *group == [0; 16] || request.is_empty() {
                return Err(AccessError::InvalidRequest);
            }
            if request.len() > MAX_SESSION_SIGN_REQUEST_BYTES {
                return Err(AccessError::Capacity);
            }
            if peer.is_some_and(|peer| peer.certificate_fingerprint().is_none()) {
                return Err(AccessError::Unauthorized);
            }
            1
        }
        Operation::RangeControl { group, request } => {
            if *group == [0; 16] || request.is_empty() {
                return Err(AccessError::InvalidRequest);
            }
            if request.len() > MAX_RANGE_CONTROL_REQUEST_BYTES {
                return Err(AccessError::Capacity);
            }
            if peer.is_some_and(|peer| peer.certificate_fingerprint().is_none()) {
                return Err(AccessError::Unauthorized);
            }
            1
        }
        Operation::SessionControl { group, request } => {
            if *group == [0; 16] || request.is_empty() {
                return Err(AccessError::InvalidRequest);
            }
            if request.len() > MAX_SESSION_CONTROL_REQUEST_BYTES {
                return Err(AccessError::Capacity);
            }
            if peer.is_some_and(|peer| peer.certificate_fingerprint().is_none()) {
                return Err(AccessError::Unauthorized);
            }
            1
        }
        Operation::Probe { request } => {
            if request.is_empty() {
                return Err(AccessError::InvalidRequest);
            }
            if request.len() > MAX_PROBE_BYTES {
                return Err(AccessError::Capacity);
            }
            // A probe speaks for a remote node identity: certificate-bound.
            if peer.is_some_and(|peer| peer.certificate_fingerprint().is_none()) {
                return Err(AccessError::Unauthorized);
            }
            1
        }
        Operation::NodeContact {
            group,
            sequence,
            acknowledged_through,
            advertise,
            region,
            zone,
            endpoint,
            ..
        } => {
            let bad_label = |label: &Option<String>| {
                label
                    .as_ref()
                    .is_some_and(|label| label.is_empty() || label.len() > MAX_TOPOLOGY_LABEL_BYTES)
            };
            if *group == [0; 16]
                || *sequence == 0
                || *acknowledged_through >= *sequence
                || bad_label(region)
                || bad_label(zone)
                || (zone.is_some() && region.is_none())
                || endpoint
                    .as_deref()
                    .is_some_and(|name| !valid_endpoint_name(name))
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
            if peer.is_some_and(|peer| peer.certificate_fingerprint().is_none()) {
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
            if peer.is_some_and(|peer| peer.certificate_fingerprint().is_none()) {
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
        Operation::Stream(stream)
        | Operation::Managed {
            operation: ManagedOperation::Cursor(stream),
            ..
        } => {
            let filter = stream.filter();
            if matches!(filter,DeltaFilter::Claims(claims) if claims.len()>limits.max_items as usize)
            {
                return Err(AccessError::Capacity);
            }
            let credits = stream.credits();
            if credits.items > limits.max_items || credits.bytes > limits.max_frame_bytes {
                return Err(AccessError::Capacity);
            }
            let scope = peer
                .map(|peer| stream_scope(peer, request.ledger, filter))
                .transpose()?;
            let validate = |token: CursorToken| {
                if token.key.ledger != request.ledger
                    || token.position.ledger != request.ledger
                    || scope.is_some_and(|scope| token.scope != scope)
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
                CustodyRequest::SeedChunk { hash, .. } => {
                    if hash.0 == [0; 32] {
                        return Err(AccessError::InvalidRequest);
                    }
                }
            }
            if let CustodyRequest::Manifest { max_bytes, .. }
            | CustodyRequest::ReadChunk { max_bytes, .. }
            | CustodyRequest::SeedChunk { max_bytes, .. } = custody
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
    Ok(())
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

fn verify_authored_claims(
    command: &Command,
    ledger: LedgerId,
    principal: ParticipantId,
    authority: &AuthorityContext,
) -> Result<(), AccessError> {
    let verify_claim = |claim: &NewClaim| {
        if claim.content.ledger != ledger
            || claim.content.cause() != Some(authority.cause.clone())
            || (!authority.runtime && claim.content.issuer() != Some(principal))
        {
            Err(AccessError::Unauthorized)
        } else {
            Ok(())
        }
    };
    match command {
        Command::GenerateClaim { claim } => verify_claim(claim)?,
        Command::GenerateClaimBatch { claims } => {
            for claim in claims {
                verify_claim(claim)?;
            }
        }
        Command::SupersedeClaim { successor, .. } => verify_claim(successor)?,
        _ => {}
    }
    Ok(())
}

#[cfg(test)]
#[path = "auth_shape_tests.rs"]
mod shape_tests;
