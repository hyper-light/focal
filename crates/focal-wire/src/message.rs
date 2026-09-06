use focal_model::*;
pub use focal_stream::{
    ConsumerId, CursorToken, DeltaFilter, Position, PositionOffset, StreamEvent,
};
use serde::{Deserialize, Serialize};

pub const PROTOCOL_VERSION: u16 = 1;
/// Explicit syntax capability for independently registered request streams.
/// Legacy envelopes and handshake structures retain their original encoding.
pub const MANAGED_PROTOCOL_VERSION: u16 = 2;
pub const ALPN: &[u8] = b"focal/1";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReadToken {
    pub ledger: LedgerId,
    pub sequence: SessionSeq,
    pub route_epoch: RouteEpoch,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct ObjectKey {
    pub kind: ObjectKind,
    pub id: ObjectId,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ReadConsistency {
    Linearizable,
    AtLeast(ReadToken),
    Exact(ReadToken),
    StaleProjection,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ReadQuery {
    Objects(Vec<ObjectRef>),
    Scan {
        after: Option<ObjectKey>,
    },
    Traverse {
        roots: Vec<ObjectRef>,
        depth: u16,
    },
    /// Fixed-prefix requirement and bounded run-summary/verdict records.
    /// A continuation must use Exact with the preceding read token.
    ValidationResults {
        id: ValidationId,
        after: Option<ValidationResultPosition>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReadRequest {
    pub consistency: ReadConsistency,
    pub query: ReadQuery,
    pub max_items: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Credits {
    pub items: u32,
    pub bytes: u32,
}

/// Bounded pull of a durable subscription. `acknowledged` is the last fully
/// consumed cursor; requesting another batch never implicitly acknowledges it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SubscribeRequest {
    pub consumer: [u8; 16],
    pub after: Option<DeltaId>,
    pub acknowledged: Option<DeltaId>,
    pub seed: bool,
    pub credits: Credits,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub enum Operation {
    Submit {
        expected_revision: Option<ObjectRevision>,
        command: Command,
    },
    Read(ReadRequest),
    Subscribe(SubscribeRequest),
    /// Consensus payload is interpreted only by authenticated node ingress.
    Raft {
        group: [u8; 16],
        message: Vec<u8>,
    },
    /// Allocate/admit only the authenticated principal's requested epoch.
    OpenEpoch {
        epoch: RequestEpoch,
    },
    Stream(StreamRequest),
    Upload(UploadRequest),
    Download {
        content: ContentRef,
        offset: u64,
        max_bytes: u32,
    },
    /// Bounded metadata RPC; interpreted only by the authenticated control owner.
    Control {
        group: [u8; 16],
        request: Vec<u8>,
    },
    /// Node-only content custody protocol. A durable reply attests one disk,
    /// never an aggregate session placement guarantee.
    Custody(CustodyRequest),
    /// Authenticated node discovery; the control owner accepts read RPCs only.
    PeerControl {
        group: [u8; 16],
        request: Vec<u8>,
    },
    /// Announce only this authenticated Node's reachable endpoint. The root
    /// binds identity and certificate from TLS; this grants no membership.
    NodeContact {
        group: [u8; 16],
        sequence: u64,
        acknowledged_through: u64,
        expected_generation: u64,
        advertise: std::net::SocketAddr,
    },
    /// Only the immutable genesis founder may use the enrollment owner's
    /// dedicated sequence stream. The root reauthorizes the certificate and
    /// permits enrollment commands/state reads only; Node is never Runtime.
    EnrollmentControl {
        group: [u8; 16],
        genesis: [u8; 32],
        request: Vec<u8>,
    },
    List(crate::ListRequest),
    /// Read only this authenticated principal's epoch or retained request
    /// receipt at an authoritative committed prefix. Absence is not abort proof.
    Reconcile(ReconcileQuery),
    Managed {
        key: ManagedRequestKey,
        operation: crate::ManagedOperation,
    },
    RequestStreamControl {
        cluster: [u8; 16],
        command: RequestStreamCommand,
    },
    RequestStreamRead {
        cluster: [u8; 16],
        query: RequestStreamQuery,
    },
    /// The authenticated node reports its actual installed decoder and current
    /// applied membership. This is never an aggregate activation certificate.
    ManagedSupport {
        group: [u8; 16],
    },
}
/// Read-only metadata selectors contain no variable-length collections.
pub const MAX_PEER_CONTROL_REQUEST_BYTES: usize = 64;
pub const MAX_ENROLLMENT_CONTROL_REQUEST_BYTES: usize = 128 * 1024;
impl Operation {
    /// IDs are registered protocol values, independent of Rust enum layout.
    pub fn registered_tag(&self) -> u16 {
        match self {
            Self::Submit { .. } => 1,
            Self::Read(_) => 2,
            Self::Subscribe(_) => 3,
            Self::Raft { .. } => 4,
            Self::OpenEpoch { .. } => 5,
            Self::Stream(_) => 6,
            Self::Upload(_) => 7,
            Self::Download { .. } => 8,
            Self::Control { .. } => 9,
            Self::Custody(_) => 10,
            Self::PeerControl { .. } => 11,
            Self::NodeContact { .. } => 12,
            Self::EnrollmentControl { .. } => 13,
            Self::List(_) => 14,
            Self::Reconcile(_) => 15,
            Self::Managed { .. } => 16,
            Self::RequestStreamControl { .. } => 17,
            Self::RequestStreamRead { .. } => 18,
            Self::ManagedSupport { .. } => 19,
        }
    }
    pub fn is_mutation(&self) -> bool {
        matches!(
            self,
            Self::Submit { .. }
                | Self::OpenEpoch { .. }
                | Self::Stream(_)
                | Self::Upload(_)
                | Self::Control { .. }
                | Self::Custody(_)
                | Self::NodeContact { .. }
                | Self::EnrollmentControl { .. }
                | Self::Managed { .. }
                | Self::RequestStreamControl { .. }
        )
    }
}

/// No actor, cause, runtime permission, policy time, or evidence attestation is
/// deserializable here. Ingress derives those from authenticated server state.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RequestEnvelope {
    pub protocol: u16,
    pub ledger: LedgerId,
    pub route_epoch: RouteEpoch,
    pub request_epoch: RequestEpoch,
    pub request_id: RequestId,
    pub operation: Operation,
}
impl RequestEnvelope {
    pub fn reply(&self, result: Response) -> ResponseEnvelope {
        ResponseEnvelope {
            protocol: self.protocol,
            ledger: self.ledger,
            route_epoch: self.route_epoch,
            request_epoch: self.request_epoch,
            request_id: self.request_id,
            result,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum MutationReply {
    Committed(MutationReceipt),
    Domain(DomainOutcome),
    Yield {
        monitor: MonitorId,
        observed: ReadToken,
    },
    /// Accepted but not known committed; this is never reported as success.
    Pending(RequestKey),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ReadObject {
    Claim {
        id: ClaimId,
        value: Claim,
    },
    Testament {
        id: TestamentId,
        value: Testament,
    },
    Validation {
        id: ValidationId,
        value: Validation,
    },
    Artifact {
        id: ArtifactId,
        value: Artifact,
    },
    ValidationResults {
        id: ValidationId,
        value: Validation,
        records: Vec<ValidationResult>,
        next: Option<ValidationResultPosition>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReadPage {
    pub token: ReadToken,
    pub objects: Vec<ReadObject>,
    pub next: Option<ObjectKey>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SubscriptionBatch {
    pub token: ReadToken,
    pub seed: Option<ReadPage>,
    pub deltas: Vec<Delta>,
    pub next: Option<DeltaId>,
    pub caught_up: bool,
}

/// Durable stream operations carry the complete cursor fence. Scope, clock,
/// lease expiry, and cursor registry revision are supplied by trusted ingress.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub enum StreamRequest {
    Open {
        consumer: ConsumerId,
        filter: DeltaFilter,
        start: Option<Position>,
        seed: bool,
        credits: Credits,
    },
    Poll {
        cursor: CursorToken,
        filter: DeltaFilter,
        acknowledged: Option<CursorToken>,
        credits: Credits,
    },
    CompleteSeed {
        cursor: CursorToken,
        filter: DeltaFilter,
        snapshot: SessionSeq,
    },
}
impl StreamRequest {
    pub fn filter(&self) -> &DeltaFilter {
        match self {
            Self::Open { filter, .. }
            | Self::Poll { filter, .. }
            | Self::CompleteSeed { filter, .. } => filter,
        }
    }
    pub fn credits(&self) -> Credits {
        match self {
            Self::Open { credits, .. } | Self::Poll { credits, .. } => *credits,
            Self::CompleteSeed { .. } => Credits { items: 0, bytes: 0 },
        }
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StreamReply {
    pub token: ReadToken,
    pub cursor: CursorToken,
    pub acknowledged: CursorToken,
    pub seed: Option<ReadPage>,
    pub events: Vec<StreamEvent>,
}

/// Resumable immutable content transfer. Ingress derives the storage upload ID
/// from the authenticated principal and ledger; this ID alone grants no access.
/// `digest` commits to the complete byte stream, not its chunk manifest root.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub enum UploadRequest {
    Begin {
        upload: [u8; 16],
        length: u64,
        digest: ContentHash,
        class: ContentClass,
    },
    Append {
        upload: [u8; 16],
        offset: u64,
        bytes: Vec<u8>,
    },
    Seal {
        upload: [u8; 16],
    },
    Cancel {
        upload: [u8; 16],
    },
}
impl UploadRequest {
    pub fn upload(&self) -> [u8; 16] {
        match self {
            Self::Begin { upload, .. }
            | Self::Append { upload, .. }
            | Self::Seal { upload }
            | Self::Cancel { upload } => *upload,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum UploadReply {
    /// Durable received prefix. An identical append retry may return a later
    /// prefix when another already acknowledged chunk followed it.
    Offset(u64),
    /// Immutable content reference after the server's configured custody gate.
    Sealed(ContentRef),
    Cancelled,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub enum CustodyRequest {
    Open {
        transfer: [u8; 16],
        policy_revision: u64,
        content: ContentRef,
        manifest: Vec<u8>,
    },
    Chunk {
        transfer: [u8; 16],
        index: u32,
        bytes: Vec<u8>,
    },
    Seal {
        transfer: [u8; 16],
    },
    Verify {
        policy_revision: u64,
        content: ContentRef,
    },
    Cancel {
        transfer: [u8; 16],
    },
    Manifest {
        policy_revision: u64,
        content: ContentRef,
        max_bytes: u32,
    },
    ReadChunk {
        transfer: [u8; 16],
        index: u32,
        max_bytes: u32,
    },
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum CustodyReply {
    Opened {
        chunks: u32,
        next_missing: u32,
    },
    ChunkStored {
        index: u32,
    },
    Durable {
        policy_revision: u64,
        content: ContentRef,
    },
    Cancelled,
    Manifest {
        content: ContentRef,
        manifest: Vec<u8>,
    },
    Chunk {
        index: u32,
        bytes: Vec<u8>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContentChunk {
    pub offset: u64,
    pub bytes: Vec<u8>,
    pub eof: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RouteHint {
    pub epoch: RouteEpoch,
    pub endpoint: String,
    pub server_name: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum AccessError {
    Unauthorized,
    UnsupportedProtocol,
    InvalidRequest,
    Capacity,
    Unavailable,
    OutcomeUnknown,
    RouteChanged(RouteHint),
    Behind { published: SessionSeq },
    SnapshotExpired,
    ResyncRequired { floor: Option<DeltaId> },
    UnsupportedOperation,
    ManagedRetired { through: u64 },
    ManagedClosed { generation: u64 },
    ManagedConflict,
    ManagedNotRegistered,
}
impl AccessError {
    pub fn registered_tag(&self) -> u16 {
        match self {
            Self::Unauthorized => 1,
            Self::UnsupportedProtocol => 2,
            Self::InvalidRequest => 3,
            Self::Capacity => 4,
            Self::Unavailable => 5,
            Self::OutcomeUnknown => 6,
            Self::RouteChanged(_) => 7,
            Self::Behind { .. } => 8,
            Self::SnapshotExpired => 9,
            Self::ResyncRequired { .. } => 10,
            Self::UnsupportedOperation => 11,
            Self::ManagedRetired { .. } => 12,
            Self::ManagedClosed { .. } => 13,
            Self::ManagedConflict => 14,
            Self::ManagedNotRegistered => 15,
        }
    }
}
impl std::fmt::Display for AccessError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "protocol access failure {}", self.registered_tag())
    }
}
impl std::error::Error for AccessError {}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
// A bounded per-request envelope; keeping its fixed cursor fences inline avoids
// another heap allocation outside the frame/connection reservation.
#[allow(clippy::large_enum_variant)]
pub enum Response {
    Submitted(MutationReply),
    Read(ReadPage),
    Subscription(SubscriptionBatch),
    PeerAccepted,
    Error(AccessError),
    Stream(StreamReply),
    Upload(UploadReply),
    Content(ContentChunk),
    Control { response: Vec<u8> },
    Custody(CustodyReply),
    Listed(crate::ListPage),
    Reconciled(crate::ReconcileReply),
    Managed(crate::ManagedReply),
    RequestStreamControlled(crate::RequestStreamControlReply),
    RequestStreamRead(crate::RequestStreamReadReply),
    ManagedSupport(ManagedFormatSupport),
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResponseEnvelope {
    pub protocol: u16,
    pub ledger: LedgerId,
    pub route_epoch: RouteEpoch,
    pub request_epoch: RequestEpoch,
    pub request_id: RequestId,
    pub result: Response,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Hello {
    pub versions: Vec<u16>,
    pub max_frame_bytes: u32,
    pub max_items: u32,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Negotiated {
    pub protocol: u16,
    pub max_frame_bytes: u32,
    pub max_items: u32,
}
impl Negotiated {
    pub fn accepts_protocol(self, requested: u16) -> bool {
        matches!(
            (self.protocol, requested),
            (PROTOCOL_VERSION, PROTOCOL_VERSION)
                | (
                    MANAGED_PROTOCOL_VERSION,
                    PROTOCOL_VERSION | MANAGED_PROTOCOL_VERSION
                )
        )
    }
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum HelloReply {
    Accepted(Negotiated),
    Rejected(AccessError),
}

#[derive(Debug, Clone)]
pub struct WireLimits {
    pub max_frame_bytes: u32,
    pub max_items: u32,
    pub max_cost: u64,
    pub max_connections: usize,
    pub streams_per_connection: u32,
    pub request_timeout: std::time::Duration,
}
impl Default for WireLimits {
    fn default() -> Self {
        Self {
            max_frame_bytes: 1024 * 1024,
            max_items: 1024,
            max_cost: 4 * 1024 * 1024,
            max_connections: 128,
            streams_per_connection: 16,
            request_timeout: std::time::Duration::from_secs(30),
        }
    }
}
impl WireLimits {
    pub fn validate(&self) -> Result<(), AccessError> {
        if !(1024..=16 * 1024 * 1024).contains(&self.max_frame_bytes)
            || self.max_items == 0
            || self.max_items > 65536
            || self.max_cost < self.max_frame_bytes as u64
            || self.max_connections == 0
            || self.max_connections > 65536
            || self.streams_per_connection == 0
            || self.streams_per_connection > 1024
            || self.request_timeout.is_zero()
            || self.request_timeout > std::time::Duration::from_secs(120)
        {
            return Err(AccessError::Capacity);
        }
        Ok(())
    }
    pub fn negotiate(&self, hello: &Hello) -> Result<Negotiated, AccessError> {
        self.negotiate_managed(hello, false)
    }
    /// Advertising syntax support grants no managed-log activation authority.
    /// The session owner separately verifies its current voters' replay support.
    pub fn negotiate_managed(
        &self,
        hello: &Hello,
        managed: bool,
    ) -> Result<Negotiated, AccessError> {
        if hello.versions.len() > 16 {
            return Err(AccessError::UnsupportedProtocol);
        }
        let protocol = if managed && hello.versions.contains(&MANAGED_PROTOCOL_VERSION) {
            MANAGED_PROTOCOL_VERSION
        } else if hello.versions.contains(&PROTOCOL_VERSION) {
            PROTOCOL_VERSION
        } else {
            return Err(AccessError::UnsupportedProtocol);
        };
        if hello.max_frame_bytes < 1024 || hello.max_items == 0 {
            return Err(AccessError::InvalidRequest);
        }
        Ok(Negotiated {
            protocol,
            max_frame_bytes: self.max_frame_bytes.min(hello.max_frame_bytes),
            max_items: self.max_items.min(hello.max_items),
        })
    }
}
