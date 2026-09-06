//! Exhaustive inventory of the existing model and transport. WireAvailable
//! means the typed protocol exists, not that this authored catalog exposes it.
use super::Capability;
use focal_model::Command;
use focal_wire::{CustodyRequest, Operation, ReadQuery, StreamRequest, UploadRequest};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Exposure {
    AuthoredTool,
    WireAvailable,
    InternalOnly,
    LegacyReplayOnly,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Coverage {
    pub name: &'static str,
    pub capability: Capability,
    pub exposure: Exposure,
    pub mutation: bool,
    /// Trusted domain/receipt prerequisites; these notes are not standing.
    pub preconditions: &'static str,
    pub result: &'static str,
    pub retry: &'static str,
}
const WRITE_RETRY: &str = "Persist the expanded command and complete request key before send; reuse them after unknown outcomes.";
const READ_RETRY: &str =
    "Preserve fixed-prefix cursor/token; expired views require an explicit new query.";
macro_rules! commands {
    ($( $variant:ident => ($name:literal,$cap:ident,$exposure:ident,$pre:literal,$result:literal)),+ $(,)?)=>{
        pub const COMMAND_INVENTORY:&[Coverage]=&[$(Coverage{name:$name,capability:Capability::$cap,exposure:Exposure::$exposure,mutation:true,preconditions:$pre,result:$result,retry:WRITE_RETRY}),+];
        /// Deliberately exhaustive: appending a model variant requires an
        /// explicit exposure/standing/retry decision here at compile time.
        pub const fn command_coverage(command:&Command)->Coverage {
            match command { $(Command::$variant{..}=>Coverage{name:$name,capability:Capability::$cap,exposure:Exposure::$exposure,mutation:true,preconditions:$pre,result:$result,retry:WRITE_RETRY}),+ }
        }
    }
}
commands! {
    NegotiateEpoch => ("request.epoch.open",Runtime,InternalOnly,"Runtime direct command; actor clients use authenticated OpenEpoch for themselves.","EpochAdmitted"),
    AdvanceEpochFloor => ("request.epoch.advance_floor",Runtime,WireAvailable,"Trusted serialized ownership; never retire another writer's unknown intent.","EpochFloorAdvanced"),
    GenerateClaim => ("claim.submit",Actor,AuthoredTool,"Issuer/cause from trusted ingress; pinned requirements and subject required.","Generated or Existing"),
    GenerateClaimBatch => ("claim.submit_batch",Actor,AuthoredTool,"Each authored claim has valid issuer/cause; bounded atomic batch.","Generated or Existing"),
    PostClaim => ("claim.post",Actor,AuthoredTool,"Existing generated claim and authorized issuer.","Claim"),
    AcquireReceipt => ("receipt.acquire",Actor,AuthoredTool,"Posted claim, designated holder and nonzero epoch.","Receipt"),
    AdoptReceipt => ("receipt.adopt",Runtime,WireAvailable,"Trusted adoption policy and previous receipt fence.","Receipt"),
    RecordProgress => ("claim.progress",Actor,AuthoredTool,"Current receipt holder and exact receipt fence.","Claim"),
    BeginEvidenceSet => ("evidence.begin",Actor,AuthoredTool,"Current receipt holder and exact receipt fence.","EvidenceSet"),
    AttachArtifact => ("artifact.submit",Actor,AuthoredTool,"Open evidence set, receipt fence, actual server custody.","Artifact"),
    CloseTestament => ("testament.submit",Actor,AuthoredTool,"Open evidence set, current receipt and exact immutable manifest.","Testament"),
    AcknowledgeTestament => ("testament.receive",Actor,AuthoredTool,"Protocol 3 requires immutable claim issuer standing and an effective-state revision fence; legacy Runtime path remains.","Claim"),
    BeginWholeWorkValidation => ("validation.begin",Actor,AuthoredTool,"Issuer standing, acknowledged testament and effective-state revision fence; records runs without invoking a tool.","Claim"),
    BeginIncrementValidation => ("validation.begin_increment",Actor,AuthoredTool,"Protocol 3 checks issuer, saved claim revision, actual attached artifact and current evidence manifest.","Validation"),
    RecordValidationVerdict => ("validation.verdict.legacy",Evaluator,LegacyReplayOnly,"Remote legacy unfenced verdicts are rejected; historical log replay only.","Validation"),
    CompleteWholeWork => ("validation.complete",Actor,AuthoredTool,"Issuer standing, revision fence and actual required run completion/graph checks; never caller passing assertion.","Claim"),
    FailPost => ("claim.fail_post",Runtime,WireAvailable,"Trusted post failure with real registered evidence.","Claim"),
    FailReceipt => ("receipt.fail",Runtime,WireAvailable,"Trusted receipt failure with real registered evidence.","Claim"),
    FailTestamentGeneration => ("testament.fail_generation",Runtime,WireAvailable,"Trusted generation failure with durable error artifact.","Testament"),
    CancelClaim => ("claim.cancel",Actor,AuthoredTool,"Authorized claim control and bounded reason.","Claim"),
    RevokeClaim => ("claim.revoke",Runtime,WireAvailable,"Trusted revocation policy and nonempty reason.","Claim"),
    ExpireClaim => ("claim.expire",Runtime,WireAvailable,"Trusted fired timer/generation/logical-time fence.","Claim"),
    SupersedeClaim => ("claim.supersede",Actor,AuthoredTool,"Authorized predecessor and newly pinned successor with explicit immutable lineage.","Generated"),
    RegisterMonitor => ("monitor.register",Actor,AuthoredTool,"Authorized owner claim, bounded wait roots and deadline.","Monitor"),
    RebindMonitor => ("monitor.rebind",Runtime,WireAvailable,"Trusted predecessor/successor transition.","Monitor"),
    ReleaseScope => ("scope.release",Runtime,WireAvailable,"Trusted owned scope completion.","Claim"),
    RegisterArtifact => ("artifact.register",Actor,AuthoredTool,"Protocol 3 permits only authenticated own producer and no execution receipt; actual schema/custody remain mandatory.","Artifact"),
    ExpireMonitor => ("monitor.expire",Runtime,WireAvailable,"Trusted monitor timer/generation/time fence.","Monitor"),
    RecordFencedValidationVerdict => ("validation.submit",Actor,AuthoredTool,"Protocol 3 rechecks designated evaluator, pinned handler/version/attempt/manifest and receipt fence in Core.","Validation")
}
const fn coverage(
    name: &'static str,
    capability: Capability,
    exposure: Exposure,
    mutation: bool,
    preconditions: &'static str,
    result: &'static str,
) -> Coverage {
    Coverage {
        name,
        capability,
        exposure,
        mutation,
        preconditions,
        result,
        retry: if mutation { WRITE_RETRY } else { READ_RETRY },
    }
}
/// The outer administrative RPC is inventoried here. Its opaque control bytes
/// are deliberately not decoded into Runtime tools by an Actor-facing client.
pub fn wire_coverage(operation: &Operation) -> Coverage {
    match operation {
        Operation::Submit { command, .. } => command_coverage(command),
        Operation::Read(read) => query_coverage(&read.query),
        Operation::Reconcile(_) => coverage(
            "request.reconcile",
            Capability::Actor,
            Exposure::AuthoredTool,
            false,
            "Authenticated own principal only; authoritative committed prefix, no epoch mutation.",
            "Epoch status or retained receipt; Unknown and BelowFloor never establish non-commit",
        ),
        Operation::Monitor { .. } => coverage(
            "monitor.get",
            Capability::Actor,
            Exposure::AuthoredTool,
            false,
            "Authorized selected ledger after fresh quorum barrier.",
            "Actual bounded MonitorPage",
        ),
        Operation::Summary => coverage(
            "ledger.summary",
            Capability::Actor,
            Exposure::AuthoredTool,
            false,
            "Fresh quorum prefix in the selected authorized ledger; constant-time committed map counts, no global statistics.",
            "LedgerSummary",
        ),
        Operation::Traverse(_) => coverage(
            "ledger.traverse",
            Capability::Actor,
            Exposure::AuthoredTool,
            false,
            "Exact bound query, principal, lease and route; bounded BFS over canonical indexes.",
            "TraversalPage with explicit cumulative truncation",
        ),
        Operation::List(_) | Operation::Select(_) => coverage(
            "objects.list",
            Capability::Actor,
            Exposure::AuthoredTool,
            false,
            "Selected authorized ledger and fixed filter/prefix cursor.",
            "ListPage",
        ),
        Operation::Validators(_) => coverage(
            "validator.contracts",
            Capability::Actor,
            Exposure::AuthoredTool,
            false,
            "Recorded immutable requirement bindings at a query/principal-bound prefix; no installed worker or execution claim.",
            "ListPage of pinned validation requirements",
        ),
        Operation::Subscribe(_) => coverage(
            "ledger.subscribe.legacy",
            Capability::Actor,
            Exposure::WireAvailable,
            false,
            "Bounded pull and explicit acknowledged cursor.",
            "SubscriptionBatch",
        ),
        Operation::Raft { .. } => coverage(
            "peer.raft",
            Capability::Node,
            Exposure::InternalOnly,
            false,
            "Authenticated current assignment and membership.",
            "PeerAccepted is not commit",
        ),
        Operation::OpenEpoch { .. } => coverage(
            "request.open_epoch",
            Capability::Actor,
            Exposure::WireAvailable,
            true,
            "Admits only authenticated principal's epoch.",
            "MutationReply",
        ),
        Operation::Stream(stream) => stream_coverage(stream),
        Operation::Upload(upload) => upload_coverage(upload),
        Operation::Download { .. } => coverage(
            "artifact.download",
            Capability::Actor,
            Exposure::WireAvailable,
            false,
            "Authorized content domain; bounded verified byte chunks.",
            "ContentChunk",
        ),
        Operation::Control { .. } => coverage(
            "cluster.control",
            Capability::Runtime,
            Exposure::WireAvailable,
            true,
            "Explicit metadata namespace/group plus current Runtime grant; committed decisions only.",
            "ControlReply",
        ),
        Operation::Custody(custody) => custody_coverage(custody),
        Operation::PeerControl { .. } => coverage(
            "peer.control_read",
            Capability::Node,
            Exposure::InternalOnly,
            false,
            "Read-only metadata discovery; no voter grant.",
            "ControlReply",
        ),
        Operation::NodeContact { .. } => coverage(
            "peer.contact",
            Capability::Node,
            Exposure::InternalOnly,
            true,
            "Active enrolled certificate/principal; advertises only itself.",
            "ControlReceipt",
        ),
        Operation::EnrollmentControl { .. } => coverage(
            "peer.enrollment_control",
            Capability::FounderNode,
            Exposure::InternalOnly,
            true,
            "Immutable genesis founder and active enrollment; only dedicated enrollment reads/writes.",
            "ControlReply",
        ),
        Operation::Managed { operation, .. } => {
            let mut item = match operation {
                focal_wire::ManagedOperation::Submit { command, .. } => command_coverage(command),
                focal_wire::ManagedOperation::Cursor(stream) => stream_coverage(stream),
            };
            item.exposure = Exposure::WireAvailable;
            item.retry = "Persist the managed stream, ordinal, independent request ID and exact intent; retired IDs cannot execute again.";
            item
        }
        Operation::RequestStreamControl { .. } => coverage(
            "request.stream.control",
            Capability::Actor,
            Exposure::WireAvailable,
            true,
            "Authenticated own principal; exact registered stream revision and persisted control identity.",
            "RequestStreamControlReceipt",
        ),
        Operation::RequestStreamRead { .. } => coverage(
            "request.stream.read",
            Capability::Actor,
            Exposure::WireAvailable,
            false,
            "Authenticated own principal; fresh authoritative committed prefix.",
            "RequestStreamRead",
        ),
        Operation::ManagedSupport { .. } => coverage(
            "peer.managed_support",
            Capability::Node,
            Exposure::InternalOnly,
            false,
            "Actual session decoder and current published configuration; authenticated peer response binding.",
            "ManagedFormatSupport",
        ),
    }
}
pub const fn query_coverage(query: &ReadQuery) -> Coverage {
    match query {
        ReadQuery::Objects(_) => coverage(
            "objects.get",
            Capability::Actor,
            Exposure::AuthoredTool,
            false,
            "Exact references in selected authorized ledger.",
            "ReadPage",
        ),
        ReadQuery::Scan { .. } => coverage(
            "ledger.scan",
            Capability::Actor,
            Exposure::WireAvailable,
            false,
            "Bounded fixed-prefix ordered object scan.",
            "ReadPage",
        ),
        ReadQuery::SeedScan { .. } => coverage(
            "watch.seed",
            Capability::Actor,
            Exposure::WireAvailable,
            false,
            "Exact snapshot token and bounded claim membership from ByClaim index.",
            "ReadPage including empty-page continuation",
        ),
        ReadQuery::Traverse { .. } => coverage(
            "ledger.traverse",
            Capability::Actor,
            Exposure::WireAvailable,
            false,
            "Bounded depth/visits and authorized roots.",
            "ReadPage",
        ),
        ReadQuery::ValidationResults { .. } => coverage(
            "validation.get",
            Capability::Actor,
            Exposure::AuthoredTool,
            false,
            "Exact requirement and fixed-prefix attempt cursor.",
            "ReadPage with actual run records",
        ),
    }
}
pub const fn stream_coverage(stream: &StreamRequest) -> Coverage {
    let name = match stream {
        StreamRequest::Open { .. } => "stream.open",
        StreamRequest::Poll { .. } => "stream.poll",
        StreamRequest::CompleteSeed { .. } => "stream.complete_seed",
    };
    coverage(
        name,
        Capability::Actor,
        Exposure::WireAvailable,
        true,
        "Authenticated consumer generation; bounded credits; only explicit consumed cursor acknowledgment.",
        "StreamReply",
    )
}
pub const fn upload_coverage(upload: &UploadRequest) -> Coverage {
    let name = match upload {
        UploadRequest::Begin { .. } => "upload.begin",
        UploadRequest::Append { .. } => "upload.append",
        UploadRequest::Seal { .. } => "upload.seal",
        UploadRequest::Cancel { .. } => "upload.cancel",
    };
    coverage(
        name,
        Capability::Actor,
        Exposure::WireAvailable,
        true,
        "Stable scoped upload identity/digest/offset; Seal waits for configured custody gate.",
        "UploadReply",
    )
}
pub const fn custody_coverage(custody: &CustodyRequest) -> Coverage {
    let (name, mutation) = match custody {
        CustodyRequest::Open { .. } => ("custody.open", true),
        CustodyRequest::Chunk { .. } => ("custody.chunk", true),
        CustodyRequest::Seal { .. } => ("custody.seal", true),
        CustodyRequest::Verify { .. } => ("custody.verify", false),
        CustodyRequest::Cancel { .. } => ("custody.cancel", true),
        CustodyRequest::Manifest { .. } => ("custody.manifest", false),
        CustodyRequest::ReadChunk { .. } => ("custody.read_chunk", false),
    };
    coverage(
        name,
        Capability::Node,
        Exposure::InternalOnly,
        mutation,
        "Authenticated assigned peer and exact custody policy/content scope; no aggregate caller assertion.",
        "CustodyReply",
    )
}
