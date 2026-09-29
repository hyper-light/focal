//! Runs a [`WorkloadShape`] end to end against an in-process node over the
//! embedded transport (the same `focal_client::Client` path the CLI and MCP
//! use), timing each committed request. Local and QUIC transports, reads, and
//! richer metrics are the natural extensions.
use crate::native;
use crate::report::{self, Report};
use crate::shape::WorkloadShape;
use focal_client::{Client, EmbeddedTransport, RetryPolicy};
use focal_ledger::NativeContentProfile;
use focal_model::{ClaimId, RequestEpoch, RequestId, RouteEpoch};
use focal_node::{config::Settings, embedded::EmbeddedNode, host::LocalHost};
use focal_wire::{
    AuthenticatedPeer, NativeClaimExpand, NativeMutationReply, NativeReadQuery, NativeReadRequest,
    Operation, PeerGrant, PeerRole, ReadConsistency, RequestEnvelope, Response, WireLimits,
};
use std::collections::BTreeSet;
use std::fmt;
use std::time::Instant;

/// Why a run stopped before its report: the stage that failed and what it
/// said. The stages are the node's own set-up steps and the tool's own
/// arithmetic, none of which a workload can be blamed for.
#[derive(Debug)]
pub struct RunError {
    stage: &'static str,
    error: String,
}
impl RunError {
    fn at(stage: &'static str, error: impl fmt::Display) -> Self {
        Self {
            stage,
            error: error.to_string(),
        }
    }
    fn overflow(stage: &'static str) -> Self {
        Self::at(stage, "arithmetic overflow")
    }
}
impl fmt::Display for RunError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.stage, self.error)
    }
}
impl From<native::FrameError> for RunError {
    fn from(error: native::FrameError) -> Self {
        Self::at("native frame", error)
    }
}

pub fn run(shape: WorkloadShape) -> Result<Report, RunError> {
    let root = tempfile::tempdir().map_err(|error| RunError::at("temporary directory", error))?;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|error| RunError::at("runtime", error))?;
    let mut settings = Settings::default();
    settings.node.data_dir = Some(root.path().into());
    focal_node::native_activation::activate_local(&settings, NativeContentProfile::ProjectionOnly)
        .map_err(|error| RunError::at("activate native", error))?;

    let node = EmbeddedNode::open(&settings).map_err(|error| RunError::at("open node", error))?;
    let ledger = node.identity.ledger;
    let issuer = node.identity.issuer;
    let worker = node.identity.worker;
    let limits = WireLimits::default();
    let (host, owner) = LocalHost::spawn(node, limits.clone())
        .map_err(|error| RunError::at("spawn host", error))?;
    let peer = AuthenticatedPeer::local(PeerGrant {
        principal: issuer,
        tenants: BTreeSet::from([ledger.tenant]),
        role: PeerRole::Runtime,
    })
    .map_err(|error| RunError::at("local grant", error))?;
    let transport = EmbeddedTransport::new(peer, host.clone(), limits.clone())
        .map_err(|error| RunError::at("embedded transport", error))?;
    let client = Client::new(transport, RetryPolicy::default(), limits, 1)
        .map_err(|error| RunError::at("client", error))?;

    // Distinct id space per seed so runs are reproducible and never collide.
    let base = u128::from(shape.seed) << 40;
    let claims = usize::try_from(shape.claims).map_err(|error| RunError::at("claims", error))?;
    let mut latencies = Vec::new();
    latencies
        .try_reserve_exact(claims)
        .map_err(|error| RunError::at("latencies", error))?;
    let mut created: Vec<u128> = Vec::new();
    created
        .try_reserve_exact(claims)
        .map_err(|error| RunError::at("created claims", error))?;
    let mut committed = 0u64;
    let mut refused = 0u64;
    let mut unknown = 0u64;

    let start = Instant::now();
    for i in 0..shape.claims {
        let request = base
            .checked_add(u128::from(i))
            .and_then(|n| n.checked_add(1))
            .ok_or_else(|| RunError::overflow("request id"))?;
        let claim = base
            .checked_add(1_000_000)
            .and_then(|n| n.checked_add(u128::from(i)))
            .ok_or_else(|| RunError::overflow("claim id"))?;
        let envelope = native::create_envelope(ledger, issuer, worker, request, claim)?;
        let op_start = Instant::now();
        let result = runtime.block_on(client.request(envelope));
        latencies.push(nanos(op_start.elapsed()));
        match result {
            Ok(env) => match env.result {
                Response::Native(NativeMutationReply::Committed(_)) => {
                    committed = committed.saturating_add(1);
                    created.push(claim);
                }
                _ => refused = refused.saturating_add(1),
            },
            Err(_) => unknown = unknown.saturating_add(1),
        }
    }
    let wall = start.elapsed();

    // Read phase: cycle through the committed claims, timing each read.
    let reads = usize::try_from(shape.reads).map_err(|error| RunError::at("reads", error))?;
    let mut read_latencies = Vec::new();
    read_latencies
        .try_reserve_exact(reads)
        .map_err(|error| RunError::at("read latencies", error))?;
    let mut read_hits = 0u64;
    if reads > 0 && !created.is_empty() {
        for i in 0..reads {
            let claim = created
                .get(i.checked_rem(created.len()).unwrap_or(0))
                .copied()
                .ok_or_else(|| RunError::at("read target", "no committed claim"))?;
            let request = base
                .checked_add(2_000_000)
                .and_then(|n| n.checked_add(u128::try_from(i).ok()?))
                .ok_or_else(|| RunError::overflow("read request id"))?;
            let envelope = RequestEnvelope {
                protocol: focal_wire::NATIVE_PROTOCOL_VERSION,
                ledger,
                route_epoch: RouteEpoch(1),
                request_epoch: RequestEpoch(1),
                request_id: RequestId::from_u128(request),
                operation: Operation::NativeRead(NativeReadRequest {
                    consistency: ReadConsistency::Linearizable,
                    query: NativeReadQuery::Claim {
                        id: ClaimId::from_u128(claim),
                        expand: NativeClaimExpand::default(),
                        after: None,
                    },
                    max_items: 1,
                }),
            };
            let op_start = Instant::now();
            let result = runtime.block_on(client.request(envelope));
            read_latencies.push(nanos(op_start.elapsed()));
            if let Ok(env) = result
                && let Response::NativeRead(page) = env.result
                && !page.objects.is_empty()
            {
                read_hits = read_hits.saturating_add(1);
            }
        }
    }
    let read_wall_ns = read_latencies
        .iter()
        .try_fold(0u64, |sum, n| sum.checked_add(*n))
        .ok_or_else(|| RunError::overflow("read wall"))?;

    drop(client);
    drop(host);
    owner
        .join()
        .map_err(|error| RunError::at("host owner", error))?;

    let reads_issued =
        u64::try_from(read_latencies.len()).map_err(|error| RunError::at("reads issued", error))?;
    Ok(Report {
        shape,
        committed,
        refused,
        unknown,
        wall_ms: u64::try_from(wall.as_millis()).unwrap_or(u64::MAX),
        throughput_ops_per_s: report::per_second(committed, nanos(wall)),
        latency_ns: report::latency(latencies),
        reads: reads_issued,
        read_hits,
        read_throughput_ops_per_s: report::per_second(read_hits, read_wall_ns),
        read_latency_ns: report::latency(read_latencies),
    })
}

/// A duration in nanoseconds; one longer than `u64` holds (five centuries)
/// is reported as the ceiling.
fn nanos(duration: std::time::Duration) -> u64 {
    u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX)
}
