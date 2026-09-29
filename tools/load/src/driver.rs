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
use std::time::Instant;

pub fn run(shape: WorkloadShape) -> Report {
    let root = tempfile::tempdir().unwrap();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let mut settings = Settings::default();
    settings.node.data_dir = Some(root.path().into());
    focal_node::native_activation::activate_local(&settings, NativeContentProfile::ProjectionOnly)
        .unwrap();

    let node = EmbeddedNode::open(&settings).unwrap();
    let ledger = node.identity.ledger;
    let issuer = node.identity.issuer;
    let worker = node.identity.worker;
    let limits = WireLimits::default();
    let (host, owner) = LocalHost::spawn(node, limits.clone()).unwrap();
    let peer = AuthenticatedPeer::local(PeerGrant {
        principal: issuer,
        tenants: BTreeSet::from([ledger.tenant]),
        role: PeerRole::Runtime,
    })
    .unwrap();
    let client = Client::new(
        EmbeddedTransport::new(peer, host.clone(), limits.clone()).unwrap(),
        RetryPolicy::default(),
        limits,
        1,
    )
    .unwrap();

    // Distinct id space per seed so runs are reproducible and never collide.
    let base = u128::from(shape.seed) << 40;
    let mut latencies = Vec::with_capacity(shape.claims as usize);
    let mut committed = 0u64;
    let mut refused = 0u64;
    let mut unknown = 0u64;
    let mut created: Vec<u128> = Vec::with_capacity(shape.claims as usize);

    let start = Instant::now();
    for i in 0..shape.claims {
        let request = base + u128::from(i) + 1;
        let claim = base + 1_000_000 + u128::from(i);
        let envelope = native::create_envelope(ledger, issuer, worker, request, claim);
        let op_start = Instant::now();
        let result = runtime.block_on(client.request(envelope));
        latencies.push(op_start.elapsed().as_nanos());
        match result {
            Ok(env) => match env.result {
                Response::Native(NativeMutationReply::Committed(_)) => {
                    committed += 1;
                    created.push(claim);
                }
                _ => refused += 1,
            },
            Err(_) => unknown += 1,
        }
    }
    let wall = start.elapsed();

    // Read phase: cycle through the committed claims, timing each read.
    let mut read_latencies = Vec::with_capacity(shape.reads as usize);
    let mut read_hits = 0u64;
    if shape.reads > 0 && !created.is_empty() {
        for i in 0..shape.reads {
            let claim = created[(i as usize) % created.len()];
            let envelope = RequestEnvelope {
                protocol: focal_wire::NATIVE_PROTOCOL_VERSION,
                ledger,
                route_epoch: RouteEpoch(1),
                request_epoch: RequestEpoch(1),
                request_id: RequestId::from_u128(base + 2_000_000 + u128::from(i)),
                operation: Operation::NativeRead(NativeReadRequest {
                    consistency: ReadConsistency::Linearizable,
                    query: NativeReadQuery::Claim {
                        id: ClaimId::from_u128(claim),
                        expand: NativeClaimExpand::default(),
                    },
                    max_items: 1,
                }),
            };
            let op_start = Instant::now();
            let result = runtime.block_on(client.request(envelope));
            read_latencies.push(op_start.elapsed().as_nanos());
            if let Ok(env) = result
                && let Response::NativeRead(page) = env.result
                && !page.objects.is_empty()
            {
                read_hits += 1;
            }
        }
    }
    let read_wall: f64 = read_latencies.iter().map(|n| *n as f64).sum::<f64>() / 1e9;

    drop(client);
    drop(host);
    owner.join().unwrap();

    let seconds = wall.as_secs_f64();
    let throughput = if seconds > 0.0 {
        committed as f64 / seconds
    } else {
        0.0
    };
    let read_throughput = if read_wall > 0.0 {
        read_hits as f64 / read_wall
    } else {
        0.0
    };
    Report {
        shape,
        committed,
        refused,
        unknown,
        wall_ms: wall.as_millis(),
        throughput_ops_per_s: throughput,
        latency_ns: report::latency(latencies),
        reads: read_latencies.len() as u64,
        read_hits,
        read_throughput_ops_per_s: read_throughput,
        read_latency_ns: report::latency(read_latencies),
    }
}
