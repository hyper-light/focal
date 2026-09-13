//! Runs a [`WorkloadShape`] end to end against an in-process node over the
//! embedded transport (the same `focal_client::Client` path the CLI and MCP
//! use), timing each committed request. Local and QUIC transports, reads, and
//! richer metrics are the natural extensions.
use crate::native;
use crate::report::{self, Report};
use crate::shape::WorkloadShape;
use focal_client::{Client, EmbeddedTransport, RetryPolicy};
use focal_ledger::NativeContentProfile;
use focal_node::{config::Settings, embedded::EmbeddedNode, host::LocalHost};
use focal_wire::{
    AuthenticatedPeer, NativeMutationReply, PeerGrant, PeerRole, Response, WireLimits,
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
                Response::Native(NativeMutationReply::Committed(_)) => committed += 1,
                _ => refused += 1,
            },
            Err(_) => unknown += 1,
        }
    }
    let wall = start.elapsed();

    drop(client);
    drop(host);
    owner.join().unwrap();

    let seconds = wall.as_secs_f64();
    let throughput = if seconds > 0.0 {
        committed as f64 / seconds
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
    }
}
