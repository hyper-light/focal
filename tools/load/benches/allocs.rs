// A measurement tool (R11 §5), not shipped: like the dependency-free benches
// it is a plain `harness = false` binary with a counting global allocator
// (crates/focal-memory/benches/support/alloc_count.rs), so panics on bad
// input and direct stdout are appropriate.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::cast_precision_loss,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    clippy::disallowed_macros
)]
//! End-to-end allocation counts: the same in-process node and `focal_client`
//! path that `focal-load`'s driver runs (src/driver.rs), with the counting
//! gate opened around each phase — node open, each claim creation, each
//! linearizable read, shutdown — so allocations per committed claim and per
//! read are separated from the fixed cost. The request envelopes are built
//! with the gate closed (they are the workload's cost, not focal's). Node
//! threads (the session owner, the WAL writer) allocate while a request is in
//! flight; the gate is process-wide, so their allocations are counted too.
//!
//! Environment: `FOCAL_LOAD_CLAIMS` (default 1000), `FOCAL_LOAD_READS`
//! (default = claims), `FOCAL_LOAD_SEED` (default 1), `FOCAL_LOAD_TOP` (sites
//! printed per phase, default 15), and the `FOCAL_ALLOC_*` sampling knobs.
#[path = "../../../crates/focal-memory/benches/support/alloc_count.rs"]
mod alloc_count;
#[path = "../src/native.rs"]
mod native;

use alloc_count::Meter;
use focal_client::{Client, EmbeddedTransport, RetryPolicy};
use focal_ledger::NativeContentProfile;
use focal_model::{ClaimId, RequestEpoch, RequestId, RouteEpoch};
use focal_node::{config::Settings, embedded::EmbeddedNode, host::LocalHost};
use focal_wire::{
    AuthenticatedPeer, NativeClaimExpand, NativeMutationReply, NativeReadQuery, NativeReadRequest,
    Operation, PeerGrant, PeerRole, ReadConsistency, RequestEnvelope, Response, WireLimits,
};
use std::collections::BTreeSet;

const CRATES: [&str; 13] = [
    "focal-wire",
    "focal-client",
    "focal-node",
    "focal-ledger",
    "focal-consensus",
    "focal-raft",
    "focal-log",
    "focal-core",
    "focal-memory",
    "focal-model",
    "tokio",
    "std/alloc",
    "other",
];
const NEEDLES: [&[&str]; 12] = [
    &["crates/focal-wire/", "focal_wire::"],
    &["crates/focal-client/", "focal_client::"],
    &["crates/focal-node/", "focal_node::"],
    &["crates/focal-ledger/", "focal_ledger::"],
    &["crates/focal-consensus/", "focal_consensus::"],
    &["crates/focal-raft/", "focal_raft::"],
    &["crates/focal-log/", "focal_log::"],
    &["crates/focal-core/", "focal_core::"],
    &["crates/focal-memory/", "focal_memory::"],
    &["crates/focal-model/", "focal_model::"],
    &["tokio-", "tokio::"],
    &[
        "library/std/",
        "library/alloc/",
        "library/core/",
        "alloc::",
        "std::",
    ],
];

fn env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn main() {
    alloc_count::configure(101, 1, 200_000);
    let claims = env_u64("FOCAL_LOAD_CLAIMS", 1000);
    let reads = env_u64("FOCAL_LOAD_READS", claims);
    let seed = env_u64("FOCAL_LOAD_SEED", 1);
    let top = env_u64("FOCAL_LOAD_TOP", 15) as usize;
    println!(
        "focal-load end-to-end allocation counts ({claims} claims, {reads} reads, seed {seed})\n"
    );
    let mut phases = Vec::new();

    let root = tempfile::tempdir().unwrap();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let mut settings = Settings::default();
    settings.node.data_dir = Some(root.path().into());

    alloc_count::reset_sites();
    let mut meter = Meter::start("open: activate + EmbeddedNode::open + host + client");
    let before = meter.open();
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
    meter.close(before);
    let phase = meter.finish();
    println!("top sites: open");
    print!(
        "{}",
        alloc_count::shares_report(&phase, &CRATES, &NEEDLES, 10)
    );
    phases.push(phase);
    print!("{}", alloc_count::sites_report(top.min(10)));

    let base = u128::from(seed) << 40;
    let mut committed = 0u64;
    let mut created: Vec<u128> = Vec::with_capacity(claims as usize);
    alloc_count::reset_sites();
    let mut meter = Meter::start("claim: Client::request(native Create) committed");
    for i in 0..claims {
        let request = base + u128::from(i) + 1;
        let claim = base + 1_000_000 + u128::from(i);
        let envelope = native::create_envelope(ledger, issuer, worker, request, claim).unwrap();
        let before = meter.open();
        let result = runtime.block_on(client.request(envelope));
        meter.close(before);
        match result {
            Ok(env) => {
                if let Response::Native(NativeMutationReply::Committed(_)) = env.result {
                    committed += 1;
                    created.push(claim);
                } else {
                    panic!("claim {i} refused: {:?}", env.result);
                }
            }
            Err(error) => panic!("claim {i} failed: {error:?}"),
        }
    }
    let phase = meter.finish();
    println!("\ntop sites: claim ({committed} committed)");
    print!(
        "{}",
        alloc_count::shares_report(&phase, &CRATES, &NEEDLES, 10)
    );
    phases.push(phase);
    print!("{}", alloc_count::sites_report(top));

    let mut read_hits = 0u64;
    alloc_count::reset_sites();
    let mut meter = Meter::start("read: Client::request(NativeRead claim, linearizable)");
    for i in 0..reads {
        let claim = created[(i as usize) % created.len().max(1)];
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
        let before = meter.open();
        let result = runtime.block_on(client.request(envelope));
        meter.close(before);
        if let Ok(env) = result
            && let Response::NativeRead(page) = env.result
            && !page.objects.is_empty()
        {
            read_hits += 1;
        }
    }
    let phase = meter.finish();
    println!("\ntop sites: read ({read_hits} hits of {reads})");
    print!(
        "{}",
        alloc_count::shares_report(&phase, &CRATES, &NEEDLES, 10)
    );
    phases.push(phase);
    print!("{}", alloc_count::sites_report(top));

    alloc_count::reset_sites();
    let mut meter = Meter::start("shutdown: drop client + host, owner.join");
    let before = meter.open();
    drop(client);
    drop(host);
    owner.join().unwrap();
    meter.close(before);
    phases.push(meter.finish());
    drop(runtime);

    println!();
    println!("{}", alloc_count::header());
    for phase in &phases {
        println!("{}", alloc_count::row(phase));
    }
    println!(
        "\nlive heap at exit (tracked): {} bytes; committed {committed}, read hits {read_hits}",
        alloc_count::live()
    );
}
