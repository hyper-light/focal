//! The audit's F16: every fixed allowance of the consensus crate's memory
//! pricing is derived from the structures it stands for, and here each is
//! held to what the counting allocator measures (benches/support of
//! focal-memory): decoding a peer message of every shape, opening a group
//! on a shared WAL, and a transition — the bytes the consensus crate and
//! the core asked for, attributed by the innermost frame, never above the
//! allowance that funds them.
#![allow(
    clippy::panic,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::unreachable,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros,
    clippy::cast_precision_loss,
    dead_code
)]
#[path = "../../focal-memory/benches/support/alloc_count.rs"]
mod alloc_count;

use focal_consensus::{ConfState, DurableNode, Entry, Message, NodeConfig, PbMessageExt, Snapshot};
use focal_log::{SharedWal, WalIdentity, WalOptions};

/// The crates whose allocations a consensus allowance funds; the WAL's are
/// the node-wide log budget's, the standard library's are attributed to
/// whichever of these asked.
const GROUPS: &[&[&str]] = &[&["focal_consensus", "focal_raft"], &["focal_log"]];

fn peak(operation: impl FnOnce()) -> usize {
    let baseline = alloc_count::reset_peak();
    operation();
    alloc_count::peak_growth(baseline)
}
/// The bytes the consensus crate and the core asked for during `operation`.
fn consensus_bytes(operation: impl FnOnce()) -> u64 {
    alloc_count::configure(1, 1, 1 << 30);
    alloc_count::reset_sites();
    let mut meter = alloc_count::Meter::start("allowance");
    let before = meter.open();
    operation();
    meter.close(before);
    let _ = meter.finish();
    let (groups, _, _) = alloc_count::bytes_by_innermost(GROUPS, 1);
    groups[0]
}
fn config(members: usize) -> NodeConfig {
    let voters: Vec<u64> = (1..=members as u64).collect();
    NodeConfig::joining(1, [1; 16], [2; 16], voters, Vec::new())
}

/// A message of every shape decodes within its charge: many entries (the
/// entries list doubling), one large entry (its buffer doubling) and a
/// snapshot with the most members a configuration may name twice over.
#[test]
fn every_allowance_covers_what_the_counting_allocator_measures() {
    // The counting allocator's site table is one for the process, so the
    // three measurements run one after another.
    a_message_decodes_within_its_charge_whatever_its_shape();
    a_group_opens_within_its_initial_allowance();
    a_transition_stays_within_its_staging();
}

fn a_message_decodes_within_its_charge_whatever_its_shape() {
    let many = Message {
        entries: vec![Entry::default(); 4096],
        ..Default::default()
    };
    let big = Message {
        entries: vec![Entry {
            data: vec![7; 1 << 20],
            ..Default::default()
        }],
        ..Default::default()
    };
    let mut snapshot = Snapshot {
        data: vec![5; 4 << 20],
        ..Default::default()
    };
    snapshot.mut_metadata().set_conf_state(ConfState {
        voters: (1..=1024).collect(),
        learners: (2000..=3023).collect(),
        ..Default::default()
    });
    let carrying = Message {
        snapshot: Some(snapshot),
        ..Default::default()
    };
    for (name, message) in [("many", many), ("big", big), ("snapshot", carrying)] {
        let bytes = message.write_to_bytes().unwrap();
        let charge = focal_consensus::decode_message_charge(&bytes).unwrap();
        let measured = peak(|| {
            let decoded = focal_consensus::decode_message(&bytes).unwrap();
            std::hint::black_box(decoded);
        });
        eprintln!(
            "F16 decode {name}: {} bytes, charge {charge}, peak {measured}",
            bytes.len()
        );
        assert!(
            measured <= charge,
            "{name}: peak {measured} above the charge {charge}"
        );
        // Derived, not padded: within a small multiple of what was measured.
        assert!(
            charge <= measured * 3 + 65536,
            "{name}: charge {charge} for a peak of {measured}"
        );
    }
}

/// A group opened on a WAL already open — alone, one member and the most —
/// allocates for itself no more than its initial allowance; the WAL's own
/// allocations are the log budget's.
fn a_group_opens_within_its_initial_allowance() {
    for members in [1usize, 1024] {
        let dir = tempfile::tempdir().unwrap();
        let options = WalOptions::new(WalIdentity {
            cluster: [1; 16],
            node: 1,
            stream: 0,
        });
        let shared = SharedWal::open(dir.path(), options).unwrap();
        let first = DurableNode::open_on_wal(config(members), shared.clone()).unwrap();
        let mut second = config(members);
        second.group_id = [3; 16];
        let estimate = DurableNode::initial_estimate(&second).unwrap();
        let budget = focal_memory::MemoryBudget::new(512 << 20, 128 << 20).unwrap();
        let measured = consensus_bytes(|| {
            let node =
                DurableNode::open_on_wal_in(second.clone(), shared.clone(), &budget).unwrap();
            std::hint::black_box(&node);
            std::mem::forget(node);
        });
        eprintln!("F16 open {members} members: estimate {estimate}, consensus bytes {measured}");
        assert!(
            u64::try_from(estimate).unwrap() >= measured,
            "{members} members: the group asked {measured} bytes, above the allowance {estimate}"
        );
        assert!(
            u64::try_from(estimate).unwrap() <= measured * 4 + 65536,
            "{members}: {estimate} for {measured}"
        );
        drop(first);
    }
}

/// A proposal's transition asks the consensus crate and the core for no
/// more than the staging its guard reserved.
fn a_transition_stays_within_its_staging() {
    let dir = tempfile::tempdir().unwrap();
    let mut node = DurableNode::open(config(1), dir.path()).unwrap();
    node.campaign().unwrap();
    node.drain().unwrap();
    node.propose(vec![7u8; 1024]).unwrap();
    node.drain().unwrap();
    // The staging the proposal's guard reserves: for its bytes.
    let estimate = node.staging_estimate_for(1024).unwrap();
    let measured = consensus_bytes(|| {
        node.propose(vec![7u8; 1024]).unwrap();
        node.drain().unwrap();
    });
    eprintln!("F16 transition: estimate {estimate}, consensus bytes {measured}");
    assert!(
        u64::try_from(estimate).unwrap() >= measured,
        "the transition asked {measured} bytes, above the staging {estimate}"
    );
}
