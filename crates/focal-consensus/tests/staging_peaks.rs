//! The audit's F16: what a heartbeat, a read barrier or a report stages is
//! the transition's own copies, never the size of the history. The guard's
//! estimate and the transition's measured peak are held to that at growing
//! histories, under a counting allocator (benches/support of focal-memory).
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

use focal_consensus::{DurableNode, NodeConfig};

fn open(dir: &std::path::Path) -> DurableNode {
    let mut node = DurableNode::open(NodeConfig::single(1, [1; 16], [2; 16]), dir).unwrap();
    node.campaign().unwrap();
    node.drain().unwrap();
    node
}
/// `count` committed entries of a kibibyte, each durable before the next.
fn fill(node: &mut DurableNode, count: usize) {
    for _ in 0..count {
        node.propose(vec![7u8; 1024]).unwrap();
        node.drain().unwrap();
    }
}
/// The peak growth of the heap while `operation` runs, in bytes.
fn peak(operation: impl FnOnce()) -> usize {
    let baseline = alloc_count::reset_peak();
    operation();
    alloc_count::peak_growth(baseline)
}
struct Measured {
    history: usize,
    estimate: usize,
    tick: usize,
    read: usize,
    beat: usize,
    propose: usize,
}
fn measure(history: usize) -> Measured {
    let dir = tempfile::tempdir().unwrap();
    let mut node = open(dir.path());
    fill(&mut node, history);
    let estimate = node.staging_estimate().unwrap();
    let tick = peak(|| node.tick().unwrap());
    let read = peak(|| {
        node.read_index(vec![9; 16]).unwrap();
        node.drain().unwrap();
    });
    let beat = peak(|| node.beat().unwrap());
    let propose = peak(|| {
        node.propose(vec![7u8; 1024]).unwrap();
        node.drain().unwrap();
    });
    Measured {
        history,
        estimate,
        tick,
        read,
        beat,
        propose,
    }
}

#[test]
fn a_transition_stages_its_own_copies_whatever_the_history() {
    let small = measure(64);
    let large = measure(1024);
    for measured in [&small, &large] {
        eprintln!(
            "F16 history {} entries: estimate {} bytes; peaks tick {} read {} beat {} propose {}",
            measured.history,
            measured.estimate,
            measured.tick,
            measured.read,
            measured.beat,
            measured.propose
        );
    }
    // The guard's estimate does not grow with the history: a node caught up
    // with itself stages what its queue, its reads and its members take.
    assert!(
        large.estimate <= small.estimate + 4096,
        "the estimate grew with the history: {} at 64, {} at 1024",
        small.estimate,
        large.estimate
    );
    // Nor do the transitions' peaks: each is within a few pages of the
    // small history's, and within the estimate the guard reserves.
    let slack = 16 * 1024;
    for (name, at_small, at_large) in [
        ("tick", small.tick, large.tick),
        ("read", small.read, large.read),
        ("beat", small.beat, large.beat),
        ("propose", small.propose, large.propose),
    ] {
        assert!(
            at_large <= at_small + slack,
            "{name}: {at_small} bytes at 64 entries, {at_large} at 1024"
        );
    }
    for (name, at_large) in [
        ("tick", large.tick),
        ("read", large.read),
        ("beat", large.beat),
    ] {
        assert!(
            at_large <= large.estimate,
            "{name} peaked at {at_large} bytes, above the estimate {}",
            large.estimate
        );
    }
}
