//! The first `activate-native` of a fresh founder through its running
//! node's admin socket (the KIND campaign's D3): the command starts the
//! durable promise of the native decoder itself, and the owner holds the
//! activation until that promise is durable — where it refused the call
//! with the write it had just staged, worded `unavailable`, and the
//! operator had to ask again. Asked once, not ridden out.
#![cfg(unix)]
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros
)]

#[path = "support/fleet.rs"]
mod fleet;

use fleet::*;
use serde_json::Value;
use std::time::Instant;

#[test]
fn a_fresh_founders_first_online_activation_is_held_until_its_promise_is_durable() {
    let founder = Node::new("founder");
    let _server = start(&founder, &["--advertise", &address()]);
    let started = Instant::now();
    let output = run(&founder, None, &["activate", "native"]);
    assert!(
        output.status.success(),
        "the first call: {}\n{}",
        String::from_utf8_lossy(&output.stderr),
        String::from_utf8_lossy(&output.stdout)
    );
    let activation: Value = serde_json::from_slice(&output.stdout).unwrap();
    eprintln!("proposed in {:?}: {activation}", started.elapsed());
    assert_eq!(
        activation["result"]["kind"], "replica_native_activation_proposed",
        "{activation}"
    );
    // Authored natively from then on.
    let client = Node::new("client");
    let alice = enroll_client(&founder, &client, "alice");
    let claim = write_claim(
        &founder,
        &alice,
        "authored natively after one activation call",
    );
    assert_eq!(claim_id(&read_claim(&founder, &claim)), claim);
}
