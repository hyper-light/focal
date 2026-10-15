#![cfg(unix)]
#![allow(
    clippy::panic,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::unreachable,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros
)]
//! Native activation and repair across a slow path (the audit's F48), on
//! real `focal` processes whose every datagram crosses a relay that delays
//! it ([`relay`]): a session's hosts are promoted to voters only once each
//! has recorded the others' durable promise of the native decoder, and the
//! exchange of those promises was given 250 ms for each of its parts — the
//! replica's own answer, the peer's, and the recording — so a healthy path
//! further than that never contributed one, and the session never settled.
//! Each part is given what it takes now: the replica its owner's periods,
//! the peer its path.
#[path = "support/fleet.rs"]
mod fleet;
#[path = "support/relay.rs"]
mod relay;
use fleet::*;
use relay::Relay;
use std::time::Duration;

/// A node behind a relay: it binds `listen` and advertises the relay's
/// front, so its peers reach it, and it answers them, across the delay.
struct Behind {
    listen: String,
    relay: Relay,
}
impl Behind {
    fn new(each_way: Duration) -> Self {
        let listen = address();
        let relay = Relay::new(
            listen.parse().unwrap(),
            each_way,
            each_way.checked_div(10).unwrap(),
        );
        Self { listen, relay }
    }
    fn advertise(&self) -> String {
        self.relay.front().to_string()
    }
}

/// Three hosts a round trip of `round_trip` apart activate the native
/// decoder and settle a session that survives one of them.
fn settles_across(round_trip: Duration) {
    let each_way = round_trip.checked_div(2).unwrap();
    let founder = Node::new("founder");
    let hosts = [Node::new("host-a"), Node::new("host-b")];
    let paths: Vec<Behind> = (0..3).map(|_| Behind::new(each_way)).collect();
    activate_native(&founder);
    let _founder_server = start(
        &founder,
        &[
            "--listen",
            &paths[0].listen,
            "--advertise",
            &paths[0].advertise(),
        ],
    );
    let (founder_node, tenant, ledger) = identity(&founder);
    let mut servers = Vec::new();
    let mut nodes = vec![founder_node];
    for (index, host) in hosts.iter().enumerate() {
        let invite = invitation(&founder, host, &format!("host-{index}"));
        let behind = &paths[index + 1];
        servers.push(start(
            host,
            &[
                "--listen",
                &behind.listen,
                "--advertise",
                &behind.advertise(),
                "--invite-file",
                invite.to_str().unwrap(),
            ],
        ));
        nodes.push(identity(host).0);
    }
    // Every host is known to the founder and alive across its relay.
    // Settled on the three, with the founder's own copy alone, before a
    // plan asks for more: a plan is refused until every host's facts are
    // in and the session's placement is known.
    wait_for(&founder, "three hosts", Duration::from_secs(240), |view| {
        settled(view, &ledger, &nodes, 0)
    });
    // The plan promotes both hosts to voters: each promotion needs the
    // host's recorded promise of the native decoder, exchanged across the
    // relays.
    let view = plan_and_settle(&founder, &tenant, &ledger, None, 1, &nodes);
    let session = session_row(&view, &ledger).unwrap();
    let mut voters = ids(&session["voters"]);
    voters.sort_unstable();
    let mut expected = nodes.clone();
    expected.sort_unstable();
    assert_eq!(voters, expected, "{session}");
    // And the session serves across them.
    let client = Node::new("client");
    let alice = enroll_client(&founder, &client, "alice");
    let claim = write_claim(&founder, &alice, "across the relays");
    assert!(!claim.is_empty());
}

#[test]
fn native_activation_and_repair_settle_at_a_round_trip_of_300_ms() {
    settles_across(Duration::from_millis(300));
}
#[test]
fn native_activation_and_repair_settle_at_a_round_trip_of_600_ms() {
    settles_across(Duration::from_millis(600));
}
#[test]
fn native_activation_and_repair_settle_at_a_round_trip_of_1200_ms() {
    settles_across(Duration::from_millis(1_200));
}
