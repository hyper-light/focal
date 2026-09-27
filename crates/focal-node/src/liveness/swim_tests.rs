//! Acknowledgement binding for one SWIM probe round. An answer counts as
//! proof of life only when it answers *this* probe: the same sequence, from
//! the probed node, at its current enrollment generation, and not a refusal.
//! Anything else decides nothing — it neither confirms the member, moves the
//! local Vivaldi coordinate, nor folds the answer's piggybacked gossip. This
//! is the regression class in which a reliable transport redelivered a dead
//! peer's earlier acknowledgements and, without the sequence check, those
//! stale replies passed for fresh ones forever; the non-vacuous half of each
//! test proves the identical answer *is* learned once it matches.
use super::*;
use crate::liveness::{
    coordinates::NetworkCoordinate,
    gossip::{LivenessUpdate, MemberStatus},
    health::LocalHealth,
    wire::{PROBE_SCHEMA, ProbeOutcome, ProbeReply},
};
use focal_memory::MemoryBudget;
use focal_wire::PeerSendError;
use std::{collections::BTreeMap, sync::Arc};

const SELF: u64 = 1;
const PEER: u64 = 2;
const RUMOURED: u64 = 3;
const GENERATION: u64 = 1;
const SEQUENCE: u64 = 7;
const SENT_AT_MS: u64 = 1_000;
/// A 40 ms round trip.
const NOW_MS: u64 = 1_040;

fn namespace() -> focal_model::LedgerId {
    focal_model::LedgerId {
        tenant: focal_model::TenantId([1; 16]),
        session: focal_model::SessionId([2; 16]),
    }
}
/// A driver that knows two members at generation 1: the peer it probes and a
/// third node the peer gossips about.
fn driver() -> (LivenessHandle, LivenessDriver, MemoryBudget) {
    let budget = MemoryBudget::new(64 << 20, 16 << 20).unwrap();
    let (handle, mut driver) =
        LivenessHandle::channel(&budget, LivenessConfig::default(), SELF, namespace()).unwrap();
    handle.report(LocalFacts {
        generation: GENERATION,
        members: Arc::new(BTreeMap::from([(PEER, GENERATION), (RUMOURED, GENERATION)])),
        witness: 1,
        overloaded: false,
    });
    driver.sync_facts(SENT_AT_MS);
    assert_eq!(driver.state.members[&PEER].status, MemberStatus::Alive);
    assert!(!driver.state.members[&PEER].confirmed);
    (handle, driver, budget)
}
/// The peer's answer, carrying a rumour that `RUMOURED` is suspect and a
/// coordinate off the origin so an accepted answer moves ours.
fn answer(node: u64, generation: u64, sequence: u64, outcome: ProbeOutcome) -> ProbeReply {
    let config = LivenessConfig::default();
    let mut coordinate = NetworkCoordinate::origin(&config.vivaldi);
    coordinate.vec[0] = 12.0;
    ProbeReply {
        schema: PROBE_SCHEMA,
        outcome,
        node,
        generation,
        sequence,
        incarnation: 1,
        coordinate,
        health: LocalHealth::default(),
        extension: None,
        updates: vec![LivenessUpdate {
            node: RUMOURED,
            generation: GENERATION,
            incarnation: 1,
            status: MemberStatus::Suspect,
            origin: node,
        }],
    }
}
fn classify(driver: &mut LivenessDriver, reply: &ProbeReply) -> Verdict {
    driver.classify(
        PEER,
        SEQUENCE,
        SENT_AT_MS,
        NOW_MS,
        Ok(reply.encode().unwrap()),
    )
}
/// Nothing about the round was learned: the peer is unconfirmed and
/// unmeasured, our coordinate is untouched and the rumour was not folded.
fn assert_nothing_learned(driver: &LivenessDriver, last_alive_before: u64) {
    let origin = NetworkCoordinate::origin(&driver.config.vivaldi);
    let peer = &driver.state.members[&PEER];
    assert!(!peer.confirmed, "the member was not confirmed");
    assert!(peer.coordinate.is_none(), "no peer coordinate was recorded");
    assert!(peer.last_rtt_ms.is_none(), "no round trip was measured");
    assert_eq!(
        peer.last_alive_ms, last_alive_before,
        "no proof of life was recorded"
    );
    assert_eq!(
        driver.state.coordinate, origin,
        "our coordinate did not move"
    );
    assert_eq!(
        driver.state.members[&RUMOURED].status,
        MemberStatus::Alive,
        "the answer's gossip was discarded, not learned"
    );
}
/// The round was learned in full from the accepted answer.
fn assert_learned(driver: &LivenessDriver) {
    let origin = NetworkCoordinate::origin(&driver.config.vivaldi);
    let peer = &driver.state.members[&PEER];
    assert!(peer.confirmed, "the acknowledged member is confirmed");
    assert!(
        peer.coordinate.is_some(),
        "the peer's coordinate was recorded"
    );
    assert_eq!(
        peer.last_rtt_ms,
        Some(NOW_MS - SENT_AT_MS),
        "the round trip was measured"
    );
    assert!(
        peer.last_alive_ms >= SENT_AT_MS,
        "direct proof of life was recorded"
    );
    assert_ne!(
        driver.state.coordinate, origin,
        "the measured round trip moved our coordinate"
    );
    assert_eq!(
        driver.state.members[&RUMOURED].status,
        MemberStatus::Suspect,
        "the accepted answer's gossip was folded"
    );
}

#[test]
fn a_stale_sequence_acknowledgement_decides_nothing_and_folds_no_gossip() {
    let (_handle, mut driver, _budget) = driver();
    let before = driver.state.members[&PEER].last_alive_ms;
    let stale = answer(PEER, GENERATION, SEQUENCE + 1, ProbeOutcome::Ack);
    assert!(
        matches!(classify(&mut driver, &stale), Verdict::Inconclusive),
        "an answer to some other probe is not proof of life for this one"
    );
    assert_nothing_learned(&driver, before);
    let fresh = answer(PEER, GENERATION, SEQUENCE, ProbeOutcome::Ack);
    assert!(
        matches!(classify(&mut driver, &fresh), Verdict::Acknowledged(_)),
        "the identical answer at the probe's own sequence acknowledges it"
    );
    assert_learned(&driver);
}

#[test]
fn an_acknowledgement_from_another_node_is_inconclusive() {
    let (_handle, mut driver, _budget) = driver();
    let before = driver.state.members[&PEER].last_alive_ms;
    let foreign = answer(RUMOURED, GENERATION, SEQUENCE, ProbeOutcome::Ack);
    assert!(matches!(
        classify(&mut driver, &foreign),
        Verdict::Inconclusive
    ));
    assert_nothing_learned(&driver, before);
}

#[test]
fn an_acknowledgement_at_a_stale_enrollment_generation_is_inconclusive() {
    let (_handle, mut driver, _budget) = driver();
    let before = driver.state.members[&PEER].last_alive_ms;
    let stale = answer(PEER, GENERATION + 1, SEQUENCE, ProbeOutcome::Ack);
    assert!(matches!(
        classify(&mut driver, &stale),
        Verdict::Inconclusive
    ));
    assert_nothing_learned(&driver, before);
    let current = answer(PEER, GENERATION, SEQUENCE, ProbeOutcome::Ack);
    assert!(matches!(
        classify(&mut driver, &current),
        Verdict::Acknowledged(_)
    ));
    assert_learned(&driver);
}

#[test]
fn a_refusal_is_not_proof_of_life() {
    let (_handle, mut driver, _budget) = driver();
    let before = driver.state.members[&PEER].last_alive_ms;
    let refused = answer(PEER, GENERATION, SEQUENCE, ProbeOutcome::Refused);
    assert!(matches!(
        classify(&mut driver, &refused),
        Verdict::Inconclusive
    ));
    assert_nothing_learned(&driver, before);
}

#[test]
fn a_lost_or_closed_lane_is_classified_without_learning_anything() {
    let (_handle, mut driver, _budget) = driver();
    let before = driver.state.members[&PEER].last_alive_ms;
    let lost = driver.classify(PEER, SEQUENCE, SENT_AT_MS, NOW_MS, Err(PeerSendError::Lost));
    assert!(
        matches!(lost, Verdict::Failed),
        "a lost message is a failed probe"
    );
    assert_nothing_learned(&driver, before);
    let closed = driver.classify(
        PEER,
        SEQUENCE,
        SENT_AT_MS,
        NOW_MS,
        Err(PeerSendError::Closed),
    );
    assert!(
        matches!(closed, Verdict::Closed),
        "a closed pool ends the round"
    );
    assert_nothing_learned(&driver, before);
}

#[test]
fn an_undecodable_answer_is_inconclusive() {
    let (_handle, mut driver, _budget) = driver();
    let before = driver.state.members[&PEER].last_alive_ms;
    let garbage = driver.classify(PEER, SEQUENCE, SENT_AT_MS, NOW_MS, Ok(vec![0xff; 7]));
    assert!(matches!(garbage, Verdict::Inconclusive));
    assert_nothing_learned(&driver, before);
}

/// A member this node has never reached is not immune to suspicion: it is
/// tolerated for `unconfirmed_patience - 1` failed rounds (the fault may be
/// ours) and then suspected like any other. Before this bound existed, a
/// node whose committed address no longer answered — a rescheduled pod —
/// timed out every probe forever while the detector kept reporting it alive,
/// and nothing ever re-placed its work.
#[test]
fn an_unconfirmed_member_is_suspected_after_bounded_patience_not_never() {
    let (_handle, mut driver, _budget) = driver();
    let patience = driver.config.unconfirmed_patience;
    assert!(patience >= 1);
    for round in 1..patience {
        driver.probe_failed(PEER, NOW_MS + u64::from(round) * 1_000);
        let peer = &driver.state.members[&PEER];
        assert_eq!(
            peer.status,
            MemberStatus::Alive,
            "round {round}: still within patience"
        );
        assert!(peer.suspicion.is_none());
        assert_eq!(peer.unconfirmed_rounds, round);
    }
    driver.probe_failed(PEER, NOW_MS + u64::from(patience) * 1_000);
    let peer = &driver.state.members[&PEER];
    assert_eq!(
        peer.status,
        MemberStatus::Suspect,
        "patience exhausted: the unreachable member is suspected"
    );
    assert!(peer.suspicion.is_some());
}

/// Direct proof of life confirms the member and clears its patience count;
/// a confirmed member that then falls silent is suspected on its first
/// failed round, as before.
#[test]
fn direct_proof_of_life_resets_the_unconfirmed_patience() {
    let (_handle, mut driver, _budget) = driver();
    let patience = driver.config.unconfirmed_patience;
    for round in 1..patience {
        driver.probe_failed(PEER, NOW_MS + u64::from(round) * 1_000);
    }
    let fresh = answer(PEER, GENERATION, SEQUENCE, ProbeOutcome::Ack);
    assert!(matches!(
        classify(&mut driver, &fresh),
        Verdict::Acknowledged(_)
    ));
    assert!(driver.state.members[&PEER].confirmed);
    assert_eq!(driver.state.members[&PEER].unconfirmed_rounds, 0);
    driver.probe_failed(PEER, NOW_MS + 10_000);
    assert_eq!(
        driver.state.members[&PEER].status,
        MemberStatus::Suspect,
        "a confirmed member is suspected on its first failed round"
    );
}
