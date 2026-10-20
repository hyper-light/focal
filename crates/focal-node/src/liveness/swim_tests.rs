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

/// A driver for `PEER` itself, knowing `SELF` and `RUMOURED`: the member the
/// tests' first driver suspects or declares dead.
fn peer_driver() -> (LivenessHandle, LivenessDriver, MemoryBudget) {
    let budget = MemoryBudget::new(64 << 20, 16 << 20).unwrap();
    let (handle, mut driver) =
        LivenessHandle::channel(&budget, LivenessConfig::default(), PEER, namespace()).unwrap();
    handle.report(LocalFacts {
        generation: GENERATION,
        members: Arc::new(BTreeMap::from([(SELF, GENERATION), (RUMOURED, GENERATION)])),
        witness: 1,
        overloaded: false,
    });
    driver.sync_facts(SENT_AT_MS);
    (handle, driver, budget)
}
/// `RUMOURED`'s word that `PEER` is `status` at `incarnation`.
fn rumour(incarnation: u64, status: MemberStatus) -> LivenessUpdate {
    LivenessUpdate {
        node: PEER,
        generation: GENERATION,
        incarnation,
        status,
        origin: RUMOURED,
    }
}
/// What the next message `driver` sends says about `PEER`.
fn told_of_peer(driver: &mut LivenessDriver) -> Vec<LivenessUpdate> {
    let reply = driver.reply(ProbeOutcome::Ack, SEQUENCE, None);
    reply
        .updates
        .into_iter()
        .filter(|update| update.node == PEER)
        .collect()
}

/// A member suspected or declared dead at its incarnation that answers at
/// that incarnation has not heard its verdict. It is not revived here: the
/// next message this node sends tells it the verdict again, it refutes with
/// the next incarnation, and that revives it; the verdict arriving again
/// afterwards is stale. Revived at the same incarnation, as it was, it was
/// alive here alone: the other members hold the verdict and take no alive no
/// newer than it, this node's buffer still carried the verdict, and the
/// verdict coming back killed the member again (macOS CI, deployment_zones,
/// 2026-10-20: a zone resumed after SIGSTOP was dead again seconds after it
/// rejoined, its verdict committed back and forth).
#[test]
fn a_member_dead_or_suspected_at_its_incarnation_comes_back_only_by_its_refutation() {
    for status in [MemberStatus::Dead, MemberStatus::Suspect] {
        let (_handle, mut driver, _budget) = driver();
        let (_peer_handle, mut peer, _peer_budget) = peer_driver();
        let incarnation = peer.state.incarnation;
        driver.learn_alive(PEER, incarnation, NOW_MS, true);
        driver.on_update(rumour(incarnation, status), NOW_MS);
        assert_eq!(driver.state.members[&PEER].status, status);
        // The member was silent long enough for every broadcast of its verdict
        // to run out.
        while !driver.state.gossip.is_empty() {
            driver.state.gossip.piggyback();
        }
        // It answers, at the incarnation it has: still suspected or dead here.
        driver.learn_alive(PEER, incarnation, NOW_MS + 1_000, true);
        let member = &driver.state.members[&PEER];
        assert_eq!(
            member.status, status,
            "{status:?}: not revived by its answer"
        );
        assert_eq!(member.incarnation, incarnation);
        // The answer it is sent tells it, and it refutes.
        let told = told_of_peer(&mut driver);
        assert_eq!(told.len(), 1, "{status:?}: the verdict rides the answer");
        assert_eq!((told[0].incarnation, told[0].status), (incarnation, status));
        for update in told {
            peer.on_update(update, NOW_MS + 1_000);
        }
        assert_eq!(peer.state.incarnation, incarnation + 1);
        let probe = peer.request(ProbeKind::Direct, None);
        driver.learn_alive(PEER, probe.incarnation, NOW_MS + 2_000, true);
        let member = &driver.state.members[&PEER];
        assert_eq!(
            member.status,
            MemberStatus::Alive,
            "{status:?}: revived by its refutation"
        );
        assert_eq!(member.incarnation, incarnation + 1);
        assert!(member.suspicion.is_none());
        // Its revival supersedes the verdict everywhere: the verdict arriving
        // again, from a member that has not heard the refutation, is stale.
        driver.on_update(rumour(incarnation, MemberStatus::Dead), NOW_MS + 3_000);
        assert_eq!(driver.state.members[&PEER].status, MemberStatus::Alive);
        let told = told_of_peer(&mut driver);
        assert!(
            told.iter()
                .all(|update| update.status == MemberStatus::Alive
                    && update.incarnation == incarnation + 1),
            "{status:?}: {told:?}"
        );
    }
}

/// The verdict is told again on every answer at the old incarnation, not once:
/// an answer whose reply is lost leaves the member to hear it from the next.
#[test]
fn every_answer_at_the_old_incarnation_tells_the_verdict_again() {
    let (_handle, mut driver, _budget) = driver();
    driver.learn_alive(PEER, 5, NOW_MS, true);
    driver.on_update(rumour(5, MemberStatus::Dead), NOW_MS);
    for round in 1..=3 {
        while !driver.state.gossip.is_empty() {
            driver.state.gossip.piggyback();
        }
        driver.learn_alive(PEER, 5, NOW_MS + round * 1_000, true);
        let told = told_of_peer(&mut driver);
        assert_eq!(told.len(), 1, "round {round}");
        assert_eq!(told[0].status, MemberStatus::Dead);
    }
    // An answer at an older incarnation (a reply delayed past a refutation)
    // proves life but tells and changes nothing.
    driver.learn_alive(PEER, 6, NOW_MS + 5_000, true);
    while !driver.state.gossip.is_empty() {
        driver.state.gossip.piggyback();
    }
    driver.learn_alive(PEER, 5, NOW_MS + 6_000, true);
    assert_eq!(driver.state.members[&PEER].status, MemberStatus::Alive);
    assert_eq!(driver.state.members[&PEER].last_alive_ms, NOW_MS + 6_000);
    assert!(told_of_peer(&mut driver).is_empty());
}

/// A member that restarts with an incarnation behind the one it was declared
/// dead at (its clock began behind) is told its death and refutes past it;
/// ignored as a stale answer, it would stay dead for good.
#[test]
fn a_member_that_restarts_behind_its_verdict_is_told_it_and_refutes_past_it() {
    let (_handle, mut driver, _budget) = driver();
    let (_peer_handle, mut peer, _peer_budget) = peer_driver();
    driver.learn_alive(PEER, 9, NOW_MS, true);
    driver.on_update(rumour(9, MemberStatus::Dead), NOW_MS);
    while !driver.state.gossip.is_empty() {
        driver.state.gossip.piggyback();
    }
    peer.state.incarnation = 7;
    let probe = peer.request(ProbeKind::Direct, None);
    driver.learn_alive(PEER, probe.incarnation, NOW_MS + 1_000, true);
    assert_eq!(driver.state.members[&PEER].status, MemberStatus::Dead);
    let told = told_of_peer(&mut driver);
    assert_eq!(told.len(), 1);
    assert_eq!(
        (told[0].incarnation, told[0].status),
        (9, MemberStatus::Dead)
    );
    for update in told {
        peer.on_update(update, NOW_MS + 1_000);
    }
    assert_eq!(peer.state.incarnation, 10);
    let probe = peer.request(ProbeKind::Direct, None);
    driver.learn_alive(PEER, probe.incarnation, NOW_MS + 2_000, true);
    let member = &driver.state.members[&PEER];
    assert_eq!(
        (member.status, member.incarnation),
        (MemberStatus::Alive, 10)
    );
}
