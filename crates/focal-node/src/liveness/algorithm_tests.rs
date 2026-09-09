use super::{
    coordinates::{NetworkCoordinate, VivaldiConfig, rtt_ucb_ms},
    gossip::{GossipBuffer, LivenessUpdate, MAX_PIGGYBACK, MAX_UPDATES, MemberStatus},
    health::{LocalHealth, MAX_SCORE},
    suspicion::{ExtensionDecision, ExtensionDenial, ExtensionTracker, MAX_EXTENSIONS, Suspicion},
};

#[test]
fn coordinates_converge_on_a_triangle_and_bound_the_round_trip() {
    let config = VivaldiConfig::default();
    // True round trips: a-b 20 ms, b-c 30 ms, a-c 40 ms.
    let truth = [[0.0, 20.0, 40.0], [20.0, 0.0, 30.0], [40.0, 30.0, 0.0]];
    let mut nodes = [NetworkCoordinate::origin(&config); 3];
    for round in 0..200 {
        let a = round % 3;
        let b = (round + 1) % 3;
        let peer = nodes[b];
        nodes[a].update(&peer, truth[a][b], &config);
        let peer = nodes[a];
        nodes[b].update(&peer, truth[a][b], &config);
    }
    for a in 0..3 {
        for b in 0..3 {
            if a == b {
                continue;
            }
            let predicted = nodes[a].distance_ms(&nodes[b]);
            assert!(
                (predicted - truth[a][b]).abs() < 6.0,
                "{a}->{b} predicted {predicted} for {}",
                truth[a][b]
            );
            let bound = rtt_ucb_ms(&nodes[a], Some(&nodes[b]), &config);
            assert!(bound >= predicted, "{bound} >= {predicted}");
            assert!(bound <= config.rtt_max_ms);
        }
        assert!(nodes[a].is_valid(&config));
        assert!(nodes[a].quality(&config) > 0.5, "{:?}", nodes[a]);
    }
    // Too few samples fall back to the conservative defaults; garbage from a
    // peer is ignored.
    let fresh = NetworkCoordinate::origin(&config);
    assert_eq!(
        rtt_ucb_ms(&fresh, Some(&nodes[0]), &config),
        config.rtt_default_ms + config.k_sigma * config.sigma_default_ms
    );
    assert_eq!(fresh.quality(&config), 0.0);
    let mut poisoned = NetworkCoordinate::origin(&config);
    poisoned.vec[0] = f64::NAN;
    assert!(!poisoned.is_valid(&config));
    let mut local = nodes[0];
    let before = local;
    local.update(&poisoned, 10.0, &config);
    assert_eq!(local, before);
    local.update(&nodes[1], -5.0, &config);
    assert_eq!(local, before);
    assert_eq!(
        rtt_ucb_ms(&local, Some(&poisoned), &config),
        config.rtt_default_ms + config.k_sigma * config.sigma_default_ms
    );
}

#[test]
fn local_health_saturates_and_scales_timeouts_between_one_and_three() {
    let mut health = LocalHealth::default();
    assert_eq!(health.multiplier(), 1.0);
    for _ in 0..20 {
        health.on_probe_timeout();
    }
    assert_eq!(health.score, MAX_SCORE);
    assert_eq!(health.multiplier(), 3.0);
    health.on_successful_probe();
    health.on_successful_answer();
    assert_eq!(health.score, MAX_SCORE - 2);
    assert_eq!(health.multiplier(), 2.5);
    for _ in 0..20 {
        health.on_successful_probe();
    }
    assert_eq!(health.score, 0);
    health.on_refutation_needed();
    health.on_late_tick();
    assert_eq!(health.multiplier(), 1.5);
}

fn update(node: u64, incarnation: u64, status: MemberStatus) -> LivenessUpdate {
    LivenessUpdate {
        node,
        generation: 1,
        incarnation,
        status,
        origin: 9,
    }
}

#[test]
fn the_gossip_buffer_keeps_the_newest_verdict_bounds_itself_and_rebroadcasts_a_bounded_number_of_times()
 {
    let mut buffer = GossipBuffer::default();
    assert!(buffer.add(update(1, 1, MemberStatus::Suspect), 10));
    // Weaker or older news about the same node is dropped; stronger or
    // newer news replaces it and starts fresh.
    assert!(!buffer.add(update(1, 1, MemberStatus::Alive), 10));
    assert!(!buffer.add(update(1, 0, MemberStatus::Dead), 10));
    assert!(buffer.add(update(1, 1, MemberStatus::Dead), 10));
    assert!(buffer.add(update(1, 2, MemberStatus::Alive), 10));
    assert_eq!(buffer.latest(1), Some(update(1, 2, MemberStatus::Alive)));
    // λ·ln(n+1) broadcasts for ten members is eight; the buffer drains once
    // every entry was carried that often, the least carried first.
    for node in 2..=5 {
        assert!(buffer.add(update(node, 1, MemberStatus::Alive), 10));
    }
    let first = buffer.piggyback();
    assert_eq!(first.len(), 5);
    let mut rounds = 1;
    while !buffer.is_empty() {
        let carried = buffer.piggyback();
        assert!(carried.len() <= MAX_PIGGYBACK);
        assert!(!carried.is_empty());
        rounds += 1;
        assert!(rounds <= 9, "updates were carried too often");
    }
    assert_eq!(rounds, 8);
    // The buffer never grows past its bound: the most disseminated entry
    // makes room.
    for node in 0..(MAX_UPDATES as u64 + 10) {
        buffer.add(update(100 + node, 1, MemberStatus::Suspect), 10);
    }
    assert_eq!(buffer.len(), MAX_UPDATES);
    let carried = buffer.piggyback();
    assert_eq!(carried.len(), MAX_PIGGYBACK);
    buffer.add(update(999, 1, MemberStatus::Dead), 10);
    assert_eq!(buffer.len(), MAX_UPDATES);
    assert!(buffer.latest(999).is_some());
    buffer.remove(999);
    assert!(buffer.latest(999).is_none());
}

#[test]
fn a_suspicion_shrinks_with_independent_confirmations_and_grows_only_by_earned_extensions() {
    let tracker = ExtensionTracker::new(3_000, 500, 1_000);
    let mut suspicion = Suspicion::new(4, 10_000, 1, 3_000, 18_000, 3, tracker);
    assert_eq!(suspicion.timeout_ms(), 18_000);
    assert_eq!(suspicion.deadline_ms(), 28_000);
    assert!(!suspicion.expired(27_999));
    assert!(suspicion.expired(28_000));
    // The originator's vote never counts; a member counts once.
    assert!(!suspicion.confirm(1));
    assert!(suspicion.confirm(2));
    assert!(!suspicion.confirm(2));
    assert_eq!(suspicion.confirmations(), 1);
    // ln(2)/ln(4) = 0.5 of the 15 s span.
    assert_eq!(suspicion.timeout_ms(), 10_500);
    assert!(suspicion.confirm(3));
    assert!(suspicion.confirm(4));
    assert_eq!(suspicion.confirmations(), 3);
    assert_eq!(suspicion.timeout_ms(), 3_000);
    assert!(suspicion.confirm(5));
    assert_eq!(suspicion.timeout_ms(), 3_000, "never below the minimum");
    // Extensions: halving grants from the base, at most five, only with
    // witnessed progress, never within the interval, never when overloaded.
    assert_eq!(
        suspicion.extensions.request(11_000, 10, true),
        ExtensionDecision::Denied(ExtensionDenial::Overloaded)
    );
    assert_eq!(
        suspicion.extensions.request(11_000, 10, false),
        ExtensionDecision::Granted { millis: 3_000 }
    );
    assert_eq!(suspicion.deadline_ms(), 10_000 + 3_000 + 3_000);
    assert_eq!(
        suspicion.extensions.request(11_500, 11, false),
        ExtensionDecision::Denied(ExtensionDenial::RateLimited)
    );
    assert_eq!(
        suspicion.extensions.request(12_500, 10, false),
        ExtensionDecision::Denied(ExtensionDenial::NoProgress)
    );
    assert_eq!(
        suspicion.extensions.request(12_500, 11, false),
        ExtensionDecision::Granted { millis: 1_500 }
    );
    assert_eq!(
        suspicion.extensions.request(14_000, 12, false),
        ExtensionDecision::Granted { millis: 750 }
    );
    assert_eq!(
        suspicion.extensions.request(16_000, 13, false),
        ExtensionDecision::Granted { millis: 500 }
    );
    assert_eq!(
        suspicion.extensions.request(18_000, 14, false),
        ExtensionDecision::Granted { millis: 500 }
    );
    assert_eq!(suspicion.extensions.count(), MAX_EXTENSIONS);
    assert_eq!(
        suspicion.extensions.request(20_000, 15, false),
        ExtensionDecision::Denied(ExtensionDenial::Exhausted)
    );
    assert_eq!(suspicion.extensions.total_ms(), 6_250);
    assert_eq!(suspicion.deadline_ms(), 10_000 + 3_000 + 6_250);
}
