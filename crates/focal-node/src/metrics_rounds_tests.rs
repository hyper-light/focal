use super::*;
use crate::{
    metrics::{MetricsPage, PeerRtt},
    network_admin::{MAX_COMMAND, operator::OperatorReply},
};
use focal_model::SessionId;
use std::collections::BTreeSet;

fn labels() -> MetricLabels {
    MetricLabels {
        node: u64::MAX,
        cluster: "f".repeat(32),
        region: Some("a-region-named-at-length".into()),
        zone: Some("a-zone-named-at-length".into()),
        role: "founder",
    }
}
fn memory() -> MemoryBudget {
    MemoryBudget::new(1 << 30, 1 << 20).unwrap()
}
fn ledger(index: u128) -> LedgerId {
    LedgerId {
        tenant: TenantId::from_u128(1),
        session: SessionId::from_u128(index),
    }
}

#[test]
fn every_value_is_counted_at_its_widest() {
    let text = "# HELP a The a.\n# TYPE a gauge\na{node=\"1\",peer=\"2 3\"} 7\n";
    assert_eq!(
        widest_bytes(text),
        Some(
            "# HELP a The a.\n".len()
                + "# TYPE a gauge\n".len()
                + "a{node=\"1\",peer=\"2 3\"} ".len()
                + VALUE_WIDTH
                + 1
        )
    );
}

/// A full page is exactly what one operator read carries: the bound is the
/// admin frame's, not a choice (the audit's F26).
#[test]
fn a_full_page_is_exactly_what_one_operator_read_carries() {
    let reply = OperatorReply::Metrics("x".repeat(MAX_PAGE_BYTES));
    assert_eq!(
        postcard::experimental::serialized_size(&reply).unwrap(),
        MAX_COMMAND
    );
}

#[test]
fn a_page_holds_its_fixed_part_and_one_of_each_family() {
    let budget = PageBudget::derive(&labels(), memory().stats()).unwrap();
    let one_each = budget.fixed + budget.entity.iter().sum::<usize>();
    assert!(one_each <= MAX_PAGE_BYTES, "{budget:?}");
    assert!(budget.entity.iter().all(|bytes| *bytes > 0), "{budget:?}");
    let [sessions, ..] = budget.capacities([4096, 0, 0, 0]);
    println!(
        "page {MAX_PAGE_BYTES} bytes: fixed {} at its widest, a session {}, a root member {}, \
         a measured peer {}, a tenant {}; {sessions} of 4,096 sessions a round",
        budget.fixed, budget.entity[0], budget.entity[1], budget.entity[2], budget.entity[3]
    );
}

/// Every entity while the page holds them all; else one of each family that
/// has any and the rest shared in proportion to what each family costs,
/// always within the page and wasting less than one entity of each family.
#[test]
fn a_round_lists_all_it_can_and_a_fair_share_of_each_family_when_it_cannot() {
    let budget = PageBudget::derive(&labels(), memory().stats()).unwrap();
    assert_eq!(budget.capacities([1, 2, 1, 1]), [1, 2, 1, 1]);
    for counts in [
        [4096, 5, 1024, 64],
        [4096, 0, 0, 0],
        [0, 1024, 1024, 0],
        [100_000, 1024, 1024, 4096],
        [1, 1024, 1, 1],
    ] {
        let listed = budget.capacities(counts);
        let bytes = budget.fixed
            + listed
                .iter()
                .zip(budget.entity)
                .map(|(listed, bytes)| listed * bytes)
                .sum::<usize>();
        assert!(bytes <= MAX_PAGE_BYTES, "{counts:?} {listed:?}");
        for (count, listed) in counts.iter().zip(listed) {
            assert!(listed <= *count, "{counts:?} {listed:?}");
            if *count > 0 {
                assert!(listed >= 1, "{counts:?} {listed:?}");
            }
        }
        if listed != counts {
            let waste = MAX_PAGE_BYTES - bytes;
            assert!(
                waste < budget.entity.iter().sum::<usize>(),
                "{counts:?} {listed:?}: {waste} bytes left over"
            );
        }
    }
}

/// The flagged first, the rest from where the family's last round stopped:
/// every entity is listed within ⌈rest / (room − flagged)⌉ rounds, and the
/// flagged in every one while they fit.
#[test]
fn the_flagged_come_first_and_the_rest_rotate_until_every_one_is_listed() {
    let flagged = [3u32, 11, 17];
    let entities: Vec<(u32, bool)> = (0..20).map(|key| (key, flagged.contains(&key))).collect();
    let mut cursor = None;
    let mut seen = BTreeSet::new();
    let mut rounds = 0;
    while seen.len() < entities.len() {
        let (chosen, next) = choose(&entities, cursor, 5).unwrap();
        assert_eq!(chosen.len(), 5);
        assert!(chosen.windows(2).all(|pair| pair[0] < pair[1]));
        for key in flagged {
            assert!(chosen.contains(&key), "round {rounds}: {chosen:?}");
        }
        seen.extend(chosen);
        cursor = next;
        rounds += 1;
    }
    assert_eq!(rounds, 17usize.div_ceil(5 - 3));
}

/// Flagged beyond the room rotate among themselves: each is listed within
/// ⌈flagged / room⌉ rounds.
#[test]
fn the_flagged_beyond_the_room_rotate_among_themselves() {
    let entities: Vec<(u32, bool)> = (0..30).map(|key| (key, key % 3 == 0)).collect();
    let mut cursor = None;
    let mut seen = BTreeSet::new();
    for _ in 0..10usize.div_ceil(4) {
        let (chosen, next) = choose(&entities, cursor, 4).unwrap();
        assert!(chosen.iter().all(|key| key % 3 == 0), "{chosen:?}");
        seen.extend(chosen);
        cursor = next;
    }
    assert_eq!(seen, (0..30).filter(|key| key % 3 == 0).collect());
}

/// The node's counts never fall: a session that leaves, starts over or is
/// installed again keeps what it counted in the node's totals; the counts
/// kept are the hosted set's only, and so is their charge.
#[test]
fn the_node_counts_never_fall_as_sessions_leave_start_over_and_return() {
    let memory = memory();
    let mut rounds = Rounds::new(&labels(), memory.clone()).unwrap();
    let counts = |n: u64| SessionCounters {
        peers_unreachable: n,
        frames_held: n,
        refused_periods: n,
        ..SessionCounters::default()
    };
    let used = memory.stats().used;
    let first = rounds
        .account(&[
            (ledger(1), 1, counts(5)),
            (ledger(2), 2, counts(7)),
            (ledger(3), 3, counts(1)),
        ])
        .unwrap();
    assert_eq!(first.peers_unreachable, 13);
    assert_eq!(rounds.last.len(), 3);
    assert!(memory.stats().used > used);
    // Session 2 left, session 3 started over, session 1 was installed again
    // and session 4 arrived.
    let second = rounds
        .account(&[
            (ledger(1), 4, counts(2)),
            (ledger(3), 3, counts(0)),
            (ledger(4), 5, counts(9)),
        ])
        .unwrap();
    assert_eq!(second.peers_unreachable, 5 + 7 + 1 + 2 + 9);
    assert_eq!(second.frames_held, second.peers_unreachable);
    assert!(second.peers_unreachable >= first.peers_unreachable);
    assert_eq!(rounds.last.len(), 3);
    let third = rounds.account(&[]).unwrap();
    assert_eq!(third, second);
    assert!(rounds.last.is_empty());
    assert_eq!(rounds.charge.as_ref().map_or(0, Allocation::bytes), 0);
    drop(rounds);
    assert_eq!(memory.stats().used, used);
}

/// A session whose owner did not answer, or whose replica was refused
/// periods since its last round, is flagged by its history.
#[test]
fn a_session_unanswered_or_refusing_periods_is_flagged_next_round() {
    let mut rounds = Rounds::new(&labels(), memory()).unwrap();
    let counts = |refused: u64| SessionCounters {
        refused_periods: refused,
        ..SessionCounters::default()
    };
    rounds
        .account(&[(ledger(1), 1, counts(0)), (ledger(2), 2, counts(4))])
        .unwrap();
    rounds.answered(&[(ledger(1), false), (ledger(2), true)]);
    assert!(rounds.flagged(ledger(1), &counts(0)));
    assert!(!rounds.flagged(ledger(2), &counts(4)));
    assert!(rounds.flagged(ledger(2), &counts(5)));
    rounds.answered(&[(ledger(1), true)]);
    assert!(!rounds.flagged(ledger(1), &counts(0)));
}

/// A page of the widest entities the room allows, every number at its
/// widest, stays within one operator read.
#[test]
fn a_page_of_the_widest_entities_the_room_allows_stays_within_one_operator_read() {
    let labels = labels();
    let memory = memory();
    let budget = PageBudget::derive(&labels, memory.stats()).unwrap();
    let [sessions, members, peers, tenants] = budget.capacities([4096, 1024, 1024, 4096]);
    let mut snapshot = MetricsSnapshot::widest(&labels, memory.stats());
    snapshot.sessions = vec![SessionMetrics::widest(); sessions];
    snapshot.root.peers = vec![widest_root_peer(); members];
    snapshot.peer_rtts = vec![
        PeerRtt {
            peer: u64::MAX,
            rtt_ms: u64::MAX,
        };
        peers
    ];
    snapshot.agent.as_mut().unwrap().admission.tenants = vec![widest_tenant(); tenants];
    let used = memory.stats().used;
    let page = MetricsPage::new(snapshot, &memory).unwrap();
    assert!(page.text.len() <= MAX_PAGE_BYTES, "{}", page.text.len());
    assert!(memory.stats().used > used, "the page is charged");
    let reply = OperatorReply::Metrics(page.text.clone());
    assert!(postcard::experimental::serialized_size(&reply).unwrap() <= MAX_COMMAND);
    drop(page);
    assert_eq!(memory.stats().used, used);
}
