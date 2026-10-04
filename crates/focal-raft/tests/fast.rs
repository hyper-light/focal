//! The fast track (27 §4): a proposal from a member that does not lead is
//! committed once a fast quorum holds it, without the round the classic
//! track takes; and whatever the schedule, what one leader committed by it
//! every later leader holds.
#![allow(
    clippy::panic,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::unreachable,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros
)]
mod support;

use focal_raft::{
    fast::{FAST_PROPOSE, FAST_VOTE},
    proto::{
        ConfChangeSingle, ConfChangeTransition, ConfChangeType, ConfChangeV2, Entry, EntryType,
        Message, MessageType,
    },
};
use support::{Cluster, Mix, New, Op, Replica, Seeded, Settings};

fn add(total: &mut focal_raft::FastStats, more: focal_raft::FastStats) {
    total.proposed += more.proposed;
    total.displaced += more.displaced;
    total.held += more.held;
    total.taken += more.taken;
    total.committed += more.committed;
    total.recovered += more.recovered;
}
fn count(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}
/// Delivers what `carried` admits until nothing it admits is left; the
/// rest stays on the network.
fn carry(group: &mut Cluster<New>, carried: impl Fn(&Message) -> bool) {
    for _ in 0..10_000 {
        let Some(at) = group.net.iter().position(&carried) else {
            return;
        };
        group.act(&Op::Deliver {
            at,
            keep: false,
            lose: false,
        });
    }
    panic!("the network does not fall quiet");
}
fn quiet(group: &mut Cluster<New>) {
    carry(group, |_| true);
}
fn group(members: u64) -> Cluster<New> {
    let voters: Vec<u64> = (1..=members).collect();
    let mut group: Cluster<New> = Cluster::new(members, &voters, Settings::fast(), 17);
    group.act(&Op::Campaign(1));
    quiet(&mut group);
    assert_eq!(group.leaders_now(), vec![1]);
    // The leader committed in its term and applied it: the fast track is
    // open.
    let view = group.peek(1).unwrap().view();
    assert_eq!((view.commit, view.applied), (1, 1));
    group
}
fn commit(group: &Cluster<New>, member: u64) -> u64 {
    group.peek(member).unwrap().view().commit
}
fn applied(group: &Cluster<New>, member: u64) -> u64 {
    group.peek(member).unwrap().app().index
}
fn fast(group: &mut Cluster<New>, member: u64, data: &[u8]) -> bool {
    group.act(&Op::Fast(member, data.to_vec()))[0].accepted == Some(true)
}

#[test]
fn a_proposal_is_committed_once_a_fast_quorum_holds_it() {
    let mut group = group(5);
    assert!(fast(&mut group, 3, b"e"));
    assert_eq!(group.peek(3).unwrap().held(), vec![(2, b"e".to_vec())]);
    // The proposal reaches every voter; each holds it, and says so once it
    // is durable.
    carry(&mut group, |message| message.msg_type == FAST_PROPOSE);
    for member in [2, 4, 5] {
        assert_eq!(group.peek(member).unwrap().held(), vec![(2, b"e".to_vec())]);
    }
    // The leader took it into its log when it heard of it.
    assert_eq!(group.peek(1).unwrap().view().last_index, 2);
    assert_eq!(commit(&group, 1), 1);
    // Three votes and the leader are the fast quorum of five; what the
    // leader sent its members is still on its way.
    let mut votes: Vec<u64> = group
        .net
        .iter()
        .filter(|message| message.msg_type == FAST_VOTE)
        .map(|message| message.from)
        .collect();
    votes.sort_unstable();
    assert_eq!(votes, vec![2, 3, 4, 5]);
    group.act(&Op::Deliver {
        at: group
            .net
            .iter()
            .position(|m| m.msg_type == FAST_VOTE && m.from == 2)
            .unwrap(),
        keep: false,
        lose: false,
    });
    group.act(&Op::Deliver {
        at: group
            .net
            .iter()
            .position(|m| m.msg_type == FAST_VOTE && m.from == 3)
            .unwrap(),
        keep: false,
        lose: false,
    });
    assert_eq!(
        commit(&group, 1),
        1,
        "two votes and the leader are no fast quorum"
    );
    group.act(&Op::Deliver {
        at: group
            .net
            .iter()
            .position(|m| m.msg_type == FAST_VOTE && m.from == 4)
            .unwrap(),
        keep: false,
        lose: false,
    });
    assert_eq!(commit(&group, 1), 2);
    assert_eq!(applied(&group, 1), 2);
    // No member answered an append of the leader's yet.
    assert!(
        !group
            .net
            .iter()
            .any(|message| message.msg_type == MessageType::MsgAppendResponse as i32)
    );
    // The members learn of the commit from the leader, and hold the entry
    // from it.
    quiet(&mut group);
    for member in 1..=5 {
        assert_eq!(applied(&group, member), 2);
        assert!(group.peek(member).unwrap().held().is_empty());
    }
    assert_eq!(group.chosen[&2].3, b"e");
}

#[test]
fn without_the_votes_the_classic_quorum_commits() {
    let mut group = group(5);
    assert!(fast(&mut group, 3, b"e"));
    carry(&mut group, |message| message.msg_type == FAST_PROPOSE);
    // Every vote is lost.
    group.net.retain(|message| message.msg_type != FAST_VOTE);
    quiet(&mut group);
    for member in 1..=5 {
        assert_eq!(applied(&group, member), 2);
    }
    assert_eq!(group.chosen[&2].3, b"e");
    // And when the leader does not hear the proposal, a vote tells it.
    assert!(fast(&mut group, 4, b"f"));
    group
        .net
        .retain(|message| !(message.msg_type == FAST_PROPOSE && message.to == 1));
    quiet(&mut group);
    for member in 1..=5 {
        assert_eq!(applied(&group, member), 3);
    }
    assert_eq!(group.chosen[&3].3, b"f");
}

#[test]
fn of_two_proposals_for_one_index_one_is_taken_and_the_other_said_to_its_proposer() {
    let mut group = group(5);
    assert!(fast(&mut group, 2, b"e"));
    assert!(fast(&mut group, 3, b"f"));
    // Members 4 and 5 hear the second first; the leader the first.
    carry(&mut group, |message| {
        message.msg_type == FAST_PROPOSE && message.from == 3 && message.to >= 4
    });
    carry(&mut group, |message| message.msg_type == FAST_PROPOSE);
    assert_eq!(group.peek(4).unwrap().held(), vec![(2, b"f".to_vec())]);
    assert_eq!(group.peek(1).unwrap().view().last_index, 2);
    let mut displaced = Vec::new();
    for _ in 0..10_000 {
        if group.net.is_empty() {
            break;
        }
        for report in group.act(&Op::Deliver {
            at: 0,
            keep: false,
            lose: false,
        }) {
            displaced.extend(
                report
                    .output
                    .displaced
                    .iter()
                    .map(|said| (report.member, said.3.clone())),
            );
        }
    }
    // Two hold the one and three the other: no fast quorum, and the
    // leader's entry is committed by the classic one.
    assert_eq!(group.chosen[&2].3, b"e");
    assert_eq!(displaced, vec![(3, b"f".to_vec())]);
    for member in 1..=5 {
        assert_eq!(applied(&group, member), 2);
        assert!(group.peek(member).unwrap().held().is_empty());
    }
    // Its proposer proposes it again, for the next index.
    assert!(fast(&mut group, 3, b"f"));
    quiet(&mut group);
    assert_eq!(group.chosen[&3].3, b"f");
}

#[test]
fn what_a_leader_committed_by_the_fast_quorum_the_next_leader_takes() {
    for lost in [1u64, 2, 3, 4, 5] {
        let mut group = group(5);
        assert!(fast(&mut group, 3, b"e"));
        carry(&mut group, |message| message.msg_type == FAST_PROPOSE);
        carry(&mut group, |message| message.msg_type == FAST_VOTE);
        assert_eq!(commit(&group, 1), 2, "the fast quorum committed");
        // The leader stops before any member heard from it, and one of
        // the others is away.
        group.net.clear();
        group.nodes[0] = None;
        if lost != 1 {
            group.nodes[(lost - 1) as usize] = None;
        }
        let candidate = (2..=5).find(|member| *member != lost).unwrap();
        for _ in 0..40 {
            for member in group.up() {
                group.act(&Op::Tick(member));
            }
            quiet(&mut group);
            if !group.leaders_now().is_empty() {
                break;
            }
        }
        let leader = *group.leaders_now().first().expect("the rest elect");
        assert!(leader >= candidate);
        quiet(&mut group);
        // What the group holds at the index is what was committed there.
        for member in group.up() {
            assert!(applied(&group, member) >= 2, "member {member}");
        }
        assert_eq!(group.chosen[&2].3, b"e");
        // Those that were away hold it when they are back.
        assert!(group.settles(200));
        for member in 1..=5 {
            assert!(applied(&group, member) >= 2);
        }
    }
}

#[test]
fn what_no_one_committed_a_later_leader_may_replace() {
    let mut group = group(5);
    // Two hold one entry and two another; the leader hears of neither.
    assert!(fast(&mut group, 2, b"e"));
    assert!(fast(&mut group, 4, b"f"));
    carry(&mut group, |message| {
        message.msg_type == FAST_PROPOSE
            && ((message.from == 2 && message.to == 3) || (message.from == 4 && message.to == 5))
    });
    group.net.clear();
    group.nodes[0] = None;
    for _ in 0..40 {
        for member in group.up() {
            group.act(&Op::Tick(member));
        }
        quiet(&mut group);
        if !group.leaders_now().is_empty() {
            break;
        }
    }
    assert_eq!(group.leaders_now().len(), 1);
    quiet(&mut group);
    // One of the two is at the index, in every log; the other was said to
    // its proposer.
    let taken = group.chosen[&2].3.clone();
    assert!(taken == b"e" || taken == b"f");
    for member in group.up() {
        assert!(applied(&group, member) >= 3);
        assert!(group.peek(member).unwrap().held().is_empty());
    }
}

#[test]
fn what_a_member_holds_it_holds_after_it_stopped() {
    let mut group = group(3);
    assert!(fast(&mut group, 2, b"e"));
    carry(&mut group, |message| {
        message.msg_type == FAST_PROPOSE && message.to == 3
    });
    group.net.clear();
    for member in [2, 3] {
        group.act(&Op::Restart(member));
        assert_eq!(group.peek(member).unwrap().held(), vec![(2, b"e".to_vec())]);
    }
    // The leader stops; whoever is elected takes what the two hold.
    group.nodes[0] = None;
    for _ in 0..40 {
        for member in group.up() {
            group.act(&Op::Tick(member));
        }
        quiet(&mut group);
        if !group.leaders_now().is_empty() {
            break;
        }
    }
    assert_eq!(group.leaders_now().len(), 1);
    quiet(&mut group);
    assert_eq!(group.chosen[&2].3, b"e");
}

#[test]
fn while_the_group_changes_the_fast_quorum_commits_nothing() {
    let mut group = group(5);
    let joint = ConfChangeV2 {
        transition: ConfChangeTransition::Explicit as i32,
        changes: vec![ConfChangeSingle {
            change_type: ConfChangeType::RemoveNode as i32,
            node_id: 5,
        }],
        context: vec![],
    };
    assert_eq!(group.act(&Op::Change(1, joint))[0].accepted, Some(true));
    quiet(&mut group);
    let before = commit(&group, 1);
    assert!(fast(&mut group, 3, b"e"));
    carry(&mut group, |message| message.msg_type == FAST_PROPOSE);
    carry(&mut group, |message| message.msg_type == FAST_VOTE);
    assert_eq!(
        commit(&group, 1),
        before,
        "a fast quorum committed in a joint configuration"
    );
    quiet(&mut group);
    assert_eq!(commit(&group, 1), before + 1);
    assert_eq!(group.chosen[&(before + 1)].3, b"e");
    // Out of it, the fast quorum is of the four that stay.
    assert_eq!(
        group.act(&Op::Change(1, ConfChangeV2::default()))[0].accepted,
        Some(true)
    );
    quiet(&mut group);
    let before = commit(&group, 1);
    assert!(fast(&mut group, 3, b"f"));
    carry(&mut group, |message| message.msg_type == FAST_PROPOSE);
    carry(&mut group, |message| message.msg_type == FAST_VOTE);
    assert_eq!(commit(&group, 1), before + 1);
}

#[test]
fn what_may_not_go_by_the_fast_track_is_refused() {
    let mut group = group(3);
    let proposal = |entry: Entry| Message {
        msg_type: FAST_PROPOSE,
        from: 3,
        to: 2,
        entries: vec![entry],
        ..Message::default()
    };
    let change = Entry {
        entry_type: EntryType::EntryConfChangeV2 as i32,
        index: 2,
        data: vec![1],
        ..Entry::default()
    };
    let empty = Entry {
        index: 2,
        ..Entry::default()
    };
    let nowhere = Entry {
        data: vec![1],
        ..Entry::default()
    };
    for entry in [change, empty, nowhere] {
        group.net.push(proposal(entry));
        let reports = group.act(&Op::Deliver {
            at: group.net.len() - 1,
            keep: false,
            lose: false,
        });
        assert_eq!(reports[0].accepted, Some(false));
        assert!(group.peek(2).unwrap().held().is_empty());
    }
    // Below the log, and beyond the window above what is committed.
    for index in [1, 2 + 256] {
        group.net.push(proposal(Entry {
            index,
            data: vec![1],
            ..Entry::default()
        }));
        group.act(&Op::Deliver {
            at: group.net.len() - 1,
            keep: false,
            lose: false,
        });
        assert!(group.peek(2).unwrap().held().is_empty());
    }
    // A group that has no fast track holds nothing and proposes nothing.
    let mut plain: Cluster<New> = Cluster::new(3, &[1, 2, 3], Settings::focal(), 1);
    plain.act(&Op::Campaign(1));
    quiet(&mut plain);
    assert!(!fast(&mut plain, 2, b"e"));
    plain.net.push(proposal(Entry {
        index: 2,
        data: vec![1],
        ..Entry::default()
    }));
    plain.act(&Op::Deliver {
        at: 0,
        keep: false,
        lose: false,
    });
    assert!(plain.peek(2).unwrap().held().is_empty() && plain.net.is_empty());
}

/// One group with the fast track under the schedule of `seed`, which is
/// safe and settles; what the fast track did is added to `did`.
fn fast_schedule(
    settings: Settings,
    seed: u64,
    steps: u64,
    mix: &Mix,
    did: &mut focal_raft::FastStats,
) -> Cluster<New> {
    let mut group: Cluster<New> = Cluster::new(5, &[1, 2, 3, 4, 5], settings, seed);
    let mut rng = Seeded(seed);
    for _ in 0..steps {
        let op = group.choose(&mut rng, mix);
        // What a member did it forgets when it stops.
        if let Op::Restart(member) = op
            && let Some(node) = group.peek(member)
        {
            add(did, node.fast_stats());
        }
        group.act(&op);
    }
    assert!(group.settles(400), "seed {seed}: the group did not settle");
    for member in group.up() {
        add(did, group.peek(member).unwrap().fast_stats());
    }
    group
}

/// The schedule that found an election committing a second entry at an
/// index that held a committed one (seed 9843 of forty thousand from seed
/// 3,000, on the core before the audit's F43, F41 and F42, whose rules it
/// keeps here so that it runs as it was found). A leader committed an index
/// by a fast quorum of which two members held the entry beside their logs
/// and had told the leader nothing of their logs; a member whose log held
/// an entry of an older term at that index was later elected by them, kept
/// its own entry and committed it. A member's held entry now counts for a
/// fast commit only once the leader knows its log holds an entry of the
/// leader's term (27 §4.6; `focal_raft::track`).
#[test]
fn an_election_never_commits_a_second_entry_at_a_committed_index() {
    let found = Settings {
        round_each: true,
        max_inflight_bytes: u64::MAX,
        bare_answers: true,
        ..Settings::fast()
    };
    let mix = Mix {
        leader_leaves: true,
        fast: 60,
        ..Mix::everything()
    };
    let mut did = focal_raft::FastStats::default();
    let group = fast_schedule(found, 9843, 4_000, &mix, &mut did);
    // It went on past the index, and every member agreed on it.
    assert!(group.chosen.len() > 32, "{did:?}");
}

/// The schedule that found an election under a configuration a member had
/// not yet applied committing a second entry (seed 54104 of forty thousand
/// from seed 43,000, once the rule above was in). A leader whose
/// configuration had demoted a member committed an index by a fast quorum
/// of its voters; one of them had not committed the demotion and counted by
/// the voters before it: it was elected by members that held another entry
/// at that index, and took theirs. A fast quorum now counts only where it
/// is a fast quorum of every configuration a member that holds an entry of
/// the term may count by (27 §4.6; `focal_raft::track`).
#[test]
fn a_member_that_counts_by_the_configuration_before_commits_no_second_entry() {
    let mix = Mix {
        leader_leaves: true,
        fast: 60,
        ..Mix::everything()
    };
    let mut did = focal_raft::FastStats::default();
    let group = fast_schedule(Settings::fast(), 54104, 4_000, &mix, &mut did);
    assert!(group.chosen.len() > 11, "{did:?}");
}

#[test]
fn a_group_with_the_fast_track_is_safe_and_settles() {
    let seeds = count("FOCAL_RAFT_SEEDS", 96);
    let steps = count("FOCAL_RAFT_STEPS", 4_000);
    let first = count("FOCAL_RAFT_SEED", 0);
    let mix = Mix {
        leader_leaves: true,
        fast: 60,
        ..Mix::everything()
    };
    let (mut terms, mut committed) = (0, 0);
    let mut did = focal_raft::FastStats::default();
    for seed in first..first + seeds {
        let group = fast_schedule(Settings::fast(), seed, steps, &mix, &mut did);
        terms += group.leaders.len();
        committed += group.chosen.len();
    }
    println!("{seeds} schedules led {terms} terms and committed {committed} entries: {did:?}");
    assert!(terms as u64 > seeds && committed as u64 > seeds * 8);
    // A schedule that never takes the fast track says nothing of it.
    assert!(did.proposed > seeds * 8 && did.taken > seeds, "{did:?}");
    assert!(
        did.committed * 4 > seeds && did.recovered > seeds,
        "{did:?}"
    );
    assert!(did.displaced > seeds, "{did:?}");
}
