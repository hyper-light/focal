//! A group under schedules that lose, repeat and hold back its messages,
//! stop its members and change who they are. Whatever the schedule: no two
//! members commit different entries at one index, and no term has two
//! leaders. Once the network is whole and the members are up, the group
//! elects and commits.
//!
//! The group is of this core alone, and of both cores together, as a group
//! is while its nodes are replaced one by one.
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

use focal_raft::proto::{
    ConfChangeSingle, ConfChangeTransition, ConfChangeType, ConfChangeV2, Message, MessageType,
};
use support::{Cluster, Either, Mix, New, Old, Op, Replica, Seeded, Settings};

fn count(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn scheduled<R: Replica>(group: &mut Cluster<R>, seed: u64, steps: u64, mix: &Mix) {
    let mut rng = Seeded(seed);
    for _ in 0..steps {
        let op = group.choose(&mut rng, mix);
        group.act(&op);
    }
}

#[test]
fn a_group_of_this_core_is_safe_and_settles() {
    let seeds = count("FOCAL_RAFT_SEEDS", 96);
    let steps = count("FOCAL_RAFT_STEPS", 4_000);
    let first = count("FOCAL_RAFT_SEED", 0);
    let mix = Mix {
        leader_leaves: true,
        bursts: true,
        windows: true,
        ..Mix::everything()
    };
    let (mut terms, mut answered) = (0, 0);
    for seed in first..first + seeds {
        let mut group: Cluster<New> = Cluster::new(5, &[1, 2, 3], Settings::focal(), seed);
        group.stop_who_left = true;
        scheduled(&mut group, seed, steps, &mix);
        assert!(group.settles(400), "seed {seed}: the group did not settle");
        assert_eq!(
            group.deposed, 0,
            "seed {seed}: a member led a group it left"
        );
        terms += group.leaders.len();
        answered += group.answered;
    }
    // Every read answered saw what was committed before it was asked
    // (`Cluster::report`), asked alone or several at a time.
    assert!(answered > seeds, "{answered} reads were answered");
    println!("{seeds} schedules led {terms} terms");
    assert!(terms as u64 > seeds);
}

#[test]
fn a_group_of_both_cores_is_safe_and_settles() {
    let seeds = count("FOCAL_RAFT_SEEDS", 96);
    let steps = count("FOCAL_RAFT_STEPS", 4_000);
    let first = count("FOCAL_RAFT_SEED", 0);
    let mix = Mix {
        bursts: true,
        windows: true,
        ..Mix::everything()
    };
    let (mut old, mut new, mut deposed) = (0, 0, 0);
    for seed in first..first + seeds {
        let mut group: Cluster<Either> = Cluster::new(5, &[1, 2, 3], Settings::focal(), seed);
        // A member of `raft-rs` that a change leaves no voter leads on; its
        // owner stops it, as the shell's fence did when it unwound.
        group.stop_who_left = true;
        scheduled(&mut group, seed, steps, &mix);
        assert!(group.settles(400), "seed {seed}: the group did not settle");
        deposed += group.deposed;
        for leader in group.leaders.values() {
            if leader % 2 == 1 {
                old += 1;
            } else {
                new += 1;
            }
        }
    }
    println!(
        "{seeds} schedules: raft-rs led {old} terms and focal-raft {new}; {deposed} members \
         of raft-rs were stopped for leading a group they were no voter of"
    );
    assert!(old > 0 && new > 0);
}

fn quiet<R: Replica>(group: &mut Cluster<R>) {
    for _ in 0..10_000 {
        if group.net.is_empty() {
            return;
        }
        group.act(&Op::Deliver {
            at: 0,
            keep: false,
            lose: false,
        });
    }
    panic!("the network does not fall quiet");
}
fn elect<R: Replica>(group: &mut Cluster<R>, id: u64) {
    group.act(&Op::Campaign(id));
    quiet(group);
    assert_eq!(group.leaders_now(), vec![id], "member {id} was not elected");
}
fn ticks<R: Replica>(group: &mut Cluster<R>, members: &[u64], count: usize) {
    for _ in 0..count {
        for member in members {
            group.act(&Op::Tick(*member));
        }
        quiet(group);
    }
}
/// Delivers, and takes from the network, the messages `which` picks.
fn deliver<R: Replica>(group: &mut Cluster<R>, which: impl Fn(&Message) -> bool) -> usize {
    let mut delivered = 0;
    while let Some(at) = group.net.iter().position(&which) {
        group.act(&Op::Deliver {
            at,
            keep: false,
            lose: false,
        });
        delivered += 1;
    }
    delivered
}

/// A round of heartbeats confirms the reads asked before it left, and no
/// read asked after. A leader sends a round for one read; the answers are
/// held on the network; the leader is cut off, another is elected and
/// commits; the old leader, which has heard nothing, is asked a second
/// read; and then the answers to the first round arrive. They prove that it
/// led when the first read was asked, and nothing about when the second
/// was: the first is answered and the second is not — answered at the old
/// leader's commit it would miss what the group committed before it was
/// asked (`Cluster::report` checks every answer against that).
#[test]
fn a_round_confirms_no_read_asked_after_it_left() {
    let mut group: Cluster<New> = Cluster::new(3, &[1, 2, 3], Settings::focal(), 7);
    elect(&mut group, 1);
    group.act(&Op::Propose(1, b"before".to_vec()));
    quiet(&mut group);
    group.act(&Op::Read(1, b"first".to_vec()));
    let heartbeat = MessageType::MsgHeartbeat as i32;
    let answer = MessageType::MsgHeartbeatResponse as i32;
    assert_eq!(
        deliver(&mut group, |message| message.msg_type == heartbeat),
        2
    );
    // The answers wait on the network while the leader is cut off.
    let held: Vec<Message> = group
        .net
        .iter()
        .filter(|message| message.msg_type == answer && message.context == b"first")
        .cloned()
        .collect();
    assert_eq!(held.len(), 2);
    group.net.clear();
    separate(&mut group, 1);
    for _ in 0..64 {
        ticks(&mut group, &[2, 3], 1);
        if group.leaders_now().iter().any(|leader| *leader != 1) {
            break;
        }
    }
    let leader = group
        .leaders_now()
        .into_iter()
        .find(|leader| *leader != 1)
        .expect("the two that remain elect one of them");
    let committed = group.chosen.len();
    group.act(&Op::Propose(leader, b"after".to_vec()));
    quiet(&mut group);
    assert!(group.chosen.len() > committed);
    // The old leader still believes it leads, and is asked again.
    assert!(group.leaders_now().contains(&1));
    group.act(&Op::Read(1, b"second".to_vec()));
    group.net.clear();
    group.act(&Op::Heal);
    group.net.extend(held);
    assert_eq!(deliver(&mut group, |message| message.msg_type == answer), 2);
    // The first read was answered; the second waits for a round of its own,
    // which the members that follow another leader will not answer.
    assert_eq!(group.answered, 1);
    quiet(&mut group);
    assert!(group.settles(400));
    assert_eq!(group.answered, 1);
}

/// A member that may not campaign refuses no one for priority. Two voters;
/// the one of higher priority leads and removes itself. The other holds the
/// removal and has not heard it committed, so by its configuration it still
/// needs the first one's vote; the first, which applied the removal and
/// follows, could never be elected itself. Its priority judges nothing: it
/// votes, and the group goes on. (It refused, and the group had no leader
/// for good: found by a schedule of three thousand.)
#[test]
fn a_member_that_left_refuses_no_one_for_priority() {
    let mut group: Cluster<New> = Cluster::new(2, &[1, 2], Settings::focal(), 11);
    elect(&mut group, 2);
    group.act(&Op::Priority(2, 3));
    group.act(&Op::Propose(2, b"before".to_vec()));
    quiet(&mut group);
    group.act(&Op::Change(2, change(ConfChangeType::RemoveNode, 2)));
    // The removal reaches member 1 and its answer reaches member 2, which
    // commits it, applies it and follows, telling member 1 to campaign.
    // That reaches member 1, which takes a term for it; the commit, and the
    // vote member 1 then asks for, are lost. Member 1 is left a term ahead
    // of member 2, so that what member 2 answers from its own term member 1
    // does not hear.
    let append = MessageType::MsgAppend as i32;
    let answer = MessageType::MsgAppendResponse as i32;
    let campaign = MessageType::MsgTimeoutNow as i32;
    let to = |kind: i32, member: u64| {
        move |message: &Message| message.msg_type == kind && message.to == member
    };
    assert!(deliver(&mut group, to(append, 1)) > 0);
    assert!(deliver(&mut group, to(answer, 2)) > 0);
    assert_eq!(deliver(&mut group, to(campaign, 1)), 1);
    group.net.clear();
    assert!(group.leaders_now().is_empty());
    let left = group.peek(2).unwrap().view();
    let stayed = group.peek(1).unwrap().view();
    assert!(!left.promotable && stayed.promotable);
    assert!(left.commit > stayed.commit && left.last_index == stayed.last_index);
    assert!(stayed.term > left.term);
    // Member 1 is elected by member 2's vote, commits the removal and leads
    // alone.
    assert!(group.settles(400));
    assert_eq!(group.leaders_now(), vec![1]);
}

fn change(kind: ConfChangeType, member: u64) -> ConfChangeV2 {
    ConfChangeV2 {
        changes: vec![ConfChangeSingle {
            change_type: kind as i32,
            node_id: member,
        }],
        ..ConfChangeV2::default()
    }
}
fn separate<R: Replica>(group: &mut Cluster<R>, member: u64) {
    for other in group.ids() {
        if other != member {
            group.act(&Op::Block(member, other));
            group.act(&Op::Block(other, member));
        }
    }
}

#[test]
fn a_leader_a_change_leaves_no_voter_hands_the_group_over_and_follows() {
    for demoted in [false, true] {
        let mut group: Cluster<New> = Cluster::new(3, &[1, 2, 3], Settings::focal(), 7);
        elect(&mut group, 1);
        let term = group.peek(1).unwrap().view().term;
        let kind = if demoted {
            ConfChangeType::AddLearnerNode
        } else {
            ConfChangeType::RemoveNode
        };
        let reports = group.act(&Op::Change(1, change(kind, 1)));
        assert_eq!(reports[0].accepted, Some(true));
        // No tick passes: the group does not wait out an election timeout.
        quiet(&mut group);
        let view = group.peek(1).unwrap().view();
        assert_eq!(view.role, 0, "the member that left still leads");
        assert!(!view.promotable);
        let leaders = group.leaders_now();
        assert_eq!(leaders.len(), 1);
        let leader = leaders[0];
        assert!(leader == 2 || leader == 3);
        assert_eq!(group.peek(leader).unwrap().view().term, term + 1);
        let conf = group.stores[(leader - 1) as usize].0.borrow().conf.clone();
        assert_eq!(conf.voters, vec![2, 3]);
        assert_eq!(conf.learners, if demoted { vec![1] } else { vec![] });
        let reports = group.act(&Op::Propose(leader, b"after".to_vec()));
        assert_eq!(reports[0].accepted, Some(true));
        quiet(&mut group);
        let index = group.peek(leader).unwrap().view().last_index;
        for member in [2, 3] {
            assert_eq!(group.peek(member).unwrap().app().index, index);
        }
        // A learner is sent what is committed; one that is removed is not.
        let applied = group.peek(1).unwrap().app().index;
        assert_eq!(applied == index, demoted);
        // One that is no voter does not campaign, whoever asks.
        assert_eq!(group.act(&Op::Campaign(1))[0].accepted, Some(false));
        ticks(&mut group, &[1], 40);
        assert_eq!(group.peek(1).unwrap().view().role, 0);
        assert_eq!(group.leaders_now(), vec![leader]);
    }
}

#[test]
fn a_leader_leaves_a_joint_configuration_it_is_no_part_of_after() {
    let mut group: Cluster<New> = Cluster::new(5, &[1, 2, 3], Settings::focal(), 11);
    elect(&mut group, 1);
    let replace = ConfChangeV2 {
        transition: ConfChangeTransition::Auto as i32,
        changes: vec![
            ConfChangeSingle {
                change_type: ConfChangeType::AddNode as i32,
                node_id: 4,
            },
            ConfChangeSingle {
                change_type: ConfChangeType::AddNode as i32,
                node_id: 5,
            },
            ConfChangeSingle {
                change_type: ConfChangeType::RemoveNode as i32,
                node_id: 1,
            },
        ],
        context: vec![],
    };
    assert_eq!(group.act(&Op::Change(1, replace))[0].accepted, Some(true));
    quiet(&mut group);
    // It led the group through the joint configuration and out of it, and
    // then handed it over.
    let leaders = group.leaders_now();
    assert_eq!(leaders.len(), 1);
    assert_ne!(leaders[0], 1);
    let conf = group.stores[(leaders[0] - 1) as usize]
        .0
        .borrow()
        .conf
        .clone();
    assert_eq!(conf.voters, vec![2, 3, 4, 5]);
    assert!(conf.voters_outgoing.is_empty() && !conf.auto_leave);
    assert_eq!(
        group.act(&Op::Propose(leaders[0], b"after".to_vec()))[0].accepted,
        Some(true)
    );
    quiet(&mut group);
    let index = group.peek(leaders[0]).unwrap().view().last_index;
    for member in [2, 3, 4, 5] {
        assert_eq!(group.peek(member).unwrap().app().index, index);
    }
}

/// Two voters whose logs are equally long and end in different terms, the
/// third away. The one of higher priority holds the older log.
fn divided<R: Replica>(settings: Settings) -> Cluster<R> {
    let mut group: Cluster<R> = Cluster::new(3, &[1, 2, 3], settings, 3);
    elect(&mut group, 1);
    group.act(&Op::Propose(1, b"held by all".to_vec()));
    quiet(&mut group);
    separate(&mut group, 1);
    // An entry only the old leader holds.
    group.act(&Op::Propose(1, b"held by one".to_vec()));
    group.net.clear();
    // The others elect, and the new leader's first entry takes the index.
    ticks(&mut group, &[2, 3], 25);
    group.act(&Op::Campaign(3));
    quiet(&mut group);
    ticks(&mut group, &[2, 3], 25);
    let leader = *group
        .leaders_now()
        .iter()
        .find(|id| **id != 1)
        .expect("the two elect");
    let other = if leader == 2 { 3 } else { 2 };
    let (old, new) = (
        group.peek(1).unwrap().view(),
        group.peek(leader).unwrap().view(),
    );
    assert_eq!(old.last_index, new.last_index);
    assert!(new.term > old.term);
    // The follower goes away, and the leader stops and opens again.
    group.nodes[(other - 1) as usize] = None;
    group.act(&Op::Restart(leader));
    group.act(&Op::Heal);
    group.net.clear();
    // The old leader finds it leads no quorum.
    ticks(&mut group, &[1], 25);
    group.net.clear();
    assert!(group.leaders_now().is_empty());
    group.act(&Op::Priority(1, 3));
    group.act(&Op::Priority(leader, 1));
    group
}
fn elects<R: Replica>(group: &mut Cluster<R>) -> bool {
    let up = group.up();
    for _ in 0..400 {
        for member in &up {
            group.act(&Op::Tick(*member));
        }
        quiet(group);
        if !group.leaders_now().is_empty() {
            return true;
        }
    }
    false
}

#[test]
fn priority_yields_to_a_log_that_is_more_current() {
    // By length alone the two refuse each other for as long as the third
    // is away: the one for priority, the other for the log.
    let mut group: Cluster<New> = divided(Settings::shell());
    assert!(!elects(&mut group), "the rule of raft-rs elected");
    let mut group: Cluster<Old> = divided(Settings::shell());
    assert!(!elects(&mut group), "raft-rs elected");
    // A voter that could not be elected itself does not refuse.
    let mut group: Cluster<New> = divided(Settings::focal());
    assert!(elects(&mut group));
    let leader = group.leaders_now()[0];
    assert_ne!(leader, 1);
    assert_eq!(
        group.act(&Op::Propose(leader, b"led".to_vec()))[0].accepted,
        Some(true)
    );
    quiet(&mut group);
    assert_eq!(
        group.peek(1).unwrap().app(),
        group.peek(leader).unwrap().app()
    );
}

#[test]
fn priority_orders_an_election_and_never_judges_a_transfer() {
    let mut group: Cluster<New> = Cluster::new(3, &[1, 2, 3], Settings::focal(), 5);
    elect(&mut group, 1);
    group.act(&Op::Propose(1, b"x".to_vec()));
    quiet(&mut group);
    group.act(&Op::Priority(2, 3));
    group.act(&Op::Priority(3, 1));
    group.act(&Op::Priority(1, 1));
    // Without the leader's vote the one of higher priority decides.
    group.act(&Op::Block(1, 3));
    group.act(&Op::Block(3, 1));
    group.act(&Op::Transfer(1, 3));
    // The leader tells the member to campaign; it cannot reach it.
    assert!(group.leaders_now() == vec![1]);
    group.act(&Op::Heal);
    group.net.clear();
    group.act(&Op::Transfer(1, 2));
    group.act(&Op::Transfer(1, 3));
    let told: Vec<_> = group
        .net
        .iter()
        .filter(|message| message.msg_type == MessageType::MsgTimeoutNow as i32)
        .map(|message| message.to)
        .collect();
    assert_eq!(told, vec![2, 3]);
    // Only the second is delivered, and then the old leader is cut off.
    group.net.retain(|message| message.to == 3);
    group.act(&Op::Deliver {
        at: 0,
        keep: false,
        lose: false,
    });
    separate(&mut group, 1);
    group.act(&Op::Block(1, 2));
    quiet(&mut group);
    assert!(
        group.leaders_now().contains(&3),
        "a member of higher priority refused the transfer"
    );
    // An election of its own it would have lost to priority.
    let mut group: Cluster<New> = Cluster::new(3, &[1, 2, 3], Settings::focal(), 5);
    elect(&mut group, 1);
    group.act(&Op::Priority(2, 3));
    group.act(&Op::Priority(3, 1));
    separate(&mut group, 1);
    ticks(&mut group, &[2, 3], 9);
    group.act(&Op::Campaign(3));
    quiet(&mut group);
    assert!(!group.leaders_now().contains(&3));
    assert!(elects(&mut group));
    assert!(group.leaders_now().contains(&2));
}

#[test]
fn a_member_that_has_no_term_refuses_no_one_for_priority() {
    // `raft-rs` unwinds here: the refusal would bear no term.
    let mut group: Cluster<New> = Cluster::new(3, &[1, 2, 3], Settings::focal(), 9);
    for member in [1, 2, 3] {
        group.act(&Op::Priority(member, member as i64));
    }
    elect(&mut group, 1);
    assert_eq!(group.peek(3).unwrap().view().leader, 1);
}
