//! Raft's safety theorems on the durable node, stated as named tests over the
//! three-voter cluster and message pump the recovery tests use: Election
//! Safety (at most one leader per term), commitment reaching every follower,
//! the election restriction (a candidate with a stale log cannot win, so it
//! can never overwrite a committed entry), and Leader Completeness across a
//! partition with CheckQuorum retiring the stale leader. The recovery suite
//! proves durability; this file proves the protocol.
use crate::StateRole;
use crate::tests::{Cluster, config};
use std::collections::BTreeMap;

const ROUNDS: usize = 40;

/// Every node that currently believes it leads, as `(term, node)`.
fn leaders(cluster: &Cluster) -> Vec<(u64, u64)> {
    cluster
        .nodes
        .iter()
        .filter(|node| node.status().role == StateRole::Leader)
        .map(|node| {
            let status = node.status();
            (status.term, status.node_id)
        })
        .collect()
}
/// One randomized election-timer tick on every node, then deliver.
fn randomize_and_tick(cluster: &mut Cluster, isolated: Option<u64>) {
    for (i, node) in cluster.nodes.iter_mut().enumerate() {
        node.raw.raft.set_randomized_election_timeout(10 + i * 3);
        node.tick().unwrap();
    }
    cluster.pump(isolated);
}
fn elect(cluster: &mut Cluster, node: usize) {
    cluster.nodes[node].campaign().unwrap();
    cluster.pump(None);
    assert_eq!(cluster.nodes[node].status().role, StateRole::Leader);
}

/// Election Safety: a campaign yields exactly one leader that every voter
/// agrees on, and across many randomized timer rounds no term ever sees two
/// leaders.
#[test]
fn an_election_produces_exactly_one_leader_and_never_two_in_a_term() {
    let mut cluster = Cluster::new();
    elect(&mut cluster, 0);
    let elected = leaders(&cluster);
    assert_eq!(elected.len(), 1, "exactly one leader: {elected:?}");
    let (term, leader) = elected[0];
    assert_eq!(leader, 1);
    for node in &cluster.nodes {
        let status = node.status();
        assert_eq!(status.leader_id, leader, "every voter names the leader");
        assert_eq!(status.term, term, "every voter is at the leader's term");
    }
    let mut leader_of_term: BTreeMap<u64, u64> = BTreeMap::new();
    for _ in 0..ROUNDS {
        randomize_and_tick(&mut cluster, None);
        let now = leaders(&cluster);
        assert!(now.len() <= 1, "two simultaneous leaders: {now:?}");
        for (term, node) in now {
            if let Some(previous) = leader_of_term.insert(term, node) {
                assert_eq!(
                    previous, node,
                    "term {term} had two leaders: {previous}, {node}"
                );
            }
        }
    }
}

/// A command the leader appends commits once a majority holds it, and then
/// every follower's commit index and applied log carry it.
#[test]
fn a_replicated_entry_commits_at_the_leader_and_reaches_every_follower() {
    let mut cluster = Cluster::new();
    elect(&mut cluster, 0);
    cluster.nodes[0].propose(b"entry".to_vec()).unwrap();
    cluster.pump(None);
    let committed = cluster.nodes[0].status().committed_index;
    assert!(committed > 0, "the entry committed at the leader");
    for (i, node) in cluster.nodes.iter().enumerate() {
        assert_eq!(
            node.status().committed_index,
            committed,
            "node {} agrees on the commit index",
            i + 1
        );
        assert_eq!(
            cluster.applied[i],
            vec![b"entry".to_vec()],
            "node {} applied the replicated entry",
            i + 1
        );
    }
}

/// The election restriction: a candidate whose log is behind the majority's
/// cannot win, so it can never become a leader that would overwrite a
/// committed entry. Node 3 misses a commit, campaigns, and is refused; the
/// up-to-date leader keeps its term and node 3 catches up to the entry.
#[test]
fn a_behind_candidate_cannot_win_an_election() {
    let mut cluster = Cluster::new();
    elect(&mut cluster, 0);
    cluster.nodes[0].propose(b"ahead".to_vec()).unwrap();
    cluster.pump(Some(3));
    assert_eq!(cluster.applied[0], vec![b"ahead".to_vec()]);
    assert_eq!(cluster.applied[1], vec![b"ahead".to_vec()]);
    assert!(cluster.applied[2].is_empty(), "node 3 missed the entry");
    let term = cluster.nodes[0].status().term;
    cluster.nodes[2].campaign().unwrap();
    cluster.pump(None);
    assert_ne!(
        cluster.nodes[2].status().role,
        StateRole::Leader,
        "a candidate with a stale log must not win"
    );
    assert_eq!(
        leaders(&cluster),
        vec![(term, 1)],
        "the up-to-date leader keeps its term"
    );
    for _ in 0..4 {
        randomize_and_tick(&mut cluster, None);
    }
    for (i, applied) in cluster.applied.iter().enumerate() {
        assert_eq!(
            applied,
            &vec![b"ahead".to_vec()],
            "node {} holds the committed entry, never an overwrite",
            i + 1
        );
    }
}

/// Leader Completeness across a leader change, with CheckQuorum: a committed
/// entry survives the old leader's partition; the survivors elect a strictly
/// higher term; the isolated leader, contacting no majority, steps down
/// rather than lingering as a stale leader; and after the heal every node
/// carries the entry plus what the new leader committed.
#[test]
fn a_partitioned_stale_leader_steps_down_and_a_committed_entry_survives_the_change() {
    let mut cluster = Cluster::new();
    elect(&mut cluster, 0);
    cluster.nodes[0].propose(b"committed".to_vec()).unwrap();
    cluster.pump(None);
    for applied in &cluster.applied {
        assert_eq!(applied, &vec![b"committed".to_vec()]);
    }
    let old_term = cluster.nodes[0].status().term;
    let mut new_leader = None;
    for _ in 0..60 {
        randomize_and_tick(&mut cluster, Some(1));
        if let Some(i) = (1..3).find(|i| cluster.nodes[*i].status().role == StateRole::Leader) {
            new_leader = Some(i);
            break;
        }
    }
    let new_leader = new_leader.expect("the survivors elect a leader");
    let new_term = cluster.nodes[new_leader].status().term;
    assert!(
        new_term > old_term,
        "the survivors lead at a strictly higher term"
    );
    assert_eq!(
        (1..3)
            .filter(|i| cluster.nodes[*i].status().role == StateRole::Leader)
            .count(),
        1,
        "one leader among the survivors"
    );
    // CheckQuorum: without contact from a majority the partitioned leader
    // steps down within a few election timeouts.
    let election_tick = config(1).election_tick;
    for _ in 0..(3 * election_tick) {
        if cluster.nodes[0].status().role != StateRole::Leader {
            break;
        }
        cluster.nodes[0].tick().unwrap();
        cluster.pump(Some(1));
    }
    assert_ne!(
        cluster.nodes[0].status().role,
        StateRole::Leader,
        "the partitioned leader steps down without quorum contact"
    );
    cluster.nodes[new_leader]
        .propose(b"after".to_vec())
        .unwrap();
    cluster.pump(Some(1));
    cluster.pump(None);
    for _ in 0..4 {
        randomize_and_tick(&mut cluster, None);
    }
    let expected = vec![b"committed".to_vec(), b"after".to_vec()];
    for (i, applied) in cluster.applied.iter().enumerate() {
        assert_eq!(
            applied,
            &expected,
            "node {} carries the committed entry across the leader change",
            i + 1
        );
    }
    assert!(
        cluster.nodes[0].status().term >= new_term,
        "the old leader adopts the higher term on heal"
    );
    assert_eq!(leaders(&cluster).len(), 1, "one leader after the heal");
}

/// PreVote: a node that was partitioned away and kept timing out rejoins
/// without disturbing the healthy leader. Its pre-votes are refused by peers
/// that still hear the leader, so it never raises the term, and the leader
/// keeps leading at the term it was elected in.
#[test]
fn a_rejoining_partitioned_node_does_not_disrupt_a_healthy_leader() {
    let mut cluster = Cluster::new();
    elect(&mut cluster, 0);
    let term = cluster.nodes[0].status().term;
    cluster.nodes[0].propose(b"steady".to_vec()).unwrap();
    cluster.pump(None);
    // Node 3 is cut off and times out many times over; without PreVote each
    // timeout would raise its term.
    let election_tick = config(3).election_tick;
    for _ in 0..(6 * election_tick) {
        cluster.nodes[2].tick().unwrap();
        cluster.nodes[0].tick().unwrap();
        cluster.nodes[1].tick().unwrap();
        cluster.pump(Some(3));
    }
    assert_eq!(
        cluster.nodes[2].status().term,
        term,
        "a pre-candidate never raises its own term"
    );
    assert_eq!(leaders(&cluster), vec![(term, 1)]);
    // The partition heals: node 3's pre-votes are refused, node 1 stays the
    // leader at the same term, and node 3 catches up.
    for _ in 0..(3 * election_tick) {
        randomize_and_tick(&mut cluster, None);
    }
    assert_eq!(
        leaders(&cluster),
        vec![(term, 1)],
        "the healthy leader is undisturbed by the rejoining node"
    );
    for node in &cluster.nodes {
        assert_eq!(node.status().term, term);
        assert_eq!(node.status().leader_id, 1);
    }
    assert_eq!(cluster.applied[2], vec![b"steady".to_vec()]);
}

/// Joint consensus: while a membership change is in its joint phase an entry
/// commits only with majorities of both the old and the new configuration.
/// Leaving {1,2,3} for {1,2}: node 2 is a majority-critical member of the new
/// configuration, so with node 2 cut off nothing commits even though {1,3}
/// is a majority of the old one; once node 2 is back the entry commits, and
/// after the explicit leave the configuration is {1,2}.
#[test]
fn a_joint_change_commits_only_with_both_configurations() {
    use crate::{ConfChangeSingle, ConfChangeTransition, ConfChangeType, ConfChangeV2};
    let mut cluster = Cluster::new();
    elect(&mut cluster, 0);
    let mut leave = ConfChangeV2::default();
    leave.set_transition(ConfChangeTransition::Explicit);
    let mut remove = ConfChangeSingle::default();
    remove.set_change_type(ConfChangeType::RemoveNode);
    remove.node_id = 3;
    leave.changes.push(remove);
    cluster.nodes[0].propose_conf_change(leave).unwrap();
    cluster.pump(None);
    let mut status = cluster.nodes[0].status();
    status.voters.sort_unstable();
    assert_eq!(
        status.voters,
        vec![1, 2],
        "the joint configuration is entered"
    );
    assert!(
        !cluster.nodes[0]
            .raw
            .store()
            .conf_state
            .voters_outgoing
            .is_empty(),
        "the outgoing configuration is still in force"
    );
    // Node 2 is cut off: {1,3} is a majority of the old configuration but
    // not of the new one, so the entry must not commit.
    cluster.nodes[0].propose(b"joint".to_vec()).unwrap();
    cluster.pump(Some(2));
    for _ in 0..4 {
        cluster.nodes[0].tick().unwrap();
        cluster.pump(Some(2));
    }
    assert!(
        cluster.applied[0].is_empty(),
        "no commit without a majority of the incoming configuration"
    );
    // Node 2 returns: both majorities hold and the entry commits.
    cluster.pump(None);
    for _ in 0..4 {
        cluster.nodes[0].tick().unwrap();
        cluster.pump(None);
    }
    assert_eq!(cluster.applied[0], vec![b"joint".to_vec()]);
    assert_eq!(cluster.applied[1], vec![b"joint".to_vec()]);
    // Leaving the joint phase is its own committed change. The departed
    // node is cut off from here: the harness delivers every message, and a
    // reply from a node the leader no longer tracks is refused by Raft.
    cluster.nodes[0]
        .propose_conf_change(ConfChangeV2::default())
        .unwrap();
    cluster.pump(Some(3));
    let mut status = cluster.nodes[0].status();
    status.voters.sort_unstable();
    assert_eq!(status.voters, vec![1, 2]);
    assert!(
        cluster.nodes[0]
            .raw
            .store()
            .conf_state
            .voters_outgoing
            .is_empty(),
        "the joint phase is left"
    );
    assert_eq!(
        cluster.nodes[0].status().role,
        StateRole::Leader,
        "the leader leads the new configuration at the same term"
    );
    assert_eq!(cluster.nodes[0].status().term, status.term);
}

/// Every live node ticks once with the given election timeouts, then the
/// messages are delivered; repeated until `done` or the rounds run out.
fn run_until(
    cluster: &mut Cluster,
    isolated: u64,
    timeouts: [usize; 3],
    mut done: impl FnMut(&Cluster) -> bool,
    mut each: impl FnMut(&Cluster),
) -> bool {
    for _ in 0..ROUNDS * 4 {
        if done(cluster) {
            return true;
        }
        for (i, node) in cluster.nodes.iter_mut().enumerate() {
            if i as u64 + 1 == isolated {
                continue;
            }
            node.raw.raft.set_randomized_election_timeout(timeouts[i]);
            node.tick().unwrap();
        }
        cluster.pump(Some(isolated));
        each(cluster);
    }
    done(cluster)
}
fn prioritize(cluster: &mut Cluster, priorities: [i64; 3]) {
    for (node, priority) in cluster.nodes.iter_mut().zip(priorities) {
        node.set_priority(priority).unwrap();
        assert_eq!(node.priority(), priority);
        assert_eq!(
            node.effective_priority(),
            priority,
            "a node with a term judges votes by its priority"
        );
    }
}
fn leads(cluster: &Cluster, node: u64, except: u64) -> bool {
    leaders(cluster)
        .iter()
        .any(|(_, leader)| *leader == node && *leader != except)
}

/// A node that has no term yet keeps the neutral priority, so a group's
/// first election is decided by timeouts and a pre-vote is never rejected
/// from term zero (which the library asserts against).
#[test]
fn a_node_without_a_term_defers_its_priority_and_the_first_election_completes() {
    let mut cluster = Cluster::new();
    for (node, priority) in cluster.nodes.iter_mut().zip([3, 2, 1]) {
        node.set_priority(priority).unwrap();
        assert_eq!(node.priority(), priority);
        assert_eq!(node.effective_priority(), 0);
    }
    assert!(cluster.nodes[0].set_priority(-1).is_err());
    // The member of lowest priority campaigns first and is not refused.
    elect(&mut cluster, 2);
    for (node, priority) in cluster.nodes.iter().zip([3, 2, 1]) {
        assert_eq!(node.effective_priority(), priority);
    }
}

/// Priority elections (27 §5): with the leader gone and logs equally
/// current, the member that times out first is refused by a voter of higher
/// priority and never leads; the member of highest priority wins.
#[test]
fn a_lower_priority_candidate_is_refused_and_the_highest_priority_member_wins() {
    let mut cluster = Cluster::new();
    elect(&mut cluster, 2);
    prioritize(&mut cluster, [3, 2, 1]);
    // Node 3, the leader, is cut off. Node 2 times out long before node 1.
    let won = run_until(
        &mut cluster,
        3,
        [19, 10, 10],
        |cluster| leads(cluster, 1, 3),
        |cluster| {
            assert!(
                !leads(cluster, 2, 3),
                "a lower priority candidate took the group"
            )
        },
    );
    assert!(won, "the highest priority member never won");
}

/// Priority is a preference, never a dependency: with the highest priority
/// member gone, the rest elect the next, and committed entries survive.
#[test]
fn the_group_elects_the_next_priority_when_the_highest_is_gone() {
    let mut cluster = Cluster::new();
    elect(&mut cluster, 0);
    prioritize(&mut cluster, [3, 2, 1]);
    cluster.nodes[0].propose(b"before".to_vec()).unwrap();
    cluster.pump(None);
    let committed = cluster.nodes[0].status().committed_index;
    // Node 1 is cut off. Node 3 times out first and is refused by node 2.
    let won = run_until(
        &mut cluster,
        1,
        [10, 19, 10],
        |cluster| leads(cluster, 2, 1),
        |cluster| {
            assert!(
                !leads(cluster, 3, 1),
                "the lowest priority member won over a higher live one"
            )
        },
    );
    assert!(won, "the next priority never won");
    assert!(cluster.nodes[1].status().committed_index >= committed);
    cluster.nodes[1].propose(b"after".to_vec()).unwrap();
    cluster.pump(Some(1));
    assert!(cluster.applied[2].iter().any(|entry| entry == b"before"));
    assert!(cluster.applied[2].iter().any(|entry| entry == b"after"));
}

/// Priority never outranks the log: a member of lower priority whose log is
/// strictly longer is granted the vote by a voter of higher priority, and a
/// member of higher priority with a shorter log cannot win.
#[test]
fn a_longer_log_wins_the_vote_over_a_higher_priority() {
    let mut cluster = Cluster::new();
    elect(&mut cluster, 0);
    prioritize(&mut cluster, [1, 2, 3]);
    // Node 3 misses an entry the other two commit.
    cluster.nodes[0].propose(b"ahead".to_vec()).unwrap();
    cluster.pump(Some(3));
    let ahead = cluster.nodes[0].status().committed_index;
    assert!(cluster.nodes[2].status().committed_index < ahead);
    // The leader is cut off. Node 3 has the highest priority and the shorter
    // log and times out first; node 2 has the longer log.
    let won = run_until(
        &mut cluster,
        1,
        [10, 19, 10],
        |cluster| leads(cluster, 2, 1),
        |cluster| assert!(!leads(cluster, 3, 1), "a shorter log won on priority"),
    );
    assert!(won, "the longer log never won");
    cluster.pump(Some(1));
    assert!(cluster.nodes[2].status().committed_index >= ahead);
    assert!(cluster.applied[2].iter().any(|entry| entry == b"ahead"));
}
