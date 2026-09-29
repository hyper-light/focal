//! Where many logs are led (27 §5, stage F).
//!
//! A node that hosts a copy of many sessions need not lead them all. The
//! preferred leader of a session is part of its committed placement, so how
//! many sessions prefer each node is a fact of the directory, and the
//! planner spreads them by it: deterministically, from committed state, and
//! never from who happens to lead at the moment.
//!
//! A preferred leader is kept unless moving it helps. Moving one session
//! from a node that leads `a` to one that leads `b` leaves them at `a - 1`
//! and `b + 1`, which is no better unless `b + 2 <= a`
//! ([`LEADER_SLACK`]). Every move by that rule lowers the sum of the
//! squares of what the nodes lead, by `2 (a - b - 1)` and so by two at
//! least: moves end, and leadership comes to rest however the sessions
//! share their nodes.
use crate::*;
use focal_model::LedgerId;
use std::collections::BTreeMap;

/// How many more sessions a node must lead than another before one of them
/// moves there.
pub const LEADER_SLACK: u64 = 2;

/// Whether a session led where `here` are is better led where `there` are.
fn helps(here: u64, there: u64) -> bool {
    here.checked_sub(LEADER_SLACK)
        .is_some_and(|floor| there <= floor)
}

/// What the planner knows of leadership when it places one session.
#[derive(Debug, Clone, Copy)]
pub struct Leading<'a> {
    /// Sessions that prefer each node as their leader, this session among
    /// them where it has one.
    pub counts: &'a BTreeMap<u64, u64>,
    /// Who this session prefers now.
    pub current: Option<u64>,
}
impl Leading<'_> {
    fn led_by(&self, node: u64) -> u64 {
        self.counts.get(&node).copied().unwrap_or(0)
    }
    /// The preferred leader among `candidates`, which are given in the
    /// order they are preferred by everything else. None for no candidate.
    pub fn choose(&self, candidates: impl IntoIterator<Item = u64>) -> Option<u64> {
        let mut kept = false;
        // The least led, and the first of them.
        let mut least: Option<(u64, u64)> = None;
        for candidate in candidates {
            if Some(candidate) == self.current {
                kept = true;
                continue;
            }
            let led = self.led_by(candidate);
            if least.is_none_or(|(_, fewest)| led < fewest) {
                least = Some((candidate, led));
            }
        }
        match (self.current, least) {
            (Some(current), Some((other, led))) if kept => {
                if helps(self.led_by(current), led) {
                    Some(other)
                } else {
                    Some(current)
                }
            }
            (Some(current), None) if kept => Some(current),
            (_, least) => least.map(|(node, _)| node),
        }
    }
}

/// How many sessions prefer each node as their leader: by the placement a
/// session moves to where it is moving, and by its active one otherwise.
/// One row per node that is preferred, so no more rows than sessions.
pub fn leading(sessions: &BTreeMap<LedgerId, SessionDescriptor>) -> BTreeMap<u64, u64> {
    let mut counts = BTreeMap::new();
    for session in sessions.values() {
        let placement = session
            .pending
            .as_ref()
            .map_or(&session.active.placement, |plan| &plan.desired.placement);
        let count: &mut u64 = counts.entry(placement.preferred_leader).or_insert(0);
        *count = count.saturating_add(1);
    }
    counts
}

/// The placement `session` should move to so that another of its voters is
/// its preferred leader, where that helps ([`LEADER_SLACK`]); the members
/// stay who they are. None while the session is moving or leaves copies
/// behind, and where no voter that could lead is led less.
pub fn leader_move(
    session: &SessionDescriptor,
    nodes: &BTreeMap<u64, NodeRecord>,
    counts: &BTreeMap<u64, u64>,
    max_members: usize,
) -> Option<PlacementSpec> {
    if session.pending.is_some() || !session.retiring.is_empty() {
        return None;
    }
    let policy = &session.active.policy;
    let placement = &session.active.placement;
    let current = placement.preferred_leader;
    // Where the session is led now, where that is known: a voter of the
    // same zone is as near to who the session serves.
    let zone = |node: &NodeRecord| {
        Some((node.enrollment.region, node.enrollment.zone))
            .filter(|(region, zone)| region.0 != [0; 16] && zone.0 != [0; 16])
    };
    let home = nodes.get(&current).and_then(zone);
    // Voters are few (`max_members`); of the least led, one in the zone the
    // session is led in, and of those the least loaded, is found without
    // ordering them.
    let mut chosen: Option<(u64, (u64, bool, u64))> = None;
    for (voter, generation) in &placement.voters {
        if *voter == current {
            continue;
        }
        let Some(node) = nodes.get(voter) else {
            continue;
        };
        let Some(load) = node.load else {
            continue;
        };
        if !node.enrollment.eligible
            || !node.is_alive()
            || node.enrollment.generation != *generation
            || load.generation != *generation
            || (!policy.home_regions.is_empty()
                && !policy.home_regions.contains(&node.enrollment.region))
        {
            continue;
        }
        let led = counts.get(voter).copied().unwrap_or(0);
        let away = home.is_none() || zone(node) != home;
        let order = (led, away, load.active_weight);
        if chosen.is_none_or(|(_, least)| order < least) {
            chosen = Some((*voter, order));
        }
    }
    let (target, (led, _, _)) = chosen?;
    if !helps(counts.get(&current).copied().unwrap_or(0), led) {
        return None;
    }
    let mut desired = session.active.clone();
    desired.placement.preferred_leader = target;
    verify_placement(&desired, nodes, max_members).ok()?;
    Some(desired)
}
