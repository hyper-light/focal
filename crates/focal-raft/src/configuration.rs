//! Who is a member, and how that changes (Ongaro's thesis §4).
//!
//! A configuration names the voters, the learners, and during a joint
//! change the outgoing voters whose majority is still needed. A change is
//! either **simple**, which moves at most one voter and is safe because any
//! two majorities of configurations one voter apart intersect, or **joint**,
//! which moves any number by passing through a configuration that needs
//! the majority of both the old voters and the new.
use crate::{MAX_MEMBERS, NodeId};

#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub enum ConfigurationError {
    #[error("a member's identity is zero")]
    ZeroMember,
    #[error("a configuration names at most {MAX_MEMBERS} members")]
    TooManyMembers,
    #[error("a configuration has no voter")]
    NoVoter,
    #[error("a member is named twice, or as a voter and a learner at once")]
    Overlap,
    #[error("the configuration is joint, and only leaving it may follow")]
    AlreadyJoint,
    #[error("the configuration is not joint")]
    NotJoint,
    #[error("a simple change moves at most one voter; more need a joint change")]
    NotSimple,
    #[error("no room for the configuration")]
    Capacity,
}

/// One step of a change.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Change {
    /// Make `0` a voter; a learner that is promoted stops being a learner.
    AddVoter(NodeId),
    /// Make `0` a learner; a voter that is demoted stops voting once the
    /// joint configuration is left.
    AddLearner(NodeId),
    /// `0` is no member any more.
    Remove(NodeId),
}

/// A configuration as a change made it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Changed {
    pub configuration: Configuration,
    /// The members the change removed and added again, in order.
    pub renewed: Vec<NodeId>,
}

/// Sorted, distinct sets.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Configuration {
    voters: Vec<NodeId>,
    /// The voters before a joint change; empty when the configuration is
    /// not joint.
    outgoing: Vec<NodeId>,
    learners: Vec<NodeId>,
    /// Outgoing voters that become learners when the joint configuration is
    /// left: until then they vote, and a member never votes and learns at
    /// once.
    learners_next: Vec<NodeId>,
    /// Whether the leader leaves the joint configuration by itself once it
    /// has committed it.
    auto_leave: bool,
}

fn sorted(mut members: Vec<NodeId>) -> Vec<NodeId> {
    members.sort_unstable();
    members
}
fn distinct(members: &[NodeId]) -> bool {
    members.windows(2).all(|pair| match pair {
        [left, right] => left < right,
        _ => true,
    })
}
fn holds(members: &[NodeId], member: NodeId) -> bool {
    members.binary_search(&member).is_ok()
}
fn insert(members: &mut Vec<NodeId>, member: NodeId) -> Result<(), ConfigurationError> {
    if let Err(position) = members.binary_search(&member) {
        members
            .try_reserve(1)
            .map_err(|_| ConfigurationError::Capacity)?;
        members.insert(position, member);
    }
    Ok(())
}
fn remove(members: &mut Vec<NodeId>, member: NodeId) {
    if let Ok(position) = members.binary_search(&member) {
        members.remove(position);
    }
}
fn copy(members: &[NodeId]) -> Result<Vec<NodeId>, ConfigurationError> {
    let mut copied = Vec::new();
    copied
        .try_reserve_exact(members.len())
        .map_err(|_| ConfigurationError::Capacity)?;
    copied.extend_from_slice(members);
    Ok(copied)
}

impl Configuration {
    /// A configuration that is not joint.
    pub fn new(voters: Vec<NodeId>, learners: Vec<NodeId>) -> Result<Self, ConfigurationError> {
        Self::from_parts(voters, Vec::new(), learners, Vec::new(), false)
    }
    /// Any configuration, as a snapshot or a recovered log states it.
    pub fn from_parts(
        voters: Vec<NodeId>,
        outgoing: Vec<NodeId>,
        learners: Vec<NodeId>,
        learners_next: Vec<NodeId>,
        auto_leave: bool,
    ) -> Result<Self, ConfigurationError> {
        let configuration = Self {
            voters: sorted(voters),
            outgoing: sorted(outgoing),
            learners: sorted(learners),
            learners_next: sorted(learners_next),
            auto_leave,
        };
        configuration.validate()?;
        Ok(configuration)
    }
    fn try_clone(&self) -> Result<Self, ConfigurationError> {
        Ok(Self {
            voters: copy(&self.voters)?,
            outgoing: copy(&self.outgoing)?,
            learners: copy(&self.learners)?,
            learners_next: copy(&self.learners_next)?,
            auto_leave: self.auto_leave,
        })
    }
    pub fn validate(&self) -> Result<(), ConfigurationError> {
        let sets = [
            &self.voters,
            &self.outgoing,
            &self.learners,
            &self.learners_next,
        ];
        let members = sets
            .iter()
            .fold(0usize, |sum, set| sum.saturating_add(set.len()));
        if members > MAX_MEMBERS.saturating_mul(2) || self.members().count() > MAX_MEMBERS {
            return Err(ConfigurationError::TooManyMembers);
        }
        if sets.iter().any(|set| set.first() == Some(&0)) {
            return Err(ConfigurationError::ZeroMember);
        }
        if sets.iter().any(|set| !distinct(set)) {
            return Err(ConfigurationError::Overlap);
        }
        if self.voters.is_empty() {
            return Err(ConfigurationError::NoVoter);
        }
        // A learner never votes, in either half.
        if self
            .learners
            .iter()
            .any(|learner| holds(&self.voters, *learner) || holds(&self.outgoing, *learner))
        {
            return Err(ConfigurationError::Overlap);
        }
        // One that will learn is an outgoing voter that the incoming
        // configuration does not keep, and no learner yet.
        if self.learners_next.iter().any(|next| {
            !holds(&self.outgoing, *next)
                || holds(&self.voters, *next)
                || holds(&self.learners, *next)
        }) {
            return Err(ConfigurationError::Overlap);
        }
        if self.outgoing.is_empty() && (!self.learners_next.is_empty() || self.auto_leave) {
            return Err(ConfigurationError::NotJoint);
        }
        Ok(())
    }
    pub fn voters(&self) -> &[NodeId] {
        &self.voters
    }
    pub fn outgoing(&self) -> &[NodeId] {
        &self.outgoing
    }
    pub fn learners(&self) -> &[NodeId] {
        &self.learners
    }
    pub fn learners_next(&self) -> &[NodeId] {
        &self.learners_next
    }
    pub fn auto_leave(&self) -> bool {
        self.auto_leave
    }
    pub fn is_joint(&self) -> bool {
        !self.outgoing.is_empty()
    }
    /// Whether `member` votes, in either half of a joint configuration.
    pub fn votes(&self, member: NodeId) -> bool {
        holds(&self.voters, member) || holds(&self.outgoing, member)
    }
    /// Whether `member` learns, now or once the joint configuration is left.
    pub fn learns(&self, member: NodeId) -> bool {
        holds(&self.learners, member) || holds(&self.learners_next, member)
    }
    pub fn contains(&self, member: NodeId) -> bool {
        self.votes(member) || self.learns(member)
    }
    /// Every member once, in order: whom a leader replicates to.
    pub fn members(&self) -> impl Iterator<Item = NodeId> + '_ {
        let mut sets = [
            self.voters.iter().peekable(),
            self.outgoing.iter().peekable(),
            self.learners.iter().peekable(),
            self.learners_next.iter().peekable(),
        ];
        let mut last = 0;
        std::iter::from_fn(move || {
            // The least member above the last one given, across the sets.
            let mut least: Option<NodeId> = None;
            for set in &mut sets {
                while set.peek().is_some_and(|member| **member <= last) {
                    set.next();
                }
                if let Some(member) = set.peek() {
                    least = Some(least.map_or(**member, |least| least.min(**member)));
                }
            }
            last = least?;
            Some(last)
        })
    }
    /// One step of a change. `renewed` names, in order, the members the
    /// change removed and then added again: what was known of such a
    /// member is forgotten, for the one added may be another.
    fn change(
        &mut self,
        change: Change,
        renewed: &mut Vec<NodeId>,
    ) -> Result<(), ConfigurationError> {
        match change {
            Change::AddVoter(member) => {
                if member == 0 {
                    return Err(ConfigurationError::ZeroMember);
                }
                remove(&mut self.learners, member);
                remove(&mut self.learners_next, member);
                insert(&mut self.voters, member)
            }
            Change::AddLearner(member) => {
                if member == 0 {
                    return Err(ConfigurationError::ZeroMember);
                }
                if holds(&self.learners, member) {
                    return Ok(());
                }
                remove(&mut self.voters, member);
                // Still a voter of the outgoing half: it learns once that
                // half is gone.
                if holds(&self.outgoing, member) {
                    insert(&mut self.learners_next, member)
                } else {
                    remove(&mut self.learners_next, member);
                    insert(&mut self.learners, member)
                }
            }
            Change::Remove(member) => {
                if member == 0 {
                    return Err(ConfigurationError::ZeroMember);
                }
                let was = self.contains(member);
                remove(&mut self.voters, member);
                remove(&mut self.learners, member);
                remove(&mut self.learners_next, member);
                // One the outgoing half still counts is a member yet.
                if was && !self.contains(member) {
                    insert(renewed, member)?;
                }
                Ok(())
            }
        }
    }
    fn changes(&mut self, changes: &[Change]) -> Result<Vec<NodeId>, ConfigurationError> {
        let mut renewed = Vec::new();
        for change in changes {
            self.change(*change, &mut renewed)?;
        }
        // Removed and not added again is not renewed: it is gone.
        renewed.retain(|member| self.contains(*member));
        Ok(renewed)
    }
    /// The configuration after a simple change: at most one voter moves.
    pub fn simple(&self, changes: &[Change]) -> Result<Changed, ConfigurationError> {
        if self.is_joint() {
            return Err(ConfigurationError::AlreadyJoint);
        }
        let mut next = self.try_clone()?;
        let renewed = next.changes(changes)?;
        let moved = self
            .voters
            .iter()
            .filter(|voter| !holds(&next.voters, **voter))
            .count()
            .saturating_add(
                next.voters
                    .iter()
                    .filter(|voter| !holds(&self.voters, **voter))
                    .count(),
            );
        if moved > 1 {
            return Err(ConfigurationError::NotSimple);
        }
        next.validate()?;
        Ok(Changed {
            configuration: next,
            renewed,
        })
    }
    /// The joint configuration a change passes through: the voters as they
    /// are become the outgoing half, and the changes make the incoming one.
    pub fn enter_joint(
        &self,
        auto_leave: bool,
        changes: &[Change],
    ) -> Result<Changed, ConfigurationError> {
        if self.is_joint() {
            return Err(ConfigurationError::AlreadyJoint);
        }
        let mut next = self.try_clone()?;
        next.outgoing = copy(&self.voters)?;
        let renewed = next.changes(changes)?;
        next.auto_leave = auto_leave;
        next.validate()?;
        Ok(Changed {
            configuration: next,
            renewed,
        })
    }
    /// The configuration after the joint one: the incoming half alone.
    pub fn leave_joint(&self) -> Result<Self, ConfigurationError> {
        if !self.is_joint() {
            return Err(ConfigurationError::NotJoint);
        }
        let mut next = self.try_clone()?;
        for learner in &self.learners_next {
            insert(&mut next.learners, *learner)?;
        }
        next.learners_next.clear();
        next.outgoing.clear();
        next.auto_leave = false;
        next.validate()?;
        Ok(next)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn of(voters: &[u64], learners: &[u64]) -> Configuration {
        Configuration::new(voters.to_vec(), learners.to_vec()).unwrap()
    }
    #[test]
    fn a_configuration_is_sorted_distinct_and_has_a_voter() {
        let configuration = of(&[3, 1, 2], &[5, 4]);
        assert_eq!(configuration.voters(), [1, 2, 3]);
        assert_eq!(configuration.learners(), [4, 5]);
        assert!(!configuration.is_joint());
        assert!(configuration.votes(2) && !configuration.votes(4));
        assert!(configuration.learns(4) && configuration.contains(4));
        assert!(!configuration.contains(6));
        assert_eq!(
            configuration.members().collect::<Vec<_>>(),
            vec![1, 2, 3, 4, 5]
        );
        for (voters, learners, error) in [
            (vec![], vec![1], ConfigurationError::NoVoter),
            (vec![0, 1], vec![], ConfigurationError::ZeroMember),
            (vec![1], vec![0], ConfigurationError::ZeroMember),
            (vec![1, 1], vec![], ConfigurationError::Overlap),
            (vec![1, 2], vec![2], ConfigurationError::Overlap),
        ] {
            assert_eq!(Configuration::new(voters, learners), Err(error));
        }
        let many: Vec<u64> = (1..=MAX_MEMBERS as u64 + 1).collect();
        assert_eq!(
            Configuration::new(many.clone(), vec![]),
            Err(ConfigurationError::TooManyMembers)
        );
        assert!(Configuration::new(many[..MAX_MEMBERS].to_vec(), vec![]).is_ok());
        assert_eq!(
            Configuration::from_parts(vec![1], vec![], vec![], vec![], true),
            Err(ConfigurationError::NotJoint)
        );
    }
    #[test]
    fn a_simple_change_moves_at_most_one_voter() {
        let start = of(&[1, 2, 3], &[]);
        let learner = start
            .simple(&[Change::AddLearner(4)])
            .unwrap()
            .configuration;
        assert_eq!(learner, of(&[1, 2, 3], &[4]));
        let promoted = learner
            .simple(&[Change::AddVoter(4)])
            .unwrap()
            .configuration;
        assert_eq!(promoted, of(&[1, 2, 3, 4], &[]));
        let removed = promoted.simple(&[Change::Remove(1)]).unwrap().configuration;
        assert_eq!(removed, of(&[2, 3, 4], &[]));
        // Learners move freely beside one voter.
        assert_eq!(
            start
                .simple(&[
                    Change::AddLearner(5),
                    Change::AddLearner(6),
                    Change::AddVoter(4)
                ])
                .unwrap()
                .configuration,
            of(&[1, 2, 3, 4], &[5, 6])
        );
        // A demotion is one voter leaving.
        assert_eq!(
            start
                .simple(&[Change::AddLearner(3)])
                .unwrap()
                .configuration,
            of(&[1, 2], &[3])
        );
        for changes in [
            vec![Change::AddVoter(4), Change::AddVoter(5)],
            vec![Change::AddVoter(4), Change::Remove(1)],
            vec![Change::Remove(1), Change::Remove(2)],
        ] {
            assert_eq!(start.simple(&changes), Err(ConfigurationError::NotSimple));
        }
        assert_eq!(
            of(&[1], &[]).simple(&[Change::Remove(1)]),
            Err(ConfigurationError::NoVoter)
        );
        assert_eq!(
            start.simple(&[Change::AddVoter(0)]),
            Err(ConfigurationError::ZeroMember)
        );
        // What changes nothing is a change all the same.
        for nothing in [vec![Change::Remove(9)], vec![Change::AddVoter(1)], vec![]] {
            let changed = start.simple(&nothing).unwrap();
            assert_eq!(changed.configuration, start);
            assert!(changed.renewed.is_empty());
        }
    }
    #[test]
    fn a_joint_change_passes_through_both_majorities() {
        let start = of(&[1, 2, 3], &[6]);
        let joint = start
            .enter_joint(
                false,
                &[
                    Change::AddVoter(4),
                    Change::AddVoter(5),
                    Change::Remove(1),
                    Change::AddLearner(2),
                    Change::AddVoter(6),
                ],
            )
            .unwrap()
            .configuration;
        assert!(joint.is_joint() && !joint.auto_leave());
        assert_eq!(joint.voters(), [3, 4, 5, 6]);
        assert_eq!(joint.outgoing(), [1, 2, 3]);
        // The demoted voter still votes in the outgoing half, so it learns
        // only once that half is gone.
        assert_eq!(joint.learners(), [] as [u64; 0]);
        assert_eq!(joint.learners_next(), [2]);
        assert!(joint.votes(1) && joint.votes(2) && joint.learns(2));
        assert_eq!(joint.members().collect::<Vec<_>>(), vec![1, 2, 3, 4, 5, 6]);
        assert_eq!(
            joint.enter_joint(false, &[]),
            Err(ConfigurationError::AlreadyJoint)
        );
        assert_eq!(joint.simple(&[]), Err(ConfigurationError::AlreadyJoint));
        let left = joint.leave_joint().unwrap();
        assert_eq!(left, of(&[3, 4, 5, 6], &[2]));
        assert_eq!(left.leave_joint(), Err(ConfigurationError::NotJoint));
        // A change of mind inside the joint change.
        let joint = start
            .enter_joint(true, &[Change::AddLearner(1), Change::AddVoter(1)])
            .unwrap()
            .configuration;
        assert_eq!(joint.voters(), [1, 2, 3]);
        assert!(joint.learners_next().is_empty() && joint.auto_leave());
        assert_eq!(joint.leave_joint().unwrap(), start);
        assert_eq!(
            start.enter_joint(
                false,
                &[Change::Remove(1), Change::Remove(2), Change::Remove(3)]
            ),
            Err(ConfigurationError::NoVoter)
        );
    }
    #[test]
    fn a_member_removed_and_added_again_is_renewed() {
        let start = of(&[1, 2, 3], &[4]);
        // A learner leaves the configuration at once.
        let changed = start
            .simple(&[Change::Remove(4), Change::AddLearner(4)])
            .unwrap();
        assert_eq!(changed.configuration, start);
        assert_eq!(changed.renewed, vec![4]);
        let changed = start
            .enter_joint(
                false,
                &[
                    Change::Remove(4),
                    Change::AddVoter(4),
                    // A voter the outgoing half counts never left.
                    Change::Remove(3),
                    Change::AddVoter(3),
                    // Removed, added and removed again is gone.
                    Change::AddLearner(5),
                    Change::Remove(5),
                    Change::AddLearner(5),
                    Change::Remove(5),
                ],
            )
            .unwrap();
        assert_eq!(changed.configuration.voters(), [1, 2, 3, 4]);
        assert_eq!(changed.renewed, vec![4]);
        assert!(!changed.configuration.contains(5));
        // Out of a joint configuration a voter of the incoming half alone
        // leaves at once.
        let joint = changed.configuration;
        assert_eq!(
            joint.simple(&[Change::Remove(4)]),
            Err(ConfigurationError::AlreadyJoint)
        );
    }
    #[test]
    fn a_recovered_configuration_is_validated() {
        let joint =
            Configuration::from_parts(vec![3, 4], vec![1, 2, 3], vec![5], vec![2], true).unwrap();
        assert!(joint.is_joint() && joint.auto_leave());
        for (voters, outgoing, learners, next) in [
            // One that will learn is no outgoing voter.
            (vec![3], vec![1], vec![], vec![2]),
            // One that will learn still votes in the incoming half.
            (vec![2, 3], vec![2], vec![], vec![2]),
            // A learner votes in the outgoing half.
            (vec![3], vec![1, 2], vec![2], vec![]),
        ] {
            assert_eq!(
                Configuration::from_parts(voters, outgoing, learners, next, false),
                Err(ConfigurationError::Overlap)
            );
        }
        assert_eq!(
            Configuration::from_parts(vec![1], vec![], vec![], vec![2], false),
            Err(ConfigurationError::Overlap)
        );
    }
}
