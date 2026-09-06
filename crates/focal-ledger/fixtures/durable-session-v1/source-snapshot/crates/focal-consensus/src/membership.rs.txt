//! Exact committed configuration views and bounded owner-authorized changes.
use super::*;

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct MembershipConfiguration {
    pub voters: Vec<u64>,
    pub learners: Vec<u64>,
    pub voters_outgoing: Vec<u64>,
    pub learners_next: Vec<u64>,
    pub auto_leave: bool,
}
impl MembershipConfiguration {
    pub(crate) fn from_conf(conf: &ConfState) -> Self {
        let mut value = Self {
            voters: conf.voters.clone(),
            learners: conf.learners.clone(),
            voters_outgoing: conf.voters_outgoing.clone(),
            learners_next: conf.learners_next.clone(),
            auto_leave: conf.auto_leave,
        };
        value.voters.sort_unstable();
        value.learners.sort_unstable();
        value.voters_outgoing.sort_unstable();
        value.learners_next.sort_unstable();
        value
    }
    pub fn validate(&self) -> Result<(), ConsensusError> {
        for members in [
            &self.voters,
            &self.learners,
            &self.voters_outgoing,
            &self.learners_next,
        ] {
            if members.len() > 1024
                || !members
                    .windows(2)
                    .all(|pair| matches!(pair, [left, right] if left < right))
            {
                return Err(ConsensusError::Configuration("noncanonical membership"));
            }
        }
        validate_conf_state(&ConfState {
            voters: self.voters.clone(),
            learners: self.learners.clone(),
            voters_outgoing: self.voters_outgoing.clone(),
            learners_next: self.learners_next.clone(),
            auto_leave: self.auto_leave,
        })
    }
    pub fn contains(&self, node: u64) -> bool {
        [
            &self.voters,
            &self.learners,
            &self.voters_outgoing,
            &self.learners_next,
        ]
        .into_iter()
        .any(|members| members.binary_search(&node).is_ok())
    }
    pub fn charged_bytes(&self) -> Result<usize, ConsensusError> {
        [
            &self.voters,
            &self.learners,
            &self.voters_outgoing,
            &self.learners_next,
        ]
        .into_iter()
        .try_fold(256usize, |bytes, members| {
            members
                .capacity()
                .checked_mul(size_of::<u64>())
                .and_then(|n| bytes.checked_add(n))
                .ok_or(ConsensusError::Capacity)
        })
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum MembershipChange {
    AddLearner { node: u64 },
    Promote { node: u64 },
    Remove { node: u64 },
    LeaveJoint,
}
impl MembershipChange {
    pub fn apply_to(
        self,
        current: &MembershipConfiguration,
    ) -> Result<MembershipConfiguration, ConsensusError> {
        current.validate()?;
        let joint = !current.voters_outgoing.is_empty();
        if joint != matches!(self, Self::LeaveJoint) {
            return Err(ConsensusError::Configuration(
                "leave joint configuration before another change",
            ));
        }
        let mut next = current.clone();
        match self {
            Self::AddLearner { node } => {
                if node == 0 || current.contains(node) {
                    return Err(ConsensusError::Configuration(
                        "learner already exists or has zero identity",
                    ));
                }
                next.learners.push(node);
                next.learners.sort_unstable();
            }
            Self::Promote { node } => {
                if node == 0 || current.learners.binary_search(&node).is_err() {
                    return Err(ConsensusError::Configuration(
                        "promotion requires an existing learner",
                    ));
                }
                next.learners.retain(|id| *id != node);
                next.voters.push(node);
                next.voters.sort_unstable();
            }
            Self::Remove { node } => {
                if node == 0
                    || !current.contains(node)
                    || (current.voters.len() == 1 && current.voters.contains(&node))
                {
                    return Err(ConsensusError::Configuration(
                        "member is absent or the final voter",
                    ));
                }
                next.learners.retain(|id| *id != node);
                next.voters.retain(|id| *id != node);
            }
            Self::LeaveJoint => {
                next.learners.extend_from_slice(&next.learners_next);
                next.learners.sort_unstable();
                next.learners_next.clear();
                next.voters_outgoing.clear();
                next.auto_leave = false;
            }
        }
        next.validate()?;
        Ok(next)
    }
    fn encode(self, context: Vec<u8>) -> ConfChangeV2 {
        let mut change = ConfChangeV2 {
            context,
            ..Default::default()
        };
        let member = match self {
            Self::AddLearner { node } => Some((node, ConfChangeType::AddLearnerNode)),
            Self::Promote { node } => Some((node, ConfChangeType::AddNode)),
            Self::Remove { node } => Some((node, ConfChangeType::RemoveNode)),
            Self::LeaveJoint => None,
        };
        if let Some((node_id, kind)) = member {
            let mut member = ConfChangeSingle {
                node_id,
                ..Default::default()
            };
            member.set_change_type(kind);
            change.changes.push(member);
        }
        change
    }
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AppliedMembership {
    pub index: u64,
    pub term: u64,
    /// Opaque application-owned request identity; no authority comes from parsing it.
    pub context: Vec<u8>,
    pub before: MembershipConfiguration,
    pub after: MembershipConfiguration,
}
impl DurableNode {
    pub fn membership_configuration(&self) -> MembershipConfiguration {
        MembershipConfiguration::from_conf(&self.raw.store().conf_state)
    }
    /// Admission only. Persist an exact request identity in context, then await
    /// the matching AppliedMembership event before releasing a durable receipt.
    pub fn propose_membership(
        &mut self,
        expected: &MembershipConfiguration,
        change: MembershipChange,
        context: Vec<u8>,
    ) -> Result<(), ConsensusError> {
        self.check_leader()?;
        if self.persistence_pending() {
            return Err(ConsensusError::PersistencePending);
        }
        if !self.has_committed_current_term()
            || self.delivered_index < self.raw.raft.raft_log.committed
        {
            return Err(ConsensusError::Configuration(
                "current-term committed configuration is not published",
            ));
        }
        if &self.membership_configuration() != expected {
            return Err(ConsensusError::Configuration(
                "membership precondition changed",
            ));
        }
        change.apply_to(expected)?;
        self.propose_conf_change(change.encode(context))
    }
}
