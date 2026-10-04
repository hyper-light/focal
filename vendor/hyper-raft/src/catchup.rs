//! Catching up a learner before it votes (Ongaro's thesis §4.2.1, "Catching up new servers";
//! mantle note 32 R13).
//!
//! A member added as a voter with an empty log leaves its group unable to commit until it catches
//! up, if a voter fails meanwhile (the thesis's Figure 4.4(a)): with three voters and a fourth added
//! empty, the loss of one leaves a quorum of three of four that needs the newcomer. So a member is
//! added as a learner first, which votes on nothing and counts toward no quorum, and is promoted
//! only once it has caught up. The thesis's rule: replication to it proceeds in rounds, each
//! replicating what the leader held when the round began; if a round lasts less than an election
//! timeout, the member is close enough to join without a significant gap; the leader "should also
//! abort the change if the new server is unavailable or is so slow that it will never catch up".
//!
//! As built here, from slates' (`RaftNode::catch_up`, its explorer and Figure 4.4(a) measured at 21
//! rounds without commits against 1) and the thesis:
//! - a round ends when the learner holds the leader's last index as the round began; one that lasted
//!   less than an election is the last, and the learner is ready; a longer one begins the next with
//!   what the leader holds then;
//! - the learner is given up when its lag behind the leader's last index did not shrink over a whole
//!   election: unavailable, or slower than the leader appends (slates' rule, which needs no count of
//!   rounds where the thesis says "such as 10");
//! - an election is the member's own measure of one: on ticks, the minimum election timeout
//!   (`Config::election_tick`, the thesis's); by suspicion, the time the group's election is expected
//!   to take by hyper-timing's law (`Timing::election`), the gap the group already accepts at its
//!   leader's loss;
//! - each learner is judged alone: one that cannot catch up holds back none that has (slates
//!   `docs/bugs/2026-09-30-one-lagging-member-held-back-every-council-promotion.md`). Promoting is
//!   its owner's change to propose.
//!
//! The rounds are the leader's alone, and a new leader stages afresh.
use crate::NodeId;

/// Where catching up a learner stands ([`crate::RawNode::catch_up`]).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CatchUp {
    /// A round ended within an election: the member may be promoted without a significant gap. A
    /// voter is ready too, with nothing to catch up.
    Ready,
    /// Rounds are under way.
    Pending,
    /// Its lag did not shrink over a whole election: the member is unavailable or too slow. Said
    /// once; asked again, it is staged afresh, and may succeed the next time, its log partly caught
    /// up (the thesis: "the caller may always try again").
    Aborted,
    /// This member does not lead: the rounds are a leader's.
    NotLeader,
    /// The configuration names no such member.
    NotMember,
}

/// One learner being caught up, on the member's clock (ticks, or the owner's nanoseconds).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Staging {
    /// The leader's last index when the round under way began: the round ends when the learner
    /// holds it.
    pub(crate) round_end: u64,
    /// When the round under way began.
    pub(crate) began: u64,
    /// The learner's lag behind the leader's last index when it was last judged, and when.
    pub(crate) lag: u64,
    pub(crate) judged: u64,
    /// A round ended within an election.
    pub(crate) ready: bool,
}

/// The learners being caught up, in order of member: at most one entry a learner of the
/// configuration, so bounded by it.
#[derive(Clone, Debug, Default)]
pub(crate) struct Stagings {
    staged: Vec<(NodeId, Staging)>,
}

impl Stagings {
    pub(crate) fn clear(&mut self) {
        self.staged.clear();
    }
    pub(crate) fn is_empty(&self) -> bool {
        self.staged.is_empty()
    }
    pub(crate) fn get_mut(&mut self, member: NodeId) -> Option<&mut Staging> {
        let position = self
            .staged
            .binary_search_by_key(&member, |(staged, _)| *staged)
            .ok()?;
        self.staged.get_mut(position).map(|(_, staging)| staging)
    }
    /// Stages `member` with `staging`, or replaces its staging.
    pub(crate) fn put(&mut self, member: NodeId, staging: Staging) -> crate::Result<()> {
        match self
            .staged
            .binary_search_by_key(&member, |(staged, _)| *staged)
        {
            Ok(position) => {
                if let Some((_, held)) = self.staged.get_mut(position) {
                    *held = staging;
                }
            }
            Err(position) => {
                self.staged
                    .try_reserve(1)
                    .map_err(|_| crate::Error::Memory)?;
                self.staged.insert(position, (member, staging));
            }
        }
        Ok(())
    }
    pub(crate) fn remove(&mut self, member: NodeId) {
        self.staged.retain(|(staged, _)| *staged != member);
    }
    /// Forgets every staging `keep` refuses: a member that is no learner any more.
    pub(crate) fn retain(&mut self, keep: impl Fn(NodeId) -> bool) {
        self.staged.retain(|(member, _)| keep(*member));
    }
    pub(crate) fn resident_bytes(&self) -> usize {
        self.staged
            .capacity()
            .saturating_mul(std::mem::size_of::<(NodeId, Staging)>())
    }
}
