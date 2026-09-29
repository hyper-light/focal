//! How many members decide.
//!
//! A **classic** quorum is a majority: any two intersect, which is what
//! makes a committed entry survive an election. A **fast** quorum is
//! ⌈3M/4⌉ (Fast Raft, Castiglia, Goldberg and Patterson, ICDCS 2020): any
//! two fast quorums intersect in more than half of the members, and a fast
//! quorum and a classic one intersect in more than half of the classic
//! one, so an entry a fast quorum chose is the most voted in every classic
//! quorum a later leader gathers.
//!
//! During a joint configuration a decision needs the quorum of **both**
//! voter sets (Ongaro's thesis §4.3).
use crate::NodeId;

/// Which quorum a decision needs.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Quorum {
    Classic,
    Fast,
}
impl Quorum {
    /// How many of `members` decide. None decide for no members: an empty
    /// set is the half of a joint configuration that is not there, and it
    /// agrees with whatever the other half decides.
    pub fn of(self, members: usize) -> usize {
        match self {
            Self::Classic => members
                .checked_div(2)
                .unwrap_or(0)
                .saturating_add(usize::from(members > 0)),
            // ⌈3M/4⌉ without the product overflowing: M − ⌊M/4⌋.
            Self::Fast => members.saturating_sub(members.checked_div(4).unwrap_or(0)),
        }
    }
}

/// What a set of voters has said so far.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Tally {
    /// A quorum said yes.
    Won,
    /// So many said no that the rest cannot make a quorum.
    Lost,
    /// Neither yet.
    Pending,
}

/// The outcome among `voters` when `answer` says what each has said: yes,
/// no, or nothing yet. Voters are distinct by construction of a
/// configuration.
pub fn tally(
    voters: &[NodeId],
    quorum: Quorum,
    mut answer: impl FnMut(NodeId) -> Option<bool>,
) -> Tally {
    if voters.is_empty() {
        return Tally::Won;
    }
    let mut yes = 0usize;
    let mut no = 0usize;
    for voter in voters {
        match answer(*voter) {
            Some(true) => yes = yes.saturating_add(1),
            Some(false) => no = no.saturating_add(1),
            None => {}
        }
    }
    let needed = quorum.of(voters.len());
    if yes >= needed {
        Tally::Won
    } else if voters.len().saturating_sub(no) < needed {
        Tally::Lost
    } else {
        Tally::Pending
    }
}

/// The outcome of a joint decision: both sets must win; either losing loses.
pub fn joint(incoming: Tally, outgoing: Tally) -> Tally {
    match (incoming, outgoing) {
        (Tally::Lost, _) | (_, Tally::Lost) => Tally::Lost,
        (Tally::Won, Tally::Won) => Tally::Won,
        _ => Tally::Pending,
    }
}

/// The highest index a quorum of `voters` has reached, when `reached` says
/// how far each has: the index the leader may commit to, were its term the
/// entry's. `u64::MAX` for no voters, which is the half of a joint
/// configuration that is not there.
///
/// `scratch` holds the voters' indexes while they are ordered; it is cleared
/// first and its capacity is the caller's to bound.
pub fn reached(
    voters: &[NodeId],
    quorum: Quorum,
    scratch: &mut Vec<u64>,
    mut reached: impl FnMut(NodeId) -> u64,
) -> u64 {
    scratch.clear();
    if voters.is_empty() {
        return u64::MAX;
    }
    // Without room to order them nothing is known to be reached.
    if scratch.try_reserve(voters.len()).is_err() {
        return 0;
    }
    for voter in voters {
        scratch.push(reached(*voter));
    }
    // Descending: the quorum-th highest is what a quorum has reached.
    scratch.sort_unstable_by(|left, right| right.cmp(left));
    let needed = quorum.of(voters.len());
    scratch.get(needed.saturating_sub(1)).copied().unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_classic_quorum_is_a_majority_and_a_fast_one_three_quarters() {
        let classic: Vec<usize> = (0..=9).map(|members| Quorum::Classic.of(members)).collect();
        assert_eq!(classic, vec![0, 1, 2, 2, 3, 3, 4, 4, 5, 5]);
        let fast: Vec<usize> = (0..=9).map(|members| Quorum::Fast.of(members)).collect();
        assert_eq!(fast, vec![0, 1, 2, 3, 3, 4, 5, 6, 6, 7]);
        for members in 1..=2_000usize {
            let classic = Quorum::Classic.of(members);
            let fast = Quorum::Fast.of(members);
            assert_eq!(fast, (3 * members).div_ceil(4));
            assert!(fast >= classic && fast <= members);
            // Two classic quorums share a member.
            assert!(2 * classic > members);
            // A fast quorum and a classic one share more than half of the
            // classic one: what a fast quorum chose is the most voted in
            // every classic quorum.
            assert!(2 * (fast + classic - members) > classic, "{members}");
            // Two fast quorums share more than half of the members.
            assert!(2 * (2 * fast - members) >= members, "{members}");
        }
        assert_eq!(Quorum::Fast.of(usize::MAX), usize::MAX - usize::MAX / 4);
        assert_eq!(Quorum::Classic.of(usize::MAX), usize::MAX / 2 + 1);
    }
    #[test]
    fn a_tally_is_won_lost_or_pending() {
        let voters = [1, 2, 3, 4, 5];
        let of = |yes: &[u64], no: &[u64], quorum| {
            tally(&voters, quorum, |voter| {
                if yes.contains(&voter) {
                    Some(true)
                } else if no.contains(&voter) {
                    Some(false)
                } else {
                    None
                }
            })
        };
        assert_eq!(of(&[1, 2, 3], &[], Quorum::Classic), Tally::Won);
        assert_eq!(of(&[1, 2], &[3], Quorum::Classic), Tally::Pending);
        assert_eq!(of(&[1, 2], &[3, 4, 5], Quorum::Classic), Tally::Lost);
        assert_eq!(of(&[1], &[3, 4], Quorum::Classic), Tally::Pending);
        // Four of five is the fast quorum: two refusals lose it.
        assert_eq!(of(&[1, 2, 3], &[], Quorum::Fast), Tally::Pending);
        assert_eq!(of(&[1, 2, 3, 4], &[], Quorum::Fast), Tally::Won);
        assert_eq!(of(&[1, 2, 3], &[4, 5], Quorum::Fast), Tally::Lost);
        assert_eq!(tally(&[], Quorum::Classic, |_| None), Tally::Won);
    }
    #[test]
    fn a_joint_decision_needs_both_sets() {
        use Tally::{Lost, Pending, Won};
        assert_eq!(joint(Won, Won), Won);
        assert_eq!(joint(Won, Pending), Pending);
        assert_eq!(joint(Pending, Won), Pending);
        assert_eq!(joint(Won, Lost), Lost);
        assert_eq!(joint(Lost, Won), Lost);
        assert_eq!(joint(Pending, Lost), Lost);
    }
    #[test]
    fn what_a_quorum_has_reached_is_its_lowest_member() {
        let mut scratch = Vec::new();
        let at = |indexes: &[(u64, u64)], quorum, scratch: &mut Vec<u64>| {
            let voters: Vec<u64> = indexes.iter().map(|(voter, _)| *voter).collect();
            reached(&voters, quorum, scratch, |voter| {
                indexes
                    .iter()
                    .find(|(known, _)| *known == voter)
                    .map_or(0, |(_, index)| *index)
            })
        };
        let five = [(1, 10), (2, 7), (3, 9), (4, 3), (5, 0)];
        assert_eq!(at(&five, Quorum::Classic, &mut scratch), 7);
        assert_eq!(at(&five, Quorum::Fast, &mut scratch), 3);
        assert_eq!(at(&[(1, 4)], Quorum::Classic, &mut scratch), 4);
        assert_eq!(at(&[(1, 4), (2, 8)], Quorum::Classic, &mut scratch), 4);
        assert_eq!(at(&[], Quorum::Classic, &mut scratch), u64::MAX);
        // A joint commit is the lower of the two.
        let incoming = at(&[(1, 10), (2, 9), (3, 2)], Quorum::Classic, &mut scratch);
        let outgoing = at(&[(1, 10), (4, 5), (5, 4)], Quorum::Classic, &mut scratch);
        assert_eq!(incoming.min(outgoing), 5);
    }
}
