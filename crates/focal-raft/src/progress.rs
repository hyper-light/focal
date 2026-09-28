//! What a leader knows of each member, and what a candidate knows of its
//! votes.
use crate::{
    Configuration, NodeId, Quorum, Tally,
    error::{Error, Result},
    quorum,
};

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum ProgressState {
    /// One message a heartbeat, until the member answers where it is.
    #[default]
    Probe,
    /// Entries are sent ahead of their answers, as the window admits.
    Replicate,
    /// A snapshot is on its way; nothing else is sent.
    Snapshot,
}

/// The last index of each message sent and not answered, in order: a
/// window of at most `cap`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Inflights {
    start: usize,
    count: usize,
    buffer: Vec<u64>,
    cap: usize,
}
impl Inflights {
    pub fn new(cap: usize) -> Self {
        Self {
            start: 0,
            count: 0,
            buffer: Vec::new(),
            cap,
        }
    }
    pub fn full(&self) -> bool {
        self.count >= self.cap
    }
    pub fn count(&self) -> usize {
        self.count
    }
    fn wrap(&self, position: usize) -> usize {
        if position >= self.cap {
            position.saturating_sub(self.cap)
        } else {
            position
        }
    }
    pub fn add(&mut self, inflight: u64) -> Result<()> {
        if self.full() {
            return Err(Error::Invariant("a message sent into a full window"));
        }
        if self.buffer.capacity() == 0 {
            self.buffer
                .try_reserve_exact(self.cap)
                .map_err(|_| Error::Memory)?;
        }
        let next = self.wrap(self.start.saturating_add(self.count));
        let length = self.buffer.len();
        match self.buffer.get_mut(next) {
            Some(slot) => *slot = inflight,
            None if next == length => self.buffer.push(inflight),
            None => return Err(Error::Invariant("the window lost its place")),
        }
        self.count = self.count.saturating_add(1);
        Ok(())
    }
    /// Everything at or below `to` is answered.
    pub fn free_to(&mut self, to: u64) {
        let mut freed = 0usize;
        let mut position = self.start;
        while freed < self.count {
            if self.buffer.get(position).is_none_or(|held| to < *held) {
                break;
            }
            position = self.wrap(position.saturating_add(1));
            freed = freed.saturating_add(1);
        }
        self.count = self.count.saturating_sub(freed);
        self.start = position;
    }
    pub fn free_first_one(&mut self) {
        if self.count > 0
            && let Some(first) = self.buffer.get(self.start).copied()
        {
            self.free_to(first);
        }
    }
    pub fn reset(&mut self) {
        self.count = 0;
        self.start = 0;
        self.buffer = Vec::new();
    }
    pub fn resident_bytes(&self) -> usize {
        self.buffer
            .capacity()
            .saturating_mul(std::mem::size_of::<u64>())
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Progress {
    /// The highest index known to be held by the member.
    pub matched: u64,
    /// The index of the next entry to send.
    pub next_index: u64,
    pub state: ProgressState,
    /// While probing: a message is out and no other is sent.
    pub paused: bool,
    /// While a snapshot is on its way: its index.
    pub pending_snapshot: u64,
    /// The index the member asked a snapshot to reach; zero for none.
    pub pending_request_snapshot: u64,
    /// Whether the member was heard from since the leader last looked.
    pub recent_active: bool,
    pub inflights: Inflights,
    /// The member's commit as it last said.
    pub committed_index: u64,
}
impl Progress {
    pub fn new(next_index: u64, window: usize) -> Self {
        Self {
            matched: 0,
            next_index,
            state: ProgressState::Probe,
            paused: false,
            pending_snapshot: 0,
            pending_request_snapshot: 0,
            recent_active: false,
            inflights: Inflights::new(window),
            committed_index: 0,
        }
    }
    fn reset_state(&mut self, state: ProgressState) {
        self.paused = false;
        self.pending_snapshot = 0;
        self.state = state;
        self.inflights.reset();
    }
    pub(crate) fn reset(&mut self, next_index: u64) {
        self.matched = 0;
        self.next_index = next_index;
        self.state = ProgressState::Probe;
        self.paused = false;
        self.pending_snapshot = 0;
        self.pending_request_snapshot = 0;
        self.recent_active = false;
        self.inflights.reset();
    }
    pub fn become_probe(&mut self) {
        // After a snapshot the member holds what the snapshot held.
        let after_snapshot = if self.state == ProgressState::Snapshot {
            self.pending_snapshot.saturating_add(1)
        } else {
            0
        };
        self.reset_state(ProgressState::Probe);
        self.next_index = self.matched.saturating_add(1).max(after_snapshot);
    }
    pub fn become_replicate(&mut self) {
        self.reset_state(ProgressState::Replicate);
        self.next_index = self.matched.saturating_add(1);
    }
    pub fn become_snapshot(&mut self, index: u64) {
        self.reset_state(ProgressState::Snapshot);
        self.pending_snapshot = index;
    }
    pub fn snapshot_failure(&mut self) {
        self.pending_snapshot = 0;
    }
    pub fn is_snapshot_caught_up(&self) -> bool {
        self.state == ProgressState::Snapshot && self.matched >= self.pending_snapshot
    }
    /// The member holds through `index`. False for an answer that says
    /// nothing new.
    pub fn maybe_update(&mut self, index: u64) -> bool {
        let news = self.matched < index;
        if news {
            self.matched = index;
            self.paused = false;
        }
        self.next_index = self.next_index.max(index.saturating_add(1));
        news
    }
    pub fn update_committed(&mut self, committed: u64) {
        self.committed_index = self.committed_index.max(committed);
    }
    /// The member refused what followed `rejected` and hints at
    /// `match_hint`. False for a refusal that is out of date.
    pub fn maybe_decrease_to(
        &mut self,
        rejected: u64,
        match_hint: u64,
        request_snapshot: u64,
    ) -> bool {
        if self.state == ProgressState::Replicate {
            if rejected < self.matched || (rejected == self.matched && request_snapshot == 0) {
                return false;
            }
            if request_snapshot == 0 {
                self.next_index = self.matched.saturating_add(1);
            } else {
                self.pending_request_snapshot = request_snapshot;
            }
            return true;
        }
        if (self.next_index == 0 || self.next_index.saturating_sub(1) != rejected)
            && request_snapshot == 0
        {
            return false;
        }
        if request_snapshot == 0 {
            self.next_index = rejected
                .min(match_hint.saturating_add(1))
                .max(self.matched.saturating_add(1));
        } else if self.pending_request_snapshot == 0 {
            self.pending_request_snapshot = request_snapshot;
        }
        self.paused = false;
        true
    }
    pub fn is_paused(&self) -> bool {
        match self.state {
            ProgressState::Probe => self.paused,
            ProgressState::Replicate => self.inflights.full(),
            ProgressState::Snapshot => true,
        }
    }
    /// Entries through `last` were sent.
    pub fn sent(&mut self, last: u64) -> Result<()> {
        match self.state {
            ProgressState::Replicate => {
                self.next_index = last.saturating_add(1);
                self.inflights.add(last)
            }
            ProgressState::Probe => {
                self.paused = true;
                Ok(())
            }
            ProgressState::Snapshot => Err(Error::Invariant(
                "entries sent while a snapshot is on its way",
            )),
        }
    }
}

/// The configuration in force, every member's progress, and the votes of
/// the election under way. All three are bounded by the configuration.
#[derive(Clone, Debug)]
pub struct Tracker {
    /// In order of member.
    progress: Vec<(NodeId, Progress)>,
    configuration: Configuration,
    /// In order of member; the first answer of each counts.
    votes: Vec<(NodeId, bool)>,
    window: usize,
    scratch: Vec<u64>,
}
impl Tracker {
    pub fn new(configuration: Configuration, next_index: u64, window: usize) -> Result<Self> {
        let mut tracker = Self {
            progress: Vec::new(),
            configuration: Configuration::default(),
            votes: Vec::new(),
            window,
            scratch: Vec::new(),
        };
        tracker.apply(configuration, &[], next_index)?;
        Ok(tracker)
    }
    pub fn configuration(&self) -> &Configuration {
        &self.configuration
    }
    fn find(&self, member: NodeId) -> std::result::Result<usize, usize> {
        self.progress
            .binary_search_by_key(&member, |(member, _)| *member)
    }
    pub fn get(&self, member: NodeId) -> Option<&Progress> {
        let position = self.find(member).ok()?;
        self.progress.get(position).map(|(_, progress)| progress)
    }
    pub fn get_mut(&mut self, member: NodeId) -> Option<&mut Progress> {
        let position = self.find(member).ok()?;
        self.progress
            .get_mut(position)
            .map(|(_, progress)| progress)
    }
    pub fn len(&self) -> usize {
        self.progress.len()
    }
    pub fn is_empty(&self) -> bool {
        self.progress.is_empty()
    }
    /// In order of member.
    pub fn iter(&self) -> impl ExactSizeIterator<Item = (NodeId, &Progress)> {
        self.progress
            .iter()
            .map(|(member, progress)| (*member, progress))
    }
    pub fn iter_mut(&mut self) -> impl ExactSizeIterator<Item = (NodeId, &mut Progress)> {
        self.progress
            .iter_mut()
            .map(|(member, progress)| (*member, progress))
    }
    /// The member at `position` in order, for one who walks the members
    /// while it changes them.
    pub(crate) fn at(&mut self, position: usize) -> Option<(NodeId, &mut Progress)> {
        self.progress
            .get_mut(position)
            .map(|(member, progress)| (*member, progress))
    }
    /// One voter alone decides.
    pub fn is_singleton(&self) -> bool {
        !self.configuration.is_joint() && self.configuration.voters().len() == 1
    }
    /// The highest index the quorum of both halves holds.
    pub fn quorum_index(&mut self) -> u64 {
        let progress = &self.progress;
        let matched = |member: NodeId| {
            progress
                .binary_search_by_key(&member, |(member, _)| *member)
                .ok()
                .and_then(|position| progress.get(position))
                .map_or(0, |(_, progress)| progress.matched)
        };
        let incoming = quorum::reached(
            self.configuration.voters(),
            Quorum::Classic,
            &mut self.scratch,
            matched,
        );
        let outgoing = quorum::reached(
            self.configuration.outgoing(),
            Quorum::Classic,
            &mut self.scratch,
            matched,
        );
        incoming.min(outgoing)
    }
    pub fn reset_votes(&mut self) {
        self.votes.clear();
    }
    /// The first answer of a member counts. One that is no voter is not
    /// recorded: it decides nothing, and the record stays bounded by the
    /// configuration.
    pub fn record_vote(&mut self, member: NodeId, vote: bool) -> Result<()> {
        if !self.configuration.votes(member) {
            return Ok(());
        }
        if let Err(position) = self
            .votes
            .binary_search_by_key(&member, |(member, _)| *member)
        {
            self.votes.try_reserve(1).map_err(|_| Error::Memory)?;
            self.votes.insert(position, (member, vote));
        }
        Ok(())
    }
    fn decided(&self, answer: impl Fn(NodeId) -> Option<bool> + Copy) -> Tally {
        quorum::joint(
            quorum::tally(self.configuration.voters(), Quorum::Classic, answer),
            quorum::tally(self.configuration.outgoing(), Quorum::Classic, answer),
        )
    }
    pub fn tally_votes(&self) -> Tally {
        self.decided(|member| {
            self.votes
                .binary_search_by_key(&member, |(member, _)| *member)
                .ok()
                .and_then(|position| self.votes.get(position))
                .map(|(_, vote)| *vote)
        })
    }
    /// Whether `members`, in order, hold the quorum of both halves.
    pub fn has_quorum(&self, members: &[NodeId]) -> bool {
        self.decided(|member| members.binary_search(&member).ok().map(|_| true)) == Tally::Won
    }
    /// Whether a quorum was heard from since this was last asked, as the
    /// leader `leader` sees it. Asking forgets what was heard.
    pub fn quorum_recently_active(&mut self, leader: NodeId) -> bool {
        for (member, progress) in &mut self.progress {
            if *member == leader {
                progress.recent_active = true;
            }
        }
        let progress = &self.progress;
        let active = self.decided_active(progress);
        for (member, progress) in &mut self.progress {
            if *member != leader {
                progress.recent_active = false;
            }
        }
        active
    }
    fn decided_active(&self, progress: &[(NodeId, Progress)]) -> bool {
        self.decided(|member| {
            progress
                .binary_search_by_key(&member, |(member, _)| *member)
                .ok()
                .and_then(|position| progress.get(position))
                .filter(|(_, progress)| progress.recent_active)
                .map(|_| true)
        }) == Tally::Won
    }
    /// Puts `configuration` in force: one that is a member now and was
    /// none, or is one of `renewed`, begins at `next_index`, and one that
    /// is none any more is forgotten.
    pub fn apply(
        &mut self,
        configuration: Configuration,
        renewed: &[NodeId],
        next_index: u64,
    ) -> Result<()> {
        let mut progress = Vec::new();
        progress
            .try_reserve_exact(configuration.members().count())
            .map_err(|_| Error::Memory)?;
        let mut known = std::mem::take(&mut self.progress).into_iter().peekable();
        for member in configuration.members() {
            while known.peek().is_some_and(|(held, _)| *held < member) {
                known.next();
            }
            match known
                .next_if(|(held, _)| *held == member)
                .filter(|_| !renewed.contains(&member))
            {
                Some(held) => progress.push(held),
                None => {
                    let mut new = Progress::new(next_index, self.window);
                    // Heard from, so that a leader that looks for its
                    // quorum before the member could answer does not step
                    // down for it.
                    new.recent_active = true;
                    progress.push((member, new));
                }
            }
        }
        self.progress = progress;
        self.votes
            .retain(|(member, _)| configuration.votes(*member));
        self.configuration = configuration;
        Ok(())
    }
    pub fn resident_bytes(&self) -> usize {
        let members = self
            .progress
            .capacity()
            .saturating_mul(std::mem::size_of::<(NodeId, Progress)>());
        let windows = self.progress.iter().fold(0usize, |bytes, (_, progress)| {
            bytes.saturating_add(progress.inflights.resident_bytes())
        });
        let votes = self
            .votes
            .capacity()
            .saturating_mul(std::mem::size_of::<(NodeId, bool)>());
        let configuration = self
            .configuration
            .members()
            .count()
            .saturating_mul(4)
            .saturating_mul(std::mem::size_of::<NodeId>());
        members
            .saturating_add(windows)
            .saturating_add(votes)
            .saturating_add(configuration)
            .saturating_add(
                self.scratch
                    .capacity()
                    .saturating_mul(std::mem::size_of::<u64>()),
            )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Change;

    #[test]
    fn a_window_admits_its_bound_and_frees_in_order() {
        let mut window = Inflights::new(4);
        for index in [10, 20, 30, 40] {
            assert!(!window.full());
            window.add(index).unwrap();
        }
        assert!(window.full());
        assert!(window.add(50).is_err());
        window.free_to(5);
        assert_eq!(window.count(), 4);
        window.free_to(20);
        assert_eq!(window.count(), 2);
        // It wraps.
        window.add(50).unwrap();
        window.add(60).unwrap();
        assert!(window.full());
        window.free_first_one();
        assert_eq!(window.count(), 3);
        window.free_to(55);
        assert_eq!(window.count(), 1);
        window.free_to(100);
        assert_eq!(window.count(), 0);
        window.free_first_one();
        for round in 0..100u64 {
            window.add(round).unwrap();
            window.free_to(round);
        }
        assert_eq!(window.count(), 0);
        assert_eq!(window.resident_bytes(), 32);
        window.reset();
        assert_eq!(window.resident_bytes(), 0);
        assert!(Inflights::new(0).full());
    }
    #[test]
    fn an_answer_moves_a_member_forward_and_never_back() {
        let mut progress = Progress::new(5, 8);
        assert!(progress.maybe_update(7));
        assert_eq!((progress.matched, progress.next_index), (7, 8));
        assert!(!progress.maybe_update(6));
        assert_eq!((progress.matched, progress.next_index), (7, 8));
        assert!(!progress.maybe_update(7));
        progress.next_index = 20;
        assert!(progress.maybe_update(9));
        assert_eq!((progress.matched, progress.next_index), (9, 20));
        progress.update_committed(4);
        progress.update_committed(3);
        assert_eq!(progress.committed_index, 4);
    }
    #[test]
    fn a_refusal_moves_the_next_entry_back_unless_it_is_out_of_date() {
        // (state, matched, next, rejected, hint, moved, next after)
        for (state, matched, next, rejected, hint, moved, after) in [
            (ProgressState::Replicate, 5, 10, 5, 5, false, 10),
            (ProgressState::Replicate, 5, 10, 4, 4, false, 10),
            (ProgressState::Replicate, 5, 10, 9, 9, true, 6),
            (ProgressState::Probe, 0, 0, 0, 0, false, 0),
            (ProgressState::Probe, 0, 10, 5, 5, false, 10),
            (ProgressState::Probe, 0, 10, 9, 9, true, 9),
            (ProgressState::Probe, 0, 2, 1, 1, true, 1),
            (ProgressState::Probe, 0, 1, 0, 0, true, 1),
            (ProgressState::Probe, 0, 10, 9, 2, true, 3),
            (ProgressState::Probe, 0, 10, 9, 0, true, 1),
        ] {
            let mut progress = Progress::new(next, 8);
            progress.state = state;
            progress.matched = matched;
            assert_eq!(progress.maybe_decrease_to(rejected, hint, 0), moved);
            assert_eq!(progress.next_index, after);
            assert_eq!(progress.matched, matched);
        }
        // A request for a snapshot is never out of date.
        let mut progress = Progress::new(10, 8);
        progress.state = ProgressState::Replicate;
        progress.matched = 5;
        assert!(progress.maybe_decrease_to(5, 5, 7));
        assert_eq!(
            (progress.pending_request_snapshot, progress.next_index),
            (7, 10)
        );
        let mut progress = Progress::new(10, 8);
        assert!(progress.maybe_decrease_to(3, 3, 7));
        assert_eq!(progress.pending_request_snapshot, 7);
        assert!(progress.maybe_decrease_to(3, 3, 9));
        assert_eq!(progress.pending_request_snapshot, 7);
    }
    #[test]
    fn a_member_is_paused_by_its_state() {
        let mut progress = Progress::new(1, 2);
        assert!(!progress.is_paused());
        progress.sent(1).unwrap();
        assert!(progress.is_paused() && progress.next_index == 1);
        progress.become_replicate();
        assert!(!progress.is_paused());
        progress.sent(3).unwrap();
        progress.sent(5).unwrap();
        assert!(progress.is_paused() && progress.next_index == 6);
        progress.matched = 5;
        progress.become_snapshot(9);
        assert!(progress.is_paused() && progress.sent(10).is_err());
        assert!(!progress.is_snapshot_caught_up());
        progress.become_probe();
        assert_eq!((progress.next_index, progress.pending_snapshot), (10, 0));
        progress.matched = 12;
        progress.become_probe();
        assert_eq!(progress.next_index, 13);
    }
    fn tracker(voters: &[u64], learners: &[u64]) -> Tracker {
        Tracker::new(
            Configuration::new(voters.to_vec(), learners.to_vec()).unwrap(),
            1,
            8,
        )
        .unwrap()
    }
    #[test]
    fn the_quorum_holds_what_a_majority_of_both_halves_holds() {
        let mut members = tracker(&[1, 2, 3], &[4]);
        for (member, matched) in [(1, 9), (2, 5), (3, 2), (4, 100)] {
            members.get_mut(member).unwrap().matched = matched;
        }
        // A learner holds no part of it.
        assert_eq!(members.quorum_index(), 5);
        let joint = members
            .configuration()
            .enter_joint(
                false,
                &[Change::AddVoter(5), Change::AddVoter(6), Change::Remove(1)],
            )
            .unwrap()
            .configuration;
        members.apply(joint, &[], 50).unwrap();
        assert_eq!(members.len(), 6);
        assert_eq!(members.get(5).unwrap().next_index, 50);
        assert!(members.get(5).unwrap().recent_active);
        // {2, 3, 5, 6} holds 5, 2, 0, 0: three of four hold 0.
        assert_eq!(members.quorum_index(), 0);
        members.get_mut(5).unwrap().matched = 7;
        members.get_mut(6).unwrap().matched = 8;
        // Incoming holds 5; outgoing {1, 2, 3} holds 5.
        assert_eq!(members.quorum_index(), 5);
        members.get_mut(2).unwrap().matched = 8;
        assert_eq!(members.quorum_index(), 7);
        let left = members.configuration().leave_joint().unwrap();
        members.apply(left.clone(), &[], 60).unwrap();
        assert_eq!(
            members.iter().map(|(member, _)| member).collect::<Vec<_>>(),
            vec![2, 3, 4, 5, 6]
        );
        assert_eq!(members.get(2).unwrap().matched, 8);
        assert!(members.get(1).is_none());
        // What was known of one that is renewed is forgotten.
        members.apply(left, &[2], 70).unwrap();
        assert_eq!(members.get(2).unwrap().matched, 0);
        assert_eq!(members.get(2).unwrap().next_index, 70);
        assert_eq!(members.get(5).unwrap().matched, 7);
    }
    #[test]
    fn an_election_needs_both_halves_and_counts_first_answers() {
        let mut members = tracker(&[1, 2, 3], &[4]);
        assert_eq!(members.tally_votes(), Tally::Pending);
        members.record_vote(1, true).unwrap();
        members.record_vote(4, true).unwrap();
        members.record_vote(9, true).unwrap();
        assert_eq!(members.tally_votes(), Tally::Pending);
        members.record_vote(2, false).unwrap();
        members.record_vote(2, true).unwrap();
        assert_eq!(members.tally_votes(), Tally::Pending);
        members.record_vote(3, true).unwrap();
        assert_eq!(members.tally_votes(), Tally::Won);
        members.reset_votes();
        members.record_vote(2, false).unwrap();
        members.record_vote(3, false).unwrap();
        assert_eq!(members.tally_votes(), Tally::Lost);
        let joint = members
            .configuration()
            .enter_joint(
                false,
                &[
                    Change::AddVoter(5),
                    Change::AddVoter(6),
                    Change::Remove(1),
                    Change::Remove(2),
                ],
            )
            .unwrap()
            .configuration;
        members.apply(joint, &[], 1).unwrap();
        members.reset_votes();
        // Incoming {3, 5, 6}, outgoing {1, 2, 3}.
        for member in [3, 5, 6] {
            members.record_vote(member, true).unwrap();
        }
        assert_eq!(members.tally_votes(), Tally::Pending);
        members.record_vote(1, true).unwrap();
        assert_eq!(members.tally_votes(), Tally::Won);
        assert!(members.has_quorum(&[1, 3, 5]));
        assert!(!members.has_quorum(&[3, 5, 6]));
        assert!(!members.has_quorum(&[1, 2, 3]));
        assert!(!members.is_singleton());
        assert!(tracker(&[7], &[8]).is_singleton());
    }
    #[test]
    fn a_leader_looks_for_a_quorum_it_heard_from_and_forgets() {
        let mut members = tracker(&[1, 2, 3], &[4]);
        for (_, progress) in members.iter_mut() {
            progress.recent_active = false;
        }
        assert!(!members.quorum_recently_active(1));
        members.get_mut(4).unwrap().recent_active = true;
        // A learner heard from is no part of the quorum.
        assert!(!members.quorum_recently_active(1));
        members.get_mut(3).unwrap().recent_active = true;
        assert!(members.quorum_recently_active(1));
        assert!(members.get(1).unwrap().recent_active);
        assert!(!members.get(3).unwrap().recent_active);
        assert!(!members.quorum_recently_active(1));
        assert!(members.resident_bytes() > 0);
    }
}
