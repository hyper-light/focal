//! The fast track as a member runs it ([`crate::fast`] says what it is and
//! keeps what it needs).
//!
//! **The leader takes what it hears of first.** For the next index of its
//! log it takes the first entry it hears of, from the proposer or from a
//! voter that holds it, and sends it to its members as it sends any entry.
//! The index is committed by whichever quorum comes first: the fast quorum
//! that holds the entry, or the classic quorum that holds it from the
//! leader. Fast Raft as its authors state it waits for the votes of a
//! classic quorum before the leader takes an entry, and pays a round when
//! the fast quorum does not come; taking at once never costs more than the
//! classic track does.
//!
//! It is safe for the reason the classic track is. Only a leader commits.
//! What a leader committed by the classic quorum every later leader holds
//! in its log, for the quorum that elected it has a member that held the
//! entry from the leader and votes for no one whose log is behind its own.
//! What a leader committed by the fast quorum R every later leader takes at
//! its election: of the members that elected it, more hold that entry by
//! themselves than are outside R, so it is the most held among them, and
//! one that holds it from the leader votes for no one whose log lacks it.
//!
//! **A vote counts once the voter's log is of the leader's term.** That
//! argument needs the later leader's log to end below the index, for the
//! entry most held is taken only above the log. An election compares logs
//! alone, and a member that holds the entry beside a log of older terms
//! votes for a candidate whose log fills the index with an entry of an
//! older term: elected, it keeps that entry, and commits it in place of
//! the one committed (found by the schedules: `fast.rs`,
//! `an_election_never_commits_a_second_entry_at_a_committed_index`). So a
//! member that holds the entry beside its log counts for the fast quorum
//! only once the leader knows its log holds an entry of the leader's term
//! (`matched` is of that term). Every member of R then votes for no
//! candidate whose last term is older than the leader's, by the classic
//! rule, and a candidate whose last term is the leader's or later holds
//! that leader's log, or a later leader's, which holds the entry, through
//! its last entry: it holds the entry, or its log ends below the index and
//! it takes the entry most held. This is Fast Paxos's condition that a
//! value is chosen in a round only by votes cast in that round (Lamport,
//! "Fast Paxos", 2006, §3.3, condition O4), with the round a vote was cast in
//! kept where elections read it: a vote is of the leader's round once the
//! voter's log is. It costs no message and no state: the leader's first
//! entry of its term reaches its members with its first append.
//!
//! **A fast quorum counts once the group's configuration is applied.** The
//! leader commits by the fast quorum only while no change is committed and
//! not applied and the configuration is not joint.
//!
//! **And it is a fast quorum of every configuration a member may count
//! by.** A member campaigns by the configuration it has applied, which can
//! be older than the leader's: one that has not heard the change
//! committed. Raft's classic argument holds across one change because any
//! two majorities of configurations one change apart meet; a fast quorum
//! of the new configuration need not be a fast quorum of the old, and a
//! member counting by the old was elected by voters most of whom held
//! another entry (`fast.rs`,
//! `a_member_that_counts_by_the_configuration_before_commits_no_second_entry`).
//! A member that took an entry of this leader's term took with it this
//! leader's commit, which covers the configuration the leader was elected
//! under, and it campaigns only once it has applied what it committed: it
//! counts by that configuration or by one the leader applied since. One
//! that took no entry of the term has an older last term than every member
//! of the fast quorum, which refuse it, and no majority of a configuration
//! near this one is without them. So the leader notes the voters it was
//! elected under and the one other set a change since named, and counts a
//! fast quorum only where it is one of each; after a second change it
//! commits by the classic quorum until the next term.
use crate::{
    NodeId,
    error::{Error, Result},
    fast::{self, Votes, same},
    log::{copy_entries_of, copy_entry},
    proto::{self, Entry, EntryType, Message},
    quorum,
    raft::{FastStats, LAST, Raft, StateRole},
    storage::Storage,
};

/// Whether `entry` may go by the fast track: it states something, it is no
/// change of the configuration, and it lies below the last index a member
/// reaches ([`crate::raft::LAST`]): a leader that recovers it at its election
/// writes its own first entry after it, which needs an index of its own
/// (mantle note 32 R6).
fn proposable(entry: &Entry) -> bool {
    entry.entry_type == EntryType::EntryNormal
        && !entry.data.is_empty()
        && entry.index != 0
        && entry.index < LAST
}

impl<S: Storage> Raft<S> {
    /// Whether this member's group runs the fast track ([`crate::Config::fast`]).
    pub fn fast(&self) -> bool {
        self.config.fast
    }
    /// What the fast track did at this member since it opened.
    pub fn fast_stats(&self) -> FastStats {
        self.fast_stats
    }
    /// What this member holds approved by itself, in order of index.
    pub fn proposals(&self) -> impl Iterator<Item = &Entry> {
        self.held.iter()
    }
    fn window(&self) -> u64 {
        self.log
            .committed()
            .saturating_add(self.config.limits.fast_window)
    }
    fn voters(&self, into: &mut Vec<NodeId>) -> Result<()> {
        into.clear();
        let configuration = self.tracker.configuration();
        for member in configuration.members() {
            if configuration.votes(member) {
                into.try_reserve(1).map_err(|_| Error::Memory)?;
                into.push(member);
            }
        }
        Ok(())
    }

    /// Proposes `data` by the fast track: to every voter, for the index
    /// after what this member holds. The index it was proposed for; what
    /// became of it the log says, and [`crate::Ready::displaced`] when
    /// another entry took the index.
    ///
    /// A leader proposes as it always did: it is where the fast track
    /// leads.
    pub fn propose_fast(&mut self, context: Vec<u8>, data: Vec<u8>) -> Result<u64> {
        if !self.config.fast {
            return Err(Error::Settings("the group has no fast track"));
        }
        if data.is_empty() {
            return Err(Error::ProposalDropped);
        }
        if self.msgs.len() >= self.config.limits.pending_messages {
            return Err(Error::Capacity("messages that wait to be taken"));
        }
        let mut entry = Entry {
            data,
            context,
            ..Entry::default()
        };
        if self.state == StateRole::Leader {
            let index = self.log.last_index()?.saturating_add(1);
            let mut message = proto::message(0, proto::MessageType::MsgPropose);
            message.from = self.id;
            message
                .entries
                .try_reserve_exact(1)
                .map_err(|_| Error::Capacity("a proposal"))?;
            message.entries.push(entry);
            self.step(message)?;
            return Ok(index);
        }
        // One that knows no leader proposes to no one who could decide.
        if self.leader_id == 0 {
            return Err(Error::ProposalDropped);
        }
        if self.displaced.len() >= self.config.limits.proposals {
            return Err(Error::Capacity("proposals whose end was not taken"));
        }
        let index = self
            .log
            .last_index()?
            .max(self.held.last_index())
            .checked_add(1)
            .filter(|index| *index < LAST)
            .ok_or(Error::Capacity("the log's indexes"))?;
        if index > self.window() {
            return Err(Error::Capacity("indexes open to proposals"));
        }
        entry.index = index;
        entry.term = self.term;
        let mut voters = std::mem::take(&mut self.holders);
        let sent = self.send_proposal(&entry, &mut voters);
        self.holders = voters;
        sent?;
        self.fast_stats.proposed = self.fast_stats.proposed.saturating_add(1);
        Ok(index)
    }
    fn send_proposal(&mut self, entry: &Entry, voters: &mut Vec<NodeId>) -> Result<()> {
        self.voters(voters)?;
        // Everything that can refuse does before anything is sent.
        let mut messages = Vec::new();
        messages
            .try_reserve_exact(voters.len())
            .map_err(|_| Error::Capacity("a proposal"))?;
        for voter in voters.iter().filter(|voter| **voter != self.id) {
            let mut message = Message {
                msg_type: fast::FAST_PROPOSE,
                to: *voter,
                ..Message::default()
            };
            message
                .entries
                .try_reserve_exact(1)
                .map_err(|_| Error::Capacity("a proposal"))?;
            message
                .entries
                .push(copy_entry(entry).map_err(|_| Error::Capacity("a proposal"))?);
            messages.push(message);
        }
        if self.tracker.configuration().votes(self.id) {
            let held = copy_entry(entry).map_err(|_| Error::Capacity("a proposal"))?;
            self.held.hold(held, false, true)?;
        }
        for message in messages {
            self.send(message)?;
        }
        Ok(())
    }

    /// A proposal arrived.
    pub(crate) fn hear_proposal(&mut self, message: Message) -> Result<()> {
        if !self.config.fast {
            return Ok(());
        }
        for entry in message.entries.iter().take(self.config.limits.proposals) {
            if !proposable(entry) {
                return Err(Error::Violation(
                    "a proposal that may not go by the fast track",
                ));
            }
            if self.state == StateRole::Leader {
                self.leader_hears(entry, None)?;
            } else {
                self.hold(entry)?;
            }
        }
        if self.state == StateRole::Leader {
            self.decide()?;
        }
        Ok(())
    }
    /// Holds `entry` if nothing is held at its index; and says again what
    /// is held there, for its proposer proposes again when the leader did
    /// not hear.
    fn hold(&mut self, entry: &Entry) -> Result<()> {
        if !self.tracker.configuration().votes(self.id)
            || entry.index <= self.log.last_index()?
            || entry.index > self.window()
        {
            return Ok(());
        }
        let Ok(copy) = copy_entry(entry) else {
            return Ok(());
        };
        match self.held.hold(copy, false, false) {
            // What there is no room for is not held, and not voted for.
            Err(Error::Capacity(_)) => Ok(()),
            Ok(true) => {
                self.fast_stats.held = self.fast_stats.held.saturating_add(1);
                Ok(())
            }
            Err(error) => Err(error),
            Ok(false) => {
                let durable = self.held.durable().any(|held| held.index == entry.index);
                if durable && self.state == StateRole::Follower && self.leader_id != 0 {
                    self.send_vote(entry.index)?;
                }
                Ok(())
            }
        }
    }
    fn send_vote(&mut self, index: u64) -> Result<()> {
        let Some(held) = self.held.get(index) else {
            return Ok(());
        };
        let mut message = Message {
            msg_type: fast::FAST_VOTE,
            to: self.leader_id,
            commit: self.log.committed(),
            ..Message::default()
        };
        message
            .entries
            .try_reserve_exact(1)
            .map_err(|_| Error::Memory)?;
        message.entries.push(copy_entry(held)?);
        self.send(message)
    }
    /// Storage holds `entries` of what this member approved by itself: it
    /// may say so.
    pub fn on_persist_proposals(&mut self, entries: &[Entry]) -> Result<()> {
        for entry in entries {
            if self.held.persisted(entry)
                && self.state == StateRole::Follower
                && self.leader_id != 0
                && self.voted_to == (self.term, self.leader_id)
            {
                self.send_vote(entry.index)?;
            }
        }
        Ok(())
    }
    /// A leader was heard from. One that was not yet told what this
    /// member holds is told.
    pub(crate) fn heard_leader(&mut self) -> Result<()> {
        if !self.config.fast
            || self.state != StateRole::Follower
            || self.leader_id == 0
            || self.voted_to == (self.term, self.leader_id)
        {
            return Ok(());
        }
        self.voted_to = (self.term, self.leader_id);
        let mut indexes = Vec::new();
        indexes
            .try_reserve_exact(self.held.len())
            .map_err(|_| Error::Memory)?;
        indexes.extend(self.held.durable().map(|held| held.index));
        for index in indexes {
            self.send_vote(index)?;
        }
        Ok(())
    }
    /// The log reaches `index`: what was held at or below it is held no
    /// more, and what was proposed here and not taken is said.
    pub(crate) fn release_proposals(&mut self, index: u64) -> Result<()> {
        if self.held.is_empty() {
            return Ok(());
        }
        let Self {
            held: proposals,
            log,
            displaced,
            ..
        } = self;
        let before = displaced.len();
        proposals.release(
            index,
            |held| {
                log.slice(held.index, held.index.saturating_add(1), u64::MAX)
                    .is_ok_and(|taken| taken.first().is_some_and(|taken| same(taken, held)))
            },
            displaced,
        )?;
        let lost = u64::try_from(displaced.len().saturating_sub(before)).unwrap_or(u64::MAX);
        self.fast_stats.displaced = self.fast_stats.displaced.saturating_add(lost);
        Ok(())
    }

    /// What a voter holds arrived.
    pub(crate) fn step_fast_vote(&mut self, message: Message) -> Result<()> {
        if !self.config.fast || message.term == 0 {
            return Ok(());
        }
        if message.term > self.term {
            // One that is of a later term knows of a leader this member
            // does not.
            return self.become_follower(message.term, 0);
        }
        if message.term < self.term
            || self.state != StateRole::Leader
            || !self.tracker.configuration().votes(message.from)
        {
            return Ok(());
        }
        if let Some(progress) = self.tracker.get_mut(message.from) {
            progress.recent_active = true;
        }
        for entry in message.entries.iter().take(self.config.limits.proposals) {
            if !proposable(entry) {
                return Err(Error::Violation(
                    "a vote for what may not go by the fast track",
                ));
            }
            self.leader_hears(entry, Some(message.from))?;
        }
        self.decide()?;
        if self.maybe_commit()? {
            self.bcast_append()?;
        }
        Ok(())
    }
    /// A leader hears of `entry`, which `holder` holds if it is some.
    fn leader_hears(&mut self, entry: &Entry, holder: Option<NodeId>) -> Result<()> {
        let last = self.log.last_index()?;
        if entry.index > last {
            if entry.index > self.window() {
                return Ok(());
            }
            // A vote there is no room for is one the fast quorum comes
            // without: the index is committed by the classic one.
            return match self.votes.vote(holder.unwrap_or(self.id), entry) {
                Err(Error::Capacity(_)) => Ok(()),
                other => other,
            };
        }
        let Some(holder) = holder else {
            return Ok(());
        };
        // Decided: whether the voter holds what was taken.
        if entry.index <= self.log.committed() || !self.decided.knows(entry.index) {
            return Ok(());
        }
        let taken = self
            .log
            .slice(entry.index, entry.index.saturating_add(1), u64::MAX)?;
        if taken.first().is_some_and(|taken| same(taken, entry)) {
            self.decided
                .holds(entry.index, holder, self.config.limits.members)?;
        }
        Ok(())
    }
    /// Takes for each next index of the log what is heard of for it.
    fn decide(&mut self) -> Result<()> {
        if self.state != StateRole::Leader || self.lead_transferee.is_some() {
            return Ok(());
        }
        let mut took = false;
        // Every index taken is one of the window.
        for _ in 0..self.config.limits.fast_window {
            let index = self.log.last_index()?.saturating_add(1);
            let Some((entry, holders)) = self.votes.most(index) else {
                break;
            };
            let mut taken = copy_entry(entry)?;
            taken.index = 0;
            taken.term = 0;
            let mut holding = std::mem::take(&mut self.holders);
            holding.clear();
            let reserved = holding.try_reserve(holders.len());
            // The leader holds what it took once its log is durable, and
            // its progress says so.
            holding.extend(holders.iter().filter(|holder| **holder != self.id));
            let outcome = match reserved {
                Err(_) => Err(Error::Memory),
                Ok(()) => self.take(index, taken, &holding),
            };
            self.holders = holding;
            if !outcome? {
                break;
            }
            took = true;
        }
        if took {
            self.bcast_append()?;
        }
        Ok(())
    }
    /// False when the leader may hold no more uncommitted.
    fn take(&mut self, index: u64, entry: Entry, holders: &[NodeId]) -> Result<bool> {
        if self.decided.knows(index) {
            return Err(Error::Invariant("an index decided twice"));
        }
        let mut entries = Vec::new();
        entries.try_reserve_exact(1).map_err(|_| Error::Memory)?;
        entries.push(entry);
        if !self.append_entry(entries)? {
            return Ok(false);
        }
        self.decided
            .decide(index, holders, self.config.limits.unstable_entries)?;
        self.votes.release(index);
        self.fast_stats.taken = self.fast_stats.taken.saturating_add(1);
        Ok(true)
    }
    /// Commits what a fast quorum holds, index by index. True when the
    /// commit moved.
    pub(crate) fn fast_commit(&mut self) -> Result<bool> {
        if !self.config.fast
            || self.state != StateRole::Leader
            || self.decided.is_empty()
            || self.has_pending_conf()
            || self.tracker.configuration().is_joint()
        {
            return Ok(false);
        }
        let mut moved = false;
        let mut holding = std::mem::take(&mut self.holders);
        let outcome = (|| -> Result<()> {
            for _ in 0..self.config.limits.fast_window {
                let index = self.log.committed().saturating_add(1);
                if !self.decided.knows(index) || self.log.term(index)? != self.term {
                    break;
                }
                holding.clear();
                holding
                    .try_reserve(self.tracker.len())
                    .map_err(|_| Error::Memory)?;
                let decided = self.decided.holders(index);
                for (member, progress) in self.tracker.iter() {
                    // One that holds the entry beside its log counts only
                    // once its log holds an entry of this term: then its
                    // log says, to every later election, that it took
                    // this leader's word, and it votes for no one whose
                    // log is older (module header).
                    let beside = || {
                        decided.binary_search(&member).is_ok()
                            && (self.planted(crate::Mutant::FastBesideAnyTerm)
                                || self
                                    .log
                                    .term(progress.matched)
                                    .is_ok_and(|term| term == self.term))
                    };
                    if progress.matched >= index || beside() {
                        holding.push(member);
                    }
                }
                if !self.fast_quorum_of_the_term(&holding) {
                    break;
                }
                self.log.commit_to(index)?;
                self.fast_stats.committed = self.fast_stats.committed.saturating_add(1);
                moved = true;
            }
            Ok(())
        })();
        self.holders = holding;
        outcome?;
        Ok(moved)
    }

    /// A member that is elected notes the configuration it counts by: a
    /// member that takes an entry of its term from it takes with it a
    /// commit that reaches that configuration, and counts by it or by one
    /// this member applies after.
    pub(crate) fn note_term_configuration(&mut self) -> Result<()> {
        self.term_voters.clear();
        self.term_next.clear();
        let configuration = self.tracker.configuration();
        self.term_known = self.config.fast && !configuration.is_joint();
        if !self.term_known {
            return Ok(());
        }
        let voters = configuration.voters();
        self.term_voters
            .try_reserve(voters.len())
            .map_err(|_| Error::Memory)?;
        self.term_voters.extend_from_slice(voters);
        Ok(())
    }
    /// A leader applied a change: its sets of voters join those of its
    /// term. A third set leaves what a member counts by not known here
    /// until the next term.
    pub(crate) fn note_term_change(&mut self) -> Result<()> {
        if !self.term_known {
            return Ok(());
        }
        let configuration = self.tracker.configuration();
        for voters in [configuration.voters(), configuration.outgoing()] {
            if voters.is_empty() || voters == self.term_voters.as_slice() {
                continue;
            }
            if self.term_next.is_empty() {
                self.term_next
                    .try_reserve(voters.len())
                    .map_err(|_| Error::Memory)?;
                self.term_next.extend_from_slice(voters);
            } else if voters != self.term_next.as_slice() {
                self.term_known = false;
            }
        }
        Ok(())
    }
    /// Whether `holding`, in order, is a fast quorum of every configuration
    /// a member that holds an entry of this term may count by: the one in
    /// force, the one this member was elected under, and the one other set
    /// of voters a change since named. Of more, it says no: what a member
    /// counts by is then not known here, and the index is committed by the
    /// classic quorum.
    fn fast_quorum_of_the_term(&self, holding: &[NodeId]) -> bool {
        let fast = |voters: &[NodeId]| {
            quorum::tally(voters, quorum::Quorum::Fast, |member| {
                holding.binary_search(&member).ok().map(|_| true)
            }) == quorum::Tally::Won
        };
        if self.planted(crate::Mutant::FastAnyConfiguration) {
            return self.tracker.has_fast_quorum(holding);
        }
        self.term_known
            && self.tracker.has_fast_quorum(holding)
            && fast(&self.term_voters)
            && (self.term_next.is_empty() || fast(&self.term_next))
    }
    /// One that granted its vote said what it holds.
    pub(crate) fn hear_report(&mut self, message: &Message) -> Result<()> {
        if !self.config.fast || !self.tracker.configuration().votes(message.from) {
            return Ok(());
        }
        let last = self.log.last_index()?;
        for entry in &message.entries {
            if !proposable(entry) {
                return Err(Error::Violation(
                    "a vote for what may not go by the fast track",
                ));
            }
            if entry.index <= last {
                continue;
            }
            // One that is elected decides by all it was told: what it has
            // no room for it may not set aside, and it is not elected.
            self.votes.vote(message.from, entry)?;
        }
        Ok(())
    }
    /// What a member that is elected takes before it writes an entry of
    /// its own: for every index above its log that a voter holds an entry
    /// at, the entry most held among those that elected it, itself among
    /// them; and for an index none of them holds anything at, an entry
    /// that states nothing.
    pub(crate) fn recover(&mut self, mut reports: Votes, last: u64) -> Result<Vec<Entry>> {
        let mut taken = Vec::new();
        if !self.config.fast {
            return Ok(taken);
        }
        reports.release(last);
        for held in self.held.durable() {
            if held.index > last {
                reports.vote(self.id, held)?;
            }
        }
        let highest = reports.last_index();
        if highest <= last {
            return Ok(taken);
        }
        let count = usize::try_from(highest.saturating_sub(last))
            .ok()
            .filter(|count| {
                *count
                    <= self
                        .config
                        .limits
                        .proposals
                        .saturating_mul(self.config.limits.members)
            })
            .ok_or(Error::Capacity("indexes held above the log"))?;
        taken.try_reserve_exact(count).map_err(|_| Error::Memory)?;
        let mut index = last;
        for _ in 0..count {
            index = index.saturating_add(1);
            match reports.most(index) {
                Some((entry, holders)) => {
                    let mut entry = copy_entry(entry)?;
                    entry.index = 0;
                    entry.term = 0;
                    taken.push(entry);
                    self.fast_stats.recovered = self.fast_stats.recovered.saturating_add(1);
                    self.decided
                        .decide(index, holders, self.config.limits.unstable_entries)?;
                }
                None => taken.push(Entry::default()),
            }
        }
        Ok(taken)
    }
    /// What was proposed here and another entry took the index of.
    pub(crate) fn take_displaced(&mut self) -> Vec<Entry> {
        std::mem::take(&mut self.displaced)
    }
    /// What this member approved by itself that no write was issued for.
    pub(crate) fn unissued_proposals(&self, into: &mut Vec<Entry>) -> Result<()> {
        copy_entries_of(self.held.unissued(), into)
    }
}
