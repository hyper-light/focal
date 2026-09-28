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
//! **A fast quorum counts once the group's configuration is applied.** The
//! leader commits by the fast quorum only while no change is committed and
//! not applied and the configuration is not joint, so that the quorum it
//! counts is of the configuration a later leader counts by.
use crate::{
    NodeId,
    error::{Error, Result},
    fast::{self, Votes, same},
    log::{copy_entries_of, copy_entry},
    proto::{self, Entry, EntryType, Message},
    raft::{FastStats, Raft, StateRole},
    storage::Storage,
};

/// Whether `entry` may go by the fast track: it states something, and it
/// is no change of the configuration.
fn proposable(entry: &Entry) -> bool {
    entry.entry_type == EntryType::EntryNormal as i32
        && !entry.data.is_empty()
        && entry.index != 0
        && entry.index != u64::MAX
}

impl<S: Storage> Raft<S> {
    pub fn fast(&self) -> bool {
        self.config.fast
    }
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
            .filter(|index| *index != u64::MAX)
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
            self.decided.holds(entry.index, holder)?;
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
                    if progress.matched >= index || decided.binary_search(&member).is_ok() {
                        holding.push(member);
                    }
                }
                if !self.tracker.has_fast_quorum(&holding) {
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
                        .saturating_mul(crate::MAX_MEMBERS)
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
    pub(crate) fn unstable_proposals(&self, into: &mut Vec<Entry>) -> Result<()> {
        copy_entries_of(self.held.unstable(), into)
    }
}
