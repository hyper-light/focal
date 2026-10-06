//! What a leader knows of each member, and what a candidate knows of its
//! votes.
use std::collections::VecDeque;

use crate::{
    Configuration, HeartbeatAnswers, NodeId, Quorum, Tally,
    error::{Error, Result},
    quorum,
    wire::MESSAGE_RECORD_FIXED_BYTES,
};

/// What a leader's append costs a member's path beyond its entries' bodies: its record's fixed
/// bytes ([`MESSAGE_RECORD_FIXED_BYTES`]).
const APPEND_FIXED_BYTES: u64 = MESSAGE_RECORD_FIXED_BYTES as u64;
/// RFC 1122 §4.2.3.4's `Fs`, "a fraction whose recommended value is 1/2", as the share of a window
/// that must be free again before a sender whose window filled sends: the divisor of the window's
/// bytes it waits to fall to.
const SILLY_WINDOW_DIVISOR: u64 = 2;

/// How a leader sends to a member.
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

/// The messages sent and not answered, in order: the last index of each
/// and the bytes it costs the path, its record ([`APPEND_FIXED_BYTES`] and its
/// entries' bodies, which is what the wire format writes). A window of at most
/// `cap` messages and `byte_cap` bytes: a message of four megabytes and one of
/// forty bytes each take one place of the first, and what they cost of the
/// second. The bytes are what the path to the member carries before it
/// answers, which its owner learns and says (`set_byte_cap`; mantle note 32
/// R16: twice what the path carries in a round trip).
///
/// A window of `usize::MAX` places counts no messages of its own: its bytes
/// bound it, for every message costs at least its fixed bytes and an entry's,
/// and its ring grows to what they admit, as it is used (raft-rs's count is
/// kept for the comparison, and a finite one reserves its ring whole at the
/// first message, as before).
///
/// A window whose bytes reached their bound sends nothing more until it has
/// room for a whole append (its fixed bytes and a page of entries) or half of
/// its bound is answered, whichever comes first: the sender's avoidance of the
/// silly window syndrome (Clark, RFC 813 §3–4; RFC 1122 §4.2.3.4, which sends
/// "if a maximum-sized segment can be sent" or "at least a fraction Fs of the
/// maximum window", `Fs` "a fraction whose recommended value is 1/2"). Without
/// it a window held at its bound by a path that carries no more opens by one
/// small answer at a time, and each opening draws one small append, its fixed
/// bytes beside one entry: on a saturated path the window spends most of what
/// the path carries on fixed bytes (`tests/timed.rs`). Below its bound a window
/// sends at once (no Nagle's rule, RFC 896).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Inflights {
    buffer: VecDeque<(u64, u64)>,
    cap: usize,
    /// The bytes of the messages in the window.
    bytes: u64,
    byte_cap: u64,
    /// The bytes reached their bound, and the room has not yet come back
    /// to a whole append or half the bound.
    draining: bool,
    /// What a whole append costs: its fixed bytes and a page of entries
    /// (`Config::max_size_per_msg`).
    page: u64,
}
/// What a whole append of `page` bytes of entries costs the path: those and its fixed bytes.
pub(crate) fn whole_append(page: u64) -> u64 {
    page.saturating_add(APPEND_FIXED_BYTES)
}
impl Inflights {
    /// An empty window of at most `cap` messages and `byte_cap` bytes;
    /// its buffer is reserved when the first is sent.
    pub fn new(cap: usize, byte_cap: u64) -> Self {
        Self {
            buffer: VecDeque::new(),
            cap,
            bytes: 0,
            byte_cap: byte_cap.max(1),
            draining: false,
            page: u64::MAX,
        }
    }
    /// What a whole append costs ([`whole_append`]): a window that filled sends
    /// again once it has room for one.
    pub fn set_page(&mut self, page: u64) {
        self.page = page;
        self.settle();
    }
    /// No message more is sent: every place is taken, or the bytes in
    /// flight reached their bound and have not yet given back room for a
    /// whole append or half the bound. A window that holds nothing is never
    /// full for its bytes, so one entry larger than the bound is sent,
    /// alone, and the member is not left waiting for good.
    pub fn full(&self) -> bool {
        self.buffer.len() >= self.cap || self.bytes >= self.byte_cap || self.draining
    }
    /// How many messages are out and not answered.
    pub fn count(&self) -> usize {
        self.buffer.len()
    }
    /// Whether the window counts messages of its own, or its bytes alone
    /// bound it.
    pub fn counts_messages(&self) -> bool {
        self.cap != usize::MAX
    }
    /// The bytes of the messages in the window.
    pub fn bytes(&self) -> u64 {
        self.bytes
    }
    /// The bytes the path to the member carries before it answers.
    pub fn byte_cap(&self) -> u64 {
        self.byte_cap
    }
    /// The bytes the next message may carry: what is left of the bound. A
    /// page is cut to it before it is copied, and holds one entry at
    /// least.
    pub fn room(&self) -> u64 {
        self.byte_cap.saturating_sub(self.bytes)
    }
    /// The path to the member carries `bytes` before it answers. Never
    /// nothing: a bound of none would send to the member no more. What is
    /// in flight stays counted; a bound below it admits nothing until
    /// enough is answered.
    pub fn set_byte_cap(&mut self, bytes: u64) {
        self.byte_cap = bytes.max(1);
        self.settle();
    }
    /// A window that filled sends again once it has room for a whole append,
    /// or half its bound.
    fn settle(&mut self) {
        let enough = self.page.min(self.byte_cap / SILLY_WINDOW_DIVISOR);
        if self.draining && self.room() >= enough {
            self.draining = false;
        }
    }
    /// A message whose last index is `inflight`, costing the path `bytes`,
    /// was sent; fatal into a full window.
    pub fn add(&mut self, inflight: u64, bytes: u64) -> Result<()> {
        if self.full() {
            return Err(Error::Invariant("a message sent into a full window"));
        }
        if self.buffer.len() == self.buffer.capacity() {
            let reserved = if self.counts_messages() && self.buffer.capacity() == 0 {
                self.buffer.try_reserve_exact(self.cap)
            } else {
                self.buffer.try_reserve(1)
            };
            reserved.map_err(|_| Error::Memory)?;
        }
        self.buffer.push_back((inflight, bytes));
        self.bytes = self.bytes.saturating_add(bytes);
        if self.bytes >= self.byte_cap {
            self.draining = true;
        }
        Ok(())
    }
    /// The message out that carried what followed `after` arrived, though
    /// it is not answered: the member kept it ahead of a hole
    /// (`Ahead::Kept`). It is in flight no more, and its bytes leave the
    /// window, as RFC 6675's pipe leaves out a segment the receiver says it
    /// holds (RFC 2018); it keeps its place, its bytes nothing, for every
    /// message costs its fixed bytes and none other costs nothing. The
    /// message is the one after the message that ended at `after`, or the
    /// first out when all through `after` was answered; its last index, or
    /// nothing where no message out began after `after`.
    pub fn delivered(&mut self, after: u64, answered: u64) -> Option<u64> {
        let at = self.buffer.iter().position(|(last, _)| *last > after)?;
        let begins_after = match at.checked_sub(1) {
            Some(before) => self
                .buffer
                .get(before)
                .is_some_and(|(last, _)| *last == after),
            None => answered == after,
        };
        if !begins_after {
            return None;
        }
        let (last, held) = self.buffer.get_mut(at)?;
        self.bytes = self.bytes.saturating_sub(*held);
        *held = 0;
        let last = *last;
        self.settle();
        Some(last)
    }
    /// Whether the window holds a message the member was taken to have kept
    /// ahead of a hole ([`Inflights::delivered`]): until what it lacks before
    /// it arrives, the member's answers wait on that hole.
    pub fn holds_kept(&self) -> bool {
        self.buffer.iter().any(|(_, held)| *held == 0)
    }
    /// Whether the message out at `position` arrived and was kept
    /// ([`Inflights::delivered`]).
    pub fn delivered_at(&self, position: usize) -> bool {
        self.buffer
            .get(position)
            .is_some_and(|(_, held)| *held == 0)
    }
    /// The last index of the message out at `position`, in the order sent.
    pub fn last_at(&self, position: usize) -> Option<u64> {
        self.buffer.get(position).map(|(last, _)| *last)
    }
    /// Everything at or below `to` is answered, and its bytes are in
    /// flight no more.
    pub fn free_to(&mut self, to: u64) {
        while let Some((_, held)) = self.buffer.front().copied().filter(|(last, _)| *last <= to) {
            self.buffer.pop_front();
            self.bytes = self.bytes.saturating_sub(held);
        }
        self.settle();
    }
    /// The oldest message out is given up as answered, and the next may go
    /// whatever the window holds: the rule of raft-rs for a full window at a
    /// heartbeat's answer (`HeartbeatAnswers::Bare`), which waits for no half.
    pub fn free_first_one(&mut self) {
        if let Some((_, held)) = self.buffer.pop_front() {
            self.bytes = self.bytes.saturating_sub(held);
        }
        self.draining = false;
    }
    /// Nothing is in flight. The bound stays: it is the path's, not the
    /// window's.
    pub fn reset(&mut self) {
        self.bytes = 0;
        self.draining = false;
        self.buffer = VecDeque::new();
    }
    /// Whether the bytes counted are those of the messages held: what is
    /// credited on an answer is what was charged when it was sent.
    pub fn check(&self) -> Result<()> {
        let bytes = self
            .buffer
            .iter()
            .fold(0u64, |bytes, (_, held)| bytes.saturating_add(*held));
        if bytes != self.bytes {
            return Err(Error::Invariant("the bytes in flight do not add up"));
        }
        Ok(())
    }
    /// The bytes the window's buffer holds, by capacity.
    pub fn resident_bytes(&self) -> usize {
        self.buffer
            .capacity()
            .saturating_mul(std::mem::size_of::<(u64, u64)>())
    }
}

/// What a leader knows of one member, and how it sends to it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Progress {
    /// The highest index known to be held by the member.
    pub matched: u64,
    /// The index of the next entry to send.
    pub next_index: u64,
    /// How the leader sends to the member.
    pub state: ProgressState,
    /// While probing: a message is out and no other is sent.
    pub paused: bool,
    /// While a snapshot is on its way: its index.
    pub pending_snapshot: u64,
    /// The index the member asked a snapshot to reach; zero for none.
    pub pending_request_snapshot: u64,
    /// Whether the member was heard from since the leader last looked.
    pub recent_active: bool,
    /// The messages sent and not answered.
    pub inflights: Inflights,
    /// The commit the member last said its storage states durably: a
    /// member's answers carry its durable commit, not its commit (core step
    /// R-6, `docs/durable.md` §4.1), so a member that restarts reopens with
    /// at least this. For the leader itself, its commit; its own durable
    /// commit is `RawNode::durable_commit`.
    pub committed_index: u64,
    /// The leader's ticks since something sent to the member was out and
    /// none of it was answered for: a full window, or a probe
    /// (`HeartbeatAnswers`). A tick is the leader's own period, which its
    /// owner stretches by the path to the group's members; so a beat of
    /// ticks is time enough, on whatever path, for what was sent to have
    /// been answered.
    pub stalled: usize,
    /// The furthest index sent again for a hole before what the member kept
    /// (`Ahead::Kept`): each message out goes again once at most. While the
    /// member holds less, a resend is out, and a beat with no answer for it
    /// probes the member, as a full window does: the resend may be lost.
    pub repaired: u64,
}
impl Progress {
    /// A member probed from `next_index`, with a window of `window`
    /// messages and `window_bytes` bytes.
    pub fn new(next_index: u64, window: usize, window_bytes: u64) -> Self {
        Self {
            matched: 0,
            next_index,
            state: ProgressState::Probe,
            paused: false,
            pending_snapshot: 0,
            pending_request_snapshot: 0,
            recent_active: false,
            inflights: Inflights::new(window, window_bytes),
            committed_index: 0,
            stalled: 0,
            repaired: 0,
        }
    }
    fn reset_state(&mut self, state: ProgressState) {
        self.paused = false;
        self.stalled = 0;
        self.repaired = 0;
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
        self.stalled = 0;
        self.repaired = 0;
        self.inflights.reset();
    }
    /// Probes again from past what the member is known to hold, or past
    /// the snapshot it was sent.
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
    /// Sends ahead of answers from past what the member is known to hold.
    pub fn become_replicate(&mut self) {
        self.reset_state(ProgressState::Replicate);
        self.next_index = self.matched.saturating_add(1);
    }
    /// A snapshot at `index` is on its way.
    pub fn become_snapshot(&mut self, index: u64) {
        self.reset_state(ProgressState::Snapshot);
        self.pending_snapshot = index;
    }
    /// The snapshot on its way did not arrive.
    pub fn snapshot_failure(&mut self) {
        self.pending_snapshot = 0;
    }
    /// Whether a member sent a snapshot holds what the snapshot held.
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
            self.stalled = 0;
        }
        self.next_index = self.next_index.max(index.saturating_add(1));
        news
    }
    /// The member lost what it acknowledged after `held`, and states
    /// `committed` durable now (core step R-5): it is probed again from
    /// past `held`, and what it said of its commit before is forgotten.
    pub fn lost(&mut self, held: u64, committed: u64) {
        self.matched = held;
        self.committed_index = committed;
        self.pending_request_snapshot = 0;
        self.reset_state(ProgressState::Probe);
        self.next_index = held.saturating_add(1);
    }
    /// The member says it committed `committed`; the highest said is kept.
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
    /// The member answered a heartbeat (`HeartbeatAnswers`): `held` is
    /// how far it said it holds this leader's log, when it said so; `news`
    /// whether that was more than was known; `beat` the leader's ticks in a
    /// heartbeat.
    pub fn heard_heartbeat(
        &mut self,
        answers: HeartbeatAnswers,
        held: Option<u64>,
        news: bool,
        beat: usize,
    ) {
        match answers {
            HeartbeatAnswers::Bare => {
                self.paused = false;
                // A full window would wait on answers that may be lost.
                if self.state == ProgressState::Replicate && self.inflights.full() {
                    self.inflights.free_first_one();
                }
                // A hole sent again and answered for nothing in a beat: the
                // resend may be lost, and the member is probed.
                if self.state == ProgressState::Replicate
                    && (self.repaired > self.matched || self.inflights.holds_kept())
                    && self.stalled >= beat
                {
                    self.become_probe();
                }
            }
            HeartbeatAnswers::Position => match self.state {
                ProgressState::Replicate => {
                    if let Some(held) = held {
                        self.inflights.free_to(held);
                    }
                    // The window is full, and the member answers
                    // heartbeats and nothing of what is out. Nothing more
                    // is sent for that: what is out stays within its
                    // bound. Once a beat has passed with no answer for any
                    // of it, the member is not being reached by it — what
                    // its owner was not told was lost — and it is asked
                    // where it is.
                    // So too once a hole sent again, or one before what the
                    // member kept, went a beat with no answer for it: a
                    // member that restarted dropped what it kept, and the
                    // hole before it is in no message out (seed 1,318 of
                    // hyper-check's group campaign, `docs/sim.md` §14.5).
                    if !news
                        && (self.inflights.full()
                            || self.repaired > self.matched
                            || self.inflights.holds_kept())
                        && self.stalled >= beat
                    {
                        self.become_probe();
                    }
                }
                // The one message a probe is, is sent again when it was
                // told lost (`MsgUnreachable`), and when a beat has passed
                // since it was sent and the member answers heartbeats and
                // not it — not at every heartbeat's answer, which on a path
                // slower than the heartbeats sent the page again and again
                // behind itself.
                ProgressState::Probe if self.paused => {
                    if self.stalled >= beat {
                        self.paused = false;
                    }
                }
                ProgressState::Probe | ProgressState::Snapshot => {}
            },
        }
    }
    /// A tick of the leader passed: counted for a member with a probe out,
    /// a window that is full, a hole sent again or one before what it kept,
    /// and for no other.
    pub fn tick(&mut self) {
        let waits = match self.state {
            ProgressState::Probe => self.paused,
            ProgressState::Replicate => {
                self.inflights.full() || self.repaired > self.matched || self.inflights.holds_kept()
            }
            ProgressState::Snapshot => false,
        };
        self.stalled = if waits {
            self.stalled.saturating_add(1)
        } else {
            0
        };
    }
    /// Whether nothing more may be sent now: a probe is out, the window is
    /// full, or a snapshot is on its way.
    pub fn is_paused(&self) -> bool {
        match self.state {
            ProgressState::Probe => self.paused,
            ProgressState::Replicate => self.inflights.full(),
            ProgressState::Snapshot => true,
        }
    }
    /// The bytes of entries a page for the member may hold: `page`, and no
    /// more than the path to the member carries beside the append's own fixed
    /// bytes — what the window has room for while entries are sent ahead of
    /// their answers, the window's bound for the one message a probe is.
    pub fn page_bytes(&self, page: u64) -> u64 {
        match self.state {
            ProgressState::Replicate => {
                page.min(self.inflights.room().saturating_sub(APPEND_FIXED_BYTES))
            }
            ProgressState::Probe => {
                page.min(self.inflights.byte_cap().saturating_sub(APPEND_FIXED_BYTES))
            }
            ProgressState::Snapshot => page,
        }
    }
    /// Entries through `last` were sent, `bytes` of entries' bodies: their
    /// append costs the window those and its own fixed bytes.
    pub fn sent(&mut self, last: u64, bytes: u64) -> Result<()> {
        match self.state {
            ProgressState::Replicate => {
                self.next_index = last.saturating_add(1);
                self.inflights
                    .add(last, bytes.saturating_add(APPEND_FIXED_BYTES))
            }
            ProgressState::Probe => {
                self.paused = true;
                self.stalled = 0;
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
    /// The bytes a member's window admits until its owner says what the
    /// path to it carries.
    window_bytes: u64,
    /// The most members a configuration in force names ([`crate::Limits::members`]).
    members: usize,
    /// What a whole append costs ([`whole_append`]).
    page: u64,
    scratch: Vec<u64>,
}
impl Tracker {
    /// Every member of `configuration`, probed from `next_index` with a
    /// window of `window` messages and `window_bytes` bytes, of at most
    /// `members` members; no votes.
    pub fn new(
        configuration: Configuration,
        next_index: u64,
        window: usize,
        window_bytes: u64,
        members: usize,
    ) -> Result<Self> {
        let mut tracker = Self {
            progress: Vec::new(),
            configuration: Configuration::default(),
            votes: Vec::new(),
            window,
            window_bytes,
            members,
            page: u64::MAX,
            scratch: Vec::new(),
        };
        tracker.apply(configuration, &[], next_index)?;
        Ok(tracker)
    }
    /// What a whole append costs every member's window, now and as members
    /// join: its fixed bytes and `max_size_per_msg` of entries.
    pub fn set_page(&mut self, max_size_per_msg: u64) {
        self.page = whole_append(max_size_per_msg);
        for (_, progress) in &mut self.progress {
            progress.inflights.set_page(self.page);
        }
    }
    /// The configuration in force.
    pub fn configuration(&self) -> &Configuration {
        &self.configuration
    }
    fn find(&self, member: NodeId) -> std::result::Result<usize, usize> {
        self.progress
            .binary_search_by_key(&member, |(member, _)| *member)
    }
    /// What is known of `member`, if it is one.
    pub fn get(&self, member: NodeId) -> Option<&Progress> {
        let position = self.find(member).ok()?;
        self.progress.get(position).map(|(_, progress)| progress)
    }
    /// What is known of `member`, to change, if it is one.
    pub fn get_mut(&mut self, member: NodeId) -> Option<&mut Progress> {
        let position = self.find(member).ok()?;
        self.progress
            .get_mut(position)
            .map(|(_, progress)| progress)
    }
    /// How many members are tracked.
    pub fn len(&self) -> usize {
        self.progress.len()
    }
    /// Whether no member is tracked.
    pub fn is_empty(&self) -> bool {
        self.progress.is_empty()
    }
    /// In order of member.
    pub fn iter(&self) -> impl ExactSizeIterator<Item = (NodeId, &Progress)> {
        self.progress
            .iter()
            .map(|(member, progress)| (*member, progress))
    }
    /// In order of member, to change.
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
    /// Forgets the votes of the last election.
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
    /// What the votes recorded decide, in both halves of a joint
    /// configuration.
    pub fn tally_votes(&self) -> Tally {
        self.decided(|member| {
            self.votes
                .binary_search_by_key(&member, |(member, _)| *member)
                .ok()
                .and_then(|position| self.votes.get(position))
                .map(|(_, vote)| *vote)
        })
    }
    /// Whether `members`, in order, are a fast quorum of the voters. Of a
    /// joint configuration there is none: the fast track is closed while
    /// the group changes.
    pub fn has_fast_quorum(&self, members: &[NodeId]) -> bool {
        !self.configuration.is_joint()
            && quorum::tally(self.configuration.voters(), Quorum::Fast, |member| {
                members.binary_search(&member).ok().map(|_| true)
            }) == Tally::Won
    }
    /// Whether the members for which `holds` is true hold the quorum of
    /// both halves.
    pub fn quorum_of(&self, holds: impl Fn(NodeId) -> bool + Copy) -> bool {
        self.decided(|member| holds(member).then_some(true)) == Tally::Won
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
    /// is none any more is forgotten. A configuration past the members the
    /// member was opened for stops it: its group's other members, opened
    /// alike, hold it, and going on without it would leave this one in
    /// another configuration.
    pub fn apply(
        &mut self,
        configuration: Configuration,
        renewed: &[NodeId],
        next_index: u64,
    ) -> Result<()> {
        if configuration.members().count() > self.members {
            return Err(Error::Invariant(
                "a configuration of more members than the member was opened for",
            ));
        }
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
                    let mut new = Progress::new(next_index, self.window, self.window_bytes);
                    new.inflights.set_page(self.page);
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
    /// What is held for the members, by their number and their windows.
    /// The votes of an election and the room a quorum is counted in are
    /// an identity or an index a member at most; they are counted for
    /// every member whether they are held or not, so that what a member
    /// holds when it rests does not depend on what it did before.
    pub fn resident_bytes(&self) -> usize {
        let members = self.progress.len();
        let windows = self.progress.iter().fold(0usize, |bytes, (_, progress)| {
            bytes.saturating_add(progress.inflights.resident_bytes())
        });
        let each = std::mem::size_of::<(NodeId, Progress)>()
            .saturating_add(std::mem::size_of::<(NodeId, bool)>())
            .saturating_add(std::mem::size_of::<u64>())
            .saturating_add(std::mem::size_of::<NodeId>().saturating_mul(4));
        members.saturating_mul(each).saturating_add(windows)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Change;

    #[test]
    fn a_window_admits_its_bound_and_frees_in_order() {
        let mut window = Inflights::new(4, u64::MAX);
        for index in [10, 20, 30, 40] {
            assert!(!window.full());
            window.add(index, 1).unwrap();
        }
        assert!(window.full());
        assert!(window.add(50, 1).is_err());
        window.free_to(5);
        assert_eq!(window.count(), 4);
        window.free_to(20);
        assert_eq!(window.count(), 2);
        // It wraps.
        window.add(50, 1).unwrap();
        window.add(60, 1).unwrap();
        assert!(window.full());
        window.free_first_one();
        assert_eq!(window.count(), 3);
        window.free_to(55);
        assert_eq!(window.count(), 1);
        window.free_to(100);
        assert_eq!(window.count(), 0);
        window.free_first_one();
        for round in 0..100u64 {
            window.add(round, 1).unwrap();
            window.free_to(round);
            window.check().unwrap();
        }
        assert_eq!(window.count(), 0);
        assert_eq!(window.bytes(), 0);
        assert_eq!(window.resident_bytes(), 64);
        window.reset();
        assert_eq!(window.resident_bytes(), 0);
        assert!(Inflights::new(0, u64::MAX).full());
    }
    /// The window is a bound on bytes as on messages. Messages of many
    /// sizes fill it by what they hold; an answer gives back exactly what
    /// the messages it answers took, in whatever order answers come, twice
    /// or late; one entry larger than the bound is sent alone and never
    /// waited for in vain; and a bound that changes while messages are out
    /// keeps what is out counted.
    #[test]
    fn a_window_is_bounded_by_the_bytes_in_flight_and_gives_back_what_it_took() {
        let mut window = Inflights::new(128, 1_000);
        // Small and large together: full by bytes long before by places.
        for (index, bytes) in [(1, 10), (2, 400), (3, 589)] {
            assert!(!window.full());
            window.add(index, bytes).unwrap();
        }
        assert_eq!((window.count(), window.bytes(), window.room()), (3, 999, 1));
        assert!(!window.full());
        // The page that follows is cut to the room, and holds one entry
        // at least: it may pass the bound by that entry.
        window.add(4, 64).unwrap();
        assert!(window.full());
        assert_eq!(window.room(), 0);
        assert!(window.add(5, 1).is_err());
        window.check().unwrap();
        // An answer out of date gives nothing back; one that repeats gives
        // back once; one that skips ahead gives back all it covers.
        window.free_to(0);
        assert_eq!(window.bytes(), 1_063);
        window.free_to(1);
        window.free_to(1);
        assert_eq!((window.count(), window.bytes()), (3, 1_053));
        window.free_to(3);
        assert_eq!((window.count(), window.bytes()), (1, 64));
        window.free_to(2);
        assert_eq!((window.count(), window.bytes()), (1, 64));
        window.check().unwrap();
        // The bound falls below what is out: nothing more is admitted
        // until enough is answered, and what is out stays counted.
        window.set_byte_cap(32);
        assert!(window.full());
        window.free_to(4);
        assert_eq!((window.count(), window.bytes()), (0, 0));
        assert!(!window.full());
        // One entry larger than the bound goes, alone.
        window.add(5, 4_000_000).unwrap();
        assert!(window.full());
        window.free_first_one();
        assert!(!window.full());
        assert_eq!(window.bytes(), 0);
        // The bound rises: more is admitted at once.
        window.add(6, 30).unwrap();
        window.add(7, 30).unwrap();
        assert!(window.full());
        window.set_byte_cap(1 << 20);
        assert!(!window.full());
        assert_eq!(window.room(), (1 << 20) - 60);
        // A change of state empties the window and keeps the bound.
        window.reset();
        assert_eq!((window.count(), window.bytes()), (0, 0));
        assert_eq!(window.byte_cap(), 1 << 20);
        window.check().unwrap();
        // A bound of nothing would send no more: it is one byte at least.
        window.set_byte_cap(0);
        assert!(!window.full());
        assert_eq!(Inflights::new(4, 0).byte_cap(), 1);
    }
    /// A window whose bytes reached their bound sends nothing more until it has room for a whole
    /// append or half of it is answered: one small answer does not draw one small append (RFC 1122
    /// §4.2.3.4's sender avoidance of the silly window syndrome, Clark, RFC 813). Then it sends
    /// again, a page of what is free; a window that never reached its bound sends at once, as
    /// before; and a bound that rises past twice what is out opens it at once. Where a whole
    /// append is less than half the window, room for one is enough.
    #[test]
    fn a_window_that_filled_waits_for_a_whole_append_or_half_of_it() {
        let mut pages = Inflights::new(usize::MAX, 1_000);
        pages.set_page(whole_append(150));
        for index in 1..=4 {
            pages.add(index, 300).unwrap();
        }
        assert!(pages.full());
        pages.free_to(1);
        assert_eq!(pages.room(), 100);
        assert!(pages.full(), "no room for a whole append of 246 bytes");
        pages.free_to(2);
        assert_eq!(pages.room(), 400);
        assert!(!pages.full(), "room for a whole append, short of half");
        let mut window = Inflights::new(usize::MAX, 1_000);
        for (index, bytes) in [(1, 300), (2, 300), (3, 300)] {
            window.add(index, bytes).unwrap();
            assert!(!window.full(), "below its bound, nothing waits");
        }
        window.add(4, 300).unwrap();
        assert!(window.full());
        window.free_to(1);
        assert_eq!((window.bytes(), window.room()), (900, 100));
        assert!(window.full(), "one small answer opens nothing");
        window.free_to(2);
        assert!(window.full());
        window.free_to(3);
        assert_eq!(window.bytes(), 300);
        assert!(!window.full(), "half of it answered");
        assert_eq!(window.room(), 700);
        window.add(5, 700).unwrap();
        assert!(window.full());
        window.set_byte_cap(2_000);
        assert!(!window.full(), "a bound of twice what is out");
        window.check().unwrap();
        // raft-rs's rule frees the first message and sends the next, whatever the window holds.
        window.add(6, 1_000).unwrap();
        assert!(window.full());
        window.free_first_one();
        assert!(!window.full());
    }
    #[test]
    fn an_answer_moves_a_member_forward_and_never_back() {
        let mut progress = Progress::new(5, 8, u64::MAX);
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
            let mut progress = Progress::new(next, 8, u64::MAX);
            progress.state = state;
            progress.matched = matched;
            assert_eq!(progress.maybe_decrease_to(rejected, hint, 0), moved);
            assert_eq!(progress.next_index, after);
            assert_eq!(progress.matched, matched);
        }
        // A request for a snapshot is never out of date.
        let mut progress = Progress::new(10, 8, u64::MAX);
        progress.state = ProgressState::Replicate;
        progress.matched = 5;
        assert!(progress.maybe_decrease_to(5, 5, 7));
        assert_eq!(
            (progress.pending_request_snapshot, progress.next_index),
            (7, 10)
        );
        let mut progress = Progress::new(10, 8, u64::MAX);
        assert!(progress.maybe_decrease_to(3, 3, 7));
        assert_eq!(progress.pending_request_snapshot, 7);
        assert!(progress.maybe_decrease_to(3, 3, 9));
        assert_eq!(progress.pending_request_snapshot, 7);
    }
    #[test]
    fn a_member_is_paused_by_its_state() {
        let fixed = APPEND_FIXED_BYTES;
        let mut progress = Progress::new(1, 3, fixed + 100);
        assert!(!progress.is_paused());
        // Probed, a member is sent one message of what its path carries, its
        // fixed bytes and its entries.
        assert_eq!(progress.page_bytes(4_096), 100);
        assert_eq!(progress.page_bytes(64), 64);
        progress.sent(1, 4_096).unwrap();
        assert!(progress.is_paused() && progress.next_index == 1);
        progress.become_replicate();
        assert!(!progress.is_paused());
        // Sent ahead of its answers, a page is cut to the window's room less
        // the append's fixed bytes.
        assert_eq!(progress.page_bytes(4_096), 100);
        progress.sent(3, 10).unwrap();
        assert_eq!(progress.inflights.bytes(), fixed + 10);
        assert_eq!(
            progress.page_bytes(4_096),
            0,
            "no room beside another's fixed bytes"
        );
        // A page of one entry goes all the same while the window has room.
        progress.sent(5, 10).unwrap();
        assert!(progress.is_paused() && progress.next_index == 6);
        progress.matched = 5;
        progress.become_snapshot(9);
        assert!(progress.is_paused() && progress.sent(10, 1).is_err());
        assert!(!progress.is_snapshot_caught_up());
        progress.become_probe();
        assert_eq!((progress.next_index, progress.pending_snapshot), (10, 0));
        progress.matched = 12;
        progress.become_probe();
        assert_eq!(progress.next_index, 13);
    }
    /// The most members a configuration of these tests names.
    const MEMBERS: usize = 6;
    fn tracker(voters: &[u64], learners: &[u64]) -> Tracker {
        Tracker::new(
            Configuration::new(voters.to_vec(), learners.to_vec()).unwrap(),
            1,
            8,
            u64::MAX,
            MEMBERS,
        )
        .unwrap()
    }
    /// A configuration past the members the member was opened for stops it, opening as
    /// changing (`Limits::members`): its group's other members hold it, and going on without
    /// it would leave this one in another configuration.
    #[test]
    fn a_configuration_past_the_members_a_member_was_opened_for_stops_it() {
        let three = || Configuration::new(vec![1, 2, 3], vec![]).unwrap();
        assert!(matches!(
            Tracker::new(three(), 1, 8, u64::MAX, 2),
            Err(Error::Invariant(_))
        ));
        let mut members = Tracker::new(three(), 1, 8, u64::MAX, 3).unwrap();
        let four = Configuration::new(vec![1, 2, 3], vec![4]).unwrap();
        let refused = members.apply(four, &[], 1);
        assert!(matches!(refused, Err(Error::Invariant(_))));
        assert!(refused.unwrap_err().is_fatal());
        assert_eq!(members.len(), 3, "nothing changed");
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
