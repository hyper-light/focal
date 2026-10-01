//! The member as its owner drives it: one [`Ready`] at a time says what to
//! persist, send and apply; [`RawNode::advance_append`] says it is
//! persisted, and [`RawNode::advance_apply_to`] how far it is applied.
//!
//! A follower's messages answer for what it holds, so they are sent only
//! after what the same `Ready` persists is durable
//! ([`Ready::persisted_messages`]). A leader's may go at once: what it
//! sends its members persist for themselves (Ongaro's thesis §10.2.1).
use raft_proto::protocompat::PbMessageExt;

use crate::{
    NodeId,
    error::{Error, Result},
    proto::{
        self, ConfChange, ConfChangeV2, ConfState, Entry, EntryType, HardState, Message,
        MessageType, Plan, Snapshot,
    },
    raft::{Config, Raft, SoftState, StateRole},
    read::ReadState,
    storage::Storage,
};

/// What became of a snapshot that was sent.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SnapshotStatus {
    Finish,
    Failure,
}

/// What a member sends itself and the network never carries.
pub fn is_local(kind: MessageType) -> bool {
    matches!(
        kind,
        MessageType::MsgHup
            | MessageType::MsgBeat
            | MessageType::MsgUnreachable
            | MessageType::MsgSnapStatus
            | MessageType::MsgCheckQuorum
    )
}
fn is_answer(kind: MessageType) -> bool {
    matches!(
        kind,
        MessageType::MsgAppendResponse
            | MessageType::MsgRequestVoteResponse
            | MessageType::MsgHeartbeatResponse
            | MessageType::MsgUnreachable
            | MessageType::MsgRequestPreVoteResponse
    )
}

/// What follows once a [`Ready`] is persisted.
#[derive(Debug, Default, PartialEq)]
pub struct LightReady {
    commit_index: Option<u64>,
    committed_entries: Vec<Entry>,
    messages: Vec<Message>,
}
impl LightReady {
    /// The commit, when it moved. It need not be durable to be acted on.
    pub fn commit_index(&self) -> Option<u64> {
        self.commit_index
    }
    pub fn committed_entries(&self) -> &[Entry] {
        &self.committed_entries
    }
    pub fn take_committed_entries(&mut self) -> Vec<Entry> {
        std::mem::take(&mut self.committed_entries)
    }
    pub fn messages(&self) -> &[Message] {
        &self.messages
    }
    pub fn take_messages(&mut self) -> Vec<Message> {
        std::mem::take(&mut self.messages)
    }
}

#[derive(Debug, Default, PartialEq)]
pub struct Ready {
    number: u64,
    soft_state: Option<SoftState>,
    hard_state: Option<HardState>,
    read_states: Vec<ReadState>,
    entries: Vec<Entry>,
    proposals: Vec<Entry>,
    displaced: Vec<Entry>,
    snapshot: Option<Snapshot>,
    after_persisting: bool,
    light: LightReady,
    must_sync: bool,
}
impl Ready {
    pub fn number(&self) -> u64 {
        self.number
    }
    /// Who leads and what this member is, when either changed.
    pub fn soft_state(&self) -> Option<&SoftState> {
        self.soft_state.as_ref()
    }
    /// To persist, when it changed.
    pub fn hard_state(&self) -> Option<&HardState> {
        self.hard_state.as_ref()
    }
    pub fn read_states(&self) -> &[ReadState] {
        &self.read_states
    }
    pub fn take_read_states(&mut self) -> Vec<ReadState> {
        std::mem::take(&mut self.read_states)
    }
    /// To persist, replacing what storage holds from the first of them on.
    pub fn entries(&self) -> &[Entry] {
        &self.entries
    }
    pub fn take_entries(&mut self) -> Vec<Entry> {
        std::mem::take(&mut self.entries)
    }
    /// To persist beside the log: what this member approved by itself
    /// ([`crate::fast`]). What it holds it says only once this is durable,
    /// and storage gives it back when the member opens
    /// ([`crate::InitialState::proposals`]) until the log reaches its
    /// index.
    pub fn proposals(&self) -> &[Entry] {
        &self.proposals
    }
    /// What was proposed here by the fast track and another entry took the
    /// index of: its proposer proposes it again.
    pub fn displaced(&self) -> &[Entry] {
        &self.displaced
    }
    pub fn take_displaced(&mut self) -> Vec<Entry> {
        std::mem::take(&mut self.displaced)
    }
    /// To persist before the entries: the log begins again after it.
    pub fn snapshot(&self) -> Option<&Snapshot> {
        self.snapshot.as_ref()
    }
    /// Committed and durable here: to apply.
    pub fn committed_entries(&self) -> &[Entry] {
        self.light.committed_entries()
    }
    pub fn take_committed_entries(&mut self) -> Vec<Entry> {
        self.light.take_committed_entries()
    }
    /// To send at once.
    pub fn messages(&self) -> &[Message] {
        if self.after_persisting {
            &[]
        } else {
            self.light.messages()
        }
    }
    pub fn take_messages(&mut self) -> Vec<Message> {
        if self.after_persisting {
            Vec::new()
        } else {
            self.light.take_messages()
        }
    }
    /// To send once what this `Ready` persists is durable.
    pub fn persisted_messages(&self) -> &[Message] {
        if self.after_persisting {
            self.light.messages()
        } else {
            &[]
        }
    }
    pub fn take_persisted_messages(&mut self) -> Vec<Message> {
        if self.after_persisting {
            self.light.take_messages()
        } else {
            Vec::new()
        }
    }
    /// False when nothing but the commit is to persist: that write need
    /// not be waited for.
    pub fn must_sync(&self) -> bool {
        self.must_sync
    }
}

/// What a [`Ready`] gave to persist.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct Given {
    number: u64,
    last_entry: Option<(u64, u64)>,
    snapshot: Option<u64>,
}

pub struct RawNode<S> {
    pub raft: Raft<S>,
    previous_soft: SoftState,
    previous_hard: HardState,
    number: u64,
    /// The `Ready` that is out; one at a time.
    given: Option<Given>,
    /// What was given to apply reaches this index.
    commit_since: u64,
}

impl<S: Storage> RawNode<S> {
    pub fn new(config: &Config, store: S) -> Result<Self> {
        let raft = Raft::new(config, store)?;
        Ok(Self {
            previous_soft: raft.soft_state(),
            previous_hard: raft.hard_state(),
            raft,
            number: 0,
            given: None,
            commit_since: config.applied,
        })
    }
    pub fn store(&self) -> &S {
        self.raft.store()
    }
    pub fn store_mut(&mut self) -> &mut S {
        self.raft.store_mut()
    }
    /// Whether a `Ready` is out and not yet advanced.
    pub fn outstanding(&self) -> bool {
        self.given.is_some()
    }
    /// An operation on the member. The priority in force is settled before
    /// it and after, never within. While a `Ready` is out the member does
    /// not change.
    fn operate<T>(&mut self, operation: impl FnOnce(&mut Raft<S>) -> Result<T>) -> Result<T> {
        if self.given.is_some() {
            return Err(Error::Invariant("an operation while a ready is out"));
        }
        self.raft.settle_priority();
        let outcome = operation(&mut self.raft);
        self.raft.settle_priority();
        outcome
    }
    pub fn set_priority(&mut self, priority: i64) {
        self.raft.set_priority(priority);
    }
    pub fn tick(&mut self) -> Result<bool> {
        self.operate(Raft::tick)
    }
    pub fn campaign(&mut self) -> Result<()> {
        self.operate(|raft| raft.step(proto::message(0, MessageType::MsgHup)))
    }
    pub fn propose(&mut self, context: Vec<u8>, data: Vec<u8>) -> Result<()> {
        self.operate(|raft| {
            let mut message = proto::message(0, MessageType::MsgPropose);
            message.from = raft.id();
            message
                .entries
                .try_reserve_exact(1)
                .map_err(|_| Error::Capacity("a proposal"))?;
            message.entries.push(Entry {
                data,
                context,
                ..Entry::default()
            });
            raft.step(message)
        })
    }
    /// Proposes by the fast track ([`crate::fast`]); the index proposed
    /// for.
    pub fn propose_fast(&mut self, context: Vec<u8>, data: Vec<u8>) -> Result<u64> {
        self.operate(|raft| raft.propose_fast(context, data))
    }
    pub fn propose_conf_change(&mut self, context: Vec<u8>, change: &ConfChangeV2) -> Result<()> {
        // What could not be read when it is applied is not proposed.
        Plan::of(change)?;
        let data = change
            .write_to_bytes()
            .map_err(|_| Error::Capacity("a change's encoding"))?;
        self.operate(|raft| {
            let mut message = proto::message(0, MessageType::MsgPropose);
            message
                .entries
                .try_reserve_exact(1)
                .map_err(|_| Error::Capacity("a proposal"))?;
            message.entries.push(Entry {
                entry_type: EntryType::EntryConfChangeV2 as i32,
                data,
                context,
                ..Entry::default()
            });
            raft.step(message)
        })
    }
    /// A committed change is applied. One the application decides not to
    /// apply is not given here.
    pub fn apply_conf_change(&mut self, change: &ConfChangeV2) -> Result<ConfState> {
        let plan = Plan::of(change)?;
        self.raft.settle_priority();
        let outcome = self.raft.apply_conf_change(&plan);
        self.raft.settle_priority();
        outcome
    }
    /// A committed change in the older encoding is applied.
    pub fn apply_conf_change_v1(&mut self, change: &ConfChange) -> Result<ConfState> {
        let entry = Entry {
            entry_type: EntryType::EntryConfChange as i32,
            data: change
                .write_to_bytes()
                .map_err(|_| Error::Capacity("a change's encoding"))?,
            ..Entry::default()
        };
        let plan = Plan::of_entry(&entry)?.ok_or(Error::Invariant("a change that states none"))?;
        self.raft.settle_priority();
        let outcome = self.raft.apply_conf_change(&plan);
        self.raft.settle_priority();
        outcome
    }
    /// A message from the network.
    pub fn step(&mut self, message: Message) -> Result<()> {
        if message.msg_type == crate::fast::FAST_PROPOSE
            || message.msg_type == crate::fast::FAST_VOTE
        {
            return self.operate(|raft| raft.step(message));
        }
        let kind = proto::message_type(&message).ok_or(Error::Violation("a message of no kind"))?;
        if is_local(kind) {
            return Err(Error::StepLocalMessage);
        }
        if is_answer(kind) && self.raft.tracker().get(message.from).is_none() {
            return Err(Error::StepPeerNotFound);
        }
        self.operate(|raft| raft.step(message))
    }
    /// A leader sends its heartbeats now.
    pub fn ping(&mut self) -> Result<()> {
        self.operate(Raft::ping)
    }
    /// What a member asks of itself on its owner's word. A refusal is no
    /// one's to hear.
    fn tell(&mut self, message: Message) -> Result<()> {
        match self.operate(|raft| raft.step(message)) {
            Err(error) if error.is_fatal() => Err(error),
            _ => Ok(()),
        }
    }
    /// The last message to `member` did not arrive.
    pub fn report_unreachable(&mut self, member: NodeId) -> Result<()> {
        let mut message = proto::message(0, MessageType::MsgUnreachable);
        message.from = member;
        self.tell(message)
    }
    pub fn report_snapshot(&mut self, member: NodeId, status: SnapshotStatus) -> Result<()> {
        let mut message = proto::message(0, MessageType::MsgSnapStatus);
        message.from = member;
        message.reject = status == SnapshotStatus::Failure;
        self.tell(message)
    }
    pub fn request_snapshot(&mut self) -> Result<()> {
        self.operate(Raft::request_snapshot)
    }
    /// `transferee` shall lead.
    pub fn transfer_leader(&mut self, transferee: NodeId) -> Result<()> {
        let mut message = proto::message(0, MessageType::MsgTransferLeader);
        message.from = transferee;
        self.tell(message)
    }
    /// Asks at which index a read may be served; the answer comes in a
    /// `Ready` with the same `context`.
    pub fn read_index(&mut self, context: Vec<u8>) -> Result<()> {
        let mut message = proto::message(0, MessageType::MsgReadIndex);
        message
            .entries
            .try_reserve_exact(1)
            .map_err(|_| Error::Capacity("a read"))?;
        message.entries.push(Entry {
            data: context,
            ..Entry::default()
        });
        match self.operate(|raft| raft.step(message)) {
            Err(error) if error.is_fatal() || matches!(error, Error::Capacity(_)) => Err(error),
            _ => Ok(()),
        }
    }

    fn light(&mut self) -> Result<LightReady> {
        let committed_entries = self
            .raft
            .log()
            .next_entries_since(self.commit_since, self.raft.committed_bytes_per_ready())?;
        self.raft.reduce_uncommitted(&committed_entries);
        if let Some(last) = committed_entries.last() {
            if self.commit_since >= last.index {
                return Err(Error::Invariant("entries given to apply twice"));
            }
            self.commit_since = last.index;
        }
        Ok(LightReady {
            commit_index: None,
            committed_entries,
            messages: self.raft.msgs.take(),
        })
    }
    /// Whether every counter the member trusts for what it holds says what
    /// a walk says ([`Raft::check_accounting`]).
    pub fn check_accounting(&self) -> Result<()> {
        self.raft.check_accounting()
    }
    pub fn has_ready(&self) -> bool {
        let raft = &self.raft;
        !raft.msgs.is_empty()
            || raft.reads_unasked()
            || raft.soft_state() != self.previous_soft
            || raft.hard_state() != self.previous_hard
            || !raft.read_states.is_empty()
            || !raft.displaced.is_empty()
            || raft.held.has_unstable()
            || !raft.log().unstable().entries().is_empty()
            || raft
                .snapshot()
                .is_some_and(|snapshot| !proto::snapshot_is_empty(snapshot))
            || raft
                .log()
                .has_next_entries_since(self.commit_since)
                .unwrap_or(false)
    }
    /// What there is to do. It is done, and advanced, before the member
    /// is given anything else.
    pub fn ready(&mut self) -> Result<Ready> {
        if self.given.is_some() {
            return Err(Error::Invariant("a ready asked for while one is out"));
        }
        let number = self
            .number
            .checked_add(1)
            .ok_or(Error::Capacity("readies"))?;
        // One round for the reads asked since the last (`Raft::ask_reads`): it
        // leaves with this `Ready`. A round refused is asked for again by
        // the next `Ready`, and by the heartbeat the leader's clock sends.
        self.raft.ask_reads()?;
        let mut ready = Ready {
            number,
            ..Ready::default()
        };
        let mut given = Given {
            number,
            ..Given::default()
        };
        // Everything that can refuse does before the member is changed.
        if let Some(snapshot) = self.raft.snapshot() {
            let index = proto::snapshot_index(snapshot);
            if self.commit_since > index {
                return Err(Error::Invariant(
                    "a snapshot behind what was given to apply",
                ));
            }
            // Entries after a snapshot are not durable before it is, so
            // none is given to apply with it.
            if self.raft.log().has_next_entries_since(index)? {
                return Err(Error::Invariant(
                    "a snapshot with entries to apply after it",
                ));
            }
            ready.snapshot = Some(crate::log::copy_snapshot(snapshot)?);
            given.snapshot = Some(index);
            ready.must_sync = true;
        }
        crate::log::copy_entries(self.raft.log().unstable().entries(), &mut ready.entries)?;
        self.raft.unstable_proposals(&mut ready.proposals)?;

        if let Some(index) = given.snapshot {
            self.commit_since = index;
        }
        ready.light = self.light()?;
        self.number = number;
        let soft = self.raft.soft_state();
        if soft != self.previous_soft {
            ready.soft_state = Some(soft);
        }
        let hard = self.raft.hard_state();
        if hard != self.previous_hard {
            if hard.vote != self.previous_hard.vote || hard.term != self.previous_hard.term {
                ready.must_sync = true;
            }
            ready.hard_state = Some(hard);
        }
        // Taken, and not emptied: what the member holds when it rests is
        // what it held before.
        ready.read_states = std::mem::take(&mut self.raft.read_states);
        ready.displaced = self.raft.take_displaced();
        if !ready.proposals.is_empty() {
            ready.must_sync = true;
        }
        if let Some(last) = ready.entries.last() {
            ready.must_sync = true;
            given.last_entry = Some((last.index, last.term));
        }
        ready.after_persisting = self.raft.state() != StateRole::Leader;
        self.given = Some(given);
        Ok(ready)
    }
    /// What `ready` gave to persist is durable, and storage answers for it.
    pub fn advance_append(&mut self, ready: Ready) -> Result<LightReady> {
        let given = self
            .given
            .filter(|given| given.number == ready.number)
            .ok_or(Error::Invariant("a ready advanced that is not the one out"))?;
        if let Some(soft) = ready.soft_state {
            self.previous_soft = soft;
        }
        if let Some(hard) = ready.hard_state {
            self.previous_hard = hard;
        }
        if let Some(index) = given.snapshot {
            self.raft.log.stable_snapshot(index)?;
        }
        if let Some((index, term)) = given.last_entry {
            self.raft.log.stable_entries(index, term)?;
        }
        self.given = None;
        self.raft.settle_priority();
        if let Some(index) = given.snapshot {
            self.raft.on_persist_snapshot(index)?;
        }
        if let Some((index, term)) = given.last_entry {
            self.raft.on_persist_entries(index, term)?;
        }
        self.raft.on_persist_proposals(&ready.proposals)?;
        self.raft.settle_priority();
        // What is to send now is sent after what `ready` persisted, whoever
        // sends it: a leader that a change it applied made a follower has
        // its last messages here.
        let mut light = self.light()?;
        let hard = self.raft.hard_state();
        if hard.commit > self.previous_hard.commit {
            light.commit_index = Some(hard.commit);
            self.previous_hard.commit = hard.commit;
        }
        if hard != self.previous_hard {
            return Err(Error::Invariant(
                "a term or a vote moved while a ready was out",
            ));
        }
        Ok(light)
    }
    /// The application applied through `applied`.
    pub fn advance_apply_to(&mut self, applied: u64) -> Result<()> {
        self.raft.settle_priority();
        let outcome = self.raft.commit_apply(applied);
        self.raft.settle_priority();
        outcome
    }
    /// Persisted and applied, for an owner that does both at once.
    pub fn advance(&mut self, ready: Ready) -> Result<LightReady> {
        let applied = self.commit_since;
        let light = self.advance_append(ready)?;
        self.advance_apply_to(applied)?;
        Ok(light)
    }
    /// Through which index entries were given to apply.
    pub fn given_to_apply(&self) -> u64 {
        self.commit_since
    }
}
