//! The message and log types, and the reading of them.
//!
//! hyper-raft's own types, written in its own format (`docs/raft.md` §3.1, [`crate::wire`]). The
//! kinds are typed: a value no kind names is refused when the bytes are read, so none is ever held.

/// An entry's kind.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub enum EntryType {
    /// An entry for the state machine.
    #[default]
    EntryNormal,
    /// A configuration change of one member, [`ConfChange`].
    EntryConfChange,
    /// A configuration change of any number, [`ConfChangeV2`].
    EntryConfChangeV2,
}

impl EntryType {
    /// The kind's byte in the format (`docs/raft.md` §3.1), which a log that keeps entries in its
    /// own frames may keep too.
    pub const fn byte(self) -> u8 {
        match self {
            EntryType::EntryNormal => 0,
            EntryType::EntryConfChange => 1,
            EntryType::EntryConfChangeV2 => 2,
        }
    }
    /// The kind a byte names; none for a byte no kind has.
    pub const fn from_byte(byte: u8) -> Option<Self> {
        match byte {
            0 => Some(EntryType::EntryNormal),
            1 => Some(EntryType::EntryConfChange),
            2 => Some(EntryType::EntryConfChangeV2),
            _ => None,
        }
    }
}

/// One entry of the log.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Entry {
    /// The kind.
    pub entry_type: EntryType,
    /// The term of the leader that proposed it.
    pub term: u64,
    /// Its place in the log.
    pub index: u64,
    /// The payload.
    pub data: Vec<u8>,
    /// What the proposer attached.
    pub context: Vec<u8>,
}

/// What a snapshot covers.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct SnapshotMetadata {
    /// The configuration as of the last entry covered.
    pub conf_state: Option<ConfState>,
    /// The index of the last entry covered.
    pub index: u64,
    /// Its term.
    pub term: u64,
}

/// A snapshot: the state machine's image and what it covers.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Snapshot {
    /// The image.
    pub data: Vec<u8>,
    /// What it covers.
    pub metadata: Option<SnapshotMetadata>,
}

/// A message's kind.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub enum MessageType {
    /// Campaign (local).
    #[default]
    MsgHup,
    /// Send heartbeats (local).
    MsgBeat,
    /// Propose entries.
    MsgPropose,
    /// Append entries.
    MsgAppend,
    /// The answer to an append.
    MsgAppendResponse,
    /// Ask for a vote.
    MsgRequestVote,
    /// The answer to a vote request.
    MsgRequestVoteResponse,
    /// Send a snapshot.
    MsgSnapshot,
    /// A leader's heartbeat.
    MsgHeartbeat,
    /// The answer to a heartbeat.
    MsgHeartbeatResponse,
    /// A member could not be reached (local).
    MsgUnreachable,
    /// How a snapshot's delivery went (local).
    MsgSnapStatus,
    /// Check the leader's quorum (local).
    MsgCheckQuorum,
    /// Hand leadership to a member.
    MsgTransferLeader,
    /// Campaign now: the leader hands over.
    MsgTimeoutNow,
    /// Ask for a read index.
    MsgReadIndex,
    /// The answer to a read-index request.
    MsgReadIndexResp,
    /// Ask for a pre-vote.
    MsgRequestPreVote,
    /// The answer to a pre-vote request.
    MsgRequestPreVoteResponse,
    /// The fast track's proposal, from a proposer to every voter (`crate::fast`).
    MsgFastPropose,
    /// The fast track's vote: what a voter holds at an index, to the leader.
    MsgFastVote,
}

/// A message between members, or from the owner to its member.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Message {
    /// The kind.
    pub msg_type: MessageType,
    /// The member it is for.
    pub to: u64,
    /// The member it is from.
    pub from: u64,
    /// The sender's term.
    pub term: u64,
    /// The term of the entry at `index`.
    pub log_term: u64,
    /// An index of the log, by kind.
    pub index: u64,
    /// The entries carried.
    pub entries: Vec<Entry>,
    /// The sender's commit index.
    pub commit: u64,
    /// The term of the entry at `commit`.
    pub commit_term: u64,
    /// The snapshot carried, boxed: only a snapshot message holds one, and inline its 144 bytes
    /// would ride in every message (`docs/benchmarks.md`, "The message's layout").
    pub snapshot: Option<Box<Snapshot>>,
    /// The index a follower asks a snapshot from.
    pub request_snapshot: u64,
    /// Whether the request is refused.
    pub reject: bool,
    /// On a refused append's answer: the member's log lacks entries it acknowledged, lost at
    /// rest (core step R-5, `docs/durable.md` §5). `reject_hint` and `log_term` name the last
    /// entry it holds; its leader takes the member's progress back to it and resends from there.
    pub lost: bool,
    /// Where a refused append may resume.
    pub reject_hint: u64,
    /// What the sender attached.
    pub context: Vec<u8>,
    /// The sender's election priority.
    pub priority: i64,
}

/// What a member must keep across a restart.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct HardState {
    /// The current term.
    pub term: u64,
    /// The member voted for in it; zero for none.
    pub vote: u64,
    /// The commit index.
    pub commit: u64,
}

/// How a joint change leaves the joint configuration.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub enum ConfChangeTransition {
    /// Simple where it can be, joint and left by the leader where not.
    #[default]
    Auto,
    /// Joint, left by the leader.
    Implicit,
    /// Joint, left by a later change.
    Explicit,
}

/// A configuration.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ConfState {
    /// The voters.
    pub voters: Vec<u64>,
    /// The learners.
    pub learners: Vec<u64>,
    /// The voters of the configuration being left.
    pub voters_outgoing: Vec<u64>,
    /// The voters that become learners once the joint configuration is left.
    pub learners_next: Vec<u64>,
    /// Whether the leader leaves the joint configuration by itself.
    pub auto_leave: bool,
}

/// What one change does to a member.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub enum ConfChangeType {
    /// Add a voter, or make a learner one.
    #[default]
    AddNode,
    /// Remove a member.
    RemoveNode,
    /// Add a learner, or make a voter one.
    AddLearnerNode,
}

/// A change of one member.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ConfChange {
    /// The kind.
    pub change_type: ConfChangeType,
    /// The member.
    pub node_id: u64,
    /// What the proposer attached.
    pub context: Vec<u8>,
}

/// One change of a joint change.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ConfChangeSingle {
    /// The kind.
    pub change_type: ConfChangeType,
    /// The member.
    pub node_id: u64,
}

/// A change of any number of members.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ConfChangeV2 {
    /// How it leaves the joint configuration.
    pub transition: ConfChangeTransition,
    /// The changes.
    pub changes: Vec<ConfChangeSingle>,
    /// What the proposer attached.
    pub context: Vec<u8>,
}

use crate::wire::{DecodeError, Record};

use crate::{
    Change, Configuration, NodeId,
    configuration::Changed,
    error::{Error, Result},
};

/// What a vote request of a transferred campaign carries: the bytes
/// `raft-rs` sends (its `CAMPAIGN_TRANSFER`), so either core reads the
/// other's.
pub const CAMPAIGN_TRANSFER: &[u8] = b"CampaignTransfer";

/// Whether the entry changes the configuration, by either encoding.
pub fn changes_configuration(entry: &Entry) -> bool {
    entry.entry_type != EntryType::EntryNormal
}
/// The bytes the entry takes in a message (`docs/raft.md` §3.1): what the core's byte bounds
/// count, so a message's bytes are the sum of its entries' and its fixed part.
pub fn encoded_bytes(entry: &Entry) -> u64 {
    u64::try_from(entry.body_len()).unwrap_or(u64::MAX)
}
/// The bytes the entry takes in a message, as a `usize`: what the core counts an unpersisted
/// entry at.
pub fn approximate_bytes(entry: &Entry) -> usize {
    entry.body_len()
}
/// What an allocator keeps for one buffer beyond its bytes — a header of a
/// few words and a rounding to its size class — taken as four words: the
/// figure `focal_memory::ALLOCATOR_OVERHEAD` keeps for every allocation
/// the engine makes, stated here because the core depends on nothing.
pub const BUFFER_OVERHEAD: usize = 4 * std::mem::size_of::<usize>();
/// What a message is allowed beyond its buffers' capacities: the message
/// itself, and the bookkeeping of the at most three buffers it owns (its
/// context, its entries, a snapshot's data).
pub const MESSAGE_ALLOWANCE: usize = std::mem::size_of::<Message>() + 3 * BUFFER_OVERHEAD;
/// The bytes a message holds, by capacity: its context, its entries' slots
/// and buffers, its snapshot, and [`MESSAGE_ALLOWANCE`]. The core counts a
/// queued message at this, and its owner charges one for the same, so that
/// a message moved from the one to the other costs the same at both.
pub fn message_bytes(message: &Message) -> usize {
    let payload = message.entries.iter().fold(0usize, |bytes, entry| {
        bytes
            .saturating_add(entry.data.capacity())
            .saturating_add(entry.context.capacity())
    });
    message_bytes_with(message, payload)
}
/// As [`message_bytes`], given what the entries' buffers hold by capacity,
/// counted where the entries were chosen, so that they are not walked
/// again.
pub(crate) fn message_bytes_with(message: &Message, payload: usize) -> usize {
    let slots = message
        .entries
        .capacity()
        .saturating_mul(std::mem::size_of::<Entry>());
    let snapshot = message.snapshot.as_ref().map_or(0, |snapshot| {
        snapshot.data.capacity().saturating_add(MESSAGE_ALLOWANCE)
    });
    MESSAGE_ALLOWANCE
        .saturating_add(message.context.capacity())
        .saturating_add(slots)
        .saturating_add(payload)
        .saturating_add(snapshot)
}
/// The index of the last entry the snapshot covers; zero when it states
/// none.
pub fn snapshot_index(snapshot: &Snapshot) -> u64 {
    snapshot
        .metadata
        .as_ref()
        .map_or(0, |metadata| metadata.index)
}
/// The term of the last entry the snapshot covers; zero when it states
/// none.
pub fn snapshot_term(snapshot: &Snapshot) -> u64 {
    snapshot
        .metadata
        .as_ref()
        .map_or(0, |metadata| metadata.term)
}
/// A snapshot that states no index states nothing.
pub fn snapshot_is_empty(snapshot: &Snapshot) -> bool {
    snapshot_index(snapshot) == 0
}
/// An empty message of `kind` for `to`.
pub fn message(to: NodeId, kind: MessageType) -> Message {
    Message {
        to,
        msg_type: kind,
        ..Message::default()
    }
}

/// The change a committed entry states, in the joint encoding; none for an entry that states
/// none. A change of one member is its joint form ([`joint`]); a change entry with no data
/// states the empty change, as a leader writes its own leave; data that is no change of its
/// kind is the decoder's error.
pub fn change_of(entry: &Entry) -> core::result::Result<Option<ConfChangeV2>, DecodeError> {
    match entry.entry_type {
        EntryType::EntryNormal => Ok(None),
        EntryType::EntryConfChange if entry.data.is_empty() => {
            Ok(Some(joint(&ConfChange::default())))
        }
        EntryType::EntryConfChange => {
            ConfChange::decode(&entry.data).map(|single| Some(joint(&single)))
        }
        EntryType::EntryConfChangeV2 if entry.data.is_empty() => Ok(Some(ConfChangeV2::default())),
        EntryType::EntryConfChangeV2 => ConfChangeV2::decode(&entry.data).map(Some),
    }
}

/// A change of one member as the joint encoding states it: the same change, by `Auto`.
pub fn joint(single: &ConfChange) -> ConfChangeV2 {
    ConfChangeV2 {
        transition: ConfChangeTransition::Auto,
        changes: vec![ConfChangeSingle {
            change_type: single.change_type,
            node_id: single.node_id,
        }],
        context: single.context.clone(),
    }
}

/// How a change moves the configuration.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Transition {
    /// At most one voter moves.
    Simple,
    /// Into the joint configuration.
    Enter {
        /// Whether the leader leaves the joint configuration by itself.
        auto_leave: bool,
    },
    /// Out of the joint configuration.
    Leave,
}
/// A change as the log states it, read once.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Plan {
    /// How the change moves the configuration.
    pub transition: Transition,
    /// The changes that are not withdrawn, in the order stated.
    pub changes: Vec<Change>,
    /// How many changes the entry states, withdrawn ones among them.
    pub stated: usize,
    /// What the proposer attached to the change.
    pub context: Vec<u8>,
}
impl Plan {
    /// The most changes one entry states.
    pub const MAX_CHANGES: usize = crate::MAX_MEMBERS;

    /// The change `change` states.
    pub fn of(change: &ConfChangeV2) -> Result<Self> {
        let transition = change.transition;
        if change.changes.len() > Self::MAX_CHANGES {
            return Err(Error::Capacity("changes in one entry"));
        }
        let mut changes = Vec::new();
        changes
            .try_reserve_exact(change.changes.len())
            .map_err(|_| Error::Capacity("changes in one entry"))?;
        for single in &change.changes {
            let kind = single.change_type;
            // A change whose member is zero was withdrawn after it was
            // proposed; it changes nothing.
            if single.node_id == 0 {
                continue;
            }
            changes.push(match kind {
                ConfChangeType::AddNode => Change::AddVoter(single.node_id),
                ConfChangeType::AddLearnerNode => Change::AddLearner(single.node_id),
                ConfChangeType::RemoveNode => Change::Remove(single.node_id),
            });
        }
        // Decided by what the entry states, before withdrawn changes are
        // set aside, as every member must decide it alike.
        let transition = if transition == ConfChangeTransition::Auto && change.changes.is_empty() {
            Transition::Leave
        } else if transition != ConfChangeTransition::Auto || change.changes.len() > 1 {
            Transition::Enter {
                auto_leave: transition != ConfChangeTransition::Explicit,
            }
        } else {
            Transition::Simple
        };
        let mut context = Vec::new();
        context
            .try_reserve_exact(change.context.len())
            .map_err(|_| Error::Capacity("context of a change"))?;
        context.extend_from_slice(&change.context);
        Ok(Self {
            transition,
            changes,
            stated: change.changes.len(),
            context,
        })
    }
    /// The change an entry states; none for an entry that states none.
    pub fn of_entry(entry: &Entry) -> Result<Option<Self>> {
        match change_of(entry) {
            Ok(None) => Ok(None),
            Ok(Some(change)) => Self::of(&change).map(Some),
            Err(_) => Err(Error::Violation("a change that does not decode")),
        }
    }
    /// The configuration after this change.
    pub fn apply(&self, configuration: &Configuration) -> Result<Changed> {
        Ok(match self.transition {
            Transition::Leave => Changed {
                configuration: configuration.leave_joint()?,
                renewed: Vec::new(),
            },
            Transition::Enter { auto_leave } => {
                configuration.enter_joint(auto_leave, &self.changes)?
            }
            Transition::Simple => configuration.simple(&self.changes)?,
        })
    }
}

impl Configuration {
    /// The configuration the log or a snapshot states.
    pub fn from_conf_state(state: &ConfState) -> Result<Self> {
        let copy = |members: &[NodeId]| -> Result<Vec<NodeId>> {
            if members.len() > crate::MAX_MEMBERS {
                return Err(Error::Configuration(
                    crate::ConfigurationError::TooManyMembers,
                ));
            }
            let mut copied = Vec::new();
            copied
                .try_reserve_exact(members.len())
                .map_err(|_| Error::Capacity("members of a configuration"))?;
            copied.extend_from_slice(members);
            Ok(copied)
        };
        Ok(Self::from_parts(
            copy(&state.voters)?,
            copy(&state.voters_outgoing)?,
            copy(&state.learners)?,
            copy(&state.learners_next)?,
            state.auto_leave,
        )?)
    }
    /// The members in order, which is how every member states them.
    pub fn to_conf_state(&self) -> Result<ConfState> {
        let copy = |members: &[NodeId]| -> Result<Vec<NodeId>> {
            let mut copied = Vec::new();
            copied
                .try_reserve_exact(members.len())
                .map_err(|_| Error::Capacity("members of a configuration"))?;
            copied.extend_from_slice(members);
            Ok(copied)
        };
        Ok(ConfState {
            voters: copy(self.voters())?,
            learners: copy(self.learners())?,
            voters_outgoing: copy(self.outgoing())?,
            learners_next: copy(self.learners_next())?,
            auto_leave: self.auto_leave(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn single(kind: ConfChangeType, node: u64) -> ConfChangeSingle {
        ConfChangeSingle {
            change_type: kind,
            node_id: node,
        }
    }
    #[test]
    fn a_change_is_simple_joint_or_a_leaving() {
        let one = ConfChangeV2 {
            changes: vec![single(ConfChangeType::AddNode, 4)],
            ..Default::default()
        };
        assert_eq!(Plan::of(&one).unwrap().transition, Transition::Simple);
        let two = ConfChangeV2 {
            changes: vec![
                single(ConfChangeType::AddNode, 4),
                single(ConfChangeType::RemoveNode, 1),
            ],
            ..Default::default()
        };
        assert_eq!(
            Plan::of(&two).unwrap().transition,
            Transition::Enter { auto_leave: true }
        );
        let explicit = ConfChangeV2 {
            transition: ConfChangeTransition::Explicit,
            changes: vec![single(ConfChangeType::AddNode, 4)],
            ..Default::default()
        };
        assert_eq!(
            Plan::of(&explicit).unwrap().transition,
            Transition::Enter { auto_leave: false }
        );
        let implicit = ConfChangeV2 {
            transition: ConfChangeTransition::Implicit,
            ..Default::default()
        };
        assert_eq!(
            Plan::of(&implicit).unwrap().transition,
            Transition::Enter { auto_leave: true }
        );
        assert_eq!(
            Plan::of(&ConfChangeV2::default()).unwrap().transition,
            Transition::Leave
        );
        // A withdrawn change states nothing and still decides the kind.
        let withdrawn = ConfChangeV2 {
            changes: vec![single(ConfChangeType::AddNode, 0)],
            ..Default::default()
        };
        let plan = Plan::of(&withdrawn).unwrap();
        assert_eq!(plan.transition, Transition::Simple);
        assert!(plan.changes.is_empty());
    }
    #[test]
    fn a_change_that_does_not_decode_is_a_violation() {
        let entry = Entry {
            entry_type: EntryType::EntryConfChangeV2,
            data: vec![0xff, 0xff, 0xff],
            ..Default::default()
        };
        assert!(matches!(Plan::of_entry(&entry), Err(Error::Violation(_))));
        assert!(changes_configuration(&entry));
    }
    #[test]
    fn both_encodings_of_a_change_read_alike() {
        let old = ConfChange {
            change_type: ConfChangeType::AddLearnerNode,
            node_id: 7,
            context: b"why".to_vec(),
        };
        let entry = Entry {
            entry_type: EntryType::EntryConfChange,
            data: old.encode_to_vec(),
            ..Default::default()
        };
        let plan = Plan::of_entry(&entry).unwrap().unwrap();
        assert_eq!(plan.transition, Transition::Simple);
        assert_eq!(plan.changes, vec![Change::AddLearner(7)]);
        assert_eq!(plan.context, b"why");
        let new = ConfChangeV2 {
            changes: vec![single(ConfChangeType::AddLearnerNode, 7)],
            context: b"why".to_vec(),
            ..Default::default()
        };
        let entry = Entry {
            entry_type: EntryType::EntryConfChangeV2,
            data: new.encode_to_vec(),
            ..Default::default()
        };
        assert_eq!(Plan::of_entry(&entry).unwrap().unwrap(), plan);
        assert_eq!(Plan::of_entry(&Entry::default()).unwrap(), None);
    }
    #[test]
    fn a_configuration_survives_its_statement() {
        let state = ConfState {
            voters: vec![3, 1, 2],
            learners: vec![9],
            voters_outgoing: vec![1, 4],
            learners_next: vec![4],
            auto_leave: true,
        };
        let configuration = Configuration::from_conf_state(&state).unwrap();
        let stated = configuration.to_conf_state().unwrap();
        assert_eq!(stated.voters, vec![1, 2, 3]);
        assert_eq!(stated.voters_outgoing, vec![1, 4]);
        assert_eq!(
            Configuration::from_conf_state(&stated).unwrap(),
            configuration
        );
        let plan = Plan {
            transition: Transition::Leave,
            changes: vec![],
            stated: 0,
            context: vec![],
        };
        let left = plan.apply(&configuration).unwrap().configuration;
        assert_eq!(left.voters(), [1, 2, 3]);
        assert_eq!(left.learners(), [4, 9]);
    }
}
