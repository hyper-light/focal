//! The wire and log types, and the reading of them that cannot unwind.
//!
//! The types are the ones `raft-rs` defines, so a member on this core and a
//! member on that one exchange the same bytes and replay the same log. Their
//! generated accessors for enumerations unwind on a value they do not know;
//! nothing here calls them.
pub use raft_proto::eraftpb::{
    ConfChange, ConfChangeSingle, ConfChangeTransition, ConfChangeType, ConfChangeV2, ConfState,
    Entry, EntryType, HardState, Message, MessageType, Snapshot, SnapshotMetadata,
};
/// The codec of the wire and log types.
pub use raft_proto::protocompat;
use raft_proto::protocompat::{PbMessage, PbMessageExt};

use crate::{
    Change, Configuration, NodeId,
    configuration::Changed,
    error::{Error, Result},
};

/// What a vote request of a transferred campaign carries.
pub const CAMPAIGN_TRANSFER: &[u8] = b"CampaignTransfer";

pub fn message_type(message: &Message) -> Option<MessageType> {
    MessageType::from_i32(message.msg_type)
}
pub fn entry_type(entry: &Entry) -> Option<EntryType> {
    EntryType::from_i32(entry.entry_type)
}
/// Whether the entry changes the configuration, by either encoding.
pub fn changes_configuration(entry: &Entry) -> bool {
    !matches!(entry_type(entry), Some(EntryType::EntryNormal))
}
/// The bytes the entry takes encoded.
pub fn encoded_bytes(entry: &Entry) -> u64 {
    u64::try_from(entry.encoded_len()).unwrap_or(u64::MAX)
}
/// About the bytes an entry holds in memory.
pub fn approximate_bytes(entry: &Entry) -> usize {
    entry
        .data
        .len()
        .saturating_add(entry.context.len())
        .saturating_add(12)
}
pub fn snapshot_index(snapshot: &Snapshot) -> u64 {
    snapshot
        .metadata
        .as_ref()
        .map_or(0, |metadata| metadata.index)
}
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
pub fn message(to: NodeId, kind: MessageType) -> Message {
    Message {
        to,
        msg_type: kind as i32,
        ..Message::default()
    }
}

/// How a change moves the configuration.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Transition {
    /// At most one voter moves.
    Simple,
    /// Into the joint configuration; `auto_leave` says whether the leader
    /// leaves it by itself.
    Enter { auto_leave: bool },
    /// Out of the joint configuration.
    Leave,
}
/// A change as the log states it, read once.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Plan {
    pub transition: Transition,
    pub changes: Vec<Change>,
    /// How many changes the entry states, withdrawn ones among them.
    pub stated: usize,
    pub context: Vec<u8>,
}
impl Plan {
    /// The most changes one entry states.
    pub const MAX_CHANGES: usize = crate::MAX_MEMBERS;

    pub fn of(change: &ConfChangeV2) -> Result<Self> {
        let transition = ConfChangeTransition::from_i32(change.transition)
            .ok_or(Error::Violation("unknown transition of a change"))?;
        if change.changes.len() > Self::MAX_CHANGES {
            return Err(Error::Capacity("changes in one entry"));
        }
        let mut changes = Vec::new();
        changes
            .try_reserve_exact(change.changes.len())
            .map_err(|_| Error::Capacity("changes in one entry"))?;
        for single in &change.changes {
            let kind = ConfChangeType::from_i32(single.change_type)
                .ok_or(Error::Violation("unknown kind of a change"))?;
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
        match entry_type(entry) {
            None => Err(Error::Violation("unknown kind of entry")),
            Some(EntryType::EntryNormal) => Ok(None),
            Some(EntryType::EntryConfChange) => {
                let mut single = ConfChange::default();
                single
                    .merge_from_bytes(&entry.data)
                    .map_err(|_| Error::Violation("a change that does not decode"))?;
                let change = ConfChangeV2 {
                    transition: ConfChangeTransition::Auto as i32,
                    changes: vec![ConfChangeSingle {
                        change_type: single.change_type,
                        node_id: single.node_id,
                    }],
                    context: single.context,
                };
                Self::of(&change).map(Some)
            }
            Some(EntryType::EntryConfChangeV2) => {
                let mut change = ConfChangeV2::default();
                change
                    .merge_from_bytes(&entry.data)
                    .map_err(|_| Error::Violation("a change that does not decode"))?;
                Self::of(&change).map(Some)
            }
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
            change_type: kind as i32,
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
            transition: ConfChangeTransition::Explicit as i32,
            changes: vec![single(ConfChangeType::AddNode, 4)],
            ..Default::default()
        };
        assert_eq!(
            Plan::of(&explicit).unwrap().transition,
            Transition::Enter { auto_leave: false }
        );
        let implicit = ConfChangeV2 {
            transition: ConfChangeTransition::Implicit as i32,
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
    fn what_the_generated_accessors_unwind_on_is_an_error_here() {
        let unknown = ConfChangeV2 {
            changes: vec![ConfChangeSingle {
                change_type: 9,
                node_id: 1,
            }],
            ..Default::default()
        };
        assert!(matches!(Plan::of(&unknown), Err(Error::Violation(_))));
        let unknown = ConfChangeV2 {
            transition: 9,
            ..Default::default()
        };
        assert!(matches!(Plan::of(&unknown), Err(Error::Violation(_))));
        let entry = Entry {
            entry_type: 9,
            ..Default::default()
        };
        assert!(matches!(Plan::of_entry(&entry), Err(Error::Violation(_))));
        assert!(changes_configuration(&entry));
        let entry = Entry {
            entry_type: EntryType::EntryConfChangeV2 as i32,
            data: vec![0xff, 0xff, 0xff],
            ..Default::default()
        };
        assert!(matches!(Plan::of_entry(&entry), Err(Error::Violation(_))));
        assert_eq!(
            message_type(&Message {
                msg_type: 99,
                ..Default::default()
            }),
            None
        );
    }
    #[test]
    fn both_encodings_of_a_change_read_alike() {
        let old = ConfChange {
            change_type: ConfChangeType::AddLearnerNode as i32,
            node_id: 7,
            context: b"why".to_vec(),
            id: 0,
        };
        let entry = Entry {
            entry_type: EntryType::EntryConfChange as i32,
            data: old.write_to_bytes().unwrap(),
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
            entry_type: EntryType::EntryConfChangeV2 as i32,
            data: new.write_to_bytes().unwrap(),
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
