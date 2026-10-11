//! What focal tells hyper-transport of its messages (its `Classes`): the kinds that cross the wire,
//! the class each sender's role gives each kind, and each class's frame bound. A message's class is
//! decided by its kind and by the role its sender authenticated as, before a byte of its body is
//! read and never by anything the sender chooses (the audit's §13.3): an operation's kind is its
//! registered tag ([`crate::Operation::registered_tag`]), and its class the tag's
//! ([`TrafficClass::of_tag`]) for a node, the tag's but never the control class for a participant.

use hyper_transport::Classes;

use crate::TrafficClass;

/// A kind of message focal's endpoints exchange.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Kind {
    /// The greeting a dialer opens every connection with: the profiles it speaks and its bounds,
    /// answered by the profile the two share ([`crate::Hello`], [`crate::HelloReply`]).
    Hello,
    /// An operation, by its registered tag.
    Operation(u16),
    /// A message of the enrollment protocol (focal-enrollment's), by its own tag.
    Enrollment(u8),
}

/// The role a sender authenticated as, which with a kind gives the class.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Sender {
    /// A node of the cluster: its certificate names a node's grant.
    Node,
    /// A participant: its certificate names an actor's, an evaluator's or a runtime's grant.
    Participant,
    /// A joiner: no certificate yet, on the enrollment protocol, which may send nothing else.
    Joiner,
}

impl Sender {
    /// The role a grant authenticates its certificate as: a node's grant a node's, any other a
    /// participant's.
    pub fn of(role: crate::PeerRole) -> Self {
        match role {
            crate::PeerRole::Node { .. } => Self::Node,
            crate::PeerRole::Actor | crate::PeerRole::Evaluator | crate::PeerRole::Runtime => {
                Self::Participant
            }
        }
    }
}

/// The code the greeting crosses as: past every registered tag's range (a tag is a `u16` the
/// operations are numbered from one in), and never zero, which hyper-transport keeps for replies.
pub const HELLO_CODE: u16 = 0x0100;

/// The first code of the enrollment protocol's kinds: its tag is added to it.
pub const ENROLLMENT_CODES: u16 = 0x0200;

/// The largest message any class carries, head and body, bytes: focal-wire's frame ceiling
/// (`frame.rs`'s `MAX_FRAME_BYTES`, 16 MiB), what `WireLimits::max_frame_bytes` may be set to at
/// most. A connection's negotiated bound ([`crate::Negotiated::max_frame_bytes`], the lesser of the
/// two sides') is enforced by the network owner on the body a message declares, before any of it
/// is reserved.
pub const MAX_MESSAGE_BYTES: u64 = 16 * 1024 * 1024;

/// focal's classes for hyper-transport.
pub struct FocalClasses;

impl Classes for FocalClasses {
    type Class = TrafficClass;
    type Kind = Kind;
    type Role = Sender;
    /// Control, exchange and bulk ([`TrafficClass::RANKS`]).
    const RANKS: u8 = 3;

    fn rank(class: TrafficClass) -> u8 {
        class.rank()
    }

    fn class_of(kind: Kind, role: Sender) -> Option<TrafficClass> {
        match (kind, role) {
            // A joiner may enroll and nothing else; anyone may enroll or renew.
            (Kind::Enrollment(_), _) => Some(TrafficClass::Control),
            (_, Sender::Joiner) => None,
            (Kind::Hello, Sender::Node) => Some(TrafficClass::Control),
            (Kind::Operation(tag), Sender::Node) => TrafficClass::of_tag(tag),
            // A participant's message is never the control class, funded from the completion
            // reserve and sent first: what completes admitted work and keeps groups led is the
            // nodes', and no participant spends or outranks it.
            (Kind::Hello, Sender::Participant) => Some(TrafficClass::Exchange),
            (Kind::Operation(tag), Sender::Participant) => {
                TrafficClass::of_tag(tag).map(|class| match class {
                    TrafficClass::Control => TrafficClass::Exchange,
                    other => other,
                })
            }
        }
    }

    fn frame_bound(_class: TrafficClass) -> u64 {
        MAX_MESSAGE_BYTES
    }

    fn kind_code(kind: Kind) -> u16 {
        match kind {
            Kind::Hello => HELLO_CODE,
            Kind::Operation(tag) => tag,
            Kind::Enrollment(tag) => ENROLLMENT_CODES.saturating_add(u16::from(tag)),
        }
    }

    fn kind_of(code: u16) -> Option<Kind> {
        if code == HELLO_CODE {
            return Some(Kind::Hello);
        }
        if let Some(tag) = code.checked_sub(ENROLLMENT_CODES)
            && let Ok(tag) = u8::try_from(tag)
        {
            return Some(Kind::Enrollment(tag));
        }
        TrafficClass::of_tag(code).map(|_| Kind::Operation(code))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_kind_crosses_as_its_code_and_back() {
        let mut kinds = vec![Kind::Hello];
        kinds.extend((1..=33).map(Kind::Operation));
        kinds.extend((0..=u8::MAX).map(Kind::Enrollment));
        let mut codes = std::collections::BTreeSet::new();
        for kind in kinds {
            let code = FocalClasses::kind_code(kind);
            assert_ne!(code, 0, "zero is hyper-transport's reply");
            assert!(codes.insert(code), "{kind:?} shares its code");
            assert_eq!(FocalClasses::kind_of(code), Some(kind));
        }
        // Codes nothing is registered as name nothing.
        for code in [0, 34, 0x00ff, 0x0101, 0x0300, u16::MAX] {
            assert_eq!(FocalClasses::kind_of(code), None, "{code:#x}");
        }
    }

    /// A node's message is its tag's class; a participant's never the control class, which is
    /// funded from the completion reserve and sent first: a participant's control-tagged
    /// operation and its greeting are exchanges, so no participant can spend what completes
    /// admitted work or outrank a node's control (the lane a body was admitted on before the move
    /// to hyper-transport: the completion lane for a node, the ordinary one for any other
    /// identity). A joiner may only enroll.
    #[test]
    fn every_registered_tag_has_a_class_and_a_joiner_may_only_enroll() {
        for tag in 1..=33 {
            let class = TrafficClass::of_tag(tag).unwrap_or_else(|| panic!("tag {tag}"));
            assert_eq!(
                FocalClasses::class_of(Kind::Operation(tag), Sender::Node),
                Some(class)
            );
            assert_eq!(
                FocalClasses::class_of(Kind::Operation(tag), Sender::Participant),
                Some(match class {
                    TrafficClass::Control => TrafficClass::Exchange,
                    other => other,
                }),
                "tag {tag}"
            );
            assert_eq!(
                FocalClasses::class_of(Kind::Operation(tag), Sender::Joiner),
                None
            );
        }
        assert_eq!(
            FocalClasses::class_of(Kind::Hello, Sender::Node),
            Some(TrafficClass::Control)
        );
        assert_eq!(
            FocalClasses::class_of(Kind::Hello, Sender::Participant),
            Some(TrafficClass::Exchange)
        );
        assert_eq!(FocalClasses::class_of(Kind::Hello, Sender::Joiner), None);
        for role in [Sender::Node, Sender::Participant, Sender::Joiner] {
            assert_eq!(
                FocalClasses::class_of(Kind::Enrollment(1), role),
                Some(TrafficClass::Control)
            );
        }
        assert_eq!(TrafficClass::of_tag(0), None);
        assert_eq!(TrafficClass::of_tag(34), None);
    }
}
