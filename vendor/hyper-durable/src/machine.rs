//! What the shell asks of the state machine a group applies to (`docs/durable.md` §9).
use hyper_raft::proto::{ConfChangeV2, ConfState};

use crate::store::{EntryRef, Point};

/// The state machine failed in a way that leaves its state unknown: the replica is fenced and
/// reopened from what is durable.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
#[error("the state machine failed: {0}")]
pub struct Fatal(pub &'static str);

/// A group's state machine, owned by its replica.
///
/// Its durable state is what a restart opens at: [`StateMachine::durable`] says how far it
/// reaches, and [`StateMachine::configuration`] the configuration it held there, which it keeps
/// beside its rows. A machine that keeps nothing durable of its own (focal's control groups, a
/// replay of the log) reports the point before the log's first entry, and the shell replays
/// what the log states committed (`docs/durable.md` §4.3).
pub trait StateMachine {
    /// What applying an entry answers its callers.
    type Answer;

    /// Applies a committed entry that changes no configuration, appending what it answers.
    fn apply(&mut self, entry: &EntryRef<'_>, answers: &mut Vec<Self::Answer>)
    -> Result<(), Fatal>;

    /// A committed change of configuration at `at`, as its entry stated it (`change`, its
    /// context among it), left the group with `configuration`, which the machine keeps with its
    /// state from this index on; the configuration before is the machine's own
    /// ([`StateMachine::configuration`]). A change the core refused, alike on every member,
    /// leaves the configuration it had. An owner that reports each change with what it carried,
    /// as focal's does, reads it here.
    fn apply_change(
        &mut self,
        at: Point,
        change: &ConfChangeV2,
        configuration: &ConfState,
    ) -> Result<(), Fatal>;

    /// The last entry a restart opens with applied, and its term.
    fn durable(&self) -> Point;

    /// The configuration as of the last entry applied.
    fn configuration(&self) -> &ConfState;

    /// Whether a member acts on this entry at its next start, before its group tells it anything
    /// (`docs/durable.md` §4.1, I5): such an entry is applied only once the member's durable
    /// commit covers it. A group whose members act at start on everything (focal's control
    /// groups) says so for every entry.
    fn acts_at_start(&self, entry: &EntryRef<'_>) -> bool;

    /// An image of the machine's state at a point it applied, written into `into`, with the
    /// point and the configuration the group held there, which a snapshot carries with it
    /// (Ongaro and Ousterhout 2014, §7): what a leader sends a member that lacks entries
    /// its log no longer holds. A machine that images on demand gives everything applied, under
    /// [`StateMachine::configuration`]; one that keeps its owner's checkpoints, as focal's does,
    /// gives the latest, which may be behind what it applied and is never behind the log's
    /// start, for a compaction never passes [`StateMachine::durable`] (I8).
    fn image(&mut self, into: &mut Vec<u8>) -> Result<(Point, ConfState), Fatal>;

    /// Replaces the machine's state with `image`, which is of `at` under `configuration`, and
    /// makes it durable before returning: the log's start moves to `at` only after (I8).
    fn install(&mut self, image: &[u8], at: Point, configuration: &ConfState) -> Result<(), Fatal>;

    /// Makes everything applied durable, so that the log may be compacted behind it.
    fn persist(&mut self) -> Result<(), Fatal>;
}
