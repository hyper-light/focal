#![cfg_attr(
    test,
    allow(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::panic,
        clippy::indexing_slicing,
        clippy::arithmetic_side_effects,
        clippy::disallowed_macros,
        clippy::cognitive_complexity
    )
)]
//! The durable shell around hyper-raft's core (`docs/durable.md`): one shell for slates, focal
//! and mantle, generic over the log a group's writes become durable in ([`LogStore`]) and the
//! state machine it applies to ([`StateMachine`]), whose release rules are stated as invariants
//! (§3), not as modes.
//!
//! The core decides; it never writes, sends or applies. A [`Replica`] turns what the core asks
//! for into durable writes, messages, applied entries and answers, in the order Raft's safety
//! needs, and what the log and the network report back into calls on the core. It takes the
//! core's `Ready`s ahead of their persistence up to the store's depth (core step R-4), holds a
//! change of configuration, and anything the state machine acts on at its next start, behind
//! the durable commit (the commit fence, I5), and opens a member on what its log and state
//! machine hold durably, finishing what a crash left between them (§4.3).
//!
//! An [`Owner`] holds one owner thread's replicas in an arena and gives them turns by deficit
//! round robin; the crate spawns no thread, opens no file and reads no clock (`CLAUDE.md` §1):
//! the owner's embedder gives it its wakers and its clock. hyper-log's group handle is the disk
//! store ([`GroupStore`]); [`RamStore`] completes each write as it is submitted, slates' case.
//!
//! Where the design waits on core steps not built yet, the shell keeps the place for them: R-5
//! (a marked member repaired by its lost entries; the shell asks for a snapshot until then),
//! R-6 (the apply pause, a leader's own-term entries applied before its own write is durable,
//! and the durable commit carried in answers), R-7 (CTRL's leader-side recovery). `ORIGIN.md`
//! records what each needs.

mod budget;
mod held;
mod hyperlog;
mod machine;
mod memory;
mod owner;
mod replica;
mod store;

pub use budget::{Budget, Bytes, Unbounded};
pub use held::Held;
pub use hyperlog::{ClaimError, ENTRY_OVERHEAD, GroupStore, decode_entry, encode_entry};
pub use machine::{Fatal, StateMachine};
pub use memory::RamStore;
pub use owner::{Full, Handle, Owner};
pub use replica::{Cause, Driven, OpenError, Output, Replica, ReplicaError, Settings, Writes};
pub use store::{Entries, EntryRef, Fault, Health, LogStore, Point, StoreView, Write};
