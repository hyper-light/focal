//! focal's network on hyper-transport and hyper-rt (27 §15, the migration of 2026-10-05): one
//! endpoint per socket, driven by one owner task ([`owner`]), reached through bounded channels.
//! focal-wire's domain layer — its envelopes, profiles, grants and handlers — rides on it: an
//! operation crosses as one exchange whose kind is its registered tag and whose body is its
//! envelope; a connection is greeted once, as focal-wire's connections always were.

mod budget;
mod classes;
mod directory;
mod owner;

pub(crate) use budget::NetBudget;
pub use budget::NetBudgetStats;
pub use classes::{ENROLLMENT_CODES, FocalClasses, HELLO_CODE, Kind, MAX_MESSAGE_BYTES, Sender};
pub use directory::Inbound;
pub(crate) use directory::NetDirectory;
pub(crate) use owner::{Answer, Command, Driver, Failed, Linked, Network};
pub use owner::{HELLO_BYTES, NetStats, Serve, ServeFuture, Served};
