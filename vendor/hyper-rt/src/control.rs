//! Control messages to a shard: spawn, cancel, shutdown. They travel over a bounded
//! standard channel per shard (`std::sync::mpsc::sync_channel`), never over the wake rings,
//! because a spawn carries a boxed future and a ring carries words (§4.3). The channel is
//! control, not a data path: a spawn is admission, and `try_send` refuses instead of blocking
//! when the bound is reached (the bound is the shard's admission limit, since nothing beyond it
//! could be admitted anyway).

use crate::mem::Encoded;

use crate::task::SpawnRequest;

/// A control message.
#[derive(Debug)]
pub enum Control {
    /// Take ownership of a spawn request (a boxed future and its placement).
    Spawn(Box<SpawnRequest>),
    /// Cancel the task named by the packed word.
    Cancel(Encoded),
    /// Finish every task and exit the loop.
    Shutdown,
}
