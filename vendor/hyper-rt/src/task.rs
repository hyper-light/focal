//! Task slots: the pinned future, its state, its parent and child links, and the counters the watchdog
//! keeps (slates §4.3, `TaskSlot`). The slot's identity (its generation), its outcome and its joiner live on
//! the shard's desk (docs/runtime.md §3.4), where tasks read them; this side is the loop's alone.
//!
//! Children are linked through their parent's slot (first child, siblings) so that a parent's
//! completion cancels them in O(children) and a child's completion unlinks in O(1); no task is
//! untracked (hecate's task-lifecycle law): a task is either joinable (its slot stays until
//! someone joins it) or detached (its slot is reaped the moment it terminates).

use std::future::Future;
use std::pin::Pin;
use std::sync::mpsc::{Receiver, RecvTimeoutError, SyncSender, sync_channel};
use std::time::Duration;

use crate::mem::Encoded;

use crate::error::RtError;
use crate::shard::TaskId;

/// A pinned, boxed future with no output: results flow through channels and handles, never
/// through the task table (hecate's model).
pub type BoxedFuture = Pin<Box<dyn Future<Output = ()> + 'static>>;

/// The "no link" sentinel for child and sibling links.
pub const NO_LINK: u32 = u32::MAX;

/// A spawn request that may cross threads: the future is `Send` because it may be created on any
/// thread and run on the shard's. It may carry the answering half of an admission receipt
/// ([`SpawnRequest::with_receipt`]): the shard answers it when it drains the request, and a request
/// dropped undrained (queued in a control channel the shard exited behind) answers it `Terminated`
/// itself, so a submitter that holds a receipt always learns what became of its task — a submission
/// is never mistaken for an admission (§4.3 "admission").
pub struct SpawnRequest {
    /// The future to run.
    pub future: Pin<Box<dyn Future<Output = ()> + Send + 'static>>,
    /// The parent task on the target shard, if structured under one.
    pub parent: Option<Encoded>,
    /// The receipt to answer at admission; none for a fire-and-forget submission.
    pub receipt: ReceiptSlot,
}

impl SpawnRequest {
    /// A request whose admission nobody reads.
    pub fn new(
        future: Pin<Box<dyn Future<Output = ()> + Send + 'static>>,
        parent: Option<Encoded>,
    ) -> Self {
        Self {
            future,
            parent,
            receipt: ReceiptSlot::none(),
        }
    }

    /// A request whose admission the submitter reads: the request, and the receipt it answers.
    pub fn with_receipt(
        future: Pin<Box<dyn Future<Output = ()> + Send + 'static>>,
        parent: Option<Encoded>,
    ) -> (Self, AdmissionReceipt) {
        let (slot, receipt) = receipt();
        (
            Self {
                future,
                parent,
                receipt: slot,
            },
            receipt,
        )
    }
}

impl std::fmt::Debug for SpawnRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SpawnRequest")
            .field("parent", &self.parent)
            .field("receipt", &self.receipt.0.is_some())
            .finish()
    }
}

/// What became of a spawn request after its submission (§4.3 "admission"): the three ends a request
/// can meet once it is in a shard's control channel, so a submitter that must know whether its task
/// exists reads one of these and never infers it from a submission that merely landed.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Admission {
    /// Admitted to the shard's arena as this task; it runs (or is cancelled) from here on.
    Admitted(TaskId),
    /// Refused by the shard when it drained the request: its arena was full (`TooManyTasks`, the
    /// capacity named), or the slab refused otherwise (`Mem`). The future was dropped unrun.
    Refused(RtError),
    /// Never admitted: the shard was already shutting down when it drained the request (a shard shutting
    /// down admits nothing new, so its arena drains to empty), or the request was dropped undrained — the
    /// shard exited with it still queued. The future was dropped unrun.
    Terminated,
}

/// The answering half of an admission receipt, carried by the request. Answering consumes it; a slot
/// dropped unanswered — the request dropped undrained — answers `Terminated` itself, so the reading
/// half is never left waiting on a request nobody will drain.
pub struct ReceiptSlot(Option<SyncSender<Admission>>);

impl ReceiptSlot {
    /// No receipt: a fire-and-forget submission.
    pub const fn none() -> Self {
        Self(None)
    }

    /// Answers the receipt, if there is one. A reader that has gone away (its wait ended first) makes
    /// the answer undeliverable, which is the reader's choice, not a fault.
    pub fn answer(mut self, admission: Admission) {
        if let Some(sender) = self.0.take() {
            let _ = sender.try_send(admission);
        }
    }
}

impl Drop for ReceiptSlot {
    fn drop(&mut self) {
        if let Some(sender) = self.0.take() {
            let _ = sender.try_send(Admission::Terminated);
        }
    }
}

/// The reading half of an admission receipt: owned by the submitter, bounded at the one answer a
/// request gets, answered exactly once by the shard or by the request's own drop.
#[derive(Debug)]
pub struct AdmissionReceipt(Receiver<Admission>);

impl AdmissionReceipt {
    /// Waits at most `timeout` for the answer; `None` while the request is still queued undrained.
    /// The answering half dropped without answering cannot happen (its drop answers), but is read as
    /// `Terminated` all the same rather than as a lost reply.
    pub fn wait(&self, timeout: Duration) -> Option<Admission> {
        match self.0.recv_timeout(timeout) {
            Ok(admission) => Some(admission),
            Err(RecvTimeoutError::Timeout) => None,
            Err(RecvTimeoutError::Disconnected) => Some(Admission::Terminated),
        }
    }
}

/// A fresh receipt: the answering half for the request, the reading half for the submitter.
/// Shape: a channel of one message — a receipt is answered exactly once, so one slot holds every
/// answer it can ever receive, and the shard's `try_send` never blocks on it.
fn receipt() -> (ReceiptSlot, AdmissionReceipt) {
    let (sender, receiver) = sync_channel(1);
    (ReceiptSlot(Some(sender)), AdmissionReceipt(receiver))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A request dropped undrained answers its receipt `Terminated` by itself: the submitter's wait
    /// ends with the request's fate, never with a lost reply.
    #[test]
    fn a_request_dropped_undrained_answers_its_receipt_terminated() {
        let (request, receipt) = SpawnRequest::with_receipt(Box::pin(async {}), None);
        assert_eq!(
            receipt.wait(Duration::ZERO),
            None,
            "unanswered while the request exists"
        );
        drop(request);
        assert_eq!(receipt.wait(Duration::ZERO), Some(Admission::Terminated));
    }
}

/// Where a task is in its life.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum State {
    /// Waiting to be woken.
    Idle,
    /// On the run queue.
    Queued,
    /// Being polled right now (its future is out of the slot).
    Running,
    /// The future finished or was dropped; waiting for live children to terminate.
    Finishing,
    /// Terminal.
    Done,
}

/// How a task ended.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Outcome {
    /// The future returned.
    Completed,
    /// The future was dropped before returning (cancelled by request, by its parent, or by
    /// shutdown).
    Cancelled,
}

/// One task slot.
pub struct TaskSlot {
    /// The state.
    pub state: State,
    /// The future while it lives (taken out while polled).
    pub future: Option<BoxedFuture>,
    /// The parent's slot index on this shard, if any.
    pub parent: Option<u32>,
    /// Live children.
    pub children: u32,
    /// The first child's slot, or `NO_LINK`.
    pub first_child: u32,
    /// The next sibling's slot, or `NO_LINK`.
    pub next_sibling: u32,
    /// The previous sibling's slot, or `NO_LINK`.
    pub prev_sibling: u32,
    /// The outcome once terminal.
    pub outcome: Option<Outcome>,
    /// Set when cancellation was requested; honoured at the next poll boundary.
    pub cancel_requested: bool,
    /// Whether the slot stays until joined (true) or is reaped at termination (false).
    pub joinable: bool,
    /// Polls so far.
    pub polls: u64,
    /// Polls past the step quantum that were this task's own: past it on the CPU, or waiting inside a call
    /// (the shard's `Counters::long_steps`; a poll the host held off the CPU is not counted).
    pub long_steps: u32,
    /// The longest poll by the wall clock, in nanoseconds.
    pub longest_step_ns: u64,
}

impl TaskSlot {
    /// A fresh slot for `future` under `parent`.
    pub fn new(future: BoxedFuture, parent: Option<u32>, joinable: bool) -> Self {
        Self {
            state: State::Idle,
            future: Some(future),
            parent,
            children: 0,
            first_child: NO_LINK,
            next_sibling: NO_LINK,
            prev_sibling: NO_LINK,
            outcome: None,
            cancel_requested: false,
            joinable,
            polls: 0,
            long_steps: 0,
            longest_step_ns: 0,
        }
    }

    /// An empty slot: no task.
    pub fn empty() -> Self {
        Self {
            state: State::Done,
            future: None,
            parent: None,
            children: 0,
            first_child: NO_LINK,
            next_sibling: NO_LINK,
            prev_sibling: NO_LINK,
            outcome: None,
            cancel_requested: false,
            joinable: false,
            polls: 0,
            long_steps: 0,
            longest_step_ns: 0,
        }
    }

    /// Whether the task reached its terminal state.
    pub const fn is_done(&self) -> bool {
        matches!(self.state, State::Done)
    }
}

impl std::fmt::Debug for TaskSlot {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TaskSlot")
            .field("state", &self.state)
            .field("parent", &self.parent)
            .field("children", &self.children)
            .field("outcome", &self.outcome)
            .field("joinable", &self.joinable)
            .field("polls", &self.polls)
            .finish()
    }
}
