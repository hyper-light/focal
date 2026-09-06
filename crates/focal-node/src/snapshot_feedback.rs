//! Owned transport feedback for one current snapshot flight per peer. The
//! receiver belongs to the replica incarnation; stale frames cannot reach a
//! replacement owner or a newer flight. No ingress queue can lose a failure.
use focal_consensus::{Message, MessageType, SnapshotStatus};
use focal_memory::{Allocation, BudgetKind, BudgetLane, MemoryBudget};
use std::collections::BTreeMap;
use tokio::sync::oneshot;

#[derive(Debug, thiserror::Error)]
pub(crate) enum SnapshotFeedbackError {
    #[error("snapshot feedback capacity exhausted")]
    Capacity,
    #[error("invalid snapshot transport identity")]
    Invalid,
}
struct Flight {
    term: u64,
    index: u64,
    receiver: oneshot::Receiver<SnapshotStatus>,
    completed: Option<SnapshotStatus>,
    _charge: Allocation,
}
#[derive(Default)]
pub(crate) struct SnapshotFeedback {
    flights: BTreeMap<u64, Flight>,
}
impl SnapshotFeedback {
    /// Call before encoding or admitting the outbound frame. Every later drop
    /// then becomes Failure, including an oversized payload or full channel.
    pub(crate) fn begin(
        &mut self,
        message: &Message,
        budget: &MemoryBudget,
    ) -> Result<Option<oneshot::Sender<SnapshotStatus>>, SnapshotFeedbackError> {
        if message.get_msg_type() != MessageType::MsgSnapshot {
            return Ok(None);
        }
        if message.to == 0 || message.term == 0 || message.get_snapshot().get_metadata().index == 0
        {
            return Err(SnapshotFeedbackError::Invalid);
        }
        if self.flights.len() >= 4096 && !self.flights.contains_key(&message.to) {
            return Err(SnapshotFeedbackError::Capacity);
        }
        // A newly emitted flight supersedes the prior receiver even when this
        // admission fails. Repeated snapshots may have the same term/index.
        self.flights.remove(&message.to);
        // The outbound frame's existing 4KiB metadata allowance independently
        // covers the sender/oneshot if a term change or replacement drops this
        // receiver first. This charge covers the oneshot and a complete BTree node per retained
        // entry, including sparsely occupied nodes after other peers finish.
        let charge = budget
            .reserve(BudgetKind::Control, BudgetLane::Completion, 4096)
            .map_err(|_| SnapshotFeedbackError::Capacity)?
            .commit();
        let (sender, receiver) = oneshot::channel();
        self.flights.insert(
            message.to,
            Flight {
                term: message.term,
                index: message.get_snapshot().get_metadata().index,
                receiver,
                completed: None,
                _charge: charge,
            },
        );
        Ok(Some(sender))
    }
    /// The caller must first finish outstanding Raft persistence. Transport
    /// Finish means remote ingress accepted, never applied or quorum committed.
    pub(crate) fn poll<E>(
        &mut self,
        term: u64,
        mut report: impl FnMut(u64, u64, u64, SnapshotStatus) -> Result<(), E>,
    ) -> Result<(), E> {
        let mut failure = None;
        self.flights.retain(|peer, flight| {
            if flight.term != term {
                return false;
            }
            if failure.is_some() {
                return true;
            }
            let status = match flight.completed {
                Some(status) => status,
                None => match flight.receiver.try_recv() {
                    Ok(status) => status,
                    Err(oneshot::error::TryRecvError::Closed) => SnapshotStatus::Failure,
                    Err(oneshot::error::TryRecvError::Empty) => return true,
                },
            };
            flight.completed = Some(status);
            match report(*peer, flight.term, flight.index, status) {
                Ok(()) => false,
                Err(error) => {
                    failure = Some(error);
                    true
                }
            }
        });
        match failure {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }
}

pub(crate) fn complete(sender: &mut Option<oneshot::Sender<SnapshotStatus>>, accepted: bool) {
    if let Some(sender) = sender.take() {
        let _ = sender.send(if accepted {
            SnapshotStatus::Finish
        } else {
            SnapshotStatus::Failure
        });
    }
}

#[cfg(test)]
#[path = "snapshot_feedback_tests.rs"]
mod tests;
