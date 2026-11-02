//! Trusted local retry-journal IO runs on a writer thread of its own beside each control owner
//! (`write_journals`). Payloads and journal locks move to it, no async executor performs the
//! fsync, and no consensus owner waits on one. On the root's owner, a placement intent's journal
//! write held the owner in `fsync` for as long as the volume took (fleet33 k29, 2026-11-02:
//! btrfs's log commit and its full commit with device barriers, on the journal's staged file and
//! its directory). Meanwhile the root replica took no input and sent no heartbeat, so its group
//! stalled for up to 20 s in a bootstrap and elected another leader as the stall ended (m7, as in
//! p9-0639).
use super::*;
use focal_enrollment::{EnrollmentError, PrivateJournal};

#[derive(Debug, thiserror::Error)]
pub(crate) enum LocalIntentError {
    #[error("local intent admission exceeds its bounded allowance")]
    Capacity,
    #[error("local intent owner is unavailable; an admitted write may have completed")]
    Unavailable,
    #[error("local intent persistence: {0}")]
    Persistence(EnrollmentError),
}

pub(crate) struct LocalIntentFailure {
    pub error: LocalIntentError,
    /// Present only when the write was definitely not admitted. An unknown
    /// outcome requires reopening the durable journal, never a new intent.
    /// Boxed: the journal with its sealing keys made the failure 128 bytes
    /// and more on Windows, where a path is wider, and every result carrying
    /// it as large.
    pub rejected: Option<Rejected>,
}
/// A write definitely not admitted: the journal it was to go to, and its bytes.
pub(crate) type Rejected = Box<(PrivateJournal, Vec<u8>)>;
impl std::fmt::Debug for LocalIntentFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("LocalIntentFailure")
            .field("error", &self.error)
            .field("rejected", &self.rejected.is_some())
            .finish()
    }
}
type IntentResult = Result<PrivateJournal, LocalIntentError>;

/// Writes the journal through its host's journal writer and waits for the answer, which comes once
/// the write is durable or failed, or as the writer ends. A write the writer did not admit (its
/// queue or memory full) is answered at once with the journal back, for its caller to ask again on
/// its next pass. An admitted write was given 5 s of the wall's clock, and one that took longer
/// was let go with the journal it held: every later write found none, and the node's network
/// controller or placement intents stopped for good. Under the load of a 33-node run on a busy
/// host the founder's root network owner stopped so in two runs' bootstraps and twelve nodes'
/// placement intents in a third (fedora, 2026-10-30): a slow disk was made a permanent failure.
pub(crate) async fn save_local_intent(
    host: &ControlHost,
    journal: &mut Option<PrivateJournal>,
    bytes: Vec<u8>,
) -> Result<(), LocalIntentError> {
    if bytes.len() > 60 * 1024 {
        return Err(LocalIntentError::Capacity);
    }
    let owner = journal.take().ok_or(LocalIntentError::Unavailable)?;
    match host.persist_local_intent(owner, bytes).await {
        Ok(owner) => {
            *journal = Some(owner);
            Ok(())
        }
        Err(failure) => {
            *journal = failure.rejected.map(|rejected| rejected.0);
            Err(failure.error)
        }
    }
}

/// What a host's journal writer takes, in the order it was admitted.
pub(super) enum JournalWork {
    Write(Box<LocalIntentWrite>),
    /// Answered once every write admitted before it is made, or failed: the barrier for one that
    /// reopens a journal whose last write it abandoned (`ControlHost::settle_local_intents`). A
    /// node opens its journals once, so only its tests reopen one.
    #[cfg(test)]
    Settled(OneshotSender<()>),
}

pub(super) struct LocalIntentWrite {
    journal: PrivateJournal,
    bytes: Vec<u8>,
    response: OneshotSender<(IntentResult, Allocation)>,
    input: Allocation,
}
impl LocalIntentWrite {
    fn rejected(self, error: LocalIntentError) -> LocalIntentFailure {
        LocalIntentFailure {
            error,
            rejected: Some(Box::new((self.journal, self.bytes))),
        }
    }
    pub(super) fn persist(mut self) {
        let result = self
            .journal
            .replace(&self.bytes)
            .map_err(LocalIntentError::Persistence);
        drop(self.bytes);
        let _ = self
            .response
            .send((result.map(|()| self.journal), self.input));
    }
}

impl ControlHost {
    /// Waits until every local intent write this host admitted before the call is made, or failed:
    /// the barrier for one that reopens a journal whose last write it abandoned, whose lock that
    /// write holds until its writer finishes it. Refused at once where the writer's queue is full
    /// or the writer is gone.
    #[cfg(test)]
    pub(crate) async fn settle_local_intents(&self) -> Result<(), LocalIntentError> {
        let (reply, settled) = hyper_rt::sync::oneshot().map_err(|_| LocalIntentError::Capacity)?;
        self.journals
            .try_send(JournalWork::Settled(reply))
            .map_err(|error| match error {
                mpsc::TrySendError::Full(_) => LocalIntentError::Capacity,
                mpsc::TrySendError::Disconnected(_) => LocalIntentError::Unavailable,
            })?;
        settled.await.map_err(|_| LocalIntentError::Unavailable)
    }
    /// Local composition only; wire callers cannot supply a path or journal.
    /// Rejected admission returns ownership intact. Once admitted, any failure
    /// consumes the journal and requires recovery of its original durable state.
    pub(crate) async fn persist_local_intent(
        &self,
        journal: PrivateJournal,
        bytes: Vec<u8>,
    ) -> Result<PrivateJournal, LocalIntentFailure> {
        let allowance = bytes
            .len()
            .checked_mul(2)
            .and_then(|size| size.checked_add(bytes.capacity()))
            .and_then(|size| size.checked_add(16 * 1024));
        let input = if bytes.len() > 60 * 1024 {
            Err(LocalIntentError::Capacity)
        } else if let Some(allowance) = allowance {
            self.budget
                .reserve(BudgetKind::Control, BudgetLane::Completion, allowance)
                .map(|reservation| reservation.commit())
                .map_err(|error| match error {
                    focal_memory::MemoryError::Capacity { .. }
                    | focal_memory::MemoryError::DiskCapacity { .. }
                    | focal_memory::MemoryError::AllocationFailed => LocalIntentError::Capacity,
                    _ => LocalIntentError::Unavailable,
                })
        } else {
            Err(LocalIntentError::Capacity)
        };
        let input = match input {
            Ok(input) => input,
            Err(error) => {
                return Err(LocalIntentFailure {
                    error,
                    rejected: Some(Box::new((journal, bytes))),
                });
            }
        };
        let Ok((response, receive)) = hyper_rt::sync::oneshot() else {
            return Err(LocalIntentFailure {
                error: LocalIntentError::Capacity,
                rejected: Some(Box::new((journal, bytes))),
            });
        };
        let write = Box::new(LocalIntentWrite {
            journal,
            bytes,
            response,
            input,
        });
        if let Err(error) = self.journals.try_send(JournalWork::Write(write)) {
            // Not admitted either way: the journal goes back whole.
            let (error, work) = match error {
                mpsc::TrySendError::Full(work) => (LocalIntentError::Capacity, work),
                mpsc::TrySendError::Disconnected(work) => (LocalIntentError::Unavailable, work),
            };
            return Err(match work {
                JournalWork::Write(write) => write.rejected(error),
                #[cfg(test)]
                JournalWork::Settled(_) => LocalIntentFailure {
                    error,
                    rejected: None,
                },
            });
        }
        let (result, _input) = receive.await.map_err(|_| LocalIntentFailure {
            error: LocalIntentError::Unavailable,
            rejected: None,
        })?;
        result.map_err(|error| LocalIntentFailure {
            error,
            rejected: None,
        })
    }
}

#[cfg(test)]
#[path = "local_intent_host_tests.rs"]
mod tests;

/// One control host's journal writer: each admitted write made whole and durable in the order it
/// was admitted, until the host's every handle is gone. A write that unwound is answered by
/// nothing: its caller hears the writer's absence, as for any write whose outcome is unknown, and
/// recovers the journal's durable state; the writes after it go on.
pub(super) fn write_journals(writes: &mpsc::Receiver<JournalWork>) {
    for work in writes {
        match work {
            JournalWork::Write(write) => {
                let _ = catch_unwind(AssertUnwindSafe(move || write.persist()));
            }
            // The writes before it are made: a caller that has gone hears nothing.
            #[cfg(test)]
            JournalWork::Settled(reply) => {
                let _ = reply.send(());
            }
        }
    }
}
