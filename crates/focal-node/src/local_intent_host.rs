//! Trusted local retry-journal IO uses the existing physical control owner.
//! Payloads and journal locks move; no async executor performs the fsync.
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

/// Writes the journal through its owner and waits for the owner's answer, which comes once the
/// write is durable or failed, or as the owner stops. A write the owner did not admit (its queue
/// or memory full) is answered at once with the journal back, for its caller to ask again on its
/// next pass. An admitted write was given 5 s of the wall's clock, and one that took longer
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
        let work = Work::PersistLocalIntent(Box::new(LocalIntentWrite {
            journal,
            bytes,
            response,
            input,
        }));
        if let Err(error) = self.sender.try_send(work) {
            let (error, work) = match error {
                mpsc::TrySendError::Full(work) => (LocalIntentError::Capacity, work),
                mpsc::TrySendError::Disconnected(work) => (LocalIntentError::Unavailable, work),
            };
            return Err(match work {
                Work::PersistLocalIntent(work) => work.rejected(error),
                _ => LocalIntentFailure {
                    error: LocalIntentError::Unavailable,
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
