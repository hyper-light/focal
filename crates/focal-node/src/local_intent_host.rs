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
    pub rejected: Option<(PrivateJournal, Vec<u8>)>,
}
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

/// Retries only definite admission rejection, preserving the original bytes.
/// A bounded deadline prevents a stalled writer/admission path from pinning a
/// controller's authorization projection indefinitely.
pub(crate) async fn save_local_intent(
    host: &ControlHost,
    journal: &mut Option<PrivateJournal>,
    mut bytes: Vec<u8>,
) -> Result<(), LocalIntentError> {
    if bytes.len() > 60 * 1024 {
        return Err(LocalIntentError::Capacity);
    }
    let deadline = Instant::now()
        .checked_add(Duration::from_secs(5))
        .ok_or(LocalIntentError::Capacity)?;
    loop {
        let owner = journal.take().ok_or(LocalIntentError::Unavailable)?;
        // An admitted write may still complete after timeout. Drop its future
        // and recover the original disk record before any subsequent request.
        let remaining = deadline.saturating_duration_since(Instant::now());
        match tokio::time::timeout(remaining, host.persist_local_intent(owner, bytes)).await {
            Ok(Ok(owner)) => {
                *journal = Some(owner);
                return Ok(());
            }
            Ok(Err(LocalIntentFailure {
                error: LocalIntentError::Capacity,
                rejected: Some((owner, returned)),
            })) => {
                *journal = Some(owner);
                bytes = returned;
                if Instant::now() >= deadline {
                    return Err(LocalIntentError::Capacity);
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            Ok(Err(failure)) => {
                *journal = failure.rejected.map(|(owner, _)| owner);
                return Err(failure.error);
            }
            Err(_) => return Err(LocalIntentError::Unavailable),
        }
    }
}

pub(super) struct LocalIntentWrite {
    journal: PrivateJournal,
    bytes: Vec<u8>,
    response: oneshot::Sender<(IntentResult, Allocation)>,
    input: Allocation,
}
impl LocalIntentWrite {
    fn rejected(self, error: LocalIntentError) -> LocalIntentFailure {
        LocalIntentFailure {
            error,
            rejected: Some((self.journal, self.bytes)),
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
                    rejected: Some((journal, bytes)),
                });
            }
        };
        let (response, receive) = oneshot::channel();
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
