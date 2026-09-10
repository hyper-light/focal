//! Durable exact-retry intents of the placement agent, one journal per
//! metadata owner it speaks to. A command is journaled before proposal and
//! retried with the identical request identity until a receipt or a
//! pre-admission refusal resolves it; a restart resumes from the journal.
use crate::{control_host::ControlHost, embedded::atomic_file};
use focal_control::{
    ControlCommand, ControlFailure, ControlIdentity, ControlReceipt, ControlRequest,
    ControlRequestId,
};
use focal_enrollment::PrivateJournal;
use serde::{Deserialize, Serialize};
use std::{future::Future, path::Path};

#[derive(Debug, thiserror::Error)]
pub enum IntentError {
    #[error("placement intent journal is inconsistent with this node")]
    Identity,
    #[error("placement intent journal exceeds its bounded allowance")]
    Capacity,
    #[error("placement intent owner stopped")]
    Stopped,
    #[error("metadata owner: {0}")]
    Control(#[from] ControlFailure),
    #[error("placement intent encoding: {0}")]
    Encoding(#[from] postcard::Error),
    #[error("placement intent journal: {0}")]
    Io(#[from] std::io::Error),
    #[error("placement intent persistence: {0}")]
    Persistence(#[from] focal_enrollment::EnrollmentError),
}

#[derive(Serialize, Deserialize)]
struct Saved {
    schema: u16,
    target: ControlIdentity,
    client: [u8; 16],
    completed: u64,
    pending: Option<ControlRequest>,
}

/// What one submission of the pending intent established.
#[derive(Debug, PartialEq, Eq)]
pub enum IntentOutcome {
    /// Durably committed with this receipt; the journal advanced.
    Committed(ControlReceipt),
    /// Refused before admission (a stale compare); the sequence stays free.
    /// The failure names why, for the agent's diagnostics.
    Refused(ControlFailure),
    /// No decision yet: not leader, not ready, capacity or an unknown outcome.
    Retry,
}

pub(crate) struct IntentJournal {
    journal: Option<PrivateJournal>,
    saved: Saved,
}
impl IntentJournal {
    /// `name` is the journal directory under `<root>/cluster/`; a marker file
    /// next to it detects a journal removed after initialization.
    pub(crate) fn open(
        root: &Path,
        name: &str,
        target: ControlIdentity,
        client: [u8; 16],
    ) -> Result<Self, IntentError> {
        let path = root.join("cluster").join(name);
        let marker = root.join(format!("{}.initialized", name.to_ascii_uppercase()));
        if marker.exists() && !path.join("journal.bin").is_file() {
            return Err(IntentError::Identity);
        }
        // A joined host has no cluster directory until its first journal.
        crate::embedded::durable_dir(&root.join("cluster"))?;
        let mut journal = PrivateJournal::open(path)?;
        let saved = match journal.read()? {
            Some(bytes) => {
                let (value, rest): (Saved, _) = postcard::take_from_bytes(&bytes)?;
                if !rest.is_empty() {
                    return Err(IntentError::Identity);
                }
                value
            }
            None => {
                let value = Saved {
                    schema: 1,
                    target,
                    client,
                    completed: 0,
                    pending: None,
                };
                journal.replace(&postcard::to_stdvec(&value)?)?;
                value
            }
        };
        if saved.schema != 1
            || saved.target != target
            || saved.client != client
            || saved.pending.as_ref().is_some_and(|request| {
                request.id.client != client
                    || Some(request.id.sequence) != saved.completed.checked_add(1)
                    || request.acknowledged_through != saved.completed
            })
        {
            return Err(IntentError::Identity);
        }
        if !marker.exists() {
            atomic_file(&marker, b"placement intent journal initialized")?;
        }
        Ok(Self {
            journal: Some(journal),
            saved,
        })
    }
    pub(crate) fn pending(&self) -> Option<&ControlRequest> {
        self.saved.pending.as_ref()
    }
    pub(crate) fn completed(&self) -> u64 {
        self.saved.completed
    }
    /// Journal one new intent. Refused while another is unresolved.
    pub(crate) async fn intend(
        &mut self,
        host: &ControlHost,
        command: ControlCommand,
    ) -> Result<(), IntentError> {
        if self.saved.pending.is_some() {
            return Err(IntentError::Identity);
        }
        self.saved.pending = Some(ControlRequest {
            id: ControlRequestId {
                client: self.saved.client,
                sequence: self
                    .saved
                    .completed
                    .checked_add(1)
                    .ok_or(IntentError::Capacity)?,
            },
            acknowledged_through: self.saved.completed,
            command,
        });
        self.save_on(host).await
    }
    /// Submit the pending intent exactly once more.
    /// Submit the pending intent exactly once more through `submit`; the
    /// journal itself persists through the local owner `host`.
    pub(crate) async fn advance<Fut>(
        &mut self,
        host: &ControlHost,
        submit: impl FnOnce(ControlRequest) -> Fut,
    ) -> Result<IntentOutcome, IntentError>
    where
        Fut: Future<Output = Result<ControlReceipt, ControlFailure>>,
    {
        let request = self
            .saved
            .pending
            .as_ref()
            .ok_or(IntentError::Identity)?
            .clone();
        let expected = request.id;
        match submit(request).await {
            Ok(receipt) => {
                if receipt.request != expected
                    || receipt.committed_index == 0
                    || receipt.committed_term == 0
                {
                    return Err(IntentError::Identity);
                }
                self.saved.completed = receipt.request.sequence;
                self.saved.pending = None;
                self.save_on(host).await?;
                Ok(IntentOutcome::Committed(receipt))
            }
            // The owner refuses before admission: the sequence stays free and
            // the caller replans from a fresh observation.
            Err(
                failure @ (ControlFailure::CompareFailed
                | ControlFailure::Rejected
                | ControlFailure::Unauthorized
                | ControlFailure::Invalid
                | ControlFailure::WrongOwner),
            ) => {
                self.saved.pending = None;
                self.save_on(host).await?;
                Ok(IntentOutcome::Refused(failure))
            }
            Err(
                ControlFailure::NotLeader { .. }
                | ControlFailure::NotReady
                | ControlFailure::Capacity
                | ControlFailure::Unavailable
                | ControlFailure::OutcomeUnknown,
            ) => Ok(IntentOutcome::Retry),
            Err(error) => Err(error.into()),
        }
    }
    async fn save_on(&mut self, host: &ControlHost) -> Result<(), IntentError> {
        crate::control_host::save_local_intent(
            host,
            &mut self.journal,
            postcard::to_stdvec(&self.saved)?,
        )
        .await
        .map_err(|error| match error {
            crate::control_host::LocalIntentError::Capacity => IntentError::Capacity,
            crate::control_host::LocalIntentError::Unavailable => IntentError::Stopped,
            crate::control_host::LocalIntentError::Persistence(error) => {
                IntentError::Persistence(error)
            }
        })
    }
}
