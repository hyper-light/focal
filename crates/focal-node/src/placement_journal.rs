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

#[cfg(test)]
#[path = "placement_journal_tests.rs"]
mod tests;

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

/// The clients one journal may be named by: a node's local client where it
/// leads the owner, its enrolled principal where it submits through the
/// leader (24 §16), and nothing else.
const MAX_CLIENTS: usize = 4;
#[derive(Serialize, Deserialize)]
struct Saved {
    schema: u16,
    target: ControlIdentity,
    client: [u8; 16],
    completed: u64,
    pending: Option<ControlRequest>,
    /// The retry window of every other client this journal was named by:
    /// the last sequence each completed, resumed when it is named again
    /// (schema 2; the audit's F24 — a founder that follows its partition
    /// after a restart submits through its leader as its principal, and
    /// leads it again later).
    #[serde(default)]
    windows: std::collections::BTreeMap<[u8; 16], u64>,
}
/// What schema 1 wrote: one client, one window.
#[derive(Deserialize)]
struct SavedV1 {
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
    /// No decision yet: not leader, not ready, capacity or an unknown
    /// outcome, named for the agent's diagnostics; asked again next pass.
    Retry(ControlFailure),
}

pub(crate) struct IntentJournal {
    journal: Option<PrivateJournal>,
    saved: Saved,
    /// The client this journal was opened with, adopted once nothing is pending.
    wanted: [u8; 16],
}

/// Names `saved` by `client`, keeping the window of the client it was named by: the owner's
/// retry floor for a client is never crossed when the client comes back.
fn adopt(saved: &mut Saved, client: [u8; 16]) -> Result<(), IntentError> {
    if saved.client == client {
        return Ok(());
    }
    if saved.windows.len() >= MAX_CLIENTS && !saved.windows.contains_key(&client) {
        return Err(IntentError::Capacity);
    }
    let previous = saved.client;
    let previous_completed = saved.completed;
    saved.completed = saved.windows.remove(&client).unwrap_or(0);
    saved.windows.insert(previous, previous_completed);
    saved.client = client;
    Ok(())
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
                let mut value = match postcard::take_from_bytes::<Saved>(&bytes) {
                    Ok((value, rest)) if rest.is_empty() && value.schema == 2 => value,
                    _ => {
                        let (old, rest): (SavedV1, _) = postcard::take_from_bytes(&bytes)?;
                        if !rest.is_empty() || old.schema != 1 {
                            return Err(IntentError::Identity);
                        }
                        Saved {
                            schema: 2,
                            target: old.target,
                            client: old.client,
                            completed: old.completed,
                            pending: old.pending,
                            windows: std::collections::BTreeMap::new(),
                        }
                    }
                };
                // A journal with nothing pending adopts the client it is
                // opened with — a host's root journal was named by a local
                // client before hosts submitted root intents through the
                // root leader as their enrolled principal (24 §16); a node
                // names its partition intents by its local client where it
                // leads the partition and by its principal where it follows
                // (F24) — keeping each client's window where it stood, so
                // the owner's retry floor for it is never crossed.
                if value.client != client && value.pending.is_none() {
                    adopt(&mut value, client)?;
                    journal.replace(&postcard::to_stdvec(&value)?)?;
                }
                value
            }
            None => {
                let value = Saved {
                    schema: 2,
                    target,
                    client,
                    completed: 0,
                    pending: None,
                    windows: std::collections::BTreeMap::new(),
                };
                journal.replace(&postcard::to_stdvec(&value)?)?;
                value
            }
        };
        // A journal holding an intent keeps the client it was journaled under
        // until the intent resolves, whichever client it is opened with now:
        // both ways in present an intent as its own client (the local ingress
        // builds its peer from it, and the owners' placement ingress admits a
        // node's local client beside its principal, 24 §16), so an intent
        // journaled while the node led resolves through the node that leads
        // after a restart. Refusing the journal instead, as it had been, left
        // the node unable to place anything in that partition until it led it
        // again (a founder restarted into a partition its seated peer came to
        // lead never merged it back: Windows CI, run 38201579852). It adopts the
        // client it is opened with once the intent resolves.
        if saved.client != client
            && saved.windows.len() >= MAX_CLIENTS
            && !saved.windows.contains_key(&client)
        {
            return Err(IntentError::Capacity);
        }
        if saved.schema != 2
            || saved.windows.len() > MAX_CLIENTS
            || saved.target != target
            || (saved.client != client && saved.pending.is_none())
            || saved.pending.as_ref().is_some_and(|request| {
                request.id.client != saved.client
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
            wanted: client,
        })
    }
    pub(crate) fn pending(&self) -> Option<&ControlRequest> {
        self.saved.pending.as_ref()
    }
    /// What this journal holds in memory: its pending request (at the bytes
    /// it encodes to, which every heap field of it is no larger than, and its
    /// own size) and the other clients' retry windows.
    pub(crate) fn held_bytes(&self) -> Option<usize> {
        let pending = match &self.saved.pending {
            Some(request) => postcard::experimental::serialized_size(request)
                .ok()?
                .checked_add(size_of::<ControlRequest>())?,
            None => 0,
        };
        focal_memory::map_bytes::<[u8; 16], u64>(self.saved.windows.len())?.checked_add(pending)
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
                self.resolved()?;
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
                self.resolved()?;
                self.save_on(host).await?;
                Ok(IntentOutcome::Refused(failure))
            }
            Err(
                failure @ (ControlFailure::NotLeader { .. }
                | ControlFailure::NotReady
                | ControlFailure::Capacity
                | ControlFailure::Unavailable
                | ControlFailure::OutcomeUnknown),
            ) => Ok(IntentOutcome::Retry(failure)),
            Err(error) => Err(error.into()),
        }
    }
    /// The pending intent is resolved: nothing is pending, and the journal is named by the
    /// client it was opened with.
    fn resolved(&mut self) -> Result<(), IntentError> {
        self.saved.pending = None;
        adopt(&mut self.saved, self.wanted)
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
