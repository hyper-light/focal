//! A node's own credential over time: the controller renews it before it
//! expires (or when the operator asks), installs the renewed receipt under
//! the same key, and presents the new certificate on every path at once.
//! The founder's identity is the bootstrap authority's own server
//! certificate and is not renewed here.
use crate::{network_listener::ListenerIdentity, placement_control::PlacementHandle};
use focal_enrollment::JoinFailure;
use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc, oneshot};

/// Renew this far ahead of expiry: a third of the standard credential life.
pub const RENEWAL_WINDOW_SECONDS: i64 = 10 * 86400;
/// Retry a failed automatic renewal no sooner than this.
pub const RENEWAL_RETRY_SECONDS: i64 = 60;
/// How long the certificate a renewal replaces keeps authorizing, so
/// connections and statements in flight complete; the sponsor decides it.
pub const DEFAULT_GRACE_SECONDS: u64 = 60;
/// A campaign knob: the sponsor's grace in seconds (zero retires the
/// previous certificate at the renewal itself). Read once at startup.
pub const GRACE_ENV: &str = "FOCAL_CREDENTIAL_GRACE_SECONDS";

/// The sponsor's grace for a renewal, bounded by the credential lifetime.
pub(crate) fn grace_seconds(credential_lifetime: u64) -> u64 {
    std::env::var_os(GRACE_ENV)
        .and_then(|value| {
            value
                .to_str()
                .and_then(|text| text.trim().parse::<u64>().ok())
        })
        .unwrap_or(DEFAULT_GRACE_SECONDS)
        .min(credential_lifetime)
}

/// What a node currently presents.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CredentialSummary {
    pub node: u64,
    pub principal: [u8; 16],
    pub issued_at: i64,
    pub expires_at: i64,
    pub certificate_fingerprint: [u8; 32],
    /// Renewals this process performed since it started.
    pub renewals: u64,
    /// The identity of the key the credential holds (24 §11), stable across
    /// renewals and changed by a rotation.
    pub key_identity: [u8; 32],
    /// Rotations this process performed since it started.
    pub rotations: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error, Serialize, Deserialize)]
pub enum RenewalError {
    #[error("the founder's identity is the bootstrap authority's own certificate")]
    Unsupported,
    #[error("the sponsor rejected the renewal: {0:?}")]
    Rejected(JoinFailure),
    #[error("the sponsor could not be reached or did not answer")]
    Unavailable,
    #[error("the renewed credential could not be installed or presented")]
    Install,
    #[error("the node's own credential material is inconsistent")]
    Identity,
    #[error("the node's controller is not running")]
    Stopped,
}

pub enum CredentialRequest {
    Renew(oneshot::Sender<Result<CredentialSummary, RenewalError>>),
    /// Rotate to a fresh key under the same identity (24 §11).
    Rotate(oneshot::Sender<Result<CredentialSummary, RenewalError>>),
    Current(oneshot::Sender<CredentialSummary>),
}
/// A bounded handle to the controller's credential state; every clone
/// shares one queue.
#[derive(Clone)]
pub struct CredentialHandle {
    sender: mpsc::Sender<CredentialRequest>,
}
impl CredentialHandle {
    pub fn channel(depth: usize) -> (Self, mpsc::Receiver<CredentialRequest>) {
        let (sender, receiver) = mpsc::channel(depth.max(1));
        (Self { sender }, receiver)
    }
    fn send(&self, request: CredentialRequest) -> Result<(), RenewalError> {
        self.sender
            .try_send(request)
            .map_err(|_| RenewalError::Stopped)
    }
    /// Renew now, whatever the expiry; answers once the renewed credential
    /// is installed and presented.
    pub async fn renew(&self) -> Result<CredentialSummary, RenewalError> {
        let (reply, receive) = oneshot::channel();
        self.send(CredentialRequest::Renew(reply))?;
        receive.await.map_err(|_| RenewalError::Stopped)?
    }
    pub async fn rotate(&self) -> Result<CredentialSummary, RenewalError> {
        let (reply, receive) = oneshot::channel();
        self.send(CredentialRequest::Rotate(reply))?;
        receive.await.map_err(|_| RenewalError::Stopped)?
    }
    pub async fn current(&self) -> Result<CredentialSummary, RenewalError> {
        let (reply, receive) = oneshot::channel();
        self.send(CredentialRequest::Current(reply))?;
        receive.await.map_err(|_| RenewalError::Stopped)
    }
}
/// The reply the local admin transport carries for a renewal.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum CredentialReply {
    Renewed(CredentialSummary),
    Failed(RenewalError),
}
#[cfg(test)]
#[path = "credential_renewal_tests.rs"]
mod tests;

/// Everything the controller swaps when a renewal lands. Without a listener
/// there is nothing to present and renewals are refused as `Install`.
pub struct CredentialSwap {
    pub listener: Option<ListenerIdentity>,
    pub placement: PlacementHandle,
    pub requests: mpsc::Receiver<CredentialRequest>,
}
impl CredentialSwap {
    /// A controller run without an endpoint of its own: nothing to present,
    /// nobody to ask; renewals are refused.
    pub fn detached() -> Self {
        let (placement, _jobs) = PlacementHandle::channel(1);
        let (_handle, requests) = CredentialHandle::channel(1);
        Self {
            listener: None,
            placement,
            requests,
        }
    }
}
