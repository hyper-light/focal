//! A node's own credential over time: the controller renews it before it
//! expires (or when the operator asks), installs the renewed receipt under
//! the same key, and presents the new certificate on every path at once.
//! The founder's node credential is one of these too: its controller asks
//! the enrollment host it runs itself (24 §11).
use crate::{network_listener::ListenerIdentity, placement_control::PlacementHandle};
use focal_enrollment::{EnrollmentReceipt, JoinFailure};
use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc, oneshot};

/// A credential is renewed once this fraction of its lifetime remains: a
/// third, the practice ACME clients follow (Let's Encrypt's integration
/// guide asks clients to renew when a third of the lifetime is left, so a
/// renewal that fails leaves two more windows' worth of lifetime before the
/// expiry). The lifetime is the cluster's committed policy
/// (`node.credential_lifetime_seconds` at genesis), read from the receipt
/// itself, so a short one is renewed at its own pace.
pub const RENEWAL_WINDOW_DIVISOR: i64 = 3;
/// A failed renewal is retried at most this many times across the window:
/// certbot's cadence, twice a day across its thirty-day window. The retry
/// interval scales with the window as the window does with the lifetime,
/// and is never under the second the registry decides in.
pub const RENEWAL_ATTEMPTS: i64 = 60;
/// How long before its expiry a credential is renewed: the last third of
/// the lifetime it was issued for.
pub fn renewal_window(receipt: &EnrollmentReceipt) -> i64 {
    window_of(receipt.issued_at, receipt.expires_at)
}
/// The renewal window of a certificate valid from `issued_at` to
/// `expires_at`: the last third of its lifetime.
pub fn window_of(issued_at: i64, expires_at: i64) -> i64 {
    expires_at
        .saturating_sub(issued_at)
        .max(0)
        .checked_div(RENEWAL_WINDOW_DIVISOR)
        .unwrap_or(0)
}
/// How soon a failed renewal is retried, for a window this long.
pub fn renewal_retry(window: i64) -> i64 {
    window.checked_div(RENEWAL_ATTEMPTS).unwrap_or(0).max(1)
}
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
/// An issuer as the admin reports it (24 §11): its fingerprint, validity,
/// whether its predecessor endorsed it, and when it was staged.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IssuerDigest {
    pub fingerprint: [u8; 32],
    pub issued_at: i64,
    pub expires_at: i64,
    pub endorsed: bool,
    pub staged_at: Option<i64>,
}
impl IssuerDigest {
    pub fn of(record: &focal_enrollment::IssuerRecord, staged_at: Option<i64>) -> Self {
        Self {
            fingerprint: record.fingerprint,
            issued_at: record.issued_at,
            expires_at: record.expires_at,
            endorsed: record.endorsement.is_some(),
            staged_at,
        }
    }
}
/// The issuers as committed (24 §11), with the fence the succession is
/// gated on (24 §21).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IssuerSummary {
    pub current: IssuerDigest,
    pub successor: Option<IssuerDigest>,
    pub retiring: Option<IssuerDigest>,
    pub fence_level: u32,
    pub succession_level: u32,
}
impl IssuerSummary {
    pub fn of(issuers: &focal_enrollment::IssuerSuccession, fence_level: u32) -> Self {
        Self {
            current: IssuerDigest::of(&issuers.current, None),
            successor: issuers
                .successor
                .as_ref()
                .map(|staged| IssuerDigest::of(&staged.record, Some(staged.staged_at))),
            retiring: issuers
                .retiring
                .as_ref()
                .map(|record| IssuerDigest::of(record, None)),
            fence_level,
            succession_level: crate::upgrade::ISSUER_SUCCESSION_LEVEL,
        }
    }
}
/// The reply the local admin transport carries for the issuers.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum IssuerReply {
    Issuers(Box<IssuerSummary>),
    /// The upgrade fence is below the level the succession needs.
    Fenced {
        level: u32,
        needed: u32,
    },
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
