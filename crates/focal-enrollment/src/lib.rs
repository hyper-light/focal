#![cfg_attr(
    test,
    allow(
        clippy::panic,
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::unreachable,
        clippy::indexing_slicing,
        clippy::arithmetic_side_effects,
        clippy::disallowed_macros
    )
)]
//! Durable, single-use enrollment with client-owned keys and constrained X.509 identity.
//! Network adapters require a Tokio runtime with enabled IO and time drivers;
//! absent runtime context returns a typed transport error before socket work.
mod files;
mod founding;
mod invitation;
mod journal;
mod pki;
mod registry;
mod statements;
mod transport;
pub use founding::*;
pub use invitation::*;
pub use journal::*;
pub use pki::*;
pub use registry::*;
pub use statements::*;
pub use transport::*;

pub type ClusterId = [u8; 16];
pub type InvitationId = [u8; 16];
pub type JoinId = [u8; 16];
pub type Fingerprint = [u8; 32];
pub const ENROLLMENT_ALPN: &[u8] = b"focal-enroll/1";
pub const MAX_MESSAGE_BYTES: usize = 16 * 1024;

#[derive(Debug, thiserror::Error)]
pub enum EnrollmentError {
    #[error("enrollment input or configuration is invalid")]
    Invalid,
    #[error("enrollment resource limit exceeded")]
    Capacity,
    #[error("enrollment authority or invitation does not match this cluster")]
    WrongCluster,
    #[error("invitation or identity authentication failed")]
    Unauthorized,
    #[error("invitation or credential has expired")]
    Expired,
    #[error("invitation or enrollment is revoked")]
    Revoked,
    #[error("invitation is already bound to another join identity")]
    Used,
    #[error("metadata revision changed; prepare again")]
    Conflict,
    #[error("enrollment metadata has not committed")]
    NotCommitted,
    #[error("private credential directory is already locked")]
    Locked,
    #[error("private credential filesystem permissions are unsafe")]
    Permissions,
    #[error("persisted enrollment state is corrupt")]
    Corrupt,
    #[error("cryptographic operation failed")]
    Crypto,
    #[error("private credential persistence failed")]
    Io(#[source] std::io::Error),
}
impl From<std::io::Error> for EnrollmentError {
    fn from(error: std::io::Error) -> Self {
        Self::Io(error)
    }
}
impl From<rcgen::Error> for EnrollmentError {
    fn from(_: rcgen::Error) -> Self {
        Self::Crypto
    }
}
impl From<postcard::Error> for EnrollmentError {
    fn from(_: postcard::Error) -> Self {
        Self::Invalid
    }
}

fn random<const N: usize>() -> Result<[u8; N], EnrollmentError> {
    let mut bytes = [0; N];
    getrandom::fill(&mut bytes).map_err(|_| EnrollmentError::Crypto)?;
    Ok(bytes)
}
fn hash(domain: &'static str, bytes: &[u8]) -> Fingerprint {
    blake3::derive_key(domain, bytes)
}
fn encode<T: serde::Serialize>(value: &T) -> Result<Vec<u8>, EnrollmentError> {
    let mut bytes = vec![0; MAX_MESSAGE_BYTES];
    let used = postcard::to_slice(value, &mut bytes)
        .map_err(|_| EnrollmentError::Capacity)?
        .len();
    bytes.truncate(used);
    Ok(bytes)
}
fn decode<T: serde::de::DeserializeOwned>(bytes: &[u8]) -> Result<T, EnrollmentError> {
    if bytes.len() > MAX_MESSAGE_BYTES {
        return Err(EnrollmentError::Capacity);
    }
    let (value, rest) = postcard::take_from_bytes(bytes)?;
    if !rest.is_empty() {
        return Err(EnrollmentError::Invalid);
    }
    Ok(value)
}
fn hex(bytes: &[u8]) -> String {
    fn digit(nibble: u8) -> char {
        char::from(if nibble < 10 {
            b'0'.saturating_add(nibble)
        } else {
            b'a'.saturating_add(nibble.saturating_sub(10))
        })
    }
    let mut text = String::new();
    for byte in bytes {
        text.push(digit(byte >> 4));
        text.push(digit(byte & 15));
    }
    text
}

#[cfg(test)]
mod tests;
