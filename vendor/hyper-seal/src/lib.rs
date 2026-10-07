//! Sealing at rest (`docs/seal.md`): the one construction mantle, focal and slates keep a tenant's
//! bytes under on every device.
//!
//! - [`keys`]: random keys in a hierarchy, each wrapped by its parent with AES-256 key wrap, and the
//!   [`keys::KeySource`] a root key comes from (§3).
//! - [`stream`]: a file written once, sealed in segments by STREAM, each an AES-256-GCM seal at a
//!   nonce counting its segment and marking the last (§4).
//! - [`log`]: an appended log's records sealed one by one under a key per writer session, each at
//!   its offset, so no key ever seals two records at one offset, crash or not (§5).
//! - [`recipient`]: a key wrapped to another machine's ML-KEM-1024 key (§6).
//! - [`name`]: the name a store gives sealed content, keyed per tenant (§7).
//! - [`Secret32`]: a key's 32 bytes, wiped when dropped (§8).
//!
//! Nonces are never random: every key seals one sequence, and its nonce is the position in it (SP
//! 800-38D §8.2.1). Every call into AWS-LC runs behind an unwind boundary ([`guarded`]), so a panic
//! there is a typed error, never an abort.
#![cfg_attr(
    test,
    allow(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::panic,
        clippy::indexing_slicing,
        clippy::arithmetic_side_effects,
        clippy::disallowed_macros,
        clippy::cast_possible_truncation,
        clippy::cognitive_complexity,
        clippy::type_complexity
    )
)]

mod error;
mod file;
#[cfg(windows)]
mod file_windows;
pub mod keys;
pub mod log;
mod memory;
pub mod name;
pub mod recipient;
pub mod stream;

pub use error::SealError;
pub use file::FileSource;
pub use memory::{Secret32, keys_held, lock_keys};

use std::panic::{AssertUnwindSafe, catch_unwind};

/// GCM's tag, the full 128 bits (SP 800-38D §5.2.1.2).
pub const TAG: usize = 16;

/// Runs one call into AWS-LC; a panic in it is [`SealError::Unwound`], and its own failure the
/// `failed` error the caller names.
pub(crate) fn guarded<T, E>(
    failed: SealError,
    call: impl FnOnce() -> Result<T, E>,
) -> Result<T, SealError> {
    match catch_unwind(AssertUnwindSafe(call)) {
        Ok(Ok(value)) => Ok(value),
        Ok(Err(_)) => Err(failed),
        Err(_) => Err(SealError::Unwound),
    }
}

/// Whether this build runs AWS-LC's FIPS 140-3 module in its approved mode, so a node whose
/// configuration demands FIPS can refuse to start on a build that is not (§9). The consumer selects
/// the module by enabling aws-lc-rs's `fips` feature in its own build; features unify, so every
/// crate here then runs on it.
pub fn fips() -> bool {
    aws_lc_rs::try_fips_mode().is_ok()
}

/// 32 bytes from the operating system's random source, straight into a [`Secret32`]: `getrandom`
/// fails with an error where AWS-LC's own generator aborts (§3.4).
pub(crate) fn random_secret() -> Result<Secret32, SealError> {
    let mut secret = Secret32::zeroed()?;
    getrandom::fill(secret.bytes_mut()).map_err(|_| SealError::Random)?;
    Ok(secret)
}

/// `N` bytes from the operating system's random source, for IDs that are not secret.
pub(crate) fn random_bytes<const N: usize>() -> Result<[u8; N], SealError> {
    let mut out = [0u8; N];
    getrandom::fill(&mut out).map_err(|_| SealError::Random)?;
    Ok(out)
}

/// The 96-bit nonce SP 800-38D §8.2.1 builds from a fixed field and an invocation field: the fixed
/// field zero, since each key seals one sequence, and the invocation field `counter`.
pub(crate) fn counter_nonce(counter: u64) -> aws_lc_rs::aead::Nonce {
    let mut nonce = [0u8; 12];
    fill(&mut nonce, &[&[0; 4], &counter.to_be_bytes()]);
    aws_lc_rs::aead::Nonce::assume_unique_for_key(nonce)
}

/// Writes `parts`, one after another, into `out`: the layout of a fixed-size record. It never
/// panics; a layout whose parts do not add up to `out` is caught by the record's round-trip test.
pub(crate) fn fill(out: &mut [u8], parts: &[&[u8]]) {
    for (to, from) in out
        .iter_mut()
        .zip(parts.iter().flat_map(|part| part.iter()))
    {
        *to = *from;
    }
}
