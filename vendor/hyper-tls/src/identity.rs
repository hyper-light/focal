use core::sync::atomic::{AtomicU64, Ordering};

/// Distinguishes one installation of a server certificate verifier, or of a client certificate
/// resolver, in a [`ClientConfig`] from every other in the process.
///
/// A client session may resume only under the verifier and credentials it was established under:
/// resumption skips server authentication (RFC 8446 §2.2), so it may only continue a session
/// authenticated under the same policy. Upstream rustls compared `Arc` pointers. A configuration
/// now owns its verifier and resolver, so each installation draws a fresh identity instead, and a
/// stored session records the identities it was made under.
///
/// [`ClientConfig`]: crate::ClientConfig
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Identity(u64);

impl Identity {
    /// An identity no other installation in this process has.
    pub(crate) fn fresh() -> Self {
        /// The next identity to hand out. Only uniqueness matters, so a relaxed read-modify-write
        /// suffices. A 64-bit counter does not wrap in practice: at one installation per
        /// nanosecond it would take 584 years.
        static NEXT: AtomicU64 = AtomicU64::new(0);
        Self(NEXT.fetch_add(1, Ordering::Relaxed))
    }
}
