/// Why a seal, an open, a wrap or an unwrap failed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum SealError {
    /// The operating system's random source failed.
    #[error("the operating system's random source failed")]
    Random,
    /// Another key wrapped it, or its bytes changed: key wrap's integrity check failed (SP 800-38F
    /// App. A.3).
    #[error("a key does not unwrap under this key")]
    Unwrap,
    /// Its bytes, its position or its file are not the ones sealed, or another key sealed it.
    #[error("sealed bytes do not open")]
    Open,
    /// Framing whose MAC fails: bytes changed by someone who could recompute their CRC (§5.1). A
    /// consumer reports it as tampering, never as a torn write.
    #[error("framing fails its MAC: tampered")]
    Tampered,
    /// A seal AWS-LC refused.
    #[error("a seal failed")]
    Seal,
    /// A size outside what a construction states: a segment, a file of more segments than a nonce
    /// counts, a record too long for one seal, a buffer shorter than its tag.
    #[error("a size outside the construction's bounds")]
    Size,
    /// A key record, file header or recipient record that does not parse.
    #[error("a malformed key record or header")]
    Malformed,
    /// A key past its originator-usage period asked to wrap (SP 800-57 §5.3.6).
    #[error("a key past its usage period asked to wrap")]
    Expired,
    /// A key source refused, with its own reason.
    #[error("the key source refused: {0}")]
    Source(&'static str),
    /// No slot for a key: the locked region was not made ([`crate::lock_keys`]), or every slot of
    /// its stated count is held.
    #[error("no slot for a key in the locked region")]
    Capacity,
    /// The process's key region was made already, for fewer keys than asked: its count.
    #[error("the key region was made already, for {slots} keys")]
    AlreadyLocked {
        /// The slots the region has.
        slots: usize,
    },
    /// The OS would not lock the region: the process's locked-memory limit.
    #[error("the OS would not lock the key region")]
    Lock,
    /// AWS-LC unwound; caught at the boundary.
    #[error("the cryptographic library unwound")]
    Unwound,
}
