//! Client request/response tracing for offline history checking (R11 §1).
//!
//! A [`TraceSink`] records one [`TraceEntry`] per request/response exchange the
//! client completes. The trace is the *client's* view of an operation: when the
//! request left the client, when its outcome became known, and what that outcome
//! was. Merged across the clients of a run and ordered by their single-host
//! timestamps, these entries are the `Invoke`/`Complete` interval events the
//! `focal-sim` linearizability checker consumes; the matching `Publish` events
//! come from an offline replay of the server WAL, so the checker itself needs no
//! change.
//!
//! The client authenticates as one principal at the transport, so the principal
//! is not carried on the wire envelope ([`RequestEnvelope`](focal_wire::RequestEnvelope)
//! has none). An entry therefore records the envelope identity the client does
//! know — `ledger`, `request_epoch`, `request_id` — and a consumer that needs a
//! full [`RequestKey`](focal_model::RequestKey) supplies the principal it
//! authenticated this client as. A committed mutation additionally reports the
//! authority's `sequence` and `command_hash` from the receipt, which pin the
//! effect independently of the client's identity.
//!
//! Tracing is opt-in and allocation-free when absent: a [`Client`](crate::Client)
//! with no sink does no per-request trace work at all.

use focal_model::{ContentHash, LedgerId, RequestEpoch, RequestId, SessionSeq};

/// One completed client request/response exchange, from the client's vantage.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct TraceEntry {
    /// Per-client monotone call number. Orders this client's calls and pairs an
    /// invocation with its completion when several are in flight at once.
    pub call: u64,
    /// Single-host wall-clock nanoseconds when the request left the client
    /// (`Invoke`). Cross-host linearizability is out of scope for this trace.
    pub invoked_nanos: u128,
    /// Single-host wall-clock nanoseconds when the outcome became known to the
    /// client (`Complete`).
    pub completed_nanos: u128,
    pub ledger: LedgerId,
    pub request_epoch: RequestEpoch,
    pub request_id: RequestId,
    /// Whether the operation mutates. A mutation with an [`TraceOutcome::Unknown`]
    /// outcome may still have committed; a read never does.
    pub mutation: bool,
    pub outcome: TraceOutcome,
}

/// What the client observed for a request. Mirrors the checker's `Outcome`
/// without depending on it: `Committed`/`Read`/`Refused` are definite;
/// `Unknown` means the client cannot know whether a mutation took effect.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum TraceOutcome {
    /// A committed mutation: the authority's assigned sequence and the command
    /// hash it recorded (the private native intent for native mutations).
    Committed {
        sequence: SessionSeq,
        command_hash: ContentHash,
    },
    /// A read that completed at a committed prefix, identified by its sequence.
    Read { sequence: SessionSeq },
    /// The authority refused the request; nothing was admitted.
    Refused,
    /// The client cannot establish whether the request committed (lost reply,
    /// transport failure after bytes may have reached the authority).
    Unknown,
}

/// A destination the client records each completed exchange to.
///
/// The client calls [`record`](TraceSink::record) inline on the request path
/// after the outcome is known, so an implementation must be cheap and must not
/// block or panic; it must also tolerate concurrent calls from a shared client.
pub trait TraceSink: Send + Sync {
    fn record(&self, entry: TraceEntry);
}
