//! Managed request identities are separate from legacy principal-wide epochs.
//! Registration/retirement authority belongs to the committed session owner.
use crate::digest::Digest;
use crate::durable_v1::Ref;
use crate::*;
use serde::{Deserialize, Serialize};

pub const MANAGED_REQUEST_SCHEMA: u16 = 1;

/// Slot zero is valid. Generation zero is a vacant slot marker only and can
/// never identify a registered stream or admitted mutation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RequestStreamIdentity {
    pub cluster: [u8; 16],
    pub ledger: LedgerId,
    pub principal: ParticipantId,
    pub slot: u32,
    pub generation: u64,
}
impl RequestStreamIdentity {
    pub fn is_valid(&self) -> bool {
        crate::semantics_v1::request_stream_valid(self)
    }
}

/// An ordinal has one ID, intent and family within its stream incarnation. The
/// independent request ID is not a client operation ID or JSON-RPC ID.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ManagedRequestKey {
    pub stream: RequestStreamIdentity,
    pub ordinal: u64,
    pub id: RequestId,
}
impl ManagedRequestKey {
    pub fn is_valid(&self) -> bool {
        crate::semantics_v1::request_key_valid(self)
    }
}

/// Trusted ingress supplies authority and verifies key.stream.principal against
/// the authenticated actor. No field is a substitute for that authentication.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ManagedAuthenticatedInput {
    pub key: ManagedRequestKey,
    pub expected_revision: Option<ObjectRevision>,
    pub authority: AuthorityContext,
    pub command: Command,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub enum ManagedRequestFamily {
    Domain,
    Cursor,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub enum ManagedReceiptOutcome {
    Domain(CommandResult),
    Cursor {
        revision: u64,
        floor: SessionSeq,
        record: Option<CursorRecordSnapshot>,
    },
    /// This fence is emitted only by a committed seal that observed no earlier
    /// outcome and prevents the exact ordinal from executing afterward.
    Sealed {
        family: ManagedRequestFamily,
    },
}

/// Exact retained managed outcome. Cursor and seal entries do not advance
/// SessionSeq, so the positive Raft index is part of the outcome identity.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ManagedReceipt {
    pub key: ManagedRequestKey,
    pub sequence: SessionSeq,
    pub raft_index: u64,
    pub intent_hash: ContentHash,
    pub outcome: ManagedReceiptOutcome,
}
impl ManagedReceipt {
    /// Stream the original outcome into its acknowledgment commitment. This
    /// excludes no fields and allocates no serialized receipt buffer.
    pub fn content_hash(&self) -> Result<ContentHash, CanonicalError> {
        managed_hash(b"focal.managed-receipt\0", &Ref(self))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ManagedReceiptAck {
    pub key: ManagedRequestKey,
    pub receipt_hash: ContentHash,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub enum RequestStreamCommand {
    Register {
        slot: u32,
        expected_generation: u64,
        /// Persisted registration/CAS identity, not a separate authorization
        /// credential. The authenticated principal owns all its streams.
        owner: RequestId,
        window: u32,
    },
    Acknowledge {
        stream: RequestStreamIdentity,
        expected_revision: u64,
        through: u64,
        receipts: Vec<ManagedReceiptAck>,
    },
    Seal {
        key: ManagedRequestKey,
        expected_revision: u64,
        family: ManagedRequestFamily,
        intent_hash: ContentHash,
    },
    Close {
        stream: RequestStreamIdentity,
        expected_revision: u64,
        issued_through: u64,
    },
}

/// Control identity is outside the ordinary ordinal window, so an acknowledgment
/// or seal can complete even when every ordinary request slot is occupied.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RequestStreamControlInput {
    pub cluster: [u8; 16],
    pub ledger: LedgerId,
    pub principal: ParticipantId,
    pub id: RequestId,
    pub command: RequestStreamCommand,
}
impl RequestStreamControlInput {
    pub fn intent_hash(&self) -> Result<ContentHash, CanonicalError> {
        request_stream_control_hash(self.cluster, self.ledger, self.principal, &self.command)
    }
}

/// Hash borrowed control intent without copying an acknowledgment manifest.
/// The independent control request ID is deliberately outside intent identity.
pub fn request_stream_control_hash(
    cluster: [u8; 16],
    ledger: LedgerId,
    principal: ParticipantId,
    command: &RequestStreamCommand,
) -> Result<ContentHash, CanonicalError> {
    managed_hash(
        b"focal.request-stream-control\0",
        // This four-field historical intent tuple excludes the independent
        // request ID. Every domain field uses its explicit V1 representation.
        &(Ref(&cluster), Ref(&ledger), Ref(&principal), Ref(command)),
    )
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub enum RequestStreamState {
    /// Generation is the last used value (zero initially). Registration compares
    /// against it and assigns checked generation + 1. Closing does not increment.
    Vacant { slot: u32, generation: u64 },
    Active {
        stream: RequestStreamIdentity,
        owner: RequestId,
        revision: u64,
        window: u32,
        acknowledged_through: u64,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub enum RequestStreamControlOutcome {
    Registered(RequestStreamState),
    Acknowledged {
        stream: RequestStreamIdentity,
        revision: u64,
        through: u64,
    },
    /// May contain an original domain/cursor outcome that preceded the seal.
    /// Only ManagedReceiptOutcome::Sealed certifies the negative decision.
    Sealed(Box<ManagedReceipt>),
    Closed {
        stream: RequestStreamIdentity,
        vacant_generation: u64,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RequestStreamControlReceipt {
    pub cluster: [u8; 16],
    pub ledger: LedgerId,
    pub principal: ParticipantId,
    pub id: RequestId,
    pub intent_hash: ContentHash,
    pub raft_index: u64,
    pub outcome: RequestStreamControlOutcome,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub enum RequestStreamQuery {
    Slot { slot: u32 },
    Receipt { key: ManagedRequestKey },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub enum ManagedReceiptResolution {
    Retained(Box<ManagedReceipt>),
    /// The outcome was acknowledged and may no longer be retained. No claim
    /// that this request never committed is implied by retirement.
    Retired {
        through: u64,
    },
    Unknown,
    /// Requests through this generation cannot execute again. A future,
    /// unregistered generation must remain Unknown rather than using this fence.
    StreamClosed {
        generation: u64,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub enum RequestStreamReadResult {
    Slot(RequestStreamState),
    Receipt {
        key: ManagedRequestKey,
        state: RequestStreamState,
        resolution: ManagedReceiptResolution,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RequestStreamRead {
    pub schema: u16,
    pub cluster: [u8; 16],
    pub ledger: LedgerId,
    pub principal: ParticipantId,
    pub sequence: SessionSeq,
    pub raft_index: u64,
    pub result: RequestStreamReadResult,
}

/// A replica advertises only its own installed managed decoder. The authenticated
/// transport binds node; the receiver checks the exact committed configuration.
/// Client-asserted aggregate support cannot activate a new persisted format.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ManagedFormatSupport {
    pub cluster: [u8; 16],
    pub ledger: LedgerId,
    pub group: [u8; 16],
    pub node: u64,
    pub configuration_index: u64,
    pub voters: Vec<u64>,
    pub voters_outgoing: Vec<u64>,
    pub learners: Vec<u64>,
    pub learners_next: Vec<u64>,
    pub auto_leave: bool,
    pub format_hash: ContentHash,
}

/// Domain intent identity keeps the legacy canonical command algorithm, while
/// deduplication is keyed by the actual managed scope rather than an alias.
pub fn managed_command_hash(
    input: &ManagedAuthenticatedInput,
) -> Result<ContentHash, CanonicalError> {
    managed_command_parts_hash(
        input.key.stream.ledger,
        input.key.stream.principal,
        &input.expected_revision,
        &input.command,
    )
}

/// Authenticated wire adapters can hash a borrowed authored command before
/// constructing trusted authority input, without cloning its payload.
pub fn managed_command_parts_hash(
    ledger: LedgerId,
    principal: ParticipantId,
    expected_revision: &Option<ObjectRevision>,
    command: &Command,
) -> Result<ContentHash, CanonicalError> {
    // Both request families share one frozen V1 identity representation/header.
    // Stream identity still belongs to deduplication, outside this intent hash.
    crate::canonical::command_parts_hash_streamed(ledger, principal, expected_revision, command)
}
fn managed_hash<T: Serialize + ?Sized>(
    domain: &[u8],
    value: &T,
) -> Result<ContentHash, CanonicalError> {
    let mut digest = Digest::new();
    digest.update(domain)?;
    // A retained V1 identity must not change if the current admission schema
    // advances. Future formats require their own explicitly selected commitment.
    const HASH_SCHEMA_V1: u16 = 1;
    digest.update(&HASH_SCHEMA_V1.to_be_bytes())?;
    Ok(postcard::serialize_with_flavor(value, digest)?)
}

#[cfg(test)]
#[path = "managed_tests.rs"]
mod tests;
