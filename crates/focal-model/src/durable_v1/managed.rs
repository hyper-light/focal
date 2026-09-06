//! Frozen V1 field lists and enum ordinals. These borrowed writers and owned
//! decoders never delegate a domain value to its current Serde implementation.
use super::{Ref, V1, Value};
use crate::*;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

#[derive(Serialize)]
enum ManagedRequestFamilyRefV1 {
    Domain,
    Cursor,
}

#[derive(Deserialize)]
enum ManagedRequestFamilyValueV1 {
    Domain,
    Cursor,
}

impl V1 for ManagedRequestFamily {
    fn serialize_v1<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self {
            Self::Domain => ManagedRequestFamilyRefV1::Domain,
            Self::Cursor => ManagedRequestFamilyRefV1::Cursor,
        }
        .serialize(serializer)
    }
    fn deserialize_v1<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        Ok(
            match ManagedRequestFamilyValueV1::deserialize(deserializer)? {
                ManagedRequestFamilyValueV1::Domain => Self::Domain,
                ManagedRequestFamilyValueV1::Cursor => Self::Cursor,
            },
        )
    }
}

#[derive(Serialize)]
enum ManagedReceiptOutcomeRefV1<'a> {
    Domain(Ref<'a, CommandResult>),
    Cursor {
        revision: Ref<'a, u64>,
        floor: Ref<'a, SessionSeq>,
        record: Ref<'a, Option<CursorRecordSnapshot>>,
    },
    Sealed {
        family: Ref<'a, ManagedRequestFamily>,
    },
}

#[derive(Deserialize)]
enum ManagedReceiptOutcomeValueV1 {
    Domain(Value<CommandResult>),
    Cursor {
        revision: Value<u64>,
        floor: Value<SessionSeq>,
        record: Value<Option<CursorRecordSnapshot>>,
    },
    Sealed {
        family: Value<ManagedRequestFamily>,
    },
}

impl V1 for ManagedReceiptOutcome {
    fn serialize_v1<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self {
            Self::Domain(value0) => ManagedReceiptOutcomeRefV1::Domain(Ref(value0)),
            Self::Cursor {
                revision,
                floor,
                record,
            } => ManagedReceiptOutcomeRefV1::Cursor {
                revision: Ref(revision),
                floor: Ref(floor),
                record: Ref(record),
            },
            Self::Sealed { family } => ManagedReceiptOutcomeRefV1::Sealed {
                family: Ref(family),
            },
        }
        .serialize(serializer)
    }
    fn deserialize_v1<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        Ok(
            match ManagedReceiptOutcomeValueV1::deserialize(deserializer)? {
                ManagedReceiptOutcomeValueV1::Domain(value0) => Self::Domain(value0.0),
                ManagedReceiptOutcomeValueV1::Cursor {
                    revision,
                    floor,
                    record,
                } => Self::Cursor {
                    revision: revision.0,
                    floor: floor.0,
                    record: record.0,
                },
                ManagedReceiptOutcomeValueV1::Sealed { family } => {
                    Self::Sealed { family: family.0 }
                }
            },
        )
    }
}

v1_struct!(ManagedReceipt {
    key: ManagedRequestKey,
    sequence: SessionSeq,
    raft_index: u64,
    intent_hash: ContentHash,
    outcome: ManagedReceiptOutcome,
});

v1_struct!(ManagedReceiptAck {
    key: ManagedRequestKey,
    receipt_hash: ContentHash,
});

#[derive(Serialize)]
enum RequestStreamCommandRefV1<'a> {
    Register {
        slot: Ref<'a, u32>,
        expected_generation: Ref<'a, u64>,
        owner: Ref<'a, RequestId>,
        window: Ref<'a, u32>,
    },
    Acknowledge {
        stream: Ref<'a, RequestStreamIdentity>,
        expected_revision: Ref<'a, u64>,
        through: Ref<'a, u64>,
        receipts: Ref<'a, Vec<ManagedReceiptAck>>,
    },
    Seal {
        key: Ref<'a, ManagedRequestKey>,
        expected_revision: Ref<'a, u64>,
        family: Ref<'a, ManagedRequestFamily>,
        intent_hash: Ref<'a, ContentHash>,
    },
    Close {
        stream: Ref<'a, RequestStreamIdentity>,
        expected_revision: Ref<'a, u64>,
        issued_through: Ref<'a, u64>,
    },
}

#[derive(Deserialize)]
enum RequestStreamCommandValueV1 {
    Register {
        slot: Value<u32>,
        expected_generation: Value<u64>,
        owner: Value<RequestId>,
        window: Value<u32>,
    },
    Acknowledge {
        stream: Value<RequestStreamIdentity>,
        expected_revision: Value<u64>,
        through: Value<u64>,
        receipts: Value<Vec<ManagedReceiptAck>>,
    },
    Seal {
        key: Value<ManagedRequestKey>,
        expected_revision: Value<u64>,
        family: Value<ManagedRequestFamily>,
        intent_hash: Value<ContentHash>,
    },
    Close {
        stream: Value<RequestStreamIdentity>,
        expected_revision: Value<u64>,
        issued_through: Value<u64>,
    },
}

impl V1 for RequestStreamCommand {
    fn serialize_v1<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self {
            Self::Register {
                slot,
                expected_generation,
                owner,
                window,
            } => RequestStreamCommandRefV1::Register {
                slot: Ref(slot),
                expected_generation: Ref(expected_generation),
                owner: Ref(owner),
                window: Ref(window),
            },
            Self::Acknowledge {
                stream,
                expected_revision,
                through,
                receipts,
            } => RequestStreamCommandRefV1::Acknowledge {
                stream: Ref(stream),
                expected_revision: Ref(expected_revision),
                through: Ref(through),
                receipts: Ref(receipts),
            },
            Self::Seal {
                key,
                expected_revision,
                family,
                intent_hash,
            } => RequestStreamCommandRefV1::Seal {
                key: Ref(key),
                expected_revision: Ref(expected_revision),
                family: Ref(family),
                intent_hash: Ref(intent_hash),
            },
            Self::Close {
                stream,
                expected_revision,
                issued_through,
            } => RequestStreamCommandRefV1::Close {
                stream: Ref(stream),
                expected_revision: Ref(expected_revision),
                issued_through: Ref(issued_through),
            },
        }
        .serialize(serializer)
    }
    fn deserialize_v1<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        Ok(
            match RequestStreamCommandValueV1::deserialize(deserializer)? {
                RequestStreamCommandValueV1::Register {
                    slot,
                    expected_generation,
                    owner,
                    window,
                } => Self::Register {
                    slot: slot.0,
                    expected_generation: expected_generation.0,
                    owner: owner.0,
                    window: window.0,
                },
                RequestStreamCommandValueV1::Acknowledge {
                    stream,
                    expected_revision,
                    through,
                    receipts,
                } => Self::Acknowledge {
                    stream: stream.0,
                    expected_revision: expected_revision.0,
                    through: through.0,
                    receipts: receipts.0,
                },
                RequestStreamCommandValueV1::Seal {
                    key,
                    expected_revision,
                    family,
                    intent_hash,
                } => Self::Seal {
                    key: key.0,
                    expected_revision: expected_revision.0,
                    family: family.0,
                    intent_hash: intent_hash.0,
                },
                RequestStreamCommandValueV1::Close {
                    stream,
                    expected_revision,
                    issued_through,
                } => Self::Close {
                    stream: stream.0,
                    expected_revision: expected_revision.0,
                    issued_through: issued_through.0,
                },
            },
        )
    }
}

v1_struct!(RequestStreamControlInput {
    cluster: [u8; 16],
    ledger: LedgerId,
    principal: ParticipantId,
    id: RequestId,
    command: RequestStreamCommand,
});

#[derive(Serialize)]
enum RequestStreamStateRefV1<'a> {
    Vacant {
        slot: Ref<'a, u32>,
        generation: Ref<'a, u64>,
    },
    Active {
        stream: Ref<'a, RequestStreamIdentity>,
        owner: Ref<'a, RequestId>,
        revision: Ref<'a, u64>,
        window: Ref<'a, u32>,
        acknowledged_through: Ref<'a, u64>,
    },
}

#[derive(Deserialize)]
enum RequestStreamStateValueV1 {
    Vacant {
        slot: Value<u32>,
        generation: Value<u64>,
    },
    Active {
        stream: Value<RequestStreamIdentity>,
        owner: Value<RequestId>,
        revision: Value<u64>,
        window: Value<u32>,
        acknowledged_through: Value<u64>,
    },
}

impl V1 for RequestStreamState {
    fn serialize_v1<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self {
            Self::Vacant { slot, generation } => RequestStreamStateRefV1::Vacant {
                slot: Ref(slot),
                generation: Ref(generation),
            },
            Self::Active {
                stream,
                owner,
                revision,
                window,
                acknowledged_through,
            } => RequestStreamStateRefV1::Active {
                stream: Ref(stream),
                owner: Ref(owner),
                revision: Ref(revision),
                window: Ref(window),
                acknowledged_through: Ref(acknowledged_through),
            },
        }
        .serialize(serializer)
    }
    fn deserialize_v1<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        Ok(
            match RequestStreamStateValueV1::deserialize(deserializer)? {
                RequestStreamStateValueV1::Vacant { slot, generation } => Self::Vacant {
                    slot: slot.0,
                    generation: generation.0,
                },
                RequestStreamStateValueV1::Active {
                    stream,
                    owner,
                    revision,
                    window,
                    acknowledged_through,
                } => Self::Active {
                    stream: stream.0,
                    owner: owner.0,
                    revision: revision.0,
                    window: window.0,
                    acknowledged_through: acknowledged_through.0,
                },
            },
        )
    }
}

#[derive(Serialize)]
enum RequestStreamControlOutcomeRefV1<'a> {
    Registered(Ref<'a, RequestStreamState>),
    Acknowledged {
        stream: Ref<'a, RequestStreamIdentity>,
        revision: Ref<'a, u64>,
        through: Ref<'a, u64>,
    },
    Sealed(Ref<'a, Box<ManagedReceipt>>),
    Closed {
        stream: Ref<'a, RequestStreamIdentity>,
        vacant_generation: Ref<'a, u64>,
    },
}

#[derive(Deserialize)]
enum RequestStreamControlOutcomeValueV1 {
    Registered(Value<RequestStreamState>),
    Acknowledged {
        stream: Value<RequestStreamIdentity>,
        revision: Value<u64>,
        through: Value<u64>,
    },
    Sealed(Value<Box<ManagedReceipt>>),
    Closed {
        stream: Value<RequestStreamIdentity>,
        vacant_generation: Value<u64>,
    },
}

impl V1 for RequestStreamControlOutcome {
    fn serialize_v1<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self {
            Self::Registered(value0) => RequestStreamControlOutcomeRefV1::Registered(Ref(value0)),
            Self::Acknowledged {
                stream,
                revision,
                through,
            } => RequestStreamControlOutcomeRefV1::Acknowledged {
                stream: Ref(stream),
                revision: Ref(revision),
                through: Ref(through),
            },
            Self::Sealed(value0) => RequestStreamControlOutcomeRefV1::Sealed(Ref(value0)),
            Self::Closed {
                stream,
                vacant_generation,
            } => RequestStreamControlOutcomeRefV1::Closed {
                stream: Ref(stream),
                vacant_generation: Ref(vacant_generation),
            },
        }
        .serialize(serializer)
    }
    fn deserialize_v1<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        Ok(
            match RequestStreamControlOutcomeValueV1::deserialize(deserializer)? {
                RequestStreamControlOutcomeValueV1::Registered(value0) => {
                    Self::Registered(value0.0)
                }
                RequestStreamControlOutcomeValueV1::Acknowledged {
                    stream,
                    revision,
                    through,
                } => Self::Acknowledged {
                    stream: stream.0,
                    revision: revision.0,
                    through: through.0,
                },
                RequestStreamControlOutcomeValueV1::Sealed(value0) => Self::Sealed(value0.0),
                RequestStreamControlOutcomeValueV1::Closed {
                    stream,
                    vacant_generation,
                } => Self::Closed {
                    stream: stream.0,
                    vacant_generation: vacant_generation.0,
                },
            },
        )
    }
}

v1_struct!(RequestStreamControlReceipt {
    cluster: [u8; 16],
    ledger: LedgerId,
    principal: ParticipantId,
    id: RequestId,
    intent_hash: ContentHash,
    raft_index: u64,
    outcome: RequestStreamControlOutcome,
});

#[derive(Serialize)]
enum RequestStreamQueryRefV1<'a> {
    Slot { slot: Ref<'a, u32> },
    Receipt { key: Ref<'a, ManagedRequestKey> },
}

#[derive(Deserialize)]
enum RequestStreamQueryValueV1 {
    Slot { slot: Value<u32> },
    Receipt { key: Value<ManagedRequestKey> },
}

impl V1 for RequestStreamQuery {
    fn serialize_v1<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self {
            Self::Slot { slot } => RequestStreamQueryRefV1::Slot { slot: Ref(slot) },
            Self::Receipt { key } => RequestStreamQueryRefV1::Receipt { key: Ref(key) },
        }
        .serialize(serializer)
    }
    fn deserialize_v1<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        Ok(
            match RequestStreamQueryValueV1::deserialize(deserializer)? {
                RequestStreamQueryValueV1::Slot { slot } => Self::Slot { slot: slot.0 },
                RequestStreamQueryValueV1::Receipt { key } => Self::Receipt { key: key.0 },
            },
        )
    }
}

#[derive(Serialize)]
enum ManagedReceiptResolutionRefV1<'a> {
    Retained(Ref<'a, Box<ManagedReceipt>>),
    Retired { through: Ref<'a, u64> },
    Unknown,
    StreamClosed { generation: Ref<'a, u64> },
}

#[derive(Deserialize)]
enum ManagedReceiptResolutionValueV1 {
    Retained(Value<Box<ManagedReceipt>>),
    Retired { through: Value<u64> },
    Unknown,
    StreamClosed { generation: Value<u64> },
}

impl V1 for ManagedReceiptResolution {
    fn serialize_v1<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self {
            Self::Retained(value0) => ManagedReceiptResolutionRefV1::Retained(Ref(value0)),
            Self::Retired { through } => ManagedReceiptResolutionRefV1::Retired {
                through: Ref(through),
            },
            Self::Unknown => ManagedReceiptResolutionRefV1::Unknown,
            Self::StreamClosed { generation } => ManagedReceiptResolutionRefV1::StreamClosed {
                generation: Ref(generation),
            },
        }
        .serialize(serializer)
    }
    fn deserialize_v1<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        Ok(
            match ManagedReceiptResolutionValueV1::deserialize(deserializer)? {
                ManagedReceiptResolutionValueV1::Retained(value0) => Self::Retained(value0.0),
                ManagedReceiptResolutionValueV1::Retired { through } => {
                    Self::Retired { through: through.0 }
                }
                ManagedReceiptResolutionValueV1::Unknown => Self::Unknown,
                ManagedReceiptResolutionValueV1::StreamClosed { generation } => {
                    Self::StreamClosed {
                        generation: generation.0,
                    }
                }
            },
        )
    }
}

#[derive(Serialize)]
enum RequestStreamReadResultRefV1<'a> {
    Slot(Ref<'a, RequestStreamState>),
    Receipt {
        key: Ref<'a, ManagedRequestKey>,
        state: Ref<'a, RequestStreamState>,
        resolution: Ref<'a, ManagedReceiptResolution>,
    },
}

#[derive(Deserialize)]
enum RequestStreamReadResultValueV1 {
    Slot(Value<RequestStreamState>),
    Receipt {
        key: Value<ManagedRequestKey>,
        state: Value<RequestStreamState>,
        resolution: Value<ManagedReceiptResolution>,
    },
}

impl V1 for RequestStreamReadResult {
    fn serialize_v1<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self {
            Self::Slot(value0) => RequestStreamReadResultRefV1::Slot(Ref(value0)),
            Self::Receipt {
                key,
                state,
                resolution,
            } => RequestStreamReadResultRefV1::Receipt {
                key: Ref(key),
                state: Ref(state),
                resolution: Ref(resolution),
            },
        }
        .serialize(serializer)
    }
    fn deserialize_v1<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        Ok(
            match RequestStreamReadResultValueV1::deserialize(deserializer)? {
                RequestStreamReadResultValueV1::Slot(value0) => Self::Slot(value0.0),
                RequestStreamReadResultValueV1::Receipt {
                    key,
                    state,
                    resolution,
                } => Self::Receipt {
                    key: key.0,
                    state: state.0,
                    resolution: resolution.0,
                },
            },
        )
    }
}

v1_struct!(RequestStreamRead {
    schema: u16,
    cluster: [u8; 16],
    ledger: LedgerId,
    principal: ParticipantId,
    sequence: SessionSeq,
    raft_index: u64,
    result: RequestStreamReadResult,
});

v1_struct!(ManagedFormatSupport {
    cluster: [u8; 16],
    ledger: LedgerId,
    group: [u8; 16],
    node: u64,
    configuration_index: u64,
    voters: Vec<u64>,
    voters_outgoing: Vec<u64>,
    learners: Vec<u64>,
    learners_next: Vec<u64>,
    auto_leave: bool,
    format_hash: ContentHash,
});

v1_struct!(CursorMutationReceipt {
    ledger: LedgerId,
    key: RequestKey,
    intent_hash: ContentHash,
    revision: u64,
    domain_sequence: SessionSeq,
    raft_index: u64,
    floor: SessionSeq,
    record: Option<CursorRecordSnapshot>,
});

v1_struct!(CursorRecordSnapshot {
    token: CursorTokenSnapshot,
    filter: CursorFilterSnapshot,
    expires_at: u64,
    mode: CursorModeSnapshot,
});

v1_struct!(CursorTokenSnapshot {
    key: CursorConsumerKeySnapshot,
    generation: u64,
    scope: ContentHash,
    position: CursorPositionSnapshot,
});

v1_struct!(CursorConsumerKeySnapshot {
    ledger: LedgerId,
    consumer: [u8; 16],
});

v1_struct!(CursorPositionSnapshot {
    ledger: LedgerId,
    sequence: SessionSeq,
    offset: CursorPositionOffsetSnapshot,
});

#[derive(Serialize)]
enum CursorPositionOffsetSnapshotRefV1<'a> {
    Delta(Ref<'a, u32>),
    Resolved,
}

#[derive(Deserialize)]
enum CursorPositionOffsetSnapshotValueV1 {
    Delta(Value<u32>),
    Resolved,
}

impl V1 for CursorPositionOffsetSnapshot {
    fn serialize_v1<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self {
            Self::Delta(value0) => CursorPositionOffsetSnapshotRefV1::Delta(Ref(value0)),
            Self::Resolved => CursorPositionOffsetSnapshotRefV1::Resolved,
        }
        .serialize(serializer)
    }
    fn deserialize_v1<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        Ok(
            match CursorPositionOffsetSnapshotValueV1::deserialize(deserializer)? {
                CursorPositionOffsetSnapshotValueV1::Delta(value0) => Self::Delta(value0.0),
                CursorPositionOffsetSnapshotValueV1::Resolved => Self::Resolved,
            },
        )
    }
}

#[derive(Serialize)]
enum CursorFilterSnapshotRefV1<'a> {
    All,
    Claims(Ref<'a, Vec<ClaimId>>),
}

#[derive(Deserialize)]
enum CursorFilterSnapshotValueV1 {
    All,
    Claims(Value<Vec<ClaimId>>),
}

impl V1 for CursorFilterSnapshot {
    fn serialize_v1<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self {
            Self::All => CursorFilterSnapshotRefV1::All,
            Self::Claims(value0) => CursorFilterSnapshotRefV1::Claims(Ref(value0)),
        }
        .serialize(serializer)
    }
    fn deserialize_v1<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        Ok(
            match CursorFilterSnapshotValueV1::deserialize(deserializer)? {
                CursorFilterSnapshotValueV1::All => Self::All,
                CursorFilterSnapshotValueV1::Claims(value0) => Self::Claims(value0.0),
            },
        )
    }
}

#[derive(Serialize)]
enum CursorModeSnapshotRefV1<'a> {
    Live,
    Seeding { snapshot: Ref<'a, SessionSeq> },
    Resync { reason: Ref<'a, CursorResyncReason> },
    Protected,
}

#[derive(Deserialize)]
enum CursorModeSnapshotValueV1 {
    Live,
    Seeding { snapshot: Value<SessionSeq> },
    Resync { reason: Value<CursorResyncReason> },
    Protected,
}

impl V1 for CursorModeSnapshot {
    fn serialize_v1<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self {
            Self::Live => CursorModeSnapshotRefV1::Live,
            Self::Seeding { snapshot } => CursorModeSnapshotRefV1::Seeding {
                snapshot: Ref(snapshot),
            },
            Self::Resync { reason } => CursorModeSnapshotRefV1::Resync {
                reason: Ref(reason),
            },
            Self::Protected => CursorModeSnapshotRefV1::Protected,
        }
        .serialize(serializer)
    }
    fn deserialize_v1<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        Ok(
            match CursorModeSnapshotValueV1::deserialize(deserializer)? {
                CursorModeSnapshotValueV1::Live => Self::Live,
                CursorModeSnapshotValueV1::Seeding { snapshot } => Self::Seeding {
                    snapshot: snapshot.0,
                },
                CursorModeSnapshotValueV1::Resync { reason } => Self::Resync { reason: reason.0 },
                CursorModeSnapshotValueV1::Protected => Self::Protected,
            },
        )
    }
}

#[derive(Serialize)]
enum CursorResyncReasonRefV1 {
    HistoryExpired,
    LeaseExpired,
    SlowConsumer,
    SnapshotExpired,
    ExplicitReset,
}

#[derive(Deserialize)]
enum CursorResyncReasonValueV1 {
    HistoryExpired,
    LeaseExpired,
    SlowConsumer,
    SnapshotExpired,
    ExplicitReset,
}

impl V1 for CursorResyncReason {
    fn serialize_v1<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self {
            Self::HistoryExpired => CursorResyncReasonRefV1::HistoryExpired,
            Self::LeaseExpired => CursorResyncReasonRefV1::LeaseExpired,
            Self::SlowConsumer => CursorResyncReasonRefV1::SlowConsumer,
            Self::SnapshotExpired => CursorResyncReasonRefV1::SnapshotExpired,
            Self::ExplicitReset => CursorResyncReasonRefV1::ExplicitReset,
        }
        .serialize(serializer)
    }
    fn deserialize_v1<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        Ok(
            match CursorResyncReasonValueV1::deserialize(deserializer)? {
                CursorResyncReasonValueV1::HistoryExpired => Self::HistoryExpired,
                CursorResyncReasonValueV1::LeaseExpired => Self::LeaseExpired,
                CursorResyncReasonValueV1::SlowConsumer => Self::SlowConsumer,
                CursorResyncReasonValueV1::SnapshotExpired => Self::SnapshotExpired,
                CursorResyncReasonValueV1::ExplicitReset => Self::ExplicitReset,
            },
        )
    }
}
