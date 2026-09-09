//! Original Session storage representation. These adapters never delegate a
//! domain field to its live Serde implementation. Consensus types have explicit
//! local adapters, avoiding a dependency from consensus back to the domain model.
//! The owner reserves recovery/output memory before invoking these codecs.
use super::*;
use crate::request_streams::StreamSlotData;
use focal_model::durable_v1::{Ref as Frozen, V1, Value as Decoded};
use serde::ser::SerializeSeq;
use serde::{Deserializer, Serializer};

macro_rules! record {
    ($name:ident { $($field:ident: $ty:ty),+ $(,)? }) => {
        impl V1 for $name {
            fn serialize_v1<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
                #[derive(Serialize)]
                struct Fields<'a> { $($field: Frozen<'a, $ty>),+ }
                let Self { $($field),+ } = self;
                Fields { $($field: Frozen($field)),+ }.serialize(serializer)
            }
            fn deserialize_v1<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
                #[derive(Deserialize)]
                struct Fields { $($field: Decoded<$ty>),+ }
                let Fields { $($field),+ } = Fields::deserialize(deserializer)?;
                Ok(Self { $($field: $field.0),+ })
            }
        }
    };
}

struct ConfigurationRef<'a>(&'a MembershipConfiguration);
struct ConfigurationValue(MembershipConfiguration);
impl Serialize for ConfigurationRef<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        #[derive(Serialize)]
        struct Fields<'a> {
            voters: Frozen<'a, Vec<u64>>,
            learners: Frozen<'a, Vec<u64>>,
            voters_outgoing: Frozen<'a, Vec<u64>>,
            learners_next: Frozen<'a, Vec<u64>>,
            auto_leave: Frozen<'a, bool>,
        }
        let MembershipConfiguration {
            voters,
            learners,
            voters_outgoing,
            learners_next,
            auto_leave,
        } = self.0;
        Fields {
            voters: Frozen(voters),
            learners: Frozen(learners),
            voters_outgoing: Frozen(voters_outgoing),
            learners_next: Frozen(learners_next),
            auto_leave: Frozen(auto_leave),
        }
        .serialize(serializer)
    }
}
impl<'de> Deserialize<'de> for ConfigurationValue {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        struct Fields {
            voters: Decoded<Vec<u64>>,
            learners: Decoded<Vec<u64>>,
            voters_outgoing: Decoded<Vec<u64>>,
            learners_next: Decoded<Vec<u64>>,
            auto_leave: Decoded<bool>,
        }
        let Fields {
            voters,
            learners,
            voters_outgoing,
            learners_next,
            auto_leave,
        } = Fields::deserialize(deserializer)?;
        Ok(Self(MembershipConfiguration {
            voters: voters.0,
            learners: learners.0,
            voters_outgoing: voters_outgoing.0,
            learners_next: learners_next.0,
            auto_leave: auto_leave.0,
        }))
    }
}
struct ChangeRef<'a>(&'a MembershipChange);
struct ChangeValue(MembershipChange);
impl Serialize for ChangeRef<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        #[derive(Serialize)]
        enum Fields {
            AddLearner { node: u64 },
            Promote { node: u64 },
            Remove { node: u64 },
            LeaveJoint,
        }
        match *self.0 {
            MembershipChange::AddLearner { node } => Fields::AddLearner { node },
            MembershipChange::Promote { node } => Fields::Promote { node },
            MembershipChange::Remove { node } => Fields::Remove { node },
            MembershipChange::LeaveJoint => Fields::LeaveJoint,
        }
        .serialize(serializer)
    }
}
impl<'de> Deserialize<'de> for ChangeValue {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        enum Fields {
            AddLearner { node: u64 },
            Promote { node: u64 },
            Remove { node: u64 },
            LeaveJoint,
        }
        Ok(Self(match Fields::deserialize(deserializer)? {
            Fields::AddLearner { node } => MembershipChange::AddLearner { node },
            Fields::Promote { node } => MembershipChange::Promote { node },
            Fields::Remove { node } => MembershipChange::Remove { node },
            Fields::LeaveJoint => MembershipChange::LeaveJoint,
        }))
    }
}

record!(SnapshotEnvelope {
    schema: u16,
    ledger: LedgerId,
    raft_index: u64,
    core: Vec<u8>,
});

record!(CursorInput {
    ledger: LedgerId,
    key: RequestKey,
    intent_hash: ContentHash,
    command: CursorCommand,
});

record!(CursorReceipt {
    ledger: LedgerId,
    key: RequestKey,
    intent_hash: ContentHash,
    revision: u64,
    domain_sequence: SessionSeq,
    raft_index: u64,
    floor: SessionSeq,
    record: Option<CursorRecord>,
});

record!(CursorMetadata {
    receipts: BTreeMap<RequestKey, CursorReceipt>,
    owners: BTreeMap<ConsumerId, ParticipantId>,
});

record!(CursorEnvelope {
    schema: u16,
    ledger: LedgerId,
    domain_sequence: SessionSeq,
    replay_floor: SessionSeq,
    trusted_control: bool,
    input: CursorInput,
});

record!(LegacyCursorEnvelope {
    schema: u16,
    ledger: LedgerId,
    domain_sequence: SessionSeq,
    trusted_control: bool,
    input: CursorInput,
});

record!(SnapshotEnvelopeV2 {
    schema: u16,
    ledger: LedgerId,
    raft_index: u64,
    core: Vec<u8>,
    cursors: CursorCheckpoint,
    cursor_meta: CursorMetadata,
    delta_floor: SessionSeq,
    deltas: Vec<Delta>,
});

record!(SnapshotEnvelopeV3 {
    state: SnapshotEnvelopeV2,
    membership: MembershipState,
});

record!(MaintenanceEnvelope {
    schema: u16,
    ledger: LedgerId,
    domain_sequence: SessionSeq,
    replay_floor: SessionSeq,
    command: CursorCommand,
});

impl V1 for SessionMembershipRequest {
    fn serialize_v1<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        #[derive(Serialize)]
        struct Fields<'a> {
            id: Frozen<'a, [u8; 16]>,
            expected_index: Frozen<'a, u64>,
            expected: ConfigurationRef<'a>,
            change: ChangeRef<'a>,
        }
        let Self {
            id,
            expected_index,
            expected,
            change,
        } = self;
        Fields {
            id: Frozen(id),
            expected_index: Frozen(expected_index),
            expected: ConfigurationRef(expected),
            change: ChangeRef(change),
        }
        .serialize(serializer)
    }
    fn deserialize_v1<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        struct Fields {
            id: Decoded<[u8; 16]>,
            expected_index: Decoded<u64>,
            expected: ConfigurationValue,
            change: ChangeValue,
        }
        let Fields {
            id,
            expected_index,
            expected,
            change,
        } = Fields::deserialize(deserializer)?;
        Ok(Self {
            id: id.0,
            expected_index: expected_index.0,
            expected: expected.0,
            change: change.0,
        })
    }
}

impl V1 for SessionMembershipReceipt {
    fn serialize_v1<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        #[derive(Serialize)]
        struct Fields<'a> {
            id: Frozen<'a, [u8; 16]>,
            request_hash: Frozen<'a, [u8; 32]>,
            index: Frozen<'a, u64>,
            term: Frozen<'a, u64>,
            configuration: ConfigurationRef<'a>,
        }
        let Self {
            id,
            request_hash,
            index,
            term,
            configuration,
        } = self;
        Fields {
            id: Frozen(id),
            request_hash: Frozen(request_hash),
            index: Frozen(index),
            term: Frozen(term),
            configuration: ConfigurationRef(configuration),
        }
        .serialize(serializer)
    }
    fn deserialize_v1<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        struct Fields {
            id: Decoded<[u8; 16]>,
            request_hash: Decoded<[u8; 32]>,
            index: Decoded<u64>,
            term: Decoded<u64>,
            configuration: ConfigurationValue,
        }
        let Fields {
            id,
            request_hash,
            index,
            term,
            configuration,
        } = Fields::deserialize(deserializer)?;
        Ok(Self {
            id: id.0,
            request_hash: request_hash.0,
            index: index.0,
            term: term.0,
            configuration: configuration.0,
        })
    }
}

record!(MembershipState {
    configuration_index: u64,
    latest: Option<SessionMembershipReceipt>,
});

impl V1 for MembershipContext {
    fn serialize_v1<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        #[derive(Serialize)]
        struct Fields<'a> {
            ledger: Frozen<'a, LedgerId>,
            id: Frozen<'a, [u8; 16]>,
            expected_index: Frozen<'a, u64>,
            change: ChangeRef<'a>,
            hash: Frozen<'a, [u8; 32]>,
        }
        let Self {
            ledger,
            id,
            expected_index,
            change,
            hash,
        } = self;
        Fields {
            ledger: Frozen(ledger),
            id: Frozen(id),
            expected_index: Frozen(expected_index),
            change: ChangeRef(change),
            hash: Frozen(hash),
        }
        .serialize(serializer)
    }
    fn deserialize_v1<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        struct Fields {
            ledger: Decoded<LedgerId>,
            id: Decoded<[u8; 16]>,
            expected_index: Decoded<u64>,
            change: ChangeValue,
            hash: Decoded<[u8; 32]>,
        }
        let Fields {
            ledger,
            id,
            expected_index,
            change,
            hash,
        } = Fields::deserialize(deserializer)?;
        Ok(Self {
            ledger: ledger.0,
            id: id.0,
            expected_index: expected_index.0,
            change: change.0,
            hash: hash.0,
        })
    }
}

record!(SessionPlacementRequest {
    expected_index: u64,
    expected_configuration_index: u64,
    operation: OperationId,
    kind: SessionFenceKind,
    from_route: RouteEpoch,
    to_route: RouteEpoch,
    membership_epoch: u64,
    placement_epoch: u64,
    placement: PlacementSpec,
});

impl V1 for PlacementRecord {
    fn serialize_v1<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        #[derive(Serialize)]
        struct Fields<'a> {
            schema: Frozen<'a, u16>,
            ledger: Frozen<'a, LedgerId>,
            group: Frozen<'a, LogGroupId>,
            genesis: Frozen<'a, ContentHash>,
            sequence: Frozen<'a, SessionSeq>,
            configuration: ConfigurationRef<'a>,
            request: Frozen<'a, SessionPlacementRequest>,
        }
        let Self {
            schema,
            ledger,
            group,
            genesis,
            sequence,
            configuration,
            request,
        } = self;
        Fields {
            schema: Frozen(schema),
            ledger: Frozen(ledger),
            group: Frozen(group),
            genesis: Frozen(genesis),
            sequence: Frozen(sequence),
            configuration: ConfigurationRef(configuration),
            request: Frozen(request),
        }
        .serialize(serializer)
    }
    fn deserialize_v1<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        struct Fields {
            schema: Decoded<u16>,
            ledger: Decoded<LedgerId>,
            group: Decoded<LogGroupId>,
            genesis: Decoded<ContentHash>,
            sequence: Decoded<SessionSeq>,
            configuration: ConfigurationValue,
            request: Decoded<SessionPlacementRequest>,
        }
        let Fields {
            schema,
            ledger,
            group,
            genesis,
            sequence,
            configuration,
            request,
        } = Fields::deserialize(deserializer)?;
        Ok(Self {
            schema: schema.0,
            ledger: ledger.0,
            group: group.0,
            genesis: genesis.0,
            sequence: sequence.0,
            configuration: configuration.0,
            request: request.0,
        })
    }
}

record!(StoredPlacement {
    record: PlacementRecord,
    fence: SessionFence,
});

record!(PlacementState {
    active: Option<StoredPlacement>,
    cutover: Option<StoredPlacement>,
});

record!(SnapshotEnvelopeV4 {
    state: SnapshotEnvelopeV3,
    placement: PlacementState,
});

record!(ManagedCursorInput {
    key: ManagedRequestKey,
    intent_hash: ContentHash,
    command: CursorCommand,
});

record!(ManagedCursorEnvelope {
    schema: u16,
    domain_sequence: SessionSeq,
    replay_floor: SessionSeq,
    trusted_control: bool,
    input: ManagedCursorInput,
});

record!(RequestStreamEnvelope {
    schema: u16,
    domain_sequence: SessionSeq,
    input: RequestStreamControlInput,
});

record!(SnapshotEnvelopeV5 {
    state: SnapshotEnvelopeV4,
    requests: RequestStreamsCheckpoint,
});

record!(SnapshotEnvelopeV6 {
    state: SnapshotEnvelopeV4,
    requests: RequestStreamsCheckpoint,
    activation: Vec<u8>,
    native: Vec<u8>,
});

// `FOCALSS7`: SS6 plus the request-stream registry's generation watermark.
record!(SnapshotEnvelopeV7 {
    state: SnapshotEnvelopeV4,
    requests: RequestStreamsCheckpoint,
    activation: Vec<u8>,
    native: Vec<u8>,
    slot_generation: u64,
});

record!(StreamSlotData {
    principal: ParticipantId,
    state: RequestStreamState,
    latest: Option<RequestStreamControlReceipt>,
    rows: Vec<ManagedReceipt>,
});

record!(RequestStreamsCheckpoint {
    activated: bool,
    slots: Vec<StreamSlotData>,
});

/// Decode only the historical body. The envelope caller retains its historical
/// trailing-byte rule; all Session snapshots require complete consumption.
pub(super) fn take<T: V1>(bytes: &[u8]) -> Result<(T, &[u8]), LedgerError> {
    let (Decoded(value), rest) = postcard::take_from_bytes(bytes)?;
    Ok((value, rest))
}

/// Frozen original writer, reserving the entire bounded result before encoding.
/// It writes directly into final storage without a second payload or zero-fill.
pub(super) fn encode<T: V1>(magic: &[u8], value: &T, limit: usize) -> Result<Vec<u8>, LedgerError> {
    encode_view(magic, &Frozen(value), limit)
}
fn encode_view<T: Serialize>(
    magic: &[u8],
    value: &T,
    limit: usize,
) -> Result<Vec<u8>, LedgerError> {
    struct Bytes {
        bytes: Vec<u8>,
        length: usize,
    }
    impl postcard::ser_flavors::Flavor for Bytes {
        type Output = Vec<u8>;
        fn try_push(&mut self, byte: u8) -> Result<(), postcard::Error> {
            if self.bytes.len() >= self.length {
                return Err(postcard::Error::SerializeBufferFull);
            }
            self.bytes.push(byte);
            Ok(())
        }
        fn try_extend(&mut self, bytes: &[u8]) -> Result<(), postcard::Error> {
            if self
                .bytes
                .len()
                .checked_add(bytes.len())
                .is_none_or(|length| length > self.length)
            {
                return Err(postcard::Error::SerializeBufferFull);
            }
            self.bytes.extend_from_slice(bytes);
            Ok(())
        }
        fn finalize(self) -> Result<Self::Output, postcard::Error> {
            if self.bytes.len() != self.length {
                return Err(postcard::Error::SerializeBufferFull);
            }
            Ok(self.bytes)
        }
    }
    let length = postcard::experimental::serialized_size(value)?
        .checked_add(magic.len())
        .filter(|length| *length <= limit)
        .ok_or(LedgerError::Capacity)?;
    let mut bytes = Vec::new();
    bytes
        .try_reserve_exact(length)
        .map_err(|_| LedgerError::Capacity)?;
    bytes.extend_from_slice(magic);
    Ok(postcard::serialize_with_flavor(
        value,
        Bytes { bytes, length },
    )?)
}

/// Postcard's Vec<u8> representation is a count followed by these same raw bytes.
struct CoreBytes<'a>(&'a [u8]);
impl Serialize for CoreBytes<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_bytes(self.0)
    }
}
struct DeltaTail<'a>(&'a VecDeque<RetainedDelta>);
impl Serialize for DeltaTail<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut sequence = serializer.serialize_seq(Some(self.0.len()))?;
        for row in self.0 {
            sequence.serialize_element(&Frozen(&row.delta))?;
        }
        sequence.end()
    }
}
struct StreamRows<'a>(&'a RequestStreams);
impl Serialize for StreamRows<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let rows = self.0.checkpoint_rows();
        let mut sequence = serializer.serialize_seq(Some(rows.len()))?;
        for row in rows {
            sequence.serialize_element(&Frozen(row))?;
        }
        sequence.end()
    }
}
#[derive(Serialize)]
struct RequestCheckpointRef<'a> {
    activated: bool,
    slots: StreamRows<'a>,
}
#[derive(Serialize)]
struct SnapshotV2Ref<'a> {
    schema: u16,
    ledger: Frozen<'a, LedgerId>,
    raft_index: u64,
    core: CoreBytes<'a>,
    cursors: Frozen<'a, CursorCheckpoint>,
    cursor_meta: Frozen<'a, CursorMetadata>,
    delta_floor: Decoded<SessionSeq>,
    deltas: DeltaTail<'a>,
}
#[derive(Serialize)]
struct SnapshotV3Ref<'a> {
    state: SnapshotV2Ref<'a>,
    membership: Frozen<'a, MembershipState>,
}
#[derive(Serialize)]
struct SnapshotV4Ref<'a> {
    state: SnapshotV3Ref<'a>,
    placement: Frozen<'a, PlacementState>,
}
#[derive(Serialize)]
struct SnapshotV5Ref<'a> {
    state: SnapshotV4Ref<'a>,
    requests: RequestCheckpointRef<'a>,
}
#[derive(Serialize)]
struct SnapshotV7Ref<'a> {
    state: SnapshotV4Ref<'a>,
    requests: RequestCheckpointRef<'a>,
    activation: CoreBytes<'a>,
    native: CoreBytes<'a>,
    slot_generation: u64,
}

pub(super) fn snapshot(
    session: &Session,
    core: &[u8],
    native: Option<&[u8]>,
) -> Result<Vec<u8>, LedgerError> {
    let state = SnapshotV3Ref {
        state: SnapshotV2Ref {
            schema: 2,
            ledger: Frozen(&session.ledger),
            raft_index: session.applied_raft,
            core: CoreBytes(core),
            cursors: Frozen(session.cursors.checkpoint()),
            cursor_meta: Frozen(&session.cursor_meta),
            delta_floor: Decoded(session.stream_bounds().floor),
            deltas: DeltaTail(&session.deltas),
        },
        membership: Frozen(&session.membership_state),
    };
    const LIMIT: usize = 8 * 1024 * 1024;
    if let Some(native) = native {
        // A native ledger always writes the complete envelope: every ancillary
        // section, its activation record and the enclosing native checkpoint.
        let activation = session
            .activation_record
            .as_deref()
            .ok_or(LedgerError::Corrupt)?;
        return encode_view(
            SNAPSHOT_V7_MAGIC,
            &SnapshotV7Ref {
                state: SnapshotV4Ref {
                    state,
                    placement: Frozen(&session.placement_state),
                },
                requests: RequestCheckpointRef {
                    activated: session.request_streams.activated,
                    slots: StreamRows(&session.request_streams),
                },
                activation: CoreBytes(activation),
                native: CoreBytes(native),
                slot_generation: session.request_streams.next_generation(),
            },
            LIMIT.saturating_mul(2),
        );
    }
    if session.request_streams.activated {
        encode_view(
            SNAPSHOT_V5_MAGIC,
            &SnapshotV5Ref {
                state: SnapshotV4Ref {
                    state,
                    placement: Frozen(&session.placement_state),
                },
                requests: RequestCheckpointRef {
                    activated: session.request_streams.activated,
                    slots: StreamRows(&session.request_streams),
                },
            },
            LIMIT,
        )
    } else if session.placement_state.latest().is_some() {
        encode_view(
            SNAPSHOT_V4_MAGIC,
            &SnapshotV4Ref {
                state,
                placement: Frozen(&session.placement_state),
            },
            LIMIT,
        )
    } else {
        encode_view(SNAPSHOT_V3_MAGIC, &state, LIMIT)
    }
}
