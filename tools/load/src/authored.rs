//! Authored claim creation for an `authored_v1` ledger: the same document the
//! CLI's `submit claim --json` takes, compiled by the shared native-client
//! compiler into the exact frame the CLI would send, with deterministic
//! identities from this run's id space. A structural `Create` is refused on
//! such a ledger by the codec itself ("creation profile"), so this is the
//! only faithful write there.
use crate::error::LoadError;
use focal_client::input::{BuildContext, InputError};
use focal_client::native_store::NativeIdentityKind;
use focal_client::operations::{NativeAuthoredOperation, NativeClaimDocument};
use focal_core::native::input_codec::EncodingLimits;
use focal_model::{LedgerId, ParticipantId, RequestEpoch, RequestId, RootCommandId, RouteEpoch};
use focal_native_client::{CompileLimits, NativeContentProfile, Resolved, compile, encode_frame};
use focal_wire::{NATIVE_PROTOCOL_VERSION, Operation, RequestEnvelope};

/// The identities one compiled creation may mint (claim, validations,
/// occurrence, ...); the id space reserves this many per write.
pub const IDS_PER_CREATION: u128 = 256;
/// The most bytes one encoded request frame may take.
const FRAME_BYTES: usize = 1 << 20;
/// The most encoder visits one frame may take.
const FRAME_VISITS: usize = 1 << 28;
/// The a1 deadline: 2100-01-01 in milliseconds.
const FAR_FUTURE_MS: u64 = 4_102_444_800_000;

/// A compiled creation and the claim it mints.
pub struct Authored {
    pub envelope: RequestEnvelope,
    pub claim: u128,
}

fn hex(id: [u8; 16]) -> String {
    id.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn fixture<E: std::fmt::Display>(what: &'static str) -> impl FnOnce(E) -> LoadError {
    move |error| LoadError::Fixture(format!("{what}: {error}"))
}

/// One compiled `claim.submit`: `actor` issues to `subject`, request
/// `request`, minting identities from `first_id` upward (fewer than
/// [`IDS_PER_CREATION`]).
pub fn create_envelope(
    ledger: LedgerId,
    actor: ParticipantId,
    subject: ParticipantId,
    root: RootCommandId,
    request: u128,
    first_id: u128,
) -> Result<Authored, LoadError> {
    let document: NativeClaimDocument = serde_json::from_value(serde_json::json!({
        "description": format!("focal-load claim {request:x}"),
        "target": hex(subject.0),
        "validations": [{
            "kind": "receipt",
            "description": "Record delivery.",
            "deadline": {"at": FAR_FUTURE_MS},
        }],
    }))
    .map_err(fixture("claim document"))?;
    let operation = NativeAuthoredOperation::ClaimSubmit(document);
    let context = BuildContext {
        ledger,
        actor,
        root,
        policy_revision: 1,
    };
    let resolved = Resolved::from_objects(ledger, &[]).map_err(fixture("resolved objects"))?;
    let end = first_id
        .checked_add(IDS_PER_CREATION)
        .ok_or(LoadError::Bound("creation id space"))?;
    let mut next = first_id;
    let mut ids = move || -> Result<[u8; 16], InputError> {
        if next >= end {
            return Err(InputError::Capacity);
        }
        let id = next.to_be_bytes();
        next = next.checked_add(1).ok_or(InputError::Capacity)?;
        Ok(id)
    };
    let compiled = compile(
        &operation,
        &context,
        RequestId::from_u128(request),
        &mut ids,
        &resolved,
        &CompileLimits::default(),
    )
    .map_err(fixture("compile"))?;
    let claim = compiled
        .created
        .iter()
        .find(|identity| identity.kind == NativeIdentityKind::Claim)
        .map(|identity| u128::from_be_bytes(identity.id))
        .ok_or_else(|| LoadError::Fixture("the compiled creation minted no claim".into()))?;
    let frame = encode_frame(
        ledger,
        NativeContentProfile::AuthoredV1,
        &compiled.input,
        EncodingLimits {
            bytes: FRAME_BYTES,
            visits: FRAME_VISITS,
        },
    )
    .map_err(fixture("frame"))?;
    Ok(Authored {
        envelope: RequestEnvelope {
            protocol: NATIVE_PROTOCOL_VERSION,
            ledger,
            route_epoch: RouteEpoch(1),
            request_epoch: RequestEpoch(1),
            request_id: RequestId::from_u128(request),
            operation: Operation::Native { frame },
        },
        claim,
    })
}
