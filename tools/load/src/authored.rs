//! Authored claim creation for an `authored_v1` ledger: the same document the
//! CLI's `submit claim --json` takes, compiled by the shared native-client
//! compiler into the exact frame the CLI would send, with deterministic
//! identities from this run's id space. A structural `Create` is refused on
//! such a ledger by the codec itself ("creation profile"), so this is the
//! only faithful write there.
use crate::error::LoadError;
use focal_client::input::{BuildContext, InputError};
use focal_client::native_store::{NativeIdentity, NativeIdentityKind};
use focal_client::operations::{NativeAuthoredOperation, NativeClaimDocument};
use focal_core::native::input_codec::EncodingLimits;
use focal_model::{LedgerId, ParticipantId, RequestKey, RootCommandId, RouteEpoch};
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

/// One compiled `claim.submit`: `actor` issues to `subject`, sent as
/// `request` (the principal, its generation and the request id), minting
/// identities from `first_id` upward (fewer than [`IDS_PER_CREATION`]). With an `evaluator`, a
/// work claim: beside the receipt validation every claim has (D-07), a required whole-work test
/// validation that `evaluator` judges, checking the claim's output slot (02 §3.3, §6).
pub fn create_envelope(
    ledger: LedgerId,
    actor: ParticipantId,
    subject: ParticipantId,
    evaluator: Option<ParticipantId>,
    root: RootCommandId,
    request: RequestKey,
    first_id: u128,
) -> Result<Authored, LoadError> {
    let receipt = serde_json::json!({
        "kind": "receipt",
        "description": "Record delivery.",
        "deadline": {"at": FAR_FUTURE_MS},
    });
    let claim = match evaluator {
        None => serde_json::json!({
            "description": format!("focal-load claim {}", hex(request.id.0)),
            "target": hex(subject.0),
            "validations": [receipt],
        }),
        Some(evaluator) => serde_json::json!({
            "description": format!("focal-load work {}", hex(request.id.0)),
            "target": hex(subject.0),
            "validations": [receipt, {
                "kind": "test",
                "description": "The work's report passes.",
                "target": {"type": "slot", "index": WORK_SLOT, "name": "report"},
                "evaluator": hex(evaluator.0),
                "handlers": [{
                    "id": hex(*VALIDATOR_ID),
                    "version": VALIDATOR_VERSION
                        .iter()
                        .map(|byte| format!("{byte:02x}"))
                        .collect::<String>(),
                    "attempts": 1,
                }],
                "deadline": {"at": FAR_FUTURE_MS},
            }],
            "slots": [{"slot": WORK_SLOT, "checks": [{"declaration": 1}]}],
        }),
    };
    let document: NativeClaimDocument =
        serde_json::from_value(claim).map_err(fixture("claim document"))?;
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
        request,
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
            request_epoch: request.epoch,
            request_id: request.id,
            operation: Operation::Native { frame },
        },
        claim,
    })
}

/// A later step of a claim's lifecycle (`claim.cancel`, `receipt.acquire`,
/// `testament.submit`, ...), compiled against the objects it binds to as they
/// were just read (`objects`), as the CLI compiles it after resolving them.
/// It mints the identities it creates (a receipt, a testament) from `ids`, a
/// span of the run's id space, none past its end; and returns them.
#[allow(clippy::too_many_arguments)] // One frame's identities, each its own.
pub fn step_envelope(
    ledger: LedgerId,
    profile: NativeContentProfile,
    actor: ParticipantId,
    root: RootCommandId,
    request: RequestKey,
    operation: &NativeAuthoredOperation,
    objects: &[focal_wire::NativeObject],
    ids: std::ops::Range<u128>,
) -> Result<(RequestEnvelope, Vec<NativeIdentity>), LoadError> {
    let context = BuildContext {
        ledger,
        actor,
        root,
        policy_revision: 1,
    };
    let resolved = Resolved::from_objects(ledger, objects).map_err(fixture("resolved objects"))?;
    let mut next = ids.start;
    let mut ids = move || -> Result<[u8; 16], InputError> {
        if next >= ids.end {
            return Err(InputError::Capacity);
        }
        let id = next.to_be_bytes();
        next = next.checked_add(1).ok_or(InputError::Capacity)?;
        Ok(id)
    };
    let compiled = compile(
        operation,
        &context,
        request,
        &mut ids,
        &resolved,
        &CompileLimits::default(),
    )
    .map_err(fixture("compile"))?;
    let frame = encode_frame(
        ledger,
        profile,
        &compiled.input,
        EncodingLimits {
            bytes: FRAME_BYTES,
            visits: FRAME_VISITS,
        },
    )
    .map_err(fixture("frame"))?;
    Ok((
        RequestEnvelope {
            protocol: NATIVE_PROTOCOL_VERSION,
            ledger,
            route_epoch: RouteEpoch(1),
            request_epoch: request.epoch,
            request_id: request.id,
            operation: Operation::Native { frame },
        },
        compiled.created,
    ))
}

/// The document a lifecycle step names its claim by.
pub fn claim_target(claim: u128) -> focal_client::operations::NativeClaimTargetDocument {
    focal_client::operations::NativeClaimTargetDocument {
        claim: hex(claim.to_be_bytes()),
    }
}

/// The document a step names its claim's testament by.
pub fn testament_target(
    claim: u128,
    testament: u128,
) -> focal_client::operations::NativeResponseTargetDocument {
    focal_client::operations::NativeResponseTargetDocument {
        claim: hex(claim.to_be_bytes()),
        testament: hex(testament.to_be_bytes()),
    }
}

/// The consumer's receipt for its claim, its identity minted.
pub fn receipt(claim: u128) -> focal_client::operations::NativeReceiptDocument {
    focal_client::operations::NativeReceiptDocument {
        claim: hex(claim.to_be_bytes()),
        id: None,
    }
}

/// The output slot a work claim declares and its worker fills.
pub const WORK_SLOT: u32 = 0;
/// The validator a work claim pins for its test validation (02 §6.1: a stable identity and a
/// version digest, which the evaluator runs in its own environment): the load tool's report
/// check, named by its own bytes.
const VALIDATOR_ID: &[u8; 16] = b"focal-load-check";
const VALIDATOR_VERSION: &[u8; 32] = b"focal-load report check, first..";
/// A work artifact's report, in the builtin test-report schema an artifact defaults to.
const REPORT: &str = r#"{"passed":1,"failed":0,"skipped":0}"#;

fn hash_hex(hash: focal_model::ContentHash) -> String {
    hash.0.iter().map(|byte| format!("{byte:02x}")).collect()
}

/// The worker's work artifact for its claim's output slot, under its receipt, its identity
/// minted.
pub fn work_artifact(claim: u128) -> focal_client::operations::NativeWorkArtifactDocument {
    focal_client::operations::NativeWorkArtifactDocument {
        claim: hex(claim.to_be_bytes()),
        slot: WORK_SLOT,
        id: None,
        kind: None,
        schema_hash: None,
        metadata: Vec::new(),
        payload: focal_client::operations::NativePayloadDocument::Text {
            text: REPORT.to_owned(),
        },
        inputs: Vec::new(),
        visibility: Vec::new(),
    }
}

/// The worker's closing testament for a work claim: complete, its output slot bound to the
/// artifact it submitted (`artifact`, as the ledger holds it), its identity minted.
pub fn work_testament(
    claim: u128,
    artifact: focal_model::ArtifactRef,
) -> focal_client::operations::NativeResponseDocument {
    focal_client::operations::NativeResponseDocument {
        manifest: vec![focal_client::operations::NativeSlotBindingDocument {
            slot: WORK_SLOT,
            artifact: focal_client::operations::NativeArtifactReferenceDocument {
                id: hex(artifact.id.0),
                hash: hash_hex(artifact.hash),
            },
        }],
        ..testament(claim)
    }
}

/// The evaluator's beginning of the current whole-work evaluation of `validation`.
pub fn begin(claim: u128, validation: u128) -> focal_client::operations::NativeEvaluationDocument {
    focal_client::operations::NativeEvaluationDocument {
        claim: hex(claim.to_be_bytes()),
        validation: hex(validation.to_be_bytes()),
        slot: None,
        phase: "whole_work".to_owned(),
        target: None,
    }
}

/// The evaluator's report of the evaluation it began: a pass, with its result artifact, its
/// identity minted.
pub fn report(claim: u128, validation: u128) -> focal_client::operations::NativeReportDocument {
    focal_client::operations::NativeReportDocument {
        claim: hex(claim.to_be_bytes()),
        validation: hex(validation.to_be_bytes()),
        slot: None,
        phase: "whole_work".to_owned(),
        target: None,
        verdict: "pass".to_owned(),
        id: None,
        kind: None,
        schema_hash: None,
        metadata: Vec::new(),
        payload: focal_client::operations::NativePayloadDocument::Text {
            text: REPORT.to_owned(),
        },
        inputs: Vec::new(),
        visibility: Vec::new(),
    }
}

/// The consumer's closing testament for its claim: the work done, at the confidence a receipt
/// claim asks, its identity minted.
pub fn testament(claim: u128) -> focal_client::operations::NativeResponseDocument {
    focal_client::operations::NativeResponseDocument {
        claim: hex(claim.to_be_bytes()),
        id: None,
        summary: "Delivered.".to_owned(),
        confidence: "committed".to_owned(),
        outcome: "complete".to_owned(),
        manifest: Vec::new(),
        diagnostics: Vec::new(),
    }
}
