//! Original V1 command bodies and canonical command tags. The representation
//! below is independent of the current Command's Serde derive and code method.
use super::{Ref, V1, Value};
use crate::*;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::collections::BTreeSet;

// One explicit historical declaration fixes both enum representations and all
// nested field orders. Exhaustive matching forces a deliberate version boundary
// when the current Command gains a variant or a field. Encoding borrows every
// input; decoding moves each final nested allocation into the current command.
macro_rules! v1_commands {
    ($($variant:ident { $($field:ident: $ty:ty),+ $(,)? } => $code:literal),+ $(,)?) => {
        #[derive(Serialize)]
        enum CommandRefV1<'a> {
            $($variant { $($field: Ref<'a, $ty>),+ }),+
        }

        #[derive(Deserialize)]
        enum CommandValueV1 {
            $($variant { $($field: Value<$ty>),+ }),+
        }

        impl V1 for Command {
            fn serialize_v1<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
                match self {
                    $(Self::$variant { $($field),+ } => CommandRefV1::$variant {
                        $($field: Ref($field)),+
                    }),+
                }.serialize(serializer)
            }

            fn deserialize_v1<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
                Ok(match CommandValueV1::deserialize(deserializer)? {
                    $(CommandValueV1::$variant { $($field),+ } => Self::$variant {
                        $($field: $field.0),+
                    }),+
                })
            }
        }

        /// Fixed canonical tags 1–29. The serialized enum ordinals are 0–28;
        /// neither mapping is delegated to the current model's implementation.
        pub(crate) const fn command_code(command: &Command) -> u16 {
            match command { $(Command::$variant { .. } => $code),+ }
        }
    };
}

v1_commands! {
    NegotiateEpoch {
        epoch: RequestEpoch,
    } => 1,
    AdvanceEpochFloor {
        minimum: RequestEpoch,
    } => 2,
    GenerateClaim {
        claim: NewClaim,
    } => 3,
    GenerateClaimBatch {
        claims: Vec<NewClaim>,
    } => 4,
    PostClaim {
        claim: ClaimId,
    } => 5,
    AcquireReceipt {
        claim: ClaimId,
        receipt: ReceiptId,
        epoch: u64,
    } => 6,
    AdoptReceipt {
        claim: ClaimId,
        previous: ReceiptFence,
        receipt: ReceiptId,
        holder: ParticipantId,
        epoch: u64,
    } => 7,
    RecordProgress {
        claim: ClaimId,
        receipt: ReceiptFence,
        message: String,
    } => 8,
    BeginEvidenceSet {
        claim: ClaimId,
        receipt: ReceiptFence,
        evidence_set: EvidenceSetId,
    } => 9,
    AttachArtifact {
        claim: ClaimId,
        receipt: ReceiptFence,
        evidence_set: EvidenceSetId,
        artifact: NewArtifact,
    } => 10,
    CloseTestament {
        claim: ClaimId,
        receipt: ReceiptFence,
        testament: TestamentId,
        evidence_set: EvidenceSetId,
        manifest: Vec<ArtifactRef>,
        summary: String,
        confidence: Confidence,
        outcome: OutcomeKind,
    } => 11,
    AcknowledgeTestament {
        claim: ClaimId,
        testament: TestamentId,
    } => 12,
    BeginWholeWorkValidation {
        claim: ClaimId,
    } => 13,
    BeginIncrementValidation {
        claim: ClaimId,
        validation: ValidationId,
        target: ContentHash,
        manifest: ContentHash,
    } => 14,
    RecordValidationVerdict {
        verdict: VerdictRecord,
    } => 15,
    CompleteWholeWork {
        claim: ClaimId,
    } => 16,
    FailPost {
        claim: ClaimId,
        evidence: ArtifactRef,
    } => 17,
    FailReceipt {
        claim: ClaimId,
        evidence: ArtifactRef,
    } => 18,
    FailTestamentGeneration {
        claim: ClaimId,
        testament: TestamentId,
        evidence_set: EvidenceSetId,
        error: NewArtifact,
        summary: String,
    } => 19,
    CancelClaim {
        claim: ClaimId,
        reason: String,
    } => 20,
    RevokeClaim {
        claim: ClaimId,
        reason: String,
    } => 21,
    ExpireClaim {
        claim: ClaimId,
        timer: TimerId,
        generation: u64,
        fired_at: u64,
    } => 22,
    SupersedeClaim {
        predecessor: ClaimId,
        successor: NewClaim,
    } => 23,
    RegisterMonitor {
        monitor: MonitorId,
        owner: ClaimId,
        roots: BTreeSet<WaitPredicate>,
        deadline: Deadline,
    } => 24,
    RebindMonitor {
        monitor: MonitorId,
        predecessor: ClaimId,
        successor: ClaimId,
    } => 25,
    ReleaseScope {
        claim: ClaimId,
    } => 26,
    RegisterArtifact {
        artifact: NewArtifact,
    } => 27,
    ExpireMonitor {
        monitor: MonitorId,
        timer: TimerId,
        generation: u64,
        fired_at: u64,
    } => 28,
    RecordFencedValidationVerdict {
        verdict: VerdictRecord,
        receipt: Option<ReceiptFence>,
    } => 29,
}

#[cfg(test)]
#[path = "commands_tests.rs"]
mod tests;
