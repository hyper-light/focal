//! Frozen V1 field lists and enum ordinals. These borrowed writers and owned
//! decoders never delegate a domain value to its current Serde implementation.
use super::{Ref, V1, Value};
use crate::*;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

v1_struct!(DeltaId {
    ledger: LedgerId,
    sequence: SessionSeq,
    ordinal: u32,
});

#[derive(Serialize)]
enum InformReasonRefV1<'a> {
    Terminal(Ref<'a, ClaimStatus>),
    Status(Ref<'a, ClaimStatus>),
    DependencyPending(Ref<'a, ClaimId>),
    ValidationPending,
    AlreadyApplied,
    StandingDenied,
}

#[derive(Deserialize)]
enum InformReasonValueV1 {
    Terminal(Value<ClaimStatus>),
    Status(Value<ClaimStatus>),
    DependencyPending(Value<ClaimId>),
    ValidationPending,
    AlreadyApplied,
    StandingDenied,
}

impl V1 for InformReason {
    fn serialize_v1<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self {
            Self::Terminal(value0) => InformReasonRefV1::Terminal(Ref(value0)),
            Self::Status(value0) => InformReasonRefV1::Status(Ref(value0)),
            Self::DependencyPending(value0) => InformReasonRefV1::DependencyPending(Ref(value0)),
            Self::ValidationPending => InformReasonRefV1::ValidationPending,
            Self::AlreadyApplied => InformReasonRefV1::AlreadyApplied,
            Self::StandingDenied => InformReasonRefV1::StandingDenied,
        }
        .serialize(serializer)
    }
    fn deserialize_v1<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        Ok(match InformReasonValueV1::deserialize(deserializer)? {
            InformReasonValueV1::Terminal(value0) => Self::Terminal(value0.0),
            InformReasonValueV1::Status(value0) => Self::Status(value0.0),
            InformReasonValueV1::DependencyPending(value0) => Self::DependencyPending(value0.0),
            InformReasonValueV1::ValidationPending => Self::ValidationPending,
            InformReasonValueV1::AlreadyApplied => Self::AlreadyApplied,
            InformReasonValueV1::StandingDenied => Self::StandingDenied,
        })
    }
}

#[derive(Serialize)]
enum DomainOutcomeRefV1<'a> {
    Refuse {
        code: Ref<'a, ErrorCode>,
        detail: Ref<'a, String>,
    },
    Inform {
        claim: Ref<'a, Option<ClaimId>>,
        reason: Ref<'a, InformReason>,
    },
    Duplicate(Ref<'a, Box<MutationReceipt>>),
}

#[derive(Deserialize)]
enum DomainOutcomeValueV1 {
    Refuse {
        code: Value<ErrorCode>,
        detail: Value<String>,
    },
    Inform {
        claim: Value<Option<ClaimId>>,
        reason: Value<InformReason>,
    },
    Duplicate(Value<Box<MutationReceipt>>),
}

impl V1 for DomainOutcome {
    fn serialize_v1<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self {
            Self::Refuse { code, detail } => DomainOutcomeRefV1::Refuse {
                code: Ref(code),
                detail: Ref(detail),
            },
            Self::Inform { claim, reason } => DomainOutcomeRefV1::Inform {
                claim: Ref(claim),
                reason: Ref(reason),
            },
            Self::Duplicate(value0) => DomainOutcomeRefV1::Duplicate(Ref(value0)),
        }
        .serialize(serializer)
    }
    fn deserialize_v1<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        Ok(match DomainOutcomeValueV1::deserialize(deserializer)? {
            DomainOutcomeValueV1::Refuse { code, detail } => Self::Refuse {
                code: code.0,
                detail: detail.0,
            },
            DomainOutcomeValueV1::Inform { claim, reason } => Self::Inform {
                claim: claim.0,
                reason: reason.0,
            },
            DomainOutcomeValueV1::Duplicate(value0) => Self::Duplicate(value0.0),
        })
    }
}

v1_struct!(Delta {
    schema: u16,
    id: DeltaId,
    action: LifecycleAction,
    actor: ParticipantId,
    claim: Option<ClaimId>,
    fact: DeltaFact,
});

#[derive(Serialize)]
enum DeltaFactRefV1<'a> {
    Status {
        previous: Ref<'a, Option<ClaimStatus>>,
        current: Ref<'a, ClaimStatus>,
        witness: Ref<'a, Option<ClaimId>>,
    },
    Progress(Ref<'a, String>),
    Receipt(Ref<'a, Receipt>),
    EvidenceSet(Ref<'a, EvidenceSetId>),
    Artifact(Ref<'a, ArtifactRef>),
    Testament {
        id: Ref<'a, TestamentId>,
        hash: Ref<'a, ContentHash>,
    },
    ValidationScheduled(Ref<'a, ValidationRunId>),
    Verdict(Ref<'a, VerdictRecord>),
    Scope(Ref<'a, MonitorId>),
    Epoch(Ref<'a, RequestEpoch>),
    LocalCompletion,
}

#[derive(Deserialize)]
enum DeltaFactValueV1 {
    Status {
        previous: Value<Option<ClaimStatus>>,
        current: Value<ClaimStatus>,
        witness: Value<Option<ClaimId>>,
    },
    Progress(Value<String>),
    Receipt(Value<Receipt>),
    EvidenceSet(Value<EvidenceSetId>),
    Artifact(Value<ArtifactRef>),
    Testament {
        id: Value<TestamentId>,
        hash: Value<ContentHash>,
    },
    ValidationScheduled(Value<ValidationRunId>),
    Verdict(Value<VerdictRecord>),
    Scope(Value<MonitorId>),
    Epoch(Value<RequestEpoch>),
    LocalCompletion,
}

impl V1 for DeltaFact {
    fn serialize_v1<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self {
            Self::Status {
                previous,
                current,
                witness,
            } => DeltaFactRefV1::Status {
                previous: Ref(previous),
                current: Ref(current),
                witness: Ref(witness),
            },
            Self::Progress(value0) => DeltaFactRefV1::Progress(Ref(value0)),
            Self::Receipt(value0) => DeltaFactRefV1::Receipt(Ref(value0)),
            Self::EvidenceSet(value0) => DeltaFactRefV1::EvidenceSet(Ref(value0)),
            Self::Artifact(value0) => DeltaFactRefV1::Artifact(Ref(value0)),
            Self::Testament { id, hash } => DeltaFactRefV1::Testament {
                id: Ref(id),
                hash: Ref(hash),
            },
            Self::ValidationScheduled(value0) => DeltaFactRefV1::ValidationScheduled(Ref(value0)),
            Self::Verdict(value0) => DeltaFactRefV1::Verdict(Ref(value0)),
            Self::Scope(value0) => DeltaFactRefV1::Scope(Ref(value0)),
            Self::Epoch(value0) => DeltaFactRefV1::Epoch(Ref(value0)),
            Self::LocalCompletion => DeltaFactRefV1::LocalCompletion,
            // Native facts are derived from committed native events on demand
            // and never enter the frozen legacy delta tail.
            Self::Native(_) => {
                return Err(serde::ser::Error::custom(
                    "native delta facts have no durable V1 encoding",
                ));
            }
        }
        .serialize(serializer)
    }
    fn deserialize_v1<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        Ok(match DeltaFactValueV1::deserialize(deserializer)? {
            DeltaFactValueV1::Status {
                previous,
                current,
                witness,
            } => Self::Status {
                previous: previous.0,
                current: current.0,
                witness: witness.0,
            },
            DeltaFactValueV1::Progress(value0) => Self::Progress(value0.0),
            DeltaFactValueV1::Receipt(value0) => Self::Receipt(value0.0),
            DeltaFactValueV1::EvidenceSet(value0) => Self::EvidenceSet(value0.0),
            DeltaFactValueV1::Artifact(value0) => Self::Artifact(value0.0),
            DeltaFactValueV1::Testament { id, hash } => Self::Testament {
                id: id.0,
                hash: hash.0,
            },
            DeltaFactValueV1::ValidationScheduled(value0) => Self::ValidationScheduled(value0.0),
            DeltaFactValueV1::Verdict(value0) => Self::Verdict(value0.0),
            DeltaFactValueV1::Scope(value0) => Self::Scope(value0.0),
            DeltaFactValueV1::Epoch(value0) => Self::Epoch(value0.0),
            DeltaFactValueV1::LocalCompletion => Self::LocalCompletion,
        })
    }
}

#[derive(Serialize)]
enum EffectIntentRefV1<'a> {
    DispatchClaim {
        claim: Ref<'a, ClaimId>,
    },
    ExecuteValidation {
        run: Ref<'a, ValidationRunId>,
        handler: Ref<'a, HandlerRef>,
        evaluator: Ref<'a, ParticipantId>,
        attempt: Ref<'a, u32>,
        manifest: Ref<'a, ContentHash>,
        quality_phase: Ref<'a, bool>,
    },
    CancelExecution {
        claim: Ref<'a, ClaimId>,
        receipt: Ref<'a, Option<ReceiptFence>>,
    },
    MonitorReleased {
        monitor: Ref<'a, MonitorId>,
    },
    ScheduleDeadline {
        claim: Ref<'a, ClaimId>,
        deadline: Ref<'a, Deadline>,
    },
}

#[derive(Deserialize)]
enum EffectIntentValueV1 {
    DispatchClaim {
        claim: Value<ClaimId>,
    },
    ExecuteValidation {
        run: Value<ValidationRunId>,
        handler: Value<HandlerRef>,
        evaluator: Value<ParticipantId>,
        attempt: Value<u32>,
        manifest: Value<ContentHash>,
        quality_phase: Value<bool>,
    },
    CancelExecution {
        claim: Value<ClaimId>,
        receipt: Value<Option<ReceiptFence>>,
    },
    MonitorReleased {
        monitor: Value<MonitorId>,
    },
    ScheduleDeadline {
        claim: Value<ClaimId>,
        deadline: Value<Deadline>,
    },
}

impl V1 for EffectIntent {
    fn serialize_v1<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self {
            Self::DispatchClaim { claim } => EffectIntentRefV1::DispatchClaim { claim: Ref(claim) },
            Self::ExecuteValidation {
                run,
                handler,
                evaluator,
                attempt,
                manifest,
                quality_phase,
            } => EffectIntentRefV1::ExecuteValidation {
                run: Ref(run),
                handler: Ref(handler),
                evaluator: Ref(evaluator),
                attempt: Ref(attempt),
                manifest: Ref(manifest),
                quality_phase: Ref(quality_phase),
            },
            Self::CancelExecution { claim, receipt } => EffectIntentRefV1::CancelExecution {
                claim: Ref(claim),
                receipt: Ref(receipt),
            },
            Self::MonitorReleased { monitor } => EffectIntentRefV1::MonitorReleased {
                monitor: Ref(monitor),
            },
            Self::ScheduleDeadline { claim, deadline } => EffectIntentRefV1::ScheduleDeadline {
                claim: Ref(claim),
                deadline: Ref(deadline),
            },
        }
        .serialize(serializer)
    }
    fn deserialize_v1<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        Ok(match EffectIntentValueV1::deserialize(deserializer)? {
            EffectIntentValueV1::DispatchClaim { claim } => Self::DispatchClaim { claim: claim.0 },
            EffectIntentValueV1::ExecuteValidation {
                run,
                handler,
                evaluator,
                attempt,
                manifest,
                quality_phase,
            } => Self::ExecuteValidation {
                run: run.0,
                handler: handler.0,
                evaluator: evaluator.0,
                attempt: attempt.0,
                manifest: manifest.0,
                quality_phase: quality_phase.0,
            },
            EffectIntentValueV1::CancelExecution { claim, receipt } => Self::CancelExecution {
                claim: claim.0,
                receipt: receipt.0,
            },
            EffectIntentValueV1::MonitorReleased { monitor } => {
                Self::MonitorReleased { monitor: monitor.0 }
            }
            EffectIntentValueV1::ScheduleDeadline { claim, deadline } => Self::ScheduleDeadline {
                claim: claim.0,
                deadline: deadline.0,
            },
        })
    }
}
