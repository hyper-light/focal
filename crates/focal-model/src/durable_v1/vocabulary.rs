use super::V1;
use crate::*;
use serde::{Deserialize, Deserializer, Serializer};

// Historical numeric vocabularies are explicit u16 codes, NOT derived enum
// ordinals. Do not call live code()/from_code(): successor variants must not
// become readable as V1 merely because the current vocabulary was extended.
macro_rules! v1_vocabulary {
    ($name:ident { $($variant:ident = $number:literal),+ $(,)? }) => {
        impl V1 for $name {
            #[inline]
            fn serialize_v1<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
                let code = match self { $(Self::$variant => $number),+ };
                serializer.serialize_u16(code)
            }
            fn deserialize_v1<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
                match u16::deserialize(deserializer)? {
                    $($number => Ok(Self::$variant),)+
                    _ => Err(serde::de::Error::custom(concat!("unknown critical V1 ", stringify!($name)))),
                }
            }
        }
    };
}
v1_vocabulary!(ObjectKind { Claim=1, Testament=2, Validation=3, Artifact=4 });
v1_vocabulary!(ClaimStatus { Generated=1, Posted=2, Received=3, Progressed=4,
    TestamentGenerated=5, TestamentAcknowledged=6, Validating=7, Satisfied=8,
    PostFailed=9, ReceiptFailed=10, TestamentGenerationFailed=11,
    ValidationIncomplete=12, ValidationFailed=13, ValidationErrored=14,
    Cancelled=15, Expired=16, Revoked=17, Superseded=18, DependencyFailed=19, Deadlocked=20 });
v1_vocabulary!(ActionType { Work=1, Consultation=2, Challenge=3, Feedback=4, Approval=5, Summon=6, Handoff=7, Evaluation=8, Correction=9, Teardown=10 });
v1_vocabulary!(ScopeKind { File=1, Symbol=2, Api=3, TestSurface=4, Component=5, UxSurface=6 });
v1_vocabulary!(RelationKind { Issuer=1, Subject=2, Evaluator=3, ClaimAction=4, Supersedes=5, DependsOn=6, Awaits=7, CausedBy=8, Refines=9, ConflictsWith=10, DerivedFrom=11, Reviews=12, Amends=13, ContributedBy=14, Invalidates=15 });
v1_vocabulary!(ValidationKind { Receipt=1, Test=2, Inspection=3, Integration=4, Contract=5, Design=6, Regression=7 });
v1_vocabulary!(ValidationPhase { Admission=1, Increment=2, WholeWork=3 });
v1_vocabulary!(ValidationMode { Observe=1, Required=2 });
v1_vocabulary!(VerdictValue { Pass=1, Fail=2, Incomplete=3, Error=4 });
v1_vocabulary!(Confidence { Hint=1, Tentative=2, Committed=3, Consensus=4 });
v1_vocabulary!(OutcomeKind { Complete=1, Partial=2, Refused=3, Impossible=4, Interrupted=5, Failed=6 });
v1_vocabulary!(ContentClass { Document=1, Evidence=2, Checkpoint=3 });
v1_vocabulary!(Disposition { Retryable=1, Terminal=2 });
v1_vocabulary!(ErrorCode { InvalidSchema=1, InvalidNamespace=2, InvalidRelation=3, InvalidCause=4, WrongActor=5, StaleReceipt=6, StaleEvaluator=7, IdempotencyConflict=8, RequestHistoryExpired=9, RequestEpochNotAdmitted=10, UnknownObject=11, ObjectIdConflict=12, Capacity=13, EvidenceNotDurable=14, ConflictingVerdict=15, DeadlineNotDue=16, RevisionConflict=17, InvalidManifest=18, InvalidTransition=19, UnsupportedSchema=20, EmptyRequiredSet=21, InvalidEpoch=22, PartialDuplicateBatch=23, MissingDeadline=24, InvalidHandler=25, StandingDenied=26 });
v1_vocabulary!(LifecycleAction { Generated=1, Posted=2, Received=3, Progressed=4, TestamentGenerated=5, TestamentAcknowledged=6, Validating=7, Satisfied=8, PostFailed=9, ReceiptFailed=10, TestamentGenerationFailed=11, ValidationIncomplete=12, ValidationFailed=13, ValidationErrored=14, Cancelled=15, Expired=16, Revoked=17, Superseded=18, DependencyFailed=19, Deadlocked=20, EvidenceOpened=21, ArtifactAttached=22, ValidationScheduled=23, ValidationVerdict=24, ReceiptAdopted=25, ScopeRegistered=26, ScopeReleased=27, ScopeRebound=28, EpochAdmitted=29, EpochFloorAdvanced=30, LocalCompleted=31 });
