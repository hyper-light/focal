use super::InputError;
use focal_model::lifecycle::evidence::EvidenceFailure;
use focal_model::*;

macro_rules! parser {
    ($function:ident,$ty:ident,{$($variant:ident => $name:literal),+ $(,)?}) => {
        pub fn $function(value: &str) -> Result<$ty, InputError> {
            match value { $($name => Ok($ty::$variant),)+ _ => Err(InputError::Invalid(concat!("unknown ", stringify!($ty)))) }
        }
    };
}
parser!(parse_action,ActionType,{Work=>"work",Consultation=>"consultation",Challenge=>"challenge",Feedback=>"feedback",Approval=>"approval",Summon=>"summon",Handoff=>"handoff",Evaluation=>"evaluation",Correction=>"correction",Teardown=>"teardown"});
parser!(parse_scope_kind,ScopeKind,{File=>"file",Symbol=>"symbol",Api=>"api",TestSurface=>"test_surface",Component=>"component",UxSurface=>"ux_surface"});
parser!(parse_relation,RelationKind,{Issuer=>"issuer",Subject=>"subject",Evaluator=>"evaluator",ClaimAction=>"claim_action",Supersedes=>"supersedes",DependsOn=>"depends_on",Awaits=>"awaits",CausedBy=>"caused_by",Refines=>"refines",ConflictsWith=>"conflicts_with",DerivedFrom=>"derived_from",Reviews=>"reviews",Amends=>"amends",ContributedBy=>"contributed_by",Invalidates=>"invalidates"});
parser!(parse_validation_kind,ValidationKind,{Receipt=>"receipt",Test=>"test",Inspection=>"inspection",Integration=>"integration",Contract=>"contract",Design=>"design",Regression=>"regression"});
parser!(parse_validation_phase,ValidationPhase,{Admission=>"admission",Increment=>"increment",WholeWork=>"whole_work"});
parser!(parse_validation_mode,ValidationMode,{Observe=>"observe",Required=>"required"});
parser!(parse_confidence,Confidence,{Hint=>"hint",Tentative=>"tentative",Committed=>"committed",Consensus=>"consensus"});
parser!(parse_outcome,OutcomeKind,{Complete=>"complete",Partial=>"partial",Refused=>"refused",Impossible=>"impossible",Interrupted=>"interrupted",Failed=>"failed"});
parser!(parse_content_class,ContentClass,{Document=>"document",Evidence=>"evidence",Checkpoint=>"checkpoint"});
parser!(parse_object_kind,ObjectKind,{Claim=>"claim",Testament=>"testament",Validation=>"validation",Artifact=>"artifact"});
parser!(parse_status,ClaimStatus,{Generated=>"generated",Posted=>"posted",Received=>"received",Progressed=>"progressed",TestamentGenerated=>"testament_generated",TestamentAcknowledged=>"testament_acknowledged",Validating=>"validating",Satisfied=>"satisfied",PostFailed=>"post_failed",ReceiptFailed=>"receipt_failed",TestamentGenerationFailed=>"testament_generation_failed",ValidationIncomplete=>"validation_incomplete",ValidationFailed=>"validation_failed",ValidationErrored=>"validation_errored",Cancelled=>"cancelled",Expired=>"expired",Revoked=>"revoked",Superseded=>"superseded",DependencyFailed=>"dependency_failed",Deadlocked=>"deadlocked"});
parser!(parse_evidence_failure,EvidenceFailure,{Work=>"work",Production=>"production",Structure=>"structure",Metadata=>"metadata"});
parser!(parse_verdict,VerdictValue,{Pass=>"pass",Fail=>"fail",Incomplete=>"incomplete",Error=>"error"});
