use crate::input::*;
use focal_model::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReceiveTestamentDocument {
    pub claim: String,
    pub testament: String,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IncrementValidationDocument {
    pub claim: String,
    pub validation: String,
    pub target_hash: String,
    pub manifest: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SupersedeDocument {
    pub predecessor: String,
    pub successor: ClaimDocument,
}
impl SupersedeDocument {
    pub fn build(
        mut self,
        context: &BuildContext,
        ids: &mut impl IdGenerator,
    ) -> Result<Command, InputError> {
        let predecessor = ClaimId(parse_id(&self.predecessor)?);
        if !self.successor.relations.iter().any(|relation| {
            relation.kind == "supersedes"
                && parse_id(&relation.target).is_ok_and(|id| id == predecessor.0)
        }) {
            if self.successor.relations.len() >= 252 {
                return Err(InputError::Capacity);
            }
            self.successor
                .relations
                .try_reserve(1)
                .map_err(|_| InputError::Capacity)?;
            self.successor.relations.push(ClaimRelationDocument {
                kind: "supersedes".into(),
                target: predecessor.to_string(),
            });
        }
        let Command::GenerateClaim { claim: successor } = self.successor.build(context, ids)?
        else {
            return Err(InputError::Invalid("successor claim"));
        };
        Ok(Command::SupersedeClaim {
            predecessor,
            successor,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ValidationVerdictDocument {
    pub validation: String,
    pub target_hash: String,
    pub phase: String,
    pub epoch: u64,
    pub handler: HandlerDocument,
    pub attempt: u32,
    pub manifest: String,
    #[serde(default)]
    pub receipt: Option<ReceiptDocument>,
    pub value: String,
    pub evidence: Vec<ArtifactReferenceDocument>,
}
impl ValidationVerdictDocument {
    pub fn build(self, context: &BuildContext) -> Result<Command, InputError> {
        if self.epoch == 0 || self.evidence.len() > 256 {
            return Err(InputError::Invalid("validation epoch or evidence count"));
        }
        let run = ValidationRunId {
            validation: ValidationId(parse_id(&self.validation)?),
            target_hash: parse_hash(&self.target_hash)?,
            phase: parse_validation_phase(&self.phase)?,
            epoch: self.epoch,
        };
        let value = match self.value.as_str() {
            "pass" => VerdictValue::Pass,
            "fail" => VerdictValue::Fail,
            "error" => VerdictValue::Error,
            "incomplete" => VerdictValue::Incomplete,
            _ => return Err(InputError::Invalid("validation verdict")),
        };
        let mut evidence = Vec::new();
        evidence
            .try_reserve_exact(self.evidence.len())
            .map_err(|_| InputError::Capacity)?;
        for reference in self.evidence {
            let reference = ArtifactRef {
                id: ArtifactId(parse_id(&reference.id)?),
                hash: parse_hash(&reference.hash)?,
            };
            if evidence
                .iter()
                .any(|old: &ArtifactRef| old.id == reference.id)
            {
                return Err(InputError::Invalid("duplicate result evidence"));
            }
            evidence.push(reference);
        }
        Ok(Command::RecordFencedValidationVerdict {
            receipt: self.receipt.map(|receipt| receipt.build()).transpose()?,
            verdict: VerdictRecord {
                run,
                evaluator: context.actor,
                handler: HandlerRef {
                    id: ValidatorId(parse_id(&self.handler.id)?),
                    version: parse_hash(&self.handler.version)?,
                    agentic: self.handler.agentic,
                },
                attempt: self.attempt,
                manifest: parse_hash(&self.manifest)?,
                value,
                evidence,
            },
        })
    }
}
