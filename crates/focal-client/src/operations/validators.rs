use super::{ListDocument, page_items, page_visits};
use crate::input::*;
use focal_model::*;
use focal_wire::*;
use serde::{Deserialize, Serialize};

/// One bounded page of recorded requirement bindings. A handler may be used
/// under different evidence contracts; these rows are never collapsed together.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ValidatorDocument {
    #[serde(default)]
    pub id: Option<String>,
    #[serde(default)]
    pub version: Option<String>,
    #[serde(default)]
    pub agentic: Option<bool>,
    #[serde(default)]
    pub schema_hash: Option<String>,
    #[serde(default)]
    pub claim: Option<String>,
    #[serde(default)]
    pub evaluator: Option<String>,
    #[serde(default)]
    pub kind: Option<String>,
    #[serde(default)]
    pub phase: Option<String>,
    #[serde(default)]
    pub mode: Option<String>,
    #[serde(default)]
    pub cursor: Option<String>,
    #[serde(default = "page_items")]
    pub limit: u32,
    #[serde(default = "page_visits")]
    pub max_visits: u32,
}
impl ValidatorDocument {
    pub fn build(
        self,
        context: &BuildContext,
        exact: bool,
    ) -> Result<ValidatorRequest, InputError> {
        if exact && (self.id.is_none() || self.version.is_none()) {
            return Err(InputError::Invalid(
                "validator get requires id and exact version",
            ));
        }
        let query = ListDocument {
            claim: self.claim,
            evaluator: self.evaluator,
            kind: self.kind,
            phase: self.phase,
            mode: self.mode,
            cursor: self.cursor,
            limit: self.limit,
            max_visits: self.max_visits,
            ..ListDocument::default()
        }
        .build(ObjectKind::Validation, context)?;
        let request = ValidatorRequest {
            query,
            handler: self
                .id
                .map(|id| parse_id(&id).map(ValidatorId))
                .transpose()?,
            version: self.version.map(|hash| parse_hash(&hash)).transpose()?,
            agentic: self.agentic,
            evidence_schema: self.schema_hash.map(|hash| parse_hash(&hash)).transpose()?,
        };
        request
            .validate(&WireLimits::default())
            .map_err(|_| InputError::Invalid("invalid validator contract query"))?;
        Ok(request)
    }
}
