use super::{GetDocument, ListDocument, PrefixDocument, ValidationPositionDocument};
use crate::input::{BuildContext, ClaimRelationDocument, InputError, ScopeDocument};
use focal_model::ObjectKind;
use focal_wire::{ListFilter, ListRequest, Operation, ReadRequest, SelectionRequest};
use serde::{Deserialize, Serialize};

/// Exact ID remains source-compatible JSON. Filter selection proves uniqueness
/// over a fresh fixed snapshot; it cannot resume from a partial list cursor.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClaimGetDocument {
    #[serde(default)]
    pub id: Option<String>,
    #[serde(default)]
    pub claim: Option<String>,
    #[serde(default)]
    pub source: Option<String>,
    #[serde(default)]
    pub target: Option<String>,
    #[serde(default)]
    pub status: Option<String>,
    #[serde(default)]
    pub action: Option<String>,
    #[serde(default)]
    pub scopes: Vec<ScopeDocument>,
    #[serde(default)]
    pub relations: Vec<ClaimRelationDocument>,
    #[serde(default)]
    pub caused_by: Option<String>,
    #[serde(default)]
    pub created_after: Option<u64>,
    #[serde(default)]
    pub created_through: Option<u64>,
    #[serde(default)]
    pub prefix: Option<PrefixDocument>,
    #[serde(default)]
    pub after: Option<ValidationPositionDocument>,
    #[serde(default = "super::documents::page_items")]
    pub limit: u32,
    #[serde(default = "super::documents::page_visits")]
    pub max_visits: u32,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClaimSelector {
    Exact(ReadRequest),
    Filter(ListRequest),
    Selection(SelectionRequest),
}
impl ClaimSelector {
    pub fn into_operation(self) -> Operation {
        match self {
            Self::Exact(read) => Operation::Read(read),
            Self::Filter(list) => Operation::List(list),
            Self::Selection(list) => Operation::Select(list),
        }
    }
}
impl ClaimGetDocument {
    pub fn build(self, context: &BuildContext) -> Result<ClaimSelector, InputError> {
        let list = ListDocument {
            claim: self.claim,
            source: self.source,
            target: self.target,
            status: self.status,
            action: self.action,
            scopes: self.scopes,
            relations: self.relations,
            caused_by: self.caused_by,
            created_after: self.created_after,
            created_through: self.created_through,
            limit: 2,
            max_visits: self.max_visits,
            ..ListDocument::default()
        }
        .build_operation(ObjectKind::Claim, context)?;
        let filtered = !matches!(&list, Operation::List(query) if query.filter == ListFilter::new(ObjectKind::Claim));
        if self.limit == 0 || self.limit > 1024 || self.after.is_some() {
            return Err(InputError::Invalid(
                "invalid singular claim read bounds or attempt cursor",
            ));
        }
        if let Some(id) = self.id {
            if filtered || self.max_visits != 1024 {
                return Err(InputError::Invalid("choose a claim ID or filters"));
            }
            return GetDocument {
                id,
                prefix: self.prefix,
                after: None,
                limit: self.limit,
            }
            .build(ObjectKind::Claim, context)
            .map(ClaimSelector::Exact);
        }
        if self.limit != 64 || self.prefix.is_some() || !filtered {
            return Err(InputError::Invalid(
                "filtered claim selection requires a fresh prefix and at least one filter",
            ));
        }
        match list {
            Operation::List(query) => Ok(ClaimSelector::Filter(query)),
            Operation::Select(query) => Ok(ClaimSelector::Selection(query)),
            _ => Err(InputError::Invalid("claim selection operation")),
        }
    }
}
