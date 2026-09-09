//! Optional predicates over values at one retained list prefix. The old
//! ListFilter/ListRequest encoding remains unchanged.
use crate::*;
use focal_model::*;
use serde::{Deserialize, Serialize};

pub const MAX_SELECTION_PREDICATES: usize = 16;
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SelectionPredicates {
    pub scopes: Vec<Scope>,
    pub relations: Vec<Relation>,
    pub inputs: Vec<ObjectRef>,
    pub outcome: Option<OutcomeKind>,
    pub confidence: Option<Confidence>,
    /// Exclusive lower and inclusive upper creation SessionSeq bounds.
    pub created_after: Option<SessionSeq>,
    pub created_through: Option<SessionSeq>,
}
impl SelectionPredicates {
    pub fn is_empty(&self) -> bool {
        self.scopes.is_empty()
            && self.relations.is_empty()
            && self.inputs.is_empty()
            && self.outcome.is_none()
            && self.confidence.is_none()
            && self.created_after.is_none()
            && self.created_through.is_none()
    }
    pub fn validate(&self, ledger: LedgerId, kind: ObjectKind) -> Result<(), AccessError> {
        if self.scopes.len() > MAX_SELECTION_PREDICATES
            || self.relations.len() > MAX_SELECTION_PREDICATES
            || self.inputs.len() > MAX_SELECTION_PREDICATES
            || self.scopes.iter().any(|scope| scope.key.len() > 16 * 1024)
        {
            return Err(AccessError::Capacity);
        }
        if ((!self.scopes.is_empty() || !self.relations.is_empty()) && kind != ObjectKind::Claim)
            || (!self.inputs.is_empty() && kind != ObjectKind::Artifact)
            || ((self.outcome.is_some() || self.confidence.is_some())
                && kind != ObjectKind::Testament)
            || self
                .scopes
                .iter()
                .any(|scope| scope.key.trim().is_empty() || scope.key.contains('\0'))
            || self
                .inputs
                .iter()
                .any(|reference| reference.ledger != ledger || reference.id.is_zero())
            || self
                .relations
                .iter()
                .any(|relation| match &relation.target {
                    RelationTarget::Participant(id) => id.is_zero(),
                    RelationTarget::Root(id) => id.is_zero(),
                    RelationTarget::Object(reference) => {
                        reference.ledger != ledger || reference.id.is_zero()
                    }
                    RelationTarget::Action(_) => false,
                    // A zero hash means any committed hash of the artifact.
                    RelationTarget::Evidence(evidence) => evidence.id.is_zero(),
                })
            || matches!((self.created_after,self.created_through),(Some(after),Some(through)) if after>=through)
        {
            return Err(AccessError::InvalidRequest);
        }
        Ok(())
    }
    pub fn created(&self, sequence: SessionSeq) -> bool {
        self.created_after.is_none_or(|after| sequence > after)
            && self
                .created_through
                .is_none_or(|through| sequence <= through)
    }
    pub fn claim(&self, value: &Claim) -> bool {
        self.created(value.lifecycle().created)
            && self
                .scopes
                .iter()
                .all(|scope| value.content().scopes.contains(scope))
            && self
                .relations
                .iter()
                .all(|relation| value.content().relations.contains(relation))
    }
    pub fn testament(&self, value: &Testament) -> bool {
        self.created(value.lifecycle().created)
            && self
                .outcome
                .is_none_or(|outcome| value.content().outcome == outcome)
            && self
                .confidence
                .is_none_or(|confidence| value.content().confidence == confidence)
    }
    pub fn artifact(&self, value: &Artifact) -> bool {
        self.created(value.lifecycle().created)
            && self
                .inputs
                .iter()
                .all(|reference| value.content().inputs.contains(reference))
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SelectionRequest {
    pub query: ListRequest,
    pub predicates: SelectionPredicates,
}
impl SelectionRequest {
    pub fn validate(&self, ledger: LedgerId, limits: &WireLimits) -> Result<(), AccessError> {
        self.query.filter.validate()?;
        self.predicates.validate(ledger, self.query.filter.kind)?;
        if self.query.max_items == 0
            || self.query.max_items > limits.max_items
            || self.query.max_visits == 0
            || self.query.max_visits > limits.max_items
            || self.query.cursor.as_ref().is_some_and(|cursor| {
                cursor.bytes.is_empty() || cursor.bytes.len() > MAX_LIST_CURSOR_BYTES
            })
        {
            return Err(AccessError::Capacity);
        }
        Ok(())
    }
    /// Check every predicate observable in the returned canonical object. Claim
    /// membership and testament manifest joins for artifacts remain owner reads.
    pub(crate) fn matches_at(&self, object: &ReadObject, prefix: SessionSeq) -> bool {
        let created = match object {
            ReadObject::Claim { value, .. } => value.lifecycle().created,
            ReadObject::Testament { value, .. } => value.lifecycle().created,
            ReadObject::Artifact { value, .. } => value.lifecycle().created,
            ReadObject::Validation { value, .. } => value.lifecycle().created,
            ReadObject::ValidationResults { .. } => return false,
        };
        created <= prefix && self.matches(object)
    }
    pub fn matches(&self, object: &ReadObject) -> bool {
        let filter = &self.query.filter;
        match object {
            ReadObject::Claim { id, value } => {
                filter.kind == ObjectKind::Claim
                    && filter.claim.is_none_or(|claim| claim == *id)
                    && filter
                        .source
                        .is_none_or(|source| value.content().issuer() == Some(source))
                    && filter
                        .target
                        .is_none_or(|target| value.content().subject() == Some(target))
                    && filter
                        .status
                        .is_none_or(|status| value.lifecycle().status == status)
                    && filter
                        .action
                        .is_none_or(|action| value.content().action() == Some(action))
                    && self.predicates.claim(value)
            }
            ReadObject::Testament { value, .. } => {
                filter.kind == ObjectKind::Testament
                    && filter
                        .claim
                        .is_none_or(|claim| value.content().claim == claim)
                    && self.predicates.testament(value)
            }
            ReadObject::Artifact { value, .. } => {
                filter.kind == ObjectKind::Artifact
                    && filter
                        .producer
                        .is_none_or(|producer| value.content().producer == producer)
                    && filter
                        .artifact_kind
                        .as_ref()
                        .is_none_or(|kind| &value.content().kind == kind)
                    && filter
                        .schema
                        .is_none_or(|schema| value.content().schema_hash == schema)
                    && self.predicates.artifact(value)
            }
            ReadObject::Validation { value, .. } => {
                filter.kind == ObjectKind::Validation
                    && filter
                        .claim
                        .is_none_or(|claim| value.content().claim == claim)
                    && filter
                        .evaluator
                        .is_none_or(|evaluator| value.content().evaluator == evaluator)
                    && filter
                        .validation_kind
                        .is_none_or(|kind| value.content().kind == kind)
                    && filter
                        .phase
                        .is_none_or(|phase| value.content().phase == phase)
                    && filter.mode.is_none_or(|mode| value.content().mode == mode)
                    && self.predicates.created(value.lifecycle().created)
            }
            ReadObject::ValidationResults { .. } => false,
        }
    }
}
pub fn selection_request(operation: &Operation) -> Option<&ListRequest> {
    match operation {
        Operation::List(query) => Some(query),
        Operation::Select(selection) => Some(&selection.query),
        _ => None,
    }
}
pub fn selection_request_mut(operation: &mut Operation) -> Option<&mut ListRequest> {
    match operation {
        Operation::List(query) => Some(query),
        Operation::Select(selection) => Some(&mut selection.query),
        _ => None,
    }
}
pub fn selection_scope(
    peer: &AuthenticatedPeer,
    ledger: LedgerId,
    request: &SelectionRequest,
) -> Result<ContentHash, AccessError> {
    let base = list_scope(peer, ledger, &request.query.filter)?;
    request
        .predicates
        .validate(ledger, request.query.filter.kind)?;
    let bytes = postcard::to_stdvec(&(
        base,
        &request.predicates,
        request.query.max_items,
        request.query.max_visits,
    ))
    .map_err(|_| AccessError::Capacity)?;
    Ok(ContentHash(blake3::derive_key(
        "focal.list.selection-scope.v1",
        &bytes,
    )))
}

#[cfg(test)]
#[path = "selection_tests.rs"]
mod tests;
