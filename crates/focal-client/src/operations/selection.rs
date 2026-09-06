//! Lower optional authored predicates without changing legacy List bytes.
use super::{ListDocument, PlannedOperation};
use crate::input::*;
use focal_model::*;
use focal_wire::*;

impl ListDocument {
    pub(super) fn has_predicates(&self) -> bool {
        !self.scopes.is_empty()
            || !self.relations.is_empty()
            || self.caused_by.is_some()
            || !self.inputs.is_empty()
            || self.outcome.is_some()
            || self.confidence.is_some()
            || self.created_after.is_some()
            || self.created_through.is_some()
    }
    /// The original list encoding is retained for inputs with no added predicates.
    pub fn build_operation(
        mut self,
        kind: ObjectKind,
        context: &BuildContext,
    ) -> Result<Operation, InputError> {
        context.validate()?;
        if !self.has_predicates() {
            return self.build(kind, context).map(Operation::List);
        }
        if self.scopes.len() > MAX_SELECTION_PREDICATES
            || self.inputs.len() > MAX_SELECTION_PREDICATES
            || self
                .relations
                .len()
                .checked_add(usize::from(self.caused_by.is_some()))
                .is_none_or(|n| n > MAX_SELECTION_PREDICATES)
        {
            return Err(InputError::Capacity);
        }
        let mut predicates = SelectionPredicates::default();
        predicates
            .scopes
            .try_reserve_exact(self.scopes.len())
            .map_err(|_| InputError::Capacity)?;
        for scope in std::mem::take(&mut self.scopes) {
            predicates.scopes.push(Scope {
                kind: parse_scope_kind(&scope.kind)?,
                key: scope.key,
            });
        }
        predicates
            .relations
            .try_reserve_exact(
                self.relations
                    .len()
                    .checked_add(usize::from(self.caused_by.is_some()))
                    .ok_or(InputError::Capacity)?,
            )
            .map_err(|_| InputError::Capacity)?;
        for relation in std::mem::take(&mut self.relations) {
            predicates.relations.push(Relation {
                kind: parse_relation(&relation.kind)?,
                target: target(&relation.target, context)?,
            });
        }
        if let Some(cause) = self.caused_by.take() {
            let cause = target(&cause, context)?;
            if !matches!(
                cause,
                RelationTarget::Root(_)
                    | RelationTarget::Object(ObjectRef {
                        kind: ObjectKind::Claim,
                        ..
                    })
            ) {
                return Err(InputError::Invalid(
                    "caused_by requires claim:ID or root:ID",
                ));
            }
            predicates.relations.push(Relation {
                kind: RelationKind::CausedBy,
                target: cause,
            });
        }
        predicates
            .inputs
            .try_reserve_exact(self.inputs.len())
            .map_err(|_| InputError::Capacity)?;
        for reference in std::mem::take(&mut self.inputs) {
            predicates.inputs.push(ObjectRef {
                ledger: context.ledger,
                kind: parse_object_kind(&reference.kind)?,
                id: ObjectId(parse_id(&reference.id)?),
            });
        }
        predicates.outcome = self
            .outcome
            .take()
            .as_deref()
            .map(parse_outcome)
            .transpose()?;
        predicates.confidence = self
            .confidence
            .take()
            .as_deref()
            .map(parse_confidence)
            .transpose()?;
        predicates.created_after = self.created_after.take().map(SessionSeq);
        predicates.created_through = self.created_through.take().map(SessionSeq);
        predicates
            .validate(context.ledger, kind)
            .map_err(|error| match error {
                AccessError::Capacity => InputError::Capacity,
                _ => InputError::Invalid("unsupported or invalid list predicate"),
            })?;
        Ok(Operation::Select(SelectionRequest {
            query: self.build(kind, context)?,
            predicates,
        }))
    }
}
fn target(value: &str, context: &BuildContext) -> Result<RelationTarget, InputError> {
    let (kind, value) = value
        .split_once(':')
        .ok_or(InputError::Invalid("relation target needs a typed prefix"))?;
    Ok(match kind {
        "participant" => RelationTarget::Participant(resolve_participant(value, context)?),
        "root" => RelationTarget::Root(RootCommandId(parse_id(value)?)),
        "action" => RelationTarget::Action(parse_action(value)?),
        _ => RelationTarget::Object(ObjectRef {
            ledger: context.ledger,
            kind: parse_object_kind(kind)?,
            id: ObjectId(parse_id(value)?),
        }),
    })
}
pub(super) fn planned(operation: Operation) -> Result<PlannedOperation, InputError> {
    match operation {
        Operation::List(query) => Ok(PlannedOperation::List(query)),
        Operation::Select(query) => Ok(PlannedOperation::Selection(query)),
        _ => Err(InputError::Invalid("not a list operation")),
    }
}

pub(super) fn extend_schema(
    definitions: &mut serde_json::Map<String, serde_json::Value>,
) -> Result<(), InputError> {
    use serde_json::json;
    // Reuse the authored relation vocabulary, but query targets have explicit types.
    let relation_kind = json!({"enum":["issuer","subject","evaluator","claim_action","supersedes","depends_on","awaits","caused_by","refines","conflicts_with","derived_from","reviews","amends","contributed_by","invalidates"]});
    let fields = definitions
        .get_mut("list")
        .and_then(|v| v.get_mut("properties"))
        .and_then(serde_json::Value::as_object_mut)
        .ok_or(InputError::Invalid("list schema"))?;
    fields.insert(
        "scopes".into(),
        json!({"type":"array","maxItems":16,"default":[],"items":{"$ref":"#/$defs/scope"}}),
    );
    fields.insert("relations".into(),json!({"type":"array","maxItems":16,"default":[],"items":{"type":"object","additionalProperties":false,"required":["kind","target"],"properties":{"kind":relation_kind,"target":{"type":"string","maxLength":80,"pattern":"^(?:(?:claim|testament|artifact|validation|root):[0-9a-fA-F]{32}|participant:(?:self|[0-9a-fA-F]{32})|action:(?:work|consultation|challenge|feedback|approval|summon|handoff|evaluation|correction|teardown))$"}}}}));
    fields.insert(
        "caused_by".into(),
        json!({"type":["string","null"],"pattern":"^(claim|root):[0-9a-fA-F]{32}$"}),
    );
    fields.insert("inputs".into(),json!({"type":"array","maxItems":16,"default":[],"items":{"$ref":"#/$defs/object_reference"}}));
    fields.insert(
        "outcome".into(),
        json!({"enum":["complete","partial","refused","impossible","interrupted","failed",null]}),
    );
    fields.insert(
        "confidence".into(),
        json!({"enum":["hint","tentative","committed","consensus",null]}),
    );
    for name in ["created_after", "created_through"] {
        fields.insert(
            name.into(),
            json!({"anyOf":[{"$ref":"#/$defs/u64"},{"type":"null"}]}),
        );
    }
    Ok(())
}

pub(super) fn extend_claim_get(
    schema: &mut serde_json::Map<String, serde_json::Value>,
) -> Result<(), InputError> {
    use serde_json::{Value, json};
    let choices = schema
        .get_mut("oneOf")
        .and_then(Value::as_array_mut)
        .ok_or(InputError::Invalid("claim selector schema"))?;
    let exact = choices
        .get_mut(0)
        .and_then(|v| v.get_mut("properties"))
        .and_then(Value::as_object_mut)
        .ok_or(InputError::Invalid("claim exact schema"))?;
    for name in ["scopes", "relations"] {
        exact.insert(name.into(), json!({"type":"array","maxItems":0}));
    }
    for name in ["caused_by", "created_after", "created_through"] {
        exact.insert(name.into(), json!({"type":"null"}));
    }
    let filtered = choices
        .get_mut(1)
        .and_then(|v| v.get_mut("anyOf"))
        .and_then(Value::as_array_mut)
        .ok_or(InputError::Invalid("claim filter schema"))?;
    for name in ["scopes", "relations"] {
        filtered.push(json!({"required":[name],"properties":{name:{"type":"array","minItems":1}}}));
    }
    filtered.push(json!({"required":["caused_by"],"properties":{"caused_by":{"type":"string"}}}));
    for name in ["created_after", "created_through"] {
        filtered.push(json!({"required":[name],"properties":{name:{"type":"integer"}}}));
    }
    schema.insert("description".into(),Value::String("Supply exact id or nonempty optional claim filters (including scopes, typed relations/cause and creation bounds), never both. Filtered reads prove uniqueness at one fresh fixed prefix; at most 64 pages and 30 seconds. prefix applies only to exact id; limit keeps exact-read compatibility. max_visits bounds each filtered page.".into()));
    Ok(())
}
