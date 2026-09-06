use crate::*;
use std::collections::{BTreeMap, BTreeSet};

/// Conservative heap charge for the current closed model schema. The 64x factor
/// bounds short encoded integers, vector spare capacity, tree nodes, padding and
/// per-container allocation bookkeeping. It intentionally overcharges the serial
/// reference copies and must be revisited when model allocation shapes change.
/// Serialization's size pass does not allocate a payload buffer.
pub fn reference_charge<T: Serialize + ?Sized>(value: &T) -> Result<usize, GraphError> {
    postcard::experimental::serialized_size(value)?
        .checked_mul(64)
        .and_then(|bytes| bytes.checked_add(4096))
        .ok_or(GraphError::Overflow)
}
type Rows = BTreeMap<GraphKey, Entry<GraphKey, GraphValue>>;
fn put(rows: &mut Rows, key: GraphKey, value: GraphValue) -> Result<(), GraphError> {
    let heap = reference_charge(&(&key, &value))?;
    rows.insert(key.clone(), Entry::new(key, value, heap));
    Ok(())
}
fn reference(ledger: LedgerId, kind: ObjectKind, id: [u8; 16]) -> ObjectRef {
    ObjectRef {
        ledger,
        kind,
        id: ObjectId(id),
    }
}
fn edge(
    rows: &mut Rows,
    source: ObjectRef,
    target: RelationTarget,
    relation: GraphRelation,
    introduced: SessionSeq,
) -> Result<(), GraphError> {
    let value = GraphValue::Edge(GraphEdge {
        source,
        target: target.clone(),
        relation,
        introduced,
    });
    put(
        rows,
        GraphKey::Forward(source, target.clone(), relation),
        value.clone(),
    )?;
    put(rows, GraphKey::Reverse(target, source, relation), value)
}
fn object(
    rows: &mut Rows,
    reference: ObjectRef,
    value: GraphObject,
    _hash: ContentHash,
    claim: Option<ClaimId>,
) -> Result<(), GraphError> {
    put(
        rows,
        GraphKey::object(reference),
        GraphValue::Object(Box::new(value)),
    )?;
    if let Some(claim) = claim {
        put(
            rows,
            GraphKey::ByClaim(claim, reference.kind, reference.id),
            GraphValue::Reference(reference),
        )?;
    }
    Ok(())
}
fn claim(rows: &mut Rows, id: ClaimId, c: &Claim) -> Result<(), GraphError> {
    let r = reference(c.content().ledger, ObjectKind::Claim, id.0);
    object(
        rows,
        r,
        GraphObject::Claim(c.clone()),
        c.content_hash(),
        Some(id),
    )?;
    put(
        rows,
        GraphKey::Lifecycle(c.lifecycle().status, id),
        GraphValue::Reference(r),
    )?;
    if let Some(d) = c
        .content()
        .deadline
        .filter(|_| c.lifecycle().status.is_active())
    {
        put(
            rows,
            GraphKey::Deadline(d.at, id, d.timer, d.generation),
            GraphValue::Reference(r),
        )?;
    }
    for relation in &c.content().relations {
        edge(
            rows,
            r,
            relation.target.clone(),
            GraphRelation::Authored(relation.kind),
            c.lifecycle().created,
        )?;
    }
    for requirement in &c.content().requirements {
        edge(
            rows,
            r,
            RelationTarget::Object(reference(
                r.ledger,
                ObjectKind::Validation,
                requirement.id.0,
            )),
            GraphRelation::Requirement,
            c.lifecycle().created,
        )?;
    }
    Ok(())
}
fn testament(rows: &mut Rows, id: TestamentId, t: &Testament) -> Result<(), GraphError> {
    let r = reference(t.content().ledger, ObjectKind::Testament, id.0);
    object(
        rows,
        r,
        GraphObject::Testament(t.clone()),
        t.content_hash(),
        Some(t.content().claim),
    )?;
    edge(
        rows,
        r,
        RelationTarget::Object(ObjectRef::claim(r.ledger, t.content().claim)),
        GraphRelation::TestamentOf,
        t.lifecycle().created,
    )?;
    for artifact in &t.content().artifacts {
        edge(
            rows,
            r,
            RelationTarget::Object(reference(r.ledger, ObjectKind::Artifact, artifact.id.0)),
            GraphRelation::Evidence,
            t.lifecycle().created,
        )?;
    }
    Ok(())
}
fn validation(rows: &mut Rows, id: ValidationId, v: &Validation) -> Result<(), GraphError> {
    let r = reference(v.content().ledger, ObjectKind::Validation, id.0);
    object(
        rows,
        r,
        GraphObject::Validation(v.clone()),
        v.content_hash(),
        Some(v.content().claim),
    )?;
    edge(
        rows,
        r,
        RelationTarget::Object(ObjectRef::claim(r.ledger, v.content().claim)),
        GraphRelation::ValidationOf,
        v.lifecycle().created,
    )?;
    edge(
        rows,
        r,
        RelationTarget::Participant(v.content().evaluator),
        GraphRelation::Authored(RelationKind::Evaluator),
        v.lifecycle().created,
    )?;
    for contributor in &v.content().contributed_by {
        edge(
            rows,
            r,
            RelationTarget::Participant(*contributor),
            GraphRelation::Authored(RelationKind::ContributedBy),
            v.lifecycle().created,
        )?;
    }
    if v.content().mode == ValidationMode::Required {
        put(
            rows,
            GraphKey::Required(v.content().claim, v.content().phase, id),
            GraphValue::Reference(r),
        )?;
    }
    Ok(())
}
fn artifact(rows: &mut Rows, id: ArtifactId, a: &Artifact) -> Result<(), GraphError> {
    let r = reference(a.content().ledger, ObjectKind::Artifact, id.0);
    object(
        rows,
        r,
        GraphObject::Artifact(a.clone()),
        a.content_hash(),
        None,
    )?;
    for input in &a.content().inputs {
        edge(
            rows,
            r,
            RelationTarget::Object(*input),
            GraphRelation::ArtifactInput,
            a.lifecycle().created,
        )?;
    }
    Ok(())
}
fn evidence(rows: &mut Rows, ledger: LedgerId, set: &EvidenceSet) -> Result<(), GraphError> {
    for artifact in &set.artifacts {
        let r = reference(ledger, ObjectKind::Artifact, artifact.id.0);
        put(
            rows,
            GraphKey::ByClaim(set.claim, ObjectKind::Artifact, r.id),
            GraphValue::Reference(r),
        )?;
    }
    Ok(())
}
pub(crate) fn project(state: &State) -> Result<Rows, GraphError> {
    let mut rows = Rows::new();
    for (id, c) in &state.claims {
        claim(&mut rows, *id, c)?
    }
    for (id, t) in &state.testaments {
        testament(&mut rows, *id, t)?
    }
    for (id, v) in &state.validations {
        validation(&mut rows, *id, v)?
    }
    for (id, a) in &state.artifacts {
        artifact(&mut rows, *id, a)?
    }
    for set in state.evidence_sets.values() {
        evidence(&mut rows, state.ledger, set)?
    }
    for ((kind, hash), id) in &state.identities {
        put(
            &mut rows,
            GraphKey::Identity(*kind, *hash),
            GraphValue::Reference(ObjectRef {
                ledger: state.ledger,
                kind: *kind,
                id: *id,
            }),
        )?;
    }
    Ok(rows)
}
fn changed<K: Ord + Copy, V: PartialEq>(
    before: &BTreeMap<K, V>,
    after: &BTreeMap<K, V>,
) -> BTreeSet<K> {
    before
        .keys()
        .chain(after.keys())
        .filter(|key| before.get(key) != after.get(key))
        .copied()
        .collect()
}
pub(crate) fn changes(
    before: &State,
    after: &State,
) -> Result<Vec<Change<GraphKey, GraphValue>>, GraphError> {
    let mut old = Rows::new();
    let mut new = Rows::new();
    for id in changed(&before.claims, &after.claims) {
        if let Some(c) = before.claims.get(&id) {
            claim(&mut old, id, c)?
        }
        if let Some(c) = after.claims.get(&id) {
            claim(&mut new, id, c)?
        }
    }
    for id in changed(&before.testaments, &after.testaments) {
        if let Some(t) = before.testaments.get(&id) {
            testament(&mut old, id, t)?
        }
        if let Some(t) = after.testaments.get(&id) {
            testament(&mut new, id, t)?
        }
    }
    for id in changed(&before.validations, &after.validations) {
        if let Some(v) = before.validations.get(&id) {
            validation(&mut old, id, v)?
        }
        if let Some(v) = after.validations.get(&id) {
            validation(&mut new, id, v)?
        }
    }
    for id in changed(&before.artifacts, &after.artifacts) {
        if let Some(a) = before.artifacts.get(&id) {
            artifact(&mut old, id, a)?
        }
        if let Some(a) = after.artifacts.get(&id) {
            artifact(&mut new, id, a)?
        }
    }
    for id in changed(&before.evidence_sets, &after.evidence_sets) {
        if let Some(e) = before.evidence_sets.get(&id) {
            evidence(&mut old, before.ledger, e)?
        }
        if let Some(e) = after.evidence_sets.get(&id) {
            evidence(&mut new, after.ledger, e)?
        }
    }
    for (kind, hash) in changed(&before.identities, &after.identities) {
        if let Some(id) = before.identities.get(&(kind, hash)) {
            put(
                &mut old,
                GraphKey::Identity(kind, hash),
                GraphValue::Reference(ObjectRef {
                    ledger: before.ledger,
                    kind,
                    id: *id,
                }),
            )?;
        }
        if let Some(id) = after.identities.get(&(kind, hash)) {
            put(
                &mut new,
                GraphKey::Identity(kind, hash),
                GraphValue::Reference(ObjectRef {
                    ledger: after.ledger,
                    kind,
                    id: *id,
                }),
            )?;
        }
    }
    difference(old, new)
}
fn difference(old: Rows, new: Rows) -> Result<Vec<Change<GraphKey, GraphValue>>, GraphError> {
    let mut changes = Vec::new();
    for (key, entry) in &old {
        if new.get(key) == Some(entry) {
            continue;
        }
        if !new.contains_key(key) {
            changes.push(Change::Delete(key.clone()))
        }
    }
    for (key, entry) in new {
        if old.get(&key) != Some(&entry) {
            changes.push(Change::Put(entry))
        }
    }
    Ok(changes)
}

pub(crate) fn patch_charge(
    before: focal_core::CoreView<'_>,
    patch: focal_core::RowPatch<'_>,
) -> Result<usize, GraphError> {
    let mut bytes = reference_charge(&patch)?
        .checked_mul(2)
        .ok_or(GraphError::Overflow)?;
    macro_rules! old {
        ($rows:ident, $get:ident) => {
            for (id, _) in patch.$rows() {
                if let Some(value) = before.$get(id) {
                    bytes = bytes
                        .checked_add(
                            reference_charge(value)?
                                .checked_mul(2)
                                .ok_or(GraphError::Overflow)?,
                        )
                        .ok_or(GraphError::Overflow)?;
                }
            }
        };
    }
    old!(claims, claim);
    old!(testaments, testament);
    old!(validations, validation);
    old!(artifacts, artifact);
    old!(evidence_sets, evidence_set);
    old!(identities, identity);
    Ok(bytes)
}
pub(crate) fn patch_changes(
    before: focal_core::CoreView<'_>,
    patch: focal_core::RowPatch<'_>,
) -> Result<Vec<Change<GraphKey, GraphValue>>, GraphError> {
    let mut old = Rows::new();
    let mut new = Rows::new();
    for (id, value) in patch.claims() {
        if let Some(previous) = before.claim(id) {
            claim(&mut old, *id, previous)?;
        }
        claim(&mut new, *id, value)?;
    }
    for (id, value) in patch.testaments() {
        if let Some(previous) = before.testament(id) {
            testament(&mut old, *id, previous)?;
        }
        testament(&mut new, *id, value)?;
    }
    for (id, value) in patch.validations() {
        if let Some(previous) = before.validation(id) {
            validation(&mut old, *id, previous)?;
        }
        validation(&mut new, *id, value)?;
    }
    for (id, value) in patch.artifacts() {
        if let Some(previous) = before.artifact(id) {
            artifact(&mut old, *id, previous)?;
        }
        artifact(&mut new, *id, value)?;
    }
    for (id, value) in patch.evidence_sets() {
        if let Some(previous) = before.evidence_set(id) {
            evidence(&mut old, before.ledger(), previous)?;
        }
        evidence(&mut new, before.ledger(), value)?;
    }
    for ((kind, hash), id) in patch.identities() {
        if let Some(previous) = before.identity(&(*kind, *hash)) {
            put(
                &mut old,
                GraphKey::Identity(*kind, *hash),
                GraphValue::Reference(ObjectRef {
                    ledger: before.ledger(),
                    kind: *kind,
                    id: *previous,
                }),
            )?;
        }
        put(
            &mut new,
            GraphKey::Identity(*kind, *hash),
            GraphValue::Reference(ObjectRef {
                ledger: before.ledger(),
                kind: *kind,
                id: *id,
            }),
        )?;
    }
    difference(old, new)
}
