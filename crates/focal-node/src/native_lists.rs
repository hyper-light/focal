//! Bounded lists over the committed native prefix (doc 22 §7). The node
//! selects the most selective indexed predicate of a filter and walks its
//! key range; every other predicate filters residually within the caller's
//! visit allowance, so a page costs `O(visited)` and never scans a family it
//! did not index. A page may be empty and still continue: the cursor names
//! the last visited row, not the last match, and only an absent cursor ends
//! the list.
//!
//! Cursors are stateless. A continuation carries the resume position and a
//! keyed digest over the ledger, principal, route epoch and exact filter, so
//! a cursor reused under another filter, principal or route is refused rather
//! than silently repositioned. Index rows are immutable except the status
//! family, whose old key is deleted when a claim moves, so a cursor stays
//! valid while the prefix grows: a later page simply sees later rows.
use crate::{host::access, native_documents as docs, native_reads::Reader};
use focal_core::native::{
    EvaluationKey, NativeIndexHit, NativeIndexScan, NativeResultKey, NativeState,
    artifact_kind_hash, scope_key_hash,
};
use focal_ledger::{Core, Session};
use focal_model::lifecycle::validation::{Declaration, PhasePolicyView, ProgramView};
use focal_model::*;
use focal_wire::*;
use serde::{Deserialize, Serialize};

/// Cursor layout version; a changed layout refuses old cursors.
const CURSOR_VERSION: u8 = 1;
const MAC_BYTES: usize = 32;
const CURSOR_DOMAIN: &[u8] = b"focal.native.list-cursor.v1";

/// Where the next page resumes: after the last visited row of the scan the
/// filter selects. The kind must match the scan, so a cursor made under a
/// filter that selects another scan is refused.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
enum Position {
    Claim(ClaimId),
    Created {
        sequence: SessionSeq,
        id: ObjectId,
    },
    Artifact(ArtifactId),
    Definition(ValidationId),
    Result(NativeResultRef),
    Evaluation(NativeEvaluationKey),
    Receipt(ReceiptId),
    Monitor(MonitorId),
    /// Responses whose cycle is below this one remain.
    Cycle(u32),
    Event {
        sequence: SessionSeq,
        ordinal: u32,
    },
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
struct Cursor {
    version: u8,
    position: Position,
}

fn filter_bytes(filter: &NativeListFilter) -> Result<Vec<u8>, AccessError> {
    postcard::to_stdvec(filter).map_err(|_| AccessError::InvalidRequest)
}
fn mac(reader: &Reader<'_>, key: &[u8; 32], filter: &[u8], body: &[u8]) -> blake3::Hash {
    let mut hasher = blake3::Hasher::new_keyed(key);
    hasher.update(CURSOR_DOMAIN);
    hasher.update(&reader.ledger.tenant.0);
    hasher.update(&reader.ledger.session.0);
    hasher.update(&reader.principal.0);
    hasher.update(&reader.route.0.to_le_bytes());
    hasher.update(&(filter.len() as u64).to_le_bytes());
    hasher.update(filter);
    hasher.update(body);
    hasher.finalize()
}
fn encode(
    reader: &Reader<'_>,
    key: &[u8; 32],
    filter: &[u8],
    position: Position,
) -> Result<NativeListCursor, AccessError> {
    let cursor = Cursor {
        version: CURSOR_VERSION,
        position,
    };
    let mut bytes = postcard::to_stdvec(&cursor).map_err(|_| AccessError::InvalidRequest)?;
    if bytes
        .len()
        .checked_add(MAC_BYTES)
        .is_none_or(|len| len > MAX_NATIVE_LIST_CURSOR_BYTES)
    {
        return Err(AccessError::Capacity);
    }
    let digest = mac(reader, key, filter, &bytes);
    bytes
        .try_reserve_exact(MAC_BYTES)
        .map_err(|_| AccessError::Capacity)?;
    bytes.extend_from_slice(digest.as_bytes());
    Ok(NativeListCursor(bytes))
}
fn decode(
    reader: &Reader<'_>,
    key: &[u8; 32],
    filter: &[u8],
    cursor: &NativeListCursor,
) -> Result<Position, AccessError> {
    if cursor.0.len() > MAX_NATIVE_LIST_CURSOR_BYTES {
        return Err(AccessError::InvalidRequest);
    }
    let split = cursor
        .0
        .len()
        .checked_sub(MAC_BYTES)
        .ok_or(AccessError::InvalidRequest)?;
    let body = cursor.0.get(..split).ok_or(AccessError::InvalidRequest)?;
    let digest: [u8; 32] = cursor
        .0
        .get(split..)
        .ok_or(AccessError::InvalidRequest)?
        .try_into()
        .map_err(|_| AccessError::InvalidRequest)?;
    // Constant-time comparison through the digest type.
    if blake3::Hash::from_bytes(digest) != mac(reader, key, filter, body) {
        return Err(AccessError::InvalidRequest);
    }
    let (decoded, remaining): (Cursor, &[u8]) =
        postcard::take_from_bytes(body).map_err(|_| AccessError::InvalidRequest)?;
    if !remaining.is_empty() || decoded.version != CURSOR_VERSION {
        return Err(AccessError::InvalidRequest);
    }
    Ok(decoded.position)
}

/// The bounded walk of one page: items are consumed until the visit allowance
/// or the page is full; the resume position is the last visited item.
struct Walk {
    objects: Vec<NativeObject>,
    visited: u32,
    max_items: u32,
    max_visits: u32,
    next: Option<Position>,
}
impl Walk {
    fn new(request: &NativeListRequest) -> Result<Self, AccessError> {
        let mut objects = Vec::new();
        objects
            .try_reserve_exact(request.max_items as usize)
            .map_err(|_| AccessError::Capacity)?;
        Ok(Self {
            objects,
            visited: 0,
            max_items: request.max_items,
            max_visits: request.max_visits,
            next: None,
        })
    }
    /// Consume `items` in order. `accept` returns the object an item yields
    /// once every residual predicate holds, or nothing for a visited miss.
    fn run<T>(
        &mut self,
        items: impl Iterator<Item = T>,
        position: impl Fn(&T) -> Position,
        mut accept: impl FnMut(T) -> Result<Option<NativeObject>, AccessError>,
    ) -> Result<(), AccessError> {
        let mut last = None;
        for item in items {
            if self.visited >= self.max_visits {
                self.next = last;
                return Ok(());
            }
            self.visited = self.visited.saturating_add(1);
            last = Some(position(&item));
            if let Some(object) = accept(item)? {
                self.objects.push(object);
                if self.objects.len() >= self.max_items as usize {
                    self.next = last;
                    return Ok(());
                }
            }
        }
        self.next = None;
        Ok(())
    }
}

/// The principals a declaration designates: the issuer of a delivery
/// program, otherwise its check evaluator and any quality evaluator. This is
/// exactly the set the evaluator index family carries (doc 22 §7).
fn evaluators(declaration: &Declaration) -> (ParticipantId, Option<ParticipantId>) {
    match declaration.program() {
        ProgramView::Delivery => (declaration.issuer(), None),
        ProgramView::Programmatic { check, quality } => {
            (check.evaluator(), quality.map(PhasePolicyView::evaluator))
        }
        ProgramView::Agentic { check } => (check.evaluator(), None),
    }
}
fn designates(declaration: &Declaration, evaluator: ParticipantId) -> bool {
    let (first, second) = evaluators(declaration);
    first == evaluator || second == Some(evaluator)
}

fn wrong_position() -> AccessError {
    AccessError::InvalidRequest
}

/// The residual predicates of a claim list, beside the indexed one.
struct ClaimPredicates<'a> {
    issuer: Option<ParticipantId>,
    subject: Option<ParticipantId>,
    status: Option<ClaimStatus>,
    action: Option<ActionType>,
    scope: Option<&'a Scope>,
    relation: Option<&'a Relation>,
    created_after: Option<SessionSeq>,
}
impl ClaimPredicates<'_> {
    fn holds(&self, core: &Core<NativeState>, id: ClaimId) -> bool {
        let Some(state) = core.native_claim(id) else {
            return false;
        };
        if self.issuer.is_some_and(|issuer| state.issuer() != issuer)
            || self
                .subject
                .is_some_and(|subject| state.subject() != subject)
            || self.status.is_some_and(|status| state.status() != status)
            || self
                .created_after
                .is_some_and(|after| state.created() <= after)
        {
            return false;
        }
        if self.action.is_none() && self.scope.is_none() && self.relation.is_none() {
            return true;
        }
        let Some(content) = core.native_claim_content(id) else {
            return false;
        };
        !(self.action.is_some_and(|action| content.action() != action)
            || self.scope.is_some_and(|scope| {
                !content
                    .scopes()
                    .any(|found| found.kind == scope.kind && found.key == scope.key)
            })
            || self.relation.is_some_and(|relation| {
                !content.relations().iter().any(|found| {
                    found.kind == relation.kind
                        && match (&found.target, &relation.target) {
                            // A filter on evidence names the artifact; a zero
                            // hash matches any committed hash of it.
                            (RelationTarget::Evidence(found), RelationTarget::Evidence(wanted)) => {
                                found.id == wanted.id
                                    && (wanted.hash.0 == [0; 32] || found.hash == wanted.hash)
                            }
                            (found, wanted) => found == wanted,
                        }
                })
            }))
    }
    /// The most selective indexed predicate: an exact relation or scope, then
    /// a participant, then the action or status family, then creation order.
    fn scan(&self) -> Result<Option<NativeIndexScan>, AccessError> {
        Ok(if let Some(relation) = self.relation {
            let target = match &relation.target {
                RelationTarget::Object(target) if target.kind == ObjectKind::Claim => {
                    ClaimId(target.id.0)
                }
                RelationTarget::Evidence(evidence) => ClaimId(evidence.id.0),
                _ => return Err(AccessError::InvalidRequest),
            };
            Some(NativeIndexScan::Relation {
                kind: relation.kind,
                target,
            })
        } else if let Some(scope) = self.scope {
            Some(NativeIndexScan::Scope {
                kind: scope.kind,
                key: scope_key_hash(&scope.key),
            })
        } else if let Some(issuer) = self.issuer {
            Some(NativeIndexScan::Issuer(issuer))
        } else if let Some(subject) = self.subject {
            Some(NativeIndexScan::Subject(subject))
        } else if let Some(action) = self.action {
            Some(NativeIndexScan::Action(action))
        } else if let Some(status) = self.status {
            Some(NativeIndexScan::Status(status))
        } else {
            self.created_after.map(|after| NativeIndexScan::Created {
                family: ObjectKind::Claim,
                after,
            })
        })
    }
}

fn claims(
    reader: &Reader<'_>,
    walk: &mut Walk,
    after: Option<Position>,
    predicates: &ClaimPredicates<'_>,
) -> Result<(), AccessError> {
    let core = reader.core;
    let accept = |id: ClaimId| -> Result<Option<NativeObject>, AccessError> {
        if !predicates.holds(core, id) {
            return Ok(None);
        }
        let Some(state) = core.native_claim(id) else {
            return Ok(None);
        };
        Ok(Some(NativeObject::Claim(Box::new(docs::claim(
            core,
            state,
            NativeClaimExpand::default(),
        )))))
    };
    match predicates.scan()? {
        Some(scan @ NativeIndexScan::Created { .. }) => {
            let resume = match after {
                None => None,
                Some(Position::Created { sequence, id }) => Some(NativeIndexHit::Created {
                    family: ObjectKind::Claim,
                    sequence,
                    id,
                }),
                Some(_) => return Err(wrong_position()),
            };
            walk.run(
                core.native_index_scan(scan, resume),
                |hit| match hit {
                    NativeIndexHit::Created { sequence, id, .. } => Position::Created {
                        sequence: *sequence,
                        id: *id,
                    },
                    _ => Position::Created {
                        sequence: SessionSeq(0),
                        id: ObjectId([0; 16]),
                    },
                },
                |hit| match hit {
                    NativeIndexHit::Created { id, .. } => accept(ClaimId(id.0)),
                    _ => Ok(None),
                },
            )
        }
        Some(scan) => {
            let resume = match after {
                None => None,
                Some(Position::Claim(id)) => Some(NativeIndexHit::Claim(id)),
                Some(_) => return Err(wrong_position()),
            };
            walk.run(
                core.native_index_scan(scan, resume),
                |hit| match hit {
                    NativeIndexHit::Claim(id) => Position::Claim(*id),
                    _ => Position::Claim(ClaimId([0; 16])),
                },
                |hit| match hit {
                    NativeIndexHit::Claim(id) => accept(id),
                    _ => Ok(None),
                },
            )
        }
        None => {
            let resume = match after {
                None => None,
                Some(Position::Claim(id)) => Some(id),
                Some(_) => return Err(wrong_position()),
            };
            walk.run(
                core.native_claims_from(resume),
                |id| Position::Claim(*id),
                accept,
            )
        }
    }
}

fn artifacts(
    reader: &Reader<'_>,
    walk: &mut Walk,
    after: Option<Position>,
    producer: Option<ParticipantId>,
    kind: Option<&str>,
    schema: Option<ContentHash>,
    input: Option<ObjectId>,
) -> Result<(), AccessError> {
    let core = reader.core;
    let accept = |id: ArtifactId| -> Result<Option<NativeObject>, AccessError> {
        let Some(artifact) = core.native_artifact(id) else {
            return Ok(None);
        };
        let descriptor = artifact.descriptor();
        if producer.is_some_and(|producer| descriptor.producer() != producer)
            || kind.is_some_and(|kind| descriptor.kind() != kind)
            || schema.is_some_and(|schema| descriptor.schema_hash() != schema)
            || input.is_some_and(|input| !descriptor.inputs().iter().any(|found| found.id == input))
        {
            return Ok(None);
        }
        Ok(Some(NativeObject::Artifact(Box::new(docs::artifact(
            artifact,
        )))))
    };
    let scan = if let Some(input) = input {
        Some(NativeIndexScan::ArtifactInput(input))
    } else if let Some(producer) = producer {
        Some(NativeIndexScan::Producer(producer))
    } else if let Some(schema) = schema {
        Some(NativeIndexScan::Schema(schema))
    } else {
        kind.map(|kind| NativeIndexScan::ArtifactKind(artifact_kind_hash(kind)))
    };
    let resume = match after {
        None => None,
        Some(Position::Artifact(id)) => Some(id),
        Some(_) => return Err(wrong_position()),
    };
    match scan {
        Some(scan) => walk.run(
            core.native_index_scan(scan, resume.map(NativeIndexHit::Artifact)),
            |hit| match hit {
                NativeIndexHit::Artifact(id) => Position::Artifact(*id),
                _ => Position::Artifact(ArtifactId([0; 16])),
            },
            |hit| match hit {
                NativeIndexHit::Artifact(id) => accept(id),
                _ => Ok(None),
            },
        ),
        None => walk.run(
            core.native_artifacts_from(resume),
            |id| Position::Artifact(*id),
            accept,
        ),
    }
}

fn definitions(
    reader: &Reader<'_>,
    walk: &mut Walk,
    after: Option<Position>,
    claim: Option<ClaimId>,
    evaluator: Option<ParticipantId>,
) -> Result<(), AccessError> {
    let core = reader.core;
    let accept = |id: ValidationId| -> Result<Option<NativeObject>, AccessError> {
        let Some(declaration) = core.native_definition(id) else {
            return Ok(None);
        };
        if claim.is_some_and(|claim| declaration.claim() != claim)
            || evaluator.is_some_and(|evaluator| !designates(declaration, evaluator))
        {
            return Ok(None);
        }
        Ok(Some(NativeObject::Definition(Box::new(docs::definition(
            core,
            declaration,
        )))))
    };
    let resume = match after {
        None => None,
        Some(Position::Definition(id)) => Some(id),
        Some(_) => return Err(wrong_position()),
    };
    if let Some(claim) = claim {
        // A claim's acceptance policy names its definitions in declaration
        // order from creation on; registrations appear only once evaluation
        // begins, so they cannot serve a list.
        let Some(state) = core.native_claim(claim) else {
            walk.visited = 1;
            return Ok(());
        };
        let ids = state
            .acceptance()
            .declarations()
            .iter()
            .map(|declared| ValidationId(declared.binding().object.0))
            .skip_while(move |id| resume.is_some_and(|resume| *id != resume))
            .skip(usize::from(resume.is_some()));
        return walk.run(ids, |id| Position::Definition(*id), accept);
    }
    match evaluator {
        Some(evaluator) => walk.run(
            core.native_index_scan(
                NativeIndexScan::Evaluator(evaluator),
                resume.map(NativeIndexHit::Definition),
            ),
            |hit| match hit {
                NativeIndexHit::Definition(id) => Position::Definition(*id),
                _ => Position::Definition(ValidationId([0; 16])),
            },
            |hit| match hit {
                NativeIndexHit::Definition(id) => accept(id),
                _ => Ok(None),
            },
        ),
        None => walk.run(
            core.native_definitions_from(resume),
            |id| Position::Definition(*id),
            accept,
        ),
    }
}

fn evaluations(
    reader: &Reader<'_>,
    walk: &mut Walk,
    after: Option<Position>,
    claim: Option<ClaimId>,
    validation: Option<ValidationId>,
    evaluator: Option<ParticipantId>,
    verdict: Option<VerdictValue>,
) -> Result<(), AccessError> {
    let core = reader.core;
    let accept = |key: EvaluationKey| -> Result<Option<NativeObject>, AccessError> {
        if claim.is_some_and(|claim| key.claim != claim)
            || validation.is_some_and(|validation| key.validation != validation)
        {
            return Ok(None);
        }
        let (Some(declaration), Some(state)) = (
            core.native_definition(key.validation),
            core.native_evaluation(key),
        ) else {
            return Ok(None);
        };
        if evaluator.is_some_and(|evaluator| !designates(declaration, evaluator)) {
            return Ok(None);
        }
        Ok(Some(NativeObject::Evaluation(Box::new(docs::evaluation(
            declaration,
            key,
            state,
        )?))))
    };
    if let Some(verdict) = verdict {
        // The verdict family is the most selective: it names accepted
        // results, each of which addresses exactly one evaluation.
        let resume = match after {
            None => None,
            Some(Position::Result(result)) => {
                Some(NativeIndexHit::Result(docs::result_key_of(result)))
            }
            Some(_) => return Err(wrong_position()),
        };
        return walk.run(
            core.native_index_scan(NativeIndexScan::Verdict(verdict), resume),
            |hit| match hit {
                NativeIndexHit::Result(key) => Position::Result(docs::result_ref(*key)),
                _ => Position::Result(docs::result_ref(NativeResultKey {
                    evaluation: EvaluationKey {
                        claim: ClaimId([0; 16]),
                        validation: ValidationId([0; 16]),
                        target: focal_core::native::EvaluationTarget::Admission,
                        generation: 0,
                    },
                    revision: ObjectRevision(0),
                })),
            },
            |hit| match hit {
                NativeIndexHit::Result(key) => accept(key.evaluation),
                _ => Ok(None),
            },
        );
    }
    let resume = match after {
        None => None,
        Some(Position::Evaluation(key)) => Some(docs::evaluation_key_of(key)),
        Some(_) => return Err(wrong_position()),
    };
    // A validation without its claim resolves to the claim it belongs to so
    // the scan stays within one claim's evaluation keys.
    let claim = match (claim, validation) {
        (Some(claim), _) => Some(claim),
        (None, Some(validation)) => match core.native_definition(validation) {
            Some(declaration) => Some(declaration.claim()),
            None => {
                walk.visited = 1;
                return Ok(());
            }
        },
        (None, None) => None,
    };
    walk.run(
        core.native_evaluations_from(claim, resume),
        |key| Position::Evaluation(docs::evaluation_key(*key)),
        accept,
    )
}

fn responses(
    reader: &Reader<'_>,
    walk: &mut Walk,
    after: Option<Position>,
    claim: ClaimId,
) -> Result<(), AccessError> {
    let core = reader.core;
    let below = match after {
        None => None,
        Some(Position::Cycle(cycle)) => Some(cycle),
        Some(_) => return Err(wrong_position()),
    };
    let Some(state) = core.native_claim(claim) else {
        walk.visited = 1;
        return Ok(());
    };
    // The response chain runs from the latest cycle back to the first.
    let mut cursor = state.latest_response().map(|link| link.testament);
    let chain = std::iter::from_fn(move || {
        let id = cursor?;
        let response = core.native_response(id)?;
        let identity = response.identity();
        cursor = identity.prior;
        Some((identity.cycle, response))
    });
    walk.run(
        chain,
        |(cycle, _)| Position::Cycle(*cycle),
        |(cycle, response)| {
            if below.is_some_and(|below| cycle >= below) {
                return Ok(None);
            }
            Ok(Some(NativeObject::Response(Box::new(docs::response(
                response,
            )))))
        },
    )
}

fn receipts(
    reader: &Reader<'_>,
    walk: &mut Walk,
    after: Option<Position>,
    holder: Option<ParticipantId>,
    claim: Option<ClaimId>,
) -> Result<(), AccessError> {
    let resume = match after {
        None => None,
        Some(Position::Receipt(id)) => Some(id),
        Some(_) => return Err(wrong_position()),
    };
    walk.run(
        reader.core.native_receipts_from(resume),
        |(id, _)| Position::Receipt(*id),
        |(_, receipt)| {
            if holder.is_some_and(|holder| receipt.holder != holder)
                || claim.is_some_and(|claim| receipt.claim != claim)
            {
                return Ok(None);
            }
            Ok(Some(NativeObject::Receipt(docs::receipt(receipt))))
        },
    )
}

fn monitors(
    reader: &Reader<'_>,
    walk: &mut Walk,
    after: Option<Position>,
    claim: ClaimId,
) -> Result<(), AccessError> {
    let resume = match after {
        None => None,
        Some(Position::Monitor(id)) => Some(id),
        Some(_) => return Err(wrong_position()),
    };
    let Some(state) = reader.core.native_claim(claim) else {
        walk.visited = 1;
        return Ok(());
    };
    let scopes = state
        .scopes()
        .iter()
        .skip_while(move |scope| resume.is_some_and(|resume| scope.id() != resume))
        .skip(usize::from(resume.is_some()));
    walk.run(
        scopes,
        |scope| Position::Monitor(scope.id()),
        |scope| {
            Ok(Some(NativeObject::Monitor(Box::new(docs::monitor(
                claim, scope,
            )))))
        },
    )
}

fn events(
    reader: &Reader<'_>,
    walk: &mut Walk,
    after: Option<Position>,
    from: Option<(SessionSeq, u32)>,
) -> Result<(), AccessError> {
    let core = reader.core;
    let start = match after {
        None => from,
        Some(Position::Event { sequence, ordinal }) => Some((sequence, ordinal)),
        Some(_) => return Err(wrong_position()),
    };
    // Event slots are dense within a record and records are dense in the
    // prefix; an empty slot ends the record.
    let (mut sequence, mut ordinal) = match start {
        None => (SessionSeq(1), 0),
        Some((sequence, ordinal)) => (sequence, ordinal.saturating_add(1)),
    };
    let prefix = core.native_sequence();
    let slots = std::iter::from_fn(move || {
        while sequence <= prefix {
            let at = (sequence, ordinal);
            match core.native_event(sequence, ordinal) {
                Some(event) => {
                    ordinal = ordinal.saturating_add(1);
                    return Some((at, Some(event)));
                }
                None if ordinal == 0 => {
                    // A record without events still occupies the sequence.
                    sequence = SessionSeq(sequence.0.checked_add(1)?);
                }
                None => {
                    sequence = SessionSeq(sequence.0.checked_add(1)?);
                    ordinal = 0;
                    return Some((at, None));
                }
            }
        }
        None
    });
    walk.run(
        slots,
        |((sequence, ordinal), _)| Position::Event {
            sequence: *sequence,
            ordinal: *ordinal,
        },
        |(_, event)| Ok(event.map(|event| NativeObject::Event(Box::new(docs::event_record(event))))),
    )
}

/// Serve one page from a fixed committed prefix.
pub(crate) fn serve(
    reader: &Reader<'_>,
    key: &[u8; 32],
    request: &NativeListRequest,
) -> Result<NativeListPage, AccessError> {
    let filter = filter_bytes(&request.filter)?;
    let after = request
        .cursor
        .as_ref()
        .map(|cursor| decode(reader, key, &filter, cursor))
        .transpose()?;
    let mut walk = Walk::new(request)?;
    match &request.filter {
        NativeListFilter::Claims {
            issuer,
            subject,
            status,
            action,
            scope,
            relation,
            created_after,
        } => claims(
            reader,
            &mut walk,
            after,
            &ClaimPredicates {
                issuer: *issuer,
                subject: *subject,
                status: *status,
                action: *action,
                scope: scope.as_ref(),
                relation: relation.as_ref(),
                created_after: *created_after,
            },
        )?,
        NativeListFilter::Artifacts {
            producer,
            kind,
            schema,
            input,
        } => artifacts(
            reader,
            &mut walk,
            after,
            *producer,
            kind.as_deref(),
            *schema,
            *input,
        )?,
        NativeListFilter::Definitions { claim, evaluator } => {
            definitions(reader, &mut walk, after, *claim, *evaluator)?
        }
        NativeListFilter::Evaluations {
            claim,
            validation,
            evaluator,
            verdict,
        } => evaluations(
            reader,
            &mut walk,
            after,
            *claim,
            *validation,
            *evaluator,
            *verdict,
        )?,
        NativeListFilter::Responses { claim } => responses(reader, &mut walk, after, *claim)?,
        NativeListFilter::Receipts { holder, claim } => {
            receipts(reader, &mut walk, after, *holder, *claim)?
        }
        NativeListFilter::Monitors { claim } => monitors(reader, &mut walk, after, *claim)?,
        NativeListFilter::Events { after: from } => events(reader, &mut walk, after, *from)?,
    }
    let next = walk
        .next
        .map(|position| encode(reader, key, &filter, position))
        .transpose()?;
    Ok(NativeListPage {
        token: ReadToken {
            ledger: reader.ledger,
            sequence: reader.core.native_sequence(),
            route_epoch: reader.route,
        },
        native_sequence: reader.core.native_sequence(),
        objects: walk.objects,
        next,
        visited: walk.visited,
    })
}

/// Serve one list on the local owner thread from the committed prefix.
pub(crate) fn local(
    session: &mut Session,
    views: &mut crate::reads::ReadViews,
    peer: &AuthenticatedPeer,
    request: &NativeListRequest,
    route: RouteEpoch,
    limits: &WireLimits,
) -> Result<NativeListPage, AccessError> {
    request.validate(limits)?;
    let profile = crate::native_reads::profile(session)?;
    let role = crate::native_reads::role(peer)?;
    let key = *views.list_key()?;
    let core: &Core<NativeState> = session.native_core().map_err(access)?;
    serve(
        &Reader {
            core,
            ledger: session.ledger(),
            profile,
            principal: peer.principal(),
            role,
            route,
        },
        &key,
        request,
    )
}
