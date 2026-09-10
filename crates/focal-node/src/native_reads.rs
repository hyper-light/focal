//! Native reads over the committed native prefix. Every page is built from one
//! fixed prefix; linearizable reads first pass the session's read barrier.
use crate::{host::access, native_documents as docs};
use focal_core::native::NativeState;
use focal_ledger::{Core, LedgerActivation, NativeContentProfile, ReadCorrelation, Session};
use focal_model::*;
use focal_wire::*;

/// Polls granted to a read barrier before the read reports unavailability.
const BARRIER_POLLS: usize = 8;
/// Event scans visit at most this many empty slots beyond the requested items.
const EVENT_SCAN_SLACK: u32 = 64;

pub(crate) struct Reader<'a> {
    pub core: &'a Core<NativeState>,
    pub ledger: LedgerId,
    pub profile: NativeProfile,
    pub principal: ParticipantId,
    pub role: NativePeerRole,
    pub route: RouteEpoch,
}

/// Where a query's rows live (25 §6): the members a replica must hold to
/// serve it. A compound query that walks several objects names each; one
/// that lists across the group names the control affinity and every object
/// it starts from.
pub(crate) fn locations(query: &NativeReadQuery) -> Vec<focal_core::native::NativeLocation> {
    use focal_core::native::NativeLocation as L;
    fn of(reference: &NativeObjectRef) -> L {
        match reference {
            NativeObjectRef::Claim(id) => L::Claim(*id),
            NativeObjectRef::Definition(id) => L::Definition(*id),
            NativeObjectRef::Evaluation(key) => L::Claim(key.claim),
            NativeObjectRef::Result(result) => L::Claim(result.evaluation.claim),
            NativeObjectRef::Artifact(id)
            | NativeObjectRef::Work(id)
            | NativeObjectRef::Diagnostic(id) => L::Artifact(*id),
            NativeObjectRef::Response(id) | NativeObjectRef::ResultTestament(id) => {
                L::Testament(*id)
            }
            NativeObjectRef::Receipt(_) => L::Receipt,
            NativeObjectRef::Monitor { id, .. } => L::Monitor(*id),
            NativeObjectRef::Outcome(invocation) | NativeObjectRef::CreationResult(invocation) => {
                invocation_location(invocation)
            }
            NativeObjectRef::Event { .. } => L::Control,
            NativeObjectRef::LegacyTestament(id) => L::Testament(*id),
            NativeObjectRef::LegacyEvidenceSet(_) => L::Control,
            NativeObjectRef::LegacyDefinition(id)
            | NativeObjectRef::LegacyRun { validation: id, .. } => L::Definition(*id),
        }
    }
    fn invocation_location(invocation: &NativeInvocationRef) -> L {
        match invocation {
            NativeInvocationRef::Request(key) => L::Principal(key.principal),
            NativeInvocationRef::EvaluationDeadline { evaluation, .. } => {
                L::Claim(evaluation.claim)
            }
            NativeInvocationRef::ClaimDeadline { claim, .. }
            | NativeInvocationRef::MonitorDeadline { claim, .. } => L::Claim(*claim),
            NativeInvocationRef::Import => L::Control,
            NativeInvocationRef::Retirement { root } => L::Claim(*root),
        }
    }
    match query {
        NativeReadQuery::Objects(references) => references.iter().map(of).collect(),
        NativeReadQuery::Claim { id, .. } | NativeReadQuery::Responses { claim: id, .. } => {
            vec![L::Claim(*id), L::Control]
        }
        NativeReadQuery::Outcome(invocation) => vec![invocation_location(invocation)],
        NativeReadQuery::Receipt(_) => vec![L::Receipt],
        NativeReadQuery::Monitor { id, .. } => vec![L::Monitor(*id)],
        NativeReadQuery::Evaluations { validation, .. } => {
            vec![L::Definition(*validation), L::Control]
        }
        NativeReadQuery::Results { evaluation, .. } => vec![L::Claim(evaluation.claim)],
        NativeReadQuery::ValidationContext(context) => {
            vec![
                L::Claim(context.claim),
                L::Definition(context.validation),
                L::Control,
            ]
        }
        NativeReadQuery::Events { .. } | NativeReadQuery::Standing => vec![L::Control],
    }
}

pub(crate) fn profile(session: &Session) -> Result<NativeProfile, AccessError> {
    match session.activation() {
        LedgerActivation::Native { profile, .. } => Ok(match profile {
            NativeContentProfile::ProjectionOnly => NativeProfile::ProjectionOnly,
            NativeContentProfile::AuthoredV1 => NativeProfile::AuthoredV1,
        }),
        LedgerActivation::V1 => Err(AccessError::UnsupportedOperation),
    }
}
pub(crate) fn role(peer: &AuthenticatedPeer) -> Result<NativePeerRole, AccessError> {
    match peer.role() {
        PeerRole::Actor => Ok(NativePeerRole::Actor),
        PeerRole::Evaluator => Ok(NativePeerRole::Evaluator),
        PeerRole::Runtime => Ok(NativePeerRole::Runtime),
        PeerRole::Node { .. } => Err(AccessError::Unauthorized),
    }
}
pub(crate) fn correlation(
    principal: ParticipantId,
    request: RequestId,
    nonce: u64,
) -> ReadCorrelation {
    let mut hash = blake3::Hasher::new_derive_key("focal.native.read-correlation.v1");
    hash.update(&principal.0);
    hash.update(&request.0);
    hash.update(&nonce.to_le_bytes());
    let mut bytes = [0; 16];
    bytes.copy_from_slice(&hash.finalize().as_bytes()[..16]);
    ReadCorrelation(bytes)
}
/// Serve one read on the local owner thread. The barrier for a linearizable
/// read is driven here; replicas wait for the boundary in their event loop.
pub(crate) fn local(
    session: &mut Session,
    peer: &AuthenticatedPeer,
    read: &NativeReadRequest,
    request_id: RequestId,
    route: RouteEpoch,
    limits: &WireLimits,
) -> Result<NativeReadPage, AccessError> {
    read.validate(limits)?;
    let profile = profile(session)?;
    let role = role(peer)?;
    if matches!(read.consistency, ReadConsistency::Linearizable) {
        let correlation = correlation(peer.principal(), request_id, 0);
        session.native_read_index(correlation).map_err(access)?;
        let mut boundary = None;
        for _ in 0..BARRIER_POLLS {
            let events = session.poll().map_err(access)?;
            if let Some(found) = events
                .native_read_boundaries
                .iter()
                .find(|boundary| boundary.correlation == correlation)
            {
                boundary = Some(*found);
                break;
            }
        }
        let boundary = boundary.ok_or(AccessError::Unavailable)?;
        let core = session.native_read_at_least(boundary).map_err(access)?;
        return page(
            &Reader {
                core,
                ledger: session.ledger(),
                profile,
                principal: peer.principal(),
                role,
                route,
            },
            read,
        );
    }
    let core = session.native_core().map_err(access)?;
    check_consistency(core, session.ledger(), &read.consistency)?;
    page(
        &Reader {
            core,
            ledger: session.ledger(),
            profile,
            principal: peer.principal(),
            role,
            route,
        },
        read,
    )
}
pub(crate) fn check_consistency(
    core: &Core<NativeState>,
    ledger: LedgerId,
    consistency: &ReadConsistency,
) -> Result<(), AccessError> {
    let sequence = core.native_sequence();
    match consistency {
        ReadConsistency::AtLeast(token) => {
            if token.ledger != ledger {
                return Err(AccessError::Unauthorized);
            }
            if token.sequence > sequence {
                return Err(AccessError::Behind {
                    published: sequence,
                });
            }
        }
        ReadConsistency::Exact(token) => {
            if token.ledger != ledger {
                return Err(AccessError::Unauthorized);
            }
            if token.sequence != sequence {
                return Err(AccessError::SnapshotExpired);
            }
        }
        ReadConsistency::Linearizable | ReadConsistency::StaleProjection => {}
    }
    Ok(())
}
fn object(reader: &Reader<'_>, reference: NativeObjectRef) -> Result<NativeObject, AccessError> {
    let core = reader.core;
    let found = match reference {
        NativeObjectRef::Claim(id) => core
            .native_claim(id)
            .map(|state| {
                NativeObject::Claim(Box::new(docs::claim(
                    core,
                    state,
                    NativeClaimExpand::default(),
                )))
            })
            .or_else(|| {
                core.native_retired(id)
                    .map(|value| NativeObject::Retired(docs::retired(id, value)))
            }),
        NativeObjectRef::Definition(id) => core.native_definition(id).map(|declaration| {
            NativeObject::Definition(Box::new(docs::definition(core, declaration)))
        }),
        NativeObjectRef::Evaluation(key) => {
            let core_key = docs::evaluation_key_of(key);
            match (
                core.native_definition(key.validation),
                core.native_evaluation(core_key),
            ) {
                (Some(declaration), Some(state)) => Some(NativeObject::Evaluation(Box::new(
                    docs::evaluation(declaration, core_key, state)?,
                ))),
                _ => None,
            }
        }
        NativeObjectRef::Result(key) => core
            .native_result(docs::result_key_of(key))
            .map(|accepted| NativeObject::Result(Box::new(docs::result(accepted)))),
        NativeObjectRef::Artifact(id) => core
            .native_artifact(id)
            .map(|artifact| NativeObject::Artifact(Box::new(docs::artifact(artifact)))),
        NativeObjectRef::Work(id) => core
            .native_work(id)
            .map(|work| NativeObject::Work(Box::new(docs::work(&work.state)))),
        NativeObjectRef::Diagnostic(id) => core.native_diagnostic(id).map(|row| {
            NativeObject::Diagnostic(Box::new(NativeWorkArtifact {
                binding: NativeBinding {
                    object: ObjectId(row.diagnostic.diagnostic().artifact.id.0),
                    content: row.diagnostic.diagnostic().artifact.hash,
                    revision: ObjectRevision(1),
                },
                reference: row.diagnostic.diagnostic().artifact,
                claim: row.diagnostic.claim(),
                cycle: row.diagnostic.cycle(),
                slot: 0,
                state: NativeWorkArtifactState::GenerationFailed,
                producer: row.diagnostic.producer(),
                receipt: row.diagnostic.receipt(),
                attachment: None,
                diagnostic: Some(NativeDiagnostic {
                    reason: match row.diagnostic.diagnostic().reason {
                        focal_model::lifecycle::evidence::EvidenceFailure::Work => {
                            NativeEvidenceFailure::Work
                        }
                        focal_model::lifecycle::evidence::EvidenceFailure::Production => {
                            NativeEvidenceFailure::Production
                        }
                        focal_model::lifecycle::evidence::EvidenceFailure::Structure => {
                            NativeEvidenceFailure::Structure
                        }
                        focal_model::lifecycle::evidence::EvidenceFailure::Metadata => {
                            NativeEvidenceFailure::Metadata
                        }
                    },
                    artifact: row.diagnostic.diagnostic().artifact,
                }),
                terminal: None,
            }))
        }),
        NativeObjectRef::Response(id) => core
            .native_response(id)
            .map(|response| NativeObject::Response(Box::new(docs::response(response)))),
        NativeObjectRef::ResultTestament(id) => core
            .native_result_testament(id)
            .map(|testament| NativeObject::ResultTestament(docs::result_testament(testament))),
        NativeObjectRef::Receipt(id) => core
            .native_receipt(id)
            .map(|receipt| NativeObject::Receipt(docs::receipt(receipt))),
        NativeObjectRef::Monitor { claim, id } => core
            .native_claim(claim)
            .and_then(|state| state.scopes().monitor(id))
            .map(|scope| NativeObject::Monitor(Box::new(docs::monitor(claim, scope)))),
        NativeObjectRef::Outcome(invocation) => core
            .native_outcome(docs::invocation_of(invocation))
            .map(|outcome| NativeObject::Outcome(Box::new(docs::outcome(outcome)))),
        NativeObjectRef::CreationResult(invocation) => {
            let key = docs::invocation_of(invocation);
            core.native_creation_result(key)
                .map(|result| NativeObject::CreationResult(docs::creation_result(key, result)))
        }
        NativeObjectRef::Event { sequence, ordinal } => core
            .native_event(sequence, ordinal)
            .map(|event| NativeObject::Event(Box::new(docs::event_record(event)))),
        NativeObjectRef::LegacyTestament(id) => legacy(
            core,
            reference,
            focal_core::native::NativeLegacyKey::Testament(id),
        ),
        NativeObjectRef::LegacyEvidenceSet(id) => legacy(
            core,
            reference,
            focal_core::native::NativeLegacyKey::EvidenceSet(id),
        ),
        NativeObjectRef::LegacyDefinition(id) => legacy(
            core,
            reference,
            focal_core::native::NativeLegacyKey::Definition(id),
        ),
        NativeObjectRef::LegacyRun { validation, run } => legacy(
            core,
            reference,
            focal_core::native::NativeLegacyKey::Run { validation, run },
        ),
    };
    Ok(found.unwrap_or(NativeObject::Missing(reference)))
}
fn legacy(
    core: &Core<NativeState>,
    reference: NativeObjectRef,
    key: focal_core::native::NativeLegacyKey,
) -> Option<NativeObject> {
    core.native_legacy_bytes(key).map(|bytes| {
        NativeObject::Legacy(NativeLegacyRow {
            key: reference,
            bytes: bytes.to_vec(),
        })
    })
}
fn token(reader: &Reader<'_>) -> ReadToken {
    ReadToken {
        ledger: reader.ledger,
        sequence: reader.core.native_sequence(),
        route_epoch: reader.route,
    }
}
fn finish(
    reader: &Reader<'_>,
    objects: Vec<NativeObject>,
    next: Option<NativeContinuation>,
    visited: u32,
) -> NativeReadPage {
    NativeReadPage {
        token: token(reader),
        native_sequence: reader.core.native_sequence(),
        logical_time: reader.core.native_logical_time(),
        objects,
        next,
        visited,
    }
}
fn count(value: usize) -> Result<u32, AccessError> {
    u32::try_from(value).map_err(|_| AccessError::Capacity)
}
/// Build one page from a fixed prefix. Expansions append related objects after
/// the requested one so every document stays flat.
pub(crate) fn page(
    reader: &Reader<'_>,
    read: &NativeReadRequest,
) -> Result<NativeReadPage, AccessError> {
    let core = reader.core;
    let max_items = read.max_items as usize;
    match &read.query {
        NativeReadQuery::Objects(references) => {
            let mut objects = Vec::with_capacity(references.len());
            for reference in references {
                objects.push(object(reader, *reference)?);
            }
            let visited = count(objects.len())?;
            Ok(finish(reader, objects, None, visited))
        }
        NativeReadQuery::Claim { id, expand } => {
            if expand.history {
                return Err(AccessError::UnsupportedOperation);
            }
            let Some(state) = core.native_claim(*id) else {
                // A retired claim answers with its continuation (26 §4).
                let object = match core.native_retired(*id) {
                    Some(value) => NativeObject::Retired(docs::retired(*id, value)),
                    None => NativeObject::Missing(NativeObjectRef::Claim(*id)),
                };
                return Ok(finish(reader, vec![object], None, 1));
            };
            let mut objects = vec![NativeObject::Claim(Box::new(docs::claim(
                core, state, *expand,
            )))];
            let mut visited = 1usize;
            if expand.responses {
                let mut link = state.latest_response();
                while let Some(current) = link
                    && objects.len() < max_items
                {
                    visited = visited.saturating_add(1);
                    if let Some(response) = core.native_response(current.testament) {
                        objects.push(NativeObject::Response(Box::new(docs::response(response))));
                    }
                    link = current
                        .prior
                        .and_then(|prior| core.native_response(prior))
                        .map(|response| {
                            let identity = response.identity();
                            focal_model::lifecycle::claim::ResponseLink {
                                testament: TestamentId(identity.binding.object.0),
                                content: identity.binding.content,
                                receipt: identity.receipt,
                                cycle: identity.cycle,
                                prior: identity.prior,
                            }
                        });
                }
            }
            if expand.evaluations
                && let Some(registrations) = core.native_registrations(*id)
            {
                for row in registrations.rows() {
                    if objects.len() >= max_items {
                        break;
                    }
                    visited = visited.saturating_add(1);
                    let validation = ValidationId(row.binding().object.0);
                    let Some(declaration) = core.native_definition(validation) else {
                        continue;
                    };
                    let key = focal_core::native::EvaluationKey {
                        claim: *id,
                        validation,
                        target: focal_core::native::EvaluationTarget::of(row.target()),
                        generation: row.generation(),
                    };
                    if let Some(evaluation) = core.native_evaluation(key) {
                        objects.push(NativeObject::Evaluation(Box::new(docs::evaluation(
                            declaration,
                            key,
                            evaluation,
                        )?)));
                    }
                }
            }
            let visited = count(visited)?;
            Ok(finish(reader, objects, None, visited))
        }
        NativeReadQuery::Outcome(invocation) => {
            let objects = vec![object(reader, NativeObjectRef::Outcome(*invocation))?];
            Ok(finish(reader, objects, None, 1))
        }
        NativeReadQuery::Receipt(id) => {
            let objects = vec![object(reader, NativeObjectRef::Receipt(*id))?];
            Ok(finish(reader, objects, None, 1))
        }
        NativeReadQuery::Monitor { claim, id } => {
            let objects = vec![object(
                reader,
                NativeObjectRef::Monitor {
                    claim: *claim,
                    id: *id,
                },
            )?];
            Ok(finish(reader, objects, None, 1))
        }
        NativeReadQuery::Responses { claim, after } => {
            let Some(state) = core.native_claim(*claim) else {
                return Ok(finish(
                    reader,
                    vec![NativeObject::Missing(NativeObjectRef::Claim(*claim))],
                    None,
                    1,
                ));
            };
            let mut objects = Vec::new();
            let mut visited = 1usize;
            let mut next = None;
            let mut cursor = state.latest_response().map(|link| link.testament);
            while let Some(id) = cursor {
                let Some(response) = core.native_response(id) else {
                    break;
                };
                let identity = response.identity();
                visited = visited.saturating_add(1);
                if after.is_none_or(|after| identity.cycle < after) {
                    if objects.len() >= max_items {
                        next = Some(NativeContinuation::Responses {
                            cycle: identity.cycle.saturating_add(1),
                        });
                        break;
                    }
                    objects.push(NativeObject::Response(Box::new(docs::response(response))));
                }
                cursor = identity.prior;
            }
            let visited = count(visited)?;
            Ok(finish(reader, objects, next, visited))
        }
        NativeReadQuery::Evaluations { validation, after } => {
            let Some(declaration) = core.native_definition(*validation) else {
                return Ok(finish(
                    reader,
                    vec![NativeObject::Missing(NativeObjectRef::Definition(
                        *validation,
                    ))],
                    None,
                    1,
                ));
            };
            let claim = declaration.claim();
            let mut objects = Vec::new();
            let mut visited = 1usize;
            let mut next = None;
            if let Some(registrations) = core.native_registrations(claim) {
                for row in registrations.rows() {
                    if ValidationId(row.binding().object.0) != *validation {
                        continue;
                    }
                    let key = focal_core::native::EvaluationKey {
                        claim,
                        validation: *validation,
                        target: focal_core::native::EvaluationTarget::of(row.target()),
                        generation: row.generation(),
                    };
                    let wire_key = docs::evaluation_key(key);
                    if after.is_some_and(|after| wire_key <= after) {
                        continue;
                    }
                    visited = visited.saturating_add(1);
                    if objects.len() >= max_items {
                        next = Some(NativeContinuation::Evaluations(wire_key));
                        break;
                    }
                    if let Some(evaluation) = core.native_evaluation(key) {
                        objects.push(NativeObject::Evaluation(Box::new(docs::evaluation(
                            declaration,
                            key,
                            evaluation,
                        )?)));
                    }
                }
            }
            let visited = count(visited)?;
            Ok(finish(reader, objects, next, visited))
        }
        NativeReadQuery::Results { evaluation, after } => {
            if after.is_some() {
                return Err(AccessError::UnsupportedOperation);
            }
            let key = docs::evaluation_key_of(*evaluation);
            let mut objects = Vec::new();
            let mut visited = 1usize;
            if let Some(state) = core.native_evaluation(key)
                && let Some(result) = state.last_result()
            {
                visited = visited.saturating_add(1);
                let result_key = focal_core::native::NativeResultKey::of(result);
                if let Some(accepted) = core.native_result(result_key) {
                    objects.push(NativeObject::Result(Box::new(docs::result(accepted))));
                }
            }
            let visited = count(visited)?;
            Ok(finish(reader, objects, None, visited))
        }
        NativeReadQuery::ValidationContext(query) => validation_context(reader, query, max_items),
        NativeReadQuery::Events { after, limit } => {
            let prefix = core.native_sequence();
            let (mut sequence, mut ordinal) = match after {
                Some((sequence, ordinal)) => (*sequence, ordinal.saturating_add(1)),
                None => (SessionSeq(1), 0),
            };
            let mut objects = Vec::new();
            let mut visited = 0u32;
            let budget = limit.saturating_add(EVENT_SCAN_SLACK);
            let mut last = None;
            while sequence <= prefix && objects.len() < *limit as usize && visited < budget {
                visited = visited.saturating_add(1);
                match core.native_event(sequence, ordinal) {
                    Some(event) => {
                        last = Some((sequence, ordinal));
                        objects.push(NativeObject::Event(Box::new(docs::event_record(event))));
                        ordinal = ordinal.saturating_add(1);
                    }
                    None => {
                        sequence = SessionSeq(sequence.0.saturating_add(1));
                        ordinal = 0;
                    }
                }
            }
            let next = last
                .filter(|_| objects.len() >= *limit as usize || visited >= budget)
                .map(|(sequence, ordinal)| NativeContinuation::Events { sequence, ordinal });
            Ok(finish(reader, objects, next, visited))
        }
        NativeReadQuery::Standing => {
            let standing = NativeStanding {
                principal: reader.principal,
                role: reader.role,
                profile: reader.profile,
                native_sequence: core.native_sequence(),
                logical_time: core.native_logical_time(),
            };
            Ok(finish(
                reader,
                vec![NativeObject::Standing(standing)],
                None,
                1,
            ))
        }
    }
}

/// Everything an evaluator needs from one fixed prefix (19 §4): the claim,
/// the definition, the selected registration and its evaluation, the
/// evaluation's target with its manifest and custody, the accepted results
/// after an optional revision cursor, and the delivery result of the same
/// response. The selection is the registration matching the requested target
/// and generation, else the highest generation registered for the definition.
fn validation_context(
    reader: &Reader<'_>,
    query: &NativeContextQuery,
    max_items: usize,
) -> Result<NativeReadPage, AccessError> {
    use focal_core::native::{EvaluationKey, EvaluationTarget, NativeResultKey};
    use focal_model::lifecycle::validation;
    let core = reader.core;
    let mut visited = 1usize;
    let Some(claim) = core.native_claim(query.claim) else {
        return Ok(finish(
            reader,
            vec![NativeObject::Missing(NativeObjectRef::Claim(query.claim))],
            None,
            1,
        ));
    };
    let Some(declaration) = core.native_definition(query.validation) else {
        return Ok(finish(
            reader,
            vec![NativeObject::Missing(NativeObjectRef::Definition(
                query.validation,
            ))],
            None,
            1,
        ));
    };
    if declaration.claim() != query.claim {
        return Err(AccessError::InvalidRequest);
    }
    let registrations = core.native_registrations(query.claim);
    let mut selected: Option<(EvaluationKey, bool)> = None;
    if let Some(registrations) = registrations {
        for row in registrations.rows() {
            visited = visited.saturating_add(1);
            if ValidationId(row.binding().object.0) != query.validation {
                continue;
            }
            let key = EvaluationKey {
                claim: query.claim,
                validation: query.validation,
                target: EvaluationTarget::of(row.target()),
                generation: row.generation(),
            };
            let family = match key.target {
                EvaluationTarget::Admission => NativeContextKind::Admission,
                EvaluationTarget::Increment { .. } => NativeContextKind::Increment,
                EvaluationTarget::Work { .. }
                | EvaluationTarget::MissingSlot { .. }
                | EvaluationTarget::Delivery { .. } => NativeContextKind::WholeWork,
            };
            if query.kind.is_some_and(|wanted| family != wanted)
                || query
                    .target
                    .is_some_and(|wanted| docs::evaluation_target(key.target) != wanted)
                || query
                    .generation
                    .is_some_and(|wanted| key.generation != wanted)
            {
                continue;
            }
            if selected.is_none_or(|(current, _)| current.generation < key.generation) {
                selected = Some((key, registrations.is_sealed()));
            }
        }
    }
    let mut manifest = Vec::new();
    let mut results = Vec::new();
    let mut next = None;
    let mut delivery = None;
    let (registration, evaluation, target) = match selected {
        None => (
            NativeRegistration {
                state: NativeRegistrationState::Missing,
                generation: 0,
                eligible: false,
                reason: Some("no registered evaluation matches".into()),
            },
            None,
            None,
        ),
        Some((key, sealed)) => {
            let state = core.native_evaluation(key);
            visited = visited.saturating_add(1);
            // The target's manifest: every slot of the response for a
            // delivery or slot check, the one increment artifact otherwise.
            let target = state.map(|state| state.target());
            let mut custody_ok = true;
            let mut entry = |slot: u32, id: Option<ArtifactId>| -> Result<(), AccessError> {
                visited = visited.saturating_add(1);
                let artifact = id.and_then(|id| core.native_artifact(id));
                let custody_verified =
                    id.is_some_and(|id| artifact.is_some() && core.native_work(id).is_some());
                custody_ok &= custody_verified;
                manifest.try_reserve(1).map_err(|_| AccessError::Capacity)?;
                manifest.push(NativeManifestEntry {
                    slot,
                    artifact: artifact.map(docs::artifact),
                    custody_verified,
                });
                Ok(())
            };
            let mut response_id = None;
            match target {
                Some(validation::Target::Artifact {
                    response,
                    slot,
                    artifact,
                }) => {
                    response_id = Some(TestamentId(response.object.0));
                    entry(slot, Some(ArtifactId(artifact.object.0)))?;
                }
                Some(validation::Target::MissingSlot { response, slot }) => {
                    response_id = Some(TestamentId(response.object.0));
                    entry(slot, None)?;
                }
                Some(validation::Target::Delivery { response }) => {
                    let id = TestamentId(response.object.0);
                    response_id = Some(id);
                    if let Some(response) = core.native_response(id) {
                        for binding in response.manifest() {
                            entry(binding.slot, Some(binding.artifact.id))?;
                        }
                    }
                }
                Some(validation::Target::Increment { artifact, .. }) => {
                    let id = ArtifactId(artifact.object.0);
                    let slot = core.native_work(id).map_or(0, |work| work.state.slot());
                    entry(slot, Some(id))?;
                }
                Some(validation::Target::Admission { .. }) | None => {}
            }
            // Accepted results of this evaluation after the cursor, one per
            // accepted revision, bounded by the request.
            if let Some(state) = state {
                let limit = usize::try_from(query.limit)
                    .map_err(|_| AccessError::Capacity)?
                    .max(1)
                    .min(max_items.max(1));
                let first = query
                    .results_after
                    .map_or(1, |after| after.0.saturating_add(1));
                let last = state.binding().revision.0;
                let mut revision = first;
                while revision <= last {
                    visited = visited.saturating_add(1);
                    let key = NativeResultKey {
                        evaluation: key,
                        revision: ObjectRevision(revision),
                    };
                    if let Some(accepted) = core.native_result(key) {
                        if results.len() >= limit {
                            next = Some(ObjectRevision(revision.saturating_sub(1)));
                            break;
                        }
                        results.try_reserve(1).map_err(|_| AccessError::Capacity)?;
                        results.push(docs::result(accepted));
                    }
                    revision = revision.saturating_add(1);
                }
            }
            // The delivery result of the same response, when one is accepted.
            if let (Some(response_id), Some(registrations)) = (response_id, registrations) {
                for row in registrations.rows() {
                    let validation::Target::Delivery { response } = row.target() else {
                        continue;
                    };
                    if TestamentId(response.object.0) != response_id {
                        continue;
                    }
                    visited = visited.saturating_add(1);
                    let delivery_key = EvaluationKey {
                        claim: query.claim,
                        validation: ValidationId(row.binding().object.0),
                        target: EvaluationTarget::of(row.target()),
                        generation: row.generation(),
                    };
                    if let Some(result) = core
                        .native_evaluation(delivery_key)
                        .and_then(|state| state.last_result())
                        .and_then(|result| core.native_delivery_result(NativeResultKey::of(result)))
                    {
                        delivery = Some(docs::delivery_result(result));
                    }
                    break;
                }
            }
            let (eligible, reason) = match state {
                None => (false, Some("the evaluation row is missing".to_owned())),
                Some(state) if state.state().is_terminal() => {
                    (false, Some("the evaluation is terminal".to_owned()))
                }
                Some(state) if state.fence().is_some() => {
                    (false, Some("the evaluation is fenced".to_owned()))
                }
                Some(_) if !custody_ok => (
                    false,
                    Some("a manifest artifact is not in verified custody".to_owned()),
                ),
                Some(_) => (true, None),
            };
            (
                NativeRegistration {
                    state: if sealed {
                        NativeRegistrationState::Sealed
                    } else {
                        NativeRegistrationState::Registered
                    },
                    generation: key.generation,
                    eligible,
                    reason,
                },
                state
                    .map(|state| docs::evaluation(declaration, key, state))
                    .transpose()?,
                target.map(docs::target),
            )
        }
    };
    let context = NativeValidationContext {
        claim: docs::claim(core, claim, NativeClaimExpand::default()),
        definition: docs::definition(core, declaration),
        registration,
        evaluation,
        target,
        manifest,
        results,
        delivery,
        next,
    };
    let visited = count(visited)?;
    Ok(finish(
        reader,
        vec![NativeObject::Context(Box::new(context))],
        None,
        visited,
    ))
}
