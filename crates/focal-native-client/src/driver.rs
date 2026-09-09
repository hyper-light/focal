//! The shared driver of a native host: resolve the objects an authored
//! operation binds to, compile and encode the exact frame, claim its `n1:`
//! identity in the journal, and plan the exact reads. The CLI and the MCP
//! server call these functions with their own blocking read callback so both
//! adapters submit byte-identical frames for identical documents (doc 19).
use crate::EvaluationSelector;
use crate::{
    CompileError, CompileLimits, Requirement, Resolved, compile, encode_frame, fingerprint,
};
use focal_client::ClientError;
use focal_client::input::{BuildContext, IdGenerator, InputError, parse_id};
use focal_client::native_store::{
    NativeOperation, NativeOperationId, NativeOperationStore, NativeStoreError,
    PreparedNativeRequest,
};
use focal_client::operation_store::OperationIntent;
use focal_client::operations::{NativeAuthoredOperation, NativeListOperation, NativeReadOperation};
use focal_client::pending::OperationContext;
use focal_core::native::NativeContentProfile;
use focal_model::*;
use focal_wire::*;
use std::cell::RefCell;

/// Items requested per fixed-prefix read page. The owner bounds what it
/// serves; a host never needs more than one bounded page per requirement.
pub const READ_ITEMS: u32 = 256;

/// One blocking fixed-prefix read. The host supplies the envelope, transport
/// and cancellation; the driver supplies only the query.
pub type Reads<'a> = dyn FnMut(NativeReadRequest) -> Result<NativeReadPage, DriveError> + 'a;

#[derive(Debug, thiserror::Error)]
pub enum DriveError {
    #[error(transparent)]
    Compile(#[from] CompileError),
    #[error(transparent)]
    Store(#[from] NativeStoreError),
    #[error(transparent)]
    Client(#[from] ClientError),
    #[error(transparent)]
    Input(#[from] InputError),
    #[error(
        "this ledger was imported projection-only; authored claim creation needs the authored content profile"
    )]
    ProjectionOnly,
    #[error("the host's wait was cancelled; a journaled frame remains recoverable")]
    Cancelled,
}

/// What the host must have before it prepares one authored operation.
pub struct Preparation<'a> {
    pub store: &'a NativeOperationStore,
    pub context: OperationContext,
    pub build: &'a BuildContext,
    pub profile: NativeContentProfile,
    pub limits: &'a CompileLimits,
}

/// The one rule a host checks before spending a read: a projection-only
/// ledger carries no authored content profile, so claim creation cannot be
/// compiled for it. Every other refusal is the owner's.
pub fn admissible(
    profile: NativeContentProfile,
    operation: &NativeAuthoredOperation,
) -> Result<(), DriveError> {
    if profile == NativeContentProfile::ProjectionOnly
        && (matches!(operation, NativeAuthoredOperation::ClaimSubmit(_))
            || focal_client::operations::authored_shape(operation.descriptor()).is_some())
    {
        return Err(DriveError::ProjectionOnly);
    }
    Ok(())
}

/// Read the objects the compiler binds to, one linearizable page per
/// requirement, in the compiler's order.
pub fn resolve(
    ledger: LedgerId,
    operation: &NativeAuthoredOperation,
    reads: &mut Reads<'_>,
) -> Result<Resolved, DriveError> {
    let requirements = crate::requirements(operation)?;
    let mut objects = Vec::new();
    for requirement in requirements {
        let query = match requirement {
            Requirement::Objects(references) => NativeReadQuery::Objects(references),
            Requirement::Evaluations { validation } => NativeReadQuery::Evaluations {
                validation,
                after: None,
            },
        };
        let page = reads(NativeReadRequest {
            consistency: ReadConsistency::Linearizable,
            query,
            max_items: READ_ITEMS,
        })?;
        objects
            .try_reserve(page.objects.len())
            .map_err(|_| InputError::Capacity)?;
        objects.extend(page.objects);
    }
    Ok(Resolved::from_objects(ledger, &objects)?)
}

/// Claim the operation's identity, resolve, compile and encode the exact
/// frame and journal it ready. An existing identity with the same canonical
/// document returns the journaled operation without any read or send; a
/// different document under the same identity is an intent conflict.
pub fn prepare(
    preparation: &Preparation<'_>,
    operation: &NativeAuthoredOperation,
    requested: Option<NativeOperationId>,
    ids: &mut dyn IdGenerator,
    reads: &mut Reads<'_>,
) -> Result<NativeOperation, DriveError> {
    admissible(preparation.profile, operation)?;
    let descriptor = operation.descriptor();
    let canonical = operation.canonical_intent()?;
    let limits = preparation.limits;
    // The store's expansion callback is infallible in type only; the actual
    // read, compile or encode failure is surfaced through this channel.
    let deferred: RefCell<Option<DriveError>> = RefCell::new(None);
    // The journal mints the operation identity and the compiler mints object
    // identities from the same generator; both borrow it through the cell.
    let ids = RefCell::new(ids);
    let mut operation_ids = || ids.borrow_mut().next_id();
    let prepared = preparation.store.prepare(
        preparation.context,
        OperationIntent {
            name: descriptor.name,
            version: descriptor.version,
            canonical: &canonical,
        },
        requested,
        &mut operation_ids,
        |request| {
            let mut expand = || -> Result<PreparedNativeRequest, DriveError> {
                let resolved = resolve(preparation.build.ledger, operation, reads)?;
                let mut object_ids = || ids.borrow_mut().next_id();
                let compiled = compile(
                    operation,
                    preparation.build,
                    request,
                    &mut object_ids,
                    &resolved,
                    limits,
                )?;
                let frame = encode_frame(
                    preparation.build.ledger,
                    preparation.profile,
                    &compiled.input,
                    limits.encoding(),
                )?;
                let fingerprint = fingerprint(&frame, limits.native, limits.frame)?;
                Ok(PreparedNativeRequest {
                    request: RequestEnvelope {
                        protocol: NATIVE_PROTOCOL_VERSION,
                        ledger: preparation.build.ledger,
                        route_epoch: RouteEpoch(1),
                        request_epoch: RequestEpoch(1),
                        request_id: request,
                        operation: Operation::Native { frame },
                    },
                    fingerprint,
                    created: compiled.created,
                })
            };
            expand().map_err(|error| {
                *deferred.borrow_mut() = Some(error);
                NativeStoreError::Expansion(InputError::Invalid("native expansion failed"))
            })
        },
    );
    match prepared {
        Ok(prepared) => Ok(prepared),
        Err(error) => Err(deferred.into_inner().unwrap_or_else(|| error.into())),
    }
}

/// The expansion of one committed claim page every host shows by default.
pub const CLAIM_EXPAND: NativeClaimExpand = NativeClaimExpand {
    content: true,
    scopes: true,
    responses: true,
    evaluations: true,
    history: false,
};

/// Serve one exact read. Every page comes from one fixed prefix; the
/// validation read chains its evaluations at least at the definition's prefix
/// so the combined page never shows a definition newer than its evaluations.
/// What a native read operation yields: an exact fixed-prefix page, or the
/// wait observer's compact result.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NativeReadOutcome {
    Page(NativeReadPage),
    Wait(focal_client::operations::NativeWaitResult),
}

/// Serve one read operation. Plain reads are one or two exact pages; the
/// lineage read composes bounded reads and lists; the wait observer probes
/// under the host's cancellable pause.
pub fn read(
    operation: &NativeReadOperation,
    build: &focal_client::input::BuildContext,
    reads: &mut Reads<'_>,
    lists: &mut Lists<'_>,
    pause: &mut crate::observe::Pause<'_>,
) -> Result<NativeReadOutcome, DriveError> {
    Ok(match operation {
        NativeReadOperation::ClaimLineage(document) => {
            NativeReadOutcome::Page(crate::observe::lineage(document, build, reads, lists)?)
        }
        NativeReadOperation::ClaimWait(document) => {
            NativeReadOutcome::Wait(crate::observe::wait(document, reads, pause)?)
        }
        _ => NativeReadOutcome::Page(read_page(operation, reads)?),
    })
}

fn read_page(
    operation: &NativeReadOperation,
    reads: &mut Reads<'_>,
) -> Result<NativeReadPage, DriveError> {
    let mut linearizable = |query| {
        reads(NativeReadRequest {
            consistency: ReadConsistency::Linearizable,
            query,
            max_items: READ_ITEMS,
        })
    };
    let id = |value: &str| -> Result<[u8; 16], DriveError> { Ok(parse_id(value)?) };
    Ok(match operation {
        NativeReadOperation::ClaimGet(document) => linearizable(NativeReadQuery::Claim {
            id: ClaimId(id(&document.id)?),
            expand: CLAIM_EXPAND,
        })?,
        NativeReadOperation::TestamentGet(document) => {
            let testament = TestamentId(id(&document.id)?);
            linearizable(NativeReadQuery::Objects(vec![
                NativeObjectRef::Response(testament),
                NativeObjectRef::ResultTestament(testament),
            ]))?
        }
        NativeReadOperation::ArtifactGet(document) => {
            let artifact = ArtifactId(id(&document.id)?);
            linearizable(NativeReadQuery::Objects(vec![
                NativeObjectRef::Artifact(artifact),
                NativeObjectRef::Work(artifact),
                NativeObjectRef::Diagnostic(artifact),
            ]))?
        }
        NativeReadOperation::ValidationGet(document) => {
            let validation = ValidationId(id(&document.id)?);
            let definition =
                linearizable(NativeReadQuery::Objects(vec![NativeObjectRef::Definition(
                    validation,
                )]))?;
            let mut evaluations = reads(NativeReadRequest {
                consistency: ReadConsistency::AtLeast(definition.token),
                query: NativeReadQuery::Evaluations {
                    validation,
                    after: None,
                },
                max_items: READ_ITEMS,
            })?;
            let mut objects = definition.objects;
            objects
                .try_reserve(evaluations.objects.len())
                .map_err(|_| InputError::Capacity)?;
            objects.append(&mut evaluations.objects);
            evaluations.objects = objects;
            evaluations
        }
        NativeReadOperation::ValidationContext(document) => {
            let validation = ValidationId(id(&document.validation)?);
            let selector = EvaluationSelector::parse(
                &document.phase,
                document.slot,
                document.target.as_deref(),
            )?;
            let definition =
                linearizable(NativeReadQuery::Objects(vec![NativeObjectRef::Definition(
                    validation,
                )]))?;
            let claim = definition
                .objects
                .iter()
                .find_map(|object| match object {
                    NativeObject::Definition(definition) => Some(definition.claim),
                    _ => None,
                })
                .ok_or(CompileError::Missing("definition"))?;
            let evaluations = reads(NativeReadRequest {
                consistency: ReadConsistency::AtLeast(definition.token),
                query: NativeReadQuery::Evaluations {
                    validation,
                    after: None,
                },
                max_items: READ_ITEMS,
            })?;
            // The same selection as validation.begin, over the wire objects:
            // the highest live generation of the requested phase, or the
            // exact generation when one is named.
            let mut selected: Option<&NativeEvaluation> = None;
            for object in &evaluations.objects {
                let NativeObject::Evaluation(evaluation) = object else {
                    continue;
                };
                let matches = match (selector, evaluation.key.target) {
                    (
                        EvaluationSelector::WholeWork { slot: None },
                        NativeEvaluationTarget::Work { .. },
                    ) => true,
                    (
                        EvaluationSelector::WholeWork { slot: Some(wanted) },
                        NativeEvaluationTarget::Work { slot, .. },
                    ) => slot == wanted,
                    (EvaluationSelector::Admission, NativeEvaluationTarget::Admission) => true,
                    (
                        EvaluationSelector::Increment { artifact: wanted },
                        NativeEvaluationTarget::Increment { artifact },
                    ) => wanted.is_none_or(|wanted| wanted == artifact),
                    _ => false,
                };
                if !matches
                    || document
                        .generation
                        .is_some_and(|wanted| evaluation.key.generation != wanted)
                {
                    continue;
                }
                if selected.is_none_or(|current| current.key.generation < evaluation.key.generation)
                {
                    selected = Some(evaluation);
                }
            }
            let (target, generation) = match selected {
                Some(evaluation) => (Some(evaluation.key.target), Some(evaluation.key.generation)),
                None => (None, document.generation),
            };
            let kind = match selector {
                EvaluationSelector::Admission => NativeContextKind::Admission,
                EvaluationSelector::Increment { .. } => NativeContextKind::Increment,
                EvaluationSelector::WholeWork { .. } => NativeContextKind::WholeWork,
            };
            reads(NativeReadRequest {
                consistency: ReadConsistency::AtLeast(evaluations.token),
                query: NativeReadQuery::ValidationContext(NativeContextQuery {
                    validation,
                    claim,
                    kind: Some(kind),
                    target,
                    generation,
                    results_after: document.results_after.map(ObjectRevision),
                    limit: document.limit,
                }),
                max_items: READ_ITEMS,
            })?
        }
        NativeReadOperation::Standing(_) => linearizable(NativeReadQuery::Standing)?,
        NativeReadOperation::ClaimLineage(_) | NativeReadOperation::ClaimWait(_) => {
            // Composed by `read`; a plain page cannot serve them.
            return Err(InputError::Invalid("composite native read").into());
        }
    })
}

/// The committed outcome of one journaled operation, read from the owner by
/// its request key: the cross-tool recovery read when the local journal of
/// another adapter is not at hand.
pub fn outcome(key: RequestKey, reads: &mut Reads<'_>) -> Result<NativeReadPage, DriveError> {
    reads(NativeReadRequest {
        consistency: ReadConsistency::Linearizable,
        query: NativeReadQuery::Outcome(NativeInvocationRef::Request(key)),
        max_items: 1,
    })
}

/// One blocking bounded list. The host supplies the envelope, transport and
/// cancellation; the driver supplies only the request.
pub type Lists<'a> = dyn FnMut(NativeListRequest) -> Result<NativeListPage, DriveError> + 'a;

fn participant(value: &str, build: &BuildContext) -> Result<ParticipantId, DriveError> {
    if value == "self" {
        return Ok(build.actor);
    }
    Ok(ParticipantId(parse_id(value)?))
}
fn optional_participant(
    value: Option<&str>,
    build: &BuildContext,
) -> Result<Option<ParticipantId>, DriveError> {
    value.map(|value| participant(value, build)).transpose()
}
fn optional_id(value: Option<&str>) -> Result<Option<[u8; 16]>, DriveError> {
    Ok(value.map(parse_id).transpose()?)
}
fn hex_bytes(text: &str) -> Result<Vec<u8>, DriveError> {
    let bytes = text.as_bytes();
    if bytes.is_empty()
        || !bytes.len().is_multiple_of(2)
        || bytes.len() > MAX_NATIVE_LIST_CURSOR_BYTES.saturating_mul(2)
    {
        return Err(InputError::Invalid("list cursor").into());
    }
    let digit = |byte: u8| -> Result<u8, DriveError> {
        match byte {
            b'0'..=b'9' => Ok(byte.wrapping_sub(b'0')),
            b'a'..=b'f' => Ok(byte.wrapping_sub(b'a').wrapping_add(10)),
            _ => Err(InputError::Invalid("list cursor").into()),
        }
    };
    let mut decoded = Vec::new();
    decoded
        .try_reserve_exact(bytes.len() / 2)
        .map_err(|_| InputError::Capacity)?;
    for pair in bytes.chunks_exact(2) {
        let (Some(high), Some(low)) = (pair.first(), pair.get(1)) else {
            return Err(InputError::Invalid("list cursor").into());
        };
        decoded.push(digit(*high)?.wrapping_shl(4) | digit(*low)?);
    }
    Ok(decoded)
}
fn relation(
    document: &focal_client::operations::NativeRelationDocument,
    build: &BuildContext,
) -> Result<Relation, DriveError> {
    let kind = focal_client::input::parse_relation(&document.kind)?;
    let Some(target) = document.target.strip_prefix("claim:") else {
        return Err(InputError::Invalid("a listed relation targets claim:ID").into());
    };
    if let Some(artifact) = target.strip_prefix("artifact:") {
        // A list filter names the evidence artifact; an omitted hash matches
        // any committed hash of it.
        let (id, hash) = artifact.split_once('@').unwrap_or((artifact, ""));
        return Ok(Relation {
            kind,
            target: RelationTarget::Evidence(focal_model::ArtifactRef {
                id: focal_model::ArtifactId(parse_id(id)?),
                hash: if hash.is_empty() {
                    focal_model::ContentHash([0; 32])
                } else {
                    focal_client::input::parse_hash(hash)?
                },
            }),
        });
    }
    let target = target.strip_prefix("claim:").unwrap_or(target);
    Ok(Relation {
        kind,
        target: RelationTarget::Object(ObjectRef::claim(build.ledger, ClaimId(parse_id(target)?))),
    })
}

/// Translate one list document into its wire request. Human spellings
/// (statuses, actions, verdicts, `self`) are resolved here; the node
/// validates the bounds again before serving.
pub fn list_request(
    operation: &NativeListOperation,
    build: &BuildContext,
) -> Result<NativeListRequest, DriveError> {
    use focal_client::input::{parse_action, parse_scope_kind, parse_status, parse_verdict};
    let filter = match operation {
        NativeListOperation::ClaimList(document) => NativeListFilter::Claims {
            issuer: optional_participant(document.issuer.as_deref(), build)?,
            subject: optional_participant(document.subject.as_deref(), build)?,
            status: document.status.as_deref().map(parse_status).transpose()?,
            action: document.action.as_deref().map(parse_action).transpose()?,
            scope: document
                .scope
                .as_ref()
                .map(|scope| {
                    Ok::<_, DriveError>(Scope {
                        kind: parse_scope_kind(&scope.kind)?,
                        key: scope.key.clone(),
                    })
                })
                .transpose()?,
            relation: document
                .relation
                .as_ref()
                .map(|document| relation(document, build))
                .transpose()?,
            created_after: document.created_after.map(SessionSeq),
        },
        NativeListOperation::ArtifactList(document) => NativeListFilter::Artifacts {
            producer: optional_participant(document.producer.as_deref(), build)?,
            kind: document.kind.clone(),
            schema: document
                .schema
                .as_deref()
                .map(focal_client::input::parse_hash)
                .transpose()?,
            input: optional_id(document.input.as_deref())?.map(ObjectId),
        },
        NativeListOperation::ValidationList(document) => NativeListFilter::Definitions {
            claim: optional_id(document.claim.as_deref())?.map(ClaimId),
            evaluator: optional_participant(document.evaluator.as_deref(), build)?,
        },
        NativeListOperation::EvaluationList(document) => NativeListFilter::Evaluations {
            claim: optional_id(document.claim.as_deref())?.map(ClaimId),
            validation: optional_id(document.validation.as_deref())?.map(ValidationId),
            evaluator: optional_participant(document.evaluator.as_deref(), build)?,
            verdict: document.verdict.as_deref().map(parse_verdict).transpose()?,
        },
        NativeListOperation::TestamentList(document) => NativeListFilter::Responses {
            claim: ClaimId(parse_id(&document.claim)?),
        },
        NativeListOperation::ReceiptList(document) => NativeListFilter::Receipts {
            holder: optional_participant(document.holder.as_deref(), build)?,
            claim: optional_id(document.claim.as_deref())?.map(ClaimId),
        },
        NativeListOperation::MonitorList(document) => NativeListFilter::Monitors {
            claim: ClaimId(parse_id(&document.claim)?),
        },
        NativeListOperation::EventList(document) => NativeListFilter::Events {
            after: document
                .after
                .map(|after| (SessionSeq(after.sequence), after.ordinal)),
        },
    };
    let page = operation.page();
    Ok(NativeListRequest {
        filter,
        cursor: page
            .cursor
            .as_deref()
            .map(hex_bytes)
            .transpose()?
            .map(NativeListCursor),
        max_items: page.limit,
        max_visits: page.max_visits,
    })
}

/// Serve one page of a list document.
pub fn list(
    operation: &NativeListOperation,
    build: &BuildContext,
    lists: &mut Lists<'_>,
) -> Result<NativeListPage, DriveError> {
    lists(list_request(operation, build)?)
}
