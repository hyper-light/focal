//! Translation of a sealed legacy prefix into the first native prefix (23 §5).
//!
//! Every replica computes the same rows from the same legacy core, writes them
//! as one root image and restores that image through the checkpoint recovery
//! path, so an imported prefix passes exactly the validation a restored
//! checkpoint passes. No legacy command is rerun and no lifecycle fact is
//! invented: claims keep their recorded status history as `Imported` events,
//! their acceptance policy is empty, testaments, evidence sets, validations and
//! runs are retained verbatim as frozen legacy rows, and artifacts are
//! re-verified from local content under the derived import request key.
use super::record_codec::{
    CodecError, EncodingLimits, InspectionLimits, checkpoint, checkpoint::StructuralCheckpoint,
    recovery,
};
use super::*;
use crate::State as LegacyState;
use focal_evidence::{ContentError, NativeCustodyReader, NativeSchemaVerifier, inline_reference};
use focal_memory::{BudgetKind, BudgetLane};
use focal_model::lifecycle::{
    Binding,
    aggregation::{AcceptancePolicy, RegistrationSet},
    artifact_descriptor::{ArtifactDescriptor, ArtifactSpec, ContentPointer, PayloadSpec},
    claim::{
        ClaimCut, ClaimDefinition, ClaimOrigin, ClaimResponseValue, ClaimSnapshotV1, ClaimState,
        ClaimTerminalSnapshotV1, ReceiptEntitlement,
    },
    graph::{self, Kind, Obligation},
    scope::{
        MonitorDisposition, OwnedChildSnapshotV1, RegistrySnapshotSource, RegistrySnapshotV1,
        ScopeLimits, ScopeSnapshotSource, ScopeSnapshotV1,
    },
    succession::{Correction, CorrectionKind, Lineage},
};
use focal_model::{
    ArtifactPayload, ContentDomainId, ObjectId, ObjectRef, ObjectRevision, RelationKind,
    RelationTarget,
};

#[cfg(test)]
#[path = "import_tests.rs"]
mod tests;

/// Why a legacy prefix cannot become a native prefix. Nothing is activated on
/// any variant; the ledger stays legacy and keeps serving.
#[derive(Debug, thiserror::Error)]
pub enum ImportError {
    #[error("legacy {kind} {id:?} cannot be represented natively: {error}")]
    Object {
        kind: &'static str,
        id: ObjectId,
        error: ContractError,
    },
    #[error("native import: {0}")]
    Native(#[from] NativeError),
    #[error("import image: {0}")]
    Codec(CodecError),
    #[error("import content: {0}")]
    Content(#[from] ContentError),
    #[error("import custody: {0}")]
    Evidence(#[from] focal_evidence::NativeEvidenceError),
    #[error("import memory: {0}")]
    Memory(#[from] MemoryError),
    #[error("frozen legacy codec: {0}")]
    Legacy(postcard::Error),
    #[error("import bound exceeded: {0}")]
    Capacity(&'static str),
    #[error("legacy prefix is not importable: {0}")]
    Unsupported(&'static str),
}

/// Everything a replica needs to translate deterministically. The chunk size
/// and manifest bound travel with the activation, never from node config.
#[derive(Debug, Clone, Copy)]
pub struct ImportRequest<'a> {
    pub ledger: LedgerId,
    pub logical_time: u64,
    /// The activation record hash: the intent identity of the import outcome
    /// and the cause of every cut it records.
    pub intent: ContentHash,
    pub content_domain: ContentDomainId,
    pub chunk_bytes: usize,
    pub max_manifest_bytes: usize,
    pub range: RangeId,
    pub limits: &'a recovery::Limits,
    pub encoding: EncodingLimits,
    pub inspection: InspectionLimits,
}

/// The restored native prefix and the exact image every replica must match.
pub struct Imported {
    pub core: Core<NativeState>,
    pub image: Vec<u8>,
    pub root: ContentHash,
}

/// Inline legacy payloads that must exist in the local content tree before
/// custody can be re-verified; sealing them with the request's chunk size
/// installs exactly the references the translation names.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InlinePayload<'a> {
    pub artifact: ArtifactId,
    pub bytes: &'a [u8],
}

const IMPORT_SEQUENCE: SessionSeq = SessionSeq(1);
/// The image names a canonical incarnation so every replica hashes the same
/// bytes; each restores it under its own process-local range.
pub const IMPORT_RANGE: RangeId = RangeId(1);

fn add(a: usize, b: usize) -> Result<usize, ImportError> {
    a.checked_add(b)
        .ok_or(ImportError::Capacity("import row count"))
}
fn object(kind: &'static str, id: [u8; 16], error: ContractError) -> ImportError {
    ImportError::Object {
        kind,
        id: ObjectId(id),
        error,
    }
}
fn revision(n: usize) -> Result<ObjectRevision, ImportError> {
    u64::try_from(n)
        .map(ObjectRevision)
        .map_err(|_| ImportError::Capacity("claim revision"))
}
fn count32(n: usize) -> Result<u32, ImportError> {
    u32::try_from(n).map_err(|_| ImportError::Capacity("import outcome count"))
}
fn cut(intent: ContentHash) -> ClaimCut {
    ClaimCut {
        position: IMPORT_SEQUENCE,
        cause: intent,
    }
}

/// Inline payloads of the legacy prefix, in artifact order.
pub fn inline_payloads(legacy: &LegacyState) -> impl Iterator<Item = InlinePayload<'_>> {
    legacy
        .artifacts
        .iter()
        .filter_map(|(id, artifact)| match &artifact.content().payload {
            ArtifactPayload::Inline(bytes) => Some(InlinePayload {
                artifact: *id,
                bytes,
            }),
            ArtifactPayload::Content(_) => None,
        })
}

struct ImportScope {
    fields: ScopeSnapshotV1,
    roots: Vec<WaitPredicate>,
}
impl ScopeSnapshotSource for &ImportScope {
    fn fields(&self) -> ScopeSnapshotV1 {
        self.fields
    }
    type Roots<'a>
        = std::iter::Map<
        std::iter::Copied<std::slice::Iter<'a, WaitPredicate>>,
        fn(WaitPredicate) -> Result<WaitPredicate, ContractError>,
    >
    where
        Self: 'a;
    fn roots(&self) -> Self::Roots<'_> {
        self.roots.iter().copied().map(Ok)
    }
}
struct ImportScopes {
    fields: RegistrySnapshotV1,
    scopes: Vec<ImportScope>,
}
impl RegistrySnapshotSource for ImportScopes {
    fn fields(&self) -> RegistrySnapshotV1 {
        self.fields
    }
    type Scope<'a>
        = &'a ImportScope
    where
        Self: 'a;
    type Scopes<'a>
        = std::iter::Map<
        std::slice::Iter<'a, ImportScope>,
        fn(&'a ImportScope) -> Result<&'a ImportScope, ContractError>,
    >
    where
        Self: 'a;
    type Children<'a>
        = std::iter::Empty<Result<OwnedChildSnapshotV1, ContractError>>
    where
        Self: 'a;
    fn scopes(&self) -> Self::Scopes<'_> {
        self.scopes.iter().map(Ok)
    }
    fn children(&self) -> Self::Children<'_> {
        std::iter::empty()
    }
}

struct Rows {
    rows: Vec<(Key, Row)>,
    events: u32,
    meta: Meta,
}
impl Rows {
    fn push(&mut self, key: Key, row: Row) -> Result<(), ImportError> {
        if self.rows.len() == self.rows.capacity() {
            return Err(ImportError::Capacity("import row estimate"));
        }
        self.rows.push((key, row));
        Ok(())
    }
    /// Every secondary index row a translated primary row implies (doc 22
    /// §9), written as ordinary rows of the import image so the restored
    /// checkpoint validates them like any other prefix. Import writes only
    /// fresh rows, so a derived deletion is a contract violation.
    fn index(
        &mut self,
        derive: impl FnOnce(
            &mut dyn FnMut(super::index_rows::IndexChange) -> Result<(), NativeError>,
        ) -> Result<(), NativeError>,
    ) -> Result<(), ImportError> {
        let mut failure = None;
        let mut sink = |change: super::index_rows::IndexChange| match change {
            super::index_rows::IndexChange::Put(key) => {
                self.push(key, Row::Index).map_err(|error| {
                    failure = Some(error);
                    NativeError::Capacity("import row estimate")
                })
            }
            super::index_rows::IndexChange::Delete(_) => Err(ContractError::InvalidManifest.into()),
        };
        match derive(&mut sink) {
            Ok(()) => Ok(()),
            Err(error) => Err(failure.unwrap_or(ImportError::from(error))),
        }
    }
    fn event(&mut self, fact: NativeFact) -> Result<(), ImportError> {
        let ordinal = self.events;
        let event = NativeEvent {
            invocation: NativeInvocation::Import,
            sequence: IMPORT_SEQUENCE,
            ordinal,
            fact,
        };
        let stored = StoredEvent::pack(event).map_err(NativeError::from)?;
        let row = OwnedEvent::new(stored)?;
        self.push(Key::Event(IMPORT_SEQUENCE, ordinal), Row::Event(row))?;
        self.events = self
            .events
            .checked_add(1)
            .ok_or(ImportError::Capacity("import events"))?;
        self.meta.events = add(self.meta.events, 1)?;
        Ok(())
    }
}

/// Upper bound on the rows the translation writes, computed before any
/// allocation so the whole workspace is reserved once.
fn estimate(legacy: &LegacyState) -> Result<usize, ImportError> {
    let mut rows = 2usize; // Meta and the import outcome.
    for claim in legacy.claims.values() {
        let content = claim.content();
        let lifecycle = claim.lifecycle();
        // Claim row, receipt row and event, status events, release event,
        // then the identity, issuer, subject, status and creation index rows
        // (doc 22 §7).
        rows = add(rows, 4)?;
        rows = add(rows, super::index_rows::CLAIM_ROWS)?;
        rows = add(rows, lifecycle.history.len())?;
        let edges = content
            .dependencies(RelationKind::DependsOn)
            .count()
            .checked_add(content.dependencies(RelationKind::Awaits).count())
            .ok_or(ImportError::Capacity("import graph edges"))?;
        // Each edge is one incoming link plus at most one head.
        rows = add(
            rows,
            edges
                .checked_mul(2)
                .ok_or(ImportError::Capacity("import graph"))?,
        )?;
    }
    for monitor in legacy.monitors.values() {
        // Allocation row, two events, one link and one head per root.
        rows = add(rows, 3)?;
        rows = add(
            rows,
            monitor
                .roots
                .len()
                .checked_mul(2)
                .ok_or(ImportError::Capacity("import monitor roots"))?,
        )?;
    }
    // Artifact row, identity row and event, then the producer, kind, schema
    // and creation index rows plus one per recorded input.
    rows = add(
        rows,
        legacy
            .artifacts
            .len()
            .checked_mul(3)
            .ok_or(ImportError::Capacity("import artifacts"))?,
    )?;
    for artifact in legacy.artifacts.values() {
        rows = add(
            rows,
            super::index_rows::artifact_rows(artifact.content().inputs.len())
                .map_err(|_| ImportError::Capacity("import artifact index"))?,
        )?;
    }
    rows = add(rows, legacy.validations.len())?;
    rows = add(rows, legacy.testaments.len())?;
    rows = add(rows, legacy.evidence_sets.len())?;
    rows = add(rows, legacy.runs.len())?;
    Ok(rows)
}

fn scope_limits(request: &ImportRequest<'_>, monitors: usize) -> ScopeLimits {
    let native = request.limits.native;
    ScopeLimits {
        scopes: monitors.max(256).min(native.monitors.max(monitors)),
        roots: 256,
        children: 256,
    }
}

/// Translate, image and restore. The returned core is the authoritative native
/// prefix one; `root` is what the activation record must carry and every
/// replica must reproduce.
pub fn import<S: NativeSchemaVerifier, R: NativeCustodyReader>(
    legacy: &LegacyState,
    request: ImportRequest<'_>,
    budget: MemoryBudget,
    store: &R,
    schemas: &S,
) -> Result<Imported, ImportError> {
    if legacy.ledger != request.ledger {
        return Err(ImportError::Unsupported(
            "legacy core belongs to another ledger",
        ));
    }
    if legacy.sequence.0 == 0 {
        return Err(ImportError::Unsupported(
            "empty legacy prefix needs no import",
        ));
    }
    if request.chunk_bytes == 0 {
        return Err(ImportError::Unsupported("import chunk size"));
    }
    let estimate = estimate(legacy)?;
    let row_bytes = estimate
        .checked_mul(size_of::<(Key, Row)>())
        .ok_or(ImportError::Capacity("import workspace"))?;
    let workspace = row_bytes
        .checked_add(request.encoding.bytes)
        .and_then(|n| n.checked_add(request.limits.native.preparation_bytes))
        .ok_or(ImportError::Capacity("import workspace"))?;
    let _workspace = budget
        .reserve(BudgetKind::Recovery, BudgetLane::Completion, workspace)?
        .commit();
    let mut rows = Vec::new();
    rows.try_reserve_exact(estimate)
        .map_err(|_| MemoryError::AllocationFailed)?;
    if rows.capacity() < estimate {
        return Err(MemoryError::AllocationFailed.into());
    }
    let mut out = Rows {
        rows,
        events: 0,
        meta: Meta {
            logical_time: request.logical_time,
            ..Meta::default()
        },
    };
    claims(legacy, &request, &mut out)?;
    graph_index(legacy, &mut out)?;
    monitors(legacy, &request, &mut out)?;
    artifacts(legacy, &request, &budget, store, schemas, &mut out)?;
    legacy_rows(legacy, &mut out)?;
    let outcome = NativeOutcome {
        ledger: request.ledger,
        invocation: NativeInvocation::Import,
        sequence: IMPORT_SEQUENCE,
        logical_time: request.logical_time,
        operation: NativeOperation::Import,
        intent: request.intent,
        created: count32(legacy.claims.len())?,
        changed: 0,
        definitions: 0,
        evaluations: 0,
        artifacts: count32(legacy.artifacts.len())?,
        results: 0,
        receipts: count32(out.meta.receipts)?,
        responses: 0,
        result_testaments: 0,
        events: out.events,
    };
    out.meta.outcomes = 1;
    out.push(
        Key::Outcome(NativeInvocation::Import),
        Row::Outcome(outcome),
    )?;
    let meta = out.meta;
    out.push(Key::Meta, Row::Meta(meta))?;
    let Rows { mut rows, .. } = out;
    rows.sort_unstable_by(|a, b| a.0.cmp(&b.0));
    if rows
        .windows(2)
        .any(|pair| matches!(pair, [a, b] if a.0 == b.0))
    {
        return Err(ImportError::Unsupported("legacy identities collide"));
    }
    let frame = checkpoint::RootFrame {
        ledger: request.ledger,
        profile: NativeContentProfile::ProjectionOnly,
        range: IMPORT_RANGE,
        prefix: IMPORT_SEQUENCE.0,
        count: rows.len(),
        // One member under the import identity, so every replica's image
        // is byte-identical; the producer identity is the caller's.
        layout: &[super::ranges::RangeBoundary {
            id: IMPORT_RANGE,
            start: None,
        }],
        layout_epoch: 0,
    };
    let (image, root) =
        checkpoint::encode_rows(frame, &rows, request.encoding).map_err(ImportError::Codec)?;
    drop(rows);
    let structural =
        StructuralCheckpoint::inspect(&image, request.inspection).map_err(ImportError::Codec)?;
    let core = recovery::restore(
        &structural,
        request.range,
        *request.limits,
        budget,
        store,
        schemas,
    )?;
    Ok(Imported { core, image, root })
}

fn claims(
    legacy: &LegacyState,
    request: &ImportRequest<'_>,
    out: &mut Rows,
) -> Result<(), ImportError> {
    let ledger = request.ledger;
    let intent = request.intent;
    for (id, claim) in &legacy.claims {
        let content = claim.content();
        let lifecycle = claim.lifecycle();
        let fail = |error| object("claim", id.0, error);
        if content.ledger != ledger {
            return Err(fail(ContractError::WrongLedger));
        }
        let issuer = content.issuer().ok_or(fail(ContractError::InvalidTarget))?;
        let subject = content
            .subject()
            .ok_or(fail(ContractError::InvalidTarget))?;
        let cause = content.cause().ok_or(fail(ContractError::InvalidTarget))?;
        if lifecycle.history.is_empty() {
            return Err(fail(ContractError::InvalidTransition));
        }
        let monitors: Vec<&focal_model::Monitor> = legacy
            .monitors
            .values()
            .filter(|monitor| monitor.owner == *id)
            .collect();
        let released_monitors = monitors
            .iter()
            .filter(|monitor| monitor.released.is_some())
            .count();
        let local_complete_event =
            usize::from(lifecycle.local_complete && !lifecycle.status.is_terminal());
        // One native event per recorded fact; each advances the revision.
        let total = add(
            add(
                add(lifecycle.history.len(), usize::from(lifecycle.released))?,
                local_complete_event,
            )?,
            add(monitors.len(), released_monitors)?,
        )?;
        let binding_at = |rev: usize| -> Result<Binding, ImportError> {
            Ok(Binding {
                ledger,
                object: ObjectId(id.0),
                content: claim.content_hash(),
                revision: revision(rev)?,
            })
        };
        let original = binding_at(1)?;
        let current = binding_at(total)?;
        let mut obligations = Vec::new();
        for (kind, relation) in [
            (Kind::DependsOn, RelationKind::DependsOn),
            (Kind::Awaits, RelationKind::Awaits),
        ] {
            for target in content.dependencies(relation) {
                if !legacy.claims.contains_key(&target) {
                    return Err(fail(ContractError::InvalidTarget));
                }
                obligations
                    .try_reserve(1)
                    .map_err(|_| MemoryError::AllocationFailed)?;
                obligations.push(Obligation { kind, target });
            }
        }
        obligations.sort_unstable();
        let graph = graph::Declaration::new(&obligations, request.limits.native.plan_edges)
            .map_err(|error| object("claim graph", id.0, error))?;
        let mut corrections = Vec::new();
        for relation in &content.relations {
            let kind = match relation.kind {
                RelationKind::Supersedes => CorrectionKind::Supersedes,
                RelationKind::Amends => CorrectionKind::Amends,
                _ => continue,
            };
            let RelationTarget::Object(predecessor) = &relation.target else {
                return Err(fail(ContractError::InvalidTarget));
            };
            corrections
                .try_reserve(1)
                .map_err(|_| MemoryError::AllocationFailed)?;
            corrections.push(Correction {
                kind,
                predecessor: *predecessor,
            });
        }
        let lineage = Lineage::new(
            original,
            cause,
            &corrections,
            request.limits.native.plan_edges,
        )
        .map_err(|error| object("claim lineage", id.0, error))?;
        let acceptance = AcceptancePolicy::legacy_empty(original, issuer)
            .map_err(|error| object("claim policy", id.0, error))?;
        let terminal = lifecycle.status.is_terminal();
        let definition = ClaimDefinition {
            binding: original,
            issuer,
            subject,
            deadline: content.deadline,
            max_responses: 1,
            created: IMPORT_SEQUENCE,
            graph,
            lineage,
            acceptance,
            scope_limits: scope_limits(request, monitors.len()),
        };
        let receipt = lifecycle
            .receipt
            .as_ref()
            .map(|receipt| ReceiptEntitlement {
                holder: receipt.holder,
                fence: receipt.fence,
            });
        let fields = ClaimSnapshotV1 {
            binding: current,
            issuer,
            subject,
            created: IMPORT_SEQUENCE,
            status: lifecycle.status,
            receipt,
            responses: 0,
            max_responses: 1,
            deadline: content.deadline,
            local_complete: lifecycle.local_complete,
            local_sealed_at: (lifecycle.local_complete || terminal).then_some(IMPORT_SEQUENCE),
            terminal_cut: terminal.then(|| ClaimTerminalSnapshotV1::Explicit(cut(intent))),
            origin: ClaimOrigin::Legacy,
        };
        let mut scopes = Vec::new();
        scopes
            .try_reserve_exact(monitors.len())
            .map_err(|_| MemoryError::AllocationFailed)?;
        for monitor in &monitors {
            let mut roots = Vec::new();
            roots
                .try_reserve_exact(monitor.roots.len())
                .map_err(|_| MemoryError::AllocationFailed)?;
            roots.extend(monitor.roots.iter().copied());
            scopes.push(ImportScope {
                fields: ScopeSnapshotV1 {
                    id: monitor.id,
                    roots: roots.len(),
                    deadline: monitor.deadline,
                    registered: IMPORT_SEQUENCE,
                    disposition: monitor
                        .released
                        .map(|_| MonitorDisposition::Released(cut(intent))),
                    last_rebinding: None,
                },
                roots,
            });
        }
        let source = ImportScopes {
            fields: RegistrySnapshotV1 {
                owner: original,
                limits: definition.scope_limits,
                scopes: scopes.len(),
                children: 0,
                released: lifecycle.released.then(|| cut(intent)),
                last_cut: if scopes.is_empty() && !lifecycle.released {
                    SessionSeq(0)
                } else {
                    IMPORT_SEQUENCE
                },
            },
            scopes,
        };
        let visits = request.limits.work.model;
        let scope_plan = focal_model::lifecycle::scope::Registry::prepare_hydration_v1(
            current,
            IMPORT_SEQUENCE,
            definition.scope_limits,
            &source,
            visits,
        )
        .map_err(|error| object("claim scopes", id.0, error))?;
        let responses: [ClaimResponseValue<'_>; 0] = [];
        let plan = ClaimState::prepare_hydration_v1(
            definition,
            fields,
            responses.as_slice(),
            scope_plan,
            visits,
        )
        .map_err(|error| object("claim state", id.0, error))?;
        let charge = plan
            .construction_charge()
            .map_err(|error| object("claim charge", id.0, error))?;
        let build_visits = plan
            .build_visits()
            .map_err(|error| object("claim visits", id.0, error))?;
        let state = plan
            .build(charge, build_visits)
            .map_err(|error| object("claim build", id.0, error))?;
        let registrations = RegistrationSet::new(
            &state,
            request.limits.native.evaluations_per_claim,
            request.limits.native.preparation_bytes,
        )
        .map_err(|error| object("claim registrations", id.0, error))?;
        out.index(|sink| {
            super::index_rows::claim(None, &state, None, &super::index_rows::NeverConsumed, sink)
        })?;
        let owned = OwnedClaim::new(state, registrations)?;
        out.push(Key::Claim(*id), Row::Claim(owned))?;
        out.meta.claims = add(out.meta.claims, 1)?;
        // Recorded status history, then the scope facts, in legacy order.
        let mut rev = 0usize;
        let mut last_status = lifecycle.status;
        let mut before = None;
        let mut imported_binding = original;
        for fact in &lifecycle.history {
            if fact.sequence.0 == 0 || fact.sequence > legacy.sequence {
                return Err(fail(ContractError::InvalidCut));
            }
            rev = add(rev, 1)?;
            let after = binding_at(rev)?;
            out.event(NativeFact::Claim(NativeClaimEvent {
                kind: NativeEventKind::Imported(fact.sequence),
                graph: None,
                owned_child: None,
                before,
                after,
                status: fact.status,
            }))?;
            before = Some(after);
            imported_binding = after;
            last_status = fact.status;
        }
        if last_status != lifecycle.status {
            return Err(fail(ContractError::InvalidTransition));
        }
        let mut plain = |out: &mut Rows, kind: NativeEventKind, rev: &mut usize| {
            *rev = add(*rev, 1)?;
            let after = binding_at(*rev)?;
            out.event(NativeFact::Claim(NativeClaimEvent {
                kind,
                graph: None,
                owned_child: None,
                before,
                after,
                status: lifecycle.status,
            }))?;
            before = Some(after);
            Ok::<(), ImportError>(())
        };
        if local_complete_event == 1 {
            plain(out, NativeEventKind::LocallyComplete, &mut rev)?;
        }
        if lifecycle.released {
            plain(out, NativeEventKind::OwnerReleased, &mut rev)?;
        }
        for monitor in &monitors {
            plain(
                out,
                NativeEventKind::Monitor(NativeMonitorEvent::Registered {
                    id: monitor.id,
                    cut: cut(intent),
                }),
                &mut rev,
            )?;
        }
        for monitor in &monitors {
            if monitor.released.is_some() {
                plain(
                    out,
                    NativeEventKind::Monitor(NativeMonitorEvent::Released {
                        id: monitor.id,
                        cut: cut(intent),
                    }),
                    &mut rev,
                )?;
            }
        }
        if rev != total {
            return Err(fail(ContractError::InvalidCut));
        }
        if let Some(receipt) = &lifecycle.receipt {
            out.push(
                Key::Receipt(receipt.fence.receipt),
                Row::Receipt(NativeReceipt {
                    claim: *id,
                    fence: receipt.fence,
                    holder: receipt.holder,
                    acquired: IMPORT_SEQUENCE,
                }),
            )?;
            out.meta.receipts = add(out.meta.receipts, 1)?;
            out.event(NativeFact::Receipt {
                claim: imported_binding,
                fence: receipt.fence,
                holder: receipt.holder,
            })?;
        }
    }
    Ok(())
}

fn graph_index(legacy: &LegacyState, out: &mut Rows) -> Result<(), ImportError> {
    let mut edges: Vec<(ClaimId, ClaimId)> = Vec::new();
    for (id, claim) in &legacy.claims {
        let content = claim.content();
        for relation in [RelationKind::DependsOn, RelationKind::Awaits] {
            for target in content.dependencies(relation) {
                edges
                    .try_reserve(1)
                    .map_err(|_| MemoryError::AllocationFailed)?;
                edges.push((target, *id));
            }
        }
    }
    edges.sort_unstable();
    edges.dedup();
    let mut position = 0usize;
    while let Some((target, _)) = edges.get(position).copied() {
        let mut end = position;
        while edges.get(end).is_some_and(|edge| edge.0 == target) {
            end = add(end, 1)?;
        }
        let members = edges
            .get(position..end)
            .ok_or(ImportError::Capacity("import graph"))?;
        let mut next = None;
        for (_, dependent) in members.iter().rev() {
            out.push(
                Key::IncomingLink(target, *dependent),
                Row::IncomingLink(incoming_graph::IncomingLink { next }),
            )?;
            next = Some(*dependent);
        }
        out.push(
            Key::IncomingHead(target),
            Row::IncomingHead(incoming_graph::IncomingHead {
                head: next,
                count: members.len(),
            }),
        )?;
        position = end;
    }
    Ok(())
}

fn monitors(
    legacy: &LegacyState,
    request: &ImportRequest<'_>,
    out: &mut Rows,
) -> Result<(), ImportError> {
    let ledger = request.ledger;
    // Live links per target, in monitor order, plus tombstones for released ones.
    let mut links: Vec<(ClaimId, focal_model::MonitorId)> = Vec::new();
    for (id, monitor) in &legacy.monitors {
        let fail = |error| object("monitor", id.0, error);
        let owner = legacy
            .claims
            .get(&monitor.owner)
            .ok_or(fail(ContractError::InvalidTarget))?;
        if monitor.id != *id || monitor.roots.is_empty() {
            return Err(fail(ContractError::InvalidTarget));
        }
        out.push(
            Key::Monitor(*id),
            Row::Monitor(monitor_index::MonitorAllocation {
                owner: Binding {
                    ledger,
                    object: ObjectId(monitor.owner.0),
                    content: owner.content_hash(),
                    revision: ObjectRevision(1),
                },
                registered: IMPORT_SEQUENCE,
                deadline: monitor.deadline,
            }),
        )?;
        out.meta.monitors = add(out.meta.monitors, 1)?;
        let mut targets: Vec<ClaimId> = Vec::new();
        for root in &monitor.roots {
            let target = match root {
                WaitPredicate::Satisfied(id)
                | WaitPredicate::Terminal(id)
                | WaitPredicate::Released(id) => *id,
            };
            if !legacy.claims.contains_key(&target) {
                return Err(fail(ContractError::InvalidTarget));
            }
            if !targets.contains(&target) {
                targets
                    .try_reserve(1)
                    .map_err(|_| MemoryError::AllocationFailed)?;
                targets.push(target);
            }
        }
        for target in targets {
            if monitor.released.is_some() {
                out.push(Key::MonitorLink(target, *id), Row::MonitorLink(None))?;
                out.meta.monitor_links = add(out.meta.monitor_links, 1)?;
            } else {
                links
                    .try_reserve(1)
                    .map_err(|_| MemoryError::AllocationFailed)?;
                links.push((target, *id));
            }
        }
    }
    links.sort_unstable();
    let mut position = 0usize;
    while let Some((target, _)) = links.get(position).copied() {
        let mut end = position;
        while links.get(end).is_some_and(|link| link.0 == target) {
            end = add(end, 1)?;
        }
        let members = links
            .get(position..end)
            .ok_or(ImportError::Capacity("import monitor links"))?;
        let mut previous = None;
        for (index, (_, id)) in members.iter().enumerate() {
            let owner = legacy
                .monitors
                .get(id)
                .map(|monitor| monitor.owner)
                .ok_or(ImportError::Capacity("import monitor owner"))?;
            let next = members.get(add(index, 1)?).map(|link| link.1);
            out.push(
                Key::MonitorLink(target, *id),
                Row::MonitorLink(Some(monitor_index::MonitorLink {
                    owner,
                    registered: IMPORT_SEQUENCE,
                    stamp: IMPORT_SEQUENCE,
                    previous,
                    next,
                })),
            )?;
            out.meta.monitor_links = add(out.meta.monitor_links, 1)?;
            previous = Some(*id);
        }
        out.push(
            Key::MonitorHead(target),
            Row::MonitorHead(monitor_index::MonitorHead {
                head: members.first().map(|link| link.1),
                count: members.len(),
            }),
        )?;
        position = end;
    }
    Ok(())
}

fn artifacts<S: NativeSchemaVerifier, R: NativeCustodyReader>(
    legacy: &LegacyState,
    request: &ImportRequest<'_>,
    budget: &MemoryBudget,
    store: &R,
    schemas: &S,
    out: &mut Rows,
) -> Result<(), ImportError> {
    let ledger = request.ledger;
    for (id, artifact) in &legacy.artifacts {
        let content = artifact.content();
        let fail = |error| object("artifact", id.0, error);
        if content.ledger != ledger {
            return Err(fail(ContractError::WrongLedger));
        }
        let (payload, expected) = match &content.payload {
            ArtifactPayload::Inline(bytes) => {
                let reference = inline_reference(
                    request.content_domain,
                    bytes,
                    request.chunk_bytes,
                    request.max_manifest_bytes,
                )?;
                (
                    PayloadSpec::Inline(bytes.as_slice()),
                    ContentPointer {
                        domain: reference.domain,
                        root: reference.root,
                        length: reference.length,
                        class: reference.class,
                    },
                )
            }
            ArtifactPayload::Content(reference) => {
                let pointer = ContentPointer {
                    domain: reference.domain,
                    root: reference.root,
                    length: reference.length,
                    class: reference.class,
                };
                (PayloadSpec::Content(pointer), pointer)
            }
        };
        let mut inputs: Vec<ObjectRef> = Vec::new();
        inputs
            .try_reserve_exact(content.inputs.len())
            .map_err(|_| MemoryError::AllocationFailed)?;
        inputs.extend(content.inputs.iter().copied());
        let mut visibility: Vec<&str> = Vec::new();
        visibility
            .try_reserve_exact(content.visibility.len())
            .map_err(|_| MemoryError::AllocationFailed)?;
        visibility.extend(content.visibility.iter().map(String::as_str));
        let spec = ArtifactSpec {
            ledger,
            id: *id,
            schema: content.schema,
            kind: &content.kind,
            schema_hash: content.schema_hash,
            metadata: &content.metadata,
            payload,
            producer: content.producer,
            receipt: content.receipt,
            result: None,
            work: None,
            inputs: &inputs,
            visibility: &visibility,
        };
        let descriptor = ArtifactDescriptor::prepare(spec, request.limits.artifact)
            .and_then(|plan| plan.build())
            .map_err(fail)?;
        let key = import_request(ledger, *id, content.producer);
        let verified =
            store.recover_native_artifact(key, &descriptor, expected, 1, budget, schemas)?;
        let native = NativeArtifact::recover(descriptor, verified.custody(), key, expected, 1)
            .map_err(fail)?;
        let binding = native.descriptor().binding();
        let hash = native.descriptor().content_hash();
        out.index(|sink| super::index_rows::artifact(native.descriptor(), sink))?;
        out.push(
            Key::Artifact(*id),
            Row::Artifact(OwnedArtifact::new(native)?),
        )?;
        out.push(Key::ArtifactIdentity(hash), Row::ArtifactIdentity(*id))?;
        out.meta.artifacts = add(out.meta.artifacts, 1)?;
        out.event(NativeFact::Artifact { binding })?;
    }
    Ok(())
}

fn legacy_rows(legacy: &LegacyState, out: &mut Rows) -> Result<(), ImportError> {
    fn frozen<T: focal_model::durable_v1::V1>(value: &T) -> Result<OwnedLegacy, ImportError> {
        let bytes = focal_model::durable_v1::encode(value).map_err(ImportError::Legacy)?;
        Ok(OwnedLegacy::new(&bytes)?)
    }
    for (id, validation) in &legacy.validations {
        if !legacy.claims.contains_key(&validation.content().claim) {
            return Err(object("validation", id.0, ContractError::InvalidTarget));
        }
        out.push(
            Key::LegacyDefinition(*id),
            Row::LegacyDefinition(frozen(validation)?),
        )?;
        out.meta.legacy = add(out.meta.legacy, 1)?;
    }
    for (id, testament) in &legacy.testaments {
        if !legacy.claims.contains_key(&testament.content().claim) {
            return Err(object("testament", id.0, ContractError::InvalidTarget));
        }
        out.push(
            Key::LegacyTestament(*id),
            Row::LegacyTestament(frozen(testament)?),
        )?;
        out.meta.legacy = add(out.meta.legacy, 1)?;
    }
    for (id, set) in &legacy.evidence_sets {
        if set.id != *id || !legacy.claims.contains_key(&set.claim) {
            return Err(object("evidence set", id.0, ContractError::InvalidTarget));
        }
        out.push(
            Key::LegacyEvidenceSet(*id),
            Row::LegacyEvidenceSet(frozen(set)?),
        )?;
        out.meta.legacy = add(out.meta.legacy, 1)?;
    }
    let mut ordinal = 0u32;
    let mut current = None;
    for (id, run) in &legacy.runs {
        if run.id != *id
            || !legacy.validations.contains_key(&id.validation)
            || !legacy.claims.contains_key(&run.claim)
        {
            return Err(object(
                "validation run",
                id.validation.0,
                ContractError::InvalidTarget,
            ));
        }
        if current != Some(id.validation) {
            current = Some(id.validation);
            ordinal = 0;
        }
        out.push(
            Key::LegacyRun(id.validation, ordinal),
            Row::LegacyRun(frozen(run)?),
        )?;
        out.meta.legacy = add(out.meta.legacy, 1)?;
        ordinal = ordinal
            .checked_add(1)
            .ok_or(ImportError::Capacity("import validation runs"))?;
    }
    Ok(())
}
