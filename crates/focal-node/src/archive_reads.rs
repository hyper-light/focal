//! Participant reads of a retired family (the audit's F11): the object a
//! bundle holds, read from the bundle as a live core is read. A claim's
//! `Retired` continuation names the bundle; a client that kept an artifact,
//! validation, testament or receipt identity follows it through the claim it
//! belonged to. The bundle is fetched from this node's custody under the
//! request's tenant scope, verified structurally, hydrated with the decoders,
//! schema verification and custody recovery a checkpoint restore runs, and
//! the object built by the same documents a live read builds. Custody this
//! node does not hold is `Unavailable`; a bundle of another ledger is
//! `Unauthorized`; a row the bundle lacks is `Missing` — never one for the
//! other. The read is its own operation, so its latency — a bundle's read and
//! hydration — is never mistaken for a live read's.
use crate::native_reads::{self, Reader};
use focal_core::native::record_codec::archive::StructuralArchive;
use focal_core::native::record_codec::seal::{SealHeader, StructuralSeal};
use focal_core::native::{NativeContentProfile, NativeError};
use focal_evidence::{BuiltinNativeSchemas, ContentStore};
use focal_ledger::NativeSessionLimits;
use focal_memory::{BudgetKind, BudgetLane, MemoryBudget, RangeId};
use focal_model::*;
use focal_wire::*;

/// The most bytes a bundle read may take: the record encoding limits a
/// session hosts under (26 §4), so any bundle a core wrote fits — the
/// operator's bound (`operator_admin`) and the collector's.
const MAX_ARCHIVE_BYTES: usize = 6 << 20;

/// The most folds a sealed lookup descends: a fold covers at least two
/// rows, so the depth is at most the width of an ordinal.
const MAX_FOLD_DEPTH: u32 = u64::BITS;

/// The content a sealed read names: the seal's bundle as an object of the
/// ledger's tenant domain under the evidence class.
pub(crate) fn seal_reference(ledger: LedgerId, query: &NativeSealQuery) -> ContentRef {
    ContentRef {
        domain: ContentDomainId(ledger.tenant.0),
        root: query.bundle,
        length: query.bytes,
        class: ContentClass::Evidence,
    }
}
/// One page holding the sealed outcome, or `Missing` when the seal never
/// held it (the audit's F12). A participant reads its own principal's
/// outcomes only; a runtime any. A fold's bundle names the member holding
/// the generation, read in turn; custody this node lacks is `Unavailable`.
#[allow(clippy::too_many_arguments)] // The read's inputs, each its own owner's.
pub(crate) fn sealed_page(
    store: &ContentStore,
    ledger: LedgerId,
    peer: &AuthenticatedPeer,
    route: RouteEpoch,
    read: &NativeReadRequest,
    query: &NativeSealQuery,
    limits: &WireLimits,
    charge: &mut focal_memory::Allocation,
) -> Result<NativeReadPage, AccessError> {
    read.validate(limits)?;
    let role = native_reads::role(peer)?;
    if role != NativePeerRole::Runtime && query.request.principal != peer.principal() {
        return Err(AccessError::Unauthorized);
    }
    let session_limits = NativeSessionLimits::standard(ContentDomainId(ledger.tenant.0));
    let invocation = focal_core::native::NativeInvocation::Request(query.request);
    let mut reference = seal_reference(ledger, query);
    let mut through = SessionSeq(0);
    let mut found = None;
    for _ in 0..MAX_FOLD_DEPTH {
        let bytes = store
            .read_bytes(&reference, MAX_ARCHIVE_BYTES)
            .map_err(|_| AccessError::Unavailable)?;
        let seal = StructuralSeal::inspect(&bytes, session_limits.inspection)
            .map_err(|_| AccessError::Unavailable)?;
        if seal.header().ledger() != ledger {
            return Err(AccessError::Unauthorized);
        }
        match seal.header() {
            SealHeader::Seal { through: at, .. } => {
                through = *at;
                found = seal
                    .outcome(invocation)
                    .map_err(|_| AccessError::Unavailable)?;
                break;
            }
            SealHeader::Fold { .. } => {
                let Some(member) = seal
                    .header()
                    .member_of(query.request.principal, query.request.epoch)
                    .and_then(|ordinal| seal.header().member(ordinal))
                else {
                    break;
                };
                reference = ContentRef {
                    domain: ContentDomainId(ledger.tenant.0),
                    root: member.bundle,
                    length: member.bytes,
                    class: ContentClass::Evidence,
                };
            }
        }
    }
    let object = match found {
        Some(outcome) => NativeObject::Outcome(Box::new(crate::native_documents::outcome(outcome))),
        None => NativeObject::Missing(NativeObjectRef::Outcome(NativeInvocationRef::Request(
            query.request,
        ))),
    };
    Ok(NativeReadPage {
        token: ReadToken {
            ledger,
            sequence: through,
            route_epoch: route,
        },
        native_sequence: through,
        logical_time: 0,
        objects: native_reads::PageObjects::new(charge).only(object)?,
        next: None,
        visited: 1,
    })
}
/// The content the query names: the bundle as an object of the ledger's
/// tenant domain under the evidence class.
pub(crate) fn reference(ledger: LedgerId, query: &NativeArchiveQuery) -> ContentRef {
    ContentRef {
        domain: ContentDomainId(ledger.tenant.0),
        root: query.bundle,
        length: query.bytes,
        class: ContentClass::Evidence,
    }
}
fn hydration_error(error: NativeError) -> AccessError {
    match error {
        NativeError::Memory(_) | NativeError::Capacity(_) => AccessError::Capacity,
        NativeError::Contract(_)
        | NativeError::Evidence(_)
        | NativeError::RequestConflict
        | NativeError::Payload(_) => AccessError::Unavailable,
    }
}
/// One page holding the archived object, or `Missing` for a row the bundle
/// does not hold. The caller has authorized the peer for the ledger and the
/// bundle's domain (`CustodyStore::check_scope`; the embedded node's one
/// ledger).
#[allow(clippy::too_many_arguments)] // The read's inputs, each its own owner's.
pub(crate) fn page(
    store: &ContentStore,
    budget: &MemoryBudget,
    ledger: LedgerId,
    peer: &AuthenticatedPeer,
    route: RouteEpoch,
    read: &NativeReadRequest,
    query: &NativeArchiveQuery,
    limits: &WireLimits,
    charge: &mut focal_memory::Allocation,
) -> Result<NativeReadPage, AccessError> {
    read.validate(limits)?;
    let reference = reference(ledger, query);
    // Custody this node does not hold, or holds corrupt, is unavailable —
    // never an unknown object, and never the caller's fault.
    let bytes = store
        .read_bytes(&reference, MAX_ARCHIVE_BYTES)
        .map_err(|_| AccessError::Unavailable)?;
    page_of(
        &bytes,
        store,
        budget,
        Bundle {
            ledger,
            root: query.bundle,
            bytes: query.bytes,
        },
        peer,
        route,
        read,
        query.object,
        charge,
    )
}
/// The family a bundle or a cold frame holds: its ledger, and its name and length.
#[derive(Debug, Clone, Copy)]
struct Bundle {
    ledger: LedgerId,
    root: ContentHash,
    bytes: u64,
}
/// One page of an object of a retired claim's family (doc 31 M2, A2), from where this replica's
/// continuation of the claim says it went: a frame of its own cold stream for a family retired
/// there, else the bundle its custody holds. A claim that never retired has no such object.
#[allow(clippy::too_many_arguments)] // The read's inputs, each its own owner's.
pub(crate) fn cold_page(
    session: &mut focal_ledger::Session,
    budget: &MemoryBudget,
    peer: &AuthenticatedPeer,
    route: RouteEpoch,
    read: &NativeReadRequest,
    query: &NativeColdQuery,
    limits: &WireLimits,
    charge: &mut focal_memory::Allocation,
) -> Result<NativeReadPage, AccessError> {
    read.validate(limits)?;
    let ledger = session.ledger();
    let retired = session
        .native_core()
        .map_err(crate::host::access)?
        .native_retired(query.claim)
        .copied();
    let Some(retired) = retired else {
        return Err(AccessError::InvalidRequest);
    };
    let custody = session
        .native_content_reader()
        .map_err(crate::host::access)?;
    let frame = session
        .native_cold_frame(retired.retired_at, retired.bundle)
        .map_err(cold_frame_error)?;
    let mut bundle = Vec::new();
    // A custody bundle's bytes are charged before they are read, and held while the page is
    // made from them, as a cold frame's are.
    let mut _charge = None;
    let bytes = match &frame {
        Some(frame) => frame.bytes(),
        None => {
            _charge = Some(
                budget
                    .reserve(
                        BudgetKind::Query,
                        BudgetLane::Ordinary,
                        usize::try_from(retired.bytes).map_err(|_| AccessError::Capacity)?,
                    )
                    .map_err(|_| AccessError::Capacity)?
                    .commit(),
            );
            custody
                .read_verified(
                    &ContentRef {
                        domain: ContentDomainId(ledger.tenant.0),
                        root: retired.bundle,
                        length: retired.bytes,
                        class: ContentClass::Evidence,
                    },
                    &mut bundle,
                )
                .map_err(|_| AccessError::Unavailable)?;
            &bundle
        }
    };
    page_of(
        bytes,
        &custody,
        budget,
        Bundle {
            ledger,
            root: retired.bundle,
            bytes: retired.bytes,
        },
        peer,
        route,
        read,
        query.object,
        charge,
    )
}
/// A cold frame this replica cannot read back is unavailable here, a typed refusal as a custody
/// bundle it holds corrupt is: its stream's bytes refused (changed, cut, or a sealed record that
/// does not open, 29 §12.6) or unreadable. A read refused for room is capacity. A read answers no
/// outcome, so none of it is an unknown one, which a client would retry as if a write's.
fn cold_frame_error(error: focal_ledger::LedgerError) -> AccessError {
    use focal_evidence::{ContentError, NativeEvidenceError};
    match error {
        focal_ledger::LedgerError::Native(focal_ledger::NativeSessionError::Native(
            NativeError::Evidence(NativeEvidenceError::Content(content)),
        )) => match content {
            ContentError::Capacity => AccessError::Capacity,
            _ => AccessError::Unavailable,
        },
        focal_ledger::LedgerError::Native(focal_ledger::NativeSessionError::Native(
            NativeError::Evidence(NativeEvidenceError::Memory(_)),
        )) => AccessError::Capacity,
        other => crate::host::access(other),
    }
}
/// One page holding `object` of the family `bytes` hold (a bundle or a cold frame), hydrated
/// against `custody`, or `Missing` for a row it does not hold.
#[allow(clippy::too_many_arguments)] // The read's inputs, each its own owner's.
fn page_of<R: focal_evidence::NativeCustodyReader>(
    bytes: &[u8],
    custody: &R,
    budget: &MemoryBudget,
    bundle: Bundle,
    peer: &AuthenticatedPeer,
    route: RouteEpoch,
    read: &NativeReadRequest,
    object: NativeObjectRef,
    charge: &mut focal_memory::Allocation,
) -> Result<NativeReadPage, AccessError> {
    let ledger = bundle.ledger;
    let role = native_reads::role(peer)?;
    let session_limits = NativeSessionLimits::standard(ContentDomainId(ledger.tenant.0));
    let archive = StructuralArchive::inspect(bytes, session_limits.inspection)
        .map_err(|_| AccessError::Unavailable)?;
    if archive.header().ledger != ledger {
        return Err(AccessError::Unauthorized);
    }
    let hydrated = archive
        .hydrate(
            RangeId(1),
            session_limits.recovery,
            budget.clone(),
            custody,
            &BuiltinNativeSchemas,
        )
        .map_err(hydration_error)?;
    let header = archive.header();
    let profile = match header.profile {
        NativeContentProfile::ProjectionOnly => NativeProfile::ProjectionOnly,
        NativeContentProfile::AuthoredV1 => NativeProfile::AuthoredV1,
    };
    let reader = Reader {
        core: hydrated.core(),
        ledger,
        profile,
        principal: peer.principal(),
        role,
        route,
    };
    // A bundle's core holds its family's rows, never another archive: an object read from it that
    // is archived is a corrupt bundle, not one to nest.
    let archived = |object: NativeObject| -> Result<NativeObject, AccessError> {
        Ok(match object {
            NativeObject::Missing(reference) => NativeObject::Missing(reference),
            object => NativeObject::Archived(Box::new(NativeArchivedObject {
                bundle: bundle.root,
                bytes: bundle.bytes,
                root: header.root,
                through: header.through,
                object: NativeBundledObject::try_from(object)
                    .map_err(|_| AccessError::Unavailable)?,
            })),
        })
    };
    let first = native_reads::object(&reader, object)?;
    // Each object charged to the read as the page takes it (`native_reads::PageObjects`); the
    // pages built inside this one take the read's charge as they build, given back before
    // their objects are taken here, archived.
    let mut objects = native_reads::PageObjects::new(charge);
    let max_items = read.max_items as usize;
    let mut visited = 1u32;
    // A validation is read with what was recorded under it: its evaluations
    // in key order and each one's accepted results, the pages a live
    // `validation.get` follows, all from the bundle.
    let expansion = match &first {
        NativeObject::Definition(definition) => {
            Some((definition.claim, ValidationId(definition.binding.object.0)))
        }
        _ => None,
    };
    if !objects.take(archived(first)?)? {
        return Err(AccessError::Capacity);
    }
    if let Some((claim, validation)) = expansion {
        let before = objects.charge().bytes();
        let evaluations = native_reads::page(
            &reader,
            &NativeReadRequest {
                consistency: read.consistency.clone(),
                query: NativeReadQuery::Evaluations {
                    claim,
                    validation,
                    after: None,
                },
                max_items: read.max_items,
            },
            objects.charge(),
        )?;
        let _ = objects.charge().shrink_to(before);
        visited = visited.saturating_add(evaluations.visited);
        let keys: Vec<NativeEvaluationKey> = evaluations
            .objects
            .iter()
            .filter_map(|object| match object {
                NativeObject::Evaluation(evaluation) => Some(evaluation.key),
                _ => None,
            })
            .collect();
        // The page holds what fits in its items and the read's budget: an archived page has no
        // continuation, so the rest is left for a narrower read, as the items' bound left it.
        let mut room = true;
        for object in evaluations.objects {
            if objects.len() >= max_items || !objects.take(archived(object)?)? {
                room = false;
                break;
            }
        }
        for key in keys {
            if !room || objects.len() >= max_items {
                break;
            }
            let before = objects.charge().bytes();
            let results = native_reads::page(
                &reader,
                &NativeReadRequest {
                    consistency: read.consistency.clone(),
                    query: NativeReadQuery::Results {
                        evaluation: key,
                        after: None,
                    },
                    max_items: read.max_items,
                },
                objects.charge(),
            )?;
            let _ = objects.charge().shrink_to(before);
            visited = visited.saturating_add(results.visited);
            for object in results.objects {
                if objects.len() >= max_items || !objects.take(archived(object)?)? {
                    room = false;
                    break;
                }
            }
        }
    }
    Ok(NativeReadPage {
        token: ReadToken {
            ledger,
            sequence: header.through,
            route_epoch: route,
        },
        native_sequence: header.through,
        // A bundle carries no clock: its objects are at or below the prefix
        // it claims, and say their own times.
        logical_time: 0,
        objects: objects.into_vec(),
        next: None,
        visited,
    })
}
