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
use focal_core::native::{NativeContentProfile, NativeError};
use focal_evidence::{BuiltinNativeSchemas, ContentStore};
use focal_ledger::NativeSessionLimits;
use focal_memory::{MemoryBudget, RangeId};
use focal_model::*;
use focal_wire::*;

/// The most bytes a bundle read may take: the record encoding limits a
/// session hosts under (26 §4), so any bundle a core wrote fits — the
/// operator's bound (`operator_admin`) and the collector's.
const MAX_ARCHIVE_BYTES: usize = 6 << 20;

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
        NativeError::Contract(_) | NativeError::Evidence(_) | NativeError::RequestConflict => {
            AccessError::Unavailable
        }
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
) -> Result<NativeReadPage, AccessError> {
    read.validate(limits)?;
    let role = native_reads::role(peer)?;
    let reference = reference(ledger, query);
    // Custody this node does not hold, or holds corrupt, is unavailable —
    // never an unknown object, and never the caller's fault.
    let bytes = store
        .read_bytes(&reference, MAX_ARCHIVE_BYTES)
        .map_err(|_| AccessError::Unavailable)?;
    let session_limits = NativeSessionLimits::standard(reference.domain);
    let archive = StructuralArchive::inspect(&bytes, session_limits.inspection)
        .map_err(|_| AccessError::Unavailable)?;
    if archive.header().ledger != ledger {
        return Err(AccessError::Unauthorized);
    }
    let hydrated = archive
        .hydrate(
            RangeId(1),
            session_limits.recovery,
            budget.clone(),
            store,
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
    let archived = |object: NativeObject| match object {
        NativeObject::Missing(reference) => NativeObject::Missing(reference),
        object => NativeObject::Archived(Box::new(NativeArchivedObject {
            bundle: query.bundle,
            bytes: query.bytes,
            root: header.root,
            through: header.through,
            object,
        })),
    };
    let first = native_reads::object(&reader, query.object)?;
    let mut objects = Vec::new();
    objects.try_reserve(1).map_err(|_| AccessError::Capacity)?;
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
    objects.push(archived(first));
    if let Some((claim, validation)) = expansion {
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
        )?;
        visited = visited.saturating_add(evaluations.visited);
        let keys: Vec<NativeEvaluationKey> = evaluations
            .objects
            .iter()
            .filter_map(|object| match object {
                NativeObject::Evaluation(evaluation) => Some(evaluation.key),
                _ => None,
            })
            .collect();
        objects
            .try_reserve(evaluations.objects.len())
            .map_err(|_| AccessError::Capacity)?;
        objects.extend(evaluations.objects.into_iter().map(archived));
        for key in keys {
            if objects.len() >= read.max_items as usize {
                break;
            }
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
            )?;
            visited = visited.saturating_add(results.visited);
            objects
                .try_reserve(results.objects.len())
                .map_err(|_| AccessError::Capacity)?;
            objects.extend(results.objects.into_iter().map(archived));
        }
        objects.truncate(read.max_items as usize);
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
        objects,
        next: None,
        visited,
    })
}
