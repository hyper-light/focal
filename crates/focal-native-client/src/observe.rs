//! Client-composed observations of the native engine: one claim's lineage as
//! a bounded page of committed claims, and a bounded wait for a predicate on
//! one claim. Both are reads only; neither mints an identity, registers a
//! monitor or touches a journal.
use crate::CompileError;
use crate::driver::{CLAIM_EXPAND, DriveError, Lists, READ_ITEMS, Reads};
use focal_client::ClientError;
use focal_client::claim_wait::{ClaimObservation, ClaimWaitCondition};
use focal_client::input::{BuildContext, InputError, parse_id};
use focal_client::operations::{NativeObjectDocument, NativeWaitDocument, NativeWaitResult};
use focal_model::*;
use focal_wire::*;
use std::time::{Duration, Instant};

/// How many `caused_by` ancestors a lineage page follows.
pub const LINEAGE_DEPTH: usize = 16;
/// How many related claims (corrections, refinements, children) one lineage
/// page reads with their content.
const LINEAGE_RELATED: usize = 64;
/// The wait observer's probe bound, one second apart under one deadline.
pub const WAIT_PROBES: u32 = 31;
/// The host's bounded, cancellable sleep between wait probes.
pub type Pause<'a> = dyn FnMut(Duration) -> Result<(), DriveError> + 'a;

const CONTENT_ONLY: NativeClaimExpand = NativeClaimExpand {
    content: true,
    scopes: false,
    responses: false,
    evaluations: false,
    history: false,
};
const STATE_ONLY: NativeClaimExpand = NativeClaimExpand {
    content: false,
    scopes: false,
    responses: false,
    evaluations: false,
    history: false,
};

fn claim_of(page: &NativeReadPage) -> Option<&NativeClaim> {
    page.objects.iter().find_map(|object| match object {
        NativeObject::Claim(claim) => Some(&**claim),
        _ => None,
    })
}
fn take_claim(page: NativeReadPage) -> Option<NativeObject> {
    page.objects
        .into_iter()
        .find(|object| matches!(object, NativeObject::Claim(_)))
}
fn push(objects: &mut Vec<NativeObject>, object: NativeObject) -> Result<(), DriveError> {
    objects.try_reserve(1).map_err(|_| InputError::Capacity)?;
    objects.push(object);
    Ok(())
}
fn add(a: u32, b: u32) -> Result<u32, DriveError> {
    a.checked_add(b).ok_or_else(|| InputError::Capacity.into())
}

/// One claim's lineage: the claim itself (full expansion), its `caused_by`
/// ancestors up to `LINEAGE_DEPTH`, then the committed claims that
/// invalidate it, refine it or are caused by it (each with its content, up to
/// `LINEAGE_RELATED`). The page carries the first read's token; every later
/// read is at least at that token, so nothing shown predates the claim shown.
pub fn lineage(
    document: &NativeObjectDocument,
    build: &BuildContext,
    reads: &mut Reads<'_>,
    lists: &mut Lists<'_>,
) -> Result<NativeReadPage, DriveError> {
    let id = ClaimId(parse_id(&document.id)?);
    let first = reads(NativeReadRequest {
        consistency: ReadConsistency::Linearizable,
        query: NativeReadQuery::Claim {
            id,
            expand: CLAIM_EXPAND,
        },
        max_items: READ_ITEMS,
    })?;
    let token = first.token;
    let native_sequence = first.native_sequence;
    let logical_time = first.logical_time;
    let mut visited = first.visited;
    let mut cause = claim_of(&first)
        .ok_or(CompileError::Missing("claim"))?
        .cause
        .clone();
    let mut objects = Vec::new();
    push(
        &mut objects,
        take_claim(first).ok_or(CompileError::Missing("claim"))?,
    )?;
    let mut at_least = |query: NativeReadQuery| {
        reads(NativeReadRequest {
            consistency: ReadConsistency::AtLeast(token),
            query,
            max_items: READ_ITEMS,
        })
    };
    // Ancestors, nearest first.
    for _ in 0..LINEAGE_DEPTH {
        let Cause::Claim(parent) = cause else {
            break;
        };
        let page = at_least(NativeReadQuery::Claim {
            id: parent,
            expand: CONTENT_ONLY,
        })?;
        visited = add(visited, page.visited)?;
        let Some(ancestor) = claim_of(&page) else {
            break;
        };
        cause = ancestor.cause.clone();
        push(
            &mut objects,
            take_claim(page).ok_or(CompileError::Missing("claim"))?,
        )?;
    }
    // Followers: corrections, refinements and children, by relation index.
    let mut related: Vec<ClaimId> = Vec::new();
    for kind in [
        RelationKind::Invalidates,
        RelationKind::Refines,
        RelationKind::CausedBy,
    ] {
        let page = lists(NativeListRequest {
            filter: NativeListFilter::Claims {
                issuer: None,
                subject: None,
                status: None,
                action: None,
                scope: None,
                relation: Some(Relation {
                    kind,
                    target: RelationTarget::Object(ObjectRef::claim(build.ledger, id)),
                }),
                created_after: None,
            },
            cursor: None,
            max_items: READ_ITEMS,
            max_visits: 1024,
        })?;
        visited = add(visited, page.visited)?;
        for object in &page.objects {
            let NativeObject::Claim(claim) = object else {
                continue;
            };
            let follower = ClaimId(claim.binding.object.0);
            if follower == id || related.contains(&follower) || related.len() >= LINEAGE_RELATED {
                continue;
            }
            related.try_reserve(1).map_err(|_| InputError::Capacity)?;
            related.push(follower);
        }
    }
    for follower in related {
        let page = at_least(NativeReadQuery::Claim {
            id: follower,
            expand: CONTENT_ONLY,
        })?;
        visited = add(visited, page.visited)?;
        if let Some(object) = take_claim(page) {
            push(&mut objects, object)?;
        }
    }
    Ok(NativeReadPage {
        token,
        native_sequence,
        logical_time,
        objects,
        next: None,
        visited,
    })
}

/// Observe one claim until the predicate holds: at most `WAIT_PROBES` fresh
/// linearizable reads one second apart under one deadline of at most 30
/// seconds, keeping only the latest observation. An observation that moves
/// backwards (an older prefix, a lower revision, a terminal status that
/// changes) is an invalid response, never a met predicate.
pub fn wait(
    document: &NativeWaitDocument,
    reads: &mut Reads<'_>,
    pause: &mut Pause<'_>,
) -> Result<NativeWaitResult, DriveError> {
    if !(1..=30_000).contains(&document.timeout_ms) {
        return Err(InputError::Invalid("timeout_ms must be in 1..=30000").into());
    }
    let id = ClaimId(parse_id(&document.claim)?);
    let deadline = Instant::now()
        .checked_add(Duration::from_millis(u64::from(document.timeout_ms)))
        .ok_or(InputError::Invalid("wait deadline"))?;
    let mut latest: Option<NativeWaitResult> = None;
    for probe in 0..WAIT_PROBES {
        if Instant::now() >= deadline {
            break;
        }
        let page = reads(NativeReadRequest {
            consistency: ReadConsistency::Linearizable,
            query: NativeReadQuery::Claim {
                id,
                expand: STATE_ONLY,
            },
            max_items: 1,
        })?;
        let claim = claim_of(&page).ok_or(CompileError::Missing("claim"))?;
        let observation = ClaimObservation {
            token: page.token,
            id,
            status: claim.status,
            revision: claim.binding.revision,
            local_complete: claim.local_complete,
            released: claim.released,
        };
        if latest.is_some_and(|previous| {
            let previous = previous.observation;
            observation.token.sequence < previous.token.sequence
                || observation.token.route_epoch < previous.token.route_epoch
                || observation.revision < previous.revision
                || (previous.status.is_terminal() && observation.status != previous.status)
                || (previous.released && !observation.released)
                || (observation.token.sequence == previous.token.sequence
                    && (observation.status != previous.status
                        || observation.revision != previous.revision
                        || observation.local_complete != previous.local_complete
                        || observation.released != previous.released))
        }) {
            return Err(ClientError::InvalidResponse.into());
        }
        let met = document.until.met(claim.status, claim.released);
        let condition = if met {
            ClaimWaitCondition::Met
        } else if document.until.unmet_when_terminal() && claim.status.is_terminal() {
            ClaimWaitCondition::Unmet
        } else {
            ClaimWaitCondition::Pending
        };
        let result = NativeWaitResult {
            condition,
            until: document.until,
            observation,
            probes: add(probe, 1)?,
        };
        if condition != ClaimWaitCondition::Pending {
            return Ok(result);
        }
        latest = Some(result);
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            break;
        }
        pause(Duration::from_secs(1).min(remaining))?;
    }
    latest.ok_or_else(|| ClientError::Transport.into())
}
