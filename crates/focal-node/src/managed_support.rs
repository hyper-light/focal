//! Borrowed node driver: capability facts come from installed Session owners,
//! travel over authenticated peer routes, and return to that same incarnation.
use crate::fleet::FleetManager;
use focal_ledger::LedgerError;
use focal_memory::{BudgetKind, BudgetLane, MemoryBudget};
use focal_model::{RequestEpoch, RequestId};
use focal_wire::{MANAGED_PROTOCOL_VERSION, Operation, PeerConnectionPool, RequestEnvelope};
use futures_util::{StreamExt, stream::FuturesUnordered};
use std::time::Duration;

pub(crate) async fn drive(
    manager: &FleetManager,
    pool: &PeerConnectionPool,
    budget: &MemoryBudget,
    content: &crate::content_host::ContentHost,
) {
    // What a replica waits for to be brought to it takes as long as the
    // path takes to carry it, and the exchange of facts is asked at its
    // own pace: neither waits for the other.
    tokio::join!(
        support(manager, pool, budget),
        bring(manager, pool, content)
    );
}
/// Bring every replica what it retained a delivery for: as many replicas
/// at once as content has streams to one peer, each of them once.
async fn bring(
    manager: &FleetManager,
    pool: &PeerConnectionPool,
    content: &crate::content_host::ContentHost,
) {
    let mut after = None;
    let mut served = std::collections::BTreeSet::new();
    let mut serving = FuturesUnordered::new();
    loop {
        // Whether every replica was looked at since the last wait.
        let mut all = false;
        while serving.len() < pool.bulk_lane() {
            let Some((ledger, host)) = manager.next_host(after) else {
                after = None;
                all = true;
                break;
            };
            after = Some(ledger);
            let progress = host.progress();
            if (progress.import_pending.is_some()
                || progress.seed_pending.is_some()
                || progress.custody_pending.is_some())
                && served.insert(ledger)
            {
                serving.push(async move {
                    serve(&host, pool, content, ledger).await;
                    ledger
                });
            }
        }
        tokio::select! {
            Some(ledger) = serving.next(), if !serving.is_empty() => {
                served.remove(&ledger);
            }
            () = tokio::time::sleep(Duration::from_millis(100)), if all => {}
            else => {}
        }
    }
}
async fn serve(
    host: &crate::fleet::ReplicaHost,
    pool: &PeerConnectionPool,
    content: &crate::content_host::ContentHost,
    ledger: focal_model::LedgerId,
) {
    if let Some(pending) = host.progress().import_pending {
        // The replica retained an import delivery until its inline legacy
        // payloads are sealed locally; seal them with the recorded chunking.
        let _ = service_import(host, content, pending).await;
    }
    if host.progress().seed_pending.is_some() {
        // The replica retained a seeded checkpoint until every chunk it
        // names is local; pull the missing ones from a peer of the
        // ledger's placement (25 §5).
        let _ = service_seed(host, pool, content, ledger).await;
    }
    if host.progress().custody_pending.is_some() {
        // The replica retained a delivery whose records or checkpoint
        // name objects this node does not hold; pull them from a peer
        // of the ledger's placement (24 §20).
        let _ = service_custody(host, pool, content, ledger).await;
    }
}
/// Exchange support facts for every hosted ledger, each ledger's discovery
/// at its own pace and none waiting on another's (the audit's F48). A
/// discovery asks this node's replica for its fact, asks each of the peers
/// the replica names for theirs, and records what they answer. No part of
/// it has a clock of its own: the replica answers in its owner's periods
/// or refuses for its queue, and an exchange with a peer is given what its
/// path takes ([`PeerConnectionPool`], 27 §7). Each part was given 250 ms,
/// so a healthy path further than that never contributed a fact, and
/// native activation and repair across it were refused for want of one.
///
/// As many ledgers' discoveries run at once as one lane to a peer holds
/// (`per_peer_inflight`): a node's ledgers share the same few peers, and
/// more at once would only wait on the lane. Each holds a charge of the
/// budget, whose refusal is the bound on them. A ledger is asked again one
/// period of its owner after its last discovery ended; when no ledger is
/// due, the loop waits for the fleet to change or for the next to be due.
async fn support(manager: &FleetManager, pool: &PeerConnectionPool, budget: &MemoryBudget) {
    let most = pool.limits().per_peer_inflight.max(1);
    let mut running = FuturesUnordered::new();
    let mut busy = std::collections::BTreeSet::new();
    // When each ledger may be asked again; what is past is pruned each pass,
    // so it holds no more than the ledgers hosted within one period.
    let mut due: std::collections::BTreeMap<focal_model::LedgerId, tokio::time::Instant> =
        std::collections::BTreeMap::new();
    let mut changes = manager.changes();
    let mut after = None;
    let mut serial = 0u128;
    loop {
        let now = tokio::time::Instant::now();
        due.retain(|_, at| *at > now);
        // Whether every ledger was looked at since the last wait.
        let mut all = false;
        while running.len() < most {
            let Some((ledger, host)) = manager.next_host(after) else {
                after = None;
                all = true;
                break;
            };
            after = Some(ledger);
            if busy.contains(&ledger) || due.contains_key(&ledger) {
                continue;
            }
            let Ok(charge) =
                budget.reserve(BudgetKind::Control, BudgetLane::Completion, 512 * 1024)
            else {
                // The bound on discoveries: the rest wait for one to end.
                all = true;
                break;
            };
            let Some(next) = serial.checked_add(4) else {
                return;
            };
            let first = serial;
            serial = next;
            busy.insert(ledger);
            running.push(async move {
                let _charge = charge.commit();
                discover(&host, pool, ledger, first).await;
                (ledger, host.tick_period())
            });
        }
        let soonest = due.values().min().copied();
        tokio::select! {
            Some((ledger, tick)) = running.next(), if !running.is_empty() => {
                busy.remove(&ledger);
                if let Some(at) = tokio::time::Instant::now().checked_add(tick) {
                    due.insert(ledger, at);
                }
            }
            changed = changes.changed(), if all => {
                if changed.is_err() {
                    return;
                }
            }
            () = async {
                match soonest {
                    Some(at) => tokio::time::sleep_until(at).await,
                    None => std::future::pending().await,
                }
            }, if all && running.is_empty() => {}
        }
    }
}
/// One discovery for `ledger`: the replica's fact, every peer it names
/// asked at once, each answer recorded as it comes.
async fn discover(
    host: &crate::fleet::ReplicaHost,
    pool: &PeerConnectionPool,
    ledger: focal_model::LedgerId,
    first: u128,
) {
    let Ok(local) = host.managed_support().await else {
        return;
    };
    let fact = local.fact();
    // A sorted bounded configuration has no untrusted route discovery. A
    // changed configuration/incarnation is rejected by the receiving owner.
    let mut exchanges = FuturesUnordered::new();
    for (offset, target) in local.targets().enumerate() {
        let Some(serial) = u128::try_from(offset)
            .ok()
            .and_then(|offset| first.checked_add(offset))
        else {
            break;
        };
        let request = RequestEnvelope {
            protocol: MANAGED_PROTOCOL_VERSION,
            ledger,
            route_epoch: local.route_epoch(),
            request_epoch: RequestEpoch(1),
            request_id: RequestId::from_u128(serial),
            operation: Operation::ManagedSupport { group: fact.group },
        };
        exchanges.push(async move {
            let Ok(remote) = pool.send_managed_support(target, &request).await else {
                return;
            };
            let _ = host.record_managed_support(target, remote).await;
        });
    }
    while exchanges.next().await.is_some() {}
}

/// As many of `wanted` at once as a transfer to the peers has streams
/// now, each by `pull`. Whether any was brought.
async fn at_once<T, F>(
    pool: &PeerConnectionPool,
    peers: &std::collections::BTreeSet<u64>,
    node: u64,
    wanted: Vec<T>,
    pull: impl Fn(T) -> F,
) -> bool
where
    F: Future<Output = bool>,
{
    let width = || {
        peers
            .iter()
            .filter(|peer| **peer != node)
            .map(|peer| pool.bulk_width(*peer))
            .max()
            .unwrap_or(1)
    };
    let mut wanted = wanted.into_iter();
    let mut pulling = FuturesUnordered::new();
    let mut brought = false;
    loop {
        while pulling.len() < width() {
            match wanted.next() {
                Some(next) => pulling.push(pull(next)),
                None => break,
            }
        }
        match pulling.next().await {
            Some(pulled) => brought |= pulled,
            None => return brought,
        }
    }
}
/// Pull every chunk a replica's pending seeded checkpoint lacks from the
/// peers of the ledger's custody policy, as many at once as the path to
/// them holds, and hand each to the replica; it installs the checkpoint
/// once all are local. An exchange is given as long as its path takes to
/// carry it (`QuicRemote::request_within`).
async fn service_seed(
    host: &crate::fleet::ReplicaHost,
    pool: &PeerConnectionPool,
    content: &crate::content_host::ContentHost,
    ledger: focal_model::LedgerId,
) -> Result<(), LedgerError> {
    let Some(policy) = content
        .policy(ledger)
        .await
        .map_err(|_| LedgerError::Capacity)?
    else {
        return Ok(());
    };
    let scope = policy.scope();
    let node = host.progress().node;
    let wanted = host.pending_seed_chunks().await?;
    let peers = &policy.peers;
    at_once(pool, peers, node, wanted, async |hash| {
        for peer in peers.iter().filter(|peer| **peer != node) {
            if let Ok(bytes) = crate::evidence_service::pull_seed(pool, *peer, scope, hash).await {
                return host.install_seed_chunk(hash, bytes).await.is_ok();
            }
        }
        false
    })
    .await;
    Ok(())
}
/// Pull every content object a replica's retained delivery lacks from the
/// peers of the ledger's custody policy, as many at once as the path to
/// them holds, and tell the replica; it retries the delivery once they
/// are local.
async fn service_custody(
    host: &crate::fleet::ReplicaHost,
    pool: &PeerConnectionPool,
    content: &crate::content_host::ContentHost,
    ledger: focal_model::LedgerId,
) -> Result<(), LedgerError> {
    let Some(policy) = content
        .policy(ledger)
        .await
        .map_err(|_| LedgerError::Capacity)?
    else {
        return Ok(());
    };
    let scope = policy.scope();
    let node = host.progress().node;
    let wanted = host.pending_custody_objects().await?;
    let peers = &policy.peers;
    let pulled = at_once(pool, peers, node, wanted, async |reference| {
        for peer in peers.iter().filter(|peer| **peer != node) {
            if crate::evidence_service::pull_object(content, pool, node, *peer, scope, &reference)
                .await
                .is_ok()
            {
                return true;
            }
        }
        false
    })
    .await;
    if pulled {
        host.custody_pulled().await?;
    }
    Ok(())
}
/// Seal every inline legacy payload of a replica's pending import through the
/// exclusive content writer; the replica applies the import at its next poll.
async fn service_import(
    host: &crate::fleet::ReplicaHost,
    content: &crate::content_host::ContentHost,
    pending: focal_ledger::PendingImport,
) -> Result<(), LedgerError> {
    let Some(import) = host.import_payloads().await? else {
        return Ok(());
    };
    let chunk_bytes = usize::try_from(pending.chunk_bytes).map_err(|_| LedgerError::Capacity)?;
    for bytes in import.payloads {
        content
            .seal_import_inline(import.domain, bytes, chunk_bytes)
            .await
            .map_err(|_| LedgerError::Capacity)?;
    }
    Ok(())
}
