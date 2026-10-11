//! Borrowed node driver: capability facts come from installed Session owners,
//! travel over authenticated peer routes, and return to that same incarnation.
use crate::fleet::FleetManager;
use focal_ledger::LedgerError;
use focal_memory::{BudgetKind, BudgetLane, MemoryBudget};
use focal_model::{RequestEpoch, RequestId};
use focal_rt::Concurrent;
use focal_wire::{MANAGED_PROTOCOL_VERSION, Operation, PeerConnectionPool, RequestEnvelope};
use hyper_rt::combine::{Either, join2, race2};
use std::future::Future;
use std::pin::pin;
use std::task::Poll;
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
    join2(
        support(manager, pool, budget),
        bring(manager, pool, content),
    )
    .await;
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
    // As many as content has streams to one peer: the loop below fills no
    // more than that.
    let Ok(mut serving) = Concurrent::new(pool.bulk_lane()) else {
        return;
    };
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
                let pushed = serving.push(async move {
                    serve(&host, pool, content, ledger).await;
                    ledger
                });
                if pushed.is_err() {
                    // Within the set's bound by the loop's own count; a
                    // refusal leaves the ledger to be looked at again.
                    served.remove(&ledger);
                    break;
                }
            }
        }
        // A replica that was served is looked at again once its serving
        // ends; when every replica was looked at, the next look is a
        // period away unless one ends first. Nothing serving and not every
        // replica looked at cannot follow from the fill above, and waits
        // the period too rather than turning the loop without a wait.
        if serving.is_empty() || all {
            match race2(
                serving.join_next(),
                focal_rt::time::sleep(Duration::from_millis(100)),
            )
            .await
            {
                Either::First(Some(ledger)) => {
                    served.remove(&ledger);
                }
                // Nothing was serving: the period is waited out.
                Either::First(None) => {
                    if focal_rt::time::sleep(Duration::from_millis(100))
                        .await
                        .is_err()
                    {
                        return;
                    }
                }
                Either::Second(slept) => {
                    if slept.is_err() {
                        return;
                    }
                }
            }
        } else if let Some(ledger) = serving.join_next().await {
            served.remove(&ledger);
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
    let Ok(mut running) = Concurrent::new(most) else {
        return;
    };
    let mut busy = std::collections::BTreeSet::new();
    // When each ledger may be asked again; what is past is pruned each pass,
    // so it holds no more than the ledgers hosted within one period.
    let mut due: std::collections::BTreeMap<focal_model::LedgerId, focal_rt::time::Instant> =
        std::collections::BTreeMap::new();
    let mut changes = manager.changes();
    let mut after = None;
    let mut serial = 0u128;
    loop {
        let now = focal_rt::time::Instant::now();
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
            let pushed = running.push(async move {
                let _charge = charge.commit();
                discover(&host, pool, ledger, first).await;
                (ledger, host.tick_period())
            });
            if pushed.is_err() {
                // Within the set's bound by the loop's own count; the
                // ledger is asked on a later pass.
                busy.remove(&ledger);
                all = true;
                break;
            }
        }
        let soonest = due.values().min().copied();
        let waiting = all && running.is_empty();
        let mut changed = pin!(changes.changed());
        let mut due_next = pin!(async {
            match soonest {
                Some(at) => focal_rt::time::sleep_until(at).await,
                None => std::future::pending().await,
            }
        });
        // A discovery that ended, the fleet's change (once every ledger was
        // looked at), or the next ledger's turn (once nothing runs).
        let event = std::future::poll_fn(|cx| {
            if !running.is_empty()
                && let Poll::Ready(Some(ended)) = running.poll_next(cx)
            {
                return Poll::Ready(Woke::Ended(ended));
            }
            if all && let Poll::Ready(changed) = changed.as_mut().poll(cx) {
                return Poll::Ready(Woke::Changed(changed.is_ok()));
            }
            if waiting && let Poll::Ready(slept) = due_next.as_mut().poll(cx) {
                return Poll::Ready(Woke::Due(slept.is_ok()));
            }
            Poll::Pending
        })
        .await;
        match event {
            Woke::Ended((ledger, tick)) => {
                busy.remove(&ledger);
                if let Some(at) = focal_rt::time::Instant::now().checked_add(tick) {
                    due.insert(ledger, at);
                }
            }
            // The fleet's manager is gone, or no wait could be kept: the
            // driver ends, and the service with it.
            Woke::Changed(false) | Woke::Due(false) => return,
            Woke::Changed(true) | Woke::Due(true) => {}
        }
    }
}
/// What woke the support loop.
enum Woke<T> {
    Ended(T),
    Changed(bool),
    Due(bool),
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
    // One exchange for each peer the fact names, at most four.
    let Ok(mut exchanges) = Concurrent::new(local.targets().count()) else {
        return;
    };
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
        let pushed = exchanges.push(async move {
            let Ok(remote) = pool.send_managed_support(target, &request).await else {
                return;
            };
            let _ = host.record_managed_support(target, remote).await;
        });
        if pushed.is_err() {
            break;
        }
    }
    while exchanges.join_next().await.is_some() {}
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
    // A transfer's width to a peer is never more than its bulk lane.
    let lane = pool.bulk_lane();
    let Ok(mut pulling) = Concurrent::new(lane) else {
        return false;
    };
    let mut brought = false;
    loop {
        while pulling.len() < width().min(lane) {
            match wanted.next() {
                Some(next) => {
                    if pulling.push(pull(next)).is_err() {
                        break;
                    }
                }
                None => break,
            }
        }
        match pulling.join_next().await {
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
