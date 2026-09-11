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
    let mut after = None;
    let mut serial = 0u128;
    loop {
        let Some((ledger, host)) = manager.next_host(after) else {
            after = None;
            tokio::time::sleep(Duration::from_millis(100)).await;
            continue;
        };
        after = Some(ledger);
        if let Some(pending) = host.progress().import_pending {
            // The replica retained an import delivery until its inline legacy
            // payloads are sealed locally; seal them with the recorded chunking.
            let _ = service_import(&host, content, pending).await;
        }
        if host.progress().seed_pending.is_some() {
            // The replica retained a seeded checkpoint until every chunk it
            // names is local; pull the missing ones from a peer of the
            // ledger's placement (25 §5).
            let _ = service_seed(&host, pool, content, ledger).await;
        }
        if host.progress().custody_pending.is_some() {
            // The replica retained a delivery whose records or checkpoint
            // name objects this node does not hold; pull them from a peer
            // of the ledger's placement (24 §20).
            let _ = service_custody(&host, pool, content, ledger).await;
        }
        let Ok(charge) = budget.reserve(BudgetKind::Control, BudgetLane::Completion, 512 * 1024)
        else {
            tokio::time::sleep(Duration::from_millis(100)).await;
            continue;
        };
        let _charge = charge.commit();
        let Ok(Ok(local)) =
            tokio::time::timeout(Duration::from_millis(250), host.managed_support()).await
        else {
            continue;
        };
        let fact = local.fact();
        // A sorted bounded configuration has no untrusted route discovery. A
        // changed configuration/incarnation is rejected by the receiving owner.
        let mut exchanges = FuturesUnordered::new();
        for target in local.targets() {
            let Some(next) = serial.checked_add(1) else {
                return;
            };
            serial = next;
            let request = RequestEnvelope {
                protocol: MANAGED_PROTOCOL_VERSION,
                ledger,
                route_epoch: local.route_epoch(),
                request_epoch: RequestEpoch(1),
                request_id: RequestId::from_u128(serial),
                operation: Operation::ManagedSupport { group: fact.group },
            };
            let host = &host;
            exchanges.push(async move {
                let Ok(Ok(remote)) = tokio::time::timeout(
                    Duration::from_millis(250),
                    pool.send_managed_support(target, &request),
                )
                .await
                else {
                    return;
                };
                let _ = tokio::time::timeout(
                    Duration::from_millis(250),
                    host.record_managed_support(target, remote),
                )
                .await;
            });
        }
        while exchanges.next().await.is_some() {}
        // Yield to replication and existing service work even on single-voter
        // laptops. No busy polling, unbounded queue, or new owner is introduced.
        tokio::task::yield_now().await;
    }
}

/// Pull every chunk a replica's pending seeded checkpoint lacks from the
/// peers of the ledger's custody policy, one at a time, and hand each to the
/// replica; it installs the checkpoint once all are local.
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
    for hash in host.pending_seed_chunks().await? {
        for peer in policy.peers.iter().filter(|peer| **peer != node) {
            let pulled = tokio::time::timeout(
                Duration::from_millis(2_000),
                crate::evidence_service::pull_seed(pool, *peer, scope, hash),
            )
            .await;
            let Ok(Ok(bytes)) = pulled else {
                continue;
            };
            host.install_seed_chunk(hash, bytes).await?;
            break;
        }
    }
    Ok(())
}
/// Pull every content object a replica's retained delivery lacks from the
/// peers of the ledger's custody policy, one at a time, and tell the
/// replica; it retries the delivery once they are local.
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
    let mut pulled = false;
    for reference in host.pending_custody_objects().await? {
        for peer in policy.peers.iter().filter(|peer| **peer != node) {
            let result = tokio::time::timeout(
                Duration::from_millis(5_000),
                crate::evidence_service::pull_object(content, pool, node, *peer, scope, &reference),
            )
            .await;
            if matches!(result, Ok(Ok(()))) {
                pulled = true;
                break;
            }
        }
    }
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
