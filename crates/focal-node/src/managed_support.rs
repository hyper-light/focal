//! Borrowed node driver: capability facts come from installed Session owners,
//! travel over authenticated peer routes, and return to that same incarnation.
use crate::fleet::FleetManager;
use focal_memory::{BudgetKind, BudgetLane, MemoryBudget};
use focal_model::{RequestEpoch, RequestId};
use focal_wire::{MANAGED_PROTOCOL_VERSION, Operation, PeerConnectionPool, RequestEnvelope};
use futures_util::{StreamExt, stream::FuturesUnordered};
use std::time::Duration;

pub(crate) async fn drive(
    manager: &FleetManager,
    pool: &PeerConnectionPool,
    budget: &MemoryBudget,
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
