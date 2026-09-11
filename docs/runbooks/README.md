# Runbooks

One runbook per failure an operator meets, each with the same sections: symptoms, read-only
diagnostics, preconditions, the commands and what they change, the guarantee that holds
while the runbook runs, stop conditions, verification, escalation, and the test that
executes it against the real binary (`crates/focal-node/tests/runbooks.rs`,
`runbook_<slug>`). Recovery limits and the guarantees each test exercised are stated in the
runbook; what was not exercised is stated too.

| Runbook | Failure | Test |
|---|---|---|
| [disk-exhaustion](disk-exhaustion.md) | The data volume refuses writes | `runbook_disk_exhaustion` |
| [corrupt-or-missing-content](corrupt-or-missing-content.md) | An artifact's bytes are lost or altered on a copy | `runbook_corrupt_or_missing_content` |
| [stalled-replication](stalled-replication.md) | A replica stops applying while the session goes on | `runbook_stalled_replication` |
| [node-loss](node-loss.md) | A host is gone for good | `runbook_node_loss` |
| [zone-loss](zone-loss.md) | Every host of one zone is gone | `runbook_zone_loss` |
| [region-loss](region-loss.md) | Every host of one region is gone | `runbook_region_loss` |
| [stale-clone](stale-clone.md) | A copy of a node's disk runs beside or instead of it | `runbook_stale_clone` |
| [failed-movement](failed-movement.md) | A range move stops half way | `runbook_failed_movement` |
| [expired-credentials](expired-credentials.md) | A node's credential is past its lifetime or revoked | `runbook_expired_credentials` |
| [interrupted-upgrade](interrupted-upgrade.md) | A binary below the committed fence, or a fence raised early | `runbook_interrupted_upgrade` |
| [interrupted-restore](interrupted-restore.md) | A restore that did not finish | `runbook_interrupted_restore` |

Common facts: a node's identity is its enrolled key, never its address or disk
([24 §24](../archictecutre/24-placement-execution-and-fleet-control.md)); acknowledged
writes are on the disks of a majority of a session's voters
([24 §7](../archictecutre/24-placement-execution-and-fleet-control.md)); the guarantee a
session actually has is `cluster placement` (`desired`, `achieved`, `blocked_by`) and
`cluster node readiness` (`policy_satisfied`), never what a plan requested
([08 §9](../archictecutre/08-stepped-complexity-and-deployment.md)).
