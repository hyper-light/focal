# Region loss

**Failure.** Every host of one region is gone, or the region is unreachable.

**Symptoms.** As [zone-loss](zone-loss.md) for the region's hosts; sessions placed for
`survive: region` keep committing across the remaining regions, with the cross-region round
trip in the write path ([08 §7](../archictecutre/08-stepped-complexity-and-deployment.md)).

**Read-only diagnostics.**

```sh
focal --data-dir DIR cluster placement          # nodes with region, sessions' residency and achieved level
focal --data-dir DIR cluster node readiness
```

**Preconditions.** The policy was `survive: region` with enough `max_failures`; the residency
(`placement.residency`) still names a region with capacity.

**Commands.** A region that returns rejoins by itself. A lost region's hosts are replaced by
hosts in a region inside the residency (`cluster nodes replace`); a host outside the
residency is refused before any byte moves (`outside_residency`).

**Preserved guarantee.** Acknowledged writes are on a majority of voters spread so that the
surviving regions hold one; residency is never crossed to recover faster.

**Stop conditions.** Stop if no region inside the residency has capacity: adding a region to
the residency is a policy change (`deployment plan`/`apply`), not a runbook step.

**Verification.** `achieved` equals `desired` for every session; every voter's region is
inside the residency.

**Escalation.** A fenced regional rejoin (the lost region returns with stale disks) is
[stale-clone](stale-clone.md) for each of its hosts.

**Executed test.** `runbook_region_loss`: a founder and two hosts in three regions, a
session placed for `survive: region, max_failures: 1`; one region's host is killed; a claim
still commits; the host returns with its disk and the guarantee is restored. Not exercised:
real cross-region latency.

Every command above is under `focal --data-dir DIR cluster ...` on the node named, over its
own admin socket ([cluster-admin.md](../cluster-admin.md)); reads never change the cluster.
The executed test runs the real binary through this runbook's commands
(`crates/focal-node/tests/runbooks.rs`); its evidence is recorded in
[implementation status](../archictecutre/09-implementation-status.md).
