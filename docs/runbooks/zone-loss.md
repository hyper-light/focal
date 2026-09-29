# Zone loss

**Failure.** Every host of one availability zone is gone at once.

**Symptoms.** As [node-loss](node-loss.md) for every node of that zone; sessions placed for
`survive: zone` keep committing, since their voters span `2f+1` zones
([24 §22](../archictecutre/24-placement-execution-and-fleet-control.md)).

**Read-only diagnostics.**

```sh
focal --data-dir DIR cluster placement          # nodes with region/zone, sessions' achieved level
focal --data-dir DIR cluster node readiness
```

**Preconditions.** The durability policy was `survive: zone` with `max_failures` at least the
number of zones lost, and the plan that activated it reported `achieved` at that level;
otherwise the loss is beyond the promise and some sessions are unavailable.

**Commands.** If the zone returns, its hosts restart with their disks and catch up: nothing
to do but watch `cluster placement` recover. If the zone is lost, enroll hosts in a
surviving or new zone (`start --invite-file` with `topology.zone` set) and replace each
lost host: `cluster nodes replace --node LOST --with NEW`. A new zone's region is
registered by the root when the first host announces it.

**Preserved guarantee.** Writes acknowledged before the loss are on voters in the surviving
zones; a placement never claims zone survival with two voters in one zone.

**Stop conditions.** Stop if the surviving hosts cannot hold `2f+1` zones: the policy is
unsatisfiable until a zone is added, and `deployment plan` says so.

**Verification.** Every session shows `achieved` equal to `desired`; the replaced hosts are
gone; the new hosts carry their zone labels in `cluster placement`.

**Escalation.** [region-loss](region-loss.md) when the zone loss is one region's whole
footprint.

**Executed test.** `runbook_zone_loss`: a founder and two hosts in three zones, a session
placed for `survive: zone, max_failures: 1`; one zone's host is killed; a claim still
commits; the host restarted with its disk rejoins and the guarantee is restored. Not
exercised: replacing the zone with a new one.

Every command above is under `focal --data-dir DIR cluster ...` on the node named, over its
own admin socket ([cluster-admin.md](../cluster-admin.md)); reads never change the cluster.
The executed test runs the real binary through this runbook's commands
(`crates/focal-node/tests/runbooks.rs`); its evidence is recorded in
[implementation status](../archictecutre/09-implementation-status.md).
