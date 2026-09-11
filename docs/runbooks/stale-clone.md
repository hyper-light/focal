# Stale clone

**Failure.** A copy of a node's data directory runs while the node runs (a cloned VM, a
restored snapshot started beside the original), or runs instead of it from an older image.

**Symptoms.** The clone starts and publishes readiness but never leaves `catching_up`: its
contact announcement is refused because the original still answers at the committed
address ([24 §24](../archictecutre/24-placement-execution-and-fleet-control.md)),
`cluster placement` keeps showing the original's `advertise`, and the clone's `cluster node
health` shows the placement agent idle with no root leader. An older image started
*instead* of the node rejoins as a follower and replays forward from the leader.

**Read-only diagnostics.**

```sh
focal --data-dir CLONE cluster node identity        # the same node id as the original
focal --data-dir FOUNDER cluster placement          # whose advertise the root holds
focal --data-dir CLONE cluster node readiness       # catching_up stays false, authoritative false
```

**Preconditions.** A node's identity is its enrolled key: two processes with one key are one
node to the cluster. The `LOCK` file protects one volume; two volumes are not protected by
it.

**Commands.** Stop the clone. If the original is gone and the clone is the only copy, it
takes over when the failure detector confirms the original dead: nothing to do but watch
`cluster placement` adopt the clone's address. If the clone's disk is older than the
original's last acknowledged writes and the node was a voter, do not run it: remove the node
(`cluster nodes drain`, `cluster nodes remove`) and enroll a fresh host instead, so the
session never counts a voter that forgot what it acknowledged.

**Preserved guarantee.** A live contact is never displaced by another process's
announcement: before moving a node's contact the root probes the committed address itself
and refuses while anything answers there as the node; a moved node is admitted as soon as
its old address stops answering, one probe timeout after it announces (the root answers the
announcement at once, "not yet" while it asks, and the mover announces again).

**Stop conditions.** Stop if two processes with one identity have both been serving clients
(both were leaders of something): compare their logs before choosing, and prefer removal and
re-enrollment.

**Verification.** `cluster placement` holds one address for the node, the one that serves;
the other process is stopped.

**Escalation.** Restore the session from backup when a rolled-back voter served writes.

**Executed test.** `runbook_stale_clone`: a host's directory is copied while it is stopped,
the host is restarted, then the clone is started at another address: its contact is refused
while the original is alive, the placement view keeps the original's address and the clone
never becomes `catching_up`; the clone is stopped. Then the original is killed and the clone
is admitted once the original is confirmed dead. Not exercised: a rolled-back voter that had
acknowledged writes.

Every command above is under `focal --data-dir DIR cluster ...` on the node named, over its
own admin socket ([cluster-admin.md](../cluster-admin.md)); reads never change the cluster.
The executed test runs the real binary through this runbook's commands
(`crates/focal-node/tests/runbooks.rs`); its evidence is recorded in
[implementation status](../archictecutre/09-implementation-status.md).
