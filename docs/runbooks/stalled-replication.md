# Stalled replication

**Failure.** One replica of a session stops applying: the process is alive but wedged, paused,
or cut off, while the leader and the other voters go on.

**Symptoms.** `cluster placement` shows the node `alive: false` once the failure detector
confirms it ([24 §12](../archictecutre/24-placement-execution-and-fleet-control.md)) and the
session's guarantee `blocked_by` that member; `cluster replicas diagnostics --session ID`
on the stalled node shows `applied_index` frozen while the leader's advances;
`focal_session_apply_lag` and `focal_liveness_members{status="suspect"}` in `cluster node
metrics`. Writes still commit while a majority of voters applies.

**Read-only diagnostics.**

```sh
focal --data-dir DIR cluster placement
focal --data-dir DIR cluster replicas diagnostics --session ID      # on the leader and the stalled node
focal --data-dir DIR cluster node readiness                          # catching_up, authoritative
focal --data-dir DIR cluster node metrics | grep -E 'apply_lag|liveness'
```

**Preconditions.** A majority of the session's voters is healthy; otherwise the session is
unavailable for writes and this is [node-loss](node-loss.md) or worse.

**Commands.**

1. Find the cause on the stalled host (a paused process, a full disk, a partition). Resume
   or restart the process: `focal --data-dir DIR start ...`. A restarted replica replays its
   log and catches up from the leader; readiness shows `catching_up` until it is level.
2. If the host will not return, treat it as lost: [node-loss](node-loss.md).

**Preserved guarantee.** Acknowledged writes are on a majority of voters' disks; the stalled
replica never serves a prefix it did not apply; the failure detector never declares a node
dead on one node's word ([24 §12](../archictecutre/24-placement-execution-and-fleet-control.md)).

**Stop conditions.** Stop when the leader itself is the stalled node and no other voter
became leader: the session is unavailable, and forcing a leader is not a command Focal
offers.

**Verification.** The node is `alive: true` again in `cluster placement`, the session's
`achieved` equals `desired` with an empty `blocked_by`, and the replica's `applied_index`
matches the leader's.

**Escalation.** [node-loss](node-loss.md) when the host is gone; a support bundle from the
stalled node's diagnostics when it is alive but never catches up.

**Executed test.** `runbook_stalled_replication`: three voters; one host is paused with
`SIGSTOP`; a claim still commits; the founder's view marks the host not alive and the
guarantee blocked; the host is resumed with `SIGCONT` and the view recovers with the
guarantee restored.

Every command above is under `focal --data-dir DIR cluster ...` on the node named, over its
own admin socket ([cluster-admin.md](../cluster-admin.md)); reads never change the cluster.
The executed test runs the real binary through this runbook's commands
(`crates/focal-node/tests/runbooks.rs`); its evidence is recorded in
[implementation status](../archictecutre/09-implementation-status.md).
