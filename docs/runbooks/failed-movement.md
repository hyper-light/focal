# Failed movement

**Failure.** A range move (`cluster replicas ranges move`) stops half way: the destination
died, or the source did.

**Symptoms.** `cluster replicas ranges list --session ID` shows a transfer in progress whose
phase does not advance (`seed`, `barrier`, `ready` without `activate`); admission of writes
to the moving range answers retryable `range_moving`; the session's other ranges serve.

**Read-only diagnostics.**

```sh
focal --data-dir DIR cluster replicas ranges list --session ID
focal --data-dir DIR cluster placement
```

**Preconditions.** The movement record is in the session's log
([25 §6](../archictecutre/25-parallel-materialization-and-ranges.md)): every step is
committed before it takes effect, so a restarted participant resumes from the record.

**Commands.** Restart the participant that died with its disk; the session leader's
controller resumes the transfer from its committed step. If the destination is lost for
good, replace it ([node-loss](node-loss.md)): the transfer is retired at the barrier and the
source keeps serving.

**Preserved guarantee.** One holder serves a range at every barrier; a partial seed is never
authoritative; a stale writer is refused with `NotOwner`.

**Stop conditions.** Stop if `ranges list` shows the transfer's seed complete and the
barrier committed but no holder ready after the destination restarted: escalate with the
listing.

**Verification.** `ranges list` shows the member on its new holder, no transfer in
progress, and writes to the range commit.

**Escalation.** Restore the session from backup when both holders are lost.

**Executed test.** `runbook_failed_movement`: a native session on three hosts; a member's
move to another host is begun and the destination is killed before it completes; the
session keeps committing; the destination restarted with its disk lets the transfer finish,
and `ranges list` shows the member on the new holder.

Every command above is under `focal --data-dir DIR cluster ...` on the node named, over its
own admin socket ([cluster-admin.md](../cluster-admin.md)); reads never change the cluster.
The executed test runs the real binary through this runbook's commands
(`crates/focal-node/tests/runbooks.rs`); its evidence is recorded in
[implementation status](../archictecutre/09-implementation-status.md).
