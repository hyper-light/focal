# Node loss

**Failure.** A host is gone for good: the machine, or its disk.

**Symptoms.** `cluster placement` shows the node `alive: false` and every session that
named it `blocked_by` it; `cluster node readiness` on any node reports `policy_satisfied:
false` for those sessions. Writes continue while each session keeps a majority.

**Read-only diagnostics.**

```sh
focal --data-dir DIR cluster placement
focal --data-dir DIR cluster nodes list
focal --data-dir DIR cluster node readiness
```

**Preconditions.** Every session that named the node keeps a majority of voters; a spare host
is enrolled and alive to take the lost node's place (`cluster invite`, `start --invite-file`).

**Commands.**

```sh
focal --data-dir FOUNDER cluster nodes replace --node LOST --with SPARE
```

Replacement drains the lost node (its grant is re-issued ineligible) once the spare reports,
the controller heals every placement that named it onto the eligible hosts, and retires its
copies ([24 §19](../archictecutre/24-placement-execution-and-fleet-control.md)). When
healing is complete:

```sh
focal --data-dir FOUNDER cluster nodes remove --node LOST      # refused while it still holds copies
```

**Preserved guarantee.** No write is acknowledged with fewer copies than the placement
promises; a healed placement is stronger only after its new copy is verified and promoted;
removal is refused while the node holds a copy something still needs.

**Stop conditions.** Stop if `nodes replace` is refused for lack of capacity
(`node_not_ready`, or the placement's `blocked_by` names capacity): add a host first. The
founder is never drained or removed.

**Verification.** `cluster placement` lists the spare as a voter of each healed session with
`achieved` equal to `desired`; the lost node is gone from `cluster nodes list`.

**Escalation.** A lost founder is not replaced by this runbook: restore it from its backup
([interrupted-restore](interrupted-restore.md)).

**Executed test.** `runbook_node_loss`: a founder, two voters and a spare; one voter is
killed; a claim still commits; `nodes replace` heals the session onto the spare and
`nodes remove` succeeds once the copies are retired; the session's guarantee is back to
`max_failures: 1`.

Every command above is under `focal --data-dir DIR cluster ...` on the node named, over its
own admin socket ([cluster-admin.md](../cluster-admin.md)); reads never change the cluster.
The executed test runs the real binary through this runbook's commands
(`crates/focal-node/tests/runbooks.rs`); its evidence is recorded in
[implementation status](../archictecutre/09-implementation-status.md).
