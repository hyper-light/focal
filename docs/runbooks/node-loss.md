# Node loss

**Failure.** A host is gone for good: the machine, or its disk.

**Symptoms.** `inspect placement` shows the node `alive: false` and every session that
named it `blocked_by` it; `inspect node --readiness` on any node reports `policy_satisfied:
false` for those sessions. Writes continue while each session keeps a majority.

**Read-only diagnostics.**

```sh
focal --data-dir DIR inspect placement
focal --data-dir DIR list nodes
focal --data-dir DIR inspect node --readiness
```

**Preconditions.** Every session that named the node keeps a majority of voters; a spare host
is enrolled and alive to take the lost node's place (`invite node`, `start node
--invite-file`).

**Commands.**

```sh
focal --data-dir FOUNDER replace node --node LOST --with SPARE
```

Replacement drains the lost node (its grant is re-issued ineligible) once the spare reports,
the controller heals every placement that named it onto the eligible hosts, and retires its
copies ([24 §19](../archictecutre/24-placement-execution-and-fleet-control.md)). When
healing is complete:

```sh
focal --data-dir FOUNDER remove node --node LOST      # refused while it still holds copies
```

**Preserved guarantee.** No write is acknowledged with fewer copies than the placement
promises; a healed placement is stronger only after its new copy is verified and promoted;
removal is refused while the node holds a copy something still needs. A death is held for
one election window of the session's group before it moves a seat
([27 §5](../archictecutre/27-consensus-roadmap-and-slates-port.md)): a node that returns
within it keeps its seat, and `plan placement` says for how many seconds a death still stands.

**Stop conditions.** Stop if `nodes replace` is refused for lack of capacity
(`node_not_ready`, or the placement's `blocked_by` names capacity): add a host first. The
founder is never drained or removed.

**Verification.** `inspect placement` lists the spare as a voter of each healed session with
`achieved` equal to `desired`; the lost node is gone from `list nodes`.

**Escalation.** A lost founder is not replaced by this runbook: restore it from its backup
([interrupted-restore](interrupted-restore.md)).

**Executed tests.** `runbook_node_loss`: a founder, two voters and a spare; one voter is
killed; a claim still commits; `nodes replace` heals the session onto the spare and
`nodes remove` succeeds once the copies are retired; the session's guarantee is back to
`max_failures: 1`. `runbook_node_loss_within_the_hold_moves_no_seat`: the same fleet; a
voter goes silent (`SIGSTOP`), its death is committed and the plan says for how long it
stands; it returns, keeps its seat, and nothing moves after the hold would have passed; it
goes silent for good, and once its death has stood the seat goes to the spare without an
operator.

Every command above is run as `focal --data-dir DIR ACTION THING ...` on the node named, over its
own admin socket ([cluster-admin.md](../cluster-admin.md)); reads never change the cluster.
The executed test runs the real binary through this runbook's commands
(`crates/focal-node/tests/runbooks.rs`); its evidence is recorded in
[implementation status](../archictecutre/09-implementation-status.md).
