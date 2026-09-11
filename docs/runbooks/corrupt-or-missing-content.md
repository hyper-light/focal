# Corrupt or missing content

**Failure.** A sealed artifact's chunk is gone from a copy's content store, or its bytes no
longer match the hash the ledger names.

**Symptoms.** `cluster replicas diagnostics` shows `custody_objects_missing` on the copy; a
read of the artifact on that node fails verification; `cluster placement` may show the
session's guarantee `blocked_by` a custody blocker; `focal_session_custody_objects_missing`
in `cluster node metrics`.

**Read-only diagnostics.**

```sh
focal --data-dir DIR cluster replicas diagnostics --session ID
focal --data-dir DIR cluster placement
focal --data-dir DIR cluster storage show
```

**Preconditions.** The session names the object in its committed prefix and at least one
other required copy holds it verified ([24 §20](../archictecutre/24-placement-execution-and-fleet-control.md)).
Repair never invents bytes: an object no copy can supply is reported, not replaced.

**Commands.**

```sh
focal --data-dir DIR cluster repair --tenant T --session S   # re-verify, recopy, complete peers
```

The reply lists what was `verified`, `repaired`, `pushed` and `unrecoverable`; a partial run
names `next_after` to continue from. Repeat until `complete`.

**Preserved guarantee.** Every object is verified against the hash the ledger names before
it is served or pushed; a corrupt chunk is replaced only by verified bytes from another
required copy; the ledger's history is untouched.

**Stop conditions.** Stop when `unrecoverable_count` is nonzero: the object exists on no
required copy. Do not delete the corrupt copy's other objects.

**Verification.** A second `cluster repair` reports everything `verified` and nothing
`repaired`; the artifact reads back on the repaired node with its recorded hash.

**Escalation.** An unrecoverable object is restored from a backup that holds it
([interrupted-restore](interrupted-restore.md)), or acknowledged as lost evidence in the
claim's record; Focal never marks it present.

**Executed test.** `runbook_corrupt_or_missing_content`: a session with three copies; one
copy loses a chunk file and another holds it with altered bytes; `cluster repair` on each
re-verifies, recopies from a healthy copy, and a second run reports nothing to repair; the
artifact's bytes read back on every copy.

Every command above is under `focal --data-dir DIR cluster ...` on the node named, over its
own admin socket ([cluster-admin.md](../cluster-admin.md)); reads never change the cluster.
The executed test runs the real binary through this runbook's commands
(`crates/focal-node/tests/runbooks.rs`); its evidence is recorded in
[implementation status](../archictecutre/09-implementation-status.md).
