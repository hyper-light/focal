# Interrupted restore

**Failure.** `cluster restore` did not finish: the node was killed during the restore, or the
command was retried after it completed.

**Symptoms.** The restored session is absent from `cluster placement` on the node, or
present without its copies verified; a retried restore is refused because the directory
already holds the ledger.

**Read-only diagnostics.**

```sh
focal --data-dir DIR cluster backup verify --input BACKUP     # runs anywhere the binary does
focal --data-dir DIR cluster placement
focal --data-dir DIR diagnose cluster --replicas --session ID
```

**Preconditions.** The backup verifies ([26 §6](../archictecutre/26-custody-archive-retention-and-restore.md));
the restore target is a node of the cluster the backup is meant for, or a recovery
incarnation is acknowledged (`--new-incarnation`) when the source cannot be fenced.

**Commands.** Restart the node if it stopped, then issue the restore again with the same
arguments: `cluster restore --input BACKUP [--new-incarnation]`. A restore that completed
is answered as restored and nothing is changed; one that was interrupted goes on where it
was cut: content and seeds are brought again, a log that holds the backup's image and
nothing else is opened as it is, and a copy that was recorded is the restore, complete. A
log of the group that holds anything else is history, and the restore is refused.

**Preserved guarantee.** A restored prefix is exactly the backup's declared prefix; a partial
restore is never registered; an unfenced source always yields a recovery incarnation, never
a claim of continuation.

**Stop conditions.** Stop if `backup verify` reports problems: a restore never proceeds from
a backup that does not verify.

**Verification.** `cluster placement` lists the restored session; a claim from the backup
reads back with its artifact bytes; the restore's `recovery_point` is the backup's prefix.

**Escalation.** None beyond another backup.

**Executed test.** `runbook_interrupted_restore`: a backup of a founder's session; a fresh
founder restores it and stops where the restore is cut, once for each place a restore can
be cut at: with the backup's content brought, with its log begun, with its copy recorded.
The node is restarted and the restore is issued again, which completes, and again, which
changes nothing; the claim and its artifact read back from the restored session.

Every command above is under `focal --data-dir DIR cluster ...` on the node named, over its
own admin socket ([cluster-admin.md](../cluster-admin.md)); reads never change the cluster.
The executed test runs the real binary through this runbook's commands
(`crates/focal-node/tests/runbooks.rs`); its evidence is recorded in
[implementation status](../archictecutre/09-implementation-status.md).
