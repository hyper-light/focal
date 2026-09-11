# Disk exhaustion

**Failure.** The data volume of a node cannot take another byte: the filesystem is full, or a
quota or size limit refuses the write.

**Symptoms.** Writes through this node fail with an operation error naming the log (exit 1,
`[operation_failed]`), never with an acknowledgement; `cluster storage show` reports the
volume's `free` near zero and `headroom` unmet; `focal_disk_free_bytes` in
`cluster node metrics` is at the floor; the node may stop its ledger owner and exit when the
write-ahead log itself cannot be extended (`ledger egress ended`). Readers keep answering
from the durable prefix.

**Read-only diagnostics.**

```sh
focal --data-dir DIR cluster storage show        # disk: free, outstanding, headroom, by kind
focal --data-dir DIR cluster node metrics | grep focal_disk
focal --data-dir DIR cluster retention show      # what holds the retention floor (cursors, archive)
focal --data-dir DIR cluster gc show             # what the collector reclaimed and quarantined
```

**Preconditions.** The volume is the node's own data directory; another node's volume is
another runbook run. A write refused for lack of space was never acknowledged
([26 §7](../archictecutre/26-custody-archive-retention-and-restore.md)): there is nothing
to reconcile on the client beyond retrying the same request identity.

**Commands.**

1. Free space on the volume, outside Focal, or raise the limit. Focal's own reclaim is
   bounded by the retention floor: a consumer that stopped acknowledging holds the log
   (`cluster retention show` shows `blocker: cursors`); an archive that has not received a
   family holds it (`blocker: archive`). Nothing below the floor is deleted.
2. Restart the node if it stopped: `focal --data-dir DIR start ...` with the same arguments.
   Recovery replays the durable log; a write that was refused is absent, a write that was
   acknowledged is present.
3. Retry the refused requests with their original identities (`focal request retry
   --operation-id ...`); an unknown outcome is answered from the retained receipt.

**Preserved guarantee.** No acknowledged write is lost; no refused write is silently applied.
The other voters of a session continue without this node as long as a majority holds.

**Stop conditions.** Stop and escalate if the node fails to start after space was freed
(the log is refused as corrupt rather than short), or if `cluster storage show` still
reports `free` at zero after the filesystem says otherwise (a stale sample: wait one
sampling interval, then escalate).

**Verification.** After restart, `cluster node readiness` reports `alive` and, for a founder,
`authoritative`; a claim written before the exhaustion is read back unchanged; a new claim
commits.

**Escalation.** Restore from a backup on a fresh volume
([interrupted-restore](interrupted-restore.md)) when the log cannot be reopened.

**Executed test.** `runbook_disk_exhaustion`: a founder is started under a file-size limit
(`ulimit -f`, with `SIGXFSZ` ignored, so an oversized write returns `EFBIG` rather than
killing the process: this models a full volume as a write failure, not as `ENOSPC`); a
claim commits before the limit is reached; the write that crosses it is refused without
acknowledgement; the node restarted without the limit reads the first claim back and
commits a new one. Not exercised: quota systems and the collector reclaiming space
under pressure.

Every command above is under `focal --data-dir DIR cluster ...` on the node named, over its
own admin socket ([cluster-admin.md](../cluster-admin.md)); reads never change the cluster.
The executed test runs the real binary through this runbook's commands
(`crates/focal-node/tests/runbooks.rs`); its evidence is recorded in
[implementation status](../archictecutre/09-implementation-status.md).
