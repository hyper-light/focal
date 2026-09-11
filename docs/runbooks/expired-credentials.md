# Expired credentials

**Failure.** A node's credential is past its lifetime, or was revoked, so its peers refuse
its connections.

**Symptoms.** `cluster placement` on any node shows the node's `credential: retired`
(the registry no longer authorizes it; `active` for every other node); the founder's
`cluster credentials get --invitation ID` shows `expires_at` in the past or the invitation
revoked; the node's own `cluster node health` shows the placement agent's `last_error`
naming `unauthorized`; `focal_credential_expires_at_seconds` in its metrics is past. Its
peers close its connections and admit no new ones, so it learns nothing more: `cluster
node readiness` on it shows `catching_up: false` and `authoritative: false`, and a node
whose own copy of the registry applied the revocation before it was cut off refuses to
serve or start (`[credential_retired]`, exit 5). It may still answer probes for a while, so
`alive` can lag: the credential field, not liveness, is the symptom.

**Read-only diagnostics.**

```sh
focal --data-dir FOUNDER cluster invitations list
focal --data-dir FOUNDER cluster credentials get --invitation ID
focal --data-dir NODE cluster node health
```

**Preconditions.** A node renews its own credential ten days ahead of expiry
([24 §11](../archictecutre/24-placement-execution-and-fleet-control.md)); a credential is
past its lifetime only when the node could not reach the founder for that long, or its
invitation was revoked.

**Commands.** Within the grace the sponsor allows: `focal --data-dir NODE cluster
credentials renew` on the node (the same key under a fresh certificate). Beyond it, or
after revocation: the node's identity is retired. Drain and remove it
(`cluster nodes drain --node N`, `cluster nodes remove --node N`) and enroll the host again
with a fresh invitation into a fresh data directory (`cluster invite`, `start
--invite-file`), then let the controller place it.

**Preserved guarantee.** A revoked or expired credential authorizes nothing, immediately and
everywhere the registry is applied; the node's copies are healed onto other hosts before
its removal completes.

**Stop conditions.** Stop if removal is refused with `node_holding` and no host can take the
copies: add capacity first.

**Verification.** The old node is gone from `cluster nodes list`; the re-enrolled host is
alive and eligible; every session's `achieved` equals `desired`.

**Escalation.** A founder whose credential expired cannot be renewed by anyone: restore it
from backup.

**Executed test.** `runbook_expired_credentials`: a host's invitation is revoked on the
founder (revocation models expiry beyond the grace; a real lifetime is thirty days); the
placement view shows its credential retired; restarted, the host either refuses
(`credential_retired`) or starts and never catches up; it is drained and removed; the same machine enrolls again from a fresh invitation into a fresh
directory and the placement heals onto it.

Every command above is under `focal --data-dir DIR cluster ...` on the node named, over its
own admin socket ([cluster-admin.md](../cluster-admin.md)); reads never change the cluster.
The executed test runs the real binary through this runbook's commands
(`crates/focal-node/tests/runbooks.rs`); its evidence is recorded in
[implementation status](../archictecutre/09-implementation-status.md).
