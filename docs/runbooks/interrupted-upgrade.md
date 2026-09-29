# Interrupted upgrade

**Failure.** A rolling upgrade stopped half way: some nodes run a binary below the committed
fence, or the fence was raised before every node was upgraded.

**Symptoms.** A node whose binary announces a level below the fence refuses to serve at
start (`[upgrade_fenced]`, exit 5, no readiness); `cluster upgrade status` on any node shows
`fence_level`, every node's reported level and `activatable`; `cluster upgrade activate`
refuses to raise the fence while a node reports less (`members_behind`, naming them).

**Read-only diagnostics.**

```sh
focal --data-dir DIR cluster upgrade status
focal --data-dir DIR cluster placement
```

**Preconditions.** The fence is committed by the founder's authority and never lowered
([24 §21](../archictecutre/24-placement-execution-and-fleet-control.md)); a binary's level
is what it announces, `FOCAL_CAPABILITY_LEVEL` may only lower it for a test.

**Commands.** Install the upgraded binary on every refused node and restart it; it serves as
soon as it announces the fence's level or more. To finish the upgrade, once `cluster upgrade
status` shows every node at the new level: `cluster upgrade activate --fence LEVEL`. A fence
raised too early is not lowered: upgrade the nodes behind it instead.

**Preserved guarantee.** No node serves below the fence, so no two binaries apply one log
with different rules; the fence rises exactly once per level.

**Stop conditions.** Stop if a node cannot be upgraded (unsupported platform): remove it
([node-loss](node-loss.md)) before raising the fence.

**Verification.** `cluster upgrade status` shows every node at or above `fence_level` and the
refused node publishes readiness.

**Escalation.** None: the fence is a committed fact with a single authority.

**Executed test.** `runbook_interrupted_upgrade`: a founder and a host at level 1; the
fence is raised to 1; the host restarted announcing level 0 refuses to serve
(`upgrade_fenced`), the fence cannot be lowered (`invalid_input`), and the host restarted at
level 1 serves again.

Every command above is under `focal --data-dir DIR cluster ...` on the node named, over its
own admin socket ([cluster-admin.md](../cluster-admin.md)); reads never change the cluster.
The executed test runs the real binary through this runbook's commands
(`crates/focal-node/tests/runbooks.rs`); its evidence is recorded in
[implementation status](../archictecutre/09-implementation-status.md).
