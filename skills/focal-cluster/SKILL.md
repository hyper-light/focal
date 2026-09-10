---
name: focal-cluster
description: >-
  Inspect a Focal cluster, enroll nodes or participants, and perform authorized
  membership changes with exact administrative recovery.
---

Use the versions in [the skill manifest](../manifest.json). Discover all tool
pages and read [the administration workflow](../references/admin-workflow.md).
The connected local Unix owner supplies administration. A remote participant
context supplies ordinary ledger operations; it does not grant node ownership.

## Determine the serving owner and requested scope

1. Inspect `cluster.node.identity`, `cluster.node.config` and `cluster.node.health`.
   Completion: identify the physical node, saved network coordinates and observed
   progress. Local health does not establish quorum or multi-region protection.
2. Use `cluster.status`, `cluster.nodes.list` and `cluster.membership.show` for
   the root metadata group. Use `cluster.replicas.list`, `cluster.replicas.show`
   and `cluster.replicas.diagnostics` for application replicas. Completion:
   select the actual session/group and committed configuration before proposing
   a change. Root membership and application placement are distinct facts.
3. Use `cluster.retention.show` for a native session's retention floor (what
   registered consumers still need, what the archive holds, the families
   retired) and `cluster.archive.show` with a retired claim's id to verify the
   bundle this node holds and which copies hold a receipt for it. Completion:
   a retired claim answers reads with its continuation; its rows live in the
   bundle, and nothing here restores them into the core.
4. Use `cluster.gc.show` for the collector's settings and its last pass
   (what it protected, expired, quarantined and deleted), and
   `cluster.gc.restore` with an object's domain and root to bring a
   quarantined object back before its round expires. Completion: quarantine
   is reversible until the quarantine grace passes; nothing the committed
   rows name is ever a candidate.
5. Use `cluster.backup.create` with an absolute output directory to write a
   coherent backup of a hosted native session at its committed prefix (the
   durable envelope, its seed chunks, every object the prefix names, and a
   manifest written last), and `cluster.backup.verify` with that directory
   to check it file by file and against its own envelope. Completion: a
   directory without a manifest is not a backup; `complete` is true only
   when every check passed and this binary carries the backup's decoder.
6. Use `cluster.restore` with a verified backup directory to host that
   session on this node from the backup's prefix. The old incarnation
   continues only when the backup came from this cluster and every other
   member of its membership is revoked; otherwise pass `new_incarnation`
   to acknowledge a recovery incarnation under a new log group. Completion:
   the restored session registers with the directory as founded here; its
   former participants' credentials belong to the old cluster, so their
   history is readable here but they cannot act until enrolled again.
7. Use `cluster.storage.show` for this node's storage pressure (the volume
   envelope by kind, its headroom and completion reserve, what uploads
   have staged), the archive agent's and the collector's settings and
   progress, and every hosted session's retention floor. Completion: the
   view initiates nothing; a floor held by `cursors` moves when consumers
   acknowledge, one held by `archive` when the archive reports.

## Enroll only the requested identity

For a physical node use `cluster.invite`; for an ordinary peer use
`cluster.client.invite`. Deliver invitations through the user's authorized
channel and retain private join/enrollment state. The CLI completes node `join`
or `context enroll`; retry the original invitation and key after interruption.
Completion: inspect `cluster.invitations.get` and `cluster.credentials.get` for
the issued identity. A new credential does not promote a voter or add custody.

Use `cluster.invitations.list` with its revision-bound continuation to inspect
issuance. Authorized revocation uses `cluster.invitations.revoke` or
`cluster.credentials.revoke` with the observed revision. Completion: confirm
committed revocation; disconnect alone does not revoke a credential.
`cluster.credentials.renew` renews the local node's own credential now (the
same key under a fresh certificate and lifetime; a node renews itself ahead
of expiry without being asked). Completion: the reply names the new expiry and
fingerprint; a retry after an interruption converges on the committed
renewal. It does not rotate the key, and the founder's identity is not
renewed this way.

## Inspect placement and the controller's plan

`cluster.placement` reads every directory partition this node acts on as its
placement agent last observed it: the enrolled nodes with their liveness and
load, and each session's placement, epochs, desired and achieved guarantee
(`survive`/`max_failures` against `achieved_*`), what blocks the guarantee
(`blocked_by`), any pending plan with per-node progress, and copies being
retired. `cluster.plan` lists the bounded next actions the controller takes
unattended (begin, install, add or promote a learner, cut over, activate,
drain, retire, replan, split or merge). Both are reads of committed facts;
neither changes placement. A guarantee is achieved only when every promised
failure domain is measurably provided by live, eligible nodes; a session with
`blocked_by` entries is served under a weaker guarantee until the controller
heals it. Reports are bounded (48 sessions and 128 nodes per partition,
`truncated` says when more exist).

## Serve a tenant and create its sessions

`cluster.tenants.admit` (founder only) admits a tenant the cluster serves as a
committed enrollment fact; every node's certificate grants and the local
socket then name it without a restart, and retrying an admitted tenant reads
as done. `cluster.tenants.list` names the founder's tenant and every admitted
one at the committed revision. `cluster.sessions.create` opens an application
session on the connected node for the founder's tenant or an admitted one:
the identity derives from the cluster, tenant and name, so the same name is
the same session and a retry answers `existing`. The reply names the ledger,
its log group and the node; the agent registers the session with the
directory and places it under the deployment policy like the founder's.
Completion: `cluster.placement` lists the session with its founding node.
Tenants are admitted, never removed.

## Ask for a durability

A session is placed under the durability its founder registered; to expand
a laptop session once hosts have joined, request the durability with
`cluster.sessions.plan` (`survive` node, zone or region and `max_failures`).
The planner picks live, eligible nodes from the committed directory and the
controller executes the plan unattended. The reply is `planned`, `pending`
(a plan is already under way; this is it) or `satisfied` (the active
placement already provides it); a retry is exact. Completion: watch
`cluster.placement` until the session shows no `pending` plan, the new
`route_epoch` and `achieved_max_failures` equal to the request; `cluster.plan`
is then empty. Only the user's requested durability is asked for; residency
and home regions stay those of the active policy. To see what would be
planned without committing anything, pass `dry_run: true`: the reply is the
same proposal and the directory journals nothing.

## Change the selected consensus configuration

Perform only the membership change covered by the user's request. Use the
observed configuration index as the precondition. Let the owner enforce catch-up,
quorum and decoder support; enrollment is not evidence of readiness.

- Root group: `cluster.membership.add_learner`, `cluster.membership.promote`,
  `cluster.membership.remove`, `cluster.membership.leave_joint`.
- Application group: `cluster.replicas.add_learner`, `cluster.replicas.promote`,
  `cluster.replicas.remove`, `cluster.replicas.leave_joint`.
- Leadership: `cluster.leader.transfer` or `cluster.replicas.transfer`.

Completion: retain the actual committed membership receipt and reread the
selected configuration. Transfer reports initiation; observe the subsequent
leader before claiming it completed. Membership removal is not a drain,
artifact migration or achieved deployment policy.

## Retire a host

To take a host out of service, drain it first: `cluster.nodes.drain`
(`node`) re-issues its grant ineligible; the controller then moves every
placement that named it and retires its copies. Completion of the drain:
`cluster.placement` lists the node with `eligible: false` and no session
names it as a voter, materializer, content copy, retiring copy or pending
assignment, and each session's guarantee is achieved again. Only then
`cluster.nodes.remove` (`node`) removes its root-group membership and
revokes its credential; it is refused while the node is eligible
(`not_drained`) or still holds copies (`node_holding`), and a repeat after
an interruption resumes. `cluster.nodes.replace` (`node`, `replacement`)
drains a host once its replacement is enrolled, alive, eligible and
reporting; `cluster.nodes.undrain` reverses a drain. The founder is never
drained. A placement the remaining hosts cannot satisfy stays where it is
and reports `NoPlacement`; add capacity or undrain rather than removing.

For interrupted root changes use `cluster.request.inspect`,
`cluster.request.retry` and `cluster.request.reconcile`. For application changes
use `cluster.replicas.request.inspect`, `cluster.replicas.request.retry` and
`cluster.replicas.request.reconcile`. Preserve the exact `a1:` or `r1:` reference
and original physical owner. Completion: recover its receipt or report its
specific unresolved/fenced state. A newer configuration does not prove an old
application request never committed.

## Report the achieved step

Use the committed facts above to say what changed and what remains pending.
For a laptop, ordinary `focal start` and local domain commands need no cluster
administration. Each larger deployment adds only its needed identity, endpoint
and placement choices. Describe unimplemented credential rotation, repair,
upgrade fences or global guarantees as unavailable; avoid substituting
a successful command from another scope.

## Native engine

`cluster.node.health` and `cluster.status` report each hosted ledger's engine
(the active storage format, its effective guarantee and decoder floor).
Activating the native engine is an offline operator command of the CLI
(`focal cluster replicas activate-native`, run before the node listens) and
has no MCP tool; a running native ledger needs no cluster administration
beyond the changes above, and no tool here changes an engine in place.
