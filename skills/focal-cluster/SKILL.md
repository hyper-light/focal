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
committed revocation; disconnect alone does not revoke a credential. These tools
do not renew the same identity or rotate a running node's key.

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
and placement choices. Describe unimplemented drain, credential renewal,
deployment activation or global guarantees as unavailable; avoid substituting
a successful command from another scope.
