# Cluster administration

Cluster commands use the selected physical node's private local admin socket. The operating-system owner authenticates this connection. A named Unix client context selects that context's node; an enrolled or remote QUIC client context cannot acquire local administration. MCP advertises the `cluster.*` tools only when this local backend is installed. Ordinary participant credentials retain their existing authority.

The root metadata group and each installed application replica have separate configurations. Root membership changes do not assign application placement. Application membership changes do not certify retained artifacts, completed custody transfer, or achieved durability policy.

## Root metadata and enrollment

| CLI | MCP tool | Result |
| --- | --- | --- |
| `cluster status` | `cluster.status` | Quorum-read root configuration and leader information |
| `cluster nodes list` | `cluster.nodes.list` | Committed node contact announcements |
| `cluster membership show` | `cluster.membership.show` | Root configuration and applied prefix |
| `cluster membership add-learner --node N` | `cluster.membership.add_learner` | Durably applied configuration receipt |
| `cluster membership promote --node N` | `cluster.membership.promote` | Durably applied voter promotion after actual catch-up |
| `cluster membership remove --node N` | `cluster.membership.remove` | Durably applied removal |
| `cluster membership leave-joint` | `cluster.membership.leave_joint` | Durably applied joint-configuration exit |
| `cluster leader transfer --node N` | `cluster.leader.transfer` | Transfer initiation; inspect the leader afterward |
| `cluster request inspect` | `cluster.request.inspect` | Latest local root admin intent or saved receipt |
| `cluster request retry A1` | `cluster.request.retry` | Retry the identical retained intent |
| `cluster request reconcile A1` | `cluster.request.reconcile` | Recover its receipt or evaluate its committed precondition fence |
| `cluster invite --node NAME --output FILE` | `cluster.invite` | Private invitation for one physical node |
| `cluster client invite --name NAME --output FILE` | `cluster.client.invite` | Private invitation for an Actor client |
| `cluster invitations list` | `cluster.invitations.list` | Redacted committed invitation page |
| `cluster invitations get ID` | `cluster.invitations.get` | Redacted invitation status |
| `cluster invitations revoke ID` | `cluster.invitations.revoke` | Durable revocation of the invitation and its issued credential |
| `cluster credentials get --invitation ID` | `cluster.credentials.get` | Issued credential metadata, without private keys |
| `cluster credentials revoke --invitation ID` | `cluster.credentials.revoke` | Durable revocation through its issuing invitation |

Membership mutations and transfers accept `--expected-configuration-index`. Revocations accept `--expected-revision`. If omitted, the adapter obtains a current quorum view and saves or submits the resulting exact precondition. A concurrent change can reject that precondition; the adapter never silently changes a saved mutation. Invitation pages accept `--after`, `--limit` (1–64, default 32), and `--expected-revision` for continuation consistency.

Issuing invitations requires the founder's signing backend. A node invitation enrolls a physical Node; a client invitation enrolls an Actor and creates no physical node identity. Use `context enroll NAME --invite-file FILE` to retain the client's private enrollment state, then select it with `--client-context NAME` or `context use NAME`. The saved client key remains private. Issuing or revoking a credential does not change consensus membership.

## Local operator inspection

| CLI | MCP tool | Observed value |
| --- | --- | --- |
| `cluster node identity` | `cluster.node.identity` | The serving physical owner's immutable identifiers |
| `cluster node health` | `cluster.node.health` | Root-owner progress and installed/running fleet counts |
| `cluster node config` | `cluster.node.config` | Validated saved listen/advertise addresses and root namespace |
| `cluster replicas diagnostics [--session ID]` | `cluster.replicas.diagnostics` | Actual application commit/applied prefix, pending work/checkpoint, compiled managed decoder, durable required decoder and managed activation |

These observations are local diagnostics. Health does not probe every dependency or establish a quorum. Configuration reports the running owner's saved network coordinates, not the invoking client's settings or an inferred placement policy. The existing top-level `identity` command remains an offline metadata read; the admin identity command reaches the authenticated live owner.

Diagnostics does not initiate a checkpoint, decoder-floor installation or format upgrade. A null required decoder means the group has no fsynced decoder floor at that observation. A durable floor with `managed_active: false` means that decoder requirement is installed but managed admission has not committed activation. `checkpoint_pending` reports current work; it does not invent a last-checkpoint completion receipt. Format transitions still require the explicit compatibility work described in [the lifecycle storage upgrade design](archictecutre/18-lifecycle-storage-upgrade.md).

## Installed application replicas

| CLI | MCP tool | Result |
| --- | --- | --- |
| `cluster replicas list` | `cluster.replicas.list` | Bounded local inventory of installed application owners |
| `cluster replicas show [--session ID]` | `cluster.replicas.show` | Quorum-read application configuration |
| `cluster replicas membership [--session ID] add-learner --node N` | `cluster.replicas.add_learner` | Durably applied learner admission |
| `cluster replicas membership [--session ID] promote --node N` | `cluster.replicas.promote` | Durably applied promotion after catch-up and decoder-support checks |
| `cluster replicas membership [--session ID] remove --node N` | `cluster.replicas.remove` | Durably applied removal |
| `cluster replicas membership [--session ID] leave-joint` | `cluster.replicas.leave_joint` | Durably applied joint-configuration exit |
| `cluster replicas transfer [--session ID] --node N` | `cluster.replicas.transfer` | Transfer initiation under an exact configuration fence |
| `cluster replicas request inspect` | `cluster.replicas.request.inspect` | Latest local application admin intent or receipt |
| `cluster replicas request retry R1` | `cluster.replicas.request.retry` | Retry the identical retained application intent |
| `cluster replicas request reconcile R1` | `cluster.replicas.request.reconcile` | Recover the exact receipt or preserve a fenced unknown outcome |

`cluster replicas membership ... show` is a CLI alias for the configuration read. Omitted session selects the node identity's original application session. List accepts `--after SESSION` and `--limit` (1–64, default 32). Its management sequence and continuation describe a live local inventory, not a fixed historical snapshot or placement-authority proof. An initially empty fleet returns an empty inventory; selecting an absent or stopped owner fails explicitly.

Every application operation binds the actual tenant, session, group and current owner incarnation before dispatch. Configuration reads and successful membership replies use the existing current-term read barrier. Metadata changes leave the domain sequence unchanged. A managed-format group still requires the actual candidate's durable decoder support, and promotion still requires replication catch-up; enrollment alone cannot satisfy either condition.

## Exact recovery and limits

Root administration stores one latest intent in `CLUSTER.admin`, protected by `CLUSTER.admin.initialized`. References have the form `a1:<node>:<counter>`. Preserve both directory and marker. Inspect and retry the same reference after a lost reply. Reconcile can recover an exact retained control receipt; a later committed revision plus the root control registry's exact receipt lookup can establish that an uncommitted precondition was superseded. Absence alone remains pending.

Application administration independently stores one latest intent in `REPLICA.admin`, protected by `REPLICA.admin.initialized`. References have the form `r1:<node>:<request-id>`. They bind a distinct request identity and exact configuration precondition. Preserve both filesets. A lost local receipt can be recovered from the application's actual retained membership receipt.

Application consensus retains only its latest membership receipt. If a later operation has replaced that receipt, a newer committed configuration can permanently fence the old request without proving whether it once committed. Reconciliation returns `FencedOutcomeUnknown`; retry will not resubmit it. A new intent may then be created, with a different reference. Previously saved exact receipts remain exact local results. Older references expire when the bounded local journal advances; export results if longer audit retention is needed.

Both journals serialize concurrent processes with private locks, save exact intent before transmission, and fail closed on initialized-state loss or corruption. These are administration references, separate from participant `m1:` streams and legacy domain request identifiers. Transfer replies report initiation and have no durable completion receipt.

Placement drain, migration/custody completion and same-identity credential renewal require additional authority and durable state transitions. They are not exposed as successful aliases for membership removal, invitation issuance or revocation. This administration surface does not claim those workflows have completed.

The executable [CLI/MCP replica regression](../crates/focal-node/tests/support/cli_replicas.rs) checks committed learner admission/removal, unchanged domain sequence and exact receipts after restart. [Owner transfer tests](../crates/focal-node/src/fleet_membership_tests.rs) exercise current-configuration fences; [journal tests](../crates/focal-node/src/cluster_admin/replica_tests.rs) distinguish a lost recoverable receipt from a later fenced unknown outcome.
