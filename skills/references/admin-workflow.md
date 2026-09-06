# Administrative operation identity and recovery

Administrative tools are installed only for an authenticated local physical node.
Discovery metadata does not grant authority. Ordinary domain operations use
their separate participant credentials and `m1:` request streams.

The root metadata group and application replicas have distinct configurations
and journals. Choose the intended group before mutation. Current configuration
reads use the owner's quorum barrier; local node health and replica inventory
are observations, not quorum or durability guarantees.

The caller may supply an observed configuration index or invitation revision.
When omitted, the adapter reads the current precondition and saves that exact
intent. Recovery never refreshes a saved mutation to a newer precondition.
Mutations require authorization from the user's actual task; tool availability
alone is not authorization to change topology or issue credentials.

Root changes return an `a1:` reference backed by `CLUSTER.admin`. Application
changes return an `r1:` reference backed by `REPLICA.admin`. Retain the reference
before continuing a dependent change. After a lost response, inspect and retry
that original reference using the same selected physical owner. A JSON-RPC
request ID is only transport correlation; repeating a fresh administrative
operation can create a different intent.

Each journal retains one latest administrative intent. Export its result before
starting an unrelated change when long-term audit retention is needed. Preserve
the initialized marker and journal together; missing initialized history must
be restored, not recreated. Older references expire when the local journal
advances.

Application consensus retains its latest membership receipt. An exact retained
receipt proves the outcome. If another membership change has replaced it,
`FencedOutcomeUnknown` proves the original precondition can no longer execute;
it does not establish whether the original operation once committed. Report that
distinction and inspect current configuration before authoring any replacement.

Leadership transfer returns initiation rather than a committed completion
receipt. Observe the new leader through configuration reads. Repeating transfer
with a different current fence is a new operator decision.

Invitation material is private. Node invitations enroll physical nodes; client
invitations enroll Actor participants. Preserve the original key, CSR, invitation
and saved request throughout a retry. Issuance and revocation operate on identity,
not consensus membership or application custody.

No tool in this skill drains a placement, renews an existing credential, installs
a lifecycle format, launches workers, or certifies global protection. Report
only the concrete state returned by the relevant authoritative subsystem.
