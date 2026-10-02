# Remediation of the 2026-09-29 audit

The audit ([2026-09-29_audit.md](2026-09-29_audit.md)) lists 35 findings. This record
tracks each to closure in the audit's own terms (§13): the cause, the fix at the cause,
the regression tests, the measurements where a finding is about cost, and the commit.
A larger limit, a longer timeout, a quiet log or a renamed guarantee is not closure.
Statuses: **open** (not started), **designed** (a written design with the exact sites,
awaiting implementation or a decision), **in tree** (implemented and tested, not yet
committed), **closed** (committed, gates green), **decision** (needs the operator's
ruling before work starts).

## Index

| ID | Priority | Status | Batch | Where |
|---|---|---|---|---|
| F01 | P1 | closed (a6cb86e) | 1 | [F01](#f01) |
| F02 | P1 | in tree | 1 | [F02](#f02) |
| F03 | P1 | in tree | 3 | [F03](#f03) |
| F04 | P1 | in tree | 2 | [F04](#f04) |
| F05 | P2 | in tree | 2 | [F05](#f05) |
| F06 | P1 | in tree | 2 | [F06](#f06) |
| F07 | P2 | in tree | 4 | [F07](#f07) |
| F08 | P2 | in tree | 4 | [F08](#f08) |
| F09 | P2 | in tree | 2 | [F09](#f09) |
| F10 | P2 | in tree | 4 | [F10](#f10) |
| F11 | P2 | in tree | 4 | [F11](#f11) |
| F12 | P1 | in tree | 5 | [F12](#f12) |
| F13 | P1 | in tree (stages 1–2; 3 designed) | 5 | [F13](#f13) |
| F14 | P1 | in tree | 6 | [F14](#f14) |
| F15 | P2 | in tree | 3 | [F15](#f15) |
| F16 | P2 | in tree | 3 | [F16](#f16) |
| F17 | P2 | in tree (the commit and the overlap; the log's fence open) | 6 | [F17](#f17) |
| F18 | P2 | open | 6 | — |
| F19 | P2 | in tree | 6 | [F19](#f19) |
| F20 | P1 | in tree | 3 | [F20](#f20) |
| F21 | P1 | open | 7 | — |
| F22 | P1 | open | 5 | — |
| F23 | P2 | open | 6 | — |
| F24 | P1 | in tree (root group; partition groups open) | 5 | [F24](#f24) |
| F25 | P2 | in tree | 5 | [F25](#f25) |
| F26 | P2 | open | 7 | — |
| F27 | P3 | closed (a6cb86e) | 1 | [F27](#f27) |
| F28 | P1 | open | 7 | — |
| F29 | P3 | open | 7 | — |
| F30 | P2 | open | 7 | — |
| F31 | P3 | in tree | 6 | [F31](#f31) |
| F32 | P1 | open | 7 | — |
| F33 | P2 | open | 4 | — |
| F34 | P2 | open | 4 | — |
| F35 | P1 | in tree | 3 | [F35](#f35) |
| F36 | P1 | in tree | 9 | [F36](#f38-and-f36) |
| F37 | P1 | in tree | 8 | [F37](#f37) |
| F38 | P2 | in tree | 8 | [F38](#f38-and-f36) |
| F39 | P2 | open | 12 | — |
| F40 | P2 | open | 10 | — |
| F41 | P2 | in tree | 10 | [F41](#f41) |
| F42 | P1 | in tree (a peer's frames in order open) | 9 | [F42](#f42) |
| F43 | P2 | in tree (the leader's read bound open) | 10 | [F43](#f43) |
| F44 | P2 | open | 10 | — |
| F45 | P2 | in tree | 10 | [F45](#f45) |
| F46 | P1 | in tree | 8 | [F46](#f46) |
| F47 | P2 | open | 11 | — |
| F48 | P1 | in tree | 9 | [F48](#f48) |
| F49 | P1 | in tree | 9 | [F49](#f49) |
| F50 | P2 | open | 11 | — |
| F51 | P2 | open | 11 | — |
| F52 | P2 | in tree (encode) | 11 | [F52](#f52) |
| F53 | P2 | open | 10 | — |
| F54 | P2 | in tree | 12 | [F54](#f54) |
| F55 | P1 | in tree | 13 | [F55](#f55) |
| F56 | P1 | in tree | 13 | [F56](#f56) |
| F57 | P1 | in tree | 14 | [F57](#f57) |
| F58 | P2 | in tree | 14 | [F58](#f58) |
| F59 | P2 | in tree | 15 | [F59](#f59) |
| F60 | P2 | in tree | 15 | [F60](#f60) |
| F61 | P2 | in tree | 15 | [F61](#f61) |
| F62 | P1 | in tree | 15 | [F62](#f62) |
| F63 | P2 | in tree | 13 | [F63](#f63) |
| F64 | P2 | in tree | 15 | [F64](#f64) |
| F65 | P2 | in tree | 15 | [F65](#f65) |

Batches follow the audit's §13 order: 1 failure handling and recovery (F01, F02); 2
sustainable participant operation (F04–F06, F09); 3 admission and authorization (F03,
F20, F35, then F15, F16); 4 native reads and historical proof (F07, F08, F10, F11, F33,
F34); 5 continuous use and fault-domain guarantees (F12, F13, F22, F24, F25); 6 physical
amplification and critical paths (F14, F17–F19, F23, F31); 7 the global architecture and
its qualification (F21, F26, F28, F32, F30), then the entry documentation and gates
(F27 — done first, since it governs every other batch's proof — and F29).

The audit's extension (§14, F36–F54, added 2026-09-29 after the first batch) follows
its §18 order: 8 the reproduced cross-request failure and the same-budget restart
refusal (F37, F38, F46); 9 legitimate poor-path progress and healthy-majority isolation
(F36, F42, F48, F49); 10 byte-fair replication, proof batching, completion wakes,
freshness and scalar views (F40, F41, F43, F44, F45, F53); 11 checkpoint and transfer
amplification (F47, F50, F51, F52); 12 the congestion signal, the controller verdict
and the instrumentation's honesty (F39, F54, §17.1), with §17.2's production-limit
matrix folded into F28's qualification campaign.

### The continuation (§19, F55–F65) and what it says of the closed findings

The audit's continuation (2026-09-29, after `fc405c6`) adds eleven findings and reviews
the concurrent fixes. Batches: **13** the replicated read path and Raft ingress (F55 a
follower's read barrier ahead of its log is lag, not corruption; F56 participant
waiters starve the Raft acknowledgments that would complete them; F63 identical read
contexts from distinct origins lose one reader), first with F01/F04–F06; **14** one
admitted-history/recovery/funding contract (F57 the recovery work envelope smaller
than admitted history; F58 completion-funded restore meeting ordinary-only
constructors) with F02 and F46; **15** continuous operation (F62 expired consumers
keep their admission slots — with the journal, enrollment and founder lifecycle
ceilings; F59 duplicate verified chunks charged to the disk estimate; F60 uncoalesced
participant dials; F61 cursor renewals that copy the registry and idle polls that
commit; F64 unjittered retry waves; F65 sequential diagnostics stalling metrics).

Of the fixes it reviewed (§19.1) it leaves open, recorded under their findings below:
F19's cached plans are not charged to a `MemoryBudget` and one mutex is held through
the chunk reads; F52's WAL record encoder still zeroes (R6) and the generic
`encode_payload` grows `postcard::to_extend`'s vector before the mismatch check when a
serializer's size changes between passes (a fallible, capacity-limited append sink);
F54's report header still says "bytes copied" for preserved length and the allocation
record's §6 (lines 382–386) still converts faults to bytes and claims agreement with
RSS — to be removed. F25 and F31 it credits as addressed, within their stated scope.

## F01

**Cause.** A lost-peer report (C3, 2026-09-29) reaching a core fenced by a write it
still persisted was propagated by the owner as the period's error, which stops the
session; the queue promised one report per member per period without keeping it.

**Fix.** Both owners gather the driver's reports each period into a bounded,
deduplicated set (`lost_peers`, at most `LOST_PEERS` = the members a configuration
names), coalescing a peer already held and dropping peers beyond the bound, both
counted (`peer_reports_coalesced`, `peer_reports_dropped` in `ReplicaProgress` and
`ControlProgress`, metrics `focal_session_peer_reports_{coalesced,dropped}_total` and
`focal_root_peers_unreachable_total`, `focal_root_peer_reports_{coalesced,dropped}_total`),
then tell the core each held peer while it can be told: `PersistencePending`,
`ConsensusError::Capacity`, `LedgerError::Capacity` and `Memory` (the session owner) and
`ControlReplica::checkpoint_retryable` (the root owner) leave the rest for the next
period, the peers keeping their place; only a failure of the session itself ends the
owner. The report is drained before the core is polled, bounded per slice.

**Tests.** `fleet::managed_support_owner::tests::a_lost_peer_reported_while_a_write_persists_is_told_next_period`
(the WAL paused under a real pending write), `…::lost_peers_are_held_each_once_and_told_when_the_core_can_be`
(the bound filled under the fence, a duplicate coalesced, one beyond dropped, all told
after), `control_host::snapshot_tests::the_root_owner_holds_lost_peers_each_once_and_tells_the_core`,
`placement_agent::tests::a_dead_voter_is_drained_and_replaced_without_waiting_it_out`
(the founder stays live), `evidence_quic` cold-leader failover. Open in this finding:
the interleavings the audit lists beyond the Ready fence (LightReady, checkpoint and
decoder-floor writes, membership changes, memory pressure) are covered by the
classification, not each by a test; a journey harness that fails on an owner-stop
diagnostic is batch 7's F26/F28 work.

## F02

**Cause.** `Core::retire_native_family` (`crates/focal-core/src/native/retirement.rs`)
incremented `meta.outcomes` and published a new prefix without enforcing
`limits.outcomes`, while checkpoint recovery (`record_codec/read_validate.rs`,
`Counts::check`) requires the restored count at or under the bound and equal to the
prefix; nothing consulted the completion book's promised slots (`check_slots`, run
for every candidate and at every owner rebuild). A retirement at the bound made a
checkpoint the same configuration refused (`Contract(Capacity)`); one that fit the
bound but took a promised outcome made every owner rebuild fail (`promote` at the
readiness barrier: `NativeOwner::new` → `Capacity`, retried without end; in the
hosted session the retained delivery re-applied the entry and `Corrupt` failed it
closed). `limits.outcomes` is per node and was committed nowhere.

**Fix.** Three checks and a committed bound. `Core::retirement_family` refuses the
family before a row is walked when `outcomes + 1 > limits.outcomes`
(`RetirementRefusal::OutcomeCapacity`, named before every other refusal);
`retire_native_family` guards `meta.outcomes == native_sequence()` (a contradiction
is `InvalidManifest`) and the bound (`Capacity("outcomes")`) before the increment —
the last fence. `NativeOwner::check_retirement` (modelled on `check_layout_change`:
nothing pending, not faulted) asks `book.check_slots` with the meta row one outcome
and the prefix one sequence ahead, so a retirement never takes an outcome a live
report was admitted against; the session names the refusal
`RetirementRefusal::OutcomesReserved`. `NativeEngine::propose_retirement` runs the
gates, derives the family, runs the reservation check, then encodes, so nothing is
proposed or fenced on a refusal; `NativeEngine::check_retirement` /
`Session::native_check_retirement` ask the same short of the family, and the archive
agent (`fleet_range.rs::archive_family`) asks it before sealing a bundle. The
retirement record is version 2 (`FOCALRT1`, 154 bytes, `outcome_limit: u64`, digest
domain `.v2`); `write_into` refuses a record without a bound or whose prefix the bound
does not hold one past, `decode` refuses the same and still reads version 1 (146
bytes, `.v1` domain, `outcome_limit: None`). At application (`apply_retirement`) a
replica whose own bound cannot hold the retirement's outcome fails closed with both
bounds named (`NativeSessionError::OutcomeBound { committed, local }`, class
`FailClosed`) when the record carries a bound; a version-1 record that does not fit
is inert and counted (`retirements_inert`, exposed by `NativeSession` and
`Session::native_retirements_inert`), as every inert record now is. Doc 26 §4
carries the rule.

**Tests.** `focal-core` `native::retirement_tests`:
`a_retirement_that_fits_the_outcome_bound_restores_under_it` (bound 4, three
outcomes: prefix = outcomes = 4, restore and `with_record_buffers` under the same
limits, exact retry `Existing`, fresh request `Capacity("outcomes")`),
`a_retirement_at_the_outcome_bound_is_refused_before_anything_changes` (bound 3:
`OutcomeCapacity` at derivation, `Capacity("outcomes")` at publication, sequence,
stats and budget unchanged), `a_retirement_past_the_bound_is_what_both_checks_refuse`
(retired at 4, restored at 3 → `Contract(Capacity)`, rebuilt at 3 → `Capacity`),
`families_retire_while_outcomes_remain_and_no_further` (two spare outcomes: two of
three families), `a_retirement_never_takes_an_outcome_promised_to_a_live_report`
(at the smallest bound an owner rebuilds under, the core check passes, the owner
refuses, retiring regardless makes `NativeOwner::new` fail — the pre-fix deadlock;
one higher, allowed, and the promised report and a deadline control admit).
`focal-ledger`: `native_session::retirement::tests::a_record_carries_the_bound_its_retirement_fits`,
`…::a_version_one_record_decodes_as_it_was_written` (golden bytes),
`native_session::tests::a_retirement_at_the_outcome_bound_is_refused_and_nothing_is_fenced`,
`…::a_retirement_that_fits_the_outcome_bound_reopens_under_it` (WAL-only and
checkpoint reopens), `…::a_replica_below_the_committed_outcome_bound_fails_closed_on_the_record`,
`…::a_version_one_record_applies_where_it_fits_and_is_inert_where_it_does_not`,
`native_session::cluster_tests::a_cluster_at_the_outcome_bound_retires_and_a_lagging_follower_restores_under_it`
(the cluster harness gained `reopen_with(limits)`),
`session::native_tests::a_hosted_authority_retires_under_the_outcome_bound_and_is_refused_at_it`.
The existing suites of both crates pass.

**Decisions.** `OutcomeCapacity` is named before `OutcomesReserved`: the permanent
refusal, not the one that frees. The record carries the bound rather than the bound
becoming committed policy: the record is where the check happened, and a committed
outcome policy belongs with F12 (lifetime history as active capacity). A replica
below the committed bound fails closed, the rule 25 §4 applies to a layout a
replica's member bound cannot hold; `apply_layout` words that refusal `Corrupt`
("identity, profile or committed prefix mismatch"), and F02 names its own error with
both bounds instead of borrowing a message that would mislead. A version-1 record is
never a stop because its authority checked nothing. A checkpoint already encoded past
its bound by the unchecked retirement is still refused at restore; its repair or
migration is the operator's decision (the audit's "existing over-limit states") and
stays open here. Left latent, not hit by any test: `crates/focal-ledger/src/session.rs`
1312–1330, the hosted apply loop applies a native entry (`engine.apply_entry`, which
advances the engine's own `applied_raft`, `native_session_apply.rs` 598–599) and then,
on a leader past its readiness barrier, reconstructs the owner in line
(`engine.promote`, 1324–1327) before it advances the session's `applied_raft` and the
delivery's cursor (1328–1329); a retryable refusal of the reconstruction (the boxed
owner's budget, `native_session_apply.rs` 203–217, or a memory refusal from
`with_record_buffers`, 220–223) is retained by `drive` (1087–1096) at the applied
entry, and the resume re-applies it, which the engine refuses as `Corrupt`
(`native_session_apply.rs` 570–572) and the session fails closed (1315–1316). Before
this fix the completion book's refusal took that route; after it only a memory
refusal at reconstruction can. The cure is to advance the cursor before the
reconstruction and retry the reconstruction alone, as the standalone engine retries
it at its readiness barrier.

**Addendum (the latent bug found while closing F02).** The hosted apply loop
(`session.rs`) reconstructed the owner right after a retirement record applied,
before it advanced the delivery cursor past the entry: a reconstruction refused for
memory was retained by `drive` at an entry the engine had already applied, the resumed
delivery met it again, the engine refused it as `Corrupt` and the session failed
closed. Reproduced (`a_reconstruction_refused_after_the_record_applied_is_retried_without_reapplying_it`,
before the fix: `Native(Corrupt) after ["retry"]`). Fixed at the cause: the cursor
passes the entry before the reconstruction, and an authority whose owner still waits
to be rebuilt rebuilds it at the end of every delivery until it can — the refusal is a
`Retry`, the record applies once, the next poll is authoritative and admits work. The
refusal is induced through a test-only hook (`refuse_next_reconstruction_for_test`),
since no external pressure reaches the reconstruction alone once the entry applied.

## F24

**Cause.** The deployment plan placed the sessions' data under the requested durability
and nothing else: `Change::{CommitPolicy, PlanSession, NoChange}` and a guarantee
derived from the sessions' achieved levels alone. The root group kept the founder's
single vote (every enrolled node is admitted as a root learner by the network
controller; promotion was `cluster membership promote`, by hand, mentioned nowhere a
zone-survival request leads), the directory partition is hosted by the founder alone
(24 §13), and the issuer's key is the founder's. The KIND campaign of 2026-09-29 (D5)
saw it: three session voters across zones, `root_leader=not_leader` while the founder
was down, no placement, membership or metadata operation until it returned. The
operator asked for "survive zone, max_failures 1" and was told it was achieved.

**Fix (the root group, 2026-10-02).** The control plane is measured, planned, applied
and reported beside the data. One rule measures both: `focal_directory::
voters_tolerance` (the voter half of `effective_guarantee`, factored out) counts a
group's voters per failure domain and answers the largest f whose f fullest domains'
loss leaves a quorum, with the same blockers (missing, ineligible, dead, unknown
domain); a control voter is named by the log's configuration, without a generation. The
root's own observation (`ControlHost::observe_root`: its configuration and the authority
checkpoint) gives every node the root's voters and learners, each partition group's
grant and the issuer's holder, and `cluster placement` carries them as `control`
(`AdminControlPlane`: the root, the partition groups and the issuer, each with what it
tolerates of nodes, zones and regions and what blocks it). The deployment plan asks the
placement agent for the root's voters as it asks for a session's (`AdminCommand::
PlanControl`, `PlacementAgent::plan_control`: the voters there are when they already
tolerate the request, else `propose_placement_keeping` over the live, eligible nodes,
keeping the incumbents, under the founder's session's residency and home regions;
journaled nowhere), and composes `Change::PlanRoot { voters, expected_configuration_
index }` after the policy commit and before the sessions, with its own
`control_guarantee` before/during/after and `blocked_control` when the root cannot be
seated (apply refuses such a plan as it refuses a blocked session). Apply promotes each
planned voter through the root once the root holds it as a learner — one exact `a1:`
request each (`ClusterAdmin::membership`), a learner behind, a request decided meanwhile
or an earlier request still deciding asked again as the root moves within the operator's
allowance, the rest journaled as under way and resumed by a repeated apply; the step
is complete when every planned voter votes, and `deployment status` sees it. Preflight
fences a planned voter the directory no longer knows (`stale: root members`), never
the root's index, which moves as the controller admits learners. Readiness
(`policy_satisfied`) now also requires the root to tolerate what the committed policy
promises (`control_satisfied`, from the same measurement), and `root.peers` names
where each member's log stands as the leader knows it, what a promotion waits on.

**Tests.** `focal-directory/tests/placement_progress.rs::the_control_groups_voters_
are_measured_by_the_sessions_rule` (one, three, two sharing a region, dead, missing,
ineligible, unknown zone, a re-enrolled voter at its current grant, the sessions'
measurement unchanged). `deployment/tests.rs::the_root_group_is_planned_before_the_
sessions_and_its_promise_is_stated_apart` (order, the two guarantees, the artifact,
preflight by members not by index, satisfied and refused). Real binaries,
`tests/deployment_control_plane.rs`: three zones; before the plan the view says the
root is the founder's vote alone, as are the partition and the issuer; `deployment
plan` names `plan_root` with the three before `plan_session`, the control promise
zone/1; `deployment apply --wait 240` completes with three root voters, `cluster
placement` measures the root at zone/1 and node/1, the partition still at 0,
readiness holds the policy and lists two peers; the same plan applied again is
exact; the founder's zone silenced (SIGSTOP), the root is led from another zone and
answers the operator on host-b with its three voters; the founder returns and the
policy holds again. The KIND campaign's D5 is this test's premise.

**Found on the way: a control read was served by the leader alone.** The first root
with three voters showed it: a founder restarted under its committed policy never
reported `Ready` — its startup asks its own root for the membership, a quorum read,
and its root now followed a host — and `cluster membership show` on any non-leader
node was refused `not_leader`; the F24 fleet's session plan stalled the same way once
root leadership had moved, the agent's root reads refused. The core forwards a
follower's read index to its leader and answers with the leader's commit (27 §5),
and the control replica refused to ask (`check_ready` demanded leadership of every
read). `ControlReplica::read_index` now asks through the leader a follower knows,
and the control host serves the read once the replica has applied the index the
barrier named; a write or a turn still needs the leader, a read no longer. Test:
`control_host::a_follower_answers_a_read_through_its_leader` (a membership read on a
follower of the rig answers with the follower's node and the leader's). The paced
turn test's rig is given forty ticks of silence before an election, since its holds
hold heartbeats too.

**Found on the way: the partition permit demanded that the root lead.** The
same restart showed the second assumption: the permit the founder's own partition
opens with was refused unless the root replica led — at its admission
(`Owner::prepare_directory`), at its barrier (`complete_directory`) and in its
minting (`authorize_first_directory`) — so a founder whose root followed another
voter never reopened the partition it hosts — no `Ready`, no directory view (`cluster placement` empty), the agent's root
intents refused for want of a route. The permit is minted on committed facts behind
a quorum barrier — asked through the leader where the replica follows — the same on
every member that applied them; it no longer asks who leads. The CLI deployment
test's founder restart under a three-voter root is the regression (`cli_deployment`,
line 591 of its journey), beside the control-plane test.

**Fix (the partition groups, batch 2, 2026-10-02).** A directory partition group is
seated like the root, by the same request, and hosted wherever the root's grant seats
a node. *Who may host.* `PartitionPlan` names its host beside its founder
(`hosted_by`); the permit (`authorize_first_directory`) admits the host that holds a
seat in the group's grant — a voter's or a learner's, at or below its generation —
instead of the founder's single seat, and `validate_destination` accepts a replica
whose configuration names its host or is still the founder's alone (a learner opened
before it applied the change that seated it). The directory startup runs on every
node (`DirectoryStartup::new(node, ..)`): the founder hosts the first partition from
the start, a member hosts what it is asked to and recorded (`HostRequest::Host { plan,
image: None }`, hosted-partition record schema 2 naming the host; schema 1 records
load as the founder's own destinations), and opens its replica with `NodeConfig::
joining(host, .., [founder])` to catch up from the founder's log. *Who asks.* The
placement agent, each pass: a node the root's grant seats in the first partition's
group and that does not host it asks to (`host_seated_partition`); a replica that
follows submits the node's own partition intents where the group leads
(`PartitionAccess::Remote { target: leader }`), and a node that hosts nothing reaches
a voter the failure detector does not hold dead, the founder first (`partition_
target`). *How the grant follows the log.* The control replica keeps the record of
the entry that last changed its configuration (`ControlMembershipRecord`: index, term,
request hash — the entry's own digest for a change made without a request — and the
configuration; checkpoint schema 8, served by `ControlRead::MembershipRecord` and the
host's `witness_membership`), so every member attests the same committed record. On
the partition's leader the agent compares the group's applied configuration with the
grant and, when they differ, collects the installed voters' signatures over
`SessionFact::Membership { next, record }` under the directory's namespace — a member
witnesses from its own replica of the group (`prepare_partition_fact`), the root
prepares the permit as for a session's group — and intends `ChangeGroup { proof }` on
the root (`follow_partition_grant`); the root's placement-control ingress admits a
`ChangeGroup` from whichever node leads the group, the proof being its authority.
*What the operator asks.* `AdminCommand::PlanControl` plans every partition group by
the root's rule (`plan_group`: the grant's voters as incumbents, the group's applied
configuration index from this node's replica or the founder's answer; `ControlPlanned
Reply` schema 2 carries `partitions[]`, `refused` where no set of nodes seats one);
the plan composes `Change::PlanPartition { partition, group, voters, expected_
configuration_index }` after `plan_root` and before the sessions (plan schema 3;
`ObservedControl.partitions`; the control guarantee is the weakest of the root's and
every partition group's; a refused partition is `blocked_control` naming it); apply
admits each planned voter the group does not hold as a learner — no controller admits
partition learners — then promotes it once it hosts a replica and has caught up, one
exact `p1:` request each through `AdminCommand::Partition` (`PartitionAdminCommand::
{Configuration, Change}` → the hosted partition's control host where this node leads
it; `ClusterAdmin::partition_change`, journaled under `PARTITION.admin` with one
retry window per group), asked again on `not_ready`, `compare_failed`, `pending` or
`unavailable` within `--wait`; preflight fences a partition whose group changed or a
planned voter the directory no longer knows. `cluster placement` reports each
partition group's leader and applied configuration index from the replicas this node
hosts (0 where it hosts none), and readiness `control_satisfied` holds the committed
policy to every control group.

**Found on the way (batch 2).** Three refusals the first three-zone run met, each
silent until the node's health said so. The root prepared a membership permit for
a session's group only (`placement_proof::prepare_membership_proof` matched
`GroupScope::Session`), so a partition group's grant never followed its log; the
scope check admits both now, the conditions being the same, and
`placement_proof_tests::a_partition_groups_membership_permit_is_prepared_for_its_
voter` guards it. The directory handle counted the first partition as hosted on
every node (`is_hosted`, `host_of`, `host_of_group` answered from the handle's plan,
which is the first partition's everywhere), so a seated member never asked to host
it and would not have found its replica once opened; the first partition is hosted
where a host exists, falling back to the hosted map. And every replica opened
through the founder's single-voter bootstrap (`permit.open`: a campaign, a barrier
held alone, the activation and the authority installed as leader), which neither a
member's replica nor the founder reopening a group that grew past it may do — the
founder's restart under three partition voters died of it (`directory egress
ended`), the control plane's own survival test. A group with other members opens by
recovery alone: the activation and every install are in the log, whichever replica
leads refreshes the root's authority (the owner answers `unavailable` where it
follows, and the refresh waits), and a member's replica also watches its seat in the
root's grant (`watch_seat`), stopping when the seat is gone. Then the founder, restarted into a
partition it followed, reported Ready only once it led it (the startup wait asked
`leader == node` of the partition; readiness never requires leadership, F25, and asks
a known leader now) and, following, asked the group's identity of the founder — itself,
to which it has no route — on every pass, so it observed nothing and `deployment
explain` saw no directory (`remote_identity` dials the node the access reaches the
group at); and once it reached it, its partition intent journal, named by its local
client while it led, refused to open under the principal it submits as when it
follows (`placement intent journal is inconsistent with this node`). A node names its
partition intents by its local client where it leads the partition and by its
enrolled principal where it follows, so the journal keeps a retry window per client
(schema 2; a journal with nothing pending adopts the client it is opened with and
resumes that client's window where it stood, never crossing the owner's retry floor),
and the root's and the partitions' ingress admit a node's local client beside its
principal and its root-intent client — all three bound to the node — so an intent
journaled while it led still resolves through the node that leads now. Node health now lists
the partitions a node hosts (leader, term, applied index) and those it was asked to
host and has not opened, with their permit refusals (`partitions`, `partitions_
pending`), so none of these is silent again; a progress named the founder as a hosted
replica's node, and names its host. The admin's partition write may outlive the
admin socket's answer while the partition host takes its turn: `partition_change`
re-drives a pending request under its exact identity before taking a new one, and
apply retries `outcome_unknown` within its allowance.

**What the push's CI found (1ee8f0a, 2026-10-02).** macOS: the CLI journey's apply,
which now seats the root, the partition group (two learners hosted, caught up and
promoted) and then the session, ran out of its 180 s allowance with the session step
committed and not yet complete — the allowance covered one control group's seating
before and covers two now; it is 300 s, as the work it watches grew, and the test's
failure prints the apply's steps so the next slow step is named. Windows: the F38
unread-stream test bounded the stalled exchange by a fixed eighth of the old wait
(6.4 s); the Windows runner's loopback round trip took it 8.6 s, which is what the
carriage's residency at that round trip gives; the bound is stated in the path's
terms now, as its lossy variant already was (`residency(STALLED + 2 × UNREAD_OTHER,
longest) + a period`, and that bound far under the old wait). Linux: the drained
leader's heal again (the item below).

**Tests (batch 2).** `focal-control/tests/membership_control.rs` (the record after
an admission, the same on a follower, carried by the snapshot a learner catches up
by, after a promotion, and on both replicas after a restart);
`control_host::membership_requires_runtime_and_returns_only_committed_configuration_
receipts` (the record read equals the receipt); `directory_bootstrap::a_host_without_
a_seat_in_the_partitions_group_is_refused_a_replica_of_it`; `network_directory::
hosted_records_of_both_schemas_load_and_name_their_host`; `deployment/tests.rs::the_
partition_groups_are_planned_after_the_root_and_weaken_the_control_promise` (order,
the weakest-of rule, the artifact, preflight by group and members, a refused
partition blocks and is named); `cli_deployment` (four changes, four steps, the
partition's voters and node/1 after apply); real binaries, `deployment_control_plane`
extended: `plan_partition` names the three after `plan_root`; after apply the
partition group's three voters tolerate zone/1; with the founder's zone silenced the
partition is led from another zone and `cluster sessions create` succeeds on host-b.
Measurements: see doc 09's entry.

**Open after batch 2.** A split destination's replica on another node: its genesis
is the hash of the sealed image it was founded on, which only its founder holds; a
seated member of such a group needs the image carried to it (the Raft snapshot path
carries the current state, not the genesis) — `host_seated_partition` hosts the first
partition's group only, and a split's groups stay the founder's until then. A
partition membership change is submitted to the node the operator runs apply on,
which must lead the group (the founder by default); a leader elsewhere refuses
`not_leader` and the apply reports it. No hand verb drives a partition group's
membership yet (the deployment's apply does); `cluster nodes remove` does not yet
take a removed node out of the partition groups it votes in. The issuer's survival
is F13 stage 3 (3a held, 3b in progress); reported as the founder's alone until
then. The founder's Kubernetes disruption budget (`maxUnavailable: 0`) stands until
both.

## F27

**Cause.** The gate denied the policy lints at a level a local `#[allow]` overrides,
and the measurement binary `tools/load` carried a crate-wide allowance of all of them.

**Fix.** `tools/load` handles its errors (typed `LoadError`, `RunError`, `FrameError`;
exit code 2; checked arithmetic; integer nearest-rank percentiles; no indexing) and the
allowance is gone. `scripts/check_production_policy.py` refuses any production source
that carries an `allow` of a policy lint — an allowance counts as production unless it
is `cfg_attr(test, ..)`, inside a `#[cfg(test)]` item, or in a file only a `#[cfg(test)]`
module includes, or under `tests/`, `benches/`, `examples/` — and proves itself first
on `scripts/fixtures/production-policy/{negative,positive}.rs`; `check-production.sh`
runs it before Clippy. A `forbid` level was tried and is not usable: derive macros
(clap's) emit `allow(clippy::style, clippy::restriction)` on their output, which
`forbid` refuses (E0453). CLAUDE.md §1 states the two-part gate.

**Tests.** The fixture self-test (the negative fixture must be refused, the positive
passed) runs on every gate; 1,061 production sources scanned.

## F54

**Cause.** The counting allocator (`crates/focal-memory/benches/support/alloc_count.rs`,
bench-only) added `min(old, new)` to "bytes moved" on every reallocation, in place or
not; its `bytes/op` was requested bytes read as if live; phase peaks were read as
per-operation; and the allocation record converted page faults at 4 KiB on a 16 KiB
host.

**Fix.** Reallocations are counted as copies only when the allocator returned another
block (`realloc_moved`, `realloc_moved_bytes`), in-place growth apart
(`realloc_in_place`); the report's columns are `moved/op` and `requested/op` with the
header saying what they are; the module comment states the meaning and the limits of
every figure (perturbation, phase-wide peaks, page size, faults as events). The
allocation record gains a dated corrections section (§6a) and the performance record
states the page size.

**Tests.** The benches that include the allocator (`focal-memory`, `-wire`, `-log`,
`-core`, `-raft`, `focal-load`) build under `--benches` clippy; the counters are
exercised by every `allocs` bench run.

## F31

**Cause.** `Arena::add_page` rebuilt the page directory at length n+1 on every page,
moving every descriptor (O(P²)) and allocating each time.

**Fix.** The directory grows to the power of two of its length, charged for its
whole capacity before anything changes; a directory with room takes a page without
an allocation or a charge. Failure atomicity, peak accounting, stable generational
handles and non-reuse are untouched.

**Tests.** `arena::tests::the_page_directory_grows_by_doubling_and_is_charged_for_its_capacity`
(a thousand pages, eleven reallocations, the root charge covering the capacity); the
generation-exhaustion test unchanged.

## F52

**Cause.** The wire encoder and the WAL's per-record encode reserved the measured size,
zeroed the whole buffer, then overwrote it.

**Fix (wire, in tree).** `encode_payload` appends into its reserved buffer
(`postcard::to_extend`); an encoding that does not match its measured size is an
invalid frame, never a grown buffer. **WAL:** R6 (one buffer per batch, written by
appending). **Receive side:** stays initialized until F03's funded arriving-bytes
reader, which allocates as bytes arrive. Fence serialization and its path strings are
R2's concern.

**Tests.** `an_encode_writes_its_reserved_buffer_once_and_exactly` (bytes identical to a
whole-vector serialization; capacity exact; the limit refusal unchanged).

## F19

**Cause.** `ContentStore::read_range` loaded, hashed, decoded and validated the whole
manifest and scanned chunks from the head on every page.

**Fix.** A validated chunk plan per sealed object (`Plan`, held in a bounded LRU of as
many plans as the store admits uploads, keyed by the object's immutable digest) with
each chunk's start; the first chunk of a range is found by search; a reference with
the right root and the wrong class or length is refused; every delivered chunk is
still read whole and hashed.

**Tests.** `store::tests::paged_reads_load_the_plan_once_and_hold_a_bounded_number_of_plans`;
the existing boundary and verification tests unchanged.

## F25

**Cause.** Readiness was wired to the identity-only `alive` probe.

**Fix.** `AdminReadiness.serving` (the root's and every installed session's owner
running, no quorum asked); `cluster node probe --check serving`; the renderer's and
the chart's readiness probes ask it while startup and liveness keep `alive`.

**Tests.** `network_service::tests::a_host_stays_alive_and_its_readiness_stays_bounded_while_the_root_leader_is_down`
(serving with the root leader down), `…::a_node_whose_session_owner_stopped_is_alive_and_not_serving`,
the render goldens and `tests/deployment_kubernetes.rs`.

## F07

**Cause.** The declaration's evaluation page walked the claim's registrations in
registration order — submission order, not key order — and compared its cursor as a
key; and it set the continuation to the row it had not shown once the page was full.
Two comparisons that disagreed about what "after" meant.

**Fix.** The core scans one declaration's evaluations in key order
(`Core::native_declaration_evaluations_from(claim, validation, after)`, an exclusive
resume after any key, a cursor of another declaration or claim restarting at the
declaration's first key); the node page judges fullness before consuming a row and
names the last consumed key as `next`; the wire requires `claim` on the query (the
rows' affinity, 25 §6), an exact prefix for any resumed page, and validates the
page's shape (strictly increasing keys of that declaration after the cursor, a
continuation never behind the last row nor the cursor sent). The claim expansion got
the same treatment: an ordered expansion (responses from the latest cycle back, then
evaluations in key order), a continuation where it fills, a resumed page without the
claim. The responses list passes the rows above a resumed cursor uncharged, so a page
of one row and one visit progresses.

**Tests.** `focal-core::native::increment_scan_tests` (registration order ≠ key order,
resume after every key and after keys no row has),
`focal-node::native_reads_tests::evaluation_pages_of_every_size_concatenate_to_the_whole_span_at_one_prefix`
(272 evaluations; sizes 1–273 incl. exact boundaries; refusals),
`…::a_claim_expansion_continues_where_its_page_filled_and_never_repeats_the_claim`,
`…::the_responses_list_reaches_the_end_of_a_long_chain_one_row_per_page`,
`focal-wire::native::tests::native_pages_are_validated_against_the_shape_their_query_names`.

## F08

**Cause.** The client requested one page of 256 evaluations, ignored its
continuation, and selected the "current" evaluation among the objects it had; the
validation context did the same. The core allows 4096 evaluations per claim.

**Fix.** The owner selects (`NativeReadQuery::SelectEvaluation(NativeSelectionQuery
{claim, validation, selector, generation, live})`): over the declaration's whole span
the targets `NativeEvaluationSelector::selects` names (the one copy of the rule the
compiler, the context read and the owner share), at the named generation when there
is one, live when asked, the tie set at the highest generation. One object is the
current evaluation, several are an ambiguity the caller narrows (`validation.begin`
without `--target` under several current increments is refused, never guessed), a
set that would not fit the page is `Capacity`, never cut. `validation.begin`/`report`
resolve through it (`Requirement::Evaluation`), `validation.context` selects through
it (`live: false`, `generation` when named), and `validation.get` follows the
declaration's pages at exactly the first page's prefix, bounded by the core's
evaluations per claim over a page (`EVALUATION_PAGES` = 16 × 256 = 4096), an endless
span a `Capacity` refusal. **Decision:** the read query's registered encoding changed
in place (`Claim` gained `after`, `Evaluations` gained `claim`, `SelectEvaluation`
appended) and the frozen fixture was re-registered, recorded in doc 19: no release
carries the native profile, so no peer speaks the previous shape.

**Tests.**
`focal-node::native_reads_tests::the_owner_selects_the_current_evaluation_over_the_whole_span_and_the_client_binds_it`
(the selected evaluation at position 264 of 272; the tie set of 16; `Capacity` at
15; a named generation; empty selectors; the compiled `BeginIncrement`; the
ambiguity; the context read), `…::validation_get_follows_the_evaluation_pages_to_the_end`
(three requests: the definition, a page at least at its prefix, the rest exactly
there), `focal-native-client::tests::validation_get_follows_pages_at_one_prefix_and_refuses_a_span_beyond_its_bound`,
the wire shape test above.
## F09

**Cause.** Discovery (`schema list|example|validate --shape-only`, name completion) used
the V1 registry and decoder regardless of the ledger's engine; there was no native
example generator; discovery's tests validated examples against the registry that
produced them.

**Fix.** One engine selector for every surface (`operations::engine`: offline `--native`
→ the only catalogue with the name → V1; online the shared standing probe, contradiction
refused, assumption reported); native examples for all 43 descriptors generated from the
native contracts; `--native` on `list`, `get`, `example`, `validate`; engine and version
in `schema list`; context-backed validation compiles against the ledger's bindings with
throwaway identities; the MCP catalogue served by the same engine; README and manual
carry the native quickstart and the rule.

**Tests.** `discovery_tests` (both engines' examples decode and validate through their own
engine; refusals by name), `engine_tests`, `cli/tests.rs`
(`every_authored_descriptor_example_loads_through_its_command_with_the_same_intent`),
`native_stdio_tests::the_generated_native_example_commits_through_the_adapter_unchanged`,
`command_tree_tests::documented_cli_commands_and_their_flags_resolve_in_the_command_tree`,
`tests/cli_discovery.rs::the_native_catalogue_examples_and_validation_need_no_state_either`,
`tests/cli_native_quickstart.rs::the_documented_native_quickstart_runs_verbatim`.

## F55

**Cause.** The hosted `Session`'s delivery treated a read barrier whose index lay
above its applied index as corruption; the leader's `MsgReadIndexResp` can precede
the append that carries that index (separate QUIC streams and exchanges), so a
lagging follower failed its session on a legitimate read. The standalone engine had
been given parking for exactly this (D4); the hosted path had not.

**Fix.** The hosted session parks such barriers (`parked_reads`, bounded by
`DurableNode::pending_reads`, charged once for the bound), lets the delivery go on so
the entries can arrive, and answers parked barriers in order at the first delivery
whose applied index reaches them, removing each only once answered (a retryable
refusal loses none); the delivery's native output is reserved for them too. At the
bound: `read_index`/`native_read_index` (hosted) and the engine's `read_index` refuse a
new read as `Capacity`; a barrier that still arrives is dropped and counted
(`reads_dropped`) in both the hosted loop and the engine's — the engine previously
propagated `Capacity` as a retryable refusal, retaining the delivery that carried the
entries its parked reads waited for (a stall at the bound; closed at its cause).
Context identity, the readiness barrier, native correlated reads and legacy reads
complete through one `complete_read`.

**Tests.** `session::native_tests::a_follower_read_answered_ahead_of_its_log_waits_for_the_entries_and_stays_live`
(before the fix: `poll 2: Corrupt`), the standalone
`native_session::cluster_tests::a_follower_under_memory_pressure_keeps_its_delivery_and_finishes_when_memory_returns`
and the D4 follower-read tests unchanged. The bound's refusal and drop are exercised
by construction (the bound is the core's in-flight read limit); a parked set of that
size needs a cluster harness that holds hundreds of answered reads, left to the
campaign.

## F56

**Cause.** One pending queue (`pending_clients` = 128) admitted participants and
peers alike; a peer's Raft message — the heartbeat or append answer quorum progress
needs — was refused `Capacity` once participants filled it, though those participants
waited on exactly that progress.

**Fix.** Two bounds: participants keep `pending_clients`; Raft ingress is admitted
under a reserve of its own — the members the configuration names (voters, learners,
the admitted) times the in-flight window the core allows one peer
(`NodeConfig::max_inflight_messages`, exposed as `DurableNode::inflight_window` and
`Session::inflight_window`) — derived, not chosen: every member may have its whole
window outstanding at once and no more. Neither side takes the other's slots
(`pending_peers`, `pending_participants`, `peer_reserve` in `fleet.rs`; all sixteen
participant admission sites count participants only). Acknowledgment stays behind the
exact Ready fence (`WaitingFor::PeerPersistence`). Byte funding is unchanged: each
request still carries its admitted charge.

**Tests.** `fleet::list_tests::peer_admission_tests::a_full_participant_queue_still_admits_the_acknowledgments_it_waits_on`
(one-slot participant queue; before the fix the followers' answers through the
leader's authenticated ingress were `Capacity`; after, `PeerAccepted` and the waiting
read completes; the reserve equals three voters' windows). The audit's 128-client
campaign with paused WAL, cancellation and joint configurations belongs to the
KIND/nightly campaigns (F26/F28).

## F63

**Cause.** `ReadOnly::add` keyed a pending read on its context and dropped a second
asker with its origin; `answer_read` answered one origin. The native correlation
hashed principal, request id and an owner-local nonce only, so the same principal's
exact retry at two followers with aligned nonces, or at one owner across a restart,
produced one context.

**Fix.** A pending read keeps every asker in the order asked (`origins`, bounded by
`MAX_MEMBERS`, memory reserved; `into_parts`), and `confirm_reads` answers each (the
context copied for all but the last). Every read context an owner mints carries the
owner's node id and an incarnation drawn at its start (`focal.native.read-correlation.v2`;
the managed, summary and list contexts extended the same way), so a nonce that
restarts or aligns with another replica's never repeats a context. The etcd
`read_only` contract (unique context per round) is the reference; the origins are the
defence where a caller collides anyway.

**Tests.** `focal-raft::tests::a_read_asked_by_two_members_under_one_context_answers_both`
(two followers forward one context; one pending read; both answered at the confirmed
index), `read::tests::reads_leave_in_the_order_asked_once_one_is_confirmed` (the
second asker recorded), the D4 follower-read and the differential suites unchanged
(the harness asks under unique contexts).

## F62

**Cause.** The cursor registry removed no row: expiry and resync released a
consumer's retention obligation, admission compared the whole map against
`max_consumers`, `AdvanceFloor` left expired rows in place, and `CursorOperation` had
no retirement; distinct consumer names therefore reached a cumulative ceiling of
4096 per registry. The session's owner record (`CursorMetadata.owners`) would have
kept the old principal even if the row had gone.

**Fix.** `released(row)` = ordinary and (lease expired or in `Resync`);
`retire_released` removes every such row when a registration finds the map at its
bound (`admit_consumer`) — not before: a consumer that comes back reads from its row
why it must reseed (`LeaseExpired`, the resync reason), which the durable-delivery
tests hold to — and names them in `PreparedCursorUpdate::retired`, which the session uses to drop
the owner record before it charges the metadata. Generations are issued from the
registry's revision (unique for ever; `validate_checkpoint` requires
`generation ≤ revision`), so a token or renewal of a retired incarnation is refused
(`MissingConsumer`, or `WrongGeneration` once the name is taken again) and never moves
the new cursor. Protected consumers are never released. No durable format changed
(schema 1; the `generation` field's values only), so no fixture moves. Owner
metadata and receipts: owners follow the row; receipts are bounded by
`cursor_receipts` as before.

**Tests.** `focal-stream/tests/consumer_retirement.rs`:
`an_expired_consumer_returns_its_slot_and_no_stale_token_reaches_the_name_s_next_incarnation`
(bound one; before the fix the second registration was `Capacity`),
`at_the_bound_the_released_rows_leave_together_and_a_protected_one_never`,
`a_churn_of_more_names_than_the_bound_passes_through_a_bounded_registry_that_restores`
(4608 names, ≤ 4096 rows, restore); `session::cursor_tests::an_expired_consumer_s_name_is_free_for_another_principal`
(owner pruned; before, the second principal was `WrongActor`).

## F58

**Cause.** Recovery charged its meters and later stages to the completion lane but
its initial roots to the ordinary one: `RangeStore::new_partitioned` (the index and
the hydration owner), `StructuralCheckpoint::layout` and `ranges::reserve` (the
assembled group's directory; `assemble` took a lane and did not pass it on). Under
ordinary pressure a required restore could not initialize; a completion-funded pool
could not restore at all.

**Fix.** `recovery::restore_in(.., lane)` threads the lane through
`read_index::Index::build`, `layout`, `RangeStore::begin_hydration_partitioned_in`
(new, with `new_partitioned_in`), `NativeRanges::from_store` and `reserve`;
`restore` is the completion-lane form, which the checkpoint install and the import
use. Layout splits and merges charge their directories to the lane they already
took. Completion funding stays restricted (`funded_child` still refuses ordinary
work through it); old state, input, scratch and new roots are all charged to the
one budget the caller passes.

**Tests.** `native::record_codec::replay::tests::a_restore_is_funded_by_the_completion_allowance_and_never_waits_on_ordinary_credit`
(before the fix: `Memory(Capacity { requested: 224, available: 0 })` — the audit's
own figure). Cancellation and error cleanup of a refused restore are the existing
`recovery` refusal tests' concern; the typed refusal when the completion reserve
itself is too small is `Memory(Capacity)` from the same budget.

## F57

**Cause.** The recovery work allowances (`recovery::Work`) were chosen constants
(`1 << 30` each) unrelated to the admission bounds, and `HistoryIndex::build`
precharged `sort_visits(2 × events)` before it counted the population; a history the
configuration admitted (4096 claims, 69 633 rows) spent 2.86 G model and 1.17 G
lookup units and was refused at restore.

**Fix.** The allowance is derived, never chosen: `Work::for_shape(visits, bytes, rows)`
— `(PHASES + 1)` whole scans of the inspection's visits plus 32 parsing visits a body
byte; source 4096/row + 64/byte; model 65 536/row + 256/byte + `sort_visits(2 × rows)`;
lookup 65 536/row — extended, once the index has counted the artifact rows, by a
custody recovery per artifact at the largest verification any schema may declare
(`NativeVerificationBudget::ceiling().recovery_work()`, the very term `Custody::recover`
charges). The sort is charged after the population is counted. `restore_measured`
returns what was allowed and used; the standard configuration's `work` is
`Work::for_limits` at its checkpoint bounds. The per-unit ceilings are measured over the
recorded workflows at authored maxima and pinned. **Envelope, not admission check:**
admitting a state whose checkpoint would not encode (rows beyond the checkpoint's row
bound) is the checkpoint's own refusal (`EncodingLimits.rows`) and the families'
retirement (26 §4) is the lifecycle that keeps history within it; F46's WAL index
expansion is separate and open.

**Measurements.** Projection workflow (301 rows, 83 619 bytes, 170 354 visits): parsing
3.06 M (18/byte beyond 9 scans), source 494/row, lookup 15 954/row, custody 6 artifacts;
authored (27 rows): model 39 179/row, 113/byte, lookup 4503/row. 4096 claims: allowed
model 10.04 G, used 2.86 G; lookup allowed 4.56 G, used 1.17 G; parsing 545 M / 318 M;
source 976 M / 27 M; 16 s in debug.

**Tests.** `bound_tests::a_restore_s_work_stays_within_the_envelope_its_checkpoint_declares`,
`replay::tests::a_history_of_thousands_of_claims_restores_under_the_derived_envelope`
(the old constant is below the used model and lookup work),
`native_session::tests::the_standard_recovery_work_is_derived_from_the_checkpoint_bounds`,
the recovery and replay suites unchanged.

## F46

**Cause.** `RecoveryIndex::push` built one `IndexChunk` per record at startup and at a
checkpoint's rewrite (frame locations plus 256 bytes each), while appends built one per
batch; the startup scan also allocated a vector per frame and an owned record per frame.
A history that fitted its budget when written did not fit when reopened.

**Fix.** Two readings of the durable prefix at open (`Wal::open_indexed` with
`ScanEvent::{Log, Counted, Frame}`): `scan_logs` reads each record's leading field
(`LogicalLogId`, the frozen layout's first field) and counts; `pack` allocates one chunk a
log at exactly its count; `scan_indexed` places each frame; `seal` checks every count was
met (a prefix that changed between the readings is the writer's failure, never capacity).
The checkpoint rewrite packs its replacement index from the old index's counts and the
checkpoint's records. `scan_frames` lends each frame from one buffer bounded by the
largest record. The per-record `push`/`prepare` paths are gone. **Cost:** the prefix is
read twice at open (sequential, checksummed both times); the format is unchanged.

**Tests.** `an_admitted_history_reopens_within_the_budget_that_admitted_it` (the audit's
shape; the old cost, 256 × 288 bytes, exceeds the retained bytes plus the scan's transient
and would refuse), `the_packed_index_never_costs_more_than_the_appends_across_groups_batches_and_checkpoints`,
the existing startup, compaction and corruption tests unchanged.

## F59

**Cause.** `import_chunk` reserved the chunk's bytes on the volume and committed the
reservation unconditionally; `install_transferred_chunk` recognised an identical
verified chunk and only refreshed its mtime, so the committed promise lowered the
free-space estimate for bytes that were never added. The transfer's manifest on a
repeated completion and a custody record installed twice had the same shape.

**Fix.** `already_installed(path, bytes, hash)` is asked before the promise: the chunk
import, the completion's manifest install and `install_custody_record` refresh an
identical file's freshness and return without a promise (a promise asked first would
itself be refused at the watermark — the duplicate must not need one); a new or
repaired payload is promised, installed and charged as before. Nothing about freshness
(GC's marks, repair) changed; the resampling cadence is as it was.

**Tests.** `store::transfer::tests::duplicate_chunk_imports_and_a_repeated_completion_charge_the_volume_once`
(the audit's shape at an exact estimate; before the fix the duplicates consumed the
estimate and the completion was refused), the transfer, upload and record suites
unchanged.

## F60

**Cause.** `QuicTransport::connection` released the cache mutex before resolving and
connecting, so concurrent cold misses for one route each opened a connection and
overwrote one entry while keeping their own; `max_connections` bounded keys, not
connections or dials in flight; the server's per-identity limit then replaced the
earlier connections under calls already dispatched, and a request's failure removed
the key whichever connection it held by then.

**Fix.** `focal_wire::RouteConnections`, which `QuicTransport` now holds: a route's dial
runs on its own task with a `watch` of its outcome; callers arriving while it dials
join it and share its connection, waiting no longer than the dial itself (resolver,
connection and greeting under one `request_timeout` each, so the task always ends and
always speaks — or, dropped with its runtime, closes the channel, which a waiter reads
as failure); a caller that gives up under its own deadline drops only its receiver;
the first caller to see the outcome settles it into the cache, later ones take what is
cached or, when it was forgotten already, dial afresh rather than re-cache it; a caller
waits on at most two dials. Dials in flight hold their room in `max_routes` (the least
recently used cached route leaves for a new one; `Limit` when every room is a dial).
Connections carry a generation and `forget(route, generation)` removes only a matching
one. Waiters are the caller's own concurrency — the cache holds nothing per waiter — and
the connection's lanes queue them as they queue any request. **Cost:** one task and one
`watch` channel per dial in flight; the redirect path (`RouteHint`) and the node's peer
pool are untouched.

**Tests.** `focal_wire::tests::cold_calls_to_one_route_share_one_dial_and_a_stale_failure_forgets_nothing`
(24 concurrent cold calls over loopback QUIC to a cache of one: one dial, one admission,
no replacement, 24 answers; a stale generation forgets nothing and the current one lets
the route be dialed again; a caller cancelled microseconds into a dial leaves it to the
next caller, which starts no third dial); with each caller dialing alone (the mutation
before the fix) callers failed with `Connection` as their connections were replaced
under them. The wire and client suites unchanged.

## F61

**Cause.** Three copies per idle poll. `CursorRegistry::prepare` cloned the whole
checkpoint — every consumer's row and filter — for any command, a scalar renewal
included, charged it whole and validated it whole; the session's candidate cloned the
receipt map and the owner map beside it (and serialised the whole metadata for its
scratch charge); and every tail poll was a managed request that committed
`AcknowledgeAndRenew` or `Renew` whether or not anything was acknowledged, took a
receipt and a window ordinal the client later retired with a control, and the CLI
polled again 250 ms later. An idle fleet of observers therefore drove consensus, the
WAL, registry copying and client disk work with no event progress; the aggregate copy
work grew as O(C²R).

**Fix.** (1) The prepared update is a delta: at most one row, named — a patch of its
scalars for a renewal, an acknowledgment, a seed's completion or a resync (nothing
copied, not even the filter), the one row for a registration or a seed, the names it
retires — validated as that row will stand (the clock only advances and the floor
moves only under the retention limit, so no untouched row's invariant can change); the
row's bytes are admitted before it is built and joined to the registry's per-lane
charge at publication, and leaving rows return theirs. `publish` writes the row,
removes the retired names and hands them back. (2) The session's cursor candidate
carries its receipt and, for a consumer named first, its owner, charged as entries;
`apply` inserts them and retires owners with the registry's names; the metadata's
charge is the sum of its entries. The managed candidate likewise. (3) A plain poll
(`Operation::Stream`) with nothing to acknowledge — no token, or one at the row's
position — proposes nothing: it is a read answered after its barrier from the row's own
token, with no receipt and no request key; one that acknowledges what is new commits
as before, and a managed poll (a durable request whose receipt the client retires)
commits as before. The lease of a polled consumer is renewed by the node's own
maintenance entry once half of it has passed (`Session::propose_cursor_renewal`, a
`Renew` under `FOCALCM1` with no receipt, validated on every replica as due from the
committed row: what remains is at most half the term granted); renewing at the half
keeps the time between renewals and the time to recover a lost renewal equal, so an
idle consumer costs at most two entries a term and one polling more often than half a
term needs every poll in the remaining half to fail before it expires. (4) On a
replicated host a read whose page is empty parks after its barrier until the stream
line moves or the owner gives the request up (then it answers the empty page it held,
its barrier having been current), holding only its request's bytes — the page's
staging is released while it waits and taken back to pump; the one-voter driver
answers in place. (5) The watch journal (schema 3; a schema-2 record is read once more
and carried forward) sends a tail poll whose cursor is no further than the
acknowledgment its last page reported as a plain read — no managed ordinal, no
receipt, no retirement control — and a durable acknowledgment when it has consumed a
page; the CLI's follow pauses 250 ms only after an empty page. **Not changed:**
acknowledgments stay durable before retention is released; exact retry of a request
answered once is answered from its receipt; the registry's checkpoint format and the
wire are untouched.

**Measurements.** Registry, the audit's row (64 consumers × 256-claim filters):
preparing one renewal charged 7,039,128 bytes before (a copy of the registry) and 0
after; a registry restored under exactly its bytes renews. Session: what a candidate
holds while it waits for its quorum is the same at 3 receipts and at 515 (one varint of
the receipt's revision apart), where before it held a copy of the map. Host: a poll
with nothing to acknowledge leaves the cursor revision and the committed index where
they were; the renewal arrives once per half term.

**Tests.** `focal-stream/tests/renewal_cost.rs` (three), `session::tests::a_cursor_command_holds_its_entry_never_the_receipts`,
`session::tests::a_polled_lease_past_its_half_life_is_renewed_by_the_node_s_own_entry`
(due at the half, not before; no receipt; a stale generation refused; a protected
consumer never; replayed from the log),
`streams_tests::a_poll_with_nothing_to_acknowledge_is_a_read_and_the_node_renews_a_lease_past_its_half`,
`streams_tests::a_poll_with_nothing_new_parks_for_a_page_and_holds_only_its_request`,
`watch_client::idle_tail_polls_are_plain_reads_that_commit_nothing_and_take_no_ordinal`
(eight idle reads, no ordinal; the node's registry at the registration's and the
acknowledgments' revision after the client resumed once); the consumer retirement,
durable delivery, cursor, managed, stream host and CLI watch suites unchanged.

**Residual closed (2026-09-30): the node's own entries on a native ledger.** The
maintenance entry judged its command on the legacy domain sequence
(`prepare_maintenance`: `cursors.prepare(.., self.sequence())`, and the floor bound beside
it), where the client commands had been moved to the stream line (23 §6). On a native
ledger the legacy sequence stays at the sealed prefix — zero on a genesis ledger — so a
renewal of any cursor that had acknowledged a native record was refused `CursorAhead`
before it was proposed, and the poll that found the lease past its half failed
(`invalid_input` at the CLI) on every later poll until the lease ran out. Found by
restarting a node under a resumed watch forty times over (each restart a poll; the third
fell past the half); every copy failed at that poll, and none of 492 restarts after the
fix. The entry is now judged on the stream line: positions and the floor against
`stream_published()`, the entry still naming the domain sequence it was made at.
`session::native_tests::a_native_cursor_is_renewed_and_expired_by_the_nodes_own_entries_across_a_restart`
(a cursor past the legacy prefix: not renewed in its first half, renewed past it with
its position kept, its expiry advancing the clock through the log, and both entries
replayed by a restart; it fails `Stream(CursorAhead)` on the code before).

## F64

**Cause.** The participant's retry loop slept the capped exponential step whole (20, 40,
80 … 500 ms), so every caller a leader loss, a service still opening or one capacity
refusal had turned away came back at the same instant, each wave as tall as the last;
the node's peer pool rested (`retry_backoff`) and cooled down (`unreachable_cooldown`) by
fixed pauses, so peers that lost one node dialed it again in step.

**Fix.** `focal_client::client::jittered(policy, backoffs, remaining, random)`: the step
from the base, capped, spread by full jitter — a wait drawn uniformly between nothing
and the whole step — under the attempt, elapsed and refusal budgets as before (a draw
never adds an attempt; a pause never outlasts what remains of the clock). Full jitter is
the spread AWS's analysis (Brooker, 2015) found finishes the same work in the fewest
calls. `focal_wire::peers::spread(delay, random)`: the pool's retry pause and its
cooldown drawn uniformly from half the configured pause to the whole of it (equal
jitter), because half of each pause is its meaning — an unreachable peer is not dialed
again before its cooldown, a lost exchange rests before it is retried. Draws are the
operating system's (`getrandom`); when it has none to give, the pause is the whole
step, never a shorter one. Both are pure functions of the draw, so tests fix it. Exact
request identity is untouched; no retry layer, token or hint was added — the existing
budgets are the allowance and the node's `Capacity`/`Unavailable` refusals the
pressure signal.

**Measurements** (`backoff_tests::a_wave_of_refused_callers_thins_over_its_spread_and_takes_fewer_calls`,
a seeded simulation: a thousand callers refused together by a service that serves fifty
a millisecond, the default policy). The whole step: 10,500 calls in twenty waves whose
tallest is 950, drained in 7,602 ms. The spread (SplitMix64, seeds 1–3): 2,046 / 2,039 /
2,029 calls, tallest wave 109 / 89 / 82, drained in 69 / 71 / 83 ms.

**Tests.** `backoff_tests::a_step_is_spread_over_itself_and_never_past_the_budget` (0 →
nothing, the largest draw → the step, the middle → half; what remains of the clock caps
it), the simulation above, `focal_wire::tests::a_peer_pause_is_spread_over_its_second_half`;
the client and pool suites unchanged (the cooldown tests hold: a send within half the
cooldown fails fast, and the peer is dialed again after the whole of it).

## F65

**Cause.** `sample_metrics` asked each hosted replica's owner for its diagnostics in turn
and awaited each without a deadline, so one busy or stuck owner delayed every later
session and the whole snapshot, the previous one staying published; an owner that
refused or had gone was skipped in silence; the sampler slept the cadence after a
collection of any length, so the nominal five seconds stretched by the collection. The
loopback rendered the complete snapshot before reading the request — outside the
two-second bound, for a `404` or `405` too — and served one connection at a time, so a
slow scraper held every other for up to two seconds.

**Fix.** `metrics::collect(asks, deadline)`: every session asked at once (bounded by
`MAX_SESSIONS`, each ask by the 64 KiB it reserves at its owner), each answer taken as
it comes, the round closed at the cadence (`SAMPLE_INTERVAL`); an ask not answered by
then is dropped with the set. A session without an answer is `observed: false`, counted
in `sessions_unobserved`, listed with what the node knows without the owner (leader,
term, the owner's pace and periods, the directory's epochs) and rendered
`focal_session_observed 0` with the owner-side series absent — never zero read as
health; `collection_ms` says how long the round took. The sampler starts each round on
the cadence (`sleep_until`), whatever the last one took. `MetricsPage` renders the text
once, when the snapshot is published; the admin socket and the loopback serve it as it
is. `serve_loopback` reads and judges the request first, under the bound, then writes;
connections are served on tasks of their own, as many at once as the admin socket admits
operators (`admin_wire_limits().max_connections`, eight) and one beyond closed
unanswered, as the socket does. Readiness (`probe`) never touched the sampler and
still does not.

**Tests.** `metrics::tests::an_unobserved_session_says_so_and_carries_no_owner_side_numbers`,
`metrics::tests::a_round_closes_at_its_deadline_with_the_late_unobserved` (an instant
answer, one ten seconds late, one refused, one ten milliseconds late, a hundred
millisecond round: `[Some, None, None, Some]`, closed at the deadline),
`metrics::tests::a_silent_scrape_delays_no_other_and_the_text_is_the_page_s` (a scrape
that never speaks holds its connection while three others are answered — `200` with
the page's text, `404`, `405` — within a second, not behind its two-second bound), the
CLI metrics test unchanged.

## F04

**Cause.** The native journal's capacity counted catalogue entries and nothing ever left
the catalogue: `record_delivered` marked a journal delivered and kept the entry, its
frame and its reservation, so the default 256 was a cumulative-use cutoff — the audit's
one-slot probe: `outstanding()` 0, usage 1, the next unrelated prepare `Capacity`.

**Fix.** A reported operation is retirable: `record_delivered` (a committed receipt
reported, or a closed refusal acknowledged) and `record_refusal` for a closed refusal
(every kind but `Capacity`, which admitted nothing, keeps the frame for the exact retry
and is never retirable) mark the entry with the order it was reported in; the operation
stays answered from its journal. A claim that finds the journal full retires the
operation reported longest ago: the identity moves from the live entries to the
catalogue's `retired` table — with the intent it was bound to and how it ended
(`NativeRetired::Committed { receipt digest, sequence }` or `Refused`) — durably first,
then its directory (frame and journal) leaves. Retired identities are bounded to
`max_operations`, the oldest leaving first, and are never another operation: `prepare`
with a retired identity under any intent, `retry` and `record_*` answer `Retired`; a
generated identity that is retired or live is passed over. The catalogue is schema 2
(schema 1 is read once more and carried forward). Live storage is therefore bounded by
the capacity and never by the work ever done; the exact-result contract is kept by the
journal while it lasts, by the retired outcome after, and by the owner, which answers
an exact retry from its receipt.

**Tests.** `native_store::tests::a_reported_operation_retires_when_a_claim_needs_its_slot_and_its_identity_stays_taken`
(five operations through a journal of two, each reported: the first three retire in
order as the claims need their slots, the retired table holds two with the oldest
gone, the last two are still answered; a retired identity under the same and another
intent is `Retired`; a generated identity that is retired or live is passed over;
reopening keeps it all), the adapted
`prepare_claims_an_identity_once_then_retry_returns_the_exact_frame_and_binds_receipts`
(a reported receipt and a closed refusal stay answered and are not outstanding; a
capacity refusal stays for the exact retry, a later commit is recorded, and it is no
result to acknowledge), the CLI and MCP native suites unchanged (`cli_native_a4`
retries committed work after it was printed).

## F05

**Cause.** `prepare` durably claimed the identity (`ready = false`) before expansion; a
failed resolution, read or compilation left the claim, invisible to `outstanding()`
(which lists ready operations) and permanent — the audit's probe: an error, zero
outstanding, usage one, nothing sent.

**Fix.** A failed expansion releases its claim with the failure, under the same hold of
the lock the claim was made under: nothing durable named the identity beyond the claim
and no bytes ever left. A claim that never became ready (a crash after the claim, or
after the frame but before ready) is swept when the store is next opened, together
with any directory of an identity the catalogue no longer lists (a retirement
interrupted before its directory left): under the lock a claim seen not ready is not
being prepared, since preparation holds the lock throughout; a reported operation is
never swept, only retired when a claim needs its slot. The crash boundaries
(claimed, prepared, ready, retired) are the injected faults of the tests; a frame
escapes only after durable ready, as before.

**Tests.** `native_store::tests::a_failed_or_interrupted_claim_holds_no_slot` (a failed
expansion releases the slot; crashes after the claim and after the frame are swept at
open; a retirement interrupted before the removal is completed at open and the identity
stays retired), `interrupted_initialization_resumes_without_minting_a_second_identity`
unchanged (the same handle resumes a claim under the same intent).

## F06

**Cause.** `record_reply`, `record_delivered` and `record_refusal` each read the
operation through `retry()` — its own open and release of the store lock — then opened
the directory again to write a journal derived from what they had read; the transition
was not serialised, so a stale `record_refusal` could overwrite a receipt another
process had just recorded, and a delayed `record_reply` could regress a delivered
journal to generation one.

**Fix.** One hold of the lock per transition: `journaled(&directory, id, context)`
reads the catalogue, the frame and the journal under the directory the caller opened,
and the transition is judged against that state and written through the same handle —
the journal's generation advances from the one read, a committed receipt is never
replaced (`ReceiptMismatch`; `Retired` once the operation retired), a repeated
identical receipt or acknowledgment is harmless, and a refusal recorded before the
owner's receipt is replaced by it. The lock is the store's file lock (`open_native`, waited for briefly), not a
process-local mutex.

**Tests.** `native_store::tests::concurrent_transitions_never_lose_a_committed_receipt`
(twenty-four rounds of two store handles racing a receipt-and-delivery against a closed
refusal on separate threads: the receipt is recorded and delivered whichever came first
— a refusal before it is replaced by the owner's receipt, one after it is judged against
the receipt and refused — and the journal ends with the receipt, delivered, every time),
the adapted `prepare_claims_...` (a refusal after a receipt is `ReceiptMismatch`; a
different receipt is refused; the identical one is harmless).

## F15

**Cause.** A page of the log was copied before it was sized: the stable fetch reserved
`high − low` entry slots and the unstable slice copied the whole selected suffix, then
`limit_bytes` cut what the page admitted — the audit's probe: 1,024 unstable entries of
4 KiB under a 4 KiB page returned one entry, requested 4,268,032 bytes and retained room
for 1,024. A lagging replica's catch-up repeated it a page at a time.

**Fix.** The page is chosen before any of it is copied (`focal_raft::log::page_of`: the
longest prefix whose running total of encoded bytes fits, and the first entry whatever
its bytes — the rule `limit_bytes` cut by, carried across the stable/unstable boundary
with the bytes already taken), reserved for exactly (`try_reserve_exact(taken)`) and
copied only that far; `Log::entries` takes the entry bound too, before the bytes are
counted (both bound a prefix, so the page is the same whichever applies first). The
stable storage (`RamLog::entries`, the differential harness's `Store`, the memory
`Storage` of the tests) chooses its own part by the same rule. A question about the
entries — whether a range holds a configuration change — copies none of them
(`Storage::any_entry`, `Log::any_entry`), where `has_unapplied_conf_changes` copied the
range. The cut after the copy is kept, so a storage that gives more than its part admits
is cut here. Every copy of entries reserves for exactly what it copies (`copy_entries`,
`copy_entries_of`): the vote's answer that carries the held proposals reserved by growth
— room for four, one entry — until the fast suite's page check found it.

**Tests.** `focal_raft::log::tests::a_page_is_chosen_before_it_is_copied_and_holds_no_spare_room`
(across the boundary, at every budget from zero to unbounded: the page's capacity is its
length, each entry's buffer is exact, and the entries are what copying everything and
cutting gives; an oversized first entry is taken alone, stable or not; what follows one
is left for the next page), `the_cores_agree_on_pages_of_a_hundred_bytes_and_a_window_of_four`
(the differential campaign at pages of a hundred bytes and a window of four: every page
the new core sends is checked to hold no spare room, and the cores say the same), the
existing slice and scan tests (`scan` is gone; `any_entry` replaces its one use),
`tests/fast.rs` (every page a member sends holds no spare room, the answer carrying the
held proposals included).

## F16

**Cause.** Every guarded transition — a tick, a campaign, a read barrier, a report, a
step — reserved `2 × resident history + (batch + snapshot) × (members + 2) + incoming ×
(members + 8) + 6 × raw + …` before it ran, and `raw` walked the message queue and the
unstable entries to say what the core held. A heartbeat over a long history asked for
history-sized headroom, and at capacity was refused although it copied a few kilobytes:
the measurement below found the estimate at 546,728 bytes for 64 committed entries and
23,385,104 for 4,096, while the transitions' measured peaks were 0 (tick), 1,184 (read
barrier), 0 (beat) and 22,132 (a proposal with its WAL append) at both.

**Fix.** The allowance names what a transition copies and nothing the size of the
history (`memory::staging_bytes`): the entries not yet durable go into the Ready, the
WAL's records and the prepared storage (2 × their payload and encoded bytes); the
proposals this member holds by itself likewise (3 ×); a snapshot on its way into the
Ready, the prepared storage and the events (3 ×); the committed page the Ready gives is
read from storage a page at most (2 ×); what the leader's sends copy out of the log is
priced from each member's progress (`sends_bytes`): a page to each member behind it —
the core's bytes and entries a message, and the entries' slots — or the snapshot to one
behind the log, and to the one member whose answer the transition may be as many pages
as its window admits (`send_append_all`; F41 is where that window becomes a byte
budget); a member whose progress the transition makes is sent the last entry, its pages
coming in later transitions and priced then; a proposal's bytes join the unstable
entries and one message a peer (`incoming × (members + 8)`, as before); the queue grows
by its own rule (`Outgoing::growth_of`); what the core holds now (`raw`, once), the
member's row in the tracker with its in-flight window, and the transition's own
structures. **Every fixed allowance is derived, none chosen** (2026-09-30, the batch's
residual): the core's message allowance is the message and the bookkeeping of its three
buffers (`proto::MESSAGE_ALLOWANCE`, `BUFFER_OVERHEAD` the four words
`focal_memory::ALLOCATOR_OVERHEAD` keeps), and the consensus crate prices one message the
same; a snapshot is the snapshot and its retained configuration with each member's id in
both and the bookkeeping of its nine buffers; the events are the structure and its five
lists; what the node keeps beside the core's resident bytes is its own state around the
core and its ten containers' bookkeeping, two allocations a member; opening a group is the
node, the smallest message queue, the identity record's encoding (twice, for the buffer's
doubling) and its append, and for each member its validation (two ordered sets at half-full
leaves and the two sorts' buffers: eight slots of an id), its identity bytes (ten a varint,
twice), its id in the log's configuration and its tracker row — the in-flight window is
priced by the transition that makes the member's progress; a transition's own structures
are the Ready, the events, the drain's phase, the hard and soft states and thirteen lists'
bookkeeping, and a record with its buffer for each entry not yet durable, the snapshot and
the hard state; decoding a message is twice its bytes (its buffers double), the message,
each entry's slot twice with its two buffers, and a snapshot's structures with each member
reserved at its length hint (its bytes are among the message's, counted once now, not
twice); replaying a record is twice its bytes and the entry, or the configuration with its
members at their hints; the log's own metadata is the log and its configuration's ids with
four lists. `tests/allowances.rs` holds each to the counting allocator, attributing the
bytes the consensus crate and the core asked for by their innermost frame (the WAL's are
the log budget's): decoding 4,096 empty entries peaks at 294,912 bytes under a charge of
868,736, a 1 MiB entry at 2,097,152 under 2,097,760, a 4 MiB snapshot naming 2,048 members
at 8,388,608 under 8,430,150; a group opened alone on a shared WAL asks 4,444 bytes under
an allowance of 5,396 with one member and 187,110 under 304,112 with 1,024 (before:
20,480 and 4,210,688); a 1 KiB proposal's transition asks 10,675 under a staging of 17,352
(before: 79,272 with the 64 KiB). `staging_peaks` stays: the estimate is 8,136 at 64 and
at 1,024 entries; peaks tick 0, read 1,184, beat 0, proposal 21,916 / 21,918 (the whole
process, the WAL's records included). Three things the 64 KiB had hidden, found by the
suites once it was gone and priced at their cause: a transition may commit and deliver
every durable entry above the applied — a campaign the whole of them — so the page the
Ready gives is priced from the durable index, not the committed; a member's answer may
reject what was sent and move its next index back to what it holds, so its page is priced
from its matched index; and each message a transition may queue (two a member) carries
the message allowance beyond its slot. The pages are read from running totals the
storage keeps beside its entries (`RamLog::bytes_between`: the bytes of any range are a
subtraction, maintained on append, replacement, compaction and snapshot, checked by
`validate`), so the estimate walks nothing but the members; the counters that say what
the core holds are kept as it changes (`Unstable.payload`, `Outgoing.payload`, the held
proposals' bytes), each checked against a walk in the differential harness
(`check_accounting`). The members a transition adds are counted where their progress is
made: a step counts the members of a snapshot it restores that the core does not track;
a change carried in an entry, proposed or stepped, adds no one until the drain applies
it, which counts the additions from the change's bytes without decoding it
(`members_added`; before, any change was priced as the most members, 1,024). A first cut
priced a joining member at a page and the snapshot: the audit's malformed message naming
1,024 members was priced at 8,599,227,675 bytes and refused for capacity before the core
could refuse it as malformed — nine focal-consensus tests said so, and are the
regression. The events a drain delivers keep the charge they carry (recovered at
opening, or delivered by the drain that built them): the staging pays for what the drain
added, and the one charge the events leave with is exact — before, the second drain
split the whole of the recovered events from a staging that, exact now, had no
history-sized room for them. `DurableNode::staging_estimate` states the bound a control
transition is held to; `campaigns_on_next_tick`/`beats_on_next_tick` let an owner know
what a tick may send. Control transitions stay on the completion lane.

**Measurements** (`tests/staging_peaks.rs`, a single voter under the counting allocator,
1 KiB entries): estimate 79,272 bytes at 64 and at 1,024 committed entries (before:
546,728 and 23,385,104 — and 2 × history further at every entry); peaks tick 0, read
barrier 1,184, beat 0, proposal 21,916 / 21,918, unchanged by the history and within the
estimate. A change naming 1,024 members on a three-member group: 7,476,224 bytes above
the change-free estimate (each member's progress, places in the queue, share of the
incoming bytes and the last entry), where the first cut asked 8,599,227,675.

**Tests.** `staging_peaks::a_transition_stages_its_own_copies_whatever_the_history`,
`memory::tests::a_member_that_joins_is_priced_its_progress_and_the_last_entry` (the
change naming the most members costs each what the transition copies for it, and less
than eight mebibytes in all),
`memory::tests::a_member_behind_is_priced_its_pages_from_the_running_totals` (a
three-member group, one member forty entries behind and probed from where it was: the
sends are what a walk of the entries says, the window's pages for that member),
`storage::tests::the_running_totals_say_what_a_walk_of_the_entries_says`, the nine
focal-consensus tests the first cut failed (`what_may_not_go_by_the_fast_track_is_refused_before_the_core`,
the joint-change, removal and unknown-peer safety tests, restart, recovery and learner
tests), the focal-raft and focal-consensus suites (the differential campaigns hold the
counters to a walk after every operation).

## F03

**Cause.** An authenticated stream validated its 16-byte header and then allocated and
zero-filled the whole announced payload — up to the negotiated frame, 10 MiB on the
production control path — before a byte of it arrived and before `dispatch_accounted`
admitted anything; the listener's 64 KiB charge per connection was no permit for it. The
audit's probe: a header announcing 10 MiB allocated 10,485,760 bytes while its reader
waited, and with `for_consensus(128)` streams a connection could hold about 1.4 GiB of
such buffers; and a body dripping `LEAST_PROGRESS` bytes every wait held its buffer for
`bytes / 1,200` waits — 73 hours for 10 MiB at the 30 s wait.

**Fix.** The body is permitted before any of it is allocated. After the header, the
stream's task asks its identity's `IngressLane` for the announced bytes
(`Admission::take`): the permit is funded from the listener's own budget (the node's
listener child, `network_service.rs`; the generic server's caller states one) — nodes on
the completion lane, so control traffic is never starved by participants' bodies, any
other identity on the ordinary lane — and bounded to the identity's share of that lane,
its capacity among the identities holding connections and one frame at least, so an
identity alone may always ask one frame and no identity takes the pool from the others.
A refusal is typed and counted (`refused_bytes`, `refused_memory`; the metric
`focal_listener_refused_total{bound="bytes"|"memory"}`, the gauge
`focal_listener_ingress_bytes`) and resets the stream (code 5) before allocating; the
permit is held through the body and its dispatch and given back with the stream,
cancelled or not. Occupancy is priced by the path: a payload's buffer is held no longer
than its bytes take at the least a live QUIC sender delivers — two datagrams of the least
size a round trip, the smallest congestion window RFC 9002 §7.2 keeps — over the
connection's measured round trip (`frame::residency`; `read_payload_arriving` gives up
at that or at the wait, whichever is longer), on the server's requests and the client's
responses alike: 10 MiB over a 200 ms path is fifteen minutes, not seventy-three hours,
and a sender slower than the smallest window is not sending. The generic server takes
its budget explicitly (`QuicServer::bind(.., budget)`).

**Tests.** `admission::a_body_is_permitted_before_it_is_allocated_within_the_identity_s_share`
(a header alone holds a permit of a frame in the budget with nothing arrived; a second
header from the same identity is refused for its share, reset, counted; the permit goes
back with the stream), the admission's existing cases, and the node's listener tests.
`residency` is exercised by every request and response read in the suites.

**Changed since (2026-10-01):** the residency is two datagrams a probe timeout, and is
the only thing a payload is given up by ([F36](#f38-and-f36)).

**Residual (2026-09-30): the residency was priced by the round trip of an idle path.**
*Cause.* `residency(bytes, rtt)` was computed once, from the connection's round trip when
the header arrived. That round trip is the handshake's: nothing queued behind it. The law
is two datagrams a round trip — the round trip those datagrams take — and a narrow path
takes longer to carry two datagrams than its idle round trip, so a live sender filling
such a path was given less than the path delivers in. The gate run of 2026-09-30 met it:
`narrow_path_carries_a_megabyte_that_takes_longer_than_a_request_is_given` failed with the
client's `early eof` (the server gave the request up and its stream ended), once in that
run and once in twelve repeats of the wire suite; a probe of the receiver showed 7.9 ms
when the megabyte began, 67 ms by its end, and 3.1 s for the megabyte — 437 round trips of
7.9 ms are 3.45 s, and a first sample a millisecond shorter is a failure. *Fix.*
`read_payload_arriving` is given the connection's round trip to ask as the payload
arrives and prices the residency by the longest it has answered; both readers (a server
of its request, a client of its response) pass the connection's own measure. A sender
slower than two datagrams a round trip of the longest the path has shown is given up on
as before, and a buffer is still held no longer than its bytes take at the least a live
path delivers. *Tests.* `a_payload_is_given_the_round_trip_the_path_shows_while_it_arrives`
(virtual time: a datagram every five milliseconds; on a path that shows a millisecond
throughout the payload is given up at its residency; on one that shows twenty once the
payload queues it arrives; with the round trip asked once the test fails, "given up after
110ms"). *Measurement.* The wire suite thirty times over: no failure, where it failed once
in twelve.


## F20

**Cause.** The listener counted every connection future — handshakes, enrollment and
established — against `max_connections` and refused newcomers at that count before
authentication, so full occupancy took with it the opportunity to authenticate a
replacement: eight participants holding sixteen connections each filled the 128 places,
and a restarting node or a participant replacing its own stale connection never reached
the identity admission that would have made room. Enrollment shared the bottleneck.

**Fix.** The outer count is gone from both listeners (`NetworkListener::serve_inner`,
`QuicServer::serve`). What is held is bounded where it is admitted: handshakes by their
pending places, enrollment by its slots, and established connections by the admission's
total (`AdmissionLimits::connections`, the listener's `max_connections`), met **after**
the replacement rule — an identity at its own bound reaches its replacement however full
the listener is, and only a connection that would be one more is refused (`Connections`,
counted, `focal_listener_refused_total{bound="connections"}`). Half-open handshakes from
addresses that have not proven themselves take no pending place while half the places
are taken: the listener answers them with QUIC's Retry (`validate_address`;
RFC 9000 §8.1.2, address validation under load), so a spoofed flood cannot hold the
handshake stage, and a real one holds at most its places for one request timeout. The
admission is one shared handle now (`Admission(Arc<Inner>)`): the tasks that serve a
connection's streams hold their identity's permits beyond any borrow of the listener's
loop (doc 10).

**Tests.** `admission::a_full_listener_still_replaces_an_identity_s_own_connection`
(three connections in all: an identity under its own bound is refused at its handshake
when it would be one more; an identity at its bound replaces the connection it used
least although the listener is full; the others serve on), the existing pending-place
and replacement cases.

## F35

**Cause.** Each stream cloned its `AuthenticatedPeer` from the registry before waiting
for the header and body, and `dispatch_accounted` verified the request against that
snapshot. Revocation removed the registry entry and nothing else: the audit's probe sent
a header, revoked the certificate, then sent the body, and the handler ran
(`registry_denies=true, handler_calls=1`); with F03's progress-based waits a peer could
pre-open incomplete requests and keep the captured grant for hours.

**Fix.** The grant is looked up twice: before anything is read for a stream
(`PeerRegistry::granted`, no copy), so a revoked certificate is refused before a body is
permitted for it, and once the whole request has arrived (`authenticate`), so what is
dispatched is authorized by the grant current then, never by one captured before its
body. Invalidation releases what waits: the registry holds the connections each
certificate has open (`attach` under the listener's bound; `LiveConnection` until
served), and `revoke` and `replace_grants` close the connections of a certificate no
longer granted (`revoked`), so nothing more is received on them, the bodies in flight end
with them and give their permits back. The authorization lease is therefore: a request is
authorized by the grant current when its complete frame was received, honoured through
its handler for at most the request timeout; a revocation takes effect for every request
not yet complete at once, and for connections at once. Work the handler already accepted
durably keeps its exact-result recovery; that is the owner's contract, not the wire's.

**Tests.** `a_grant_revoked_while_a_body_arrives_dispatches_nothing_and_closes_the_connection`
(the half body's permit is held, the revocation closes the connection with `revoked`, the
rest of the body has nowhere to go, the handler never runs, the permit and the
connection are given back), `a_complete_request_is_authorized_by_the_grant_current_at_dispatch`
(a grant withdrawn while a request runs: nothing after it is served on the connection),
the existing `mutual_tls_tenant_isolation_live_revocation_and_independent_streams`.

## F10

**Cause.** The client composed a claim's lineage into an ordinary `NativeReadPage`:
sixteen ancestors and sixty-four related claims at most, the relation lists' continuations
ignored and the extra related claims dropped, `next: None` — a page-shaped value that
could be taken for a complete observation. And the objects were read `AtLeast` the first
token while the page carried that first token, so the page's prefix claim did not hold
for what it showed.

**Fix.** The lineage is one observation at one prefix (`NativeLineage`, a distinct
result kind `native_lineage`): the first read is linearizable and every later read is
`Exact` at its token, so the token, native sequence and logical time the observation
carries are those of every object in it. Its bounds stay and their effects are visible:
`ancestors_beyond` names the next ancestor past the depth (the chain continues above
it), `ancestors_missing` names an ancestor unreadable at the prefix (the chain stops
short of its root), and `followers_beyond` says, per relation, how many followers the
list named that the observation did not read and carries the list's own continuation
(`claim.list` with the same relation filter resumes there); `is_complete` is all of
them absent. The CLI prints `claim lineage` by role (`CLAIM`, `ANCESTOR`, `FOLLOWER`)
with a `COMPLETE` flag and the `*_BEYOND`/`*_MISSING` rows; MCP returns
`native_lineage` under condition `Lineage`; the schema names every field; the peers
skill (version 2) tells an agent to check the bounds before reading a sample as the
whole.

**Tests.** `the_wait_observer_and_the_lineage_read_compose_bounded_exact_reads` (every
later read exact at the first token; the observation complete),
`a_lineage_names_what_its_bounds_left_beyond_it` (a chain of forty names the seventeenth
ancestor; lists of a hundred each count what they named past the bound of sixty-four and
carry their continuations; an ancestor unreadable at the prefix is named where the chain
stops), the CLI and MCP peer-workflow suites (`lineage_ids`/`lineage` assert a complete
observation), the catalogue's kind and schema tests, the skill contract.

## F11

**Cause.** A claim read answered a retired family with its `Retired` continuation, but
every other exact identity a participant kept — an artifact, a validation, an evaluation,
a result, a testament — read only the live core's rows and reported the object missing
once the family had left; nothing on the participant surface followed an identity into
the bundle, though the bytes were under custody and the operator could inspect them. A
missing live object implied that accepted proof had never existed.

**Fix.** The bundle is read as the live core was. In the core, the phased hydration a
checkpoint restore runs is shared (`recovery::hydrate_frame` over a `RowFrame`: the
checkpoint, or the archive), and `StructuralArchive::hydrate` builds a core of the family
alone — the same decoders, schema verification and custody recovery of its artifacts,
validated as a family (every member claim present, no accounting rows, which the
inspection already refuses) and laid out as one member at the prefix the bundle claims
(`ArchiveCore`, read-only by construction). On the wire, `NativeReadQuery::Archived
{ bundle, bytes, object }` asks for one object of the bundle a continuation names, and
`NativeObject::Archived` carries the object with the bundle, the family's root and the
prefix it claims (`native_contract_version` 2, append-only). In the node the read is the
content owner's, never a session's (`archive_reads`; `FleetService` routes it to the
content host, the embedded host serves it from its store): the bundle is fetched under the
request's tenant scope (`check_scope`), inspected, hydrated, and the object built by the
documents a live read builds (`native_reads::object`); a validation comes with its
evaluations in key order and their accepted results, the pages a live `validation.get`
follows. What is told apart: denied tenant access is `Unauthorized`, custody this node
does not hold or holds corrupt is `Unavailable` (never the caller's fault), a row the
bundle never held is `Missing`. A plain read of an evaluation or a result whose claim
retired answers with the claim's continuation, since its key names the claim. The client
follows an identity through its claim (`archive.get`, `get archived CLAIM [--artifact |
--work | --diagnostic | --validation | --testament | --receipt ID]`): the claim read says
where the family is — live, and the object is read from the ledger unwrapped; retired,
and it is read from the bundle as `Archived` — so online and archival reads are distinct
operations with distinct latencies. The evidence skill (version 9) requires the tool.

**Tests.** `retirement_tests::a_bundle_hydrates_into_a_core_of_the_family_read_as_the_live_one_was`
(the family's claim, definition and content read from the hydrated core equal the live
core's before retirement; nothing of the sibling family, no accounting; the live core keeps
the continuation while the bundle still answers),
`cli_archive::a_retired_family_is_read_from_its_bundle_by_every_identity_a_participant_kept`
(the A1 two-party cycle, satisfied and released, retires; `get archived` follows the
claim, the artifact by its content hash, the validation with its `Validated` evaluation and
accepted result, and the testament, by the issuer and by the respondent; a live family
answers unwrapped; an object the bundle never held is `Missing`; a tampered bundle chunk is
`unavailable`; a kill and restart change none of it), the catalogue, schema, example and
skill contract tests, the client suites (`NativeReadOutcome` and the read page cover the
new object).

## F12

**Cause.** Every native request's outcome row stayed resident for good — the exact retry
that might still ask it had no other place to look — and the checkpoint's contract pinned
the outcome count to the native prefix, so a session's lifetime history was its live
capacity: retirement freed a family's rows and left its outcomes (and published one more),
and `limits.outcomes` counted down from the first request to the last, whatever the live
obligations were. Nothing distinguished an outcome something could still ask through the
live path from one nothing could, and nothing fenced a retry of a request whose outcome
had gone, so the only safe answer was to keep them all.

**Fix.** Resident outcomes are exactly the open obligations and the unsealed tail, and the
history leaves the live core into bundles under custody without ever executing old work
again. A request is closed when its generation is below its principal's floor: the owner
keeps one window per principal (`Key::Epochs`, family 50: the floor, the sealed-through
generation, at most two open generations with their resident counts and last logical times,
and which seal holds each sealed generation), admits a request only in an open generation
or the next one (`EpochNotAdmitted` otherwise), refuses a request below the floor by name
(`RequestHistoryExpired`) before it is prepared, and writes the window beside every request
record so replay derives and checks it. The client's journal issues in generations
(catalogue schema 3, carried from schema 2 with everything in generation one), opens the
next once half its capacity was issued in the current one, and advances the floor itself
with the protocol operation `epoch.advance` (`AdvanceEpochFloor`, input tag 28, wire
operation 32; a participant frame the journal issues, never an authored tool) once every
operation of the generations below is delivered; a request never closes its own
generation. A **seal** is a session decision beside retirement (`FOCALSO1`; core
`seal.rs`): the authority derives from its committed state at the committed prefix the
closed outcome and creation-result rows (whole generations of whole principals, then the
retirements' and seals' own outcomes), writes them into an `FCNSEAL1` bundle under custody,
and proposes the record naming the prefix, the bundle, the count, the bound of the
derivation, the resident outcome bound it was derived under and the floors it forces; every
replica derives the same plan and applies it alike (rows leave, windows record the seal,
`Key::Seal(ordinal)` family 51 is written, the Meta counts `sealed`, `sealed_events` and
`seals`, the seal's own outcome is published), inert at another prefix, fail-closed on a
different count or on floors that differ under another outcome bound (`OutcomeBound`), and
fencing every other proposal while in flight (`Sealing`). Resident outcomes
(`outcomes − sealed`) are what admission, retirement, the completion book and the checkpoint
bound; `outcomes` still equals the prefix. Under pressure — resident plus the candidates that
may still be admitted past the bound — the seal forces floors on the least recently used
open generations (by last logical time, then principal; derived deterministically and
re-derived at apply), and the window is shared among principals (each holds at most its
share, at least one). The seal index is bounded (`limits.seals`, derived from the bundle
bound over a fold member's bytes): at the bound a seal carries a fold of the oldest half into
one directory row (`FCNSEAL1` kind 1) and every window's ranges follow it, the fold applied
to a window before the seal it carries is recorded. A sealed outcome is read where it went:
an outcome read of a sealed generation answers `NativeObject::Sealed`, the client follows it
(`NativeReadQuery::Sealed`) to the content owner, which reads the bundle under the tenant
scope, descending folds; a journal without the operation asks the owner's window
(`NativeReadQuery::Epochs`) which generations to probe. A client refused by name resolves the
outcome it may have from the seal, records it as the receipt it is, learns the owner's window
and continues in the generation the owner admits. Seal bundles are content roots (kept by the
collector, carried by a backup). The archive agent proposes a seal when the pressure floors
are non-empty or the closed rows reach half a bundle, and counts `seals_proposed` and
`seals_waiting`. The node's resident window is `FOCAL_NATIVE_OUTCOMES` for qualification. The embedded node
(`focal start` without a network) runs the agent's walk on its owner thread
(`EmbeddedArchive`, the derivations shared with the fleet's owner in `archive_derive`), each
bundle sealed into its own store: before, it neither retired nor — now — sealed, so its window
would have filled for good; found by driving `focal-load` under a small window. The load tool
itself issues in generations as the CLI's journal does (one journal for the run's callers,
rotation at half the journal's capacity, `epoch.advance` once the generation below drained, a
refusal by name learned from the owner's window and the write re-issued once; `expired` and
`floors_advanced` in its report).
Found on the way and fixed at cause: the wire header check refused the new command tag
(`NATIVE_COMMAND_TAGS`); the window row's rebuild budget counted one visit per range where the
reader takes three fields; the fold directory wrote its range count as eight bytes where its
reader takes four; the principals bound and the timers' outcome bound reused a helper whose
refusal was labelled "preparation bytes"; and the ledger's apply path for the seal record
had been lost between edits and was restored.

**Tests.** Core (`seal_tests`): `a_principals_generations_open_in_order_and_close_at_the_floor`
(order, two open, the advance's bounds, a request never closes its own generation, the exact
retry of a resident outcome still answers, a fresh request below the floor is refused by name),
`the_window_is_bounded_for_everyone_and_shared_among_principals` (the bound first, then the
share, then the principals bound), `a_closed_generation_seals_into_a_bundle_the_live_core_points_to`
(the plan, the bundle read by its inspector, the rows gone, the window and seal row, the seal's
outcome, the fence, the checkpoint restore and an owner rebuilt over it),
`pressure_closes_the_least_recent_generations_and_the_index_folds_at_its_bound` (LRU floors,
the record must name them, a seal refused at the range bound without a fold and applied with
it, the fold's directory names its members, the folded index restores),
`a_seal_record_that_differs_from_the_derived_plan_is_refused_unchanged`; the codec's
frame, checkpoint and replay pins for tag 28, families 50/51 and the Meta counters. Client
(`native_store::tests`): `a_journal_rotates_its_generation_and_advances_its_floor_when_the_old_one_is_delivered`,
`a_catalogue_from_before_generations_is_carried_into_the_first_generation`; the coverage
table pins the client-protocol row; the wire pins tag 28 admitted and tag 29 refused. Ledger
(`native_session_cluster_tests`):
`committed_seals_apply_on_every_replica_fence_proposals_and_close_the_generation` (only the
authority proposes, refusals leave nothing in flight, a pending candidate makes it wait and a
stale plan is refused, the fences, a follower that receives the seal in one delivery, one
restarted from its log and one caught up by a checkpoint hold the same rows and digest, the
exact retry of the sealed request is refused by name and the bundle answers what it committed).
Node (`cli_native_epochs.rs`):
`a_closed_generation_is_learned_by_name_and_its_sealed_outcomes_are_still_read` (the real
binary under a small resident window: the founder's generation is closed under pressure and
the refusal is by name with exit 5, the next command commits in the admitted generation, every
committed operation of the closed generation is still answered by its journal and read from
the seal by `request inspect --remote`, the claims stay live, and a restart changes none of
it), `an_embedded_node_seals_its_closed_generations_and_retires_released_families` (the same
on the embedded node, and a released family retires behind its continuation across a
restart). `focal-load` (`generations::tests`): the rotation at the journal's pace, the
advance once the old generation drains, the window learned from a refusal; a run of 300
claims from four callers under the standard window advances two floors with nothing refused,
and from two callers under `FOCAL_NATIVE_OUTCOMES=48` commits them all through the forced
floors (16 writes refused by name and re-issued, 6 floors advanced, nothing refused or
unknown; before the embedded node ran the agent, 48 committed and the rest were refused for
capacity). Measurements: the seal plan and bundle are derived once per tick on the authority
within the archive agent's interval; the resident window is a configured count, not a
lifetime one.


## F13

**Cause.** The founder's node credential was issued at genesis for the registry's
credential lifetime (thirty days) and excluded from the renewal every joined node has:
`founding_principal` accepted the founding subject at revision 1 only, so a renewal of the
founding key could not be issued under it; the controller refused the founder's renewal
and rotation (`Unsupported`); startup authorized the genesis draft's receipt, never one
the key had renewed to; the founder's authority over the root pinned the fingerprint of
its genesis certificate (`FounderControlAuthority::certificate`, checked at construction
and at every enrollment-control request), so a renewed founder could not have committed
an enrollment; the sponsor route joined nodes keep was checked against the genesis
certificate; the renewal window was a constant ten days and the retry a constant minute,
neither derived from the lifetime; and the lifetime itself was a constant compared by
equality on every restore (`registry.limits != limits`), so no cluster could commit
another. A rotation of the founder's key would also have failed the founding draft's
binding to the key it began with, and the root's own group record for the first
directory partition seated the founder at the generation of its genesis grant and was
verified by equality against the current grant, so a founder re-granted for any reason
(a rotated key, a changed topology) could not restart its directory.

**Fix.** The founder's credential is an ordinary credential of its genesis key. The
founding subject binds the assigned principal at every revision (`founding_principal`
without the revision pin; `prepare_renew` issues a founding receipt's renewal with
`issue_founder`, a rotation carries the principal as any rotation does). The controller
has no founder exclusion: it asks the enrollment host it runs itself
(`with_local_sponsor`, `QuorumEnrollmentHost::renew` in-process — not the registered
handler, whose wait for the grant is this controller's own next refresh), under the key
beside the genesis authority (`cluster/network/node-key`), and publishes the fingerprint
it presents once a refresh has granted it (`with_presented`; the founder's enrollment
control reads the watch and authenticates that fingerprint at each local request). The
founder's authority pins its identity and no certificate: `verify_peer` yields the
request's fingerprint and `authorize_current` authorizes it for the founder's node and
principal against the committed registry; an enrollment-control request carries its
`RootPeer` like a Raft one, re-authorized at dispatch and at completion. At start the
founder presents the receipt its key holds, adopts the committed renewal of the same key
when a crash lost the install, and the founding draft accepts a key a committed rotation
moved the identity to (`bound_to_key || carried_to_key`). The sponsor route is checked
against the founder's certificate as committed now. The renewal window is a third of the
receipt's own lifetime (`renewal_window`, `RENEWAL_WINDOW_DIVISOR = 3`: the ACME practice
of renewing when a third of the lifetime remains) and a failed attempt is retried at a
sixtieth of the window (`renewal_retry`, `RENEWAL_ATTEMPTS = 60`: certbot's twice-daily
cadence across its thirty-day window; never under the second the registry decides in).
The lifetime is committed policy: `node.credential_lifetime_seconds` (schema, doc 08)
founds the registry's limits, a start that asks for another is refused as
`CommittedPolicyChange`, `EnrollmentRegistry::restore` adopts the committed lifetimes and
compares only the capacities, and a registry admits three seconds at least
(`MIN_CREDENTIAL_LIFETIME`: a second to be issued in, one to renew in, one to expire in)
and a year at most. The root's group record seats the founder at the generation of its
grant and the founder re-granted since holds the seat at or below its current generation
(`directory_bootstrap`, the verifier's rule for root group records, 24 §19).

**Tests.** Enrollment: `the_founder_renews_under_its_founding_subject_and_rotates_carrying_its_principal`
(revision 2 under the founding subject, authorized after restore, the genesis certificate
retired at the grace; the rotation carries the principal and a renewal under the rotated
key carries it again),
`a_restored_registry_keeps_the_lifetimes_it_committed_and_the_capacities_it_is_given`
(an hour's lifetime restored under the standard limits, a capacity mismatch still refused,
lifetimes of 0, 2 and a year and a second refused, the shortest founds and renews in its
second second). Node (`credential_renewal::tests`):
`the_founder_renews_and_rotates_its_own_credential_and_restarts_on_what_it_holds` (the
founder renews through its own host, the registry lists the renewal under the founding
identity with the genesis certificate retiring, the renewed certificate is announced, a
joined host renews through the founder and a new host enrolls after; the crash window —
the genesis receipt written back — restarts on the committed renewal without another
issuance; a rotation is re-granted under the new key, the restart finds the rotated key
beside the genesis draft, and a renewal under it and the joined host's renewal follow),
`a_committed_short_lifetime_renews_every_node_ahead_of_expiry_and_admits_a_late_joiner`
(a twelve-second lifetime issued to the founder and to a joined host; each renews itself
twice unasked; past the genesis expiry the registry authorizes neither genesis
certificate and a new host enrolls),
`the_credential_lifetime_is_committed_at_genesis_and_a_later_change_is_refused`; the
settings test pins the field's bounds; `network_control::tests` build the enrollment
control from the registry and the presented fingerprint. `cli_deployment` now waits on
the guarantee after the founder's restart without a topology: that restart moves the
founder to the unknown region and re-grants it at its next generation, and with the
seat rule the founder's directory starts on the stale seat and the controller re-seats
every group it votes in (about nine seconds in the run) — before, the verification
refused the re-granted founder's directory at that restart. Docs 08 and 24 §11, the
runbook, `cluster-admin.md`, `network-startup.md` and the schema carry the change.

**Measurements.** Derived quantities under the default lifetime: window 10 days, retry
every 4 hours; under the twelve-second test lifetime: window 4 s, retry every second;
the founder test runs in 11.6 s, the short-lifetime test watches two renewals of two
nodes within its 60 s allowance.

**Stage 2 (2026-09-30): the bootstrap server certificate succeeds itself.** Cause: the
certificate the enrollment endpoint presents was issued once for a year (`authority.bin`,
schema 1), pinned by every invitation and by every joined node's `NetworkState.sponsor`,
and nothing could replace it: a changed pin was an identity change (`same_identity`), and
the registry knew nothing of it. Fix: it lasts the cluster's credential lifetime
(`open_or_create_for`) and the registry names it (schema 5, `BootstrapServer { current,
successor }`, `Change::BootstrapServer` moving it record → stage → activate under the
founder authority, `prepare_bootstrap_server` deriving the move from the authority's
bundle, schema 2 with a staged successor); the founder's controller steps it at the
credential retry cadence (`maintain_bootstrap_server`: stage in the last third, activate
once every invitation open at the staging has closed, swap the bundle after the commit,
present a committed activation after a crash between), the listener presents the new
identity from the next handshake (`ListenerIdentity::replace(node, enrollment)`),
`ServerTrust` carries and accepts a successor pin (invitations schema 2, network states
schema 3, legacy decode with one pin, `fingerprint_as` binding a record to its
invitation's schema), joined nodes adopt the committed pins on every refresh
(`NetworkState::write`), and the pins are no longer identity (`same_sponsor`). Found on the
way, at the twelve-second lifetime: a node's grant in the root authority expired with the
credential it was granted under, so a renewed founder could not restart its directory once
the genesis lifetime had passed — a renewed credential now extends the grant at its
generation (`GrantNode` extension; `next_root_command` issues it when the committed receipt
outlives the grant); group grants expired with their members' grants as issued, so no proof
could be prepared or verified for a group past its first lifetime — a group is now
authorized while its members are (`group_expires_at`, read wherever the grant's expiry was);
and a renewed leader's new certificate was refused by a follower that had not applied the
renewal's commit — which it learns only from that leader — so replication to it stopped for
good (stage 1 made the founder renew and so made this reachable; a joined leader had the
same hole): the transport now admits a renewal it does not know by the key it renews (the grant projection
carries the enrolled keys; a CA-verified certificate of an enrolled key issued after the one
the registry names is granted as the key is, `PeerRegistry::authenticate_certificate`, and
authorized at dispatch by that key, `authorize_node_peer`). A joiner also adopted pins from
its first, empty view of the root; pins are adopted only from a registry at least as new as
the node's own receipt. Tests: wire
`a_renewal_of_an_enrolled_key_is_admitted_until_the_projection_names_or_drops_it`; control
`an_enrolled_key_authorizes_the_node_that_holds_it_and_no_other`;
enrollment
`the_bootstrap_server_certificate_is_recorded_staged_and_presented_once_older_invitations_close`
(the founding registry names the certificate; an older invitation pins one certificate
and is awaited; staging is idempotent; a newer invitation carries both pins and its trust
accepts either; activation waits for the older invitation, then names the successor; the
authority presents it and reopens on it; inadmissible moves refused; checkpoints,
schema-4 checkpoints, schema-1 bundles and schema-1 tokens restore);
directory `a_renewed_credential_extends_the_grant_at_its_generation_and_a_group_lasts_with_its_members`;
node `a_committed_short_lifetime_renews_every_node_ahead_of_expiry_and_admits_a_late_joiner`
(under the twelve-second lifetime the certificate succeeds itself, the joined host renews
through the succeeded one, a late host enrolls with the invitation's pins, and the founder
restarts past its genesis lifetime on it and enrolls another).

**Found on the way: a host that joins an old cluster was never admitted.** *Cause.*
`NetworkController::refresh` installed the route to the sponsor only when the certificate
the observed registry names for the founder was granted. A joiner's first observation is
the genesis, which names the founder's genesis certificate; once that had expired (one
credential lifetime: thirty days by default, twelve seconds in the test) the joiner had no
route, announced its contact to no one, and stayed outside the root for good — enrolled,
renewing while its pins lasted, never admitted. The earlier late-joiner check stopped the
host right after its enrollment and never looked at its admission. *Fix.* The route stands
while the founder's enrollment is unrevoked; what the founder presents is verified by the
pool under the cluster's roots and the founder's name. *Test.* Node
`a_host_that_joins_after_the_founders_genesis_certificate_expired_is_admitted_and_catches_up`
(the founder's genesis certificate expires, a host joins, its contact is committed, it
replicates the root and renews); it fails on the rule before, the host never past the
genesis.

**Residual (designed, next batch).** Stage 3 — issuer succession. Facts the design rests
on (read 2026-09-30): the root is issued with path length zero (`pki.rs`,
`BasicConstraints::Constrained(0)`), so it cannot sign an intermediate and a successor
cannot be cross-certified under it; and the CA is pinned as bytes or by hash in the
registry (`ca_certificate`, `check_authority`), every `ServerTrust`, the listener's and the
pool's trust roots, `verify_issued`, the saved network state, and the root authority's
anchor (`AuthorityAnchor.enrollment_ca`, which every proof statement carries). The
succession is therefore a second root every verifier holds beside the first: the registry
commits the successor (`Change::Issuer`, staged then activated as the bootstrap server is),
trust becomes a set of at most two roots (the listener, the pool, `ServerTrust`,
`verify_issued`), the anchor names the successor's hash beside the first, issuance moves
to the successor at activation, and the first root retires one credential lifetime after
it, when every credential issued under it has been renewed. Recovery of a lost
`authority.bin` (the CA key is custody, never replicated: a verified backup of the private
directory, or a documented re-founding with re-enrollment; the operator decides whether
the private directory is part of `cluster backup`). A founder whose credential expired
outright (down for the last third of its lifetime and longer) cannot sign a renewal
request: the runbook's escalation stands.

## F14

**Cause.** A logical group's checkpoint was a rewrite of the physical log.
`Writer::checkpoint` reserved disk for every indexed byte (`index.total_bytes()`), and
`Wal::rewrite_log_encoded` streamed every other group's record into a new generation,
wrote the group's retained records, installed the fence, scanned the new generation and
removed the old one — on the one writer thread, so every group's append and replay waited
for it. The log had no other way to give space back: the fence named only a tail, and a
scan started at segment zero of the generation. A checkpoint's cost was therefore the
whole log's size, its headroom the whole log again, and each of many groups at its
cadence paid it over.

**Fix.** Three parts, in `focal-log`.
(1) *A checkpoint is its group's own.* It is one group commit: the records the group
keeps, written at the tail, and a floor frame (`RecordKind::Floor`, variant 10: every
frame of the group whose origin is before the sequence it names is dead; the frames it
kept are the `term` just before it). Nothing of any other group is read or written, and
the volume is asked for those records and the floor. `rewrite_log_checkpoint` and
`rewrite_log_encoded` are gone.
(2) *The fence names a base.* `CURRENT` (version 2; a version 1 fence reads as a prefix
from the stream's first frame) carries `DurableBase {segment, byte, sequence, checksum}`:
where the durable prefix starts, and the chain state before it. Scans start there; the
segments before it are removed after the fence that names it, and again at the next open.
(3) *The base moves, and what it meets alive is written again under its origin.* A frame's
origin is the sequence it was first written at. A group's order is its origins' order and
a floor is compared with origins, so one frame can be moved alone, from anywhere, any
number of times, without its group's order depending on where frames stand — which is what
moving part of a group needs, and what a first design (the whole head group moved in one
commit behind a floor) could not give: it made a step as large as a group's history, and
a group larger than a batch immovable. One step of cleaning (`Writer::clean`) takes the
base toward the tail over frames the last fence made durable: a segment with no live frame
is left unread; while the log holds more dead bytes than live ones and a segment — past
that point a whole pass, which writes the live bytes once, frees more than it writes —
each frame is read and verified against the chain, and a live one is written at the tail
as `RecordKind::Moved` (variant 11: the origin, and the record as first encoded). The
copies and the new base are made durable by one fence, so a crash leaves a frame in one
place or the other, never both and never neither. A step reads at most a batch and writes
at most what it is given: inside a commit, the bytes the callers' own commands wrote and
cleaning has not spent (banked up to one step), behind their fence and their flush;
while no command waits, one step at a time with a look at the queue between. A checkpoint
puts its floor in the index before its commit's step, so what it retired is passed in the
same commit and never written again. A step that finds no memory or no room on the volume
leaves the base where it is. Recovery reads headers first — a floor sets its group's floor
and the count of the frames it kept that the scan still reads (those the base passed were
written again after the floor and count there) — then places frames at or above their
group's floor and orders each group by origin; a duplicate origin fails the open. The
single-owner `Wal::replay` delivers live records of a stream with floors and refuses one
with moved frames (`LogError::Relocated`); a caller's batch that holds a floor or a moved
frame is refused (`Identity`). Memory: the index holds one more word a frame (the origin)
and one row a segment, charged; rows for the segments a write may open are reserved
before the write and returned after. The node exports the log's physical and live bytes
and the cleaning counters (`focal_wal_*`).

**Measurements** (`cargo bench -p focal-log --bench checkpoints`: 64 groups on one writer,
56 cold with 256 records each — a 4.1 MiB old log — and 8 hot appending 256 records and
checkpointing to a 4 KiB snapshot sixteen times; one more group appending from its own
thread throughout; 1 MiB segments; the same workload against the code before, where what
a checkpoint wrote is the generation it left; one host, one after the other, each commit
three flushes of its disk):

| | before | after |
|---|---|---|
| written by 128 checkpoints | 599.2 MiB | 5.1 MiB (0.5 their own, 4.6 frames written again) |
| written a byte freed | 64.8 | 0.64 |
| most disk a checkpoint added | 5.2 MiB (the log again) | 0.67 MiB |
| checkpoint p50 / p99 / max | 170 / 476 / 628 ms | 26 / 172 / 192 ms |
| the other group's append p50 / p99 / max | 26 / 185 / 631 ms | 26 / 59 / 451 ms |
| on disk at the end | 4.7 MiB | 11.1 MiB for 5.0 MiB live |

The last row is the bound's price: the log keeps up to as many dead bytes as live ones and
a segment, where a rewrite left none; the tail of the neighbour's waits (the maximum, in
both columns) is the host's own flush.

**Tests** (`focal-log`, `writer::tests`): `a_checkpoint_writes_what_its_group_keeps_and_its_floor_and_asks_the_volume_for_those_bytes`
(a volume with room for the checkpoint's own frames admits it and one byte less refuses
it cleanly; the log grows by exactly those bytes; a reopen learns the floor);
`a_cold_group_the_base_meets_is_written_again_in_its_order_and_the_log_keeps_its_bound`
(a hundred and twenty rounds of a hot group behind a cold one: the cold frames moved lap after
lap, moved frames moved again, the order kept, dead ≤ live + a segment once settled, less
written than freed, the files on disk from the base's segment, memory returned, the same
after a reopen, and the single-owner reader refusing the stream);
`a_crash_at_every_cut_of_a_cleaning_commit_recovers_every_group` (after the append, after
the data flush, after the fence, and after the fence that moved the base with the segments
behind it still on disk); `a_base_inside_the_frames_a_checkpoint_kept_recovers_the_group_whole`;
`garbage_is_worked_off_while_no_command_waits`;
`a_fence_of_the_first_version_opens_as_a_prefix_from_the_start_and_is_written_forward`;
`a_single_owner_replays_the_live_records_of_a_stream_with_floors`;
`a_caller_cannot_write_the_physical_layers_records_and_the_largest_record_is_moved`;
`seeded_histories_of_appends_checkpoints_cuts_and_reopens_replay_every_group_as_written`
(five groups against a model: appends, checkpoints that keep nothing to three records,
leases given up and retaken, cleaning inside commits and between them, a cut armed at any
of the four boundaries, reopens; four seeds in an ordinary run, `FOCAL_SEED_START` /
`FOCAL_SEED_COUNT` for a campaign — seeds 100 to 399, five hundred steps each, replayed
every group as the model held it). The existing writer, consensus and
session suites are unchanged but for one assertion: a checkpoint no longer starts a
generation.

## F17

**Cause.** The core says what a `Ready` needs — which messages may be sent before its
write, whether it asks for a write at all (`must_sync`), that a commit "need not be durable
to be acted on" — and the shell used none of it (`focal-consensus/src/persistence.rs`).
Every output of a `Ready` waited for its write, a leader's appends among them, and for
every later `Ready` of the same drain. A commit that moved once a write was durable was
given a write and a flush of its own (`Phase::Light`) before anything it committed was
released. A `Ready` that moved nothing but the commit was written and waited for. So a
member that alone decides paid two group commits for every entry; a follower paid one for
every commit it was told of; a leader's followers began to persist only once the leader
had; and the owner that shares a thread among sessions learned of an answered write by
asking the log every millisecond (`group_deadline`). The user pointed at mantle's replica,
which drives the same core and fixed what it found of this for itself
(`../mantle/docs/design/replica.md` §3, its audit §11 and its resolution rows 5.1, R19);
each of its findings was checked against this shell and its owners before any was taken
(27 §9 has the table: the owners here always queued what came behind a pending write, so
its R19 did not apply; a tick that comes meanwhile is dropped by design).

**Fix.** 27 §9 states the rules and why each is safe. A commit waits for no write of its
own: a `Ready` that asks for none is not waited for, and a commit that moves once a write
is durable is released at once, kept with the stored hard state, and carried by the
group's next record or by a checkpoint. A member that alone decides writes the commit of
its entries in the append that holds them — true exactly when that append is durable —
and the core's commit is checked against it. A commit no record has carried for a whole
period of its owner is written then, and when the member is dropped, by a write no one
waits for; a group that keeps writing never writes one for itself. A change of membership
is the exception: it is applied, and said to have committed, only once a write has stated
the commit that covers it and that write is durable — whoever hears that a member was
removed may stop it, and a member that restarted without that commit would count the
removed one again and wait for it for good. What a leader sends
leaves while its write is in flight (`DurableNode::sendable`, never a snapshot;
`wait_persisted` for an owner on its own thread), and nothing else is said early: the
events of a drain are still given whole, so no owner meets events while its replica
refuses mutation. The session owner (both forms) and the control owner send before they
wait. The shared owner waits on a signal — work was queued, or the log's writer answered a
session's write (`focal_log::Persisted`, `DurableNode::notify_persisted`,
`fleet_group::Signal`) — and its millisecond poll remains only for a signal that found
the queue full.

**What the node suite found.** With every commit volatile,
`cluster::actual_cli_promotes_caught_up_learner_transfers_and_removes_with_exact_restart_receipt`
failed: the founder removed its one peer, the command answered `committed`, both processes
were killed inside the founder's period, and the founder opened with a log that held the
removal and not its commit — two voters, one gone for good, and no election to be won.
The exception for changes of membership above is that test's finding;
`a_change_of_membership_is_applied_only_once_the_log_holds_its_commit` holds the leader's
disk and sees the removal neither applied nor given until its commit is durable, and
fails without the rule.

A second run found the same cause in another place.
`cli_upgrade::the_fence_rises_only_once_every_node_reports_the_level_and_a_lower_binary_refuses_to_serve`
failed: a host that had applied the upgrade fence was killed inside its owner's period and
started below the fence; its root replica opened without the activation, the host published
that it was ready, and it stopped only when its group told it of the fence again (24 §21
says it "does not start again while its applied registry carries it"). The registry's
revocations are read at a start the same way. This is not one entry's exception: what a
control group applies — enrollment, the fence, placement — its members act on when they
next start, before the group has told them anything, where what a ledger applied is served
only through its group. So a control group applies on a commit its log holds
(`DurableNode::apply_on_written_commit`, set by `ControlReplica::open` and `open_on_wal`,
the two places a control group is opened): the commit rides the group's next append under
load, a quiet group pays one flush for it, and a group of one voter pays nothing.
`a_group_its_members_act_on_at_their_start_applies_only_on_a_commit_its_log_holds` cuts two
voters the moment after each applied an entry and opens what their disks held: with the
rule each holds the entry, and without it neither does.

**What was tried and left.** Releasing everything a `Ready` says that waits for nothing —
the reads it confirms and the entries already committed, with the messages — while its
write is in flight: owners act on events by mutating the replica (the session's readiness
barrier, the control owner's enrollment write behind a read barrier), and a replica with a
`Ready` out refuses them; the events were kept whole. Writing the commit behind every
release, as soon as it moved: on a group that writes again at once the next entry's write
queued behind that flush (64 ms a commit against 37 ms, three members on one disk); it is
written once the group has been quiet for a period.

**Found on the way.** A member authorized the credential it holds against the registry
its own replica recovered (`seed_peer_registry`, `FoundingNetwork::prepare`). A replica
can be behind the registry its credential was committed in: a follower always could — a
founder that followed another root leader and restarted between its renewal and its own
replica's apply could not start — and with a volatile commit a leader can, for the last
period before a cut. `EnrollmentRegistry::authorize_held` admits a credential issued at a
revision the registry has not reached, for the identity the registry lists under the same
enrollment, the CA's, unrevoked and in its validity; the founder's enrollment control
authenticates what the founder presents at each request, not when it is built.

**Tests.** `focal-consensus`: `single_owner_queues_many_groups_into_one_covering_flush`
(twelve groups commit an entry each on one flush and each log gives it back committed);
`a_failed_write_never_releases_success_and_recovery_uses_its_exact_fence` (the three cuts
of the one write an entry and its commit share);
`a_leaders_appends_leave_while_it_flushes_and_a_followers_answer_after` (the leader's disk
held: its appends are given, nothing else, and the member refuses a tick; a follower with
its disk held gives nothing; the entry commits once the leader's write is durable);
`what_is_sent_early_is_charged_and_leaves_a_snapshot_for_the_drain`;
`a_commit_is_waited_for_by_no_write_and_is_written_behind_what_it_released` (every disk
held while a commit is applied by all three; each log holds it after a stop);
`dropping_pending_owner_releases_ram_but_cannot_cancel_an_admitted_write` (a sole voter's
admitted write is committed when the log is opened again). `focal-control`
`io_failure_has_no_completion_and_fail_stops_until_disk_recovery` (a cut after the fence
leaves the request committed and found by its exact retry). `focal-enrollment`
`a_member_opens_on_a_registry_that_has_not_reached_the_renewal_it_holds`. The existing
restart tests of the consensus, control, ledger and node suites pass unchanged.

**Measurements** (`cargo bench -p focal-consensus --bench commits`, release, this host: an
APFS volume where a group commit is three `F_FULLFSYNC`; the three members' logs share the
one disk, so their flushes queue behind each other and the overlap shows as far less than
it is on a disk each):

| | before | after, an owner that waits before it sends | after |
|---|---|---|---|
| One voter, an entry at a time, median | 25.5 ms | 12.8 ms | 12.8 ms |
| One voter, group commits a commit | 2.00 | 1.00 | 1.00 |
| Three voters, an entry at a time, median | 70.1 ms | 38.5 ms | 36.2 ms |
| Three voters, 4,000 entries as fast as the leader takes them | 18,092 /s | 24,213 /s | 28,880 /s |
| Three voters, group commits by member for 4,201 entries | 405, 404, 404 | 203, 204, 203 | 202, 203, 203 |

**Residual.** A group commit is three device flushes (`focal_log::install_fence`: the
data, the fence file, the directory entry of its rename), where the fence could be made
durable in place; it is the next stage, with the research it needs (what tells a torn
fence from a damaged one once it is overwritten in place). A replacement of a member is not held until every voter
knows the configuration it made (mantle's `configuration_known`; the audit's F24/F25).

## F43

**Cause.** Two things, one above the other. The core confirmed every read asked before
the one a quorum answered for, and still broadcast a round of heartbeats for each read as
it was asked (`Raft::read_index` called `bcast_heartbeat_with` for the read's own
context), and again each time a read was asked again: the audit's twenty reads and forty
heartbeats. And the owners drained after every request (`Work::Request` then `drain`, in
`fleet.rs` and `control_host.rs`), so a core that shared rounds would still have been
asked for a round each: a read was never in the core with another that had not been sent
for.

**Fix.** 27 §10 has the rules and why each is safe. The core queues a read and sends
nothing; one round leaves when the member is next asked what there is to do
(`RawNode::ready` calls `Raft::ask_reads`), carrying the last read asked, for every read
asked since the round before (`ReadOnly::asked` counts how many of those that wait a
round sent asks for). A read asked after a round left is asked for by the next and never
confirmed by the one before; a round that was lost is asked again by the leader's own
beat, whose heartbeat carries the last read. The owners take what is queued behind a read
before the drain that sends its round (`Owner::take`; the control owner's loop), counted
by what the owner admits at once; a request that is no read is drained for as it was. The
rule of raft-rs is kept in the core (`ReadRounds::Each`) for the comparison alone.

A round for each drain was chosen over one round out at a time, which mantle's replica
does: there a read asked while a round is out waits for that round's answers before its
own leaves, up to a round trip more for every read that overlaps another, and on a quorum
a long path away that is half again of a read's latency. Here nothing waits: a read asked
alone leaves with the drain that follows it, and the rounds are bounded by the owner's
drains, which under load carry what queued meanwhile.

**Tests.** `focal-raft`: `reads_asked_together_leave_in_one_round_and_one_answer_confirms_them`
(twenty reads, two heartbeats, one answer); `a_read_asked_after_a_round_left_is_asked_for_by_the_next`;
`a_round_that_was_lost_is_asked_again_by_the_leaders_clock` (and a deposed leader has no
round left to send); `tests/group.rs` `a_round_confirms_no_read_asked_after_it_left` (a
round's answers held while another leader is elected and commits; the old leader asked a
second read; with `ReadOnly::advance` confirming past the round, the read is answered at
2 when 4 was committed, and the test fails). Every answered read in every schedule is
now checked against the highest index any member had committed when it was asked
(`Cluster::report`), and the schedules of this core and of both cores ask reads several
at a time (`Op::Reads`); the comparison with raft-rs runs under `ReadRounds::Each` and is
unchanged. `focal-consensus`: `reads_asked_together_are_confirmed_by_one_round_of_heartbeats`
(1, 32 and 128 reads: two heartbeats each time, every barrier from one follower's answer;
a write that commits while a round is out is seen by the read asked after it and not
required of the one asked before). `focal-node`:
`reads_queued_together_leave_in_one_round_and_all_are_answered` (a hundred requests queued
for a session's owner leave in two heartbeats and all are answered; a write queued behind
a read ends the taking and the read behind it stays queued; with the drain after every
request restored it fails).

**Measured.** By count: 128 reads asked together were 256 heartbeats and are 2. No
latency or throughput is claimed: the audit measured none, and a read asked alone costs
what it did.

**Residual.** The reads a leader may hold are bounded by the window it lets a peer have
in flight (`DurableNode::pending_reads`), sized for a round in flight for each read. A
read a follower forwards to a leader at that bound is refused there and found by its
asker's deadline. The directory's own barriers (`directory_bootstrap`,
`directory_authority_host`) drain as they did: each is one read by one operation.

## F41

**Cause.** A leader's window on what it sends a member ahead of its answers counted
messages and nothing else (`focal-raft` `Inflights`: a ring of last indexes, 128 places by
the shell's default). A message is a page of up to an entry's bound and a kilobyte, so the
same window stood for a few kilobytes or for half a gigabyte, on a loopback, on a
64 kbit/s path and on a long fat one alike; and the shell priced every transition's sends
at 128 pages for the member whose answer it might be (`memory::sends_bytes`), so what
bounded the bytes queued for a slow member was the memory that refused them.

**Fix.** 27 §11 has the rules. The window holds bytes with its messages and is full by
either (`Inflights::{full, room, add, free_to, check}`); a page is cut to the window's
room before any of it is copied (`Progress::page_bytes`), and holds one entry at least,
so an entry larger than the bound is sent alone and nothing waits for good. An answer
gives back exactly what the messages it answers took. The bound is each member's
(`RawNode::set_inflight_bytes`), a page until its owner says what the path carries. The
session owner says it from what the transport already measures: twice the congestion
window of the connection to the peer (the rule a sender's buffer is sized by, Linux
`tcp_sndbuf_expand`), and a page at least where the path carries a page within one beat
of the leader (`fleet::inflight_bytes`, fed by the node's pacer from
`PeerConnectionPool::window` and the path's round trip). The shell cuts it to what the
group's budget, and every budget above it, can stage for one transition
(`DurableNode::set_inflight_bytes`), and prices a transition's sends by each member's
bound.

**Tests.** `focal-raft`: `a_window_is_bounded_by_the_bytes_in_flight_and_gives_back_what_it_took`
(mixed sizes, stale, repeated and skipping answers, a bound that falls and rises while
messages are out, an entry larger than the bound, a change of state);
`a_member_is_sent_no_more_bytes_ahead_of_its_answers_than_its_path_carries` (a leader of
three with a bound of a thousand bytes: what is out to the member that answers never
passes the bound by more than an entry, the member that answers nothing is sent one
message, the two that answer commit everything, an oversized entry goes alone and the
next waits, the bound changes mid-flight); every schedule of this core and of both cores
now runs with a bound of 256 bytes and changes members' bounds as it goes (`Op::Window`),
with the bytes counted checked against the messages held after every step
(`Raft::check_accounting`); with an answer made to give back half of what it took, three
tests fail. The comparison with raft-rs runs with no byte bound, as raft-rs has none, and
is unchanged. `focal-consensus`:
`the_bytes_sent_ahead_of_a_members_answers_are_what_its_path_carries_and_the_budget_stages`
(the page until it is said; never nothing; cut to what the strictest budget above the
group stages — the test found the first version cutting by the group's own limit, which a
parent's is below). `focal-node`: `the_bytes_sent_ahead_follow_what_the_path_carries` (a
new path in one room, a grown one, a thin one, a long fat one, nothing measured, the
largest of each) and `an_owner_told_of_a_thin_path_sends_its_peer_by_it`.

**Measured.** By what is bounded: a member's window stood for up to 128 pages, half a
gigabyte at the default entry bound, and stands for a page until its path is known and for
twice the path's window after. No throughput is claimed; none was measured over a real
path in this batch.

**Residual, closed with F42.** An answer to a heartbeat from a member whose window was
full freed the window's first message and sent the next (raft-rs's rule), so the bytes out
passed the bound by a message a beat. See F42: a heartbeat's answer says how far the
member's log goes and is taken as an append's answer, a full window waits, and every frame
that is not delivered is told. Control groups keep the page.

## F45

**Cause.** A session with a write out gave the owner that shares a thread among sessions
a deadline one millisecond away, every time it was asked (`Owner::group_deadline`): the
owner asked the log for every such session a thousand times a second, each ask a receipt
polled and a progress published, and a write answered just after an ask waited for the
next. The log's receipt could wake its waiter all along.

**Fix.** F17 gave a group's writes a call the log's writer makes when it answers them, and
the owner a queue of signals it waits on (27 §9). That left the millisecond in place beside
it, signalled only a `Ready`'s write and a commit's, and read the signals only when the
owner had nothing due — under steady work a session whose write was answered was found by
its millisecond, not by its signal. Now a checkpoint's write and a decoder floor's are
signalled too (`rewrite_checkpoint_async_notified`); `DurableNode::wakes_owner` says
whether everything the group waits for was taken by the log and will signal; the owner
takes its signals at the top of every pass (`take_signals`); and a session that persists
has its tick for its deadline and nothing sooner. A write the log had no room for signals
no one: such sessions are kept (`GroupOwner::unwoken`), one is asked again for each write
of the owner's that the log answers, and each at its tick. A stop under way and an
evidence call out keep their millisecond: they wait on other things than the log.

**Tests.** `a_held_log_is_asked_nothing_and_its_answer_wakes_every_session_that_waits`: the
log is held with 1, 100 and 1,000 sessions each with a write out, the owners' periods
stretched to ten seconds. While it is held no session is asked again
(`ReplicaProgress::waits_asked`, which counts the asks that found a write still out); when
it answers, every write is committed long before a tick. With the millisecond restored one
session is asked 166 times in a quarter of a second and the test fails; with the signal
ignored the write is committed 9.7 seconds later, at its tick, and the test fails. The
existing shared-owner tests (a covering flush for several groups, a stop reaching a
retained `Ready`, the last session stopping on a stalled writer) pass unchanged.

**Measured** (this host, a debug build; the time from the log's answer to the last
commit): one session 15 ms, a hundred 53 ms, a thousand 741 ms; asks of a held log in
250 ms: 0, where one session made 166.

**Residual.** A session the log had no room for, where the room is held by writes of
another owner, is asked again at its tick and not when that room is given back: the log
tells an owner of its own writes only.

## F42

**Cause.** `replication::drive` pushed a send into its active set and only then did the
send wait for its peer's lane (`PeerConnectionPool::exchange`: `slot.inflight.acquire()`
under the exchange's timeout). The set held 1,024; with a peer that stopped answering,
its lane's worth of sends hung on the peer and the rest hung on the lane, the set filled,
and the driver stopped taking from the owners' channel — where the frames of the peers
that answered then waited, for as long as the slow peer's timeouts ran. Beside it: only
`PeerSendError::Lost` was told to a frame's owner. A lane that was full, a peer that
refused for the room, a route that changed, and a frame the owner itself dropped when its
channel or its budget had no room were counted and forgotten, and the group's core went
on sending ahead to a member that was receiving nothing.

**Fix.** 27 §12 has the rules. A send begins only when its peer's lane has a place
(`Waiting::sending`, the pool's own width); what has none waits in that peer's queue, a
heartbeat, a vote or an answer before entries (`ReplicationFrame::urgent`). The driver
never stops receiving and is sized to what the pool itself admits; when it is full the
other peer with the most waiting gives up its newest frame for the frame that came,
unless that frame's own peer holds more (on a tie the other's goes, since 2026-10-01:
below, "Found by CI"). Every
frame that is not accepted is told to its owner — by the driver for what was lost, refused
or given up, by the owners for what they drop themselves — and the core probes that
member. The pool is behind a small trait (`Carrier`) so that the driver is tested with
paths the test holds, not sockets.

With every loss told, the core no longer needs to send on a heartbeat's answer alone
(27 §11): a member's answer says how far its log goes (`HeartbeatAnswers::Position`), a
leader whose own term the member's last entry is of takes it as an append's answer, a full
window otherwise waits, a member whose window is full and that has answered for none of
it through a beat of the leader's ticks is probed with one message cut to its path, and a
probe is sent again when it is told lost or once a beat has passed since it was sent.
raft-rs's rule stays for the comparison (`HeartbeatAnswers::Bare`). (A first version
counted heartbeat answers, an election timeout of them: five tests of the shell that
rejoin a member after a partition found it waiting that long to be probed again. A beat
of ticks is what the answers came at before, on a path that does not stretch them.)

**Tests.** `focal-node` `replication::tests`:
`a_peer_that_answers_nothing_holds_its_own_lane_and_nothing_of_anothers` (twenty frames
for a peer that answers nothing fill a driver of eight; five for a peer that answers come
behind them and are all carried while the first answers nothing; all twenty are told;
every charge is given back);
`what_a_group_cannot_do_without_goes_before_the_entries_that_wait`;
`a_frame_its_peer_refuses_is_told_to_its_owner` (a full lane, a peer with no room, a
route that changed); `many_groups_share_the_driver_and_a_dropped_driver_gives_everything_back`
(twelve hundred frames for four peers, two of which answer nothing, in a driver of a
hundred: the six hundred for the peers that answer are carried, the rest are told or
held, and a driver dropped mid-flight gives back every charge). With what is urgent sent
last, or only an unreachable peer told, two of them fail. `focal-raft`:
`a_heartbeats_answer_gives_back_what_the_member_holds_and_nothing_more`, and every
schedule of the core runs under the new rule; the comparison with raft-rs runs under
`HeartbeatAnswers::Bare` and is unchanged. Three thousand schedules of each (the groups
of this core, of both cores, the fast groups and the comparison) pass, where an ordinary
run has ninety-six; twenty thousand of the groups of this core pass. (Twenty thousand
of the groups of both cores do not all settle, before this change or after it: a member
of raft-rs of higher priority and a longer, older log refuses the other voter for
priority, which refuses it for its log — raft-rs's own rule, 27 §4.5.) Two tests of the
shell and one of the control owner that answer a heartbeat for a member the moment after
its probe let a beat pass first.

**Found on the way.**

*A member that left vetoed the only candidate.* Three thousand schedules of the fast
group did not all settle on the pushed tree (seed 1707; ninety-six do). A leader of higher
priority removed itself; the voter that remained held the removal and had not heard it
committed, so it still needed the first one's vote, and had taken a term to campaign in;
the first, which follows and can never be elected, refused it for priority, from a term
the candidate no longer heard. A refusal for priority is safe only because the member that
refuses could be elected itself. `Raft::settle_priority` puts no priority in force for a
member that may not campaign; `a_member_that_left_refuses_no_one_for_priority` builds the
run and fails without it. The classic core had this, and so does any group whose owner
sets priorities (27 §5).

*The fast track's election was not safe.* Forty thousand schedules of the fast group
commit two entries at one index (seed 9843 on the commit before any change of this date;
`a_group_with_the_fast_track_is_safe_and_settles`). 27 §4.6 has the cause: a fast quorum's
members hold the committed entry beside their logs, vote by the classic comparison of
logs, and the leader they elect keeps its own older entry at that index. It was recorded
in this batch and mended in the next (below). No owner uses the fast track.

*The settle check proposed once.* `Cluster::settles` proposed to the leader of the moment
and waited for that index for good; a proposal taken by a leader that was then deposed is
gone with its term. It proposes again to the leader that followed.

**Residual.** A peer's frames go each on its own stream and arrive in no order (27 §12).

### The fast track's election, mended (2026-10-01)

**Cause.** A member votes for an entry in a leader's round and for a candidate by its
log, and nothing an election reads recorded the first. And a member campaigns by the
configuration it has applied, of which a fast quorum of the leader's need not be one.

**Fix, at the cause** (27 §4.6; first made in mantle's copy of the core, `hyper-raft`
565e19e, and taken here against focal's own schedules). A member that holds the entry
beside its log counts toward a fast quorum only once the leader knows its log holds an
entry of the leader's term: Fast Paxos's rule that a value is chosen by votes of one
round, with the round kept where an election reads it. And a fast quorum is counted
only where it is one of the voters the leader was elected under and of the one other set
a change in its term named; after a third set, the classic quorum until the term ends.
No message, field or durable state is added.

**The model lacked the run twice over.** `docs/models/FastTrack.tla` had no step by which
a leader that was deposed campaigns again with the log it led with, so no member whose
log held what no other took was ever elected, at any bound; and its five voters ran two
terms where the run takes three. The model now has the step and the rule
(`OfTheRound`). Without the rule the checker refuses it (`FastTrackAnyRound.cfg`: four
voters, three terms, a leader that lacks what was committed, in 190,662 states); with it
the same four voters hold every property in all 3,207,204 states (`FastTrackFour.cfg`).
With the rule one index shows nothing of the fast quorum, so three voters are checked
over two indexes (`FastTrackRound.cfg`, 2,462,010 states), and the checker must find an
index committed there by what members hold by themselves (`FastTrackReached.cfg`).

**The checker had no bound, and has three.** The model as first changed did not end at
two indexes: 208 million states and 26 GB on disk in 87 minutes, under a JVM free to
take half the machine's memory (rule 2 of this repository, broken by its own tool). An
election is one step of the model and a member's vote is what it says, which keeps
every run: three voters at three terms were 1,219,562 states without the deposed
leader's step and are 560,563 with it, and the rule one that is elected does not follow
is refused in 89,337 states where it took 26,212,234; every
configuration states its distinct states and the checker stops at one more
(`StateBudget`, `WithinBudget`), a passing one having exactly that many; and
`scripts/check-model.sh` gives the checker 256 MB of heap and as much beside it (what
the largest configuration was measured to need), one thread unless told of more, and
removes a run's states however it ends. Six configurations run on every change.

**Tests.** `an_election_never_commits_a_second_entry_at_a_committed_index` (seed 9843,
under the rules it was found under) and
`a_member_that_counts_by_the_configuration_before_commits_no_second_entry` (seed 54104);
each fails without its rule, with the entry and the index the schedules reported.

**Measured.** 160,000 fast schedules pass, 40,000 from each of the seeds 3,000, 43,000,
100,000 and 200,000; 3,000 of the group and of the comparison with raft-rs pass as
before. Fast commits in the ordinary ninety-six schedules: 418 before, 188 after; the
others are committed by the classic quorum a round later (27 §4.6 says which).

**Residual.** A leader that outlives two changes of its configuration has no fast track
until its term ends; the model has no change of configuration. Both stand in 27 §8.4
against any owner taking the fast track.

## F37

**Cause.** In `PeerConnectionPool::exchange` a peer's answer `Unavailable` or
`OutcomeUnknown` matched an arm that did nothing, and fell through to what follows a
failed exchange: `remote.close()` and the cached connection forgotten. Every wire error
of one stream took the same path. The connection carries every group's messages to that
peer, its probes and its content; so one owner that was not ready, or one request that
timed out, ended all of them, and the next exchange paid a handshake and began its
congestion window again. The caller of the refused request was told `Lost` — to the
liveness driver, that the peer answered nothing.

**Fix.** A peer that answers `Unavailable` or `OutcomeUnknown` answered: the connection
carried the question and the answer, and is kept. The same request is asked again on it,
after the same spread wait as before, and what the peer last said is what the caller is
told (`PeerSendError::Rejected(error)`): a refusal, which the liveness driver already
reads as a peer that is there. An exchange that fails on the wire closes its connection
only when the connection is what failed (`peers::connection_failed`): it has ended
(`QuicRemote::closed`), it could not be used or trusted, the peer did not speak the
protocol on it, or the peer answered nothing on it — this exchange or any other — since
this one was sent (`Slot::answered`). A stream that timed out or ended early while the
peer answered others failed alone. The pool's own deadline for an exchange drops the
exchange and never touched the connection.

**Tests.** `focal-wire`:
`an_operation_refused_or_unanswered_takes_no_other_exchange_with_its_connection` (real
QUIC on the loopback: a request the peer holds while it refuses two others is answered,
and the two are told `Rejected(Unavailable)` and `Rejected(OutcomeUnknown)`; a request the
peer's handler never answers ends, is asked again and ends, while the peer answers probes
and a request sent meanwhile is held across that and answered; one connection is opened
through all of it; with the refusal closing the connection again the test fails);
`a_connection_is_closed_for_an_exchange_only_when_the_connection_failed` (the decision,
by kind of failure, with and without another answer meanwhile).
`peer_pool_caches_connections_reconnects_identical_packets_and_fences_routes` expected
two connections for a message answered `Unavailable` and then accepted: it expects one.

**Residual.** Closed with F38 (below): an exchange's wait is told by its own stream, so
one a peer never answers ends at its own time whatever else the connection carries.

## F38 and F36

**Found by the port (2026-10-02; hyper-raft ca8d44f, told by the mantle session): a body
still arriving was refused.** `frame::read_payload_arriving` gave a body one budget from
its first byte, its residency at the longest round trip seen and a period at least — a
sender limited by its path alone keeps that pace; one that writes as it has, shares its
connection under strict priority or is short of CPU does not, and on a short round trip
the budget is a period. hyper-raft reproduced it at will under a CPU quota (macOS 3/24,
Linux at half a core 7/15, at a fifth 25/30); focal's CI saw it as rare refusals on
ubuntu-24.04 and windows-11-arm. A body's arrival is now charged with what arrives
(`frame::Arriving`, judged every period or probe timeout of the longest round trip; the
connection's delivered bytes of the body's class and the less urgent ones against what
the peer owes of them; silence or a withheld body alone give it up), with one
`frame::Delivery` per connection on either role (27 §7). Tests:
`a_payload_that_keeps_arriving_is_never_given_up_and_one_that_stops_is` (a datagram every
five milliseconds on a one-millisecond path arrives, three times slower than the
residency allowed; a sender that stops is given up one judgement after its last
datagram), `a_bodys_arrival_is_charged_with_what_arrives_against_what_is_owed` (the
judgement apart from any stream: steady arrival never cut off, silence, a body shorter
than a datagram, a body withheld behind others of a more urgent class and of its own,
the judgement stretching with the path), and the F36/F38 measurements unchanged. The
test that priced a payload by its residency was rewritten: its first half asserted the
old law.

Measured first, on the tree as it was (`slow_paths_measured`: the pool as a node
configures it, one message of a group, through a relay that carries so many bits a second
each way): nothing was delivered at or below 8 kbit/s, and nothing that takes its path
more than five seconds at any rate — 64 KiB at 64 kbit/s, a megabyte at 256.

**Causes.** Four, each found by a measurement and mended at its cause.

1. *One time for the whole of an exchange* (F36). `send_bounded` gave what is not content
   the pool's five seconds for the lane, the dial, the request, the answer and every
   attempt together; a dial was given the same five seconds, failed with its callers, and
   was begun again from nothing by the next.
2. *A wait charged to what the connection sent* (F38). `transport::carried` took
   `udp_tx.bytes - lost_bytes` of the whole connection for the progress of one exchange.
   With the outer time gone, sixty-four kilobytes over eight kilobits a second were given
   up at 60.0 s, twelve periods to the second, 5 s before they had arrived: the connection
   had *sent* as much as the request when its last window, seventeen kilobytes, was still
   on the path, and the peer's period to answer began then. The same counters kept a stream
   its peer had stopped reading for as long as the other exchanges moved a datagram a
   period (25 s for two megabytes on the loopback).
3. *The least a live path delivers, taken for its best* (F36). `residency` priced a
   payload at two datagrams a round trip, which a path delivers at its best with the
   smallest window QUIC keeps. 256 KiB over 8 kbit/s took 262 s and were given 225.
4. *A datagram a period asked of every path* (F36). `read_payload_arriving` gave a
   payload up when a period brought less than 1,200 bytes of it. A sender whose flight is
   lost sends again when its probe timer ends, three round trips on; on a path of 4 kbit/s
   that is half a minute in which nothing arrives and nothing is wrong. Fourteen of
   thirty-six exchanges over 4, 8 and 16 kbit/s with none, two and ten percent loss ended
   that way (`narrow_lossy_paths_measured`).

**Fix** (27 §7). Each part of an exchange has its own wait: its lane and its connection
the pool's time each, and the dial goes on without its caller, given what the connector
gives a handshake. What is sent waits for the one thing a sender knows of its own stream,
that the peer acknowledged all of it or stopped taking it (`Carriage::acknowledged`,
`SendStream::stopped`), and the peer's period to answer begins there. Between the last
byte written and that acknowledgment there is nothing to see, so the wait ends by a
bound: the residency of what the connection held to send beside it. The residency is two
datagrams a probe timeout, three round trips (RFC 9002 §5.3, §6.2.1, §6.2.4, §7.2), at
the longest round trip the path has shown; a receiver holds a payload to that and to
nothing else. A group's exchange that ends by time, waiting for its connection or for its
answer, is not asked a second time by the pool: one time used to end all its attempts
together, and its owner asks again by its own clock. No counter of the connection is
read for any of it.

Tried and taken out: giving a write up when its stream took nothing in a period. A stream
takes a window at once and more only as the peer's reading lets it, an eighth of a window
at a time, which a narrow path takes longer than a period to read (a megabyte over 64
kbit/s, given up at 10 s).

**The contract** (the audit's "declared operating contract"). A path is one that returns
a datagram of the least size within a connection's idle timeout: 1,920 bit/s. Below it no
connection is made and every caller is told `Lost` in the pool's time; above it a message
of any size the frame allows is carried in the time the path takes, and held no longer
than its residency, from a budget (F03).

**Tests.** `focal-wire`, real QUIC:
`a_stream_its_peer_does_not_read_ends_whatever_else_its_connection_carries` (two megabytes
a peer never reads, beside four kilobytes ten times a second: 0.5 s; 25.2 s on the tree
before); `..._ends_on_a_path_that_loses_and_reorders` (the same as content behind a
group's messages, one datagram in a hundred lost and a millisecond of jitter: ended
within its residency at the longest round trip the path showed);
`a_peers_time_to_answer_begins_when_it_has_what_was_asked` (32 KiB over 64 kbit/s to a
peer that answers 0.4 s after it has them, a period of 0.5 s; with the period begun when
the request was written it fails at 1.5 s);
`a_groups_message_is_carried_for_as_long_as_its_path_takes` (64 KiB over 256 kbit/s by a
pool whose time is one second; `Lost` before);
`a_dial_is_given_a_handshakes_time_and_outlives_the_callers_that_asked_for_it` (16 kbit/s,
callers that wait half a second: one dial, one connection; 120 failed tries in two
seconds before). The relay of these tests carries a rate each way, a delay, jitter, loss
and an outage (`Shape`).

**Measured** (27 §7 has the table). 8 kbit/s: the dial takes 8.4 s, so the first send is
`Lost` at five and the next is delivered; 64 KiB in 67 s on an open connection, a megabyte
in 1,078 s. 64 kbit/s: a megabyte in 136.7 s. 256 kbit/s: a megabyte in 35.6 s. With 2% and 10% loss, 50 ms of jitter, 8 kbit/s against the other direction,
and outages of 3 s and 8 s, every message of 4, 64 and 256 KiB at 64 and 256 kbit/s is
delivered; an outage of 15 s ends the connection, the message is `Lost` and the next is
delivered. All thirty-six narrow lossy exchanges are answered.

**Residual.** The pool's five seconds and the connection's ten of idle are set, not
derived (27 §8.4). A stream starved by others of its connection ends at its residency,
which is long on a slow path; bandwidth is not reserved for a class (the audit's F40).

## Found by CI (2026-10-01)

Five of the day's twelve CI runs on `slates-port` failed, each on one test that
passes on this machine. Each is a defect, not weather; three are mended here and
the fourth is open below.

**A batch's caller was told before what the batch held was given back**
(`focal-log`; Linux run of 99191da,
`a_checkpoint_writes_what_its_group_keeps_and_its_floor_and_asks_the_volume_for_those_bytes`:
72 bytes of the volume outstanding after a refused checkpoint). *Cause.* The
writer's thread answered the batch's reply and then dropped the batch, whose disk
reservation, memory and slot were given back by the drop; a caller woken by the reply
looked, or asked again, before that. *Fix.* `Batch::refuse` and `Batch::done`: what a
batch holds is given back, or charged to the volume, before its caller is told, in
every path of `append` and `checkpoint`; a command's slot is dropped before its reply
likewise. *Test.* `a_batchs_caller_is_told_once_what_the_batch_held_is_given_back`
asks the volume and the budget on the writer's own thread at the moment it answers
(`Persisted`); with the reply first it sees the 72 bytes CI saw.

**A lock stayed held by a descriptor a child inherited** (`focal-node`; Linux run of
6af1c6b, `cli::context::tests::administration_resolves_selected_unix_node_and_rejects_remote_fallback`:
`Locked` on the catalog the line before had written). *Cause.* Every lock but the
client's was a bare `File` released by its close. A POSIX lock belongs to the open
file, and a process that starts another copies its descriptors to the child for the
moment before the child's program closes them, so a lock released by its close is held
for that moment by a child that never knew of it. A test process that starts `focal`
binaries meanwhile met it; a node that starts a validation handler would. *Fix.*
`focal_platform::FileLock`, the one owner of a lock, `!Clone`, which unlocks the file
when it ends; every lock site uses it (the log, the content and seed stores, the node
directory, enrollment, the MCP bootstrap, the invitation output, the native journal's
creation lock, the client's lock). *Test.*
`a_lock_its_owner_let_go_is_free_while_a_copy_of_its_descriptor_is_open` shows the bare
file's lock held by a copy and the owner's released past one.

**What came while the control owner decided a command was refused for capacity**
(`focal-node`; macOS run of f5f9cac,
`cluster::actual_cli_promotes_caught_up_learner_transfers_and_removes_with_exact_restart_receipt`:
`[capacity] metadata admission capacity exceeded` on `membership remove`). *Cause.* A
control replica decides one command at a time (`Pending`) and answered `Busy` to a
second, which the host told its caller as `Capacity`; the operator's removal met a
placement intent of the node's own, which on a slow runner was still deciding. A
caller's exact retry would have found its request, but a caller is not asked to retry
what the owner can hold. *Fix.* `Waiting::Turn`: a write or a transfer that finds the
proposal held waits its turn in the owner's list of pending requests, in the order it
came, and is proposed by the drain in which the proposal frees; the list's own bound is
the capacity refusal. *Test.*
`what_comes_while_the_owner_decides_another_command_waits_its_turn` (five writes at
once, each expecting the revision the one before leaves, and a transfer behind them:
all committed, in order; without the turn, four of the five are refused).

**A write that waited its turn was given one request time for all before it**
(`focal-node`; macOS run of 4074ead, the test above: the fifth write answered
`OutcomeUnknown`). *Cause.* A pending request's deadline was set when it was admitted,
the request time (350 ms in the rig) and the owner's patience from then; a write that
waited its turn behind four others kept that deadline, took its turn with most of it
spent, and on a runner whose five commits took longer than one request time was given
up while its own write was in flight. *Fix.* A request is given its request time from
its turn, and one that waits its turn is given it again each time the turn passes: its
wait is charged to the progress of what it waits on — the commands decided before it,
at most as many as the owner admits — never to the time they took; a turn that passes
to no one leaves every waiter's deadline where it was, so a stalled group still gives
them up in one request time. *Test.* The same test, paced: the rig's routers hold the
group at each applied index until the test allows the next, one commit at a time,
each hold more than half the request time so that four holds outlast it (a request
time of four seconds and as many ticks of silence before an election, since the holds
hold heartbeats and a starved runner's commit — two seconds after a hold on a macOS
run — must fit beside a hold); every write is decided, and without the renewal the
fifth is given up.

**A stopped session owner was still serving, by the count** (`focal-node`; Windows run
of 7a6170e, `network_service::tests::a_node_whose_session_owner_stopped_is_alive_and_not_serving`:
`serving: true` with the session gone from the list). *Cause.* Readiness's `serving`
asked the fleet's count, `running == installed`, and the count follows a round of the
fleet's worker after the owner stopped, while the host says of itself that it stopped
before its stop is answered; a probe between the two found every owner running. *Fix.*
`FleetManager::stopped_hosts` counts the installed hosts whose own progress says they
stopped, and `serving` asks the hosts beside the count. *Test.* The same test, which
asks the probe the moment the stop is answered.

**The driver's test of a dead peer beside a live one hung** (`focal-node`, found by this
batch's own run of the suite: 2 of 60 runs of
`a_peer_that_answers_nothing_holds_its_own_lane_and_nothing_of_anothers` under the rule
before, each parked with nothing to wake it; and then found to be what the macOS run of
7ea6f63 had been stuck on for two hours, which was cancelled for its log). *Cause.* When the driver is full and the
frame that came finds its own peer holding as much as the peer that holds the most, the
frame that came was given up. The live peer's five frames arrive behind the dead peer's
twenty; whether the driver has seen any of the live peer's sends end before its fifth
arrives is the order a `select!` of two ready branches takes, and when it has seen none,
the fifth finds both peers holding two and goes — told to its owner, as the rule says,
but the test waited for all five before it opened the dead peer's gate, so neither
future ever woke. *Fix.* On a tie the other peer's newest goes: it is the older of the
two, and of a group's frames the newer carries the more (an append supersedes the
appends before it, a heartbeat the heartbeats). The frame that came is given up only
when its own peer holds strictly the most. *Test.*
`a_frame_whose_peer_holds_as_much_as_another_takes_the_others_newest_place` (both
peers gated, a lane of one, a driver of four: the frame given up is the first peer's,
told before anything moves; with the rule before, it is the second peer's). The test
that hung is deterministic under the new rule: the live peer's frames are carried in
every order.

**Open: the drained leader's heal did not complete on macOS** (two runs, on the trees
of F41 and F45, `a_drained_session_leader_hands_leadership_on_before_it_is_removed`:
the replacement host's copy stayed `Installed`, `through 0`, for the whole 300 s budget
while the two other voters were `CaughtUp`; leadership had moved from the drained host
to the founder as designed). What the test printed says where to look: every owner ran
its 100 ms periods with none longer than 165 ms and none refused, so no owner stalled;
the replacement's session owner ran 3,174 periods, so it hosted the copy throughout;
and its metrics named the founder as the session's leader, so its replica heard from the
leader — and committed nothing in five minutes. That is a fresh learner the leader
beats and never appends to, or whose appends it never takes. Seven local repetitions
(three idle, four under the load of the other suites) pass in 50 to 67 s, and the four
CI runs on the trees since F42 pass, whose rules for a member's heartbeat answers and
probes are the ones that changed between. It is not reproduced and not mended; it stands
here until a run on a current tree shows it again or a directed schedule of the core
reaches it. **A run on a current tree showed it again (Linux, 7a6170e, 2026-10-02):**
the same state — the replacement `Installed`, `through 0`, the two others `CaughtUp`,
the active placement still naming the drained host, the plan in `Catchup` for 300 s —
with the replacement's health naming no error and the third host's last refusal a
load report whose revision had moved (`metadata comparison failed`, the periodic
report's compare, not the plan's). The report could not tell whether the
replacement's copy ever learned a leader or was ever appended to; the test now prints
each node's `cluster replicas diagnostics` (leader, commit, apply per replica) and
`cluster plan` (the controller's next actions) beside its health, so the next run says
which. **It did (Linux, d5d4143, 2026-10-02):** the replacement's replica knew the
leader (term 6, so it was beaten through several terms), held nothing (`applied 0`,
`committed 0`, no delivery retained, no seed missing, native not yet active), and the
leader and the other voter had checkpointed (eight entries since), so its log before
the checkpoint was compacted: what the replacement needed was a snapshot, and none
reached it in five minutes. A directed schedule now guards the path on real QUIC
replicas (`fleet_quic::a_member_behind_a_compacted_log_is_brought_up_by_snapshot_by_
any_leader`: a member away while the leader checkpoints, re-admitted as a learner,
leadership handed to the other voter before it caught up — it comes up by the new
leader's snapshot and is promoted, in under two seconds locally), so the hand-off and
compaction alone are not it. What remains particular to the failing run: the
replacement's log was empty (a fresh copy, never a member) and its session not yet
native, where the schedule's member had been a voter. The two ways a snapshot is
dropped on the sending side were read for a strand (the core moves a member out of
its `Snapshot` state only on a snapshot status report; an unreachable report resets a
replicating member alone): both — a frame refused by the owner's budget and one too
large for the peer frame — settle the snapshot flight as failed (`snapshot_feedback`
begins the flight before either check, and a dropped sender is a failure), so neither
strands. The fresh-copy schedule now exists too (the harness opens a fourth, empty-logged
replica, admitted to nothing until a test admits it, with native hosting when asked):
`a_fresh_copy_with_an_empty_log_is_brought_up_by_snapshot_by_any_leader` and
`a_fresh_copy_is_brought_up_by_a_native_snapshot_by_any_leader` — the native one
promising the decoder among the voters, activating, checkpointing, admitting the
fresh copy, handing leadership on, bringing it up natively by the new leader's
snapshot and promoting it — both pass in under four seconds. Writing the native one
found the gate the replacement most likely sits behind: a native group admits a
learner, and promotes a voter, only once its leader holds that node's promise of the
native decoder at the current configuration index (`native_membership_guard`); a node
not yet a member cannot push its promise, so the leader's discovery asks the
candidate of a membership change it holds queued, and the candidate answers only an
asker in its own voters, learners or admitted members (the founder, for a fresh copy;
others once the host's agent admitted them) — an admission queued as unsupported is
answered unknown at the owner's deadline, and the controller asks again, so the loop
closes only if discovery's ask of the candidate lands while the admission is queued.
On a starved runner that window can be missed every time. The replica diagnostics
now print the promises a replica holds and the index they stand at (`promises_at`,
`managed_promises`, `native_promises`), so the next run of the heal shows whether the
replacement's promise ever reached the leader; closing the window itself — asking a
candidate's promise when the directory names it, not only while an admission is
queued — was the next step, and is done: the discovery's round-robin now covers,
beside the voters, the learners (a promotion wants the learner's promise at the
configuration that admitted it) and the nodes the directory names for the session
that the log does not hold yet (the placement agent admits a pending plan's voters on
every hosted copy, the leader's included, before the log names them), so a healing
placement's replacement is asked for its promise before its admission is asked for,
and the admission waits on nothing; guarded in `fleet_managed_tests.rs`. Writing the
schedules also found the replication driver
ending with frames still waiting for a peer's lane when its owner's egress ended —
dropped without a count or a word to their owners; it gives them up as every frame
not sent is now, and its report says so (`sent`, beside `attempted`, `accepted`,
`lost`, `saturated` and `refused`; the QUIC harness asserts both halves of the
identity at every stop).

## F48

**Cause.** `managed_support::support` gave each of the three parts of a discovery 250 ms
of the clock: the replica's own fact (an ask of its owner, answered in the owner's
periods), the exchange with each peer (an exchange of the pool, which since F36 waits
on what the path takes), and the recording of the answer (another ask of the owner). A
healthy path whose exchange takes longer than that never contributed a fact, and native
activation and the promotion of a learner, which need every voter's recorded promise,
were refused for want of one. The loop also finished every exchange of one ledger before
it looked at the next, so one slow ledger's discovery delayed every other's, and it asked
again every 100 ms of the clock.

**Fix.** No part of a discovery has a clock of its own: the replica answers in its
owner's periods or refuses for its queue (`Capacity`), and an exchange with a peer is
given what its path takes. Discoveries of different ledgers run at once, as many as one
lane to a peer holds (`per_peer_inflight`: a node's ledgers share the same few peers, and
more would only wait on the lane), each holding its charge of the budget, whose refusal
is the bound. A ledger is asked again one period of its owner after its last discovery
ended (`ReplicaHost::tick_period`), and when none is due the loop waits for the fleet to
change (`FleetManager::changes`) or for the next to be due. What the receiving owner
accepts is unchanged: the exact current configuration index, or a prospective learner's
bootstrap configuration, and the request's route epoch; an obsolete fact is refused as
before.

**Tests.** `native_support_across_latency` on real processes: three hosts, each behind a
relay that delays every datagram (`tests/support/relay.rs`), a round trip of 300, 600 and
1,200 ms with a tenth of jitter; the founder activates the native decoder, two hosts
join, a plan promotes both to voters — each promotion needs the host's promise, exchanged
across the relays — and the session settles and serves a claim. The three pass in 282 s
together on this host; with the three ceilings put back, the 1,200 ms one does not settle
(the plan's promotions wait for promises that never arrive).

**Residual.** The relay delays a node's every datagram, its clients' and the admin
socket's excepted, so the join and the plan themselves cross it; the test states no bound
on how long they take beyond the harness's.

## F49

**Cause.** A transfer's lease (`CustodyConfig::transfer_ttl`, 60 s) was renewed only when
a request for it was executed, and the body of a request is read before it is executed:
a megabyte chunk takes 131 s to cross 64 kbit/s and 66 s to cross 128 kbit/s, so the
chunk arrived to a transfer that had expired and was refused `Unavailable`, the push
failed, and the copy was never made at that rate, though durable chunks resume. The
chunk is the unit of custody — it is what the manifest hashes — and could not be made
smaller without changing every object's identity.

**Fix.** A chunk goes in parts where its path takes longer than an exchange's time to
carry it whole (27 §7). The sender sizes a part to what the path carries in the time the
pool gives an exchange, at the rate its law holds in flight over its round trip
(`PeerConnectionPool::part_bytes`): a datagram at least, the chunk at most, the whole
chunk where the path is not yet measured. Each part is a request (`ChunkPart`,
`ReadChunkPart` for a pull), so it crosses within an exchange's time, and each part taken
renews the transfer's lease — the lease is tied to admitted progress, and the sixty
seconds cover twelve crossings. The receiver holds a chunk's parts in order in a staging
file beside the objects, promised to the volume part by part (an exact retry takes
nothing; a gap is refused; a part of a chunk held whole is answered as held), and when
the last byte is held verifies the whole against the manifest's hash and installs it
under the same name, so a chunk made of parts is the chunk (`ContentStore::
import_chunk_part`); one that does not verify is discarded whole. A transfer that
expires or is cancelled discards its staged parts (`discard_chunk_parts`): nothing
unfinished outlives its lease. The receiver tells the sender how much of the chunk it
holds, so a lost reply is resumed from the truth.

**Tests.** `focal-evidence`: `a_chunk_imported_in_parts_is_the_chunk_once_whole`.
`focal-node`: `custody::tests::a_chunk_in_parts_renews_the_lease_and_is_the_chunk_once_whole`
(parts in order, a retry, a gap; the lease renewed by a part; a last part that is not the
chunk's discarded whole; parts gone with an expired transfer, the whole chunk kept; a
cancelled transfer's parts gone; sealed durable) and
`a_chunk_is_read_in_parts_from_a_verified_whole`;
`a_megabyte_chunk_reaches_its_copy_across_128_kbit_per_second` in `evidence_quic` (three
in-process replicas over real QUIC, each reached by the others through a relay that
carries 128 kbit/s each way: an upload of a megabyte and seven bytes, in one chunk of a
megabyte, is sealed once its required copy on node 2 holds it; the relay counts what
crossed toward the copy, and the test holds it under one and a half chunks). Measured
(2026-10-02, this machine): at 128 kbit/s the seal came 75.9 s after the upload began,
for a path that takes 65 s to carry the chunk, 1,152,399 bytes crossing toward the copy
for a chunk of 1,048,583 in 87 parts, none lost; at 64 kbit/s the seal came 152.9 s after the upload began, 1,193,907 bytes crossing, for a path of 131 s.
With `part_bytes` made to answer the whole chunk, the 128 kbit/s test fails: the seal never completes within four crossings (260 s), the whole chunk arriving to a lease that expired at sixty.

**Found on the way: an exact retry pushed beside its first.** The client's exchange for
the seal is given its request time (15 s in the test; a client's configured timeout in
the product), and the copy takes longer than that across the relay, so the client asks
the seal again, exactly — and the coordinator ran the retry's job beside the first: two
pushes of the same transfer to the same copy, each sending every part, the receiver's
staging taking what each brought past what it held. 1,970,721 bytes crossed for the
megabyte, at 124 s, where one push takes 75.9 s; and before the parts, the mutation
of the whole-chunk sender passed the test for the same reason, two senders' chunks
arriving to a transfer each had kept alive. A participant job is named by its ledger,
its request (a managed one by its key, as its transfer is) and its kind; a job that
comes while one of the same identity is in flight runs after it (`EvidenceDriver::
run_inner`, `same`), and finds what the first did — a copy that holds the object is
asked nothing (`replicate_to`). At most as many wait behind one as run at once; the rest
are refused the room. The relay's count in the test above is the guard: with the retry
running beside the first it crosses 1.9 chunks.

**Residual.** The pull of a part reads the whole chunk to verify it and cuts the part from
it: a megabyte read for each part. The lease's sixty seconds are set, not derived; what is
derived is that a part crosses in a twelfth of them at the rate the path showed.

