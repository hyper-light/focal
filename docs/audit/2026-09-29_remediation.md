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
| F13 | P1 | in tree (stage 1; 2–3 designed) | 5 | [F13](#f13) |
| F14 | P1 | open | 6 | — |
| F15 | P2 | in tree | 3 | [F15](#f15) |
| F16 | P2 | in tree | 3 | [F16](#f16) |
| F17 | P2 | open | 6 | — |
| F18 | P2 | open | 6 | — |
| F19 | P2 | in tree | 6 | [F19](#f19) |
| F20 | P1 | in tree | 3 | [F20](#f20) |
| F21 | P1 | open | 7 | — |
| F22 | P1 | open | 5 | — |
| F23 | P2 | open | 6 | — |
| F24 | P1 | designed | 5 | [F24](#f24) |
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
| F36 | P1 | open | 9 | — |
| F37 | P1 | open | 8 | — |
| F38 | P2 | open | 8 | — |
| F39 | P2 | open | 12 | — |
| F40 | P2 | open | 10 | — |
| F41 | P2 | open | 10 | — |
| F42 | P1 | open | 9 | — |
| F43 | P2 | open | 10 | — |
| F44 | P2 | open | 10 | — |
| F45 | P2 | open | 10 | — |
| F46 | P1 | in tree | 8 | [F46](#f46) |
| F47 | P2 | open | 11 | — |
| F48 | P1 | open | 9 | — |
| F49 | P1 | open | 9 | — |
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

Designed as the metadata-plane package (root voters by policy, the guarantee counting
the control plane, a founder that returns to lead, replicated partition groups, the PDB
last): `docs/qualification/campaigns/2026-09-29-kind.md` D5 and the design note kept
with the program. Needs the operator's ordering against the other batches.

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

**Residual (designed, next batches).** Stage 2 — the bootstrap enrollment server's
certificate (a year, pinned by every invitation and by the founder's own
`NetworkState.sponsor`): the registry commits the server certificate and a staged
successor (`Change::BootstrapServer`), `ServerTrust` gains a successor pin joiners and
joined nodes accept, the founder stages at a third of the lifetime and switches the
enrollment listener once every invitation issued before the staging has expired
(`max_invitation_lifetime`), `state.install` accepts a pin that chains to the same CA.
Stage 3 — issuer succession (a successor CA cross-signed both ways, committed as
`Change::SucceedIssuer`; nodes accept a committed successor chained from the pinned CA;
renewals issue under it; the old CA retires at its expiry) and recovery of a lost
`authority.bin` (the CA key is custody, never replicated: a verified backup of the private
directory, or a documented re-founding with re-enrollment; the operator decides whether
the private directory is part of `cluster backup`). A founder whose credential expired
outright (down for the last third of its lifetime and longer) cannot sign a renewal
request: the runbook's escalation stands.
