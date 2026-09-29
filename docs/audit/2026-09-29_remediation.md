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
| F02 | P1 | designed | 1 | — |
| F03 | P1 | open | 3 | — |
| F04 | P1 | open | 2 | — |
| F05 | P2 | open | 2 | — |
| F06 | P1 | open | 2 | — |
| F07 | P2 | in tree | 4 | [F07](#f07) |
| F08 | P2 | in tree | 4 | [F08](#f08) |
| F09 | P2 | in tree | 2 | [F09](#f09) |
| F10 | P2 | open | 4 | — |
| F11 | P2 | open | 4 | — |
| F12 | P1 | open | 5 | — |
| F13 | P1 | open | 5 | — |
| F14 | P1 | open | 6 | — |
| F15 | P2 | open | 3 | — |
| F16 | P2 | open | 3 | — |
| F17 | P2 | open | 6 | — |
| F18 | P2 | open | 6 | — |
| F19 | P2 | in tree | 6 | [F19](#f19) |
| F20 | P1 | open | 3 | — |
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
| F35 | P1 | open | 3 | — |
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
| F46 | P1 | open | 8 | — |
| F47 | P2 | open | 11 | — |
| F48 | P1 | open | 9 | — |
| F49 | P1 | open | 9 | — |
| F50 | P2 | open | 11 | — |
| F51 | P2 | open | 11 | — |
| F52 | P2 | in tree (encode) | 11 | [F52](#f52) |
| F53 | P2 | open | 10 | — |
| F54 | P2 | in tree | 12 | [F54](#f54) |

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
