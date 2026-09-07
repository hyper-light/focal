# Implementation evidence and remaining work

Updated 2026-09-06. The objective is the complete P00–P20 plan: the original P00–P16 scope plus the user's manual CLI, skills, MCP, challenge and consultation extension in [13](13-cli-and-agent-implementation-plan.md). This record distinguishes executable components from integration and deployment qualification. No package is marked complete merely because its crate compiles. Imported Hecate references remain unchanged. The current execution boundary is strictly peer-to-peer: Focal records claims, testament/artifact evidence and authenticated verdicts; participants invoke their own validating tools/skills or request evaluation from another peer through ordinary claims. Focal is not an agent/worker launcher or model-job scheduler. Existing optional execution helpers do not make participant execution a daemon responsibility.

A further source audit identifies a real four-family lifecycle gap. Claim
status/history and durable validation attempts exist, but testament lifecycle is
currently only created/acknowledged, artifact lifecycle only created/custody
revision, and validation lifecycle only created/latest epoch. Separate family
state transitions, artifact-target binding, independent testament posting and
terminal outcomes, and atomic child-to-parent propagation remain required.
D-03's single closing attempt is not equivalent to Sylk's multiple posted
testament aggregation. The [02 gap matrix](02-domain-and-lifecycle.md#35-four-coordinated-lifecycle-families-implementation-gap)
distinguishes existing code from that required model and migration work. Existing
tests prove their stated current profile, not completion of the richer lifecycle.
The [peer-validation contract](16-peer-validation-contract.md) records the exact
required lifecycle, participant authorization and migration work. Its concrete
transition/authority decisions are frozen in [17](17-lifecycle-state-and-authority.md);
the durable decoder/activation and historical import plan is in
[18](18-lifecycle-storage-upgrade.md). Native state machines and owner transactions are being implemented below;
their complete durable integration and activation remain open. The current-profile peer mutation surface is implemented as documented in
[19](19-cli-mcp-implementation.md).

The dated sections below retain earlier qualification records; their catalogue
and test counts describe those increments. The latest interface inventory and
checks are in [the current CLI/MCP qualification](#current-climcp-and-readme-qualification).
The subsequent [V1 storage compatibility work](#nested-v1-storage-and-command-identity)
records the lifecycle migration prerequisite now implemented.
The [historical execution increment](#historical-v1-execution) records the
subsequent explicit replay boundary and its qualification.
The [executable lifecycle contract](#executable-successor-lifecycle-contract)
records the subsequent native state/authority/aggregation increment. Its types
are not enabled in the live ledger or CLI/MCP protocol.
The [acceptance and graph increment](#owned-acceptance-graph-and-scope-contracts)
records the latest native guards and compatibility qualification.
The [Session storage increment](#session-v1-envelopes-and-output-identities)
records the subsequent frozen surrounding formats and borrowed checkpoint writer.
The [decoder transition](#bounded-decoder-transition) adds the durable local
upgrade mechanism; production Session still selects only V1.
The [validation ownership increment](#owned-native-validation-definitions-and-retained-evaluation-state)
removes native definition/state self-reference. The subsequent
[RAM preparation increment](#fallible-owned-ram-preparation-and-lineage-chronology)
supports non-Clone values through the existing atomic storage path. The
[persistent directory increment](#persistent-page-directory-and-bounded-path-construction--2026-09-06)
replaces full-directory copies with bounded node edits while preserving those
publication and snapshot contracts. The
[future write and Admission increment](#future-write-bounds-and-admission-completion-prerequisites--2026-09-06)
adds occupancy-independent accounting and eventual-projection checks before
the owner completion book is implemented. The subsequent
[expandable funding increment](#expandable-owner-memory-funding--2026-09-06)
adds explicit owner-controlled growth and trimming with retained backing. The
[completion ownership increment](#native-admission-completion-ownership--2026-09-06)
integrates held Admission report capacity, pinned verification contracts and
candidate journals into the native owner. The
[indexed completion increment](#indexed-native-completion-grants--2026-09-06)
replaces whole-array grant shifts and repeated parent checks; durable activation
remains open. The [respondent evidence increment](#native-respondent-evidence-and-authored-response-cycles--2026-09-06)
adds native work/diagnostic ownership and explicit response closure, posting and
claimant receipt, including bounded multiple cycles. These paths still require
the native durable activation before they reach the running CLI/MCP.
The [failure and delivery increment](#native-work-failures-and-pure-receipt-results--2026-09-06)
adds exact failed-work retention, claimant rejection and handler-free delivery
results. The subsequent [funded Increment implementation](#native-increment-checks-and-target-sealing--2026-09-06)
registers and evaluates exact work products while preserving independent response
progress. Claimant receipt now also registers the complete WholeWork Ready cohort
against actual attached or missing targets; the latest section below records its
qualification. The subsequent projection reconstructs acceptance from actual
retained sources. Explicit claimant WholeWork entry now publishes structural and
zero-check outcomes plus indexed dependency effects. Funded external WholeWork
Begin/report, audit/scope integration and durable activation remain on the critical
path before the native lifecycle reaches users.

The remaining delivery milestones are substantial and are not equally sized:

1. **Finish native lifecycle integration.** Funded WholeWork Begin/report transactions,
   independently progressing artifact/response outcomes, acceptance, audits and
   complete graph effects must publish atomically within held resource budgets.
   Explicit claimant entry and its structural/zero-check consequences are qualified
   in the latest section below; external WholeWork reports are still not enabled.
2. **Activate the lifecycle end to end.** Complete native codecs/import, recovery,
   WAL/Session/quorum dispatch and the shared CLI/MCP path. A complete laptop
   workflow must preserve both successful and failed testimony across restart.
3. **Complete peer workflows.** Finish challenge/consult constructors, proof/work
   policy, authorized corrective/follow-up claims and continuation recovery.
4. **Complete distributed operations.** Integrate shard movement, placement and
   custody changes, retention/restore and regional failure recovery; execute all
   six deployment journeys with their minimal configuration increments.
5. **Ship and qualify.** Publish and verify client/server binaries, finish platform
   support, execute operator runbooks and fault matrices, and measure capacity.

Work is currently within the first milestone. Existing service, interface,
networking and replication components provide foundations for the later ones;
their presence is not evidence that the complete deployment goal is finished.
No completion percentage or delivery date is established by this record.

| Package | Concrete implementation | Remaining acceptance work |
|---|---|---|
| P00 | Rust 1.94.1 workspace/lockfile, domain registry and canonical fixtures, deployment schema, offline contract checker, two-platform CI definition, deterministic network/disk harness, passing dependency policy scan, production panic lint gate | Execute clean Linux build and CI; release notices |
| P01 | Typed IDs/vocabularies, explicit numeric tags, four immutable content families, canonical identity, command/result/delta/effect shapes, frozen canonical vectors | Complete malformed-input allocation bounds, service identity registry, full disposition/Yield API contract |
| P02 | Serial deterministic reducer and owned row overlays; ordered pending admission; retries/epoch floors, revision and actor checks, lifecycle and evidence closure; replay/checkpoints; bounded actual reducer access reports | Independent testament/artifact/validation lifecycles, exact evidence binding and atomic propagation with compatible migration; exhaustive transitions and complete resource accounting |
| P03 | Accounted arena/index primitives; immutable content sharing; prepared COW pages with byte bounds and persistent directory paths; four object families and derived graph indexes integrated with sequencer; fixed-prefix leased reads; bounded scans and traversal | Remove remaining reference-tree publication allocations; production scale and complete recovery budget qualification |
| P04 | Real one-voter Raft, checksummed framed multiplexed WAL, bounded disk-owner queue with cross-group flush batching, one-pass indexed startup, nonblocking durable Ready state machine integrated with grouped scheduling, fail-stop disk behavior and checkpoint generations; session commit/publication integration; persistent local identity | Full crash-cut enumeration, sustained resource-pressure qualification, production pinned checkpoints beyond the bounded reference state |
| P05 | Resumable uploads, bounded verified manifests/chunks, pinned validators; one content owner per node; authenticated placement-fenced peer transfers; required-copy sealing and artifact admission; cold-leader retrieval; bounded live custody/placement replacement with exact-scope retries and stale-job rejection; owned checkpoint/prefix custody verification | Connect replacement to committed placement recovery, grants/schema registry, validator isolation and policy qualification, recovery reconciliation and full GC |
| P06 | Fixed-point graph oracle, durable monitor/attempt records and receipt fences; optional participant-owned focal-runtime helper with bounded pool, quorum-fenced attempt/recovery tests, deadlines and retained charges | Child-cause/continuation seams and full policy/standing qualification; authenticated current-profile peer evaluation is implemented, with no daemon worker scheduler |
| P07 | mTLS QUIC and kernel-authenticated Unix framing/client; snapshot reads, upload/download and durable cursor integration; atomic ACK/renew; gap-free seed and tail in local and quorum hosts; idle lease maintenance; response allocations survive transport | Complete embedded/network conformance and scale qualification; bounded filtered seeds and cursor-bound wire traversal are implemented |
| P08 | Real multi-peer Raft and bounded replica host; borrowed direct/grouped peer drivers; committed membership changes with exact configuration fences, caught-up learner promotion and quorum-fenced replies; real QUIC leader-loss/retry/restart and distributed evidence tests; content/stream/runtime composition; pinned enrollment with commit-before-release credentials; one-port data/enrollment listener, durable founding bootstrap and recoverable pinned join intent; foreground network service, invitation/join CLI, certificate-bound node capabilities and automatic committed root-learner admission | Committed application capabilities, live placement/custody assignment and complete join-to-voter journey |
| P09 | Shared physical WAL; durable root/partition control state machines and authenticated multi-voter owners; quorum metadata reads and durable retries; committed configuration changes and certificate-bound node contacts; placement barriers, route caches and weighted fair scheduler; managed grouped session owner with bounded live installation, removal, incarnation fences and hierarchical tenant/node admission; replicated-root enrollment adapter and founder-pinned remote enrollment routing; bounded signed authority registry with explicit control-log activation, versioned snapshot installation and installed verification; durable-prefix contact/grant projection; real session placement witnesses and installed-authority signing permits; managed service routing; live root-authorized first directory bootstrap over the shared WAL | Quorum-share collection and delegation evidence producers; general directory scheduling, authority refresh and placement execution; committed-assignment recovery and admission qualification across every component |
| P10 | Serial oracle and owned pending row reservations; default sequencer executes scoped parallel epochs with actual access audits, cursor barriers and whole-epoch serial fallback; prevalidated graph publication | Remove reference-tree publication allocations; persistent worker scheduling, complete memory/performance measurements and broader fallback qualification |
| P11 | Memory range primitives and unintegrated range coordinator groundwork; narrow bounds, accounting, provenance and checkpoint tests | Distributed range integration, cross-range execution, movement barriers, restart and cursor translation; full acceptance suite |
| P12 | Durable local content store and bounded checkpoint framing | Archive catalog/retirement, request reclamation, retention floor coordination, GC and complete restore |
| P13 | Deterministic node/zone/region placement solver checks voter and content survivability and residency; directory retains active guarantee until verified transition barriers | Geographic placement execution, distributed custody transfer, regional partitions and disaster restore |
| P14 | Focused deterministic, corruption, concurrency and crash/restart tests; instrumented committed-prefix/receipt history checker; real CLI SIGKILL tests for writes, uploads and streams | Black-box linearizability exploration, full fault matrix, load runner, capacity evidence and long-duration mixed-fault runs |
| P15 | Strict configuration parser and offline deployment explanation, persisted local identity/policy, foreground local and network services, private invitation/join CLI, bounded ingress and shutdown, build/check harness | Deployment plan/apply and live guarantee explanation, remaining cluster operations, metrics, packaging, rolling upgrades, release artifacts and executed runbooks |
| P16 | Target deployment contracts and placement solver | All six real deployment journeys, operator walkthroughs and measured concept/configuration increments |
| P17 | Exhaustive current-model operation inventory; strict authored JSON/YAML and atomic claim batches; indexed optional filters, bounded singular selection and authenticated cursors; validator contracts and immutable run/attempt projections; peer admission with saved revision/run fences; durable local journals and scoped managed streams; summary/traversal and watches with explicit consumed-page acknowledgment | Remaining query predicates, richer lifecycle storage/model, managed close/rotation and scoped child-cause admission |
| P18 | Four-family submit/get/list and peer lifecycle commands; durable large artifact upload/registration/download and explicit transfer inspection/cancellation; validator discovery; named Unix/QUIC/enrolled contexts and joined-node principal checks; exact recovery; bounded watch/traversal/summary; table/JSON/YAML output; generated operation schemas/examples, offline input/request validation and five shell completions; root/application administration and passive operator diagnostics; real binary restart tests | Deployment plan/apply, placement drain, same-identity credential renewal, backup/restore and all six deployment journeys |
| P19 | Bounded stdio MCP with both protocol profiles; shared authored operations, six managed recovery tools, five transfers, four durable watches, and 32 conditional local administration tools; four packaged skills with pinned live contracts; actual CLI/MCP restart and peer-validation parity | Complete external-client interoperability qualification; challenge/consult policy and continuation tooling; independent lifecycle migration and required deployment journeys |
| P20 | Existing claim/testament/validation lifecycle and fenced evidence/verdict primitives; source policy audit | Complete peer challenge/consult constructors, proof/work acceptance policy, authorized participant corrective/follow-up issuance, child-cause authority, participant continuation recovery and CLI/MCP journeys |

Local focused tests currently establish these properties:

- The serial core covers mixed dependency/wait/release propagation, including exhaustive three-node graph combinations; immutable content survives lifecycle changes; pending close/cancel/epoch races have deterministic results.
- Memory tests compare randomized operations with ordered-map and graph oracles, retain old views through concurrent publication, sweep allocation failures, and prove prepared publication requires no new allocation.
- WAL/consensus tests use real disk files for failed persistence, checksum corruption, lost quorum, restart, learner snapshots and shared-group checkpoint isolation. A follower cannot acknowledge a write whose required disk synchronization failed.
- The integrated example stores a content-addressed test report, commits an acknowledged testament, executes the pinned report validator and reaches `Satisfied`. Checkpoint/reopen/retry preserves the same proof, history and session sequence.
- The executable survives SIGKILL between successful requests and restart: mutation receipts stay identical, uploads resume at the durable offset, and unacknowledged stream deltas replay with the same IDs. Cursor ACKs persist through the session log and checkpoint; lease-only maintenance adds no client receipts and cannot release protected history.
- Directory tests reject unverified placement/authority changes and exercise durable checkpoint restore, bounded route invalidation and weighted scheduling under pressure. Three-voter control tests cover root/partition authorization, quorum reads, partitioned writes, exact retries and restart over real TLS/QUIC. Enrollment tests reject mismatched pins before token disclosure, strip caller-requested certificate privileges and release credentials only after the public metadata decision commits.
- The real three-node evidence test verifies placement-selected durable copies, refusal while a required copy is unavailable, authenticated upload ownership, and a cold replacement leader fetching content before admitting a new artifact. An exact already-committed retry returns its receipt even with both copy routes unavailable; it does not require a fresh content fetch. Reusing that request ID with changed content returns an idempotency conflict. Reopening each disk verifies the transferred content.
- Optional participant-runtime embedding tests prevent its local tool dispatch before assignment commitment, cancel its work on authority loss, and preserve pending intent under backpressure. They do not establish a Focal daemon agent launcher or external job system. The appended fenced-verdict command checks the effective pending receipt, including an absent receipt; legacy verdict encodings remain available for replay but are refused at new remote ingress.
- Multi-voter stream tests fence reads with a current-term quorum barrier, commit acknowledgments before replying, retain seed prefixes through concurrent publication, and recover identical durable cursor decisions after restart. The host alone drains consensus messages.

Reproduce the current test set using [building instructions](../building.md). CI and tests added during continued implementation must be executed before their result is recorded as evidence. Fixed test seeds and assertion histories live alongside their corresponding tests.

The core retains a bounded reference state using ordered maps. Pending admission owns changed rows instead of full reference-state candidates. The default session sequencer executes bounded, audited epoch worker waves, checks the complete epoch before publication and retains a whole-epoch serial fallback. Cursor entries form barriers. Reference-tree publication still allocates, preparation still scans/hashes effective state, and persistent worker scheduling and measured performance remain open. The physical WAL has one bounded disk owner and batches concurrent group appends. The Raft adapter and session expose nonblocking persistence polling that retains each exact Ready and its allowances until durability completes. Grouped node owners poll without waiting on the shared disk writer. Blocked inputs retain their original queues and allowances while eligible groups progress. Static admission bounds reject impossible batches; resource-pressure retries retain the exact Ready. Sustained capacity qualification remains open. An embedded seal proves one local durable copy. The fleet coordinator waits for every required copy in an installed verified placement, but live placement transitions and real geographic failure qualification remain open. The [dependency audit](../dependencies/README.md) passes advisories, licenses, sources and bans with no suppressed advisories; ten duplicate-version warnings remain. No Meta-scale throughput, global operation, or completed release is claimed.

## Ownership and failure cleanup

The [production policy](10-ownership-and-failure-policy.md) now governs the existing implementation. Owner-local metadata and host handles no longer require `Arc`; worker tasks, data deliveries, and their allocations move between owners. Remaining sharing has a documented concurrent lifetime or dependency requirement. Canonical encoding, indexing, arithmetic, deadlines, and output failures use fallible paths. Upstream Raft and runtime initialization have explicit unwind boundaries.

The cleanup also closes a custody failure path: a content store stopped by an I/O error cannot verify or serve evidence until recovery. Recovery syncs surviving upload data and object directory entries before returning an available store. Shutdown no longer waits indefinitely in Tokio runtime destruction after the service deadline; stop admission and owner joining avoid panic-prone blocking-pool startup. Regression tests cover closed stdout, capacity overflow, shutdown backpressure, failed-store reads, callback failure, stale publication and allocation retention.

Continued cleanup preserves owned input and response allowances across oneshot delivery, evidence preflight, and actual Unix/QUIC delivery or cancellation. Local and fleet ingress reserve response construction capacity before admission, then shrink the allowance to the completed response. Slow-reader, connection-loss and owner-shutdown tests cover permit lifetime. Manifest decoding validates the declared chunk count against available encoded bytes before allocating. Intrinsic content-format bounds are independent of each node's local upload preferences, so imported content remains readable after restart with smaller upload settings. Downloads return a bounded partial page without requiring callers to know the internal chunk size.

The shared-worker cleanup adds immutable budget ancestry, transactional ancestor reservation/rollback, and allocation splitting without another permit wrapper. Consensus entries, snapshot buffers, Ready staging and emitted events participate in the hierarchy. Session and control adapters move event allowances through consumption and encoding. One exhaustive command policy preserves completion capacity through local/fleet ingress, queue slots, scheduling and consensus. Tests distinguish ordinary pressure, under which reserved work progresses, from exhaustion of the entire allowance, which safely stops the owner and requires recovery.

The grouped host serves 48 idle sessions without allocating their full active publication queues. Tests cover tenant isolation, multiple independent session quorums on each node, leader loss, durable restart and both remaining-handle drop orders. Shutdown drains election/readiness output without starting new runtime effects; index zero remains the empty-prefix sentinel. The WAL disk owner preserves active empty groups across another group's checkpoint, rejects blocking replay reentry, and returns a typed consumed-receipt error for repeated/mixed polling. Disk and grouped-worker stacks have explicit bounds and reservations. The fleet retains each physical WAL independently of its logical sessions, so removing a stopped session cannot join a stalled disk thread on the worker serving healthy sessions. Final physical-writer joining remains part of whole-fleet teardown. These checks establish specific resource and failure behavior; they do not measure fleet capacity.

Final cleanup qualification on macOS arm64, 2026-09-05: the workspace run passes all 424 tests with no failures or ignored tests. Coverage includes default epoch application and inline singleton waves, bounded staging on populated state, grouped WAL persistence and covering flushes, immutable ancestor admission limits, scheduler starvation, completion pressure, channel/event lifetimes, shutdown with a stalled final writer, one-port QUIC enrollment, missing runtime drivers and moved transport handles, durable join retries, lost policy rejection, founder revocation and interrupted bootstrap recovery, committed authority activation and compatible recovery, membership catch-up/promotion/recovery, exact snapshot configuration checks, pending membership shutdown, contact replay and revocation, leader rediscovery, distributed custody and CLI crash/restart. Strict all-target Clippy, the separate production policy pass, formatting, imported-source/architecture contracts, and the full workspace release build pass. The external dependency inventory remains unchanged at 203 packages. This is local correctness/build evidence, not throughput or deployment-scale qualification; the earlier unexplained QUIC timeout is recorded below.

The continuing cleanup replaces boxed network startup errors with typed failures and centralizes physical identity validation. Initialization markers prevent a lost identity, join journal, policy or public manifest from silently creating a replacement deployment. Local and founding network startup share one policy check before creating stores or private network credentials; missing policy beside an existing WAL is rejected. Existing policy bytes retain their original format. A private founding draft pins exact credentials before root initialization; startup checks the recovered enrollment registry, preserving later revocations. The manifest validates the original one-founder genesis and CA. Missing initialized private keys, corrupt or trailing payload bytes, incompatible policy and legacy bootstrap fail closed. The strict singleton constructor rejects expanded membership; the network service recovers it and establishes readiness through live replication and a current-term quorum barrier. Network startup buffers retain owned reservations, and directory ownership lasts through store shutdown.

Pending admission uses a bounded staging allowance independent of the total core payload. Input copies, changed rows and reducer worklists consume it before allocation. Exhaustion returns a local capacity error before consensus admission. A populated-state regression fills ordinary memory and verifies that a small completion operation still commits using its reserve. The obsolete cloned pending-state oracle is compiled only for tests; the production sequencer uses owned row reservations and audited epoch output. Singleton execution waves run inline with the same output audit and unwind containment; one-command and one-worker epochs reserve no worker-stack allowance. Concurrent waves still use scoped workers and join every started worker on failure.

The shared-port listener selects separate data and bootstrap certificates by ALPN. Real QUIC tests cover pin-before-token enrollment, Node-only metadata reads, withheld data grants, role/CSR privilege rejection and live revocation. Network helper boundaries return errors for missing Tokio context and contain dependency failures from runtimes missing IO/timer drivers. Bind checks run once; exchanges contain dependency unwinds even when a handle moves to another runtime, without probing a timer per request. Failed Unix setup removes only the socket inode it created. Connection admission accounts for bookkeeping; it is not a complete measurement of transport buffer RSS. Authority activation, grants and proof-consuming commands now commit in the control log, preserving previous bootstrap hashes and command tags. Once activated, injected legacy verifiers cannot bypass installed authority. Partitions install authenticated root snapshots through their own log; revocations take effect there after installation. Live session/custody/delegation evidence producers remain open. These additions do not complete the join-to-voter service or any distributed deployment journey.

The join component keeps one private key, CSR and request identity across unknown enrollment outcomes and restart. Verified enrollment is persisted before physical identity installation, and loss of initialized private state is rejected. Invitation files use private, non-clobbering atomic installation; private invitation/join wrappers redact and clear their secret buffers on drop. Generic transport buffers do not provide that zeroization guarantee. `cluster invite` calls a founder-only OS-authenticated administrative socket; its named request survives retries and signer restart. `join` installs identity and exits; plain `start` selects the persisted network service. A joining Node certificate creates no voter, session replica or Runtime grant.

Membership cleanup binds each change to the complete applied configuration and its Raft index, so returning to the same voter set cannot satisfy a stale request. Receipts come from applying the configuration entry itself. The session replica host waits for a subsequent current-term quorum read before returning a membership result. Control writes return the committed receipt; obtaining the current control configuration requires a separate quorum-fenced read. An unassigned or removed node cannot campaign through an upstream path that assumes local voter progress exists. Session checkpoint V3 retains the latest membership receipt and reads the earlier formats; older superseded membership requests require reconciliation from a fresh view. Control checkpoint V3 retains configuration indices and contacts while preserving its existing retry table. Leadership transfer reports initiation, not a committed membership result. These APIs remain trusted composition boundaries; a Node certificate grants no placement or Runtime authority.

Recovery checks a retained membership receipt against the snapshot's own configuration and term, before applying any later configuration entries from the same drain. Tests reject checksum-valid snapshots with a different valid voter configuration or a future receipt term, and preserve correct snapshot-plus-suffix replay. Local membership views return a persistence-pending error while the consensus owner retains a Ready, preventing mixed published metadata. Pending membership participates in shutdown accounting, so a durable but uncommitted change is recovered without trying to checkpoint uncommitted application state. Membership request buffers and queue backing are released before their allowance passes to a reply consumer.

Node contact announcements derive identity from the authenticated certificate and recheck committed enrollment, including for retries. They commit only reachability, with an expected generation and exact request identity. A later server timestamp does not change retry intent. Tests cover uncheckpointed replay, checkpoint recovery, lost replies and revocation despite a cached TLS grant. The running network controller checks historical contacts against active enrollment before installing transport routes and atomically replacing grants. It exports state, contacts and configuration at one durable local prefix, preserving follower route reconstruction without claiming a quorum read. The founder persists a separate exact learner-admission intent before submission. Joining nodes catch up as root learners and recover that state on restart; they have no local ledger policy or application assignment.

The network service preserves an existing laptop ledger's identity, mutation receipts and content. It owns root, content and managed ledger workers, plus the founder’s initial directory owner, over the shared physical WAL, and runs control/data replication, enrollment and local/QUIC ingress. The enrollment signer routes through installed contacts to the current root leader with a narrowly pinned founder capability; generic Node certificates cannot submit root administration. Placement-driven ledger creation, voter promotion and stronger durability remain separate required work. The service does not implement the deployment plan/apply commands or qualify the complete VM journey.

The next service integration must connect committed placement/custody capabilities to dynamic replica creation and recovery, then use verified catch-up and content evidence for membership promotion and readiness. The current bootstrap admits joining nodes as root learners; fleet-scale operation must replace that broad bootstrap replication with bounded control-group placement and regional/partition delegation. A global roster replicated to every node is not the target architecture. The unchanged proof workflow must then survive founder loss with exact unknown-outcome retries before a stronger durability plan can activate.

The continuing implementation adds an initially empty managed session fleet over one existing worker and a fixed set of physical WAL writers. Installation, inspection, exact latest retry and removal use bounded owner messages. A process-local incarnation fences obsolete handles; removal requires a stopped session and preserves its WAL. The manager's sequence is a reconciliation aid, not a durable placement decision. A controller must recover its committed assignment and inspect an unknown installation before constructing another session on that logical WAL. New per-incarnation allowances live in the existing progress watch; the single physical queue backing lease remains shared across unrelated session handles, management and egress.

Session placement records now produce opaque committed witnesses for Created, Cutover and Activated. Their Raft indexes increase while domain `SessionSeq` may remain unchanged, including zero for an empty ledger. Snapshot V4 and log replay retain active placement and the last cutover receipt; cutover pauses new domain admission. The bounded control-owner proof request checks the actual installed group, genesis, node generations, configuration, enrollment and time window before returning an owned signing permit. Signing borrows the credential. These facts attest session-log authorization; they do not claim content-copy completion or manufacture `ReplicaReady`. Quorum-share collection and connection of verified-through custody to directory readiness remain required before a deployment can activate stronger durability.

Network root admission now activates its authority registry and commits certificate-bound node capabilities before learner admission. Unknown region and zone are explicit sentinels: node-level placement can use such a capability, while geographic survivability and residency require verified geography. Distinct stale activation and authority-revision intents fail their comparisons before time checks obscure a safe retry. Learner admission checks the installed capability, and serialized root preparation prevents a saved intent from overriding a later quarantine or revocation. Already-capable candidates precede new grants; registry saturation returns bounded capacity failure before cloning candidate state. The controller is the sole live transport-grant writer; enrollment waits for its installed grant instead of reinserting one after a delayed authorization reply. Failed observation removes grants while preserving recovery routes. Root Raft and peer-control dispatch recheck the actual committed enrollment, and pending peer reads recheck again before release. These changes preserve the existing bootstrap's limited application assignment.

Network service qualification on macOS arm64, 2026-09-05: all 445 workspace tests pass, with no failures or ignored tests. Documentation targets, strict all-target Clippy, the separate production no-panic policy pass, formatting, architecture contracts and the complete workspace release build pass. New regressions cover actual CLI enrollment/restart, root-learner catch-up, preserved ledger receipts/content, recovered revocation before ingress, exact learner journal recovery, atomic grant replacement, owned observation/recovery-message lifetimes, callback and runtime-driver failures, and cancellation retaining the directory lock. A real QUIC regression blackholes the preferred leader beyond the round deadline and verifies that the reachable alternate receives the identical contact request; separate per-peer deadlines prevent preferred-leader starvation. The [startup guide](../network-startup.md) documents the implemented commands and limitations. No new dependency or `Arc` wrapper was introduced by this service integration. Deployment-scale qualification remains open.

Following placement and ownership qualification on the same platform/date: the unfiltered workspace run passes **474 tests across 47 targets**, with no failures or ignored tests. Strict all-target Clippy, the separate production no-panic gate, documentation targets, formatting, architecture contracts and the full release build pass. Added coverage includes live fleet replacement and stale handles, canceled installation, a stalled retained writer beside another live writer, placement commit/replay and route fencing, owned proof delivery and topology admission, live custody replacement, root capability capacity/expiry/withdrawal, exact learner-intent recovery, and revocation under observation pressure or delayed replies. Controller cancellation withdraws grants even before the returned future is first polled. Both root QUIC fixtures now use enrollment-issued peer credentials committed through the actual root replicas; a TLS grant alone cannot substitute for those records. The only lockfile dependency change is the internal ledger-to-directory edge; the external package inventory is unchanged. The existing single physical fleet queue allowance remains shared, with no per-session `Arc` wrapper added. Live committed-assignment orchestration, verified-through custody evidence, stronger durability activation and deployment-scale qualification remain open.


The running service now uses `ManagedService` on every node. Tenant authorization precedes a short lookup in the existing bounded installation watch; the selected incarnation remains fixed through receipt preflight, evidence admission and submission. Custody requests can reach a copy-only node. Joining creates an empty managed fleet and evidence coordinator, with no application assignment. Shutdown first quiesces installation/routing, stops one current replica at a time and then stops the physical fleet. There is no cloned fleet-wide host list or await under a routing borrow.

The founder now commits the first all-range directory delegation and its exact single-founder group grant through the existing durable root intent stream. Unknown region is explicit and requires no invented region record. `ControlHost::prepare_directory` waits for a new root ReadIndex barrier and then returns an opaque owned permit binding committed delegation, immutable genesis, membership and live enrolled identity. Its physical directory owner performs recovery and authority activation on its own thread, using the same WAL as root and sessions. The service registers that owner before yielding and publishes `Ready` only after its activation has applied. Reopening preserves group identity and reconciles interrupted activation using one deterministic retry client. The directory owns session metadata; the root retains only delegation/group metadata. This is one bounded initial owner, not an unbounded thread-per-partition design.

`Session::checkpoint_evidence` preserves the exact durable checkpoint bytes and an immutable graph lease. The existing content owner advances bounded verification steps over retained artifact references and content chunks before installing the checkpoint and prefix manifest. The resulting `VerifiedCustody` is an opaque local witness: checkpoint, group/genesis, route, placement/membership epochs, sequence and Raft prefix remain bound together. An expected active custody scope is checked throughout verification without replacing the active serving policy. A prefix captured before later writes cannot prove their final cutover. Caller cancellation drops the owned continuation and its leases; restart must verify again before obtaining a trusted witness.

The next placement slice must (1) commit the existing founder session’s group grant and Created fence; (2) sign and consume its actual witness in the initial directory; (3) persist/recover assignment intents before opening logical WAL leases and installing them into the managed fleet; (4) propagate authenticated authority snapshots and assigned directory routes; (5) copy and verify the exact final cutover checkpoint/content prefix; and (6) collect required live signatures, promote caught-up members, and commit directory/session activation fences before reporting stronger durability. General grouped directory ownership, delegation movement, geographic qualification and deployment plan/apply remain required. The first-directory service now refreshes its root authority both on recovery and continuously through the existing physical owner; general remote/grouped authority distribution remains open.

Directory/custody qualification on macOS arm64, 2026-09-05: the final unfiltered workspace run passes **499 tests across 48 targets**, with no failed, ignored or filtered tests. Strict workspace all-target Clippy, the separate production no-panic gate, documentation targets, formatting, architecture/import contracts and the full workspace release build pass. Added coverage includes managed QUIC evidence routing, graceful quiescing against concurrent installation, durable first-directory startup/restart and real Unix routing, fresh root quorum admission, election-time readiness, interrupted authority installation, retained physical-owner/egress budgets, unknown-geography delegation, exact-prefix content corruption/expiry/cancellation checks, and checkpoint scratch pressure. Root and partition revision comparisons precede authority-clock validation, allowing saved stale intents to replan after restart while current intents still enforce clock monotonicity. No per-session or per-verification `Arc` wrapper or external dependency was added. These results establish the tested local behavior; stronger placement and scale claims still require the remaining plan.

The first workspace run of the directory/custody slice hit the runtime quality-evaluator test’s unchanged three-second deadline while still `Validating`. The original workspace binary passed the focused case and all 26 runtime tests on rerun, and standalone runtime reruns also passed. No production cause was established. Failure diagnostics now record elapsed time, drive count, longest synchronous drive, durable sequence, pending input and run state; the deadline and assertions remain unchanged. This observation remains part of mixed-load qualification.

One exploratory run of the three fleet QUIC tests failed with quorum timeouts while separate ledger tests and compilation were also running. Unchanged isolated, grouped and concurrent ledger/QUIC reruns passed; the cause remains unconfirmed. The tests now report replica progress, peer-pool counters, last read failure and driver liveness when this recurs. Timeouts and quorum checks were preserved. This observation remains part of the open mixed-load qualification work.

Workspace qualification also exposed a stale-leader assumption in the partition-healing test. Converged replicas can elect a different leader, so the harness now retries the identical request after a transient unavailable or unknown result and rediscovers a quorum-ready leader. A forced leadership-transfer regression verifies the original receipt and unchanged session sequence. The existing deadline and full-response equality assertions remain in place.

## Recorded implementation decisions

The continuing ownership cleanup adds three bounded mechanisms. First, the first directory installs newer root authority through its existing control queue and returns only after durable apply plus a fresh directory read barrier. An appended trusted `StateAndAuthority` query exports one exact applied prefix; Node-only peer reads cannot select it. Second, the founder-session registration helper derives a stable Created intent, group grant, enrollment and signed session registration from actual recovered Session state. Tests preserve preexisting claims and reject unsupported placement or revoked identity; the service registration controller and durable assignment journal are still required before this is automatic. Third, the grouped replica checkpoint API polls an owned physical WAL rewrite, preserving unrelated writer progress while cancellation retains admitted persistence. Direct synchronous checkpoint callers still use an explicit monotone read clock.

Local root-intent journal writes now run on the existing control owner. Tests cover full/disconnected queues, payload spare-capacity accounting, canceled writes, lost completion and reopening. The full workspace run exposed stale test assumptions about resetting the snapshot clock and immediately reopening a canceled journal. Fixtures now use monotone clocks and an actual FIFO owner barrier; immediate grant withdrawal, real fsync-delay expiry, and unknown-write recovery remain enforced.

Ownership/authority/checkpoint qualification on macOS arm64, 2026-09-05: the final unfiltered workspace run passes **515 tests across 48 targets**, with zero failed, ignored or filtered tests. Strict workspace all-target Clippy, the separate production no-panic gate, documentation targets, formatting, architecture contracts and the complete workspace release build pass. The architecture check resolves **291 links**, preserves all **37 imported source hashes**, and validates **15 frozen vocabularies**. No additional thread, task, external dependency or `Arc` wrapper was introduced for directory authority refresh, private intent persistence or grouped evidence export. The physical WAL remains a FIFO durability boundary; independent writers are necessary for IO isolation. This qualifies the exercised local behaviors, not the remaining global-scale or new CLI/MCP scope.

The CLI/agent source research and P17–P20 tasks remain required work. The subsequent increments below implement the local CLI, MCP adapter and remote mutation-receipt lookup. Safe epoch-floor advancement and scoped child-cause admission remain open.

Authored requirement identity excludes the allocated `ValidationId` and the generated parent `ClaimId` used to attach a requirement to its owner. `ValidationContent::specification_hash()` covers the authored specification; a claim hashes its ordered specification digests. The stored requirement references still contain their IDs, and a full validation object hash binds its parent claim. Authored semantic references, such as dependencies on a particular existing claim, remain hashed. This prevents allocated linkage IDs from defeating content deduplication while preserving relationship integrity. Frozen fixtures and fresh-allocation dedup tests cover this distinction.

The implemented owned in-memory Raft storage follows TiKV's `raft-rs` 0.7 API; shared library `MemStorage` is not the authoritative application store. `SessionSeq` counts committed domain mutations and is distinct from the Raft index, which also includes elections, configuration changes, cursor metadata, maintenance and no-op entries.

Internal prepared mutations and checkpoints use a schema-tagged pinned postcard representation. Authored object identity uses Focal's explicit canonical encoder. Changes to Rust enum order in internal persistent structures require a new compatible decoder/migration; an enum's canonical command code alone does not make an arbitrary serializer append-safe. Full rolling-format migration remains P15 work.


## Manual CLI and bounded validation results

The local binary now accepts `submit claim`, `submit testament`, and `submit artifact` through flags, JSON, YAML or bounded files/stdin. Shared `focal-client::input` builders derive authenticated issuer/producer, root cause and pinned validation specification hashes; they reject unknown/duplicate/spoofed fields and enforce the actual reducer contract. `--target` means subject; `--source` filters issuer. `self` resolves only the selected principal, including the domain's self-work restriction.

`get/list` cover claims, testaments, artifacts and validations with no mandatory list filters. Family indexes and bounded residual visits avoid an unbounded client scan. MAC-protected list cursors bind principal, role, ledger, filters, route and exact prefix; empty filtered pages advance past visited rows. Artifact/testament filtering reads the immutable manifest. Singular filtered reads reject ambiguity and query-budget exhaustion. Validation queries return a requirement plus bounded immutable run summaries and individual verdict attempts, atomically projected with each domain publication and restored from WAL/checkpoint state. Their continuation reads the exact previous prefix. The default snapshot lease is 30 seconds; owner restart expires retained list views. One oversized result record returns Capacity without advancing its cursor.

Each manual mutation persists the fully expanded request and a distinct epoch-admission request before transmission. Owner-private, checksummed journals retain exclusive filesystem locks across waits and survive lost epoch/business replies. Fixed epoch one permits independent processes without racing an epoch-floor advance; no automatic floor/receipt GC is claimed. Only a verified committed or duplicate receipt completes an operation. `request inspect/retry` reopens exact state; missing initialized state, mismatched context and ambiguous journal writes fail closed. Journal fsync and CLI file IO run on the main OS thread between network waits. Manual commands use a current-thread async runtime rather than starting a worker pool for each process. No new async actor, worker thread or `Arc` wrapper is introduced by this adapter.

`claim post/progress/cancel`, `receipt acquire`, and `evidence begin` expose existing domain operations. Artifact downloads use at most 64 KiB network pages, server manifest/chunk verification and exact client offset/length/EOF checks; complete bytes are synced and atomically linked into a new private output file without overwriting an existing path. The content root addresses a manifest rather than raw byte concatenation. Independent client manifest-proof export and convenient resumable uploads remain open. JSON output uses fallible typed envelopes for paths and object results, retaining exact Unix path bytes. Joined-node local contexts explicitly reject before transmission until the adapter can resolve that node's actual authenticated principal and ledger.

The [manual guide](../manual-cli.md) documents executable examples and limits. This increment does not automatically acknowledge a testament or run work validators in the foreground service. `TestamentGenerated` remains distinct from satisfaction. MCP/skills, complete challenge/consult workflow policy and corrective/follow-up issuance, remote contexts, remaining cluster operations and deployment/scale gates are still required.


Full-suite qualification exposed and deterministically reproduced a control-owner shutdown race: committing an authority refresh during the final drain could queue a follow-up ReadIndex after the drain, leaving a new Raft Ready that invalidated the final checkpoint index. Stop now releases pending caller/read-refresh interest before its final drain, while already admitted replica proposals remain owned and durable. The regression proves that the refresh commits, checkpointing succeeds and restart returns the exact original receipt. No test retry or timeout was widened to hide the failure. Response validation also binds result rows to the immutable validation phase and pinned handler contract; cached graph row charges bound source work before result serialization or cloning.


Final manual CLI qualification on macOS arm64, 2026-09-05: **561 tests across 51 all-target workspace targets pass**, with zero failed, ignored or filtered tests. Strict workspace all-target Clippy, the separate production no-panic gate, documentation tests, formatting and the complete workspace release build pass. New coverage includes flags/JSON/YAML canonical parity through the executable, all four optional-filter list families, exact testament manifests, validation run/verdict pagination across restart, source-query ambiguity, private journal corruption/lock/fsync recovery, lost epoch/business replies, atomic artifact publication, path handling and the deterministic control-owner shutdown regression. macOS rejects invalid-UTF-8 filenames before transmission; lossless JSON path serialization is tested without filesystem access, while the successful non-UTF-8 filesystem workflow is present for other Unix hosts and was not executed here. No external dependency version or package was added; Cargo.lock changes only dependency edges among packages already present. This is correctness/build evidence for the implemented scope, not global-scale or completed P17–P20 qualification.


Final architecture checks resolve **309 links**, preserve all **37 imported Hecate source hashes**, and validate **15 frozen vocabularies**. [MCP protocol research](14-mcp-protocol-research.md) records the verified primary revision, compatibility choice, Rust ownership/transport tradeoffs and future conformance gates; it does not claim an implemented MCP server.


The local MCP increment adds `focal mcp serve` over the existing authenticated Unix client. The shared Rust registry contains 16 authored operations and exhaustive coverage metadata for the 29 model commands plus wire/query/stream/upload/custody families. CLI lifecycle/read/list construction now uses those same builders. MCP adds `request.inspect` and `request.retry`, with caller-selected durable operation IDs independent of JSON-RPC IDs. Flags, JSON, YAML and MCP continue to compile into the existing frozen wire types; this increment adds no model or command ordinal.

A private operation-ID store binds cluster/principal/ledger, operation version, authored intent and expected revision before generating IDs. It persists complete expanded epoch/business requests before transmission, retains exact replies in ordinary CLI journals, and conservatively reserves aggregate disk quota before admitting another ID. Initialization faults after complete prepared requests recover those same bytes; an incomplete durable ID claim without them fails closed. A separate context-bound bootstrap marker prevents a missing initialized store from silently resetting retry identity. Root catalog locks end before network waits; each operation owns its own journal lock. No automatic history eviction or epoch-floor advancement is introduced.

The MCP process implements the pinned `2026-07-28` profile and explicit `2025-11-25` compatibility. Its protocol owner handles bounded framing, strict JSON structure, catalog pagination, active request IDs and cancellation. Input, ledger execution and output have separate owned threads and bounded channels. JSON serialization retains the actual response allocation through write/discard, including duplicate text and structured content. One business operation runs at a time while protocol control remains responsive. EOF and failed transport stop the process through the documented bounded shutdown seam. The [protocol implementation record](14-mcp-protocol-research.md#7-local-implementation-boundaries) records admission limits and the distinction from measured RSS.

Exact object read responses now bind returned IDs/families/order to the requested references, reject unsolicited duplicates and continuations, and bind route/prefix fences. This closes a shared client verification gap found during MCP review. Existing repeated-request-reference semantics remain intact.

The executable MCP workflow tests cover both profiles, all four read/list families, claim posting, receipt acquisition, progress, artifact/testament submission, immutable manifest binding, changed-intent/revision rejection, and CLI replay of an MCP journal with the identical receipt. Another test discards the first response, kills both processes, and recovers the same generated occurrence and single stored claim. Controlled transport tests apply a mutation through the actual Core while withholding the reply; cancellation suppresses its response, inspection retains the exact pending request, and restart recovers the original duplicate receipt without advancing Core sequence. Protocol tests pin upstream fixture digests and exercise malformed/oversized input, stale tokens, cursor fences, memory pressure and serialization failure.

The released thin skills cover claim authoring and evidence/testament submission using discovered operation schemas. Their required tool names/versions and content are pinned by a local manifest contract test. Challenge proof obligations and normal consult follow-up are recorded as behavioral requirements; automatic remediation, trusted child-cause issuance, durable continuations and the associated end-to-end policy tests remain P20 work.

This is verified local adapter progress, not completion of P17–P20. Remote credential contexts, capability-specific discovery, complete nested generated output schemas, external MCP client-distribution interoperability, large-content MCP transfer, evaluator/runtime/admin tool surfaces, safe request-history retirement, and all remaining cluster/deployment/scale qualification remain open. The MCP/client layers add no explicit `Arc` wrapper or external dependency package. The accompanying network shutdown fix replaces Quinn’s required runtime `Arc` allocation with a delegating runtime carrying an owned release signal; this is a dependency-required shared lifetime, documented in [the ownership policy](10-ownership-and-failure-policy.md).


Qualification also found and corrected a real network shutdown race: Quinn can retain a UDP socket after its public endpoint has closed and its connections have left the drained map. The shared-port listener now observes final socket-owner release through Quinn's required runtime handle, with an owned completion allowance. Physical ledger owners stop first; listener release remains inside the existing shutdown deadline. The receiver survives cancellation and repeated shutdown is idempotent. Quinn is pinned to the audited 0.11.11 version, and its socket/runtime drop ordering must be reviewed on upgrade. Tests transfer a held UDP reservation into startup, exercise real peer traffic, and rebind immediately after successful shutdown without port-selection retries or sleeps.

Additional operation-store fault tests cover the create-no-clobber hard-link window and partial initial journal creation. Recovery accepts only the expected same-directory temporary/destination inode pair, with exact private ownership, two links and a verified bounded frame before removing the temporary. An unready journal with no initialized record may resume from already saved complete envelopes; valid existing receipts survive, and corrupted, ready or initialized missing state is never reset. These tests reproduce the actual intermediate filesystem states rather than only injecting errors after complete high-level writes.


Final local CLI/MCP qualification on macOS arm64, 2026-09-05: **616 tests across 55 targets pass**, with zero failed, ignored or filtered tests in the unfiltered workspace run. Strict all-target Clippy, the separate production no-panic policy, documentation targets, formatting, architecture contracts (**321 links, 37 imported hashes, 15 frozen vocabularies**) and the complete workspace release build pass. The external dependency package inventory is unchanged; Quinn's existing resolved version is now explicitly pinned for its audited shutdown lifetime. This increment is **verified progress** toward the active P00–P20 goal; the open requirements above remain required work.

## Authenticated request reconciliation

The next P17.10 increment appends `Operation::Reconcile` and `Response::Reconciled`. It queries the authenticated principal's epoch window or complete request key at a new quorum ReadIndex barrier. Replica owners bind the result to their current term, serving route, committed domain sequence and applied Raft index. The single-voter host uses the same query after its own read barrier; managed routing preserves the selected physical owner incarnation. Callers cannot select another principal or downgrade consistency to an old graph lease.

The lookup spans both retained domain and cursor mutation receipts. Cursor metadata has a separate revision and can advance without changing the domain sequence, so the reply includes the actual applied Raft index. A cursor reply copies the original committed result, including its exact position, filter, mode and expiry; it never substitutes today's consumer state. The model's read DTO avoids a dependency cycle with the stream executor. Core and Session borrow retained state until response admission and copy only the bounded result. No reconciliation path clones an entire epoch set, graph, cursor registry or ledger.

Retained results win even below the epoch floor. `BelowFloor` means that new admission is fenced at the committed prefix, with historical outcome unknown. All other missing results remain `Unknown`, including pending uncommitted proposals. Quorum loss, a stale owner/route or capacity exhaustion returns an operational error. The client validates query, principal, ledger, request key, route, sequence and cursor application bounds before exposing a result.

The CLI exposes `request status`, `request epoch` and `request inspect --remote`. MCP exposes shared `request.status`/`request.epoch` operations and optional `remote: true` on `request.inspect`. Remote journal inspection always queries the business request, even if local recovery still awaits epoch admission. It checks a returned domain receipt's command hash and any saved receipt, rejects cursor-family collisions, and never advances or clears the journal. Exact retry remains the action that persists recovered receipts. Existing local inspection remains available without a remote request. The packaged skill contract now covers all 20 advertised tools and preserves these uncertainty rules.

Safe concurrent ownership and retirement of each principal's epoch stream remain P17.11. Read-only observation cannot by itself authorize epoch-floor advancement, acknowledge another process's request or garbage-collect its recovery journal. Remote credential selection, broader lifecycle/workflow integration and the complete P00–P20 deployment gates remain required.

Qualification on macOS arm64, 2026-09-05: **642 tests across 56 targets pass**, with zero failed, ignored or filtered tests. The complete invocation is `bash scripts/cargo.sh test --workspace --all-targets --locked --offline -- --test-threads=4`. Strict workspace all-target Clippy, the separate production no-panic gate, documentation targets, formatting, the complete workspace release build and architecture contracts (**325 links, 37 imported hashes, 15 frozen vocabularies**) pass. The 203-package external dependency inventory is unchanged. Reconciliation adds no explicit `Arc`, worker, actor, thread or dependency package. This is verified progress; only P17.10 is newly checked complete, and the full P00–P20 goal remains active.

Qualification also exposed timing sensitivity in existing physical-owner tests under unrestricted parallel execution. One full run missed the independent-writer 300 ms deadline with both sessions still live; an earlier export returned `OutcomeUnknown` with a 500 ms request deadline. Focused cases passed unchanged, as did a four-thread full node control. Tracing did not distinguish OS scheduling from shared physical IO contention, so no release-path defect or fix is claimed. The checkpoint test now uses a bounded test-only notification after successful preparation and the first pending WAL poll, replacing a generic progress notification that did not establish actual checkpoint admission. All 250/300/500 ms deadlines and the independent-writer assertion remain unchanged; diagnostic traces were removed. The final full workspace qualification uses four test threads to control concurrent physical fixtures. Unrestricted-parallel timing stability remains a qualification limitation.

The namespace audit also refines P17.11's implementation order in the plan. Legacy raw requests remain separate from new managed streams; ACKs and seals must cover both outcome families, and bounded slot reuse needs generation fencing. Client operation IDs require their own durable binding and explicit retirement contract. Deleting an acknowledged journal and accepting its ID as new would violate exact retry even if the server had safely retired the old receipt.

## Managed request streams: implementation in progress

The managed increment implements the model, scoped Core reduction, Session registry,
durable WAL/checkpoint state, protocol-two transport and private client store
described in [15](15-managed-request-streams.md). Domain and cursor outcomes share
a bounded ordinal window. Registration uses an exact generation CAS; receipts
retain their original key, intent, outcome and applied index. Acknowledgment
requires hashes of complete, locally durable receipts before atomically retiring
the prefix. Sealing either returns the earlier committed outcome or commits an
admission fence. Closing preserves the used generation. Ordinary outcome
insertion does not advance the separate control CAS revision, and close does not
require incrementing that revision at its maximum value.

The filesystem store reserves canonical `m1` IDs before expanding a command,
publishes immutable request and receipt bodies before catalogue completion flags,
and persists its retired prefix before deleting bodies. Short filesystem locks
serialize concurrent processes without surviving network waits. Recovery resumes
bounded cleanup; missing completed state fails closed. Legacy journals and raw
protocol-one IDs retain their prior encodings and semantics. Automatic local store
selection and registration now use the shared coordinator described below.
Automatic close and rotation remain open.

Activation additionally requires an irreversible durable decoder promise. The
actual application confirms its compiled fingerprint, the physical writer fsyncs
the logical WAL floor, and only then can the Session advertise support. The first
activation requires support from every voter, including both joint sets. Later
membership changes guard learner addition and promotion. A committed activation
therefore permits ordinary quorum availability after restart, without requiring
every existing voter to be reachable again. Learners persist their own floor
before accepting their first managed entry or snapshot. Untouched legacy groups
remain free of the new floor; this first floor is immutable rather than a general
multi-format upgrade protocol.

Compatibility qualification uses an actual separately built binary from commit
`974c8b52efd34031fd08e1ae8de145319ee49f63`. Before managed activation and again
after checkpointing, that older binary refuses a ledger containing the durable
decoder floor with a typed WAL corruption error and leaves every stored file's
hash unchanged. Conversely, it opens an untouched legacy-format ledger created
by the current code, completes the claim/artifact/validation workflow through
domain sequence 13, and leaves state the current code can recover and checkpoint
without introducing a floor. The isolated fixtures and machine-readable evidence
are `/tmp/focal-legacy-qualification.ZAvfzH`,
`/tmp/focal-managed-real-downgrade.json` and
`/tmp/focal-managed-real-legacy-parity.json`. These are local qualification
artifacts, not committed dependencies.

Qualification also exposed missing snapshot transport feedback in the existing
replication driver. A canceled, locally rejected or remotely rejected transfer
could leave Raft's peer progress paused indefinitely. Ledger and cluster-control
frames now carry owned completion senders, with bounded receiver metadata in
their originating physical owner. Failed admission or sender loss reports
failure; successful remote ingress reports transport completion. Current term,
snapshot index and receiver replacement fence obsolete completions. Retryable
reporting failure retains the completed status for the next owner turn. The frame
and receiver each retain sufficient accounting through their independent
lifetimes, without another shared wrapper or completion ingress queue.

Qualification on macOS arm64, 2026-09-06: **698 tests across 57 targets pass**, with
zero failed, ignored or filtered tests, using
`bash scripts/cargo.sh test --workspace --all-targets --locked --offline -- --test-threads=4`.
Strict all-target workspace Clippy, the separate production no-panic gate,
documentation targets, formatting and the complete workspace release build pass.
Architecture checks resolve **333 links**, preserve all **37 imported Hecate
source hashes**, and validate **15 frozen vocabularies**. The external dependency
inventory remains the same 203 packages. The managed stream and snapshot-feedback
paths add no explicit `Arc` wrapper or new physical owner/thread. Qualification
logs are `/tmp/focal-managed-final-{tests,clippy,production,doctests,format,contracts,release}.log`,
with counts in `/tmp/focal-managed-final-counts.json`.

Coverage includes independent processes reserving distinct durable ordinals,
request/receipt publication crash cuts, exact lost-control retry, shared
domain/cursor acknowledgment at capacity, seals across a leader change, generation
reuse, checkpoint plus WAL-tail recovery and unchanged legacy receipt recovery.
The real three-voter QUIC workflow restarts with one voter isolated, recovers an
exact ACK and retired-request result, and commits a new managed domain request.
Existing learners recover through both entries and V5 snapshots after persisting
their floor. Physical-owner tests prove snapshot-specific local rejection,
cancellation, remote refusal, same-term retransmission, obsolete feedback fencing
and charge release; a test-only counter observes the actual snapshot rejection
branch without changing production fields or fixture deadlines. The earlier
unrestricted-parallel timing limitation remains open.

P17.11 remains unchecked. Its open gates include bounded automatic close/rotation,
managed domain epoch batching, sustained principal churn across finite registry
slots and the full adapter recovery qualification. General trusted assignment proof for a
fresh learner whose bootstrap membership excludes today's leader and future
persistent-format transitions also remain required. The full P00–P20 goal remains
active; this increment does not establish the deployment or scale gates.

## Automatic local managed requests and lifecycle audit

Normal CLI mutations now use the shared durable ownership coordinator. First use
selects a free slot and registers it; subsequent calls reserve and prepare
internally. Successful output shows object IDs; unresolved work receives a copyable
recovery command. Normal use needs no
epoch or stream configuration. A successfully flushed result is marked delivered
on disk before maintenance ACKs the contiguous delivered prefix. A broken output
stream, timeout or noncommitted domain result leaves its exact operation pending;
later successes cannot retire that gap. Cleanup failure after successful output
is deferred. `request pending/inspect/retry` recover IDs; `request seal` (alias
`abandon`) explicitly fences a request without canceling a business claim.

MCP uses a separate managed store with explicit `request.reserve`, discovery and
consumed-result `request.acknowledge`. Reservation executes no business work;
lost reservation output is recovered through `request.pending`. Tool completion,
inspection, retry and cancellation do not acknowledge consumption. Sealing returns
the earlier result or a durable fence and likewise needs explicit consumption
ACK. CLI discovery can recover either local store. Legacy unqualified IDs, raw
protocol-one requests and explicit path journals preserve their semantics. The
server's managed persisted format is unchanged by this adapter increment.
[15](15-managed-request-streams.md#automatic-local-ownership-and-result-delivery)
records the delivery contract and remaining close/rotation limits.

The new default managed path requires an owner-private data directory (`0700`).
Existing permissive roots are rejected with an actionable error; no permissions
are silently changed. Explicit legacy path behavior remains available. The
[manual guide](../manual-cli.md) documents this compatibility boundary.

Focused CLI qualification passes all eight existing/new manual tests, including
40 ordinary mutations across the default 32-request window, broken stdout,
unobserved-prefix retention, WAL restart, exact retry and flags/JSON/YAML parity.
This is separate from the **698-test prior qualification record above**. The
subsequent offline/locked workspace all-target run passed **731 tests across 58
targets**, with no failures, ignored tests or filtered tests, using four test
threads per target. Its five real MCP executable tests include lost responses,
cross-adapter recovery, independent result consumption, retirement across the
bounded window, and evidence lifecycle checks. The final supersession admission
guard extension separately passes the complete 66-test Core suite; the historical
decoder/replay behavior remains unchanged.

The final human-output cleanup separately passes all eight manual/managed CLI
integration tests. Successful commands leave stderr quiet. A failed output stream
retains its exact receipt and prints recovery guidance; a closed stderr cannot
prevent submission, replace a domain error or retire an unobserved result.

The same increment passes strict workspace all-target Clippy, the production
no-panic/unchecked-operation lint gate, formatting, doctests, and the offline/locked
optimized `focal` build. The release executable's claim help also succeeds. Updated skill
content and its pinned hashes pass both client and MCP contract tests. Logs are
`/tmp/focal-managed-adapters-final-{tests,clippy,production,doctests,format}.log`,
with separate `client-skills`, `mcp-skills`, `release` and `help` logs under that prefix. This is
regression evidence for the implemented increment, not global-scale qualification.

A fresh Hecate/Sylk lifecycle audit found and closed an admission gap: a Receipt
requirement could declare evidence or a quality bar that its automatic delivery
Pass did not evaluate. New client/Core admission now restricts Receipt to pure
whole-work delivery; evidence and quality require separate Test, Inspection or
Contract requirements. New acknowledgment/whole-work begin/completion on a
historical malformed Receipt contract also fail explicitly. Previously committed
versioned intents replay their original result, including historical Pass; the
fix neither rewrites old truth nor makes unrelated WAL history unrecoverable.
The focused Core suite passes 66 tests, including prior-format legacy/managed
prepared-intent replay and checkpoint recovery, pure Receipt completion and the
separate deterministic-then-agentic quality path.

The source audit does not establish P20 completion. Trusted child-cause admission,
challenge target/proof contracts, authorized corrective versus consultation
follow-up issuance by authorized participants, and authenticated peer evaluator submission remain required.
See the [source-to-implementation traceability](13-cli-and-agent-implementation-plan.md#source-lifecycle-traceability-2026-09-06).

## Coherent validation context and lifecycle contract integration

The architecture now specifies separate transition/authority tables, exact
per-artifact targets, multiple response cycles, atomic aggregate consequences,
short-circuit and late-result rules, and the asymmetric result-artifact/result-
testament audit path. The first response's generation advances its claim to
TestamentGenerated while attaching its work artifacts; response posting remains
a later, independent fact. Agentic-only checks need no synthetic programmatic
Pass. A declared programmatic-plus-quality check still requires the real
programmatic Pass before quality evaluation. Participants execute their own
capabilities; no Focal launcher or mandatory evaluator subclaim is introduced.

The storage plan preserves frozen historical DTOs, hashes, prepared-command
replay semantics and exact receipts. A bounded successor decoder requirement and
committed per-group activation precede new lifecycle formats. In particular,
historical open evidence sets cannot be migrated to Attached by interpreting the
old ArtifactAttached event as testament attachment. L1 executable transition
fixtures and L2–L8 implementation/qualification remain open.

The additive implemented read is `Client::validation_context`, exposed as
`focal get validation ID --context` and MCP `validation.context`. It uses at most
three existing reads and returns a pinned requirement, its owning claim, optional
current closing testament with exact manifest, and a bounded run/verdict page.
Every component has the same token; cursor expiry fails without a fresh-prefix
restart. Initial absence differs from a missing referenced parent. Existing
receipt adoption does not rebind old testament/run evidence. Both aggregate JSON
size and total wait time are bounded; owned/borrowed values add no explicit Arc.
No new persisted or wire format, managed mutation ordinal, execution lease or
inferred artifact target is introduced.

The catalogue now contains 19 authored operations plus six recovery tools. Both
packaged skills are version 3 with pinned content hashes and the new read listed
as an actual dependency. Twelve focused SDK tests and fourteen real CLI/MCP
integration tests pass, including both MCP protocol profiles, exact pagination,
CLI/MCP context parity, before/after testament closure, missing data, and restart
with expired cursors. The prior 731-test qualification above remains the record
for the earlier managed-adapter increment.

Final context-increment qualification, macOS arm64, 2026-09-06: the offline/locked
workspace all-target run passes **744 tests across 58 targets**, with zero failed,
ignored or filtered tests. Strict workspace all-target Clippy, the production
no-panic/unchecked-operation gate, doctests and formatting checks pass. The
architecture checker verifies 434 links, 37 unchanged imported source hashes and
15 frozen vocabularies. The complete workspace run includes both updated skill
contract tests. Logs use `/tmp/focal-lifecycle-context-` with `workspace.log`,
`counts.json`, `adapters.log`, `unit.log`, `clippy.log`, `production.log`,
`doctests.log`, `format-check.log` and `contracts.log`. These results qualify the
implemented snapshot-inspection increment; they do not qualify new lifecycle
mutation formats or global-scale deployment.

The optimized `focal` binary also builds offline/locked and exposes the new
`get validation --context` option in its actual help. Release build and help logs
are `/tmp/focal-lifecycle-context-release.log` and
`/tmp/focal-lifecycle-context-release-help.log`.

## Current CLI/MCP and README qualification

The shared authored registry now contains **34 operations**. MCP can additionally
expose six managed-recovery, five artifact-transfer, four durable-watch and
32 local-administration tools when their corresponding backends are present.
Four packaged skills pin the live contracts: claims version 7, evidence version 6,
validation version 2 and cluster version 1. The validation instructions explicitly
distinguish the supported agentic check from the still-unsupported agentic-only
`quality_bar` admission, and never ask a participant to invent a programmatic Pass.

The implemented interface includes atomic claim batches, authenticated peer
evidence and verdict submission, optional filters for every list family, bounded
singular claim selection, validator-contract reads, fixed-prefix graph traversal,
durable watches, a bounded read-only claim wait, and explicit durable monitors.
JSON/YAML and field flags share typed builders and revision/run fences. Offline
schemas, examples, input validation and frozen request files use that same
contract. Help and all five shell completions expose the filters applicable to
each family; unsupported filters still return the structured input error.

Named local/remote participant contexts preserve their own request and upload
history. Large transfers retain exact source bytes and final command identity;
inspection and cancellation preserve unknown outcomes and server terminal fences.
Root and installed-application administration use their own configuration-fenced
durable identities. A joined physical owner exposes authorized local diagnostics;
that does not give it the founder's invitation-signing key or Runtime standing.
These behaviors, bounded output/backpressure and restart/reconciliation are
covered by executable SDK, CLI and MCP tests. The live contract and remaining
limitations are in [19](19-cli-mcp-implementation.md), [the manual](../manual-cli.md),
[MCP documentation](../mcp.md), and [cluster administration](../cluster-admin.md).

The [project README](../../README.md) now explains the peer domain and roles,
source installation, actual local defaults, a live quickstart and resumable demo,
human input formats, MCP/skills, memory and disk durability, and deployment steps.
Three Mermaid diagrams show peer evidence flow, the current claim lifecycle and
the commit/publication path. The independent four-family lifecycle and geographic
deployment remain explicitly identified as targets. The diagrams were reviewed
against source; no rendered Mermaid visual check is claimed.

The first storage-migration prerequisite is also implemented: Core checkpoint and
Session legacy/managed prepared-entry paths use an explicit top-level V1 codec.
It preserves existing writer bytes and checks schema before nested decoding,
requires complete input consumption, borrows maps for encoding and moves decoded
allocations into the authoritative Core. Twenty-three fixed original-writer
fixtures retain actual checkpoint, input, receipt, delta and effect bytes; their
generator and capture/source provenance are checked in. Tests cover replay at
each restart boundary, exact duplicates, malformed/trailing data and failure
before Core/graph/managed-receipt publication. This freezes the exercised envelope
and State layout, **not** every nested model DTO or the historical reducer.
[The storage contract](18-lifecycle-storage-upgrade.md) retains those requirements
and the successor floor/activation/migration work.

The final all-target workspace run on macOS arm64, 2026-09-06, passes **888 tests
across 66 targets**, with zero failed, ignored or filtered tests. The invocation
is `bash scripts/cargo.sh test --workspace --all-targets --no-fail-fast --offline --locked -- --test-threads=4`.
Strict workspace all-target Clippy, the separate production no-panic and unchecked-
operation gate, doctests, formatting and `git diff --check` also pass. The
architecture checker verifies **454 links, 37 unchanged imported source hashes
and 15 frozen vocabularies**. The complete workspace release build passes.

The resulting optimized binary passes 22 README smoke-check commands in fresh
private temporary directories: local startup, generated example submission,
exact get/post and filtered listing, summary counts, shutdown/restart with the
same posted claim, flags/JSON/YAML submission as three distinct durable intents,
all four unfiltered lists, schema discovery and help. Running the demo twice
recovers identical JSON with the same claim and proof. The live check report is
`/private/tmp/focal-readme-smoke.json`; the release build log is
`/tmp/focal-cli-mcp-release.log`. These are local checks of the documented commands,
not a clean-host dependency installation or rendered diagram test.

Two control-host setup calls initially returned the protocol's `OutcomeUnknown`
under concurrent fixtures. Their isolated original eight-test suite passed.
The fixture now retries the **same** request ID and content, rediscovers the
quorum-ready leader, and verifies the committed receipt within one bounded setup
deadline. Minority, authorization and failure assertions retain their direct
calls; production timeouts and retry behavior were not widened. Two stale CLI
expectations were also corrected: recovery commands include their saved client
context, and joined nodes have a local administration socket. Their tests still
check exact request recovery and refusal to forge privileges or issue invitations.

The dependency inventory contains 206 external packages. The cached RustSec scan
passes advisories, bans, licenses and sources with ten existing duplicate-version
warnings and no suppressed advisories; it is not a fresh online audit. Local logs
use `/tmp/focal-cli-mcp-` with `workspace.log`, `counts.json`, `clippy.log`,
`production.log`, `doctests.log` and `dependencies.log`. These results establish
the exercised current profile. Independent object lifecycles, typed external
validator definitions, challenge/consult continuations, trusted child causes,
deployment activation, credential renewal, request-history reclamation and the
six deployment journeys remain required work in the active full plan.

## Nested V1 storage and command identity

The complete Core checkpoint graph now uses explicit historical model codecs:
all four objects and lifecycles, relations/scopes, evidence sets, validation
runs/attempts, monitors, identity indexes, epoch windows and retained legacy
receipts. Scalar vocabulary codes and Postcard variant ordinals are frozen
separately. Borrowed views encode current rows without cloning their data;
decoding moves each row into its final map or vector. Untrusted collection
counts do not authorize eager vector allocation, and content-reference length
remains metadata. Historical absent states, manifest order, duplicate collection
semantics and stored hashes are preserved rather than normalized by admission.

Both prepared command families use the same frozen nested codecs for all 29
commands, new-object inputs, authority/custody facts and request identities.
Legacy and managed domain intent hashing now share one explicit V1 algorithm,
including the original domain separator, big-endian fields, fixed schema/tag,
body length and exact Postcard body. Legacy hashing serializes the body once
and hashes it directly, retaining one body vector and eliminating its former
second preimage vector. Managed hashing retains its allocation-free streaming
path, with a fixed 1 KiB stack buffer for scalar bytes and direct bulk byte
updates to BLAKE3. Other managed receipt/control hashing retains its original
serialized contract while sharing the buffered digest implementation.

The existing actual workflow fixture set remains unchanged. Two additional
corpora preserve bytes captured from the pre-change compiled writers, with
generator sources and source/rlib/executable identities. Fifteen nested data
files cover every current nested enum and collection family, including 20 claim
statuses, 42 validation kind/phase/mode combinations and all twelve command
results. Seventy-one command data files cover 58 populated/alternate command
shapes and both input/prepared/hash families. These are serialization-valid
synthetic values, deliberately including states not admissible as new work;
they are codec/hash evidence, not fabricated reducer histories or service-ready
snapshots. The [storage contract](18-lifecycle-storage-upgrade.md) links both
corpora and describes their exact scope.

The old `FOCALSS1`/`FOCALSS2` readers now reject trailing bytes before publication.
V1 retains its initial decoded Core to establish its missing prefix and complete
restoration, eliminating a second full decode. Tests restore both original
envelope formats, preserve the exact Core and duplicate receipt, and reject
suffixes without changing the live Core/graph. No new object, command, wire,
receipt, checkpoint or decoder-floor encoding is introduced by these changes.

The all-target workspace run passes **916 tests across 66 targets**, with zero
failed, ignored or filtered tests, on macOS arm64, 2026-09-06. The invocation is
`bash scripts/cargo.sh test --workspace --all-targets --no-fail-fast --offline --locked -- --test-threads=4`.
That includes **164 focused tests**: 31 Model, 76 Core and 57 Ledger.
Coverage includes every original command tag and top-level field, both saved
canonical hash families, all nested checkpoint maps, complete-body/schema
rejection, original mixed legacy/managed replay after every checkpoint, exact
receipts/deltas/effects, and buffered hashing across scalar/bulk boundaries.
Strict workspace all-target Clippy, the separate production no-panic and
unchecked-operation gate, formatting and `git diff --check` pass. The complete
workspace release build passes. Documentation checks complete for all 19
library targets (currently zero doctest examples). The architecture checker
verifies **458 links, 37 unchanged imported source hashes and 15 frozen
vocabularies**. Local evidence uses `/tmp/focal-v1-codecs-` with `workspace.log`,
`counts.json`, `clippy.log`, `production.log`, `doctests.log` and `release.log`.
No dependency package or explicit `Arc` is added by this storage increment.
The decode-budget review traces the existing checked `snapshot_bytes * 64 + 4096`
Recovery/Completion reservation through restoration and the separately charged
published Core/graph; this is a source audit, not a new measured memory bound.

An optimized local hashing comparison on Rust 1.94.1/macOS arm64 verifies all
58 saved identities against the original algorithm, the new legacy path and
the managed path before timing. Nine rotating samples of 1,000 warm calls give
the following representative medians. These measure hashing CPU time only;
the structured command corpus is synthetic, and the 64 KiB inline case exceeds
the default inline admission limit.

| Input | Original legacy algorithm | Frozen legacy path | Frozen managed path |
|---|---:|---:|---:|
| Tiny operation | 304 ns | 329 ns | 281 ns |
| Rich structured claim | 10.666 µs | 11.984 µs | 17.844 µs |
| Captured artifact | 790 ns | 804 ns | 819 ns |
| Derived 4 KiB inline artifact | 3.778 µs | 3.860 µs | 3.746 µs |
| Derived 64 KiB inline artifact | 46.069 µs | 28.475 µs | 27.224 µs |

The legacy path avoids a second command traversal and preimage allocation, but
the frozen structured encoding still costs about 12% on the rich-claim case.
Managed hashing retains its two traversals to avoid a payload-sized allocation;
its column is not a comparison against the original managed algorithm. Bulk
byte serialization and buffering remove the much larger regressions found in
the first streaming implementation. This does not establish application
throughput or allocation bounds. Exact executable/library identities, benchmark
source and both final outputs are in the local
`/private/tmp/focal-command-hash-final-bench*` artifacts.

L2 remains open. Historical reducer/admission execution, surrounding Session
metadata/delta/effect/managed-receipt formats, original key ordering and successor
floor/activation still require their explicit version boundaries and checks.
The implemented codecs do not enable new lifecycle states or complete L1–L8.

## Historical V1 execution

The Core now selects execution from each prepared entry's explicit schema.
Direct, tracked and serial-oracle apply, pending audit, epoch planning, scoped
workers and serial fallback use the same selected V1 rules. Managed replay and
audit carry an explicit `Replay(Version)` through staging; new proposals select
`AdmitV1`. The original reducer, validation scheduling and graph implementations
belong to [execution_v1](../../crates/focal-core/src/execution_v1.rs), while the
stricter current Receipt policy belongs to the separate admission module. This
removes dependence on a future process-wide default without copying the reducer
or adding another mutable state owner.

[Immutable model semantics](../../crates/focal-model/src/semantics_v1.rs) pin the
original terminal/activity classification, lifecycle event mapping, verdict
severity, typed relation interpretation, all 29 command revision targets and
managed key validity. Existing public convenience methods delegate to those
rules. The manifest hash now pins its original schema header explicitly. Domain
content admission, generated testaments, deltas and prepared records retain
schema 1. Historical command-size guards use the already frozen input codecs.
Old namespace, actor, receipt and evaluator checks still run inside the reducer;
the new Receipt admission policy is not retroactively applied during replay.

Both legacy and managed pending candidates reject unknown prepared schemas
before acceptance or publication. Candidate reuse still checks its original
basis, hashes and output provenance. Changed owner limits cannot authorize
publication of an earlier candidate. There is no new serialized profile field,
wire operation, decoder-floor promise, lifecycle activation or daemon executor.
No dependency, production panic or explicit `Arc` is introduced.

The all-target workspace run passes **931 tests across 66 targets**, with zero
failed, ignored or filtered tests, on macOS arm64, 2026-09-06. The invocation is
`bash scripts/cargo.sh test --workspace --all-targets --no-fail-fast --offline --locked -- --test-threads=4`.
That includes **179 focused tests**: 39 Model, 83 Core and 57 Ledger.
Eight new semantic tests cover all twenty claim statuses, four verdict severities,
all fifteen relation kinds against typed target alternatives, ordered relation
lookups, both rounds of the 29-command corpus, managed identity boundaries and
fixed manifest hashes captured before extraction. Seven new Core tests cover
the original admitted mixed history through direct, epoch and forced fallback
execution; one unpublished legacy/managed pending prefix with independent
audits; historical stronger Receipt replay; unknown prepared/candidate schemas;
and changed-owner provenance rejection. Exact checkpoint, receipt, delta and
effect bytes remain equal to the captured workflow. The stronger Receipt test
is a differential extension, not a newly captured original-writer history.
Strict workspace all-target Clippy, the separate production no-panic and
unchecked-operation gate, formatting and `git diff --check` pass. The complete
workspace release build passes. Documentation checks complete for all 19 library
targets (currently zero doctest examples). The architecture checker verifies
**472 links, 37 unchanged imported source hashes and 15 frozen vocabularies**.
Local qualification logs use `/tmp/focal-v1-execution-` with `focused.log`,
`workspace.log`, `counts.json`, `clippy.log`, `production.log`, `doctests.log`
and `release.log`.

A real binary compatibility check runs the complete standalone demo with the
previous release, reopens the same durable directory with the new release, then
reopens it with the previous release again. All three JSON reports are identical:
the same Satisfied claim, sequence 13, Pass verdict, artifact reference and status
history. The report and exact executable hashes are preserved in
`/tmp/focal-v1-execution-binary-compat.json`. The previous executable remains at
`/private/tmp/focal-before-execution-v1/focal`, with its checksum and capture
metadata. This checks the exercised real workflow in both directions before a
successor floor exists; it does not qualify a future downgrade after activation.

This completes the explicit historical execution boundary, not L2 or the new
four-family lifecycle. The original content/state/output types and key orderings
remain immutable V1 contracts; successor representations and projections must
respect them. Surrounding Session codecs and acknowledgment/placement hashes,
broader historical failure/agentic fixtures, the successor floor, all-voter
activation and independent lifecycle behavior remain required. The exact next
storage boundaries are recorded in [the upgrade contract](18-lifecycle-storage-upgrade.md).

## Executable successor lifecycle contract

The [new model module](../../crates/focal-model/src/lifecycle/mod.rs) implements
the successor domain contract independently from stored V1 types and historical
execution. It has no serialization, numeric tags, new service commands or durable
activation. The existing CLI/MCP surface continues to use its current profile.
No dependency or explicit `Arc` is added. Handler plans are borrowed; response
status plans move the original manifest, and bounded index/audit buffers use
fallible reservation. Full owner publication and resource permits remain separate.

The contract checks Actor roles, immutable identity/revision, execution receipts,
exact response/artifact targets, declared definitions, evaluator generations,
attempts, deadlines and expressly required policy evidence. An enrolled Node
cannot act as a participant. Artifact receipt is independent from response closing;
closing prepares an exact response and all attachment replacements together.
Posting, claimant receipt and evaluation remain distinct. An eligible evaluator's
actual checked begin advances the response/artifact/claim without another issuer
invocation. Agentic-only checks enter quality directly; programmatic-plus-quality
retains the real first-phase evidence. Only Error advances a bounded retry or
fallback. Optional outcomes stay visible without becoming required failures.

Aggregation requires one exact artifact ID/digest for all required checks of a
slot and an independently checked pure delivery result. Slot-presence declaration
indexes are explicit and collision checked. Already successful alternative slots
are considered before failing the claim; a later response cannot repair an old
terminal cut. Required checks on an optional slot can fail that artifact without
failing the response or claim. Already-begun unrelated checks can still contribute
where the claim remains open, while the failed response retains its original cut.
The incremental coverage index does not scan other response manifests per result.

Claim receipt adoption before first delivery no longer strands TestamentGenerated:
the replacement entitlement's first eligible response can advance acknowledgment
under explicit lineage, while stale response and evaluator authority still reject.
Local completion records its exact sealing position before remaining graph waits.
Audit closure binds that same position, requires complete accepted attempt history,
accounts for begun Observe checks and explicit suppression/fences, and reserves
bounded completion storage. It never invents a result for a Ready or fenced check.
Result artifacts remain evaluator-owned Generated evidence; the claimant's audit
testament ends at Posted and cannot feed ordinary response acceptance.

L1 remains open for complete Required non-artifact acceptance indexing,
DependencyFailed/Deadlocked graph witnesses and immutable successor relation
checks. The current predicate inputs are effective owner views, not wire-supplied
permissions. L2–L5 still need successor storage activation, complete histories,
creation/diagnostic cut positions, atomic core/graph/index publication, reserved
proof/history/output budgets, and the actual shared CLI/MCP mutation/read path.
The executable native exchange does not claim that end-to-end integration is done.

The full all-target workspace qualification passes **988 tests across 66 targets**,
with zero failed, ignored or filtered tests, on macOS arm64, 2026-09-06:
`bash scripts/cargo.sh test --workspace --all-targets --no-fail-fast --offline --locked -- --test-threads=4`.
The Model target passes 96 tests, including 57 new contract tests: 14 claim,
7 evidence, 18 validation, 13 aggregation, 3 audit, one shared identity guard and
one real two-Actor contract exchange. The aggregation permutations use explicitly
test-only result factories; the cross-family exchange uses actual checked begin,
receipt and report transitions. Existing historical codec/replay, storage,
network, human CLI and both MCP profile tests still pass. This is native/local
qualification, not a multi-region scale measurement or successor wire exchange.
Strict all-target workspace Clippy, formatting and `git diff --check` pass.
The test and count records are `/tmp/focal-lifecycle-workspace.log` and
`/tmp/focal-lifecycle-counts.json`; the Clippy log is
`/tmp/focal-lifecycle-clippy.log`.

The separate production no-panic/unchecked-operation gate passes, as does the
complete workspace release build. Documentation checks pass for all 19 library
targets (currently zero doctest examples). The architecture checker verifies
482 links, 37 unchanged imported source hashes and 15 frozen vocabularies.
Logs are `/tmp/focal-lifecycle-production.log`,
`/tmp/focal-lifecycle-doctests.log` and `/tmp/focal-lifecycle-release.log`.
No geographic scale, throughput or recovery-memory bound is inferred from these
checks. The complete P00–P20/L1–L8 goal remains active.

## Owned acceptance, graph and scope contracts

Claim generation now retains complete owned acceptance, graph and lineage
declarations plus creation position. Acceptance cannot be reconstructed from a
smaller supplied slot list. Every declared Required pure Receipt check must be
represented by an exact accepted proof; the canonical delivery witness retains
the complete proof set. The bounded registry pins evaluation content, target,
generation and receipt before exposure. Admission gates receipt acquisition;
sealed Required Increment targets must have final outcomes before whole-work
entry. Closing/posting/receiving a response can proceed while increments finish.
Response entry and received-target readiness reuse the claim's actual checked
increment and receipt guard. Final audit sealing is independent of increment
sealing, remains possible after business completion, and requires every registered
evaluation and its full accepted history. Two increments of one declaration
retain distinct artifact-ID/digest targets in aggregation and audit ordering.

The graph contract captures a complete bounded effective closure and computes
the least fixed point. It distinguishes DependsOn failure propagation from Awaits
and runtime monitor predicates. Private proofs bind every read revision, canonical
originating dependency cause, and deadline-triggered SCC victim. Consequences
cannot precede the creation, terminal, local-seal or scope facts represented in
the snapshot. Monitor roots and owned children live in the claim's bounded registry;
registration does not reuse a graph captured before adding the new edges. Named
rebind requires an actual compatible Supersedes relation. Monitor settlement and
owner release occur once; owner release cannot omit an unreleased owned child.
Public root generation cannot create an unregistered Claim-caused child: the
joint child-generation path installs ownership before returning that child.
Succession plans check compatible identities, explicit lineage, bounded acyclic
closure and effective read fences, preserving an already-terminal predecessor.

The real two-Actor exchange now includes a running Required Increment during
response closing, refusal of premature whole-work entry, actual evaluation
completion, independent artifact/response/claim outcomes, an audit bundle closed
while a runtime terminal wait remains, and later graph release. These are native
contract transitions; no handler is launched and no successor service command or
durable format is installed. Complete creation-lineage validation, owned-tree
terminal/fencing fanout, persistent authoritative registries, affected-only graph
indexes and atomic owner publication remain required under L1–L5.

Native stream V1 adapters now cover twelve types and all nine cursor operations.
The original-writer fixture corpus has 37 data files and preserved source/build
identities. Eight codec tests compare exact historical bytes, enum variants,
truncation, normalization, untrusted collection counts and enclosing suffix rules.
The adapters are not yet wired into Ledger Session snapshots, metadata entries
or managed cursor contracts; the surrounding L2 migration remains open.

README now provides the purpose and domain roles, source installation, local
restart and complete demo workflows, flags/JSON/YAML input, MCP launch/configuration,
four diagrams, RAM/WAL ownership and the six deployment stages. The laptop path
is explicitly local WAL; replicated groups add Raft. Current versus target
lifecycles and deployment guarantees are labeled. Its command smoke record remains
the executed 22-command run documented above; this increment additionally checks
local links and the MCP JSON example without inventing release binaries or scale
measurements.

This increment passes **1,037 tests across 66 workspace targets**, with zero failed,
ignored or filtered tests on macOS arm64, 2026-09-06. The Model target has 137 tests:
41 new cases beyond the prior 96. Native stream adds eight original-format codec
tests. The focused Model/Stream run passes 164 tests including its 19 existing
delivery tests. The full command was
`bash scripts/cargo.sh test --workspace --all-targets --no-fail-fast --offline --locked -- --test-threads=4`;
records are `/tmp/focal-acceptance-workspace.log`,
`/tmp/focal-acceptance-model-stream.log` and `/tmp/focal-acceptance-counts.json`.
Strict workspace all-target Clippy and the separate production no-panic/unchecked-
operation gate pass (`/tmp/focal-acceptance-clippy.log` and
`/tmp/focal-acceptance-production.log`). Formatting and diff checks pass. The
architecture check verifies 488 links, 37 unchanged Hecate imports and 15 frozen
vocabularies; the native cursor corpus's 37 files total 106,290 bytes and match
their recorded SHA-256 values. No dependency or explicit `Arc` is added by this
increment. This does not establish successor storage activation, complete CLI/MCP
lifecycle integration or geographic-scale qualification.

## Session V1 envelopes and output identities

The actual Session readers and writers now use explicit V1 representations for
all seven metadata entry tags and all five snapshot tags. Frozen Model output,
managed receipt/control and cursor DTOs, native stream types, Directory placement
types and 24 private Ledger row/envelope types compose through their owning crates.
Membership configurations and changes use explicit local adapters; Consensus
does not acquire a Model dependency. Existing wire Serde representations remain
unchanged. Managed receipt/control hashes, membership request hashes, placement
record identity and placement digests use the same frozen bytes and original
schema/domain headers. Managed admission/replay byte limits and Delta retention
sizes also select V1; allocation accounting still measures current owned state.

SS3/SS4/SS5 checkpoint writing borrows cursor records, cursor receipts, retained
deltas, membership, placement and request slots directly. It eliminates their
temporary cloned graphs and the separate outer payload buffer. The existing
eight-MiB output limit is checked before fallible final-vector reservation. Core
bytes are released before an optional retained checkpoint copy. Recovery retains
its owner-funded Completion reservation and constructs final collections directly.
The conservative reservation formulas are unchanged; this increment does not
claim measured throughput, reduced configured budgets or geographic scale.

Original output capture preceded live writer replacement. The new Model corpus
has 24 row files, the Directory corpus 49 data files, and the Session corpus
170 outputs totaling 2,993,005 bytes. The latter preserves 119 original source
identities, the exact capture generator and an immutable executable hash. Its
original generator is outside the executable test tree. Broad synthetic variants
are labeled separately from the actual SS3→SS4→SS5 admitted history; original
SS1/SS2 envelopes and reader normalization/suffix probes are also identified.
See the [storage contract](18-lifecycle-storage-upgrade.md#32-surrounding-session-formats-and-output-identities)
and [Session corpus](../../crates/focal-ledger/fixtures/durable-session-v1/README.md).

Eleven focused Session tests pass: all row/envelope variants and original hashes;
actual writer byte parity, disk restart and exact retries; all five snapshot
readers and subsequent log/delta replay; refusal before application publication;
and original managed-input/retention byte thresholds. CU1/CU2/CM1 retain their
original tolerant body-suffix replay. MC1/PL1/MU1/MS1 and SS1–SS5 retain exact-body
checks. No successor descriptor, application tag, floor or lifecycle activation
is installed by this compatibility increment. L2–L5 and the full objective remain
open.

The Session increment passes **1,062 tests across 66 workspace targets**, with
zero failures, ignored or filtered tests. This is the build preceding the
subsequent decoder-transition source changes. The complete run and count record
are `/tmp/focal-session-v1-workspace.log` and `/tmp/focal-session-v1-counts.json`;
the eleven targeted tests are in `/tmp/focal-session-v1-storage-tests.log`.
The production no-panic/unchecked-operation gate passes in
`/tmp/focal-session-v1-production.log`. Formatting and diff checks pass. Fixture
verification confirms all 170 Session outputs and 119 preserved sources, all 24
Model output files (332,655 bytes), and all 49 Directory files (13,738 bytes).
The architecture checker verifies 496 links, 37 unchanged imports and 15 frozen
vocabularies. The Session increment adds no dependency or explicit `Arc`.

## Bounded decoder transition

Consensus now supports exactly one ordered transition from an original durable
floor to a registered successor. Trusted composition confirms the complete compiled
pair; it cannot replace an earlier registration, bypass the baseline promise, or
advertise support from a caller-provided version flag. Fresh transitioned recovery
requires that full pair before application output or Raft participation, including
votes and reads. The predecessor remains supported after transition; the effective
required decoder is the successor. Matching retries preserve pending work.

`RecordKind::DecoderTransition` appends ordinal 7, preserving ordinals 0–6. The
fixed 74-byte payload is `FOCALDT1`, big-endian schema 1, predecessor and successor
fingerprints; its index and term are zero. The existing Completion-lane disk owner
persists it through one retained `WalAppend` receipt. Only observing successful
fsync publishes readiness. Checkpoint rewriting retains both the original floor
and transition in order, including when another logical group triggers rewriting.
There is no new disk worker, dependency or explicit `Arc`.

Eight new consensus regressions and one WAL format test qualify the fixed layout,
unknown/malformed history refusal, pair-only recovery, guarded participation,
monotone confirmation while baseline persistence is pending, real delayed fsync
under Ordinary memory pressure, repeated and conflicting retries, abandoned
callers, both ambiguous-fsync cuts, and own/other-group checkpoint retention with
the exact application suffix. The focused Log/Consensus run passes 65 tests.

An [actual preserved older executable](../../crates/focal-consensus/fixtures/decoder-transition-old-binary/README.md)
accepts the unchanged and original-floor controls, then refuses the transition
twice before compaction and twice after each of two compaction paths. All six
refusals report physical `invalid durable record` without a panic or application
report, and leave every file's name, length and SHA-256 unchanged. Original
application snapshot bytes survive both rewrites. This is real binary evidence,
separate from the reconstructed old-enum test. The record preserves the helper,
exact selected library/compiler identities, commands, outcomes, file manifests
and independently checked WAL frames; all 74 evidence-file hashes verify.

The combined increment passes **1,071 tests across 66 workspace targets**, with
zero failures, ignored or filtered tests. Records are
`/tmp/focal-decoder-transition-workspace.log` and
`/tmp/focal-decoder-transition-counts.json`. Strict workspace all-target Clippy,
the separate production no-panic/unchecked-operation gate, formatting and diff
checks pass; their logs use `/tmp/focal-decoder-transition-`. This qualification
precedes the subsequent native validation-definition ownership refactor.

The transition mechanism is dormant in production Session, which still confirms
only its unchanged managed V1 descriptor. The experiment's successor is explicitly
test-only. No lifecycle-V2 descriptor, decoder, wire shape or activation is claimed.
Actual successor owner state, complete lifecycle histories, all-voter activation,
learner admission fences, CLI/MCP integration and geographic qualification remain
required under the [storage plan](18-lifecycle-storage-upgrade.md).

## Owned native validation definitions and retained evaluation state

Native `Declaration` now owns its slot text and ordered handler policies.
`Declaration::prepare` checks the complete borrowed input without allocating and
returns a construction plan with checked requested row/buffer bytes. Building the
plan uses fallible exact reservation; `retained_bytes` reports actual capacities.
The future Core owner must reserve its budget before construction and reconcile
actual retained allocations. This increment does not install that accounting.

`EvaluationState` is a reference-free, independently retainable row. A temporary
`Evaluation` view borrows the definition only while checking transitions;
`bind` and `into_state` allocate nothing and copy no policy buffers. A private
semantic stamp covers every actual immutable declaration field. It prevents a
caller from substituting different handlers, attempt policies, deadlines or
targets under the same supplied content binding. Exact independently reconstructed
definitions can rebind. Acceptance declarations, evaluation registration, result
capabilities and audit membership carry the same guard, including late results
against a sealed audit. No mutable detached-state access bypasses the check.
The stamp is an internal consistency guard with no durable or wire identity.

Five new regressions cover dropping all construction inputs and declaration
owners across Error/retry, programmatic Pass and quality Pass; 22 same-binding
semantic substitutions; preallocation bounds and charges; alternate-definition
materialization, registration and results; and substitutions at audit sealing and
late result admission. The existing multiple-Required-Receipt test still verifies
missing, duplicate and complete exact proof sets. See
[retention tests](../../crates/focal-model/src/lifecycle/validation_retention_tests.rs),
[acceptance tests](../../crates/focal-model/src/lifecycle/aggregation_tests.rs), and
[audit tests](../../crates/focal-model/src/lifecycle/audit_tests.rs).

Qualification passes **231 focused tests**: 148 Model and 83 Core, with zero
failed, ignored or filtered tests, plus both crates' doc-test targets. Strict
workspace all-target Clippy and the separate production no-panic/unchecked-operation
gate pass. Logs are `/tmp/focal-owned-validation-focused.log`,
`/tmp/focal-owned-validation-clippy.log`, and
`/tmp/focal-owned-validation-production.log`. The preceding 1,071-test workspace
run remains evidence for the earlier decoder-transition build; it is not relabeled
as a run of this refactor. No dependency or explicit `Arc` is added.

This completes the independent definition/state ownership prerequisite in
[the successor owner sequence](18-lifecycle-storage-upgrade.md#61-concrete-successor-owner-integration).
It does not activate successor storage, extend live CLI/MCP behavior or close L1.
The sole Core owner, mandatory complete creation-lineage checks, joint child/control
publication, versioned durable representations and activation remain required.

## Fallible owned RAM preparation and lineage chronology

The existing RAM range owner now accepts non-`Clone` values through
`prepare_batch_with` and `prepare_after_with`. Incoming `Put` entries move directly
into final candidate pages; they are not copied. Only retained entries in touched
pages invoke the explicit fallible copier, after the full page allocation has been
reserved. Unchanged pages preserve their existing shared lifetime. Failure drops
every provisional page and permit without changing published state or pinned reads.
The recorded heap charge must conservatively cover the copied key/value capacities
and allocator overhead; any additional copier workspace belongs to its caller.

`PreparedRange::entries` exposes the complete ordered unpublished prefix.
`SnapshotLease::project_next` no longer requires `Clone` and still prevents borrowed
entries from escaping its checked lease lifetime. Existing Clone preparation APIs
delegate through the same implementation. Owner identity, exact base-root lineage,
ordered chain validation and allocation-free publication remain intact. The change
adds no per-object wrapper or additional `Arc` site. The page directory still costs
O(number of pages) to copy; this is not scale qualification.

Seven [owned-range regressions](../../crates/focal-memory/src/owned_range_tests.rs)
cover moved payload pointers, exact selective-copy sets, reservation before copier
entry, failure after earlier pages have already been prepared, pinned snapshot
isolation, complete-page deletion, effective pending iteration, foreign/stale
candidate refusal and real Ordinary/Completion exhaustion. Publication succeeds
after the final remaining budget has been reserved by other work. These exercise
the real memory primitive, not a mock owner.

Separately, native `SuccessionPlan` now checks chronology on every traversed
Cause/Supersedes/Amends edge, including targets already visited by DFS. A historical
claim cannot refer to a claim created later merely because both precede today's
successor. Five [regressions](../../crates/focal-model/src/lifecycle/succession_tests.rs)
cover direct and transitive violations, already-visited targets, valid ordered
history, same-cut acyclic references and same-cut cycles. Mandatory checked
creation against the future Core registry remains open; this correction strengthens
the existing succession path only.

The first workspace attempt exposed a test synchronization race: a blocked replay
visitor signaled before the disk owner necessarily reserved its next buffered
record, changing the test's observed memory by exactly 319 bytes. The floor and
transition tests now use the existing acknowledged physical `WalPause` test seam.
Their exact retained-memory and real pending-fsync assertions remain unchanged;
no sleeps or relaxed comparisons substitute for the barrier. Consensus enables
the already-existing `focal-log/test-support` development feature; production
dependencies and behavior do not change. Thirteen focused decoder tests pass after
this correction. The first-attempt log is retained separately as
`/tmp/focal-owned-range-workspace-first-attempt.log`.

The [Core ownership plan](18-lifecycle-storage-upgrade.md#62-concrete-ram-owner-and-publication-seam)
now identifies the existing RAM publication path, an explicitly specialized native
Core, complete effective-state creation checks, and joint owned-child/control
transactions. The storage prerequisite is implemented; the native Core transaction,
durable representations, activation and richer lifecycle CLI/MCP path remain open.


## Respondent-authored testimony, diagnostic evidence and binary delivery

The README now introduces Focal as an inter-agent communication protocol and
event-driven ledger and uses one two-party exchange diagram. Requester and
respondent have separate lanes: directed claim, execution receipt, participant
work, respondent-authored testament, claimant receipt, participant validation and
derived acceptance. Both old sprawling diagrams were removed. Mermaid 11.12.0
and cached Chromium rendered the new diagram at desktop and narrow widths;
inspection found no clipping or overlap. The first render caught an unescaped
semicolon, corrected before the successful render. Narrow screens still need
zoom for comfortable reading. Rendering artifacts are in
`/private/tmp/focal-readme-mermaid-viul94z9`; no project dependency was added.

### Running service and shared CLI/MCP

New admission rejects historical runtime-synthesized `FailTestamentGeneration`.
The current receipt holder must submit ordinary testimony after work completes
or fails, with its own summary, confidence, explicit outcome and exact manifest.
Every non-Complete outcome requires a real durable error artifact. Core verifies
its effective committed/pending row, ledger, receipt, producer, schema and hash;
CLI/MCP builders reject empty unsuccessful reports before submission. Historical
V1 reducers, hashes, checkpoint layouts, command tags and exact retained retries
remain unchanged. Direct, epoch and managed replay tests preserve the old rules.

At the last available artifact slot, new admission preserves diagnostic headroom
unless an eligible diagnostic is already retained. An error-shaped artifact with
zero custody cannot consume that reserved slot, and content deduplication cannot
repair old custody by supplying a new attestation. Both legacy and managed
proposal paths see pending rows. The shared authored manifest bound now matches
the default 1024-artifact Core set bound; previously its 256 limit could strand a
larger staged set. Full-size JSON/YAML close input is covered by regression tests.

A pre-policy full manifest without usable diagnostic evidence cannot be backfilled
under frozen V1. Its historical replay remains valid, but a new unsupported failure
report is refused. A versioned continuation/migration is still needed for that
case. Manifest headroom does not reserve all node/global object, request, memory,
evidence-storage or disk capacity; complete resource reservation for terminal
reporting remains native-owner work under [18](18-lifecycle-storage-upgrade.md).

The service now admits a pinned language-agnostic `error-report` payload as well
as the original `test-report`. Errors carry required bounded code/message and
optional details, with object-only strict JSON, duplicate/unknown-field refusal,
fixed blank-codepoint semantics, and a 64-KiB encoded limit. Shape checking retains
no field strings and decides no verdict. Local and replicated custody use the
same checker and schema-specific content read bounds. Local inline attestation
now borrows bytes instead of cloning them. Existing test-report identity and
parsing behavior remain unchanged. `focal schema get error-report` exposes its
exact descriptor/hash/example; list and shell completion discovery include it.
MCP submits the same typed artifact, though payload-schema lookup currently
remains CLI discovery. Runtime/tool failure needs no invented test counts.

Actual CLI success and failed-test workflows pass. The MCP failure workflow
submits a real tool-unavailable diagnostic, retains explicit Failed/Tentative
respondent testimony, receives it, invokes an acceptance check in the participant
process, records separate proof and a failing verdict, restarts server and MCP,
and verifies unchanged evidence, report and exact retained retry. These adapter
tests use one authenticated participant's self-handoff roles; separate-principal
and receipt-adoption authority is covered by Core/native tests. The 18-test actual
CLI/MCP suite passed before complete workspace qualification.

### Independent native contract

Native responses now own their bounded summary, confidence, one of all six
reported outcomes, work-slot manifest and separate typed diagnostic references.
`ClosePreparation` checks before allocation, reports construction charge and
builds fallibly; actual retained capacities are inspectable. Transitions reuse
owned buffers and need no new `Arc` or production `Clone`. Diagnostic tokens
verify actual owner-resolved artifact metadata and custody; diagnostics never
stand in for missing requested work. A failed response with no work slots remains
receivable and inspectable before MissingSlot is assessed at evaluation.

Private report stamps prevent changed reports from reusing a binding in response
transitions, claim observations or validation readiness. A closing incident is a
checked publication plan retaining a real diagnostic and a new claim revision;
it does not terminalize the claim or fabricate a testament. Eight new native
regressions cover these facts and the existing pending-Increment exchange remains
valid. The 161-test model suite passed. Native owner storage, versioned encoding
and independent-lifecycle CLI/MCP activation remain open.

### Prebuilt binary distribution

The [release workflow](../../.github/workflows/release.yml) now builds one raw
`focal` server/client/MCP executable for each of six native macOS/Linux targets.
No end-user source checkout, Rust, protobuf compiler or Python is required.
Collection requires every platform, SHA256SUMS and matching provenance. Manual
runs produce CI artifacts; intentional version-tag runs upload and verify an
unpublished draft before publication. Existing releases are never overwritten.
Eleven Python orchestration tests pass, including incomplete/altered asset sets
and failed publication verification. No tag or public release was created.
[20](20-binary-distribution.md) records native Windows implementation work and
clean-machine/platform-release qualification, which remain open.

### Verification notes

The first current workspace run found one new test fixture using the issuer to
register a worker-produced historical artifact. Correcting the fixture principal
preserved the zero-custody rejection being tested; all fifteen Core admission
regressions subsequently passed. A later workspace run found a read-pagination
fixture that still closed an empty Interrupted report. It now uses a Complete
delivery account; the test's pagination, recorded-validation and recovery assertions
are unchanged, and its submission helper prints the actual refusal on failure.
The focused test passes. A workspace audit found no other unintended live
non-Complete/empty-manifest fixture; historical and refusal fixtures stay intact.
Clippy also identified one needless test clone,
replaced by a borrowed slice. All-target Clippy, the separate production no-panic
lint gate, formatting and architecture source/link checks then passed.

The prior owned-range workspace run's fleet test observed a legitimate unknown
outcome while opening a healthy leader epoch under load. The healthy setup paths
now use the fixture's existing bounded exact-request retry helper; the isolated
partition still makes one uncertain request and retains its original assertion.
No production timeout or outcome assertion was relaxed. Full-workspace evidence
for the combined increment is recorded below only after execution completes.


Final qualification on the local macOS arm64 checkout:

- `bash scripts/cargo.sh test --workspace --offline --locked -- --test-threads=4`
  passed **1,121 tests across 66 unit/integration targets**, followed by all doc
  tests. Evidence: `/tmp/focal-authored-report-workspace.log`. This includes the
  earlier owned-range, validation-ownership, chronology, decoder-pause and fleet
  fixture corrections as well as this report/diagnostic increment.
- Packaged claims/evidence/validation instructions were then aligned with the
  corrected reporting obligation. Their versions are 8/7/3; cluster remains 1.
  The shared reference pins the real error-report descriptor hash and example
  for MCP-only participants without inventing a schema tool/resource. Manifest
  digests were refreshed. **47 focused evidence/MCP library tests passed** after
  that instruction update, including one added hash/example regression:
  `/tmp/focal-authored-report-skill-contracts.log`.
- `bash scripts/cargo.sh build --release -p focal-node --bin focal --offline --locked`
  produced a Mach-O arm64 `target/release/focal`. Its SHA-256 is
  `4a83e4602b2dc906ff91557d7e707a565c940db2a2e657a4cc35e698b237efd3`.
  The actual binary passed `python3.14 scripts/release/smoke.py target/release/focal`:
  real server, CLI, MCP, abrupt process kill and exact acknowledged-write recovery.
  Build/smoke logs: `/tmp/focal-authored-report-release-build.log` and
  `/tmp/focal-authored-report-release-smoke.log`.
- Eleven release-script tests passed, and the authored diagram was rendered and
  visually inspected. These checks do not substitute for the six hosted native
  release lanes, clean-machine installation, Windows execution, or publication.

The first and second current workspace failure logs remain separately available
as `/tmp/focal-authored-report-workspace-first-attempt.log` and
`/tmp/focal-authored-report-workspace-second-attempt.log`; neither is represented
as a passing run. This increment adds no production `Arc` site, changes no frozen
V1 bytes or replay interpretation, and does not claim the native owner migration
or global deployment plan is complete.

## Native claim owner, atomic lineage and owned cancellation — 2026-09-06

The first transaction in [18 §6.2](18-lifecycle-storage-upgrade.md#62-concrete-ram-owner-and-publication-seam)
now runs through the actual [Core owner](../../crates/focal-core/src/native.rs).
`Core<S = State>` has a sealed state parameter: the default retains V1 APIs and
codecs, while `Core<NativeState>` owns a custom `RangeStore`. It has no native
Serde implementation or arbitrary row insertion, and no Session/CLI/MCP path
selects it yet. It is an in-process publication boundary, not an installed native
WAL or a second editable representation of the V1 ledger.

Implemented facts:

- Core assigns creation positions and admits root, owned-child and successor
  proposals through one mandatory complete-lineage plan. Every Cause, Supersedes
  and Amends endpoint resolves from the actual committed/prepared root. New-ID
  absence, disconnected cycles, same-batch ancestry, historical chronology and
  exact parent receipt/revision authority are checked before publication. Raw
  claim constructors are restricted to model internals/test fixtures.
- Parent child registries and successor consequences publish with new rows.
  Cancellation authenticates the selected root's issuer once, follows the entire
  stored owned tree through terminal descendants, and uses private derived
  authority for live children. It preserves prior terminal cuts, unrelated peers
  and unreleased scopes; it does not impersonate descendant issuers or fabricate
  respondent testimony.
- Metadata, claims, retained successful request outcomes and history events share
  one canonical range root. Native intent fingerprints cover the complete
  immutable proposal, including private acceptance stamps. Exact retries identify
  whether the original outcome is pending or committed. Different intents under
  one key refuse; no outcome or history becomes visible ahead of its rows.
- History records creation at revision one, every owned-child registration with
  its captured child binding and successive parent revision, then supersession
  or cancellation. One atomic sequence includes these explicit phases and its
  event count. Ledger-local stored bindings expand into exact public bindings
  without allocating or repeating the ledger identity in every stored field.
- Real MemoryBudget permits precede model workspace, changes/history buffers and
  claim-container construction. Fallible copies expose retained/requested byte
  and allocation counts. Actual capacities are reconciled, including scope
  replacement buffers before installation. Large claim rows have a private
  fallibly allocated owner so smaller range entries do not use claim-sized slots;
  claim policies move without duplicating their buffers. No per-object Arc or
  infallible boxing is introduced.
- The complete pending chain must have exact owner/root provenance. Publication
  allocates nothing and returns an intact refused candidate. Fixed-prefix leases
  keep earlier facts visible until release/expiry; failed preparation and dropped
  suffixes reclaim their permits without changing the live root. The caller must
  establish durability before calling publication and account projected output.

Owner tests cover atomic parent/child/outcome/history visibility, full lineage
against pending state, successor replacement, reverse-ID registration history,
exact replay and conflicting intents, foreign and stale forks, reordered
publication, suffix drop, terminal-descendant preservation, real memory pressure,
publication with no remaining budget, mid-neighbor-copy failure, and lease expiry.
Model tests additionally exercise different-issuer descendants, complete-row
token checks, all nested copy failure positions and actual-capacity refusal before
scope installation.

The first complete workspace attempt found a stale client skill-contract fixture
still expecting versions 7/6/2 after the prior reporting update installed 8/7/3.
The fixture now matches the installed claims/evidence/validation versions while
retaining its digest, schema and required-operation checks. The failed run is
retained as `/tmp/focal-native-owner-workspace-first-attempt.log`.

Remaining work: store full immutable validation declarations and independently
owned evaluation rows in the same authoritative registry; add response, artifact,
aggregation, graph, audit, authority-fence and retained-refusal transactions;
qualify native encoding, import, recovery and Session activation. The current
owner still reserves a conservative per-attempt peak, retains history without a
native retirement policy, and uses RangeStore's linear page-directory rewrite.
It does not establish global capacity, automatic sharding or multi-region
deployment qualification. The broader implementation goal remains open.

Qualification completed on the local macOS arm64 checkout:

- `bash scripts/cargo.sh test --workspace --offline --locked -- --test-threads=4`
  passed **1,171 tests across 66 unit/integration targets**, followed by all doc
  tests. Evidence: `/tmp/focal-native-owner-workspace.log`. The run includes the
  current skill instructions, actual CLI/MCP failure-reporting workflows,
  replication/recovery tests and frozen V1 fixtures.
- A final owner regression then exercised a live grandchild below a superseded
  child: cancellation reaches the grandchild while retaining the child's original
  binding and terminal cut, and leaving the unrelated successor alone. All
  **18 focused native-owner tests passed**, including that additional regression:
  `/tmp/focal-native-owner-final-tests.log`. The complete model suite also passed
  **193 tests**, including 19 mandatory-creation, six owned-cancellation and seven
  copy/identity regressions.
- All-target Clippy, the separate production no-panic gate, formatting and diff
  checks passed. Production evidence: `/tmp/focal-native-owner-production.log`;
  all-target lint evidence: `/tmp/focal-native-owner-clippy.log`. The architecture
  checker verified **541 links, 37 imported source hashes and 15 frozen domain
  vocabularies**. No V1 durable tag, wire ordinal or replay interpretation changed.

No release build or hosted cross-platform release qualification was performed for
this native-owner increment. The prior binary smoke belongs to the preceding
reporting/distribution increment and is not represented as native activation.

## Native definitions, Admission entry and evaluation control — 2026-09-06

The [native Core owner](../../crates/focal-core/src/native.rs) now retains complete
validation declarations and independent evaluation rows in the same range as
claims, successful request outcomes and typed history. This extends the previous
creation/cancellation increment; it does not activate a successor Session codec.

Implemented facts:

- `Create { claims, declarations }` requires the complete immutable declaration
  cohort for every acceptance manifest. Missing, extra, duplicate, orphaned,
  retained-ID and semantic substitutions refuse atomically. Full definition
  intent participates in request identity. Each declaration owns its handler and
  slot buffers; temporary evaluation views borrow that actual stored definition.
- A compact claim-owned `RegistrationSet` records actual evaluation membership
  without copying acceptance policies or handler definitions. Evaluation keys
  distinguish claim, definition, target and generation; retained rows additionally
  pin complete content, revision and receipt identity. Child registration preserves
  an existing parent's evaluation membership.
- `Post` resolves the retained definitions, checks the actual Generated claim and
  its authenticated issuer, then stages Posted and every Admission Ready row and
  registration together. The baseline native posting policy permits a structurally
  valid claim addressed to a nonzero participant; it assumes no global participant
  registry and manufactures no configurable policy grant. Posting starts neither
  respondent work nor a validator and creates no testament.
- `BeginAdmission` checks the current claim, exact registration, evaluation
  revision, designated evaluator, definition, generation and deadline. It enters
  the declared programmatic or direct-agentic phase without running any handler.
  A trusted `NativeContext` supplies monotonic logical time independently of
  participant intent. Exact retries retain the original accepted time and pending
  or committed outcome. Additional `required_policy` grants remain unsupported;
  their absence refuses entry.
- Cancellation resolves evaluations for every owned transition, including already
  terminal descendants. Supersession uses a private token from the actual checked
  creation plan. It fences only a predecessor newly transitioned to Superseded;
  Amends and succession of a terminal predecessor do not invent new control.
  Fences preserve validation state, existing terminal results and earlier fences.
  They produce no verdict, respondent testament or implicit scope release.
- Typed history distinguishes declaration retention, claim transitions and
  evaluation Materialized, Begun and AuthorityFenced facts. Exact bindings,
  target/generation, phase, applicable attempt and fence cause share their rows'
  publication. Fixed-prefix reads include full definitions and independent
  evaluations; stale or foreign candidate publication retains ownership on refusal.
- The owner precharges temporary descriptors, row/event containers, policy heaps
  and simultaneous old/replacement registration buffers. Extra-row descriptors
  allocate lazily and grow fallibly. Actual allocator capacities are reconciled
  before retention. A lint check found evaluation history was padding every range
  row; fallible event indirection now prevents that inflation and charges its full
  heap. No per-object `Arc`, infallible boxing or production panic was introduced.

Focused owner tests cover pending Create → Post → Begin → Cancel, both evaluator
phases, actor/node and revision/time/target refusals, exact complete definitions,
parent registry preservation, Required and Observe membership, supersession versus
Amends, pinned reads, global and per-claim limits, request reuse after refusal,
ordinary/completion pressure, allocation-free publication and retained-neighbor
copy failures. Staging tests separately check lazy allocation, growth refusal,
preserved facts across retry, singleton charges and complete outer reservation.

Remaining work: native receipt acquisition/adoption, evidence custody and artifact
rows, respondent-authored response close and diagnostics, result admission,
Increment/WholeWork materialization, aggregation, graph/scope consequences, audit
seals and retained semantic refusals. The result path must handle terminal
evaluation history explicitly rather than using the current nonterminal
materialize/begin/fence helper. Stored grants, native snapshots and prepared
commands, versioned recovery/import, WAL integration and Session activation also
remain open. The currently running CLI/MCP continues to use V1. This increment
does not qualify history retirement, automatic sharding, global throughput or
cross-platform release deployment; the overall implementation goal stays open.

Qualification completed on the local macOS arm64 checkout:

- The full workspace command
  `bash scripts/cargo.sh test --workspace --offline --locked -- --test-threads=4`
  passed **1,222 tests across 66 unit/integration targets**, followed by all
  **19 doc-test targets**. This includes **144 Core tests** and **215 model
  tests**, the frozen V1 corpora, replication/recovery and actual CLI/MCP
  failure-evidence/retry workflows. Evidence:
  `/tmp/focal-native-admission-workspace.log`.
- All **46 focused native tests** passed, including 19 Admission owner cases,
  seven staging/memory cases and the claim/owned-container regressions.
  Evidence: `/tmp/focal-native-admission-final-tests.log`.
- All-target Clippy and the separate production no-panic gate passed:
  `/tmp/focal-native-admission-clippy-final.log` and
  `/tmp/focal-native-admission-production.log`. The earlier layout finding is
  retained separately in `/tmp/focal-native-admission-clippy.log`; it is not a
  passing lint run. Formatting and diff checks passed. The architecture checker
  verified **544 links, 37 imported hashes and 15 frozen vocabularies**.
- The README's two-agent sequence was visually checked at desktop and narrow
  sizes. It places authored testimony after the respondent's work attempt and
  makes failure/error artifacts available for the claimant's assessment.

No release build, native durability activation or hosted release qualification
was performed for this increment. The next concrete integration order and
identified model interfaces are recorded in storage-plan §6.3.

## Native Admission reports and verified local evidence — 2026-09-06

The [native reporting transaction](../../crates/focal-core/src/native/reporting.rs)
now accepts actual evaluator reports and publishes their immutable artifacts,
accepted attempts, evaluation changes, first Admission failure and request history
through the existing RAM owner. This is an internal typed path; Session, wire,
WAL, CLI and MCP continue to use the qualified V1 implementation.

Implemented facts:

- The [native artifact descriptor](../../crates/focal-model/src/lifecycle/artifact_descriptor.rs)
  owns its bounded metadata, payload, input references and visibility. Its native
  content identity includes typed result provenance: claim, validation, complete
  target bindings, generation, handler/version, reporting phase/attempt, evaluator
  and verdict. Its own allocated artifact ID remains excluded from content
  identity, while the request/custody fingerprint includes that ID. Identical
  diagnostic bytes on different real attempts need no invented metadata and may
  share the same immutable content tree while retaining distinct descriptors.
- [Local custody verification](../../crates/focal-evidence/src/native_artifact.rs)
  validates real schema-conforming bytes under a reserved memory allowance. Inline
  evidence is synced using the original authenticated chunk/manifest format;
  referenced evidence is read and authenticated against its complete tree.
  Descriptor, producer, request principal/epoch/ID and content coordinates are
  pinned by a private capability. Exact manifest-size and buffer limits are
  checked before installing chunks. Capacity refusal creates no new storage
  identities or false custody token. This capability proves a synced local copy;
  it does not assert replicated placement or that an external check really ran.
- `prepare_native_evidenced` authenticates and resolves the exact retained request,
  definition, claim, registration and evaluation. It checks actual pre-report
  authority, attempt, deadline, typed provenance, declared evidence schema and
  request-bound custody. Input objects must exist at the effective pending root;
  derived artifacts retain their source artifacts' visibility restrictions.
  Incomplete/Error requires an `error` descriptor with the declared diagnostic
  schema. Core constructs `EvidenceFacts`; callers cannot supply verified-state
  or custody booleans. New artifact IDs and canonical content identities are
  checked together, including unpublished candidates.
- The [report authority frame](../../crates/focal-model/src/lifecycle/validation_admission.rs)
  distinguishes beginning a check from reporting one already begun. Ordinary
  parent progress, receipt acquisition and PostFailed do not by themselves revoke
  an eligible begun chain. Actual cancellation, supersession, expiry and installed
  authority/adoption fences still refuse stale reports. Owner-level tests cover
  pending control and late siblings; post-receipt reporting is currently model
  qualification because native receipt acquisition remains unimplemented.
- [Admission projection](../../crates/focal-model/src/lifecycle/aggregation_admission.rs)
  borrows the complete actual registration, declarations, evaluation states and
  accepted-result rows. It allocates no aggregate or second policy copy. Required
  terminal results determine acceptance; Observe and intermediate retry/quality
  results remain inspectable without becoming false acceptance witnesses. The
  original earliest blocking sequence and canonical cause are preserved when
  other begun checks report later.
- An accepted report retains its exact **reporting** attempt, including the
  handler/version, and original sequence/ordinal. The next evaluation may already
  name a different retry or quality phase; history does not reconstruct the old
  attempt from that next state. Each report records artifact creation at ordinal
  0, the evaluation transition at 1, the accepted result at 2, then a derived
  PostFailed transition at 3 when applicable. Secondary identity-index rows create
  no invented domain events. There is no respondent testament side effect.
- [Fallible artifact/result containers](../../crates/focal-core/src/native/result_owned.rs)
  account for actual capacities and touched-page copies. Artifact facts project
  from one descriptor provenance; accepted history retains one ResultArtifact
  rather than a duplicate AcceptedResult. New result/artifact limits and events
  join the same immutable candidate root. Snapshot projections include both
  families; failed or out-of-order publication returns the intact candidate.
  An exact pending or committed retry returns the original outcome before
  requiring another custody token. Changed intent under that key still refuses.

The README's two-agent sequence now explicitly branches between completed and
failed/partial respondent work, with authored testimony after either branch.
Both branches converge on delivery to the requester and assessment of the exact
artifacts. The updated Mermaid source was rendered with cached Mermaid 11.12.0
and visually checked at desktop and narrow widths. Rendering caught a semicolon
syntax error in message text; that source was corrected and rerendered.

Remaining work is explicit: owner-gathered graph/start preflight and real receipt
acquisition; respondent artifact/cycle ownership and close transactions;
Increment/WholeWork evaluation entry and aggregation; policy-grant resolution;
audit seals and graph consequences; durable native encoding/replay and service
activation. Report preparation uses Completion capacity, but Begin does not yet
reserve guaranteed end-to-end headroom for future raw-byte verification and
result retention. Local evidence verification currently reserves a conservative
workspace of roughly 9 MiB for the largest built-in schema. Installed replicated
placement must qualify local custody before distributed native ingress can
promise stronger durability. This increment qualifies neither global scale nor
native durable recovery.

Qualification completed on the local macOS arm64 checkout:

- `bash scripts/cargo.sh test --workspace --offline --locked -- --test-threads=4`
  passed **1,282 tests across 66 unit/integration targets**, followed by all
  **19 doc-test targets**. This includes **166 Core tests**, **245 model tests**
  and **31 evidence tests**. The native owner suite now contains **68 tests**;
  its 16 report cases and six artifact/result ownership cases cover real custody,
  identical diagnostics on separate attempts, substitution refusal, late sibling
  reports, control fences, quality/retry history, capacity rollback and snapshots.
  Evidence: `/tmp/focal-native-report-workspace.log`.
- The separate production no-panic gate passed with
  `CARGO_NET_OFFLINE=true bash scripts/check-production.sh`.
  Evidence: `/tmp/focal-native-report-production.log`. No per-object `Arc` or
  infallible boxing was introduced by this increment.
- All-target Clippy passed with `-D warnings` after the final source changes.
  Evidence: `/tmp/focal-native-report-clippy-final.log`. The final formatting
  and diff checks passed as well.
- The architecture checker verified **556 links, 37 imported source hashes and
  15 frozen domain vocabularies**. The native additions did not change V1 durable
  tags, hashes, receipts or replay interpretation. Formatting and diff checks
  passed; no release build or hosted cross-platform qualification was performed.

The next implementation work remains receipt/start projection with bounded
owner-gathered graph facts, completion headroom reserved before evaluation entry,
and respondent evidence/response transactions, followed by native durable
activation. Existing CLI/MCP workflows passed their regressions; they do not yet
route to these native owner operations.

## Native first receipts and bounded graph capture — 2026-09-06

The [receipt owner transaction](../../crates/focal-core/src/native/receipt.rs)
now connects the actual subject's first responsibility receipt to retained
Admission results and the effective claim graph. It remains an internal typed
operation; native Session/wire/WAL and CLI/MCP activation are still open.

Implemented facts:

- `AcquireReceipt { expected, receipt }` requires the exact Posted claim and its
  authenticated subject. It rejects new acquisition at an authored deadline and
  records no invented Expired/ReceiptFailed transition. The owner assigns epoch
  one and retains the nonzero receipt ID in a ledger-wide allocation index,
  including pending candidates. Cancellation does not free that identity. An
  exact retry returns the original outcome even after deadline or later control.
- The shared [Admission view](../../crates/focal-core/src/native/admission_view.rs)
  reads complete actual declarations, registration, evaluation and result history.
  Required terminal Pass is mandatory; a programmatic Pass with pending quality
  remains insufficient. Observe does not block receipt. Ready Observe remains
  Ready and cannot begin afterward; already-begun Observe Error/retry/quality
  chains remain authorized to report without repainting the receipt.
- Closure discovery follows stored dependency/await edges, active runtime roots
  and every owned descendant, validating child binding, Cause and registration
  chronology. It includes terminal endpoints and pending rows, bounds node/edge
  traversal, and never scans unrelated ledger claims or trusts caller-selected
  peers. Missing endpoints, incomplete closure and resource exhaustion refuse
  before any published change.
- [Graph capture preflight](../../crates/focal-model/src/lifecycle/graph_capture.rs)
  counts all four metadata vectors without allocating. The owner reserves the
  charge before construction; every actual capacity and the total retained charge
  are checked. The compatibility `Snapshot::capture` uses the same implementation
  and least-fixpoint logic. The receipt path checks the effective cut and consumes
  the private Start witness against exact sorted original peer bindings.
- Received, holder/fence, acquisition sequence, immutable receipt allocation,
  history and request outcome share one candidate root. `NativeReceipt` is a
  compact Copy allocation fact, not a duplicate mutable receipt owner. Prepared,
  committed and expiring snapshot reads expose the same allocation. First
  acquisition creates no testament, artifact, evaluation or verdict and releases
  no scopes. Publication refusal returns the intact candidate.

The outgoing-closure adapter can stage checked graph-release consequences for
locally complete dependencies. Native operations cannot yet produce those states
or active runtime scopes. Full incoming monitor/subscriber consequences and
scope-release publication remain required before activating WholeWork/scope
operations; the present branch must not be described as complete global graph
propagation. Actual owner tests currently prove failure-settled Awaits, unresolved
DependsOn, owned closure and Admission-driven receipt behavior. Broader least-
fixpoint and active-root semantics have model qualification.

Completion capacity is still an open protocol obligation. Safe capacity refusal
and use of the Completion lane do not guarantee that every accepted respondent
or begun evaluator can later report. The next memory work must provide transferable
funding through evidence verification, native preparation and retained range
pages, and reserve discrete result/artifact/history limits across the complete
retry/quality chain. Receipt/Begin entry must not promise this guarantee until
those actual owner reservations are integrated. Respondent artifact/cycle and
closing-testament transactions, receipt adoption, remaining graph consequences
and native durable activation also remain open.

## Range preparation preflight and client lock ownership — 2026-09-06

[Checked range preparation](../../crates/focal-memory/src/range_preflight.rs)
now owns the incoming write set and borrows its exact immutable base. Planning
sorts the input in place, walks the page directory and inspects retained rows only
on touched pages; it allocates no new buffers and copies no values. The plan
reports the exact output page count, incoming Pending charge, new directory and
page charges, maximum merge workspace and conservative **additional** peak.
Existing base roots, pending predecessors, snapshots and caller/copier workspace
remain separately charged. This still traverses the full page directory; it is
not a constant-cost persistent-tree operation.

The existing Clone and fallible-copy preparation APIs consume the same checked
plan. New values move into their destination pages, while untouched pages stay
shared. Directory, merge and entry-vector capacities are checked against their
precharged bounds immediately after allocation; excess capacity is refused and
freed without a later attempt to reserve more. Checkpoint import applies the same
checks to its change and payload-permit staging vectors. Page accounting includes
both the page allocation and its entry-vector allocation. Exact base/input
ownership prevents substituting a more expensive pending root under a cheaper
quote. Publication retains its original ordered-root checks and rollback rules.

This is a preflight boundary, **not completion funding**. A plan holds no permit,
and another owner may consume capacity before its build. Building still uses the
existing budget reservations. The supplied `Entry.heap_bytes` contract continues
to cover dynamic key/value capacities and their allocation overhead; arbitrary
key Clone implementations, value copiers and their additional workspace cannot
be measured by the range engine. Transferable completion reservations, future
root/neighbor growth and discrete domain-row allowances remain separate work.

The [client lock guard](../../crates/focal-client/src/file_lock.rs) now makes lock
release follow the owning critical section. A duplicated open file description
can outlive its original File during process creation; relying on that File's
close alone could retain the lock until the duplicate closed. The guard explicitly
unlocks on owner drop, including initialization failures, and preserves exclusion
while the owner is live. It covers pending requests, legacy/managed operation
stores, coordinator/watch owners, artifact uploads/catalogues and upload
bootstrap markers. Marker contents, request identities and acquisition/durability
error paths retain their existing formats and checks. Drop retries interrupted
unlock calls without panicking; closing the descriptor remains the fallback for
other unlock errors.

Seven new range tests exercise exact charges, non-Clone moves, one-byte-below
preflight refusal, actual budget pressure, suffix identity/heap differences and
pinned rollback. Injected failures and excess capacities cover all five range
vector stages and eleven checkpoint-import stages, including a partially built
private store. Six deterministic Unix client tests keep duplicate descriptors
open across owner release, check repeated live-owner exclusion and preserved
request/marker bytes, and exercise initialization failure. These are implemented
test cases; qualification for this combined increment is recorded below.
No V1 wire or durable encoding changes are
introduced by either change.

Qualification for native first receipts, graph capture, range preflight and
client lock ownership — 2026-09-06:

- The full offline, locked workspace run passed **1,316 tests across 66
  unit/integration targets**, followed by all **19 doc-test targets**. This
  includes **182 Core tests**, **250 model tests**, **137 client tests** and all
  memory targets. The native owner suite contains **84 tests**, including the
  16 new first-receipt cases. Five graph-capture, seven range-preflight and six
  client lock regressions also passed. Evidence:
  `/tmp/focal-receipt-preflight-workspace.log`.
- The earlier workspace attempt exposed a client `Store(Locked)` refusal. Its
  isolated rerun passed, but investigation found that closing an original File
  need not release a lock while a duplicated description stays open. The new
  deterministic regressions exercise that lifetime directly; the successful
  workspace run includes the explicit-unlock fix. Two new tests initially
  omitted required private parent permissions; their fixtures now use `0700`,
  with production permission checks unchanged.
- All-target Clippy passed with `-D warnings`:
  `/tmp/focal-receipt-preflight-clippy.log`. The separate production no-panic
  gate passed: `/tmp/focal-receipt-preflight-production.log`. This increment
  introduces no per-object `Arc`; range plans borrow the exact existing base.
  Workspace formatting and the final diff checks also passed.
- The architecture checker verified **565 links, 37 imported source hashes and
  15 frozen domain vocabularies**. Native changes remain internal typed owner
  operations. The passing service/CLI/MCP/recovery tests exercise their current
  V1 paths; they do not constitute native wire/WAL activation or hosted
  cross-platform release qualification.

The next capacity work follows [18 §6.4](18-lifecycle-storage-upgrade.md): shared
funding through the actual allocation paths, exclusive owner-managed completion
entitlements, future root/neighbor growth and discrete record allowances, then
independent durable-storage and replica capacity qualification. Respondent
artifact/cycle and closing-testament transactions, adoption, complete graph
consequences and native durable/CLI/MCP activation remain open.

## Funded owner memory through custody and native publication — 2026-09-06

[MemoryBudget::funded_child](../../crates/focal-memory/src/budget.rs) now reserves
spendable capacity and the pool's metadata before returning a reusable owner
budget. Allocations issued from that pool consume its held capacity without
seeking fresh ancestor admission. Shrinking or dropping an allocation returns
credit to the pool. Ancestor categories move between Reserved and the actual
allocation kind; ancestor totals retain the original backing. Release restores
those categories before making local credit reusable, including concurrent
spending and mixed normal/funded descendants.

Ordinary-funded capacity can pay for Completion work while its complete original
ordinary charge remains held at the funding ancestors. Completion-funded capacity
cannot admit Ordinary work, including through descendants. Split, absorb and
shrink retain the existing source/category/lane checks. Each coarse owner pool
adds one shared counter allocation; pages and custody tokens use their existing
Allocation ownership rather than new per-evaluation sharing wrappers. The entire
backing, including metadata, remains held until the last pool handle, descendant
and issued allocation disappears. A small retained token can therefore keep idle
capacity reserved. There is no implicit trim or close; pools are intended for
reuse by the owner rather than creation for each domain object.

The actual construction paths now accept this funding:

- [RangePreparationPlan::build_in and build_in_with](../../crates/focal-memory/src/range_preflight.rs)
  require the range owner's budget or a descendant. The same source pays for
  input Pending, directory Roots, merge workspace and new Pages. Untouched pages
  retain their previous source. A private input guard drops the iterator and its
  remaining payloads before returning the input permit, including failure before
  the first page allocation. Prepared, published and pinned pages retain their
  debit until their last owning reference disappears.
- [Core::prepare_native_in](../../crates/focal-core/src/native/prepare.rs)
  accepts a trusted owner source and checks ancestry before spending. Operation
  semantics still choose the lane. Native scratch, changes and retained range
  construction use that source together; scratch returns only after its payloads
  have been dropped or moved into charged pages. Existing APIs use the original
  owner budget. Exact pending/committed retries still return their original
  outcomes without requiring another allocation or custody token.
- [Native evidence verification](../../crates/focal-evidence/src/native_artifact.rs)
  uses its existing budget argument. Passing the same funded source covers actual
  schema verification, authenticated reads and synced inline content. Its envelope
  shrinks to the retained custody token after workspace is discarded. Schema,
  corruption and capacity failures return credit without manufacturing custody.
  This remains proof of local storage; funding adds no replica or disk guarantee.

Eleven budget, five range, three evidence and three Core regression cases cover
pool metadata, ancestry/lanes, nested and concurrent refunds, full-parent pressure,
copy/refusal rollback, pinned lifetime and actual custody. The integrated owner
case completes Error → programmatic Pass → quality Pass, exact pending/committed
retries and first receipt while parent capacity is exhausted and original pages
remain pinned. Receipt acquisition still creates no testament.

Focused qualification passed **185 Core tests** and **34 evidence tests** in
`/tmp/focal-funded-focused.log`. The subsequent memory run passed **37 unit tests**
and all existing memory integration targets in `/tmp/focal-funded-memory.log`.
The first memory attempt exposed a test assumption about an expired snapshot
handle: its weak-control metadata intentionally stays charged until that handle
drops. The corrected test separately checks released funded backing, remaining
ReadPins metadata and final zero usage; production behavior was unchanged.
Full qualification for this increment:

- The full offline, locked workspace run passed **1,338 tests across 66
  unit/integration targets**, followed by all **19 doc-test targets**. It includes
  **185 Core tests**, **250 model tests**, **137 client tests**, **34 evidence
  tests** and every memory target. The native owner suite contains **87 tests**.
  Evidence: `/tmp/focal-funded-workspace.log`.
- All-target Clippy passed with `-D warnings` in
  `/tmp/focal-funded-clippy.log`; the separate production no-panic gate passed in
  `/tmp/focal-funded-production.log`. Workspace formatting and diff checks passed.
  No per-object or per-evaluation sharing wrapper was introduced; each reusable
  owner pool uses the existing shared budget lifetime mechanism.
- The architecture checker verified **570 links, 37 imported source hashes and
  15 frozen domain vocabularies**. Existing V1 CLI/MCP, network and recovery
  regressions passed. This adds no native wire or durable encoding and does not
  constitute a hosted cross-platform release qualification.

A funded pool does not assign an exclusive completion entitlement to each receipt
or begun evaluation. Automatic reservation at entry, a book that prevents pending
forks from promising the same share twice, future directory/neighbor growth,
discrete artifact/result/history allowances and complete retry/quality budgets
remain required. This increment provides reusable accounting capacity through
real allocation paths. It does not guarantee physical allocation, disk space,
replica capacity or eventual external-agent reporting, and it does not activate
native Session, WAL, wire, CLI or MCP formats.

## Exclusive native candidate ownership — 2026-09-06

[NativeOwner](../../crates/focal-core/src/native/owner.rs) now consumes a native
Core and retains all unpublished candidates in one bounded queue. Its constructor
precharges the queue and checks the allocator's actual capacity; refusal returns
the original Core so the caller can relieve pressure and retry. The owner adds
no per-candidate or per-object `Arc` allocation.

Preparation requires exclusive owner access and reads the complete pending chain
through a borrowed iterator, without allocating a temporary reference vector.
Callers receive an opaque process-local ticket containing an owner incarnation
and a checked, monotonically increasing serial. Committed and candidate views
expose borrowed rows, never the Core or an owned prepared candidate. A caller
cannot extract a candidate and use it to create a second managed branch.

Only the head can publish after the external log owner establishes durability.
An out-of-order, foreign or stale ticket changes nothing. A storage publication
refusal restores the identical head and ticket without allocating; later
candidates remain in their original chain. Exact retries return the original
pending ticket or committed outcome before queue or memory admission. Reusing a
request and numerical sequence after discard creates a fresh ticket, so delayed
callbacks cannot affect the replacement.

Discard removes a suffix from tail to head, dropping dependent candidate pages
before earlier candidates. Committed rows remain intact. The durable owner must
first establish that the discarded suffix cannot commit: a client timeout or
unresolved append does not justify discarding it. Owner destruction also drops
the pending suffix in reverse order. Snapshot leases pin committed state only.

This implements the exclusive candidate boundary required by
[18 §6.4](18-lifecycle-storage-upgrade.md). Per-evaluation completion envelopes,
the resource book and candidate resource deltas remain open. The managed owner
currently uses normal operation-lane admission; it does not automatically fund
Begin or receipt acquisition. Future directory/neighbor growth, discrete record
allowances, durable resource recovery, and disk/replica reservations still need
implementation and qualification before completion capacity can be promised.
Native wire, Session, WAL, CLI and MCP activation remain gated.

Thirteen [managed owner regressions](../../crates/focal-core/src/native/owner_tests.rs)
passed in `/tmp/focal-native-owner-focused.log`. They cover full queue/ancestor
pressure with allocation-free pending and committed retries; ordered publication;
foreign and discarded tickets; same-request/same-prefix replacement; owned-child
registry and clock rollback; injected retained-value copy failure; real local
custody and accepted-result discard/retry; snapshot isolation and expiry; counter
exhaustion; recoverable constructor refusal; and final accounting refunds. A
private test substitutes a foreign root to force lower-level publication refusal
and verifies restoration of the identical head and successors; the public API
does not permit that substitution.

Full qualification for this increment:

- The offline, locked workspace run passed **1,351 tests across 66
  unit/integration targets**, followed by all **19 doc-test targets**. Core now
  has **198 tests**, including **100 native tests**. Evidence:
  `/tmp/focal-native-owner-workspace.log`. The existing V1 compatibility,
  CLI/MCP, network and restart tests passed; this does not qualify native
  wire/WAL activation or hosted cross-platform release jobs.
- All-target Clippy passed with `-D warnings` in
  `/tmp/focal-native-owner-clippy.log`. The separate production no-panic gate
  passed in `/tmp/focal-native-owner-production.log`. Workspace formatting and
  diff checks passed. The new owner adds no sharing wrapper to domain rows or
  candidates; it uses one preallocated queue and borrowed read projections.
- The architecture checker verified **575 links, 37 imported source hashes and
  15 frozen domain vocabularies** in `/tmp/focal-native-owner-contracts.log`.

The next capacity step must combine this exclusive chain with the private
completion book and checked candidate resource deltas. Scalable publication
bounds require bounded leaf bytes and bounded directory path copying, rather
than reserving a copy of the current flat directory for every future report.
Those bounds, complete retry/quality envelopes and independent durable-storage
reservations remain open; this increment makes no completion-capacity promise.

## Bounded native leaf copies and import staging — 2026-09-06

The [shared leaf layout](../../crates/focal-memory/src/range_layout.rs) now bounds
ordinary page copies by bytes as well as entry count. `RangeConfig::page_bytes`
includes Page/Arc bookkeeping, entry-vector bookkeeping, inline entries and the
declared key/value heap. `max_entry_bytes` includes inline entry plus declared
heap, excluding page bookkeeping. Construction rejects limits that cannot hold
one empty entry. Input entries above their byte ceiling refuse during preflight,
before allocating or copying destination pages.

A larger admitted entry occupies an isolated singleton page. Inserting keys
beside that unchanged entry splits the incoming write set around its key and
shares its original page; neither its owned payload nor its key is copied.
Same-key replacement moves the new entry, and deletion removes the old one.
One streaming partitioner emits identical byte/count boundaries for preflight
and construction. Exact new-page accounting excludes shared singleton payloads,
while the new directory reference remains charged. Funded sources continue to
pay for the actual buffers and retained pages.

Checkpoint import now stages byte-bounded chunks, including oversized entries
individually. It preserves strict global key ordering and applies the same layout
and maximum-entry checks. Review also found and fixed an existing early-return
drop-order issue: staged payloads now drop before their payload reservations on
ordering, capacity or later-input refusal. The outer permit continues to cover
both staging vectors.

Native Core derives a maximum 64 KiB ordinary page charge and a complete entry
ceiling from its existing preparation allowance, claim/event container charges
and inline entry size. Tighter internal limits remain effective. These are
internal accounting limits, with no new mandatory user configuration. Generic
RangeStore defaults preserve the previous count-only layout, and no V1 wire,
hash, checkpoint or interpretation changes are introduced. The native owner
continues to receive and publish the same typed domain transactions.

The flat directory still costs O(number of pages) to copy. Bounded directory
path copying, complete completion envelopes, discrete record allowances and
durable capacity reservations remain required. This increment bounds incidental
leaf copies; it does not yet promise completion capacity or activate native
Session, WAL, CLI or MCP formats.

Focused qualification passed **55 memory unit tests and all memory integration
targets** in `/tmp/focal-layout-memory-qualified.log`, plus **204 Core tests** in
`/tmp/focal-layout-core.log`. The new coverage consists of:

- Twelve [leaf layout tests](../../crates/focal-memory/src/range_layout_tests.rs)
  for exact byte/count boundaries, maximum entries, singleton sharing, replacement
  and deletion, pending roots, real funded preparation under ancestor pressure,
  copy rollback, pins, input refusal and checkpoint layout.
- Two [map/import tests](../../crates/focal-memory/src/range_layout_model_tests.rs):
  three deterministic seeds run 1,080 mixed-size batches against a full-byte
  ordered-map oracle, with retained snapshots, discarded candidates, exact page
  charges and oversized sharing checks. The import case demonstrates that
  byte-bounded chunks fit a budget exceeded by count-only staging, preserve the
  original large payload addresses, invoke no Clone calls and refund all charges.
- Four [import cleanup tests](../../crates/focal-memory/src/range_import_tests.rs)
  observe actual payload destructors on duplicate/descending keys, oversized
  later input and insufficient later payload capacity. Each verifies that staged
  heap remains Pending-charged through destruction and all counters recover.
- Six [native layout tests](../../crates/focal-core/src/native/layout_tests.rs)
  cover finite derived ceilings, tighter supplied limits, actual pending
  Create/Post/Begin and ContentStore-verified reporting at small page bounds,
  oversized claim address preservation, and atomic refusal followed by a valid
  retry. Admission reporting still creates no respondent testament.

Full qualification for this increment:

- The offline, locked workspace suite passed **1,375 tests across 66
  unit/integration targets**, followed by all **19 doc-test targets**. This
  includes **204 Core tests** (**106 native**) and **55 memory unit tests**, plus
  all memory integration targets. Evidence: `/tmp/focal-layout-workspace.log`.
  Existing V1 fixture, CLI/MCP, network, replica and restart tests also passed.
- All-target Clippy passed with `-D warnings` in
  `/tmp/focal-layout-clippy.log`; the separate production no-panic gate passed
  in `/tmp/focal-layout-production.log`. Workspace formatting and final diff
  checks passed. No new per-row or per-object sharing wrapper was introduced.
- The architecture checker verified **582 links, 37 imported source hashes and
  15 frozen domain vocabularies** in `/tmp/focal-layout-contracts.log`.

The directory conversion is specified in section 6.5 of document 18: bounded
branches and height, no copied separator keys, touched-path preparation, complete
preflight charges, borrowed scan cursors, canonical import and failure/scale
qualification. It remains implementation work, followed by completion resource
ownership and the remaining independent lifecycle/durable activation work. These
checks do not establish native production activation or a global-scale result.

## Persistent page directory and bounded path construction — 2026-09-06

[RangeStore](../../crates/focal-memory/src/range.rs) now uses a persistent
[page directory](../../crates/focal-memory/src/range_directory.rs) instead of
copying a flat vector of every page on each mutation. Directory leaves contain
page handles; branches contain child-node handles and checked descendant page
counts. Non-root nodes contain 16–32 handles. Overflow splits, deletion borrows
or merges siblings, and unary roots collapse. A checked height ceiling derives
from the representable page count and minimum fanout. Coarse node/page sharing
preserves pending and snapshot lifetimes without a new per-row sharing wrapper.

Keys remain in their original entry pages. Routing borrows descendant minima;
node construction checks only bounded neighboring handles and their key bounds.
A borrowed cursor uses a fixed ancestor stack to walk ordered pages without heap
allocation or repeated root searches. Generic separator keys are never cloned.
These extra minimum/maximum descents have a bounded height cost; this change
makes no measured global-throughput claim.

The shared [group selector](../../crates/focal-memory/src/range_groups.rs) routes
sorted writes against immutable base boundaries. Preflight and construction
visit only touched leaves. Construction translates their ranks past earlier
splits/deletions and emits pages directly into persistent edits. Unchanged
oversized singleton pages remain in place while neighboring pages are inserted.
Empty batches share the directory and allocate only new outer-root metadata.
No whole-directory staging vector or scan remains in batch preparation.

The construction quote now separates exact entry-page charges from conservative
cumulative directory charges. The directory bound includes every issued node,
including temporary underfull and redistributed nodes, using a checked maximum
of `6 × height_bound + 2` node charges per elementary edit. A build debits actual
node/control-block/vector capacity before allocation and decrements its private
cumulative allowance; temporary-node refunds do not replenish that allowance.
The full quoted bound is not also retained as a second debit. Shared nodes keep
their original source, and failure drops provisional payloads before permits.
Outer-root metadata is constant. Additional peak/retained totals are upper bounds;
a smaller pool may fit the actual build. A quote itself reserves no capacity.

New qualification coverage includes:

- Eight [directory integration tests](../../crates/focal-memory/src/range_directory_tests.rs)
  exercise fanout boundaries, sparse rank shifts, deletion and root collapse,
  384 deterministic mixed batches against an independent ordered map,
  observable generic-key cloning, one-leaf quote growth through 4,096 leaves,
  pending forks, old scan continuations and snapshot isolation. A mixed update
  on a 2,049-leaf tree injects failure and excessive reported capacity at every
  observed buffer allocation, including after value copying, and checks exact
  budget recovery, unchanged base identity and valid old pinned reads.
- Two tests inside the directory module check internal subtree pointer identity
  at multiple levels and refusal one byte below the exact cumulative charge of
  a replacement path, followed by success at that exact charge.
- Existing preparation, funded-source, byte-layout and import tests now check
  exact new-page charges and actual retained Roots within the conservative bound.
  Their allocation faults cover every observed construction site rather than a
  fixed allocation count. Full ancestor pressure and actual insufficient-source
  refusal remain distinct from the conservative plan policy limit.

This completes the flat-directory replacement in document 18 §6.5. Completion
write envelopes, individual entitlements and candidate spending loans, durable
capacity reservations, the remaining independent lifecycle transactions and
native protocol activation remain required. The entry-page compaction, shared-
path batch optimization, distributed range movement and global load evidence
also remain outside this increment. V1 wire, hashes, checkpoint schemas and
replay interpretation are unchanged.

Executed qualification on macOS arm64:

- The offline, locked workspace suite passed **1,385 tests across 66
  unit/integration targets**, followed by all **19 doc-test targets**, with zero
  failures or ignored tests. This includes **65 memory unit tests**, all memory
  integration targets and **204 Core tests**. Existing CLI/MCP, V1 fixture,
  network, replica and restart tests passed. Evidence:
  `/tmp/focal-directory-workspace.log`.
- Strict workspace all-target Clippy and the separate production no-panic gate
  passed in `/tmp/focal-directory-clippy.log` and
  `/tmp/focal-directory-production.log`. Workspace formatting and diff checks
  passed. No dependency package or wire-format change was introduced.
- The architecture checker verified **590 links, 37 imported source hashes and
  15 frozen domain vocabularies** in `/tmp/focal-directory-contracts.log`.

The active P00–P20 goal remains open. The next storage step is the complete
completion envelope and owner-controlled reservation book described in document
18 §6.4; a bound for today's prepared write is not an entitlement for all future
accepted reports, their persistent history or durable evidence custody.

## Future write bounds and Admission completion prerequisites — 2026-09-06

[RangeWriteEnvelope](../../crates/focal-memory/src/range_envelope.rs) now bounds a
future write independently of current ledger occupancy. Its explicit limits cover
changed keys, deleted keys, summed incoming Put heap and actual input-vector
capacity. It binds the owner incarnation and uses immutable page limits and the
owner's total allowance to bound possible old leaves and directory height.

For at most `m` changed keys, retained subsequences of touched ordinary leaves
still fit their original page limits. Incoming rows split those subsequences
into at most `m + puts` fitting runs and add at most `puts` singleton runs.
The greedy partition therefore emits at most `3m` new pages; that bound also
covers empty deletion groups and edits around shared oversized leaves. The
quote includes new page bookkeeping, retained ordinary payloads, incoming heaps,
separate deletion-key accounting, merge scratch and cumulative directory edits.
Put-only writes pay no hypothetical large deletion-key charge. `check_plan`
validates every component against the actual owned-input plan before spending
an allowance derived from it. Neither obtaining nor checking a quote reserves
memory or promises completion.

Native preparation now separates [checked request admission](../../crates/focal-core/src/native/prepare.rs)
from construction. Exact committed/pending retries resolve before new workspace
or source selection. A fresh capability owns the input and borrows the exact
base, while binding intent, context, sequence and internal limits. Construction
rechecks source ancestry. This capability validates request/base identity; actual
claim, evaluation, actor, attempt and custody authorization still runs in the
native transaction. It cannot by itself grant an evaluation's resource loan.
`NativeOwner` uses this same boundary without exposing Core or allocation sources.
The bounded capability stays on the stack, avoiding a heap allocation before
funding selection.

[ConstructionBudget](../../crates/focal-core/src/native/prepare_budget.rs) derives
operation-specific temporary allowances. Begin uses four writes, one evaluation
extra, one event and no changed claim. Reports allow nine writes, or eleven with
the first derived claim failure, four extras and at most four events. Actual
counts are checked before the final change vector allocation. Extras replacement
buffers coexist under their permit, and descriptor/claim heaps still obey the
existing Scratch contract. Other operations preserve their previous allowances.
Begin and Report also check their actual range plans against the occupancy-
independent write envelope. An actual Begin now succeeds with less parent
headroom than the former all-operation workspace reservation required.

The [Admission preflight](../../crates/focal-core/src/native/admission_budget.rs)
fixes two eventual-progress gaps. It refuses Begin when the configured write set
cannot hold the eleven-row failure path, and checks projection work after every
Admission member has a result. Previously, a cohort could fit the read budget
when first begun but exceed it as sibling results arrived. The calculation lives
beside the actual model projection and includes complete declaration/member
checks and publication-uniqueness visits. A real thirty-member cohort requires
4,278 visits: the old 4,096 allowance now refuses before beginning, while the
exact sufficient allowance supports every real verified report.

The native Admission owner helper also checks policy-evidence requirements in
all reachable phases. It cannot admit a grant-free programmatic phase whose
later required quality phase needs a registry the owner does not have. General
explicit-owner evaluation paths retain their real policy-evidence checks; no
unknown schema or authorization is accepted automatically. Reports from eligible
already-begun siblings after `PostFailed` retain their existing authorization.

New tests cover:

- Seven memory envelope tests: growth, pending bases and pinned roots, explicit
  input/owner guards, arithmetic limits, put-only pricing, pre-funded construction
  after complete parent exhaustion, 480 deterministic mixed-layout batches and
  exact empty-delete/oversized-reuse accounting.
- Four checked-admission tests: source choice after parent exhaustion, foreign
  source refusal, retries under full queues/budgets, and a real small Begin under
  memory pressure.
- Nine construction-budget tests: operation bounds, actual nine/eleven-write
  boundaries, tight scratch, coexisting buffers, prior-operation parity and
  overflow.
- Seven native Admission tests and one model policy test: future cohort work,
  exact visit limits through real ContentStore reports, failed-write capacity,
  actor precedence, pending Post state, missing definitions and unsupported future
  quality policy without publication.

The focused runs passed **72 memory unit tests and all memory integration tests**
in `/tmp/focal-envelope-memory.log`, plus **224 Core and 251 model tests** in
`/tmp/focal-envelope-core-model.log`. These are prerequisites for the full
completion guarantee. Complete descriptor/visibility/input and pinned-schema
contracts, the full remaining attempt-chain envelope, discrete slot reservations,
owner-controlled completion credit and exclusive candidate spending, evidence
verification funding and durable capacity reservations remain open. No native
Session/WAL/CLI/MCP activation or global deployment claim follows from this work.

Full qualification for this increment passed on macOS arm64:

- **1,413 tests across 66 unit/integration targets**, followed by all **19
  doc-test targets**, with zero failures or ignored tests. Evidence:
  `/tmp/focal-envelope-workspace.log`. This includes the current CLI/MCP,
  authenticated network, replicated-service, restart and frozen V1 suites.
- Workspace all-target Clippy with `-D warnings`, the separate production
  no-panic gate, formatting and diff checks passed. Evidence:
  `/tmp/focal-envelope-clippy.log`, `/tmp/focal-envelope-production.log` and
  `/tmp/focal-envelope-fmt.log`. The changes add no Arc wrapper or dependency.
- Architecture checks preserve **37 imported source hashes and 15 frozen
  vocabularies**, and resolve **598 links** in `/tmp/focal-envelope-contracts.log`.

Document 18 §6.6 now specifies the next expandable owner-funding primitive:
immutable structural ceilings, atomic available-credit admission, explicit growth
and trimming, coarse backing lifetime and concurrent refund ordering. It remains
implementation work, followed by the complete owner entitlement/loan mechanism.
The active P00–P20 goal remains open.

## Expandable owner memory funding — 2026-09-06

[ElasticFundedPool](../../crates/focal-memory/src/budget_elastic.rs) adds one
non-Clone resize controller over the existing shareable accounting source.
`elastic_funded_child(lane, ceiling, initial_capacity)` permits a metadata-only
initial source. `grow` reserves parent backing in its original lane before making
additional credit usable; `trim_unused` atomically acquires idle credit before
returning the corresponding backing. Structural limits stay immutable, so range
envelopes remain valid across resizing.

The [budget integration](../../crates/focal-memory/src/budget.rs) distinguishes
ordinary, fixed-funded and elastic-backed counters. Elastic allocations acquire
credit before local usage or ancestor category transfers. A refund restores every
ancestor category and local usage before publishing reusable credit. Both local
admission error paths return acquired credit. Trimming therefore cannot release
an in-flight allocation merely because a diagnostic usage snapshot has not yet
observed it. The unique controller serializes resizes while issued allocations
may reserve, split, shrink and drop on other threads.

Backing follows the source through descendants, prepared pages and pinned roots;
dropping the controller alone does not release it. Final counter destruction
returns the remaining aggregate and metadata once. Fixed-funded backing retains
its existing Allocation destructor and lifetime. Nested pools charge their full
backing as an issued allocation in their parent. Metadata is never spendable or
trimmable, and Completion funding cannot be used for Ordinary work at any depth.

There is no per-growth permit vector, mutex, per-evaluation source or new domain
object Arc. The pool uses the existing coarse atomic-counter ownership shared by
allocations that can move between threads. These are accounting reservations;
recoverable allocation errors remain typed and disk capacity is independent.

NativeOwner integration remains required. The pool knows issued bytes, not
logically promised idle credit. An exclusive completion book must preserve those
promises, authorize each evaluation's spending, and journal candidate resource
effects through publication and tail discard. Pinned schemas, descriptor and
attempt-chain bounds, discrete slots, custody verification, durable reservations
and recovery remain required before completion guarantees or native activation.

The focused run passed **87 memory unit tests and all memory integration tests**
in `/tmp/focal-elastic-memory.log`. Fifteen new tests cover:

- [Eight accounting tests](../../crates/focal-memory/src/budget_elastic_tests.rs):
  metadata-only creation, exact growth/trim refusal, full-parent reuse,
  normal/fixed/elastic hierarchies and both lanes, sibling isolation, overflow and
  depth, controller/source/descendant/zero-byte-permit lifetimes, and four-worker
  split/shrink/absorb churn concurrent with resizing across sixteen rounds.
- [Four controlled interleavings](../../crates/focal-memory/src/budget_interleaving_tests.rs):
  acquired credit before visible usage; refunded categories before reusable
  credit; parent growth funding before credit publication; and trim acquisition
  before parent refund. Source-scoped, one-shot test hooks pause the actual
  operations at these boundaries. Competing reservations/trims refuse until the
  appropriate publication; final accounting returns exactly to baseline.
- [Three range tests](../../crates/focal-memory/src/range_elastic_tests.rs):
  unchanged future-write bounds across resizing and preparation under parent
  exhaustion, protection for prepared suffixes and pinned roots in both drop
  orders, and failed-copy refund followed by trim, regrowth and successful retry.
  Existing fixed-funded tests remain unchanged and pass.

Full qualification for this increment passed on macOS arm64:

- **1,428 tests across 66 unit/integration targets**, followed by all **19
  doc-test targets**, with zero failures or ignored tests. Evidence:
  `/tmp/focal-elastic-workspace.log`. Current-profile CLI/MCP, authenticated
  networking, replicated service, restart and frozen V1 regressions all passed.
- Workspace all-target Clippy with `-D warnings`, the separate production
  no-panic gate, formatting and diff checks passed. Evidence:
  `/tmp/focal-elastic-clippy.log`, `/tmp/focal-elastic-production.log` and
  `/tmp/focal-elastic-fmt.log`. No dependency was added.
- Architecture checks resolve **604 links**, preserve all **37 imported source
  hashes** and validate **15 frozen vocabularies**. Evidence:
  `/tmp/focal-elastic-contracts.log`.

The next increment is owner integration of protected per-evaluation capacity and
exclusive spending. This pool does not activate native durable lifecycles or
complete the CLI/MCP, deployment and global-scale plan. The P00–P20 goal remains
active.

## Native Admission completion ownership — 2026-09-06

[NativeOwner](../../crates/focal-core/src/native/owner.rs) now owns one private
[CompletionBook](../../crates/focal-core/src/native/completion_book.rs) over the
expandable funded source. Before accepting Begin, it checks the actual effective
evaluation and reserves the bounded future report chain, including retry,
fallback, diagnostic evidence and optional quality phases. Ordinary admission
funds that responsibility; authorized reports spend held capacity even when the
parent budget has no available bytes. Low-level Core construction remains a
separate mechanism and does not itself promise future completion.

The [completion envelope](../../crates/focal-core/src/native/completion_envelope.rs)
prices descriptor ingress, custody verification, staging and retained range
versions. It protects a Required evaluation's possible parent-failure write,
declared parent scope growth and the exact registered cohort. It bounds compound
descriptor construction as well as individual dimensions; minimum diagnostic
descriptors and durable content references remain representable. The actual
report plan must fit the admitted envelope before range construction.

[Pinned schema quotes](../../crates/focal-core/src/native/completion_schemas.rs)
cover every reachable proof/diagnostic schema, with one uniquely owned, charged
buffer per grant. A report's trusted registry must still declare the same hash
and maximum verification size. Unsupported or changed contracts refuse before
custody I/O. The schema identity does not fingerprint a custom verifier's runtime
implementation; the embedding must honor its declared immutable contract.

The [shared authority check](../../crates/focal-core/src/native/admission_authority.rs)
borrows the actual claim, declaration, registration and evaluation. It verifies
the actor, revision, attempt, deadline, provenance, schema, artifact identity and
input visibility before the owner lends completion capacity or touches content
storage. `prepare_with_custody` performs real verification with that source;
`prepare_evidenced_with_schemas` accepts an embedding's exact verified capability.
Both resolve committed and pending retries before new authority, funding or
custody work. Neither launches validators, workers, tools, skills or agents.

Native accounting now tracks cumulative event rows explicitly. Each candidate
preserves outstanding report slots for artifacts, identities, results, events,
outcomes and sequences, plus evaluation/parent revision and candidate-counter
margins. One control outcome, sequence and ticket remain while grants are live.
That finite-record margin does not promise RAM or disk for an arbitrary
cancellation closure. Actual report failures consume four events; ordinary
reports consume three. Rejected candidates publish no partial event counts.

Non-cloneable journals track provisional credit alongside each pending candidate.
Head publication finalizes its journal without repainting credit already changed
by later reports. Tail discard drops candidate pages before restoring the exact
credit and metadata buffer. A failed/discarded Begin returns exactly its added
pool backing, including when older candidates remain pending. General excess
stays held while any candidate could roll back. The shared workspace keeps its
high-water bound until no grants are live, avoiding a whole-book scan at every
completion. Issued and pinned pages retain their own charges after grant removal
and owner teardown.

`with_schemas` reconstructs live begun/unfenced grants from an already-existing
native Core before accepting mutations. It includes still-eligible chains after
PostFailed or first receipt; terminal summaries do not fabricate evaluation
fences. A refused transfer returns the original Core intact. This is in-memory
ownership reconstruction, not native WAL recovery. It creates no respondent
testament: respondents still author their work or failure evidence independently.

Focused qualification passed **264 Core tests** in
`/tmp/focal-completion-core.log`. Forty new tests cover the envelope, schema pins,
authority-before-custody, actual event totals, private journal accounting and
owner integration. The owner tests run a real five-report retry/fallback/quality
chain with a fully exhausted parent, retain multiple pending reports, discard and
retry results, restore grant-buffer growth, reconstruct live evaluations after
parent failure/receipt, retain pinned pages after teardown, and protect the last
candidate identities for promised reports. Model/evidence tests additionally
exercise shared report authorization and immutable verification quotes.

Full qualification for this increment passed on macOS arm64:

- **1,476 tests across 66 unit/integration targets**, followed by all **19
  doc-test targets**, with zero failures or ignored tests. Evidence:
  `/tmp/focal-completion-workspace.log`. Current-profile CLI/MCP, authenticated
  networking, replicated service, restart and frozen V1 regressions all passed.
- Workspace all-target Clippy with `-D warnings`, the separate production
  no-panic gate, formatting and diff checks passed. Evidence:
  `/tmp/focal-completion-clippy.log`, `/tmp/focal-completion-production.log` and
  `/tmp/focal-completion-fmt.log`. No dependency or domain-object Arc was added.
  Report preflight borrows retained evaluation state rather than copying a full
  state into each authorization frame or boxing that frame.
- Architecture checks resolve **615 links**, preserve all **37 imported source
  hashes** and validate **15 frozen vocabularies**. Evidence:
  `/tmp/focal-completion-contracts.log`.

**Remaining boundaries:** grant lookup uses a sorted owner-local vector, so
insertion and removal still shift O(N) entries. This is not a qualified large
shard index; [18 §6.7](18-lifecycle-storage-upgrade.md#67-integrated-nativeowner-ram-completion-contract)
specifies a uniquely owned indexed balanced-tree replacement. Disk-space and
replica-custody reservations, durable native encoding/recovery, respondent
artifact/testament and Work envelopes, Session/CLI/MCP activation and the
deployment/global-scale acceptance work remain open. The P00–P20 goal remains
active.

## Indexed native completion grants — 2026-09-06

The private [CompletionIndex](../../crates/focal-core/src/native/completion_index.rs)
replaces CompletionBook's sorted grant vector with a uniquely owned AVL tree.
Its integer links refer to stable physical slots. Rotations and two-child
deletion preserve every surviving grant's slot; a vacancy list permits reuse
without moving other grants. Lookup, report updates, removal and insertion into
available capacity touch logarithmic paths. The cursor seeks one parent's grant
interval and walks it with constant cursor storage.

Subtree metadata maintains the exact maximum active workspace. Retiring a large
grant therefore lowers the logical shared requirement while smaller grants
remain active. Candidate journals still protect prior promises: general pool
trimming requires an empty pending queue, and rollback restores the exact credit
and workspace maximum. Actual retained pages remain separately charged.

Geometric buffer growth remains O(N), reserves the full replacement before
allocation, and keeps the emptied original buffer charged in the Begin journal.
Rollback checks that added slots can be removed, unlinks only those vacancies,
and moves the retained prefix back without allocating. Stable slots preserve
intervening committed deletions rather than restoring an obsolete root or
resurrecting grants. Growth rollback is also O(N); this implementation does not
claim every Begin or discard has constant or logarithmic latency.

[ParentFacts](../../crates/focal-core/src/native/completion_envelope.rs) now checks
and prices the actual parent and registry once. Each grant pins its exact
registration ordinal during Begin or reconstruction. Affected-parent checks
seek the real grant interval, perform scalar envelope checks and verify each
saved ordinal against the actual registration key. A batch with several
ChildRegistered events checks the cohort at the parent's final revision once.
This removes repeated policy hashing and nested membership scans without
accepting a missing, reordered or substituted registration cohort.

Bounded searches, repairs and cursors reject detected invalid links rather than
treating them as empty subtrees. An inconsistent index remains poisoned; the
owner refuses fresh work, and the book checks health before using maxima or
returning funding. The cursor checks key ordering, reciprocal parent links and
bounded output. These are fail-closed private ownership guards, not an automatic
repair or durable recovery mechanism. The implementation adds no unsafe code,
Arc, per-grant shared wrapper or dependency.

Focused qualification passed **280 Core tests**, including sixteen new tests,
with zero failures or ignored tests. Evidence: `/tmp/focal-index-core.log`.

- Ten index tests cover all AVL rotation shapes, physical slot preservation,
  6,000 deterministic edits against an ordered-map oracle, exact maxima and
  lower bounds, cohort cursors, parent-budget/capacity refusal, nested growth
  rollback and allocation refunds. Structural node-touch bounds cover sustained
  churn at 64, 512 and 4,096 entries. Corruption cases cover missing roots/children,
  parent/cursor cycles, invalid reciprocal links and vacancy self-cycles; later
  mutations refuse without invoking value callbacks or changing budget counters.
- Three parent-fact tests cover a real mixed cohort, policy/registry substitution,
  exact heap limits and phase/revision margins. Existing actual child-growth
  coverage now checks both cached and direct envelope paths.
- Three book integration tests cover heterogeneous workspace retirement and
  rollback after an older commit with retained page debt, exact ordinal refusal,
  and measured cohort visits for one versus four actual ChildRegistered facts.

Workspace all-target Clippy with `-D warnings`, the separate production no-panic
gate, formatting and diff checks passed on macOS arm64. Evidence:
`/tmp/focal-index-clippy.log`, `/tmp/focal-index-production.log` and
`/tmp/focal-index-fmt.log`. Architecture verification resolves **621 links**,
preserves all **37 imported source hashes** and checks **15 frozen vocabularies**
(`/tmp/focal-index-contracts.log`).

The broader 1,476-test workspace run belongs to the preceding increment; this
increment's behavioral qualification is the affected Core suite. Structural
bounds establish the index's operation shape, not fleet throughput or geographic
failure tolerance. Native durable codecs/recovery, disk and replica reservations,
respondent/Work ownership and complete CLI/MCP activation remain the next
integration work. The complete deployment and P00–P20 goal remains active.

## Native respondent evidence and authored response cycles — 2026-09-06

The typed native owner now admits respondent work and diagnostic artifacts,
claimant artifact observations, explicit response closure, response posting and
claimant response receipt. These are in-process native transactions; the running
Session and its CLI/MCP still select the earlier durable profile. Acquiring a
receipt creates no testament. The respondent supplies the summary, confidence
and explicit Complete, Partial, Refused, Impossible, Interrupted or Failed outcome
when closing its work cycle. Every non-Complete close requires actual typed error
evidence. Diagnostics can close a failed cycle with no requested outputs present;
their existence does not count as successful output or validation.

- [Immutable work provenance](../../crates/focal-model/src/lifecycle/artifact_descriptor.rs)
  binds the claim, receipt, cycle and output-slot or diagnostic role. Identical
  output/error bytes in different cycles have distinct descriptor identities.
  Native descriptors without work provenance retain their prior hashes; V1
  content, commands and decoding are unchanged. Result and work roles are mutually
  exclusive. Input existence and inherited visibility remain owner-checked.
- [Work submission](../../crates/focal-core/src/native/work_artifacts.rs) checks
  the actual holder, current receipt, declared slot, cycle, immutable provenance
  and capacity before the custody owner invokes schema verification or content IO.
  Successful custody is bound to the exact request and descriptor. Artifact,
  content identity, work/diagnostic row and cycle membership publish atomically.
  Separate work and diagnostic counts preserve diagnostic entry room when the
  configured work count is reached; this is not yet a future memory/disk guarantee.
- [Response closure](../../crates/focal-core/src/native/responses.rs) walks only
  the owner-maintained cycle membership, with bounded counts and exact slot
  membership checks. Each evidence insertion changes a cycle head and fixed rows;
  it does not copy an ever-growing member array or scan unrelated claims. Closing
  gathers and sorts the bounded actual membership and checks the participant's
  complete work and diagnostic manifest. It attaches every work row, freezes the
  response and records the claim observation in one candidate. No partial
  manifest, earlier-cycle evidence or diagnostic-as-output substitution is accepted.
- Generated, Posted and Received responses are separate stored transitions. The
  first response's claim observations are TestamentGenerated and
  TestamentAcknowledged; later cycles retain explicit prior-response lineage and
  same-phase observation history. Claimant observation may independently advance
  unattached work or a still-current-entitlement posted response after claim
  cancellation. It preserves the terminal claim, its revision and acceptance cut.
  Posting a new response still requires an open entitlement.
- Model construction and row copies are fallible, precharged and reconcile actual
  capacities. Claim history reserves one new response entry before observation.
  Admission completion grants price the full authored maximum response history,
  so a live Observe Admission report remains admissible after later response
  cycles. Impossible history caps refuse before a completion promise is made.
  [Static closure bounds](../../crates/focal-core/src/native/response_budget.rs)
  also prevent accepting a work set whose attachments cannot fit one atomic
  closing batch. Full future respondent completion reservations remain open.
- Borrowed committed/pending and expiring snapshot reads expose independent work,
  diagnostic and response rows. Compact history retains their exact before/after
  revisions. Exact retries bind the full authored report and resolve before new
  custody or capacity work. Discarding a pending close restores unattached work
  and the original parent; old pinned reads retain their observed prefix.

Qualification: the unfiltered library invocation for Core, model and evidence
passed **598 tests** (297 Core, 264 model, 37 evidence), with no failures or ignored
tests: `/tmp/focal-response-tests.log`. The subsequent **10-test owner response
suite** adds the actual live Admission-grant/report-through-two-response-cycles
case; it passed in `/tmp/focal-response-focused.log`. It also covers every reported
failure outcome, absent/partial/duplicate/foreign manifests, role checks before
schema work, explicit posting and receipt, cycle provenance, snapshot isolation,
tail discard, exact retry/conflict, attachment/receipt races, late observations
after cancellation and a real nine-row closing boundary. Model tests additionally
sweep response copy/close allocation failures and preserve terminal report stamps.

Workspace all-target Clippy with `-D warnings`, the separate production no-panic
gate and formatting pass on macOS arm64. Evidence:
`/tmp/focal-response-clippy.log` and `/tmp/focal-response-production.log`.
Architecture checks preserve all **37 imported source hashes** and **15 frozen
vocabularies**; the final link check is recorded in
`/tmp/focal-response-contracts.log`. Whitespace checks pass. No unrelated network
or full-workspace behavioral qualification is claimed for this increment.

This increment introduces no Arc, unsafe code, thread or dependency package.
Required next integration includes typed production/receipt failure transitions,
Receipt/Increment/WholeWork materialization and reports, aggregation/audit/graph
consequences and receipt adoption, respondent completion RAM/disk/replica capacity,
native codecs/import/recovery, WAL/Session/quorum activation and CLI/MCP dispatch.
The earlier full-workspace qualification remains a separate recorded run. These
native tests do not establish durable activation or geographic/scale acceptance;
the complete P00–P20 goal remains active.

## Native work failures and pure Receipt results — 2026-09-06

The native owner now records failed production, claimant rejection of unattached
work, and pure Receipt checks when the claimant explicitly receives a posted
response. Receipt does not author a testament, invoke a participant tool or
establish work quality. These transactions remain in-process native operations;
the running durable Session and CLI/MCP still select the earlier profile.

- [Failure transactions](../../crates/focal-core/src/native/work_failures.rs)
  reuse an actual respondent Production diagnostic as the GenerationFailed work
  object. They allocate no fictitious output or second artifact. Claimant
  rejection retains the original output and stores a distinct schema-checked
  error artifact bound to its exact identity, receipt, cycle and rejection reason.
  Actor, revision, provenance, inherited visibility and capacity checks precede
  custody IO. A claimant may observe/reject unattached work after parent
  cancellation without changing the terminal claim.
- [Response closure](../../crates/focal-model/src/lifecycle/evidence_report.rs)
  retains terminal failed work in a separate immutable `failed_work` collection.
  It attaches only Generated/Received outputs. Binding, slot, failure state and
  diagnostic enter the report stamp and all copy/construction charges. The real
  Production diagnostic may also be the respondent's report diagnostic. A
  claimant's rejection alone does not satisfy the respondent's obligation to
  author error evidence for a non-Complete outcome. An authored Complete outcome
  remains a report, not an acceptance verdict.
- [Pure Receipt publication](../../crates/focal-core/src/native/delivery.rs)
  checks the complete stored declaration set and derives readiness from the
  exact claimant-observed response, receipt, cycle and frozen report stamp. It
  records an artifact-free Pass without an external attempt, reporter or tool.
  Response receipt, Ready materialization, delivery result and claim observation
  publish in that history order in one atomic candidate. A receipt at/after its
  declared deadline retains Ready with no Pass; the separate trusted deadline
  action remains open. Receipt after terminal/local completion is observation
  only and creates no acceptance cohort.
- [Registration bounds](../../crates/focal-core/src/native/response_budget.rs)
  count Admission declarations once, plus every authored response's Delivery,
  WholeWork slot and possible Increment targets. First receipt refuses a registry
  that cannot hold that complete count; it never silently reduces the authored
  response allowance. Receipt expands and precharges the complete cohort buffer
  once. Admission completion funding now includes future registration growth and
  preserves existing row positions as responses arrive. These structural guards
  are not future respondent RAM, disk or replica reservations.
- Borrowed reads expose delivery results separately from evidence-bearing
  evaluator results. Exact retries resolve before new work, pinned reads preserve
  old prefixes, and pending production/rejection/response chains discard together
  with their artifact identities, slot membership and memory charges.

Qualification: **631 unfiltered library tests pass** (320 Core, 274 model,
37 evidence), with no failures, ignored or filtered tests:
`/tmp/focal-failure-delivery-tests.log`. The focused **26-test response suite**
also passes in `/tmp/focal-failure-delivery-focused.log`. Coverage includes real
ContentStore custody, both Generated/Received rejection, actor and provenance
refusals, missing/schema-invalid evidence, mixed successful and failed work,
explicit respondent closure, out-of-order response receipt, deadline handling,
terminal observations, exact retry, snapshot isolation, atomic rollback and
first-responsibility capacity refusal. The full suite also checks late Admission
reports through all authored response cycles and bounds future target families.
The existing model assertion for attaching failed work now expects
InvalidManifest: failed evidence is valid in a response, but not in its output
attachment manifest.

Workspace all-target Clippy with `-D warnings`, the separate production no-panic
gate and formatting pass on macOS arm64. Logs:
`/tmp/focal-failure-delivery-clippy.log` and
`/tmp/focal-failure-delivery-production.log`. Architecture checks verify **638
links**, **37 imported source hashes** and **15 frozen vocabularies** in
`/tmp/focal-failure-delivery-contracts.log`; whitespace checks pass. These are
affected-library behavioral checks and workspace static checks, not a new full
workspace behavioral or deployment qualification run.

No Arc, unsafe code, thread or dependency package was introduced. Remaining work
includes Increment/non-Receipt WholeWork materialization and reporting,
aggregation/audit/graph consequences and receipt adoption, trusted deadline and
fence actions, respondent completion RAM/disk/replica guarantees, native
codecs/import/recovery, WAL/Session/quorum activation and live CLI/MCP dispatch.
Cross-platform release and the full deployment, geographic failure and scale
qualification remain open under P00–P20.

## Native Increment checks and target sealing — 2026-09-06

Submitting a native work output now registers every declared Increment check
against its exact immutable content, original receipt and response cycle in the
same candidate. `BeginIncrement` and `ReportIncrement` use the authorized peer's
actual declaration and attempt. Focal records the result; the participant executes
its own programmatic or agentic check. These operations remain in-process native
transactions pending durable Session activation and live CLI/MCP dispatch.

- [Materialization](../../crates/focal-core/src/native/increments.rs) resolves
  the complete declaration set and precharges one expanded registration buffer.
  Work, slot/cycle membership, Ready evaluations and registration history publish
  together. First receipt checks the complete submission shape; partial cohorts
  never become visible. Registration-only writes preserve the claim lifecycle
  and binding while recording an explicit registration fact.
- [Authority](../../crates/focal-core/src/native/increment_authority.rs) resolves
  the real work/slot/cycle and descriptor, full registered target and designated
  evaluator. Original content/receipt/cycle remain pinned through later work
  receipt, response closure/posting/receipt and later response cycles. Results
  carry exact evaluator/attempt/schema provenance and inherit mandatory target
  visibility even when no explicit input list is supplied. Input consistency and
  producer checks precede custody IO for both Admission and Increment reports.
- Required Increment sources whose mandatory visibility cannot fit a future
  result refuse before work/custody admission; diagnostic-only failure testimony
  remains possible. Observe-only sources may publish, but an unfundable Begin
  refuses without responsibility. Begin and owner reconstruction recheck the
  actual immutable source's label count, visit bound, individual label sizes and
  minimum diagnostic descriptor size. Descriptor limits exclude allocator
  bookkeeping; the owner separately funds those allocations.
- The completion book now funds Increment's complete retry/fallback/quality
  chain before Begin. Each report writes artifact, identity, evaluation, accepted
  result, three facts, metadata and request outcome. Required Increment failure
  does not carry Admission's parent-failure surcharge or terminalize the working
  claim/artifact. The indexed book preserves exact target keys, registration
  ordinals, response-history growth and funding through pending rollback and
  owner reconstruction. This is RAM report funding, not a new disk/replica promise.
- Claimant `SealIncrementTargets` freezes the actual registered set. It does not
  revoke registered checks or close the respondent's cycle; diagnostics and
  testimony remain independent. New target-producing submissions refuse after
  sealing. Repeated sealing is inert, and discarded pending sealing restores the
  original membership state. Borrowed committed, pending and expiring snapshot
  reads expose the complete registration set and its seals.
- An already registered Increment may begin after claimant receipt rejection:
  its original output bytes still exist and the evaluator must be able to assess
  them. The [model](../../crates/focal-model/src/lifecycle/validation_increment.rs)
  requires live original authority, preserves ReceiptFailed and its diagnostic,
  and never attaches or rehabilitates that output. GenerationFailed has no
  submitted product eligible for this cohort. This resolves Sylk's WholeWork
  dispatch rule separately from Focal's Increment phase; no automatic Incomplete
  is invented. Controls, adoption, deadlines and evaluation fences still reject
  unauthorized continuation.

Qualification: **659 unfiltered library tests pass** (341 Core, 281 model,
37 evidence), with no failures, ignored or filtered tests:
`/tmp/focal-increment-tests.log`. They include **16 actual owner Increment tests**
and **7 model Increment tests**, exact funded descriptor boundaries and
cross-family grant refusal. Actual ContentStore custody covers the five-report
retry/fallback/agentic-quality chain under exhausted general memory, independent
response progress, rejected-output assessment, owner reconstruction with live
funding, cancellation, snapshot isolation, exact retry and atomic rollback.
Admission and frozen V1 regressions pass in the same invocation.

Workspace all-target Clippy with `-D warnings`, the separate production no-panic
gate and formatting pass on macOS arm64:
`/tmp/focal-increment-clippy.log` and `/tmp/focal-increment-production.log`.
Architecture checks verify **648 links**, **37 imported source hashes** and
**15 frozen vocabularies** in `/tmp/focal-increment-contracts.log`; whitespace
checks pass. No new full-workspace behavioral, platform-release or geographic
qualification is claimed by these affected-library and workspace static checks.

No Arc, unsafe code, thread or dependency package was introduced. Complete
non-Receipt WholeWork materialization/reporting, atomic acceptance/audit/graph
consequences, deadline/fence actions and receipt adoption remain required. Native
codecs/import/recovery, WAL/Session/quorum activation, live CLI/MCP operations,
respondent completion and durable/replica reservations, deployment journeys and
scale qualification remain open. Registration insertion still scans bounded
existing membership per new check; this increment makes no throughput claim.

## Native WholeWork registration and independent delivery order — 2026-09-06

Claimant `ReceiveResponse` now registers the complete immutable WholeWork slot
cohort alongside pure Receipt, using one prepaid registry expansion. Every
Required and Observe check pins the actual attached work binding or explicit
MissingSlot, response binding, receipt and authored cycle. Work checks remain
Ready; receiving a failure testament or passing its Receipt does not begin a
validator, fabricate result evidence or decide acceptance. The
[owner implementation](../../crates/focal-core/src/native/work_checks.rs) resolves
the actual closed cycle, output descriptor/provenance and exact attachment.

First responsibility qualifies both receive cohorts together. With `D` Delivery
and `W` WholeWork checks, the complete receive batch requires at most
`4*D + 2*W + 6` rows. The candidate stages response/claim observation, complete
registration, evaluation/results, counts and events atomically. Expired checks
remain Ready; late receipt after claim terminalization remains observational.
Pure Receipt results now preserve their original event ordinal as well as sequence
for later chronological acceptance reconstruction.

The [WholeWork model helpers](../../crates/focal-model/src/lifecycle/validation_work.rs)
materialize from actual received source rows and gate Begin with a checked
Required Increment decision. Missing Required evidence can produce the existing
artifact-free Incomplete result at entry; Observe missing targets are suppressed.
Begun reports retain exact source authority through ordinary terminal outcomes,
while explicit controls, adoption and deadlines fence them. No native WholeWork
Begin/report command is enabled by these model helpers.

Two integration regressions corrected real lifecycle/completion gaps:

- Receipt order is independent of authored cycle order. The first actual received
  response under the current entitlement advances the claim to
  TestamentAcknowledged. An earlier still-Posted response keeps its history and
  may be received later without regressing the claim's attained phase. Native
  response #2 therefore need not wait for response #1 delivery to register or
  become eligible for checking. Historical V1 execution remains unchanged.
- Admission reports reduce the Admission gate only while the parent is Posted.
  Later reports still verify their exact registered attempt, actor and evidence,
  but do not rescan growing work/response cohorts or rewrite parent acceptance.
  The regression retains an Observe Admission grant, grows from 1 to 50
  registrations, proves the earlier projection would exceed 4096 visits, then
  completes the saved report through real custody under a full parent RAM budget.

The [completion book](../../crates/focal-core/src/native/completion_book.rs) now
sums checked envelope-derived finite-slot vectors. It no longer assumes that
every report family has the same event/row count. Admission and Increment keep
their existing demand; partial reports, reconstruction and rollback retain all
seven accounting dimensions. WholeWork-specific report pricing, complete atomic
acceptance/audit/graph effects and native execution remain required under
[18 §6.9](18-lifecycle-storage-upgrade.md#69-planned-wholework-owner-projection-and-funding).

Qualification: **679 unfiltered library tests pass** (354 Core, 288 model,
37 evidence), with zero failures, ignored or filtered tests:
`bash scripts/cargo.sh test -p focal-core -p focal-model -p focal-evidence --offline --locked --lib -- --test-threads=4`.
The executed record is `/tmp/focal-work-tests.log`. New coverage includes six
WholeWork owner scenarios, six checked source/gate/continuation model scenarios,
the actual reverse-order model entry regression, the funded large-cohort Admission
regression, exact combined receive-batch boundaries and five completion-slot
accounting tests. These exercise real ContentStore custody, fixed-prefix reads,
exact retries, partial-staging refusal, pressure and pending rollback.

Workspace all-target Clippy with `-D warnings`, the separate production no-panic
gate and formatting pass on macOS arm64: `/tmp/focal-work-clippy.log` and
`/tmp/focal-work-production.log`. Architecture checks verify **657 links**,
**37 imported source hashes** and **15 frozen vocabularies** in
`/tmp/focal-work-contracts.log`; whitespace checks pass.
These affected-library tests do not establish a new full-workspace behavioral,
platform-release or geographic qualification.

Native codecs/import,
WAL/Session/quorum and CLI/MCP activation, respondent/disk/replica completion
reservations, deployment journeys and scale qualification remain open. No Arc,
unsafe code, thread or dependency package was introduced.

## Bounded acceptance projection and original response positions — 2026-09-06

The native response row now retains original claimant-receipt and WholeWork-entry
publication positions, including event ordinals. Actual Post/Receive transactions
use the checked row transition and preserve those coordinates through page copies,
pending retries, rollback and pinned reads. The native entry command is still
unimplemented; checked entry-coordinate retention is exercised through the actual
model transition and owned row. Late receipt remains an observed response fact,
without changing the sealed claim's receipt history or acceptance.

The [model projection](../../crates/focal-model/src/lifecycle/aggregation_projection.rs)
and [native adapter](../../crates/focal-core/src/native/projection.rs) now derive
acceptance from one actual effective prefix. They resolve the complete authored
response chain, immutable declarations, evaluation registrations, accepted result
positions and owner-maintained work membership, including unclosed work. The
[work iterator](../../crates/focal-core/src/native/projection_work.rs) rejects
broken links/counts, substituted slots, wrong response membership and invalid
output/diagnostic provenance. Its finite traversal budgets include both link
preflight and yielded-row verification.

Preparation validates sources and quotes the temporary construction peak before
allocation. The owner holds one Query reservation through the borrowed proof's
use and destruction. The projection borrows the canonical policy, uses bounded
flat buffers and supplies the existing private-backed artifact, response and claim
decision guards. It does not retain a second mutable aggregate, invoke validators
or publish any lifecycle transition. Tests check the exact live reservation and
full refund after callback failure and pending-tail rollback.

Chronological reduction preserves first terminal cuts, separate artifact/response
outcomes and complete acceptance witnesses. Checks on different artifacts cannot
combine into a false slot pass. Same-sequence complete coverage precedes an
uncovered blocking cause; later coverage cannot repair an earlier terminal claim.
Received-but-unentered testimony remains Pending even with present zero-check
slots. Pure Receipt Pass gates coverage, while independent WholeWork entry uses
the Required Increment gate. An explicitly sealed, completely enumerated empty
eligible-output set is Increment-ready; required output absence is assessed at
WholeWork entry. The model and existing owned aggregation now agree on that rule.
A real diagnostic-only Failed testament exercises this path without inventing a
successful output or suppressing the respondent's failure report.

Qualification: **700 unfiltered affected-library tests pass** (368 Core, 295
model, 37 evidence), with zero failed, ignored or filtered tests:
`bash scripts/cargo.sh test -p focal-model -p focal-core -p focal-evidence --offline --locked --lib -- --test-threads=4`.
The record is `/tmp/focal-projection-tests.log`. New coverage includes five
response-position tests, five actual-work iterator/index-corruption tests, four
owner query/custody/rollback tests, six projection tests including allocation
failure injection, and a private response-history authority regression. Initial
fixture failures were corrected to use typed IDs and to enter the claim once
while entering each response independently; production lifecycle guards were not
relaxed.

Workspace all-target Clippy with `-D warnings`, the separate production no-panic
gate and formatting pass on macOS arm64: `/tmp/focal-projection-clippy.log` and
`/tmp/focal-projection-production.log`. Architecture checks verify **662 links**,
**37 imported source hashes** and **15 frozen vocabularies** in
`/tmp/focal-projection-contracts.log`; whitespace checks pass. These are affected
library behavioral checks and workspace compilation/lint gates, not a new full
workspace behavioral, platform or geographic qualification.

WholeWork entry/report mutations, internal MissingSlot result publication,
complete audit/graph effects and per-attempt completion funding remain open.
Native codec/import, WAL/Session/quorum and CLI/MCP activation are also required.
The read-only projection does not establish durable native operation, released
platform support, deployment automation or scale. No Arc, unsafe code, thread or
dependency package was added by this increment.

## Explicit WholeWork entry and indexed dependency consequences — 2026-09-06

The native owner now accepts `EnterWholeWork` from the actual claimant for an
exact Received testament. Receiving the testament still does not enter validation.
Entry consumes the complete checked Increment gate, records the claim's first
evaluation request when needed, and advances that response and its attached work
independently. It invokes no participant tool. Present zero-check slots can
complete under their declared presence policy; absent Required slots produce the
original Incomplete consequence. Later response entry does not repeat the claim's
initial request or rewrite earlier response history.

The [entry transaction](../../crates/focal-core/src/native/whole_work.rs) prepares
all rows before common range publication. Its explicit bounded journal retains
every intermediate claim, response and work transition even when the final row
passes through several states in the same sequence. Original response entry and
internal result ordinals identify their actual journal facts. Claim history
verification checks every exact one-revision step through the final stored row.
Pending-tail discard and exact retries preserve the existing publication contract.

The [structural settlement helper](../../crates/focal-core/src/native/missing_results.rs)
resolves each existing MissingSlot cohort member and its immutable declaration.
Required absence records an artifact-free Incomplete result; Observe absence
records suppression without an attempt or verdict. Handler deadlines and handler
policy grants do not block this structural operation because it runs no handler.
Existing explicit fences and sealed evaluations remain unchanged. Generic external
Begin/report guards retain their separate authority, deadline and policy checks.
Missing results have independent owned rows and committed, prepared and leased
read paths; they cannot impersonate evaluator-produced artifacts.

Native creation now maintains a
[reverse immutable graph index](../../crates/focal-core/src/native/incoming_graph.rs).
DependsOn/Awaits edges to the same target share one dependent membership. The
bounded iterator checks actual immutable declarations and complete finite linked
chains before exposing any member. Creation prices the additional index rows,
including multiple dependent additions sharing one target in a single batch.

The [entry consequence pass](../../crates/focal-core/src/native/graph_effects.rs)
discovers actual incoming/outgoing dependencies and owned/lineage membership,
then captures one snapshot containing the changed root and original peers. It
stages checked transitive dependency failures and least-fixed-point satisfaction
with their original provenance. Existing terminal cuts remain unchanged. Graph
capture, peer buffers, copies and dependency-failure witness workspace are charged
before allocation. Active runtime scopes remain explicitly refused until their
reverse membership and consequence transactions are implemented.

Qualification: **732 unfiltered affected-library tests pass** (396 Core, 299
model, 37 evidence), with zero failures, ignored or filtered tests:
`bash scripts/cargo.sh test -p focal-core -p focal-model -p focal-evidence --offline --locked --lib -- --test-threads=4`.
The record is `/tmp/focal-entry-tests.log`. New coverage comprises seven actual
owner entry/custody/retry/rollback scenarios, six reverse-index tests, four graph
consequence tests, eight internal-result ownership tests, three MissingSlot owner
helper tests and four structural-settlement model tests. Initial fixture failures
were corrected for the actual command field names, facade borrow lifetime,
index insertion's legitimate neighbor copy and the increased creation allowance.

Workspace all-target Clippy with `-D warnings`, the separate production no-panic
gate and formatting pass on macOS arm64: `/tmp/focal-entry-clippy.log` and
`/tmp/focal-entry-production.log`. Architecture checks verify **666 links**,
**37 imported source hashes** and **15 frozen vocabularies** in
`/tmp/focal-entry-contracts.log`; whitespace checks pass. These are affected-library
behavioral checks and workspace lint/compilation checks, not a new full-workspace
behavioral, platform or geographic qualification.

Entry uses ordinary capacity admission. It does not acquire a future external
WholeWork report grant or establish respondent, disk or replica completion
reservations. Funded external WholeWork Begin/report, complete audit closure,
runtime scope consequences and propagation from the other native control and
succession mutations remain required. Native codecs/import, WAL/Session/quorum
and CLI/MCP activation are still open. The unhooked WholeWork authority helper
and its tests are preparation for that next funded execution path; they are not
included in the passing test count. This increment adds no Arc, unsafe code,
thread or dependency package.
