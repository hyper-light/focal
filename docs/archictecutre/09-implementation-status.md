# Implementation evidence and remaining work

Updated 2026-09-08. The objective is the complete P00–P20 plan: the original P00–P16 scope plus the user's manual CLI, skills, MCP, challenge and consultation extension in [13](13-cli-and-agent-implementation-plan.md). This record distinguishes executable components from integration and deployment qualification. No package is marked complete merely because its crate compiles. Imported Hecate references remain unchanged. The current execution boundary is strictly peer-to-peer: Focal records claims, testament/artifact evidence and authenticated verdicts; participants invoke their own validating tools/skills or request evaluation from another peer through ordinary claims. Focal is not an agent/worker launcher or model-job scheduler. Existing optional execution helpers do not make participant execution a daemon responsibility.

A source audit identifies a real four-family lifecycle gap in the active V1
storage profile. Claim status/history and durable validation attempts exist, but testament lifecycle is
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
zero-check outcomes plus indexed dependency effects. Checked response-entry
authority now covers every exact attached artifact and structural missing slot
without impersonating a claimant; the atomic journal checks cross-object order
and original publication positions. The subsequent
[funded WholeWork increment](#native-wholework-report-funding-and-graph-protection--2026-09-06)
adds actual designated-evaluator Begin/report transactions and protects their
future projection and graph capacity. Trusted exact evaluation deadlines now
record authority fences and separate typed timer outcomes through the same
funded owner. Claim deadlines now apply canonical SCC precedence and bounded
atomic expiry/fencing. The [automatic cohort sealing increment](#automatic-native-cohort-sealing--2026-09-07)
records complete native validation cohorts with each new local outcome and
rebinds existing report grants in the same candidate. Earlier sections describing
that writer as disabled record its preceding prerequisites. The subsequent
[native audit inspection increment](#bounded-native-audit-inspection--2026-09-07)
derives complete sealed membership and original result history from one actual
owner prefix. The subsequent
[claimant result-testament increment](#native-claimant-result-testaments--2026-09-07)
adds generation and posting of that complete immutable bundle in native RAM.
The subsequent [receipt adoption increment](#native-receipt-adoption-and-retained-open-cycles--2026-09-07)
transfers the actual receipt entitlement, fences its complete evaluation cohort,
and retains abandoned open cycles and their original evidence. The subsequent
[owner-scope release increment](#native-owner-scope-release--2026-09-07)
adds explicit release of terminal owned claims after their children have
released; executed qualification is recorded below.
The subsequent bounded monitor model increment adds preflighted registration,
rebinding and disposition, plus exact monitor deadline assessment and a private
negative-SCC expiry capability. The subsequent native monitor increment adds
atomic register/rebind/disposition history, direct-root reverse subscriptions,
typed monitor timers and bounded repeated consequences, with held WholeWork
report reservations. Guaranteed whole-claim completion funding, complete
Admission/control consequences, durable monitor reconciliation and durable
activation remain on the critical path before the native lifecycle reaches users.

The [raw admission and recovery increment](#raw-native-admission-and-recovery-construction-primitives--2026-09-07)
connects complete native frames to the RAM owner and adds caller-owned wire reads,
precharged range hydration and checked scalar validation restoration. The latest
[write-set and lifecycle restoration increment](#native-write-sets-and-lifecycle-restoration--2026-09-07)
retains actual candidate writes and restores the remaining lifecycle histories.
The subsequent [record encoding and phased construction increment](#native-record-encoding-and-phased-recovery-construction--2026-09-07)
encodes those retained facts and adds a dependency-aware detached loader. Native
row decoding and detached checkpoint restoration are now connected in the
[checkpoint restoration increment](#native-checkpoint-restoration--2026-09-07),
including history/aggregate validation and the consolidated qualification below.
The subsequent [incremental replay batch](#native-incremental-replay-and-restored-owner-qualification--2026-09-08)
connects native mutation construction and adds restored-owner pressure coverage.
Its qualification status is recorded separately below. Durable service
integration, completion-buffer funding and live server/CLI/MCP activation remain
unfinished.

The remaining delivery milestones are substantial and are not equally sized:

1. **Finish native lifecycle integration.** Claimant and designated-evaluator entry,
   funded WholeWork reports and their atomic artifact/response/acceptance effects
   are implemented in the native RAM owner, including exact evaluation-deadline
   publication, claim expiry/deadlock control, automatic cohort sealing and
   bounded audit inspection and claimant result-testament generation/posting.
   Receipt adoption, complete retained open-cycle discovery and explicit
   terminal owner-scope release are implemented and qualified below. Native
   monitor transactions, timer resolution and indexed consequences now extend
   work reports, cancellation and supersession. Guaranteed claim-completion
   funding, Admission failure propagation, complete runtime-scope qualification
   and durable reconciliation remain required.
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

Work currently spans the unfinished lifecycle guarantees in milestone 1 and
native persistence integration in milestone 2. The next user-visible acceptance gate is a
two-participant CLI/MCP workflow on the corrected independent lifecycles: explicit
successful or failed respondent testimony, error artifacts, designated checks,
derived acceptance, and identical evidence/history after a service restart.
That gate requires durable service replay and interface integration around the
qualified Core checkpoint/replay components; component tests cannot satisfy it.
Existing service, interface, networking and replication components provide
foundations for later milestones; their presence is not evidence that the
complete deployment goal is finished. No completion percentage or delivery date
is established by this record.

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


## Checked response-entry authority and journal integrity — 2026-09-06

The native explicit entry path now derives all artifact and structural missing
consequences from one checked response-entry capability. The model supports the
same derivation after an actual designated evaluator Begin. That evaluator-first
native command still awaits its complete held-resource contract; this increment
does not expose an unfunded Begin/report path.

Implemented:

- [ResponseEntry](../../crates/focal-model/src/lifecycle/evidence_entry.rs) is a
  private-field, ephemeral capability created from an actual Received → Validating
  plan, the checked current claim and a refreshed acceptance decision. It pins the
  exact claim/response revisions, report stamp, receipt, cycle and respondent.
  `WorkArtifact::begin_entered` and `Evaluation::settle_missing_entered` consume
  that capability without creating a claimant Principal. Existing explicit
  claimant APIs retain their actor checks.
- [Native entry](../../crates/focal-core/src/native/whole_work.rs) now uses that
  capability for every exact attached manifest artifact and the complete
  structural missing cohort. Missing Required targets retain artifact-free
  Incomplete results; Observe omissions record suppression. Explicit evaluation
  fences and seals remain unchanged. Failed/unattached work is not converted to
  an attached success artifact, and respondent-authored testimony is preserved.
- [Object journal validation](../../crates/focal-core/src/native/object_journal.rs)
  checks complete work/response revision chains, immutable testimony and target
  fields, one-to-one missing-evaluation/result correspondence, and exact retained
  event coordinates. A precharged sorted index matches journal facts to final
  rows without a ledger scan or per-object allocation. The entry phases enforce
  claim entry, response entry, work entry, structural assessment, work outcomes,
  response outcome, claim acceptance and graph effects in causal order. Observe
  missing evaluations must follow the actual response entry even though they
  create no accepted-result row. Omitted/duplicate rows or facts, stale revisions,
  shifted result coordinates and reordered cross-object events are refused.
- [Typed completion use](../../crates/focal-core/src/native/completion_envelope.rs)
  replaces a generic changed-claim boolean in range-envelope selection and book
  spending. Only an actual ReportAdmission Posted → PostFailed transition with
  the exact next binding and Required Admission cut consumes AdmissionFailure
  credit. Unchanged parents and other operations select Regular. This preserves
  Admission/Increment reservations while preventing future WholeWork claim
  changes from being mistaken for an Admission surcharge.

Executed on the final source for this increment:

- **748 unfiltered library tests passed:** `focal-core` 405, `focal-model` 306,
  `focal-evidence` 37, with offline locked Cargo and four test threads. Seven new
  model capability tests cover designated-evaluator entry, all-slot effects,
  Required/Observe parity, exact report/receipt/generation, stale and controlled
  sources, fences/seals and the Increment gate. Seven new native journal tests
  exercise actual prepared candidates and corrupt their copies; two additional
  completion tests distinguish real Admission failure from unrelated changes.
- Workspace all-target Clippy with `-D warnings`, the production no-panic gate,
  `cargo fmt --all -- --check` and `git diff --check` all passed. Contract checks
  verified **671 architecture links, 37 imported hashes and 15 frozen vocabularies**.
  These checks do not constitute a new full-workspace behavioral, network,
  fault-injection or cross-platform qualification run.

The funding review also identified the concrete remaining activation requirements:
future rather than present projection occupancy; separate byte, model-visit and
native-cursor budgets; every-attempt work/response/claim/graph effect costs; first
entry costs; and protected revision capacity. In particular, a newly created
incoming dependency can enlarge a funded claim's future graph work without
changing its claim row, so the current parent-event checks alone are insufficient.
[Document 18](18-lifecycle-storage-upgrade.md#wholework-funding-implementation-checklist)
now provides checked-arithmetic shape/visit equations, per-report row/event
formulas, growth guards and the qualification sequence to implement next. These
are planning bounds awaiting executable qualification, not active Work grants.

No new durable codec/import, WAL/Session/quorum activation, CLI/MCP native dispatch,
full runtime-scope propagation, platform release or distributed-scale qualification
is claimed here. The existing unhooked `work_authority` source is still outside
these executed tests. Full audit closure and non-entry control/succession graph
consequences remain required. Frozen V1 wire/hash/replay behavior is preserved.

## Future projection bounds and graph preflight — 2026-09-06

The next WholeWork funding prerequisites are implemented and exercised against
the actual projection and graph algorithms. They establish checked estimates and
bounded execution; they do not yet install future Work report grants.

- The model's [ProjectionQuote](../../crates/focal-model/src/lifecycle/aggregation_projection_quote.rs)
  estimates all eleven simultaneous projection buffers plus separate inspection
  and reduction visit ceilings from immutable policy and promised future response,
  work and evaluation counts. Checked arithmetic rejects overflow and excess
  dimensions before scanning policy slots. Counts cover unclosed and failed work,
  missing targets and every registered check, including suppressed or terminal
  members. The owner must preserve the promised shape before admitting later growth.
- [NativeProjectionQuote](../../crates/focal-core/src/native/projection_quote.rs)
  adds direct lookups, nested work-cursor traversal, source provenance checks and
  each provisional overlay comparison. A single
  [owner-local visit allowance](../../crates/focal-core/src/native/projection_visits.rs)
  spans inspection and reduction; opening or interleaving cursors cannot replenish
  it. Exhaustion returns Capacity before an absent lookup can be interpreted as
  missing participant evidence or an acceptance callback can run. The adapter
  uses scalar Cell values and borrowed lifetimes without shared heap ownership.
- [Graph preflight](../../crates/focal-core/src/native/graph_effects.rs) captures
  the same complete indexed dependency/ownership closure consumed by execution.
  It quotes discovery, snapshot, witness, copied claim/registry, output row, event
  and heap costs; checks one graph revision per nonterminal member; and verifies
  actual output costs. An optional prior ceiling compares the entire proposed
  closure, including new incoming links whose target Claim row did not change.
  Source identities stay borrowed, and each allocation is charged before creation.

Qualification: **767 unfiltered affected-library tests passed**: Core 416, model
314 and evidence 37, with zero failures, ignored or filtered tests. Executed:
`bash scripts/cargo.sh test -p focal-core -p focal-model -p focal-evidence --offline --locked --lib -- --test-threads=4`.
The record is `/tmp/focal-work-bounds-tests.log`. Eight new model tests compare
quotes with measured real reducer visits across future growth, missing and failed
work, retryable Error, terminal outcomes and alternate chronological coverage;
they also exercise allocation refusal and arithmetic boundaries. Six new native
projection tests cover shared/exhausted/interleaved cursor budgets, overlay costs,
callback refusal, future open/closed response growth and exact quote boundaries.
Five graph tests cover actual output fit, pending incoming-link and registry
growth, prefix/limit changes, exact stage-byte boundaries and active-scope refusal.
One preexisting multi-response fixture needed a larger explicit lookup allowance
because all nested traversal is now counted together; runtime guards remain intact.

Workspace all-target Clippy with `-D warnings`, the separate production no-panic
gate, formatting and whitespace checks passed on macOS arm64. Logs are
`/tmp/focal-work-bounds-clippy.log` and `/tmp/focal-work-bounds-production.log`.
Contract checks verified **675 architecture links, 37 imported hashes and 15 frozen
vocabularies** in `/tmp/focal-work-bounds-contracts.log`. These are affected-library
behavioral checks and workspace lint/compilation checks, not a new full-workspace
behavioral, network, fault-injection or cross-platform qualification run.

The remaining activation path is unchanged: install typed per-attempt Work grants,
fund all report and first-entry effects, preserve work/response/claim/peer revision
headroom, and enforce graph/response/registration growth checks before every
relevant publication. Native Begin/report, complete audit and runtime-scope/control
consequences, codecs/import, WAL/Session/quorum and CLI/MCP activation remain open.
The estimates exclude range-tree descent and uncounted model helper/sort work;
graph preflight excludes non-graph rows, range construction and the common
object-journal index. These costs still belong in the full report envelope.
No new Arc, unsafe code, thread or dependency package was introduced. Frozen V1
wire/hash/replay behavior is preserved. No platform release or distributed-scale
qualification is established by this increment.

## Native WholeWork report funding and graph protection — 2026-09-06

The native RAM owner now accepts funded `BeginWork` and `ReportWork` transactions
against an exact attached artifact, response, receipt and declaration generation.
This completes the external WholeWork report path inside `NativeOwner`. Native
durable codecs, Session/quorum dispatch and the CLI/MCP lifecycle migration remain
separate activation requirements.

- **Actual participant authority.** A designated evaluator may begin against a
  Received response. The first actual Begun evaluation event authorizes entry of
  that exact response and the consequences for its complete manifest, including
  structural missing slots and zero-check artifacts. No claimant is impersonated.
  Subsequent checks against an entered response record their own Begin without
  repeating entry. Low-level unfunded Work preparation refuses the bypass.
- **One report and its real consequences.** The shared
  [report staging path](../../crates/focal-core/src/native/reporting.rs) verifies
  actor, exact attempt, provenance, schema and custody before accepting the real
  proof or error artifact. Artifact, evaluation and accepted-result history keep
  their original positions. A single bounded projection then applies the target
  artifact, response, claim acceptance and actual indexed graph effects in one
  candidate. A report after ordinary claim completion can finish its already-begun
  evaluation without changing earlier terminal cuts. Retryable Error remains an
  inspectable result with real diagnostic evidence.
- **Future capacity before responsibility.** The
  [Work envelope](../../crates/focal-core/src/native/completion_work.rs) covers the
  complete remaining retry/fallback/quality chain, all authored response cycles,
  work and registration growth, immutable response heap, graph member copies,
  revision margins, verification, object journal and range construction. Each
  attempt reserves possible artifact/response/claim/graph writes; it cannot spend
  Admission-only failure credit. Retained versions remain funded when old reads
  pin earlier pages. Begin's first entry is separately admitted through Ordinary
  capacity; later reports spend their held Completion allowance.
- **Indexed protection of every affected graph member.** The
  [reverse membership index](../../crates/focal-core/src/native/completion_protection.rs)
  records the complete actual dependency/ownership closure with each Work grant.
  [Candidate checks](../../crates/focal-core/src/native/completion_growth.rs) find
  affected grants from owning-claim facts and new claims' actual dependency and
  lineage endpoints. A new incoming edge is therefore checked even when its target
  Claim row does not change. Each affected grant is checked once using held serial
  scratch; unrelated grants and ledger rows are not scanned. The complete affected
  cohort's temporary marks are reset before each check, including after refusal.
  Changed component membership is refused while that original promise is live;
  automatic additional funding for topology changes remains future work.
- **Rollback and independent retention.** Grant installation, reverse memberships,
  index growth and pool funding share the pending candidate journal. Refusal and
  suffix discard restore original credit and memberships. A pending terminal
  report retains its membership records until publication so rollback requires
  no replacement allocation. Committed retirement releases logical promises;
  retained pages and evidence keep their own actual memory charges.
- **Operation-specific causal checking.** The
  [object journal](../../crates/focal-core/src/native/object_journal.rs) now checks
  claimant entry, first/subsequent evaluator Begin and report profiles separately.
  It verifies actual Begun authority before derived entry, exact report evidence
  and original ordinals, object revision histories and staged consequences. This
  does not replace the acceptance/graph projection with a second reducer.

The implementation introduces no Arc, unsafe code, thread or dependency package.
Explicit native command-intent tags 18 and 19 append the Work operations without
changing the earlier tags. Frozen V1 wire, content hashes and replay are unchanged.
No durable restart, release platform or distributed-scale qualification is implied
by this native RAM increment.

Qualification: **802 unfiltered affected-library tests passed**: Core 451, model
314 and evidence 37, with zero failures, ignored or filtered tests. Executed:
`bash scripts/cargo.sh test -p focal-core -p focal-model -p focal-evidence --offline --locked --lib -- --test-threads=4`.
The record is `/tmp/focal-work-funded-tests.log`. The 35 added Core tests cover:

- Actual designated-evaluator first entry, subsequent Begin, Required failure,
  late Observe report, actor refusal and atomic entry rollback.
- Envelope refusal before responsibility, exact slot counts, full authored
  response/registration growth, future graph heap copies and terminal peers.
- Retryable Error and final Pass under exhausted ancestor RAM with pinned old
  versions; a distinct agentic quality evaluator after programmatic retry and
  Pass; wrong-evaluator refusal before custody; retained proof provenance for
  every phase; no premature work/response/claim success.
- Reverse membership identity and interval isolation, partial installation
  failure and exact refund, journal rollback after earlier grant retirement,
  incoming dependency and owned-child growth refusal, unrelated creation,
  and successful completion after those refused mutations.
- Operation-specific journal entry/report order, missing or substituted
  evidence, omitted consequences, original result coordinates and late reports
  without terminal-object rewrites.
- Owner reconstruction from actual retained native rows after a custody-backed
  retryable Error, followed by a funded final Pass under exhausted parent RAM.
  This uses the existing native reconstruction harness, not disk replay or a
  checkpoint decoder. Original Error coordinates survive reconstruction and
  final owner teardown releases its memory charges.

Workspace all-target Clippy with `-D warnings`, the separate production no-panic
gate, formatting and whitespace checks passed on macOS arm64. Logs are
`/tmp/focal-work-funded-clippy.log`, `/tmp/focal-work-funded-production.log` and
`/tmp/focal-work-funded-fmt.log`. Contract checks verified **680 architecture links,
37 imported hashes and 15 frozen vocabularies** in
`/tmp/focal-work-funded-contracts.log`.

Remaining native work includes trusted deadline transactions, complete audit
closure, adoption and non-entry control/succession/runtime-scope graph effects.
Respondent responsibility and closing capacity, durable reservations and recovery,
native codecs/import, WAL/Session/quorum and CLI/MCP activation remain required.
The held membership policy can refuse topology growth; it does not yet acquire
additional graph funding automatically. These behavioral checks exercise the
affected libraries; they are not full-workspace network, fault-injection,
cross-platform release or global deployment qualification.

## Trusted native evaluation deadlines — 2026-09-06

`NativeOwner::prepare_evaluation_deadline` now publishes a due evaluation's
authority fence or records consumption of an already-terminal/fenced timer.
It accepts a separate typed timer input and trusted owner time. It does not
accept a participant command or impersonate a claimant, evaluator or respondent.

- The model's [deadline transition](../../crates/focal-model/src/lifecycle/validation_deadline.rs)
  checks the exact binding, authored declaration, target/generation, claim source,
  complete timer identity, due time and publication cut. Expired Ready checks,
  handler-free Receipt/MissingSlot targets and declared-policy checks do not need
  external report authorization. Receipt adoption, parent terminality and
  cohort sealing do not rewrite historical timer authority. An actual fence
  preserves state, attempts and original accepted evidence; no verdict, artifact,
  response or claim outcome is manufactured.
- The [native deadline path](../../crates/focal-core/src/native/deadlines.rs)
  resolves actual owner registration once and caches the checked replacement
  against the borrowed immutable effective prefix. `NativeInvocation` separates
  actor requests from evaluation deadlines throughout outcome keys and event
  history. Timer identity includes the full evaluation key and timer generation;
  the separate intent domain commits authored due time. Exact pending/committed
  retries precede clock, queue and memory checks. Changing the authored due time
  under an already-consumed key is a conflict. A no-op consumes only its typed
  outcome and Meta update, preserving the previous event/result coordinates.
- Begun evaluations use a dedicated checked deadline loan from their actual
  report grant. Begin and owner reconstruction now prove the four-put,
  one-event, two-new-row fence fits a Regular report's reserved storage, workspace
  and discrete slots. The actual write plan must fit that envelope. Ready and
  terminal/no-op deadlines use bounded Ordinary admission. Single-grant fence
  retirement uses an inline journal update with no new Ordinary allocation.
  Rollback restores exact credit and Work reverse protections; committed
  retirement leaves existing pinned pages charged to their actual owner.
- Actor mutation fingerprints and frozen V1 wire/hash/replay remain unchanged.
  The new invocation fields belong to the unactivated native API. Trusted timer
  preparation stays separate from the actor-only `NativeInput` surface. No new
  Arc, unsafe code, thread or dependency package was introduced.

Qualification: **814 unfiltered affected-library tests passed**: Core 458,
model 319 and evidence 37, with no failures, ignored or filtered tests. Executed:
`bash scripts/cargo.sh test -p focal-core -p focal-model -p focal-evidence --offline --locked --lib -- --test-threads=4`.
The record is `/tmp/focal-deadline-full-tests.log`. Five model tests cover all
five target families, Required-policy Ready checks, invalid source/timer/cut and
revision capacity, retained retry/quality evidence, actual receipt adoption and
unchanged terminal/fenced history. Seven
[owner integration tests](../../crates/focal-core/src/native/deadline_owner_tests.rs)
exercise Ready expiry, retry/quality expiry under full ancestor RAM with pins,
pending report/deadline order, suffix discard, immutable completed evidence,
full-queue/full-RAM exact retries, and actual Work graph-protection retirement
and restoration. Existing actor-only identity assertions now inspect their
explicit Request invocation; artifact custody still uses its real RequestKey.

Workspace all-target Clippy with `-D warnings`, the separate production no-panic
gate, formatting and whitespace checks passed on macOS arm64. Logs are
`/tmp/focal-deadline-clippy.log`, `/tmp/focal-deadline-production.log` and
`/tmp/focal-deadline-fmt.log`. Contract checks verified **683 architecture links,
37 imported hashes and 15 frozen vocabularies** in
`/tmp/focal-deadline-contracts.log`.

These are native RAM transactions. Due-timer scheduling and durable timer
recovery, claim expiry with canonical deadlock precedence, cohort seal rebinds,
audit bundles, adoption and remaining scope/graph effects are still required.
Native codecs/import, WAL/Session/quorum and CLI/MCP activation remain open.
No disk-restart, network, release-platform or global-scale qualification is
established by this increment.

## Native claim deadlines and canonical deadlock resolution — 2026-09-07

`NativeOwner::prepare_claim_deadline` now accepts a trusted, separately typed
claim deadline. It resolves the authored timer and current claim from the
effective owner prefix, including pending predecessors. Its invocation key is
independent of actor and evaluation-deadline identities. Exact pending and
committed retries retain their original accepted time, outcome and history;
changed intent under the same key refuses. A terminal trigger consumes the
timer without rewriting its prior claim cut or evidence.

- The [model SCC query](../../crates/focal-model/src/lifecycle/graph_deadline.rs)
  returns an explicit checked absence of a qualifying unsatisfied cycle. Wrong
  timers, early time, bad cuts, incomplete sources, traversal exhaustion and
  allocation refusal remain errors. All reachability, queue and component
  buffers are charged before construction. Canonical victim selection uses
  original creation position and then claim ID; original fingerprint ordering
  is preserved. A settled self-edge cannot create a false wait cycle.
- The [native transaction](../../crates/focal-core/src/native/claim_deadlines.rs)
  resolves canonical SCC victims and their dependency/release consequences
  against complete frozen snapshots. It then reassesses the original due claim.
  A victim distinct from that trigger must not consume its only timer and leave
  it open: each round terminalizes a previously live member, until the trigger
  is terminal or a checked absence of a remaining cycle permits ordinary expiry.
  The [D-08 clarification](02-domain-and-lifecycle.md#73-deadlock-and-cancellation)
  records that rule for successor semantics. Every intermediate witness retains
  its actual source bindings, deadline provenance and publication cut. All
  rounds share finite traversal and cumulative construction bounds.
- Ordinary expiry resolves the entire retained registration set and uses the
  model's [actual claim-expiry fence](../../crates/focal-model/src/lifecycle/validation_claim_deadline.rs).
  Nonterminal unfenced checks acquire only an authority fence; Ready/retry/quality
  state and original evidence remain intact. Terminal results and earlier
  fences remain unchanged. Deadlocked and DependencyFailed preserve eligible
  already-begun reports. No timeout generates respondent testimony, work bytes,
  diagnostics or an evaluation verdict.
- The dedicated deadline journal validates initiating cuts, complete expiry
  fence membership and ordering, final rows and permitted graph events. Control
  construction and the additional coexisting multi-grant retirement journal
  use Completion-lane admission. Commit/discard preserve exact report credit,
  pinned history and reverse-protection lifetimes. A refusal after an earlier
  prepared round leaves no claim change, consumed timer or partial retirement.
- The new API is native RAM state only. Actor mutation fingerprints and frozen
  V1 wire/hash/replay are unchanged. The increment adds no Arc, unsafe code,
  thread or dependency package.

Qualification: **839 unfiltered affected-library tests passed**: Core 471,
model 331 and evidence 37, with no failures, ignored or filtered tests. Executed:
`bash scripts/cargo.sh test -p focal-core -p focal-model -p focal-evidence --offline --locked --lib -- --test-threads=4`.
The record is `/tmp/focal-claim-deadline-full-tests.log`.

Ten [owner deadline tests](../../crates/focal-core/src/native/claim_deadline_owner_tests.rs)
cover actual Ready/begun/terminal cohorts, preserved response and result evidence,
canonical victims distinct from the trigger, overlapping cycles requiring
several victims, late reports after business failure, timer conflicts and exact
retries, both pending report/expiry orders, and suffix discard. Multi-grant expiry
succeeds with Ordinary capacity full and old states pinned. A construction
allowance one byte below the complete quote refuses after private deadlock
progress, leaving published rows, clock, timer outcome, credits and memory
unchanged. Three [journal tests](../../crates/focal-core/src/native/claim_deadline_journal_tests.rs)
reject omitted registry members even when both row and event are removed,
reordered/duplicate/false fence events, extra rows and altered timer/cut identity.

The twelve added model tests cover actual claim-expiry authority and preserved
retry/quality evidence, exact canonical SCC identity and source errors, all SCC
allocation boundaries, and [shared traversal accounting](../../crates/focal-model/src/lifecycle/graph_visit_tests.rs).
Capture, build and successive SCC/dependency queries spend one allowance;
partial failures retain already-spent visits. A refused bulk debit preserves
unused visits without saturating or renewing the budget. Compatibility wrappers
retain their original witness fingerprints.

Workspace all-target Clippy with `-D warnings`, the separate production no-panic
gate, formatting and whitespace checks passed on macOS arm64. Logs are
`/tmp/focal-claim-deadline-clippy.log`, `/tmp/focal-claim-deadline-production.log`
and `/tmp/focal-claim-deadline-fmt.log`. Contract checks verified **692 architecture
links, 37 imported hashes and 15 frozen vocabularies** in
`/tmp/focal-claim-deadline-contracts.log`.

This control path does not reserve guaranteed future completion for every
admitted claim. Registered evaluation and graph cohorts can grow beyond one
deadline transaction; oversized work refuses atomically. Before guaranteeing
timer liveness, admission and future growth must fund the complete claim
obligation or an explicit continuation, including control journals, discrete
rows/events, RAM coexistence, durable storage and replicas. Active runtime-scope
closures also await their owner integration. Due-timer scheduling/recovery,
cohort seal rebinds and audit bundles, adoption and other scope/graph effects,
native codecs/import, WAL/Session/quorum and CLI/MCP activation remain required.
No disk-restart, network, release-platform or global-scale qualification is
established by this increment.


## Checked evaluation seals and mixed completion journals — 2026-09-07

The [checked model seal](../../crates/focal-model/src/lifecycle/validation_seal.rs)
now derives cohort recording from the actual claim and its original local seal
position. It verifies the immutable definition, claim identity and expected
revision, preserves begun retry/quality attempts, proof, diagnostics and existing
fences, and records suppression for an unbegun Ready member. Terminal and
already-sealed members remain unchanged. Its cause is stable across later claim
revisions and graph release; an empty cause is rejected. This process-local
identity does not alter frozen V1 hashes or define a successor wire format.

`SealTransition` retains exact private before/after states and checks the complete
allowed delta. A binding alone cannot substitute another attempt or evidence
chain. Future preparation must precharge the full token buffer; the capability
performs no allocation. A seal records cohort membership and does not consume a
report, revoke an otherwise eligible begun attempt or generate a verdict.

The [completion journal](../../crates/focal-core/src/native/completion_updates.rs)
now collects actual candidate report, seal and fence events against the same
immutable effective source used for preparation. Existing owner reports and
controls use this checked path. It validates every event group before changing
credit, checks exact range-root ancestry and actual registry membership even for
members without live grants, binds actual Accepted coordinates after their
Reported event inside the declared publication, and folds multiple transitions of one
evaluation into one credit update. A report followed by a seal spends exactly
one report and binds to the final sealed revision. A seal alone preserves credit,
schemas, workspace and reverse protections. An older pending retirement keeps
its original zero-credit binding when a later audit-only seal is recorded.

Single updates stay inline. Multiple updates reserve their complete buffer from
the actual triggering allocation source and lane. Commit removes only retired
grants; a live sealed grant remains even if a later pending report has already
advanced it. Tail-first rollback restores exact bindings, counts and index
weights. Begin and reconstruction also retain one extra revision for an unsealed
member. Existing report envelopes preflight the complete immutable-policy and
event traversal bound at Begin and reconstruction. Reconstruction now also
rechecks the existing Admission projection bound for a still-Posted parent;
lower owner limits cannot accept a recovered grant whose later report will not
fit. Sorting, registry lookup,
state-chain and credit passes share a finite visit allowance; an exhausted
allowance refuses before grant mutation. Source inspection copies only borrowed
references, with no root clone, per-evaluation Arc or new dependency.

Report and begun-deadline journal funding now resolves the same held owner pool
by borrowing it internally after actual loan authorization. External controls
retain their original borrowed source and lane. This removes the temporary
shared-budget handle clones from each report and deadline path; it does not
substitute a grant or change the reserved funding contract.

Qualification: **943 unfiltered affected-library tests passed**: Core 478,
model 338, evidence 37 and memory 90. Executed:
`bash scripts/cargo.sh test -p focal-core -p focal-model -p focal-evidence -p focal-memory --lib --offline --locked -- --test-threads=4`.
The record is `/tmp/focal-seal-full-tests.log`.

Seven new model tests exercise checked source identity, immutable policy and
state deltas, Ready suppression, retry/quality preservation and stable seal
causes. Five [journal tests](../../crates/focal-core/src/native/completion_book_rebind_tests.rs)
cover mixed retirement/live rebinds with younger pending reports, one debit for
Report followed by Seal, missing/duplicate tokens, absent source membership,
insufficient actual funding, and shared traversal refusal without changed
credits or memory. Two owner tests cover Begin/reconstruction refusal below
complete report-work bounds, intact Core return, and real Required failure
after sufficient admission, including exhausted parent RAM. Three
[range tests](../../crates/focal-memory/src/range_successor_tests.rs) reject
sibling branches and unrelated owners with matching IDs/prefixes and check
valid sequential candidates without allocation under full memory pressure.

Workspace all-target Clippy with `-D warnings`, the separate production no-panic
gate, formatting and whitespace checks passed on macOS arm64. Logs are
`/tmp/focal-seal-clippy.log`, `/tmp/focal-seal-production.log` and
`/tmp/focal-seal-format.log`. Contract checks verified **697 architecture links,
37 imported hashes and 15 frozen vocabularies** in `/tmp/focal-seal-contracts.log`.
These checks do not qualify automatic owner cohort publication.

Automatic cohort sealing is still disabled. Existing production calls pass an
empty seal-capability list; no participant command can force an unpriced cohort
write. Enabling the writer requires complete affected registration overrides,
seal/history profiles and future report/control envelopes, including token and
mixed-journal coexistence. Evaluator-first BeginWork additionally needs one
commit/rollback unit combining grant installation and all resulting seal updates.
These concrete steps are recorded in [18 §6.10](18-lifecycle-storage-upgrade.md#610-native-deadlines-and-remaining-audit-integration).

This increment does not create retained native AuditCohort rows or claimant result
bundles. Their complete original Accepted/Delivery/Missing history, late audit
updates, native codecs/import, WAL/Session/quorum and CLI/MCP activation remain
required. Qualification here cannot establish disk recovery, release-platform
support or distributed-scale behavior.

## Multiple claim registry overrides — 2026-09-07

Native transaction plans now carry [owned registry overrides](../../crates/focal-core/src/native/registry_overrides.rs)
for several affected claims. Empty and single-claim plans retain their inline
representation; multiple claims use a bounded, precharged buffer. Canonical
appends avoid searching earlier claims and geometric growth gives linear total
movement over that append sequence. An out-of-order insert uses binary lookup
and at most one bounded shift. Allocation and actual-capacity checks precede
every ownership transfer; refusal preserves the earlier collection and the
caller’s scratch accounting.

The common final-row builder checks sorted, unique claim identities and complete
override ownership before constructing storage changes. It merges final claims
with the sorted overrides, consumes each replacement once, and moves each
already-owned registration buffer into its final claim row. Existing receipt,
received-testament and Increment-target writers use this path. Other writers
retain empty overrides and copy their actual source registry as before.

This removes the single-claim structural restriction needed by the future
automatic cohort writer. It does not emit cohort seals. Future affected-graph
envelopes still need the full override-buffer growth charge and both policy
checks for each replacement, alongside evaluation/seal history, token buffers,
mixed journals and durable storage. No extra participant command, user-facing
configuration, dependency or shared ownership wrapper was added.

Qualification of the integrated multi-claim path and borrowed journal funding:
**950 unfiltered affected-library tests passed**: Core 485, model 338, evidence
37 and memory 90. Executed the same four-library test command as the preceding
checkpoint; the record is `/tmp/focal-registry-full-tests.log`.

The subsequently added two-claim publication test passed with all eight
[registry tests](../../crates/focal-core/src/native/registry_overrides_tests.rs)
using `bash scripts/cargo.sh test -p focal-core --lib native::transactions::registry_overrides --offline --locked`.
That focused run has 8 passed and 478 filtered tests, recorded in
`/tmp/focal-registry-integration-tests.log`; it adds one distinct test to the
950-test run. It combines two actual authorized Increment-target seal plans,
merges both registries through the production final-row builder and range
copier, and publishes after the actual pending receipt predecessor. Both
registries and their events appear together; the earlier source and pinned
snapshot remain unchanged. The other tests cover inline ownership, canonical
spill and out-of-order insertion, duplicate/foreign/unconsumed owners, insufficient
scratch, unexpectedly oversized allocator capacity, and preserved received
testament/Delivery target, receipt, cycle, seal flags and owned-buffer identity.

Workspace all-target Clippy with `-D warnings`, the separate production no-panic
gate, formatting and whitespace checks passed on macOS arm64. Logs are
`/tmp/focal-registry-clippy.log`, `/tmp/focal-registry-production.log` and
`/tmp/focal-registry-fmt.log`. Contract checks verified **699 architecture links,
37 imported hashes and 15 frozen vocabularies** in
`/tmp/focal-registry-contracts.log`. These results cover the borrowed journal
funding refactor too. Automatic cohort publication, audit bundle authoring,
native durable/CLI/MCP activation and distributed qualification remain open.

## Begin composition, original history and indexed seals — 2026-09-07

Every managed native Begin now keeps a private checked transition borrowing its
complete source evaluation state and retaining its authorized next state,
exact request/intent/time, and immutable publication source. It outlives
consumption of the preparation command without cloning a root or shared budget
handle. The
completion collector verifies this proof, the real Begun event and its original
position before accepting an intermediate begun revision followed by a seal.
An ordinary Begin with no later credit change creates no additional book
journal or phantom revision. Every Begun fact requires its matching proof,
including one without an attempt; an invented handler-free Begin cannot hide
behind an unchanged Ready row. Structural MissingTarget facts retain their
separate event kind and do not require external Begin authority.

The [candidate journal](../../crates/focal-core/src/native/completion_composition.rs)
holds at most two journals inline: the Begin installation and the complete
mixed update. The owner reserves its exact pending representation and checks
owner identities, adjacent revisions and original totals before transferring
either journal. Refusal drops candidate pages, rolls back mixed updates, then
returns the Begin's new funding and restores its prior index. Ordered commit
preserves a live grant even when a younger pending report has advanced it.
This path is now used by active BeginAdmission, BeginIncrement and BeginWork.

The shared [history visitor](../../crates/focal-core/src/native/history_assembly.rs)
also drives current publication. It emits the complete original fact sequence,
including creation/child-registration revisions, original evidence-result
ordinals and derived claim facts. It reuses the existing precharged history
workspace; it neither grows a hidden capture buffer nor changes current
construction profiles. An immutable
[original plan](../../crates/focal-core/src/native/original_plan.rs) now owns the
claim plan, extra rows and source/outcome context until that single emission
pass finishes. Explicit object-journal checks run before encapsulation; implicit
claim chains remain checked during emission. Original payloads cannot be edited
through this boundary and move only after the original event count is verified.
A future writer can append a separately checked seal suffix while retaining
this original prefix, without allocating a duplicate history buffer.

The [completion collector](../../crates/focal-core/src/native/completion_updates.rs)
now borrows a checked canonical index over the caller's existing seal-proof
slice. One linear validation rejects unchanged, duplicate or misordered proofs;
each lookup then uses binary search with a debit for every probe. Keys retain
the complete before binding, every target binding and slot, and generation.
The index allocates nothing and adds no visits to empty-proof report/deadline
paths. Proof ordering does not reorder the candidate's original events.

Qualification: **all 502 Core library tests passed**, with no failures, ignored
or filtered tests, using
`bash scripts/cargo.sh test -p focal-core --lib --offline --locked -- --test-threads=4`.
The final record is `/tmp/focal-seal-index-tests.log`. The preceding 499-test
set passed before and after the original-plan ownership refactor, recorded in
`/tmp/focal-begin-final-tests.log` and `/tmp/focal-original-plan-tests.log`.
This increment changes native Core
ownership/history code; the unchanged model, memory and evidence library
qualification remains recorded in the preceding four-library runs.

Four [composition tests](../../crates/focal-core/src/native/completion_composition_tests.rs)
cover mixed live rebind/retirement, exact funding/index restoration, a composed
head committed after a younger report, empty updates and invalid journal
ownership/order. Four [Begin-proof tests](../../crates/focal-core/src/native/prepare_begin_tests.rs)
use real Fresh authorization and candidate construction to check proof lifetime,
request/time identity and exact pending source roots. Omitted proofs,
missing/duplicate/foreign-key/misordered Begun facts and altered final states
refuse without losing the installed grant or its report credit. The fourth
test rejects a fake attempt-free Begun fact against the real source root and
an unchanged Ready row. Five
[history tests](../../crates/focal-core/src/native/history_assembly_tests.rs)
compare the visitor with actual publication for nested creation revisions,
Admission Accepted, claimant-received Delivery, and explicit WholeWork
Missing/entry positions; malformed rows and exhausted sinks/workspaces refuse.
Three [seal-lookup tests](../../crates/focal-core/src/native/completion_seal_lookup_tests.rs)
check every target variant's binding and slot identity, foreign generation and
binding refusal, and fifteen actual native Ready-member seal proofs searched
within four probes each after one complete order check. One fewer visit than
needed refuses. The mixed-journal fixture preserves reversed original seal
event order while supplying canonical proofs; reversed or duplicate proof
slices refuse without changing report credit, funding or book revision.

Workspace all-target Clippy with `-D warnings`, the separate production no-panic
gate, formatting and whitespace checks passed on macOS arm64. Logs are
`/tmp/focal-seal-index-clippy.log`, `/tmp/focal-seal-index-production.log` and
`/tmp/focal-seal-index-fmt.log`. Contract checks verified **707 architecture
links, 37 imported hashes and 15 frozen vocabularies**, recorded in
`/tmp/focal-seal-index-contracts.log`. These final checks cover the complete
Begin, original-plan and indexed-lookup changes; no new shared ownership was added.

Automatic cohort construction is still disabled. Remaining activation work includes full
affected-cohort demand and growth guards, checked seal-suffix assembly,
actual seal/registry writes and capability buffers.
Audit bundle authoring, complete claim/control completion promises, native
codecs/WAL/Session/quorum, CLI/MCP dispatch and distributed qualification remain
required; these ownership/history changes do not complete the storage rollout.

## Automatic native cohort sealing — 2026-09-07

The native RAM preparation path now records a complete validation cohort when
an actual claim first acquires its local seal. The
[automatic writer](../../crates/focal-core/src/native/cohort_seals.rs) compares
each final claim with the exact immutable effective source, including pending
predecessors. An unchanged original seal produces no second cohort. Newly
sealed claims include Required Admission failure, WholeWork entry/report and
graph consequences, cancellation, supersession and claim expiry/deadlock.
No additional participant command is required.

For each selected claim, the writer resolves its complete actual registration
set and immutable declarations. It uses staged evaluation rows before source
rows, so a report, authority fence or structural MissingTarget transition can
be followed by a seal without losing the intermediate state. The checked
model transition preserves target, generation, receipt, attempt, accepted
evidence and any prior authority fence. Begun chains keep their independent
report obligations; unbegun members remain Ready with a cohort suppression,
and terminal or already-sealed evaluation rows remain unchanged. The registry
is sealed once even when no evaluation needs a new revision.

The immutable [original plan](../../crates/focal-core/src/native/original_plan.rs)
retains the complete operation and its original history until one checked
emission pass finishes. A separately owned suffix supplies evaluation seals
and registry replacements. Original Accepted, Delivery and Missing sequence
and ordinal coordinates remain intact; seal events start after the complete
original prefix. This streams history directly into final storage changes
without an additional full-history capture buffer. Explicit journal validation
still checks the original transaction, and final merging consumes every
replacement exactly once.

The [prepared owner value](../../crates/focal-core/src/native/prepare.rs)
keeps checked seal capabilities alongside its candidate under the original
construction allocation. After range construction, that allocation shrinks to
the token buffer's actual retained capacity and survives until the completion
collector has checked every source, event and final state. Candidate pages
retain their own storage charges. The book combines report advancement,
same-attempt rebindings, retirement and any new Begin grant in the candidate's
commit/rollback journal. Refusal returns all provisional ownership and credit;
a seal cannot fabricate a report or consume an outstanding attempt.

The shared [cohort cost profile](../../crates/focal-core/src/native/cohort_budget.rs)
prices full future registration bounds, seal tokens and lookup records,
coexisting evaluation containers, registry copies, suffix history, range writes
and mixed-journal traversal. Required Admission holds its complete cohort as
a one-time failure surcharge; each remaining Work report holds the bounded
cohorts of its protected graph component. Later registration, policy and
retained-heap growth are checked against those original promises. Begin and
owner reconstruction validate complete actual sources and reserve seal
revision headroom; candidate evaluation facts preserve that headroom for
unbegun rows. Ordinary entry and controls still admit their own complete
consequence before publication. These are RAM report guarantees, not a promise
that every future claim/control operation or disk write is already funded.

The [completion collector](../../crates/focal-core/src/native/completion_updates.rs)
now verifies the actual MissingTarget-to-seal chain, including exact response
entry, absent manifest slot and Required internal result. Its structural
MissingTarget event phase is checked independently from the retained
evaluation's configured phase. This handler-free entry creates no external
attempt and spends no report credit.

Qualification also exposed a retained-journal accounting issue: a multi-update
book journal survives with its pending candidate until commit or discard, so
several pending reports can retain several journals. The correction to the
[Admission envelope](../../crates/focal-core/src/native/completion_envelope.rs)
and [Work envelope](../../crates/focal-core/src/native/completion_work.rs)
reserves that journal with Admission's one-time failure allowance and
with every remaining Work report. A maximum shared workspace alone cannot
cover those overlapping lifetimes. Construction and temporary seal proofs
continue to use their separately accounted preparation workspace. Visit-limit
refusals now distinguish cohort writer, source and completion-journal work
from a preparation-byte refusal.

Qualification: **980 unfiltered affected-library tests passed**: Core 515,
model 338, evidence 37 and memory 90. Executed
`bash scripts/cargo.sh test -p focal-core -p focal-model -p focal-evidence -p focal-memory --lib --offline --locked -- --test-threads=4`.
The complete record is `/tmp/focal-automatic-seal-final-tests.log`. The first
integration run failed; this qualification covers the corrected source and
fixtures, not that intermediate build.

The tests exercise real Required-failure and WholeWork publication, a Begin
followed by its own seal, late reports, complete registry closure, pending
rollback and exhausted ancestor capacity. Structural missing-target tests
reject omitted, reordered and impersonated history while retaining original
terminal Delivery evidence. Cancellation and deadline tests now verify the
distinct fence and seal revisions and preserve prior result coordinates.
The pending-journal regression keeps two real sealing reports pending together
with ancestor RAM exhausted; their separate retained charges survive head
commit and return exactly on tail discard. Exact envelope assertions separate
those retained journals from reusable construction workspace. Original-history
fixtures replay the same authorized operation against its actual source and
compare the complete original prefix before testing malformed journals.

Workspace all-target Clippy with `-D warnings`, the separate production no-panic
gate, formatting and whitespace checks passed on macOS arm64. Logs are
`/tmp/focal-automatic-seal-clippy.log`, `/tmp/focal-automatic-seal-production.log`
and `/tmp/focal-automatic-seal-format.log`. No additional Arc wrapper, thread,
unsafe code or dependency package was introduced. Architecture checks verify **715 links, 37 imported
source hashes and 15 frozen vocabularies** in
`/tmp/focal-automatic-seal-contracts.log`.

Native audit cohort rows and claimant result bundles still need complete
Accepted/Delivery/Missing history, bounded construction and late audit updates.
Guaranteed whole-claim completion funding and complete adoption, control and
runtime-scope integration remain open. Native codecs/import, WAL/Session/quorum
recovery and CLI/MCP activation are also unfinished; this increment does not
qualify a native disk restart. Client/server binary distribution, cross-platform
release execution and the distributed deployment/failure journeys remain
separate delivery work. The next lifecycle steps are recorded in
[18 §6.10](18-lifecycle-storage-upgrade.md#610-native-deadlines-and-remaining-audit-integration).

## Bounded native audit inspection — 2026-09-07

The native owner now derives an audit from one committed or effective pending
prefix through a funded borrowed callback. The
[audit reader](../../crates/focal-core/src/native/audit.rs) resolves the actual
claim, its complete sealed registration set, immutable definitions and current
evaluation states. It retains no second mutable evaluation registry. Incomplete
cohorts remain inspectable while previously begun participants finish; a read
does not freeze or post a claimant bundle.

[RegistrationSet](../../crates/focal-model/src/lifecycle/registration.rs) now
retains the original local-seal position. Its private borrowed audit capability
requires that stamp to match the actual claim's original seal and immutable
policy. An earlier open-claim target seal has no audit authority without this
stamp. Terminal evaluations that completed before the seal retain their exact
original rows; unbegun suppressed members retain no invented verdict.

The [native history scan](../../crates/focal-core/src/native/audit_history.rs)
seeks each registered evaluation's complete Accepted, Delivery and Missing key
ranges. It refuses the wrong result family and validates every result against
its original event and request/timer outcome. External evidence also matches
the actual artifact's ID, descriptor hash and exact result provenance. Original
accepted revisions and publication positions must increase within each member;
attempt ordinals cannot substitute for revision-based storage keys. Late
reports may occur after the original seal, but every retained position must
belong to the captured prefix.

The [model bridge](../../crates/focal-model/src/lifecycle/audit_native.rs) checks
evaluations in complete registry order and consumes grouped history with one
forward cursor. Exact private declaration stamps, targets, generations and
receipts remain bound to each member. Contiguous attempts and the exact retained
latest result reject omitted or substituted retries. Counted in-place sorts
produce canonical audit ordering; a separate native index preserves original
publication positions. There is no repeated whole-history scan per member.

Byte and visit admission precedes owned construction. Source arrays, audit
buffers and the publication index remain charged through the callback, including
failure or unwind. Requested capacities are reconciled against actual
allocations before use. Completed cohorts reserve exactly their retained
results; incomplete cohorts reserve all remaining admitted result capacity.
[Fallible copies](../../crates/focal-model/src/lifecycle/audit_memory.rs) preserve
that capacity and independent owned buffers. Existing legacy audit construction
and result-testament APIs remain available with their original semantics; their
late-record path now also refuses removal or revision regression of prior
accepted evidence.

Qualification: **992 unfiltered affected-library tests passed**: Core 519,
model 343, memory 93 and evidence 37, with no failures or ignored tests.
The initial Core/model/memory run is recorded in
`/tmp/focal-native-audit-tests.log`. After the exact artifact-ID guard and its
corruption case were added, Core and evidence passed in
`/tmp/focal-native-audit-final-tests.log`; the count does not double-count
the repeated Core suite.

The five [model bridge tests](../../crates/focal-model/src/lifecycle/audit_native_tests.rs)
cover exact membership and original seal identity, malformed history, byte/visit
boundaries, both allocation-failure stages, independent owned copies and late
completion without new allocation. Four
[native owner tests](../../crates/focal-core/src/native/audit_tests.rs) cover
pending and committed retry/quality history, late Observe results, suppressed
Ready members, artifact-free Delivery/Missing positions, discard, query pressure
and corrupted history/publication/artifact identity. The separate production
no-panic gate passed in `/tmp/focal-native-audit-production.log`.
Final workspace all-target Clippy with `-D warnings` passed in
`/tmp/focal-native-audit-final-clippy.log`; formatting and whitespace checks
also passed. Architecture checks verified **727 links, 37 imported source
hashes and 15 frozen vocabularies** in `/tmp/focal-native-audit-contracts.log`.
This increment adds no Arc wrapper, unsafe code, thread or dependency package.

At this inspection checkpoint, native claimant-authorized result-testament
generation and posting remained unimplemented. This prerequisite does not guarantee funded
whole-claim closure, finish adoption/control/runtime-scope consequences, or
activate native codecs, disk recovery, WAL/Session/quorum or CLI/MCP dispatch.
Release distribution and the distributed deployment journeys remain separate
qualification work.

## Native claimant result testaments — 2026-09-07

The [native bundle owner](../../crates/focal-core/src/native/audit_bundle.rs)
implements `GenerateResultTestament { claim, id }` and
`PostResultTestament { expected }`. Generation authenticates the original
claimant against the actual claim and checks its current binding. The complete
source audit must have its original local seal and every member must be
terminal, explicitly fenced, or sealed and suppressed without having begun.
Begun Observe checks therefore still prevent premature bundle closure.
The guard accepts local completion independently of terminal claim status.
Qualification of generation while a live runtime scope still blocks graph
release requires the remaining native scope-register/release ingress. Current
native receipt acquisition settles declared start dependencies, and owned child
membership alone does not add a release wait; no synthetic scope state is used
to claim that end-to-end case passes.

Generation consumes the actual audit and publication index from one committed
or effective pending prefix. The bundle retains that captured prefix, the
original claim seal, every accepted result's original sequence/ordinal, and
exact suppression, seal-cause and fence facts. Its native content hash commits
the complete canonical model audit and the separate publication witnesses.
The model retains the optional seal-cause hash itself, not merely a sealed flag.
Canonical validation and hashing use bounded linear passes; capacities and
arrival order are not audit content. The native owner assigns bundle revision
one and records its Generated position independently of the source capture.

Posting requires the claimant and exact stored Generated bundle binding. It
advances that object's revision and state to Posted while preserving its
content, frozen cohort and original positions. It does not reconstruct the
audit or require the parent to remain at its earlier revision after graph
progress. Exact retries return the retained request outcome. A per-claim index
permits only one bundle, including across pending candidates, and both bundle
generation and response closure reject a testament ID already used by the
other role.

The bundle has a separate native row, per-claim index, history fact and counter.
Generation uses five changed keys: the bundle, index, Generated event, metadata
and outcome. Posting uses four: the replacement bundle, Posted event, metadata
and outcome. Both leave claim, response, artifact and evaluation rows unchanged.
The ledger counter increases only on generation; the outcome counts the touched
bundle row. Existing evaluator-authored per-result artifacts remain the bundle's
evidence. No second aggregate artifact, result, receipt or response testimony is
manufactured, and bundle membership does not attach evidence to work slots.

Both operations use ordinary RAM admission. The
[consuming audit builder](../../crates/focal-core/src/native/audit.rs) charges
all coexisting source/output buffers to the transaction's preheld `Scratch`
allowance, then moves the cohort and publication index into the owned row.
It does not borrow and prematurely release query permits or copy the complete
audit again. Posting precharges the complete fallible owned copy. With `E`
members and `R` results, final canonical hashing and copying have checked
linear bounds in `E + R`; posting counts both its result and publication arrays.
Native stored-history packing and borrowed/pinned read projections cover the
distinct role. Existing V1 bytes, codecs and command identities remain unchanged.

Qualification: **1,005 unfiltered affected-library tests passed**: Core 528,
model 347, memory 93 and evidence 37, with no failures or ignored tests. Final
Core results are in `/tmp/focal-native-bundle-core-final.log`; the final model
run, including exact seal causes, is in `/tmp/focal-native-bundle-model-final.log`.
Memory/evidence results are in `/tmp/focal-native-bundle-model-tests.log`;
repeated suites are not counted twice.

Nine native bundle tests cover claimant authority, exact bindings and retries,
pending uniqueness and reciprocal role collisions, complete late Observe
retry/quality history, deadline fences and structural results without invented
artifacts, rollback and memory pressure, and independently retained read leases.
Actual transactions refuse four/three-key limits and succeed at five/four keys
for generation/posting. A retained Core reconstructs a new RAM owner which can
post and retry the frozen bundle. Another actual owner history completes a
second claim and generates its independent audit before posting the first;
the first bundle's original capture and result positions remain unchanged.
That test establishes independent ledger progress, not the pending runtime-scope
activation case above. Four model fingerprint tests cover the fixed native
encoding, complete field sensitivity, canonical late-arrival order and immutable
content through posting.

Workspace all-target Clippy with `-D warnings`, the separate production no-panic
gate, formatting and whitespace checks passed. Logs are
`/tmp/focal-native-bundle-final-clippy.log`,
`/tmp/focal-native-bundle-production.log` and
`/tmp/focal-native-bundle-format.log`. Architecture checks verified **733 links,
37 imported source hashes and 15 frozen vocabularies** in
`/tmp/focal-native-bundle-contracts.log`. No Arc wrapper, unsafe code, thread or
dependency package was introduced by this increment.

These operations do not reserve guaranteed future whole-claim closure. At this
bundle checkpoint, complete adoption/control/runtime-scope consequences, native durable codecs/import,
WAL/Session/quorum recovery, CLI/MCP activation, release distribution and the
distributed deployment journeys remained open. This is an implemented native RAM
transaction boundary, not a qualified native disk-restart or live protocol path.

## Native receipt adoption and retained open cycles — 2026-09-07

The [native adoption transaction](../../crates/focal-core/src/native/adoption.rs)
implements `AdoptReceipt { expected, previous, receipt, holder }`. Only the
claim's original issuer may replace its actual current receipt entitlement.
The owner checks the exact claim binding and previous fence against retained
receipt allocation, assigns exactly the next epoch, and requires a fresh
nonzero receipt ID across the ledger and pending prefix. The replacement holder
must be an addressable nonzero participant. Adoption refuses an expired authored
deadline, local or terminal completion, or an exhausted response-cycle allowance;
it cannot create extra authored response capacity.

The checked [model capability](../../crates/focal-model/src/lifecycle/claim_adoption.rs)
borrows the exact original claim. Applying it advances the claim revision once
and replaces the entitlement while preserving immutable content, the attained
claim status, response history and local outcome facts. Publication records the
same-status claim revision, exact previous and replacement entitlements, original
cause and every changed evaluation fence in one candidate. The transfer records
one new receipt allocation without inventing a testament, diagnostic, work
artifact, acceptance result or graph outcome. A receipt change alone does not change dependency or
local-completion truth.

The owner visits the full actual registration set and validates every retained
definition, target, original receipt and evaluation state. Every eligible
unfenced evaluation receives the adoption fence, including Ready and begun
Admission members that have no receipt field. Existing terminal results and
earlier fences remain unchanged. Accepted results, original attempts, artifact
evidence and publication positions remain available for the later sealed audit;
the transfer does not reuse old results as new-holder evidence.

The same checked completion journal retires active report capacity and graph
protections for the fenced grants before the proposed prefix passes ordinary
growth and slot checks. A stale old-holder response or evaluator report cannot
acquire authority from the replacement receipt. Reports already accepted before
adoption retain their original outcomes, and exact request retries still resolve
before fresh authority or resource admission. Discarding or refusing a pending
adoption restores the prior receipt, grant bindings, remaining report credits and
funding; committed adoption retires responsibility without discarding retained
or pinned result pages.

An adoption can leave real work or diagnostics in an unclosed cycle. The
[retired-cycle index](../../crates/focal-core/src/native/retired_cycles.rs)
retains that exact cycle and its original holder through a per-claim head and
immutable links. Empty open cycles add no retired entry. Closed cycles remain
reachable through the original response chain. The replacement holder uses the
next cycle derived from response history; receipt ID and epoch distinguish an
abandoned cycle from a new cycle with the same numeric position or slot name.
Old work, diagnostics, responses and their slot indexes are preserved, and old
responses are not automatically adopted or attached as replacement evidence.
The claimant can still explicitly acknowledge an old unattached artifact after
adoption. That advances only its independent artifact lifecycle, retaining its
original receipt and provenance; it supplies no replacement-holder work.

The [complete work cursor](../../crates/focal-core/src/native/projection_work.rs)
traverses the current open cycle, retained abandoned cycles and the complete
closed response lineage without a ledger scan or allocated membership list.
Exact link termination, descending receipt epochs, original receipt allocation,
work provenance and aggregate counts reject broken or duplicated membership.
Diagnostic-only retired cycles consume traversal capacity even when they yield
no work. The model retains all evaluation registrations for audit, reopens only
the replacement entitlement's Increment target seal, and checks current readiness
and historical entry facts against their respective receipt identities.

[Registration bounds](../../crates/focal-core/src/native/response_budget.rs)
add retired work's Increment targets to the immutable authored-response baseline.
The [projection quote](../../crates/focal-core/src/native/projection_quote.rs)
includes both retained work and the extra cycle, receipt and link lookups.
New WholeWork completion contracts price that complete shape; held contracts
continue to protect their original limits. Adoption refuses atomically if
retained membership, future registration capacity, visits, batch rows, memory or
an affected held grant's bound cannot accommodate it. It does not expand a
previously funded promise without admission or silently omit old evidence.

Qualification: **1,031 unfiltered affected-library tests passed**: Core 544,
model 357, memory 93 and evidence 37, with zero failed or ignored tests. Final
Core results are in `/tmp/focal-native-adoption-core-qualified.log`; model,
memory and evidence results are in `/tmp/focal-native-adoption-libraries.log`.
Repeated Core runs are not counted twice.

The 16 new native tests cover actual authority and receipt-ID conflicts,
same-status claim and receipt history, pending/committed retries, no automatic
testimony, replacement success/failure testimony, preserved old-artifact
observation, Admission and Work fences, report/adoption ordering, pressure refusal
and rollback restoring held report capacity. Increment coverage retains a real
Error result, resets the old target seal, discards a staged adoption and fresh
work together, then validates replacement work under the new receipt and reaches
satisfaction with the old history still present in the audit. Retired-cycle tests
exercise repeated slot reuse, diagnostic-only cycles, bounded traversal and
corrupt chain termination. Two deliberately corrupted-source tests reject a
broken closed-cycle link and a substituted MissingSlot target when the real
response contains that slot; healthy copied owner history first proves the
transfer is otherwise admissible. These are explicit corruption fixtures, not
claims that participant ingress permits those states.

Ten model tests cover the complete borrowed adoption capability, source changes,
exact-next-epoch and overflow boundaries, all relevant validation phases,
terminal/fenced preservation, registry reopening, and receipt-qualified
historical entry and work membership. Existing exact cursor-budget tests now
account for the additional retired-index lookup without relaxing their failure
boundaries.

Workspace all-target Clippy with `-D warnings` and the separate production
no-panic gate passed, recorded in `/tmp/focal-native-adoption-clippy-final.log`
and `/tmp/focal-native-adoption-production.log`. Formatting and whitespace checks
passed; the format check is in `/tmp/focal-native-adoption-format.log`.
Architecture checks verified **750 links, 37 imported source hashes and 15 frozen
vocabularies**, recorded in `/tmp/focal-native-adoption-contracts-final.log`.
This increment adds no Arc wrapper, unsafe code, thread or dependency.

This remains a native RAM owner transaction. Whole-claim completion funding,
remaining control/succession/runtime-scope integration, native durable codecs and
import, WAL/Session/quorum recovery and CLI/MCP activation remain open. Adoption
does not launch a participant, worker, tool or model job, and this implementation
does not establish a native disk-restart or deployment guarantee.
Document 18 §6.12 records the next scope integration sequence, including the
precharge, monitor-release, reverse-subscription, deadline and funded graph
consequence prerequisites which must accompany active native monitor admission.

## Native owner-scope release — 2026-09-07

The [native release transaction](../../crates/focal-core/src/native/scope_release.rs)
implements `ReleaseScope { expected }`. It authenticates the original issuer,
checks the exact current claim binding and requires a terminal, unreleased owner.
The complete retained owned-child set must already be released. Neither a caller
summary nor cancellation alone supplies that proof. The transaction does not
cancel children, release another owner's scope, detach work, or create testimony
or evaluator evidence. Fresh release of an already-released owner refuses;
exact request retries return their original outcome.

The initiating `OwnerReleased` claim event records a single revision advance
without changing status. The claim retains its original terminal cut, local
seal, receipt, immutable content and response history; its separate scope release
cut records the actual publication sequence and request intent. Release adds no
evaluation fence and does not retire eligible late report grants. Already-begun
checks continue under their existing receipt, evaluator, deadline and control
guards, with their original evidence and acceptance history intact.

The [bounded model release plan](../../crates/focal-model/src/lifecycle/scope_release.rs)
checks actual owner and peer bindings before allocation. Its construction quote
includes the replacement scope registry, nested roots, owned children, complete
read-binding buffer and allocator overhead. The native owner charges that quote
before building, verifies actual capacities, and separately charges graph/peer
buffers and the claim copy. Canonically ordered children and peers use a bounded
merge instead of repeated child-by-peer searches. The native bounded path refuses
unreleased active monitors; their terminal-owner disposition remains a separate
integration requirement.

The private [released-root capability](../../crates/focal-core/src/native/graph_owner_release.rs)
consumes a real checked model `OwnerReleased` transition against the exact
borrowed source view. It exposes the resulting claim only immutably before the
initial event is recorded. Graph propagation consumes that capability through a
separate entry; ordinary report preparation retains its unchanged-scope guard.
Complete outgoing/incoming and owned-child discovery uses retained native indexes,
including original terminal peers. Only genuinely proved dependency failure or
graph satisfaction may add subsequent claim transitions. Those rows, original
release event, any checked cohort suffix and request outcome publish atomically.

This command uses ordinary RAM admission and existing completion protection
checks. Byte, visit, revision, event, batch and outcome limits can refuse the
entire candidate. Refusal or pending discard preserves the prior release state,
claim history and held report capacity; no intermediate release becomes visible.
Traversal allowances currently bound individual preparation and history-checking
stages; they are not a single transaction-wide visit allowance. Shared traversal
accounting remains part of complete scope integration.
It does not establish guaranteed future scope or whole-claim completion funding.

Qualification on macOS arm64, 2026-09-07: the final unfiltered affected-library
run passes **915 tests** (554 core and 361 model), with no failures, ignored or
filtered tests. Four model tests cover exact byte/visit bounds, allocation
failures, complete peer bindings and monitor/child disposition. Ten core tests
cover the checked graph entry, real cancelled ownership trees, pending child-first
release, issuer/revision guards, exact retries, memory refusal and suffix discard,
unchanged satisfied testimony, and already-begun late reports under pressure.
The journal regression uses real external dependent claims and rejects eleven
corruptions, including duplicate facts hiding an unused final row.

Strict workspace all-target Clippy, the separate production no-panic gate,
formatting and diff checks pass. Architecture contracts resolve **762 links**,
preserve all **37 imported source hashes** and validate **15 frozen vocabularies**.
No Arc, unsafe code, thread or dependency was added by this increment. These
checks qualify the exercised native RAM paths; this was not a full workspace
test run or a native durable-restart/deployment qualification.

Active monitor registration and named rebinding, reverse monitor subscriptions,
monitor deadlines and complete funded multi-revision settlement remain required
by [18 §6.12](18-lifecycle-storage-upgrade.md#612-runtime-scope-integration-sequence).
Native durable codecs/import, WAL/Session/quorum recovery and CLI/MCP activation
remain open. This owner release records a peer's explicit lifecycle action; it
does not launch a worker, tool or participant continuation.

## Bounded monitor construction, disposition and deadline assessment — 2026-09-07

The [monitor construction plans](../../crates/focal-model/src/lifecycle/scope_monitor.rs)
authorize registration and named successor rebinding against the exact owner,
receipt and complete graph peers. Successful release requires each exact root
predicate to settle. The borrowed plan allocates nothing during preflight and
quotes the complete compact scope registry, nested roots, children, binding-read
buffer and allocator metadata. It accounts for construction traversals before
building, checks every allocated capacity and reports actual final charge.
Source graph/peers, caller ClaimState copies and future native subscriptions are
separate construction obligations. The existing legacy methods remain available;
the new bounded entry points provide the native construction seam.

Explicit terminal-owner monitor cancellation closes a previously unresolved
teardown case. A `Satisfied(B)` wait cannot successfully release after B fails.
The original claimant may instead cancel that monitor, subject to current owner
binding/receipt and terminal-state checks. A single disposition enum records
either successful release or cancellation, with the original terminal position
and distinct cancellation cut. Roots, deadline, registration and rebinding history
remain intact. The graph excludes cancelled waits and advances its minimum cut;
the owner-release model accepts disposed monitors but still requires every owned
child to have released. No target, child, claim outcome, response or validation
result is fabricated by cancellation. The [glossary](../../CONTEXT.md) and
[domain contract](02-domain-and-lifecycle.md#72-monitor-implementation-contract)
record this distinction.

The [monitor deadline plan](../../crates/focal-model/src/lifecycle/scope_deadline.rs)
checks monitor identity, full deadline, due logical time and complete source cut
before allocation. It quotes SCC workspace and carries one traversal allowance
through preflight/query. Inactive and settled waits return without SCC allocation.
An unsettled wait must use the captured earliest effective deadline: the query
either returns a canonical deadlock witness or constructs private negative-SCC
expiry authority. The exact owner/peer check on `expire_monitor` permits a real
earlier monitor deadline without modifying the claim's authored deadline.
Deadline mismatch, stale peers, construction failure and exhausted traversal
remain errors, never authority for an expiry fallback.

Qualification on macOS arm64, 2026-09-07: the final unfiltered affected-library
run passes **928 tests** (554 core and 374 model), with zero failed, ignored or
filtered tests. Thirteen new model tests cover allocation-free quoting and exact
limits, each construction allocation's refusal, capacity reconciliation, actor/
receipt/revision and complete-peer guards, actual named successor rebinding,
distinct root predicates, impossible-wait cancellation, preservation of owned
children and original cuts, earlier monitor expiry, canonical SCC precedence,
settled/inactive timers and refusal without negative-cycle evidence. One initial
cycle fixture supplied graph sources out of canonical order; the corrected
fixture uses the ordered source graph and real checked registration transitions.

Strict workspace all-target Clippy, the separate production no-panic gate,
formatting and diff checks pass. Architecture contracts resolve **772 links**,
preserve all **37 imported source hashes** and validate **15 frozen vocabularies**.
No Arc, unsafe code, thread or dependency was added. This is a model increment,
not native monitor ingress or disk-restart qualification; the full workspace test
suite was not rerun for this increment.
The native graph and receipt readers recognize disposed monitors while retaining
their guards against active monitor admission. Reverse subscription storage,
atomic native register/rebind/disposition history, typed durable timer retries,
complete iterative consequences and held completion bounds remain on the
critical path in [18 §6.12](18-lifecycle-storage-upgrade.md#612-runtime-scope-integration-sequence).

## Native monitor publication and report reservations — 2026-09-07

The [native monitor commands](../../crates/focal-core/src/native/monitor_commands.rs)
implement claimant registration, explicit named-successor rebinding and terminal
owner cancellation. Each checks the exact current owner binding and receipt.
Registration retains a globally unique monitor allocation; rebinding retains its
original registration and finite deadline. Each actual scope transition records
one claim revision and a typed monitor event at the same publication cut.
Cancellation retains the owner's original failure and does not masquerade as
successful predicate settlement or release its owned children.

The [reverse index](../../crates/focal-core/src/native/monitor_index.rs) stores
direct-root subscriptions with checked owner, registration and rebind provenance.
Removed links become bounded retained tombstones; IDs cannot be recycled. Both
directions are checked against actual scopes before graph discovery. Journal
validation independently reconstructs all and only the proposed index changes,
including registration followed by immediate release and several owners sharing
one subscription chain. This is direct-root indexing with connected graph
discovery; the planned materialized transitive closure/cache remains separate.

The [consequence engine](../../crates/focal-core/src/native/graph_monitor_effects.rs)
recaptures a frozen graph after each real transition, up to one release per
active monitor and one terminal transition per live claim. Terminal owners can
still release their monitors without changing their original outcome, receipt or
testimony. Work entry/reporting, owner release, cancellation, supersession and
deadline resolution use these consequences. Explicit owner-scope release remains
an independently authorized claimant action. The
[control proof](../../crates/focal-core/src/native/control_graph.rs) retains the
real original creation/control plan and exact history prefix before appending
graph effects; it does not reconstruct imaginary intermediate claim states.

[Monitor timer ingress](../../crates/focal-core/src/native/monitor_ingress.rs)
uses a disjoint claim/monitor/timer/generation key and hashes the complete
deadline into retry identity. Resolution validates the retained monitor
allocation even for inactive timers. SCC resolution, release and fresh-trigger
reassessment finish before timer consumption. Only a typed negative-SCC witness
permits expiry; earlier effective deadlines, stale sources and resource refusal
remain errors. An actual monitor can expire a claim with no claim-level deadline,
and its real evaluation cohort is fenced without manufacturing evidence.

[Held Work report contracts](../../crates/focal-core/src/native/completion_work.rs)
now reserve monitor revisions/events, index edits, cumulative graph captures and
row copies, journal replay, cohort consequences and source rechecks. Ordinary
bounded stages retain their existing admission limits; accepting a Work attempt
additionally requires the complete future report bound. Protected component
membership survives a monitor's release by retaining the original member IDs.
The topology fingerprint permits disposition changes while pinning monitor
identity, roots, registration, rebind and deadline. New monitor endpoints also
visit affected grants, so a change cannot evade a held report's growth checks
through an otherwise unchanged target row.

Qualification on macOS arm64, 2026-09-07: the final unfiltered affected-library
run passes **953 tests** (576 core and 377 model), with zero failed, ignored or
filtered tests. Twenty-five added regressions cover exact subscription replay,
multiple releases and intermediate revisions, earlier monitor deadlines and SCC
precedence, deadline-less claim expiry, actor/receipt/revision refusal, real named
supersession, terminal-owner cancellation, retained original cuts and rejection
of forged control history. A held Work report settles two real monitors with
the ancestor RAM budget exhausted; discarding that candidate restores its waits
and the original report promise, and the report succeeds again under pressure.
Frozen V1 codec/hash tests remain in the passing library run. The complete
workspace test suite and disk-restart/distributed monitor tests were not run for
this native increment.

Strict workspace all-target Clippy and the separate production no-panic gate
pass. Formatting, diff and architecture checks pass: **784 links**, all **37
imported source hashes** and **15 frozen vocabularies**. No Arc, unsafe code,
thread or dependency was added for native monitors.

The live service, codecs and CLI/MCP still select V1. Remaining work includes
Admission failure propagation and its held report bounds, complete predicate and
control interleaving qualification, guaranteed whole-claim RAM and disk capacity,
materialized monitor closures, durable wake/reconciliation, native codecs,
WAL/Session/quorum recovery and interface activation. These RAM transactions do
not establish disk restart or distributed monitor recovery.

## Native Admission failure propagation and held reports — 2026-09-07

The Admission graph work left open in the preceding increment is now implemented
in the native RAM owner. A Required Admission result that closes a Posted claim
retains its actual evidence artifact, evaluation transition and accepted result
at original ordinals 0, 1 and 2. The original `PostFailed` event follows at ordinal
3. The [report proof](../../crates/focal-core/src/native/admission_graph.rs)
preserves that exact failure binding and Required cut while the same transaction
publishes dependent failures, monitor releases and automatic evaluation seals.
Later monitor revisions can advance the reporting claim without replacing its
original failure or changing the accepted result's coordinates. Receipt or a
terminal parent closes Admission; eligible begun reports afterward remain
independent audit evidence. No graph consequence fabricates respondent testimony
or an explicit owner-scope release.

The [Admission completion envelope](../../crates/focal-core/src/native/completion_admission_graph.rs)
reserves the complete possible failure before a Required attempt on a Posted
claim is accepted. It includes graph and monitor revisions, reverse-index edits,
original and seal history, final row heaps, temporary copies, journal validation
and completion accounting. One possible failure has a separate allowance from
the remaining ordinary reports. Reconstruction of already begun evaluations
derives the same remaining responsibility and returns the original Core intact
if funding is refused. Observe checks and already closed Admission parents keep
their ordinary report contracts.

[Shared graph protection](../../crates/focal-core/src/native/completion_graph.rs)
now covers both Admission and Work grants. Even an isolated Posted parent retains
its original component membership, so a later incoming dependency or monitor
cannot add unfunded consequences through an unchanged target row. Complete
original membership survives monitor release. Pending publication and discard
journal grant accounting and protection membership together; committing a retired
grant refunds its retained member buffer.

The completion collector now classifies a failure from the actual original
event and source parent. Inferring it from a final revision rejected a valid
failure followed by a monitor release. Counting every Admission journal longer
than three events as a new failure also rejected valid late reports with cohort
seals. Both owner dispatch and collection use the checked source transition,
original event identity and original terminal cut. Missing original failure
history is refused, and the additional indexed lookup is priced before Begin.

Qualification on macOS arm64, 2026-09-07: the final unfiltered affected-library
run passes **963 tests** (586 core and 377 model), with zero failed, ignored or
filtered tests. Ten new regressions cover failure journal corruption, preserved
evidence and accepted ordinals, retained component growth, reconstruction and
complete report funding. The pressure case retains an earlier Error artifact,
then publishes a failing result, a dependent failure and two monitor releases
with the ancestor RAM budget exhausted. One release advances the reporting
claim itself after its initial failure. Discard restores the original claims,
monitors, evaluation states, report credit and result visibility; the same report
then succeeds again under pressure. A late Observe report preserves those
terminal cuts and adds only its three original report facts. Existing frozen V1
codec/hash tests remain in the passing library run.

Strict workspace all-target Clippy and the separate production no-panic gate
pass. Formatting, diff and architecture checks pass: **795 links**, all **37
imported source hashes** and **15 frozen vocabularies**. No Arc, unsafe code,
thread or dependency was added for this increment.

The live service, codecs and CLI/MCP still select V1. Remaining work includes
whole-claim RAM and disk capacity guarantees, complete control/predicate
interleaving qualification, materialized monitor closures, durable wake and
reconciliation, native codecs, WAL/Session/quorum recovery and interface
activation. This increment does not qualify disk restart or distributed recovery;
the complete workspace test suite was not rerun.

## Native respondent reporting capacity — 2026-09-07

The native owner now reserves mandatory respondent reporting capacity before
accepting an acquired or adopted receipt. The
[respondent envelope](../../crates/focal-core/src/native/respondent_envelope.rs)
funds one bounded Work-failure diagnostic, one authored response close and one
post per remaining response cycle. Optional successful output payloads retain
ordinary admission. Receipt does not generate a testament: the authenticated
holder supplies the outcome, summary, confidence and actual evidence after its
work succeeds or fails.

The [state reader](../../crates/focal-core/src/native/respondent_state.rs)
reconstructs independent diagnostic, close and post counts from actual receipts,
cycle membership, complete response lineage and exact claim-history stamps.
Several Generated responses can await posting independently. Closing a later
cycle preserves the earlier reports' posting capacity. The first actual Work
diagnostic fulfills its cycle's diagnostic allowance; other diagnostic reasons
and extra diagnostics use ordinary admission and cannot consume the last
unspent mandatory diagnostic slot.

[Respondent bookkeeping](../../crates/focal-core/src/native/respondent_book.rs)
uses the existing completion pool and a uniquely owned index. Receipt admission
funds future retained range writes and a shared maximum construction/verification
workspace through Ordinary capacity. Full finite-record accounting includes
response rows, artifacts, outcomes, events and sequence/candidate margins.
Optional writes must preserve the admitted source, parent/registry, authored
buffer, response history and revision bounds. No per-receipt Arc or separate
memory pool was introduced.

The [owner adapter](../../crates/focal-core/src/native/respondent_owner.rs)
selects the authenticated operation's loan before custody verification or owned
construction. Its guaranteed diagnostic path pins the builtin error-report
schema; other trusted schemas use ordinary verification. Closing and posting
stored testimony do not consult a new diagnostic verifier. Reconstruction
validates exhausted receipts without acquiring an unnecessary schema contract.
Unavailable funding returns the original Core intact; this is checked transfer
of an already-owned native RAM state, not disk checkpoint decoding.

Candidate journals compose evaluator updates, respondent retirement and new
receipt installation in fixed inline ownership slots. Refusal drops prepared
pages before reversing their credit; tail discard restores the old receipt's
funding, and head commit preserves later pending operations. Adoption must fund
the replacement receipt before it takes effect. Graph-triggering Work reports
and the one possible Admission failure additionally price retirement journals
for affected respondents. Independent evidence and original response history
remain retained after their reporting allowances retire.

Qualification on macOS arm64, 2026-09-07: the final unfiltered affected-library
run passes **985 tests** (608 core and 377 model), with zero failed, ignored or
filtered tests. Twenty-two new regressions cover independent credits and Generated
backlogs, actual-source reconstruction, bounded descriptors and authored buffers,
finite-record accounting, adoption, composed journal rollback, and schema selection.
The managed pressure test records diagnostics and authored Failed/Partial
testaments through all four admitted cycles, refilling the ancestor RAM budget
before each diagnostic, close and post. It retains an earlier snapshot, exercises
discard/retry at each action, and retires the final allowance without erasing
evidence. Separate cases preserve the old holder's reporting ability after
refused receipt/adoption, reject a diagnostic that would strand mandatory failure
reporting, and reconstruct outstanding close/post responsibility. A corrupted
response-cycle association is now rejected during owner reconstruction, returning
the original Core, terminal Receipt results, evidence and budget intact.

Strict workspace all-target Clippy and the separate production no-panic gate
pass. Formatting, diff and architecture checks pass: **806 links**, all **37
imported source hashes** and **15 frozen vocabularies**. No Arc, unsafe code,
thread or dependency was added for this increment. Frozen V1 codec/hash tests
remain in the passing library run; the full workspace test suite was not rerun.

The live service, codecs and CLI/MCP still select V1. This milestone establishes
native RAM/discrete-record reporting capacity, not disk restart or distributed
durability. Remaining work includes claimant receipt/evaluation-entry and complete
whole-claim/control guarantees, disk and replica quotas, native codecs/import,
WAL/Session/quorum recovery and interface activation, durable reconciliation and
distributed deployment qualification. External execution and physical allocation,
storage or quorum failures remain explicit failures, never invented testimony or
successful durability acknowledgments.

## Bounded native input preparation — 2026-09-07

The allocation-free input inspection prerequisites now expose the complete
fields currently represented by native declarations, acceptance policies,
artifact descriptors and respondent reports. These are Rust model APIs; no byte
decoder or owner wire ingress was activated.

[AcceptancePlan](../../crates/focal-model/src/lifecycle/acceptance_prepare.rs)
checks full slot/declaration correspondence, mandatory Delivery, duplicate
identities and missing-slot index collisions before allocating. It quotes final
buffers and allocator counts separately, preserves declaration order independence,
and builds under an explicit byte allowance. The canonical fingerprint shares
the existing owned-policy encoding. Canonical selection currently has bounded
quadratic work in the declaration count; a future decoder must budget the actual
scans and sort as well as buffers.

[Declaration views](../../crates/focal-model/src/lifecycle/validation_definition.rs)
expose complete deadlines, target policy, phase definitions and ordered borrowed
handler fallbacks. Prepared declarations expose the same identity as their built
form. [Artifact plans](../../crates/focal-model/src/lifecycle/artifact_descriptor.rs)
derive content and native request identity from the checked borrowed descriptor.
[Response input plans](../../crates/focal-core/src/native/response_input.rs)
quote and construct the respondent's actual summary, confidence, outcome,
manifest and diagnostics. They do not fabricate evidence or close a work cycle.

At this prerequisite checkpoint, the unfiltered core/model run passed **1,000
tests** (612 core and 388 model), with zero failures, ignored or filtered tests.
Fifteen new tests cover full borrowed views, unchanged identities, correspondence,
all reported outcome/confidence values, dimensions and fallible construction.
Strict workspace all-target Clippy and the separate production no-panic gate
passed. The contract check passed **815 links**, all **37 imported source hashes**
and **15 frozen vocabularies**. These counts precede the authored-content work
below; they are not a full-workspace or distributed qualification.

The codec audit also identified a prerequisite semantic gap: native claim rows
and validation declarations omit the authored request and checking instructions.
Opaque bindings cannot recover those fields. [18 §6.13](18-lifecycle-storage-upgrade.md#613-successor-input-codec-dependency-plan)
now requires complete authored content and checked projections before format
freeze, alongside all 27 actor commands and three trusted timer namespaces.
The live service, CLI and MCP continue to use V1.

## Authored claim and validation descriptor foundations — 2026-09-07

Standalone native descriptors now retain the request and checking instructions
that the existing lifecycle projections omit. They are internal checked Rust
types, with no new persisted schema allocation or live dispatch.

The [claim descriptor](../../crates/focal-model/src/lifecycle/claim_descriptor.rs)
owns bounded instruction text, occurrence, canonical party/action/cause and
contextual relations, normalized authored work scopes, ordered requirement pins,
deadline and complete output-slot rules. Zero-check slots and separate required
presence rules remain part of the body. Party/action views derive from the
immutable relations. Self-directed drafts remain representable; a separate local
posting check rejects them except for Handoff, whose legitimacy still needs
owner verification. Unsupported relation profiles are refused, not discarded.

The [validation descriptor](../../crates/focal-model/src/lifecycle/validation_descriptor.rs)
owns the actual instruction, optional quality standard, contributor provenance
and policy revision with the exact target, evaluator, ordered fallback, attempt,
schema and deadline policy. Programmatic and agentic behavior follows the
declared program; text does not choose an execution phase. Pure Delivery refuses
a quality rubric. Contributors and referenced policy documents do not confer
evaluator authority, and Focal runs no tools or workers.

Both descriptors compute content identity from the complete checked body,
excluding their own allocated ID. Address-sensitive request identity remains
separate. Validation additionally computes an independent requirement
specification hash that excludes the allocated parent claim. A claim's
`RequirementRef.specification` pins that specification; the validation's full
content separately binds it to its actual parent. Existing native stamps, their
retry preimages and frozen V1 identities are unchanged. Pinned external evaluator
standards retain their original identities.

Borrowed preparation allocates nothing and quotes final dynamic bytes plus a
separate allocator count. Construction and compact copying use fallible exact
buffers with immediate capacity reconciliation; refusal leaves the input and
existing descriptor intact. Existing declaration construction/copying now uses
the same checked allocation mechanism, including target text and both handler
arrays. No per-object shared ownership was introduced.

Qualification on macOS arm64, 2026-09-07: the unfiltered affected-library run
passes **1,018 tests** (612 core and 406 model), with zero failed, ignored or
filtered tests. Eighteen new regressions cover independently encoded content
vectors, complete supported claim relations/scopes/slot policies and validation
program/target shapes, occurrence and requirement-specification identity,
authored-field mutations, malformed inputs and all partial construction/copy
allocation failures followed by exact retry. They preserve actual external
standard identities and keep Delivery distinct from evidence evaluation.

Strict workspace all-target Clippy and the separate production no-panic gate
pass. Formatting, diff and architecture checks pass: **839 links**, all **37
imported source hashes** and **15 frozen vocabularies**. No Arc, unsafe code,
thread or dependency was added for this increment. Frozen V1 codec/hash checks
remain in the passing library run; the full workspace test suite was not rerun.

These descriptors are not yet retained by Core. The next assembly/store work in
[18 §6.13](18-lifecycle-storage-upgrade.md#613-successor-input-codec-dependency-plan)
must derive or verify the actual graph, lineage and acceptance projection;
resolve exact declaration specifications and reference authority; install
immutable content and dedup indices atomically; and expose matching content/state
through one effective prefix. Independent immutable rows also need real page
isolation before claiming lifecycle updates never copy request text. Codec,
WAL/checkpoint/Session/quorum integration and CLI/MCP activation remain pending.

## Authored native creation and immutable page isolation — 2026-09-07

The native RAM owner now has a separate complete-content profile. It retains the
actual request and validation instructions together with their derived lifecycle
state; it does not reconstruct omitted text from projection hashes. The live
service, SDK, CLI and MCP still select V1.

[AuthoredCreationPlan](../../crates/focal-model/src/lifecycle/authored_creation.rs)
assembles graph, correction lineage and acceptance from exact claim and validation
descriptors. It verifies complete parent/issuer/specification/slot correspondence,
including Required Delivery and zero-check slots. Descriptor order need not match
requirement order. Its bounded construction quote includes final and temporary
buffers and allocator bookkeeping; canonical graph/lineage buffers move into the
projection without another copy. Actual counted inspection and build traversals
share one allowance. The consuming projection releases borrows before storage
moves the original bodies.

[CreateAuthored](../../crates/focal-core/src/native/authored.rs) groups each claim
with its actual validation bodies, response/scope capacity profile and original
owner precondition. The normal multi-claim CreationPlan still supplies owner,
lineage, graph and ordered-cut checks, including supersession and existing control
consequences. Complete contextual relation endpoints resolve against committed
state, earlier pending candidates and the proposed batch. Claims and validations
may reuse the same numeric ID bytes because addresses are scoped by object family.

`Core::new_native_authored` starts an empty AuthoredV1 RAM root; `new_native`
remains ProjectionOnly. Each refuses the other's creation input. The original
Create command's native tag/intent stays unchanged; the new command has its own
internal identity. This distinction assigns no durable schema, imports no missing
content and activates no transport.

[Owned content](../../crates/focal-core/src/native/owned.rs) retains each original
claim descriptor and responsibility profile in one fallible container. Definition
rows retain either the old declaration or one complete validation descriptor;
existing policy readers borrow the descriptor's actual declaration. Moving inputs
preserves instruction and handler allocations. The
[range partition layout](../../crates/focal-memory/src/range.rs) now has an optional
immutable classifier shared by preflight, construction and import. Native content,
definition, artifact and frozen creation-result namespaces occupy actual separate
pages. Unchanged small pages can be reused across lifecycle-only writes, as well
as already oversized rows; this uses no fake padding or new per-object Arc.
Unpartitioned range layout and merge charges remain unchanged.

[Preparation](../../crates/focal-core/src/native/authored_prepare.rs) installs
body, family/schema/full-content identity, definition, lifecycle, history and
ordered creation-result rows under one root. Entirely existing content produces
an outcome and frozen requested/resolved-ID mapping without new object facts.
Partial duplicates, conflicting IDs, changed responsibility profiles and duplicate
same-family content refuse atomically. Validation dedup uses the parent-bound
content hash; requirement pins use the separate specification hash. No parent or
requirement reference is silently rewritten.

The [publication audit](../../crates/focal-core/src/native/authored_check.rs)
checks exact row/index membership and the private retained-content proof before
common history emission. That proof binds full descriptor identities, the original
owner fence and every returned ordinal/requested/resolved ID. Publication auditing
has a separately preflighted, dimension-derived visit allowance; it does not reset
its budget for each lookup. Reconstruction checks actual bodies, descriptor
variants, profile/cause shape, bidirectional indices and retained heap charges,
returning the original Core on refusal. This is checked transfer of an existing
RAM root, not checkpoint decoding.

[Borrowed reads](../../crates/focal-core/src/native/authored_reads.rs) expose body,
policy, state and frozen result from the same effective or retained prefix.
Authored claim reads reject an existing state with a missing body. Posting checks
self-work from the actual descriptor. Self-Handoff currently refuses too: the
native owner does not yet have an authenticated transfer capability, and a label
alone cannot grant one. Participants continue to run their own tools and author
success/failure testimony explicitly; no receipt fabricates a testament.

Qualification on macOS arm64, 2026-09-07: the unfiltered affected-crate run passes
**1,175 tests** (638 core, 413 model and 124 memory), with zero failed, ignored or
filtered tests. Thirty-eight new regressions cover model assembly, real page
partition/import reuse, atomic authored retention and policy correspondence,
family-scoped IDs, complete/pending references, retry/dedup/profile refusals,
original-owner/result proof integrity, pinned reads and lifecycle allocation
addresses, malformed reconstruction, and fallible copying with exact retry.

Strict workspace all-target Clippy and the separate production no-panic gate
pass. Formatting, diff and architecture checks pass: **856 links**, all **37
imported source hashes** and **15 frozen vocabularies**. No per-object Arc,
unsafe code, thread or dependency was added. Existing range page/root sharing
remains in the memory engine. Frozen V1 codec/hash checks remain in the passing
libraries; the full workspace test suite was not rerun.

The remaining plan includes legitimate transfer/reference-policy authority,
complete claimant/whole-claim resource guarantees, successor input and recorded
mutation codecs, bounded checkpoint import, WAL/Session/quorum recovery and
CLI/MCP activation. Disk/replica quotas, durable reconciliation and global
placement/deployment qualification also remain. This milestone makes no claim of
native restart durability, live successor-interface activation or global scale.

## Complete native input framing and fixed-field decoding — 2026-09-07

The dormant [native input codec](../../crates/focal-core/src/native/input_codec.rs)
now represents all **28 actor commands** and the three separate evaluation,
claim and monitor timer namespaces. The complete grammar, field widths, tags,
resource rules and remaining activation gates are specified in
[21](21-native-input-format.md). No V1 format, intent preimage, replay dispatch or
live service/SDK/CLI/MCP selection changes.

Encoding covers complete authored claim and validation descriptors, legacy
projection/declaration cohorts, artifact bodies and provenance, response summary,
confidence, all six reported outcomes, manifests, diagnostics, monitor roots and
all deadline fields. The existing semantic fingerprints remain separate from
these bytes. Derived descriptor hashes/stamps are not serialized as substitutes
for bodies; external definition, handler, schema and requirement pins remain
explicit. Legacy acceptance summaries are omitted only after bounded verification
against the actual global declaration cohort. Assigned creation cuts and trusted
logical/firing times remain owner values.

`EncodingPlan` measures an immutable borrowed source before writing the same
source into an exact-size caller buffer. Checked cursor/sink primitives use
fixed-width little endian fields, explicit length prefixes and safe slice access.
They share deterministic byte/work accounting without allocating. A wrong-sized
destination refuses before modification. The complete structural inspector uses
one visit allowance and cumulative item/text/blob budgets across nested arrays
and fields, checking all closed tags, schema versions, UTF-8, truncation and
trailing bytes. Its borrowed header and dimension quote grant no authentication,
semantic identity, custody, retry decision or completion loan.

[Fixed-field decoding](../../crates/focal-core/src/native/input_codec/fixed.rs)
constructs typed inputs for **18 commands and all three timers** without heap
allocation. Its separate bounded pass preserves all bindings, IDs, revisions,
receipt fences, evaluation-target shapes and authored deadline values. The
structural quote supplies its exact second-pass visit allowance; callers must
account for both traversals. The normal owner retains all semantic and authority
checks. The ten dynamic commands return `None` after complete structural inspection
and remain pending semantic/funded construction. No empty body or alternative
decoder is substituted. The typed frame stays inline on the stack; a narrow
large-enum lint allowance avoids allocating before admission.

Qualification on macOS arm64, 2026-09-07: the unfiltered Core library suite passes
**664 tests**, with zero failed, ignored or filtered tests. The **26 new tests**
cover primitive byte/work refusal, complete independent descriptor/response/timer
vectors, all actor tags and timer target shapes, every actor-frame truncation,
malformed nested fields, shared quota exhaustion, output sentinel preservation,
creation-profile/acceptance correspondence, and fixed decoding's existing
byte/intent parity. A decoded missing-claim request is refused by the real owner
without range, budget, outcome or pending-candidate changes; structurally valid
invalid timer values still fail the existing semantic identity check.

Strict workspace all-target Clippy and the separate production no-panic gate
pass. Formatting, diff and architecture checks pass: **874 links**, all **37
imported source hashes** and **15 frozen vocabularies**. No per-object Arc,
unsafe code, thread or dependency was added. The full workspace test suite was
not rerun; frozen V1 codec/hash checks remain in the passing Core library suite.

The next boundary is allocation-free semantic inspection of the ten dynamic
commands, shared descriptor/intent validation and actual owner-funded typed
construction, including promised reports and responses with Ordinary RAM
exhausted. Recorded-mutation codecs, checked checkpoint hydration/import,
WAL/Ready and Session/quorum integration, then live CLI/MCP activation remain
required. This input milestone supplies no native restart guarantee or global
deployment qualification.

## Borrowed dynamic input construction — 2026-09-07

Eight more dormant native commands now construct typed inputs from bounded
borrowed bytes: CloseResponse, RegisterMonitor, SubmitWork, SubmitDiagnostic,
RejectWork, and Admission/Increment/WholeWork reports. Together with the fixed
decoder, this covers **26 of 28 actor commands** and all three timer namespaces.
Create and CreateAuthored remain. Live V1 service/SDK/CLI/MCP dispatch is unchanged;
these construction plans do not select an owner loan or activate persistence.

The [artifact value-source model](../../crates/focal-model/src/lifecycle/artifact_descriptor_source.rs)
shares semantic validation, canonical ordering, content/native-intent hashing,
construction accounting and fallible construction with the existing descriptor
API. Generic sources expose copied ObjectRefs and borrowed labels through bounded
repeatable iterators. Preparation requires exact declared cardinality and checks
full provenance/content; construction checks the actual produced descriptor,
including its allocated ID, against the captured identity. Changed sources,
including changes during iteration, cannot substitute a different body. Actual
capacities and allocation counts are reconciled before retaining output, with no
typed scratch array needed by the byte adapter.

[Response and monitor sources](../../crates/focal-core/src/native/response_source.rs)
likewise read copied manifest/diagnostic/root values without temporary vectors.
They preserve every confidence/outcome value and authored ordering. Plans price
final buffers and all preparation/hash/build work, require exact termination,
and hash the actual final owned body before returning. A failed response hash
leaves the caller's hasher unchanged. Existing immutable-slice response APIs
retain their signatures and native preimage through shared helpers.

The [response/monitor decoder](../../crates/focal-core/src/native/input_codec/response.rs)
borrows fixed-width encoded collections and captures the complete existing
request identity. The [artifact decoder](../../crates/focal-core/src/native/input_codec/artifact.rs)
borrows scalar fields, content pointers/inline bytes, and encoded reference/label
spans. One source parsing counter spans model preparation and construction. It
also verifies that the known second pass fits before returning the plan; build
cannot replenish that allowance. Model checking and native wrapper/hashing costs
have distinct explicit limits. The final artifact quote includes its singleton
container and all allocator bookkeeping, and final command identity is checked
after construction. [21 §6](21-native-input-format.md#6-borrowed-dynamic-construction)
records each component and the remaining ingress obligations.

The [shared artifact-command preimage](../../crates/focal-core/src/native/artifact_intent.rs)
and monitor/request helpers keep owned and borrowed command identities aligned.
Independent legacy preimage checks cover ReportAdmission, SubmitWork and RejectWork;
byte/intent roundtrips cover all six artifact-bearing command tags. These helpers
do not replace actual evaluator, receipt, custody or state-dependent admission
checks, and no participant execution was added.

Qualification on macOS arm64, 2026-09-07: the unfiltered affected-library run passes
**1,135 tests** (679 Core, 419 model, 37 evidence), with zero failed, ignored or
filtered tests. The **21 new tests** cover generic-source substitutions and exact
termination, partial allocation failure/retry, complete encoded bodies and all
artifact provenance roles, response outcomes and monitor predicates, separate
and shared byte/work limits, invalid raw fields that pass framing but fail model
checks, and unchanged native identities. Frozen V1 checks remain in the passing
libraries; the full workspace test suite was not rerun.

Strict workspace all-target Clippy and the production no-panic gate pass.
Formatting, diff and architecture checks pass: **885 links**, all **37 imported
source hashes** and **15 frozen vocabularies**. No Arc, unsafe code, thread or
dependency was added. Decoded scalar command fields stay on the stack with a
narrow large-enum lint allowance, avoiding allocation before admission.

Remaining native ingress work includes complete claim/validation creation
decoding, borrowed state-dependent admission, exact retry/held-source selection,
and keeping the selected capacity through construction, custody and candidate
preparation. Promised responses/reports still need end-to-end decoding tests with
Ordinary admission exhausted. Native recorded-mutation codecs, checkpoint
hydration, WAL/Ready, Session/quorum recovery and live CLI/MCP activation remain
required, followed by the outstanding deployment and scale qualification.

## Borrowed creation descriptor construction — 2026-09-07

The dormant native codec now constructs complete standalone claim, legacy
declaration and authored validation bodies through
[borrowed body plans](../../crates/focal-core/src/native/input_codec/creation_content.rs).
This completes their local descriptor prerequisites; whole Create/CreateAuthored
frame assembly, borrowed acceptance/cohort correspondence and owner admission
remain pending. Complete actor-command construction coverage therefore remains
**26 of 28**, plus all three timer namespaces. Live V1 service/SDK/CLI/MCP
dispatch and native persistence selection remain unchanged.

The model's new
[claim sources](../../crates/focal-model/src/lifecycle/claim_source.rs),
[declaration sources](../../crates/focal-model/src/lifecycle/validation_definition_source.rs)
and [authored validation sources](../../crates/focal-model/src/lifecycle/validation_descriptor_source.rs)
share semantic checking, original hash preimages and owned construction with the
existing slice APIs. Claim values retain canonical relations and work scopes,
ordered requirement pins, nested output checks and zero-check slots. Validation
values retain every program/phase/fallback field, description, quality standard,
contributor and policy revision. Each declared stream must yield exactly its
count and then end. Construction verifies actual owned content and capacities
against the prepared identity, including changes made by a generic source while
copying. Original native declaration stamps and claim/full/specification hashes
are preserved; external policy/schema pins remain separate authored references.

The byte adapters keep complete scalar fields and encoded collection spans
borrowed. A shared parsing counter covers repeated nested source callbacks,
separately from model checking and hash work. The borrowing plan cannot replenish
that counter. Preparation also checks that the known build passes fit before
returning a plan. Claim construction reads every outer collection and nested
check stream once; legacy declaration construction reads each handler stream
once. Authored validation construction explicitly charges three handler passes
and two contributor passes before final owned checking. Final-buffer capacities,
allocator bookkeeping and complete conservative model work are checked before
allocation; actual source consumption is reconciled after construction. No typed
scratch policy/reference arrays are allocated by these adapters. See
[21 §6.1](21-native-input-format.md#61-borrowed-creation-descriptor-bodies).

Qualification on macOS arm64, 2026-09-07: the unfiltered affected-library run
passes **1,155 tests** (685 Core, 433 model and 37 evidence), with zero failed,
ignored or filtered tests. The **20 new tests** cover complete nested byte bodies,
all validator programs and target families, exact byte/work/stream bounds,
truncation and trailing bytes, actual caller/semantic refusals, allocation
failure/retry, original identity preimages and source substitution during
construction. Frozen V1 checks remain in the passing libraries; the full
workspace test suite was not rerun. Strict workspace all-target Clippy and the
production no-panic gate pass. Formatting, diff and architecture checks pass:
**894 links**, all **37 imported source hashes** and **15 frozen vocabularies**.
No Arc, unsafe code, thread or dependency was added.

These are local content proofs, not authenticated request or memory-admission
capabilities. Remaining work includes full borrowed creation-cohort assembly,
Required Delivery and actual descriptor correspondence before owner-selected
funding, exact pending/committed retries, and reserved-source construction/custody
under Ordinary exhaustion. Native mutation/checkpoint codecs, restart recovery,
WAL/Ready, Session/quorum, live CLI/MCP activation and deployment/scale
qualification remain required. No receipt creates a testament, and these policy
decoders invoke no tool, skill, script or agent.

## Complete native creation frames and checked acceptance sources — 2026-09-07

The dormant native codec now constructs typed inputs for **all 28 actor commands**
and all three timer namespaces. The complete
[authored creation decoder](../../crates/focal-core/src/native/input_codec/authored_creation.rs)
and [projection creation decoder](../../crates/focal-core/src/native/input_codec/legacy_creation.rs)
check local creation correspondence and derive the existing native request
identity before allocating final owned inputs. Live server/SDK/CLI/MCP dispatch
still uses V1; input construction does not activate native durability.

The model's repeatable
[acceptance source plan](../../crates/focal-model/src/lifecycle/acceptance_source.rs)
requires actual opaque
[checked declaration metadata](../../crates/focal-model/src/lifecycle/validation_checked.rs),
produced only from real model declarations or successful source preparation.
Authored validation metadata performs an extra bounded pass over the same body,
deriving the original declaration stamp with its actual content binding while
rechecking full/specification identities. No supplied summary or digest can stand
in for missing policies. Acceptance checks Required Delivery, exact parent,
issuer and ledger, duplicate IDs/indices, missing-index conflicts and complete
slot/check correspondence. Zero-check slots retain their presence obligations.
Repeated passes verify complete source cardinality and identity; final
construction checks actual owned policy output. Existing typed acceptance API traversal
semantics and acceptance hash preimages are preserved.

Authored frame preparation checks complete requirement/specification matches,
per-claim descriptor cardinality and family-scoped IDs across groups, preserving
authored ordering and runtime scope/response/owner fields. Projection preparation
checks canonical graph/lineage streams and actual interleaved global declarations.
New bounded model value checks and consuming constructors move final
graph/correction vectors without temporary typed arrays. Both decoders use shared
original intent writers and verify the actual constructed native input. The owner
still assigns creation cuts and verifies effective-state authority and references.

Five cumulative work domains separate parsing, encoded-source callbacks,
descriptor checks, acceptance checks and structural/native work. Nested callbacks
cannot renew their enclosing allowance. Quotes include replayed preparation and
every final allocation; acceptance exposes actual full-scan counts so its repeated
raw callbacks are priced before construction. Frame-only body inspection does not
require unused future build headroom, while actual body builds check the complete
remaining source budget before allocation. Final authored capacity checks price
both scans over every scope and slot. Graph/lineage validators charge terminal
iterator probes even for empty streams. Review found and fixed those two variable
work-accounting gaps before final qualification. No Arc, unsafe code, thread or
dependency was added. Details and remaining ingress obligations are in
[21 §6.2](21-native-input-format.md#62-complete-creation-frames).

Qualification on macOS arm64, 2026-09-07: the unfiltered affected-library run
passes **1,175 tests** (697 Core, 441 model and 37 evidence), with zero failed,
ignored or filtered tests. The **20 new tests** cover complete mixed creation
frames, interleaved declarations, actual nested policies and specification pins,
byte/native-intent parity, exact five-domain limits, source substitution, final
allocation refusal/retry, callback terminal budgets and variable final inspection
work. A decoded authored request enters the real RAM owner and publishes its
claim content with zero response testaments. Frozen V1 checks remain in the
passing libraries; the full workspace test suite was not rerun. Strict workspace
all-target Clippy and the production no-panic gate pass. Architecture checks
verify **904 links**, all **37 imported source hashes** and **15 frozen
vocabularies**. Formatting and diff checks pass; all six legacy decoder tests
also pass after the final lint cleanup.

The remaining boundary is authenticated, effective-prefix owner inspection and
exact pending/committed retry selection, followed by preserving the selected
Ordinary or held capacity through construction, custody and candidate preparation.
Promised reports/responses still need complete ingress qualification with Ordinary
RAM exhausted. Native mutation/checkpoint codecs, recovery and entitlement
reconstruction, WAL/Ready, Session/quorum, live CLI/MCP activation, and deployment
and scale qualification remain required. Participants continue to author their
own evidence and success/failure testimony; no decoder invokes execution or
creates testimony from claim receipt.

## Managed admission of borrowed native requests — 2026-09-07

The [decoded-request wrapper](../../crates/focal-core/src/native/input_codec/admission.rs)
and [owner ingress](../../crates/focal-core/src/native/owner_ingress.rs) now connect
complete native input plans to the exclusive RAM owner. Fixed actor commands and
all dynamic plan families preserve their actual intent and profile; actor plan
conversion rejects timer namespaces. The owner verifies its ledger/profile and
the supplied authenticated principal, then shares exact committed/pending request
lookup with owned-input preparation. Original outcomes and pending tickets return
before fresh queue, clock, work, memory or current-state report admission.

Borrowed Admission, Increment and WholeWork report checks share actual evaluation,
attempt, evaluator, parent, target, provenance and inherited-visibility rules with
the existing owned path. A sealed artifact view accepts actual descriptors or
opaque model source plans, bounds replayed streams by captured dimensions and
retains the original source counter. The completion book checks the real held
parent/registration/schema contract before lending memory. Respondent selection
uses the actual receipt/holder/cycle and recorded response, with early close-ID
collision checks. The mandatory first Work diagnostic and authored close/post
operations select their existing credits; unrelated work retains Ordinary
admission.

The selected input reservation precedes final buffer construction and stays live
through custody and candidate preparation. Existing envelopes already include
that simultaneous input, verification and Core construction demand. A second
source-budget check refuses before allocation if borrowed preflight has consumed
the final build pass. Full owned authority and identity checks still precede
custody/publication. Rejections and discarded candidates release input capacity
and preserve the existing journal/retry contract. No Arc, unsafe code, thread or
dependency was added. See [21 §6.3](21-native-input-format.md#63-managed-owner-admission-of-decoded-requests).

Qualification on macOS arm64, 2026-09-07: the unfiltered affected-library run
passes **1,186 tests** (708 Core, 441 model and 37 evidence), with zero failed,
ignored or filtered tests. The **11 new tests** cover borrowed/owned report
authority parity and refusals, managed creation/fixed command chaining,
principal/profile/ledger/timer boundaries, complete Admission reporting and the
respondent error-artifact → authored Failed testament → explicit posting sequence
under full ancestor-memory exhaustion. Missing custody, stale receipts, malformed
identity, insufficient construction/source work, pending/committed retries and
discard/retry preserve source capacity and responsibility credits. Receipt still
creates no testimony. Strict workspace all-target Clippy and the production
no-panic gate pass; frozen V1 tests remain in the passing affected libraries.
The full workspace test suite was not rerun. Formatting and diff checks pass;
architecture checks verify **909 links**, all **37 imported source hashes** and
**15 frozen vocabularies**. Independent read-only review found no remaining
authority, funding-lifetime or retry blocker in these new seams.

The live server/SDK/CLI/MCP remain V1. Remaining work includes authenticated raw
transport with bounded receive buffers and complete ingress work accounting,
reference-policy/Handoff authority, broader integrated target/profile coverage,
native mutation/checkpoint codecs, recovery and entitlement reconstruction,
WAL/Ready and Session/quorum activation, binary/deployment qualification and
multi-region scale/failure testing. The managed plan API supplies no durable
acknowledgment or automatic agent execution.

## Raw native admission and recovery construction primitives — 2026-09-07

The [complete frame dispatcher](../../crates/focal-core/src/native/input_codec/ingress.rs)
now feeds all 28 actor commands into the managed native owner. One internal limit
set derives semantic bounds from existing node limits and tracks five cumulative
input-work domains through initial inspection, preparation and final construction.
The owner verifies the actual ledger/profile and authenticated actor header before
scanning the variable body. An opaque remaining-work marker prevents construction
from resetting any domain; exact pending/committed retries require the complete
actual intent but no unused final build headroom. A test exposed an artifact
inspection requirement for unused future source capacity; the internal frame path
now defers that requirement until fresh admission, preserving the standalone
artifact plan's stronger construction promise. Borrowed authority checks still
share the raw source counter and cannot consume the final build pass.

The [wire reader](../../crates/focal-wire/src/frame.rs) can now inspect a fixed
header and read one exact payload into caller-owned storage without allocating.
Typed compatibility reads/writes also reserve their frame buffers fallibly, and
the client preserves uncertain outcomes when a response allocation fails. Frozen
V1 framing is unchanged. These helpers do not negotiate native transport or fund
asynchronous receive buffers. The remaining receive/retry pools, credential/tenant
selection, queue/Quinn accounting and enclosing work contract are explicit in
[21 §§6.4–6.5](21-native-input-format.md#64-complete-frame-dispatch-and-cumulative-input-work).

Recovery construction now includes a
[partitioned range loader](../../crates/focal-memory/src/range_hydration.rs)
for non-Clone values built from borrowed row plans. It reserves each complete row
allowance before construction, reconciles actual owned capacity, bounds staging
to one chunk and discards the entire detached root on failure. Existing page/root
sharing is retained; no per-object Arc was introduced. The
[scalar validation snapshots](../../crates/focal-model/src/lifecycle/validation_snapshot.rs)
restore every evaluation/accepted-result field against the actual declaration
without allocation or replaying current participant authority. Intrinsic checks
preserve real fallback/quality cursors, historical results, seals/fences and
structural outcomes. They do not authenticate a checkpoint or establish complete
cross-row history. The recorded-write-set, remaining model hydration, native row
codec, detached-root checks and Session/WAL integration sequence is specified in
[18 §6.14](18-lifecycle-storage-upgrade.md#614-constructing-durable-native-state-without-cloning-or-re-executing-it).

The live server, SDK, CLI and MCP still select V1. No native restart, quorum
activation, binary publication or scale/deployment qualification is claimed by
this increment. Respondents still author their own success or failure testimony;
raw creation, claim receipt and recovery do not fabricate a testament.

Qualification on macOS arm64, 2026-09-07: the unfiltered affected-library runs
pass **1,492 tests** (714 Core, 448 model, 37 evidence, 103 memory, 137 client and
53 wire), with zero failed, ignored or filtered tests in the successful runs.
The wire suite initially encountered sandbox denials for local sockets; the full
53-test rerun with permission to bind loopback/Unix test servers passes. The
**24 new tests** cover all-command raw byte/intent parity, cumulative work bounds,
header rejection before body scans, held reporting and exact retries without
build headroom, authored raw creation without testimony, fragmented/truncated
frames, precharged non-Clone restoration and scalar lifecycle/history corruption.
Two existing client uncertainty tests additionally exercise allocation failure.
Strict workspace all-target Clippy, the production no-panic gate, formatting and
diff checks pass. Architecture checks verify **919 links**, all **37 imported
source hashes** and **15 frozen vocabularies**. Independent ingress review found
no concrete budget, authority-ordering, source-lifetime or exact-retry defect.
The full workspace test suite and other operating systems were not rerun.

## Native write sets and lifecycle restoration — 2026-09-07

Each real native candidate now retains its
[exact write set](../../crates/focal-core/src/native/mutation.rs), captured from
the storage plan's canonical inputs. This includes all 30 row families and
distinguishes deleted keys from present tombstones. Metadata, request outcomes,
events, identities and indices are preserved alongside domain rows. Values stay
in the candidate's one immutable root; capture neither clones those values nor
scans unrelated ledger rows. `NativeOwner::prepared_candidate` borrows the exact
unpublished ticket for the future durable writer. Publication refusal retains
the write set and its funding; commit and rollback release it at their actual
ownership boundary.

Evaluator and respondent completion envelopes now price a separate retained key
vector for each promised action. Construction workspace excludes it. Review
identified a peak gap in reserving the maximum failure shape during a smaller
regular report: capture now reserves the exact canonical count after storage
envelope validation, before allocating. The owned allocation moves into the
write set without an additional accounting-handle clone. This remains subject to
the same Ordinary or held source selected for the candidate.

Model restoration now covers claim state/history, scope registries and children,
registration membership, work/diagnostic/failure evidence, responses, audit
cohorts and claimant result testaments, alongside the prior scalar evaluation
and accepted-result restorers. Actual restored response bodies supply claim
history's private report stamps; actual declarations supply registration/audit
definition stamps. Original generation bindings come from retained history.
Broader reachable model states, including noncanonical starting revisions, zero
content where originally allowed and unstamped legacy seals, remain preserved;
the native importer enforces its original command profile separately.

Borrowed plans bound inspection and final construction, reconcile actual owned
capacities and reject changed sources. Response/work terminal causes must name
the correct response and required role; required claim response cuts must resolve
to the actual retained received response. Restored histories preserve errors,
retry exhaustion, programmatic-to-quality evidence, receipt adoption, independent
delivery, scope cancellation/release and original terminal cuts. Review also
corrected omitted summary-comparison work and preserved original generation
semantics before qualification. No production panic, per-object Arc, execution
worker or change to frozen V1 bytes is introduced by these APIs.

The remaining durable integration is in
[18 §6.14](18-lifecycle-storage-upgrade.md#614-constructing-durable-native-state-without-cloning-or-re-executing-it):
complete native row/record codecs and encoded-buffer funding, bounded phased
construction in an unpublished range, authenticated history and complete-root
validation, local custody restoration, entitlement reconstruction, Session/WAL
and quorum activation. Canonical key order is not model hydration dependency
order. The live server, SDK, CLI and MCP remain V1; this increment does not yet
provide native restart recovery or a native durable acknowledgment.

Qualification on macOS arm64, 2026-09-07: the unfiltered affected-library run
passes **1,333 tests** (719 Core, 474 model, 103 memory and 37 evidence), with
zero failed, ignored or filtered tests. The **31 new tests** cover exact write-set
ownership and funding through publication refusal, commit and rollback, plus
restoration of actual lifecycle histories, failures, retries and terminal causes.
They exercise bounded construction, allocation failure and changed-source
rejection. Strict workspace all-target Clippy, the production no-panic gate,
formatting and diff checks pass. Architecture checks verify **927 links**, all
**37 imported source hashes** and **15 frozen vocabularies**. The full workspace
test suite and other operating systems were not rerun. These checks qualify the
new component APIs; end-to-end native restart and activation remain unverified.

## Native record encoding and phased recovery construction — 2026-09-07

The [native mutation encoder](../../crates/focal-core/src/native/record_codec.rs)
now records complete values from the actual candidate's retained write set. It
includes all 30 row families: immutable descriptors, lifecycle state, responses
and failed-work evidence, receipts/cycles, independent evaluations and accepted
history, audit bundles, original events, counters, indices, identity mappings and
outcomes. Participant and all three timer namespaces stay disjoint. Explicit
stable tags, little-endian fields and checked widths avoid Rust enum/host-layout
coupling. Scalar ledger/head/cycle counters use u64 rather than collection sizes.
No frozen V1 bytes or existing content-hash preimages change.

Measurement and writing borrow the same immutable candidate. Exact byte/work
quotes include row sizing, bounded key lookups, snapshot/digest hashing and
collection iteration. The writer uses the caller's precharged destination and
allocates no buffer, row copy, offset array or additional shared owner. Review
added explicit iteration costs to shared descriptor writers; their byte streams
remain unchanged. The destination's pending WAL funding and its inclusion in
future completion promises remain integration work.

The outer inspector checks format/profile, sequence/ledger correspondence,
strict key ordering, row bounds, put/delete distinctions, mandatory metadata,
exact outcome correspondence, complete consumption and a domain-separated
BLAKE3 digest. Its iterator returns bounded borrowed bodies. These bodies remain
untrusted: a changed body with a recomputed digest can pass structural inspection
and still fail model/native validation. No API publishes an inspected record.
The original process-local range incarnation likewise requires a proven recovery
mapping, not equality with a new owner's identity.

The [complete-root checkpoint encoder](../../crates/focal-core/src/native/record_codec/checkpoint.rs)
uses the same body writers while streaming every retained row, including unchanged
content and original history. A separate `FCNROOTS` envelope, checksum domain and
u64 full-row count distinguish it from mutations. Genesis is an empty prefix-zero
root; nonzero roots require Meta and a retained outcome. Checkpoint rows cannot be
deletions. Encoding and structural scans acquire no snapshot, shared root handle,
whole-ledger staging array or row index. Session metadata, persistence funding and
complete native checkpoint decoding/validation remain separate requirements.
The callback writer additionally streams the same bytes through caller-funded
bounded buffering, avoiding a second encoded-root allocation. Original output
errors return by value without boxing or cloning. A failed stream leaves no
successful checkpoint result; callers discard its prefix and establish their own
flush/publication fences. Online scheduling from an accounted fixed-prefix pin
remains separate from the current borrowed-Core encoding plan.

Response and claimant result-testament owners retain their actual Generated
revision as one additional scalar. Delivery/posting/copying preserve it; recovery
must compare it with the original event. Work errors, production diagnostics,
failed artifacts and pure missing/delivery results remain separate retained facts.
Artifact bytes carry the immutable descriptor plus local tree address/revision;
the process-local custody token is never serialized or accepted as follower proof.

The [phased memory builder](../../crates/focal-memory/src/range_hydration_phased.rs)
constructs non-Clone rows in dependency order while keeping one unpublished owner.
Each phase has exact cardinality and strictly ordered, previously absent keys;
phases may differ from canonical key order. Scoped lookups include earlier phases
and earlier staged rows. Each complete final-row allowance is acquired before
building, actual capacity is checked before the next row, and only one bounded
chunk is staged. Any failure discards every phase and returns its funding. Final
cardinality and complete validation precede prefix binding and owner exposure.
Source/decoder workspace and callback visits still require enclosing accounting.

The [record-format contract](22-native-record-format.md) and [18 §6.14](18-lifecycle-storage-upgrade.md#614-constructing-durable-native-state-without-cloning-or-re-executing-it)
retain the next obligations: row-body decoders and native phased adapters,
complete cross-row/history validation, local custody and entitlement recovery,
checkpoint/mutation application, encoded-buffer funding, Session/WAL/Raft mapping
and replicated activation. The live server, SDK, CLI and MCP remain V1; these
components do not yet demonstrate native restart recovery or a native durable
acknowledgment.

Qualification of the encoding/phased-construction increment above, before the
subsequent decoder implementation: **1,367 affected-library tests passed**
(746 Core, 474 model, 110 memory and 37 evidence), with **34 new tests** and no
failed, ignored or filtered tests. Strict workspace all-target Clippy passed;
formatting completed. The last architecture pass checked **946 links**, all
**37 imported source hashes**, and **15 frozen vocabularies**. These results
apply to that earlier source increment, not the recovery work below.

## Native checkpoint restoration — 2026-09-07

The source now connects [complete row dispatch](../../crates/focal-core/src/native/record_codec/read_dispatch.rs)
to [native checkpoint recovery](../../crates/focal-core/src/native/record_codec/recovery.rs).
All 30 row families have explicit readers and checked construction paths.
Dependency-aware quotation precedes the engine's final allocation; mutable
descriptor plans remain scoped and are re-prepared from the same borrowed bytes
under cumulative work bounds. Eight internal phases restore immutable bodies,
actual evidence, independent evaluations/results, respondent testimony, complete
claims and frozen claimant audits without exposing an intermediate owner.

The implementation resolves the policy/response/claim dependency cycle with a
temporary funded acceptance policy derived from the original claim body and
actual declarations. A bounded paged index retains borrowed claim spans and
original artifact publication coordinates. Root validation uses a funded
sequence bitmap, logical-time array and scalar history index to check coverage
without a full event scan per object. Scratch indices drop before owner publication. Artifact
custody recovery reads and verifies existing local trees and schemas; it never
recreates missing content from inline descriptor bytes.

The complete-root validator checks retained handler retries/fallbacks, quality
transitions, exact evidence schemas and original result publications. Funded
model projections derive required outcomes chronologically, reject omitted or
substituted terminal decisions, and preserve earlier cuts when optional results
arrive later. Exact native cohort seals, receipt epochs, linked indices, authored
identity mappings and frozen audit membership are checked before publication.
Opaque parsers and model inspections borrow work allowances exclusively, so
nested callbacks cannot spend the same allowance twice.

Qualification on macOS arm64, 2026-09-07: **1,417 affected-library tests passed**
(787 Core, 476 model, 115 memory and 39 evidence), with **50 new tests** and zero
failed, ignored or filtered tests in the successful run. These cover explicit
failed testimony with error artifacts, delivery stages, independent validations,
cold evidence-store reopen, authored content and reused identities, malformed
state with recomputed checksums, absent local evidence, cumulative work limits,
memory refusal and refunds. A restored Core continues response delivery and
claimant audit generation/posting with the exact uninterrupted history, then
restores again. Qualification corrected original versus current claim revision
handling and distinguished declared missing-slot validation results from
structural missing-work causes. Strict workspace all-target Clippy, the
production no-panic gate and formatting pass. Architecture checks verify
**957 links**, all **37 imported source hashes** and **15 frozen vocabularies**.
The full workspace test suite and other operating systems were not rerun.
The functionality was connected before this consolidated validation pass;
subsequent reruns resolved its concrete failures.

This is Core checkpoint restoration. It does not supply incremental native
mutation replay, Session checkpoint provenance, WAL/Raft prefix mapping, decoder
activation, or live native CLI/MCP dispatch. The existing
`NativeOwner::with_schemas` reconstructs evaluator/respondent RAM credits;
connecting and qualifying that path after checkpoint recovery, and funding
durable completion buffers, remain service integration work. The final-state checkpoint also
cannot independently reconstruct every historical graph/root set; observable
witnesses are checked, and the enclosing service must establish the trusted
checkpoint/log chain. [22](22-native-record-format.md#native-checkpoint-restoration)
records the construction order, accounting requirements and remaining integration.

## Native incremental replay and restored-owner qualification — 2026-09-08

This source batch adds [incremental native replay](../../crates/focal-core/src/native/record_codec/replay.rs)
against an exact predecessor. It checks the ledger, profile, original range
mapping and adjacent sequence before construction. One funded canonical change
array masks changed and deleted base rows. Eight dependency phases construct
the successor with actual local evidence custody, original publication indices,
retained immutable policy borrowing and bounded changed-event lookups. It
prepares an unpublished candidate; the enclosing service must still establish
log provenance, persistence and publication barriers.

Replay validates original object histories, authoring identities, receipt and
response bindings, independent attempt progress, audit membership, linked
indices and derived acceptance. Graph consequences retain their exact capture
ordinal in the original event. A bounded scalar graph reconstruction checks
canonical dependency paths, SCC decisions, monitor eligibility and release
fingerprints at that boundary. Control batches preserve their complete original
root union, including disconnected successors. Validation rejects omitted
dependency and satisfaction consequences. This exposed a producer defect:
control propagation previously depended on monitor presence. Create, Cancel
and Post now settle ordinary dependency consequences independently of monitors.
A newly created claim that fails in the same transaction retains its actual
original empty registry before sealing, including the original binding and
exact seal cut. Legacy and authored creation share this construction path.

The dormant native record and checkpoint envelopes advance explicitly to
version 2 because prior native events lacked graph capture provenance. Readers
refuse dormant native version 1 instead of inferring a matching historical
graph. Frozen live V1 formats, hashes and decoder registration remain unchanged;
neither dormant native envelope is activated in the running service.

The [range builder](../../crates/focal-memory/src/range_preflight.rs) accepts the
already held exact input allowance from the same budget, category and lane.
Incoming payloads move without a second admission charge. Source inputs remain
funded through destruction on refusals; destination pages and workspaces retain
their separate charges. Replay still accounts for encoded-row staging and copies
only retained neighbors of touched pages. No per-object shared owner is added.

New [restored-owner scenarios](../../crates/focal-core/src/native/record_codec/recovery_owner_tests.rs)
connect actual checkpoint recovery to evaluator retries/quality checks and to
pending failed respondent testimony, diagnostic closure and posting under full
parent-memory pressure. They cover constructor refusal, retained credits, exact
request retries and refunds. These are Core/RAM-owner checks; they do not prove
WAL/Raft completion-buffer funding or native service restart.

Qualification on macOS arm64, 2026-09-08: **1,460 affected-library tests passed**
(822 Core, 480 model, 119 memory and 39 evidence), including **43 new tests**,
with no failed, ignored or filtered tests in the successful runs. The complete
functionality batch preceded consolidated verification; reruns addressed its
concrete integration failures. Tests cover exact replay parity and recheckpoint,
malformed records with recomputed checksums, original graph captures, full
control membership, corruption refusal, local custody, independent validation
progress, failed testimony, retained completion credits and memory refunds.
Strict workspace all-target Clippy, the production no-panic gate, formatting and
diff checks pass. Architecture checks verify **964 links**, all **37 imported
source hashes** and **15 frozen vocabularies**. The full workspace test suite,
Linux and other platforms were not rerun.

Native Session/WAL replay, checkpoint provenance, encoded-buffer funding,
quorum-prefix mapping and live CLI/MCP activation remain required before the
two-participant restart acceptance gate. Passing these component tests does not
qualify live native service recovery, regional deployment or throughput.

## Durable native Session, cluster harness and record-bound proof — 2026-09-08

This batch finishes the interrupted composition and delivers the R0–R2 packages
of [REMAINING](../REMAINING.md). The workspace compiles again: the allocator
overhead constant is public in `focal-memory`, the completion book carries its
record-buffer profile, and the ledger registers its native checkpoint module.
The single native decoder identity is the enclosing checkpoint descriptor hash,
which now names the input frames as well; `NativeSession` confirms exactly that
hash as the group's durable floor.

[NativeSession](../../crates/focal-ledger/src/native_session.rs) is the durable
native Session over the consensus replica. Its contract is recorded in
[22 §6](22-native-record-format.md#6-durable-session-contract): a committed
`FCNGENES1` genesis before any native mutation; `NativeCommit` carrying Raft
index and term next to the native sequence; membership merged by Raft index;
suffix disposition only on a matched head, a conflicting committed prefix, an
installed snapshot or an applied newer-term barrier; 16-byte read correlations;
an explicit retryable/authority/request/fail-closed error classification;
snapshot installation that rebuilds a complete replacement root under a derived
incarnation before replacing the domain; and startup that returns its recovered
events to the caller. Admission through typed input, borrowed `FCNINPUT1` frames
and trusted timers shares one path; timer namespaces are refused from frames.

Two defects surfaced by the three-node harness were fixed at their source. A
refused consensus staging reservation taken before any Raft state is touched no
longer marks the replica failed; it is retryable
([persistence.rs](../../crates/focal-consensus/src/persistence.rs)). Restored
replicas no longer draw a random producer range; the incarnation derives from
the attested genesis, node and install event, and the ledger crate lost its
`getrandom` dependency. `DurableNode` gained leader transfer, snapshot feedback
and free-space passthroughs, plus a `test-support` election pacing hook.

The R1 lifetime work is proven rather than argued. The future record bound in
[buffer.rs](../../crates/focal-core/src/native/record_codec/buffer.rs) is one
quote function; [bound_tests](../../crates/focal-core/src/native/record_codec/bound_tests.rs)
drives complete two-party workflows at authored maxima (largest metadata,
inputs, visibility labels and summaries, monitors, deadlines, adoption, child and
dependent claims, audit testaments, authored descriptors) and checks every changed
key of all thirty row families against its fixed allowance plus four bytes per
charged heap byte, every frame against the quote, and every completion promise
against the report it later funds. Record buffers and their permits are shown to
live exactly as long as their candidate, read leases keep only their own charge
once the owner is gone, extraction refuses without allocation while candidates
are pending, and consensus staging is released after a checkpoint finishes.
Admission promises are explicit in [18 §6.7](18-lifecycle-storage-upgrade.md#67-integrated-nativeowner-ram-completion-contract):
RAM for construction, the encoded record and retained pages, never disk, quorum
or fan-out; a free-space watermark on the WAL filesystem refuses fresh candidates
before any in-memory acknowledgement while exact retries are still answered.

Evidence on one disk-backed node: create, post, receipt, work and diagnostic
artifacts, authored success and failed testimony, posting, receipt of testimony,
whole-work entry, evaluator reports with derived acceptance, checkpoint, restart,
exact retry across restart and a new term; frames and typed inputs share one
identity; queue-slot, delivery-under-memory-pressure, refused-open and disk
watermark bounds each refuse without losing state. Evidence on three disk-backed
voters exchanging Raft messages in-process: follower replay equality of native
prefixes, producer ranges and claim state; leader loss with an unresolved
candidate discarded only by the newer-term barrier or a conflicting committed
prefix and its request key reusable afterwards; a commit before the reply
surviving the leader crash with the exact retry finding it; a lagging follower
installing the enclosing checkpoint, replaying the tail and taking authority by
planned handover; concurrent correlated read barriers; a follower without
evidence custody retaining the record until the content is imported through the
transfer API and applying it once; a follower under memory pressure keeping its
delivery until memory returns; corrupted record bytes in transit stopping only
the receiving replica.

Qualification on macOS arm64 (Darwin 25.4.0, Rust 1.94.1), 2026-09-08, on this
tree: `bash scripts/cargo.sh test --workspace --offline` passed **2,330 tests
across 85 test binaries** with no failures (822 Core library tests grew
to 828, 77 ledger to 91, 40 consensus to 43); strict
workspace all-target Clippy, the production no-panic gate, formatting and
`--locked` checks pass; architecture checks verify **981 links**, all
**37 imported source hashes** and **15 frozen vocabularies**. One node library
test with a 300 ms timeout flaked once under the full parallel run and passed
alone and on rerun; it is timing, not state. Linux, Windows and released
binaries were not exercised.

Still open before the first product gate: activating this Session inside the
running service next to the ancillary protocols with V1 import (R3), and native
operations through the service, CLI and MCP (R4).
## One Session with two engines: native activation, hosting and barriers — 2026-09-08

This batch delivers the R3 packages except populated-history import. The
durable native engine of the previous batch is now a component
([native_session_engine.rs](../../crates/focal-ledger/src/native_session_engine.rs))
that the standalone native session and the unified
[Session](../../crates/focal-ledger/src/session.rs) share: every consensus
interaction takes the replica explicitly, native replay and recovery read
evidence through a lock-free [ContentReader](../../crates/focal-evidence/src/store.rs)
over the node's content directory, and the exclusive writer keeps installation
and inline sealing. The Session keeps every ancillary protocol and adds a
committed `FOCALAC1` activation record, the `FOCALSS6` envelope carrying every
legacy section plus the native section, native routing in one Raft-ordered
apply loop with deliveries retained across retryable native refusals, readiness
promotion of the native owner, legacy refusals after activation, and a native
admission/read API. [23](23-native-activation-and-import.md) records the field
matrix and the protocol.

Activation is gated by durable promises: the managed baseline floor, the
recorded transition to the native decoder, and a support exchange that now
advertises the native descriptor so the leader proposes only when every voter
in both configuration sets has promised. Replicas fence native history at
ingress until their own transition is durable; a replica without hosting
refuses permanently and a downgraded binary cannot open a native ledger.
Membership additions and promotions require the native promise after
activation. Nodes host the engine at construction over `<data>/content`
([network_service.rs](../../crates/focal-node/src/network_service.rs),
[embedded.rs](../../crates/focal-node/src/embedded.rs)); the support driver
makes hosted replicas promise; `cluster replicas activate-native --session`
proposes activation through the replica admin protocol; replica diagnostics
report native hosting, readiness and activation.

Evidence in [session_native_tests.rs](../../crates/focal-ledger/src/session_native_tests.rs):
an empty ledger activates through the unified Session, runs the two-party
workflow natively, refuses legacy commands with a typed outcome, writes the SS6
checkpoint, restarts to the same prefix and answers an exact retry; a voter
without hosting blocks activation with a typed refusal and a downgraded replica
is refused at open while a rehosted one catches up; a lagging replica installs
the SS6 checkpoint with native state and takes authority by planned handover.
The standalone native suites, the record-bound proof and the V1 fixture corpus
keep passing unchanged.

Timing guards in the fleet evidence tests were widened to the 30-second export
bound so a loaded host cannot turn a causal check into a timeout; the ordering
assertions are unchanged.

Qualification on macOS arm64 (Darwin 25.4.0, Rust 1.94.1), 2026-09-08, on this
tree: `bash scripts/cargo.sh test --workspace --offline --no-fail-fast` passed
**2333 tests across 86 test binaries** (0 failed);
strict workspace all-target Clippy, the production no-panic gate, formatting
and `--locked` checks pass; architecture checks verify **991 links**, all
**37 imported source hashes** and **15 frozen vocabularies**. Linux, Windows
and released binaries were not exercised.

Still open in R3: import of populated legacy history (the `Imported` activation
kind is refused at apply until then), crash cuts at each durable boundary of
the transition, and resumed watches across it. R4 follows with native
operations through the service, CLI and MCP.

## Populated legacy history imports into native prefix one — 2026-09-08

This batch delivers the import path R3 left open. A legacy ledger with history
activates through the same `FOCALAC1` record (now schema 2, kind `Imported`):
the record carries the sealed legacy prefix, the trusted logical time, the
canonical inline chunking and the translation root; every replica translates
its own legacy core with the recorded-fact constructors recovery uses
([import.rs](../../crates/focal-core/src/native/import.rs)), writes one
`FCNROOTS2` image under a canonical incarnation, restores it through the
checkpoint recovery path and compares the root; a divergent replica fails
closed and nothing is transferred or rerun. The representation is frozen in
[23 §5](23-native-activation-and-import.md): claims keep their recorded status
history as `Imported` events with an explicit legacy origin and an empty
acceptance policy; testaments, evidence sets, validations and runs are retained
verbatim as frozen legacy rows (families 30–33); artifacts are re-verified from
local content under a derived import request key; receipts, monitors and graph
indices are native rows. Legacy claims refuse every native completion
operation; cancellation, expiry, supersession, scope and graph effects apply.
The enclosing checkpoint (`FCNSESS1` version 2) records the prefix that holds
no native record so the first record after an import binds to sequence one.

Hosts seal inline legacy payloads through the exclusive content writer
(`ContentHost::seal_import_inline`, canonical chunk size from the record). The
leader's admin path seals before proposing; a replica whose host has not sealed
retains the import delivery (`CustodyPending`, retryable) and reports
`native_import_pending`; the support driver seals and the next poll applies.
The replica worker no longer stops on a retained delivery, a not-yet-possible
shutdown checkpoint (genesis in flight) is skipped rather than reported as a
failure, and diagnostics gained `native_import_pending` and
`native_authoritative`.

Evidence: [import_tests.rs](../../crates/focal-core/src/native/import_tests.rs)
imports a populated legacy history (a satisfied claim with an inline artifact,
closed testament and run; a posted dependent with released and active monitors;
a cancelled, released claim; a superseded claim; a content-backed standalone
artifact), restores it, re-encodes the same rows, reproduces the root on a
second replica under another range, and refuses empty prefixes, missing
content, the synthetic broad corpus and a starved budget with typed outcomes;
[session_native_tests.rs](../../crates/focal-ledger/src/session_native_tests.rs)
imports on three replicas (followers seal through the host path), keeps legacy
exact-retry receipts resolving from the frozen prefix, refuses legacy commands,
continues native work to sequence two on every replica, checkpoints and
restarts with the imported prefix;
[fleet_import_tests.rs](../../crates/focal-node/src/fleet_import_tests.rs)
drives the node path: the worker hands payloads to the content host, an
unsealed proposal is refused with `CustodyPending`, and after sealing the
ledger becomes native and authoritative with no import pending. The V1 fixture
corpora keep their bytes and hashes.

The transition itself is qualified under crash cuts and across the ancillary
protocols in the same suite: the authority crashes right after proposing (the
record commits once or is superseded and proposed again, never applied twice),
a follower crashes with the record appended but unapplied and applies it after
restart, the authority crashes after applying activation and before genesis
commits (its shutdown checkpoint is skipped, the restarted authority commits
genesis and opens native admission), a replica whose host seals late retains
the import across its own restart and applies the same record once; a
protected watch registered before activation replays its legacy deltas from
its position without a resync, its cursor and receipts survive the transition,
a managed request committed before activation keeps resolving exactly, a late
legacy managed command is refused with a typed outcome, and all of it survives
the `FOCALSS6` checkpoint and a restart. Opening a session with a retained
import delivery is not an open failure; the host services it.

Two test-harness races surfaced under machine load while this batch was
qualified and are fixed with it. The network CLI tests chose loopback ports
from the operating system's ephemeral range and released them before the child
process bound them, so another process could take the port in between and the
node failed with "address already in use"; the helper now probes ports outside
that range, keeps a per-process registry so concurrently running tests never
share one, and probes both protocols
([cli_network.rs](../../crates/focal-node/tests/cli_network.rs)). The control
host budget test exhausted the memory budget from a single statistics snapshot
while the host's own tick could move bytes; it now reserves until a single byte
is refused ([control_host.rs](../../crates/focal-node/tests/control_host.rs)).
Three complete workspace runs of these sources before the qualifying run
reported one, three and one failures: the budget race above, one MCP stdio
watch test that received `resync_required` after a server restart under load
and passed alone twice
(`watch_stdio_retains_seed_and_events_until_explicit_consumption_across_restart`),
and the network cluster test failing at a node start with "address already in
use" (twice before the port change and once after it, each time passing alone
and in repeated runs of its own binary). The port change removes one collision
source but is not proven to be the whole cause; the helper now records the
holder of every socket in the test range when a start fails, so the next
occurrence names it. Both flaky tests are otherwise unchanged and remain under
observation.

Qualification on macOS arm64 (Darwin 25.4.0, Rust 1.94.1), 2026-09-08 15:23
CDT, on this tree: `bash scripts/cargo.sh test --workspace --offline --no-fail-fast` passed
**2,340 tests across 85 test binaries** (0 failed);
strict workspace all-target Clippy, the production no-panic gate, formatting
and `--locked` checks pass; architecture checks verify **1,003 links**, all
**37 imported source hashes** and **15 frozen vocabularies**. Linux, Windows
and released binaries were not exercised.

R3 closes with this batch. R4 follows with native operations through the
service, CLI and MCP; the first product gate (a two-party workflow through CLI
and MCP with restart) is its close condition.

## Native wire profile and node ingress — 2026-09-08

This batch opens R4 by carrying native operations across the wire and through
the node. Protocol version 4 ([native.rs](../../crates/focal-wire/src/native.rs))
registers `Native { frame }` (tag 25), `NativeRead` (26) and `NativeList` (27)
with their replies; a handler advertises it only when it admits managed,
participant and native requests, and the profile admits a closed operation set
(native operations, managed request streams, reconcile, summary, stream and
content transfer). Admissibility inspects only a frame's fixed header (magic,
format version, content profile, actor namespace, ledger, principal and request
identity bound to the authenticated envelope); node peers hold no native
capability. Mutation replies are the committed native receipt, a pending ticket
or a closed refusal (invalid input, unauthorized, not found, stale binding,
conflict, capacity, or a contract code mirroring the lifecycle contract errors).
Read documents mirror every committed native row family explicitly, from claims
with obligations, lineage, acceptance slots, scopes and authored content down to
events and frozen legacy bytes; the registered encodings of the three
operations are pinned by hash in the wire tests. The node projects committed
rows into those documents
([native_documents.rs](../../crates/focal-node/src/native_documents.rs)),
admits frames on the embedded owner with custody through its exclusive content
writer ([native_ingress.rs](../../crates/focal-node/src/native_ingress.rs)),
serves reads under every consistency mode with the native read barrier for
linearizable reads ([native_reads.rs](../../crates/focal-node/src/native_reads.rs)),
and on the replicated path proposes non-artifact frames, resolves pending
outcomes and linearizable reads from the session's committed events, and
refuses artifact-bearing frames until the content host proves custody for
them. Bounded lists, the validation context read, claim history expansion and
node-scheduled native timers are refused as unsupported until the index
families of the next batch exist; nothing is partially served.

Activation gained the pieces a laptop needs: `cluster replicas activate-native`
on a node without a network listener runs offline against the exclusive data
directory ([native_activation.rs](../../crates/focal-node/src/native_activation.rs)),
commits the record under the node's own authority, checkpoints and returns;
genesis activation (fleet admin or offline) now uses the authored content
profile so claims are authored natively, while a populated prefix is still
imported projection-only. The first node test exposed that a restored replica
reported the activation record's Raft index as a bound derived from the sealed
legacy prefix rather than the exact position; `FCNSESS1` version 3 retains the
index, the hosted engine records it when the record applies, the standalone
engine records its genesis entry, and restore refuses an index outside the
sealed prefix and the applied position ([22](22-native-record-format.md)).

Decisions recorded in [07](07-decisions-and-traceability.md): F16 (frames on
the wire), F17 (the planned input-codec extraction was surveyed and deferred
because the codec is coupled to admission helpers; `focal-client` and
`focal-mcp` use `focal-core` only in tests, so native documents stay in the
client while the document-to-frame compiler and native journal go into a
host-side crate), F18 (retained activation index; authored genesis profile).

Both servers negotiate the profile only when their handler admits native
requests, both clients offer it, the embedded transport gates it, and reply
validation checks native receipts, tickets, pages and cursors against the
request identity and bounds.

Evidence: [native_tests.rs](../../crates/focal-wire/src/native_tests.rs) covers
header inspection, admissibility against the authenticated envelope, tags,
capability and mutation class, profile gating, negotiation, request validation,
reply validation, frozen encodings and a local-socket round trip that reaches
a native-capable handler and is rejected at negotiation by one that is not;
[native_host_tests.rs](../../crates/focal-node/src/native_host_tests.rs) runs a
V1 embedded node that refuses the profile, activates it offline (idempotently),
admits a projection frame through the local host, returns the same receipt for
the identical frame, refuses a different intent under the same key as a
conflict, refuses a foreign principal and the legacy profile, commits a second
frame, reads the claim with its evaluations, an outcome, a missing claim and a
definition in one page, scans events, reports standing, refuses a stale exact
token and the unsupported list, then restarts with the prefix and activation
intact. The R3 activation, import, checkpoint and cluster suites pass on the
version 3 envelope.

Qualification on macOS arm64 (Darwin 25.4.0, Rust 1.94.1), 2026-09-08 16:27
CDT, on this tree: `bash scripts/cargo.sh test --workspace --offline --no-fail-fast`
passed **2,350 tests across 85 test binaries** (0 failed);
strict workspace all-target Clippy, the production no-panic gate, formatting
and `--locked` checks pass; architecture checks verify **1,019 links**, all
**37 imported source hashes** and **15 frozen vocabularies**. Linux, Windows
and released binaries were not exercised.

R4 continues with the coverage table, the client document and frame layer, and
the first product gate through the CLI.

## Native client documents, compiler and journal — 2026-09-08

This batch delivers the client side of R4: the authored surface through which
every host submits native operations as byte-identical frames. `focal-client`
gains the native documents and their descriptors at version 2 of the verbs
humans already use ([native_documents.rs](../../crates/focal-client/src/operations/native_documents.rs),
[native_catalog.rs](../../crates/focal-client/src/operations/native_catalog.rs)),
hand-written input schemas ([native_schema.rs](../../crates/focal-client/src/operations/native_schema.rs)),
the R4.0 coverage table as an exhaustive match over the owner's operations with
codec frame tags, actors, CLI paths, reads and exposure
([native_inventory.rs](../../crates/focal-client/src/operations/native_inventory.rs)),
the `n1:` operation journal that claims the request identity under the
canonical document, persists the exact frame with its fingerprint and minted
identities, and records only receipts bound to that frame
([native_store.rs](../../crates/focal-client/src/native_store.rs)), the
`submit_native`, `native_read` and `native_list` client calls with pending
tickets treated as uncertain writes, and the native refusal exit classes.
Descriptors now carry their wire profile and retry identity (F21). The new
`focal-native-client` crate compiles documents plus the bindings read from one
fixed prefix into `NativeInput`, encodes the frame and recomputes the owner's
intent fingerprint with the owner's own decoder
([compile.rs](../../crates/focal-native-client/src/compile.rs),
[resolve.rs](../../crates/focal-native-client/src/resolve.rs),
[frame.rs](../../crates/focal-native-client/src/frame.rs)). The wire evaluation
document gained the current attempt so a report can be compiled from a read.

Writing the compiler against the real owner surfaced four contract rules the
documents now respect explicitly and doc [21](21-native-input-format.md)
records: authored relations target committed claims of the same ledger (the
owner refuses a relation to an absent claim), a slot's missing-slot obligation
is a virtual declaration index no declaration uses, a closing testament cites
every diagnostic of its cycle in ascending artifact order, and beginning or
reporting work is admitted only through the managed owner's completion
contract, never through a direct core call. Decisions F20 and F21 are recorded
in [07](07-decisions-and-traceability.md).

Evidence: the compiler suite ([tests.rs](../../crates/focal-native-client/src/tests.rs))
runs the complete two-party workflow from documents through `NativeOwner`
frame ingress with real content custody — create, post, acquire the receipt,
submit work and a diagnostic, close the cycle citing both, post, receive,
begin and report the whole-work evaluation — and checks after every step that
identical documents and identities encode identical bytes, that the frame
header carries the request identity, and that the committed receipt's intent
equals the client's fingerprint; the claim ends `Satisfied`. It also pins the
creation refusals (missing delivery declaration, derived or non-claim
relations, check outside the declarations, empty handler list, phase and
target mismatch, self-work, duplicate scopes, missing parent), diagnostic
citation rules and content binding for failed testimony, and the mapping from
wire objects to bindings including current-evaluation selection across
generations and slots. `focal-client` tests cover the catalog (sorted,
version 2, every serialized field has a schema property, unknown fields
refused, V1 rows untouched), the coverage table (every frame tag 0–27 owned by
exactly one operation, exposed rows resolve to descriptors, the exact
`WireOnly` set), the journal (claim once, exact retry, receipt binding by
invocation and intent, intent and context conflicts, malformed expansions,
interrupted initialization at each durable step resuming without a second
identity, capacity and limit mismatch) and the client (a pending ticket is
resent as the identical frame until it commits, a ticket that never commits
is an unknown outcome with the request retained, refusals are final, reads
return pages, a handler without the engine refuses the profile before any
frame is seen).

Qualification on macOS arm64 (Darwin 25.4.0, Rust 1.94.1), 2026-09-08 18:03
CDT, on this tree: `bash scripts/cargo.sh test --workspace --offline --no-fail-fast`
passed **2,367 tests across 87 test binaries** (0 failed);
strict workspace all-target Clippy, the production no-panic gate, formatting
and `--locked` checks pass; architecture checks verify **1,041 links**, all
**37 imported source hashes** and **15 frozen vocabularies**. Linux, Windows
and released binaries were not exercised; no CLI or MCP host yet drives the
native path, which is the next batch.


## Native CLI verbs and the first product gate — 2026-09-08

This batch reaches the first product gate on the native engine through the
real binary (REMAINING §9 A1). The manual CLI probes the engine once per
invocation with a standing read under the native profile and, on a native
ledger, adapts every verb's flags or document into the native document,
reads the objects the compiler names, compiles and encodes the exact frame,
claims its `n1:` identity in the client journal and drives the frame to a
committed receipt, a closed refusal or a recovery command
([native.rs](../../crates/focal-node/src/cli/native.rs),
[native_documents.rs](../../crates/focal-node/src/cli/native_documents.rs)).
New verbs and flags: `artifact submit --slot`, `artifact diagnostic --reason`,
`testament submit --slot SLOT=ID:HASH --diagnostic ID:HASH`, `testament post`,
`validation begin --validation --slot`, `validation report --verdict`, and
`--parent`, `--max-responses`, `--slot-json` on `submit claim`; `request
retry|inspect --operation-id n1:…`, `request pending` rows for native
operations, `schema coverage`, `schema get NAME --native`, and an
engine-aware `status`. The client journal records reported refusals and a
delivery marker so a reply lost after the commit stays listed until the
recovery command reprints it. Results share the application result shape
at schema version 2 (`native`, `native_read`, `native_list`).

Driving the binary exposed three faults on the replicated host that the
embedded host never showed, each fixed at its cause and pinned by a test:
the replica's reply accounting had no native arm, so any native read page
larger than the fixed slack was replaced by an unknown outcome
([fleet.rs](../../crates/focal-node/src/fleet.rs)); artifact-bearing frames
were refused on the replicated path, so the data service now has the
exclusive content writer seal and verify the frame's inline payload under
the current custody placement and submits the frame with that evidence,
which the owner binds to the exact frame
([evidence_service.rs](../../crates/focal-node/src/evidence_service.rs),
[content_host.rs](../../crates/focal-node/src/content_host.rs), the codec's
`DecodedRequest::into_artifact`); and the standard native session limits
carried the default owner shapes, whose future record buffer exceeds the
4 MiB encoding envelope, so every completion-class admission on a node was
refused as capacity (decision F22 in [07](07-decisions-and-traceability.md);
`NativeSessionLimits::standard` now carries bounded shapes and
[native_session_workflow_tests.rs](../../crates/focal-ledger/src/native_session_workflow_tests.rs)
runs the complete cycle under exactly those limits).

Evidence: [cli_native_a1.rs](../../crates/focal-node/tests/cli_native_a1.rs)
runs the real binary: offline activation of a fresh data directory, a
network listener, `status` reporting the authored native standing, a second
participant enrolled over QUIC, the issuer's claim (delivery check plus a
programmatic slot check), post, the respondent's receipt, work artifact,
closing testament citing the artifact by identity and hash, post, the
issuer's receipt of the testament, begin and report of the evaluation,
`get claim` showing the derived `Satisfied` status and `get validation`
showing the validated evaluation, an exact retry printing the same receipt,
SIGKILL and restart with identical reads and the same retried receipt, a
post of the satisfied claim refused with the conflict exit class, and a
reply lost on a closed stdout recovered through `request pending` and the
printed recovery command. [fleet_native_tests.rs](../../crates/focal-node/src/fleet_native_tests.rs)
drives the replicated host in process (activation, standing, creation,
linearizable object reads, post and a second principal's receipt), and the
CLI unit tests check that flags and documents compile to identical native
intents, that V1-only fences are refused, and that every exposed coverage
row resolves to a leaf of the clap tree.

The first workspace run of this batch failed seven CLI binary tests: the
engine probe ran before local validation and journaling, so an unreachable
transport surfaced as a transport error where the V1 path reports an input
error or journals first, and a shared host serving a V1 ledger next to
native ones answers the probe with an operation refusal rather than the
protocol refusal an embedded V1 host gives. The probe now treats both
refusals as the V1 engine, and an unreachable transport as V1 only for a
context that has never journaled a native operation; a context that has
stays native and refuses to mint a V1 identity for a native ledger. The
seven tests and the native gate pass on the recorded tree.

Qualification on macOS arm64 (Darwin 25.4.0, Rust 1.94.1), 2026-09-08 18:53
CDT, on this tree: `bash scripts/cargo.sh test --workspace --offline --no-fail-fast`
passed **2,372 tests across 88 test binaries** (0 failed);
strict workspace all-target Clippy, the production no-panic gate, formatting
and `--locked` checks pass; architecture checks verify **1,059 links**, all
**37 imported source hashes** and **15 frozen vocabularies**. Linux, Windows
and released binaries were not exercised. Known limits carried into the next
batch: multi-node custody of native inline payloads is not yet replicated to
followers, native claim creation on a projection-only (imported) ledger is
refused because imported prefixes carry no authored profile, and the MCP
server does not yet expose the native catalog (R4.4).

## Native MCP tools and the first product gate through the adapter — 2026-09-08

This batch closes the MCP half of the first product gate (REMAINING §9 A1).
The adapter decides its catalogue once per connection: `serve` now creates
the worker runtime before anything else, runs the engine probe on it (a
remote transport binds its endpoint to the first runtime that drives it),
and on a native ledger opens the adapter's own `n1:` journal
(`client/mcp-native`) and serves the native catalogue
([catalog_native.rs](../../crates/focal-mcp/src/catalog_native.rs),
[native_backend.rs](../../crates/focal-mcp/src/native_backend.rs)): the
native descriptors the standing permits, five exact reads (`claim.get`,
`testament.get`, `artifact.get`, `validation.get`, `ledger.standing`, added
to the native descriptor table at version 2), and `request.inspect`,
`request.retry`, `request.pending` and `request.acknowledge` over `n1:`
references, with output schemas under `urn:focal:mcp:NAME:output:2`. Native
mutations take an optional `n1:` reference, journal the exact frame before
the send, return `native` receipts, typed `native_refused` refusals or an
unknown outcome, and stay listed by `request.pending` until acknowledged;
`request.inspect` with `remote: true` and the CLI's `request inspect
--operation-id n1:… --remote` read the owner's committed outcome by request
key so each adapter observes the other's operations (decision F23 in
[07](07-decisions-and-traceability.md)).

The CLI's native path moved into one shared driver
([driver.rs](../../crates/focal-native-client/src/driver.rs)): resolve the
compiler's requirements with the host's blocking read, compile, encode,
fingerprint, claim the identity in the journal and plan the exact reads, so
flags, documents and MCP tools compile byte-identical frames through one
function. The client gained the engine probe (`Client::native_standing`,
which treats a protocol refusal from an embedded V1 host and an operation
refusal from a shared host serving a V1 ledger alike) and the
`native_refused` output kind; native refusals count as error results the way
V1 domain refusals do.

Evidence: [mcp_native_a1.rs](../../crates/focal-node/tests/mcp_native_a1.rs)
runs the real binary with two `mcp serve` processes, the issuer on the local
socket and the respondent on an enrolled QUIC client context: the native
catalogue is discovered by both, the complete cycle (claim, post, receipt,
work artifact, testament, post, receive, begin, report) runs through native
tools to the derived `Satisfied` status and the validated evaluation, the
node is killed and restarted with both adapters still connected, reads and
journaled receipts are unchanged, the owner's outcome is observed through
the adapter and the CLI, a stale post is a typed refusal that leaves the
pending list, acknowledgment retires results idempotently, an adapter killed
after a commit is replaced by a fresh one that lists and replays the exact
frame, the same reference with the same input resumes, and different input
under the same reference is refused as a conflict. In-crate stdio tests
([native_stdio_tests.rs](../../crates/focal-mcp/src/native_stdio_tests.rs))
drive the probe, catalogue, journaling, pending tickets, refusals, reads and
restart with a controlled transport, and pin that a V1 answer or an
unreachable owner keeps the V1 catalogue unless a native journal exists.
Catalogue tests pin the exact native tool set, the version 2 identities and
the projection-only filter.

Qualification on macOS arm64 (Darwin 25.4.0, Rust 1.94.1), 2026-09-08 19:20
CDT, on this tree: `bash scripts/cargo.sh test --workspace --offline --no-fail-fast`
passed **2,377 tests across 89 test binaries** (0 failed);
strict workspace all-target Clippy, the production no-panic gate, formatting
and `--locked` checks pass; architecture checks verify **1,072 links**, all
**37 imported source hashes** and **15 frozen vocabularies**. Linux, Windows,
released binaries and external MCP clients were not exercised. Known limits
carried forward: the chunked upload, download, watch, list, wait, monitor,
summary and validation-context tools are withheld on native ledgers until
the native index families and content transfer (R4.5); the packaged skills
still describe the V1 tools (R4.10); follower custody of native inline
payloads is not replicated; projection-only imported ledgers cannot author
claims.

## Native index families and bounded lists — 2026-09-08

This batch gives the native engine its secondary indexes and the bounded
lists they serve (R4.5 Batch D; [22 §7](22-native-record-format.md), decision
F24 in [07](07-decisions-and-traceability.md)). Thirteen index families
(record tags 34–46) are unit rows of the same range, derived from exactly one
primary row each by one shared function
([index_rows.rs](../../crates/focal-core/src/native/index_rows.rs)): the
leader derives them from the exact rows a plan writes
([original_plan.rs](../../crates/focal-core/src/native/original_plan.rs)),
replay validation rederives every put and delete and requires the record to
carry exactly those changes
([replay_validate_index.rs](../../crates/focal-core/src/native/record_codec/replay_validate_index.rs)),
checkpoint validation rederives every retained row and requires every
primary row to be covered
([read_validate_index.rs](../../crates/focal-core/src/native/record_codec/read_validate_index.rs)),
and the import image writes the same rows for translated claims and
artifacts. `FCMUTATE` and `FCNROOTS` advance to version 3 (the decoder
identity follows). Only the status family is ever deleted; the count
validator admits exactly that deletion. Scans are bounded iterators over one
key range ([index_scan.rs](../../crates/focal-core/src/native/index_scan.rs)).

Funding is exact rather than assumed: every construction ceiling carries
its possible index rows (`max_index_rows`), completion and respondent
promises quote them in their write envelopes, record buffers and slot
demands, and a promised artifact's inputs are bounded by the new
`NativeLimits::artifact_inputs` (16) and narrowed by a small batch
(`cap_inputs`) rather than refusing every report. Two accounting defects
this exposed are fixed at their source: the range write envelope priced
every deletion as the largest entry the range admits (a status move cost
megabytes), so `RangeWriteLimits` gains `deleted_heap` and the plan check
verifies the actual deleted heap
([range_envelope.rs](../../crates/focal-memory/src/range_envelope.rs)); and
range reconstruction charged each neighbour copy at the maximum entry size,
so restore and replay now charge the copied row's own footprint
([recovery.rs](../../crates/focal-core/src/native/record_codec/recovery.rs)).
The standard session record envelope grows to 6 MiB so a preparation-sized
body at the codec's fourfold expansion plus every fixed row still fits; the
respondent's diagnostic was a few kilobytes over the old 4 MiB.

Lists are stateless on the node
([native_lists.rs](../../crates/focal-node/src/native_lists.rs), host and
fleet dispatch): the filter selects one indexed predicate, the rest filter
residually within `max_visits`, the continuation names the last visited row
(an empty page may continue; only an absent cursor ends the list), and a
cursor carries a keyed BLAKE3 digest over the ledger, principal, route epoch
and exact filter under a per-incarnation key, so tampering, reuse under
another filter or principal, and a node restart are refused. The client
gains eight version 2 list descriptors and documents (`claim.list`,
`artifact.list`, `validation.list`, `evaluation.list`, `testament.list`,
`receipt.list`, `monitor.list`, `event.list`; 24 native descriptors in
all), the shared driver translates them into wire filters
([driver.rs](../../crates/focal-native-client/src/driver.rs)), `focal list`
serves all eight families on a native ledger with the shared flags plus
`--validation`, `--verdict`, `--holder` and `--after` (refusing flags a
family does not index, and refused on V1), and the MCP adapter offers the
eight tools with `native_list` results. Evidence:
[cli_native_lists.rs](../../crates/focal-node/tests/cli_native_lists.rs)
drives the real binary through every family and filter, paging, the
residual-filtered empty page, `--all`, a tampered cursor, a cursor reused
under another filter and family, V1-only flags, the MCP list tools and a
restart that retires the previous cursors; core, memory, ledger, client and
MCP suites were retuned to the exact new row shapes.

Qualification on macOS arm64 (Darwin 25.4.0, Rust 1.94.1), 2026-09-08 21:13
CDT, on this tree: `bash scripts/cargo.sh test --workspace --offline --no-fail-fast`
ran **2,378 tests across 90 test binaries**; 2,377 passed and one failed in
the parallel run: `fleet::evidence_tests::checkpoint_waits_for_exact_writer_fence_while_another_writer_commits_and_reopens`
(its 300 ms fence timeout under a full parallel workspace run), which passed
three consecutive times in isolation (`--lib` filter) immediately after. The
preceding run of the same tree at 21:07 CDT (before a formatting-only change
to `cli_native_lists.rs`) passed that test and instead timed out
`fleet_tests::reconciliation_tests::receipt_reads_require_live_quorum_preserve_privacy_and_recover_after_leader_restart`
once, which likewise passed three times alone; both are timing-sensitive
fleet tests, not regressions of this batch. Strict workspace all-target
Clippy, the production no-panic gate, formatting and `--locked` checks pass;
architecture checks verify **1,103 links**, all **37 imported source hashes**
and **15 frozen vocabularies**. Linux, Windows, released binaries and
external MCP clients were not exercised. Known limits carried forward: the
sixteen wire-only native verbs, timers, the validation context read, deltas
and watches, chunked transfer and the packaged skills remain open on the
native engine (R4.5 Batches E–G, R4.10); follower custody of native inline
payloads is not replicated; projection-only imported ledgers cannot author
claims.

## The remaining native verbs through CLI and MCP — 2026-09-08

This batch closes the authored gap of the R4.0 coverage table (R4.5 Batch
E): the sixteen operations that were `WireOnly` are authored tools, and the
table's test now asserts that set is empty
([native_inventory.rs](../../crates/focal-client/src/operations/native_inventory.rs),
[native_tests.rs](../../crates/focal-client/src/operations/native_tests.rs)).
Twelve descriptors are new (`claim.release_scope`, `receipt.adopt`,
`artifact.fail`, `artifact.receive`, `artifact.reject`,
`validation.seal_increments`, `validation.enter_whole_work`,
`audit.generate`, `audit.post`, `monitor.register`, `monitor.rebind`,
`monitor.cancel`; 36 native descriptors in all) with strict documents and
schemas ([native_documents.rs](../../crates/focal-client/src/operations/native_documents.rs),
[native_catalog.rs](../../crates/focal-client/src/operations/native_catalog.rs),
[native_schema.rs](../../crates/focal-client/src/operations/native_schema.rs));
the four admission and increment evaluation kinds share the existing
`validation.begin` and `validation.report` descriptors, whose documents gain
`phase` (`whole_work` by default, `admission`, `increment`) and `target`
(the increment's work artifact), so one verb selects the current evaluation
of any phase and the compiler emits the matching owner command.

The shared compiler binds every verb to exactly the committed objects it
names ([resolve.rs](../../crates/focal-native-client/src/resolve.rs),
[compile.rs](../../crates/focal-native-client/src/compile.rs)): a rejection
reads the work artifact's binding, receipt and cycle and the descriptor
whose visibility the diagnostic inherits, then authors the
`ReceiptRejection` diagnostic under the issuer with the builtin error
schema; a failed slot reads the holder's committed production diagnostic
(refusing any other reason, and a pinned hash that differs); adoption fences
the claim's current receipt and mints the next one; monitors carry the
claim's current fence and parse their wait predicates and deadline;
rebinding names both claims; audits mint the result testament identity and
post it by its committed binding. Nothing is derived from wall-clock time and
every minted identity is journaled with the frame as before. The CLI gains
`claim release-scope`, `receipt adopt`, `artifact fail|receive|reject`,
`validation seal-increments|enter-whole-work`, the `audit generate|post`
root (the one addition to the root list of
[13](13-cli-and-agent-implementation-plan.md)), `monitor rebind|cancel`,
native `monitor register` on the existing flags, and `--phase`/`--target` on
`validation begin|report`; each adapts flags and documents identically
([native_documents.rs](../../crates/focal-node/src/cli/native_documents.rs),
[monitor.rs](../../crates/focal-node/src/cli/monitor.rs)) and is refused by
name on the V1 engine. The MCP adapter serves the twelve tools from the
descriptors with no adapter change beyond the listing test.

Evidence: [cli_native_a3.rs](../../crates/focal-node/tests/cli_native_a3.rs)
drives the real binary with two enrolled participants through an admission
check selected by phase (and refused under the default phase), an increment
check of the submitted artifact, work receipt, sealing (with an exact retry),
the testament cycle, explicit whole-work entry, the slot check, scope release
(refused a second time), audit generation and posting; a second claim
through a rejected work product (a `work` reason refused as invalid input,
the diagnostic inheriting `team` visibility), a production diagnostic and
the failed slot (a work diagnostic refused), adoption by the issuer (epoch
two, two receipts listed, the respondent's late testimony refused); a third
claim's monitor registered over a satisfied root, rebound after a committed
supersession (an unrelated successor refused), cancelled only once the owner
is cancelled; then a kill and restart with identical claim and monitor reads
and an exact retry of the audit posting. Unit coverage: the twelve verbs'
read requirements, missing-object refusals, adoption and monitor compilation
and their input refusals, plus release, audit generation and posting through
the owner ([tests.rs](../../crates/focal-native-client/src/tests.rs)); flag
and document parity for every new verb
([cli/tests.rs](../../crates/focal-node/src/cli/tests.rs)); the MCP A1 test
asserts the twelve tools are listed.

Qualification on macOS arm64 (Darwin 25.4.0, Rust 1.94.1), 2026-09-08 21:48–21:53
CDT, on this tree: `bash scripts/cargo.sh test --workspace --offline --no-fail-fast`
ran **2,381 tests across 91 test binaries with 0 failures**; strict workspace
all-target Clippy (`-D warnings`), the production no-panic gate, `cargo fmt
--all --check` and `cargo check --workspace --all-targets --locked --offline`
pass; `scripts/check-contracts.py` verifies **1,118 links**, all **37 imported
source hashes** and **15 frozen vocabularies**. The preceding run of this
batch (21:40 CDT, before the last two edits) failed two tests under the
full parallel load: the fleet fence test recorded as flaky in the previous
two batches expired its fixture's 500 ms `request_timeout` on the other
ledger's commit, so the fixture bound is now 2 s and the stop-deadline test
scales with it ([fleet_evidence_tests.rs](../../crates/focal-node/src/fleet_evidence_tests.rs);
the product default is 5 s), and the V1 MCP stdio test's 5 s process-exit
bound after EOF was exceeded once (its adapter shutdown bound is 3 s); both
passed alone and in this recorded run, and the stdio bound is left as a
known load sensitivity rather than widened. Linux, Windows, released binaries
and external MCP clients were not exercised. Known limits carried forward:
the trusted timers (claim, evaluation and monitor deadlines), the validation
context read, deltas and watches, chunked transfer and the packaged skills
remain open on the native engine (R4.5 Batches F–G, R4.10); follower custody
of native inline payloads is not replicated; projection-only imported
ledgers cannot author claims; `list testaments` scans the response chain
and the result testament is read by `get testament`.

## Trusted timers: the due-timer index and the node's sweep — 2026-09-08

This batch makes the three trusted timers fire (R4.5 Batch F, first part;
decision F25 in [07](07-decisions-and-traceability.md)). The native store
gains a fourteenth index family, `DueTimer` (record tag 47;
[22 §7](22-native-record-format.md)): one unit row per undelivered claim,
monitor and evaluation deadline, keyed by logical time then target, derived
by the same shared function as every other index row
([index_rows.rs](../../crates/focal-core/src/native/index_rows.rs)). A row
exists exactly while the timer has not been delivered and its target is
still live: a claim's deadline until its timer's outcome row exists (terminal
transitions leave it, so ordinary transitions never pay for claim timers and
the timer fires once on the terminal claim); a monitor's deadline while the
scope is active and undelivered; an evaluation's declaration deadline while
the evaluation is neither terminal nor fenced and undelivered. Consumption
is the retained outcome row of the timer's invocation, so the leader, replay
validation
([replay_validate_index.rs](../../crates/focal-core/src/native/record_codec/replay_validate_index.rs),
which now also requires the consumption delete of a delivered timer whose
target row did not change) and checkpoint validation
([read_validate_index.rs](../../crates/focal-core/src/native/record_codec/read_validate_index.rs),
which now covers every registered evaluation) all derive the same rows;
cohort seals derive the sealed evaluations' rows from the sealed states.
`FCMUTATE` and `FCNROOTS` advance to version 4.

Funding is per operation rather than a blanket ceiling: creation pays one
timer per claim, registrations one per new evaluation, a monitor
registration one, a report its own evaluation's plus every sealed cohort
evaluation's and every graph consequence's (added where the cohort and graph
are known), the three timers their own consumption; every other operation
is bounded by its extras and events
([prepare_budget.rs](../../crates/focal-core/src/native/prepare_budget.rs)
`max_timer_rows`, `timer_bound`). Completion, work and admission-graph
promises quote the same allowance in their fixed rows, write envelopes and
count checks, and the begin/report write envelope admits timer deletions
beside status moves, so a promise is never smaller than the construction it
must fund. The scan gains `Due { through }`
([index_scan.rs](../../crates/focal-core/src/native/index_scan.rs)).

The node sweeps the due rows each tick
([native_timers.rs](../../crates/focal-node/src/native_timers.rs)): the
embedded host from its one-second maintenance, the fleet's replica owner from
its tick on the leader only. A sweep reads at most sixty-four due rows at the
committed prefix, reads each timer's identity (timer and generation) from
its primary row, and delivers through `Session::deliver_native_timer` with
the node's logical clock (never behind the last committed record); a
redelivered timer is an exact retry that resolves to its recorded outcome, a
deferrable refusal (capacity, readiness, consensus) ends the sweep, a
contract refusal is counted, and a restart needs no memory because the next
sweep rescans the index.

Evidence: [due_timer_tests.rs](../../crates/focal-core/src/native/due_timer_tests.rs)
(rows at creation and registration, the not-yet-due refusal, delivery
retiring the row, the exact redelivery finding the outcome, a live claim's
expiry, a monitor's registration and delivery); the replay, recovery,
import, envelope, budget and response-shape suites retuned to the new row
counts; and [cli_native_a3.rs](../../crates/focal-node/tests/cli_native_a3.rs)
now posts a claim with a deadline a few seconds ahead and a monitor with its
own deadline through the real binary, observes the claim expire from the
node's clock, the monitor's timer consumed on the terminal owner without a
fabricated release, a later post refused as stale, the terminal status
surviving a cancellation attempt, and the expiry surviving a kill and
restart.

Qualification on macOS arm64 (Darwin 25.4.0, Rust 1.94.1), 2026-09-08 22:47–22:53
CDT, on this tree: `bash scripts/cargo.sh test --workspace --offline --no-fail-fast`
ran **2,383 tests across 91 test binaries with 0 failures**; strict workspace
all-target Clippy (`-D warnings`), the production no-panic gate, `cargo fmt
--all --check` and `cargo check --workspace --all-targets --locked --offline`
pass; `scripts/check-contracts.py` verifies **1,136 links**, all **37 imported
source hashes** and **15 frozen vocabularies**. One node library test
(`fleet::async_tests::shared_owner_queues_covering_flush_and_serves_another_group_while_disk_waits`)
failed once in an earlier partial run of this batch under parallel load and
passed twice alone and in this recorded run; it is timing-sensitive like the
fleet fence test of the previous batch. Linux, Windows, released binaries
and external MCP clients were not exercised. Known limits carried forward:
the validation context read, deltas and watches, chunked transfer and the
packaged skills remain open on the native engine (R4.5 Batch F second part,
Batch G, R4.10); follower custody of native inline payloads is not
replicated; projection-only imported ledgers cannot author claims; a claim
whose timer is delivered on a terminal claim records only its outcome, so a
terminal claim's timer row lives until its deadline passes.

## The evaluator's validation context read — 2026-09-08

This batch serves the composed read an evaluator needs before it runs
anything (R4.5 Batch F, second part; [19 §4](19-cli-mcp-implementation.md)).
`NativeReadQuery::ValidationContext` is answered from one committed prefix
([native_reads.rs](../../crates/focal-node/src/native_reads.rs)
`validation_context`): the claim, the definition, the registration selected
among the declaration's registrations (by family — admission, increment or
whole work — and optionally the exact target or generation, else the highest
generation of that family), that registration's evaluation, its target with
the manifest it covers (every slot of the response for a delivery or slot
check, the one artifact of an increment) and each artifact's verified
custody, the accepted results after an optional revision cursor bounded by
the request, and the delivery result of the same response. The registration
reports why it is not eligible (missing, terminal, fenced, custody), and the
delivery result has its own wire shape (`NativeDeliveryOutcome`) because the
owner records the issuer's acknowledgment without any handler attempt; the
query gains `kind` so a family with no registration reports `Missing`
instead of silently selecting another. Nothing is fabricated: absent rows
are absent, and no validator runs.

The client gains the `validation.context` descriptor and document
(`validation`, `phase`, `slot`, `target`, `generation`, `results_after`,
`limit`; 37 native descriptors), the driver selects the evaluation exactly as
`validation.begin` does over the definition's evaluations page and reads the
context at a prefix no older than that page
([driver.rs](../../crates/focal-native-client/src/driver.rs)), the CLI's
`get validation ID --context` takes `--phase`, `--slot`, `--target`,
`--generation`, `--limit` and `--cursor REVISION` on the native engine, and
the MCP adapter serves the tool from the descriptor. Evidence:
[cli_native_a3.rs](../../crates/focal-node/tests/cli_native_a3.rs) reads the
context of the satisfied claim's slot check (registration sealed and no
longer eligible, evaluation validated, manifest slot zero with the submitted
artifact in verified custody, the accepted result and the delivery result),
the admission check by phase (no manifest, one result) and an increment
context the definition never had (`Missing`, no evaluation); the MCP A1 test
lists the tool; the client's descriptor suite checks the document against
its schema.

Qualification on macOS arm64 (Darwin 25.4.0, Rust 1.94.1), 2026-09-08 23:28–23:34
CDT, on this tree: `bash scripts/cargo.sh test --workspace --offline --no-fail-fast`
ran **2,383 tests across 92 test binaries with 0 failures**; strict workspace
all-target Clippy (`-D warnings`), the production no-panic gate, `cargo fmt
--all --check` and `cargo check --workspace --all-targets --locked --offline`
pass; `scripts/check-contracts.py` verifies **1,141 links**, all **37 imported
source hashes** and **15 frozen vocabularies**. Linux, Windows, released
binaries and external MCP clients were not exercised. Known limits carried
forward: deltas and watches, chunked transfer and the packaged skills remain
open on the native engine (R4.5 Batch F second part, Batch G, R4.10);
follower custody of native inline payloads is not replicated; projection-only
imported ledgers cannot author claims; a claim whose timer is delivered on a
terminal claim records only its outcome, so a terminal claim's timer row
lives until its deadline passes.

## Deltas and watches on the native engine — 2026-09-08

This batch closes the last R4.5 Batch F item: durable watches observe a
native ledger through the same cursor protocol, and the deltas they receive
are derived from the committed native records themselves
([07 F26](07-decisions-and-traceability.md),
[23 §6](23-native-activation-and-import.md)). The event vocabulary the native
wire profile already carried (twenty-two record types from
`NativeEvaluationTarget` to `NativeEventRecord`) moved unchanged into
[`focal_model::native_event`](../../crates/focal-model/src/native_event.rs)
and is re-exported by the wire crate, so the ledger builds deltas without a
wire dependency and the frozen native reply bytes are untouched. `DeltaFact`
gains its twelfth variant, `Native(Box<NativeEventRecord>)`, under delta
schema 2 (`NATIVE_DELTA_SCHEMA`); the frozen durable V1 codec refuses it
([outputs.rs](../../crates/focal-model/src/durable_v1/outputs.rs)), so no
legacy delta tail can ever hold one. The projection of a committed
`NativeEvent` into its record, its nearest legacy lifecycle action (evaluation
facts through their state, work and response facts through theirs, imported
status facts exactly as the legacy engine mapped them), its actor (the
request principal, or the zero participant for trusted timers and the
import) and the claim it concerns (a registered artifact belongs to none)
lives once in the core
([event_record.rs](../../crates/focal-core/src/native/event_record.rs)); the
node's read documents delegate to the same functions, so the `events` read
and the delta stream carry one shape.

The Session keeps one continuous stream sequence line
([native_deltas.rs](../../crates/focal-ledger/src/native_deltas.rs)): the
sealed legacy prefix `0..=N` followed by the native records, native record
`s` of a genesis ledger being stream sequence `s` and record `s > 1` of an
imported ledger `N + s - 1` (native sequence one is the import image and
emits nothing). `stream_published` is the end of that line, `stream_bounds`,
replay validation and the registry's position checks use it, while cursor
envelopes and receipts keep naming the sealed domain sequence, so the leader's
candidate and every follower's apply still agree. A replay walks the retained
legacy tail and then the committed `Key::Event` rows, building each schema-2
delta on demand under the same item, byte and sequence limits (a first delta
that cannot fit is a capacity refusal, never a skip); a delta position past
the prefix is valid exactly when that event exists. Nothing new is persisted:
events are retained with the prefix, so native history never expires before
the retention floor, and checkpoints and restarts derive the same deltas.

The node ([streams.rs](../../crates/focal-node/src/streams.rs)) admits stream
requests on a native ledger only under the native wire profile (the proof the
consumer decodes schema 2; older profiles are refused before any cursor is
registered, and the managed request identity accepts that profile for the
managed operations it admits), takes the native read barrier as the stream
prefix, pins a seeded watch's snapshot at that prefix without a server-side
scan (the native engine keeps no historical read snapshot), reports the
published end of the stream line in the reply token and returns no tail while
a cursor is seeding. Cursor receipts, legacy and managed, name the published
end of the line when their entry applies (`apply_cursor_entry`,
`PreparedStream::set_cursor_sequence`), computed identically on every replica,
so the frozen receipt validators still bound every position a record names;
the checkpoint restore validates restored cursors and receipts against the
same line, rebuilding the native engine first. The stream subscription admits
schema-2 deltas exactly when they carry a native fact
([subscription.rs](../../crates/focal-stream/src/subscription.rs)). The client
journal ([watch.rs](../../crates/focal-client/src/watch.rs),
[journal.rs](../../crates/focal-client/src/watch/journal.rs), record
`FCLWAT02`, schema 2) saves the engine with the watch's options and, on the
native engine, reads its own seed through linearizable native reads after the
snapshot is pinned: a claim filter reads each claim with its responses and
evaluations, an unfiltered watch lists the family (claims for `claims`,
`testaments` and `all`, artifacts, definitions) in bounded pages, delivering
`NativeSeed` pages (objects, prefix token, next step) before completing the
seed and following the tail; family selection classifies native facts and
objects. `focal watch` routes to the same commands on a native ledger and
prints native changes compactly in tables; the MCP adapter offers the four
`watch.*` tools next to the version-2 catalogue and creates native watches.

Evidence: [event_record_tests.rs](../../crates/focal-core/src/native/event_record_tests.rs)
(record, action, actor and claim of claim, timer, import, artifact, work,
evaluation, result and monitor facts);
[session_native_tests.rs](../../crates/focal-ledger/src/session_native_tests.rs)
(a genesis ledger streams its records as schema-2 deltas on its native
sequence, resumption after an exact delta, refusal of a delta position naming
no event and of a position ahead, one-item replays walking the same deltas,
a native position acknowledged by the registry, identical deltas after
checkpoint and restart; the imported ledger continues past its sealed prefix
at `N + s - 1` with the legacy tail replayed first);
[native_host_tests.rs](../../crates/focal-node/src/native_host_tests.rs)
(legacy profile refused at the door, native profile admitted);
[watch_client.rs](../../crates/focal-node/tests/watch_client.rs) (the client
journal over an in-process native node: the seeded claim watch reads the
claim with its evaluations and completes, the seed's facts are not replayed,
another claim's creation stays out of the claim filter, an unseeded watch
replays both records as schema-2 deltas with actor and claim, a family watch
seeds through the definitions list, and every watch resumes after the host
restarts);
[cli_native_watch.rs](../../crates/focal-node/tests/cli_native_watch.rs)
(seeded claim watch with its evaluations, the cancellation arriving as a
schema-2 delta, an unseeded replay of the whole history in order, family
seeds through lists, a second participant's watch, table output, and
resumption after the node is killed and restarted); the MCP A1 test opens,
acknowledges and polls a native watch through the adapter.

Limits: objects committed between the pinned snapshot and the seed's read
prefix arrive both in the seed and as deltas (at-least-once, deduplicated by
binding); an unfiltered testament watch seeds the claims, responses being
reached through their claims; raw legacy `Operation::Stream` cursors need an
admitted V1 request epoch and are therefore invalid on a native ledger (watches
use managed cursors); watch journals of earlier development builds are
refused; native event retention and the retention floor's reclamation of
events belong to R8.

Qualification on macOS arm64 (Darwin 25.4.0, Rust 1.94.1), 2026-09-09 00:24–00:31
CDT, on this tree: `bash scripts/cargo.sh test --workspace --offline --no-fail-fast`
ran **2,388 tests across 93 test binaries with 0 failures**; strict workspace
all-target Clippy (`-D warnings`), the production no-panic gate, `cargo fmt
--all --check` and `cargo check --workspace --all-targets --locked --offline`
pass; `scripts/check-contracts.py` verifies **1,166 links**, all **37 imported
source hashes** and **15 frozen vocabularies**. Linux, Windows, released
binaries and external MCP clients were not exercised. Known limits carried
forward: chunked transfer and the packaged skills remain open on the native
engine (Batch G, R4.10); follower custody of native inline payloads is not
replicated; projection-only imported ledgers cannot author claims; a claim
whose timer is delivered on a terminal claim records only its outcome, so a
terminal claim's timer row lives until its deadline passes; native events
are retained with the prefix until R8 defines their retention floor; an
unfiltered testament watch seeds the claims rather than the responses.

## Child causes and follower custody of native payloads — 2026-09-09

This batch closes two of the limits the R4 native batches carried forward
(Batch G, first part). An artifact-bearing native frame's inline payload was
sealed and verified by the leader's data service but never replicated: a
follower held the record and not the bytes. `attest_native`
([evidence_service.rs](../../crates/focal-node/src/evidence_service.rs)) now
replicates the sealed payload to every other required copy of the current
placement through the same custody transfer a sealed upload uses, before the
frame is admitted, and an unreachable required copy refuses the frame; the
verified artifact's payload pointer is the content reference the transfer
moves. Evidence:
[evidence_service_tests.rs](../../crates/focal-node/src/evidence_service_tests.rs)
(`native_inline_payloads_are_sealed_locally_and_replicated_to_every_required_copy`:
the payload is readable as durable content under the placement after
attestation, and a second required copy that cannot be reached refuses the
frame). Node tests may now use the core's native fixtures
(`focal-core` `test-support` as a dev-dependency).

P17.12, the narrow child-cause authority, was already enforced by the model
and the owner (parent existence at the effective prefix, exact binding and
current receipt, actor equal to the parent's issuer or its current receipt
holder, live and unreleased parent, same ledger, owner-derived cause and
lineage, child registration as one parent revision, cancellation of pending
children with the parent) and reached by the shared claim document's
`parent`; what it lacked was qualification through the real surfaces and its
record. It is now exercised by
[cli_native_children.rs](../../crates/focal-node/tests/cli_native_children.rs)
(the issuer and the receipt holder each register a child through the binary,
the parent's registry names both at their committed bindings, a third
participant is refused as unauthorized, a forged parent is refused before
sending, cancelling the parent cancels its pending children and refuses a
late child, and the lineage survives a kill and restart),
[native_host_tests.rs](../../crates/focal-node/src/native_host_tests.rs)
(a frame naming a foreign-ledger parent, a stale parent binding and a
subject without a receipt are each refused with their typed code), and the
MCP A1 test (the issuer's follow-up through `claim.submit` with `parent`, the
respondent without a receipt refused with a typed outcome). Documents 13, 19
and the manual record the rule; designated-evaluator follow-ups arrive with
R5.

Qualification on macOS arm64 (Darwin 25.4.0, Rust 1.94.1), 2026-09-09 00:55–01:02
CDT, on this tree: `bash scripts/cargo.sh test --workspace --offline --no-fail-fast`
ran **2,391 tests across 94 test binaries with 0 failures**; strict workspace
all-target Clippy (`-D warnings`), the production no-panic gate, `cargo fmt
--all --check` and `cargo check --workspace --all-targets --locked --offline`
pass; `scripts/check-contracts.py` verifies **1,172 links**, all **37 imported
source hashes** and **15 frozen vocabularies**. Linux, Windows, released
binaries and external MCP clients were not exercised. Known limits carried
forward: automatic managed-stream rotation and registry recycling under
principal churn (P17.11, next batch), chunked transfer and the packaged skills
on the native engine (R4.10); multi-node native activation through the node's
support driver is exercised only at the ledger level until the R6 fleet
harness; projection-only imported ledgers cannot author claims; a terminal
claim's timer row lives until its deadline passes; native events are retained
with the prefix until R8; an unfiltered testament watch seeds the claims.

## Bounded generations: automatic rotation and registry recycling — 2026-09-09

This batch closes the P17.11 limits the managed-stream work carried since
2026-09-06 (Batch G, second part; decision F27 in
[07](07-decisions-and-traceability.md)). A principal's request stream is now
finite on both sides without any time-based deletion.

On the client, a generation issues at most its rotation bound of ordinals
(`DEFAULT_ROTATION` = 65,536). When the issuance frontier reaches the bound
and every ordinal through it is retired, `ManagedOperationStore::stop_if_drained`
stops issuance durably (a reservation refused during the drain is `Stopped`),
the coordinator issues the exact `Close`, and on the `Closed` reply
`begin_rotation` records the retired `(slot, generation)` fence (the last
sixteen are kept locally; the server keeps every slot's last generation),
names the next child store (`<name>.g<generation+1>`), removes the old store
and observes the slot again before registering above the generation it
presents. The coordinator record is `FCLMCO02` (schema 2: rotation bound,
active child, pending cleanup, retired fences; the bound is part of the record,
so a different bound cannot open it). A crash between the fence and the
removal finishes the removal at the next open; a lost close or read reply
re-issues the same request. References into a retired generation report
`Retired` from every adapter and never execute again. The CLI and the MCP
adapter open their stores through `cli/managed.rs::rotation()`, which reads
`FOCAL_MANAGED_ROTATION` for campaigns.
Evidence: [managed_requests/tests.rs](../../crates/focal-client/src/managed_requests/tests.rs)
(`a_drained_generation_at_its_rotation_bound_closes_and_the_next_registers_in_a_fresh_store`:
acknowledgment below the bound does not close, the exact close survives a
fresh process, the rotated store is removed, the slot is observed again and
the registration cites the presented generation, old references are retired
and the new generation issues from ordinal one;
`a_rotation_interrupted_between_its_fence_and_the_store_removal_finishes_on_the_next_open`;
`only_pre_network_initialization_can_repair_a_partial_external_marker` on the
schema-2 marker) and the binary campaign in
[cli_managed.rs](../../crates/focal-node/tests/cli_managed.rs)
(`a_bounded_generation_rotates_automatically_and_retires_its_references_across_processes`:
seven submits under `FOCAL_MANAGED_ROTATION=3` across seven processes rotate
twice with ordinals `1,2,3,1,2,3,1` and strictly increasing generations, the
ledger sequence advances exactly once per submit, the rotated store exists
under its generation name and the original is gone, a different bound is
refused, every retired reference inspects as `Retired`, nothing is pending,
a retry of a retired reference after a server restart is refused with exit
code 5 and commits nothing, and the next submit continues at ordinal two).

On the server, the registry (`request_streams.rs`) recycles pairs under a
persisted **slot-generation watermark**: `next_generation` is the highest
generation ever assigned, advanced by every registration and validated at
publication (`assigned > watermark`). At capacity a registration with no pair
of its own evicts the vacant pair with the smallest committed stamp
(`PreparedStream::evict`, validated against the same stamp and vacancy at
publication so every replica removes the same victim); occupied pairs are
never evicted and a registry of occupied pairs refuses `Capacity`. A vacant
pair presents `max(last generation, watermark)`; a registration cites that
presented generation and is assigned exactly one above it, which keeps the
frozen wire and client validators (`expected + 1 == assigned`) exact while
guaranteeing that a re-created or reassigned pair never reissues a generation
another principal's delayed traffic still names. Occupancy reads answer only
for the authenticated principal's own pairs, so no read exposes another
principal's owner nonce or receipts. The watermark is carried by the
`FOCALSS7` checkpoint envelope (`SnapshotEnvelopeV7`: SS6 plus
`slot_generation`); SS6 checkpoints are still decoded and derive the
watermark from their retained pairs, and a persisted watermark below the
retained pairs is refused as corrupt. Evidence:
[managed_tests.rs](../../crates/focal-ledger/src/managed_tests.rs)
(`registry_at_capacity_evicts_the_longest_closed_pair_and_never_reuses_a_generation`:
a two-pair registry, a stale registration citing zero conflicts, the
longest-closed pair is evicted by a third principal, the evicted principal's
seal is `NotRegistered` and its receipt reads `Unknown`, the survivor's slot
read is unchanged, a fourth principal is refused at capacity, the evicted
principal returns above every generation ever issued, and the watermark
survives checkpoint and restart), plus the unchanged managed suite whose
registrations now cite the presented generation.

Documents 13, 15, 07, 19, 22, 23, the manual and the MCP guide record the
contract; document 15 keeps the throughput (managed batching) and
mixed-version network gates open.

Qualification on macOS arm64 (Darwin 25.4.0, Rust 1.94.1), 2026-09-09 01:41–01:47
CDT, on this tree: `bash scripts/cargo.sh test --workspace --offline --no-fail-fast`
ran **2,395 tests across 94 test binaries with 1 failure**:
[cli_network.rs](../../crates/focal-node/tests/cli_network.rs)
`cluster::actual_cli_promotes_caught_up_learner_transfers_and_removes_with_exact_restart_receipt`,
whose founder process found the UDP port it chose from the shared
24,000–32,000 test range already bound by another test binary's node under
the parallel run (the panic lists the holder); the binary passes alone (six
tests, twice, 01:47 CDT). The preceding full run on the same tree (01:33–01:40
CDT, while another cargo build ran on the machine) instead failed three other
timing-sensitive tests that each pass alone:
`borrowed_proposal_tests::funded_checkpoint_transfers_source_lifetime_without_losing_durable_prefix`
(budget statistics compared while a writer thread released 582 bytes), the
`cli_network.rs` founder join, and the `fleet_quic.rs` trusted-membership
scenario (no quorum leader within its deadline with 97 lost messages). None
of the four touches this batch's files. Strict workspace all-target Clippy
(`-D warnings`), the production no-panic gate, `cargo fmt --all --check` and
`cargo check --workspace --all-targets --locked --offline` pass;
`scripts/check-contracts.py` verifies **1,183 links**, all **37 imported
source hashes** and **15 frozen vocabularies**. Linux, Windows, released
binaries and external MCP clients were not exercised. Known limits carried
forward: managed batching throughput and the mixed-version network campaign
(P17.11 order 9), chunked transfer and the packaged skills on the native
engine (R4.10); multi-node native activation through the node's support
driver is exercised only at the ledger level until the R6 fleet harness;
projection-only imported ledgers cannot author claims; a terminal claim's
timer row lives until its deadline passes; native events are retained with
the prefix until R8; an unfiltered testament watch seeds the claims.

## Failed work and evaluator errors as evidence (A2) — 2026-09-09

The second product gate (REMAINING §9 A2) now holds through the real binary
and the MCP adapter for its failure branches. Two participants run two claims
on a native ledger. On the first, the respondent's work fails: a failed
testament without its diagnostic is refused before anything is sent (typed
input failure); the respondent records the actual diagnostic with `artifact
diagnostic --reason work` and authors `testament submit --outcome failed
--diagnostic ID:HASH`; the reply is lost on a closed stdout (CLI) or the
adapter dies before the result is consumed (MCP), the journaled frame is
found committed and replayed, and exactly one testament exists. The claimant
receives it, reads the testament's `diagnostics` (producer, reason `Work`,
exact artifact reference) and the diagnostic bytes through `get artifact`;
the evaluator's context read names the missing slot as the exact target with
the delivery check passed and the registration ineligible; `validation
begin` and `validation report` on the missing slot are refused; `validation
enter-whole-work` assesses it, ending the required check and the claim
`ValidationIncomplete` from a missing-target result with no attempt and no
evidence, never Satisfied. On the second claim the work succeeds and the
evaluator cannot run its handler: `validation report --verdict error` retains
an error report whose provenance names the claim, validation, exact artifact
target and attempt zero, the evaluation stays open on attempt one of a
declared bound of two, the claim stays Validating and the work is untouched;
the retry passes on attempt one, the claim is Satisfied and the error report
remains beside the passing one. Both histories, the diagnostic bytes, the
report artifacts and every journaled receipt read identically after a kill
and restart, and the human CLI observes the adapter's error report through
the owner. Evidence:
[cli_native_a2.rs](../../crates/focal-node/tests/cli_native_a2.rs) and
[mcp_native_a2.rs](../../crates/focal-node/tests/mcp_native_a2.rs). A2's
challenge/consult bullet (an authorized evidence-backed corrective claim, an
ordinary follow-up consult) is R5 work; the manual and the MCP guide record
the failure contract.

Qualification on macOS arm64 (Darwin 25.4.0, Rust 1.94.1), 2026-09-09 02:05–02:11
CDT, on this tree: `bash scripts/cargo.sh test --workspace --offline --no-fail-fast`
ran **2,397 tests across 96 test binaries with 1 failure**:
`fleet_tests::reconciliation_tests::receipt_reads_require_live_quorum_preserve_privacy_and_recover_after_leader_restart`
in the `focal-node` library, whose in-process three-node fleet answered a
receipt read `Unavailable` (no live quorum within its window) under the
parallel run while another cargo build ran on the machine; it passes alone
(rerun at 02:11 CDT, and after the previous batch's run). Strict workspace
all-target Clippy (`-D warnings`), the production no-panic gate, `cargo fmt
--all --check` and `cargo check --workspace --all-targets --locked --offline`
pass; `scripts/check-contracts.py` verifies **1,185 links**, all **37 imported
source hashes** and **15 frozen vocabularies**. Linux, Windows, released
binaries and external MCP clients were not exercised. Known limits carried
forward: the A4 gate; A2's corrective/consult bullet (R5); managed batching
throughput and the mixed-version network campaign (P17.11 order 9); chunked
transfer, artifact payload download and the packaged skills on the native
engine (R4.10); multi-node native activation through the node's support
driver is exercised only at the ledger level until the R6 fleet harness;
projection-only imported ledgers cannot author claims; a terminal claim's
timer row lives until its deadline passes; native events are retained with
the prefix until R8; an unfiltered testament watch seeds the claims.

## Concurrency, lost replies and admission pressure (A4) — 2026-09-09

The fourth product gate (REMAINING §9 A4) holds through the real binary and
the MCP adapter on one node. The campaign exposed and fixed five defects in
the client layer before it could pass, each recorded here with its fix.

- **Concurrent processes on one data directory.** Six CLI processes of two
  participants submitting at once failed three ways: the enrolled context's
  credential directory and the context catalogue were opened under exclusive
  locks for the whole process, so a second process was refused ("already
  locked"); two processes racing to create the native request journal on
  first use produced `Exists` and `Corrupt`; and the journal's per-operation
  lock failed immediately on contention. Readers now take shared locks
  (`PrivateDirectory::open_shared`, `PrivateJournal::open_shared`,
  `JoinKey::open_shared`, `PendingClientJoin::resume_shared`,
  `Store::open_read`); writers stay exclusive and a reader meeting a writer
  fails closed; a refused write on a shared handle no longer poisons it.
  Journal creation is serialized by `<name>.lock` beside the marker
  (`cli/native.rs::store_in`), an interrupted creation without its marker is
  redone, and the native layout's directory lock waits up to five seconds
  (`FileLock::acquire_within`). Evidence: the concurrency sections of
  [cli_native_a4.rs](../../crates/focal-node/tests/cli_native_a4.rs) and
  [mcp_native_a4.rs](../../crates/focal-node/tests/mcp_native_a4.rs), and
  `journal::tests::shared_readers_coexist_and_exclude_writers_without_writing`.
- **Capacity refusals.** A refusal without effect was reported as an unknown
  outcome once it exhausted the attempt budget. The client now resends a
  request refused for capacity up to three times with backoff and, when every
  attempt was such a refusal, reports the refusal itself
  (`retry_uncertainty_tests.rs`,
  `capacity_refusals_are_resent_with_backoff_and_reported_as_refusals_not_unknown_outcomes`).
- **Lost replies at the durable boundaries.** `crates/focal-node/src/fault.rs`
  (feature `test-support`, `FOCAL_FAULT=<site>:<n>`, see [06 §3](06-verification-and-operations.md))
  aborts the node before the proposal or after the commit and before the
  reply. Before the proposal: the client reports an unknown outcome, the
  restarted node holds nothing, the reference is `Pending`, the exact retry
  commits once and a second retry returns the same receipt. After the commit:
  the restarted node re-commits its durable tail in its new term, the exact
  retry is answered by the owner's committed outcome for the same identity,
  the remote inspection shows the same intent, and no second claim exists.
- **Dead peers over QUIC.** An enrolled client's in-flight request waited
  the whole request timeout on a connection to a node that had died. The QUIC
  transport now bounds silence at ten seconds with keep-alive pings every
  two and a half (`focal-wire/src/transport.rs`), so the next attempt
  reconnects instead of waiting on a dead connection; connecting to an
  endpoint that is still down remains bounded by the request timeout.
- **Admission pressure.** `FOCAL_DISK_HEADROOM_BYTES` (read once at hosting,
  `network_service.rs::native_limits`) sets the engine's free-space watermark.
  Raised above the volume's free space, every fresh native candidate is
  refused `capacity` with the ledger sequence unchanged, while the exact retry
  of a committed operation is answered with its unchanged receipt and the
  remote inspection shows its intent; the refused reference stays `Pending`
  and commits exactly once when the pressure lifts. Read pins, partial apply
  under memory pressure and completion-budget pressure remain qualified at the
  ledger and owner level (`delivery_under_session_memory_pressure_is_retained_and_completes_exactly_once`,
  `a_follower_under_memory_pressure_keeps_its_delivery_and_finishes_when_memory_returns`,
  the R1 record-buffer and lease tests).

A cancelled MCP tool call (`notifications/cancelled`) drops only the
adapter's wait; the journal decides afterwards and the exact retry settles
the reference once. The manual, the MCP guide and document 06 record the
contract.

Qualification on macOS arm64 (Darwin 25.4.0, Rust 1.94.1), 2026-09-09 03:07–03:15
CDT, on this tree: `bash scripts/cargo.sh test --workspace --offline --no-fail-fast`
ran **2,401 tests across 98 test binaries with 0 failures**; strict workspace
all-target Clippy (`-D warnings`), the production no-panic gate, `cargo fmt
--all --check` and `cargo check --workspace --all-targets --locked --offline`
pass; `scripts/check-contracts.py` verifies **1,188 links**, all **37 imported
source hashes** and **15 frozen vocabularies**. Two earlier full runs of the
same batch (02:49–02:57 and 02:58–03:06 CDT) each failed one `cli_network.rs`
scenario whose founder found the UDP port it chose from the shared
24,000–32,000 test range already bound by another test binary's node, and
the first also a `cli_native_a2.rs` enrollment whose stderr the test did not
yet print (both pass alone; the gate tests now print the enrollment's
output). Linux, Windows, released binaries and external MCP clients were not
exercised. Known limits carried forward: A2's corrective/consult bullet (R5);
managed batching throughput and the mixed-version network campaign (P17.11
order 9); the packaged skills on the native engine (R4.10), the shared
operator/watch/transfer descriptors and the external MCP client
qualification (R4.9); chunked transfer and artifact payload download on the
native engine; multi-node native activation through the node's support
driver is exercised only at the ledger level until the R6 fleet harness;
projection-only imported ledgers cannot author claims; a terminal claim's
timer row lives until its deadline passes; native events are retained with
the prefix until R8; an unfiltered testament watch seeds the claims.

## Skills on the native engine — 2026-09-09

The four packaged skills (`skills/focal-claims`, `focal-evidence`,
`focal-validation`, `focal-cluster`) now carry an "On a native ledger" branch
and the shared contract a "Native engine" section
([workflow-contract.md](../../skills/references/workflow-contract.md)):
`ledger.standing` selects the branch; native identity is minted per call and
resumed through `operation_id` (no reservation or seal), recovery is the
four `request.*` tools, refusals are typed and a `capacity` refusal leaves
the reference pending; version-2 documents (slots, phases, handlers with
attempts, deadlines, `parent`), inline payloads, code-valued vocabularies,
bounded lists and schema-2 watches are described once and referenced from
each skill. The claims branch covers authoring/posting/cancelling, receipts
and adoption, lists and events, monitors and scope release; the evidence
branch the slot-bound work, diagnostics, failed slots, testaments with
diagnostics, posting/receiving and issuer acceptance/rejection; the
validation branch the context read, begin/report with zero-based attempts
and the error-retry rule, the missing-slot assessment at whole-work entry,
increment sealing and audits; the cluster skill the engine reporting and the
offline activation command. `skills/manifest.json` is schema 3: the adapter
pins `native_contract_version` 2 and every skill lists its
`required_native_operations`, whose union must equal the native catalogue
(37 descriptors). Both contract tests enforce it
([skill_contract.rs](../../crates/focal-client/tests/skill_contract.rs):
descriptor existence, version 2, `$id` `…:input:2`, use in the skill text,
the native section in the shared contract, and the union;
[skill_contract_tests.rs](../../crates/focal-mcp/src/skill_contract_tests.rs):
every required native operation is an advertised native tool whose
`operation_id` is an optional `n1:` resume argument), and an ignored helper
(`print_skill_digests`) prints the digests to re-pin after an edit. The MCP
guide and documents 13 and 19 record the packaging rule. The challenge,
consult and continuation procedures (`skills/focal-peers`) follow their typed
operations in R5.

Qualification on macOS arm64 (Darwin 25.4.0, Rust 1.94.1), 2026-09-09 03:21–03:29
CDT, on this tree: `bash scripts/cargo.sh test --workspace --offline --no-fail-fast`
ran **2,401 tests across 98 test binaries with 0 failures**; strict workspace
all-target Clippy (`-D warnings`), the production no-panic gate, `cargo fmt
--all --check` and `cargo check --workspace --all-targets --locked --offline`
pass; `scripts/check-contracts.py` verifies **1,192 links**, all **37 imported
source hashes** and **15 frozen vocabularies**. Linux, Windows, released
binaries and external MCP clients were not exercised. Known limits carried
forward: the peer skill and A2's corrective/consult bullet (R5); the shared
operator/watch/transfer descriptors and the external MCP client qualification
(R4.9); managed batching throughput and the mixed-version network campaign
(P17.11 order 9); chunked transfer and artifact payload download on the
native engine; multi-node native activation through the node's support
driver is exercised only at the ledger level until the R6 fleet harness;
projection-only imported ledgers cannot author claims; a terminal claim's
timer row lives until its deadline passes; native events are retained with
the prefix until R8; an unfiltered testament watch seeds the claims.

## One registry for every adapter surface — 2026-09-09

The watch, transfer and administration tools of the MCP adapter are now
shared `OperationDescriptor`s in the client crate beside the two application
catalogues (R4.9): `Surface::{Application, Watch, Transfer, Administration}`,
`Capability::{Actor, Node, FounderNode}`, a `RetryIdentity` naming the `a1:`
and `r1:` administration references, exact-argument resumption for watches
and uploads, or a fresh intent per call, the reviewed input schema as a
literal, and the CLI path that performs the same operation
(`catalog_watch.rs`, `catalog_transfer.rs`, `catalog_admin.rs`;
`find_surface`, `surface_of`). The adapter's catalogues are derived from
them with byte-equivalent tool schemas (the existing catalogue, schema and
administration tests pass unchanged), `tools/list` remains one pass filtered
by the standing the adapter can prove, and `execute_inner` dispatches by
surface so a tool the adapter did not advertise is refused by the dispatcher
as well as by the protocol layer (a transfer tool on a native ledger, an
administration tool without local node ownership). Evidence:
[command_tree_tests.rs](../../crates/focal-node/src/cli/command_tree_tests.rs)
(`every_surface_descriptor_cli_path_resolves_in_the_command_tree`: each named
path resolves to a leaf of the clap tree; the surface lookup is exact) and
the unchanged catalogue tests of the adapter. Documents 19 and the MCP guide
record the registry rule; the R9 operator surfaces extend it.

External MCP client qualification is packaged as scripts under
[crates/focal-mcp/tests/external](../../crates/focal-mcp/tests/external/README.md)
(the MCP Inspector CLI, Claude Code's `claude mcp add`, and the Python SDK
client), driven by [mcp_external.rs](../../crates/focal-node/tests/mcp_external.rs)
only when `FOCAL_EXTERNAL_MCP=1`; without the variable the test records the
skip. They were **not executed** in this offline session: the versions run
and their results are to be recorded here when they are.

Qualification on macOS arm64 (Darwin 25.4.0, Rust 1.94.1), 2026-09-09 03:39–03:47
CDT, on this tree: `bash scripts/cargo.sh test --workspace --offline --no-fail-fast`
ran **2,403 tests across 99 test binaries with 1 failure**:
`network_service::tests::joined_service_receives_committed_root_learner_and_restarts_without_ledger_policy`
in the `focal-node` library, whose in-process peer's invitation redemption
answered `Invalid` under the parallel run; it passes alone (twice, 03:48
CDT). This is the fourth network scenario today to fail only under the full
parallel run: every test binary draws the node ports it advertises from the
same 24,000–32,000 range, so binaries can race for a port between probing
and binding. Serializing that allocation across binaries is a test-harness
robustness item carried forward, not a defect of the batch. Strict workspace
all-target Clippy (`-D warnings`), the production no-panic gate, `cargo fmt
--all --check` and `cargo check --workspace --all-targets --locked --offline`
pass; `scripts/check-contracts.py` verifies **1,198 links**, all **37 imported
source hashes** and **15 frozen vocabularies**. Linux, Windows, released
binaries and external MCP clients were not exercised; the external client
scripts above have not run. Known limits carried forward: the peer skill and
A2's corrective/consult bullet (R5); the parallel-run port allocation of the
node tests; managed batching throughput and the mixed-version network
campaign (P17.11 order 9); chunked transfer and artifact payload download on
the native engine; multi-node native activation through the node's support
driver is exercised only at the ledger level until the R6 fleet harness;
projection-only imported ledgers cannot author claims; a terminal claim's
timer row lives until its deadline passes; native events are retained with
the prefix until R8; an unfiltered testament watch seeds the claims.

## Exact evidence and peer policy (R5.1) — 2026-09-09

A claim can now cite one exact committed artifact and carry its own
follow-up rules (decision F28, [07](07-decisions-and-traceability.md)),
the representation the challenge, consult and corrective workflows of R5 are
built on. Claim descriptor schema 2 appends an optional `PeerPolicy`
(`corrective_allowed`, `max_follow_ups` bounded by `MAX_FOLLOW_UPS` = 1,024,
`single_issuer`, `escalation` none/holder/evaluator) after the deadline and
admits `RelationTarget::Evidence(ArtifactRef)` as the target of `reviews` and
`derived_from` relations only; schema 1 refuses both (`InvalidPolicy`), any
other relation kind naming evidence is `InvalidTarget`, and the descriptor
hash covers the policy's presence and fields from schema 2 on so schema-1
identities, bytes and hashes are unchanged
([21 §5](21-native-input-format.md), [22 §7](22-native-record-format.md)).
The input codec writes the target as tag 4 (artifact identity and descriptor
hash) and the policy as a presence byte plus fields, the frame inspector
bounds both without decoding, the canonical record codec carries the target
as tag 5, the V1 durable codec refuses the new target, and a creation result
records claims of schema 1 or 2 and definitions of schema 1 only. The owner
admits an evidence relation only when that artifact is committed on the same
ledger at exactly that descriptor hash (a pending artifact of the same batch
cannot be cited), replay validation requires the same, and `ByRelation`
indexes the relation by the artifact's identity so the reviews and
derivations of one artifact are listable (`--relation reviews=artifact:ID`,
optionally `@HASH`). The host-side compiler selects schema 2 exactly when a
document carries `policy` or an evidence relation; documents, the reviewed
JSON schema, the CLI's `--relation reviews:artifact:ID@HASH` form and the MCP
`claim.submit` tool share that one rule, and `get claim` returns the policy.

Evidence: [claim_descriptor_tests.rs](../../crates/focal-model/src/lifecycle/claim_descriptor_tests.rs)
(schema-2 construction and hashing; a schema-1 policy or evidence target and
an unsupported schema refused), [objects_tests.rs](../../crates/focal-model/src/durable_v1/objects_tests.rs)
(the V1 codec refuses an evidence target), [creation_content_tests.rs](../../crates/focal-core/src/native/input_codec/creation_content_tests.rs)
(input round trip of the target and policy; a schema flip changes the
identity), [frame_tests.rs](../../crates/focal-core/src/native/input_codec/frame_tests.rs)
(schema 3 refused by the inspector), [creation_result_tests.rs](../../crates/focal-core/src/native/creation_result_tests.rs)
(claim schema 2 recorded, definition schema 2 refused) and
[focal-native-client tests.rs](../../crates/focal-native-client/src/tests.rs)
(`a_challenge_cites_exact_committed_evidence_and_carries_its_policy_through_the_owner`:
the challenge compiles to schema 2, commits through the owner with its
policy and exact target readable, an ordinary claim keeps schema 1, the wrong
hash and an unknown artifact are refused `InvalidTarget` by the owner, and a
`depends_on` on evidence or an evidence target without its hash is refused by
the compiler before any identity is spent). The CLI `claim challenge|consult|
correct|follow-up|lineage` verbs, the owner's corrective and follow-up rules
(cause authority for a designated evaluator, single-issuer and follow-up
bounds, verdict citation) and the peer skill are R5.2–R5.5.

Qualification on macOS arm64 (Darwin 25.4.0, Rust 1.94.1), 2026-09-09: `bash
scripts/cargo.sh test --workspace --offline --no-fail-fast` at 04:15–04:23 CDT
ran **2,407 tests across 99 test binaries with 0 failures**. Three edits
followed that run and were re-tested in their crates on the final tree: a
Clippy-required rewrite of the replay evidence check in
`authored_check.rs` to the equivalent `is_none_or` form (`focal-core` library,
834 tests, 04:24 CDT), the `--relation` help text (`focal-node` library, 163
tests, 04:24 CDT) and a scoped borrow in the native-client test (6 tests,
04:26 CDT); the node's binary-level suites were not rerun for the equivalent
rewrite. On that final tree strict workspace all-target Clippy
(`-D warnings`), the production no-panic gate, `cargo fmt --all --check` and
`cargo check --workspace --all-targets --locked --offline` pass (04:26 CDT);
`scripts/check-contracts.py` verifies **1,210 links**, all **37 imported
source hashes** and **15 frozen vocabularies**. Linux, Windows, released
binaries and external MCP clients were not exercised. Known limits carried
forward are those of the registry batch above, with the peer verbs, owner
rules, skill and fault journeys of R5 still open.

## Corrections and follow-ups under the authored policy (R5.2) — 2026-09-09

The owner now enforces the peer rules that the schema-2 policy declares
(decision F29, [07](07-decisions-and-traceability.md); rules in
[21 §5](21-native-input-format.md)). A challenge whose verdict failed and a
consultation that was answered are terminal, and a terminal or released
claim cannot own a child, so a follow-up never reopens the claim it follows:
a correction (action `correction`) `invalidates` exactly one committed
challenge and `reviews` exactly one exact artifact, the report of that
challenge's terminal Fail, Incomplete or Error verdict at its current
registration generation; a follow-up consultation `refines` the consultation
it continues. `authored_peer.rs` applies the rules at admission and, from
the recorded contents, at replay: the invalidated claim must be a challenge
whose policy has `corrective_allowed` (`InvalidTarget`, `InvalidPolicy`),
the cited artifact must be that challenge's terminal negative verdict
(`MissingEvidence` for any other artifact, `InvalidTransition` for a passing
or still-retryable verdict, `StaleEvaluation` when the challenge has been
re-registered since), the author must be the challenge's issuer, its
current holder unless `escalation` is `none`, or under `escalation:
evaluator` the evaluator who reported that verdict (`WrongActor`), and under
`single_issuer` a second correction, committed or in the same batch, is
`ConflictingCause`; a follow-up consultation of a claim with a policy is
authorized by the same escalation and bounded by `max_follow_ups` across
the committed prefix and the batch (`InvalidPolicy`), while a claim without
a policy bounds nobody. Both counts come from the relation index through
`View::relation_sources`, exposed as `NativeView::related_claims`; each
claim's citation walk and scans are bounded by `plan_edges` of their own
rather than the exact creation allowance. The same escalation now governs
`caused_by` children of a live parent through two default methods on the
model's `EffectiveClaims` (`cause_escalation`, `is_designated_evaluator`):
`none` reserves them to the issuer, `holder` keeps the schema-1 rule, and
`evaluator` also admits a designated evaluator of the parent's declarations
(`Declaration::designates`). Descriptor schema 2 admits the `invalidates`
relation (never to the descriptor itself); the compiler refuses an
`invalidates` on anything but a correction and requires a correction to
carry exactly one `invalidates` and one reviewed verdict artifact before any
identity is spent; the reviewed JSON schema lists the kind.

Evidence: [creation_tests.rs](../../crates/focal-model/src/lifecycle/creation_tests.rs)
(`authored_escalation_decides_who_besides_the_issuer_may_cite_a_parent`:
`none` refuses the holder, `evaluator` admits only a designated evaluator, a
node principal is always refused), [claim_descriptor_tests.rs](../../crates/focal-model/src/lifecycle/claim_descriptor_tests.rs)
(`invalidates` accepted at schema 2, refused at schema 1 and when
reflexive) and [focal-native-client tests.rs](../../crates/focal-native-client/src/tests.rs)
(`a_correction_rests_on_the_challenge_s_failed_verdict_under_its_authored_policy`:
a full two-party challenge to a Fail verdict, then a stranger refused
`WrongActor`, the work artifact instead of the report `MissingEvidence`, a
plain claim `InvalidTarget`, a challenge without a policy `InvalidPolicy`, a
passed challenge `InvalidTransition`, the wrong document shapes refused by
the compiler, the reporting evaluator's correction committed at schema 2
with its relations readable and the challenge untouched, and the issuer's
and holder's later corrections `ConflictingCause`;
`consult_follow_ups_refine_their_parent_within_its_authored_policy`: the
subject refused under `escalation: none`, one follow-up admitted, the second
refused `InvalidPolicy`, and a consultation without a policy taking
follow-ups from anyone). The peer verbs, documents, `claim wait` on the
native engine, the peer skill and the CLI/MCP fault journeys are R5.3–R5.5.

Qualification on macOS arm64 (Darwin 25.4.0, Rust 1.94.1), 2026-09-09
04:53–05:01 CDT, on this tree: `bash scripts/cargo.sh test --workspace
--offline --no-fail-fast` ran **2,410 tests across 99 test binaries with 0
failures**; strict workspace all-target Clippy (`-D warnings`), the
production no-panic gate, `cargo fmt --all --check` and `cargo check
--workspace --all-targets --locked --offline` pass; `scripts/check-contracts.py`
verifies **1,218 links**, all **37 imported source hashes** and **15 frozen
vocabularies**. Linux, Windows, released binaries and external MCP clients
were not exercised. Known limits carried forward are those of the R5.1
section, with the peer verbs, skill and fault journeys of R5 still open.

## Peer verbs, lineage, the testament wait and the peer skill (R5.3–R5.5) — 2026-09-09

The peer workflows now have their typed surface on both adapters. Four
native descriptors are **authored shapes of `claim.submit`**
(`claim.challenge`, `claim.consult`, `claim.correct`, `claim.follow_up`):
typed documents that `focal-native-client/src/peer.rs` lowers to one
complete claim document before the shared claim compiler runs, so they
produce the same `FCNINPUT` frame, `n1:` identity and receipt as a
hand-written claim with the same content; the coverage table claims them
through `claim.submit`'s frame tags (`native_catalog::authored_shape`) and a
projection-only ledger withholds them with it. A challenge names the
disputed artifact as `ID` or `ID@HASH` (an omitted hash is read from the
ledger before the frame is compiled) and must carry its policy; a correction
names the challenge and the report artifact of its verdict (`verdict`,
again `ID` or `ID@HASH`), defaults its target to the challenge's subject and
lowers to `invalidates` + `reviews`; a follow-up names the consultation it
`refines` and defaults its target to that consultation's subject. A
correction's and a follow-up's **occurrence identity derives from the facts
they rest on** (ledger, author, challenge or refined claim, verdict at its
hash, description) and, with it, the claim's, every validation's and every
timer's identity, so the same facts produce byte-identical descriptors and
the owner resolves a repeated delivery to the one committed claim: a lost
reply, a retried tool call or a second CLI invocation never mints a second
correction. Two composed reads join them: `claim.lineage` (one
`native_read` page: the claim with its content, its `caused_by` ancestors
nearest first up to 16, then up to 64 committed corrections, refinements and
children with their content, every read at or after the first read's token)
and `claim.wait` on the native engine (`observe.rs`: the V1 observer's
bounds and monotonic checks over exact native claim reads, plus the
`testament` predicate met once the issuer has received a closing
testament; result kind `native_wait`). The wire claim content now carries
the authored `policy`, so `get claim` returns it. The CLI adds `claim
challenge|consult|correct|follow-up|lineage` and serves `claim wait` on
native ledgers (with `--until testament`); the MCP adapter derives the six
tools from the descriptors, dispatches the composed reads through one
`focal_native_client::read` entry point with a cancellable pause, and its
ordinary memory headroom grew to 80 MiB for the 47-tool catalogue. The
`focal-peers` skill (manifest schema 3, five skills) sequences the verbs
with `references/peer-workflows.md`; the union of the skills' native
requirements still equals the native catalogue (43 descriptors).

Evidence: [focal-native-client tests.rs](../../crates/focal-native-client/src/tests.rs)
(`peer_verbs_are_authored_shapes_of_claim_submit_with_derived_identities`:
a challenge disputing an exact artifact with its hash read from the ledger;
a correction by the reporting evaluator whose second delivery under a fresh
request resolves to the same claim with identical derived identities and
whose variant is `ConflictingCause`; a consultation, its follow-up addressed
to the consultation's subject, the same follow-up as one claim, and the
subject refused; `the_wait_observer_and_the_lineage_read_compose_bounded_exact_reads`:
pending then met with one pause, `Unmet` at once on a terminal claim, a
backwards observation refused, and a lineage page ordered claim, ancestor,
correction, child at the first token), [native_tests.rs](../../crates/focal-client/src/operations/native_tests.rs)
(43 descriptors, the four authored shapes claimed through `claim.submit`,
every new document field in its schema), the MCP catalogue tests (the peer
shapes withheld on projection-only ledgers, `claim.wait` as `native_wait`)
and the two journeys through the real binary:
[cli_peer_workflows.rs](../../crates/focal-node/tests/cli_peer_workflows.rs)
and [mcp_peer_workflows.rs](../../crates/focal-node/tests/mcp_peer_workflows.rs)
(three participants on one native node: a consultation answered, observed
with `--until testament` Pending before and Met after receipt, followed up
once with the same command twice being one claim, a second follow-up refused
by the policy and the subject refused; a challenge disputing the exact
answer, failed by its evaluator, `--until satisfied` Unmet; a correction
citing the work instead of the verdict refused `missing_evidence`, the
reporting evaluator's correction committed, repeated as the same claim and
retried by reference, the holder's and issuer's corrections
`conflicting_cause`, the challenge's revision and status untouched; lineage
pages naming the correction and the follow-up; a kill-and-restart after
which every read and refusal is the same; and on MCP the follow-up
cancelled and observed terminal and the correction's outcome read by
identity with `request.inspect`). Not exercised in this batch: the
replicated journeys (they wait for the R6 fleet harness), deadline expiry
under a controlled clock, receipt adoption during a challenge, and evaluator
error-retry exhaustion on a challenge (its mechanics are qualified by A2).

Qualification on macOS arm64 (Darwin 25.4.0, Rust 1.94.1), 2026-09-09: `bash
scripts/cargo.sh test --workspace --offline --no-fail-fast` at 05:50–05:58 CDT
ran **2,414 tests across 101 test binaries with 0 failures**. One edit
followed that run: Clippy's `clone_on_copy` lint required dropping four
`.clone()` calls on a `Copy` document field in `peer.rs`; on the final tree
the native-client suite (10 tests) and both peer journeys were rerun
(05:58–05:59 CDT) and strict workspace all-target Clippy (`-D warnings`), the
production no-panic gate, `cargo fmt --all --check` and `cargo check
--workspace --all-targets --locked --offline` pass (05:59 CDT);
`scripts/check-contracts.py` verifies **1,226 links**, all **37 imported
source hashes** and **15 frozen vocabularies**. Linux, Windows, released
binaries and external MCP clients were not exercised. Known limits carried
forward: the replicated peer journeys and the remaining fault cases named
above (R6 harness), plus those of the R5.1 section.

## Placement progress in the directory (R6.1) — 2026-09-09

The partition directory now records what a placement change has achieved,
not only that one is pending. Design: [24](24-placement-execution-and-fleet-control.md);
decision F31 in [07](07-decisions-and-traceability.md).

**Model** (`crates/focal-directory/src/partition_progress.rs`,
`partition_session.rs`, `partition.rs`, `types.rs`, `placement.rs`,
`authority_proof.rs`):

- `AssignmentProgress { node, node_generation, roles, phase, attempt,
  through, custody_epoch, refusal }` per node of a pending placement, created
  at `BeginPreparation`; `AssignmentPhase` ladder `Assigned → Installed →
  CaughtUp → CustodyVerified → Promoted` (`Active | Draining | Retired` for
  retiring copies, `Failed` off the ladder); `AssignmentRole` derived from the
  desired placement and refused when a report disagrees.
- `SessionChange::{Progress, Refuse, Drain, Retire}` appended;
  `SessionChange::Plan` carries `observations` (load-report epochs that must be
  at most the committed report of a node in the placement).
- `PlacementPhase::{Planned, Preparing, Catchup, Custody, Promoting, Cutover,
  Failed}`; every phase after `Planned` is derived from committed rows and the
  checkpoint validator refuses a stored phase that differs.
- Rules: progress is monotone within an attempt and an exact repeat is a
  no-op; a new attempt may restart the ladder; `CustodyVerified` and `Promoted`
  require the node's own signed `ReplicaReady` (`NotReady` otherwise) and
  readiness itself raises a row to `CustodyVerified`; `Promoted` only for
  voters; the cutover fence is accepted only when every desired voter is
  `Promoted` and no copy is `Failed`; after the fence a refusal can no longer
  fail an assignment; `Activate` additionally requires every copy at its
  required phase with `custody_epoch == next_placement`, and moves the copies
  the new placement drops into `SessionDescriptor.retiring` (`Active`, drained
  by `Drain`, removed by `Retire`, both keyed by the activation's operation).
- `Refusal { operation, code: RefusalCode, node, attempt, at }` with a bounded
  per-session ring (`PartitionConfig::max_refusals`, default 16); a named
  refusal fails that attempt exactly once; an unnamed refusal never names the
  live plan or the active authority; `RefusalCode::retryable()`.
- `effective_guarantee(&SessionDescriptor, &nodes) -> GuaranteeReport {
  desired, achieved, blocked_by, phase }`: `achieved` is the largest number of
  promised failure domains the active placement survives with the live node
  registry (missing, re-enrolled and ineligible members count as lost; an
  unevaluable domain yields `None`); `blocked_by` names outstanding
  assignments, refusals, the awaited cutover or activation, and draining
  copies; bounded to 256 blockers under `try_reserve_exact`.
- `NodeLoad.disk_available`; `propose_placement(nodes, policy, max_members,
  min_disk_available)` skips nodes below the headroom and prefers roomier ones;
  `PartitionConfig::min_disk_available` (64 MiB). The frozen V1 row codec
  (`durable_v1.rs`, new `v1_struct_later!`) writes the original five fields and
  restores the sixth as zero; the fixture parity test reads those rows through
  the frozen path only.
- `AuthorityFact::Custody(CustodyProof)` and `AuthorityVerifier::verify_custody`
  (self-signed, attestation zeroed in the body, epoch non-zero), implemented by
  the installed verifier and every stub verifier in the tree.
- Memory: every new row is charged in `partition_charge` and per change in
  `partition_session::change_charge` (progress rows with their role sets,
  observations, the refusal ring, retiring rows at activation).

**Formats.** `PartitionCheckpoint.schema = 2`; the digest domain is
`focal:directory-partition-checkpoint:v2`. `partition_v1.rs` keeps
`PartitionCheckpointV1` (with `NodeLoadV1`, `PendingPlacementV1`,
`SessionDescriptorV1`) and a fallible conversion: rows derive from recorded
readiness, voters become `Promoted` under a recorded cutover fence, and a fence
over a voter without readiness refuses (`Phase`) instead of inventing custody;
`PartitionCheckpoint::decode_any` accepts either schema and refuses trailing
bytes. `focal-control` writes checkpoint schema 4 (the schema 3 layout with the
current bootstrap; the structs are generic over the state type) and reads
schemas 1–3 through `LegacyControlBootstrap`; the command envelope is schema 2
and schema 1 entries still decode except for load reports and plans, which
predate these fields. A partition group bootstrapped at schema 1 has a
different genesis identity from the same delegation at schema 2 (asserted in
`state::legacy_tests`); root groups are unchanged. None of these formats has
shipped.

**Tests.** `crates/focal-directory/tests/placement_progress.rs` (5): the
full ladder with every refusal of stale, forged or premature progress, cutover
gated on promotion, activation gated on readiness at the barrier, shrinking a
placement and draining/retiring the copies left behind, restore equality;
refusals failing one attempt, restarting under a new attempt, the bounded
ring, plan-level refusals and abort; the measured guarantee under stale,
missing, ineligible and unevaluable members; schema 1 decode, conversion,
continuation through activation, and the refused orphaned fence; the planner's
disk headroom filter and ordering. `control_state.rs` reordered its two
lifecycle tests for the promotion-before-cutover rule. `focal-control`
`state::legacy_tests` pins the root bootstrap bytes and the partition
conversion. Not exercised: replica-level restore of a schema 1–3 control
snapshot (no writer for those schemas remains; the conversion is tested at the
directory and bootstrap level).

**Deviations from the plan text, recorded.** `PlacementPhase` has no
`Draining` value: draining is a property of retiring copies, which live in
`SessionDescriptor.retiring` after activation frees the pending slot;
refusals are kept per session rather than per pending plan so that a refused
plan that never became pending has a record.

**Evidence** (macOS arm64, this tree, 06:36–06:44 CDT):
`bash scripts/cargo.sh test --workspace --offline --no-fail-fast` — **2,420
tests across 102 test binaries, 0 failures**; `bash scripts/cargo.sh clippy
--workspace --all-targets --offline -- -D warnings` clean; `bash
scripts/check-production.sh` clean; `cargo fmt --all --check` clean; `cargo
check --workspace --all-targets --locked --offline` clean; `python3
scripts/check-contracts.py` — 1,236 links, 37 hashes, 15 vocabularies.

**Remaining for R6** (24 §7): agent and journal, `PlacementControl` and the
proof collector, the controller, admission and load, credential renewal and
revocation, split/merge with the route cache, and the operator API.

## The placement agent registers the founder's session (R6.2) — 2026-09-09

Every `NetworkService` now runs a `PlacementAgent` beside the root controller
(`crates/focal-node/src/placement_agent.rs`; design in
[24](24-placement-execution-and-fleet-control.md) §7). It acts where the
partition owner is hosted locally and this node leads it, which is the
founder's first partition; nodes without a local partition owner idle until
the peer placement RPC exists.

**What it does.** One bounded pass every 250 ms, at most one command per pass:

- **Exact-retry journals** (`placement_journal.rs`): `IntentJournal` under
  `cluster/placement-root` and `cluster/placement-partition` with
  `PLACEMENT-ROOT.initialized` / `PLACEMENT-PARTITION.initialized` markers,
  one stable client identity per node (`PlacementAgent::client`), the pending
  `ControlRequest` journaled before proposal through the owner's
  `save_local_intent`, resubmitted with the identical identity until a receipt
  or a pre-admission refusal (`CompareFailed`, `Rejected`, `Unauthorized`,
  `Invalid`, `WrongOwner`) resolves it; not-leader, not-ready, capacity,
  unavailable and unknown outcomes keep it pending.
- **Founder registration** over the hosted replica: `ReplicaHost::registration_facts`
  (new `fleet_registration.rs`, read on the owner thread) feeds
  `FirstSessionPlan::capture_facts` (the former `capture` now builds
  `HostedSessionFacts` from a `Session` and delegates), then in order the root
  `BootstrapGroup` grant, the session-log `Created` witness
  (`propose_placement` answers the exact request with the committed record),
  the partition `Enroll`, the root-signed session fact
  (`prepare_session_proof`) and `CreateSession`. Once the directory holds the
  session the plan is never captured again, so a session whose log moved past
  its creation fence is not a conflict.
- **Load reports**: `NodeLoad { available_memory }` from the node's whole
  allowance, `active_weight` from installed replicas, `disk_available` from
  the data directory's filesystem, report epoch above the committed one and
  the clock; due when no row exists, the generation changed, or 30 s passed.
- **Signed readiness**: for a pending plan naming this node in preparation,
  once the hosted replica's own placement fence is the plan's cutover record,
  `checkpoint_evidence` → `ContentHost::verify_prefix` →
  `ReplicaReady { through, custody }` → root `prepare_replica_ready`
  (new `placement_proof::prepare_replica_ready_proof`: this node at its
  enrolled generation in the session's installed group, enrollment current for
  the window) → `SessionProofPermit::attestation` completes the fact → signed
  with the node credential → `SessionChange::Ready` under `VerifiedPartition`
  evidence carrying the proof (`session_registration::control_evidence`).
  Reported once per verified prefix, again only past a recorded barrier, at
  most once per two seconds per plan.
- **Failure policy**: a failed tick never ends the node; retryable failures
  wait one tick, others back off five seconds, the last error is retained for
  diagnostics; a runtime without a timer driver is the only fatal condition.

`ControlHost` gains `Work::PrepareReplicaReady` / `prepare_replica_ready`;
the fleet owner gains `Work::Registration` (control lane). The service moves
the node credential into the agent after the listener and connector copied
what they need and drives the agent in `run_tasks`.

**Tests.** `crates/focal-node/src/placement_agent_tests.rs` (2, real
`NetworkService` over QUIC and the Unix socket): the founder's session appears
in the partition with its `Created` fence, single-voter placement and a load
row with positive disk headroom; both journals exist; a restart re-reads them
and adds exactly one fresh load report and nothing else within the interval;
a Plan and BeginPreparation submitted as the controller produce no readiness
while the log is still at the old route, the session-log cutover record then
yields one signed `ReplicaReady` at route 2 with a non-zero custody digest and
attestation that the partition's installed verifier accepted, progress
`CustodyVerified` at `custody_epoch` 2 and plan phase `Promoting`, and a
restart adds only a load report. `network_service_tests` now waits for the
founder's session group grant on the root instead of asserting one group.

**Evidence** (macOS arm64, this tree, 07:15–07:23 CDT):
`bash scripts/cargo.sh test --workspace --offline --no-fail-fast` — **2,422
tests across 102 test binaries, 0 failures**; strict all-target Clippy,
`scripts/check-production.sh`, `cargo fmt --all --check` and the `--locked`
check clean; `python3 scripts/check-contracts.py` — 1,239 links, 37 hashes,
15 vocabularies. An earlier run of the same batch (07:04–07:12) failed three
tests because the agent ended the service on a registration conflict and a
service test pinned one root group; both are fixed above and the final tree
was rerun in full.

**Limits recorded.** The agent acts only where the partition owner is local
and led by this node; a session whose membership changed before its first
registration cannot be registered by `FirstSessionPlan`; readiness for copies
on other nodes, learner promotion and the cutover/activation fences await the
collector and controller batches (24 §8).

## A joined host installs its assignment and signs under a quorum (R6.3) — 2026-09-09

The placement agent now acts on every node, and a founder's session expands
onto a joined host end to end. Design: [24](24-placement-execution-and-fleet-control.md)
§8; wire tags in [03](03-rust-workspace-and-interfaces.md) §9.

**Wire** (`crates/focal-wire`): `Operation::PlacementControl { group,
request }` (tag 28, mutation, 256 KiB) and `Operation::SessionSign { group,
request }` (tag 29, read, 64 KiB); both `Capability::Replication`,
certificate-bound (a trusted local Node grant is refused), unavailable to
participant ingress, answered in `Response::Control`;
`PeerConnectionPool::send_placement`; the client inventory rows
`peer.placement_control` and `peer.session_sign`. Test
`placement_control_and_session_sign_are_node_only_certificate_bound_and_bounded`.

**Node** (`crates/focal-node`):

- `placement_control.rs`: `decode_placement_control` admits reads through the
  discovery selectors and a submit only when it is a `VerifiedPartition`
  command about the sender itself (`Enroll`, `ReportLoad`, `Ready`,
  `Progress` at `Assigned` or `Installed`); the control host binds the
  sender's certificate to the enrollment it has installed
  (`ControlReplica::installed_enrollment`, the installed authority's copy for
  a partition) before decoding. `SessionFact::{Placement, Membership}`,
  `SessionSignRequest`/`SessionSignReply`; `prepare_session_fact` witnesses a
  placement record on any replica (new `ReplicaHost::placement_witness`) or
  checks a membership against the replica's applied configuration and its
  change receipt, then the root owner prepares the permit. `PlacementHandle`
  (`NetworkHandles::placement`) carries `sign`, `collect` and `status` jobs
  to the agent, which owns the node credential; the managed service answers
  `SessionSign` through it.
- `placement_collect.rs`: `Collected` merges signatures over one identical
  statement, drops a differing statement, and completes at
  `voters / 2 + 1`; `remote_signature` asks one voter and yields nothing for
  an unreachable or refusing one.
- `placement_proof.rs`: `prepare_membership_proof` (root owner; the signer
  must be a current voter, `next` a legal successor whose members are
  enrolled for the window; the unsigned share must fail only on quorum) and
  `MembershipRecord`; `ControlHost::prepare_membership_proof`.
- `placement_agent.rs`: partition access is local where the owner is hosted
  and led here, otherwise `PeerControl` reads and `PlacementControl` submits
  at the founder node; the partition journal binds the identity its owner
  sees (derived local principal locally, enrolled principal on the wire);
  `enroll_self` from the root grant; `install_assignment` opens a
  `DurableNode` on the shared WAL under the founder's bootstrap membership,
  hosts the native engine, installs into the fleet at the log's committed
  route and records the copy in `cluster/placement-installs`
  (`PLACEMENT-INSTALLS.initialized`), reopened at every start;
  `install_custody`/`sync_custody` keep each hosted ledger's custody scope at
  the placement the directory activated; `collect` signs locally and asks the
  other voters; `AgentStatus` for diagnostics.
- `network_service.rs`: the founder's serving scope (`ReplicaConfig::{route_epoch,
  policy_revision}`) and custody policy follow the session's *active* placement
  fence (`Session::active_fence`, `active_placement`), never a pending cutover;
  node peer grants include the first directory's namespace tenant.
- `focal-directory`: `verify_membership` keeps the membership epoch for a
  learner-only change and advances it by one for a voter change, so the epoch
  a fence carries equals the grant's epoch; `verify_membership` is public.
- `network_join.rs`: `PendingJoin::redeem` resamples the clock after the
  exchange before checking `issued_at`, closing a second-boundary race that
  refused fresh receipts as future-dated under load.

**Tests.** `placement_agent_tests::a_joined_host_installs_its_assignment_and_the_expanded_placement_activates_under_a_signed_quorum`
(two real `NetworkService`s over QUIC): the joined host enrolls itself and
reports load over the wire; the test as controller plans two voters and
begins preparation; both agents report `Installed` and the joined host
serves a replica; the learner is added, its group change installed on the
root under the founder's signature, it catches up and is promoted, the
promotion installed as epoch 2; the session log commits the cutover record;
both copies verify custody and sign readiness (the joined host's proof
verified against the partition's installed authority); promotion is
recorded, the cutover fence is signed by both voters through
`PlacementHandle::collect` (one local signature, one over `SessionSign`) and
installed, the activated record is signed and installed; the directory holds
the two-voter placement at route 2 with `effective_guarantee` reporting the
desired tolerance and no blockers; the joined host restarts and reopens its
copy from its install journal. The two R6.2 tests still pass;
`authority_tests` adds a learner without advancing the epoch.

**Evidence** (macOS arm64, this tree, 08:16–08:24 CDT):
`bash scripts/cargo.sh test --workspace --offline --no-fail-fast` — **2,424
tests across 102 test binaries, 0 failures**; strict all-target Clippy,
`scripts/check-production.sh`, `cargo fmt --all --check` and the `--locked`
check clean; `python3 scripts/check-contracts.py` — 1,243 links, 37 hashes,
15 vocabularies. An earlier run of the same batch (08:06–08:15) failed the
learner-epoch assertion and the join redemption race named above; both are
fixed and the final tree was rerun in full.

**Limits recorded.** The test acts as the controller; the controller batch
automates every step it performed. A cutover fence must carry exactly one
epoch above the active one, which two or more promotions before one cutover
cannot satisfy; the controller batch redefines the fence's membership epoch
as the grant's actual epoch at signing (a lower bound in the directory and
the ledger). After a placement change a host serves the new route epoch and
participant clients must be told it (route discovery is R7). Remote partition
submits target the node the first partition was delegated to.

## The controller drives a plan to activation and heals a lost host (R6.4) — 2026-09-09

The placement controller of [24](24-placement-execution-and-fleet-control.md) §9
is implemented in `crates/focal-node/src/placement_controller.rs` as the last
step of the placement agent's tick, on the node that leads both the partition
owner and the session's log. It reconstructs every step from the committed
partition checkpoint, the session's applied membership and its placement
fences, so a restarted or re-elected controller resumes where the committed
state stands, and every command goes through the agent's exact-retry journals.

- **Grant follows log.** A configuration the session log committed but the
  root grant does not name is installed first (`ChangeGroup` with a
  membership proof collected from the current grant's voters); the epoch
  advances only for a voter change.
- **Driving a plan.** `BeginPreparation` from `Planned`; `AddLearner` once a
  desired voter reports `Installed` and `Promote` once it reports `CaughtUp`
  (deterministic change ids, so a lost reply finds the retained receipt);
  the cutover record once the log's voters are the desired voters; `Promoted`
  for each voter the grant names whose custody is verified; the voter-majority
  proof of the cutover fence recorded as the barrier; the activated record and
  its proof once every copy has signed readiness at or beyond the barrier.
- **After activation.** Retiring copies are drained, removed from the log and
  retired; when the active placement no longer verifies against the live
  registry the controller re-plans under the active policy, or records one
  `NoPlacement` refusal.
- **Epoch rule.** A cutover fence carries the group's actual membership epoch,
  which several learner and promotion changes raise above the one the
  placement implies: the directory's transition check, the session log's live
  cutover rule and its checkpoint validator now all require the fence's epoch
  to be at least the implied one (the validator previously demanded exactly
  one above the active fence, which refused a copy's own checkpoint at reopen
  as `Corrupt`); activation sets the session's epoch to the fence's.
- Copies report `CaughtUp` from their own replica diagnostics; the agent's
  `Behind` and `Collect` errors are retryable; `CollectRequest` names one
  statement to be signed by a majority; the healer uses the partition's
  configured bounds.

**Tests.** `placement_agent_tests::the_controller_completes_a_plan_on_one_host_with_signed_readiness_and_fences`
(a one-host plan driven from `Planned` to `Activated` by the controller alone)
and `the_controller_expands_a_laptop_session_to_three_hosts_that_survive_one_loss`
(three real `NetworkService`s over QUIC: two hosts join and enroll with load;
the operator plans one tolerated node loss; the controller adds and promotes
both learners with signed group changes, commits the cutover, records the
promotions, collects the cutover and activation proofs and activates; the
directory holds the three-voter placement at route 2, the root grant names
three voters at membership epoch 3, `effective_guarantee` reports the desired
tolerance with no blockers, every host serves one installed copy; one host
stops and the founder still answers a quorum read and stays leader; the host
returns, reopens its copy from its own checkpoint and rejoins as a voter).
The ledger's placement lifecycle test now also runs with a cutover epoch above
the implied one, with and without a checkpoint, and refuses one below it.

**Evidence** (macOS arm64, this tree, 09:07–09:16 CDT):
`bash scripts/cargo.sh test --workspace --offline --no-fail-fast` — **2,424 tests across 102 test binaries, 0 failures**;
strict all-target Clippy, `scripts/check-production.sh`,
`cargo fmt --all --check` and the `--locked` check clean;
`python3 scripts/check-contracts.py` — 1,246 links, 37 hashes, 15 vocabularies.

**Limits recorded.** The controller does not transfer leadership to the
planner's preferred leader; the root grant's expiry is the earliest member
expiry; retirement is not yet gated on retention pins (R8); a session whose
membership changed before its first registration cannot register; remote
partition submits target the founder node; participant route discovery after
a placement change is R7.

## Tenant admission and the disk envelope (R6.5) — 2026-09-09

Instruction 5 of R6 ([24](24-placement-execution-and-fleet-control.md) §10;
decision F32): a session's admission is connected to the node's memory
budget, its volume and its custody staging, and a noisy tenant is throttled
by its own quota.

- **`focal_memory::DiskBudget`** (`crates/focal-memory/src/disk.rs`): one
  shared envelope per volume with no IO of its own. Owners sample the volume
  at the cadence it decides and report free bytes; every durable write is
  promised its bytes (`reserve(kind, lane, bytes)`) and refused with the new
  `MemoryError::DiskCapacity { requested, available }` before any
  acknowledgement; the headroom is never spent and the completion reserve
  only by completion-lane work; a reservation committed after its fence
  lowers the estimate until the next sample, one dropped returns its promise.
  `DiskCapacity` joins `Capacity`/`AllocationFailed` in every retryable
  classification (control RPC, ledger apply, node hosts, runtime).
- **WAL** (`focal-log`): `SharedWal::open_with_budgets` takes the envelope;
  every append and checkpoint-rewrite batch is promised in `batch()` and
  committed by the writer after `fsync` and the fence install (Raft snapshots
  are WAL records, so checkpoints are covered); `available_bytes` reports
  the envelope's unpromised free bytes, zero while the volume cannot be
  sampled; `disk_budget()` shares the envelope.
- **Content store** (`focal-evidence`): `ContentStore::open_with_disk`;
  staging bytes promised at `begin` and returned at `finish`, the sealed
  object promised at `seal` and committed once its manifest is installed,
  imported chunks and manifests and custody records promised on the
  completion lane and committed once installed; recovered uploads promise
  their remainder. `StoreLimits::domain_staging_bytes()` bounds one
  domain's staged bytes to half the staging allowance (never below one
  maximal upload), rebuilt from recovered uploads at open.
- **Node**: one envelope per node under the standard physical watermark
  (`network_service::disk_budget`), shared by the WAL and the content
  store; the load report's `disk_available` is the envelope's unpromised
  free bytes. `FOCAL_DISK_HEADROOM_BYTES` remains the native admission gate
  for fresh work only: raised above the volume's free space it still lets
  the node recover and commit its control plane (the A4 campaigns depend on
  that), which an envelope bound to it would refuse. `crates/focal-node/src/admission.rs`: `AdmissionPolicy`
  (`node.max_tenants`, default 8, at most 1024, validated by the
  configuration; 512 MiB allowance with a 128 MiB completion reserve per
  tenant) and `TenantAdmission` (the founder's tenant admitted at start;
  `admit(tenant, required_memory)` refuses `Tenants` at the bound and
  `Memory` when the node budget cannot fund the plan's requirement, answers
  an admitted tenant identically). `FleetManager::{is_admitted,
  admit_tenant, tenant_usage}` register a tenant on the running worker
  (scheduler quota, budgets, per-session slots) and report queue usage; the
  agent admits a tenant before its first copy and otherwise records
  `Refuse { NodeCapacity, node }` against its own assignment;
  `AgentStatus.admission` is an `AdmissionReport` (bound, node memory, the
  volume's free, promised and headroom bytes, and per tenant weight,
  allowance, use, sessions and queued items and bytes).

**Tests.** `focal-memory` `disk_tests` (unknown space, lanes and the
reserve, commit versus drop, sampling cadence and a failed probe, four
threads sharing one envelope); `focal-log` `a_batch_is_promised_its_volume_bytes_before_queueing_and_charged_after_its_fence`
(a watermark above the volume refuses an append before any record is
written and keeps nothing promised; the sample is charged by an append and
a checkpoint rewrite); `focal-evidence` `uploads_are_promised_volume_bytes_before_any_part_exists`
and `one_domain_cannot_fill_the_whole_staging_allowance`; `focal-node`
`admission::tests` (policy bounds, admission under the node budget until
the bound and identical re-admission, the report), `fleet::admission_tests::a_tenant_admitted_at_runtime_installs_its_sessions_under_its_own_quota`
(a second tenant's session is refused before admission with its candidate
returned, a foreign budget is not a tenant, admission is idempotent, the
session then installs and both tenants report usage), the founder agent test
asserting the admission report, and `config::tests::the_tenant_bound_is_optional_and_bounded`.

**Evidence** (macOS arm64, this tree, 10:18–10:26 CDT):
`bash scripts/cargo.sh test --workspace --offline --no-fail-fast` — **2,437 tests across 102 test binaries, 0 failures**;
strict all-target Clippy, `scripts/check-production.sh`,
`cargo fmt --all --check` and the `--locked` check clean;
`python3 scripts/check-contracts.py` — 1,257 links, 37 hashes, 15 vocabularies.

Two earlier runs of this batch (09:41–09:49 and 09:52–10:16) failed the A4
campaigns, because the envelope first took its headroom from the admission
knob and refused the node's own recovery writes, and then the client join
test, whose receipt check compared against the clock sampled before the
exchange (the race the node join path had already closed; `client_join.rs`
now resamples the clock too); the final tree was rerun in full.

**Limits recorded.** No path creates a session of a second tenant yet, so
the agent's refusal is exercised at the admission table and the fleet, not
through a foreign assignment (the operator batch adds session creation). The
per-tenant allowance is fixed, not derived from the plan's policy. Custody
capacity is bounded per domain at staging only. A promise and the sample
that already counts the bytes it protects can overlap between a commit and
the next sample, which is conservative, never optimistic.

## A node renews its own credential and every proof follows its key (R6.6) — 2026-09-09

Instruction 6 of R6 ([24](24-placement-execution-and-fleet-control.md) §11;
decision F33).

- **Identity by key.** `NodeEnrollment.identity` is the enrolled key's
  identity (`EnrollmentReceipt::public_key`); `focal_enrollment::certificate_key_hash`
  derives it from a certificate. The directory verifier, node grants, the
  control checkpoint's liveness check, the placement proof permits, the
  directory bootstrap, session registration and the root learner admission
  all compare keys now; the transport, contacts and the peer registry keep
  the certificate fingerprint.
- **Registry** (`focal-enrollment`, schema 2): `Change::Renew { invitation,
  receipt, retire_previous_at }`, a bounded `retired` table swept at every
  apply, `prepare_renew`/`release_renewal`/`authenticate_renewal`
  (`RenewRequest` signed by the holder's credential through the node
  statement path; `RenewPreparation::{Existing, Commit}`; a same-second
  renewal is refused), `authorize_certificate` through retirement,
  `retired(now)`, restore validation of the retired table,
  `EnrollmentCommand::renewed_invitation`, `JoinKey::renew`,
  `ServerTrust::{client_config, verify_quic}`, enrollment transport frame
  kind 3 (`JoinHandler::renew`, refused by join-only handlers;
  `EnrollmentClient::renew`), `CredentialMaterial: Clone`.
- **Node**: `credential_renewal.rs` (`CredentialHandle::{renew, current}`,
  `CredentialSummary`, `RenewalError`, `CredentialSwap`, the ten-day window,
  one-minute retry and the sponsor's grace knob); the network controller
  renews (automatic, on request, or when the committed registry is ahead of
  the receipt it holds), installs and swaps the listener identity
  (`ListenerIdentity`), the peer pool identity
  (`PeerConnectionPool::replace_identity`, `QuicConnector::replace_tls`) and
  the placement agent's credential (`AgentJob::Credentials`), re-announces
  its contact under the committed generation, and starts on a retired
  receipt; the founder's enrollment host serves renewals (`Action::Renew`,
  `RegisteredEnrollment::renew` waits for its own grant); contacts and
  grants keep retired certificates through their grace;
  `AdminCommand::RenewCredential`, `ClusterAdmin::renew_credential`,
  `AdminResult::CredentialRenewed`, CLI `cluster credentials renew`, MCP
  `cluster.credentials.renew` (33 administration tools; the cluster skill is
  version 3).
- **Contact retries.** A contact announcement retried after a lost reply is
  never byte-identical to the request the root retained (the root stamps its
  own decision time into the command), so the root answers `RetryConflict`;
  the controller used to end on it. It now reads a conflicting or
  compare-failed reply as an earlier attempt having committed and lets the
  next observation settle it. The renewal test exposed this under the full
  binary's load; the first announcement carried the same latent fault.

**Tests.** `focal-enrollment`: `a_renewal_keeps_the_key_and_identity_retires_the_old_certificate_after_grace_and_is_idempotent`,
`renewals_need_the_holder_s_own_key_and_a_live_unrevoked_enrollment`,
`a_node_renews_over_the_enrollment_transport_and_a_join_only_handler_refuses`;
`focal-node`: `credential_renewal::tests::a_joined_host_renews_its_credential_presents_it_everywhere_and_converges_after_a_crash`
(two real services over QUIC: the founder refuses to renew its own identity;
the joined host renews on request, holds a later expiry and a new fingerprint
under the same principal, the root's contact table carries the fingerprint it
now presents, its agent keeps its state; the host stops with the previous
receipt written back, starts on it and converges on the committed renewal by
itself; a later renewal commits again); the identity-by-key fixtures in the
directory, control, bootstrap, placement-proof and registration tests.

**Evidence** (macOS arm64, this tree, 11:30–11:38 CDT):
`bash scripts/cargo.sh test --workspace --offline --no-fail-fast` — **2,441 tests across 102 test binaries, 0 failures**;
strict all-target Clippy, `scripts/check-production.sh`,
`cargo fmt --all --check` and the `--locked` check clean;
`python3 scripts/check-contracts.py` — 1,261 links, 37 hashes, 15 vocabularies.

Two earlier runs of the batch on the same code: 11:00–11:08 failed the
renewal test itself under load, which exposed the contact-retry fault above,
and the known port-range race of `cli_network.rs`; 11:19–11:28 failed only
`fleet::async_tests::shared_owner_queues_covering_flush_and_serves_another_group_while_disk_waits`,
a timing assertion on a paused WAL writer that passes alone and in its whole
binary. The final run is clean.

**Limits recorded.** The founder's identity is not renewed; CA rotation and
client credential renewal are not implemented; key rotation is planned; the
stale-certificate refusal after grace is qualified in the registry, not
across two services (the grace is sixty seconds); the contact
re-announcement assumes every committed contact command advanced the node's
generation.

## Liveness: SWIM with Lifeguard, late extension and coordinates (R6.7) — 2026-09-09

Instruction 7 of R6 ([24](24-placement-execution-and-fleet-control.md) §12;
decision F34). The reference behaviours were taken from hyperscale's SWIM
package (local health multiplier, suspicion manager and state, gossip
buffer, Vivaldi coordinate engine, AD-26 extensions).

- **Directory** (`focal-directory`): `NodeLiveness { alive, incarnation,
  witness, decided_at }` on `NodeRecord` (`is_alive()`), partition checkpoint
  schema 3 (schemas 1 and 2 restore with no liveness known; `PartitionCheckpointV2`),
  `PartitionOperation::Liveness` applied only at the current enrollment
  generation with a non-decreasing incarnation and time (the same verdict, an
  older incarnation or an earlier time is `StaleNode`; a first "alive" is a
  `Duplicate`), `DirectoryError::DeadNode`, the planner skips dead nodes,
  `verify_placement` refuses a dead member, `BlockReason::DeadNode` in the
  guarantee. Tests `crates/focal-directory/tests/liveness.rs`.
- **Wire** (`focal-wire`): `Operation::Probe { request }` (tag 30, ≤ 8 KiB,
  Replication capability, certificate-bound), `Response::Probe`, accepted by
  the peer pool's `send_probe`; inventory row `peer.probe`.
- **Algorithms** (`focal-node/src/liveness/`): `health::LocalHealth` (score
  0..8, multiplier 1–3×), `suspicion::{Suspicion, ExtensionTracker}`
  (`max − (max − min)·ln(C+1)/ln(K+1)`, confirmations deduplicated, the
  originator never counts, logarithmic grants capped at five with witness,
  interval and overload rules), `gossip::GossipBuffer` (newest verdict per
  node, λ·log(n+1) rebroadcasts, least-broadcast first, 64 updates, 8 per
  probe), `coordinates::NetworkCoordinate` (Vivaldi, eight dimensions plus
  height, adjustment, error and gravity; `rtt_ucb_ms = rtt̂ + k_σ·σ` with
  conservative defaults below three samples).
- **Driver** (`liveness/driver.rs`): `LivenessHandle::channel(budget, config,
  node, namespace)` charges the whole state up front (640 B per member for
  1,024 members plus 96 KiB) and returns the handle the data service and the
  agent use and the driver the service runs; one tick per second probes the
  next member of a shuffled round with timeout `clamp(300 ms, 3·rtt_ucb,
  2 s)·lhm`, a timeout fans out up to three indirect probes through
  confirmed members, a member is confirmed by its first acknowledgement and
  suspected only when confirmed, outside a grace window and not already
  suspected, an expired suspicion is a death gossiped from this node, an
  acknowledgement or a higher incarnation revives; gossip about this node at
  its incarnation bumps the incarnation, raises the health score and, when
  the score is at or above two, queues an extension request to the accuser;
  late ticks raise the score; the view (`LivenessView`: members with status,
  incarnation, confirmation, suspicion originator, confirmations, grants and
  the timeout in force; health; coordinate; 32 events; counters) is published
  after every step. `ProbeRequest`/`ProbeReply` (`liveness/wire.rs`, schema
  1) are validated on decode (schema, sender, generation, finite coordinate,
  health bound, at most eight updates, nonzero ids); the data service answers
  `Probe` only for `PeerRole::Node` and refuses a probe whose sender is not
  the authenticated node.
- **Service and agent**: `NetworkHandles.liveness`; the driver runs beside
  the controller and the agent in `NetworkService::run`; the agent reports
  facts each tick (`LocalFacts { generation, members, witness = completed
  ticks, overloaded }`, overloaded when memory use reaches 95 % of the node
  budget or the volume's free bytes fall under the headroom) and, when it
  leads the partition, commits one settled verdict per tick (§12 rules).

**Tests.** `focal-node`:
`liveness_tests::a_stopped_host_is_committed_dead_by_the_partition_leader_and_revived_on_restart`
(founder plus two joined hosts over real QUIC confirm each other and learn
coordinates; no verdict exists for healthy hosts; a stopped host is probed,
suspected and declared dead in the founder's view and the partition commits
`alive = false` once; the other host stays alive; the stopped host restarts
with a fresh incarnation and the partition commits `alive = true` at the
higher incarnation without re-enrollment; the third host converges on the
same membership),
`liveness_tests::probes_bind_their_sender_refute_self_suspicion_and_ration_extensions`
(a probe for another node is refused; a direct probe is acknowledged with the
founder's generation and incarnation; an extension is granted once with the
configured minimum, rate-limited within one period, refused while the
requester reports overload, and counted in the view; a gossiped suspicion of
the founder at its incarnation is refuted by the next incarnation, the
refutation rides the answer, stale gossip changes nothing),
`liveness_tests::the_driver_refuses_an_unusable_configuration_and_charges_its_state`,
and the four algorithm tests in `liveness/algorithm_tests.rs`. The peer pool
accepting `Response::Probe` was the first fault the fleet test exposed (every
acknowledgement had been classified as lost); the second was starvation: the
session-log leader's replication retransmissions to the stopped host held
that peer's two inflight permits, so every probe of it returned `Busy` and
the leader could never suspect the host itself (it learned the death only
from a peer's gossip). Probes now travel on their own lane in the pool
(`PeerPoolLimits.max_probe_inflight`, sixteen overall and one per peer, the
connection still shared), so no data-plane traffic can starve the
detector. The first whole-workspace run
(12:16–12:24 CDT) aborted `focal-node`'s library binary with a stack
overflow in `network_service::tests::missing_runtime_drivers_fail_before_ingress_and_release_started_owners`:
the service's task set (`run_tasks`, 176 KiB of pinned driver state, 380 KiB
for the whole `run_until` future) is driven by `block_on` on a two-mebibyte
test thread, and the debug build's copies while constructing the nested
futures crossed that bound once the liveness driver joined the select.
`run_until` now pins the task set on the heap once for the service's life
(one allocation at start, no per-request or per-tick allocation), so the
caller's stack carries only its own frame; the measurement was taken with a
temporary size trace and removed.

**Evidence** (macOS arm64, this tree, 12:47–12:56 CDT):
`bash scripts/cargo.sh test --workspace --offline --no-fail-fast` — **2,451 tests across 103 test binaries, 0 failures**;
`bash scripts/cargo.sh clippy --workspace --all-targets --offline -- -D warnings` clean;
`bash scripts/check-production.sh` clean; `cargo fmt --all --check` and the
`--locked` check clean; `python3 scripts/check-contracts.py` clean. Earlier runs of this batch: 12:16–12:24
aborted `focal-node`'s library binary with the stack overflow described
above (every other binary passed); 12:28–12:36 failed the fleet liveness test
on the bounded event ring (replaced by the monotone counters) and, under
load, the two A4 campaigns (`cli_native_a4.rs`, `mcp_native_a4.rs`), which
pass alone and passed in the final run.

**Limits recorded.** See [24](24-placement-execution-and-fleet-control.md) §12:
node-local constants, flat membership, the founder never judged, the
extension path qualified by crafted probes rather than a loaded host.

## Namespace split and merge (R6.8) — 2026-09-09

Instruction 4's last clause of R6 ([24](24-placement-execution-and-fleet-control.md) §13):
"complete directory namespace split/merge and bounded delegated routing so
the control plane itself can scale without a single unbounded metadata
owner".

- **Directory** (`focal-directory`, partition checkpoint schema 4 with
  `PartitionCheckpointV3`/`PartitionSealV3` conversions): `PartitionSeal
  { .., moved, source }`, `PartitionOperation::{SealForSplit, Release,
  Install, Absorb}`, `split_image`, `split_partition_id`/`split_group_id`,
  `RootOperation::{Split, Merge}`, shape-validated activation fences
  (arrived / released / absorbed), `verify_delegation` scoped by
  containment or adjacency, `PartitionConfig.max_absorb_sessions`,
  `Delegation`/`DelegationFence` are `Copy`. Tests
  `crates/focal-directory/tests/split_merge.rs` (a split end to end with
  every refusal: wrong key, wrong geometry, tampered image, wrong or
  unverified release; a merge with the tampered and over-bound absorbs
  refused; a schema-3 checkpoint restoring with a whole-namespace seal).
- **Hosting** (`focal-node`): `PartitionPlan` (alias `FirstDirectoryPlan`)
  with `split_destination`, `bootstrap(image)`, `accepts`; permits and
  grants for delegated or image plans (`authorize_first_directory`,
  `next_first_directory_command`); `DirectoryHandle::{host_of,
  host_of_group, hosted, request}`, `HostRequest::{Host, Retire}`, the host
  manager in `network_directory.rs` with durable records under
  `cluster/partitions/`, `OWNER_SLOTS = 4 + MAX_HOSTED_PARTITIONS`,
  every hosted partition stopped at shutdown; `DataService` routes by
  group; `ControlHost::prepare_delegation_proof` and
  `placement_proof::prepare_delegation_proof`
  (`AuthorityFact::DelegationSource/Destination` signed as a voter of the
  named group).
- **Agent**: the tick visits every root delegation with one journal per
  partition (`Journals.partitions`), observes each partition once
  (`Observed`), reports liveness facts from the union of node tables, keys
  load reports by partition and skips a sealed partition's steps;
  `partition_split.rs` (`reshape`, `continue_split`, `absorb`,
  `delegation_proofs`, `thresholds`, `override_thresholds` under
  `test-support`) drives split and merge one committed step per tick.
- **Test** `crates/focal-node/src/partition_split_tests.rs::a_crowded_partition_splits_survives_a_restart_and_merges_back`
  (threshold one: the founder's session crowds the first partition, which
  seals at the session's key; the destination group is granted, hosted from
  the image, the root splits under both signatures, the destination installs
  and the source releases; both halves serve at epoch 2, the session lives
  above and its log keeps serving; the founder restarts and reopens the
  hosted partition from its record with the delegations untouched; with the
  merge allowed the upper seals, the root merges at epoch 3, the lower
  absorbs, the record is removed and the union stays under the threshold).
  Faults the test forced: a split image must present itself as the
  destination (the control genesis binds the bootstrap's group), the
  partition-level install/release/absorb need the same two proofs the root
  verified, a root permit must authorize a partition by identity rather
  than by its initial delegation (the first partition could not refresh its
  authority after a split), a partition whose only session sits at its start
  key has no split key, and every hosted partition must be stopped at
  shutdown or the owner registry never joins.

**Evidence** (macOS arm64, this tree, 14:15–14:24 CDT):
`bash scripts/cargo.sh test --workspace --offline --no-fail-fast` — **2,455 tests across 104 test binaries, 0 failures**;
`bash scripts/cargo.sh clippy --workspace --all-targets --offline -- -D warnings` clean;
`bash scripts/check-production.sh` clean; `cargo fmt --all --check` and the
`--locked` check clean; `python3 scripts/check-contracts.py` clean (1,272 links, 37 hashes, 15 vocabularies).

**Limits recorded.** See [24](24-placement-execution-and-fleet-control.md) §13:
partition groups on the founder alone, absorb bounded by 256 sessions, no
route cache, the split qualification at a threshold of one session.

## The route cache and serving fences (R6.9) — 2026-09-09

Instruction 4's "bounded delegated routing" and the carried limit "route
discovery after placement change" of R6 ([24](24-placement-execution-and-fleet-control.md) §14).

- **Directory** (`focal-directory`, partition checkpoint schema 5 with
  `PartitionCheckpointV4` conversions): `PartitionCheckpoint::{routes,
  routes_from}`, `RouteChange`, `PartitionConfig.max_route_log`,
  `DirectoryPartition::route_changes`, route-log validation (ordered, above
  the floor, several per revision), the log in the charge, an empty log in
  split images; `RouteCache::watches`. Tests
  `crates/focal-directory/tests/split_merge.rs::the_route_log_reports_exactly_what_moved_and_a_cache_too_far_behind_reads_a_gap`
  (creation changes, a watched revision, eviction and the gap batch, a
  cache applying it, a schema-4 checkpoint restoring with an empty log).
- **Control** (`focal-control`): `ControlRead::{Route, RouteChanges}`,
  `ControlReadResult::{Route, RouteChanges}`, partition-scope reads with
  their charges, allowed through the read-only peer decoder.
- **Wire**: `PeerConnectionPool::route_endpoint`.
- **Node**: `route_cache_host.rs` (`RouteCacheHandle::{channel, resolve,
  hint}`, `RouteCacheDriver::run` beside the other drivers,
  `NetworkHandles.routes`); `ManagedService::with_routes` and `redirect`
  (client requests only; no copy → the directory's hint; a stale epoch →
  the current epoch at the replica's leader; a mutation on a follower → the
  leader; current reads served here); `DataService` exposed to the crate's
  tests through `Running.data`.
- **Tests** `crates/focal-node/src/route_cache_tests.rs::a_node_that_does_not_serve_a_ledger_redirects_to_its_leader`
  (the partition answers `Route` for the founder's session and `None` for
  an unknown one, `RouteChanges` names it at epoch 1; a joined host with no
  copy answers a `Summary` read with `RouteChanged` to the founder's
  endpoint at epoch 1 while the founder serves it; an unknown ledger stays
  unavailable) and, in
  `placement_agent::tests::the_controller_expands_a_laptop_session_to_three_hosts_that_survive_one_loss`,
  after activation at route epoch 2: a client at epoch 1 is answered with
  epoch 2 at the founder's endpoint by the founder and by a follower, and a
  current client is served without a redirect. The fault the expansion test
  exposed: routing every request through the directory (a diagnostics call
  and a cache lookup per Raft message) delayed heartbeat responses until
  the leader stepped down after one host loss; only client requests are
  routed now, and a current read never consults the replica's leader.

**Evidence** (macOS arm64, this tree, 14:53–15:02 CDT):
`bash scripts/cargo.sh test --workspace --offline --no-fail-fast` — **2,457 tests across 104 test binaries, 0 failures**;
`bash scripts/cargo.sh clippy --workspace --all-targets --offline -- -D warnings` clean;
`bash scripts/check-production.sh` clean; `cargo fmt --all --check` and the
`--locked` check clean; `python3 scripts/check-contracts.py` clean.

**Limits recorded.** See [24](24-placement-execution-and-fleet-control.md) §14.

## The operator's placement view (R6.10) — 2026-09-09

Instruction 5 of R6 ([24](24-placement-execution-and-fleet-control.md) §15):
`cluster placement` and `cluster plan` on the CLI and MCP.

- **Client** (`focal-client`): `AdminResult::{Placement, Plan}`,
  `AdminPlacement`, `AdminPartition`, `AdminSeal`, `AdminPlacementNode`,
  `AdminSessionPlacement`, `AdminPendingPlacement`,
  `AdminAssignmentProgress`, `AdminPlannedAction`; descriptors
  `cluster.placement` and `cluster.plan` (`ADMIN_TOOL_COUNT` 35).
- **Node**: `AgentJob::Directory` / `PlacementHandle::directory` answer with
  the agent's last observation (`DirectoryReport`); `AdminCommand::Placement`
  served by `LocalNetworkAdmin::placement` (`with_placement`) through the
  projection `placement_reply` (bounded, guarantee measured by
  `effective_guarantee`, actions by `placement_controller::planned_actions`
  and the partition's reshape state); `ClusterAdmin::{placement, plan}`;
  CLI `cluster placement|plan`; MCP `AdminAction::{Placement, Plan}`.
- **Contracts**: the cluster skill (version 4, digest re-pinned) documents
  both tools; the manifest requires them; the MCP catalogue budget test uses
  the production envelope (160 MiB / 80 MiB) now that the catalogue carries
  35 administration tools; `docs/cluster-admin.md` lists both commands.
- **Test**: `placement_agent::tests::founder_agent_registers_its_session_reports_load_and_restarts_without_repeating`
  reads the view through the admin socket: one partition at epoch 1, the
  founder alive and loaded, its session at the single-node guarantee with
  nothing blocking and no pending plan, and an empty plan. The fault the
  test exposed: the agent's tick timer was recreated after every job it
  served, so a client polling the view every hundred milliseconds starved
  the tick and the view never advanced; the tick is a fixed deadline now.
  The whole-workspace run then exposed a second fault: the split test's
  in-process threshold knob was process-global, so the founder test running
  beside it in the same binary saw its own partition split; the knob is
  keyed by cluster now (`override_thresholds(cluster, split, merge)`,
  `thresholds(cluster)`).

**Evidence** (macOS arm64, this tree, 15:27–15:36 CDT):
`bash scripts/cargo.sh test --workspace --offline --no-fail-fast` — **2,457 tests across 104 test binaries, 0 failures**;
`bash scripts/cargo.sh clippy --workspace --all-targets --offline -- -D warnings` clean;
`bash scripts/check-production.sh` clean; `cargo fmt --all --check` and the
`--locked` check clean; `python3 scripts/check-contracts.py` clean. The 15:15–15:24 run failed the founder
test on the process-global knob described above and the known
`fleet::async_tests::shared_owner_queues_covering_flush_and_serves_another_group_while_disk_waits`
load flake, which passes alone.

**Limits recorded.** See [24](24-placement-execution-and-fleet-control.md) §15.

## Tenants and application sessions (R6.11) — 2026-09-09

The last R6 instruction before the real-binary qualification
([24](24-placement-execution-and-fleet-control.md) §16, decision F37): a
cluster serves more than the founder's tenant, and an operator creates
application sessions by name.

- **Enrollment** (`focal-enrollment`): registry schema 3 with
  `tenants: BTreeSet<[u8;16]>`, `Change::AdmitTenant`, `EnrollmentLimits.max_tenants`
  (1,024; 64 bytes charged per tenant), `prepare_admit_tenant` (founder
  authority; `Conflict` when admitted, `Capacity` when full),
  `tenants()`/`admits_tenant()`, `EnrollmentCommand::admitted_tenant`; a
  schema-2 checkpoint restores with no tenants and its own charge.
- **Node grants**: `QuorumEnrollmentHost::admit_tenant` commits the fact
  through the same journaled control path as an invitation and reads it
  back before answering; `authorize` issues every certificate grant as the
  configured tenants plus the registry's. The local Unix socket is bound
  with a watched grant (`UnixServer::bind_watched`; each accepted connection
  is served under the value at accept) that the network controller
  republishes on every registry refresh (`follow_local_grant`), so admission
  never needs a restart.
- **Sessions**: `AdminCommand::CreateSession{tenant, name}` →
  `PlacementHandle::create_session` → the agent derives the identity
  (`created_session_id`, `focal.session.created.v1` over cluster, tenant,
  length-prefixed name), answers `existing` when the fleet hosts it, admits
  the tenant (`TenantAdmission::admit` + fleet), records the copy as
  `created` in `cluster/placement-installs` (schema 2; schema 1 converts)
  before opening it, and opens a single-voter log on the shared WAL at the
  session's own group. Registration is generalised: `FirstSessionPlan::capture_hosted`
  registers any session a node hosts alone under the node's identity at
  that ledger, and the agent's tick runs it for the founder's session and
  every created one whose namespace the partition holds. The partition
  checkpoint is schema 6: `SessionDescriptor.founder` records the founding
  node so an assigned copy replays that exact bootstrap membership
  (schemas 1–5 convert with `None`, read as the cluster founder).
- **Surfaces**: `AdminCommand::{AdmitTenant, Tenants, CreateSession}`
  (`TenantsReply`, `SessionCreatedReply`), `ClusterAdmin::{admit_tenant,
  tenants, create_session}` (the client recomputes the expected session
  identity and refuses any other answer), `AdminResult::{Tenants,
  SessionCreated}`, `AdminSessionPlacement.founder`; CLI `cluster tenants
  admit|list`, `cluster sessions create`; MCP `AdminAction::{AdmitTenant,
  Tenants, CreateSession}` and descriptors `cluster.tenants.admit|list`,
  `cluster.sessions.create` (`ADMIN_TOOL_COUNT` 38); cluster skill version 5
  (digest re-pinned), manifest, `docs/cluster-admin.md`.
- **Tests**: `focal-enrollment` admission/restore/upgrade; `focal-wire`
  `unix_watched_grant_governs_connections_accepted_after_it_changes`;
  `focal-directory` schema-5 conversion; node `network_control` (admit once,
  retry is done, zero refused, the grant names the tenant),
  `session_registration` (hosted capture: distinct identities, refusals),
  `placement_agent::tests::{created_session_identity_is_exact_per_cluster_tenant_and_name,
  install_records_before_created_sessions_decode_as_assigned_copies,
  operators_admit_tenants_and_create_sessions_that_register_serve_and_survive_restart}`
  (an unserved tenant is refused on both the admin and the local socket;
  admit, retry, list; create, exact retry, a second session under the
  founder's tenant; both registered with `founder = node`; served through
  the data handler and through the local socket without a restart; the
  operator view names the founder; restart reopens both and the name still
  finds the session). The restart half exposed one fault: a copy recorded
  as installed was reopened before its tenant was admitted on the fresh
  agent; `open_copy` now admits the recorded copy's tenant itself.

**Evidence** (macOS arm64, this tree, 16:34–16:42 CDT):
`bash scripts/cargo.sh test --workspace --offline --no-fail-fast` — **2,462 tests across 104 test binaries, 0 failures**;
`bash scripts/cargo.sh clippy --workspace --all-targets --offline -- -D warnings` clean;
`bash scripts/check-production.sh` clean; `cargo fmt --all --check` and the
`--locked` check clean; `python3 scripts/check-contracts.py` clean. The
16:23–16:31 run before it failed three ways on this batch: the directory's
`CreateSession` refused a placement with several voters (the founding node is
now recorded only for a single-voter creation), an indexed slice pair in the
tenants reply check, and a clippy default-reassignment in the enrollment
test; plus the known
`fleet::async_tests::shared_owner_queues_covering_flush_and_serves_another_group_while_disk_waits`
load flake, which passed in the clean run.

**Limits recorded.** See [24](24-placement-execution-and-fleet-control.md) §16.

## An operator's durability request across three real processes (R6.12) — 2026-09-09

The R6 close ([24](24-placement-execution-and-fleet-control.md) §17): the
lever an operator needs to expand a laptop session, and the real-binary
qualification the plan names for R6.

- **Request**: `AdminCommand::PlanSession{tenant, session, survive, max_failures}`
  → `PlacementHandle::plan_session` → `AgentJob::PlanSession` queued in the
  agent (`MAX_PLAN_REQUESTS` 64 sessions, 8 waiters each; a request under
  another durability replaces the queued one and its waiters are told) and
  answered by `answer_plan_request` on the next pass over the partition
  holding the session: `pending` (the plan under way), `satisfied` (the
  active policy carries the durability and verifies), or `planned` (a
  `SessionChange::Plan` journaled for the partition under
  `propose_placement` with the active policy's residency and the requested
  durability, identity `focal.placement.request.v1` over ledger, authority
  record and durability). `SessionPlannedReply`, `ClusterAdmin::plan_session`,
  `AdminResult::SessionPlanned`, CLI `cluster sessions plan`, MCP
  `cluster.sessions.plan` (`ADMIN_TOOL_COUNT` 39), skill version 6.
- **Local socket redirects**: the Unix client transport resends at a hinted
  epoch to the same node instead of refusing the hint; the node answers the
  same hint again when its log leads elsewhere, which the client reports.
  Without this a local `status` on the founder failed with `route_changed`
  after the expansion moved its own session to epoch 2.
- **Re-fencing after activation**: a hosted replica served clients only at
  the route it was installed with (`serves_route`), and nothing moved that
  fence when the directory activated a new route, so every client request
  to an expanded session was refused `Unavailable` at every host (the
  in-process expansion test had tolerated that answer). `ReplicaHost::refence`
  (`Work::Refence`, control lane) moves the serving fence and the read
  views' epoch once the session's active route is that route and no
  cutover is pending (`ReadViews::set_route_epoch` keeps the read clock; a
  fresh view set had run the clock backwards and stopped the owner);
  `ReplicaProgress.route_epoch` reports the served route, and the agent's
  `sync_custody` re-fences every copy it hosts whose served route is behind
  the committed one. The expansion test now requires a `Summary` answer at
  epoch 2 from the founder.
- **Test** `crates/focal-node/tests/placement_binary.rs::a_laptop_session_expands_to_three_processes_and_converges_after_its_leader_is_killed_mid_plan`:
  three real `focal` processes (founder with `--advertise`, two hosts
  invited with `cluster invite`, joined with `join`, started), enrollment
  and load observed through `cluster placement`, `cluster sessions plan`
  for one tolerated node loss (`planned` with the three voters; the retry
  names the same operation as `planned` or `pending`), the founder killed
  with SIGKILL as soon as the pending plan leaves `Planned` (or activated),
  restarted, and activation observed from the committed directory (no
  pending plan, route epoch 2, membership epoch 3, placement epoch 2,
  three voters, `achieved_max_failures` 1, nothing blocking, nothing
  retiring, founder recorded, `satisfied` on the same request, an empty
  `cluster plan`); host-b killed (a `status` quorum read still answers
  through the founder's local socket; the directory suspects the host and
  measures no tolerated failure), then restarted (alive, the guarantee
  whole, no pending plan).

**Evidence** (macOS arm64, this tree, 17:19–17:28 CDT):
`bash scripts/cargo.sh test --workspace --offline --no-fail-fast` — **2,463 tests across 105 test binaries, 0 failures** (the real-binary placement qualification runs in about a minute);
`bash scripts/cargo.sh clippy --workspace --all-targets --offline -- -D warnings` clean;
`bash scripts/check-production.sh` clean; `cargo fmt --all --check` and the
`--locked` check clean; `python3 scripts/check-contracts.py` clean. The
17:08–17:18 run before it passed every test and failed clippy on two
elidable lifetimes in the new test file, fixed for this run.

**Limits recorded.** See [24](24-placement-execution-and-fleet-control.md) §17.

## Deterministic parallel materialization (R7.1) — 2026-09-09

The first R7 batch ([25](25-parallel-materialization-and-ranges.md) §2,
decision F38): committed records this session did not author are
materialized in dependency waves on bounded workers, read-traced, checked at
a barrier and installed in order, byte-identical to serial replay.

- **Core** (`focal-core`): `record_codec::replay` gains the `BaseRows` seam
  (every base read of a replay goes through it) and splits `prepare` into
  `stage` (decode, custody, validate against any base view → `StagedRecord`)
  and `install` (pages against the published root); `stage_meta` decodes a
  record's meta row alone. New `record_codec::materialize`:
  `MaterializerLimits`, `BatchReport`, `materialize_batch` (footprints from
  header keys, `Affinity` per key, edges by affinity and exact key with the
  meta row excluded, pre-decoded meta rows, waves on scoped threads with a
  fixed stack, per-task `TaskBase` view and trace, the barrier, discard and
  serial completion, in-order install and publish, the first refusal at its
  index). `checkpoint::rows_digest` hashes the rows under a fixed range
  identity. `NativeSchemaVerifier` is `Sync` (shared with workers); test
  verifiers count through `fixtures::SyncCell`.
- **Ledger** (`focal-ledger`): `NativeSessionLimits.materializer`,
  `MaterializerStats` (`NativeSession::materializer_stats`),
  `NativeSession::native_state_digest`; the follower delivery loop batches a
  run of consecutive native records up to the next membership entry
  (`record_run`, `materialize_run`) when the session holds no candidates and
  more than one worker is configured; the per-record path is otherwise
  unchanged.
- **Node**: hosted sessions run `max_workers = available_parallelism().clamp(1, 4)`.
- **Tests**: `materialize::tests` (affinity, intersections, latest writer,
  limits); `native_session::cluster_tests::{parallel_materialization_matches_serial_replay_on_independent_and_dependent_records,
  a_planner_that_omits_every_edge_is_caught_by_the_barrier_and_still_matches}`
  (digests equal across a one-worker and a four-worker follower over
  independent creations, dependent posts and receipts, and a restart replay;
  a no-edge planner with eight workers stages a post beside its creation,
  the barrier reports violations and a serial fallback, rows still match).
  Two faults the suite found: the meta row every record writes chained every
  pair of records into serial waves until it was excluded from key edges;
  and the checkpoint frame hash includes the store's range identity, which
  differs per replica, so the digest hashes rows under a fixed identity.
- **Measurement** (P10.7): `materializer_throughput_at_one_and_four_workers`
  (`--ignored`, release) writes `target/measurements/materializer-<time>.json`;
  eight independent records took 910 µs on the serial path and 620 µs in two
  four-worker waves, a chained shape 928 µs against 913 µs
  ([25](25-parallel-materialization-and-ranges.md) §2). `MaterializerStats`
  also counts the per-record path (`serial_records`, `serial_micros`).

**Evidence** (macOS arm64, this tree, 17:52–18:02 CDT):
`bash scripts/cargo.sh test --workspace --offline --no-fail-fast` — **2,468 tests across 105 test binaries, 0 failures**;
`bash scripts/cargo.sh clippy --workspace --all-targets --offline -- -D warnings` clean;
`bash scripts/check-production.sh` clean; `cargo fmt --all --check` and the
`--locked` check clean; `python3 scripts/check-contracts.py` clean.

**Limits recorded.** See [25](25-parallel-materialization-and-ranges.md) §2.

## The storage layout order (R7.2, first step) — 2026-09-09

The first sharding step ([25](25-parallel-materialization-and-ranges.md) §3,
decision F39): one order for every native key, affinity then family then
fields, so an object's rows are one contiguous span.

- **Core**: `native::layout` (`affinity`, `family`, the slot-sequence
  `OrderKey`, `impl Ord for Key`); `Key` no longer derives its order.
  `FCMUTATE` and `FCNROOTS` advance to version 5 (rows in this order; no row
  bytes change), so the native decoder identity changes with them.
- **Tests**: `layout::tests` (a corpus over all 48 families: the order is
  total, agrees with equality, transitive on every triple, ends at the
  sentinel; one claim's rows are contiguous, its cycles and evaluations
  ordered by their fields, a status bucket holds its claims together, due
  timers sit in time order under the control affinity). Three fixtures that
  copied only the rows they expected beside a forged key now copy every
  neighbour, since a claim's other rows share its span; the version
  assertions follow the constants. One fault the suite found: an order key
  that compared all identities before all scalars let a timer's target
  outrank its time, so a due scan's bound stopped nothing; the order key is
  now one slot sequence in declaration order.
- **Identity index**: the affinity-first order scatters a family's rows
  across claim spans, so the identity-order listings (`claims`, `artifacts`,
  `definitions`, and unrestricted `evaluations`, which now nests each
  claim's evaluations under its identity) walk a new unit family,
  `ByObject(family code, object)` (tag 48; [22](22-native-record-format.md)
  §2 and §7), written with the claim, artifact or declaration row, replayed,
  checkpointed and imported like every other index row. Receipts stay one
  table through their affinity. The write-set bounds count the new row:
  `ARTIFACT_FIXED_ROWS` is four (identity, producer, kind, schema), the
  smallest failed admission report eighteen rows, a projection-only claim's
  index rows six; the completion, admission-graph, work and respondent
  envelopes derive their fixed rows from `report_rows(0)`/`artifact_rows(0)`
  instead of literals, so the promised artifact's input cap and the actual
  batch agree. The full workspace gate (gate68) caught the omission first:
  every node list and watch scan assumed family contiguity; the crate's own
  suites then caught two family tables (`mutation::check_family`,
  `read_validate`) that refused the new row as an invalid manifest, and
  eleven exact-count tests moved with the bounds.

**Evidence** (macOS arm64, this tree, 2026-09-09 19:02–19:12 CDT, gate69):
`bash scripts/cargo.sh test --workspace --offline --no-fail-fast` ran 105
test binaries, 2,469 passed, 1 failed: `fleet::async_tests::shared_owner_queues_covering_flush_and_serves_another_group_while_disk_waits`,
the known timing-sensitive fleet test, which passed when rerun alone on the
same tree (`--lib -- fleet::async_tests::shared_owner_queues_covering_flush`,
1 passed). `clippy --workspace --all-targets --offline -- -D warnings`,
`scripts/check-production.sh`, `cargo fmt --all --check` and
`cargo check --workspace --all-targets --locked --offline` were clean;
`python3 scripts/check-contracts.py` verified 1,313 links, 37 hashes and 15
vocabularies. Earlier on this tree: `test -p focal-core -p focal-ledger
-p focal-evidence` (839 + 103 + 41 passed) and the four node suites gate68
had failed (`cli_native_a3`, `cli_native_lists`, `cli_native_watch`,
`watch_client`: 1, 1, 1 and 5 passed).

**Limits recorded.** See [25](25-parallel-materialization-and-ranges.md) §3.

## Range groups (R7.2, second step) — 2026-09-09

The second sharding step ([25](25-parallel-materialization-and-ranges.md)
§4, decision F40): a native state's rows are a group of stores over affinity
spans.

- **Memory crate**: the generic range map (`range_map.rs`: `KeySpan<K>`,
  `RangeDescriptor<K, M>`, `RangeMap<K, M>` with gap-free validation, routing
  and one contiguous replacement under generation rules; an optional `serde`
  feature; the dormant `focal-ranges` crate now builds on it with a
  `Placement` meta and the memory crate's `RangeId`), page-sharing division
  and joining of stores (`range_split.rs`: `split_with`, `split_where_with`
  under a monotone predicate, `merge_with`, `new_sibling`, `is_sibling`; one
  directory build per result, `PageDirectory::from_pages`), group write
  envelopes (`future_write_envelope_shared`, `check_plans` over a set of
  fragment plans, `with_group_bytes`), `project_from` on a lease and a
  `budget()` accessor.
- **Core**: `native::ranges` (`RangeLayout`, `NativeRanges`, `RangesPlan`,
  `Fragments`, `RangeLeases`, `Affinity`, `MAX_LAYOUT_MEMBERS`); the native
  state holds a group, a prepared candidate holds one fragment per member, a
  read one lease per member; every plan, build, publication, chain check,
  scan and projection routes by affinity; `NativeLimits.max_ranges` (64)
  sizes every envelope; `Core::{native_layout, split_native_range,
  merge_native_range}`. `FCNROOTS` advances to version 6 and carries the
  layout; `StructuralCheckpoint::{members, layout}`; restoration hydrates
  one store then divides it per the layout; the rows digest frames a
  canonical layout; import images name one member under the import identity.
  Replay funds the extra input vectors a divided write needs.
- **Ledger**: `NativeSession::{native_layout, split_native_range,
  merge_native_range}` through the owner (refused with candidates pending)
  and the engine (refused during a delivery).
- **Tests**: memory `range_map::tests` (4), `range::split_tests` (5: page
  sharing, boundary pages, page-edge and past-the-end boundaries, merge
  refusals, shared clock and envelope, bulk directory shapes at eight widths);
  core `ranges::tests` (4: one versus five members hold identical state and
  serve identical reads, pins across members, refusals under leases and at
  boundaries, split and merge with rows; checkpoints carry the layout and
  restore durable member identities under a fresh producer; records replay
  into a differently laid-out incarnation; layout frames refuse disorder,
  repeated identities, a bounded first member, no members and any changed
  byte); ledger `replicas_lay_rows_out_independently_and_checkpoints_carry_the_layout`
  (one-, three- and four-member replicas byte-identical, a split refused with
  a candidate pending, a lagging follower adopting the leader's layout from
  its checkpoint, a restart restoring a replica's own, a merge, further
  records). Two faults the suites found: the first design allocated the
  group's per-write vectors from the core budget outside the funded
  completion envelope (every under-pressure owner test refused), so a group
  of one now allocates nothing per write and a wider group's vectors are
  funded by the write's source within the envelope's group bytes; and
  replaying a record into a divided group needs a vector per touched member
  beyond the one the record funded.

**Evidence** (macOS arm64, this tree, 2026-09-09 20:07–20:17 CDT, gate71):
`bash scripts/cargo.sh test --workspace --offline --no-fail-fast` ran 105
test binaries, 2,484 passed, 0 failed; `clippy --workspace --all-targets
--offline -- -D warnings`, `scripts/check-production.sh`, `cargo fmt --all
--check` and `cargo check --workspace --all-targets --locked --offline` were
clean; `python3 scripts/check-contracts.py` verified 1,327 links, 37 hashes
and 15 vocabularies. The run before it (gate70, 19:55–20:05) passed the same
2,484 tests but clippy refused four findings in the new code (a redundant
closure, a large `Err` variant on the all-or-nothing publication, a bare
remainder in the layout reader, a `&Vec` parameter in a test copier); they
were fixed and the whole gate rerun.

**Limits recorded.** See [25](25-parallel-materialization-and-ranges.md) §4.

## Committed layout changes (R7.3, first step) — 2026-09-09

The layout of a session's range group becomes a session decision
([25](25-parallel-materialization-and-ranges.md) §4; decision F40 revised).

- **Ledger**: `native_session_range.rs` (`FOCALRG1`: `LayoutRecord {ledger,
  expected_epoch, operation}` with `LayoutOperation::{Split {at, id}, Merge
  {left}}`, 115 fixed bytes under a digest; `origin_member(genesis)`);
  `NativeSession::{propose_layout, native_layout, native_layout_epoch}`
  replace the replica-local split and merge of the second step; the engine
  keeps the record in flight (`layout_change`), refuses native admission with
  the new retryable `NativeSessionError::LayoutChanging` while it is, refuses
  a change while candidates are pending or another change is in flight, and
  lets an uncommitted record go on a term change; `apply_entry` applies the
  record between native records (inert when its epoch has passed, the same
  split or merge on every replica otherwise, `SuffixEvidence::LayoutChanged`
  ending any candidates an authority still holds, a refusal fail-closed);
  `apply_genesis` names the origin member from the genesis;
  `is_native_entry` covers the record so the hosting Session routes it and
  the materializer's batches end at it.
- **Core**: `RangeLayout` carries an `epoch` (one more per applied change,
  written and read with the checkpoint layout section, so `ROW_START` in the
  checkpoint tests is now 104), `check_split`/`check_merge` pre-check a
  change, `rename` gives a member its durable identity, split and merge no
  longer refuse under leases: leases of an earlier epoch expire with the
  members they pinned and are released by member identity; `Core::{rename_native_member}`,
  owner `rename_native_member`.
- **Tests**: ledger `range::tests::layout_records_round_trip_and_refuse_every_forgery`
  and `committed_layout_changes_apply_on_every_replica_and_fence_proposals`
  (a follower cannot propose; an inapplicable merge is refused before the
  log; a change waits for a pending candidate; a change in flight refuses a
  second change and a native proposal; all three replicas land on one layout
  and epoch; records flow over it; a lagging follower installs a checkpoint
  at epoch 1 then replays the record to epoch 2 and the records after it; a
  restart replays the records; a merge; a handover to a new authority that
  changes the layout again); core `ranges::tests` extended for epochs and
  for leases across a layout change.

**Evidence** (macOS arm64, this tree, 2026-09-09 20:26–20:36 CDT, gate72):
`bash scripts/cargo.sh test --workspace --offline --no-fail-fast` ran 105
test binaries, 2,485 passed, 0 failed; `clippy --workspace --all-targets
--offline -- -D warnings`, `scripts/check-production.sh`, `cargo fmt --all
--check` and `cargo check --workspace --all-targets --locked --offline` were
clean; `python3 scripts/check-contracts.py` verified 1,331 links, 37 hashes
and 15 vocabularies.

## Chunked checkpoint seeds (R7.3, second step, 2026-09-09)

**Scope.** The consensus checkpoint buffer keeps its explicit limit; a native
Core root beyond the inline bound is sealed as content-addressed 1 MiB seed
chunks while it streams and the Raft snapshot carries only their manifest
([25](25-parallel-materialization-and-ranges.md) §5; decision F41;
`FCNSESS1` version 4, [22](22-native-record-format.md) §2). A replica
retains a seeded snapshot until it holds every chunk, pulling the missing
ones from a peer of the ledger's installed placement or of a placement the
directory is preparing; hosted replicas checkpoint on an operator's request;
and the real-binary expansion of a native session is qualified end to end,
which closed three gaps in native expansion that the in-process suites had
bypassed.

- **Evidence**: `seeds.rs` (`SeedStore`/`SeedReader`, `<hash>.seed` files
  written through a temporary, fsynced and renamed, charged to the node's
  `DiskBudget` as checkpoint bytes, verified on install and on read;
  `SEED_CHUNK_BYTES` = 1 MiB); `WalLease::disk_budget`,
  `DurableNode::disk_budget`.
- **Ledger**: the session envelope's form byte (inline / seeded chunk table;
  `Limits {inline_bytes 4 MiB, assembled_bytes 256 MiB}`;
  `EncodingPlan::encode_in_seeded` streams the root through one chunk
  buffer and the inline paths refuse a seeded plan with `Error::Seeded`);
  `Checkpoint::describe → Option<SeedManifest>` (`None` is inline),
  `SeedManifest::{missing, assemble}`, `Checkpoint::inspect_seeded`; the
  engine records `PendingSeed {index, term, missing}` and answers the install
  with a retryable `CustodyPending`; the hosting `Session` keeps the missing
  list itself when the rebuilt engine is not adopted
  (`seed_pending`, `pending_seed`, `install_seed_chunk`, `seed_reader`,
  `seed_waiting`: a retained delivery is not resumed until a chunk lands);
  `Session::{delivery_retained, snapshot_index}`; a replica whose durable
  floor names the native successor takes part in the support exchange after
  a restart (`managed_support_demanded`).
- **Wire, client, node**: `CustodyRequest::SeedChunk {hash, max_bytes}` /
  `CustodyReply::SeedChunk`; `CustodyStore::{announce_pending,
  authorize_seed}` (the placement agent announces a pending plan's scope and
  peers to its content host, `ContentHost::announce_pending`, and withdraws
  it; custody requests authorize inside the store); a peer without the seed
  answers a definite refusal; `evidence_service::pull_seed`; fleet
  `Work::{SeedChunks, InstallSeed, Checkpoint}`,
  `ReplicaHost::{pending_seed_chunks, install_seed_chunk, checkpoint}`,
  `ReplicaProgress.seed_pending`, `managed_support::service_seed`; the
  authority checkpoints once an `AddLearner` it proposed has applied behind
  a compacted log (`checkpoint_due`: Raft discards a snapshot whose
  configuration does not name the recipient); a hosted replica answers a
  `ManagedSupport` probe with its successor promise (`native_support`),
  which is how a native authority admits a prospective learner; the grouped
  worker reports a stopped session's reason on standard error; replica
  diagnostics gain `seed_chunks_missing` and `delivery_retained`; the
  replica admin protocol and `focal cluster replicas checkpoint` take an
  explicit synchronous checkpoint; `FOCAL_SEED_INLINE_BYTES` moves the
  inline bound for qualification.
- **Tests**: evidence `seeds_are_sealed_read_back_verified_and_removed_only_on_purpose`;
  ledger `a_root_beyond_the_inline_bound_is_seeded_and_assembled_back_exactly`
  (plus the form byte in the forgery test), cluster
  `a_seeded_checkpoint_installs_once_its_chunks_are_pulled` and
  `a_checkpoint_beyond_one_seed_chunk_installs_only_when_every_chunk_is_local`
  (a root past one chunk: every chunk but the last exactly 1 MiB, the
  delivery retained while one is missing, byte-identical state after), and
  the unified Session's
  `a_lagging_replica_installs_a_seeded_ss6_checkpoint_once_its_host_pulls_the_chunks`;
  node `seed_chunks_are_served_to_installed_peers_and_announced_pending_peers_only`
  and the real-binary
  `a_seeded_native_checkpoint_carries_the_founder_session_to_new_hosts`
  (offline native activation, a 64-byte inline bound, an explicit
  checkpoint sealing seeds, the founder killed and restored from its own
  seeds, two hosts joined and one tolerated node loss requested: activation
  at route epoch 2 proves each copy pulled every chunk under the pending
  announcement and assembled the root; every host holds seeds the founder
  sealed).
- **Docs**: [25](25-parallel-materialization-and-ranges.md) §5,
  [22](22-native-record-format.md) §2, [23](23-native-activation-and-import.md)
  §3 (items 7–8), [24](24-placement-execution-and-fleet-control.md) §8,
  [07](07-decisions-and-traceability.md) F41, [05](05-implementation-plan.md)
  P11.5, [cluster-admin.md](../cluster-admin.md), REMAINING R7 progress.

**Evidence** (macOS arm64, this tree, 2026-09-09 22:32–22:42 CDT, gate74):
`bash scripts/cargo.sh test --workspace --offline --no-fail-fast` ran 105
test binaries, 2,491 passed, 1 failed:
`placed_copies_artifact_attachment_and_cold_leader_pull_use_real_quic`
(`evidence_quic.rs`), whose first submit answered `OutcomeUnknown` under the
full-workspace load before the test's retry loop; the binary passed three
of three reruns alone on the same tree, and gate73 (22:18–22:28 CDT, the
same behaviour before three clippy-only refactors) passed all 2,492.
`clippy --workspace --all-targets --offline -- -D warnings`,
`scripts/check-production.sh`, `cargo fmt --all --check` and `cargo check
--workspace --all-targets --locked --offline` were clean; `python3
scripts/check-contracts.py` verified 1,354 links, 37 hashes and 15
vocabularies.

**Follow-up (2026-09-09, after gate74).** The flake was the test's own
contract: five submits (`OpenEpoch` on both actors, `GenerateClaim`,
`PostClaim`, `AcquireReceipt`, `BeginEvidenceSet`) and the final
linearizable artifact read asserted a committed reply from one send, while
the wire allows a replica under load to answer a fresh request with
`OutcomeUnknown` inside its request timeout — the honest reply the test's
`eventual` helper exists to retry with the identical envelope. Those sites
now retry exactly like every other submit in the file; no product behaviour
changed. Evidence: `cargo test -p focal-node --test evidence_quic` passed
six of six reruns on this tree (2 tests each), `clippy -p focal-node
--all-targets -- -D warnings` and `cargo fmt --all --check` were clean.

## Movement records in the session log (R7.3, third step, 2026-09-09)

**Scope.** The durable movement state machine of [25](25-parallel-materialization-and-ranges.md)
§6 (decision F42): each step of a range transfer — `Begin`, `Snapshot`,
`Barrier`, `SourceSealed`, `Ready`, `Activate`, `Abort`, `Cleanup` — is a
`FOCALRM1` record the authority proposes and every replica applies through
the range crate's coordinator against the committed state, under a commit
proof minted from the entry and attested from the session genesis. Members
are held by the voters (the log itself) or by a materializer replica;
proofs are demanded only of replica holders. The coordinator state travels
in the session checkpoint, so a restart or a lagging replica's snapshot
resumes a transfer from the committed step. Between barrier and activation
the authority refuses mutations on the moving member (`RangeMoving`).

- **Range crate** (no longer dormant): `Holder::{Voters, Replica}` in
  `Placement` (`voters()`, `replica()`, `replica_owner()`); `CommitProof`
  gains the control `ordinal` (records order by ordinal, position by native
  prefix); `RangeCheckpoint` schema 2 with `control_ordinal`;
  `prepare(ordinal, sequence, op)` requires the next ordinal exactly;
  `Snapshot`/`Ready`/`SourceSealed`/`unchanged` are required of replica-held
  ranges only; `published()` needs progress only from replica-held members;
  `relayout` re-lays the map after a committed layout change;
  `range_command_hash` v2 binds the ordinal.
- **Ledger**: `native_session_movement.rs` (`MovementRecord` codec,
  `LedgerRangeVerifier` with `attest_*` helpers, `map_from_layout`,
  `Movement` wrapper: `apply` mints the proof and counts deterministic
  refusals, `split`/`merge` follow layout records, `fenced_members`);
  `NativeSessionLimits.ranges`; `NativeSessionError::{RangeMoving,
  Range(RangeError)}`; engine `movement` built at genesis or restored from
  the checkpoint's movement section, `propose_range` (authority, no pending
  candidates, no layout change or step in flight, `Cleanup` waits for pins),
  the admission fence on touched members (`NativePrepared::touched_members`),
  a layout change refused while a transfer is pending and inert when
  committed behind one, `in_flight` cleared on a term change;
  `NativeSession::{propose_range, range_map, range_epoch, movement_pending,
  movement_checkpoint, movement_refusals, movement_in_flight,
  range_verifier, range_activation_operation, range_activation}`; FCNSESS
  ancillary byte 0 and the movement section (`Limits.movement_bytes`,
  `EncodingPlan::prepare_with`, `Checkpoint::movement`,
  `SeedManifest::movement`).
- **Tests**: cluster
  `a_member_moves_under_one_authoritative_decision_and_faults_at_every_barrier_recover`
  (a follower cannot propose; a step in flight fences the next; a layout
  change waits; records commit before the barrier; a fenced mutation is
  `RangeMoving` and `Abort` is `Sealed` after it; a lagging follower installs
  the checkpoint carrying the pending transfer; a forged `Ready` is refused
  before proposal and the attested one applies everywhere; activation moves
  the map to the next epoch under the replica holder and reopens admission;
  `Cleanup` retires the history; a move back to the voters needs no
  readiness, a restart mid-transfer resumes, the source's seal is demanded,
  a new authority activates; a later split re-lays the map with inherited
  placement); `movement_records_round_trip_and_refuse_every_forgery`,
  `genesis_attestations_bind_every_proof_field_and_the_genesis`,
  `a_movement_section_rides_both_forms_and_every_forgery_of_it_is_refused`;
  the range crate's suite adapted to holders and ordinals.
- **Docs**: [25](25-parallel-materialization-and-ranges.md) §6,
  [22](22-native-record-format.md) §2 and §6, [07](07-decisions-and-traceability.md)
  F42, [05](05-implementation-plan.md) P11.3/P11.5, REMAINING R7 progress.

**Evidence** (macOS arm64, this tree, 2026-09-09 23:24–23:35 CDT, gate75):
`bash scripts/cargo.sh test --workspace --offline --no-fail-fast` ran 105
test binaries, 2,496 passed, 0 failed; `clippy --workspace --all-targets
--offline -- -D warnings`, `scripts/check-production.sh`, `cargo fmt --all
--check` and `cargo check --workspace --all-targets --locked --offline` were
clean; `python3 scripts/check-contracts.py` verified 1,369 links, 37 hashes
and 15 vocabularies.

## Holders that serve (R7.3, fourth step, 2026-09-09)

**Scope.** [25](25-parallel-materialization-and-ranges.md) §7 (decision
F43): a holder of a member is a Raft learner with native hosting that
materializes the whole session (rows hydrate from their claims' retained
policies at decode time, so a member-scoped store is not self-contained —
the recorded reason P11.3 stays open); movement redistributes serving. A
holder states its readiness, seal and progress facts with its member digest
over its own authenticated connection; the authority verifies each against
its digest of the barrier-frozen member, attests it and proposes it. The
session leader's controller carries an operator's move through every step.

- **Core**: `NativeLocation` (where an object's rows live) with
  `location_affinity`; `RangeLayout::{route_affinity, member_id}`;
  `NativeRanges::member_entries`; `checkpoint::member_digest` (the root
  frame of one member's rows; an empty member digests its prefix);
  `Core::{native_member_for, native_member_digest}`.
- **Ledger**: `Session::{native_range_map, native_movement_pending,
  native_movement_checkpoint, native_movement_refusals,
  native_movement_in_flight, native_range_verifier,
  native_range_activation_operation, native_range_activation,
  native_propose_range, native_member_for, native_member_at,
  native_member_digest}`.
- **Wire**: `Operation::RangeControl {group, request}` (tag 31, node-only,
  certificate required, `MAX_RANGE_CONTROL_REQUEST_BYTES` 4 KiB), admitted
  by `auth.rs`, valid for the peer pool's `send_placement`, covered in the
  client inventory as `peer.range_control`.
- **Node**: `fleet_range.rs` (`RangeView`/`RangeMemberView`/
  `RangePendingView`/`RangeHistoryView`, `RangeFactRequest`/`RangeFact`,
  `RangeControlRequest`/`RangeControlReply`; `ReplicaHost::{range_view,
  propose_range, move_range, activate_range, range_fact}`;
  `Work::Range`; the owner builds a move's intent from the committed map
  with a derived transfer identity, states facts under
  `ReplicaConfig.node_generation`, and serves a native read on a learner
  only for members it holds (`serves_member`, `serves_all_members`,
  `native_reads::locations`); `verify_fact`/`verify_progress`);
  `ManagedService::range_control`; `AgentJob::MoveRange` and
  `PlacementHandle::move_range`, the agent's bounded `move_requests`
  answered by the leader's controller; `placement_controller::drive_movement`
  (seed, barrier, readiness and seals asked over `RangeControl` with a
  five-second bound, progress, activation, cleanup; refusals retried next
  pass); admin `ReplicaAdminCommand::Ranges` → `ReplicaAdminReply::Ranges`,
  `AdminCommand::MoveRange` → `RangeMovedReply`; `ClusterAdmin::{replica_ranges,
  move_range, tenant}`; client `AdminResult::{ReplicaRanges, RangeMoveProposed}`
  with `AdminRangeView`/`AdminRangeMember`/`AdminRangePending`/
  `AdminRangeHistory`; CLI `cluster replicas ranges list|move`.
- **Tests**: in-process
  `a_fresh_replicated_ledger_activates_admits_frames_and_serves_linearizable_reads`
  extended with a full move through the host (same request, same transfer;
  seed and barrier; a fenced mutation refused; a readiness fact verified
  against the authority's digest, a forged digest and a wrong peer refused;
  activation under the holder reopens admission; cleanup); real-binary
  `a_seeded_native_checkpoint_carries_the_founder_session_to_new_hosts`
  extended with an operator move to host-a carried by the founder's
  controller over QUIC, every process reporting the same map, and the
  founder reporting it again after a kill and restart.
- **Fixed on the way**: the wire's reply pairing now accepts a `Control`
  reply for `RangeControl` (a peer's fact was discarded as lost otherwise);
  the ranges view answers from any replica (`ReplicaAdminCommand::Ranges`
  takes an optional group); a founder restarted into a group led by another
  voter reports `Ready` as a follower (startup waited for a leader-only
  quorum read and never reported, [24](24-placement-execution-and-fleet-control.md)
  §17).
- **Docs**: [25](25-parallel-materialization-and-ranges.md) §7,
  [24](24-placement-execution-and-fleet-control.md) §8 and §17,
  [07](07-decisions-and-traceability.md) F43, [05](05-implementation-plan.md)
  P11.4/P11.5, [cluster-admin.md](../cluster-admin.md), REMAINING R7 progress.

**Evidence** (macOS arm64, this tree, 2026-09-10 00:12–00:22 CDT, gate76):
`bash scripts/cargo.sh test --workspace --offline --no-fail-fast` ran 105
test binaries, 2,496 passed, 0 failed; `clippy --workspace --all-targets
--offline -- -D warnings`, `scripts/check-production.sh`, `cargo fmt --all
--check` and `cargo check --workspace --all-targets --locked --offline` were
clean; `python3 scripts/check-contracts.py` verified 1,383 links, 37 hashes
and 15 vocabularies.

## Automatic decisions (R7.4, 2026-09-10)

**Scope.** [25](25-parallel-materialization-and-ranges.md) §8 (decision
F44): the session leader's controller decides a group's shape from its
members' row counts with hysteresis and proposes ordinary layout records;
after a move every replica renames the core layout's member to the map's
identity.

- **Core**: `NativeRanges::{member_stats, member_split_point}` and
  `Core::{native_member_stats, native_member_split_point}` (a member's rows
  and pages; the affinity at or past half its rows that is not its start).
- **Ledger**: `Session::{native_member_stats, native_member_split_point,
  native_propose_layout}`; the engine adopts the map's member identities
  after every applied movement (`adopt_map_identities`; a differing member
  count is `Corrupt`).
- **Node**: `range_balancer.rs` (`BalancerConfig` from
  `FOCAL_RANGE_TARGET_ENTRIES`, default 250,000, split factor 2, merge
  divisor 4, three observations; `MemberLoad`; `Decision::{Split, Merge}`;
  `Balancer::observe` with a bounded, pruned observation table);
  `RangeMemberView.entries`; `RangeCall::{Layout, SplitPoint}` and
  `ReplicaHost::{propose_layout, split_point}`; the agent's `balancer`;
  `placement_controller::drive_balance` after `drive_movement` (skipped while
  a transfer is pending or in flight; a split names the new member from the
  ledger, member, affinity and epoch; refusals are observed again); client
  `AdminRangeMember.entries`; `cluster replicas ranges list` shows it.
- **Tests**: `range_balancer::tests` (a dip resets, a split precedes a
  merge, a full layout never splits, non-adjacent or unequal pairs never
  merge, a departed member drops its observations); in-process
  `a_fresh_replicated_ledger_activates_admits_frames_and_serves_linearizable_reads`
  extended with a split at the member's middle and a merge through the host
  after the move; real-binary `cli_native_a1` runs the complete cycle, kill
  and restart under `FOCAL_RANGE_TARGET_ENTRIES=4` and ends with several
  members that all hold rows at a later epoch.
- **Fixed on the way**: a transfer's replacement carried a fresh identity in
  the map while the core layout kept the source's, so a merge (or any
  layout record naming the moved member) was refused as an unknown member
  after a move; the identities are now adopted in the same apply on every
  replica.
- **Docs**: [25](25-parallel-materialization-and-ranges.md) §8,
  [07](07-decisions-and-traceability.md) F44, [05](05-implementation-plan.md)
  P11.6, [cluster-admin.md](../cluster-admin.md), REMAINING R7 progress
  (what keeps R7 open).

**Evidence** (macOS arm64, this tree, 2026-09-10 00:39–00:49 CDT, gate77):
`bash scripts/cargo.sh test --workspace --offline --no-fail-fast` ran
105 test binaries, 2,498 passed, 0 failed;
`clippy --workspace --all-targets --offline -- -D warnings`,
`scripts/check-production.sh`, `cargo fmt --all --check` and `cargo check
--workspace --all-targets --locked --offline` were clean; `python3
scripts/check-contracts.py` verified 1,393 links, 37 hashes and 15
vocabularies.

## Movement under faults, holders in the directory (R7.5, 2026-09-10)

**Scope.** [25](25-parallel-materialization-and-ranges.md) §9 (decision
F45): every movement step resumes from the committed map after a crash of
its controller; a controller claims leadership of the sessions it must
drive; the directory publishes each session's holders.

- **Consensus**: a follower may ask its leader for leadership for itself
  (`transfer_leader(self)` forwards the raft transfer request to the
  leader it knows); any other target from a follower stays `NotLeader`.
- **Node**: `fault.rs` gains seven movement sites (`movement-begin`,
  `movement-seed`, `movement-barrier`, `movement-ready`, `movement-seal`,
  `movement-activate`, `movement-cleanup`) placed in
  `placement_controller::drive_movement` after each step's evidence and
  before its proposal; `claim_leadership` asks for a session's leadership
  when this voter does not lead it and the session has work (a plan, a
  pending transfer, a queued move, a retired map, unpublished holders);
  `publish_holders` records the settled map as `SessionChange::Holders`
  once per range epoch.
- **Directory**: `SessionDescriptor.holders: Option<RangeHolders>`
  (`RangeHolder { member, start, node, generation }`,
  `MAX_PUBLISHED_HOLDERS` 1,024), `SessionChange::Holders`, partition
  checkpoint schema 7 with `SessionDescriptorV6`/`PartitionCheckpointV6`
  converting on decode; a publication applies only above the held epoch
  (idempotent at the same epoch for the same members, `CompareFailed`
  otherwise, `StaleEpoch` below), in key order with unique identities, and
  with every holding replica a member of the active placement at its
  enrolled generation (`Missing`/`StaleNode`).
- **Admin**: `AdminSessionPlacement.{range_epoch, holders}` with
  `AdminRangeHolder`; `cluster placement` shows them.
- **Tests**: `placement_progress::holders_publish_in_epoch_order_for_placement_members_only`
  (every refusal, idempotence, conflict, staleness, the schema 7 round trip
  and a schema 6 restore); real-binary
  `movement_survives_a_cut_at_every_step_a_dead_destination_and_duplicate_requests`
  (seven cuts with the founder restarted and claiming the session each
  time, seven moves settling on every process; a duplicate move; a dead
  destination holding at the barrier with a submission refused; a listing
  continued across the move; the directory's publication).
- **Docs**: [25](25-parallel-materialization-and-ranges.md) §9,
  [24](24-placement-execution-and-fleet-control.md) §6, §9, §15 and its
  limit notes, [06](06-verification-and-operations.md) §3,
  [07](07-decisions-and-traceability.md) F45, [05](05-implementation-plan.md)
  P11.5/P11.6, [cluster-admin.md](../cluster-admin.md), REMAINING R7
  progress (what keeps R7 open).

**Evidence** (macOS arm64, this tree, 2026-09-10 01:14–01:27 CDT, gate78):
`bash scripts/cargo.sh test --workspace --offline --no-fail-fast` ran
105 test binaries, 2,500 passed, 0 failed;
`clippy --workspace --all-targets --offline -- -D warnings`,
`scripts/check-production.sh`, `cargo fmt --all --check` and `cargo check
--workspace --all-targets --locked --offline` were clean; `python3
scripts/check-contracts.py` verified 1,411 links, 37 hashes and 15
vocabularies.

## Custody receipts and obligations (R8.2, 2026-09-10)

**Scope.** [26](26-custody-archive-retention-and-restore.md) §1 (decision
F46): a copy's verified custody is kept as an attested receipt and read as
an obligation before a validation phase against an artifact is admitted.

- **Evidence**: `custody_receipt.rs` (`CustodyReceipt`, `FCCRCPT1`,
  `RECEIPT_BYTES` 160, keyed attestation, `name(ledger, root, node)`);
  `CustodyRecordKind::Receipt` (`receipts/`);
  `ContentStore::{replace_named_custody_record, read_named_custody_record,
  record_custody_receipt, custody_receipt}`.
- **Core**: `DecodedRequest::into_evaluation_artifact` (the artifact a
  `BeginIncrement`/`BeginWork` frame evaluates; fixed plans only).
- **Node**: `ContentHost::{record_receipt, receipt}` (`Command::{RecordReceipt,
  Receipt}`); `evidence_service::{CustodyObligation, receipt_for,
  obligation}`, `EvidenceCoordinator::obligation` (`JobKind::Obligation`),
  receipts recorded in `replicate` for this node and every `Durable` copy;
  `native_ingress::{evaluates_artifact, evaluation_artifact_of_frame}`;
  `ReplicaHost::artifact_pointer` (`Work::ArtifactPointer`);
  `FleetService::eligible` refuses a phase whose artifact's copies fall
  short as a retryable capacity refusal naming the missing nodes.
- **Tests**: `custody_receipt::tests` (round trip, every forgery, names,
  reopen, replacement, corruption); `evidence_service::tests`
  (`replication_records_a_receipt_per_verified_copy_and_the_obligation_reads_them`,
  `a_phase_beginning_frame_names_its_artifact_and_other_frames_do_not`);
  the A1 real-binary cycle runs `BeginWork` through the check.
- **Docs**: [26](26-custody-archive-retention-and-restore.md) §1 (new
  document, indexed), [04](04-storage-and-distribution.md) §7,
  [07](07-decisions-and-traceability.md) F46, [05](05-implementation-plan.md)
  P12 progress, REMAINING R8 progress.

**Evidence** (macOS arm64, this tree, 2026-09-10 01:44–01:57 CDT, gate79):
`bash scripts/cargo.sh test --workspace --offline --no-fail-fast` ran
105 test binaries, 2,503 passed, 1 (`cli_network` `replicas::actual_cli_and_mcp_administer_installed_data_membership_with_distinct_restart_receipts`: the node's start refused a loopback UDP port another test binary's node had bound seconds after this binary probed it free — a cross-process race in the test harness's port allocation, not in the product; the test passes alone, and the harness now claims ports with an exclusive lock file shared by every real-binary test, gated next) failed;
`clippy --workspace --all-targets --offline -- -D warnings`,
`scripts/check-production.sh`, `cargo fmt --all --check` and `cargo check
--workspace --all-targets --locked --offline` were clean; `python3
scripts/check-contracts.py` verified 1,429 links, 37 hashes and 15
vocabularies.

## Registry history, checkpoint cadence and retention floors (R8.3–R8.4a, 2026-09-10)

**Scope.** [26](26-custody-archive-retention-and-restore.md) §2 (decision
F47): schema and validator identities survive upgrades. §3 (decision F48):
every replica checkpoints by entry cadence, the retention floor and its
blocker are computed and shown, and the archive's report rides the
checkpoint; a sequence-based event retirement was built, failed restore
validation against complete object histories, and was withdrawn in favour
of per-object reclamation with the archive (§4, next).

- **Evidence**: `registry_history.rs` (`SchemaRegistry` with
  `SchemaRecord {descriptor, maximum_bytes, introduced_at, superseded_by,
  superseded_at}`, `new`/`new_in`/`with_builtins`, `register`, `supersede`,
  `record`, `current`, `NativeSchemaVerifier`; `MAX_SCHEMA_DESCRIPTOR_BYTES`
  4 KiB); `validators::{Lifetime, RegistryError::Retired}`,
  `Registry::{register_at, retire, lookup, lifetime}`, `execute` refusing a
  retired version.
- **Ledger**: `native_session_retention.rs` (`RetentionReport`,
  `RetentionBlocker`); engine `archived_through` with `note_archived`;
  `RetentionSection { archived_through }` under `FCNSESS` ancillary byte 1
  (`EncodingPlan::prepare_with_sections`, `Checkpoint::retention`,
  `SeedManifest::retention`); `Session::{native_retention,
  native_note_archived}`; `NativeSession::{archived_through, note_archived}`.
- **Node**: `ReplicaConfig::checkpoint_after_entries` (4,096) with
  `checkpoint_by_cadence`/`try_checkpoint` in the owner's tick;
  `log_entries_since_checkpoint` and `retention: Option<AdminRetention>`
  in `AdminReplicaDiagnostics`.
- **Harness**: `tests/support/ports.rs` claims each loopback port with an
  exclusive lock file shared by every real-binary test binary (a stale lock
  is reclaimed after half an hour) before probing it, replacing fifteen
  per-process allocators whose ranges overlapped (the gate79 collision).
- **Tests**: `registry_history::tests::{schemas_keep_their_identity_and_bound_across_supersession,
  a_retired_validator_keeps_its_identity_and_runs_nothing_new}`;
  `native_session::retention::tests`, `native_checkpoint`
  `a_retention_section_rides_both_forms_beside_the_movement_section`,
  `native_session::cluster_tests::the_archives_report_is_monotone_and_rides_checkpoints`;
  the fleet native test under a four-entry cadence observes compaction and
  the floor.
- **Docs**: [26](26-custody-archive-retention-and-restore.md) §2–§3,
  [07](07-decisions-and-traceability.md) F47–F48, [05](05-implementation-plan.md)
  P12 progress, [cluster-admin.md](../cluster-admin.md), REMAINING R8
  progress.

**Evidence** (macOS arm64, this tree, 2026-09-10 02:37–02:50 CDT, gate80):
`bash scripts/cargo.sh test --workspace --offline --no-fail-fast` ran
105 test binaries, 2,508 passed, 1 (`fleet::async_tests::shared_owner_queues_covering_flush_and_serves_another_group_while_disk_waits`, the recorded shared-owner timing case: one of three parallel writes answered under the full gate's load with something other than a commit; it passed three of three reruns alone; the harness is made exact-retrying in the next batch rather than loosened) failed;
`clippy --workspace --all-targets --offline -- -D warnings`,
`scripts/check-production.sh`, `cargo fmt --all --check` and `cargo check
--workspace --all-targets --locked --offline` were clean; `python3
scripts/check-contracts.py` verified 1,446 links, 37 hashes and 15
vocabularies.


## Retirement to the archive (R8.4b, 2026-09-10)

**Scope.** [26](26-custody-archive-retention-and-restore.md) §4 (decision
F49): a terminal, released family leaves the core to a content-addressed
bundle under custody through a committed record, behind typed
continuations; the archive agent drives it; the operator reads the floor's
counts and verifies a bundle.

- **Core** (`crates/focal-core/src/native/retirement.rs`, `record_codec/archive.rs`):
  `RetirementFamily {root, members, events, through}` derived by
  `Core::retirement_family` from the committed state alone (members by
  owner lineage; every row under a member's affinity that names it, never a
  colliding bucket's; declarations from the acceptance policy and the
  registration set; cycles, receipts, responses, testaments, work and
  diagnostic chains, accepted artifacts, monitors, index rows through
  `index_rows::{claim,definition,artifact,accepted}`, content identities,
  the event rows describing any family key); refusals `RetirementRefusal::{Unknown,
  NotTerminal, NotReleased, LiveParent, LiveDependent, LiveMonitor,
  LiveReference, LiveEvaluation, TooLarge, Corrupt}`; bounds 64 members,
  65,536 rows; `retirement_candidates` walks the terminal status buckets
  with a resumable cursor; `archive_family_quote`/`archive_family_into`
  write the `FCNARCHV` frame; `StructuralArchive::inspect` verifies one
  (digest, header, members, row order and families, no accounting row,
  nothing trailing) and counts rows by family; `retire_native_family`
  deletes the family, decrements Meta per family, writes `Key::Retired`
  (family 49: bundle root, length, prefix claimed, final binding and
  status, retirement sequence, events that left) and one outcome
  (`NativeInvocation::Retirement`, operation `Retire`, no events).
  Validators: `read_validate` reconciles missing outcome events against
  the continuations' counts; the authored profile's strict walk
  (`authored_check.rs`) and the checkpoint's `objects::authored` accept a
  creation-result entry whose identity left when its claim's continuation
  (or, for a definition, a retired claim of the same creation) vouches for
  it. Owner reconstruction over a retired core is tested.
- **Ledger** (`native_session_retirement.rs`, engine, apply, checkpoint,
  hosting): `FOCALRT1` (146 bytes) `RetirementRecord {ledger,
  expected_prefix, root, bundle, bytes, through}`;
  `propose_retirement` on the authority after deriving the family
  (`NativeSessionError::{Retiring, Retirement(refusal)}`); native
  admission, layout changes and movement steps answer `Retiring` while a
  record is in flight; `apply_retirement` is inert on a passed prefix, a
  pending movement or a refused family, otherwise retires through the
  committed core (`SuffixEvidence::Retired` for pending candidates; the
  hosted session reconstructs the authority's owner at once when it is
  past this term's readiness barrier, as after activation); the replica
  counts applied records and the count rides the checkpoint's retention
  section (`RetentionSection {archived_through, retired_families}`, +8
  bytes) and restores from it; `RetentionReport {retired, retiring}` and
  `allows_family` (inclusive of the cursor floor).
- **Node**: `RangeCall::{Candidates, Archive, Retire, Retired}` and
  `ReplicaHost::{retirement_candidates, archive_family(root, min_age_ms),
  propose_retirement, retired}` (`ArchivedFamily` charged to the replica
  budget; only on the authority, past the floor, and settled at least the
  grace ago on the node's logical clock); `EvidenceCoordinator::archive`
  seals the bundle as an evidence-class object of the ledger's tenant
  domain, replicates it to every required copy exactly as an upload and
  reports the obligation (`ArchiveOutcome`); `archive_agent.rs`
  (`FOCAL_RETIRE_INTERVAL_MS` 5,000; `FOCAL_RETIRE_AFTER_MS` 86,400,000;
  1,024 index rows per replica per tick; one family per replica per tick)
  runs in every `NetworkService`; `NativeObject::Retired(NativeRetiredClaim)`
  on reads of a retired claim; `OperatorRead::Archive {session, claim}`
  → `AdminArchiveBundle` (continuation, verified structure, families,
  receipts); `AdminRetention {retired, retiring}`; CLI `cluster retention
  show`, `cluster archive show --claim`; MCP `cluster.retention.show`,
  `cluster.archive.show` (41 admin descriptors; cluster skill version 7,
  manifest re-pinned).
- **Tests**: `native::retirement_tests` (3), `record_codec::archive` through
  them, `native_session::retirement::tests`,
  `native_session::cluster_tests::committed_retirements_apply_on_every_replica_and_fence_proposals`,
  `session_native_tests::a_hosted_authority_retires_a_family_and_stays_authoritative`,
  `native_session::retention::tests` (2), `native_checkpoint` section test
  (+16 bytes), wire/MCP/skill contract tests updated,
  `crates/focal-node/tests/cli_retention.rs` (real binary: create, cancel,
  release, agent-driven retirement, continuation read, retention counts,
  verified bundle with its receipt, no archive for a live claim, exact
  retry of the retired claim's creation, kill and restart).
- **Docs**: [26](26-custody-archive-retention-and-restore.md) §4,
  [22](22-native-record-format.md) (`FOCALRT1`, `FCNARCHV`, family 49 body),
  [07](07-decisions-and-traceability.md) F49, [05](05-implementation-plan.md) P12,
  `docs/cluster-admin.md`, `docs/REMAINING.md` R8 progress,
  `skills/focal-cluster/SKILL.md` + `skills/manifest.json`.

**Lessons.** (1) A family's rows are found by affinity, but an affinity is
a bucket: a participant's index rows and any colliding id share it, so the
scan keeps only rows that name the member. (2) The authored profile
cross-links creation results, identities, contents and claims; identities
leave with the family and the continuation vouches for the creation
result's entries. (3) The hosted session promotes the engine at its own
readiness barrier, never through the engine's request path, so a record
that demotes the authority must reconstruct it there. (4) Finished claims
must stay readable for a grace: without one, background retirement
changed the answers of every journey that reads a released claim (the A3
audit re-post read `not_found` instead of `conflict`).

**Evidence (gate 82).** `bash scripts/cargo.sh test --workspace --offline
--no-fail-fast` 2026-09-10 04:46:17–04:59:32 CDT, macOS arm64: 106 test
binaries, 2,517 passed, 0 failed. `cargo clippy --workspace --all-targets
--offline -- -D warnings`: clean. `bash scripts/check-production.sh`:
clean. `cargo fmt --all --check`: clean. `cargo check --workspace
--all-targets --locked --offline`: clean. `python3
scripts/check-contracts.py`: 1,454 architecture links, 37 imported hashes,
15 frozen vocabularies. Gate 81 (04:32–04:45) had the same test totals and
one clippy refusal (`while_let_on_iterator` in the candidate walk),
rewritten before gate 82.

**Limits.** As [26](26-custody-archive-retention-and-restore.md) §4:
continuations are permanent until R8.6's catalog; whole families only;
bundle hydration is restore's; the grace and interval are node-local
settings until R9's committed policy; a bundle whose copies never answer
waits for R8.5's collector.

## The collector (R8.5, 2026-09-10)

**Scope.** [26](26-custody-archive-retention-and-restore.md) §5 (decision
F50): every node reclaims bytes outside what the committed rows name, after
a grace, through a dated quarantine that is reversible until its own grace
expires; the roots come from the hosted replicas' rows and the bundles
those rows name; domains this node cannot walk are opaque.

**Delivered.**

- `focal-core`: `Core::native_content_roots(cursor, max_visits)` walks
  every row a page at a time and yields `ContentRoot::{Artifact(root),
  Inline(stream digest), Bundle{root, bytes}}`; the archive frame
  (`FCNARCHV`) carries the family's `content` roots and `inline` digests in
  strictly ascending order, read back by `StructuralArchive::header()`.
- `focal-evidence` (`store/gc.rs`): `ProtectionSet` (objects by root,
  streams by digest, custody records in use, opaque domains),
  `CollectorConfig` (grace one day, quarantine seven days, terminal fence
  seven days, four records kept, 1,048,576 marks), the phased
  `ContentStore::collect_step` (uploads → terminals → per-domain manifests
  and chunks → custody records → receipts → quarantine rounds), rename
  into `quarantine/<round>/…` with directory syncs, `restore_quarantined`,
  reopen re-syncing the rounds, and `SeedStore::collect`; `CollectorReport`
  and `SeedReport` count every action.
- `focal-ledger`: `Session::native_content_roots`, `native_seed_chunks`
  (the latest checkpoint's chunks and any pending seed) and
  `native_collect_seeds`.
- `focal-node`: the collector agent (`gc.rs`; `FOCAL_GC_INTERVAL_MS`,
  `FOCAL_GC_GRACE_MS`, `FOCAL_GC_QUARANTINE_MS`, `FOCAL_GC_TERMINAL_MS`)
  gathers roots through `RangeCall::ContentRoots`, reads each bundle's
  header once, installs the protection set and drives `collect` through
  `ContentHost::{protect, collect, restore_quarantined}`, then sweeps seeds
  through `RangeCall::CollectSeeds`; `OperatorRead::Gc`,
  `AdminCommand::GcRestore`, `cluster gc show`, `cluster gc restore
  --domain --root`, MCP `cluster.gc.show` and `cluster.gc.restore` (43 admin
  descriptors; cluster skill v8).

**Evidence (macOS arm64, `--offline`).** Gate 84, 2026-09-10
05:54:29–06:07:57 CDT, on the final tree of this section:

- `bash scripts/cargo.sh test --workspace --offline --no-fail-fast`: 107
  test binaries, 2,524 passed, 0 failed, 0 ignored failures (the run also
  exercised the new `crates/focal-node/tests/cli_gc.rs`,
  `focal_evidence::store::gc::tests` and the retirement bundle-header
  tests).
- `bash scripts/cargo.sh clippy --workspace --all-targets --offline -- -D
  warnings`: clean (gate 83 at 05:39 had refused a `windows(2)` index in the
  archive frame's order check; replaced by a `zip`-based
  `strictly_ascending`).
- `bash scripts/check-production.sh`: clean.
- `cargo fmt --all --check`: clean.
- `cargo check --workspace --all-targets --locked --offline`: clean.
- `python3 scripts/check-contracts.py`: 1,464 architecture links, 37
  imported source hashes, 15 frozen vocabularies.

**Remaining in R8.** Backup at a declared prefix, restore verification and
the recovery incarnation, the operator surface for both (R8.6); the
compaction of continuations; the sustained-bounds loop and the corruption
suite of the close gate.

## Backups at a declared prefix (R8.6, first step, 2026-09-10)

**Scope.** [26](26-custody-archive-retention-and-restore.md) §6 (decision
F51): a coherent backup of one hosted native session — the envelope its
replica installed durably at one applied index, the seed chunks of that
envelope's root, the exact tree of every content object the envelope's
rows and archive bundles name, and a hashed manifest written last — and
its verification, which runs without a node.

**Delivered.**

- `focal-core`: `ContentRoot::{Artifact(pointer), Inline(pointer)}` carry
  the recorded custody pointer (an inline payload's sealed object has the
  same root on every replica, since every replica seals it under the
  canonical chunking and reads it back under the record's pointer); the
  `FCNARCHV` header's `inline` list names those roots; the collector
  protects both by root.
- `focal-evidence`: `ContentReader::{describe_object, read_transfer_chunk}`
  and `describe_encoded` export an installed object's exact tree from its
  manifest alone; `TransferManifest::chunk(index)`.
- `focal-ledger` (`session_backup.rs`, `focal_ledger::backup`): the
  `FCLBKUP1` manifest ([22](22-native-record-format.md) §8), the
  `BackupMedium` install sequence (temporary, sync, rename, directory
  sync) implemented by the filesystem and by the simulated disk, the
  `ObjectSource`/`SeedSource` readers over a store or a backup directory,
  `inventory` (decode the envelope, assemble a seeded root, rebuild the
  Core through recovery, walk the roots, read each bundle's header),
  `write` (refuses an existing manifest), `verify` (every file, then the
  envelope rebuilt against the backup's own files and compared with the
  manifest, problems listed), and `decoder_pair`;
  `SeedManifest::assemble_with` reads chunks from any source.
- `focal-node`: `backup.rs` (`create` through the replica's evidence export
  and a blocking write; `verify` offline), `AdminCommand::BackupCreate`,
  `cluster backup create --output DIR [--session ID]`, `cluster backup
  verify --input DIR` (runs before any node or data directory is opened),
  MCP `cluster.backup.create` and `cluster.backup.verify` (45 admin
  descriptors, cluster skill v9), `AdminResult::{BackupCreated,
  BackupVerified}` with `AdminBackup`, `AdminBackupPrefix` and
  `AdminBackupVerification`; a session without a committed placement
  answers `unavailable`.
- `focal-sim`: `Disk::operations()` names the cut coordinate.

**Evidence (macOS arm64, `--offline`).** Gate 86, 2026-09-10
06:56:20–07:09:51 CDT, on the final tree of this section:

- `bash scripts/cargo.sh test --workspace --offline --no-fail-fast`: 108
  test binaries, 2,525 passed, 2 failed — both in the `focal-node` library
  suite and both known load flakes that pass alone and passed in gate 84:
  `fleet_tests::exact_retry_rediscovery_preserves_receipt_after_cached_owner_loses_leadership`
  (`NotLeader` before the re-election settled) and
  `placement_agent::tests::operators_admit_tenants_and_create_sessions_that_register_serve_and_survive_restart`
  (the operator's placement read landed before the agent's registration
  pass); rerun alone immediately after the gate: 2 passed. The run
  includes the new `crates/focal-node/tests/cli_backup.rs` and the two
  ledger backup tests (every durable cut; a seeded root).
- Gate 85 (06:40–06:54) on the previous tree found one clippy refusal
  (`items_after_test_module` in `transfer.rs`) and one timing race in
  `cli_retention.rs` (the claim's status read after its release raced the
  archive agent at a zero grace); both fixed, gate 86 is the evidence.
- `bash scripts/cargo.sh clippy --workspace --all-targets --offline -- -D
  warnings`: clean.
- `bash scripts/check-production.sh`: clean.
- `cargo fmt --all --check`: clean.
- `cargo check --workspace --all-targets --locked --offline`: clean.
- `python3 scripts/check-contracts.py`: 1,471 architecture links, 37
  imported source hashes, 15 frozen vocabularies.

**Remaining in R8.6.** Restore: verification before serving, the
recovery incarnation (a new log group and genesis, the placement and
membership sections reset, the session registered as this node's own),
the fenced same-incarnation decision, `cluster restore`, and the
real-binary journey through a killed node and a fresh cluster.

## Restore and the recovery incarnation (R8.6, second step, 2026-09-10)

**Scope.** [26](26-custody-archive-retention-and-restore.md) §6 (decision
F52): a verified backup becomes a hosted session on this node from the
backup's prefix, under the incarnation the committed enrollment registry
allows; nothing is served before verification; a restore never overwrites
history.

**Delivered.**

- `focal-consensus`: `RestoredLog` and `DurableNode::{restore_in,
  restore_on_wal_in}` begin a logical group's log from an image — on an
  empty log only: identity, decoder floor and transition, the snapshot at
  the image's index and term under the bootstrap membership, a hard state
  that commits it — and open the group as a restart would.
- `focal-ledger` (`backup`): `decode_image` (shared by inventory, verify
  and restore), `Incarnation`, `rewrite` (the envelope re-encoded under the
  target's cluster, group and re-derived genesis, a bootstrap membership of
  the restoring node, placement and membership sections reset, movement
  dropped, a seeded root sealed into the target seed store), `import_content`
  (each object installed chunk by chunk as a custody transfer does) and
  `import_seeds`.
- `focal-node`: `backup::{RestoreDecision, decide, recovery_group,
  RestoreRequest, RestoredSession, read_manifest}`, the placement agent's
  `restore_session` (verify, refuse a hosted or directory-held session,
  admit the tenant, import through `ContentHost::restore_content`, install
  seeds and rewrite on a blocking thread, begin the log, record the copy as
  created here, attach it through the refactored `attach_copy`; the next
  pass registers it), `AdminCommand::Restore`, `cluster restore --input DIR
  [--new-incarnation]`, MCP `cluster.restore` (46 admin descriptors,
  cluster skill v10), `AdminResult::Restored` with `AdminRestore`;
  `cluster backup create` takes `--tenant` for a served tenant's session;
  saved connections address another served session (`context add NAME
  --node-data-dir DIR --tenant ID --session ID`, `context add NAME
  --enrolled-as CONTEXT --session ID`).

**Evidence (macOS arm64, `--offline`).** Gate 87, 2026-09-10
07:35:10–07:48:50 CDT, on the final tree of this section:

- `bash scripts/cargo.sh test --workspace --offline --no-fail-fast`: 109
  test binaries, 2,529 passed, 0 failed (including the new
  `crates/focal-node/tests/cli_restore.rs` and the ledger's in-process
  restore round trip
  `a_backup_restores_into_a_new_incarnation_that_holds_the_prefix_and_continues`).
- `bash scripts/cargo.sh clippy --workspace --all-targets --offline -- -D
  warnings`: clean.
- `bash scripts/check-production.sh`: clean.
- `cargo fmt --all --check`: clean.
- `cargo check --workspace --all-targets --locked --offline`: clean.
- `python3 scripts/check-contracts.py`: 1,477 architecture links, 37
  imported source hashes, 15 frozen vocabularies.

**Remaining in R8.** The operator view of retention policy, storage
pressure and repair progress beyond the retention, gc and backup reads
(instruction 9; the committed policy is R9's); the compaction of
continuations; the sustained-bounds loop and the corruption suite of the
close gate; the same-cluster restore of a session the directory still
names (a placement decision, R9).

## The operator's storage view and R8's close (2026-09-10)

**Scope.** [26](26-custody-archive-retention-and-restore.md) §7 (decision
F53): one local read of the node's storage pressure, agents and retention
floors; the sustained-bounds workload and the corruption cases R8's close
gate names.

**Delivered.**

- `focal-node`: `OperatorRead::Storage`, `cluster storage show`, MCP
  `cluster.storage.show` (47 admin descriptors, cluster skill v11);
  `ContentHost::disk_stats` (the volume envelope's statistics and what is
  staged); the archive agent publishes `AdminArchiveAgent` (interval,
  grace, ticks, proposed, waiting) through an `ArchiveHandle` the admin
  serves; `AdminStorage`, `AdminDiskStats`, `AdminSessionRetention` and
  `AdminResult::Storage` in `focal-client`.
- Evidence added to the suites: the ledger's
  `a_sustained_workload_stays_within_its_budgets_and_keeps_every_outcome`
  (24 rounds of create/finish/release/archive/retire with checkpoints and
  collector passes every fourth round, memory within its allowance and
  plateaued, no proof quarantined, every outcome exact, every bundle
  verified, a backup written and verified); `cli_retention.rs` sees a
  corrupted bundle chunk turn the archive unverified and back;
  `cli_restore.rs` sees a corrupted backup refused before any file moves;
  `cli_backup.rs` reads the storage view.

**Evidence (macOS arm64, `--offline`).** Gate 88, 2026-09-10
07:58:55–08:12:36 CDT, on the final tree of this section:

- `bash scripts/cargo.sh test --workspace --offline --no-fail-fast`: 109
  test binaries, 2,530 passed, 0 failed.
- `bash scripts/cargo.sh clippy --workspace --all-targets --offline -- -D
  warnings`: clean.
- `bash scripts/check-production.sh`: clean.
- `cargo fmt --all --check`: clean.
- `cargo check --workspace --all-targets --locked --offline`: clean.
- `python3 scripts/check-contracts.py`: 1,482 architecture links, 37
  imported source hashes, 15 frozen vocabularies.

**R8 is closed** on this evidence; its stated limits (the receipt floors
of P12.3, cold proof lookup beyond bundle verification, searchable indexes
over bundles, pacing by work credits, the compaction of continuations, the
committed retention policy of R9, and the same-cluster restore of a
session the directory still names) are recorded in [26](26-custody-archive-retention-and-restore.md).

## Typed configuration with ownership (R9.1, 2026-09-10)

**Scope.** [08](08-stepped-complexity-and-deployment.md) §2: one versioned
schema, two authorities. Node-local facts may be overridden at startup;
the durability and placement intent is committed with the store and
changes only through plan and apply; unknown keys fail by their full path;
omitted fields keep committed values; every value names its source.

**Delivered.**

- `crates/focal-node/src/config/` replaces `config.rs`: `schema.rs`
  (`check_unknown_keys` walks the embedded JSON schema and names the first
  unknown key by its dotted path; `properties` lets a test hold the schema
  and the typed settings to the same fields), `local.rs` (`LocalFacts`),
  `policy.rs` (`PolicyIntent`, `PolicyRevision`, `CommittedPolicy` in the
  `POLICY` file as `FCLPOL2` with a revision and a hash — the original bare
  pair reads as revision 1 and is never rewritten; `read_committed`,
  `install_or_check`, `commit`), `resolve.rs` (`CliOverrides`,
  `FilePresence`, `ConfigSource::{CommandLine, File, CreationDefault,
  Committed(revision)}`, `resolve`).
- `ConfigError::{UnknownKey{path}, CommittedPolicyChange{field},
  PolicyMissing, PolicyEncoding}`; both open paths (`EmbeddedNode::open`,
  the founding network) check the committed policy before re-solving
  placement, so a differing file is refused by its field and a lost policy
  beside a store is data loss (`policy_missing`, exit 1), while an unknown
  key or a committed change is operator input (exit 2, `committed_policy`).
- `node.metrics_listen` (loopback only) joins the schema and the settings;
  the schema now declares `node.max_tenants`.
- `deployment explain` prints `requested`, `effective`, `committed_revision`
  and `sources`.
- Tests: `config/tests.rs` (unknown-key paths, schema/serde agreement,
  precedence and sources, committed policy over omitted fields and refusal
  by name, the policy file's forms and revisions, loopback metrics);
  `cli_failures.rs::committed_policy_and_unknown_keys_are_refused_by_name`
  on the real binary.

**Evidence (macOS arm64, `--offline`).** Gate 90, 2026-09-10
09:20:51–09:34:09 CDT, on the final tree of this section: `test
--workspace`: 109 binaries, 2,537 passed, 0 failed; clippy, production
lints, fmt and the locked check clean; contracts 1,486 links (gate 89 at
08:25 had one test still expecting the old opaque error for a lost policy;
fixed, gate 90 is the evidence).


## Deployment plans and their application (R9.2, 2026-09-10)

Instruction 2 of R9 ([REMAINING §14](../REMAINING.md); [08](08-stepped-complexity-and-deployment.md) §9):
a plan is an immutable artifact computed from what the node observes and
what the operator requests; applying it rechecks every observation it was
built on and refuses a stale plan before any side effect; progress is
journaled per change so a repeated apply resumes and repeats nothing.

- Dry-run placement requests: `AdminCommand::PlanSession` carries `dry_run`
  (`SessionPlannedReply` schema 2 echoes it); the placement agent keeps its
  waiters as `(reply, dry_run)` pairs and journals `SessionChange::Plan`
  only when a waiter is not a dry run — a request that is only dry runs
  proposes from the committed directory and commits nothing; `pending` and
  `satisfied` answers never journaled. `cluster sessions plan --dry-run`,
  MCP `cluster.sessions.plan` v2 (`dry_run`), `AdminResult::SessionPlanned
  { dry_run }`; cluster skill v12.
- `crates/focal-node/src/deployment/`: `plan.rs` (`FCLPLAN1`: magic,
  postcard `DeploymentPlan { schema, plan_id, created_ms, observed_at, body
  }`, BLAKE3 trailer; `PlanBody { deployment {cluster, node}, observed
  {policy_revision, policy_hash, policy, sessions[epochs, voters, desired,
  achieved, pending, blocked_by], nodes}, requested, policy_hash, changes,
  guarantee {before, during, after}, blocked }`; `Change::{CommitPolicy,
  PlanSession {operation, voters, expected epochs, pending}, NoChange}`;
  `plan_id` = `derive_key("focal.deployment.plan.v1", body)[..16]`, so
  the same observation and request make the same plan; `compose` orders the
  policy commit first when the request differs, one change per session in
  observation order, blocked sessions listed with the guarantee after equal
  to the guarantee before — the weakest achieved level across the sessions
  (`GuaranteeLevel` ranked by tolerated failures, then domain breadth), or
  the committed level without sessions — and the guarantee during equal to
  the guarantee before; bounds 4 MiB and 4,096 sessions; a truncated
  directory view is refused), `observe.rs` (the committed policy and the
  placement view converted once, sessions deduplicated), `apply.rs`
  (`FCLAPLY1` journal under `cluster/apply/<plan>/JOURNAL` with the plan
  beside it; `Phase::{Prepared, Committed, Verified, Complete}` per step,
  `Outcome::{InProgress, Complete, Stale}`; `preflight` checks every step not
  yet committed against the current policy revision and session epochs
  without sending anything; `apply` refuses another cluster's plan, a plan
  with blocked sessions and a stale plan (no journal is created), commits
  the policy as the expected next revision and reads it back, sends each
  placement request (a reply naming another operation marks the journal
  stale), observes it under way and complete, waits at most `--wait`;
  `status` re-checks journaled plans), `mod.rs` (`DeploymentError` with
  exits `stale_plan` 5, `wrong_deployment` 2, `plan_corrupt` 2,
  `guarantee_unsatisfied` 6).
- `cli/deployment.rs` (moved out of `main.rs`): `deployment plan --config
  FILE (--output NEW_FILE | --dry-run)`, `apply --plan-file FILE [--wait
  SECONDS]`, `status [--plan ID]`, and the existing `explain` (now printing
  `requested`, `effective`, `committed_revision` and `sources` also when the
  local inventory cannot satisfy the policy) and `schema`.
- Configuration: `load_settings` resolves against the data directory's
  committed policy for every command, so omitted policy fields take the
  committed values at startup (the R9.1 rule, previously only stated) and a
  differing file is refused by name at load; `resolve_request` serves
  `deployment plan`/`explain`, where the file is the request. The founding
  network node solves the single-node placement only when pinning its first
  policy, and the service falls back to the single-node scope before its
  session registers, so a committed policy stronger than one host provides
  no longer refuses a restart.
- Tests: `deployment/tests.rs` (ordering and identity, blocked sessions,
  artifact round trip and every tamper, preflight by the fact that moved,
  journal monotonicity, progress from the directory), `config/tests.rs`
  (request resolution), `cli_deployment.rs` on the real binary (founder and
  two hosts: a dry run prints the plan and journals nothing, a plan needing
  more domains is blocked and refused by apply, the written plan equals the
  dry run, is immutable, applies with `--wait` to activation at route epoch
  2 and resumes complete, a plan from an older observation is refused stale
  without a journal, a tampered plan and a laptop's plan are refused, the
  laptop applies its own plan and restarts under it, and the founder
  restarts under the stronger committed policy with the requesting file or
  none while the old value is refused by name).

**Evidence (macOS arm64, `--offline`).** Gate 91, 2026-09-10
10:03:23–10:18:05 CDT, on the final tree of this section: `test
--workspace`: 110 binaries, 2,544 passed, 0 failed; clippy, production
lints, fmt and the locked check clean; contracts 1,487 links, 37 imported
hashes, 15 frozen vocabularies.

## Draining, replacing and removing nodes, and the session driver (R9.3, first step, 2026-09-10)

Instruction 3 of R9 ([REMAINING §14](../REMAINING.md); [24](24-placement-execution-and-fleet-control.md)
§19, §9; [08](08-stepped-complexity-and-deployment.md) §10; decisions F55,
F56): a host leaves placement through one committed fact and is removed only
once nothing names it; and the partition leader drives every session of its
partition, through the session's own leader when it does not lead the log.

- **Eligibility as a grant fact.** `ControlRead::PrepareEligibility { node,
  eligible }` → `ControlReadResult::PreparedEligibility { generation,
  command }` (root scope; excluded from peer read-only ingress) prepares the
  node's topology grant re-issued at its next generation with the requested
  eligibility; `AdminCommand::Authority(ControlRequest)` commits it as the
  ordinary exact journaled request (validated to a `GrantNode` at the next
  generation with a zero attestation); the founder is refused before the
  root. `ClusterAdmin::{node_eligibility, remove_node, replace_node}`; CLI
  `cluster nodes drain|undrain|remove|replace`; MCP
  `cluster.nodes.drain|undrain|remove|replace` (51 descriptors, cluster skill
  v13); `AdminResult::{NodeEligibility, NodeRemoved}`; errors `not_drained`
  5, `node_holding` 5, `unknown_node` 4, `node_not_ready` 5.
- **The partition learns every grant.** `enroll_nodes` (replacing
  `enroll_self`): a node enrolls its own grant, and the partition leader
  enrolls every other node's newer grant, eligible or not.
- **Eligibility gates placement only.** Group grants may name an ineligible
  member (`validate_group_proof`), the grant follows a log containing one
  (`prepare_membership_proof`), and its signatures count
  (`InstalledAuthorityVerifier`). A seat belongs to the node identity at
  the generation of its grant: a seat at or below the node's current
  generation is still held (signature counting, `prepare_session_proof`,
  `prepare_membership_proof`, readiness); membership epochs and the
  single-step rule count voting nodes, not re-grants; the grant follows a
  member's re-grant so every seat names the generation the node signs at.
- **Cutover with a superset.** The controller, the ledger's cutover rule
  and the root's proof preparation ask that every desired voter votes, not
  that nothing else does: a current voter the plan drops keeps its vote
  until activation retires it. `propose_placement_keeping` prefers the
  active placement's voters among equally eligible candidates, so
  expansions add to existing copies and heals move only what they must.
- **The session driver.** `Operation::SessionControl { group, request }`
  (tag 32, node-only): `SessionCall::{Facts, Membership, Placement}`,
  `SessionControlReply::{Facts, Membership, Placed, Refused}`; served by
  `session_control::serve` on the node whose hosted replica leads the log
  (`NotLeader { leader }` otherwise), honoured only from a voter of the
  group owning the session's partition (`authorized_controller`).
  `SessionDriver::{Local(ReplicaHost), Remote { leader }}` in
  `placement_controller.rs`: facts, membership changes and placement
  records go through it; the leader is the hosted replica's, the last
  redirect's, the route's or the placement's preferred; a plan claims no
  leadership; range movement, balancing and holder publication still run
  only where this node leads the log, and for that work alone a voter that
  does not lead asks for leadership (`claim_for_ranges`).
- **Diagnostics.** `cluster node health` reports the placement agent
  (`AdminPlacementAgent { root_intents, partition_intents, installed,
  last_error, last_refusal }`); refused intents are recorded by kind and
  failure (`IntentOutcome::Refused(failure)`).
- Hosts admit tenants from their own applied registry when a quorum read is
  not theirs to make (`local_registry`), so a host creates sessions for an
  admitted tenant.
- Tests: `session_control::tests`, `cli_nodes.rs` (four hosts: a voter
  drained, healed around and removed; a drain without capacity refused;
  undrain heals; replace after a fifth host joins), `cli_session_remote.rs`
  (a session created on a host, expanded and healed by the founder through
  the session's own leader, the leader drained and removed).
- **Sessions created on hosts (2026-09-10).** A host's journaled root
  intents (the bootstrap of a session group it created) are submitted
  through the root leader's placement-control ingress under a client
  derived from its enrolled principal (`root_intent_client`; an empty
  journal adopts it), which the root admits for `BootstrapGroup` grants
  naming the sender alone; the partition owner admits the host's own
  `CreateSession` (a single-voter placement under a `Created` fence it
  verifies by proof); hosts check tenant admission against their applied
  registry when a quorum read is not theirs; node peers are no longer
  scoped by their grant's tenants (`permits_tenant`, `verify_request`), so
  sessions of tenants admitted after a connection was authorized are
  replicated, signed for and driven over it.
- **Readiness.** `OperatorRead::Readiness` → `AdminReadiness { alive,
  catching_up, authoritative, policy_satisfied, root, sessions, truncated }`
  derived from the root replica's progress, every hosted replica's
  diagnostics and the agent's last placement view ([24](24-placement-execution-and-fleet-control.md)
  §15); CLI `cluster node readiness` and `cluster node probe --check
  alive|catching-up|authoritative|policy` (exit 1 `probe_failed` when the
  check does not hold); MCP `cluster.node.readiness` (52 descriptors,
  cluster skill v14).

**Evidence (macOS arm64, `--offline`).** Gate 94, 2026-09-10
12:53:47–13:10:34 CDT, on the final tree of this section: `test
--workspace`: 112 binaries, 2,547 passed, 0 failed; clippy, production
lints, fmt and the locked check clean; contracts 1,500 links, 37 imported
hashes, 15 frozen vocabularies (gate 92 at 12:13 had the skill manifest
pinning a tool version and a test still scoping node peers by tenant; gate
93 at 12:31 had the range-movement journey stalled without the leadership
claim for range work; both fixed, gate 94 is the evidence).

## Credential rotation (R9.3, second step, 2026-09-10)

Instruction 3 of R9 ([REMAINING §14](../REMAINING.md); [24](24-placement-execution-and-fleet-control.md)
§11; instruction 6 of R6): a host moves to a fresh key under the same
identity through one committed registry fact its previous key authorized,
and every party that verifies the identity-to-key binding keeps verifying
it.

- **The registry fact.** `RenewRequest` schema 2 (`ROTATION_SCHEMA`) is a
  rotation: the new key's request identity and CSR, proven by the credential
  held (`CredentialMaterial::rotation_request(current, next, receipt)`,
  refused up front for the key already held). The sponsor prepares
  `Change::Rotate { invitation, receipt, retire_previous_at }`: a
  certificate issued for the new CSR under the same identity, the receipt
  naming the new request, key and CSR at the next revision, the previous
  certificate retired with the renewal's grace. Applying it moves the
  enrolled-key index to the new key and refuses a key already enrolled; a
  rotation already committed to the request answers with its receipt
  (`RenewPreparation::Existing`), and the proof of a retry is accepted from
  the key the rotation retired while its certificate still authorizes.
- **The binding stays verifiable.** A principal is derived from the key an
  enrollment began with (`assigned`); a rotated key did not derive it, so
  the certificate issued for a rotation, and for every renewal after one,
  carries the principal in a CA-signed subject
  (`focal-carried-principal:<principal>`, the founder's genesis rule
  generalized; `BootstrapAuthority::issue_carried`). The registry checkpoint
  (`restore`), the sponsor's apply of a renewal or rotation and the holder's
  saved-material inspection (`JoinKey::inspect_saved`) accept an identity
  only when its key derived it or a verified certificate carries it
  (`pki::identity_bound`); a retired credential is a renewal's (same key and
  request) or a rotation's (both differ, the key enrolled no more).
- **The holder.** The host stages a key with its own request identity
  (`JOIN/node-key.next`, kept until adopted), asks the sponsor over the
  enrollment transport, and adopts the receipt with `JoinKey::rotate_into`:
  the receipt first, then the key, then the staged material is cleared; the
  new credential is presented on the listener, the peer pool and the
  placement agent before the reply, as a renewal is. A crash between the
  sponsor's commit and the adoption leaves the host on its previous key: it
  starts (the held certificate authorizes through the grace, and
  `seed_peer_registry` no longer requires the held key to be the
  registry's), sees the registry's receipt under another key
  (`rotation_ahead`) and adopts the committed rotation from the staged key
  at once. The root re-grants a node whose enrolled key changed under its
  new identity at the next generation before other root work
  (`next_root_command`); the partition learns the re-grant like a drain's.
- **Surface.** `AdminCommand::RotateCredential` → `CredentialRotated`;
  `CredentialSummary` and the renewal reply gain `key_identity` and
  `rotations`; CLI `cluster credentials rotate`; MCP
  `cluster.credentials.rotate` (53 descriptors, cluster skill v15); the
  founder answers `unsupported`.
- **Tests.** `focal-enrollment` `a_rotation_changes_the_key_under_the_same_identity_and_the_old_key_signs_only_through_the_grace`
  (commit, retry, checkpoint round trip after the rotation, adoption,
  renewal under the new key, the old key refused past the grace);
  `focal-node` `a_joined_host_rotates_its_key_is_regranted_under_it_and_adopts_a_committed_rotation_after_a_crash`
  (a joined host rotates, its contact carries the new fingerprint, the root
  re-grants it under the new key identity, a renewal under the rotated key
  is ordinary, and a host restarted with the previous key and receipt beside
  the staged key adopts the committed rotation by itself).

**Evidence (macOS arm64, `--offline`).** Gate 95, 2026-09-10
13:29:52–13:46:43 CDT, on the final tree of this section: `test
--workspace`: 112 binaries, 2,546 passed, 3 failed; clippy, production
lints, fmt and the locked check clean; contracts 1,502 links, 37 imported
hashes, 15 frozen vocabularies. The three failures are startup and
election timing under the full run — `cli_network` (a node's listener
found its port already in use), `fleet_quic` (a membership removal and a
managed registration answered unknown while the replicas had no leader) —
and each binary passed whole when rerun alone immediately after (4 of 4
and 6 of 6); no test of this section failed.

## Repairing a session's custody (R9.3, third step, 2026-09-10)

Instruction 3 of R9 ([REMAINING §14](../REMAINING.md); [24](24-placement-execution-and-fleet-control.md)
§20; [08](08-stepped-complexity-and-deployment.md) §10: "missing
destination data triggers verified recopy, not fresh object identity"):
forward repair of the copies a placement already requires, and the pull a
fresh copy makes for the objects its committed history names.

- **The walk.** `cluster repair [--tenant T] [--session S] [--after A]
  [--limit N]` (`AdminCommand::Repair` → `RepairedReply` → `AdminResult::Repaired
  { repair: AdminRepair }`; MCP `cluster.repair`; 54 descriptors, cluster
  skill v16). The replica exports its committed prefix (the same
  `checkpoint_evidence` a backup takes) and the evidence coordinator walks
  its artifact projection as a trusted node job (`JobKind::Repair`) under
  the session's current placement: this node verifies each object through
  its own store (`verified`), pulls what it lacks or fails to verify from
  the content copies then the voters, chunk by verified chunk under the
  same identity (`repaired`), lists what no copy answers with
  (`unrecoverable`, bounded to 64, `unrecoverable_count` exact,
  `restore_required`), records its own receipt when it is a required copy,
  and re-asks every other required copy to verify, receipt or not,
  recording the answer or giving it the object (`push`, `pushed`). `limit`
  (256 default, 4,096 at most) and the export's 30 s lease bound one call;
  `complete` and `next_after` say where a following call resumes. The
  walk is idempotent and changes no placement.
- **The native projection.** `DurableEvidenceSnapshot` now carries, for a
  hosted native session, the content roots the committed rows name
  (`Core::native_content_roots`, whose `ContentRoot` variants now carry
  the artifact id or the retired claim id) projected as the
  `ArtifactEvidence` every custody consumer walks (`artifact_after`), so
  custody verification for readiness (§7), backups and repair all see the
  native objects; the prefix's `artifacts` counts them.
- **Fresh copies pull what their history names** ([24](24-placement-execution-and-fleet-control.md)
  §20, "A fresh copy's objects"): a recording custody reader names the
  objects a replay, a snapshot install or a legacy translation could not
  read (`PendingCustody`, `custody_pending`, `custody_objects_missing`);
  the host pulls them from the placement's peers (`pull_object`) and the
  retained delivery resumes; custody reads admit the nodes of an announced
  pending placement at its route, writes only the installed placement's.
  This is what let a native session with artifacts expand onto new hosts
  at all: before it, the added copies' deliveries stayed retained on a
  missing object and the plan never left `Custody`.
- **Corrupt chunks are replaced by verified recopies**
  (`install_transferred_chunk`; the custody host's `Open` counts a chunk
  that fails its hash as missing so the transfer resumes at it); every
  other install path still refuses a differing existing file.
- **Tests.** `crates/focal-node/tests/cli_repair.rs`: a native session
  with one artifact is expanded onto two hosts (three voters, two content
  copies), every node's repair verifies the object and is idempotent; a
  copy that lost its chunk recopies it and one that holds it corrupt
  receives verified bytes over it, both byte-identical to the original; with
  every copy's chunk gone the walk reports the object unrecoverable (both
  other nodes asked) and `restore_required`, manufacturing nothing; once one
  copy's bytes are back the holder pushes the object to the required copy
  that lacks it and the voter recopies it through its own repair; a walk
  bounded to one object resumes after `next_after`; a session this node
  does not host is refused. `focal-evidence` keeps its fail-closed install
  tests for seeds and custody records.

**Evidence (macOS arm64, `--offline`).** Gate 97, 2026-09-10
16:41:31–16:58:48 CDT, on the final tree of this section: `test
--workspace`: 113 binaries, 2,550 passed, 0 failed; clippy, production
lints, fmt and the locked check clean; contracts 1,506 links, 37 imported
hashes, 15 frozen vocabularies (gate 96 at 16:10 had the native projection
refusing legacy replicas, a delivery gate that stalled imports and one
custody-read expectation; all three fixed, gate 97 is the evidence).

## The upgrade fence (R9.3, fourth step, 2026-09-10)

Instruction 3 of R9 ([REMAINING §14](../REMAINING.md); [24](24-placement-execution-and-fleet-control.md)
§21; [08](08-stepped-complexity-and-deployment.md) §10): incompatible
behaviour activates only behind a committed fence every node supports, and
a binary behind the fence refuses to serve.

- **Levels.** `upgrade::CAPABILITY_LEVEL` (1) is what this binary
  implements; `announced_level()` is what it announces — the same, or a
  lower level set through `FOCAL_CAPABILITY_LEVEL` for a staged rollout or
  a rehearsal (never raised). Every load report carries it
  (`NodeLoad::capability`; the frozen V1 row codec restores it as zero,
  unknown; `AdminPlacementNode.capability` shows it).
- **The fence.** `UpgradeFence { level, activated_at, revision }` in the
  enrollment registry (schema 4; a schema-3 checkpoint restores with none;
  `encode_as_schema_three_for_tests` covers the upgrade), raised only by
  the founder authority through `Change::ActivateFence { level }`
  (`prepare_activate_fence`: zero or lower is invalid, the same level a
  conflict read as done; `EnrollmentCommand::activated_fence`), committed
  through the quorum enrollment host (`QuorumEnrollmentHost::activate_fence`).
- **Surface.** `cluster upgrade status` / `cluster.upgrade.status`
  (`AdminCommand::UpgradeStatus` → `UpgradeReply` → `AdminResult::Upgrade
  { upgrade: AdminUpgrade }`: the fence, this binary's compiled and
  announced levels, every directory-listed node with the level it last
  reported, `activatable` = the least reported level) and `cluster upgrade
  activate --fence LEVEL` / `cluster.upgrade.activate`
  (`AdminCommand::ActivateFence`, founder only → `AdminResult::FenceActivated
  { upgrade, changed }`; `members_behind` exit 5 names nodes reporting less
  or none, a lower level is `invalid_input`, the same level reads as done);
  56 descriptors, cluster skill v17.
- **Refusal.** The network controller checks the fence against the
  announced level every time it observes the root and stops with
  `ControllerError::Fenced` (exit `upgrade_fenced`, 5); `NetworkService::open`
  makes the same check against the registry its root replica has applied,
  so a rolled-back binary neither starts nor keeps serving. Nothing is
  gated on a level yet (`upgrade::opened`); the fence's first work is the
  rollback refusal R10's upgrade qualification needs.
- **Tests.** `crates/focal-node/tests/cli_upgrade.rs`: a founder and a
  host both report level 1 and no fence; a host may read but not raise
  the fence (`unauthorized`); level 2 is refused naming both nodes; zero is
  invalid; the fence rises to 1 exactly once (`changed` true, then false at
  the same revision); the host observes it; restarted announcing level 0
  the host exits 5 with `[upgrade_fenced]` and publishes no readiness;
  restarted at level 1 it serves again, also when announcing level 1
  explicitly. `focal-enrollment`
  `the_upgrade_fence_rises_once_under_the_founder_authority_and_survives_schema_three_checkpoints`.

**Evidence (macOS arm64, `--offline`).** Gate 98, 2026-09-10
17:03:43–17:21:11 CDT, on the final tree of this section: `test
--workspace`: 114 binaries, 2,553 passed, 0 failed; clippy, production
lints, fmt and the locked check clean; contracts 1,510 links, 37 imported
hashes, 15 frozen vocabularies.

## Topology facts and the residency fence (R9.4, 2026-09-10)

Instruction 4 of R9 ([REMAINING §14](../REMAINING.md); [24](24-placement-execution-and-fleet-control.md)
§22; [08](08-stepped-complexity-and-deployment.md) §6, §7): the
geographic executor, not only the solver. Before this batch every node was
granted with unknown geography (the controller granted region and zone
zero), so `survive: zone|region` could not be satisfied by any fleet and
`placement.residency` could not resolve; the planner filtered by residency
but nothing executed it.

- **Declared and announced.** `topology.region`/`topology.zone` (local
  configuration, at most 64 bytes; a zone declared without a region is a
  local fact that is never announced) ride on the
  node's contact (`Operation::NodeContact { region, zone }`,
  `NodeContactCommand`, `ContactRecord`; contact checkpoint schema 2 and
  control checkpoint schema 5 with the schema 3/4 shape decoded through
  `ContactCheckpointV1`). The controller announces them and re-announces
  when they change (`with_topology`).
- **Identities and grants.** `topology::region_id(label)` and
  `zone_id(region, zone)` derive directory identities from labels, the same
  on every node. The root leader registers a region the root does not know
  (`RootOperation::RegisterRegion` under the controller's evidence,
  `authority_epoch` 1; the founder registers every region its policy names
  ahead of a node running there) and grants each node with its identities
  and the region's epoch; a node whose announced topology changes is
  re-granted at its next generation like a rotated key (`next_root_command`,
  `root_admission_command`). Config refuses labels beyond 64 bytes; the
  wire refuses a zone without a region, and the controller announces a
  zone only with one.
- **The fence.** `placement_executor::ResidencyFence { residency, regions }`
  is installed with every session's custody scope
  (`EvidencePlacement::committed(.., fence)`; the agent keeps every listed
  node's region and re-installs a fence that changed under the same scope,
  which the coordinator accepts as a fence-only replacement) and checked
  before any byte moves: a sealed artifact's replication, a repair's pull
  or push and an obligation's ask skip or refuse a node outside the
  boundary; an operator's range move outside it is refused by name
  (`outside_residency`, exit 5, from the placement view) and again by the
  controller (`AgentError::Residency`). The founder's startup placement
  carries its own declared region.
- **Views.** `AdminPlacementNode.region/zone` (announced labels; a region
  known only by identity shows its hex), `AdminSessionPlacement.residency/home_regions`
  (labels), `ObservedNode.region/zone` for deployment observation;
  `TopologyLabels` joins the root's region registry and contacts.
- **Tests.** `crates/focal-node/tests/cli_zones.rs`: a founder and three
  hosts declare `ra/a1..a3` and `rb/b1` with residency `[ra]`; every node
  is granted and shown with its labels and the session with its residency;
  a zone-survival plan activates three voters in three zones of `ra` and
  never the `rb` host; a range move to the `rb` host is refused
  `outside_residency` naming the region; a region-survival plan cannot be
  placed. Unit: `topology` identities, `placement_executor` fence rules,
  wire label validation, contact checkpoint labels.

**Evidence (macOS arm64, `--offline`).** Gate 99, 2026-09-10
17:29:00–17:46:36 CDT, on the tree before this section's last two fixes:
`test --workspace`: 115 binaries, 2,554 passed, 2 failed —
`config::ownership_tests::precedence_is_command_line_then_file_then_creation_default_and_every_value_names_its_source`
(the precedence fixture declares a zone alone; the config rule refusing
that was removed and the controller now announces a zone only with its
region) and `cli_upgrade` (the host's activation precheck read a directory
view in which the founder's load had not arrived, so it named the founder
behind instead of refusing as unauthorized; the test now waits for both
nodes' levels in the host's view as it already did in the founder's).
Clippy, production lints, fmt and the locked check clean. Gate 100 runs on
the final tree and is recorded under R9.5 below.

## Metrics (R9.5, 2026-09-10)

Instruction 5 of R9 ([REMAINING §14](../REMAINING.md); [24](24-placement-execution-and-fleet-control.md)
§23; [08](08-stepped-complexity-and-deployment.md) §9): what a node
knows about itself, rendered for a scraper. The four readiness probes
were implemented with R9.3; this batch adds the metrics they are explained
by.

- **One sampled snapshot.** `focal_node::metrics::MetricsSnapshot` is
  built by the service every `SAMPLE_INTERVAL` (five seconds) from what it
  already owns — `MemoryBudget::stats`, the content host's `DiskStats` and
  staged uploads, `SharedWal::stats`, the fleet's status and the root
  replica's progress, every hosted replica's `AdminReplicaDiagnostics`
  (indices, apply lag, sequence, pending proposals, log kept beyond the
  checkpoint, retention floor and cursor lag, pending seeds and objects)
  joined with the directory's route and placement epochs and
  `effective_guarantee`, `PeerPoolStats`, the failure detector's view and
  counters, the credential's expiry and renewal and rotation counts, the
  placement agent's intents and admission report, and the committed
  upgrade fence — and published through a `watch`. Sessions beyond
  `MAX_SESSIONS` (512) are counted as truncated. Nothing is sampled on a
  caller's behalf.
- **Rendering.** Prometheus text exposition 0.0.4: `# HELP`/`# TYPE` per
  series, fixed `node` and `cluster` labels on every sample,
  `focal_node_info` carrying `role`, `region`, `zone` and `capability`,
  per-session series labelled by tenant and session, per-tenant admission
  queues, label values escaped.
- **Surfaces.** `OperatorRead::Metrics` → `OperatorReply::Metrics(String)`
  → `AdminResult::Metrics { text }` (bounded at 8 MiB); CLI
  `cluster node metrics` prints the text as it is (never JSON); MCP
  `cluster.node.metrics` (catalog 57 admin tools, skill `focal-cluster`
  v18). `node.metrics_listen` (loopback only, [08](08-stepped-complexity-and-deployment.md)
  §2) binds a `TcpListener` at open and `metrics::serve_loopback` answers
  `GET /metrics` over HTTP/1.0 — one connection at a time, a request
  bounded to 4 KiB and two seconds, `Connection: close`, `404` for another
  path, `405` for another method, no HTTP crate — read-only and
  unauthenticated by construction, which is why it never leaves loopback.
- **Tests.** `crates/focal-node/tests/cli_metrics.rs`: a founder with
  `node.metrics_listen` and a declared topology renders the fixed labels,
  the founder's session series and the fence over the admin socket, and
  the loopback endpoint answers `GET /metrics` with the same exposition,
  `404` for another path, `405` for another method and again `200`
  afterwards. Unit: `metrics::tests` (labels, escaping, derived lags).

**Evidence (macOS arm64, `--offline`).** Gate 100, 2026-09-10
17:55:21–18:13:22 CDT, on the final tree of R9.4 and this section: `test
--workspace`: 116 binaries, 2,558 passed, 0 failed; clippy, production
lints, fmt and the locked check clean; contracts 1,521 links, 37 imported
hashes, 15 frozen vocabularies. R9.4 and R9.5 close on this run.

## Packaging and reachability (R9.6, 2026-09-10)

Instruction 7 of R9 ([REMAINING §14](../REMAINING.md); [08](08-stepped-complexity-and-deployment.md)
§3, §5; [24](24-placement-execution-and-fleet-control.md) §24): service
supervision and Kubernetes packaging around the same binary, and the
reachability model a packaged host needs. Before this batch a node's
addresses were pinned at its first start (a restart with other addresses
was refused as another identity), contacts carried addresses only, and an
invitation named the founder's address, so a rescheduled pod could not be
found and could not start.

- **Reachability restated, not pinned.** `NetworkState` schema 2 carries
  the advertised name (`endpoint`); `startup_addresses` reports what the
  operator restated and `install` adopts it for the same node, sponsor and
  genesis (the join journal's addresses no longer override a later
  adoption); the controller announces a changed address or name as it
  announces a renewed certificate. `Operation::NodeContact { endpoint }`
  (a DNS host and a nonzero port, at most 259 bytes, validated at the
  wire and by the control owner), `NodeContactCommand`/`ContactRecord`
  `endpoint`, contact checkpoint schema 3 and control checkpoint schema 6
  (schemas 1–5 decoded). `PeerEndpoint.name`: the pool dials the announced
  address and, when it fails, re-resolves the name within the same
  deadline and tries at most four fresh addresses; the certificate check is
  unchanged. Invitations name the founder as it was started
  (`InviteIntent.endpoint`), a sponsor endpoint may be a name (resolved at
  each use, `resolve_endpoint`), and the founder's route falls back to the
  address the name resolved to at this start. A node that moved before
  applying its previous contact re-reads the root's contact table from
  the peer that refused its stale announcement and announces once more
  from the current generation (`ContactOutcome`), so a twice-moved node is
  never stranded behind an address the leader cannot reach. `cluster
  placement` shows every node's `advertise` and `endpoint`.
- **One command for a packaged host.** `start --invite-file FILE` enrolls
  when the directory holds no identity, then starts; `prepare-volume
  --owner UID:GID` creates the data directory for the node's user;
  `cluster invite --output -` writes the invitation to a pipe.
- **Rendering.** `deployment/render/{systemd,kubernetes}.rs` and
  `deployment render systemd|kubernetes`: deterministic files, never
  overwritten, every lacking fact named as `missing` (image, storage
  class, invitation secret, zones; a systemd host without an address),
  region survival refused by name. Kubernetes: headless Service with
  not-ready addresses published, founder StatefulSet plus one host set
  (node survival, spread by hostname) or one per zone (zone survival,
  node affinity, `2f+1` zones), per-set ConfigMap, disruption budgets
  (founder 0, hosts `max_failures`), an init step that gives the volume to
  uid 65532 with the same image, probes that ask the node (`probe --check
  alive`), pods advertising their StatefulSet names, `invitations.sh`
  issuing one invitation per host pod through `kubectl exec ... invite
  --output -` and installing the secret. Systemd: a hardened unit
  (state directory 0700, SIGTERM, `TimeoutStopSec=45` above the 30 s
  cleanup bound, restart on failure, no capabilities) and the
  configuration. `deploy/config/{kubernetes,systemd}.yaml` →
  `deploy/kubernetes`, `deploy/systemd` (goldens), `deploy/helm/focal`
  (the same objects templated), `deploy/container/Dockerfile` (the
  release's pinned musl image into `FROM scratch`, uid 65532, SIGTERM).
- **Tests.** `crates/focal-node/tests/cli_reachability.rs`: a founder
  advertising a name, an invitation through a pipe, a host that enrolls
  and starts in one command advertising a name, the placement view showing
  both contacts, the host restarted at another address without a name and
  again with a name at a third address, found each time under the same
  identity and generation. `crates/focal-node/tests/deployment_render.rs`:
  the checked-in manifests and unit are byte-for-byte the renderer's
  output for `deploy/config` (drift fails), the chart templates the same
  objects, the Dockerfile pins the release toolchain image; `helm template`
  runs when `helm` is installed and records otherwise that it did not.
  Unit: `deployment::render::tests` (sets, affinity, missing facts,
  refusals, the unit, shipped configurations reading back as the
  requested policy); `focal-wire`
  `peer_pool_re_resolves_a_named_endpoint_when_its_address_stops_answering`
  and the contact name rules; contact checkpoint schema 3 round trips.
- **Not executed here.** A run on a real Kubernetes cluster, the container
  image build (no crate registry offline) and `helm template` (no `helm`
  installed). The Kubernetes journey of R9.8 stands in with local
  processes.

**Gate 101 (macOS arm64, `--offline`), 2026-09-10 18:49:16–19:07:17 CDT,
on the tree of this section before its last fix:** `test --workspace`:
117 binaries, 2,355 passed, 0 failed, and the `focal-node` library test
binary aborted with a stack overflow in
`placement_agent::tests::the_controller_expands_a_laptop_session_to_three_hosts_that_survive_one_loss`
after its other tests passed; clippy, production lints, fmt and the locked
check clean. The overflow: the service's startup state machine
(`NetworkService::open_with_socket`, every recovered owner and handle
across its awaits) lived in the caller's future, and an in-process fleet
test that opens three services on one 2 MiB test thread crossed the edge
once this batch's awaits were added. The body is boxed now
(`open_with_socket` awaits `Box::pin(open_with_socket_inner)`, as
`run_until` already boxed `run_tasks`), so a caller's stack carries one
frame per service; the test passes at the default stack and the library
binary passes in full (211 tests). Gate 102 runs on the final tree and is
recorded under R9.7 below.

## Runbooks (R9.7, 2026-09-10)

Instruction 8 of R9 ([REMAINING §14](../REMAINING.md)): runbooks for
disk exhaustion, corrupt or missing content, stalled replication, node,
zone and region loss, stale clones, failed movement, expired credentials
and interrupted upgrade and restore, each executed against the real
binary. `docs/runbooks/` holds one file per failure with the same
sections (symptoms, read-only diagnostics, preconditions, commands,
preserved guarantee, stop conditions, verification, escalation, executed
test) and an index naming the recovery limits.

- **One rule the runbooks needed.** A live contact is never displaced
  ([24 §24](24-placement-execution-and-fleet-control.md)): before the
  root's data service forwards a contact announcement that would move a
  node, it probes the committed address itself
  (`DataService::contact_admission` → `LivenessHandle::confirm` →
  `PeerConnectionPool::probe_at`, a direct probe on a connection opened
  for that address, never a re-resolved name) and refuses while anything
  answers there as the node (`CompareFailed`); the detector's verdict is
  not the test, since a moved node refutes its suspicion from its new
  address. A copy of a node's disk started beside the node therefore
  cannot take its place, and a moved node is admitted one probe timeout
  after its old address stops answering. The first shape of this rule
  made the root wait on the probe inside the announcement: the reply came
  after the mover's one-second deadline, so a node that moved before its
  own replica applied its previous contact announced a stale generation
  for ever (the root could not reach it to replicate the newer one, and
  the `Stale` reply that would have had it re-read the table never
  arrived); `cli_reachability` failed every time the machine was busy.
  The driver now answers at once from a bounded cache of verdicts
  (`MAX_CONFIRMATIONS`, fresh for two probe caps), starting the probe
  when none is in flight, and an unknown verdict is `Unavailable`: a
  retry, never an admission.
- **A heal of a native session could never finish.** A fresh copy the
  agent installs for a plan served the default route (`ReplicaConfig::new`,
  route 1), while the leader probing a prospective learner for its format
  support speaks the session's current route; the copy refused every such
  probe as unauthorized once the session had activated once, the leader
  never recorded the learner's native promise, `AddLearner` stayed
  `Unsupported` until each membership call timed out, and the plan sat in
  `Catchup` for ever. The first expansion of a session (route 1) never
  showed it; every later expansion or heal did. A fresh copy now serves the
  session's current route, the plan's target route less one
  (`PendingPlacement::next_route` is the current route plus one), until its
  own log commits a fence (`placement_agent::attach_copy`). Found by
  `runbook_node_loss`.
- **Refusals are not blockers once the promise holds.** The directory's
  guarantee report lists a session-level refusal (`Refused(NoPlacement)`
  and the like) only while the achieved level is below the desired one,
  so `blocked_by` and `policy_satisfied` recover with the placement instead
  of carrying a refusal the controller recorded during an outage.
- **Delivered invitations.** An invitation file is read through links and
  may be group-readable (a mounted secret), never group-writable or
  world-readable (`check_invitation`); the journals a node writes stay at
  `0600`.
- **A retired credential stops serving.** The controller refuses at its
  next refresh, and the service at start, when the committed registry no
  longer authorizes the node's own certificate (revoked, or expired past its
  grace): `ControllerError::Retired`, `[credential_retired]` exit 5, beside
  the upgrade fence. A rotation in progress keeps the previous certificate
  authorized through its grace, so a node between the sponsor's commit and
  its adoption is not refused.
- **A credential's standing is visible.** `AdminPlacementNode.credential`
  (`active`, `retired`, `unknown`) comes from the enrollment registry the
  founder's root holds, so an operator sees a revoked or expired node as
  such even while it still answers probes (it authenticates its peers,
  they refuse it).
- **A retired node can still be drained and dropped.** The directory's
  topology grant demanded a live credential for every publication and
  every seat, so a revoked node could neither be withdrawn (`drain`
  refused `UnverifiedAuthority`) nor leave a group (`authority change
  group` refused while it held a seat) and the placement never healed.
  A withdrawal, a grant that only turns `eligible` off and restates the
  committed identity, region, zone, endpoint, authority epoch, principal
  and expiry, and a group change carrying an ineligible seat, tolerate
  `UnverifiedAuthority` and `Expired` from the credential check
  (`authority::apply` GrantNode, `validate_live_group`,
  `authority_proof::verify_enrollment`); anything that changes what the
  node publishes, or a live seat, still needs the live credential. Found
  by `runbook_expired_credentials`.
- **Tests.** `crates/focal-node/tests/runbooks.rs` over the shared fleet
  harness `tests/support/fleet.rs` (real processes, invitations through a
  pipe, one-command joins, pauses with `SIGSTOP`, the founder's placement
  view, one participant workload): `runbook_disk_exhaustion` (a founder
  under `ulimit -f` with `SIGXFSZ` ignored refuses an oversized write and,
  restarted without the limit, reads the earlier claim and commits a new
  one), `runbook_corrupt_or_missing_content` (a lost and a corrupt chunk
  repaired from another copy; a second repair finds nothing),
  `runbook_stalled_replication` (a paused voter is confirmed, the guarantee
  is blocked, writes continue, the resumed voter restores it),
  `runbook_node_loss` (a killed voter replaced by a spare and removed),
  `runbook_zone_loss` and `runbook_region_loss` (a lost domain's host,
  returned with its disk, restores the zone or region guarantee),
  `runbook_stale_clone` (a disk copy started beside the node is refused and
  admitted only after the node is gone), `runbook_failed_movement` (a
  range move whose destination dies finishes when it returns),
  `runbook_expired_credentials` (a revoked host drops out, is drained and
  removed, and the machine enrolls again fresh; revocation models expiry
  beyond the grace), `runbook_interrupted_upgrade` (a binary below the
  fence refuses to serve, the fence never lowers, the upgraded host
  serves), `runbook_interrupted_restore` (a restore whose node is killed
  as it is issued completes or is refused as done on the retry, and the
  claim reads back).

**Gate 102/103 (macOS arm64, `--offline`), 2026-09-10/11.** The runbook
suite (`runbook_<slug>` ×11) passes on the real binary, alone and beside a
second copy of the suite under load: the contact confirmation the root asks
before it moves a node's address no longer waited on the probe inside the
announcement (which came back after the mover's one-second deadline, so a
twice-moved node announced a stale generation for ever), and now answers at
once from a bounded cache of verdicts, so `cli_reachability` passes every
time the machine is busy. Gate 103 ran the whole workspace: 121 binaries,
2,579 passed, 1 failed, and that one was `cli_deployment`'s explain
assertion, tightened in the same batch (see R9.8 below) and re-run green;
strict all-target Clippy, the production no-panic gate, formatting and the
locked check are clean. **Gate 104 (macOS arm64, `--offline`),
2026-09-11 00:23–00:45 CDT, on the final tree of this batch** (the fixed
`cli_deployment` and both journey stages together): 121 binaries, 2,580
passed, 0 failed; Clippy, the production gate, formatting and the locked
check clean. Contracts verify 1,529 architecture links.

## Deployment journeys, laptop and VM stages (R9.8, 2026-09-11)

Instruction 9 of R9 ([REMAINING §14](../REMAINING.md); [08](08-stepped-complexity-and-deployment.md)
§11; DC01–DC20): the six stepped deployment journeys, each executed against
the real binary, each recording the operator concepts and inputs it needs
so the progression is measured, not asserted. This batch delivers the first
two stages and the recorder they share; the zone, region, Kubernetes,
workload and upgrade stages follow.

- **A recorded journey.** `crates/focal-node/tests/support/journey.rs`
  wraps the fleet harness: every command it runs is kept as a `Step`
  (the concept it needs, its inputs and the command with machine-specific
  values replaced by their kind), and at the end the recorded `Stage`
  (`builds_on`, the concepts it `introduces` over the stages it builds on,
  its `inputs`, the demonstrations it does `not` run and why, and the
  transcript) is compared byte-for-byte with the stage's entry in
  `tests/deployment/concepts.json`. A new mandatory concept fails the test
  until the file records it, so DC20's measure — an added concept needs an
  explicit decision — is enforced by the suite. The recorder also collects
  every command's output and `assert_redacted` searches it for the
  participant's and the node's invitation tokens (DC17).
  `docs/qualification/deployment-complexity.md` is generated from the file.
- **The laptop (DC01, DC02, DC04, DC13, DC15, DC16, DC17).**
  `crates/focal-node/tests/deployment_laptop.rs`: an empty directory, the
  native engine and one start on a loopback address; the claims demo
  between the node's principal and a participant enrolled on the same
  machine; a `SIGKILL` and a restart that read the same claim, artifact and
  receipt; a second writer refused (`directory_owned`, exit 6), an
  unwritable directory refused (`permission_denied`, exit 2) with nothing
  written under it, and a full volume (`ulimit -f`) refusing an oversized
  write with no acknowledgement; a backup restored on a fresh laptop as a
  recovery incarnation, and only with `--new-incarnation`, read back
  through a saved connection; `deployment explain` naming requested,
  effective and observed values against a golden; a dry run that writes
  nothing under the directory; and a plan of another deployment
  (`wrong_deployment`) and a tampered plan (`plan_corrupt`) refused before
  any side effect. Power loss and the MCP path are named as not executed
  here (the crash matrix is R11; `mcp_native_a1` runs the same claims over
  MCP). The laptop's one input beyond its directory is a loopback address:
  the native engine admits no self-issued work, so the second principal is
  a client context, which connects to the node's listener.
- **VMs or bare metal (DC03, DC05, DC06, DC16, DC19).**
  `crates/focal-node/tests/deployment_fleet.rs`, building on the laptop
  stage so it introduces only invitation, join, remove and drain: hosts
  enroll from invitations through a pipe and start in one command, and a
  replayed, tampered or revoked invitation enrolls nothing; a durability
  intent (`survive: node`, `max_failures: 1`) planned and applied derives
  three voters and advertises the stronger guarantee only once the copies
  are ready, with the demo unchanged before and after and read back through
  the participant's own connection; a two-failure policy the three hosts
  cannot provide is planned as blocked and its apply refused
  (`guarantee_unsatisfied`), the contract intact; the same request composed
  again resumes its journal as complete, and a plan made at an older policy
  revision, applied after another policy commits, is refused as stale
  (`stale_plan`, exit 5) before any side effect; a host that holds copies
  is not removed, a drained one leaves the guarantee visibly short until a
  replacement joins and the placement heals, and is removed only once
  nothing names it.
- **Explain observes the running directory.** `deployment explain` without
  an inventory now, on a node that runs a directory, plans against every
  node the directory knows (with its announced domains and standing) and
  reports each session's achieved level and what blocks it in an `observed`
  section, and `activated` is true only when every session has the
  effective level ([08](08-stepped-complexity-and-deployment.md) §9); a
  node that runs no directory, or an explicit `--inventory`, is explained
  against those facts alone. `cli_deployment`'s explain assertion was
  tightened to the observed truth (a three-host fleet makes `max_failures`
  1 valid and, once applied, active) and a single-node-inventory case added
  for the unmet path.
- **Two directory refinements this batch needed.** A plan is refused before
  any side effect when the directory has not yet reported this node
  (`DeploymentError::NotObserved`, `not_observed` exit 6), so a plan is
  never composed from a view that omits the node it would place. A drained
  node's guarantee report keeps a session-level refusal in `blocked_by`
  while the placement is short of its promise (a level below the desired
  one *or* a member the report already names), not only while the achieved
  level is below the desired one, so an operator sees why the controller has
  not healed a session whose sole voter is down.
- **A client resends an availability refusal within its clock.** A CLI
  request that a node refuses `Unavailable` (a leader change, a service
  still opening) is resent with backoff up to `UNAVAILABLE_RESENDS`, within
  the retry policy's deadline, and reported as the refusal it was rather
  than an unknown outcome or a transport failure; a workload that writes
  through the founder while the fleet reconfigures no longer fails on a
  transient leader change (`focal-client` `client.rs`, unit test
  `availability_refusals_are_resent_within_the_clock_and_reported_as_refusals`).
- **Shrinking the voter set by lowering `max_failures` (fixed).**
  Lowering a session's `max_failures` so the desired voter set is a proper
  subset of the current one (three voters to one) now completes, and the
  sole surviving voter serves the session's whole history
  (`crates/focal-node/tests/placement_downgrade.rs`). The bug was a
  membership-epoch model that a shrink's deferred removal did not satisfy.
  At plan time the directory set
  `pending.next_membership = active.membership_epoch + 1` because the
  placement's voter set differs (`SessionChange::Plan`). An expansion then
  steps the group's real epoch to that value before the cut-over
  (AddLearner then Promote commit membership changes), so the cut-over and
  activation fences carry an epoch the three checks agree on: the session's
  `validate_placement_transition` (cut-over needs `>= active + (voters
  differ)`), the directory's `validate_transition_fence` (needs `>=
  pending.next_membership`), and the authority verifier's
  `verify_session_fence` (needs `== the signed group's epoch`). A shrink
  makes no membership change before the cut-over — the dropped voters keep
  voting until activation retires them (24 §4, §19) — so the group's real
  epoch stays at `active`, one below what the plan and the transition-fence
  check demand. Forcing the fence to `active + 1` (the controller) satisfies
  the session and transition-fence checks and lets the cut-over barrier set,
  but the activation then fails the authority verifier, whose group epoch is
  derived from the signed proof and does not match. A correct fix has to
  reconcile the epoch across all three layers for a deferred removal — for
  example, deriving `next_membership` from whether the group configuration
  actually changes at the cut-over rather than from the placement voter set,
  or stepping the epoch at activation when the removal commits — and must be
  proven not to regress the expansion and heal paths. That is a focused
  protocol change left for its own batch. The supported shrink — removing a
  host — goes through `nodes drain` then `nodes remove` and works (DC19,
  `runbook_node_loss`, `deployment_fleet`); the max-failures downgrade is
  not required by any DC scenario, and the journeys use drain/remove for
  every shrink and never a voter downgrade.
  **The fix.** The membership epoch steps once per committed voter-set
  change, and a change happens before a cut-over only when voters are
  *added* — an expansion promotes them first, while a voter a placement
  drops keeps voting until activation retires it (24 §4, §19). So the four
  epoch-step sites now key on whether the new placement adds a voter the
  active one lacked (`Placement::adds_voter_over`), not on whether the
  voter sets merely differ: the directory's plan (`next_membership`) and its
  plan validation, and the session's cut-over and activation transition
  checks (`validate_placement_transition`). A pure shrink adds none, so its
  cut-over keeps the group's actual epoch — which the authority verifier
  (`verify_session_fence`) requires the fence to equal — and steps it only
  at activation, when the removal commits. An expansion adds voters, so the
  epoch steps before the cut-over exactly as before: the expansion and heal
  paths are unchanged (their tests, `placement_fleet` and the runbooks,
  still pass). A separate, pre-existing limitation remains: a client whose
  entry node is dropped from a session's voters (as the founder is by a
  full downgrade) does not yet rediscover the new leader through it, so the
  downgrade test reads from the surviving voter directly; the supported
  operator shrink (drain then remove of non-founder hosts) does not drop a
  client's entry node.

## Deployment journeys, all seven stages (R9.8, 2026-09-11)

Instruction 9 of R9 is complete: the six stepped journeys plus the workload
and rolling-upgrade demonstrations, each executed against the real binary and
each recording the operator concepts it needs so the progression is measured
(DC20). The shared recorder (`crates/focal-node/tests/support/journey.rs`)
compares each stage's recording byte-for-byte with
`tests/deployment/concepts.json`, so a new mandatory concept fails the test
until the file records it; `docs/qualification/deployment-complexity.md` is
generated from the file (243 recorded commands across the seven stages).

- **The stages.** `deployment_laptop` (DC01, DC02, DC04, DC13, DC15, DC16,
  DC17), `deployment_fleet` (DC03, DC05, DC06, DC16, DC19),
  `deployment_kubernetes` (DC07, DC08), `deployment_zones` (DC09),
  `deployment_regions` (DC10, DC11, DC12), `deployment_upgrade` (DC18) and
  `deployment_workloads` (DC14). Each builds on the earlier stages and
  introduces only its new concepts: the fleet adds invitation, join, remove
  and drain over the laptop; zones add the zone fact and zone survival;
  regions add the region fact, home regions, the measured peer latency and
  the residency fence; upgrade adds the capability level and the fence;
  workloads add the budget metrics; Kubernetes adds the render command.
- **What each demonstrates.** The laptop: one directory and a loopback
  address, the claims demo, a crash and restart reading the same records, a
  second writer and an unwritable directory and a full volume refused with no
  volatile acknowledgement, explain against a golden, a dry run that writes
  nothing, and a foreign or tampered plan refused. The fleet: one-command
  enrolment, a replayed, tampered or revoked invitation refused, a durability
  intent planned and applied that advertises the stronger guarantee only once
  the copies are ready, a two-failure policy the hosts cannot provide left
  blocked with the contract intact, an identical plan resuming and a stale
  plan refused, and a host drained, healed around and removed. Kubernetes:
  plain manifests with a volume per identity, secret references not plaintext,
  no CRD, and a migration (local processes for pods) that keeps the demo and
  its receipts across a killed-and-resumed joiner. Zones and regions: voters
  spread across domains, a lost domain surviving with the guarantee visibly
  degraded until it returns, the measured peer RTT shown
  (`focal_peer_rtt_ms`), and the residency fence refusing a move outside it.
  Upgrade: the fence rising only once every node reports the level, the demo
  surviving the activation, and a below-fence binary refusing to serve.
  Workloads: a sustained run staying within the node's RAM budget (used never
  exceeds the limit), a capacity refusal tolerated as the budget holding, and
  the guarantee and placement unchanged under load.
- **Measured RTT.** `MemberView.rtt_ms` records the last probe round trip per
  peer from the liveness driver's Vivaldi measurement; the metrics sampler
  renders it as `focal_peer_rtt_ms{peer}` (bounded by the fleet's member
  count), the operator's view of inter-node and so inter-region latency.
- **Harness.** The recorder tolerates a transient capacity or unavailable
  refusal (exit 6) on a workload command by resending within 30 s, so a
  moment of reconfiguration does not flake a journey; a definite error is
  returned at once. `Journey::start_refused` runs a node expected to exit
  (a below-fence binary) and returns its code; `Journey::admin_bare` runs an
  offline command that carries its own `--config` (a render).
- **Not executed.** A real Kubernetes cluster and `helm template` (neither
  available; the manifests are byte-checked against the renderer in
  `deployment_render`), a real newer binary at a higher capability level, and
  measured cross-region latency under a real WAN — each named in the stage
  that would run it.

**Close R9.** Instructions 1–9 are implemented and executed. The stepped
progression runs on the released interface with prior domain semantics, each
stage adds only its required inputs, the claimed failures are demonstrated,
and the effective guarantee is inspectable at every step; the runbooks and the
journeys are green, and the concepts study enforces the DC20 measure.

**Gate 106 (macOS arm64, `--offline`), 2026-09-11 10:19–10:44 CDT:** the full
workspace test run is 127 binaries, 2,586 passed, 0 failed, including the
seven journeys, the runbooks, the real-binary placement tests and the
voter-downgrade test. The production no-panic gate, formatting, the locked
check and the architecture contracts (1,532 links) are clean; strict
all-target Clippy is clean after one test-only `== false` was rewritten as a
negation in the Kubernetes journey (the assertion is unchanged and the
journey re-passed). This closes R9.

## Platform crate, fs2 removal and the unsafe boundary (R10, first step, 2026-09-11)

Decisions 10 and 11 (doc 03 §12, doc 10, doc 20): the platform-specific
filesystem behaviour moves into one crate, `fs2` is dropped for the standard
library's stabilised file locks, and `unsafe` is confined to one audited file.

- **`crates/focal-platform`.** A new crate with the process-scoped advisory
  file locks (`try_lock_exclusive`, `try_lock_shared`, `unlock`) built on the
  now-stable `std::fs::File` lock methods, mapping `std::fs::TryLockError` to
  `io::Error` so callers keep their `WouldBlock` classification; and
  `available_space(path)`, via `rustix::fs::statvfs` on Unix (no `unsafe`) and
  `GetDiskFreeSpaceExW` on Windows. The lock helpers are free functions, not
  an extension trait, so they never collide with the standard library's
  inherent `File` methods.
- **`fs2` removed.** Every call site (`focal-client/file_lock.rs`,
  `focal-enrollment/files.rs`, `focal-evidence/{seeds,store}.rs`,
  `focal-log/{lib,writer}.rs`, `focal-node/{cli/mcp,network_join,
  node_directory}.rs`) now uses `focal_platform`; `fs2` is gone from the six
  crate manifests, the workspace dependencies, `Cargo.lock` and the
  third-party inventory. The lock semantics are unchanged (the whole suite,
  including the same-user exclusion tests, passes).
- **The unsafe boundary.** `[workspace.lints.rust] unsafe_code` is now `deny`
  (was `forbid`), allowed in exactly one file,
  `crates/focal-platform/src/windows.rs`, which carries the one `unsafe` block
  behind `#[allow(unsafe_code)]` with a per-call safety argument.
  `scripts/check-contracts.py` fails if an `unsafe` keyword (block, `fn`,
  `impl`, `trait`, `extern`) or an `allow(unsafe_code)` appears in any other
  source under `crates/`, and if the audited file is missing — belt and
  suspenders to the compiler lint.
- **Windows compile-checked.** The Windows FFI type-checks for
  `x86_64-pc-windows-msvc` (`cargo check --target`, which does not link), so
  the `GetDiskFreeSpaceExW` call and the wide-string handling compile against
  the real `windows-sys` bindings even though this host cannot link or run a
  Windows binary.
- **Not executed here.** The rest of R10 needs a Windows host and a CI
  environment this macOS machine does not have, and is not started so that no
  nominal catalog entry ships without a runnable artifact: the protected
  Windows filesystem types (`PrivateDir`/`PrivateFile`, DACL ownership,
  reparse-point and hard-link defenses, write-through rename), the Windows
  named-pipe local transport, the Windows CI lanes and `platforms.json`
  entries, release signing/notarization, and the clean-machine install and
  upgrade checks. The Unix release lanes and the existing catalog are
  unchanged.

## Adversarial input allocation bound (R11, first step, 2026-09-11)

Correctness qualification instruction 3 of R11 ([REMAINING §16](../REMAINING.md)):
a maliciously large or malformed frame must be rejected before the decoder
allocates a buffer for the attacker's declared length.
`crates/focal-wire/tests/adversarial.rs` makes the bound machine-checked with
a counting `#[global_allocator]` in the test binary (a test-only harness;
decision 11 governs shipped code and `check-contracts.py` scopes the `unsafe`
ban to `src/`, so the allocator's `unsafe impl GlobalAlloc` is confined to the
test). One sequential test resets a peak-heap counter immediately before each
`read_frame` and asserts the peak growth: a header declaring `u32::MAX` or 8
MiB over the caller's limit is refused at the fixed header with under 256 KiB
of growth (never a buffer sized to the claim); a truncated header and a wrong
magic are refused with the same bound; and a within-limit declared length
whose payload is then truncated allocates only up to that declared bound plus
bounded overhead before failing. This complements the wire crate's existing
truncation and oversized-header unit tests with an explicit allocation ceiling.
The rest of R11 (black-box linearizability histories across the real
transports, the long-running fault campaign, and the performance/capacity
envelope on recorded hardware) is not yet built; the linearization checker
(`focal-sim::history`) and the crash-fault sites (`focal-node::fault`, exercised
by `cli_native_a4` and `placement_binary`) are in place for it.

**Gate 108 (macOS arm64, `--offline`), 2026-09-11 11:37–12:04 CDT:** the full
workspace on the final tree of the platform, downgrade-fix, deployment-journey
and adversarial work is 130 binaries, 2,589 passed, 0 failed; strict
all-target Clippy, the production no-panic gate (now with `unsafe_code` denied
outside the one audited file), formatting, the locked check and the
architecture contracts (1,532 links plus the unsafe boundary) are clean.

**R10 native Windows — first product gate green (Windows Server 2022 CI,
2026-09-11):** the dedicated Windows workflow (`.github/workflows/windows.yml`,
`windows-2022`, pinned Rust 1.94.1) builds the whole workspace and runs, all
green: the platform FFI suite (DACL/SID ownership, handle identity, locks,
paths — 7), the named-pipe local transport round-trip (`focal-wire` — 59), the
enrollment credential suite (`BootstrapAuthority`/`JoinKey` persistence, PKI,
invitations — 22), and the A1 product gate on the shipped **release** binary
(`cli_native_a1`: two participants complete a native claim cycle through the
real `focal.exe` — submit/post, receipt, work artifact, testament, validation,
derived acceptance — then survive a SIGKILL restart and exact retry — 1 passed).
Getting there fixed several genuine cross-platform durability defects at the
root, each also correct (and unchanged) on Unix:

- `sync_all()` on a **read-only** handle returns `ERROR_ACCESS_DENIED` on
  Windows (`FlushFileBuffers` requires a writable handle); Unix silently
  tolerates it. Every durability-critical read-path re-sync was redundant (the
  writer fsyncs before the file is visible; the directory fence recovers a prior
  ambiguous `sync_dir`) and was removed across the enrollment `PrivateDirectory`,
  the client operation/pending stores, the cluster-admin and MCP verify paths,
  and the backup medium. An exhaustive sweep confirms none remain in `src/`.
- Durable installs now publish through `focal_platform::fs::atomic_replace`
  (`std::fs::rename` + directory fsync on Unix; `MoveFileExW(REPLACE_EXISTING |
  WRITE_THROUGH)` on Windows, where there is no directory fsync), always closing
  the temp handle first (Windows refuses to rename an open file).
- The invitation/credential publisher (`network_join::write_private_new`) was a
  Unix-only stub; its Windows path now uses a new
  `focal_platform::fs::atomic_create_new` (`MoveFileExW(WRITE_THROUGH)` without
  `REPLACE_EXISTING` — atomic, durable, no-clobber). The Unix hard-link/nlink
  crash-recovery path is kept verbatim (the `renameat2`/`renamex_np` no-clobber
  rename would need libc FFI, which the unsafe policy confines to `windows.rs`).
- The network host recovers the content store and the founder's durable session
  on a `spawn_blocking` thread (like the founder bootstrap), off the async
  executor's shallow 1 MiB Windows main-thread stack.

A fast `cargo test -p focal-enrollment` Windows step runs before the multi-minute
release A1 build so a credential-persistence regression is caught in seconds and
names its failing operation and path. Still open for R10: the arm64 Windows lane,
folding Windows into the main CI matrix, cross-platform focal-node lib tests
(un-gating ~49 Unix-only `#[cfg(test)]` sites), and the release/signing/notices/
install-check lanes.

**R10 Windows CLI + MCP product gates both green (Windows Server 2022 CI, run
34672668845, commit 727278c):** the MCP A1 gate now runs beside the CLI gate —
two participants each behind their own `focal mcp serve`, the full native claim
cycle through MCP tools over stdio, a killed and restarted node, identical reads
and exact retries — and both pass on the shipped release binary. This closes the
plan's R10 criterion "the Windows CI lane runs platform, transport, CLI, MCP and
restart tests". `mcp_native_a1` was un-gated from `#![cfg(unix)]` the same way
`cli_native_a1` was (a no-op `private()` on Windows, a default-temp `scratch()`).
The last Windows durability defect it surfaced was one more read-only-handle
`sync_all` (the artifact-transfer store's `recover_marker`), removed; a
helper-aware sweep that follows `sync_all` receivers through read-only opener
helpers (`checked_open`, `open_file`, `open_link_pair`, `open_private` write=false)
confirms none remain in `src/`. Release qualification also advanced: a new
`scripts/release/notices.py` renders `THIRD-PARTY-NOTICES.txt` and an SPDX 2.3
`sbom.spdx.json` from `Cargo.lock` offline, cross-checked against the reviewed
dependency roster (which caught and dropped three stale `winapi` entries), and
both join the collected/verified/published release asset set; the release matrix
guard was corrected to require all eight targets (it still demanded only the six
Unix ones after Windows was added to the catalog).

**R10 Windows node library tests green (run 34675753864, 2026-09-12):**
`cargo test -p focal-node --lib` now compiles and passes on Windows (210 tests,
0 failures) and is a permanent Windows CI step. The full Windows lane is build +
platform FFI + named-pipe wire + enrollment + node lib + the CLI and MCP A1
product gates. Making the lib suites portable added a cfg(test) `set_test_mode`
helper (POSIX chmod on Unix, a no-op on Windows where a fresh temp directory
already carries an owner-only DACL) for the library test modules; the bin-only
`cli` module tree — whose cfg(test) code cannot reach a library helper across the
crate boundary — uses inline cfg(unix) setup instead. POSIX-only assertions
(mode, symlink and hard-link rejection; non-UTF8 `OsString` paths) are cfg(unix)
or cfg(all(test, unix)), their Windows equivalents (DACLs, reparse-point refusal,
link count) covered by the platform FFI suite. ci.yml was also aligned to the
release workflow's `--test-threads=4`, so the heavy in-process fleet and runbook
suites no longer oversubscribe memory (where the node correctly refuses at
capacity and an otherwise-sound test cannot outlast the transient refusal).
