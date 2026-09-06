# Directory, placement, and fair admission

This crate implements bounded, deterministic control state and scheduling primitives for P09/P13. `RootDirectory` owns only region records and nonoverlapping namespace delegations. Each `DirectoryPartition` owns an explicitly bounded namespace, enrolled node facts, and that namespace's session descriptors. Normal claim operations require neither root lookup nor a fleet-wide session map.

## Durable hosting contract

`RootCommand` and `PartitionCommand` are serializable CAS commands. Their owners expose `prepare`, `Prepared*Update::checkpoint`, and `publish`. Preparation validates the command and reserves a full next version before mutation. Dropping preparation rolls back reservations. Publication checks the exact owner/base version and swaps the prepared root without allocating. The host must commit the exact command to its authoritative metadata log before publishing and serialize preparation/commit/publication per owner. These types do not perform disk writes or consensus themselves.

Checkpoints contain a schema version, cluster identity, revision, delegation epoch, and complete bounded owner state. `restore` validates structural invariants; the host must additionally authenticate the checkpoint, bound its frame and collection sizes before deserialization, and verify its committed log provenance. Root and each metadata partition use independent consensus groups. Initial prepared versions clone their bounded metadata partition; they are not per-claim storage and are not yet an optimized incremental metadata tree.

`AuthorityVerifier` supplies pinned verified evidence for node enrollment, session control records, replica catch-up/content custody, and delegation activation. A nonzero hash is never accepted as a credential by this crate alone. Verification must be deterministic against authenticated evidence recorded with the command; it must not use ambient time or a remote call during replay. Enrollment binds infrastructure-issued region/zone labels, endpoint, identity, and incarnation. New incarnations invalidate old measured load reports.

## Placement and transfer

`propose_placement` orders eligible, measured nodes by ordering-home preference, active weight, available RAM, and stable node ID. It chooses independent requested failure domains and returns the exact load-report epochs it observed. `verify_placement` independently verifies voter quorum, materializer survival, and content survival against the worst allowed domain losses. A placement proposal does not execute node enrollment, learner joining, content transfer, or membership changes.

Session placement changes follow `Plan`, `BeginPreparation`, verified `Ready` receipts, a committed `Cutover` fence, then a committed `Activate` fence. All operations bind the operation ID, ledger/log identity, route epoch, membership epoch, placement epoch, exact node incarnations, and canonical placement digest. Active placement stays unchanged until activation has newer session-log coordinates and every destination role has caught up through the cut. Aborting after a committed cut is rejected. `ServingFence` must be installed from local committed session records and checked at the serving resource; cached routes grant no authority. A retired owner can finish pinned reads through the cut and rejects new mutations.

Metadata partition transfer requires a durable source `SealForTransfer`, authenticated destination readiness, and root CAS activation. The source permanently rejects further mutations. `install_transferred` compares the actual sealed source checkpoint digest and exact source/destination/delegation epochs before installing the destination. This crate implements transfer fences; it does not run a transfer reconciler or split/merge namespace ranges.

## Bounded routing and scheduling

`RouteCache` stores only queried sessions and their relevant partition watches. It enforces entry/partition limits, explicit monotonic-time TTL, deterministic LRU eviction, and scoped invalidations. A watch gap or ownership-epoch change clears only that partition's hints. Point lookups never advance a watch beyond unseen changes. A failed allocation leaves admitted entries intact apart from expiration. A new partition can be rejected at the watch limit even when an LRU entry could be evicted; callers may explicitly expire or recreate their bounded cache.

`FairScheduler<T>` provides node, tenant, and session byte/item quotas. Ordinary append/query/transfer work cannot consume reserved apply/completion/control capacity. Weighted deficit scheduling charges caller-supplied bounded work cost, rotates classes, and limits consecutive priority dispatches. `ScheduleOutcome::Continue` means the bounded visit slice has unfinished work; the runtime must schedule another slice without waiting for another enqueue notification. No worker or polling task is created for idle sessions.

`Dispatch` owns the admission permits through worker completion, so dequeuing does not prematurely release pressure. Duplicate work IDs remain rejected while queued or running. Cancellation is allowed only before dispatch. Failed admission drops partial reservations; completed weak-index entries and idle session quota machinery are reclaimed on reconciliation. Queue buffer capacity, indexes, permit backing allocations, and conservative map-node overhead are charged separately from payload heap capacity supplied by the caller. Mutable payload access requires the caller to preserve the admitted heap bound. Charges are conservative estimates for Rust standard collections, not a custom allocator's exact heap measurements.

Scheduling is single-owner and contains no global lock. Reconciliation currently scans the configured bounded job and tenant maps; planner calls allocate bounded scratch proportional to the supplied node set and should execute in a host-reserved control task. The crate establishes local correctness and admission contracts. Fleet-scale throughput, automated rebalance, cross-region transport, authenticated certificate enrollment, and production deployment remain host responsibilities.

Run `cargo test -p focal-directory --offline` and `cargo clippy -p focal-directory --all-targets --offline -- -D warnings` for recovery, transfer-fence, stale-route, quota, and fairness checks.

Mutable root and partition state, including unpublished candidates, are owned.
Publication checks process-local owner identity and base revision before moving
the prepared state. The scheduler retains one shared job lease solely for its
cross-thread completion seam: a worker owns the strong dispatch reference and
the scheduler observes a weak liveness reference to keep work IDs reserved until
completion. Dropping either owner cannot release a running worker's quota early.
There is no shared mutable directory state or global scheduler lock.

## Committed authority assignments and peer statements

`AuthorityRegistry` provides a concrete implementation behind `AuthorityVerifier`.
Its immutable `AuthorityAnchor` pins cluster, metadata log group, public genesis,
namespace and enrollment CA fingerprint. The host obtains that anchor from its
own trusted bootstrap configuration. It obtains registry checkpoints from its
own committed metadata log. An incoming peer cannot install an anchor or declare
its supplied checkpoint authoritative.

`focal-control` installs this registry through explicit root/partition activation
commands in their existing Raft logs. A trusted Runtime owner obtains a root
ReadIndex snapshot before proposing a partition installation; a Node discovery
response alone grants no installation authority. After activation, the control
owner rejects legacy directory commands and uses the installed verifier for
proof-bearing commands. Schema 2 checkpoints retain exact source identity,
public enrollment state and recorded time while old bootstrap hashes and legacy
schema 1 remain unchanged. See [control authority activation](../focal-control/README.md#explicit-installed-authority-activation).

`AuthorityCommand` binds the expected authority revision, exact enrollment
revision and a trusted, recorded decision time. `prepare` reserves the complete
replacement before cloning; `publish` accepts only the same owner/base and a
new actual committed Raft index. Dropped preparations release their reservations.
State, statements, signatures, endpoint bytes, memberships and checkpoint encoding
have independent bounds. Restoration checks the expected anchor and canonical
grant hashes. No background work, shared mutable registry or application `Arc`
is introduced by this implementation.

An authenticated infrastructure operator issues `GrantNode` with region, zone,
endpoint, authority epoch and generation. The grant's identity must match an
active Node enrollment, its principal must match the assigned principal, and its
expiry cannot exceed the certificate expiry. Generation starts at one and
advances exactly once per replacement. A Node certificate alone cannot select
these topology labels. `prepare` sets the canonical attestation digest; only the
published `registry.node(id).enrollment` may be supplied to directory enrollment.
Verification compares the complete grant and rechecks current enrollment
revocation and expiry. The identity hash is
`focal_enrollment::server_fingerprint(certificate)`, not the wire peer table's
separate certificate fingerprint domain.

`BootstrapGroup` installs a group's initial public genesis, exact scope and
voter/learner generations. It cannot replace a group or certify any application
prefix. `ChangeGroup` requires enrolled peer signatures over the exact committed
configuration record, next configuration, previous membership epoch, group
genesis, authority/enrollment revisions and expiry. The verifier counts signers
against its installed voters, including both majorities during joint consensus.
It rejects duplicate certificates, nonmembers, stale incarnations, scope changes
and configuration replacement outside the single-change/joint transition rules.
Issuing credentials or topology grants never promotes a voter.

`AuthorityStatement::sign` is a signing primitive. A producer must derive a
session fence from its actual committed session authority record, a replica
readiness statement from its durable local custody result, and source/destination
delegation statements from their respective committed partition state. The
verifier requires the installed session quorum for a session fence, the exact
assigned replica for its own readiness, and both installed partition quorums for
a delegation. Every signature covers the full statement and is checked against
an active enrolled Node key. Readiness sets `attestation` to zero in the signed
body and uses `AuthorityProof::attestation()` in the presented receipt.

The embedding host must authenticate operator mutation rights, establish current
metadata with ReadIndex when required, and record the decision time and bounded
proofs with the consuming command. Deterministic replay uses the exact authority
and enrollment versions installed at that command's position. A later arbitrary
snapshot or wall clock cannot replace those versions during historical replay.
New admission uses current versions, so revocations and generation changes take
effect. The verifier performs no remote IO and reads no ambient clock.

**Integration boundary:** `focal-control` now supplies append-compatible
activation, committed mutation/installation, proof-bearing commands and recovery.
`ControlBootstrap` intentionally remains unchanged to preserve existing genesis
hashes. Operators still need to orchestrate snapshot propagation; no background
refresh or global revocation freshness policy is provided. There is no production
session-fence, custody-attestation or delegation-signing producer here. Their
absence fails verification; the tests do not fabricate a successful session
fence. Tests cover actual Raft publication of topology metadata, a real
three-voter committed configuration change signed by enrolled peers, partial
quorum/duplicate/nonmember rejection, revocation, bounded failed preparation,
namespace/generation checks, and checkpoint restoration.
