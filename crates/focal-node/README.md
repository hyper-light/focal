# Node service

`focal start` owns one private data directory, one local session, one content store and one Unix socket. Identity, desired policy, consensus state, content and checkpoints survive restart. An exclusive directory lock prevents a second writer. Kernel credentials determine the local principal; request payloads cannot supply runtime authority.

Async ingress submits to a bounded queue and one blocking owner. The owner performs admission, fsync, publication and snapshot capture in order. A vanished caller cannot cancel an admitted mutation. `OutcomeUnknown` requires retrying the same request ID, epoch and operation. A periodic owner task expires graph views and durably advances due projection leases; an idle disconnected client cannot indefinitely pin the tail. Owner or listener failure initiates service shutdown. Shutdown stops ingress and checkpoints within a 30-second grace period; a timeout exits for crash recovery.

The executable provides local service commands and the live founder/invitation/join workflow described in [Start and join a network](../../docs/network-startup.md). A network start uses saved authenticated endpoints and the same physical identity; plain local startup remains the default before networking is requested. Offline `deployment explain` and `deployment schema` do not apply placement changes.

Successful network shutdown also waits for Quinn to release the listener socket, within the existing 30-second grace period and after stopping the physical owners. Closing an `Endpoint` alone leaves its connection drivers alive temporarily. The listener replaces Quinn's required shared runtime allocation with a delegating runtime whose final drop signals completion; its fixed bookkeeping allowance stays with that runtime through cancellation. Quinn is pinned to 0.11.11 because this guarantee relies on its endpoint and connection states dropping socket ownership before runtime ownership. A dependency upgrade must recheck that ordering and the immediate-rebind regression.

## Durable subscriptions

Open a request epoch with `Operation::OpenEpoch`, then use `Operation::Stream`:

1. `Open { seed: false, start: None, ... }` registers a consumer at the beginning. An unavailable history prefix returns resync; it never silently starts at the retention floor. An explicit start deliberately selects a different prefix.
2. `Poll` takes a full generation/scope cursor. Its `cursor` selects a delivery continuation; it does not acknowledge the bytes. `acknowledged` asserts the projection's durably installed position. That acknowledgment and lease renewal commit atomically before a reply.
3. On reconnect, resume from the last locally saved position. Unacknowledged events remain replayable with identical delta IDs. Retrying a request does not create a new durable cursor receipt, although data may reflect later committed progress.
4. `Open { seed: true, ... }` captures a snapshot and immediately commits a tail retention pin at that prefix. It returns a first page. Fetch remaining pages using `ReadConsistency::Exact(seed.token)` and the page's typed continuation. Install the snapshot durably, then send `CompleteSeed`; subsequent polls replay the tail after the captured prefix. A new seed increments the generation and fences old cursors.

Seed currently supports `DeltaFilter::All`; filtered live replay is supported. Snapshot views expire after 30 seconds, projection leases after 60 seconds without renewal. An expired snapshot requires a new seed, not a newer page under the old token. Caller-owned projections may assert their own progress; these acknowledgments are not evidence that an external effect executed. Protected proof/effect consumers cannot use this generic network interface.

The stream uses bounded item, byte and scan credits. `Resolved(S)` means the complete sequence prefix was visited; an ordinal cursor covers only that delta. The legacy delta-only `Subscribe` operation remains explicitly unsupported because it cannot represent complete-prefix progress without ambiguity.

## Evidence and reads

Upload IDs are derived from authenticated principal, tenant, session and the caller's ID. `Begin`, `Append`, `Seal` and `Download` support durable offsets, identical retransmission and verified bounded chunk ranges. Sealing establishes one local durable copy; distributed custody is a separate acceptance gate.

`FleetService` composes the replica, node content owner and bounded evidence coordinator. A fleet seal waits for every required copy in its verified placement; artifact admission carries a private request/placement witness into the Raft owner. Exact committed retries use the original receipt before checking current custody availability. A cold leader fetches and verifies content for a new attachment. The public node-only custody protocol proves one disk's durable copy per response; clients cannot submit aggregate custody authority.

Local and fleet handlers move response allocations into `OwnedResponse`. Network adapters keep them through send completion or cancellation. Admission reserves construction space before a read, then releases unused capacity after constructing its response. Preferred upload sizes do not restrict imported content-format chunks; downloads may return fewer bytes than requested with an exact offset and end-of-content indication.

Reads use the graph store's leased immutable roots. Linearizable reads pass a Raft read barrier. Exact pages remain bound to their principal and original prefix. Point reads and typed object scans are implemented; wire traversal remains unsupported until its continuation contract is complete.

Tests include actual binary startup, Unix credentials, SIGKILL, WAL recovery, exact mutation retries, resumable content transfer and durable stream acknowledgment recovery. Unit integration tests cover snapshot/tail handoff, cursor generation fences and idle lease expiry. These tests establish local behavior, not global capacity or completed deployment qualification.

## Grouped session ownership

`fleet::ReplicaFleet::spawn` installs an initial set of authorized sessions in one physical worker. `spawn_managed` starts an empty worker with a bounded session count, management queue, tenant registry and pre-retained physical WAL set. Both use the same weighted tenant scheduler and bounded timer, ingress and dispatch slices. A retained asynchronous WAL write delays its own session while unrelated sessions remain runnable.

Construct each physical `SharedWal` under the node budget, each `DurableNode` with `open_on_wal_in`, and its `Session` with `from_node_in` under the registered tenant budget. Dynamic installation checks node, cluster, ledger, budget ancestry, configured route and exact process-local writer identity before enqueueing. An invalid candidate is returned to the caller. Every approved physical writer remains owned until the entire fleet stops, so retiring its last logical session cannot join a stalled disk thread on the shared worker.

One trusted controller owns consecutive management sequence numbers. `FleetManager::install(sequence, FleetReplica)` returns an incarnation and host. A canceled or lost reply may still have installed the session: use `inspect(ledger)` or `retry_install(sequence)` before opening another logical WAL lease. Only the latest successful management operation has an exact retry receipt; older operations return `RetryExpired`, conflicting reuse returns `Conflict`, and gaps return `OutOfOrder`. These receipts describe local process ownership, not committed placement authority. After process restart the controller reconciles durable placement and reconstructs authorized sessions.

Replacement requires stopping the old host, then `remove(next_sequence, ledger, incarnation)`, then installing a newly constructed session. Removal requires the exact stopped incarnation and never deletes or truncates its WAL. The trusted controller must already hold unassignment authority. Old queued work and old host clones cannot address the replacement even when the ledger and group IDs match. Dropping the last manager stops its fleet; explicit `shutdown` plus `ReplicaOwner::join` provides orderly owner teardown. Interrupted proposals retain their existing unknown-outcome semantics.

Management replies retain their queue slot and byte permit through delivery. Each installed incarnation owns its metadata allowance inside the existing progress watch, including after owner shutdown while a host or delayed reply survives. `FleetReplication`, all hosts, the manager and the worker share one physical queue allowance. That single existing `Arc<Allocation>` is necessary because these independent lifetimes outlive different session watches; no per-session shared-allocation wrapper is added.

`ManagedService` routes every verified application ledger through that same bounded installation registry. Tenant authorization precedes lookup. A short watch borrow clones one current host and ends before any await; ordinary Raft/read requests do not use a management RPC. The selected incarnation stays fixed through receipt lookup, evidence custody and proposal, so an in-flight artifact cannot migrate to a replacement owner. The existing `FleetService` implements the evidence path without duplication. Custody requests still use committed content-copy policy on nodes without a local application replica. Empty, stopped and quiesced application routes return typed unavailability.

The service retains a manager clone. `FleetManager::stop_all` first quiesces routing and new installation, then stops one current incarnation at a time and shuts down the worker. It clones neither the full map nor a lock guard across awaits. Host stop errors remain unknown, and the caller supplies an overall grace deadline. Canceling `stop_all` leaves the fleet quiesced; resume it or invoke immediate `shutdown`, then join the physical owner. The latter remains a fail-stop operation and does not promise per-session checkpoints.

`ReplicaHost::propose_placement` is a trusted in-process operation. It waits for durable session application and a fresh quorum ReadIndex before returning `PlacementReply`. `into_witness` transfers the opaque, separately charged `CommittedPlacement` to the control proof owner. A Node transport identity alone cannot call it. Preserve the exact placement request after an unknown outcome. A committed cutover closes application/probe ingress and queued reads/streams; Raft traffic continues. Activation with a new route leaves old serving metadata closed until trusted removal/reinstallation supplies that route. Recovered active routes must match the installation configuration.

Completion commands use one exhaustive classification through byte admission, queue slots, scheduling and consensus. Ordinary pressure cannot consume their reserved capacity. Tests cover 48 idle sessions, tenant pressure, multiple concurrent quorums, physical-node isolation, WAL restart, canceled installation, bounded receipts, stale-incarnation rejection, committed placement replay, route fencing and retained-writer shutdown. These establish bounded composition behavior, not global throughput or automatic placement reconciliation.

## Initial directory owner

A `FirstDirectoryPlan` describes stable coordinates. Only a committed root-owner ReadIndex can mint its `PartitionBootstrapPermit`. `ControlHost::spawn_directory(permit, wal, budget)` returns the host, physical owner and `DirectoryReplication` synchronously. The caller registers that owner before awaiting readiness. Its one bounded thread performs WAL recovery and authority activation, then enters the ordinary control loop; there is no detached bootstrap task or second bootstrap thread.

Initial progress identifies the assigned group/node with zero leader and applied prefix. Startup failure closes ingress and marks progress stopped. The existing private progress watch owns the fixed stack and channel allowance, and both escaped host clones and `DirectoryReplication` retain it. `drive_directory_replication` keeps this receiver alive through pending sends. Tests pause the real WAL to verify that registration precedes blocking recovery and exercise success/failure drop orders. This is one initial physical metadata owner; grouping many metadata partitions remains separate work.

## Replicated enrollment owner

`quorum_enrollment::QuorumEnrollmentHost` composes private signing custody with a multi-voter root `ControlHost`. `create` is explicit first initialization; every subsequent start uses `open`, which refuses missing journal state. Both return a cheap cloned host handle and one owned driver. The driver runs with a borrowed `EnrollmentControl` router; `LocalEnrollmentControl` adapts an existing control host and a server-owned Runtime grant. A fleet router may select replacement leaders while preserving the pinned cluster/group/genesis and signer principal. The signer does not own a hidden single-voter control replica.

A dedicated signer principal has one durable consecutive request stream. Before sending a mutation, the owner atomically saves its exact public `ControlRequest`. Unknown outcomes remain pending across task cancellation and disk restart. The owner resolves that request before preparing another mutation, checks its committed receipt/hash, then advances the journal. Definitive comparison rejections permit preparation at the new revision; invitations retain the same persisted secret. Parent records detect loss of a whole invitation draft directory. A failed persistence fence stops the owner, and reopening reconciles the previous or newly installed complete record.

`invite`, `redeem`, `revoke`, and `authorize_certificate` read the root through a quorum barrier and validate its public CA against the local signing key. A prepared certificate never becomes a successful response. The host implements `focal_enrollment::JoinHandler` for the existing pinned TLS enrollment listener. Retain `authority.server_identity()` before moving the authority into the driver. Certificate authorization returns only the assigned Node or Actor grant with server-owned tenant scopes; voter membership and Runtime authority are separate decisions. Callers must propagate revocations to live transport registries.

The queue, pending/reply allocations, journal and checkpoint restoration are bounded under the supplied `MemoryBudget`. Completed replies retain their reservation until received. The driver owns signing and private-file IO sequentially; the process may run it on a dedicated executor. Private CA custody remains at that signer and its explicitly pinned bootstrap endpoint. Root leader replacement is supported through routing, and the live service wires local invitation administration through this owner. Automatic CA migration and credential renewal remain separate work. Existing `RootEnrollment` remains the local founding compatibility owner.

The quorum enrollment tests use three actual disk-backed control replicas and authenticated peer dispatch. They cover minority refusal, leader replacement, lost submit/commit responses, exact token/CSR recovery after signer and root restart, concurrent metadata comparison changes, missing private state, expiry, revocation, rogue CAs, grant roles, and queue accounting. TLS pin-before-token and wire bounds are independently covered by the enrollment transport tests.

## Network bootstrap and one-port transport

`NodeDirectory` owns the physical identity and exclusive directory lease used by
both local startup and network bootstrap. An initialization marker prevents a
lost identity file from silently creating a replacement cluster. Existing
identities gain the marker without changing their format or IDs. Interrupted
join/network state blocks standalone initialization. Validated join installation
is an exact, idempotent operation; the caller must first verify the pinned
sponsor's enrollment and bootstrap.

`FoundingNetwork::open` preserves the local session, node and physical WAL while
installing a distinct root metadata group. The private CA, client-owned key and
exact founding enrollment draft precede root initialization. A current-term
ReadIndex barrier and authorization against the recovered enrollment registry
precede enabling the founder credential. A revocation committed after genesis
therefore survives restart. Missing previously initialized keys or public state
fail recovery instead of creating replacements. Typed errors distinguish invalid
identity, unavailable quorum, unsupported owner transitions and resource failure.

The bounded `NETWORK` manifest pins the immutable initial root genesis, founding
identity, CA and resolved addresses. Live metadata grows in the root log, not
this manifest. Recovery without repeated address flags uses those persisted
addresses; changes require an explicit reachability transition. Bootstrap
buffers carry owned memory allowances. The returned directory owner must remain
alive until all extracted stores and drivers have stopped.

`NetworkListener` serves issued-node data (`focal/1`) and pinned bootstrap
enrollment (`focal-enroll/1`) on one UDP port. ALPN selects distinct server
certificates. Enrollment may connect without a client certificate; data requires
a client certificate and a live registry grant even on the shared listener.
Peer metadata discovery uses bounded Node-only `Operation::PeerControl` reads
with a real ReadIndex barrier. Runtime-only control writes remain separate.
Connections and handshakes have bounded admission and deadlines; the connection
bookkeeping reservation is not a measurement of all Quinn buffer memory.

The live service wires founder startup, private invitation administration,
durable joining, authenticated contact/capability publication and root learner
admission. Every node runs a bounded managed fleet; the founder also commits and
opens the initial directory partition on the shared WAL. Its recovery path preserves expanded root membership; the strict
single-owner `FoundingNetwork::open` convenience API refuses an expanded root
rather than resetting it. See [Start and join a network](../../docs/network-startup.md)
for the executable workflow and restart rules. Application placement, root voter
promotion, evidence-copy placement and geographic deployment qualification remain
unfinished. Joining a root learner does not strengthen application durability.


## Durable joining identity

`network_join::NodeInvitation` binds a bounded node name, the immutable network
genesis and an enrollment invitation. Validation ties the cluster, public CA,
root group and root namespace together; only Node invitations are accepted.
The encoded bundle is at most 48 KiB. Its token-bearing buffers are zeroized and
Debug/errors omit secrets. `write_new` installs mode-0600 files without replacing
an existing different output. An output-specific private lock and temporary file
make exact retries recover partial writes and the hard-link/unlink crash window.
The parent directory must already exist; POSIX permissions are required.

`PendingJoin::open` holds the physical node-directory lock, persists the pinned
JOIN intent and then binds its persistent `JoinKey` request ID and CSR before any
network request. The root `JOIN.initialized` marker detects loss of the entire
private directory. Changed invitation/address/key inputs fail closed.
`resume` needs only the saved directory. The pinned `EnrollmentClient` exchange
retains the same request on an unknown outcome and persists a verified receipt
before returning it. `install` verifies that certificate against the saved key,
Node role and invited CA, preserves every founder domain identity, and installs
only the assigned physical node number. `JoinedNode::open` recovers an enrollment
that completed before an interrupted physical identity installation.

The resulting value creates no WAL, voter, domain replica, topology or Runtime
grant. `discover_root` performs a Node-only `PeerControl::Read(State)` against the
pinned founder endpoint, verifies the returned root identity and reauthorizes its
own certificate against that current root registry. It does not silently follow
untrusted endpoint hints. The [live network service](../../docs/network-startup.md)
then publishes the authenticated contact and capability and admits the node as a
root metadata learner. This does not grant an application replica, voter role or
evidence-copy assignment. Credential renewal and application placement remain
unfinished.

These APIs use owned state and introduce no Arc. Fixed file/frame limits bound
this bootstrap path; they do not provide complete node-wide allocator accounting.
Existing codecs and TLS retain their documented allocation/unwind boundaries.
Tests cover private atomic outputs, pin/role mismatch, complete key/CSR retry,
missing JOIN/key state, directory races, identity conflicts, forged receipts and
an actual pinned QUIC enrollment with a deliberately lost first response.
