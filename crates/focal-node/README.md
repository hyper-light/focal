# Node service

`focal start` owns one private data directory, one local session, one content store and one Unix socket. Identity, desired policy, consensus state, content and checkpoints survive restart. An exclusive directory lock prevents a second writer. Kernel credentials determine the local principal; request payloads cannot supply runtime authority.

Async ingress submits to a bounded queue and one blocking owner. The owner performs admission, fsync, publication and snapshot capture in order. A vanished caller cannot cancel an admitted mutation. `OutcomeUnknown` requires retrying the same request ID, epoch and operation. A periodic owner task expires graph views and durably advances due projection leases; an idle disconnected client cannot indefinitely pin the tail. Owner or listener failure initiates service shutdown. Shutdown stops ingress and checkpoints within a 30-second grace period; a timeout exits for crash recovery.

The executable currently provides `start`, `status`, `identity`, `request`, the exclusive embedded `demo`, and offline `deployment explain`/`deployment schema`. Network cluster commands are under implementation. Local startup rejects network configuration instead of claiming it has activated replication.

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

`fleet::ReplicaFleet` installs multiple authorized sessions in one physical node worker. It accepts existing sessions and tenant budgets, verifies their node/cluster identity and budget ancestry, and returns one host per ledger plus a replication receiver. A bounded ingress queue feeds weighted tenant scheduling; timer, ingress and dispatch slices are bounded so one busy session cannot monopolize the loop. Stopping one session leaves the others running. This is a composition API; dynamic discovery, placement and operator cluster commands remain separate integration work.

Construct the physical `SharedWal` with a node budget, each `DurableNode` with `open_on_wal_in`, and its `Session` with `from_node_in` under the same tenant budget. Session, consensus, graph, stream and ingress charges then roll up to the tenant and node. Idle sessions allocate publication queues only when needed. Completion commands use one exhaustive classification through byte admission, queue slots, scheduling and consensus; ordinary pressure cannot consume their reserved capacity. The WAL has its own node allowance because its disk owner serves multiple tenants.

`FleetReplication` owns the outbound receiver and its storage allowance. Its receiver, host clones and the worker share one lifetime guard; dropping the owner alone does not release reachable channel backing. The grouped tests exercise both remaining-host and remaining-receiver drop orders, 48 idle sessions, tenant pressure, multiple concurrent session quorums, physical-node isolation and WAL restart. They establish bounded composition behavior, not a throughput target. Session apply and disk waiting remain synchronous inside the worker.

## Replicated enrollment owner

`quorum_enrollment::QuorumEnrollmentHost` composes private signing custody with a multi-voter root `ControlHost`. `create` is explicit first initialization; every subsequent start uses `open`, which refuses missing journal state. Both return a cheap cloned host handle and one owned driver. The driver runs with a borrowed `EnrollmentControl` router; `LocalEnrollmentControl` adapts an existing control host and a server-owned Runtime grant. A fleet router may select replacement leaders while preserving the pinned cluster/group/genesis and signer principal. The signer does not own a hidden single-voter control replica.

A dedicated signer principal has one durable consecutive request stream. Before sending a mutation, the owner atomically saves its exact public `ControlRequest`. Unknown outcomes remain pending across task cancellation and disk restart. The owner resolves that request before preparing another mutation, checks its committed receipt/hash, then advances the journal. Definitive comparison rejections permit preparation at the new revision; invitations retain the same persisted secret. Parent records detect loss of a whole invitation draft directory. A failed persistence fence stops the owner, and reopening reconciles the previous or newly installed complete record.

`invite`, `redeem`, `revoke`, and `authorize_certificate` read the root through a quorum barrier and validate its public CA against the local signing key. A prepared certificate never becomes a successful response. The host implements `focal_enrollment::JoinHandler` for the existing pinned TLS enrollment listener. Retain `authority.server_identity()` before moving the authority into the driver. Certificate authorization returns only the assigned Node or Actor grant with server-owned tenant scopes; voter membership and Runtime authority are separate decisions. Callers must propagate revocations to live transport registries.

The queue, pending/reply allocations, journal and checkpoint restoration are bounded under the supplied `MemoryBudget`. Completed replies retain their reservation until received. The driver owns signing and private-file IO sequentially; the process may run it on a dedicated executor. Private CA custody remains at that signer and its explicitly pinned bootstrap endpoint. Root leader replacement is supported through routing; automatic CA migration, renewal and operator CLI wiring are separate work. Existing `RootEnrollment` remains the local founding compatibility owner.

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

These are tested composition APIs. Invitation/join CLI wiring, learner-to-voter
promotion, live directory authority installation, expanded-root service recovery
and geographic placement remain unfinished. `FoundingNetwork` explicitly refuses
an already expanded root or the legacy local enrollment bootstrap; it cannot
reset either into a new one-voter deployment. The executable still exposes the
local service described above.


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
untrusted endpoint hints. Membership admission, replicated serving, credential
renewal and CLI composition remain separate work.

These APIs use owned state and introduce no Arc. Fixed file/frame limits bound
this bootstrap path; they do not provide complete node-wide allocator accounting.
Existing codecs and TLS retain their documented allocation/unwind boundaries.
Tests cover private atomic outputs, pin/role mismatch, complete key/CSR retry,
missing JOIN/key state, directory races, identity conflicts, forged receipts and
an actual pinned QUIC enrollment with a deliberately lost first response.
