# Stepped complexity and deployment contract

Status: target product and implementation contract. The local Rust binary,
configuration schema, and focused integration tests exist. Cluster commands and
deployment journeys below remain targets unless the [implementation evidence](09-implementation-status.md)
records them as executed. Use the [node service instructions](../../crates/focal-node/README.md)
for currently available commands.

The same Rust implementation must serve one laptop, several VMs or bare-metal hosts, a Kubernetes cluster,
several availability zones, several regions, and a global fleet. At each step the operator supplies only
new facts and decisions that cannot be derived safely. Capacity adds no ledger model, format, API, or shard-management concepts.

This is a separate acceptance condition from scale. A system that reaches the required
throughput but requires its operator to understand consensus internals to add a machine
does not satisfy this design. Conversely, concise setup must not conceal weaker
durability, missing isolation, an untested capacity claim, or an unresolved residency
decision. See [target architecture](00-target-architecture.md) for consistency and
[storage and distribution](04-storage-and-distribution.md) for the underlying mechanics.

## 1. The progression contract

Every stage retains the preceding stage's domain API, content identity, ledger history,
request receipts, subscriptions, and command semantics. A stage is a documentation
milestone, not a runtime mode or a profile that selects a different storage engine.
Stages may be skipped or combined: a bare-metal fleet can span zones, and a regional
deployment does not require Kubernetes.

| Stage | Smallest new operator concept | New required input | Derived or retained by Focal | What the step does not promise |
|---|---|---|---|---|
| 1. Laptop | Durable local workspace | None for default startup; optionally a data directory | Identity, local endpoint, disk log, RAM budgets, one-node placement | Survival of destruction of its only disk/node |
| 2. VMs / bare metal | Secure membership and desired node-failure tolerance | One reachable endpoint per host; one-use invitations; tolerated node failures if stronger protection is wanted | Credentials, bootstrap peers, placement, voter count, apply partitions | Zone/region survival merely because hosts have different names |
| 3. Kubernetes | Deployment packaging and persistent storage | Namespace and usable storage allocation; cluster access supplied by the operator | Manifests, stable identities, claims-volume mapping, requests from measured/resource inputs | Better ledger durability from rescheduling alone |
| 4. Multiple AZs | Failure domains | Verified zone labels; tolerated zone failures | Voter/artifact placement, disruption constraints, relocation plan | Region loss survival |
| 5. Multiple regions | Data geography and remote-commit tradeoff | Region labels, home-region eligibility, residency boundary, regional failure objective | Regional routing, session placement, secure peer discovery, failover placement | Low local write latency together with synchronous remote durability |
| 6. Global fleet | Workload admission policy across populations | Tenant/workload geographic policies and resource allocations where policies differ | Regional directory partitioning, cell sizing, hot-range placement, bounded routing | A global total order or unlimited throughput for one hot session |

The operator never has to choose a shard count, Raft term, routing epoch, WAL segment
ID, range split key, or materializer-worker topology. Those remain observable for
diagnosis and controlled experiments, but are not prerequisites to any normal stage.

The irreducible choices are what failures to survive, where data may exist, who may
join/access it, and what latency/cost/resource constraints to accept. Focal can derive
a placement that satisfies these choices; it cannot invent their business meaning.

## 2. One configuration schema, explicit intent

Use a versioned schema. `plan --config` updates the supplied policy fields; omitted fields
retain committed values or inherit their containing scope. Creation defaults apply only
to a new deployment, never as an implicit reset. Unknown keys fail with their full path.
Examples use YAML for human readability; Rust owns a typed canonical representation.

```yaml
version: 1
node:
  data_dir: /var/lib/focal
  listen: "0.0.0.0:7443"
  advertise: "node-a.example.internal:7443"
topology:
  zone: zone-a
  region: region-a
durability:
  survive: node
  max_failures: 1
```

This example describes an explicitly networked node and a desired one-node-failure
contract. The port and path are example operator selections, not required Focal
constants. A one-node deployment cannot satisfy that desired protection; preflight
must show the missing independent capacity and keep the existing guarantee unchanged.

| Field | Owner and semantics |
|---|---|
| `version` | Human configuration schema version; unrelated to wire or WAL version |
| `node.data_dir` | Node-local durable identity and storage; defaults to the OS application-data location |
| `node.listen` | Local bind address; default is local-only transport; explicit override for NAT/multi-interface environments |
| `node.advertise` | Endpoint peers can reach and authenticate; commonly the only networking field the operator sets |
| `node.seeds` | Optional bootstrap override for managed recovery/infrastructure; normal join persists a bounded peer set automatically |
| `node.max_tenants` | Tenants this node hosts sessions for at most, its own included (default 8, at most 1024); a placement that needs one more is refused by this node as a capacity refusal ([24](24-placement-execution-and-fleet-control.md) §10) |
| `node.metrics_listen` | An optional loopback endpoint for the read-only metrics text (§9); node-local, never a cluster fact |
| `topology.zone`, `topology.region` | Infrastructure facts, validated against the configured source of topology authority |
| `durability.survive` | Failure-domain class `node`, `zone`, or `region` |
| `durability.max_failures` | Number of simultaneous independent failures in that class to tolerate |
| `placement.home_regions` | Regions eligible to host the session's normal ordering authority; introduced when geography matters |
| `placement.residency` | Allowed regions for scoped data, including log replicas, artifact bytes, snapshots, archives, and derived persistent copies |

The default local contract is `survive: node, max_failures: 0`: acknowledged commits
survive process crash and power loss, followed by restart on the same intact,
durability-capable storage. It is durable, not replicated.
No ordinary deployment command enables memory-only acknowledgments. Test-only volatile
adapters must be structurally separate from production startup and visibly identified.

`node` and `topology` are local deployment facts; `durability` and `placement` are
cluster/session policy intent. They share a schema but have different authorities.
An operator may keep local facts in `node.yaml` and policy in `deployment.yaml`.
`start` can bootstrap a new store from combined input; on an initialized store it
cannot change committed protection or residency. It directs such changes to plan/apply.

Configuration precedence is explicit: command-line overrides apply only to eligible
node-local startup fields, then the supplied file, then creation defaults. A conflicting
cluster intent does not become a last-writer-wins startup option. `explain` names each
value's source. Implemented 2026-09-10 (R9.1, `crates/focal-node/src/config/`): the
schema check names an unknown key by its full path (`node.shards`) before the typed
parse; the store's committed policy lives in `POLICY` as `FCLPOL2` with a revision and a
hash (the original bare pair reads as revision 1, unchanged on disk); a start whose file
sets a policy field to another value is refused as `CommittedPolicyChange` naming the
field and directing to plan/apply, while omitted policy fields take the committed values
and are reported as `committed` at that revision; `deployment explain` prints `requested`
(the file), `effective` (the committed policy) and `sources` per field
(`command_line`, `file`, `creation_default`, `committed`). Identity keys, membership epochs, seeds learned from peers, placements,
and measured scheduling decisions live in managed state, not generated user YAML.

## 3. Stage 1: laptop, with durable storage from the first run

```sh
focal start
focal deployment explain
focal demo claims --session deployment-check
```

`start` runs in the foreground and logs its local endpoint/readiness. Run the other
commands from a second terminal; VM/Kubernetes packaging supplies its normal process
supervisor. Focal does not require a separate daemonization concept or silently fork a
background server. `join` and read-only inspection commands exit after their operation.

`start` creates a durable node/deployment identity once, opens the custom RAM store and
disk log, discovers local resource limits, and publishes a local client endpoint through
an OS-owned discovery file/socket. Startup does not require a port, certificate, shard
count, config file, account, or network access. Another OS user receives no access merely
because a local service is running. Restart uses the same identity, log, and sessions.

The disk location and guarantee appear in `explain`. An explicit `--data-dir` chooses
another path; an unwritable path or unsupported durability prerequisite fails startup
without falling back to temporary or memory-only storage. An exclusive writer lock
excludes another process using the same directory. Within a functioning cluster,
authenticated persistent node identity and committed incarnation fences reject stale
instances. Independent offline clones cannot be fenced without shared authority;
running both violates the single-authoritative-copy assumption and is unsupported.
Supported moves retire the source first. A restore whose source cannot be fenced must
explicitly create a new deployment/recovery incarnation, never claim uninterrupted
continuation of the old authority. This exception is disaster recovery, not ordinary scale-out.

The claims demo uses an embedded deterministic validator. It creates a directed claim,
streams typed evidence, closes a testament, validates it, and verifies satisfaction.
The qualification harness then crashes/restarts the server and reads the same request
receipt and evidence hash. The demo itself does not shut down the running server. This
same demo command remains the application-level acceptance probe at every later stage.

Implemented 2026-09-10 (R9.6, [24 §24](24-placement-execution-and-fleet-control.md)):
`focal deployment render systemd --config FILE --output DIR` writes the hardened unit
and its configuration (`deploy/systemd` is that output for `deploy/config/systemd.yaml`);
`start --invite-file` lets a supervised host enroll and start in one command; a host
restarted at another address, or advertising a name, is adopted and announced.

## 4. Stage 2: add machines through secure membership

The initial network expansion adds reachability and membership, not a replacement
database. An existing local node retains its deployment ID and serves as the sponsor.
`--advertise` derives the bind address when the advertised endpoint resolves to a local
interface; otherwise startup explains the need for a separate `--listen` mapping.

```sh
focal start --advertise node-a.example.internal:7443
focal cluster invite --node node-b --output node-b.invite
focal cluster invite --node node-c --output node-c.invite
```

Transfer each invitation through the operator's chosen secure administrative channel.
On its named destination, with the same supported binary:

```sh
focal join --invite-file node-b.invite --advertise node-b.example.internal:7443
focal start
```

`join` enrolls identity and persists configuration; `start` runs the service. A join
ticket contains the deployment identity, pinned trust root, bounded sponsor endpoints,
one permitted node identity, expiration, and a single-use enrollment secret. Files are
created with restrictive permissions; normal output never prints bearer material.
Consumption is durable, concurrent reuse fails, and expired/revoked tickets fail closed.

Peer authentication and transport encryption are mandatory on the first network hop.
The inviter proves administrative authority through the existing local identity or an
authenticated administrative connection. Joining grants a node role, not unrestricted
application-user or tenant administration. Trust roots and node certificates have a
rotation/revocation path before network deployment is production-qualified.

New members start as catch-up recipients, prove log/checkpoint and artifact custody,
and enter ownership through the existing fenced membership protocol. They cannot vote,
run validators, serve authoritative reads, or claim a session from a received snapshot
alone. Bootstrap peers are persisted automatically; operators do not edit a full mesh.

For one-node failure tolerance, the additional desired-policy file is only:

```yaml
version: 1
durability:
  survive: node
  max_failures: 1
```

```sh
focal deployment plan --config deployment.yaml --output fleet.plan
focal deployment apply --plan-file fleet.plan
focal deployment explain
```

The planner derives a valid quorum and artifact-copy layout and names the independent
hosts it needs. With insufficient capacity the plan is blocked, not silently reduced.
Joining capacity by itself does not change an acknowledged durability promise. Existing
sessions retain their old contract until the strengthening plan completes; new sessions
requesting the stronger contract remain unadmitted until it can actually be provided.

## 5. Stage 3: Kubernetes is packaging, with the same membership

Basic Kubernetes use requires no Focal operator, CRD, custom scheduler, service mesh,
or external consensus service. Ship versioned manifests and a Helm chart as alternative
packages around the same binary. A future operator may automate existing APIs; it must
not become a second placement authority or a prerequisite for ordinary deployment.

```sh
focal deployment render kubernetes --config deployment.yaml --namespace focal --output deploy/
focal deployment explain --manifest-dir deploy/
```

Rendered assets cover stable pod identities, persistent volumes, peer discovery,
service endpoints, restricted credentials, probes, resource requests, and disruption
constraints. Kubernetes StatefulSets provide stable network/storage identity, but the
ledger still owns replication and recovery. [Kubernetes StatefulSets documentation](https://kubernetes.io/docs/concepts/workloads/controllers/statefulset/)
describes the underlying identity and volume behavior.

The renderer requests only missing infrastructure facts. It can use an unambiguous
usable default storage class; if none or several candidates satisfy the requirements,
it returns the exact storage choice needed. For migration, volume size and RAM requests
come from measured retained bytes, live-state size, recovery staging, and the declared
retention/resource allocation. A new unmeasured deployment uses a documented bounded
bootstrap allocation and labels it unqualified; it cannot claim a measured envelope.

The operator reviews and applies generated assets with its existing Kubernetes tooling.
Rendering and dry-run do not alter a cluster. Enrollment uses bounded bootstrap tickets
delivered through the configured secret mechanism; rendered public manifests contain
secret references, not invitation or private-key values. The renderer emits a separate
restricted credential-installation action when no secret reference has been supplied.

Existing VM-hosted sessions move by adding Kubernetes members to the existing
deployment, catching them up, transferring fenced ownership, and draining old members.
Do not bootstrap another empty Focal deployment with the same name and call it migration.
Do not mount one writable ledger directory into two live pods. A pod identity requires
its own durable storage identity and its own member enrollment.

Liveness must not restart a healthy node merely because quorum is temporarily absent.
Readiness distinguishes process availability, catch-up, authoritative serving, and
policy satisfaction. A disruption budget complements Focal's membership checks; it
does not prove that arbitrary eviction preserves quorum or artifact custody.

Implemented 2026-09-10 (R9.6, [24 §24](24-placement-execution-and-fleet-control.md)):
`focal deployment render kubernetes --config FILE --namespace NS --output DIR [--image
--storage-class --secret --zone ... --nodes --volume --port]` writes the objects above as
plain manifests and a kustomization, names the facts it lacks (`missing`: image, storage
class, invitation secret, zones) and never touches a cluster; `deploy/kubernetes` is that
output for `deploy/config/kubernetes.yaml` and `deploy/helm/focal` templates the same
objects. Pods advertise their StatefulSet names, so a rescheduled pod is found again
through the name its contact carries; a founder pod's invitations name the founder. Not
yet executed: a run on a real cluster (`kind` or otherwise), the image build, and `helm
template` (neither `helm` nor a cluster was available where this was written); the
Kubernetes journey (DC07/DC08) stands in with local processes.

## 6. Stage 4: availability-zone survival adds domain facts and one intent

```yaml
version: 1
durability:
  survive: zone
  max_failures: 1
```

Zone identity is imported from the configured trusted infrastructure adapter or supplied
as a node fact (`topology.region` and `topology.zone` in the node's local configuration,
announced with its contact and granted as its failure domains; [24 §22](24-placement-execution-and-fleet-control.md)). Kubernetes commonly exposes `topology.kubernetes.io/zone` and
`topology.kubernetes.io/region`; topology spreading operates on such labels. See
[Kubernetes topology spread constraints](https://kubernetes.io/docs/concepts/scheduling-eviction/topology-spread-constraints/).
Names alone do not prove independence: operators own correct physical failure-domain
mapping, including storage and network dependencies shared by nominally separate hosts.

Focal validates the promised zone-failure combinations for ordering voters, artifact
custody, metadata routes, archive dependencies, and surviving RAM/recovery capacity.
The planner names any missing zone or dependent storage placement. It does not claim
zone survival because three pods happened to be scheduled on three machines.

The Kubernetes package derives required scheduling/anti-colocation constraints from
the chosen policy, and unsatisfiable placement stays pending. VM and bare-metal members
use the same planner with registered domain facts. No Kubernetes-specific durability
flag exists. The resulting plan uses the same `plan`, `apply`, and `explain` commands.

Qualification removes an entire zone, including its disks and network routes. A passing
test keeps every prior acknowledged claim/evidence pair retrievable, elects authority
only where quorum survives, and reports reduced protection until repair completes.
Reducing tolerance requires an explicit new policy plan; an outage never triggers it.

## 7. Stage 5: multiple regions add geography and a visible latency decision

Region membership uses the same invitation flow and trusted topology facts. Most
operators can leave node-level network details to their infrastructure adapter, while
retaining an explicit view of which endpoints and certificates are being installed.
The new user-facing policy is geographic:

```yaml
version: 1
durability:
  survive: region
  max_failures: 1
placement:
  home_regions: [region-a]
  residency: [region-a, region-b, region-c]
```

`home_regions` selects normal authority placement eligibility. `residency` is the hard
boundary for all scoped durable copies and derived state, not merely a leader location
(executed by the residency fence, [24 §22](24-placement-execution-and-fleet-control.md)).
The example permits regional survival using eligible remote locations while keeping
normal writes homed in region-a. During an authorized region outage, temporary authority
may run in a surviving residency region under the selected failover contract; `explain`
shows that implication before apply. An application requiring authority never to leave
its home region must choose unavailable-on-home-loss, not this regional-survival promise.

Preflight enumerates failure sets and rejects arrangements that lose a voting majority
under a promised failure. It reports measured inter-region latency and the write-path
consequence of remote durable acknowledgment. Two regions do not automatically suffice
for survival of loss of either region. A remote asynchronous backup is a separate
disaster-recovery capability with an explicit lag/recovery point, never an implicit
substitute for synchronous region survival.

No region names are guessed from IP geolocation or nearest RTT. Missing locality or
ambiguous residency blocks geographic placement and identifies the missing decision.
Existing data stays in its authorized region while the new plan is unresolved. Cloud
credentials and network/firewall provisioning remain infrastructure inputs, represented
as specific missing capabilities rather than an opaque failed deployment.

Cross-region transfer streams existing immutable records, checkpoints, artifact chunks,
request receipts, and cursor state. IDs, hashes, session sequence semantics, and wire
formats remain unchanged. Placement/membership epochs change through ordinary committed
reconfiguration; clients refresh routes without replacing their application integration.

## 8. Stage 6: global distribution adds policy populations, not global knobs

A homogeneous fleet can expand the existing region list and add members without learning
another configuration layer. Different tenant or workload obligations introduce named
policy bindings. The operator supplies allowed geography, failure objective, and resource
allocation for the population; Focal applies the same session placement rules within it.

Proposed administrative usage keeps policy content in the same versioned schema:

```sh
focal deployment plan --config eu-workloads.yaml --scope tenant:example-eu --output eu.plan
focal deployment apply --plan-file eu.plan
focal deployment explain --scope tenant:example-eu
```

For example, `eu-workloads.yaml` contains only the geographic policy delta:

```yaml
version: 1
placement:
  home_regions: [eu-a]
  residency: [eu-a, eu-b, eu-c]
```

Policy scopes are authenticated tenancy boundaries, not arbitrary string labels that a
client can self-assign to escape restrictions. More-specific policy may narrow allowed
regions or request stronger protection, but cannot broaden a containing residency fence
or weaken a mandatory parent guarantee without the appropriate authority. Conflicting
requirements yield an explicit unsatisfiable-policy result, not precedence guessing.

Directory partitions, regional cells, connection pools, dormant-session activation, RAM
range placement, and within-session parallelism derive from measured pressure and fair
admission budgets. Control metadata itself must partition: there is no all-session global
watch, full-mesh fleet membership feed, or global sequencer on ordinary request paths.
The user sees available capacity, bottleneck class, and the minimum additional resource
or policy decision. High-contention single-session work remains bounded by its ordering
and publication ceiling; the product cannot disguise that with fleet-wide averages.

Automatic scheduling consumes only already authorized resources. Adding cloud accounts,
expanding spend/resource allocation, moving data outside residency, weakening protection,
or splitting one session's semantic authority requires an explicit policy decision.
Measured CPU/RAM/disk/RTT inputs tune scheduling; they do not authorize those changes.

## 9. Introspection, plans, and safe explanations

```sh
focal deployment capabilities
focal deployment explain --session deployment-check --format json
focal deployment plan --config deployment.yaml --dry-run
```

`capabilities` reports supported binary/wire/storage versions, available local resources,
verified disk-flush support, transport reachability, identity readiness, known failure
domains, artifact/archive dependencies, and qualification state. “Unknown” stays unknown.
Every unsupported requirement has a typed reason and a named remediation.

`explain` separates requested, effective, and currently observed protection. It includes
configuration source, measured/assumed resource inputs, placement reasons, replica and
artifact custody, current degradation, blocking conditions, and the expected impact of
the next step. Internal IDs may be inspected, but ordinary explanations use “another
independent zone is needed” instead of “set three Raft peers and increase shard count.”

Plans are immutable versioned artifacts containing a deployment identity, observed
configuration revision, membership/resource facts, desired policy hash, ordered changes,
data movement estimate, guarantee before/during/after, and rollback limits. `apply`
rechecks prerequisites and refuses a stale plan before side effects; it never blindly
executes a plan against another deployment. Dry-run is read-only, including no ticket
creation, credential rotation, schema migration, membership change, or cloud allocation.

Implemented 2026-09-10 (R9.2, `crates/focal-node/src/deployment/`): `deployment plan
--config FILE` composes a plan from what the node observes — its committed policy
revision and hash and, on a node that runs a directory, every session the placement
view names with its route, membership and placement epochs, voters and achieved
guarantee, plus the nodes' liveness and disk — and what the file requests (omitted
policy fields take the committed values). Each session is asked as a dry run
(`cluster sessions plan --dry-run`): the placement agent proposes from the committed
directory and journals nothing, so planning creates no file, ticket or directory record.
The plan's changes are ordered: the policy commit (revision *n* → *n*+1) when the request
differs, then per session either the placement request it denotes (the exact operation
identity, the voters the planner picked, the epochs it expects) or no change when the
active placement already provides the durability. A session the planner refuses is
listed as blocked and the guarantee after the plan stays the guarantee before it (the
weakest achieved level across the sessions, or the committed level without sessions);
the guarantee during the plan is the guarantee before it, because the old contract holds
until the new placement is verified. The artifact is `FCLPLAN1` (magic, postcard body,
BLAKE3 trailer) whose identity is derived from the facts alone, so the same observation
and request make the same plan whenever it is computed; `--output` writes a new file and
never overwrites one, `--dry-run` prints without writing. `deployment apply --plan-file`
refuses a plan made for another cluster, a plan with blocked sessions (missing capacity
leaves the contract intact), a tampered plan, and — before any side effect and without
journaling — a stale plan whose observed policy revision or session epochs moved. It
then journals each change under `cluster/apply/<plan>/` (`FCLAPLY1`, the plan kept
beside it) through `Prepared → Committed → Verified → Complete`: the policy is committed
as the next revision and read back; the placement request is sent (a reply naming
another operation marks the plan stale), then observed under way (`pending` names the
operation) and complete (no plan pending and the achieved guarantee covers the request);
`--wait` bounds how long apply watches. A repeated apply resumes the journal and repeats
nothing; `deployment status` re-checks journaled plans against the directory. A
committed policy stronger than one host provides no longer refuses the founder's
restart: the local solve pins only the first policy, the directory satisfies committed
ones. Residency and home-region changes commit as policy intent; their enforcement over
copies is the residency executor's (instruction 4).

Normal text/JSON output redacts invitation secrets, credentials, private endpoints where
the caller lacks access, and claim/artifact content. Redaction applies to errors, logs,
plans, shell completion, and support bundles. Authorized troubleshooting can reveal a
specific field through a separate explicit access path; no generic debug flag dumps keys.

## 10. Migration, failure, rollback, and upgrades

Each transition is resumable and has an operation ID. Every phase records progress in
the owning control metadata, with source and destination fencing. Application progress
text is not migration authority. The source remains recoverable until custody and the
new policy are verified; cleanup is a separately gated completion step.

| Transition | Preflight | Safe cutover | Failure / rollback behavior |
|---|---|---|---|
| Laptop → fleet | Identity, writable disks, authenticated peer reachability, retained-prefix/hash inventory | Catch up learners, verify evidence, commit membership, then transfer ownership | Before membership commit remove unfinished learner; after commit use inverse membership change while quorum remains |
| Fleet → Kubernetes | Compatible binary, unique persistent volumes, secret delivery, routing, retained capacity | Add enrolled pod members, catch up, drain old hosts after fenced transfer | Keep old hosts until drain completes; do not restart their retired identities as writers |
| One zone → several | Verified zones, quorum/artifact failure-set solver, storage independence | Relocate/add copies before promising zone protection | Continue old contract until verified; never publish the stronger label midway |
| One region → several | Residency, region-failure solver, bandwidth/RTT, metadata and artifact custody | Stream retained data, verify hashes/prefixes, commit policy and ownership | No unsafe promotion on missing quorum; stop transfer if residency authority changes |
| Regional → global | Scoped policy resolution, regional directory capacity, routing cache limits, tenant quotas | Add bounded directory/placement cells and migrate sessions independently | A failed population rollout does not roll unrelated sessions back or create a global lock |
| Any → smaller fleet | Remaining quorum, RAM headroom, disk/archive custody, exact current guarantees | Drain workload, transfer authority, remove member, then release storage | Reject shrink if guarantees/capacity cannot hold; require an explicit policy change before a weaker layout |

Crash recovery resumes from committed phase state. Re-running the same operation is
idempotent; missing destination data triggers verified recopy, not fresh object identity.
If quorum is lost, the deployment reports unavailable and preserves safety. If all durable
copies are lost, it reports unrecoverable/restore-required; it cannot manufacture proof.

Upgrades use the same binary distribution at every stage. Preflight checks the supported
mixed-version window, on-disk schema, wire capabilities, validator versions, and retained
replay inputs. Roll one eligible member at a time while maintaining its policy. Activate
new incompatible features only through an explicit committed upgrade fence after all
required members support them. Binary downgrade is allowed only before that fence and
only when stored data remains readable; afterward use forward repair or a declared
restore workflow with a reported recovery point, never silent log truncation.

## 11. Implementation work and acceptance gates

P16 qualifies deployment progression after P00–P15; CLI/schema design starts in P00.
P04 exercises stage 1's durable restart subset; P05 completes its streamed evidence and
validator demonstration. P07/P08 add authenticated networking and replication, P13 adds
geography, and P15 supplies upgrades and operations. Each exercises its stage as it lands;
P16 verifies their composition and the minimal operator-facing progression.

Create `crates/focal-node/src/cli/deployment/` for parse/explain/plan/apply commands,
`crates/focal-node/src/config/` for versioned input and authority-aware resolution, and
`crates/focal-node/src/deployment/` for capability inspection, policy solving, resumable
rollout, and plan artifacts, matching the composition owner in [03](03-rust-workspace-and-interfaces.md).
Keep renderers under `deploy/kubernetes/` and `deploy/helm/`, fixtures under `tests/deployment/`,
and progression evidence in `docs/qualification/deployment-complexity.md` as P16 specifies.
Renderers consume the same typed plan; they cannot implement policy.

| Gate | Executable acceptance scenario |
|---|---|
| DC01 | Empty local directory → `start` → claims demo → process-crash and power-loss recovery on intact qualified storage: same IDs/hashes/receipts, no config or network dependency |
| DC02 | Disk-full, flush failure, unwritable directory, second writer: no successful volatile acknowledgment or temporary-store fallback |
| DC03 | First network join: pinned authenticated encryption; wrong deployment/root/identity and replayed/expired/revoked invite all rejected |
| DC04 | Same-directory writer exclusion; cluster reincarnation fences stale instances; supported restore creates new authority when source fencing is impossible; independent offline clones remain explicitly outside the guarantee |
| DC05 | Two new independent hosts and node-failure intent: planner derives placement, catches up, then advertises protection; no shard/replica knob required |
| DC06 | Too few independent domains: plan identifies missing capacity and leaves existing contract intact |
| DC07 | VM → Kubernetes migration: unchanged application demo and session receipts; kill a destination mid-copy and resume without rewriting history |
| DC08 | Kubernetes render from one-node and fleet inputs: no operator/CRD prerequisite, persistent storage per identity, no plaintext credentials |
| DC09 | Loss of a zone: promised writes/evidence survive; false/missing topology facts prevent qualification; degraded state stays visible |
| DC10 | Regional policy: measured RTT and acknowledgment consequence shown; unsatisfiable quorum or residency combinations rejected |
| DC11 | Regional evacuation with crash at each custody/cutover boundary: stable IDs and cursors; no dual leader, missing artifact, or unauthorized geography |
| DC12 | Global scoped policies: hard residency fences cannot be bypassed by child policy/client labels; unrelated sessions proceed during rollout |
| DC13 | Every stage runs the same claims demo/Rust client with identical domain results and wire semantics; only connection discovery changes |
| DC14 | Saturating/mixed workloads: derived partitions and scheduling stay within RAM/disk/network budgets; no automatic spending or policy weakening |
| DC15 | Explain/config provenance golden tests: requested/effective/observed distinct; unknown facts visible; no internal control required for ordinary progression |
| DC16 | Dry-run and stale-plan tests: zero mutations or secrets created; changed deployment identity/revision invalidates apply safely |
| DC17 | Log/error/JSON/plan/support-bundle redaction corpus: secrets and unauthorized evidence never appear, including malformed-input paths |
| DC18 | Rolling upgrade/rollback at each permitted version fence: quorum and policy maintained; unsupported downgrade rejected before opening for write |
| DC19 | Shrink/reverse migration: retained guarantees and capacity verified before member removal; unsafe deprovisioning is refused |
| DC20 | Fresh-operator progression study: record every required concept, input, command, and manual repair; any extra mandatory infrastructure concept needs an explicit design review |

P16 delivers reproducible transcripts for all six stages, machine-readable plans, supported-environment
matrices, runbooks, and measured operator steps alongside capacity envelopes. Simulated faults test invariants;
real trials qualify adapters. Global-scale claims need workload/failure evidence beyond a three-node walkthrough.
Acceptance requires operational correctness and the smallest honest extra configuration for each new use case.
