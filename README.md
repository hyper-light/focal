<p align="center">
  <a href="docs/assets/brand/focal-prism-preview.png">
    <picture>
      <source media="(prefers-color-scheme: dark)" srcset="docs/assets/brand/focal-prism-dark.svg">
      <source media="(prefers-color-scheme: light)" srcset="docs/assets/brand/focal-prism-light.svg">
      <img src="docs/assets/brand/focal-prism-light.svg" alt="Focal logo: a triangular prism with three straight incoming rays and one outgoing ray" width="194" height="90">
    </picture>
  </a>
</p>

<h1 align="center">focal</h1>
<p align="center"><em>Agentic proof-of-work ledger and protocol.</em></p>

Hand work to a swarm of agents and you soon want to know who was asked to do what, who
picked it up, what came back, and whether anyone checked it. Focal keeps that record. Your
agents can:

- Ask each other for work with the acceptance criteria written down up front
- Take responsibility for a task, so you know who is on it
- Report what they did, success or failure, with the evidence attached
- Check each other's evidence and record what passed
- Keep doing all of that when the swarm spans machines, zones or regions

A task counts as done when the checks passed, not when an agent says it is. Focal does not
run any of your tools. Agents use whatever they like, in any language, and Focal keeps the
history.

It is one binary: the service, the command line and the MCP server.

```console
$ focal start
{ "condition": "Ready", ... }

$ focal schema example claim.submit > claim.json
$ focal submit claim --file claim.json
Committed
Claim: d4cc83c48c5ad2bafaa1802bb87c3e07

$ focal claim post d4cc83c48c5ad2bafaa1802bb87c3e07
Committed
Claim: d4cc83c48c5ad2bafaa1802bb87c3e07 (Posted)

$ focal receipt acquire d4cc83c48c5ad2bafaa1802bb87c3e07
Committed
Receipt: e2c5670ea738bf9bfd18035388f80e8f (epoch 1)
Claim: d4cc83c48c5ad2bafaa1802bb87c3e07
```

> [!NOTE]
> Console output in this README was captured from a debug build at `125effd` on macOS with
> `--data-dir` pointed at a scratch directory; JSON is trimmed where marked with `...`.

> [!IMPORTANT]
> Focal has no release yet. The local service, the whole claim and evidence workflow through
> the CLI and MCP, restart recovery, durable retries, and joining nodes into a cluster all
> work today. Automatic placement across hosts, range movement, archival and restore,
> Kubernetes packaging, multi-region operation and Windows are still being built.
> [docs/REMAINING.md](docs/REMAINING.md) lists each piece and what closes it.

## Install

Until the first tagged release, build from source. You need Rust 1.94.1 (pinned in the
toolchain file, so `rustup` picks it up) and a protobuf compiler (`brew install protobuf`
or `apt-get install protobuf-compiler`):

```sh
git clone https://github.com/hyper-light/focal && cd focal
bash scripts/cargo.sh build --release -p focal-node --bin focal --locked
sudo mv target/release/focal /usr/local/bin/   # or add target/release to PATH
focal --help
```

> [!NOTE]
> The release workflow builds one raw binary per platform (macOS arm64 and x64, Linux arm64
> and x64 on glibc and static musl, and Windows x64 and arm64), smoke-tests each on its own
> hardware, and attaches them with `SHA256SUMS`. When a release is published, download the
> file for your platform, check its digest, `chmod +x` it (or, on Windows, unblock the
> `.exe`) and put it on your `PATH`; nothing else is needed. The native Windows build
> compiles in CI, and its filesystem layer (owner-only DACLs, file identity) and named-pipe
> local transport are tested there on every push; the full Windows CLI/MCP test lane and
> signed binaries are landing.

## Quickstart

Start the service in one terminal and leave it running:

```sh
focal start
```

It creates a private data directory (`~/Library/Application Support/Focal` on macOS,
`~/.local/share/focal` on Linux), an identity, and a local Unix socket. There is no
configuration file and no network port. To keep a ledger somewhere else, pass
`--data-dir /absolute/path` to every command, including `start`. 
> [!WARNING]
> The startup record says what durability you actually have. On a laptop, writes are synced
> to this disk and loss of the disk loses the ledger.

In another terminal, make a claim from the built-in example and post it:

```console
$ focal schema example claim.submit > claim.json
$ focal submit claim --file claim.json
Committed
Claim: d4cc83c48c5ad2bafaa1802bb87c3e07

$ focal list claims
KIND	ID	STATE/HASH	DESCRIPTION
claim	d4cc83c48c5ad2bafaa1802bb87c3e07	Generated	"Deliver the checked report"
SEQUENCE	1	VISITED	1

$ focal claim post d4cc83c48c5ad2bafaa1802bb87c3e07
Committed
Claim: d4cc83c48c5ad2bafaa1802bb87c3e07 (Posted)

$ focal ledger summary
LEDGER	fdb5795f2c6d5a00871e7e521eedf9b8/4040bf729c435aa42d8e669849605d5a
SEQUENCE	1
ROUTE EPOCH	1
APPLIED INDEX	4
CLAIMS	1
TESTAMENTS	0
ARTIFACTS	0
VALIDATIONS	1
EVIDENCE SETS	0
VALIDATION RUNS	0
```

You create a claim, then post it. Only a posted claim can be picked up. The example is a
claim on yourself with one check, that the report is received, so you can try the mechanics
without setting up an evaluator. IDs are 32 hex characters and hashes are 64. Every read
prints the `SEQUENCE` it was served at, so you know how current it is.

Stop the service with Ctrl-C and run `focal start` again: the same ledger comes back.

### Run the whole workflow

The demo takes a claim all the way through: post, receipt, a stored test report, a
testament, its receipt by the claimant, a recorded validation, and the derived result.

> [!WARNING]
> The demo owns its directory. Give it one that no running service is using.

```console
$ focal --data-dir /tmp/focal-demo demo
{ "claim": [...], "status": 8, "sequence": 13, "validation": 1,
  "artifact": { "root": [...], "length": 35, "class": 2 },
  "history": [ { "status": 1, "sequence": 4 }, ..., { "status": 8, "sequence": 13 } ] }

$ focal --data-dir /tmp/focal-demo demo      # the same claim and proof, nothing re-run
```

`status: 8` is `Satisfied` and `validation: 1` is `Pass`; `focal schema get domain-registry`
prints the vocabulary. The second run recovers rather than repeats.

<details>
<summary>The same steps by hand, as the respondent</summary>

```sh
focal receipt acquire CLAIM_ID
focal evidence begin --claim CLAIM_ID --receipt RECEIPT_ID --receipt-epoch 1
focal submit artifact --claim CLAIM_ID --receipt RECEIPT_ID --receipt-epoch 1 \
  --evidence-set EVIDENCE_SET_ID --kind test-report --schema-hash SCHEMA_HASH \
  --text '{"passed":1,"failed":0,"skipped":0}'
focal submit testament --claim CLAIM_ID --receipt RECEIPT_ID --receipt-epoch 1 \
  --evidence-set EVIDENCE_SET_ID --artifact ARTIFACT_ID:DESCRIPTOR_HASH \
  --summary 'Checked report attached' --confidence committed --outcome complete
```

The claimant then receives the testament, reads the artifact, and records the validation.
Failed work is reported the same way, with an error artifact instead of a report; a
testament that says "failed" is still a testament. Every step with its flags is in the
[manual](docs/manual-cli.md#deliver-artifacts-and-a-testament).

</details>

## What two agents see

```mermaid
sequenceDiagram
    participant A as Agent A · requester and evaluator
    participant B as Agent B · respondent
    A->>B: Claim: fix the regression, tests must pass
    B-->>A: Receipt: I accept responsibility
    Note over B: B does the work with its own tools
    B-->>A: Testament: outcome, summary, exact artifact references
    A->>B: Testament receipt: I have your report
    Note over A: A inspects the evidence and runs the declared checks
    A-->>B: Validation result and proof artifacts
    Note over A,B: Focal derives acceptance from the required results
```

Every arrow is a record. Both agents can read it back, and so can you. B writes its own
report whether the work succeeded or failed. Focal never writes one for it, so an agent that
went quiet shows up as an agent that went quiet. When you write a claim you say who checks it
and with what. The checker runs that tool itself and records what it found. A claim is
satisfied when its checks pass and the claims it depends on are done. Not before.

| Object | What it records |
|---|---|
| **Claim** | A directed request, its acceptance requirements, scope, and relations to other claims |
| **Testament** | The respondent's closing statement and the exact ordered list of artifacts |
| **Artifact** | Typed, immutable evidence: inline bytes or a stored content reference, with a descriptor hash |
| **Validation** | A declared check, its evaluator, and the recorded runs and verdicts |

A fresh ledger runs the V1 engine. Turn on the **native engine** with
`focal cluster replicas activate-native` and each of the four gets its own lifecycle, failed
work becomes evidence, and agents get peer workflows: a **challenge** asks the respondent to
prove something, a **consult** asks for work that answers a question, and a correction or
follow-up points at the exact record it is answering. An existing V1 ledger is imported, not
rewritten.

## Use it with an AI agent (MCP)

`focal mcp serve` is a [Model Context Protocol] server over stdio. Start the service, then
point your client at it:

> [!TIP]
> Give the client the binary and the data directory by absolute path. MCP clients start
> servers with no working directory and often no `PATH`, and the adapter must see the same
> ledger the service owns.

**Claude Code**
```sh
claude mcp add focal -- /usr/local/bin/focal --data-dir /absolute/path/to/ledger mcp serve
```

**Claude Desktop, Cursor and other `mcpServers` clients**
```json
{
  "mcpServers": {
    "focal": {
      "command": "/usr/local/bin/focal",
      "args": ["--data-dir", "/absolute/path/to/ledger", "mcp", "serve"]
    }
  }
}
```

**Codex CLI**, in `~/.codex/config.toml`:
```toml
[mcp_servers.focal]
command = "/usr/local/bin/focal"
args = ["--data-dir", "/absolute/path/to/ledger", "mcp", "serve"]
```

A remote participant uses `--client-context NAME` (an enrolled QUIC connection) in place
of the data directory. The handshake against the ledger above:

```console
$ focal mcp serve
← {"jsonrpc":"2.0","id":1,"result":{"protocolVersion":"2025-11-25","capabilities":{"tools":{}},"serverInfo":{"name":"focal","version":"0.1.0"}}}
← tools/list: 16 tools on the first page, nextCursor present
```

`tools/list` is paged; follow `nextCursor` until it is gone and use the schemas you get
back. The tools are the same operations as the CLI: `claim.*`, `receipt.acquire`,
`evidence.begin`, `artifact.*`, `testament.*`, `validation.*`, `monitor.*`,
`ledger.summary`, plus `request.*` for recovering a lost reply, `upload.*` for large
payloads and `watch.*` for following changes. Every write carries its own durable ID. If an
agent crashes before it sees the reply, it retries with the same ID and gets the original
result. It cannot do the work twice.

Five skills teach an agent the workflow, pinned to the tool versions in
[skills/manifest.json](skills/manifest.json):

| Skill | What it covers |
|---|---|
| [focal-claims](skills/focal-claims/SKILL.md) | Author and progress claims; recover exact operations |
| [focal-evidence](skills/focal-evidence/SKILL.md) | Produce artifacts and submit testaments |
| [focal-validation](skills/focal-validation/SKILL.md) | Run external checks and record results against the pinned context |
| [focal-peers](skills/focal-peers/SKILL.md) | Challenge, consult, correct and follow up on a native ledger |
| [focal-cluster](skills/focal-cluster/SKILL.md) | Inspect and administer local cluster state |

Tool contracts, the request stream and recovery: **[docs/mcp.md](docs/mcp.md)**.

## CLI reference

| Command | What it does |
|---|---|
| `focal start` | Run the service (`--advertise HOST:PORT` to accept peers) |
| `focal submit claim \| testament \| artifact \| validation` | Author from flags, `--json`, `--yaml` or `--file` |
| `focal claim post \| progress \| cancel \| wait ID` | Progress a claim you issued |
| `focal receipt acquire ID` · `evidence begin` | Take responsibility; open an evidence set |
| `focal get claim \| testament \| artifact \| validation ID` | One object, as a table or `--format json` |
| `focal list claims \| testaments \| artifacts \| validations [filters]` | Bounded pages; every filter optional; `--all` follows pages |
| `focal ledger summary` · `ledger traverse claim:ID` | Counts; the graph around one object |
| `focal watch claims` · `monitor register` | Follow changes; durable wait predicates |
| `focal schema list \| get \| example NAME` | Contracts and examples, no service needed |
| `focal request retry --operation-id ID` · `request pending` | Resolve a lost reply |
| `focal context` | Save and select connections, local or remote |
| `focal cluster …` · `focal join` · `focal deployment explain` | Membership, enrollment, offline placement checks |
| `focal mcp serve` · `focal demo` · `focal completion SHELL` | The MCP server; the demo; shell completions |

Flags, JSON and YAML all build the same request. Unknown fields and duplicate keys are
rejected. Exit codes and every flag: **[docs/manual-cli.md](docs/manual-cli.md)**.

## How it works

- **Reads are whole.** A change goes to a log on disk first. Only then is it applied in
  memory and published, all at once. You never read half a change.
- **Restarts are boring.** Recovery replays the log. It runs no tools, no clocks, no
  validators, because their results were recorded the first time. You get the same ledger
  back at the same sequence.
- **Evidence cannot be swapped.** Artifact bytes are stored under their hash and checked
  against it before the ledger can point at them. The bytes a report says it checked are the
  bytes you will find.
- **A lost reply is safe.** An agent writes down what it is about to do, under its own ID,
  before sending it. Lose the reply, resend the ID, get the original result.
- **It says no before it falls over.** Pages, scans, queues and request windows all have
  limits. When the service is under pressure it tells you, instead of growing until it dies.

The design is in [docs/archictecutre/](docs/archictecutre/README.md) (the directory name
is deliberate), starting with the [target architecture](docs/archictecutre/00-target-architecture.md).

## From a laptop to a fleet

Your swarm might be on one laptop today, a few VMs next month, and several regions after
that. Each step asks you only the questions that step brings, and the claims, evidence and
commands do not change. You never pick a Raft term, a shard count or a split key.

- **You say what to survive.** A node, a zone, a region, and how many at once. Focal places
  the voters to make that true and prints the guarantee it can actually keep, never a
  stronger one.
- **A failover does not repeat work.** An agent whose reply was lost resends the same ID to
  whichever node is now leading and gets the original result.
- **Evidence is tracked on its own.** Artifact bytes are replicated and verified separately
  from the log. Adding a replica does not quietly copy your evidence, and Focal tells you
  which copies have been verified.
- **Nodes join by invitation.** The founder writes a one-use invitation and the new node
  enrolls from it over one authenticated UDP/QUIC port. Credentials renew themselves before
  they expire. Revoking one is a single command.
- **A slow node does not take the cluster down with it.** A node that is struggling stretches
  its own timeouts instead of accusing healthy peers, and a verdict needs confirmations from
  more than one node. This is SWIM with the Lifeguard extensions.
- **A fleet is more ledgers, not a bigger log.** Each ledger has one total order. Growing
  means adding ledgers, never scanning everything.

What you get at each step:

| Your placement | A write is acknowledged when | If a region is lost |
|---|---|---|
| Laptop, one voter | It is on this disk | Restart recovers from intact storage. Lose the disk and you lose the ledger; the startup record says so |
| Regional, survive N nodes or zones | A majority of voters have it on disk | The region can go dark. Focal never promotes a minority |
| Synchronous multi-region | A majority spread so the surviving regions still hold one | The survivors elect a leader and keep serving. The cross-region round trip is in your write path |
| Disaster-recovery copy | As the primary, plus a lagging archive | Restore to a known point. Focal reports the recovery point and never calls it zero-loss |

Three voters in three regions survive losing any one region. Five placed 2/2/1 survive
losing either two-voter region. Three placed 2/1 do not survive losing the larger side, and
the planner will not call that regional survival.

### Two machines

The founder advertises an endpoint and writes a one-use invitation; the second machine
enrolls from it and starts:

```sh
focal --data-dir ~/focal-founder start --advertise founder.example:7443
focal --data-dir ~/focal-founder cluster invite --node worker-2 --output worker-2.invite

# on the second host, after copying the invitation: enroll and start in one command
focal --data-dir ~/focal-node start --invite-file worker-2.invite --advertise worker-2.example:7443
```

An address works as well as a name; a name is announced with the node so peers find it
again when the address behind it changes. For a supervised host or a Kubernetes
namespace, `focal deployment render systemd|kubernetes` writes the packaging
([deploy/](deploy/)).

> [!IMPORTANT]
> Joining lets the new node take part in the cluster; it does not yet copy your ledger onto
> it or make your data survive the loss of the first machine. Today you do that yourself with
> `cluster membership` and `cluster replicas`; the automatic placement that will do it for you
> is the next batch of work.

Two nodes on one laptop, hosts on different machines, and every administration command are
in [docs/network-startup.md](docs/network-startup.md) and [docs/cluster-admin.md](docs/cluster-admin.md).

### Where each step stands

| Step | What you decide | Today (2026-09-09) |
|---|---|---|
| Laptop | Where to keep the data | Works: durable service, restart, the full workflow through CLI and MCP |
| VMs or bare metal | Reachable addresses, who may join, how many node failures to survive | Works: join (also `start --invite-file`), authenticated transport, names that outlive addresses, membership, leader transfer, credential renewal and rotation, drain/remove/replace, repair, backup/restore, the upgrade fence; `deployment plan`/`apply` commit a durability policy and the placement controller expands and heals the ledgers; `deployment render systemd` writes the unit |
| Kubernetes | Storage and packaging | `deployment render kubernetes` writes the manifests (`deploy/kubernetes`), a Helm chart and a container recipe are checked in; not yet executed on a real cluster |
| Several zones | Verified failure domains, what zone loss you accept | Declared zones are announced and granted, voters spread across them, residency fenced; zone-loss journeys remain to be recorded |
| Several regions | Residency, home regions, the latency you will pay for remote durability | Architecture and schema; the geographic executor remains |
| Global fleet | Per-tenant geography and resource policy | Target; the partitioned directory exists, scale qualification remains |

> [!NOTE]
> Small-cluster tests are not evidence of global throughput, and Focal has no published
> benchmark yet. The contract for each step is
> [08](docs/archictecutre/08-stepped-complexity-and-deployment.md); the placement design is
> [24](docs/archictecutre/24-placement-execution-and-fleet-control.md).

## Documentation

| Doc | What's in it |
|---|---|
| [Manual CLI](docs/manual-cli.md) | Every command with examples, input formats, filters, exit codes, recovery |
| [MCP and skills](docs/mcp.md) | Connecting a client, the tool catalogue, the request stream, skill packaging |
| [Network startup](docs/network-startup.md) · [Cluster administration](docs/cluster-admin.md) | Founder, invitations, joining, membership, diagnostics |
| [Monitors](docs/monitors.md) | Durable wait predicates under a claim |
| [Building](docs/building.md) | Toolchain, checks, the release lane |
| [Architecture](docs/archictecutre/README.md) | Design documents 00–24 |
| [Remaining work](docs/REMAINING.md) · [Implementation status](docs/archictecutre/09-implementation-status.md) | What is left, and the dated evidence for what is done |

## Contributing / development

```sh
bash scripts/cargo.sh build -p focal-node --bin focal --locked
bash scripts/cargo.sh test --workspace --all-targets --locked -- --test-threads=4
bash scripts/cargo.sh clippy --workspace --all-targets --locked -- -D warnings
bash scripts/check-production.sh        # no panics in non-test code
python3 scripts/check-contracts.py      # architecture links, imported hashes, frozen vocabularies
```

`scripts/cargo.sh` sets up the protobuf build environment and forwards to `cargo`. The
rules that hold throughout are in [docs/REMAINING.md §2](docs/REMAINING.md#2-requirements-that-must-not-be-redesigned-away):
no production panics, owned state over shared ownership, frozen history, independent
lifecycles, and participant-owned execution.

## Acknowledgements

Focal's domain model and distribution design descend from the [hecate] specifications and
the [sylk] implementation, both kept under
[docs/archictecutre/reference](docs/archictecutre/reference/README.md). Failure detection
follows SWIM with the Lifeguard extensions and Vivaldi coordinates.

## License

MIT — © 2026 Hyperlight. See [LICENSE](LICENSE).

[Model Context Protocol]: https://modelcontextprotocol.io
[hecate]: https://github.com/hyper-light/hecate
[sylk]: https://github.com/hyper-light/sylk
