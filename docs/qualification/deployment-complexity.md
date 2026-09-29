# Deployment complexity study

Generated from `crates/focal-node/tests/deployment/concepts.json`, which each
stage's journey test writes and checks byte-for-byte against the real binary
(08 §11, DC20): a stage that introduces a new operator concept fails until the
file records it. Every command below is one the test ran; `<...>` marks a value
that varies between runs. A stage's **introduces** is the concepts it adds over
the stages it builds on — the measure DC20 holds to.

## Stage 1 — Laptop (DC01, DC02, DC04, DC13, DC15, DC16, DC17)

**Introduces** (21): native engine, start, participant, standing, claim, receipt, artifact, read, crash, one writer, unwritable directory, full volume, identity, placement view, explain, plan, apply, backup, tenant, restore, connection.

**Operator inputs**: data directory, loopback address, participant name, invitation file, claim document, artifact content, file size limit, policy file, plan file, backup directory, tenant, session.

Executed by `crates/focal-node/tests/deployment_laptop.rs`.

Not executed here:
- power loss: the crash is a kill; storage cuts at every durable boundary are the crash matrix (R11)
- the demo through MCP: mcp_native_a1 runs the same claims through the MCP server

<details><summary>Transcript</summary>

```
focal --data-dir <laptop> cluster replicas activate-native
focal --data-dir <laptop> start --advertise <address>
focal --data-dir <laptop> cluster client invite --name alice --output <client>/alice.invite
focal --data-dir <client> context enroll alice --invite-file <client>/alice.invite
focal --data-dir <client> --client-context alice status
focal --data-dir <laptop> submit claim --json <document> --format json
focal --data-dir <laptop> claim post <id> --format json
focal --data-dir <client> --client-context alice receipt acquire <id> --format json
focal --data-dir <client> --client-context alice artifact submit --claim <id> --slot 0 --text <document> --format json
focal --data-dir <laptop> get claim <id> --format json
focal --data-dir <laptop> get artifact <id> --format json
kill -KILL <laptop process>
focal --data-dir <laptop> start
focal --data-dir <laptop> get claim <id> --format json
focal --data-dir <laptop> get artifact <id> --format json
focal --data-dir <laptop> start
chmod 0500 <unwritable>
focal --data-dir <unwritable> start
ulimit -f <blocks> focal --data-dir <laptop> start
focal --data-dir <laptop> submit claim --json <document> --format json
focal --data-dir <laptop> start
focal --data-dir <laptop> get claim <id> --format json
focal --data-dir <laptop> get artifact <id> --format json
focal --data-dir <laptop> submit claim --json <document> --format json
focal --data-dir <laptop> claim post <id> --format json
focal --data-dir <laptop> cluster node identity
focal --data-dir <laptop> cluster placement
focal --data-dir <laptop> deployment explain
focal --data-dir <laptop> --config <laptop>/policy.yaml deployment plan --dry-run
focal --data-dir <laptop> --config <laptop>/policy.yaml deployment plan --output <laptop>/laptop.plan
focal --data-dir <laptop> deployment apply --plan-file <laptop>/laptop.plan
focal --data-dir <laptop> deployment apply --plan-file <laptop>/tampered.plan
focal --data-dir <laptop> cluster backup create --output <laptop>/backup
kill -KILL <laptop process>
focal --data-dir <laptop-b> cluster replicas activate-native
focal --data-dir <laptop-b> start --advertise <address>
focal --data-dir <laptop-b> deployment apply --plan-file <laptop>/laptop.plan
focal --data-dir <laptop-b> cluster tenants admit --tenant <id>
focal --data-dir <laptop-b> cluster restore --input <laptop>/backup
focal --data-dir <laptop-b> cluster restore --input <laptop>/backup --new-incarnation
focal --data-dir <reader> context add restored --node-data-dir <laptop-b> --tenant <id> --session <id>
focal --data-dir <reader> --client-context restored get claim <id> --format json
focal --data-dir <fresh> start --invite-file <laptop-b>/garbage.invite
focal --data-dir <laptop-b> submit claim --json <document>
```

</details>

## Stage 2 — VMs or bare metal (DC03, DC05, DC06, DC16, DC19)

Builds on: laptop.

**Introduces** (4): invitation, join, remove, drain.

**Operator inputs**: data directory, address, participant name, invitation file, claim document, artifact content, host name, invitation id, policy file, plan file, node id.

Executed by `crates/focal-node/tests/deployment_fleet.rs`.

Not executed here:
- an expired invitation: lifetimes are a day by default and at most seven; expiry is the enrollment registry's unit tests

<details><summary>Transcript</summary>

```
focal --config <founder>/focal.yaml --data-dir <founder> cluster replicas activate-native
focal --config <founder>/focal.yaml --data-dir <founder> start --advertise <address>
focal --config <founder>/focal.yaml --data-dir <founder> cluster client invite --name alice --output <client>/alice.invite
focal --data-dir <client> context enroll alice --invite-file <client>/alice.invite
focal --data-dir <client> --client-context alice status
focal --config <founder>/focal.yaml --data-dir <founder> submit claim --json <document> --format json
focal --config <founder>/focal.yaml --data-dir <founder> claim post <id> --format json
focal --data-dir <client> --client-context alice receipt acquire <id> --format json
focal --data-dir <client> --client-context alice artifact submit --claim <id> --slot 0 --text <document> --format json
focal --config <founder>/focal.yaml --data-dir <founder> get claim <id> --format json
focal --config <founder>/focal.yaml --data-dir <founder> get artifact <id> --format json
focal --config <founder>/focal.yaml --data-dir <founder> cluster invite --node host-a --output -
focal --config <host-a>/focal.yaml --data-dir <host-a> start --advertise <address> --invite-file <host-a>/host-a.invite
focal --data-dir <replay> start --advertise <address> --invite-file <host-a>/host-a.invite
focal --config <founder>/focal.yaml --data-dir <founder> cluster invite --node host-b --output -
focal --data-dir <tampered> start --advertise <address> --invite-file <tampered>/tampered.invite
focal --config <host-b>/focal.yaml --data-dir <host-b> start --advertise <address> --invite-file <host-b>/host-b.invite
focal --config <founder>/focal.yaml --data-dir <founder> cluster invite --node host-r --output -
focal --config <founder>/focal.yaml --data-dir <founder> cluster invitations list
focal --config <founder>/focal.yaml --data-dir <founder> cluster invitations revoke <id>
focal --data-dir <revoked> start --advertise <address> --invite-file <revoked>/host-r.invite
focal --config <founder>/focal.yaml --data-dir <founder> cluster placement
focal --data-dir <founder> --config <founder>/node-1.yaml deployment plan --output <founder>/node-1.plan
focal --config <founder>/focal.yaml --data-dir <founder> deployment apply --plan-file <founder>/node-1.plan --wait 180
focal --config <founder>/focal.yaml --data-dir <founder> get claim <id> --format json
focal --config <founder>/focal.yaml --data-dir <founder> get artifact <id> --format json
focal --config <founder>/focal.yaml --data-dir <founder> submit claim --json <document> --format json
focal --config <founder>/focal.yaml --data-dir <founder> claim post <id> --format json
focal --data-dir <client> --client-context alice receipt acquire <id> --format json
focal --data-dir <client> --client-context alice artifact submit --claim <id> --slot 0 --text <document> --format json
focal --config <founder>/focal.yaml --data-dir <founder> get claim <id> --format json
focal --config <founder>/focal.yaml --data-dir <founder> get artifact <id> --format json
focal --data-dir <client> --client-context alice get claim <id> --format json
focal --data-dir <client> --client-context alice get artifact <id> --format json
focal --data-dir <founder> --config <founder>/node-2.yaml deployment plan --output <founder>/node-2.plan
focal --config <founder>/focal.yaml --data-dir <founder> deployment apply --plan-file <founder>/node-2.plan
focal --data-dir <founder> --config <founder>/node-1-again.yaml deployment plan --output <founder>/node-1-again.plan
focal --config <founder>/focal.yaml --data-dir <founder> deployment apply --plan-file <founder>/node-1-again.plan
focal --data-dir <founder> --config <founder>/node-1-home-early.yaml deployment plan --output <founder>/node-1-home-early.plan
focal --data-dir <founder> --config <founder>/node-1-res.yaml deployment plan --output <founder>/node-1-res.plan
focal --config <founder>/focal.yaml --data-dir <founder> deployment apply --plan-file <founder>/node-1-res.plan --wait 180
focal --config <founder>/focal.yaml --data-dir <founder> deployment explain
focal --config <founder>/focal.yaml --data-dir <founder> deployment apply --plan-file <founder>/node-1-home-early.plan
focal --config <founder>/focal.yaml --data-dir <founder> cluster nodes remove --node <id>
focal --config <founder>/focal.yaml --data-dir <founder> cluster nodes drain --node <id>
focal --config <founder>/focal.yaml --data-dir <founder> get claim <id> --format json
focal --config <founder>/focal.yaml --data-dir <founder> get artifact <id> --format json
focal --config <founder>/focal.yaml --data-dir <founder> cluster invite --node host-c --output -
focal --config <host-c>/focal.yaml --data-dir <host-c> start --advertise <address> --invite-file <host-c>/host-c.invite
focal --data-dir <founder> cluster nodes remove --node <n>
focal --config <founder>/focal.yaml --data-dir <founder> get claim <id> --format json
focal --config <founder>/focal.yaml --data-dir <founder> get artifact <id> --format json
focal --config <founder>/focal.yaml --data-dir <founder> submit claim --json <document> --format json
focal --config <founder>/focal.yaml --data-dir <founder> claim post <id> --format json
focal --config <founder>/focal.yaml --data-dir <founder> get claim <id> --format json
```

</details>

## Stage 3 — Kubernetes (DC07, DC08)

Builds on: laptop, fleet.

**Introduces** (1): render kubernetes.

**Operator inputs**: config, namespace, image, invitation secret, zones, data directory, address, participant name, invitation file, claim document, artifact content, host name.

Executed by `crates/focal-node/tests/deployment_kubernetes.rs`.

Not executed here:
- a run on a real Kubernetes cluster and `helm template`: neither a cluster nor helm is available here; the manifests are byte-checked against the renderer in deployment_render, and pods stand in with local processes

<details><summary>Transcript</summary>

```
focal --data-dir <render> --config <render>/kubernetes.yaml deployment render kubernetes --namespace focal --image focal:0.1.0 --secret focal-invitations --zone a --zone b --zone c --output <render>/rendered
focal --config <config> deployment render kubernetes --namespace focal --image <image> --secret <secret> --zone <zone> --output <dir>
focal --data-dir <founder> cluster replicas activate-native
focal --data-dir <founder> start --advertise <address>
focal --data-dir <founder> cluster client invite --name alice --output <client>/alice.invite
focal --data-dir <client> context enroll alice --invite-file <client>/alice.invite
focal --data-dir <client> --client-context alice status
focal --data-dir <founder> submit claim --json <document> --format json
focal --data-dir <founder> claim post <id> --format json
focal --data-dir <client> --client-context alice receipt acquire <id> --format json
focal --data-dir <client> --client-context alice artifact submit --claim <id> --slot 0 --text <document> --format json
focal --data-dir <founder> get claim <id> --format json
focal --data-dir <founder> get artifact <id> --format json
focal --data-dir <founder> cluster invite --node host --output -
kill -KILL <joining pod>
focal --data-dir <host> start --advertise <address> --invite-file <host>/host.invite
focal --data-dir <founder> get claim <id> --format json
focal --data-dir <founder> get artifact <id> --format json
focal --data-dir <founder> submit claim --json <document> --format json
focal --data-dir <founder> claim post <id> --format json
focal --data-dir <client> --client-context alice receipt acquire <id> --format json
focal --data-dir <client> --client-context alice artifact submit --claim <id> --slot 0 --text <document> --format json
focal --data-dir <founder> get claim <id> --format json
focal --data-dir <founder> get artifact <id> --format json
```

</details>

## Stage 4 — Availability zones (DC09)

Builds on: laptop, fleet.

**Introduces** (4): zone survival, zone loss, zone return, region survival.

**Operator inputs**: data directory, address, zone fact, participant name, invitation file, host name, claim document, artifact content, tenant, session, survive zone, survive region.

Executed by `crates/focal-node/tests/deployment_zones.rs`.

Not executed here:
- a host that announced no zone: the directory grants no zone domain to it, so a zone-survival plan never counts it; the cli_zones unit covers the domain grant

<details><summary>Transcript</summary>

```
focal --config <founder>/focal.yaml --data-dir <founder> cluster replicas activate-native
focal --config <founder>/focal.yaml --data-dir <founder> start --advertise <address>
focal --config <founder>/focal.yaml --data-dir <founder> cluster client invite --name alice --output <client>/alice.invite
focal --data-dir <client> context enroll alice --invite-file <client>/alice.invite
focal --data-dir <client> --client-context alice status
focal --config <founder>/focal.yaml --data-dir <founder> cluster invite --node host-b --output -
focal --config <host-b>/focal.yaml --data-dir <host-b> start --advertise <address> --invite-file <host-b>/host-b.invite
focal --config <founder>/focal.yaml --data-dir <founder> cluster invite --node host-c --output -
focal --config <host-c>/focal.yaml --data-dir <host-c> start --advertise <address> --invite-file <host-c>/host-c.invite
focal --config <founder>/focal.yaml --data-dir <founder> cluster placement
focal --config <founder>/focal.yaml --data-dir <founder> submit claim --json <document> --format json
focal --config <founder>/focal.yaml --data-dir <founder> claim post <id> --format json
focal --data-dir <client> --client-context alice receipt acquire <id> --format json
focal --data-dir <client> --client-context alice artifact submit --claim <id> --slot 0 --text <document> --format json
focal --config <founder>/focal.yaml --data-dir <founder> get claim <id> --format json
focal --config <founder>/focal.yaml --data-dir <founder> get artifact <id> --format json
focal --config <founder>/focal.yaml --data-dir <founder> cluster sessions plan --tenant <id> --session <id> --survive zone --max-failures 1
kill -STOP <host in a3>
focal --config <founder>/focal.yaml --data-dir <founder> submit claim --json <document> --format json
focal --config <founder>/focal.yaml --data-dir <founder> claim post <id> --format json
focal --config <founder>/focal.yaml --data-dir <founder> get claim <id> --format json
kill -CONT <host in a3>
focal --config <founder>/focal.yaml --data-dir <founder> get claim <id> --format json
focal --config <founder>/focal.yaml --data-dir <founder> cluster sessions plan --tenant <id> --session <id> --survive region --max-failures 1 --dry-run
focal --config <founder>/focal.yaml --data-dir <founder> get claim <id> --format json
focal --config <founder>/focal.yaml --data-dir <founder> get artifact <id> --format json
```

</details>

## Stage 5 — Regions (DC10, DC11, DC12)

Builds on: laptop, fleet, zones.

**Introduces** (5): peer latency, region loss, region return, ranges, residency fence.

**Operator inputs**: data directory, address, region fact, participant name, invitation file, host name, claim document, artifact content, tenant, session, survive region, member, node outside residency.

Executed by `crates/focal-node/tests/deployment_regions.rs`.

Not executed here:
- measured cross-region RTT under real latency: the fleet is one host, so the RTT gauge is present and near zero; a real trial qualifies the adapter

<details><summary>Transcript</summary>

```
focal --config <founder>/focal.yaml --data-dir <founder> cluster replicas activate-native
focal --config <founder>/focal.yaml --data-dir <founder> start --advertise <address>
focal --config <founder>/focal.yaml --data-dir <founder> cluster client invite --name alice --output <client>/alice.invite
focal --data-dir <client> context enroll alice --invite-file <client>/alice.invite
focal --data-dir <client> --client-context alice status
focal --config <founder>/focal.yaml --data-dir <founder> cluster invite --node host-b --output -
focal --config <host-b>/focal.yaml --data-dir <host-b> start --advertise <address> --invite-file <host-b>/host-b.invite
focal --config <founder>/focal.yaml --data-dir <founder> cluster invite --node host-c --output -
focal --config <host-c>/focal.yaml --data-dir <host-c> start --advertise <address> --invite-file <host-c>/host-c.invite
focal --config <founder>/focal.yaml --data-dir <founder> cluster invite --node host-d --output -
focal --config <host-d>/focal.yaml --data-dir <host-d> start --advertise <address> --invite-file <host-d>/host-d.invite
focal --config <founder>/focal.yaml --data-dir <founder> submit claim --json <document> --format json
focal --config <founder>/focal.yaml --data-dir <founder> claim post <id> --format json
focal --data-dir <client> --client-context alice receipt acquire <id> --format json
focal --data-dir <client> --client-context alice artifact submit --claim <id> --slot 0 --text <document> --format json
focal --config <founder>/focal.yaml --data-dir <founder> get claim <id> --format json
focal --config <founder>/focal.yaml --data-dir <founder> get artifact <id> --format json
focal --config <founder>/focal.yaml --data-dir <founder> cluster sessions plan --tenant <id> --session <id> --survive region --max-failures 1
focal --data-dir <founder> cluster node metrics
kill -STOP <host in r3>
focal --config <founder>/focal.yaml --data-dir <founder> submit claim --json <document> --format json
focal --config <founder>/focal.yaml --data-dir <founder> claim post <id> --format json
kill -CONT <host in r3>
focal --config <founder>/focal.yaml --data-dir <founder> cluster replicas ranges --session <id> list
focal --config <founder>/focal.yaml --data-dir <founder> cluster replicas ranges --session <id> move --member <id> --node <id>
```

</details>

## Rolling upgrade (DC18)

Builds on: laptop, fleet.

**Introduces** (3): upgrade status, upgrade activate, capability level.

**Operator inputs**: data directory, address, participant name, invitation file, host name, claim document, artifact content, fence level, capability level, env FOCAL_CAPABILITY_LEVEL.

Executed by `crates/focal-node/tests/deployment_upgrade.rs`.

Not executed here:
- a real newer binary at a higher level: the fence mechanism is exercised with the levels one binary announces; a second binary at a higher capability qualifies a real version step

<details><summary>Transcript</summary>

```
focal --data-dir <founder> cluster replicas activate-native
focal --data-dir <founder> start --advertise <address>
focal --data-dir <founder> cluster client invite --name alice --output <client>/alice.invite
focal --data-dir <client> context enroll alice --invite-file <client>/alice.invite
focal --data-dir <client> --client-context alice status
focal --data-dir <founder> cluster invite --node host --output -
focal --data-dir <host> start --advertise <address> --invite-file <host>/host.invite
focal --data-dir <founder> submit claim --json <document> --format json
focal --data-dir <founder> claim post <id> --format json
focal --data-dir <client> --client-context alice receipt acquire <id> --format json
focal --data-dir <client> --client-context alice artifact submit --claim <id> --slot 0 --text <document> --format json
focal --data-dir <founder> get claim <id> --format json
focal --data-dir <founder> get artifact <id> --format json
focal --data-dir <founder> cluster upgrade status
focal --data-dir <host> cluster upgrade activate --fence 1
focal --data-dir <founder> cluster upgrade activate --fence 2
focal --data-dir <founder> cluster upgrade activate --fence 1
focal --data-dir <founder> get claim <id> --format json
focal --data-dir <founder> get artifact <id> --format json
focal --data-dir <host> start --advertise <address>
focal --data-dir <host> start --advertise <address>
focal --data-dir <founder> submit claim --json <document> --format json
focal --data-dir <founder> claim post <id> --format json
```

</details>

## Workloads (DC14)

Builds on: laptop, fleet.

**Introduces** (1): budget metrics.

**Operator inputs**: data directory, address, participant name, invitation file, host name, claim document, artifact content.

Executed by `crates/focal-node/tests/deployment_workloads.rs`.

Not executed here:
- disk-growth and checkpoint/recovery envelopes under a long mixed load: measured by the R11 workload generator, not this correctness walk

<details><summary>Transcript</summary>

```
focal --data-dir <founder> cluster replicas activate-native
focal --data-dir <founder> start --advertise <address>
focal --data-dir <founder> cluster client invite --name alice --output <client>/alice.invite
focal --data-dir <client> context enroll alice --invite-file <client>/alice.invite
focal --data-dir <client> --client-context alice status
focal --data-dir <founder> cluster invite --node host-a --output -
focal --data-dir <host-a> start --advertise <address> --invite-file <host-a>/host-a.invite
focal --data-dir <founder> cluster invite --node host-b --output -
focal --data-dir <host-b> start --advertise <address> --invite-file <host-b>/host-b.invite
focal --data-dir <founder> submit claim --json <document> --format json
focal --data-dir <founder> claim post <id> --format json
focal --data-dir <founder> submit claim --json <document> --format json
focal --data-dir <founder> claim post <id> --format json
focal --data-dir <founder> submit claim --json <document> --format json
focal --data-dir <founder> claim post <id> --format json
focal --data-dir <founder> submit claim --json <document> --format json
focal --data-dir <founder> claim post <id> --format json
focal --data-dir <founder> submit claim --json <document> --format json
focal --data-dir <founder> claim post <id> --format json
focal --data-dir <founder> submit claim --json <document> --format json
focal --data-dir <founder> claim post <id> --format json
focal --data-dir <founder> submit claim --json <document> --format json
focal --data-dir <founder> claim post <id> --format json
focal --data-dir <founder> submit claim --json <document> --format json
focal --data-dir <founder> claim post <id> --format json
focal --data-dir <founder> submit claim --json <document> --format json
focal --data-dir <founder> claim post <id> --format json
focal --data-dir <founder> submit claim --json <document> --format json
focal --data-dir <founder> claim post <id> --format json
focal --data-dir <founder> submit claim --json <document> --format json
focal --data-dir <founder> claim post <id> --format json
focal --data-dir <founder> submit claim --json <document> --format json
focal --data-dir <founder> claim post <id> --format json
focal --data-dir <founder> submit claim --json <document> --format json
focal --data-dir <founder> claim post <id> --format json
focal --data-dir <founder> submit claim --json <document> --format json
focal --data-dir <founder> claim post <id> --format json
focal --data-dir <founder> submit claim --json <document> --format json
focal --data-dir <founder> claim post <id> --format json
focal --data-dir <founder> submit claim --json <document> --format json
focal --data-dir <founder> claim post <id> --format json
focal --data-dir <founder> submit claim --json <document> --format json
focal --data-dir <founder> claim post <id> --format json
focal --data-dir <client> --client-context alice receipt acquire <id> --format json
focal --data-dir <client> --client-context alice artifact submit --claim <id> --slot 0 --text <document> --format json
focal --data-dir <founder> cluster node metrics
```

</details>
