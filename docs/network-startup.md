# Start and join a network

The network service supports a founder, private invitations, durable node enrollment, and automatic admission of joined nodes as **root metadata learners**. The founder hosts the initial application ledger and durably bootstraps the first directory partition. Every node has a bounded managed replica owner; joined nodes start with it empty. This join workflow does not assign application replicas, promote voters, or add evidence copies. Joining alone therefore does **not** increase the application's durability guarantee. Separate [cluster administration commands](cluster-admin.md) inspect and change actual root or installed application membership.

Build the binary from the repository root after following the prerequisites in [Building and checking Focal](building.md):

```sh
bash scripts/cargo.sh build -p focal-node --bin focal --locked
export PATH="$PWD/target/debug:$PATH"
```

Run the examples from shells where `focal` is on `PATH`. `--data-dir` is a global option: it works before or after the subcommand. These examples put it before the subcommand to make the physical node being operated on explicit. Always reuse that node's directory on restart. Each simultaneously running node needs its own directory and endpoint.

## Two nodes on one host

These examples use `/tmp` for an experiment. Use persistent storage for a node whose data you intend to keep.

In the first terminal, start the founder and leave it running:

```sh
focal --data-dir /tmp/focal-founder start --advertise 127.0.0.1:7443
```

Wait for the JSON startup record with `"condition": "Ready"`. The process runs in the foreground. The advertised endpoint is also the listen address unless `--listen` is supplied. The service uses one UDP/QUIC port for authenticated peer traffic and invitation redemption, plus private local Unix sockets.

In a second terminal, ask the running founder to write an invitation:

```sh
focal --data-dir /tmp/focal-founder cluster invite \
  --node worker-2 --output /tmp/worker-2.invite
```

The command prints `InvitationWritten` and the output path, never the invitation token. It talks to the founder's local administrative socket, so run it on the founder's host as the OS user that owns the service.

Enroll the second node using a different, initially empty data directory:

```sh
focal --data-dir /tmp/focal-worker-2 join \
  --invite-file /tmp/worker-2.invite --advertise 127.0.0.1:7444
```

`join` saves the enrollment and prints only the verified node identity, then exits. The cluster and application ledger IDs match the founder; the physical node ID differs. Start the enrolled node in the same terminal:

```sh
focal --data-dir /tmp/focal-worker-2 start
```

The founder’s `Ready` record also waits for committed initial directory activation. This requires no new configuration or geographic labels.

The joined node’s startup record is `CatchingUp`, with `assigned_ledger: false`. The network controller commits its authenticated contact and node capability before admitting it as a root learner. The capability records verified identity and eligibility; region and zone remain unknown until an infrastructure authority supplies them. No geography settings are needed for this step. The startup record is not a continuous progress report. A joined node has its own private local admin socket for live inspection and authorized membership operations; issuing invitations still requires the founder's signing backend.

From another terminal, inspect the saved identities or the founder's published application prefix:

```sh
focal --data-dir /tmp/focal-founder identity
focal --data-dir /tmp/focal-worker-2 identity
focal --data-dir /tmp/focal-founder status
focal --data-dir /tmp/focal-worker-2 cluster node health
focal --data-dir /tmp/focal-worker-2 cluster status
focal --data-dir /tmp/focal-worker-2 cluster replicas list
```

Top-level `status` queries the application ledger, so a joined node without that assignment cannot serve it. `cluster node health` observes the live local owner; `cluster status` performs a root quorum read. An empty `cluster replicas list` is the expected initial joined-node application inventory. None of these observations assigns placement or changes durability.

## Nodes on different hosts

Install a compatible `focal` binary on each host. Choose an IP address that the other hosts can reach and allow UDP traffic on the chosen port. Replace the example addresses `192.0.2.10` and `192.0.2.20` with the hosts' actual addresses; these documentation addresses are not usable destinations.

On the founder host:

```sh
focal --data-dir "$HOME/focal-node" start \
  --advertise 192.0.2.10:7443 --listen 0.0.0.0:7443
```

In another terminal on that same host:

```sh
focal --data-dir "$HOME/focal-node" cluster invite \
  --node worker-2 --output "$HOME/worker-2.invite"
scp -p "$HOME/worker-2.invite" worker-user@192.0.2.20:worker-2.invite
```

Replace `worker-user` with the account that will own the second node. The invitation is a secret: transfer it privately. `scp -p` preserves its required `0600` mode. Do not paste its contents into logs or chat.

On the second host, as that account:

```sh
chmod 600 "$HOME/worker-2.invite"
focal --data-dir "$HOME/focal-node" join \
  --invite-file "$HOME/worker-2.invite" \
  --advertise 192.0.2.20:7443 --listen 0.0.0.0:7443
focal --data-dir "$HOME/focal-node" start
```

Different hosts may use the same port and directory spelling because those resources are local to each host. `--advertise` identifies the reachable endpoint; `--listen` selects the local bind address. An unspecified address such as `0.0.0.0` is valid only for listening. Neither address may use port zero. No seed list or manually copied certificate configuration is required: the invitation pins the cluster, founder endpoint, and trust material.

## Restart and retry

Stop a foreground service with Ctrl-C, then restart it with the same directory:

```sh
focal --data-dir /tmp/focal-founder start
focal --data-dir /tmp/focal-worker-2 start
```

Run these in separate terminals. On different hosts, each instead uses its saved `$HOME/focal-node` directory. Plain `start` loads the persisted network identity and endpoints; it needs neither the invitation file nor repeated address flags. Supplying changed addresses is rejected. There is no endpoint-change command yet.

A stopped local-only node can introduce networking by starting its existing directory with `--advertise`. Keep the directory intact; the service preserves the existing application identity and data. After that first network start, use plain `start` for recovery. Never run two owners against one data directory.

An invitation name contains 1–63 ASCII letters, digits, dots, underscores, or hyphens. Within one cluster, the same name identifies the same durable invitation request. Repeating `cluster invite` with that name returns the original invitation, including after founder restart or successful redemption. Repeating the same output path succeeds only when its private file already contains exactly those bytes; a different existing file is never overwritten. The output directory must already exist.

An unused invitation expires **one hour after its first preparation**. Repeating the command does not renew that deadline. A fresh invitation needs a fresh name and output path. Inspect invitations with `cluster invitations list` or `cluster invitations get ID`; `cluster invitations revoke ID` durably revokes that invitation and its issued credential. Revocation does not remove a node from consensus membership. One invitation enrolls one saved join identity, not an arbitrary sequence of new nodes. A node name is an invitation label, not a topology label or permission grant.

If `join` fails or its reply is lost, retry the exact command with the same directory, invitation, and endpoints. The saved private key, CSR, and request identity are reused. A previously committed enrollment can be recovered using that identity even after the invitation's initial redemption window closes, while the issued credential remains valid. Preserve the pending directory; substituting another invitation or endpoint is rejected. Do not delete unknown-outcome join state to manufacture a new attempt.

After successful enrollment, keep the node directory intact. Startup uses its saved credentials and fails closed if initialized identity, policy, or join state is missing. Root and installed application membership removal are available through [cluster administration](cluster-admin.md). Membership removal does not drain application placement or complete evidence migration. A joined node renews its own credential ten days ahead of expiry, or on `cluster credentials renew` (the same key under a fresh certificate; see [cluster administration](cluster-admin.md)); the complete operational recovery journeys remain work in progress.

## Current deployment boundary

This workflow establishes authenticated reachability and root metadata replication. It does not perform application ledger placement, root voter promotion, evidence-copy placement, or activation of a stronger durability policy. Root promotion is separately available with `cluster membership promote --node N` after actual catch-up. Installed application replicas have their own `cluster replicas membership` commands and configuration fences; changing root membership does not install an application replica. A joined Node certificate grants none of the founder's local Runtime permissions.

The initial directory is owned by the founder and currently requires that founder to lead the root group while preparing its directory authority permit. After root voting membership is expanded and leadership moves elsewhere, founder restart cannot reach `Ready` until root leadership returns. The local permit path also governs directory authority refresh. Remote directory-bootstrap authorization remains unimplemented. The CLI can initiate a root leadership transfer with `cluster leader transfer --node N`; inspect the resulting leader rather than treating transfer initiation as completion. This startup constraint remains even though root membership and transfer commands are available.

The CLI currently has offline `deployment explain` and `deployment schema` commands. It has **no `deployment plan` or `deployment apply` command**. An offline placement explanation does not change the running deployment. Multi-AZ, multi-region, and global operation are not qualified by this two-node workflow; see [implementation status](archictecutre/09-implementation-status.md) for the remaining work.
