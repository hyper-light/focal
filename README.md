# focal
Protocol and ledger platform for dynamic multi-agent communication.

Rust architecture and detailed implementation plan: [docs/archictecutre](docs/archictecutre/README.md).

Production Rust must handle failures without panicking and use owned or borrowed values wherever possible. The [ownership and failure policy](docs/archictecutre/10-ownership-and-failure-policy.md) defines enforcement and the remaining concurrent sharing boundaries.

Implementation is in progress. The current Rust workspace includes the domain state machine, custom memory primitives, a disk-backed Raft ledger, evidence storage and validators, and a local service. Distributed deployment and scale qualification remain open; see [implementation status](docs/archictecutre/09-implementation-status.md).

Run the durable claim/testament/validator example with a dedicated data directory:

```sh
bash scripts/cargo.sh run -p focal-node --bin focal -- demo --data-dir /tmp/focal-example
```

Repeat the command to recover and verify the same proof. To run the local service, use `focal start`; `focal status` reads its published prefix. `focal demo` opens the directory exclusively, so stop a running service before using that embedded example. `focal request REQUEST.json` sends a versioned request to the service; preserve the same file when retrying an unknown outcome.

The [manual CLI guide](docs/manual-cli.md) covers `submit claim/testament/artifact` with flags, JSON or YAML; `get` and optional-filter `list` commands; artifact downloads; validation results; and durable `request inspect/retry` recovery. The [source-backed CLI and agent plan](docs/archictecutre/13-cli-and-agent-implementation-plan.md) tracks the remaining cluster, remote-tool and challenge/consult work. `focal mcp serve` exposes the same typed operations through a bounded stdio adapter; see [the MCP guide and agent skills](docs/mcp.md).

Source builds require Rust and protobuf; see [building and verification](docs/building.md). A local acknowledgment means the write is synced to this disk. The single-node default does not survive loss of that disk.
