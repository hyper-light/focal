# External MCP client qualification

These scripts drive a real `focal mcp serve` adapter from third-party MCP
clients. They are not part of the ordinary test run: the Rust runner
(`crates/focal-node/tests/mcp_external.rs`) executes them only when
`FOCAL_EXTERNAL_MCP=1` is set, because each needs a tool from outside this
repository and network access to install it. Without the variable the runner
records the skip and passes.

| Script | Client | Needs |
| --- | --- | --- |
| `inspector.sh` | MCP Inspector CLI (`@modelcontextprotocol/inspector`) | `npx` (Node.js) |
| `claude-code.sh` | Claude Code (`claude mcp add`) | the `claude` CLI on `PATH` |
| `python-sdk.py` | The official Python SDK (`mcp` package) | `python3` with `mcp` installed |

Every script receives the adapter command through `FOCAL_BIN` and
`FOCAL_DATA_DIR` (a running founder node's data directory) and must exit
zero only after it has (1) completed the MCP initialization handshake,
(2) listed every tool page, and (3) called `ledger.standing` (native) or
`ledger.summary` (V1) and received a structured application result. A
script that cannot find its client exits with status 3, which the runner
reports as "client unavailable" and treats as a failure under
`FOCAL_EXTERNAL_MCP=1`.

Record each executed run in document 09 with the client and its version
(`npx @modelcontextprotocol/inspector --version`, `claude --version`,
`python3 -c 'import mcp; print(mcp.__version__)'`).
