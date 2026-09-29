# focal-node

The `focal` executable — the Focal server, the human CLI and the stdio MCP
server in one binary — installed from PyPI. The wheel contains no Python
code: it places the native executable on your `PATH` as `focal`, one wheel per
platform, the same byte-for-byte file attached to the matching
[GitHub release](https://github.com/hyper-light/focal/releases).

```sh
pip install focal-node
focal --version

# or run it without installing, for example as an MCP server:
uvx --from focal-node focal --data-dir /absolute/path/to/ledger mcp serve
```

Supported: macOS 15+ (arm64, x64), Linux (arm64, x64; a glibc 2.39+ wheel
and a fully static musl wheel that installs on any other libc), Windows 10
1809+ (x64, arm64). `THIRD-PARTY-NOTICES.txt` in the wheel lists the bundled
crates and their licenses. See the
[repository](https://github.com/hyper-light/focal) for the documentation.
