# @hyper-light/focal

The `focal` executable — the Focal server, the human CLI and the stdio MCP
server in one binary — installed from npm. This package carries no code of its
own: it depends on one platform package per supported target
(`@hyper-light/focal-<os>-<arch>[-<libc>]`), npm installs the one matching this
machine, and the `focal` command on `PATH` is that native executable (on
Windows, a shim that runs `focal.exe`).

```sh
npm install -g @hyper-light/focal
focal --version

# or run it without installing, for example as an MCP server:
npx -y @hyper-light/focal --data-dir /absolute/path/to/ledger mcp serve
```

Every binary here is the same byte-for-byte file attached to the matching
[GitHub release](https://github.com/hyper-light/focal/releases), built and
smoke-tested on its own architecture; `THIRD-PARTY-NOTICES.txt` in each
platform package lists the bundled crates and their licenses.

Supported: macOS 15+ (arm64, x64), Linux (arm64, x64; glibc 2.39+ or any
libc with the static musl build), Windows 10 1809+ (x64, arm64). See the
[repository](https://github.com/hyper-light/focal) for the documentation.
