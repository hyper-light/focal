# Native binary releases

One `focal` executable provides the server (`start`), human CLI and MCP stdio
server (`mcp serve`). Release assets are raw binaries, not source-install helpers.
This workflow does not imply that a release/tag already exists.

`platforms.json` is the exact required asset matrix. Each target is built and
executed on its own architecture. macOS uses macOS 15; GNU Linux uses Ubuntu 24.04
with a checked glibc 2.39 symbol ceiling. Static musl binaries run both inside the
pinned official Rust Alpine image and on the corresponding native Ubuntu runner.
ELF interpreter/dependency checks reject dynamically linked musl output. macOS
dependency checks reject non-system libraries. These are release qualification
lanes; they are not a claim that all six have already passed in GitHub Actions.

Windows is an outstanding port, not a successful skipped lane. Current Unix
socket/peer credentials, private-file checks and directory durability operations
must be implemented and tested on Windows before its binaries enter this matrix.
macOS binaries are not yet signed or notarized.

## Run and publish

- Manually dispatch **Downloadable server and client** to build installable CI
  artifacts. Dispatching it on a tag still does not publish a release. Download
  the `complete-release-assets` artifact, extract it, verify `SHA256SUMS`, and
  make the chosen Unix binary executable with `chmod +x`.
- An intentional push of `v<workspace-version>` runs the same checks and publishes
  only after every matrix job, the workspace tests and complete collection pass.
  The workspace packages, lockfile, toolchain and tag must agree. This document
  does not push or create a tag.
- Publishing first creates an unpublished draft, uploads the exact complete
  asset set, downloads every uploaded asset and verifies its bytes, and finally
  publishes the draft. A failed upload/verification leaves an unpublished draft
  for inspection. Remove that failed draft explicitly before retrying; published
  releases and previously existing drafts are never overwritten by this script.

The `contents: write` token is restricted to the final tag-only job. Manual builds
and matrix jobs have read permission. Concurrent runs for the same ref serialize.

`SHA256SUMS` covers every binary and `release-manifest.json`. The manifest records
target, runtime baseline, commit, compiler, roles, size and SHA-256. Collection and
publication independently reject missing, duplicate, unexpected, altered or
mismatched files. Hashes establish byte integrity; this is not code signing.

## Local script checks

These developer/CI scripts require Python 3.11 or later (for standard-library
TOML parsing); the downloaded Focal executable does not require Python. The
workflow installs Python 3.13 explicitly rather than using the runner's default.

```sh
python3 -m unittest discover -s scripts/release -p 'test_*.py'
python3 scripts/release/smoke.py /absolute/path/to/built/focal
```

The first command uses synthetic byte files and mocked GitHub calls; it does not
build, execute a server, access the network or publish. The second executes only
the supplied binary, in an isolated temporary data directory: real CLI submit,
post, list and get, MCP initialize/catalog/claim read, SIGKILL after acknowledged
writes, recovery by a new server process, and another MCP read. It never invokes
Cargo. This proves local acknowledged-write recovery, not replicated durability.

After an artifact download, the standalone verifier is:

```sh
python3 scripts/release/release.py verify --input release-assets
```

Run it from the same source commit as the release; it also checks that provenance.
Users without Python can verify the conventional checksum file with
`sha256sum -c SHA256SUMS` on Linux or `shasum -a 256 -c SHA256SUMS` on macOS.

The native runner labels follow [GitHub's runner reference](https://docs.github.com/en/actions/reference/runners/github-hosted-runners).
The Alpine compiler image comes from the [official Rust image](https://hub.docker.com/_/rust).
The exact multi-platform index reference is
`rust:1.94.1-alpine@sha256:77237dd363a0b127bb5ef532c2d64c0deb380b738e43a9c4bdac73398d6d0a08`,
also recorded in `platforms.json`; update that digest with the pinned Rust version.
Artifact
actions use the published [upload v4.6.2](https://github.com/actions/upload-artifact/releases/tag/v4.6.2)
and [download v4.3.0](https://github.com/actions/download-artifact/releases/tag/v4.3.0).
