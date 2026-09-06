# Native server and client distribution

Status: release orchestration implemented; platform execution and publication
remain qualification gates. This extends P15 and P18. Source compilation is an
optional contributor workflow, never a prerequisite for ordinary installation.

## 1. Product contract

One downloaded `focal` executable contains the foreground server (`focal start`),
human CLI, shell-script interface and stdio MCP server (`focal mcp serve`). Users
need no Rust, protobuf compiler, Python, source tree or agent framework. A
separate client machine installs the same executable and selects an authenticated
context. Deployment size does not introduce a different client distribution.

The README installation path starts with binaries. All platform statements name
their actual runtime baseline and execution evidence. Publishing an artifact is
not evidence of correctness; every binary must run its own native qualification.

## 2. Implemented release path

The [release workflow](../../.github/workflows/release.yml) consumes the exact
[platform catalog](../../scripts/release/platforms.json). The initial matrix has
macOS 15 arm64/x86_64, GNU Linux arm64/x86_64 built on Ubuntu 24.04, and static
musl Linux arm64/x86_64. Each runs on its native processor. The musl binary runs
inside the pinned Rust Alpine image and on the native Ubuntu runner; binary
inspection verifies architecture and absence of dynamic musl dependencies.

The [release implementation](../../scripts/release/release.py) separates checks:

1. Guard workspace, lockfile, toolchain, platform catalog and intentional version
   tag agreement. Manual dispatch builds downloadable CI artifacts only.
2. Build the actual executable. Inspect the native binary architecture and
   runtime dependencies before staging. Reject unexpected libraries, missing
   binaries or a platform that merely cross-compiles without native execution.
3. Run the [binary smoke](../../scripts/release/smoke.py): CLI submission, posting,
   listing and reading; MCP negotiation, catalog and read; kill the server after
   acknowledged writes, reopen the same data directory and verify exact recovery.
   It invokes the supplied executable, never Cargo or an embedding substitute.
4. Require every platform and the workspace verification job. Collect exactly
   the six binaries, provenance manifest and SHA256SUMS. Reject missing, duplicate,
   altered or unexpected files and inconsistent version/source metadata.
5. For an intentional version-tag push, verify the remote tag and refuse an
   already-existing release. Create an unpublished draft, upload the entire set,
   download and compare every uploaded byte, then publish. A failed upload or
   verification leaves the draft unpublished. A manual run cannot publish.

Raw binary names, checksums and machine-readable provenance remain stable public
installation interfaces. SHA-256 provides byte-integrity checks; OS code signing
and macOS notarization need their own qualification. Only the publication job has
repository write permission. See [operator instructions](../../scripts/release/README.md)
for artifact retrieval and deliberate recovery from a failed unpublished draft.

## 3. Remaining native Windows implementation

Windows must receive a working server and client binary. A workflow entry that
skips server execution or a cfg-only build does not complete the port. Existing
Unix socket authentication, private directory checks and directory fsync prevent
the current implementation from meeting this contract on Windows.

Implement these in dependency order:

1. **Owned platform filesystem boundary.** Extract private-directory and file
   operations used by enrollment, client pending journals, operation journals,
   artifact transfer, embedded storage and the log. Keep Unix behavior intact.
   Provide safe Rust Windows implementations for user SID/DACL ownership,
   reparse-point/hardlink refusal, handle identity and private creation. Respect
   the workspace unsafe-code policy; do not spread FFI across adapters.
2. **Durable publication.** Specify and implement Windows file flush, directory
   and rename/replace barriers for WAL segments, CURRENT, checkpoints, evidence
   seals and client journals. Surface unsupported durability as an error. Test
   acknowledged writes across process kill and reopen, interrupted publication,
   locked destinations, disk-full conditions and stale temporary files.
3. **Authenticated local transport.** Implement bounded same-user named pipes or
   an equivalently authenticated native local transport for data and admin
   operations. Reuse framed typed requests, deadlines, backpressure and owned
   buffers. Match Unix peer-credential guarantees without opening an implicit
   unauthenticated TCP port. Test wrong-user access, server replacement,
   concurrent clients, disconnects and shutdown.
4. **Client paths and process integration.** Resolve the native LOCALAPPDATA
   location, retain non-lossy OS paths, and provide locks, cancellation, signal/EOF
   shutdown and private output publication for CLI/MCP. Test spaces, non-ASCII
   paths, long paths, existing output, interrupted transfer and exact retry.
5. **Hosted binary qualification.** Add real MSVC x64 and arm64 runners only when
   the full server, CLI, MCP, enrollment, evidence and restart paths execute.
   Qualify x86 separately if adding Vorpal's full matrix; verify address-space
   limits, conversion bounds and storage compatibility before advertising it.
6. **Atomic catalog expansion.** Add each qualified target to the platform
   catalog, binary inspector, complete-asset verifier, checksums, smoke matrix
   and installation table in one change. A release remains all-or-nothing for
   its declared catalog. Publish no placeholder Windows artifact.

## 4. Qualification and release acceptance

The orchestration unit tests use synthetic binaries and mocked publishing; they
prove selection and integrity behavior, not hosted builds. A local native smoke
proves only its executed platform. Record exact commands, source revision,
platform, compiler, binary digest and results in [09](09-implementation-status.md).

Release acceptance requires all native lanes, workspace checks, crash recovery,
complete byte verification and an actual downloadable tagged release. Test
installation on a clean machine without developer tools, then run server, CLI and
MCP with no source checkout. Verify a separate client can use its authenticated
context and that binary replacement preserves existing ledger and retry history.
Keep checksums and runtime baselines with the published assets. Regional scale
and deployment guarantees still require [08](08-stepped-complexity-and-deployment.md);
packaging cannot substitute for those measurements.
