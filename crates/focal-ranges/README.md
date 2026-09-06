# focal-ranges

Experimental P11 groundwork. This crate is not connected to the node's write,
recovery, placement, or transport paths and does not establish distributed range
correctness or the architecture's scale target. Feature work was paused for the
workspace ownership and panic audit.

The current components provide immutable checked key interval maps, owned
prepared session metadata, bounded leased read plans, hashed snapshot staging,
and a RAM range replica backed by `focal-memory::RangeStore`. The session ledger
owns every sequencing decision. `RangeVerifier` is an explicit host seam for
committed decisions, exact range fragments, retained reads and durable custody;
constructing a hash or checkpoint does not prove disk durability. Requests for
source-seal and destination-ready receipts require the host to persist their
checkpoint and supply an authenticated attestation before use.

Metadata candidates are owned and bind process-local owner lineage and a base
sequence. Range pages use the memory crate's shared immutable reader leases;
there are no shared mutable metadata roots. Read responses hold their own memory
permit until dropped, and staged blocks, checkpoints, pending updates, map
history, progress and cursor pins have explicit memory allowances.

The cleanup regression suite covers interval gaps/overlaps and boundary routing,
epoch exhaustion, candidate ownership and reservation rollback, allocation-free
publication, retry stability, snapshot block checksum/restart, pin expiry and
response ownership, monotonic checkpoint publication floors across leadership
changes, and transfer-intent digest binding. It does not exercise the complete
split/move lifecycle or distributed failures.

Before enabling this crate in a node, finish and review the P11 implementation
plan in `docs/archictecutre`, including:

- End-to-end session-log command/fragment integration, range worker custody,
  deterministic materialization and one publication barrier for every affected
  range.
- Complete adversarial verification of pending and historical checkpoint
  certificates, activation, source retirement, translated historical reads and
  bounded retention cleanup.
- Durable transfer transport and crash recovery qualification at every snapshot,
  tail, seal, ready and activation boundary, including quorum loss and stale
  replica incarnations.
- Full split/move tests, concurrent pinned reader tests through relocation,
  pressure/fairness tests against the runtime, and differential graph/index
  reconstruction after restore.

```sh
cargo test -p focal-ranges --offline
cargo clippy -p focal-ranges --all-targets --offline -- -D warnings
```
