# Focal: rules for every change

These are absolute. They apply to every crate in the workspace, including crates
that exist only to support tests (`focal-sim`, `tools/load`): anything compiled
outside a `#[cfg(test)]` module or a `tests/` directory is production code.

## 1. No panics in production code. Ever.

Production code returns and handles errors. It never aborts the thread it runs on.

- No `panic!`, `unwrap`, `expect`, `unreachable!`, `todo!`, `unimplemented!`,
  `assert!` family, slice or map indexing (`a[i]`), or unchecked arithmetic
  (`+ - * / %`, `as` narrowing). Use `checked_*`, `saturating_*`, `try_from`,
  `.get(..)`, and typed errors.
- A lock that may be poisoned, a channel that may be closed and a task that may
  have ended are errors to return, not conditions to unwrap.
- A dependency that can panic is called behind the existing unwind boundary
  (`DurableNode::guarded_in`), and the path that reaches the panic is closed at
  its cause as well. The boundary is the last fence, never the fix.
- The workspace lints deny these (`Cargo.toml`, `[workspace.lints.clippy]`) and
  `bash scripts/check-production.sh` enforces them twice over: Clippy on every
  library and binary target, and `scripts/check_production_policy.py` on the
  source, which refuses any production `#[allow]` of one of them (the opt-out a
  `deny` would honour) and proves itself on its fixtures first. Never add an
  `#[allow]` for one of them to production code — the measurement tools
  (`tools/load`) and `focal-sim` included. Test modules carry the
  `cfg_attr(test, allow(..))` block the other crates use, or an `allow` inside
  a `#[cfg(test)]` item.

## 2. Nothing grows without a bound.

Every collection, queue, cache, map, log, journal, retry loop and wait has a
stated bound, and reaching the bound is a typed refusal (`Capacity`) or an
eviction with a stated rule. Never silent growth, never a silent drop.

- A map keyed by something a peer or a caller chooses (node ids, flows,
  ledgers, request ids) has a maximum size and is pruned when its key leaves
  the configuration.
- A loop ends on a counted budget or a deadline, including loops that end
  "with probability one".
- Memory is charged to `focal_memory::MemoryBudget`; disk to the disk budget.
- A long-running node that degrades is a bug to root-cause, never environmental.

## 3. Root causes, no shortcuts.

Fix the cause, never the symptom. Do not raise a limit, lengthen a timeout or
retry to get past a failure. Do not defer work that the change requires. A test
waits on the fact it needs, charged to the progress of what it waits on
(`focal_timing::ProgressDeadline`), never on a wall-clock guess.

## 4. Gates

Every batch passes all of these on its final tree, on Linux, macOS and Windows CI:

```
cargo fmt --all --check
python3 scripts/check-contracts.py
bash scripts/cargo.sh clippy --workspace --all-targets --locked -- -D warnings
bash scripts/check-production.sh
cargo deny check advisories bans licenses sources
bash scripts/cargo.sh test --workspace --locked -- --test-threads=4
```
