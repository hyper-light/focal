# Streaming merge gate: green is increment-validated, disk is claim-satisfied

Completed work does not land in one batch at testament close. The Engineer's work
streams as increments; each increment's own validations (Guardian safety, lint,
conflict check) pass and it merges into green immediately (canonical-rebase engine,
ADR-0005); the claim's whole-work
validations (tests green, Inspector approval) gate the **disk commit**, not green
entry. Failures fix forward via superseding increments — green is never rolled back in
place. There is no post-merge audit loop: validation precedes merge, always.

This resolves the tension between green purity ("no unvalidated work visible to other
pods") and freshness ("no arbitrary staleness window while validation runs") without a
second staging-layer class. It reuses machinery the ledger already has — artifact
streaming before the closing testament, per-validation evaluation, supersession — and
it makes Sylk's three ugliest merge faults (rejected work in green, whole-overlay disk
flushes, audits reading a pre-merge base) unrepresentable rather than merely fixed.

## Considered Options

An "amber" pending-validation layer other pods could opt into reading was rejected: it
relocates the arbitrary-freshness split instead of removing it, and adds a fourth layer
class to every read path.
