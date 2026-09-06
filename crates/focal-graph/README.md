# Paged domain graph

`GraphStore` adapts the four domain object families to `focal-memory` COW pages.
One ordered range contains object rows and every associated index: kind, claim,
content identity, forward/reverse relations, lifecycle, active deadlines, and
required validations. Canonical authored relations and derived family associations
have distinct `GraphRelation` variants; indexing does not change authored content.

`prepare_patch(previous_view, patch, predecessor, lane)` projects exact owned row
updates from the core without comparing whole state maps. It reads prior versions
only for affected objects and associations, then produces the page changes. The
legacy `prepare_transition` map-comparison path remains available for reference
checks. `PreparedGraph` retains
every page allocation before log admission. Multiple preparations form an exact
root-ancestry chain. `validate_publication` checks every link before any root
changes, and `publish` performs an allocation-free root swap. Rejected
proposals or invalidated speculative suffixes release their pages through RAII.
`from_state` reconstructs indexes at an arbitrary verified recovery prefix; `audit`
independently rebuilds the row projection and compares all entries and charges.

`GraphSnapshot` pins one prefix with an expiring memory lease. Point reads, sorted
scans, typed object keyset pagination, and breadth-first traversal all read that
root. Traversal uses indexed forward/reverse edges, budgets actual row probes,
binds root/direction/relation filters/authority scope to an opaque continuation,
and reports cumulative truncation explicitly. It never reports a budget-truncated
graph as complete. Serving layers still own authentication, authorization and
external cursor signatures; this crate does not infer authority from a digest.

The ledger still retains one authoritative map-backed `Core` alongside the graph.
Default graph preparation is proportional to the changed projection; legacy map
comparison and recovery rebuild remain O(session state). Memory is accounted conservatively:
`reference_charge` uses a nonallocating serialization-size pass with a 64x factor
and fixed bookkeeping allowance for the current closed model schema. It covers
short encoded integers, container allocation, spare capacity, padding and tree
overhead, and intentionally overcharges. This is not an RSS measurement or a
claim of production memory efficiency. A future schema adding allocation shapes
must revisit this bound. Exact typed capacity accounting remains necessary to replace this conservative
allowance and qualify the duplicated core/graph representation.

The ledger reserves pending row versions, epoch workspace, immutable results,
graph pages and retained delta copies before leader proposal. Followers prepare
and reserve the whole bounded epoch before local publication. Capacity can reduce
an epoch to a smaller batch; if one command cannot fit, application stops safely.
Core outputs, the entire graph chain and retained delta bounds are validated
before the first mutation. Recovery rebuilds
indexes before readiness. This does not yet implement state sharding, archive
retirement, full RSS accounting, or the global P03/P09 qualification envelope.

Verification: `bash scripts/cargo.sh test -p focal-graph -p focal-ledger --offline`
and `bash scripts/cargo.sh clippy -p focal-graph -p focal-ledger --all-targets --offline -- -D warnings`.
