# Domain fixtures

`canonical-v1.json` freezes canonical bytes and BLAKE3 digests for one example of each
object family. The unit test constructs the fixed authored examples and compares both
bytes and digests against this checked-in file; tests never regenerate expected values.

The encoding starts with `focal.authored\0`, big-endian schema `u16`, tenant/session
16-byte IDs, and family `u16`. Integers have fixed big-endian width. Variable data has
a `u32` byte/count prefix; relation and scope sets have canonical typed ordering.
The object's allocated address and mutable lifecycle are excluded from identity.

Generated requirement linkage is special: claim identity includes each ordered
`ValidationContent::specification_hash()`, excluding the requirement's allocation ID
and its parent's allocated claim ID. The stored `RequirementRef` still binds the exact
allocated validation ID, and the full validation object's content hash includes its
parent claim. This prevents fresh allocations on an otherwise identical occurrence
from defeating claim deduplication. Other authored object references retain their IDs.

The samples bind tenant 1/session 2; claim 3, evaluator 4, issuer 5, occurrence 6,
root cause 7, validation 8, receipt 9, schema digest `0a` repeated 32 times, evidence
set 11, and artifact 12. The artifact payload is the five ASCII bytes `proof`.
Enum allocation tests freeze twenty lifecycle codes and thirteen terminal statuses;
unrecognized critical vocabulary codes fail decoding.
