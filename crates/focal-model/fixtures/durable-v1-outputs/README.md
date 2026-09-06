# Original V1 output and managed metadata corpus

These bytes were captured with the original model Serde/Postcard writers and
original managed receipt/control hashing functions before adding their dedicated
V1 representations. `capture-source.json` records the source SHA-256 values,
matched Cargo dependency fingerprints, exact rlib identities, compiler and
standalone `rustc` command. `writer-source.tar.gz` preserves those source files;
`manifest.json` records each native output's length and SHA-256. No current V1
writer generated these expectations.

The generator uses `durable-v1-nested/receipts.rows` from the existing Core corpus
for all 12 command-result alternatives plus empty Generated/Existing vectors.
Every `.rows` file is an original Postcard `Vec<T>`; names identify the element
except `families` (`ManagedRequestFamily`) and the two hash files (`ContentHash`).

Coverage includes:

- All 11 DeltaFact, five EffectIntent, six InformReason and three DomainOutcome
  alternatives; optional values, all 26 error codes and numeric boundaries.
- Every cursor offset, filter, mode and resynchronization reason. The 81 cursor
  records include empty, unsorted and duplicate filter IDs deliberately: codecs
  preserve historical representations independently of current admission.
- 98 managed receipts: every command result, absent/present cursor records and
  both sealed families, with their original acknowledgment commitments.
- Every stream command, state, outcome, query, resolution and read result; empty
  and populated acknowledgments and voter sets; zero and maximum counters.
- Original control intent hashes, including the independent control request ID's
  deliberate exclusion from that commitment.

Tests decode these fixed native artifacts, round-trip their exact bytes through
V1, compare the original hashes and reject unknown critical enum ordinals.
