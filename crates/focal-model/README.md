# Versioned domain model

The four authored object families, lifecycle vocabulary and typed identifiers form
the shared Rust contract. Persisted vocabulary values use explicit integer codes;
unknown critical codes fail decoding.

Canonical v1 identity uses fixed-width big-endian integers, checked u32 lengths,
and canonical ordered collections. `CanonicalContent::canonical_bytes`,
`CanonicalContent::content_hash`, `ValidationContent::specification_hash`, and
`manifest_hash` return `Result` with `CanonicalError`. Oversized fields and failed
buffer reservation never produce a usable partial identity. Valid bytes and hashes
remain identical to the checked-in v1 golden vectors. Stored objects expose their
already validated hash through infallible `StoredObject::content_hash`.

A requirement specification excludes allocated validation and parent claim IDs.
Claim identity uses those requirement specifications, allowing content-identical
intent to deduplicate despite fresh allocations. Authored content and mutable
lifecycle state are separate types. The model owns its values and uses no `Arc`.

Run `cargo test -p focal-model --offline` for golden vectors, vocabulary rejection,
length-bound failures and authored/lifecycle separation checks.

The appended `RecordFencedValidationVerdict` command has canonical tag 29 and
Postcard variant index 28. It carries the exact optional receipt generation;
all earlier command encodings, including legacy verdict records, remain unchanged.
