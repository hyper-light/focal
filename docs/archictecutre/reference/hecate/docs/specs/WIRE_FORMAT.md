# SPEC: hecate-wire — the canonical payload format

Status: ACCEPTED 2026-08-17 ("accepted for the sake of output" — user; presented
in-message through the full revision arc: base draft → media amendments → bounds
correction → transfer-research revision → position-bound-CV addendum). Amended
2026-08-20: §6 adds the QUEUE/FANOUT queue/topic/subscription + ephemeral-notify
record vectors as new append-only `#[derive(Wire)]` types (trybuild-gated; queue
bodies map to `ContentClass::Opaque`). Satisfies
PROTOCOL.md §6's process rule: this document merges before any codec
implementation lands. Ratified inputs: PROTOCOL.md §6 (borsh canonicalization ×
postcard varint density, one derive macro, canonical-or-reject); REGISTRY.md §2b
(`DocValue` rides this format); RUNTIME.md lint wall. Scope: Rust-only; TS/Python
SDKs speak MCP/JSON at the tool plane and never touch this format; stability is
guaranteed by committed test vectors, never cross-language parity. Companion:
`TRANSFER.md` (the transfer plane's machinery; this document owns only the
record shapes and their rejection clauses).

## 1. The axiom

**One value, one encoding.** For every value of every wire type there is exactly
one legal byte sequence, and the decoder rejects all others. Every rule carries
its rejection clause; every rejection clause carries a committed negative test
vector. Content identity (BLAKE3 over spec bytes), dedup, and replay comparison
all stand on this axiom — an accepted non-canonical encoding is a
content-identity fork, the named Sylk-class fault this format exists to make
unrepresentable.

## 2. Primitives

| Type | Encoding | Rejection clauses |
|---|---|---|
| `u8`/`i8` | one raw byte | — |
| `u16`–`u128` | **LEB128 varint, minimal form** | overlong encoding; value exceeding the type's range; input exhausted mid-varint |
| `i16`–`i128` | zigzag, then LEB128 minimal | same as unsigned |
| `usize`/`isize` | **do not exist on the wire** (platform-dependent) | the derive refuses the type at compile time |
| `bool` | `0x00`/`0x01` | any other byte |
| `f32`/`f64` | IEEE-754, little-endian, fixed width | any NaN bit pattern other than the canonical quiet NaN (`0x7FC0_0000` / `0x7FF8_0000_0000_0000`); negative zero (encoder normalizes `−0 → +0`; decoder rejects `−0`) |
| `()` | zero bytes | — |
| `Option<T>` | `0x00` = None; `0x01` + payload = Some | tag ≥ `0x02` |

## 2b. The two length domains (types, not conventions)

Length fields are typed by domain; **carriers follow bounds, bounds never follow
carriers** — no width literal ever appears as a justification:

- **`FrameLen`** — inline lengths (`String`, `Vec<T>`, inline byte fields, map
  counts). Bounded by the derived frame cap (PROTOCOL's, referenced, not
  redefined) — a control-latency constant (RFC 9113 §4.2's rationale: large
  frames delay time-sensitive control frames), unrelated to content size.
  Varint on the wire; validated against the cap at encode and decode.
- **`ContentLen(u64)`** — anything describing stored content (a file, a chunk,
  a transfer, a range). The law: **the codec must never be the binding
  constraint on content size** — that bound belongs to physics (store capacity,
  budgets, the manifest-arithmetic ceiling of §3b), never to a format decision.
  Varint on the wire; validated against derived caps at the validation points
  named in `TRANSFER.md` §5; a protocol-declared ceiling sits below type width
  (the QUIC 2⁶² pattern). Every surveyed production system sizes content
  lengths 64-bit-class (bao u64, QUIC 62-bit, WebSocket 63-bit); cloud object
  ceilings sit at 2⁴⁵–2⁴⁸ and grow.
- The derive refuses a raw integer in any length position — the field type must
  be `FrameLen` or `ContentLen`.

## 3. Composites

- **`String`**: `FrameLen` + UTF-8 bytes. Reject: invalid UTF-8 (including
  surrogates), length exceeding remaining input or the derived allocation cap.
- **`Vec<u8>` / `WireBytes`**: `FrameLen` + raw bytes. Same rejections.
- **`Vec<T>` / sequences**: `FrameLen` count + elements. Count validated against
  remaining input *before* allocation.
- **Maps and sets** (`BTreeMap`/`BTreeSet` only): count + entries in **strictly
  ascending canonical-encoding byte order** (the order is over each key's own
  canonical encoding, compared lexicographically — language-independent, immune
  to `Ord`-vs-encoding divergence). Reject: out-of-order entries, duplicate keys.
- **Structs**: fields concatenated in declaration order. No tags, no padding.
- **Enums**: varint variant index (declaration order) + payload. Reject: index
  with no variant in the decoder's schema (§5 cross-version rule).
- **Tuples / fixed arrays**: element concatenation, no prefix.
- **Nesting depth**: bounded by a derived cap (deepest committed schema × stated
  headroom, derivation at the definition site); exceeding it rejects — decode is
  bounded in time and allocation on arbitrary garbage (PROTOCOL P3 as format law).

## 3b. `ContentRef` — how payloads name bulk content

```rust
struct ContentRef {
    root:  [u8; 32],      // full BLAKE3 tree root — never the envelope's truncated form
    len:   ContentLen,
    class: ContentClass,  // closed, append-only wire enum
}
enum ContentClass {
    SourceText, Document, Image, Audio, Video, Archive, Media(u32), Opaque,
}
```

- `root` is **chunking-independent** (the BLAKE3 tree property): storage
  chunking and transfer granularity can differ and change without touching
  identity. In private dedup domains the tree runs in BLAKE3 **keyed mode**
  (OBJECT_TIER §9's settlement) — same structure, domain-keyed.
- `ContentClass` is the **chunking-policy selector** recorded at write time
  (CDC for text/source, fixed-size framing for opaque media — deterministic,
  recorded, never sniffed at read). It is also the **verification-structure
  selector** (the class-aligned law, position-bound-CV addendum): a CDC-class
  object's trusted structure is its content-addressed **manifest of standalone
  chunk hashes** (the casync model — BLAKE3 chaining values are position-bound,
  so standalone chunk hashes are the correct position-independent dedup
  identity); a fixed-framed media object's trusted structure is the
  **whole-blob BLAKE3 tree** with bao outboards for verified ranges against the
  bare root (the iroh model). One law: verification structure follows content
  class — never two mechanisms for one object.
- MIME/dimensions/duration/codec parameters are **not** here — they live in the
  per-class media descriptor document (Branch 26), an ordinary wire struct
  referencing the `ContentRef`.
- `len` inconsistent with resolved content is a *store-level* verify failure
  (typed, names the chunk), not a decode failure.
- **The object-size bound is manifest arithmetic**: the protocol constant is
  the maximum manifest length (derived, `TRANSFER.md` §5), with
  HashSeq/piece-layer indirection past it — never a length-type width.

## 3c. The inline-vs-reference law (derive-enforced)

Any byte field in a **ledger-content type** (claims, testaments, validations,
artifacts — types whose hashes are content identity) is capped at a derived
inline budget (`frame_cap − AAD − record_header`, derivation at the definition
site). Above the budget, content is a `ContentRef`, structurally: **the derive
refuses unbounded byte fields in ledger-content types** — the field type must be
`BoundedBytes<CAP>` or `ContentRef`. This generalizes HEALTH.md H8's type-walk
into format law: *no plane carries bulk bytes by value except the content
plane.* A 2 GB video submission never touches claims-plane framing — the upload
is `TRANSFER.md` machinery; the claim carries a ~42-byte `ContentRef` +
descriptor ref.

## 4. Trailing bytes

A payload decodes to exactly one value and must consume exactly `payload_len`
bytes. Reject: trailing bytes after the root value; input exhausted before the
root value completes — **except** §5's appended-field rule, the single,
schema-checked exemption.

## 5. Schema reflection, `schema_hash`, and evolution

- `#[derive(Wire)]` is **the single interpreter** of a type definition: it emits
  the canonical encoder, rejecting decoder, schema reflection, schema hash, and
  (for tool-plane-shared types) the JSON Schema projection. No serde, no
  schemars, no second interpreter, ever.
- **Schema reflection** is itself a wire type (`SchemaNode`: primitives,
  sequences, maps, ordered struct/enum field lists, a named-type table for
  recursion) — encoded under this document's own rules; the format describes
  itself.
- **`schema_hash`** = first 8 bytes of BLAKE3 over the canonical encoding of the
  reflection. The envelope carries it; the parser checks it before decode.
- **Evolution is append-only, compiler-enforced**: every wire type is checked
  against a **committed schema snapshot**; removing, reordering, or retyping a
  field or variant is a compile error (trybuild-gated). Legal: appending
  `Option<T>`/defaulted struct fields, appending enum variants.
- **Cross-version decode by ancestor-hash matching, never tolerant reading**:
  the snapshot registry maps each type's current hash to its committed ancestor
  hashes within the same major version. An ancestor's hash decodes under that
  ancestor's schema — input exhausting at the ancestor's boundary fills appended
  fields with `None`/default (the one §4 exemption, legal only under a matched
  ancestor hash). Unknown hash ⇒ `schema_unknown` rejection at the parser,
  pre-decode, counted. A major-version bump abandons the ancestor set; there is
  no decode-across-major path and never will be.

## 6. Test vectors (the stability contract)

Committed corpus: varint boundaries (2⁷ᵏ ± 1 per width), zigzag extremes,
`u128`/`i128` maxima, the NaN zoo (every non-canonical pattern as a negative),
`−0`, nested `Option`, empty containers, map-ordering positives and
out-of-order/duplicate negatives, UTF-8 boundary cases, max-depth positive +
depth-bomb negative, ancestor-decode pairs, trailing-byte negatives,
`ContentLen` extremes, `ContentRef` roundtrips, `ContentClass` ancestor-decode,
inline-cap boundary (`CAP` positive, `CAP+1` negative), transfer-record vectors
(`TRANSFER.md` shapes: missing-set negotiation, span-record chunks,
manifest-last close, resume-after-kill, gap/overlap negatives), queue/topic/
subscription record vectors (`QUEUE.md`/`FANOUT.md` shapes — enqueue body, lease/ack,
and topic/subscription/filter descriptors — as new append-only `#[derive(Wire)]`
types under §5's trybuild gate, with queue bodies mapping to `ContentClass::Opaque`;
the ephemeral-class notify codec is likewise a new append-only type; positive +
negative vectors per rule). **Rules: every
rejection clause has at least one negative vector; vectors never change within a
major version; adding a rule adds its vectors in the same change.**

## 7. Test matrix (failure each catches)

| # | Test | Catches |
|---|---|---|
| WF1 | Roundtrip property fuzz: arbitrary values encode→decode to identity | codec correctness (P1) |
| WF2 | Canonical-reject fuzz: every mutation class ⇒ typed rejection, never acceptance | the content-identity break class (P2) |
| WF3 | Garbage/truncation/depth-bomb fuzz: no panic, bounded time and allocation, counts validated pre-allocation | untrusted-input DoS (P3) |
| WF4 | Evolution: ancestor-hash decode fills appended fields exactly; unknown hash rejects pre-decode; reorder/remove/retype = compile failure | silent wire breaks (P4) |
| WF5 | Schema self-description: every committed type's reflection encodes canonically under this format's own rules; hash stable across platforms/runs | reflection drifting from the format it describes |
| WF6 | Vector conformance: full corpus passes on every platform; a vector diff within a major version fails CI structurally | stability by hope |
| WF7 | Identity ⇔ equality: values encode to identical bytes iff equal (property fuzz) | content identity resting on a non-canonical corner |
| WF8 | Derive refusal: unbounded `Vec<u8>`/`String` field in a ledger-content type = compile error; raw integer in a length position = compile error (trybuild) | the inline-vs-reference and length-domain laws eroding |
| WF9 | Class-aligned verification: CDC-class objects verify via manifest chunk hashes, media-class via tree root + bao path — cross-class verification attempts are typed errors | one object served by two verification structures |
| WF10 | Cross-plane audit: no code path encodes content bytes into a claims-plane payload above the inline cap (architecture test) | bulk bytes leaking into the claims plane |

## 8. Acceptance criteria

1. This document merges **before** any codec code exists (satisfied by
   construction).
2. Every rule has a paired rejection clause and negative vector (WF2/WF6
   permanent CI).
3. The derive is the only interpreter in the tree; no serde/schemars dependency
   in any wire crate (architecture test).
4. The schema snapshot is committed with the first wire type and every type
   thereafter; trybuild gates permanent.
5. Depth, allocation, length, and inline caps are derived with derivations at
   definition sites; the frame cap is PROTOCOL's, referenced.
6. `usize`/platform-dependent types and raw-integer length fields are
   unrepresentable on the wire (compile-time).
7. The object-size bound is the derived max-manifest-length; no length-type
   width is ever cited as a bound.
